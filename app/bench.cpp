// bench: load generator for a running cluster. Measures throughput and
// latency, and writes bench/results/<date>_<name>.json (CLAUDE.md format).
//
//   bench --peers 1=127.0.0.1:50051,... --clients 16 --duration 30
//         [--read-pct 50] [--value-bytes 128] [--fsync fullfsync]
//         [--name c16_fullfsync] [--notes "..."]
//
// Each client is a thread with its own KvClient (one operation in flight, as
// the dedup design requires), so --clients is the concurrency level.
// --fsync is only a label: it must match how the cluster was started.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "kv/kv_client.hpp"
#include "net/grpc_client_transport.hpp"

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
  std::map<rafty::NodeId, std::string> peers;
  int clients = 16;
  int duration_s = 30;
  int read_pct = 50;
  int value_bytes = 128;
  std::string fsync = "fullfsync";
  std::string name;
  std::string notes;
};

[[noreturn]] void usage() {
  std::fprintf(stderr,
               "usage: bench --peers ID=HOST:PORT,... [--clients N] "
               "[--duration S] [--read-pct P] [--value-bytes B] "
               "[--fsync LABEL] [--name NAME] [--notes TEXT]\n");
  std::exit(2);
}

Options parse(int argc, char** argv) {
  Options o;
  for (int i = 1; i + 1 < argc; i += 2) {
    const std::string f = argv[i], v = argv[i + 1];
    if (f == "--peers") {
      std::stringstream ss(v);
      std::string item;
      while (std::getline(ss, item, ',')) {
        const auto eq = item.find('=');
        if (eq == std::string::npos) usage();
        o.peers[std::stoull(item.substr(0, eq))] = item.substr(eq + 1);
      }
    } else if (f == "--clients") { o.clients = std::stoi(v);
    } else if (f == "--duration") { o.duration_s = std::stoi(v);
    } else if (f == "--read-pct") { o.read_pct = std::stoi(v);
    } else if (f == "--value-bytes") { o.value_bytes = std::stoi(v);
    } else if (f == "--fsync") { o.fsync = v;
    } else if (f == "--name") { o.name = v;
    } else if (f == "--notes") { o.notes = v;
    } else { usage(); }
  }
  if (o.peers.empty() || argc % 2 == 0) usage();
  if (o.name.empty()) {
    o.name = "c" + std::to_string(o.clients) + "_" + o.fsync;
  }
  return o;
}

// Output of a shell command, trimmed (for commit and hardware info).
std::string sh(const char* cmd) {
  std::string out;
  if (FILE* p = popen(cmd, "r")) {
    char buf[256];
    while (fgets(buf, sizeof buf, p)) out += buf;
    pclose(p);
  }
  while (!out.empty() && (out.back() == '\n' || out.back() == ' ')) out.pop_back();
  return out;
}

std::string hardware() {
#ifdef __APPLE__
  return sh("sysctl -n machdep.cpu.brand_string") + ", " +
         sh("sysctl -n hw.ncpu") + " cores, internal SSD, macOS " +
         sh("sw_vers -productVersion");
#else
  return sh("uname -srm") + ", " + sh("nproc") + " cores";
#endif
}

// One client thread: issue operations back to back until `stop`.
void run_client(const Options& o, uint64_t client_id, const std::atomic<bool>& stop,
                std::vector<double>& latencies_ms, uint64_t& errors) {
  net::GrpcClientTransport transport(o.peers, rafty::Millis{500});
  rafty::SteadyClock clock;
  std::vector<rafty::NodeId> servers;
  for (const auto& [id, addr] : o.peers) servers.push_back(id);
  kv::KvClient client(client_id, servers, transport, clock, rafty::Millis{500},
                      rafty::Millis{20});

  std::mt19937_64 rng(client_id);
  const std::string value(static_cast<std::size_t>(o.value_bytes), 'v');
  while (!stop) {
    const std::string key = "k" + std::to_string(rng() % 1000);
    bool done = false;
    auto on_done = [&](const kv::KvResponse&) { done = true; };
    const auto t0 = Clock::now();
    if (static_cast<int>(rng() % 100) < o.read_pct) {
      client.get(key, on_done);
    } else {
      client.put(key, value, on_done);
    }
    const auto give_up = t0 + std::chrono::seconds(10);
    while (!done && Clock::now() < give_up) {
      transport.pump(client);
      if (!done) std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    if (!done) {  // cluster unavailable for 10 s: count it and stop this client
      ++errors;
      return;
    }
    latencies_ms.push_back(
        std::chrono::duration<double, std::milli>(Clock::now() - t0).count());
  }
}

double percentile(std::vector<double>& v, double p) {
  if (v.empty()) return 0;
  const auto k = static_cast<std::size_t>(p / 100.0 * static_cast<double>(v.size() - 1));
  std::nth_element(v.begin(), v.begin() + static_cast<long>(k), v.end());
  return v[k];
}

}  // namespace

int main(int argc, char** argv) {
  const Options o = parse(argc, argv);
  std::fprintf(stderr, "bench: %d clients, %ds, %d%% reads, %dB values...\n",
               o.clients, o.duration_s, o.read_pct, o.value_bytes);

  std::atomic<bool> stop{false};
  std::vector<std::vector<double>> lat(static_cast<std::size_t>(o.clients));
  std::vector<uint64_t> errors(static_cast<std::size_t>(o.clients), 0);
  std::random_device rd;
  const uint64_t base_id = (static_cast<uint64_t>(rd()) << 32) | 1;

  const auto start = Clock::now();
  std::vector<std::thread> threads;
  for (int c = 0; c < o.clients; ++c) {
    const auto i = static_cast<std::size_t>(c);
    threads.emplace_back([&, i] {
      run_client(o, base_id + 2 * i, stop, lat[i], errors[i]);
    });
  }
  std::this_thread::sleep_for(std::chrono::seconds(o.duration_s));
  stop = true;
  for (auto& t : threads) t.join();
  const double elapsed =
      std::chrono::duration<double>(Clock::now() - start).count();

  std::vector<double> all;
  uint64_t total_errors = 0;
  for (std::size_t i = 0; i < lat.size(); ++i) {
    all.insert(all.end(), lat[i].begin(), lat[i].end());
    total_errors += errors[i];
  }
  const double tput = static_cast<double>(all.size()) / elapsed;
  const double p50 = percentile(all, 50), p95 = percentile(all, 95),
               p99 = percentile(all, 99);

  // ---- results JSON (CLAUDE.md "Results format") ----
  char date[16];
  const std::time_t now = std::time(nullptr);
  std::strftime(date, sizeof date, "%Y-%m-%d", std::localtime(&now));
  std::filesystem::create_directories("bench/results");
  const std::string path =
      "bench/results/" + std::string(date) + "_" + o.name + ".json";
  std::ofstream f(path);
  f << "{\n"
    << "  \"commit\": \"" << sh("git rev-parse --short HEAD") << "\",\n"
    << "  \"date\": \"" << date << "\",\n"
    << "  \"hardware\": \"" << hardware() << "\",\n"
    << "  \"fsync_mode\": \"" << o.fsync << "\",\n"
    << "  \"cluster_size\": " << o.peers.size() << ",\n"
    << "  \"workload\": {\n"
    << "    \"read_pct\": " << o.read_pct << ",\n"
    << "    \"value_bytes\": " << o.value_bytes << ",\n"
    << "    \"clients\": " << o.clients << ",\n"
    << "    \"duration_s\": " << o.duration_s << "\n"
    << "  },\n"
    << "  \"throughput_ops_s\": " << static_cast<long>(tput) << ",\n"
    << "  \"latency_ms\": { \"p50\": " << p50 << ", \"p95\": " << p95
    << ", \"p99\": " << p99 << " },\n"
    << "  \"notes\": \"" << o.notes << (total_errors ? " (client errors: " +
                                            std::to_string(total_errors) + ")"
                                                         : "")
    << "\"\n"
    << "}\n";

  std::printf("%s: %.0f ops/s  p50 %.2f ms  p95 %.2f ms  p99 %.2f ms  -> %s\n",
              o.name.c_str(), tput, p50, p95, p99, path.c_str());
  return 0;
}
