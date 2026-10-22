// kv_node: runs one Raft + KV server process.
//
//   kv_node --id 1 --peers 1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053
//           [--data ./data/n1] [--fsync none|fsync|fullfsync]
//
// --peers lists EVERY node, including this one; this node listens on its own
// address. Ctrl-C / SIGTERM shuts down cleanly. kill -9 does not, on purpose:
// restarting with the same --data recovers from the WAL.

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <map>
#include <sstream>
#include <string>

#include "net/kv_node.hpp"

namespace {

[[noreturn]] void usage() {
  std::fprintf(stderr,
               "usage: kv_node --id N --peers ID=HOST:PORT,... "
               "[--data DIR] [--fsync none|fsync|fullfsync]\n");
  std::exit(2);
}

std::map<rafty::NodeId, std::string> parse_peers(const std::string& s) {
  std::map<rafty::NodeId, std::string> out;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, ',')) {
    const auto eq = item.find('=');
    if (eq == std::string::npos) usage();
    out[std::stoull(item.substr(0, eq))] = item.substr(eq + 1);
  }
  return out;
}

rafty::FsyncMode parse_fsync(const std::string& s) {
  if (s == "none") return rafty::FsyncMode::None;
  if (s == "fsync") return rafty::FsyncMode::Fsync;
  if (s == "fullfsync") return rafty::FsyncMode::FullFsync;
  usage();
}

net::NodeConfig parse_args(int argc, char** argv) {
  net::NodeConfig cfg;
  for (int i = 1; i < argc; ++i) {
    const std::string flag = argv[i];
    if (i + 1 >= argc) usage();
    const std::string val = argv[++i];
    if (flag == "--id") {
      cfg.id = std::stoull(val);
    } else if (flag == "--peers") {
      cfg.addrs = parse_peers(val);
    } else if (flag == "--data") {
      cfg.data_dir = val;
    } else if (flag == "--fsync") {
      cfg.fsync = parse_fsync(val);
    } else {
      usage();
    }
  }
  if (cfg.id == rafty::kNoNode || cfg.addrs.empty()) usage();
  if (cfg.data_dir.empty()) cfg.data_dir = "data/n" + std::to_string(cfg.id);
  return cfg;
}

}  // namespace

int main(int argc, char** argv) {
  const net::NodeConfig cfg = parse_args(argc, argv);

  // Block SIGINT/SIGTERM in every thread (threads inherit this mask), then
  // wait for them here. Shutdown runs on the main thread as ordinary code,
  // not inside a signal handler, where almost nothing is safe to call.
  sigset_t sigs;
  sigemptyset(&sigs);
  sigaddset(&sigs, SIGINT);
  sigaddset(&sigs, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &sigs, nullptr);

  try {
    net::KvNode node(cfg);
    node.start();
    int sig = 0;
    sigwait(&sigs, &sig);
    std::fprintf(stderr, "[node %llu] signal %d, shutting down\n",
                 static_cast<unsigned long long>(cfg.id), sig);
    node.stop();
  } catch (const std::exception& e) {
    std::fprintf(stderr, "kv_node: %s\n", e.what());
    return 1;
  }
  return 0;
}
