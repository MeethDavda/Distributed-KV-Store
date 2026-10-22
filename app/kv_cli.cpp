// kv_cli: command-line client for the cluster.
//
//   kv_cli --peers 1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053 \
//          put KEY VALUE | append KEY VALUE | get KEY | del KEY
//
// Uses the same KvClient as the simulator tests (leader hints, retries with
// the same seq, so a retried write is applied once). The only new piece is
// GrpcClientTransport below, which sends KvClient's requests over gRPC.

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "kv.grpc.pb.h"
#include "kv/kv_client.hpp"
#include "net/convert.hpp"

namespace {

using namespace std::chrono_literals;

// Per-attempt timeout and NotLeader backoff. Longer than in the simulator:
// a real write waits for disk syncs on a majority of nodes.
constexpr rafty::Millis kAttemptTimeout{500};
constexpr rafty::Millis kBackoff{50};
// Give up on the whole operation after this long.
constexpr auto kOverallLimit = 10s;

// kv::ClientTransport over gRPC. send() does a blocking call (bounded by the
// attempt timeout) and stores any reply in `inbox`; the main loop then hands
// replies to KvClient::on_response(). No threads needed.
class GrpcClientTransport final : public kv::ClientTransport {
 public:
  explicit GrpcClientTransport(const std::map<rafty::NodeId, std::string>& addrs) {
    for (const auto& [id, addr] : addrs) {
      stubs_[id] = kvpb::KV::NewStub(
          grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
    }
  }

  void send(rafty::NodeId server, const kv::KvRequest& req) override {
    auto it = stubs_.find(server);
    if (it == stubs_.end()) return;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + kAttemptTimeout);
    kvpb::KvResponse resp;
    // On failure (node down, deadline) there is no reply: KvClient's own
    // timeout will move on to the next server.
    if (!it->second->Execute(&ctx, net::to_proto(req), &resp).ok()) return;
    if (auto r = net::from_proto(resp)) inbox.emplace_back(server, *r);
  }

  std::vector<std::pair<rafty::NodeId, kv::KvResponse>> inbox;

 private:
  std::map<rafty::NodeId, std::unique_ptr<kvpb::KV::Stub>> stubs_;
};

[[noreturn]] void usage() {
  std::fprintf(stderr,
               "usage: kv_cli --peers ID=HOST:PORT,... "
               "(put K V | append K V | get K | del K)\n");
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

}  // namespace

int main(int argc, char** argv) {
  if (argc < 5 || std::string(argv[1]) != "--peers") usage();
  const auto addrs = parse_peers(argv[2]);
  const std::string cmd = argv[3];
  const std::string key = argv[4];
  const bool has_value = cmd == "put" || cmd == "append";
  if (has_value && argc != 6) usage();
  if (!has_value && argc != 5) usage();
  const std::string value = has_value ? argv[5] : "";

  std::vector<rafty::NodeId> servers;
  for (const auto& [id, addr] : addrs) servers.push_back(id);

  GrpcClientTransport transport(addrs);
  rafty::SteadyClock clock;
  // A fresh random 64-bit client id per invocation (seq starts at 1), so two
  // CLI runs never share a dedup entry.
  std::random_device rd;
  const uint64_t client_id =
      (static_cast<uint64_t>(rd()) << 32 | rd()) | 1;  // never 0
  kv::KvClient client(client_id, servers, transport, clock, kAttemptTimeout,
                      kBackoff);

  std::optional<kv::KvResponse> result;
  auto done = [&](const kv::KvResponse& r) { result = r; };
  if (cmd == "put") {
    client.put(key, value, done);
  } else if (cmd == "append") {
    client.append(key, value, done);
  } else if (cmd == "get") {
    client.get(key, done);
  } else if (cmd == "del") {
    client.del(key, done);
  } else {
    usage();
  }

  // Drive the client: deliver replies, let it retry, until done or give up.
  const auto give_up = std::chrono::steady_clock::now() + kOverallLimit;
  while (!result && std::chrono::steady_clock::now() < give_up) {
    auto replies = std::exchange(transport.inbox, {});
    for (const auto& [from, r] : replies) client.on_response(from, r);
    client.tick();
    if (!result) std::this_thread::sleep_for(5ms);
  }

  if (!result) {
    std::fprintf(stderr, "error: no answer from the cluster\n");
    return 1;
  }
  if (cmd == "get") {
    std::printf("%s\n", result->found ? result->value.c_str() : "(not found)");
  } else {
    std::printf("OK\n");
  }
  return 0;
}
