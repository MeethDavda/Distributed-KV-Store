#pragma once

// kv::ClientTransport over gRPC, shared by kv_cli and bench.
//
// send() does a blocking unary call (bounded by `timeout`) and stores any
// reply in `inbox`; the caller's loop then hands replies to
// KvClient::on_response(). Not thread-safe: use one per client thread.

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kv.grpc.pb.h"
#include "kv/kv_client.hpp"
#include "net/convert.hpp"

namespace net {

class GrpcClientTransport final : public kv::ClientTransport {
 public:
  GrpcClientTransport(const std::map<rafty::NodeId, std::string>& addrs,
                      rafty::Millis timeout)
      : timeout_(timeout) {
    for (const auto& [id, addr] : addrs) {
      stubs_[id] = kvpb::KV::NewStub(
          grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
    }
  }

  void send(rafty::NodeId server, const kv::KvRequest& req) override {
    auto it = stubs_.find(server);
    if (it == stubs_.end()) return;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + timeout_);
    kvpb::KvResponse resp;
    // On failure (node down, deadline) there is no reply: KvClient's own
    // timeout moves on to the next server.
    if (!it->second->Execute(&ctx, to_proto(req), &resp).ok()) return;
    if (auto r = from_proto(resp)) inbox.emplace_back(server, *r);
  }

  // Delivers queued replies to `client` and lets it retry if needed.
  void pump(kv::KvClient& client) {
    auto replies = std::exchange(inbox, {});
    for (const auto& [from, r] : replies) client.on_response(from, r);
    client.tick();
  }

  std::vector<std::pair<rafty::NodeId, kv::KvResponse>> inbox;

 private:
  rafty::Millis timeout_;
  std::map<rafty::NodeId, std::unique_ptr<kvpb::KV::Stub>> stubs_;
};

}  // namespace net
