#pragma once

// KvSim: the full stack (KvClient -> KvServer -> Raft -> StateMachine) in one
// process, on top of SimCluster.
//
//   - One KvServer per live node, fed through SimCluster's drain hook, so
//     every Raft safety check keeps running.
//   - k KvClients (client ids 1..k) talking to servers through a simulated
//     client network: requests and replies are queued and delivered on the
//     next step, and each can be dropped with a set probability.
//
// Crash and restart nodes through KvSim (not cluster()), so the node's
// KvServer is thrown away with it: its parked requests vanish, exactly like
// a real process dying, and clients must time out and retry.

#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <utility>
#include <vector>

#include "kv/kv_client.hpp"
#include "kv/kv_server.hpp"
#include "toolings/sim_cluster.hpp"

namespace toolings {

class KvSim final : public kv::ClientTransport {
 public:
  KvSim(int nodes, uint64_t seed, int clients,
        std::optional<std::filesystem::path> data_dir = std::nullopt)
      : cluster_(nodes, seed, {}, std::move(data_dir)),
        rng_(seed ^ 0xC11E47ULL) {
    cluster_.set_drain([this](NodeId id, Raft&) {
      return server(id).process();
    });
    for (int c = 1; c <= clients; ++c) {
      clients_.push_back(std::make_unique<kv::KvClient>(
          static_cast<uint64_t>(c), cluster_.ids(), *this, cluster_.clock()));
    }
  }

  // ---- Driving time ----

  // One step: deliver queued client requests and replies, step the cluster
  // (which runs every KvServer::process via the drain), tick the clients.
  void step() {
    deliver_requests();
    deliver_replies();
    cluster_.step();
    for (auto& c : clients_) c->tick();
  }

  void run_for(Millis d) {
    const Millis end = cluster_.clock().now() + d;
    while (cluster_.clock().now() < end) step();
  }

  bool run_until(const std::function<bool()>& pred, Millis timeout) {
    const Millis end = cluster_.clock().now() + timeout;
    while (cluster_.clock().now() < end) {
      if (pred()) return true;
      step();
    }
    return pred();
  }

  // Runs one operation on client `c` (1-based) to completion. `start` is
  // given the client and the Done callback. Returns the final response, or
  // nullopt if it did not finish within `timeout`.
  std::optional<kv::KvResponse> run_op(
      int c, const std::function<void(kv::KvClient&, kv::KvClient::Done)>& start,
      Millis timeout = Millis{5000}) {
    // shared_ptr, not a local: if we time out, the client still holds the
    // callback and may call it later, after this function has returned.
    auto result = std::make_shared<std::optional<kv::KvResponse>>();
    start(client(c), [result](const kv::KvResponse& r) { *result = r; });
    run_until([&] { return result->has_value(); }, timeout);
    return *result;
  }

  // ---- Faults ----

  void crash(NodeId id) {
    cluster_.crash(id);
    servers_.erase(id);  // parked requests die with the process
  }
  void crash_with_torn_tail(NodeId id) {
    cluster_.crash_with_torn_tail(id);
    servers_.erase(id);
  }
  void restart(NodeId id) { cluster_.restart(id); }  // server made lazily

  // Probability that a client request / server reply is lost in transit.
  void set_request_drop_rate(double p) { request_drop_ = p; }
  void set_reply_drop_rate(double p) { reply_drop_ = p; }

  // ---- Access ----

  kv::KvClient& client(int c) { return *clients_.at(static_cast<std::size_t>(c - 1)); }
  int num_clients() const { return static_cast<int>(clients_.size()); }
  SimCluster& cluster() { return cluster_; }

  // A live node's server, for tests that must talk to one specific node
  // (clients follow leader hints, so they cannot be aimed at a stale leader).
  kv::KvServer* live_server(NodeId id) {
    return cluster_.mutable_node(id) ? &server(id) : nullptr;
  }

  // The KV state on a live node (nullptr if crashed or not started yet).
  const kv::StateMachine* state(NodeId id) const {
    auto it = servers_.find(id);
    return it == servers_.end() ? nullptr : &it->second->state();
  }

  // ---- kv::ClientTransport (called by clients) ----

  void send(NodeId server, const kv::KvRequest& req) override {
    requests_.push_back({server, req});
  }

 private:
  struct QueuedRequest {
    NodeId server;
    kv::KvRequest req;
  };
  struct QueuedReply {
    NodeId server;
    uint64_t client_id;
    kv::KvResponse resp;
  };

  // The node's KvServer, created on first use after (re)start. Its state
  // machine starts empty and is rebuilt as committed entries are re-applied.
  kv::KvServer& server(NodeId id) {
    auto& s = servers_[id];
    if (!s) s = std::make_unique<kv::KvServer>(*cluster_.mutable_node(id));
    return *s;
  }

  bool lost(double p) {
    return p > 0 && std::uniform_real_distribution<double>(0, 1)(rng_) < p;
  }

  void deliver_requests() {
    auto batch = std::exchange(requests_, {});
    for (auto& q : batch) {
      if (lost(request_drop_)) continue;
      if (!cluster_.mutable_node(q.server)) continue;  // node is down
      const uint64_t client_id = q.req.client_id;
      const NodeId from = q.server;
      // Replies are queued, never delivered inside handle()/process(), so a
      // client callback can never re-enter a KvServer mid-update.
      server(q.server).handle(q.req, [this, client_id, from](const kv::KvResponse& r) {
        replies_.push_back({from, client_id, r});
      });
    }
  }

  void deliver_replies() {
    auto batch = std::exchange(replies_, {});
    for (auto& q : batch) {
      if (lost(reply_drop_)) continue;
      client(static_cast<int>(q.client_id)).on_response(q.server, q.resp);
    }
  }

  SimCluster cluster_;  // first: servers and clients refer into it
  std::mt19937_64 rng_;
  std::map<NodeId, std::unique_ptr<kv::KvServer>> servers_;
  std::vector<std::unique_ptr<kv::KvClient>> clients_;
  std::vector<QueuedRequest> requests_;
  std::vector<QueuedReply> replies_;
  double request_drop_ = 0.0;
  double reply_drop_ = 0.0;
};

}  // namespace toolings
