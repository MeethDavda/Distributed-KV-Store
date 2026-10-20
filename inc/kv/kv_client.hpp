#pragma once

// KvClient: issues operations to the cluster and retries until they succeed.
//
// Rules that make retries safe (exactly-once, together with server dedup):
//   - One operation in flight at a time.
//   - Each new operation gets the next seq (1, 2, 3, ...). Every retry of
//     that operation reuses the same seq, so the state machine applies it at
//     most once no matter how many copies get committed.
//
// Retry policy:
//   - NotLeader: switch to the leader hint (or the next server) and resend
//     after a short backoff, so an election in progress is not spammed.
//   - No reply before the timeout (crashed node, lost message): switch to
//     the next server and resend.
//
// Same style as Raft and KvServer: single-threaded, no I/O, no sleeping. The
// driver calls on_response() for each reply and tick() as time passes. The
// network is behind ClientTransport (simulated now, gRPC in step 4d).

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "kv/kv_server.hpp"
#include "rafty/interfaces.hpp"

namespace kv {

class ClientTransport {
 public:
  virtual ~ClientTransport() = default;
  // Fire-and-forget; the reply (if any) comes back via on_response().
  virtual void send(rafty::NodeId server, const KvRequest& req) = 0;
};

class KvClient {
 public:
  // Called once per operation, with the final Ok response.
  using Done = std::function<void(const KvResponse&)>;

  KvClient(uint64_t client_id, std::vector<rafty::NodeId> servers,
           ClientTransport& transport, const rafty::Clock& clock,
           rafty::Millis timeout = rafty::Millis{100},
           rafty::Millis backoff = rafty::Millis{10});

  // Start an operation. Precondition: !busy().
  void get(const std::string& key, Done done);
  void put(const std::string& key, const std::string& value, Done done);
  void append(const std::string& key, const std::string& value, Done done);
  void del(const std::string& key, Done done);

  bool busy() const { return current_.has_value(); }
  uint64_t id() const { return client_id_; }

  // Driver entry points.
  void on_response(rafty::NodeId from, const KvResponse& resp);
  void tick();

 private:
  void start(Op op, const std::string& key, const std::string& value,
             Done done);
  void send_now();
  void retry_later(rafty::NodeId next_target);
  rafty::NodeId next_server() const;

  uint64_t client_id_;
  std::vector<rafty::NodeId> servers_;
  ClientTransport& transport_;
  const rafty::Clock& clock_;
  rafty::Millis timeout_;
  rafty::Millis backoff_;

  uint64_t last_seq_ = 0;
  std::optional<KvRequest> current_;  // the operation in flight
  Done done_;
  rafty::NodeId target_;              // server we are talking to
  rafty::Millis deadline_{0};         // resend if no reply by then
  bool waiting_to_retry_ = false;     // true during NotLeader backoff
};

}  // namespace kv
