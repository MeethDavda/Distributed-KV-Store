#include "kv/kv_client.hpp"

#include <algorithm>
#include <cassert>
#include <utility>

namespace kv {

KvClient::KvClient(uint64_t client_id, std::vector<rafty::NodeId> servers,
                   ClientTransport& transport, const rafty::Clock& clock,
                   rafty::Millis timeout, rafty::Millis backoff)
    : client_id_(client_id),
      servers_(std::move(servers)),
      transport_(transport),
      clock_(clock),
      timeout_(timeout),
      backoff_(backoff),
      target_(servers_.front()) {}

// ===========================================================================
// Operations
// ===========================================================================

void KvClient::get(const std::string& key, Done done) {
  start(Op::Get, key, "", std::move(done));
}
void KvClient::put(const std::string& key, const std::string& value,
                   Done done) {
  start(Op::Put, key, value, std::move(done));
}
void KvClient::append(const std::string& key, const std::string& value,
                      Done done) {
  start(Op::Append, key, value, std::move(done));
}
void KvClient::del(const std::string& key, Done done) {
  start(Op::Delete, key, "", std::move(done));
}

void KvClient::start(Op op, const std::string& key, const std::string& value,
                     Done done) {
  assert(!busy() && "one operation in flight at a time (dedup relies on it)");
  KvRequest r;
  r.op = op;
  r.client_id = client_id_;
  r.seq = ++last_seq_;  // new operation, new seq; retries keep this one
  r.key = key;
  r.value = value;
  current_ = std::move(r);
  done_ = std::move(done);
  send_now();
}

// ===========================================================================
// Replies and time
// ===========================================================================

void KvClient::on_response(rafty::NodeId from, const KvResponse& resp) {
  // A reply to an earlier operation, or to nothing: ignore it.
  if (!current_ || resp.seq != current_->seq) return;

  if (resp.status == Status::Ok) {
    // Clear state BEFORE calling done, so the callback can start the next
    // operation right away.
    Done done = std::move(done_);
    current_.reset();
    done(resp);
    return;
  }

  // NotLeader: follow the hint if it points somewhere new, else rotate.
  const bool useful_hint =
      resp.leader_hint != rafty::kNoNode && resp.leader_hint != from;
  retry_later(useful_hint ? resp.leader_hint : next_server());
}

void KvClient::tick() {
  if (!current_ || clock_.now() < deadline_) return;
  if (waiting_to_retry_) {
    send_now();  // backoff after NotLeader is over
    return;
  }
  // Timed out: the server may be down or the message lost. Same seq, so if
  // the earlier attempt did commit, dedup will skip this copy.
  target_ = next_server();
  send_now();
}

// ===========================================================================
// Helpers
// ===========================================================================

void KvClient::send_now() {
  waiting_to_retry_ = false;
  deadline_ = clock_.now() + timeout_;
  transport_.send(target_, *current_);
}

void KvClient::retry_later(rafty::NodeId next_target) {
  target_ = next_target;
  waiting_to_retry_ = true;
  deadline_ = clock_.now() + backoff_;
}

rafty::NodeId KvClient::next_server() const {
  auto it = std::find(servers_.begin(), servers_.end(), target_);
  if (it == servers_.end() || ++it == servers_.end()) return servers_.front();
  return *it;
}

}  // namespace kv
