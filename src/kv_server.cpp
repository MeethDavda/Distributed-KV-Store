#include "kv/kv_server.hpp"

#include <utility>

namespace kv {

namespace {

OpType to_op_type(Op op) {
  switch (op) {
    case Op::Put: return OpType::Put;
    case Op::Append: return OpType::Append;
    case Op::Delete: return OpType::Delete;
    case Op::Get: break;  // reads never go through the log
  }
  return OpType::Put;  // unreachable for writes
}

}  // namespace

// ===========================================================================
// Requests in
// ===========================================================================

void KvServer::handle(const KvRequest& req, ReplyFn reply) {
  if (raft_.role() != rafty::Role::Leader) {
    reply(not_leader(req.seq));
    return;
  }

  if (req.op == Op::Get) {
    // ReadIndex: no log entry, just a leadership check (K5).
    const uint64_t id = next_read_id_++;
    if (!raft_.read_index(id)) {
      reply(not_leader(req.seq));
      return;
    }
    pending_reads_[id] = PendingRead{req.seq, req.key, std::move(reply)};
    return;
  }

  Command c;
  c.op = to_op_type(req.op);
  c.client_id = req.client_id;
  c.seq = req.seq;
  c.key = req.key;
  c.value = req.value;
  const auto index = raft_.propose(encode(c));
  if (!index) {
    reply(not_leader(req.seq));
    return;
  }

  // Remember the term we proposed in: K4 checks it when index is applied.
  // An older request parked at the same index belonged to an entry that was
  // overwritten, so it must retry.
  auto old = pending_writes_.find(*index);
  if (old != pending_writes_.end()) {
    old->second.reply(not_leader(old->second.seq));
    pending_writes_.erase(old);
  }
  pending_writes_[*index] = PendingWrite{raft_.term(), req.seq, std::move(reply)};
}

// ===========================================================================
// Driving: called after every Raft tick/step
// ===========================================================================

std::vector<rafty::LogEntry> KvServer::process() {
  std::vector<rafty::LogEntry> applied;
  // Order matters: apply first, so writes that DID commit are answered Ok
  // even if we lost leadership in the same moment.
  apply_committed(applied);
  collect_reads();
  answer_ready_reads();
  if (raft_.role() != rafty::Role::Leader) fail_everything();
  return applied;
}

void KvServer::apply_committed(std::vector<rafty::LogEntry>& out) {
  for (rafty::LogEntry& e : raft_.take_committed()) {
    sm_.apply(e);

    auto it = pending_writes_.find(e.index);
    if (it != pending_writes_.end()) {
      // K4: same term => this is the entry we proposed. A different term
      // means a new leader put a different entry here; ours is gone.
      KvResponse r = not_leader(it->second.seq);
      if (e.term == it->second.term) r.status = Status::Ok;
      it->second.reply(r);
      pending_writes_.erase(it);
    }
    out.push_back(std::move(e));
  }
}

void KvServer::collect_reads() {
  for (const auto& rs : raft_.take_reads()) {
    auto it = pending_reads_.find(rs.id);
    if (it == pending_reads_.end()) continue;
    if (!rs.ok) {  // K6: leadership lost before the read was confirmed
      it->second.reply(not_leader(it->second.seq));
      pending_reads_.erase(it);
      continue;
    }
    it->second.confirmed = true;
    it->second.index = rs.index;
  }
}

void KvServer::answer_ready_reads() {
  // K5 step 4: serve only once the state machine includes everything that
  // was committed when the read arrived.
  for (auto it = pending_reads_.begin(); it != pending_reads_.end();) {
    const PendingRead& r = it->second;
    if (!r.confirmed || sm_.last_applied() < r.index) {
      ++it;
      continue;
    }
    KvResponse resp;
    resp.status = Status::Ok;
    resp.seq = r.seq;
    if (auto v = sm_.get(r.key)) {
      resp.found = true;
      resp.value = *v;
    }
    r.reply(resp);
    it = pending_reads_.erase(it);
  }
}

void KvServer::fail_everything() {
  // Move the maps out first, so a reply callback can never see (or modify)
  // a half-cleared map.
  auto writes = std::exchange(pending_writes_, {});
  auto reads = std::exchange(pending_reads_, {});
  for (auto& [idx, w] : writes) w.reply(not_leader(w.seq));
  for (auto& [id, r] : reads) r.reply(not_leader(r.seq));
}

KvResponse KvServer::not_leader(uint64_t seq) const {
  KvResponse r;
  r.status = Status::NotLeader;
  r.seq = seq;
  r.leader_hint = raft_.leader_id();
  return r;
}

}  // namespace kv
