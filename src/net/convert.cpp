#include "net/convert.hpp"

#include <variant>

namespace net {

namespace {

template <class... Fs>
struct Overloaded : Fs... {
  using Fs::operator()...;
};
template <class... Fs>
Overloaded(Fs...) -> Overloaded<Fs...>;

// ---- Log entries ----

void fill(raftpb::LogEntry* p, const rafty::LogEntry& e) {
  p->set_term(e.term);
  p->set_index(e.index);
  p->set_type(e.type == rafty::EntryType::NoOp ? raftpb::ENTRY_NOOP
                                               : raftpb::ENTRY_NORMAL);
  p->set_data(e.data);
}

std::optional<rafty::LogEntry> entry_from(const raftpb::LogEntry& p) {
  rafty::LogEntry e;
  e.term = p.term();
  e.index = p.index();
  switch (p.type()) {
    case raftpb::ENTRY_NORMAL: e.type = rafty::EntryType::Normal; break;
    case raftpb::ENTRY_NOOP: e.type = rafty::EntryType::NoOp; break;
    default: return std::nullopt;  // unknown type: refuse, never guess
  }
  e.data = p.data();
  return e;
}

}  // namespace

// ===========================================================================
// Raft messages
// ===========================================================================

raftpb::RaftMessage to_proto(const rafty::Message& m) {
  raftpb::RaftMessage p;
  p.set_from(m.from);
  p.set_to(m.to);
  std::visit(
      Overloaded{
          [&](const rafty::RequestVoteReq& r) {
            auto* q = p.mutable_request_vote();
            q->set_term(r.term);
            q->set_candidate_id(r.candidate_id);
            q->set_last_log_index(r.last_log_index);
            q->set_last_log_term(r.last_log_term);
            q->set_pre_vote(r.pre_vote);
          },
          [&](const rafty::RequestVoteResp& r) {
            auto* q = p.mutable_request_vote_resp();
            q->set_term(r.term);
            q->set_vote_granted(r.vote_granted);
            q->set_pre_vote(r.pre_vote);
          },
          [&](const rafty::AppendEntriesReq& r) {
            auto* q = p.mutable_append_entries();
            q->set_term(r.term);
            q->set_leader_id(r.leader_id);
            q->set_prev_log_index(r.prev_log_index);
            q->set_prev_log_term(r.prev_log_term);
            for (const auto& e : r.entries) fill(q->add_entries(), e);
            q->set_leader_commit(r.leader_commit);
            q->set_read_round(r.read_round);
          },
          [&](const rafty::AppendEntriesResp& r) {
            auto* q = p.mutable_append_entries_resp();
            q->set_term(r.term);
            q->set_success(r.success);
            q->set_match_index(r.match_index);
            q->set_rejected_index(r.rejected_index);
            q->set_last_log_index(r.last_log_index);
            q->set_read_round(r.read_round);
          },
          [&](const rafty::InstallSnapshotReq& r) {
            auto* q = p.mutable_install_snapshot();
            q->set_term(r.term);
            q->set_leader_id(r.leader_id);
            q->set_snapshot_index(r.snapshot.index);
            q->set_snapshot_term(r.snapshot.term);
            q->set_snapshot_data(r.snapshot.data);
          },
          [&](const rafty::InstallSnapshotResp& r) {
            auto* q = p.mutable_install_snapshot_resp();
            q->set_term(r.term);
            q->set_match_index(r.match_index);
          },
      },
      m.payload);
  return p;
}

std::optional<rafty::Message> from_proto(const raftpb::RaftMessage& p) {
  rafty::Message m;
  m.from = p.from();
  m.to = p.to();

  switch (p.payload_case()) {
    case raftpb::RaftMessage::kRequestVote: {
      const auto& q = p.request_vote();
      m.payload = rafty::RequestVoteReq{q.term(), q.candidate_id(),
                                        q.last_log_index(), q.last_log_term(),
                                        q.pre_vote()};
      return m;
    }
    case raftpb::RaftMessage::kRequestVoteResp: {
      const auto& q = p.request_vote_resp();
      m.payload = rafty::RequestVoteResp{q.term(), q.vote_granted(),
                                         q.pre_vote()};
      return m;
    }
    case raftpb::RaftMessage::kAppendEntries: {
      const auto& q = p.append_entries();
      rafty::AppendEntriesReq r;
      r.term = q.term();
      r.leader_id = q.leader_id();
      r.prev_log_index = q.prev_log_index();
      r.prev_log_term = q.prev_log_term();
      r.entries.reserve(static_cast<std::size_t>(q.entries_size()));
      for (const auto& pe : q.entries()) {
        auto e = entry_from(pe);
        if (!e) return std::nullopt;
        r.entries.push_back(std::move(*e));
      }
      r.leader_commit = q.leader_commit();
      r.read_round = q.read_round();
      m.payload = std::move(r);
      return m;
    }
    case raftpb::RaftMessage::kAppendEntriesResp: {
      const auto& q = p.append_entries_resp();
      rafty::AppendEntriesResp r;
      r.term = q.term();
      r.success = q.success();
      r.match_index = q.match_index();
      r.rejected_index = q.rejected_index();
      r.last_log_index = q.last_log_index();
      r.read_round = q.read_round();
      m.payload = r;
      return m;
    }
    case raftpb::RaftMessage::kInstallSnapshot: {
      const auto& q = p.install_snapshot();
      m.payload = rafty::InstallSnapshotReq{
          q.term(), q.leader_id(),
          rafty::Snapshot{q.snapshot_index(), q.snapshot_term(),
                          q.snapshot_data()}};
      return m;
    }
    case raftpb::RaftMessage::kInstallSnapshotResp: {
      const auto& q = p.install_snapshot_resp();
      m.payload = rafty::InstallSnapshotResp{q.term(), q.match_index()};
      return m;
    }
    default:
      return std::nullopt;  // no payload set
  }
}

// ===========================================================================
// KV requests and responses
// ===========================================================================

kvpb::KvRequest to_proto(const kv::KvRequest& r) {
  kvpb::KvRequest p;
  switch (r.op) {
    case kv::Op::Get: p.set_op(kvpb::OP_GET); break;
    case kv::Op::Put: p.set_op(kvpb::OP_PUT); break;
    case kv::Op::Append: p.set_op(kvpb::OP_APPEND); break;
    case kv::Op::Delete: p.set_op(kvpb::OP_DELETE); break;
  }
  p.set_client_id(r.client_id);
  p.set_seq(r.seq);
  p.set_key(r.key);
  p.set_value(r.value);
  return p;
}

std::optional<kv::KvRequest> from_proto(const kvpb::KvRequest& p) {
  kv::KvRequest r;
  switch (p.op()) {
    case kvpb::OP_GET: r.op = kv::Op::Get; break;
    case kvpb::OP_PUT: r.op = kv::Op::Put; break;
    case kvpb::OP_APPEND: r.op = kv::Op::Append; break;
    case kvpb::OP_DELETE: r.op = kv::Op::Delete; break;
    default: return std::nullopt;
  }
  r.client_id = p.client_id();
  r.seq = p.seq();
  r.key = p.key();
  r.value = p.value();
  return r;
}

kvpb::KvResponse to_proto(const kv::KvResponse& r) {
  kvpb::KvResponse p;
  p.set_status(r.status == kv::Status::Ok ? kvpb::STATUS_OK
                                          : kvpb::STATUS_NOT_LEADER);
  p.set_seq(r.seq);
  p.set_found(r.found);
  p.set_value(r.value);
  p.set_leader_hint(r.leader_hint);
  return p;
}

std::optional<kv::KvResponse> from_proto(const kvpb::KvResponse& p) {
  kv::KvResponse r;
  switch (p.status()) {
    case kvpb::STATUS_OK: r.status = kv::Status::Ok; break;
    case kvpb::STATUS_NOT_LEADER: r.status = kv::Status::NotLeader; break;
    default: return std::nullopt;
  }
  r.seq = p.seq();
  r.found = p.found();
  r.value = p.value();
  r.leader_hint = p.leader_hint();
  return r;
}

}  // namespace net
