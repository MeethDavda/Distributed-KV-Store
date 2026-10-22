#pragma once

// KvServer: serves client requests on top of one Raft node.
//
//   write (Put/Append/Delete): propose to Raft, park the request under its
//       log index, answer once that index is applied.
//   read (Get): ReadIndex. Park the request under a read id, answer once
//       Raft confirms the read and the state machine has caught up.
//
// Like Raft itself, KvServer is single-threaded and does no I/O. The driver
// (simulator now, gRPC node in step 4d) calls handle() for each request and
// process() after every Raft tick/step. Replies go out through callbacks.
//
// K4 (reply only after apply, at the right index): a parked write for index
// N is answered Ok only if the entry applied at N has the term we proposed
// it in. A different term means leadership changed and our entry was
// overwritten, so the client is told to retry. Telling a client to retry is
// always safe: the retry carries the same (client_id, seq), so dedup makes
// sure the write takes effect at most once.

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "kv/state_machine.hpp"
#include "rafty/raft.hpp"

namespace kv {

enum class Op : uint8_t { Get, Put, Append, Delete };

struct KvRequest {
  Op op = Op::Get;
  uint64_t client_id = 0;
  uint64_t seq = 0;  // identifies this operation; retries reuse it
  std::string key;
  std::string value;  // Put/Append only
};

enum class Status : uint8_t {
  Ok,         // done (Get: `value` holds the result, `found` says if it existed)
  NotLeader,  // this node cannot serve it; try `leader_hint` or another node
};

struct KvResponse {
  Status status = Status::NotLeader;
  uint64_t seq = 0;  // echoes the request, so clients can drop stale replies
  bool found = false;
  std::string value;
  rafty::NodeId leader_hint = rafty::kNoNode;
};

using ReplyFn = std::function<void(const KvResponse&)>;

class KvServer {
 public:
  // Takes a snapshot (log compaction) every `snapshot_every` applied entries.
  explicit KvServer(rafty::Raft& raft, rafty::Index snapshot_every = 1000)
      : raft_(raft), snapshot_every_(snapshot_every) {}

  // Accepts one client request. The reply is sent later (from process()),
  // or immediately if this node is not the leader.
  void handle(const KvRequest& req, ReplyFn reply);

  // Drains Raft: applies committed entries, answers parked writes and reads.
  // Returns the entries applied in this call (the simulator checks them).
  std::vector<rafty::LogEntry> process();

  const StateMachine& state() const { return sm_; }
  const rafty::Raft& raft() const { return raft_; }

 private:
  struct PendingWrite {
    rafty::Term term = 0;  // term we proposed in; must match at apply (K4)
    uint64_t seq = 0;
    ReplyFn reply;
  };
  struct PendingRead {
    uint64_t seq = 0;
    std::string key;
    ReplyFn reply;
    rafty::Index index = 0;  // set once Raft confirms the read
    bool confirmed = false;
  };

  void apply_committed(std::vector<rafty::LogEntry>& out);
  void collect_reads();
  void answer_ready_reads();
  void fail_everything();  // lost leadership: every parked request retries
  KvResponse not_leader(uint64_t seq) const;

  rafty::Raft& raft_;
  rafty::Index snapshot_every_;
  rafty::Index last_snapshot_ = 0;
  StateMachine sm_;
  std::map<rafty::Index, PendingWrite> pending_writes_;  // by log index
  std::map<uint64_t, PendingRead> pending_reads_;        // by read id
  uint64_t next_read_id_ = 1;
};

}  // namespace kv
