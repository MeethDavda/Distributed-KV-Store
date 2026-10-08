#pragma once

// Core Raft types and messages.
//
// These are plain C++ structs on purpose: the Raft core has no dependency on
// protobuf or gRPC. The real network transport (step 4) converts between
// these structs and proto messages at the boundary. This keeps the core
// testable in-process with a simulated network.

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace rafty {

using NodeId = uint64_t;  // 0 is reserved for "none"
using Term = uint64_t;    // starts at 0, only ever increases (term monotonicity)
using Index = uint64_t;   // log indices start at 1; 0 means "empty log"

inline constexpr NodeId kNoNode = 0;

enum class Role { Follower, PreCandidate, Candidate, Leader };

// ---------------------------------------------------------------------------
// Log entries
//
// An entry is identified by (index, term). Log Matching says two entries with
// the same index and term hold the same data, so (index, term) is all that
// is ever compared during replication.
// ---------------------------------------------------------------------------
enum class EntryType : uint8_t {
  Normal,  // client command; `data` is opaque to Raft (KV command in step 4)
  NoOp,    // appended by every new leader so it can commit in its own term
};

struct LogEntry {
  Term term = 0;
  Index index = 0;
  EntryType type = EntryType::Normal;
  std::string data{};  // opaque bytes

  bool operator==(const LogEntry&) const = default;
};

// ---------------------------------------------------------------------------
// RequestVote (also used for pre-vote)
//
// Pre-vote (Raft thesis 9.6): before starting a real election, a node asks
// "would you vote for me at term + 1?" without anyone changing state. Only if
// a majority says yes does it increment its term and run a real election.
// `term` in a pre-vote request is the term the candidate WOULD use; the
// receiver does not adopt it.
// ---------------------------------------------------------------------------
struct RequestVoteReq {
  Term term = 0;
  NodeId candidate_id = kNoNode;
  Index last_log_index = 0;  // used for the "log at least as up to date" check
  Term last_log_term = 0;
  bool pre_vote = false;
};

struct RequestVoteResp {
  Term term = 0;  // responder's current term (or the echoed pre-vote term)
  bool vote_granted = false;
  bool pre_vote = false;
};

// ---------------------------------------------------------------------------
// AppendEntries
//
// Replicates log entries and doubles as the heartbeat (entries empty).
//
// Consistency check (Log Matching): the follower accepts only if its log has
// an entry at prev_log_index with term prev_log_term. prev_log_index = 0
// always matches (the empty prefix).
// ---------------------------------------------------------------------------
struct AppendEntriesReq {
  Term term = 0;
  NodeId leader_id = kNoNode;
  Index prev_log_index = 0;
  Term prev_log_term = 0;
  // `{}` gives every field a default initializer, so partial brace-init like
  // AppendEntriesReq{term, leader} (a heartbeat) is warning-free.
  std::vector<LogEntry> entries{};  // indices prev_log_index+1, +2, ...
  Index leader_commit = 0;
};

struct AppendEntriesResp {
  Term term = 0;
  bool success = false;

  // On success: the highest index the follower now has that matches the
  // leader (prev_log_index + entries.size()). An absolute value, so the
  // leader can apply replies in any order: match_index = max(old, this).
  Index match_index = 0;

  // On rejection: which prev_log_index was rejected (lets the leader ignore
  // stale rejections), and the follower's last log index (lets the leader
  // jump next_index back in one step instead of one entry per round trip).
  Index rejected_index = 0;
  Index last_log_index = 0;
};

// Envelope used by Transport. `from`/`to` live here, not in every payload.
using Payload = std::variant<RequestVoteReq, RequestVoteResp, AppendEntriesReq,
                             AppendEntriesResp>;

struct Message {
  NodeId from = kNoNode;
  NodeId to = kNoNode;
  Payload payload;
};

}  // namespace rafty
