#pragma once

// Core Raft types and messages.
//
// These are plain C++ structs on purpose: the Raft core has no dependency on
// protobuf or gRPC. The real network transport (step 4) converts between
// these structs and proto messages at the boundary. This keeps the core
// testable in-process with a simulated network.

#include <cstdint>
#include <variant>

namespace rafty {

using NodeId = uint64_t;  // 0 is reserved for "none"
using Term = uint64_t;    // starts at 0, only ever increases (term monotonicity)
using Index = uint64_t;   // log indices start at 1; 0 means "empty log"

inline constexpr NodeId kNoNode = 0;

enum class Role { Follower, PreCandidate, Candidate, Leader };

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
// In step 1 this is only a heartbeat: it asserts leadership and resets
// followers' election timers. Log fields (prev_log_index, entries,
// leader_commit) are added in step 2.
// ---------------------------------------------------------------------------
struct AppendEntriesReq {
  Term term = 0;
  NodeId leader_id = kNoNode;
};

struct AppendEntriesResp {
  Term term = 0;
  bool success = false;
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
