#pragma once

// Durable state for a Raft node.
//
// Raft splits node state into two kinds:
//   - Persistent ("hard") state: currentTerm, votedFor, and the log. Must
//     survive crashes, otherwise safety breaks (see HardState below).
//   - Volatile state: role, commitIndex, who voted for us, timers. Rebuilt
//     from scratch after a restart.
//
// raft.cpp only sees the Storage interface. Step 1 uses MemStorage (below).
// Step 3 adds a WAL-backed implementation in storage.cpp with CRCs and fsync.
// Step 2 extends the interface with log entry methods.

#include "rafty/types.hpp"

namespace rafty {

// Persistent state needed for elections.
//
// Invariant protected (Election Safety): a node grants at most one vote per
// term. If votedFor were lost in a crash, a node could restart and vote for a
// second candidate in the same term, letting two leaders win that term.
// currentTerm must persist for the same reason: forgetting it would let the
// node accept stale messages and vote in terms it already left behind.
struct HardState {
  Term current_term = 0;
  NodeId voted_for = kNoNode;  // kNoNode = has not voted in current_term

  bool operator==(const HardState&) const = default;
};

class Storage {
 public:
  virtual ~Storage() = default;

  // Contract: when save_hard_state() returns, the state is durable. Raft
  // relies on this to send a vote reply only AFTER the vote is recorded.
  virtual void save_hard_state(const HardState& hs) = 0;

  // Returns the last saved state, or a default HardState for a fresh node.
  virtual HardState load_hard_state() const = 0;
};

// In-memory Storage for tests.
//
// To simulate a crash, a test destroys the Raft object but keeps its
// MemStorage alive, then builds a new Raft on the same MemStorage. That models
// "process died, disk survived". Durability is trivially true here; real
// fsync behaviour is tested against the WAL implementation in step 3.
class MemStorage final : public Storage {
 public:
  void save_hard_state(const HardState& hs) override {
    hard_state_ = hs;
    ++save_count_;
  }

  HardState load_hard_state() const override { return hard_state_; }

  // Test hook: lets unit tests assert that a vote was persisted before the
  // reply was sent (save count increases before the message appears).
  int save_count() const { return save_count_; }

 private:
  HardState hard_state_;
  int save_count_ = 0;
};

}  // namespace rafty
