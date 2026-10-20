// Unit tests for the KV state machine and its dedup table.
//
// Entries are built by hand and applied in order, exactly as Raft's
// take_committed() would hand them over.

#include <gtest/gtest.h>

#include <random>
#include <string>
#include <vector>

#include "kv/state_machine.hpp"

namespace {

using kv::Command;
using kv::OpType;
using kv::StateMachine;
using rafty::EntryType;
using rafty::Index;
using rafty::LogEntry;

Command cmd(OpType op, uint64_t client, uint64_t seq, std::string key,
            std::string value = "") {
  Command c;
  c.op = op;
  c.client_id = client;
  c.seq = seq;
  c.key = std::move(key);
  c.value = std::move(value);
  return c;
}

// Appends entries to a state machine with consecutive indices.
class StateMachineTest : public ::testing::Test {
 protected:
  kv::ApplyResult apply(const Command& c) {
    LogEntry e;
    e.term = 1;
    e.index = next_index_++;
    e.data = kv::encode(c);
    return sm.apply(e);
  }

  kv::ApplyResult apply_noop() {
    LogEntry e;
    e.term = 1;
    e.index = next_index_++;
    e.type = EntryType::NoOp;
    return sm.apply(e);
  }

  StateMachine sm;

 private:
  Index next_index_ = 1;
};

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

TEST(Codec, RoundTripsAllFieldsIncludingBinary) {
  const Command c = cmd(OpType::Append, 7, 42, std::string("k\0y", 3),
                        std::string("v\0\xff", 3));
  auto back = kv::decode(kv::encode(c));
  ASSERT_TRUE(back.has_value());
  EXPECT_EQ(*back, c);
}

TEST(Codec, RejectsTruncatedInputAtEveryLength) {
  const std::string bytes = kv::encode(cmd(OpType::Put, 1, 1, "key", "value"));
  for (std::size_t n = 0; n < bytes.size(); ++n) {
    EXPECT_FALSE(kv::decode(bytes.substr(0, n)).has_value()) << "length " << n;
  }
}

TEST(Codec, RejectsTrailingBytesAndBadOp) {
  std::string bytes = kv::encode(cmd(OpType::Put, 1, 1, "k", "v"));
  EXPECT_FALSE(kv::decode(bytes + "x").has_value());
  bytes[0] = 9;  // not a valid OpType
  EXPECT_FALSE(kv::decode(bytes).has_value());
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

TEST_F(StateMachineTest, PutAppendDelete) {
  apply(cmd(OpType::Put, 1, 1, "x", "a"));
  EXPECT_EQ(sm.get("x"), "a");
  apply(cmd(OpType::Append, 1, 2, "x", "b"));
  EXPECT_EQ(sm.get("x"), "ab");
  apply(cmd(OpType::Delete, 1, 3, "x"));
  EXPECT_FALSE(sm.get("x").has_value());
}

TEST_F(StateMachineTest, AppendToMissingKeyStartsEmpty) {
  apply(cmd(OpType::Append, 1, 1, "y", "z"));
  EXPECT_EQ(sm.get("y"), "z");
}

TEST_F(StateMachineTest, NoOpChangesNothingButAdvancesIndex) {
  auto r = apply_noop();
  EXPECT_FALSE(r.is_command);
  EXPECT_EQ(sm.last_applied(), 1u);
  EXPECT_TRUE(sm.data().empty());
}

TEST_F(StateMachineTest, ResultIdentifiesTheEntry) {
  auto r = apply(cmd(OpType::Put, 5, 1, "k", "v"));
  EXPECT_EQ(r.index, 1u);
  EXPECT_EQ(r.term, 1u);
  EXPECT_TRUE(r.is_command);
  EXPECT_EQ(r.command.client_id, 5u);
  EXPECT_FALSE(r.duplicate);
}

// ---------------------------------------------------------------------------
// Dedup (K2): exactly-once writes
// ---------------------------------------------------------------------------

TEST_F(StateMachineTest, RetriedWriteIsAppliedOnce) {
  // The client's first attempt committed, but the reply was lost, so it
  // retried with the same seq and that copy committed too.
  apply(cmd(OpType::Append, 1, 1, "x", "a"));
  auto retry = apply(cmd(OpType::Append, 1, 1, "x", "a"));
  EXPECT_TRUE(retry.duplicate);
  EXPECT_EQ(sm.get("x"), "a");  // not "aa"
}

TEST_F(StateMachineTest, LateOldCopyCannotUndoNewerWrite) {
  apply(cmd(OpType::Put, 1, 1, "x", "1"));
  apply(cmd(OpType::Put, 1, 2, "x", "2"));
  auto late = apply(cmd(OpType::Put, 1, 1, "x", "1"));  // delayed retry
  EXPECT_TRUE(late.duplicate);
  EXPECT_EQ(sm.get("x"), "2");
}

TEST_F(StateMachineTest, ClientsAreDedupedIndependently) {
  apply(cmd(OpType::Append, 1, 1, "x", "a"));
  auto other = apply(cmd(OpType::Append, 2, 1, "x", "b"));  // same seq, new client
  EXPECT_FALSE(other.duplicate);
  EXPECT_EQ(sm.get("x"), "ab");
  EXPECT_EQ(sm.last_seq().at(1), 1u);
  EXPECT_EQ(sm.last_seq().at(2), 1u);
}

TEST_F(StateMachineTest, DuplicateStillAdvancesLastApplied) {
  // A skipped duplicate is still a committed entry that has been processed.
  apply(cmd(OpType::Put, 1, 1, "x", "a"));
  apply(cmd(OpType::Put, 1, 1, "x", "a"));
  EXPECT_EQ(sm.last_applied(), 2u);
}

// ---------------------------------------------------------------------------
// Determinism (K1)
// ---------------------------------------------------------------------------

TEST(StateMachineDeterminism, SameLogGivesSameStateEverywhere) {
  // A random log with duplicates, late copies, and no-ops, applied to two
  // separate state machines (two "nodes"): they must end up identical.
  for (uint64_t seed = 1; seed <= 20; ++seed) {
    SCOPED_TRACE("seed=" + std::to_string(seed));
    std::mt19937_64 rng(seed);
    std::vector<LogEntry> log;
    std::vector<uint64_t> next_seq(4, 1);

    for (Index i = 1; i <= 500; ++i) {
      LogEntry e;
      e.term = 1;
      e.index = i;
      if (rng() % 10 == 0) {
        e.type = EntryType::NoOp;
      } else {
        const auto client = 1 + rng() % 3;
        // Mostly new requests, sometimes a retry of an older seq.
        uint64_t seq = next_seq[client];
        if (seq > 1 && rng() % 4 == 0) {
          seq = 1 + rng() % (seq - 1);
        } else {
          ++next_seq[client];
        }
        const auto op = static_cast<OpType>(1 + rng() % 3);
        const std::string key = "k" + std::to_string(rng() % 5);
        const std::string value = "v" + std::to_string(rng() % 100);
        e.data = kv::encode(cmd(op, client, seq, key, value));
      }
      log.push_back(e);
    }

    StateMachine a, b;
    for (const auto& e : log) a.apply(e);
    for (const auto& e : log) b.apply(e);
    EXPECT_EQ(a.data(), b.data());
    EXPECT_EQ(a.last_seq(), b.last_seq());
  }
}

}  // namespace
