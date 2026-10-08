// Integration tests for log replication and commit: whole clusters on the
// simulated network, with client writes under crashes, partitions, message
// loss, and a seeded chaos loop.
//
// SimCluster checks after every step: Election Safety, term and commit
// monotonicity, State Machine Safety, in-order apply; on every new leader:
// Leader Completeness; periodically: Log Matching.
//
// Replay a failing chaos seed with:  RAFT_SEED=<seed> ./integration_tests
//   --gtest_filter='ReplicationChaos.*'

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iterator>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "toolings/sim_cluster.hpp"

namespace {

using namespace std::chrono_literals;
using rafty::EntryType;
using rafty::Index;
using rafty::LogEntry;
using rafty::NodeId;
using rafty::Role;
using toolings::SimCluster;

#define EXPECT_NO_VIOLATIONS(c)                                     \
  EXPECT_TRUE((c).violations().empty())                             \
      << "seed=" << (c).seed() << " first violation: "              \
      << ((c).violations().empty() ? "" : (c).violations().front())

bool wait_for_leader(SimCluster& c, rafty::Millis timeout = 3s) {
  return c.run_until([&] { return c.leader().has_value(); }, timeout);
}

// True when every node is up, has the leader's whole log, and has applied
// everything the leader has committed.
bool all_caught_up(const SimCluster& c) {
  auto l = c.leader();
  if (!l) return false;
  const auto* leader = c.node(*l);
  for (NodeId id : c.ids()) {
    const auto* r = c.node(id);
    if (!r) return false;
    if (r->last_log_index() != leader->last_log_index()) return false;
    if (r->last_applied() != leader->commit_index()) return false;
  }
  return leader->commit_index() == leader->last_log_index();
}

// Client data of every Normal entry in a node's log, in order.
std::vector<std::string> log_data(const SimCluster& c, NodeId id) {
  const auto& s = c.storage(id);
  std::vector<std::string> out;
  if (s.last_index() == 0) return out;
  for (const LogEntry& e : s.entries(1, s.last_index() + 1)) {
    if (e.type == EntryType::Normal) out.push_back(e.data);
  }
  return out;
}

void expect_identical_logs(const SimCluster& c) {
  const auto& first = c.storage(c.ids()[0]);
  for (NodeId id : c.ids()) {
    const auto& s = c.storage(id);
    ASSERT_EQ(s.last_index(), first.last_index()) << "node " << id;
    if (s.last_index() == 0) continue;
    EXPECT_EQ(s.entries(1, s.last_index() + 1),
              first.entries(1, first.last_index() + 1))
        << "node " << id;
  }
}

// ---------------------------------------------------------------------------
// Healthy cluster
// ---------------------------------------------------------------------------

class ReplicationBySize : public ::testing::TestWithParam<int> {};

TEST_P(ReplicationBySize, WritesReachEveryNodeInOrder) {
  SimCluster c(GetParam(), /*seed=*/11);
  ASSERT_TRUE(wait_for_leader(c));

  for (int i = 0; i < 20; ++i) {
    ASSERT_TRUE(c.propose("w" + std::to_string(i)).has_value());
    c.run_for(5ms);
  }
  ASSERT_TRUE(c.run_until([&] { return all_caught_up(c); }, 2s));

  std::vector<std::string> expected;
  for (int i = 0; i < 20; ++i) expected.push_back("w" + std::to_string(i));
  for (NodeId id : c.ids()) EXPECT_EQ(log_data(c, id), expected);
  expect_identical_logs(c);
  EXPECT_NO_VIOLATIONS(c);
}

INSTANTIATE_TEST_SUITE_P(Sizes, ReplicationBySize, ::testing::Values(3, 5));

TEST(Replication, ProgressWithOneFollowerDown) {
  SimCluster c(3, /*seed=*/12);
  ASSERT_TRUE(wait_for_leader(c));
  NodeId follower = c.ids()[0] == *c.leader() ? c.ids()[1] : c.ids()[0];
  c.crash(follower);

  // 2 of 3 is still a majority: writes commit.
  for (int i = 0; i < 10; ++i) c.propose("w" + std::to_string(i));
  ASSERT_TRUE(c.run_until([&] { return c.is_committed("w9"); }, 2s));

  // The crashed follower restarts and catches up from its own log onward.
  c.restart(follower);
  ASSERT_TRUE(c.run_until([&] { return all_caught_up(c); }, 3s));
  expect_identical_logs(c);
  EXPECT_NO_VIOLATIONS(c);
}

TEST(Replication, FarBehindFollowerCatchesUpInBatches) {
  SimCluster c(3, /*seed=*/13);
  ASSERT_TRUE(wait_for_leader(c));
  NodeId follower = c.ids()[0] == *c.leader() ? c.ids()[1] : c.ids()[0];
  c.crash(follower);

  // More entries than max_entries_per_msg (64), so catch-up needs several
  // messages.
  for (int i = 0; i < 300; ++i) {
    c.propose("w" + std::to_string(i));
    if (i % 20 == 0) c.run_for(10ms);
  }
  ASSERT_TRUE(c.run_until([&] { return c.is_committed("w299"); }, 3s));

  c.restart(follower);
  ASSERT_TRUE(c.run_until([&] { return all_caught_up(c); }, 5s));
  expect_identical_logs(c);
  EXPECT_NO_VIOLATIONS(c);
}

// ---------------------------------------------------------------------------
// Uncommitted writes on a leader that loses its majority
// ---------------------------------------------------------------------------

TEST(Replication, MinorityLeaderWritesNeverCommitAndAreOverwritten) {
  SimCluster c(5, /*seed=*/14);
  ASSERT_TRUE(wait_for_leader(c));
  const NodeId old_leader = *c.leader();

  std::vector<NodeId> minority{old_leader};
  std::vector<NodeId> majority;
  for (NodeId id : c.ids()) {
    if (id == old_leader) continue;
    (minority.size() < 2 ? minority : majority).push_back(id);
  }
  c.net().partition({minority, majority});

  // The old leader still thinks it leads for a moment and accepts writes,
  // but can only reach 2 of 5 nodes: these must never commit.
  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(
        c.propose_on(old_leader, "stale" + std::to_string(i)).has_value());
  }

  // The majority elects a new leader and commits its own writes.
  ASSERT_TRUE(c.run_until(
      [&] {
        for (NodeId id : majority) {
          if (c.node(id)->role() == Role::Leader) return true;
        }
        return false;
      },
      3s));
  for (int i = 0; i < 5; ++i) c.propose("fresh" + std::to_string(i));
  ASSERT_TRUE(c.run_until([&] { return c.is_committed("fresh4"); }, 2s));

  // Heal: the minority's conflicting entries are truncated and replaced.
  c.net().heal();
  ASSERT_TRUE(c.run_until([&] { return all_caught_up(c); }, 3s));

  for (int i = 0; i < 5; ++i) {
    EXPECT_FALSE(c.is_committed("stale" + std::to_string(i)));
  }
  for (NodeId id : c.ids()) {
    for (const auto& d : log_data(c, id)) {
      EXPECT_EQ(d.rfind("stale", 0), std::string::npos)
          << "node " << id << " still holds " << d;
    }
  }
  expect_identical_logs(c);
  EXPECT_NO_VIOLATIONS(c);
}

TEST(Replication, LeaderCrashBeforeReplicatingLosesOnlyUncommittedWrite) {
  SimCluster c(3, /*seed=*/15);
  ASSERT_TRUE(wait_for_leader(c));
  c.propose("committed");
  ASSERT_TRUE(c.run_until([&] { return c.is_committed("committed"); }, 2s));

  // Cut the leader off, give it a write it cannot replicate, then kill it.
  const NodeId old_leader = *c.leader();
  std::vector<NodeId> rest;
  for (NodeId id : c.ids()) {
    if (id != old_leader) rest.push_back(id);
  }
  c.net().partition({{old_leader}, rest});
  ASSERT_TRUE(c.propose_on(old_leader, "uncommitted").has_value());
  c.crash(old_leader);
  c.net().heal();

  ASSERT_TRUE(wait_for_leader(c));
  c.propose("after");
  ASSERT_TRUE(c.run_until([&] { return c.is_committed("after"); }, 2s));

  // The old leader restarts with "uncommitted" in its log; it must be
  // replaced, and the committed write must survive everywhere.
  c.restart(old_leader);
  ASSERT_TRUE(c.run_until([&] { return all_caught_up(c); }, 3s));
  EXPECT_TRUE(c.is_committed("committed"));
  EXPECT_FALSE(c.is_committed("uncommitted"));
  for (NodeId id : c.ids()) {
    EXPECT_EQ(log_data(c, id),
              (std::vector<std::string>{"committed", "after"}))
        << "node " << id;
  }
  EXPECT_NO_VIOLATIONS(c);
}

TEST(Replication, RepeatedLeaderCrashesKeepCommittedWrites) {
  SimCluster c(5, /*seed=*/16);
  std::vector<std::string> committed_writes;

  for (int round = 0; round < 10; ++round) {
    ASSERT_TRUE(wait_for_leader(c)) << "round " << round;
    const std::string w = "r" + std::to_string(round);
    c.propose(w);
    ASSERT_TRUE(c.run_until([&] { return c.is_committed(w); }, 2s))
        << "round " << round;
    committed_writes.push_back(w);

    const NodeId l = *c.leader();
    c.crash(l);
    c.run_for(50ms);
    c.restart(l);
  }

  ASSERT_TRUE(c.run_until([&] { return all_caught_up(c); }, 3s));
  for (NodeId id : c.ids()) EXPECT_EQ(log_data(c, id), committed_writes);
  EXPECT_NO_VIOLATIONS(c);
}

// ---------------------------------------------------------------------------
// Chaos: random faults plus a steady stream of writes, many seeds
// ---------------------------------------------------------------------------

void run_chaos(uint64_t seed) {
  SCOPED_TRACE("replay with RAFT_SEED=" + std::to_string(seed));
  SimCluster c(5, seed);
  c.net().set_drop_rate(0.1);
  c.net().set_delay(1ms, 20ms);

  std::mt19937_64 rng(seed ^ 0xC0FFEEULL);  // drives fault choices only
  std::set<NodeId> down;
  int next_write = 0;

  const auto end = c.clock().now() + 20s;
  while (c.clock().now() < end) {
    // Run for a while, offering a write to whoever leads every ~10ms.
    const int ms = std::uniform_int_distribution<int>(100, 1000)(rng);
    for (int t = 0; t < ms; t += 10) {
      c.propose("w" + std::to_string(next_write++));
      c.run_for(10ms);
    }

    switch (std::uniform_int_distribution<int>(0, 3)(rng)) {
      case 0: {  // crash a random node, keeping at most 2 down
        NodeId n = c.ids()[rng() % c.ids().size()];
        if (down.size() < 2 && !down.count(n)) {
          c.crash(n);
          down.insert(n);
        }
        break;
      }
      case 1: {  // restart a random crashed node
        if (!down.empty()) {
          auto it = down.begin();
          std::advance(it, static_cast<long>(rng() % down.size()));
          c.restart(*it);
          down.erase(it);
        }
        break;
      }
      case 2: {  // random two-way partition
        std::vector<NodeId> ids = c.ids();
        std::shuffle(ids.begin(), ids.end(), rng);
        auto cut = static_cast<long>(1 + rng() % (ids.size() - 1));
        c.net().partition({{ids.begin(), ids.begin() + cut},
                           {ids.begin() + cut, ids.end()}});
        break;
      }
      case 3:
        c.net().heal();
        break;
    }
  }

  // Repair everything. One final write must commit, and every node must end
  // with the same fully applied log.
  for (NodeId n : down) c.restart(n);
  c.net().heal();
  c.net().set_drop_rate(0.0);
  ASSERT_TRUE(wait_for_leader(c, 5s)) << "no leader after repair";
  c.propose("final");
  ASSERT_TRUE(c.run_until([&] { return c.is_committed("final"); }, 5s))
      << "final write did not commit";
  ASSERT_TRUE(c.run_until([&] { return all_caught_up(c); }, 5s))
      << "nodes did not converge";
  expect_identical_logs(c);

  // Every node applied exactly the cluster's committed history, and no write
  // was committed twice.
  for (NodeId id : c.ids()) {
    EXPECT_EQ(c.applied(id).size(), c.committed().size()) << "node " << id;
  }
  std::set<std::string> seen;
  for (const auto& [idx, e] : c.committed()) {
    if (e.type != EntryType::Normal) continue;
    EXPECT_TRUE(seen.insert(e.data).second) << "duplicate commit " << e.data;
  }
  EXPECT_NO_VIOLATIONS(c);
}

TEST(ReplicationChaos, RandomFaultsWithWritesManySeeds) {
  if (const char* s = std::getenv("RAFT_SEED")) {
    run_chaos(std::strtoull(s, nullptr, 10));
    return;
  }
  for (uint64_t seed = 1; seed <= 100; ++seed) {
    run_chaos(seed);
    if (HasFailure()) return;  // stop at the first failing seed
  }
}

}  // namespace
