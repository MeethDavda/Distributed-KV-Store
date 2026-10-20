// Integration tests for crash recovery on real files.
//
// Every SimCluster here runs in FileStorage mode: a crash closes the node's
// WAL and hard state files, and a restart reopens them through recovery.
// crash_with_torn_tail() additionally leaves a half-written record at the
// end of the WAL, as a power cut mid-write would.
//
// All SimCluster invariant checks (Election Safety, State Machine Safety,
// Leader Completeness, Log Matching, ...) run throughout.
//
// Replay a failing chaos seed with:
//   RAFT_SEED=<seed> ./integration_tests --gtest_filter='RecoveryChaos.*'

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iterator>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "toolings/sim_cluster.hpp"

namespace {

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using rafty::EntryType;
using rafty::LogEntry;
using rafty::NodeId;
using toolings::SimCluster;

#define EXPECT_NO_VIOLATIONS(c)                                     \
  EXPECT_TRUE((c).violations().empty())                             \
      << "seed=" << (c).seed() << " first violation: "              \
      << ((c).violations().empty() ? "" : (c).violations().front())

// A fresh temp directory name per test (SimCluster creates and deletes it).
fs::path temp_dir(const std::string& name) {
  return fs::temp_directory_path() /
         ("raftkv-it-" + name + "-" + std::to_string(::getpid()));
}

bool wait_for_leader(SimCluster& c, rafty::Millis timeout = 3s) {
  return c.run_until([&] { return c.leader().has_value(); }, timeout);
}

// Every node is up, has the leader's whole log, and has applied all of it.
bool all_caught_up(const SimCluster& c) {
  auto l = c.leader();
  if (!l) return false;
  const auto* leader = c.node(*l);
  for (NodeId id : c.ids()) {
    const auto* r = c.node(id);
    if (!r || r->last_log_index() != leader->last_log_index() ||
        r->last_applied() != leader->commit_index()) {
      return false;
    }
  }
  return leader->commit_index() == leader->last_log_index();
}

// Client writes (Normal entries) in a node's log, in order.
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

NodeId some_follower(const SimCluster& c) {
  for (NodeId id : c.ids()) {
    if (id != *c.leader()) return id;
  }
  return 0;
}

// ---------------------------------------------------------------------------
// Whole-cluster restart
// ---------------------------------------------------------------------------

TEST(Recovery, WholeClusterRestartKeepsCommittedWritesAndTerms) {
  SimCluster c(3, /*seed=*/21, {}, temp_dir("whole"));
  ASSERT_TRUE(wait_for_leader(c));
  std::vector<std::string> writes;
  for (int i = 0; i < 20; ++i) {
    writes.push_back("w" + std::to_string(i));
    c.propose(writes.back());
  }
  ASSERT_TRUE(c.run_until([&] { return all_caught_up(c); }, 2s));
  const auto term_before = c.node(*c.leader())->term();

  // Every process dies at once; only the files remain.
  for (NodeId id : c.ids()) c.crash(id);
  for (NodeId id : c.ids()) c.restart(id);

  // Terms came back from disk (SimCluster also checks they never go down).
  for (NodeId id : c.ids()) EXPECT_GE(c.node(id)->term(), term_before);

  ASSERT_TRUE(wait_for_leader(c));
  ASSERT_TRUE(c.run_until([&] { return all_caught_up(c); }, 2s));
  for (NodeId id : c.ids()) EXPECT_EQ(log_data(c, id), writes);
  EXPECT_NO_VIOLATIONS(c);
}

// ---------------------------------------------------------------------------
// Torn writes
// ---------------------------------------------------------------------------

TEST(Recovery, TornTailOnFollowerIsCutOffAndRepaired) {
  SimCluster c(3, /*seed=*/22, {}, temp_dir("torn-follower"));
  ASSERT_TRUE(wait_for_leader(c));
  c.propose("before");
  ASSERT_TRUE(c.run_until([&] { return c.is_committed("before"); }, 2s));

  const NodeId f = some_follower(c);
  c.crash_with_torn_tail(f);
  c.propose("during");  // 2 of 3 still up: commits
  ASSERT_TRUE(c.run_until([&] { return c.is_committed("during"); }, 2s));

  c.restart(f);  // recovery must drop the junk and catch up normally
  ASSERT_TRUE(c.run_until([&] { return all_caught_up(c); }, 3s));
  EXPECT_EQ(log_data(c, f), (std::vector<std::string>{"before", "during"}));
  expect_identical_logs(c);
  EXPECT_NO_VIOLATIONS(c);
}

TEST(Recovery, TornTailOnLeaderLosesNoCommittedWrite) {
  SimCluster c(3, /*seed=*/23, {}, temp_dir("torn-leader"));
  ASSERT_TRUE(wait_for_leader(c));
  for (int i = 0; i < 5; ++i) c.propose("w" + std::to_string(i));
  ASSERT_TRUE(c.run_until([&] { return c.is_committed("w4"); }, 2s));

  const NodeId old_leader = *c.leader();
  c.crash_with_torn_tail(old_leader);
  ASSERT_TRUE(wait_for_leader(c));
  c.propose("after");
  ASSERT_TRUE(c.run_until([&] { return c.is_committed("after"); }, 2s));

  c.restart(old_leader);
  ASSERT_TRUE(c.run_until([&] { return all_caught_up(c); }, 3s));
  for (NodeId id : c.ids()) {
    EXPECT_EQ(log_data(c, id),
              (std::vector<std::string>{"w0", "w1", "w2", "w3", "w4", "after"}))
        << "node " << id;
  }
  EXPECT_NO_VIOLATIONS(c);
}

// ---------------------------------------------------------------------------
// Repeated restarts under load (a CLAUDE.md integration scenario)
// ---------------------------------------------------------------------------

TEST(Recovery, RepeatedRestartsUnderLoad) {
  SimCluster c(5, /*seed=*/24, {}, temp_dir("restarts"));
  std::mt19937_64 rng(24);
  int next = 0;

  for (int round = 0; round < 30; ++round) {
    // Writes keep flowing while one node at a time crashes and restarts.
    for (int t = 0; t < 20; ++t) {
      c.propose("w" + std::to_string(next++));
      c.run_for(10ms);
    }
    const NodeId victim = c.ids()[rng() % c.ids().size()];
    if (rng() % 2) {
      c.crash_with_torn_tail(victim);
    } else {
      c.crash(victim);
    }
    c.run_for(100ms);
    c.restart(victim);
  }

  ASSERT_TRUE(wait_for_leader(c));
  c.propose("final");
  ASSERT_TRUE(c.run_until([&] { return c.is_committed("final"); }, 3s));
  ASSERT_TRUE(c.run_until([&] { return all_caught_up(c); }, 3s));
  expect_identical_logs(c);
  EXPECT_NO_VIOLATIONS(c);
}

// ---------------------------------------------------------------------------
// Chaos on real files: crashes (some torn), partitions, loss, writes
// ---------------------------------------------------------------------------

void run_chaos(uint64_t seed) {
  SCOPED_TRACE("replay with RAFT_SEED=" + std::to_string(seed));
  SimCluster c(5, seed, {}, temp_dir("chaos-" + std::to_string(seed)));
  c.net().set_drop_rate(0.1);
  c.net().set_delay(1ms, 20ms);

  std::mt19937_64 rng(seed ^ 0xC0FFEEULL);  // drives fault choices only
  std::set<NodeId> down;
  int next = 0;

  const auto end = c.clock().now() + 20s;
  while (c.clock().now() < end) {
    const int ms = std::uniform_int_distribution<int>(100, 1000)(rng);
    for (int t = 0; t < ms; t += 10) {
      c.propose("w" + std::to_string(next++));
      c.run_for(10ms);
    }

    switch (std::uniform_int_distribution<int>(0, 3)(rng)) {
      case 0: {  // crash a node (half the time mid-write), at most 2 down
        const NodeId n = c.ids()[rng() % c.ids().size()];
        if (down.size() < 2 && !down.count(n)) {
          if (rng() % 2) {
            c.crash_with_torn_tail(n);
          } else {
            c.crash(n);
          }
          down.insert(n);
        }
        break;
      }
      case 1: {  // restart a crashed node: runs WAL recovery
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
        const auto cut = static_cast<long>(1 + rng() % (ids.size() - 1));
        c.net().partition({{ids.begin(), ids.begin() + cut},
                           {ids.begin() + cut, ids.end()}});
        break;
      }
      case 3:
        c.net().heal();
        break;
    }
  }

  // Repair everything; a final write must commit and all logs must converge.
  for (NodeId n : down) c.restart(n);
  c.net().heal();
  c.net().set_drop_rate(0.0);
  ASSERT_TRUE(wait_for_leader(c, 5s)) << "no leader after repair";
  c.propose("final");
  ASSERT_TRUE(c.run_until([&] { return c.is_committed("final"); }, 5s));
  ASSERT_TRUE(c.run_until([&] { return all_caught_up(c); }, 5s));
  expect_identical_logs(c);
  EXPECT_NO_VIOLATIONS(c);
}

TEST(RecoveryChaos, RandomFaultsOnRealFilesManySeeds) {
  if (const char* s = std::getenv("RAFT_SEED")) {
    run_chaos(std::strtoull(s, nullptr, 10));
    return;
  }
  // Fewer seeds than the in-memory chaos tests: real file I/O is slower.
  for (uint64_t seed = 1; seed <= 30; ++seed) {
    run_chaos(seed);
    if (HasFailure()) return;
  }
}

}  // namespace
