// Integration tests for leader election: whole clusters on a simulated
// network, with crashes, partitions, message loss, and a seeded chaos loop.
//
// SimCluster checks Election Safety and term monotonicity after EVERY
// simulated millisecond, so each test also asserts violations() is empty.
//
// Replay a failing chaos seed with:  RAFT_SEED=<seed> ./election_test

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iterator>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "toolings/sim_cluster.hpp"

namespace {

using namespace std::chrono_literals;
using rafty::Config;
using rafty::NodeId;
using rafty::Role;
using rafty::Term;
using toolings::SimCluster;

// Live leader among `group`, if any.
std::optional<NodeId> leader_in(const SimCluster& c,
                                const std::vector<NodeId>& group) {
  for (NodeId id : group) {
    const auto* r = c.node(id);
    if (r && r->role() == Role::Leader) return id;
  }
  return std::nullopt;
}

Term max_term(const SimCluster& c) {
  Term t = 0;
  for (NodeId id : c.ids()) {
    if (const auto* r = c.node(id)) t = std::max(t, r->term());
  }
  return t;
}

#define EXPECT_NO_VIOLATIONS(c)                                     \
  EXPECT_TRUE((c).violations().empty())                             \
      << "seed=" << (c).seed() << " first violation: "              \
      << ((c).violations().empty() ? "" : (c).violations().front())

// ---------------------------------------------------------------------------
// Basic election, run for 3- and 5-node clusters
// ---------------------------------------------------------------------------

class ElectionBySize : public ::testing::TestWithParam<int> {};

TEST_P(ElectionBySize, ElectsOneStableLeader) {
  SimCluster c(GetParam(), /*seed=*/1);
  ASSERT_TRUE(c.run_until([&] { return c.leader().has_value(); }, 2s));
  const NodeId leader = *c.leader();
  const Term term = c.node(leader)->term();

  // With a healthy network, the leader keeps its job: no spurious elections.
  c.run_for(3s);
  EXPECT_EQ(c.leader(), leader);
  EXPECT_EQ(c.node(leader)->term(), term);
  EXPECT_EQ(c.leader_count(), 1);
  for (NodeId id : c.ids()) EXPECT_EQ(c.node(id)->leader_id(), leader);
  EXPECT_NO_VIOLATIONS(c);
}

TEST_P(ElectionBySize, LeaderCrashLeadsToNewLeader) {
  SimCluster c(GetParam(), /*seed=*/2);
  ASSERT_TRUE(c.run_until([&] { return c.leader().has_value(); }, 2s));
  const NodeId old_leader = *c.leader();
  const Term old_term = c.node(old_leader)->term();

  c.crash(old_leader);
  ASSERT_TRUE(c.run_until([&] { return c.leader().has_value(); }, 2s))
      << "no new leader after crash, seed=" << c.seed();
  EXPECT_NE(*c.leader(), old_leader);
  EXPECT_GT(c.node(*c.leader())->term(), old_term);

  // The old leader comes back and must follow, not lead.
  c.restart(old_leader);
  const NodeId new_leader = *c.leader();
  c.run_for(1s);
  EXPECT_EQ(c.node(old_leader)->role(), Role::Follower);
  EXPECT_EQ(c.node(old_leader)->leader_id(), new_leader);
  EXPECT_EQ(c.leader(), new_leader);
  EXPECT_NO_VIOLATIONS(c);
}

INSTANTIATE_TEST_SUITE_P(Sizes, ElectionBySize, ::testing::Values(3, 5));

// ---------------------------------------------------------------------------
// Partitions
// ---------------------------------------------------------------------------

TEST(ElectionPartition, MajoritySideElectsMinorityLeaderStepsDown) {
  SimCluster c(5, /*seed=*/3);
  ASSERT_TRUE(c.run_until([&] { return c.leader().has_value(); }, 2s));
  const NodeId old_leader = *c.leader();
  const Term old_term = c.node(old_leader)->term();

  // Old leader plus one follower on the minority side, three on the majority.
  std::vector<NodeId> minority{old_leader};
  std::vector<NodeId> majority;
  for (NodeId id : c.ids()) {
    if (id == old_leader) continue;
    if (minority.size() < 2) {
      minority.push_back(id);
    } else {
      majority.push_back(id);
    }
  }
  c.net().partition({minority, majority});
  c.run_for(3s);

  // Majority makes progress in a newer term.
  auto maj_leader = leader_in(c, majority);
  ASSERT_TRUE(maj_leader.has_value()) << "seed=" << c.seed();
  EXPECT_GT(c.node(*maj_leader)->term(), old_term);
  // Minority cannot elect anyone, and check-quorum removed the old leader.
  EXPECT_FALSE(leader_in(c, minority).has_value());

  // Heal: the minority rejoins and follows the majority's leader.
  c.net().heal();
  c.run_for(2s);
  EXPECT_EQ(c.leader_count(), 1);
  for (NodeId id : c.ids()) EXPECT_EQ(c.node(id)->leader_id(), *c.leader());
  EXPECT_NO_VIOLATIONS(c);
}

TEST(ElectionPartition, NoLeaderWithoutMajority) {
  SimCluster c(3, /*seed=*/4);
  ASSERT_TRUE(c.run_until([&] { return c.leader().has_value(); }, 2s));

  // Kill both followers: the leader can never gather a majority again.
  const NodeId survivor = *c.leader();
  std::vector<NodeId> crashed;
  for (NodeId id : c.ids()) {
    if (id != survivor) {
      c.crash(id);
      crashed.push_back(id);
    }
  }
  c.run_for(3s);
  EXPECT_FALSE(c.leader().has_value());  // check-quorum stepped it down

  // Bring one back: majority restored, a leader emerges again.
  c.restart(crashed[0]);
  EXPECT_TRUE(c.run_until([&] { return c.leader().has_value(); }, 3s));
  EXPECT_NO_VIOLATIONS(c);
}

// ---------------------------------------------------------------------------
// Pre-vote: a node that was cut off must not disrupt the cluster on rejoin
// ---------------------------------------------------------------------------

// Isolates one follower for 5s, heals, and returns how much the cluster's
// highest term grew.
Term term_growth_after_isolating_follower(bool pre_vote) {
  Config base;
  base.pre_vote = pre_vote;
  SimCluster c(3, /*seed=*/5, base);
  EXPECT_TRUE(c.run_until([&] { return c.leader().has_value(); }, 2s));
  const NodeId leader = *c.leader();
  const Term before = c.node(leader)->term();

  NodeId isolated = 0;
  std::vector<NodeId> rest;
  for (NodeId id : c.ids()) {
    if (id != leader && isolated == 0) {
      isolated = id;
    } else {
      rest.push_back(id);
    }
  }
  c.net().partition({{isolated}, rest});
  c.run_for(5s);
  c.net().heal();
  c.run_for(2s);

  EXPECT_TRUE(c.leader().has_value());
  EXPECT_NO_VIOLATIONS(c);
  return max_term(c) - before;
}

TEST(ElectionPreVote, IsolatedFollowerDoesNotDisruptOnRejoin) {
  // With pre-vote, the isolated node never wins a pre-vote, so it never
  // bumps its term, and the leader keeps its term after the heal.
  EXPECT_EQ(term_growth_after_isolating_follower(/*pre_vote=*/true), 0u);
}

TEST(ElectionPreVote, WithoutPreVoteIsolatedFollowerInflatesTerm) {
  // Control experiment: without pre-vote the isolated node increments its
  // term on every timeout, and on rejoin its higher term forces an election.
  EXPECT_GT(term_growth_after_isolating_follower(/*pre_vote=*/false), 0u);
}

// ---------------------------------------------------------------------------
// Chaos: random crashes, restarts, partitions, loss, and delay, many seeds
// ---------------------------------------------------------------------------

// Runs one chaos scenario. Safety is checked every step by SimCluster;
// liveness is checked at the end, after all faults are repaired.
void run_chaos(uint64_t seed) {
  SCOPED_TRACE("replay with RAFT_SEED=" + std::to_string(seed));
  SimCluster c(5, seed);
  c.net().set_drop_rate(0.1);
  c.net().set_delay(1ms, 20ms);

  std::mt19937_64 rng(seed ^ 0xC0FFEEULL);  // drives fault choices only
  std::set<NodeId> down;

  const auto end = c.clock().now() + 20s;
  while (c.clock().now() < end) {
    c.run_for(rafty::Millis{std::uniform_int_distribution<int>(100, 1000)(rng)});

    switch (std::uniform_int_distribution<int>(0, 3)(rng)) {
      case 0: {  // crash a random live node, keeping at most 2 down
        if (down.size() < 2) {
          NodeId n = c.ids()[rng() % c.ids().size()];
          if (!down.count(n)) {
            c.crash(n);
            down.insert(n);
          }
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

  // Repair everything; a leader must emerge and every node must follow it.
  for (NodeId n : down) c.restart(n);
  c.net().heal();
  c.net().set_drop_rate(0.0);
  ASSERT_TRUE(c.run_until([&] { return c.leader().has_value(); }, 5s))
      << "no leader after repair";
  c.run_for(1s);
  EXPECT_EQ(c.leader_count(), 1);
  for (NodeId id : c.ids()) EXPECT_EQ(c.node(id)->leader_id(), *c.leader());
  EXPECT_NO_VIOLATIONS(c);
}

TEST(ElectionChaos, RandomFaultsManySeeds) {
  if (const char* s = std::getenv("RAFT_SEED")) {
    run_chaos(std::strtoull(s, nullptr, 10));
    return;
  }
  for (uint64_t seed = 1; seed <= 200; ++seed) {
    run_chaos(seed);
    if (HasFailure()) return;  // stop at the first failing seed
  }
}

}  // namespace
