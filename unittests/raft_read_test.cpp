// Unit tests for ReadIndex (linearizable reads), one node at a time.
//
// Node 1 is driven to leader by hand; follower replies are crafted to control
// exactly which heartbeat rounds have been acknowledged.

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <variant>
#include <vector>

#include "rafty/raft.hpp"
#include "rafty/storage.hpp"
#include "toolings/sim_network.hpp"

namespace {

using namespace std::chrono_literals;
using rafty::AppendEntriesReq;
using rafty::AppendEntriesResp;
using rafty::Config;
using rafty::Index;
using rafty::MemStorage;
using rafty::Message;
using rafty::NodeId;
using rafty::Payload;
using rafty::Raft;
using rafty::RequestVoteResp;
using rafty::Role;
using rafty::Term;
using toolings::FakeClock;

// These tests only need the list of sent messages (the other unit test files
// also snapshot the disk at send time, which is not needed here).
class Recorder final : public rafty::Transport {
 public:
  void send(const Message& m) override { sent.push_back(m); }
  std::vector<AppendEntriesReq> appends_to(NodeId to) const {
    std::vector<AppendEntriesReq> out;
    for (const auto& m : sent) {
      if (m.to != to) continue;
      if (auto* p = std::get_if<AppendEntriesReq>(&m.payload)) out.push_back(*p);
    }
    return out;
  }
  std::vector<Message> sent;
};

AppendEntriesResp reply(Term term, bool success, Index match, uint64_t round) {
  AppendEntriesResp r;
  r.term = term;
  r.success = success;
  r.match_index = success ? match : 0;
  r.rejected_index = success ? 0 : match;
  r.read_round = round;
  return r;
}

class RaftReadTest : public ::testing::Test {
 protected:
  void make(std::vector<NodeId> peers = {2, 3}) {
    Config cfg;
    cfg.id = 1;
    cfg.peers = std::move(peers);
    cfg.seed = 42;
    raft = std::make_unique<Raft>(cfg, transport, storage, clock);
  }

  void deliver(NodeId from, Payload p) {
    raft->step(Message{from, 1, std::move(p)});
  }

  // Leader via pre-votes and votes from nodes 2 and 3. The no-op (index 1)
  // is appended but NOT yet committed.
  void become_leader() {
    clock.advance(300ms);
    raft->tick();
    const Term next = raft->term() + 1;
    deliver(2, RequestVoteResp{next, true, true});
    deliver(3, RequestVoteResp{next, true, true});
    deliver(2, RequestVoteResp{next, true, false});
    deliver(3, RequestVoteResp{next, true, false});
    ASSERT_EQ(raft->role(), Role::Leader);
    transport.sent.clear();
  }

  // Node 2 acknowledges the no-op, so it commits (condition 1 holds).
  void commit_noop() {
    deliver(2, reply(raft->term(), true, 1, 0));
    ASSERT_EQ(raft->commit_index(), 1u);
  }

  // The read round stamped on the latest AppendEntries sent to node 2.
  uint64_t current_round() const {
    auto v = transport.appends_to(2);
    return v.empty() ? 0 : v.back().read_round;
  }

  FakeClock clock;
  MemStorage storage;
  Recorder transport;
  std::unique_ptr<Raft> raft;
};

// ---------------------------------------------------------------------------
// Who may serve reads
// ---------------------------------------------------------------------------

TEST_F(RaftReadTest, FollowerRefuses) {
  make();
  EXPECT_FALSE(raft->read_index(1));
}

TEST_F(RaftReadTest, SingleNodeReadsImmediately) {
  make({});
  clock.advance(300ms);
  raft->tick();  // elects itself and commits its no-op
  ASSERT_TRUE(raft->read_index(1));
  auto reads = raft->take_reads();
  ASSERT_EQ(reads.size(), 1u);
  EXPECT_TRUE(reads[0].ok);
  EXPECT_EQ(reads[0].index, 1u);
}

// ---------------------------------------------------------------------------
// Condition 1: committed in the current term
// ---------------------------------------------------------------------------

TEST_F(RaftReadTest, WaitsUntilNoOpCommits) {
  make();
  become_leader();
  ASSERT_TRUE(raft->read_index(1));
  EXPECT_TRUE(transport.sent.empty());  // no round started yet
  EXPECT_TRUE(raft->take_reads().empty());

  commit_noop();                    // now the read gets index + round
  const uint64_t round = current_round();
  ASSERT_GT(round, 0u);
  EXPECT_TRUE(raft->take_reads().empty());  // round not acked yet

  deliver(2, reply(raft->term(), true, 1, round));
  auto reads = raft->take_reads();
  ASSERT_EQ(reads.size(), 1u);
  EXPECT_TRUE(reads[0].ok);
  EXPECT_EQ(reads[0].index, 1u);
}

TEST_F(RaftReadTest, IndexIsCommitIndexAtRequestTimeNotLastLogIndex) {
  make();
  become_leader();
  commit_noop();
  raft->propose("x");  // index 2, not committed
  ASSERT_TRUE(raft->read_index(7));

  // Node 2 acks the round but still only matches index 1: commit stays 1.
  deliver(2, reply(raft->term(), true, 1, current_round()));
  auto reads = raft->take_reads();
  ASSERT_EQ(reads.size(), 1u);
  EXPECT_EQ(reads[0].id, 7u);
  EXPECT_EQ(reads[0].index, 1u);  // the uncommitted write is not visible
}

// ---------------------------------------------------------------------------
// Condition 2: a majority acked a round that started after the read
// ---------------------------------------------------------------------------

TEST_F(RaftReadTest, AckOfAnOlderRoundDoesNotConfirm) {
  make();
  become_leader();
  commit_noop();

  raft->read_index(1);
  const uint64_t first = current_round();
  deliver(2, reply(raft->term(), true, 1, first));
  ASSERT_EQ(raft->take_reads().size(), 1u);

  raft->read_index(2);
  const uint64_t second = current_round();
  ASSERT_GT(second, first);
  deliver(3, reply(raft->term(), true, 1, first));  // old round: not enough
  EXPECT_TRUE(raft->take_reads().empty());
  deliver(3, reply(raft->term(), true, 1, second));
  EXPECT_EQ(raft->take_reads().size(), 1u);
}

TEST_F(RaftReadTest, FiveNodesNeedTwoAcks) {
  make({2, 3, 4, 5});
  become_leader();
  deliver(2, reply(raft->term(), true, 1, 0));
  deliver(3, reply(raft->term(), true, 1, 0));  // no-op committed (3 of 5)
  ASSERT_EQ(raft->commit_index(), 1u);

  raft->read_index(1);
  const uint64_t round = current_round();
  deliver(4, reply(raft->term(), true, 1, round));
  EXPECT_TRUE(raft->take_reads().empty());  // self + 1 = 2 of 5
  deliver(5, reply(raft->term(), true, 1, round));
  EXPECT_EQ(raft->take_reads().size(), 1u);  // self + 2 = 3 of 5
}

TEST_F(RaftReadTest, RejectionInCurrentTermAlsoConfirms) {
  // A log mismatch is still "I accept you as leader for this term".
  make();
  become_leader();
  commit_noop();
  raft->read_index(1);
  deliver(3, reply(raft->term(), false, 1, current_round()));
  EXPECT_EQ(raft->take_reads().size(), 1u);
}

TEST_F(RaftReadTest, ReadsArrivingTogetherShareOneRound) {
  make();
  become_leader();
  raft->read_index(1);
  raft->read_index(2);
  commit_noop();  // both reads start now, in the same round

  EXPECT_EQ(transport.appends_to(2).size(), 1u);  // one broadcast, not two
  deliver(2, reply(raft->term(), true, 1, current_round()));
  EXPECT_EQ(raft->take_reads().size(), 2u);  // one ack confirms both
}

// ---------------------------------------------------------------------------
// K6: losing leadership fails pending reads
// ---------------------------------------------------------------------------

TEST_F(RaftReadTest, HigherTermFailsPendingReads) {
  make();
  become_leader();
  commit_noop();
  raft->read_index(1);
  deliver(2, reply(raft->term() + 1, false, 0, 0));  // a newer term exists

  auto reads = raft->take_reads();
  ASSERT_EQ(reads.size(), 1u);
  EXPECT_FALSE(reads[0].ok);
  EXPECT_EQ(raft->role(), Role::Follower);
}

TEST_F(RaftReadTest, CheckQuorumStepDownFailsPendingReads) {
  // A leader cut off from everyone: its read can never be confirmed, and
  // check-quorum eventually removes it, failing the read.
  make();
  become_leader();
  commit_noop();
  raft->read_index(1);
  for (int i = 0; i < 40; ++i) {
    clock.advance(10ms);
    raft->tick();
  }
  ASSERT_EQ(raft->role(), Role::Follower);
  auto reads = raft->take_reads();
  ASSERT_EQ(reads.size(), 1u);
  EXPECT_FALSE(reads[0].ok);
}

// ---------------------------------------------------------------------------
// Follower side
// ---------------------------------------------------------------------------

TEST_F(RaftReadTest, FollowerEchoesRoundInSuccessAndReject) {
  make();
  AppendEntriesReq ok;
  ok.term = 1;
  ok.leader_id = 2;
  ok.read_round = 9;
  deliver(2, ok);

  AppendEntriesReq bad = ok;
  bad.prev_log_index = 5;  // we do not have it: reject
  bad.prev_log_term = 1;
  bad.read_round = 10;
  deliver(2, bad);

  std::vector<AppendEntriesResp> resps;
  for (const auto& m : transport.sent) {
    if (auto* p = std::get_if<AppendEntriesResp>(&m.payload)) resps.push_back(*p);
  }
  ASSERT_EQ(resps.size(), 2u);
  EXPECT_TRUE(resps[0].success);
  EXPECT_EQ(resps[0].read_round, 9u);
  EXPECT_FALSE(resps[1].success);
  EXPECT_EQ(resps[1].read_round, 10u);
}

}  // namespace
