// Unit tests for Raft leader election, one node at a time.
//
// Each test drives a single Raft node (id 1, peers 2 and 3 unless noted) by
// hand: it advances a FakeClock, feeds in crafted messages, and inspects what
// the node sent and persisted. No network, no other nodes, fully
// deterministic.

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
using rafty::HardState;
using rafty::kNoNode;
using rafty::MemStorage;
using rafty::Message;
using rafty::Millis;
using rafty::NodeId;
using rafty::Payload;
using rafty::Raft;
using rafty::RequestVoteReq;
using rafty::RequestVoteResp;
using rafty::Role;
using toolings::FakeClock;

// Captures every outbound message, plus what was on "disk" at the moment it
// was sent. That snapshot is how we prove persist-before-reply.
class RecordingTransport final : public rafty::Transport {
 public:
  explicit RecordingTransport(const MemStorage& storage) : storage_(storage) {}

  void send(const Message& msg) override {
    sent.push_back(msg);
    disk_at_send.push_back(storage_.load_hard_state());
  }

  // All sent payloads of type T, in send order.
  template <class T>
  std::vector<T> sent_of() const {
    std::vector<T> out;
    for (const Message& m : sent) {
      if (auto* p = std::get_if<T>(&m.payload)) out.push_back(*p);
    }
    return out;
  }

  void clear() {
    sent.clear();
    disk_at_send.clear();
  }

  std::vector<Message> sent;
  std::vector<HardState> disk_at_send;

 private:
  const MemStorage& storage_;
};

class RaftElectionTest : public ::testing::Test {
 protected:
  void make(std::vector<NodeId> peers = {2, 3}, bool pre_vote = true,
            bool check_quorum = true) {
    Config cfg;
    cfg.id = 1;
    cfg.peers = std::move(peers);
    cfg.election_timeout_min = 150ms;
    cfg.election_timeout_max = 300ms;
    cfg.heartbeat_interval = 50ms;
    cfg.pre_vote = pre_vote;
    cfg.check_quorum = check_quorum;
    cfg.seed = 42;
    raft = std::make_unique<Raft>(cfg, transport, storage, clock);
  }

  void deliver(NodeId from, Payload p) {
    raft->step(Message{from, 1, std::move(p)});
  }

  // Advances past the maximum election timeout so the node campaigns.
  void fire_election_timeout() {
    clock.advance(300ms);
    raft->tick();
  }

  // Drives node 1 to leader in a 3-node cluster: timeout -> pre-vote grant
  // from node 2 -> real vote grant from node 2.
  void become_leader() {
    fire_election_timeout();
    ASSERT_EQ(raft->role(), Role::PreCandidate);
    deliver(2, RequestVoteResp{raft->term() + 1, true, true});
    ASSERT_EQ(raft->role(), Role::Candidate);
    deliver(2, RequestVoteResp{raft->term(), true, false});
    ASSERT_EQ(raft->role(), Role::Leader);
    transport.clear();
  }

  FakeClock clock;
  MemStorage storage;
  RecordingTransport transport{storage};
  std::unique_ptr<Raft> raft;
};

// ---------------------------------------------------------------------------
// Startup and restart recovery
// ---------------------------------------------------------------------------

TEST_F(RaftElectionTest, StartsAsFollowerWithTermAndVoteFromStorage) {
  storage.save_hard_state(HardState{5, 2});
  make();
  EXPECT_EQ(raft->role(), Role::Follower);
  EXPECT_EQ(raft->term(), 5u);
  EXPECT_EQ(raft->voted_for(), 2u);
  EXPECT_EQ(raft->leader_id(), kNoNode);
}

TEST_F(RaftElectionTest, NoElectionBeforeTimeout) {
  make();
  clock.advance(149ms);  // below election_timeout_min
  raft->tick();
  EXPECT_EQ(raft->role(), Role::Follower);
  EXPECT_TRUE(transport.sent.empty());
}

// ---------------------------------------------------------------------------
// Pre-vote (I5): campaigning must not disturb terms until a majority agrees
// ---------------------------------------------------------------------------

TEST_F(RaftElectionTest, TimeoutStartsPreVoteWithoutBumpingTerm) {
  make();
  const int saves_before = storage.save_count();
  fire_election_timeout();

  EXPECT_EQ(raft->role(), Role::PreCandidate);
  EXPECT_EQ(raft->term(), 0u);  // unchanged
  EXPECT_EQ(storage.save_count(), saves_before);

  auto reqs = transport.sent_of<RequestVoteReq>();
  ASSERT_EQ(reqs.size(), 2u);
  for (const auto& r : reqs) {
    EXPECT_TRUE(r.pre_vote);
    EXPECT_EQ(r.term, 1u);  // the term it WOULD use
  }
}

TEST_F(RaftElectionTest, PreVoteMajorityStartsRealElection) {
  make();
  fire_election_timeout();
  transport.clear();

  deliver(2, RequestVoteResp{1, true, true});

  EXPECT_EQ(raft->role(), Role::Candidate);
  EXPECT_EQ(raft->term(), 1u);
  EXPECT_EQ(raft->voted_for(), 1u);
  // Self-vote persisted before the RequestVotes went out.
  EXPECT_EQ(storage.load_hard_state(), (HardState{1, 1}));
  auto reqs = transport.sent_of<RequestVoteReq>();
  ASSERT_EQ(reqs.size(), 2u);
  EXPECT_FALSE(reqs[0].pre_vote);
  EXPECT_EQ(transport.disk_at_send[0], (HardState{1, 1}));
}

TEST_F(RaftElectionTest, PreVoteRequestDoesNotChangeReceiverState) {
  make();
  const int saves_before = storage.save_count();

  deliver(2, RequestVoteReq{1, 2, 0, 0, /*pre_vote=*/true});

  auto resps = transport.sent_of<RequestVoteResp>();
  ASSERT_EQ(resps.size(), 1u);
  EXPECT_TRUE(resps[0].vote_granted);
  EXPECT_EQ(raft->term(), 0u);
  EXPECT_EQ(raft->voted_for(), kNoNode);
  EXPECT_EQ(storage.save_count(), saves_before);
}

TEST_F(RaftElectionTest, PreVoteRejectionFromNewerTermMakesUsCatchUp) {
  make();
  fire_election_timeout();
  deliver(2, RequestVoteResp{7, false, true});
  EXPECT_EQ(raft->role(), Role::Follower);
  EXPECT_EQ(raft->term(), 7u);
}

// ---------------------------------------------------------------------------
// Voting (I1, I3): one vote per term, persisted before the reply
// ---------------------------------------------------------------------------

TEST_F(RaftElectionTest, GrantsAtMostOneVotePerTerm) {
  make();
  deliver(2, RequestVoteReq{1, 2, 0, 0, false});
  deliver(3, RequestVoteReq{1, 3, 0, 0, false});

  auto resps = transport.sent_of<RequestVoteResp>();
  ASSERT_EQ(resps.size(), 2u);
  EXPECT_TRUE(resps[0].vote_granted);
  EXPECT_FALSE(resps[1].vote_granted);
  EXPECT_EQ(raft->voted_for(), 2u);
}

TEST_F(RaftElectionTest, RegrantsToSameCandidateInSameTerm) {
  // The first reply may have been lost; answering again is safe.
  make();
  deliver(2, RequestVoteReq{1, 2, 0, 0, false});
  deliver(2, RequestVoteReq{1, 2, 0, 0, false});
  auto resps = transport.sent_of<RequestVoteResp>();
  ASSERT_EQ(resps.size(), 2u);
  EXPECT_TRUE(resps[1].vote_granted);
}

TEST_F(RaftElectionTest, VoteIsPersistedBeforeReplyIsSent) {
  make();
  deliver(2, RequestVoteReq{1, 2, 0, 0, false});
  ASSERT_EQ(transport.sent.size(), 1u);
  // What was on disk at the instant the reply left the node.
  EXPECT_EQ(transport.disk_at_send[0], (HardState{1, 2}));
}

TEST_F(RaftElectionTest, VoteSurvivesCrashAndRestart) {
  make();
  deliver(2, RequestVoteReq{1, 2, 0, 0, false});
  make();  // "crash": new Raft object on the same storage
  transport.clear();
  deliver(3, RequestVoteReq{1, 3, 0, 0, false});
  auto resps = transport.sent_of<RequestVoteResp>();
  ASSERT_EQ(resps.size(), 1u);
  EXPECT_FALSE(resps[0].vote_granted);  // already voted for 2 in term 1
}

TEST_F(RaftElectionTest, NewTermClearsOldVote) {
  make();
  deliver(2, RequestVoteReq{1, 2, 0, 0, false});
  deliver(3, RequestVoteReq{2, 3, 0, 0, false});
  auto resps = transport.sent_of<RequestVoteResp>();
  ASSERT_EQ(resps.size(), 2u);
  EXPECT_TRUE(resps[1].vote_granted);
  EXPECT_EQ(raft->term(), 2u);
  EXPECT_EQ(raft->voted_for(), 3u);
}

// ---------------------------------------------------------------------------
// Term rules (I2)
// ---------------------------------------------------------------------------

TEST_F(RaftElectionTest, RejectsVoteFromStaleTerm) {
  storage.save_hard_state(HardState{5, kNoNode});
  make();
  deliver(2, RequestVoteReq{4, 2, 0, 0, false});
  auto resps = transport.sent_of<RequestVoteResp>();
  ASSERT_EQ(resps.size(), 1u);
  EXPECT_FALSE(resps[0].vote_granted);
  EXPECT_EQ(resps[0].term, 5u);  // tells the candidate it is behind
  EXPECT_EQ(raft->term(), 5u);
}

TEST_F(RaftElectionTest, RejectsHeartbeatFromStaleLeader) {
  storage.save_hard_state(HardState{5, kNoNode});
  make();
  deliver(2, AppendEntriesReq{3, 2});
  auto resps = transport.sent_of<AppendEntriesResp>();
  ASSERT_EQ(resps.size(), 1u);
  EXPECT_FALSE(resps[0].success);
  EXPECT_EQ(resps[0].term, 5u);
  EXPECT_EQ(raft->leader_id(), kNoNode);
}

TEST_F(RaftElectionTest, LeaderStepsDownOnHigherTermReply) {
  make();
  become_leader();
  deliver(2, AppendEntriesResp{9, false});
  EXPECT_EQ(raft->role(), Role::Follower);
  EXPECT_EQ(raft->term(), 9u);
  EXPECT_EQ(raft->voted_for(), kNoNode);
}

TEST_F(RaftElectionTest, CandidateStepsDownOnHeartbeatFromWinner) {
  make();
  fire_election_timeout();
  deliver(2, RequestVoteResp{1, true, true});
  ASSERT_EQ(raft->role(), Role::Candidate);

  deliver(3, AppendEntriesReq{1, 3});  // node 3 won term 1
  EXPECT_EQ(raft->role(), Role::Follower);
  EXPECT_EQ(raft->leader_id(), 3u);
  EXPECT_EQ(raft->term(), 1u);
}

// ---------------------------------------------------------------------------
// Winning
// ---------------------------------------------------------------------------

TEST_F(RaftElectionTest, MajorityOfVotesMakesLeaderAndSendsHeartbeats) {
  make();
  fire_election_timeout();
  deliver(2, RequestVoteResp{1, true, true});
  transport.clear();
  deliver(3, RequestVoteResp{1, true, false});

  EXPECT_EQ(raft->role(), Role::Leader);
  EXPECT_EQ(raft->leader_id(), 1u);
  EXPECT_EQ(transport.sent_of<AppendEntriesReq>().size(), 2u);
}

TEST_F(RaftElectionTest, DuplicateVoteIsCountedOnce) {
  make({2, 3, 4, 5});  // 5 nodes: quorum is 3
  fire_election_timeout();
  deliver(2, RequestVoteResp{1, true, true});
  deliver(3, RequestVoteResp{1, true, true});
  ASSERT_EQ(raft->role(), Role::Candidate);

  deliver(2, RequestVoteResp{1, true, false});
  deliver(2, RequestVoteResp{1, true, false});  // duplicate
  EXPECT_EQ(raft->role(), Role::Candidate);      // self + 2 = only 2 votes
}

TEST_F(RaftElectionTest, SingleNodeClusterElectsItself) {
  make({});
  fire_election_timeout();
  EXPECT_EQ(raft->role(), Role::Leader);
  EXPECT_EQ(raft->term(), 1u);
}

TEST_F(RaftElectionTest, LeaderSendsHeartbeatsEveryInterval) {
  make();
  become_leader();
  clock.advance(50ms);
  raft->tick();
  EXPECT_EQ(transport.sent_of<AppendEntriesReq>().size(), 2u);
}

// ---------------------------------------------------------------------------
// Leader stickiness and check-quorum (I5, I6)
// ---------------------------------------------------------------------------

TEST_F(RaftElectionTest, IgnoresVotesWhileHearingFromLeader) {
  make();
  deliver(2, AppendEntriesReq{1, 2});  // node 2 is leader of term 1
  clock.advance(100ms);                // still within the lease
  deliver(3, RequestVoteReq{2, 3, 0, 0, false});

  auto resps = transport.sent_of<RequestVoteResp>();
  ASSERT_EQ(resps.size(), 1u);
  EXPECT_FALSE(resps[0].vote_granted);
  EXPECT_EQ(raft->term(), 1u);  // a disruptive candidate cannot bump our term
}

TEST_F(RaftElectionTest, VotesAgainOnceLeaderLeaseExpires) {
  make();
  deliver(2, AppendEntriesReq{1, 2});
  clock.advance(150ms);  // lease is election_timeout_min
  deliver(3, RequestVoteReq{2, 3, 0, 0, false});

  auto resps = transport.sent_of<RequestVoteResp>();
  ASSERT_EQ(resps.size(), 1u);
  EXPECT_TRUE(resps[0].vote_granted);
  EXPECT_EQ(raft->term(), 2u);
}

TEST_F(RaftElectionTest, LeaderCutOffFromMajorityStepsDown) {
  make();
  become_leader();
  for (int i = 0; i < 30; ++i) {  // 300ms, no acks from anyone
    clock.advance(10ms);
    raft->tick();
  }
  EXPECT_EQ(raft->role(), Role::Follower);
  EXPECT_EQ(raft->leader_id(), kNoNode);
}

TEST_F(RaftElectionTest, LeaderWithMajorityAcksStaysLeader) {
  make();
  become_leader();
  for (int i = 0; i < 100; ++i) {  // 1s, node 2 keeps acking
    clock.advance(10ms);
    raft->tick();
    if (i % 5 == 0) deliver(2, AppendEntriesResp{raft->term(), true});
  }
  EXPECT_EQ(raft->role(), Role::Leader);
}

TEST_F(RaftElectionTest, WithoutCheckQuorumIsolatedLeaderStaysLeader) {
  // Shows the problem check-quorum solves.
  make({2, 3}, /*pre_vote=*/true, /*check_quorum=*/false);
  become_leader();
  for (int i = 0; i < 100; ++i) {
    clock.advance(10ms);
    raft->tick();
  }
  EXPECT_EQ(raft->role(), Role::Leader);
}

// ---------------------------------------------------------------------------
// Safety tripwire
// ---------------------------------------------------------------------------

TEST_F(RaftElectionTest, TwoLeadersInSameTermAborts) {
  make();
  become_leader();
  EXPECT_DEATH(deliver(2, AppendEntriesReq{raft->term(), 2}),
               "election safety violated");
}

}  // namespace
