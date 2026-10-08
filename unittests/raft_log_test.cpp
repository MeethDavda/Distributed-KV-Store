// Unit tests for log replication and commit, one node at a time.
//
// Same approach as raft_election_test.cpp: drive node 1 (peers 2 and 3
// unless noted) with hand-crafted messages and inspect what it sent, what it
// stored, and what it committed.

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
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
using rafty::EntryType;
using rafty::HardState;
using rafty::Index;
using rafty::LogEntry;
using rafty::MemStorage;
using rafty::Message;
using rafty::NodeId;
using rafty::Payload;
using rafty::Raft;
using rafty::RequestVoteReq;
using rafty::RequestVoteResp;
using rafty::Role;
using rafty::Term;
using toolings::FakeClock;

LogEntry entry(Term term, Index index, std::string data = "") {
  LogEntry e;
  e.term = term;
  e.index = index;
  e.data = std::move(data);
  return e;
}

AppendEntriesReq ae(Term term, NodeId leader, Index prev_index, Term prev_term,
                    std::vector<LogEntry> entries, Index leader_commit) {
  AppendEntriesReq r;
  r.term = term;
  r.leader_id = leader;
  r.prev_log_index = prev_index;
  r.prev_log_term = prev_term;
  r.entries = std::move(entries);
  r.leader_commit = leader_commit;
  return r;
}

AppendEntriesResp ack(Term term, Index match) {
  AppendEntriesResp r;
  r.term = term;
  r.success = true;
  r.match_index = match;
  return r;
}

AppendEntriesResp nack(Term term, Index rejected, Index follower_last) {
  AppendEntriesResp r;
  r.term = term;
  r.success = false;
  r.rejected_index = rejected;
  r.last_log_index = follower_last;
  return r;
}

// Captures outbound messages plus the log length on "disk" at the moment
// each was sent, which is how we prove entries are durable before the ack.
class RecordingTransport final : public rafty::Transport {
 public:
  explicit RecordingTransport(const MemStorage& storage) : storage_(storage) {}

  void send(const Message& msg) override {
    sent.push_back(msg);
    disk_last_index_at_send.push_back(storage_.last_index());
  }

  // Payloads of type T sent to `to` (or to anyone if to == 0), in order.
  template <class T>
  std::vector<T> sent_of(NodeId to = 0) const {
    std::vector<T> out;
    for (const Message& m : sent) {
      if (to != 0 && m.to != to) continue;
      if (auto* p = std::get_if<T>(&m.payload)) out.push_back(*p);
    }
    return out;
  }

  void clear() {
    sent.clear();
    disk_last_index_at_send.clear();
  }

  std::vector<Message> sent;
  std::vector<Index> disk_last_index_at_send;

 private:
  const MemStorage& storage_;
};

class RaftLogTest : public ::testing::Test {
 protected:
  void make(std::vector<NodeId> peers = {2, 3}) {
    Config cfg;
    cfg.id = 1;
    cfg.peers = std::move(peers);
    cfg.seed = 42;
    raft = std::make_unique<Raft>(cfg, transport, storage, clock);
  }

  // Pre-fills the "disk" before the node starts, as if from an earlier life.
  void seed_log(const std::vector<LogEntry>& entries, Term current_term) {
    storage.append(entries);
    storage.save_hard_state(HardState{current_term, rafty::kNoNode});
  }

  void deliver(NodeId from, Payload p) {
    raft->step(Message{from, 1, std::move(p)});
  }

  // Drives node 1 to leader with pre-votes and votes from nodes 2 and 3 (a
  // majority for 3 or 5 nodes; grants arriving after a win are ignored as
  // stale), then clears the recorder. The new leader has appended its no-op.
  void become_leader() {
    clock.advance(300ms);
    raft->tick();
    ASSERT_EQ(raft->role(), Role::PreCandidate);
    const Term next = raft->term() + 1;
    deliver(2, RequestVoteResp{next, true, true});
    deliver(3, RequestVoteResp{next, true, true});
    ASSERT_EQ(raft->role(), Role::Candidate);
    deliver(2, RequestVoteResp{next, true, false});
    deliver(3, RequestVoteResp{next, true, false});
    ASSERT_EQ(raft->role(), Role::Leader);
    transport.clear();
  }

  AppendEntriesResp last_ae_resp() const {
    auto v = transport.sent_of<AppendEntriesResp>();
    EXPECT_FALSE(v.empty());
    return v.empty() ? AppendEntriesResp{} : v.back();
  }

  FakeClock clock;
  MemStorage storage;
  RecordingTransport transport{storage};
  std::unique_ptr<Raft> raft;
};

// ===========================================================================
// Follower: consistency check (L1)
// ===========================================================================

TEST_F(RaftLogTest, AppendsEntriesToEmptyLog) {
  make();
  deliver(2, ae(1, 2, 0, 0, {entry(1, 1, "a"), entry(1, 2, "b")}, 0));

  auto r = last_ae_resp();
  EXPECT_TRUE(r.success);
  EXPECT_EQ(r.match_index, 2u);
  EXPECT_EQ(storage.last_index(), 2u);
  EXPECT_EQ(storage.entries(1, 3)[1].data, "b");
}

TEST_F(RaftLogTest, RejectsWhenPrevEntryMissing) {
  make();
  deliver(2, ae(1, 2, 5, 1, {entry(1, 6)}, 0));

  auto r = last_ae_resp();
  EXPECT_FALSE(r.success);
  EXPECT_EQ(r.rejected_index, 5u);
  EXPECT_EQ(r.last_log_index, 0u);  // hint: "my log is empty"
  EXPECT_EQ(storage.last_index(), 0u);
}

TEST_F(RaftLogTest, RejectsWhenPrevTermDiffers) {
  seed_log({entry(1, 1), entry(1, 2)}, 1);
  make();
  deliver(2, ae(2, 2, 2, 2, {entry(2, 3)}, 0));  // leader thinks index 2 is t2

  EXPECT_FALSE(last_ae_resp().success);
  EXPECT_EQ(storage.last_index(), 2u);  // nothing appended
}

// ===========================================================================
// Follower: truncation only on real conflict (L3)
// ===========================================================================

TEST_F(RaftLogTest, TruncatesConflictingSuffix) {
  // Entries 2 and 3 came from a term-1 leader that lost; the term-2 leader
  // has a different entry at index 2.
  seed_log({entry(1, 1), entry(1, 2, "old"), entry(1, 3, "old")}, 1);
  make();
  deliver(2, ae(2, 2, 1, 1, {entry(2, 2, "new")}, 0));

  EXPECT_TRUE(last_ae_resp().success);
  ASSERT_EQ(storage.last_index(), 2u);
  EXPECT_EQ(storage.term_at(2), 2u);
  EXPECT_EQ(storage.entries(2, 3)[0].data, "new");
}

TEST_F(RaftLogTest, DelayedOldAppendDoesNotTruncateNewerEntries) {
  make();
  // The leader sent two messages; the newer one (entries 1..3) arrives first.
  deliver(2, ae(1, 2, 0, 0, {entry(1, 1), entry(1, 2), entry(1, 3)}, 0));
  deliver(2, ae(1, 2, 0, 0, {entry(1, 1)}, 0));  // the delayed older one

  EXPECT_EQ(storage.last_index(), 3u);  // entries 2 and 3 survived
  auto r = last_ae_resp();
  EXPECT_TRUE(r.success);
  EXPECT_EQ(r.match_index, 1u);  // only claims what this message proved
}

TEST_F(RaftLogTest, DuplicateAppendIsIdempotent) {
  make();
  auto msg = ae(1, 2, 0, 0, {entry(1, 1, "a"), entry(1, 2, "b")}, 0);
  deliver(2, msg);
  deliver(2, msg);
  EXPECT_EQ(storage.last_index(), 2u);
  EXPECT_EQ(last_ae_resp().match_index, 2u);
}

// ===========================================================================
// Follower: durability and commit (L4, L8)
// ===========================================================================

TEST_F(RaftLogTest, EntriesAreDurableBeforeAck) {
  make();
  deliver(2, ae(1, 2, 0, 0, {entry(1, 1), entry(1, 2)}, 0));
  ASSERT_EQ(transport.sent.size(), 1u);
  // On disk at the instant the success reply left the node.
  EXPECT_EQ(transport.disk_last_index_at_send[0], 2u);
}

TEST_F(RaftLogTest, FollowerCommitIsCappedAtLastNewEntry) {
  make();
  deliver(2, ae(1, 2, 0, 0, {entry(1, 1), entry(1, 2)}, /*commit=*/5));
  EXPECT_EQ(raft->commit_index(), 2u);
}

TEST_F(RaftLogTest, FollowerDoesNotCommitUnverifiedStaleEntries) {
  // We hold entries 2..3 from a dead term-1 leader. The term-2 leader has
  // committed index 3, but ITS index 3 may differ from ours. This message
  // only proves index 1 matches, so we may commit only up to 1.
  seed_log({entry(1, 1), entry(1, 2), entry(1, 3)}, 1);
  make();
  deliver(2, ae(2, 2, 1, 1, {}, /*commit=*/3));
  EXPECT_EQ(raft->commit_index(), 1u);
}

TEST_F(RaftLogTest, FollowerCommitNeverDecreases) {
  make();
  deliver(2, ae(1, 2, 0, 0, {entry(1, 1), entry(1, 2)}, 2));
  deliver(2, ae(1, 2, 0, 0, {entry(1, 1)}, 1));  // older message
  EXPECT_EQ(raft->commit_index(), 2u);
}

TEST_F(RaftLogTest, TakeCommittedHandsOutEachEntryOnceInOrder) {
  make();
  deliver(2, ae(1, 2, 0, 0, {entry(1, 1, "a"), entry(1, 2, "b")}, 1));
  auto first = raft->take_committed();
  ASSERT_EQ(first.size(), 1u);
  EXPECT_EQ(first[0].data, "a");
  EXPECT_TRUE(raft->take_committed().empty());

  deliver(2, ae(1, 2, 2, 1, {}, 2));
  auto second = raft->take_committed();
  ASSERT_EQ(second.size(), 1u);
  EXPECT_EQ(second[0].data, "b");
  EXPECT_EQ(raft->last_applied(), 2u);
}

TEST_F(RaftLogTest, RestartKeepsLogButRelearnsCommit) {
  make();
  deliver(2, ae(1, 2, 0, 0, {entry(1, 1), entry(1, 2)}, 2));
  make();  // crash + restart on the same storage
  EXPECT_EQ(raft->last_log_index(), 2u);
  EXPECT_EQ(raft->commit_index(), 0u);  // volatile
  deliver(2, ae(1, 2, 2, 1, {}, 2));
  EXPECT_EQ(raft->commit_index(), 2u);
  EXPECT_EQ(raft->take_committed().size(), 2u);  // replayed from index 1
}

// ===========================================================================
// Leader: no-op, propose, commit rule (L2, L5)
// ===========================================================================

TEST_F(RaftLogTest, NewLeaderAppendsNoOpInItsTerm) {
  make();
  become_leader();
  ASSERT_EQ(storage.last_index(), 1u);
  const LogEntry noop = storage.entries(1, 2)[0];
  EXPECT_EQ(noop.type, EntryType::NoOp);
  EXPECT_EQ(noop.term, raft->term());
}

TEST_F(RaftLogTest, OnlyLeaderAcceptsProposals) {
  make();
  EXPECT_FALSE(raft->propose("x").has_value());
  become_leader();
  auto idx = raft->propose("x");
  ASSERT_TRUE(idx.has_value());
  EXPECT_EQ(*idx, 2u);  // after the no-op
  // Replicated to both followers right away.
  EXPECT_EQ(transport.sent_of<AppendEntriesReq>(2).back().entries.back().data,
            "x");
  EXPECT_EQ(transport.sent_of<AppendEntriesReq>(3).back().entries.back().data,
            "x");
}

TEST_F(RaftLogTest, CommitsOnceMajorityHasEntry) {
  make();
  become_leader();
  raft->propose("x");
  EXPECT_EQ(raft->commit_index(), 0u);

  deliver(2, ack(raft->term(), 2));  // self + node 2 = 2 of 3
  EXPECT_EQ(raft->commit_index(), 2u);
  auto applied = raft->take_committed();
  ASSERT_EQ(applied.size(), 2u);
  EXPECT_EQ(applied[0].type, EntryType::NoOp);
  EXPECT_EQ(applied[1].data, "x");
}

TEST_F(RaftLogTest, NoCommitWithoutMajority) {
  make({2, 3, 4, 5});  // quorum 3
  become_leader();
  raft->propose("x");
  deliver(2, ack(raft->term(), 2));  // self + 1 = 2 of 5
  EXPECT_EQ(raft->commit_index(), 0u);
  deliver(3, ack(raft->term(), 2));
  EXPECT_EQ(raft->commit_index(), 2u);
}

TEST_F(RaftLogTest, DoesNotCommitOldTermEntryByCountingReplicas) {
  // Figure 8: index 1 is from term 1. As term-2 leader, seeing it on a
  // majority is NOT enough to commit it. It commits only together with an
  // entry from term 2 (here, the no-op at index 2).
  seed_log({entry(1, 1, "old")}, 1);
  make();
  become_leader();
  ASSERT_EQ(raft->term(), 2u);

  deliver(2, ack(2, 1));  // node 2 has index 1: majority, but old term
  EXPECT_EQ(raft->commit_index(), 0u);

  deliver(2, ack(2, 2));  // node 2 now has the term-2 no-op too
  EXPECT_EQ(raft->commit_index(), 2u);  // commits 1 and 2 together
}

TEST_F(RaftLogTest, OutOfOrderAcksNeverMoveMatchBackwards) {
  make({2, 3, 4, 5});
  become_leader();
  raft->propose("x");
  deliver(2, ack(raft->term(), 2));
  deliver(2, ack(raft->term(), 1));  // older ack arrives late
  deliver(3, ack(raft->term(), 2));
  EXPECT_EQ(raft->commit_index(), 2u);  // node 2 still counted at 2
}

TEST_F(RaftLogTest, SingleNodeCommitsImmediately) {
  make({});
  clock.advance(300ms);
  raft->tick();
  ASSERT_EQ(raft->role(), Role::Leader);
  raft->propose("x");
  EXPECT_EQ(raft->commit_index(), 2u);
}

// ===========================================================================
// Leader: catching up a lagging follower
// ===========================================================================

TEST_F(RaftLogTest, RejectionBacksUpStraightToFollowersLog) {
  seed_log({entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4), entry(1, 5)},
           1);
  make();
  become_leader();  // no-op at index 6; first probe uses prev = 5

  deliver(2, nack(raft->term(), /*rejected=*/5, /*follower_last=*/2));

  auto sent = transport.sent_of<AppendEntriesReq>(2);
  ASSERT_FALSE(sent.empty());
  EXPECT_EQ(sent.back().prev_log_index, 2u);  // jumped, not 5 -> 4 -> 3
  ASSERT_EQ(sent.back().entries.size(), 4u);  // indices 3..6
  EXPECT_EQ(sent.back().entries.front().index, 3u);
}

TEST_F(RaftLogTest, IgnoresStaleRejection) {
  seed_log({entry(1, 1), entry(1, 2), entry(1, 3)}, 1);
  make();
  become_leader();  // probing node 2 at prev = 3

  deliver(2, nack(raft->term(), /*rejected=*/1, /*follower_last=*/0));
  EXPECT_TRUE(transport.sent_of<AppendEntriesReq>(2).empty());
}

TEST_F(RaftLogTest, HeartbeatRetriesLaggingFollower) {
  make();
  become_leader();
  raft->propose("x");
  transport.clear();
  // No ack from node 3. The next heartbeat must carry the entries again.
  clock.advance(50ms);
  raft->tick();
  auto sent = transport.sent_of<AppendEntriesReq>(3);
  ASSERT_EQ(sent.size(), 1u);
  EXPECT_EQ(sent[0].entries.size(), 2u);  // no-op and "x"
}

// ===========================================================================
// Voting with real logs (L6)
// ===========================================================================

TEST_F(RaftLogTest, VoteDeniedToCandidateWithOlderLastTerm) {
  seed_log({entry(1, 1), entry(2, 2)}, 2);
  make();
  // Longer log, but its last term is older: not up to date.
  deliver(3, RequestVoteReq{3, 3, /*last_index=*/5, /*last_term=*/1, false});
  EXPECT_FALSE(transport.sent_of<RequestVoteResp>().back().vote_granted);
}

TEST_F(RaftLogTest, VoteDeniedToCandidateWithShorterLogSameTerm) {
  seed_log({entry(1, 1), entry(2, 2)}, 2);
  make();
  deliver(3, RequestVoteReq{3, 3, /*last_index=*/1, /*last_term=*/2, false});
  EXPECT_FALSE(transport.sent_of<RequestVoteResp>().back().vote_granted);
}

TEST_F(RaftLogTest, VoteGrantedToCandidateAtLeastAsUpToDate) {
  seed_log({entry(1, 1), entry(2, 2)}, 2);
  make();
  deliver(3, RequestVoteReq{3, 3, /*last_index=*/2, /*last_term=*/2, false});
  EXPECT_TRUE(transport.sent_of<RequestVoteResp>().back().vote_granted);
}

}  // namespace
