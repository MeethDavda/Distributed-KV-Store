#pragma once

// The Raft node: a single-threaded, deterministic state machine.
//
// Inputs:  step(msg) for every inbound message, tick() to let time pass,
//          propose(data) for new client commands (leader only).
// Outputs: messages via Transport, durable writes via Storage, and committed
//          entries via take_committed() for the state machine to apply.
// It never blocks, never sleeps, and never reads the system clock directly.
//
// Scope so far: leader election (pre-vote, leader stickiness, check-quorum)
// and log replication with the commit rule.

#include <cstdint>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "rafty/interfaces.hpp"
#include "rafty/storage.hpp"
#include "rafty/types.hpp"

namespace rafty {

struct Config {
  NodeId id = kNoNode;
  std::vector<NodeId> peers;  // every OTHER node in the cluster

  // Election timeout is chosen uniformly in [min, max) after every reset.
  // Randomization makes split votes unlikely (liveness).
  Millis election_timeout_min{150};
  Millis election_timeout_max{300};
  // Must be well below election_timeout_min, or followers time out on a
  // healthy leader.
  Millis heartbeat_interval{50};

  bool pre_vote = true;
  bool check_quorum = true;

  // Cap on entries per AppendEntries, so a follower that is far behind is
  // caught up in bounded-size messages.
  std::size_t max_entries_per_msg = 64;

  // Seeds the election-timeout RNG. Tests set this so runs are replayable.
  uint64_t seed = 1;
};

class Raft {
 public:
  // Loads HardState from storage and starts as a follower. The log is
  // already in storage; commit_index starts at 0 and is relearned from the
  // leader (it is volatile state).
  Raft(Config cfg, Transport& transport, Storage& storage, const Clock& clock);

  // Called periodically by the driver (more often than heartbeat_interval).
  // Fires election timeouts, heartbeats, and check-quorum.
  void tick();

  // Delivers one inbound message.
  void step(const Message& msg);

  // Leader only: appends a client command to the log and starts replicating
  // it. Returns the entry's index, or nullopt if this node is not leader.
  // The index is NOT committed yet; the caller learns that via
  // take_committed(). An entry can still be lost if leadership changes.
  std::optional<Index> propose(std::string data);

  // Returns committed entries not yet handed out, in log order, and marks
  // them applied. The driver feeds these to the state machine.
  std::vector<LogEntry> take_committed();

  // Read-only accessors for drivers and tests.
  NodeId id() const { return cfg_.id; }
  Role role() const { return role_; }
  Term term() const { return hs_.current_term; }
  NodeId voted_for() const { return hs_.voted_for; }
  NodeId leader_id() const { return leader_id_; }
  Index commit_index() const { return commit_index_; }
  Index last_applied() const { return last_applied_; }
  Index last_log_index() const { return storage_.last_index(); }
  Term last_log_term() const { return storage_.term_at(last_log_index()); }

 private:
  // Leader's view of one follower's log.
  struct Progress {
    Index next = 1;   // next index to send
    Index match = 0;  // highest index known to be replicated on the follower
  };

  // ---- role transitions ----
  void become_follower(Term term, NodeId leader);
  void become_pre_candidate();
  void become_candidate();
  void become_leader();

  // ---- message handlers ----
  void handle_request_vote(NodeId from, const RequestVoteReq& req);
  void handle_request_vote_resp(NodeId from, const RequestVoteResp& resp);
  void handle_append_entries(NodeId from, const AppendEntriesReq& req);
  void handle_append_entries_resp(NodeId from, const AppendEntriesResp& resp);

  // ---- replication ----
  void send_append(NodeId peer);   // entries from progress_[peer].next
  void broadcast_append();         // send_append to every peer
  void append_to_own_log(EntryType type, std::string data);  // leader only
  void maybe_advance_commit();     // leader: apply the commit rule
  // Follower: merge req.entries into our log; returns index of last new entry.
  Index merge_entries(const AppendEntriesReq& req);

  // ---- helpers ----
  void broadcast_request_vote(bool pre_vote);
  void send(NodeId to, Payload payload);
  void persist();  // writes hs_ through Storage; durable on return

  void reset_election_timer();
  bool in_leader_lease() const;  // heard from a live leader recently?
  void check_quorum();           // leader steps down if cut off from majority

  std::size_t quorum() const { return (cfg_.peers.size() + 1) / 2 + 1; }
  bool candidate_log_up_to_date(Index last_index, Term last_term) const;

  // ---- dependencies ----
  Config cfg_;
  Transport& transport_;
  Storage& storage_;
  const Clock& clock_;
  std::mt19937_64 rng_;

  // ---- persistent state (mirrors Storage; the log lives only in Storage) ----
  HardState hs_;

  // ---- volatile state ----
  Role role_ = Role::Follower;
  NodeId leader_id_ = kNoNode;
  std::set<NodeId> votes_;  // granted votes (pre or real) in this round

  Index commit_index_ = 0;  // highest index known committed; never decreases
  Index last_applied_ = 0;  // highest index handed out by take_committed()

  Millis election_deadline_{0};       // follower/candidate: start election
  Millis next_heartbeat_{0};          // leader: send heartbeats
  Millis last_leader_contact_{0};     // for leader stickiness
  Millis next_quorum_check_{0};       // leader: check-quorum window end
  std::map<NodeId, Millis> last_ack_; // leader: last response per peer

  std::map<NodeId, Progress> progress_;  // leader only, reset on election
};

}  // namespace rafty
