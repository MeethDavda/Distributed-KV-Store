#pragma once

// The Raft node: a single-threaded, deterministic state machine.
//
// Inputs:  step(msg) for every inbound message, tick() to let time pass.
// Outputs: messages via Transport, durable writes via Storage.
// It never blocks, never sleeps, and never reads the system clock directly.
//
// Step 1 scope: leader election with pre-vote, leader stickiness, and
// check-quorum. Log replication is added in step 2.

#include <cstdint>
#include <map>
#include <random>
#include <set>
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

  // Seeds the election-timeout RNG. Tests set this so runs are replayable.
  uint64_t seed = 1;
};

class Raft {
 public:
  // Loads HardState from storage and starts as a follower.
  Raft(Config cfg, Transport& transport, Storage& storage, const Clock& clock);

  // Called periodically by the driver (more often than heartbeat_interval).
  // Fires election timeouts, heartbeats, and check-quorum.
  void tick();

  // Delivers one inbound message.
  void step(const Message& msg);

  // Read-only accessors for drivers and tests.
  NodeId id() const { return cfg_.id; }
  Role role() const { return role_; }
  Term term() const { return hs_.current_term; }
  NodeId voted_for() const { return hs_.voted_for; }
  NodeId leader_id() const { return leader_id_; }

 private:
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

  // ---- helpers ----
  void broadcast_request_vote(bool pre_vote);
  void broadcast_heartbeat();
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

  // ---- persistent state (mirrors Storage) ----
  HardState hs_;

  // ---- volatile state ----
  Role role_ = Role::Follower;
  NodeId leader_id_ = kNoNode;
  std::set<NodeId> votes_;  // granted votes (pre or real) in this round

  Millis election_deadline_{0};       // follower/candidate: start election
  Millis next_heartbeat_{0};          // leader: send heartbeats
  Millis last_leader_contact_{0};     // for leader stickiness
  Millis next_quorum_check_{0};       // leader: check-quorum window end
  std::map<NodeId, Millis> last_ack_; // leader: last response per peer
};

}  // namespace rafty
