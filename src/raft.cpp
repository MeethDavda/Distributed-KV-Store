#include "rafty/raft.hpp"

#include <cstdio>
#include <cstdlib>
#include <utility>
#include <variant>

namespace rafty {

namespace {

// Lets std::visit take one lambda per variant alternative.
template <class... Fs>
struct Overloaded : Fs... {
  using Fs::operator()...;
};
template <class... Fs>
Overloaded(Fs...) -> Overloaded<Fs...>;

// Gives each node a different RNG stream from the same test seed, so nodes
// pick different election timeouts but the whole run is still replayable.
uint64_t mix_seed(uint64_t seed, NodeId id) {
  return seed * 0x9E3779B97F4A7C15ULL + id;
}

}  // namespace

// ===========================================================================
// Construction
// ===========================================================================

Raft::Raft(Config cfg, Transport& transport, Storage& storage,
           const Clock& clock)
    : cfg_(std::move(cfg)),
      transport_(transport),
      storage_(storage),
      clock_(clock),
      rng_(mix_seed(cfg_.seed, cfg_.id)) {
  // Restart recovery: term and vote come back from durable storage, so a
  // restarted node cannot vote twice in a term it already voted in.
  hs_ = storage_.load_hard_state();
  // Every node starts as a follower, even one that was leader before a crash.
  become_follower(hs_.current_term, kNoNode);
}

// ===========================================================================
// Driver entry points
// ===========================================================================

void Raft::tick() {
  const Millis now = clock_.now();

  if (role_ == Role::Leader) {
    if (now >= next_heartbeat_) {
      broadcast_heartbeat();
      next_heartbeat_ = now + cfg_.heartbeat_interval;
    }
    if (cfg_.check_quorum && now >= next_quorum_check_) {
      check_quorum();
    }
    return;
  }

  // Follower, PreCandidate, or Candidate: no leader heard from in time.
  if (now >= election_deadline_) {
    if (cfg_.pre_vote) {
      become_pre_candidate();
    } else {
      become_candidate();
    }
  }
}

void Raft::step(const Message& msg) {
  if (msg.to != cfg_.id || msg.from == cfg_.id) return;  // misrouted

  std::visit(
      Overloaded{
          [&](const RequestVoteReq& m) { handle_request_vote(msg.from, m); },
          [&](const RequestVoteResp& m) {
            handle_request_vote_resp(msg.from, m);
          },
          [&](const AppendEntriesReq& m) {
            handle_append_entries(msg.from, m);
          },
          [&](const AppendEntriesResp& m) {
            handle_append_entries_resp(msg.from, m);
          },
      },
      msg.payload);
}

// ===========================================================================
// Role transitions
// ===========================================================================

void Raft::become_follower(Term term, NodeId leader) {
  // Term monotonicity: callers only ever pass a term >= ours.
  if (term > hs_.current_term) {
    // New term: the old vote belonged to the old term, so clear it.
    hs_.current_term = term;
    hs_.voted_for = kNoNode;
    persist();
  }
  role_ = Role::Follower;
  leader_id_ = leader;
  votes_.clear();
  if (leader != kNoNode) last_leader_contact_ = clock_.now();
  reset_election_timer();
}

void Raft::become_pre_candidate() {
  // Pre-vote: ask for votes at term + 1 WITHOUT changing our own term or
  // vote. If we are partitioned and cannot win, nobody's term is disturbed.
  role_ = Role::PreCandidate;
  leader_id_ = kNoNode;
  votes_ = {cfg_.id};
  reset_election_timer();
  broadcast_request_vote(/*pre_vote=*/true);

  if (votes_.size() >= quorum()) become_candidate();  // single-node cluster
}

void Raft::become_candidate() {
  // Real election: new term, vote for self, and persist BEFORE asking others,
  // so a crash cannot make us forget we already voted in this term.
  hs_.current_term += 1;
  hs_.voted_for = cfg_.id;
  persist();

  role_ = Role::Candidate;
  leader_id_ = kNoNode;
  votes_ = {cfg_.id};
  reset_election_timer();
  broadcast_request_vote(/*pre_vote=*/false);

  if (votes_.size() >= quorum()) become_leader();  // single-node cluster
}

void Raft::become_leader() {
  const Millis now = clock_.now();
  role_ = Role::Leader;
  leader_id_ = cfg_.id;
  votes_.clear();
  last_ack_.clear();

  // Announce leadership right away so other candidates step down quickly.
  broadcast_heartbeat();
  next_heartbeat_ = now + cfg_.heartbeat_interval;
  next_quorum_check_ = now + cfg_.election_timeout_max;
}

// ===========================================================================
// Message handlers
// ===========================================================================

void Raft::handle_request_vote(NodeId from, const RequestVoteReq& req) {
  auto reject = [&] {
    send(from, RequestVoteResp{hs_.current_term, false, req.pre_vote});
  };

  // Leader stickiness: while we are hearing from a live leader, refuse to
  // help anyone replace it. Checked before the term rule so a disruptive
  // candidate cannot even bump our term.
  if (in_leader_lease()) {
    reject();
    return;
  }

  if (req.pre_vote) {
    // Pre-vote never changes our state: no term update, no recorded vote.
    // Grant only if the candidate's proposed term is newer than ours and its
    // log is at least as up to date as ours.
    const bool grant = req.term > hs_.current_term &&
                       candidate_log_up_to_date(req.last_log_index,
                                                req.last_log_term);
    if (grant) {
      send(from, RequestVoteResp{req.term, true, true});
    } else {
      reject();
    }
    return;
  }

  // Term rule: stale candidate -> reject and tell it our term.
  if (req.term < hs_.current_term) {
    reject();
    return;
  }
  // Term rule: newer term -> adopt it (clears our vote), then decide.
  if (req.term > hs_.current_term) become_follower(req.term, kNoNode);

  // Election Safety: at most one vote per term. Re-granting to the same
  // candidate is fine (its first reply may have been lost).
  const bool can_vote =
      hs_.voted_for == kNoNode || hs_.voted_for == req.candidate_id;
  const bool grant =
      can_vote &&
      candidate_log_up_to_date(req.last_log_index, req.last_log_term);

  if (grant) {
    hs_.voted_for = req.candidate_id;
    persist();               // durable BEFORE the reply leaves this node
    reset_election_timer();  // we just backed a candidate; give it time
  }
  send(from, RequestVoteResp{hs_.current_term, grant, false});
}

void Raft::handle_request_vote_resp(NodeId from,
                                    const RequestVoteResp& resp) {
  if (resp.pre_vote) {
    if (role_ != Role::PreCandidate) return;  // stale reply
    // A rejection carries the responder's real term. If it is ahead of us,
    // we are behind: catch up instead of campaigning.
    if (!resp.vote_granted && resp.term > hs_.current_term) {
      become_follower(resp.term, kNoNode);
      return;
    }
    // Count only grants for the term we are actually proposing.
    if (resp.vote_granted && resp.term == hs_.current_term + 1) {
      votes_.insert(from);
      if (votes_.size() >= quorum()) become_candidate();
    }
    return;
  }

  // Term rule.
  if (resp.term > hs_.current_term) {
    become_follower(resp.term, kNoNode);
    return;
  }
  if (role_ != Role::Candidate || resp.term != hs_.current_term) return;

  if (resp.vote_granted) {
    votes_.insert(from);  // a set, so duplicate replies count once
    if (votes_.size() >= quorum()) become_leader();
  }
}

void Raft::handle_append_entries(NodeId from, const AppendEntriesReq& req) {
  // Term rule: a stale leader learns our term from the rejection and steps
  // down.
  if (req.term < hs_.current_term) {
    send(from, AppendEntriesResp{hs_.current_term, false});
    return;
  }

  // Election Safety check: two leaders in the same term must be impossible.
  // If this ever fires, the voting logic is broken, so fail loudly.
  if (role_ == Role::Leader && req.term == hs_.current_term) {
    std::fprintf(stderr,
                 "FATAL: election safety violated: nodes %llu and %llu both "
                 "lead term %llu\n",
                 static_cast<unsigned long long>(cfg_.id),
                 static_cast<unsigned long long>(from),
                 static_cast<unsigned long long>(req.term));
    std::abort();
  }

  // A valid leader exists for term >= ours. Candidates and pre-candidates in
  // this term lost the election; everyone resets their election timer.
  become_follower(req.term, from);
  send(from, AppendEntriesResp{hs_.current_term, true});
}

void Raft::handle_append_entries_resp(NodeId from,
                                      const AppendEntriesResp& resp) {
  // Term rule: a follower is in a newer term, so we are no longer leader.
  if (resp.term > hs_.current_term) {
    become_follower(resp.term, kNoNode);
    return;
  }
  if (role_ != Role::Leader) return;
  last_ack_[from] = clock_.now();  // proof this peer is reachable
}

// ===========================================================================
// Helpers
// ===========================================================================

void Raft::broadcast_request_vote(bool pre_vote) {
  // Pre-vote asks about the term we WOULD use; a real vote uses our new term.
  const Term term = pre_vote ? hs_.current_term + 1 : hs_.current_term;
  // Step 1: the log is empty, so last index and term are 0. Step 2 fills
  // these from the real log.
  for (NodeId peer : cfg_.peers) {
    send(peer, RequestVoteReq{term, cfg_.id, /*last_log_index=*/0,
                              /*last_log_term=*/0, pre_vote});
  }
}

void Raft::broadcast_heartbeat() {
  for (NodeId peer : cfg_.peers) {
    send(peer, AppendEntriesReq{hs_.current_term, cfg_.id});
  }
}

void Raft::send(NodeId to, Payload payload) {
  transport_.send(Message{cfg_.id, to, std::move(payload)});
}

void Raft::persist() { storage_.save_hard_state(hs_); }

void Raft::reset_election_timer() {
  std::uniform_int_distribution<Millis::rep> dist(
      cfg_.election_timeout_min.count(), cfg_.election_timeout_max.count() - 1);
  election_deadline_ = clock_.now() + Millis{dist(rng_)};
}

bool Raft::in_leader_lease() const {
  // Only trusted when check-quorum is on: then a leader that loses its
  // majority steps down, so "I heard from a leader recently" really means
  // "a healthy leader exists".
  if (!cfg_.check_quorum) return false;
  if (role_ == Role::Leader) return true;
  // leader_id_ guard: on a fresh node last_leader_contact_ is 0 and the fake
  // clock may also be at 0, which would look like a lease that never existed.
  return leader_id_ != kNoNode &&
         clock_.now() - last_leader_contact_ < cfg_.election_timeout_min;
}

void Raft::check_quorum() {
  const Millis now = clock_.now();
  std::size_t active = 1;  // count ourselves
  for (NodeId peer : cfg_.peers) {
    auto it = last_ack_.find(peer);
    if (it != last_ack_.end() && now - it->second < cfg_.election_timeout_max) {
      ++active;
    }
  }
  if (active < quorum()) {
    // Cut off from the majority: stop acting as leader. Same term, no leader.
    become_follower(hs_.current_term, kNoNode);
    return;
  }
  next_quorum_check_ = now + cfg_.election_timeout_max;
}

bool Raft::candidate_log_up_to_date(Index last_index, Term last_term) const {
  // Raft 5.4.1: compare last log terms first, then lengths.
  // Step 1: our log is empty (index 0, term 0), so every candidate passes.
  constexpr Index kMyLastIndex = 0;
  constexpr Term kMyLastTerm = 0;
  if (last_term != kMyLastTerm) return last_term > kMyLastTerm;
  return last_index >= kMyLastIndex;
}

}  // namespace rafty
