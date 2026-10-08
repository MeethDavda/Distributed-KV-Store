#include "rafty/raft.hpp"

#include <algorithm>
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

// A broken safety invariant means the algorithm is wrong; continuing would
// corrupt data. Fail loudly so tests cannot miss it.
[[noreturn]] void fatal(const char* what, NodeId self, uint64_t a,
                        uint64_t b) {
  std::fprintf(stderr, "FATAL on node %llu: %s (%llu, %llu)\n",
               static_cast<unsigned long long>(self), what,
               static_cast<unsigned long long>(a),
               static_cast<unsigned long long>(b));
  std::abort();
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
  // restarted node cannot vote twice in a term it already voted in. The log
  // is read from storage on demand. commit_index_ and last_applied_ start at
  // 0: the state machine is rebuilt by re-applying committed entries as the
  // leader re-announces its commit index.
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
      broadcast_append();  // heartbeat, plus any entries a follower is missing
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

std::optional<Index> Raft::propose(std::string data) {
  if (role_ != Role::Leader) return std::nullopt;
  append_to_own_log(EntryType::Normal, std::move(data));
  const Index index = last_log_index();
  broadcast_append();
  maybe_advance_commit();  // single-node cluster commits immediately
  return index;
}

std::vector<LogEntry> Raft::take_committed() {
  if (last_applied_ >= commit_index_) return {};
  // Only committed entries are handed out, so the state machine never sees
  // an entry that could later be truncated (State Machine Safety).
  auto out = storage_.entries(last_applied_ + 1, commit_index_ + 1);
  last_applied_ = commit_index_;
  return out;
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

  // Optimistic start: assume every follower matches our whole log. Wrong
  // guesses are fixed by rejections. match starts at 0 because nothing is
  // PROVEN replicated yet, and commit decisions use only match.
  progress_.clear();
  for (NodeId peer : cfg_.peers) {
    progress_[peer] = Progress{last_log_index() + 1, 0};
  }

  // No-op in our own term (Raft thesis 6.4). The commit rule only lets a
  // leader commit entries from its current term, so without this, entries
  // left over from earlier terms could stay uncommitted until a client
  // happened to write.
  append_to_own_log(EntryType::NoOp, "");

  broadcast_append();  // also announces leadership
  next_heartbeat_ = now + cfg_.heartbeat_interval;
  next_quorum_check_ = now + cfg_.election_timeout_max;
  maybe_advance_commit();  // single-node cluster
}

// ===========================================================================
// Message handlers: elections
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
  // Leader Completeness: only vote for a log at least as up to date as ours,
  // so a winner always holds every committed entry.
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

// ===========================================================================
// Message handlers: replication
// ===========================================================================

void Raft::handle_append_entries(NodeId from, const AppendEntriesReq& req) {
  auto reject = [&] {
    AppendEntriesResp r;
    r.term = hs_.current_term;
    r.success = false;
    r.rejected_index = req.prev_log_index;
    r.last_log_index = last_log_index();
    send(from, r);
  };

  // Term rule: a stale leader learns our term from the rejection and steps
  // down.
  if (req.term < hs_.current_term) {
    reject();
    return;
  }

  // Election Safety check: two leaders in the same term must be impossible.
  if (role_ == Role::Leader && req.term == hs_.current_term) {
    fatal("election safety violated: two leaders in one term", cfg_.id,
          from, req.term);
  }

  // A valid leader exists for term >= ours. Candidates and pre-candidates in
  // this term lost the election; everyone resets their election timer.
  become_follower(req.term, from);

  // Log Matching consistency check: we must hold the entry the new entries
  // follow. If not, reject; the leader will back up and retry.
  if (req.prev_log_index > last_log_index() ||
      storage_.term_at(req.prev_log_index) != req.prev_log_term) {
    reject();
    return;
  }

  const Index last_new = merge_entries(req);  // durable before the ack below

  // Commit rule (follower side): commit up to what the leader says, but no
  // further than the entries THIS message proved match the leader. Anything
  // past last_new may be stale entries from an old term. commit_index_
  // never decreases, even if this message is old.
  const Index new_commit = std::min(req.leader_commit, last_new);
  if (new_commit > commit_index_) commit_index_ = new_commit;

  AppendEntriesResp ok;
  ok.term = hs_.current_term;
  ok.success = true;
  ok.match_index = last_new;
  send(from, ok);
}

Index Raft::merge_entries(const AppendEntriesReq& req) {
  const auto& in = req.entries;
  for (std::size_t i = 0; i < in.size(); ++i) {
    const Index idx = in[i].index;

    if (idx <= last_log_index()) {
      // Already have an entry here. Same term means same entry (Log
      // Matching), so skip it. This is what stops a delayed or duplicated
      // AppendEntries from truncating entries that arrived after it.
      if (storage_.term_at(idx) == in[i].term) continue;

      // Real conflict: our entry came from a leader that lost. Committed
      // entries can never conflict (Leader Completeness); if one does, the
      // algorithm is broken.
      if (idx <= commit_index_) {
        fatal("conflict at a committed index", cfg_.id, idx, commit_index_);
      }
      storage_.truncate_from(idx);
    }

    // Everything from i on is new: append it in one durable write.
    storage_.append({in.begin() + static_cast<long>(i), in.end()});
    break;
  }
  return req.prev_log_index + in.size();
}

void Raft::handle_append_entries_resp(NodeId from,
                                      const AppendEntriesResp& resp) {
  // Term rule: a follower is in a newer term, so we are no longer leader.
  if (resp.term > hs_.current_term) {
    become_follower(resp.term, kNoNode);
    return;
  }
  // Ignore replies from older terms and anything after we stopped leading.
  if (role_ != Role::Leader || resp.term != hs_.current_term) return;

  last_ack_[from] = clock_.now();  // proof this peer is reachable
  Progress& p = progress_[from];

  if (resp.success) {
    // match_index is absolute, so out-of-order replies are safe: keep max.
    if (resp.match_index > p.match) {
      p.match = resp.match_index;
      p.next = std::max(p.next, p.match + 1);
      maybe_advance_commit();
      // Still behind? Keep streaming instead of waiting for the heartbeat.
      if (p.next <= last_log_index()) send_append(from);
    }
    return;
  }

  // Rejection. Ignore it unless it answers the probe we are making now;
  // a stale rejection would otherwise push next backwards for nothing.
  if (resp.rejected_index != p.next - 1) return;

  // Back up. Jump straight past the follower's last entry if it is far
  // behind, but never below what we already know matches.
  p.next = std::min(p.next - 1, resp.last_log_index + 1);
  p.next = std::max(p.next, p.match + 1);
  send_append(from);
}

// ===========================================================================
// Replication helpers
// ===========================================================================

void Raft::append_to_own_log(EntryType type, std::string data) {
  // Leader Append-Only: a leader only ever adds to the end of its log.
  LogEntry e;
  e.term = hs_.current_term;
  e.index = last_log_index() + 1;
  e.type = type;
  e.data = std::move(data);
  storage_.append({std::move(e)});  // durable before anyone is told about it
}

void Raft::send_append(NodeId peer) {
  Progress& p = progress_[peer];
  const Index last = last_log_index();
  p.next = std::min(p.next, last + 1);

  AppendEntriesReq req;
  req.term = hs_.current_term;
  req.leader_id = cfg_.id;
  req.prev_log_index = p.next - 1;
  req.prev_log_term = storage_.term_at(req.prev_log_index);
  const Index hi = std::min<Index>(last + 1, p.next + cfg_.max_entries_per_msg);
  if (p.next < hi) req.entries = storage_.entries(p.next, hi);
  req.leader_commit = commit_index_;
  send(peer, std::move(req));
}

void Raft::broadcast_append() {
  for (NodeId peer : cfg_.peers) send_append(peer);
}

void Raft::maybe_advance_commit() {
  if (role_ != Role::Leader) return;

  // Commit rule (Raft 5.4.2): commit N only if a majority stores N AND
  // log[N].term == currentTerm. Counting replicas of an OLDER-term entry is
  // not enough: that entry can still be overwritten by a later leader (the
  // paper's Figure 8). Older entries commit implicitly once a current-term
  // entry after them commits.
  //
  // Terms in a log never decrease, so scan down from the end and stop at the
  // first entry from an older term.
  for (Index n = last_log_index(); n > commit_index_; --n) {
    if (storage_.term_at(n) != hs_.current_term) break;
    std::size_t replicas = 1;  // ourselves: the leader's log has n
    for (const auto& [peer, p] : progress_) {
      if (p.match >= n) ++replicas;
    }
    if (replicas >= quorum()) {
      commit_index_ = n;
      return;
    }
  }
}

// ===========================================================================
// Election helpers
// ===========================================================================

void Raft::broadcast_request_vote(bool pre_vote) {
  // Pre-vote asks about the term we WOULD use; a real vote uses our new term.
  const Term term = pre_vote ? hs_.current_term + 1 : hs_.current_term;
  for (NodeId peer : cfg_.peers) {
    send(peer, RequestVoteReq{term, cfg_.id, last_log_index(), last_log_term(),
                              pre_vote});
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
  // Raft 5.4.1: the log whose last entry has the higher term is more up to
  // date; if the terms are equal, the longer log is.
  const Term my_term = last_log_term();
  if (last_term != my_term) return last_term > my_term;
  return last_index >= last_log_index();
}

}  // namespace rafty
