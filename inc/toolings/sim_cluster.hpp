#pragma once

// Test driver: runs an N-node Raft cluster in one process on a FakeClock and
// SimNetwork, and checks safety invariants after every simulated step.
//
// Typical test:
//   SimCluster c(5, seed);
//   ASSERT_TRUE(c.run_until([&] { return c.leader().has_value(); }, 2s));
//   c.propose("x");
//   c.net().partition({{1, 2}, {3, 4, 5}});
//   c.run_for(5s);
//   EXPECT_TRUE(c.violations().empty()) << "seed=" << c.seed();
//
// Invariants checked:
//   every step:   Election Safety, term monotonicity, commit monotonicity,
//                 State Machine Safety (no two nodes apply different entries
//                 at one index), in-order apply
//   new leader:   Leader Completeness (it holds every committed entry)
//   periodically: Log Matching across every pair of nodes

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "rafty/raft.hpp"
#include "rafty/storage.hpp"
#include "toolings/sim_network.hpp"

namespace toolings {

using rafty::Config;
using rafty::Index;
using rafty::LogEntry;
using rafty::MemStorage;
using rafty::Raft;
using rafty::Role;
using rafty::Term;

class SimCluster {
 public:
  // Nodes get ids 1..n. `base` supplies timeouts and feature flags; id,
  // peers, and seed are filled in per node.
  SimCluster(int n, uint64_t seed, Config base = {})
      : seed_(seed), base_(std::move(base)), net_(clock_, seed) {
    for (NodeId id = 1; id <= static_cast<NodeId>(n); ++id) ids_.push_back(id);
    for (NodeId id : ids_) {
      storage_[id] = std::make_unique<MemStorage>();
      start_node(id);
    }
  }

  // ---- Driving time ----

  // One simulated step: advance time, deliver due messages, tick every live
  // node, collect newly committed entries, then check invariants.
  void step(Millis dt = Millis{1}) {
    clock_.advance(dt);
    for (const Message& m : net_.take_due()) {
      if (auto& node = nodes_[m.to]) node->step(m);
    }
    for (NodeId id : ids_) {
      if (auto& node = nodes_[id]) node->tick();
    }
    collect_applied();
    check_invariants();
    if (++steps_ % kLogMatchingEvery == 0) check_log_matching();
  }

  void run_for(Millis d) {
    const Millis end = clock_.now() + d;
    while (clock_.now() < end) step();
  }

  // Steps until pred() is true or `timeout` of simulated time passes.
  bool run_until(const std::function<bool()>& pred, Millis timeout) {
    const Millis end = clock_.now() + timeout;
    while (clock_.now() < end) {
      if (pred()) return true;
      step();
    }
    return pred();
  }

  // ---- Client writes ----

  // Proposes on the current leader. Returns the log index it was given, or
  // nullopt if there is no leader. Not committed until it shows up in
  // committed().
  std::optional<Index> propose(const std::string& data) {
    auto l = leader();
    if (!l) return std::nullopt;
    return nodes_[*l]->propose(data);
  }

  // Proposes on a specific node (tests use this to write to a stale leader).
  std::optional<Index> propose_on(NodeId id, const std::string& data) {
    auto& node = nodes_[id];
    return node ? node->propose(data) : std::nullopt;
  }

  // ---- Faults ----

  // Process dies: the Raft object (all volatile state) is destroyed, but its
  // MemStorage survives, like a disk would.
  void crash(NodeId id) {
    net_.set_down(id, true);
    nodes_[id].reset();
  }

  // Process comes back and recovers term, vote, and log from its storage.
  // Its commit index and applied state start over (they are volatile).
  void restart(NodeId id) {
    net_.set_down(id, false);
    last_commit_.erase(id);
    applied_.erase(id);
    start_node(id);
  }

  // ---- Inspection ----

  // The live leader with the highest term, if any. During a partition a
  // stale leader may still exist in a lower term; this returns the newest.
  std::optional<NodeId> leader() const {
    std::optional<NodeId> best;
    Term best_term = 0;
    for (NodeId id : ids_) {
      const Raft* r = node(id);
      if (r && r->role() == Role::Leader && (!best || r->term() > best_term)) {
        best = id;
        best_term = r->term();
      }
    }
    return best;
  }

  // Number of live nodes that currently believe they are leader.
  int leader_count() const {
    int n = 0;
    for (NodeId id : ids_) {
      const Raft* r = node(id);
      if (r && r->role() == Role::Leader) ++n;
    }
    return n;
  }

  const Raft* node(NodeId id) const {
    auto it = nodes_.find(id);
    return it == nodes_.end() ? nullptr : it->second.get();
  }

  // Every entry any node has applied, by index. Because of State Machine
  // Safety this is the single agreed history of the cluster.
  const std::map<Index, LogEntry>& committed() const { return committed_; }

  // Entries node `id` has applied since it last (re)started, in order.
  const std::vector<LogEntry>& applied(NodeId id) { return applied_[id]; }

  // True if `data` was committed (as a Normal entry) at any index.
  bool is_committed(const std::string& data) const {
    for (const auto& [idx, e] : committed_) {
      if (e.type == rafty::EntryType::Normal && e.data == data) return true;
    }
    return false;
  }

  const std::vector<NodeId>& ids() const { return ids_; }
  const MemStorage& storage(NodeId id) const { return *storage_.at(id); }
  SimNetwork& net() { return net_; }
  FakeClock& clock() { return clock_; }
  uint64_t seed() const { return seed_; }

  // Every invariant violation seen so far. Tests assert this is empty and
  // print seed() on failure so the run can be replayed.
  const std::vector<std::string>& violations() const { return violations_; }

 private:
  // Log Matching copies whole logs, so it runs every N steps rather than
  // every step. Trade-off: a mismatch that appears and is truncated away
  // within N steps can be missed. Tests also compare full logs at the end,
  // and State Machine Safety (checked every step) catches any mismatch that
  // was ever applied.
  static constexpr uint64_t kLogMatchingEvery = 200;  // steps

  void start_node(NodeId id) {
    Config cfg = base_;
    cfg.id = id;
    cfg.peers.clear();
    for (NodeId other : ids_) {
      if (other != id) cfg.peers.push_back(other);
    }
    // Distinct per restart, so a restarted node does not replay the exact
    // same timeouts, but still fully determined by the test seed.
    cfg.seed = seed_ + restarts_++;
    nodes_[id] = std::make_unique<Raft>(cfg, net_, *storage_[id], clock_);
  }

  // Pulls committed entries from every live node, as a state machine would,
  // and checks them against the cluster-wide history.
  void collect_applied() {
    for (NodeId id : ids_) {
      auto& r = nodes_[id];
      if (!r) continue;
      for (LogEntry& e : r->take_committed()) {
        auto& mine = applied_[id];
        // Entries must be applied exactly once each, in index order.
        const Index expected = mine.empty() ? 1 : mine.back().index + 1;
        if (e.index != expected) {
          fail("node " + std::to_string(id) + " applied index " +
               std::to_string(e.index) + ", expected " +
               std::to_string(expected));
        }
        // State Machine Safety: everyone applies the same entry at an index.
        auto [it, first] = committed_.try_emplace(e.index, e);
        if (!first && !(it->second == e)) {
          fail("state machine safety: node " + std::to_string(id) +
               " applied a different entry at index " +
               std::to_string(e.index));
        }
        mine.push_back(std::move(e));
      }
    }
  }

  void check_invariants() {
    for (NodeId id : ids_) {
      const Raft* r = node(id);
      if (!r) continue;

      // Term monotonicity: a node's term never decreases, across restarts too
      // (the term is reloaded from storage).
      auto [it, inserted] = last_term_.try_emplace(id, r->term());
      if (!inserted) {
        if (r->term() < it->second) {
          fail("term went backwards on node " + std::to_string(id) + ": " +
               std::to_string(it->second) + " -> " + std::to_string(r->term()));
        }
        it->second = r->term();
      }

      // Commit monotonicity while the node is up (reset on restart, since
      // commit_index is volatile).
      auto [ct, cfirst] = last_commit_.try_emplace(id, r->commit_index());
      if (!cfirst) {
        if (r->commit_index() < ct->second) {
          fail("commit index went backwards on node " + std::to_string(id));
        }
        ct->second = r->commit_index();
      }

      // Election Safety: at most one leader per term, ever. Remembered for the
      // whole run, so two leaders that are never live at the same moment are
      // still caught.
      if (r->role() == Role::Leader) {
        auto [lt, first] = leader_of_term_.try_emplace(r->term(), id);
        if (!first && lt->second != id) {
          fail("two leaders in term " + std::to_string(r->term()) + ": " +
               std::to_string(lt->second) + " and " + std::to_string(id));
        }
        if (first) check_leader_completeness(id);
      }
    }
  }

  // Leader Completeness: a new leader's log contains every committed entry.
  void check_leader_completeness(NodeId id) {
    const MemStorage& s = *storage_[id];
    for (const auto& [idx, e] : committed_) {
      if (idx > s.last_index() || s.term_at(idx) != e.term) {
        fail("leader completeness: new leader " + std::to_string(id) +
             " is missing committed index " + std::to_string(idx));
        return;
      }
    }
  }

  // Log Matching: if two logs have an entry with the same index and term,
  // they are identical up to and including that index. For each pair, find
  // the highest index where the terms agree and compare the full prefixes.
  void check_log_matching() {
    for (std::size_t a = 0; a < ids_.size(); ++a) {
      for (std::size_t b = a + 1; b < ids_.size(); ++b) {
        const MemStorage& sa = *storage_[ids_[a]];
        const MemStorage& sb = *storage_[ids_[b]];
        Index i = std::min(sa.last_index(), sb.last_index());
        while (i > 0 && sa.term_at(i) != sb.term_at(i)) --i;
        if (i == 0) continue;
        if (sa.entries(1, i + 1) != sb.entries(1, i + 1)) {
          fail("log matching: nodes " + std::to_string(ids_[a]) + " and " +
               std::to_string(ids_[b]) + " agree on index " +
               std::to_string(i) + " but differ before it");
        }
      }
    }
  }

  void fail(const std::string& what) {
    violations_.push_back("t=" + std::to_string(clock_.now().count()) +
                          "ms seed=" + std::to_string(seed_) + ": " + what);
  }

  uint64_t seed_;
  Config base_;
  FakeClock clock_;
  SimNetwork net_;  // declared after clock_: it holds a reference to it

  std::vector<NodeId> ids_;
  std::map<NodeId, std::unique_ptr<MemStorage>> storage_;
  std::map<NodeId, std::unique_ptr<Raft>> nodes_;  // null while crashed
  uint64_t restarts_ = 0;
  uint64_t steps_ = 0;

  std::map<NodeId, Term> last_term_;
  std::map<NodeId, Index> last_commit_;
  std::map<Term, NodeId> leader_of_term_;
  std::map<Index, LogEntry> committed_;
  std::map<NodeId, std::vector<LogEntry>> applied_;
  std::vector<std::string> violations_;
};

}  // namespace toolings
