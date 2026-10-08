#pragma once

// Deterministic simulation of time and network for tests.
//
// FakeClock:  time only moves when the test calls advance().
// SimNetwork: a Transport that queues messages in memory and delivers them
//             later, with seeded random delay and drop, plus partitions and
//             crashed nodes.
//
// Determinism: all randomness comes from one seeded RNG, and messages with
// equal delivery times are ordered by send sequence number. Same seed, same
// run, every time. That is what makes a failing seed replayable.

#include <cstdint>
#include <map>
#include <queue>
#include <random>
#include <set>
#include <vector>

#include "rafty/interfaces.hpp"
#include "rafty/types.hpp"

namespace toolings {

using rafty::Message;
using rafty::Millis;
using rafty::NodeId;

class FakeClock final : public rafty::Clock {
 public:
  Millis now() const override { return now_; }
  void advance(Millis d) { now_ += d; }

 private:
  Millis now_{0};
};

class SimNetwork final : public rafty::Transport {
 public:
  SimNetwork(const FakeClock& clock, uint64_t seed)
      : clock_(clock), rng_(seed) {}

  // ---- Transport ----

  // Called by Raft nodes. Decides now whether the message is dropped, and if
  // not, when it will arrive.
  void send(const Message& msg) override {
    if (!can_reach(msg.from, msg.to)) return;
    if (drop_rate_ > 0 && coin_(rng_) < drop_rate_) return;

    std::uniform_int_distribution<Millis::rep> delay(min_delay_.count(),
                                                     max_delay_.count());
    queue_.push(InFlight{clock_.now() + Millis{delay(rng_)}, next_seq_++, msg});
  }

  // ---- Delivery (called by the test driver) ----

  // Removes and returns every message due at or before now, in deterministic
  // order. Reachability is checked again here, so a partition or crash that
  // happens while a message is in flight also drops it.
  std::vector<Message> take_due() {
    std::vector<Message> out;
    while (!queue_.empty() && queue_.top().deliver_at <= clock_.now()) {
      Message m = queue_.top().msg;
      queue_.pop();
      if (can_reach(m.from, m.to)) out.push_back(std::move(m));
    }
    return out;
  }

  // ---- Fault injection ----

  // Probability in [0, 1] that any single message is lost.
  void set_drop_rate(double p) { drop_rate_ = p; }

  // Each delivered message takes a uniform delay in [min, max].
  void set_delay(Millis min, Millis max) {
    min_delay_ = min;
    max_delay_ = max;
  }

  // Split the cluster: nodes can talk only within their own group. Nodes not
  // listed in any group are isolated from everyone.
  void partition(const std::vector<std::vector<NodeId>>& groups) {
    group_of_.clear();
    partitioned_ = true;
    int g = 0;
    for (const auto& group : groups) {
      for (NodeId n : group) group_of_[n] = g;
      ++g;
    }
  }

  // Remove all partitions: everyone can reach everyone (crashed nodes aside).
  void heal() {
    partitioned_ = false;
    group_of_.clear();
  }

  // Crashed nodes neither send nor receive. Messages already in flight to or
  // from them are dropped at delivery time.
  void set_down(NodeId n, bool down) {
    if (down) {
      down_.insert(n);
    } else {
      down_.erase(n);
    }
  }

  std::size_t in_flight() const { return queue_.size(); }

 private:
  bool can_reach(NodeId from, NodeId to) const {
    if (down_.count(from) || down_.count(to)) return false;
    if (!partitioned_) return true;
    auto a = group_of_.find(from);
    auto b = group_of_.find(to);
    return a != group_of_.end() && b != group_of_.end() &&
           a->second == b->second;
  }

  struct InFlight {
    Millis deliver_at;
    uint64_t seq;  // tie-breaker: equal times deliver in send order
    Message msg;
  };
  // Min-heap on (deliver_at, seq).
  struct Later {
    bool operator()(const InFlight& a, const InFlight& b) const {
      if (a.deliver_at != b.deliver_at) return a.deliver_at > b.deliver_at;
      return a.seq > b.seq;
    }
  };

  const FakeClock& clock_;
  std::mt19937_64 rng_;
  std::uniform_real_distribution<double> coin_{0.0, 1.0};

  std::priority_queue<InFlight, std::vector<InFlight>, Later> queue_;
  uint64_t next_seq_ = 0;

  double drop_rate_ = 0.0;
  Millis min_delay_{1};
  Millis max_delay_{5};

  bool partitioned_ = false;
  std::map<NodeId, int> group_of_;
  std::set<NodeId> down_;
};

}  // namespace toolings
