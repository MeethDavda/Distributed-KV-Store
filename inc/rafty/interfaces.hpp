#pragma once

// Interfaces the Raft core uses to reach the outside world.
//
// Architecture rule: raft.cpp never touches sockets, files, or the system
// clock directly. It only talks to these interfaces (plus Storage in
// storage.hpp). Production wires in gRPC and the steady clock; tests wire in
// a simulated network and a fake clock, which makes every run deterministic
// and replayable from a seed.
//
// Threading model: each Raft node is driven by a single thread (event loop).
// The driver delivers inbound messages by calling Raft::step() and advances
// time by calling Raft::tick(). So these interfaces need no locking from the
// core's point of view.

#include <chrono>

#include "rafty/types.hpp"

namespace rafty {

// Outbound message delivery. Fire-and-forget: Raft already tolerates lost,
// duplicated, delayed, and reordered messages, so send() gives no guarantees
// and never blocks or reports errors. Replies arrive later as separate
// messages through Raft::step().
class Transport {
 public:
  virtual ~Transport() = default;
  virtual void send(const Message& msg) = 0;
};

// Monotonic time source. Only differences between readings are meaningful
// (election timeouts, heartbeat intervals, check-quorum windows). Must never
// go backwards, which is why it is not wall-clock time.
using Millis = std::chrono::milliseconds;

class Clock {
 public:
  virtual ~Clock() = default;
  virtual Millis now() const = 0;
};

// Production clock: std::chrono::steady_clock, which is monotonic.
class SteadyClock final : public Clock {
 public:
  Millis now() const override {
    return std::chrono::duration_cast<Millis>(
        std::chrono::steady_clock::now().time_since_epoch());
  }
};

}  // namespace rafty
