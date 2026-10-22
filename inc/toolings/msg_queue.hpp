#pragma once

// BlockingQueue: a thread-safe FIFO for handing work between threads.
//
// Used where real threads meet the single-threaded core (step 4d):
//   - gRPC handler threads push incoming Raft messages and client requests;
//     the node's loop thread pops them. Only the loop thread ever touches
//     Raft, KvServer, or Storage (invariant N1).
//   - The loop thread pushes outgoing messages onto one queue per peer; a
//     sender thread per peer pops and does the RPC.
//
// push() never blocks. If the queue is full (or closed) it returns false and
// the caller drops the item. That is deliberate: the loop thread must never
// wait on the network (invariant N2), and Raft already tolerates lost
// messages.
//
// Implementation: one mutex guards the deque, one condition variable wakes
// poppers. Simple and obviously correct; throughput is not the bottleneck
// next to an fsync or a network round trip.

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <limits>
#include <mutex>
#include <optional>
#include <utility>

namespace toolings {

template <class T>
class BlockingQueue {
 public:
  explicit BlockingQueue(
      std::size_t capacity = std::numeric_limits<std::size_t>::max())
      : capacity_(capacity) {}

  BlockingQueue(const BlockingQueue&) = delete;
  BlockingQueue& operator=(const BlockingQueue&) = delete;

  // Adds an item. Returns false (item not added) if full or closed.
  bool push(T item) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (closed_ || items_.size() >= capacity_) return false;
      items_.push_back(std::move(item));
    }
    cv_.notify_one();  // after unlocking, so the woken thread can lock at once
    return true;
  }

  // Removes the oldest item, waiting up to `timeout` for one to arrive.
  // Returns nullopt on timeout, or once the queue is closed AND empty
  // (items pushed before close() are still delivered).
  template <class Rep, class Period>
  std::optional<T> pop_for(std::chrono::duration<Rep, Period> timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    // The predicate guards against spurious wakeups.
    cv_.wait_for(lock, timeout, [&] { return closed_ || !items_.empty(); });
    if (items_.empty()) return std::nullopt;
    T item = std::move(items_.front());
    items_.pop_front();
    return item;
  }

  // Stops the queue: further pushes fail, and every waiting pop_for wakes up.
  // Used for clean shutdown of the threads blocked on this queue.
  void close() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      closed_ = true;
    }
    cv_.notify_all();
  }

  std::size_t size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return items_.size();
  }

 private:
  const std::size_t capacity_;
  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::deque<T> items_;
  bool closed_ = false;
};

}  // namespace toolings
