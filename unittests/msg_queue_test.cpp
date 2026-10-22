// Unit tests for BlockingQueue. The multi-threaded tests matter most under
// TSan (build-tsan), which checks the locking is actually race-free.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <set>
#include <thread>
#include <vector>

#include "toolings/msg_queue.hpp"

namespace {

using namespace std::chrono_literals;
using toolings::BlockingQueue;

TEST(BlockingQueue, FifoOrder) {
  BlockingQueue<int> q;
  for (int i = 0; i < 5; ++i) ASSERT_TRUE(q.push(i));
  for (int i = 0; i < 5; ++i) EXPECT_EQ(q.pop_for(0ms), i);
}

TEST(BlockingQueue, PopTimesOutWhenEmpty) {
  BlockingQueue<int> q;
  const auto start = std::chrono::steady_clock::now();
  EXPECT_FALSE(q.pop_for(20ms).has_value());
  EXPECT_GE(std::chrono::steady_clock::now() - start, 20ms);
}

TEST(BlockingQueue, PushFailsWhenFullInsteadOfBlocking) {
  // N2: the loop thread must never wait; a full queue drops the item.
  BlockingQueue<int> q(2);
  EXPECT_TRUE(q.push(1));
  EXPECT_TRUE(q.push(2));
  EXPECT_FALSE(q.push(3));
  EXPECT_EQ(q.size(), 2u);
}

TEST(BlockingQueue, CloseWakesBlockedPopper) {
  BlockingQueue<int> q;
  std::thread t([&] { EXPECT_FALSE(q.pop_for(10s).has_value()); });
  std::this_thread::sleep_for(20ms);
  q.close();
  t.join();  // would hang for 10s if close() did not wake it
}

TEST(BlockingQueue, ItemsPushedBeforeCloseAreStillDelivered) {
  BlockingQueue<int> q;
  q.push(7);
  q.close();
  EXPECT_FALSE(q.push(8));       // closed: rejected
  EXPECT_EQ(q.pop_for(0ms), 7);  // but the earlier item drains
  EXPECT_FALSE(q.pop_for(0ms).has_value());
}

TEST(BlockingQueue, ManyProducersOneConsumerLoseNothing) {
  // The real use: several gRPC threads push, one loop thread pops.
  BlockingQueue<int> q;
  constexpr int kProducers = 4, kEach = 2000;
  std::vector<std::thread> producers;
  for (int p = 0; p < kProducers; ++p) {
    producers.emplace_back([&, p] {
      for (int i = 0; i < kEach; ++i) q.push(p * kEach + i);
    });
  }

  std::set<int> got;
  while (got.size() < static_cast<std::size_t>(kProducers * kEach)) {
    auto v = q.pop_for(1s);
    ASSERT_TRUE(v.has_value()) << "lost items, got " << got.size();
    EXPECT_TRUE(got.insert(*v).second) << "duplicate " << *v;
  }
  for (auto& t : producers) t.join();
  EXPECT_EQ(q.size(), 0u);
}

}  // namespace
