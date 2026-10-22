// Unit tests for the linearizability checker, on hand-written histories.
//
// "A checker that always passes is worse than none" (CLAUDE.md), so most of
// these are BAD histories that must be rejected: stale read, lost write,
// reordered writes. The good ones make sure it does not reject valid
// concurrency.
//
// Times are small integers; [call, ret] is each operation's interval.

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "toolings/lin_checker.hpp"

namespace {

using toolings::check_linearizable;
using toolings::kNever;
using toolings::LinResult;
using toolings::Operation;
using toolings::OpKind;

Operation put(uint64_t c, std::string v, int64_t call, int64_t ret,
              std::string key = "x") {
  return {c, OpKind::Put, std::move(key), std::move(v), false, call, ret};
}
Operation append(uint64_t c, std::string v, int64_t call, int64_t ret) {
  return {c, OpKind::Append, "x", std::move(v), false, call, ret};
}
Operation get(uint64_t c, std::string v, int64_t call, int64_t ret,
              std::string key = "x") {
  return {c, OpKind::Get, std::move(key), std::move(v), true, call, ret};
}
Operation get_missing(uint64_t c, int64_t call, int64_t ret) {
  return {c, OpKind::Get, "x", "", false, call, ret};
}

bool ok(const std::vector<Operation>& h) {
  return check_linearizable(h).verdict == LinResult::Verdict::Ok;
}

// ---------------------------------------------------------------------------
// Good histories: must pass
// ---------------------------------------------------------------------------

TEST(LinChecker, SequentialHistoryPasses) {
  EXPECT_TRUE(ok({put(1, "a", 0, 1), get(1, "a", 2, 3), append(1, "b", 4, 5),
                  get(2, "ab", 6, 7)}));
}

TEST(LinChecker, ConcurrentWritesMayLandInEitherOrder) {
  // Two overlapping puts: a later read may see either one.
  EXPECT_TRUE(ok({put(1, "a", 0, 10), put(2, "b", 0, 10), get(3, "a", 11, 12)}));
  EXPECT_TRUE(ok({put(1, "a", 0, 10), put(2, "b", 0, 10), get(3, "b", 11, 12)}));
}

TEST(LinChecker, ReadOverlappingAWriteMaySeeOldOrNew) {
  EXPECT_TRUE(ok({put(1, "a", 0, 10), get_missing(2, 1, 2)}));
  EXPECT_TRUE(ok({put(1, "a", 0, 10), get(2, "a", 1, 2)}));
}

TEST(LinChecker, UnfinishedWriteMayOrMayNotHaveHappened) {
  // The client never heard back (ret = never).
  EXPECT_TRUE(ok({put(1, "a", 0, kNever), get_missing(2, 5, 6)}));
  EXPECT_TRUE(ok({put(1, "a", 0, kNever), get(2, "a", 5, 6)}));
}

TEST(LinChecker, KeysAreIndependent) {
  EXPECT_TRUE(ok({put(1, "a", 0, 1, "x"), put(1, "b", 2, 3, "y"),
                  get(2, "a", 4, 5, "x"), get(2, "b", 4, 5, "y")}));
}

// ---------------------------------------------------------------------------
// Bad histories: must be caught
// ---------------------------------------------------------------------------

TEST(LinChecker, StaleReadIsCaught) {
  // x=2 finished before the read started, yet the read returned 1.
  auto r = check_linearizable(
      {put(1, "1", 0, 1), put(1, "2", 2, 3), get(2, "1", 4, 5)});
  EXPECT_EQ(r.verdict, LinResult::Verdict::Violation);
  EXPECT_EQ(r.key, "x");
  EXPECT_FALSE(r.detail.empty());
}

TEST(LinChecker, LostWriteIsCaught) {
  // Both appends finished, but the read shows only the second.
  EXPECT_FALSE(ok({append(1, "a", 0, 1), append(1, "b", 2, 3),
                   get(2, "b", 4, 5)}));
}

TEST(LinChecker, DuplicatedWriteIsCaught) {
  // The exactly-once failure mode: one append applied twice.
  EXPECT_FALSE(ok({append(1, "a", 0, 1), get(2, "aa", 2, 3)}));
}

TEST(LinChecker, ReorderedWritesAreCaught) {
  // Sequential puts 1 then 2, but readers see 2 and then 1.
  EXPECT_FALSE(ok({put(1, "1", 0, 1), put(1, "2", 2, 3), get(2, "2", 4, 5),
                   get(2, "1", 6, 7)}));
}

TEST(LinChecker, ReadCannotGoBackInTime) {
  // Once a read has seen the new value, a later read cannot see the old one,
  // even while the write is still in progress.
  EXPECT_FALSE(ok({put(1, "a", 0, 100), get(2, "a", 1, 2),
                   get_missing(3, 3, 4)}));
}

TEST(LinChecker, ValueNobodyWroteIsCaught) {
  EXPECT_FALSE(ok({put(1, "a", 0, 1), get(2, "zzz", 2, 3)}));
}

TEST(LinChecker, ViolationOnOneKeyIsReportedWithThatKey) {
  auto r = check_linearizable({put(1, "a", 0, 1, "good"), get(2, "a", 2, 3, "good"),
                               put(1, "1", 0, 1, "bad"), get(2, "9", 2, 3, "bad")});
  EXPECT_EQ(r.verdict, LinResult::Verdict::Violation);
  EXPECT_EQ(r.key, "bad");
}

}  // namespace
