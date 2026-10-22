#pragma once

// Linearizability checker for KV histories (Wing & Gong style search).
//
// Linearizable = every operation appears to take effect at one instant
// between its call and its return, and the results match SOME order of those
// instants that respects real time (if A returned before B was called, A is
// ordered before B).
//
// Method:
//   1. Split the history by key: operations on different keys never affect
//      each other, so each key is checked alone (small searches).
//   2. Per key, depth-first search: pick an operation that may go next (no
//      other remaining operation returned before it was called), apply it to
//      a model of the key's value, check its result, backtrack on mismatch.
//   3. Memoize (set of operations placed, model value) so no state is
//      explored twice, and stop after a step budget (result: Timeout).
//
// An operation that never returned (ret = kNever) may take effect any time
// after its call, or never; giving it an infinite return time models exactly
// that.

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace toolings {

enum class OpKind { Get, Put, Append, Delete };

struct Operation {
  uint64_t client = 0;
  OpKind kind = OpKind::Get;
  std::string key;
  std::string value;       // Put/Append: the input. Get: the value read.
  bool found = false;      // Get only: whether the key existed
  int64_t call = 0;        // time the client sent it
  int64_t ret = 0;         // time the client got the answer, or kNever
};

inline constexpr int64_t kNever = std::numeric_limits<int64_t>::max();

struct LinResult {
  enum class Verdict { Ok, Violation, Timeout };
  Verdict verdict = Verdict::Ok;
  std::string key;     // offending key (Violation/Timeout)
  std::string detail;  // that key's operations, for debugging
};

// Checks the whole history. `max_steps` bounds the search per key.
LinResult check_linearizable(const std::vector<Operation>& history,
                             uint64_t max_steps = 5'000'000);

}  // namespace toolings
