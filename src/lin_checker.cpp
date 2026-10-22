#include "toolings/lin_checker.hpp"

#include <algorithm>
#include <map>
#include <sstream>
#include <unordered_set>

namespace toolings {

namespace {

// The model of one key: absent (nullopt) or present with a value.
using Value = std::optional<std::string>;

// Applies `op` to `v`. Returns the new value, or nullopt-of-optional if the
// operation's observed result is impossible from `v` (a Get that read
// something else).
std::optional<Value> step(const Operation& op, const Value& v) {
  switch (op.kind) {
    case OpKind::Get:
      if (op.found != v.has_value()) return std::nullopt;
      if (op.found && op.value != *v) return std::nullopt;
      return v;
    case OpKind::Put:
      return Value(op.value);
    case OpKind::Append:
      return Value(v.value_or("") + op.value);
    case OpKind::Delete:
      return Value(std::nullopt);
  }
  return std::nullopt;
}

// Search state for one key's operations.
class KeySearch {
 public:
  KeySearch(std::vector<Operation> ops, uint64_t max_steps)
      : ops_(std::move(ops)), done_(ops_.size(), false), budget_(max_steps) {}

  // true = linearizable, false = not; sets timed_out() if out of budget.
  bool run() { return dfs(Value(std::nullopt), 0); }
  bool timed_out() const { return timed_out_; }

 private:
  bool dfs(const Value& v, std::size_t placed) {
    if (placed == ops_.size()) return true;
    if (budget_-- == 0) {
      timed_out_ = true;
      return false;
    }

    // Real-time order: an operation may go next only if it was called
    // before every remaining operation returned. So it must be called
    // before the earliest return among the remaining ones.
    int64_t earliest_ret = kNever;
    for (std::size_t i = 0; i < ops_.size(); ++i) {
      if (!done_[i]) earliest_ret = std::min(earliest_ret, ops_[i].ret);
    }

    for (std::size_t i = 0; i < ops_.size(); ++i) {
      if (done_[i] || ops_[i].call > earliest_ret) continue;
      auto next = step(ops_[i], v);
      if (!next) continue;  // its result contradicts this order

      done_[i] = true;
      if (seen_.insert(key_of(*next)).second && dfs(*next, placed + 1)) {
        return true;
      }
      done_[i] = false;
      if (timed_out_) return false;
    }
    return false;
  }

  // Memo key: which operations are placed + the model value.
  std::string key_of(const Value& v) const {
    std::string k(done_.begin(), done_.end());  // one char per op (0/1)
    k += v ? "\x01" + *v : std::string("\x00", 1);
    return k;
  }

  std::vector<Operation> ops_;
  std::vector<bool> done_;
  uint64_t budget_;
  bool timed_out_ = false;
  std::unordered_set<std::string> seen_;
};

const char* kind_name(OpKind k) {
  switch (k) {
    case OpKind::Get: return "get";
    case OpKind::Put: return "put";
    case OpKind::Append: return "append";
    case OpKind::Delete: return "delete";
  }
  return "?";
}

// The offending key's operations, in call order (first 40), for the report.
std::string describe(std::vector<Operation> ops) {
  std::sort(ops.begin(), ops.end(),
            [](const Operation& a, const Operation& b) { return a.call < b.call; });
  std::ostringstream out;
  for (std::size_t i = 0; i < ops.size() && i < 40; ++i) {
    const auto& o = ops[i];
    out << "  client " << o.client << " " << kind_name(o.kind) << " ";
    if (o.kind == OpKind::Get) {
      out << "-> " << (o.found ? o.value : "(none)");
    } else {
      out << o.value;
    }
    out << "  [" << o.call << ", "
        << (o.ret == kNever ? std::string("never") : std::to_string(o.ret))
        << "]\n";
  }
  if (ops.size() > 40) out << "  ... " << ops.size() - 40 << " more\n";
  return out.str();
}

}  // namespace

LinResult check_linearizable(const std::vector<Operation>& history,
                             uint64_t max_steps) {
  std::map<std::string, std::vector<Operation>> by_key;
  for (const auto& op : history) by_key[op.key].push_back(op);

  for (auto& [key, ops] : by_key) {
    KeySearch search(ops, max_steps);
    if (search.run()) continue;
    LinResult r;
    r.verdict = search.timed_out() ? LinResult::Verdict::Timeout
                                   : LinResult::Verdict::Violation;
    r.key = key;
    r.detail = describe(ops);
    return r;
  }
  return {};
}

}  // namespace toolings
