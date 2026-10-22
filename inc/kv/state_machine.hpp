#pragma once

// The replicated key-value state machine.
//
// Raft guarantees every node sees the same committed log in the same order.
// This class turns that log into the same map on every node, by applying
// each committed entry exactly once, in index order.
//
// Invariants:
//   K1 Deterministic apply: apply() depends only on the entry and the current
//      state. No clocks, no randomness, so every node computes the same state.
//   K2 Exactly-once writes: each write carries (client_id, seq). The dedup
//      table remembers each client's last applied seq; a command with
//      seq <= last is a retry and is skipped.
//   K3 The dedup table is updated here, during apply, so it is identical on
//      every node. A new leader after failover therefore still recognises
//      retries of writes the old leader applied.
//
// Dedup stores only the last seq per client, not the result, because writes
// return nothing but "ok". This relies on each client having one request in
// flight at a time (KvClient enforces it), so seqs arrive in order.

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>

#include "rafty/types.hpp"

namespace kv {

enum class OpType : uint8_t { Put = 1, Append = 2, Delete = 3 };

// A write command, as stored in a Raft log entry's data.
struct Command {
  OpType op = OpType::Put;
  uint64_t client_id = 0;
  uint64_t seq = 0;  // per client, starts at 1, +1 for each new request
  std::string key;
  std::string value;  // unused for Delete

  bool operator==(const Command&) const = default;
};

// Binary encoding for LogEntry::data:
//   [u8 op][u64 client_id][u64 seq][u32 key_len][key][u32 value_len][value]
std::string encode(const Command& c);
// Returns nullopt if `bytes` is not a well-formed command.
std::optional<Command> decode(std::string_view bytes);

// What applying one entry did. KvServer uses it to answer the client that
// proposed the entry.
struct ApplyResult {
  rafty::Index index = 0;
  rafty::Term term = 0;
  bool is_command = false;  // false for NoOp entries
  Command command;          // valid if is_command
  bool duplicate = false;   // true if skipped by dedup (K2)
};

class StateMachine {
 public:
  // Applies one committed entry. Entries must arrive in index order with no
  // gaps (Raft's take_committed() guarantees this).
  ApplyResult apply(const rafty::LogEntry& e);

  // Reads a key from the applied state. Linearizability of reads is the
  // caller's job (ReadIndex); this just reads the map.
  std::optional<std::string> get(const std::string& key) const;

  rafty::Index last_applied() const { return last_applied_; }

  // Snapshot support. serialize() captures the map, the dedup table (S2:
  // exactly-once must survive compaction), and last_applied. restore()
  // replaces the whole state with a serialized one.
  std::string serialize() const;
  void restore(const std::string& data);

  // Full state, for determinism tests (and snapshots in step 5).
  const std::map<std::string, std::string>& data() const { return data_; }
  const std::map<uint64_t, uint64_t>& last_seq() const { return last_seq_; }

 private:
  // std::map, not unordered_map: iteration order is defined, so a snapshot
  // of the state (step 5) is byte-for-byte identical on every node.
  std::map<std::string, std::string> data_;
  std::map<uint64_t, uint64_t> last_seq_;  // client_id -> last applied seq
  rafty::Index last_applied_ = 0;
};

}  // namespace kv
