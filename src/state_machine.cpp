#include "kv/state_machine.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>

namespace kv {

namespace {

// Little-endian integer encoding, same convention as the WAL in storage.cpp.
void put_u32(std::string& out, uint32_t v) {
  for (int i = 0; i < 4; ++i) out.push_back(static_cast<char>(v >> (8 * i)));
}
void put_u64(std::string& out, uint64_t v) {
  for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>(v >> (8 * i)));
}

// Reads fields from a byte string, refusing to read past the end. Any short
// read sets ok = false, so decode() can reject garbage instead of crashing.
struct Reader {
  std::string_view in;
  std::size_t pos = 0;
  bool ok = true;

  bool has(std::size_t n) {
    if (in.size() - pos < n) ok = false;
    return ok;
  }
  uint8_t u8() {
    if (!has(1)) return 0;
    return static_cast<uint8_t>(in[pos++]);
  }
  uint64_t uint(int bytes) {
    if (!has(static_cast<std::size_t>(bytes))) return 0;
    uint64_t v = 0;
    for (int i = 0; i < bytes; ++i) {
      v |= static_cast<uint64_t>(static_cast<uint8_t>(in[pos++])) << (8 * i);
    }
    return v;
  }
  std::string str() {
    const auto len = static_cast<std::size_t>(uint(4));
    if (!has(len)) return {};
    std::string s(in.substr(pos, len));
    pos += len;
    return s;
  }
};

}  // namespace

// ===========================================================================
// Encoding
// ===========================================================================

std::string encode(const Command& c) {
  std::string out;
  out.push_back(static_cast<char>(c.op));
  put_u64(out, c.client_id);
  put_u64(out, c.seq);
  put_u32(out, static_cast<uint32_t>(c.key.size()));
  out += c.key;
  put_u32(out, static_cast<uint32_t>(c.value.size()));
  out += c.value;
  return out;
}

std::optional<Command> decode(std::string_view bytes) {
  Reader r{bytes};
  Command c;
  const uint8_t op = r.u8();
  c.client_id = r.uint(8);
  c.seq = r.uint(8);
  c.key = r.str();
  c.value = r.str();
  if (!r.ok || r.pos != bytes.size()) return std::nullopt;  // short or extra
  if (op < 1 || op > 3) return std::nullopt;
  c.op = static_cast<OpType>(op);
  return c;
}

// ===========================================================================
// Apply
// ===========================================================================

ApplyResult StateMachine::apply(const rafty::LogEntry& e) {
  // Committed entries arrive in order with no gaps.
  assert(e.index == last_applied_ + 1);
  last_applied_ = e.index;

  ApplyResult res;
  res.index = e.index;
  res.term = e.term;
  if (e.type == rafty::EntryType::NoOp) return res;  // leader's no-op

  // Only KvServer proposes entries, and it always encodes a valid Command.
  // An undecodable committed entry means a bug; every node would hit the
  // same entry, so stop loudly rather than diverge or hide it.
  auto cmd = decode(e.data);
  if (!cmd) {
    std::fprintf(stderr, "FATAL: undecodable command at index %llu\n",
                 static_cast<unsigned long long>(e.index));
    std::abort();
  }
  res.is_command = true;
  res.command = *cmd;

  // K2 exactly-once: a seq we have already applied for this client is a
  // retry (or a late copy). Skip it, so the write takes effect only once and
  // an old retry can never undo a newer write.
  uint64_t& last = last_seq_[cmd->client_id];
  if (cmd->seq <= last) {
    res.duplicate = true;
    return res;
  }
  last = cmd->seq;  // K3: updated during apply, identically on every node

  switch (cmd->op) {
    case OpType::Put:
      data_[cmd->key] = cmd->value;
      break;
    case OpType::Append:
      data_[cmd->key] += cmd->value;
      break;
    case OpType::Delete:
      data_.erase(cmd->key);
      break;
  }
  return res;
}

std::optional<std::string> StateMachine::get(const std::string& key) const {
  auto it = data_.find(key);
  if (it == data_.end()) return std::nullopt;
  return it->second;
}

}  // namespace kv
