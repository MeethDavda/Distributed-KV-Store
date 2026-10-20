#pragma once

// Durable state for a Raft node.
//
// Raft splits node state into two kinds:
//   - Persistent ("hard") state: currentTerm, votedFor, and the log. Must
//     survive crashes, otherwise safety breaks (see HardState below).
//   - Volatile state: role, commitIndex, who voted for us, timers. Rebuilt
//     from scratch after a restart.
//
// raft.cpp only sees the Storage interface. Two implementations:
//   - MemStorage: in-memory, for unit tests and fast simulation.
//   - FileStorage: write-ahead log + hard state file on disk (storage.cpp).

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <vector>

#include "rafty/types.hpp"

namespace rafty {

// How hard FileStorage pushes writes to stable storage. Recorded in every
// benchmark result, because it dominates write latency.
//   None:      no sync at all. NOT durable; benchmarks/comparison only.
//   Fsync:     fsync(2). On Linux this flushes to the device. On macOS it
//              only reaches the drive's volatile cache.
//   FullFsync: fcntl(F_FULLFSYNC) on macOS, which also flushes the drive
//              cache. Falls back to fsync where F_FULLFSYNC does not exist.
enum class FsyncMode { None, Fsync, FullFsync };

const char* to_string(FsyncMode m);

// Persistent state needed for elections.
//
// Invariant protected (Election Safety): a node grants at most one vote per
// term. If votedFor were lost in a crash, a node could restart and vote for a
// second candidate in the same term, letting two leaders win that term.
// currentTerm must persist for the same reason: forgetting it would let the
// node accept stale messages and vote in terms it already left behind.
struct HardState {
  Term current_term = 0;
  NodeId voted_for = kNoNode;  // kNoNode = has not voted in current_term

  bool operator==(const HardState&) const = default;
};

class Storage {
 public:
  virtual ~Storage() = default;

  // ---- Hard state ----

  // Contract: when save_hard_state() returns, the state is durable. Raft
  // relies on this to send a vote reply only AFTER the vote is recorded.
  virtual void save_hard_state(const HardState& hs) = 0;

  // Returns the last saved state, or a default HardState for a fresh node.
  virtual HardState load_hard_state() const = 0;

  // ---- Log ----
  //
  // The log is a contiguous run of entries with indices 1..last_index().
  // Writes are durable on return, so a follower can acknowledge entries
  // right after append() (invariant: durable before ack, or a "committed"
  // entry could vanish in a crash).

  // Index of the last entry, or 0 if the log is empty.
  virtual Index last_index() const = 0;

  // Term of the entry at index i. term_at(0) is 0 (the empty prefix).
  // Precondition: i <= last_index().
  virtual Term term_at(Index i) const = 0;

  // Entries with indices in [lo, hi). Precondition: 1 <= lo <= hi <=
  // last_index() + 1.
  virtual std::vector<LogEntry> entries(Index lo, Index hi) const = 0;

  // Appends entries to the end. Precondition: they are consecutive and the
  // first one has index last_index() + 1.
  virtual void append(const std::vector<LogEntry>& entries) = 0;

  // Deletes every entry with index >= i. Only followers call this, and only
  // when an incoming entry conflicts with ours (same index, different term).
  // Leaders never truncate their own log (Leader Append-Only).
  virtual void truncate_from(Index i) = 0;
};

// In-memory Storage for tests.
//
// To simulate a crash, a test destroys the Raft object but keeps its
// MemStorage alive, then builds a new Raft on the same MemStorage. That models
// "process died, disk survived". Durability is trivially true here; real
// fsync behaviour is tested against the WAL implementation in step 3.
//
// Preconditions are checked with assert: violating one is a bug in raft.cpp,
// and tests run in Debug, where asserts are on.
class MemStorage final : public Storage {
 public:
  void save_hard_state(const HardState& hs) override {
    hard_state_ = hs;
    ++save_count_;
  }

  HardState load_hard_state() const override { return hard_state_; }

  Index last_index() const override { return log_.size(); }

  Term term_at(Index i) const override {
    assert(i <= last_index());
    return i == 0 ? 0 : log_[i - 1].term;
  }

  std::vector<LogEntry> entries(Index lo, Index hi) const override {
    assert(1 <= lo && lo <= hi && hi <= last_index() + 1);
    return {log_.begin() + static_cast<long>(lo - 1),
            log_.begin() + static_cast<long>(hi - 1)};
  }

  void append(const std::vector<LogEntry>& entries) override {
    for (const LogEntry& e : entries) {
      assert(e.index == last_index() + 1);
      log_.push_back(e);
    }
  }

  void truncate_from(Index i) override {
    assert(i >= 1);
    if (i <= last_index()) log_.resize(i - 1);
  }

  // Test hook: lets unit tests assert that a vote was persisted before the
  // reply was sent (save count increases before the message appears).
  int save_count() const { return save_count_; }

 private:
  HardState hard_state_;
  std::vector<LogEntry> log_;  // log_[i - 1] holds index i
  int save_count_ = 0;
};

// Durable Storage on disk. Four ideas:
//
//   1. Append-only log file ("wal"). Each entry is one record added to the
//      end:  [u32 length][u32 crc][u64 term][u64 index][u8 type][data]
//      On startup the file is replayed to rebuild the log.
//
//   2. A checksum (CRC32) per record. A crash mid-write leaves a half-written
//      last record. Recovery detects it (short or bad CRC) and cuts it off.
//      Safe, because that append() never returned, so it was never acked.
//
//   3. Sync before returning. The OS caches writes in RAM; sync_file()
//      forces them to disk (F_FULLFSYNC on macOS, see FsyncMode).
//
//   4. Atomic rename for term + vote ("hardstate"). Write hardstate.tmp,
//      sync, rename over hardstate. rename() is atomic, so a crash leaves
//      the old or the new version, never a mix.
//
// Errors: a failed write or sync aborts the process. Retrying a failed
// fsync can falsely "succeed" because the kernel may have dropped the data
// (PostgreSQL's fsyncgate), so crash and recover from the log instead.
//
// The log is also kept in memory for reads; snapshots (step 5) bound it.
class FileStorage final : public Storage {
 public:
  // Opens (or creates) `dir` and runs recovery.
  FileStorage(std::filesystem::path dir, FsyncMode mode);
  ~FileStorage() override;

  FileStorage(const FileStorage&) = delete;
  FileStorage& operator=(const FileStorage&) = delete;

  void save_hard_state(const HardState& hs) override;
  HardState load_hard_state() const override { return hs_; }

  Index last_index() const override { return log_.size(); }
  Term term_at(Index i) const override;
  std::vector<LogEntry> entries(Index lo, Index hi) const override;
  void append(const std::vector<LogEntry>& entries) override;
  void truncate_from(Index i) override;

  // File names inside `dir`, exposed so tests can damage them on purpose.
  static constexpr const char* kWalFile = "wal";
  static constexpr const char* kHardStateFile = "hardstate";

 private:
  void load_hard_state_file();
  void replay_wal();
  void sync_file(int fd) const;
  void sync_dir() const;

  std::filesystem::path dir_;
  FsyncMode mode_;
  int wal_fd_ = -1;

  HardState hs_;
  std::vector<LogEntry> log_;  // log_[i - 1] holds index i
};

// CRC-32 (IEEE 802.3 polynomial, as used by zlib/PNG). Exposed for tests.
uint32_t crc32(const void* data, std::size_t len);

}  // namespace rafty
