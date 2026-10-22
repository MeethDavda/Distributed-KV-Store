// FileStorage: the log and hard state on disk.
//
// See storage.hpp for the four ideas (append-only log, CRC per record, sync
// before returning, atomic rename). This is the only file that does disk I/O
// for Raft state; raft.cpp never touches files.

#include "rafty/storage.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

namespace rafty {

namespace {

// Record layout: [u32 length][u32 crc][payload], payload =
// [u64 term][u64 index][u8 type][data]. `length` is the payload size.
constexpr std::size_t kHeader = 8;          // length + crc
constexpr std::size_t kPayloadFixed = 17;   // term + index + type

// Hard state file: [u64 term][u64 voted_for][u32 crc of the 16 bytes before].
constexpr std::size_t kHardStateSize = 20;

// I/O failures are fatal: see "Errors" in storage.hpp.
[[noreturn]] void die(const char* op, const std::filesystem::path& p) {
  std::fprintf(stderr, "FATAL storage: %s %s: %s\n", op, p.c_str(),
               std::strerror(errno));
  std::abort();
}

// Integers are written byte by byte, little-endian, so the file format does
// not depend on the machine.
void put_u32(std::string& out, uint32_t v) {
  for (int i = 0; i < 4; ++i) out.push_back(static_cast<char>(v >> (8 * i)));
}
void put_u64(std::string& out, uint64_t v) {
  for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>(v >> (8 * i)));
}
uint32_t get_u32(const unsigned char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(p[i]) << (8 * i);
  return v;
}
uint64_t get_u64(const unsigned char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(p[i]) << (8 * i);
  return v;
}

// write() may write only part of the buffer or be interrupted; loop until
// every byte is out.
void write_all(int fd, const std::string& buf, const std::filesystem::path& p) {
  std::size_t done = 0;
  while (done < buf.size()) {
    ssize_t n = ::write(fd, buf.data() + done, buf.size() - done);
    if (n < 0 && errno == EINTR) continue;
    if (n < 0) die("write", p);
    done += static_cast<std::size_t>(n);
  }
}

// Reads a whole file. Returns false if it does not exist.
bool read_file(const std::filesystem::path& p, std::string& out) {
  int fd = ::open(p.c_str(), O_RDONLY);
  if (fd < 0 && errno == ENOENT) return false;
  if (fd < 0) die("open", p);
  out.clear();
  char chunk[65536];
  for (;;) {
    ssize_t n = ::read(fd, chunk, sizeof chunk);
    if (n < 0 && errno == EINTR) continue;
    if (n < 0) die("read", p);
    if (n == 0) break;
    out.append(chunk, static_cast<std::size_t>(n));
  }
  ::close(fd);
  return true;
}

std::size_t record_size(const LogEntry& e) {
  return kHeader + kPayloadFixed + e.data.size();
}

void encode_record(const LogEntry& e, std::string& out) {
  std::string payload;
  put_u64(payload, e.term);
  put_u64(payload, e.index);
  payload.push_back(static_cast<char>(e.type));
  payload += e.data;

  put_u32(out, static_cast<uint32_t>(payload.size()));
  put_u32(out, crc32(payload.data(), payload.size()));
  out += payload;
}

// CRC-32 lookup table, built at compile time (same algorithm as zlib).
constexpr std::array<uint32_t, 256> make_crc_table() {
  std::array<uint32_t, 256> t{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t c = i;
    for (int k = 0; k < 8; ++k) c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
    t[i] = c;
  }
  return t;
}
constexpr auto kCrcTable = make_crc_table();

}  // namespace

uint32_t crc32(const void* data, std::size_t len) {
  const auto* p = static_cast<const unsigned char*>(data);
  uint32_t c = 0xFFFFFFFFu;
  for (std::size_t i = 0; i < len; ++i) {
    c = kCrcTable[(c ^ p[i]) & 0xFFu] ^ (c >> 8);
  }
  return c ^ 0xFFFFFFFFu;
}

const char* to_string(FsyncMode m) {
  switch (m) {
    case FsyncMode::None: return "none";
    case FsyncMode::Fsync: return "fsync";
    case FsyncMode::FullFsync: return "fullfsync";
  }
  return "unknown";
}

// ===========================================================================
// Startup: load hard state, replay the log
// ===========================================================================

FileStorage::FileStorage(std::filesystem::path dir, FsyncMode mode)
    : dir_(std::move(dir)), mode_(mode) {
  std::filesystem::create_directories(dir_);
  load_hard_state_file();
  load_snapshot_file();
  replay_wal();
}

FileStorage::~FileStorage() {
  if (wal_fd_ >= 0) ::close(wal_fd_);
}

void FileStorage::load_hard_state_file() {
  std::string buf;
  if (!read_file(dir_ / kHardStateFile, buf)) return;  // fresh node

  // The file is replaced atomically, so it is never half-written. If it is
  // damaged anyway, refuse to start: guessing the term or vote could let this
  // node vote twice in one term.
  const auto* p = reinterpret_cast<const unsigned char*>(buf.data());
  if (buf.size() != kHardStateSize || get_u32(p + 16) != crc32(p, 16)) {
    errno = EINVAL;
    die("corrupt hard state", dir_ / kHardStateFile);
  }
  hs_.current_term = get_u64(p);
  hs_.voted_for = get_u64(p + 8);
}

void FileStorage::load_snapshot_file() {
  // Layout: [u64 index][u64 term][data...][u32 crc of everything before].
  std::string buf;
  if (!read_file(dir_ / kSnapshotFile, buf)) return;  // no snapshot yet

  // Written atomically like hardstate, so damage means a real problem; the
  // log entries it replaced are gone, so there is nothing safe to fall back
  // to. Refuse to start.
  const auto* p = reinterpret_cast<const unsigned char*>(buf.data());
  if (buf.size() < 20 ||
      get_u32(p + buf.size() - 4) != crc32(p, buf.size() - 4)) {
    errno = EINVAL;
    die("corrupt snapshot", dir_ / kSnapshotFile);
  }
  snap_.index = get_u64(p);
  snap_.term = get_u64(p + 8);
  snap_.data.assign(buf, 16, buf.size() - 20);
}

void FileStorage::replay_wal() {
  const auto path = dir_ / kWalFile;
  std::string buf;
  read_file(path, buf);
  const auto* base = reinterpret_cast<const unsigned char*>(buf.data());

  // Read records until the first one that is incomplete, fails its CRC, or
  // does not continue the log. That can only be the tail of a write that a
  // crash interrupted, so everything from there on is dropped.
  //
  // Records the snapshot already covers (index <= snap_.index) are skipped:
  // they appear only if we crashed after writing a snapshot but before
  // rewriting the WAL. The record AT the snapshot index must match the
  // snapshot's term; if not, the WAL is a different (stale) history, as after
  // an InstallSnapshot, and nothing after it is kept.
  std::size_t off = 0;
  Index prev = 0;
  bool clean = true;  // false if anything was skipped or cut
  while (buf.size() - off >= kHeader) {
    const uint32_t len = get_u32(base + off);
    const uint32_t crc = get_u32(base + off + 4);
    const unsigned char* payload = base + off + kHeader;
    if (len < kPayloadFixed || buf.size() - off - kHeader < len) break;
    if (crc32(payload, len) != crc) break;

    LogEntry e;
    e.term = get_u64(payload);
    e.index = get_u64(payload + 8);
    e.type = static_cast<EntryType>(payload[16]);
    e.data.assign(reinterpret_cast<const char*>(payload + kPayloadFixed),
                  len - kPayloadFixed);
    if (prev != 0 && e.index != prev + 1) break;  // records must be contiguous
    prev = e.index;
    off += kHeader + len;

    if (e.index <= snap_.index) {  // covered by the snapshot
      clean = false;
      if (e.index == snap_.index && e.term != snap_.term) break;
      continue;
    }
    if (e.index != last_index() + 1) {  // must continue the log
      clean = false;
      break;
    }
    log_.push_back(std::move(e));
  }
  if (off < buf.size()) clean = false;

  wal_fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
  if (wal_fd_ < 0) die("open", path);
  sync_dir();  // the WAL file may be new; make its directory entry durable
  // If we skipped or cut anything, rewrite the WAL to hold exactly log_, so
  // byte offsets (used by truncate_from) match the in-memory log again.
  if (!clean) rewrite_wal();
}

// ===========================================================================
// Writes: always on disk before returning
// ===========================================================================

void FileStorage::write_file_atomically(const char* name,
                                        const std::string& bytes) {
  // Write a temp file, sync it, then atomically rename it into place, and
  // sync the directory so the rename itself is durable.
  const auto tmp = dir_ / (std::string(name) + ".tmp");
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) die("open", tmp);
  write_all(fd, bytes, tmp);
  sync_file(fd);
  ::close(fd);
  if (::rename(tmp.c_str(), (dir_ / name).c_str()) != 0) die("rename", tmp);
  sync_dir();
}

void FileStorage::save_hard_state(const HardState& hs) {
  std::string buf;
  put_u64(buf, hs.current_term);
  put_u64(buf, hs.voted_for);
  put_u32(buf, crc32(buf.data(), buf.size()));
  write_file_atomically(kHardStateFile, buf);
  hs_ = hs;
}

void FileStorage::save_snapshot(const Snapshot& s) {
  assert(s.index > snap_.index);

  // S3 step 1: the snapshot is durable before any log entry is deleted.
  std::string buf;
  put_u64(buf, s.index);
  put_u64(buf, s.term);
  buf += s.data;
  put_u32(buf, crc32(buf.data(), buf.size()));
  write_file_atomically(kSnapshotFile, buf);

  // S3 step 2: drop covered entries. Keep the suffix only if our log has the
  // snapshot's last entry (same index and term = same history).
  const bool same_history =
      s.index <= last_index() && term_at(s.index) == s.term;
  if (same_history) {
    log_.erase(log_.begin(), log_.begin() + static_cast<long>(pos(s.index) + 1));
  } else {
    log_.clear();
  }
  snap_ = s;
  rewrite_wal();
}

void FileStorage::rewrite_wal() {
  std::string buf;
  for (const LogEntry& e : log_) encode_record(e, buf);
  if (wal_fd_ >= 0) ::close(wal_fd_);
  write_file_atomically(kWalFile, buf);
  const auto path = dir_ / kWalFile;
  wal_fd_ = ::open(path.c_str(), O_RDWR | O_APPEND);
  if (wal_fd_ < 0) die("open", path);
}

void FileStorage::append(const std::vector<LogEntry>& entries) {
  // The whole batch is written at once and synced once: one disk flush per
  // batch instead of one per entry.
  std::string buf;
  Index expected = last_index() + 1;
  for (const LogEntry& e : entries) {
    assert(e.index == expected++);  // must continue the log with no gaps
    encode_record(e, buf);
  }
  (void)expected;  // only read by assert, which Release builds compile out
  if (buf.empty()) return;
  write_all(wal_fd_, buf, dir_ / kWalFile);
  sync_file(wal_fd_);

  log_.insert(log_.end(), entries.begin(), entries.end());
}

void FileStorage::truncate_from(Index i) {
  assert(i > snap_.index);
  if (i > last_index()) return;

  // The WAL holds exactly log_, so entry i starts right after the records of
  // the entries before it.
  uint64_t offset = 0;
  for (std::size_t k = 0; k < pos(i); ++k) offset += record_size(log_[k]);

  if (::ftruncate(wal_fd_, static_cast<off_t>(offset)) != 0) {
    die("ftruncate", dir_ / kWalFile);
  }
  sync_file(wal_fd_);
  log_.resize(pos(i));
}

// ===========================================================================
// Reads: served from memory
// ===========================================================================

Term FileStorage::term_at(Index i) const {
  assert(snap_.index <= i && i <= last_index());
  return i == snap_.index ? snap_.term : log_[pos(i)].term;
}

std::vector<LogEntry> FileStorage::entries(Index lo, Index hi) const {
  assert(snap_.index < lo && lo <= hi && hi <= last_index() + 1);
  return {log_.begin() + static_cast<long>(pos(lo)),
          log_.begin() + static_cast<long>(pos(hi))};
}

// ===========================================================================
// Sync: push cached writes to the physical disk
// ===========================================================================

void FileStorage::sync_file(int fd) const {
  if (mode_ == FsyncMode::None) return;
#ifdef F_FULLFSYNC
  // macOS: plain fsync stops at the drive's cache; F_FULLFSYNC flushes it.
  if (mode_ == FsyncMode::FullFsync && ::fcntl(fd, F_FULLFSYNC) == 0) return;
#endif
  if (::fsync(fd) != 0) die("fsync", dir_);
}

void FileStorage::sync_dir() const {
  if (mode_ == FsyncMode::None) return;
  int fd = ::open(dir_.c_str(), O_RDONLY);
  if (fd < 0) die("open dir", dir_);
  sync_file(fd);
  ::close(fd);
}

}  // namespace rafty
