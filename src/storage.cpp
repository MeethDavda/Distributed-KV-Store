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

void FileStorage::replay_wal() {
  const auto path = dir_ / kWalFile;
  std::string buf;
  read_file(path, buf);
  const auto* base = reinterpret_cast<const unsigned char*>(buf.data());

  // Read records until the first one that is incomplete, fails its CRC, or
  // does not continue the log. That can only be the tail of a write that a
  // crash interrupted, so everything from there on is dropped.
  std::size_t pos = 0;
  while (buf.size() - pos >= kHeader) {
    const uint32_t len = get_u32(base + pos);
    const uint32_t crc = get_u32(base + pos + 4);
    const unsigned char* payload = base + pos + kHeader;
    if (len < kPayloadFixed || buf.size() - pos - kHeader < len) break;
    if (crc32(payload, len) != crc) break;

    LogEntry e;
    e.term = get_u64(payload);
    e.index = get_u64(payload + 8);
    e.type = static_cast<EntryType>(payload[16]);
    e.data.assign(reinterpret_cast<const char*>(payload + kPayloadFixed),
                  len - kPayloadFixed);
    if (e.index != log_.size() + 1) break;  // must be the next index

    log_.push_back(std::move(e));
    pos += kHeader + len;
  }

  wal_fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
  if (wal_fd_ < 0) die("open", path);
  if (pos < buf.size()) {
    // Cut off the broken tail so new records go right after the good ones.
    if (::ftruncate(wal_fd_, static_cast<off_t>(pos)) != 0) {
      die("ftruncate", path);
    }
  }
  sync_file(wal_fd_);
  sync_dir();  // the WAL file may be new; make its directory entry durable
}

// ===========================================================================
// Writes: always on disk before returning
// ===========================================================================

void FileStorage::save_hard_state(const HardState& hs) {
  std::string buf;
  put_u64(buf, hs.current_term);
  put_u64(buf, hs.voted_for);
  put_u32(buf, crc32(buf.data(), buf.size()));

  // Write a temp file, sync it, then atomically rename it into place.
  const auto tmp = dir_ / "hardstate.tmp";
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) die("open", tmp);
  write_all(fd, buf, tmp);
  sync_file(fd);
  ::close(fd);
  if (::rename(tmp.c_str(), (dir_ / kHardStateFile).c_str()) != 0) {
    die("rename", tmp);
  }
  sync_dir();  // the rename changes the directory; make that durable too

  hs_ = hs;
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
  assert(i >= 1);
  if (i > last_index()) return;

  // Entry i starts right after the records of entries 1..i-1.
  uint64_t offset = 0;
  for (Index k = 1; k < i; ++k) offset += record_size(log_[k - 1]);

  if (::ftruncate(wal_fd_, static_cast<off_t>(offset)) != 0) {
    die("ftruncate", dir_ / kWalFile);
  }
  sync_file(wal_fd_);
  log_.resize(i - 1);
}

// ===========================================================================
// Reads: served from memory
// ===========================================================================

Term FileStorage::term_at(Index i) const {
  assert(i <= last_index());
  return i == 0 ? 0 : log_[i - 1].term;
}

std::vector<LogEntry> FileStorage::entries(Index lo, Index hi) const {
  assert(1 <= lo && lo <= hi && hi <= last_index() + 1);
  return {log_.begin() + static_cast<long>(lo - 1),
          log_.begin() + static_cast<long>(hi - 1)};
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
