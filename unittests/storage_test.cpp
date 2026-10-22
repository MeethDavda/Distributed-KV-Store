// Unit tests for FileStorage.
//
// The pattern in almost every test: write something, close the storage
// ("crash"), open it again ("restart"), and check what came back. Damaged
// files are made by editing the WAL bytes directly, the way a crash
// mid-write would leave them.

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "rafty/storage.hpp"

namespace {

namespace fs = std::filesystem;
using rafty::crc32;
using rafty::EntryType;
using rafty::FileStorage;
using rafty::FsyncMode;
using rafty::HardState;
using rafty::Index;
using rafty::LogEntry;
using rafty::MemStorage;
using rafty::Term;

LogEntry entry(Term term, Index index, std::string data = "") {
  LogEntry e;
  e.term = term;
  e.index = index;
  e.data = std::move(data);
  return e;
}

std::string read_bytes(const fs::path& p) {
  std::ifstream in(p, std::ios::binary);
  return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void write_bytes(const fs::path& p, const std::string& bytes) {
  std::ofstream out(p, std::ios::binary | std::ios::trunc);
  out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

class FileStorageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // One fresh directory per test, so tests never see each other's files.
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    dir = fs::temp_directory_path() /
          ("raftkv-" + std::string(info->name()) + "-" +
           std::to_string(::getpid()));
    fs::remove_all(dir);
  }

  void TearDown() override {
    store.reset();
    fs::remove_all(dir);
  }

  // Closes any open storage (a "crash") and opens it again (a "restart").
  FileStorage& reopen(FsyncMode mode = FsyncMode::None) {
    store.reset();
    store = std::make_unique<FileStorage>(dir, mode);
    return *store;
  }

  fs::path wal() const { return dir / FileStorage::kWalFile; }

  fs::path dir;
  std::unique_ptr<FileStorage> store;
};

// ---------------------------------------------------------------------------
// Basics
// ---------------------------------------------------------------------------

TEST(Crc32, MatchesStandardCheckValue) {
  // The published check value for CRC-32/IEEE: same as zlib's crc32().
  EXPECT_EQ(crc32("123456789", 9), 0xCBF43926u);
  EXPECT_EQ(crc32("", 0), 0u);
}

TEST_F(FileStorageTest, FreshDirectoryStartsEmpty) {
  auto& s = reopen();
  EXPECT_EQ(s.load_hard_state(), HardState{});
  EXPECT_EQ(s.last_index(), 0u);
  EXPECT_EQ(s.term_at(0), 0u);
}

TEST_F(FileStorageTest, HardStateSurvivesRestart) {
  reopen().save_hard_state(HardState{7, 3});
  EXPECT_EQ(reopen().load_hard_state(), (HardState{7, 3}));
}

TEST_F(FileStorageTest, EntriesSurviveRestart) {
  LogEntry noop = entry(1, 1);
  noop.type = EntryType::NoOp;
  // Data with a zero byte: records are length-prefixed, not text.
  const std::string binary("a\0b", 3);
  reopen().append({noop, entry(1, 2, binary), entry(2, 3, "c")});

  auto& s = reopen();
  ASSERT_EQ(s.last_index(), 3u);
  EXPECT_EQ(s.entries(1, 4),
            (std::vector<LogEntry>{noop, entry(1, 2, binary), entry(2, 3, "c")}));
}

TEST_F(FileStorageTest, TruncateSurvivesRestart) {
  reopen().append({entry(1, 1), entry(1, 2), entry(1, 3)});
  store->truncate_from(2);
  EXPECT_EQ(reopen().last_index(), 1u);
}

TEST_F(FileStorageTest, AppendAfterTruncateSurvivesRestart) {
  reopen().append({entry(1, 1), entry(1, 2, "old"), entry(1, 3, "old")});
  store->truncate_from(2);
  store->append({entry(2, 2, "new")});

  auto& s = reopen();
  ASSERT_EQ(s.last_index(), 2u);
  EXPECT_EQ(s.entries(2, 3)[0], entry(2, 2, "new"));
}

TEST_F(FileStorageTest, FullFsyncModeWorks) {
  // Exercises the real F_FULLFSYNC path (slow, so only here).
  auto& s = reopen(FsyncMode::FullFsync);
  s.save_hard_state(HardState{1, 1});
  s.append({entry(1, 1, "x")});
  auto& r = reopen(FsyncMode::FullFsync);
  EXPECT_EQ(r.load_hard_state(), (HardState{1, 1}));
  EXPECT_EQ(r.last_index(), 1u);
}

// ---------------------------------------------------------------------------
// Crash damage: torn and corrupt tails
// ---------------------------------------------------------------------------

TEST_F(FileStorageTest, TornLastRecordIsDroppedAtEveryCutPoint) {
  // Write 2 entries, note the file size, then a 3rd entry. A crash during
  // the 3rd write could leave the file cut at ANY byte inside the 3rd
  // record. Try every one of them.
  reopen().append({entry(1, 1, "first"), entry(1, 2, "second")});
  const auto two_records = fs::file_size(wal());
  store->append({entry(1, 3, "third-entry-payload")});
  store.reset();
  const std::string full = read_bytes(wal());

  for (auto cut = two_records; cut < full.size(); ++cut) {
    SCOPED_TRACE("file cut at byte " + std::to_string(cut));
    store.reset();  // closed while we damage the file
    write_bytes(wal(), full.substr(0, cut));

    auto& s = reopen();
    ASSERT_EQ(s.last_index(), 2u);  // the torn 3rd record is gone
    EXPECT_EQ(s.entries(2, 3)[0].data, "second");

    // The bad tail was cut off, so a new append lands cleanly after entry 2.
    s.append({entry(1, 3, "retry")});
    ASSERT_EQ(reopen().last_index(), 3u);
    EXPECT_EQ(store->entries(3, 4)[0].data, "retry");
  }
}

TEST_F(FileStorageTest, FlippedBitInLastRecordIsDropped) {
  reopen().append({entry(1, 1), entry(1, 2, "abc")});
  store.reset();
  std::string bytes = read_bytes(wal());
  bytes.back() ^= 0x01;  // damage the last data byte; the CRC no longer matches
  write_bytes(wal(), bytes);

  EXPECT_EQ(reopen().last_index(), 1u);
}

TEST_F(FileStorageTest, GarbageAfterLastRecordIsDropped) {
  reopen().append({entry(1, 1), entry(1, 2)});
  store.reset();
  // A huge length field followed by nothing: must not be trusted.
  write_bytes(wal(), read_bytes(wal()) + std::string(8, '\xFF'));

  auto& s = reopen();
  EXPECT_EQ(s.last_index(), 2u);
  s.append({entry(1, 3)});
  EXPECT_EQ(reopen().last_index(), 3u);
}

// ---------------------------------------------------------------------------
// Hard state file
// ---------------------------------------------------------------------------

TEST_F(FileStorageTest, LeftoverTempFileFromCrashBeforeRenameIsIgnored) {
  reopen().save_hard_state(HardState{3, 2});
  store.reset();
  // A crash after writing the temp file but before the rename.
  write_bytes(dir / "hardstate.tmp", "half-written junk");

  auto& s = reopen();
  EXPECT_EQ(s.load_hard_state(), (HardState{3, 2}));  // old state wins
  s.save_hard_state(HardState{4, 1});
  EXPECT_EQ(reopen().load_hard_state(), (HardState{4, 1}));
}

TEST_F(FileStorageTest, CorruptHardStateRefusesToStart) {
  reopen().save_hard_state(HardState{3, 2});
  store.reset();
  std::string bytes = read_bytes(dir / FileStorage::kHardStateFile);
  bytes[0] ^= 0x01;
  write_bytes(dir / FileStorage::kHardStateFile, bytes);

  // Guessing a term or vote could break Election Safety: stop instead.
  EXPECT_DEATH({ FileStorage s(dir, FsyncMode::None); }, "corrupt hard state");
}

// ---------------------------------------------------------------------------
// Differential test: FileStorage must behave exactly like MemStorage
// ---------------------------------------------------------------------------

TEST_F(FileStorageTest, BehavesLikeMemStorageUnderRandomOperations) {
  // Random appends, truncates, hard state saves, and restarts, applied to
  // both. After every operation the two must hold the same state. Seeded,
  // so a failure names the seed and replays exactly.
  for (uint64_t seed = 1; seed <= 20; ++seed) {
    SCOPED_TRACE("seed=" + std::to_string(seed));
    fs::remove_all(dir);
    reopen();
    MemStorage mem;
    std::mt19937_64 rng(seed);
    Term term = 1;

    for (int op = 0; op < 300; ++op) {
      switch (rng() % 10) {
        case 0: case 1: case 2: case 3: {  // append 1-5 entries
          std::vector<LogEntry> batch;
          const auto n = 1 + rng() % 5;
          for (uint64_t k = 0; k < n; ++k) {
            // Separate statements: the order of two rng() calls inside one
            // expression is unspecified, which would break seed replay.
            const auto len = rng() % 40;
            const auto ch = static_cast<char>('a' + rng() % 26);
            batch.push_back(
                entry(term, mem.last_index() + 1 + k, std::string(len, ch)));
          }
          mem.append(batch);
          store->append(batch);
          break;
        }
        case 4: case 5: {  // truncate after the snapshot (possibly past end)
          const Index snap = mem.snapshot().index;
          const Index i = snap + 1 + rng() % (mem.last_index() - snap + 1);
          mem.truncate_from(i);
          store->truncate_from(i);
          break;
        }
        case 7: {  // compact: snapshot up to a random existing entry
          const Index snap = mem.snapshot().index;
          if (mem.last_index() == snap) break;
          const Index i = snap + 1 + rng() % (mem.last_index() - snap);
          const rafty::Snapshot s{i, mem.term_at(i), "state@" + std::to_string(i)};
          mem.save_snapshot(s);
          store->save_snapshot(s);
          break;
        }
        case 6: {  // new term + vote
          term += rng() % 2;
          const rafty::NodeId vote = 1 + rng() % 5;
          const HardState hs{term, vote};
          mem.save_hard_state(hs);
          store->save_hard_state(hs);
          break;
        }
        default:  // crash and restart
          reopen();
          break;
      }

      ASSERT_EQ(store->load_hard_state(), mem.load_hard_state()) << "op " << op;
      ASSERT_EQ(store->last_index(), mem.last_index()) << "op " << op;
      ASSERT_EQ(store->snapshot(), mem.snapshot()) << "op " << op;
      const Index first = mem.snapshot().index + 1;
      ASSERT_EQ(store->entries(first, mem.last_index() + 1),
                mem.entries(first, mem.last_index() + 1))
          << "op " << op;
    }
  }
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

TEST_F(FileStorageTest, SnapshotSurvivesRestartAndKeepsLaterEntries) {
  reopen().append({entry(1, 1), entry(1, 2), entry(1, 3), entry(2, 4, "d"),
                   entry(2, 5, "e")});
  store->save_snapshot(rafty::Snapshot{3, 1, "state"});

  auto& s = reopen();
  EXPECT_EQ(s.snapshot(), (rafty::Snapshot{3, 1, "state"}));
  EXPECT_EQ(s.last_index(), 5u);
  EXPECT_EQ(s.term_at(3), 1u);  // boundary term comes from the snapshot
  EXPECT_EQ(s.entries(4, 6),
            (std::vector<LogEntry>{entry(2, 4, "d"), entry(2, 5, "e")}));
}

TEST_F(FileStorageTest, SnapshotNotMatchingLogReplacesWholeLog) {
  // InstallSnapshot from a leader: our short log does not reach index 5.
  reopen().append({entry(1, 1), entry(1, 2)});
  store->save_snapshot(rafty::Snapshot{5, 2, "leader state"});
  auto& s = reopen();
  EXPECT_EQ(s.last_index(), 5u);
  EXPECT_EQ(s.term_at(5), 2u);
}

TEST_F(FileStorageTest, CrashBetweenSnapshotAndWalRewriteLosesNothing) {
  reopen().append({entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4, "d"),
                   entry(1, 5, "e")});
  store.reset();
  const std::string old_wal = read_bytes(wal());

  // Take the snapshot, then put the old WAL back: as if we crashed after the
  // snapshot file was written but before the WAL was rewritten.
  reopen().save_snapshot(rafty::Snapshot{3, 1, "state"});
  store.reset();
  write_bytes(wal(), old_wal);

  auto& s = reopen();
  EXPECT_EQ(s.snapshot().index, 3u);
  ASSERT_EQ(s.last_index(), 5u);  // entries 4..5 kept, 1..3 skipped
  EXPECT_EQ(s.entries(4, 6)[1].data, "e");
  s.append({entry(1, 6, "f")});    // the WAL was cleaned up: appends work
  EXPECT_EQ(reopen().last_index(), 6u);
}

}  // namespace
