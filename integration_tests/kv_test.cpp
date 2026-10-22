// Integration tests for the KV service: KvClient -> KvServer -> Raft ->
// StateMachine, all in one process on the simulated network (KvSim).
//
// What these prove, beyond the Raft checks SimCluster runs every step:
//   - exactly-once writes under dropped replies, crashes, and failover
//   - acknowledged writes are never lost
//   - reads never return stale data, even from a deposed leader
//   - reads see every write that finished before they started
//
// The full linearizability checker (Wing & Gong over recorded histories)
// comes in step 6; the chaos test here checks the same properties directly.
//
// Replay a failing chaos seed with:
//   RAFT_SEED=<seed> ./integration_tests --gtest_filter='KvChaos.*'

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "toolings/kv_sim.hpp"
#include "toolings/lin_checker.hpp"

namespace {

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using kv::KvClient;
using kv::KvResponse;
using kv::Status;
using rafty::NodeId;
using toolings::KvSim;

#define EXPECT_NO_VIOLATIONS(kvsim)                                      \
  EXPECT_TRUE((kvsim).cluster().violations().empty())                    \
      << "seed=" << (kvsim).cluster().seed() << " first violation: "     \
      << ((kvsim).cluster().violations().empty()                         \
              ? ""                                                       \
              : (kvsim).cluster().violations().front())

bool wait_for_leader(KvSim& kv, rafty::Millis timeout = 3s) {
  return kv.run_until([&] { return kv.cluster().leader().has_value(); },
                      timeout);
}

// Blocking-style helpers for single-client tests. Each returns false/nullopt
// if the operation did not finish in time.
bool put(KvSim& kv, int c, const std::string& k, const std::string& v) {
  return kv.run_op(c, [&](KvClient& cl, KvClient::Done d) { cl.put(k, v, d); })
      .has_value();
}
bool append(KvSim& kv, int c, const std::string& k, const std::string& v) {
  return kv
      .run_op(c, [&](KvClient& cl, KvClient::Done d) { cl.append(k, v, d); })
      .has_value();
}
bool del(KvSim& kv, int c, const std::string& k) {
  return kv.run_op(c, [&](KvClient& cl, KvClient::Done d) { cl.del(k, d); })
      .has_value();
}
// Outer optional: did it finish. Inner: was the key present.
std::optional<std::optional<std::string>> get(KvSim& kv, int c,
                                              const std::string& k) {
  auto r = kv.run_op(c, [&](KvClient& cl, KvClient::Done d) { cl.get(k, d); });
  if (!r) return std::nullopt;
  return r->found ? std::optional<std::string>(r->value) : std::nullopt;
}

std::vector<std::string> split_tokens(const std::string& s) {
  std::vector<std::string> out;
  std::stringstream ss(s);
  std::string t;
  while (std::getline(ss, t, ';')) {
    if (!t.empty()) out.push_back(t);
  }
  return out;
}

fs::path temp_dir(const std::string& name) {
  return fs::temp_directory_path() /
         ("raftkv-kv-" + name + "-" + std::to_string(::getpid()));
}

// ---------------------------------------------------------------------------
// Basics
// ---------------------------------------------------------------------------

TEST(Kv, PutGetAppendDelete) {
  KvSim kv(3, /*seed=*/31, /*clients=*/1);
  ASSERT_TRUE(wait_for_leader(kv));

  ASSERT_TRUE(put(kv, 1, "x", "a"));
  EXPECT_EQ(get(kv, 1, "x"), std::optional<std::string>("a"));
  ASSERT_TRUE(append(kv, 1, "x", "b"));
  EXPECT_EQ(get(kv, 1, "x"), std::optional<std::string>("ab"));
  ASSERT_TRUE(del(kv, 1, "x"));
  auto gone = get(kv, 1, "x");
  ASSERT_TRUE(gone.has_value());
  EXPECT_FALSE(gone->has_value());
  EXPECT_NO_VIOLATIONS(kv);
}

TEST(Kv, ClientFindsLeaderFromAnyStartingNode) {
  // Clients start by talking to node 1, which is usually not the leader:
  // the NotLeader hint must lead them to the right node.
  KvSim kv(5, /*seed=*/32, /*clients=*/1);
  ASSERT_TRUE(wait_for_leader(kv));
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(put(kv, 1, "k", std::to_string(i)));
  }
  EXPECT_EQ(get(kv, 1, "k"), std::optional<std::string>("9"));
  EXPECT_NO_VIOLATIONS(kv);
}

TEST(Kv, ReadYourWrites) {
  KvSim kv(3, /*seed=*/33, /*clients=*/1);
  ASSERT_TRUE(wait_for_leader(kv));
  std::mt19937_64 rng(33);
  std::map<std::string, std::string> expected;
  for (int i = 0; i < 50; ++i) {
    const std::string key = "k" + std::to_string(rng() % 4);
    const std::string val = "v" + std::to_string(i);
    ASSERT_TRUE(put(kv, 1, key, val));
    expected[key] = val;
    EXPECT_EQ(get(kv, 1, key), std::optional<std::string>(expected[key]));
  }
  EXPECT_NO_VIOLATIONS(kv);
}

// ---------------------------------------------------------------------------
// Exactly-once (K2, K3)
// ---------------------------------------------------------------------------

TEST(KvExactlyOnce, DroppedRepliesNeverDuplicateAppends) {
  // 30% of replies vanish: the write committed but the client never hears,
  // so it retries with the same seq. Dedup must make each token appear once.
  KvSim kv(3, /*seed=*/34, /*clients=*/1);
  kv.set_reply_drop_rate(0.3);
  ASSERT_TRUE(wait_for_leader(kv));

  std::string expected;
  for (int i = 0; i < 50; ++i) {
    const std::string tok = "t" + std::to_string(i) + ";";
    ASSERT_TRUE(append(kv, 1, "log", tok)) << "token " << i;
    expected += tok;
  }
  kv.set_reply_drop_rate(0.0);
  EXPECT_EQ(get(kv, 1, "log"), std::optional<std::string>(expected));
  EXPECT_NO_VIOLATIONS(kv);
}

TEST(KvExactlyOnce, LeaderCrashMidWriteDoesNotDuplicate) {
  // Crash the leader while a write is in flight, many times. The write may
  // or may not have committed; either way the retry (to the new leader)
  // must not apply it a second time, because the dedup table is replicated.
  KvSim kv(5, /*seed=*/35, /*clients=*/1);
  ASSERT_TRUE(wait_for_leader(kv));

  std::string expected;
  for (int i = 0; i < 20; ++i) {
    const std::string tok = "t" + std::to_string(i) + ";";
    auto done = std::make_shared<bool>(false);
    kv.client(1).append("log", tok, [done](const KvResponse&) { *done = true; });
    kv.run_for(std::chrono::milliseconds(2 + i % 5));  // write is in flight

    if (auto l = kv.cluster().leader()) {
      kv.crash(*l);
      ASSERT_TRUE(kv.run_until([&] { return *done; }, 5s)) << "token " << i;
      kv.restart(*l);
    } else {
      ASSERT_TRUE(kv.run_until([&] { return *done; }, 5s)) << "token " << i;
    }
    expected += tok;
  }
  EXPECT_EQ(get(kv, 1, "log"), std::optional<std::string>(expected));
  EXPECT_NO_VIOLATIONS(kv);
}

// ---------------------------------------------------------------------------
// Stale reads (K5, K6)
// ---------------------------------------------------------------------------

TEST(KvReads, DeposedLeaderNeverServesStaleRead) {
  KvSim kv(5, /*seed=*/36, /*clients=*/1);
  ASSERT_TRUE(wait_for_leader(kv));
  ASSERT_TRUE(put(kv, 1, "x", "old"));
  const NodeId old_leader = *kv.cluster().leader();

  // Old leader + one follower on the minority side.
  std::vector<NodeId> minority{old_leader};
  std::vector<NodeId> majority;
  for (NodeId id : kv.cluster().ids()) {
    if (id == old_leader) continue;
    (minority.size() < 2 ? minority : majority).push_back(id);
  }
  kv.cluster().net().partition({minority, majority});

  // Ask the old leader for x right away, while it still believes it leads.
  auto read = std::make_shared<std::optional<KvResponse>>();
  kv::KvRequest req;
  req.op = kv::Op::Get;
  req.client_id = 99;
  req.seq = 1;
  req.key = "x";
  kv.live_server(old_leader)->handle(req, [read](const KvResponse& r) {
    *read = r;
  });

  // Meanwhile the majority elects a new leader and overwrites x. The client
  // can reach every node, so it finds the new leader.
  ASSERT_TRUE(put(kv, 1, "x", "new"));
  kv.run_until([&] { return read->has_value(); }, 3s);

  // The old leader could not get a majority to confirm the read, and
  // check-quorum removed it: the read must be refused, never "old".
  ASSERT_TRUE(read->has_value()) << "read was never answered";
  EXPECT_EQ((*read)->status, Status::NotLeader);
  EXPECT_NO_VIOLATIONS(kv);
}

// ---------------------------------------------------------------------------
// Durability through the KV layer
// ---------------------------------------------------------------------------

TEST(KvDurability, AcknowledgedWritesSurviveWholeClusterRestart) {
  // 50 writes > the snapshot interval (20), so the restart loads snapshot
  // files and replays only the log after them.
  KvSim kv(3, /*seed=*/37, /*clients=*/1, temp_dir("restart"));
  ASSERT_TRUE(wait_for_leader(kv));
  for (int i = 0; i < 50; ++i) {
    ASSERT_TRUE(put(kv, 1, "k" + std::to_string(i), "v" + std::to_string(i)));
  }

  for (NodeId id : kv.cluster().ids()) kv.crash(id);
  for (NodeId id : kv.cluster().ids()) kv.restart(id);

  ASSERT_TRUE(wait_for_leader(kv));
  for (int i = 0; i < 50; ++i) {
    EXPECT_EQ(get(kv, 1, "k" + std::to_string(i)),
              std::optional<std::string>("v" + std::to_string(i)));
  }
  EXPECT_NO_VIOLATIONS(kv);
}

TEST(KvSnapshots, FarBehindFollowerCatchesUpViaInstallSnapshot) {
  KvSim kv(3, /*seed=*/38, /*clients=*/1);
  ASSERT_TRUE(wait_for_leader(kv));
  const NodeId leader = *kv.cluster().leader();
  const NodeId f = kv.cluster().ids()[0] == leader ? kv.cluster().ids()[1]
                                                   : kv.cluster().ids()[0];
  kv.crash(f);

  // 100 writes while it is down: the leader compacts its log several times,
  // so the entries the follower is missing no longer exist as log entries.
  for (int i = 0; i < 100; ++i) {
    ASSERT_TRUE(put(kv, 1, "k" + std::to_string(i % 10), std::to_string(i)));
  }
  ASSERT_GT(kv.cluster().storage(*kv.cluster().leader()).snapshot().index, 0u);

  kv.restart(f);
  ASSERT_TRUE(kv.run_until(
      [&] {
        const auto* fs = kv.state(f);
        const auto* ls = kv.state(*kv.cluster().leader());
        return fs && ls && fs->data() == ls->data();
      },
      3s))
      << "follower did not catch up";
  EXPECT_NO_VIOLATIONS(kv);
}

// ---------------------------------------------------------------------------
// Chaos: several clients, all fault types, direct property checks
// ---------------------------------------------------------------------------

// Each client appends unique tokens "c<id>-<n>" to one of a few keys and
// sometimes reads. Checked:
//   - every read contains every token acknowledged to that key BEFORE the
//     read started (no stale reads, real-time order), each token at most
//     once, and only tokens that were actually proposed
//   - at the end, every acknowledged token appears exactly once, and each
//     client's tokens appear in the order it issued them
void run_chaos(uint64_t seed) {
  SCOPED_TRACE("replay with RAFT_SEED=" + std::to_string(seed));
  constexpr int kClients = 4;
  const std::vector<std::string> keys{"a", "b", "c"};

  KvSim kv(5, seed, kClients);
  kv.cluster().net().set_drop_rate(0.05);
  kv.cluster().net().set_delay(1ms, 10ms);
  kv.set_request_drop_rate(0.1);
  kv.set_reply_drop_rate(0.2);

  std::mt19937_64 rng(seed ^ 0xC0FFEEULL);
  std::map<std::string, std::set<std::string>> proposed, acked;
  std::vector<int> next_token(kClients + 1, 0);
  std::set<NodeId> down;
  int reads_checked = 0;

  auto check_read = [&](const std::string& key, const KvResponse& r,
                        const std::set<std::string>& must_contain) {
    ++reads_checked;
    std::set<std::string> seen;
    for (const auto& t : split_tokens(r.found ? r.value : "")) {
      EXPECT_TRUE(seen.insert(t).second) << "duplicate " << t << " in " << key;
      EXPECT_TRUE(proposed[key].count(t)) << "never proposed: " << t;
    }
    for (const auto& t : must_contain) {
      EXPECT_TRUE(seen.count(t)) << "stale read of " << key << ": missing " << t;
    }
  };

  // Start a random operation on every idle client.
  // Every operation's [call, return] interval, for the linearizability check.
  std::vector<toolings::Operation> history;
  auto now = [&] { return kv.cluster().clock().now().count(); };

  auto issue = [&] {
    for (int c = 1; c <= kClients; ++c) {
      KvClient& cl = kv.client(c);
      if (cl.busy()) continue;
      const std::string key = keys[rng() % keys.size()];
      const std::size_t h = history.size();  // index: the vector may grow
      if (rng() % 4 == 0) {
        history.push_back({static_cast<uint64_t>(c), toolings::OpKind::Get, key,
                           "", false, now(), toolings::kNever});
        auto before = acked[key];  // must all be visible to this read
        cl.get(key, [&, key, before, h](const KvResponse& r) {
          history[h].found = r.found;
          history[h].value = r.value;
          history[h].ret = now();
          check_read(key, r, before);
        });
      } else {
        const std::string tok =
            "c" + std::to_string(c) + "-" + std::to_string(next_token[c]++);
        proposed[key].insert(tok);
        history.push_back({static_cast<uint64_t>(c), toolings::OpKind::Append,
                           key, tok + ";", false, now(), toolings::kNever});
        cl.append(key, tok + ";", [&, key, tok, h](const KvResponse&) {
          history[h].ret = now();
          acked[key].insert(tok);
        });
      }
    }
  };

  const auto end = kv.cluster().clock().now() + 15s;
  while (kv.cluster().clock().now() < end) {
    const int ms = std::uniform_int_distribution<int>(100, 800)(rng);
    for (int t = 0; t < ms; ++t) {
      issue();
      kv.step();
    }
    switch (rng() % 4) {
      case 0: {
        const NodeId n = kv.cluster().ids()[rng() % 5];
        if (down.size() < 2 && !down.count(n)) {
          kv.crash(n);
          down.insert(n);
        }
        break;
      }
      case 1:
        if (!down.empty()) {
          kv.restart(*down.begin());
          down.erase(down.begin());
        }
        break;
      case 2: {
        std::vector<NodeId> ids = kv.cluster().ids();
        std::shuffle(ids.begin(), ids.end(), rng);
        const auto cut = static_cast<long>(1 + rng() % 4);
        kv.cluster().net().partition(
            {{ids.begin(), ids.begin() + cut}, {ids.begin() + cut, ids.end()}});
        break;
      }
      case 3:
        kv.cluster().net().heal();
        break;
    }
    if (::testing::Test::HasFailure()) return;
  }

  // Repair; let every in-flight operation finish.
  for (NodeId n : down) kv.restart(n);
  kv.cluster().net().heal();
  kv.cluster().net().set_drop_rate(0.0);
  kv.set_request_drop_rate(0.0);
  kv.set_reply_drop_rate(0.0);
  ASSERT_TRUE(kv.run_until(
      [&] {
        for (int c = 1; c <= kClients; ++c) {
          if (kv.client(c).busy()) return false;
        }
        return true;
      },
      10s))
      << "operations did not finish after repair";

  // Final state: every acked token exactly once, per-client order kept.
  for (const auto& key : keys) {
    auto value = get(kv, 1, key);
    ASSERT_TRUE(value.has_value());
    std::map<std::string, int> count;
    std::map<std::string, int> last_n;  // per client prefix "cX"
    for (const auto& t : split_tokens(value->value_or(""))) {
      ++count[t];
      const auto dash = t.find('-');
      const std::string who = t.substr(0, dash);
      const int n = std::stoi(t.substr(dash + 1));
      auto it = last_n.find(who);
      EXPECT_TRUE(it == last_n.end() || n > it->second)
          << who << " tokens out of order in " << key;
      last_n[who] = n;
    }
    for (const auto& t : acked[key]) {
      EXPECT_EQ(count[t], 1) << "acked token " << t << " in " << key;
    }
    for (const auto& [t, n] : count) {
      EXPECT_EQ(n, 1) << "token " << t << " applied " << n << " times";
    }
  }
  EXPECT_GT(reads_checked, 0);

  // The full check: is the whole recorded history linearizable?
  const auto lin = toolings::check_linearizable(history);
  EXPECT_EQ(lin.verdict, toolings::LinResult::Verdict::Ok)
      << "key " << lin.key << " (" << history.size() << " ops):\n"
      << lin.detail;
  EXPECT_NO_VIOLATIONS(kv);
}

TEST(KvChaos, ClientsUnderAllFaultsManySeeds) {
  if (const char* s = std::getenv("RAFT_SEED")) {
    run_chaos(std::strtoull(s, nullptr, 10));
    return;
  }
  for (uint64_t seed = 1; seed <= 30; ++seed) {
    run_chaos(seed);
    if (HasFailure()) return;
  }
}

}  // namespace
