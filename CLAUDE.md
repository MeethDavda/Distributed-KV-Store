# CLAUDE.md

## Project

raft-kv: a fault tolerant, linearizable key value store built on the Raft consensus algorithm, written in C++. Built as a resume/portfolio systems project, so correctness evidence (tests, fault injection, linearizability checks) and measured results matter as much as the code.

## What it solves

- Keeps a KV store available and consistent when nodes crash, restart, or get partitioned
- Replicates a log across a cluster, elects a leader, and applies committed entries to a KV state machine
- Gives exactly once write semantics for retrying clients (RIFL style dedup)
- Recovers from crashes using a WAL and snapshots

## Tech stack

- Language: C++17 or newer (no Go anywhere in this repo)
- Build: CMake
- RPC: gRPC + Protocol Buffers
- Tests: GoogleTest for unit tests, custom harness for integration and fault tests
- Linearizability check: in house C++ checker (`src/lin_checker.cpp`, CLI `app/check_history`). The client library logs call/return history to a file. The checker splits the history by key and runs a Wing and Gong style search with memoization and a timeout.
- Observability: OpenTelemetry C++ SDK exporting OTLP to the `grafana/otel-lgtm` container (Grafana, Prometheus, Tempo, Loki)

## Folder structure

```
.
├── CMakeLists.txt
├── README.md
├── proto/                    # raft.proto, kv.proto, tester.proto
├── inc/
│   ├── rafty/                # raft.hpp, storage.hpp
│   ├── kv/                   # kv_server.hpp (state machine + dedup), kv_client.hpp
│   ├── telemetry/            # metrics.hpp
│   └── toolings/             # test harness, config gen, msg queue, fault injection,
│                             # history.hpp, lin_checker.hpp
├── src/                      # raft.cpp, storage.cpp, kv_server.cpp, kv_client.cpp,
│                             # telemetry.cpp, lin_checker.cpp
├── app/                      # raft_node, kv_node, latency, tput, multinode, check_history
├── unittests/                # includes lin_checker_test.cpp
├── integration_tests/        # elections, partitions, crash recovery, snapshots
├── bench/                    # raw results + plots
├── tools/
│   ├── grafana-suite/        # run.sh + dashboards/raft.json
│   └── grpcr/                # gRPC replay for deterministic debugging
└── scripts/                  # dependency install
```

## Architecture rules (important)

- `raft.cpp` must not do direct network or disk I/O. It talks to `Transport`, `Storage`, and `Clock` interfaces so tests can inject fakes and run deterministically.
- WAL and snapshot code lives in `storage.cpp`, never inside `raft.cpp`.
- Fault injection (drop, delay, partition, kill, restart) goes through `toolings/` and is driven by `tester.proto`.
- Every client write carries (client_id, seq_no). The KV server dedups on this so retries are safe.
- Persist `currentTerm`, `votedFor`, and log entries before replying to any RPC that depends on them. Sync to disk on the commit path.
- Keep functions small and comment the Raft invariant a block protects (term checks, log matching, commit rules).

## Linearizability checker design

- Input: history file of call and return events with timestamps and client ids, written by `kv_client`
- Split by key, since operations on different keys are independent, which keeps each history small
- Per key: Wing and Gong style search. Pick an operation that could be linearized next, apply it to the model (current value of the key), backtrack on mismatch
- Memoize on (set of linearized ops, model state), and apply a timeout
- Output: pass/fail, plus the offending key and a minimal history snippet on failure
- Unit test the checker on hand written good and bad histories (stale read, lost write, reordered writes) before trusting it. A checker that always passes is worse than none.

## Build and run (Never do this without my permission)

```
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j
ctest --output-on-failure              # unit + integration tests
./app/multinode --nodes 3              # local 3 node cluster
./app/check_history <history_file>     # linearizability check
./tools/grafana-suite/run.sh           # Grafana at localhost:3000, OTLP at localhost:4317
```

Adjust paths if the build layout differs. Keep these commands current in README.

## Testing requirements

- Unit: log math, WAL (CRC, torn write), snapshot, dedup table, message queue, linearizability checker
- Integration scenarios: leader election, leader crash, follower crash and rejoin, network partition (minority and majority), crash during snapshot, repeated restarts under load
- Linearizability: record client histories during chaos runs, check with `app/check_history`. A run only counts if the checker passes.
- Run with sanitizers (ASan, TSan) in at least one pass
- Tests must be reproducible: seed all randomness and log the seed on failure

## Results format

Benchmarks write to `bench/results/<date>_<name>.json`:

```json
{
  "commit": "<git sha>",
  "date": "YYYY-MM-DD",
  "hardware": "<machine, cores, disk, OS>",
  "fsync_mode": "none | fsync | fullfsync",
  "cluster_size": 3,
  "workload": {
    "read_pct": 50,
    "value_bytes": 128,
    "clients": 16,
    "duration_s": 60
  },
  "throughput_ops_s": 0,
  "latency_ms": { "p50": 0, "p95": 0, "p99": 0 },
  "notes": "batching on/off, snapshot interval"
}
```

- Plots go in `bench/plots/` (throughput and p99 vs clients, vs cluster size, with and without batching)
- README has one summary table of the best numbers, plus fault test results (scenario, runs, linearizable: pass/fail)
- Never report a number without the commit, hardware, fsync mode, and workload that produced it

## Observability

Export these via OpenTelemetry: term, role per node, commit index, last applied, election count, time to new leader, AppendEntries latency, replication lag per follower, WAL fsync latency, snapshot size and duration, client request p50/p99. One trace per client request with spans for propose, replicate, commit, apply. Dashboard JSON lives in `tools/grafana-suite/dashboards/`.

## macOS notes

- Dev machine is a MacBook Pro. Use Homebrew for grpc, protobuf, abseil, cmake.
- WAL durability: plain fsync does not guarantee a real disk flush on macOS. Use `fcntl(F_FULLFSYNC)`. Make fsync mode a config flag (none, fsync, fullfsync) and record it in every benchmark result.
- No iptables or tc: partition and delay tests use the simulated transport. Real process tests use `kill -9` and restart.
- Laptop numbers are noisy (thermal throttling). Rerun final benchmarks on a Linux VM and record the hardware in the results JSON.
- Raise `ulimit -n` before multi node runs with many clients.
- Check opentelemetry-cpp availability via Homebrew, else use vcpkg or CMake FetchContent.

## Build order

1. Leader election (with simulated transport)
2. Log replication and commit
3. WAL, hard state, restart recovery
4. KV server, client, dedup
5. Snapshots and InstallSnapshot
6. Fault injection, integration tests, history logging, linearizability checker
7. Telemetry and Grafana dashboard
8. Benchmarks, plots, README with numbers

## Working style for Claude

- Go file by file and explain what each file does (what each function does), and only after I say next go to next file or build step. This is for me to understand the project while its being built.
- Work one build step at a time. Finish it with passing tests before starting the next.
- Before writing code for a step, state the Raft invariants involved and how the test will check them.
- Prefer simple and correct over clever. Do not add features outside the build order.
- When a test fails, find the root cause. Do not loosen the test to make it pass.
- Explain non obvious design choices in comments or README so the author can defend them in interviews.
- Ask before adding a new dependency.
