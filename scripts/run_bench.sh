#!/usr/bin/env bash
# Benchmark sweep: for each fsync mode, start a fresh 3-node cluster and run
# bench with 1, 4, 16, 32 clients. Results land in bench/results/.
#
#   cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
#   cmake --build build-release -j
#   scripts/run_bench.sh
#
#   MODES="fullfsync none" CLIENTS="1 16" DURATION=10 scripts/run_bench.sh

set -euo pipefail

BIN=${BIN:-build-release/app}
MODES=${MODES:-"fullfsync fsync none"}
CLIENTS=${CLIENTS:-"1 4 16 32"}
DURATION=${DURATION:-20}
PEERS="1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053"

for f in kv_node bench; do
  [[ -x "$BIN/$f" ]] || { echo "missing $BIN/$f (build Release first)" >&2; exit 1; }
done

for mode in $MODES; do
  echo "=== fsync=$mode ==="
  rm -rf bench/tmp && mkdir -p bench/tmp
  PIDS=()
  for i in 1 2 3; do
    "$BIN/kv_node" --id "$i" --peers "$PEERS" --data "bench/tmp/n$i" \
      --fsync "$mode" >"bench/tmp/n$i.log" 2>&1 &
    PIDS+=($!)
  done
  sleep 2  # leader election

  for c in $CLIENTS; do
    "$BIN/bench" --peers "$PEERS" --clients "$c" --duration "$DURATION" \
      --fsync "$mode" --name "c${c}_${mode}" \
      --notes "3 local processes on one machine, snapshot every 1000"
  done

  kill "${PIDS[@]}" 2>/dev/null || true
  wait 2>/dev/null || true
done
rm -rf bench/tmp
echo "done: bench/results/   (plot with: python3 scripts/plot_bench.py)"
