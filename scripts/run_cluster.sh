#!/usr/bin/env bash
# Starts a local raft-kv cluster: N kv_node processes on 127.0.0.1.
#
#   scripts/run_cluster.sh            # 3 nodes, fsync=fullfsync
#   NODES=5 FSYNC=none scripts/run_cluster.sh
#   OTLP=localhost:4317 scripts/run_cluster.sh   # export metrics to Grafana
#
# Node i listens on port 50050+i and stores its data in data/n<i>. Logs go to
# data/n<i>.log. Ctrl-C stops every node. To test crash recovery, find the
# leader in the logs ("term N: leader"), kill -9 its pid (printed below),
# then restart it with the command printed for that node.

set -euo pipefail

NODES=${NODES:-3}
FSYNC=${FSYNC:-fullfsync}
BIN=${BIN:-build/app/kv_node}

if [[ ! -x "$BIN" ]]; then
  echo "kv_node not found at $BIN (build first: cmake --build build -j)" >&2
  exit 1
fi

# Every node gets the same full peer list, including itself.
PEERS=""
for i in $(seq 1 "$NODES"); do
  PEERS+="${PEERS:+,}$i=127.0.0.1:$((50050 + i))"
done

mkdir -p data
PIDS=()
for i in $(seq 1 "$NODES"); do
  CMD=("$BIN" --id "$i" --peers "$PEERS" --data "data/n$i" --fsync "$FSYNC")
  if [[ -n "${OTLP:-}" ]]; then CMD+=(--otlp "$OTLP"); fi
  "${CMD[@]}" >"data/n$i.log" 2>&1 &
  PIDS+=($!)
  echo "node $i  pid $!  restart: ${CMD[*]} >>data/n$i.log 2>&1 &"
done

echo
echo "client:  build/app/kv_cli --peers $PEERS put x 1"
echo "logs:    tail -f data/n*.log"
echo "Ctrl-C to stop all nodes."

trap 'kill "${PIDS[@]}" 2>/dev/null; wait; echo stopped' INT TERM
wait
