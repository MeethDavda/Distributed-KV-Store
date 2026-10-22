#!/usr/bin/env bash
# Starts Grafana + Prometheus + an OTLP collector in one container
# (grafana/otel-lgtm) and loads the raft-kv dashboard.
#
#   tools/grafana-suite/run.sh
#   OTLP=localhost:4317 scripts/run_cluster.sh      # in another terminal
#   open http://localhost:3000   (login admin / admin)
#
# Stop with: docker rm -f raftkv-lgtm

set -euo pipefail
cd "$(dirname "$0")"

docker rm -f raftkv-lgtm >/dev/null 2>&1 || true
docker run -d --name raftkv-lgtm \
  -p 3000:3000 -p 4317:4317 -p 4318:4318 \
  grafana/otel-lgtm >/dev/null
echo "starting grafana/otel-lgtm ..."

# Wait for Grafana's API, then upload the dashboard.
for _ in $(seq 1 60); do
  if curl -sf -u admin:admin http://localhost:3000/api/health >/dev/null; then
    break
  fi
  sleep 1
done
curl -sf -u admin:admin -H 'Content-Type: application/json' \
  -X POST http://localhost:3000/api/dashboards/db \
  -d "{\"dashboard\": $(cat dashboards/raft.json), \"overwrite\": true}" \
  >/dev/null && echo "dashboard loaded"

echo "Grafana:  http://localhost:3000  (admin/admin) -> Dashboards -> raft-kv"
echo "OTLP:     localhost:4317"
