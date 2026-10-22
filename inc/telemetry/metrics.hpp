#pragma once

// Node metrics, exported via OpenTelemetry (OTLP/gRPC) to the
// grafana/otel-lgtm container (tools/grafana-suite/run.sh).
//
// Metrics (each tagged with the node via the service.instance.id resource):
//   raft_term, raft_is_leader, raft_commit_index, raft_last_applied  (gauges)
//   kv_requests_total                                               (counter)
//   kv_request_latency_ms                         (histogram -> p50/p99)
//
// The OpenTelemetry types are hidden behind a pointer (pimpl), so nothing
// else in the project includes OTel headers. An empty endpoint disables
// metrics entirely (every call becomes a no-op).
//
// Thread safety: OTel instruments are thread-safe, so the loop thread can set
// the gauges while gRPC threads record request latencies.

#include <cstdint>
#include <memory>
#include <string>

namespace telemetry {

class Metrics {
 public:
  Metrics(uint64_t node_id, const std::string& otlp_endpoint);
  ~Metrics();

  Metrics(const Metrics&) = delete;
  Metrics& operator=(const Metrics&) = delete;

  void set_raft_state(uint64_t term, bool is_leader, uint64_t commit_index,
                      uint64_t last_applied);
  void record_request(double latency_ms);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;  // null when disabled
};

}  // namespace telemetry
