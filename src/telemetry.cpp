#include "telemetry/metrics.hpp"

#include <atomic>
#include <chrono>
#include <vector>

#include "opentelemetry/context/context.h"
#include "opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_options.h"
#include "opentelemetry/metrics/meter.h"
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include "opentelemetry/sdk/metrics/meter_provider_factory.h"
#include "opentelemetry/sdk/metrics/view/view_registry_factory.h"
#include "opentelemetry/sdk/resource/resource.h"

namespace telemetry {

namespace nostd = opentelemetry::nostd;
namespace otel_metrics = opentelemetry::metrics;
namespace sdk_metrics = opentelemetry::sdk::metrics;
namespace otlp = opentelemetry::exporter::otlp;

namespace {

// Observable-gauge callback: OTel calls it on its export thread to read the
// current value, which the loop thread keeps in an atomic.
void observe(otel_metrics::ObserverResult result, void* state) {
  const auto v = static_cast<std::atomic<int64_t>*>(state)->load();
  nostd::get<nostd::shared_ptr<otel_metrics::ObserverResultT<int64_t>>>(result)
      ->Observe(v);
}

}  // namespace

struct Metrics::Impl {
  std::unique_ptr<sdk_metrics::MeterProvider> provider;

  // Gauge values (written by the loop thread, read by the OTel thread).
  std::atomic<int64_t> term{0}, is_leader{0}, commit_index{0}, last_applied{0};
  std::vector<nostd::shared_ptr<otel_metrics::ObservableInstrument>> gauges;

  nostd::unique_ptr<otel_metrics::Counter<uint64_t>> requests;
  nostd::unique_ptr<otel_metrics::Histogram<double>> latency;
};

Metrics::Metrics(uint64_t node_id, const std::string& otlp_endpoint) {
  if (otlp_endpoint.empty()) return;  // metrics off
  impl_ = std::make_unique<Impl>();

  // Exporter: push to the collector every second over OTLP/gRPC.
  otlp::OtlpGrpcMetricExporterOptions exporter_opts;
  exporter_opts.endpoint = otlp_endpoint;
  sdk_metrics::PeriodicExportingMetricReaderOptions reader_opts;
  reader_opts.export_interval_millis = std::chrono::milliseconds(1000);
  reader_opts.export_timeout_millis = std::chrono::milliseconds(500);
  auto reader = sdk_metrics::PeriodicExportingMetricReaderFactory::Create(
      otlp::OtlpGrpcMetricExporterFactory::Create(exporter_opts), reader_opts);

  // Resource attributes identify the node; Grafana shows them as labels.
  const auto resource = opentelemetry::sdk::resource::Resource::Create(
      {{"service.name", "raft-kv"},
       {"service.instance.id", "node-" + std::to_string(node_id)}});
  impl_->provider = sdk_metrics::MeterProviderFactory::Create(
      sdk_metrics::ViewRegistryFactory::Create(), resource);
  impl_->provider->AddMetricReader(std::move(reader));

  auto meter = impl_->provider->GetMeter("raft-kv");
  auto gauge = [&](const char* name, std::atomic<int64_t>& value) {
    auto g = meter->CreateInt64ObservableGauge(name);
    g->AddCallback(observe, &value);
    impl_->gauges.push_back(g);
  };
  gauge("raft_term", impl_->term);
  gauge("raft_is_leader", impl_->is_leader);
  gauge("raft_commit_index", impl_->commit_index);
  gauge("raft_last_applied", impl_->last_applied);
  impl_->requests = meter->CreateUInt64Counter("kv_requests");
  impl_->latency = meter->CreateDoubleHistogram("kv_request_latency_ms");
}

Metrics::~Metrics() {
  if (impl_) impl_->provider->Shutdown();  // flush the last export
}

void Metrics::set_raft_state(uint64_t term, bool is_leader,
                             uint64_t commit_index, uint64_t last_applied) {
  if (!impl_) return;
  impl_->term = static_cast<int64_t>(term);
  impl_->is_leader = is_leader ? 1 : 0;
  impl_->commit_index = static_cast<int64_t>(commit_index);
  impl_->last_applied = static_cast<int64_t>(last_applied);
}

void Metrics::record_request(double latency_ms) {
  if (!impl_) return;
  impl_->requests->Add(1);
  // ABI v1 (Homebrew's build) has no Record(value) overload for histograms.
  impl_->latency->Record(latency_ms, opentelemetry::context::Context{});
}

}  // namespace telemetry
