//! OpenTelemetry OTLP metrics export — the same mechanism the TS zero-cache
//! uses (`server/otel-start.ts`: a NodeSDK that PUSHES OTLP to a collector).
//!
//! Transport is OTLP over **HTTP/protobuf**, matching the TS exporter
//! (`@opentelemetry/exporter-metrics-otlp-http`, collector port 4318) — NOT
//! gRPC — so both engines push to the same collector endpoint the sandbox wires
//! (`OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318`). Using gRPC/tonic
//! here would target 4317 and fail against the HTTP receiver.
//!
//! We build an `SdkMeterProvider` with a `PeriodicReader` + OTLP/HTTP exporter
//! and install it as the global meter provider, so instruments created from
//! `global::meter("zero")` (see [`crate::metrics`]) export over OTLP. Gating and
//! endpoint discovery mirror TS `otel/src/enabled.ts` / the standard `OTEL_*`
//! env vars: metrics are enabled iff `OTEL_EXPORTER_OTLP_ENDPOINT`,
//! `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`, or `OTEL_METRICS_EXPORTER` is set. The
//! HTTP exporter reads `OTEL_EXPORTER_OTLP_ENDPOINT` (default
//! `http://localhost:4318`) and POSTs to its `/v1/metrics` path.

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};

/// Port of TS `otelMetricsEnabled()`.
fn metrics_enabled() -> bool {
    [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        "OTEL_METRICS_EXPORTER",
    ]
    .iter()
    .any(|k| std::env::var(k).map(|v| !v.is_empty()).unwrap_or(false))
}

/// The instruments exported as base2 exponential histograms — TS
/// `NATIVE_HISTOGRAM_INSTRUMENT_NAMES` (observability/metrics.ts:25-29),
/// verbatim and in TS order. `server/otel-start.ts:75` maps every name in it to
/// an `EXPONENTIAL_HISTOGRAM` view, so an instrument missing here is exported as
/// an explicit-bucket histogram and the two arms emit different OTLP types for
/// it — exactly what this list exists to prevent.
pub(crate) const NATIVE_HISTOGRAM_INSTRUMENTS: [&str; 3] = [
    "zero.sync.view_syncer_lag",
    "zero.sync.view_syncer_hydration",
    "zero.sync.e2e_serving_lag",
];

/// Initialize OTLP metrics export and install the global meter provider. Returns
/// the provider (keep it alive for the process lifetime; drop/`shutdown()` on
/// exit flushes a final batch). Returns `None` when metrics are disabled, so the
/// global meter stays a no-op and instruments cost nothing.
///
/// MUST be called BEFORE any instruments are created (i.e. before
/// [`crate::metrics::Metrics::default`]) so they bind to the SDK provider.
pub fn init_metrics(service_version: &str) -> Option<SdkMeterProvider> {
    if !metrics_enabled() {
        return None;
    }

    // opentelemetry-otlp 0.32's HTTP exporter does NOT append the `/v1/metrics`
    // signal path to `OTEL_EXPORTER_OTLP_ENDPOINT` — it POSTs to the base URL,
    // which the collector's OTLP/HTTP receiver answers with 404 (verified: POST
    // `:4318` → 404, `:4318/v1/metrics` → 200), so metrics are silently dropped.
    // Per the OTLP spec the base endpoint must have the signal path appended,
    // while `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` is used verbatim. Build the URL
    // ourselves so metrics land at the same path the TS/node exporter uses.
    use opentelemetry_otlp::WithExportConfig as _;
    let mut builder = opentelemetry_otlp::MetricExporter::builder().with_http();
    if std::env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT").is_err()
        && let Ok(base) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        && !base.is_empty()
    {
        let base = base.trim_end_matches('/');
        builder = builder.with_endpoint(format!("{base}/v1/metrics"));
    }
    let exporter = match builder.build() {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!("OTLP metrics exporter init failed; metrics disabled: {e}");
            return None;
        }
    };

    // Honor OTEL_METRIC_EXPORT_INTERVAL (standard OTel env, milliseconds —
    // the TS NodeSDK reader reads it too); a manual PeriodicReader would
    // otherwise silently ignore it. Default 10s: timely delivery without
    // excessive traffic (the OTel spec default is 60s).
    let interval_ms = std::env::var("OTEL_METRIC_EXPORT_INTERVAL")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(10_000);
    let reader = PeriodicReader::builder(exporter)
        .with_interval(std::time::Duration::from_millis(interval_ms))
        .build();
    // TS builds the same resource in server/otel-start.ts:50-61 — service
    // version plus `process.worker` / `process.worker_index` — and additionally
    // runs NodeSDK with `autoDetectResources: true`, which is where `host.name`
    // and the `process.*` attributes on every TS series come from.
    //
    // Rust set only service.name + service.version, so rust-syncer's metrics
    // arrived at the collector with NO host or worker identity at all. TS's own
    // comment says why that matters: without the worker tags, "N syncer workers
    // sharing the same pod labels clobber each other in the OTel collector on
    // every scrape interval" — and with no host.name, rust's series cannot be
    // attributed to a pod at all in a real deployment. It also made the two
    // arms indistinguishable when diffing a shared collector.
    //
    // Rust is ONE process whose shards are threads, so there is no worker fan-
    // out to disambiguate and TS's `process.worker_index` is deliberately NOT
    // ported: it exists to separate N syncer processes, and a constant 0 would
    // be a label carrying no information. `process.worker` IS kept — it names
    // the ROLE (the TS worker whose job rust performs), and the node
    // change-streamer / dispatcher / serving-replicator that ship in the SAME
    // rust image already tag themselves that way, so without it rust-syncer
    // would be the only component in its own container with no worker
    // identity.
    let resource = Resource::builder()
        .with_service_name("zero-cache")
        .with_attribute(KeyValue::new(
            "service.version",
            service_version.to_string(),
        ))
        .with_attribute(KeyValue::new("process.worker", "syncer"))
        .with_attribute(KeyValue::new("process.runtime.name", "rust"))
        // Docker/Kubernetes set HOSTNAME to the container/pod name; this is the
        // same value NodeSDK's host detector reports for the TS arm.
        .with_attribute(KeyValue::new(
            "host.name",
            std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()),
        ))
        .build();

    // TS exports its "native" latency histograms as base2 EXPONENTIAL
    // histograms (observability/metrics.ts NATIVE_HISTOGRAM_INSTRUMENT_NAMES →
    // an exponential-histogram View in otel-start.ts). Match that here — with
    // fixed explicit boundaries capped at 30s, everything above 30s landed in
    // +Inf (truncating exactly the stuck-then-recovered tail the serving-lag
    // metric exists to expose) and the two implementations exported different
    // OTLP data types, so dashboards could not aggregate them together.
    // max_size 160 matches the JS SDK's exponential-histogram default.
    let native_histogram_view =
        |instrument: &opentelemetry_sdk::metrics::Instrument|
         -> Option<opentelemetry_sdk::metrics::Stream> {
            if !NATIVE_HISTOGRAM_INSTRUMENTS.contains(&instrument.name()) {
                return None;
            }
            opentelemetry_sdk::metrics::Stream::builder()
                .with_aggregation(
                    opentelemetry_sdk::metrics::Aggregation::Base2ExponentialHistogram {
                        max_size: 160,
                        max_scale: 20,
                        record_min_max: true,
                    },
                )
                .build()
                .ok()
        };

    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .with_view(native_histogram_view)
        .build();

    global::set_meter_provider(provider.clone());
    tracing::info!("OTLP metrics export enabled (meter=zero)");
    Some(provider)
}

#[cfg(test)]
mod tests {
    /// TS `NATIVE_HISTOGRAM_INSTRUMENT_NAMES` (server/otel-start.ts) has exactly
    /// these two. Mutation test: an earlier version also listed
    /// `zero.sync.e2e_serving_lag`, exporting it as an exponential histogram
    /// while TS exports explicit buckets.
    #[test]
    fn native_histogram_list_is_the_ts_list() {
        // Compared as slices, not arrays: an added/removed instrument must fail the
        // assertion at runtime rather than only failing to type-check.
        // The literal is TS `NATIVE_HISTOGRAM_INSTRUMENT_NAMES`
        // (observability/metrics.ts:25-29) transcribed in TS order.
        assert_eq!(
            super::NATIVE_HISTOGRAM_INSTRUMENTS.as_slice(),
            [
                "zero.sync.view_syncer_lag",
                "zero.sync.view_syncer_hydration",
                "zero.sync.e2e_serving_lag"
            ]
            .as_slice()
        );
    }
}
