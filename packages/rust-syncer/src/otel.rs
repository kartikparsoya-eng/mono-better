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

    // Export every 10s (a manual PeriodicReader ignores OTEL_METRIC_EXPORT_INTERVAL;
    // the default is 60s). 10s gives timely delivery without excessive traffic.
    let reader = PeriodicReader::builder(exporter)
        .with_interval(std::time::Duration::from_secs(10))
        .build();
    let resource = Resource::builder()
        .with_service_name("zero-cache")
        .with_attribute(KeyValue::new(
            "service.version",
            service_version.to_string(),
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
    const NATIVE_HISTOGRAM_INSTRUMENTS: [&str; 2] = [
        "zero.sync.e2e_serving_lag",
        "zero.sync.view_syncer_hydration",
    ];
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
