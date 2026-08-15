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

    let exporter = match opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()
    {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!("OTLP metrics exporter init failed; metrics disabled: {e}");
            return None;
        }
    };

    let reader = PeriodicReader::builder(exporter).build();
    let resource = Resource::builder()
        .with_service_name("zero-cache")
        .with_attribute(KeyValue::new(
            "service.version",
            service_version.to_string(),
        ))
        .build();

    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();

    global::set_meter_provider(provider.clone());
    tracing::info!("OTLP metrics export enabled (meter=zero)");
    Some(provider)
}
