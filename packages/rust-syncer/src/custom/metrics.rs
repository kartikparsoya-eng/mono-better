//! Port of `zero-cache/src/custom/metrics.ts` — the query-API request
//! instruments (`zero.server.api.*`) and their recorders (L9 Stage 5b move
//! out of the process metric registry).

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram as OtelHistogram, UpDownCounter};
use std::sync::OnceLock;

/// Query-API request instruments — TS `custom/metrics.ts` (#6203):
/// `zero.server.api.requests` / `api.request_duration` / `api.attempts` /
/// `api.attempt_duration` / `api.in_flight`, recorded around the transform
/// fetch with `operation: "query"`. No-op when OTLP is disabled.
struct ApiOtel {
    requests: Counter<u64>,
    request_duration: OtelHistogram<f64>,
    attempts: Counter<u64>,
    attempt_duration: OtelHistogram<f64>,
    in_flight: UpDownCounter<i64>,
}

/// TS `API_DURATION_HISTOGRAM_BOUNDARIES_S`.
const API_DURATION_BOUNDARIES_S: [f64; 16] = [
    0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0,
];

fn api_otel() -> &'static ApiOtel {
    static INSTRUMENTS: OnceLock<ApiOtel> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let m = global::meter("zero");
        ApiOtel {
            requests: m
                .u64_counter("zero.server.api.requests")
                .with_description("API requests, labeled by operation and result.")
                .build(),
            request_duration: m
                .f64_histogram("zero.server.api.request_duration")
                .with_unit("s")
                .with_description("End-to-end API request duration, including retries.")
                .with_boundaries(API_DURATION_BOUNDARIES_S.to_vec())
                .build(),
            attempts: m
                .u64_counter("zero.server.api.attempts")
                .with_description("API HTTP fetch attempts")
                .build(),
            attempt_duration: m
                .f64_histogram("zero.server.api.attempt_duration")
                .with_unit("s")
                .with_description("API HTTP fetch attempt duration, excluding retry sleep.")
                .with_boundaries(API_DURATION_BOUNDARIES_S.to_vec())
                .build(),
            in_flight: m
                .i64_up_down_counter("zero.server.api.in_flight")
                .with_description("API requests currently in flight.")
                .build(),
        }
    })
}

fn api_request_metric_attrs(result: &'static str) -> [opentelemetry::KeyValue; 2] {
    [
        opentelemetry::KeyValue::new("operation", "query"),
        opentelemetry::KeyValue::new("result", result),
    ]
}

/// One completed API request (all attempts) — TS `apiRequests().add(1, attrs)`.
pub fn record_api_request(result: &'static str) {
    api_otel()
        .requests
        .add(1, &api_request_metric_attrs(result));
}

/// End-to-end request duration in ms (including retry sleeps).
pub fn record_api_request_duration(elapsed_ms: f64) {
    api_otel().request_duration.record(
        elapsed_ms / 1000.0,
        &[opentelemetry::KeyValue::new("operation", "query")],
    );
}

/// One HTTP fetch attempt — TS `recordApiAttempt`, with the same attempt
/// number + HTTP status attributes TS records (custom/metrics.ts:40-48), so a
/// dashboard can split retries-by-attempt and errors-by-status.
pub fn record_api_attempt(
    result: &'static str,
    will_retry: bool,
    elapsed_ms: f64,
    attempt: u32,
    http_status: Option<u16>,
) {
    let mut attrs = vec![
        opentelemetry::KeyValue::new("operation", "query"),
        opentelemetry::KeyValue::new("result", result),
        opentelemetry::KeyValue::new("will_retry", will_retry),
        opentelemetry::KeyValue::new("attempt", attempt as i64),
    ];
    if let Some(code) = http_status {
        attrs.push(opentelemetry::KeyValue::new(
            "http_status_code",
            code as i64,
        ));
        attrs.push(opentelemetry::KeyValue::new(
            "http_status_class",
            format!("{}xx", code / 100),
        ));
    }
    api_otel().attempts.add(1, &attrs);
    api_otel().attempt_duration.record(
        elapsed_ms / 1000.0,
        &[opentelemetry::KeyValue::new("operation", "query")],
    );
}

/// In-flight request delta (+1 on start, -1 on completion) — TS labels this by
/// operation (custom/fetch.ts:116).
pub fn record_api_in_flight(delta: i64) {
    api_otel()
        .in_flight
        .add(delta, &[opentelemetry::KeyValue::new("operation", "query")]);
}
