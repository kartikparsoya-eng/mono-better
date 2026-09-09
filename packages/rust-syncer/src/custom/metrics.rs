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

/// The error fields TS lifts from a non-2xx API body
/// (`apiResponseErrorMetricAttrs`, custom/fetch.ts:528-547: `error_kind =
/// errorBody.kind`, `error_reason = errorBody.reason` when present).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ApiErrorAttrs {
    pub kind: String,
    pub reason: Option<String>,
}

impl ApiErrorAttrs {
    /// TS parses the error body as JSON and reads `kind`/`reason`; a body
    /// that is not an object with a string `kind` contributes nothing.
    pub fn from_body(body: &str) -> Option<ApiErrorAttrs> {
        let v: serde_json::Value = serde_json::from_str(body).ok()?;
        let kind = v.get("kind")?.as_str()?.to_string();
        let reason = v.get("reason").and_then(|r| r.as_str()).map(str::to_string);
        Some(ApiErrorAttrs { kind, reason })
    }
}

/// `apiResponseErrorMetricAttrs` (custom/fetch.ts:528-547): `http_status_code`
/// and `http_status_class` when there was a response, `error_kind` (plus
/// `error_reason`) when there was an error body.
fn push_response_error_attrs(
    attrs: &mut Vec<opentelemetry::KeyValue>,
    http_status: Option<u16>,
    error: Option<&ApiErrorAttrs>,
) {
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
    if let Some(e) = error {
        attrs.push(opentelemetry::KeyValue::new("error_kind", e.kind.clone()));
        if let Some(reason) = &e.reason {
            attrs.push(opentelemetry::KeyValue::new("error_reason", reason.clone()));
        }
    }
}

/// `apiRequestMetricAttrs` (custom/fetch.ts:516-526): base `{operation}` +
/// `result` + `attempt_count` + the response/error attrs. `operation` is
/// always `query` here — the push relay's fetches are TS's own
/// (`rust-push-relay.ts`).
pub fn api_request_attrs(
    result: &'static str,
    attempt_count: u32,
    http_status: Option<u16>,
    error: Option<&ApiErrorAttrs>,
) -> Vec<opentelemetry::KeyValue> {
    let mut attrs = vec![
        opentelemetry::KeyValue::new("operation", "query"),
        opentelemetry::KeyValue::new("result", result),
        opentelemetry::KeyValue::new("attempt_count", attempt_count as i64),
    ];
    push_response_error_attrs(&mut attrs, http_status, error);
    attrs
}

/// `recordApiAttempt`'s attrs (custom/fetch.ts:549-568): base + `attempt` +
/// `result` + `will_retry` + the response/error attrs.
pub fn api_attempt_attrs(
    result: &'static str,
    will_retry: bool,
    attempt: u32,
    http_status: Option<u16>,
    error: Option<&ApiErrorAttrs>,
) -> Vec<opentelemetry::KeyValue> {
    let mut attrs = vec![
        opentelemetry::KeyValue::new("operation", "query"),
        opentelemetry::KeyValue::new("attempt", attempt as i64),
        opentelemetry::KeyValue::new("result", result),
        opentelemetry::KeyValue::new("will_retry", will_retry),
    ];
    push_response_error_attrs(&mut attrs, http_status, error);
    attrs
}

/// One completed API request (all attempts) — TS's `finally` in
/// `fetchFromAPIServer` (custom/fetch.ts:365-372): `apiRequests().add(1,
/// attrs)` AND `apiRequestDuration().recordMs(…, attrs)` with the SAME attrs.
pub fn record_api_request(
    result: &'static str,
    attempt_count: u32,
    elapsed_ms: f64,
    http_status: Option<u16>,
    error: Option<&ApiErrorAttrs>,
) {
    let attrs = api_request_attrs(result, attempt_count, http_status, error);
    api_otel().requests.add(1, &attrs);
    api_otel()
        .request_duration
        .record(elapsed_ms / 1000.0, &attrs);
}

/// One HTTP fetch attempt — TS `recordApiAttempt` (custom/fetch.ts:549-568):
/// `apiAttempts().add(1, attrs)` AND `apiAttemptDuration().recordMs(…, attrs)`
/// with the SAME attrs.
pub fn record_api_attempt(
    result: &'static str,
    will_retry: bool,
    elapsed_ms: f64,
    attempt: u32,
    http_status: Option<u16>,
    error: Option<&ApiErrorAttrs>,
) {
    let attrs = api_attempt_attrs(result, will_retry, attempt, http_status, error);
    api_otel().attempts.add(1, &attrs);
    api_otel()
        .attempt_duration
        .record(elapsed_ms / 1000.0, &attrs);
}

/// In-flight request delta (+1 on start, -1 on completion) — TS labels this by
/// operation (custom/fetch.ts:116).
pub fn record_api_in_flight(delta: i64) {
    api_otel()
        .in_flight
        .add(delta, &[opentelemetry::KeyValue::new("operation", "query")]);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(attrs: &[opentelemetry::KeyValue]) -> Vec<String> {
        attrs.iter().map(|kv| kv.key.to_string()).collect()
    }

    /// TS `apiRequestMetricAttrs` (custom/fetch.ts:516-526) +
    /// `apiResponseErrorMetricAttrs` (:528-547). NON-VACUOUS: until 2026-09-09
    /// request attrs were `{operation, result}` only and `request_duration`
    /// carried `{operation}` alone.
    #[test]
    fn api_request_attrs_carry_attempt_count_status_class_and_error_kind() {
        let err = ApiErrorAttrs::from_body(r#"{"kind":"auth","reason":"expired"}"#).unwrap();
        let attrs = api_request_attrs("http_error", 3, Some(401), Some(&err));
        assert_eq!(
            keys(&attrs),
            [
                "operation",
                "result",
                "attempt_count",
                "http_status_code",
                "http_status_class",
                "error_kind",
                "error_reason"
            ]
        );
        assert_eq!(attrs[4].value.to_string(), "4xx");
        assert_eq!(attrs[5].value.to_string(), "auth");
        // No response, no error body: base + result + attempt_count only.
        assert_eq!(
            keys(&api_request_attrs("config_error", 0, None, None)),
            ["operation", "result", "attempt_count"]
        );
    }

    /// TS `recordApiAttempt` attrs (custom/fetch.ts:549-568).
    #[test]
    fn api_attempt_attrs_carry_will_retry_status_and_error_kind() {
        let err = ApiErrorAttrs::from_body(r#"{"kind":"internal"}"#).unwrap();
        let attrs = api_attempt_attrs("http_error", true, 2, Some(503), Some(&err));
        assert_eq!(
            keys(&attrs),
            [
                "operation",
                "attempt",
                "result",
                "will_retry",
                "http_status_code",
                "http_status_class",
                "error_kind"
            ]
        );
        assert!(ApiErrorAttrs::from_body("not json").is_none());
        assert!(ApiErrorAttrs::from_body(r#"{"message":"x"}"#).is_none());
    }
}
