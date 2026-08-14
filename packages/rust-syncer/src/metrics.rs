//! Process-wide metrics — a Rust analog of the OTel instruments the TS
//! view-syncer maintains (`#hydrations`, `#pipelineResets`, hydration/advance
//! timings, pokes, …). The CG threads (which own the `!Send` `SyncEngine`)
//! record into these atomically; the HTTP handlers read snapshots on the tokio
//! thread — `/statz` returns JSON, `/metrics` returns Prometheus text.
//!
//! TS pushes OTLP to a collector; here we EXPOSE a Prometheus `/metrics` scrape
//! endpoint instead (pull vs push) — the ART telemetry gate scrapes
//! `zero_sync_*_seconds_bucket`/`_count` histograms, which this emits with the
//! same `zero_sync_*` names. Rendering is hand-rolled (no OTel SDK dependency)
//! and fully unit-testable.

use std::fmt::Write as _;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram as OtelHistogram, UpDownCounter};

/// Cumulative histogram upper bounds in SECONDS (ascending), a standard latency
/// ladder covering sub-ms hydrations up to multi-second stalls.
const HIST_BOUNDS_SECS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Latency-histogram bucket boundaries in SECONDS — byte-identical to TS
/// `LATENCY_HISTOGRAM_BOUNDARIES_S` (observability/metrics.ts) so the OTLP
/// histograms bucket the same as the TS syncer's.
const OTEL_LATENCY_BOUNDARIES_S: &[f64] = &[
    0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0,
];

/// OTel instruments, created from the global `zero` meter and exported over OTLP
/// (see [`crate::otel`]). Names/types/units mirror the TS syncer's `zero.sync.*`
/// instruments exactly. When no meter provider is installed (tests, OTLP
/// disabled) these are no-ops. TS pushes these over OTLP; so do we.
pub struct Otel {
    hydration: Counter<u64>,
    hydration_time: OtelHistogram<f64>,
    advance_time: OtelHistogram<f64>,
    pipeline_resets: Counter<u64>,
}

impl std::fmt::Debug for Otel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Otel { zero.sync.* instruments }")
    }
}

impl Default for Otel {
    fn default() -> Self {
        let m = global::meter("zero");
        let latency = |name: &'static str, desc: &'static str| {
            m.f64_histogram(name)
                .with_unit("s")
                .with_description(desc)
                .with_boundaries(OTEL_LATENCY_BOUNDARIES_S.to_vec())
                .build()
        };
        Self {
            hydration: m
                .u64_counter("zero.sync.hydration")
                .with_description("Number of query hydrations")
                .build(),
            hydration_time: latency("zero.sync.hydration-time", "Time to hydrate a query."),
            advance_time: latency(
                "zero.sync.advance-time",
                "Time to advance all queries for a given client group after applying a new transaction to the replica.",
            ),
            pipeline_resets: m
                .u64_counter("zero.sync.pipeline-resets")
                .with_description("Number of pipeline resets")
                .build(),
        }
    }
}

/// Custom-query transformation instruments — TS view-syncer's `#queryTransformations`,
/// `#queryTransformationTime`, `#queryTransformationHashChanges`, and
/// `#queryTransformationNoOps` (all `zero.sync.query.*`). These fire deep inside
/// `SyncEngine::config_and_hydrate_with_profile`, which holds no `Metrics`, so —
/// like the rust-cvr/rust-ivm instruments — they're recorded through free
/// functions off the *global* `zero` meter (created once via `OnceLock`). No-op
/// when OTLP is disabled.
struct QueryTransformOtel {
    transformations: Counter<u64>,
    transformation_time: OtelHistogram<f64>,
    hash_changes: Counter<u64>,
    no_ops: Counter<u64>,
}

fn query_transform_otel() -> &'static QueryTransformOtel {
    static INSTRUMENTS: OnceLock<QueryTransformOtel> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let m = global::meter("zero");
        QueryTransformOtel {
            transformations: m
                .u64_counter("zero.sync.query.transformations")
                .with_description("Number of query transformations performed")
                .build(),
            transformation_time: m
                .f64_histogram("zero.sync.query.transformation-time")
                .with_unit("s")
                .with_description("Time to transform custom queries via API server.")
                .with_boundaries(OTEL_LATENCY_BOUNDARIES_S.to_vec())
                .build(),
            hash_changes: m
                .u64_counter("zero.sync.query.transformation-hash-changes")
                .with_description("Number of times query transformation hash changed")
                .build(),
            no_ops: m
                .u64_counter("zero.sync.query.transformation-no-ops")
                .with_description(
                    "Number of times query transformation resulted in no-op (hash unchanged)",
                )
                .build(),
        }
    })
}

/// Record one custom-query transform invocation — TS
/// `#queryTransformations.add(1, {result})`. `success` maps to `result=success`,
/// else `result=error`.
pub fn record_query_transformation(success: bool) {
    let result = if success { "success" } else { "error" };
    query_transform_otel()
        .transformations
        .add(1, &[KeyValue::new("result", result)]);
}

/// Record the wall-clock (ms) of a custom-query transform invocation — TS
/// `#queryTransformationTime.recordMs` (recorded in the `finally`, so both the
/// success and error paths observe).
pub fn record_query_transformation_time(elapsed_ms: f64) {
    query_transform_otel()
        .transformation_time
        .record(elapsed_ms / 1000.0, &[]);
}

/// Record a custom query whose transformation hash changed vs the CVR — TS
/// `#queryTransformationHashChanges.add(1)` (drift → re-hydrate).
pub fn record_query_transformation_hash_change() {
    query_transform_otel().hash_changes.add(1, &[]);
}

/// Record a custom query whose transformation hash was unchanged — TS
/// `#queryTransformationNoOps.add(1)` (no re-hydration needed).
pub fn record_query_transformation_no_op() {
    query_transform_otel().no_ops.add(1, &[]);
}

/// Active sync clients — TS view-syncer's `#activeClients` UpDownCounter
/// (`zero.sync.active-clients`, dimensioned by protocol version). Recorded from
/// the router (the rust view-syncer) on client register (+1) / disconnect (-1)
/// through the *global* `zero` meter. No-op when OTLP is disabled.
fn active_clients() -> &'static UpDownCounter<i64> {
    static ACTIVE_CLIENTS: OnceLock<UpDownCounter<i64>> = OnceLock::new();
    ACTIVE_CLIENTS.get_or_init(|| {
        global::meter("zero")
            .i64_up_down_counter("zero.sync.active-clients")
            .with_description("Number of active sync clients")
            .build()
    })
}

/// Adjust the active-clients gauge by `delta` (+1 on connect, -1 on disconnect),
/// tagged by the client's sync protocol version — TS `#activeClients.add(delta,
/// {[PROTOCOL_VERSION_ATTR]: protocolVersion})`.
pub fn record_active_client_delta(delta: i64, protocol_version: u32) {
    active_clients().add(
        delta,
        &[KeyValue::new("protocol.version", protocol_version as i64)],
    );
}

/// A minimal, thread-safe, exporter-free histogram rendered as Prometheus
/// `_bucket{le=...}` / `_sum` / `_count` series. `sum` is accumulated in
/// microseconds (integer atomic) and divided to seconds at render time.
#[derive(Debug)]
pub struct Histogram {
    /// One slot per bound + a trailing `+Inf` overflow slot.
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: (0..=HIST_BOUNDS_SECS.len())
                .map(|_| AtomicU64::new(0))
                .collect(),
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
        }
    }
}

impl Histogram {
    /// Record an observation given in seconds.
    pub fn observe_secs(&self, v: f64) {
        let v = v.max(0.0);
        let idx = HIST_BOUNDS_SECS
            .iter()
            .position(|&b| v <= b)
            .unwrap_or(HIST_BOUNDS_SECS.len());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros
            .fetch_add((v * 1_000_000.0) as u64, Ordering::Relaxed);
    }

    /// Convenience: record an observation given in milliseconds.
    pub fn observe_millis(&self, ms: f64) {
        self.observe_secs(ms / 1000.0);
    }

    fn render(&self, name: &str, help: &str, out: &mut String) {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        let mut cumulative = 0u64;
        for (i, &bound) in HIST_BOUNDS_SECS.iter().enumerate() {
            cumulative += self.buckets[i].load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
        }
        cumulative += self.buckets[HIST_BOUNDS_SECS.len()].load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {cumulative}");
        let sum_secs = self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let _ = writeln!(out, "{name}_sum {sum_secs}");
        let _ = writeln!(out, "{name}_count {}", self.count.load(Ordering::Relaxed));
    }
}

/// Shared counters + latency histograms. Cheap to clone the `Arc`.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Query hydrations (a `config_and_hydrate` that added ≥1 query).
    pub hydrations: AtomicU64,
    /// Advances applied from a change-streamer notification.
    pub advances: AtomicU64,
    /// Pipeline resets (advance reported a reset → re-init + rehydrate).
    pub resets: AtomicU64,
    /// Queries evicted by the TTL scheduler.
    pub expired_queries: AtomicU64,
    /// `updateAuth` messages that changed the resolved auth (re-transform).
    pub auth_changes: AtomicU64,
    /// deleteClients operations processed.
    pub client_deletions: AtomicU64,
    /// Read-permission hot-reloads (deployed doc changed → re-transform +
    /// rehydrate).
    pub permission_reloads: AtomicU64,
    /// Periodic auth-maintenance ticks that ran (JWT re-validation + retransform).
    pub auth_revalidations: AtomicU64,
    /// Connections closed by periodic revalidation because their token was no
    /// longer valid (expired / revoked).
    pub auth_revalidation_failures: AtomicU64,

    /// Wall-clock of `config_and_hydrate` (query materialization) — TS
    /// `zero.sync.hydration-time`.
    pub hydration_time: Histogram,
    /// Wall-clock of `advance_and_sync` — TS `zero.sync.advance-time`.
    pub advance_time: Histogram,

    /// OTLP instruments (TS parity — pushed to the collector). Recorded
    /// alongside the atomics/Prometheus histograms via the `record_*` methods.
    otel: Otel,
}

impl Metrics {
    pub fn inc(field: &AtomicU64) {
        field.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(field: &AtomicU64, n: u64) {
        field.fetch_add(n, Ordering::Relaxed);
    }

    /// Record a query hydration and its wall-clock (ms). Updates the `/statz`
    /// counter, the Prometheus `/metrics` histogram, AND the OTLP
    /// `zero.sync.hydration` / `zero.sync.hydration-time` instruments.
    pub fn record_hydration(&self, elapsed_ms: f64) {
        self.hydrations.fetch_add(1, Ordering::Relaxed);
        self.hydration_time.observe_millis(elapsed_ms);
        self.otel.hydration.add(1, &[]);
        self.otel.hydration_time.record(elapsed_ms / 1000.0, &[]);
    }

    /// Record an advance and its wall-clock (ms). Mirrors TS
    /// `zero.sync.advance-time` (seconds).
    pub fn record_advance(&self, elapsed_ms: f64) {
        self.advances.fetch_add(1, Ordering::Relaxed);
        self.advance_time.observe_millis(elapsed_ms);
        self.otel.advance_time.record(elapsed_ms / 1000.0, &[]);
    }

    /// Record a pipeline reset — TS `zero.sync.pipeline-resets`.
    pub fn record_reset(&self) {
        self.resets.fetch_add(1, Ordering::Relaxed);
        self.otel.pipeline_resets.add(1, &[]);
    }

    /// A JSON snapshot for the `/statz` endpoint.
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "hydrations": self.hydrations.load(Ordering::Relaxed),
            "advances": self.advances.load(Ordering::Relaxed),
            "resets": self.resets.load(Ordering::Relaxed),
            "expiredQueries": self.expired_queries.load(Ordering::Relaxed),
            "authChanges": self.auth_changes.load(Ordering::Relaxed),
            "clientDeletions": self.client_deletions.load(Ordering::Relaxed),
            "permissionReloads": self.permission_reloads.load(Ordering::Relaxed),
            "authRevalidations": self.auth_revalidations.load(Ordering::Relaxed),
            "authRevalidationFailures": self.auth_revalidation_failures.load(Ordering::Relaxed),
        })
    }

    /// Prometheus text-format snapshot for the `/metrics` endpoint. `active_*`
    /// gauges are process-scoped and passed in by the handler (it holds the
    /// router). Metric names mirror TS's `zero.sync.*` (dots → underscores).
    pub fn render_prometheus(&self, active_client_groups: u64) -> String {
        let mut out = String::new();
        let counter = |out: &mut String, name: &str, help: &str, v: u64| {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} counter");
            let _ = writeln!(out, "{name} {v}");
        };
        let gauge = |out: &mut String, name: &str, help: &str, v: u64| {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} gauge");
            let _ = writeln!(out, "{name} {v}");
        };

        gauge(
            &mut out,
            "zero_sync_active_client_groups",
            "Client groups currently hosted",
            active_client_groups,
        );
        let l = Ordering::Relaxed;
        counter(
            &mut out,
            "zero_sync_hydrations_total",
            "Query hydrations",
            self.hydrations.load(l),
        );
        counter(
            &mut out,
            "zero_sync_advances_total",
            "Change-stream advances",
            self.advances.load(l),
        );
        counter(
            &mut out,
            "zero_sync_pipeline_resets_total",
            "Pipeline resets",
            self.resets.load(l),
        );
        counter(
            &mut out,
            "zero_sync_expired_queries_total",
            "TTL-evicted queries",
            self.expired_queries.load(l),
        );
        counter(
            &mut out,
            "zero_sync_auth_changes_total",
            "updateAuth re-transforms",
            self.auth_changes.load(l),
        );
        counter(
            &mut out,
            "zero_sync_client_deletions_total",
            "deleteClients processed",
            self.client_deletions.load(l),
        );
        counter(
            &mut out,
            "zero_sync_permission_reloads_total",
            "Permission hot-reloads",
            self.permission_reloads.load(l),
        );
        counter(
            &mut out,
            "zero_sync_auth_revalidations_total",
            "Periodic auth-maintenance ticks",
            self.auth_revalidations.load(l),
        );
        counter(
            &mut out,
            "zero_sync_auth_revalidation_failures_total",
            "Connections closed by revalidation",
            self.auth_revalidation_failures.load(l),
        );

        self.hydration_time.render(
            "zero_sync_hydration_time_seconds",
            "config_and_hydrate wall-clock",
            &mut out,
        );
        self.advance_time.render(
            "zero_sync_advance_time_seconds",
            "advance_and_sync wall-clock",
            &mut out,
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_increment_and_snapshot() {
        let m = Metrics::default();
        Metrics::inc(&m.hydrations);
        Metrics::inc(&m.hydrations);
        Metrics::add(&m.expired_queries, 3);
        let s = m.snapshot();
        assert_eq!(s["hydrations"], 2);
        assert_eq!(s["expiredQueries"], 3);
        assert_eq!(s["advances"], 0);
    }

    #[test]
    fn histogram_observe_and_render() {
        let h = Histogram::default();
        h.observe_millis(2.0); // 0.002s -> le=0.005 bucket
        h.observe_millis(2.0);
        h.observe_secs(3.0); // -> le=5.0 bucket (overflows le<=2.5)
        assert_eq!(h.count.load(Ordering::Relaxed), 3);
        let mut out = String::new();
        h.render("zero_sync_hydration_time_seconds", "help", &mut out);
        // Cumulative: le=0.005 sees the two 2ms samples; le=+Inf sees all 3.
        assert!(out.contains("zero_sync_hydration_time_seconds_bucket{le=\"0.005\"} 2"));
        assert!(out.contains("zero_sync_hydration_time_seconds_bucket{le=\"+Inf\"} 3"));
        assert!(out.contains("zero_sync_hydration_time_seconds_count 3"));
    }

    #[test]
    fn render_prometheus_emits_gate_series() {
        let m = Metrics::default();
        Metrics::inc(&m.hydrations);
        m.hydration_time.observe_millis(12.0);
        m.advance_time.observe_millis(3.0);
        let text = m.render_prometheus(7);
        // The ART G17 gate scrapes zero_sync_*_seconds_bucket and _count.
        assert!(text.contains("zero_sync_hydration_time_seconds_bucket{le="));
        assert!(text.contains("zero_sync_hydration_time_seconds_count 1"));
        assert!(text.contains("zero_sync_advance_time_seconds_count 1"));
        assert!(text.contains("zero_sync_active_client_groups 7"));
        assert!(text.contains("zero_sync_hydrations_total 1"));
    }

    #[test]
    fn record_methods_update_statz_and_histograms() {
        // The OTel instruments are no-op here (no meter provider installed), but
        // the record_* methods must still update the /statz counters and the
        // Prometheus histograms without panicking.
        let m = Metrics::default();
        m.record_hydration(12.0);
        m.record_hydration(8.0);
        m.record_advance(3.0);
        m.record_reset();

        let s = m.snapshot();
        assert_eq!(s["hydrations"], 2);
        assert_eq!(s["advances"], 1);
        assert_eq!(s["resets"], 1);

        let text = m.render_prometheus(0);
        assert!(text.contains("zero_sync_hydration_time_seconds_count 2"));
        assert!(text.contains("zero_sync_advance_time_seconds_count 1"));
        assert!(text.contains("zero_sync_pipeline_resets_total 1"));
    }

    #[test]
    fn query_transformation_records_are_noops_without_provider() {
        // No meter provider installed → global meter is a no-op. These free
        // functions must not panic (they fire from SyncEngine, which holds no
        // Metrics).
        record_query_transformation(true);
        record_query_transformation(false);
        record_query_transformation_time(4.0);
        record_query_transformation_hash_change();
        record_query_transformation_no_op();
        record_active_client_delta(1, 51);
        record_active_client_delta(-1, 51);
    }
}
