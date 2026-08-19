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

/// End-to-end serving-lag instruments — TS view-syncer's `#e2eServingLag`
/// (`zero.sync.e2e_serving_lag`, seconds) + `#e2eServingLagClamps`
/// (`zero.sync.e2e_serving_lag_clamps`). Recorded once per served version from
/// the CG thread through the *global* `zero` meter. No-op when OTLP is disabled.
struct ServingLagOtel {
    e2e_serving_lag: OtelHistogram<f64>,
    e2e_serving_lag_clamps: Counter<u64>,
}

fn serving_lag_otel() -> &'static ServingLagOtel {
    static INSTRUMENTS: OnceLock<ServingLagOtel> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let m = global::meter("zero");
        ServingLagOtel {
            e2e_serving_lag: m
                .f64_histogram("zero.sync.e2e_serving_lag")
                .with_unit("s")
                .with_description(
                    "End-to-end lag from upstream commit to ViewSyncer output. Spans the whole \
                     pipeline: the upstream transaction commit, replication to the replica, IVM \
                     advancement, CVR flush, and pokeEnd. Recorded once per served version.",
                )
                // No explicit boundaries: the SDK view in otel.rs exports this
                // instrument as a base2 exponential histogram (TS native-
                // histogram parity; fixed 30s-capped buckets truncated the tail).
                .build(),
            e2e_serving_lag_clamps: m
                .u64_counter("zero.sync.e2e_serving_lag_clamps")
                .with_description(
                    "Observations of sync.e2e_serving_lag that came out negative and were clamped \
                     to zero (upstream DB clock running ahead of this pod by more than the entire \
                     pipeline latency).",
                )
                .build(),
        }
    })
}

/// Record one end-to-end serving-lag observation (ms) — TS
/// `#e2eServingLag.recordMs(observation.lagMs)`.
pub fn record_e2e_serving_lag(lag_ms: f64) {
    serving_lag_otel()
        .e2e_serving_lag
        .record(lag_ms / 1000.0, &[]);
}

/// Record a clamped (negative) serving-lag observation — TS
/// `#e2eServingLagClamps.add(1)`.
pub fn record_e2e_serving_lag_clamp() {
    serving_lag_otel().e2e_serving_lag_clamps.add(1, &[]);
}

// NOTE ON `zero.sync.view_syncer_lag` (TS `Syncer.#viewSyncerLag`, zero/v1.9.0):
// this is the periodic *backlog* companion to `e2e_serving_lag` — a setInterval
// in the TS Syncer worker that, every tick, samples `now - replicaReadyTime` for
// EVERY active client group and records one observation each, so a stuck CG
// re-reports its growing age on every tick. It is intentionally NOT ported: the
// rust syncer runs each CG on its own single-threaded executor + LocalSet with
// no central, timer-driven CG registry to enumerate, and the completion-based
// `e2e_serving_lag` (ported) already captures served-version lag. Adding a
// cross-executor lag registry sampled on a process timer would introduce shared
// mutable state on the advance hot path for a purely-observational metric.

/// View-syncer hydration native histogram — TS view-syncer's
/// `#viewSyncerHydration` (`zero.sync.view_syncer_hydration`, seconds, zero/v1.9.0
/// #6207/#6209). Recorded once per query-sync that actually hydrated ≥1 query,
/// spanning transformation → materialization → CVR flush → catchup → pokeEnd.
/// This is the aggregable native-histogram companion to the legacy
/// `zero.sync.hydration-time` latency histogram. No-op when OTLP is disabled.
fn view_syncer_hydration_otel() -> &'static OtelHistogram<f64> {
    static INSTRUMENT: OnceLock<OtelHistogram<f64>> = OnceLock::new();
    INSTRUMENT.get_or_init(|| {
        global::meter("zero")
            .f64_histogram("zero.sync.view_syncer_hydration")
            .with_unit("s")
            .with_description(
                "Time from ViewSyncer query sync requiring hydration to output for a client \
                 group. Includes query transformation, query materialization, CVR flush, \
                 catchup, and pokeEnd.",
            )
            // Exponential-histogram view in otel.rs (TS native-histogram parity).
            .build()
    })
}

/// Record one view-syncer hydration observation (ms) — TS
/// `#viewSyncerHydration.recordMs(performance.now() - start)`.
pub fn record_view_syncer_hydration(elapsed_ms: f64) {
    view_syncer_hydration_otel().record(elapsed_ms / 1000.0, &[]);
}

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

fn api_attrs(result: &'static str) -> [opentelemetry::KeyValue; 2] {
    [
        opentelemetry::KeyValue::new("operation", "query"),
        opentelemetry::KeyValue::new("result", result),
    ]
}

/// One completed API request (all attempts) — TS `apiRequests().add(1, attrs)`.
pub fn record_api_request(result: &'static str) {
    api_otel().requests.add(1, &api_attrs(result));
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

// ─── WebSocket front-door instruments (TS workers/syncer.ts:303-322 +
// connection.ts:87). These are the connect-SLO metrics: a connect storm or an
// auth-failure spike must be visible on OTLP dashboards, not just in logs. ───

fn ws_open_connections() -> &'static UpDownCounter<i64> {
    static C: OnceLock<UpDownCounter<i64>> = OnceLock::new();
    C.get_or_init(|| {
        global::meter("zero")
            .i64_up_down_counter("zero.sync.websocket.open_connections")
            .with_description("Open client WebSocket connections.")
            .build()
    })
}

fn ws_connection_attempts() -> &'static Counter<u64> {
    static C: OnceLock<Counter<u64>> = OnceLock::new();
    C.get_or_init(|| {
        global::meter("zero")
            .u64_counter("zero.sync.websocket.connection_attempts")
            .with_description("Client WebSocket connection attempts.")
            .build()
    })
}

fn ws_connection_successes() -> &'static Counter<u64> {
    static C: OnceLock<Counter<u64>> = OnceLock::new();
    C.get_or_init(|| {
        global::meter("zero")
            .u64_counter("zero.sync.websocket.connection_successes")
            .with_description("Client WebSocket connections successfully initialized.")
            .build()
    })
}

fn ws_connection_failures() -> &'static Counter<u64> {
    static C: OnceLock<Counter<u64>> = OnceLock::new();
    C.get_or_init(|| {
        global::meter("zero")
            .u64_counter("zero.sync.websocket.connection_failures")
            .with_description(
                "Client WebSocket connection attempts that failed before initialization.",
            )
            .build()
    })
}

fn proto_attr(protocol_version: u32) -> KeyValue {
    KeyValue::new("protocol.version", protocol_version as i64)
}

pub fn record_ws_connection_attempt(protocol_version: u32) {
    ws_connection_attempts().add(1, &[proto_attr(protocol_version)]);
}

pub fn record_ws_connection_success(protocol_version: u32) {
    ws_connection_successes().add(1, &[proto_attr(protocol_version)]);
}

/// `reason` follows the TS reason vocabulary (`auth`, `protocol_version`,
/// `configuration`, `internal`, ...) plus rust-specific handshake stages.
pub fn record_ws_connection_failure(protocol_version: u32, reason: &str) {
    ws_connection_failures().add(
        1,
        &[
            proto_attr(protocol_version),
            KeyValue::new("reason", reason.to_string()),
        ],
    );
}

/// Slow-client sheds — a client DISCONNECTED because it couldn't keep up
/// (downstream queue crossed a HWM) or went unresponsive (liveness). This is the
/// terminal event of the slow-client incident; without a counter it was
/// `warn!`-log-only and un-alertable. `reason` is a CLOSED vocabulary
/// (`frame_hwm` / `byte_hwm` / `liveness`) — never pass a dynamic string.
fn ws_sheds() -> &'static Counter<u64> {
    static C: OnceLock<Counter<u64>> = OnceLock::new();
    C.get_or_init(|| {
        global::meter("zero")
            .u64_counter("zero.sync.websocket.sheds")
            .with_description("Clients disconnected by the slow-client shed (by reason).")
            .build()
    })
}

pub fn record_ws_shed(reason: &'static str) {
    ws_sheds().add(1, &[KeyValue::new("reason", reason)]);
}

pub fn record_ws_open_delta(delta: i64, protocol_version: u32) {
    ws_open_connections().add(delta, &[proto_attr(protocol_version)]);
}

// ─── Failure/pressure telemetry (the signals that precede a capacity incident;
// previously error-log-only, i.e. invisible to dashboards/alerts) ────────────

/// CVR flush failures. A rising rate here (pool exhaustion, ownership churn,
/// PG trouble) is the leading indicator of the fail_group → reconnect storm.
fn cvr_flush_failures() -> &'static Counter<u64> {
    static C: OnceLock<Counter<u64>> = OnceLock::new();
    C.get_or_init(|| {
        global::meter("zero")
            .u64_counter("zero.sync.cvr.flush-failures")
            .with_description("Number of failed CVR store flushes")
            .build()
    })
}

pub fn record_cvr_flush_failure() {
    cvr_flush_failures().add(1, &[]);
}

/// CVR load/flush attempt instruments — TS `zero.sync.cvr.load_attempts` /
/// `load_duration` / `flush_attempts` (cvr-store.ts:207-217). Alert rules on
/// `flush_attempts{result="error"}` / load-latency dashboards written for the
/// TS syncer keep working under rust.
struct CvrAttemptOtel {
    load_attempts: Counter<u64>,
    load_duration: OtelHistogram<f64>,
    flush_attempts: Counter<u64>,
}

fn cvr_attempt_otel() -> &'static CvrAttemptOtel {
    static I: OnceLock<CvrAttemptOtel> = OnceLock::new();
    I.get_or_init(|| {
        let m = global::meter("zero");
        CvrAttemptOtel {
            load_attempts: m
                .u64_counter("zero.sync.cvr.load_attempts")
                .with_description("CVR load attempts, labeled by result.")
                .build(),
            load_duration: m
                .f64_histogram("zero.sync.cvr.load_duration")
                .with_unit("s")
                .with_description("CVR load duration.")
                .with_boundaries(OTEL_LATENCY_BOUNDARIES_S.to_vec())
                .build(),
            flush_attempts: m
                .u64_counter("zero.sync.cvr.flush_attempts")
                .with_description("CVR flush attempts, labeled by result and flush.type.")
                .build(),
        }
    })
}

pub fn record_cvr_load_attempt(success: bool, elapsed_ms: f64) {
    let result = if success { "success" } else { "error" };
    let attrs = [KeyValue::new("result", result)];
    cvr_attempt_otel().load_attempts.add(1, &attrs);
    cvr_attempt_otel()
        .load_duration
        .record(elapsed_ms / 1000.0, &attrs);
}

/// `flush.type` is always `sync` in rust — there is no deferred-flush path.
pub fn record_cvr_flush_attempt(success: bool) {
    let result = if success { "success" } else { "error" };
    cvr_attempt_otel().flush_attempts.add(
        1,
        &[
            KeyValue::new("result", result),
            KeyValue::new("flush.type", "sync"),
        ],
    );
}

/// Client groups torn down via `fail_group` (all their clients rehomed).
fn failed_client_groups() -> &'static Counter<u64> {
    static C: OnceLock<Counter<u64>> = OnceLock::new();
    C.get_or_init(|| {
        global::meter("zero")
            .u64_counter("zero.sync.failed-client-groups")
            .with_description("Number of client groups torn down by a sync failure")
            .build()
    })
}

/// `reason` is a CLOSED vocabulary so a 2am responder can tell a panic
/// (`panic` — code bug) from a normal sync teardown (`sync` — usually CVR/PG
/// flap) from an executor thread dying (`executor_exit`). Never pass a dynamic
/// string.
pub fn record_fail_group(reason: &'static str) {
    failed_client_groups().add(1, &[KeyValue::new("reason", reason)]);
}

/// Total WS downstream frames queued (all connections) — the unbounded
/// channel's aggregate depth. Observable gauge backed by a process atomic; the
/// per-connection HWM shed policy bounds each connection, this makes the
/// aggregate visible.
static WS_QUEUED_FRAMES: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

fn ws_queued_frames_gauge() -> &'static opentelemetry::metrics::ObservableGauge<i64> {
    static G: OnceLock<opentelemetry::metrics::ObservableGauge<i64>> = OnceLock::new();
    G.get_or_init(|| {
        global::meter("zero")
            .i64_observable_gauge("zero.sync.websocket.queued-frames")
            .with_description("Downstream WS frames queued across all connections")
            .with_callback(|o| o.observe(WS_QUEUED_FRAMES.load(Ordering::Relaxed), &[]))
            .build()
    })
}

pub fn record_ws_queued_delta(delta: i64) {
    // Touch the gauge so its callback is registered on first use.
    let _ = ws_queued_frames_gauge();
    WS_QUEUED_FRAMES.fetch_add(delta, Ordering::Relaxed);
}

/// Estimated serialized bytes queued downstream across all connections. The
/// byte-aware slow-client shed bounds each connection; this makes the aggregate
/// pressure visible (and, paired with queued-frames, the mean frame size).
static WS_QUEUED_BYTES: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

fn ws_queued_bytes_gauge() -> &'static opentelemetry::metrics::ObservableGauge<i64> {
    static G: OnceLock<opentelemetry::metrics::ObservableGauge<i64>> = OnceLock::new();
    G.get_or_init(|| {
        global::meter("zero")
            .i64_observable_gauge("zero.sync.websocket.queued-bytes")
            .with_description("Estimated downstream WS bytes queued across all connections")
            .with_callback(|o| o.observe(WS_QUEUED_BYTES.load(Ordering::Relaxed), &[]))
            .build()
    })
}

pub fn record_ws_queued_bytes_delta(delta: i64) {
    let _ = ws_queued_bytes_gauge();
    WS_QUEUED_BYTES.fetch_add(delta, Ordering::Relaxed);
}

/// CVR PgPool gauges (size + idle). The pool is the prime capacity-cliff
/// suspect (per-flush contention against `CVR_MAX_CONNS`); without these an
/// acquire convoy is invisible until it becomes 10s-timeout fail_groups.
/// Called once from main after the pool is built; the instruments live in a
/// static so their observe callbacks stay registered.
pub fn register_cvr_pool_gauges(pool: sqlx::PgPool) {
    static G: OnceLock<(
        opentelemetry::metrics::ObservableGauge<u64>,
        opentelemetry::metrics::ObservableGauge<u64>,
    )> = OnceLock::new();
    G.get_or_init(|| {
        let m = global::meter("zero");
        let p1 = pool.clone();
        let size = m
            .u64_observable_gauge("zero.sync.cvr.pool-connections")
            .with_description("Open connections in the shared CVR PgPool")
            .with_callback(move |o| o.observe(p1.size() as u64, &[]))
            .build();
        let idle = m
            .u64_observable_gauge("zero.sync.cvr.pool-idle-connections")
            .with_description("Idle connections in the shared CVR PgPool")
            .with_callback(move |o| o.observe(pool.num_idle() as u64, &[]))
            .build();
        (size, idle)
    });
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
    /// `reason` labels the OTLP series like TS `#pipelineResets.add(1,
    /// {reason})` — an operator distinguishing schema-change resets from
    /// snapshot-drift resets needs the attribute, not just the total.
    pub fn record_reset(&self, reason: &str) {
        self.resets.fetch_add(1, Ordering::Relaxed);
        self.otel
            .pipeline_resets
            .add(1, &[KeyValue::new("reason", reason.to_string())]);
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
        m.record_reset("test");

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
