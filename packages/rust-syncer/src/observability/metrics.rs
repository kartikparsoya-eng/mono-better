//! Process-wide metrics — the OTel instruments the TS view-syncer maintains
//! (`#hydrations`, `#pipelineResets`, hydration/advance timings, pokes, …),
//! ported name-for-name on the `zero` meter. The CG threads (which own the
//! `!Send` `SyncEngine`) record into them; a few atomic counters back the
//! `/statz` JSON snapshot read on the tokio thread.
//!
//! Export is OTLP push only, exactly like TS (`server/otel_start.rs`, gated
//! on the same env as `packages/otel/src/enabled.ts`). The hand-rolled
//! Prometheus `/metrics` registry this file used to render was removed in
//! 204359376 (2026-09-07): TS has no pull endpoint, and the ART G17 gate
//! scrapes the collector. Instruments owned by another TS class live in that
//! class's port (CVRStore's in rust-cvr/src/otel_metrics.rs — see
//! tests/metric_ownership_test.rs).

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram as OtelHistogram, UpDownCounter};

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
    same_hash_rehydration_version_bumps: Counter<u64>,
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
            same_hash_rehydration_version_bumps: m
                .u64_counter("zero.sync.query.same-hash-rehydrations-forced-bump")
                .with_description(
                    "Number of times query-set reconciliation forced a configVersion bump \
                     for already-gotten same-transformation-hash query rehydration because \
                     trackQueries would not otherwise bump. Expected to be near-zero; \
                     non-zero values indicate pipeline/CVR row-set drift reached query-set \
                     reconciliation.",
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

/// Record a forced `configVersion` bump for an already-gotten,
/// same-transformation-hash query rehydration — TS
/// `#sameHashRehydrationVersionBumps.add(1, {reason})` (view-syncer.ts:2213).
/// `reason` is one of `missing-pipeline`, `row-set-signature-drift`, `mixed`.
pub fn record_same_hash_rehydration_version_bump(reason: &'static str) {
    query_transform_otel()
        .same_hash_rehydration_version_bumps
        .add(1, &[KeyValue::new("reason", reason)]);
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
                // No explicit boundaries: SDK default buckets. TS creates this
                // with `getOrCreateNativeHistogram` but does NOT list it in
                // `NATIVE_HISTOGRAM_INSTRUMENT_NAMES` (server/otel-start.ts) —
                // only view_syncer_hydration / view_syncer_lag get the
                // exponential view — so it is exported as an explicit-bucket
                // histogram there too (`server/otel_start.rs`
                // `NATIVE_HISTOGRAM_INSTRUMENTS`).
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

/// `zero.sync.view_syncer_lag` native histogram — TS `Syncer.#viewSyncerLag`
/// (zero/v1.9.0). The periodic *backlog* companion to `e2e_serving_lag`: the 60s
/// sampler (`main`) records `now - replicaReadyTime` for every serving-lag-eligible
/// CG's earliest-unserved change, so a stuck CG re-reports its growing age each
/// tick. Fed from the cross-CG `ServingLagRegistry`.
fn view_syncer_lag_otel() -> &'static OtelHistogram<f64> {
    static INSTRUMENT: OnceLock<OtelHistogram<f64>> = OnceLock::new();
    INSTRUMENT.get_or_init(|| {
        global::meter("zero")
            .f64_histogram("zero.sync.view_syncer_lag")
            .with_unit("s")
            .with_description(
                "Lag from replica-ready change to ViewSyncer output for active client groups. A \
                 change is output after IVM advancement, CVR flush, and pokeEnd.",
            )
            // Exported as a base2 exponential histogram via the
            // `server/otel_start.rs` view (TS getOrCreateNativeHistogram parity).
            .build()
    })
}

/// Record one `view_syncer_lag` sample (ms) — TS `#viewSyncerLag.recordMs(lagMs)`.
pub fn record_view_syncer_lag_ms(lag_ms: f64) {
    view_syncer_lag_otel().record(lag_ms / 1000.0, &[]);
}

/// Wall-clock epoch milliseconds (for the serving-lag gauge callbacks).
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Register the cross-CG serving-lag observable gauges — TS `Syncer`'s
/// `getOrCreateGauge(...).addCallback(...)` set: `serving_lag` (max),
/// `serving_lag_stats` (min/p50/p75/p99/max), `serving_lagging_client_groups`,
/// `queries`, and `rows`. Kept alive in a static so the callbacks keep firing.
pub fn register_serving_lag_gauges(
    registry: std::sync::Arc<crate::workers::syncer::ServingLagRegistry>,
) {
    use opentelemetry::metrics::ObservableGauge;
    type ServingLagGauges = (
        ObservableGauge<i64>,
        ObservableGauge<i64>,
        ObservableGauge<i64>,
        ObservableGauge<u64>,
        ObservableGauge<u64>,
        ObservableGauge<u64>,
    );
    static GAUGES: OnceLock<ServingLagGauges> = OnceLock::new();
    GAUGES.get_or_init(|| {
        let m = global::meter("zero");
        let r_max = registry.clone();
        let serving_lag = m
            .i64_observable_gauge("zero.sync.serving_lag")
            // TS declares `unit: 'millisecond'` (workers/syncer.ts:433). The
            // unit is part of the instrument, and the Prometheus exporter
            // appends it to the series name — `ms` normalises to
            // `..._milliseconds`, `millisecond` stays `..._millisecond`. With
            // `ms` here the two engines published DIFFERENT series names for
            // the same gauge, so a dashboard or alert built on the TS name went
            // blank against rust. Caught by diffing what the two arms actually
            // export, not by reading the sources: both declare
            // `zero.sync.serving_lag`, and only the rendered name differs.
            .with_unit("millisecond")
            .with_description(
                "Maximum time active ViewSyncer client groups have had unserved replica changes.",
            )
            .with_callback(move |o| {
                o.observe(r_max.compute_serving_lag_distribution(now_ms()).max_ms, &[]);
            })
            .build();
        let r_stats = registry.clone();
        let serving_lag_stats = m
            .i64_observable_gauge("zero.sync.serving_lag_stats")
            // TS `unit: 'millisecond'` (workers/syncer.ts) — see the note above.
            .with_unit("millisecond")
            .with_description(
                "Distribution of time active ViewSyncer client groups have had unserved replica \
                 changes.",
            )
            .with_callback(move |o| {
                let d = r_stats.compute_serving_lag_distribution(now_ms());
                o.observe(d.min_ms, &[KeyValue::new("stat", "min")]);
                o.observe(d.p50_ms, &[KeyValue::new("stat", "p50")]);
                o.observe(d.p75_ms, &[KeyValue::new("stat", "p75")]);
                o.observe(d.p99_ms, &[KeyValue::new("stat", "p99")]);
                o.observe(d.max_ms, &[KeyValue::new("stat", "max")]);
            })
            .build();
        let r_lag = registry.clone();
        let serving_lagging_client_groups = m
            .i64_observable_gauge("zero.sync.serving_lagging_client_groups")
            .with_description("Number of active client groups with unserved replica changes.")
            .with_callback(move |o| {
                o.observe(
                    r_lag
                        .compute_serving_lag_distribution(now_ms())
                        .lagging_client_groups as i64,
                    &[],
                );
            })
            .build();
        let r_q = registry.clone();
        let queries = m
            .u64_observable_gauge("zero.sync.queries")
            .with_description("Active queries (pipelines) across all client groups.")
            .with_callback(move |o| o.observe(r_q.total_queries(), &[]))
            .build();
        let r_rows = registry.clone();
        let rows = m
            .u64_observable_gauge("zero.sync.rows")
            .with_description("Tracked rows across all client groups.")
            .with_callback(move |o| o.observe(r_rows.total_rows(), &[]))
            .build();
        // TS declares this in the SAME block as `queries`/`rows`
        // (workers/syncer.ts:398-402) over the same `#viewSyncers` map. It was
        // the one gauge of that block rust never exposed on OTLP — it existed
        // only in the hand-rolled `/metrics` text registry, which has no TS
        // twin at all and is now gone.
        let r_cgs = registry.clone();
        let active_client_groups = m
            .u64_observable_gauge("zero.sync.active-client-groups")
            .with_description("Number of active client groups")
            .with_callback(move |o| o.observe(r_cgs.total_client_groups(), &[]))
            .build();
        (
            serving_lag,
            serving_lag_stats,
            serving_lagging_client_groups,
            queries,
            rows,
            active_client_groups,
        )
    });
}

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
            // Exponential-histogram view in `server/otel_start.rs`
            // (TS native-histogram parity).
            .build()
    })
}

/// `zero.sync.lock-wait-time` — TS view-syncer's `#lockWaitTime`
/// (`getOrCreateLatencyHistogram('sync', 'lock-wait-time', 'Time spent waiting
/// to acquire the ViewSyncer lock.')`, view-syncer.ts:366-370), recorded in
/// `#runInLockWithCVR` the moment `#lock.withLock` grants the lock (:459-461).
/// Rust's lock is the client group's serial message queue (INVENTIONS.md I-1):
/// the wait is the time from enqueue to the CG task dequeuing the message —
/// recorded in `dispatch_cg_message` for every message kind that carries an
/// `enqueued_at` (new connection, inbound frame, change-streamer notification).
fn lock_wait_time_otel() -> &'static OtelHistogram<f64> {
    static INSTRUMENT: OnceLock<OtelHistogram<f64>> = OnceLock::new();
    INSTRUMENT.get_or_init(|| {
        global::meter("zero")
            .f64_histogram("zero.sync.lock-wait-time")
            .with_unit("s")
            .with_description("Time spent waiting to acquire the ViewSyncer lock.")
            .with_boundaries(OTEL_LATENCY_BOUNDARIES_S.to_vec())
            .build()
    })
}

/// Record one lock wait (ms) — TS `#lockWaitTime.recordMs(performance.now() -
/// lockWaitStart)` (view-syncer.ts:461).
pub fn record_lock_wait_ms(elapsed_ms: f64) {
    #[cfg(test)]
    {
        *LAST_LOCK_WAIT_MS.lock().unwrap() = Some(elapsed_ms);
    }
    lock_wait_time_otel().record(elapsed_ms / 1000.0, &[]);
}

/// TEST SEAMS (rust-only): the last value handed to an instrument, so a unit
/// test can pin WHAT a site records without an OTLP reader. The global meter
/// is a no-op under `cargo test`, so this is the only observable.
#[cfg(test)]
pub(crate) static LAST_LOCK_WAIT_MS: std::sync::Mutex<Option<f64>> = std::sync::Mutex::new(None);
#[cfg(test)]
pub(crate) static LAST_ADVANCE_MS: std::sync::Mutex<Option<f64>> = std::sync::Mutex::new(None);

/// Record one view-syncer hydration observation (ms) — TS
/// `#viewSyncerHydration.recordMs(performance.now() - start)`.
pub fn record_view_syncer_hydration(elapsed_ms: f64) {
    view_syncer_hydration_otel().record(elapsed_ms / 1000.0, &[]);
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

/// `zero.mutation.custom` + `zero.mutation.pushes` — TS `Pusher`'s
/// `#customMutations` / `#pushes` (mutagen/pusher.ts:279-288), both tagged with
/// `clientGroupID` and added in `#processPush` (:490-495). Recorded from rust's
/// ported `services/mutagen/pusher.rs`.
///
/// TS's third mutation counter, `zero.mutation.crud` (mutagen.ts:71-73), has NO
/// rust twin: rust does not execute CRUD mutations at all — it relays pushes to
/// the TS API server (the Option-A relay, INVENTIONS.md), so there is no rust
/// site at which a CRUD mutation is processed.
struct MutationOtel {
    custom: Counter<u64>,
    pushes: Counter<u64>,
}

fn mutation_otel() -> &'static MutationOtel {
    static INSTRUMENTS: OnceLock<MutationOtel> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let m = global::meter("zero");
        MutationOtel {
            custom: m
                .u64_counter("zero.mutation.custom")
                .with_description("Number of custom mutations processed")
                .build(),
            pushes: m
                .u64_counter("zero.mutation.pushes")
                .with_description("Number of pushes processed by the pusher")
                .build(),
        }
    })
}

/// TS `#processPush`: `#customMutations.add(mutations.length, {clientGroupID})`
/// then `#pushes.add(1, {clientGroupID})` (pusher.ts:490-495).
pub fn record_push(client_group_id: &str, mutation_count: u64) {
    let attrs = [KeyValue::new("clientGroupID", client_group_id.to_string())];
    let i = mutation_otel();
    i.custom.add(mutation_count, &attrs);
    i.pushes.add(1, &attrs);
}

/// `zero.sync.max-protocol-version` + `zero.server.uptime` — TS declares BOTH in
/// `server/worker-dispatcher.ts` (:56-64 and :168-171). Rust is one process with
/// shards and has no `worker_dispatcher.rs` twin, so per the established
/// exception (no twin file → fold into the consumer, keep the TS metric name
/// 1:1) they are registered here and fed from the connect path.
///
/// TS observes `maxProtocolVersion` only once it is non-zero; the callback here
/// does the same, so an idle process reports nothing rather than 0.
static MAX_PROTOCOL_VERSION: AtomicU64 = AtomicU64::new(0);

/// Record a connecting client's sync protocol version — TS
/// `maxProtocolVersion = Math.max(maxProtocolVersion, params.protocolVersion)`
/// in the dispatcher's connect handler.
pub fn record_client_protocol_version(protocol_version: u32) {
    MAX_PROTOCOL_VERSION.fetch_max(protocol_version as u64, Ordering::Relaxed);
}

/// `zero.server.startup_duration` has NO rust twin: TS records it from the
/// MAIN process only (`recordStartupDurationMs`, main.ts:426 → life-cycle.ts:82,
/// `{component: 'dispatcher'}`), and in the rust image that main process is the
/// node zero-cache that spawns this binary. The rust process is a WORKER —
/// `processes.addWorker(emitter, 'user-facing', `rust-syncer (${id})`)`
/// (main.ts:268) — whose startup the parent times as
/// `zero.server.worker_startup_duration{worker: 'syncer', type: 'user-facing'}`
/// (life-cycle.ts:227), exactly as it does for a TS syncer worker. Until
/// 2026-09-09 rust also recorded `startup_duration{component: 'dispatcher'}`
/// from here, double-counting the dispatcher's series.
/// Register the two process-scoped dispatcher gauges. Called once from the
/// serving bootstrap, after which the callbacks keep firing.
pub fn register_process_gauges() {
    use opentelemetry::metrics::ObservableGauge;
    type ProcessGauges = (ObservableGauge<u64>, ObservableGauge<f64>);
    static GAUGES: OnceLock<ProcessGauges> = OnceLock::new();
    GAUGES.get_or_init(|| {
        let m = global::meter("zero");
        let max_protocol_version = m
            .u64_observable_gauge("zero.sync.max-protocol-version")
            .with_description("Latest sync protocol version from a connecting client")
            .with_callback(|o| {
                let v = MAX_PROTOCOL_VERSION.load(Ordering::Relaxed);
                if v != 0 {
                    o.observe(v, &[]);
                }
            })
            .build();
        // TS starts this clock when requests begin being served (`run()`), not
        // at process start.
        let ready_start = std::time::Instant::now();
        let uptime = m
            .f64_observable_gauge("zero.server.uptime")
            .with_unit("s")
            .with_description("Cumulative uptime, starting from when requests are served")
            .with_callback(move |o| o.observe(ready_start.elapsed().as_secs_f64(), &[]))
            .build();
        (max_protocol_version, uptime)
    });
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

/// The `reason` vocabulary of TS `recordConnectionFailure` (workers/syncer.ts:
/// 571 `configuration`, 608 `auth`, 613/694 `internal`, 637 `user_mismatch`,
/// 707 `protocol_version`) — every value TS can emit and nothing else, so the
/// two arms' `zero.sync.websocket.connection_failures{reason}` series line up.
/// A failed WebSocket upgrade never reaches TS's `#createConnection`, so it is
/// not a connection failure there and is not counted here either.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionFailureReason {
    Auth,
    Configuration,
    /// Any exception while establishing the connection (TS's catch-all,
    /// syncer.ts:613/694) — rust's capacity/executor-shutdown Rehome included.
    Internal,
    ProtocolVersion,
    UserMismatch,
}

impl ConnectionFailureReason {
    /// Exhaustiveness fixture for `connection_failure_reasons_are_the_ts_vocabulary`
    /// — a rust-only test helper, NOT a port (TS never enumerates the reasons;
    /// `recordConnectionFailure` takes one at each call site). `#[cfg(test)]`
    /// keeps it out of the M11 prod-reachability surface, where a `pub const
    /// ALL` otherwise binds by name to the unrelated `ALL` in `ivm/db.ts` and
    /// reads as a ported-but-unreachable item.
    #[cfg(test)]
    pub const ALL: [ConnectionFailureReason; 5] = [
        ConnectionFailureReason::Auth,
        ConnectionFailureReason::Configuration,
        ConnectionFailureReason::Internal,
        ConnectionFailureReason::ProtocolVersion,
        ConnectionFailureReason::UserMismatch,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ConnectionFailureReason::Auth => "auth",
            ConnectionFailureReason::Configuration => "configuration",
            ConnectionFailureReason::Internal => "internal",
            ConnectionFailureReason::ProtocolVersion => "protocol_version",
            ConnectionFailureReason::UserMismatch => "user_mismatch",
        }
    }
}

pub fn record_ws_connection_failure(protocol_version: u32, reason: ConnectionFailureReason) {
    ws_connection_failures().add(
        1,
        &[
            proto_attr(protocol_version),
            KeyValue::new("reason", reason.as_str()),
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

/// Client WebSocket error events. Port of TS `Connection.#webSocketErrors`
/// (`getOrCreateCounter('sync', 'websocket.errors', ...)`) — incremented on an
/// unclean close (`event_type=unclean_close`) or a transport/protocol error
/// (`event_type=error_event`), tagged by protocol version and event type.
fn ws_errors() -> &'static Counter<u64> {
    static C: OnceLock<Counter<u64>> = OnceLock::new();
    C.get_or_init(|| {
        global::meter("zero")
            .u64_counter("zero.sync.websocket.errors")
            .with_description("Client WebSocket error events.")
            .build()
    })
}

/// `event_type` is a CLOSED vocabulary matching TS: `unclean_close` (the socket
/// ended without an RFC 6455 close handshake) or `error_event` (a real
/// transport/protocol error). Never pass a dynamic string.
pub fn record_websocket_error(event_type: &'static str, protocol_version: u32) {
    ws_errors().add(
        1,
        &[
            proto_attr(protocol_version),
            KeyValue::new("event.type", event_type),
        ],
    );
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

// The CVRStore instruments (`zero.sync.cvr.load_attempts` / `load_duration` /
// `flush_attempts`, cvr-store.ts:207-217) are built and bumped ONLY by the
// cvr-store port, rust-cvr/src/otel_metrics.rs (record_load /
// record_flush_attempt), so alert rules and dashboards written for the TS
// syncer keep working under rust. A second copy used to live here and doubled
// every load/flush count (caught at the collector 2026-09-07: 12 attempts vs
// 6 durations); tests/metric_ownership_test.rs pins the single owner.

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

/// Shared counters + latency histograms. Cheap to clone the `Arc`.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Query hydrations — TS `#hydrations` (`zero.sync.hydration`): one per
    /// `#addAndRemoveQueries` batch (view-syncer.ts:2300) plus one per query
    /// rehydrated by `#hydrateUnchangedQueries` (:1638). NOT one per config
    /// pass: an `already caught up` pass adds nothing.
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

    /// Record a query hydration and its process time (ms — the TimeSliceTimer's
    /// yield-excluded `totalProcessTime` / `totalElapsed()`). Updates the `/statz`
    /// counter, the Prometheus `/metrics` histogram, AND the OTLP
    /// `zero.sync.hydration` / `zero.sync.hydration-time` instruments.
    pub fn record_hydration(&self, elapsed_ms: f64) {
        self.hydrations.fetch_add(1, Ordering::Relaxed);
        self.otel.hydration.add(1, &[]);
        self.otel.hydration_time.record(elapsed_ms / 1000.0, &[]);
    }

    /// Record a completed advance and its PROCESS time (ms) — TS
    /// `#transactionAdvanceTime.recordMs(totalProcessTime)` with
    /// `totalProcessTime = timer.totalElapsed()` (view-syncer.ts:2628-2632):
    /// the TimeSliceTimer's yield-excluded time, not wall-clock, recorded once
    /// per successful `#advancePipelines` after pokeEnd. A reset
    /// (`ResetPipelinesSignal`) throws past the record, so a reset advance is
    /// NOT counted.
    pub fn record_advance(&self, process_time_ms: f64) {
        #[cfg(test)]
        {
            *LAST_ADVANCE_MS.lock().unwrap() = Some(process_time_ms);
        }
        self.advances.fetch_add(1, Ordering::Relaxed);
        self.otel.advance_time.record(process_time_ms / 1000.0, &[]);
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
}

#[cfg(test)]
mod tests {
    /// TS `recordConnectionFailure` reasons, verbatim (workers/syncer.ts:571,
    /// 608, 613, 637, 694, 707). NON-VACUOUS: the pre-2026-09-09 sites emitted
    /// `handshake` and `rehome`, which TS never does.
    #[test]
    fn connection_failure_reasons_are_the_ts_vocabulary() {
        let got: Vec<&str> = super::ConnectionFailureReason::ALL
            .iter()
            .map(|r| r.as_str())
            .collect();
        assert_eq!(
            got,
            [
                "auth",
                "configuration",
                "internal",
                "protocol_version",
                "user_mismatch"
            ]
        );
    }

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
    fn record_methods_update_statz_and_histograms() {
        // The OTel instruments are no-op here (no meter provider installed), but
        // the record_* methods must still update the /statz counters without
        // panicking.
        let m = Metrics::default();
        m.record_hydration(12.0);
        m.record_hydration(8.0);
        m.record_advance(3.0);
        m.record_reset("test");

        let s = m.snapshot();
        assert_eq!(s["hydrations"], 2);
        assert_eq!(s["advances"], 1);
        assert_eq!(s["resets"], 1);
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
