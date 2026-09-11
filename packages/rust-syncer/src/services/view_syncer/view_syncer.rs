//! `ViewSyncerService` — port of
//! `zero-cache/src/services/view-syncer/view-syncer.ts`.
//!
//! The `!Send` serving core that owns ONE client group's world: the IVM
//! pipelines, the CVR store handle + row cache, the per-client poke sinks, and
//! the connection/auth state (`ConnectionContextManager`). Everything the client
//! observes — hydrate, advance, diff, poke, CVR flush, inspector metrics —
//! happens here, driven by `cg_event_loop` (this file), which runs as a
//! `spawn_local` task on one of the `K` sharded executor threads (`RUST-SYNCER-ARCHITECTURE.md` §4 — there
//! is no per-CG OS thread). CVR Postgres I/O is `offload`ed onto the main
//! multi-thread runtime so it never blocks this serial thread's CPU.
//!
//! What is NOT here (moved out in d632507bc): the connection
//! ROUTER — accepting a socket, JWT validation, `place_cg`, the
//! `DashMap<client_group_id, CGHandle>`, and emitting the `connected` ack — lives
//! in `workers/syncer.rs` (`create_connection`, port of TS
//! `Syncer.#createConnection`/`handleConnection`). Crucially, the `connected` ack
//! is sent THERE, on the per-connection accept task, BEFORE the connection is
//! handed to this serial CG thread — decoupling the connect-ack from
//! `config_and_hydrate` (TS parity; the connect-ack fix).

use super::pipeline_driver::Timer;
use crate::auth::read_authorizer::{hash_of_ast, transform_and_hash_query};
use crate::custom_queries::transform_query::{
    CustomQueryContext, CustomQuerySpec, CustomTransformed, transform,
};
use crate::db::specs::LiteTableSpec;
use crate::server::priority_op::{is_priority_op_running, run_priority_op};
use crate::services::view_syncer::connection_context_manager::{
    CCMError, ConnectParamsForRegistration, ConnectionContext as CcmConnectionContext,
    ConnectionContextManager, ConnectionSelector as CcmConnectionSelector, ConnectionValidation,
    FetchConfig, InitConnectionBody, MaintenanceKind, UpdateAuthBody, resolve_auth,
};
use crate::services::view_syncer::pipeline_driver::{
    AdvanceOutcome, HydrateQuery, IvmPipelines, IvmTableSpec, PipelineHydrationReason,
    json_to_value,
};
use crate::services::view_syncer::query_covering::{
    QueryCoverageShadowHit, QueryCoveringIndex, RunningQuery,
};
#[cfg(test)]
use crate::workers::cg_executor::CGHandle;
use crate::workers::cg_executor::{CGMessage, CgTaskContext};
use crate::workers::connect_params::ConnectParams;
use crate::workers::connection::{Connection, JsError, Thrown};
use crate::workers::syncer::ConnectionInfo;
#[cfg(test)]
use crate::workers::syncer::check_and_pin_user;
use crate::workers::syncer_ws_message_handler::{
    ConnContextInfo, ConnContextManagerDispatch, ConnectionSelector, MutagenDispatch,
    PusherDispatch, SyncerWsMessageHandler, ViewSyncerDispatch,
};
#[cfg(test)]
use crate::ws_server::ConnectionContext;
use crate::ws_sink::DirectWebSocketSink;
use rust_cvr::change_processor::{ChangeProcessor, RowChangeType};
use rust_cvr::client_handler::{ClientHandler, MultiPoker, WebSocketSink};
use rust_cvr::client_handler::{Patch, PatchToVersion, RowPatch};
use rust_cvr::cvr::{CVR, DesiredQuerySpec, StoreOp};
use rust_cvr::cvr::{CVRConfigDrivenUpdater, CVRQueryDrivenUpdater, RowRecordMap};
use rust_cvr::cvr_store::{CVRStoreError, CVRStoreHandle, InspectQueryRow};
use rust_cvr::schema::types::{
    CVRVersion, EMPTY_CVR_VERSION, NullableCVRVersion, cmp_versions, maybe_version_string,
    version_string, version_to_cookie,
};
use rust_cvr::schema::types::{ClientSchema, QueryRecord, RowID};
use rust_cvr::shards::ShardID;
use rust_cvr::ttl_clock::TTLClock;
use rust_ivm::ivm::stream::StreamItem;
use std::cell::RefCell;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::mpsc;

/// Small delay added when scheduling TTL eviction so many near-simultaneous
/// expirations collapse into one timer wakeup. Port of TS `TTL_TIMER_HYSTERESIS`.
const TTL_TIMER_HYSTERESIS_MS: i64 = 50;
/// Interval between periodic ttlClock persistence ticks. Port of TS
/// `TTL_CLOCK_INTERVAL` (view-syncer.ts:202).
const TTL_CLOCK_INTERVAL: i64 = 60_000;

/// Whether a `sync_query_pipeline_set` pass re-transforms EVERY custom (named)
/// query, or only those missing from the pipeline. Port of TS
/// `type CustomQueryTransformMode = 'all' | 'missing'` (view-syncer.ts:212).
///
/// `All` is used where TS re-validates authorization with the user's API server
/// on every pass — new connections (view-syncer.ts:945), `updateAuth`
/// (view-syncer.ts:1019), and the background retransform (view-syncer.ts:2670).
/// `Missing` is the steady-state mode — `changeDesiredQueries`
/// (view-syncer.ts:978), `deleteClients` (view-syncer.ts:1040), the run-loop
/// init sync (view-syncer.ts:599), and `#removeExpiredQueries`
/// (view-syncer.ts:644) — where an already-hydrated custom query keeps its
/// existing transform instead of paying another API round-trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CustomQueryTransformMode {
    All,
    Missing,
}

/// Which TS `ViewSyncerService` method a config pass is standing in for.
///
/// Rust-only (AGENTS rule 5), and it exists to RECOVER a TS distinction rather
/// than invent one: TS has four separate entry points into `#handleConfigUpdate`
/// — `initConnection` (:945), `changeDesiredQueries` (:978), `updateAuth` (:1019)
/// and `#runBackgroundRetransform` (:2670) — and they differ in what they do
/// BEFORE the config update. Only `initConnection` runs the client-schema /
/// cookie checks and `#validateConnection` (:942). Rust folds all four into
/// `handle_desired_queries`, and the single `is_init: bool` that stood in for
/// them conflated "this is initConnection" with "run the config pass even
/// without a query change", so `updateAuth` and the background retransform were
/// also running initConnection's cookie + client-schema checks, which their TS
/// twins do not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigPassOrigin {
    /// TS `initConnection` (view-syncer.ts:945).
    InitConnection,
    /// TS `changeDesiredQueries` (view-syncer.ts:978).
    ChangeDesiredQueries,
    /// TS `updateAuth` (view-syncer.ts:1019).
    UpdateAuth,
    /// TS `#runBackgroundRetransform` (view-syncer.ts:2670).
    BackgroundRetransform,
}

impl ConfigPassOrigin {
    /// The initConnection-only preamble: cookie + client-schema validation and
    /// `#validateConnection`. TS runs these in `initConnection` and nowhere else.
    fn is_init_connection(self) -> bool {
        matches!(self, ConfigPassOrigin::InitConnection)
    }

    /// Whether the config/hydrate pass runs even with no desired-query change.
    /// TS's `changeDesiredQueries` is the only entry point that exists solely to
    /// carry a query change; the other three must re-transform regardless (a new
    /// connection, a refreshed credential, a background re-authorization).
    fn forces_config_pass(self) -> bool {
        !matches!(self, ConfigPassOrigin::ChangeDesiredQueries)
    }

    /// The TS message name — `cmd` in `#runInLockForClient` (view-syncer.ts
    /// :1192) — for the origins that carry a client message.
    fn cmd(self) -> &'static str {
        match self {
            ConfigPassOrigin::InitConnection => "initConnection",
            ConfigPassOrigin::ChangeDesiredQueries => "changeDesiredQueries",
            ConfigPassOrigin::UpdateAuth => "updateAuth",
            ConfigPassOrigin::BackgroundRetransform => "backgroundRetransform",
        }
    }
}
/// Upper bound on a single eviction-timer delay (matches `rust_cvr::ttl::MAX_TTL_MS`).
const MAX_TTL_MS: i64 = 600_000;
/// How long an empty client-group worker stays warm after its latest connection.
/// Matches TS `ViewSyncerService`'s `DEFAULT_KEEPALIVE_MS`.
const CG_KEEPALIVE_MS: i64 = 5_000;

/// Validate the cookie supplied by an `initConnection` against the loaded CVR.
/// This is the Rust equivalent of TS `checkClientAndCVRVersions` and deliberately
/// distinguishes a purged/missing CVR from a stale server CVR.
fn check_client_and_cvr_versions(
    client: &NullableCVRVersion,
    cvr: &CVRVersion,
) -> Result<(), Box<crate::protocol::ErrorBody>> {
    let empty = Some(EMPTY_CVR_VERSION.clone());
    if cmp_versions(&Some(cvr.clone()), &empty) == CmpOrdering::Equal
        && cmp_versions(client, &empty) == CmpOrdering::Greater
    {
        return Err(Box::new(crate::protocol::ErrorBody::client_not_found(
            "Client not found",
        )));
    }

    if cmp_versions(client, &Some(cvr.clone())) == CmpOrdering::Greater {
        return Err(Box::new(crate::protocol::ErrorBody::basic(
            crate::protocol::ErrorKind::InvalidConnectionRequestBaseCookie,
            format!("CVR is at version {}", version_string(cvr)),
        )));
    }

    Ok(())
}

/// Returns `Some(message)` when a loaded CVR was written by a NEWER replica than
/// the one this syncer is serving (an older-replica rollback) — TS's
/// "Cannot sync from older replica" case. A `state_version == "00"` CVR is brand
/// new (never synced) so it is exempt. The message string is byte-identical to
/// TS (`view-syncer.pg.test.ts`), and the caller fails the group with a
/// `ClientNotFound` carrying it. `None` means it is safe to sync.
fn older_replica_error(cvr: &CVR, replica_version: &str) -> Option<String> {
    if cvr.version.state_version != "00"
        && cvr
            .replica_version
            .as_deref()
            .is_some_and(|v| v > replica_version)
    {
        Some(format!(
            "Cannot sync from older replica: CVR={}, DB={}",
            cvr.replica_version.as_deref().unwrap_or_default(),
            replica_version
        ))
    } else {
        None
    }
}

/// Compute which clients to remove on a config/deleteClients pass — the
/// `activeClients` garbage collection plus explicit deletions. TS
/// (`ViewSyncer.#patchQueries`/`deleteClients`): any CVR client absent from the
/// connection's `activeClients` set is inactivated (its queries get a TTL and
/// are expired later), and explicit `deleted.clientIDs` are removed too (a
/// client may not delete itself — the caller filters that into `ack_ids`).
/// `active_clients == None` means no GC (only explicit deletions apply).
fn clients_to_delete(
    cvr_client_ids: &[String],
    active_clients: Option<&[String]>,
    ack_ids: &[String],
) -> Vec<String> {
    let mut delete_ids: Vec<String> = Vec::new();
    if let Some(active) = active_clients {
        let active_set: HashSet<&str> = active.iter().map(String::as_str).collect();
        for id in cvr_client_ids {
            if !active_set.contains(id.as_str()) {
                delete_ids.push(id.clone());
            }
        }
    }
    for id in ack_ids {
        if !delete_ids.contains(id) {
            delete_ids.push(id.clone());
        }
    }
    delete_ids
}

/// Stable hash of a client group into `[0, num_shards)`. Uses a fixed-seed
/// `DefaultHasher` (not `RandomState`), so the result is deterministic within a
/// process run. Used by [`Syncer::place_cg`] to break ties among
/// equally-loaded executors so a cold/uniform system still spreads groups.
pub(crate) fn shard_for(cg_id: &str, num_shards: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    cg_id.hash(&mut h);
    (h.finish() % num_shards as u64) as usize
}

/// CVR Postgres store identity for a client group.
///
/// This carries only the *identity* of the CVR (schema + ids); the `PgPool` it
/// binds to is the ONE process-wide shared pool, built on the main runtime and
/// handed to each executor (`RUST-SYNCER-ARCHITECTURE.md` §3). CVR I/O is offloaded onto that
/// pool's own runtime via `SyncEngine::offload`, so every connection is polled
/// by the reactor that created it (§5.1) while the whole `cvr_max_conns` budget
/// stays one shared pool — matching TS's one-`cvrDB`-pool-per-worker model.
#[derive(Clone)]
pub struct CvrPgConfig {
    pub schema: String,
    /// CVR id (== client group id).
    pub cvr_id: String,
    pub task_id: String,
}

/// Everything the CG thread needs to build its `SyncEngine` locally. `Send` so
/// it can cross into the CG thread; the (`!Send`) `SyncEngine` is then
/// constructed on that thread.
pub struct SyncEngineConfig {
    /// Fatal replica/configuration load error discovered by the factory. The
    /// CG is created only long enough to return a structured error to its first
    /// accepted socket; it never serves from partial or fabricated metadata.
    pub initialization_error: Option<String>,
    pub tables: Vec<IvmTableSpec>,
    /// Every replica table as TS `listTables` reports it (`fullTables` of
    /// `computeZqlSpecs`) — `check_client_schema` input.
    pub full_tables: Vec<LiteTableSpec>,
    /// SQLite replica path; `None` selects in-memory sources (test/dev).
    pub replica_path: Option<String>,
    pub app_id: String,
    /// Immutable creation version from `_zero.replicationConfig`. This is not
    /// the live snapshot watermark.
    pub replica_version: String,
    pub shard: ShardID,
    pub cvr_pg: Option<CvrPgConfig>,
    /// Compiled read-permissions (`PermissionsConfig` JSON) loaded from the
    /// replica, or `None` if none are deployed. A `None` doc still transforms
    /// client-AST queries with an EMPTY config — deny-by-default per table
    /// (TS view-syncer.ts:1549 `?? {tables: {}}`).
    pub permissions: Option<serde_json::Value>,
    /// The deployed permissions `hash` at load time, used to detect a
    /// hot-reload (a redeploy of `zero-deploy-permissions`). `None` when no
    /// permissions are deployed. Port of TS `LoadedPermissions.hash`.
    pub permissions_hash: Option<String>,
    /// Interval (ms) between periodic JWT re-validation + query re-transform for
    /// live connections. Port of TS `--auth-revalidate-interval-seconds`
    /// (default 300s). `None` disables periodic auth maintenance.
    pub revalidate_interval_ms: Option<i64>,
    /// Normalized server-side query endpoint configuration. The first URL is
    /// the default; the full list is the allow-list for client overrides.
    pub query_config: Option<FetchConfig>,
    /// Shadow-mode query-covering detection during hydration (TS
    /// `zeroConfig.enableQueryCovering`, default true); log-only.
    pub enable_query_covering: bool,
    /// Cost-based query-flip planning (TS `zeroConfig.enableQueryPlanner`,
    /// zero-config.ts:510 default true → PipelineDriver `enablePlanner`).
    pub enable_query_planner: bool,
    /// The two IVM time-slice thresholds TS `createViewSyncer` derives from
    /// `config.yieldThresholdMs` (server/syncer.ts:209-213): `max(threshold/4,
    /// 2)` while a priority op is running on this event loop, `max(threshold,
    /// 2)` otherwise. The driver's `yield_threshold_ms` selector picks between
    /// them per call (syncer.ts:230-233).
    pub priority_op_running_yield_threshold_ms: f64,
    pub normal_yield_threshold_ms: f64,
    /// Runtime handle for the `block_on` PG I/O edge on the CG thread.
    pub tokio_handle: tokio::runtime::Handle,
    /// Admin password gating the inspector protocol (TS `isAdminPasswordValid`).
    /// `None` disables the inspector (every `authenticate` fails).
    pub admin_password: Option<String>,
    /// Server version reported by the inspector `version` op.
    pub server_version: String,
    /// Shared process metrics (incremented on this CG's hot path).
    pub metrics: Arc<crate::metrics::Metrics>,
}

/// Factory trait for creating per-CG services.
pub trait CGServicesFactory: Send + Sync {
    /// Create the mutagen for a new CG (if configured).
    fn create_mutagen(&self, client_group_id: &str) -> Option<Arc<dyn MutagenDispatch>>;

    /// Create the pusher for a new CG (if configured).
    fn create_pusher(&self, client_group_id: &str) -> Option<Arc<dyn PusherDispatch>>;

    /// Build the `SyncEngine` config (engine + CVR store) for a new CG.
    fn create_sync_engine_config(&self, client_group_id: &str) -> SyncEngineConfig;
}

/// Auth validator trait — validates JWT tokens before connection creation.
///
/// Port of `resolveAuth()` in `auth.ts`. Runs on the tokio runtime
/// (may fetch JWKS).
#[async_trait::async_trait]
pub trait AuthValidator: Send + Sync {
    /// Validate auth token. Returns Ok(()) if valid, Err(error_body) if rejected.
    async fn validate_auth(
        &self,
        client_group_id: &str,
        client_id: &str,
        user_id: Option<&str>,
        auth: Option<&str>,
    ) -> Result<(), crate::protocol::ErrorBody>;
}

pub use crate::workers::syncer::{ConnectionSinks, GroupAuthState, Syncer};

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Threshold (ms) above which a hydration is logged as a slow query — the prod
/// signal operators use to find pathological queries. Port of TS
/// `log.slowHydrateThreshold` (otel/src/log-options.ts:24-29: env
/// `ZERO_LOG_SLOW_HYDRATE_THRESHOLD`, default 100 ms), which the view-syncer
/// receives as its `slowHydrateThreshold` ctor arg (view-syncer.ts:414). Read
/// once, cached.
///
/// `pub(crate)` so the pipeline driver's `VENDED` log gate reads the same
/// threshold — TS shares one `#logConfig.slowHydrateThreshold` across the
/// view-syncer's slow-hydrate log and the pipeline-driver's VENDED log.
pub(crate) fn slow_hydrate_threshold_ms() -> f64 {
    #[cfg(test)]
    if let Some(t) = SLOW_HYDRATE_THRESHOLD_OVERRIDE.with(|o| *o.borrow()) {
        return t;
    }
    static T: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *T.get_or_init(|| slow_hydrate_threshold_from_env(|k| std::env::var(k).ok()))
}

/// Env resolution for [`slow_hydrate_threshold_ms`], pure for testing. The TS
/// option's env name wins; `ZERO_SLOW_HYDRATE_THRESHOLD_MS` is the rust-only
/// name an earlier TS bridge emitted (rust-syncer-bridge.ts), kept
/// as a deprecated alias so a rust binary newer than its bridge still honors
/// the configured value. Unset / unparseable → TS's default of 100.
pub(crate) fn slow_hydrate_threshold_from_env(var: impl Fn(&str) -> Option<String>) -> f64 {
    [
        "ZERO_LOG_SLOW_HYDRATE_THRESHOLD",
        "ZERO_SLOW_HYDRATE_THRESHOLD_MS",
    ]
    .iter()
    .find_map(|k| var(k).and_then(|s| s.trim().parse::<f64>().ok()))
    .unwrap_or(100.0)
}

#[cfg(test)]
thread_local! {
    // Test seam (rust-only): lets a test pin the threshold without touching the
    // process env behind the `OnceLock` (TS tests pass `slowHydrateThreshold`
    // straight into the ViewSyncerService ctor).
    static SLOW_HYDRATE_THRESHOLD_OVERRIDE: std::cell::RefCell<Option<f64>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn set_slow_hydrate_threshold_for_test(threshold: Option<f64>) {
    SLOW_HYDRATE_THRESHOLD_OVERRIDE.with(|o| *o.borrow_mut() = threshold);
}

thread_local! {
    /// Port of TS `timeSliceQueue` (view-syncer.ts:2845-2859): "A global Lock
    /// acts as a queue to run a single IVM time slice per iteration of the node
    /// event loop, thus bounding I/O delay to the duration of a single time
    /// slice." TS's global is per sync-worker process, i.e. per event loop; the
    /// rust twin is per shard thread — each shard is one `current_thread`
    /// runtime, i.e. one event loop — so the queue spans exactly the client
    /// groups that share an event loop on both sides.
    static TIME_SLICE_QUEUE: Rc<tokio::sync::Mutex<()>> = Rc::new(tokio::sync::Mutex::new(()));
}

/// Port of TS `yieldProcess` (view-syncer.ts:2861-2863):
/// `timeSliceQueue.withLock(() => new Promise(setImmediate))`.
///
/// tokio's `yield_now` is the `setImmediate` twin: it defers the task until
/// after the runtime has polled its I/O driver (the tokio crate's own
/// `task/yield_now.rs`, not a file in this repo:
/// "the scheduler ... wakes deferred tasks only after it has run [the
/// driver]"), so every other ready task on this shard — the other client
/// groups' inbound frames and notifications, timers — runs before the next
/// slice: the one-slice-per-event-loop-iteration TS achieves with the
/// recursive-`setImmediate` note at view-syncer.ts:2853-2857. The FIFO
/// `tokio::sync::Mutex` is the `Lock`.
pub(crate) async fn yield_process() {
    let queue = TIME_SLICE_QUEUE.with(Rc::clone);
    let _slice = queue.lock().await;
    tokio::task::yield_now().await;
}

/// Port of TS `TimeSliceTimer` (view-syncer.ts:2943-3010): process-time
/// accounting for one IVM pass. `total` accumulates finished laps; `start` is
/// the running lap's start (`None` = not running, TS `#start === 0`).
/// `yield_process` stops the lap, yields the time slice, and starts a new one,
/// so `total_elapsed` EXCLUDES yielded time (what per-query hydration time and
/// the advance budget are measured in) and `elapsed_lap` is the current
/// slice's age (what `PipelineDriver#shouldYield` compares to the threshold).
pub struct TimeSliceTimer {
    total: std::cell::Cell<f64>,
    /// The running lap's start, on [`process_clock_ms`]'s clock (`None` = not
    /// running, TS `#start === 0`).
    start: std::cell::Cell<Option<f64>>,
}

/// The clock `TimeSliceTimer` laps on: this THREAD's CPU time.
///
/// Rust-only adaptation (AGENTS.md rule 5) that makes rust measure the same
/// QUANTITY as TS, not a different one. TS reads `performance.now()` — wall
/// time — but TS runs one event loop per sync-worker PROCESS
/// (`ZERO_NUM_SYNC_WORKERS`, 6 in the replay sandbox), so a running slice is never
/// preempted and its wall time IS its execution time. Rust's shard model
/// (INVENTIONS.md I-12) runs `ZERO_SYNCER_SHARDS` `current_thread` executors as
/// OS threads — 1,523 threads on a 20-core cpuset in the replay sandbox — so wall
/// time additionally counts OS preemption TS never experiences.
/// `CLOCK_THREAD_CPUTIME_ID` is the quantity that equals TS's
/// `performance.now()` delta under TS's execution model.
///
/// This matters because `MIN_ADVANCEMENT_TIME_LIMIT_MS` (50ms, advance_gate.rs)
/// is an ABSOLUTE floor: it is what stops TS's advance budget from firing on
/// short advances, and inflated wall time walks straight through it. Measured
/// on the same image and the same compressed 60m trace, ONLY
/// `ZERO_SYNCER_SHARDS` changed:
///
///   1500 shards / 1523 threads -> 1,194 `advancement-timeout` resets / 10 min
///     40 shards /   63 threads ->     3
///
/// Identical work; only the preemption differed. Each reset destroys every
/// pipeline in the group and forces a full re-hydrate, so this drove a
/// rehydrate storm (rust 2.9x TS's hydrations) and the client-visible p99 tail.
/// Dropping the shard count is NOT the fix — at 40 shards the client groups
/// serialize and steady p95 goes to 6,124 ms.
///
/// A lap never spans an `.await` (`stop_lap` runs before, `start_lap` after)
/// and each shard is a `current_thread` executor, so a lap is always measured
/// on one thread. Blocking I/O inside a lap is NOT counted, where TS's wall
/// clock would count it; the rust-only `ADVANCE_WALL_CLOCK_CEILING_MS` arm
/// (60s, exclusion-free wall time) remains the backstop for that.
fn process_clock_ms() -> f64 {
    let cpu = crate::trace::thread_cpu_ms();
    if cpu.is_nan() {
        // Platform without CLOCK_THREAD_CPUTIME_ID: fall back to wall time,
        // which is exactly TS's `performance.now()`.
        return std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64() * 1000.0)
            .unwrap_or(0.0);
    }
    cpu
}

impl Default for TimeSliceTimer {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeSliceTimer {
    pub fn new() -> Self {
        TimeSliceTimer {
            total: std::cell::Cell::new(0.0),
            start: std::cell::Cell::new(None),
        }
    }

    /// TS `start()`: "yield at the very beginning so that the first time slice
    /// is properly processed by the time-slice queue", then start.
    pub async fn start(&self) {
        yield_process().await;
        self.start_without_yielding();
    }

    /// TS `startWithoutYielding()`.
    pub fn start_without_yielding(&self) {
        self.total.set(0.0);
        self.start_lap();
    }

    /// TS `yieldProcess(_msgForTesting?)`: stop the lap, yield, start a lap.
    pub async fn yield_process(&self) {
        self.stop_lap();
        yield_process().await;
        self.start_lap();
    }

    fn start_lap(&self) {
        assert!(self.start.get().is_none(), "already running");
        self.start.set(Some(process_clock_ms()));
    }

    /// TS `elapsedLap()`.
    pub fn elapsed_lap(&self) -> f64 {
        let start = self.start.get().expect("not running");
        (process_clock_ms() - start).max(0.0)
    }

    fn stop_lap(&self) {
        let start = self.start.get().expect("not running");
        self.total
            .set(self.total.get() + (process_clock_ms() - start).max(0.0));
        self.start.set(None);
    }

    /// TS `stop()`: returns the total elapsed (process) time.
    pub fn stop(&self) -> f64 {
        self.stop_lap();
        self.total.get()
    }

    /// TS `totalElapsed()`: valid while running or after `stop`.
    pub fn total_elapsed(&self) -> f64 {
        match self.start.get() {
            None => self.total.get(),
            Some(start) => self.total.get() + (process_clock_ms() - start).max(0.0),
        }
    }
}

impl Timer for TimeSliceTimer {
    fn elapsed_lap(&self) -> f64 {
        TimeSliceTimer::elapsed_lap(self)
    }
    fn total_elapsed(&self) -> f64 {
        TimeSliceTimer::total_elapsed(self)
    }
}

/// Balance an admitted connection without allowing duplicate close/error paths
/// to wrap the unsigned counter to `u64::MAX`.
pub(crate) fn decrement_nonzero(count: &AtomicU64) {
    let _ = count.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(1))
    });
}

/// Router maps remain usable after an unrelated connection task panics. These
/// mutexes protect containers, not multi-step transactional invariants, so
/// cascading `PoisonError` panics only turn one socket failure into an outage.
pub(crate) fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Extract a JSON array of strings (dropping non-strings). Used for the
/// `activeClients` / `deleted.clientIDs` / `deleted.clientGroupIDs` body fields.
fn str_array(v: Option<&serde_json::Value>) -> Vec<String> {
    v.and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Parse a `desiredQueriesPatch` array into (puts, deletes). Port of the patch
/// shape from `zero-protocol/src/queries-patch.ts` (`op: put|del|clear`).
fn parse_desired_queries_patch(
    body: &serde_json::Value,
) -> (Vec<DesiredQuerySpec>, Vec<String>, bool) {
    let mut puts = Vec::new();
    let mut dels = Vec::new();
    let mut clear = false;
    let Some(patch) = body.get("desiredQueriesPatch").and_then(|v| v.as_array()) else {
        return (puts, dels, clear);
    };
    for entry in patch {
        match entry.get("op").and_then(|v| v.as_str()) {
            Some("put") => {
                let Some(hash) = entry.get("hash").and_then(|v| v.as_str()) else {
                    continue;
                };
                puts.push(DesiredQuerySpec {
                    hash: hash.to_string(),
                    ast: entry.get("ast").cloned(),
                    name: entry
                        .get("name")
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                    args: entry.get("args").and_then(|v| v.as_array()).cloned(),
                    ttl: entry.get("ttl").and_then(|v| v.as_i64()),
                });
            }
            Some("del") => {
                if let Some(hash) = entry.get("hash").and_then(|v| v.as_str()) {
                    dels.push(hash.to_string());
                }
            }
            // `clear` removes ALL of the client's desired queries (TS
            // `#patchQueries` → `clearDesiredQueries`). Applied before puts so a
            // clear-then-resubscribe patch replaces the whole set.
            Some("clear") => clear = true,
            _ => {}
        }
    }
    (puts, dels, clear)
}

/// Build the custom-query fetch context from the ConnectionContextManager's live
/// per-connection context. The CCM is the single owner of the query
/// url/headers/auth/userID; TS reads `mustGetConnectionContext(selector)` and
/// composes the query fetch from `connection.queryContext`, `connection.auth?.raw`
/// and `connection.user.id` at request time (transform-query.ts), rather than
/// from a separate cached map. This adapter maps the CCM's `ConnectionFetchContext`
/// onto the transform's `CustomQueryContext`.
///
/// Returns `None` when the connection has no resolved query URL (no configured
/// default and no `initConnection` `userQueryURL` override) — the client-fallback
/// path where no custom query API is reachable, matching TS's absent fetch config.
///
/// Rust-only adapter (no TS twin): TS's transformer reads the connection-context
/// fields inline; rust flattens them into `CustomQueryContext` because the ported
/// transform_query module consumes that shape. Header maps are sorted so the
/// forwarded set is deterministic regardless of `HashMap` iteration order.
fn custom_query_context_from(ctx: &CcmConnectionContext) -> Option<CustomQueryContext> {
    let query = &ctx.query_context;
    let url = query.url.clone()?;
    let sorted = |map: Option<&HashMap<String, String>>| {
        let mut headers: Vec<(String, String)> = map
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        headers.sort();
        headers
    };
    Some(CustomQueryContext {
        url,
        allowed_urls: query.allowed_url_patterns.clone().unwrap_or_default(),
        api_key: query
            .header_options
            .api_key
            .clone()
            .filter(|value| !value.is_empty()),
        client_headers: sorted(query.header_options.custom_headers.as_ref()),
        request_headers: sorted(query.header_options.request_headers.as_ref()),
        cookie: query.header_options.cookie.clone(),
        origin: query.header_options.origin.clone(),
        auth: ctx
            .auth
            .as_ref()
            .map(|a| a.raw().to_string())
            .filter(|value| !value.is_empty()),
        user_id: ctx.user.id.clone().filter(|value| !value.is_empty()),
        client_id: ctx.client_id.clone(),
        ws_id: ctx.ws_id.clone(),
        revision: ctx.revision,
    })
}

/// Format the WARN message TS logs for a per-query custom-query transform
/// failure. Byte-for-byte port of TS view-syncer.ts:1716:
///   `Error transforming custom query ${q.name}: ${q.error}${q.details ? ` ${JSON.stringify(q.details)}` : ''}`
/// `error` is the raw per-query error body returned by the API server
/// (`{id, name, error, details?}`).
fn format_transform_error_message(error: &serde_json::Value) -> String {
    let name = error
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    // TS interpolates `${q.error}`: a JSON string renders as its contents; the
    // API server returns a string error code (e.g. "app", "http", "zero").
    let err = match error.get("error") {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
        None => String::new(),
    };
    // TS: `${q.details ? ` ${JSON.stringify(q.details)}` : ''}` — a leading
    // space + compact JSON when `details` is present (truthy), else nothing.
    let details = match error.get("details") {
        Some(d) if !d.is_null() => format!(" {}", serde_json::to_string(d).unwrap_or_default()),
        _ => String::new(),
    };
    format!("Error transforming custom query {name}: {err}{details}")
}

/// Handle one errored custom-query transform result. Port of the `'error' in q`
/// branch of TS `#processTransformedCustomQueries` (view-syncer.ts:1715-1719):
/// log a WARN (so an operator sees clients hitting invalid/failing custom
/// queries) AND collect the error to forward to affected clients as a
/// `transformError`. Before this, rust forwarded to clients but was silent in
/// the logs — an observability divergence from TS.
fn record_transform_error(error: serde_json::Value, transform_errors: &mut Vec<serde_json::Value>) {
    tracing::warn!("{}", format_transform_error_message(&error));
    transform_errors.push(error);
}

/// Port of TS `wrapWithProtocolError` (zero-cache/src/types/error-with-level.ts):
/// an error that is not already a protocol error reaches the client as
/// `{kind: Internal, message: getErrorMessage(error), origin: ZeroCache}`.
/// Rust's failure paths carry plain `String` errors, so only the wrapping
/// branch lives here (the one protocol error a store load can raise,
/// ClientNotFound, is matched at its sites and passed through unchanged);
/// the message is the underlying error text, as TS's `getErrorMessage`
/// yields for an `Error`.
/// Lives in the consumer because the crate has no `types/` twin module.
pub(crate) fn wrap_with_protocol_error(message: &str) -> crate::protocol::ErrorBody {
    crate::protocol::ErrorBody::basic(crate::protocol::ErrorKind::Internal, message.to_string())
}

/// Port of `wrapWithProtocolError`'s PASSTHROUGH branch (error-with-level.ts:
/// 33-35 — `if (isProtocolError(error)) return error`) for the errors
/// `CVRStore` raises. TS defines them as ProtocolError SUBCLASSES
/// (cvr-store.ts:1354-1420), so each reaches the client with its OWN kind:
/// `OwnershipError` → Rehome + `maxBackoffMs: 0` (the CVR moved to another
/// task — reconnect NOW, do not back off), `ConcurrentModificationException` →
/// Rehome, `InvalidClientSchemaError` → SchemaVersionNotSupported,
/// `ClientNotFoundError` → ClientNotFound. Only the non-protocol errors
/// (sqlx, version parse, rows-behind) wrap as `Internal`.
///
/// Lives in the consumer because the twin file (`rust-cvr/src/cvr_store.rs`,
/// where the messages are built) cannot construct a rust-syncer `ErrorBody`:
/// rust-syncer depends on rust-cvr, not the other way round.
fn cvr_store_error_body(error: &CVRStoreError) -> crate::protocol::ErrorBody {
    use crate::protocol::{ErrorBody, ErrorKind};
    match error {
        CVRStoreError::ClientNotFound(message) => ErrorBody::client_not_found(message.clone()),
        // TS OwnershipError sets `maxBackoffMs: 0` (cvr-store.ts:1391).
        CVRStoreError::OwnershipError { .. } => {
            ErrorBody::rehome_with_max_backoff_ms(error.to_string(), 0)
        }
        CVRStoreError::ConcurrentModification { .. } => ErrorBody::rehome(error.to_string()),
        CVRStoreError::InvalidClientSchema(cause) => ErrorBody::basic(
            ErrorKind::SchemaVersionNotSupported,
            format!("Could not parse clientSchema stored in CVR: {cause}"),
        ),
        _ => wrap_with_protocol_error(&error.to_string()),
    }
}

/// The LEVEL TS attaches to the same failure. Each store error is thrown as a
/// `ProtocolErrorWithLevel` subclass (cvr-store.ts:1354-1415), so
/// `#cleanup(err)` → `client.fail(err)` logs at `getLogLevel(err)` and
/// `sendError` sees that level too: `ClientNotFoundError` 'warn' (:1362),
/// `ConcurrentModificationException` 'warn' (:1377), `OwnershipError` **'info'**
/// (:1400), `InvalidClientSchemaError` 'warn' (:1415). Anything else —
/// `RowsVersionBehindError` is a plain `Error` (:1437), as is a PG failure — is
/// the raw-throw branch: `getLogLevel` → 'error', and `sendError` runs its
/// errno / transient-socket checks against the thrown, so it travels as
/// `Thrown::Other` carrying its message.
///
/// Sibling of [`cvr_store_error_body`], in the consumer for the same reason.
/// Before d11e0f171 every caller passed `None` → 'warn' for all of
/// them, which logged an ownership transfer at WARN where TS logs INFO and a
/// PG outage at WARN where TS logs ERROR.
fn cvr_store_error_thrown<'a>(error: &CVRStoreError, message: &'a str) -> Thrown<'a> {
    use crate::workers::connection::LogLevel;
    // `name` is what TS `String(e)` prints before the message: `ProtocolError`
    // unless the class overrides `Error.name` (cvr-store.ts `readonly name`).
    match error {
        // cvr-store.ts:1354 — no `name` override, inherits 'ProtocolError'.
        CVRStoreError::ClientNotFound(_) => Thrown::WithLevel {
            level: LogLevel::Warn,
            name: "ProtocolError",
        },
        // cvr-store.ts:1368 `readonly name = 'ConcurrentModificationException'`.
        CVRStoreError::ConcurrentModification { .. } => Thrown::WithLevel {
            level: LogLevel::Warn,
            name: "ConcurrentModificationException",
        },
        // cvr-store.ts:1406 `readonly name = 'InvalidClientSchemaError'`.
        CVRStoreError::InvalidClientSchema(_) => Thrown::WithLevel {
            level: LogLevel::Warn,
            name: "InvalidClientSchemaError",
        },
        // cvr-store.ts:1383 `readonly name = 'OwnershipError'`.
        CVRStoreError::OwnershipError { .. } => Thrown::WithLevel {
            level: LogLevel::Info,
            name: "OwnershipError",
        },
        // cvr-store.ts:1438 `readonly name = 'RowsVersionBehindError'` (a plain
        // Error subclass, not a ProtocolError).
        CVRStoreError::RowsVersionBehind { .. } => Thrown::Other {
            name: "RowsVersionBehindError",
            message,
        },
        // TS `versionFromString`: a third `:` part is `new TypeError(...)`
        // (schema/types.ts:339); every other failure is a plain `Error`
        // (:333, lexi-version.ts:54).
        CVRStoreError::VersionParse(rust_cvr::schema::types::VersionError::TooManyParts(_)) => {
            Thrown::Other {
                name: "TypeError",
                message,
            }
        }
        // A PG server error is postgres.js's `PostgresError`; a pool / IO
        // failure surfaces as a plain `Error`.
        CVRStoreError::Sqlx(sqlx::Error::Database(_)) => Thrown::Other {
            name: "PostgresError",
            message,
        },
        _ => Thrown::Other {
            name: "Error",
            message,
        },
    }
}

/// Rust-only adapter for the TS union parameter of
/// `#sendQueryTransformErrorToClients` (`ErroredQuery[] | TransformFailedBody`,
/// view-syncer.ts:1730): `Failed` = the whole-batch `TransformFailedBody`
/// (carries `queryIDs`), `Application` = per-query `ErroredQuery` bodies.
enum QueryTransformErrors<'a> {
    Failed(&'a serde_json::Value),
    Application(&'a [serde_json::Value]),
}

/// Outcome of one background-retransform attempt. Mirrors the three control
/// paths of TS `#runBackgroundRetransform`'s `try/catch` (view-syncer.ts:2695):
/// the attempt either succeeds (`markBackgroundRetransformSuccess`), throws an
/// auth error (`isAuthErrorBody` → fail the connection + retry with a
/// replacement), or throws a transient transform-failed error
/// (`isTransformFailedError` → defer maintenance).
#[derive(Debug, Clone)]
enum RetransformOutcome {
    Success,
    /// TS `isAuthErrorBody(e.errorBody)` — carries the auth error body.
    AuthError(serde_json::Value),
    /// TS `isTransformFailedError(e)` — a transient / API-down transform failure.
    TransformFailed(serde_json::Value),
}

/// Classify a background retransform from the whole-batch custom-query transform
/// failure (if any) captured during its re-hydrate. Port of TS
/// `#runBackgroundRetransform`'s catch dispatch (view-syncer.ts:2700-2723):
/// `isAuthErrorBody(e.errorBody)` → `AuthError`, else `isTransformFailedError(e)`
/// → `TransformFailed`. `None` (no whole-batch failure recorded → the re-hydrate
/// did not throw) → `Success`. `is_auth_error_body` is the same predicate TS's
/// `#runBackgroundRetransform` uses, so the auth/transient split matches TS
/// exactly (auth.ts `isAuthErrorBody`).
fn classify_retransform_failure(failure: Option<serde_json::Value>) -> RetransformOutcome {
    match failure {
        None => RetransformOutcome::Success,
        Some(body) if crate::custom_queries::transform_query::is_auth_error_body(&body) => {
            RetransformOutcome::AuthError(body)
        }
        Some(body) => RetransformOutcome::TransformFailed(body),
    }
}

/// The message TS logs as `message: e.message` for a retransform failure
/// (view-syncer.ts:2705/2714). The `TransformFailedBody` carries the human
/// string in its `message` field; fall back to the compact JSON if absent.
fn transform_failure_message(body: &serde_json::Value) -> String {
    match body.get("message").and_then(serde_json::Value::as_str) {
        Some(m) => m.to_string(),
        None => body.to_string(),
    }
}

/// Rust-only adapter (no TS twin): backs the message handler's
/// `ConnContextManagerDispatch` with the ported [`ConnectionContextManager`], so
/// the handler's live reads — the mutagen-CRUD auth (`syncer_ws_message_handler.rs`)
/// and the relayed-push auth — see the SINGLE owner's CURRENT per-connection auth
/// at use time. Mirrors TS, which reads `mustGetConnectionContext(selector)` fresh
/// on the CRUD/push paths (pusher.ts:107). Replaces `PlaceholderConnContextManager`
/// (which returned `auth:None` — the I-8 latent divergence) for the router's live
/// handler.
///
/// `update_auth` here is ADVISORY only: the live CCM refresh for an
/// `updateAuth` message happens in `ViewSyncerService::handle_update_auth`
/// (unchanged-token skip, sub-pin, `ccm.update_auth` + re-validation — the
/// port of TS view-syncer.ts:1012), which the handler reaches via
/// `ViewSyncerDispatch::update_auth`. `init_connection` IS live here (records
/// the connection's URL/header overrides on its context).
struct CcmDispatchAdapter {
    ccm: Arc<Mutex<ConnectionContextManager>>,
}

impl CcmDispatchAdapter {
    fn new(ccm: Arc<Mutex<ConnectionContextManager>>) -> Self {
        Self { ccm }
    }
}

impl ConnContextManagerDispatch for CcmDispatchAdapter {
    fn must_get_connection_context(
        &self,
        selector: &ConnectionSelector,
    ) -> Result<ConnContextInfo, Box<crate::protocol::ErrorBody>> {
        let sel = CcmConnectionSelector {
            client_id: selector.client_id.clone(),
            ws_id: selector.ws_id.clone(),
        };
        // MUST semantics — port of TS `mustGetConnectionContext` (a missing
        // context THROWS `InvalidConnectionRequest`). This adapter previously
        // defaulted to `auth: None` on a miss, which the push relay then
        // forwarded as an Authorization-less POST — the production
        // "No token provided" push-relay 401s. Never default; surface the error.
        match lock_unpoisoned(&self.ccm).must_get_connection_context(&sel) {
            Ok(ctx) => Ok(ConnContextInfo {
                auth: ctx.auth.as_ref().map(|a| a.raw().to_string()),
                is_opaque: matches!(
                    ctx.auth,
                    Some(
                        crate::services::view_syncer::connection_context_manager::Auth::Opaque { .. }
                    )
                ),
                revision: ctx.revision,
            }),
            // Each CCMError variant already names its TS ProtocolError kind.
            Err(CCMError::InvalidConnectionRequest(m)) => {
                Err(Box::new(crate::protocol::ErrorBody::basic(
                    crate::protocol::ErrorKind::InvalidConnectionRequest,
                    m,
                )))
            }
            Err(CCMError::Unauthorized(m)) => Err(Box::new(crate::protocol::ErrorBody::basic(
                crate::protocol::ErrorKind::Unauthorized,
                m,
            ))),
            Err(CCMError::AuthInvalidated(m)) => Err(Box::new(crate::protocol::ErrorBody::basic(
                crate::protocol::ErrorKind::AuthInvalidated,
                m,
            ))),
        }
    }

    /// Port of the TS `SyncerWsMessageHandler` 'initConnection' side effect
    /// `connContextManager.initConnection(...)`: record the connection's
    /// URL/header overrides on its context (this dispatch is the single
    /// recording site).
    fn init_connection(&self, selector: &ConnectionSelector, body: &serde_json::Value) {
        let str_field = |k: &str| {
            body.get(k)
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        };
        let map_field = |k: &str| {
            body.get(k).and_then(|v| v.as_object()).map(|o| {
                o.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect::<std::collections::HashMap<String, String>>()
            })
        };
        let init_body = InitConnectionBody {
            user_query_url: str_field("userQueryURL"),
            user_query_headers: map_field("userQueryHeaders"),
            user_push_url: str_field("userPushURL"),
            user_push_headers: map_field("userPushHeaders"),
        };
        let ccm_selector = CcmConnectionSelector {
            client_id: selector.client_id.clone(),
            ws_id: selector.ws_id.clone(),
        };
        let _ = lock_unpoisoned(&self.ccm).init_connection(&ccm_selector, &init_body);
    }

    /// The revision result is advisory on this path: the live view-syncer
    /// dispatch (`handle_update_auth`) performs the ported unchanged-check
    /// itself (raw-token compare against the CCM) and owns the CCM refresh,
    /// so the handler's pre-dispatch CCM update is a no-op here.
    fn update_auth(&self, _selector: &ConnectionSelector, _body: &serde_json::Value) -> bool {
        true
    }
}

/// The live `ViewSyncerDispatch` — TS `Connection` holds
/// `#viewSyncer`, and each `SyncerWsMessageHandler` arm calls
/// `viewSyncer.<method>`, whose body runs under the view-syncer `#lock`.
/// Rust twin: the adapter holds the CG task's own service cell and runs the
/// 1:1 method INLINE to completion — the CG task is the lock. `borrow_mut` is
/// safe because the inbound path (`on_inbound`) releases its borrow before
/// awaiting the handler, and nothing inside these bodies re-enters the cell.
pub(crate) struct CgViewSyncer {
    svc: std::rc::Weak<std::cell::RefCell<ViewSyncerService>>,
}

/// The message body (`["tag", body]` second element) of a raw upstream frame.
fn second_element(msg: &str) -> serde_json::Value {
    let arr: Vec<serde_json::Value> = serde_json::from_str(msg).unwrap_or_default();
    arr.get(1).cloned().unwrap_or(serde_json::Value::Null)
}

// The cell is confined to the single-threaded CG task: a `RefCell` borrow held
// across an await cannot race (no other task touches it), and the only re-entry
// path — the inbound dispatch — releases its borrow before awaiting the handler
// (see `on_inbound`). The lint guards multi-task executors; this is the
// deliberate TS-`#lock` twin.
#[allow(clippy::await_holding_refcell_ref)]
#[async_trait::async_trait(?Send)]
impl ViewSyncerDispatch for CgViewSyncer {
    async fn change_desired_queries(&self, selector: &ConnectionSelector, msg: &str) {
        let Some(svc) = self.svc.upgrade() else {
            return;
        };
        let body = second_element(msg);
        svc.borrow_mut()
            // TS `changeDesiredQueries` -> `#handleConfigUpdate(..., 'missing', ...)`
            // (view-syncer.ts:981).
            .handle_desired_queries(
                &selector.client_id,
                &body,
                ConfigPassOrigin::ChangeDesiredQueries,
                CustomQueryTransformMode::Missing,
            )
            .await;
    }

    async fn update_auth(
        &self,
        selector: &ConnectionSelector,
        msg: &str,
        _auth_revision_changed: bool,
    ) {
        let Some(svc) = self.svc.upgrade() else {
            return;
        };
        let token = second_element(msg)
            .get("auth")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        svc.borrow_mut()
            .handle_update_auth(&selector.client_id, &token)
            .await;
    }

    async fn delete_clients(&self, selector: &ConnectionSelector, msg: &str) -> Vec<String> {
        let Some(svc) = self.svc.upgrade() else {
            return Vec::new();
        };
        let body = second_element(msg);
        let del_ids = str_array(body.get("clientIDs"));
        let group_ids = str_array(body.get("clientGroupIDs"));
        svc.borrow_mut()
            .apply_client_deletions(&selector.client_id, None, &del_ids, &group_ids)
            .await;
        // The ack'd (cleanup-eligible) ids — explicit deletions minus the
        // caller; the handler relays `_zero_cleanupResults` for these (TS
        // `deleteClients` returns `deleted.clientIDs`).
        del_ids
            .into_iter()
            .filter(|c| c.as_str() != selector.client_id)
            .collect()
    }

    async fn init_connection(&self, selector: &ConnectionSelector, msg: &str) -> bool {
        let Some(svc) = self.svc.upgrade() else {
            return false;
        };
        let body = second_element(msg);
        svc.borrow_mut()
            // TS `initConnection` -> `#handleConfigUpdate(..., 'all', ...)`:
            // "re transform all on new connections" (view-syncer.ts:949).
            .handle_desired_queries(
                &selector.client_id,
                &body,
                ConfigPassOrigin::InitConnection,
                CustomQueryTransformMode::All,
            )
            .await
    }

    async fn inspect(&self, selector: &ConnectionSelector, msg: &str) {
        let Some(svc) = self.svc.upgrade() else {
            return;
        };
        let body = second_element(msg);
        svc.borrow_mut()
            .handle_inspect(&selector.client_id, &body)
            .await;
    }
}

/// One query's replacement history inside the thrash window. Port of the TS
/// `#queryReplacements` record shape `{count, windowStart}` (view-syncer.ts
/// `#checkForThrashing`).
struct QueryReplacementRecord {
    count: u32,
    window_start: i64,
}

/// Per-CG state, owned by (and confined to) the CG thread. Holds the `!Send`
/// [`SyncEngine`] plus the live connections. Extracted from the event loop so
/// the message handlers are unit-testable.
pub struct ViewSyncerService {
    cg_id: String,

    pipelines: IvmPipelines,
    store: Option<Arc<tokio::sync::Mutex<CVRStoreHandle>>>,
    /// Read-source for `existing_rows` (the row records the client already has).
    /// The store persists the `rows` table; this cache reads it back.
    clients: HashMap<String, Arc<ClientHandler>>,
    /// Handle to the shared-pool runtime (the process's main multi-thread
    /// runtime) onto which CVR Postgres I/O is offloaded. The client group runs
    /// on a single-threaded executor whose reactor does NOT drive the shared CVR
    /// pool's connections; spawning the I/O future here runs it on the runtime
    /// that DOES drive them (`RUST-SYNCER-ARCHITECTURE.md` §3), while the executor only awaits the
    /// resulting `JoinHandle` and stays free to run its other client groups.
    /// `None` in unit tests that inject no handle — I/O then runs inline.
    tokio_handle: Option<tokio::runtime::Handle>,
    /// Shadow-mode query-covering detection during hydration. Port of TS
    /// `zeroConfig.enableQueryCovering` (default true). When on, each hydration
    /// batch compares its queries against the running set and logs aggregate
    /// coverage stats — it has NO effect on what is served.
    enable_query_covering: bool,
    /// Set when a store flush was MATERIAL (`flushed: true` in TS terms).
    /// Rust-only bridge: TS's `#flushUpdater` sees `{flushed}` directly and
    /// restarts the ttlClock interval on it (view-syncer.ts:1083-1086); here
    /// the router polls this flag after each dispatched message via
    /// `take_flush_observed` to do the same. `Cell` because the engine is
    /// single-threaded (`!Send`) and flush helpers take `&self`.
    flush_observed: std::cell::Cell<bool>,
    /// Live-instance census guard for the dissolved engine seat (the
    /// `SYNC_ENGINE` counter kept alive for /statz + the census gate; counts
    /// identically to `_census` since the merge).
    _engine_census: crate::live_count::Guard,
    /// Handle to this service's own shared cell, set by `cg_event_loop` right
    /// after construction (`None` in the storeless engine-surface scaffold).
    /// Rust-only: each connection's message handler gets a
    /// `CgViewSyncer` dispatch built from this handle — the twin of TS
    /// `Connection` holding `#viewSyncer` — so `viewSyncer.<method>` executes
    /// inline on the CG task through a `RefCell` borrow (safe: the CG task is
    /// single-threaded and the inbound path releases its borrow before
    /// awaiting the handler).
    self_handle: Option<std::rc::Weak<std::cell::RefCell<ViewSyncerService>>>,
    /// The single owner of per-connection auth/context state — the ported 1:1
    /// `ConnectionContextManager` (TS `ViewSyncerService`'s `#connContextManager`).
    /// `Arc<Mutex>` (rust-only: the CCM is shared with the `Send+Sync` dispatch;
    /// uncontended on the single CG thread). Migration status is tracked in
    /// `parity/I8-CCM-PROMOTION-SPEC.md`, not here.
    ccm: Arc<Mutex<ConnectionContextManager>>,
    mutagen: Option<Arc<dyn MutagenDispatch>>,
    pusher: Option<Arc<dyn PusherDispatch>>,
    shard: ShardID,
    replica_version: String,
    cvr_pg: bool,
    /// Table specs + replica path + app id retained so the pipeline can be
    /// re-initialized on an advance reset (TS `#pipelines.reset` /
    /// `ResetPipelinesSignal` → rehydrate).
    tables: Vec<IvmTableSpec>,
    full_tables: Vec<LiteTableSpec>,
    replica_path: Option<String>,
    app_id: String,
    /// Compiled read-permissions for this CG's app (loaded from the replica).
    permissions: Option<serde_json::Value>,
    /// The deployed permissions `hash` last loaded, for hot-reload detection
    /// (TS `reloadPermissionsIfChanged`).
    permissions_hash: Option<String>,
    /// Wall-clock (ms) deadline for the next auth-maintenance tick, or `None`
    /// when nothing is armed. Sourced from the ported CCM planner
    /// (`plan_maintenance().earliest_deadline_at` — TS
    /// `#scheduleAuthMaintenance`); the revalidate interval itself lives in the
    /// CCM (single owner), not here.
    next_auth_maintenance_at: Option<i64>,
    /// Rust-only adapter for TS `#syncQueryPipelineSet`'s THROW on a whole-batch
    /// custom-query transform failure (view-syncer.ts:1983
    /// `throw new ProtocolErrorWithLevel(result, 'warn')`). `sync_query_pipeline_set`
    /// forwards per-query transform errors to clients inline and cannot unwind
    /// across the serial re-hydrate, so it records the whole-batch
    /// `TransformFailedBody` here; `run_background_retransform` — the only caller
    /// that inspects it — resets it before its re-hydrate and reads it after, then
    /// branches exactly like TS `#runBackgroundRetransform`. Serial CG thread ⇒ no
    /// races. Left `None` on the init / changeDesiredQueries / updateAuth paths,
    /// which never read it (their whole-batch handling is unchanged).
    background_retransform_failure: Option<serde_json::Value>,
    /// Test seam (empty in production): forced outcomes for
    /// `attempt_background_retransform`, so `run_background_retransform`'s
    /// mark/warn/fail/retry/defer control flow can be pinned without a live
    /// query-API round trip. The real capture→classify transport is covered by
    /// `classify_retransform_failure`'s unit test.
    forced_retransform_outcomes: std::collections::VecDeque<RetransformOutcome>,
    /// Port of TS `#pipelinesSynced` (view-syncer.ts:274). `false` until the CG's
    /// pipelines are first built from the (possibly populated) CVR; set `true`
    /// after that init, and reset to `false` only on a pipeline reset
    /// (`reset_pipelines_and_rehydrate`, TS `#pipelines.reset()` +
    /// `#pipelinesSynced = false`, view-syncer.ts:575-576). Gates
    /// `hydrate_unchanged_queries`: TS runs `#hydrateUnchangedQueries` ONCE in the
    /// run-loop init block (view-syncer.ts:592, guarded by this flag), NOT on
    /// every connect/config-change — a per-sync re-hydrate re-materialized every
    /// alive pipeline on each reconnect (whale client-group hydrates of 20-88 s).
    pipelines_synced: bool,
    /// Test observability (increments only when the `hydrate_unchanged_queries`
    /// gate opens): pins that the proactive re-hydrate runs once per pipeline init,
    /// not per sync.
    #[cfg(test)]
    hydrate_unchanged_runs: u64,
    /// Test observability: how many times `validate_connection` (TS
    /// `#validateConnection`) actually ran. Pins the TS gates that decide WHETHER
    /// to validate — `updateAuth`'s `if (!this.#pipelinesSynced)`
    /// (view-syncer.ts:1011) above all — which no other observable distinguishes
    /// without a live query-API round trip.
    #[cfg(test)]
    validate_connection_runs: u64,
    /// Test observability: how many times the config/hydrate pass actually ran.
    /// `updateAuth` and the background retransform arrive with an EMPTY body, so
    /// every "did the re-transform happen?" assertion otherwise has to infer it
    /// from a side effect — and the `forced_retransform_outcomes` seam skips the
    /// call entirely, which is how a gate that made both of them no-ops passed
    /// the existing tests.
    #[cfg(test)]
    config_pass_runs: u64,
    /// Test observability: the CCM client id whose context each post-reset
    /// rehydrate pass ran with. TS runs exactly ONE pass, with the background
    /// connection's context (view-syncer.ts:592-606, :1500-1501, :1913-1914).
    #[cfg(test)]
    reset_pass_contexts: Vec<String>,
    /// Test seam (empty in production): forced outcomes for `flush_ops_to_store`,
    /// so the storeless harness can exercise the QUIET-COMMIT branch
    /// (`flushed: false` — TS `#flush` returning `{cvr: this._orig, flushed:
    /// false}` when nothing material changed). Without it that branch needs a
    /// live PG CVR store, and it is the branch the config-poke ORDER bug lived
    /// in (`config_update_no_op_flush_does_not_close_client`).
    #[cfg(test)]
    forced_flush_outcomes: std::cell::RefCell<std::collections::VecDeque<bool>>,
    /// Test observability: how many times `existing_rows()` was CALLED (not how
    /// many hit the store). Pins the TS-parity laziness of the CVR row-map read
    /// on the advance path — an advance that collected no row changes must not
    /// reach for the map at all, because TS never materialises one for an
    /// advance (`updater.received(lc, rows)` takes only the changed batch, and
    /// the store reads row records solely inside `#flush` under
    /// `if (this.#pendingRowRecordUpdates.size)`). No other observable
    /// distinguishes "skipped the read" from "read a warm cache".
    #[cfg(test)]
    existing_rows_calls: std::cell::Cell<usize>,
    /// The userID this client group is pinned to (the `sub` of the first authed
    /// connection; `None` for an anonymous group). Admission
    /// (`check_and_pin_user`) guarantees every connection reaching this CG shares
    /// it. Enforced on `updateAuth` and periodic revalidation so a validly-signed
    /// token for a DIFFERENT user cannot re-scope the group mid-connection. Port
    /// of `GroupAuthState.pinnedUser` + `pickToken`'s single-user pin.
    pinned_user_id: Option<String>,
    /// The in-memory CVR, lazily loaded from the store on first notification.
    cvr: Option<CVR>,
    /// Per-query transformation-replacement records for thrash detection.
    /// Port of TS `#queryReplacements` (view-syncer.ts, consumed by
    /// `#checkForThrashing`).
    query_replacements: HashMap<String, QueryReplacementRecord>,
    /// End-to-end serving-lag tracker: pairs each `version-ready`'s upstream
    /// commit time with the moment its version is served, feeding the
    /// `zero.sync.e2e_serving_lag` histogram. Port of TS `#e2eServingLagTracker`.
    e2e_serving_lag: crate::services::view_syncer::e2e_serving_lag::E2EServingLagTracker,
    /// Monotonic TTL clock (ms), seeded from `cvr.ttl_clock` when the CVR is
    /// loaded and advanced by wall-time delta while this CG runs — so a long
    /// downtime does not mass-expire queries. Port of TS `#ttlClock`.
    ttl_clock: TTLClock,
    /// Wall-clock (ms) at the last `get_ttl_clock`. Port of TS `#ttlClockBase`.
    ttl_clock_base: i64,
    /// Port of TS `#ttlClockInterval` (view-syncer.ts:260). Realized as the
    /// wall-clock (ms) deadline of the next ttlClock persistence tick rather
    /// than a timer handle — the CG event loop multiplexes deadlines instead of
    /// holding per-purpose timers. `None` = interval not running (TS `0`).
    ttl_clock_interval: Option<i64>,
    /// Port of TS `#expiredQueriesTimer` (view-syncer.ts:278). Wall-clock (ms)
    /// deadline of the next TTL-eviction pass, armed by `schedule_expire_eviction`
    /// and cleared by `stop_expire_timer`. Realized as a deadline the CG event
    /// loop multiplexes rather than a timer handle. `None` = timer stopped
    /// (TS `0`) — an idle group with no connected clients runs no eviction.
    expired_queries_timer: Option<i64>,
    /// Wall-clock time of the most recent newly established connection. This is
    /// the ownership lease boundary passed to every CVR load/flush.
    last_connect_time: i64,
    /// Earliest time an empty CG may shut down. TS view-syncers stop after five
    /// seconds without clients so their SQLite readers, PG pools, and OS thread
    /// do not accumulate under cold-client churn.
    keepalive_until: i64,
    /// client_id → Connection. `Rc`: the inbound path clones the
    /// connection out and releases the service-cell borrow before awaiting the
    /// handler (whose live dispatch re-borrows the cell).
    connections: HashMap<String, Rc<Connection>>,
    /// client_id → ws_id, for clients registered with the SyncEngine.
    registered_ws: HashMap<String, String>,
    /// Client cookie captured before the CVR is loaded. TS validates this
    /// against the loaded CVR before accepting initConnection.
    client_base_versions: HashMap<String, NullableCVRVersion>,
    /// Accepted sockets whose increment of `connection_count` has not yet been
    /// balanced by a close event. This includes superseded sockets.
    open_ws_ids: HashSet<String>,
    /// ws_id → sync protocol version, for balancing the `zero.sync.active-clients`
    /// UpDownCounter: +1 (tagged with the version) on register, -1 (same version)
    /// on disconnect. Only decrement a ws we incremented, so the gauge stays
    /// balanced across supersede/close races.
    active_client_pv: HashMap<String, u32>,
    /// client_id → raw auth/header material captured at connect, needed to
    /// relay a `_zero_cleanupResults` push when this client explicitly deletes
    /// other clients (`deleteClients` → `pusher.delete_client_mutations`).
    client_push_headers:
        HashMap<String, crate::workers::syncer_ws_message_handler::PushRelayHeaders>,
    /// profileID supplied in the connection URL, persisted into the CVR on init.
    client_profile_ids: HashMap<String, String>,
    /// Admin password gating the inspector protocol; server version for the
    /// inspector `version` op.
    admin_password: Option<String>,
    server_version: String,
    /// Shared process metrics.
    metrics: Arc<crate::metrics::Metrics>,
    /// Whether this client group has authenticated to the inspector protocol
    /// (TS `InspectorDelegate.isAuthenticated(clientGroupID)`). Set once per CG
    /// by a successful `authenticate` op.
    inspector_authenticated: bool,
    /// Per-CG inspector server-metrics + queryID→AST store (TS
    /// `InspectorDelegate`, server/inspector_delegate.rs). Fed the per-query
    /// `query-materialization-server` timing at hydrate and `add_query`/
    /// `remove_query` at the query lifecycle; read by the `metrics`/`queries`
    /// inspect ops. `RefCell` because `to_json` mutates the digest (`#process`)
    /// while the read ops borrow the service immutably.
    inspector_delegate: std::cell::RefCell<crate::server::inspector_delegate::InspectorDelegate>,
    /// JWT validator, for re-verifying an `updateAuth` token mid-connection.
    auth_validator: Arc<dyn AuthValidator>,
    global_connections: Arc<Mutex<HashMap<String, ConnectionInfo>>>,
    connection_count: Arc<AtomicU64>,
    accepting: Arc<AtomicBool>,
    /// A fatal sync/store failure makes the CG unusable: the snapshot and IVM
    /// graph may already have advanced while the CVR did not commit.
    terminal: bool,
    /// Wall-clock (ms) at construction. Port of TS `ViewSyncerService.createdAtMs`
    /// (a serving-lag input: replica states before this are already accounted).
    created_at_ms: i64,
    /// The last stateVersion poked to clients. Port of TS `#servedVersion`; feeds
    /// `serving_lag_eligible`'s "unserved" computation via the shared registry.
    served_version: Option<String>,
    /// Last observed tracked-row count (published to the `rows` gauge). Refreshed
    /// where the CG already holds the row map, so reading it costs no CVR I/O.
    /// Port of TS `ViewSyncerService.rowCount` (there a cheap in-memory getter).
    /// TS `ViewSyncerService.rowCount` reads `#cvrStore.rowCount`, which the
    /// store sets in exactly two places, both inside `#flush` (cvr-store.ts:1068
    /// and :1217-1218). Rust copies the store's `row_count()` out under the
    /// store lock right after each flush (`flush_ops_to_store`) — shared with
    /// the offloaded flush task, hence the atomic. Nothing else writes it: an
    /// advance that collected nothing never touches the row map (4453a0f91) and
    /// must not zero this either (an earlier version did).
    last_row_count: Arc<std::sync::atomic::AtomicUsize>,
    /// Process-wide serving-lag registry this CG publishes its snapshot into.
    serving_lag_registry: Arc<crate::workers::syncer::ServingLagRegistry>,
    /// Live-instance census guard (leak hunt): inc on construct, dec on drop.
    /// THE most important census — a ViewSyncerService owns the `SyncEngine` (IVM graph +
    /// CVR store), so a residual count after all clients disconnect pins
    /// everything below. See the `Drop` impl for the teardown backtrace hook.
    _census: crate::live_count::Guard,
}

impl Drop for ViewSyncerService {
    fn drop(&mut self) {
        // Drop this CG's serving-lag snapshot on every teardown path (normal
        // return, TTL/idle shutdown, panic-unwind) — TS drops it when the
        // view-syncer service stops.
        self.serving_lag_registry.remove_view_syncer(&self.cg_id);
        // TS `stop()`/`#cleanup` both call `setSharedRetransformReady(false)`
        // (view-syncer.ts:2803/2811) — the group's shared retransform must not
        // run once teardown starts.
        lock_unpoisoned(&self.ccm).set_shared_retransform_ready(false);
        // Attribute *who* tore down this client group when
        // `RUST_SYNCER_DROP_BACKTRACE=1`. The census counter dec's via the
        // `_census` guard's own `Drop`.
        crate::live_count::drop_backtrace("ViewSyncerService");
    }
}

impl ViewSyncerService {
    /// The SQLite replica path this CG serves, if any (`None` for in-memory
    /// test/dev CGs). Used by the `analyze-query` inspect op to open its own
    /// read-only analysis engine — TS `config.replica.file`.
    pub fn replica_path(&self) -> Option<&str> {
        self.replica_path.as_deref()
    }

    /// The app id (schema prefix) — TS `config.app.id`. Needed to open the
    /// analysis engine's snapshotter over the replica.
    pub fn app_id(&self) -> &str {
        &self.app_id
    }

    /// This CG's shard id. Used by the `analyze-query` named-query path to
    /// transform custom queries against the user's query API server.
    pub fn shard(&self) -> &ShardID {
        &self.shard
    }

    /// This CG's inspector metrics + AST store (TS `InspectorDelegate`). Read by
    /// the `metrics`/`queries` inspect ops and written at the query lifecycle.
    pub fn inspector_delegate(
        &self,
    ) -> &std::cell::RefCell<crate::server::inspector_delegate::InspectorDelegate> {
        &self.inspector_delegate
    }

    /// The sync protocol version negotiated by a connection (TS
    /// `ctx.protocolVersion`), used by the `queries` op's `metricsForProtocol`
    /// wire-shape selection. Falls back to the server's current
    /// `PROTOCOL_VERSION` when the ws is not (yet) registered.
    pub fn protocol_version_for_ws(&self, ws_id: &str) -> u32 {
        self.active_client_pv
            .get(ws_id)
            .copied()
            .unwrap_or(crate::protocol::PROTOCOL_VERSION)
    }

    #[cfg(test)]
    fn new_test(
        cg_id: &str,
        services_factory: &Arc<dyn CGServicesFactory>,
        auth_validator: Arc<dyn AuthValidator>,
        global_connections: Arc<Mutex<HashMap<String, ConnectionInfo>>>,
        connection_count: Arc<AtomicU64>,
    ) -> Self {
        Self::new_with_accepting(
            cg_id,
            services_factory,
            auth_validator,
            global_connections,
            connection_count,
            Arc::new(AtomicBool::new(true)),
            None,
        )
    }

    /// `cvr_pool` is the ONE process-wide shared CVR pool (built on the main
    /// runtime — `RUST-SYNCER-ARCHITECTURE.md` §3). When the factory config requests a CVR
    /// store (`cvr_pg`), the store binds to this pool; the engine then offloads
    /// its CVR I/O onto the pool's own runtime (`SyncEngine::offload`, §5.1).
    /// `None` selects an in-memory / storeless CG (tests, no-PG dev).
    fn new_with_accepting(
        cg_id: &str,
        services_factory: &Arc<dyn CGServicesFactory>,
        auth_validator: Arc<dyn AuthValidator>,
        global_connections: Arc<Mutex<HashMap<String, ConnectionInfo>>>,
        connection_count: Arc<AtomicU64>,
        accepting: Arc<AtomicBool>,
        cvr_pool: Option<sqlx::PgPool>,
    ) -> Self {
        let mutagen = services_factory.create_mutagen(cg_id);
        let pusher = services_factory.create_pusher(cg_id);
        let config = services_factory.create_sync_engine_config(cg_id);

        // Build the SyncEngine on this thread (it is !Send). Retain the table
        // specs + replica path + app id so the pipeline can be re-initialized on
        // an advance reset.
        let admin_password = config.admin_password.clone();
        let server_version = config.server_version.clone();
        let metrics = config.metrics.clone();
        let tables = config.tables.clone();
        let full_tables = config.full_tables.clone();
        let replica_path = config.replica_path.clone();
        let app_id = config.app_id.clone();
        // Engine seat (the former `SyncEngine::new`): pipelines + CVR-store
        // fields now live directly on the service, per TS ownership.
        let mut pipelines = IvmPipelines::new();
        // TS threads `config.enableQueryPlanner` into the PipelineDriver ctor
        // (server/syncer.ts:222); set before `init` so `build_engine` sees it.
        pipelines.enable_query_planner = config.enable_query_planner;
        // TS server/syncer.ts:230-233 — the PipelineDriver ctor's
        // `yieldThresholdMs` selector: the priority-op threshold while a
        // priority op is running on this event loop, else the normal one. Set
        // before `init` so `build_engine` wires it into every source.
        let priority_op_running_yield_threshold_ms = config.priority_op_running_yield_threshold_ms;
        let normal_yield_threshold_ms = config.normal_yield_threshold_ms;
        pipelines.set_yield_threshold_ms(Rc::new(move || {
            if is_priority_op_running() {
                priority_op_running_yield_threshold_ms
            } else {
                normal_yield_threshold_ms
            }
        }));
        let tokio_handle = Some(config.tokio_handle.clone());
        let enable_query_covering = config.enable_query_covering;
        let mut initialization_failed = config.initialization_error.is_some();
        if let Some(error) = &config.initialization_error {
            tracing::error!("CG {cg_id}: initialization failed: {error}");
        }
        if let Err(e) = pipelines.init(
            config.tables,
            config.replica_path.as_deref(),
            &config.app_id,
        ) {
            tracing::error!("CG {cg_id}: pipelines init failed: {e}");
            initialization_failed = true;
        }
        let replica_version = config.replica_version;
        let permissions = config.permissions;
        let permissions_hash = config.permissions_hash;
        let revalidate_interval_ms = config.revalidate_interval_ms;

        // Construct the ConnectionContextManager (TS `new ConnectionContextManager`).
        // The CCM is the single owner of the query fetch config; `push_config`/
        // `validate_legacy_jwt` are `None`: the modern path has no legacy JWT
        // validator (TS `validateLegacyJWT` undefined) and no consumer reads
        // `mutate_context`; `now` defaults to `now_ms`. Seconds granularity
        // matches the TS constructor (it re-multiplies to ms internally).
        let ccm = Arc::new(Mutex::new(ConnectionContextManager::new(
            revalidate_interval_ms.map(|ms| (ms / 1000).max(0) as u64),
            None,
            config.query_config,
            None,
            None,
            None,
        )));

        // Wire the pusher's auth-failure invalidation into THIS CG's CCM (TS
        // pusher.ts:539: `isAuthErrorBody(response)` → `#connContextManager
        // .failConnection(entry.connCtx, entry.connCtx.revision)`). The hook
        // runs on the pusher's drainer task; the CCM's revision guard makes a
        // stale invalidation (connection already re-authed) a no-op.
        if let Some(p) = &pusher {
            let hook_ccm = ccm.clone();
            p.set_auth_fail_hook(Arc::new(move |selector, revision| {
                let sel = CcmConnectionSelector {
                    client_id: selector.client_id.clone(),
                    ws_id: selector.ws_id.clone(),
                };
                let failed = lock_unpoisoned(&hook_ccm).fail_connection(&sel, revision);
                if failed.is_some() {
                    tracing::warn!(
                        client_id = %selector.client_id,
                        "Push auth failed; invalidating connection"
                    );
                }
            }));
        }

        // TS pusher.ts:545-556: a SUCCESSFUL push validates the connection's
        // current auth snapshot (`server-validated` with the API server's
        // userID, else `client-fallback`) — the CCM marks it Validated, pins
        // the group user and arms revalidation; a stale revision is a no-op
        // and a userID mismatch is `Unauthorized` (the pusher turns that into
        // `failConnection` + a PushFailed http 401, pusher.ts:578-590).
        if let Some(p) = &pusher {
            let hook_ccm = ccm.clone();
            p.set_validate_hook(Arc::new(move |selector, revision, validation| {
                let sel = CcmConnectionSelector {
                    client_id: selector.client_id.clone(),
                    ws_id: selector.ws_id.clone(),
                };
                lock_unpoisoned(&hook_ccm)
                    .validate_connection(&sel, revision, &validation)
                    .map(|_| ())
            }));
        }

        let created_at = now_ms();
        let mut svc = ViewSyncerService {
            cg_id: cg_id.to_string(),
            pipelines,
            store: None,
            query_replacements: HashMap::new(),
            clients: HashMap::new(),
            tokio_handle,
            enable_query_covering,
            flush_observed: std::cell::Cell::new(false),
            _engine_census: crate::live_count::Guard::new(&crate::live_count::SYNC_ENGINE),
            self_handle: None,
            ccm,
            mutagen,
            pusher,
            shard: config.shard,
            replica_version,
            cvr_pg: false,
            tables,
            full_tables,
            replica_path,
            app_id,
            permissions,
            permissions_hash,
            next_auth_maintenance_at: None,
            background_retransform_failure: None,
            forced_retransform_outcomes: std::collections::VecDeque::new(),
            pipelines_synced: false,
            #[cfg(test)]
            hydrate_unchanged_runs: 0,
            #[cfg(test)]
            validate_connection_runs: 0,
            #[cfg(test)]
            config_pass_runs: 0,
            #[cfg(test)]
            reset_pass_contexts: Vec::new(),
            #[cfg(test)]
            forced_flush_outcomes: std::cell::RefCell::new(std::collections::VecDeque::new()),
            #[cfg(test)]
            existing_rows_calls: std::cell::Cell::new(0),
            pinned_user_id: None,
            cvr: None,
            e2e_serving_lag:
                crate::services::view_syncer::e2e_serving_lag::E2EServingLagTracker::new(),
            ttl_clock: 0,
            ttl_clock_base: created_at,
            ttl_clock_interval: None,
            expired_queries_timer: None,
            last_connect_time: created_at,
            keepalive_until: created_at + CG_KEEPALIVE_MS,
            connections: HashMap::new(),
            registered_ws: HashMap::new(),
            client_base_versions: HashMap::new(),
            open_ws_ids: HashSet::new(),
            active_client_pv: HashMap::new(),
            client_push_headers: HashMap::new(),
            client_profile_ids: HashMap::new(),
            admin_password,
            server_version,
            metrics,
            inspector_authenticated: false,
            inspector_delegate: std::cell::RefCell::new(
                crate::server::inspector_delegate::InspectorDelegate::new(),
            ),
            auth_validator,
            global_connections,
            connection_count,
            accepting,
            terminal: initialization_failed,
            created_at_ms: created_at,
            served_version: None,
            last_row_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            // Replaced by the process-wide registry in `cg_event_loop`; a
            // standalone default keeps the test constructor self-contained.
            serving_lag_registry: Arc::new(crate::workers::syncer::ServingLagRegistry::new()),
            _census: crate::live_count::Guard::new(&crate::live_count::CLIENT_GROUP),
        };
        // Former `sync_engine.set_cvr_store(...)`, now on the service itself
        // (after construction so the method can run against the real fields).
        if let Some(pg) = config.cvr_pg {
            match cvr_pool {
                Some(pool) => match svc.set_cvr_store(pool, pg.schema, pg.cvr_id, pg.task_id) {
                    Ok(()) => svc.cvr_pg = true,
                    Err(e) => {
                        tracing::error!("CG {cg_id}: set_cvr_store failed: {e}");
                        svc.terminal = true;
                    }
                },
                None => {
                    // The factory asked for a CVR store but the hosting executor
                    // has no pool. This is a wiring bug (PG configured but the
                    // router was built without a pool config), not a per-connection
                    // condition — refuse to serve rather than silently run storeless.
                    tracing::error!("CG {cg_id}: CVR store requested but executor has no CVR pool");
                    svc.terminal = true;
                }
            }
        }
        svc
    }

    /// TS `ViewSyncerService.servingLagEligible` (view-syncer.ts:670-675):
    /// `#clients.size > 0 && getBackgroundConnectionContext() !== undefined`.
    fn serving_lag_eligible(&self) -> bool {
        !self.clients.is_empty()
            && lock_unpoisoned(&self.ccm)
                .get_background_connection_context()
                .is_some()
    }

    /// TS `ViewSyncerService.queryCount`: `#pipelines.initialized() ?
    /// #pipelines.queries().size : 0`. The count of active (hydrated) queries.
    fn query_count(&mut self) -> usize {
        self.pipelines().active_query_ids().len()
    }

    /// TS `ViewSyncerService.rowCount`: `#cvrStore.rowCount`. The tracked-row
    /// count as last observed while the CG held the row map (no CVR I/O here).
    fn row_count(&self) -> usize {
        self.last_row_count.load(Ordering::Relaxed)
    }

    /// Publish (or refresh) this CG's snapshot into the shared serving-lag
    /// registry (TS's per-scrape iteration over `viewSyncers.getServices()`).
    fn publish_serving_lag(&mut self) {
        let num_queries = self.query_count();
        let num_rows = self.row_count();
        let snapshot = crate::workers::syncer::CgServingSnapshot {
            lag: crate::workers::syncer::ServingLagViewSyncer {
                created_at_ms: self.created_at_ms,
                served_version: self.served_version.clone(),
                serving_lag_eligible: self.serving_lag_eligible(),
            },
            num_queries,
            num_rows,
        };
        self.serving_lag_registry
            .upsert_view_syncer(&self.cg_id, snapshot);
    }

    /// Advance and return the monotonic TTL clock to wall-time `now`. Port of
    /// TS `#getTTLClock`: `ttlClock += now - base; base = now`.
    fn get_ttl_clock(&mut self, now: i64) -> TTLClock {
        let delta = now - self.ttl_clock_base;
        if delta > 0 {
            self.ttl_clock += delta;
        }
        self.ttl_clock_base = now;
        self.ttl_clock
    }

    /// The live custom-query fetch context for a connection, read from the
    /// ConnectionContextManager (the single owner of url/headers/auth/userID) at
    /// use time. Mirrors TS, where the transformer reads
    /// `mustGetConnectionContext(selector)` and composes the fetch from
    /// `connection.queryContext` fresh on every transform (transform-query.ts).
    /// `None` when the connection is gone or has no resolved query URL.
    fn query_context_for(&self, client_id: &str, ws_id: &str) -> Option<CustomQueryContext> {
        lock_unpoisoned(&self.ccm)
            .must_get_connection_context(&CcmConnectionSelector {
                client_id: client_id.to_string(),
                ws_id: ws_id.to_string(),
            })
            .ok()
            .as_ref()
            .and_then(custom_query_context_from)
    }

    /// Port of TS `#startTTLClockInterval` (view-syncer.ts:1091-1097): (re)arm
    /// the periodic ttlClock persistence tick. Called after every material CVR
    /// flush (`if (flushed)`, view-syncer.ts:1083-1086) and by the tick itself,
    /// so the interval self-perpetuates once the first flush starts it.
    fn start_ttl_clock_interval(&mut self) {
        self.stop_ttl_clock_interval();
        self.ttl_clock_interval = Some(now_ms() + TTL_CLOCK_INTERVAL);
    }

    /// Port of TS `#stopTTLClockInterval` (view-syncer.ts:1099-1102).
    fn stop_ttl_clock_interval(&mut self) {
        self.ttl_clock_interval = None;
    }

    /// The delay until the next ttlClock persistence tick, or `None` when the
    /// interval is not running. Rust-only adapter: the CG event loop
    /// multiplexes deadlines, so it needs the remaining delay rather than a
    /// timer callback.
    fn next_ttl_clock_delay(&self) -> Option<Duration> {
        let deadline = self.ttl_clock_interval?;
        Some(Duration::from_millis((deadline - now_ms()).max(0) as u64))
    }

    /// Port of TS `#updateTTLClockInCVRWithoutLock` (view-syncer.ts:1104-1119):
    /// advance the in-memory ttlClock and persist it (with `lastActive = now`)
    /// via `CVRStore.updateTTLClock`, outside any flush/lock. Fire-and-forget —
    /// the store call is offloaded and failures are logged, exactly the TS
    /// `.catch` (a missed tick self-heals on the next one).
    fn update_ttl_clock_in_cvr_without_lock(&mut self) {
        // TS guards call sites on `#ttlClock !== undefined`; here the clock is
        // seeded when the CVR loads, so a loaded CVR is the equivalent guard.
        if self.cvr.is_none() {
            return;
        }
        let start = now_ms();
        let ttl_clock = self.get_ttl_clock(start);
        self.update_ttl_clock(ttl_clock, start);
    }

    /// Ensure the group CVR is loaded (from the store) or, when `allow_create`,
    /// freshly created. Seeds the TTL clock from the CVR's stored value on the
    /// load/create transition (TS `#ttlClock = cvr.ttlClock; #ttlClockBase =
    /// now`). Returns whether a CVR is now available.
    async fn ensure_cvr(&mut self, allow_create: bool) -> Result<bool, LoadCvrError> {
        if self.cvr.is_some() {
            return Ok(true);
        }
        if self.cvr_pg {
            // The test seam injects the store's verdict as the LOAD RESULT, so
            // the failure arm below (its log line included) is the one exercised.
            #[cfg(test)]
            let loaded = match force_load_error_take() {
                Some(error) => Err(LoadCvrError::Store(error)),
                None => self.load_cvr(self.last_connect_time as f64).await,
            };
            #[cfg(not(test))]
            let loaded = self.load_cvr(self.last_connect_time as f64).await;
            match loaded {
                Ok(cvr) => self.cvr = cvr,
                Err(e) => {
                    // TS logs nothing at the load site: the one line for a load
                    // failure is `sendError`'s, at the thrown error's level
                    // (connection.ts:428 — `warn` for ClientNotFound, cvr-store.ts:
                    // 1362), which `Connection::send_error` emits. Rust-only
                    // diagnostic, kept below INFO so it never masquerades as a
                    // second, higher-severity event.
                    tracing::debug!("CG {}: load_cvr failed: {e}", self.cg_id);
                    return Err(e);
                }
            }
        } else if self.cvr.is_none() && allow_create {
            self.cvr = Some(empty_cvr(&self.cg_id, &self.replica_version));
        }
        match &self.cvr {
            Some(cvr) => {
                if let Some(message) = older_replica_error(cvr, &self.replica_version) {
                    // TS fails the client with a ClientNotFound carrying this
                    // exact message (view-syncer.pg.test.ts "sends reset for CVR
                    // from older replica version up"), NOT a generic Rehome — the
                    // client must wipe local state and re-sync fresh, not just
                    // reconnect elsewhere. Fail the group with that error here; the
                    // caller's generic `fail_group` is then a no-op (terminal set).
                    // TS view-syncer.ts:564 `lc.info?.(`resetting CVR: ${message}`)`.
                    tracing::info!(cg_id = %self.cg_id, "resetting CVR: {message}");
                    self.cvr = None;
                    self.fail_group_with_error(
                        crate::protocol::ErrorBody::client_not_found(message),
                        None,
                    );
                    return Ok(false);
                }
                self.ttl_clock = cvr.ttl_clock;
                self.ttl_clock_base = now_ms();
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Port of TS `#scheduleExpireEviction` (view-syncer.ts:1394-1432). Arms the
    /// eviction timer for the earliest inactive-query expiry: stop the existing
    /// timer, then — if any inactive query has a TTL — arm `#expiredQueriesTimer`
    /// at the collapse-windowed delay `clamp(next - ttlClock + hysteresis,
    /// hysteresis, MAX)`. Takes the CVR by ref to mirror TS
    /// `#scheduleExpireEviction(lc, cvr)`; the delay is relative to the live
    /// ttlClock, which TS reads off the freshly-synced `cvr.ttlClock` and which
    /// `self.ttl_clock` holds here (both are the same monotonic clock value at
    /// scheduling time).
    fn schedule_expire_eviction(&mut self, cvr: &CVR) {
        self.stop_expire_timer();
        // First see if there is any inactive query with a ttl (TS `nextEvictionTime`).
        let Some(next) = rust_cvr::cvr::next_eviction_time(cvr) else {
            // No inactive queries with a ttl; leave the timer stopped.
            return;
        };
        let raw = (next - self.ttl_clock) + TTL_TIMER_HYSTERESIS_MS;
        let delay = raw.clamp(TTL_TIMER_HYSTERESIS_MS, MAX_TTL_MS);
        self.expired_queries_timer = Some(now_ms() + delay);
    }

    /// Port of TS `#stopExpireTimer` (view-syncer.ts:773-777). Clears the
    /// eviction timer; no eviction runs until `schedule_expire_eviction` re-arms
    /// it. The last-client-disconnect branch calls this (TS view-syncer.ts:767)
    /// so an idle group with no connected clients performs zero eviction work.
    fn stop_expire_timer(&mut self) {
        self.expired_queries_timer = None;
    }

    /// The delay until the armed eviction timer fires, or `None` when it is
    /// stopped (TS `#expiredQueriesTimer === 0`). Rust-only adapter: the CG
    /// event loop multiplexes deadlines, so it needs the remaining delay rather
    /// than a timer callback.
    fn next_expiry_delay(&self) -> Option<Duration> {
        let deadline = self.expired_queries_timer?;
        Some(Duration::from_millis((deadline - now_ms()).max(0) as u64))
    }

    /// Fired when the eviction timer elapses: remove any now-expired queries and
    /// poke their removals. Port of TS `#removeExpiredQueries`.
    async fn on_expiry_tick(&mut self) {
        let Some(cvr) = self.cvr.take() else {
            return;
        };
        let now = now_ms();
        let ttl_clock = self.get_ttl_clock(now);
        // TS `#removeExpiredQueries` syncs the pipeline set ONLY when pipelines
        // are synced: `if (this.#pipelinesSynced) { await
        // this.#syncQueryPipelineSet(lc, cvr, 'missing', undefined); }`
        // (view-syncer.ts:642-645). Before pipelines are initialized there is no
        // pipeline to remove a query FROM; the timer is still rescheduled below
        // (TS reschedules outside the `hasExpiredQueries` branch), so the expiry
        // simply lands on the next tick after init.
        if !self.pipelines_synced {
            self.schedule_expire_eviction(&cvr);
            self.cvr = Some(cvr);
            return;
        }
        let client_ids: Vec<String> = self.registered_ws.values().cloned().collect();
        // No eager row-map read here. TS's expiry pass reaches the row records
        // only through `#lookupRowsForExecutedAndRemovedQueries`, which skips the
        // read entirely unless queries were executed or removed (cvr.ts:661-667);
        // `hydrate_and_sync` now performs that read behind the same condition.
        // `#rowCount` is maintained by the store during flush (cvr-store.ts:1068),
        // not recomputed at the top of every pass.
        match self
            .remove_expired_queries(cvr, &client_ids, self.last_connect_time, now, ttl_clock)
            .await
        {
            Ok((cvr, n)) => {
                if n > 0 {
                    tracing::debug!("CG {}: expired {n} queries", self.cg_id);
                    crate::metrics::Metrics::add(&self.metrics.expired_queries, n as u64);
                }
                // Expiry runs through the same query-sync path TS marks served
                // at the end of (`#syncQueryPipelineSet`).
                self.mark_version_served(&cvr.version);
                // TS `#removeExpiredQueries` reschedules the eviction timer for
                // the next inactive query at its tail (view-syncer.ts:651).
                self.schedule_expire_eviction(&cvr);
                self.cvr = Some(cvr);
            }
            Err(e) => {
                // A failed expiry pass is NOT recoverable by continuing: the
                // engine has already torn down the expired queries' pipelines
                // (remove_query runs before the flush), so warning-and-carrying-
                // on leaves the engine and the (reloaded-from-PG) CVR disagreeing
                // about which queries run — rows for those queries silently stop
                // syncing. Treat it like every other sync path: fail the group;
                // clients rehome and the next owner reloads a consistent pair.
                tracing::error!("CG {}: remove_expired_queries failed: {e}", self.cg_id);
                self.fail_group(&e.to_string());
            }
        }
    }

    /// Port of TS `#checkForThrashing` (view-syncer.ts:2121-2148): sliding
    /// 60s window per query — a query whose transformation hash is replaced
    /// ≥3 times inside the window warns (it usually means clients with
    /// DIFFERENT auth contexts share one client group, each re-transform
    /// tearing down the other's pipeline). Warn-only, like TS.
    fn check_for_thrashing(&mut self, query_id: &str) {
        const THRASH_WINDOW_MS: i64 = 60_000; // TS: 60 seconds
        const THRASH_THRESHOLD: u32 = 3;
        let now = now_ms();

        match self.query_replacements.get_mut(query_id) {
            None => {
                self.query_replacements.insert(
                    query_id.to_string(),
                    QueryReplacementRecord {
                        count: 1,
                        window_start: now,
                    },
                );
            }
            // TS: outside the window → delete the old entry, start a fresh one.
            Some(record) if now - record.window_start > THRASH_WINDOW_MS => {
                record.count = 1;
                record.window_start = now;
            }
            Some(record) => {
                record.count += 1;
                if record.count >= THRASH_THRESHOLD {
                    tracing::warn!(
                        "Query thrashing detected for query {query_id}. {} replacements in 60s. \
                         This may indicate clients with different auth contexts connecting to \
                         the same client group.",
                        record.count
                    );
                }
            }
        }
    }

    /// Recompute the group auth-maintenance deadline from the ported planner.
    /// Port of TS `#scheduleAuthMaintenance` (view-syncer.ts:793): stop the old
    /// timer, ask `planMaintenance()` for `earliestDeadlineAt` (per-connection
    /// `revalidate_at` + the group retransform deadline, with deferral backoff
    /// applied by the CCM), and arm — or disarm when the plan reports no
    /// deadline ("No auth maintenance wakeup scheduled").
    fn schedule_auth_maintenance(&mut self) {
        self.next_auth_maintenance_at = lock_unpoisoned(&self.ccm)
            .plan_maintenance()
            .earliest_deadline_at;
    }

    /// The delay until the next auth-maintenance tick, or `None` if none is
    /// armed. Mirrors `next_expiry_delay` so the CG loop can wake for whichever
    /// deadline comes first.
    fn next_auth_maintenance_delay(&self) -> Option<Duration> {
        let deadline = self.next_auth_maintenance_at?;
        let delay = (deadline - now_ms()).max(0);
        Some(Duration::from_millis(delay as u64))
    }

    /// Periodic auth maintenance: re-validate each live connection's JWT and, for
    /// the survivors, re-transform their queries. Port of TS
    /// `#runAuthMaintenance` (`planMaintenance` → `dueRevalidations` +
    /// `dueRetransform`):
    ///
    ///  - Revalidation (security-critical, always run): a token that has since
    ///    expired or been revoked now fails `validate_auth`, so the connection is
    ///    closed — a live socket cannot outlive its credential. A still-valid
    ///    token is a no-op for that connection.
    ///  - Retransform: after revalidation, re-run each surviving client's
    ///    config/hydrate pass (`changed = true`) so read-permission / server-side
    ///    authorization drift is picked up (custom queries are re-fetched with
    ///    the current Bearer token). This folds TS's separate `retransform`
    ///    interval into the same tick; both default to 300s so the observable
    ///    cadence is identical.
    ///
    /// Re-arms the deadline (or clears it if no authed connection remains).
    async fn run_auth_maintenance(&mut self) {
        // Plan from the ported CCM (TS `#runAuthMaintenance`, view-syncer.ts:825:
        // `planMaintenance()` → `dueRevalidations` + `dueRetransform`). The CCM
        // owns the deadlines, the deferral backoff, and the background-connection
        // choice; this loop only executes the plan.
        let plan = lock_unpoisoned(&self.ccm).plan_maintenance();
        if plan.due_revalidations.is_empty() && !plan.due_retransform {
            tracing::debug!(
                "CG {}: auth maintenance woke up with no due work",
                self.cg_id
            );
            self.schedule_auth_maintenance();
            return;
        }

        let mut survivors: Vec<String> = Vec::new();
        for due_ctx in &plan.due_revalidations {
            let client_id = due_ctx.client_id.clone();
            // The connection may have closed (or been replaced by a new wsID)
            // since the plan snapshot.
            if self.registered_ws.get(&client_id) != Some(&due_ctx.ws_id) {
                continue;
            }
            let selector = CcmConnectionSelector {
                client_id: client_id.clone(),
                ws_id: due_ctx.ws_id.clone(),
            };
            // An untokened (cookie/anonymous) connection has no JWT to locally
            // re-verify — it goes straight to the server-side probe below, and
            // its validation is recorded as the TS `client-fallback` kind.
            let Some(token) = due_ctx.auth.as_ref().map(|a| a.raw().to_string()) else {
                survivors.push(client_id);
                continue;
            };
            // Bind the subject to the group's PINNED user (not the token's own
            // `sub`, which would be a tautological `sub == sub` check). This is
            // what makes revalidation reject a token that has been swapped for a
            // DIFFERENT user's — as well as one that has since expired/revoked.
            // Falls back to the token's `sub` only for an unpinned (anonymous)
            // group.
            let expected_sub = self.pinned_user_id.clone().or_else(|| {
                crate::auth::jwt::decode_jwt_claims(&token)
                    .get("sub")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            });
            let verify = self
                .auth_validator
                .validate_auth(
                    &self.cg_id,
                    &client_id,
                    expected_sub.as_deref(),
                    Some(&token),
                )
                .await;
            match verify {
                Err(error_body) => {
                    tracing::info!(
                        "CG {}: periodic revalidation failed for client {client_id}; closing",
                        self.cg_id
                    );
                    crate::metrics::Metrics::inc(&self.metrics.auth_revalidation_failures);
                    // TS `#failMaintenanceConnection`: record the failure in the
                    // CCM (drops the context, revision-guarded) BEFORE failing
                    // the socket.
                    lock_unpoisoned(&self.ccm).fail_connection(&selector, due_ctx.revision);
                    if let Some(conn) = self.connections.get(&client_id) {
                        conn.fail(error_body, None);
                    }
                    if let Some(ws_id) = self.registered_ws.get(&client_id).cloned() {
                        self.delete_client_due_to_disconnect(&client_id, &ws_id);
                    }
                }
                Ok(()) => survivors.push(client_id),
            }
        }

        crate::metrics::Metrics::inc(&self.metrics.auth_revalidations);

        // Probe + record each surviving due connection (TS `#validateConnection`
        // per `dueRevalidations` entry).
        // TS iterates the plan's `ConnectionContext`s directly; keep the real
        // contexts (not a fabricated triple) so `validate_connection` sees the
        // same `revision` TS's `#validateConnection` reads off `connCtx`.
        let plan_by_client: std::collections::HashMap<String, CcmConnectionContext> = plan
            .due_revalidations
            .iter()
            .map(|c| (c.client_id.clone(), c.clone()))
            .collect();
        for client_id in survivors {
            let Some(conn_ctx) = plan_by_client.get(&client_id).cloned() else {
                continue;
            };
            let ws_id = conn_ctx.ws_id.clone();
            if self.registered_ws.get(&client_id) != Some(&ws_id) {
                continue;
            }
            // TS: `for (const connCtx of plan.dueRevalidations) { try {
            //        await this.#validateConnection(connCtx); } catch (e) { ... } }`
            // (view-syncer.ts:836-856). The revocation probe, the auth-error
            // close and the CCM record all live inside `validate_connection`;
            // this loop handles only what TS's `catch` handles — a
            // TransformFailed (API down / 5xx) DEFERS the remaining maintenance
            // and returns, never closing the connection on a blip.
            match self.validate_connection(&conn_ctx).await {
                Ok(true) => {}
                // Auth error: `validate_connection` already failed + closed it.
                Ok(false) => continue,
                Err(_body) => {
                    // Exact TS wording (view-syncer.ts:841-848): the message
                    // string mirrors TS verbatim; cg/client go in structured
                    // fields the way TS passes `{clientID, wsID, message}`.
                    tracing::warn!(
                        cg_id = %self.cg_id,
                        client_id = %client_id,
                        "Scheduled auth revalidation failed; deferring auth maintenance"
                    );
                    lock_unpoisoned(&self.ccm).defer_maintenance(MaintenanceKind::Revalidate);
                    self.schedule_auth_maintenance();
                    return;
                }
            }
        }

        // Revalidation can change which connection is safe for shared background
        // work — replan before deciding on the group retransform (TS
        // `refreshedPlan`, view-syncer.ts:858). ONE retransform for the group on
        // the background connection's context (TS `#runBackgroundRetransform`),
        // not one per survivor: `handle_desired_queries(_, {}, UpdateAuth)`
        // re-runs the whole CG's config/hydrate pass, so a single call already
        // re-fetches every query with current auth/permissions.
        let refreshed = lock_unpoisoned(&self.ccm).plan_maintenance();
        if refreshed.due_retransform {
            self.run_background_retransform().await;
        }

        // Re-arm from the refreshed plan (TS `#scheduleAuthMaintenance` in the
        // locked-op `finally`).
        self.schedule_auth_maintenance();
    }

    /// Validate a connection's credential against the query API server and record
    /// the result on the CCM. Port of TS `ViewSyncerService.#validateConnection`
    /// (view-syncer.ts:2749-2783).
    ///
    /// TS returns `Promise<boolean>` and THROWS on a non-auth failure; the rust
    /// mapping is `Result<bool, Value>`:
    ///   * `Ok(true)`  — validated (TS `return true`).
    ///   * `Ok(false)` — an AUTH error (401/403/AuthInvalidated/Unauthorized)
    ///     invalidated the connection; `fail_maintenance_connection` already
    ///     closed it (TS `catch` → `#failMaintenanceConnection` → `return false`).
    ///   * `Err(body)` — a NON-auth failure (API down / 5xx / malformed). TS
    ///     rethrows here so each CALLER decides: auth maintenance defers
    ///     (view-syncer.ts:839-856), init/updateAuth propagate.
    ///
    /// Rust limitation (NOT TS parity — do not read this as equivalent): TS uses
    /// `response.validation`, which can carry the API server's authoritative
    /// userID; rust's `validate` returns `Ok(())` ("Success is
    /// intentionally opaque"), so a successful probe still records the
    /// `client-fallback` kind. Threading the server userID through is tracked as
    /// an open row in `parity/ZERO-DIVERGENCE-PLAN.md`'s M0 matrix.
    async fn validate_connection(
        &mut self,
        conn_ctx: &CcmConnectionContext,
    ) -> Result<bool, serde_json::Value> {
        #[cfg(test)]
        {
            self.validate_connection_runs += 1;
        }
        let selector = CcmConnectionSelector {
            client_id: conn_ctx.client_id.clone(),
            ws_id: conn_ctx.ws_id.clone(),
        };
        // TS view-syncer.ts:2751-2760: with a transformer the `/query` probe's
        // response carries the API server's validation (`server-validated`
        // with its userID, else `client-fallback`); without one it is
        // `client-fallback`. Rust's transformer twin is the per-connection
        // `CustomQueryContext`; `None` (no `userQueryURL` configured) is TS's
        // no-transformer branch.
        let validation =
            if let Some(ctx) = self.query_context_for(&conn_ctx.client_id, &conn_ctx.ws_id) {
                let shard = self.shard.clone();
                match crate::custom_queries::transform_query::validate(&ctx, &shard).await {
                    Err(body) => {
                        // TS throws `ProtocolErrorWithLevel(response, 'warn')` here and
                        // the catch below splits auth-error from everything else.
                        if crate::custom_queries::transform_query::is_auth_error_body(&body) {
                            // Exact TS wording + structured fields (view-syncer.ts:2769-2777).
                            tracing::warn!(
                                cg_id = %self.cg_id,
                                client_id = %conn_ctx.client_id,
                                ws_id = %conn_ctx.ws_id,
                                revision = conn_ctx.revision,
                                message = %transform_failure_message(&body),
                                "Connection auth validation failed; invalidating connection"
                            );
                            crate::metrics::Metrics::inc(&self.metrics.auth_revalidation_failures);
                            self.fail_maintenance_connection(
                                conn_ctx,
                                crate::protocol::ErrorBody::unauthorized(
                                    "Connection auth validation failed",
                                ),
                            );
                            return Ok(false);
                        }
                        return Err(body);
                    }
                    // TS: `validation = response.validation` (view-syncer.ts:2757).
                    Ok(validation) => validation,
                }
            } else {
                ConnectionValidation::ClientFallback
            };

        // TS view-syncer.ts:2762-2767: `validateConnection` THROWS when the
        // API server's userID differs from the connection's (`Unauthorized`,
        // connection-context-manager.ts:420) — that lands in the same catch
        // as a probe failure: auth error → warn + `#failMaintenanceConnection`
        // + `false`; anything else propagates.
        // Bind first: the guard temporary must drop before `self` is borrowed mutably.
        let recorded = lock_unpoisoned(&self.ccm).validate_connection(
            &selector,
            conn_ctx.revision,
            &validation,
        );
        if let Err(e) = recorded {
            let body = e.to_error_body();
            let body_value = serde_json::to_value(&body).unwrap_or_default();
            if crate::custom_queries::transform_query::is_auth_error_body(&body_value) {
                tracing::warn!(
                    cg_id = %self.cg_id,
                    client_id = %conn_ctx.client_id,
                    ws_id = %conn_ctx.ws_id,
                    revision = conn_ctx.revision,
                    message = %body.message(),
                    "Connection auth validation failed; invalidating connection"
                );
                crate::metrics::Metrics::inc(&self.metrics.auth_revalidation_failures);
                self.fail_maintenance_connection(conn_ctx, body);
                return Ok(false);
            }
            return Err(body_value);
        }
        Ok(true)
    }

    /// Fail an auth-maintenance connection: drop it from the CCM (revision-
    /// guarded), then, if it is still the client's live socket, close that socket
    /// with `error`. Port of TS `#failMaintenanceConnection` (view-syncer.ts:2786):
    /// `failConnection` returns falsy when the context was already gone/replaced,
    /// in which case TS returns WITHOUT failing the socket (`if (!failed) return`)
    /// — the rust `fail_connection` returns `None` in exactly that case. The
    /// `client?.wsID === wsID` guard is the `registered_ws` check below.
    ///
    /// (The two periodic-revalidation close sites above inline the same shape
    /// against `fail_connection`; they predate this method and are left as-is to
    /// keep this fix scoped to the background-retransform path.)
    /// The inputs `sync_query_pipeline_set` needs beyond the CVR, resolved the
    /// way the config pass resolves them (`handle_desired_queries`): the
    /// deployed permissions, the connection's JWT claims, its custom-query
    /// context, the pipelines' current state version and the replica version.
    /// `conn` is TS's `connCtx` argument; `None` is TS
    /// `mustGetBackgroundConnectionContext()` (view-syncer.ts:1906-1908).
    fn sync_query_pipeline_set_inputs(
        &mut self,
        cvr: &CVR,
        conn: Option<(&str, &str)>,
    ) -> (
        Option<serde_json::Value>,
        serde_json::Value,
        Option<CustomQueryContext>,
        String,
        String,
    ) {
        let ctx = {
            let ccm = lock_unpoisoned(&self.ccm);
            match conn {
                Some((client_id, ws_id)) => ccm
                    .must_get_connection_context(&CcmConnectionSelector {
                        client_id: client_id.to_string(),
                        ws_id: ws_id.to_string(),
                    })
                    .ok(),
                None => ccm.get_background_connection_context(),
            }
        };
        let auth_data = ctx
            .as_ref()
            .and_then(|c| c.auth.as_ref())
            .map(|a| crate::auth::jwt::decode_jwt_claims(a.raw()))
            .unwrap_or_else(|| serde_json::json!({}));
        let custom_ctx = ctx.as_ref().and_then(custom_query_context_from);
        let state_version = self
            .pipelines()
            .current_version()
            .unwrap_or_else(|| cvr.version.state_version.clone());
        (
            self.permissions.clone(),
            auth_data,
            custom_ctx,
            state_version,
            self.replica_version.clone(),
        )
    }

    fn fail_maintenance_connection(
        &mut self,
        conn_ctx: &CcmConnectionContext,
        error: crate::protocol::ErrorBody,
    ) {
        let selector = CcmConnectionSelector {
            client_id: conn_ctx.client_id.clone(),
            ws_id: conn_ctx.ws_id.clone(),
        };
        // TS `const failed = failConnection(connCtx, revision); if (!failed) return;`
        if lock_unpoisoned(&self.ccm)
            .fail_connection(&selector, conn_ctx.revision)
            .is_none()
        {
            return;
        }
        // TS `if (client?.wsID === wsID) client.fail(wrapped)` — only fail the
        // socket that is still the client's current one.
        if self.registered_ws.get(conn_ctx.client_id.as_str()) == Some(&conn_ctx.ws_id) {
            if let Some(conn) = self.connections.get(conn_ctx.client_id.as_str()) {
                conn.fail(error, None);
            }
            self.delete_client_due_to_disconnect(&conn_ctx.client_id, &conn_ctx.ws_id);
        }
    }

    /// Re-run the group's query pipelines under a background connection's auth and
    /// report the outcome. Port of the inner `attemptRetransform` closure of TS
    /// `#runBackgroundRetransform` (view-syncer.ts:2669): `#syncQueryPipelineSet('all')`
    /// then `markBackgroundRetransformSuccess`. Here the re-hydrate is
    /// `handle_desired_queries(_, {}, BackgroundRetransform)` (the whole-CG config/hydrate
    /// pass); its whole-batch transform failure — if any — is captured into
    /// `background_retransform_failure` and classified, so the caller can act like
    /// TS's `try/catch` (mark success vs. auth-fail vs. defer). Marking success is
    /// left to the caller (TS marks inside the closure only when it did not throw).
    async fn attempt_background_retransform(
        &mut self,
        bg: &CcmConnectionContext,
    ) -> RetransformOutcome {
        // Test seam: exercise `run_background_retransform`'s control flow without a
        // live query-API round trip. Empty in production.
        if let Some(forced) = self.forced_retransform_outcomes.pop_front() {
            return forced;
        }
        // Reset before the re-hydrate so a whole-batch failure recorded on an
        // earlier init/changeDesiredQueries pass cannot be misread as this
        // retransform's outcome.
        self.background_retransform_failure = None;
        let empty_body = serde_json::json!({});
        // TS `#runBackgroundRetransform` -> `#syncQueryPipelineSet(..., 'all', connCtx)`
        // (view-syncer.ts:2673): the whole point is to re-authorize every custom
        // query against the refreshed credential.
        self.handle_desired_queries(
            &bg.client_id,
            &empty_body,
            ConfigPassOrigin::BackgroundRetransform,
            CustomQueryTransformMode::All,
        )
        .await;
        classify_retransform_failure(self.background_retransform_failure.take())
    }

    /// Run ONE shared background retransform for the client group under the
    /// selected background connection's auth. Port of TS `#runBackgroundRetransform`
    /// (view-syncer.ts:2668):
    ///  - no selected connection → skip (unschedulable until one exists);
    ///  - loop: attempt under the current bg connection;
    ///    - success → `markBackgroundRetransformSuccess` + return;
    ///    - auth error → WARN + `#failMaintenanceConnection` + retry with the
    ///      replacement connection (or return when none remains);
    ///    - transform-failed (transient) → WARN + `deferMaintenance('retransform')`
    ///      + return.
    ///
    /// Divergence from TS, labeled: TS bare-returns on "no selected connection" /
    /// "no replacement" and relies on its deadline getter (which omits the absent
    /// retransform) to avoid a hot re-arm. Rust's `plan_maintenance` keeps
    /// `retransform_at` set until a mark/defer moves it, so a bare return here
    /// would re-arm at delay 0 and spin; rust therefore `defer_maintenance` on
    /// those exits (the prior inline code did the same). Client-observable
    /// behavior is unchanged — both eventually retry under a valid credential.
    async fn run_background_retransform(&mut self) {
        let mut bg = match lock_unpoisoned(&self.ccm).get_background_connection_context() {
            Some(c) => c,
            None => {
                tracing::debug!(
                    "CG {}: Skipping background retransform with no selected connection",
                    self.cg_id
                );
                return;
            }
        };

        loop {
            // rust guard: `handle_desired_queries` needs a registered ws; a bg
            // context whose ws is no longer registered cannot be retransformed.
            // Treat it as unschedulable → defer (see the method-level note).
            if self.registered_ws.get(bg.client_id.as_str()) != Some(&bg.ws_id) {
                lock_unpoisoned(&self.ccm).defer_maintenance(MaintenanceKind::Retransform);
                return;
            }

            match self.attempt_background_retransform(&bg).await {
                RetransformOutcome::Success => {
                    lock_unpoisoned(&self.ccm).mark_background_retransform_success(
                        &CcmConnectionSelector {
                            client_id: bg.client_id.clone(),
                            ws_id: bg.ws_id.clone(),
                        },
                        bg.revision,
                    );
                    return;
                }
                RetransformOutcome::AuthError(body) => {
                    // TS view-syncer.ts:2702-2708 (`{clientID, message: e.message}`).
                    tracing::warn!(
                        cg_id = %self.cg_id,
                        client_id = %bg.client_id,
                        message = %transform_failure_message(&body),
                        "Background retransform auth failed; failing connection and searching for replacement"
                    );
                    self.fail_maintenance_connection(
                        &bg,
                        crate::protocol::ErrorBody::unauthorized(
                            "Connection auth validation failed",
                        ),
                    );
                }
                RetransformOutcome::TransformFailed(body) => {
                    // TS view-syncer.ts:2711-2717 (`{clientID, message: e.message}`).
                    tracing::warn!(
                        cg_id = %self.cg_id,
                        client_id = %bg.client_id,
                        message = %transform_failure_message(&body),
                        "Background retransform failed; deferring auth maintenance"
                    );
                    lock_unpoisoned(&self.ccm).defer_maintenance(MaintenanceKind::Retransform);
                    return;
                }
            }

            // TS `getBackgroundConnectionContext()` after a failed connection: the
            // CCM re-selected the newest remaining validated connection (or None).
            match lock_unpoisoned(&self.ccm).get_background_connection_context() {
                Some(replacement) => {
                    tracing::debug!(
                        "CG {}: Retrying background retransform with replacement connection",
                        self.cg_id
                    );
                    bg = replacement;
                }
                None => {
                    tracing::debug!(
                        "CG {}: No replacement connection available for background retransform",
                        self.cg_id
                    );
                    // rust-scheduler defer (see the method-level note): no bg left.
                    lock_unpoisoned(&self.ccm).defer_maintenance(MaintenanceKind::Retransform);
                    return;
                }
            }
        }
    }

    /// Returns the piggybacked `initConnection` message (sec-websocket-protocol
    /// header), if any, for the caller to dispatch through the normal inbound
    /// path — TS `Connection.init()` routes it through `#handleMessage` like
    /// any frame. Dispatching it here would hold this method's `&mut self`
    /// borrow across the handler.
    async fn on_new_connection(
        &mut self,
        params: ConnectParams,
        sink: DirectWebSocketSink,
    ) -> Option<(Arc<str>, Arc<str>, String)> {
        crate::trace::note!(
            "conn-open",
            "cg={} client={} ws={}",
            self.cg_id,
            params.client_id,
            params.ws_id
        );
        // TS `keepalive()` at socket accept (syncer.ts:370 → view-syncer.ts
        // :718-724): `#keepAliveUntil = Date.now() + keepaliveMs`. `#lastConnectTime`
        // is NOT set here — TS sets it in `#runInLockForClient` when the
        // initConnection message arrives (:1194-1196); see `run_in_lock_for_client`.
        self.keepalive_until = now_ms() + CG_KEEPALIVE_MS;
        let client_id = params.client_id.clone();
        let ws_id = params.ws_id.clone();
        let protocol_version = params.protocol_version;
        let client_group_id = params.client_group_id.clone();

        // Port of syncer.ts:643-650 (`handleConnection`): a clientID that is
        // already connected has its EXISTING socket closed frame-less —
        // `existing.close(`replaced by ${params.wsID}`)` → `Connection.close`
        // → ws close, no error frame. In production the router already sent
        // `CGMessage::CloseConnection` for it (`close_connection`); this is the
        // same close for a connection that reached the CG thread without one.
        if let Some(prev_ws_id) = self.registered_ws.get(&client_id).cloned()
            && prev_ws_id != ws_id
        {
            tracing::debug!("client {client_id} already connected, closing existing connection");
            if let Some(conn) = self.connections.get(&client_id) {
                conn.close(&format!("replaced by {ws_id}"));
            }
            self.unregister_client(&prev_ws_id);
            self.decrement_active_client(&prev_ws_id);
        }

        // NOT registered as a poke target here: TS creates the `ClientHandler`
        // and puts it in `#clients` when the initConnection MESSAGE arrives
        // (`initConnection`, view-syncer.ts:903-914), and `#activeClients` moves
        // there too (:888-890) — both ported in `init_connection`. Between accept
        // and that message the socket is a connection (CCM-registered below) but
        // not a client: no pokes, no deleteClients acks, and its other messages
        // are dropped by `run_in_lock_for_client`.
        self.open_ws_ids.insert(ws_id.clone());
        // TS's dispatcher tracks the highest protocol version any client has
        // connected with (`zero.sync.max-protocol-version`,
        // server/worker-dispatcher.ts:56-64). Rust has no worker_dispatcher
        // twin, so it is fed from the same connect point that already moves the
        // active-clients gauge.
        crate::metrics::record_client_protocol_version(params.protocol_version);
        // Connection fully initialized (TS `recordConnectionSuccessMetric`).
        crate::metrics::record_ws_connection_success(params.protocol_version);
        self.registered_ws.insert(client_id.clone(), ws_id.clone());
        self.client_base_versions.insert(
            client_id.clone(),
            // base_cookie is client-supplied: a malformed one must not panic the
            // CG task (which hosts EVERY client of the group). Treat it as no
            // base version — the SAME fallback `ClientHandler::new` applies, so
            // the version this map validates matches the version the poker uses.
            params.base_cookie.as_deref().and_then(|c| {
                match rust_cvr::schema::types::maybe_version_string(c) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        // DEBUG, not warn: TS parses the cookie only at the
                        // initConnection message (`new ClientHandler`,
                        // view-syncer.ts:903-910) and THROWS there; the 1:1
                        // rejection is `init_connection`'s. This accept-time
                        // fallback exists only so a malformed cookie cannot
                        // panic the CG task, so it must not add a rust-only
                        // WARN with no TS twin (pinned by `parity/log_differential.py`).
                        tracing::debug!(
                            "CG {}: ignoring malformed base cookie {c:?}: {e}",
                            self.cg_id
                        );
                        None
                    }
                }
            }),
        );
        self.client_push_headers.remove(&client_id);
        self.client_profile_ids.remove(&client_id);
        // Until profileID is required in the URL, default it to `cg{clientGroupID}`
        // (the value the schema migration writes), exactly as TS does at the
        // initConnection config-update call site (view-syncer.ts:862:
        // `connCtx.profileID ?? \`cg${this.id}\``, where `this.id` is the client
        // group ID). set_profile_id is materiality-guarded, so re-passing this on
        // later config updates is a no-op once the CVR has it.
        let profile_id = params
            .profile_id
            .clone()
            .unwrap_or_else(|| format!("cg{}", self.cg_id));
        self.client_profile_ids
            .insert(client_id.clone(), profile_id);

        // Pin the group's userID on the first connection that carries one.
        // Admission (`check_and_pin_user`) already guarantees every connection
        // reaching this CG shares the same userID, so capturing the first is
        // sufficient. This is the identity `updateAuth`/revalidation enforce the
        // token against, so a validly-signed token for a DIFFERENT user cannot
        // re-scope the group mid-connection. Port of `GroupAuthState.pinnedUser`.
        if self.pinned_user_id.is_none() {
            self.pinned_user_id = params
                .user_id
                .as_deref()
                .filter(|u| !u.is_empty())
                .map(str::to_string);
        }
        // Register the connection into the ConnectionContextManager (TS
        // `registerConnection`) BEFORE arming maintenance, which reads the CCM to
        // decide whether any connection carries auth. Auth is resolved from the
        // connect params: the modern path (no legacy validator) yields
        // `Opaque{raw}` when a token is present and `None` when absent
        // (auth.ts:74-77 / :108-112). The token was already signature-verified at
        // admission (`AuthValidator`), as TS runs the auth validator before the
        // manager.
        {
            let selector = CcmConnectionSelector {
                client_id: params.client_id.clone(),
                ws_id: params.ws_id.clone(),
            };
            let reg = ConnectParamsForRegistration {
                client_id: params.client_id.clone(),
                ws_id: params.ws_id.clone(),
                user_id: params.user_id.clone().filter(|v| !v.is_empty()),
                profile_id: params.profile_id.clone(),
                base_cookie: params.base_cookie.clone(),
                protocol_version: params.protocol_version,
                http_cookie: params.http_cookie.clone(),
                origin: params.origin.clone(),
                request_headers: params
                    .request_headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            };
            let user_id = params.user_id.as_deref().filter(|v| !v.is_empty());
            let wire = params.auth.as_deref().filter(|t| !t.is_empty());
            let auth = resolve_auth(None, user_id, wire, None).unwrap_or(None);
            let mut ccm = lock_unpoisoned(&self.ccm);
            ccm.register_connection(&selector, &reg, auth);
            // NOTE: the connection is registered here but NOT validated. TS
            // validates in `initConnection` — `#validateConnection` at
            // view-syncer.ts:942 — not at socket accept, and that ordering is the
            // point: validation POSTs to the query API server, so doing it here
            // would validate a socket that has not yet asked for anything.
            // `handle_desired_queries(ConfigPassOrigin::InitConnection, ..)`
            // performs it, which is also what records `revalidate_at` and lets
            // the CCM promote a background connection.
        }

        // Recompute the auth-maintenance wakeup now that the CCM has a newly
        // validated connection (TS re-arms via `#scheduleAuthMaintenance` after
        // every locked operation).
        self.schedule_auth_maintenance();

        // Raw auth/header material captured at connect, forwarded on a relayed
        // custom push so the TS endpoint can rebuild the userPushURL request.
        // The TS side applies its own push-config allowlist, so send raw here.
        let mut relay_request_headers: Vec<(String, String)> = params
            .request_headers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        relay_request_headers.sort();
        let push_relay_headers = crate::workers::syncer_ws_message_handler::PushRelayHeaders {
            // Base headers only. `auth` is filled FRESH from the CCM at each relay
            // (handler: `relay_headers_for`; router deleteClients cleanup: read
            // below), so no stale connect-time token is ever forwarded (I-8: the
            // CCM is the single owner; no parallel auth cell).
            auth: None,
            // Filled together with `auth` per relay (relay_headers_for).
            revision: 0,
            cookie: params.http_cookie.clone(),
            origin: params.origin.clone(),
            request_headers: relay_request_headers,
            user_id: params.user_id.clone().filter(|v| !v.is_empty()),
            push_override: Default::default(),
        };
        // Retained per client for the router-side `deleteClients` cleanup relay
        // (the message-handler path keeps its own copy).
        self.client_push_headers
            .insert(client_id.clone(), push_relay_headers.clone());

        // The live dispatch: the handler's `viewSyncer.<method>`
        // calls execute inline on this CG task via the service's own cell (TS
        // `Connection` holds `#viewSyncer`). A scaffold-constructed service has
        // no cell; its adapter no-ops (those tests drive the engine surface
        // directly).
        let cg_view_syncer: Rc<dyn ViewSyncerDispatch> = Rc::new(CgViewSyncer {
            svc: self.self_handle.clone().unwrap_or_default(),
        });
        let handler = Box::new(SyncerWsMessageHandler::new(
            cg_view_syncer,
            // The handler's connection-context reads (mutagen-CRUD auth + relayed
            // push auth) go through the ported CCM — the single owner — not the
            // `auth:None` placeholder (I-8).
            Arc::new(CcmDispatchAdapter::new(self.ccm.clone())),
            self.mutagen.clone(),
            self.pusher.clone(),
            client_group_id.clone(),
            client_id.clone(),
            ws_id.clone(),
            push_relay_headers,
        ));

        let cid = client_id.clone();
        let close_ws_id = ws_id.clone();
        let conns = self.global_connections.clone();
        let on_close = Box::new(move || {
            let mut conns = lock_unpoisoned(&conns);
            if conns
                .get(&cid)
                .is_some_and(|info| info.ws_id == close_ws_id)
            {
                conns.remove(&cid);
            }
        });

        let conn = Connection::new(
            sink,
            protocol_version,
            ws_id.clone(),
            client_id.clone(),
            client_group_id,
            self.shard.app_id.clone(),
            self.shard.shard_num,
            handler,
            on_close,
        );

        // TS `Connection.init()` (connection.ts) does the protocol-version gate +
        // `connected` send on the accept handler (`syncer.ts#handleConnection`).
        // Rust-specific (rule 5): the `Connection` is built HERE on the serial CG
        // thread because its message handler binds the CG-local dispatch services,
        // so `init()` cannot run on the accept task. The two observable effects of
        // `init()` are therefore produced on the accept path instead: the version
        // gate in `accept_connection` (ws_server.rs), and the `connected` frame in
        // `handle_connection` via the 1:1 `connected_message()` builder — emitted
        // BEFORE this CG-thread work so the ack is never queued behind
        // `config_and_hydrate`. No version re-check here: every connection reaching
        // this point already passed `accept_connection`'s gate.
        self.connections.insert(client_id.clone(), Rc::new(conn));

        // Piggybacked initConnection from the sec-websocket-protocol header:
        // hand the raw message back to the caller, which dispatches it through
        // the SAME path as a socket frame (Connection -> SyncerWsMessageHandler
        // -> ViewSyncerDispatch), exactly like TS `Connection.init()` feeding
        // `#handleMessage`.
        params.init_connection_msg.as_ref().and_then(|init_msg| {
            serde_json::to_string(init_msg).ok().map(|text| {
                (
                    Arc::from(client_id.as_str()),
                    Arc::from(ws_id.as_str()),
                    text,
                )
            })
        })
    }

    /// Route a client's `initConnection` / `changeDesiredQueries` body to the
    /// SyncEngine: record desired queries and hydrate. Loads/creates the group
    /// CVR on first use. (Part 2 — functional cut; see `config_and_hydrate`.)
    /// Returns whether the config pass was accepted (TS: the ViewSyncer stream
    /// started) — the handler gates `pusher.initConnection` on it.
    async fn handle_desired_queries(
        &mut self,
        client_id: &str,
        body: &serde_json::Value,
        origin: ConfigPassOrigin,
        // TS passes the mode EXPLICITLY at every `#handleConfigUpdate` call site
        // rather than deriving it (view-syncer.ts:949/981/1023/1043); each rust
        // caller does the same and cites its TS line.
        custom_query_transform_mode: CustomQueryTransformMode,
    ) -> bool {
        let Some(ws_id) = self.registered_ws.get(client_id).cloned() else {
            tracing::warn!(
                "CG {}: desired queries for unregistered client {client_id}",
                self.cg_id
            );
            return false;
        };
        // TS's initConnection-only preamble vs "run the pass anyway"; see
        // `ConfigPassOrigin`.
        let is_init = origin.is_init_connection();
        let force_config_pass = origin.forces_config_pass();
        // TS `initConnection` (view-syncer.ts:864-914): the ClientHandler is
        // created and registered when the initConnection MESSAGE arrives, before
        // the locked body below — never at socket accept.
        if is_init && !self.init_connection(client_id, &ws_id) {
            return false;
        }
        // TS `#runInLockForClient` (view-syncer.ts:1180-1250) wraps
        // initConnection, changeDesiredQueries and updateAuth; the background
        // retransform is a `#runInLockWithCVR` with no client (:2670) and skips
        // the gate.
        if origin != ConfigPassOrigin::BackgroundRetransform
            && self
                .run_in_lock_for_client(client_id, &ws_id, origin.cmd(), is_init)
                .is_none()
        {
            return false;
        }
        let (puts, dels, clear) = parse_desired_queries_patch(body);
        // Client push overrides (TS ConnectionContextManager handleInitConnection:
        // `userPushURL` replaces the push target; `userPushHeaders` become
        // customHeaders after the TS-side `allowedClientHeaders` filter).
        // Stored through the shared `push_override` cell so the message
        // handler's clone of `PushRelayHeaders` sees them too.
        if is_init
            && (body.get("userPushURL").is_some() || body.get("userPushHeaders").is_some())
            && let Some(headers) = self.client_push_headers.get(client_id)
            && let Ok(mut ov) = headers.push_override.lock()
        {
            *ov = Some(crate::workers::syncer_ws_message_handler::PushOverride {
                url: body
                    .get("userPushURL")
                    .and_then(|v| v.as_str())
                    .map(str::to_string),
                headers: body
                    .get("userPushHeaders")
                    .and_then(|v| v.as_object())
                    .map(|m| {
                        m.iter()
                            .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                            .collect()
                    }),
            });
        }
        let client_schema = body
            .get("clientSchema")
            .filter(|value| !value.is_null())
            .cloned();
        // TS `#initAndResetCommon` → `checkClientSchema(shardID, clientSchema,
        // tableSpecs, fullTables)` (pipeline-driver.ts:364): the thrown
        // ProtocolError (Internal | SchemaVersionNotSupported) closes the client.
        if let Some(schema) = client_schema.as_ref()
            && let Err(body) = crate::services::view_syncer::client_schema::check_client_schema(
                &self.shard,
                schema,
                &self.tables,
                &self.full_tables,
            )
        {
            tracing::info!(
                "CG {}: rejecting incompatible client schema: {}",
                self.cg_id,
                body.message()
            );
            if let Some(conn) = self.connections.get(client_id) {
                conn.fail(body, None);
            }
            self.delete_client_due_to_disconnect(client_id, &ws_id);
            return false;
        }
        // The custom-query API context (`userQueryURL` + allowlisted headers)
        // was recorded on the ConnectionContextManager by the handler's
        // `connContextManager.initConnection(...)` dispatch BEFORE this method
        // ran (TS `SyncerWsMessageHandler` 'initConnection' — the recording
        // recording site is `CcmDispatchAdapter::init_connection`);
        // `custom_query_context_from` reads it back at transform time.

        // Client-deletion inputs the body may also carry (TS `#handleConfigUpdate`
        // applies query patches AND client deletions in one pass).
        let active_clients = body.get("activeClients").map(|v| str_array(Some(v)));
        let deleted_ids = str_array(body.get("deleted").and_then(|d| d.get("clientIDs")));
        let deleted_groups = str_array(body.get("deleted").and_then(|d| d.get("clientGroupIDs")));
        let has_query_change =
            !puts.is_empty() || !dels.is_empty() || clear || client_schema.is_some();
        let has_deletions =
            active_clients.is_some() || !deleted_ids.is_empty() || !deleted_groups.is_empty();
        // An `initConnection` always runs the sync flow (TS invokes
        // `#syncQueryPipelineSet` on every connect): even with an empty
        // desired-queries patch, the client must be recorded in the CVR (with
        // its internal `lmids` / `mutationResults` queries) and caught up on any
        // patches produced while it was disconnected. A `changeDesiredQueries`
        // with no query change and no deletions is a genuine no-op.
        // NB: `force_config_pass`, not `is_init` — `updateAuth` and the background
        // retransform both arrive with an EMPTY body (no query change, no
        // deletions) and MUST still run the pass; re-transforming under the
        // refreshed credential is their entire purpose. Gating this on
        // `is_init` alone silently made both of them no-ops.
        if !force_config_pass && !has_query_change && !has_deletions {
            return false;
        }

        // Ensure a group CVR: load from the store, or start fresh (dev/no-PG).
        match self.ensure_cvr(true).await {
            Ok(true) => {}
            Ok(false) => {
                self.fail_group("Unable to load the client view state");
                return false;
            }
            Err(LoadCvrError::Store(rust_cvr::cvr_store::CVRStoreError::ClientNotFound(
                message,
            ))) => {
                if let Some(conn) = self.connections.get(client_id) {
                    conn.fail(crate::protocol::ErrorBody::client_not_found(message), None);
                }
                self.delete_client_due_to_disconnect(client_id, &ws_id);
                return false;
            }
            Err(LoadCvrError::Store(error)) => {
                // Level lives on the `send_error` line (see `ensure_cvr`).
                tracing::debug!("CG {}: unable to load CVR: {error}", self.cg_id);
                let message = error.to_string();
                self.fail_group_with_error(
                    cvr_store_error_body(&error),
                    Some(cvr_store_error_thrown(&error, &message)),
                );
                return false;
            }
        }

        let cvr = self
            .cvr
            .as_ref()
            .expect("the CVR is loaded for the life of the service");
        let client_version = self
            .client_base_versions
            .get(client_id)
            .cloned()
            .unwrap_or(None);
        if is_init && let Err(error) = check_client_and_cvr_versions(&client_version, &cvr.version)
        {
            if let Some(conn) = self.connections.get(client_id) {
                conn.fail(*error, None);
            }
            self.delete_client_due_to_disconnect(client_id, &ws_id);
            return false;
        }
        if is_init && cvr.client_schema.is_none() && client_schema.is_none() {
            if let Some(conn) = self.connections.get(client_id) {
                conn.fail(crate::protocol::ErrorBody::basic(
                    crate::protocol::ErrorKind::InvalidConnectionRequest,
                    "The initConnection message for a new client group must include client schema."
                        .to_string(),
                ), None);
            }
            self.delete_client_due_to_disconnect(client_id, &ws_id);
            return false;
        }

        // TS, verbatim (view-syncer.ts:936-944), between the client-schema check
        // and `#handleConfigUpdate`:
        //   // Validate auth before sending any data is sent to this connection.
        //   // the #handleConfigUpdate call below will also transform
        //   // queries, but that may hit the transform cache so do not rely on
        //   // it for validation. This also ensures shared maintenance always has
        //   // a validated connection to fall back to.
        //   if (!(await this.#validateConnection(connCtx))) {
        //     return;
        //   }
        // The cache point is the whole reason this cannot lean on the `'all'`
        // re-transform below: a cached transform answers without asking the API
        // server, so a token revoked at the app layer would be served data.
        if is_init {
            let conn_ctx = lock_unpoisoned(&self.ccm)
                .must_get_connection_context(&CcmConnectionSelector {
                    client_id: client_id.to_string(),
                    ws_id: ws_id.clone(),
                })
                .ok();
            if let Some(conn_ctx) = conn_ctx {
                match self.validate_connection(&conn_ctx).await {
                    Ok(true) => {}
                    // Auth error: `validate_connection` already failed the
                    // connection (TS `#failMaintenanceConnection`).
                    Ok(false) => return false,
                    Err(body) => {
                        // TS rethrows, which fails this connection's downstream
                        // via `#runInLockForClient`'s catch. Rust cannot unwind
                        // across the serial CG task, so close the socket with the
                        // transform failure the same way the transform path does.
                        tracing::warn!(
                            cg_id = %self.cg_id,
                            client_id = %client_id,
                            message = %transform_failure_message(&body),
                            "initConnection auth validation failed"
                        );
                        // Same emission the whole-batch transform failure uses
                        // (`ClientHandler::send_query_transform_failed_error` =
                        // TS `this.fail(new ProtocolError(error))`), so the client
                        // sees one error shape for a failed transform whether it
                        // failed during validation or during the transform itself.
                        for c in &self.get_clients(std::slice::from_ref(&ws_id)) {
                            c.send_query_transform_failed_error(&body);
                        }
                        self.delete_client_due_to_disconnect(client_id, &ws_id);
                        return false;
                    }
                }
            }
        }

        // Query-config pass (records the client + desired queries, hydrates,
        // then catches the client up). Always runs on initConnection.
        let mut config_accepted = false;
        if force_config_pass || has_query_change {
            #[cfg(test)]
            {
                self.config_pass_runs += 1;
            }
            let cvr = self
                .cvr
                .take()
                .expect("the CVR is loaded for the life of the service");
            let state_version = self
                .pipelines()
                .current_version()
                .unwrap_or_else(|| cvr.version.state_version.clone());
            let replica_version = self.replica_version.clone();
            // The rows the client already has (from the CVR row cache).
            // No eager row-map read. TS reaches the row records only through
            // `#lookupRowsForExecutedAndRemovedQueries`, which returns without
            // reading them when nothing was executed or removed — "Query-less
            // update. This can happen for config only changes." (cvr.ts:661-667).
            // `hydrate_and_sync` performs that read behind the same condition, so
            // a config-only pass no longer touches `cvr.rows` at all. `rowCount`
            // is maintained by the store during flush (cvr-store.ts:1068/:1217),
            // which is where TS reads it from too.
            // The client's decoded JWT claims (`authData` for permission rules),
            // read from the ConnectionContextManager at use time — TS passes
            // `mustGetConnectionContext(selector).auth?.raw` to the transform,
            // which decodes the claims (view-syncer-test-util.ts:861).
            let auth_data = lock_unpoisoned(&self.ccm)
                .must_get_connection_context(&CcmConnectionSelector {
                    client_id: client_id.to_string(),
                    ws_id: ws_id.clone(),
                })
                .ok()
                .and_then(|c| c.auth)
                .map(|a| crate::auth::jwt::decode_jwt_claims(a.raw()))
                .unwrap_or_else(|| serde_json::json!({}));
            let now = now_ms();
            let ttl_clock = self.get_ttl_clock(now);
            let hydrate_started = std::time::Instant::now();
            // TS view-syncer.ts:590 `lc.info?.(`init pipelines@${version} (cvr@${cvrVer})`)`
            // is reached ONLY on the run loop's `!#pipelinesSynced` path (:568-606):
            // the first sync after (re)init, never on a later `changeDesiredQueries`
            // (`#syncQueryPipelineSet('missing')`, :644). Rust folds both into this
            // method, so gate on the same flag (`sync_query_pipeline_set` flips it
            // after the first sync; `reset_pipelines_and_rehydrate` re-arms it).
            if !self.pipelines_synced {
                tracing::info!(
                    cg_id = %self.cg_id,
                    "init pipelines@{state_version} (cvr@{})",
                    rust_cvr::schema::types::version_string(&cvr.version)
                );
            }
            crate::trace::note!("hydrate-start", "cg={} client={client_id}", self.cg_id);
            // Poke EVERY registered connection, not just the requester — TS
            // `#syncQueryPipelineSet` pokes `#getClients()` unfiltered. Scoping
            // to `&[ws_id]` left the group's other tabs on the old cookie, and
            // `advance_poke_targets` (which only pokes at-version clients) then
            // excluded them from every future advance: a live-but-frozen
            // connection. The per-client base filters inside the pokers deliver
            // each connection exactly what it hasn't seen.
            let all_ws_ids: Vec<String> = self.registered_ws.values().cloned().collect();
            let query_ctx = self.query_context_for(client_id, &ws_id);
            // Staged clones: `&mut self` receiver vs `&self.<field>` args (the
            // dissolved engine methods live on the service itself now).
            let shard = self.shard.clone();
            let profile_id = self.client_profile_ids.get(client_id).cloned();
            let permissions = self.permissions.clone();
            let hydrated = self
                .config_and_hydrate_with_profile(
                    cvr,
                    client_id,
                    &all_ws_ids,
                    &shard,
                    puts,
                    dels,
                    clear,
                    client_schema,
                    custom_query_transform_mode,
                    profile_id.as_deref(),
                    permissions.as_ref(),
                    &auth_data,
                    query_ctx.as_ref(),
                    state_version,
                    replica_version,
                    self.last_connect_time,
                    now,
                    ttl_clock,
                )
                .await;
            // TEST SEAM: arm the next pass to answer `Err`, exactly as an
            // unhydratable query does in production (a planner probe SQL that
            // will not prepare — TS `db.prepare(sql)` throwing a SqliteError).
            #[cfg(test)]
            let hydrated = match force_hydrate_error_take() {
                Some(error) => Err(error),
                None => hydrated,
            };
            match hydrated {
                Ok(cvr) => {
                    // TS marks the version served at the end of
                    // `#syncQueryPipelineSet` (initConnection /
                    // changeDesiredQueries / catchup), not only after advances:
                    // a hydrate that serves the pending watermark must clear the
                    // serving-lag pending, or the next advance records a lag
                    // inflated by the whole idle gap.
                    self.mark_version_served(&cvr.version);
                    self.cvr = Some(cvr);
                    config_accepted = true;
                    // Pipelines are synced — the group's shared background
                    // retransform may now run (TS view-syncer.ts:607:
                    // `#pipelinesSynced = true; setSharedRetransformReady(true)`).
                    lock_unpoisoned(&self.ccm).set_shared_retransform_ready(true);
                    let elapsed_ms = hydrate_started.elapsed().as_secs_f64() * 1000.0;
                    crate::trace::note!(
                        "hydrate-end",
                        "cg={} client={client_id} elapsed_ms={elapsed_ms:.1}",
                        self.cg_id
                    );
                    // No whole-pass metric or slow warn here. TS never counts a
                    // config pass: `#hydrations.add(1)` fires once per
                    // `#addAndRemoveQueries` batch (view-syncer.ts:2300-2301,
                    // recorded from `hydrate_and_sync`) and once per query in
                    // `#hydrateUnchangedQueries` (:1638-1639, recorded there).
                    // Counting it here made every `already caught up` no-op
                    // pass a "hydration": 720 vs TS 396 on the same replay
                    // (collector diff). TS's `Slow query
                    // materialization` is likewise PER QUERY (:2305-2307).
                }
                Err(e) => {
                    // TS `#runInLockForClient`'s catch (view-syncer.ts:1236-1249):
                    //   lc[getLogLevel(e)]?.(`closing connection with error`, e);
                    //   if (connCtx) this.connContextManager.failConnection(...);
                    //   if (client) client.fail(e); else throw e;
                    // A hydrate throw therefore fails the REQUESTING CONNECTION
                    // only — sibling connections of the same group keep serving
                    // and the view-syncer keeps running. `#addQueryImpl` logs
                    // `query-pipeline-hydrate-failed` and RETHROWS
                    // (pipeline-driver.ts:794-812); that rethrow lands in this
                    // catch, NOT in the run loop's `#cleanup(err)`, which is the
                    // only path that fails every client.
                    //
                    // Rust called `fail_group` here, so ONE client's unhydratable
                    // query evicted every client of the group and terminated the
                    // CG thread. Caught by the runtime log differential:
                    // for the same replay rust logged 47
                    // `terminating after fatal synchronization error` where TS
                    // logged 47 per-query `query hydration failed` and kept every
                    // group alive. The trigger was a filter value SQLite cannot
                    // parse, reaching the planner's inlined probe SQL
                    // (`rust-ivm/src/sqlite/sqlite_cost_model.rs`, whose TS twin
                    // `db.prepare(sql)` simply throws a `SqliteError`).
                    //
                    // The background retransform has NO requesting connection
                    // (TS `#runBackgroundRetransform` is a `#runInLockWithCVR`
                    // that catches its own failure, view-syncer.ts:2670+), so it
                    // must not fail a connection here either.
                    // TS `lc[getLogLevel(e)]?.('closing connection with error', e)`
                    // (view-syncer.ts:1241): the message is exactly
                    // `closing connection with error` and the error rides as a
                    // separate argument. Keep it out of the message text so the
                    // line reads identically on both arms.
                    tracing::error!(
                        client_id,
                        ws_id = %ws_id,
                        cmd = origin.cmd(),
                        error = %e,
                        "closing connection with error"
                    );
                    if origin != ConfigPassOrigin::BackgroundRetransform {
                        let selector = CcmConnectionSelector {
                            client_id: client_id.to_string(),
                            ws_id: ws_id.clone(),
                        };
                        // TS passes `connCtx.revision` — the revision the failed
                        // operation ran under.
                        let revision = lock_unpoisoned(&self.ccm)
                            .get_connection_context(&selector)
                            .map(|c| c.revision);
                        if let Some(revision) = revision {
                            lock_unpoisoned(&self.ccm).fail_connection(&selector, revision);
                        }
                        if let Some(conn) = self.connections.get(client_id) {
                            // TS `client.fail(e)` (view-syncer.ts:1249). The
                            // ClientHandler logs `view-syncer closing connection
                            // with error` at `getLogLevel(e)` — `error` for a raw
                            // hydrate throw, matching the `closing connection
                            // with error` line above — and then fails the
                            // downstream with `wrapWithProtocolError(e)`, which
                            // is what decides the FRAME's level (`warn`).
                            // `e` knows its JS class (`SqliteError` for the
                            // cost-model probe, `Error` otherwise), so the
                            // `String(e)` line prints TS's name.
                            conn.fail(wrap_with_protocol_error(&e.message), Some(e.thrown()));
                        }
                        self.delete_client_due_to_disconnect(client_id, &ws_id);
                    }
                    return false;
                }
            }
        }

        // Client-deletion pass (activeClients GC + explicit `deleted`).
        if has_deletions {
            self.apply_client_deletions(
                client_id,
                active_clients.as_deref(),
                &deleted_ids,
                &deleted_groups,
            )
            .await;
        }
        // The handler's `initConnection` arm runs `pusher.initConnection` when
        // this returns true (TS: only after the ViewSyncer stream started).
        config_accepted
    }

    /// Handle an `updateAuth` message: re-verify the new credential and, if the
    /// resolved auth data changed, re-transform every query for the client group.
    /// Port of TS `ViewSyncer.updateAuth` (+ `ConnectionContextManager` auth
    /// revision tracking): unchanged auth is a no-op; changed auth re-runs the
    /// config/hydrate pass, which recomputes each query's read-permission
    /// transform against the new `authData` and re-hydrates the pipelines whose
    /// transformation hash drifted.
    async fn handle_update_auth(&mut self, client_id: &str, token: &str) {
        if token.is_empty() {
            return;
        }
        // Decode the new claims (unverified) — used both to compare against the
        // stored auth data and to extract the `sub`.
        let new_claims = crate::auth::jwt::decode_jwt_claims(token);
        let new_sub = new_claims
            .get("sub")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Single-user pin (port of `pickToken`, auth.ts:166-174): a client group
        // is pinned to one userID. If this group already has a pinned user and the
        // new token DECODES to a `sub`, that `sub` MUST match — otherwise a
        // validly-signed JWT for a DIFFERENT user (the signing key is shared across
        // users) could re-scope the entire group's `authData` mid-connection.
        //
        // The `sub` check applies ONLY to a token that carries one (a JWT). A truly
        // OPAQUE token has no claims — it decodes to `{}`, contributes no `authData`
        // identity, and TS's modern path (`validateLegacyJWT` undefined) stores it
        // as `opaque` and does NO sub-pin on updateAuth (auth.ts:94-112); the pin is
        // the connection's fixed `userID`, which a refresh never changes. Rejecting
        // an opaque refresh here (as the unconditional `new_sub != pinned` check did,
        // since `None != Some(pinned)`) wrongly closed valid opaque token rotations.
        let pin_mismatch = self
            .pinned_user_id
            .as_deref()
            .is_some_and(|pinned| new_sub.is_some() && new_sub.as_deref() != Some(pinned));
        if pin_mismatch {
            tracing::warn!(
                "CG {}: updateAuth userID mismatch (pinned={:?}, new={new_sub:?}); closing",
                self.cg_id,
                self.pinned_user_id
            );
            crate::metrics::Metrics::inc(&self.metrics.auth_revalidation_failures);
            if let Some(conn) = self.connections.get(client_id) {
                conn.fail(
                    crate::protocol::ErrorBody::unauthorized(
                        "The user id in the new token does not match the previous token. \
                     Client groups are pinned to a single user.",
                    ),
                    None,
                );
            }
            if let Some(ws_id) = self.registered_ws.get(client_id).cloned() {
                self.delete_client_due_to_disconnect(client_id, &ws_id);
            }
            return;
        }

        // Re-verify the token signature with the same validator as the handshake,
        // binding the subject to the group's PINNED user (not the token's own
        // `sub` — that would be a tautological `sub == sub` check). Falls back to
        // the token's `sub` only for an as-yet-unpinned (anonymous) group.
        let expected_sub = self.pinned_user_id.clone().or_else(|| new_sub.clone());
        let verify = self
            .auth_validator
            .validate_auth(&self.cg_id, client_id, expected_sub.as_deref(), Some(token))
            .await;
        if let Err(error_body) = verify {
            tracing::warn!(
                "CG {}: updateAuth verification failed for client {client_id}",
                self.cg_id
            );
            if let Some(conn) = self.connections.get(client_id) {
                conn.fail(error_body, None);
            }
            if let Some(ws_id) = self.registered_ws.get(client_id).cloned() {
                self.delete_client_due_to_disconnect(client_id, &ws_id);
            }
            return;
        }

        // No change in the RAW token → skip re-validation + re-transformation.
        // TS: `authChanged = !authEquals(prev, next)`, and `authEquals` compares
        // the raw token string for BOTH opaque and JWT auth
        // (connection-context-manager.ts:349 → auth.ts `authEquals`).
        // Comparing decoded JWT claims
        // here (the old behavior) wrongly treated an OPAQUE token refresh as
        // unchanged — opaque tokens carry no claims, so both decode to `{}` and a
        // `token-1` → `token-2` swap was skipped, never re-transforming custom
        // queries against the new Bearer token (view-syncer.pg.test.ts
        // "retransforms custom queries when opaque auth refreshes").
        let unchanged = self
            .registered_ws
            .get(client_id)
            .and_then(|ws_id| {
                lock_unpoisoned(&self.ccm)
                    .must_get_connection_context(&CcmConnectionSelector {
                        client_id: client_id.to_string(),
                        ws_id: ws_id.clone(),
                    })
                    .ok()
            })
            .and_then(|ctx| ctx.auth)
            .map(|prev| prev.raw() == token)
            .unwrap_or(false);
        if unchanged {
            tracing::debug!(
                "CG {}: updateAuth unchanged for client {client_id}, skipping re-transform",
                self.cg_id
            );
            return;
        }

        // Refresh the auth on the ConnectionContextManager (below), then re-run
        // the config/hydrate pass with an empty desired-queries patch. Phase 2
        // recomputes every query's transform against the updated authData (and
        // re-fetches custom queries with the new Bearer token — `updateAuth`
        // flows into `connection.auth`, which `custom_query_context_from` reads),
        // detects the hash drift, and re-hydrates.
        crate::metrics::Metrics::inc(&self.metrics.auth_changes);
        // The relayed-push token is NOT snapshotted here — every relay reads the
        // CCM's current auth fresh (handler `relay_headers_for` / router
        // deleteClients cleanup), so refreshing the CCM below is sufficient. TS
        // parity: pusher.ts reads `mustGetConnectionContext` fresh on every push.
        // Refresh the auth on the ConnectionContextManager (TS `updateAuth`).
        if let Some(ws_id) = self.registered_ws.get(client_id).cloned() {
            let selector = CcmConnectionSelector {
                client_id: client_id.to_string(),
                ws_id: ws_id.clone(),
            };
            let conn_ctx = {
                let mut ccm = lock_unpoisoned(&self.ccm);
                let _ = ccm.update_auth(
                    &selector,
                    &UpdateAuthBody {
                        auth: Some(token.to_string()),
                    },
                );
                // Read the context back AFTER the bump so validation is recorded
                // against the refreshed credential's revision.
                ccm.must_get_connection_context(&selector).ok()
            };
            // TS `updateAuth` runs inside `#runInLockForClient` (view-syncer.ts
            // :995): a socket that has not sent initConnection has no handler and
            // is dropped here, after the handler-level CCM refresh above.
            if self
                .run_in_lock_for_client(client_id, &ws_id, "updateAuth", false)
                .is_none()
            {
                return;
            }
            // TS: "If pipelines are not yet synced, there is no transform request
            // that can absorb validation, so validate immediately."
            //   if (!this.#pipelinesSynced) {
            //     if (!(await this.#validateConnection(connCtx))) return;
            //   }
            // (view-syncer.ts:1009-1015). Once pipelines ARE synced the `'all'`
            // re-transform below carries the validation, and TS deliberately lets
            // it — validating here as well would record a CLIENT-asserted identity
            // instead of the API server's, which is what TS's comment at
            // view-syncer.ts:1988-1990 exists to prevent. A failed validation
            // RETURNS: no re-transform on a credential that did not validate.
            if !self.pipelines_synced
                && let Some(conn_ctx) = conn_ctx
            {
                match self.validate_connection(&conn_ctx).await {
                    Ok(true) => {}
                    Ok(false) => return,
                    Err(e) => {
                        // TS rethrows; the ViewSyncer's caller turns it into a
                        // failed connection. Rust has no unwind across the serial
                        // CG task, so warn and stop this updateAuth here.
                        tracing::warn!(
                            "CG {}: updateAuth connection validation failed: {e:?}",
                            self.cg_id
                        );
                        return;
                    }
                }
            }
        }
        let empty_body = serde_json::json!({});
        // TS `updateAuth` -> `#handleConfigUpdate(..., 'all', ...)`: "Re-transform
        // all queries so auth-sensitive query expansion matches the newly
        // validated credential" (view-syncer.ts:1023).
        self.handle_desired_queries(
            client_id,
            &empty_body,
            ConfigPassOrigin::UpdateAuth,
            CustomQueryTransformMode::All,
        )
        .await;
        // Locked-op re-arm (TS `#scheduleAuthMaintenance` in the `finally`).
        self.schedule_auth_maintenance();
    }

    /// Handle an inspector `["inspect", {op, id, ...}]` message. Port of
    /// `handleInspect` (`inspect-handler.ts`): every op except `authenticate`
    /// requires the client group to have authenticated first; unauthenticated
    /// requests get an `authenticated:false` challenge instead of a result.
    /// Any op failure — including an unknown op, where TS throws via
    /// `unreachable(body)` — answers with the `{op:"error", id, value:<string>}`
    /// shape of inspect-handler.ts's catch block (:171-178): a silent drop
    /// would hang the client's inspector RPC forever.
    async fn handle_inspect(&mut self, client_id: &str, body: &serde_json::Value) {
        let Some(ws_id) = self.registered_ws.get(client_id).cloned() else {
            return;
        };
        // TS `inspect` → `#runInLockForClient(selector, msg, this.#handleInspect)`
        // (view-syncer.ts:2640).
        if self
            .run_in_lock_for_client(client_id, &ws_id, "inspect", false)
            .is_none()
        {
            return;
        }
        // Resolve the per-CG dependencies (socket, TTL clock) and delegate to
        // the 1:1 `handleInspect` (services/view_syncer/inspect_handler.rs),
        // mirroring how TS's lock body hands inspect-handler.ts the resolved
        // client / cvr / cvrStore.
        let now = now_ms();
        let ttl_clock = self.get_ttl_clock(now);
        // The requesting connection's decoded JWT claims for `analyze-query`'s
        // permission binding — TS `ctx.auth?.type === 'jwt' ? ctx.auth : undefined`
        // (inspect-handler.ts:157). Read from the CCM at USE time (freshness,
        // HARD RULE 9), mirroring the sync path's `mustGetConnectionContext(...)
        // .auth?.raw` decode. `None` when the connection carries no auth (so
        // run_ast warns + binds NULL); `Some` (even `{}`) means an auth is present.
        let analyze_auth: Option<serde_json::Value> = lock_unpoisoned(&self.ccm)
            .must_get_connection_context(&CcmConnectionSelector {
                client_id: client_id.to_string(),
                ws_id: ws_id.clone(),
            })
            .ok()
            .and_then(|c| c.auth)
            .map(|a| crate::auth::jwt::decode_jwt_claims(a.raw()));
        // The requesting connection's custom-query transform context (API-server
        // url/headers/auth), built from the CCM at use time — TS passes `ctx` to
        // `inspectorDelegate.transformCustomQuery` for the `analyze-query` named
        // path (inspect-handler.ts:121). `None` when no CustomQueryTransformer is
        // configured for this connection.
        let analyze_custom_ctx = self.query_context_for(client_id, &ws_id);
        // Copy the auth flag in/out around the borrow of `self` (bool is Copy;
        // the CG task is strictly serial, so nothing else reads it meanwhile).
        let mut inspector_authenticated = self.inspector_authenticated;
        crate::services::view_syncer::inspect_handler::handle_inspect(
            &self.cg_id,
            body,
            &ws_id,
            self,
            &mut inspector_authenticated,
            self.admin_password.as_deref(),
            &self.server_version,
            ttl_clock,
            analyze_auth,
            analyze_custom_ctx,
        )
        .await;
        self.inspector_authenticated = inspector_authenticated;
    }

    /// Apply client deletions from an `initConnection` / `deleteClients` body.
    /// `active_clients`, when present, deletes any CVR client NOT in the set
    /// (implicit GC of disconnected clients — no ack). `deleted_client_ids` /
    /// `deleted_group_ids` are explicit client-requested deletions (acked). Port
    /// of the client-deletion portion of TS `#handleConfigUpdate`.
    async fn apply_client_deletions(
        &mut self,
        caller_client_id: &str,
        active_clients: Option<&[String]>,
        deleted_client_ids: &[String],
        deleted_group_ids: &[String],
    ) {
        // TS `deleteClients` runs inside `#runInLockForClient` (view-syncer.ts
        // :1036): the requesting socket must have sent initConnection. (The
        // deletion pass of `handle_desired_queries` re-enters here already gated.)
        let Some(ws_id) = self.registered_ws.get(caller_client_id).cloned() else {
            return;
        };
        if self
            .run_in_lock_for_client(caller_client_id, &ws_id, "deleteClients", false)
            .is_none()
        {
            return;
        }
        match self.ensure_cvr(true).await {
            Ok(true) => {}
            Ok(false) => {
                self.fail_group("Unable to load the client view state");
                return;
            }
            // SCOPE: only the REQUESTING client, like TS. `deleteClients` and the
            // deletion pass of `initConnection`/`changeDesiredQueries` all run
            // inside `#runInLockForClient`; `CVRStore.load` throws inside
            // `#runInLockWithCVR` (view-syncer.ts:489-493) BEFORE the callback,
            // so `client` is still undefined in the catch and the error is
            // RETHROWN (view-syncer.ts:1249) -> `Connection.#handleMessage`'s
            // catch -> `#closeWithThrown` closes that ONE socket
            // (workers/connection.ts:229-230). The group keeps serving its other
            // clients. This is NOT the I-6 2b group-teardown case: that deviation
            // covers store WRITE failures (a write-behind batch may already have
            // been served); a failed load served nothing, so there is no
            // durability reason to widen the blast radius past TS's.
            Err(LoadCvrError::Store(rust_cvr::cvr_store::CVRStoreError::ClientNotFound(
                message,
            ))) => {
                let ws_id = self.registered_ws.get(caller_client_id).cloned();
                if let Some(conn) = self.connections.get(caller_client_id) {
                    conn.close_with_error(crate::protocol::ErrorBody::client_not_found(message));
                }
                if let Some(ws_id) = ws_id {
                    self.delete_client_due_to_disconnect(caller_client_id, &ws_id);
                }
                return;
            }
            Err(LoadCvrError::Store(e)) => {
                // Level lives on the `send_error` line (see `ensure_cvr`).
                tracing::debug!("CG {}: unable to load CVR: {e}", self.cg_id);
                let message = e.to_string();
                self.fail_group_with_error(
                    cvr_store_error_body(&e),
                    Some(cvr_store_error_thrown(&e, &message)),
                );
                return;
            }
        }

        // Explicit deletions are acked; a client may not delete itself.
        let ack_ids: Vec<String> = deleted_client_ids
            .iter()
            .filter(|c| c.as_str() != caller_client_id)
            .cloned()
            .collect();
        let cvr_client_ids: Vec<String> = self
            .cvr
            .as_ref()
            .map(|c| c.clients.keys().cloned().collect())
            .unwrap_or_default();
        let delete_ids = clients_to_delete(&cvr_client_ids, active_clients, &ack_ids);

        if delete_ids.is_empty() && deleted_group_ids.is_empty() {
            return;
        }

        let cvr = self
            .cvr
            .take()
            .expect("the CVR is loaded for the life of the service");
        let poke_ws: Vec<String> = self.registered_ws.values().cloned().collect();
        let now = now_ms();
        let ttl_clock = self.get_ttl_clock(now);
        // Staged clone: `&mut self` receiver vs `&self.shard` arg.
        let shard = self.shard.clone();
        let caller_ws_id = self
            .registered_ws
            .get(caller_client_id)
            .cloned()
            .unwrap_or_default();
        match self
            .delete_clients(
                cvr,
                &shard,
                caller_client_id,
                &caller_ws_id,
                &delete_ids,
                &ack_ids,
                deleted_group_ids,
                &poke_ws,
                self.last_connect_time,
                now,
                ttl_clock,
            )
            .await
        {
            Ok(cvr) => {
                self.cvr = Some(cvr);
                crate::metrics::Metrics::add(
                    &self.metrics.client_deletions,
                    delete_ids.len() as u64,
                );
            }
            Err(e) => {
                tracing::error!("CG {}: delete_clients failed: {e}", self.cg_id);
                self.fail_group(&e.to_string());
            }
        }
        // Mutation-result cleanup for EXPLICIT deleteClients messages is
        // relayed by the caller (see the `deleteClients` arm in
        // `handle_message`) — the implicit activeClients GC path deliberately
        // does not clean up, matching TS.
    }

    fn delete_client_due_to_disconnect(&mut self, client_id: &str, ws_id: &str) {
        // TS view-syncer.ts:880-883: the downstream's cleanup logs `client closed`
        // (an error close was already logged as "closing connection … with error").
        tracing::info!(client_id, ws_id, "client closed");
        crate::trace::note!(
            "conn-close",
            "cg={} client={client_id} ws={ws_id}",
            self.cg_id
        );
        // Every accepted socket increments the CG handle count, including a
        // socket later superseded by another wsID.
        if self.open_ws_ids.remove(ws_id) {
            decrement_nonzero(&self.connection_count);
        }

        // A delayed close from the superseded socket must not remove the current
        // connection that happens to share its clientID.
        if self.registered_ws.get(client_id).map(String::as_str) != Some(ws_id) {
            return;
        }
        self.connections.remove(client_id);
        self.registered_ws.remove(client_id);
        self.client_base_versions.remove(client_id);
        self.unregister_client(ws_id);
        self.decrement_active_client(ws_id);
        self.client_push_headers.remove(client_id);
        self.client_profile_ids.remove(client_id);
        // Drop the connection from the ConnectionContextManager (TS
        // `closeConnection`).
        lock_unpoisoned(&self.ccm).close_connection(&CcmConnectionSelector {
            client_id: client_id.to_string(),
            ws_id: ws_id.to_string(),
        });
        let mut global = lock_unpoisoned(&self.global_connections);
        if global
            .get(client_id)
            .is_some_and(|info| info.ws_id.as_str() == ws_id)
        {
            global.remove(client_id);
        }
        drop(global);
        // Last client gone: sync the ttlClock to the CVR one final time before
        // the group idles out — port of TS `#deleteClientDueToDisconnect`'s
        // `#clients.size === 0` branch (view-syncer.ts:761-766; the `#ttlClock
        // !== undefined` guard is the loaded-CVR check inside the callee).
        // `clients` — not `connections` — because TS counts initConnection'd
        // handlers, not accepted sockets.
        if self.clients.is_empty() {
            self.update_ttl_clock_in_cvr_without_lock();
            // TS `#deleteClientDueToDisconnect` also stops the eviction timer on
            // the last disconnect (view-syncer.ts:767): an idle group with no
            // connected clients runs zero query eviction until a client
            // reconnects and re-arms via `schedule_expire_eviction`.
            self.stop_expire_timer();
        }
    }

    /// Delay until this client-group worker can be torn down. Keeping this in
    /// the same event loop as TTL/auth deadlines avoids a timer task retaining
    /// the group. Both the logical connection map and the admission counter
    /// must be empty: superseded sockets can close after their replacement.
    fn next_idle_shutdown_delay(&self) -> Option<Duration> {
        if !self.connections.is_empty() || self.connection_count.load(Ordering::Relaxed) != 0 {
            return None;
        }
        Some(Duration::from_millis(
            (self.keepalive_until - now_ms()).max(0) as u64,
        ))
    }

    fn idle_shutdown_due(&self) -> bool {
        self.connections.is_empty()
            && self.connection_count.load(Ordering::Relaxed) == 0
            && now_ms() >= self.keepalive_until
    }

    fn close_connection(&mut self, client_id: &str, ws_id: &str) {
        if self.registered_ws.get(client_id).map(String::as_str) != Some(ws_id) {
            return;
        }
        if let Some(conn) = self.connections.get(client_id) {
            // TS supersede closes the replaced connection FRAME-LESS
            // (view-syncer.ts:913 `client.close("replaced by wsID: …")` →
            // `ClientHandler.close` → `downstream.cancel()`); it does NOT send an
            // error frame. Emitting a `Rehome` here made the superseded socket's
            // client observe a spurious "reconnect elsewhere" signal even though
            // the SAME client had already reconnected (this method only runs for
            // the same-clientID supersede, CGMessage::CloseConnection). Caught by
            // the ownership differential: rust=Rehome, TS=none.
            conn.close("Connection superseded by a newer connection");
        }
        self.delete_client_due_to_disconnect(client_id, ws_id);
    }

    /// Hot-reload the read-permissions doc if it changed on the replica since
    /// the last check. Port of TS `PipelineDriver.currentPermissions()` →
    /// `reloadPermissionsIfChanged`, which the view-syncer consults every sync
    /// cycle: a `zero-deploy-permissions` redeploy flows through the replica as
    /// a WAL commit, so by the time this CG is notified the new doc is
    /// committed. Returns `true` if the permissions changed (the caller must
    /// then re-transform + re-hydrate every query under the new rules).
    ///
    /// No-ops for in-memory CGs (no `replica_path`, e.g. unit tests).
    /// Change-streamer notification: advance the pipelines to head and poke all
    /// clients. Loads the CVR from the store on first use. A no-store / no-CVR
    /// CG (e.g. tests without PG) logs and skips.
    /// Pair the just-served version with the pending upstream commit for the
    /// end-to-end serving-lag histogram (no-op when nothing is pending or the
    /// served version does not yet cover it). Port of TS `#markVersionServed`.
    fn mark_version_served(&mut self, version: &CVRVersion) {
        crate::trace::note!(
            "poke-sent",
            "cg={} version={} clients={}",
            self.cg_id,
            version.state_version,
            self.registered_ws.len()
        );
        if let Some(obs) = self
            .e2e_serving_lag
            .on_version_served(&version.state_version, now_ms() as f64)
        {
            crate::metrics::record_e2e_serving_lag(obs.lag_ms);
            if obs.clamped {
                crate::metrics::record_e2e_serving_lag_clamp();
            }
        }
        // TS `#servedVersion = version.stateVersion`. Refresh the cross-CG
        // serving-lag snapshot now that this CG has caught up to `version`.
        self.served_version = Some(version.state_version.clone());
        self.publish_serving_lag();
    }

    /// Record the upstream commit behind a `version-ready` so the served
    /// version can be paired with it for the end-to-end serving-lag histogram.
    /// Port of TS `#e2eServingLagTracker.onVersionReady(replicaState)`.
    ///
    /// Replay guard: the bridge's `/notify` POST is retried, so an
    /// already-processed notification can be redelivered (the in-process TS
    /// notifier cannot replay). A watermark at or behind the CVR's current
    /// state version is already served — re-arming the tracker with it would
    /// record a spurious, retry-latency-inflated lag observation on the next
    /// serve. Skip arming for those; the advance itself stays harmless
    /// (idempotent, and a zero-change advance now no-op-flushes to orig).
    fn arm_serving_lag(&mut self, notification: &serde_json::Value) {
        let watermark = notification.get("watermark").and_then(|v| v.as_str());
        if let (Some(w), Some(cvr)) = (watermark, self.cvr.as_ref())
            && w <= cvr.version.state_version.as_str()
        {
            return;
        }
        self.e2e_serving_lag.on_version_ready(
            watermark,
            notification
                .get("upstreamCommitTimeMs")
                .and_then(|v| v.as_f64()),
        );
    }

    async fn on_notification(&mut self, notification: serde_json::Value) {
        self.arm_serving_lag(&notification);

        // A notification can only advance an existing CVR (no create): without a
        // loaded CVR there is nothing to advance.
        match self.ensure_cvr(false).await {
            Ok(true) => {}
            Ok(false) => {
                self.fail_group("Unable to load the client view state");
                return;
            }
            // A protocol error passes through TS `wrapWithProtocolError` unchanged:
            // the store's ClientNotFound reaches the clients as ClientNotFound.
            Err(LoadCvrError::Store(rust_cvr::cvr_store::CVRStoreError::ClientNotFound(
                message,
            ))) => {
                self.fail_group_with_error(
                    crate::protocol::ErrorBody::client_not_found(message),
                    None,
                );
                return;
            }
            Err(LoadCvrError::Store(e)) => {
                // Level lives on the `send_error` line (see `ensure_cvr`).
                tracing::debug!("CG {}: unable to load CVR: {e}", self.cg_id);
                let message = e.to_string();
                self.fail_group_with_error(
                    cvr_store_error_body(&e),
                    Some(cvr_store_error_thrown(&e, &message)),
                );
                return;
            }
        }
        // No permissions check here: TS `#advancePipelines` (view-syncer.ts:
        // 2567-2640) never re-reads permissions; they are re-read at the
        // transform sites via `PipelineDriver.currentPermissions()` through the
        // pinned snapshot (see `sync_query_pipeline_set`). Checking per
        // notification through a freshly opened replica connection cost
        // ~780 ms per notification on the replay box (9794 notifications, pre-
        // advance p50 784 ms vs 17 ms for the advance itself).
        let cvr = self
            .cvr
            .take()
            .expect("the CVR is loaded for the life of the service");

        let client_ids: Vec<String> = self.registered_ws.values().cloned().collect();
        let now = now_ms();
        let ttl_clock = self.get_ttl_clock(now);
        let advance_started = std::time::Instant::now();
        crate::trace::note!(
            "advance-start",
            "cg={} clients={}",
            self.cg_id,
            client_ids.len()
        );
        match self
            .advance_and_sync(
                cvr,
                self.replica_version.clone(),
                &client_ids,
                self.last_connect_time,
                now,
                ttl_clock,
            )
            .await
        {
            Ok(result) => {
                let advance_ms = advance_started.elapsed().as_secs_f64() * 1000.0;
                crate::trace::note!(
                    "advance-end",
                    "cg={} elapsed_ms={advance_ms:.1} reset={}",
                    self.cg_id,
                    result.reset_reason.is_some()
                );
                // `zero.sync.advance-time` is recorded inside `advance_and_sync`
                // (TS `#advancePipelines`, view-syncer.ts:2631) from the process
                // clock, and only on the success path — see `Metrics::record_advance`.
                if let Some(reason) = result.reset_reason.clone() {
                    // The engine could not advance in place (snapshot/schema
                    // drift). Port of TS `ResetPipelinesSignal` handling: the
                    // in-flight poke was already cancelled; re-init the pipeline
                    // and re-hydrate every query from scratch.
                    // TS logs `result.message` (the detailed signal text) and
                    // labels the metric with `result.reason`
                    // (view-syncer.ts:573-574). `reset_msg` carries the arm that
                    // fired plus elapsed/budget/pos — without it the log cannot
                    // say WHY the advance was abandoned.
                    self.metrics.record_reset(&reason);
                    let message = result.reset_msg.clone().unwrap_or_else(|| reason.clone());
                    self.reset_pipelines_and_rehydrate(result.cvr, &message)
                        .await;
                } else {
                    self.mark_version_served(&result.cvr.version);
                    self.cvr = Some(result.cvr);
                }
            }
            Err(e) => {
                tracing::error!("CG {}: advance_and_sync failed: {e}", self.cg_id);
                self.fail_group(&e.to_string());
            }
        }
    }

    /// Re-initialize the IVM pipeline from a fresh replica snapshot and
    /// re-hydrate every query currently in the CVR. Port of the reset branch in
    /// TS `#syncQueryPipelines`: `#pipelines.reset()` then re-run the query
    /// pipeline set. Called when `advance_and_sync` reports a reset.
    async fn reset_pipelines_and_rehydrate(&mut self, cvr: CVR, message: &str) {
        // TS view-syncer.ts:573 `lc.info?.(`resetting pipelines: ${result.message}`)`
        // — the DETAILED signal text (which budget arm fired, elapsed vs the
        // hydration budget, pos/numChanges), not the coarse `result.reason`,
        // which TS uses only as the `#pipelineResets` metric label (:574).
        tracing::info!(cg_id = %self.cg_id, "resetting pipelines: {message}");
        // TS `#pipelines.reset(clientSchema)` (view-syncer.ts:575) with
        // `must(cvr.clientSchema, 'cvr.clientSchema missing after initialization')`
        // (:548-551); the ProtocolError `#initAndResetCommon` throws fails the
        // view syncer, i.e. every connection gets the body.
        let Some(client_schema) = cvr.client_schema.clone() else {
            self.fail_group("cvr.clientSchema missing after initialization");
            return;
        };
        if let Err(body) = crate::services::view_syncer::pipeline_driver::init_and_reset_common(
            self.replica_path.as_deref(),
            &self.shard,
            &client_schema,
            &mut self.tables,
            &mut self.full_tables,
        ) {
            tracing::error!(
                "CG {}: pipeline reset rejected the client schema: {}",
                self.cg_id,
                body.message()
            );
            self.fail_group_with_error(body, None);
            return;
        }
        // Re-init the engine against a fresh snapshot; this clears every hydrated
        // query so the rehydrate below re-adds the full set.
        // Staged clones: `self.pipelines()` takes `&mut self`, so the config
        // fields must be read out first now that both live on the service.
        let tables = self.tables.clone();
        let replica_path = self.replica_path.clone();
        let app_id = self.app_id.clone();
        if let Err(e) = self
            .pipelines()
            .init(tables, replica_path.as_deref(), &app_id)
        {
            tracing::error!(
                "CG {}: pipeline re-init after reset failed: {e}",
                self.cg_id
            );
            self.fail_group("Client view pipeline reset failed");
            return;
        }
        // The pipelines were just cleared — port of TS `#pipelinesSynced = false`
        // right after `#pipelines.reset()` (view-syncer.ts:575-576). This re-arms
        // the once-per-init `hydrate_unchanged_queries` so the first re-hydrate
        // pass below rebuilds every already-gotten query from the CVR.
        self.pipelines_synced = false;
        // TS's post-reset rehydrate (view-syncer.ts:592-606) is ONE pass —
        // `#hydrateUnchangedQueries(lc, cvr)` then `#syncQueryPipelineSet(lc,
        // cvr, 'missing', undefined, driftedQueryIDs)` — and both resolve their
        // `connCtx` to `connContextManager.mustGetBackgroundConnectionContext()`
        // (view-syncer.ts:1500-1501 and :1913-1914). It never iterates
        // `#clients`; the pokes go to `#getClients()`, every connection.
        //
        // An earlier version ran this pass once PER registered client, each
        // with that client's own context, and — with no client registered —
        // once with an empty auth (d8c00a28f). Same frames, but a different
        // context for the custom-query transform, and a group TS would have
        // stopped kept serving. One pass, the background connection's context,
        // read at use time (rules 8 and 9).
        let background = lock_unpoisoned(&self.ccm).get_background_connection_context();
        let Some(background) = background else {
            // TS `mustGetBackgroundConnectionContext()` throws
            // `ProtocolErrorWithLevel({kind: InvalidConnectionRequest, message},
            // 'warn')` (connection-context-manager.ts:555-565). Thrown inside
            // the `#stateChanges` loop it escapes `run()`, whose catch logs
            // `stopping view-syncer ${id}: ${String(e)}` at `getLogLevel(e)`
            // (view-syncer.ts:617-622; `String(e)` of a ProtocolError is
            // `ProtocolError: <message>`, zero-protocol error.ts:165-166) and
            // hands it to `#cleanup(e)`, which fails every client with it. A
            // group whose last client has dropped — the reap not yet fired —
            // takes exactly this path in TS.
            const MESSAGE: &str = "No validated connection is available for shared query work.";
            // A `ProtocolErrorWithLevel` without a `name` override prints as
            // `ProtocolError: <message>` under `String(e)`.
            let thrown = Thrown::WithLevel {
                level: crate::workers::connection::LogLevel::Warn,
                name: "ProtocolError",
            };
            tracing::warn!(
                cg_id = %self.cg_id,
                "stopping view-syncer {}: {}",
                self.cg_id,
                thrown.js_string(MESSAGE)
            );
            self.fail_group_with_error(
                crate::protocol::ErrorBody::basic(
                    crate::protocol::ErrorKind::InvalidConnectionRequest,
                    MESSAGE.to_string(),
                ),
                Some(thrown),
            );
            return;
        };
        #[cfg(test)]
        self.reset_pass_contexts.push(background.client_id.clone());
        let now = now_ms();
        // TS `#getClients()` — every registered connection is a poke target.
        let all_ws_ids: Vec<String> = self.registered_ws.values().cloned().collect();
        let state_version = self
            .pipelines()
            .current_version()
            .unwrap_or_else(|| cvr.version.state_version.clone());
        let replica_version = self.replica_version.clone();
        // No eager row-map read. TS reaches the row records only through
        // `#lookupRowsForExecutedAndRemovedQueries`, which returns without
        // reading them when nothing was executed or removed. `rowCount` is
        // maintained by the store during flush (cvr-store.ts:1068/:1217),
        // which is where TS reads it from too.
        // authData: the BACKGROUND connection's, decoded at use time (TS
        // `resolvedConnCtx.auth?.raw`).
        let auth_data = background
            .auth
            .as_ref()
            .map(|a| crate::auth::jwt::decode_jwt_claims(a.raw()))
            .unwrap_or_else(|| serde_json::json!({}));
        let ttl_clock = self.get_ttl_clock(now);
        let query_ctx = self.query_context_for(&background.client_id, &background.ws_id);
        // Staged clones: `self.pipelines()` takes `&mut self`.
        let shard = self.shard.clone();
        let profile_id = self.client_profile_ids.get(&background.client_id).cloned();
        let permissions = self.permissions.clone();
        let cvr = match self
            .config_and_hydrate_with_profile(
                cvr,
                &background.client_id,
                &all_ws_ids,
                &shard,
                Vec::new(),
                Vec::new(),
                false,
                None,
                // TS's run-loop init sync re-transforms only what is missing:
                // `#hydrateUnchangedQueries` (which just transformed every
                // custom query) is followed by
                // `#syncQueryPipelineSet(lc, cvr, 'missing', undefined, drifted)`
                // (view-syncer.ts:599-605).
                CustomQueryTransformMode::Missing,
                profile_id.as_deref(),
                permissions.as_ref(),
                &auth_data,
                query_ctx.as_ref(),
                state_version,
                replica_version,
                self.last_connect_time,
                now,
                ttl_clock,
            )
            .await
        {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("CG {}: rehydrate after reset failed: {e}", self.cg_id);
                self.fail_group_js(&e);
                return;
            }
        };
        self.mark_version_served(&cvr.version);
        self.cvr = Some(cvr);
    }

    /// Active-clients gauge -1 (TS `#activeClients.add(-1, {protocol.version})`),
    /// balanced: only decrements a ws we incremented at register. Idempotent —
    /// a superseded-then-closed ws is decremented once.
    fn decrement_active_client(&mut self, ws_id: &str) {
        if let Some(pv) = self.active_client_pv.remove(ws_id) {
            crate::metrics::record_active_client_delta(-1, pv);
        }
    }

    /// Port of TS `#cleanup()` with NO error (view-syncer.ts:2810-2824) — the
    /// path every drain and every idle expiry takes. `Syncer.drain()` calls
    /// `vs.stop()` (workers/syncer.ts:746) → `#stateChanges.cancel()` → the run
    /// loop ends NORMALLY → `#cleanup()` → `client.close(`closed
    /// clientGroupID=${id}`)` → `Connection.close` → `ws.close()`: no error
    /// frame, no status. The idle path is the same loop exit
    /// (`#stateChanges.cancel()` inside `#runInLockWithCVR`, :486).
    ///
    /// An earlier version sent `["error", Rehome "Reconnect required"]` and
    /// cited `#cleanup`'s `client.fail(...)`. Wrong on both counts:
    /// `#cleanup(err)` fails clients only with the error that ESCAPED the run
    /// loop — the cvr-store ownership / CAS Rehomes (cvr-store.ts:1367-1398,
    /// `warn`, different messages), which rust reaches through `fail_group` —
    /// and the "Reconnect required" body is thrown to a REQUESTING op in
    /// `#runInLockWithCVR` (:464-478), never handed to `#cleanup`; rust emits it
    /// where TS does, for an admission queued behind the stop
    /// (`reject_queued_connection`). zero-client maps a frame-less close and a
    /// param-less Rehome to the same `NO_STATUS_TRANSITION` reconnect
    /// (error.ts:163, zero.ts:2392), so the reconnect cadence never differed;
    /// the frame on the wire and the client's log line did.
    fn shutdown(&mut self) {
        self.accepting.store(false, Ordering::SeqCst);
        let reason = format!("closed clientGroupID={}", self.cg_id);
        for (_, conn) in self.connections.drain() {
            // TS `ClientHandler.close(reason)` (client-handler.ts:183-186) logs
            // at DEBUG, then `downstream.cancel()` ends the connection's stream
            // and `Connection.close` logs `closing connection: …` at INFO.
            tracing::debug!(
                client_id = %conn.client_id(),
                "view-syncer closing connection: {reason}"
            );
            conn.close(&reason);
        }
        self.registered_ws.clear();
        self.client_base_versions.clear();
        self.open_ws_ids.clear();
        // Active-clients gauge: -1 for every still-active client (TS decrements on
        // each disconnect during cleanup).
        for (_, pv) in self.active_client_pv.drain() {
            crate::metrics::record_active_client_delta(-1, pv);
        }
        self.connection_count.store(0, Ordering::Relaxed);
    }

    /// Permanently fail this CG: every client gets the error frame and its
    /// socket closes — TS `#cleanup(err)` → `client.fail(err)` for each client
    /// (view-syncer.ts:2810), the outcome of any throw out of the
    /// `#stateChanges` loop or of a timer-driven lock op (`#stateChanges.fail`).
    ///
    /// Error kind/message follow TS `wrapWithProtocolError` (see
    /// [`wrap_with_protocol_error`]): `Internal` + the underlying error text.
    /// An earlier version sent `Rehome` with a fixed label — a different
    /// kind (zero-client reconnects immediately on Rehome, backs off on
    /// Internal) and no diagnostic for the app.
    ///
    /// SCOPE NOTE (rust-only, INVENTIONS.md I-6): TS fails only the requesting
    /// client for a client-initiated lock op (`#runInLockForClient` catch →
    /// `failConnection` + `client.fail`) and keeps serving the group. Rust also
    /// tears the group down for those ops because the CVR write-behind (I-6)
    /// may already have served a version the store never recorded; continuing
    /// would let the next notification skip that batch. The error the
    /// requesting client sees is identical to TS.
    fn fail_group(&mut self, message: &str) {
        // TS `#cleanup(err)` calls `client.fail(err)` with the RAW value that
        // escaped the `#stateChanges` loop, so `getLogLevel` yields `error`,
        // not the `warn` a bare ProtocolError would get. A bare `&str` here is
        // a plain JS `Error` (`must()`, `assert()`, `new Error(msg)`) — its
        // `String(e)` prints `Error: <message>`; a caller holding a `JsError`
        // with a real class uses `fail_group_js`.
        self.fail_group_with_error(
            wrap_with_protocol_error(message),
            Some(Thrown::Other {
                name: "Error",
                message,
            }),
        );
    }

    /// [`fail_group`] for an error that knows its JS class (a hydrate throw:
    /// `SqliteError` from the cost-model probe, `Error` otherwise), so the
    /// `String(e)` log lines print TS's name.
    fn fail_group_js(&mut self, error: &JsError) {
        self.fail_group_with_error(
            wrap_with_protocol_error(&error.message),
            Some(error.thrown()),
        );
    }

    /// Like [`fail_group`], but closes every connection with a specific
    /// `ErrorBody` instead of the default `Rehome`. Used for the older-replica
    /// case, where TS fails clients with a `ClientNotFound` (so the client wipes
    /// local state and re-syncs fresh) rather than a reconnect-elsewhere Rehome.
    fn fail_group_with_error(
        &mut self,
        error: crate::protocol::ErrorBody,
        thrown: Option<Thrown<'_>>,
    ) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        crate::metrics::record_fail_group("sync");
        self.accepting.store(false, Ordering::SeqCst);
        for (_, conn) in self.connections.drain() {
            conn.fail(error.clone(), thrown);
        }
        // Drain into a local first: `drain()` holds `&mut self.registered_ws`
        // while `unregister_client` needs `&mut self` (dissolved engine method).
        let drained: Vec<String> = self.registered_ws.drain().map(|(_, ws)| ws).collect();
        for ws_id in drained {
            self.unregister_client(&ws_id);
        }
        for (_, pv) in self.active_client_pv.drain() {
            crate::metrics::record_active_client_delta(-1, pv);
        }
        self.client_base_versions.clear();
        self.client_profile_ids.clear();
        self.open_ws_ids.clear();
        self.connection_count.store(0, Ordering::Relaxed);
    }
}

/// Route one inbound socket frame through the ported dispatch chain:
/// `Connection.#handleMessage` → `SyncerWsMessageHandler` →
/// `ViewSyncerDispatch` (the `CgViewSyncer` adapter), all inline on this CG
/// task (the handler is the single dispatch). Three phases so the service cell is NOT borrowed
/// while the handler runs (the live dispatch re-borrows it).
async fn on_inbound(
    state_rc: &Rc<RefCell<ViewSyncerService>>,
    client_id: Arc<str>,
    ws_id: Arc<str>,
    text: String,
) {
    // Phase 1 (borrow): stale-frame check + resolve the connection.
    let conn = {
        let state = state_rc.borrow();
        // A superseded socket can have frames already queued when its
        // replacement is installed. Never route those frames through the new
        // connection.
        if state.registered_ws.get(&*client_id).map(String::as_str) != Some(&*ws_id) {
            tracing::debug!(
                "CG {}: ignoring stale inbound frame for {client_id}/{ws_id}",
                state.cg_id
            );
            return;
        }
        match state.connections.get(&*client_id) {
            Some(conn) => Rc::clone(conn),
            None => return,
        }
    };
    // Phase 2 (no borrow): the ported dispatch, executed to completion.
    let closed = !conn.handle_inbound(&text).await;
    // Phase 3 (borrow): close bookkeeping.
    if closed {
        state_rc
            .borrow_mut()
            .delete_client_due_to_disconnect(&client_id, &ws_id);
    }
}

/// The async body hosting one client group, run as a `spawn_local` task on its
/// executor's `current_thread` runtime + `LocalSet`. Owns the (`!Send`)
/// [`SyncEngine`]; drives connection setup, inbound frames, disconnects, and
/// change-streamer notifications. Message handling and the TTL-eviction /
/// auth-maintenance / idle-shutdown deadline ticks are multiplexed with
/// `tokio::select!` over `rx.recv()` and `tokio::time::sleep`.
// See `CgViewSyncer`'s allow note: the cell is confined to the single-threaded CG task: a `RefCell` borrow held
// across an await cannot race (no other task touches it), and the only re-entry
// path — the inbound dispatch — releases its borrow before awaiting the handler
// (see `on_inbound`). The lint guards multi-task executors; this is the
// deliberate TS-`#lock` twin.
#[allow(clippy::await_holding_refcell_ref)]
pub(crate) async fn cg_event_loop(
    cg_id: &str,
    mut rx: mpsc::UnboundedReceiver<CGMessage>,
    connection_count: Arc<AtomicU64>,
    accepting: Arc<AtomicBool>,
    ctx: CgTaskContext,
    last_notification: Option<serde_json::Value>,
) {
    // The service lives in a shared cell: the per-connection
    // handler's `CgViewSyncer` dispatch re-borrows it inline on this task (TS
    // `Connection` holds `#viewSyncer`). Borrows are scoped; only this task
    // touches the cell, and the inbound path releases its borrow before
    // awaiting the handler.
    let state_rc = Rc::new(RefCell::new(ViewSyncerService::new_with_accepting(
        cg_id,
        &ctx.services_factory,
        ctx.auth_validator,
        ctx.connections.clone(),
        connection_count,
        accepting,
        ctx.cvr_pool,
    )));
    state_rc.borrow_mut().self_handle = Some(Rc::downgrade(&state_rc));
    {
        let mut state = state_rc.borrow_mut();
        // Publish into the process-wide serving-lag registry (replacing the
        // standalone default the constructor installed) and register an initial
        // snapshot so the sampler/gauges see this CG immediately.
        state.serving_lag_registry = ctx.serving_lag_registry;
        state.publish_serving_lag();
        // Arm the serving-lag tracker with the newest pre-spawn commit (TS notifier
        // latest-state replay): the group's FIRST serve then records an observation
        // instead of silently swallowing everything before the next commit.
        if let Some(n) = &last_notification {
            state.arm_serving_lag(n);
        }
    }
    if state_rc.borrow().terminal {
        // Surface initialization failure to the accepted socket instead of
        // dropping the queued connection silently.
        state_rc.borrow().accepting.store(false, Ordering::SeqCst);
        if let Some(CGMessage::NewConnection {
            params,
            sink,
            enqueued_at,
        }) = rx.recv().await
        {
            crate::metrics::record_lock_wait_ms(enqueued_at.elapsed().as_secs_f64() * 1000.0);
            let state = state_rc.borrow();
            let mut global = lock_unpoisoned(&state.global_connections);
            if global
                .get(&params.client_id)
                .is_some_and(|info| info.ws_id == params.ws_id)
            {
                global.remove(&params.client_id);
            }
            drop(global);
            decrement_nonzero(&state.connection_count);
            // TS Connection#closeWithError: error frame, then ws.close() with no status.
            sink.fail_with_code(
                crate::protocol::ErrorBody::internal(
                    "Failed to initialize the client-group sync engine",
                ),
                None,
            );
        }
        state_rc
            .borrow()
            .connection_count
            .store(0, Ordering::Relaxed);
        return;
    }

    // Event loop: await the next message, but wake early when a deadline is due
    // — a query TTL eviction (TS `#scheduleExpireEviction` /
    // `#removeExpiredQueries`) or a periodic auth-maintenance tick (TS
    // `#scheduleAuthMaintenance` / `#runAuthMaintenance`). We wake at the
    // earliest of the deadlines and run whichever ones are actually due. With
    // nothing pending we await the channel indefinitely.
    //
    // `stashed` holds a non-notification message popped while coalescing a run
    // of queued notifications (see the Notification arm); it is handled before
    // the channel is polled again, preserving message order.
    let mut stashed: std::collections::VecDeque<CGMessage> = std::collections::VecDeque::new();
    loop {
        if let Some(msg) = stashed.pop_front() {
            if !dispatch_cg_message(&state_rc, &mut rx, &mut stashed, msg).await {
                tracing::info!("CG thread {cg_id}: shutting down");
                break;
            }
            let mut state = state_rc.borrow_mut();
            if state.terminal {
                tracing::error!("CG thread {cg_id}: terminating after fatal synchronization error");
                break;
            }
            // A material CVR flush (re)starts the ttlClock interval — port of
            // TS `#flushUpdater`'s `if (flushed)` (view-syncer.ts:1083-1086).
            if state.take_flush_observed() {
                state.start_ttl_clock_interval();
            }
            continue;
        }
        let next_delay = {
            let state = state_rc.borrow();
            [
                state.next_expiry_delay(),
                state.next_auth_maintenance_delay(),
                state.next_idle_shutdown_delay(),
                state.next_ttl_clock_delay(),
            ]
            .into_iter()
            .flatten()
            .min()
        };

        let msg = match next_delay {
            Some(delay) => {
                tokio::select! {
                    biased;
                    recv = rx.recv() => match recv {
                        Some(msg) => msg,
                        None => break,
                    },
                    _ = tokio::time::sleep(delay) => {
                        // Deadline ticks never dispatch through the handler, so
                        // one borrow may span the whole block (single-task cell).
                        let mut state = state_rc.borrow_mut();
                        if state.idle_shutdown_due() {
                            // Port of TS view-syncer.ts:482: on the all-clients-
                            // disconnected shutdown path, log `closing
                            // clientGroupID=<id>` at INFO. rust reaches this via
                            // idle-keepalive expiry (the mirror of TS
                            // #checkForShutdownConditionsInLock); emit the same
                            // lifecycle line so log-sequence parity holds (D gate).
                            // TS view-syncer.ts:2803 `stop()` → `lc.info?.('stopping view syncer')`.
                            tracing::info!("stopping view syncer");
                            tracing::info!("closing clientGroupID={cg_id}");
                            tracing::info!(
                                "CG thread {cg_id}: idle keepalive elapsed; shutting down"
                            );
                            state.shutdown();
                            // TS `lc.info?.(`view-syncer ${this.id} finished`)`
                            // at the tail of `run()` (view-syncer.ts:629).
                            tracing::info!("view-syncer {cg_id} finished");
                            break;
                        }
                        // A wake could be for either deadline; run each if due.
                        if state
                            .next_auth_maintenance_at
                            .is_some_and(|at| at <= now_ms())
                        {
                            state.run_auth_maintenance().await;
                        }
                        // Fire the eviction timer only when its own deadline has
                        // elapsed (TS `#expiredQueriesTimer` setTimeout callback);
                        // a shared wake for another deadline must not run eviction
                        // early. TS clears the handle at the start of the callback
                        // (view-syncer.ts:1423) and reschedules at the tail of
                        // `#removeExpiredQueries` (651) — so clear first, then run
                        // on_expiry_tick (which re-arms on success).
                        if state
                            .expired_queries_timer
                            .is_some_and(|at| at <= now_ms())
                        {
                            state.stop_expire_timer();
                            state.on_expiry_tick().await;
                        }
                        // Periodic ttlClock persistence (TS #startTTLClockInterval's
                        // callback, view-syncer.ts:1093-1096: update, then re-arm).
                        if state.ttl_clock_interval.is_some_and(|at| at <= now_ms()) {
                            state.update_ttl_clock_in_cvr_without_lock();
                            state.start_ttl_clock_interval();
                        }
                        // An expiry tick can materially flush the CVR; a flush
                        // restarts the ttlClock interval (view-syncer.ts:1083-1086).
                        if state.take_flush_observed() {
                            state.start_ttl_clock_interval();
                        }
                        continue;
                    }
                }
            }
            None => match rx.recv().await {
                Some(msg) => msg,
                None => break,
            },
        };
        if !dispatch_cg_message(&state_rc, &mut rx, &mut stashed, msg).await {
            tracing::info!("CG thread {cg_id}: shutting down");
            break;
        }
        let mut state = state_rc.borrow_mut();
        if state.terminal {
            tracing::error!("CG thread {cg_id}: terminating after fatal synchronization error");
            break;
        }
        // A material CVR flush (re)starts the ttlClock interval — port of
        // TS `#flushUpdater`'s `if (flushed)` (view-syncer.ts:1083-1086).
        if state.take_flush_observed() {
            state.start_ttl_clock_interval();
        }
    }

    // The loop has stopped (drain `Shutdown`, idle expiry, or a terminal
    // failure). Port of the `!this.#stateChanges.active` branch of TS
    // `#runInLockWithCVR` (view-syncer.ts:464-478): an op reaching a
    // view-syncer whose run loop has ended — "a backlog of tasks queued on the
    // lock, or ... a client connects before the ViewSyncer has been deleted
    // from the ServiceRunner" (runner.ts:36-46 deletes only in `run().finally`)
    // — is answered with `ProtocolErrorWithLevel(Rehome "Reconnect required",
    // 'info')`. Rust's mailbox IS that lock queue: a `NewConnection` the
    // router sent before it observed `accepting == false` is still in `rx`.
    // An earlier version dropped it with the receiver, its sink with it, and
    // the writer task ended on a closed channel — the socket closed with NO
    // frame (ws_server.rs `None => break`). A queued `Inbound` needs nothing:
    // TS's queued op throws the same Rehome into a downstream `#cleanup` has
    // already closed (no second frame), and rust's socket is already closed by
    // `shutdown()` / `fail_group`. Anything the router sends AFTER this drain
    // fails at `tx.send` once `rx` drops and is rehomed there
    // (syncer.rs `Client-group worker restarted`).
    while let Ok(msg) = rx.try_recv() {
        if let CGMessage::NewConnection { params, sink, .. } = msg {
            reject_queued_connection(&state_rc.borrow(), &params, sink);
        }
    }
}

/// Answer a `NewConnection` that reached a group whose loop has stopped — TS
/// `#runInLockWithCVR`'s inactive branch (view-syncer.ts:464-478) as seen from
/// `initConnection`: the throw is caught by `.catch(e => newClient.fail(e))`
/// (:964), so `ClientHandler.fail` logs at the error's OWN level (`info`,
/// client-handler.ts:176), and `Connection` sends the frame and closes with no
/// status (`#closeWithThrown` → `sendError` + `close`, connection.ts:319-337).
/// No `Connection` exists yet for a queued admission, so the three TS lines are
/// emitted here and the frame goes straight to the sink.
fn reject_queued_connection(
    state: &ViewSyncerService,
    params: &ConnectParams,
    sink: DirectWebSocketSink,
) {
    // TS `this.#lc.debug?.('state changes are inactive')` (view-syncer.ts:469).
    tracing::debug!(
        client_id = %params.client_id,
        ws_id = %params.ws_id,
        "state changes are inactive"
    );
    let error = crate::protocol::ErrorBody::rehome("Reconnect required");
    // TS `ClientHandler.fail(e)` — `getLogLevel(e)` is the ProtocolErrorWithLevel's own `info`.
    // `ProtocolErrorWithLevel(Rehome, 'info')` has no `name` override, so
    // `String(e)` prints `ProtocolError: Reconnect required`.
    let thrown = Thrown::WithLevel {
        level: crate::workers::connection::LogLevel::Info,
        name: "ProtocolError",
    };
    tracing::info!(
        client_id = %params.client_id,
        ws_id = %params.ws_id,
        "view-syncer closing connection with error: {}",
        thrown.js_string(error.message())
    );
    // TS `sendError` (connection.ts:429): `thrown instanceof ProtocolErrorWithLevel` → its level.
    let frame = crate::protocol::error_message(&error);
    let error_body = frame
        .get(1)
        .map(ToString::to_string)
        .unwrap_or_else(|| "null".to_string());
    tracing::info!(
        client_id = %params.client_id,
        error_kind = ?error.kind(),
        error_body = %error_body,
        "Sending error on WebSocket"
    );
    // The admission was counted by the router (`get_or_create_cg`) and recorded
    // in the global map by the accept path; undo both, as the init-failure
    // reject above does.
    let mut global = lock_unpoisoned(&state.global_connections);
    if global
        .get(&params.client_id)
        .is_some_and(|info| info.ws_id == params.ws_id)
    {
        global.remove(&params.client_id);
    }
    drop(global);
    decrement_nonzero(&state.connection_count);
    // TS `Connection.#closeWithError`: error frame, then `ws.close()` with no status.
    sink.fail_with_code(error, None);
}

/// Handle one CG message. Returns `false` when the event loop must stop
/// (`Shutdown`).
///
/// The `Notification` arm coalesces any immediately-queued run of further
/// notifications into ONE advance, mirroring the TS notifier subscription's
/// coalesce-while-busy contract (notifier.ts: newest state wins, oldest
/// upstream commit time is kept). Without this, a slow CG behind a commit
/// burst runs one full `advance_and_sync` per queued notification — N advances
/// (and N small serving-lag observations) where TS does one — and its
/// unbounded queue grows with the backlog. A non-notification message popped
/// while draining is pushed to `stashed` for in-order handling by the caller.
#[allow(clippy::await_holding_refcell_ref)] // single-task cell — see `CgViewSyncer`
async fn dispatch_cg_message(
    state_rc: &Rc<RefCell<ViewSyncerService>>,
    rx: &mut mpsc::UnboundedReceiver<CGMessage>,
    stashed: &mut std::collections::VecDeque<CGMessage>,
    msg: CGMessage,
) -> bool {
    match msg {
        CGMessage::NewConnection {
            params,
            sink,
            enqueued_at,
        } => {
            // TS `#runInLockWithCVR` lock wait (view-syncer.ts:459-461).
            crate::metrics::record_lock_wait_ms(enqueued_at.elapsed().as_secs_f64() * 1000.0);
            let accepted_at = std::time::Instant::now();
            let piggyback = state_rc.borrow_mut().on_new_connection(*params, sink).await;
            if crate::trace::enabled() {
                let cg_id = state_rc.borrow().cg_id.clone();
                crate::trace::note!(
                    "cg-new-connection",
                    "cg={cg_id} setup_ms={:.1}",
                    accepted_at.elapsed().as_secs_f64() * 1000.0
                );
            }
            // Piggybacked initConnection: dispatched through the SAME inbound
            // path as a socket frame (TS `Connection.init()` feeds
            // `#handleMessage`), after the setup borrow above is released.
            if let Some((client_id, ws_id, text)) = piggyback {
                on_inbound(state_rc, client_id, ws_id, text).await;
            }
        }
        CGMessage::Inbound {
            client_id,
            ws_id,
            text,
            enqueued_at,
        } => {
            let queue_wait = enqueued_at.elapsed();
            // TS `#runInLockWithCVR` lock wait (view-syncer.ts:459-461): the
            // client message waited this long for the group's serial lock.
            crate::metrics::record_lock_wait_ms(queue_wait.as_secs_f64() * 1000.0);
            let handled_at = std::time::Instant::now();
            let handled_cpu = crate::trace::thread_cpu_ms();
            // The frame's message kind (`["changeDesiredQueries", ...]` → the
            // first string), for the trace only.
            let kind = if crate::trace::enabled() {
                text.trim_start_matches(['[', ' ', '"'])
                    .split('"')
                    .next()
                    .unwrap_or("")
                    .to_string()
            } else {
                String::new()
            };
            on_inbound(state_rc, client_id, ws_id, text).await;
            if crate::trace::enabled() {
                let cg_id = state_rc.borrow().cg_id.clone();
                crate::trace::note!(
                    "cg-inbound",
                    "cg={cg_id} kind={kind} queue_wait_ms={:.1} handle_ms={:.1} handle_cpu_ms={:.1}",
                    queue_wait.as_secs_f64() * 1000.0,
                    handled_at.elapsed().as_secs_f64() * 1000.0,
                    crate::trace::thread_cpu_ms() - handled_cpu
                );
            }
        }
        CGMessage::ConnectionClosed { client_id, ws_id } => state_rc
            .borrow_mut()
            .delete_client_due_to_disconnect(&client_id, &ws_id),
        CGMessage::CloseConnection { client_id, ws_id } => {
            state_rc.borrow_mut().close_connection(&client_id, &ws_id)
        }
        CGMessage::Notification {
            value: n,
            enqueued_at,
        } => {
            // TS `#runInLockWithCVR` lock wait (view-syncer.ts:459-461): the
            // advance waited this long behind the group's other work.
            crate::metrics::record_lock_wait_ms(enqueued_at.elapsed().as_secs_f64() * 1000.0);
            let mut merged = n;
            let mut merged_count = 1u32;
            loop {
                match rx.try_recv() {
                    Ok(CGMessage::Notification {
                        value: next,
                        enqueued_at,
                    }) => {
                        // Each coalesced notification was its own lock task in
                        // TS and would have recorded its own wait.
                        crate::metrics::record_lock_wait_ms(
                            enqueued_at.elapsed().as_secs_f64() * 1000.0,
                        );
                        merged = merge_notifications(merged, next);
                        merged_count += 1;
                    }
                    Ok(other) => {
                        stashed.push_back(other);
                        break;
                    }
                    Err(_) => break,
                }
            }
            let notified_at = std::time::Instant::now();
            state_rc.borrow_mut().on_notification(merged).await;
            if crate::trace::enabled() {
                let cg_id = state_rc.borrow().cg_id.clone();
                crate::trace::note!(
                    "cg-notification",
                    "cg={cg_id} merged={merged_count} handle_ms={:.1}",
                    notified_at.elapsed().as_secs_f64() * 1000.0
                );
            }
        }
        CGMessage::Shutdown => {
            state_rc.borrow_mut().shutdown();
            return false;
        }
    }
    true
}

/// Merge two coalesced notifications: the newer notification's fields win
/// (`{...prev, ...curr}` in TS notifier.ts), except `upstreamCommitTimeMs`
/// keeps the OLDEST value — it bounds the lag of every commit the merged
/// notification subsumes.
fn merge_notifications(prev: serde_json::Value, next: serde_json::Value) -> serde_json::Value {
    let min_commit = match (
        prev.get("upstreamCommitTimeMs").and_then(|v| v.as_f64()),
        next.get("upstreamCommitTimeMs").and_then(|v| v.as_f64()),
    ) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    let mut merged = match (prev, next) {
        (serde_json::Value::Object(mut p), serde_json::Value::Object(n)) => {
            for (k, v) in n {
                p.insert(k, v);
            }
            serde_json::Value::Object(p)
        }
        (_, n) => n,
    };
    if let (Some(obj), Some(t)) = (merged.as_object_mut(), min_commit) {
        obj.insert(
            "upstreamCommitTimeMs".to_string(),
            serde_json::Value::from(t),
        );
    }
    merged
}

// TEST SEAM (rust-only, `#[cfg(test)]`, no TS twin — TS drives these cases from
// real `instances` rows in view-syncer.pg.test.ts / cvr-store.pg.test.ts). Arms
// the next `ensure_cvr` store load to fail with the given `CVRStoreError` so the
// failure SCOPE (requesting client vs whole group) and the client-visible error
// KIND can be pinned without Postgres. Consumed once, per thread (a CG owns its
// thread). A doc comment cannot document a macro invocation (rustc
// `unused_doc_comments`), hence `//`.
#[cfg(test)]
thread_local! {
    static FORCE_LOAD_ERROR: RefCell<Option<CVRStoreError>> = const { RefCell::new(None) };
}

// TEST SEAM (rust-only, `#[cfg(test)]`, no TS twin). Arms the next config pass
// so the hydrate answers `Err`, exactly as an unhydratable query does in
// production. Injecting the RESULT the handler matches on — not an earlier
// precondition — is what makes the failure arm, and its per-connection scope,
// the code actually exercised.
#[cfg(test)]
thread_local! {
    static FORCE_HYDRATE_ERROR: RefCell<Option<JsError>> = const { RefCell::new(None) };
}

#[cfg(test)]
fn force_hydrate_error(error: JsError) {
    FORCE_HYDRATE_ERROR.with(|c| *c.borrow_mut() = Some(error));
}

#[cfg(test)]
fn force_hydrate_error_take() -> Option<JsError> {
    FORCE_HYDRATE_ERROR.with(|c| c.borrow_mut().take())
}

#[cfg(test)]
fn force_load_error(error: CVRStoreError) {
    FORCE_LOAD_ERROR.with(|c| *c.borrow_mut() = Some(error));
}

#[cfg(test)]
fn force_load_error_take() -> Option<CVRStoreError> {
    FORCE_LOAD_ERROR.with(|c| c.borrow_mut().take())
}

// ─── Engine + CVR hot path (the dissolved `SyncEngine` seat) ─────────────────
// The former `sync_engine.rs` engine + CVR hot path, merged into
// `ViewSyncerService` per TS: `view-syncer.ts` owns `#pipelines` / `#cvrStore` /
// `#clients` directly. Port of the `CVRState` + `hydrate_and_sync` /
// `advance_and_sync` logic the removed native bridge carried (a5e502ad9),
// with its thread-hop machinery stripped. Drives the
// flow:
//
//   engine `RowChange` → `ChangeProcessor::on_row_change` →
//   `CVRQueryDrivenUpdater` → `MultiPoker` (poke frames) → `DirectWebSocketSink`
//   → `CVRStoreHandle::flush` (PG).
//
// Runs on the CG task; not `Send`/`Sync`.

/// Result of `hydrate_and_sync` / `advance_and_sync`.
#[derive(Debug)]
pub struct SyncResult {
    /// The flushed CVR snapshot (or the unchanged input on a reset).
    pub cvr: CVR,
    /// The new CVR version string (empty on a reset).
    pub version: String,
    /// Config/query patches produced by `track_queries` (empty for advance).
    pub query_patches: Vec<PatchToVersion>,
    /// Number of row changes processed.
    pub num_changes: usize,
    /// Set when the engine requested a reset (rehydrate) rather than advancing.
    pub reset_reason: Option<String>,
    pub reset_msg: Option<String>,
    /// PROCESS time (yielded time excluded) spent hydrating this pass — TS
    /// `totalProcessTime`, accumulated across `generateRowChanges`
    /// (view-syncer.ts:2295) and reported by the caller alongside the wall time
    /// of the whole add/remove span. 0 on the advance path, which has no such
    /// TS log line.
    pub process_time_ms: f64,
}

#[derive(Debug, thiserror::Error)]
pub enum LoadCvrError {
    #[error(transparent)]
    Store(#[from] CVRStoreError),
}

/// Inert auth seat for the storeless engine-surface constructor
/// (`ViewSyncerService::new`) — rust-only test scaffold, never reached by
/// production wiring (which constructs via `new_with_accepting`).
struct InertAuthValidator;

#[async_trait::async_trait]
impl AuthValidator for InertAuthValidator {
    async fn validate_auth(
        &self,
        _client_group_id: &str,
        _client_id: &str,
        _user_id: Option<&str>,
        _auth: Option<&str>,
    ) -> Result<(), crate::protocol::ErrorBody> {
        Ok(())
    }
}

/// Rust-only diagnostic (AGENTS.md rule 5, no TS twin): per-process tally of how
/// each advance ended, so the no-op advance storm can be attributed WITHOUT
/// per-event logging. `SYNCER_TRACE=1` emits ~2,900 lines/s, which would distort
/// the very latency a measurement run exists to capture; these are three atomic
/// increments plus one summary line per minute.
///
/// Classes (they partition every advance):
///   `no_changes`  - the IVM advance collected nothing for this CG's queries.
///   `quiet`       - it collected changes, but the CVR flush found nothing
///                   material (pruned as unchanged) and discarded the bump.
///   `material`    - the flush persisted.
static ADV_NO_CHANGES: AtomicU64 = AtomicU64::new(0);
static ADV_QUIET: AtomicU64 = AtomicU64::new(0);
static ADV_MATERIAL: AtomicU64 = AtomicU64::new(0);
static ADV_SUMMARY_AT_MS: AtomicU64 = AtomicU64::new(0);

/// Emit the advance-classification tally at most once a minute, per process.
fn maybe_log_advance_summary() {
    const EVERY_MS: u64 = 60_000;
    let now = now_ms() as u64;
    let last = ADV_SUMMARY_AT_MS.load(Ordering::Relaxed);
    if now.saturating_sub(last) < EVERY_MS {
        return;
    }
    if ADV_SUMMARY_AT_MS
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return; // another thread is emitting this tick
    }
    let (n, q, m) = (
        ADV_NO_CHANGES.load(Ordering::Relaxed),
        ADV_QUIET.load(Ordering::Relaxed),
        ADV_MATERIAL.load(Ordering::Relaxed),
    );
    let total = n + q + m;
    tracing::info!(
        "advance-classes total={total} no_changes={n} quiet={q} material={m} \
         no_changes_pct={:.1}",
        if total > 0 {
            n as f64 * 100.0 / total as f64
        } else {
            0.0
        }
    );
}

/// Whether a failed CVR flush may be retried.
///
/// Rust-only (AGENTS.md rule 5): TS has no flush retry at all, so this
/// predicate exists only to bound the rust-only convoy mitigation. It must
/// stay TRUE for exactly the errors `CVRStoreHandle::flush` can raise BEFORE
/// `std::mem::take(&mut self.pending)` — today that is the `pool.begin()`
/// acquire. Every other error has already eaten the writes, so retrying it
/// cannot re-send them; it can only turn a hard failure into a silent quiet
/// commit. Anything not listed here propagates and fails the group, which is
/// what TS does with every flush error.
fn is_retryable_flush_error(e: &rust_cvr::cvr_store::CVRStoreError) -> bool {
    matches!(
        e,
        rust_cvr::cvr_store::CVRStoreError::Sqlx(
            sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed
        )
    )
}

impl ViewSyncerService {
    /// The storeless engine-surface constructor (the former standalone
    /// `SyncEngine::new`). Rust-only test scaffold, no TS twin: the engine-level
    /// harness tests (stage_e / pg_harness) drive the dissolved data-path
    /// surface (`register_client` / `config_and_hydrate` / `advance_and_sync` /
    /// `catchup_clients`) without CG machinery; every non-engine field is an
    /// inert default that surface never reads. Production constructs via
    /// `new_with_accepting` (the factory path).
    pub fn new(pipelines: IvmPipelines) -> Self {
        let ccm = Arc::new(Mutex::new(ConnectionContextManager::new(
            None, None, None, None, None, None,
        )));
        let created_at = now_ms();
        ViewSyncerService {
            cg_id: String::new(),
            pipelines,
            store: None,
            query_replacements: HashMap::new(),
            clients: HashMap::new(),
            tokio_handle: None,
            enable_query_covering: true,
            flush_observed: std::cell::Cell::new(false),
            _engine_census: crate::live_count::Guard::new(&crate::live_count::SYNC_ENGINE),
            self_handle: None,
            ccm,
            mutagen: None,
            pusher: None,
            shard: ShardID {
                app_id: String::new(),
                shard_num: 0,
            },
            replica_version: String::new(),
            cvr_pg: false,
            tables: Vec::new(),
            full_tables: Vec::new(),
            replica_path: None,
            app_id: String::new(),
            permissions: None,
            permissions_hash: None,
            next_auth_maintenance_at: None,
            background_retransform_failure: None,
            forced_retransform_outcomes: std::collections::VecDeque::new(),
            pipelines_synced: false,
            #[cfg(test)]
            hydrate_unchanged_runs: 0,
            #[cfg(test)]
            validate_connection_runs: 0,
            #[cfg(test)]
            config_pass_runs: 0,
            #[cfg(test)]
            reset_pass_contexts: Vec::new(),
            #[cfg(test)]
            forced_flush_outcomes: std::cell::RefCell::new(std::collections::VecDeque::new()),
            #[cfg(test)]
            existing_rows_calls: std::cell::Cell::new(0),
            pinned_user_id: None,
            cvr: None,
            e2e_serving_lag:
                crate::services::view_syncer::e2e_serving_lag::E2EServingLagTracker::new(),
            ttl_clock: 0,
            ttl_clock_base: created_at,
            ttl_clock_interval: None,
            expired_queries_timer: None,
            last_connect_time: created_at,
            keepalive_until: created_at + CG_KEEPALIVE_MS,
            connections: HashMap::new(),
            registered_ws: HashMap::new(),
            client_base_versions: HashMap::new(),
            open_ws_ids: HashSet::new(),
            active_client_pv: HashMap::new(),
            client_push_headers: HashMap::new(),
            client_profile_ids: HashMap::new(),
            admin_password: None,
            server_version: String::new(),
            metrics: Arc::new(crate::metrics::Metrics::default()),
            inspector_authenticated: false,
            inspector_delegate: std::cell::RefCell::new(
                crate::server::inspector_delegate::InspectorDelegate::new(),
            ),
            auth_validator: Arc::new(InertAuthValidator),
            global_connections: Arc::new(Mutex::new(HashMap::new())),
            connection_count: Arc::new(AtomicU64::new(0)),
            accepting: Arc::new(AtomicBool::new(true)),
            terminal: false,
            created_at_ms: created_at,
            served_version: None,
            last_row_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            serving_lag_registry: Arc::new(crate::workers::syncer::ServingLagRegistry::new()),
            _census: crate::live_count::Guard::new(&crate::live_count::CLIENT_GROUP),
        }
    }

    /// Consume the "a material CVR flush happened" signal (see
    /// `flush_observed`). Returns true at most once per flush.
    pub fn take_flush_observed(&self) -> bool {
        self.flush_observed.replace(false)
    }

    /// Rust-only relay to `CVRStore.updateTTLClock` (no TS twin): in TS the
    /// view-syncer holds `#cvrStore` and calls `updateTTLClock` on it directly;
    /// here the caller cannot reach the `!Send` store (the engine owns it), so
    /// this forwards the call, fire-and-forget on the shared-pool runtime (TS
    /// `.catch`es and logs — view-syncer.ts:1110-1114). The 1:1 port of
    /// `#updateTTLClockInCVRWithoutLock` itself is `update_ttl_clock_in_cvr_
    /// without_lock`, further down this file.
    /// No-op without a store (in-memory / test CGs).
    pub fn update_ttl_clock(&self, ttl_clock: rust_cvr::ttl_clock::TTLClock, last_active: i64) {
        let Some(store_arc) = self.store.clone() else {
            return;
        };
        let fut = async move {
            let store = store_arc.lock().await;
            if let Err(e) = store.update_ttl_clock(ttl_clock, last_active as f64).await {
                // TS view-syncer.ts:1115 `lc.warn?.('failed to update TTL clock', …)`.
                tracing::warn!(error = %e, "failed to update TTL clock");
            }
        };
        match &self.tokio_handle {
            // Fire-and-forget on the shared-pool runtime (the CG thread's
            // reactor does not drive the CVR pool's connections).
            Some(handle) => {
                handle.spawn(fut);
            }
            // No injected handle (unit tests): run inline on the current task.
            None => {
                tokio::task::spawn_local(fut);
            }
        }
    }

    /// Enable/disable shadow-mode query-covering logging. Port of
    /// `zeroConfig.enableQueryCovering` (default true).
    pub fn set_enable_query_covering(&mut self, enabled: bool) {
        self.enable_query_covering = enabled;
    }

    /// Read the current row records from the CVR (the client's `existing_rows`).
    /// Port of TS's `this.#cvrStore.getRowRecords()` reads: the store owns the
    /// row-record cache (cvr-store.ts:246) and loads it lazily, keeping it
    /// current through `flush`'s `apply`, so this never re-reads Postgres after
    /// the first call. Empty when there is no store.
    ///
    /// Returns an `Arc` snapshot (O(1)) — NOT a deep copy. This runs once per
    /// advance/config/TTL pass per client group; the previous full-map clone was
    /// the dominant per-advance allocation at high client counts.
    ///
    /// A load failure PROPAGATES, as TS's rejecting `getRowRecords()` does. It
    /// must not degrade to an empty map: `execute_row_updates` prunes tombstones
    /// for rows absent from this set, so an empty existing-row set would silently
    /// swallow real row DELs.
    pub async fn existing_rows(&self) -> Result<Arc<RowRecordMap>, CVRStoreError> {
        #[cfg(test)]
        self.existing_rows_calls
            .set(self.existing_rows_calls.get() + 1);
        let Some(store_arc) = self.store.clone() else {
            return Ok(Arc::new(HashMap::new()));
        };
        // Offload the (idempotent) cache load + read onto the shared-pool
        // runtime — the CG executor's reactor does not drive the CVR pool.
        self.offload(async move { store_arc.lock().await.get_row_records().await })
            .await
    }

    /// Inspector query view — delegates to `CVRStore::inspect_queries` (the SQL
    /// port of TS `CVRStore.inspectQueries`). Empty when no store is attached.
    pub async fn inspect_queries(
        &self,
        ttl_clock: TTLClock,
        client_id: Option<&str>,
    ) -> Result<Vec<InspectQueryRow>, CVRStoreError> {
        let Some(store_arc) = self.store.clone() else {
            return Ok(vec![]);
        };
        let store = store_arc.lock().await;
        Ok(store.inspect_queries(ttl_clock, client_id).await?)
    }

    /// How many times the CVR row cache deep-copied its entire map on `apply`
    /// because a `get_row_records()` snapshot was still alive. Rust-only test
    /// observability (AGENTS rule 5) over the rust-only copy-on-write; TS
    /// cannot copy at all (`getRowRecords()` returns the live `Map`). The
    /// serving paths must hold this at 0 — see the snapshot-scoping notes in
    /// `hydrate_and_sync` and `advance_and_sync`.
    #[doc(hidden)]
    pub async fn row_cache_cow_copies(&self) -> u64 {
        match self.store.clone() {
            Some(store) => store.lock().await.row_cache_cow_copies().await,
            None => 0,
        }
    }

    /// Inject the shared-pool runtime handle used to offload CVR store I/O
    /// (must be the runtime that owns the CVR `PgPool`).
    pub fn set_tokio_handle(&mut self, handle: tokio::runtime::Handle) {
        self.tokio_handle = Some(handle);
    }

    /// Run a `Send` CVR-I/O future on the shared-pool runtime instead of the
    /// caller's single-threaded executor runtime. The pool's connections are
    /// polled by that runtime's reactor, so awaiting them there avoids the
    /// cross-runtime starvation of `RUST-SYNCER-ARCHITECTURE.md` §3; the executor awaits only the
    /// (cross-runtime-safe) `JoinHandle` and is free to drive its other client
    /// groups meanwhile. With no handle injected (some unit tests) the future
    /// runs inline on the current runtime.
    async fn offload<F, T>(&self, fut: F) -> T
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        match &self.tokio_handle {
            Some(handle) => match handle.spawn(fut).await {
                Ok(v) => v,
                Err(e) => {
                    // Log with context BEFORE escalating: the resulting CG-task
                    // panic is caught + logged by the executor wrapper, but this
                    // is the only place that knows it originated in an offloaded
                    // CVR I/O future (vs the CG loop itself).
                    tracing::error!("offloaded CVR I/O task failed: {e}");
                    panic!("CVR I/O task panicked: {e}");
                }
            },
            None => fut.await,
        }
    }

    /// Load the CVR snapshot from the store (or `None` if no store is set).
    pub async fn load_cvr(&self, last_connect_time: f64) -> Result<Option<CVR>, LoadCvrError> {
        let Some(store_arc) = self.store.clone() else {
            return Ok(None);
        };
        // Offload the load onto the shared-pool runtime (`RUST-SYNCER-ARCHITECTURE.md` §3).
        let load_started = std::time::Instant::now();
        // TS view-syncer.ts:491: `this.#runPriorityOp(lc, 'loading cvr', () =>
        // this.#cvrStore.load(...))` — IVM on this event loop slices finer
        // while a connect waits on its CVR.
        let result = run_priority_op(
            "loading cvr",
            self.offload(async move {
                let mut store = store_arc.lock().await;
                store.load(last_connect_time).await
            }),
        )
        .await;
        // `cvr.load_attempts` / `cvr.load_duration` are recorded inside
        // `CVRStore::load` (rust-cvr otel_metrics::record_load, the port of TS
        // `#recordLoad` cvr-store.ts:308-311) — not here, or every load counts twice.
        let result = result?;
        // TS cvr-store.ts:511 `lc.info?.(`loaded cvr@${versionString(cvr.version)} (${ms} ms)`)`.
        tracing::info!(
            "loaded cvr@{} ({:.0} ms)",
            rust_cvr::schema::types::version_string(&result.cvr.version),
            load_started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(Some(result.cvr))
    }

    /// Access the underlying IVM pipelines (e.g. for init / get_row / catchup).
    pub fn pipelines(&mut self) -> &mut IvmPipelines {
        &mut self.pipelines
    }

    /// TEST SEAM (no TS twin — rust-only, AGENTS.md rule 5). Production builds
    /// the service through `new_with_accepting`, which sets `app_id`, `shard`
    /// and `cg_id` from the CG's `SyncEngineConfig`; the bare `new(pipelines)`
    /// constructor used by integration tests leaves them blank. Tests that
    /// assert on the identity TS carries in its LogContext (`appID`,
    /// `shardNum`, `clientGroupID` on the `flushed cvr@…` line, cvr-store.ts:
    /// 1249) need those populated to pin the VALUES rather than just the field
    /// names. Sets only identity; it cannot change serving behavior.
    #[doc(hidden)]
    pub fn set_identity_for_tests(&mut self, app_id: &str, shard: ShardID, cg_id: &str) {
        self.app_id = app_id.to_string();
        self.shard = shard;
        self.cg_id = cg_id.to_string();
    }

    /// Create the CVR Postgres store (once, shared across all calls) — the
    /// twin of TS's `#cvrStore` construction in the `ViewSyncerService` ctor.
    pub fn set_cvr_store(
        &mut self,
        pool: sqlx::PgPool,
        schema: String,
        cvr_id: String,
        task_id: String,
    ) -> Result<(), String> {
        // The pool is the ONE process-wide CVR pool, shared across every client
        // group (cloning it is cheap — `PgPool` is an `Arc` internally, so all
        // CGs draw from the same bounded set of Postgres connections). Building a
        // pool per CG previously multiplied connection demand by the number of
        // groups and exhausted Postgres backends, stalling `block_on` acquires on
        // the CG loop. Matches TS's one-pool-per-worker model.
        // The store builds and owns its RowRecordCache, exactly like TS's
        // `new CVRStore(...)` does (cvr-store.ts:246).
        let store = CVRStoreHandle::new(pool, schema, cvr_id, task_id);
        self.store = Some(Arc::new(tokio::sync::Mutex::new(store)));
        Ok(())
    }

    /// Port of TS `initConnection`'s SYNCHRONOUS prefix (view-syncer.ts:864-914),
    /// run when the initConnection MESSAGE arrives — not at socket accept:
    /// `#activeClients.add(1)` (:888), the first-client `#ttlClockBase` reset
    /// (:893-899), `new ClientHandler(...)` whose constructor parses the base
    /// cookie (:903-910), the `replaced by wsID` close of a prior handler
    /// (:911-913) and `#clients.set` (:914). The locked body TS then runs
    /// (`#runInLockForClient(... #handleConfigUpdate)`, :922-961) is the rest of
    /// `handle_desired_queries`. Returns `false` when the connection was failed.
    fn init_connection(&mut self, client_id: &str, ws_id: &str) -> bool {
        tracing::debug!("viewSyncer.initConnection");
        let Some(conn) = self.connections.get(client_id).cloned() else {
            return false;
        };
        // Active-clients gauge +1; remembered per socket so the matching
        // disconnect decrements the same protocol-version tag (:884-890).
        if !self.active_client_pv.contains_key(ws_id) {
            let protocol_version = conn.protocol_version();
            self.active_client_pv
                .insert(ws_id.to_string(), protocol_version);
            crate::metrics::record_active_client_delta(1, protocol_version);
        }
        // "First connection to this ViewSyncerService": TTLs count CONNECTED
        // time, so the idle gap since the last client left is dropped.
        if self.clients.is_empty() {
            self.ttl_clock_base = now_ms();
        }
        // `connCtx.baseCookie` (:909) — read back from the
        // ConnectionContextManager, as TS does.
        let base_cookie: Option<String> = lock_unpoisoned(&self.ccm)
            .get_connection_context(&CcmConnectionSelector {
                client_id: client_id.to_string(),
                ws_id: ws_id.to_string(),
            })
            .and_then(|c| c.base_cookie);
        // A malformed baseCookie throws in the ClientHandler constructor
        // (client-handler.ts `cookieToVersion` → `versionFromString`,
        // schema/types.ts); the throw escapes to `Connection.#handleMessage` and
        // is wrapped as a fatal `Internal` error (`wrapWithProtocolError`,
        // types/error-with-level.ts). Rust's constructor is lenient (CG-task
        // panic safety), so reproduce the TS-visible outcome here:
        // ["error",{kind:"Internal"}] then close.
        if let Some(c) = base_cookie.as_deref()
            && let Err(e) = rust_cvr::schema::types::maybe_version_string(c)
        {
            // What TS throws here is a RAW `TypeError` / `Error`
            // (schema/types.ts:333/338), so `#closeWithThrown(e)` hands
            // `sendError` the raw value and `getLogLevel` yields 'error' — not
            // the 'info' a bodiless `close_with_error` classified this as.
            let message = e.to_string();
            // TS `versionFromString`: a third `:` part throws `new TypeError(
            // `Invalid version string ${str}`)` (schema/types.ts:339); every
            // other failure is a plain `Error` (:333, lexi-version.ts:54).
            // `String(e)` prints that class before the message.
            let name = match e {
                rust_cvr::schema::types::VersionError::TooManyParts(_) => "TypeError",
                _ => "Error",
            };
            conn.close_with_error_thrown(
                crate::protocol::ErrorBody::internal(message.clone()),
                Some(Thrown::Other {
                    name,
                    message: &message,
                }),
            );
            self.delete_client_due_to_disconnect(client_id, ws_id);
            return false;
        }
        // `this.#clients.get(connCtx.clientID)?.close(`replaced by wsID: ${wsID}`)`.
        let prior: Vec<String> = self
            .clients
            .values()
            .filter(|c| c.client_id == client_id && c.ws_id != ws_id)
            .map(|c| c.ws_id.clone())
            .collect();
        for prev_ws_id in prior {
            if let Some(c) = self.clients.get(&prev_ws_id) {
                c.close(&format!("replaced by wsID: {ws_id}"));
            }
            self.unregister_client(&prev_ws_id);
            self.decrement_active_client(&prev_ws_id);
        }
        // `this.#clients.set(connCtx.clientID, newClient)`.
        let shard = self.shard.clone();
        let client_group_id = self.cg_id.clone();
        let sink: Arc<dyn WebSocketSink> = Arc::new(conn.sink().clone());
        self.register_client(
            client_id,
            ws_id,
            &client_group_id,
            &shard,
            base_cookie.as_deref(),
            sink,
        );
        true
    }

    /// TS `#clients.has(clientID)`.
    fn has_client(&self, client_id: &str) -> bool {
        self.clients.values().any(|c| c.client_id == client_id)
    }

    /// TS `this.#clients.get(clientID)` (a Map keyed by clientID). Rust keys
    /// handlers by ws_id: the current socket for a clientID is the registered
    /// one, else any handler carrying that clientID (fixtures register
    /// handlers without the router's ws registration).
    fn client_handler_for(&self, client_id: &str) -> Option<Arc<ClientHandler>> {
        self.registered_ws
            .get(client_id)
            .and_then(|ws_id| self.clients.get(ws_id))
            .or_else(|| self.clients.values().find(|c| c.client_id == client_id))
            .cloned()
    }

    /// Port of TS `#runInLockForClient` (view-syncer.ts:1180-1250) up to the
    /// locked body. The lock itself is the serial CG thread (I-1), so what is
    /// ported is its bookkeeping and its client gate: a message is honored only
    /// when the clientID's CURRENT handler in `clients` — set by
    /// `init_connection` — carries the sender's wsID; otherwise `mismatched
    /// wsID` and the message is dropped (:1216-1221). That gate is how TS
    /// ignores every non-initConnection message from a socket that has not sent
    /// `initConnection` (no handler yet) and every frame from a superseded
    /// socket. Returns the resolved handler.
    fn run_in_lock_for_client(
        &mut self,
        client_id: &str,
        ws_id: &str,
        cmd: &str,
        new_client: bool,
    ) -> Option<Arc<ClientHandler>> {
        tracing::debug!("viewSyncer.#runInLockForClient");
        // :1194-1196.
        if new_client || !self.has_client(client_id) {
            self.last_connect_time = now_ms();
        }
        // :1213 — entering the CG thread's handling IS acquiring the lock.
        tracing::debug!("acquired lock for cvr");
        let client = self.client_handler_for(client_id);
        if client.as_ref().map(|c| c.ws_id.as_str()) != Some(ws_id) {
            // TS passes both ids as extra args, not in the message text.
            tracing::debug!(
                client_ws_id = client.as_ref().map(|c| c.ws_id.as_str()),
                ws_id,
                "mismatched wsID"
            );
            return None;
        }
        // `checkClientAndCVRVersions(client.version(), cvr.version)` (:1230)
        // for a new client needs the loaded CVR; `handle_desired_queries` runs
        // it right after `ensure_cvr`, at the same point of the init flow. The
        // `else if` is unreachable in TS as well — a missing handler already
        // failed the wsID match above — and kept for the 1:1 branch (:1231-1233).
        if !new_client && !self.has_client(client_id) {
            tracing::warn!("Processing {cmd} before initConnection was received");
        }
        client
    }

    /// Register a client for poke delivery — TS `#clients.set(clientID,
    /// newClient)` (view-syncer.ts:914), called from `init_connection` when the
    /// initConnection message arrives. `sink` is typically a
    /// `DirectWebSocketSink`.
    pub fn register_client(
        &mut self,
        client_id: &str,
        ws_id: &str,
        client_group_id: &str,
        shard: &ShardID,
        base_cookie: Option<&str>,
        sink: Arc<dyn WebSocketSink>,
    ) {
        let handler =
            ClientHandler::new(client_group_id, client_id, ws_id, shard, base_cookie, sink);
        self.clients.insert(ws_id.to_string(), Arc::new(handler));
    }

    /// Unregister a client — TS `#clients.delete(clientID)` (view-syncer.ts:759).
    pub fn unregister_client(&mut self, ws_id: &str) {
        self.clients.remove(ws_id);
    }

    /// Send an `inspect` response to a specific client's WebSocket. Port of
    /// `ClientHandler.sendInspectResponse`.
    pub fn send_inspect_response(&self, ws_id: &str, response: serde_json::Value) {
        if let Some(c) = self.clients.get(ws_id) {
            c.send_inspect_response(response);
        }
    }

    /// Fail (send an error + close the socket of) a specific client by ws_id, if
    /// still registered. Used to close a connection that a newer connection for
    /// the same clientID has superseded. Returns whether a client was found.
    pub fn fail_client(&self, ws_id: &str, msg: &str) -> bool {
        if let Some(c) = self.clients.get(ws_id) {
            c.fail(msg);
            true
        } else {
            false
        }
    }

    /// Resolve handlers by WebSocket id — `self.clients` is keyed by `ws_id`
    /// (see `register_client`), and every caller passes ws ids. The parameter
    /// was previously named `client_ids`, inviting a real keying bug.
    /// Port of TS `#sendQueryTransformErrorToClients` (view-syncer.ts:1728-1766).
    /// A custom-query transform error is delivered ONLY to the clients whose CVR
    /// `clientState` desires that query (TS `getAffectedClientIDs`), grouped per
    /// client for application errors — never to every poked socket. The got-del
    /// poke is client-group-wide, so a sibling socket that never desired the
    /// erroring query sees the del but must NOT see a `transformError`
    /// (a frame capture showed rust over-emitting 12 such frames). `custom_query_map` is the sync's CVR snapshot
    /// (TS builds `customQueries` from `cvr.queries` at the top of
    /// `#syncQueryPipelineSet`).
    fn send_query_transform_error_to_clients(
        &self,
        custom_query_map: &BTreeMap<String, QueryRecord>,
        error_or_errors: QueryTransformErrors<'_>,
    ) {
        let get_affected_client_ids = |query_ids: &[&str]| -> Vec<String> {
            let mut client_ids: Vec<String> = Vec::new();
            for query_id in query_ids {
                match custom_query_map.get(*query_id) {
                    Some(QueryRecord::Custom(q)) => {
                        for id in q.client_state.keys() {
                            if !client_ids.contains(id) {
                                client_ids.push(id.clone());
                            }
                        }
                    }
                    // TS: `assert(q, 'got an error for query ... that does not map
                    // back to a custom query')` — cannot happen (the ids come from
                    // the custom queries this sync just transformed); log, don't
                    // take the CG thread down.
                    _ => tracing::error!(
                        "got an error for query {query_id} that does not map back to a custom query"
                    ),
                }
            }
            client_ids
        };
        // TS `this.#clients.get(clientId)`.
        let client_handler = |client_id: &str| self.client_handler_for(client_id);
        match error_or_errors {
            QueryTransformErrors::Failed(failed) => {
                let query_ids: Vec<&str> = failed
                    .get("queryIDs")
                    .and_then(|v| v.as_array())
                    .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                    .unwrap_or_default();
                for client_id in get_affected_client_ids(&query_ids) {
                    if let Some(c) = client_handler(&client_id) {
                        c.send_query_transform_failed_error(failed);
                    }
                }
            }
            QueryTransformErrors::Application(errors) => {
                let mut app_error_groups: BTreeMap<String, Vec<serde_json::Value>> =
                    BTreeMap::new();
                for err in errors {
                    let id = err.get("id").and_then(|v| v.as_str()).unwrap_or_default();
                    for client_id in get_affected_client_ids(&[id]) {
                        app_error_groups
                            .entry(client_id)
                            .or_default()
                            .push(err.clone());
                    }
                }
                for (client_id, errs) in app_error_groups {
                    if let Some(c) = client_handler(&client_id) {
                        let _ = c.send_query_transform_application_errors(errs);
                    }
                }
            }
        }
    }

    /// The poke targets for `ws_ids`, at most ONE handler per socket.
    ///
    /// The de-duplication is load-bearing, not defensive tidiness. TS's
    /// `#getClients()` returns the values of a Map keyed by clientID, so a
    /// duplicate is unrepresentable; rust looks handlers up from a LIST of
    /// ws_ids (`registered_ws.values()`, keyed by client_id, so a repeated
    /// ws_id is representable) and a repeat would put the same
    /// `Arc<ClientHandler>` into one `MultiPoker` twice. The second poker then
    /// contends its own client's `poke_chain` — which `acquire_chain` cannot
    /// resolve, because the holder is the first poker on this very thread.
    fn get_clients(&self, ws_ids: &[String]) -> Vec<Arc<ClientHandler>> {
        let mut seen: HashSet<&str> = HashSet::with_capacity(ws_ids.len());
        ws_ids
            .iter()
            .filter(|id| seen.insert(id.as_str()))
            .filter_map(|id| self.clients.get(id).cloned())
            .collect()
    }

    /// Flush the updater's buffered store ops + CVR to Postgres (no-op when no
    /// store is set). Requires a current tokio runtime handle when a store is
    /// present.
    ///
    /// Returns whether the store MATERIALLY flushed (see `flush_ops_to_store`).
    /// On `false` the caller must fall back to the updater's ORIGINAL CVR —
    /// TS `CVRUpdater.flush`'s `if (!flushed) return {cvr: this._orig}`
    /// (cvr.ts) — because nothing was persisted: adopting the bumped working
    /// CVR would poke clients to a version the store never wrote and make the
    /// next material flush fail its version guard (`ConcurrentModification`).
    async fn flush_to_store(
        &self,
        updater: &mut CVRQueryDrivenUpdater,
        flushed_cvr: Arc<CVR>,
        last_connect_time: i64,
    ) -> Result<bool, String> {
        let expected_current_version = updater.base.orig.version.clone();
        self.flush_ops_to_store(
            updater.base.drain_store_ops(),
            &expected_current_version,
            flushed_cvr,
            last_connect_time,
        )
        .await
    }

    /// Apply buffered store ops and flush the CVR to Postgres (no-op without a
    /// store). Requires the injected tokio handle when a store is present. After
    /// the flush, the same row deltas are written back into the row-record cache
    /// (`RowRecordCache::apply` with `flushed=true`) so `existing_rows()` stays
    /// current without re-reading Postgres.
    /// Returns `Ok(true)` when the store materially flushed (or when no store
    /// is configured — the in-memory path has no version guard to desync, so
    /// callers keep the working CVR as before). Returns `Ok(false)` when the
    /// store found nothing material to write and skipped the flush entirely:
    /// the on-disk CVR version did NOT advance, so the caller must stay on the
    /// updater's original CVR (TS `flush` → `{cvr: this._orig, flushed: false}`).
    async fn flush_ops_to_store(
        &self,
        ops: Vec<StoreOp>,
        expected_current_version: &CVRVersion,
        flushed_cvr: Arc<CVR>,
        last_connect_time: i64,
    ) -> Result<bool, String> {
        // Test seam (empty in production): forces the store-flush outcome so the
        // storeless harness can reach the quiet-commit (`flushed: false`) path.
        #[cfg(test)]
        if let Some(forced) = self.forced_flush_outcomes.borrow_mut().pop_front() {
            return Ok(forced);
        }
        let Some(store_arc) = self.store.clone() else {
            return Ok(true);
        };

        // No row-write dedup here. TS prunes pending row records in exactly ONE
        // place — inside `#flush` (cvr-store.ts:1066-1086) — and that pruning is
        // ported 1:1 in `CVRStoreHandle::flush_internal`. Running an identical
        // filter here as well was a rust-only DUPLICATE, and it was the last
        // consumer forcing every caller to materialise the whole per-CG row map
        // before the pass, including the config-only passes TS skips entirely
        // (cvr.ts:661-667). The store reads what it needs from its own cache.

        // Offload the whole PG-touching section — apply ops, flush the CVR, and
        // mirror the row deltas back into the read cache — onto the shared-pool
        // runtime (`RUST-SYNCER-ARCHITECTURE.md` §3). The store's `!Send` engine state is not touched
        // here (only the `Send` `Arc<Mutex<CVRStoreHandle>>` / cache), so the
        // whole unit can run off-thread while the executor drives other groups.
        // `flushed_cvr` is an `Arc<CVR>`: moving it into the task is a refcount
        // bump, not a deep CVR copy — the caller reclaims the CVR via
        // `Arc::try_unwrap` once this awaited task drops its clone.
        let expected = expected_current_version.clone();
        let flushed = flushed_cvr;
        // TS's `flushed cvr@…` line is emitted through the view-syncer's
        // LogContext, so it carries `appID`, `shardNum`, `clientGroupID`,
        // `instance`, `lock`, `stateVersion` and `cvrFlushID` (cvr-store.ts:1238
        // `lc.withContext('cvrFlushID', flushCounter++)`, logged at :1249).
        // Rust emitted the message with NO fields at all, so a flush could not
        // be attributed to a client group — which is exactly what blocked
        // localising the extra-config-poke divergence from the
        // logs. `instance` and `lock` have no rust twin: there is no `#lock`
        // (the CG thread is serial, INVENTIONS.md I-12) and no per-service
        // instance id.
        let log_app_id = self.app_id.clone();
        let log_shard_num = self.shard.shard_num;
        let log_cg_id = self.cg_id.clone();
        let cvr_flush_id = rust_cvr::cvr_store::next_cvr_flush_id();
        let last_row_count = self.last_row_count.clone();
        // TS `#flushUpdater` wraps EVERY CVR flush (config, hydrate, advance)
        // in `#runPriorityOp(lc, 'flushing cvr', ...)` (view-syncer.ts:
        // 1069-1071); this is the one seat all rust flushes pass through.
        run_priority_op(
            "flushing cvr",
            self.offload(async move {
            if !ops.is_empty() {
                store_arc.lock().await.apply_store_ops(ops);
            }
            let flush_started = std::time::Instant::now();
            let store_flushed = {
                // Bounded retry-with-backoff before declaring the group dead. A
                // failed flush is terminal (fail_group → every client rehomes and
                // REHYDRATES), so under a pool-acquire convoy fail-fast is
                // self-amplifying: timeouts kill groups, rehydrates deepen the
                // convoy. TS has NO acquire timeout and NO shedding — postgres.js
                // simply QUEUES, so transient CVR saturation degrades to latency,
                // not a storm. We approximate that: retry a few times with growing
                // jittered backoff so a saturation spike is ridden out as latency.
                // Unlike TS's unbounded queue this is BOUNDED, so a genuinely-dead
                // CVR still fails the group promptly rather than wedging. Jitter
                // de-synchronizes the convoy.
                //
                // This block once claimed "a failed attempt
                // leaves nothing behind, so retries are safe" and retried EVERY
                // error. A failed attempt leaves nothing behind in the DATABASE
                // (the tx rolls back) but it has already consumed the store's
                // pending set, so the retry re-sends NOTHING, reports a quiet
                // commit, and silently discards the writes. Only errors raised
                // BEFORE the pending set is taken may be retried —
                // `is_retryable_flush_error`.
                const MAX_FLUSH_ATTEMPTS: u32 = 3;
                let mut attempt = 1u32;
                let result = loop {
                    let outcome = {
                        let mut store = store_arc.lock().await;
                        let outcome = store
                            .flush(&expected, &flushed, last_connect_time as f64)
                            .await;
                        // TS `get rowCount() { return this.#cvrStore.rowCount; }`
                        // — the count as `#flush` left it (cvr-store.ts:1068,
                        // 1217-1218), copied out under the lock so
                        // `publish_serving_lag` reads what TS reads.
                        last_row_count.store(store.row_count(), Ordering::Relaxed);
                        outcome
                    };
                    match outcome {
                        Ok(r) => break Ok(r),
                        // ONLY the pool-acquire convoy is retryable. TS has NO
                        // retry at all: `#flushUpdater` lets the error out and
                        // the view-syncer fails the group, so every client
                        // rehomes and rehydrates. Retrying anything else is not
                        // just un-TS, it is UNSOUND — `flush` consumes its
                        // pending set, so a second attempt after a write failure
                        // sends nothing, reports a quiet commit, and silently
                        // discards the writes (see the ACQUIRE BEFORE CONSUMING
                        // note in cvr_store.rs). A pool acquire fails before the
                        // `take`, so that one CAN be replayed truthfully.
                        Err(e) if !is_retryable_flush_error(&e) => break Err(e),
                        Err(e) if attempt >= MAX_FLUSH_ATTEMPTS => break Err(e),
                        Err(e) => {
                            let jitter = (std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.subsec_nanos())
                                .unwrap_or(0)
                                % 200) as u64;
                            // Growing backoff: ~100ms, ~200ms, … per attempt.
                            let backoff_ms = 100 * attempt as u64 + jitter;
                            tracing::warn!(
                                "CVR flush failed ({e}); retry {attempt}/{MAX_FLUSH_ATTEMPTS} in {backoff_ms}ms"
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                            attempt += 1;
                        }
                    }
                };
                // `cvr.flush_attempts` is recorded inside `CVRStore::flush`
                // (rust-cvr otel_metrics::record_flush_attempt, TS cvr-store.ts:1254-1264).
                result.map_err(|e| {
                    // Counted, not just logged: a rising flush-failure rate
                    // (pool exhaustion, ownership churn) is the leading
                    // indicator of the fail_group → reconnect storm.
                    crate::metrics::record_cvr_flush_failure();
                    format!("store flush: {e}")
                })?
            };
            // The store applies the row deltas to its own cache inside `flush`
            // (TS cvr-store.ts:1218), so there is nothing to mirror here.
            if let Some(stats) = &store_flushed {
                // TS cvr-store.ts:1248 `lc.info?.(`flushed cvr@${versionString(cvr.version)}
                // ${JSON.stringify(stats)} in (${elapsed} ms)`)`, with the
                // LogContext TS carries into it (see the note at the captures).
                tracing::info!(
                    appID = %log_app_id,
                    shardNum = log_shard_num,
                    clientGroupID = %log_cg_id,
                    stateVersion = %flushed.version.state_version,
                    cvrFlushID = cvr_flush_id,
                    "flushed cvr@{} {} in ({:.1} ms)",
                    rust_cvr::schema::types::version_string(&flushed.version),
                    serde_json::to_string(stats).unwrap_or_default(),
                    flush_started.elapsed().as_secs_f64() * 1000.0
                );
            }
            Ok(store_flushed.is_some())
            }),
        )
        .await
        .inspect(|&store_flushed| {
            // Record material flushes for the router's ttlClock-interval
            // restart (TS view-syncer.ts:1083-1086 `if (flushed)`).
            if store_flushed {
                self.flush_observed.set(true);
            }
        })
    }

    /// Apply a client's desired-queries change (from `initConnection` /
    /// `changeDesiredQueries`) and hydrate the newly-desired queries.
    ///
    /// This is the Rust-side of TS `#handleConfigUpdate` + `#syncQueryPipelineSet`:
    /// the config-driven pass records the client + desired queries into the CVR
    /// (and, on the client's first appearance, the internal `lmids` /
    /// `mutationResults` queries via `ensure_client`) and pokes the config
    /// patches; the query-driven pass syncs the engine to the CVR's full query
    /// set — read-permission-transforming each client query (internal queries
    /// skip the transform) and hydrating those not already running — then pokes
    /// got-queries + rows.
    #[allow(clippy::too_many_arguments)]
    pub async fn config_and_hydrate(
        &mut self,
        cvr: CVR,
        client_id: &str,
        poke_ws_ids: &[String],
        shard: &ShardID,
        desired_puts: Vec<DesiredQuerySpec>,
        desired_dels: Vec<String>,
        desired_clear: bool,
        client_schema: Option<ClientSchema>,
        custom_query_transform_mode: CustomQueryTransformMode,
        permissions: Option<&serde_json::Value>,
        auth_data: &serde_json::Value,
        custom_ctx: Option<&CustomQueryContext>,
        state_version: String,
        replica_version: String,
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
    ) -> Result<CVR, JsError> {
        self.config_and_hydrate_with_profile(
            cvr,
            client_id,
            poke_ws_ids,
            shard,
            desired_puts,
            desired_dels,
            desired_clear,
            client_schema,
            custom_query_transform_mode,
            None,
            permissions,
            auth_data,
            custom_ctx,
            state_version,
            replica_version,
            last_connect_time,
            last_active,
            ttl_clock,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn config_and_hydrate_with_profile(
        &mut self,
        cvr: CVR,
        client_id: &str,
        poke_ws_ids: &[String],
        shard: &ShardID,
        desired_puts: Vec<DesiredQuerySpec>,
        desired_dels: Vec<String>,
        // A `clear` op removes ALL of the client's desired queries (applied
        // before puts, so a clear+resubscribe patch replaces the whole set).
        desired_clear: bool,
        client_schema: Option<ClientSchema>,
        // Whether this pass re-transforms EVERY custom query or only those
        // missing from the pipeline. TS threads this through
        // `#handleConfigUpdate` -> `#updateCVRConfig` -> `#syncQueryPipelineSet`
        // (view-syncer.ts:1128/1281/1875); rust's orchestrator hands it to
        // `sync_query_pipeline_set` directly (the two TS methods it chains).
        custom_query_transform_mode: CustomQueryTransformMode,
        profile_id: Option<&str>,
        // Read-permission transformation inputs. When `permissions` is `None`
        // (no permissions deployed) client queries are transformed with an
        // EMPTY config — denying every table — per TS view-syncer.ts:1549
        // `currentPermissions().permissions ?? {tables: {}}`.
        permissions: Option<&serde_json::Value>,
        auth_data: &serde_json::Value,
        // Per-connection context for resolving named/custom queries via the
        // user's query API server. `None` when the connection sent no
        // `userQueryURL` (custom queries are then skipped with a warning).
        custom_ctx: Option<&CustomQueryContext>,
        state_version: String,
        replica_version: String,
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
    ) -> Result<CVR, JsError> {
        // Snapshot each connected client's cookie BEFORE any poke advances it.
        // Both the config poke and the hydrate poke call `end()`, which advances
        // `base_version` to the new CVR version; catch-up (below) must replay from
        // these ORIGINAL cookies, not the post-poke ones, or a reconnecting client
        // loses the whole `[oldCookie, current]` interval. See `catchup_clients`.
        let original_client_versions: std::collections::HashMap<String, NullableCVRVersion> = self
            .get_clients(poke_ws_ids)
            .iter()
            .map(|c| (c.ws_id.clone(), c.version()))
            .collect();

        // Client-facing rowKey emission must be keyed by the CLIENT's declared
        // primary key (TS `buildPrimaryKeys(clientSchema)`), not the IVM
        // `keyCmp[0]`. Take the per-table client PKs from the incoming schema,
        // or the one already persisted in the CVR (reconnects send no schema),
        // and install them on the pipelines so the stored rowKey matches what
        // the client indexes by (else `toPrimaryKeyString` throws "Got
        // undefined"). Must run BEFORE `cvr`/`client_schema` are moved below.
        if let Some(cs) = client_schema.as_ref().or(cvr.client_schema.as_ref()) {
            let client_pks = client_primary_keys_from_schema(cs);
            if !client_pks.is_empty() {
                self.pipelines.set_client_primary_keys(client_pks);
            }
        }

        // TS routes every config-bearing message through `#handleConfigUpdate`
        // and then (when pipelines are synced) `#syncQueryPipelineSet`
        // (view-syncer.ts). This orchestrator is the CG-dispatch seat that
        // chains the two 1:1 methods on the serial CG task.
        let cfg_started = std::time::Instant::now();
        let cfg_cvr = self
            .handle_config_update(
                cvr,
                client_id,
                poke_ws_ids,
                shard,
                desired_puts,
                desired_dels,
                desired_clear,
                client_schema,
                profile_id,
                last_connect_time,
                last_active,
                ttl_clock,
            )
            .await?;
        // Phase profiling (SYNCER_TRACE): the config-update phase does query
        // transformation (read-permission rewrite + named/custom-query
        // resolution + flip planning) BEFORE any row fetch. Timing it separately
        // isolates a data-independent per-query planning cost from the
        // fetch/materialize and flush phases below.
        crate::trace::note!(
            "hydrate-config",
            "cg={} config_update_ms={:.1}",
            self.cg_id,
            cfg_started.elapsed().as_secs_f64() * 1000.0
        );
        self.sync_query_pipeline_set(
            cfg_cvr,
            custom_query_transform_mode,
            poke_ws_ids,
            shard,
            permissions,
            auth_data,
            custom_ctx,
            state_version,
            replica_version,
            last_connect_time,
            last_active,
            ttl_clock,
            original_client_versions,
        )
        .await
    }

    /// Record the client + its desired-query changes into the CVR and poke the
    /// config patches. Port of TS `ViewSyncerService.#handleConfigUpdate` /
    /// `#updateCVRConfig` (view-syncer.ts) — the config-driven half of every
    /// initConnection / changeDesiredQueries / deleteClients cycle.
    #[allow(clippy::too_many_arguments)]
    async fn handle_config_update(
        &mut self,
        cvr: CVR,
        client_id: &str,
        poke_ws_ids: &[String],
        shard: &ShardID,
        desired_puts: Vec<DesiredQuerySpec>,
        desired_dels: Vec<String>,
        desired_clear: bool,
        client_schema: Option<ClientSchema>,
        profile_id: Option<&str>,
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
    ) -> Result<CVR, JsError> {
        // ── Phase 1: config-driven — record client + desired queries. ──
        let mut cfg = CVRConfigDrivenUpdater::new(cvr, shard.clone());
        cfg.ensure_client(client_id);
        if let Some(cs) = client_schema {
            cfg.set_client_schema(cs)?;
        }
        if let Some(profile_id) = profile_id {
            cfg.set_profile_id(profile_id);
        }
        // A `clear` drops every desired query for the client first (TS
        // `#patchQueries` → `clearDesiredQueries`); puts below then establish the
        // new set.
        let mut config_patches = if desired_clear {
            cfg.clear_desired_queries(client_id)
        } else {
            Vec::new()
        };
        config_patches.extend(cfg.put_desired_queries(client_id, &desired_puts));
        if !desired_dels.is_empty() {
            // A client `del` marks the query inactive with its TTL (so a quick
            // resubscribe is free); the query keeps running until the TTL
            // scheduler expires it. This mirrors TS `#patchQueries` mapping
            // `del` → `markDesiredQueriesAsInactive` (NOT a hard delete).
            config_patches.extend(cfg.mark_desired_queries_as_inactive(
                client_id,
                &desired_dels,
                ttl_clock,
            ));
        }
        let (mut cfg_cvr, _stats) = cfg.flush(last_connect_time, last_active, ttl_clock);
        let expected_current_version = cfg.base.orig.version.clone();
        let cfg_ops = cfg.base.drain_store_ops();
        {
            // TS `#updateCVRConfig` pokes only clients at the pre-config CVR
            // version. A lagging reconnect must stay on its old cookie until
            // `catchup_clients`; advancing it here would make every catch-up
            // patch look stale and silently drop the missed rows.
            // ORDER IS THE PORT (AGENTS.md rule 8). TS `#updateCVRConfig`
            // FLUSHES FIRST and only then pokes, guarded on the version having
            // actually advanced:
            //
            //   this.#cvr = await this.#flushUpdater(lc, updater);
            //   if (cmpVersions(cvr.version, this.#cvr.version) < 0) {
            //     const pokers = startPoke(this.#getClients(cvr.version), newCVR.version);
            //     ... addPatch ... ; await pokers.end(newCVR.version);
            //   }
            //
            // (view-syncer.ts:1140-1159). Rust used to open the pokers BEFORE
            // the flush, at the optimistically bumped `cfg_cvr.version`, admit
            // the config patches at that version, and only then discover the
            // flush was a no-op and revert to `orig`. `end(orig)` then found
            // `base == final` and CLOSED the client with `Patches were sent but
            // finalVersion ... is not greater than baseVersion` — 256 of 256
            // such closes in a replay came from this site, against ZERO on the
            // TS arm running the same trace. Flushing first makes the failure
            // structurally unreachable: when nothing was persisted the version
            // did not advance, the guard is false, and no poke is opened at a
            // version that is about to be thrown away.
            let cfg_arc = Arc::new(cfg_cvr);
            let store_flushed = self
                .flush_ops_to_store(
                    cfg_ops,
                    &expected_current_version,
                    cfg_arc.clone(),
                    last_connect_time,
                )
                .await?;
            // No-op store flush → stay on the ORIGINAL CVR (TS `flush` returns
            // `this._orig`): nothing was persisted, so adopting the bumped
            // working copy would advance client cookies past the stored version
            // and fail the next material flush's version guard.
            cfg_cvr = if store_flushed {
                Arc::try_unwrap(cfg_arc).unwrap_or_else(|a| (*a).clone())
            } else {
                cfg.base.orig.clone()
            };
            // TS `cmpVersions(cvr.version, this.#cvr.version) < 0` — poke only
            // when the flushed CVR is strictly ahead of the pre-config version.
            if cmp_versions(
                &Some(expected_current_version.clone()),
                &Some(cfg_cvr.version.clone()),
            ) == std::cmp::Ordering::Less
            {
                // TS `#getClients(cvr.version)` — clients at the PRE-config
                // version; a lagging reconnect stays on its old cookie until
                // `catchup_clients`.
                let clients = Self::config_poke_targets(
                    self.get_clients(poke_ws_ids),
                    &expected_current_version,
                );
                let client_refs: Vec<&ClientHandler> = clients.iter().map(|c| c.as_ref()).collect();
                let pokers = MultiPoker::new(&client_refs, cfg_cvr.version.clone(), "config-cvr");
                for p in &config_patches {
                    pokers.add_patch(p);
                }
                pokers.end(cfg_cvr.version.clone());
            }
        }
        // TS `#handleConfigUpdate` arms the eviction timer for the updated CVR's
        // inactive queries at its tail (view-syncer.ts:1390).
        self.schedule_expire_eviction(&cfg_cvr);
        Ok(cfg_cvr)
    }

    /// Sync the pipeline set to the CVR's FULL query set (transform, add/remove,
    /// hydrate, poke, catch up). Port of TS
    /// `ViewSyncerService.#syncQueryPipelineSet` (view-syncer.ts).
    #[allow(clippy::too_many_arguments)]
    async fn sync_query_pipeline_set(
        &mut self,
        cfg_cvr: CVR,
        custom_query_transform_mode: CustomQueryTransformMode,
        poke_ws_ids: &[String],
        shard: &ShardID,
        permissions: Option<&serde_json::Value>,
        auth_data: &serde_json::Value,
        custom_ctx: Option<&CustomQueryContext>,
        state_version: String,
        replica_version: String,
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
        original_client_versions: std::collections::HashMap<String, NullableCVRVersion>,
    ) -> Result<CVR, JsError> {
        // TS `#syncQueryPipelineSet` first runs `#hydrateUnchangedQueries`
        // (view-syncer.ts:592/1449) — a PROACTIVE re-hydrate of every
        // already-gotten same-hash query each sync, to drift-check still-alive
        // pipelines. That is ported below as `hydrate_unchanged_queries`, called
        // once `executed` is built. PERF: it re-executes every alive same-hash
        // pipeline on every sync (TS's design) — a serving-path cost that must be
        // confirmed by a release-gate replay before deploy; it changes no client-observable
        // output. Its `drifted_query_ids` feed the `hydrate_and_sync` force-bump
        // reason label.
        //
        // ── Phase 2: query-driven — sync the pipeline to the CVR's FULL query
        // set (port of TS `#syncQueryPipelineSet`). The executed set is derived
        // from `cfg_cvr.queries` — which, after `ensure_client`, includes the
        // internal `lmids` / `mutationResults` queries — NOT just the incoming
        // desired puts. We add only queries missing from the pipeline (so a
        // config change re-hydrates nothing already running); expired-query
        // removal is driven separately by the TTL scheduler.
        //
        // Read-permission transformation: each executed query uses the
        // TRANSFORMED ast + `hashOfAST(transformed)` as its transformation hash.
        // Internal queries SKIP the transform (TS
        // `transformAndHashQuery(..., internalQuery=true)`), using the raw ast.
        // We recompute every CVR query's transformed AST + hash and compare it
        // to what the pipeline is currently running. A query is (re-)hydrated
        // when it is missing OR its transformation hash changed — the latter
        // happens when `authData` changes (updateAuth) and the read-permission
        // rules expand differently. A changed-hash query has its old pipeline
        // torn down first (`remove_query`, WITHOUT a CVR got-query del — the
        // query is still desired) then re-added, mirroring TS
        // `PipelineManager.addQuery(id, differentHash)`.
        // First compute each query's transformed AST + hash. Internal queries
        // use the raw ast; client queries go through the read-permission
        // transform; custom (named) queries are resolved in a single batch call
        // to the user's query API server (`custom_ctx`).
        // Start of the query-driven hydration span for the
        // `zero.sync.view_syncer_hydration` histogram (TS `start` at the top of
        // `#syncQueryPipelineSet`); recorded below only when ≥1 query hydrated.
        let hydration_start = std::time::Instant::now();
        // Port of TS view-syncer.ts:1549/:1929:
        //   `must(this.#pipelines.currentPermissions()).permissions ?? {tables: {}}`
        // — when NO permissions doc is deployed, TS still TRANSFORMS every
        // client query with an EMPTY config, which deny-by-defaults every
        // table (transformQueryInternal adds the empty-OR FALSE sentinel).
        // Passing the AST through untransformed here was a fail-OPEN data
        // leak (served the full table; caught by the release-gate data differential, b4754f12d).
        let empty_permissions = serde_json::json!({"tables": {}});
        // `this.#pipelines.currentPermissions()` at USE time (view-syncer.ts:1933;
        // AGENTS rule 9 freshness): the doc is re-read through the pinned
        // snapshot and swapped in only when its hash changed. The `permissions`
        // parameter stays authoritative for snapshotter-less (test) engines.
        let refreshed_permissions: Option<serde_json::Value> = match self
            .pipelines
            .current_permissions(&self.app_id, self.permissions_hash.as_deref())
        {
            Some(crate::auth::load_permissions::PermissionsReload::Changed {
                permissions,
                hash,
            }) => {
                tracing::info!(
                    "CG {}: read-permissions changed (hash {:?} → {:?}); transforming with the new doc",
                    self.cg_id,
                    self.permissions_hash,
                    hash
                );
                self.permissions = permissions.clone();
                self.permissions_hash = hash;
                crate::metrics::Metrics::inc(&self.metrics.permission_reloads);
                permissions
            }
            Some(crate::auth::load_permissions::PermissionsReload::Unchanged) => {
                self.permissions.clone()
            }
            None => None,
        };
        let permissions: Option<&serde_json::Value> =
            refreshed_permissions.as_ref().or(permissions);
        let mut executed: Vec<(String, serde_json::Value, String)> = Vec::new();
        // TS `erroredQueryIDs` = `#processTransformedCustomQueries` return value
        // (`appQueryErrors.map(q => q.id)`, view-syncer.ts:1723) → joined into
        // `removeQueriesQueryIds` (:2066) so the errored query is removed from
        // the CVR and its got-`del` is poked in this pass.
        let mut errored_query_ids: Vec<String> = Vec::new();
        // TS `customQueries` — the CVR's FULL custom-query set (view-syncer.ts:1899).
        let mut custom_queries: Vec<CustomQuerySpec> = Vec::new();
        for (qid, record) in &cfg_cvr.queries {
            match record {
                QueryRecord::Internal(r) => {
                    executed.push((qid.clone(), r.ast.clone(), hash_of_ast(&r.ast)));
                }
                QueryRecord::Client(r) => {
                    let perms = permissions.unwrap_or(&empty_permissions);
                    let (ast, hash) = transform_and_hash_query(&r.ast, perms, auth_data, false);
                    executed.push((qid.clone(), ast, hash));
                }
                QueryRecord::Custom(r) => custom_queries.push(CustomQuerySpec {
                    id: qid.clone(),
                    name: r.name.clone(),
                    args: r.args.clone(),
                }),
            }
        }

        // TS emits this warning off the FULL custom-query set, BEFORE the mode
        // filter (view-syncer.ts:1948-1952): a client asking for named queries
        // when no query URL is configured is warned regardless of what this pass
        // would actually re-transform.
        if !custom_queries.is_empty() && custom_ctx.is_none() {
            // TS view-syncer.ts:1496 — exact text; the count is a rust-side detail.
            tracing::warn!(
                skipped = custom_queries.len(),
                "Custom/named queries were requested but no `ZERO_QUERY_URL` is configured for Zero Cache."
            );
        }

        // Port of TS `customQueriesToTransform` (view-syncer.ts:1954-1959):
        //   customQueryTransformMode === 'all'
        //     ? [...customQueries.values()]
        //     : [...customQueries.values()].filter(q => !this.#pipelines.queries().has(q.id))
        // `All` re-transforms every custom query — TS does this on new
        // connections / `updateAuth` / background retransform so the user's API
        // server re-authorizes against the current auth context. `Missing`
        // re-transforms only queries that are not already hydrated, so a
        // steady-state pass costs no API round-trip for queries already running.
        // Skipping the filter made every sync re-transform the whole custom set.
        let custom_queries_to_transform: Vec<CustomQuerySpec> = match custom_query_transform_mode {
            CustomQueryTransformMode::All => custom_queries,
            CustomQueryTransformMode::Missing => custom_queries
                .into_iter()
                .filter(|q| !self.pipelines.has_query(&q.id))
                .collect(),
        };

        // Resolve custom queries against the API server. Per-query errors are
        // forwarded to the client as `transformError` (healthy queries proceed);
        // a whole-request failure fails the connection with the transform error.
        // TS: `if (customQueryTransformer && customQueriesToTransform.length > 0)`
        // (view-syncer.ts:1960) — no transformer configured means no transform at
        // all (it already warned above).
        if let Some(ctx) = custom_ctx
            && !custom_queries_to_transform.is_empty()
        {
            let mut transform_errors: Vec<serde_json::Value> = Vec::new();
            {
                // TS wraps the transform in try/catch/finally, recording
                // `zero.sync.query.transformations{result}` + timing
                // (view-syncer.ts:1782-1789). Time the API-server round-trip
                // and tag the outcome; the histogram observes on both paths.
                let transform_started = std::time::Instant::now();
                // TS view-syncer.ts:1970-1976: `this.#runPriorityOp(lc,
                // '#syncQueryPipelineSet transforming custom queries', ...)`.
                let transform_result = run_priority_op(
                    "#syncQueryPipelineSet transforming custom queries",
                    transform(ctx, shard, &custom_queries_to_transform),
                )
                .await;
                let transform_ms = transform_started.elapsed().as_secs_f64() * 1000.0;
                crate::trace::note!(
                    "transform",
                    "cg={} queries={} transform_ms={:.1} ok={}",
                    self.cg_id,
                    custom_queries_to_transform.len(),
                    transform_ms,
                    transform_result.is_ok()
                );
                crate::metrics::record_query_transformation_time(transform_ms);
                crate::metrics::record_query_transformation(transform_result.is_ok());
                match transform_result {
                    Ok(response) => {
                        // TS view-syncer.ts:1984-1992: an UNCACHED transform
                        // validates the connection with the API server's
                        // authoritative userID (a cached batch re-asserted
                        // nothing). `validateConnection` THROWS on a userID
                        // mismatch (`Unauthorized`); TS's `#runInLockForClient`
                        // catch then fails THAT client (`failConnection` +
                        // `client.fail`) and none of the batch is applied —
                        // existing pipelines stay intact. Rust cannot unwind
                        // across the serial re-hydrate, so it fails the
                        // connection here and stashes the body like the
                        // whole-batch failure arm below.
                        let mut rejected: Option<crate::protocol::ErrorBody> = None;
                        if !response.cached
                            && let Some(validation) = &response.validation
                        {
                            let sel = CcmConnectionSelector {
                                client_id: ctx.client_id.clone(),
                                ws_id: ctx.ws_id.clone(),
                            };
                            let recorded = lock_unpoisoned(&self.ccm).validate_connection(
                                &sel,
                                ctx.revision,
                                validation,
                            );
                            if let Err(e) = recorded {
                                rejected = Some(e.to_error_body());
                            }
                        }
                        if let Some(err) = rejected {
                            tracing::warn!(
                                cg_id = %self.cg_id,
                                client_id = %ctx.client_id,
                                ws_id = %ctx.ws_id,
                                revision = ctx.revision,
                                message = %err.message(),
                                "Connection auth validation failed; invalidating connection"
                            );
                            let conn = lock_unpoisoned(&self.ccm).get_connection_context(
                                &CcmConnectionSelector {
                                    client_id: ctx.client_id.clone(),
                                    ws_id: ctx.ws_id.clone(),
                                },
                            );
                            if let Some(conn) = conn {
                                self.fail_maintenance_connection(&conn, err.clone());
                            }
                            self.background_retransform_failure =
                                Some(serde_json::to_value(&err).unwrap_or_default());
                        } else {
                            for r in response.result {
                                match r {
                                    CustomTransformed::Ok(tq) => {
                                        executed.push((tq.id, tq.ast, tq.hash))
                                    }
                                    CustomTransformed::Errored { id, error } => {
                                        errored_query_ids.push(id);
                                        record_transform_error(error, &mut transform_errors)
                                    }
                                }
                            }
                        }
                    }
                    Err(failed) => {
                        // Whole-batch failure (TS throws `TransformFailed` →
                        // the client fails). Surface it and leave existing
                        // pipelines intact.
                        self.send_query_transform_error_to_clients(
                            &cfg_cvr.queries,
                            QueryTransformErrors::Failed(&failed),
                        );
                        // Record the whole-batch failure body for a background
                        // retransform to branch on (TS `#syncQueryPipelineSet`
                        // THROWS here, view-syncer.ts:1983; rust cannot unwind
                        // across the serial re-hydrate so it stashes the body).
                        // Only `run_background_retransform` reads this — it
                        // resets the cell before its re-hydrate — so setting it
                        // on the init/changeDesiredQueries path is a harmless
                        // no-op there. See `background_retransform_failure`.
                        self.background_retransform_failure = Some(failed);
                    }
                }
            }
            if !transform_errors.is_empty() {
                self.send_query_transform_error_to_clients(
                    &cfg_cvr.queries,
                    QueryTransformErrors::Application(&transform_errors),
                );
            }
        }

        // Port of TS `#hydrateUnchangedQueries` (view-syncer.ts:592): PROACTIVELY
        // re-hydrate already-gotten same-hash queries and drift-check them against
        // the CVR-stored signature. Non-drifted ones keep their rebuilt pipeline
        // (the loop below then skips them — no bump); drifted ones are removed here
        // and fall through to the loop as `None` (re-added → re-executed via the
        // updater path, with the force-bump reason keyed off this drifted set).
        //
        // TS runs `#hydrateUnchangedQueries` ONCE, in the run-loop init block gated
        // by `#pipelinesSynced` (view-syncer.ts:568-606); subsequent connects /
        // changeDesiredQueries call `#syncQueryPipelineSet('missing')` WITHOUT it.
        // Rust folds config-update + init + missing-add into this one method (called
        // from `config_and_hydrate` on every connect), so reproduce TS's semantics
        // by gating on `pipelines_synced`: run the proactive re-hydrate only on the
        // first sync after (re)init. Skipping it on later syncs does NOT skip
        // new-query hydration — the drift/add loop below still hydrates any query
        // missing from the pipeline (TS `'missing'` mode). Without this gate a big
        // CG re-materialized every alive pipeline on every reconnect (whale
        // client-group hydrates of 20-88 s).
        let drifted_query_ids = if self.pipelines_synced {
            std::collections::HashSet::new()
        } else {
            #[cfg(test)]
            {
                self.hydrate_unchanged_runs += 1;
            }
            self.hydrate_unchanged_queries(&cfg_cvr, &executed, &errored_query_ids, &state_version)
                .await?
        };

        // Drift check: (re-)hydrate a query when it is missing OR its
        // transformation hash changed (auth re-transform / a new custom AST).
        // Port of TS `removeQueriesQueryIds` (view-syncer.ts:2062-2067): the
        // expired queries (`expired(ttlClock, q)`) plus the errored custom
        // queries, removed in THIS sync pass — `trackQueries(removed)` deletes
        // them from the CVR and pokes their got-`del`. The TTL scheduler pass
        // (`remove_expired_queries`) only guarantees a pass runs at expiry.
        let mut remove_queries: Vec<String> = rust_cvr::cvr::get_inactive_queries(&cfg_cvr)
            .into_iter()
            .filter(|q| q.inactivated_at + q.ttl <= ttl_clock)
            .map(|q| q.hash)
            .collect();
        // Counted before the loop moves the Vec — TS reads
        // `erroredQueryIDs?.length ?? 0` for its summary line below.
        let errored_count = errored_query_ids.len();
        for id in errored_query_ids {
            if !remove_queries.contains(&id) {
                remove_queries.push(id);
            }
        }
        let mut add_queries: Vec<(String, String)> = Vec::new();
        let mut queries: Vec<(String, String)> = Vec::new();
        // The to-be-hydrated queries with their parsed ASTs, kept for the
        // shadow-mode covering pass below (avoids re-parsing the JSON strings).
        let mut covering_candidates: Vec<(String, serde_json::Value, String)> = Vec::new();
        let mut retransform_removes: Vec<String> = Vec::new();
        // TS scopes `query.transformation-{hash-changes,no-ops}` to custom
        // queries with an existing CVR transform hash (view-syncer.ts:1818-1843).
        // The `Some(_)` arms below imply an existing hash; gate on custom id to
        // match TS (internal/client re-transforms are not counted here).
        let custom_ids: std::collections::HashSet<&str> = custom_queries_to_transform
            .iter()
            .map(|s| s.id.as_str())
            .collect();
        for (qid, transformed_ast, transformation_hash) in executed {
            // TS `addQueries` filter (view-syncer.ts:2074-2075): a query in the
            // removal set is never (re-)added in the same pass.
            if remove_queries.contains(&qid) {
                continue;
            }
            let is_custom = custom_ids.contains(qid.as_str());
            // Owned copy: `check_for_thrashing` needs `&mut self` inside the
            // changed-hash arm, which a live `&self.pipelines` borrow forbids.
            let running_hash = self
                .pipelines
                .query_transformation_hash(&qid)
                .map(str::to_string);
            match running_hash {
                // Already running with this exact transform → nothing to do.
                Some(h) if h == transformation_hash => {
                    if is_custom {
                        crate::metrics::record_query_transformation_no_op();
                    }
                    continue;
                }
                // Running with a DIFFERENT transform → drift: tear the old
                // pipeline down before re-hydrating with the new transform.
                Some(old_hash) => {
                    // TS `lc.info?.(`Query ${queryID} transformation changed:
                    // ${oldHash} -> ${newHash}`)` (view-syncer.ts:2048-2050),
                    // logged BEFORE the thrash check.
                    tracing::info!(
                        "Query {qid} transformation changed: {old_hash} -> {transformation_hash}"
                    );
                    if is_custom {
                        // TS order: `#checkForThrashing(queryID)` THEN
                        // `#queryTransformationHashChanges.add(1)`.
                        self.check_for_thrashing(&qid);
                        crate::metrics::record_query_transformation_hash_change();
                    }
                    retransform_removes.push(qid.clone());
                }
                // Not hydrated → a normal add.
                None => {}
            }
            if self.enable_query_covering {
                covering_candidates.push((
                    qid.clone(),
                    transformed_ast.clone(),
                    transformation_hash.clone(),
                ));
            }
            add_queries.push((qid.clone(), transformation_hash));
            queries.push((qid, transformed_ast.to_string()));
        }
        // TS `lc.info?.(`syncQueryPipelineSet: ${cvrQueryEntires.length} CVR
        // queries, ${customQueriesToTransform.length} custom re-transformed,
        // ${erroredQueryIDs?.length ?? 0} errored, ${removeQueriesQueryIds.size}
        // to remove, ${addQueries.length} to add`)` (view-syncer.ts:2082-2088).
        tracing::info!(
            "syncQueryPipelineSet: {} CVR queries, {} custom re-transformed, \
             {errored_count} errored, {} to remove, {} to add",
            cfg_cvr.queries.len(),
            custom_queries_to_transform.len(),
            remove_queries.len(),
            add_queries.len()
        );
        // Tear down drifted pipelines directly (no CVR removal — the query is
        // still desired; only its compiled pipeline is rebuilt). TS does this
        // inside `addQuery` as `removeQuery(queryID, 'replace-query')`
        // (pipeline-driver.ts:606), so the stop reason is `replace-query`.
        for qid in &retransform_removes {
            self.pipelines.remove_query(qid, "replace-query");
        }

        // Shadow-mode query covering (#6182): seeded lazily and AFTER the
        // drifted-pipeline teardown — TS builds its index from
        // `pipelines.queries()` after `removeQuery` runs, so a stale drifted
        // AST never acts as a covering query; and a config change that hydrates
        // nothing skips the (parse + normalize all running ASTs) cost entirely.
        // Purely observational — no effect on what is served.
        if self.enable_query_covering && !covering_candidates.is_empty() {
            let mut idx = QueryCoveringIndex::new();
            for (qid, ast_json, hash) in self.pipelines.running_queries() {
                match serde_json::from_str::<serde_json::Value>(&ast_json) {
                    Ok(ast) => {
                        let q = RunningQuery {
                            transformed_ast: ast,
                            transformation_hash: hash,
                            query_name: query_name_of(&cfg_cvr, &qid),
                        };
                        let normalized =
                            crate::auth::read_authorizer::normalize_ast(&q.transformed_ast);
                        idx.add(&qid, normalized, &q);
                    }
                    Err(e) => {
                        tracing::warn!("query covering: unparseable stored AST for {qid}: {e}")
                    }
                }
            }
            let mut total_hydrated_queries = 0usize;
            let mut covered_hydrated_queries = 0usize;
            let mut first_covered: Option<QueryCoverageShadowHit> = None;
            for (qid, transformed_ast, transformation_hash) in &covering_candidates {
                let query_name = query_name_of(&cfg_cvr, qid);
                total_hydrated_queries += 1;
                // Normalized ONCE and used for both the lookup and the
                // insert below. TS gets that for free from `normalizeAST`'s
                // WeakMap memo (zero-protocol/src/ast.ts:432-450); rust has no
                // AST object identity to key one on, so the memo lives here.
                // See `QueryCoveringIndex::add`.
                let normalized_ast = crate::auth::read_authorizer::normalize_ast(transformed_ast);
                if let Some(cov) = idx.find_covering_query(qid, &normalized_ast) {
                    covered_hydrated_queries += 1;
                    if first_covered.is_none() {
                        first_covered = Some(QueryCoverageShadowHit {
                            covered_query_hash: qid.clone(),
                            covered_transformation_hash: transformation_hash.clone(),
                            covered_query_name: query_name.clone(),
                            covering_query_hash: cov.query_id,
                            covering_transformation_hash: cov.transformation_hash,
                            covering_query_name: cov.query_name,
                        });
                    }
                }
                idx.add(
                    qid,
                    normalized_ast,
                    &RunningQuery {
                        transformed_ast: transformed_ast.clone(),
                        transformation_hash: transformation_hash.clone(),
                        query_name,
                    },
                );
            }
            // TS `#logQueryCoverageShadowSummary`, hydrationPath 'add'.
            crate::services::view_syncer::query_covering::log_shadow_summary(
                &shard.app_id,
                shard.shard_num,
                &cfg_cvr.id,
                "add",
                total_hydrated_queries,
                covered_hydrated_queries,
                first_covered.as_ref(),
            );
        }

        // Mirror TS `#syncQueryPipelineSet`'s terminal branch: when there are
        // queries to hydrate, hydrate them (poking their full state up to the
        // new version) and THEN catch reconnecting clients up on everything
        // else (excluding the just-hydrated queries, whose state was already
        // fully poked). When nothing needs hydrating, skip straight to catchup —
        // a reconnecting client with an old cookie still needs the row/config
        // patches between its cookie and the current CVR version.
        //
        // The pipeline set is now (re)synced for this CG — port of TS
        // `#pipelinesSynced = true` at the end of the run-loop init block
        // (view-syncer.ts:606). Gates the once-per-init `hydrate_unchanged_queries`
        // above; reset to `false` by `reset_pipelines_and_rehydrate` (TS
        // `#pipelines.reset()` + `#pipelinesSynced = false`, view-syncer.ts:575-576).
        self.pipelines_synced = true;
        if add_queries.is_empty() && remove_queries.is_empty() {
            self.catchup_clients(
                &cfg_cvr,
                &cfg_cvr.version,
                &[],
                poke_ws_ids,
                &original_client_versions,
            )
            .await?;
            Ok(cfg_cvr)
        } else {
            let excluded: Vec<String> = add_queries.iter().map(|(id, _)| id.clone()).collect();
            // TS `#catchupClients(lc, cvr, finalVersion, addQueries ids, pokers)`
            // (view-syncer.ts:2350-2356): `cvr` is the PRE-hydrate snapshot, so
            // the catch-up scan's upper bound is the version BEFORE this pass
            // (config patches ≤ it) while `current` is the flushed final
            // version. Bounding at the final version replayed the got-`put`
            // just tracked in this pass as a duplicate entry.
            let pre_hydrate_version = cfg_cvr.version.clone();
            // TS `#addAndRemoveQueries`'s own `const start = performance.now()`
            // (view-syncer.ts:2168) — a SECOND clock, distinct from
            // `hydration_start` (`#syncQueryPipelineSet`'s, :1880). It is the
            // one the `finished processing queries` line reports as `wall`, and
            // it spans hydrate + deleteUnreferencedRows + flushUpdater +
            // catchupClients + pokeEnd. Rust reported `fetch_started.elapsed()`
            // there — the fetch loop ALONE — so the same message meant a
            // different span on each engine and, worse, hid the CVR flush,
            // catch-up and poke from the one line built to expose them.
            let add_and_remove_start = std::time::Instant::now();
            let (result, pokers) = self
                .hydrate_and_sync(
                    cfg_cvr,
                    state_version,
                    replica_version,
                    &add_queries,
                    // TS `removeQueries` (view-syncer.ts:2062-2067): expired +
                    // errored, tracked as removed in this pass (got `del` poked).
                    &remove_queries,
                    poke_ws_ids,
                    &queries,
                    last_connect_time,
                    last_active,
                    ttl_clock,
                    &drifted_query_ids,
                )
                .await?;
            // Catch-up rides the SAME poke as the hydrate (TS shape: catchup
            // before pokeEnd). Each poker's live per-client base filter delivers
            // exactly the patches that client hasn't seen; ending the hydrate
            // poke first (the previous shape) advanced every base to the new
            // version and made a separate catch-up poke inert — a reconnecting
            // client silently lost the whole `(oldCookie, current]` interval.
            let clients = self.get_clients(poke_ws_ids);
            let catchup_from =
                Self::catchup_floor(&pre_hydrate_version, &clients, &original_client_versions);
            let catchup_started = std::time::Instant::now();
            let patches = self
                .gather_catchup_patches(
                    &pre_hydrate_version,
                    &result.cvr.version,
                    &excluded,
                    catchup_from,
                )
                .await?;
            crate::trace::note!(
                "catchup",
                "cg={} patches={} catchup_ms={:.1}",
                self.cg_id,
                patches.len(),
                catchup_started.elapsed().as_secs_f64() * 1000.0
            );
            for p in &patches {
                pokers.add_patch(p);
            }
            pokers.end(result.cvr.version.clone());
            // TS view-syncer.ts:2364-2366 `lc.info?.(`finished processing
            // queries (process: ${totalProcessTime} ms, wall: ${wallTime}
            // ms)`)` — emitted HERE, at the end of the add/remove span, with
            // `wall` measured from that span's own start.
            tracing::info!(
                cg_id = %self.cg_id,
                "finished processing queries (process: {:.1} ms, wall: {:.1} ms)",
                result.process_time_ms,
                add_and_remove_start.elapsed().as_secs_f64() * 1000.0
            );
            // TS `#viewSyncerHydration.recordMs(performance.now() - start)` —
            // recorded once per sync that hydrated ≥1 query, after pokeEnd +
            // catchup.
            crate::metrics::record_view_syncer_hydration(
                hydration_start.elapsed().as_secs_f64() * 1000.0,
            );
            Ok(result.cvr)
        }
    }

    /// Catch reconnecting clients up on the row + config patches they missed
    /// while disconnected. Port of TS `ViewSyncer.#catchupClients`.
    ///
    /// A client reconnects presenting a base cookie that may be older than the
    /// CVR's current version. The hydrate/advance pokes only cover the delta
    /// from the CVR version they were computed against; the patches a client
    /// missed while away — for queries that already existed in the group — must
    /// be replayed from the CVR store's `rows` / `desires` history. We compute
    /// the oldest connected client's cookie (`catchupFrom`), stream the row
    /// patches in `(catchupFrom, current]` from the row-record cache (rebuilding
    /// PUT contents from the live engine via `getRow`, or emitting a DEL when
    /// the stored `refCounts` is null), then append the config patches, and poke
    /// the whole set at `cvr.version`. `exclude_query_hashes` skips queries whose
    /// full state a hydrate just poked (they need no replay).
    ///
    /// No-op when there is no CVR store (dev/tests) — the patches live in PG.
    /// Clients eligible for an advance-delta poke: exactly those whose cookie is
    /// the pre-advance `cvr_version`. A lagging client (behind that version) must
    /// be excluded — its cookie doesn't match the delta poke's baseCookie, so
    /// applying the delta would skip the `[clientCookie, cvr_version]` gap; it is
    /// instead caught up on its next `initConnection`. Port of TS
    /// `#advancePipelines`, which pokes `#getClients(cvr.version)`. Split out so
    /// the exclusion is unit-testable.
    fn advance_poke_targets(
        clients: Vec<Arc<ClientHandler>>,
        cvr_version: &CVRVersion,
    ) -> Vec<Arc<ClientHandler>> {
        clients
            .into_iter()
            .filter(|c| c.version() == Some(cvr_version.clone()))
            .collect()
    }

    /// Config-poke targets mirror TS `#getClients(cvr.version)`: a client with
    /// no cookie is treated as being at `EMPTY_CVR_VERSION`, while a reconnect
    /// with an older cookie is excluded and caught up after pipeline sync.
    fn config_poke_targets(
        clients: Vec<Arc<ClientHandler>>,
        cvr_version: &CVRVersion,
    ) -> Vec<Arc<ClientHandler>> {
        clients
            .into_iter()
            .filter(|client| {
                let version = client
                    .version()
                    .unwrap_or_else(|| EMPTY_CVR_VERSION.clone());
                cmp_versions(&Some(version), &Some(cvr_version.clone()))
                    == std::cmp::Ordering::Equal
            })
            .collect()
    }

    /// The catch-up floor: `min(cvr_version, min over clients of their ORIGINAL
    /// cookie)`. A client's original cookie comes from `original_versions` (the
    /// cycle-start snapshot); only if a client is absent there do we fall back to
    /// its live `version()`. Split out so the original-vs-live selection — the
    /// crux of the reconnect-catch-up fix — is unit-testable without a store.
    fn catchup_floor(
        cvr_version: &CVRVersion,
        clients: &[Arc<ClientHandler>],
        original_versions: &std::collections::HashMap<String, NullableCVRVersion>,
    ) -> NullableCVRVersion {
        let mut floor: NullableCVRVersion = Some(cvr_version.clone());
        for c in clients {
            let v = original_versions
                .get(&c.ws_id)
                .cloned()
                .unwrap_or_else(|| c.version());
            if cmp_versions(&v, &floor) == std::cmp::Ordering::Less {
                floor = v;
            }
        }
        floor
    }

    pub async fn catchup_clients(
        &mut self,
        cvr: &CVR,
        current: &CVRVersion,
        exclude_query_hashes: &[String],
        poke_ws_ids: &[String],
        // Each connected client's cookie as of the START of this config/hydrate
        // cycle (keyed by ws_id), captured BEFORE any poke advanced it. Using the
        // client's live `version()` here instead would be wrong: the config and
        // hydrate pokes' `end()` already advanced `base_version` to the new CVR
        // version, so the catch-up interval would collapse to `[current, current]`
        // and a reconnecting client would silently lose every patch between its
        // real cookie and now. TS `#catchupClients` runs before `pokeEnd`, i.e.
        // against the un-advanced cookies — this snapshot reproduces that.
        original_versions: &std::collections::HashMap<String, NullableCVRVersion>,
    ) -> Result<(), String> {
        // TS has no client-count guard: `startPoke([], …)` is a poker with no
        // targets, the patches are still gathered, and `#markVersionServed`
        // still runs (view-syncer.ts:2399-2400, 2464-2467). Returning early on
        // an empty set skipped the served mark — the serving-lag observation
        // and `servedVersion` TS records for a group whose last client has
        // just dropped.
        let clients = self.get_clients(poke_ws_ids);

        // TS creates the pokers BEFORE gathering any patches
        // (view-syncer.ts:2400 `const pokers = usePokers ?? startPoke(...)`) and
        // ends them unconditionally (2464-2467) — there is no empty-patch-set
        // early return. That is what delivers the forced empty initial poke to a
        // client with nothing to catch up on, so it "can learn its got-queries
        // state has been reconciled with the server" (client-handler.ts:123).
        // Hoisting this above the floor computation is safe: `start_poke` never
        // advances `base_version` (only `end()` does), so `catchup_from` is
        // unaffected.
        let client_refs: Vec<&ClientHandler> = clients.iter().map(|c| c.as_ref()).collect();
        let pokers = MultiPoker::new(&client_refs, cvr.version.clone(), "catchup-clients");

        // catchupFrom = min(cvr.version, min over connected clients' ORIGINAL
        // cookies). Port of `clients.map(c => c.version()).reduce(min, cvr.version)`
        // — but against the cycle-start snapshot, since each client's live
        // `version()` has already been advanced by the config/hydrate pokes.
        let catchup_from = Self::catchup_floor(&cvr.version, &clients, original_versions);
        let catchup_started = std::time::Instant::now();
        let patches = self
            .gather_catchup_patches(&cvr.version, current, exclude_query_hashes, catchup_from)
            .await?;
        crate::trace::note!(
            "catchup",
            "cg={} patches={} catchup_ms={:.1}",
            self.cg_id,
            patches.len(),
            catchup_started.elapsed().as_secs_f64() * 1000.0
        );

        for p in &patches {
            pokers.add_patch(p);
        }
        pokers.end(cvr.version.clone());
        // TS `this.#markVersionServed(cvr.version)` runs right after
        // `pokers.end(...)` in the same `!usePokers` branch (view-syncer.ts:2466).
        self.mark_version_served(&cvr.version);
        Ok(())
    }

    /// Build the catch-up patch set (row patches first, then config patches —
    /// matching TS ordering) WITHOUT poking. The hydrate path appends these to
    /// its still-open poke; the standalone [`catchup_clients`] wraps them in
    /// their own poke. Returns an empty Vec when there is no store (nothing
    /// persisted to catch up from).
    async fn gather_catchup_patches(
        &mut self,
        up_to_version: &CVRVersion,
        current: &CVRVersion,
        exclude_query_hashes: &[String],
        catchup_from: NullableCVRVersion,
    ) -> Result<Vec<PatchToVersion>, String> {
        let Some(store_arc) = self.store.clone() else {
            return Ok(Vec::new()); // no store → nothing persisted to catch up from
        };

        // Gather the row pages + config patches from PG (async), then release
        // the store borrow before touching the engine (`getRow`).
        let (raw_rows, cfg_patches): (Vec<rust_cvr::schema::cvr::RowsRow>, Vec<PatchToVersion>) = {
            let store_for_rows = store_arc.clone();
            // The row cursor borrows the store guard, so it MUST be drained and
            // dropped inside this scope: `tokio::sync::Mutex` is not reentrant,
            // and the `catchup_reader()` lock below is the SAME mutex. Holding
            // the guard across it self-deadlocks the CG task — and catchup runs
            // on every client connect, so the whole group freezes after its
            // first flush with no error logged.
            let mut rows = Vec::new();
            // Phase timing (trace-only): `open` covers the store lock + the
            // `flushed()` wait inside `catchup_row_patches` (TS
            // row-record-cache.ts:361 awaits the deferred write-back before
            // reading), `read` the page loop, `cfg` the config-patch read.
            let catchup_open_started = std::time::Instant::now();
            let catchup_open_ms;
            {
                let store_guard = store_for_rows.lock().await;
                let mut cursor = store_guard
                    .catchup_row_patches(
                        catchup_from.clone(),
                        up_to_version,
                        current,
                        exclude_query_hashes,
                    )
                    .await
                    .map_err(|e| format!("catchup_row_patches: {e}"))?;
                catchup_open_ms = catchup_open_started.elapsed().as_secs_f64() * 1000.0;
                while let Some(page) = cursor
                    .next_page()
                    .await
                    .map_err(|e| format!("catchup rows page: {e}"))?
                {
                    rows.extend(page);
                }
            }
            let catchup_read_ms =
                catchup_open_started.elapsed().as_secs_f64() * 1000.0 - catchup_open_ms;
            let catchup_cfg_started = std::time::Instant::now();
            let store_reader = store_arc.lock().await.catchup_reader();
            let cfg = store_reader
                .catchup_config_patches(catchup_from.clone(), up_to_version, current)
                .await
                .map_err(|e| format!("catchup_config_patches: {e}"))?;
            if crate::trace::enabled() {
                crate::trace::note!(
                    "catchup-rows",
                    "cg={} rows={} catchup_open_ms={catchup_open_ms:.1} catchup_read_ms={catchup_read_ms:.1} catchup_cfg_ms={:.1}",
                    self.cg_id,
                    rows.len(),
                    catchup_cfg_started.elapsed().as_secs_f64() * 1000.0
                );
            }
            Ok::<_, String>((rows, cfg))
        }?;

        let mut patches: Vec<PatchToVersion> =
            Vec::with_capacity(raw_rows.len() + cfg_patches.len());
        for row in raw_rows {
            let row_key = match row.row_key {
                serde_json::Value::Object(m) => m,
                other => return Err(format!("catchup row_key is not an object: {other}")),
            };
            let id = RowID {
                schema: row.schema.clone(),
                table: row.table.clone(),
                row_key: row_key.clone(),
            };
            let to_version = maybe_version_string(&row.patch_version)
                .map_err(|e| format!("catchup: invalid patchVersion in rows table: {e}"))?;
            let patch = if row.ref_counts.is_none() {
                // Null refCounts = tombstone → the client should delete the row.
                Patch::Row(RowPatch::Del { id })
            } else {
                // Live row → rebuild contents from the engine (TS `getRow` +
                // `contentsAndVersion`), stripping the `_0_version` column.
                let pk: Vec<(String, rust_ivm::ivm::data::Value)> = row_key
                    .iter()
                    .map(|(k, v)| (k.clone(), json_to_value(v.clone())))
                    .collect();
                let contents = match self.pipelines.get_row(&row.table, &pk) {
                    Some(r) => Arc::new(row_to_contents(&r)),
                    None => {
                        return Err(format!(
                            "catchup: missing row {}:{}",
                            row.table,
                            serde_json::to_string(&row_key).unwrap_or_default()
                        ));
                    }
                };
                Patch::Row(RowPatch::Put { id, contents })
            };
            patches.push(PatchToVersion { patch, to_version });
        }
        patches.extend(cfg_patches);
        Ok(patches)
    }

    /// Build a row-set-signature provider for a `CVRQueryDrivenUpdater` plus the
    /// shared map it reads from. The updater's provider must be `Send + Sync`,
    /// but the engine (`IvmPipelines`) is `!Send`; so instead of capturing the
    /// engine we hand the updater a closure over a shared map, which we populate
    /// from the engine (`populate_signatures`) after the row changes are applied
    /// but before flush. Port of TS `queryID => this.#pipelines.rowSetSignature(queryID)`
    /// — the updater persists a query's signature and flags drift on change.
    #[allow(clippy::type_complexity)]
    fn signature_provider() -> (
        Arc<Mutex<HashMap<String, u64>>>,
        Box<dyn Fn(&str) -> Option<u64> + Send + Sync>,
    ) {
        let sigs: Arc<Mutex<HashMap<String, u64>>> = Arc::new(Mutex::new(HashMap::new()));
        let for_provider = sigs.clone();
        let provider = Box::new(move |qid: &str| for_provider.lock().unwrap().get(qid).copied());
        (sigs, provider)
    }

    /// Seed a signature accumulator from a CVR's persisted per-query signatures
    /// (parsed from hex). Used before an advance so the folded delta continues
    /// from the query's prior full signature. Port of the engine seeding a
    /// query's running signature from its stored value before XOR-folding a
    /// change.
    fn seed_signatures_from_cvr(cvr: &CVR) -> HashMap<String, u64> {
        let mut acc = HashMap::new();
        for (qid, q) in &cvr.queries {
            if let Some(hex) = q.base().row_set_signature.as_deref()
                && let Ok(sig) = rust_cvr::row_set_signature::parse_signature(Some(hex))
            {
                acc.insert(qid.clone(), sig);
            }
        }
        acc
    }

    /// Port of TS `#hydrateUnchangedQueries` (view-syncer.ts:1449). On a
    /// config-sync where the CVR is at the current db state, PROACTIVELY
    /// re-hydrate every already-gotten, same-transformation-hash, still-desired
    /// query and compare its freshly-computed row-set signature against the
    /// CVR-stored one. A mismatch means non-deterministic execution (e.g. a Cap
    /// operator picking a different N-row subset) → record the drift, remove the
    /// pipeline so the main reconciliation re-executes it via the updater path
    /// (emitting the row diff), and return its id in the drifted set. Non-drifted
    /// queries keep their rebuilt pipeline WITHOUT a version bump (their rows are
    /// already in the CVR) — this is what stops a plain reconnect from
    /// force-bumping every query.
    ///
    /// `executed` is the already-transformed `(qid, transformed_ast, new_hash)`
    /// set the caller built (rust transforms once and reuses; TS re-transforms
    /// here — behaviorally identical, same ASTs + hashes). Returns the drifted
    /// query ids, which `hydrate_and_sync` uses for the force-bump reason label.
    ///
    /// PERF: this re-executes every alive same-hash pipeline on every sync
    /// (TS's design). It is a serving-path cost that must be confirmed by a release-gate
    /// replay before deploy; it does not change WHAT a client observes.
    async fn hydrate_unchanged_queries(
        &mut self,
        cfg_cvr: &CVR,
        executed: &[(String, serde_json::Value, String)],
        errored_query_ids: &[String],
        state_version: &str,
    ) -> Result<std::collections::HashSet<String>, JsError> {
        let mut drifted: std::collections::HashSet<String> = std::collections::HashSet::new();
        // TS view-syncer.ts:1458 — when the CVR is behind the db, hydration must
        // run through the updater path, so skip the proactive re-check.
        if cfg_cvr.version.state_version != state_version {
            // TS `lc.info?.(`CVR (${versionToCookie(cvrVersion)}) is behind db
            // ${dbVersion}`)` (view-syncer.ts:1459-1461).
            tracing::info!(
                "CVR ({}) is behind db {state_version}",
                version_to_cookie(&cfg_cvr.version)
            );
            return Ok(drifted);
        }
        // TS `gotQueries` (view-syncer.ts:1465-1467): every CVR query with a
        // transformationHash, whatever its type.
        let got_queries: Vec<(&String, &QueryRecord)> = cfg_cvr
            .queries
            .iter()
            .filter(|(_, q)| q.base().transformation_hash.is_some())
            .collect();
        // TS transforms the got queries INSIDE this method (custom :1503-1512,
        // other :1547-1556) and classifies each result. Rust's caller
        // (`sync_query_pipeline_set`) ran that one transform over the same CVR
        // query set and hands the result down — `executed` holds every success,
        // `errored_query_ids` every custom transform error — so the
        // classification below is TS's, applied to the same result.
        let executed_by_id: HashMap<&str, (&serde_json::Value, &String)> = executed
            .iter()
            .map(|(id, ast, hash)| (id.as_str(), (ast, hash)))
            .collect();
        let mut inactivated_count = 0usize;
        let mut custom_error_count = 0usize;
        let mut custom_hash_mismatch_count = 0usize;
        let mut other_hash_mismatch_count = 0usize;
        // TS `transformedQueries` (:1471): the same-hash survivors, hydrated below.
        let mut transformed_queries: Vec<(&String, &QueryRecord, &serde_json::Value, &String)> =
            Vec::new();
        for &(qid, record) in &got_queries {
            // No-longer-desired: every client state inactivated (TS
            // view-syncer.ts:1474-1482). Internal queries are always desired.
            // NOTE: TS uses `Array.every`, which is VACUOUSLY TRUE for an empty
            // clientState — so a gotten query with no live client is skipped;
            // rust must not add a `!cs.is_empty()` guard (that would diverge).
            if !record.is_internal()
                && let Some(cs) = record.client_state()
                && cs.values().all(|s| s.inactivated_at.is_some())
            {
                inactivated_count += 1;
                continue;
            }
            let stored_hash = record.base().transformation_hash.as_deref();
            let is_custom = matches!(record, QueryRecord::Custom(_));
            match executed_by_id.get(qid.as_str()) {
                // Only SAME-transformation-hash results are hydrated here (TS
                // :1538-1541 custom, :1561-1566 other); a changed hash is left to
                // `#syncQueryPipelineSet`, which re-executes it.
                Some((ast, new_hash)) if Some(new_hash.as_str()) == stored_hash => {
                    transformed_queries.push((qid, record, ast, new_hash));
                }
                Some(_) if is_custom => custom_hash_mismatch_count += 1,
                Some(_) => other_hash_mismatch_count += 1,
                // TS :1536-1537 `'error' in q` → customErrorCount.
                None if is_custom && errored_query_ids.iter().any(|e| e == qid) => {
                    custom_error_count += 1;
                }
                // A custom query this pass did not transform (no transformer
                // configured — TS :1496-1500 warns and transforms nothing — or
                // outside its transform set): TS neither counts nor hydrates it.
                None => {}
            }
        }
        // TS view-syncer.ts:1570-1577, verbatim.
        tracing::info!(
            "hydrateUnchangedQueries: {} got queries, {inactivated_count} inactivated, \
             {custom_error_count} custom transform errors, \
             {custom_hash_mismatch_count} custom hash mismatches, \
             {other_hash_mismatch_count} other hash mismatches, {} hydrated",
            got_queries.len(),
            transformed_queries.len()
        );
        for (qid, record, transformed_ast, new_hash) in transformed_queries {
            // Re-hydrate (TS `#pipelines.addQuery(..., 'unchanged-query-rehydrate')`),
            // folding the candidate row-set signature caller-side — rust's
            // streaming `hydrate` does not maintain `engine.row_set_signature`, so
            // the caller folds it exactly as `hydrate_and_sync` does. Rows are
            // discarded: the CVR already holds them; this pass only rebuilds the
            // pipeline and checks drift.
            let mut sig_acc: HashMap<String, u64> = HashMap::new();
            // TS `addQuery(transformationHash, queryID, ast, timer, queryName,
            // 'unchanged-query-rehydrate')` (view-syncer.ts:1620-1626).
            let one = [HydrateQuery {
                query_id: qid.clone(),
                ast_json: transformed_ast.to_string(),
                transformation_hash: new_hash.clone(),
                query_name: query_name_of(cfg_cvr, qid),
                hydration_reason: PipelineHydrationReason::UnchangedQueryRehydrate,
            }];
            // TS view-syncer.ts:1608-1637: a fresh `TimeSliceTimer` per query,
            // `await timer.start()`, and `timer.yieldProcess('yield in
            // hydrateUnchangedQueries')` on every `'yield'`.
            let timer = Rc::new(TimeSliceTimer::new());
            timer.start().await;
            let mut count = 0usize;
            {
                // TS `#hydrateUnchangedQueries` puts NO try/catch around
                // `#pipelines.addQuery` (view-syncer.ts:1620-1635): a hydrate
                // throw propagates out of the whole pass and fails the group.
                // Rust must not warn-and-skip — that would turn a failed
                // rehydrate into a query silently left on a torn-down pipeline.
                let mut changes = self
                    .pipelines
                    .hydrate(&one, Rc::clone(&timer) as Rc<dyn Timer>)?;
                for item in changes.by_ref() {
                    match item {
                        StreamItem::Yield => timer.yield_process().await,
                        StreamItem::Data(rc) => {
                            accumulate_signature(&mut sig_acc, &rc);
                            count += 1;
                        }
                    }
                }
                changes.finish()?;
            }
            let elapsed = timer.total_elapsed();
            // TS view-syncer.ts:1638-1639, per rehydrated query:
            //   this.#hydrations.add(1);
            //   this.#hydrationTime.recordMs(elapsed);
            self.metrics.record_hydration(elapsed);
            tracing::debug!("hydrated {count} rows for {qid} ({elapsed} ms)");
            self.pipelines.set_query_transformation_hash(qid, new_hash);
            // Inspector recording — port of the `#hydrateUnchangedQueries` tail
            // (view-syncer.ts:1640-1641): `#addQueryMaterializationServerMetric(
            // transformationHash, elapsed)` + `#inspectorDelegate.addQuery(
            // transformationHash, transformedAst)`. Faithful to TS, BOTH are keyed
            // by the transformationHash (`new_hash`), not the queryID — recorded
            // before the drift check, so a query that drifts and is removed below
            // still contributed its materialization sample to the global aggregate.
            if let Some(ms) = self.pipelines.hydration_time_ms(qid) {
                self.inspector_delegate.borrow_mut().add_metric(
                    rust_ivm::query::metrics_delegate::Metric::QueryMaterializationServer,
                    ms,
                    new_hash,
                );
            }
            self.inspector_delegate
                .borrow_mut()
                .add_query(new_hash, transformed_ast.clone());
            // Drift detection (TS view-syncer.ts:1659-1673): compare the candidate
            // signature to the CVR-stored one. Skip when there is NO stored
            // signature (legacy pre-feature query — a forced re-execution would
            // needlessly resend rows). A mismatch → record the drift, remove the
            // pipeline (so the main reconciliation re-executes + emits the diff),
            // and mark it drifted.
            let candidate = sig_acc.get(qid).copied().unwrap_or(0);
            if let Some(hex) = record.base().row_set_signature.as_deref()
                && let Ok(stored) = rust_cvr::row_set_signature::parse_signature(Some(hex))
                && stored != candidate
            {
                // Text is 1:1 with TS (view-syncer.ts:1664-1667), `({count}
                // rows)` and the sentence punctuation included — the row count
                // is the field that says how big the drifted set was.
                tracing::warn!(
                    "rowSetSignature drift for query {qid}: \
                     prior={stored:x} new={candidate:x} \
                     ({count} rows). Removing from pipelines for full re-execution."
                );
                rust_cvr::otel_metrics::record_row_set_signature_drift();
                self.pipelines.remove_query(qid, "remove-query");
                drifted.insert(qid.clone());
            }
        }
        Ok(drifted)
    }

    /// Hydrate queries AND apply to CVR + push pokes to clients — the whole
    /// hydrate hot path — the hydrate arm of TS `#addAndRemoveQueries`
    /// (view-syncer.ts:2151) through `#processChanges` (:2472).
    ///
    /// `add_queries` is `(query_id, transformation_hash)`; `queries` is
    /// `(query_id, ast_json)` for the pipelines to hydrate. A hydrate panic
    /// (source-drift assert) propagates out for teardown, after the engine rolls
    /// back its partial source connections.
    #[allow(clippy::too_many_arguments)]
    /// Hydrate queries and poke their rows. Returns the still-OPEN `MultiPoker`
    /// alongside the result: the caller MUST call `pokers.end(result.cvr.version)`
    /// after adding any remaining patches (catch-up rides the same poke). This is
    /// the TS `#syncQueryPipelineSet` shape — one `pokeStart(baseCookie=old)` →
    /// hydrate parts + catchup parts → one `pokeEnd(new)`. Ending the poke here
    /// (as this function previously did) advanced every client's `base_version`
    /// to the new CVR version, which made the subsequent catch-up poke a NOOP —
    /// silently dropping every patch a reconnecting client missed while away.
    pub async fn hydrate_and_sync(
        &mut self,
        cvr: CVR,
        state_version: String,
        replica_version: String,
        add_queries: &[(String, String)],
        remove_queries: &[String],
        client_ids: &[String],
        queries: &[(String, String)],
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
        drifted_query_ids: &std::collections::HashSet<String>,
    ) -> Result<(SyncResult, MultiPoker), JsError> {
        // Port of TS `#lookupRowsForExecutedAndRemovedQueries` (cvr.ts:652-667).
        // TS kicks that off from `trackQueries` and it returns WITHOUT reading the
        // row cache when nothing was executed or removed — its own comment:
        // "Query-less update. This can happen for config only changes." The read
        // therefore belongs HERE, at the seat that holds `executed`/`removed`, not
        // at the top of the pass: rust used to materialise the whole per-CG row map
        // for every pass and only then reach the equivalent early return inside
        // `delete_unreferenced_rows`, so a config-only pass paid a full `cvr.rows`
        // read that TS provably skips.
        //
        // Rust-only shape (rule 5): TS parks an un-awaited Promise in `#existingRows`
        // and awaits it later. Rust reads it eagerly at this one point because the
        // consumer, `received()`, runs inside `pipelines.hydrate`'s SYNCHRONOUS
        // `FnMut` callback and cannot await. `queries` is included in the guard so
        // the map is always present for any pass that actually hydrates.
        let rows_started = std::time::Instant::now();
        let existing_rows_owned: Arc<RowRecordMap> =
            if add_queries.is_empty() && remove_queries.is_empty() && queries.is_empty() {
                Arc::new(HashMap::new())
            } else {
                self.existing_rows().await.map_err(|e| e.to_string())?
            };
        crate::trace::note!(
            "hydrate-rows",
            "cg={} existing_rows={} existing_rows_ms={:.1}",
            self.cg_id,
            existing_rows_owned.len(),
            rows_started.elapsed().as_secs_f64() * 1000.0
        );
        let existing_rows: &RowRecordMap = &existing_rows_owned;
        let (sigs, provider) = Self::signature_provider();
        // Port of TS `#addAndRemoveQueries` force-bump (view-syncer.ts:2194-2215):
        // an already-gotten, same-transformation-hash query re-executed without a
        // stateVersion/hash change or a removal would NOT bump `configVersion` via
        // `track_queries`, so a row diff from `received()` would have no new
        // `patchVersion` to attach (the `#assertNewVersion` no-bump wedge). Decide
        // BEFORE `cvr`/`state_version` move into the updater; force the bump after.
        // `drifted_query_ids` (from `hydrate_unchanged_queries`) selects the reason
        // label — `row-set-signature-drift`/`mixed` when the re-added query drifted,
        // `missing-pipeline` when it was merely reaped.
        // TS `lc.info?.(`hydrating ${addQueries.length} queries`)`
        // (view-syncer.ts:2172), immediately before the updater is built.
        tracing::info!(
            state_version = %state_version,
            "hydrating {} queries",
            add_queries.len()
        );
        let bump_reason = same_hash_rehydration_bump_reason(
            &cvr,
            add_queries,
            remove_queries,
            &state_version,
            drifted_query_ids,
        );
        // The TS `addQuery` identity of each query in this batch
        // (view-syncer.ts:2286-2293: `q.transformationHash`, `q.id`, `q.ast`,
        // `q.name`, `'query-set-sync'`): hash from `add_queries`, custom-query
        // name from the CVR record. Built before `cvr` moves into the updater.
        let add_hash: HashMap<&str, &str> = add_queries
            .iter()
            .map(|(q, h)| (q.as_str(), h.as_str()))
            .collect();
        let hydrate_queries: Vec<HydrateQuery> = queries
            .iter()
            .map(|(qid, ast_json)| HydrateQuery {
                query_id: qid.clone(),
                ast_json: ast_json.clone(),
                transformation_hash: add_hash
                    .get(qid.as_str())
                    .map(|h| h.to_string())
                    .unwrap_or_default(),
                query_name: query_name_of(&cvr, qid),
                hydration_reason: PipelineHydrationReason::QuerySetSync,
            })
            .collect();
        let mut updater =
            CVRQueryDrivenUpdater::new(cvr, state_version, replica_version, Some(provider));
        if let Some(reason) = bump_reason {
            crate::metrics::record_same_hash_rehydration_version_bump(reason);
            // The bump is what a quiet commit later throws away, producing the
            // `Patches were sent but finalVersion ...` close (pokeID == final+1).
            // The OTEL counter carries the reason but is not scraped, so the
            // label — `missing-pipeline` vs `row-set-signature-drift` — was
            // invisible in production. It is the field that says WHICH upstream
            // condition fires: a missing pipeline (rust dropped a pipeline TS
            // keeps) or signature drift (re-execution chose a different row
            // subset). TS logs nothing here, so this is rust-only diagnostics on
            // a rust-only symptom: a 434-vs-0 divergence against the TS arm on
            // identical traffic (a dual replay run).
            tracing::info!(
                cg_id = %self.cg_id,
                add_queries = add_queries.len(),
                "same-hash rehydration version bump: {reason}"
            );
            updater.ensure_new_version();
        }

        let executed_refs: Vec<(&str, &str)> = add_queries
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect();
        let removed_refs: Vec<&str> = remove_queries.iter().map(|s| s.as_str()).collect();
        let (new_version, query_patches) = updater.track_queries(&executed_refs, &removed_refs);

        let clients = self.get_clients(client_ids);
        let client_refs: Vec<&ClientHandler> = clients.iter().map(|c| c.as_ref()).collect();
        let pokers = MultiPoker::new(&client_refs, new_version, "hydrate-and-sync");
        for patch in &query_patches {
            pokers.add_patch(patch);
        }

        // Remove queries from the engine before hydrating new ones (matches the
        // TS path that calls `pipelines.removeQuery(q.id)` before hydrate). These
        // are TTL/errored removals, TS's default stop reason `remove-query`.
        for qid in remove_queries {
            self.pipelines.remove_query(qid, "remove-query");
            // Port of `this.#inspectorDelegate.removeQuery(q.id)` (view-syncer.ts:
            // 2238) — drop this query's per-query server metrics + stored AST.
            self.inspector_delegate.borrow_mut().remove_query(qid);
        }

        // Freshly-hydrated queries start from an empty row set (signature 0), so
        // the fold over this hydrate's changes yields the query's full signature.
        let mut sig_acc: HashMap<String, u64> = HashMap::new();
        let mut processor = ChangeProcessor::new(&mut updater, &pokers);
        // Phase profiling (SYNCER_TRACE): the `pipelines.hydrate` call is the
        // initial fetch (SQLite source reads) + IVM operator materialization —
        // the dominant hydration cost. Timing it separately from the CVR flush,
        // together with the row-change count, distinguishes "fetching too many
        // rows" (query-shape / planning) from "slow per-row cold I/O".
        let fetch_started = std::time::Instant::now();
        let fetch_cpu_started = crate::trace::thread_cpu_ms();
        // A `received` failure (the CVR version-bump invariant) is a recoverable
        // error, not a panic — TS's `#assertNewVersion` throws and aborts the
        // whole pass. Capture the first one, stop feeding the updater (but keep
        // draining, so the engine's fetch reaches a normal end-of-stream for
        // the Take/Cap initial-fetch guard), and surface it below so the caller
        // fails the connection and the client re-hydrates from the last
        // consistent CVR (no partial flush).
        let mut cvr_err: Option<String> = None;
        // Port of TS `#hydrateAndSync`'s time-slicing (view-syncer.ts:2244-2260)
        // consumed by `#processChanges` (:2508-2512): one `TimeSliceTimer` for
        // the pass, `await yieldProcess(lc)` "at the very beginning so that the
        // first time slice is properly processed by the time-slice queue", then
        // the change stream is pulled with `timer.yieldProcess()` on every
        // `'yield'`. The timer is process time (stopped while yielded), which is
        // what the per-query hydration time (`timer.totalElapsed()`) and the
        // advance budget are measured in; TS's per-query
        // `timer.startWithoutYielding()` / `timer.stop()` bracketing becomes
        // one lap across rust's batched hydrate, with the engine taking
        // per-query deltas of the same clock.
        let timer = Rc::new(TimeSliceTimer::new());
        yield_process().await;
        timer.start_without_yielding();
        let mut yields = 0u32;
        let mut yielded = std::time::Duration::ZERO;
        {
            let mut changes = self
                .pipelines
                .hydrate(&hydrate_queries, Rc::clone(&timer) as Rc<dyn Timer>)?;
            for item in changes.by_ref() {
                match item {
                    StreamItem::Yield => {
                        let yielded_at = std::time::Instant::now();
                        timer.yield_process().await;
                        yields += 1;
                        yielded += yielded_at.elapsed();
                    }
                    StreamItem::Data(rc) => {
                        accumulate_signature(&mut sig_acc, &rc);
                        if cvr_err.is_some() {
                            continue;
                        }
                        if let Some((ct, qid, table, rk, row)) = row_change_to_maps(&rc)
                            && let Err(e) =
                                processor.on_row_change(ct, &qid, &table, rk, row, existing_rows)
                        {
                            cvr_err = Some(e);
                        }
                    }
                }
            }
            // TS `addQuery` throwing (a source-drift assert, a Take/Cap
            // invariant, an unpreparable cost-model probe) reaches the
            // view-syncer as an ERROR, which fails the group with an error
            // frame — never as a bare task death. Surface it on the same `Err`
            // channel the CVR version-bump failure above uses.
            changes.finish()?;
        }
        let total_process_time_ms = timer.stop();
        if let Some(e) = cvr_err {
            return Err(e.into());
        }
        // TS `generateRowChanges` tail (view-syncer.ts:2300-2301):
        //   hydrations.add(1);
        //   hydrationTime.recordMs(totalProcessTime);
        // ONE count per `#addAndRemoveQueries` batch — not per query, not per
        // config pass — with the batch's process time (the
        // `startWithoutYielding()`/`stop()` bracket, yields excluded). They are
        // the generator's last statements, so a throw mid-batch (a hydrate
        // error, the CVR version-bump failure above) never reaches them —
        // hence after both early returns.
        self.metrics.record_hydration(total_process_time_ms);
        crate::trace::note!(
            "hydrate-fetch",
            "cg={} queries={} rows={} fetch_materialize_ms={:.1} cpu_ms={:.1} process_ms={:.1} yields={} yielded_ms={:.1}",
            self.cg_id,
            queries.len(),
            processor.total_processed(),
            fetch_started.elapsed().as_secs_f64() * 1000.0,
            crate::trace::thread_cpu_ms() - fetch_cpu_started,
            total_process_time_ms,
            yields,
            yielded.as_secs_f64() * 1000.0
        );
        // Record the transformation hash each query was hydrated with, so a later
        // config pass can detect a changed hash (drift / auth re-transform) and
        // re-hydrate. Port of the `transformationHash` carried in the TS pipeline
        // query map.
        for (qid, hash) in add_queries {
            self.pipelines.set_query_transformation_hash(qid, hash);
        }
        // Record the per-query server materialization metric + AST for the
        // inspector — port of the `#syncQueryPipelineSet` add tail
        // (view-syncer.ts:2297-2298): `#addQueryMaterializationServerMetric(q.id,
        // elapsed)` + `#inspectorDelegate.addQuery(q.id, q.ast)`, both keyed by the
        // queryID (`q.id`), which is what the `queries` op looks up per row. Rust's
        // per-query hydrate time comes from the engine's own timing
        // (`hydration_time_ms`, set during the batched hydrate above) rather than a
        // TS wall-clock `timer`, so a query the engine did not register (e.g.
        // cancel-during-hydrate) simply records no metric.
        for q in &hydrate_queries {
            let qid = &q.query_id;
            if let Some(ms) = self.pipelines.hydration_time_ms(qid) {
                self.inspector_delegate.borrow_mut().add_metric(
                    rust_ivm::query::metrics_delegate::Metric::QueryMaterializationServer,
                    ms,
                    qid,
                );
                // TS view-syncer.ts:2305-2307, per query on the SAME process-time
                // `elapsed` the metric above records:
                //   if (elapsed > slowHydrateThreshold)
                //     queryLC.warn?.('Slow query materialization', elapsed, q.ast);
                // `queryLC` = lc + `hash` / `queryHash` / `transformationHash`
                // (+ `queryName` when defined) (:2265-2271); `ast` is the
                // transformed AST TS attaches as the log payload.
                if ms > slow_hydrate_threshold_ms() {
                    tracing::warn!(
                        hash = %qid,
                        query_hash = %qid,
                        transformation_hash = %q.transformation_hash,
                        query_name = q.query_name.as_deref(),
                        elapsed_ms = ms,
                        ast = %q.ast_json,
                        "Slow query materialization"
                    );
                }
            }
            if let Ok(ast) = serde_json::from_str::<serde_json::Value>(&q.ast_json) {
                self.inspector_delegate.borrow_mut().add_query(qid, ast);
            }
        }
        processor.finish(existing_rows)?;
        let num_changes = processor.total_processed();
        drop(processor);
        // Release the row-record snapshot before the flush — see the twin note
        // in `advance_and_sync`. Held past here, it forces
        // `RowRecordCache::apply` to copy the client group's entire row set.
        drop(existing_rows_owned);

        // Hand the folded signatures to the updater's provider so its flush can
        // persist each hydrated query's signature and flag drift.
        *sigs.lock().unwrap() = sig_acc;
        let (flushed_cvr, _stats) = updater.flush(last_connect_time, last_active, ttl_clock);
        // Share the CVR with the offloaded flush via `Arc` (refcount bump, not a
        // deep copy); reclaim it after the awaited flush drops its clone.
        let flushed_arc = Arc::new(flushed_cvr);
        // Version the updater PRODUCED, kept for the discarded-bump check below.
        let attempted_version = flushed_arc.version.clone();
        let flush_started = std::time::Instant::now();
        let store_flushed = self
            .flush_to_store(&mut updater, flushed_arc.clone(), last_connect_time)
            .await?;
        // Phase profiling (SYNCER_TRACE): CVR-store persist cost, split from the
        // fetch/materialize above so total hydration is fully attributed.
        crate::trace::note!(
            "hydrate-flush",
            "cg={} store_flush_ms={:.1} flushed={}",
            self.cg_id,
            flush_started.elapsed().as_secs_f64() * 1000.0,
            store_flushed
        );
        // No-op store flush → revert to the ORIGINAL CVR (see `flush_to_store`).
        let flushed_cvr = if store_flushed {
            Arc::try_unwrap(flushed_arc).unwrap_or_else(|a| (*a).clone())
        } else {
            let orig = updater.base.orig.clone();
            // THIS is the failing path: the pokers were opened at
            // `new_version` (the post-bump version `track_queries` returned) and
            // the caller ends them at the reverted version, so a patch tagged
            // with the bump starts the poke and `end()` then finds
            // `base == final` and CLOSES the client. Logging the delta names
            // whichever path bumped — the same-hash rehydration path was ruled
            // OUT by production data (244 errors, ZERO such bumps).
            //
            // Gated on `any_started()`: a discarded bump is only
            // dangerous when a patch actually WENT OUT, because TS raises
            // `Patches were sent but finalVersion ...` only on the `pokeStarted`
            // branch (client-handler.ts:327-334) — an unstarted poke no-ops or
            // opens a fresh `pokeStart` and cannot raise. Ungated, this line
            // fired on the benign case too: 54 times in 3.5 min of GKE sandbox
            // traffic with ZERO started pokes, zero closes, and 2 of 187 poke
            // frames ever ending off their opening cookie.
            if pokers.any_started()
                && rust_cvr::schema::types::cmp_cvr(&attempted_version, &orig.version)
                    != std::cmp::Ordering::Equal
            {
                tracing::warn!(
                    cg_id = %self.cg_id,
                    "hydrate quiet commit discarded a version bump: {} -> {}",
                    rust_cvr::schema::types::version_string(&attempted_version),
                    rust_cvr::schema::types::version_string(&orig.version),
                );
            }
            orig
        };
        // NOTE: the poke is NOT ended here — the caller ends it after appending
        // catch-up patches (or immediately, when no catch-up applies).
        // Cookie formatting goes through the 1:1 `version_to_cookie` (TS
        // client-handler.ts:189/201/318), not raw `version_string`, so the
        // cookie call sites stay auditable against the TS spec.
        let version = version_to_cookie(&flushed_cvr.version);
        Ok((
            SyncResult {
                cvr: flushed_cvr,
                version,
                query_patches,
                num_changes,
                reset_reason: None,
                reset_msg: None,
                process_time_ms: total_process_time_ms,
            },
            pokers,
        ))
    }

    /// Advance the replica to head AND apply to CVR + push pokes to clients —
    /// TS `#advancePipelines` (view-syncer.ts:2567) through `#processChanges`
    /// (:2601). On a reset, the in-flight
    /// poke is cancelled and the caller is expected to rehydrate.
    #[allow(clippy::too_many_arguments)]
    pub async fn advance_and_sync(
        &mut self,
        cvr: CVR,
        replica_version: String,
        client_ids: &[String],
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
    ) -> Result<SyncResult, String> {
        // The pre-advance CVR version — only clients AT this version may receive
        // the advance delta (see the poke-target filter below).
        let cvr_version = cvr.version.clone();
        // An advance folds its delta onto each query's PRIOR full signature, so
        // seed the accumulator from the CVR's persisted per-query signatures.
        let mut sig_acc = Self::seed_signatures_from_cvr(&cvr);

        // Advance FIRST, capturing the new state version from the header (the
        // version the snapshot advanced TO) and collecting the delta. The updater
        // must be constructed with THIS version, not an empty placeholder: its
        // `new()` asserts `stateVersion >= cvr.version.stateVersion` (which "" is
        // NOT, for any non-empty CVR version → panic), and the rows must be tagged
        // with the correct cookie. This mirrors TS, which does
        // `const {version, changes} = await pipelines.advance()` and only THEN
        // constructs the `CVRQueryDrivenUpdater` with `version`
        // (view-syncer.ts). An advance delta is small (only changes since the
        // last version — TS likewise returns `changes` as an array), so buffering
        // it is cheap, unlike a full hydrate.
        type CollectedChange = (
            RowChangeType,
            String,
            String,
            serde_json::Map<String, serde_json::Value>,
            Option<serde_json::Map<String, serde_json::Value>>,
        );
        let mut collected: Vec<CollectedChange> = Vec::new();
        // Port of TS `#advancePipelines` (view-syncer.ts:2596-2606): `const
        // {version, numChanges, changes} = this.#pipelines.advance(timer)`, then
        // `#processChanges(lc, await timer.start(), changes, ...)` — the timer
        // starts (after a first yield to the time-slice queue) once the header
        // is known, and every `'yield'` from the change stream awaits a slice.
        let timer = Rc::new(TimeSliceTimer::new());
        let advance_started = std::time::Instant::now();
        let mut yields = 0u32;
        let (new_version, num_changes, outcome) = {
            let mut changes = self.pipelines.advance(Rc::clone(&timer) as Rc<dyn Timer>)?;
            let (version, n) = changes.header();
            let (new_version, num_changes) = (version.to_string(), n);
            timer.start().await;
            for item in changes.by_ref() {
                match item {
                    StreamItem::Yield => {
                        timer.yield_process().await;
                        yields += 1;
                    }
                    StreamItem::Data(rc) => {
                        accumulate_signature(&mut sig_acc, &rc);
                        collected.extend(row_change_to_maps(&rc));
                    }
                }
            }
            let outcome = changes.finish()?;
            (new_version, num_changes, outcome)
        };
        let mut num_changes = num_changes;
        let total_process_time_ms = timer.stop();
        if crate::trace::enabled() {
            crate::trace::note!(
                "advance-changes",
                "cg={} changes={} rows={} advance_ivm_ms={:.1} process_ms={:.1} yields={}",
                self.cg_id,
                num_changes,
                collected.len(),
                advance_started.elapsed().as_secs_f64() * 1000.0,
                total_process_time_ms,
                yields
            );
        }

        if let AdvanceOutcome::Reset { reason, msg } = outcome {
            // No poke was started (the pokers are built below, after a clean
            // advance), so there is nothing to cancel — just report the reset.
            // `cvr` itself, not a defensive copy: this `return` is upstream of
            // the `CVRQueryDrivenUpdater::new(cvr, ...)` that consumes it, and
            // nothing between the top of this method and here takes `cvr` by
            // `&mut`. It used to be a `cvr.clone()` taken at the TOP of every
            // advance — one full CVR deep copy (both `BTreeMap`s, every
            // `QueryRecord`'s AST) per replication commit, to serve a path that
            // fires only on a reset.
            return Ok(SyncResult {
                cvr,
                version: String::new(),
                query_patches: Vec::new(),
                num_changes,
                reset_reason: Some(reason),
                reset_msg: Some(msg),
                process_time_ms: 0.0,
            });
        }

        // LAZY, like TS. `existing_rows` is read ONLY by `on_row_change` /
        // `finish_received` below, so an advance that collected no row changes
        // never needs it. TS never materialises a row map for an advance at all:
        // `#advancePipelines` hands `#processChanges` just the changed-row batch
        // (view-syncer.ts:2472-2505 -> `updater.received(lc, rows)`), and the
        // store reads row records only inside `#flush`, guarded by
        // `if (this.#pendingRowRecordUpdates.size)` (cvr-store.ts:1066-1067).
        //
        // Rust used to load this in `on_notification` BEFORE the advance, so
        // every notification paid an `offload()` hop onto the shared pool plus a
        // store-mutex acquire even when the commit touched nothing this CG
        // queries. That cost scales with (live CGs x commit rate), which is why
        // it is invisible on a 5-minute trace and dominates a 60-minute one:
        // 1.5M no-op advance cycles at ~900 CGs, against ~3K on the TS arm.
        // The same anti-pattern is already called out and avoided inside
        // `cvr_store::flush_internal` ("work TS never performs"); this is the
        // same read one level up, at the notification site.
        let collected_empty = collected.is_empty();
        let existing_rows_owned: Arc<RowRecordMap> = if collected_empty {
            Arc::new(HashMap::new())
        } else {
            self.existing_rows().await.map_err(|e| e.to_string())?
        };
        // Build the updater with the real post-advance version, then replay the
        // collected delta through it (order preserved).
        let (sigs, provider) = Self::signature_provider();
        let mut updater =
            CVRQueryDrivenUpdater::new(cvr, new_version, replica_version, Some(provider));
        let pokers_version = updater.updated_version();

        // Only poke clients that are AT the pre-advance `cvr.version` (see
        // `advance_poke_targets`).
        let clients = Self::advance_poke_targets(self.get_clients(client_ids), &cvr_version);
        let client_refs: Vec<&ClientHandler> = clients.iter().map(|c| c.as_ref()).collect();
        let pokers = MultiPoker::new(&client_refs, pokers_version, "advance-and-sync");

        // The row-record snapshot is CONSUMED by this block and released with
        // it, before the flush below. TS releases it per `received()` call — its
        // `const existingRows = await this._cvrStore.getRowRecords()`
        // (cvr.ts:845) is a local, so no reference survives the call. Rust has
        // to read it once up front (the consumer runs inside a synchronous
        // `FnMut` and cannot await), but it must not hold it any LONGER than
        // TS: a live snapshot makes the flush's `RowRecordCache::apply`
        // copy-on-write the client group's entire row set (`Arc::make_mut`
        // sees strong_count > 1). Moving `existing_rows_owned` in here is what
        // makes that structural rather than a comment — `cow_copies` stays 0.
        {
            let existing_rows_owned = existing_rows_owned;
            let existing_rows: &RowRecordMap = &existing_rows_owned;
            let mut processor = ChangeProcessor::new(&mut updater, &pokers);
            for (ct, qid, table, rk, row) in collected {
                // A `received` version-bump failure is recoverable (TS throws);
                // abort the advance before any flush so the client re-hydrates.
                processor.on_row_change(ct, &qid, &table, rk, row, existing_rows)?;
            }
            // TS `#advancePipelines` only processes received row changes. It
            // does not reconcile unreferenced rows because no queries are being
            // executed/removed in an advance pass.
            processor.finish_received(existing_rows)?;
            num_changes = processor.total_processed();
        }

        // Hand the folded post-advance signatures to the updater's provider.
        *sigs.lock().unwrap() = sig_acc;
        let (flushed_cvr, _stats) = updater.flush(last_connect_time, last_active, ttl_clock);
        // Share the CVR with the offloaded flush via `Arc` (refcount bump, not a
        // deep copy); reclaim it after the awaited flush drops its clone.
        let flushed_arc = Arc::new(flushed_cvr);
        // Version the updater PRODUCED, before the flush decides whether it is
        // material. Compared against `orig` below to catch a discarded bump.
        let attempted_version = flushed_arc.version.clone();
        let store_flushed = self
            .flush_to_store(&mut updater, flushed_arc.clone(), last_connect_time)
            .await?;
        // Quiet commit (zero IVM output for this CG, e.g. the batch only touched
        // other groups' rows): the store flush is a no-op, so revert to the
        // ORIGINAL CVR (TS `flush` → `this._orig`). `pokers.end(orig)` then
        // no-ops for caught-up clients instead of advancing their cookies to a
        // version that was never persisted, and the next material flush's
        // `expected_current_version` still matches the on-disk version.
        if collected_empty {
            ADV_NO_CHANGES.fetch_add(1, Ordering::Relaxed);
        } else if store_flushed {
            ADV_MATERIAL.fetch_add(1, Ordering::Relaxed);
        } else {
            ADV_QUIET.fetch_add(1, Ordering::Relaxed);
        }
        maybe_log_advance_summary();
        let flushed_cvr = if store_flushed {
            Arc::try_unwrap(flushed_arc).unwrap_or_else(|a| (*a).clone())
        } else {
            // MOVED, not cloned: `updater` is dead after this arm (the
            // `flush_to_store` borrow ended above and nothing below touches
            // it), and the TS twin hands back the original BY REFERENCE —
            // `return {cvr: this._orig, flushed: false}` (cvr.ts:201). Cloning
            // deep-copied the whole CVR (both `BTreeMap`s, every
            // `QueryRecord`'s AST) on what is the COMMON advance outcome on a
            // busy replica — the `ADV_QUIET` counter below is how often.
            let orig = updater.base.orig;
            // A quiet commit that DISCARDS a version bump is the
            // `Patches were sent but finalVersion ...` close: the pokers were
            // opened at `attempted_version`, any patch tagged with it was
            // admitted and started the poke, and `end(orig)` then finds
            // `base == final`. The same-hash rehydration path was ruled OUT by
            // production data (244 errors, ZERO of those bumps), so log the
            // delta here to name whichever path actually bumped.
            //
            // Gated on `any_started()` — see the twin note in `hydrate_and_sync`
            // for why an unstarted poke makes the discard benign.
            if pokers.any_started()
                && rust_cvr::schema::types::cmp_cvr(&attempted_version, &orig.version)
                    != std::cmp::Ordering::Equal
            {
                tracing::warn!(
                    cg_id = %self.cg_id,
                    "quiet commit discarded a version bump: {} -> {}",
                    rust_cvr::schema::types::version_string(&attempted_version),
                    rust_cvr::schema::types::version_string(&orig.version),
                );
            }
            orig
        };
        pokers.end(flushed_cvr.version.clone());

        // 1:1 cookie formatting — see the twin note at the config-path site.
        let version = version_to_cookie(&flushed_cvr.version);
        self.metrics.record_advance(total_process_time_ms);
        Ok(SyncResult {
            cvr: flushed_cvr,
            version,
            query_patches: Vec::new(),
            num_changes,
            reset_reason: None,
            reset_msg: None,
            process_time_ms: total_process_time_ms,
        })
    }

    /// Remove queries whose TTL has elapsed (inactive for ALL clients, past
    /// `inactivated_at + ttl` relative to `ttl_clock`): tear them out of the
    /// pipeline + CVR and poke the resulting query/row removals. Port of TS
    /// `#removeExpiredQueries` → the removal side of `#syncQueryPipelineSet`.
    /// Returns the flushed CVR and the number of queries removed (0 = no-op).
    #[allow(clippy::too_many_arguments)]
    pub async fn remove_expired_queries(
        &mut self,
        cvr: CVR,
        client_ids: &[String],
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
    ) -> Result<(CVR, usize), String> {
        // `get_inactive_queries` returns queries inactive for every client with
        // the longest per-client eviction time; expired = that time is at or
        // before the current ttl_clock. Internal queries never appear here.
        let expired: Vec<String> = rust_cvr::cvr::get_inactive_queries(&cvr)
            .into_iter()
            .filter(|q| q.inactivated_at + q.ttl <= ttl_clock)
            .map(|q| q.hash)
            .collect();
        if expired.is_empty() {
            return Ok((cvr, 0));
        }
        // TS `#removeExpiredQueries` (view-syncer.ts:635-652): `if
        // (this.#pipelinesSynced) await this.#syncQueryPipelineSet(lc, cvr,
        // 'missing', undefined)` — a FULL re-sync from the CVR, not a targeted
        // removal: the expired queries fall out through the sync's
        // `removeQueriesQueryIds`, and any CVR query absent from the pipelines
        // is (re-)added, transforming only the missing custom ones. `connCtx`
        // is undefined → the background connection's context.
        if !self.pipelines_synced {
            return Ok((cvr, 0));
        }
        let (permissions, auth_data, custom_ctx, state_version, replica_version) =
            self.sync_query_pipeline_set_inputs(&cvr, None);
        let original_client_versions: std::collections::HashMap<String, NullableCVRVersion> = self
            .get_clients(client_ids)
            .iter()
            .map(|c| (c.ws_id.clone(), c.version()))
            .collect();
        let shard = self.shard.clone();
        let cvr = self
            .sync_query_pipeline_set(
                cvr,
                CustomQueryTransformMode::Missing,
                client_ids,
                &shard,
                permissions.as_ref(),
                &auth_data,
                custom_ctx.as_ref(),
                state_version,
                replica_version,
                last_connect_time,
                last_active,
                ttl_clock,
                original_client_versions,
            )
            .await?;
        Ok((cvr, expired.len()))
    }

    /// Delete clients from the CVR: each client's desired queries are marked
    /// inactive (so the TTL scheduler later expires them) and the client record
    /// is removed. Flushes + pokes the config patches, and broadcasts a
    /// `deleteClients` ack for `ack_client_ids` / `ack_group_ids`. Port of the
    /// client-deletion loop + ack broadcast in TS `#handleConfigUpdate`.
    ///
    /// `delete_client_ids` is every client to remove (both `activeClients`
    /// cleanup and explicit `deleted.clientIDs`); `ack_client_ids` is the subset
    /// the client explicitly asked to delete — TS only acks those (not the
    /// implicit inactive-client cleanup).
    #[allow(clippy::too_many_arguments)]
    pub async fn delete_clients(
        &mut self,
        cvr: CVR,
        shard: &ShardID,
        caller_client_id: &str,
        caller_ws_id: &str,
        delete_client_ids: &[String],
        ack_client_ids: &[String],
        ack_group_ids: &[String],
        poke_ws_ids: &[String],
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
    ) -> Result<CVR, String> {
        let mut cfg = CVRConfigDrivenUpdater::new(cvr, shard.clone());
        let mut patches: Vec<PatchToVersion> = Vec::new();
        for cid in delete_client_ids {
            // A no-op for clients not in this group (returns no patches).
            patches.extend(cfg.delete_client(cid, ttl_clock));
        }
        let (mut cfg_cvr, _stats) = cfg.flush(last_connect_time, last_active, ttl_clock);
        let expected_current_version = cfg.base.orig.version.clone();
        let ops = cfg.base.drain_store_ops();

        // deleteClients produces config ops (client removal + desire
        // inactivation), not row writes — but snapshot the CVR rows anyway so the
        // store flush's row dedup is correct regardless.
        // No eager row-map read: TS's `deleteClients` path routes through
        // `#handleConfigUpdate` and never touches the row records
        // (view-syncer.ts:1032-1050).
        let clients = self.get_clients(poke_ws_ids);
        {
            // ORDER IS THE PORT (AGENTS.md rule 8) — the SECOND port of TS
            // `#updateCVRConfig`. `deleteClients` routes through
            // `#handleConfigUpdate` → `#updateCVRConfig` (view-syncer.ts:1046),
            // which FLUSHES FIRST and pokes only when the version actually
            // advanced (view-syncer.ts:1140-1159). Opening the pokers at the
            // optimistically bumped version and reverting underneath them on a
            // quiet commit is what closed clients with `Patches were sent but
            // finalVersion ... is not greater than baseVersion` on the sibling
            // `handle_config_update` path. Both ports of the same TS function
            // must have the same order.
            //
            // NOTE the contrast with `hydrate_and_sync` / `advance_and_sync`:
            // TS `#addAndRemoveQueries` (2226/2345/2360) and `#advancePipelines`
            // (2594/2617/2622) genuinely DO start the poke before the flush and
            // end at the post-flush version — those rust sites stay poke-first
            // because that is their TS shape.
            let cfg_arc = Arc::new(cfg_cvr);
            let store_flushed = self
                .flush_ops_to_store(
                    ops,
                    &expected_current_version,
                    cfg_arc.clone(),
                    last_connect_time,
                )
                .await?;
            // No-op flush (e.g. every requested client was foreign to this
            // group) → stay on the original CVR (see `flush_to_store`).
            cfg_cvr = if store_flushed {
                Arc::try_unwrap(cfg_arc).unwrap_or_else(|a| (*a).clone())
            } else {
                cfg.base.orig.clone()
            };
            // TS `cmpVersions(cvr.version, this.#cvr.version) < 0`.
            if cmp_versions(
                &Some(expected_current_version.clone()),
                &Some(cfg_cvr.version.clone()),
            ) == std::cmp::Ordering::Less
            {
                // Like the config poke (`config_poke_targets`): only clients AT
                // the pre-delete CVR version get this delta poke. A lagging
                // reconnect must keep its old cookie for `catchup_clients` —
                // ending a poke at the new version here would jump it over its
                // catch-up interval.
                let poke_clients =
                    Self::config_poke_targets(clients.clone(), &expected_current_version);
                let refs: Vec<&ClientHandler> = poke_clients.iter().map(|c| c.as_ref()).collect();
                let pokers = MultiPoker::new(&refs, cfg_cvr.version.clone(), "config-refs");
                for p in &patches {
                    pokers.add_patch(p);
                }
                pokers.end(cfg_cvr.version.clone());
            }
        }

        // TS `#updateCVRConfig` (view-syncer.ts:1160-1167): once the config
        // change is flushed and poked, `if (this.#pipelinesSynced) await
        // this.#syncQueryPipelineSet(lc, this.#cvr, 'missing', connCtx)` with
        // the CALLER's connection context (`deleteClients` hands
        // `mustGetConnectionContext(selector)` to `#handleConfigUpdate`,
        // view-syncer.ts:1046). Runs BEFORE the ack, as in TS.
        if self.pipelines_synced {
            let (permissions, auth_data, custom_ctx, state_version, replica_version) = self
                .sync_query_pipeline_set_inputs(&cfg_cvr, Some((caller_client_id, caller_ws_id)));
            let original_client_versions: std::collections::HashMap<String, NullableCVRVersion> =
                self.get_clients(poke_ws_ids)
                    .iter()
                    .map(|c| (c.ws_id.clone(), c.version()))
                    .collect();
            cfg_cvr = self
                .sync_query_pipeline_set(
                    cfg_cvr,
                    CustomQueryTransformMode::Missing,
                    poke_ws_ids,
                    shard,
                    permissions.as_ref(),
                    &auth_data,
                    custom_ctx.as_ref(),
                    state_version,
                    replica_version,
                    last_connect_time,
                    last_active,
                    ttl_clock,
                    original_client_versions,
                )
                .await?;
        }

        // Broadcast the deleteClients ack (TS acks only explicit client-requested
        // deletions + deleted client groups, not implicit inactive cleanup).
        if !ack_client_ids.is_empty() || !ack_group_ids.is_empty() {
            for c in &clients {
                if let Err(e) =
                    c.send_delete_clients(ack_client_ids.to_vec(), ack_group_ids.to_vec())
                {
                    tracing::warn!("send_delete_clients failed: {e}");
                }
            }
        }
        Ok(cfg_cvr)
    }
}

// ─── RowChange → CVR maps ────────────────────────────────────────────────────

/// Convert a `rust_ivm` `RowChange` into the `(change_type, query_id, table,
/// row_key, row)` shape `ChangeProcessor::on_row_change` expects. Rust-only
/// adapter between the two crates (AGENTS.md rule 5).
type RowChangeMaps = (
    RowChangeType,
    String,
    String,
    serde_json::Map<String, serde_json::Value>,
    Option<serde_json::Map<String, serde_json::Value>>,
);

/// Extract per-table client-declared primary keys from a client schema JSON
/// (`{tables: {<name>: {primaryKey: [..]}}}`). Port of the `clientSchema.tables`
/// half of TS `buildPrimaryKeys`. Tables with an empty/absent primary key are
/// skipped (emission then falls back to the IVM `keyCmp[0]` for them).
fn client_primary_keys_from_schema(
    client_schema: &serde_json::Value,
) -> HashMap<String, Vec<String>> {
    let mut out: HashMap<String, Vec<String>> = HashMap::new();
    let Some(tables) = client_schema.get("tables").and_then(|v| v.as_object()) else {
        return out;
    };
    for (name, table) in tables {
        if let Some(pk) = table.get("primaryKey").and_then(|v| v.as_array()) {
            let cols: Vec<String> = pk
                .iter()
                .filter_map(|v| v.as_str())
                .map(str::to_string)
                .collect();
            if !cols.is_empty() {
                out.insert(name.clone(), cols);
            }
        }
    }
    out
}

/// The custom-query name for a query id, or `None` for internal/client queries.
/// Mirrors TS `query.type === 'custom' ? query.name : undefined`, used to label
/// shadow-mode coverage log entries.
fn query_name_of(cvr: &CVR, qid: &str) -> Option<String> {
    match cvr.queries.get(qid) {
        Some(QueryRecord::Custom(r)) => Some(r.name.clone()),
        _ => None,
    }
}

/// Maps `ivm::ChangeType` → the CVR `RowChangeType`. Returns `None` for `Child`,
/// which the streamer never emits at the row level (see `streamer::stream_nodes`,
/// which only streams Add/Remove/Edit) — skipping it preserves the prior
/// `on_row_change` behavior of ignoring non-row changes, without a panic.
fn row_change_to_maps(rc: &rust_ivm::streamer::RowChange) -> Option<RowChangeMaps> {
    let change_type = match rc.change_type {
        rust_ivm::ivm::change::ChangeType::Add => RowChangeType::Add,
        rust_ivm::ivm::change::ChangeType::Remove => RowChangeType::Remove,
        rust_ivm::ivm::change::ChangeType::Edit => RowChangeType::Edit,
        rust_ivm::ivm::change::ChangeType::Child => return None,
    };
    let row_key = {
        let mut m = serde_json::Map::with_capacity(rc.row_key.len());
        for (k, v) in rc.row_key.iter() {
            m.insert(k.to_string(), value_to_serde_json(v));
        }
        m
    };
    let row = rc.row.as_ref().map(|r| {
        let mut m = serde_json::Map::with_capacity(r.len());
        for (k, v) in r.iter() {
            m.insert(k.to_string(), value_to_serde_json(v));
        }
        m
    });
    Some((
        change_type,
        rc.query_id.clone(),
        rc.table.clone(),
        row_key,
        row,
    ))
}

/// XOR-fold a streamed `RowChange` into a per-query row-set-signature
/// accumulator, mirroring the engine's `add_queries` fold: every non-Edit change
/// (Add or Remove) XORs the table+rowKey unit, so a Remove undoes a prior Add.
/// Uses the original `rust_ivm` row key (not the JSON-converted one) so the hash
/// matches `row_signature_unit` byte-for-byte.
fn accumulate_signature(acc: &mut HashMap<String, u64>, rc: &rust_ivm::streamer::RowChange) {
    if rc.change_type != rust_ivm::ivm::change::ChangeType::Edit {
        let unit = rust_ivm::row_signature_unit(&rc.table, &rc.row_key);
        // TS reads then writes the SAME key reference
        // (`#rowSetSignatures.get(change.queryID) ?? 0n` … `.set(change.queryID,
        // cur ^ unit)`, pipeline-driver.ts:889-895); a JS Map key is a
        // reference, so TS allocates nothing per row here. `entry(k.clone())`
        // takes its key eagerly and so allocated a fresh `String` for EVERY row
        // change even once the query was present — `query_id` is a per-query
        // constant on a per-row path. Looking up first keeps the clone for the
        // first row of each query only. `0 ^ unit == unit`, so seeding the
        // absent entry with `unit` is TS's `?? 0n` followed by the XOR.
        match acc.get_mut(&rc.query_id) {
            Some(sig) => *sig ^= unit,
            None => {
                acc.insert(rc.query_id.clone(), unit);
            }
        }
    }
}

/// Convert a `rust_ivm` `Value` to `serde_json::Value`, matching TS
/// `JSON.stringify` semantics. Rust-only adapter (AGENTS.md rule 5).
fn value_to_serde_json(v: &rust_ivm::ivm::data::Value) -> serde_json::Value {
    use rust_ivm::ivm::data::Value;
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::F64(n) => {
            if n.fract() == 0.0 && n.is_finite() && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                serde_json::Value::Number((*n as i64).into())
            } else if let Some(num) = serde_json::Number::from_f64(*n) {
                serde_json::Value::Number(num)
            } else {
                sqlite_real_to_json(*n)
            }
        }
        Value::Str(s) => serde_json::Value::String(s.to_string()),
        Value::Json(j) => {
            serde_json::from_str(j).unwrap_or_else(|_| serde_json::Value::String(j.to_string()))
        }
    }
}

fn sqlite_real_to_json(value: f64) -> serde_json::Value {
    serde_json::Number::from_f64(value)
        .map(serde_json::Value::Number)
        .unwrap_or_else(|| {
            let encoded = if value.is_nan() {
                "NaN"
            } else if value.is_sign_negative() {
                "-Infinity"
            } else {
                "Infinity"
            };
            serde_json::json!({ "__rustIvmSqliteReal": encoded })
        })
}

/// The reserved replica version column stripped from row contents before they
/// are sent to clients (TS `contentsAndVersion` / `ZERO_VERSION_COLUMN_NAME`).
const ZERO_VERSION_COLUMN: &str = "_0_version";

/// Convert an engine `Row` into the `contents` value for a row PUT patch,
/// dropping the `_0_version` column. Port of TS `contentsAndVersion(row)`.
fn row_to_contents(row: &rust_ivm::ivm::data::Row) -> serde_json::Value {
    let mut m = serde_json::Map::with_capacity(row.len());
    for (k, v) in row.iter() {
        if k == ZERO_VERSION_COLUMN {
            continue;
        }
        m.insert(k.clone(), value_to_serde_json(v));
    }
    serde_json::Value::Object(m)
}

/// Build a fresh, empty CVR for a client group (used when there is no store to
/// load from, e.g. dev/tests). Real deployments load via `SyncEngine::load_cvr`.
pub fn empty_cvr(id: &str, replica_version: &str) -> CVR {
    CVR {
        id: id.to_string(),
        version: CVRVersion {
            state_version: "00".to_string(),
            config_version: None,
        },
        last_active: 0,
        ttl_clock: 0,
        replica_version: Some(replica_version.to_string()),
        clients: BTreeMap::new(),
        queries: BTreeMap::new(),
        client_schema: None,
        profile_id: None,
    }
}

/// Port of the TS `#addAndRemoveQueries` force-bump guard (view-syncer.ts:
/// 2182-2214): when an already-gotten, same-transformation-hash query is being
/// re-executed and `track_queries` would NOT otherwise bump the `configVersion`
/// (no `stateVersion` advance, no removals, no hash change), the caller must
/// `ensure_new_version()` before `track_queries` so a row diff from `received()`
/// gets a fresh `patchVersion` (the `#assertNewVersion` invariant; skipping it is
/// the prod no-bump wedge). Returns `Some(reason)` when a bump must be forced —
/// `reason` is TS's `drifted && missing ? 'mixed' : drifted ?
/// 'row-set-signature-drift' : 'missing-pipeline'` (view-syncer.ts:2203-2212),
/// keyed off `drifted_query_ids` from `hydrate_unchanged_queries` — else `None`.
/// Pure, so it can be unit-tested against the TS golden scenarios.
fn same_hash_rehydration_bump_reason(
    cvr: &CVR,
    add_queries: &[(String, String)],
    remove_queries: &[String],
    state_version: &str,
    drifted_query_ids: &std::collections::HashSet<String>,
) -> Option<&'static str> {
    let cvr_hash = |id: &str| -> Option<String> {
        cvr.queries
            .get(id)
            .and_then(|q| q.base().transformation_hash.clone())
    };
    // sameHashRehydratedQueryIDs = addQueries whose CVR-stored transformation
    // hash equals the new one (view-syncer.ts:2182-2186).
    let same_hash: Vec<&str> = add_queries
        .iter()
        .filter(|(id, hash)| cvr_hash(id).as_deref() == Some(hash.as_str()))
        .map(|(id, _)| id.as_str())
        .collect();
    // trackQueriesWillBumpVersion (view-syncer.ts:2187-2192).
    let track_queries_will_bump_version = state_version > cvr.version.state_version.as_str()
        || !remove_queries.is_empty()
        || add_queries
            .iter()
            .any(|(id, hash)| cvr_hash(id).as_deref() != Some(hash.as_str()));
    if same_hash.is_empty() || track_queries_will_bump_version {
        return None;
    }
    // Reason label (view-syncer.ts:2203-2212).
    let drifted = same_hash
        .iter()
        .filter(|id| drifted_query_ids.contains(**id))
        .count();
    let missing = same_hash.len() - drifted;
    Some(if drifted > 0 && missing > 0 {
        "mixed"
    } else if drifted > 0 {
        "row-set-signature-drift"
    } else {
        "missing-pipeline"
    })
}

// NOTE: a `parse_existing_rows(json) -> RowRecordMap` helper once lived here
// (pre-rust-cvr existing-rows parsing). It had no TS twin — TS loads the CVR
// row records via `CVRStore.load` — and no remaining caller after `CVRStore`
// took over loading; removed as dead drift.

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "tests/view_syncer_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/view_syncer_engine_tests.rs"]
mod engine_tests;
