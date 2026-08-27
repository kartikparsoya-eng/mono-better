//! Connection router — port of the connection lifecycle in `syncer.ts`.
//!
//! Routes incoming WebSocket connections to the appropriate client group. Each
//! CG runs as a `spawn_local` task on one of the `K` sharded executor threads
//! (doc 91 — there is no per-CG OS thread). The router maintains a
//! `DashMap<client_group_id, CGHandle>` for lookup.
//!
//! Connection lifecycle (port of `Syncer.#createConnection`):
//! 1. Auth validation (JWT) — BEFORE touching existing connections
//! 2. User ID pinning check — reject if group is pinned to a different user
//! 3. Close existing connection for same clientID (replacement)
//! 4. Register connection in context manager
//! 5. Send `connected` on the ACCEPT task (`handle_connection`), BEFORE the
//!    connection is handed to the serial CG thread — TS parity with
//!    `syncer.ts#handleConnection` sending `connection.init()`'s `connected`
//!    before `await handleInitConnection`. This decouples the connect-ack from
//!    `config_and_hydrate` (see `handle_connection`; version gate in
//!    `ws_server::accept_connection`, both TS `Connection.init()` effects).
//! 6. Create Connection + MessageHandler on the CG thread (version gate only)
//! 7. Handle piggybacked `initConnection` from sec-websocket-protocol header

use crate::custom_queries::transform_query::CustomQueryContext;
use crate::services::view_syncer::connection_context_manager::{
    ConnectParamsForRegistration, ConnectionContext as CcmConnectionContext,
    ConnectionContextManager, ConnectionSelector as CcmConnectionSelector, FetchConfig,
    InitConnectionBody, UpdateAuthBody, resolve_auth,
};
use crate::services::view_syncer::pipeline_driver::{IvmPipelines, IvmTableSpec};
use crate::sync_engine::{SyncEngine, empty_cvr};
use crate::workers::connect_params::ConnectParams;
use crate::workers::connection::Connection;
use crate::workers::syncer_ws_message_handler::{
    ConnContextManagerDispatch, ConnectionSelector, MutagenDispatch, PusherDispatch,
    SyncerWsMessageHandler, ViewSyncerDispatch,
};
use crate::ws_server::ConnectionContext;
use crate::ws_sink::DirectWebSocketSink;
use dashmap::DashMap;
use rust_cvr::cvr::{CVR, DesiredQuerySpec};
use rust_cvr::schema::types::{
    CVRVersion, EMPTY_CVR_VERSION, NullableCVRVersion, cmp_versions, version_string,
};
use rust_cvr::shards::ShardID;
use rust_cvr::ttl_clock::TTLClock;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::mpsc;

/// Small delay added when scheduling TTL eviction so many near-simultaneous
/// expirations collapse into one timer wakeup. Port of TS `TTL_TIMER_HYSTERESIS`.
const TTL_TIMER_HYSTERESIS_MS: i64 = 50;
/// Interval between periodic ttlClock persistence ticks. Port of TS
/// `TTL_CLOCK_INTERVAL` (view-syncer.ts:202).
const TTL_CLOCK_INTERVAL: i64 = 60_000;
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

/// Message sent to a CG thread's unified event loop.
///
/// All inputs — new connections, inbound WS frames, disconnects, change-streamer
/// notifications, shutdown — flow through this single channel so the CG thread
/// is a non-blocking single-threaded event loop (doc 89's CG dispatch model),
/// rather than blocking on one connection at a time.
pub enum CGMessage {
    /// A new connection was accepted — set it up (send `connected`, register a
    /// client handler with the SyncEngine).
    NewConnection {
        params: Box<ConnectParams>,
        sink: DirectWebSocketSink,
    },
    /// An inbound WS text frame for a connection (forwarded from the WS reader).
    /// `client_id`/`ws_id` are `Arc<str>` so the per-frame forward is a refcount
    /// bump, not a fresh `String` allocation on the busiest path.
    Inbound {
        client_id: Arc<str>,
        ws_id: Arc<str>,
        text: String,
    },
    /// A connection's WS closed (its upstream channel ended).
    ConnectionClosed {
        client_id: Arc<str>,
        ws_id: Arc<str>,
    },
    /// Explicitly close a superseded socket before installing its replacement.
    CloseConnection {
        client_id: Arc<str>,
        ws_id: Arc<str>,
    },
    /// Change-streamer notification — new data is available; advance + poke.
    Notification(serde_json::Value),
    /// The CG should shut down (no more connections).
    Shutdown,
}

/// Handle to a client group's async task, which runs on one of the `K` shared
/// executor threads (doc 91). The task itself is a `spawn_local` future on its
/// executor's `LocalSet`; this handle only carries the channel + shared counters
/// used to route to it and account for it. There is no per-CG OS thread and thus
/// no per-CG `JoinHandle` — draining is done by shutting down the executors.
pub struct CGHandle {
    /// Sender for messages to the CG task. Sends are synchronous on an unbounded
    /// tokio sender (no `.await`), so the router's async tasks never block here.
    tx: mpsc::UnboundedSender<CGMessage>,
    /// Number of active connections on this CG.
    connection_count: Arc<AtomicU64>,
    accepting: Arc<AtomicBool>,
    /// Index into `ConnectionRouter::executors` of the executor hosting this CG.
    /// Fixed at placement for the group's lifetime (the `!Send` `SyncEngine` is
    /// pinned to that one thread). Read by `place_cg` to compute per-executor
    /// load; carried on returned/cloned handles only for struct consistency.
    executor_idx: usize,
}

impl CGHandle {
    /// Send a message to the CG task. Sends are synchronous on an unbounded
    /// tokio sender (no `.await`), so the router's async tasks never block here.
    pub fn send(&self, msg: CGMessage) -> Result<(), mpsc::error::SendError<CGMessage>> {
        if !self.accepting.load(Ordering::SeqCst) {
            return Err(mpsc::error::SendError(msg));
        }
        self.tx.send(msg)
    }

    /// Ask the CG task to shut down. Non-blocking: the task fails its sockets with
    /// a Rehome error and terminates on its executor; the executor's own
    /// drain-join (see [`ConnectionRouter::shutdown`]) is what guarantees the task
    /// has finished before the process exits.
    pub fn shutdown(&mut self) {
        self.accepting.store(false, Ordering::SeqCst);
        let _ = self.tx.send(CGMessage::Shutdown);
    }

    /// Number of active connections.
    pub fn connection_count(&self) -> u64 {
        self.connection_count.load(Ordering::Relaxed)
    }
}

/// Control-plane command sent from the router (any thread) to one executor.
enum ExecutorCommand {
    /// Host a new client group on this executor: build its `!Send` `SyncEngine`
    /// (bound to the executor's pool) and `spawn_local` its event loop.
    SpawnCg {
        cg_id: String,
        rx: mpsc::UnboundedReceiver<CGMessage>,
        /// Same sender stored in the `CGHandle`, used only for the identity check
        /// when the task removes its own map entry on exit.
        self_tx: mpsc::UnboundedSender<CGMessage>,
        connection_count: Arc<AtomicU64>,
        accepting: Arc<AtomicBool>,
        /// The newest broadcast notification at creation time, used to arm the
        /// group's serving-lag tracker (TS notifier latest-state replay).
        last_notification: Option<serde_json::Value>,
        /// Process-wide serving-lag registry this CG publishes its snapshot into.
        serving_lag_registry: Arc<crate::workers::syncer::ServingLagRegistry>,
    },
    /// Stop accepting new groups and drain the ones already hosted, then exit.
    Shutdown,
}

/// Router-side handle to one executor thread.
struct Executor {
    ctrl_tx: mpsc::UnboundedSender<ExecutorCommand>,
    /// Joined once, during [`ConnectionRouter::shutdown`]. Behind a `Mutex<Option>`
    /// so `shutdown(&self)` can take ownership of the handle to join it.
    join: Mutex<Option<JoinHandle<()>>>,
    /// Set when a `SpawnCg` send finds the control channel closed — i.e. the
    /// executor thread died. A dead executor hosts 0 groups, so without this
    /// flag `place_cg` would rank it least-loaded FOREVER and every new client
    /// group process-wide would fail placement and rehome — a half-dead state
    /// invisible to the load balancer. Dead executors are excluded from
    /// placement; existing groups on other executors are unaffected.
    dead: AtomicBool,
}

/// Default executor count: one per available core, matching the design's
/// `K ≈ num_cores`. Falls back to 4 if the platform can't report parallelism.
fn default_num_shards() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .max(1)
}

/// Stable hash of a client group into `[0, num_shards)`. Uses a fixed-seed
/// `DefaultHasher` (not `RandomState`), so the result is deterministic within a
/// process run. Used by [`ConnectionRouter::place_cg`] to break ties among
/// equally-loaded executors so a cold/uniform system still spreads groups.
fn shard_for(cg_id: &str, num_shards: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    cg_id.hash(&mut h);
    (h.finish() % num_shards as u64) as usize
}

/// CVR Postgres store identity for a client group.
///
/// This carries only the *identity* of the CVR (schema + ids); the `PgPool` it
/// binds to is the ONE process-wide shared pool, built on the main runtime and
/// handed to each executor (doc 91 Iteration C). CVR I/O is offloaded onto that
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
    /// SQLite replica path; `None` selects in-memory sources (test/dev).
    pub replica_path: Option<String>,
    pub app_id: String,
    /// Immutable creation version from `_zero.replicationConfig`. This is not
    /// the live snapshot watermark.
    pub replica_version: String,
    pub shard: ShardID,
    pub cvr_pg: Option<CvrPgConfig>,
    /// Compiled read-permissions (`PermissionsConfig` JSON) loaded from the
    /// replica, or `None` if none are deployed (queries pass through).
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
    /// Create the ViewSyncer dispatch for a new CG (per-connection message
    /// handler path).
    fn create_view_syncer(&self, client_group_id: &str) -> Arc<dyn ViewSyncerDispatch>;

    /// Create the connection context manager for a new CG.
    fn create_conn_context_manager(
        &self,
        client_group_id: &str,
    ) -> Arc<dyn ConnContextManagerDispatch>;

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

/// Group auth state — tracks the pinned user for a client group.
///
/// Port of `GroupAuthState` in `connection-context-manager.ts`.
#[derive(Debug, Clone, Default)]
pub struct GroupAuthState {
    /// The user ID that this client group is pinned to.
    /// `None` = no user has been validated yet.
    pub pinned_user_id: Option<String>,
}

/// Check the incoming userID against the group's pin and, on the first
/// connection, BIND it. `Ok` = allowed (and now pinned); `Err` = the group is
/// already pinned to a different userID and the connection must be rejected.
/// Port of the pin logic in TS `ConnectionContextManager.validateConnection`.
fn check_and_pin_user(group: &mut GroupAuthState, incoming: &str) -> Result<(), ()> {
    match group.pinned_user_id.clone() {
        Some(pinned) if pinned != incoming => Err(()),
        Some(_) => Ok(()),
        None => {
            group.pinned_user_id = Some(incoming.to_string());
            Ok(())
        }
    }
}

/// The connection router — hosts client groups on a bounded pool of `K` executor
/// threads and routes connections to them (doc 91, sharded async executors).
///
/// Port of the `Syncer` class's connection management.
pub struct ConnectionRouter {
    /// Map of client_group_id → CG handle.
    cg_handles: Arc<DashMap<String, CGHandle>>,
    /// Serializes lookup/create/evict so two first connections cannot register
    /// two tasks for the same client group.
    cg_creation_lock: Arc<Mutex<()>>,
    max_client_groups: usize,
    /// The `K` executor threads. A new client group is placed on the least-loaded
    /// executor (see [`place_cg`](Self::place_cg)) and hosted there for its
    /// lifetime, pinning its `!Send` `SyncEngine` to one thread by construction.
    /// Each executor holds its own clone of the services factory, so the router
    /// does not retain one.
    executors: Vec<Executor>,
    /// Auth validator (used to validate a connection's JWT before admission).
    auth_validator: Arc<dyn AuthValidator>,
    /// Shared process metrics (read by `/statz`, written by CG threads).
    metrics: Arc<crate::metrics::Metrics>,
    /// Active connections: client_id → connection info.
    connections: Arc<Mutex<HashMap<String, ConnectionInfo>>>,
    /// Group auth states: client_group_id → GroupAuthState.
    group_auth_states: Arc<Mutex<HashMap<String, GroupAuthState>>>,
    /// The most recent broadcast notification. A client group created AFTER the
    /// last commit would otherwise never learn that commit's watermark/commit
    /// time until the NEXT commit — TS's in-process notifier replays the latest
    /// `ReplicaState` to every new subscriber (notifier.ts). Handed to each
    /// newly spawned CG to arm its serving-lag tracker.
    last_notification: Arc<Mutex<Option<serde_json::Value>>>,
    /// Process-wide serving-lag state (replica-ready log + per-CG snapshots),
    /// read by the 60s sampler + the `serving_lag*`/`queries`/`rows` gauges. Port
    /// of the `Syncer` class's `#replicaReadyStates` + view-syncer iteration.
    serving_lag_registry: Arc<crate::workers::syncer::ServingLagRegistry>,
    /// Whether the router is shutting down.
    shutting_down: Arc<AtomicBool>,
    /// Server shard identity ({appID, shardNum}). Read on the accept task to
    /// build the `connected` message body (`handle_connection`). TS reads the
    /// same from the shard config in `syncer.ts#handleConnection` when
    /// constructing `['connected', {wsid, timestamp, appID, shardNum}]`.
    shard: ShardID,
}

/// Info about an active connection.
#[derive(Clone)]
struct ConnectionInfo {
    client_group_id: String,
    ws_id: String,
    /// The connection's downstream sink. Cloneable + `Send + Sync` (an
    /// `UnboundedSender` + `Arc<SinkLimits>`), so services running on the tokio
    /// runtime — e.g. the push relay drainer — can deliver a frame to this
    /// client's socket without reaching into the CG executor threads.
    sink: DirectWebSocketSink,
}

/// Sink registry handed to services that must deliver frames to a specific
/// client's socket from the tokio runtime (the push-relay drainer, which learns
/// of a POST failure long after the message-handling path has returned). Wraps
/// the router's live connection map, so delivery follows the exact
/// insert/remove lifecycle already maintained for `ConnectionInfo` — no second
/// structure to leak. Delivery is `ws_id`-guarded (see `send_error_if_current`).
#[derive(Clone)]
pub struct ConnectionSinks(Arc<Mutex<HashMap<String, ConnectionInfo>>>);

impl ConnectionSinks {
    /// A fresh, empty registry. Prod shares one instance between the router
    /// (which populates it) and the services factory (which hands it to the
    /// pusher); tests/other constructors get their own.
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    /// Send a non-fatal error frame to `client_id`'s CURRENT socket iff it is
    /// still `ws_id`. Never closes the connection. Returns whether delivered.
    ///
    /// The `ws_id` guard matters: by the time a relay POST fails the client may
    /// have reconnected (new socket, same `client_id`). The replacement
    /// connection re-pushes anything above the server lmid on reconnect, so
    /// failing the *new* socket for the *old* socket's push would be a spurious
    /// disconnect. Rust is deliberately stricter here than TS (which routes by
    /// `clientID` only) — a documented, strictly-safer divergence.
    pub fn send_error_if_current(
        &self,
        client_id: &str,
        ws_id: &str,
        error: &crate::protocol::ErrorBody,
    ) -> bool {
        let sink = {
            let conns = lock_unpoisoned(&self.0);
            match conns.get(client_id) {
                Some(info) if info.ws_id == ws_id => info.sink.clone(),
                _ => {
                    tracing::debug!(
                        client_id,
                        ws_id,
                        "push-failure target is no longer the current socket; dropping frame"
                    );
                    return false;
                }
            }
            // guard dropped here — never hold the lock across the push
        };
        sink.push(crate::protocol::error_message(error));
        true
    }
}

impl Default for ConnectionSinks {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl ConnectionSinks {
    /// Register a sink under `client_id`/`ws_id` for tests that exercise
    /// delivery without going through the full connection-admission path.
    pub(crate) fn insert_for_test(&self, client_id: &str, ws_id: &str, sink: DirectWebSocketSink) {
        lock_unpoisoned(&self.0).insert(
            client_id.to_string(),
            ConnectionInfo {
                client_group_id: "cg-test".to_string(),
                ws_id: ws_id.to_string(),
                sink,
            },
        );
    }
}

impl ConnectionRouter {
    pub fn new(
        services_factory: Arc<dyn CGServicesFactory>,
        auth_validator: Arc<dyn AuthValidator>,
        metrics: Arc<crate::metrics::Metrics>,
    ) -> Self {
        Self::new_with_limit(services_factory, auth_validator, metrics, 100)
    }

    /// Construct with a client-group cap but no CVR pool and the default executor
    /// count. Used by tests and in-memory dev (storeless CGs).
    pub fn new_with_limit(
        services_factory: Arc<dyn CGServicesFactory>,
        auth_validator: Arc<dyn AuthValidator>,
        metrics: Arc<crate::metrics::Metrics>,
        max_client_groups: usize,
    ) -> Self {
        Self::new_sharded(
            services_factory,
            auth_validator,
            metrics,
            max_client_groups,
            default_num_shards(),
            None,
            ConnectionSinks::new(),
            // Storeless/test default; the real shard is threaded in from `main`.
            ShardID {
                app_id: "zero".to_string(),
                shard_num: 0,
            },
        )
    }

    /// Full constructor: spawn `num_shards` executor threads, each running a
    /// `current_thread` runtime + `LocalSet` hosting a hash-shard of client
    /// groups (doc 91). `cvr_pool` is the ONE shared CVR `PgPool` (built on the
    /// process's main runtime); a clone is handed to every executor so groups
    /// draw from a single bounded connection budget, and CVR I/O is offloaded
    /// back onto that pool's runtime (`SyncEngine::offload`). `None` selects
    /// storeless CGs (tests / no-PG dev).
    #[allow(clippy::too_many_arguments)]
    pub fn new_sharded(
        services_factory: Arc<dyn CGServicesFactory>,
        auth_validator: Arc<dyn AuthValidator>,
        metrics: Arc<crate::metrics::Metrics>,
        max_client_groups: usize,
        num_shards: usize,
        cvr_pool: Option<sqlx::PgPool>,
        connection_sinks: ConnectionSinks,
        shard: ShardID,
    ) -> Self {
        let num_shards = num_shards.max(1);
        let cg_handles: Arc<DashMap<String, CGHandle>> = Arc::new(DashMap::new());
        // Share the registry's map so the pusher (given a clone of the same
        // `ConnectionSinks`) sees the connections this router admits.
        let connections = connection_sinks.0.clone();

        let shutting_down = Arc::new(AtomicBool::new(false));
        let mut executors = Vec::with_capacity(num_shards);
        for idx in 0..num_shards {
            let (ctrl_tx, ctrl_rx) = mpsc::unbounded_channel::<ExecutorCommand>();
            let factory = services_factory.clone();
            let validator = auth_validator.clone();
            let conns = connections.clone();
            let handles = cg_handles.clone();
            let pool = cvr_pool.clone();
            let shutdown_flag = shutting_down.clone();
            let join = std::thread::Builder::new()
                .name(format!("cg-exec-{idx}"))
                .spawn(move || {
                    run_executor(idx, ctrl_rx, factory, validator, conns, handles, pool);
                    // An executor thread must outlive the process outside of
                    // shutdown. Before this line, a dead shard was discovered
                    // only when a later CG *placement* happened to target it —
                    // operators learned about it from tail latency. Make the
                    // death loud and countable the moment it happens.
                    if !shutdown_flag.load(Ordering::SeqCst) {
                        tracing::error!(
                            "CG executor {idx} exited outside shutdown — its client groups are                              orphaned until their clients reconnect and re-place"
                        );
                        crate::metrics::record_fail_group("executor_exit");
                    }
                })
                .expect("failed to spawn CG executor thread");
            executors.push(Executor {
                ctrl_tx,
                join: Mutex::new(Some(join)),
                dead: AtomicBool::new(false),
            });
        }

        Self {
            cg_handles,
            cg_creation_lock: Arc::new(Mutex::new(())),
            max_client_groups: max_client_groups.max(1),
            executors,
            auth_validator,
            metrics,
            connections,
            group_auth_states: Arc::new(Mutex::new(HashMap::new())),
            last_notification: Arc::new(Mutex::new(None)),
            serving_lag_registry: Arc::new(crate::workers::syncer::ServingLagRegistry::new()),
            shutting_down,
            shard,
        }
    }

    /// The process-wide serving-lag registry (for `main` to register its gauges
    /// + spawn the 60s sampler).
    pub fn serving_lag_registry(&self) -> Arc<crate::workers::syncer::ServingLagRegistry> {
        self.serving_lag_registry.clone()
    }

    /// A JSON snapshot of the process metrics (for `/statz`).
    pub fn metrics_snapshot(&self) -> serde_json::Value {
        self.metrics.snapshot()
    }

    /// Prometheus text-format metrics (for `/metrics`), including the live
    /// active-client-groups gauge.
    pub fn metrics_prometheus(&self) -> String {
        self.metrics.render_prometheus(self.cg_count() as u64)
    }

    /// Handle a new WebSocket connection.
    ///
    /// Port of `Syncer.#createConnection()`.
    /// This runs on the tokio runtime (async) because auth validation
    /// may require HTTP fetches (JWKS).
    pub async fn handle_connection(&self, ctx: ConnectionContext) {
        if self.shutting_down.load(Ordering::SeqCst) {
            ctx.sink
                .fail(crate::protocol::ErrorBody::rehome("Server is draining"));
            return;
        }
        let client_id = ctx.params.client_id.clone();
        let client_group_id = ctx.params.client_group_id.clone();
        let ws_id = ctx.params.ws_id.clone();
        let user_id = ctx.params.user_id.clone();
        let auth = ctx.params.auth.clone();
        let pv = ctx.params.protocol_version;

        tracing::debug!(
            "creating connection: cg={client_group_id}, client={client_id}, ws={ws_id}"
        );

        // 1. Validate auth BEFORE touching existing connections.
        // This prevents unauthenticated attackers from force-disconnecting
        // legitimate users via DoS.
        if let Some(auth_str) = &auth
            && !auth_str.is_empty()
        {
            match self
                .auth_validator
                .validate_auth(
                    &client_group_id,
                    &client_id,
                    user_id.as_deref(),
                    Some(auth_str),
                )
                .await
            {
                Ok(()) => {}
                Err(error_body) => {
                    tracing::warn!(
                        "Rejecting sync connection during initial auth resolution: \
                             cg={client_group_id}, client={client_id}, user={user_id:?}"
                    );
                    crate::metrics::record_ws_connection_failure(pv, "auth");
                    // Send error and close.
                    ctx.sink.fail(error_body);
                    return;
                }
            }
        }

        // 2. Reserve/create the bounded CG worker before retaining any
        //    per-group auth or connection state. Rejected group IDs therefore
        //    cannot grow either map without bound.
        let cg_handle = match self.get_or_create_cg(&client_group_id) {
            Ok(handle) => handle,
            Err(message) => {
                // Shed load gracefully: REHOME the client (reconnect — a load
                // balancer can place it on another instance) rather than a hard
                // `ServerOverloaded` reject. This mirrors TS, which never rejects
                // for capacity; it drains/rehomes via DrainCoordinator. A hard
                // reject at the (formerly too-low) cap turned a reconnect blip
                // near saturation into a retry storm; Rehome is the retryable,
                // spread-the-load signal. Covers both cap-overflow and
                // executor-shutdown Errs from get_or_create_cg.
                tracing::warn!("rehoming connection for {client_group_id}: {message}");
                crate::metrics::record_ws_connection_failure(pv, "rehome");
                ctx.sink.fail(crate::protocol::ErrorBody::rehome(message));
                return;
            }
        };

        // 3. Check (and, on the first connection, BIND) the group's userID.
        //    Port of TS `ConnectionContextManager.validateConnection`: the first
        //    successful connection pins the client group to its userID; every
        //    later connection must match it. Without the bind step the check
        //    below is inert — the group is never pinned, so two different users
        //    could share one client group.
        {
            let mut states = lock_unpoisoned(&self.group_auth_states);
            // CG workers are the lifetime boundary for auth pins. Failed or
            // terminated workers may leave a pin until the next admission;
            // prune those stale entries before inserting the current group.
            states.retain(|group_id, _| self.cg_handles.contains_key(group_id));
            let group = states.entry(client_group_id.clone()).or_default();
            let incoming = user_id.as_deref().unwrap_or("");
            if check_and_pin_user(group, incoming).is_err() {
                let error = crate::protocol::ErrorBody::unauthorized(
                    "Client groups are pinned to a single userID. \
                     Connection userID does not match existing client group userID.",
                );
                tracing::warn!(
                    "User ID mismatch: pinned={:?}, incoming={incoming}",
                    group.pinned_user_id
                );
                decrement_nonzero(&cg_handle.connection_count);
                crate::metrics::record_ws_connection_failure(pv, "user_mismatch");
                ctx.sink.fail(error);
                return;
            }
        }

        // 4. Close existing connection for same clientID (replacement).
        let superseded = {
            let mut conns = lock_unpoisoned(&self.connections);
            let existing = conns.get(&client_id).cloned();
            if existing.is_some() {
                tracing::debug!(
                    "client {client_id} already connected, closing existing connection"
                );
                conns.remove(&client_id);
            }
            conns.insert(
                client_id.clone(),
                ConnectionInfo {
                    client_group_id: client_group_id.clone(),
                    ws_id: ws_id.clone(),
                    sink: ctx.sink.clone(),
                },
            );
            existing
        };
        if let Some(existing) = superseded
            && let Some(handle) = self.cg_handles.get(&existing.client_group_id)
        {
            let _ = handle.send(CGMessage::CloseConnection {
                client_id: Arc::from(client_id.as_str()),
                ws_id: Arc::from(existing.ws_id.as_str()),
            });
        }

        // 5. Emit `connected` HERE, on the per-connection accept task, BEFORE the
        //    connection is handed to the serial CG thread. TS parity:
        //    `syncer.ts#handleConnection` sends `connection.init()`'s `connected`
        //    before `await connection.handleInitConnection` (which drives
        //    hydration). Emitting it on this task decouples the connect-ack from
        //    `config_and_hydrate`: a client whose CG thread is mid-hydrate is
        //    still acknowledged immediately and never trips its 10s connect
        //    timeout. Previously `connected` was sent by `Connection::init()`
        //    inside `on_new_connection` on the CG thread, so a reconnect arriving
        //    during an in-flight hydrate was queued behind it → connect-timeout →
        //    idle reap → cold re-hydrate thrash. The protocol version — TS
        //    `init()`'s other effect — is validated in `accept_connection` with
        //    the byte-identical `VersionNotSupported` message, so `on_new_connection`
        //    never version-checks.
        ctx.sink.push(crate::protocol::connected_message(
            &ws_id,
            &self.shard.app_id,
            self.shard.shard_num,
        ));

        // 6. Split the context: the CG thread owns connection setup + the sink,
        //    while a lightweight forwarder task funnels inbound WS frames into
        //    the CG's unified channel (so the CG loop never blocks on one conn).
        let ConnectionContext {
            params,
            sink,
            upstream_rx,
        } = ctx;
        match cg_handle.send(CGMessage::NewConnection {
            params: Box::new(params),
            sink,
        }) {
            Ok(()) => {
                tokio::spawn(forward_inbound(
                    upstream_rx,
                    cg_handle.tx.clone(),
                    Arc::from(client_id.as_str()),
                    Arc::from(ws_id.as_str()),
                ));
            }
            Err(err) => {
                tracing::error!("Failed to send connection to CG thread for {client_group_id}");
                decrement_nonzero(&cg_handle.connection_count);
                let mut conns = lock_unpoisoned(&self.connections);
                if conns
                    .get(&client_id)
                    .is_some_and(|info| info.ws_id == ws_id)
                {
                    conns.remove(&client_id);
                }
                drop(conns);
                if !self.cg_handles.contains_key(&client_group_id) {
                    lock_unpoisoned(&self.group_auth_states).remove(&client_group_id);
                }
                if let CGMessage::NewConnection { sink, .. } = err.0 {
                    sink.fail(crate::protocol::ErrorBody::rehome(
                        "Client-group worker restarted; reconnect required",
                    ));
                }
            }
        }
    }

    /// Get or create the hosting task for a client group ID. On the create path
    /// the group is placed by [`place_cg`](Self::place_cg) (least-loaded) and a
    /// `SpawnCg` is dispatched to that executor, which builds the `!Send`
    /// `SyncEngine` (bound to its own pool) and `spawn_local`s the event loop.
    fn get_or_create_cg(&self, client_group_id: &str) -> Result<Arc<CGHandle>, String> {
        let _creation = lock_unpoisoned(&self.cg_creation_lock);
        // Fast path: CG already exists.
        if let Some(handle) = self.cg_handles.get(client_group_id) {
            if !handle.accepting.load(Ordering::SeqCst) {
                drop(handle);
                if let Some((_, mut stale)) = self.cg_handles.remove(client_group_id) {
                    stale.shutdown();
                }
            } else {
                // We can't just return a reference to the DashMap entry because
                // we need to potentially create a new CG if it doesn't exist.
                // Instead, we clone the necessary parts.
                handle.connection_count.fetch_add(1, Ordering::Relaxed);
                return Ok(Arc::new(CGHandle {
                    tx: handle.tx.clone(),
                    connection_count: handle.connection_count.clone(),
                    accepting: handle.accepting.clone(),
                    executor_idx: handle.executor_idx,
                }));
            }
        }

        // Keep the process bounded. Idle groups remain warm, but are evicted on
        // demand once the configured capacity is reached.
        if self.cg_handles.len() >= self.max_client_groups {
            let idle = self
                .cg_handles
                .iter()
                .find(|entry| entry.connection_count() == 0)
                .map(|entry| entry.key().clone());
            if let Some(idle_id) = idle {
                if let Some((_, mut handle)) = self.cg_handles.remove(&idle_id) {
                    handle.shutdown();
                    lock_unpoisoned(&self.group_auth_states).remove(&idle_id);
                }
            } else {
                return Err(format!(
                    "maximum active client groups ({}) reached",
                    self.max_client_groups
                ));
            }
        }

        // Create path: allocate the group's channel + shared counters, register
        // the handle, and hand ownership of the receiver to the placed executor.
        let (tx, rx) = mpsc::unbounded_channel::<CGMessage>();
        let connection_count = Arc::new(AtomicU64::new(1));
        let accepting = Arc::new(AtomicBool::new(true));

        let mut spawn = ExecutorCommand::SpawnCg {
            cg_id: client_group_id.to_string(),
            rx,
            self_tx: tx.clone(),
            connection_count: connection_count.clone(),
            accepting: accepting.clone(),
            last_notification: lock_unpoisoned(&self.last_notification).clone(),
            serving_lag_registry: self.serving_lag_registry.clone(),
        };
        // A closed control channel means the executor THREAD died. Mark it dead
        // (so `place_cg` stops ranking its empty slot least-loaded) and retry on
        // the remaining executors instead of failing every new group forever.
        let mut placed: Option<usize> = None;
        for _ in 0..self.executors.len() {
            let shard = self.place_cg(client_group_id);
            match self.executors[shard].ctrl_tx.send(spawn) {
                Ok(()) => {
                    placed = Some(shard);
                    break;
                }
                Err(mpsc::error::SendError(returned)) => {
                    if !self.executors[shard].dead.swap(true, Ordering::SeqCst) {
                        tracing::error!(
                            "executor {shard} is dead (control channel closed); \
                             excluding it from client-group placement"
                        );
                    }
                    spawn = returned;
                }
            }
        }
        let Some(shard) = placed else {
            return Err("no executor is accepting new client groups".to_string());
        };

        self.cg_handles.insert(
            client_group_id.to_string(),
            CGHandle {
                tx: tx.clone(),
                connection_count: connection_count.clone(),
                accepting: accepting.clone(),
                executor_idx: shard,
            },
        );

        Ok(Arc::new(CGHandle {
            tx,
            connection_count,
            accepting,
            executor_idx: shard,
        }))
    }

    /// Choose the executor to host a NEW client group: the one currently hosting
    /// the fewest groups (least-loaded placement, doc 91). Replaces blind
    /// `shard_for` hashing, which is load-oblivious and leaves executors lumpy
    /// when the hash happens to cluster. A group's `!Send` `SyncEngine` pins it to
    /// its executor for life, so we balance by *placement*, never by migration
    /// (migration would force a full IVM rehydrate — rejected by design).
    ///
    /// V1 metric is **group count per executor**. Because placement is serialized
    /// under `cg_creation_lock` and the just-placed group is inserted into
    /// `cg_handles` before the lock is released, consecutive placements observe
    /// each other, so this degenerates to round-robin and keeps per-executor group
    /// counts within 1 of each other (max−min ≤ 1) absent churn. When a group is
    /// evicted (idle) or exits, it simply drops out of `cg_handles` and its slot is
    /// refilled by the next placement — no decrement bookkeeping to keep in sync.
    ///
    /// Known caveat: group count is a coarse proxy — a single hot group still pins
    /// one core and this can't correct it post-placement. A connection-weighted or
    /// advance-cost metric is a deliberate follow-up (V2/V3), not part of V1.
    ///
    /// Cost is O(N) over live groups per placement; placement is rare relative to
    /// message routing and runs under the creation lock, so this is not on any hot
    /// path. If N grows large this can move to an incremental per-executor counter.
    fn place_cg(&self, cg_id: &str) -> usize {
        let k = self.executors.len();
        let mut load = vec![0u64; k];
        for entry in self.cg_handles.iter() {
            // Defensive: an entry's executor_idx is always a valid index (set at
            // placement), but guard against an out-of-range value rather than
            // panic on the placement path.
            if let Some(slot) = load.get_mut(entry.executor_idx) {
                *slot += 1;
            }
        }
        // Only live executors are candidates — a dead one hosts 0 groups and
        // would otherwise be ranked least-loaded forever (see `Executor::dead`).
        // If EVERY executor is marked dead, fall back to all of them; the
        // subsequent send fails and the caller surfaces the error.
        let live: Vec<usize> = (0..k)
            .filter(|&i| !self.executors[i].dead.load(Ordering::SeqCst))
            .collect();
        let pool = if live.is_empty() {
            (0..k).collect::<Vec<usize>>()
        } else {
            live
        };
        let min = pool.iter().map(|&i| load[i]).min().unwrap_or(0);
        // Deterministically break ties AMONG the least-loaded executors by hashing
        // the cg_id, so a cold/uniform system still spreads groups (rather than
        // always piling the first ones onto executor 0).
        let candidates: Vec<usize> = pool.into_iter().filter(|&i| load[i] == min).collect();
        candidates[shard_for(cg_id, candidates.len())]
    }

    /// Drain and stop: fail every connection with a Rehome error (so clients
    /// reconnect elsewhere), then shut the executor threads down and join them so
    /// their CVR pools close before the process exits.
    pub async fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);

        // Ask each hosted CG to drain. The task fails its sockets with a Rehome
        // error and terminates on its executor.
        let ids: Vec<String> = self.cg_handles.iter().map(|e| e.key().clone()).collect();
        for id in ids {
            if let Some((_, mut handle)) = self.cg_handles.remove(&id) {
                handle.shutdown();
                lock_unpoisoned(&self.group_auth_states).remove(&id);
            }
        }
        lock_unpoisoned(&self.group_auth_states).clear();

        // Tell each executor to stop accepting and drain its remaining tasks,
        // then join the thread. Joining is a blocking op, so run it off the async
        // runtime via `spawn_blocking` to avoid stalling the caller's reactor.
        for exec in &self.executors {
            let _ = exec.ctrl_tx.send(ExecutorCommand::Shutdown);
        }
        let joins: Vec<JoinHandle<()>> = self
            .executors
            .iter()
            .filter_map(|exec| lock_unpoisoned(&exec.join).take())
            .collect();
        let _ = tokio::task::spawn_blocking(move || {
            for join in joins {
                let _ = join.join();
            }
        })
        .await;
    }

    /// Staggered graceful drain on SIGTERM — port of TS `Syncer.drain()`
    /// (workers/syncer.ts:732) paced by the `DrainCoordinator`. Rehomes ONE
    /// client group per drain interval instead of failing every socket at once
    /// (`shutdown`), so a deploy does not stampede the receiving servers with
    /// simultaneous reconnect+rehydrate storms.
    ///
    /// Pacing: TS re-arms each interval with the drained view-syncer's
    /// hydration time; the router does not track per-CG hydration time, so the
    /// drain budget is spread evenly across the live groups instead. The whole
    /// drain is bounded by `MAX_DRAIN_MS`: the parent ProcessManager
    /// (life-cycle.ts) waits indefinitely for the child after SIGTERM, but
    /// orchestrators SIGKILL after their stop-grace period (commonly 30s), so
    /// staying inside it keeps the final `shutdown()` sweep + executor join
    /// (and e.g. a dhat profile dump) graceful.
    pub async fn drain(&self) {
        /// Upper bound on the elective/staggered phase; the final sweep runs after.
        const MAX_DRAIN_MS: u64 = 25_000;

        // Refuse new connections for the whole drain, not just the final
        // sweep — a socket accepted mid-drain would only be rehomed moments
        // later anyway.
        self.shutting_down.store(true, Ordering::SeqCst);

        let start = std::time::Instant::now();
        let deadline = start + std::time::Duration::from_millis(MAX_DRAIN_MS);
        let total = self.cg_handles.len() as u64;
        tracing::info!("draining {total} client groups");

        if total > 0 {
            let coordinator =
                crate::services::view_syncer::drain_coordinator::DrainCoordinator::new();
            // Kick off with `drainNextIn(0)` (TS Syncer.drain): the first
            // force-drain timeout fires ~immediately, then each drained CG
            // re-arms it for the next interval.
            coordinator.drain_next_in(0);
            // Spacing such that the full sweep fits inside the budget.
            // `drain_next_in` divides by TARGET_UTILIZATION (0.6) internally,
            // so pre-scale to make the EFFECTIVE spacing budget/total.
            let interval_ms = MAX_DRAIN_MS.saturating_mul(6) / 10 / total.max(1);
            while !self.cg_handles.is_empty() {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    tracing::warn!("drain budget exhausted; rehoming remaining groups at once");
                    break;
                }
                tokio::select! {
                    () = coordinator.force_drain_timeout() => {}
                    () = tokio::time::sleep(remaining) => break,
                }
                // Pick an arbitrary live CG and rehome it (TS picks the first
                // view-syncer in its service map).
                let Some(id) = self.cg_handles.iter().next().map(|e| e.key().clone()) else {
                    break;
                };
                if let Some((_, mut handle)) = self.cg_handles.remove(&id) {
                    tracing::debug!("draining client group {id}");
                    handle.shutdown();
                    lock_unpoisoned(&self.group_auth_states).remove(&id);
                }
                coordinator.drain_next_in(interval_ms);
            }
        }

        // Final sweep: rehome anything left and join the executor threads.
        self.shutdown().await;
        tracing::info!("finished draining ({} ms)", start.elapsed().as_millis());
    }

    /// Number of active CG threads.
    pub fn cg_count(&self) -> usize {
        self.cg_handles.len()
    }

    /// Send a change-streamer notification to the CG thread for the given client group.
    /// Returns false if no CG thread exists for the given ID.
    pub fn send_notification(&self, cg_id: &str, notification: serde_json::Value) -> bool {
        if let Some(handle) = self.cg_handles.get(cg_id) {
            handle.send(CGMessage::Notification(notification)).is_ok()
        } else {
            false
        }
    }

    /// Broadcast a change-streamer notification to EVERY CG thread. A replica
    /// commit advances the whole replica to a new head, so all client groups
    /// hosted by this syncer must advance + poke — mirroring TS, where a single
    /// `version-ready` from the replicator's `Subscription<ReplicaState>` drives
    /// every pipeline. Returns the number of CG threads notified.
    pub fn broadcast_notification(&self, notification: serde_json::Value) -> usize {
        // Remember the newest state so a CG created between commits can arm its
        // serving-lag tracker at spawn (TS notifier latest-state replay).
        *lock_unpoisoned(&self.last_notification) = Some(notification.clone());
        // Feed the process-wide replica-ready log (TS `#recordReplicaReadyState`):
        // once per commit, watermark + upstream commit time. This is the single
        // process-wide replica-ready feed in the per-CG Rust arch.
        if let Some(watermark) = notification.get("watermark").and_then(|v| v.as_str()) {
            let ready_ms = notification
                .get("upstreamCommitTimeMs")
                .and_then(|v| v.as_f64())
                .map(|f| f as i64)
                .unwrap_or_else(now_ms);
            self.serving_lag_registry
                .record_replica_ready_state(watermark, ready_ms);
        }
        let mut sent = 0;
        for entry in self.cg_handles.iter() {
            if entry
                .value()
                .send(CGMessage::Notification(notification.clone()))
                .is_ok()
            {
                sent += 1;
            }
        }
        sent
    }
}

/// Forwarder: bridges a connection's tokio inbound channel into the CG's
/// unified `tokio::sync::mpsc` channel, so the CG thread never blocks on a
/// single connection. Runs as a tokio task. Emits `ConnectionClosed` when the
/// WS ends.
async fn forward_inbound(
    mut upstream_rx: mpsc::Receiver<String>,
    cg_tx: mpsc::UnboundedSender<CGMessage>,
    client_id: Arc<str>,
    ws_id: Arc<str>,
) {
    while let Some(text) = upstream_rx.recv().await {
        if cg_tx
            .send(CGMessage::Inbound {
                client_id: client_id.clone(),
                ws_id: ws_id.clone(),
                text,
            })
            .is_err()
        {
            return; // CG thread gone.
        }
    }
    let _ = cg_tx.send(CGMessage::ConnectionClosed { client_id, ws_id });
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Threshold (ms) above which a hydration is logged as a slow query — the prod
/// signal operators use to find pathological queries. Port of TS's
/// `slowHydrateThreshold` (view-syncer.ts / pipeline-driver.ts). Read once from
/// `ZERO_SLOW_HYDRATE_THRESHOLD_MS` (default 1000), cached.
fn slow_hydrate_threshold_ms() -> f64 {
    static T: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *T.get_or_init(|| {
        std::env::var("ZERO_SLOW_HYDRATE_THRESHOLD_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000.0)
    })
}

/// Balance an admitted connection without allowing duplicate close/error paths
/// to wrap the unsigned counter to `u64::MAX`.
fn decrement_nonzero(count: &AtomicU64) {
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
    })
}

/// Per-CG state, owned by (and confined to) the CG thread. Holds the `!Send`
/// [`SyncEngine`] plus the live connections. Extracted from the event loop so
/// the message handlers are unit-testable.
struct CgState {
    cg_id: String,
    sync_engine: SyncEngine,
    view_syncer: Arc<dyn ViewSyncerDispatch>,
    conn_context_manager: Arc<dyn ConnContextManagerDispatch>,
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
    replica_path: Option<String>,
    app_id: String,
    /// Compiled read-permissions for this CG's app (loaded from the replica).
    permissions: Option<serde_json::Value>,
    /// The deployed permissions `hash` last loaded, for hot-reload detection
    /// (TS `reloadPermissionsIfChanged`).
    permissions_hash: Option<String>,
    /// Interval (ms) between periodic auth maintenance ticks (JWT re-validation
    /// + query re-transform). `None` disables it. Port of TS
    ///   `ConnectionContextManager`'s `revalidateIntervalMs`.
    revalidate_interval_ms: Option<i64>,
    /// Wall-clock (ms) deadline for the next auth-maintenance tick, or `None`
    /// when nothing is armed (no authed connection, or feature disabled). Port
    /// of the group's earliest `revalidateAt` deadline.
    next_auth_maintenance_at: Option<i64>,
    /// The userID this client group is pinned to (the `sub` of the first authed
    /// connection; `None` for an anonymous group). Admission
    /// (`check_and_pin_user`) guarantees every connection reaching this CG shares
    /// it. Enforced on `updateAuth` and periodic revalidation so a validly-signed
    /// token for a DIFFERENT user cannot re-scope the group mid-connection. Port
    /// of `GroupAuthState.pinnedUser` + `pickToken`'s single-user pin.
    pinned_user_id: Option<String>,
    /// The in-memory CVR, lazily loaded from the store on first notification.
    cvr: Option<CVR>,
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
    /// Wall-clock time of the most recent newly established connection. This is
    /// the ownership lease boundary passed to every CVR load/flush.
    last_connect_time: i64,
    /// Earliest time an empty CG may shut down. TS view-syncers stop after five
    /// seconds without clients so their SQLite readers, PG pools, and OS thread
    /// do not accumulate under cold-client churn.
    keepalive_until: i64,
    /// client_id → Connection.
    connections: HashMap<String, Connection>,
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
    last_row_count: usize,
    /// Process-wide serving-lag registry this CG publishes its snapshot into.
    serving_lag_registry: Arc<crate::workers::syncer::ServingLagRegistry>,
    /// Live-instance census guard (leak hunt): inc on construct, dec on drop.
    /// THE most important census — a CgState owns the `SyncEngine` (IVM graph +
    /// CVR store), so a residual count after all clients disconnect pins
    /// everything below. See the `Drop` impl for the teardown backtrace hook.
    _census: crate::live_count::Guard,
}

impl Drop for CgState {
    fn drop(&mut self) {
        // Drop this CG's serving-lag snapshot on every teardown path (normal
        // return, TTL/idle shutdown, panic-unwind) — TS drops it when the
        // view-syncer service stops.
        self.serving_lag_registry.remove_view_syncer(&self.cg_id);
        // Attribute *who* tore down this client group when
        // `RUST_SYNCER_DROP_BACKTRACE=1`. The census counter dec's via the
        // `_census` guard's own `Drop`.
        crate::live_count::drop_backtrace("CgState");
    }
}

impl CgState {
    #[cfg(test)]
    fn new(
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
    /// runtime — doc 91 Iteration C). When the factory config requests a CVR
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
        let view_syncer = services_factory.create_view_syncer(cg_id);
        let conn_context_manager = services_factory.create_conn_context_manager(cg_id);
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
        let replica_path = config.replica_path.clone();
        let app_id = config.app_id.clone();
        let mut sync_engine = SyncEngine::new(IvmPipelines::new());
        sync_engine.set_tokio_handle(config.tokio_handle.clone());
        sync_engine.set_enable_query_covering(config.enable_query_covering);
        let mut initialization_failed = config.initialization_error.is_some();
        if let Some(error) = &config.initialization_error {
            tracing::error!("CG {cg_id}: initialization failed: {error}");
        }
        if let Err(e) = sync_engine.pipelines().init(
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

        let mut cvr_pg = false;
        if let Some(pg) = config.cvr_pg {
            match cvr_pool {
                Some(pool) => {
                    match sync_engine.set_cvr_store(pool, pg.schema, pg.cvr_id, pg.task_id) {
                        Ok(()) => cvr_pg = true,
                        Err(e) => {
                            tracing::error!("CG {cg_id}: set_cvr_store failed: {e}");
                            initialization_failed = true;
                        }
                    }
                }
                None => {
                    // The factory asked for a CVR store but the hosting executor
                    // has no pool. This is a wiring bug (PG configured but the
                    // router was built without a pool config), not a per-connection
                    // condition — refuse to serve rather than silently run storeless.
                    tracing::error!("CG {cg_id}: CVR store requested but executor has no CVR pool");
                    initialization_failed = true;
                }
            }
        }

        let created_at = now_ms();
        CgState {
            cg_id: cg_id.to_string(),
            sync_engine,
            view_syncer,
            conn_context_manager,
            ccm,
            mutagen,
            pusher,
            shard: config.shard,
            replica_version,
            cvr_pg,
            tables,
            replica_path,
            app_id,
            permissions,
            permissions_hash,
            revalidate_interval_ms,
            next_auth_maintenance_at: None,
            pinned_user_id: None,
            cvr: None,
            e2e_serving_lag:
                crate::services::view_syncer::e2e_serving_lag::E2EServingLagTracker::new(),
            ttl_clock: 0,
            ttl_clock_base: created_at,
            ttl_clock_interval: None,
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
            auth_validator,
            global_connections,
            connection_count,
            accepting,
            terminal: initialization_failed,
            created_at_ms: created_at,
            served_version: None,
            last_row_count: 0,
            // Replaced by the process-wide registry in `cg_event_loop`; a
            // standalone default keeps the test constructor self-contained.
            serving_lag_registry: Arc::new(crate::workers::syncer::ServingLagRegistry::new()),
            _census: crate::live_count::Guard::new(&crate::live_count::CLIENT_GROUP),
        }
    }

    /// TS `ViewSyncerService.servingLagEligible`: `#clients.size > 0 &&
    /// getBackgroundConnectionContext() !== undefined`. Approximated here as
    /// "has a registered client with a live connection to serve".
    fn serving_lag_eligible(&self) -> bool {
        !self.registered_ws.is_empty() && !self.connections.is_empty()
    }

    /// TS `ViewSyncerService.queryCount`: `#pipelines.initialized() ?
    /// #pipelines.queries().size : 0`. The count of active (hydrated) queries.
    fn query_count(&mut self) -> usize {
        self.sync_engine.pipelines().active_query_ids().len()
    }

    /// TS `ViewSyncerService.rowCount`: `#cvrStore.rowCount`. The tracked-row
    /// count as last observed while the CG held the row map (no CVR I/O here).
    fn row_count(&self) -> usize {
        self.last_row_count
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
        self.sync_engine.update_ttl_clock(ttl_clock, start);
    }

    /// Ensure the group CVR is loaded (from the store) or, when `allow_create`,
    /// freshly created. Seeds the TTL clock from the CVR's stored value on the
    /// load/create transition (TS `#ttlClock = cvr.ttlClock; #ttlClockBase =
    /// now`). Returns whether a CVR is now available.
    async fn ensure_cvr(
        &mut self,
        allow_create: bool,
    ) -> Result<bool, crate::sync_engine::LoadCvrError> {
        if self.cvr.is_some() {
            return Ok(true);
        }
        if self.cvr_pg {
            match self
                .sync_engine
                .load_cvr(self.last_connect_time as f64)
                .await
            {
                Ok(cvr) => self.cvr = cvr,
                Err(e) => {
                    tracing::error!("CG {}: load_cvr failed: {e}", self.cg_id);
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
                    tracing::error!("CG {}: {message}", self.cg_id);
                    self.cvr = None;
                    self.fail_group_with_error(crate::protocol::ErrorBody::client_not_found(
                        message,
                    ));
                    return Ok(false);
                }
                self.ttl_clock = cvr.ttl_clock;
                self.ttl_clock_base = now_ms();
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// The delay until the next TTL eviction should run, or `None` if no
    /// inactive queries are pending. Port of TS `#scheduleExpireEviction`'s
    /// delay computation: `clamp(next - ttlClock + hysteresis, hysteresis, MAX)`.
    fn next_expiry_delay(&self) -> Option<Duration> {
        let cvr = self.cvr.as_ref()?;
        let next = rust_cvr::cvr::next_eviction_time(cvr)?;
        let raw = (next - self.ttl_clock) + TTL_TIMER_HYSTERESIS_MS;
        let delay = raw.clamp(TTL_TIMER_HYSTERESIS_MS, MAX_TTL_MS);
        Some(Duration::from_millis(delay as u64))
    }

    /// Fired when the eviction timer elapses: remove any now-expired queries and
    /// poke their removals. Port of TS `#removeExpiredQueries`.
    async fn on_expiry_tick(&mut self) {
        let Some(cvr) = self.cvr.take() else {
            return;
        };
        let now = now_ms();
        let ttl_clock = self.get_ttl_clock(now);
        let client_ids: Vec<String> = self.registered_ws.values().cloned().collect();
        let existing_rows = self.sync_engine.existing_rows().await;
        self.last_row_count = existing_rows.len();
        match self
            .sync_engine
            .remove_expired_queries(
                cvr,
                &client_ids,
                &existing_rows,
                self.last_connect_time,
                now,
                ttl_clock,
            )
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
                self.fail_group("TTL expiry sync failed");
            }
        }
    }

    /// Arm the group auth-maintenance deadline if the feature is enabled and at
    /// least one connection carries a JWT. Idempotent: an already-armed deadline
    /// is left in place (matches TS, where a new validated connection does not
    /// pull the group's earliest deadline earlier than an existing one at the
    /// same interval). Port of the group's earliest `revalidateAt`.
    fn arm_auth_maintenance(&mut self) {
        let Some(interval) = self.revalidate_interval_ms else {
            return;
        };
        if self.next_auth_maintenance_at.is_some() {
            return;
        }
        // Nothing to maintain if no registered connection carries auth.
        let any_auth = {
            let ccm = lock_unpoisoned(&self.ccm);
            self.registered_ws.iter().any(|(c, w)| {
                ccm.must_get_connection_context(&CcmConnectionSelector {
                    client_id: c.clone(),
                    ws_id: w.clone(),
                })
                .ok()
                .and_then(|ctx| ctx.auth)
                .is_some()
            })
        };
        if !any_auth {
            return;
        }
        self.next_auth_maintenance_at = Some(now_ms() + interval);
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
    async fn on_auth_maintenance_tick(&mut self) {
        // Snapshot the (client_id, raw_token) pairs to re-verify, read from the
        // ConnectionContextManager (TS `mustGetConnectionContext(selector).auth`).
        // Only tokened connections are subject to revalidation.
        let due: Vec<(String, String)> = {
            let ccm = lock_unpoisoned(&self.ccm);
            self.registered_ws
                .iter()
                .filter_map(|(c, w)| {
                    ccm.must_get_connection_context(&CcmConnectionSelector {
                        client_id: c.clone(),
                        ws_id: w.clone(),
                    })
                    .ok()
                    .and_then(|ctx| ctx.auth)
                    .map(|a| (c.clone(), a.raw().to_string()))
                })
                .collect()
        };

        let mut survivors: Vec<String> = Vec::new();
        for (client_id, token) in due {
            // The connection may have closed since we snapshotted.
            if !self.registered_ws.contains_key(&client_id) {
                continue;
            }
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
                    if let Some(conn) = self.connections.get(&client_id) {
                        conn.close_with_error(error_body);
                    }
                    if let Some(ws_id) = self.registered_ws.get(&client_id).cloned() {
                        self.on_connection_closed(&client_id, &ws_id);
                    }
                }
                Ok(()) => survivors.push(client_id),
            }
        }

        crate::metrics::Metrics::inc(&self.metrics.auth_revalidations);

        // Retransform each surviving connection's queries against current auth +
        // permissions (re-fetching custom queries with the current token).
        let empty_body = serde_json::json!({});
        let shard = self.shard.clone();
        for client_id in survivors {
            if !self.registered_ws.contains_key(&client_id) {
                continue;
            }

            // Server-side revocation probe. Port of TS `#validateConnection` →
            // `CustomQueryTransformer.validate`: when a custom query API is
            // configured for this client, POST an empty transform so the API
            // server can reject a token that is cryptographically valid but
            // revoked/deauthorized at the app layer (local `validate_auth` above
            // only catches expiry/signature/user-swap). No query API configured →
            // no ctx → skip (TS client-fallback path — no probe). An AUTH error
            // (401/403) invalidates the connection; a transient failure (API down
            // / 5xx) is DEFERRED — keep the connection and retry on the next
            // scheduled tick (TS `deferMaintenance('revalidate')`), never close on
            // a blip.
            let ctx = self
                .registered_ws
                .get(&client_id)
                .cloned()
                .and_then(|ws_id| self.query_context_for(&client_id, &ws_id));
            if let Some(ctx) = ctx {
                match crate::custom_queries::transform_query::validate_custom_queries(&ctx, &shard)
                    .await
                {
                    Ok(()) => {}
                    Err(body) => {
                        if crate::custom_queries::transform_query::is_auth_error_body(&body) {
                            tracing::warn!(
                                "CG {}: server-side auth revocation for client {client_id}; closing",
                                self.cg_id
                            );
                            crate::metrics::Metrics::inc(&self.metrics.auth_revalidation_failures);
                            if let Some(conn) = self.connections.get(&client_id) {
                                conn.close_with_error(crate::protocol::ErrorBody::unauthorized(
                                    "Connection auth validation failed",
                                ));
                            }
                            if let Some(ws_id) = self.registered_ws.get(&client_id).cloned() {
                                self.on_connection_closed(&client_id, &ws_id);
                            }
                            continue;
                        }
                        tracing::warn!(
                            "CG {}: query-transform validation failed transiently for \
                             client {client_id}; deferring retransform",
                            self.cg_id
                        );
                        continue;
                    }
                }
            }

            self.handle_desired_queries(&client_id, &empty_body, true)
                .await;
        }

        // Re-arm: schedule the next tick if any authed connection remains,
        // otherwise disarm until the next connection arrives.
        self.next_auth_maintenance_at = None;
        self.arm_auth_maintenance();
    }

    async fn on_new_connection(&mut self, params: ConnectParams, sink: DirectWebSocketSink) {
        crate::trace::note(
            "conn-open",
            &format!(
                "cg={} client={} ws={}",
                self.cg_id, params.client_id, params.ws_id
            ),
        );
        self.last_connect_time = now_ms();
        self.keepalive_until = self.last_connect_time + CG_KEEPALIVE_MS;
        let client_id = params.client_id.clone();
        let ws_id = params.ws_id.clone();
        let protocol_version = params.protocol_version;
        let client_group_id = params.client_group_id.clone();

        // Close any prior connection for this clientID before installing the new
        // one. Otherwise the previous ws_id's ClientHandler stays registered in
        // the SyncEngine — it keeps receiving pokes and its socket is never
        // closed, so a stale connection can go on emitting under the same
        // clientID. TS closes the superseded connection when a client reconnects.
        if let Some(prev_ws_id) = self.registered_ws.get(&client_id).cloned()
            && prev_ws_id != ws_id
        {
            tracing::debug!(
                "CG {}: client {client_id} reconnected; closing superseded connection (ws {prev_ws_id} -> {ws_id})",
                self.cg_id
            );
            self.sync_engine.fail_client(
                &prev_ws_id,
                "Connection superseded by a newer connection for the same clientID",
            );
            self.sync_engine.unregister_client(&prev_ws_id);
            self.decrement_active_client(&prev_ws_id);
        }

        // Register the client with the SyncEngine so notifications can poke it.
        let cvr_sink: Arc<dyn rust_cvr::client_handler::WebSocketSink> = Arc::new(sink.clone());
        self.sync_engine.register_client(
            &client_id,
            &ws_id,
            &client_group_id,
            &self.shard,
            params.base_cookie.as_deref(),
            cvr_sink,
        );
        self.open_ws_ids.insert(ws_id.clone());
        // Active-clients gauge +1 (TS `#activeClients.add(1, {protocol.version})`).
        // Remember the version so the matching disconnect decrements the same tag.
        self.active_client_pv
            .insert(ws_id.clone(), params.protocol_version);
        crate::metrics::record_active_client_delta(1, params.protocol_version);
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
                        tracing::warn!(
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
            lock_unpoisoned(&self.ccm).register_connection(&selector, &reg, auth);
        }

        // Arm periodic auth maintenance for this (now validated) connection.
        // Port of `validateConnection` setting `revalidateAt = now + interval`.
        self.arm_auth_maintenance();

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
            // Shared cell so `updateAuth` refreshes the token the relayed push
            // forwards (TS reads it fresh per push; a snapshot goes stale → 401).
            auth: std::sync::Arc::new(std::sync::Mutex::new(
                params.auth.clone().filter(|v| !v.is_empty()),
            )),
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

        let handler = Box::new(SyncerWsMessageHandler::new(
            self.view_syncer.clone(),
            self.conn_context_manager.clone(),
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
        self.connections.insert(client_id.clone(), conn);

        // TS parity: a malformed baseCookie FAILS the connection. TS parses it
        // in the ClientHandler constructor (client-handler.ts `cookieToVersion`
        // → `versionFromString`, schema/types.ts) — the throw escapes to the
        // connection boundary and is wrapped as a fatal `Internal` error
        // (`wrapWithProtocolError`, types/error-with-level.ts), sent after
        // `connected`. The lenient registrations above keep the CG task
        // panic-safe (Rust-only concern); this check reproduces the TS-visible
        // outcome: ["error",{kind:"Internal"}] then close.
        if let Some(c) = params.base_cookie.as_deref()
            && let Err(e) = rust_cvr::schema::types::maybe_version_string(c)
        {
            if let Some(conn) = self.connections.get(&*client_id) {
                conn.close_with_error(crate::protocol::ErrorBody::internal(e.to_string()));
            }
            self.on_connection_closed(&client_id, &ws_id);
            return;
        }

        // Handle piggybacked initConnection from the sec-websocket-protocol
        // header, routing its desired queries to the CG-owned SyncEngine.
        if let Some(init_msg) = params.init_connection_msg.clone() {
            let v = serde_json::to_value(&init_msg).unwrap_or(serde_json::Value::Null);
            let body = match v {
                serde_json::Value::Array(mut arr) if arr.len() > 1 => arr.remove(1),
                other => other,
            };
            self.handle_desired_queries(&client_id, &body, true).await;
        }
    }

    async fn on_inbound(&mut self, client_id: Arc<str>, ws_id: Arc<str>, text: String) {
        // A superseded socket can have frames already queued when its replacement
        // is installed. Never route those frames through the new connection.
        if self.registered_ws.get(&*client_id).map(String::as_str) != Some(&*ws_id) {
            tracing::debug!(
                "CG {}: ignoring stale inbound frame for {client_id}/{ws_id}",
                self.cg_id
            );
            return;
        }
        // Parse the frame's JSON exactly once; validation and the tag dispatch
        // below share the parsed array.
        let parsed: Result<Vec<serde_json::Value>, _> = serde_json::from_str(&text);
        // Do not let the direct-engine intercept bypass protocol validation.
        // Malformed messages must take the normal Connection fatal-error path,
        // rather than being partially parsed and silently dropped below.
        let valid = parsed
            .as_deref()
            .is_ok_and(|arr| crate::protocol::parse_upstream_array(arr).is_ok());
        if !valid {
            let closed = match self.connections.get(&*client_id) {
                Some(conn) => !conn.handle_inbound(&text),
                None => return,
            };
            if closed {
                self.on_connection_closed(&client_id, &ws_id);
            }
            return;
        }
        // Intercept desired-query messages and route them to the CG-owned
        // SyncEngine — the placeholder `ViewSyncerDispatch` can't reach the
        // `!Send` engine. Everything else (ping, etc.) goes through Connection.
        let arr = parsed.expect("checked valid above");
        if let Some(tag) = arr.first().and_then(|v| v.as_str()) {
            if tag == "initConnection" || tag == "changeDesiredQueries" {
                if let Some(body) = arr.get(1) {
                    self.handle_desired_queries(&client_id, body, tag == "initConnection")
                        .await;
                }
                return;
            }
            if tag == "deleteClients" {
                // `["deleteClients", {clientIDs, clientGroupIDs}]` — an
                // explicit client-requested deletion (acked).
                if let Some(body) = arr.get(1) {
                    let del_ids = str_array(body.get("clientIDs"));
                    let group_ids = str_array(body.get("clientGroupIDs"));
                    self.apply_client_deletions(&client_id, None, &del_ids, &group_ids)
                        .await;
                    // TS parity (`syncer-ws-message-handler.ts` 'deleteClients'):
                    // an explicit deletion also prunes the deleted clients'
                    // stored mutation results via a `_zero_cleanupResults`
                    // relay push. Only the explicit path does this — the
                    // initConnection activeClients GC does not, same as TS.
                    let cleanup_ids: Vec<String> = del_ids
                        .iter()
                        .filter(|c| c.as_str() != &*client_id)
                        .cloned()
                        .collect();
                    if !cleanup_ids.is_empty()
                        && let Some(pusher) = &self.pusher
                        && let Some(headers) = self.client_push_headers.get(&*client_id)
                    {
                        let selector =
                            crate::workers::syncer_ws_message_handler::ConnectionSelector {
                                client_id: client_id.to_string(),
                                ws_id: self
                                    .registered_ws
                                    .get(&*client_id)
                                    .cloned()
                                    .unwrap_or_default(),
                            };
                        pusher.delete_client_mutations(
                            &selector,
                            &cleanup_ids,
                            headers,
                            &self.cg_id,
                        );
                    }
                }
                return;
            }
            if tag == "updateAuth" {
                // `["updateAuth", {auth}]` — a fresh credential for this
                // client group. Re-verify it and, if the resolved auth data
                // changed, re-transform every query (TS `ViewSyncer.updateAuth`
                // → `#handleConfigUpdate(..., 'all')`).
                let token = arr
                    .get(1)
                    .and_then(|b| b.get("auth"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                self.handle_update_auth(&client_id, token).await;
                return;
            }
            if tag == "inspect" {
                // `["inspect", {op, id, ...}]` — inspector protocol. Reaches
                // the CG engine (the placeholder ViewSyncer can't), which
                // gates on the admin password and answers per op.
                if let Some(body) = arr.get(1) {
                    self.handle_inspect(&client_id, body).await;
                }
                return;
            }
        }
        let closed = match self.connections.get(&*client_id) {
            Some(conn) => !conn.handle_inbound(&text),
            None => return,
        };
        if closed {
            self.on_connection_closed(&client_id, &ws_id);
        }
    }

    /// Route a client's `initConnection` / `changeDesiredQueries` body to the
    /// SyncEngine: record desired queries and hydrate. Loads/creates the group
    /// CVR on first use. (Part 2 — functional cut; see `config_and_hydrate`.)
    async fn handle_desired_queries(
        &mut self,
        client_id: &str,
        body: &serde_json::Value,
        is_init: bool,
    ) {
        let Some(ws_id) = self.registered_ws.get(client_id).cloned() else {
            tracing::warn!(
                "CG {}: desired queries for unregistered client {client_id}",
                self.cg_id
            );
            return;
        };
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
        if let Some(schema) = client_schema.as_ref()
            && let Err(message) =
                crate::db::lite_tables::validate_client_schema(schema, &self.tables)
        {
            tracing::info!(
                "CG {}: rejecting incompatible client schema: {message}",
                self.cg_id
            );
            if let Some(conn) = self.connections.get(client_id) {
                conn.close_with_error(crate::protocol::ErrorBody::basic(
                    crate::protocol::ErrorKind::SchemaVersionNotSupported,
                    message,
                ));
            }
            self.on_connection_closed(client_id, &ws_id);
            return;
        }
        // The custom-query API context (`userQueryURL` + allowlisted headers) is
        // recorded on the ConnectionContextManager's `initConnection` side effect
        // below; `custom_query_context_from` reads it back at transform time. The
        // context persists for the connection's lifetime (a later
        // `changeDesiredQueries` doesn't re-send the URL).

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
        if !is_init && !has_query_change && !has_deletions {
            return;
        }

        // On an `initConnection`, run the ConnectionContextManager init side
        // effect FIRST (TS `SyncerWsMessageHandler` calls
        // `connContextManager.initConnection(...)` before dispatching to the
        // ViewSyncer). This records the connection's auth/context for the group.
        // The router intercepts `initConnection` before it reaches the message
        // handler, so this side effect must fire here or it is dropped.
        let selector = ConnectionSelector {
            client_id: client_id.to_string(),
            ws_id: ws_id.clone(),
        };
        if is_init {
            self.conn_context_manager.init_connection(&selector, body);
            // Apply the initConnection URL/header overrides to the
            // ConnectionContextManager (TS `handleInitConnection`).
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
                client_id: client_id.to_string(),
                ws_id: ws_id.clone(),
            };
            let _ = lock_unpoisoned(&self.ccm).init_connection(&ccm_selector, &init_body);
        }

        // Ensure a group CVR: load from the store, or start fresh (dev/no-PG).
        match self.ensure_cvr(true).await {
            Ok(true) => {}
            Ok(false) => {
                self.fail_group("Unable to load the client view state");
                return;
            }
            Err(crate::sync_engine::LoadCvrError::Store(
                rust_cvr::cvr_store::CVRStoreError::ClientNotFound(message),
            )) => {
                if let Some(conn) = self.connections.get(client_id) {
                    conn.close_with_error(crate::protocol::ErrorBody::client_not_found(message));
                }
                self.on_connection_closed(client_id, &ws_id);
                return;
            }
            Err(error) => {
                tracing::error!("CG {}: unable to load CVR: {error}", self.cg_id);
                self.fail_group("Unable to load the client view state");
                return;
            }
        }

        let cvr = self.cvr.as_ref().unwrap();
        let client_version = self
            .client_base_versions
            .get(client_id)
            .cloned()
            .unwrap_or(None);
        if is_init && let Err(error) = check_client_and_cvr_versions(&client_version, &cvr.version)
        {
            if let Some(conn) = self.connections.get(client_id) {
                conn.close_with_error(*error);
            }
            self.on_connection_closed(client_id, &ws_id);
            return;
        }
        if is_init && cvr.client_schema.is_none() && client_schema.is_none() {
            if let Some(conn) = self.connections.get(client_id) {
                conn.close_with_error(crate::protocol::ErrorBody::basic(
                    crate::protocol::ErrorKind::InvalidConnectionRequest,
                    "The initConnection message for a new client group must include client schema."
                        .to_string(),
                ));
            }
            self.on_connection_closed(client_id, &ws_id);
            return;
        }

        // Query-config pass (records the client + desired queries, hydrates,
        // then catches the client up). Always runs on initConnection.
        let mut config_accepted = false;
        if is_init || has_query_change {
            let cvr = self.cvr.take().unwrap();
            let state_version = self
                .sync_engine
                .pipelines()
                .current_version()
                .unwrap_or_else(|| cvr.version.state_version.clone());
            let replica_version = self.replica_version.clone();
            // The rows the client already has (from the CVR row cache).
            let existing_rows = self.sync_engine.existing_rows().await;
            self.last_row_count = existing_rows.len();
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
            crate::trace::note(
                "hydrate-start",
                &format!("cg={} client={client_id}", self.cg_id),
            );
            // Poke EVERY registered connection, not just the requester — TS
            // `#syncQueryPipelineSet` pokes `#getClients()` unfiltered. Scoping
            // to `&[ws_id]` left the group's other tabs on the old cookie, and
            // `advance_poke_targets` (which only pokes at-version clients) then
            // excluded them from every future advance: a live-but-frozen
            // connection. The per-client base filters inside the pokers deliver
            // each connection exactly what it hasn't seen.
            let all_ws_ids: Vec<String> = self.registered_ws.values().cloned().collect();
            let query_ctx = self.query_context_for(client_id, &ws_id);
            match self
                .sync_engine
                .config_and_hydrate_with_profile(
                    cvr,
                    client_id,
                    &all_ws_ids,
                    &self.shard,
                    puts,
                    dels,
                    clear,
                    client_schema,
                    self.client_profile_ids.get(client_id).map(String::as_str),
                    self.permissions.as_ref(),
                    &auth_data,
                    query_ctx.as_ref(),
                    state_version,
                    replica_version,
                    &existing_rows,
                    self.last_connect_time,
                    now,
                    ttl_clock,
                )
                .await
            {
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
                    let elapsed_ms = hydrate_started.elapsed().as_secs_f64() * 1000.0;
                    crate::trace::note(
                        "hydrate-end",
                        &format!(
                            "cg={} client={client_id} elapsed_ms={elapsed_ms:.1}",
                            self.cg_id
                        ),
                    );
                    self.metrics.record_hydration(elapsed_ms);
                    if elapsed_ms > slow_hydrate_threshold_ms() {
                        tracing::warn!(
                            "CG {}: Slow query materialization: config_and_hydrate took \
                             {elapsed_ms:.0}ms for client {client_id}",
                            self.cg_id
                        );
                    }
                }
                Err(e) => {
                    tracing::error!("CG {}: config_and_hydrate failed: {e}", self.cg_id);
                    self.fail_group("Client view synchronization failed");
                }
            }
        }

        // On an accepted `initConnection`, run the Pusher init side effect (TS
        // calls `pusher.initConnection(...)` only after the ViewSyncer stream
        // started). Also intercepted-away from the message handler, so it fires
        // here. No-op when no Pusher is configured (mutations forwarded in TS).
        if is_init
            && config_accepted
            && let Some(pusher) = &self.pusher
        {
            pusher.init_connection(&selector);
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

        // Single-user pin (port of `pickToken`): a client group is pinned to one
        // userID. If this group already has a pinned user, the new token's `sub`
        // MUST match it — otherwise a validly-signed token for a DIFFERENT user
        // (the signing key is shared across users) could re-scope the entire
        // group's `authData` mid-connection. Reject the mismatch as Unauthorized
        // and close the connection.
        let pin_mismatch = self
            .pinned_user_id
            .as_deref()
            .is_some_and(|pinned| new_sub.as_deref() != Some(pinned));
        if pin_mismatch {
            tracing::warn!(
                "CG {}: updateAuth userID mismatch (pinned={:?}, new={new_sub:?}); closing",
                self.cg_id,
                self.pinned_user_id
            );
            crate::metrics::Metrics::inc(&self.metrics.auth_revalidation_failures);
            if let Some(conn) = self.connections.get(client_id) {
                conn.close_with_error(crate::protocol::ErrorBody::unauthorized(
                    "The user id in the new token does not match the previous token. \
                     Client groups are pinned to a single user.",
                ));
            }
            if let Some(ws_id) = self.registered_ws.get(client_id).cloned() {
                self.on_connection_closed(client_id, &ws_id);
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
                conn.close_with_error(error_body);
            }
            if let Some(ws_id) = self.registered_ws.get(client_id).cloned() {
                self.on_connection_closed(client_id, &ws_id);
            }
            return;
        }

        // No change in the RAW token → skip re-validation + re-transformation.
        // TS: `authChanged = !authEquals(prev, next)`, and `authEquals` compares
        // the raw token string for BOTH opaque and JWT auth (connection-context-
        // manager.ts:349 → auth.ts `authEquals`). Comparing decoded JWT claims
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
        // Refresh the token forwarded on relayed pushes. The message handler
        // shares this `Arc` (a plain snapshot would keep relaying the expired
        // connect-time token → API-server 401). TS parity: pusher.ts reads
        // `mustGetConnectionContext` fresh on every push.
        if let Some(headers) = self.client_push_headers.get(client_id)
            && let Ok(mut cell) = headers.auth.lock()
        {
            *cell = Some(token.to_string());
        }
        // Refresh the auth on the ConnectionContextManager (TS `updateAuth`).
        if let Some(ws_id) = self.registered_ws.get(client_id).cloned() {
            let selector = CcmConnectionSelector {
                client_id: client_id.to_string(),
                ws_id,
            };
            let _ = lock_unpoisoned(&self.ccm).update_auth(
                &selector,
                &UpdateAuthBody {
                    auth: Some(token.to_string()),
                },
            );
        }
        let empty_body = serde_json::json!({});
        self.handle_desired_queries(client_id, &empty_body, true)
            .await;
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
        let op = body.get("op").and_then(|v| v.as_str()).unwrap_or("");
        let id = body.get("id").cloned().unwrap_or(serde_json::Value::Null);

        // Auth gate — only `authenticate` is allowed before authenticating.
        if op != "authenticate" && !self.inspector_authenticated {
            self.sync_engine.send_inspect_response(
                &ws_id,
                serde_json::json!({"op": "authenticated", "id": id, "value": false}),
            );
            return;
        }

        // Each arm yields `Ok((responseOp, value))` or `Err(message)`; the
        // response frame (success vs `op:"error"`) is assembled once below so
        // every path — present and future — flows through the error shape.
        let result: Result<(&str, serde_json::Value), String> = match op {
            "authenticate" => {
                let password = body.get("value").and_then(|v| v.as_str()).unwrap_or("");
                // Valid only if an admin password is configured AND matches.
                let ok = self
                    .admin_password
                    .as_deref()
                    .is_some_and(|p| !p.is_empty() && p == password);
                self.inspector_authenticated = ok;
                Ok(("authenticated", serde_json::json!(ok)))
            }
            "version" => Ok(("version", serde_json::json!(self.server_version))),
            "queries" => {
                let filter_client = body
                    .get("clientID")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let now = now_ms();
                let ttl_clock = self.get_ttl_clock(now);
                let value = self
                    .inspect_queries_value(filter_client.as_deref(), ttl_clock)
                    .await;
                Ok(("queries", value))
            }
            "metrics" => {
                // The wire value is a RECORD with two REQUIRED TDigest fields
                // (`serverMetricsSchema`, inspect-down.ts:7-10) — an array or
                // `{}` fails the client's valita parse and rejects the RPC.
                // Rust tracks no server TDigests yet, so send empty digests:
                // `[1000]` is `new TDigest().toJSON()` (default compression,
                // no centroids), which zero-client parses as a valid digest.
                Ok((
                    "metrics",
                    serde_json::json!({
                        "query-materialization-server": [1000],
                        "query-update-server": [1000],
                    }),
                ))
            }
            // `analyzeQuery` (query plan / vended-rows analysis) is not ported.
            // Route through the error op: an `analyze-query` success frame with
            // an `{error}` payload would fail `analyzeQueryResultSchema` on the
            // client and hang the RPC.
            "analyze-query" => {
                Err("analyze-query is not supported by the rust syncer yet".to_string())
            }
            other => {
                tracing::warn!("CG {}: unknown inspect op {other:?}", self.cg_id);
                Err(format!("unknown inspect op: {other}"))
            }
        };
        let frame = match result {
            Ok((resp_op, value)) => {
                serde_json::json!({"op": resp_op, "id": id, "value": value})
            }
            Err(message) => serde_json::json!({"op": "error", "id": id, "value": message}),
        };
        self.sync_engine.send_inspect_response(&ws_id, frame);
    }

    /// Build the `queries` inspector value by delegating to the CVR store's
    /// `inspect_queries` (SQL port of TS `CVRStore.inspectQueries`), then adding
    /// `metrics: null` to each row. The InspectorDelegate materialization metrics
    /// and the custom-query transformed-AST fallback are server-side machinery not
    /// ported to the Rust syncer (the TS inspect-handler.ts enrichment layer).
    async fn inspect_queries_value(
        &self,
        filter_client: Option<&str>,
        ttl_clock: TTLClock,
    ) -> serde_json::Value {
        let rows = match self
            .sync_engine
            .inspect_queries(ttl_clock, filter_client)
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!("CG {}: inspect_queries failed: {e}", self.cg_id);
                return serde_json::json!([]);
            }
        };
        let out: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                let mut v = serde_json::to_value(row).unwrap_or(serde_json::Value::Null);
                if let serde_json::Value::Object(map) = &mut v {
                    map.insert("metrics".to_string(), serde_json::Value::Null);
                }
                v
            })
            .collect();
        serde_json::Value::Array(out)
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
        if !matches!(self.ensure_cvr(true).await, Ok(true)) {
            self.fail_group("Unable to load the client view state");
            return;
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

        let cvr = self.cvr.take().unwrap();
        let poke_ws: Vec<String> = self.registered_ws.values().cloned().collect();
        let now = now_ms();
        let ttl_clock = self.get_ttl_clock(now);
        match self
            .sync_engine
            .delete_clients(
                cvr,
                &self.shard,
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
                self.fail_group("Client view synchronization failed");
            }
        }
        // Mutation-result cleanup for EXPLICIT deleteClients messages is
        // relayed by the caller (see the `deleteClients` arm in
        // `handle_message`) — the implicit activeClients GC path deliberately
        // does not clean up, matching TS.
    }

    fn on_connection_closed(&mut self, client_id: &str, ws_id: &str) {
        crate::trace::note(
            "conn-close",
            &format!("cg={} client={client_id} ws={ws_id}", self.cg_id),
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
        self.sync_engine.unregister_client(ws_id);
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
        // the group idles out — port of TS `#removeClient`'s clients-empty
        // branch (view-syncer.ts:761-766; the `#ttlClock !== undefined` guard
        // is the loaded-CVR check inside the callee).
        if self.connections.is_empty() {
            self.update_ttl_clock_in_cvr_without_lock();
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
            conn.close_with_error(crate::protocol::ErrorBody::rehome(
                "Connection superseded by a newer connection",
            ));
        }
        self.on_connection_closed(client_id, ws_id);
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
    fn maybe_reload_permissions(&mut self) -> bool {
        let Some(path) = self.replica_path.as_deref() else {
            return false;
        };
        // READ_ONLY like every other replica reader: the default
        // `Connection::open` is READ_WRITE|CREATE, which would silently create
        // an empty db if the replica is missing/swapped at this instant — then
        // find no permissions and keep serving stale rules instead of surfacing
        // the problem. Fail cleanly into the warn+return-false path instead.
        let conn = match crate::db::lite_tables::open_replica_read_only(path) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    "CG {}: could not open replica to check permissions: {e}",
                    self.cg_id
                );
                return false;
            }
        };
        match crate::auth::read_authorizer::reload_permissions_if_changed(
            &conn,
            &self.app_id,
            self.permissions_hash.as_deref(),
        ) {
            crate::auth::read_authorizer::PermissionsReload::Unchanged => false,
            crate::auth::read_authorizer::PermissionsReload::Changed { permissions, hash } => {
                tracing::info!(
                    "CG {}: read-permissions changed (hash {:?} → {:?}); re-transforming queries",
                    self.cg_id,
                    self.permissions_hash,
                    hash
                );
                self.permissions = permissions;
                self.permissions_hash = hash;
                crate::metrics::Metrics::inc(&self.metrics.permission_reloads);
                true
            }
        }
    }

    /// Change-streamer notification: advance the pipelines to head and poke all
    /// clients. Loads the CVR from the store on first use. A no-store / no-CVR
    /// CG (e.g. tests without PG) logs and skips.
    /// Pair the just-served version with the pending upstream commit for the
    /// end-to-end serving-lag histogram (no-op when nothing is pending or the
    /// served version does not yet cover it). Port of TS `#markVersionServed`.
    fn mark_version_served(&mut self, version: &CVRVersion) {
        crate::trace::note(
            "poke-sent",
            &format!(
                "cg={} version={} clients={}",
                self.cg_id,
                version.state_version,
                self.registered_ws.len()
            ),
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
        if !matches!(self.ensure_cvr(false).await, Ok(true)) {
            self.fail_group("Unable to load the client view state");
            return;
        }
        // Hot-reload permissions before advancing. If the deployed doc changed,
        // every query's read-permission expansion (and thus its transformation
        // hash) may differ, so we re-init the pipeline and re-hydrate the whole
        // CVR under the new rules — the same reset path used for schema drift.
        // This subsumes the normal advance for this cycle (rehydrate pulls to
        // head), matching TS, where a permission change forces the query
        // pipeline set to be re-synced.
        if self.maybe_reload_permissions() {
            let cvr = self.cvr.take().unwrap();
            self.reset_pipelines_and_rehydrate(cvr, "read-permissions changed")
                .await;
            return;
        }
        let cvr = self.cvr.take().unwrap();

        let client_ids: Vec<String> = self.registered_ws.values().cloned().collect();
        let existing_rows = self.sync_engine.existing_rows().await;
        self.last_row_count = existing_rows.len();
        let now = now_ms();
        let ttl_clock = self.get_ttl_clock(now);
        let advance_started = std::time::Instant::now();
        crate::trace::note(
            "advance-start",
            &format!("cg={} clients={}", self.cg_id, client_ids.len()),
        );
        match self
            .sync_engine
            .advance_and_sync(
                cvr,
                self.replica_version.clone(),
                &client_ids,
                &existing_rows,
                self.last_connect_time,
                now,
                ttl_clock,
            )
            .await
        {
            Ok(result) => {
                let advance_ms = advance_started.elapsed().as_secs_f64() * 1000.0;
                crate::trace::note(
                    "advance-end",
                    &format!(
                        "cg={} elapsed_ms={advance_ms:.1} reset={}",
                        self.cg_id,
                        result.reset_reason.is_some()
                    ),
                );
                self.metrics.record_advance(advance_ms);
                if let Some(reason) = result.reset_reason.clone() {
                    // The engine could not advance in place (snapshot/schema
                    // drift). Port of TS `ResetPipelinesSignal` handling: the
                    // in-flight poke was already cancelled; re-init the pipeline
                    // and re-hydrate every query from scratch.
                    self.metrics.record_reset(&reason);
                    self.reset_pipelines_and_rehydrate(result.cvr, &reason)
                        .await;
                } else {
                    self.mark_version_served(&result.cvr.version);
                    self.cvr = Some(result.cvr);
                }
            }
            Err(e) => {
                tracing::error!("CG {}: advance_and_sync failed: {e}", self.cg_id);
                self.fail_group("Client view synchronization failed");
            }
        }
    }

    /// Re-initialize the IVM pipeline from a fresh replica snapshot and
    /// re-hydrate every query currently in the CVR. Port of the reset branch in
    /// TS `#syncQueryPipelines`: `#pipelines.reset()` then re-run the query
    /// pipeline set. Called when `advance_and_sync` reports a reset.
    async fn reset_pipelines_and_rehydrate(&mut self, cvr: CVR, reason: &str) {
        tracing::warn!(
            "CG {}: pipeline reset ({reason}); re-initializing engine + rehydrating",
            self.cg_id
        );
        // Schema-change resets must re-read the replica schema. Reusing the specs
        // captured at CG creation would rebuild the engine with the same stale
        // table/column set and either reset-loop or serve an obsolete schema.
        if let Some(path) = self.replica_path.as_deref() {
            match crate::db::lite_tables::compute_table_specs_from_path(path) {
                Ok(tables) => self.tables = tables,
                Err(e) => {
                    tracing::error!("CG {}: schema reload after reset failed: {e}", self.cg_id);
                    self.fail_group("Replica schema reload failed");
                    return;
                }
            }
        }
        // Re-init the engine against a fresh snapshot; this clears every hydrated
        // query so the rehydrate below re-adds the full set.
        if let Err(e) = self.sync_engine.pipelines().init(
            self.tables.clone(),
            self.replica_path.as_deref(),
            &self.app_id,
        ) {
            tracing::error!(
                "CG {}: pipeline re-init after reset failed: {e}",
                self.cg_id
            );
            self.fail_group("Client view pipeline reset failed");
            return;
        }
        // Re-hydrate by re-running the config pass for every connected client
        // with an empty desired-queries patch. Since the pipeline is now empty,
        // Phase 2 re-adds all of the client's (and the internal) queries. The
        // first client hydrates the shared/internal queries; later clients only
        // add what's still missing (`has_query` guards duplicates).
        let clients: Vec<(String, String)> = self
            .registered_ws
            .iter()
            .map(|(c, w)| (c.clone(), w.clone()))
            .collect();
        let now = now_ms();
        let mut cvr = cvr;
        // Poke every registered connection on each pass (TS `#getClients()`).
        // The first client's pass re-hydrates the full query set and pokes
        // everyone to the new version; later passes then find their queries
        // running and their catch-up interval empty (cheap no-ops).
        let all_ws_ids: Vec<String> = self.registered_ws.values().cloned().collect();
        for (client_id, ws_id) in clients {
            let state_version = self
                .sync_engine
                .pipelines()
                .current_version()
                .unwrap_or_else(|| cvr.version.state_version.clone());
            let replica_version = self.replica_version.clone();
            let existing_rows = self.sync_engine.existing_rows().await;
            self.last_row_count = existing_rows.len();
            // authData read from the ConnectionContextManager at use time (TS
            // `mustGetConnectionContext(selector).auth?.raw`, decoded).
            let auth_data = lock_unpoisoned(&self.ccm)
                .must_get_connection_context(&CcmConnectionSelector {
                    client_id: client_id.clone(),
                    ws_id: ws_id.clone(),
                })
                .ok()
                .and_then(|c| c.auth)
                .map(|a| crate::auth::jwt::decode_jwt_claims(a.raw()))
                .unwrap_or_else(|| serde_json::json!({}));
            let ttl_clock = self.get_ttl_clock(now);
            let query_ctx = self.query_context_for(&client_id, &ws_id);
            // Clone the CVR into the call so a failure doesn't consume it.
            match self
                .sync_engine
                .config_and_hydrate_with_profile(
                    cvr.clone(),
                    &client_id,
                    &all_ws_ids,
                    &self.shard,
                    Vec::new(),
                    Vec::new(),
                    false,
                    None,
                    self.client_profile_ids.get(&client_id).map(String::as_str),
                    self.permissions.as_ref(),
                    &auth_data,
                    query_ctx.as_ref(),
                    state_version,
                    replica_version,
                    &existing_rows,
                    self.last_connect_time,
                    now,
                    ttl_clock,
                )
                .await
            {
                Ok(c) => cvr = c,
                Err(e) => {
                    tracing::error!("CG {}: rehydrate after reset failed: {e}", self.cg_id);
                    self.fail_group("Client view rehydration failed");
                    return;
                }
            }
        }
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

    fn shutdown(&mut self) {
        self.accepting.store(false, Ordering::SeqCst);
        // Draining: tell each client to reconnect (elsewhere) with a Rehome
        // error, mirroring TS `#cleanup`'s `client.fail(Rehome "Reconnect
        // required")`, rather than a silent close. The client library treats
        // Rehome as "reconnect to another instance".
        for (_, conn) in self.connections.drain() {
            conn.close_with_error(crate::protocol::ErrorBody::rehome("Reconnect required"));
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

    /// Permanently fail this CG. Continuing would be unsafe because Rust IVM
    /// advancement is not rollbackable after the snapshot swaps; a failed CVR
    /// commit would otherwise cause the next notification to skip that batch.
    fn fail_group(&mut self, message: &str) {
        self.fail_group_with_error(crate::protocol::ErrorBody::rehome(message));
    }

    /// Like [`fail_group`], but closes every connection with a specific
    /// `ErrorBody` instead of the default `Rehome`. Used for the older-replica
    /// case, where TS fails clients with a `ClientNotFound` (so the client wipes
    /// local state and re-syncs fresh) rather than a reconnect-elsewhere Rehome.
    fn fail_group_with_error(&mut self, error: crate::protocol::ErrorBody) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        crate::metrics::record_fail_group("sync");
        self.accepting.store(false, Ordering::SeqCst);
        for (_, conn) in self.connections.drain() {
            conn.close_with_error(error.clone());
        }
        for (_, ws_id) in self.registered_ws.drain() {
            self.sync_engine.unregister_client(&ws_id);
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

/// Run one executor thread (doc 91): a `current_thread` tokio runtime + `LocalSet`
/// that hosts a hash-shard of client groups as `spawn_local` tasks. The `!Send`
/// `SyncEngine` of each hosted group lives on this one thread and its IVM compute
/// runs inline; CVR/PG I/O is *offloaded* onto the shared-pool runtime (the
/// process's main multi-thread runtime) via `SyncEngine::offload`, so this
/// executor never blocks on Postgres and, crucially, the CVR connection budget is
/// ONE shared pool (not fragmented per executor) — every group can use any of the
/// pool's connections, matching TS's one-pool-per-worker behavior.
fn run_executor(
    idx: usize,
    ctrl_rx: mpsc::UnboundedReceiver<ExecutorCommand>,
    services_factory: Arc<dyn CGServicesFactory>,
    auth_validator: Arc<dyn AuthValidator>,
    connections: Arc<Mutex<HashMap<String, ConnectionInfo>>>,
    cg_handles: Arc<DashMap<String, CGHandle>>,
    pool: Option<sqlx::PgPool>,
) {
    tracing::info!("CG executor {idx} started");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build CG executor current-thread runtime");
    let local = tokio::task::LocalSet::new();
    local.block_on(
        &rt,
        executor_loop(
            idx,
            ctrl_rx,
            services_factory,
            auth_validator,
            connections,
            cg_handles,
            pool,
        ),
    );
    tracing::info!("CG executor {idx} exited");
}

/// The executor's control loop, run inside its `LocalSet`. Spawns a
/// `cg_event_loop` per hosted client group and, on shutdown (explicit command or
/// the router dropping every control sender), drains the in-flight CG tasks
/// before returning so their Rehome failures are delivered.
async fn executor_loop(
    idx: usize,
    mut ctrl_rx: mpsc::UnboundedReceiver<ExecutorCommand>,
    services_factory: Arc<dyn CGServicesFactory>,
    auth_validator: Arc<dyn AuthValidator>,
    connections: Arc<Mutex<HashMap<String, ConnectionInfo>>>,
    cg_handles: Arc<DashMap<String, CGHandle>>,
    pool: Option<sqlx::PgPool>,
) {
    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    loop {
        // Reap finished CG tasks so the tracking vec doesn't grow unbounded over
        // the executor's lifetime (groups come and go via TTL/idle shutdown).
        tasks.retain(|h| !h.is_finished());
        match ctrl_rx.recv().await {
            Some(ExecutorCommand::SpawnCg {
                cg_id,
                rx,
                self_tx,
                connection_count,
                accepting,
                last_notification,
                serving_lag_registry,
            }) => {
                let ctx = CgTaskContext {
                    services_factory: services_factory.clone(),
                    auth_validator: auth_validator.clone(),
                    connections: connections.clone(),
                    cvr_pool: pool.clone(),
                    serving_lag_registry,
                };
                // A panic in one group's engine must not poison its executor.
                // `spawn_local` isolates the panic to this task; the drop guard
                // then removes the map entry on BOTH the normal-return and the
                // panic-unwind path (parity with the old per-CG `catch_unwind` +
                // cleanup), so a panicked group never leaves a stale handle that
                // would fail every reconnect for that id.
                let cleanup = CgMapCleanup {
                    handles: cg_handles.clone(),
                    cg_id: cg_id.clone(),
                    self_tx,
                };
                let task = tokio::task::spawn_local(async move {
                    let _cleanup = cleanup;
                    // Catch panics HERE so they are logged with the cg_id and
                    // counted. The executor's reaper (`retain(!is_finished)`)
                    // and drain (`let _ = task.await`) discard JoinErrors — a
                    // recurring per-CG panic otherwise looks like "clients keep
                    // reconnecting" with no correlating log line.
                    let result = futures_util::FutureExt::catch_unwind(
                        std::panic::AssertUnwindSafe(cg_event_loop(
                            &cg_id,
                            rx,
                            connection_count,
                            accepting,
                            ctx,
                            last_notification,
                        )),
                    )
                    .await;
                    if let Err(panic) = result {
                        let msg = panic
                            .downcast_ref::<&str>()
                            .map(|s| s.to_string())
                            .or_else(|| panic.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "non-string panic payload".to_string());
                        tracing::error!("CG {cg_id} task panicked: {msg}");
                        // A panic bypasses fail_group (which counts terminal
                        // teardowns), so count the group death here.
                        crate::metrics::record_fail_group("panic");
                    }
                });
                tasks.push(task);
            }
            Some(ExecutorCommand::Shutdown) | None => break,
        }
    }
    // Drain: the router has already asked each CG to shut down (Rehome), so the
    // tasks terminate promptly; await them so the failures are flushed before the
    // pool (dropped with `rt`) closes.
    tracing::info!("CG executor {idx}: draining {} task(s)", tasks.len());
    for task in tasks {
        let _ = task.await;
    }
}

/// Drop guard that removes a client group's `cg_handles` entry when its task
/// ends — on normal completion OR panic unwind. Only removes the entry if it is
/// still this task's generation (`same_channel`), since a replacement task for
/// the same id may already have re-registered.
struct CgMapCleanup {
    handles: Arc<DashMap<String, CGHandle>>,
    cg_id: String,
    self_tx: mpsc::UnboundedSender<CGMessage>,
}

impl Drop for CgMapCleanup {
    fn drop(&mut self) {
        self.handles
            .remove_if(&self.cg_id, |_, h| h.tx.same_channel(&self.self_tx));
    }
}

/// Shared, per-executor context handed to each hosted client group's task. Holds
/// the executor's services factory, auth validator, the process-wide connection
/// map, and the executor's own CVR pool.
struct CgTaskContext {
    services_factory: Arc<dyn CGServicesFactory>,
    auth_validator: Arc<dyn AuthValidator>,
    connections: Arc<Mutex<HashMap<String, ConnectionInfo>>>,
    cvr_pool: Option<sqlx::PgPool>,
    serving_lag_registry: Arc<crate::workers::syncer::ServingLagRegistry>,
}

/// The async body hosting one client group, run as a `spawn_local` task on its
/// executor's `current_thread` runtime + `LocalSet`. Owns the (`!Send`)
/// [`SyncEngine`]; drives connection setup, inbound frames, disconnects, and
/// change-streamer notifications. Message handling and the TTL-eviction /
/// auth-maintenance / idle-shutdown deadline ticks are multiplexed with
/// `tokio::select!` over `rx.recv()` and `tokio::time::sleep`.
async fn cg_event_loop(
    cg_id: &str,
    mut rx: mpsc::UnboundedReceiver<CGMessage>,
    connection_count: Arc<AtomicU64>,
    accepting: Arc<AtomicBool>,
    ctx: CgTaskContext,
    last_notification: Option<serde_json::Value>,
) {
    let mut state = CgState::new_with_accepting(
        cg_id,
        &ctx.services_factory,
        ctx.auth_validator,
        ctx.connections.clone(),
        connection_count,
        accepting,
        ctx.cvr_pool,
    );
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
    if state.terminal {
        // Surface initialization failure to the accepted socket instead of
        // dropping the queued connection silently.
        state.accepting.store(false, Ordering::SeqCst);
        if let Some(CGMessage::NewConnection { params, sink }) = rx.recv().await {
            let mut global = lock_unpoisoned(&state.global_connections);
            if global
                .get(&params.client_id)
                .is_some_and(|info| info.ws_id == params.ws_id)
            {
                global.remove(&params.client_id);
            }
            drop(global);
            decrement_nonzero(&state.connection_count);
            sink.fail(crate::protocol::ErrorBody::internal(
                "Failed to initialize the client-group sync engine",
            ));
        }
        state.connection_count.store(0, Ordering::Relaxed);
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
            if !dispatch_cg_message(&mut state, &mut rx, &mut stashed, msg).await {
                tracing::info!("CG thread {cg_id}: shutting down");
                break;
            }
            if state.terminal {
                tracing::error!("CG thread {cg_id}: terminating after fatal synchronization error");
                break;
            }
            // A material CVR flush (re)starts the ttlClock interval — port of
            // TS `#flushUpdater`'s `if (flushed)` (view-syncer.ts:1083-1086).
            if state.sync_engine.take_flush_observed() {
                state.start_ttl_clock_interval();
            }
            continue;
        }
        let next_delay = [
            state.next_expiry_delay(),
            state.next_auth_maintenance_delay(),
            state.next_idle_shutdown_delay(),
            state.next_ttl_clock_delay(),
        ]
        .into_iter()
        .flatten()
        .min();

        let msg = match next_delay {
            Some(delay) => {
                tokio::select! {
                    biased;
                    recv = rx.recv() => match recv {
                        Some(msg) => msg,
                        None => break,
                    },
                    _ = tokio::time::sleep(delay) => {
                        if state.idle_shutdown_due() {
                            tracing::info!(
                                "CG thread {cg_id}: idle keepalive elapsed; shutting down"
                            );
                            state.shutdown();
                            break;
                        }
                        // A wake could be for either deadline; run each if due.
                        if state
                            .next_auth_maintenance_at
                            .is_some_and(|at| at <= now_ms())
                        {
                            state.on_auth_maintenance_tick().await;
                        }
                        // Evict expired queries only when some are pending (running
                        // early on an auth-only wake would be a needless engine call).
                        if state.next_expiry_delay().is_some() {
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
                        if state.sync_engine.take_flush_observed() {
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
        if !dispatch_cg_message(&mut state, &mut rx, &mut stashed, msg).await {
            tracing::info!("CG thread {cg_id}: shutting down");
            break;
        }
        if state.terminal {
            tracing::error!("CG thread {cg_id}: terminating after fatal synchronization error");
            break;
        }
        // A material CVR flush (re)starts the ttlClock interval — port of
        // TS `#flushUpdater`'s `if (flushed)` (view-syncer.ts:1083-1086).
        if state.sync_engine.take_flush_observed() {
            state.start_ttl_clock_interval();
        }
    }
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
async fn dispatch_cg_message(
    state: &mut CgState,
    rx: &mut mpsc::UnboundedReceiver<CGMessage>,
    stashed: &mut std::collections::VecDeque<CGMessage>,
    msg: CGMessage,
) -> bool {
    match msg {
        CGMessage::NewConnection { params, sink } => state.on_new_connection(*params, sink).await,
        CGMessage::Inbound {
            client_id,
            ws_id,
            text,
        } => state.on_inbound(client_id, ws_id, text).await,
        CGMessage::ConnectionClosed { client_id, ws_id } => {
            state.on_connection_closed(&client_id, &ws_id)
        }
        CGMessage::CloseConnection { client_id, ws_id } => {
            state.close_connection(&client_id, &ws_id)
        }
        CGMessage::Notification(n) => {
            let mut merged = n;
            loop {
                match rx.try_recv() {
                    Ok(CGMessage::Notification(next)) => {
                        merged = merge_notifications(merged, next);
                    }
                    Ok(other) => {
                        stashed.push_back(other);
                        break;
                    }
                    Err(_) => break,
                }
            }
            state.on_notification(merged).await
        }
        CGMessage::Shutdown => {
            state.shutdown();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::PROTOCOL_VERSION;
    use crate::workers::syncer_ws_message_handler::{
        ConnContextInfo, ConnContextManagerDispatch, ConnectionSelector, ViewSyncerDispatch,
    };
    use crate::ws_sink::{DirectWebSocketSink, WsCommand};
    use rust_cvr::schema::types::version_from_string;

    /// `send_error_if_current` delivers only to the client's CURRENT socket:
    /// a matching ws_id gets the error frame; a stale ws_id or unknown client
    /// is dropped (the reconnected socket re-pushes on its own).
    #[test]
    fn connection_sinks_deliver_only_to_current_socket() {
        use crate::protocol::{ErrorBody, ErrorKind};
        let sinks = ConnectionSinks::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        sinks.insert_for_test("cA", "ws1", DirectWebSocketSink::new(tx));
        let err = ErrorBody::basic(ErrorKind::PushFailed, "boom".to_string());

        // Stale ws_id → dropped, nothing delivered.
        assert!(!sinks.send_error_if_current("cA", "ws0", &err));
        // Unknown client → dropped.
        assert!(!sinks.send_error_if_current("cZ", "ws1", &err));
        assert!(rx.try_recv().is_err(), "no frame for stale/unknown targets");

        // Current ws_id → delivered as an ["error", …] frame (not a close).
        assert!(sinks.send_error_if_current("cA", "ws1", &err));
        match rx.try_recv() {
            Ok(WsCommand::Send { msg, .. }) => assert_eq!(msg[0], "error"),
            _ => panic!("expected an error Send frame on the current socket"),
        }
    }

    /// Coalescing parity with TS notifier.ts: the newer notification's fields
    /// win, but the merged upstream commit time keeps the OLDEST value (it
    /// bounds the lag of everything the merge subsumed).
    #[test]
    fn merge_notifications_keeps_newest_fields_and_oldest_commit_time() {
        let m = merge_notifications(
            serde_json::json!({"state":"version-ready","watermark":"05","upstreamCommitTimeMs":100.0}),
            serde_json::json!({"state":"version-ready","watermark":"09","upstreamCommitTimeMs":80.0}),
        );
        assert_eq!(m.get("watermark").unwrap(), "09");
        assert_eq!(m.get("upstreamCommitTimeMs").unwrap().as_f64(), Some(80.0));

        // A notification missing the commit time inherits the older one's.
        let m = merge_notifications(
            serde_json::json!({"watermark":"05","upstreamCommitTimeMs":50.0}),
            serde_json::json!({"watermark":"09"}),
        );
        assert_eq!(m.get("watermark").unwrap(), "09");
        assert_eq!(m.get("upstreamCommitTimeMs").unwrap().as_f64(), Some(50.0));

        // Fields present only on the older notification survive the merge.
        let m = merge_notifications(
            serde_json::json!({"state":"version-ready","watermark":"05"}),
            serde_json::json!({"watermark":"09"}),
        );
        assert_eq!(m.get("state").unwrap(), "version-ready");
        assert_eq!(m.get("watermark").unwrap(), "09");
    }

    #[test]
    fn non_empty_client_cookie_against_empty_cvr_is_client_not_found() {
        let client = Some(version_from_string("01"));
        let error = check_client_and_cvr_versions(&client, &EMPTY_CVR_VERSION).unwrap_err();
        assert_eq!(error.kind(), &crate::protocol::ErrorKind::ClientNotFound);
        assert_eq!(error.message(), "Client not found");
    }

    #[test]
    fn client_cookie_ahead_of_non_empty_cvr_is_invalid_base_cookie() {
        let client = Some(version_from_string("02"));
        // "01:00" parses to configVersion Some(0); versionString renders it as
        // the bare "01" (configVersion 0 is falsy in TS), so the error message
        // reads "01", not "01:00". See version_string's falsy-zero contract.
        let cvr = version_from_string("01:00");
        let error = check_client_and_cvr_versions(&client, &cvr).unwrap_err();
        assert_eq!(
            error.kind(),
            &crate::protocol::ErrorKind::InvalidConnectionRequestBaseCookie
        );
        assert_eq!(error.message(), "CVR is at version 01");
    }

    #[test]
    fn client_cookie_at_or_behind_cvr_is_accepted() {
        let cvr = version_from_string("02");
        assert!(check_client_and_cvr_versions(&Some(cvr.clone()), &cvr).is_ok());
        assert!(check_client_and_cvr_versions(&Some(version_from_string("01")), &cvr).is_ok());
        assert!(check_client_and_cvr_versions(&None, &cvr).is_ok());
    }

    struct NoopViewSyncer;
    impl ViewSyncerDispatch for NoopViewSyncer {
        fn change_desired_queries(&self, _s: &ConnectionSelector, _m: &str) {}
        fn update_auth(&self, _s: &ConnectionSelector, _m: &str, _c: bool) {}
        fn delete_clients(&self, _s: &ConnectionSelector, _m: &str) -> Vec<String> {
            Vec::new()
        }
        fn init_connection(&self, _s: &ConnectionSelector, _m: &str) -> bool {
            true
        }
        fn inspect(&self, _s: &ConnectionSelector, _m: &str) {}
    }

    struct NoopCcm;
    impl ConnContextManagerDispatch for NoopCcm {
        fn must_get_connection_context(&self, _s: &ConnectionSelector) -> ConnContextInfo {
            ConnContextInfo {
                auth: None,
                revision: 0,
            }
        }
        fn init_connection(&self, _s: &ConnectionSelector, _b: &serde_json::Value) {}
        fn update_auth(&self, _s: &ConnectionSelector, _b: &serde_json::Value) -> bool {
            true
        }
    }

    /// A CCM that counts `init_connection` calls, to prove the CG-thread path
    /// fires the side effect the message handler would (task 12).
    struct CountingCcm {
        init_calls: Arc<AtomicU64>,
    }
    impl ConnContextManagerDispatch for CountingCcm {
        fn must_get_connection_context(&self, _s: &ConnectionSelector) -> ConnContextInfo {
            ConnContextInfo {
                auth: None,
                revision: 0,
            }
        }
        fn init_connection(&self, _s: &ConnectionSelector, _b: &serde_json::Value) {
            self.init_calls.fetch_add(1, Ordering::SeqCst);
        }
        fn update_auth(&self, _s: &ConnectionSelector, _b: &serde_json::Value) -> bool {
            true
        }
    }

    struct CountingCcmFactory {
        handle: tokio::runtime::Handle,
        init_calls: Arc<AtomicU64>,
    }
    impl CGServicesFactory for CountingCcmFactory {
        fn create_view_syncer(&self, _cg: &str) -> Arc<dyn ViewSyncerDispatch> {
            Arc::new(NoopViewSyncer)
        }
        fn create_conn_context_manager(&self, _cg: &str) -> Arc<dyn ConnContextManagerDispatch> {
            Arc::new(CountingCcm {
                init_calls: self.init_calls.clone(),
            })
        }
        fn create_mutagen(&self, _cg: &str) -> Option<Arc<dyn MutagenDispatch>> {
            None
        }
        fn create_pusher(&self, _cg: &str) -> Option<Arc<dyn PusherDispatch>> {
            None
        }
        fn create_sync_engine_config(&self, _cg: &str) -> SyncEngineConfig {
            SyncEngineConfig {
                initialization_error: None,
                tables: Vec::new(),
                replica_path: None,
                app_id: "zero".to_string(),
                replica_version: "00".to_string(),
                shard: ShardID {
                    app_id: "zero".to_string(),
                    shard_num: 0,
                },
                cvr_pg: None,
                permissions: None,
                permissions_hash: None,
                revalidate_interval_ms: None,
                query_config: None,
                enable_query_covering: true,
                tokio_handle: self.handle.clone(),
                admin_password: None,
                server_version: "test".to_string(),
                metrics: std::sync::Arc::new(crate::metrics::Metrics::default()),
            }
        }
    }

    struct TestFactory {
        handle: tokio::runtime::Handle,
    }
    impl CGServicesFactory for TestFactory {
        fn create_view_syncer(&self, _cg: &str) -> Arc<dyn ViewSyncerDispatch> {
            Arc::new(NoopViewSyncer)
        }
        fn create_conn_context_manager(&self, _cg: &str) -> Arc<dyn ConnContextManagerDispatch> {
            Arc::new(NoopCcm)
        }
        fn create_mutagen(&self, _cg: &str) -> Option<Arc<dyn MutagenDispatch>> {
            None
        }
        fn create_pusher(&self, _cg: &str) -> Option<Arc<dyn PusherDispatch>> {
            None
        }
        fn create_sync_engine_config(&self, _cg: &str) -> SyncEngineConfig {
            SyncEngineConfig {
                initialization_error: None,
                tables: Vec::new(),
                replica_path: None, // in-memory (no PG, no replica)
                app_id: "zero".to_string(),
                replica_version: "00".to_string(),
                shard: ShardID {
                    app_id: "zero".to_string(),
                    shard_num: 0,
                },
                cvr_pg: None,
                permissions: None,
                permissions_hash: None,
                revalidate_interval_ms: None,
                query_config: None,
                enable_query_covering: true,
                tokio_handle: self.handle.clone(),
                admin_password: None,
                server_version: "test".to_string(),
                metrics: std::sync::Arc::new(crate::metrics::Metrics::default()),
            }
        }
    }

    /// Factory that points a CG at a real on-disk replica (with a
    /// `zero.permissions` row) and seeds an initial permissions hash — for the
    /// hot-reload test.
    struct PermsReloadFactory {
        handle: tokio::runtime::Handle,
        replica_path: String,
        initial_hash: Option<String>,
        initial_permissions: Option<serde_json::Value>,
    }
    impl CGServicesFactory for PermsReloadFactory {
        fn create_view_syncer(&self, _cg: &str) -> Arc<dyn ViewSyncerDispatch> {
            Arc::new(NoopViewSyncer)
        }
        fn create_conn_context_manager(&self, _cg: &str) -> Arc<dyn ConnContextManagerDispatch> {
            Arc::new(NoopCcm)
        }
        fn create_mutagen(&self, _cg: &str) -> Option<Arc<dyn MutagenDispatch>> {
            None
        }
        fn create_pusher(&self, _cg: &str) -> Option<Arc<dyn PusherDispatch>> {
            None
        }
        fn create_sync_engine_config(&self, _cg: &str) -> SyncEngineConfig {
            SyncEngineConfig {
                initialization_error: None,
                tables: Vec::new(),
                replica_path: Some(self.replica_path.clone()),
                app_id: "zero".to_string(),
                replica_version: "00".to_string(),
                shard: ShardID {
                    app_id: "zero".to_string(),
                    shard_num: 0,
                },
                cvr_pg: None,
                permissions: self.initial_permissions.clone(),
                permissions_hash: self.initial_hash.clone(),
                revalidate_interval_ms: None,
                query_config: None,
                enable_query_covering: true,
                tokio_handle: self.handle.clone(),
                admin_password: None,
                server_version: "test".to_string(),
                metrics: std::sync::Arc::new(crate::metrics::Metrics::default()),
            }
        }
    }

    /// `maybe_reload_permissions` is a no-op while the deployed hash is
    /// unchanged, and on a redeploy (new hash) it swaps in the new compiled
    /// permissions, remembers the new hash, and bumps the reload metric. This is
    /// the CG-thread half of the TS `reloadPermissionsIfChanged` hot-reload.
    #[test]
    fn maybe_reload_permissions_swaps_on_redeploy() {
        use rusqlite::Connection;
        let db_path = "/tmp/rust-syncer-perms-reload-test.db";
        for suffix in ["", "-wal", "-wal2", "-shm"] {
            let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
        }
        let doc_v1 = r#"{"tables":{}}"#;
        {
            let conn = Connection::open(db_path).unwrap();
            conn.execute_batch(r#"CREATE TABLE "zero.permissions" (permissions TEXT, hash TEXT);"#)
                .unwrap();
            conn.execute(
                r#"INSERT INTO "zero.permissions" (permissions, hash) VALUES (?1, 'h1')"#,
                rusqlite::params![doc_v1],
            )
            .unwrap();
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(PermsReloadFactory {
            handle: rt.handle().clone(),
            replica_path: db_path.to_string(),
            initial_hash: Some("h1".to_string()),
            initial_permissions: Some(serde_json::json!({"tables": {}})),
        });
        let global = Arc::new(Mutex::new(HashMap::new()));
        let count = Arc::new(AtomicU64::new(0));
        let mut state = CgState::new(
            "cg1",
            &factory,
            Arc::new(crate::auth::jwt::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            global,
            count,
        );

        // Same hash → no reload.
        assert!(!state.maybe_reload_permissions());
        assert_eq!(state.permissions_hash.as_deref(), Some("h1"));
        assert_eq!(state.metrics.snapshot()["permissionReloads"], 0);

        // Simulate a redeploy: new doc + new hash committed to the replica.
        let doc_v2 = r#"{"tables":{"issue":{"row":{"select":[]}}}}"#;
        {
            let conn = Connection::open(db_path).unwrap();
            conn.execute(
                r#"UPDATE "zero.permissions" SET permissions = ?1, hash = 'h2'"#,
                rusqlite::params![doc_v2],
            )
            .unwrap();
        }

        // Hash moved h1 → h2: reload swaps in the new compiled doc + hash.
        assert!(state.maybe_reload_permissions());
        assert_eq!(state.permissions_hash.as_deref(), Some("h2"));
        assert_eq!(
            state.permissions,
            Some(serde_json::json!({"tables":{"issue":{"row":{"select":[]}}}}))
        );
        assert_eq!(state.metrics.snapshot()["permissionReloads"], 1);

        for suffix in ["", "-wal", "-wal2", "-shm"] {
            let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
        }
    }

    fn test_params(client_id: &str, ws_id: &str) -> ConnectParams {
        ConnectParams {
            protocol_version: PROTOCOL_VERSION,
            client_id: client_id.to_string(),
            client_group_id: "cg1".to_string(),
            profile_id: None,
            base_cookie: None,
            timestamp: 0,
            lm_id: 0,
            ws_id: ws_id.to_string(),
            debug_perf: false,
            auth: None,
            user_id: None,
            init_connection_msg: None,
            http_cookie: None,
            origin: None,
            request_headers: Default::default(),
        }
    }

    fn seed_test_client_schema(state: &mut CgState) {
        let mut cvr = empty_cvr(&state.cg_id, &state.replica_version);
        cvr.client_schema = Some(serde_json::json!({"tables": {}}));
        state.cvr = Some(cvr);
    }

    /// I-8 Step 2 golden: `custom_query_context_from` maps the
    /// ConnectionContextManager's live `ConnectionContext` (the single owner of
    /// url/headers/auth/userID) onto the transform's `CustomQueryContext`, exactly
    /// as the deleted `client_query_ctx` map did. Drives a real
    /// register + initConnection through the CCM, then asserts every TS-config-
    /// derived field survives the mapping (url/auth/api_key/cookie/origin/
    /// allowed_urls/userID + the allowlist-filtered client headers).
    ///
    /// NON-VACUOUS: drop any field in `custom_query_context_from` (e.g. the
    /// `auth` or `cookie` mapping) and the corresponding assert fails.
    #[test]
    fn configured_query_context_matches_typescript_defaults_and_header_filtering() {
        use crate::services::view_syncer::connection_context_manager::{
            Auth, ConnectionContextManager,
        };
        let config = FetchConfig {
            url: Some(vec!["https://api.example/query".to_string()]),
            api_key: Some("secret".to_string()),
            allowed_client_headers: Some(vec!["X-Request-ID".to_string()]),
            allowed_request_headers: None,
            forward_cookies: true,
        };
        let mut ccm = ConnectionContextManager::new(None, None, Some(config), None, None, None);
        let selector = CcmConnectionSelector {
            client_id: "c1".to_string(),
            ws_id: "w1".to_string(),
        };
        let reg = ConnectParamsForRegistration {
            client_id: "c1".to_string(),
            ws_id: "w1".to_string(),
            user_id: Some("u1".to_string()),
            profile_id: None,
            base_cookie: None,
            protocol_version: 1,
            http_cookie: Some("session=1".to_string()),
            origin: Some("https://app.example".to_string()),
            request_headers: Vec::new(),
        };
        // An authenticated connection (TS requires a userID alongside a token).
        ccm.register_connection(
            &selector,
            &reg,
            Some(Auth::Opaque {
                raw: "jwt".to_string(),
            }),
        );
        // initConnection carries client headers; only the allowlisted one survives.
        ccm.init_connection(
            &selector,
            &InitConnectionBody {
                user_query_url: None,
                user_query_headers: Some(std::collections::HashMap::from([
                    ("x-request-id".to_string(), "allowed".to_string()),
                    ("authorization".to_string(), "blocked".to_string()),
                ])),
                user_push_url: None,
                user_push_headers: None,
            },
        )
        .unwrap();

        let ctx = ccm.must_get_connection_context(&selector).unwrap();
        let context = custom_query_context_from(&ctx).unwrap();
        assert_eq!(context.url, "https://api.example/query");
        assert_eq!(context.auth.as_deref(), Some("jwt"));
        assert_eq!(context.api_key.as_deref(), Some("secret"));
        assert_eq!(context.cookie.as_deref(), Some("session=1"));
        assert_eq!(context.origin.as_deref(), Some("https://app.example"));
        assert_eq!(context.allowed_urls, vec!["https://api.example/query"]);
        assert_eq!(context.user_id.as_deref(), Some("u1"));
        // initConnection header filtering: allowlisted survives, others dropped.
        assert_eq!(
            context.client_headers,
            vec![("x-request-id".to_string(), "allowed".to_string())]
        );
        // The composed outgoing set carries them all (TS fetchFromAPIServer).
        let composed = context.composed_headers();
        assert!(composed.contains(&("X-Api-Key".to_string(), "secret".to_string())));
        assert!(composed.contains(&("Cookie".to_string(), "session=1".to_string())));
        assert!(composed.contains(&("Origin".to_string(), "https://app.example".to_string())));
    }

    #[test]
    fn forwards_allowlisted_incoming_request_headers() {
        // Port of #6144: only headers on `allowed_request_headers` (case-
        // insensitive) are forwarded from the incoming request to the query API.
        // Read back from the CCM via `custom_query_context_from` (I-8 Step 2).
        use crate::services::view_syncer::connection_context_manager::ConnectionContextManager;
        let config = FetchConfig {
            url: Some(vec!["https://api.example/query".to_string()]),
            api_key: None,
            allowed_client_headers: None,
            allowed_request_headers: Some(vec![
                "X-Forwarded-For".to_string(),
                "x-tenant".to_string(),
            ]),
            forward_cookies: false,
        };
        let mut ccm = ConnectionContextManager::new(None, None, Some(config), None, None, None);
        let selector = CcmConnectionSelector {
            client_id: "c1".to_string(),
            ws_id: "w1".to_string(),
        };
        let reg = ConnectParamsForRegistration {
            client_id: "c1".to_string(),
            ws_id: "w1".to_string(),
            user_id: None,
            profile_id: None,
            base_cookie: None,
            protocol_version: 1,
            http_cookie: None,
            origin: None,
            request_headers: vec![
                ("x-forwarded-for".to_string(), "203.0.113.7".to_string()),
                ("x-tenant".to_string(), "acme".to_string()),
                ("authorization".to_string(), "secret".to_string()),
            ],
        };
        ccm.register_connection(&selector, &reg, None);

        let ctx = ccm.must_get_connection_context(&selector).unwrap();
        let context = custom_query_context_from(&ctx).unwrap();
        // Allowlisted (case-insensitive) headers forwarded; others dropped.
        assert!(
            context
                .request_headers
                .contains(&("x-forwarded-for".to_string(), "203.0.113.7".to_string()))
        );
        assert!(
            context
                .request_headers
                .contains(&("x-tenant".to_string(), "acme".to_string()))
        );
        assert!(
            !context
                .request_headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("authorization"))
        );
    }

    fn authed_params(client_id: &str, ws_id: &str, token: &str) -> ConnectParams {
        let mut p = test_params(client_id, ws_id);
        p.auth = Some(token.to_string());
        p
    }

    /// A minimal decodable JWT (`header.payload.sig`) whose payload is `{sub}`.
    /// Only the payload is read by `decode_jwt_claims`; the signature is unused
    /// by the pin pre-check.
    fn fake_jwt(sub: &str) -> String {
        use base64::Engine;
        let payload = serde_json::json!({ "sub": sub }).to_string();
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
        format!("hdr.{b64}.sig")
    }

    fn pinned_params(client_id: &str, ws_id: &str, user_id: &str) -> ConnectParams {
        let mut p = authed_params(client_id, ws_id, &fake_jwt(user_id));
        p.user_id = Some(user_id.to_string());
        p
    }

    /// The raw auth token the ConnectionContextManager holds for a connection.
    /// Replaces the deleted `client_raw_auth` map in tests — the CCM is now the
    /// single owner of per-connection auth (I-8).
    fn ccm_raw_auth(state: &CgState, client_id: &str, ws_id: &str) -> Option<String> {
        lock_unpoisoned(&state.ccm)
            .get_connection_context(&CcmConnectionSelector {
                client_id: client_id.to_string(),
                ws_id: ws_id.to_string(),
            })
            .and_then(|c| c.auth)
            .map(|a| a.raw().to_string())
    }

    /// AuthValidator whose verdict flips with a shared flag — to simulate a
    /// token that later expires / is revoked.
    struct ToggleAuthValidator {
        valid: Arc<std::sync::atomic::AtomicBool>,
    }
    #[async_trait::async_trait]
    impl AuthValidator for ToggleAuthValidator {
        async fn validate_auth(
            &self,
            _cg: &str,
            _cid: &str,
            _uid: Option<&str>,
            _auth: Option<&str>,
        ) -> Result<(), crate::protocol::ErrorBody> {
            if self.valid.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(crate::protocol::ErrorBody::unauthorized("token expired"))
            }
        }
    }

    /// Factory with a configurable periodic auth-maintenance interval (no PG,
    /// in-memory sources).
    struct RevalidateFactory {
        handle: tokio::runtime::Handle,
        revalidate_interval_ms: Option<i64>,
    }
    impl CGServicesFactory for RevalidateFactory {
        fn create_view_syncer(&self, _cg: &str) -> Arc<dyn ViewSyncerDispatch> {
            Arc::new(NoopViewSyncer)
        }
        fn create_conn_context_manager(&self, _cg: &str) -> Arc<dyn ConnContextManagerDispatch> {
            Arc::new(NoopCcm)
        }
        fn create_mutagen(&self, _cg: &str) -> Option<Arc<dyn MutagenDispatch>> {
            None
        }
        fn create_pusher(&self, _cg: &str) -> Option<Arc<dyn PusherDispatch>> {
            None
        }
        fn create_sync_engine_config(&self, _cg: &str) -> SyncEngineConfig {
            SyncEngineConfig {
                initialization_error: None,
                tables: Vec::new(),
                replica_path: None,
                app_id: "zero".to_string(),
                replica_version: "00".to_string(),
                shard: ShardID {
                    app_id: "zero".to_string(),
                    shard_num: 0,
                },
                cvr_pg: None,
                permissions: None,
                permissions_hash: None,
                revalidate_interval_ms: self.revalidate_interval_ms,
                query_config: None,
                enable_query_covering: true,
                tokio_handle: self.handle.clone(),
                admin_password: None,
                server_version: "test".to_string(),
                metrics: std::sync::Arc::new(crate::metrics::Metrics::default()),
            }
        }
    }

    fn revalidate_state(
        rt: &tokio::runtime::Runtime,
        interval_ms: Option<i64>,
        valid: Arc<std::sync::atomic::AtomicBool>,
    ) -> CgState {
        let factory: Arc<dyn CGServicesFactory> = Arc::new(RevalidateFactory {
            handle: rt.handle().clone(),
            revalidate_interval_ms: interval_ms,
        });
        CgState::new(
            "cg1",
            &factory,
            Arc::new(ToggleAuthValidator { valid }),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicU64::new(0)),
        )
    }

    /// F-CVR-STORE-8 interval state machine — ports of TS
    /// `#startTTLClockInterval` / `#stopTTLClockInterval` /
    /// `#updateTTLClockInCVRWithoutLock` (view-syncer.ts:1091-1119). The
    /// interval must be OFF until a flush arms it (TS arms only in
    /// `#flushUpdater`'s `if (flushed)`), arm ~TTL_CLOCK_INTERVAL out, and the
    /// no-CVR guard must keep the update call a no-op.
    #[test]
    fn ttl_clock_interval_state_machine() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, None, valid);

        // Not running until a flush starts it (a read-only CG never ticks).
        assert!(state.ttl_clock_interval.is_none());
        assert!(state.next_ttl_clock_delay().is_none());

        let before = now_ms();
        state.start_ttl_clock_interval();
        let deadline = state.ttl_clock_interval.expect("armed");
        assert!(
            deadline >= before + TTL_CLOCK_INTERVAL && deadline <= now_ms() + TTL_CLOCK_INTERVAL,
            "deadline must be TTL_CLOCK_INTERVAL (60s) out"
        );
        assert!(state.next_ttl_clock_delay().is_some());

        // Restart replaces the deadline (TS stop-then-set).
        state.start_ttl_clock_interval();
        assert!(state.ttl_clock_interval.expect("re-armed") >= deadline);

        state.stop_ttl_clock_interval();
        assert!(state.ttl_clock_interval.is_none());

        // No loaded CVR (TS `#ttlClock !== undefined` guard): update is a no-op
        // and must not advance the in-memory clock bookkeeping.
        let base_before = state.ttl_clock_base;
        state.update_ttl_clock_in_cvr_without_lock();
        assert_eq!(state.ttl_clock_base, base_before);
    }

    /// Periodic revalidation must CLOSE a connection whose token no longer
    /// validates (expired/revoked). Security core of TS `#runAuthMaintenance`'s
    /// `dueRevalidations` → `#validateConnection` failure path.
    #[test]
    fn periodic_revalidation_closes_expired_connection() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid.clone());

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        // A userID-bearing (JWT) connection — the case revalidation applies to.
        rt.block_on(state.on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));
        assert_eq!(state.registered_ws.len(), 1);

        // Arming happened on connect (interval set + a token present).
        assert!(state.next_auth_maintenance_at.is_some());

        // The token expires; the next maintenance tick must drop the connection.
        valid.store(false, Ordering::SeqCst);
        rt.block_on(state.on_auth_maintenance_tick());

        assert_eq!(
            state.registered_ws.len(),
            0,
            "expired connection must be closed"
        );
        assert!(
            ccm_raw_auth(&state, "c1", "ws1").is_none(),
            "closed connection's auth must be gone from the CCM"
        );
        assert_eq!(state.metrics.snapshot()["authRevalidationFailures"], 1);
        // No authed connection remains → disarmed.
        assert!(state.next_auth_maintenance_at.is_none());
    }

    /// initConnection with NO profileID must default the CVR profileID to
    /// `cg{clientGroupID}` — TS view-syncer.ts:862
    /// (`connCtx.profileID ?? `cg${this.id}``). Ported from
    /// view-syncer.pg.test.ts "initConnectionMessage with no profileID sets a
    /// default profileID based on the client group ID".
    #[test]
    fn absent_profile_id_defaults_to_cg_client_group_id() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        // authed_params → test_params, which sets profile_id = None,
        // client_group_id = "cg1".
        rt.block_on(state.on_new_connection(
            authed_params("c1", "ws1", "tok-c1"),
            DirectWebSocketSink::new(tx),
        ));

        assert_eq!(
            state.client_profile_ids.get("c1").map(String::as_str),
            Some("cgcg1"),
            "absent profileID must default to cg{{clientGroupID}}"
        );
    }

    /// A profileID supplied in the connection URL is used verbatim (the default
    /// only applies when absent).
    #[test]
    fn present_profile_id_is_used_verbatim() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let mut params = authed_params("c1", "ws1", "tok-c1");
        params.profile_id = Some("p-explicit".to_string());
        rt.block_on(state.on_new_connection(params, DirectWebSocketSink::new(tx)));

        assert_eq!(
            state.client_profile_ids.get("c1").map(String::as_str),
            Some("p-explicit"),
            "explicit profileID must be used verbatim, not defaulted"
        );
    }

    /// A `deleteClients` frame arriving on a SUPERSEDED (old) wsID must be
    /// ignored — the stale-frame guard drops it, so the targeted client is not
    /// deleted. Ports view-syncer.pg.test.ts "ignores deleteClients from old
    /// wsID".
    #[test]
    fn delete_clients_from_stale_ws_id_is_ignored() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        // Connect "foo" on ws1, then reconnect "foo" on ws2 (supersedes ws1).
        let (tx1, _d1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            authed_params("foo", "ws1", "tok"),
            DirectWebSocketSink::new(tx1),
        ));
        let (tx2, _d2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            authed_params("foo", "ws2", "tok"),
            DirectWebSocketSink::new(tx2),
        ));
        assert_eq!(
            state.registered_ws.get("foo").map(String::as_str),
            Some("ws2"),
            "reconnect should supersede ws1 with ws2"
        );

        // deleteClients targeting "foo" arrives on the STALE ws1 → must be dropped.
        rt.block_on(state.on_inbound(
            "foo".into(),
            "ws1".into(),
            r#"["deleteClients",{"clientIDs":["foo"]}]"#.to_string(),
        ));

        // The stale frame was ignored: "foo" is still registered (on ws2), not
        // deleted.
        assert_eq!(
            state.registered_ws.get("foo").map(String::as_str),
            Some("ws2"),
            "deleteClients from a stale wsID must not delete the client"
        );
    }

    /// `activeClients` GC: any CVR client absent from the active set is selected
    /// for removal (its queries are then inactivated + TTL-expired). Ports the
    /// selection core of view-syncer.pg.test.ts "activeClients inactivates queries
    /// from inactive clients". The inactivate→expire chain itself is covered by
    /// `delete_clients_removes_client_and_acks` +
    /// `expired_query_is_removed_after_ttl_elapses`.
    #[test]
    fn active_clients_gc_selects_clients_not_in_set() {
        let cvr_clients = vec![
            "clientA".to_string(),
            "clientB".to_string(),
            "clientC".to_string(),
        ];
        // activeClients = [A, B] → C is inactive and must be removed.
        let del = clients_to_delete(
            &cvr_clients,
            Some(&["clientA".to_string(), "clientB".to_string()]),
            &[],
        );
        assert_eq!(del, vec!["clientC".to_string()]);
    }

    /// activeClients GC unions with explicit deletions, without duplicating a
    /// client already selected by the GC.
    #[test]
    fn active_clients_gc_unions_explicit_deletions() {
        let cvr_clients = vec![
            "clientA".to_string(),
            "clientB".to_string(),
            "clientC".to_string(),
        ];
        // active = [A]; explicit delete of B and C (C also GC-selected → no dup).
        let del = clients_to_delete(
            &cvr_clients,
            Some(&["clientA".to_string()]),
            &["clientB".to_string(), "clientC".to_string()],
        );
        assert_eq!(del, vec!["clientB".to_string(), "clientC".to_string()]);
    }

    /// No `activeClients` → no GC; only explicit deletions are removed.
    #[test]
    fn no_active_clients_means_only_explicit_deletions() {
        let cvr_clients = vec!["clientA".to_string(), "clientB".to_string()];
        assert!(clients_to_delete(&cvr_clients, None, &[]).is_empty());
        assert_eq!(
            clients_to_delete(&cvr_clients, None, &["clientB".to_string()]),
            vec!["clientB".to_string()]
        );
    }

    /// A CVR written by a NEWER replica than the one we serve must produce the
    /// exact TS ClientNotFound message (view-syncer.pg.test.ts "sends reset for
    /// CVR from older replica version up"). This drives the client to wipe local
    /// state and re-sync fresh rather than reconnect elsewhere.
    #[test]
    fn older_replica_error_matches_ts_message() {
        let mut cvr = empty_cvr("cg1", "01");
        // A synced CVR (state_version != "00") from replica "101" > our "01".
        cvr.version.state_version = "07".to_string();
        cvr.replica_version = Some("101".to_string());
        assert_eq!(
            older_replica_error(&cvr, "01").as_deref(),
            Some("Cannot sync from older replica: CVR=101, DB=01"),
        );
    }

    /// No error when the replica is the same/older, or the CVR is brand new
    /// (state_version "00", never synced — exempt even if its replica is newer).
    #[test]
    fn older_replica_error_none_when_not_older() {
        // Same replica version → safe.
        let mut same = empty_cvr("cg1", "01");
        same.version.state_version = "07".to_string();
        same.replica_version = Some("01".to_string());
        assert!(older_replica_error(&same, "01").is_none());

        // Brand-new CVR (state_version "00") is exempt even from a newer replica.
        let mut fresh = empty_cvr("cg1", "01");
        fresh.replica_version = Some("101".to_string());
        assert_eq!(fresh.version.state_version, "00");
        assert!(older_replica_error(&fresh, "01").is_none());
    }

    /// updateAuth with a refreshed OPAQUE token must re-transform (opaque tokens
    /// carry no claims, so the change must be detected on the raw token, not
    /// decoded claims). Ported from view-syncer.pg.test.ts "retransforms custom
    /// queries when opaque auth refreshes". `auth_changes` increments only on the
    /// re-transform path, so it is the signal that a re-transform was triggered.
    #[test]
    fn update_auth_opaque_token_change_retransforms() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        // Opaque token (not a JWT), user_id None → group stays unpinned.
        rt.block_on(state.on_new_connection(
            authed_params("c1", "ws1", "opaque-token-1"),
            DirectWebSocketSink::new(tx),
        ));
        assert_eq!(state.metrics.snapshot()["authChanges"], 0);

        // Refresh to a DIFFERENT opaque token → must re-transform. `auth_changes`
        // is incremented on the re-transform path (before the config/hydrate),
        // so it is the robust signal that the change was detected — regardless of
        // what the barebones test factory's config/hydrate then does downstream.
        rt.block_on(state.handle_update_auth("c1", "opaque-token-2"));
        assert_eq!(
            state.metrics.snapshot()["authChanges"],
            1,
            "opaque token refresh must trigger a re-transform"
        );
    }

    /// updateAuth with the SAME opaque token is a no-op (no re-transform) — the
    /// raw-token comparison must treat an unchanged token as unchanged.
    #[test]
    fn update_auth_same_opaque_token_skips_retransform() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        // Opaque token WITH a userID — `resolve_auth` requires a userID whenever a
        // token is present (auth.ts:79-85), so the ConnectionContextManager holds
        // the token and the unchanged-check can compare against it.
        let mut params = authed_params("c1", "ws1", "opaque-token-1");
        params.user_id = Some("user-1".to_string());
        rt.block_on(state.on_new_connection(params, DirectWebSocketSink::new(tx)));

        rt.block_on(state.handle_update_auth("c1", "opaque-token-1"));
        assert_eq!(
            state.metrics.snapshot()["authChanges"],
            0,
            "an unchanged opaque token must NOT trigger a re-transform"
        );
    }

    /// Regression (push-relay 401, prod incident 2026-08-27): `updateAuth` MUST
    /// refresh the token forwarded on relayed custom-mutation pushes. Rust
    /// snapshotted `PushRelayHeaders.auth` at `initConnection` and never updated
    /// it, so a client that refreshed its JWT mid-session kept having the STALE
    /// connect-time token relayed to the API server → 401 "Invalid or expired
    /// token" on every mutation. TS reads `mustGetConnectionContext` fresh per
    /// push (pusher.ts), so the forwarded token always tracks `updateAuth`.
    ///
    /// NON-VACUOUS: before the fix, `handle_update_auth` did not touch the push
    /// header, so the shared cell stays at the connect-time token and this second
    /// assert fails. (Verified by reverting the `handle_update_auth` refresh.)
    #[test]
    fn update_auth_refreshes_the_forwarded_push_relay_token() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            authed_params("c1", "ws1", "token-1"),
            DirectWebSocketSink::new(tx),
        ));

        // Capture the SHARED auth cell (the message handler holds the same Arc).
        // Cloning the Arc keeps the assertion valid even though a downstream
        // re-hydrate in this storeless harness later drops the map entry.
        let auth_cell = state
            .client_push_headers
            .get("c1")
            .expect("push headers for c1")
            .auth
            .clone();

        // The relayed push forwards the connect-time token.
        assert_eq!(
            auth_cell.lock().unwrap().as_deref(),
            Some("token-1"),
            "initial forwarded push token is the connect-time token"
        );

        // A refreshed token must be forwarded on subsequent pushes.
        rt.block_on(state.handle_update_auth("c1", "token-2"));
        assert_eq!(
            auth_cell.lock().unwrap().as_deref(),
            Some("token-2"),
            "updateAuth must refresh the token forwarded on relayed pushes \
             (a stale snapshot is what caused the API-server 401 storm)"
        );
    }

    /// The ConnectionContextManager owns per-connection auth: `registerConnection`
    /// seeds the connect-time token, `updateAuth` refreshes it, and
    /// `closeConnection` drops the entry (no leaked auth — the bug-2 soil). Pins
    /// that the seeded auth equals the connect token and survives a token refresh.
    ///
    /// NON-VACUOUS: registering with `auth: None` fails the seeded-auth assert;
    /// skipping the `close_connection` on teardown fails the final `is_err`.
    #[test]
    fn connection_context_manager_tracks_register_update_and_close() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        // Authed, user-pinned connection (`resolve_auth` requires a userID when a
        // token is present — TS auth.ts:79-85).
        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));

        let selector = CcmConnectionSelector {
            client_id: "c1".to_string(),
            ws_id: "ws1".to_string(),
        };
        assert!(
            state
                .ccm
                .lock()
                .unwrap()
                .must_get_connection_context(&selector)
                .is_ok(),
            "on_new_connection must register the connection in the CCM"
        );

        // The connect-time auth is seeded from the connect token.
        let seeded = state
            .ccm
            .lock()
            .unwrap()
            .must_get_connection_context(&selector)
            .unwrap()
            .auth
            .map(|a| a.raw().to_string());
        assert!(
            seeded.is_some(),
            "connect-time auth must be seeded at register"
        );
        assert_eq!(
            seeded.as_deref(),
            Some(fake_jwt("user-1").as_str()),
            "seeded CCM auth must equal the connect token"
        );

        // A refreshed token for the SAME user (distinct raw) flows through
        // `updateAuth`. We call it directly rather than the full
        // `handle_update_auth`, whose no-PG re-transform (`ensure_cvr`
        // ClientNotFound) tears the connection down — that teardown is asserted
        // below.
        let token2 = {
            use base64::Engine;
            let payload = serde_json::json!({"sub": "user-1", "iat": 2}).to_string();
            let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
            format!("hdr.{b64}.sig")
        };
        let _ = state.ccm.lock().unwrap().update_auth(
            &selector,
            &UpdateAuthBody {
                auth: Some(token2.clone()),
            },
        );
        let raw = state
            .ccm
            .lock()
            .unwrap()
            .must_get_connection_context(&selector)
            .expect("still registered")
            .auth
            .map(|a| a.raw().to_string());
        assert_eq!(
            raw.as_deref(),
            Some(token2.as_str()),
            "updateAuth must refresh the CCM's auth token"
        );

        // A teardown drops the connection from the CCM (no leaked auth).
        state.on_connection_closed("c1", "ws1");
        assert!(
            state
                .ccm
                .lock()
                .unwrap()
                .must_get_connection_context(&selector)
                .is_err(),
            "on_connection_closed must drop the connection from the CCM"
        );
    }

    /// The permission `authData` is read from the ConnectionContextManager at use
    /// time (TS `mustGetConnectionContext(selector).auth?.raw`, decoded), not from
    /// a separate cache. For a JWT connection the CCM-derived claims must carry the
    /// token's `sub` and equal the decoded connect token — read-permission
    /// evaluation is unchanged.
    ///
    /// NON-VACUOUS: a CCM returning no auth yields `{}` instead of
    /// `{sub:"user-1"}`, failing the assert.
    #[test]
    fn authdata_reads_from_connection_context_manager() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));

        let via_ccm = state
            .ccm
            .lock()
            .unwrap()
            .must_get_connection_context(&CcmConnectionSelector {
                client_id: "c1".to_string(),
                ws_id: "ws1".to_string(),
            })
            .unwrap()
            .auth
            .map(|a| crate::auth::jwt::decode_jwt_claims(a.raw()))
            .unwrap_or_else(|| serde_json::json!({}));
        assert_eq!(
            via_ccm.get("sub").and_then(|v| v.as_str()),
            Some("user-1"),
            "authData from the CCM must carry the JWT sub"
        );
        let token = ccm_raw_auth(&state, "c1", "ws1").unwrap();
        assert_eq!(
            via_ccm,
            crate::auth::jwt::decode_jwt_claims(&token),
            "CCM authData must equal the decoded connect token"
        );
    }

    /// A still-valid token survives the tick and the deadline is re-armed for the
    /// next interval.
    #[test]
    fn periodic_revalidation_keeps_valid_connection_and_rearms() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        // A userID-bearing (JWT) connection — the case revalidation applies to;
        // `resolve_auth` requires a userID with a token (auth.ts:79-85), so the
        // ConnectionContextManager holds its auth.
        rt.block_on(state.on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));
        seed_test_client_schema(&mut state);
        let armed_before = state.next_auth_maintenance_at;
        assert!(armed_before.is_some());

        rt.block_on(state.on_auth_maintenance_tick());

        assert_eq!(
            state.registered_ws.len(),
            1,
            "valid connection must survive"
        );
        assert_eq!(state.metrics.snapshot()["authRevalidations"], 1);
        // Re-armed (still a token present).
        assert!(state.next_auth_maintenance_at.is_some());
    }

    /// Auth maintenance reads the token from the ConnectionContextManager — the
    /// single owner of per-connection auth. Arming + revalidation are driven
    /// entirely from the CCM (there is no separate auth map anymore).
    ///
    /// NON-VACUOUS: if arming/revalidation did not read the CCM, the connection's
    /// auth would be invisible → no arm, no revalidation, and both asserts fail.
    #[test]
    fn auth_maintenance_reads_token_from_the_connection_context_manager() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));
        seed_test_client_schema(&mut state);

        state.next_auth_maintenance_at = None;
        state.arm_auth_maintenance();
        assert!(
            state.next_auth_maintenance_at.is_some(),
            "arm_auth_maintenance must see the connection's auth via the CCM"
        );

        rt.block_on(state.on_auth_maintenance_tick());
        assert_eq!(
            state.registered_ws.len(),
            1,
            "the (valid) connection must be revalidated and survive"
        );
        assert_eq!(state.metrics.snapshot()["authRevalidations"], 1);
    }

    /// With the feature disabled (interval None) no deadline is ever armed, and a
    /// connection without a token is never subject to revalidation.
    #[test]
    fn periodic_revalidation_disabled_or_unauthed_never_arms() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));

        // Disabled: interval None.
        let mut disabled = revalidate_state(&rt, None, valid.clone());
        let (tx, _d) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(disabled.on_new_connection(
            authed_params("c1", "ws1", "tok"),
            DirectWebSocketSink::new(tx),
        ));
        assert!(disabled.next_auth_maintenance_at.is_none());
        assert!(disabled.next_auth_maintenance_delay().is_none());

        // Enabled but the connection carries no token → nothing to revalidate.
        let mut unauthed = revalidate_state(&rt, Some(300_000), valid);
        let (tx2, _d2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(
            unauthed.on_new_connection(test_params("c2", "ws2"), DirectWebSocketSink::new(tx2)),
        );
        assert!(unauthed.next_auth_maintenance_at.is_none());
    }

    /// Single-user pin: once a group is pinned to user-1, an `updateAuth` bearing
    /// a validly-formed token for a DIFFERENT user (user-2) must be REJECTED and
    /// the connection closed — a group cannot be re-scoped to another user
    /// mid-connection. Port of `pickToken`'s "pinned to a single user" rule.
    #[test]
    fn update_auth_rejects_cross_user_token() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));
        assert_eq!(state.pinned_user_id.as_deref(), Some("user-1"));
        assert_eq!(state.registered_ws.len(), 1);

        // updateAuth with a token for user-2 → rejected, connection closed.
        rt.block_on(state.handle_update_auth("c1", &fake_jwt("user-2")));
        assert_eq!(
            state.registered_ws.len(),
            0,
            "cross-user updateAuth must close the connection"
        );
        assert_eq!(state.metrics.snapshot()["authRevalidationFailures"], 1);
    }

    /// The pin allows an `updateAuth` whose token stays on the SAME user.
    #[test]
    fn update_auth_accepts_same_user_token() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));

        // Same-user token → not rejected (connection stays open).
        rt.block_on(state.handle_update_auth("c1", &fake_jwt("user-1")));
        assert_eq!(
            state.registered_ws.len(),
            1,
            "same-user updateAuth must keep the connection open"
        );
    }

    /// The CG event loop: a new connection registers a client with the
    /// SyncEngine; a notification with no CVR is graceful; a disconnect
    /// unregisters the client. Runs on the test thread (not a tokio worker), so
    /// the sink's `blocking_send` is legal.
    ///
    /// Note: `on_new_connection` (the SERIAL CG-thread path) must NOT emit
    /// `connected` — that message is sent on the accept task
    /// (`handle_connection`, TS `syncer.ts#handleConnection`) so the connect-ack
    /// is never queued behind an in-flight `config_and_hydrate`. This test pins
    /// that: registration happens here, `connected` does not.
    #[test]
    fn cg_state_connection_lifecycle_and_notification() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let global = Arc::new(Mutex::new(HashMap::new()));
        let count = Arc::new(AtomicU64::new(0));
        let mut state = CgState::new(
            "cg1",
            &factory,
            Arc::new(crate::auth::jwt::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            global,
            count,
        );

        let (tx, mut drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let sink = DirectWebSocketSink::new(tx);
        rt.block_on(state.on_new_connection(test_params("c1", "ws1"), sink));

        // The CG-thread path registers the client but does NOT emit `connected`
        // (that is the accept task's job — see `handle_connection`).
        let mut connected = false;
        while let Ok(cmd) = drx.try_recv() {
            if let WsCommand::Send { msg: v, .. } = cmd
                && v[0] == "connected"
            {
                connected = true;
            }
        }
        assert!(
            !connected,
            "on_new_connection (CG thread) must NOT send `connected`; the accept \
             task does (decoupling the ack from config_and_hydrate)"
        );
        assert_eq!(state.registered_ws.len(), 1);
        assert_eq!(state.connections.len(), 1);

        // Notification with no loaded CVR (no PG) is a graceful no-op.
        rt.block_on(state.on_notification(serde_json::json!({"state": "version-ready"})));

        // Disconnect unregisters the client.
        state.on_connection_closed("c1", "ws1");
        assert_eq!(state.registered_ws.len(), 0);
        assert_eq!(state.connections.len(), 0);
    }

    /// L7 cancel-during-hydrate teardown completeness (view-syncer.ts:916 —
    /// closes its GAP). TS returns `downstream` synchronously from
    /// `initConnection` so a close arriving DURING hydrate can still cancel the
    /// subscription "even if #runInLockForClient() has not had a chance to run."
    /// In rust that close reaches the serial CG thread as a `ConnectionClosed`
    /// enqueued AFTER `NewConnection` (FIFO), so it is processed once the blocked
    /// hydrate releases — that the serial channel DOES process a message queued
    /// behind a blocked hydrate is the other half, proven by
    /// `connected_ack_is_decoupled_from_a_blocked_cg_hydrate`. This test pins
    /// THIS half: when that close finally runs, `on_connection_closed` must fully
    /// tear the client down. No per-client state (auth, raw auth, query ctx, push
    /// headers, profile id, base version, sink registration) may leak — a leak
    /// would let a reconnecting client or a later relayed push read stale auth
    /// (the bug-2 class this framework exists to kill).
    ///
    /// NON-VACUOUS: delete any single `self.<map>.remove(client_id)` line from
    /// `on_connection_closed` and the matching `is_empty()` assertion fails.
    #[test]
    fn a_close_fully_tears_down_all_per_client_state() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let count = Arc::new(AtomicU64::new(0));
        let mut state = CgState::new(
            "cg-teardown",
            &factory,
            Arc::new(crate::auth::jwt::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            Arc::new(Mutex::new(HashMap::new())),
            count,
        );

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let sink = DirectWebSocketSink::new(tx);
        rt.block_on(state.on_new_connection(test_params("c1", "ws1"), sink));

        // Simulate a fully-hydrated client: seed the remaining per-client state
        // the way a completed initConnection would (connections/registered_ws are
        // already set by on_new_connection; the per-connection auth/query context
        // lives in the ConnectionContextManager, registered by on_new_connection).
        state
            .client_profile_ids
            .insert("c1".into(), "profile-1".into());
        let sel = CcmConnectionSelector {
            client_id: "c1".to_string(),
            ws_id: "ws1".to_string(),
        };
        assert!(
            lock_unpoisoned(&state.ccm)
                .get_connection_context(&sel)
                .is_some()
                && !state.connections.is_empty()
        );

        // The mid-hydrate cancel, delivered to the serial thread after the
        // hydrate as `ConnectionClosed`.
        state.on_connection_closed("c1", "ws1");

        assert!(state.connections.is_empty(), "connections leaked");
        assert!(state.registered_ws.is_empty(), "registered_ws leaked");
        assert!(
            lock_unpoisoned(&state.ccm)
                .get_connection_context(&sel)
                .is_none(),
            "ConnectionContextManager leaked — stale per-connection auth/query \
             context survives close"
        );
        assert!(
            state.client_push_headers.is_empty(),
            "client_push_headers leaked — stale relay headers survive close"
        );
        assert!(
            state.client_profile_ids.is_empty(),
            "client_profile_ids leaked"
        );
        assert!(
            state.client_base_versions.is_empty(),
            "client_base_versions leaked"
        );
    }

    #[test]
    fn idle_shutdown_requires_both_keepalive_expiry_and_zero_admissions() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let count = Arc::new(AtomicU64::new(0));
        let mut state = CgState::new(
            "cg-idle",
            &factory,
            Arc::new(crate::auth::jwt::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            Arc::new(Mutex::new(HashMap::new())),
            count.clone(),
        );

        // Empty is not enough: the TS-compatible keepalive protects a recently
        // disconnected group from reconnect thrash.
        state.keepalive_until = now_ms() + 60_000;
        assert!(!state.idle_shutdown_due());
        assert!(state.next_idle_shutdown_delay().is_some());

        // Expiry makes the empty group eligible.
        state.keepalive_until = now_ms() - 1;
        assert!(state.idle_shutdown_due());

        // A connection admitted by the router but not installed on the CG
        // thread yet must keep the group alive as well.
        count.store(1, Ordering::Relaxed);
        assert!(!state.idle_shutdown_due());
        assert!(state.next_idle_shutdown_delay().is_none());
    }

    /// The first connection binds the client group's userID; a later connection
    /// with a different userID is rejected, while the same userID is allowed.
    #[test]
    fn group_pins_user_id_on_first_connection() {
        let mut group = GroupAuthState::default();
        // First connection binds the pin.
        assert!(check_and_pin_user(&mut group, "user-1").is_ok());
        assert_eq!(group.pinned_user_id.as_deref(), Some("user-1"));
        // Same user → allowed, pin unchanged.
        assert!(check_and_pin_user(&mut group, "user-1").is_ok());
        assert_eq!(group.pinned_user_id.as_deref(), Some("user-1"));
        // Different user → rejected, pin unchanged.
        assert!(check_and_pin_user(&mut group, "user-2").is_err());
        assert_eq!(group.pinned_user_id.as_deref(), Some("user-1"));
    }

    /// When a client reconnects (same clientID, new wsID) the superseded
    /// connection's socket must be failed/closed and unregistered — otherwise the
    /// old ws_id keeps receiving pokes and its socket lingers open.
    #[test]
    fn reconnect_closes_superseded_connection() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let global = Arc::new(Mutex::new(HashMap::new()));
        let count = Arc::new(AtomicU64::new(2));
        let mut state = CgState::new(
            "cg1",
            &factory,
            Arc::new(crate::auth::jwt::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            global,
            count,
        );

        // First connection: client c1 on ws1.
        let (tx1, mut drx1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(
            state.on_new_connection(test_params("c1", "ws1"), DirectWebSocketSink::new(tx1)),
        );
        while drx1.try_recv().is_ok() {} // drain ws1's `connected` frame

        // Reconnect: same client c1 on a NEW ws2.
        let (tx2, _drx2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(
            state.on_new_connection(test_params("c1", "ws2"), DirectWebSocketSink::new(tx2)),
        );

        // The superseded ws1 socket must have been failed/closed.
        let mut ws1_failed = false;
        while let Ok(cmd) = drx1.try_recv() {
            if matches!(cmd, WsCommand::Fail(_)) {
                ws1_failed = true;
            }
        }
        assert!(
            ws1_failed,
            "the superseded ws1 connection must be failed/closed"
        );

        // The mapping now points at ws2, with exactly one registered client.
        assert_eq!(
            state.registered_ws.get("c1").map(String::as_str),
            Some("ws2")
        );
        assert_eq!(state.registered_ws.len(), 1);

        // The delayed close event from ws1 must not tear down ws2.
        state.on_connection_closed("c1", "ws1");
        assert_eq!(
            state.registered_ws.get("c1").map(String::as_str),
            Some("ws2")
        );
        assert!(state.connections.contains_key("c1"));
    }

    /// On shutdown (drain), each connection is failed with a `Rehome` error so
    /// the client reconnects elsewhere, rather than a silent close.
    #[test]
    fn shutdown_fails_connections_with_rehome() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let global = Arc::new(Mutex::new(HashMap::new()));
        let count = Arc::new(AtomicU64::new(0));
        let mut state = CgState::new(
            "cg1",
            &factory,
            Arc::new(crate::auth::jwt::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            global,
            count,
        );

        let (tx, mut drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let sink = DirectWebSocketSink::new(tx);
        rt.block_on(state.on_new_connection(test_params("c1", "ws1"), sink));

        state.shutdown();

        let mut saw_rehome = false;
        while let Ok(WsCommand::Send { msg: v, .. }) = drx.try_recv() {
            if v[0] == "error" {
                let s = serde_json::to_string(&v).unwrap();
                if s.contains("Rehome") {
                    saw_rehome = true;
                }
            }
        }
        assert!(saw_rehome, "expected a Rehome error frame on shutdown");
        assert_eq!(state.connections.len(), 0);
        assert_eq!(state.registered_ws.len(), 0);
        assert!(!state.accepting.load(Ordering::SeqCst));

        // Cleanup is deliberately idempotent: shutdown can arrive after a
        // terminal failure or an idle-expiry path.
        state.shutdown();
        assert_eq!(state.connection_count.load(Ordering::Relaxed), 0);
    }

    /// `broadcast_notification` fans out to every CG thread. With none
    /// registered it is a no-op returning 0 (the global-commit path is exercised
    /// end-to-end by the replica/PG harness).
    #[test]
    fn broadcast_notification_with_no_cgs_returns_zero() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        });
        let router = ConnectionRouter::new(
            factory,
            validator,
            Arc::new(crate::metrics::Metrics::default()),
        );
        assert_eq!(
            router.broadcast_notification(serde_json::json!({"state": "version-ready"})),
            0
        );
    }

    #[test]
    fn client_group_creation_is_single_owner_and_bounded() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        });
        let router = Arc::new(ConnectionRouter::new_with_limit(
            factory,
            validator,
            Arc::new(crate::metrics::Metrics::default()),
            1,
        ));

        std::thread::scope(|scope| {
            for _ in 0..8 {
                let router = router.clone();
                scope.spawn(move || {
                    router.get_or_create_cg("cg1").unwrap();
                });
            }
        });
        assert_eq!(router.cg_count(), 1, "one CG id must have one owner thread");
        assert!(
            router.get_or_create_cg("cg2").is_err(),
            "an active CG must not be evicted past the configured limit"
        );

        let cg1 = router.cg_handles.get("cg1").unwrap();
        cg1.connection_count.store(0, Ordering::Relaxed);
        drop(cg1);
        assert!(router.get_or_create_cg("cg2").is_ok());
        assert_eq!(
            router.cg_count(),
            1,
            "an idle CG is evicted for the new group"
        );
        rt.block_on(router.shutdown());
    }

    /// At the client-group cap with no idle CG to evict, a new group's
    /// connection is REHOMED (retryable, load-shed), not hard-rejected with
    /// `ServerOverloaded`. Mirrors TS's drain/rehome load-shedding and avoids the
    /// reject→retry storm the old cap behavior caused near saturation.
    #[test]
    fn overflow_rehomes_instead_of_server_overloaded() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        });
        // Cap of 1: the first group fills the only slot.
        let router = Arc::new(ConnectionRouter::new_with_limit(
            factory,
            validator,
            Arc::new(crate::metrics::Metrics::default()),
            1,
        ));

        let make_ctx = |cgid: &str, cid: &str, ws: &str| {
            let mut params = test_params(cid, ws);
            params.client_group_id = cgid.to_string();
            let (up_tx, up_rx) = tokio::sync::mpsc::channel::<String>(8);
            let (sink_tx, sink_rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
            (
                ConnectionContext {
                    params,
                    sink: DirectWebSocketSink::new(sink_tx),
                    upstream_rx: up_rx,
                },
                up_tx,
                sink_rx,
            )
        };

        // 1. First group takes the single slot. Keep its upstream sender alive so
        //    the connection stays active (connection_count == 1) and the CG is
        //    NOT idle/evictable.
        let (ctx1, _keep_alive1, _sink1) = make_ctx("cgA", "cA", "wsA");
        rt.block_on(router.handle_connection(ctx1));
        assert_eq!(router.cg_count(), 1);

        // 2. A second, distinct group at cap with no idle CG -> its sink must get
        //    a Rehome error, never ServerOverloaded.
        let (ctx2, _keep_alive2, mut sink2) = make_ctx("cgB", "cB", "wsB");
        rt.block_on(router.handle_connection(ctx2));

        let mut saw_rehome = false;
        let mut saw_overloaded = false;
        while let Ok(cmd) = sink2.try_recv() {
            if let WsCommand::Fail(body) = cmd {
                match body.kind() {
                    crate::protocol::ErrorKind::Rehome => saw_rehome = true,
                    crate::protocol::ErrorKind::ServerOverloaded => saw_overloaded = true,
                    _ => {}
                }
            }
        }
        assert!(saw_rehome, "overflow must Rehome (load-shed)");
        assert!(
            !saw_overloaded,
            "overflow must NOT ServerOverloaded — that reject was the storm cause"
        );
        // The overflow group was not admitted.
        assert_eq!(router.cg_count(), 1);

        rt.block_on(router.shutdown());
    }

    /// A CCM whose `init_connection` blocks the CG thread on the first call,
    /// simulating a long synchronous `config_and_hydrate`. Signals `entered`
    /// when it reaches the block and holds until the test flips `release`.
    struct BlockingCcm {
        entered: Arc<AtomicBool>,
        release: Arc<(Mutex<bool>, std::sync::Condvar)>,
        blocked_once: AtomicBool,
    }
    impl ConnContextManagerDispatch for BlockingCcm {
        fn must_get_connection_context(&self, _s: &ConnectionSelector) -> ConnContextInfo {
            ConnContextInfo {
                auth: None,
                revision: 0,
            }
        }
        fn init_connection(&self, _s: &ConnectionSelector, _b: &serde_json::Value) {
            // Only the first (blocker) connection holds the thread.
            if self.blocked_once.swap(true, Ordering::SeqCst) {
                return;
            }
            self.entered.store(true, Ordering::SeqCst);
            let (m, cv) = &*self.release;
            let mut released = m.lock().unwrap();
            while !*released {
                released = cv.wait(released).unwrap();
            }
        }
        fn update_auth(&self, _s: &ConnectionSelector, _b: &serde_json::Value) -> bool {
            true
        }
    }

    struct BlockingCcmFactory {
        handle: tokio::runtime::Handle,
        ccm: Arc<BlockingCcm>,
    }
    impl CGServicesFactory for BlockingCcmFactory {
        fn create_view_syncer(&self, _cg: &str) -> Arc<dyn ViewSyncerDispatch> {
            Arc::new(NoopViewSyncer)
        }
        fn create_conn_context_manager(&self, _cg: &str) -> Arc<dyn ConnContextManagerDispatch> {
            self.ccm.clone()
        }
        fn create_mutagen(&self, _cg: &str) -> Option<Arc<dyn MutagenDispatch>> {
            None
        }
        fn create_pusher(&self, _cg: &str) -> Option<Arc<dyn PusherDispatch>> {
            None
        }
        fn create_sync_engine_config(&self, _cg: &str) -> SyncEngineConfig {
            SyncEngineConfig {
                initialization_error: None,
                tables: Vec::new(),
                replica_path: None,
                app_id: "zero".to_string(),
                replica_version: "00".to_string(),
                shard: ShardID {
                    app_id: "zero".to_string(),
                    shard_num: 0,
                },
                cvr_pg: None,
                permissions: None,
                permissions_hash: None,
                revalidate_interval_ms: None,
                query_config: None,
                enable_query_covering: true,
                tokio_handle: self.handle.clone(),
                admin_password: None,
                server_version: "test".to_string(),
                metrics: std::sync::Arc::new(crate::metrics::Metrics::default()),
            }
        }
    }

    /// Regression (connect-ack decoupling, prod incident 2026-08-27): the
    /// `connected` message MUST be emitted on the accept task
    /// (`handle_connection`), NOT on the serial CG thread. When a client's CG
    /// thread is blocked in an in-flight `config_and_hydrate` (here simulated by
    /// a blocking `ConnectionContextManager::init_connection`), a SECOND client
    /// on the SAME group must still receive `connected` immediately — otherwise
    /// its 10s connect timeout fires, it disconnects, the idle CG is reaped, and
    /// the reconnect pays a full cold re-hydrate (the thrash we observed).
    ///
    /// NON-VACUOUS: before the fix, `connected` was sent by `Connection::init()`
    /// inside `on_new_connection` on the CG thread, so client B's ack was queued
    /// behind the blocked hydrate and this `try_recv` finds nothing → the assert
    /// fails. (Verified by reverting the `handle_connection` emission.)
    #[test]
    fn connected_ack_is_decoupled_from_a_blocked_cg_hydrate() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let entered = Arc::new(AtomicBool::new(false));
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let ccm = Arc::new(BlockingCcm {
            entered: entered.clone(),
            release: release.clone(),
            blocked_once: AtomicBool::new(false),
        });
        let factory: Arc<dyn CGServicesFactory> = Arc::new(BlockingCcmFactory {
            handle: rt.handle().clone(),
            ccm,
        });
        let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        });
        let router = Arc::new(ConnectionRouter::new_with_limit(
            factory,
            validator,
            Arc::new(crate::metrics::Metrics::default()),
            10,
        ));

        let make_ctx = |cid: &str, ws: &str, init: bool| {
            let mut params = test_params(cid, ws);
            params.client_group_id = "cgX".to_string();
            if init {
                params.init_connection_msg = Some(
                    serde_json::from_value(serde_json::json!([
                        "initConnection",
                        {"desiredQueriesPatch": []}
                    ]))
                    .unwrap(),
                );
            }
            let (up_tx, up_rx) = tokio::sync::mpsc::channel::<String>(8);
            let (sink_tx, sink_rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
            (
                ConnectionContext {
                    params,
                    sink: DirectWebSocketSink::new(sink_tx),
                    upstream_rx: up_rx,
                },
                up_tx,
                sink_rx,
            )
        };

        // Blocker A: its `initConnection` drives the CG thread into the blocking
        // `init_connection`, holding the thread like a long hydrate.
        let (ctx_a, _keep_a, _sink_a) = make_ctx("cA", "wsA", true);
        rt.block_on(router.handle_connection(ctx_a));

        // Wait until the CG thread is actually blocked.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !entered.load(Ordering::SeqCst) {
            assert!(
                std::time::Instant::now() < deadline,
                "blocker never reached the CG-thread init_connection"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        // Client B on the SAME group. Its `connected` must arrive from the accept
        // task even though the CG thread is blocked on A.
        let (ctx_b, _keep_b, mut sink_b) = make_ctx("cB", "wsB", false);
        rt.block_on(router.handle_connection(ctx_b));

        let mut saw_connected = false;
        while let Ok(cmd) = sink_b.try_recv() {
            if let WsCommand::Send { msg, .. } = cmd
                && msg.get(0).and_then(|v| v.as_str()) == Some("connected")
            {
                saw_connected = true;
            }
        }

        // Release the blocked CG thread FIRST so shutdown is clean regardless of
        // the assertion outcome.
        {
            let (m, cv) = &*release;
            *m.lock().unwrap() = true;
            cv.notify_all();
        }
        assert!(
            saw_connected,
            "client B must receive `connected` from the accept task while the CG \
             thread is mid-hydrate (connect-ack must not be serialized behind it)"
        );

        rt.block_on(router.shutdown());
    }

    /// `place_cg` chooses the executor hosting the FEWEST groups, ignoring the
    /// cg_id hash except to break ties among equally-loaded executors. Proves the
    /// least-loaded contract directly: a heavily-loaded executor is avoided even
    /// when the hash would otherwise select it.
    #[test]
    fn place_cg_picks_least_loaded_executor() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        });
        let router = ConnectionRouter::new_sharded(
            factory,
            validator,
            Arc::new(crate::metrics::Metrics::default()),
            100,
            3,
            None,
            ConnectionSinks::new(),
            ShardID {
                app_id: "zero".to_string(),
                shard_num: 0,
            },
        );

        // Fake handles (no real CG task) let us load specific executors without
        // spawning groups — place_cg only reads `executor_idx`, never the channel.
        let dummy = |executor_idx: usize| {
            let (tx, _rx) = mpsc::unbounded_channel::<CGMessage>();
            CGHandle {
                tx,
                connection_count: Arc::new(AtomicU64::new(1)),
                accepting: Arc::new(AtomicBool::new(true)),
                executor_idx,
            }
        };

        // Empty router: no load anywhere, so every executor ties and the pick is
        // the deterministic hash tie-break (spreads a cold system).
        assert_eq!(
            router.place_cg("some-cg"),
            shard_for("some-cg", 3),
            "on an empty router placement falls back to the hash tie-break"
        );

        // Load executor 0 with two groups and executor 1 with one; executor 2 is
        // empty, so it MUST be chosen regardless of the cg_id hash.
        router.cg_handles.insert("a".to_string(), dummy(0));
        router.cg_handles.insert("b".to_string(), dummy(0));
        router.cg_handles.insert("c".to_string(), dummy(1));
        for cg in ["x", "y", "z", "hash-would-pick-0", "another"] {
            assert_eq!(
                router.place_cg(cg),
                2,
                "least-loaded executor (2) must win for {cg} despite the hash"
            );
        }

        rt.block_on(router.shutdown());
    }

    /// End-to-end, placing many real groups spreads them evenly across executors:
    /// because placement is serialized and each placed group is registered before
    /// the next placement, least-loaded degenerates to round-robin and keeps the
    /// per-executor group counts within 1 of each other.
    #[test]
    fn placement_balances_groups_across_executors() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        });
        let k = 4;
        let n = 40;
        let router = ConnectionRouter::new_sharded(
            factory,
            validator,
            Arc::new(crate::metrics::Metrics::default()),
            200,
            k,
            None,
            ConnectionSinks::new(),
            ShardID {
                app_id: "zero".to_string(),
                shard_num: 0,
            },
        );

        for i in 0..n {
            router.get_or_create_cg(&format!("cg{i}")).unwrap();
        }

        let mut counts = vec![0usize; k];
        for entry in router.cg_handles.iter() {
            counts[entry.executor_idx] += 1;
        }
        let max = *counts.iter().max().unwrap();
        let min = *counts.iter().min().unwrap();
        assert!(
            max - min <= 1,
            "expected round-robin-balanced placement, got {counts:?}"
        );
        assert_eq!(counts.iter().sum::<usize>(), n, "every group placed once");

        rt.block_on(router.shutdown());
    }

    /// A piggybacked `initConnection` fires the ConnectionContextManager init
    /// side effect through the CG-thread path (task 12) — previously dropped
    /// because the router intercepts `initConnection` before the message handler.
    #[test]
    fn init_connection_fires_ccm_init_side_effect() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let init_calls = Arc::new(AtomicU64::new(0));
        let factory: Arc<dyn CGServicesFactory> = Arc::new(CountingCcmFactory {
            handle: rt.handle().clone(),
            init_calls: init_calls.clone(),
        });
        let global = Arc::new(Mutex::new(HashMap::new()));
        let count = Arc::new(AtomicU64::new(0));
        let mut state = CgState::new(
            "cg1",
            &factory,
            Arc::new(crate::auth::jwt::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            global,
            count,
        );

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let sink = DirectWebSocketSink::new(tx);
        seed_test_client_schema(&mut state);
        let mut params = test_params("c1", "ws1");
        // Piggyback an initConnection carrying an empty desired-queries patch.
        params.init_connection_msg = Some(
            serde_json::from_value(serde_json::json!([
                "initConnection",
                {"desiredQueriesPatch": []}
            ]))
            .unwrap(),
        );
        rt.block_on(state.on_new_connection(params, sink));

        assert_eq!(
            init_calls.load(Ordering::SeqCst),
            1,
            "ccm.init_connection should fire once on initConnection"
        );
        // The initConnection hydrated the client's (internal) queries, so the
        // hot-path `hydrations` metric incremented.
        assert_eq!(state.metrics.snapshot()["hydrations"], 1);
    }

    #[test]
    fn new_client_group_rejects_init_without_client_schema() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let mut state = CgState::new(
            "cg1",
            &factory,
            Arc::new(crate::auth::jwt::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicU64::new(1)),
        );
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(
            state.on_new_connection(test_params("c1", "ws1"), DirectWebSocketSink::new(tx)),
        );
        rt.block_on(state.handle_desired_queries(
            "c1",
            &serde_json::json!({"desiredQueriesPatch": []}),
            true,
        ));

        let error = std::iter::from_fn(|| rx.try_recv().ok()).find_map(|command| match command {
            WsCommand::Send { msg: value, .. }
                if value.get(0).and_then(serde_json::Value::as_str) == Some("error") =>
            {
                value.get(1).cloned()
            }
            _ => None,
        });
        let error = error.expect("missing schema must close with a protocol error");
        assert_eq!(error["kind"], "InvalidConnectionRequest");
        assert!(
            error["message"]
                .as_str()
                .is_some_and(|message| message.contains("must include client schema"))
        );
    }

    /// G36 garbage-cookie / overlarge-configversion: a baseCookie that fails
    /// `versionFromString` (TS schema/types.ts, called from the ClientHandler
    /// constructor's `cookieToVersion`) FAILS the connection with a fatal
    /// `Internal` error (`wrapWithProtocolError`, types/error-with-level.ts) —
    /// it must NOT be silently treated as "no base version". Covers both G36
    /// shapes: a non-Lexi stateVersion and a configVersion above 2^53.
    ///
    /// The `connected`-before-`error` ordering TS guarantees is now structural:
    /// `handle_connection` (accept task) sends `connected` BEFORE dispatching
    /// `NewConnection` to the CG thread, and this `Internal` error is emitted
    /// later on the CG thread by `on_new_connection`. This unit test drives
    /// `on_new_connection` directly, so it asserts only the CG-thread half (the
    /// `Internal` close); the ordering is covered by `handle_connection`'s
    /// accept-task emission.
    #[test]
    fn malformed_base_cookie_closes_with_internal_error() {
        for bad_cookie in ["!!notlexi!!", "00:b100000000000"] {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
                handle: rt.handle().clone(),
            });
            let mut state = CgState::new(
                "cg1",
                &factory,
                Arc::new(crate::auth::jwt::JwtAuthValidator {
                    jwk: None,
                    secret: None,
                    jwks_url: None,
                    issuer: None,
                    audience: None,
                }),
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(AtomicU64::new(1)),
            );
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
            let mut params = test_params("c1", "ws1");
            params.base_cookie = Some(bad_cookie.to_string());
            rt.block_on(state.on_new_connection(params, DirectWebSocketSink::new(tx)));

            let mut saw_connected = false;
            let mut error = None;
            while let Ok(command) = rx.try_recv() {
                if let WsCommand::Send { msg: value, .. } = command {
                    match value.get(0).and_then(serde_json::Value::as_str) {
                        Some("connected") => saw_connected = true,
                        Some("error") => error = value.get(1).cloned(),
                        _ => {}
                    }
                }
            }
            // `connected` is emitted on the accept task, NOT on this CG-thread
            // path, so the CG thread must not send it here.
            assert!(
                !saw_connected,
                "[{bad_cookie}] on_new_connection must not send `connected` (accept task does)"
            );
            let error =
                error.unwrap_or_else(|| panic!("[{bad_cookie}] must close with a protocol error"));
            assert_eq!(error["kind"], "Internal", "[{bad_cookie}] {error}");
            assert!(
                !state.connections.contains_key("c1"),
                "[{bad_cookie}] connection must be torn down"
            );
        }
    }

    /// The inspector protocol gates every op behind an `authenticate` that
    /// matches the configured admin password; `version` then returns the
    /// configured server version.
    #[test]
    fn inspect_auth_gate_then_version() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let global = Arc::new(Mutex::new(HashMap::new()));
        let count = Arc::new(AtomicU64::new(0));
        let mut state = CgState::new(
            "cg1",
            &factory,
            Arc::new(crate::auth::jwt::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            global,
            count,
        );
        // Configure an admin password for the inspector.
        state.admin_password = Some("s3cret".to_string());
        state.server_version = "9.9.9".to_string();

        let (tx, mut drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let sink = DirectWebSocketSink::new(tx);
        rt.block_on(state.on_new_connection(test_params("c1", "ws1"), sink));

        let drain =
            |drx: &mut tokio::sync::mpsc::UnboundedReceiver<WsCommand>| -> Vec<serde_json::Value> {
                let mut v = Vec::new();
                while let Ok(WsCommand::Send { msg: m, .. }) = drx.try_recv() {
                    v.push(m);
                }
                v
            };
        let _ = drain(&mut drx); // discard the `connected` frame

        // 1) `version` before authenticating → challenge (authenticated:false).
        rt.block_on(state.on_inbound(
            "c1".into(),
            "ws1".into(),
            r#"["inspect",{"op":"version","id":"1"}]"#.to_string(),
        ));
        let frames = drain(&mut drx);
        let last = frames.last().unwrap();
        assert_eq!(last[0], "inspect");
        assert_eq!(last[1]["op"], "authenticated");
        assert_eq!(last[1]["value"], false);

        // 2) authenticate with the wrong password → false.
        rt.block_on(state.on_inbound(
            "c1".into(),
            "ws1".into(),
            r#"["inspect",{"op":"authenticate","id":"2","value":"nope"}]"#.to_string(),
        ));
        assert_eq!(drain(&mut drx).last().unwrap()[1]["value"], false);
        assert!(!state.inspector_authenticated);

        // 3) authenticate with the right password → true.
        rt.block_on(state.on_inbound(
            "c1".into(),
            "ws1".into(),
            r#"["inspect",{"op":"authenticate","id":"3","value":"s3cret"}]"#.to_string(),
        ));
        assert_eq!(drain(&mut drx).last().unwrap()[1]["value"], true);
        assert!(state.inspector_authenticated);

        // 4) `version` now returns the configured server version.
        rt.block_on(state.on_inbound(
            "c1".into(),
            "ws1".into(),
            r#"["inspect",{"op":"version","id":"4"}]"#.to_string(),
        ));
        let last = drain(&mut drx).into_iter().next_back().unwrap();
        assert_eq!(last[1]["op"], "version");
        assert_eq!(last[1]["value"], "9.9.9");
    }

    /// A CgState pre-authenticated to the inspector, with a live connection.
    /// Returns the state, runtime, and the sink's receive channel.
    fn inspect_test_state() -> (
        CgState,
        tokio::runtime::Runtime,
        tokio::sync::mpsc::UnboundedReceiver<WsCommand>,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let mut state = CgState::new(
            "cg1",
            &factory,
            Arc::new(crate::auth::jwt::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicU64::new(0)),
        );
        state.admin_password = Some("s3cret".to_string());
        state.inspector_authenticated = true;
        let (tx, drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let sink = DirectWebSocketSink::new(tx);
        rt.block_on(state.on_new_connection(test_params("c1", "ws1"), sink));
        (state, rt, drx)
    }

    fn last_inspect_frame(
        drx: &mut tokio::sync::mpsc::UnboundedReceiver<WsCommand>,
    ) -> serde_json::Value {
        let mut last = serde_json::Value::Null;
        while let Ok(WsCommand::Send { msg: m, .. }) = drx.try_recv() {
            last = m;
        }
        last
    }

    // NOTE: the `queries` inspector rows are now produced by the SQL port
    // `CVRStore::inspect_queries` (rust-cvr) — router::inspect_queries_value just
    // delegates + adds `metrics: null`. The row-shape / TTL-filter / got-flag /
    // rowCount / client-filter coverage lives in rust-cvr tests/inspect_pg_test.rs
    // (PG-gated), against the real desires/queries/rows tables.

    #[test]
    fn inspect_metrics_is_a_record_of_tdigests() {
        let (mut state, rt, mut drx) = inspect_test_state();
        rt.block_on(state.on_inbound(
            "c1".into(),
            "ws1".into(),
            r#"["inspect",{"op":"metrics","id":"m1"}]"#.to_string(),
        ));
        let frame = last_inspect_frame(&mut drx);
        assert_eq!(frame[0], "inspect");
        assert_eq!(frame[1]["op"], "metrics");
        assert_eq!(frame[1]["id"], "m1");
        let value = &frame[1]["value"];
        assert!(
            value.is_object(),
            "metrics value must be a record, not an array"
        );
        // Both fields REQUIRED by serverMetricsSchema, each a TDigest JSON
        // (non-empty number array; [compression] for an empty digest).
        for key in ["query-materialization-server", "query-update-server"] {
            let digest = value[key].as_array().unwrap();
            assert!(!digest.is_empty());
            assert!(digest[0].is_number());
        }
    }

    #[test]
    fn inspect_unsupported_and_unknown_ops_answer_with_error_op() {
        let (mut state, rt, mut drx) = inspect_test_state();

        // analyze-query is not ported → `{op:"error"}`, not a success frame
        // carrying an `{error}` payload.
        rt.block_on(state.on_inbound(
            "c1".into(),
            "ws1".into(),
            r#"["inspect",{"op":"analyze-query","id":"a1"}]"#.to_string(),
        ));
        let frame = last_inspect_frame(&mut drx);
        assert_eq!(frame[1]["op"], "error");
        assert_eq!(frame[1]["id"], "a1");
        assert!(
            frame[1]["value"]
                .as_str()
                .unwrap()
                .contains("not supported"),
            "error value must be a string message"
        );

        // Unknown op (TS `unreachable` throw → catch) → error op, not silence.
        // Driven through handle_inspect directly: protocol validation upstream
        // (parse_upstream_array, mirroring the TS valita layer) rejects unknown
        // ops before dispatch, so this covers the defensive arm.
        rt.block_on(state.handle_inspect("c1", &serde_json::json!({"op": "bogus", "id": "b1"})));
        let frame = last_inspect_frame(&mut drx);
        assert_eq!(frame[1]["op"], "error");
        assert_eq!(frame[1]["id"], "b1");
        assert!(frame[1]["value"].is_string());
    }

    // ─── CgState mock-factory harness ───────────────────────────────────────
    //
    // Drives CgState (the fused port of TS view-syncer.ts + syncer-ws-message-
    // handler.ts dispatch) directly on the test thread, with mock dispatch
    // services (Noop* above), an in-memory replica carrying a real `issue`
    // table spec, and a channel-backed DirectWebSocketSink standing in for the
    // client socket. Models the TS view-syncer.pg.test.ts `connect()` +
    // `nextPoke()` pattern.

    fn issue_table_spec() -> crate::services::view_syncer::pipeline_driver::IvmTableSpec {
        use crate::services::view_syncer::pipeline_driver::{IvmColumnSchema, IvmTableSpec};
        IvmTableSpec {
            table: "issue".to_string(),
            columns: HashMap::from([
                (
                    "id".to_string(),
                    IvmColumnSchema {
                        r#type: "string".to_string(),
                        optional: false,
                    },
                ),
                (
                    "title".to_string(),
                    IvmColumnSchema {
                        r#type: "string".to_string(),
                        optional: true,
                    },
                ),
            ]),
            primary_key: vec!["id".to_string()],
            unique_keys: None,
            min_row_version: None,
        }
    }

    /// Factory whose in-memory engine has a REAL `issue` table spec, so
    /// desired-query puts against it hydrate instead of failing the group.
    struct TablesFactory {
        handle: tokio::runtime::Handle,
    }
    impl CGServicesFactory for TablesFactory {
        fn create_view_syncer(&self, _cg: &str) -> Arc<dyn ViewSyncerDispatch> {
            Arc::new(NoopViewSyncer)
        }
        fn create_conn_context_manager(&self, _cg: &str) -> Arc<dyn ConnContextManagerDispatch> {
            Arc::new(NoopCcm)
        }
        fn create_mutagen(&self, _cg: &str) -> Option<Arc<dyn MutagenDispatch>> {
            None
        }
        fn create_pusher(&self, _cg: &str) -> Option<Arc<dyn PusherDispatch>> {
            None
        }
        fn create_sync_engine_config(&self, _cg: &str) -> SyncEngineConfig {
            SyncEngineConfig {
                initialization_error: None,
                tables: vec![issue_table_spec()],
                replica_path: None, // in-memory sources
                app_id: "zero".to_string(),
                replica_version: "00".to_string(),
                shard: ShardID {
                    app_id: "zero".to_string(),
                    shard_num: 0,
                },
                cvr_pg: None,
                permissions: None,
                permissions_hash: None,
                revalidate_interval_ms: None,
                query_config: None,
                enable_query_covering: true,
                tokio_handle: self.handle.clone(),
                admin_password: None,
                server_version: "test".to_string(),
                metrics: std::sync::Arc::new(crate::metrics::Metrics::default()),
            }
        }
    }

    /// A CgState over the `issue`-table factory, plus its runtime.
    fn tables_state(rt: &tokio::runtime::Runtime) -> CgState {
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TablesFactory {
            handle: rt.handle().clone(),
        });
        CgState::new(
            "cg1",
            &factory,
            Arc::new(crate::auth::jwt::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicU64::new(1)),
        )
    }

    /// Drain every queued `Send` frame off the sink channel.
    fn drain_sends(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<WsCommand>,
    ) -> Vec<serde_json::Value> {
        std::iter::from_fn(|| rx.try_recv().ok())
            .filter_map(|command| match command {
                WsCommand::Send { msg, .. } => Some(msg),
                _ => None,
            })
            .collect()
    }

    /// Connect `c1`/`ws1` on the CG-thread path and drain any queued frames so
    /// the returned rx starts clean for the caller's own assertions.
    ///
    /// Note: `on_new_connection` no longer emits `connected` (that is the accept
    /// task's job — see `handle_connection`), so this helper does not expect it.
    fn connect_c1(
        rt: &tokio::runtime::Runtime,
        state: &mut CgState,
    ) -> tokio::sync::mpsc::UnboundedReceiver<WsCommand> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(
            state.on_new_connection(test_params("c1", "ws1"), DirectWebSocketSink::new(tx)),
        );
        let frames = drain_sends(&mut rx);
        assert!(
            !frames.iter().any(|f| f[0] == "connected"),
            "on_new_connection (CG thread) must not emit `connected`; the accept task does"
        );
        rx
    }

    const INIT_CONNECTION_HASH1: &str = r#"["initConnection",{"clientSchema":{"tables":{}},"desiredQueriesPatch":[{"op":"put","hash":"query-hash1","ast":{"table":"issue"}}]}]"#;

    /// Port of TS connection.ts `#handleMessage` ping fast-path, driven through
    /// the CG dispatch (`on_inbound` → Connection): `["ping",{}]` answers
    /// exactly `["pong",{}]` and nothing else.
    #[test]
    fn on_inbound_ping_answers_pong() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut state = tables_state(&rt);
        let mut rx = connect_c1(&rt, &mut state);

        rt.block_on(state.on_inbound("c1".into(), "ws1".into(), r#"["ping",{}]"#.to_string()));
        let frames = drain_sends(&mut rx);
        assert_eq!(frames, vec![serde_json::json!(["pong", {}])]);
        assert!(
            state.connections.contains_key("c1"),
            "ping must not close the connection"
        );
    }

    /// Port of TS connection.ts `#handleMessage` parse/valita catch: malformed
    /// JSON and an unknown message tag both fail `upstreamSchema` → the exact
    /// InvalidMessage error frame, then the connection is torn down.
    #[test]
    fn on_inbound_malformed_message_closes_with_invalid_message() {
        for bad in ["{not json", r#"["definitelyNotAThing",{}]"#] {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let mut state = tables_state(&rt);
            let mut rx = connect_c1(&rt, &mut state);

            rt.block_on(state.on_inbound("c1".into(), "ws1".into(), bad.to_string()));
            let frames = drain_sends(&mut rx);
            let error = frames
                .iter()
                .find(|f| f[0] == "error")
                .unwrap_or_else(|| panic!("[{bad}] expected an error frame"));
            assert_eq!(error[1]["kind"], "InvalidMessage", "[{bad}]");
            assert!(
                !state.connections.contains_key("c1"),
                "[{bad}] the connection must be closed"
            );
            assert!(
                !state.registered_ws.contains_key("c1"),
                "[{bad}] the client must be unregistered"
            );
        }
    }

    /// Port of TS view-syncer.pg.test.ts "initial hydration" (first poke):
    /// an initConnection with a put patch pokes the desired-queries config —
    /// `pokeStart {pokeID:"00:01", baseCookie:null}` → `pokePart` whose
    /// `desiredQueriesPatches` carries the client's `{op:"put",
    /// hash:"query-hash1"}` → `pokeEnd {cookie:"00:01"}` — and records the
    /// client + the internal `lmids` query in the CVR (TS EXPECTED_LMIDS_AST).
    #[test]
    fn init_connection_pokes_desired_queries_patch() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut state = tables_state(&rt);
        let mut rx = connect_c1(&rt, &mut state);

        rt.block_on(state.on_inbound("c1".into(), "ws1".into(), INIT_CONNECTION_HASH1.to_string()));
        let frames = drain_sends(&mut rx);

        let poke_start = frames
            .iter()
            .find(|f| f[0] == "pokeStart")
            .expect("expected a pokeStart");
        assert_eq!(poke_start[1]["pokeID"], "00:01");
        assert!(
            poke_start[1]["baseCookie"].is_null(),
            "first poke must be from a null baseCookie: {poke_start}"
        );

        let desired = frames
            .iter()
            .filter(|f| f[0] == "pokePart")
            .find_map(|f| f[1].get("desiredQueriesPatches").cloned())
            .expect("expected a pokePart with desiredQueriesPatches");
        let c1_patch = desired["c1"]
            .as_array()
            .expect("desiredQueriesPatches keyed by clientID");
        assert!(
            c1_patch
                .iter()
                .any(|op| op["op"] == "put" && op["hash"] == "query-hash1"),
            "expected the put for query-hash1, got {c1_patch:?}"
        );

        let poke_end = frames
            .iter()
            .find(|f| f[0] == "pokeEnd")
            .expect("expected a pokeEnd");
        assert_eq!(poke_end[1]["cookie"], "00:01");

        // The hydrate pass follows as a SECOND poke: with no replica advance the
        // stateVersion is unchanged ("00"), so the got-queries update bumps the
        // minor (config) version — TS `CVRQueryDrivenUpdater.trackQueries`
        // bumps the minor version when hydrating at an unchanged stateVersion
        // (in TS's pg test the same got patch instead rides stateVersion "01"
        // after `version-ready`).
        let got = frames
            .iter()
            .filter(|f| f[0] == "pokePart")
            .find_map(|f| f[1].get("gotQueriesPatch").cloned())
            .expect("expected a pokePart with gotQueriesPatch");
        assert!(
            got.as_array()
                .unwrap()
                .iter()
                .any(|op| op["op"] == "put" && op["hash"] == "query-hash1"),
            "expected got put for query-hash1, got {got:?}"
        );

        // CVR state after config + hydrate (TS "responds to changeDesiredQueries
        // patch" asserts the same records via CVRStore.load).
        let cvr = state.cvr.as_ref().expect("CVR loaded");
        assert_eq!(
            cvr.clients.get("c1").map(|c| c.desired_query_ids.clone()),
            Some(vec!["query-hash1".to_string()])
        );
        assert!(
            cvr.queries.contains_key("lmids"),
            "the internal lmids query must be recorded"
        );
        assert_eq!(cvr.version.state_version, "00");
        // 1 = the desired-queries config bump; 2 = the same-state hydrate bump.
        assert_eq!(cvr.version.config_version, Some(2));
    }

    /// Port of TS view-syncer.pg.test.ts "responds to changeDesiredQueries
    /// patch": a `changeDesiredQueries` with `[put query-hash2, del
    /// query-hash1]` bumps the config version to 2, pokes both ops to the
    /// client, leaves `desiredQueryIDs = [query-hash2]`, and keeps the deleted
    /// query-hash1 record with the client's state INACTIVATED (TTL grace), not
    /// erased.
    #[test]
    fn change_desired_queries_pokes_put_and_del_and_updates_cvr() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut state = tables_state(&rt);
        let mut rx = connect_c1(&rt, &mut state);
        rt.block_on(state.on_inbound("c1".into(), "ws1".into(), INIT_CONNECTION_HASH1.to_string()));
        let _ = drain_sends(&mut rx);

        rt.block_on(state.on_inbound(
            "c1".into(),
            "ws1".into(),
            r#"["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"put","hash":"query-hash2","ast":{"table":"issue"}},{"op":"del","hash":"query-hash1"}]}]"#
                .to_string(),
        ));
        let frames = drain_sends(&mut rx);

        // After the init's two version bumps (config "00:01" + hydrate "00:02")
        // this change's config poke is "00:03".
        let poke_start = frames
            .iter()
            .find(|f| f[0] == "pokeStart")
            .expect("expected a pokeStart");
        assert_eq!(poke_start[1]["pokeID"], "00:03");
        let c1_ops: Vec<serde_json::Value> = frames
            .iter()
            .filter(|f| f[0] == "pokePart")
            .filter_map(|f| f[1]["desiredQueriesPatches"]["c1"].as_array().cloned())
            .flatten()
            .collect();
        assert!(
            c1_ops
                .iter()
                .any(|op| op["op"] == "put" && op["hash"] == "query-hash2"),
            "expected the put for query-hash2, got {c1_ops:?}"
        );
        assert!(
            c1_ops
                .iter()
                .any(|op| op["op"] == "del" && op["hash"] == "query-hash1"),
            "expected the del for query-hash1, got {c1_ops:?}"
        );
        let poke_end = frames
            .iter()
            .find(|f| f[0] == "pokeEnd")
            .expect("expected a pokeEnd");
        assert_eq!(poke_end[1]["cookie"], "00:03");

        let cvr = state.cvr.as_ref().expect("CVR loaded");
        // "00:03" = the put/del config bump; "00:04" = the same-state hydrate
        // bump for the newly-got query-hash2 (see the init test).
        assert_eq!(cvr.version.config_version, Some(4));
        assert_eq!(
            cvr.clients.get("c1").map(|c| c.desired_query_ids.clone()),
            Some(vec!["query-hash2".to_string()])
        );
        // TS keeps the deleted query record with `clientState.foo.inactivatedAt`
        // set (the TTL grace window), rather than deleting it outright.
        let hash1 = cvr
            .queries
            .get("query-hash1")
            .expect("query-hash1 must survive the del (inactivated, not erased)");
        assert!(
            hash1
                .client_state()
                .and_then(|cs| cs.get("c1"))
                .is_some_and(|cs| cs.inactivated_at.is_some()),
            "query-hash1 must be inactivated for c1"
        );
    }

    /// Port of TS view-syncer.pg.test.ts "responds to changeDesiredQueries
    /// patch" (the old-wsid arm): a changeDesiredQueries arriving on a stale
    /// wsID is IGNORED — no poke, no CVR change.
    #[test]
    fn change_desired_queries_from_stale_ws_is_ignored() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut state = tables_state(&rt);
        let mut rx = connect_c1(&rt, &mut state);
        rt.block_on(state.on_inbound("c1".into(), "ws1".into(), INIT_CONNECTION_HASH1.to_string()));
        let _ = drain_sends(&mut rx);

        rt.block_on(state.on_inbound(
            "c1".into(),
            "old-wsid".into(),
            r#"["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"put","hash":"query-hash-1234567890","ast":{"table":"issue"}}]}]"#
                .to_string(),
        ));
        assert!(
            drain_sends(&mut rx).is_empty(),
            "a stale-wsID frame must produce no output"
        );
        let cvr = state.cvr.as_ref().unwrap();
        assert_eq!(
            cvr.clients.get("c1").map(|c| c.desired_query_ids.clone()),
            Some(vec!["query-hash1".to_string()]),
            "the stale frame must not change the desired set"
        );
        // Still at the init's config+hydrate version (see the init test): the
        // stale frame must not bump it further.
        assert_eq!(cvr.version.config_version, Some(2));
    }

    // The protocol-version gate is TS `Connection.init()` (connection.ts); its
    // exact `VersionNotSupported` message is pinned 1:1 by connection.rs
    // `init_out_of_range_closes_with_exact_version_not_supported_message`. The
    // prod gate is applied on the accept path (`ws_server::accept_connection`)
    // with the byte-identical message, since Rust builds `Connection` on the CG
    // thread and `on_new_connection` never sees an unvalidated version.

    /// Port of `pickToken`'s pinned-user rule through the WIRE dispatch
    /// (`["updateAuth", …]` → `handle_update_auth`): a validly-formed token for
    /// a different user gets the exact Unauthorized error body and the
    /// connection is closed.
    #[test]
    fn update_auth_cross_user_via_wire_gets_exact_unauthorized_error() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));
        let _ = drain_sends(&mut rx);

        rt.block_on(state.on_inbound(
            "c1".into(),
            "ws1".into(),
            format!(r#"["updateAuth",{{"auth":"{}"}}]"#, fake_jwt("user-2")),
        ));
        let frames = drain_sends(&mut rx);
        let error = frames
            .iter()
            .find(|f| f[0] == "error")
            .expect("cross-user updateAuth must error");
        assert_eq!(error[1]["kind"], "Unauthorized");
        assert_eq!(
            error[1]["message"],
            "The user id in the new token does not match the previous token. \
             Client groups are pinned to a single user."
        );
        assert!(state.registered_ws.is_empty(), "connection must be closed");
    }

    /// TS `updateAuth` with an empty/absent token is a no-op: no error, no
    /// re-transform, connection stays registered.
    #[test]
    fn update_auth_empty_token_is_a_noop() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));
        let _ = drain_sends(&mut rx);

        rt.block_on(state.on_inbound(
            "c1".into(),
            "ws1".into(),
            r#"["updateAuth",{"auth":""}]"#.to_string(),
        ));
        assert!(drain_sends(&mut rx).is_empty(), "no output for empty auth");
        assert_eq!(state.registered_ws.len(), 1, "connection must survive");
        assert_eq!(state.metrics.snapshot()["authChanges"], 0);
    }
}
