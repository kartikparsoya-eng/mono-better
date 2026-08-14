//! Connection router — port of the connection lifecycle in `syncer.ts`.
//!
//! Routes incoming WebSocket connections to the appropriate CG (client group)
//! thread. Each CG gets a dedicated OS thread. The router maintains a
//! `DashMap<client_group_id, CGHandle>` for lookup.
//!
//! Connection lifecycle (port of `Syncer.#createConnection`):
//! 1. Auth validation (JWT) — BEFORE touching existing connections
//! 2. User ID pinning check — reject if group is pinned to a different user
//! 3. Close existing connection for same clientID (replacement)
//! 4. Register connection in context manager
//! 5. Create Connection + MessageHandler
//! 6. Call `connection.init()` (send `connected` message)
//! 7. Handle piggybacked `initConnection` from sec-websocket-protocol header

use crate::connect_params::ConnectParams;
use crate::connection::Connection;
use crate::connection_context::FetchConfig;
use crate::custom_query::CustomQueryContext;
use crate::message_handler::{
    ConnContextManagerDispatch, ConnectionSelector, MutagenDispatch, PusherDispatch,
    SyncerWsMessageHandler, ViewSyncerDispatch,
};
use crate::pipeline_driver::{IvmPipelines, IvmTableSpec};
use crate::sync_engine::{SyncEngine, empty_cvr};
use crate::ws_server::ConnectionContext;
use crate::ws_sink::DirectWebSocketSink;
use dashmap::DashMap;
use rust_cvr::types::{CVR, DesiredQuerySpec, ShardID, TTLClock};
use rust_cvr::version::{
    CVRVersion, EMPTY_CVR_VERSION, NullableCVRVersion, cmp_versions, version_from_string,
    version_string,
};
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
    Inbound {
        client_id: String,
        ws_id: String,
        text: String,
    },
    /// A connection's WS closed (its upstream channel ended).
    ConnectionClosed { client_id: String, ws_id: String },
    /// Explicitly close a superseded socket before installing its replacement.
    CloseConnection { client_id: String, ws_id: String },
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
/// binds to is supplied by the executor hosting the client group, NOT by the
/// factory. Each of the `K` executors owns its own pool, created on and driven
/// by that executor's `current_thread` runtime and sized `cvr_max_conns / K`, so
/// every connection is polled by the same reactor that `.await`s it (doc 91,
/// §5.1 — a pool built on a different runtime starves current-thread executors).
/// The `K × maxConns/K` split keeps the process-wide connection budget bounded,
/// matching TS's one-`cvrDB`-pool-per-sync-worker model (`server/syncer.ts`).
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
    /// Whether the router is shutting down.
    shutting_down: Arc<AtomicBool>,
}

/// Info about an active connection.
#[derive(Clone)]
struct ConnectionInfo {
    client_group_id: String,
    ws_id: String,
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
        )
    }

    /// Full constructor: spawn `num_shards` executor threads, each running a
    /// `current_thread` runtime + `LocalSet` hosting a hash-shard of client
    /// groups (doc 91). `cvr_pool` is the ONE shared CVR `PgPool` (built on the
    /// process's main runtime); a clone is handed to every executor so groups
    /// draw from a single bounded connection budget, and CVR I/O is offloaded
    /// back onto that pool's runtime (`SyncEngine::offload`). `None` selects
    /// storeless CGs (tests / no-PG dev).
    pub fn new_sharded(
        services_factory: Arc<dyn CGServicesFactory>,
        auth_validator: Arc<dyn AuthValidator>,
        metrics: Arc<crate::metrics::Metrics>,
        max_client_groups: usize,
        num_shards: usize,
        cvr_pool: Option<sqlx::PgPool>,
    ) -> Self {
        let num_shards = num_shards.max(1);
        let cg_handles: Arc<DashMap<String, CGHandle>> = Arc::new(DashMap::new());
        let connections = Arc::new(Mutex::new(HashMap::new()));

        let mut executors = Vec::with_capacity(num_shards);
        for idx in 0..num_shards {
            let (ctrl_tx, ctrl_rx) = mpsc::unbounded_channel::<ExecutorCommand>();
            let factory = services_factory.clone();
            let validator = auth_validator.clone();
            let conns = connections.clone();
            let handles = cg_handles.clone();
            let pool = cvr_pool.clone();
            let join = std::thread::Builder::new()
                .name(format!("cg-exec-{idx}"))
                .spawn(move || {
                    run_executor(idx, ctrl_rx, factory, validator, conns, handles, pool);
                })
                .expect("failed to spawn CG executor thread");
            executors.push(Executor {
                ctrl_tx,
                join: Mutex::new(Some(join)),
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
            shutting_down: Arc::new(AtomicBool::new(false)),
        }
    }

    /// A JSON snapshot of the process metrics (for `/statz`).
    pub fn metrics_snapshot(&self) -> serde_json::Value {
        self.metrics.snapshot()
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
                },
            );
            existing
        };
        if let Some(existing) = superseded
            && let Some(handle) = self.cg_handles.get(&existing.client_group_id)
        {
            let _ = handle.send(CGMessage::CloseConnection {
                client_id: client_id.clone(),
                ws_id: existing.ws_id,
            });
        }

        // 5. Split the context: the CG thread owns connection setup + the sink,
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
                    client_id,
                    ws_id,
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
                    self.group_auth_states
                        .lock()
                        .unwrap()
                        .remove(&client_group_id);
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

        let shard = self.place_cg(client_group_id);
        let spawn = ExecutorCommand::SpawnCg {
            cg_id: client_group_id.to_string(),
            rx,
            self_tx: tx.clone(),
            connection_count: connection_count.clone(),
            accepting: accepting.clone(),
        };
        if self.executors[shard].ctrl_tx.send(spawn).is_err() {
            return Err(format!(
                "executor {shard} is not accepting new client groups (shutting down)"
            ));
        }

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
        let min = load.iter().copied().min().unwrap_or(0);
        // Deterministically break ties AMONG the least-loaded executors by hashing
        // the cg_id, so a cold/uniform system still spreads groups (rather than
        // always piling the first ones onto executor 0).
        let candidates: Vec<usize> = (0..k).filter(|&i| load[i] == min).collect();
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
    client_id: String,
    ws_id: String,
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
fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
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

/// Build the server-configured query context established during connection
/// registration. This mirrors TS `ConnectionContextManager#getContext('query')`:
/// the first configured URL is the default and server-controlled forwarding
/// headers are present before any `initConnection` override is applied.
fn default_query_context(
    config: Option<&FetchConfig>,
    params: &ConnectParams,
) -> Option<CustomQueryContext> {
    let config = config?;
    let url = config.url.as_ref()?.first()?.clone();
    let mut headers = Vec::new();
    if let Some(api_key) = config.api_key.as_ref().filter(|value| !value.is_empty()) {
        headers.push(("X-Api-Key".to_string(), api_key.clone()));
    }
    if config.forward_cookies
        && let Some(cookie) = params.http_cookie.as_ref()
    {
        headers.push(("Cookie".to_string(), cookie.clone()));
    }
    if let Some(origin) = params.origin.as_ref() {
        headers.push(("Origin".to_string(), origin.clone()));
    }
    Some(CustomQueryContext {
        url,
        headers,
        auth: params.auth.clone().filter(|value| !value.is_empty()),
    })
}

fn query_url_is_allowed(config: Option<&FetchConfig>, url: &str) -> bool {
    config
        .and_then(|config| config.url.as_ref())
        .is_some_and(|urls| urls.iter().any(|allowed| allowed == url))
}

fn filtered_query_headers(
    config: Option<&FetchConfig>,
    body: &serde_json::Value,
) -> Vec<(String, String)> {
    let Some(allowed) = config.and_then(|config| config.allowed_client_headers.as_ref()) else {
        return Vec::new();
    };
    let allowed: HashSet<String> = allowed
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect();
    body.get("userQueryHeaders")
        .and_then(|value| value.as_object())
        .into_iter()
        .flat_map(|headers| headers.iter())
        .filter_map(|(name, value)| {
            (allowed.contains(&name.to_ascii_lowercase()))
                .then(|| {
                    value
                        .as_str()
                        .map(|value| (name.clone(), value.to_string()))
                })
                .flatten()
        })
        .collect()
}

/// Per-CG state, owned by (and confined to) the CG thread. Holds the `!Send`
/// [`SyncEngine`] plus the live connections. Extracted from the event loop so
/// the message handlers are unit-testable.
struct CgState {
    cg_id: String,
    sync_engine: SyncEngine,
    view_syncer: Arc<dyn ViewSyncerDispatch>,
    conn_context_manager: Arc<dyn ConnContextManagerDispatch>,
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
    /// Server default and allow-list for custom query transformation.
    query_config: Option<FetchConfig>,
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
    /// Monotonic TTL clock (ms), seeded from `cvr.ttl_clock` when the CVR is
    /// loaded and advanced by wall-time delta while this CG runs — so a long
    /// downtime does not mass-expire queries. Port of TS `#ttlClock`.
    ttl_clock: TTLClock,
    /// Wall-clock (ms) at the last `get_ttl_clock`. Port of TS `#ttlClockBase`.
    ttl_clock_base: i64,
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
    /// client_id → decoded JWT claims (`authData` for permission rules).
    client_auth: HashMap<String, serde_json::Value>,
    /// client_id → raw JWT (for the `Authorization: Bearer` header on
    /// custom-query transform requests).
    client_raw_auth: HashMap<String, String>,
    /// client_id → the custom-query API context (`userQueryURL` + headers +
    /// auth), captured from the client's `initConnection`. Present only for
    /// clients that use named/custom queries.
    client_query_ctx: HashMap<String, CustomQueryContext>,
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

    /// `cvr_pool` is the pool of the executor hosting this client group. When the
    /// factory config requests a CVR store (`cvr_pg`), the store binds to THIS
    /// pool — created on the executor's own runtime — so its Postgres connections
    /// are driven by the same reactor that `.await`s them (doc 91, §5.1). `None`
    /// selects an in-memory / storeless CG (tests, no-PG dev).
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
        let query_config = config.query_config;

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
            query_config,
            next_auth_maintenance_at: None,
            pinned_user_id: None,
            cvr: None,
            ttl_clock: 0,
            ttl_clock_base: created_at,
            last_connect_time: created_at,
            keepalive_until: created_at + CG_KEEPALIVE_MS,
            connections: HashMap::new(),
            registered_ws: HashMap::new(),
            client_base_versions: HashMap::new(),
            open_ws_ids: HashSet::new(),
            client_auth: HashMap::new(),
            client_raw_auth: HashMap::new(),
            client_query_ctx: HashMap::new(),
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
        }
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
                if cvr.version.state_version != "00"
                    && cvr
                        .replica_version
                        .as_deref()
                        .is_some_and(|v| v > self.replica_version.as_str())
                {
                    tracing::error!(
                        "CG {}: cannot sync from older replica: CVR={}, DB={}",
                        self.cg_id,
                        cvr.replica_version.as_deref().unwrap_or_default(),
                        self.replica_version
                    );
                    self.cvr = None;
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
                self.cvr = Some(cvr);
            }
            Err(e) => tracing::warn!("CG {}: remove_expired_queries failed: {e}", self.cg_id),
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
        if self.client_raw_auth.is_empty() {
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
        // Snapshot the (client_id, raw_token) pairs to re-verify. Only tokened
        // connections are subject to revalidation.
        let due: Vec<(String, String)> = self
            .client_raw_auth
            .iter()
            .map(|(c, t)| (c.clone(), t.clone()))
            .collect();

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
                crate::auth::decode_jwt_claims(&token)
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
                        self.on_connection_closed(client_id, ws_id);
                    }
                }
                Ok(()) => survivors.push(client_id),
            }
        }

        crate::metrics::Metrics::inc(&self.metrics.auth_revalidations);

        // Retransform each surviving connection's queries against current auth +
        // permissions (re-fetching custom queries with the current token).
        let empty_body = serde_json::json!({});
        for client_id in survivors {
            if self.registered_ws.contains_key(&client_id) {
                self.handle_desired_queries(&client_id, &empty_body, true)
                    .await;
            }
        }

        // Re-arm: schedule the next tick if any authed connection remains,
        // otherwise disarm until the next connection arrives.
        self.next_auth_maintenance_at = None;
        self.arm_auth_maintenance();
    }

    async fn on_new_connection(&mut self, params: ConnectParams, sink: DirectWebSocketSink) {
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
        self.registered_ws.insert(client_id.clone(), ws_id.clone());
        self.client_base_versions.insert(
            client_id.clone(),
            params.base_cookie.as_deref().map(version_from_string),
        );
        self.client_raw_auth.remove(&client_id);
        self.client_query_ctx.remove(&client_id);
        self.client_profile_ids.remove(&client_id);
        if let Some(profile_id) = params.profile_id.as_ref() {
            self.client_profile_ids
                .insert(client_id.clone(), profile_id.clone());
        }

        // Decode the (already-verified) JWT claims for use as `authData` in
        // read-permission rules. Opaque/no token → empty claims.
        let claims = params
            .auth
            .as_deref()
            .filter(|t| !t.is_empty())
            .map(crate::auth::decode_jwt_claims)
            .unwrap_or_else(|| serde_json::json!({}));
        self.client_auth.insert(client_id.clone(), claims);
        // Retain the raw token for the `Authorization: Bearer` header on
        // custom-query transform requests.
        if let Some(tok) = params.auth.as_deref().filter(|t| !t.is_empty()) {
            self.client_raw_auth
                .insert(client_id.clone(), tok.to_string());
        }
        if let Some(context) = default_query_context(self.query_config.as_ref(), &params) {
            self.client_query_ctx.insert(client_id.clone(), context);
        }
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
        // Arm periodic auth maintenance for this (now validated) connection.
        // Port of `validateConnection` setting `revalidateAt = now + interval`.
        self.arm_auth_maintenance();

        let handler = Box::new(SyncerWsMessageHandler::new(
            self.view_syncer.clone(),
            self.conn_context_manager.clone(),
            self.mutagen.clone(),
            self.pusher.clone(),
            client_group_id.clone(),
            client_id.clone(),
            ws_id.clone(),
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

        // Init: send `connected`, check protocol version.
        if !conn.init() {
            self.drop_registration(&client_id, &ws_id);
            if self.open_ws_ids.remove(&ws_id) {
                decrement_nonzero(&self.connection_count);
            }
            return;
        }
        self.connections.insert(client_id.clone(), conn);

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

    async fn on_inbound(&mut self, client_id: String, ws_id: String, text: String) {
        // A superseded socket can have frames already queued when its replacement
        // is installed. Never route those frames through the new connection.
        if self.registered_ws.get(&client_id) != Some(&ws_id) {
            tracing::debug!(
                "CG {}: ignoring stale inbound frame for {client_id}/{ws_id}",
                self.cg_id
            );
            return;
        }
        // Do not let the direct-engine intercept bypass protocol validation.
        // Malformed messages must take the normal Connection fatal-error path,
        // rather than being partially parsed and silently dropped below.
        if crate::protocol::parse_upstream(&text).is_err() {
            let closed = match self.connections.get(&client_id) {
                Some(conn) => !conn.handle_inbound(&text),
                None => return,
            };
            if closed {
                self.on_connection_closed(client_id, ws_id);
            }
            return;
        }
        // Intercept desired-query messages and route them to the CG-owned
        // SyncEngine — the placeholder `ViewSyncerDispatch` can't reach the
        // `!Send` engine. Everything else (ping, etc.) goes through Connection.
        if let Ok(serde_json::Value::Array(arr)) = serde_json::from_str::<serde_json::Value>(&text)
            && let Some(tag) = arr.first().and_then(|v| v.as_str())
        {
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
                    self.handle_inspect(&client_id, body);
                }
                return;
            }
        }
        let closed = match self.connections.get(&client_id) {
            Some(conn) => !conn.handle_inbound(&text),
            None => return,
        };
        if closed {
            self.on_connection_closed(client_id, ws_id);
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
        let client_schema = body
            .get("clientSchema")
            .filter(|value| !value.is_null())
            .cloned();
        if let Some(schema) = client_schema.as_ref()
            && let Err(message) =
                crate::replica_schema::validate_client_schema(schema, &self.tables)
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
            self.on_connection_closed(client_id.to_string(), ws_id);
            return;
        }
        // Capture the custom-query API context from an `initConnection` that
        // carries a `userQueryURL` (named queries are resolved against it). The
        // context persists for the connection's lifetime (a later
        // `changeDesiredQueries` doesn't re-send the URL).
        if let Some(url) = body.get("userQueryURL").and_then(|v| v.as_str())
            && !url.is_empty()
        {
            if !query_url_is_allowed(self.query_config.as_ref(), url) {
                let message =
                    format!("URL \"{url}\" is not allowed by the ZERO_QUERY_URL configuration");
                if let Some(conn) = self.connections.get(client_id) {
                    conn.close_with_error(crate::protocol::ErrorBody::TransformFailedZeroCache(
                        crate::protocol::TransformFailedZeroCacheBody {
                            kind: crate::protocol::ErrorKind::TransformFailed,
                            details: None,
                            query_ids: Vec::new(),
                            message,
                            origin: crate::protocol::ErrorOrigin::ZeroCache,
                            reason: crate::protocol::ErrorReason::Internal,
                        },
                    ));
                }
                return;
            }
            let custom_headers = filtered_query_headers(self.query_config.as_ref(), body);
            let auth = self.client_raw_auth.get(client_id).cloned();
            let context = self
                .client_query_ctx
                .entry(client_id.to_string())
                .or_insert_with(|| CustomQueryContext {
                    url: url.to_string(),
                    headers: Vec::new(),
                    auth,
                });
            context.url = url.to_string();
            context.headers.extend(custom_headers);
        }
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
        }

        // Ensure a group CVR: load from the store, or start fresh (dev/no-PG).
        match self.ensure_cvr(true).await {
            Ok(true) => {}
            Ok(false) => {
                self.fail_group("Unable to load the client view state");
                return;
            }
            Err(crate::sync_engine::LoadCvrError::Store(
                rust_cvr::store::CVRStoreError::ClientNotFound(message),
            )) => {
                if let Some(conn) = self.connections.get(client_id) {
                    conn.close_with_error(crate::protocol::ErrorBody::client_not_found(message));
                }
                self.on_connection_closed(client_id.to_string(), ws_id);
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
            self.on_connection_closed(client_id.to_string(), ws_id);
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
            self.on_connection_closed(client_id.to_string(), ws_id);
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
            // The client's decoded JWT claims (`authData` for permission rules).
            let auth_data = self
                .client_auth
                .get(client_id)
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let now = now_ms();
            let ttl_clock = self.get_ttl_clock(now);
            match self
                .sync_engine
                .config_and_hydrate_with_profile(
                    cvr,
                    client_id,
                    &[ws_id],
                    &self.shard,
                    puts,
                    dels,
                    clear,
                    client_schema,
                    self.client_profile_ids.get(client_id).map(String::as_str),
                    self.permissions.as_ref(),
                    &auth_data,
                    self.client_query_ctx.get(client_id),
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
                    self.cvr = Some(cvr);
                    config_accepted = true;
                    crate::metrics::Metrics::inc(&self.metrics.hydrations);
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
        let new_claims = crate::auth::decode_jwt_claims(token);
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
                self.on_connection_closed(client_id.to_string(), ws_id);
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
                self.on_connection_closed(client_id.to_string(), ws_id);
            }
            return;
        }

        // No change in resolved auth → skip re-validation + re-transformation
        // (TS `authRevisionChanged` guard).
        let unchanged = self
            .client_auth
            .get(client_id)
            .map(|prev| prev == &new_claims)
            .unwrap_or(false);
        if unchanged {
            tracing::debug!(
                "CG {}: updateAuth unchanged for client {client_id}, skipping re-transform",
                self.cg_id
            );
            return;
        }

        // Store the new auth data + raw token, then re-run the config/hydrate
        // pass with an empty desired-queries patch. Phase 2 recomputes every
        // query's transform against the updated authData (and re-fetches custom
        // queries with the new Bearer token), detects the hash drift, and
        // re-hydrates.
        crate::metrics::Metrics::inc(&self.metrics.auth_changes);
        self.client_auth.insert(client_id.to_string(), new_claims);
        self.client_raw_auth
            .insert(client_id.to_string(), token.to_string());
        if let Some(ctx) = self.client_query_ctx.get_mut(client_id) {
            ctx.auth = Some(token.to_string());
        }
        let empty_body = serde_json::json!({});
        self.handle_desired_queries(client_id, &empty_body, true)
            .await;
    }

    /// Handle an inspector `["inspect", {op, id, ...}]` message. Port of
    /// `handleInspect` (`inspect-handler.ts`): every op except `authenticate`
    /// requires the client group to have authenticated first; unauthenticated
    /// requests get an `authenticated:false` challenge instead of a result.
    fn handle_inspect(&mut self, client_id: &str, body: &serde_json::Value) {
        let Some(ws_id) = self.registered_ws.get(client_id).cloned() else {
            return;
        };
        let op = body.get("op").and_then(|v| v.as_str()).unwrap_or("");
        let id = body.get("id").cloned().unwrap_or(serde_json::Value::Null);
        let respond = |engine: &SyncEngine, resp: serde_json::Value| {
            engine.send_inspect_response(&ws_id, resp);
        };

        // Auth gate — only `authenticate` is allowed before authenticating.
        if op != "authenticate" && !self.inspector_authenticated {
            respond(
                &self.sync_engine,
                serde_json::json!({"op": "authenticated", "id": id, "value": false}),
            );
            return;
        }

        match op {
            "authenticate" => {
                let password = body.get("value").and_then(|v| v.as_str()).unwrap_or("");
                // Valid only if an admin password is configured AND matches.
                let ok = self
                    .admin_password
                    .as_deref()
                    .is_some_and(|p| !p.is_empty() && p == password);
                self.inspector_authenticated = ok;
                respond(
                    &self.sync_engine,
                    serde_json::json!({"op": "authenticated", "id": id, "value": ok}),
                );
            }
            "version" => {
                respond(
                    &self.sync_engine,
                    serde_json::json!({"op": "version", "id": id, "value": self.server_version}),
                );
            }
            "queries" => {
                // Best-effort from the in-memory CVR: id, ast, ttl, and the
                // clients that desire each query. (TS also folds server-side
                // materialization metrics from the InspectorDelegate, which is
                // not ported — the `metrics` field is left null.)
                let filter_client = body.get("clientID").and_then(|v| v.as_str());
                let value = self.inspect_queries_value(filter_client);
                respond(
                    &self.sync_engine,
                    serde_json::json!({"op": "queries", "id": id, "value": value}),
                );
            }
            "metrics" => {
                // Server metrics come from the OTel/InspectorDelegate layer,
                // which is not yet ported (task 14) — report empty.
                respond(
                    &self.sync_engine,
                    serde_json::json!({"op": "metrics", "id": id, "value": []}),
                );
            }
            "analyze-query" => {
                // `analyzeQuery` (query plan / vended-rows analysis) is not
                // ported; answer with a clear, non-hanging error value.
                respond(
                    &self.sync_engine,
                    serde_json::json!({
                        "op": "analyze-query",
                        "id": id,
                        "value": {"error": "analyze-query is not supported by rust-syncer"}
                    }),
                );
            }
            other => {
                tracing::warn!("CG {}: unknown inspect op {other:?}", self.cg_id);
            }
        }
    }

    /// Build the `queries` inspector value from the in-memory CVR.
    fn inspect_queries_value(&self, filter_client: Option<&str>) -> serde_json::Value {
        let Some(cvr) = &self.cvr else {
            return serde_json::json!([]);
        };
        let mut out = Vec::new();
        for (qid, record) in &cvr.queries {
            // Which clients desire this query (and optionally filter to one).
            let mut client_ids: Vec<&String> = cvr
                .clients
                .iter()
                .filter(|(_, c)| c.desired_query_ids.iter().any(|q| q == qid))
                .map(|(id, _)| id)
                .collect();
            if let Some(fc) = filter_client {
                if !client_ids.iter().any(|c| c.as_str() == fc) {
                    continue;
                }
                client_ids.retain(|c| c.as_str() == fc);
            }
            let (ast, name) = match record {
                rust_cvr::types::QueryRecord::Client(r) => (Some(r.ast.clone()), None),
                rust_cvr::types::QueryRecord::Internal(r) => (Some(r.ast.clone()), None),
                rust_cvr::types::QueryRecord::Custom(r) => (None, Some(r.name.clone())),
            };
            out.push(serde_json::json!({
                "queryID": qid,
                "ast": ast,
                "name": name,
                "got": record.base().transformation_hash.is_some(),
                "clientIDs": client_ids,
                "metrics": serde_json::Value::Null,
            }));
        }
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

        let mut delete_ids: Vec<String> = Vec::new();
        if let Some(active) = active_clients {
            let active_set: HashSet<&str> = active.iter().map(String::as_str).collect();
            if let Some(cvr) = &self.cvr {
                for id in cvr.clients.keys() {
                    if !active_set.contains(id.as_str()) {
                        delete_ids.push(id.clone());
                    }
                }
            }
        }
        // Explicit deletions are acked; a client may not delete itself.
        let ack_ids: Vec<String> = deleted_client_ids
            .iter()
            .filter(|c| c.as_str() != caller_client_id)
            .cloned()
            .collect();
        for id in &ack_ids {
            if !delete_ids.contains(id) {
                delete_ids.push(id.clone());
            }
        }

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
        // NOTE: mutations are HTTP-direct (task 8), so there is no pusher here
        // to run `pusher.delete_client_mutations` — mutation cleanup happens on
        // the TS mutation path, not in the Rust syncer.
    }

    fn on_connection_closed(&mut self, client_id: String, ws_id: String) {
        // Every accepted socket increments the CG handle count, including a
        // socket later superseded by another wsID.
        if self.open_ws_ids.remove(&ws_id) {
            decrement_nonzero(&self.connection_count);
        }

        // A delayed close from the superseded socket must not remove the current
        // connection that happens to share its clientID.
        if self.registered_ws.get(&client_id) != Some(&ws_id) {
            return;
        }
        self.connections.remove(&client_id);
        self.registered_ws.remove(&client_id);
        self.client_base_versions.remove(&client_id);
        self.sync_engine.unregister_client(&ws_id);
        self.client_auth.remove(&client_id);
        self.client_raw_auth.remove(&client_id);
        self.client_query_ctx.remove(&client_id);
        self.client_profile_ids.remove(&client_id);
        let mut global = lock_unpoisoned(&self.global_connections);
        if global
            .get(&client_id)
            .is_some_and(|info| info.ws_id == ws_id)
        {
            global.remove(&client_id);
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

    fn close_connection(&mut self, client_id: String, ws_id: String) {
        if self.registered_ws.get(&client_id) != Some(&ws_id) {
            return;
        }
        if let Some(conn) = self.connections.get(&client_id) {
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
        let conn = match rusqlite::Connection::open(path) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    "CG {}: could not open replica to check permissions: {e}",
                    self.cg_id
                );
                return false;
            }
        };
        match crate::permissions::reload_permissions_if_changed(
            &conn,
            &self.app_id,
            self.permissions_hash.as_deref(),
        ) {
            crate::permissions::PermissionsReload::Unchanged => false,
            crate::permissions::PermissionsReload::Changed { permissions, hash } => {
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
    async fn on_notification(&mut self) {
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
        let now = now_ms();
        let ttl_clock = self.get_ttl_clock(now);
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
                crate::metrics::Metrics::inc(&self.metrics.advances);
                if let Some(reason) = result.reset_reason.clone() {
                    // The engine could not advance in place (snapshot/schema
                    // drift). Port of TS `ResetPipelinesSignal` handling: the
                    // in-flight poke was already cancelled; re-init the pipeline
                    // and re-hydrate every query from scratch.
                    crate::metrics::Metrics::inc(&self.metrics.resets);
                    self.reset_pipelines_and_rehydrate(result.cvr, &reason)
                        .await;
                } else {
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
            match crate::replica_schema::compute_table_specs_from_path(path) {
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
        for (client_id, ws_id) in clients {
            let state_version = self
                .sync_engine
                .pipelines()
                .current_version()
                .unwrap_or_else(|| cvr.version.state_version.clone());
            let replica_version = self.replica_version.clone();
            let existing_rows = self.sync_engine.existing_rows().await;
            let auth_data = self
                .client_auth
                .get(&client_id)
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let ttl_clock = self.get_ttl_clock(now);
            // Clone the CVR into the call so a failure doesn't consume it.
            match self
                .sync_engine
                .config_and_hydrate_with_profile(
                    cvr.clone(),
                    &client_id,
                    &[ws_id],
                    &self.shard,
                    Vec::new(),
                    Vec::new(),
                    false,
                    None,
                    self.client_profile_ids.get(&client_id).map(String::as_str),
                    self.permissions.as_ref(),
                    &auth_data,
                    self.client_query_ctx.get(&client_id),
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
        self.cvr = Some(cvr);
    }

    fn drop_registration(&mut self, client_id: &str, ws_id: &str) {
        self.registered_ws.remove(client_id);
        self.client_base_versions.remove(client_id);
        self.sync_engine.unregister_client(ws_id);
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
        self.connection_count.store(0, Ordering::Relaxed);
    }

    /// Permanently fail this CG. Continuing would be unsafe because Rust IVM
    /// advancement is not rollbackable after the snapshot swaps; a failed CVR
    /// commit would otherwise cause the next notification to skip that batch.
    fn fail_group(&mut self, message: &str) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        self.accepting.store(false, Ordering::SeqCst);
        for (_, conn) in self.connections.drain() {
            conn.close_with_error(crate::protocol::ErrorBody::rehome(message));
        }
        for (_, ws_id) in self.registered_ws.drain() {
            self.sync_engine.unregister_client(&ws_id);
        }
        self.client_base_versions.clear();
        self.client_auth.clear();
        self.client_raw_auth.clear();
        self.client_query_ctx.clear();
        self.client_profile_ids.clear();
        self.open_ws_ids.clear();
        self.connection_count.store(0, Ordering::Relaxed);
    }
}

/// Build an executor's CVR Postgres pool on the *current* runtime (the executor's
/// own `current_thread` runtime). Every connection this pool opens is therefore
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
            }) => {
                let ctx = CgTaskContext {
                    services_factory: services_factory.clone(),
                    auth_validator: auth_validator.clone(),
                    connections: connections.clone(),
                    cvr_pool: pool.clone(),
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
                    cg_event_loop(&cg_id, rx, connection_count, accepting, ctx).await;
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
    loop {
        let next_delay = [
            state.next_expiry_delay(),
            state.next_auth_maintenance_delay(),
            state.next_idle_shutdown_delay(),
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
                        continue;
                    }
                }
            }
            None => match rx.recv().await {
                Some(msg) => msg,
                None => break,
            },
        };
        match msg {
            CGMessage::NewConnection { params, sink } => {
                state.on_new_connection(*params, sink).await
            }
            CGMessage::Inbound {
                client_id,
                ws_id,
                text,
            } => state.on_inbound(client_id, ws_id, text).await,
            CGMessage::ConnectionClosed { client_id, ws_id } => {
                state.on_connection_closed(client_id, ws_id)
            }
            CGMessage::CloseConnection { client_id, ws_id } => {
                state.close_connection(client_id, ws_id)
            }
            CGMessage::Notification(_) => state.on_notification().await,
            CGMessage::Shutdown => {
                tracing::info!("CG thread {cg_id}: shutting down");
                state.shutdown();
                break;
            }
        }
        if state.terminal {
            tracing::error!("CG thread {cg_id}: terminating after fatal synchronization error");
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_handler::{
        ConnContextInfo, ConnContextManagerDispatch, ConnectionSelector, ViewSyncerDispatch,
    };
    use crate::protocol::PROTOCOL_VERSION;
    use crate::ws_sink::{DirectWebSocketSink, WsCommand};

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
            Arc::new(crate::auth::JwtAuthValidator {
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
        }
    }

    fn seed_test_client_schema(state: &mut CgState) {
        let mut cvr = empty_cvr(&state.cg_id, &state.replica_version);
        cvr.client_schema = Some(serde_json::json!({"tables": {}}));
        state.cvr = Some(cvr);
    }

    #[test]
    fn configured_query_context_matches_typescript_defaults_and_header_filtering() {
        let config = FetchConfig {
            url: Some(vec!["https://api.example/query".to_string()]),
            api_key: Some("secret".to_string()),
            allowed_client_headers: Some(vec!["X-Request-ID".to_string()]),
            forward_cookies: true,
        };
        let mut params = test_params("c1", "w1");
        params.auth = Some("jwt".to_string());
        params.origin = Some("https://app.example".to_string());
        params.http_cookie = Some("session=1".to_string());

        let context = default_query_context(Some(&config), &params).unwrap();
        assert_eq!(context.url, "https://api.example/query");
        assert_eq!(context.auth.as_deref(), Some("jwt"));
        assert!(
            context
                .headers
                .contains(&("X-Api-Key".to_string(), "secret".to_string()))
        );
        assert!(
            context
                .headers
                .contains(&("Cookie".to_string(), "session=1".to_string()))
        );
        assert!(
            context
                .headers
                .contains(&("Origin".to_string(), "https://app.example".to_string()))
        );

        let body = serde_json::json!({
            "userQueryHeaders": {
                "x-request-id": "allowed",
                "authorization": "blocked"
            }
        });
        assert_eq!(
            filtered_query_headers(Some(&config), &body),
            vec![("x-request-id".to_string(), "allowed".to_string())]
        );
        assert!(query_url_is_allowed(
            Some(&config),
            "https://api.example/query"
        ));
        assert!(!query_url_is_allowed(
            Some(&config),
            "https://evil.example/query"
        ));
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

    /// Periodic revalidation must CLOSE a connection whose token no longer
    /// validates (expired/revoked). Security core of TS `#runAuthMaintenance`'s
    /// `dueRevalidations` → `#validateConnection` failure path.
    #[test]
    fn periodic_revalidation_closes_expired_connection() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid.clone());

        let (tx, _drx) = tokio::sync::mpsc::channel::<WsCommand>(64);
        rt.block_on(state.on_new_connection(
            authed_params("c1", "ws1", "tok-c1"),
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
        assert!(state.client_raw_auth.is_empty());
        assert_eq!(state.metrics.snapshot()["authRevalidationFailures"], 1);
        // No authed connection remains → disarmed.
        assert!(state.next_auth_maintenance_at.is_none());
    }

    /// A still-valid token survives the tick and the deadline is re-armed for the
    /// next interval.
    #[test]
    fn periodic_revalidation_keeps_valid_connection_and_rearms() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        let (tx, _drx) = tokio::sync::mpsc::channel::<WsCommand>(64);
        rt.block_on(state.on_new_connection(
            authed_params("c1", "ws1", "tok-c1"),
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

    /// With the feature disabled (interval None) no deadline is ever armed, and a
    /// connection without a token is never subject to revalidation.
    #[test]
    fn periodic_revalidation_disabled_or_unauthed_never_arms() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));

        // Disabled: interval None.
        let mut disabled = revalidate_state(&rt, None, valid.clone());
        let (tx, _d) = tokio::sync::mpsc::channel::<WsCommand>(64);
        rt.block_on(disabled.on_new_connection(
            authed_params("c1", "ws1", "tok"),
            DirectWebSocketSink::new(tx),
        ));
        assert!(disabled.next_auth_maintenance_at.is_none());
        assert!(disabled.next_auth_maintenance_delay().is_none());

        // Enabled but the connection carries no token → nothing to revalidate.
        let mut unauthed = revalidate_state(&rt, Some(300_000), valid);
        let (tx2, _d2) = tokio::sync::mpsc::channel::<WsCommand>(64);
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

        let (tx, _drx) = tokio::sync::mpsc::channel::<WsCommand>(64);
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

        let (tx, _drx) = tokio::sync::mpsc::channel::<WsCommand>(64);
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

    /// The CG event loop: a new connection sends `connected` and registers a
    /// client with the SyncEngine; a notification with no CVR is graceful; a
    /// disconnect unregisters the client. Runs on the test thread (not a tokio
    /// worker), so the sink's `blocking_send` is legal.
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
            Arc::new(crate::auth::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            global,
            count,
        );

        let (tx, mut drx) = tokio::sync::mpsc::channel::<WsCommand>(64);
        let sink = DirectWebSocketSink::new(tx);
        rt.block_on(state.on_new_connection(test_params("c1", "ws1"), sink));

        // `connected` was pushed to the sink and the client is registered.
        let mut connected = false;
        while let Ok(cmd) = drx.try_recv() {
            if let WsCommand::Send(v) = cmd
                && v[0] == "connected"
            {
                connected = true;
            }
        }
        assert!(connected, "expected a connected frame");
        assert_eq!(state.registered_ws.len(), 1);
        assert_eq!(state.connections.len(), 1);

        // Notification with no loaded CVR (no PG) is a graceful no-op.
        rt.block_on(state.on_notification());

        // Disconnect unregisters the client.
        state.on_connection_closed("c1".to_string(), "ws1".to_string());
        assert_eq!(state.registered_ws.len(), 0);
        assert_eq!(state.connections.len(), 0);
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
            Arc::new(crate::auth::JwtAuthValidator {
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
            Arc::new(crate::auth::JwtAuthValidator {
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
        let (tx1, mut drx1) = tokio::sync::mpsc::channel::<WsCommand>(64);
        rt.block_on(
            state.on_new_connection(test_params("c1", "ws1"), DirectWebSocketSink::new(tx1)),
        );
        while drx1.try_recv().is_ok() {} // drain ws1's `connected` frame

        // Reconnect: same client c1 on a NEW ws2.
        let (tx2, _drx2) = tokio::sync::mpsc::channel::<WsCommand>(64);
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
        state.on_connection_closed("c1".to_string(), "ws1".to_string());
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
            Arc::new(crate::auth::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            global,
            count,
        );

        let (tx, mut drx) = tokio::sync::mpsc::channel::<WsCommand>(64);
        let sink = DirectWebSocketSink::new(tx);
        rt.block_on(state.on_new_connection(test_params("c1", "ws1"), sink));

        state.shutdown();

        let mut saw_rehome = false;
        while let Ok(WsCommand::Send(v)) = drx.try_recv() {
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
        let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::JwtAuthValidator {
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
        let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::JwtAuthValidator {
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
        let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::JwtAuthValidator {
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
            let (sink_tx, sink_rx) = tokio::sync::mpsc::channel::<WsCommand>(64);
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
        let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::JwtAuthValidator {
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
        let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::JwtAuthValidator {
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
            Arc::new(crate::auth::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            global,
            count,
        );

        let (tx, _drx) = tokio::sync::mpsc::channel::<WsCommand>(64);
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
            Arc::new(crate::auth::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicU64::new(1)),
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel::<WsCommand>(64);
        rt.block_on(
            state.on_new_connection(test_params("c1", "ws1"), DirectWebSocketSink::new(tx)),
        );
        rt.block_on(state.handle_desired_queries(
            "c1",
            &serde_json::json!({"desiredQueriesPatch": []}),
            true,
        ));

        let error = std::iter::from_fn(|| rx.try_recv().ok()).find_map(|command| match command {
            WsCommand::Send(value)
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
            Arc::new(crate::auth::JwtAuthValidator {
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

        let (tx, mut drx) = tokio::sync::mpsc::channel::<WsCommand>(64);
        let sink = DirectWebSocketSink::new(tx);
        rt.block_on(state.on_new_connection(test_params("c1", "ws1"), sink));

        let drain = |drx: &mut tokio::sync::mpsc::Receiver<WsCommand>| -> Vec<serde_json::Value> {
            let mut v = Vec::new();
            while let Ok(WsCommand::Send(m)) = drx.try_recv() {
                v.push(m);
            }
            v
        };
        let _ = drain(&mut drx); // discard the `connected` frame

        // 1) `version` before authenticating → challenge (authenticated:false).
        rt.block_on(state.on_inbound(
            "c1".to_string(),
            "ws1".to_string(),
            r#"["inspect",{"op":"version","id":"1"}]"#.to_string(),
        ));
        let frames = drain(&mut drx);
        let last = frames.last().unwrap();
        assert_eq!(last[0], "inspect");
        assert_eq!(last[1]["op"], "authenticated");
        assert_eq!(last[1]["value"], false);

        // 2) authenticate with the wrong password → false.
        rt.block_on(state.on_inbound(
            "c1".to_string(),
            "ws1".to_string(),
            r#"["inspect",{"op":"authenticate","id":"2","value":"nope"}]"#.to_string(),
        ));
        assert_eq!(drain(&mut drx).last().unwrap()[1]["value"], false);
        assert!(!state.inspector_authenticated);

        // 3) authenticate with the right password → true.
        rt.block_on(state.on_inbound(
            "c1".to_string(),
            "ws1".to_string(),
            r#"["inspect",{"op":"authenticate","id":"3","value":"s3cret"}]"#.to_string(),
        ));
        assert_eq!(drain(&mut drx).last().unwrap()[1]["value"], true);
        assert!(state.inspector_authenticated);

        // 4) `version` now returns the configured server version.
        rt.block_on(state.on_inbound(
            "c1".to_string(),
            "ws1".to_string(),
            r#"["inspect",{"op":"version","id":"4"}]"#.to_string(),
        ));
        let last = drain(&mut drx).into_iter().next_back().unwrap();
        assert_eq!(last[1]["op"], "version");
        assert_eq!(last[1]["value"], "9.9.9");
    }
}
