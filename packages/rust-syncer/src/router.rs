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
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::mpsc;

/// Small delay added when scheduling TTL eviction so many near-simultaneous
/// expirations collapse into one timer wakeup. Port of TS `TTL_TIMER_HYSTERESIS`.
const TTL_TIMER_HYSTERESIS_MS: i64 = 50;
/// Upper bound on a single eviction-timer delay (matches `rust_cvr::ttl::MAX_TTL_MS`).
const MAX_TTL_MS: i64 = 600_000;

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
        params: ConnectParams,
        sink: DirectWebSocketSink,
    },
    /// An inbound WS text frame for a connection (forwarded from the WS reader).
    Inbound { client_id: String, text: String },
    /// A connection's WS closed (its upstream channel ended).
    ConnectionClosed { client_id: String },
    /// Change-streamer notification — new data is available; advance + poke.
    Notification(serde_json::Value),
    /// The CG should shut down (no more connections).
    Shutdown,
}

/// Handle to a CG thread.
pub struct CGHandle {
    /// Sender for messages to the CG thread.
    tx: crossbeam_channel::Sender<CGMessage>,
    /// Join handle for the CG thread.
    handle: Option<JoinHandle<()>>,
    /// Number of active connections on this CG.
    connection_count: Arc<AtomicU64>,
}

impl CGHandle {
    /// Send a message to the CG thread.
    pub fn send(&self, msg: CGMessage) -> Result<(), crossbeam_channel::SendError<CGMessage>> {
        self.tx.send(msg)
    }

    /// Shut down the CG thread.
    pub fn shutdown(&mut self) {
        let _ = self.tx.send(CGMessage::Shutdown);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    /// Number of active connections.
    pub fn connection_count(&self) -> u64 {
        self.connection_count.load(Ordering::Relaxed)
    }
}

/// CVR Postgres connection config for a client group.
#[derive(Clone)]
pub struct CvrPgConfig {
    pub pg_uri: String,
    pub schema: String,
    /// CVR id (== client group id).
    pub cvr_id: String,
    pub task_id: String,
    /// Max Postgres connections for this CG's CVR pool (TS parity:
    /// `--cvr-max-conns-per-worker`).
    pub max_conns: u32,
}

/// Everything the CG thread needs to build its `SyncEngine` locally. `Send` so
/// it can cross into the CG thread; the (`!Send`) `SyncEngine` is then
/// constructed on that thread.
pub struct SyncEngineConfig {
    pub tables: Vec<IvmTableSpec>,
    /// SQLite replica path; `None` selects in-memory sources (test/dev).
    pub replica_path: Option<String>,
    pub app_id: String,
    pub shard: ShardID,
    pub cvr_pg: Option<CvrPgConfig>,
    /// Compiled read-permissions (`PermissionsConfig` JSON) loaded from the
    /// replica, or `None` if none are deployed (queries pass through).
    pub permissions: Option<serde_json::Value>,
    /// The deployed permissions `hash` at load time, used to detect a
    /// hot-reload (a redeploy of `zero-deploy-permissions`). `None` when no
    /// permissions are deployed. Port of TS `LoadedPermissions.hash`.
    pub permissions_hash: Option<String>,
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

/// The connection router — manages CG threads and routes connections.
///
/// Port of the `Syncer` class's connection management.
pub struct ConnectionRouter {
    /// Map of client_group_id → CG handle.
    cg_handles: DashMap<String, CGHandle>,
    /// Factory for creating per-CG services.
    services_factory: Arc<dyn CGServicesFactory>,
    /// Auth validator.
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
        Self {
            cg_handles: DashMap::new(),
            services_factory,
            auth_validator,
            metrics,
            connections: Arc::new(Mutex::new(HashMap::new())),
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
        if let Some(auth_str) = &auth {
            if !auth_str.is_empty() {
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
        }

        // 2. Check (and, on the first connection, BIND) the group's userID.
        //    Port of TS `ConnectionContextManager.validateConnection`: the first
        //    successful connection pins the client group to its userID; every
        //    later connection must match it. Without the bind step the check
        //    below is inert — the group is never pinned, so two different users
        //    could share one client group.
        {
            let mut states = self.group_auth_states.lock().unwrap();
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
                ctx.sink.fail(error);
                return;
            }
        }

        // 3. Close existing connection for same clientID (replacement).
        {
            let mut conns = self.connections.lock().unwrap();
            if let Some(existing) = conns.get(&client_id) {
                tracing::debug!(
                    "client {client_id} already connected, closing existing connection"
                );
                // In the full implementation, we'd close the existing connection.
                // For now, just remove it — the WS sink close will happen via the CG thread.
                let _ = existing;
                conns.remove(&client_id);
            }
            conns.insert(
                client_id.clone(),
                ConnectionInfo {
                    client_group_id: client_group_id.clone(),
                    ws_id: ws_id.clone(),
                },
            );
        }

        // 4. Get or create CG thread.
        let cg_handle = self.get_or_create_cg(&client_group_id);

        // 5. Split the context: the CG thread owns connection setup + the sink,
        //    while a lightweight forwarder task funnels inbound WS frames into
        //    the CG's unified channel (so the CG loop never blocks on one conn).
        let ConnectionContext {
            params,
            sink,
            upstream_rx,
        } = ctx;
        let cg_tx = cg_handle.tx.clone();
        tokio::spawn(forward_inbound(
            upstream_rx,
            cg_tx.clone(),
            client_id.clone(),
        ));
        if cg_tx
            .send(CGMessage::NewConnection { params, sink })
            .is_err()
        {
            tracing::error!("Failed to send connection to CG thread for {client_group_id}");
        }
    }

    /// Get or create a CG thread for the given client group ID.
    fn get_or_create_cg(&self, client_group_id: &str) -> Arc<CGHandle> {
        // Fast path: CG already exists.
        if let Some(handle) = self.cg_handles.get(client_group_id) {
            // We can't just return a reference to the DashMap entry because
            // we need to potentially create a new CG if it doesn't exist.
            // Instead, we clone the necessary parts.
            handle.connection_count.fetch_add(1, Ordering::Relaxed);
            return Arc::new(CGHandle {
                tx: handle.tx.clone(),
                handle: None,
                connection_count: handle.connection_count.clone(),
            });
        }

        // Slow path: create new CG thread.
        let (tx, rx) = crossbeam_channel::unbounded::<CGMessage>();
        let connection_count = Arc::new(AtomicU64::new(1));

        let services_factory = self.services_factory.clone();
        let auth_validator = self.auth_validator.clone();
        let connections = self.connections.clone();
        let cg_id = client_group_id.to_string();
        let conn_count = connection_count.clone();

        let handle = std::thread::Builder::new()
            .name(format!("cg-{cg_id}"))
            .spawn(move || {
                run_cg_thread(
                    &cg_id,
                    rx,
                    &services_factory,
                    auth_validator,
                    &connections,
                    conn_count,
                );
            })
            .expect("failed to spawn CG thread");

        let cg_handle = CGHandle {
            tx,
            handle: Some(handle),
            connection_count: connection_count.clone(),
        };

        self.cg_handles
            .insert(client_group_id.to_string(), cg_handle);

        // Return a handle (without the join handle — that stays in the map).
        // Look it up again.
        let entry = self.cg_handles.get(client_group_id).unwrap();
        Arc::new(CGHandle {
            tx: entry.tx.clone(),
            handle: None,
            connection_count: entry.connection_count.clone(),
        })
    }

    /// Shut down all CG threads.
    pub async fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);

        for mut entry in self.cg_handles.iter_mut() {
            entry.value_mut().shutdown();
        }
        self.cg_handles.clear();
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
/// unified crossbeam channel, so the CG thread never blocks on a single
/// connection. Runs as a tokio task. Emits `ConnectionClosed` when the WS ends.
async fn forward_inbound(
    mut upstream_rx: mpsc::Receiver<String>,
    cg_tx: crossbeam_channel::Sender<CGMessage>,
    client_id: String,
) {
    while let Some(text) = upstream_rx.recv().await {
        if cg_tx
            .send(CGMessage::Inbound {
                client_id: client_id.clone(),
                text,
            })
            .is_err()
        {
            return; // CG thread gone.
        }
    }
    let _ = cg_tx.send(CGMessage::ConnectionClosed { client_id });
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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
    /// The in-memory CVR, lazily loaded from the store on first notification.
    cvr: Option<CVR>,
    /// Monotonic TTL clock (ms), seeded from `cvr.ttl_clock` when the CVR is
    /// loaded and advanced by wall-time delta while this CG runs — so a long
    /// downtime does not mass-expire queries. Port of TS `#ttlClock`.
    ttl_clock: TTLClock,
    /// Wall-clock (ms) at the last `get_ttl_clock`. Port of TS `#ttlClockBase`.
    ttl_clock_base: i64,
    /// client_id → Connection.
    connections: HashMap<String, Connection>,
    /// client_id → ws_id, for clients registered with the SyncEngine.
    registered_ws: HashMap<String, String>,
    /// client_id → decoded JWT claims (`authData` for permission rules).
    client_auth: HashMap<String, serde_json::Value>,
    /// client_id → raw JWT (for the `Authorization: Bearer` header on
    /// custom-query transform requests).
    client_raw_auth: HashMap<String, String>,
    /// client_id → the custom-query API context (`userQueryURL` + headers +
    /// auth), captured from the client's `initConnection`. Present only for
    /// clients that use named/custom queries.
    client_query_ctx: HashMap<String, CustomQueryContext>,
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
    /// Runtime handle for `block_on` at async edges on this (runtime-less) CG
    /// thread (e.g. re-verifying an `updateAuth` token).
    tokio_handle: tokio::runtime::Handle,
    global_connections: Arc<Mutex<HashMap<String, ConnectionInfo>>>,
    connection_count: Arc<AtomicU64>,
}

impl CgState {
    fn new(
        cg_id: &str,
        services_factory: &Arc<dyn CGServicesFactory>,
        auth_validator: Arc<dyn AuthValidator>,
        global_connections: Arc<Mutex<HashMap<String, ConnectionInfo>>>,
        connection_count: Arc<AtomicU64>,
    ) -> Self {
        let view_syncer = services_factory.create_view_syncer(cg_id);
        let conn_context_manager = services_factory.create_conn_context_manager(cg_id);
        let mutagen = services_factory.create_mutagen(cg_id);
        let pusher = services_factory.create_pusher(cg_id);
        let config = services_factory.create_sync_engine_config(cg_id);

        // Build the SyncEngine on this thread (it is !Send). Retain the table
        // specs + replica path + app id so the pipeline can be re-initialized on
        // an advance reset.
        let tokio_handle = config.tokio_handle.clone();
        let admin_password = config.admin_password.clone();
        let server_version = config.server_version.clone();
        let metrics = config.metrics.clone();
        let tables = config.tables.clone();
        let replica_path = config.replica_path.clone();
        let app_id = config.app_id.clone();
        let mut sync_engine = SyncEngine::new(IvmPipelines::new());
        sync_engine.set_tokio_handle(config.tokio_handle.clone());
        if let Err(e) = sync_engine.pipelines().init(
            config.tables,
            config.replica_path.as_deref(),
            &config.app_id,
        ) {
            tracing::error!("CG {cg_id}: pipelines init failed: {e}");
        }
        let replica_version = sync_engine
            .pipelines()
            .current_version()
            .unwrap_or_default();
        let permissions = config.permissions;
        let permissions_hash = config.permissions_hash;

        let mut cvr_pg = false;
        if let Some(pg) = config.cvr_pg {
            match sync_engine.set_cvr_store(
                &pg.pg_uri,
                pg.schema,
                pg.cvr_id,
                pg.task_id,
                pg.max_conns,
            ) {
                Ok(()) => cvr_pg = true,
                Err(e) => tracing::error!("CG {cg_id}: set_cvr_store failed: {e}"),
            }
        }

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
            cvr: None,
            ttl_clock: 0,
            ttl_clock_base: now_ms(),
            connections: HashMap::new(),
            registered_ws: HashMap::new(),
            client_auth: HashMap::new(),
            client_raw_auth: HashMap::new(),
            client_query_ctx: HashMap::new(),
            admin_password,
            server_version,
            metrics,
            inspector_authenticated: false,
            auth_validator,
            tokio_handle,
            global_connections,
            connection_count,
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
    fn ensure_cvr(&mut self, allow_create: bool) -> bool {
        if self.cvr.is_some() {
            return true;
        }
        if self.cvr_pg {
            match self.sync_engine.load_cvr(now_ms() as f64) {
                Ok(cvr) => self.cvr = cvr,
                Err(e) => tracing::warn!("CG {}: load_cvr failed: {e}", self.cg_id),
            }
        }
        if self.cvr.is_none() && allow_create {
            self.cvr = Some(empty_cvr(&self.cg_id, &self.replica_version));
        }
        match &self.cvr {
            Some(cvr) => {
                self.ttl_clock = cvr.ttl_clock;
                self.ttl_clock_base = now_ms();
                true
            }
            None => false,
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
    fn on_expiry_tick(&mut self) {
        let Some(cvr) = self.cvr.take() else {
            return;
        };
        let now = now_ms();
        let ttl_clock = self.get_ttl_clock(now);
        let client_ids: Vec<String> = self.registered_ws.values().cloned().collect();
        let existing_rows = self.sync_engine.existing_rows();
        match self.sync_engine.remove_expired_queries(
            cvr,
            &client_ids,
            &existing_rows,
            now,
            now,
            ttl_clock,
        ) {
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

    fn on_new_connection(&mut self, params: ConnectParams, sink: DirectWebSocketSink) {
        let client_id = params.client_id.clone();
        let ws_id = params.ws_id.clone();
        let protocol_version = params.protocol_version;
        let client_group_id = params.client_group_id.clone();

        // Close any prior connection for this clientID before installing the new
        // one. Otherwise the previous ws_id's ClientHandler stays registered in
        // the SyncEngine — it keeps receiving pokes and its socket is never
        // closed, so a stale connection can go on emitting under the same
        // clientID. TS closes the superseded connection when a client reconnects.
        if let Some(prev_ws_id) = self.registered_ws.get(&client_id).cloned() {
            if prev_ws_id != ws_id {
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
        self.registered_ws.insert(client_id.clone(), ws_id.clone());

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
        let conns = self.global_connections.clone();
        let on_close = Box::new(move || {
            conns.lock().unwrap().remove(&cid);
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
            self.connection_count.fetch_sub(1, Ordering::Relaxed);
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
            self.handle_desired_queries(&client_id, &body, true);
        }
    }

    fn on_inbound(&mut self, client_id: String, text: String) {
        // Intercept desired-query messages and route them to the CG-owned
        // SyncEngine — the placeholder `ViewSyncerDispatch` can't reach the
        // `!Send` engine. Everything else (ping, etc.) goes through Connection.
        if let Ok(serde_json::Value::Array(arr)) = serde_json::from_str::<serde_json::Value>(&text)
        {
            if let Some(tag) = arr.first().and_then(|v| v.as_str()) {
                if tag == "initConnection" || tag == "changeDesiredQueries" {
                    if let Some(body) = arr.get(1) {
                        self.handle_desired_queries(&client_id, body, tag == "initConnection");
                    }
                    return;
                }
                if tag == "deleteClients" {
                    // `["deleteClients", {clientIDs, clientGroupIDs}]` — an
                    // explicit client-requested deletion (acked).
                    if let Some(body) = arr.get(1) {
                        let del_ids = str_array(body.get("clientIDs"));
                        let group_ids = str_array(body.get("clientGroupIDs"));
                        self.apply_client_deletions(&client_id, None, &del_ids, &group_ids);
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
                    self.handle_update_auth(&client_id, token);
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
        }
        let closed = match self.connections.get(&client_id) {
            Some(conn) => !conn.handle_inbound(&text),
            None => return,
        };
        if closed {
            self.on_connection_closed(client_id);
        }
    }

    /// Route a client's `initConnection` / `changeDesiredQueries` body to the
    /// SyncEngine: record desired queries and hydrate. Loads/creates the group
    /// CVR on first use. (Part 2 — functional cut; see `config_and_hydrate`.)
    fn handle_desired_queries(&mut self, client_id: &str, body: &serde_json::Value, is_init: bool) {
        let Some(ws_id) = self.registered_ws.get(client_id).cloned() else {
            tracing::warn!(
                "CG {}: desired queries for unregistered client {client_id}",
                self.cg_id
            );
            return;
        };
        let (puts, dels, clear) = parse_desired_queries_patch(body);
        let client_schema = body.get("clientSchema").cloned();
        // Capture the custom-query API context from an `initConnection` that
        // carries a `userQueryURL` (named queries are resolved against it). The
        // context persists for the connection's lifetime (a later
        // `changeDesiredQueries` doesn't re-send the URL).
        if let Some(url) = body.get("userQueryURL").and_then(|v| v.as_str()) {
            if !url.is_empty() {
                let headers = body
                    .get("userQueryHeaders")
                    .and_then(|v| v.as_object())
                    .map(|m| {
                        m.iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                self.client_query_ctx.insert(
                    client_id.to_string(),
                    CustomQueryContext {
                        url: url.to_string(),
                        headers,
                        auth: self.client_raw_auth.get(client_id).cloned(),
                    },
                );
            }
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
        self.ensure_cvr(true);

        // Query-config pass (records the client + desired queries, hydrates,
        // then catches the client up). Always runs on initConnection.
        let mut config_accepted = false;
        if is_init || has_query_change {
            let cvr = self.cvr.take().unwrap();
            let state_version = cvr.version.state_version.clone();
            let replica_version = cvr.replica_version.clone().unwrap_or_default();
            // The rows the client already has (from the CVR row cache).
            let existing_rows = self.sync_engine.existing_rows();
            // The client's decoded JWT claims (`authData` for permission rules).
            let auth_data = self
                .client_auth
                .get(client_id)
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let now = now_ms();
            let ttl_clock = self.get_ttl_clock(now);
            match self.sync_engine.config_and_hydrate(
                cvr,
                client_id,
                &[ws_id],
                &self.shard,
                puts,
                dels,
                clear,
                client_schema,
                self.permissions.as_ref(),
                &auth_data,
                self.client_query_ctx.get(client_id),
                state_version,
                replica_version,
                &existing_rows,
                now,
                now,
                ttl_clock,
            ) {
                Ok(cvr) => {
                    self.cvr = Some(cvr);
                    config_accepted = true;
                    crate::metrics::Metrics::inc(&self.metrics.hydrations);
                }
                Err(e) => tracing::warn!("CG {}: config_and_hydrate failed: {e}", self.cg_id),
            }
        }

        // On an accepted `initConnection`, run the Pusher init side effect (TS
        // calls `pusher.initConnection(...)` only after the ViewSyncer stream
        // started). Also intercepted-away from the message handler, so it fires
        // here. No-op when no Pusher is configured (mutations forwarded in TS).
        if is_init && config_accepted {
            if let Some(pusher) = &self.pusher {
                pusher.init_connection(&selector);
            }
        }

        // Client-deletion pass (activeClients GC + explicit `deleted`).
        if has_deletions {
            self.apply_client_deletions(
                client_id,
                active_clients.as_deref(),
                &deleted_ids,
                &deleted_groups,
            );
        }
    }

    /// Handle an `updateAuth` message: re-verify the new credential and, if the
    /// resolved auth data changed, re-transform every query for the client group.
    /// Port of TS `ViewSyncer.updateAuth` (+ `ConnectionContextManager` auth
    /// revision tracking): unchanged auth is a no-op; changed auth re-runs the
    /// config/hydrate pass, which recomputes each query's read-permission
    /// transform against the new `authData` and re-hydrates the pipelines whose
    /// transformation hash drifted.
    fn handle_update_auth(&mut self, client_id: &str, token: &str) {
        if token.is_empty() {
            return;
        }
        // Decode the new claims (unverified) — used both to compare against the
        // stored auth data and to extract the `sub` for signature verification.
        let new_claims = crate::auth::decode_jwt_claims(token);
        let user_id = new_claims
            .get("sub")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Re-verify the token signature with the same validator as the handshake.
        let validator = self.auth_validator.clone();
        let cg_id = self.cg_id.clone();
        let cid = client_id.to_string();
        let verify = self.tokio_handle.block_on(async move {
            validator
                .validate_auth(&cg_id, &cid, user_id.as_deref(), Some(token))
                .await
        });
        if let Err(error_body) = verify {
            tracing::warn!(
                "CG {}: updateAuth verification failed for client {client_id}",
                self.cg_id
            );
            if let Some(conn) = self.connections.get(client_id) {
                conn.close_with_error(error_body);
            }
            self.on_connection_closed(client_id.to_string());
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
        self.handle_desired_queries(client_id, &empty_body, true);
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
    fn apply_client_deletions(
        &mut self,
        caller_client_id: &str,
        active_clients: Option<&[String]>,
        deleted_client_ids: &[String],
        deleted_group_ids: &[String],
    ) {
        self.ensure_cvr(true);

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
        match self.sync_engine.delete_clients(
            cvr,
            &self.shard,
            &delete_ids,
            &ack_ids,
            deleted_group_ids,
            &poke_ws,
            now,
            now,
            ttl_clock,
        ) {
            Ok(cvr) => {
                self.cvr = Some(cvr);
                crate::metrics::Metrics::add(
                    &self.metrics.client_deletions,
                    delete_ids.len() as u64,
                );
            }
            Err(e) => tracing::warn!("CG {}: delete_clients failed: {e}", self.cg_id),
        }
        // NOTE: mutations are HTTP-direct (task 8), so there is no pusher here
        // to run `pusher.delete_client_mutations` — mutation cleanup happens on
        // the TS mutation path, not in the Rust syncer.
    }

    fn on_connection_closed(&mut self, client_id: String) {
        if self.connections.remove(&client_id).is_some() {
            self.connection_count.fetch_sub(1, Ordering::Relaxed);
        }
        if let Some(ws_id) = self.registered_ws.remove(&client_id) {
            self.sync_engine.unregister_client(&ws_id);
        }
        self.client_auth.remove(&client_id);
        self.client_raw_auth.remove(&client_id);
        self.client_query_ctx.remove(&client_id);
        self.global_connections.lock().unwrap().remove(&client_id);
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
    fn on_notification(&mut self) {
        // A notification can only advance an existing CVR (no create): without a
        // loaded CVR there is nothing to advance.
        if !self.ensure_cvr(false) {
            tracing::debug!(
                "CG {}: notification with no CVR loaded; skipping advance",
                self.cg_id
            );
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
            self.reset_pipelines_and_rehydrate(cvr, "read-permissions changed");
            return;
        }
        let cvr = self.cvr.take().unwrap();

        let client_ids: Vec<String> = self.registered_ws.values().cloned().collect();
        let existing_rows = self.sync_engine.existing_rows();
        let now = now_ms();
        let ttl_clock = self.get_ttl_clock(now);
        match self.sync_engine.advance_and_sync(
            cvr,
            self.replica_version.clone(),
            &client_ids,
            &existing_rows,
            now,
            now,
            ttl_clock,
        ) {
            Ok(result) => {
                crate::metrics::Metrics::inc(&self.metrics.advances);
                if let Some(reason) = result.reset_reason.clone() {
                    // The engine could not advance in place (snapshot/schema
                    // drift). Port of TS `ResetPipelinesSignal` handling: the
                    // in-flight poke was already cancelled; re-init the pipeline
                    // and re-hydrate every query from scratch.
                    crate::metrics::Metrics::inc(&self.metrics.resets);
                    self.reset_pipelines_and_rehydrate(result.cvr, &reason);
                } else {
                    self.cvr = Some(result.cvr);
                }
            }
            Err(e) => tracing::warn!("CG {}: advance_and_sync failed: {e}", self.cg_id),
        }
    }

    /// Re-initialize the IVM pipeline from a fresh replica snapshot and
    /// re-hydrate every query currently in the CVR. Port of the reset branch in
    /// TS `#syncQueryPipelines`: `#pipelines.reset()` then re-run the query
    /// pipeline set. Called when `advance_and_sync` reports a reset.
    fn reset_pipelines_and_rehydrate(&mut self, cvr: CVR, reason: &str) {
        tracing::warn!(
            "CG {}: pipeline reset ({reason}); re-initializing engine + rehydrating",
            self.cg_id
        );
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
            // Nothing more we can safely do; keep the CVR so state isn't lost.
            self.cvr = Some(cvr);
            return;
        }
        self.replica_version = self
            .sync_engine
            .pipelines()
            .current_version()
            .unwrap_or_default();

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
            let state_version = cvr.version.state_version.clone();
            let replica_version = cvr.replica_version.clone().unwrap_or_default();
            let existing_rows = self.sync_engine.existing_rows();
            let auth_data = self
                .client_auth
                .get(&client_id)
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let ttl_clock = self.get_ttl_clock(now);
            // Clone the CVR into the call so a failure doesn't consume it.
            match self.sync_engine.config_and_hydrate(
                cvr.clone(),
                &client_id,
                &[ws_id],
                &self.shard,
                Vec::new(),
                Vec::new(),
                false,
                None,
                self.permissions.as_ref(),
                &auth_data,
                self.client_query_ctx.get(&client_id),
                state_version,
                replica_version,
                &existing_rows,
                now,
                now,
                ttl_clock,
            ) {
                Ok(c) => cvr = c,
                Err(e) => {
                    tracing::warn!("CG {}: rehydrate after reset failed: {e}", self.cg_id)
                }
            }
        }
        self.cvr = Some(cvr);
    }

    fn drop_registration(&mut self, client_id: &str, ws_id: &str) {
        self.registered_ws.remove(client_id);
        self.sync_engine.unregister_client(ws_id);
    }

    fn shutdown(&mut self) {
        // Draining: tell each client to reconnect (elsewhere) with a Rehome
        // error, mirroring TS `#cleanup`'s `client.fail(Rehome "Reconnect
        // required")`, rather than a silent close. The client library treats
        // Rehome as "reconnect to another instance".
        for (_, conn) in self.connections.drain() {
            conn.close_with_error(crate::protocol::ErrorBody::rehome("Reconnect required"));
            self.connection_count.fetch_sub(1, Ordering::Relaxed);
        }
        self.registered_ws.clear();
    }
}

/// Run the CG thread: a single-threaded, non-blocking event loop over the
/// unified [`CGMessage`] channel. Owns the [`SyncEngine`]; drives connection
/// setup, inbound frames, disconnects, and change-streamer notifications.
fn run_cg_thread(
    cg_id: &str,
    rx: crossbeam_channel::Receiver<CGMessage>,
    services_factory: &Arc<dyn CGServicesFactory>,
    auth_validator: Arc<dyn AuthValidator>,
    connections: &Arc<Mutex<HashMap<String, ConnectionInfo>>>,
    connection_count: Arc<AtomicU64>,
) {
    tracing::info!("CG thread started: {cg_id}");
    let mut state = CgState::new(
        cg_id,
        services_factory,
        auth_validator,
        connections.clone(),
        connection_count,
    );

    // Event loop: block on the next message, but wake early when a query's TTL
    // is due so expired queries are evicted + poked (TS `#scheduleExpireEviction`
    // / `#removeExpiredQueries`). With nothing pending we block indefinitely.
    loop {
        let msg = match state.next_expiry_delay() {
            Some(delay) => match rx.recv_timeout(delay) {
                Ok(msg) => msg,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    state.on_expiry_tick();
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            },
            None => match rx.recv() {
                Ok(msg) => msg,
                Err(_) => break,
            },
        };
        match msg {
            CGMessage::NewConnection { params, sink } => state.on_new_connection(params, sink),
            CGMessage::Inbound { client_id, text } => state.on_inbound(client_id, text),
            CGMessage::ConnectionClosed { client_id } => state.on_connection_closed(client_id),
            CGMessage::Notification(_) => state.on_notification(),
            CGMessage::Shutdown => {
                tracing::info!("CG thread {cg_id}: shutting down");
                state.shutdown();
                break;
            }
        }
    }

    tracing::info!("CG thread exited: {cg_id}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_handler::{
        ConnContextInfo, ConnContextManagerDispatch, ConnectionSelector, ViewSyncerDispatch,
    };
    use crate::protocol::PROTOCOL_VERSION;
    use crate::ws_sink::{DirectWebSocketSink, WsCommand};

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
                tables: Vec::new(),
                replica_path: None,
                app_id: "zero".to_string(),
                shard: ShardID {
                    app_id: "zero".to_string(),
                    shard_num: 0,
                },
                cvr_pg: None,
                permissions: None,
                permissions_hash: None,
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
                tables: Vec::new(),
                replica_path: None, // in-memory (no PG, no replica)
                app_id: "zero".to_string(),
                shard: ShardID {
                    app_id: "zero".to_string(),
                    shard_num: 0,
                },
                cvr_pg: None,
                permissions: None,
                permissions_hash: None,
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
                tables: Vec::new(),
                replica_path: Some(self.replica_path.clone()),
                app_id: "zero".to_string(),
                shard: ShardID {
                    app_id: "zero".to_string(),
                    shard_num: 0,
                },
                cvr_pg: None,
                permissions: self.initial_permissions.clone(),
                permissions_hash: self.initial_hash.clone(),
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
        state.on_new_connection(test_params("c1", "ws1"), sink);

        // `connected` was pushed to the sink and the client is registered.
        let mut connected = false;
        while let Ok(cmd) = drx.try_recv() {
            if let WsCommand::Send(v) = cmd {
                if v[0] == "connected" {
                    connected = true;
                }
            }
        }
        assert!(connected, "expected a connected frame");
        assert_eq!(state.registered_ws.len(), 1);
        assert_eq!(state.connections.len(), 1);

        // Notification with no loaded CVR (no PG) is a graceful no-op.
        state.on_notification();

        // Disconnect unregisters the client.
        state.on_connection_closed("c1".to_string());
        assert_eq!(state.registered_ws.len(), 0);
        assert_eq!(state.connections.len(), 0);
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

        // First connection: client c1 on ws1.
        let (tx1, mut drx1) = tokio::sync::mpsc::channel::<WsCommand>(64);
        state.on_new_connection(test_params("c1", "ws1"), DirectWebSocketSink::new(tx1));
        while drx1.try_recv().is_ok() {} // drain ws1's `connected` frame

        // Reconnect: same client c1 on a NEW ws2.
        let (tx2, _drx2) = tokio::sync::mpsc::channel::<WsCommand>(64);
        state.on_new_connection(test_params("c1", "ws2"), DirectWebSocketSink::new(tx2));

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
        state.on_new_connection(test_params("c1", "ws1"), sink);

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
        let mut params = test_params("c1", "ws1");
        // Piggyback an initConnection carrying an empty desired-queries patch.
        params.init_connection_msg = Some(
            serde_json::from_value(serde_json::json!([
                "initConnection",
                {"desiredQueriesPatch": []}
            ]))
            .unwrap(),
        );
        state.on_new_connection(params, sink);

        assert_eq!(
            init_calls.load(Ordering::SeqCst),
            1,
            "ccm.init_connection should fire once on initConnection"
        );
        // The initConnection hydrated the client's (internal) queries, so the
        // hot-path `hydrations` metric incremented.
        assert_eq!(state.metrics.snapshot()["hydrations"], 1);
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
        state.on_new_connection(test_params("c1", "ws1"), sink);

        let drain = |drx: &mut tokio::sync::mpsc::Receiver<WsCommand>| -> Vec<serde_json::Value> {
            let mut v = Vec::new();
            while let Ok(WsCommand::Send(m)) = drx.try_recv() {
                v.push(m);
            }
            v
        };
        let _ = drain(&mut drx); // discard the `connected` frame

        // 1) `version` before authenticating → challenge (authenticated:false).
        state.on_inbound(
            "c1".to_string(),
            r#"["inspect",{"op":"version","id":"1"}]"#.to_string(),
        );
        let frames = drain(&mut drx);
        let last = frames.last().unwrap();
        assert_eq!(last[0], "inspect");
        assert_eq!(last[1]["op"], "authenticated");
        assert_eq!(last[1]["value"], false);

        // 2) authenticate with the wrong password → false.
        state.on_inbound(
            "c1".to_string(),
            r#"["inspect",{"op":"authenticate","id":"2","value":"nope"}]"#.to_string(),
        );
        assert_eq!(drain(&mut drx).last().unwrap()[1]["value"], false);
        assert!(!state.inspector_authenticated);

        // 3) authenticate with the right password → true.
        state.on_inbound(
            "c1".to_string(),
            r#"["inspect",{"op":"authenticate","id":"3","value":"s3cret"}]"#.to_string(),
        );
        assert_eq!(drain(&mut drx).last().unwrap()[1]["value"], true);
        assert!(state.inspector_authenticated);

        // 4) `version` now returns the configured server version.
        state.on_inbound(
            "c1".to_string(),
            r#"["inspect",{"op":"version","id":"4"}]"#.to_string(),
        );
        let last = drain(&mut drx).into_iter().next_back().unwrap();
        assert_eq!(last[1]["op"], "version");
        assert_eq!(last[1]["value"], "9.9.9");
    }
}
