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

use crate::connection::{Connection, MessageHandler};
use crate::message_handler::{
    ConnContextManagerDispatch, ConnContextInfo, ConnectionSelector,
    MutagenDispatch, PusherDispatch, SyncerWsMessageHandler, ViewSyncerDispatch,
};
use crate::ws_sink::DirectWebSocketSink;
use crate::ws_server::ConnectionContext;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// Message sent to a CG thread to control a connection.
pub enum CGMessage {
    /// A new connection has been accepted — start processing.
    NewConnection {
        ctx: ConnectionContext,
    },
    /// The CG should shut down (no more connections).
    Shutdown,
    /// Change-streamer notification — new data is available.
    Notification(serde_json::Value),
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

/// Factory trait for creating per-CG services.
///
/// In Phase 2, this is a trait that the full binary implements.
/// Phase 4 provides the real implementation with ViewSyncer + Engine.
pub trait CGServicesFactory: Send + Sync {
    /// Create the ViewSyncer dispatch for a new CG.
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
    ) -> Self {
        Self {
            cg_handles: DashMap::new(),
            services_factory,
            auth_validator,
            connections: Arc::new(Mutex::new(HashMap::new())),
            group_auth_states: Arc::new(Mutex::new(HashMap::new())),
            shutting_down: Arc::new(AtomicBool::new(false)),
        }
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

        // 2. Check user ID pinning.
        {
            let mut states = self.group_auth_states.lock().unwrap();
            let group = states.entry(client_group_id.clone()).or_default();
            if let Some(ref pinned) = group.pinned_user_id {
                let incoming = user_id.as_deref().unwrap_or("");
                if pinned.as_str() != incoming {
                    let error = crate::protocol::ErrorBody::unauthorized(
                        "Client groups are pinned to a single userID. \
                         Connection userID does not match existing client group userID.",
                    );
                    tracing::warn!(
                        "User ID mismatch: pinned={pinned}, incoming={incoming}"
                    );
                    ctx.sink.fail(error);
                    return;
                }
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

        // 5. Send the connection to the CG thread.
        if cg_handle.send(CGMessage::NewConnection { ctx }).is_err() {
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
        let connections = self.connections.clone();
        let cg_id = client_group_id.to_string();
        let conn_count = connection_count.clone();

        let handle = std::thread::Builder::new()
            .name(format!("cg-{cg_id}"))
            .spawn(move || {
                run_cg_thread(&cg_id, rx, &services_factory, &connections, conn_count);
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
}

/// Run the CG thread.
///
/// Each CG thread:
/// 1. Receives `NewConnection` messages from the router.
/// 2. Creates a `Connection` + `SyncerWsMessageHandler` for each.
/// 3. Runs the connection's message loop (receive from upstream channel,
///    dispatch, send downstream).
/// 4. Handles `Shutdown` to exit.
fn run_cg_thread(
    cg_id: &str,
    rx: crossbeam_channel::Receiver<CGMessage>,
    services_factory: &Arc<dyn CGServicesFactory>,
    connections: &Arc<Mutex<HashMap<String, ConnectionInfo>>>,
    connection_count: Arc<AtomicU64>,
) {
    tracing::info!("CG thread started: {cg_id}");

    // Create per-CG services.
    let view_syncer = services_factory.create_view_syncer(cg_id);
    let conn_context_manager = services_factory.create_conn_context_manager(cg_id);
    let mutagen = services_factory.create_mutagen(cg_id);
    let pusher = services_factory.create_pusher(cg_id);

    // Active connections on this CG: client_id → Connection.
    let mut active_connections: HashMap<String, Connection> = HashMap::new();

    loop {
        let msg = match rx.recv() {
            Ok(msg) => msg,
            Err(_) => {
                tracing::info!("CG thread {cg_id}: channel closed, exiting");
                break;
            }
        };

        match msg {
            CGMessage::NewConnection { ctx } => {
                let ConnectionContext {
                    params,
                    sink,
                    upstream_rx,
                } = ctx;

                let client_id = params.client_id.clone();
                let ws_id = params.ws_id.clone();
                let protocol_version = params.protocol_version;
                let client_group_id = params.client_group_id.clone();

                // Create the message handler.
                let handler = Box::new(SyncerWsMessageHandler::new(
                    view_syncer.clone(),
                    conn_context_manager.clone(),
                    mutagen.clone(),
                    pusher.clone(),
                    client_group_id.clone(),
                    client_id.clone(),
                    ws_id.clone(),
                ));

                // Connection close callback.
                let cid = client_id.clone();
                let conns = connections.clone();
                let on_close = Box::new(move || {
                    let mut c = conns.lock().unwrap();
                    c.remove(&cid);
                });

                // Create the connection.
                let conn = Connection::new(
                    sink,
                    protocol_version,
                    ws_id,
                    client_id.clone(),
                    client_group_id,
                    handler,
                    on_close,
                );

                // Init: send `connected` message, check protocol version.
                if !conn.init() {
                    // Protocol version mismatch — connection was closed.
                    connection_count.fetch_sub(1, Ordering::Relaxed);
                    continue;
                }

                // Handle piggybacked initConnection from sec-websocket-protocol.
                if let Some(ref init_msg) = params.init_connection_msg {
                    let init_json = serde_json::to_string(init_msg).unwrap_or_default();
                    tracing::debug!(
                        "handling init connection message from sec header: {client_id}"
                    );
                    if !conn.handle_init_connection(&init_json) {
                        // Connection was closed during init.
                        connection_count.fetch_sub(1, Ordering::Relaxed);
                        continue;
                    }
                }

                // Store the connection.
                active_connections.insert(client_id.clone(), conn);

                // Now process messages from the upstream channel.
                // We use blocking_recv on the tokio channel via a helper.
                // The CG thread blocks on receiving messages — this is the
                // main dispatch loop.
                process_connection_messages(cg_id, &mut active_connections, upstream_rx);
            }
            CGMessage::Shutdown => {
                tracing::info!("CG thread {cg_id}: shutting down");
                // Close all connections.
                for (_, conn) in active_connections.drain() {
                    conn.close("server shutting down");
                    connection_count.fetch_sub(1, Ordering::Relaxed);
                }
                break;
            }
            CGMessage::Notification(notification) => {
                tracing::debug!("CG thread {cg_id}: received notification: {}",
                    serde_json::to_string(&notification).unwrap_or_default());
                // Forward to the ViewSyncer's state changes channel.
                // In the full implementation, this calls view_syncer.run()
                // with the new state. For now, we log.
                // TODO: wire to RustViewSyncer's state_changes_rx
            }
        }
    }

    tracing::info!("CG thread exited: {cg_id}");
}

/// Process messages from the upstream channel for active connections.
///
/// In the current implementation, we process one connection at a time.
/// Each connection's upstream_rx is a tokio mpsc::Receiver — we need to
/// block on it from the CG thread.
fn process_connection_messages(
    cg_id: &str,
    active_connections: &mut HashMap<String, Connection>,
    upstream_rx: tokio::sync::mpsc::Receiver<String>,
) {
    // We need to block on the tokio receiver. The CG thread is a plain
    // OS thread, so we use a blocking call.
    //
    // In the TS code, messages are processed via a pipeline (stream).
    // In Rust, we block on the channel and dispatch each message.
    //
    // Note: this is a simplified version — the full implementation will
    // handle multiple connections concurrently on the same CG thread.
    // For now, we process messages from this connection until it closes.

    let mut rx = upstream_rx;

    // Use tokio's blocking_recv via a runtime handle.
    // The CG thread doesn't have a tokio runtime, so we create a new one
    // for blocking on the channel.
    // Actually, we should use the tokio handle from the main runtime.
    // For now, we use tokio::task::block_in_place which requires a runtime.
    // Instead, we'll use a crossbeam-based approach: convert the tokio
    // receiver to a blocking one.

    loop {
        // Block on the tokio channel from a non-tokio thread.
        match rx.blocking_recv() {
            Some(text) => {
                // Parse to find client_id — in the full implementation,
                // we'd route based on which connection the message is from.
                // For now, we process on the first active connection.
                if let Some(conn) = active_connections.values().next() {
                    if !conn.handle_inbound(&text) {
                        // Connection was closed.
                        break;
                    }
                } else {
                    // No active connections — drop the message.
                    break;
                }
            }
            None => {
                // Channel closed — WebSocket disconnected.
                break;
            }
        }
    }

    // Remove closed connections.
    let closed: Vec<String> = active_connections
        .iter()
        .filter(|(_, c)| c.is_closed())
        .map(|(k, _)| k.clone())
        .collect();
    for cid in closed {
        active_connections.remove(&cid);
    }
}

// We need Clone for the trait objects. Add a helper trait.
// Since Box<dyn Trait> is not Clone, we add a CloneableDispatch wrapper.
