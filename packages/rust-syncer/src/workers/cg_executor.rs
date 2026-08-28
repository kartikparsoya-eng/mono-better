//! CG executor substrate — RUST-ONLY INVENTION (no TS twin; see
//! `parity/INVENTIONS.md` I-1/doc 91). TS gets per-client-group serialization
//! from the `ViewSyncerService` `#lock` on a worker's event loop; rust hosts
//! each (`!Send`) client-group task on one of `K` `current_thread` executor
//! threads (`LocalSet` + `spawn_local`), serialized by an unbounded ordered
//! channel (the `#lock` twin). This module holds ONLY the scheduling
//! machinery: the channel message type, the per-CG handle, the executor
//! threads, and the per-connection inbound forwarder. The SERVING LOOP that
//! runs on top of it (`cg_event_loop` → `ViewSyncerService::run`) lives with
//! the view-syncer, not here.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64};

use tokio::sync::mpsc;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;

use dashmap::DashMap;

// The serving loop (`cg_event_loop`, TS ViewSyncerService.run twin) and the
// service wiring it needs still live in router.rs until Stage 3b; the executor
// spawns it back through these crate-internal imports.
use crate::services::view_syncer::view_syncer::{AuthValidator, CGServicesFactory, cg_event_loop};
use crate::workers::connect_params::ConnectParams;
use crate::workers::syncer::ConnectionInfo;
use crate::ws_sink::DirectWebSocketSink;

/// Message sent to a CG thread's unified event loop.
///
/// All inputs — new connections, inbound WS frames, disconnects, change-streamer
/// notifications, shutdown — flow through this single channel so the CG thread
/// is a non-blocking single-threaded event loop (doc 89's CG dispatch model),
/// rather than blocking on one connection at a time.
pub enum CGMessage {
    /// A new connection was accepted — register its client handler with the
    /// SyncEngine. (`connected` is ALREADY sent by `handle_connection` on the
    /// accept task, before this message is enqueued — the 2026-08-27 connect-ack
    /// decoupling, task #152.)
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
    pub(crate) tx: mpsc::UnboundedSender<CGMessage>,
    /// Number of active connections on this CG.
    pub(crate) connection_count: Arc<AtomicU64>,
    pub(crate) accepting: Arc<AtomicBool>,
    /// Index into `Syncer::executors` of the executor hosting this CG.
    /// Fixed at placement for the group's lifetime (the `!Send` `SyncEngine` is
    /// pinned to that one thread). Read by `place_cg` to compute per-executor
    /// load; carried on returned/cloned handles only for struct consistency.
    pub(crate) executor_idx: usize,
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
    /// drain-join (see [`Syncer::shutdown`]) is what guarantees the task
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
pub(crate) enum ExecutorCommand {
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
pub(crate) struct Executor {
    pub(crate) ctrl_tx: mpsc::UnboundedSender<ExecutorCommand>,
    /// Joined once, during [`Syncer::shutdown`]. Behind a `Mutex<Option>`
    /// so `shutdown(&self)` can take ownership of the handle to join it.
    pub(crate) join: Mutex<Option<JoinHandle<()>>>,
    /// Set when a `SpawnCg` send finds the control channel closed — i.e. the
    /// executor thread died. A dead executor hosts 0 groups, so without this
    /// flag `place_cg` would rank it least-loaded FOREVER and every new client
    /// group process-wide would fail placement and rehome — a half-dead state
    /// invisible to the load balancer. Dead executors are excluded from
    /// placement; existing groups on other executors are unaffected.
    pub(crate) dead: AtomicBool,
}

/// Default executor count: one per available core, matching the design's
/// `K ≈ num_cores`. Falls back to 4 if the platform can't report parallelism.
pub(crate) fn default_num_shards() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .max(1)
}

/// Forwarder: bridges a connection's tokio inbound channel into the CG's
/// unified `tokio::sync::mpsc` channel, so the CG thread never blocks on a
/// single connection. Runs as a tokio task. Emits `ConnectionClosed` when the
/// WS ends.
pub(crate) async fn forward_inbound(
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

/// Run one executor thread (doc 91): a `current_thread` tokio runtime + `LocalSet`
/// that hosts a hash-shard of client groups as `spawn_local` tasks. The `!Send`
/// `SyncEngine` of each hosted group lives on this one thread and its IVM compute
/// runs inline; CVR/PG I/O is *offloaded* onto the shared-pool runtime (the
/// process's main multi-thread runtime) via `SyncEngine::offload`, so this
/// executor never blocks on Postgres and, crucially, the CVR connection budget is
/// ONE shared pool (not fragmented per executor) — every group can use any of the
/// pool's connections, matching TS's one-pool-per-worker behavior.
pub(crate) fn run_executor(
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
pub(crate) struct CgTaskContext {
    pub(crate) services_factory: Arc<dyn CGServicesFactory>,
    pub(crate) auth_validator: Arc<dyn AuthValidator>,
    pub(crate) connections: Arc<Mutex<HashMap<String, ConnectionInfo>>>,
    pub(crate) cvr_pool: Option<sqlx::PgPool>,
    pub(crate) serving_lag_registry: Arc<crate::workers::syncer::ServingLagRegistry>,
}
