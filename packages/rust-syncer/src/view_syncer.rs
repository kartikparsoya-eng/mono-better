//! ViewSyncer — port of `view-syncer.ts` (~2940 LOC).
//!
//! The core dispatch loop for a single client group. Runs on the CG thread.
//! No lock needed — the CG thread is single-threaded. stateChanges is a
//! channel. CVR load is synchronous (block_on or pre-loaded).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_channel::Receiver;

use crate::connection_context::{
    Auth, CCMError, ConnectionContext, ConnectionContextManager,
    ConnectionSelector, ConnectionValidation, GroupAuthState,
    MaintenanceKind, MaintenancePlan,
};
use crate::drain::DrainCoordinator;
use crate::protocol::ErrorBody;

// ─── CVR types (mirrors of rust-cvr types) ─────────────────────────────────

/// CVR version — mirrors `CVRVersion` from `rust-cvr/src/types.rs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CVRVersion {
    pub state_version: String,
    pub config_version: u32,
    pub patch_version: u32,
}

/// CVR snapshot — mirrors `CVRSnapshot` from `rust-cvr/src/types.rs`.
#[derive(Debug, Clone, Default)]
pub struct CVRSnapshot {
    pub version: CVRVersion,
    pub client_schema: Option<serde_json::Value>,
    pub replica_version: Option<String>,
    pub ttl_clock: i64,
    pub queries: HashMap<String, CVRQuery>,
}

#[derive(Debug, Clone, Default)]
pub struct CVRQuery {
    pub id: String,
    pub hash: u64,
    pub row_set_signature: Option<String>,
    pub internal: bool,
    pub ttl: Option<i64>,
    pub deactivated_at: Option<i64>,
}

/// What kind of transform to apply during sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransformMode {
    All,
    Missing,
}

/// Result of advance pipelines.
#[derive(Debug)]
pub enum AdvanceResult {
    Success,
    ResetPipelines { reason: String, message: String },
}

/// State version notification from the change streamer.
#[derive(Debug, Clone)]
pub struct AdvanceNotification {
    pub state: String, // "version-ready"
}

// ─── Pipeline driver trait ─────────────────────────────────────────────────

/// Abstracts the IVM engine operations.
/// In the full Rust syncer, this is implemented by the rust-ivm Engine.
pub trait PipelineDriver: Send + Sync {
    /// Whether pipelines have been initialized with a client schema.
    fn initialized(&self) -> bool;

    /// Initialize pipelines with the given client schema.
    fn init(&self, client_schema: &serde_json::Value);

    /// Reset pipelines (e.g., after a ResetPipelinesSignal).
    fn reset(&self, client_schema: &serde_json::Value);

    /// Advance the snapshot without computing a diff. Returns the new version.
    fn advance_without_diff(&self) -> String;

    /// Get the current replica version.
    fn replica_version(&self) -> Option<String>;

    /// Hydrate and sync — the main hydrate path.
    /// Delegates to `engine.hydrateAndSync()`.
    fn hydrate_and_sync(&self, params: &HydrateParams) -> Result<HydrateResult, ErrorBody>;

    /// Advance and sync — the main advance path.
    /// Delegates to `engine.advanceAndSync()`.
    fn advance_and_sync(&self, params: &AdvanceParams) -> Result<AdvanceSyncResult, ErrorBody>;

    /// Destroy pipelines.
    fn destroy(&self);

    /// Get the row set signature for a query.
    fn row_set_signature(&self, query_id: &str) -> Option<String>;
}

/// Parameters for hydrate_and_sync.
#[derive(Debug, Clone, Default)]
pub struct HydrateParams {
    pub client_id: String,
    pub queries_to_add: Vec<serde_json::Value>,
    pub queries_to_remove: Vec<String>,
    pub drifted_query_ids: Vec<String>,
    pub transform_mode: TransformMode,
    pub auth: Option<Auth>,
    pub connection_selector: ConnectionSelector,
    pub cvr_version: CVRVersion,
    pub ttl_clock: i64,
}

impl Default for TransformMode {
    fn default() -> Self {
        TransformMode::Missing
    }
}

impl Default for ConnectionSelector {
    fn default() -> Self {
        ConnectionSelector {
            client_id: String::new(),
            ws_id: String::new(),
        }
    }
}

/// Result of hydrate_and_sync.
#[derive(Debug, Clone, Default)]
pub struct HydrateResult {
    pub new_version: CVRVersion,
    pub patch_version: u32,
    pub query_patches: Vec<serde_json::Value>,
    pub row_patches: Vec<serde_json::Value>,
    pub hydration_count: u32,
    pub hydration_time_ms: u64,
    pub transaction_advance_time_ms: u64,
    pub per_query_metrics: HashMap<String, u64>,
}

/// Parameters for advance_and_sync.
#[derive(Debug, Clone, Default)]
pub struct AdvanceParams {
    pub client_id: String,
    pub auth: Option<Auth>,
    pub connection_selector: ConnectionSelector,
    pub cvr_version: CVRVersion,
    pub ttl_clock: i64,
}

/// Result of advance_and_sync.
#[derive(Debug, Clone, Default)]
pub struct AdvanceSyncResult {
    pub new_version: CVRVersion,
    pub patch_version: u32,
    pub query_patches: Vec<serde_json::Value>,
    pub row_patches: Vec<serde_json::Value>,
    pub transaction_advance_time_ms: u64,
}

// ─── CVR store trait ───────────────────────────────────────────────────────

/// Abstracts CVR store operations.
/// In the full Rust syncer, this is `CVRStoreHandle` from rust-cvr.
pub trait CVRStoreOps: Send + Sync {
    /// Load the CVR snapshot from the database.
    fn load(&self, last_connect_time: i64) -> Result<CVRSnapshot, ErrorBody>;

    /// Update the TTL clock in the CVR store (fire-and-forget).
    fn update_ttl_clock(&self, ttl_clock: i64, now: i64);

    /// Check if the store has flushed all pending writes.
    fn flushed(&self) -> bool;

    /// Wait for all pending writes to flush.
    fn wait_flushed(&self) -> Result<(), ErrorBody>;
}

// ─── Inspector delegate trait ──────────────────────────────────────────────

/// Abstracts inspector operations.
pub trait InspectorDelegate: Send + Sync {
    fn add_query(&self, query_id: &str, ast: &serde_json::Value);
    fn remove_query(&self, query_id: &str);
    fn is_authenticated(&self, id: &str) -> bool;
}

// ─── TTL Clock ─────────────────────────────────────────────────────────────

const TTL_CLOCK_INTERVAL_MS: i64 = 60_000;

/// TTL clock — mirrors `ttl-clock.ts` behavior.
/// Must be synchronous (no async).
pub struct TTLClock {
    clock: AtomicI64,
    base: AtomicI64,
}

impl TTLClock {
    pub fn new() -> Self {
        Self {
            clock: AtomicI64::new(0),
            base: AtomicI64::new(0),
        }
    }

    pub fn init(&self, ttl_clock: i64, now: i64) {
        self.clock.store(ttl_clock, Ordering::SeqCst);
        self.base.store(now, Ordering::SeqCst);
    }

    /// Get the current TTL clock value. Must be synchronous.
    /// Computes `delta = now - base`, `clock += delta`, `base = now`.
    pub fn get(&self, now: i64) -> i64 {
        let base = self.base.load(Ordering::SeqCst);
        let delta = now - base;
        let new_clock = self.clock.fetch_add(delta, Ordering::SeqCst) + delta;
        self.base.store(now, Ordering::SeqCst);
        debug_assert!(new_clock <= now, "ttlClock should be <= now");
        new_clock
    }

    pub fn value(&self) -> i64 {
        self.clock.load(Ordering::SeqCst)
    }
}

impl Default for TTLClock {
    fn default() -> Self {
        Self::new()
    }
}

// ─── RustViewSyncer ────────────────────────────────────────────────────────

const DEFAULT_KEEPALIVE_MS: i64 = 5_000;
const MAX_TTL_MS: i64 = 5_000_000;

/// The core ViewSyncer for a single client group.
/// Runs on the CG thread — no locks needed.
pub struct RustViewSyncer {
    pub id: String,
    pub shard: String,

    // Services (injected)
    pipelines: Arc<dyn PipelineDriver>,
    cvr_store: Arc<dyn CVRStoreOps>,
    conn_context_manager: ConnectionContextManager,
    drain_coordinator: DrainCoordinator,
    inspector_delegate: Option<Arc<dyn InspectorDelegate>>,

    // State
    cvr: Option<CVRSnapshot>,
    pipelines_synced: bool,
    ttl_clock: TTLClock,
    keep_alive_until: AtomicI64,
    initialized: AtomicBool,
    stopped: AtomicBool,
    active_clients: AtomicU32,

    // Channel for advance notifications from change streamer
    state_changes_rx: Receiver<AdvanceNotification>,
}

impl RustViewSyncer {
    pub fn new(
        id: String,
        shard: String,
        pipelines: Arc<dyn PipelineDriver>,
        cvr_store: Arc<dyn CVRStoreOps>,
        conn_context_manager: ConnectionContextManager,
        drain_coordinator: DrainCoordinator,
        inspector_delegate: Option<Arc<dyn InspectorDelegate>>,
        state_changes_rx: Receiver<AdvanceNotification>,
    ) -> Self {
        let now = now_ms();
        Self {
            id,
            shard,
            pipelines,
            cvr_store,
            conn_context_manager,
            drain_coordinator,
            inspector_delegate,
            cvr: None,
            pipelines_synced: false,
            ttl_clock: TTLClock::new(),
            keep_alive_until: AtomicI64::new(now + DEFAULT_KEEPALIVE_MS),
            initialized: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            active_clients: AtomicU32::new(0),
            state_changes_rx,
        }
    }

    /// Keepalive — extends the shutdown deadline.
    pub fn keepalive(&self) -> bool {
        let now = now_ms();
        self.keep_alive_until
            .store(now + DEFAULT_KEEPALIVE_MS, Ordering::SeqCst);
        true
    }

    /// Check if the syncer is initialized.
    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }

    /// Set initialized flag (for testing).
    pub fn set_initialized(&self, val: bool) {
        self.initialized.store(val, Ordering::SeqCst);
    }

    /// Check if the syncer is stopped.
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    // ─── Main dispatch loop ────────────────────────────────────────────────

    /// Main run loop. Processes state changes from the change streamer.
    /// Port of `view-syncer.ts` `run()`.
    pub fn run(&mut self) {
        // Wait for initialization
        if !self.initialized.load(Ordering::SeqCst) {
            // Check if draining before initialization
            if self.drain_coordinator.should_drain() {
                tracing::debug!("draining view-syncer {} before running", self.id);
                self.stop();
                return;
            }
            // Block until initialized or draining
            // In TS: Promise.race([#initialized.promise, drainCoordinator.draining])
            // In Rust: we can't block on a future on the CG thread without a runtime.
            // The init happens via init_connection() which sets the flag.
            // We'll spin-wait with a yield.
            while !self.initialized.load(Ordering::SeqCst) {
                if self.drain_coordinator.should_drain() {
                    self.stop();
                    return;
                }
                // Check for incoming messages while waiting
                match self.state_changes_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                    Ok(_) => {
                        // Process state change even before initialization
                        // (shouldn't normally happen)
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                        tracing::info!("state changes channel closed, stopping");
                        self.stop();
                        return;
                    }
                }
            }
        }

        // Main loop: process state changes
        while let Ok(notification) = self.state_changes_rx.recv() {
            if self.drain_coordinator.should_drain() {
                tracing::debug!("draining view-syncer {} (elective)", self.id);
                break;
            }

            debug_assert_eq!(notification.state, "version-ready");

            if let Err(e) = self.run_in_lock_with_cvr() {
                tracing::error!("error in run loop: {:?}", e);
                self.cvr = None;
                break;
            }
        }

        // Post-loop: handle drain or shutdown
        if self.drain_coordinator.should_drain() {
            self.drain_coordinator
                .drain_next_in(self.total_hydration_time_ms());
        }

        self.cleanup(None);
    }

    /// Port of `#runInLockWithCVR`. No lock needed — CG thread is single-threaded.
    fn run_in_lock_with_cvr(&mut self) -> Result<(), ErrorBody> {
        // Check if state changes are still active
        if self.stopped.load(Ordering::SeqCst) {
            return Err(ErrorBody::basic(
                crate::protocol::ErrorKind::Rehome,
                "Reconnect required".to_string(),
            ));
        }

        // Check shutdown conditions
        if self.check_for_shutdown_conditions() {
            tracing::info!("closing clientGroupID={}", self.id);
            self.initialized.store(false, Ordering::SeqCst);
            self.stopped.store(true, Ordering::SeqCst);
            return Err(ErrorBody::basic(
                crate::protocol::ErrorKind::Rehome,
                "Reconnect required".to_string(),
            ));
        }

        // Load CVR if not cached
        if self.cvr.is_none() {
            tracing::debug!("loading cvr");
            let last_connect_time = 0i64; // TODO: track last connect time
            let cvr = self.cvr_store.load(last_connect_time)?;
            self.ttl_clock.init(cvr.ttl_clock, now_ms());
            self.cvr = Some(cvr);
        } else {
            // Update TTL clock
            let now = now_ms();
            if let Some(ref mut cvr) = self.cvr {
                cvr.ttl_clock = self.ttl_clock.get(now);
            }
        }

        // Execute the main logic
        let result = self.process_state_change();

        // Schedule auth maintenance (always, in finally block of TS)
        self.schedule_auth_maintenance();

        result
    }

    /// Port of the inner fn of `#runInLockWithCVR` in `run()`.
    fn process_state_change(&mut self) -> Result<(), ErrorBody> {
        let cvr = self.cvr.as_ref().unwrap().clone();
        let client_schema = cvr.client_schema.as_ref().ok_or_else(|| {
            ErrorBody::internal("cvr.clientSchema missing after initialization")
        })?;

        // Initialize pipelines if needed
        if !self.pipelines.initialized() {
            self.pipelines.init(client_schema);
        }

        // Check replica version
        if cvr.replica_version.is_some()
            && cvr.version.state_version != "00"
            && self.pipelines.replica_version() < cvr.replica_version
        {
            let message = format!(
                "Cannot sync from older replica: CVR={}, DB={:?}",
                cvr.replica_version.as_ref().unwrap(),
                self.pipelines.replica_version()
            );
            tracing::info!("resetting CVR: {}", message);
            return Err(ErrorBody::client_not_found(&message));
        }

        // If pipelines are synced, try to advance
        if self.pipelines_synced {
            match self.advance_pipelines(&cvr)? {
                AdvanceResult::Success => return Ok(()),
                AdvanceResult::ResetPipelines { reason, message } => {
                    tracing::info!("resetting pipelines: {}", message);
                    self.pipelines.reset(client_schema);
                    self.pipelines_synced = false;
                    self.conn_context_manager.set_shared_retransform_ready(false);
                }
            }
        }

        // Advance without diff
        let version = self.pipelines.advance_without_diff();
        let cvr_ver = &cvr.version.state_version;

        if version.as_str() < cvr_ver.as_str() {
            tracing::debug!("replica@{} is behind cvr@{}", version, cvr_ver);
            return Ok(()); // Wait for the next advancement
        }

        // Hydrate unchanged queries
        tracing::info!("init pipelines@{} (cvr@{})", version, cvr_ver);
        let drifted_query_ids = self.hydrate_unchanged_queries(&cvr)?;

        // Sync query pipeline set
        self.sync_query_pipeline_set(&cvr, TransformMode::Missing, None, &drifted_query_ids)?;

        self.pipelines_synced = true;
        self.conn_context_manager.set_shared_retransform_ready(true);

        Ok(())
    }

    // ─── Advance pipelines ─────────────────────────────────────────────────

    /// Port of `#advancePipelines`. Delegates to `engine.advanceAndSync()`.
    fn advance_pipelines(&self, cvr: &CVRSnapshot) -> Result<AdvanceResult, ErrorBody> {
        let now = now_ms();
        let ttl_clock = self.ttl_clock.get(now);

        let params = AdvanceParams {
            client_id: String::new(), // Not used for advance
            auth: None,               // TODO: get from background connection
            connection_selector: ConnectionSelector {
                client_id: String::new(),
                ws_id: String::new(),
            },
            cvr_version: cvr.version.clone(),
            ttl_clock,
        };

        match self.pipelines.advance_and_sync(&params) {
            Ok(result) => {
                // Update CVR version
                tracing::debug!(
                    "advance result: new_version={:?}, patches={}",
                    result.new_version,
                    result.row_patches.len()
                );
                Ok(AdvanceResult::Success)
            }
            Err(e) => Err(e),
        }
    }

    // ─── Hydrate unchanged queries ─────────────────────────────────────────

    /// Port of `#hydrateUnchangedQueries`.
    /// Runs at init when `pipelinesSynced === false`.
    fn hydrate_unchanged_queries(&self, cvr: &CVRSnapshot) -> Result<Vec<String>, ErrorBody> {
        let mut drifted_query_ids = Vec::new();

        let now = now_ms();
        let ttl_clock = self.ttl_clock.get(now);

        for (query_id, query) in &cvr.queries {
            // Skip if already in pipelines
            if self.pipelines.row_set_signature(query_id).is_some() {
                // Drift detection
                if let Some(ref cvr_sig) = query.row_set_signature {
                    if let Some(ref pipe_sig) = self.pipelines.row_set_signature(query_id) {
                        if cvr_sig != pipe_sig {
                            tracing::warn!(
                                "row set signature drift for query {}",
                                query_id
                            );
                            drifted_query_ids.push(query_id.clone());
                        }
                    }
                }
                continue;
            }

            // Hydrate this query
            let start = std::time::Instant::now();
            // In the full Rust syncer, this would call engine.hydrateAndSync
            // for each query. For now, we log.
            tracing::debug!("hydrating query {}", query_id);
            let elapsed = start.elapsed().as_millis();

            // Record metrics
            tracing::debug!(
                "hydrated query {} in {}ms",
                query_id,
                elapsed
            );

            // Inspector
            if let Some(ref inspector) = self.inspector_delegate {
                inspector.add_query(query_id, &serde_json::Value::Null);
            }
        }

        Ok(drifted_query_ids)
    }

    // ─── Sync query pipeline set ───────────────────────────────────────────

    /// Port of `#syncQueryPipelineSet`. The hydrate path.
    fn sync_query_pipeline_set(
        &self,
        cvr: &CVRSnapshot,
        transform_mode: TransformMode,
        _conn_ctx: Option<&ConnectionContext>,
        drifted_query_ids: &[String],
    ) -> Result<(), ErrorBody> {
        let now = now_ms();
        let ttl_clock = self.ttl_clock.get(now);

        // In the full Rust syncer, this delegates to engine.hydrateAndSync()
        let params = HydrateParams {
            client_id: String::new(),
            queries_to_add: Vec::new(),   // TODO: compute from CVR diff
            queries_to_remove: Vec::new(), // TODO: compute from CVR diff
            drifted_query_ids: drifted_query_ids.to_vec(),
            transform_mode,
            auth: None, // TODO: get from background connection
            connection_selector: ConnectionSelector {
                client_id: String::new(),
                ws_id: String::new(),
            },
            cvr_version: cvr.version.clone(),
            ttl_clock,
        };

        let result = self.pipelines.hydrate_and_sync(&params)?;

        // Record metrics
        tracing::debug!(
            "hydrate result: version={:?}, hydration_count={}, hydration_time={}ms",
            result.new_version,
            result.hydration_count,
            result.hydration_time_ms
        );

        Ok(())
    }

    // ─── Message handler implementations ───────────────────────────────────

    /// Port of `initConnection()`. Creates a ClientHandler and processes
    /// the init connection message.
    pub fn init_connection(
        &mut self,
        selector: &ConnectionSelector,
        msg: &serde_json::Value,
    ) -> Result<(), ErrorBody> {
        let conn_ctx = self
            .conn_context_manager
            .must_get_connection_context(selector)
            .map_err(|e| e.to_error_body())?;

        self.active_clients.fetch_add(1, Ordering::SeqCst);

        if self.active_clients.load(Ordering::SeqCst) == 1 {
            // First connection
            let now = now_ms();
            self.ttl_clock.init(0, now);
        }

        // Validate auth before sending any data
        if !self.validate_connection(&conn_ctx)? {
            return Ok(());
        }

        // Handle config update
        self.handle_config_update(
            &conn_ctx.client_id,
            msg,
            TransformMode::All,
            &conn_ctx,
        )?;

        // Signal initialization
        self.initialized.store(true, Ordering::SeqCst);
        self.keepalive();

        Ok(())
    }

    /// Port of `changeDesiredQueries()`.
    pub fn change_desired_queries(
        &mut self,
        selector: &ConnectionSelector,
        msg: &serde_json::Value,
    ) -> Result<(), ErrorBody> {
        let conn_ctx = self
            .conn_context_manager
            .must_get_connection_context(selector)
            .map_err(|e| e.to_error_body())?;

        self.handle_config_update(
            &conn_ctx.client_id,
            msg,
            TransformMode::Missing,
            &conn_ctx,
        )
    }

    /// Port of `updateAuth()`.
    pub fn update_auth(
        &mut self,
        selector: &ConnectionSelector,
        _msg: &serde_json::Value,
        auth_revision_changed: bool,
    ) -> Result<(), ErrorBody> {
        if !auth_revision_changed {
            tracing::debug!("Auth unchanged, skipping query re-transformation");
            return Ok(());
        }

        let conn_ctx = self
            .conn_context_manager
            .must_get_connection_context(selector)
            .map_err(|e| e.to_error_body())?;

        if !self.pipelines_synced {
            if !self.validate_connection(&conn_ctx)? {
                return Ok(());
            }
        }

        self.handle_config_update(
            &conn_ctx.client_id,
            &serde_json::json!({}),
            TransformMode::All,
            &conn_ctx,
        )
    }

    /// Port of `deleteClients()`.
    pub fn delete_clients(
        &mut self,
        selector: &ConnectionSelector,
        msg: &serde_json::Value,
    ) -> Result<Vec<String>, ErrorBody> {
        let conn_ctx = self
            .conn_context_manager
            .must_get_connection_context(selector)
            .map_err(|e| e.to_error_body())?;

        self.handle_config_update(
            &conn_ctx.client_id,
            msg,
            TransformMode::Missing,
            &conn_ctx,
        )?;

        // Return deleted client IDs
        // TODO: extract from handle_config_update result
        Ok(Vec::new())
    }

    /// Port of `inspect()`.
    pub fn inspect(
        &self,
        _selector: &ConnectionSelector,
        msg: &serde_json::Value,
    ) -> Result<serde_json::Value, ErrorBody> {
        if let Some(ref inspector) = self.inspector_delegate {
            // TODO: implement full inspect
            return Ok(serde_json::json!({
                "queries": [],
                "server": {},
            }));
        }
        Err(ErrorBody::internal("inspector not configured"))
    }

    // ─── Config update ─────────────────────────────────────────────────────

    /// Port of `#handleConfigUpdate`.
    fn handle_config_update(
        &mut self,
        client_id: &str,
        msg: &serde_json::Value,
        transform_mode: TransformMode,
        conn_ctx: &ConnectionContext,
    ) -> Result<(), ErrorBody> {
        // Load CVR if not cached
        if self.cvr.is_none() {
            let last_connect_time = 0i64;
            let cvr = self.cvr_store.load(last_connect_time)?;
            self.ttl_clock.init(cvr.ttl_clock, now_ms());
            self.cvr = Some(cvr);
        }

        let cvr = self.cvr.as_ref().unwrap().clone();

        // Process the config update
        // In TS: #updateCVRConfig with an updater callback
        // In Rust: we delegate to the CVR store directly

        // Extract desired queries patch from message
        if let Some(patch) = msg.get("desiredQueriesPatch") {
            tracing::debug!(
                "processing desiredQueriesPatch for client {}",
                client_id
            );
            // TODO: apply patch to CVR store
        }

        // Extract client schema if provided
        if let Some(schema) = msg.get("clientSchema") {
            tracing::debug!("setting client schema for client {}", client_id);
            // TODO: set client schema in CVR store
        }

        // Extract deleted clients if provided
        if let Some(deleted) = msg.get("deleted") {
            if let Some(client_ids) = deleted.get("clientIDs").and_then(|v| v.as_array()) {
                for cid in client_ids {
                    if let Some(cid_str) = cid.as_str() {
                        tracing::debug!("deleting client {}", cid_str);
                        // TODO: delete client from CVR store
                    }
                }
            }
        }

        // Sync query pipeline set if pipelines are synced
        if self.pipelines_synced {
            self.sync_query_pipeline_set(
                &cvr,
                transform_mode,
                Some(conn_ctx),
                &[],
            )?;
        }

        Ok(())
    }

    // ─── Auth maintenance ──────────────────────────────────────────────────

    /// Port of `#validateConnection`.
    fn validate_connection(&mut self, conn_ctx: &ConnectionContext) -> Result<bool, ErrorBody> {
        // In TS: calls customQueryTransformer.validate(connCtx) if configured
        // Then calls connContextManager.validateConnection(selector, revision, validation)

        let selector = ConnectionSelector {
            client_id: conn_ctx.client_id.clone(),
            ws_id: conn_ctx.ws_id.clone(),
        };

        match self.conn_context_manager.validate_connection(
            &selector,
            conn_ctx.revision,
            &ConnectionValidation::ClientFallback,
        ) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => {
                tracing::warn!("connection validation failed: {:?}", e);
                Err(e.to_error_body())
            }
        }
    }

    /// Port of `#scheduleAuthMaintenance`.
    fn schedule_auth_maintenance(&self) {
        let plan = self.conn_context_manager.plan_maintenance();
        if let Some(deadline) = plan.earliest_deadline_at {
            let now = now_ms();
            let delay = (deadline - now).max(0);
            tracing::debug!(
                "scheduling auth maintenance in {}ms (deadline={})",
                delay,
                deadline
            );
            // In TS: setTimeout(#runAuthMaintenance, delay)
            // In Rust: we can't set a timer on the CG thread without a runtime.
            // The CG thread checks plan_maintenance() on each iteration.
        }
    }

    // ─── Shutdown ──────────────────────────────────────────────────────────

    /// Port of `stop()`.
    pub fn stop(&mut self) {
        self.conn_context_manager.set_shared_retransform_ready(false);
        self.initialized.store(false, Ordering::SeqCst);
        self.stopped.store(true, Ordering::SeqCst);
    }

    /// Port of `#checkForShutdownConditionsInLock`.
    fn check_for_shutdown_conditions(&self) -> bool {
        if self.active_clients.load(Ordering::SeqCst) > 0 {
            return false;
        }

        // Wait for CVR store to flush
        if !self.cvr_store.flushed() {
            return false;
        }

        let now = now_ms();
        if now <= self.keep_alive_until.load(Ordering::SeqCst) {
            return false;
        }

        true
    }

    /// Port of `#cleanup`.
    fn cleanup(&mut self, _err: Option<&ErrorBody>) {
        self.conn_context_manager.set_shared_retransform_ready(false);
        self.pipelines.destroy();
        self.stopped.store(true, Ordering::SeqCst);
    }

    /// Total hydration time in ms (for drain coordinator).
    fn total_hydration_time_ms(&self) -> u64 {
        0 // TODO: track actual hydration time
    }
}

// ─── Helpers ───────────────────────────────────────────────────────────────

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Check if a query is expired.
/// A query is expired when ALL clients have `inactivatedAt` set AND
/// `inactivatedAt + clampTTL(ttl) <= ttlClock` for all.
/// Internal queries never expire.
pub fn is_expired(ttl_clock: i64, query: &CVRQuery) -> bool {
    if query.internal {
        return false;
    }
    let deactivated_at = match query.deactivated_at {
        Some(v) => v,
        None => return false,
    };
    let ttl = query.ttl.unwrap_or(0);
    let clamped_ttl = ttl.min(MAX_TTL_MS).max(0);
    deactivated_at + clamped_ttl <= ttl_clock
}

/// Check if the CVR has any expired queries.
pub fn has_expired_queries(cvr: &CVRSnapshot) -> bool {
    cvr.queries.values().any(|q| is_expired(cvr.ttl_clock, q))
}
