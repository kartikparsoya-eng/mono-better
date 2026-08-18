//! SyncEngine — the combined engine + CVR hot path (Stage B of Phase 7).
//!
//! Port of the `CVRState` + `hydrate_and_sync` / `advance_and_sync` logic in
//! `rust-ivm/napi/src/lib.rs`, with the napi / TSFN / actor-thread machinery
//! stripped. It owns the [`IvmPipelines`] (Stage A) plus the CVR store handle
//! and the per-client `ClientHandler`s, and drives the flow:
//!
//!   engine `RowChange` → `ChangeProcessor::on_row_change` →
//!   `CVRQueryDrivenUpdater` → `MultiPoker` (poke frames) → `DirectWebSocketSink`
//!   → `CVRStoreHandle::flush` (PG).
//!
//! This is where the first real poke reaches a client. Like TS `view-syncer.ts`
//! (and unlike the placeholder `PipelineDriver`/`CVRStoreOps` traits), the CVR
//! combination lives here — the pure-IVM driver only streams row changes.
//!
//! Lives on the ViewSyncer's dedicated CG thread; not `Send`/`Sync`.
//!
//! See `packages/zero-cache/docs/rust-cvr-port/90-phase7-real-wiring-plan.md`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use rust_cvr::change_processor::ChangeProcessor;
use rust_cvr::client_handler::{ClientHandler, MultiPoker, WebSocketSink};
use rust_cvr::row_key::row_id_string;
use rust_cvr::row_record_cache::RowRecordCache;
use rust_cvr::store::{CVRStoreError, CVRStoreHandle};
use rust_cvr::types::{
    CVR, ClientSchema, DesiredQuerySpec, Patch, PatchToVersion, QueryRecord, RowID, RowPatch,
    RowRecord, ShardID, StoreOp, TTLClock,
};
use rust_cvr::updater::{CVRConfigDrivenUpdater, CVRQueryDrivenUpdater, RowRecordMap};
use rust_cvr::version::{
    CVRVersion, EMPTY_CVR_VERSION, NullableCVRVersion, cmp_versions, version_from_string,
    version_string,
};

use crate::custom_query::{
    CustomQueryContext, CustomQuerySpec, CustomTransformed, transform_custom_queries,
};
use crate::permissions::{hash_of_ast, transform_and_hash};
use crate::pipeline_driver::{AdvanceOutcome, IvmPipelines, json_to_value};
use crate::query_covering::{QueryCoverageShadowHit, QueryCoveringIndex, RunningQuery};

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
}

#[derive(Debug, thiserror::Error)]
pub enum LoadCvrError {
    #[error(transparent)]
    Store(#[from] CVRStoreError),
}

/// Combined engine + CVR driver for a single client group.
pub struct SyncEngine {
    pipelines: IvmPipelines,
    store: Option<Arc<tokio::sync::Mutex<CVRStoreHandle>>>,
    /// Read-source for `existing_rows` (the row records the client already has).
    /// The store persists the `rows` table; this cache reads it back.
    row_cache: Option<RowRecordCache>,
    clients: HashMap<String, Arc<ClientHandler>>,
    /// Handle to the shared-pool runtime (the process's main multi-thread
    /// runtime) onto which CVR Postgres I/O is offloaded. The client group runs
    /// on a single-threaded executor whose reactor does NOT drive the shared CVR
    /// pool's connections; spawning the I/O future here runs it on the runtime
    /// that DOES drive them (doc 91 §5.1), while the executor only awaits the
    /// resulting `JoinHandle` and stays free to run its other client groups.
    /// `None` in unit tests that inject no handle — I/O then runs inline.
    tokio_handle: Option<tokio::runtime::Handle>,
    /// Shadow-mode query-covering detection during hydration. Port of TS
    /// `zeroConfig.enableQueryCovering` (default true). When on, each hydration
    /// batch compares its queries against the running set and logs aggregate
    /// coverage stats — it has NO effect on what is served.
    enable_query_covering: bool,
    /// Live-instance census guard (leak hunt): inc on construct, dec on drop.
    _census: crate::live_count::Guard,
}

impl SyncEngine {
    pub fn new(pipelines: IvmPipelines) -> Self {
        SyncEngine {
            pipelines,
            store: None,
            row_cache: None,
            clients: HashMap::new(),
            tokio_handle: None,
            enable_query_covering: true,
            _census: crate::live_count::Guard::new(&crate::live_count::SYNC_ENGINE),
        }
    }

    /// Enable/disable shadow-mode query-covering logging. Port of
    /// `zeroConfig.enableQueryCovering` (default true).
    pub fn set_enable_query_covering(&mut self, enabled: bool) {
        self.enable_query_covering = enabled;
    }

    /// Read the current row records from the CVR (the client's `existing_rows`).
    /// The cache is loaded once (lazily) and kept warm; the write-back path
    /// (`flush_ops_to_store`) applies each flushed row delta to it, so this never
    /// re-reads Postgres. Empty when there is no store/cache.
    pub async fn existing_rows(&self) -> RowRecordMap {
        let Some(cache) = &self.row_cache else {
            return HashMap::new();
        };
        // Offload the (idempotent) cache load + read onto the shared-pool
        // runtime. `load()` populates the cache on first call and returns early
        // once loaded; the cache stays current via the write-back in
        // `flush_ops_to_store`, so we never `clear()` here.
        let cache = cache.clone();
        self.offload(async move {
            if let Err(e) = cache.load().await {
                tracing::warn!("row cache load failed: {e}");
                return HashMap::new();
            }
            cache
                .get_row_records()
                .await
                .into_iter()
                .map(|(k, v)| (k, cache_record_to_types(v)))
                .collect()
        })
        .await
    }

    /// Inject the shared-pool runtime handle used to offload CVR store I/O
    /// (must be the runtime that owns the CVR `PgPool`).
    pub fn set_tokio_handle(&mut self, handle: tokio::runtime::Handle) {
        self.tokio_handle = Some(handle);
    }

    /// Run a `Send` CVR-I/O future on the shared-pool runtime instead of the
    /// caller's single-threaded executor runtime. The pool's connections are
    /// polled by that runtime's reactor, so awaiting them there avoids the
    /// cross-runtime starvation of doc 91 §5.1; the executor awaits only the
    /// (cross-runtime-safe) `JoinHandle` and is free to drive its other client
    /// groups meanwhile. With no handle injected (some unit tests) the future
    /// runs inline on the current runtime.
    async fn offload<F, T>(&self, fut: F) -> T
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        match &self.tokio_handle {
            Some(handle) => handle.spawn(fut).await.expect("CVR I/O task panicked"),
            None => fut.await,
        }
    }

    /// Load the CVR snapshot from the store (or `None` if no store is set).
    pub async fn load_cvr(&self, last_connect_time: f64) -> Result<Option<CVR>, LoadCvrError> {
        let Some(store_arc) = self.store.clone() else {
            return Ok(None);
        };
        // Offload the load onto the shared-pool runtime (doc 91 §5.1).
        let result = self
            .offload(async move {
                let mut store = store_arc.lock().await;
                store.load(last_connect_time).await
            })
            .await?;
        Ok(Some(result.cvr))
    }

    /// Access the underlying IVM pipelines (e.g. for init / get_row / catchup).
    pub fn pipelines(&mut self) -> &mut IvmPipelines {
        &mut self.pipelines
    }

    /// Create the CVR Postgres store (once, shared across all calls). Port of
    /// napi `set_cvr_store`.
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
        let store = CVRStoreHandle::new(pool.clone(), schema.clone(), cvr_id.clone(), task_id);
        // Row-record cache (reads the `rows` table the store writes). A no-op
        // fail callback + no metrics for now.
        let fail: rust_cvr::row_record_cache::FailCallback = Arc::new(|e: String| {
            tracing::warn!("row cache: {e}");
        });
        let cache = RowRecordCache::new(pool, schema, cvr_id, 100, fail, None);
        self.store = Some(Arc::new(tokio::sync::Mutex::new(store)));
        self.row_cache = Some(cache);
        Ok(())
    }

    /// Register a client for poke delivery. `sink` is typically a
    /// `DirectWebSocketSink`. Port of napi `register_client`.
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

    /// Unregister a client. Port of napi `unregister_client`.
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
    fn clients_for(&self, ws_ids: &[String]) -> Vec<Arc<ClientHandler>> {
        ws_ids
            .iter()
            .filter_map(|id| self.clients.get(id).cloned())
            .collect()
    }

    /// Flush the updater's buffered store ops + CVR to Postgres (no-op when no
    /// store is set). Requires a current tokio runtime handle when a store is
    /// present, mirroring the napi path.
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
        existing_rows: &RowRecordMap,
    ) -> Result<bool, String> {
        let expected_current_version = updater.base.orig.version.clone();
        self.flush_ops_to_store(
            updater.base.drain_store_ops(),
            &expected_current_version,
            flushed_cvr,
            last_connect_time,
            existing_rows,
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
        existing_rows: &RowRecordMap,
    ) -> Result<bool, String> {
        let Some(store_arc) = self.store.clone() else {
            return Ok(true);
        };

        // Row-write dedup (port of TS `#flush`'s pending-row dedup): drop row ops
        // that would write nothing new — a record identical to what's already in
        // the CVR, or an unreferenced row/delete for a row not in the CVR anyway.
        // Without this, every row touched in a cycle is rewritten even when
        // unchanged (write amplification that inflates flush latency on hot rows).
        // Uses the caller's `existing_rows` snapshot (already read for this cycle),
        // so no extra cache clone is taken here. `force_updates` is never populated
        // in this port, so no forced-write exception is needed.
        let ops: Vec<StoreOp> = ops
            .into_iter()
            .filter(|op| !row_op_is_noop(op, existing_rows))
            .collect();

        // Extract the row deltas (from the DEDUPED ops) before they are moved into
        // the store, so we can mirror the same writes into the read cache after
        // the PG write succeeds.
        let row_deltas: Vec<(RowID, Option<rust_cvr::row_record_cache::RowRecord>)> = ops
            .iter()
            .filter_map(|op| match op {
                StoreOp::PutRowRecord(r) => {
                    Some((r.id.clone(), Some(types_record_to_cache(r.clone()))))
                }
                StoreOp::DelRowRecord(id) => Some((id.clone(), None)),
                _ => None,
            })
            .collect();

        // Offload the whole PG-touching section — apply ops, flush the CVR, and
        // mirror the row deltas back into the read cache — onto the shared-pool
        // runtime (doc 91 §5.1). The store's `!Send` engine state is not touched
        // here (only the `Send` `Arc<Mutex<CVRStoreHandle>>` / cache), so the
        // whole unit can run off-thread while the executor drives other groups.
        // `flushed_cvr` is an `Arc<CVR>`: moving it into the task is a refcount
        // bump, not a deep CVR copy — the caller reclaims the CVR via
        // `Arc::try_unwrap` once this awaited task drops its clone.
        let cache = self.row_cache.clone();
        let expected = expected_current_version.clone();
        let flushed = flushed_cvr;
        self.offload(async move {
            if !ops.is_empty() {
                store_arc.lock().await.apply_store_ops(ops);
            }
            let store_flushed = {
                let mut store = store_arc.lock().await;
                store
                    .flush(&expected, &flushed, last_connect_time as f64)
                    .await
                    .map_err(|e| format!("store flush: {e}"))?
                    .is_some()
            };

            // Write-back: the store just persisted these rows to PG, so update
            // the in-memory cache with the same deltas (`flushed=true` →
            // cache-only, no second PG write). Keeps the cache in lockstep so the
            // next `existing_rows()` needs no reload.
            if !row_deltas.is_empty()
                && let Some(cache) = &cache
            {
                let ver = flushed.version.clone();
                // Ensure the cache is loaded before applying (idempotent).
                if let Err(e) = cache.load().await {
                    tracing::warn!("row cache load before write-back failed: {e}");
                } else if let Err(e) = cache.apply(row_deltas, ver, true).await {
                    tracing::warn!("row cache write-back failed: {e}");
                }
            }
            Ok(store_flushed)
        })
        .await
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
        permissions: Option<&serde_json::Value>,
        auth_data: &serde_json::Value,
        custom_ctx: Option<&CustomQueryContext>,
        state_version: String,
        replica_version: String,
        existing_rows: &RowRecordMap,
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
    ) -> Result<CVR, String> {
        self.config_and_hydrate_with_profile(
            cvr,
            client_id,
            poke_ws_ids,
            shard,
            desired_puts,
            desired_dels,
            desired_clear,
            client_schema,
            None,
            permissions,
            auth_data,
            custom_ctx,
            state_version,
            replica_version,
            existing_rows,
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
        profile_id: Option<&str>,
        // Read-permission transformation inputs. When `permissions` is `None`
        // (no permissions deployed) queries pass through untransformed.
        permissions: Option<&serde_json::Value>,
        auth_data: &serde_json::Value,
        // Per-connection context for resolving named/custom queries via the
        // user's query API server. `None` when the connection sent no
        // `userQueryURL` (custom queries are then skipped with a warning).
        custom_ctx: Option<&CustomQueryContext>,
        state_version: String,
        replica_version: String,
        existing_rows: &RowRecordMap,
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
    ) -> Result<CVR, String> {
        // Snapshot each connected client's cookie BEFORE any poke advances it.
        // Both the config poke and the hydrate poke call `end()`, which advances
        // `base_version` to the new CVR version; catch-up (below) must replay from
        // these ORIGINAL cookies, not the post-poke ones, or a reconnecting client
        // loses the whole `[oldCookie, current]` interval. See `catchup_clients`.
        let original_client_versions: std::collections::HashMap<String, NullableCVRVersion> = self
            .clients_for(poke_ws_ids)
            .iter()
            .map(|c| (c.ws_id.clone(), c.version()))
            .collect();

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
            let clients =
                Self::config_poke_targets(self.clients_for(poke_ws_ids), &expected_current_version);
            let client_refs: Vec<&ClientHandler> = clients.iter().map(|c| c.as_ref()).collect();
            let pokers = MultiPoker::new(&client_refs, cfg_cvr.version.clone());
            for p in &config_patches {
                pokers.add_patch(p);
            }
            let cfg_arc = Arc::new(cfg_cvr);
            let store_flushed = self
                .flush_ops_to_store(
                    cfg_ops,
                    &expected_current_version,
                    cfg_arc.clone(),
                    last_connect_time,
                    existing_rows,
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
            pokers.end(cfg_cvr.version.clone());
        }

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
        let mut executed: Vec<(String, serde_json::Value, String)> = Vec::new();
        let mut custom_specs: Vec<CustomQuerySpec> = Vec::new();
        for (qid, record) in &cfg_cvr.queries {
            match record {
                QueryRecord::Internal(r) => {
                    executed.push((qid.clone(), r.ast.clone(), hash_of_ast(&r.ast)));
                }
                QueryRecord::Client(r) => {
                    let (ast, hash) = match permissions {
                        Some(perms) => transform_and_hash(&r.ast, perms, auth_data, false),
                        None => (r.ast.clone(), hash_of_ast(&r.ast)),
                    };
                    executed.push((qid.clone(), ast, hash));
                }
                QueryRecord::Custom(r) => custom_specs.push(CustomQuerySpec {
                    id: qid.clone(),
                    name: r.name.clone(),
                    args: r.args.clone(),
                }),
            }
        }

        // Resolve custom queries against the API server. Per-query errors are
        // forwarded to the client as `transformError` (healthy queries proceed);
        // a whole-request failure fails the connection with the transform error.
        if !custom_specs.is_empty() {
            let mut transform_errors: Vec<serde_json::Value> = Vec::new();
            match custom_ctx {
                Some(ctx) => {
                    // TS wraps the transform in try/catch/finally, recording
                    // `zero.sync.query.transformations{result}` + timing
                    // (view-syncer.ts:1782-1789). Time the API-server round-trip
                    // and tag the outcome; the histogram observes on both paths.
                    let transform_started = std::time::Instant::now();
                    let transform_result =
                        transform_custom_queries(ctx, shard, &custom_specs).await;
                    crate::metrics::record_query_transformation_time(
                        transform_started.elapsed().as_secs_f64() * 1000.0,
                    );
                    crate::metrics::record_query_transformation(transform_result.is_ok());
                    match transform_result {
                        Ok(results) => {
                            for r in results {
                                match r {
                                    CustomTransformed::Ok(tq) => {
                                        executed.push((tq.id, tq.ast, tq.hash))
                                    }
                                    CustomTransformed::Errored { id: _, error } => {
                                        transform_errors.push(error)
                                    }
                                }
                            }
                        }
                        Err(failed) => {
                            // Whole-batch failure (TS throws `TransformFailed` →
                            // the client fails). Surface it and leave existing
                            // pipelines intact.
                            for c in &self.clients_for(poke_ws_ids) {
                                c.send_query_transform_failed_error(&failed);
                            }
                        }
                    }
                }
                None => tracing::warn!(
                    "custom queries present but no userQueryURL context; skipping {} query(ies)",
                    custom_specs.len()
                ),
            }
            if !transform_errors.is_empty() {
                for c in &self.clients_for(poke_ws_ids) {
                    let _ = c.send_query_transform_application_errors(transform_errors.clone());
                }
            }
        }

        // Drift check: (re-)hydrate a query when it is missing OR its
        // transformation hash changed (auth re-transform / a new custom AST).
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
        let custom_ids: std::collections::HashSet<&str> =
            custom_specs.iter().map(|s| s.id.as_str()).collect();
        for (qid, transformed_ast, transformation_hash) in executed {
            let is_custom = custom_ids.contains(qid.as_str());
            match self.pipelines.query_transformation_hash(&qid) {
                // Already running with this exact transform → nothing to do.
                Some(h) if h == transformation_hash => {
                    if is_custom {
                        crate::metrics::record_query_transformation_no_op();
                    }
                    continue;
                }
                // Running with a DIFFERENT transform → drift: tear the old
                // pipeline down before re-hydrating with the new transform.
                Some(_) => {
                    if is_custom {
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
        // Tear down drifted pipelines directly (no CVR removal — the query is
        // still desired; only its compiled pipeline is rebuilt).
        for qid in &retransform_removes {
            self.pipelines.remove_query(qid);
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
                    Ok(ast) => idx.add(
                        &qid,
                        &RunningQuery {
                            transformed_ast: ast,
                            transformation_hash: hash,
                            query_name: query_name_of(&cfg_cvr, &qid),
                        },
                    ),
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
                if let Some(cov) = idx.find_covering_query(qid, transformed_ast) {
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
                    &RunningQuery {
                        transformed_ast: transformed_ast.clone(),
                        transformation_hash: transformation_hash.clone(),
                        query_name,
                    },
                );
            }
            // TS `#logQueryCoverageShadowSummary`, hydrationPath 'add'.
            crate::query_covering::log_shadow_summary(
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
        if add_queries.is_empty() {
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
            let result = self
                .hydrate_and_sync(
                    cfg_cvr,
                    state_version,
                    replica_version,
                    &add_queries,
                    // Removals are TTL-scheduler-driven (a `del` only inactivates
                    // the desired query above); nothing is removed here.
                    &[],
                    poke_ws_ids,
                    &queries,
                    existing_rows,
                    last_connect_time,
                    last_active,
                    ttl_clock,
                )
                .await?;
            self.catchup_clients(
                &result.cvr,
                &result.cvr.version,
                &excluded,
                poke_ws_ids,
                &original_client_versions,
            )
            .await?;
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
        let (Some(store_arc), Some(cache)) = (self.store.clone(), self.row_cache.as_ref()) else {
            return Ok(()); // no store → nothing persisted to catch up from
        };

        let clients = self.clients_for(poke_ws_ids);
        if clients.is_empty() {
            return Ok(());
        }

        // catchupFrom = min(cvr.version, min over connected clients' ORIGINAL
        // cookies). Port of `clients.map(c => c.version()).reduce(min, cvr.version)`
        // — but against the cycle-start snapshot, since each client's live
        // `version()` has already been advanced by the config/hydrate pokes.
        let catchup_from = Self::catchup_floor(&cvr.version, &clients, original_versions);

        // Gather the row pages + config patches from PG (async), then release
        // the cache/store borrows before touching the engine (`getRow`).
        let cache_ref = cache;
        let (raw_rows, cfg_patches): (
            Vec<rust_cvr::row_record_cache::RowsRow>,
            Vec<PatchToVersion>,
        ) = {
            let mut cursor = cache_ref
                .catchup_row_patches(
                    catchup_from.clone(),
                    &cvr.version,
                    current,
                    exclude_query_hashes,
                )
                .await
                .map_err(|e| format!("catchup_row_patches: {e}"))?;
            let mut rows = Vec::new();
            while let Some(page) = cursor
                .next_page()
                .await
                .map_err(|e| format!("catchup rows page: {e}"))?
            {
                rows.extend(page);
            }
            let store_reader = store_arc.lock().await.catchup_reader();
            let cfg = store_reader
                .catchup_config_patches(catchup_from.clone(), &cvr.version, current)
                .await
                .map_err(|e| format!("catchup_config_patches: {e}"))?;
            Ok::<_, String>((rows, cfg))
        }?;

        if raw_rows.is_empty() && cfg_patches.is_empty() {
            return Ok(());
        }

        let client_refs: Vec<&ClientHandler> = clients.iter().map(|c| c.as_ref()).collect();
        let pokers = MultiPoker::new(&client_refs, cvr.version.clone());

        // Row patches first (so the AsyncGenerator-equivalent has fully drained),
        // then config patches — matching TS ordering.
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
            let to_version = version_from_string(&row.patch_version);
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
                    Some(r) => row_to_contents(&r),
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
            pokers.add_patch(&PatchToVersion { patch, to_version });
        }
        for p in &cfg_patches {
            pokers.add_patch(p);
        }
        pokers.end(cvr.version.clone());
        Ok(())
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

    /// Hydrate queries AND apply to CVR + push pokes to clients — the whole
    /// hydrate hot path. Port of napi `HydrateAndSyncTask::compute`.
    ///
    /// `add_queries` is `(query_id, transformation_hash)`; `queries` is
    /// `(query_id, ast_json)` for the pipelines to hydrate. A hydrate panic
    /// (source-drift assert) propagates out for teardown, after the engine rolls
    /// back its partial source connections.
    #[allow(clippy::too_many_arguments)]
    pub async fn hydrate_and_sync(
        &mut self,
        cvr: CVR,
        state_version: String,
        replica_version: String,
        add_queries: &[(String, String)],
        remove_queries: &[String],
        client_ids: &[String],
        queries: &[(String, String)],
        existing_rows: &RowRecordMap,
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
    ) -> Result<SyncResult, String> {
        let (sigs, provider) = Self::signature_provider();
        let mut updater =
            CVRQueryDrivenUpdater::new(cvr, state_version, replica_version, Some(provider));

        let executed_refs: Vec<(&str, &str)> = add_queries
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect();
        let removed_refs: Vec<&str> = remove_queries.iter().map(|s| s.as_str()).collect();
        let (new_version, query_patches) = updater.track_queries(&executed_refs, &removed_refs);

        let clients = self.clients_for(client_ids);
        let client_refs: Vec<&ClientHandler> = clients.iter().map(|c| c.as_ref()).collect();
        let pokers = MultiPoker::new(&client_refs, new_version);
        for patch in &query_patches {
            pokers.add_patch(patch);
        }

        // Remove queries from the engine before hydrating new ones (matches the
        // TS path that calls `pipelines.removeQuery(q.id)` before hydrate).
        for qid in remove_queries {
            self.pipelines.remove_query(qid);
        }

        // Freshly-hydrated queries start from an empty row set (signature 0), so
        // the fold over this hydrate's changes yields the query's full signature.
        let mut sig_acc: HashMap<String, u64> = HashMap::new();
        let mut processor = ChangeProcessor::new(&mut updater, &pokers);
        self.pipelines.hydrate(queries, |rc| {
            accumulate_signature(&mut sig_acc, rc);
            let (ct, qid, table, rk, row) = row_change_to_maps(rc);
            processor.on_row_change(ct, &qid, &table, &rk, row.as_ref(), existing_rows);
        })?;
        // Record the transformation hash each query was hydrated with, so a later
        // config pass can detect a changed hash (drift / auth re-transform) and
        // re-hydrate. Port of the `transformationHash` carried in the TS pipeline
        // query map.
        for (qid, hash) in add_queries {
            self.pipelines.set_query_transformation_hash(qid, hash);
        }
        processor.finish(existing_rows);
        let num_changes = processor.total_processed();
        drop(processor);

        // Hand the folded signatures to the updater's provider so its flush can
        // persist each hydrated query's signature and flag drift.
        *sigs.lock().unwrap() = sig_acc;
        let (flushed_cvr, _stats) = updater.flush(last_connect_time, last_active, ttl_clock);
        // Share the CVR with the offloaded flush via `Arc` (refcount bump, not a
        // deep copy); reclaim it after the awaited flush drops its clone.
        let flushed_arc = Arc::new(flushed_cvr);
        let store_flushed = self
            .flush_to_store(
                &mut updater,
                flushed_arc.clone(),
                last_connect_time,
                existing_rows,
            )
            .await?;
        // No-op store flush → revert to the ORIGINAL CVR (see `flush_to_store`).
        let flushed_cvr = if store_flushed {
            Arc::try_unwrap(flushed_arc).unwrap_or_else(|a| (*a).clone())
        } else {
            updater.base.orig.clone()
        };
        pokers.end(flushed_cvr.version.clone());

        let version = version_string(&flushed_cvr.version);
        Ok(SyncResult {
            cvr: flushed_cvr,
            version,
            query_patches,
            num_changes,
            reset_reason: None,
            reset_msg: None,
        })
    }

    /// Advance the replica to head AND apply to CVR + push pokes to clients.
    /// Port of napi `AdvanceAndSyncTask::compute`. On a reset, the in-flight
    /// poke is cancelled and the caller is expected to rehydrate.
    #[allow(clippy::too_many_arguments)]
    pub async fn advance_and_sync(
        &mut self,
        cvr: CVR,
        replica_version: String,
        client_ids: &[String],
        existing_rows: &RowRecordMap,
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
    ) -> Result<SyncResult, String> {
        let cvr_for_reset = cvr.clone();
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
            u8,
            String,
            String,
            serde_json::Map<String, serde_json::Value>,
            Option<serde_json::Map<String, serde_json::Value>>,
        );
        let mut new_version = String::new();
        let mut num_changes = 0usize;
        let mut collected: Vec<CollectedChange> = Vec::new();
        let outcome = self.pipelines.advance(
            |version, n| {
                new_version = version.to_string();
                num_changes = n;
            },
            |rc| {
                accumulate_signature(&mut sig_acc, rc);
                collected.push(row_change_to_maps(rc));
            },
        )?;

        if let AdvanceOutcome::Reset { reason, msg } = outcome {
            // No poke was started (the pokers are built below, after a clean
            // advance), so there is nothing to cancel — just report the reset.
            return Ok(SyncResult {
                cvr: cvr_for_reset,
                version: String::new(),
                query_patches: Vec::new(),
                num_changes,
                reset_reason: Some(reason),
                reset_msg: Some(msg),
            });
        }

        // Build the updater with the real post-advance version, then replay the
        // collected delta through it (order preserved).
        let (sigs, provider) = Self::signature_provider();
        let mut updater =
            CVRQueryDrivenUpdater::new(cvr, new_version, replica_version, Some(provider));
        let pokers_version = updater.updated_version();

        // Only poke clients that are AT the pre-advance `cvr.version` (see
        // `advance_poke_targets`).
        let clients = Self::advance_poke_targets(self.clients_for(client_ids), &cvr_version);
        let client_refs: Vec<&ClientHandler> = clients.iter().map(|c| c.as_ref()).collect();
        let pokers = MultiPoker::new(&client_refs, pokers_version);

        {
            let mut processor = ChangeProcessor::new(&mut updater, &pokers);
            for (ct, qid, table, rk, row) in &collected {
                processor.on_row_change(*ct, qid, table, rk, row.as_ref(), existing_rows);
            }
            // TS `#advancePipelines` only processes received row changes. It
            // does not reconcile unreferenced rows because no queries are being
            // executed/removed in an advance pass.
            processor.finish_received(existing_rows);
            num_changes = processor.total_processed();
        }

        // Hand the folded post-advance signatures to the updater's provider.
        *sigs.lock().unwrap() = sig_acc;
        let (flushed_cvr, _stats) = updater.flush(last_connect_time, last_active, ttl_clock);
        // Share the CVR with the offloaded flush via `Arc` (refcount bump, not a
        // deep copy); reclaim it after the awaited flush drops its clone.
        let flushed_arc = Arc::new(flushed_cvr);
        let store_flushed = self
            .flush_to_store(
                &mut updater,
                flushed_arc.clone(),
                last_connect_time,
                existing_rows,
            )
            .await?;
        // Quiet commit (zero IVM output for this CG, e.g. the batch only touched
        // other groups' rows): the store flush is a no-op, so revert to the
        // ORIGINAL CVR (TS `flush` → `this._orig`). `pokers.end(orig)` then
        // no-ops for caught-up clients instead of advancing their cookies to a
        // version that was never persisted, and the next material flush's
        // `expected_current_version` still matches the on-disk version.
        let flushed_cvr = if store_flushed {
            Arc::try_unwrap(flushed_arc).unwrap_or_else(|a| (*a).clone())
        } else {
            updater.base.orig.clone()
        };
        pokers.end(flushed_cvr.version.clone());

        let version = version_string(&flushed_cvr.version);
        Ok(SyncResult {
            cvr: flushed_cvr,
            version,
            query_patches: Vec::new(),
            num_changes,
            reset_reason: None,
            reset_msg: None,
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
        existing_rows: &RowRecordMap,
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
        let state_version = cvr.version.state_version.clone();
        let replica_version = cvr.replica_version.clone().unwrap_or_default();
        // A removal-only query-driven pass: track_queries(removed) emits the
        // got-query `del` patches + bumps the config version, remove_query
        // tears each pipeline down, and `finish` → delete_unreferenced_rows
        // pokes the now-orphaned rows away.
        let result = self
            .hydrate_and_sync(
                cvr,
                state_version,
                replica_version,
                &[],
                &expired,
                client_ids,
                &[],
                existing_rows,
                last_connect_time,
                last_active,
                ttl_clock,
            )
            .await?;
        Ok((result.cvr, expired.len()))
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
        let existing_rows = self.existing_rows().await;
        let clients = self.clients_for(poke_ws_ids);
        {
            let refs: Vec<&ClientHandler> = clients.iter().map(|c| c.as_ref()).collect();
            let pokers = MultiPoker::new(&refs, cfg_cvr.version.clone());
            for p in &patches {
                pokers.add_patch(p);
            }
            let cfg_arc = Arc::new(cfg_cvr);
            let store_flushed = self
                .flush_ops_to_store(
                    ops,
                    &expected_current_version,
                    cfg_arc.clone(),
                    last_connect_time,
                    &existing_rows,
                )
                .await?;
            // No-op flush (e.g. every requested client was foreign to this
            // group) → stay on the original CVR (see `flush_to_store`).
            cfg_cvr = if store_flushed {
                Arc::try_unwrap(cfg_arc).unwrap_or_else(|a| (*a).clone())
            } else {
                cfg.base.orig.clone()
            };
            pokers.end(cfg_cvr.version.clone());
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
/// row_key, row)` shape `ChangeProcessor::on_row_change` expects. Port of napi
/// `row_change_to_maps`.
type RowChangeMaps = (
    u8,
    String,
    String,
    serde_json::Map<String, serde_json::Value>,
    Option<serde_json::Map<String, serde_json::Value>>,
);

/// The custom-query name for a query id, or `None` for internal/client queries.
/// Mirrors TS `query.type === 'custom' ? query.name : undefined`, used to label
/// shadow-mode coverage log entries.
fn query_name_of(cvr: &CVR, qid: &str) -> Option<String> {
    match cvr.queries.get(qid) {
        Some(QueryRecord::Custom(r)) => Some(r.name.clone()),
        _ => None,
    }
}

fn row_change_to_maps(rc: &rust_ivm::streamer::RowChange) -> RowChangeMaps {
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
    (
        rc.change_type as u8,
        rc.query_id.clone(),
        rc.table.clone(),
        row_key,
        row,
    )
}

/// XOR-fold a streamed `RowChange` into a per-query row-set-signature
/// accumulator, mirroring the engine's `add_queries` fold: every non-Edit change
/// (Add or Remove) XORs the table+rowKey unit, so a Remove undoes a prior Add.
/// Uses the original `rust_ivm` row key (not the JSON-converted one) so the hash
/// matches `row_signature_unit` byte-for-byte.
fn accumulate_signature(acc: &mut HashMap<String, u64>, rc: &rust_ivm::streamer::RowChange) {
    if rc.change_type != rust_ivm::ivm::change::ChangeType::Edit {
        let unit = rust_ivm::row_signature_unit(&rc.table, &rc.row_key);
        *acc.entry(rc.query_id.clone()).or_insert(0) ^= unit;
    }
}

/// Convert a `rust_ivm` `Value` to `serde_json::Value`, matching TS
/// `JSON.stringify` semantics. Port of napi `value_to_serde_json`.
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

/// Convert a row-record-cache `RowRecord` into the `types::RowRecord` the
/// updater / ChangeProcessor expect. They differ only in the `ref_counts` map
/// type (the cache uses `HashMap`, `types::RefCounts` is a `BTreeMap`).
/// Whether a row op writes nothing new against the current CVR rows — a port of
/// the two drop conditions in TS `CVRStore.#flush`: (a) an unreferenced row or a
/// delete for a row that isn't in the CVR anyway, and (b) a record that exactly
/// equals what's already stored (`deepEqual`). Such ops are pure write
/// amplification and are filtered out before the store flush.
fn row_op_is_noop(op: &StoreOp, existing: &RowRecordMap) -> bool {
    match op {
        StoreOp::PutRowRecord(r) => {
            let key = rust_cvr::row_key::row_id_string(&r.id);
            let ex = existing.get(&key);
            // (1) unreferenced (tombstone) and not present, or (2) unchanged.
            (ex.is_none() && r.ref_counts.is_none()) || ex == Some(r)
        }
        StoreOp::DelRowRecord(id) => {
            // Deleting a row that isn't in the CVR is a no-op.
            let key = rust_cvr::row_key::row_id_string(id);
            existing.get(&key).is_none()
        }
        _ => false,
    }
}

fn cache_record_to_types(r: rust_cvr::row_record_cache::RowRecord) -> RowRecord {
    RowRecord {
        id: r.id,
        row_version: r.row_version,
        patch_version: r.patch_version,
        ref_counts: r.ref_counts.map(|m| m.into_iter().collect()),
    }
}

/// Inverse of [`cache_record_to_types`] — convert a `types::RowRecord` (from a
/// flushed `StoreOp::PutRowRecord`) into the row-record-cache's `RowRecord`, for
/// the write-back path that keeps the cache in lockstep with PG.
fn types_record_to_cache(r: RowRecord) -> rust_cvr::row_record_cache::RowRecord {
    rust_cvr::row_record_cache::RowRecord {
        id: r.id,
        row_version: r.row_version,
        patch_version: r.patch_version,
        ref_counts: r.ref_counts.map(|m| m.into_iter().collect()),
    }
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

/// Parse a `Vec<RowRecord>` JSON blob into a `RowRecordMap` (keyed by row id
/// string). Helper for callers that hold existing rows as JSON.
pub fn parse_existing_rows(json: &str) -> Result<RowRecordMap, String> {
    if json.is_empty() || json == "null" {
        return Ok(HashMap::new());
    }
    let records: Vec<RowRecord> =
        serde_json::from_str(json).map_err(|e| format!("invalid existing_rows: {e}"))?;
    Ok(records
        .into_iter()
        .map(|r| (row_id_string(&r.id), r))
        .collect())
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline_driver::{IvmColumnSchema, IvmTableSpec};
    use crate::ws_sink::{DirectWebSocketSink, WsCommand};
    use rust_cvr::types::{BaseQueryRecord, CVR, ClientQueryRecord, QueryRecord, ShardID};
    use rust_cvr::version::CVRVersion;
    use std::collections::BTreeMap;

    /// A censused type must return its live-object counter to baseline once it
    /// drops — otherwise the census leaks and defeats the leak hunt. `SyncEngine`
    /// carries a `live_count::Guard` on `SYNC_ENGINE`; construct one, assert the
    /// counter went up, drop it, assert it came back down.
    #[test]
    fn sync_engine_census_returns_to_baseline_after_drop() {
        use crate::live_count::SYNC_ENGINE;
        use std::sync::atomic::Ordering;
        let base = SYNC_ENGINE.load(Ordering::Relaxed);
        {
            let _engine = SyncEngine::new(IvmPipelines::new());
            assert_eq!(SYNC_ENGINE.load(Ordering::Relaxed), base + 1);
        }
        assert_eq!(
            SYNC_ENGINE.load(Ordering::Relaxed),
            base,
            "SyncEngine census leaked on drop"
        );
    }

    fn row_id(id: &str) -> RowID {
        let mut key = serde_json::Map::new();
        key.insert("id".to_string(), serde_json::Value::String(id.to_string()));
        RowID {
            schema: "public".to_string(),
            table: "issue".to_string(),
            row_key: key,
        }
    }

    fn row_record(id: &str, patch_version: &str, referenced: bool) -> RowRecord {
        RowRecord {
            id: row_id(id),
            row_version: "r1".to_string(),
            patch_version: version_from_string(patch_version),
            ref_counts: referenced.then(|| {
                let mut m = BTreeMap::new();
                m.insert("q1".to_string(), 1i64);
                m
            }),
        }
    }

    /// The row-write dedup (TS `#flush`): unchanged records and unreferenced /
    /// absent-row deletes are no-ops and must be filtered; real adds, changes, and
    /// deletes of present rows must NOT be filtered.
    #[test]
    fn row_op_is_noop_matches_ts_dedup() {
        let rec = row_record("i1", "01", true);
        let mut existing: RowRecordMap = HashMap::new();
        existing.insert(row_id_string(&rec.id), rec.clone());

        // Unchanged record → no-op (dropped).
        assert!(row_op_is_noop(
            &StoreOp::PutRowRecord(rec.clone()),
            &existing
        ));
        // Changed record (different patch_version) → NOT a no-op.
        let changed = row_record("i1", "02", true);
        assert!(!row_op_is_noop(&StoreOp::PutRowRecord(changed), &existing));
        // Brand-new referenced row (not in CVR) → NOT a no-op (a real add).
        let added = row_record("i2", "01", true);
        assert!(!row_op_is_noop(&StoreOp::PutRowRecord(added), &existing));
        // Tombstone (unreferenced) for a row not in the CVR → no-op.
        let ghost_tombstone = row_record("i3", "01", false);
        assert!(row_op_is_noop(
            &StoreOp::PutRowRecord(ghost_tombstone),
            &existing
        ));
        // Delete of a present row → NOT a no-op (a real delete).
        assert!(!row_op_is_noop(
            &StoreOp::DelRowRecord(row_id("i1")),
            &existing
        ));
        // Delete of an absent row → no-op.
        assert!(row_op_is_noop(
            &StoreOp::DelRowRecord(row_id("gone")),
            &existing
        ));
    }

    /// A CVR with a single client query `q1` (mirrors rust-cvr's test helper),
    /// so `track_queries` produces a got-query patch.
    fn make_cvr() -> CVR {
        let mut cvr = CVR {
            id: "cg1".to_string(),
            version: CVRVersion {
                state_version: "00".to_string(),
                config_version: None,
            },
            last_active: 0,
            ttl_clock: 0,
            replica_version: Some("v1".to_string()),
            clients: BTreeMap::new(),
            queries: BTreeMap::new(),
            client_schema: None,
            profile_id: None,
        };
        let query = QueryRecord::Client(ClientQueryRecord {
            base: BaseQueryRecord {
                id: "q1".to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
            },
            ast: serde_json::json!({"table": "users"}),
            client_state: BTreeMap::new(),
            patch_version: None,
        });
        cvr.queries.insert("q1".to_string(), query);
        cvr
    }

    fn users_spec() -> IvmTableSpec {
        IvmTableSpec {
            table: "users".to_string(),
            columns: HashMap::from([(
                "id".to_string(),
                IvmColumnSchema {
                    r#type: "string".to_string(),
                    optional: false,
                },
            )]),
            primary_key: vec!["id".to_string()],
            unique_keys: None,
            min_row_version: None,
        }
    }

    #[tokio::test]
    async fn hydrate_and_sync_emits_poke_frames() {
        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();

        let mut engine = SyncEngine::new(pipelines);

        // Wire a client whose sink drains into a channel (buffer large enough
        // that blocking_send never blocks for the few poke frames).
        let (tx, mut rx) = tokio::sync::mpsc::channel::<WsCommand>(64);
        let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
        engine.register_client(
            "client1",
            "ws1",
            "cg1",
            &ShardID {
                app_id: "app".to_string(),
                shard_num: 0,
            },
            None,
            sink,
        );

        let existing_rows: RowRecordMap = HashMap::new();
        let result = engine
            .hydrate_and_sync(
                make_cvr(),
                "00".to_string(),
                "v1".to_string(),
                &[("q1".to_string(), "hash1".to_string())],
                &[],
                &["ws1".to_string()],
                &[("q1".to_string(), r#"{"table":"users"}"#.to_string())],
                &existing_rows,
                0,
                0,
                0,
            )
            .await
            .unwrap();

        // Store is None → no flush; the got-query patch still produces a poke.
        assert!(result.reset_reason.is_none());
        assert!(
            !result.query_patches.is_empty(),
            "expected a got-query patch"
        );

        let mut frames = Vec::new();
        while let Ok(cmd) = rx.try_recv() {
            if let WsCommand::Send(v) = cmd {
                frames.push(v);
            }
        }
        assert!(
            frames.len() >= 2,
            "expected at least pokeStart + pokeEnd, got {}",
            frames.len()
        );
        assert_eq!(frames.first().unwrap()[0], "pokeStart");
        assert_eq!(frames.last().unwrap()[0], "pokeEnd");
    }

    /// Regression for the advance-path panic: `advance_and_sync` must construct
    /// the query-driven updater with the REAL post-advance version, not an empty
    /// placeholder. The old code passed `String::new()`, and `new()` asserts
    /// `stateVersion >= cvr.version.stateVersion` — false for any non-empty CVR
    /// version (`"" >= "00"` is false in Rust) → panic on the FIRST advance after
    /// hydration. `make_cvr()` has stateVersion "00", so the old code panicked
    /// here; the fix advances first and uses the header version. Needs a
    /// snapshotter-backed pipeline (advance is unavailable on MemorySource).
    #[tokio::test]
    async fn advance_and_sync_uses_header_version_not_empty() {
        use rusqlite::Connection;

        let db_path = "/tmp/rust-syncer-advance-and-sync-test.db";
        let cleanup = || {
            for suffix in ["", "-wal", "-wal2", "-shm"] {
                let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
            }
        };
        cleanup();
        {
            let conn = Connection::open(db_path).unwrap();
            let _ = conn.pragma_update(None, "journal_mode", "wal2");
            conn.execute_batch(
                r#"
                CREATE TABLE "_zero.replicationConfig" (
                    lock TEXT PRIMARY KEY DEFAULT 'singleton',
                    replicaVersion TEXT NOT NULL,
                    publications TEXT NOT NULL
                );
                CREATE TABLE "_zero.replicationState" (
                    lock TEXT PRIMARY KEY DEFAULT 'singleton',
                    stateVersion TEXT NOT NULL
                );
                CREATE TABLE "_zero.changeLog2" (
                    "stateVersion" TEXT NOT NULL,
                    "table"        TEXT NOT NULL,
                    "rowKey"       TEXT NOT NULL,
                    "op"           TEXT NOT NULL,
                    "pos"          INTEGER NOT NULL,
                    PRIMARY KEY ("stateVersion", "pos")
                );
                CREATE TABLE users (id TEXT PRIMARY KEY, "_0_version" TEXT NOT NULL);
                INSERT INTO "_zero.replicationConfig" (lock, replicaVersion, publications)
                    VALUES ('singleton', 'v1', '[]');
                INSERT INTO "_zero.replicationState" (lock, stateVersion)
                    VALUES ('singleton', 'v1');
                "#,
            )
            .unwrap();
        }

        let mut pipelines = IvmPipelines::new();
        pipelines
            .init(vec![users_spec()], Some(db_path), "app")
            .unwrap();
        let mut engine = SyncEngine::new(pipelines);

        let (tx, _rx) = tokio::sync::mpsc::channel::<WsCommand>(64);
        let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
        engine.register_client(
            "client1",
            "ws1",
            "cg1",
            &ShardID {
                app_id: "app".to_string(),
                shard_num: 0,
            },
            None,
            sink,
        );

        // make_cvr() has stateVersion "00" and replicaVersion "v1"; advancing a
        // snapshot pinned at "v1" MUST NOT panic (it did before the fix).
        let existing_rows: RowRecordMap = HashMap::new();
        let result = engine
            .advance_and_sync(
                make_cvr(),
                "v1".to_string(),
                &["ws1".to_string()],
                &existing_rows,
                0,
                0,
                0,
            )
            .await;

        cleanup();
        let result = result.expect("advance_and_sync must not error/panic");
        assert!(result.reset_reason.is_none(), "unexpected reset");
        assert!(
            !result.version.is_empty(),
            "advance produced an empty version — the updater was built without the header version"
        );
    }

    #[tokio::test]
    async fn config_and_hydrate_from_desired_queries_pokes_client() {
        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);

        let (tx, mut rx) = tokio::sync::mpsc::channel::<WsCommand>(128);
        let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };
        engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

        // A fresh CVR + a single desired query (as an initConnection would carry).
        let cvr = super::empty_cvr("cg1", "v1");
        let puts = vec![DesiredQuerySpec {
            hash: "q1".to_string(),
            ast: Some(serde_json::json!({"table": "users"})),
            name: None,
            args: None,
            ttl: None,
        }];
        let existing_rows: RowRecordMap = HashMap::new();

        let result_cvr = engine
            .config_and_hydrate(
                cvr,
                "client1",
                &["ws1".to_string()],
                &shard,
                puts,
                Vec::new(),
                false,
                None,
                None,
                &serde_json::json!({}),
                None,
                "00".to_string(),
                "v1".to_string(),
                &existing_rows,
                0,
                0,
                0,
            )
            .await
            .unwrap();

        // The client group now tracks the desired query, and the client got
        // both a config poke and a hydrate poke.
        assert!(result_cvr.clients.contains_key("client1"));
        assert!(result_cvr.queries.contains_key("q1"));

        let mut starts = 0;
        let mut ends = 0;
        while let Ok(WsCommand::Send(v)) = rx.try_recv() {
            match v[0].as_str() {
                Some("pokeStart") => starts += 1,
                Some("pokeEnd") => ends += 1,
                _ => {}
            }
        }
        assert!(
            starts >= 1 && ends >= 1,
            "expected poke frames: {starts} starts, {ends} ends"
        );
    }

    #[tokio::test]
    async fn expired_query_is_removed_after_ttl_elapses() {
        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);

        let (tx, _rx) = tokio::sync::mpsc::channel::<WsCommand>(256);
        let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };
        engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

        let existing_rows: RowRecordMap = HashMap::new();
        let ws = vec!["ws1".to_string()];

        // 1) Subscribe q1 with a 1000ms TTL.
        let puts = vec![DesiredQuerySpec {
            hash: "q1".to_string(),
            ast: Some(serde_json::json!({"table": "users"})),
            name: None,
            args: None,
            ttl: Some(1000),
        }];
        let cvr = engine
            .config_and_hydrate(
                super::empty_cvr("cg1", "v1"),
                "client1",
                &ws,
                &shard,
                puts,
                Vec::new(),
                false,
                None,
                None,
                &serde_json::json!({}),
                None,
                "00".to_string(),
                "v1".to_string(),
                &existing_rows,
                0,
                0,
                0,
            )
            .await
            .unwrap();
        assert!(engine.pipelines().has_query("q1"));

        // 2) Unsubscribe (del) at ttl_clock=0 → q1 marked inactive, still running.
        let cvr = engine
            .config_and_hydrate(
                cvr,
                "client1",
                &ws,
                &shard,
                Vec::new(),
                vec!["q1".to_string()],
                false,
                None,
                None,
                &serde_json::json!({}),
                None,
                "00".to_string(),
                "v1".to_string(),
                &existing_rows,
                0,
                0,
                0,
            )
            .await
            .unwrap();
        assert!(engine.pipelines().has_query("q1"), "inactive query lingers");

        // 3) Not yet expired at ttl_clock=500 (< inactivated_at 0 + ttl 1000).
        let (cvr, removed) = engine
            .remove_expired_queries(cvr, &ws, &existing_rows, 0, 0, 500)
            .await
            .unwrap();
        assert_eq!(removed, 0);
        assert!(engine.pipelines().has_query("q1"));

        // 4) Expired at ttl_clock=2000 → removed from pipeline + CVR.
        let (cvr, removed) = engine
            .remove_expired_queries(cvr, &ws, &existing_rows, 0, 0, 2000)
            .await
            .unwrap();
        assert_eq!(removed, 1);
        assert!(!engine.pipelines().has_query("q1"));
        assert!(
            !cvr.queries.contains_key("q1"),
            "expired query removed from CVR"
        );
    }

    #[test]
    fn row_record_cache_type_conversion_roundtrips() {
        // The write-back path converts types::RowRecord → cache RowRecord; the
        // read path converts back. The two must roundtrip losslessly.
        let mut row_key = serde_json::Map::new();
        row_key.insert("id".to_string(), serde_json::json!("i1"));
        let mut ref_counts = std::collections::BTreeMap::new();
        ref_counts.insert("q1".to_string(), 2i64);
        let original = RowRecord {
            id: rust_cvr::row_key::RowID {
                schema: "public".to_string(),
                table: "issue".to_string(),
                row_key,
            },
            row_version: "01".to_string(),
            patch_version: CVRVersion {
                state_version: "01".to_string(),
                config_version: None,
            },
            ref_counts: Some(ref_counts),
        };
        let back = super::cache_record_to_types(super::types_record_to_cache(original.clone()));
        assert_eq!(back, original);
    }

    #[tokio::test]
    async fn clear_op_drops_all_desired_queries() {
        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);

        let (tx, _rx) = tokio::sync::mpsc::channel::<WsCommand>(128);
        let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };
        engine.register_client("client1", "ws1", "cg1", &shard, None, sink);
        let existing_rows: RowRecordMap = HashMap::new();

        // Subscribe q1.
        let cvr = engine
            .config_and_hydrate(
                super::empty_cvr("cg1", "v1"),
                "client1",
                &["ws1".to_string()],
                &shard,
                vec![DesiredQuerySpec {
                    hash: "q1".to_string(),
                    ast: Some(serde_json::json!({"table": "users"})),
                    name: None,
                    args: None,
                    ttl: None,
                }],
                Vec::new(),
                false,
                None,
                None,
                &serde_json::json!({}),
                None,
                "00".to_string(),
                "v1".to_string(),
                &existing_rows,
                0,
                0,
                0,
            )
            .await
            .unwrap();
        assert!(
            cvr.clients["client1"]
                .desired_query_ids
                .contains(&"q1".to_string())
        );

        // A `clear` op removes all of the client's desired queries.
        let cvr = engine
            .config_and_hydrate(
                cvr,
                "client1",
                &["ws1".to_string()],
                &shard,
                Vec::new(),
                Vec::new(),
                true, // clear
                None,
                None,
                &serde_json::json!({}),
                None,
                "00".to_string(),
                "v1".to_string(),
                &existing_rows,
                0,
                0,
                0,
            )
            .await
            .unwrap();
        assert!(
            !cvr.clients["client1"]
                .desired_query_ids
                .contains(&"q1".to_string()),
            "clear should drop the client's desired q1"
        );
    }

    #[tokio::test]
    async fn config_and_hydrate_reissue_takes_catchup_branch_without_store() {
        // A second config_and_hydrate for an already-hydrated query has an empty
        // add set, so it takes the catchup branch. With no CVR store wired,
        // catchup is a clean no-op and the call still returns the CVR intact.
        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);

        let (tx, _rx) = tokio::sync::mpsc::channel::<WsCommand>(128);
        let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };
        engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

        let existing_rows: RowRecordMap = HashMap::new();
        let put = || {
            vec![DesiredQuerySpec {
                hash: "q1".to_string(),
                ast: Some(serde_json::json!({"table": "users"})),
                name: None,
                args: None,
                ttl: None,
            }]
        };

        // First call hydrates q1.
        let cvr = engine
            .config_and_hydrate(
                super::empty_cvr("cg1", "v1"),
                "client1",
                &["ws1".to_string()],
                &shard,
                put(),
                Vec::new(),
                false,
                None,
                None,
                &serde_json::json!({}),
                None,
                "00".to_string(),
                "v1".to_string(),
                &existing_rows,
                0,
                0,
                0,
            )
            .await
            .unwrap();
        assert!(engine.pipelines().has_query("q1"));

        // Second call: q1 already in the pipeline → empty add set → catchup path.
        let cvr = engine
            .config_and_hydrate(
                cvr,
                "client1",
                &["ws1".to_string()],
                &shard,
                put(),
                Vec::new(),
                false,
                None,
                None,
                &serde_json::json!({}),
                None,
                "00".to_string(),
                "v1".to_string(),
                &existing_rows,
                0,
                0,
                0,
            )
            .await
            .unwrap();
        assert!(cvr.queries.contains_key("q1"));
        assert!(cvr.clients.contains_key("client1"));
    }

    #[tokio::test]
    async fn changed_transformation_hash_rehydrates_query() {
        // Simulates the updateAuth re-transform path: a query already hydrated
        // with one transformation hash is re-hydrated when the recomputed hash
        // differs (as it would when authData changes the permission expansion).
        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);

        let (tx, _rx) = tokio::sync::mpsc::channel::<WsCommand>(128);
        let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };
        engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

        let existing_rows: RowRecordMap = HashMap::new();
        let put = || {
            vec![DesiredQuerySpec {
                hash: "q1".to_string(),
                ast: Some(serde_json::json!({"table": "users"})),
                name: None,
                args: None,
                ttl: None,
            }]
        };

        let cvr = engine
            .config_and_hydrate(
                super::empty_cvr("cg1", "v1"),
                "client1",
                &["ws1".to_string()],
                &shard,
                put(),
                Vec::new(),
                false,
                None,
                None,
                &serde_json::json!({}),
                None,
                "00".to_string(),
                "v1".to_string(),
                &existing_rows,
                0,
                0,
                0,
            )
            .await
            .unwrap();
        let real_hash = engine
            .pipelines()
            .query_transformation_hash("q1")
            .unwrap()
            .to_string();
        assert!(!real_hash.is_empty());

        // Force a stale recorded hash (as if the previous transform used a
        // different authData), then re-run: q1 must be torn down + re-hydrated,
        // restoring the correct hash.
        engine
            .pipelines()
            .set_query_transformation_hash("q1", "STALE-HASH");
        assert_eq!(
            engine.pipelines().query_transformation_hash("q1"),
            Some("STALE-HASH")
        );

        engine
            .config_and_hydrate(
                cvr,
                "client1",
                &["ws1".to_string()],
                &shard,
                put(),
                Vec::new(),
                false,
                None,
                None,
                &serde_json::json!({}),
                None,
                "00".to_string(),
                "v1".to_string(),
                &existing_rows,
                0,
                0,
                0,
            )
            .await
            .unwrap();
        assert!(engine.pipelines().has_query("q1"));
        assert_eq!(
            engine.pipelines().query_transformation_hash("q1"),
            Some(real_hash.as_str()),
            "drifted query should be re-hydrated back to the correct transform hash"
        );
    }

    #[tokio::test]
    async fn catchup_clients_without_store_is_noop() {
        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };
        let (tx, _rx) = tokio::sync::mpsc::channel::<WsCommand>(16);
        engine.register_client(
            "client1",
            "ws1",
            "cg1",
            &shard,
            None,
            Arc::new(DirectWebSocketSink::new(tx)),
        );
        let cvr = super::empty_cvr("cg1", "v1");
        // No store set → catchup returns Ok(()) without touching PG.
        engine
            .catchup_clients(
                &cvr,
                &cvr.version.clone(),
                &[],
                &["ws1".to_string()],
                &std::collections::HashMap::new(),
            )
            .await
            .unwrap();
    }

    /// Regression for reconnect catch-up: the floor must be each client's cookie
    /// as of the START of the config/hydrate cycle — NOT its live `version()`,
    /// which the config & hydrate pokes' `end()` have already advanced to the new
    /// CVR version. Feeding the live version collapses the interval to
    /// `[current, current]` and a reconnecting client loses everything it missed.
    #[test]
    fn catchup_floor_uses_original_cookie_not_advanced_version() {
        use rust_cvr::version::version_from_string;

        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);

        let (tx, _rx) = tokio::sync::mpsc::channel::<WsCommand>(8);
        let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };
        // Client connected at cookie "01".
        engine.register_client("c1", "ws1", "cg1", &shard, Some("01"), sink);

        // Cycle-start snapshot (captured before any poke advanced base_version).
        let original: std::collections::HashMap<String, NullableCVRVersion> =
            std::collections::HashMap::from([("ws1".to_string(), Some(version_from_string("01")))]);

        // Simulate the config/hydrate pokes advancing base_version to the new "05".
        let clients = engine.clients_for(&["ws1".to_string()]);
        clients[0].set_base_version_for_test(version_from_string("05"));

        let cvr_version = version_from_string("05");

        // With the snapshot the floor is the ORIGINAL "01" — catch-up replays the
        // whole [01, 05] interval the reconnecting client missed.
        let floor = SyncEngine::catchup_floor(&cvr_version, &clients, &original);
        assert_eq!(floor, Some(version_from_string("01")));

        // Guard: the OLD behavior (reading the already-advanced live version)
        // collapses the floor to "05" == current → an empty catch-up interval.
        let buggy =
            SyncEngine::catchup_floor(&cvr_version, &clients, &std::collections::HashMap::new());
        assert_eq!(buggy, Some(version_from_string("05")));
        assert_ne!(
            floor, buggy,
            "the fix must not collapse the catch-up interval"
        );
    }

    /// An advance may only poke clients that are AT the pre-advance cvr.version;
    /// lagging clients (behind it) and never-poked clients are excluded and get
    /// caught up on reconnect instead. Port of TS `#getClients(cvr.version)`.
    #[test]
    fn advance_poke_targets_excludes_lagging_clients() {
        use rust_cvr::version::version_from_string;

        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };
        let mk = || -> Arc<dyn WebSocketSink> {
            let (tx, _rx) = tokio::sync::mpsc::channel::<WsCommand>(8);
            Arc::new(DirectWebSocketSink::new(tx))
        };
        engine.register_client("cA", "wsA", "cg1", &shard, Some("05"), mk()); // at cvr.version
        engine.register_client("cB", "wsB", "cg1", &shard, Some("03"), mk()); // lagging
        engine.register_client("cC", "wsC", "cg1", &shard, None, mk()); // never poked

        let all = engine.clients_for(&["wsA".to_string(), "wsB".to_string(), "wsC".to_string()]);
        let targets = SyncEngine::advance_poke_targets(all, &version_from_string("05"));
        let ids: Vec<String> = targets.iter().map(|c| c.ws_id.clone()).collect();
        assert_eq!(
            ids,
            vec!["wsA".to_string()],
            "only the client at cvr.version may receive the advance delta"
        );
    }

    #[test]
    fn config_poke_targets_include_new_but_exclude_lagging_clients() {
        use rust_cvr::version::version_from_string;

        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };
        let mk = || -> Arc<dyn WebSocketSink> {
            let (tx, _rx) = tokio::sync::mpsc::channel::<WsCommand>(8);
            Arc::new(DirectWebSocketSink::new(tx))
        };
        engine.register_client("new", "ws-new", "cg1", &shard, None, mk());
        engine.register_client("current", "ws-current", "cg1", &shard, Some("02"), mk());
        engine.register_client("lagging", "ws-lagging", "cg1", &shard, Some("01"), mk());

        let new_targets = SyncEngine::config_poke_targets(
            engine.clients_for(&["ws-new".to_string()]),
            &version_from_string("00"),
        );
        assert_eq!(new_targets.len(), 1, "no cookie is TS empty version 00");

        let targets = SyncEngine::config_poke_targets(
            engine.clients_for(&["ws-current".to_string(), "ws-lagging".to_string()]),
            &version_from_string("02"),
        );
        let ids: Vec<_> = targets.iter().map(|client| client.ws_id.as_str()).collect();
        assert_eq!(ids, vec!["ws-current"]);
    }

    #[tokio::test]
    async fn delete_clients_removes_client_and_acks() {
        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };

        let (tx1, mut rx1) = tokio::sync::mpsc::channel::<WsCommand>(256);
        engine.register_client(
            "client1",
            "ws1",
            "cg1",
            &shard,
            None,
            Arc::new(DirectWebSocketSink::new(tx1)),
        );
        let (tx2, _rx2) = tokio::sync::mpsc::channel::<WsCommand>(256);
        engine.register_client(
            "client2",
            "ws2",
            "cg1",
            &shard,
            None,
            Arc::new(DirectWebSocketSink::new(tx2)),
        );

        let existing_rows: RowRecordMap = HashMap::new();
        let q = |h: &str| DesiredQuerySpec {
            hash: h.to_string(),
            ast: Some(serde_json::json!({"table": "users"})),
            name: None,
            args: None,
            ttl: None,
        };

        let cvr = engine
            .config_and_hydrate(
                super::empty_cvr("cg1", "v1"),
                "client1",
                &["ws1".to_string()],
                &shard,
                vec![q("q1")],
                Vec::new(),
                false,
                None,
                None,
                &serde_json::json!({}),
                None,
                "00".to_string(),
                "v1".to_string(),
                &existing_rows,
                0,
                0,
                0,
            )
            .await
            .unwrap();
        let cvr = engine
            .config_and_hydrate(
                cvr,
                "client2",
                &["ws2".to_string()],
                &shard,
                vec![q("q2")],
                Vec::new(),
                false,
                None,
                None,
                &serde_json::json!({}),
                None,
                "00".to_string(),
                "v1".to_string(),
                &existing_rows,
                0,
                0,
                0,
            )
            .await
            .unwrap();
        assert!(cvr.clients.contains_key("client1"));
        assert!(cvr.clients.contains_key("client2"));

        // Delete client2, poking both connected clients + acking.
        let cvr = engine
            .delete_clients(
                cvr,
                &shard,
                &["client2".to_string()],
                &["client2".to_string()],
                &[],
                &["ws1".to_string(), "ws2".to_string()],
                0,
                0,
                0,
            )
            .await
            .unwrap();
        assert!(cvr.clients.contains_key("client1"));
        assert!(
            !cvr.clients.contains_key("client2"),
            "client2 removed from CVR"
        );

        // client1 received a deleteClients ack naming client2.
        let mut saw_ack = false;
        while let Ok(WsCommand::Send(v)) = rx1.try_recv() {
            if v[0] == "deleteClients"
                && let Some(ids) = v[1]["clientIDs"].as_array()
                && ids.iter().any(|x| x == "client2")
            {
                saw_ack = true;
            }
        }
        assert!(saw_ack, "expected deleteClients ack naming client2");
    }
}
