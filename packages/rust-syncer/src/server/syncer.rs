//! Port of `zero-cache/src/server/syncer.ts` — the syncer worker bootstrap's
//! per-CG services seat (L9 Stage 5c move out of `main.rs`): TS builds the
//! view-syncer/mutagen/pusher services + their config there; rust's twin is
//! the `CGServicesFactory` the executors call per client group.

use std::sync::Arc;

use crate::config::zero_config::SyncerConfig;
use crate::services::view_syncer::view_syncer::CGServicesFactory;

/// Per-CG services factory. Builds a real `SyncEngine` config from the process
/// config (replica path, CVR Postgres, shard). `create_mutagen` returns `None`
/// (legacy CRUD is disabled by design — a CRUD push gets the "disabled"
/// rejection); `create_pusher` builds the LIVE Option-A push relay when
/// `PUSHER_URL` is set. Connection-context state is owned by the per-CG
/// `ConnectionContextManager`, dispatched to the handler via
/// `CcmDispatchAdapter` (view_syncer.rs).
pub struct RealServicesFactory {
    pub config: Arc<SyncerConfig>,
    pub tokio_handle: tokio::runtime::Handle,
    pub metrics: Arc<crate::metrics::Metrics>,
    /// Shared with the router: lets the per-CG push relay deliver a PushFailed
    /// frame back to a client's socket when a relay POST fails on the drainer.
    pub connection_sinks: crate::ConnectionSinks,
}

impl CGServicesFactory for RealServicesFactory {
    // The Rust syncer runs ZERO mutation logic. CRUD mutations (mutagen → PG)
    // genuinely require mutation processing and stay unsupported here (no app
    // uses them on this path); a CRUD push still hits the "legacy CRUD disabled"
    // rejection.
    fn create_mutagen(&self, _cg_id: &str) -> Option<Arc<dyn crate::MutagenDispatch>> {
        None
    }

    // Custom mutations are RELAYED, not processed. When `PUSHER_URL` is set, a
    // custom WS push is forwarded (with this connection's auth/header material)
    // to the TS push endpoint, which runs the real pusher → `userPushURL`. The
    // result flows back through the CVR's `lmids`/`mutationResults` queries this
    // syncer already hydrates and pokes — so the relay is one-directional and
    // adds no mutation logic here. With `PUSHER_URL` unset, a custom push hits
    // the read-only rejection (the prior behavior).
    fn create_pusher(&self, _cg_id: &str) -> Option<Arc<dyn crate::PusherDispatch>> {
        let url = self.config.pusher_url.clone()?;
        Some(Arc::new(crate::PusherService::new(
            url,
            self.config.pusher_auth_token.clone(),
            self.tokio_handle.clone(),
            self.connection_sinks.clone(),
        )))
    }

    fn create_sync_engine_config(&self, cg_id: &str) -> crate::SyncEngineConfig {
        let shard_num = self.config.shard.parse::<u32>().unwrap_or(0);
        let mut initialization_errors = Vec::new();
        let replica_version =
            match crate::read_replica_versions_from_path(&self.config.replica_file) {
                Ok(versions) => versions.replica_version,
                Err(error) => {
                    initialization_errors.push(format!(
                        "failed to read replica versions from {}: {error}",
                        self.config.replica_file
                    ));
                    String::new()
                }
            };
        // Read the syncable table specs + read-permissions from the replica.
        // TS `#initAndResetCommon`: `computeZqlSpecs(lc, db, opts, tableSpecs, fullTables)`
        // — the syncable specs AND every replica table (`checkClientSchema` input).
        let mut full_tables = Vec::new();
        let tables = match crate::db::lite_tables::open_replica_read_only(&self.config.replica_file)
            .and_then(|conn| {
                // pipeline-driver.ts:359 `{includeBackfillingColumns: false}`.
                crate::compute_zql_specs(
                    &conn,
                    &crate::ZqlSpecOptions {
                        include_backfilling_columns: false,
                    },
                    Some(&mut full_tables),
                )
            }) {
            Ok(tables) => tables,
            Err(error) => {
                initialization_errors.push(format!(
                    "failed to read replica table specs from {}: {error}",
                    self.config.replica_file
                ));
                Vec::new()
            }
        };
        tracing::info!(
            "CG {cg_id}: loaded {} table specs from replica",
            tables.len()
        );
        let load_result: Result<Option<serde_json::Value>, String> =
            crate::db::lite_tables::open_replica_read_only(&self.config.replica_file)
                .and_then(|conn| crate::load_permissions(&conn, &self.config.app_id))
                .map(|loaded| loaded.permissions);
        match &load_result {
            Ok(Some(_)) => tracing::info!("CG {cg_id}: loaded read-permissions from replica"),
            Ok(None) => {
                // Port of the `permissions === null` branch in TS `loadPermissions`
                // (`auth/load-permissions.ts`): warn to run `zero-deploy-permissions`
                // UNLESS custom endpoints are configured (`hasCustomEndpoints`) — a
                // deployment using a custom query+mutate API legitimately runs with
                // no deployed permissions doc, so the nudge would be noise. TS's
                // `hasCustomEndpoints = (push||mutate url) && (query||getQueries url)`
                // maps to `pusher_url` (write) AND `query_config` (read) here.
                // Emitted at this single CG-load consumer (not inside
                // `load_permissions`, which is called twice per load) to stay
                // single-fire.
                let has_custom_endpoints =
                    self.config.pusher_url.is_some() && self.config.query_config.is_some();
                if !has_custom_endpoints {
                    let app_id_flag = if self.config.app_id == "zero" {
                        String::new()
                    } else {
                        format!(" --app-id={}", self.config.app_id)
                    };
                    // TS warn text (load-permissions.ts:38-42) — note queries do
                    // NOT pass through: the view-syncer transforms with
                    // `?? {tables: {}}` (deny-all) until a doc is deployed.
                    tracing::warn!(
                        "CG {cg_id}: No upstream permissions deployed. Run \
                         'npx zero-deploy-permissions{app_id_flag}' to enforce \
                         permissions."
                    );
                }
            }
            // Fail CLOSED: an existing-but-unloadable permissions doc must not
            // silently disable authorization. `resolve_permissions` substitutes a
            // deny-all config so no unauthorized row is served.
            Err(e) => tracing::error!(
                "CG {cg_id}: failed to load permissions ({e}); denying all client queries (fail-closed)"
            ),
        }
        // Re-read the hash separately for hot-reload detection. It shares the
        // load path but we keep it even when `resolve_permissions` substitutes
        // deny-all on error: a later successful read with a real hash then
        // differs from this seed and triggers a self-healing reload.
        let permissions_hash: Option<String> =
            rusqlite::Connection::open(&self.config.replica_file)
                .ok()
                .and_then(|conn| crate::load_permissions(&conn, &self.config.app_id).ok())
                .and_then(|loaded| loaded.hash);
        let permissions = crate::resolve_permissions(load_result);
        let app_id = self.config.app_id.clone();
        crate::SyncEngineConfig {
            initialization_error: (!initialization_errors.is_empty())
                .then(|| initialization_errors.join("; ")),
            tables,
            full_tables,
            replica_path: Some(self.config.replica_file.clone()),
            app_id: app_id.clone(),
            replica_version,
            shard: rust_cvr::shards::ShardID {
                app_id: app_id.clone(),
                shard_num,
            },
            cvr_pg: Some(crate::CvrPgConfig {
                // Identity only; the pool is supplied by the hosting executor
                // (doc 91, §5.1). Every CG on a given executor draws from that
                // executor's bounded pool.
                schema: format!("{}_{}/cvr", app_id, self.config.shard),
                cvr_id: cg_id.to_string(),
                task_id: self.config.task_id.clone(),
            }),
            permissions,
            permissions_hash,
            revalidate_interval_ms: self.config.revalidate_interval_ms,
            query_config: self.config.query_config.clone(),
            enable_query_covering: self.config.enable_query_covering,
            enable_query_planner: self.config.enable_query_planner,
            // TS server/syncer.ts:209-213:
            //   priorityOpRunningYieldThresholdMs = max(yieldThresholdMs / 4, 2)
            //   normalYieldThresholdMs = max(yieldThresholdMs, 2)
            priority_op_running_yield_threshold_ms: (self.config.yield_threshold_ms / 4.0).max(2.0),
            normal_yield_threshold_ms: self.config.yield_threshold_ms.max(2.0),
            tokio_handle: self.tokio_handle.clone(),
            admin_password: self.config.admin_password.clone(),
            server_version: self.config.server_version.clone(),
            metrics: self.metrics.clone(),
        }
    }
}
