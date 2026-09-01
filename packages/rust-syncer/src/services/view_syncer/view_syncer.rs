//! `ViewSyncerService` — port of
//! `zero-cache/src/services/view-syncer/view-syncer.ts`.
//!
//! The `!Send` serving core that owns ONE client group's world: the IVM
//! pipelines, the CVR store handle + row cache, the per-client poke sinks, and
//! the connection/auth state (`ConnectionContextManager`). Everything the client
//! observes — hydrate, advance, diff, poke, CVR flush, inspector metrics —
//! happens here, driven by `cg_event_loop` (this file), which runs as a
//! `spawn_local` task on one of the `K` sharded executor threads (doc 91 — there
//! is no per-CG OS thread). CVR Postgres I/O is `offload`ed onto the main
//! multi-thread runtime so it never blocks this serial thread's CPU.
//!
//! What is NOT here (moved out in the L9 refactor, task #160): the connection
//! ROUTER — accepting a socket, JWT validation, `place_cg`, the
//! `DashMap<client_group_id, CGHandle>`, and emitting the `connected` ack — lives
//! in `workers/syncer.rs` (`create_connection`, port of TS
//! `Syncer.#createConnection`/`handleConnection`). Crucially, the `connected` ack
//! is sent THERE, on the per-connection accept task, BEFORE the connection is
//! handed to this serial CG thread — decoupling the connect-ack from
//! `config_and_hydrate` (TS parity; the 2026-08-27 prod fix, task #152).

use crate::auth::read_authorizer::{hash_of_ast, transform_and_hash_query};
use crate::custom_queries::transform_query::{
    CustomQueryContext, CustomQuerySpec, CustomTransformed, transform_custom_queries,
};
use crate::services::view_syncer::connection_context_manager::{
    CCMError, ConnectParamsForRegistration, ConnectionContext as CcmConnectionContext,
    ConnectionContextManager, ConnectionSelector as CcmConnectionSelector, ConnectionValidation,
    FetchConfig, InitConnectionBody, MaintenanceKind, UpdateAuthBody, resolve_auth,
};
use crate::services::view_syncer::pipeline_driver::{
    AdvanceOutcome, IvmPipelines, IvmTableSpec, json_to_value,
};
use crate::services::view_syncer::query_covering::{
    QueryCoverageShadowHit, QueryCoveringIndex, RunningQuery,
};
#[cfg(test)]
use crate::workers::cg_executor::CGHandle;
use crate::workers::cg_executor::{CGMessage, CgTaskContext};
use crate::workers::connect_params::ConnectParams;
use crate::workers::connection::Connection;
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
use rust_cvr::row_record_cache::RowRecordCache;
use rust_cvr::schema::types::{
    CVRVersion, EMPTY_CVR_VERSION, NullableCVRVersion, cmp_versions, maybe_version_string,
    version_string, version_to_cookie,
};
use rust_cvr::schema::types::{ClientSchema, QueryRecord, RowID, RowRecord};
use rust_cvr::shards::ShardID;
use rust_cvr::ttl_clock::TTLClock;
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
    /// replica, or `None` if none are deployed. A `None` doc still transforms
    /// client-AST queries with an EMPTY config — deny-by-default per table
    /// (TS view-syncer.ts:1549 `?? {tables: {}}`; fixed 2026-08-28).
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
/// signal operators use to find pathological queries. Port of TS's
/// `slowHydrateThreshold` (view-syncer.ts / pipeline-driver.ts). Read once from
/// `ZERO_SLOW_HYDRATE_THRESHOLD_MS` (default 1000), cached.
///
/// `pub(crate)` so the pipeline driver's `VENDED` log gate reads the same
/// threshold — TS shares one `#logConfig.slowHydrateThreshold` across the
/// view-syncer's slow-hydrate log and the pipeline-driver's VENDED log.
pub(crate) fn slow_hydrate_threshold_ms() -> f64 {
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
        // forwarded as an Authorization-less POST — the 2026-08-29 prod
        // "No token provided" 401s. Never default; surface the error.
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
    /// URL/header overrides on its context (moved here from the CG-thread
    /// intercept in L9 Stage 3d — this dispatch is now the single site).
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

/// The live `ViewSyncerDispatch` (L9 Stage 3d) — TS `Connection` holds
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
// deliberate TS-`#lock` twin (L9 Stage 3d).
#[allow(clippy::await_holding_refcell_ref)]
#[async_trait::async_trait(?Send)]
impl ViewSyncerDispatch for CgViewSyncer {
    async fn change_desired_queries(&self, selector: &ConnectionSelector, msg: &str) {
        let Some(svc) = self.svc.upgrade() else {
            return;
        };
        let body = second_element(msg);
        svc.borrow_mut()
            .handle_desired_queries(&selector.client_id, &body, false)
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
            .handle_desired_queries(&selector.client_id, &body, true)
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
    /// Set when a store flush was MATERIAL (`flushed: true` in TS terms).
    /// Rust-only bridge: TS's `#flushUpdater` sees `{flushed}` directly and
    /// restarts the ttlClock interval on it (view-syncer.ts:1083-1086); here
    /// the router polls this flag after each dispatched message via
    /// `take_flush_observed` to do the same. `Cell` because the engine is
    /// single-threaded (`!Send`) and flush helpers take `&self`.
    flush_observed: std::cell::Cell<bool>,
    /// Live-instance census guard for the dissolved engine seat (the
    /// `SYNC_ENGINE` counter kept alive for /statz + the G7 gate; counts
    /// identically to `_census` since the merge).
    _engine_census: crate::live_count::Guard,
    /// Handle to this service's own shared cell, set by `cg_event_loop` right
    /// after construction (`None` in the storeless engine-surface scaffold).
    /// Rust-only (L9 Stage 3d): each connection's message handler gets a
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
    /// client_id → Connection. `Rc` (L9 Stage 3d): the inbound path clones the
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
    last_row_count: usize,
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
        // Engine seat (the former `SyncEngine::new`): pipelines + CVR-store
        // fields now live directly on the service, per TS ownership.
        let mut pipelines = IvmPipelines::new();
        // TS threads `config.enableQueryPlanner` into the PipelineDriver ctor
        // (server/syncer.ts:222); set before `init` so `build_engine` sees it.
        pipelines.enable_query_planner = config.enable_query_planner;
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

        let created_at = now_ms();
        let mut svc = ViewSyncerService {
            cg_id: cg_id.to_string(),
            pipelines,
            store: None,
            row_cache: None,
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
            replica_path,
            app_id,
            permissions,
            permissions_hash,
            next_auth_maintenance_at: None,
            background_retransform_failure: None,
            forced_retransform_outcomes: std::collections::VecDeque::new(),
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
            last_row_count: 0,
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

    /// TS `ViewSyncerService.servingLagEligible`: `#clients.size > 0 &&
    /// getBackgroundConnectionContext() !== undefined`. Approximated here as
    /// "has a registered client with a live connection to serve".
    fn serving_lag_eligible(&self) -> bool {
        !self.registered_ws.is_empty() && !self.connections.is_empty()
    }

    /// TS `ViewSyncerService.queryCount`: `#pipelines.initialized() ?
    /// #pipelines.queries().size : 0`. The count of active (hydrated) queries.
    fn query_count(&mut self) -> usize {
        self.pipelines().active_query_ids().len()
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
            match self.load_cvr(self.last_connect_time as f64).await {
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
        let client_ids: Vec<String> = self.registered_ws.values().cloned().collect();
        let existing_rows = self.existing_rows().await;
        self.last_row_count = existing_rows.len();
        match self
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
                self.fail_group("TTL expiry sync failed");
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
    fn arm_auth_maintenance(&mut self) {
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
    async fn on_auth_maintenance_tick(&mut self) {
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
            self.arm_auth_maintenance();
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

        // Probe + record each surviving due connection (TS `#validateConnection`
        // per `dueRevalidations` entry).
        let shard = self.shard.clone();
        let plan_by_client: std::collections::HashMap<String, (String, u32)> = plan
            .due_revalidations
            .iter()
            .map(|c| (c.client_id.clone(), (c.ws_id.clone(), c.revision)))
            .collect();
        for client_id in survivors {
            let Some((ws_id, revision)) = plan_by_client.get(&client_id).cloned() else {
                continue;
            };
            if self.registered_ws.get(&client_id) != Some(&ws_id) {
                continue;
            }
            let selector = CcmConnectionSelector {
                client_id: client_id.clone(),
                ws_id: ws_id.clone(),
            };

            // Server-side revocation probe. Port of TS `#validateConnection` →
            // `CustomQueryTransformer.validate`: when a custom query API is
            // configured for this client, POST an empty transform so the API
            // server can reject a token that is cryptographically valid but
            // revoked/deauthorized at the app layer (local `validate_auth` above
            // only catches expiry/signature/user-swap). No query API configured →
            // no ctx → no probe (the TS `client-fallback` path). An AUTH error
            // (401/403) invalidates the connection; a transient failure (API down
            // / 5xx) DEFERS the remaining maintenance — keep the connection and
            // retry at the deferred deadline (TS `deferMaintenance('revalidate')`
            // + early return), never close on a blip.
            let ctx = self.query_context_for(&client_id, &ws_id);
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
                            lock_unpoisoned(&self.ccm).fail_connection(&selector, revision);
                            if let Some(conn) = self.connections.get(&client_id) {
                                conn.close_with_error(crate::protocol::ErrorBody::unauthorized(
                                    "Connection auth validation failed",
                                ));
                            }
                            if let Some(ws) = self.registered_ws.get(&client_id).cloned() {
                                self.on_connection_closed(&client_id, &ws);
                            }
                            continue;
                        }
                        // Exact TS wording (view-syncer.ts:841-848): the message
                        // string mirrors TS verbatim; cg/client go in structured
                        // fields the way TS passes `{clientID, wsID, message}`.
                        tracing::warn!(
                            cg_id = %self.cg_id,
                            client_id = %client_id,
                            "Scheduled auth revalidation failed; deferring auth maintenance"
                        );
                        lock_unpoisoned(&self.ccm).defer_maintenance(MaintenanceKind::Revalidate);
                        self.arm_auth_maintenance();
                        return;
                    }
                }
            }

            // Record the successful (re)validation: refreshes `revalidate_at`
            // and lets the CCM promote/refresh the background connection (TS
            // `connContextManager.validateConnection(connCtx, revision,
            // validation)` with the client-fallback kind — the probe returns no
            // server-validated userID).
            if let Err(e) = lock_unpoisoned(&self.ccm).validate_connection(
                &selector,
                revision,
                &ConnectionValidation::ClientFallback,
            ) {
                tracing::warn!(
                    "CG {}: recording revalidation failed for client {client_id}: {e:?}",
                    self.cg_id
                );
            }
        }

        // Revalidation can change which connection is safe for shared background
        // work — replan before deciding on the group retransform (TS
        // `refreshedPlan`, view-syncer.ts:858). ONE retransform for the group on
        // the background connection's context (TS `#runBackgroundRetransform`),
        // not one per survivor: `handle_desired_queries(_, {}, changed=true)`
        // re-runs the whole CG's config/hydrate pass, so a single call already
        // re-fetches every query with current auth/permissions.
        let refreshed = lock_unpoisoned(&self.ccm).plan_maintenance();
        if refreshed.due_retransform {
            self.run_background_retransform().await;
        }

        // Re-arm from the refreshed plan (TS `#scheduleAuthMaintenance` in the
        // locked-op `finally`).
        self.arm_auth_maintenance();
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
                conn.close_with_error(error);
            }
            self.on_connection_closed(&conn_ctx.client_id, &conn_ctx.ws_id);
        }
    }

    /// Re-run the group's query pipelines under a background connection's auth and
    /// report the outcome. Port of the inner `attemptRetransform` closure of TS
    /// `#runBackgroundRetransform` (view-syncer.ts:2669): `#syncQueryPipelineSet('all')`
    /// then `markBackgroundRetransformSuccess`. Here the re-hydrate is
    /// `handle_desired_queries(_, {}, is_init=true)` (the whole-CG config/hydrate
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
        self.handle_desired_queries(&bg.client_id, &empty_body, true)
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
    /// borrow across the handler (L9 Stage 3d).
    async fn on_new_connection(
        &mut self,
        params: ConnectParams,
        sink: DirectWebSocketSink,
    ) -> Option<(Arc<str>, Arc<str>, String)> {
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
            self.fail_client(
                &prev_ws_id,
                "Connection superseded by a newer connection for the same clientID",
            );
            self.unregister_client(&prev_ws_id);
            self.decrement_active_client(&prev_ws_id);
        }

        // Register the client with the SyncEngine so notifications can poke it.
        let cvr_sink: Arc<dyn rust_cvr::client_handler::WebSocketSink> = Arc::new(sink.clone());
        // Staged clones: the borrow checker cannot split `&mut self` (the
        // receiver) from `&self.<field>` args now that the engine methods live
        // on the service itself.
        let shard = self.shard.clone();
        self.register_client(
            &client_id,
            &ws_id,
            &client_group_id,
            &shard,
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
            let mut ccm = lock_unpoisoned(&self.ccm);
            ccm.register_connection(&selector, &reg, auth);
            // TS validates the new connection right away (initConnection →
            // `#validateConnection` → `connContextManager.validateConnection`,
            // view-syncer.ts:942): recording the validation gives the
            // connection its `revalidate_at` deadline and lets the group
            // promote a background connection. The transport-level JWT check
            // already passed upstream; with no server-validated userID in hand
            // this is the TS `client-fallback` validation.
            if let Ok(ctx) = ccm.must_get_connection_context(&selector)
                && let Err(e) = ccm.validate_connection(
                    &selector,
                    ctx.revision,
                    &ConnectionValidation::ClientFallback,
                )
            {
                tracing::warn!(
                    "CG {}: connect-time validateConnection failed: {e:?}",
                    self.cg_id
                );
            }
        }

        // Recompute the auth-maintenance wakeup now that the CCM has a newly
        // validated connection (TS re-arms via `#scheduleAuthMaintenance` after
        // every locked operation).
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

        // The live dispatch (L9 Stage 3d): the handler's `viewSyncer.<method>`
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
            return None;
        }

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
        is_init: bool,
    ) -> bool {
        let Some(ws_id) = self.registered_ws.get(client_id).cloned() else {
            tracing::warn!(
                "CG {}: desired queries for unregistered client {client_id}",
                self.cg_id
            );
            return false;
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
            return false;
        }
        // The custom-query API context (`userQueryURL` + allowlisted headers)
        // was recorded on the ConnectionContextManager by the handler's
        // `connContextManager.initConnection(...)` dispatch BEFORE this method
        // ran (TS `SyncerWsMessageHandler` 'initConnection' — the recording
        // moved to `CcmDispatchAdapter::init_connection` in L9 Stage 3d);
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
        if !is_init && !has_query_change && !has_deletions {
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
                    conn.close_with_error(crate::protocol::ErrorBody::client_not_found(message));
                }
                self.on_connection_closed(client_id, &ws_id);
                return false;
            }
            Err(error) => {
                tracing::error!("CG {}: unable to load CVR: {error}", self.cg_id);
                self.fail_group("Unable to load the client view state");
                return false;
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
            return false;
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
            return false;
        }

        // Query-config pass (records the client + desired queries, hydrates,
        // then catches the client up). Always runs on initConnection.
        let mut config_accepted = false;
        if is_init || has_query_change {
            let cvr = self.cvr.take().unwrap();
            let state_version = self
                .pipelines()
                .current_version()
                .unwrap_or_else(|| cvr.version.state_version.clone());
            let replica_version = self.replica_version.clone();
            // The rows the client already has (from the CVR row cache).
            let existing_rows = self.existing_rows().await;
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
            // Staged clones: `&mut self` receiver vs `&self.<field>` args (the
            // dissolved engine methods live on the service itself now).
            let shard = self.shard.clone();
            let profile_id = self.client_profile_ids.get(client_id).cloned();
            let permissions = self.permissions.clone();
            match self
                .config_and_hydrate_with_profile(
                    cvr,
                    client_id,
                    &all_ws_ids,
                    &shard,
                    puts,
                    dels,
                    clear,
                    client_schema,
                    profile_id.as_deref(),
                    permissions.as_ref(),
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
                    // Pipelines are synced — the group's shared background
                    // retransform may now run (TS view-syncer.ts:607:
                    // `#pipelinesSynced = true; setSharedRetransformReady(true)`).
                    lock_unpoisoned(&self.ccm).set_shared_retransform_ready(true);
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
        // The relayed-push token is NOT snapshotted here — every relay reads the
        // CCM's current auth fresh (handler `relay_headers_for` / router
        // deleteClients cleanup), so refreshing the CCM below is sufficient. TS
        // parity: pusher.ts reads `mustGetConnectionContext` fresh on every push.
        // Refresh the auth on the ConnectionContextManager (TS `updateAuth`).
        if let Some(ws_id) = self.registered_ws.get(client_id).cloned() {
            let selector = CcmConnectionSelector {
                client_id: client_id.to_string(),
                ws_id,
            };
            let mut ccm = lock_unpoisoned(&self.ccm);
            let _ = ccm.update_auth(
                &selector,
                &UpdateAuthBody {
                    auth: Some(token.to_string()),
                },
            );
            // TS re-validates on updateAuth (view-syncer.ts:1012 →
            // `connContextManager.validateConnection`): record against the
            // BUMPED revision so the refreshed credential gets a fresh
            // `revalidate_at` deadline.
            if let Ok(ctx) = ccm.must_get_connection_context(&selector)
                && let Err(e) = ccm.validate_connection(
                    &selector,
                    ctx.revision,
                    &ConnectionValidation::ClientFallback,
                )
            {
                tracing::warn!(
                    "CG {}: updateAuth validateConnection failed: {e:?}",
                    self.cg_id
                );
            }
        }
        let empty_body = serde_json::json!({});
        self.handle_desired_queries(client_id, &empty_body, true)
            .await;
        // Locked-op re-arm (TS `#scheduleAuthMaintenance` in the `finally`).
        self.arm_auth_maintenance();
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
        // Staged clone: `&mut self` receiver vs `&self.shard` arg.
        let shard = self.shard.clone();
        match self
            .delete_clients(
                cvr,
                &shard,
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
        // the group idles out — port of TS `#removeClient`'s clients-empty
        // branch (view-syncer.ts:761-766; the `#ttlClock !== undefined` guard
        // is the loaded-CVR check inside the callee).
        if self.connections.is_empty() {
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
            // the G49 ownership differential (2026-08-28): rust=Rehome, TS=none.
            conn.close("Connection superseded by a newer connection");
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
        match crate::auth::load_permissions::reload_permissions_if_changed(
            &conn,
            &self.app_id,
            self.permissions_hash.as_deref(),
        ) {
            crate::auth::load_permissions::PermissionsReload::Unchanged => false,
            crate::auth::load_permissions::PermissionsReload::Changed { permissions, hash } => {
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
        let existing_rows = self.existing_rows().await;
        self.last_row_count = existing_rows.len();
        let now = now_ms();
        let ttl_clock = self.get_ttl_clock(now);
        let advance_started = std::time::Instant::now();
        crate::trace::note(
            "advance-start",
            &format!("cg={} clients={}", self.cg_id, client_ids.len()),
        );
        match self
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
                .pipelines()
                .current_version()
                .unwrap_or_else(|| cvr.version.state_version.clone());
            let replica_version = self.replica_version.clone();
            let existing_rows = self.existing_rows().await;
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
            // Staged clones (same &mut-receiver split as above).
            let shard = self.shard.clone();
            let profile_id = self.client_profile_ids.get(&client_id).cloned();
            let permissions = self.permissions.clone();
            match self
                .config_and_hydrate_with_profile(
                    cvr.clone(),
                    &client_id,
                    &all_ws_ids,
                    &shard,
                    Vec::new(),
                    Vec::new(),
                    false,
                    None,
                    profile_id.as_deref(),
                    permissions.as_ref(),
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
/// task (L9 Stage 3d — the CG-thread tag interception is gone; the handler is
/// the single dispatch). Three phases so the service cell is NOT borrowed
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
            .on_connection_closed(&client_id, &ws_id);
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
// deliberate TS-`#lock` twin (L9 Stage 3d).
#[allow(clippy::await_holding_refcell_ref)]
pub(crate) async fn cg_event_loop(
    cg_id: &str,
    mut rx: mpsc::UnboundedReceiver<CGMessage>,
    connection_count: Arc<AtomicU64>,
    accepting: Arc<AtomicBool>,
    ctx: CgTaskContext,
    last_notification: Option<serde_json::Value>,
) {
    // The service lives in a shared cell (L9 Stage 3d): the per-connection
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
        if let Some(CGMessage::NewConnection { params, sink }) = rx.recv().await {
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
            sink.fail(crate::protocol::ErrorBody::internal(
                "Failed to initialize the client-group sync engine",
            ));
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
                            tracing::info!("closing clientGroupID={cg_id}");
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
        CGMessage::NewConnection { params, sink } => {
            let piggyback = state_rc.borrow_mut().on_new_connection(*params, sink).await;
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
        } => on_inbound(state_rc, client_id, ws_id, text).await,
        CGMessage::ConnectionClosed { client_id, ws_id } => state_rc
            .borrow_mut()
            .on_connection_closed(&client_id, &ws_id),
        CGMessage::CloseConnection { client_id, ws_id } => {
            state_rc.borrow_mut().close_connection(&client_id, &ws_id)
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
            state_rc.borrow_mut().on_notification(merged).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::PROTOCOL_VERSION;
    use crate::workers::syncer_ws_message_handler::{
        ConnContextManagerDispatch, ConnectionSelector,
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

    /// Wrap a test service in the shared cell + self-handle the live dispatch
    /// needs (L9 Stage 3d) — the test twin of `cg_event_loop`'s setup.
    fn shared(state: ViewSyncerService) -> Rc<RefCell<ViewSyncerService>> {
        let rc = Rc::new(RefCell::new(state));
        let weak = Rc::downgrade(&rc);
        rc.borrow_mut().self_handle = Some(weak);
        rc
    }

    struct TestFactory {
        handle: tokio::runtime::Handle,
    }
    impl CGServicesFactory for TestFactory {
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
                enable_query_planner: true,
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
                enable_query_planner: true,
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
        let mut state = ViewSyncerService::new_test(
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

    fn seed_test_client_schema(state: &mut ViewSyncerService) {
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

    pub(super) fn pinned_params(client_id: &str, ws_id: &str, user_id: &str) -> ConnectParams {
        let mut p = authed_params(client_id, ws_id, &fake_jwt(user_id));
        p.user_id = Some(user_id.to_string());
        p
    }

    /// The raw auth token the ConnectionContextManager holds for a connection.
    /// Replaces the deleted `client_raw_auth` map in tests — the CCM is now the
    /// single owner of per-connection auth (I-8).
    fn ccm_raw_auth(state: &ViewSyncerService, client_id: &str, ws_id: &str) -> Option<String> {
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
                enable_query_planner: true,
                tokio_handle: self.handle.clone(),
                admin_password: None,
                server_version: "test".to_string(),
                metrics: std::sync::Arc::new(crate::metrics::Metrics::default()),
            }
        }
    }

    pub(super) fn revalidate_state(
        rt: &tokio::runtime::Runtime,
        interval_ms: Option<i64>,
        valid: Arc<std::sync::atomic::AtomicBool>,
    ) -> ViewSyncerService {
        let factory: Arc<dyn CGServicesFactory> = Arc::new(RevalidateFactory {
            handle: rt.handle().clone(),
            revalidate_interval_ms: interval_ms,
        });
        ViewSyncerService::new_test(
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
    /// Port-parity for `#checkForThrashing` (view-syncer.ts:2121-2148): three
    /// transformation replacements INSIDE the 60s window reach the warn
    /// threshold; a replacement OUTSIDE the window starts a FRESH record
    /// (count back to 1 — TS deletes + reinserts `{count: 1}`), and records
    /// are per-query. Pins the window-reset branch: a port that only ever
    /// increments would pass the in-window assertions but fail the reset one.
    #[test]
    fn check_for_thrashing_window_and_threshold() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, None, valid);

        state.check_for_thrashing("q1");
        assert_eq!(state.query_replacements.get("q1").unwrap().count, 1);
        state.check_for_thrashing("q1");
        state.check_for_thrashing("q1");
        assert_eq!(
            state.query_replacements.get("q1").unwrap().count,
            3,
            "3 in-window replacements must reach the TS THRASH_THRESHOLD"
        );

        // Age the record past THRASH_WINDOW_MS (60s): the next replacement
        // must reset, not increment to 4.
        state.query_replacements.get_mut("q1").unwrap().window_start = now_ms() - 60_001;
        state.check_for_thrashing("q1");
        let rec = state.query_replacements.get("q1").unwrap();
        assert_eq!(
            rec.count, 1,
            "outside-window replacement starts a fresh count"
        );
        assert!(now_ms() - rec.window_start < 60_000, "fresh window start");

        // Records are per-query (TS keys #queryReplacements by queryID).
        state.check_for_thrashing("q2");
        assert_eq!(state.query_replacements.get("q2").unwrap().count, 1);
        assert_eq!(state.query_replacements.get("q1").unwrap().count, 1);
    }

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
        // Interval 0 → the CCM marks the connection due immediately, so the
        // manually-fired tick below actually has revalidation work (the plan-
        // driven tick honors `revalidate_at`; an early fire is a no-op).
        let mut state = revalidate_state(&rt, Some(0), valid.clone());

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
        let cell = shared(revalidate_state(&rt, Some(300_000), valid));

        // Connect "foo" on ws1, then reconnect "foo" on ws2 (supersedes ws1).
        let (tx1, _d1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let _ = rt.block_on(cell.borrow_mut().on_new_connection(
            authed_params("foo", "ws1", "tok"),
            DirectWebSocketSink::new(tx1),
        ));
        let (tx2, _d2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let _ = rt.block_on(cell.borrow_mut().on_new_connection(
            authed_params("foo", "ws2", "tok"),
            DirectWebSocketSink::new(tx2),
        ));
        assert_eq!(
            cell.borrow().registered_ws.get("foo").map(String::as_str),
            Some("ws2"),
            "reconnect should supersede ws1 with ws2"
        );

        // deleteClients targeting "foo" arrives on the STALE ws1 → must be dropped.
        rt.block_on(on_inbound(
            &cell,
            "foo".into(),
            "ws1".into(),
            r#"["deleteClients",{"clientIDs":["foo"]}]"#.to_string(),
        ));

        // The stale frame was ignored: "foo" is still registered (on ws2), not
        // deleted.
        assert_eq!(
            cell.borrow().registered_ws.get("foo").map(String::as_str),
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
        // Opaque token (not a JWT) WITH a userID — the CCM records the token so the
        // unchanged-check compares against a REAL previous opaque auth (this is what
        // makes the raw-vs-decoded distinction non-vacuous: two opaque tokens both
        // decode to `{}`, so a decoded-claims comparison would falsely skip). The
        // group pins to `user-1`; the opaque refresh must NOT be closed by the pin.
        let mut params = authed_params("c1", "ws1", "opaque-token-1");
        params.user_id = Some("user-1".to_string());
        rt.block_on(state.on_new_connection(params, DirectWebSocketSink::new(tx)));
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
        // the token and the unchanged-check can compare against it. A pinned group
        // must NOT close an opaque refresh (opaque tokens carry no `sub`).
        let mut params = authed_params("c1", "ws1", "opaque-token-1");
        params.user_id = Some("user-1".to_string());
        rt.block_on(state.on_new_connection(params, DirectWebSocketSink::new(tx)));

        rt.block_on(state.handle_update_auth("c1", "opaque-token-1"));
        assert_eq!(
            state.metrics.snapshot()["authChanges"],
            0,
            "an unchanged opaque token must NOT trigger a re-transform"
        );
        // Passing for the RIGHT reason: the connection SURVIVES (an unchanged skip,
        // not a pin-mismatch close). Before the opaque sub-pin fix, a pinned group
        // closed the connection here — which also read authChanges==0, masking the
        // divergence.
        assert_eq!(
            state.registered_ws.len(),
            1,
            "an unchanged opaque refresh must keep the connection open"
        );
    }

    /// I-8: the message handler's connection-context dispatch is backed by the
    /// ported CCM (`CcmDispatchAdapter`), NOT the `auth:None` placeholder — so the
    /// handler's live reads (mutagen-CRUD auth + relayed-push auth) see the single
    /// owner's real per-connection auth. Pins that the adapter surfaces the CCM's
    /// auth + revision, and returns `None` for an unknown connection.
    ///
    /// NON-VACUOUS: the old `PlaceholderConnContextManager` returned `auth:None`
    /// unconditionally — this asserts a NON-None token, so wiring the placeholder
    /// (or breaking the adapter's `auth` mapping) fails the first assert.
    #[test]
    fn ccm_dispatch_adapter_surfaces_real_connection_auth() {
        use crate::services::view_syncer::connection_context_manager::{
            Auth, ConnectionContextManager,
        };
        let ccm = Arc::new(Mutex::new(ConnectionContextManager::new(
            None, None, None, None, None, None,
        )));
        let reg = ConnectParamsForRegistration {
            client_id: "c1".to_string(),
            ws_id: "ws1".to_string(),
            user_id: Some("user-1".to_string()),
            profile_id: None,
            base_cookie: None,
            protocol_version: 1,
            http_cookie: None,
            origin: None,
            request_headers: Vec::new(),
        };
        lock_unpoisoned(&ccm).register_connection(
            &CcmConnectionSelector {
                client_id: "c1".to_string(),
                ws_id: "ws1".to_string(),
            },
            &reg,
            Some(Auth::Opaque {
                raw: "the-token".to_string(),
            }),
        );

        let adapter = CcmDispatchAdapter::new(ccm);
        let info = adapter
            .must_get_connection_context(&ConnectionSelector {
                client_id: "c1".to_string(),
                ws_id: "ws1".to_string(),
            })
            .expect("registered connection must resolve");
        assert_eq!(
            info.auth.as_deref(),
            Some("the-token"),
            "adapter must surface the CCM's real auth, not the placeholder None"
        );

        // Unknown connection → the TS `mustGetConnectionContext` THROW
        // (InvalidConnectionRequest), NOT a defaulted `auth: None`. The old
        // "safe default" here is exactly what relayed Authorization-less
        // pushes in prod (2026-08-29 "No token provided" 401s).
        let missing = adapter
            .must_get_connection_context(&ConnectionSelector {
                client_id: "nope".to_string(),
                ws_id: "ws1".to_string(),
            })
            .expect_err("missing connection context must be an error, never a default");
        assert_eq!(
            *missing.kind(),
            crate::protocol::ErrorKind::InvalidConnectionRequest,
            "must mirror the TS mustGetConnectionContext ProtocolError kind"
        );
    }

    /// Regression (push-relay 401, prod incident 2026-08-27): the token forwarded
    /// on relayed custom-mutation pushes MUST track `updateAuth`. Rust once
    /// snapshotted the connect-time token and never refreshed it → the API server
    /// 401'd every mutation on any session longer than the token TTL.
    ///
    /// After the I-8 push-relay flip there is NO parallel auth cell: every relay
    /// fills `PushRelayHeaders.auth` fresh from `mustGetConnectionContext(selector)
    /// .auth` (handler `relay_headers_for` / router deleteClients cleanup / the
    /// `CcmDispatchAdapter`), so the forwarded token is whatever the CCM — the
    /// single owner — currently holds. This asserts exactly that value across an
    /// `updateAuth`.
    ///
    /// NON-VACUOUS: `updateAuth` (via the CCM) stores the new token; the relay
    /// reads the CCM at use time, so a broken adapter/mapping or a CCM that failed
    /// to refresh keeps forwarding the connect-time token and the second assert
    /// fails. (The `updateAuth` re-transform is exercised via the CCM directly
    /// because the storeless harness tears the connection down on a full
    /// `handle_update_auth` re-hydrate — a harness limitation, not the relay path.)
    #[test]
    fn update_auth_refreshes_the_forwarded_push_relay_token() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));

        // The value the relay forwards == the CCM's current auth (what the handler's
        // `relay_headers_for` / the router deleteClients cleanup read).
        assert_eq!(
            ccm_raw_auth(&state, "c1", "ws1").as_deref(),
            Some(fake_jwt("user-1").as_str()),
            "initial forwarded push token is the connect-time token"
        );

        // A refreshed token (same user, newer iat) flows through `updateAuth` into
        // the CCM; the relay then forwards the NEW token on subsequent pushes.
        let token2 = {
            use base64::Engine;
            let payload = serde_json::json!({"sub": "user-1", "iat": 2}).to_string();
            let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
            format!("hdr.{b64}.sig")
        };
        let _ = lock_unpoisoned(&state.ccm).update_auth(
            &CcmConnectionSelector {
                client_id: "c1".to_string(),
                ws_id: "ws1".to_string(),
            },
            &UpdateAuthBody {
                auth: Some(token2.clone()),
            },
        );
        assert_eq!(
            ccm_raw_auth(&state, "c1", "ws1").as_deref(),
            Some(token2.as_str()),
            "updateAuth must refresh the token the relay forwards \
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
        // Interval 0 → due immediately at the manual tick (see the expired test).
        let mut state = revalidate_state(&rt, Some(0), valid);

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
        // Interval 0 → due immediately at the manual tick (see the expired test).
        let mut state = revalidate_state(&rt, Some(0), valid);

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

    /// With the feature disabled (interval None) no deadline is ever armed. An
    /// UNTOKENED (cookie/anonymous) connection, however, IS scheduled: TS
    /// `validateConnection` stamps `revalidateAt` on every validated connection
    /// regardless of token presence, and maintenance re-validates it via the
    /// server-side probe (`#validateConnection` → transformer.validate).
    ///
    /// NON-VACUOUS for the plan-driven migration: the previous interval-driven
    /// arm skipped untokened connections entirely, so the `is_some` assert below
    /// fails against the old code.
    #[test]
    fn periodic_revalidation_disabled_never_arms_but_unauthed_is_scheduled() {
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

        // Enabled + no token → still validated, still scheduled (TS parity).
        let mut unauthed = revalidate_state(&rt, Some(300_000), valid);
        let (tx2, _d2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(
            unauthed.on_new_connection(test_params("c2", "ws2"), DirectWebSocketSink::new(tx2)),
        );
        assert!(
            unauthed.next_auth_maintenance_at.is_some(),
            "a validated cookie connection gets a revalidate deadline (TS \
             validateConnection stamps revalidateAt unconditionally)"
        );
    }

    /// The tick executes the CCM's PLAN, not a flat re-check of every
    /// connection: with a 300s revalidate interval, a tick fired right after
    /// connect finds nothing due and touches nothing.
    ///
    /// NON-VACUOUS: the previous interval-driven tick revalidated every tokened
    /// connection on ANY tick, so `authRevalidations == 0` fails against it.
    #[test]
    fn maintenance_honors_ccm_revalidate_deadlines() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));
        seed_test_client_schema(&mut state);
        assert!(state.next_auth_maintenance_at.is_some());

        // Fire the tick 300s EARLY: the connection's `revalidate_at` is not due,
        // so no revalidation work runs and the connection is untouched.
        rt.block_on(state.on_auth_maintenance_tick());
        assert_eq!(state.registered_ws.len(), 1);
        assert_eq!(state.metrics.snapshot()["authRevalidations"], 0);
        // Still armed for the real deadline.
        assert!(state.next_auth_maintenance_at.is_some());
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
        let mut state = ViewSyncerService::new_test(
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
        let mut state = ViewSyncerService::new_test(
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

    /// Port fidelity for the `#expiredQueriesTimer` / `#scheduleExpireEviction` /
    /// `#stopExpireTimer` trio (view-syncer.ts:278/1394/773). A config update
    /// arms the eviction timer for an inactive TTL query; the LAST client
    /// disconnecting must STOP it (TS `#deleteClientDueToDisconnect`,
    /// view-syncer.ts:767) so an idle group with no clients runs zero eviction —
    /// matching TS, which clears the timer on last disconnect and never re-arms
    /// it until a client reconnects. Non-vacuous: reverting the
    /// `stop_expire_timer()` call at the last-disconnect branch leaves the timer
    /// armed and the `is_none()` assertions below fail.
    #[test]
    fn last_disconnect_stops_the_eviction_timer() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let count = Arc::new(AtomicU64::new(0));
        let mut state = ViewSyncerService::new_test(
            "cg-expire",
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
        assert!(!state.connections.is_empty());

        // Build a CVR whose only query is inactive for its only client with a
        // 1s TTL, so `next_eviction_time` is Some — then arm the timer exactly
        // as `#handleConfigUpdate`'s tail does (view-syncer.ts:1390).
        let version = CVRVersion {
            state_version: "00".to_string(),
            config_version: None,
        };
        let mut client_state = std::collections::BTreeMap::new();
        client_state.insert(
            "c1".to_string(),
            rust_cvr::schema::types::ClientState {
                inactivated_at: Some(0),
                ttl: 1_000,
                version: version.clone(),
            },
        );
        let query = QueryRecord::Client(rust_cvr::schema::types::ClientQueryRecord {
            base: rust_cvr::schema::types::BaseQueryRecord {
                id: "q1".to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
            },
            ast: serde_json::json!({"table": "users"}),
            client_state,
            patch_version: None,
        });
        let mut queries = std::collections::BTreeMap::new();
        queries.insert("q1".to_string(), query);
        let cvr = CVR {
            id: "cg-expire".to_string(),
            version,
            last_active: 0,
            ttl_clock: 0,
            replica_version: Some("v1".to_string()),
            clients: std::collections::BTreeMap::new(),
            queries,
            client_schema: None,
            profile_id: None,
        };
        state.cvr = Some(cvr.clone());
        state.schedule_expire_eviction(&cvr);
        assert!(
            state.expired_queries_timer.is_some(),
            "a config update with an inactive TTL query must arm the eviction timer"
        );
        assert!(state.next_expiry_delay().is_some());

        // Last client disconnects → TS `#stopExpireTimer` (view-syncer.ts:767).
        state.on_connection_closed("c1", "ws1");
        assert!(state.connections.is_empty());
        assert!(
            state.expired_queries_timer.is_none(),
            "last disconnect must stop the eviction timer (TS #stopExpireTimer, \
             view-syncer.ts:767): an idle group with no clients runs 0 evictions"
        );
        assert!(state.next_expiry_delay().is_none());
    }

    /// A same-clientID supersede (CGMessage::CloseConnection → close_connection)
    /// must close the replaced socket FRAME-LESS, matching TS
    /// (view-syncer.ts:913 `client.close("replaced by wsID: …")` →
    /// `downstream.cancel()`). It must NOT send an `["error", …]` frame — a
    /// Rehome there tells the superseded client to reconnect elsewhere even
    /// though the SAME client already reconnected. Non-vacuous: reverting
    /// `close_connection` to `close_with_error(rehome(...))` makes an error frame
    /// appear and the assertion fails. (Caught by the G49 ownership differential:
    /// rust=Rehome, TS=none, 2026-08-28.)
    #[test]
    fn supersede_close_is_frameless_like_ts() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let count = Arc::new(AtomicU64::new(0));
        let mut state = ViewSyncerService::new_test(
            "cg-supersede",
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

        let (tx, mut drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let sink = DirectWebSocketSink::new(tx);
        rt.block_on(state.on_new_connection(test_params("c1", "ws1"), sink));
        assert_eq!(
            state.registered_ws.get("c1").map(String::as_str),
            Some("ws1")
        );
        // Drain any connect-time commands queued on this sink.
        while drx.try_recv().is_ok() {}

        // Supersede: the CG receives CloseConnection for the still-registered ws.
        state.close_connection("c1", "ws1");

        let mut saw_error_frame = false;
        let mut saw_close = false;
        while let Ok(cmd) = drx.try_recv() {
            match cmd {
                WsCommand::Fail(_) => saw_error_frame = true,
                WsCommand::Send { msg, .. } => {
                    if msg.get(0).and_then(|v| v.as_str()) == Some("error") {
                        saw_error_frame = true;
                    }
                }
                WsCommand::Close(_) | WsCommand::CloseWithCode { .. } => saw_close = true,
            }
        }
        assert!(
            !saw_error_frame,
            "supersede must NOT send an error frame — TS closes frame-less \
             (view-syncer.ts:913); a Rehome here is a spurious reconnect signal"
        );
        assert!(
            saw_close,
            "supersede must still close the superseded socket"
        );
    }

    #[test]
    fn idle_shutdown_requires_both_keepalive_expiry_and_zero_admissions() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let count = Arc::new(AtomicU64::new(0));
        let mut state = ViewSyncerService::new_test(
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
        let mut state = ViewSyncerService::new_test(
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
        let mut state = ViewSyncerService::new_test(
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
        let router = crate::workers::syncer::Syncer::new(
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
        let router = Arc::new(crate::workers::syncer::Syncer::new_with_limit(
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
        let router = Arc::new(crate::workers::syncer::Syncer::new_with_limit(
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
        rt.block_on(router.create_connection(ctx1));
        assert_eq!(router.cg_count(), 1);

        // 2. A second, distinct group at cap with no idle CG -> its sink must get
        //    a Rehome error, never ServerOverloaded.
        let (ctx2, _keep_alive2, mut sink2) = make_ctx("cgB", "cB", "wsB");
        rt.block_on(router.create_connection(ctx2));

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

    /// A Pusher whose `init_connection` blocks the CG thread on the first call,
    /// simulating a long synchronous `config_and_hydrate`. (The seam moved with
    /// the L9 Stage 3d un-interception: `pusher.initConnection` now fires from
    /// the handler's `initConnection` arm ON the CG task, inside the same
    /// dispatch that runs the config/hydrate pass — the old injectable
    /// placeholder-CCM call is gone.) Signals `entered` when it reaches the
    /// block and holds until the test flips `release`.
    struct BlockingPusher {
        entered: Arc<AtomicBool>,
        release: Arc<(Mutex<bool>, std::sync::Condvar)>,
        blocked_once: AtomicBool,
    }
    impl PusherDispatch for BlockingPusher {
        fn enqueue_push(
            &self,
            _selector: &ConnectionSelector,
            _body: &serde_json::Value,
            _headers: &crate::workers::syncer_ws_message_handler::PushRelayHeaders,
            _client_group_id: &str,
        ) -> crate::workers::connection::HandlerResult {
            crate::workers::connection::HandlerResult::Ok
        }
        fn init_connection(&self, _s: &ConnectionSelector) {
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
        fn ack_mutation_responses(
            &self,
            _selector: &ConnectionSelector,
            _body: &serde_json::Value,
            _headers: &crate::workers::syncer_ws_message_handler::PushRelayHeaders,
            _client_group_id: &str,
        ) {
        }
        fn delete_client_mutations(
            &self,
            _selector: &ConnectionSelector,
            _client_ids: &[String],
            _headers: &crate::workers::syncer_ws_message_handler::PushRelayHeaders,
            _client_group_id: &str,
        ) {
        }
    }

    struct BlockingPusherFactory {
        handle: tokio::runtime::Handle,
        pusher: Arc<BlockingPusher>,
    }
    impl CGServicesFactory for BlockingPusherFactory {
        fn create_mutagen(&self, _cg: &str) -> Option<Arc<dyn MutagenDispatch>> {
            None
        }
        fn create_pusher(&self, _cg: &str) -> Option<Arc<dyn PusherDispatch>> {
            Some(self.pusher.clone())
        }
        fn create_sync_engine_config(&self, _cg: &str) -> SyncEngineConfig {
            SyncEngineConfig {
                initialization_error: None,
                tables: vec![issue_table_spec()],
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
                enable_query_planner: true,
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
    /// a blocking `PusherDispatch::init_connection`, which the handler's
    /// initConnection arm fires ON the CG task), a SECOND client
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
        let pusher = Arc::new(BlockingPusher {
            entered: entered.clone(),
            release: release.clone(),
            blocked_once: AtomicBool::new(false),
        });
        let factory: Arc<dyn CGServicesFactory> = Arc::new(BlockingPusherFactory {
            handle: rt.handle().clone(),
            pusher,
        });
        let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        });
        let router = Arc::new(crate::workers::syncer::Syncer::new_with_limit(
            factory,
            validator,
            Arc::new(crate::metrics::Metrics::default()),
            10,
        ));

        let make_ctx = |cid: &str, ws: &str, init: bool| {
            let mut params = test_params(cid, ws);
            params.client_group_id = "cgX".to_string();
            if init {
                // clientSchema so the new group's init is ACCEPTED — the
                // blocking seam (`pusher.initConnection`) only fires on an
                // accepted config pass (TS: after the ViewSyncer stream starts).
                params.init_connection_msg = Some(
                    serde_json::from_value(serde_json::json!([
                        "initConnection",
                        {"desiredQueriesPatch": [], "clientSchema": {"tables": {}}}
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
        // `pusher.init_connection` (fired by the handler after the accepted
        // config pass), holding the thread like a long hydrate.
        let (ctx_a, _keep_a, _sink_a) = make_ctx("cA", "wsA", true);
        rt.block_on(router.create_connection(ctx_a));

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
        rt.block_on(router.create_connection(ctx_b));

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
        let router = crate::workers::syncer::Syncer::new_sharded(
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
        let router = crate::workers::syncer::Syncer::new_sharded(
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

    /// L9 Stage 3d regression: a piggybacked `initConnection` is dispatched
    /// through the SAME path as a socket frame (Connection → handler →
    /// ViewSyncerDispatch), and the handler's `connContextManager.initConnection`
    /// dispatch — now the SINGLE recording site — lands the body's
    /// `userQueryURL` in the real CCM. Fails if the piggyback bypasses the
    /// handler or the CCM recording is dropped/duplicated elsewhere.
    #[test]
    fn init_connection_fires_ccm_init_side_effect() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let global = Arc::new(Mutex::new(HashMap::new()));
        let count = Arc::new(AtomicU64::new(0));
        let mut state = ViewSyncerService::new_test(
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
        seed_test_client_schema(&mut state);
        let cell = shared(state);

        let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let sink = DirectWebSocketSink::new(tx);
        let mut params = test_params("c1", "ws1");
        // Piggyback an initConnection with an empty desired-queries patch and a
        // custom-query URL (recorded only via the handler's ccm dispatch).
        params.init_connection_msg = Some(
            serde_json::from_value(serde_json::json!([
                "initConnection",
                {"desiredQueriesPatch": [], "userQueryURL": "https://api.example.com/z"}
            ]))
            .unwrap(),
        );
        let piggyback = rt.block_on(cell.borrow_mut().on_new_connection(params, sink));
        let (client_id, ws_id, text) =
            piggyback.expect("on_new_connection must hand back the piggybacked initConnection");
        rt.block_on(on_inbound(&cell, client_id, ws_id, text));

        let state = cell.borrow();
        let ctx = lock_unpoisoned(&state.ccm)
            .must_get_connection_context(&CcmConnectionSelector {
                client_id: "c1".to_string(),
                ws_id: "ws1".to_string(),
            })
            .expect("the connection context must be registered");
        assert_eq!(
            ctx.query_context.url.as_deref(),
            Some("https://api.example.com/z"),
            "userQueryURL must be recorded through the handler's ccm.initConnection dispatch"
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
        let mut state = ViewSyncerService::new_test(
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
            let mut state = ViewSyncerService::new_test(
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
        let mut state = ViewSyncerService::new_test(
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
        let cell = shared(state);

        let (tx, mut drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let sink = DirectWebSocketSink::new(tx);
        let _ = rt.block_on(
            cell.borrow_mut()
                .on_new_connection(test_params("c1", "ws1"), sink),
        );

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
        rt.block_on(on_inbound(
            &cell,
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
        rt.block_on(on_inbound(
            &cell,
            "c1".into(),
            "ws1".into(),
            r#"["inspect",{"op":"authenticate","id":"2","value":"nope"}]"#.to_string(),
        ));
        assert_eq!(drain(&mut drx).last().unwrap()[1]["value"], false);
        assert!(!cell.borrow().inspector_authenticated);

        // 3) authenticate with the right password → true.
        rt.block_on(on_inbound(
            &cell,
            "c1".into(),
            "ws1".into(),
            r#"["inspect",{"op":"authenticate","id":"3","value":"s3cret"}]"#.to_string(),
        ));
        assert_eq!(drain(&mut drx).last().unwrap()[1]["value"], true);
        assert!(cell.borrow().inspector_authenticated);

        // 4) `version` now returns the configured server version.
        rt.block_on(on_inbound(
            &cell,
            "c1".into(),
            "ws1".into(),
            r#"["inspect",{"op":"version","id":"4"}]"#.to_string(),
        ));
        let last = drain(&mut drx).into_iter().next_back().unwrap();
        assert_eq!(last[1]["op"], "version");
        assert_eq!(last[1]["value"], "9.9.9");
    }

    /// A ViewSyncerService pre-authenticated to the inspector, with a live connection.
    /// Returns the state, runtime, and the sink's receive channel.
    fn inspect_test_state() -> (
        Rc<RefCell<ViewSyncerService>>,
        tokio::runtime::Runtime,
        tokio::sync::mpsc::UnboundedReceiver<WsCommand>,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let mut state = ViewSyncerService::new_test(
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
        let cell = shared(state);
        let (tx, drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let sink = DirectWebSocketSink::new(tx);
        let _ = rt.block_on(
            cell.borrow_mut()
                .on_new_connection(test_params("c1", "ws1"), sink),
        );
        (cell, rt, drx)
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

    // NOTE: the `queries` inspector rows are produced by the SQL port
    // `CVRStore::inspect_queries` (rust-cvr); `inspect_queries_value` enriches
    // each row with the per-query `metrics` (via the InspectorDelegate +
    // `metrics_for_protocol`) and the `getASTForQuery` AST fallback, 1:1 with
    // TS inspect-handler.ts:63-70. The row-shape / TTL-filter / got-flag /
    // rowCount / client-filter coverage lives in rust-cvr tests/inspect_pg_test.rs
    // (PG-gated), against the real desires/queries/rows tables.

    /// The `metrics` op returns the InspectorDelegate's real global aggregate
    /// digests (TS `getMetricsJSON()`), not a hardcoded empty pair. NON-VACUOUS:
    /// a `query-materialization-server` metric seeded into the delegate shows up
    /// as `[1000, 12, 1]` in the frame; reverting the op to the old hardcoded
    /// `[1000]` (or not feeding the delegate) makes the exact-array assert fail.
    #[test]
    fn inspect_metrics_returns_delegate_global_aggregates() {
        use rust_ivm::query::metrics_delegate::Metric;
        let (cell, rt, mut drx) = inspect_test_state();
        // Seed one materialization sample into this CG's delegate.
        cell.borrow().inspector_delegate().borrow_mut().add_metric(
            Metric::QueryMaterializationServer,
            12.0,
            "q1",
        );
        rt.block_on(on_inbound(
            &cell,
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
        // The seeded materialization point flows through to the global digest.
        assert_eq!(
            value["query-materialization-server"],
            serde_json::json!([1000, 12, 1]),
            "seeded materialization metric must appear in the global aggregate"
        );
        // No update samples → the update digest is empty `[1000]`.
        assert_eq!(value["query-update-server"], serde_json::json!([1000]));
    }

    #[test]
    fn inspect_unsupported_and_unknown_ops_answer_with_error_op() {
        let (cell, rt, mut drx) = inspect_test_state();

        // analyze-query IS ported, but a request with no AST must answer with
        // `{op:"error"}` (AST required) — NOT a success frame carrying an
        // `{error}` payload, which would fail the client's
        // `analyzeQueryResultSchema`. (Port of the TS `throw new Error('AST is
        // required...')`, inspect-handler.ts:131, surfaced through the error op.)
        rt.block_on(on_inbound(
            &cell,
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
                .contains("AST is required"),
            "error value must be the AST-required message; got {:?}",
            frame[1]["value"]
        );

        // Unknown op (TS `unreachable` throw → catch) → error op, not silence.
        // Driven through handle_inspect directly: protocol validation upstream
        // (parse_upstream_array, mirroring the TS valita layer) rejects unknown
        // ops before dispatch, so this covers the defensive arm.
        rt.block_on(
            cell.borrow_mut()
                .handle_inspect("c1", &serde_json::json!({"op": "bogus", "id": "b1"})),
        );
        let frame = last_inspect_frame(&mut drx);
        assert_eq!(frame[1]["op"], "error");
        assert_eq!(frame[1]["id"], "b1");
        assert!(frame[1]["value"].is_string());
    }

    // ─── ViewSyncerService mock-factory harness ───────────────────────────────────────
    //
    // Drives ViewSyncerService (the fused port of TS view-syncer.ts + syncer-ws-message-
    // handler.ts dispatch) directly on the test thread, with mock dispatch
    // services (Noop* above), an in-memory replica carrying a real `issue`
    // table spec, and a channel-backed DirectWebSocketSink standing in for the
    // client socket. Models the TS view-syncer.pg.test.ts `connect()` +
    // `nextPoke()` pattern.

    fn issue_table_spec() -> crate::services::view_syncer::pipeline_driver::IvmTableSpec {
        use crate::services::view_syncer::pipeline_driver::{IvmColumnSchema, IvmTableSpec};
        IvmTableSpec {
            table: "issue".to_string(),
            column_order: Vec::new(),
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
                enable_query_planner: true,
                tokio_handle: self.handle.clone(),
                admin_password: None,
                server_version: "test".to_string(),
                metrics: std::sync::Arc::new(crate::metrics::Metrics::default()),
            }
        }
    }

    /// A ViewSyncerService over the `issue`-table factory, plus its runtime.
    fn tables_state(rt: &tokio::runtime::Runtime) -> ViewSyncerService {
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TablesFactory {
            handle: rt.handle().clone(),
        });
        ViewSyncerService::new_test(
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
        cell: &Rc<RefCell<ViewSyncerService>>,
    ) -> tokio::sync::mpsc::UnboundedReceiver<WsCommand> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let _ = rt.block_on(
            cell.borrow_mut()
                .on_new_connection(test_params("c1", "ws1"), DirectWebSocketSink::new(tx)),
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
        let cell = shared(tables_state(&rt));
        let mut rx = connect_c1(&rt, &cell);

        rt.block_on(on_inbound(
            &cell,
            "c1".into(),
            "ws1".into(),
            r#"["ping",{}]"#.to_string(),
        ));
        let frames = drain_sends(&mut rx);
        assert_eq!(frames, vec![serde_json::json!(["pong", {}])]);
        assert!(
            cell.borrow().connections.contains_key("c1"),
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
            let cell = shared(tables_state(&rt));
            let mut rx = connect_c1(&rt, &cell);

            rt.block_on(on_inbound(
                &cell,
                "c1".into(),
                "ws1".into(),
                bad.to_string(),
            ));
            let frames = drain_sends(&mut rx);
            let error = frames
                .iter()
                .find(|f| f[0] == "error")
                .unwrap_or_else(|| panic!("[{bad}] expected an error frame"));
            assert_eq!(error[1]["kind"], "InvalidMessage", "[{bad}]");
            assert!(
                !cell.borrow().connections.contains_key("c1"),
                "[{bad}] the connection must be closed"
            );
            assert!(
                !cell.borrow().registered_ws.contains_key("c1"),
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
        let cell = shared(tables_state(&rt));
        let mut rx = connect_c1(&rt, &cell);

        rt.block_on(on_inbound(
            &cell,
            "c1".into(),
            "ws1".into(),
            INIT_CONNECTION_HASH1.to_string(),
        ));
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
        let state = cell.borrow();
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
        let cell = shared(tables_state(&rt));
        let mut rx = connect_c1(&rt, &cell);
        rt.block_on(on_inbound(
            &cell,
            "c1".into(),
            "ws1".into(),
            INIT_CONNECTION_HASH1.to_string(),
        ));
        let _ = drain_sends(&mut rx);

        rt.block_on(on_inbound(
            &cell,
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

        let state = cell.borrow();
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
        let cell = shared(tables_state(&rt));
        let mut rx = connect_c1(&rt, &cell);
        rt.block_on(on_inbound(
            &cell,
            "c1".into(),
            "ws1".into(),
            INIT_CONNECTION_HASH1.to_string(),
        ));
        let _ = drain_sends(&mut rx);

        rt.block_on(on_inbound(
            &cell,
            "c1".into(),
            "old-wsid".into(),
            r#"["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"put","hash":"query-hash-1234567890","ast":{"table":"issue"}}]}]"#
                .to_string(),
        ));
        assert!(
            drain_sends(&mut rx).is_empty(),
            "a stale-wsID frame must produce no output"
        );
        let state = cell.borrow();
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
        let cell = shared(revalidate_state(&rt, Some(300_000), valid));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let _ = rt.block_on(cell.borrow_mut().on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));
        let _ = drain_sends(&mut rx);

        rt.block_on(on_inbound(
            &cell,
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
        assert!(
            cell.borrow().registered_ws.is_empty(),
            "connection must be closed"
        );
    }

    /// TS `updateAuth` with an empty/absent token is a no-op: no error, no
    /// re-transform, connection stays registered.
    #[test]
    fn update_auth_empty_token_is_a_noop() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let cell = shared(revalidate_state(&rt, Some(300_000), valid));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let _ = rt.block_on(cell.borrow_mut().on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));
        let _ = drain_sends(&mut rx);

        rt.block_on(on_inbound(
            &cell,
            "c1".into(),
            "ws1".into(),
            r#"["updateAuth",{"auth":""}]"#.to_string(),
        ));
        assert!(drain_sends(&mut rx).is_empty(), "no output for empty auth");
        assert_eq!(
            cell.borrow().registered_ws.len(),
            1,
            "connection must survive"
        );
        assert_eq!(cell.borrow().metrics.snapshot()["authChanges"], 0);
    }
}

// ─── Dissolved SyncEngine seat (L9 Stage 3c-iii) ─────────────────────────────
// The former `sync_engine.rs` engine + CVR hot path, merged into
// `ViewSyncerService` per TS: `view-syncer.ts` owns `#pipelines` / `#cvrStore` /
// `#clients` directly. Port of the `CVRState` + `hydrate_and_sync` /
// `advance_and_sync` logic in `rust-ivm/napi/src/lib.rs`, with the napi / TSFN /
// actor-thread machinery stripped. Drives the flow:
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
            row_cache: None,
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
            replica_path: None,
            app_id: String::new(),
            permissions: None,
            permissions_hash: None,
            next_auth_maintenance_at: None,
            background_retransform_failure: None,
            forced_retransform_outcomes: std::collections::VecDeque::new(),
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
            last_row_count: 0,
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
    /// here the router cannot reach the `!Send` store (the engine owns it), so
    /// this forwards the call, fire-and-forget on the shared-pool runtime (TS
    /// `.catch`es and logs — view-syncer.ts:1110-1114). The 1:1 port of
    /// `#updateTTLClockInCVRWithoutLock` itself lives in `router.rs`.
    /// No-op without a store (in-memory / test CGs).
    pub fn update_ttl_clock(&self, ttl_clock: rust_cvr::ttl_clock::TTLClock, last_active: i64) {
        let Some(store_arc) = self.store.clone() else {
            return;
        };
        let fut = async move {
            let store = store_arc.lock().await;
            if let Err(e) = store.update_ttl_clock(ttl_clock, last_active as f64).await {
                tracing::error!("failed to update ttlClock: {e}");
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
    /// The cache is loaded once (lazily) and kept warm; the write-back path
    /// (`flush_ops_to_store`) applies each flushed row delta to it, so this never
    /// re-reads Postgres. Empty when there is no store/cache.
    ///
    /// Returns an `Arc` snapshot (O(1)) — NOT a deep copy. This runs once per
    /// advance/config/TTL pass per client group; the previous full-map clone was
    /// the dominant per-advance allocation at high client counts.
    pub async fn existing_rows(&self) -> Arc<RowRecordMap> {
        let Some(cache) = &self.row_cache else {
            return Arc::new(HashMap::new());
        };
        // Offload the (idempotent) cache load + read onto the shared-pool
        // runtime. `load()` populates the cache on first call and returns early
        // once loaded; the cache stays current via the write-back in
        // `flush_ops_to_store`, so we never `clear()` here.
        let cache = cache.clone();
        self.offload(async move {
            if let Err(e) = cache.load().await {
                tracing::warn!("row cache load failed: {e}");
                return Arc::new(HashMap::new());
            }
            // The cache and updater share one `RowRecord` type, so this is
            // `RowRecordMap` directly — no per-row conversion.
            cache.get_row_records().await
        })
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
        // Offload the load onto the shared-pool runtime (doc 91 §5.1).
        let load_started = std::time::Instant::now();
        let result = self
            .offload(async move {
                let mut store = store_arc.lock().await;
                store.load(last_connect_time).await
            })
            .await;
        crate::metrics::record_cvr_load_attempt(
            result.is_ok(),
            load_started.elapsed().as_secs_f64() * 1000.0,
        );
        let result = result?;
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
        // fail callback + the async-flush metrics recorder (TS
        // `#recordAsyncFlushStats`, wired via the write-behind flush loop).
        let fail: rust_cvr::row_record_cache::FailCallback = Arc::new(|e: String| {
            tracing::warn!("row cache: {e}");
        });
        let metrics: rust_cvr::row_record_cache::MetricsCallback =
            Arc::new(|rows: usize, elapsed_ms: f64| {
                rust_cvr::otel_metrics::record_async_flush_stats(rows as u64, elapsed_ms);
            });
        let cache = RowRecordCache::new(pool, schema, cvr_id, 100, fail, Some(metrics));
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
        let row_deltas: Vec<(RowID, Option<RowRecord>)> = ops
            .iter()
            .filter_map(|op| match op {
                StoreOp::PutRowRecord(r) => Some((r.id.clone(), Some(r.clone()))),
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
                // Bounded retry-with-backoff before declaring the group dead. A
                // failed flush is terminal (fail_group → every client rehomes and
                // REHYDRATES), so under a pool-acquire convoy fail-fast is
                // self-amplifying: timeouts kill groups, rehydrates deepen the
                // convoy. TS has NO acquire timeout and NO shedding — postgres.js
                // simply QUEUES, so transient CVR saturation degrades to latency,
                // not a storm. We approximate that: retry a few times with growing
                // jittered backoff so a saturation spike is ridden out as latency.
                // The flush is one PG transaction (a failed attempt leaves nothing
                // behind, so retries are safe) and deterministic errors (ownership
                // handoff) just re-fail cheaply. Unlike TS's unbounded queue this
                // is BOUNDED, so a genuinely-dead CVR still fails the group
                // promptly rather than wedging. Jitter de-synchronizes the convoy.
                const MAX_FLUSH_ATTEMPTS: u32 = 3;
                let mut attempt = 1u32;
                // Snapshot of the CVR's current row records for the flush's
                // no-op pruning (TS #flush reads this.getRowRecords() from its
                // embedded cache; our store doesn't own the cache, so the
                // snapshot is passed in). O(1) — Arc clone of the cache map.
                let existing_rows = match &cache {
                    Some(c) => c.get_row_records().await,
                    None => Arc::new(HashMap::new()),
                };
                let result = loop {
                    let outcome = {
                        let mut store = store_arc.lock().await;
                        store
                            .flush(&expected, &flushed, last_connect_time as f64, &existing_rows)
                            .await
                    };
                    match outcome {
                        Ok(r) => break Ok(r),
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
                crate::metrics::record_cvr_flush_attempt(result.is_ok());
                result
                    .map_err(|e| {
                        // Counted, not just logged: a rising flush-failure rate
                        // (pool exhaustion, ownership churn) is the leading
                        // indicator of the fail_group → reconnect storm.
                        crate::metrics::record_cvr_flush_failure();
                        format!("store flush: {e}")
                    })?
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
                existing_rows,
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
        crate::trace::note(
            "hydrate-config",
            &format!(
                "cg={} config_update_ms={:.1}",
                self.cg_id,
                cfg_started.elapsed().as_secs_f64() * 1000.0
            ),
        );
        self.sync_query_pipeline_set(
            cfg_cvr,
            poke_ws_ids,
            shard,
            permissions,
            auth_data,
            custom_ctx,
            state_version,
            replica_version,
            existing_rows,
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
        existing_rows: &RowRecordMap,
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
    ) -> Result<CVR, String> {
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
        poke_ws_ids: &[String],
        shard: &ShardID,
        permissions: Option<&serde_json::Value>,
        auth_data: &serde_json::Value,
        custom_ctx: Option<&CustomQueryContext>,
        state_version: String,
        replica_version: String,
        existing_rows: &RowRecordMap,
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
        original_client_versions: std::collections::HashMap<String, NullableCVRVersion>,
    ) -> Result<CVR, String> {
        // TS `#syncQueryPipelineSet` first runs `#hydrateUnchangedQueries`
        // (view-syncer.ts:592/1449) — a PROACTIVE re-hydrate of every
        // already-gotten same-hash query each sync, to drift-check still-alive
        // pipelines. That is ported below as `hydrate_unchanged_queries`, called
        // once `executed` is built. PERF/ART: it re-executes every alive same-hash
        // pipeline on every sync (TS's design) — a serving-path cost that must be
        // confirmed by an ART gate before deploy; it changes no client-observable
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
        // leak (served the full table; caught by ART G8 via the #158 rider).
        let empty_permissions = serde_json::json!({"tables": {}});
        let mut executed: Vec<(String, serde_json::Value, String)> = Vec::new();
        let mut custom_specs: Vec<CustomQuerySpec> = Vec::new();
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
                                        record_transform_error(error, &mut transform_errors)
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

        // Port of TS `#hydrateUnchangedQueries` (view-syncer.ts:592): PROACTIVELY
        // re-hydrate already-gotten same-hash queries and drift-check them against
        // the CVR-stored signature. Non-drifted ones keep their rebuilt pipeline
        // (the loop below then skips them — no bump); drifted ones are removed here
        // and fall through to the loop as `None` (re-added → re-executed via the
        // updater path, with the force-bump reason keyed off this drifted set).
        let drifted_query_ids = self.hydrate_unchanged_queries(&cfg_cvr, &executed, &state_version);

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
                Some(_) => {
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
            let (result, pokers) = self
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
                    &drifted_query_ids,
                )
                .await?;
            // Catch-up rides the SAME poke as the hydrate (TS shape: catchup
            // before pokeEnd). Each poker's live per-client base filter delivers
            // exactly the patches that client hasn't seen; ending the hydrate
            // poke first (the previous shape) advanced every base to the new
            // version and made a separate catch-up poke inert — a reconnecting
            // client silently lost the whole `(oldCookie, current]` interval.
            let clients = self.clients_for(poke_ws_ids);
            let catchup_from =
                Self::catchup_floor(&result.cvr.version, &clients, &original_client_versions);
            let patches = self
                .gather_catchup_patches(&result.cvr, &result.cvr.version, &excluded, catchup_from)
                .await?;
            for p in &patches {
                pokers.add_patch(p);
            }
            pokers.end(result.cvr.version.clone());
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
        let clients = self.clients_for(poke_ws_ids);
        if clients.is_empty() {
            return Ok(());
        }

        // catchupFrom = min(cvr.version, min over connected clients' ORIGINAL
        // cookies). Port of `clients.map(c => c.version()).reduce(min, cvr.version)`
        // — but against the cycle-start snapshot, since each client's live
        // `version()` has already been advanced by the config/hydrate pokes.
        let catchup_from = Self::catchup_floor(&cvr.version, &clients, original_versions);
        let patches = self
            .gather_catchup_patches(cvr, current, exclude_query_hashes, catchup_from)
            .await?;
        if patches.is_empty() {
            return Ok(());
        }

        let client_refs: Vec<&ClientHandler> = clients.iter().map(|c| c.as_ref()).collect();
        let pokers = MultiPoker::new(&client_refs, cvr.version.clone());
        for p in &patches {
            pokers.add_patch(p);
        }
        pokers.end(cvr.version.clone());
        Ok(())
    }

    /// Build the catch-up patch set (row patches first, then config patches —
    /// matching TS ordering) WITHOUT poking. The hydrate path appends these to
    /// its still-open poke; the standalone [`catchup_clients`] wraps them in
    /// their own poke. Returns an empty Vec when there is no store (nothing
    /// persisted to catch up from).
    async fn gather_catchup_patches(
        &mut self,
        cvr: &CVR,
        current: &CVRVersion,
        exclude_query_hashes: &[String],
        catchup_from: NullableCVRVersion,
    ) -> Result<Vec<PatchToVersion>, String> {
        let (Some(store_arc), Some(cache)) = (self.store.clone(), self.row_cache.as_ref()) else {
            return Ok(Vec::new()); // no store → nothing persisted to catch up from
        };

        // Gather the row pages + config patches from PG (async), then release
        // the cache/store borrows before touching the engine (`getRow`).
        let cache_ref = cache;
        let (raw_rows, cfg_patches): (Vec<rust_cvr::schema::cvr::RowsRow>, Vec<PatchToVersion>) = {
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
    /// PERF/ART: this re-executes every alive same-hash pipeline on every sync
    /// (TS's design). It is a serving-path cost that must be confirmed by an ART
    /// gate before deploy; it does not change WHAT a client observes.
    fn hydrate_unchanged_queries(
        &mut self,
        cfg_cvr: &CVR,
        executed: &[(String, serde_json::Value, String)],
        state_version: &str,
    ) -> std::collections::HashSet<String> {
        let mut drifted: std::collections::HashSet<String> = std::collections::HashSet::new();
        // TS view-syncer.ts:1458 — when the CVR is behind the db, hydration must
        // run through the updater path, so skip the proactive re-check.
        if cfg_cvr.version.state_version != state_version {
            return drifted;
        }
        for (qid, transformed_ast, new_hash) in executed {
            let Some(record) = cfg_cvr.queries.get(qid) else {
                continue;
            };
            // Only already-gotten, SAME-transformation-hash queries (TS
            // `gotQueries` + the `transformationHash === q.transformationHash`
            // keep, view-syncer.ts:1465/1538/1561).
            if record.base().transformation_hash.as_deref() != Some(new_hash.as_str()) {
                continue;
            }
            // No-longer-desired: every client state inactivated (TS
            // view-syncer.ts:1474-1482). Internal queries are always desired.
            // NOTE: TS uses `Array.every`, which is VACUOUSLY TRUE for an empty
            // clientState — so a gotten query with no live client is skipped;
            // rust must not add a `!cs.is_empty()` guard (that would diverge).
            if !record.is_internal()
                && let Some(cs) = record.client_state()
                && cs.values().all(|s| s.inactivated_at.is_some())
            {
                continue;
            }
            // Re-hydrate (TS `#pipelines.addQuery(..., 'unchanged-query-rehydrate')`),
            // folding the candidate row-set signature caller-side — rust's
            // streaming `hydrate` does not maintain `engine.row_set_signature`, so
            // the caller folds it exactly as `hydrate_and_sync` does. Rows are
            // discarded: the CVR already holds them; this pass only rebuilds the
            // pipeline and checks drift.
            let mut sig_acc: HashMap<String, u64> = HashMap::new();
            let one = [(qid.clone(), transformed_ast.to_string())];
            if let Err(e) = self
                .pipelines
                .hydrate(&one, |rc| accumulate_signature(&mut sig_acc, rc))
            {
                tracing::warn!("hydrate_unchanged_queries: hydrate {qid} failed: {e}");
                continue;
            }
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
                tracing::warn!(
                    "rowSetSignature drift for query {qid}: prior={stored:x} new={candidate:x}; \
                     removing from pipelines for full re-execution"
                );
                rust_cvr::otel_metrics::record_row_set_signature_drift();
                self.pipelines.remove_query(qid, "remove-query");
                drifted.insert(qid.clone());
            }
        }
        drifted
    }

    /// Hydrate queries AND apply to CVR + push pokes to clients — the whole
    /// hydrate hot path. Port of napi `HydrateAndSyncTask::compute`.
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
        existing_rows: &RowRecordMap,
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
        drifted_query_ids: &std::collections::HashSet<String>,
    ) -> Result<(SyncResult, MultiPoker), String> {
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
        let bump_reason = same_hash_rehydration_bump_reason(
            &cvr,
            add_queries,
            remove_queries,
            &state_version,
            drifted_query_ids,
        );
        let mut updater =
            CVRQueryDrivenUpdater::new(cvr, state_version, replica_version, Some(provider));
        if let Some(reason) = bump_reason {
            crate::metrics::record_same_hash_rehydration_version_bump(reason);
            updater.ensure_new_version();
        }

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
        // A `received` failure (the CVR version-bump invariant) is a recoverable
        // error, not a panic — TS's `#assertNewVersion` throws and aborts the
        // whole pass. Capture the first one, stop feeding the updater, and
        // surface it below so the caller fails the connection and the client
        // re-hydrates from the last consistent CVR (no partial flush).
        let mut cvr_err: Option<String> = None;
        self.pipelines.hydrate(queries, |rc| {
            accumulate_signature(&mut sig_acc, rc);
            if cvr_err.is_some() {
                return;
            }
            if let Some((ct, qid, table, rk, row)) = row_change_to_maps(rc)
                && let Err(e) = processor.on_row_change(ct, &qid, &table, rk, row, existing_rows)
            {
                cvr_err = Some(e);
            }
        })?;
        if let Some(e) = cvr_err {
            return Err(e);
        }
        crate::trace::note(
            "hydrate-fetch",
            &format!(
                "cg={} queries={} rows={} fetch_materialize_ms={:.1}",
                self.cg_id,
                queries.len(),
                processor.total_processed(),
                fetch_started.elapsed().as_secs_f64() * 1000.0
            ),
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
        for (qid, ast_json) in queries {
            if let Some(ms) = self.pipelines.hydration_time_ms(qid) {
                self.inspector_delegate.borrow_mut().add_metric(
                    rust_ivm::query::metrics_delegate::Metric::QueryMaterializationServer,
                    ms,
                    qid,
                );
            }
            if let Ok(ast) = serde_json::from_str::<serde_json::Value>(ast_json) {
                self.inspector_delegate.borrow_mut().add_query(qid, ast);
            }
        }
        processor.finish(existing_rows)?;
        let num_changes = processor.total_processed();
        drop(processor);

        // Hand the folded signatures to the updater's provider so its flush can
        // persist each hydrated query's signature and flag drift.
        *sigs.lock().unwrap() = sig_acc;
        let (flushed_cvr, _stats) = updater.flush(last_connect_time, last_active, ttl_clock);
        // Share the CVR with the offloaded flush via `Arc` (refcount bump, not a
        // deep copy); reclaim it after the awaited flush drops its clone.
        let flushed_arc = Arc::new(flushed_cvr);
        let flush_started = std::time::Instant::now();
        let store_flushed = self
            .flush_to_store(
                &mut updater,
                flushed_arc.clone(),
                last_connect_time,
                existing_rows,
            )
            .await?;
        // Phase profiling (SYNCER_TRACE): CVR-store persist cost, split from the
        // fetch/materialize above so total hydration is fully attributed.
        crate::trace::note(
            "hydrate-flush",
            &format!(
                "cg={} store_flush_ms={:.1} flushed={}",
                self.cg_id,
                flush_started.elapsed().as_secs_f64() * 1000.0,
                store_flushed
            ),
        );
        // No-op store flush → revert to the ORIGINAL CVR (see `flush_to_store`).
        let flushed_cvr = if store_flushed {
            Arc::try_unwrap(flushed_arc).unwrap_or_else(|a| (*a).clone())
        } else {
            updater.base.orig.clone()
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
            },
            pokers,
        ))
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
            RowChangeType,
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
                collected.extend(row_change_to_maps(rc));
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

        // 1:1 cookie formatting — see the twin note at the config-path site.
        let version = version_to_cookie(&flushed_cvr.version);
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
        let (result, pokers) = self
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
                // Removal-only pass: no queries are added, so the same-hash
                // force-bump never fires — no drifted set to thread.
                &std::collections::HashSet::new(),
            )
            .await?;
        // Expiry removals need no catch-up (connected clients are current, and
        // nothing was hydrated) — end the poke directly.
        pokers.end(result.cvr.version.clone());
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
            // Like the config poke (`config_poke_targets`): only clients AT the
            // pre-delete CVR version get this delta poke. A lagging reconnect
            // must keep its old cookie for `catchup_clients` — ending a poke at
            // the new version here would jump it over its catch-up interval.
            let poke_clients =
                Self::config_poke_targets(clients.clone(), &expected_current_version);
            let refs: Vec<&ClientHandler> = poke_clients.iter().map(|c| c.as_ref()).collect();
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
mod engine_tests {
    use super::*;
    // The dissolved engine (L9 Stage 3c-iii): tests keep the old name.
    use super::ViewSyncerService as SyncEngine;
    // Auth-maintenance harness helpers live in the sibling `tests` module.
    use super::tests::{pinned_params, revalidate_state};
    use crate::services::view_syncer::pipeline_driver::{IvmColumnSchema, IvmTableSpec};
    use crate::ws_sink::{DirectWebSocketSink, WsCommand};
    use rust_cvr::cvr::CVR;
    use rust_cvr::row_key::row_id_string;
    use rust_cvr::schema::types::{BaseQueryRecord, ClientQueryRecord, QueryRecord};
    use rust_cvr::schema::types::{CVRVersion, version_from_string};
    use rust_cvr::shards::ShardID;
    use std::collections::BTreeMap;

    /// Non-vacuous port guard for the TS `#addAndRemoveQueries` force-bump +
    /// reason (view-syncer.ts:2182-2214): a same-transformation-hash rehydrate
    /// with no other bump trigger MUST force a `configVersion` bump (the mechanism
    /// preventing the no-bump `#assertNewVersion` wedge), and the reason label is
    /// keyed off `driftedQueryIDs` — `row-set-signature-drift` when the re-added
    /// query drifted, `missing-pipeline` when merely reaped, `mixed` for both.
    /// Reverting the guard to `None` makes the first assertion fail; the negative
    /// branches pin every `trackQueriesWillBumpVersion` term.
    #[test]
    fn same_hash_rehydration_forces_bump_matches_ts_guard() {
        use std::collections::HashSet;
        let insert_q = |cvr: &mut CVR, id: &str, hash: &str| {
            cvr.queries.insert(
                id.to_string(),
                QueryRecord::Client(ClientQueryRecord {
                    base: BaseQueryRecord {
                        id: id.to_string(),
                        transformation_hash: Some(hash.to_string()),
                        transformation_version: None,
                        row_set_signature: None,
                    },
                    ast: serde_json::json!({"table": "users"}),
                    client_state: BTreeMap::new(),
                    patch_version: None,
                }),
            );
        };
        // CVR already has q1 at hash "H", state version "05".
        let mut cvr = empty_cvr("cg1", "01");
        cvr.version = CVRVersion {
            state_version: "05".to_string(),
            config_version: None,
        };
        insert_q(&mut cvr, "q1", "H");
        let no_drift: HashSet<String> = HashSet::new();

        // Same hash, same stateVersion, no removals, no hash change, NOT drifted
        // → force bump with reason `missing-pipeline`.
        assert_eq!(
            same_hash_rehydration_bump_reason(
                &cvr,
                &[("q1".to_string(), "H".to_string())],
                &[],
                "05",
                &no_drift
            ),
            Some("missing-pipeline"),
            "same-hash rehydrate with no other bump trigger MUST force a bump"
        );
        // Same as above but q1 IS in the drifted set → reason `row-set-signature-drift`.
        let drifted: HashSet<String> = ["q1".to_string()].into_iter().collect();
        assert_eq!(
            same_hash_rehydration_bump_reason(
                &cvr,
                &[("q1".to_string(), "H".to_string())],
                &[],
                "05",
                &drifted
            ),
            Some("row-set-signature-drift"),
            "a drifted same-hash query bumps with the drift reason"
        );
        // Two same-hash queries, one drifted one not → reason `mixed`.
        insert_q(&mut cvr, "q2", "H");
        assert_eq!(
            same_hash_rehydration_bump_reason(
                &cvr,
                &[
                    ("q1".to_string(), "H".to_string()),
                    ("q2".to_string(), "H".to_string())
                ],
                &[],
                "05",
                &drifted
            ),
            Some("mixed"),
            "a mix of drifted + reaped same-hash queries → mixed reason"
        );
        // Changed transformation hash → track_queries bumps → no force.
        assert_eq!(
            same_hash_rehydration_bump_reason(
                &cvr,
                &[("q1".to_string(), "H2".to_string())],
                &[],
                "05",
                &no_drift
            ),
            None,
            "a changed transformation hash bumps via track_queries; no force"
        );
        // Advanced stateVersion → track_queries bumps → no force.
        assert_eq!(
            same_hash_rehydration_bump_reason(
                &cvr,
                &[("q1".to_string(), "H".to_string())],
                &[],
                "06",
                &no_drift
            ),
            None,
            "an advanced stateVersion bumps via track_queries; no force"
        );
        // A removal present → track_queries bumps → no force.
        assert_eq!(
            same_hash_rehydration_bump_reason(
                &cvr,
                &[("q1".to_string(), "H".to_string())],
                &["qZ".to_string()],
                "05",
                &no_drift
            ),
            None,
            "a removal bumps via track_queries; no force"
        );
        // No already-gotten same-hash query at all → nothing to force.
        assert_eq!(
            same_hash_rehydration_bump_reason(
                &cvr,
                &[("qX".to_string(), "H".to_string())],
                &[],
                "05",
                &no_drift
            ),
            None,
            "no already-gotten same-hash query → no force"
        );
    }

    /// Port of napi `value_to_serde_json` REAL→JSON semantics (TS
    /// `JSON.stringify` of a JS Number): an integral, in-i64-range REAL
    /// serializes as an INTEGER token (JS `2` not `2.0`), a fractional REAL
    /// keeps its fraction, and the non-finite fallbacks route through
    /// `sqlite_real_to_json`'s sentinel object (JSON has no NaN/Infinity).
    #[test]
    fn real_to_json_matches_js_number_semantics() {
        use rust_ivm::ivm::data::Value as IvmValue;
        // Integral float → integer token, exactly as JS stringifies `2.0`.
        assert_eq!(
            serde_json::to_string(&value_to_serde_json(&IvmValue::F64(2.0))).unwrap(),
            "2"
        );
        assert_eq!(
            serde_json::to_string(&value_to_serde_json(&IvmValue::F64(-0.0))).unwrap(),
            "0"
        );
        // JS max safe integer round-trips as an integer.
        assert_eq!(
            serde_json::to_string(&value_to_serde_json(&IvmValue::F64(9007199254740991.0)))
                .unwrap(),
            "9007199254740991"
        );
        // Fractional stays fractional.
        assert_eq!(
            serde_json::to_string(&value_to_serde_json(&IvmValue::F64(1.5))).unwrap(),
            "1.5"
        );
    }

    /// `sqlite_real_to_json` non-finite fallback: JSON cannot represent
    /// NaN/±Infinity, so the value is wrapped in the `__rustIvmSqliteReal`
    /// sentinel (Rust-only encoding for a value TS could never emit through
    /// JSON.stringify — flagged, not silently nulled).
    #[test]
    fn sqlite_real_to_json_nonfinite_uses_sentinel() {
        assert_eq!(
            sqlite_real_to_json(f64::NAN),
            serde_json::json!({"__rustIvmSqliteReal": "NaN"})
        );
        assert_eq!(
            sqlite_real_to_json(f64::INFINITY),
            serde_json::json!({"__rustIvmSqliteReal": "Infinity"})
        );
        assert_eq!(
            sqlite_real_to_json(f64::NEG_INFINITY),
            serde_json::json!({"__rustIvmSqliteReal": "-Infinity"})
        );
        // A finite value passes through as a plain JSON number.
        assert_eq!(sqlite_real_to_json(2.5), serde_json::json!(2.5));
    }

    /// A censused type must return its live-object counter to baseline once it
    /// drops — otherwise the census leaks and defeats the leak hunt. `SyncEngine`
    /// carries a `live_count::Guard` on `SYNC_ENGINE`; construct one, assert the
    /// counter went up, drop it, assert it came back down.
    #[test]
    fn sync_engine_census_returns_to_baseline_after_drop() {
        use crate::live_count::SYNC_ENGINE;
        use std::sync::atomic::Ordering;
        // The census counter is process-global and the harness runs tests on
        // parallel threads, so a sibling test constructing/dropping its own
        // SyncEngine mid-assertion makes an exact-count check flaky (it aborted
        // a release run). Retry a few times; a real Guard leak fails EVERY
        // attempt (the counter never returns to its snapshot), while transient
        // cross-test interference passes on a quiet retry.
        let mut last: Option<(i64, i64, i64)> = None;
        for _ in 0..8 {
            let base = SYNC_ENGINE.load(Ordering::Relaxed);
            let held = {
                let _engine = SyncEngine::new(IvmPipelines::new());
                SYNC_ENGINE.load(Ordering::Relaxed)
            };
            let after = SYNC_ENGINE.load(Ordering::Relaxed);
            if held == base + 1 && after == base {
                return;
            }
            last = Some((base, held, after));
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        panic!("SyncEngine census never returned to baseline: {last:?}");
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
            column_order: Vec::new(),
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

    /// Non-vacuous port guard for `hydrate_unchanged_queries` (TS
    /// `#hydrateUnchangedQueries`, view-syncer.ts:1449): re-hydrate a gotten,
    /// same-transformation-hash query and compare its freshly-computed row-set
    /// signature to the CVR-stored one — a MISMATCH drifts (record the drift +
    /// remove the pipeline so it re-executes), a MATCH does not. Reverting the
    /// drift branch (never insert into `drifted`) fails the first assertion.
    #[test]
    fn hydrate_unchanged_queries_detects_drift() {
        // Build a fresh engine over an EMPTY users source (so the re-hydrated
        // row-set signature is 0), with q1 gotten at hash "H", the given stored
        // signature, and one LIVE (non-inactivated) client.
        let build = |stored_sig: u64| {
            let mut pipelines = IvmPipelines::new();
            pipelines.init(vec![users_spec()], None, "zero").unwrap();
            let engine = SyncEngine::new(pipelines);
            let mut cvr = make_cvr();
            let q = cvr.queries.get_mut("q1").unwrap();
            q.base_mut().transformation_hash = Some("H".to_string());
            q.base_mut().row_set_signature =
                Some(rust_cvr::row_set_signature::format_signature(stored_sig));
            if let Some(cs) = q.client_state_mut() {
                cs.insert(
                    "client1".to_string(),
                    rust_cvr::schema::types::ClientState {
                        inactivated_at: None,
                        ttl: 1000,
                        version: CVRVersion {
                            state_version: "00".to_string(),
                            config_version: None,
                        },
                    },
                );
            }
            (engine, cvr)
        };
        let executed = vec![(
            "q1".to_string(),
            serde_json::json!({"table": "users"}),
            "H".to_string(),
        )];

        // Drift: stored (999) != candidate (0) → q1 drifts + pipeline removed.
        let (mut engine, cvr) = build(999);
        let drifted = engine.hydrate_unchanged_queries(&cvr, &executed, "00");
        assert!(
            drifted.contains("q1"),
            "a mismatched stored signature must drift"
        );
        assert!(
            engine.pipelines.query_transformation_hash("q1").is_none(),
            "a drifted query's pipeline is removed for full re-execution"
        );

        // No drift: stored (0) == candidate (0) → q1 kept, not drifted.
        let (mut engine, cvr) = build(0);
        let drifted = engine.hydrate_unchanged_queries(&cvr, &executed, "00");
        assert!(
            !drifted.contains("q1"),
            "a matching stored signature must NOT drift"
        );
        assert_eq!(
            engine.pipelines.query_transformation_hash("q1"),
            Some("H"),
            "a non-drifted query keeps its rebuilt pipeline"
        );
    }

    #[tokio::test]
    async fn hydrate_and_sync_emits_poke_frames() {
        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();

        let mut engine = SyncEngine::new(pipelines);

        // Wire a client whose sink drains into a channel (buffer large enough
        // that blocking_send never blocks for the few poke frames).
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
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
        let (result, pokers) = engine
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
                &std::collections::HashSet::new(),
            )
            .await
            .unwrap();

        // Store is None → no flush; the got-query patch still produces a poke.
        assert!(result.reset_reason.is_none());
        assert!(
            !result.query_patches.is_empty(),
            "expected a got-query patch"
        );

        // Reconnect-catch-up regression: the poke must still be OPEN after
        // `hydrate_and_sync` returns, so catch-up patches appended here ride the
        // SAME poke and are delivered. (Previously `end()` ran inside, advancing
        // every client's base to the new version — a separate catch-up poke then
        // NOOP-dropped every patch a reconnecting client missed while away.)
        let mut del_key = serde_json::Map::new();
        del_key.insert("id".to_string(), serde_json::json!("stale-row"));
        pokers.add_patch(&PatchToVersion {
            patch: Patch::Row(RowPatch::Del {
                id: RowID {
                    schema: String::new(),
                    table: "users".to_string(),
                    row_key: del_key,
                },
            }),
            to_version: result.cvr.version.clone(),
        });
        pokers.end(result.cvr.version.clone());

        let mut frames = Vec::new();
        while let Ok(cmd) = rx.try_recv() {
            if let WsCommand::Send { msg: v, .. } = cmd {
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
        // Exactly ONE poke (no nested pokeStart), and the late catch-up del is in it.
        let starts = frames.iter().filter(|f| f[0] == "pokeStart").count();
        assert_eq!(starts, 1, "hydrate + catch-up must share one poke");
        let has_del = frames.iter().any(|f| {
            f[0] == "pokePart"
                && f[1]["rowsPatch"]
                    .as_array()
                    .is_some_and(|ps| ps.iter().any(|p| p["op"] == "del"))
        });
        assert!(has_del, "late catch-up patch must be delivered in the poke");
    }

    /// D-c: `hydrate_and_sync` records the per-query inspector server metrics —
    /// `query-materialization-server` (from the engine's hydration timing) and
    /// the queryID→AST map (`add_query`), both keyed by the queryID, exactly as
    /// TS `#syncQueryPipelineSet` (view-syncer.ts:2297-2298). NON-VACUOUS: after
    /// hydrating q1, the delegate reports the AST and a `query-hydration-server-ms`
    /// for q1; removing the recording loop makes both `get_ast_for_query` and
    /// `get_metrics_json_for_query` return `None`, failing the asserts.
    #[tokio::test]
    async fn hydrate_and_sync_records_inspector_materialization_and_ast() {
        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
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
        let ast = r#"{"table":"users"}"#;
        engine
            .hydrate_and_sync(
                make_cvr(),
                "00".to_string(),
                "v1".to_string(),
                &[("q1".to_string(), "hash1".to_string())],
                &[],
                &["ws1".to_string()],
                &[("q1".to_string(), ast.to_string())],
                &existing_rows,
                0,
                0,
                0,
                &std::collections::HashSet::new(),
            )
            .await
            .unwrap();

        // The AST is stored for the `queries` op fallback (`getASTForQuery`).
        assert_eq!(
            engine.inspector_delegate().borrow().get_ast_for_query("q1"),
            Some(&serde_json::json!({"table": "users"})),
            "hydrate must record the query AST in the inspector delegate"
        );
        // A per-query materialization metric was recorded (hydration ms present).
        let per_query = engine
            .inspector_delegate()
            .borrow_mut()
            .get_metrics_json_for_query("q1")
            .expect("q1 must have per-query metrics after hydrate");
        assert!(
            per_query.get("query-hydration-server-ms").is_some(),
            "the hydration ms must be recorded for q1; got {per_query}"
        );
        // The global materialization aggregate carries exactly one sample.
        let global = engine.inspector_delegate().borrow_mut().get_metrics_json();
        let mat = global["query-materialization-server"].as_array().unwrap();
        // `[compression, mean, weight]` — one centroid of weight 1.
        assert_eq!(mat.len(), 3, "one materialization sample: {mat:?}");
        assert_eq!(mat[2], serde_json::json!(1), "weight 1");
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

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
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

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
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
        while let Ok(WsCommand::Send { msg: v, .. }) = rx.try_recv() {
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

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
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

    #[tokio::test]
    async fn clear_op_drops_all_desired_queries() {
        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
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

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
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

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
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
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
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
        use rust_cvr::schema::types::version_from_string;

        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
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
        use rust_cvr::schema::types::version_from_string;

        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };
        let mk = || -> Arc<dyn WebSocketSink> {
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
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
        use rust_cvr::schema::types::version_from_string;

        let mut pipelines = IvmPipelines::new();
        pipelines.init(vec![users_spec()], None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };
        let mk = || -> Arc<dyn WebSocketSink> {
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
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

        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        engine.register_client(
            "client1",
            "ws1",
            "cg1",
            &shard,
            None,
            Arc::new(DirectWebSocketSink::new(tx1)),
        );
        let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
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
        while let Ok(WsCommand::Send { msg: v, .. }) = rx1.try_recv() {
            if v[0] == "deleteClients"
                && let Some(ids) = v[1]["clientIDs"].as_array()
                && ids.iter().any(|x| x == "client2")
            {
                saw_ack = true;
            }
        }
        assert!(saw_ack, "expected deleteClients ack naming client2");
    }

    /// NON-VACUOUS (parity fix 2026-09-01): a failed custom-query transform must
    /// emit the TS WARN AND still forward the error to clients. Port of TS
    /// `#processTransformedCustomQueries` (view-syncer.ts:1715-1719,
    /// `lc.warn?.(errorMessage, q)`). Before the fix rust forwarded to clients
    /// but logged nothing — silent in ops. Revert the `tracing::warn!` in
    /// `record_transform_error` and the emission assertion fails; break the
    /// message format and the `format_transform_error_message` asserts fail.
    #[test]
    fn record_transform_error_emits_ts_warn_and_forwards() {
        use std::sync::{Arc, Mutex};

        #[derive(Clone)]
        struct BufWriter(Arc<Mutex<Vec<u8>>>);
        struct BufGuard(Arc<Mutex<Vec<u8>>>);
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
            type Writer = BufGuard;
            fn make_writer(&'a self) -> BufGuard {
                BufGuard(self.0.clone())
            }
        }
        impl std::io::Write for BufGuard {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        // Exact shape the API server returns for an InputValidationError, as seen
        // in prod TS logs: {id, name, error:"app", details:{...}}.
        let err = serde_json::json!({
            "id": "392e943e2358d54f",
            "name": "ticketsByIds",
            "error": "app",
            "details": {"type": "InputValidationError"}
        });

        // Pure-format parity (pins the exact TS wording, view-syncer.ts:1716):
        assert_eq!(
            format_transform_error_message(&err),
            "Error transforming custom query ticketsByIds: app {\"type\":\"InputValidationError\"}"
        );
        // details absent → no trailing segment (TS `q.details ? ... : ''`).
        assert_eq!(
            format_transform_error_message(&serde_json::json!({"name": "q2", "error": "http"})),
            "Error transforming custom query q2: http"
        );

        // Emission + forwarding parity:
        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(BufWriter(buf.clone()))
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .finish();
        let mut forwarded: Vec<serde_json::Value> = Vec::new();
        tracing::subscriber::with_default(subscriber, || {
            record_transform_error(err.clone(), &mut forwarded);
        });

        let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            logged.contains(
                "Error transforming custom query ticketsByIds: app {\"type\":\"InputValidationError\"}"
            ),
            "expected TS-parity transform-error WARN; got: {logged}"
        );
        assert!(logged.contains("WARN"), "must be WARN level; got: {logged}");
        // Client forwarding is preserved (the pre-fix behavior).
        assert_eq!(forwarded, vec![err]);
    }

    /// Capture WARN-level logs emitted while running `f`. Used to pin the exact
    /// TS-parity auth-maintenance warnings.
    fn capture_warns<F: FnOnce()>(f: F) -> String {
        use std::sync::{Arc, Mutex};
        #[derive(Clone)]
        struct BufWriter(Arc<Mutex<Vec<u8>>>);
        struct BufGuard(Arc<Mutex<Vec<u8>>>);
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
            type Writer = BufGuard;
            fn make_writer(&'a self) -> BufGuard {
                BufGuard(self.0.clone())
            }
        }
        impl std::io::Write for BufGuard {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(BufWriter(buf.clone()))
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        String::from_utf8(buf.lock().unwrap().clone()).unwrap()
    }

    /// Pure classifier parity (TS `#runBackgroundRetransform` catch dispatch,
    /// view-syncer.ts:2700-2723): an auth error body → `AuthError`, a transient
    /// transform-failed body → `TransformFailed`, and `None` (no throw) →
    /// `Success`. NON-VACUOUS: mis-map any arm (e.g. treat every failure as
    /// transient) and the corresponding assert fails.
    #[test]
    fn classify_retransform_failure_splits_auth_transient_success() {
        assert!(matches!(
            classify_retransform_failure(None),
            RetransformOutcome::Success
        ));
        // {kind: Unauthorized} and http 401/403 are auth (auth.ts isAuthErrorBody).
        assert!(matches!(
            classify_retransform_failure(Some(serde_json::json!({"kind": "Unauthorized"}))),
            RetransformOutcome::AuthError(_)
        ));
        assert!(matches!(
            classify_retransform_failure(Some(serde_json::json!({
                "kind": "TransformFailed", "reason": "http", "status": 401
            }))),
            RetransformOutcome::AuthError(_)
        ));
        // A 5xx / non-auth transform failure is transient → deferred, not fatal.
        assert!(matches!(
            classify_retransform_failure(Some(serde_json::json!({
                "kind": "TransformFailed", "reason": "http", "status": 503
            }))),
            RetransformOutcome::TransformFailed(_)
        ));
        assert!(matches!(
            classify_retransform_failure(Some(serde_json::json!({
                "kind": "TransformFailed", "reason": "internal", "message": "boom"
            }))),
            RetransformOutcome::TransformFailed(_)
        ));
    }

    /// NON-VACUOUS (fix #2, 2026-09-01): a background retransform whose re-hydrate
    /// hits an AUTH error must NOT mark success — it must WARN, fail the stale
    /// connection, and retry under a replacement (TS `#runBackgroundRetransform`
    /// view-syncer.ts:2700-2709,2726-2745). The pre-fix code ran the re-hydrate
    /// and marked success UNCONDITIONALLY (the 2026-08-27 stale-auth outage
    /// class): no warn, no fail, no retry. Revert `run_background_retransform` to
    /// that unconditional mark and every assert below fails.
    #[test]
    fn background_retransform_auth_error_fails_connection_and_retries() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        // Two validated connections in the group, both pinned to user-1, so a
        // failed background connection has a replacement to retry under.
        let (tx1, _d1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx1),
        ));
        let (tx2, _d2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            pinned_params("c2", "ws2", "user-1"),
            DirectWebSocketSink::new(tx2),
        ));
        assert_eq!(state.registered_ws.len(), 2);

        // First attempt: auth error. Second (on the replacement): success.
        state
            .forced_retransform_outcomes
            .push_back(RetransformOutcome::AuthError(
                serde_json::json!({"kind": "Unauthorized"}),
            ));
        state
            .forced_retransform_outcomes
            .push_back(RetransformOutcome::Success);

        let logged = capture_warns(|| rt.block_on(state.run_background_retransform()));

        assert!(
            logged.contains(
                "Background retransform auth failed; failing connection and searching for replacement"
            ),
            "expected the TS auth-fail WARN; got: {logged}"
        );
        assert_eq!(
            state.registered_ws.len(),
            1,
            "the auth-failed background connection must be dropped, its replacement kept"
        );
        assert!(
            state.forced_retransform_outcomes.is_empty(),
            "both attempts must run — the retry under the replacement connection"
        );
    }

    /// NON-VACUOUS (fix #2): a background retransform whose re-hydrate hits a
    /// TRANSIENT transform failure must WARN + defer maintenance and KEEP the
    /// connection — never mark success, never close the socket (TS
    /// `#runBackgroundRetransform` view-syncer.ts:2710-2719). Revert to the
    /// unconditional mark and the WARN assert fails (the old path was silent and
    /// marked success).
    #[test]
    fn background_retransform_transform_failed_defers_and_keeps_connection() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        let (tx, _d) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));
        assert_eq!(state.registered_ws.len(), 1);

        state
            .forced_retransform_outcomes
            .push_back(RetransformOutcome::TransformFailed(
                serde_json::json!({"kind": "TransformFailed", "reason": "http", "status": 503}),
            ));

        let logged = capture_warns(|| rt.block_on(state.run_background_retransform()));

        assert!(
            logged.contains("Background retransform failed; deferring auth maintenance"),
            "expected the TS transform-failed defer WARN; got: {logged}"
        );
        assert_eq!(
            state.registered_ws.len(),
            1,
            "a transient transform failure must NOT close the connection"
        );
        assert!(
            state.forced_retransform_outcomes.is_empty(),
            "the single attempt must run"
        );
    }

    /// A successful background retransform takes the success branch silently:
    /// no maintenance WARN, connection retained. (The `markBackgroundRetransform
    /// Success` call itself has no observable deadline effect here because the
    /// test CCM is built with no retransform interval; the auth/transform-failed
    /// tests above carry the non-vacuous weight of the fix.)
    #[test]
    fn background_retransform_success_is_silent_and_keeps_connection() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut state = revalidate_state(&rt, Some(300_000), valid);

        let (tx, _d) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));

        state
            .forced_retransform_outcomes
            .push_back(RetransformOutcome::Success);

        let logged = capture_warns(|| rt.block_on(state.run_background_retransform()));

        assert!(
            !logged.contains("Background retransform"),
            "success path must emit no retransform WARN; got: {logged}"
        );
        assert_eq!(state.registered_ws.len(), 1);
        assert!(state.forced_retransform_outcomes.is_empty());
    }
}
