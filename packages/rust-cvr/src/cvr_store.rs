//! Port of `packages/zero-cache/src/services/view-syncer/cvr-store.ts`.
//!
//! The CVRStore is the only component that writes to Postgres. It buffers
//! writes in a pending queue and flushes them atomically in a single
//! transaction.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;

use crate::client_handler::{Patch, PatchToVersion};
use crate::cvr::*;
use crate::row_record_cache::{ExecuteResult, FlushMode, RowRecordCache};
use crate::schema::cvr::{ClientsRow, DesiresRow, InstancesRow, QueriesRow};
use crate::schema::types::*;
use crate::schema::types::{
    CVRVersion, NullableCVRVersion, VersionError, cmp_cvr, maybe_version_string, version_string,
};
use crate::ttl::{DEFAULT_TTL_MS, TTL, clamp_ttl};
use crate::ttl_clock::TTLClock;
use std::cmp::Ordering;

// The time to wait between load() attempts when the rows table is behind the
// CVR instance version. Port of TS `LOAD_ATTEMPT_INTERVAL_MS`.
const LOAD_ATTEMPT_INTERVAL_MS: u64 = 500;
// The maximum number of load() attempts if the rowsVersion is behind (~5s of
// catchup wait before the CVR is considered invalid). Port of TS
// `MAX_LOAD_ATTEMPTS`.
const MAX_LOAD_ATTEMPTS: u32 = 10;

// ─── Error types ───────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum CVRStoreError {
    #[error(
        "CVR ownership was transferred to {owner} at {granted_at} (last connect: {last_connect_time})"
    )]
    OwnershipError {
        owner: String,
        granted_at: f64,
        last_connect_time: f64,
    },
    #[error("CVR has been concurrently modified. Expected {expected}, got {actual}")]
    ConcurrentModification { expected: String, actual: String },
    #[error("Client not found: {0}")]
    ClientNotFound(String),
    #[error("Rows version behind: cvr={cvr_version}, rows={rows_version:?}")]
    RowsVersionBehind {
        cvr_version: String,
        rows_version: Option<String>,
    },
    #[error("Invalid client schema: {0}")]
    InvalidClientSchema(String),
    #[error("Invalid version string in CVR data: {0}")]
    VersionParse(#[from] crate::schema::types::VersionError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

type InstanceLoadRow = (
    String,
    f64,
    f64,
    Option<String>,
    Option<Value>,
    Option<String>,
    Option<String>,
    Option<f64>,
    bool,
    Option<String>,
);
type QueryLoadRow = (
    String,
    Option<Value>,
    Option<String>,
    Option<Value>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<bool>,
    Option<bool>,
    Option<String>,
);
// `deleted` is `Option<bool>`: the DDL column is nullable and TS reads NULL as
// falsy (schema/cvr.ts:164 `deleted: boolean | null`) — F-CVR-SCHEMA-1.
type DesireLoadRow = (
    String,
    String,
    String,
    Option<bool>,
    Option<f64>,
    Option<f64>,
);

// ─── Flush stats ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CVRFlushStats {
    pub instances: usize,
    pub queries: usize,
    pub desires: usize,
    pub clients: usize,
    pub rows: usize,
    pub rows_deferred: usize,
    pub statements: usize,
    /// TS `rowsFlushed` (cvr-store.ts:1207): whether the row-record statements
    /// actually ran in this transaction, or were deferred to the
    /// `RowRecordCache` write-back. The caller uses it as the `flushed`
    /// argument to `RowRecordCache::apply`.
    pub rows_flushed: bool,
}

// ─── Load result ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct LoadResult {
    pub cvr: CVR,
    pub is_new: bool,
}

// ─── Pending writes ────────────────────────────────────────────────────────

#[derive(Default, Clone)]
pub struct PartialQueriesRow {
    /// Outer `None` means "leave unchanged"; inner `None` means SQL NULL.
    /// TS distinguishes `undefined` from `null` for all nullable partial fields.
    pub patch_version: Option<Option<String>>,
    pub deleted: Option<bool>,
    pub transformation_hash: Option<Option<String>>,
    pub transformation_version: Option<Option<String>>,
    pub row_set_signature: Option<String>,
}

#[derive(Default)]
pub struct PendingWrites {
    pub pending_instance_write: Option<InstancesRow>,
    pub pending_clients_insert: Vec<ClientsRow>,
    pub pending_clients_delete: Vec<String>,
    pub pending_query_updates: BTreeMap<String, QueriesRow>,
    pub pending_query_partial_updates: BTreeMap<String, PartialQueriesRow>,
    pub pending_desire_updates: BTreeMap<String, DesiresRow>,
    // Keyed by rowIDString. The `RowID` is kept alongside the (optional) record
    // so a delete (`None`, or a `Some` tombstone with `refCounts = None`) can be
    // turned into a `DELETE ... WHERE rowKey = ...` — the row_key is otherwise
    // unrecoverable from the string key.
    pub pending_row_record_updates: HashMap<String, (RowID, Option<RowRecord>)>,
    pub force_updates: HashSet<String>,
}

impl PendingWrites {
    fn is_empty(&self) -> bool {
        self.pending_instance_write.is_none()
            && self.pending_clients_insert.is_empty()
            && self.pending_clients_delete.is_empty()
            && self.pending_query_updates.is_empty()
            && self.pending_query_partial_updates.is_empty()
            && self.pending_desire_updates.is_empty()
            && self.pending_row_record_updates.is_empty()
    }
}

// ─── CVRStoreHandle ────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct CVRStoreCatchupReader {
    pool: PgPool,
    schema: String,
    cvr_id: String,
}

impl CVRStoreCatchupReader {
    pub async fn catchup_config_patches(
        &self,
        after_version: NullableCVRVersion,
        up_to_version: &CVRVersion,
        current: &CVRVersion,
    ) -> Result<Vec<PatchToVersion>, CVRStoreError> {
        // Catch-up reads are independent of the handle's buffered writes. A
        // read-only clone lets callers release their std::sync::Mutex before
        // awaiting PostgreSQL, preventing one slow query from blocking every
        // other operation on this client group.
        CVRStoreHandle::new(
            self.pool.clone(),
            self.schema.clone(),
            self.cvr_id.clone(),
            String::new(),
        )
        .catchup_config_patches(after_version, up_to_version, current)
        .await
    }
}

/// One row of `CVRStore.inspectQueries` — the per-(client, query) inspector view.
/// Mirrors TS `InspectQueryRow` (zero-protocol `inspect-down.ts`) minus `metrics`,
/// which the inspect handler enriches from the (unported) server InspectorDelegate.
/// Field order matches the TS protocol object for byte-identical JSON.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InspectQueryRow {
    #[serde(rename = "clientID")]
    pub client_id: String,
    #[serde(rename = "queryID")]
    pub query_id: String,
    pub ast: Option<Value>,
    pub name: Option<String>,
    pub args: Option<Value>,
    pub got: bool,
    pub deleted: bool,
    pub ttl: i64,
    #[serde(rename = "inactivatedAt")]
    pub inactivated_at: Option<i64>,
    #[serde(rename = "rowCount")]
    pub row_count: i64,
}

/// sqlx decode form for the `inspect_queries` SELECT (camelCase column aliases).
#[derive(sqlx::FromRow)]
struct InspectQueryRowDb {
    #[sqlx(rename = "clientID")]
    client_id: String,
    #[sqlx(rename = "queryID")]
    query_id: String,
    ttl: i64,
    #[sqlx(rename = "inactivatedAt")]
    inactivated_at: Option<i64>,
    #[sqlx(rename = "rowCount")]
    row_count: i64,
    ast: Option<Value>,
    got: bool,
    deleted: bool,
    name: Option<String>,
    args: Option<Value>,
}

impl From<InspectQueryRowDb> for InspectQueryRow {
    fn from(d: InspectQueryRowDb) -> Self {
        InspectQueryRow {
            client_id: d.client_id,
            query_id: d.query_id,
            ast: d.ast,
            name: d.name,
            args: d.args,
            got: d.got,
            deleted: d.deleted,
            ttl: d.ttl,
            inactivated_at: d.inactivated_at,
            row_count: d.row_count,
        }
    }
}

pub struct CVRStoreHandle {
    pool: PgPool,
    schema: String,
    cvr_id: String,
    task_id: String,
    pending: PendingWrites,
    row_count: usize,
    /// Port of TS `CVRStore.#rowCache` (cvr-store.ts:246): the store OWNS the
    /// row-record cache and is the only thing that talks to it — `getRowRecords`
    /// for the flush's no-op pruning, `executeRowUpdates` for the write-or-defer
    /// decision, `apply` to land the result. Previously the syncer owned it and
    /// passed both the snapshot and the cache into `flush()`.
    row_cache: RowRecordCache,
    /// Live-instance census guard (leak hunting). Inc on `new`, dec on Drop.
    _census: crate::live_count::Guard,
}

impl Drop for CVRStoreHandle {
    fn drop(&mut self) {
        // Dropping the single PG writer with buffered writes still queued means
        // those material changes never reach Postgres — a leak-suspect teardown
        // (the version won't advance and the client can't catch up). Name the
        // drop path via a gated backtrace so the field can identify who bypassed
        // the flush. `RUST_CVR_DROP_BACKTRACE=1` to enable; prod pays nothing.
        if !self.pending.is_empty() {
            eprintln!(
                "[cvr] CVRStoreHandle dropped with pending writes (cvr_id={}) \
                 — buffered changes were NOT flushed [census {}]",
                self.cvr_id,
                crate::live_count::snapshot(),
            );
            crate::live_count::drop_backtrace("CVRStoreHandle(pending)");
        }
    }
}

impl CVRStoreHandle {
    /// Port of TS `new CVRStore(...)` (cvr-store.ts:229). TS's constructor also
    /// takes `failService`, `loadAttemptIntervalMs`, `maxLoadAttempts`,
    /// `deferredRowFlushThreshold` and `setTimeoutFn`, all with DEFAULTS. Rust
    /// has no default arguments, so those defaults are applied here — the same
    /// values TS uses (`DEFAULT_DEFERRED_THRESHOLD` = 100,
    /// `LOAD_ATTEMPT_INTERVAL_MS`, `MAX_LOAD_ATTEMPTS`) and the same fail
    /// behavior the one rust caller supplied (log the error). Add explicit
    /// parameters only when a caller actually needs to override one.
    pub fn new(pool: PgPool, schema: String, cvr_id: String, task_id: String) -> Self {
        let fail: crate::row_record_cache::FailCallback = Arc::new(|e: String| {
            eprintln!("[cvr] row cache: {e}");
        });
        // TS `#recordAsyncFlushStats` — the write-back flush loop's OTel stats.
        let metrics: crate::row_record_cache::MetricsCallback =
            Arc::new(|rows: usize, elapsed_ms: f64| {
                crate::otel_metrics::record_async_flush_stats(rows as u64, elapsed_ms);
            });
        let row_cache = RowRecordCache::new(
            pool.clone(),
            schema.clone(),
            cvr_id.clone(),
            crate::row_record_cache::DEFAULT_DEFERRED_THRESHOLD,
            fail,
            Some(metrics),
        );
        Self {
            pool,
            schema,
            cvr_id,
            task_id,
            pending: PendingWrites::default(),
            row_count: 0,
            row_cache,
            _census: crate::live_count::Guard::new(&crate::live_count::CVR_STORE),
        }
    }

    /// Port of TS `CVRStore.getRowRecords()` (cvr-store.ts:520-521): the row
    /// records the client already has, read from the owned cache (loaded lazily,
    /// then kept current by `flush`'s `apply`). `Arc` snapshot — O(1), not a deep
    /// copy. TS is a bare delegate to `#rowCache.getRowRecords()` and lets a load
    /// failure reject; so does this.
    pub async fn get_row_records(&self) -> Result<Arc<HashMap<String, RowRecord>>, CVRStoreError> {
        Ok(self.row_cache.get_row_records().await?)
    }

    /// Test-only: seed the owned row cache, standing in for the `cvr.rows` a TS
    /// test would INSERT before calling `flush`.
    #[cfg(test)]
    pub(crate) async fn seed_row_cache_for_test(&self, rows: HashMap<String, RowRecord>) {
        self.row_cache.seed_for_test(rows).await;
    }

    /// Port of TS `CVRStore.flushed(lc)` (cvr-store.ts): resolves once the
    /// write-back has landed every deferred row.
    pub async fn flushed(&self) -> Result<(), String> {
        self.row_cache.flushed().await
    }

    /// Port of TS `CVRStore.catchupRowPatches` (cvr-store.ts:709), which
    /// delegates straight to `#rowCache.catchupRowPatches`.
    pub async fn catchup_row_patches(
        &self,
        after_version: NullableCVRVersion,
        up_to_version: &CVRVersion,
        current: &CVRVersion,
        exclude_query_hashes: &[String],
    ) -> Result<crate::row_record_cache::CatchupCursor, sqlx::Error> {
        self.row_cache
            .catchup_row_patches(after_version, up_to_version, current, exclude_query_hashes)
            .await
    }

    pub fn has_pending_writes(&self) -> bool {
        !self.pending.is_empty()
    }

    pub fn catchup_reader(&self) -> CVRStoreCatchupReader {
        CVRStoreCatchupReader {
            pool: self.pool.clone(),
            schema: self.schema.clone(),
            cvr_id: self.cvr_id.clone(),
        }
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Port of TS `CVRStore.inspectQueries` (cvr-store.ts). Returns the inspector
    /// view of every desired query for this client group (optionally filtered to
    /// one `client_id`), read from committed Postgres state via a plain SELECT —
    /// the same SQL as TS: `desires LEFT JOIN queries`, a per-query `rowCount`
    /// over the `rows` table (`refCounts ? queryHash`), and a TTL-expiry filter.
    /// `metrics` is NOT included here — the inspect handler adds it from the
    /// server InspectorDelegate (unported), matching the TS layering.
    pub async fn inspect_queries(
        &self,
        ttl_clock: TTLClock,
        client_id: Option<&str>,
    ) -> Result<Vec<InspectQueryRow>, sqlx::Error> {
        let sql = format!(
            r#"
            SELECT DISTINCT ON (d."clientID", d."queryHash")
                d."clientID"                                    AS "clientID",
                d."queryHash"                                   AS "queryID",
                (COALESCE(d."ttlMs", {default_ttl}))::bigint    AS "ttl",
                d."inactivatedAtMs"::bigint                     AS "inactivatedAt",
                (SELECT COUNT(*)::bigint FROM "{schema}".rows r
                   WHERE r."clientGroupID" = d."clientGroupID"
                     AND jsonb_exists(r."refCounts", d."queryHash")) AS "rowCount",
                q."clientAST"                                   AS "ast",
                (q."patchVersion" IS NOT NULL)                  AS "got",
                COALESCE(d."deleted", FALSE)                    AS "deleted",
                q."queryName"                                   AS "name",
                q."queryArgs"                                   AS "args"
            FROM "{schema}".desires d
            LEFT JOIN "{schema}".queries q
                ON q."clientGroupID" = d."clientGroupID"
               AND q."queryHash" = d."queryHash"
            WHERE d."clientGroupID" = $1
              AND ($2::text IS NULL OR d."clientID" = $2)
              AND NOT (
                  d."inactivatedAtMs" IS NOT NULL
                  AND d."ttlMs" IS NOT NULL
                  AND (d."inactivatedAtMs" + d."ttlMs") <= $3
              )
            ORDER BY d."clientID", d."queryHash""#,
            default_ttl = DEFAULT_TTL_MS,
            schema = self.schema,
        );
        let rows = sqlx::query_as::<_, InspectQueryRowDb>(&sql)
            .bind(&self.cvr_id)
            .bind(client_id)
            .bind(ttl_clock as f64)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(InspectQueryRow::from).collect())
    }

    /// Port of TS `CVRStore.updateTTLClock` (cvr-store.ts:555-561): persist the
    /// `ttlClock` + `lastActive` of the CVR instance outside any flush. The
    /// view-syncer calls this every `TTL_CLOCK_INTERVAL` (60s) so the on-disk
    /// clock does not go stale between flushes — a stale clock reloaded after a
    /// restart/rehome defers inactive-query TTL expiry, and a stale
    /// `lastActive` skews CVR-purge GC.
    pub async fn update_ttl_clock(
        &self,
        ttl_clock: TTLClock,
        last_active: f64,
    ) -> Result<(), CVRStoreError> {
        sqlx::query(&format!(
            r#"UPDATE "{}".instances
               SET "lastActive" = to_timestamp($1 / 1000.0),
                   "ttlClock" = $2
               WHERE "clientGroupID" = $3"#,
            self.schema
        ))
        .bind(last_active)
        .bind(ttl_clock as f64)
        .bind(&self.cvr_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Port of TS `CVRStore.getTTLClock` (cvr-store.ts:568-583): the current
    /// on-disk `ttlClock` of the CVR instance, or `None` when the CVR has never
    /// been initialized for this client group.
    pub async fn get_ttl_clock(&self) -> Result<Option<TTLClock>, CVRStoreError> {
        let row: Option<(f64,)> = sqlx::query_as(&format!(
            r#"SELECT "ttlClock" FROM "{}".instances
               WHERE "clientGroupID" = $1"#,
            self.schema
        ))
        .bind(&self.cvr_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(t,)| t as TTLClock))
    }

    // ─── Buffered write methods ──────────────────────────────────────

    pub fn put_instance(&mut self, cvr: &CVR) {
        self.pending.pending_instance_write = Some(InstancesRow {
            client_group_id: cvr.id.clone(),
            version: version_string(&cvr.version),
            last_active: cvr.last_active as f64,
            ttl_clock: cvr.ttl_clock as f64,
            replica_version: cvr.replica_version.clone(),
            owner: None,
            granted_at: None,
            client_schema: cvr.client_schema.clone(),
            profile_id: cvr.profile_id.clone(),
        });
    }

    pub fn insert_client(&mut self, client: &ClientRecord) {
        self.pending.pending_clients_insert.push(ClientsRow {
            client_group_id: self.cvr_id.clone(),
            client_id: client.id.clone(),
        });
    }

    pub fn delete_client(&mut self, client_id: &str) {
        self.pending
            .pending_clients_delete
            .push(client_id.to_string());
    }

    pub fn put_query(&mut self, query: &QueryRecord) {
        let row = query_record_to_query_row(&self.cvr_id, query);
        self.pending
            .pending_query_updates
            .insert(query.id().to_string(), row);
    }

    pub fn update_query(&mut self, query: &QueryRecord) {
        // TS updateQuery writes all mutable query-state fields. The nested
        // Options preserve its undefined-vs-null distinction: these fields are
        // explicitly cleared for internal/untransformed queries rather than
        // accidentally left at their previous values.
        let existing = self
            .pending
            .pending_query_partial_updates
            .entry(query.id().to_string())
            .or_default();
        existing.patch_version = Some(if query.is_internal() {
            None
        } else {
            query.patch_version().map(version_string)
        });
        existing.transformation_hash = Some(query.base().transformation_hash.clone());
        existing.transformation_version = Some(
            query
                .base()
                .transformation_version
                .as_ref()
                .map(version_string),
        );
        existing.deleted = Some(false);
    }

    pub fn mark_query_as_deleted(&mut self, version: &CVRVersion, query_patch: &QueryPatch) {
        let id = match query_patch {
            QueryPatch::Del { id, .. } => id,
            QueryPatch::Put { id, .. } => id,
        };
        let existing = self
            .pending
            .pending_query_partial_updates
            .entry(id.clone())
            .or_default();
        existing.patch_version = Some(Some(version_string(version)));
        existing.deleted = Some(true);
        existing.transformation_hash = Some(None);
        existing.transformation_version = Some(None);
    }

    pub fn update_row_set_signature(&mut self, query_hash: &str, signature: &str) {
        let existing = self
            .pending
            .pending_query_partial_updates
            .entry(query_hash.to_string())
            .or_default();
        existing.row_set_signature = Some(signature.to_string());
    }

    pub fn put_desired_query(
        &mut self,
        version: &CVRVersion,
        query_id: &str,
        client_id: &str,
        deleted: bool,
        inactivated_at: Option<TTLClock>,
        ttl: i64,
    ) {
        let key = format!("{}:{}", client_id, query_id);
        self.pending.pending_desire_updates.insert(
            key,
            DesiresRow {
                client_group_id: self.cvr_id.clone(),
                client_id: client_id.to_string(),
                query_hash: query_id.to_string(),
                patch_version: version_string(version),
                // Writers always set an explicit boolean (as TS does); only
                // DB reads can observe NULL.
                deleted: Some(deleted),
                // TS `convertTTLValues`: `ttlMs = ttl < 0 ? null : ttl` — a
                // negative TTL (the "forever" sentinel) is persisted as SQL NULL,
                // not a negative number. `clamp_ttl` maps -1 → MAX_TTL_MS so this
                // is unreachable via the normal callers today, but match TS's
                // function contract defensively. See BEHAVIORAL-SWEEP-FINDINGS.md.
                ttl: if ttl < 0 { None } else { Some(ttl as f64) },
                inactivated_at: inactivated_at.map(|t| t as f64),
            },
        );
    }

    pub fn put_row_record(&mut self, row: &RowRecord) {
        let id_str = crate::row_key::row_id_string(&row.id);
        self.pending
            .pending_row_record_updates
            .insert(id_str, (row.id.clone(), Some(row.clone())));
    }

    pub fn del_row_record(&mut self, id: &RowID) {
        let id_str = crate::row_key::row_id_string(id);
        self.pending
            .pending_row_record_updates
            .insert(id_str, (id.clone(), None));
    }

    pub fn force_updates(&mut self, ids: &[RowID]) {
        for id in ids {
            self.pending
                .force_updates
                .insert(crate::row_key::row_id_string(id));
        }
    }

    /// Apply a batch of StoreOps directly to this store's pending writes buffer.
    /// This is the internal Rust-to-Rust path — no napi boundary crossing.
    pub fn apply_store_ops(&mut self, ops: Vec<StoreOp>) {
        for op in ops {
            match op {
                StoreOp::InsertClient(c) => self.insert_client(&c),
                StoreOp::PutQuery(q) => self.put_query(&q),
                StoreOp::PutDesiredQuery {
                    version,
                    query_id,
                    client_id,
                    deleted,
                    inactivated_at,
                    ttl,
                } => self.put_desired_query(
                    &version,
                    &query_id,
                    &client_id,
                    deleted,
                    inactivated_at,
                    ttl,
                ),
                StoreOp::PutInstance(cvr) => self.put_instance(&cvr),
                StoreOp::DeleteClient(id) => self.delete_client(&id),
                StoreOp::UpdateQuery(q) => self.update_query(&q),
                StoreOp::MarkQueryAsDeleted { version, patch } => {
                    self.mark_query_as_deleted(&version, &patch)
                }
                StoreOp::PutRowRecord(r) => self.put_row_record(&r),
                StoreOp::DelRowRecord(id) => self.del_row_record(&id),
                StoreOp::UpdateRowSetSignature { query_id, hex } => {
                    self.update_row_set_signature(&query_id, &hex)
                }
            }
        }
    }

    // ─── Flush ───────────────────────────────────────────────────────

    /// Port of the public TS `flush` wrapper (cvr-store.ts:1234-1268): run
    /// `#flush` and count the attempt on `cvr.flush_attempts`, labeled
    /// result=success with flush.type sync|noop, or result=error with
    /// error.kind — F-RRC-4.
    pub async fn flush(
        &mut self,
        expected_current_version: &CVRVersion,
        cvr: &CVR,
        last_connect_time: f64,
    ) -> Result<Option<CVRFlushStats>, CVRStoreError> {
        let result = self
            .flush_internal(expected_current_version, cvr, last_connect_time)
            .await;
        match &result {
            Ok(stats) => crate::otel_metrics::record_flush_attempt(
                "success",
                if stats.is_some() { "sync" } else { "noop" },
                None,
            ),
            Err(e) => {
                crate::otel_metrics::record_flush_attempt("error", "sync", Some(cvr_error_kind(e)))
            }
        }
        result
    }

    /// Port of TS `#flush` (cvr-store.ts:1051-1130).
    async fn flush_internal(
        &mut self,
        expected_current_version: &CVRVersion,
        cvr: &CVR,
        last_connect_time: f64,
    ) -> Result<Option<CVRFlushStats>, CVRStoreError> {
        // Port of TS `#flush` no-op pruning (cvr-store.ts:1066-1086): before
        // deciding materiality, drop pending row records that would not change
        // the CVR — (a) a delete / unreferenced tombstone for a row that is not
        // in the CVR, and (b) a record deep-equal to what is already stored.
        // Without this, redundant re-receives are re-written and tombstones for
        // never-present rows are upserted, producing extra DB rows and spurious
        // catchup row-del patches TS never emits.
        //
        // `getRowRecords()` is read INSIDE this branch, exactly as TS does
        // (`if (this.#pendingRowRecordUpdates.size) { ... await this.getRowRecords() }`,
        // cvr-store.ts:1066-1067). Hoisting it above the branch — as an earlier
        // revision did — makes every CONFIG-ONLY flush pay a cache read (and the
        // first one a full per-CG `cvr.rows` scan) while the store mutex is held,
        // work TS never performs.
        let row_records_before_prune = self.pending.pending_row_record_updates.len();
        if !self.pending.pending_row_record_updates.is_empty() {
            let existing_rows = self.get_row_records().await?;
            // TS `this.#rowCount = existingRowRecords.size` (cvr-store.ts:1068).
            self.row_count = existing_rows.len();
            let PendingWrites {
                pending_row_record_updates,
                force_updates,
                ..
            } = &mut self.pending;
            pending_row_record_updates.retain(|id, (_, row)| {
                if force_updates.contains(id) {
                    return true;
                }
                let existing = existing_rows.get(id);
                // TS: `existing === undefined && !row?.refCounts`
                let unreferenced_and_absent =
                    existing.is_none() && row.as_ref().is_none_or(|r| r.ref_counts.is_none());
                // TS: `deepEqual(row ?? undefined, existing)` — RowRecord's
                // derived Eq over BTreeMap refCounts is canonical, so `==` is
                // deepEqual.
                let unchanged = match (row.as_ref(), existing) {
                    (None, None) => true,
                    (Some(r), Some(e)) => r == e,
                    _ => false,
                };
                !(unreferenced_and_absent || unchanged)
            });
        }
        // Materiality check (port of TS `#flush`): the CVR instance row is
        // only advanced when there are material changes buffered — clients,
        // queries, desires, rows, or a pre-queued instance write from
        // `set_client_schema`/`set_profile_id`. Queuing the derivable instance
        // write *before* this check (as the old code did) made the guard dead, so
        // every no-op flush advanced `instances.version` (and, since the store is
        // the single writer, `rowsVersion` too) — defeating the
        // "instance-updated-only-on-material-change" invariant and adding a PG
        // round-trip per no-op cycle. `is_empty()` here correctly excludes the
        // not-yet-queued derivable instance write but includes any pre-queued one.
        if self.pending.is_empty() {
            // Composition of the pending set at a NO-OP flush. A no-op reverts
            // the CVR to `orig`, discarding whatever version this pass produced
            // — harmless on its own, but if a patch was also poked at the
            // discarded version the client is CLOSED with
            // `Patches were sent but finalVersion ...` (rust 434 vs TS 0 on
            // identical traffic). `pruned` is the giveaway: row records that
            // were queued and then dropped as unchanged mean rows were
            // "received" without changing anything, which is the case TS's
            // design says cannot coexist with an emitted patch.
            // Ungated: the gated version logged only 6 of 248 failing passes,
            // so the MAJORITY shape (no rows queued at all, yet a patch was
            // still poked at the discarded version) was invisible.
            {
                tracing::info!(
                    cvr_id = %self.cvr_id,
                    "no-op CVR flush discarding version {}: rows_queued={} rows_pruned={} \
                     (clients_ins={} clients_del={} queries={} partial={} desires={} instance={})",
                    crate::schema::types::version_string(&cvr.version),
                    row_records_before_prune,
                    row_records_before_prune - self.pending.pending_row_record_updates.len(),
                    self.pending.pending_clients_insert.len(),
                    self.pending.pending_clients_delete.len(),
                    self.pending.pending_query_updates.len(),
                    self.pending.pending_query_partial_updates.len(),
                    self.pending.pending_desire_updates.len(),
                    self.pending.pending_instance_write.is_some(),
                );
            }
            return Ok(None);
        }
        // There ARE material changes — now record the instance write and proceed.
        // Time the flush for `zero.sync.cvr.flush-time` (TS `recordSyncFlushStats`).
        // A no-op flush returns above without recording, matching TS.
        let flush_started = std::time::Instant::now();
        self.put_instance(cvr);

        // Take the buffered writes out of `self` so they are consumed by this
        // flush attempt WHETHER OR NOT it succeeds. An errored flush (ownership
        // lost, concurrent modification, PG failure) rolls the tx back — but the
        // ops must not linger to be replayed by a LATER flush against a reloaded
        // CVR: they're tagged with the old version, and replaying them is exactly
        // the cross-owner corruption the version guard exists to prevent. TS gets
        // this for free by discarding the whole CVRStore with the failed service.
        let pending = std::mem::take(&mut self.pending);

        if crate::tracer::enabled() {
            crate::tracer::note(
                "CVRStore",
                &format!(
                    "flush start cvr_id={} rows={} clients={} queries={} desires={}",
                    self.cvr_id,
                    pending.pending_row_record_updates.len(),
                    pending.pending_clients_insert.len() + pending.pending_clients_delete.len(),
                    pending.pending_query_updates.len()
                        + pending.pending_query_partial_updates.len(),
                    pending.pending_desire_updates.len(),
                ),
            );
        }

        let mut stats = CVRFlushStats::default();
        let mut tx = self.pool.begin().await?;
        // Port of TS `runTx` (run-transaction.ts:37-56): every zero-cache
        // transaction disables the session `statement_timeout` (providers set
        // one at the database level) and bounds an orphaned transaction with
        // `idle_in_transaction_session_timeout`. TS fires both without
        // awaiting (pipelined); rust awaits them in order on the same
        // connection — the same statements reach the server before the guard.

        // TS runTx fires both `SET LOCAL`s without awaiting (run-transaction.ts:
        // 47-55) — pipelined; rust awaits each (two round trips).
        sqlx::query("SET LOCAL statement_timeout = 0")
            .execute(&mut *tx)
            .await?;
        sqlx::query(&format!(
            "SET LOCAL idle_in_transaction_session_timeout = {}",
            crate::row_record_cache::IDLE_TX_TIMEOUT_MS
        ))
        .execute(&mut *tx)
        .await?;

        // ── Version + ownership guard (port of TS `#checkVersionAndOwnership`) ──
        // Lock the instance row and refuse to write if (a) another task now owns
        // this CVR (was granted it AFTER our last connect) or (b) the on-disk
        // version has moved since we last saw it. Returning Err here drops `tx`
        // uncommitted → the transaction rolls back, so no partial write escapes.
        // Without this, two syncers owning the same client group would clobber
        // each other's CVR (lost updates / cross-owner corruption).
        {
            let row: Option<(String, Option<String>, Option<f64>)> = sqlx::query_as(&format!(
                r#"SELECT "version", "owner",
                          (extract(epoch from "grantedAt") * 1000.0)::double precision
                   FROM "{}".instances
                   WHERE "clientGroupID" = $1
                   FOR UPDATE"#,
                self.schema
            ))
            .bind(&self.cvr_id)
            .fetch_optional(&mut *tx)
            .await?;

            let (db_version, owner, granted_at) = match row {
                Some((v, o, g)) => (v, o, g),
                // No instance row yet → a brand-new CVR (empty version, no owner).
                None => (
                    crate::schema::types::EMPTY_CVR_VERSION
                        .state_version
                        .to_string(),
                    None,
                    None,
                ),
            };
            if owner.as_deref() != Some(self.task_id.as_str())
                && granted_at.unwrap_or(0.0) > last_connect_time
            {
                return Err(CVRStoreError::OwnershipError {
                    owner: owner.unwrap_or_default(),
                    granted_at: granted_at.unwrap_or(0.0),
                    last_connect_time,
                });
            }
            let expected_str = version_string(expected_current_version);
            if db_version != expected_str {
                return Err(CVRStoreError::ConcurrentModification {
                    expected: expected_str,
                    actual: db_version,
                });
            }
        }

        // 1. Instance upsert with ownership check
        if let Some(instance) = &pending.pending_instance_write {
            let sql = format!(
                // owner + grantedAt re-assert this task's ownership at its connect
                // time (NOT `NOW()`), matching TS `putInstance` which writes
                // `owner=taskID, grantedAt=lastConnectTime` and updates BOTH
                // unconditionally on conflict. The `#checkVersionAndOwnership`
                // guard above has already ensured we're allowed to take it.
                r#"INSERT INTO "{}".instances
                   ("clientGroupID", "version", "lastActive", "ttlClock",
                    "replicaVersion", "owner", "grantedAt", "clientSchema", "profileID")
                   VALUES ($1, $2, to_timestamp($3 / 1000.0), $4, $5, $6,
                           to_timestamp($9 / 1000.0), $7, $8)
                   ON CONFLICT ("clientGroupID") DO UPDATE SET
                    "version" = excluded."version",
                    "lastActive" = excluded."lastActive",
                    "ttlClock" = excluded."ttlClock",
                    "replicaVersion" = excluded."replicaVersion",
                    "owner" = excluded."owner",
                    "grantedAt" = excluded."grantedAt",
                    "clientSchema" = COALESCE("{}".instances."clientSchema", excluded."clientSchema"),
                    "profileID" = COALESCE("{}".instances."profileID", excluded."profileID")
                "#,
                self.schema, self.schema, self.schema
            );
            sqlx::query(&sql)
                .bind(&instance.client_group_id)
                .bind(&instance.version)
                .bind(instance.last_active)
                .bind(instance.ttl_clock)
                .bind(&instance.replica_version)
                .bind(&self.task_id)
                .bind(&instance.client_schema)
                .bind(&instance.profile_id)
                .bind(last_connect_time)
                .execute(&mut *tx)
                .await?;
            stats.instances = 1;
        }

        // 2. Clients inserts — batched into ONE `json_to_recordset` statement (was
        // one awaited INSERT per client). Identical semantics: insert each
        // (clientGroupID, clientID), ON CONFLICT DO NOTHING. Rust-equivalent of
        // TS's pipelined per-client inserts (same rows, one round-trip).
        if !pending.pending_clients_insert.is_empty() {
            let rows_json = Value::Array(
                pending
                    .pending_clients_insert
                    .iter()
                    .map(|c| {
                        serde_json::json!({
                            "clientGroupID": c.client_group_id,
                            "clientID": c.client_id,
                        })
                    })
                    .collect(),
            );
            let sql = format!(
                r#"INSERT INTO "{}".clients ("clientGroupID", "clientID")
                   SELECT "clientGroupID", "clientID"
                   FROM json_to_recordset($1::json) AS x(
                     "clientGroupID" TEXT,
                     "clientID" TEXT
                   )
                   ON CONFLICT ("clientGroupID", "clientID") DO NOTHING"#,
                self.schema
            );
            sqlx::query(&sql).bind(&rows_json).execute(&mut *tx).await?;
            stats.clients = pending.pending_clients_insert.len();
        }

        // 3. Clients deletes — batched into ONE statement via `= ANY`.
        if !pending.pending_clients_delete.is_empty() {
            let sql = format!(
                r#"DELETE FROM "{}".clients
                   WHERE "clientGroupID" = $1 AND "clientID" = ANY($2)"#,
                self.schema
            );
            sqlx::query(&sql)
                .bind(&self.cvr_id)
                .bind(&pending.pending_clients_delete)
                .execute(&mut *tx)
                .await?;
        }

        // 4. Query upserts (full) — batched into ONE `json_to_recordset` statement
        // (was one awaited upsert per query). Mirrors TS `#flushQueries` full batch.
        // `clientAST` (JSONB) and `queryArgs` (JSON) are read directly as their JSON
        // types because rust stores them as parsed `Value`s — unlike TS's
        // pre-stringified `TEXT::json` — for the identical end state.
        if !pending.pending_query_updates.is_empty() {
            let rows_json = Value::Array(
                pending
                    .pending_query_updates
                    .values()
                    .map(|row| {
                        serde_json::json!({
                            "clientGroupID": row.client_group_id,
                            "queryHash": row.query_hash,
                            "clientAST": row.client_ast,
                            "queryName": row.query_name,
                            "queryArgs": row.query_args,
                            "patchVersion": row.patch_version,
                            "transformationHash": row.transformation_hash,
                            "transformationVersion": row.transformation_version,
                            "internal": row.internal,
                            "deleted": row.deleted,
                            "rowSetSignature": row.row_set_signature,
                        })
                    })
                    .collect(),
            );
            let sql = format!(
                r#"INSERT INTO "{}".queries
                   ("clientGroupID", "queryHash", "clientAST", "queryName", "queryArgs",
                    "patchVersion", "transformationHash", "transformationVersion",
                    "internal", "deleted", "rowSetSignature")
                   SELECT "clientGroupID", "queryHash", "clientAST", "queryName", "queryArgs",
                          "patchVersion", "transformationHash", "transformationVersion",
                          "internal", "deleted", "rowSetSignature"
                   FROM json_to_recordset($1::json) AS x(
                     "clientGroupID" TEXT,
                     "queryHash" TEXT,
                     "clientAST" JSONB,
                     "queryName" TEXT,
                     "queryArgs" JSON,
                     "patchVersion" TEXT,
                     "transformationHash" TEXT,
                     "transformationVersion" TEXT,
                     "internal" BOOLEAN,
                     "deleted" BOOLEAN,
                     "rowSetSignature" TEXT
                   )
                   ON CONFLICT ("clientGroupID", "queryHash") DO UPDATE SET
                    "clientAST" = excluded."clientAST",
                    "queryName" = excluded."queryName",
                    "queryArgs" = excluded."queryArgs",
                    "patchVersion" = excluded."patchVersion",
                    "transformationHash" = excluded."transformationHash",
                    "transformationVersion" = excluded."transformationVersion",
                    "internal" = excluded."internal",
                    "deleted" = excluded."deleted",
                    "rowSetSignature" = excluded."rowSetSignature""#,
                self.schema
            );
            sqlx::query(&sql).bind(&rows_json).execute(&mut *tx).await?;
            stats.queries += pending.pending_query_updates.len();
        }

        // 5. Query partial updates — batched into ONE `UPDATE ... FROM
        // json_to_recordset` with per-column `WHEN "<col>Set"` CASE guards (mirrors
        // TS `#flushQueries` partial batch). Preserves the outer/inner `Option`
        // semantics: a column is written iff it was set (outer `Some`); a
        // set-to-NULL (inner `None`) is honored via the flattened value.
        if !pending.pending_query_partial_updates.is_empty() {
            let rows_json = Value::Array(
                pending
                    .pending_query_partial_updates
                    .iter()
                    .map(|(hash, p)| {
                        serde_json::json!({
                            "clientGroupID": self.cvr_id,
                            "queryHash": hash,
                            "patchVersionSet": p.patch_version.is_some(),
                            "patchVersion": p.patch_version.clone().flatten(),
                            "deletedSet": p.deleted.is_some(),
                            "deleted": p.deleted,
                            "transformationHashSet": p.transformation_hash.is_some(),
                            "transformationHash": p.transformation_hash.clone().flatten(),
                            "transformationVersionSet": p.transformation_version.is_some(),
                            "transformationVersion": p.transformation_version.clone().flatten(),
                            "rowSetSignatureSet": p.row_set_signature.is_some(),
                            "rowSetSignature": p.row_set_signature,
                        })
                    })
                    .collect(),
            );
            let sql = format!(
                r#"UPDATE "{}".queries AS q SET
                    "patchVersion" = CASE WHEN u."patchVersionSet" THEN u."patchVersion" ELSE q."patchVersion" END,
                    "deleted" = CASE WHEN u."deletedSet" THEN u."deleted" ELSE q."deleted" END,
                    "transformationHash" = CASE WHEN u."transformationHashSet" THEN u."transformationHash" ELSE q."transformationHash" END,
                    "transformationVersion" = CASE WHEN u."transformationVersionSet" THEN u."transformationVersion" ELSE q."transformationVersion" END,
                    "rowSetSignature" = CASE WHEN u."rowSetSignatureSet" THEN u."rowSetSignature" ELSE q."rowSetSignature" END
                   FROM json_to_recordset($1::json) AS u(
                     "clientGroupID" TEXT,
                     "queryHash" TEXT,
                     "patchVersionSet" BOOLEAN,
                     "patchVersion" TEXT,
                     "deletedSet" BOOLEAN,
                     "deleted" BOOLEAN,
                     "transformationHashSet" BOOLEAN,
                     "transformationHash" TEXT,
                     "transformationVersionSet" BOOLEAN,
                     "transformationVersion" TEXT,
                     "rowSetSignatureSet" BOOLEAN,
                     "rowSetSignature" TEXT
                   )
                   WHERE q."clientGroupID" = u."clientGroupID"
                     AND q."queryHash" = u."queryHash""#,
                self.schema
            );
            sqlx::query(&sql).bind(&rows_json).execute(&mut *tx).await?;
            stats.queries += pending.pending_query_partial_updates.len();
        }

        // 6. Desire upserts.
        //
        // Dual-write the DEPRECATED "ttl" (INTERVAL) / "inactivatedAt"
        // (TIMESTAMPTZ) columns alongside the current *Ms columns — TS
        // `#flushDesires` does the same (cvr-store.ts convertTTLValues). Without
        // it, during a rolling upgrade an OLD-image reader that still reads the
        // deprecated columns sees NULL and mis-clamps TTL / re-activates an
        // inactivated query. Derived arithmetically from the bound ms params
        // (safer/more precise than TS's json-number→INTERVAL cast): ttl seconds
        // = ttlMs/1000, timestamp = to_timestamp(inactivatedAtMs/1000). A NULL
        // or negative ttlMs → NULL interval (TS `ttl < 0 ? null`).
        // Batched into ONE `json_to_recordset` statement (was one awaited upsert per
        // desire). The per-row CASE derivations are unchanged — rust's deliberate
        // ms→INTERVAL / to_timestamp derivation (kept over TS's json-number cast) —
        // now applied set-wise over the batch instead of one PG round-trip per row.
        if !pending.pending_desire_updates.is_empty() {
            let rows_json = Value::Array(
                pending
                    .pending_desire_updates
                    .values()
                    .map(|row| {
                        serde_json::json!({
                            "clientGroupID": row.client_group_id,
                            "clientID": row.client_id,
                            "queryHash": row.query_hash,
                            "patchVersion": row.patch_version,
                            "deleted": row.deleted,
                            "ttlMs": row.ttl,
                            "inactivatedAtMs": row.inactivated_at,
                        })
                    })
                    .collect(),
            );
            let sql = format!(
                r#"INSERT INTO "{}".desires
                   ("clientGroupID", "clientID", "queryHash", "patchVersion",
                    "deleted", "ttl", "ttlMs", "inactivatedAt", "inactivatedAtMs")
                   SELECT "clientGroupID", "clientID", "queryHash", "patchVersion", "deleted",
                    CASE WHEN "ttlMs" IS NULL OR "ttlMs" < 0 THEN NULL
                         ELSE ("ttlMs" / 1000.0) * INTERVAL '1 second' END,
                    "ttlMs",
                    CASE WHEN "inactivatedAtMs" IS NULL THEN NULL
                         ELSE to_timestamp("inactivatedAtMs" / 1000.0) END,
                    "inactivatedAtMs"
                   FROM json_to_recordset($1::json) AS x(
                     "clientGroupID" TEXT,
                     "clientID" TEXT,
                     "queryHash" TEXT,
                     "patchVersion" TEXT,
                     "deleted" BOOLEAN,
                     "ttlMs" DOUBLE PRECISION,
                     "inactivatedAtMs" DOUBLE PRECISION
                   )
                   ON CONFLICT ("clientGroupID", "clientID", "queryHash") DO UPDATE SET
                    "patchVersion" = excluded."patchVersion",
                    "deleted" = excluded."deleted",
                    "ttl" = excluded."ttl",
                    "ttlMs" = excluded."ttlMs",
                    "inactivatedAt" = excluded."inactivatedAt",
                    "inactivatedAtMs" = excluded."inactivatedAtMs""#,
                self.schema
            );
            sqlx::query(&sql).bind(&rows_json).execute(&mut *tx).await?;
            stats.desires += pending.pending_desire_updates.len();
        }

        // 7. Row record upserts and deletes — TS cvr-store.ts:1166
        // `this.#rowCache.executeRowUpdates(tx, cvr.version, updates,
        // 'allow-defer')`.
        //
        // The decision to WRITE or DEFER belongs to the `RowRecordCache` (a
        // flush already in flight, or a batch over `DEFAULT_DEFERRED_THRESHOLD`
        // ⇒ defer), which this store OWNS exactly like TS's CVRStore does.
        // When it defers, NOTHING row-related is written here: not `cvr.rows`,
        // not `cvr.rowsVersion`. `instances.version` therefore runs ahead of
        // `rowsVersion.version` until the cache's background flush catches up,
        // which is exactly TS's design (`load()` retries
        // `MAX_LOAD_ATTEMPTS` × `LOAD_ATTEMPT_INTERVAL_MS` on
        // `RowsVersionBehind`). Deferring is what keeps the heavyweight row
        // commit off the poke's critical path — writing it inline put ~1900
        // upserts in front of every large hydrate's `pokeEnd`.
        let row_updates: Vec<(RowID, Option<RowRecord>)> = pending
            .pending_row_record_updates
            .values()
            .map(|(id, r)| (id.clone(), r.clone()))
            .collect();
        let row_plan =
            self.row_cache
                .execute_row_updates(&cvr.version, &row_updates, FlushMode::AllowDefer);
        let statements = match row_plan {
            ExecuteResult::Defer => {
                stats.rows_deferred = row_updates.len();
                None
            }
            ExecuteResult::Execute(stmts) => Some(stmts),
        };
        stats.rows_flushed = statements.is_some();

        // The `rows` table has a FOREIGN KEY to `rowsVersion(clientGroupID)`, so
        // a `rowsVersion` row must exist first. It is upserted on every
        // NON-deferred flush (not only when rows change), so a config-only
        // advance does not leave `rowsVersion` behind and make every subsequent
        // `load` falsely detect a rows-behind CVR. That matches TS, whose
        // `executeRowUpdates` also emits the `rowsVersion` upsert as its first
        // statement regardless of how many row updates there are.
        if statements.is_some() {
            let rv_sql = format!(
                r#"INSERT INTO "{}"."rowsVersion" ("clientGroupID", "version")
                   VALUES ($1, $2)
                   ON CONFLICT ("clientGroupID") DO UPDATE SET "version" = excluded."version""#,
                self.schema
            );
            sqlx::query(&rv_sql)
                .bind(&self.cvr_id)
                .bind(version_string(&cvr.version))
                .execute(&mut *tx)
                .await?;
        }
        // Execute the plan the RowRecordCache handed back. `deletes` are the
        // literal-`None` records; everything else — INCLUDING refCounts-NULL
        // tombstones — is upserted, so a tombstone stays in `rows` carrying the
        // deletion's patchVersion, which is what catchup reads to emit row DELs
        // to reconnecting clients.
        //
        // Both batches collapse to ONE `json_to_recordset` statement. TS emits
        // one DELETE per row but PIPELINES them through postgres.js, so the
        // round-trip count matches; semantics are identical (rows matched by
        // (clientGroupID, schema, table, rowKey) with JSONB rowKey equality).
        // Sequential awaits here were the flush-convoy driver behind the
        // capacity cliff.
        if let Some(stmts) = &statements {
            if !stmts.deletes.is_empty() {
                let n = stmts.deletes.len();
                let del_json = Value::Array(
                    stmts
                        .deletes
                        .iter()
                        .map(|d| {
                            let mut obj = serde_json::Map::new();
                            obj.insert("schema".into(), Value::String(d.schema.clone()));
                            obj.insert("table".into(), Value::String(d.table.clone()));
                            obj.insert("rowKey".into(), d.row_key.clone());
                            Value::Object(obj)
                        })
                        .collect(),
                );
                let sql = format!(
                    r#"DELETE FROM "{}".rows AS r
                   USING json_to_recordset($1::json) AS d(
                     "schema" TEXT,
                     "table" TEXT,
                     "rowKey" JSONB
                   )
                   WHERE r."clientGroupID" = $2
                     AND r."schema" = d."schema"
                     AND r."table" = d."table"
                     AND r."rowKey" = d."rowKey""#,
                    self.schema
                );
                sqlx::query(&sql)
                    .bind(&del_json)
                    .bind(&self.cvr_id)
                    .execute(&mut *tx)
                    .await?;
                stats.rows += n;
            }
            if !stmts.inserts.is_empty() {
                let rows_json = Value::Array(
                    stmts
                        .inserts
                        .iter()
                        .map(|r| serde_json::to_value(r).unwrap_or(Value::Null))
                        .collect(),
                );
                let sql = format!(
                    r#"INSERT INTO "{}".rows
                   ("clientGroupID", "schema", "table", "rowKey",
                    "rowVersion", "patchVersion", "refCounts")
                   SELECT "clientGroupID", "schema", "table", "rowKey",
                          "rowVersion", "patchVersion", "refCounts"
                   FROM json_to_recordset($1::json) AS x(
                     "clientGroupID" TEXT,
                     "schema" TEXT,
                     "table" TEXT,
                     "rowKey" JSONB,
                     "rowVersion" TEXT,
                     "patchVersion" TEXT,
                     "refCounts" JSONB
                   )
                   ON CONFLICT ("clientGroupID", "schema", "table", "rowKey")
                   DO UPDATE SET
                    "rowVersion" = excluded."rowVersion",
                    "patchVersion" = excluded."patchVersion",
                    "refCounts" = excluded."refCounts""#,
                    self.schema
                );
                sqlx::query(&sql).bind(&rows_json).execute(&mut *tx).await?;
                stats.rows += stmts.inserts.len();
            }
        }

        tx.commit().await?;

        // Port of TS cvr-store.ts:1218 `this.#rowCount = await
        // this.#rowCache.apply(updates, cvr.version, rowsFlushed)`. With
        // `rows_flushed` the transaction above already persisted the rows, so
        // this is cache-only; otherwise the cache queues them in `pending` and
        // spawns the background flush, and the caller's poke goes out without
        // waiting for the row commit.
        // TS calls this UNCONDITIONALLY (cvr-store.ts:1217), including on a
        // config-only flush with an empty row map: `apply` is also what advances
        // the cache's `flushed_rows_version` (and its watch channel) when
        // `rows_flushed`, so skipping it on an empty map leaves the cache's
        // recorded version behind the CVR's for every config-only pass. Do not
        // reintroduce an `is_empty()` guard here.
        match self
            .row_cache
            .apply(row_updates, cvr.version.clone(), stats.rows_flushed)
            .await
        {
            Ok(count) => self.row_count = count,
            Err(e) => eprintln!("[cvr] row cache apply failed: {e}"),
        }

        // (`self.pending` was consumed by the mem::take above — nothing to clear.)
        stats.statements =
            stats.instances + stats.clients + stats.queries + stats.desires + stats.rows;

        // OTLP: the "sync" flush TS records via `recordSyncFlushStats`
        // (flush.type=sync). The `rows_deferred == 0` row-count gate lives
        // inside the ported fn (1:1 with row-record-cache.ts:144-151).
        let elapsed_ms = flush_started.elapsed().as_secs_f64() * 1000.0;
        crate::otel_metrics::record_sync_flush_stats(
            stats.rows as u64,
            stats.rows_deferred as u64,
            elapsed_ms,
        );

        if crate::tracer::enabled() {
            crate::tracer::note(
                "CVRStore",
                &format!(
                    "flush end cvr_id={} rows={} rows_deferred={} elapsed_ms={:.2}",
                    self.cvr_id, stats.rows, stats.rows_deferred, elapsed_ms,
                ),
            );
        }

        Ok(Some(stats))
    }

    // ─── Load ─────────────────────────────────────────────────────────

    /// Load the CVR, retrying while the rows table lags the CVR instance
    /// version. Port of TS `CVRStore.load`'s retry loop: a lagging rows table
    /// means the previous owner hasn't yet flushed its pending row writes, so we
    /// wait (having signalled it via the ownership grant) and retry up to
    /// `MAX_LOAD_ATTEMPTS` before declaring the CVR invalid.
    pub async fn load(&mut self, last_connect_time: f64) -> Result<LoadResult, CVRStoreError> {
        crate::tracer::note("CVRStore", &format!("load cvr_id={}", self.cvr_id));
        // TS `load` wraps the retry loop with `#recordLoad` (cvr-store.ts:
        // 289-303): cvr.load_attempts + cvr.load_duration, labeled
        // success/error (+ error.kind) — F-RRC-4.
        let start = std::time::Instant::now();
        let result = self.load_with_retries(last_connect_time).await;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        match &result {
            Ok(_) => crate::otel_metrics::record_load(elapsed_ms, "success", None),
            Err(e) => {
                crate::otel_metrics::record_load(elapsed_ms, "error", Some(cvr_error_kind(e)))
            }
        }
        result
    }

    async fn load_with_retries(
        &mut self,
        last_connect_time: f64,
    ) -> Result<LoadResult, CVRStoreError> {
        let mut last_behind: Option<CVRStoreError> = None;
        for attempt in 0..MAX_LOAD_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(LOAD_ATTEMPT_INTERVAL_MS))
                    .await;
            }
            match self.load_once(last_connect_time).await {
                Err(e @ CVRStoreError::RowsVersionBehind { .. }) => {
                    eprintln!("CVR load attempt {}: {e}", attempt + 1);
                    last_behind = Some(e);
                    continue;
                }
                // Rust-only addition (no TS twin: postgres.js has no acquire
                // timeout — pool contention QUEUES instead of erroring, see
                // main.rs pool comment). A pool-acquire timeout here is
                // transient back-pressure, not an invalid CVR: retry like the
                // flush path does, instead of failing the client group into a
                // reconnect + cold-rehydrate storm.
                Err(CVRStoreError::Sqlx(sqlx::Error::PoolTimedOut)) => {
                    eprintln!(
                        "CVR load attempt {}: pool acquire timed out; retrying",
                        attempt + 1
                    );
                    if attempt + 1 == MAX_LOAD_ATTEMPTS {
                        // Exhausted on pool pressure: surface the REAL error —
                        // must not fall through to ClientNotFound below, which
                        // would discard the client group's identity.
                        return Err(CVRStoreError::Sqlx(sqlx::Error::PoolTimedOut));
                    }
                    continue;
                }
                other => return other,
            }
        }
        // Exhausted attempts waiting for row catchup: the CVR is invalid (TS
        // throws ClientNotFoundError, which spawns a fresh client group).
        let detail = last_behind
            .map(|e| e.to_string())
            .unwrap_or_else(|| "rows never caught up".to_string());
        Err(CVRStoreError::ClientNotFound(format!(
            "max attempts exceeded waiting for CVR to catch up ({detail})"
        )))
    }

    async fn load_once(&mut self, last_connect_time: f64) -> Result<LoadResult, CVRStoreError> {
        let mut tx = self.pool.begin().await?;
        // TS `runTx(this.#db, ..., {mode: Mode.READONLY})` (cvr-store.ts:332-387):
        // `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`, then runTx's
        // `SET LOCAL statement_timeout = 0` + idle timeout (run-transaction.ts:
        // 37-56).
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;

        // TS runTx fires both `SET LOCAL`s without awaiting (run-transaction.ts:
        // 47-55) — pipelined; rust awaits each (two round trips).
        sqlx::query("SET LOCAL statement_timeout = 0")
            .execute(&mut *tx)
            .await?;
        sqlx::query(&format!(
            "SET LOCAL idle_in_transaction_session_timeout = {}",
            crate::row_record_cache::IDLE_TX_TIMEOUT_MS
        ))
        .execute(&mut *tx)
        .await?;

        // Load instance. LEFT JOIN the rows table's version so we can detect a
        // rows-behind CVR (the previous owner's pending row writes not yet
        // flushed) — see the `RowsVersionBehind` check below.
        let instance_sql = format!(
            r#"SELECT cvr."version",
                      (extract(epoch from cvr."lastActive") * 1000)::float8 AS "lastActive",
                      cvr."ttlClock", cvr."replicaVersion",
                      cvr."clientSchema", cvr."profileID", cvr."owner",
                      (extract(epoch from cvr."grantedAt") * 1000)::float8 AS "grantedAt",
                      COALESCE(cvr."deleted", false) AS "deleted",
                      rows."version" AS "rowsVersion"
               FROM "{0}".instances AS cvr
               LEFT JOIN "{0}"."rowsVersion" AS rows
                 ON cvr."clientGroupID" = rows."clientGroupID"
               WHERE cvr."clientGroupID" = $1"#,
            self.schema
        );
        let instance: Option<InstanceLoadRow> = sqlx::query_as(&instance_sql)
            .bind(&self.cvr_id)
            .fetch_optional(&mut *tx)
            .await?;

        let is_new = instance.is_none();
        // Deferred ownership grant: when this task is taking over a CVR whose
        // ownership lease has lapsed (or was never set), it grants itself
        // ownership AFTER the read-only load tx (a fire-and-forget UPDATE on the
        // pool, gated so it only wins under the same conditions as TS). Set here,
        // executed just before returning.
        let mut grant_ownership = false;
        // The raw instance version string and the rows table's version, compared
        // after the ownership grant to detect a rows-behind CVR.
        let mut rows_behind: Option<(String, Option<String>)> = None;

        let cvr = match instance {
            None => {
                // New CVR
                let cvr = CVR {
                    id: self.cvr_id.clone(),
                    version: crate::schema::types::EMPTY_CVR_VERSION.clone(),
                    last_active: 0,
                    ttl_clock: 0,
                    replica_version: None,
                    clients: BTreeMap::new(),
                    queries: BTreeMap::new(),
                    client_schema: None,
                    profile_id: None,
                };
                drop(tx);
                // First-sight initialization: TS `load` calls `putInstance(...)`
                // when the instance row is absent ("first time we see this CVR"),
                // queuing a pending instance write so the FIRST flush persists the
                // instance row even when the transaction has no material content
                // changes (e.g. a connect whose only op is a no-op). Without this,
                // a brand-new CVR whose first flush is content-empty would never
                // get its instance row written (the materiality guard would skip
                // it) — a divergence the sequence differential caught. Only fires
                // on first-sight (is_new), so it cannot resurrect the "no-op flush
                // advances an EXISTING instance" bug, which concerns loads where
                // the instance already exists. The flush-time `put_instance(cvr)`
                // overwrites this single-slot write with the final version/clock.
                self.put_instance(&cvr);
                return Ok(LoadResult { cvr, is_new: true });
            }
            Some((
                version,
                last_active,
                ttl_clock,
                replica_version,
                client_schema,
                profile_id,
                owner,
                granted_at,
                deleted,
                rows_version,
            )) => {
                // A CVR that was purged for inactivity is gone (TS throws
                // ClientNotFoundError, which triggers a fresh client group). The
                // message is byte-exact with TS (cvr-store.ts:423-424) because it
                // reaches the client verbatim as the `["error",…]` frame message.
                if deleted {
                    drop(tx);
                    return Err(CVRStoreError::ClientNotFound(
                        "Client has been purged due to inactivity".to_string(),
                    ));
                }
                // Ownership: if another task owns this CVR and its lease is still
                // live (granted after our last connect), refuse to load it.
                // Otherwise take it over by granting ourselves ownership below.
                // Port of TS `load`'s ownership handling.
                if owner.as_deref() != Some(self.task_id.as_str()) {
                    if granted_at.unwrap_or(0.0) > last_connect_time {
                        drop(tx);
                        return Err(CVRStoreError::OwnershipError {
                            owner: owner.unwrap_or_default(),
                            granted_at: granted_at.unwrap_or(0.0),
                            last_connect_time,
                        });
                    }
                    grant_ownership = true;
                }
                // Detect a rows-behind CVR: the raw instance version must equal
                // the rows table version (EMPTY "00" when absent). Checked AFTER
                // the ownership grant fires (below) so the previous owner is
                // signalled to flush before we retry. Port of TS `#load`'s
                // `version !== (rowsVersion ?? EMPTY_CVR_VERSION.stateVersion)`.
                let expected_rows = rows_version.clone().unwrap_or_else(|| {
                    crate::schema::types::EMPTY_CVR_VERSION
                        .state_version
                        .to_string()
                });
                if version != expected_rows {
                    rows_behind = Some((version.clone(), rows_version));
                }
                let cvr_version = maybe_version_string(&version)?;
                CVR {
                    id: self.cvr_id.clone(),
                    version: cvr_version,
                    last_active: last_active as i64,
                    ttl_clock: ttl_clock as i64,
                    replica_version,
                    clients: BTreeMap::new(),
                    queries: BTreeMap::new(),
                    client_schema,
                    profile_id,
                }
            }
        };

        // Load clients
        let clients_sql = format!(
            r#"SELECT "clientID" FROM "{}".clients WHERE "clientGroupID" = $1"#,
            self.schema
        );
        let clients: Vec<(String,)> = sqlx::query_as(&clients_sql)
            .bind(&self.cvr_id)
            .fetch_all(&mut *tx)
            .await?;

        // Load queries
        let queries_sql = format!(
            r#"SELECT "queryHash", "clientAST", "queryName", "queryArgs",
                      "patchVersion", "transformationHash", "transformationVersion",
                      "internal", "deleted", "rowSetSignature"
               FROM "{}".queries WHERE "clientGroupID" = $1 AND COALESCE("deleted", false) = false"#,
            self.schema
        );
        let queries: Vec<QueryLoadRow> = sqlx::query_as(&queries_sql)
            .bind(&self.cvr_id)
            .fetch_all(&mut *tx)
            .await?;

        // Load desires
        let desires_sql = format!(
            r#"SELECT "clientID", "queryHash", "patchVersion", "deleted", "ttlMs", "inactivatedAtMs"
               FROM "{}".desires WHERE "clientGroupID" = $1"#,
            self.schema
        );
        let desires: Vec<DesireLoadRow> = sqlx::query_as(&desires_sql)
            .bind(&self.cvr_id)
            .fetch_all(&mut *tx)
            .await?;

        drop(tx);

        // Build CVR from rows
        let mut cvr = cvr;

        for (client_id,) in clients {
            cvr.clients.insert(
                client_id.clone(),
                ClientRecord {
                    id: client_id,
                    desired_query_ids: Vec::new(),
                },
            );
        }

        for row in queries {
            let qrow = QueriesRow {
                client_group_id: self.cvr_id.clone(),
                query_hash: row.0.clone(),
                client_ast: row.1,
                query_name: row.2,
                query_args: row.3,
                patch_version: row.4,
                transformation_hash: row.5,
                transformation_version: row.6,
                internal: row.7,
                deleted: row.8,
                row_set_signature: row.9,
            };
            let query = as_query(&qrow)?;
            cvr.queries.insert(qrow.query_hash, query);
        }

        // Rebuild each client's desired-query list AND the per-client desire
        // state (inactivatedAt / ttl / version) on the corresponding query.
        // Without the client_state an inactive (TTL-pending) desire reloads as
        // fully active, so the TTL scheduler can never see it to expire it, and
        // its ttl/version are lost. Port of TS `loadCVR`, which reconstructs
        // `clientState` from the desires rows.
        for (client_id, query_hash, patch_version, deleted, ttl_ms, inactivated_at_ms) in &desires {
            // TS reads the nullable column as falsy (`!deleted`).
            let deleted = deleted.unwrap_or(false);
            // Only an active desire belongs in desiredQueryIDs. An inactive
            // desire remains solely in query.clientState until its TTL expires.
            if !deleted
                && inactivated_at_ms.is_none()
                && let Some(client) = cvr.clients.get_mut(client_id)
            {
                client.desired_query_ids.push(query_hash.clone());
            }
            // TS retains an inactivated tombstone's state even when `deleted`
            // is true, because its TTL/version still drive eviction and catchup.
            if deleted && inactivated_at_ms.is_none() {
                continue;
            }
            if let Some(state) = cvr
                .queries
                .get_mut(query_hash)
                .and_then(|q| q.client_state_mut())
            {
                state.insert(
                    client_id.clone(),
                    ClientState {
                        inactivated_at: (*inactivated_at_ms).map(|ms| ms as TTLClock),
                        ttl: clamp_ttl(TTL::Ms(
                            (*ttl_ms).map(|ms| ms as i64).unwrap_or(DEFAULT_TTL_MS),
                        )),
                        version: maybe_version_string(patch_version)?,
                    },
                );
            }
        }
        // NOTE: TS deliberately does NOT sort/dedup desiredQueryIDs at load
        // (cvr-store.ts:515 "why do we not sort desiredQueryIDs here?"), and
        // the desires PK makes duplicates impossible — F-CVR-STORE-6: keep the
        // DB scan order, matching TS.

        // Take over ownership of a CVR whose lease has lapsed. Done AFTER the
        // read-only load tx, on the pool, and gated so it only wins if nobody has
        // been granted the CVR more recently than our connect time. Fire-and-
        // forget / non-fatal (a lost race means another task legitimately won,
        // which its own flush guard enforces). Port of TS `load`'s ownership
        // UPDATE. Once granted, our own `flush` guard rejects a stale ex-owner.
        if grant_ownership {
            let grant_sql = format!(
                r#"UPDATE "{}".instances
                   SET "owner" = $1, "grantedAt" = to_timestamp($2 / 1000.0)
                   WHERE "clientGroupID" = $3
                     AND ("grantedAt" IS NULL
                          OR "grantedAt" <= to_timestamp($2 / 1000.0))"#,
                self.schema
            );
            let _ = sqlx::query(&grant_sql)
                .bind(&self.task_id)
                .bind(last_connect_time)
                .bind(&self.cvr_id)
                .execute(&self.pool)
                .await;
        }

        // Rows-behind: the ownership grant above has signalled the previous
        // owner to stop and flush; return so `load` waits and retries.
        if let Some((cvr_version, rows_version)) = rows_behind {
            return Err(CVRStoreError::RowsVersionBehind {
                cvr_version,
                rows_version,
            });
        }

        Ok(LoadResult { cvr, is_new })
    }

    // ─── Catchup config patches ──────────────────────────────────────

    pub async fn catchup_config_patches(
        &self,
        after_version: NullableCVRVersion,
        up_to_version: &CVRVersion,
        current: &CVRVersion,
    ) -> Result<Vec<PatchToVersion>, CVRStoreError> {
        // TS early return (cvr-store.ts:731-733): nothing to catch up when the
        // client is already at/past the target — before any SQL or version check.
        if crate::schema::types::cmp_versions(&after_version, &Some(up_to_version.clone()))
            != Ordering::Less
        {
            return Ok(Vec::new());
        }
        let start = after_version
            .as_ref()
            .map(version_string)
            .unwrap_or_default();
        let end = version_string(up_to_version);

        let mut tx = self.pool.begin().await?;
        // TS `new TransactionPool(lc, {mode: Mode.READONLY}).run(db)`
        // (cvr-store.ts:740/1296) → `runTx(db, worker, {mode})`
        // (transaction-pool.ts:285): READONLY plus runTx's two SET LOCALs.
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;

        // TS runTx fires both `SET LOCAL`s without awaiting (run-transaction.ts:
        // 47-55) — pipelined; rust awaits each (two round trips).
        sqlx::query("SET LOCAL statement_timeout = 0")
            .execute(&mut *tx)
            .await?;
        sqlx::query(&format!(
            "SET LOCAL idle_in_transaction_session_timeout = {}",
            crate::row_record_cache::IDLE_TX_TIMEOUT_MS
        ))
        .execute(&mut *tx)
        .await?;

        // Check version
        let check_sql = format!(
            r#"SELECT "version" FROM "{}".instances WHERE "clientGroupID" = $1"#,
            self.schema
        );
        let current_version: Option<(String,)> = sqlx::query_as(&check_sql)
            .bind(&self.cvr_id)
            .fetch_optional(&mut *tx)
            .await?;

        // TS `checkVersion` defaults a MISSING instance row to EMPTY_CVR_VERSION
        // and still compares (cvr-store.ts:1348). A client group that vanished
        // mid-catchup (GC/purge racing a reconnect) must therefore fail with
        // ConcurrentModification so the client cleanly rehomes — not silently
        // proceed and return stale/empty patches under a now-absent CVR.
        let cv = match &current_version {
            Some((cv,)) => maybe_version_string(cv)?,
            None => crate::schema::types::EMPTY_CVR_VERSION.clone(),
        };
        // TS `checkVersion(tx, ..., current)` (cvr-store.ts:743-745) verifies
        // the on-disk version against the caller's CURRENT CVR snapshot — not
        // against `upToCVR.version` (F-CVR-STORE-9: they coincide for today's
        // callers, but a caller catching up to an older snapshot while the CVR
        // has advanced must fail on `current`, exactly like TS).
        if cmp_cvr(&cv, current) != Ordering::Equal {
            return Err(CVRStoreError::ConcurrentModification {
                expected: version_string(current),
                actual: version_string(&cv),
            });
        }

        // Read GOT-query patches (query-state, no clientID) — the queries the
        // server transformed/removed in this range, needed so a reconnecting
        // client can rebuild its `gotQueriesPatch`. TS `catchupConfigPatches`
        // reads BOTH the queries and desires tables; the old code read only
        // desires, so reconnecting clients never learned which queries had
        // become "got" between their cookie and now.
        let queries_sql = format!(
            r#"SELECT "deleted", "queryHash", "patchVersion"
               FROM "{}".queries
               WHERE "clientGroupID" = $1 AND "patchVersion" > $2 AND "patchVersion" <= $3"#,
            self.schema
        );
        let query_rows: Vec<(Option<bool>, String, Option<String>)> = sqlx::query_as(&queries_sql)
            .bind(&self.cvr_id)
            .bind(&start)
            .bind(&end)
            .fetch_all(&mut *tx)
            .await?;

        // Read desires patches (per-client)
        let desires_sql = format!(
            r#"SELECT "clientID", "queryHash", "patchVersion", "deleted", "ttlMs", "inactivatedAtMs"
               FROM "{}".desires
               WHERE "clientGroupID" = $1 AND "patchVersion" > $2 AND "patchVersion" <= $3"#,
            self.schema
        );
        let desires: Vec<DesireLoadRow> = sqlx::query_as(&desires_sql)
            .bind(&self.cvr_id)
            .bind(&start)
            .bind(&end)
            .fetch_all(&mut *tx)
            .await?;

        drop(tx);

        let mut patches = Vec::new();
        // Got-query patches first (matching TS order), each with no clientID.
        for (deleted, query_hash, patch_version) in query_rows {
            let Some(pv) = patch_version else {
                continue; // patchVersion must be set for a query patch
            };
            let to_version = maybe_version_string(&pv)?;
            let patch = if deleted.unwrap_or(false) {
                Patch::Query(QueryPatch::Del {
                    id: query_hash,
                    client_id: None,
                })
            } else {
                Patch::Query(QueryPatch::Put {
                    id: query_hash,
                    client_id: None,
                })
            };
            patches.push(PatchToVersion { patch, to_version });
        }
        for (client_id, query_hash, patch_version, deleted, _, _) in desires {
            let to_version = maybe_version_string(&patch_version)?;
            // TS reads the nullable column as falsy.
            let patch = if deleted.unwrap_or(false) {
                Patch::Query(QueryPatch::Del {
                    id: query_hash,
                    client_id: Some(client_id),
                })
            } else {
                Patch::Query(QueryPatch::Put {
                    id: query_hash,
                    client_id: Some(client_id),
                })
            };
            patches.push(PatchToVersion { patch, to_version });
        }

        Ok(patches)
    }
}

// ─── Conversion functions ──────────────────────────────────────────────────

/// Convert a QueriesRow (DB row) to a QueryRecord (in-memory).
/// Mirrors TS `asQuery()` from cvr-store.ts.
/// Port of TS `cvrErrorKind` (cvr-store.ts:1421-1435): the `error.kind` label
/// for cvr.load_attempts / cvr.flush_attempts.
fn cvr_error_kind(e: &CVRStoreError) -> &'static str {
    match e {
        CVRStoreError::ClientNotFound(_) => "client_not_found",
        CVRStoreError::ConcurrentModification { .. } => "concurrent_modification",
        CVRStoreError::OwnershipError { .. } => "ownership",
        CVRStoreError::InvalidClientSchema(_) => "invalid_client_schema",
        _ => "error",
    }
}

pub fn as_query(row: &QueriesRow) -> Result<QueryRecord, VersionError> {
    // Version strings here come from the DB; a corrupt value is a recoverable
    // load error (TS `versionFromString` throws → caught), not a thread abort.
    let base = BaseQueryRecord {
        id: row.query_hash.clone(),
        transformation_hash: row.transformation_hash.clone(),
        transformation_version: row
            .transformation_version
            .as_deref()
            .map(maybe_version_string)
            .transpose()?,
        row_set_signature: row.row_set_signature.clone(),
    };

    // Discriminator matches TS `asQuery` (cvr-store.ts:119-168): a NULL clientAST
    // means a CUSTOM query; otherwise the `internal` flag distinguishes internal
    // from client. The old Rust checked `internal` first and keyed "custom" off
    // `query_name`, which for a corrupt custom row (clientAST null, queryName
    // null) silently built a client query with `ast = Null` instead of the
    // recoverable load error TS raises via its `assert(queryName && queryArgs)`.
    if row.client_ast.is_none() {
        // Custom query. TS asserts name & args are both set.
        let name = match &row.query_name {
            Some(n) if row.query_args.is_some() => n.clone(),
            _ => {
                return Err(VersionError::MalformedQuery {
                    query_hash: row.query_hash.clone(),
                    reason: "queryName and queryArgs must be set for custom queries",
                });
            }
        };
        return Ok(QueryRecord::Custom(CustomQueryRecord {
            base,
            name,
            args: row
                .query_args
                .as_ref()
                .and_then(|v| v.as_array())
                .map(|a| a.to_vec())
                .unwrap_or_default(),
            client_state: BTreeMap::new(),
            patch_version: row
                .patch_version
                .as_deref()
                .map(maybe_version_string)
                .transpose()?,
        }));
    }

    // TS: `const ast = astSchema.parse(row.clientAST)` (cvr-store.ts:148) —
    // valita validation at load, so a corrupt stored AST is a RECOVERABLE load
    // error, not a value that silently flows into pipeline building. The full
    // AST type lives downstream (rust-ivm), so this validates the structural
    // envelope every astSchema AST has — a JSON object with a string `table` —
    // and deep validation still happens when the pipeline is built
    // (F-CVR-STORE-1).
    let ast = row.client_ast.clone().unwrap_or(Value::Null);
    if !ast
        .as_object()
        .is_some_and(|o| o.get("table").is_some_and(Value::is_string))
    {
        return Err(VersionError::MalformedQuery {
            query_hash: row.query_hash.clone(),
            reason: "clientAST failed astSchema validation (not an object with a string `table`)",
        });
    }
    if row.internal == Some(true) {
        return Ok(QueryRecord::Internal(InternalQueryRecord { base, ast }));
    }

    Ok(QueryRecord::Client(ClientQueryRecord {
        base,
        ast,
        client_state: BTreeMap::new(),
        patch_version: row
            .patch_version
            .as_deref()
            .map(maybe_version_string)
            .transpose()?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Port of TS `cvrErrorKind` (cvr-store.ts:1421-1435): pins the exact otel
    /// `error.kind` attribute string for every `CVRStoreError` variant. TS keys
    /// on `instanceof` of four named errors and falls back to `"error"`; Rust
    /// keys on the enum variant. This is the label emitted on the tracing span
    /// (`ERROR_KIND_ATTRIBUTE`), so a wrong string silently mis-buckets error
    /// telemetry. Table-driven so a new variant that forgets its arm is caught.
    #[test]
    fn cvr_error_kind_matches_ts_labels() {
        use crate::schema::types::VersionError;
        let cases: Vec<(CVRStoreError, &str)> = vec![
            (
                CVRStoreError::ClientNotFound("c1".to_string()),
                "client_not_found",
            ),
            (
                CVRStoreError::ConcurrentModification {
                    expected: "00:01".to_string(),
                    actual: "00:02".to_string(),
                },
                "concurrent_modification",
            ),
            (
                CVRStoreError::OwnershipError {
                    owner: "task-2".to_string(),
                    granted_at: 1.0,
                    last_connect_time: 0.0,
                },
                "ownership",
            ),
            (
                CVRStoreError::InvalidClientSchema("bad".to_string()),
                "invalid_client_schema",
            ),
            // Fallback arm ("error"): the variants with no dedicated TS label.
            (
                CVRStoreError::RowsVersionBehind {
                    cvr_version: "00:03".to_string(),
                    rows_version: None,
                },
                "error",
            ),
            (
                CVRStoreError::VersionParse(VersionError::TooManyParts("a:b:c".to_string())),
                "error",
            ),
        ];
        for (err, want) in &cases {
            assert_eq!(
                cvr_error_kind(err),
                *want,
                "cvr_error_kind mismatch for {err:?}"
            );
        }
    }

    fn test_store() -> CVRStoreHandle {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgresql://localhost/test")
            .unwrap();
        CVRStoreHandle::new(
            pool,
            "test/cvr".to_string(),
            "cg1".to_string(),
            "task1".to_string(),
        )
    }

    fn make_cvr() -> CVR {
        CVR {
            id: "cg1".to_string(),
            version: CVRVersion {
                state_version: "v1".to_string(),
                config_version: None,
            },
            last_active: 0,
            ttl_clock: 0,
            replica_version: Some("r1".to_string()),
            clients: BTreeMap::new(),
            queries: BTreeMap::new(),
            client_schema: None,
            profile_id: None,
        }
    }

    fn make_client_query(id: &str) -> QueryRecord {
        QueryRecord::Client(ClientQueryRecord {
            base: BaseQueryRecord {
                id: id.to_string(),
                transformation_hash: Some("th1".to_string()),
                transformation_version: None,
                row_set_signature: None,
            },
            ast: serde_json::json!({"schema": "s", "table": "t"}),
            client_state: BTreeMap::new(),
            patch_version: None,
        })
    }

    #[test]
    fn test_pending_writes_empty_by_default() {
        let pending = PendingWrites::default();
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn catchup_reader_does_not_consume_buffered_writes() {
        let mut store = test_store();
        store.put_instance(&make_cvr());
        assert!(store.has_pending_writes());

        let reader = store.catchup_reader();
        assert_eq!(reader.schema, "test/cvr");
        assert_eq!(reader.cvr_id, "cg1");
        assert!(store.has_pending_writes());
    }

    #[test]
    fn test_put_instance_queues_write() {
        let cvr = make_cvr();
        // Simulate put_instance logic
        let pending = PendingWrites {
            pending_instance_write: Some(InstancesRow {
                client_group_id: cvr.id,
                version: "v1".to_string(),
                last_active: 0.0,
                ttl_clock: 0.0,
                replica_version: None,
                owner: None,
                granted_at: None,
                client_schema: None,
                profile_id: None,
            }),
            ..Default::default()
        };
        assert!(!pending.is_empty());
    }

    #[test]
    fn test_insert_client_queues_write() {
        let mut pending = PendingWrites::default();
        pending.pending_clients_insert.push(ClientsRow {
            client_group_id: "cg1".to_string(),
            client_id: "c1".to_string(),
        });
        assert!(!pending.is_empty());
    }

    #[test]
    fn test_put_query_queues_full_update() {
        let mut pending = PendingWrites::default();
        let q = make_client_query("hash1");
        let row = query_record_to_query_row("cg1", &q);
        pending
            .pending_query_updates
            .insert("hash1".to_string(), row);
        assert!(!pending.is_empty());
    }

    #[tokio::test]
    async fn test_update_query_queues_partial_update() {
        let mut store = test_store();
        let mut query = make_client_query("hash1");
        let QueryRecord::Client(query) = &mut query else {
            unreachable!()
        };
        query.patch_version = Some(CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        });
        store.update_query(&QueryRecord::Client(query.clone()));

        let partial = &store.pending.pending_query_partial_updates["hash1"];
        assert_eq!(partial.patch_version, Some(Some("v2".to_string())));
        assert_eq!(partial.transformation_hash, Some(Some("th1".to_string())));
        assert_eq!(partial.deleted, Some(false));
    }

    #[tokio::test]
    async fn test_mark_query_as_deleted() {
        let mut store = test_store();
        let patch = QueryPatch::Del {
            id: "hash1".to_string(),
            client_id: None,
        };
        let version = CVRVersion {
            state_version: "v3".to_string(),
            config_version: None,
        };
        store.mark_query_as_deleted(&version, &patch);

        let partial = &store.pending.pending_query_partial_updates["hash1"];
        assert_eq!(partial.patch_version, Some(Some("v3".to_string())));
        assert_eq!(partial.deleted, Some(true));
        assert_eq!(partial.transformation_hash, Some(None));
        assert_eq!(partial.transformation_version, Some(None));
    }

    #[test]
    fn test_update_row_set_signature() {
        let mut pending = PendingWrites::default();
        pending
            .pending_query_partial_updates
            .entry("hash1".to_string())
            .or_default()
            .row_set_signature = Some("deadbeef".to_string());
        assert!(!pending.is_empty());
    }

    #[test]
    fn test_put_desired_query_deduplicates() {
        let mut pending = PendingWrites::default();
        let key = "c1:hash1".to_string();
        let row = DesiresRow {
            client_group_id: "cg1".to_string(),
            client_id: "c1".to_string(),
            query_hash: "hash1".to_string(),
            patch_version: "v1".to_string(),
            deleted: Some(false),
            ttl: Some(300000.0),
            inactivated_at: None,
        };
        pending
            .pending_desire_updates
            .insert(key.clone(), row.clone());
        // Overwrite with updated version
        let row2 = DesiresRow {
            deleted: Some(true),
            ..row
        };
        pending.pending_desire_updates.insert(key, row2);
        assert_eq!(pending.pending_desire_updates.len(), 1);
        assert_eq!(
            pending
                .pending_desire_updates
                .get("c1:hash1")
                .unwrap()
                .deleted,
            Some(true)
        );
    }

    #[test]
    fn test_put_row_record_and_del() {
        let mut pending = PendingWrites::default();
        let id = RowID {
            schema: "s".to_string(),
            table: "t".to_string(),
            row_key: serde_json::Map::new(),
        };
        let id_str = crate::row_key::row_id_string(&id);
        let record = RowRecord {
            id: id.clone(),
            row_version: "rv1".to_string(),
            patch_version: CVRVersion {
                state_version: "v1".to_string(),
                config_version: None,
            },
            ref_counts: Some(BTreeMap::new()),
        };
        pending
            .pending_row_record_updates
            .insert(id_str.clone(), (id.clone(), Some(record)));
        assert!(!pending.is_empty());
        // Now delete
        pending
            .pending_row_record_updates
            .insert(id_str, (id, None));
    }

    #[test]
    fn test_force_updates() {
        let mut pending = PendingWrites::default();
        let id = RowID {
            schema: "s".to_string(),
            table: "t".to_string(),
            row_key: serde_json::Map::new(),
        };
        let id_str = crate::row_key::row_id_string(&id);
        pending.force_updates.insert(id_str);
        assert!(!pending.force_updates.is_empty());
    }

    /// Port-parity regression for F-CVR-STORE-11 (TS cvr-store.ts:1066-1086):
    /// `flush` must prune (a) row records deep-equal to what the CVR already
    /// stores and (b) delete/unreferenced tombstones for rows not in the CVR —
    /// and a pending set consisting ONLY of such no-ops must be a no-op flush
    /// (`Ok(None)`), never touching PostgreSQL. The store's pool is lazy and
    /// points at no live server, so pre-fix (no pruning) this flush attempted
    /// PG and errored — proving the test non-vacuous.
    #[tokio::test]
    async fn flush_prunes_noop_row_updates_like_ts() {
        let mut store = test_store();
        let cvr = make_cvr();

        let existing_id = RowID {
            schema: "public".to_string(),
            table: "issue".to_string(),
            row_key: serde_json::json!({"id": "1"}).as_object().unwrap().clone(),
        };
        let existing_record = RowRecord {
            id: existing_id.clone(),
            row_version: "rv1".to_string(),
            patch_version: CVRVersion {
                state_version: "v1".to_string(),
                config_version: None,
            },
            ref_counts: Some(BTreeMap::from([("q1".to_string(), 1i64)])),
        };
        // Seed the store's OWN row cache — the store reads `existing_rows` from
        // it (TS `CVRStore.getRowRecords()` -> `#rowCache`, cvr-store.ts:520), so
        // this is what a TS test's pre-inserted `cvr.rows` provides. Seeding also
        // marks the cache loaded, so the pruning path never reaches Postgres.
        store
            .seed_row_cache_for_test(HashMap::from([(
                crate::row_key::row_id_string(&existing_id),
                existing_record.clone(),
            )]))
            .await;

        // (a) re-write of an identical record — TS deepEqual prune.
        store.put_row_record(&existing_record);
        // (b) tombstone for a row that was never in the CVR — TS
        // `existing === undefined && !row?.refCounts` prune.
        let absent_id = RowID {
            schema: "public".to_string(),
            table: "issue".to_string(),
            row_key: serde_json::json!({"id": "ghost"})
                .as_object()
                .unwrap()
                .clone(),
        };
        store.del_row_record(&absent_id);
        // (b') refCounts-null record (not a plain delete) for an absent row —
        // also pruned by the same TS branch.
        store.put_row_record(&RowRecord {
            id: RowID {
                schema: "public".to_string(),
                table: "issue".to_string(),
                row_key: serde_json::json!({"id": "ghost2"})
                    .as_object()
                    .unwrap()
                    .clone(),
            },
            row_version: "rv1".to_string(),
            patch_version: CVRVersion {
                state_version: "v1".to_string(),
                config_version: None,
            },
            ref_counts: None,
        });

        let expected = CVRVersion {
            state_version: "v1".to_string(),
            config_version: None,
        };
        let result = store.flush(&expected, &cvr, 0.0).await;
        assert!(
            matches!(result, Ok(None)),
            "an all-no-op pending row set must be pruned to a no-op flush \
             (TS returns null); got {result:?}"
        );
    }

    /// A failure to load the row-record cache must PROPAGATE out of `flush`,
    /// exactly as TS's `getRowRecords()` rejects (`r.reject(e); throw e;`,
    /// row-record-cache.ts:208-211) and aborts the pass.
    ///
    /// Why this matters beyond error hygiene: `execute_row_updates` prunes a
    /// tombstone whose row is absent from `existing_rows` (TS's
    /// `existing === undefined && !row?.refCounts` branch). If a load failure
    /// degraded to an EMPTY existing-row set, every real tombstone would look
    /// like a row the CVR never had, get pruned, and the client would never
    /// receive the row DEL — silent data divergence, reported as a clean flush.
    ///
    /// Non-vacuous: restore the `if let Err(e) = load() { return empty }` swallow
    /// in `get_row_records` and this flush returns `Ok(None)` (the tombstone is
    /// pruned against the empty set) instead of `Err`, failing the assert. The
    /// cache is deliberately NOT seeded here, so `load()` hits the unreachable
    /// `postgresql://localhost/test` pool.
    #[tokio::test]
    async fn flush_propagates_a_row_cache_load_failure_instead_of_assuming_no_rows() {
        let mut store = test_store();
        let cvr = make_cvr();

        // A tombstone for a row the (unloadable) CVR may well contain.
        store.del_row_record(&RowID {
            schema: "public".to_string(),
            table: "issue".to_string(),
            row_key: serde_json::json!({"id": "1"}).as_object().unwrap().clone(),
        });

        let expected = CVRVersion {
            state_version: "v1".to_string(),
            config_version: None,
        };
        let result = store.flush(&expected, &cvr, 0.0).await;
        assert!(
            result.is_err(),
            "a row-cache load failure must surface as Err (TS rethrows), not be \
             silently treated as an empty existing-row set; got {result:?}"
        );
    }

    #[test]
    fn test_as_query_internal() {
        let row = QueriesRow {
            client_group_id: "cg1".to_string(),
            query_hash: "lmids".to_string(),
            client_ast: Some(serde_json::json!({"table": "app.clients"})),
            query_name: None,
            query_args: None,
            patch_version: None,
            transformation_hash: None,
            transformation_version: None,
            internal: Some(true),
            deleted: Some(false),
            row_set_signature: None,
        };
        let q = as_query(&row).unwrap();
        assert!(matches!(q, QueryRecord::Internal(_)));
    }

    #[test]
    fn test_as_query_client() {
        let row = QueriesRow {
            client_group_id: "cg1".to_string(),
            query_hash: "hash1".to_string(),
            client_ast: Some(serde_json::json!({"schema": "s", "table": "t"})),
            query_name: None,
            query_args: None,
            patch_version: None,
            transformation_hash: None,
            transformation_version: None,
            internal: None,
            deleted: Some(false),
            row_set_signature: None,
        };
        let q = as_query(&row).unwrap();
        assert!(matches!(q, QueryRecord::Client(_)));
    }

    /// F-CVR-STORE-1: TS `asQuery` runs `astSchema.parse(row.clientAST)`
    /// (cvr-store.ts:148), so a corrupt stored AST is a RECOVERABLE load error.
    /// Pre-fix, Rust passed the raw JSON through unvalidated (proven by
    /// temp-revert: the match below failed with Ok(Client)).
    #[test]
    fn test_as_query_rejects_malformed_client_ast() {
        let mut row = QueriesRow {
            client_group_id: "cg1".to_string(),
            query_hash: "hash1".to_string(),
            client_ast: Some(serde_json::json!({"bogus": 1})),
            query_name: None,
            query_args: None,
            patch_version: None,
            transformation_hash: None,
            transformation_version: None,
            internal: None,
            deleted: Some(false),
            row_set_signature: None,
        };
        assert!(
            matches!(as_query(&row), Err(VersionError::MalformedQuery { .. })),
            "an AST without a string `table` must fail like TS astSchema.parse"
        );
        // Non-object AST (e.g. a bare string) fails too.
        row.client_ast = Some(serde_json::json!("garbage"));
        assert!(matches!(
            as_query(&row),
            Err(VersionError::MalformedQuery { .. })
        ));
        // Internal queries validate through the same parse (TS line 148 runs
        // before the internal/client branch).
        row.client_ast = Some(serde_json::json!({"table": 42}));
        row.internal = Some(true);
        assert!(matches!(
            as_query(&row),
            Err(VersionError::MalformedQuery { .. })
        ));
    }

    #[test]
    fn test_as_query_custom() {
        let row = QueriesRow {
            client_group_id: "cg1".to_string(),
            query_hash: "hash1".to_string(),
            client_ast: None,
            query_name: Some("myQuery".to_string()),
            query_args: Some(serde_json::json!([1, "x"])),
            patch_version: None,
            transformation_hash: None,
            transformation_version: None,
            internal: None,
            deleted: Some(false),
            row_set_signature: None,
        };
        let q = as_query(&row).unwrap();
        match q {
            QueryRecord::Custom(r) => {
                assert_eq!(r.name, "myQuery");
                assert_eq!(r.args.len(), 2);
            }
            _ => panic!("expected Custom query"),
        }
    }

    /// Parity (BEHAVIORAL-SWEEP-FINDINGS.md, `as_query`): a corrupt custom row
    /// (clientAST null, no queryName) must be a recoverable load error — matching
    /// TS's `assert(queryName && queryArgs …)` — not a silently-built null-AST
    /// client query (the old Rust keyed "custom" off query_name and fell through
    /// to Client here).
    #[test]
    fn test_as_query_null_ast_missing_name_is_error() {
        let row = QueriesRow {
            client_group_id: "cg1".to_string(),
            query_hash: "hashCorrupt".to_string(),
            client_ast: None,
            query_name: None,
            query_args: None,
            patch_version: None,
            transformation_hash: None,
            transformation_version: None,
            internal: None,
            deleted: Some(false),
            row_set_signature: None,
        };
        assert!(matches!(
            as_query(&row),
            Err(VersionError::MalformedQuery { .. })
        ));
    }

    #[test]
    fn test_query_record_to_query_row_client() {
        let q = make_client_query("hash1");
        let row = query_record_to_query_row("cg1", &q);
        assert_eq!(row.query_hash, "hash1");
        assert_eq!(row.client_group_id, "cg1");
        assert!(row.client_ast.is_some());
        assert!(row.query_name.is_none());
        assert_eq!(row.internal, None);
        assert_eq!(row.transformation_hash, Some("th1".to_string()));
    }

    #[test]
    fn test_query_record_to_query_row_internal() {
        let q = QueryRecord::Internal(InternalQueryRecord {
            base: BaseQueryRecord {
                id: "lmids".to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
            },
            ast: serde_json::json!({"table": "app.clients"}),
        });
        let row = query_record_to_query_row("cg1", &q);
        assert_eq!(row.query_hash, "lmids");
        assert_eq!(row.internal, Some(true));
    }

    #[test]
    fn test_query_record_to_query_row_custom() {
        let q = QueryRecord::Custom(CustomQueryRecord {
            base: BaseQueryRecord {
                id: "hash1".to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
            },
            name: "myQuery".to_string(),
            args: vec![serde_json::json!(1)],
            client_state: BTreeMap::new(),
            patch_version: None,
        });
        let row = query_record_to_query_row("cg1", &q);
        assert_eq!(row.query_name, Some("myQuery".to_string()));
        assert!(row.query_args.is_some());
    }
}
