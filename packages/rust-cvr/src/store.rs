//! Port of `packages/zero-cache/src/services/view-syncer/cvr-store.ts`.
//!
//! The CVRStore is the only component that writes to Postgres. It buffers
//! writes in a pending queue and flushes them atomically in a single
//! transaction.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;

use crate::ttl::{DEFAULT_TTL_MS, TTL, clamp_ttl};
use crate::types::StoreOp;
use crate::types::*;
use crate::version::{
    CVRVersion, NullableCVRVersion, VersionError, cmp_cvr, try_version_from_string, version_string,
};
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
    VersionParse(#[from] crate::version::VersionError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

// ─── Row types (mirroring PG schema) ────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstancesRow {
    pub client_group_id: String,
    pub version: String,
    pub last_active: f64,
    pub ttl_clock: f64,
    pub replica_version: Option<String>,
    pub owner: Option<String>,
    pub granted_at: Option<f64>,
    pub client_schema: Option<Value>,
    pub profile_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientsRow {
    pub client_group_id: String,
    pub client_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueriesRow {
    pub client_group_id: String,
    pub query_hash: String,
    pub client_ast: Option<Value>,
    pub query_name: Option<String>,
    pub query_args: Option<Value>,
    pub patch_version: Option<String>,
    pub transformation_hash: Option<String>,
    pub transformation_version: Option<String>,
    pub internal: Option<bool>,
    pub deleted: Option<bool>,
    pub row_set_signature: Option<String>,
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
type DesireLoadRow = (String, String, String, bool, Option<f64>, Option<f64>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DesiresRow {
    pub client_group_id: String,
    pub client_id: String,
    pub query_hash: String,
    pub patch_version: String,
    pub deleted: bool,
    pub ttl: Option<f64>,
    pub inactivated_at: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowsRow {
    pub client_group_id: String,
    pub schema: String,
    pub table: String,
    pub row_key: Value,
    pub row_version: String,
    pub patch_version: String,
    pub ref_counts: Option<Value>,
}

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

pub struct CVRStoreHandle {
    pool: PgPool,
    schema: String,
    cvr_id: String,
    task_id: String,
    pending: PendingWrites,
    row_count: usize,
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
    pub fn new(pool: PgPool, schema: String, cvr_id: String, task_id: String) -> Self {
        Self {
            pool,
            schema,
            cvr_id,
            task_id,
            pending: PendingWrites::default(),
            row_count: 0,
            _census: crate::live_count::Guard::new(&crate::live_count::CVR_STORE),
        }
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
                deleted,
                ttl: Some(ttl as f64),
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

    pub async fn flush(
        &mut self,
        expected_current_version: &CVRVersion,
        cvr: &CVR,
        last_connect_time: f64,
    ) -> Result<Option<CVRFlushStats>, CVRStoreError> {
        // Materiality check FIRST (port of TS `#flush`): the CVR instance row is
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

        if crate::trace::enabled() {
            crate::trace::note(
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
                    crate::version::EMPTY_CVR_VERSION.state_version.to_string(),
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

        // 7. Row record upserts and deletes.
        //
        // The `rows` table has a FOREIGN KEY to `rowsVersion(clientGroupID)`, so
        // a `rowsVersion` row must exist first. Upsert `rowsVersion` = the CVR
        // version on EVERY flush (not only when rows change): in this Rust port
        // the store is the single atomic PG writer (instance + rows + rowsVersion
        // in one tx; the RowRecordCache write-back is `flushed=true`, cache-only),
        // so `rowsVersion` must stay in lockstep with `instances.version`. If we
        // wrote it only on row changes, a config-only advance would leave
        // `rowsVersion` behind and make every subsequent `load` falsely detect a
        // rows-behind CVR (see the `RowsVersionBehind` check in `load_once`).
        {
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
        // Row updates, mirroring TS `RowRecordCache.executeRowUpdates`:
        // - a literal `None` record is a hard DELETE;
        // - EVERYTHING else — including refCounts-NULL tombstones — is upserted.
        //   A tombstone stays in the `rows` table carrying the deletion's
        //   patchVersion, which is precisely what catch-up reads to emit row
        //   DELs to reconnecting clients. (Hard-deleting tombstones — the
        //   previous behavior — starved catch-up of DELs: a reconnecting client
        //   could never learn a row was removed while it was away.) Tombstones
        //   never reach the row-record cache, whose load filters
        //   `refCounts IS NOT NULL`.
        // Upserts are batched into ONE `json_to_recordset` statement (TS shape):
        // a large hydration previously paid one PG round trip per row while
        // holding a shared pool connection — the flush-convoy driver behind the
        // capacity cliff.
        let mut upserts: Vec<&RowRecord> = Vec::new();
        let mut deletes: Vec<Value> = Vec::new();
        for (row_id, record) in pending.pending_row_record_updates.values() {
            match record {
                None => {
                    // Collect the (schema, table, rowKey) identity; batched below.
                    let mut obj = serde_json::Map::new();
                    obj.insert("schema".into(), Value::String(row_id.schema.clone()));
                    obj.insert("table".into(), Value::String(row_id.table.clone()));
                    obj.insert(
                        "rowKey".into(),
                        serde_json::to_value(&row_id.row_key).unwrap_or(Value::Null),
                    );
                    deletes.push(Value::Object(obj));
                }
                Some(row) => upserts.push(row),
            }
        }
        // Batch hard DELETEs into ONE statement (was one awaited DELETE per row →
        // N sequential PG round-trips, the flush-convoy driver behind the sandbox
        // ~20s hydrate stall on a latent CVR DB). Mirrors the upsert batch below
        // and TS's pipelined `executeRowUpdates` deletes: identical semantics —
        // rows are matched by (clientGroupID, schema, table, rowKey) with JSONB
        // rowKey equality, exactly as the per-row DELETE did — collapsed to one
        // round-trip via a `json_to_recordset` join.
        if !deletes.is_empty() {
            let n = deletes.len();
            let del_json = Value::Array(deletes);
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
        if !upserts.is_empty() {
            let rows_json = Value::Array(
                upserts
                    .iter()
                    .map(|r| {
                        serde_json::to_value(crate::row_record_cache::row_record_to_rows_row(
                            &self.cvr_id,
                            r,
                        ))
                        .unwrap_or(Value::Null)
                    })
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
            stats.rows += upserts.len();
        }

        tx.commit().await?;

        // (`self.pending` was consumed by the mem::take above — nothing to clear.)
        stats.statements =
            stats.instances + stats.clients + stats.queries + stats.desires + stats.rows;

        // OTLP: this store is the single atomic PG writer, so every flush here is
        // the "sync" flush TS records via `recordSyncFlushStats` (flush.type=sync).
        // TS only counts rows-flushed when nothing was deferred; mirror that.
        let elapsed_ms = flush_started.elapsed().as_secs_f64() * 1000.0;
        let rows_flushed = if stats.rows_deferred == 0 {
            stats.rows as u64
        } else {
            0
        };
        crate::otel_metrics::record_cvr_flush(elapsed_ms, rows_flushed, "sync");

        if crate::trace::enabled() {
            crate::trace::note(
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
        crate::trace::note("CVRStore", &format!("load cvr_id={}", self.cvr_id));
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
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
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
                    version: crate::version::EMPTY_CVR_VERSION.clone(),
                    last_active: 0,
                    ttl_clock: 0,
                    replica_version: None,
                    clients: BTreeMap::new(),
                    queries: BTreeMap::new(),
                    client_schema: None,
                    profile_id: None,
                };
                drop(tx);
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
                // ClientNotFoundError, which triggers a fresh client group).
                if deleted {
                    drop(tx);
                    return Err(CVRStoreError::ClientNotFound(self.cvr_id.clone()));
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
                let expected_rows = rows_version
                    .clone()
                    .unwrap_or_else(|| crate::version::EMPTY_CVR_VERSION.state_version.to_string());
                if version != expected_rows {
                    rows_behind = Some((version.clone(), rows_version));
                }
                let cvr_version = try_version_from_string(&version)?;
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
            // Only an active desire belongs in desiredQueryIDs. An inactive
            // desire remains solely in query.clientState until its TTL expires.
            if !*deleted
                && inactivated_at_ms.is_none()
                && let Some(client) = cvr.clients.get_mut(client_id)
            {
                client.desired_query_ids.push(query_hash.clone());
            }
            // TS retains an inactivated tombstone's state even when `deleted`
            // is true, because its TTL/version still drive eviction and catchup.
            if *deleted && inactivated_at_ms.is_none() {
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
                        version: try_version_from_string(patch_version)?,
                    },
                );
            }
        }
        // Sort desired query IDs
        for client in cvr.clients.values_mut() {
            client.desired_query_ids.sort();
            client.desired_query_ids.dedup();
        }

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
        _current: &CVRVersion,
    ) -> Result<Vec<PatchToVersion>, CVRStoreError> {
        let start = after_version
            .as_ref()
            .map(version_string)
            .unwrap_or_default();
        let end = version_string(up_to_version);

        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
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
            Some((cv,)) => try_version_from_string(cv)?,
            None => crate::version::EMPTY_CVR_VERSION.clone(),
        };
        if cmp_cvr(&cv, up_to_version) != Ordering::Equal {
            return Err(CVRStoreError::ConcurrentModification {
                expected: version_string(up_to_version),
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
               WHERE "clientGroupID" = $1 AND "patchVersion" > $2 AND "patchVersion" <= $3
               ORDER BY "patchVersion""#,
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
               WHERE "clientGroupID" = $1 AND "patchVersion" > $2 AND "patchVersion" <= $3
               ORDER BY "patchVersion""#,
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
            let to_version = try_version_from_string(&pv)?;
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
            let to_version = try_version_from_string(&patch_version)?;
            let patch = if deleted {
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
pub fn as_query(row: &QueriesRow) -> Result<QueryRecord, VersionError> {
    // Version strings here come from the DB; a corrupt value is a recoverable
    // load error (TS `versionFromString` throws → caught), not a thread abort.
    let base = BaseQueryRecord {
        id: row.query_hash.clone(),
        transformation_hash: row.transformation_hash.clone(),
        transformation_version: row
            .transformation_version
            .as_deref()
            .map(try_version_from_string)
            .transpose()?,
        row_set_signature: row.row_set_signature.clone(),
    };

    if row.internal == Some(true) {
        return Ok(QueryRecord::Internal(InternalQueryRecord {
            base,
            ast: row.client_ast.clone().unwrap_or(Value::Null),
        }));
    }

    // External query — check if client or custom
    if let Some(name) = &row.query_name {
        return Ok(QueryRecord::Custom(CustomQueryRecord {
            base,
            name: name.clone(),
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
                .map(try_version_from_string)
                .transpose()?,
        }));
    }

    Ok(QueryRecord::Client(ClientQueryRecord {
        base,
        ast: row.client_ast.clone().unwrap_or(Value::Null),
        client_state: BTreeMap::new(),
        patch_version: row
            .patch_version
            .as_deref()
            .map(try_version_from_string)
            .transpose()?,
    }))
}

/// Convert a QueryRecord to a QueriesRow for storage.
/// Mirrors TS `queryRecordToQueryRow` from schema/types.ts.
pub fn query_record_to_query_row(cvr_id: &str, query: &QueryRecord) -> QueriesRow {
    match query {
        QueryRecord::Internal(r) => QueriesRow {
            client_group_id: cvr_id.to_string(),
            query_hash: r.base.id.clone(),
            client_ast: Some(r.ast.clone()),
            query_name: None,
            query_args: None,
            patch_version: None,
            transformation_hash: r.base.transformation_hash.clone(),
            transformation_version: r.base.transformation_version.as_ref().map(version_string),
            internal: Some(true),
            deleted: Some(false),
            row_set_signature: r.base.row_set_signature.clone(),
        },
        QueryRecord::Client(r) => QueriesRow {
            client_group_id: cvr_id.to_string(),
            query_hash: r.base.id.clone(),
            client_ast: Some(r.ast.clone()),
            query_name: None,
            query_args: None,
            patch_version: r.patch_version.as_ref().map(version_string),
            transformation_hash: r.base.transformation_hash.clone(),
            transformation_version: r.base.transformation_version.as_ref().map(version_string),
            internal: None,
            deleted: Some(false),
            row_set_signature: r.base.row_set_signature.clone(),
        },
        QueryRecord::Custom(r) => QueriesRow {
            client_group_id: cvr_id.to_string(),
            query_hash: r.base.id.clone(),
            client_ast: None,
            query_name: Some(r.name.clone()),
            query_args: Some(Value::Array(r.args.clone())),
            patch_version: r.patch_version.as_ref().map(version_string),
            transformation_hash: r.base.transformation_hash.clone(),
            transformation_version: r.base.transformation_version.as_ref().map(version_string),
            internal: None,
            deleted: Some(false),
            row_set_signature: r.base.row_set_signature.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            deleted: false,
            ttl: Some(300000.0),
            inactivated_at: None,
        };
        pending
            .pending_desire_updates
            .insert(key.clone(), row.clone());
        // Overwrite with updated version
        let row2 = DesiresRow {
            deleted: true,
            ..row
        };
        pending.pending_desire_updates.insert(key, row2);
        assert_eq!(pending.pending_desire_updates.len(), 1);
        assert!(
            pending
                .pending_desire_updates
                .get("c1:hash1")
                .unwrap()
                .deleted
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
