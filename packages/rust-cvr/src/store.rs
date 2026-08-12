//! Port of `packages/zero-cache/src/services/view-syncer/cvr-store.ts`.
//!
//! The CVRStore is the only component that writes to Postgres. It buffers
//! writes in a pending queue and flushes them atomically in a single
//! transaction.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;

use crate::ttl::DEFAULT_TTL_MS;
use crate::types::StoreOp;
use crate::types::*;
use crate::version::{
    CVRVersion, NullableCVRVersion, cmp_versions, version_from_string, version_string,
};
use std::cmp::Ordering;

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
    pub patch_version: Option<String>,
    pub deleted: Option<bool>,
    pub transformation_hash: Option<String>,
    pub transformation_version: Option<String>,
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

pub struct CVRStoreHandle {
    pool: PgPool,
    schema: String,
    cvr_id: String,
    task_id: String,
    pending: PendingWrites,
    row_count: usize,
    /// The CVR version this store believes is currently persisted — set on
    /// `load` and advanced after each successful `flush`. It equals the
    /// `_orig.version` that TS threads into `flush` as `expectedCurrentVersion`
    /// (each updater is built from the last-flushed CVR), so tracking it here
    /// lets the flush do the optimistic version+ownership check without the
    /// caller having to pass the pre-update version. `None` until first
    /// load/flush (a brand-new CVR, whose expected version is the empty version).
    current_version: Option<CVRVersion>,
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
            current_version: None,
        }
    }

    pub fn has_pending_writes(&self) -> bool {
        !self.pending.is_empty()
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
        // Partial update — only set changed fields.
        let existing = self
            .pending
            .pending_query_partial_updates
            .entry(query.id().to_string())
            .or_default();
        existing.transformation_hash = query.base().transformation_hash.clone();
        existing.transformation_version = query
            .base()
            .transformation_version
            .as_ref()
            .map(version_string);
    }

    pub fn mark_query_as_deleted(&mut self, _version: &CVRVersion, query_patch: &QueryPatch) {
        let id = match query_patch {
            QueryPatch::Del { id, .. } => id,
            QueryPatch::Put { id, .. } => id,
        };
        let existing = self
            .pending
            .pending_query_partial_updates
            .entry(id.clone())
            .or_default();
        existing.deleted = Some(true);
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
        // Kept for signature parity with TS `flush(expectedCurrentVersion, ...)`,
        // but the store's own `current_version` (set on load, advanced after each
        // flush) is authoritative — it equals TS's `_orig.version`. When the store
        // was never loaded (a brand-new CVR) the expected on-disk version is the
        // empty version (no instance row yet).
        _expected_current_version: &CVRVersion,
        cvr: &CVR,
        last_connect_time: f64,
    ) -> Result<Option<CVRFlushStats>, CVRStoreError> {
        // Queue the instance write
        self.put_instance(cvr);

        if self.pending.is_empty() && self.pending.pending_instance_write.is_none() {
            return Ok(None);
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
            let expected = self
                .current_version
                .clone()
                .unwrap_or_else(|| crate::version::EMPTY_CVR_VERSION);
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
                None => (crate::version::EMPTY_CVR_VERSION.state_version.to_string(), None, None),
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
            let expected_str = version_string(&expected);
            if db_version != expected_str {
                return Err(CVRStoreError::ConcurrentModification {
                    expected: expected_str,
                    actual: db_version,
                });
            }
        }

        // 1. Instance upsert with ownership check
        if let Some(instance) = &self.pending.pending_instance_write {
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

        // 2. Clients inserts
        if !self.pending.pending_clients_insert.is_empty() {
            for client in &self.pending.pending_clients_insert {
                let sql = format!(
                    r#"INSERT INTO "{}".clients ("clientGroupID", "clientID")
                       VALUES ($1, $2)
                       ON CONFLICT ("clientGroupID", "clientID") DO NOTHING"#,
                    self.schema
                );
                sqlx::query(&sql)
                    .bind(&client.client_group_id)
                    .bind(&client.client_id)
                    .execute(&mut *tx)
                    .await?;
            }
            stats.clients = self.pending.pending_clients_insert.len();
        }

        // 3. Clients deletes
        for client_id in &self.pending.pending_clients_delete {
            let sql = format!(
                r#"DELETE FROM "{}".clients WHERE "clientGroupID" = $1 AND "clientID" = $2"#,
                self.schema
            );
            sqlx::query(&sql)
                .bind(&self.cvr_id)
                .bind(client_id)
                .execute(&mut *tx)
                .await?;
        }

        // 4. Query upserts (full)
        for (hash, row) in &self.pending.pending_query_updates {
            let sql = format!(
                r#"INSERT INTO "{}".queries
                   ("clientGroupID", "queryHash", "clientAST", "queryName", "queryArgs",
                    "patchVersion", "transformationHash", "transformationVersion",
                    "internal", "deleted", "rowSetSignature")
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
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
            sqlx::query(&sql)
                .bind(&row.client_group_id)
                .bind(&row.query_hash)
                .bind(&row.client_ast)
                .bind(&row.query_name)
                .bind(&row.query_args)
                .bind(&row.patch_version)
                .bind(&row.transformation_hash)
                .bind(&row.transformation_version)
                .bind(row.internal)
                .bind(row.deleted)
                .bind(&row.row_set_signature)
                .execute(&mut *tx)
                .await?;
            stats.queries += 1;
        }

        // 5. Query partial updates
        for (hash, partial) in &self.pending.pending_query_partial_updates {
            let mut sets = Vec::new();
            let mut bind_idx = 2u32;
            if let Some(ref pv) = partial.patch_version {
                sets.push(format!(r#""patchVersion" = ${}"#, bind_idx));
                bind_idx += 1;
            }
            if let Some(d) = partial.deleted {
                sets.push(format!(r#""deleted" = ${}"#, bind_idx));
                bind_idx += 1;
            }
            if let Some(ref th) = partial.transformation_hash {
                sets.push(format!(r#""transformationHash" = ${}"#, bind_idx));
                bind_idx += 1;
            }
            if let Some(ref tv) = partial.transformation_version {
                sets.push(format!(r#""transformationVersion" = ${}"#, bind_idx));
                bind_idx += 1;
            }
            if let Some(ref rs) = partial.row_set_signature {
                sets.push(format!(r#""rowSetSignature" = ${}"#, bind_idx));
                bind_idx += 1;
            }
            if sets.is_empty() {
                continue;
            }
            let sql = format!(
                r#"UPDATE "{}".queries SET {} WHERE "clientGroupID" = $1 AND "queryHash" = ${}"#,
                self.schema,
                sets.join(", "),
                bind_idx
            );
            let mut q = sqlx::query(&sql).bind(&self.cvr_id);
            if let Some(ref pv) = partial.patch_version {
                q = q.bind(pv);
            }
            if let Some(d) = partial.deleted {
                q = q.bind(d);
            }
            if let Some(ref th) = partial.transformation_hash {
                q = q.bind(th);
            }
            if let Some(ref tv) = partial.transformation_version {
                q = q.bind(tv);
            }
            if let Some(ref rs) = partial.row_set_signature {
                q = q.bind(rs);
            }
            q = q.bind(hash);
            q.execute(&mut *tx).await?;
            stats.queries += 1;
        }

        // 6. Desire upserts
        for (_key, row) in &self.pending.pending_desire_updates {
            let sql = format!(
                r#"INSERT INTO "{}".desires
                   ("clientGroupID", "clientID", "queryHash", "patchVersion",
                    "deleted", "ttlMs", "inactivatedAtMs")
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   ON CONFLICT ("clientGroupID", "clientID", "queryHash") DO UPDATE SET
                    "patchVersion" = excluded."patchVersion",
                    "deleted" = excluded."deleted",
                    "ttlMs" = excluded."ttlMs",
                    "inactivatedAtMs" = excluded."inactivatedAtMs""#,
                self.schema
            );
            sqlx::query(&sql)
                .bind(&row.client_group_id)
                .bind(&row.client_id)
                .bind(&row.query_hash)
                .bind(&row.patch_version)
                .bind(row.deleted)
                .bind(row.ttl)
                .bind(row.inactivated_at)
                .execute(&mut *tx)
                .await?;
            stats.desires += 1;
        }

        // 7. Row record upserts and deletes.
        //
        // The `rows` table has a FOREIGN KEY to `rowsVersion(clientGroupID)`, so
        // a `rowsVersion` row must exist first. Upsert it (to the CVR version)
        // before writing any rows. The `rowsVersion` may lag `instances.version`
        // in general, but writing them together here is correct and satisfies
        // the constraint.
        if !self.pending.pending_row_record_updates.is_empty() {
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
        for (_id_str, (row_id, record)) in &self.pending.pending_row_record_updates {
            // A row leaves the CVR either as an explicit del (`None`) or as a put
            // with `refCounts = null` (the tombstone form used when a row is no
            // longer referenced by any query). BOTH must DELETE the row from the
            // `rows` table, matching TS `executeRowUpdates`, which routes `null`
            // and `refCounts == null` records into `deletes`. The old code skipped
            // `None` entirely and wrote tombstones as `refCounts = NULL` upserts —
            // leaking dead rows and silently dropping real deletions.
            let is_delete = match record {
                None => true,
                Some(row) => row.ref_counts.is_none(),
            };
            if is_delete {
                let row_key_json: Value =
                    serde_json::to_value(&row_id.row_key).unwrap_or(Value::Null);
                let sql = format!(
                    r#"DELETE FROM "{}".rows
                       WHERE "clientGroupID" = $1 AND "schema" = $2
                         AND "table" = $3 AND "rowKey" = $4"#,
                    self.schema
                );
                sqlx::query(&sql)
                    .bind(&self.cvr_id)
                    .bind(&row_id.schema)
                    .bind(&row_id.table)
                    .bind(&row_key_json)
                    .execute(&mut *tx)
                    .await?;
                stats.rows += 1;
                continue;
            }

            let row = record.as_ref().expect("non-delete row has a record");
            let row_key_json: Value =
                serde_json::to_value(&row.id.row_key).unwrap_or(Value::Null);
            let ref_counts_json: Option<Value> = row
                .ref_counts
                .as_ref()
                .map(|rc| serde_json::to_value(rc).unwrap_or(Value::Null));
            let sql = format!(
                r#"INSERT INTO "{}".rows
                   ("clientGroupID", "schema", "table", "rowKey",
                    "rowVersion", "patchVersion", "refCounts")
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   ON CONFLICT ("clientGroupID", "schema", "table", "rowKey")
                   DO UPDATE SET
                    "rowVersion" = excluded."rowVersion",
                    "patchVersion" = excluded."patchVersion",
                    "refCounts" = excluded."refCounts""#,
                self.schema
            );
            sqlx::query(&sql)
                .bind(&self.cvr_id)
                .bind(&row.id.schema)
                .bind(&row.id.table)
                .bind(&row_key_json)
                .bind(&row.row_version)
                .bind(version_string(&row.patch_version))
                .bind(&ref_counts_json)
                .execute(&mut *tx)
                .await?;
            stats.rows += 1;
        }

        tx.commit().await?;

        // The write committed at `cvr.version`; that is now the expected on-disk
        // version for the next flush's optimistic check.
        self.current_version = Some(cvr.version.clone());

        // Clear pending
        self.pending = PendingWrites::default();

        stats.statements =
            stats.instances + stats.clients + stats.queries + stats.desires + stats.rows;
        Ok(Some(stats))
    }

    // ─── Load ─────────────────────────────────────────────────────────

    pub async fn load(&mut self, last_connect_time: f64) -> Result<LoadResult, CVRStoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;

        // Load instance
        let instance_sql = format!(
            r#"SELECT "version",
                      (extract(epoch from "lastActive") * 1000)::float8 AS "lastActive",
                      "ttlClock", "replicaVersion",
                      "clientSchema", "profileID", "owner",
                      (extract(epoch from "grantedAt") * 1000)::float8 AS "grantedAt",
                      COALESCE("deleted", false) AS "deleted"
               FROM "{}".instances WHERE "clientGroupID" = $1"#,
            self.schema
        );
        let instance: Option<(
            String,
            f64,
            f64,
            Option<String>,
            Option<Value>,
            Option<String>,
            Option<String>,
            Option<f64>,
            bool,
        )> = sqlx::query_as(&instance_sql)
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

        let cvr = match instance {
            None => {
                // New CVR
                let cvr = CVR {
                    id: self.cvr_id.clone(),
                    version: crate::version::EMPTY_CVR_VERSION,
                    last_active: 0,
                    ttl_clock: 0,
                    replica_version: None,
                    clients: BTreeMap::new(),
                    queries: BTreeMap::new(),
                    client_schema: None,
                    profile_id: None,
                };
                drop(tx);
                self.current_version = Some(cvr.version.clone());
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
                let cvr_version = version_from_string(&version);
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
        let queries: Vec<(
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
        )> = sqlx::query_as(&queries_sql)
            .bind(&self.cvr_id)
            .fetch_all(&mut *tx)
            .await?;

        // Load desires
        let desires_sql = format!(
            r#"SELECT "clientID", "queryHash", "patchVersion", "deleted", "ttlMs", "inactivatedAtMs"
               FROM "{}".desires WHERE "clientGroupID" = $1"#,
            self.schema
        );
        let desires: Vec<(String, String, String, bool, Option<f64>, Option<f64>)> =
            sqlx::query_as(&desires_sql)
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
            let query = as_query(&qrow);
            cvr.queries.insert(qrow.query_hash, query);
        }

        // Rebuild each client's desired-query list AND the per-client desire
        // state (inactivatedAt / ttl / version) on the corresponding query.
        // Without the client_state an inactive (TTL-pending) desire reloads as
        // fully active, so the TTL scheduler can never see it to expire it, and
        // its ttl/version are lost. Port of TS `loadCVR`, which reconstructs
        // `clientState` from the desires rows.
        for (client_id, query_hash, patch_version, deleted, ttl_ms, inactivated_at_ms) in &desires {
            if *deleted {
                continue;
            }
            if let Some(client) = cvr.clients.get_mut(client_id) {
                client.desired_query_ids.push(query_hash.clone());
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
                        ttl: (*ttl_ms).map(|ms| ms as i64).unwrap_or(DEFAULT_TTL_MS),
                        version: version_from_string(patch_version),
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

        self.current_version = Some(cvr.version.clone());
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

        if let Some((cv,)) = &current_version {
            let cv = version_from_string(cv);
            if cmp_versions(&Some(cv.clone()), &Some(up_to_version.clone())) != Ordering::Equal {
                return Err(CVRStoreError::ConcurrentModification {
                    expected: version_string(up_to_version),
                    actual: version_string(&cv),
                });
            }
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
        let desires: Vec<(String, String, String, bool, Option<f64>, Option<f64>)> =
            sqlx::query_as(&desires_sql)
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
            let to_version = version_from_string(&pv);
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
            let to_version = version_from_string(&patch_version);
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
pub fn as_query(row: &QueriesRow) -> QueryRecord {
    let base = BaseQueryRecord {
        id: row.query_hash.clone(),
        transformation_hash: row.transformation_hash.clone(),
        transformation_version: row
            .transformation_version
            .as_deref()
            .map(version_from_string),
        row_set_signature: row.row_set_signature.clone(),
    };

    if row.internal == Some(true) {
        return QueryRecord::Internal(InternalQueryRecord {
            base,
            ast: row.client_ast.clone().unwrap_or(Value::Null),
        });
    }

    // External query — check if client or custom
    if let Some(name) = &row.query_name {
        return QueryRecord::Custom(CustomQueryRecord {
            base,
            name: name.clone(),
            args: row
                .query_args
                .as_ref()
                .and_then(|v| v.as_array())
                .map(|a| a.to_vec())
                .unwrap_or_default(),
            client_state: BTreeMap::new(),
            patch_version: row.patch_version.as_deref().map(version_from_string),
        });
    }

    QueryRecord::Client(ClientQueryRecord {
        base,
        ast: row.client_ast.clone().unwrap_or(Value::Null),
        client_state: BTreeMap::new(),
        patch_version: row.patch_version.as_deref().map(version_from_string),
    })
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

    #[test]
    fn test_put_instance_queues_write() {
        let cvr = make_cvr();
        let mut pending = PendingWrites::default();
        // Simulate put_instance logic
        pending.pending_instance_write = Some(InstancesRow {
            client_group_id: cvr.id,
            version: "v1".to_string(),
            last_active: 0.0,
            ttl_clock: 0.0,
            replica_version: None,
            owner: None,
            granted_at: None,
            client_schema: None,
            profile_id: None,
        });
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

    #[test]
    fn test_update_query_queues_partial_update() {
        let mut pending = PendingWrites::default();
        let mut partial = PartialQueriesRow::default();
        partial.transformation_hash = Some("th2".to_string());
        pending
            .pending_query_partial_updates
            .insert("hash1".to_string(), partial);
        assert!(!pending.is_empty());
    }

    #[test]
    fn test_mark_query_as_deleted() {
        let mut pending = PendingWrites::default();
        let patch = QueryPatch::Del {
            id: "hash1".to_string(),
            client_id: None,
        };
        pending
            .pending_query_partial_updates
            .entry("hash1".to_string())
            .or_default()
            .deleted = Some(true);
        assert!(!pending.is_empty());
        assert_eq!(
            patch,
            QueryPatch::Del {
                id: "hash1".to_string(),
                client_id: None
            }
        );
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
        let q = as_query(&row);
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
        let q = as_query(&row);
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
        let q = as_query(&row);
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
