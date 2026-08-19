//! TS-parity port of `packages/zero-cache/src/services/view-syncer/row-record-cache.ts`.
//!
//! ## Invariants preserved (zero divergence from TS)
//!
//! 1. In-memory `cache` loaded once per CG via `load()`, kept in sync via `apply()`.
//! 2. `pending` holds deferred writes not yet in Postgres.
//! 3. `pending_rows_version` is the CVR version those pending rows bring us to.
//! 4. `flushed_rows_version` is the CVR version actually on disk.
//! 5. When `pending != flushed`, a background flush task pushes rows and advances
//!    `flushed_rows_version`.
//! 6. Write-back latch: once flushing starts, ALL subsequent `execute_row_updates`
//!    with `allow-defer` return empty (defer) until the flush catches up.
//! 7. Deferred threshold: 100 rows by default.
//!
//! ## Transaction modes (verified from `db/mode-enum.ts` + `run-transaction.ts`)
//!
//! - Flush tx: `BEGIN ISOLATION LEVEL READ COMMITTED` + `SET LOCAL statement_timeout = 0`
//!   + `SET LOCAL idle_in_transaction_session_timeout = 60000`.
//! - Catchup tx: `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` + same SET LOCALs.

use crate::row_key::{RowID, row_id_string};
use crate::version::{CVRVersion, NullableCVRVersion, try_version_from_string, version_string};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex as TokioMutex, mpsc, watch};

/// The cache and the CVR updater share one `RowRecord` type (`crate::types`),
/// so no per-row conversion is needed when the cache's records cross into the
/// updater's `RowRecordMap` (and back on flush). `ref_counts` is a `BTreeMap`
/// (`RefCounts`), giving deterministic key order for the DB `refCounts` jsonb.
pub use crate::types::RowRecord;

/// Mirrors TS `RowsRow` from `schema/cvr.ts` — the DB row form.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowsRow {
    #[serde(rename = "clientGroupID")]
    pub client_group_id: String,
    pub schema: String,
    pub table: String,
    #[serde(rename = "rowKey")]
    pub row_key: serde_json::Value,
    #[serde(rename = "rowVersion")]
    pub row_version: String,
    #[serde(rename = "patchVersion")]
    pub patch_version: String,
    #[serde(rename = "refCounts")]
    pub ref_counts: Option<serde_json::Value>,
}

/// Database row form for sqlx `FromRow`.
#[derive(sqlx::FromRow)]
struct RowsRowDb {
    #[sqlx(rename = "clientGroupID")]
    client_group_id: String,
    schema: String,
    table: String,
    #[sqlx(rename = "rowKey")]
    row_key: serde_json::Value,
    #[sqlx(rename = "rowVersion")]
    row_version: String,
    #[sqlx(rename = "patchVersion")]
    patch_version: String,
    #[sqlx(rename = "refCounts")]
    ref_counts: Option<serde_json::Value>,
}

impl From<RowsRowDb> for RowsRow {
    fn from(db: RowsRowDb) -> Self {
        RowsRow {
            client_group_id: db.client_group_id,
            schema: db.schema,
            table: db.table,
            row_key: db.row_key,
            row_version: db.row_version,
            patch_version: db.patch_version,
            ref_counts: db.ref_counts,
        }
    }
}

/// Error from decoding a DB `RowsRow` into a `RowRecord`. All variants indicate
/// malformed data in the `rows` table; mapped to `sqlx::Error::Decode` at the
/// call site so a corrupt row fails the load recoverably instead of aborting the
/// task (matching TS, which throws on malformed shapes).
#[derive(Debug, thiserror::Error)]
pub enum RowRecordError {
    #[error("rowKey is not an object: {0:?}")]
    RowKeyNotObject(serde_json::Value),
    #[error("refCounts is not an object: {0:?}")]
    RefCountsNotObject(serde_json::Value),
    #[error("refCount value is not an integer: {0:?}")]
    RefCountNotInteger(serde_json::Value),
    #[error("invalid patchVersion: {0}")]
    Version(#[from] crate::version::VersionError),
}

/// Converts a `RowsRow` (DB form) to a `RowRecord` (cache form).
/// Mirrors TS `rowsRowToRowRecord` from `schema/cvr.ts`.
pub fn rows_row_to_row_record(row: &RowsRow) -> Result<RowRecord, RowRecordError> {
    let row_key_map = match &row.row_key {
        serde_json::Value::Object(m) => m.clone(),
        other => return Err(RowRecordError::RowKeyNotObject(other.clone())),
    };
    let ref_counts = row
        .ref_counts
        .as_ref()
        .map(|v| match v {
            serde_json::Value::Object(m) => m
                .iter()
                .map(|(k, v)| {
                    v.as_i64()
                        .map(|n| (k.clone(), n))
                        .ok_or_else(|| RowRecordError::RefCountNotInteger(v.clone()))
                })
                .collect::<Result<_, _>>(),
            other => Err(RowRecordError::RefCountsNotObject(other.clone())),
        })
        .transpose()?;
    Ok(RowRecord {
        id: RowID {
            schema: row.schema.clone(),
            table: row.table.clone(),
            row_key: row_key_map,
        },
        row_version: row.row_version.clone(),
        patch_version: try_version_from_string(&row.patch_version)?,
        ref_counts,
    })
}

/// Converts a `RowRecord` (cache form) to a `RowsRow` (DB form).
/// Mirrors TS `rowRecordToRowsRow` from `schema/cvr.ts`.
pub fn row_record_to_rows_row(client_group_id: &str, record: &RowRecord) -> RowsRow {
    let ref_counts = record.ref_counts.as_ref().map(|rc| {
        let map: serde_json::Map<String, serde_json::Value> = rc
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::Number((*v).into())))
            .collect();
        serde_json::Value::Object(map)
    });

    RowsRow {
        client_group_id: client_group_id.to_string(),
        schema: record.id.schema.clone(),
        table: record.id.table.clone(),
        row_key: serde_json::Value::Object(record.id.row_key.clone()),
        row_version: record.row_version.clone(),
        patch_version: version_string(&record.patch_version),
        ref_counts,
    }
}

/// The flush mode for `execute_row_updates`.
/// Mirrors TS `'allow-defer' | 'force'`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushMode {
    AllowDefer,
    Force,
}

/// The structured result of `execute_row_updates` when not deferred.
/// The TS wrapper uses this to build the exact same SQL via postgres.js tagged
/// templates (zero SQL text divergence). The Rust flush path uses it to execute
/// via sqlx.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowUpdateStatements {
    /// The rowsVersion upsert row: `{clientGroupID, version}`.
    pub rows_version: RowsVersionRow,
    /// Per-row deletes (schema, table, rowKey).
    pub deletes: Vec<RowKeyRef>,
    /// Bulk insert rows for `json_to_recordset`.
    pub inserts: Vec<RowsRow>,
    /// Total count of row updates (including deletes).
    pub total_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowsVersionRow {
    #[serde(rename = "clientGroupID")]
    pub client_group_id: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowKeyRef {
    pub schema: String,
    pub table: String,
    pub row_key: serde_json::Value,
}

/// Result of `execute_row_updates`: either deferred (no-op) or statements to execute.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ExecuteResult {
    /// Deferred — caller should not write anything.
    #[serde(rename = "defer")]
    Defer,
    /// Execute these statements on the caller's transaction.
    #[serde(rename = "execute")]
    Execute(RowUpdateStatements),
}

/// Internal cache state, protected by a tokio mutex (flush task needs async access).
struct CacheState {
    /// Loaded once, then kept in sync. Keyed by `rowIDString(id)`. Behind an
    /// `Arc` so `get_row_records` snapshots are O(1) (writers `Arc::make_mut`).
    cache: Option<Arc<HashMap<String, RowRecord>>>,
    /// Pending deferred writes. Keyed by `rowIDString(id)`. Value is (RowID, Option<RowRecord>)
    /// where the RowID is needed for tombstone DELETEs.
    pending: HashMap<String, (RowID, Option<RowRecord>)>,
    pending_rows_version: Option<CVRVersion>,
    flushed_rows_version: Option<CVRVersion>,
    /// Whether a background flush task is currently running.
    flushing: bool,
    /// Set when a background flush fails. `flushed()` returns this as an error
    /// instead of blocking forever (in TS a flush failure calls `failService`,
    /// tearing down the whole service; here we surface it to awaiters).
    flush_error: Option<String>,
    /// Live-instance census guard (leak hunting). `RowRecordCache` is `Clone` —
    /// clones share this `Arc<TokioMutex<CacheState>>`, so the guard lives here,
    /// in the single shared inner state, and the census counts logical caches,
    /// not handle-clones. Inc on `CacheState::new`, dec when the last clone (and
    /// thus the Arc) drops.
    _census: crate::live_count::Guard,
}

impl CacheState {
    fn new() -> Self {
        CacheState {
            cache: None,
            pending: HashMap::new(),
            pending_rows_version: None,
            flushed_rows_version: None,
            flushing: false,
            flush_error: None,
            _census: crate::live_count::Guard::new(&crate::live_count::ROW_RECORD_CACHE),
        }
    }
}

impl Drop for CacheState {
    fn drop(&mut self) {
        // This drops exactly once — when the last `RowRecordCache` clone (and
        // thus the shared `Arc<TokioMutex<CacheState>>`) is gone. Dropping with
        // deferred writes still pending means those row updates never reached
        // Postgres and `flushed_rows_version` never caught up to
        // `pending_rows_version` — a leak-suspect teardown. Gated backtrace
        // (`RUST_CVR_DROP_BACKTRACE=1`) names the drop path; prod pays nothing.
        if !self.pending.is_empty() || self.pending_rows_version != self.flushed_rows_version {
            eprintln!(
                "[cvr] RowRecordCache dropped with pending row writes \
                 (pending={}, pending_version={:?}, flushed_version={:?}) \
                 — deferred rows were NOT flushed [census {}]",
                self.pending.len(),
                self.pending_rows_version,
                self.flushed_rows_version,
                crate::live_count::snapshot(),
            );
            crate::live_count::drop_backtrace("RowRecordCache(pending)");
        }
    }
}

/// Callback for service failure (mirrors TS `failService`).
pub type FailCallback = Arc<dyn Fn(String) + Send + Sync + 'static>;

/// Callback for async flush metrics (mirrors TS `#recordAsyncFlushStats`).
pub type MetricsCallback = Arc<dyn Fn(usize, f64) + Send + Sync + 'static>;

/// The RowRecordCache — write-through/write-back adapter for `cvr.rows`.
///
/// State is behind `Arc<TokioMutex<CacheState>>` because the background flush
/// task (spawned via `tokio::spawn`) needs async access to the same state.
///
/// `Clone` is cheap — every field is either an `Arc`/`PgPool` (both refcounted)
/// or a small value — and all clones share the same `state`/`pool`. The syncer
/// offloads CVR I/O onto its shared-pool runtime by moving a clone of the cache
/// into a spawned task (doc 91 spawn-offload), so the clone must alias the same
/// underlying state.
#[derive(Clone)]
pub struct RowRecordCache {
    state: Arc<TokioMutex<CacheState>>,
    pool: sqlx::PgPool,
    schema: String,
    cvr_id: String,
    deferred_threshold: usize,
    fail_service: FailCallback,
    metrics_callback: Option<MetricsCallback>,
    /// Watch channel tracking the current flushed version.
    /// `flushed()` awaits until this reaches `pending_rows_version`.
    flushed_version_tx: watch::Sender<Option<CVRVersion>>,
    /// Atomic flag for sync defer-decision (mirrors TS `#flushing !== null`).
    pub is_flushing: Arc<AtomicBool>,
}

/// The maximum page size for `load()` cursor (matches TS `.cursor(5000)`).
/// The maximum page size for `catchupRowPatches` cursor (matches TS `.cursor(10000)`).
const CATCHUP_PAGE_SIZE: usize = 10000;

/// The default deferred row flush threshold (matches TS default `100`).
pub const DEFAULT_DEFERRED_THRESHOLD: usize = 100;

/// The idle-in-transaction timeout (matches TS `run-transaction.ts`).
const IDLE_TX_TIMEOUT_MS: u32 = 60_000;

impl RowRecordCache {
    pub fn new(
        pool: sqlx::PgPool,
        schema: String,
        cvr_id: String,
        deferred_threshold: usize,
        fail_service: FailCallback,
        metrics_callback: Option<MetricsCallback>,
    ) -> Self {
        let (flushed_version_tx, _) = watch::channel(None);
        let is_flushing = Arc::new(AtomicBool::new(false));
        RowRecordCache {
            state: Arc::new(TokioMutex::new(CacheState::new())),
            pool,
            schema,
            cvr_id,
            deferred_threshold,
            fail_service,
            metrics_callback,
            flushed_version_tx,
            is_flushing,
        }
    }

    /// Mirrors TS `#ensureLoaded()` / `load()`. Streams rows in pages of 5000.
    /// Returns the number of rows loaded.
    pub async fn load(&self) -> Result<usize, sqlx::Error> {
        let mut state = self.state.lock().await;
        if state.cache.is_some() {
            return Ok(state.cache.as_ref().unwrap().len());
        }

        let sql = format!(
            r#"SELECT "clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts"
            FROM "{}"."rows"
              WHERE "clientGroupID" = $1 AND "refCounts" IS NOT NULL"#,
            self.schema
        );

        let mut stream = sqlx::query_as::<_, RowsRowDb>(&sql)
            .bind(&self.cvr_id)
            .fetch(&self.pool);

        let mut cache: HashMap<String, RowRecord> = HashMap::new();

        while let Some(row_result) = stream.next().await {
            let db_row = row_result?;
            let rows_row: RowsRow = db_row.into();
            let record =
                rows_row_to_row_record(&rows_row).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
            let key = row_id_string(&record.id);
            cache.insert(key, record);
        }

        let count = cache.len();
        state.cache = Some(Arc::new(cache));
        Ok(count)
    }

    /// Mirrors TS `getRowRecords()`. Returns an `Arc` snapshot of the cache
    /// keyed by `rowIDString` — an O(1) refcount bump, NOT a deep copy. This is
    /// called once per advance/config/TTL pass per client group; deep-cloning
    /// the full CVR row set here (the previous behavior) was the dominant
    /// allocator load at high client counts. Writers use `Arc::make_mut`, so a
    /// snapshot held across a concurrent write-back sees a stable view.
    /// Must be called after `load()`.
    pub async fn get_row_records(&self) -> Arc<HashMap<String, RowRecord>> {
        let state = self.state.lock().await;
        match &state.cache {
            Some(c) => Arc::clone(c),
            None => Arc::new(HashMap::new()),
        }
    }

    /// Mirrors TS `apply(rowRecords, rowsVersion, flushed)`.
    /// Returns the cache size after applying.
    pub async fn apply(
        &self,
        row_records: Vec<(RowID, Option<RowRecord>)>,
        rows_version: CVRVersion,
        flushed: bool,
    ) -> Result<usize, String> {
        let mut state = self.state.lock().await;

        // Ensure cache is loaded.
        if state.cache.is_none() {
            return Err("cache not loaded".to_string());
        }

        // Update cache first (separate borrow scope). `make_mut` mutates in
        // place when no snapshot is outstanding (refcount 1); with a live
        // `get_row_records` snapshot it copies-on-write once — paid only on
        // row-writing flushes, instead of the old deep clone on every read.
        {
            let cache = Arc::make_mut(state.cache.as_mut().unwrap());
            for (id, row) in &row_records {
                let id_str = row_id_string(id);
                match row {
                    None => {
                        cache.remove(&id_str);
                    }
                    Some(r) if r.ref_counts.is_none() => {
                        cache.remove(&id_str);
                    }
                    Some(r) => {
                        cache.insert(id_str, r.clone());
                    }
                }
            }
        }

        // Then update pending (separate borrow scope).
        if !flushed {
            for (id, row) in &row_records {
                let id_str = row_id_string(id);
                state.pending.insert(id_str, (id.clone(), row.clone()));
            }
        }

        state.pending_rows_version = Some(rows_version.clone());

        // When the caller already persisted these rows (`flushed`), the on-disk
        // version advances too — otherwise `flushed()` (which waits for
        // `flushed_rows_version == pending_rows_version`) would block forever,
        // since no background flush is spawned in this path.
        if flushed {
            state.flushed_rows_version = Some(rows_version.clone());
            let _ = self.flushed_version_tx.send(Some(rows_version.clone()));
        }

        // Snapshot the post-update cache size while we still hold this guard.
        // The tokio `Mutex` is NOT reentrant: re-locking `self.state` below while
        // this guard is live deadlocks — and the `flushed=true` write-back path
        // (which does not spawn a flush) never dropped the guard before the old
        // re-lock, hanging the CG thread on every hydrate. Compute `count` here
        // and drop the guard explicitly in both branches instead.
        let count = state.cache.as_ref().map(|c| c.len()).unwrap_or(0);

        // Initiate a flush if not already flushing and not flushed.
        let should_spawn_flush = !flushed && !self.is_flushing.load(Ordering::SeqCst);

        if should_spawn_flush {
            self.is_flushing.store(true, Ordering::SeqCst);
            state.flushing = true;
            drop(state);

            // Spawn the background flush task.
            let state_clone = Arc::clone(&self.state);
            let pool = self.pool.clone();
            let schema = self.schema.clone();
            let cvr_id = self.cvr_id.clone();
            let fail_service = Arc::clone(&self.fail_service);
            let metrics_callback = self.metrics_callback.clone();
            let flushed_tx = self.flushed_version_tx.clone();

            let is_flushing_clone = Arc::clone(&self.is_flushing);
            tokio::spawn(flush_loop(FlushLoopContext {
                state: state_clone,
                pool,
                schema,
                cvr_id,
                fail_service,
                metrics_callback,
                flushed_tx,
                is_flushing: is_flushing_clone,
            }));
        } else {
            drop(state);
        }

        Ok(count)
    }

    /// Mirrors TS `hasPendingUpdates()`.
    pub async fn has_pending_updates(&self) -> bool {
        let state = self.state.lock().await;
        state.flushing
    }

    /// Mirrors TS `flushed(lc)`. Waits until all pending writes are flushed, or
    /// returns an error if the background flush failed (rather than blocking
    /// forever). Compares `flushed_rows_version` to the LIVE
    /// `pending_rows_version`, so a later `apply` that raises `pending` is also
    /// awaited — the flush loop itself runs until the two are equal.
    pub async fn flushed(&self) -> Result<(), String> {
        // Subscribe BEFORE the first check so no wakeup is missed between the
        // check and the `changed().await`.
        let mut rx = self.flushed_version_tx.subscribe();
        loop {
            {
                let state = self.state.lock().await;
                if let Some(e) = &state.flush_error {
                    return Err(e.clone());
                }
                if state.pending_rows_version == state.flushed_rows_version {
                    return Ok(());
                }
            }
            if rx.changed().await.is_err() {
                return Err("flush version channel closed".to_string());
            }
        }
    }

    /// Mirrors TS `clear()`. Clears the in-memory cache but preserves pending writes.
    pub async fn clear(&self) {
        let mut state = self.state.lock().await;
        state.cache = None;
    }

    /// Mirrors TS `executeRowUpdates(tx, version, rowUpdates, mode)`.
    ///
    /// Returns `Defer` if the mode is `AllowDefer` and either:
    /// - A flush is currently in progress, OR
    /// - The batch exceeds `deferred_threshold`.
    ///
    /// Otherwise returns `Execute(statements)` containing the structured data
    /// needed to build the exact same SQL as the TS implementation.
    pub fn execute_row_updates(
        &self,
        version: &CVRVersion,
        row_updates: &[(RowID, Option<RowRecord>)],
        mode: FlushMode,
    ) -> ExecuteResult {
        // Check defer conditions using atomics (matches TS exactly).
        if mode == FlushMode::AllowDefer
            && (self.is_flushing.load(Ordering::SeqCst)
                || row_updates.len() > self.deferred_threshold)
        {
            return ExecuteResult::Defer;
        }

        let version_str = version_string(version);
        let rows_version = RowsVersionRow {
            client_group_id: self.cvr_id.clone(),
            version: version_str,
        };

        let mut deletes = Vec::new();
        let mut inserts = Vec::new();

        for (id, row) in row_updates {
            match row {
                None => {
                    deletes.push(RowKeyRef {
                        schema: id.schema.clone(),
                        table: id.table.clone(),
                        row_key: serde_json::Value::Object(id.row_key.clone()),
                    });
                }
                Some(r) if r.ref_counts.is_none() => {
                    deletes.push(RowKeyRef {
                        schema: r.id.schema.clone(),
                        table: r.id.table.clone(),
                        row_key: serde_json::Value::Object(r.id.row_key.clone()),
                    });
                }
                Some(r) => {
                    inserts.push(row_record_to_rows_row(&self.cvr_id, r));
                }
            }
        }

        let total_count = deletes.len() + inserts.len();

        ExecuteResult::Execute(RowUpdateStatements {
            rows_version,
            deletes,
            inserts,
            total_count,
        })
    }

    /// Mirrors TS `catchupRowPatches(lc, afterVersion, upToCVR, current, excludeQueryHashes)`.
    ///
    /// Returns a `CatchupCursor` that yields pages of 10000 rows.
    /// The transaction is REPEATABLE READ READ ONLY (matching Mode.READONLY).
    /// The transaction lives until the cursor is consumed or dropped.
    pub async fn catchup_row_patches(
        &self,
        after_version: NullableCVRVersion,
        up_to_version: &CVRVersion,
        current: &CVRVersion,
        exclude_query_hashes: &[String],
    ) -> Result<CatchupCursor, sqlx::Error> {
        // Before reading, pending flushes must complete (TS: `await this.flushed(lc)`).
        self.flushed()
            .await
            .map_err(|e| sqlx::Error::Configuration(e.into()))?;

        let start = after_version
            .as_ref()
            .map(version_string)
            .unwrap_or_default();
        let end = version_string(up_to_version);

        // If after >= up_to, nothing to send.
        if crate::version::cmp_versions(&after_version, &Some(up_to_version.clone()))
            != std::cmp::Ordering::Less
        {
            return Ok(CatchupCursor::empty());
        }

        let current_str = version_string(current);

        // Build the SQL.
        let base_select = format!(
            r#"SELECT "clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts"
            FROM "{}"."rows"
        WHERE "clientGroupID" = $1
          AND "patchVersion" > $2
          AND "patchVersion" <= $3"#,
            self.schema
        );

        let sql = if exclude_query_hashes.is_empty() {
            base_select
        } else {
            format!(
                r#"{} AND ("refCounts" IS NULL OR NOT "refCounts" ?| $4)"#,
                base_select
            )
        };

        let use_exclude = !exclude_query_hashes.is_empty();

        // Spawn a task that owns the transaction and streams rows through a channel.
        let (tx, rx) = mpsc::channel::<Result<Vec<RowsRow>, String>>(4);

        let pool = self.pool.clone();
        let schema = self.schema.clone();
        let cvr_id = self.cvr_id.clone();
        let exclude = exclude_query_hashes.to_vec();

        tokio::spawn(catchup_task(CatchupTaskContext {
            pool,
            schema,
            cvr_id,
            start,
            end,
            current_str,
            sql,
            use_exclude,
            exclude_query_hashes: exclude,
            page_sender: tx,
        }));

        Ok(CatchupCursor { rx })
    }
}

/// A streaming cursor for `catchupRowPatches`. Yields pages of up to 10000 rows.
pub struct CatchupCursor {
    rx: mpsc::Receiver<Result<Vec<RowsRow>, String>>,
}

impl CatchupCursor {
    fn empty() -> Self {
        let (_tx, rx) = mpsc::channel(1);
        CatchupCursor { rx }
    }

    /// Pulls the next page of rows. Returns `None` when the stream is exhausted.
    pub async fn next_page(&mut self) -> Result<Option<Vec<RowsRow>>, String> {
        match self.rx.recv().await {
            None => Ok(None),
            Some(Ok(rows)) => Ok(Some(rows)),
            Some(Err(e)) => Err(e),
        }
    }
}

/// The background flush loop. Runs until `pending_rows_version == flushed_rows_version`.
///
/// Mirrors TS `#flush()`:
/// ```text
/// while (pendingRowsVersion !== flushedRowsVersion) {
///   begin tx (READ COMMITTED)
///   executeRowUpdates(tx, pendingRowsVersion, pending, 'force')
///   clear pending
///   commit
///   flushedRowsVersion = pendingRowsVersion
/// }
/// ```
struct FlushLoopContext {
    state: Arc<TokioMutex<CacheState>>,
    pool: sqlx::PgPool,
    schema: String,
    cvr_id: String,
    fail_service: FailCallback,
    metrics_callback: Option<MetricsCallback>,
    flushed_tx: watch::Sender<Option<CVRVersion>>,
    is_flushing: Arc<AtomicBool>,
}

async fn flush_loop(context: FlushLoopContext) {
    let FlushLoopContext {
        state,
        pool,
        schema,
        cvr_id,
        fail_service,
        metrics_callback,
        flushed_tx,
        is_flushing,
    } = context;
    loop {
        let (pending_clone, pending_version) = {
            let mut state = state.lock().await;
            if state.pending_rows_version == state.flushed_rows_version {
                // Caught up — done.
                state.flushing = false;
                is_flushing.store(false, Ordering::SeqCst);
                return;
            }
            (state.pending.clone(), state.pending_rows_version.clone())
        };

        let version = match &pending_version {
            Some(v) => v.clone(),
            None => {
                let mut state = state.lock().await;
                state.flushing = false;
                return;
            }
        };

        let start = std::time::Instant::now();
        let rows_count = pending_clone.len();

        match flush_one_iteration(&pool, &schema, &cvr_id, &version, &pending_clone).await {
            Ok(()) => {
                let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

                // Update state: clear pending, advance flushed version.
                let mut state = state.lock().await;
                state.pending.clear();
                state.flushed_rows_version = Some(version.clone());
                drop(state);

                // Notify watchers.
                let _ = flushed_tx.send(Some(version.clone()));

                if let Some(cb) = &metrics_callback {
                    cb(rows_count, elapsed_ms);
                }
            }
            Err(e) => {
                let err_msg = format!("row record flush failed: {}", e);
                (fail_service)(err_msg.clone());
                let last = {
                    let mut state = state.lock().await;
                    state.flush_error = Some(err_msg);
                    state.flushing = false;
                    state.flushed_rows_version.clone()
                };
                is_flushing.store(false, Ordering::SeqCst);
                // Wake any `flushed()` awaiters so they observe `flush_error`
                // and return an error instead of blocking forever. `watch::send`
                // always marks the value changed, even if it is unchanged.
                let _ = flushed_tx.send(last);
                return;
            }
        }
    }
}

/// Executes one flush iteration: begin tx, upsert rowsVersion, apply row updates, commit.
async fn flush_one_iteration(
    pool: &sqlx::PgPool,
    schema: &str,
    cvr_id: &str,
    version: &CVRVersion,
    pending: &HashMap<String, (RowID, Option<RowRecord>)>,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    // SET LOCAL statement_timeout = 0 (matches run-transaction.ts).
    sqlx::query("SET LOCAL statement_timeout = 0")
        .execute(&mut *tx)
        .await?;
    // SET LOCAL idle_in_transaction_session_timeout = 60000.
    sqlx::query(&format!(
        "SET LOCAL idle_in_transaction_session_timeout = {}",
        IDLE_TX_TIMEOUT_MS
    ))
    .execute(&mut *tx)
    .await?;

    let version_str = version_string(version);

    // 1. Upsert rowsVersion.
    let rows_version_sql = format!(
        r#"INSERT INTO "{}"."rowsVersion" ("clientGroupID", "version") VALUES ($1, $2)
           ON CONFLICT ("clientGroupID")
           DO UPDATE SET "clientGroupID" = $1, "version" = $2"#,
        schema
    );
    sqlx::query(&rows_version_sql)
        .bind(cvr_id)
        .bind(&version_str)
        .execute(&mut *tx)
        .await?;

    // 2. Process deletes and inserts.
    let mut inserts: Vec<RowsRow> = Vec::new();

    for (id, row) in pending.values() {
        match row {
            None => {
                let delete_sql = format!(
                    r#"DELETE FROM "{}"."rows"
                    WHERE "clientGroupID" = $1
                      AND "schema" = $2
                      AND "table" = $3
                      AND "rowKey" = $4"#,
                    schema
                );
                sqlx::query(&delete_sql)
                    .bind(cvr_id)
                    .bind(&id.schema)
                    .bind(&id.table)
                    .bind(serde_json::Value::Object(id.row_key.clone()))
                    .execute(&mut *tx)
                    .await?;
            }
            Some(r) if r.ref_counts.is_none() => {
                let delete_sql = format!(
                    r#"DELETE FROM "{}"."rows"
                    WHERE "clientGroupID" = $1
                      AND "schema" = $2
                      AND "table" = $3
                      AND "rowKey" = $4"#,
                    schema
                );
                sqlx::query(&delete_sql)
                    .bind(cvr_id)
                    .bind(&r.id.schema)
                    .bind(&r.id.table)
                    .bind(serde_json::Value::Object(r.id.row_key.clone()))
                    .execute(&mut *tx)
                    .await?;
            }
            Some(r) => {
                inserts.push(row_record_to_rows_row(cvr_id, r));
            }
        }
    }

    // 3. Bulk insert via json_to_recordset (matches TS exactly).
    if !inserts.is_empty() {
        let inserts_json =
            serde_json::to_value(&inserts).unwrap_or(serde_json::Value::Array(vec![]));
        let bulk_sql = format!(
            r#"INSERT INTO "{}"."rows"(
      "clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts"
  ) SELECT
      "clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts"
    FROM json_to_recordset($1::json) AS x(
      "clientGroupID" TEXT,
      "schema" TEXT,
      "table" TEXT,
      "rowKey" JSONB,
      "rowVersion" TEXT,
      "patchVersion" TEXT,
      "refCounts" JSONB
  ) ON CONFLICT ("clientGroupID", "schema", "table", "rowKey")
    DO UPDATE SET "rowVersion" = excluded."rowVersion",
      "patchVersion" = excluded."patchVersion",
      "refCounts" = excluded."refCounts""#,
            schema
        );
        sqlx::query(&bulk_sql)
            .bind(inserts_json)
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;
    Ok(())
}

/// The catchup streaming task. Owns the transaction and sends pages through a channel.
struct CatchupTaskContext {
    pool: sqlx::PgPool,
    schema: String,
    cvr_id: String,
    start: String,
    end: String,
    current_str: String,
    sql: String,
    use_exclude: bool,
    exclude_query_hashes: Vec<String>,
    page_sender: mpsc::Sender<Result<Vec<RowsRow>, String>>,
}

async fn catchup_task(context: CatchupTaskContext) {
    let result = catchup_task_inner(&context).await;

    if let Err(e) = result {
        let _ = context.page_sender.send(Err(e)).await;
    }
    // Sender drops here — signals "done" to the receiver.
}

async fn catchup_task_inner(context: &CatchupTaskContext) -> Result<(), String> {
    let CatchupTaskContext {
        pool,
        schema,
        cvr_id,
        start,
        end,
        current_str,
        sql,
        use_exclude,
        exclude_query_hashes,
        page_sender,
    } = context;
    // Begin READ ONLY transaction (matches Mode.READONLY = REPEATABLE READ READ ONLY).
    let mut tx = pool
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .map_err(|e| format!("begin tx: {}", e))?;

    // SET LOCAL (matches run-transaction.ts).
    sqlx::query("SET LOCAL statement_timeout = 0")
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("set statement_timeout: {}", e))?;
    sqlx::query(&format!(
        "SET LOCAL idle_in_transaction_session_timeout = {}",
        IDLE_TX_TIMEOUT_MS
    ))
    .execute(&mut *tx)
    .await
    .map_err(|e| format!("set idle timeout: {}", e))?;

    // checkVersion: verify the CVR version matches.
    let check_sql = format!(
        r#"SELECT version FROM "{}".instances WHERE "clientGroupID" = $1"#,
        schema
    );
    let version_row: Option<(String,)> = sqlx::query_as(&check_sql)
        .bind(cvr_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| format!("checkVersion query: {}", e))?;

    let actual_version = version_row
        .map(|(v,)| v)
        .unwrap_or_else(|| crate::version::EMPTY_CVR_VERSION.state_version.clone());

    if actual_version != *current_str {
        // Version mismatch — abort (matches TS checkVersion throwing CVRVersionMismatch).
        let _ = tx.rollback().await;
        return Err(format!(
            "CVR version mismatch: expected {}, got {}",
            current_str, actual_version
        ));
    }

    // Stream rows in pages of CATCHUP_PAGE_SIZE.
    let mut query_builder = sqlx::query_as::<_, RowsRowDb>(sql)
        .bind(cvr_id)
        .bind(start)
        .bind(end);
    if *use_exclude {
        let arr: Vec<String> = exclude_query_hashes.to_vec();
        query_builder = query_builder.bind(arr);
    }

    let mut stream = query_builder.fetch(&mut *tx);
    let mut chunk: Vec<RowsRow> = Vec::with_capacity(CATCHUP_PAGE_SIZE);
    let mut stream_error: Option<String> = None;

    while let Some(row_result) = stream.next().await {
        match row_result {
            Ok(db_row) => {
                chunk.push(db_row.into());
                if chunk.len() >= CATCHUP_PAGE_SIZE
                    && page_sender
                        .send(Ok(std::mem::take(&mut chunk)))
                        .await
                        .is_err()
                {
                    // Consumer dropped — abort.
                    drop(stream);
                    let _ = tx.rollback().await;
                    return Ok(());
                }
            }
            Err(e) => {
                stream_error = Some(format!("fetch row: {}", e));
                break;
            }
        }
    }

    // Drop the stream to release the borrow on tx.
    drop(stream);

    // Send any remaining rows.
    if !chunk.is_empty() {
        let _ = page_sender.send(Ok(chunk)).await;
    }

    if let Some(e) = stream_error {
        let _ = tx.rollback().await;
        return Err(e);
    }

    // Commit (doesn't matter for READ ONLY, but matches TS reader.setDone()).
    tx.commit().await.map_err(|e| format!("commit tx: {e}"))?;

    Ok(())
}

// ---- Pure-logic unit tests (no DB required) ----

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(schema: &str, table: &str, key_val: i64, version: &str) -> RowRecord {
        let mut row_key = serde_json::Map::new();
        row_key.insert("id".to_string(), serde_json::Value::Number(key_val.into()));
        RowRecord {
            id: RowID {
                schema: schema.to_string(),
                table: table.to_string(),
                row_key,
            },
            row_version: version.to_string(),
            patch_version: CVRVersion {
                state_version: "01".to_string(),
                config_version: None,
            },
            ref_counts: Some(std::collections::BTreeMap::from([("q1".to_string(), 1)])),
        }
    }

    #[test]
    fn test_rows_row_to_row_record_basic() {
        let rows_row = RowsRow {
            client_group_id: "cg1".to_string(),
            schema: "public".to_string(),
            table: "users".to_string(),
            row_key: serde_json::json!({"id": 42}),
            row_version: "v1".to_string(),
            patch_version: "01".to_string(),
            ref_counts: Some(serde_json::json!({"q1": 1})),
        };
        let record = rows_row_to_row_record(&rows_row).unwrap();
        assert_eq!(record.id.schema, "public");
        assert_eq!(record.id.table, "users");
        assert_eq!(record.row_version, "v1");
        assert_eq!(record.patch_version.state_version, "01");
        assert_eq!(
            record.ref_counts,
            Some(std::collections::BTreeMap::from([("q1".to_string(), 1)]))
        );
    }

    #[test]
    fn test_row_record_to_rows_row_roundtrip() {
        let record = make_record("public", "users", 42, "v1");
        let rows_row = row_record_to_rows_row("cg1", &record);
        assert_eq!(rows_row.client_group_id, "cg1");
        assert_eq!(rows_row.schema, "public");
        assert_eq!(rows_row.table, "users");
        assert_eq!(rows_row.row_version, "v1");
        assert_eq!(rows_row.patch_version, "01");
        assert_eq!(rows_row.ref_counts, Some(serde_json::json!({"q1": 1})));
    }

    #[test]
    fn test_rows_row_to_record_null_refcounts() {
        let rows_row = RowsRow {
            client_group_id: "cg1".to_string(),
            schema: "public".to_string(),
            table: "users".to_string(),
            row_key: serde_json::json!({"id": 42}),
            row_version: "v1".to_string(),
            patch_version: "01".to_string(),
            ref_counts: None,
        };
        let record = rows_row_to_row_record(&rows_row).unwrap();
        assert_eq!(record.ref_counts, None);
    }

    #[test]
    fn test_rows_row_to_record_malformed_is_err_not_panic() {
        let base = || RowsRow {
            client_group_id: "cg1".to_string(),
            schema: "public".to_string(),
            table: "users".to_string(),
            row_key: serde_json::json!({"id": 42}),
            row_version: "v1".to_string(),
            patch_version: "01".to_string(),
            ref_counts: None,
        };
        // rowKey not an object
        let mut r = base();
        r.row_key = serde_json::json!("not-an-object");
        assert!(matches!(
            rows_row_to_row_record(&r),
            Err(RowRecordError::RowKeyNotObject(_))
        ));
        // refCounts not an object
        let mut r = base();
        r.ref_counts = Some(serde_json::json!(5));
        assert!(matches!(
            rows_row_to_row_record(&r),
            Err(RowRecordError::RefCountsNotObject(_))
        ));
        // refCount value not an integer
        let mut r = base();
        r.ref_counts = Some(serde_json::json!({"q1": "x"}));
        assert!(matches!(
            rows_row_to_row_record(&r),
            Err(RowRecordError::RefCountNotInteger(_))
        ));
        // malformed patchVersion (>2 colon parts)
        let mut r = base();
        r.patch_version = "a:b:c".to_string();
        assert!(matches!(
            rows_row_to_row_record(&r),
            Err(RowRecordError::Version(_))
        ));
    }

    #[test]
    fn test_execute_result_serde_tag() {
        let defer = ExecuteResult::Defer;
        let json = serde_json::to_string(&defer).unwrap();
        assert_eq!(json, r#"{"type":"defer"}"#);

        let stmts = ExecuteResult::Execute(RowUpdateStatements {
            rows_version: RowsVersionRow {
                client_group_id: "cg1".to_string(),
                version: "01".to_string(),
            },
            deletes: vec![],
            inserts: vec![],
            total_count: 0,
        });
        let json = serde_json::to_string(&stmts).unwrap();
        assert!(json.contains(r#""type":"execute""#));
    }

    #[test]
    fn test_flush_mode_equality() {
        assert_eq!(FlushMode::AllowDefer, FlushMode::AllowDefer);
        assert_ne!(FlushMode::AllowDefer, FlushMode::Force);
    }

    /// Build a cache backed by a lazy (never-connecting) pool whose in-memory
    /// cache is already marked loaded, so `apply(flushed=true)` runs without PG.
    fn loaded_cache_for_test() -> RowRecordCache {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://u:p@127.0.0.1:1/db")
            .expect("lazy pool");
        let cache = RowRecordCache::new(
            pool,
            "s".to_string(),
            "cvr1".to_string(),
            100,
            Arc::new(|_| {}),
            None,
        );
        cache.state.try_lock().unwrap().cache = Some(Arc::new(HashMap::new()));
        cache
    }

    /// Regression: `apply(flushed=true)` must not deadlock. The `flushed=true`
    /// write-back path (used on every hydrate) does NOT spawn a background flush,
    /// so it must not fall through to a second `self.state.lock().await` while the
    /// first guard is still held — the tokio `Mutex` is not reentrant and that
    /// would hang the CG thread forever (poke opens, never completes). Only the
    /// `flushed=false` path dropped the guard, so this went unnoticed until a live
    /// hydrate. Guarded by a timeout so a re-introduced re-lock fails, not hangs.
    #[tokio::test]
    async fn apply_flushed_true_does_not_deadlock() {
        let cache = loaded_cache_for_test();
        let deltas = vec![(
            make_record("public", "label", 1, "v1").id,
            Some(make_record("public", "label", 1, "v1")),
        )];
        let res = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            cache.apply(deltas, CVRVersion::empty(), true),
        )
        .await;
        assert!(
            res.is_ok(),
            "apply(flushed=true) deadlocked — it re-locked the non-reentrant state Mutex"
        );
        assert_eq!(
            res.unwrap().unwrap(),
            1,
            "the applied row should be in cache"
        );
    }
}
