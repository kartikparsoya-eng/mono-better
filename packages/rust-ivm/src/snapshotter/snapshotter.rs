//! Snapshotter — owns the replica DB snapshot lifecycle.
//!
//! Port of TS `Snapshotter` + `Snapshot` (snapshotter.ts).
//! Rewritten to be `Send` — uses plain `rusqlite::Connection` (no Rc/RefCell),
//! so the whole Snapshotter can be moved across threads (into a tokio task).
//!
//! The leapfrog model:
//!   Replicator:  t1 ----> t2 ----> t3 ---->
//!   ViewSyncer:   [snap_a] -> [snap_b] -> [snap_c]
//!                  (conn_1)    (conn_2)    (conn_1)  ← reused
//!
//! Each Snapshot holds ONE connection with an open BEGIN CONCURRENT read tx.
//! `advance()` rolls back prev's tx, re-pins at head, swaps prev↔curr.
//! The diff between prev and curr is derived from `_zero.changeLog2`.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use std::sync::{Arc, Mutex};

use crate::snapshotter::diff::Diff;
use crate::snapshotter::read_pool::FramePinnedPool;
use crate::snapshotter::spec::LiteAndZqlSpec;

/// Shared handle to a pinned snapshot connection. The TableSources share these
/// so that, during advance, each source can be pointed at the PREV snapshot
/// while changes are processed and at CURR afterwards — matching TS
/// `pipeline-driver.ts` `table.setDB(curr.db.db)`. Confined to the engine actor
/// thread (like the rest of EngineState), so Rc/RefCell is fine.
pub type SharedConn = Rc<RefCell<rusqlite::Connection>>;

/// A `Snapshotter` manages leapfrogging BEGIN CONCURRENT snapshots on a wal2
/// replica file. Each advance produces a `Diff` derived from `_zero.changeLog2`.
///
/// `Send` — can be moved into a worker thread or tokio task. Not `Sync` —
/// access from one thread at a time (matching Go's `sync.Mutex` serialization).
pub struct Snapshotter {
    db_file: String,
    app_id: String,
    page_cache_size_kib: Option<i64>,
    curr: Option<Snapshot>,
    prev: Option<Snapshot>,
    destroyed: bool,
    /// Frame-pinned read pool for parallel cold hydrate (read-level
    /// parallelism). Co-pinned with `curr` at `init` while `head ==
    /// curr.version` (PipelineCount==0). Unpinned once advancing begins — later
    /// (warm) hydrates fall back to serial. See DESIGN-read-parallelism.md §2b.
    read_pool: Arc<FramePinnedPool>,
    /// How many connections to co-pin (≤ pool capacity).
    pool_pin_count: usize,
    /// Watchdog interrupt registry (N1) — pooled connections register here so a
    /// slow parallel read can be hard-aborted like the actor's own connection.
    interrupt_registry: Option<Arc<Mutex<Vec<rusqlite::InterruptHandle>>>>,
}

impl Snapshotter {
    pub fn new(db_file: &str, app_id: &str, page_cache_size_kib: Option<i64>) -> Self {
        Self::with_read_pool(db_file, app_id, page_cache_size_kib, 0, None)
    }

    /// Construct with an explicit parallel-read pool size and watchdog registry.
    /// `pool_capacity == 0` disables read-level parallelism (serial hydrate).
    pub fn with_read_pool(
        db_file: &str,
        app_id: &str,
        page_cache_size_kib: Option<i64>,
        pool_capacity: usize,
        interrupt_registry: Option<Arc<Mutex<Vec<rusqlite::InterruptHandle>>>>,
    ) -> Self {
        Snapshotter {
            db_file: db_file.to_string(),
            app_id: app_id.to_string(),
            page_cache_size_kib,
            curr: None,
            prev: None,
            destroyed: false,
            read_pool: Arc::new(FramePinnedPool::new(
                db_file,
                page_cache_size_kib,
                pool_capacity.max(1),
            )),
            pool_pin_count: pool_capacity,
            interrupt_registry,
        }
    }

    /// The frame-pinned parallel-read pool (shared with the TableSources).
    pub fn read_pool(&self) -> Arc<FramePinnedPool> {
        self.read_pool.clone()
    }

    /// Pin the initial snapshot at the current head. Must be called exactly once.
    /// Co-pins the parallel-read pool at the same frame (head == curr.version,
    /// PipelineCount==0) so the first cold hydrate can fan its reads out.
    pub fn init(&mut self) -> Result<(), String> {
        if self.curr.is_some() {
            return Err("Already initialized".to_string());
        }
        let snap = Snapshot::create(&self.db_file, self.page_cache_size_kib)?;
        let version = snap.version.clone();
        self.curr = Some(snap);
        self.pin_read_pool(&version);
        Ok(())
    }

    /// Co-pin the read pool at `version` (best-effort: on failure the pool is
    /// left unpinned and hydrate reads fall back to serial — never wrong-frame).
    fn pin_read_pool(&self, version: &str) {
        if self.pool_pin_count == 0 {
            return;
        }
        if let Err(e) = self.read_pool.pin_frame(
            version,
            self.pool_pin_count,
            self.interrupt_registry.as_ref(),
        ) {
            eprintln!("[rust-ivm] read pool co-pin at {} failed (serial hydrate): {}", version, e);
        }
    }

    pub fn initialized(&self) -> bool {
        self.curr.is_some()
    }

    pub fn destroyed(&self) -> bool {
        self.destroyed
    }

    /// Returns the current snapshot's version.
    pub fn current_version(&self) -> Result<&str, String> {
        self.curr
            .as_ref()
            .map(|s| s.version.as_str())
            .ok_or_else(|| "Snapshotter has not been initialized".to_string())
    }

    /// Returns a shared handle to the previous snapshot's connection.
    pub fn prev_conn(&self) -> Result<SharedConn, String> {
        self.prev
            .as_ref()
            .map(|s| s.conn.clone())
            .ok_or_else(|| "No previous snapshot".to_string())
    }

    /// Returns a shared handle to the current snapshot's connection.
    pub fn current_conn(&self) -> Result<SharedConn, String> {
        self.curr
            .as_ref()
            .map(|s| s.conn.clone())
            .ok_or_else(|| "Snapshotter has not been initialized".to_string())
    }

    /// Take the current snapshot's cross-thread interrupt handle (N1).
    /// The snapshot keeps its connection; only the interrupt handle moves out
    /// so the napi layer can register it with the EngineHandle watchdog.
    /// Returns `None` if the snapshotter isn't initialized or the handle was
    /// already taken (e.g. on a leapfrog swap — the new snapshot carries a fresh
    /// handle; call this again after `advance`).
    pub fn take_current_interrupt_handle(&mut self) -> Option<rusqlite::InterruptHandle> {
        self.curr.as_mut().and_then(|s| s.take_interrupt_handle())
    }

    /// Advance to head, returning a diff between the previous snapshot and
    /// a new snapshot at head. The diff is only valid until the next advance.
    ///
    /// FAILURE-ATOMIC: the prev/curr swap commits only after the new head pin
    /// AND the diff count succeed. On error, s.curr is untouched — the caller
    /// may retry advance in place.
    pub fn advance(
        &mut self,
        syncable_tables: &HashMap<String, LiteAndZqlSpec>,
        all_table_names: &HashSet<String>,
    ) -> Result<DiffOwned, String> {
        // Advancing moves curr off its cold-hydrate frame — release the parallel
        // read pool (rollback its read txs so the WAL can checkpoint). Later
        // (warm) hydrates run serially until the next cold re-pin.
        self.read_pool.unpin_frame();
        // Prepare the head pin WITHOUT touching prev/curr (leapfrog core).
        let mut next = if let Some(mut prev_snap) = self.prev.take() {
            // Reuse the prev connection: rollback + re-pin at head.
            prev_snap.reset_to_head()?;
            prev_snap
        } else {
            // First advance: no prev yet, open a fresh second connection.
            Snapshot::create(&self.db_file, self.page_cache_size_kib)?
        };

        // Read the change count BEFORE swapping — only fallible step after pin.
        let prev_version = self
            .curr
            .as_ref()
            .map(|s| s.version.clone())
            .ok_or_else(|| "Snapshotter has not been initialized".to_string())?;

        let change_count = next.num_changes_since(&prev_version)?;

        // Commit the swap: prev = old curr, curr = next (at head).
        let old_curr = self.curr.take().unwrap();
        let prev_version_for_diff = old_curr.version.clone();
        let curr_version_for_diff = next.version.clone();

        // Extract the connections — DiffOwned owns them to prevent use-after-swap.
        let prev_conn = old_curr.conn;
        let prev_interrupt_handle = old_curr.interrupt_handle;
        let curr_conn = next.conn;
        let curr_interrupt_handle = next.interrupt_handle;

        self.prev = Some(Snapshot {
            conn: prev_conn,
            version: prev_version_for_diff.clone(),
            interrupt_handle: prev_interrupt_handle,
        });

        // We need to keep curr's connection for the engine's table sources.
        // But we also need it for the diff. Solution: the diff borrows curr's
        // connection. Since we own it, we store it and lend a reference.
        let curr_snapshot = Snapshot {
            conn: curr_conn,
            version: curr_version_for_diff.clone(),
            interrupt_handle: curr_interrupt_handle,
        };
        self.curr = Some(curr_snapshot);

        Ok(DiffOwned {
            app_id: self.app_id.clone(),
            syncable_tables: syncable_tables.clone(),
            all_table_names: all_table_names.clone(),
            prev_version: prev_version_for_diff,
            curr_version: curr_version_for_diff,
            change_count,
        })
    }

    /// Advance to head WITHOUT computing a diff (matches TS Snapshotter.advanceWithoutDiff()).
    /// Swaps prev/curr so curr is now at head. Returns the new curr version.
    ///
    /// FAILURE-ATOMIC: the swap commits only after the new head pin succeeds.
    /// On error, prev/curr are untouched — the caller may retry in place.
    pub fn advance_without_diff(&mut self) -> Result<&str, String> {
        self.read_pool.unpin_frame();
        let next = if let Some(mut prev_snap) = self.prev.take() {
            prev_snap.reset_to_head()?;
            prev_snap
        } else {
            Snapshot::create(&self.db_file, self.page_cache_size_kib)?
        };

        // Commit the swap: prev = old curr, curr = next at head.
        self.prev = self.curr.take();
        self.curr = Some(next);
        Ok(&self.curr.as_ref().unwrap().version)
    }

    /// Re-pin the current snapshot at the latest replica head on its existing
    /// connection. Called before the first hydrate: init() pins curr at
    /// handleInit time, but the replicator may have advanced by the time
    /// addQueries arrives.
    ///
    /// ONLY safe when no pipeline depends on curr yet (initial hydrate).
    pub fn refresh_current_to_head(&mut self) -> Result<(), String> {
        // Re-pin curr at head (safe only at PipelineCount==0) — and co-pin the
        // read pool at the same fresh frame so this cold hydrate can fan out.
        self.read_pool.unpin_frame();
        if let Some(curr) = self.curr.as_mut() {
            curr.reset_to_head()?;
            let version = curr.version.clone();
            self.pin_read_pool(&version);
            Ok(())
        } else {
            Err("Snapshotter has not been initialized".to_string())
        }
    }

    /// Close both snapshot connections.
    pub fn destroy(&mut self) {
        self.destroyed = true;
        self.read_pool.unpin_frame();
        self.curr.take();
        self.prev.take();
    }

}

/// A single pinned frame plus its stateVersion.
/// Owns ONE `rusqlite::Connection` with an open BEGIN CONCURRENT read tx.
pub struct Snapshot {
    conn: SharedConn,
    version: String,
    /// Cross-thread interrupt handle (N1). `None` only if the snapshot was
    /// constructed bypassing `create` (tests). The napi layer can extract this
    /// to register with the EngineHandle watchdog.
    interrupt_handle: Option<rusqlite::InterruptHandle>,
}

impl Snapshot {
    /// Open a fresh connection and pin it at the current head.
    fn create(db_file: &str, page_cache_size_kib: Option<i64>) -> Result<Self, String> {
        // Open READ-ONLY to avoid lock contention with the zero-cache write-worker's
        // WAL2-aware SQLite. Standard SQLite (rusqlite) doesn't support WAL2 mode,
        // so read-write connections hold incompatible locks that block the
        // write-worker's COMMITs.
        let conn = rusqlite::Connection::open_with_flags(
            db_file,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )
        .map_err(|e| format!("Snapshot::create open: {}", e))?;

        // N1 (DESIGN §1a seam 1): install a cross-thread interrupt handle on
        // every connection open. The snapshot connection is read-only and pinned;
        // its handle lets the watchdog/cancel hard-abort a slow replicationState
        // read or a leapfrog re-pin. Stored on the Snapshot so the napi layer can
        // register it with the EngineHandle watchdog in Phase 1.
        let interrupt_handle = crate::sqlite::install_interrupt(&conn);

        // These pragmas are write operations that fail in read-only mode.
        // They're performance hints, not correctness requirements — ignore errors.
        let _ = conn.pragma_update(None, "synchronous", "OFF");
        let _ = conn.pragma_update(None, "case_sensitive_like", "ON");
        if let Some(cache_kib) = page_cache_size_kib {
            let _ = conn.pragma_update(None, "cache_size", -(cache_kib));
        }

        let _mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .map_err(|e| format!("pragma journal_mode: {}", e))?;

        let mut snap = Snapshot {
            conn: Rc::new(RefCell::new(conn)),
            version: String::new(),
            interrupt_handle: Some(interrupt_handle),
        };
        snap.begin_and_pin()?;
        Ok(snap)
    }

    /// BEGIN CONCURRENT then read `_zero.replicationState` to acquire the read lock.
    /// The read is mandatory — BEGIN CONCURRENT alone does NOT create the snapshot.
    fn begin_and_pin(&mut self) -> Result<(), String> {
        // Use BEGIN instead of BEGIN CONCURRENT — the latter is a WAL2 write
        // operation that requires write access. In read-only mode, BEGIN creates
        // a deferred transaction; the subsequent read of replicationState pins
        // the snapshot at the current head. In WAL/WAL2 mode, this read snapshot
        // does not block the write-worker.
        self.conn
            .borrow()
            .execute_batch("BEGIN")
            .map_err(|e| format!("BEGIN: {}", e))?;

        let version: String = self
            .conn
            .borrow()
            .query_row(
                "SELECT stateVersion FROM \"_zero.replicationState\"",
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("read replicationState: {}", e))?;

        self.version = version;
        Ok(())
    }

    /// Get the stateVersion this frame is pinned at.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Get a clonable shared handle to the underlying pinned connection.
    pub fn conn(&self) -> SharedConn {
        self.conn.clone()
    }

    /// Take the cross-thread interrupt handle (N1). `None` if already taken
    /// or if the snapshot was constructed bypassing `create` (tests). The napi
    /// layer takes this to register with the EngineHandle watchdog.
    pub fn take_interrupt_handle(&mut self) -> Option<rusqlite::InterruptHandle> {
        self.interrupt_handle.take()
    }

    /// Count change-log entries with stateVersion > prevVersion.
    pub fn num_changes_since(&self, prev_version: &str) -> Result<i64, String> {
        let count: i64 = self
            .conn
            .borrow()
            .query_row(
                "SELECT COUNT(*) FROM \"_zero.changeLog2\" WHERE stateVersion > ?",
                [prev_version],
                |row| row.get(0),
            )
            .map_err(|e| format!("numChangesSince: {}", e))?;
        Ok(count)
    }

    /// End this snapshot's tx and re-pin the same connection at the new head.
    fn reset_to_head(&mut self) -> Result<(), String> {
        self.conn
            .borrow()
            .execute_batch("ROLLBACK")
            .map_err(|e| format!("resetToHead ROLLBACK: {}", e))?;

        self.conn
            .borrow()
            .execute_batch("BEGIN")
            .map_err(|e| format!("resetToHead BEGIN: {}", e))?;

        let version: String = self
            .conn
            .borrow()
            .query_row(
                "SELECT stateVersion FROM \"_zero.replicationState\"",
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("resetToHead read replicationState: {}", e))?;

        self.version = version;
        Ok(())
    }
}

/// Owned diff data — the caller iterates this to produce Changes.
/// Unlike the TS/Go version which borrows snapshot connections, this version
/// reads the changelog eagerly and resolves rows on demand via the snapshotter's
/// connections. The caller must hold the Snapshotter alive while iterating.
///
/// In practice, the engine calls `snapshotter.advance()`, gets the DiffOwned,
/// then calls `snapshotter.iterate_diff(&diff_owned)` which borrows both
/// connections. This is safe because advance() is not called during iteration.
#[derive(Clone)]
pub struct DiffOwned {
    pub app_id: String,
    pub syncable_tables: HashMap<String, LiteAndZqlSpec>,
    pub all_table_names: HashSet<String>,
    pub prev_version: String,
    pub curr_version: String,
    pub change_count: i64,
}

impl DiffOwned {
    pub fn changes(&self) -> i64 {
        self.change_count
    }

    pub fn prev_version(&self) -> &str {
        &self.prev_version
    }

    pub fn curr_version(&self) -> &str {
        &self.curr_version
    }
}

/// A change from the diff — table, prev rows to remove, next row to add.
#[derive(Debug, Clone)]
pub struct SnapshotChange {
    pub table: String,
    pub prev_values: Vec<HashMap<String, rusqlite::types::Value>>,
    pub next_value: Option<HashMap<String, rusqlite::types::Value>>,
    pub row_key: HashMap<String, rusqlite::types::Value>,
}

/// Reasons for a pipeline reset during diff iteration.
pub const REASON_SCHEMA_CHANGE: &str = "schema-change";
pub const REASON_TRUNCATION: &str = "truncation";
pub const REASON_PERMISSIONS_CHANGE: &str = "permissions-change";

/// A reset signal — aborts diff iteration and tells the caller to re-hydrate.
/// Port of TS `ResetPipelinesSignal` (snapshotter.ts:262).
#[derive(Debug)]
pub struct ResetPipelinesSignal {
    pub reason: &'static str,
    pub msg: String,
}

impl std::fmt::Display for ResetPipelinesSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for ResetPipelinesSignal {}

/// Error when a diff is consumed after its snapshots have advanced.
#[derive(Debug)]
pub struct InvalidDiffError {
    pub msg: String,
}

impl std::fmt::Display for InvalidDiffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for InvalidDiffError {}
