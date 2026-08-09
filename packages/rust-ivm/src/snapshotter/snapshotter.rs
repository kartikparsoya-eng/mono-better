//! Snapshotter — owns the replica DB snapshot lifecycle.
//!
//! Port of TS `Snapshotter` + `Snapshot` (snapshotter.ts).
//! The graph and snapshot connections remain confined to the engine actor.
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
    /// Interrupt handles for the live snapshot connections (`prev` + `curr`).
    snapshot_interrupt_registry: Option<Arc<Mutex<Vec<rusqlite::InterruptHandle>>>>,
}

impl Snapshotter {
    pub fn new(db_file: &str, app_id: &str, page_cache_size_kib: Option<i64>) -> Self {
        Snapshotter {
            db_file: db_file.to_string(),
            app_id: app_id.to_string(),
            page_cache_size_kib,
            curr: None,
            prev: None,
            destroyed: false,
            snapshot_interrupt_registry: None,
        }
    }

    /// Install the out-of-band interrupt registry used by the NAPI owner.
    /// The registry is republished after every snapshot swap so cancel always
    /// targets the connections that TableSource and diff iteration actually use.
    pub fn set_snapshot_interrupt_registry(
        &mut self,
        registry: Arc<Mutex<Vec<rusqlite::InterruptHandle>>>,
    ) {
        self.snapshot_interrupt_registry = Some(registry);
        self.publish_snapshot_interrupt_handles();
    }

    fn publish_snapshot_interrupt_handles(&self) {
        let Some(registry) = &self.snapshot_interrupt_registry else {
            return;
        };
        let mut handles = Vec::with_capacity(2);
        if let Some(prev) = &self.prev {
            handles.push(crate::sqlite::install_interrupt(&prev.conn.borrow()));
        }
        if let Some(curr) = &self.curr {
            handles.push(crate::sqlite::install_interrupt(&curr.conn.borrow()));
        }
        *registry.lock().unwrap() = handles;
    }

    /// Pin the initial snapshot at the current head. Must be called exactly once.
    pub fn init(&mut self) -> Result<(), String> {
        if self.curr.is_some() {
            return Err("Already initialized".to_string());
        }
        let snap = Snapshot::create(&self.db_file, self.page_cache_size_kib)?;
        self.curr = Some(snap);
        self.publish_snapshot_interrupt_handles();
        Ok(())
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

    /// Advance to head, returning a diff between the previous snapshot and
    /// a new snapshot at head. The diff is only valid until the next advance.
    pub fn advance(
        &mut self,
        syncable_tables: &HashMap<String, LiteAndZqlSpec>,
        all_table_names: &HashSet<String>,
    ) -> Result<DiffOwned, String> {
        if self.curr.is_none() {
            return Err("Snapshotter has not been initialized".to_string());
        }

        // Match TS Snapshotter.advanceWithoutDiff exactly: after the first
        // advance, roll the older connection back and re-pin it at head. The
        // two connections continually leapfrog; do not allocate a new snapshot
        // connection on every advance.
        let next = {
            let _t = crate::perf_trace::scope("snapshot.repin");
            match self.prev.take() {
                Some(mut prev) => {
                    prev.reset_to_head()?;
                    prev
                }
                None => Snapshot::create(&self.db_file, self.page_cache_size_kib)?,
            }
        };

        self.prev = self.curr.take();
        self.curr = Some(next);
        self.publish_snapshot_interrupt_handles();

        // TS constructs Diff after the swap, so a count failure is observable
        // as an advance error after the snapshot lifecycle has moved forward.
        let prev_version_for_diff = self.prev.as_ref().unwrap().version.clone();
        let curr = self.curr.as_ref().unwrap();
        let curr_version_for_diff = curr.version.clone();
        let change_count = {
            let _t = crate::perf_trace::scope("advance.count");
            curr.num_changes_since(&prev_version_for_diff)?
        };

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
    pub fn advance_without_diff(&mut self) -> Result<&str, String> {
        if self.curr.is_none() {
            return Err("Snapshotter has not been initialized".to_string());
        }
        let next = {
            let _t = crate::perf_trace::scope("snapshot.repin");
            match self.prev.take() {
                Some(mut prev) => {
                    prev.reset_to_head()?;
                    prev
                }
                None => Snapshot::create(&self.db_file, self.page_cache_size_kib)?,
            }
        };
        self.prev = self.curr.take();
        self.curr = Some(next);
        self.publish_snapshot_interrupt_handles();
        Ok(&self.curr.as_ref().unwrap().version)
    }

    /// Close both snapshot connections.
    pub fn destroy(&mut self) {
        self.destroyed = true;
        self.curr.take();
        self.prev.take();
        if let Some(registry) = &self.snapshot_interrupt_registry {
            registry.lock().unwrap().clear();
        }
    }
}

/// A single pinned frame plus its stateVersion.
/// Owns ONE `rusqlite::Connection` with an open BEGIN CONCURRENT read tx.
pub struct Snapshot {
    conn: SharedConn,
    version: String,
    /// Whether the replica is in wal2 mode (determined at open).
    is_wal2: bool,
}

/// Reset every live statement on `conn` (an `sqlite3_next_stmt` walk), returning
/// how many were BUSY (stepped, un-reset — i.e. holding an open cursor).
///
/// This is the better-sqlite3 close contract: statements are settled before the
/// connection's transaction state is touched. Without it, a stray cursor makes
/// `sqlite3_close` fail with SQLITE_BUSY — an error rusqlite's `Drop` silently
/// swallows (`InnerConnection::drop`, `#[allow(unused_must_use)]`), leaking the
/// connection at the C level **with its read transaction still open**. On wal2
/// that orphaned read-mark is a permanent checkpoint pin: the CG recovers on a
/// fresh connection and logs healthily while the WAL grows at the write rate
/// (the unbounded prod WAL-growth mechanism; see zombie_pin_repro_test.rs).
///
/// Statements are only reset, never finalized: their Rust owners (if any still
/// exist) hold the `sqlite3_stmt` and will finalize on their own drop —
/// finalizing here would double-free. A reset is sufficient to release the
/// cursor so ROLLBACK fully settles the connection.
fn settle_statements(conn: &rusqlite::Connection) -> usize {
    let mut busy = 0usize;
    unsafe {
        let db = conn.handle();
        let mut stmt = rusqlite::ffi::sqlite3_next_stmt(db, std::ptr::null_mut());
        while !stmt.is_null() {
            if rusqlite::ffi::sqlite3_stmt_busy(stmt) != 0 {
                busy += 1;
                rusqlite::ffi::sqlite3_reset(stmt);
            }
            stmt = rusqlite::ffi::sqlite3_next_stmt(db, stmt);
        }
    }
    busy
}

impl Drop for Snapshot {
    /// Every drop path — leapfrog-failure orphan, `Snapshotter::destroy()`,
    /// error unwind — must release the read-mark. The pin is the open
    /// transaction, not the connection handle: even if `sqlite3_close` later
    /// fails on an unfinalized statement and rusqlite leaks the handle, a
    /// rolled-back connection holds no read-mark and cannot block checkpoints.
    fn drop(&mut self) {
        let conn = self.conn.borrow();
        // Finalize cached statements FIRST: rusqlite's `Connection` drops its
        // InnerConnection (close) BEFORE its StatementCache, so un-flushed
        // cached statements would make sqlite3_close fail and leak the handle.
        conn.flush_prepared_statement_cache();
        let busy = settle_statements(&conn);
        if busy > 0 {
            eprintln!(
                "[rust-ivm] snapshot drop: settled {} busy statement(s) that outlived \
                 snapshot version {:?} — the connection handle may leak on close, but \
                 the read-mark is released (no checkpoint pin)",
                busy, self.version,
            );
        }
        if let Err(e) = conn.execute_batch("ROLLBACK") {
            let msg = e.to_string();
            // "no transaction is active" is the normal case for a snapshot that
            // was never pinned (create-failure unwind) — not worth logging.
            if !msg.contains("no transaction is active") {
                eprintln!(
                    "[rust-ivm] snapshot drop: ROLLBACK failed for version {:?}: {} — \
                     if the close below also fails, this connection is a zombie \
                     checkpoint pin (unbounded WAL growth)",
                    self.version, msg,
                );
            }
        }
        drop(conn);

        // Close LOUDLY. rusqlite's Drop calls sqlite3_close and swallows the
        // error; a failed close (any live statement) silently leaks the entire
        // handle — ~11.5MB of schema + sqlite_stat4 decode + page cache, plus
        // the fds — per occurrence. When this Snapshot is the sole holder
        // (engine/sources drop before the snapshotter in EngineState, so this
        // is the normal teardown order), close explicitly and report failure.
        // Otherwise report the outstanding holders: their eventual implicit
        // close is unobservable, which is exactly the leak-risk to surface.
        let holders = Rc::strong_count(&self.conn);
        if holders > 1 {
            eprintln!(
                "[rust-ivm] snapshot drop: {} outstanding conn holder(s) at drop \
                 (version {:?}); close defers to the last holder and any close \
                 failure there is SILENT — leaked-handle risk",
                holders - 1,
                self.version,
            );
            return;
        }
        // Sole owner: swap in a throwaway in-memory conn so the real one can
        // be moved out and closed with an observable result. If the dummy
        // can't be opened (never in practice), fall back to the silent drop.
        if let Ok(dummy) = rusqlite::Connection::open_in_memory() {
            let rc = std::mem::replace(&mut self.conn, Rc::new(RefCell::new(dummy)));
            if let Ok(cell) = Rc::try_unwrap(rc)
                && let Err((leaked, e)) = cell.into_inner().close()
            {
                eprintln!(
                    "[rust-ivm] snapshot close FAILED for version {:?}: {} — \
                     sqlite handle leaked (schema/stat4/page-cache retained)",
                    self.version, e,
                );
                drop(leaked);
            }
        }
    }
}

impl Snapshot {
    /// Open a fresh connection and pin it at the current head.
    fn create(db_file: &str, page_cache_size_kib: Option<i64>) -> Result<Self, String> {
        let _t = crate::perf_trace::scope("snapshot.begin");
        // Open read-write so wal2 can register a checkpoint-blocking read-mark
        // in -shm via BEGIN CONCURRENT. A read-only open cannot write -shm, so the
        // checkpointer IGNORES it and recycles needed frames under the pinned
        // snapshot → torn read → InvalidDiff teardown one advance later. TS opens
        // rw + beginConcurrent and has NO read-only fallback; we match that.
        //
        // TS performs one synchronous Database open and propagates failure.
        // rusqlite::open uses a read-write connection (and creates a missing
        // file), matching the native TS driver's default open mode.
        let conn = rusqlite::Connection::open(db_file)
            .map_err(|e| format!("Snapshot::create open: {}", e))?;

        // Statement cache for the fetch hot path (TS parity: zqlite's
        // StatementCache). Sized above rusqlite's default 16 so a multi-query
        // CG's distinct fetch shapes don't thrash it.
        conn.set_prepared_statement_cache_capacity(64);

        conn.pragma_update(None, "synchronous", "OFF")
            .map_err(|e| format!("pragma synchronous: {}", e))?;
        conn.pragma_update(None, "case_sensitive_like", "ON")
            .map_err(|e| format!("pragma case_sensitive_like: {}", e))?;
        if let Some(cache_kib) = page_cache_size_kib {
            conn.pragma_update(None, "cache_size", -(cache_kib))
                .map_err(|e| format!("pragma cache_size: {}", e))?;
        }

        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .map_err(|e| format!("pragma journal_mode: {}", e))?;
        let is_wal2 = mode.eq_ignore_ascii_case("wal2");
        if !is_wal2 && !cfg!(feature = "non-wal2-test-support") {
            return Err(format!(
                "replica db must be in wal2 mode (current: {})",
                mode
            ));
        }

        let mut snap = Snapshot {
            conn: Rc::new(RefCell::new(conn)),
            version: String::new(),
            is_wal2,
        };
        snap.begin_and_pin()?;
        Ok(snap)
    }

    /// BEGIN CONCURRENT then read `_zero.replicationState` to acquire the read lock.
    /// The read is mandatory — BEGIN CONCURRENT alone does NOT create the snapshot.
    fn begin_and_pin(&mut self) -> Result<(), String> {
        // BEGIN CONCURRENT on wal2 registers a read-mark in -shm that the
        // checkpointer respects. On plain WAL (tests), BEGIN creates a
        // deferred read snapshot. Both are followed by the mandatory
        // replicationState read that pins the snapshot frame.
        let sql = if self.is_wal2 {
            "BEGIN CONCURRENT"
        } else {
            "BEGIN"
        };
        self.conn
            .borrow()
            .execute_batch(sql)
            .map_err(|e| format!("{}: {}", sql, e))?;

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

    /// Roll this connection's snapshot back and re-pin it at the current head.
    /// This is the Rust equivalent of TS `Snapshot.resetToHead()`.
    fn reset_to_head(&mut self) -> Result<(), String> {
        // A busy statement here means a cursor outlived the advance that
        // created it — a bug in whoever stashed it, but never a reason to
        // orphan this connection (the zombie-pin class). Settle it, loudly.
        let busy = settle_statements(&self.conn.borrow());
        if busy > 0 {
            eprintln!(
                "[rust-ivm] snapshot leapfrog: settled {} busy statement(s) still \
                 open on snapshot version {:?} — a cursor outlived its advance",
                busy, self.version,
            );
        }
        self.conn
            .borrow()
            .execute_batch("ROLLBACK")
            .map_err(|e| format!("ROLLBACK: {}", e))?;
        self.begin_and_pin()
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
