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
    /// Interrupt handles for the live snapshot connections (`prev` + `curr`).
    /// Kept separate from the read-pool registry because the pool tracks a
    /// contiguous range that it owns and drains on unpin.
    snapshot_interrupt_registry: Option<Arc<Mutex<Vec<rusqlite::InterruptHandle>>>>,
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
                interrupt_registry,
            )),
            pool_pin_count: pool_capacity,
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
        self.publish_snapshot_interrupt_handles();
        self.pin_read_pool(&version);
        Ok(())
    }

    /// Co-pin the read pool at `version` (best-effort: on failure the pool is
    /// left unpinned and hydrate reads fall back to serial — never wrong-frame).
    fn pin_read_pool(&self, version: &str) {
        if self.pool_pin_count == 0 {
            return;
        }
        // Anchor the pool on `curr`'s live snapshot connection so (under the
        // wal2_coread feature) the pool co-reads at curr's frame, sharing its
        // read-mark instead of each pool conn claiming its own (which exhausts
        // wal2's fixed read-mark slots under churn → prev-snapshot slips).
        // Safe to borrow: pin_read_pool runs at init / refresh_current_to_head,
        // i.e. PipelineCount==0, when no source is borrowing curr.
        let anchor = self.curr.as_ref().map(|s| s.conn.borrow());
        if let Err(e) = self
            .read_pool
            .pin_frame(anchor.as_deref(), version, self.pool_pin_count)
        {
            eprintln!(
                "[rust-ivm] read pool co-pin at {} failed (serial hydrate): {}",
                version, e
            );
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
        // Leapfrog WITHOUT a read-mark gap (match TS — TS never tears). Open the
        // new head snapshot on a FRESH connection: its BEGIN CONCURRENT locks the
        // head frames while every other reader's mark is still held. Reusing the
        // prev connection via reset_to_head does ROLLBACK-in-place, dropping its
        // read-mark between ROLLBACK and the re-lock — under drive-mode ramp the
        // checkpointer recycles frames in that window, poisoning the snapshot,
        // which later tears in get_rows ("database disk image is malformed"). A
        // fresh connection never drops a live mark, so no frame is ever
        // unprotected. (See DESIGN-wal2-snapshot-isolation.md §2.)
        let next = Snapshot::create(&self.db_file, self.page_cache_size_kib)?;
        // Release the stale prev connection now that `next` holds head — its
        // frames are no longer referenced by any reader (safe: mark held until
        // now, and `next` already locked what the diff needs).
        drop(self.prev.take());

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
            is_wal2: old_curr.is_wal2,
            interrupt_handle: prev_interrupt_handle,
        });

        // We need to keep curr's connection for the engine's table sources.
        // But we also need it for the diff. Solution: the diff borrows curr's
        // connection. Since we own it, we store it and lend a reference.
        let curr_snapshot = Snapshot {
            conn: curr_conn,
            version: curr_version_for_diff.clone(),
            is_wal2: next.is_wal2,
            interrupt_handle: curr_interrupt_handle,
        };
        self.curr = Some(curr_snapshot);
        self.publish_snapshot_interrupt_handles();

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
        // Gap-free leapfrog (see advance()): fresh head connection, drop the
        // stale prev after — never a ROLLBACK-in-place read-mark gap.
        let next = Snapshot::create(&self.db_file, self.page_cache_size_kib)?;
        drop(self.prev.take());

        // Commit the swap: prev = old curr, curr = next at head.
        self.prev = self.curr.take();
        self.curr = Some(next);
        self.publish_snapshot_interrupt_handles();
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
        if self.curr.is_some() {
            // Gap-free re-pin (see advance()): create a fresh head snapshot, then
            // drop the old curr — no ROLLBACK-in-place read-mark gap. Safe here:
            // called only at PipelineCount==0, so no source references old curr's
            // connection yet (the hydrate re-reads curr.conn after this).
            let fresh = Snapshot::create(&self.db_file, self.page_cache_size_kib)?;
            let version = fresh.version.clone();
            self.curr = Some(fresh);
            self.publish_snapshot_interrupt_handles();
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
    /// Cross-thread interrupt handle (N1). `None` only if the snapshot was
    /// constructed bypassing `create` (tests). The napi layer can extract this
    /// to register with the EngineHandle watchdog.
    interrupt_handle: Option<rusqlite::InterruptHandle>,
}

impl Snapshot {
    /// Open a fresh connection and pin it at the current head.
    fn create(db_file: &str, page_cache_size_kib: Option<i64>) -> Result<Self, String> {
        // Open read-write so wal2 can register a checkpoint-blocking read-mark
        // in -shm via BEGIN CONCURRENT. A read-only open cannot write -shm, so the
        // checkpointer IGNORES it and recycles needed frames under the pinned
        // snapshot → torn read → InvalidDiff teardown one advance later. TS opens
        // rw + beginConcurrent and has NO read-only fallback; we match that.
        //
        // Instead of silently degrading to an unsafe RO connection on rw-open
        // failure (e.g. a transient exclusive lock), retry the rw open a few times
        // with brief backoff; if it still fails, surface a retryable open error so
        // the caller can retry — never pin an unmarked RO snapshot.
        const RW_OPEN_ATTEMPTS: u32 = 5;
        const RW_OPEN_BACKOFF_MS: u64 = 20;
        let rw_flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
            | rusqlite::OpenFlags::SQLITE_OPEN_URI;
        let mut last_err: Option<rusqlite::Error> = None;
        let mut conn = None;
        for attempt in 0..RW_OPEN_ATTEMPTS {
            match rusqlite::Connection::open_with_flags(db_file, rw_flags) {
                Ok(c) => {
                    conn = Some(c);
                    break;
                }
                Err(e) => {
                    eprintln!(
                        "[rust-ivm] read-write snapshot open failed (attempt {}/{}): {} — retrying (NO read-only fallback: RO cannot write the wal2 -shm read-mark)",
                        attempt + 1,
                        RW_OPEN_ATTEMPTS,
                        e
                    );
                    last_err = Some(e);
                    if attempt + 1 < RW_OPEN_ATTEMPTS {
                        std::thread::sleep(std::time::Duration::from_millis(
                            RW_OPEN_BACKOFF_MS * (attempt as u64 + 1),
                        ));
                    }
                }
            }
        }
        let conn = conn.ok_or_else(|| {
            format!(
                "Snapshot::create open: read-write open failed after {} attempts (retryable): {}",
                RW_OPEN_ATTEMPTS,
                last_err
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            )
        })?;

        let interrupt_handle = crate::sqlite::install_interrupt(&conn);

        // These pragmas now take effect (read-write connection).
        let _ = conn.pragma_update(None, "synchronous", "OFF");
        let _ = conn.pragma_update(None, "case_sensitive_like", "ON");
        if let Some(cache_kib) = page_cache_size_kib {
            let _ = conn.pragma_update(None, "cache_size", -(cache_kib));
        }

        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .map_err(|e| format!("pragma journal_mode: {}", e))?;
        let is_wal2 = mode.eq_ignore_ascii_case("wal2");

        let mut snap = Snapshot {
            conn: Rc::new(RefCell::new(conn)),
            version: String::new(),
            is_wal2,
            interrupt_handle: Some(interrupt_handle),
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

    // reset_to_head (ROLLBACK-in-place re-pin) was removed: it dropped the wal2
    // read-mark between ROLLBACK and the re-lock, poisoning the snapshot under
    // drive-mode checkpoint churn (torn read → "database disk image is
    // malformed"). All leapfrog/re-pin paths now open a fresh gap-free snapshot
    // via Snapshot::create instead. See advance() / DESIGN-wal2-snapshot-isolation.md.
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
/// A prev/curr snapshot advanced past its pinned version mid-diff — RECOVERABLE
/// snapshot-staleness (the replica is intact; the diff just can't be computed).
/// Rehydrating at head fully recovers. Distinct from the schema/truncate/perms
/// resets only in trigger, not in handling. See diff.rs check_valid.
pub const REASON_STALE_SNAPSHOT: &str = "stale-snapshot";

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
