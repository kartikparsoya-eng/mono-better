//! Frame-pinned parallel read pool (DESIGN-read-parallelism.md §2b/2a).
//!
//! The read-level parallelism substrate: a bounded set of read-only SQLite
//! connections **co-pinned at the same frame** as the actor's snapshot, so
//! worker threads can run the hydrate's SQL reads in parallel without ever
//! observing a different `stateVersion` than the actor's single-threaded graph.
//!
//! ## Why "frame-pinned", not lazy-acquire
//! The snapshotter pins `curr` with an open `BEGIN` read tx at a specific frame.
//! Under active replication, head advances **past** that frame. A connection
//! opened lazily (at read time) would `BEGIN` at *head* (newer) → it could never
//! reach the actor's older frame, so a version check would fail on every read
//! and fall back to serial. `sqlite3_snapshot_open` is dead on wal2, so an old
//! frame cannot be re-opened after the fact.
//!
//! Therefore the pool's connections are opened + `BEGIN`-pinned **at the instant
//! the snapshot pins** (`pin_frame`, called from `Snapshotter::init` /
//! `refresh_current_to_head` while `head == curr.version`) and held pinned for
//! the frame's whole lifetime. `parallel_read` borrows them read-only and
//! returns them **still pinned** — the read tx is released only on `unpin_frame`
//! (the next re-pin) or drop. This is the ephemeral, cold-hydrate-only pool from
//! memory `project_wal2_blocks_snapshot_pool`.
//!
//! ## Safety
//! - **Never mix frames.** `pin_frame` is all-or-nothing: if any connection pins
//!   a `stateVersion` other than the target, the whole pin is rolled back and
//!   `Err` is returned → the caller runs serially on the actor's own pinned
//!   connection. `parallel_read` refuses to run unless the pool is pinned at the
//!   requested version.
//! - **Byte-identical to serial.** `parallel_read` returns results strictly in
//!   input order regardless of which worker computed which task.
//! - **Single-writer preserved.** Workers receive only `Send` closures over a
//!   `&rusqlite::Connection`; the `!Send` engine graph is never touched.
//! - **Interruptible (N1).** Each pooled connection's cross-thread interrupt
//!   handle is registered so cancel()/the watchdog can hard-abort a slow read.
//! - **No WAL-pin leak.** `unpin_frame`/`Drop` ROLLBACK every connection so the
//!   WAL can checkpoint.

use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OpenFlags};

/// A bounded pool of read-only connections, all pinned at one snapshot frame.
///
/// `Send + Sync` (guarded by a `Mutex`). Lives on the actor thread but lends
/// bare `Connection`s (which are `Send`) to worker threads for the duration of a
/// `parallel_read`.
pub struct FramePinnedPool {
    db_file: String,
    page_cache_size_kib: Option<i64>,
    capacity: usize,
    /// Watchdog/cancel interrupt registry (N1). Pooled connections' handles are
    /// appended here on `pin_frame` — so `cancel()`/the watchdog reach them while
    /// the frame is pinned — and drained on `unpin_frame` so no stale handles
    /// accumulate. Shared with the actor's own connection handles; the pool only
    /// ever touches the contiguous range it appended (see `pinned_range`).
    registry: Option<Arc<Mutex<Vec<rusqlite::InterruptHandle>>>>,
    inner: Mutex<PoolInner>,
}

struct PoolInner {
    /// The frame every pooled connection is pinned at (`None` = unpinned).
    version: Option<String>,
    /// Pinned, idle connections (each in an open `BEGIN` read tx at `version`).
    free: Vec<Connection>,
    /// Number of connections currently lent to workers (for leak assertions).
    borrowed: usize,
    /// `(start, count)` of the handles this pool appended to `registry` for the
    /// current frame, so `unpin_frame` can drain exactly them. The registry is
    /// append-only between a pin and its unpin (only the pool drains, and only
    /// here), so the range stays valid.
    pinned_range: Option<(usize, usize)>,
}

impl FramePinnedPool {
    /// Create an unpinned pool of up to `capacity` connections on `db_file`.
    /// No connections are opened until `pin_frame`. `registry` (if given)
    /// receives pooled connections' interrupt handles while pinned (N1).
    pub fn new(
        db_file: &str,
        page_cache_size_kib: Option<i64>,
        capacity: usize,
        registry: Option<Arc<Mutex<Vec<rusqlite::InterruptHandle>>>>,
    ) -> Self {
        FramePinnedPool {
            db_file: db_file.to_string(),
            page_cache_size_kib,
            capacity: capacity.max(1),
            registry,
            inner: Mutex::new(PoolInner {
                version: None,
                free: Vec::new(),
                borrowed: 0,
                pinned_range: None,
            }),
        }
    }

    /// The frame the pool is currently pinned at, if any.
    pub fn pinned_version(&self) -> Option<String> {
        self.inner.lock().unwrap().version.clone()
    }

    /// Idle pinned connections (for soak assertions).
    pub fn free_count(&self) -> usize {
        self.inner.lock().unwrap().free.len()
    }

    /// Pin `count` (≤ capacity) fresh connections at `target_version`.
    ///
    /// Idempotent: if already pinned at `target_version`, returns `Ok` without
    /// re-opening. Otherwise the previous frame is unpinned first.
    ///
    /// **All-or-nothing** (never mix frames): if any connection's `BEGIN` pins a
    /// `stateVersion` other than `target_version` (head advanced between the
    /// snapshot's pin and this call), every opened connection is rolled back and
    /// dropped, the pool is left unpinned, and `Err` is returned → the caller
    /// hydrates serially on the actor's own pinned connection.
    ///
    /// Must be called while `head == target_version` (i.e. right after the
    /// snapshot pins `curr` to head at cold hydrate). Each connection's
    /// cross-thread interrupt handle is appended to the pool's registry (N1) and
    /// drained again by `unpin_frame`.
    pub fn pin_frame(&self, target_version: &str, count: usize) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap();
        if inner.version.as_deref() == Some(target_version) {
            return Ok(()); // already pinned at this frame
        }
        assert_eq!(
            inner.borrowed, 0,
            "pin_frame while connections are borrowed (concurrent read?)"
        );
        // Roll back any prior frame's connections + drain its interrupt handles
        // before re-pinning (so handles never accumulate across frames).
        self.unpin_locked(&mut inner);

        let n = count.min(self.capacity).max(1);
        let mut conns: Vec<Connection> = Vec::with_capacity(n);
        for _ in 0..n {
            let conn = match open_and_pin(&self.db_file, target_version, self.page_cache_size_kib) {
                Ok(c) => c,
                Err(e) => {
                    // Frame moved (or open failed) — abandon the whole pin.
                    rollback_all(&mut conns);
                    return Err(e);
                }
            };
            conns.push(conn);
        }
        // Append this frame's interrupt handles as a contiguous range so
        // `unpin_frame` can drain exactly them.
        if let Some(reg) = &self.registry {
            let mut r = reg.lock().unwrap();
            let start = r.len();
            for conn in &conns {
                r.push(crate::sqlite::install_interrupt(conn));
            }
            inner.pinned_range = Some((start, conns.len()));
        }
        inner.free = conns;
        inner.version = Some(target_version.to_string());
        Ok(())
    }

    /// Release the current frame: ROLLBACK every connection (so the WAL can
    /// checkpoint), drop them, and drain this frame's interrupt handles from the
    /// registry. Called before the snapshot re-pins to a new frame, and on
    /// destroy.
    pub fn unpin_frame(&self) {
        let mut inner = self.inner.lock().unwrap();
        self.unpin_locked(&mut inner);
    }

    /// Shared unpin body (caller holds `inner`). Rolls back connections and
    /// drains the pool's contiguous handle range from the registry. Locks
    /// `registry` only while already holding `inner` (the `inner → registry`
    /// order used everywhere) so it can't deadlock `interrupt_all`/`cancel`.
    fn unpin_locked(&self, inner: &mut PoolInner) {
        debug_assert_eq!(inner.borrowed, 0, "unpin while connections borrowed");
        rollback_all(&mut inner.free);
        inner.version = None;
        if let (Some(reg), Some((start, count))) = (&self.registry, inner.pinned_range.take()) {
            let mut r = reg.lock().unwrap();
            let end = (start + count).min(r.len());
            if start <= r.len() {
                r.drain(start..end);
            }
        }
    }

    /// Run `tasks` in parallel across the pinned connections, returning results
    /// in input order (byte-identical to serial).
    ///
    /// Refuses to run (returns `Err`) unless the pool is pinned at
    /// `target_version` — the caller then falls back to a serial read on the
    /// actor's own pinned connection. Never mixes frames.
    ///
    /// Each worker owns one pooled connection exclusively for the call and
    /// processes a slice of the tasks on it; connections are returned **still
    /// pinned** (no rollback) so the frame survives across reads within a
    /// hydrate.
    pub fn parallel_read<T, F>(
        &self,
        target_version: &str,
        tasks: Vec<F>,
    ) -> Result<Vec<T>, String>
    where
        T: Send,
        F: FnOnce(&Connection) -> Result<T, String> + Send,
    {
        let n = tasks.len();
        if n == 0 {
            return Ok(Vec::new());
        }

        // Take the connections we'll use out of the pool up front (all pinned at
        // the target frame). If the pool isn't pinned here, bail → serial.
        let lent: Vec<Connection> = {
            let mut inner = self.inner.lock().unwrap();
            if inner.version.as_deref() != Some(target_version) {
                return Err(format!(
                    "read pool not pinned at {} (pinned at {:?}) — serial fallback",
                    target_version, inner.version
                ));
            }
            let take = inner.free.len().min(n);
            if take == 0 {
                return Err("read pool has no free pinned connections".to_string());
            }
            inner.borrowed += take;
            let split = inner.free.len() - take;
            inner.free.split_off(split)
        };
        let n_workers = lent.len();

        // Result slots (input order), shared task-index queue, first-error-wins.
        let results: Vec<Mutex<Option<T>>> = (0..n).map(|_| Mutex::new(None)).collect();
        let tasks: Vec<Mutex<Option<F>>> = tasks.into_iter().map(|t| Mutex::new(Some(t))).collect();
        let queue = Mutex::new(std::collections::VecDeque::<usize>::from_iter(0..n));
        let first_err: Mutex<Option<String>> = Mutex::new(None);

        // Each worker OWNS one connection (`Connection` is `Send` but not `Sync`,
        // so a shared `&Connection` can't cross threads) and returns it when done
        // so we can put it back into the pool still pinned.
        let mut returned: Vec<Connection> = std::thread::scope(|s| {
            let handles: Vec<_> = lent
                .into_iter()
                .map(|conn| {
                    let results = &results;
                    let tasks = &tasks;
                    let queue = &queue;
                    let first_err = &first_err;
                    s.spawn(move || {
                        loop {
                            if first_err.lock().unwrap().is_some() {
                                break;
                            }
                            let idx = { queue.lock().unwrap().pop_front() };
                            let Some(idx) = idx else { break };
                            let task = { tasks[idx].lock().unwrap().take() };
                            let Some(task) = task else { continue };
                            // A task can panic (e.g. `sqlite_value_to_ivm`'s
                            // ±2^53 overflow assert). Catch it here so the worker
                            // NEVER unwinds: unwinding would skip returning the
                            // connection and leave `borrowed` corrupted. First
                            // panic wins → the batch aborts → serial fallback (the
                            // serial path re-hits the same panic → clean reset).
                            let outcome = std::panic::catch_unwind(
                                std::panic::AssertUnwindSafe(|| task(&conn)),
                            );
                            match outcome {
                                Ok(Ok(v)) => *results[idx].lock().unwrap() = Some(v),
                                Ok(Err(msg)) => {
                                    let mut e = first_err.lock().unwrap();
                                    if e.is_none() {
                                        *e = Some(msg);
                                    }
                                    break;
                                }
                                Err(panic) => {
                                    let mut e = first_err.lock().unwrap();
                                    if e.is_none() {
                                        *e = Some(format!(
                                            "read-pool worker panic: {}",
                                            panic_msg(&panic)
                                        ));
                                    }
                                    break;
                                }
                            }
                        }
                        conn // hand the pinned connection back
                    })
                })
                .collect();
            // `filter_map(join.ok())`: with `catch_unwind` above a worker won't
            // unwind, but stay resilient — a lost connection must never leave the
            // `borrowed` counter wrong (that would poison the next `pin_frame`).
            handles.into_iter().filter_map(|h| h.join().ok()).collect()
        });

        // Return the connections to the pool (STILL pinned — no rollback).
        // `borrowed` is decremented by the full `n_workers` unconditionally, so a
        // connection lost to an unexpected worker panic shrinks capacity by one
        // (restored on the next `pin_frame`) rather than corrupting the counter.
        {
            let mut inner = self.inner.lock().unwrap();
            inner.borrowed -= n_workers;
            inner.free.append(&mut returned);
        }

        if let Some(msg) = first_err.into_inner().unwrap() {
            return Err(msg);
        }
        let mut out = Vec::with_capacity(n);
        for slot in results {
            match slot.into_inner().unwrap() {
                Some(v) => out.push(v),
                None => return Err("read pool: internal — result slot unfilled".to_string()),
            }
        }
        Ok(out)
    }
}

impl Drop for FramePinnedPool {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.inner.lock() {
            rollback_all(&mut inner.free);
        }
    }
}

/// Open a read-only connection on the replica file (matches `Snapshot::create`).
fn open_readonly(db_file: &str) -> rusqlite::Result<Connection> {
    Connection::open_with_flags(
        db_file,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_URI,
    )
}

/// Open a connection and `BEGIN`-pin it, returning it only if it pinned exactly
/// `target_version`. On mismatch/failure the connection is rolled back + dropped
/// and `Err` is returned (caller abandons the whole pin → serial).
fn open_and_pin(
    db_file: &str,
    target_version: &str,
    page_cache_size_kib: Option<i64>,
) -> Result<Connection, String> {
    let conn = open_readonly(db_file).map_err(|e| format!("read pool open: {}", e))?;
    let _ = conn.pragma_update(None, "synchronous", "OFF");
    let _ = conn.pragma_update(None, "case_sensitive_like", "ON");
    if let Some(cache_kib) = page_cache_size_kib {
        let _ = conn.pragma_update(None, "cache_size", -(cache_kib));
    }
    conn.execute_batch("BEGIN").map_err(|e| format!("read pool BEGIN: {}", e))?;
    let version: String = conn
        .query_row("SELECT stateVersion FROM \"_zero.replicationState\"", [], |r| r.get(0))
        .map_err(|e| format!("read pool replicationState: {}", e))?;
    if version != target_version {
        let _ = conn.execute_batch("ROLLBACK");
        return Err(format!(
            "read pool pinned {} but target is {} (head advanced) — serial fallback",
            version, target_version
        ));
    }
    Ok(conn)
}

/// Best-effort string for a caught panic payload.
fn panic_msg(p: &Box<dyn std::any::Any + Send>) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string())
}

/// ROLLBACK and drop every connection in `conns` (release the read tx so the WAL
/// can checkpoint — a leaked read tx pins the WAL → unbounded growth).
fn rollback_all(conns: &mut Vec<Connection>) {
    for conn in conns.drain(..) {
        let _ = conn.execute_batch("ROLLBACK");
        drop(conn);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static UNIQ: AtomicU64 = AtomicU64::new(0);

    /// A minimal replica file with `_zero.replicationState` at 'v1'.
    struct Replica {
        path: String,
    }
    impl Replica {
        fn new() -> Self {
            let n = UNIQ.fetch_add(1, Ordering::SeqCst);
            let path = format!("/tmp/rust-ivm-framepool-{}-{}.db", std::process::id(), n);
            let _ = std::fs::remove_file(&path);
            let conn = rusqlite::Connection::open(&path).unwrap();
            // WAL so a reader's BEGIN snapshot doesn't block a concurrent writer
            // (mirrors the wal2 replica: readers pin an old frame while head
            // advances). Rollback-journal mode would deadlock set_version().
            let _: String = conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0)).unwrap();
            conn.execute_batch(
                "CREATE TABLE \"_zero.replicationState\" (stateVersion TEXT PRIMARY KEY);
                 INSERT INTO \"_zero.replicationState\" (stateVersion) VALUES ('v1');",
            )
            .unwrap();
            drop(conn);
            Replica { path }
        }
        /// Advance the replica's head version (simulates the replicator).
        fn set_version(&self, v: &str) {
            let conn = rusqlite::Connection::open(&self.path).unwrap();
            conn.execute("UPDATE \"_zero.replicationState\" SET stateVersion = ?", [v]).unwrap();
            drop(conn);
        }
    }
    impl Drop for Replica {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_file(format!("{}-wal", self.path));
            let _ = std::fs::remove_file(format!("{}-shm", self.path));
        }
    }

    #[test]
    fn pin_frame_at_head_then_parallel_read_in_order() {
        let tf = Replica::new();
        let pool = FramePinnedPool::new(&tf.path, None, 4, None);
        pool.pin_frame("v1", 4).unwrap();
        assert_eq!(pool.pinned_version().as_deref(), Some("v1"));

        let tasks: Vec<_> = (0..20usize)
            .map(|i| {
                move |conn: &Connection| -> Result<usize, String> {
                    let v: String = conn
                        .query_row("SELECT stateVersion FROM \"_zero.replicationState\"", [], |r| r.get(0))
                        .map_err(|e| e.to_string())?;
                    assert_eq!(v, "v1", "every read observes the pinned frame");
                    Ok(i)
                }
            })
            .collect();
        let out = pool.parallel_read("v1", tasks).unwrap();
        assert_eq!(out, (0..20).collect::<Vec<_>>(), "results strictly in input order");
        assert_eq!(pool.free_count(), 4, "all connections returned still pinned");
    }

    #[test]
    fn parallel_read_holds_frame_across_multiple_calls() {
        // The whole point: the pool must keep reading the OLD frame even after
        // the replicator advances head (lazy-open would fail here).
        let tf = Replica::new();
        let pool = FramePinnedPool::new(&tf.path, None, 3, None);
        pool.pin_frame("v1", 3).unwrap();
        // Replicator moves head forward AFTER we pinned.
        tf.set_version("v2");
        for _ in 0..3 {
            let tasks: Vec<_> = (0..6usize)
                .map(|i| {
                    move |conn: &Connection| -> Result<usize, String> {
                        let v: String = conn
                            .query_row("SELECT stateVersion FROM \"_zero.replicationState\"", [], |r| r.get(0))
                            .map_err(|e| e.to_string())?;
                        // Still sees v1 — the BEGIN read tx pins the old frame.
                        assert_eq!(v, "v1");
                        Ok(i)
                    }
                })
                .collect();
            let out = pool.parallel_read("v1", tasks).unwrap();
            assert_eq!(out, (0..6).collect::<Vec<_>>());
        }
    }

    #[test]
    fn pin_frame_wrong_version_errs_all_or_nothing() {
        let tf = Replica::new();
        let pool = FramePinnedPool::new(&tf.path, None, 4, None);
        // Head is v1; asking to pin v2 must fail without leaving a partial pin.
        assert!(pool.pin_frame("v2", 4).is_err());
        assert_eq!(pool.pinned_version(), None, "no partial frame left");
        assert_eq!(pool.free_count(), 0);
    }

    #[test]
    fn parallel_read_unpinned_errs_for_serial_fallback() {
        let tf = Replica::new();
        let pool = FramePinnedPool::new(&tf.path, None, 2, None);
        let tasks: Vec<Box<dyn FnOnce(&Connection) -> Result<usize, String> + Send>> =
            (0..4usize).map(|i| Box::new(move |_c: &Connection| Ok(i)) as Box<_>).collect();
        // Not pinned → Err → serial fallback.
        assert!(pool.parallel_read("v1", tasks).is_err());
    }

    #[test]
    fn parallel_read_task_error_wins() {
        let tf = Replica::new();
        let pool = FramePinnedPool::new(&tf.path, None, 3, None);
        pool.pin_frame("v1", 3).unwrap();
        let tasks: Vec<Box<dyn FnOnce(&Connection) -> Result<usize, String> + Send>> = (0..12usize)
            .map(|i| {
                Box::new(move |_c: &Connection| -> Result<usize, String> {
                    if i == 5 { Err("boom".to_string()) } else { Ok(i) }
                }) as Box<_>
            })
            .collect();
        assert!(matches!(pool.parallel_read("v1", tasks), Err(ref m) if m == "boom"));
        assert_eq!(pool.free_count(), 3, "connections returned after error");
    }

    #[test]
    fn parallel_read_worker_panic_errs_and_pool_stays_usable() {
        // A panicking task (e.g. a value-overflow assert) must NOT corrupt the
        // pool: it returns Err (→ serial fallback), connections come back, the
        // `borrowed` counter is restored, and a subsequent pin/read still works.
        let tf = Replica::new();
        let pool = FramePinnedPool::new(&tf.path, None, 3, None);
        pool.pin_frame("v1", 3).unwrap();
        let tasks: Vec<Box<dyn FnOnce(&Connection) -> Result<usize, String> + Send>> = (0..9usize)
            .map(|i| {
                Box::new(move |_c: &Connection| -> Result<usize, String> {
                    if i == 4 {
                        panic!("value out of bounds");
                    }
                    Ok(i)
                }) as Box<_>
            })
            .collect();
        let res = pool.parallel_read("v1", tasks);
        assert!(matches!(res, Err(ref m) if m.contains("worker panic")), "panic → Err, got {res:?}");
        assert_eq!(pool.free_count(), 3, "connections returned after a worker panic");
        // Pool still usable — `borrowed` was not corrupted (this would panic on a
        // bad counter via pin_frame's assert), and a fresh read succeeds.
        let ok_tasks: Vec<_> = (0..6usize)
            .map(|i| move |_c: &Connection| -> Result<usize, String> { Ok(i) })
            .collect();
        assert_eq!(pool.parallel_read("v1", ok_tasks).unwrap(), (0..6).collect::<Vec<_>>());
    }

    #[test]
    fn unpin_then_repin_new_frame() {
        let tf = Replica::new();
        let pool = FramePinnedPool::new(&tf.path, None, 2, None);
        pool.pin_frame("v1", 2).unwrap();
        pool.unpin_frame();
        assert_eq!(pool.pinned_version(), None);
        // Head moved to v2; re-pin succeeds at the new frame.
        tf.set_version("v2");
        pool.pin_frame("v2", 2).unwrap();
        assert_eq!(pool.pinned_version().as_deref(), Some("v2"));
    }

    #[test]
    fn interrupt_handles_registered_and_drained_on_unpin() {
        let tf = Replica::new();
        let reg = Arc::new(Mutex::new(Vec::new()));
        // A pre-existing handle (stands in for the actor's own connection) — the
        // pool must never disturb it, only its own appended range.
        {
            let c = rusqlite::Connection::open(&tf.path).unwrap();
            reg.lock().unwrap().push(crate::sqlite::install_interrupt(&c));
            std::mem::forget(c); // keep the handle valid for the test
        }
        let pool = FramePinnedPool::new(&tf.path, None, 3, Some(reg.clone()));
        pool.pin_frame("v1", 3).unwrap();
        // While pinned the pool's handles live in the shared registry, so the
        // existing cancel()/watchdog sweep reaches them (N1).
        assert_eq!(reg.lock().unwrap().len(), 4, "1 pre-existing + 3 pooled handles");
        // Unpin drains exactly the pool's 3 handles, leaving the pre-existing one.
        pool.unpin_frame();
        assert_eq!(reg.lock().unwrap().len(), 1, "pool handles drained on unpin, actor's kept");
        // Re-pin appends a fresh range (no accumulation across frames).
        pool.pin_frame("v1", 3).unwrap();
        assert_eq!(reg.lock().unwrap().len(), 4, "re-pin appends exactly its own range again");
    }
}
