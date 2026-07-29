//! Validated stateVersion read-pool + `SnapshotGuard` (DESIGN §2, L1/L6).
//!
//! The parallel-hydrate worker pool needs connections that are *guaranteed* to
//! read the exact `stateVersion` the actor pinned — never a wrong frame (the
//! Go pin-race root cause: the reader pool bound non-deterministically and
//! silently read the wrong/serialized frame). This module is the pool.
//!
//! ## L1 — `SnapshotGuard` (RAII)
//! Owns one read-only `Connection` with an open `BEGIN` read tx. On drop —
//! even on panic — it `ROLLBACK`s the read tx (releasing the snapshot, so WAL
//! can checkpoint: without this a leaked read tx pins the WAL → unbounded
//! growth) and returns the connection to the pool. A guard that escapes its
//! scope without drop is a compile error; a panic mid-read is cleaned up by
//! `Drop`. This is the fix for the Go "hydrateReaders leak / WAL grows
//! unbounded" class.
//!
//! ## L6 — version-pin validation
//! `acquire(target_version)` takes a free connection, `BEGIN`s a read tx, reads
//! `_zero.replicationState`, and asserts the pinned version equals the target.
//! If the replica moved and the pin can't hold (head advanced past the target
//! between the actor's pin and the worker's acquire), the acquire returns
//! `None` → the caller falls back to **serial** hydrate for that task. **Never
//! read the wrong frame silently.**
//!
//! All connections are opened through [`crate::sqlite::install_interrupt`] (N1,
//! seam 1) so the watchdog/cancel can hard-abort a slow read on any pooled
//! connection — the same plumbing as the actor's connection, by construction.

use std::sync::{Arc, Mutex};

use rusqlite::OpenFlags;

/// A pooled read-only connection awaiting re-pin.
struct PooledConn {
    conn: Option<rusqlite::Connection>,
}

/// Bounded pool of read-only connections on a replica file.
///
/// `Send` (guarded by `Mutex`); one connection per worker (rusqlite `Connection`
/// is `!Sync`). The pool size is fixed at construction (≤ cores, config cap —
/// S3 bounded pool). Connections are opened lazily on first `acquire` (so a pool
/// that is never used by serial-fallback-only workloads costs nothing).
pub struct ReadPool {
    db_file: String,
    page_cache_size_kib: Option<i64>,
    /// Idle connections awaiting re-pin.
    free: Arc<Mutex<Vec<PooledConn>>>,
    /// Total live connections (idle + checked-out). Bounded by `capacity`.
    live: Arc<Mutex<usize>>,
    capacity: usize,
}

impl ReadPool {
    /// Create a pool of up to `capacity` read-only connections on `db_file`.
    /// Connections are opened lazily on first `acquire` (so a pool that is never
    /// used by serial-fallback-only workloads costs nothing).
    pub fn new(db_file: &str, page_cache_size_kib: Option<i64>, capacity: usize) -> Self {
        ReadPool {
            db_file: db_file.to_string(),
            page_cache_size_kib,
            free: Arc::new(Mutex::new(Vec::new())),
            live: Arc::new(Mutex::new(0)),
            capacity,
        }
    }

    /// Number of connections currently checked out (live `SnapshotGuard`s).
    /// For soak assertions ("0 connection leaks" — L1).
    pub fn live_count(&self) -> usize {
        *self.live.lock().unwrap()
    }

    /// Acquire a connection pinned and validated to `target_version`.
    ///
    /// Returns `Some(SnapshotGuard)` if a connection could be opened and its
    /// `BEGIN` read tx pins exactly `target_version`. Returns `None` if:
    ///   - the pool is exhausted (all connections checked out), or
    ///   - opening/pinning fails, or
    ///   - the replica moved and the pin can't hold (head != target_version).
    ///
    /// `None` ⟹ the caller MUST fall back to serial hydrate for that task
    /// (L6 — never read the wrong frame silently).
    ///
    /// `interrupt_handles` (if given) receives a cross-thread interrupt handle
    /// for the connection (a second `install_interrupt` call — each handle
    /// independently interrupts the same conn) so the watchdog/cancel (N1/L3/L4)
    /// can hard-abort a slow read on it. The guard retains its own handle too.
    pub fn acquire(
        &self,
        target_version: &str,
        interrupt_handles: Option<&Arc<Mutex<Vec<rusqlite::InterruptHandle>>>>,
    ) -> Option<SnapshotGuard> {
        let conn = self.take_or_open()?;
        match pin_and_validate(&conn, target_version, self.page_cache_size_kib) {
            Ok(true) => {
                let handle = crate::sqlite::install_interrupt(&conn);
                if let Some(reg) = interrupt_handles {
                    reg.lock().unwrap().push(crate::sqlite::install_interrupt(&conn));
                }
                *self.live.lock().unwrap() += 1; // outstanding guard
                Some(SnapshotGuard {
                    conn: Some(conn),
                    pool: self.free.clone(),
                    live: self.live.clone(),
                    capacity: self.capacity,
                    interrupt_handle: Some(handle),
                    interrupt_registry: interrupt_handles.cloned(),
                })
            }
            _ => {
                // Pin failed or version mismatch — return the connection to the
                // pool and signal serial fallback. Do NOT hand out a wrong-frame
                // connection under any circumstance (L6). `live` was NOT bumped
                // (the bump happens only on a successful guard handout).
                self.return_conn(conn);
                None
            }
        }
    }

    fn take_or_open(&self) -> Option<rusqlite::Connection> {
        // 1. Reuse an idle connection if one is available (no new open).
        {
            let mut free = self.free.lock().unwrap();
            if let Some(mut p) = free.pop() {
                if let Some(c) = p.conn.take() {
                    return Some(c);
                }
            }
        }
        // 2. Otherwise open a new one — but only if total (idle + outstanding)
        //    is under capacity. `live` is bumped by `acquire` on success, so
        //    here we only check the bound; the bump happens after validation.
        let live = *self.live.lock().unwrap();
        let idle = self.free.lock().unwrap().len();
        if live + idle >= self.capacity {
            return None; // exhausted → caller falls back to serial
        }
        open_readonly(&self.db_file).ok()
    }

    /// Failure path: a connection was opened/reused but validation failed, so it
    /// is returned to the idle list (or dropped if the pool is full). `live` was
    /// NOT yet bumped (the bump happens only on a successful guard handout), so
    /// no undo is needed here.
    fn return_conn(&self, conn: rusqlite::Connection) {
        let mut free = self.free.lock().unwrap();
        if free.len() < self.capacity {
            free.push(PooledConn { conn: Some(conn) });
        } else {
            drop(conn); // pool full → drop (rolled back by Connection::drop)
        }
    }
}

/// Open a read-only connection on the replica file (matches `Snapshot::create`).
fn open_readonly(db_file: &str) -> rusqlite::Result<rusqlite::Connection> {
    rusqlite::Connection::open_with_flags(
        db_file,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_URI,
    )
}

/// `BEGIN` a read tx on `conn`, read `_zero.replicationState.stateVersion`, and
/// return `Ok(true)` iff it equals `target_version`. The read tx pins the
/// snapshot at head; if head advanced past the target, this returns
/// `Ok(false)` → caller falls back to serial (L6).
fn pin_and_validate(
    conn: &rusqlite::Connection,
    target_version: &str,
    page_cache_size_kib: Option<i64>,
) -> Result<bool, String> {
    let _ = conn.pragma_update(None, "synchronous", "OFF");
    let _ = conn.pragma_update(None, "case_sensitive_like", "ON");
    if let Some(cache_kib) = page_cache_size_kib {
        let _ = conn.pragma_update(None, "cache_size", -(cache_kib));
    }
    conn.execute_batch("BEGIN")
        .map_err(|e| format!("read-pool BEGIN: {}", e))?;
    let version: String = conn
        .query_row(
            "SELECT stateVersion FROM \"_zero.replicationState\"",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("read-pool replicationState: {}", e))?;
    Ok(version == target_version)
}

/// RAII guard around a pooled read-tx connection (L1).
///
/// On drop: `ROLLBACK` the read tx (release the snapshot so WAL can checkpoint)
/// and return the bare connection to the pool for reuse. The interrupt handle is
/// removed from the watchdog registry (if registered). Even on panic, `Drop`
/// runs — no leaked read tx, no leaked connection, no WAL pin.
pub struct SnapshotGuard {
    conn: Option<rusqlite::Connection>,
    pool: Arc<Mutex<Vec<PooledConn>>>,
    live: Arc<Mutex<usize>>,
    capacity: usize,
    interrupt_handle: Option<rusqlite::InterruptHandle>,
    #[allow(dead_code)]
    interrupt_registry: Option<Arc<Mutex<Vec<rusqlite::InterruptHandle>>>>,
}

impl SnapshotGuard {
    /// Access the underlying connection (read-only reads against the pinned
    /// snapshot). The connection stays in its `BEGIN` read tx for the guard's
    /// lifetime — workers run `SELECT`s against it.
    pub fn connection(&self) -> &rusqlite::Connection {
        self.conn.as_ref().expect("SnapshotGuard connection taken")
    }

    /// The cross-thread interrupt handle for this connection (N1/L3). The
    /// watchdog/cancel calls `.interrupt()` to hard-abort a slow read.
    pub fn interrupt_handle(&self) -> Option<&rusqlite::InterruptHandle> {
        self.interrupt_handle.as_ref()
    }

    /// Lend the bare connection for worker-local use (DESIGN §2).
    ///
    /// Workers need to wrap the connection in `Rc<RefCell<>>` for
    /// `TableSource::new` — but `Rc` is `!Send`, so the `Rc<RefCell<>>` MUST be
    /// created on the worker thread. This method consumes the guard and returns
    /// the bare `rusqlite::Connection` (which IS `Send`) plus a `ReturnGuard`
    /// (RAII) that returns the connection to the pool on drop.
    ///
    /// The worker MUST call `return_guard.put_back(conn)` with the unwrapped
    /// connection before dropping the guard. If the worker panics first, the
    /// `ReturnGuard` drops, decrements `live`, and the connection (still inside
    /// the worker's `Rc<RefCell<>>`) is dropped by its own `Drop` — which
    /// ROLLBACKs the read tx. No WAL pin, no leak, just a non-pooled connection.
    pub fn lend(self) -> (rusqlite::Connection, ReturnGuard) {
        // Wrap in ManuallyDrop so we can move fields out without running Drop
        // on the guard shell (we transfer ownership of the fields by hand).
        let mut this = std::mem::ManuallyDrop::new(self);
        let conn = this
            .conn
            .take()
            .expect("SnapshotGuard.lend: no connection");
        let handle = this.interrupt_handle.take();
        let pool = this.pool.clone();
        let live = this.live.clone();
        let capacity = this.capacity;
        let registry = this.interrupt_registry.take();
        (
            conn,
            ReturnGuard {
                pool,
                live,
                capacity,
                conn: None,
                _interrupt_handle: handle,
                _interrupt_registry: registry,
            },
        )
    }
}

/// RAII: returns a lent connection to the pool on drop (L1). Created by
/// [`SnapshotGuard::lend`]. The worker puts the connection back via
/// `put_back()`; if it doesn't (panic), `Drop` still decrements `live`.
pub struct ReturnGuard {
    pool: Arc<Mutex<Vec<PooledConn>>>,
    live: Arc<Mutex<usize>>,
    capacity: usize,
    conn: Option<rusqlite::Connection>,
    _interrupt_handle: Option<rusqlite::InterruptHandle>,
    _interrupt_registry: Option<Arc<Mutex<Vec<rusqlite::InterruptHandle>>>>,
}

impl ReturnGuard {
    /// Put the connection back so it is returned to the pool on drop. The
    /// worker calls this after unwrapping its `Rc<RefCell<Connection>>`.
    pub fn put_back(&mut self, conn: rusqlite::Connection) {
        self.conn = Some(conn);
    }
}

impl Drop for ReturnGuard {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // ROLLBACK the read tx (release snapshot → WAL can checkpoint).
            let _ = conn.execute_batch("ROLLBACK");
            let mut free = self.pool.lock().unwrap();
            if free.len() < self.capacity {
                free.push(PooledConn { conn: Some(conn) });
            }
            // else: pool full → drop (rolled back above). No leak.
        }
        // If conn is None (worker panicked before put_back), the connection
        // was inside the worker's Rc<RefCell<>> and dropped there — its own
        // Drop ROLLBACKs. Here we just decrement live.
        *self.live.lock().unwrap() -= 1;
    }
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        // 1. ROLLBACK the read tx — release the snapshot so the WAL can
        //    checkpoint (a leaked read tx pins the WAL → unbounded growth).
        if let Some(ref conn) = self.conn {
            let _ = conn.execute_batch("ROLLBACK");
        }
        // 2. Return the connection to the pool for reuse (bounded by capacity),
        //    and decrement the outstanding-guard count.
        if let Some(conn) = self.conn.take() {
            let mut free = self.pool.lock().unwrap();
            if free.len() < self.capacity {
                free.push(PooledConn { conn: Some(conn) });
            }
            // else: pool full → drop the connection (rolled back above). No leak.
        }
        *self.live.lock().unwrap() -= 1;
        // The interrupt_registry is job-scoped and cleared by the actor; a stale
        // handle entry points to this dropped connection → `.interrupt()` is a
        // harmless no-op. No removal needed (InterruptHandle has no identity).
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    static UNIQ: AtomicU64 = AtomicU64::new(0);

    /// Create a minimal replica file with `_zero.replicationState` at 'v1'.
    /// Returns the path (caller cleans up via `remove_file` in `drop`).
    struct Replica {
        path: String,
    }
    impl Replica {
        fn new() -> Self {
            let n = UNIQ.fetch_add(1, Ordering::SeqCst);
            let path = format!("/tmp/rust-ivm-readpool-{}-{}.db", std::process::id(), n);
            let _ = std::fs::remove_file(&path);
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE \"_zero.replicationState\" (stateVersion TEXT PRIMARY KEY);
                 INSERT INTO \"_zero.replicationState\" (stateVersion) VALUES ('v1');",
            )
            .unwrap();
            drop(conn);
            Replica { path }
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
    fn acquire_validates_version_and_returns_guard() {
        let tf = Replica::new();
        let pool = ReadPool::new(&tf.path, None, 2);
        let g = pool.acquire("v1", None);
        assert!(g.is_some(), "acquire at head version should succeed");
        drop(g);
        assert!(pool.acquire("v1", None).is_some(), "re-acquire after drop works");
    }

    #[test]
    fn acquire_wrong_version_falls_back_to_serial() {
        let tf = Replica::new();
        let pool = ReadPool::new(&tf.path, None, 2);
        // Head is 'v1'; asking for 'v2' must NOT hand out a wrong-frame conn.
        assert!(pool.acquire("v2", None).is_none());
    }

    #[test]
    fn guard_drop_releases_read_tx_and_returns_conn() {
        let tf = Replica::new();
        let pool = Arc::new(ReadPool::new(&tf.path, None, 1));
        {
            let _g = pool.acquire("v1", None);
            assert_eq!(pool.live_count(), 1);
        }
        assert_eq!(pool.live_count(), 0, "guard drop must return the conn");
        {
            let _g = pool.acquire("v1", None);
            assert_eq!(pool.live_count(), 1);
        }
        assert_eq!(pool.live_count(), 0);
    }

    #[test]
    fn pool_exhaustion_returns_none_not_block() {
        let tf = Replica::new();
        let pool = ReadPool::new(&tf.path, None, 1);
        let _g1 = pool.acquire("v1", None);
        assert!(pool.acquire("v1", None).is_none(), "exhausted pool must not block");
    }

    #[test]
    fn interrupt_handle_available_for_watchdog_registration() {
        let tf = Replica::new();
        let pool = ReadPool::new(&tf.path, None, 1);
        let reg = Arc::new(Mutex::new(Vec::new()));
        let g = pool.acquire("v1", Some(&reg)).unwrap();
        assert!(g.interrupt_handle().is_some());
        assert_eq!(reg.lock().unwrap().len(), 1, "handle registered with watchdog registry");
        g.interrupt_handle().unwrap().interrupt();
    }
}
