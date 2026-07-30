//! Phase 0 tests (N1 + N2): cross-thread SQLite interrupt + job-scoped watchdog.
//!
//! Validates the two safety additions from DESIGN §1a/§4:
//! - N1: `install_interrupt(conn)` produces a `Send + Sync` handle whose
//!   `.interrupt()` aborts a query running on that connection in-flight.
//! - N2: `JobWatchdog` registers a (deadline, handles, cancel) entry; on
//!   deadline it flips the cancel token AND `.interrupt()`s the handles.
//!
//! Both tests assert the wedge is closed: a deliberately-slow SQLite query
//! that the between-rows cancel check would never reach is hard-aborted under
//! the deadline via the cross-thread interrupt.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rust_ivm::engine::CancellationToken;
use rust_ivm::sqlite::{JobWatchdog, install_interrupt};

/// A slow SQLite query: `SELECT ... FROM generate_series` wrapped in a busy
/// loop so it runs for several seconds if uninterrupted. Using an in-memory
/// temp DB keeps the test hermetic (no file on disk).
fn slow_query_conn() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    // Load the generate_series extension if available; otherwise emulate a
    // slow query with a recursive CTE that produces many rows.
    let _ = conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
         WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 1000000)
         INSERT INTO t SELECT x, printf('row%d', x) FROM seq;",
    );
    conn
}

/// A query that does a lot of work — a cross join + aggregation over the
/// 1M-row table. Uninterrupted this takes seconds; interrupted it returns
/// SQLITE_INTERRUPT promptly.
fn run_slow_query(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    // Force a full table scan with an aggregation that can't use an index.
    // Qualify columns to avoid ambiguity in the cross join.
    let mut stmt = conn.prepare(
        "SELECT COUNT(*), SUM(length(a.v)) FROM t a CROSS JOIN t b WHERE a.v LIKE '%999%'",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(_row) = rows.next()? {
        // drain
    }
    Ok(())
}

#[test]
fn install_interrupt_returns_send_sync_handle() {
    // The handle must be usable from another thread. We can't prove Send+Sync
    // at runtime, but we CAN prove the handle interrupts a running query.
    let query_conn = rusqlite::Connection::open_in_memory().unwrap();
    let _ = query_conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
         WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 100000)
         INSERT INTO t SELECT x, printf('row%d', x) FROM seq;",
    );
    let handle = install_interrupt(&query_conn);
    let done = Arc::new(Mutex::new(false));
    let done_clone = done.clone();
    let join = std::thread::spawn(move || {
        let r = run_slow_query(&query_conn);
        *done_clone.lock().unwrap() = true;
        r
    });
    // Give the query a moment to start, then interrupt it.
    std::thread::sleep(Duration::from_millis(100));
    handle.interrupt();
    let result = join.join().unwrap();
    // The query should have been interrupted (SQLITE_INTERRUPT) and returned
    // promptly — well under the time it would take uninterrupted.
    assert!(
        *done.lock().unwrap(),
        "interrupted query should have returned"
    );
    match result {
        Err(rusqlite::Error::SqliteFailure(err, _))
            if err.extended_code == rusqlite::ffi::SQLITE_INTERRUPT =>
        {
            // expected
        }
        Err(e) => panic!("expected SQLITE_INTERRUPT, got: {e}"),
        Ok(()) => panic!("query should have been interrupted, not completed"),
    }
}

#[test]
fn watchdog_fires_at_abort_bound_flips_cancel_and_interrupts() {
    let watchdog = JobWatchdog::new();
    let cancel = CancellationToken::new();
    let conn = slow_query_conn();
    let handle = install_interrupt(&conn);
    let handles = Arc::new(Mutex::new(vec![handle]));
    let cancel_clone = cancel.clone();

    // Run a slow query on a thread; the watchdog should fire at the ABORT
    // bound and abort it. `warn_at` is already in the past (the monitor logs a
    // slow-job signal immediately — NON-aborting); `abort_at` is the short
    // bound that flips cancel + interrupts. This mirrors the prod semantics:
    // warn is log-only, abort is the hard cancel+interrupt.
    let warn_at = Instant::now();
    let abort_at = Instant::now() + Duration::from_millis(200);
    let _guard = watchdog.register(warn_at, abort_at, cancel.clone(), handles.clone());

    let done = Arc::new(Mutex::new(false));
    let done_clone = done.clone();
    let join = std::thread::spawn(move || {
        let r = run_slow_query(&conn);
        *done_clone.lock().unwrap() = true;
        r
    });

    let result = join.join().unwrap();
    assert!(
        *done.lock().unwrap(),
        "query should have returned after interrupt"
    );
    // The cancel token should have been flipped by the watchdog at abort_at.
    assert!(
        cancel_clone.is_cancelled(),
        "watchdog should have flipped the cancel token at abort bound"
    );
    match result {
        Err(rusqlite::Error::SqliteFailure(err, _))
            if err.extended_code == rusqlite::ffi::SQLITE_INTERRUPT =>
        {
            // expected
        }
        Err(e) => panic!("expected SQLITE_INTERRUPT, got: {e}"),
        Ok(()) => panic!("query should have been interrupted by the watchdog abort bound"),
    }
}

#[test]
fn watchdog_unregisters_on_guard_drop() {
    let watchdog = JobWatchdog::new();
    let cancel = CancellationToken::new();
    let handles = Arc::new(Mutex::new(vec![]));

    // Register a job with a far-future abort bound, then drop the guard.
    // The cancel token should NOT be flipped after the abort bound if the
    // guard was dropped (the job returned in time). warn_at is log-only and
    // never flips cancel, so only the abort bound matters here.
    {
        let warn_at = Instant::now() + Duration::from_millis(100);
        let abort_at = Instant::now() + Duration::from_secs(5);
        let _guard = watchdog.register(warn_at, abort_at, cancel.clone(), handles);
        // guard dropped here
    }
    // Wait past the original warn bound (and well past any abort window the
    // dropped guard would have left).
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        !cancel.is_cancelled(),
        "dropped guard must not fire the watchdog abort"
    );
}

#[test]
fn cancel_mid_query_aborts_via_interrupt_handle() {
    // Simulates the N1 cancel path: the driver calls cancel() which flips the
    // token AND .interrupt()s every registered handle. A query wedged in
    // SQLite (between-rows check never reached) must abort promptly.
    let cancel = CancellationToken::new();
    let conn = slow_query_conn();
    let handle = install_interrupt(&conn);
    let handles: Vec<rusqlite::InterruptHandle> = vec![handle];

    let done = Arc::new(Mutex::new(false));
    let done_clone = done.clone();
    let cancel_clone = cancel.clone();
    let join = std::thread::spawn(move || {
        let r = run_slow_query(&conn);
        *done_clone.lock().unwrap() = true;
        r
    });

    // Let the query start, then simulate cancel().
    std::thread::sleep(Duration::from_millis(100));
    cancel_clone.cancel();
    for h in &handles {
        h.interrupt();
    }

    let result = join.join().unwrap();
    assert!(
        *done.lock().unwrap(),
        "cancelled query should have returned"
    );
    match result {
        Err(rusqlite::Error::SqliteFailure(err, _))
            if err.extended_code == rusqlite::ffi::SQLITE_INTERRUPT =>
        {
            // expected
        }
        Err(e) => panic!("expected SQLITE_INTERRUPT from cancel(), got: {e}"),
        Ok(()) => panic!("query should have been interrupted by cancel()"),
    }
}

#[test]
fn watchdog_warn_bound_is_non_aborting() {
    // Prod-safety regression (Finding 1): the warn bound must NOT abort. A
    // legit cold hydrate under load can take 43–144s, so a job past the warn
    // bound must only be logged, not cancel/interrupted — otherwise the
    // watchdog would cause a reset-storm on large hydrates. Only the abort
    // bound (well above any legit op) flips cancel + interrupts.
    let watchdog = JobWatchdog::new();
    let cancel = CancellationToken::new();
    let handles = Arc::new(Mutex::new(vec![]));

    // warn_at is near (will fire during the test); abort_at is far (must NOT).
    let warn_at = Instant::now() + Duration::from_millis(100);
    let abort_at = Instant::now() + Duration::from_secs(30);
    let _guard = watchdog.register(warn_at, abort_at, cancel.clone(), handles);

    // Sleep past the warn bound but well before the abort bound.
    std::thread::sleep(Duration::from_millis(400));
    assert!(
        !cancel.is_cancelled(),
        "warn bound must be non-aborting — a job past warn but before abort must not be cancelled"
    );
}
