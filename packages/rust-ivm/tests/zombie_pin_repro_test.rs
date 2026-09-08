//! Zombie-pin repro: the prod WAL-growth mechanism.
//!
//! If ANY busy (stepped, un-reset) statement exists on a snapshot connection
//! when `Snapshotter::advance()` leapfrogs it, `reset_to_head`'s ROLLBACK fails
//! ("cannot rollback transaction - SQL statements in progress"). On current
//! code the `?` propagates, the `Snapshot` is dropped, and rusqlite's
//! `InnerConnection::drop` calls `sqlite3_close` IGNORING the SQLITE_BUSY it
//! returns (`#[allow(unused_must_use)]`, rusqlite-0.32.1) — the connection
//! leaks at the C level **with its read transaction still open**. That open
//! read txn is a permanent, invisible checkpoint pin: the CG itself recovers on
//! the next advance (`prev.take() → None → Snapshot::create` fresh conn) and
//! logs healthily forever, while the WAL grows at the write rate. This is the
//! rust-only divergence from TS: better-sqlite3's `close()` finalizes every
//! open statement before closing, so a failed rollback can never orphan an
//! open read txn.
//!
//! These tests assert the DESIRED invariant (the better-sqlite3 contract):
//!  1. advance() must survive a stray busy statement — reset it, roll back,
//!     re-pin (never orphan the connection).
//!  2. No drop path may leak an open read transaction, even when the
//!     underlying `sqlite3_close` fails on an unfinalized statement.
//!
//! They are RED on the pre-fix code (advance → Err; checkpoint busy forever)
//! and GREEN with the statement-reset-before-ROLLBACK/Drop fix.
//!
//! Plain-WAL locally (`non-wal2-test-support`); the pin/checkpoint semantics
//! under test are journal-mode-independent (see wal_cursor_pin_test.rs).

use rusqlite::Connection;
use rust_ivm::snapshotter::Snapshotter;

fn clean_db(path: &str) {
    for p in [
        path.to_string(),
        format!("{path}-wal"),
        format!("{path}-wal2"),
        format!("{path}-shm"),
    ] {
        let _ = std::fs::remove_file(p);
    }
}

/// Minimal replica: zero replication tables + one data table, WAL mode.
fn create_replica(path: &str) {
    clean_db(path);
    let conn = Connection::open(path).unwrap();
    let _ = conn.pragma_update(None, "journal_mode", "wal2");
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    if !mode.eq_ignore_ascii_case("wal2") {
        conn.pragma_update(None, "journal_mode", "wal").unwrap();
    }
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS "_zero.replicationState" (
            lock TEXT PRIMARY KEY DEFAULT 'singleton',
            stateVersion TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS "_zero.changeLog2" (
            "stateVersion" TEXT NOT NULL,
            "table"        TEXT NOT NULL,
            "rowKey"       TEXT NOT NULL,
            "op"           TEXT NOT NULL,
            "pos"          INTEGER NOT NULL,
            PRIMARY KEY ("stateVersion", "pos")
        );
        CREATE TABLE IF NOT EXISTS users (
            id         TEXT PRIMARY KEY,
            name       TEXT NOT NULL,
            _0_version TEXT NOT NULL
        );
        INSERT OR REPLACE INTO "_zero.replicationState" (lock, stateVersion)
            VALUES ('singleton', 'v1');
        INSERT INTO users (id, name, _0_version) VALUES ('u1', 'Alice', 'v1');
        "#,
    )
    .unwrap();
    drop(conn);
}

/// (busy, checkpointed) from `PRAGMA wal_checkpoint(TRUNCATE)`.
fn checkpoint_truncate(conn: &Connection) -> (i64, i64) {
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
        Ok((r.get(0)?, r.get(2)?))
    })
    .unwrap()
}

/// Open a cursor on `conn` and step it once WITHOUT resetting or finalizing —
/// exactly the C-level state a stashed/leaked `LazyRows` leaves behind. The
/// statement is then `mem::forget`-ten so it survives every Rust drop path,
/// modelling a cursor whose owner never runs its destructor (the only way a
/// statement can still exist when the connection itself is torn down).
fn leak_busy_statement(conn: &std::rc::Rc<std::cell::RefCell<Connection>>) {
    let guard = conn.borrow();
    // Erase the Ref lifetime so stmt/rows can be forgotten independently of
    // the scope — the same transmute pattern LazyRows uses in table_source.rs.
    let guard_static: std::cell::Ref<'static, Connection> = unsafe { std::mem::transmute(guard) };
    let stmt = guard_static.prepare("SELECT id, name FROM users").unwrap();
    let mut stmt_static: rusqlite::Statement<'static> = unsafe { std::mem::transmute(stmt) };
    let stmt_ptr: *mut rusqlite::Statement<'static> = &mut stmt_static;
    let mut rows = unsafe { (*stmt_ptr).query([]) }.unwrap();
    let first = rows.next().unwrap();
    assert!(first.is_some(), "expected to step one row");
    std::mem::forget(rows);
    std::mem::forget(stmt_static);
    std::mem::forget(guard_static);
}

/// Invariant 1: a stray busy statement on the leapfrogged (prev) connection
/// must NOT fail the advance. The snapshotter owns its connections' statement
/// lifecycle at rollback time (better-sqlite3 contract): reset stray cursors,
/// ROLLBACK, re-pin.
///
/// Pre-fix: `reset_to_head` → ROLLBACK → "SQL statements in progress" → Err,
/// and the pinned connection is dropped-and-leaked (see invariant 2).
#[test]
fn advance_survives_busy_statement_on_prev_connection() {
    let db_path = "/tmp/rust-ivm-zombie-pin-1.db";
    create_replica(db_path);

    let mut snap = Snapshotter::new(db_path, "zero", None);
    snap.init().unwrap();
    // First advance: creates the second connection (prev = original).
    snap.advance_without_diff().unwrap();

    // A busy cursor on prev — the connection the NEXT advance will ROLLBACK.
    leak_busy_statement(&snap.prev_conn().unwrap());

    let result = snap.advance_without_diff().map(|v| v.to_string());
    assert!(
        result.is_ok(),
        "advance() must survive a busy statement on the prev connection \
         (reset stray cursors before ROLLBACK, like better-sqlite3 close). \
         Failing it orphans a permanently-pinned connection: {result:?}",
    );

    snap.destroy();
    clean_db(db_path);
}

/// Invariant 2 — THE ZOMBIE: after the snapshotter is done with a connection
/// (failed advance orphan-drop, or destroy), NO open read transaction may
/// survive, even when `sqlite3_close` fails on an unfinalized statement.
/// The read-mark (open txn) is the checkpoint pin; a leaked *handle* is
/// harmless, a leaked *transaction* grows the WAL at the write rate forever.
///
/// Pre-fix sequence being reproduced (prod pod hf2cg, linear 5.2GB WAL):
///   busy stmt on prev → advance → ROLLBACK fails → Err propagates → Snapshot
///   dropped → close() BUSY silently ignored → C-level connection leak with
///   read txn open → CG recovers on next advance with a fresh conn (logs look
///   healthy) → checkpoint busy FOREVER.
#[test]
fn no_drop_path_leaks_an_open_read_transaction() {
    let db_path = "/tmp/rust-ivm-zombie-pin-2.db";
    create_replica(db_path);

    let mut snap = Snapshotter::new(db_path, "zero", None);
    snap.init().unwrap();
    snap.advance_without_diff().unwrap();

    // Leak a busy cursor on prev, holding NO Rust-side references afterwards.
    leak_busy_statement(&snap.prev_conn().unwrap());

    // The advance that hits the busy statement. Pre-fix this is Err and the
    // prev Snapshot is dropped with its txn open; post-fix it succeeds. Either
    // way the invariant below must hold once the snapshotter is destroyed.
    let _ = snap.advance_without_diff();

    // CG-recovery advance (prev==None → fresh conn) — proves the snapshotter
    // itself looks healthy after the incident, exactly like the prod logs.
    snap.advance_without_diff()
        .expect("snapshotter must recover with a fresh connection");

    snap.destroy();

    // All snapshotter connections are gone. Any surviving read txn is a
    // zombie pin. Grow the WAL from a writer and demand a full checkpoint.
    let writer = Connection::open(db_path).unwrap();
    for i in 0..500 {
        writer
            .execute(
                "INSERT INTO users (id, name, _0_version) VALUES (?, 'w', 'v2')",
                [format!("w{i}")],
            )
            .unwrap();
    }
    let (busy, checkpointed) = checkpoint_truncate(&writer);
    assert_eq!(
        busy, 0,
        "ZOMBIE PIN: a snapshot connection was torn down with its read \
         transaction still open (sqlite3_close failure silently ignored). \
         This is the unbounded prod WAL-growth mechanism — checkpoint will \
         stay busy forever (checkpointed={checkpointed})",
    );

    clean_db(db_path);
}
