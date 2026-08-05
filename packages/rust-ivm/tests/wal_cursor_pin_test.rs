//! WAL-pin repro: a `LazyRows` read cursor (the engine's streaming fetch) holds
//! a SQLite read transaction open, which BLOCKS `wal_checkpoint(TRUNCATE)` on a
//! separate (litestream-style) connection — the mechanism behind the unbounded
//! WAL growth on preprod rust pods (84hwf 2.83 GiB / hf2cg 3.41 GiB, `wal`
//! metric). Once the cursor is dropped, the checkpoint reclaims the WAL.
//!
//! Faithful to production: the engine reads on ITS OWN connection (one per
//! syncer, opened by `set_db_path`), while checkpointing is driven by a
//! SEPARATE connection (in prod, the litestream replication writer). The
//! engine's open read cursor pins the snapshot so the separate checkpointer
//! cannot advance past it — WAL grows without bound.
//!
//! Deterministic, single-threaded. Run: cargo test --test wal_cursor_pin_test

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rusqlite::{Connection, params};

use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::stream::StreamItem;
use rust_ivm::sqlite::table_source::TableSource;

fn clean(path: &str) {
    for p in [
        path.to_string(),
        format!("{path}-wal"),
        format!("{path}-shm"),
    ] {
        let _ = std::fs::remove_file(p);
    }
}

fn wal_size(path: &str) -> u64 {
    std::fs::metadata(format!("{path}-wal"))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Returns (busy, log_frames, checkpointed_frames). `busy == 1` means the
/// checkpoint could NOT complete (a reader is pinning the WAL snapshot).
fn checkpoint_truncate(conn: &rusqlite::Connection) -> (i64, i64, i64) {
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    })
    .unwrap()
}

fn columns() -> HashMap<String, ColumnType> {
    let mut c = HashMap::new();
    c.insert("id".to_string(), ColumnType::Number { optional: false });
    c.insert("name".to_string(), ColumnType::String { optional: false });
    c
}

#[test]
fn lazyrows_cursor_pins_wal_and_blocks_checkpoint_until_dropped() {
    let db_path = "/tmp/rust-ivm-wal-cursor-pin.db";
    clean(db_path);

    // --- Seed a WAL-mode DB, then checkpoint so the WAL starts empty. ---
    {
        let c = rusqlite::Connection::open(db_path).unwrap();
        c.execute_batch("PRAGMA journal_mode=wal; PRAGMA synchronous=NORMAL;")
            .unwrap();
        c.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
            .unwrap();
        for i in 0..200 {
            c.execute(
                "INSERT INTO users (id, name) VALUES (?, ?)",
                params![i, format!("u{i}")],
            )
            .unwrap();
        }
        checkpoint_truncate(&c); // WAL now ~0
    }

    // --- Engine read path: open a streaming fetch on the source's OWN
    // connection and HOLD it partially consumed (one row stepped, not
    // finalized) — this is exactly what a streaming hydrate does while relaying
    // rows to JS across macrotasks. The cursor keeps a read transaction open. ---
    let read_conn = Rc::new(RefCell::new(Connection::open(db_path).unwrap()));
    let mut ts = TableSource::new(read_conn.clone(), "users", columns(), vec!["id".to_string()]);
    let input = ts.connect(None, None, None, None);
    let mut held_stream = input.borrow().fetch(&Default::default());
    // Step exactly one row: begins the read txn, does NOT finalize the cursor.
    let first = held_stream.next();
    assert!(
        matches!(first, Some(StreamItem::Data(_))),
        "expected to step one row and hold the cursor open",
    );

    // --- Separate litestream-style connection: write a load of frames (grow the
    // WAL), then try to TRUNCATE-checkpoint. ---
    let writer = rusqlite::Connection::open(db_path).unwrap();
    for i in 200..3000 {
        writer
            .execute(
                "INSERT INTO users (id, name) VALUES (?, ?)",
                params![i, format!("u{i}")],
            )
            .unwrap();
    }
    let (busy_held, _log, ckpt_held) = checkpoint_truncate(&writer);
    let wal_while_held = wal_size(db_path);

    // The held cursor pins the snapshot: TRUNCATE is BUSY and reclaims nothing.
    assert_eq!(
        busy_held, 1,
        "wal_checkpoint(TRUNCATE) must report BUSY while the LazyRows cursor pins the snapshot",
    );
    assert_eq!(
        ckpt_held, 0,
        "no frames may be checkpointed while a reader pins the WAL (got {ckpt_held})",
    );
    assert!(
        wal_while_held > 0,
        "WAL must remain un-truncated while the cursor is held (size={wal_while_held})",
    );

    // --- Release the cursor (finalize the read txn). ---
    drop(held_stream);

    // Now the same checkpointer reclaims the WAL.
    let (busy_free, _log2, _ckpt_free) = checkpoint_truncate(&writer);
    let wal_after = wal_size(db_path);

    assert_eq!(
        busy_free, 0,
        "wal_checkpoint(TRUNCATE) must succeed once the cursor is dropped",
    );
    assert!(
        wal_after < wal_while_held,
        "WAL must shrink after the cursor is released (held={wal_while_held} after={wal_after})",
    );

    clean(db_path);
}
