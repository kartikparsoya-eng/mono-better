//! BUG 5-TORN regression: a torn/corrupt SQLite read must NOT be silently
//! swallowed (truncating the result), and a cancellation (SQLITE_INTERRUPT)
//! must be a CLEAN stop — not misreported as a "row read error".
//!
//! TS reference (`packages/zqlite/src/table-source.ts` `#mapFromSQLiteTypes`,
//! line 377): `rowIterator.next()` is called with NO try/catch. Any step error
//! THROWS and propagates out of `#fetch`, aborting the pipeline → view-syncer
//! teardown → client rehydrate. TS never truncates the stream on a read error.
//! Cancellation there is driver-level (the caller stops pulling).
//!
//! The Rust `TableSource` row iterator (src/sqlite/table_source.rs) previously
//! logged the error and returned `None` — silently truncating for BOTH
//! corruption and interrupt. The fix classifies the error:
//!  - SQLITE_INTERRUPT  → quiet clean stop (cancellation; the cancel path owns
//!    teardown).
//!  - SQLITE_CORRUPT/other → panic (propagate); the napi `catch_unwind`
//!    surfaces it as a thrown error, matching TS.
//!
//! These tests drive the PUBLIC `Input::fetch` path over a real SQLite DB:
//!  - `corrupt_read_propagates_not_truncates`: a real on-disk corruption makes
//!    draining the fetch stream PANIC (fix) instead of returning a truncated
//!    row count (pre-fix bug).
//!  - `interrupt_is_clean_stop_not_panic`: a cross-thread `.interrupt()` mid
//!    drain stops the stream WITHOUT panicking (clean cancellation).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rust_ivm::ivm::operator::{FetchRequest, Input};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::stream::StreamItem;
use rust_ivm::sqlite::install_interrupt;
use rust_ivm::sqlite::table_source::TableSource;

fn cleanup(path: &str) {
    for p in [
        path.to_string(),
        format!("{}-wal", path),
        format!("{}-shm", path),
        format!("{}-journal", path),
    ] {
        let _ = std::fs::remove_file(&p);
    }
}

fn columns() -> HashMap<String, ColumnType> {
    [
        ("id".to_string(), ColumnType::Number { optional: false }),
        ("v".to_string(), ColumnType::String { optional: false }),
    ]
    .into_iter()
    .collect()
}

/// Build a DELETE-journal DB with `n` rows, then corrupt a mid-file page so a
/// full `SELECT * ORDER BY id` reads some rows and then hits SQLITE_CORRUPT
/// ("database disk image is malformed") — the exact torn-read symptom.
fn create_and_corrupt(path: &str, n: i64) {
    cleanup(path);
    {
        let c = rusqlite::Connection::open(path).unwrap();
        c.execute_batch(
            "PRAGMA journal_mode=DELETE; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);",
        )
        .unwrap();
        for i in 0..n {
            c.execute(
                "INSERT INTO t(id,v) VALUES(?1,?2)",
                rusqlite::params![i, format!("val{}", i)],
            )
            .unwrap();
        }
    }
    // Overwrite the body of every page from page 3 onward with 0xFF, leaving
    // the header page (page 1) intact so the DB still opens and the corruption
    // surfaces mid-scan rather than at open time. The page size is read from the
    // header (bytes 16..18, big-endian) so this is robust to the vendored
    // SQLite build's default page size.
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut hdr = [0u8; 18];
    f.seek(SeekFrom::Start(0)).unwrap();
    f.read_exact(&mut hdr).unwrap();
    let page_size: u64 = match u16::from_be_bytes([hdr[16], hdr[17]]) {
        1 => 65536, // SQLite encodes a 64K page size as the value 1.
        n => n as u64,
    };
    let len = f.metadata().unwrap().len();
    // Start at page 3 (skip header page 1 + the schema/root region on page 2)
    // and clobber the rest of the file — guarantees a corrupt data/leaf page in
    // the scan path regardless of page size.
    let start = 2 * page_size;
    if start < len {
        let garbage = vec![0xFFu8; (len - start) as usize];
        f.seek(SeekFrom::Start(start)).unwrap();
        f.write_all(&garbage).unwrap();
    }
    f.flush().unwrap();
}

/// The table named "t" — build a TableSource over the given connection and
/// connect a default (PK-sorted) input.
fn table_input(conn: Rc<RefCell<rusqlite::Connection>>) -> Rc<RefCell<dyn Input>> {
    let mut src = TableSource::new(conn, "t", columns(), vec!["id".to_string()]);
    src.connect(None, None, None, None, None)
}

#[test]
fn corrupt_read_propagates_not_truncates() {
    let path = "/tmp/rust-ivm-torn-corrupt.db";
    create_and_corrupt(path, 2000);

    let conn = Rc::new(RefCell::new(rusqlite::Connection::open(path).unwrap()));
    let input = table_input(conn);

    // Draining the full-scan stream must PANIC on the corrupt page rather than
    // silently truncate. Pre-fix this returned a partial count and NO panic.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let inp = input.borrow();
        let stream = inp.fetch(&FetchRequest::default());
        let mut count = 0usize;
        for item in stream {
            if let StreamItem::Data(_) = item {
                count += 1;
            }
        }
        count
    }));

    cleanup(path);

    match result {
        Ok(count) => panic!(
            "torn/corrupt read was SWALLOWED (truncated to {} rows) instead of \
             propagating — this is the correctness leak BUG 5-TORN fixes",
            count
        ),
        Err(payload) => {
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            assert!(
                msg.contains("row read error") && msg.contains("malformed"),
                "expected propagated corrupt read-error panic, got: {msg}"
            );
        }
    }
}

#[test]
fn interrupt_is_clean_stop_not_panic() {
    let path = "/tmp/rust-ivm-torn-interrupt.db";
    cleanup(path);
    {
        let c = rusqlite::Connection::open(path).unwrap();
        c.execute_batch(
            "PRAGMA journal_mode=DELETE; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);",
        )
        .unwrap();
        // Enough rows that the scan is still in flight when we interrupt.
        let tx = c.unchecked_transaction().unwrap();
        for i in 0..200_000 {
            tx.execute(
                "INSERT INTO t(id,v) VALUES(?1,?2)",
                rusqlite::params![i, format!("val{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    let raw = rusqlite::Connection::open(path).unwrap();
    // Grab a Send+Sync interrupt handle BEFORE the fetch borrows the connection.
    let handle = install_interrupt(&raw);
    let conn = Rc::new(RefCell::new(raw));
    let input = table_input(conn);

    // Fire the interrupt from another thread shortly after we start draining.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2 = stop.clone();
    let interrupter = std::thread::spawn(move || {
        // Spin briefly so the main-thread scan is in flight, then interrupt
        // repeatedly until the drain signals completion.
        while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
            handle.interrupt();
            std::thread::sleep(std::time::Duration::from_micros(200));
        }
    });

    // Exercise the race repeatedly. The interrupter can land during prepare,
    // bind/query setup, or stepping; every phase must classify it as the same
    // clean cancellation instead of occasionally panicking.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        for _ in 0..32 {
            let inp = input.borrow();
            let stream = inp.fetch(&FetchRequest::default());
            for item in stream {
                if let StreamItem::Data(_) = item {
                    // Keep pulling until SQLite observes an interrupt.
                }
            }
        }
    }));

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = interrupter.join();
    cleanup(path);

    assert!(
        result.is_ok(),
        "SQLITE_INTERRUPT (cancellation) must be a CLEAN stop, not a panic; \
         got a propagated error: {:?}",
        result.err().map(|p| p
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default())
    );
}
