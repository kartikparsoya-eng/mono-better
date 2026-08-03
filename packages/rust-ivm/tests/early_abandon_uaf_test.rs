//! Drops a `TableSource` fetch stream BEFORE it is exhausted.
//!
//! `rusqlite::Rows` only clears its `&Statement` when the cursor runs to
//! completion (`advance()` -> `Ok(false)` -> `reset()`). A stream abandoned
//! early still holds that reference, so `Rows::drop` -> `reset()` dereferences
//! the statement. If `LazyRows` declares `_stmt` before `rows`, Rust drops the
//! boxed statement FIRST and that deref touches freed memory.
//!
//! Run under Guard Malloc to turn the use-after-free into a hard SIGSEGV:
//!   DYLD_INSERT_LIBRARIES=/usr/lib/libgmalloc.dylib \
//!   cargo test --release --test early_abandon_uaf_test

use rusqlite::Connection;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::sqlite::table_source::TableSource;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

#[test]
fn abandoning_a_fetch_early_is_sound() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT);")
        .unwrap();
    for i in 0..500 {
        conn.execute(
            "INSERT INTO t (id,a) VALUES (?,?)",
            [&format!("id{i}"), &format!("a{i}")],
        )
        .unwrap();
    }

    let columns: HashMap<String, ColumnType> = ["id", "a"]
        .into_iter()
        .map(|c| (c.to_string(), ColumnType::String { optional: false }))
        .collect();

    let conn = Rc::new(RefCell::new(conn));
    let mut src = TableSource::new(conn, "t", columns, vec!["id".to_string()]);
    let input = src.connect(None, None, None, None);

    // Repeatedly start a 500-row scan and walk only one row, then drop the
    // stream. Each drop exercises Rows::drop -> reset() on a live statement.
    for _ in 0..200 {
        let stream = input.borrow().fetch(&Default::default());
        let mut it = rust_ivm::ivm::stream::skip_yields(stream);
        let first = it.next();
        assert!(first.is_some(), "expected at least one row");
        drop(it); // abandoned with 499 rows still pending
    }
}
