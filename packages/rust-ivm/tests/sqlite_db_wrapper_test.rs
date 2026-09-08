//! Pins the `sqlite/db.rs` Database/Statement wrapper — flagged by the L2
//! triage as nearly untouched (18/22 fns FNDA=0; TS twin `zqlite/src/db.ts`
//! with `db.test.ts`). These wrappers sit under every syncer SQLite read, so
//! a drifted return shape (missing column, wrong lossy coercion, get vs all
//! semantics) would corrupt query results rather than fail loudly.

use rusqlite::types::Value as SqlValue;
use rust_ivm::sqlite::db::{Database, Statement};

fn make_db() -> Database {
    let db = Database::in_memory().expect("in-memory db");
    db.exec(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, score REAL);\
         INSERT INTO t VALUES (1, 'a', 1.5);\
         INSERT INTO t VALUES (2, 'b', 2.5);\
         INSERT INTO t VALUES (3, NULL, 3.5);",
    )
    .expect("seed");
    db
}

/// `Statement::get` = TS `stmt.get()`: FIRST row as a column-name map, or
/// None on empty result; NULL columns come through as SQL NULL, not absent.
#[test]
fn statement_get_returns_first_row_or_none() {
    let db = make_db();
    let stmt = Statement::new(db.conn(), "SELECT * FROM t WHERE id >= ?1 ORDER BY id").unwrap();

    let row = stmt.get(&[&2i64]).unwrap().expect("row exists");
    assert_eq!(row.get("id"), Some(&SqlValue::Integer(2)));
    assert_eq!(row.get("name"), Some(&SqlValue::Text("b".into())));
    assert_eq!(row.get("score"), Some(&SqlValue::Real(2.5)));

    assert!(stmt.get(&[&99i64]).unwrap().is_none(), "no match → None");

    // NULL column present in the map as Null (TS get() keeps the key).
    let row = stmt.get(&[&3i64]).unwrap().expect("row 3");
    assert_eq!(row.get("name"), Some(&SqlValue::Null));
}

/// `Statement::all` = TS `stmt.all()`: every row, in query order; empty
/// result is an empty Vec, not an error.
#[test]
fn statement_all_returns_every_row_in_order() {
    let db = make_db();
    let stmt = Statement::new(db.conn(), "SELECT id FROM t WHERE id >= ?1 ORDER BY id").unwrap();

    let rows = stmt.all(&[&2i64]).unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| r.get("id").cloned().unwrap())
            .collect::<Vec<_>>(),
        vec![SqlValue::Integer(2), SqlValue::Integer(3)]
    );
    assert!(stmt.all(&[&99i64]).unwrap().is_empty());
}

/// `Statement::run` = TS `stmt.run()`: returns the affected-row count; the
/// statement is re-runnable with fresh params.
#[test]
fn statement_run_returns_change_count_and_is_reusable() {
    let db = make_db();
    let ins = Statement::new(db.conn(), "INSERT INTO t VALUES (?1, ?2, ?3)").unwrap();
    assert_eq!(ins.run(&[&10i64, &"x", &0.5f64]).unwrap(), 1);
    assert_eq!(ins.run(&[&11i64, &"y", &0.6f64]).unwrap(), 1);

    let upd = Statement::new(db.conn(), "UPDATE t SET score = 0 WHERE id >= ?1").unwrap();
    assert_eq!(upd.run(&[&10i64]).unwrap(), 2, "both inserted rows updated");
}

/// Invalid SQL surfaces as Err at prepare time — TS `db.prepare` throws.
#[test]
fn statement_new_rejects_invalid_sql() {
    let db = make_db();
    assert!(Statement::new(db.conn(), "SELEC nope FROM t").is_err());
}

/// Database plumbing: `exec` batch DDL, pragma readers (string + int),
/// `name()` on an in-memory db, and `page_size` consistency with PRAGMA.
#[test]
fn database_exec_and_pragma_readers() {
    let db = make_db();

    let journal = db.pragma_query_value_string("journal_mode").unwrap();
    assert!(!journal.is_empty());
    let page = db.pragma_query_value_int("page_size").unwrap();
    assert!(page > 0);
    assert_eq!(db.page_size() as i64, page, "page_size() mirrors PRAGMA");
    assert!(
        db.exec("this is not sql").is_err(),
        "exec propagates errors"
    );
}

/// `compact` with a huge threshold is a no-op success (nothing freeable);
/// `close` succeeds on a quiescent db. Both are the maintenance arms the
/// syncer calls between serving windows — they must never poison the handle.
#[test]
fn database_compact_and_close() {
    let db = make_db();
    db.compact(usize::MAX).expect("no-op compact");
    // Free some pages then compact with threshold 0 (always eligible).
    db.exec("DELETE FROM t").unwrap();
    db.compact(0).expect("compact after delete");
    db.close().expect("close");
}
