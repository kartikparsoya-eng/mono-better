//! Tests for `Statement::run` / `get` / `all` and a few `Database` accessors —
//! port of the `Statement` and `Database` methods in `zqlite/src/db.ts`
//! (exercised by `zqlite/src/db.test.ts`). db_test.rs covers the KV storage
//! layer; the prepared-statement run/get/all path and name/page_size/close were
//! untested (triage #18).

use rusqlite::types::Value;

use rust_ivm::sqlite::db::{Database, Statement};

fn setup() -> Database {
    let db = Database::in_memory().expect("open in-memory db");
    db.exec("CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER)")
        .expect("create table");
    db
}

// Port of TS `Statement.run`: executes with params, returns rows-affected.
#[test]
fn statement_run_inserts_and_reports_rows_affected() {
    let db = setup();
    let insert = Statement::new(db.conn(), "INSERT INTO kv (k, v) VALUES (?, ?)").expect("prepare");

    let n1 = insert.run(&[&"a", &1i64]).expect("run a");
    let n2 = insert.run(&[&"b", &2i64]).expect("run b");
    assert_eq!(n1, 1, "one row affected");
    assert_eq!(n2, 1, "one row affected");
}

// Port of TS `Statement.get`: returns the first row as a column->value map, or
// None when there is no matching row.
#[test]
fn statement_get_returns_first_row_or_none() {
    let db = setup();
    let insert = Statement::new(db.conn(), "INSERT INTO kv (k, v) VALUES (?, ?)").expect("prepare");
    insert.run(&[&"a", &1i64]).expect("run a");
    insert.run(&[&"b", &2i64]).expect("run b");

    let sel = Statement::new(db.conn(), "SELECT v FROM kv WHERE k = ?").expect("prepare select");

    let hit = sel.get(&[&"a"]).expect("get a").expect("row a present");
    assert_eq!(hit.get("v"), Some(&Value::Integer(1)));

    let miss = sel.get(&[&"zzz"]).expect("get zzz");
    assert!(miss.is_none(), "no matching row => None");
}

// Port of TS `Statement.all`: returns every matching row as a list of maps.
#[test]
fn statement_all_returns_every_row() {
    let db = setup();
    let insert = Statement::new(db.conn(), "INSERT INTO kv (k, v) VALUES (?, ?)").expect("prepare");
    insert.run(&[&"a", &1i64]).expect("run a");
    insert.run(&[&"b", &2i64]).expect("run b");
    insert.run(&[&"c", &3i64]).expect("run c");

    let sel =
        Statement::new(db.conn(), "SELECT k, v FROM kv ORDER BY k ASC").expect("prepare select");
    let rows = sel.all(&[]).expect("all");

    assert_eq!(rows.len(), 3);
    // Ordered by k asc: a=1, b=2, c=3.
    assert_eq!(rows[0].get("k"), Some(&Value::Text("a".to_string())));
    assert_eq!(rows[0].get("v"), Some(&Value::Integer(1)));
    assert_eq!(rows[2].get("k"), Some(&Value::Text("c".to_string())));
    assert_eq!(rows[2].get("v"), Some(&Value::Integer(3)));
}

// Database accessors: an in-memory db reports its name and a positive page size,
// and closes cleanly.
#[test]
fn database_name_page_size_and_close() {
    let db = setup();
    assert!(!db.name().is_empty(), "in-memory db has a name");
    assert!(db.page_size() > 0, "page size is positive");
    db.close().expect("close cleanly");
}
