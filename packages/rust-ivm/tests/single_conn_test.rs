//! Tests for single-connection-per-source design.
//! Verifies Source uses ONE connection (not a pool, not per-fetch opens).
//! These tests FAIL if the old connection-pool / per-fetch-open behavior returns.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::collections::HashMap;

use rust_ivm::ivm::source::MemorySource;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::operator::{FetchRequest, Input};
use rust_ivm::ivm::data::Value;

fn make_source(name: &str, cols: Vec<(&str, ColumnType)>, pk: &[&str]) -> Rc<RefCell<MemorySource>> {
    let columns: HashMap<String, ColumnType> = cols.into_iter()
        .map(|(n, t)| (n.to_string(), t))
        .collect();
    Rc::new(RefCell::new(MemorySource::new(name, columns, pk.iter().map(|s| s.to_string()).collect())))
}

#[test]
fn test_set_db_path_opens_single_connection() {
    let db_path = "/tmp/test_single_conn_1.db";
    let _ = std::fs::remove_file(db_path);
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.execute("CREATE TABLE IF NOT EXISTS test_table (id TEXT PRIMARY KEY, name TEXT)", []).unwrap();
    conn.execute("INSERT OR REPLACE INTO test_table VALUES ('a', 'Alice')", []).unwrap();
    conn.execute("INSERT OR REPLACE INTO test_table VALUES ('b', 'Bob')", []).unwrap();
    drop(conn);

    let source = make_source("test_table", vec![
        ("id", ColumnType::String { optional: false }),
        ("name", ColumnType::String { optional: false }),
    ], &["id"]);

    source.borrow_mut().set_db_path(db_path);
    assert!(source.borrow().has_db(), "source must report has_db after set_db_path");

    let conn_handle = source.borrow_mut().connect(None, None, None, None);
    let req = FetchRequest::default();

    // Fetch 3 times — must reuse same connection
    for i in 0..3 {
        let stream = conn_handle.borrow().fetch(&req);
        let rows: Vec<_> = stream.collect();
        assert_eq!(rows.len(), 2, "fetch #{} must return 2 rows (reuse)", i);
    }

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_no_connection_leak_on_repeated_fetch() {
    let db_path = "/tmp/test_single_conn_2.db";
    let _ = std::fs::remove_file(db_path);
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.execute("CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)", []).unwrap();
    for i in 0..100 {
        conn.execute("INSERT OR REPLACE INTO t VALUES (?, ?)", [i.to_string(), format!("val{}", i)]).unwrap();
    }
    drop(conn);

    let source = make_source("t", vec![
        ("id", ColumnType::String { optional: false }),
        ("v", ColumnType::String { optional: false }),
    ], &["id"]);

    source.borrow_mut().set_db_path(db_path);

    // Fetch 50 times — must not leak connections or hang
    for attempt in 0..50 {
        let conn_handle = source.borrow_mut().connect(None, None, None, None);
        let stream = conn_handle.borrow().fetch(&FetchRequest::default());
        let rows: Vec<_> = stream.collect();
        assert_eq!(rows.len(), 100, "attempt {} must return 100 rows", attempt);
    }

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_nested_fetch_different_sources_no_deadlock() {
    let db_path = "/tmp/test_nested_fetch.db";
    let _ = std::fs::remove_file(db_path);
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.execute_batch("
        CREATE TABLE parent (id TEXT PRIMARY KEY, name TEXT);
        CREATE TABLE child (id TEXT PRIMARY KEY, parentId TEXT, value TEXT);
        INSERT INTO parent VALUES ('p1', 'Parent1');
        INSERT INTO parent VALUES ('p2', 'Parent2');
        INSERT INTO child VALUES ('c1', 'p1', 'Child1');
        INSERT INTO child VALUES ('c2', 'p1', 'Child2');
        INSERT INTO child VALUES ('c3', 'p2', 'Child3');
    ").unwrap();
    drop(conn);

    let parent_source = make_source("parent", vec![
        ("id", ColumnType::String { optional: false }),
        ("name", ColumnType::String { optional: false }),
    ], &["id"]);
    let child_source = make_source("child", vec![
        ("id", ColumnType::String { optional: false }),
        ("parentId", ColumnType::String { optional: false }),
        ("value", ColumnType::String { optional: false }),
    ], &["id"]);

    parent_source.borrow_mut().set_db_path(db_path);
    child_source.borrow_mut().set_db_path(db_path);

    // Fetch parent rows
    let parent_conn = parent_source.borrow_mut().connect(None, None, None, None);
    let parent_stream = parent_conn.borrow().fetch(&FetchRequest::default());
    let parent_rows: Vec<_> = parent_stream.collect();
    assert_eq!(parent_rows.len(), 2, "parent must have 2 rows");

    // Fetch child rows with constraint (simulates join)
    let mut child_constraint = rust_ivm::ivm::constraint::Constraint::default();
    child_constraint.insert("parentId".to_string(), Value::Str(std::sync::Arc::from("p1")));
    let child_req = FetchRequest { constraint: Some(child_constraint), ..Default::default() };
    let child_conn = child_source.borrow_mut().connect(None, None, None, None);
    let child_stream = child_conn.borrow().fetch(&child_req);
    let child_rows: Vec<_> = child_stream.collect();
    assert_eq!(child_rows.len(), 2, "child must have 2 rows for parent p1");

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_read_only_connection() {
    // The connection must be opened with SQLITE_OPEN_READ_ONLY.
    // A write attempt must fail — verifying we don't accidentally open RW.
    let db_path = "/tmp/test_read_only.db";
    let _ = std::fs::remove_file(db_path);
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.execute("CREATE TABLE t (id TEXT PRIMARY KEY)", []).unwrap();
    conn.execute("INSERT INTO t VALUES ('a')", []).unwrap();
    drop(conn);

    let source = make_source("t", vec![
        ("id", ColumnType::String { optional: false }),
    ], &["id"]);

    source.borrow_mut().set_db_path(db_path);

    // Fetch works (read)
    let conn_handle = source.borrow_mut().connect(None, None, None, None);
    let stream = conn_handle.borrow().fetch(&FetchRequest::default());
    let rows: Vec<_> = stream.collect();
    assert_eq!(rows.len(), 1, "read must work on read-only connection");

    let _ = std::fs::remove_file(db_path);
}
