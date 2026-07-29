//! Tests for the SQLite database wrapper and database storage.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::operator::Storage;
use rust_ivm::sqlite::db::Database;
use rust_ivm::sqlite::database_storage::{
    ClientGroupStorage, CREATE_STORAGE_TABLE,
};

#[test]
fn test_database_in_memory() {
    let db = Database::in_memory().expect("Failed to open in-memory database");
    db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
        .expect("Failed to create table");
    db.exec("INSERT INTO test VALUES (1, 'hello')")
        .expect("Failed to insert");

    let conn = db.conn();
    let conn = conn.borrow();
    let result: String = conn
        .query_row("SELECT name FROM test WHERE id = 1", [], |row| row.get(0))
        .expect("Failed to query");
    assert_eq!(result, "hello");
}

#[test]
fn test_database_pragma() {
    let db = Database::in_memory().expect("Failed to open database");
    // Verify case_sensitive_like is ON: 'A' LIKE 'a' should return false.
    let result: i64 = db
        .conn()
        .borrow()
        .query_row("SELECT 'A' LIKE 'a'", [], |row| row.get(0))
        .expect("Failed to query LIKE");
    assert_eq!(result, 0);
}

#[test]
fn test_database_storage_create() {
    let db = Database::in_memory().expect("Failed to open database");
    db.exec("PRAGMA journal_mode = OFF").expect("pragma");
    db.exec("PRAGMA synchronous = OFF").expect("pragma");
    db.exec(CREATE_STORAGE_TABLE).expect("Failed to create storage table");

    let conn = db.conn();
    let conn = conn.borrow();
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='storage'",
            [],
            |row| row.get(0),
        )
        .expect("Failed to query");
    assert_eq!(count, 1);
}

#[test]
fn test_database_storage_set_get_del() {
    let db = Database::in_memory().expect("Failed to open database");
    db.exec(CREATE_STORAGE_TABLE).expect("create table");

    let cg = ClientGroupStorage::new(Rc::new(RefCell::new(db)), "test-cg".to_string());
    let storage = cg.create_storage();

    storage.borrow_mut().set("mykey".to_string(), Value::Str("myvalue".into()));

    let result = storage.borrow().get("mykey");
    assert!(result.is_some());
    match result {
        Some(Value::Str(s)) => assert_eq!(s, Arc::from("myvalue")),
        _ => panic!("Expected string value"),
    }

    storage.borrow_mut().del("mykey");
    let result = storage.borrow().get("mykey");
    assert!(result.is_none());
}

#[test]
fn test_database_storage_scan() {
    let db = Database::in_memory().expect("Failed to open database");
    db.exec(CREATE_STORAGE_TABLE).expect("create table");

    let cg = ClientGroupStorage::new(Rc::new(RefCell::new(db)), "test-cg".to_string());
    let storage = cg.create_storage();

    storage.borrow_mut().set("alpha".to_string(), Value::F64(1.0));
    storage.borrow_mut().set("beta".to_string(), Value::F64(2.0));
    storage.borrow_mut().set("gamma".to_string(), Value::F64(3.0));

    let all = storage.borrow().scan(None);
    assert_eq!(all.len(), 3);

    let filtered = storage.borrow().scan(Some("b"));
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].0, "beta");
}

#[test]
fn test_database_storage_number_values() {
    let db = Database::in_memory().expect("Failed to open database");
    db.exec(CREATE_STORAGE_TABLE).expect("create table");

    let cg = ClientGroupStorage::new(Rc::new(RefCell::new(db)), "test-cg".to_string());
    let storage = cg.create_storage();

    storage.borrow_mut().set("count".to_string(), Value::F64(42.0));
    let result = storage.borrow().get("count");
    match result {
        Some(Value::F64(n)) => assert_eq!(n, 42.0),
        _ => panic!("Expected number value"),
    }
}

#[test]
fn test_database_storage_bool_values() {
    let db = Database::in_memory().expect("Failed to open database");
    db.exec(CREATE_STORAGE_TABLE).expect("create table");

    let cg = ClientGroupStorage::new(Rc::new(RefCell::new(db)), "test-cg".to_string());
    let storage = cg.create_storage();

    storage.borrow_mut().set("flag".to_string(), Value::Bool(true));
    let result = storage.borrow().get("flag");
    match result {
        Some(Value::Bool(b)) => assert!(b),
        _ => panic!("Expected bool value"),
    }
}
