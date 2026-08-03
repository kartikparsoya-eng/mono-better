//! Integration test: MemorySource with SQLite-backed on-demand fetch.
//!
//! Creates a temp SQLite database, sets it on MemorySource, and verifies
//! that fetch() queries SQLite correctly — no preloading needed.
//!
//! Run: cargo test --test sqlite_fetch_test -- --test-threads=1

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rust_ivm::builder::ast::Ast;
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::change::{
    make_source_change_add, make_source_change_edit, make_source_change_remove,
};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;

fn clean_db(path: &str) {
    for p in [path, &format!("{}-wal", path), &format!("{}-shm", path)] {
        let _ = std::fs::remove_file(p);
    }
}

fn create_test_db(path: &str) {
    clean_db(path);
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch("PRAGMA journal_mode = wal; PRAGMA synchronous = NORMAL;")
        .unwrap();
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER,
            active INTEGER DEFAULT 1
        );",
    )
    .unwrap();
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY,
            userId INTEGER NOT NULL,
            title TEXT NOT NULL,
            body TEXT
        );",
    )
    .unwrap();

    // Insert test data
    let users = [
        (1, "Alice", "alice@example.com", 30, 1),
        (2, "Bob", "bob@example.com", 25, 1),
        (3, "Charlie", "charlie@example.com", 35, 0),
        (4, "Diana", "diana@example.com", 28, 1),
        (5, "Eve", "eve@example.com", 40, 1),
    ];
    for (id, name, email, age, active) in users {
        conn.execute(
            "INSERT OR REPLACE INTO users (id, name, email, age, active) VALUES (?, ?, ?, ?, ?)",
            rusqlite::params![id, name, email, age, active],
        )
        .unwrap();
    }

    let posts = [
        (1, 1, "Hello World", "My first post"),
        (2, 1, "Second Post", "Another post"),
        (3, 2, "Bob Post", "Bob content"),
        (4, 3, "Charlie Post", "Charlie content"),
        (5, 1, "Third Post", "Alice third"),
    ];
    for (id, user_id, title, body) in posts {
        conn.execute(
            "INSERT OR REPLACE INTO posts (id, userId, title, body) VALUES (?, ?, ?, ?)",
            rusqlite::params![id, user_id, title, body],
        )
        .unwrap();
    }
}

fn make_columns() -> HashMap<String, ColumnType> {
    let mut columns = HashMap::new();
    columns.insert("id".to_string(), ColumnType::Number { optional: false });
    columns.insert("name".to_string(), ColumnType::String { optional: false });
    columns.insert("email".to_string(), ColumnType::String { optional: false });
    columns.insert("age".to_string(), ColumnType::Number { optional: false });
    columns.insert("active".to_string(), ColumnType::Number { optional: false });
    columns
}

fn make_source(
    table: &str,
    columns: HashMap<String, ColumnType>,
    pk: Vec<String>,
) -> Rc<RefCell<MemorySource>> {
    Rc::new(RefCell::new(MemorySource::new(table, columns, pk)))
}

#[test]
fn test_sqlite_fetch_returns_all_rows() {
    let db_path = "/tmp/rust-ivm-test-fetch.db";
    create_test_db(db_path);

    let source = make_source("users", make_columns(), vec!["id".to_string()]);
    source.borrow_mut().set_db_path(db_path);

    let input = source.borrow_mut().connect(None, None, None, None);
    let stream = input.borrow().fetch(&Default::default());
    let rows: Vec<_> = stream.collect();

    assert_eq!(rows.len(), 5, "Should return 5 rows from SQLite");
    assert!(
        rows.iter().all(|r| match r {
            rust_ivm::ivm::stream::StreamItem::Data(n) => n.row.contains_key("id"),
            _ => false,
        }),
        "All rows should have id"
    );
}

#[test]
fn test_sqlite_fetch_with_order_by() {
    let db_path = "/tmp/rust-ivm-test-order.db";
    create_test_db(db_path);

    let source = make_source("users", make_columns(), vec!["id".to_string()]);
    source.borrow_mut().set_db_path(db_path);

    let sort = Arc::new(vec![["age".to_string(), "desc".to_string()]]);
    let input = source.borrow_mut().connect(Some(sort), None, None, None);
    let stream = input.borrow().fetch(&Default::default());
    let rows: Vec<_> = stream.collect();

    assert_eq!(rows.len(), 5, "Should return 5 rows");
    // First row should have highest age (Eve, 40)
    let first_age = match &rows[0] {
        rust_ivm::ivm::stream::StreamItem::Data(n) => n.row.get("age"),
        _ => None,
    };
    assert_eq!(
        *first_age.unwrap(),
        Value::F64(40.0),
        "First row should be Eve (age 40)"
    );
}

#[test]
fn test_sqlite_fetch_with_constraint() {
    let db_path = "/tmp/rust-ivm-test-constraint.db";
    create_test_db(db_path);

    let source = make_source("users", make_columns(), vec!["id".to_string()]);
    source.borrow_mut().set_db_path(db_path);

    let mut constraint = rustc_hash::FxHashMap::default();
    constraint.insert("active".to_string(), Value::F64(1.0));

    let req = rust_ivm::ivm::operator::FetchRequest {
        constraint: Some(constraint),
        multi_constraints: vec![],
        start: None,
        reverse: false,
        limit: None,
    };

    let input = source.borrow_mut().connect(None, None, None, None);
    let stream = input.borrow().fetch(&req);
    let rows: Vec<_> = stream.collect();

    assert_eq!(rows.len(), 4, "Should return 4 active users");
}

#[test]
fn test_sqlite_engine_add_queries() {
    let db_path = "/tmp/rust-ivm-test-engine.db";
    create_test_db(db_path);

    let mut engine = Engine::new(HashMap::new());

    let source = make_source("users", make_columns(), vec!["id".to_string()]);
    engine.register_source(source.clone());

    source.borrow_mut().set_db_path(db_path);

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: Some(vec![rust_ivm::builder::ast::OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].changes.len(),
        5,
        "Query should return 5 row changes"
    );

    // Verify all changes are "add" type
    for rc in &results[0].changes {
        assert_eq!(rc.change_type, rust_ivm::ivm::change::ChangeType::Add);
        assert_eq!(rc.table, "users");
    }
}

#[test]
fn test_sqlite_engine_add_queries_with_limit() {
    let db_path = "/tmp/rust-ivm-test-limit.db";
    create_test_db(db_path);

    let mut engine = Engine::new(HashMap::new());

    let source = make_source("users", make_columns(), vec!["id".to_string()]);
    engine.register_source(source.clone());

    source.borrow_mut().set_db_path(db_path);

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: Some(2),
        order_by: Some(vec![rust_ivm::builder::ast::OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(results[0].changes.len(), 2, "LIMIT 2 should return 2 rows");
}

#[test]
fn test_sqlite_engine_advance_add() {
    let db_path = "/tmp/rust-ivm-test-advance-add.db";
    create_test_db(db_path);

    let mut engine = Engine::new(HashMap::new());

    let source = make_source("users", make_columns(), vec!["id".to_string()]);
    engine.register_source(source.clone());

    source.borrow_mut().set_db_path(db_path);

    // Add initial query
    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: Some(vec![rust_ivm::builder::ast::OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    };
    engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);

    // Advance with a new user
    let new_row = Arc::new(rustc_hash::FxHashMap::from_iter([
        ("id".to_string(), Value::F64(6.0)),
        ("name".to_string(), Value::Str(Arc::from("Frank"))),
        (
            "email".to_string(),
            Value::Str(Arc::from("frank@example.com")),
        ),
        ("age".to_string(), Value::F64(50.0)),
        ("active".to_string(), Value::F64(1.0)),
    ]));

    let changes = engine.advance(&[("users".to_string(), make_source_change_add(new_row))]);

    assert!(
        !changes.is_empty(),
        "Advance should produce changes for new user"
    );
}

#[test]
fn test_sqlite_engine_advance_edit() {
    let db_path = "/tmp/rust-ivm-test-advance-edit.db";
    create_test_db(db_path);

    let mut engine = Engine::new(HashMap::new());

    let source = make_source("users", make_columns(), vec!["id".to_string()]);
    engine.register_source(source.clone());

    source.borrow_mut().set_db_path(db_path);

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: Some(vec![rust_ivm::builder::ast::OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    };
    engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);

    let old_row = Arc::new(rustc_hash::FxHashMap::from_iter([
        ("id".to_string(), Value::F64(1.0)),
        ("name".to_string(), Value::Str(Arc::from("Alice"))),
        (
            "email".to_string(),
            Value::Str(Arc::from("alice@example.com")),
        ),
        ("age".to_string(), Value::F64(30.0)),
        ("active".to_string(), Value::F64(1.0)),
    ]));
    let new_row = Arc::new(rustc_hash::FxHashMap::from_iter([
        ("id".to_string(), Value::F64(1.0)),
        ("name".to_string(), Value::Str(Arc::from("Alice Updated"))),
        (
            "email".to_string(),
            Value::Str(Arc::from("alice@example.com")),
        ),
        ("age".to_string(), Value::F64(31.0)),
        ("active".to_string(), Value::F64(1.0)),
    ]));

    let changes = engine.advance(&[(
        "users".to_string(),
        make_source_change_edit(new_row, old_row),
    )]);

    // Edit should produce changes
    assert!(!changes.is_empty(), "Edit advance should produce changes");
}

#[test]
fn test_sqlite_engine_advance_remove() {
    let db_path = "/tmp/rust-ivm-test-advance-remove.db";
    create_test_db(db_path);

    let mut engine = Engine::new(HashMap::new());

    let source = make_source("users", make_columns(), vec!["id".to_string()]);
    engine.register_source(source.clone());

    source.borrow_mut().set_db_path(db_path);

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: Some(vec![rust_ivm::builder::ast::OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    };
    engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);

    let remove_row = Arc::new(rustc_hash::FxHashMap::from_iter([
        ("id".to_string(), Value::F64(3.0)),
        ("name".to_string(), Value::Str(Arc::from("Charlie"))),
        (
            "email".to_string(),
            Value::Str(Arc::from("charlie@example.com")),
        ),
        ("age".to_string(), Value::F64(35.0)),
        ("active".to_string(), Value::F64(0.0)),
    ]));

    let changes = engine.advance(&[("users".to_string(), make_source_change_remove(remove_row))]);

    assert!(!changes.is_empty(), "Remove advance should produce changes");
}

#[test]
fn test_sqlite_fetch_empty_table() {
    let db_path = "/tmp/rust-ivm-test-empty.db";
    clean_db(db_path);
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.execute_batch("CREATE TABLE empty (id INTEGER PRIMARY KEY, val TEXT);")
        .unwrap();
    drop(conn);

    let source = make_source(
        "empty",
        HashMap::from([
            ("id".to_string(), ColumnType::Number { optional: false }),
            ("val".to_string(), ColumnType::String { optional: false }),
        ]),
        vec!["id".to_string()],
    );
    source.borrow_mut().set_db_path(db_path);

    let input = source.borrow_mut().connect(None, None, None, None);
    let stream = input.borrow().fetch(&Default::default());
    let rows: Vec<_> = stream.collect();

    assert_eq!(rows.len(), 0, "Empty table should return 0 rows");
}

#[test]
#[should_panic(expected = "failed to prepare SQLite source query")]
fn test_sqlite_fetch_nonexistent_table() {
    let db_path = "/tmp/rust-ivm-test-nonexistent.db";
    clean_db(db_path);
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.execute_batch("CREATE TABLE real_table (id INTEGER PRIMARY KEY);")
        .unwrap();
    drop(conn);

    let source = make_source(
        "nonexistent_table",
        HashMap::from([("id".to_string(), ColumnType::Number { optional: false })]),
        vec!["id".to_string()],
    );
    source.borrow_mut().set_db_path(db_path);

    // Match PipelineDriver: SQLite prepare failures propagate; they must never
    // masquerade as an empty result set.
    let input = source.borrow_mut().connect(None, None, None, None);
    let stream = input.borrow().fetch(&Default::default());
    let _: Vec<_> = stream.collect();
}

#[test]
fn test_sqlite_multiple_sources_same_db() {
    let db_path = "/tmp/rust-ivm-test-multi.db";
    create_test_db(db_path);

    let users_source = make_source("users", make_columns(), vec!["id".to_string()]);
    users_source.borrow_mut().set_db_path(db_path);

    let posts_source = make_source(
        "posts",
        HashMap::from([
            ("id".to_string(), ColumnType::Number { optional: false }),
            ("userId".to_string(), ColumnType::Number { optional: false }),
            ("title".to_string(), ColumnType::String { optional: false }),
            ("body".to_string(), ColumnType::String { optional: false }),
        ]),
        vec!["id".to_string()],
    );
    posts_source.borrow_mut().set_db_path(db_path);

    // Fetch from users
    let users_input = users_source.borrow_mut().connect(None, None, None, None);
    let users_rows: Vec<_> = users_input.borrow().fetch(&Default::default()).collect();
    assert_eq!(users_rows.len(), 5, "Users: 5 rows");

    // Fetch from posts
    let posts_input = posts_source.borrow_mut().connect(None, None, None, None);
    let posts_rows: Vec<_> = posts_input.borrow().fetch(&Default::default()).collect();
    assert_eq!(posts_rows.len(), 5, "Posts: 5 rows");
}

#[test]
fn test_sqlite_row_set_signature() {
    let db_path = "/tmp/rust-ivm-test-sig.db";
    create_test_db(db_path);

    let mut engine = Engine::new(HashMap::new());

    let source = make_source("users", make_columns(), vec!["id".to_string()]);
    engine.register_source(source.clone());

    source.borrow_mut().set_db_path(db_path);

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: Some(vec![rust_ivm::builder::ast::OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    };

    engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);

    let sig = engine.row_set_signature("q1");
    assert!(sig.is_some(), "Row set signature should be set");
    assert_ne!(sig.unwrap(), 0, "Signature should be non-zero for 5 rows");
}

#[test]
fn test_sqlite_query_with_where_clause() {
    let db_path = "/tmp/rust-ivm-test-where.db";
    create_test_db(db_path);

    let mut engine = Engine::new(HashMap::new());

    let source = make_source("users", make_columns(), vec!["id".to_string()]);
    engine.register_source(source.clone());

    source.borrow_mut().set_db_path(db_path);

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: Some(rust_ivm::builder::ast::Condition::Simple(
            rust_ivm::builder::ast::SimpleCondition {
                op: "=".to_string(),
                left: rust_ivm::builder::ast::ValuePosition::Column {
                    name: "active".to_string(),
                },
                right: rust_ivm::builder::ast::ValuePosition::Literal {
                    value: Value::F64(1.0),
                },
            },
        )),
        related: vec![],
        limit: None,
        order_by: Some(vec![rust_ivm::builder::ast::OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(
        results[0].changes.len(),
        4,
        "WHERE active=1 should return 4 users"
    );
}

#[test]
fn test_sqlite_no_db_returns_empty() {
    // Without a DB set, fetch should return from in-memory data (which is empty)
    let source = make_source("users", make_columns(), vec!["id".to_string()]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let stream = input.borrow().fetch(&Default::default());
    let rows: Vec<_> = stream.collect();

    assert_eq!(
        rows.len(),
        0,
        "No DB set should return 0 rows from in-memory"
    );
}
