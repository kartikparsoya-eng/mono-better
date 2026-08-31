//! End-to-end tests for TableSource — the production SQLite-backed source.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rusqlite::Connection;
use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{Condition, SimpleCondition, ValuePosition};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::memory_source::CollectOutput;
use rust_ivm::ivm::operator::OutputHandle;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::sqlite::table_source::TableSource;

fn create_db_with_table(table_name: &str, columns: &[(&str, &str)]) -> Rc<RefCell<Connection>> {
    let conn = Connection::open_in_memory().unwrap();
    let col_defs: Vec<String> = columns
        .iter()
        .map(|(name, col)| format!("\"{}\" {}", name, col))
        .collect();
    let sql = format!("CREATE TABLE \"{}\" ({});", table_name, col_defs.join(", "));
    conn.execute(&sql, []).unwrap();
    Rc::new(RefCell::new(conn))
}

fn make_row(pairs: &[(&str, Value)]) -> rust_ivm::ivm::data::Row {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Arc::new(map)
}

#[test]
fn test_table_source_fetch_all() {
    let db = create_db_with_table("users", &[("id", "INTEGER PRIMARY KEY"), ("name", "TEXT")]);

    // Insert some data
    db.borrow()
        .execute(
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
            [],
        )
        .unwrap();

    let mut columns = HashMap::new();
    columns.insert("id".to_string(), ColumnType::Number { optional: false });
    columns.insert("name".to_string(), ColumnType::String { optional: false });

    let mut source = TableSource::new(db, "users", columns, vec!["id".to_string()]);
    let input = source.connect(None, None, None, None, None);

    // Fetch all rows
    let stream = input.borrow().fetch(&Default::default());
    let nodes: Vec<_> = rust_ivm::ivm::stream::skip_yields(stream).collect();

    assert_eq!(nodes.len(), 3);
    let first = &nodes[0].row;
    let name = first.get("name").cloned().unwrap_or(Value::Null);
    assert_eq!(name, Value::Str("Alice".into()));
}

#[test]
fn test_table_source_fetch_with_constraint() {
    let db = create_db_with_table("users", &[("id", "INTEGER PRIMARY KEY"), ("name", "TEXT")]);
    db.borrow()
        .execute(
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
            [],
        )
        .unwrap();

    let mut columns = HashMap::new();
    columns.insert("id".to_string(), ColumnType::Number { optional: false });
    columns.insert("name".to_string(), ColumnType::String { optional: false });

    let mut source = TableSource::new(db, "users", columns, vec!["id".to_string()]);
    let input = source.connect(None, None, None, None, None);

    // Fetch with constraint: id = 2
    let mut constraint = rustc_hash::FxHashMap::default();
    constraint.insert("id".to_string(), Value::F64(2.0));
    let req = rust_ivm::ivm::operator::FetchRequest {
        constraint: Some(constraint),
        ..Default::default()
    };

    let stream = input.borrow().fetch(&req);
    let nodes: Vec<_> = rust_ivm::ivm::stream::skip_yields(stream).collect();

    assert_eq!(nodes.len(), 1);
    let name = nodes[0].row.get("name").cloned().unwrap_or(Value::Null);
    assert_eq!(name, Value::Str("Bob".into()));
}

#[test]
fn test_table_source_fetch_with_filter() {
    let db = create_db_with_table(
        "users",
        &[
            ("id", "INTEGER PRIMARY KEY"),
            ("name", "TEXT"),
            ("age", "INTEGER"),
        ],
    );
    db.borrow()
        .execute(
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Carol', 25)",
            [],
        )
        .unwrap();

    let mut columns = HashMap::new();
    columns.insert("id".to_string(), ColumnType::Number { optional: false });
    columns.insert("name".to_string(), ColumnType::String { optional: false });
    columns.insert("age".to_string(), ColumnType::Number { optional: false });

    let mut source = TableSource::new(db, "users", columns, vec!["id".to_string()]);

    // Filter: age = 25
    let predicate: Arc<dyn Fn(&rust_ivm::ivm::data::Row) -> bool> =
        Arc::new(|row: &rust_ivm::ivm::data::Row| {
            row.get("age").cloned().unwrap_or(Value::Null) == Value::F64(25.0)
        });

    let condition = Condition::Simple(SimpleCondition {
        op: "=".to_string(),
        left: ValuePosition::Column {
            name: "age".to_string(),
        },
        right: ValuePosition::Literal {
            value: Value::F64(25.0),
        },
    });
    let input = source.connect(None, Some(condition), Some(predicate), None, None);

    let stream = input.borrow().fetch(&Default::default());
    let nodes: Vec<_> = rust_ivm::ivm::stream::skip_yields(stream).collect();

    assert_eq!(nodes.len(), 2);
    for node in &nodes {
        let age = node.row.get("age").cloned().unwrap_or(Value::Null);
        assert_eq!(age, Value::F64(25.0));
    }
}

#[test]
fn test_table_source_fetch_with_order() {
    let db = create_db_with_table("users", &[("id", "INTEGER PRIMARY KEY"), ("name", "TEXT")]);
    db.borrow()
        .execute(
            "INSERT INTO users (id, name) VALUES (3, 'Carol'), (1, 'Alice'), (2, 'Bob')",
            [],
        )
        .unwrap();

    let mut columns = HashMap::new();
    columns.insert("id".to_string(), ColumnType::Number { optional: false });
    columns.insert("name".to_string(), ColumnType::String { optional: false });

    let mut source = TableSource::new(db, "users", columns, vec!["id".to_string()]);

    let sort: rust_ivm::ivm::data::SortOrder =
        Arc::new(vec![["id".to_string(), "asc".to_string()]]);

    let input = source.connect(Some(sort), None, None, None, None);

    let stream = input.borrow().fetch(&Default::default());
    let nodes: Vec<_> = rust_ivm::ivm::stream::skip_yields(stream).collect();

    assert_eq!(nodes.len(), 3);
    // Should be ordered by id asc: 1, 2, 3
    assert_eq!(
        nodes[0].row.get("id").cloned().unwrap_or(Value::Null),
        Value::F64(1.0)
    );
    assert_eq!(
        nodes[1].row.get("id").cloned().unwrap_or(Value::Null),
        Value::F64(2.0)
    );
    assert_eq!(
        nodes[2].row.get("id").cloned().unwrap_or(Value::Null),
        Value::F64(3.0)
    );
}

#[test]
#[ignore = "write_change is intentionally a no-op in production"]
fn test_table_source_push_add() {
    let db = create_db_with_table("users", &[("id", "INTEGER PRIMARY KEY"), ("name", "TEXT")]);
    db.borrow()
        .execute("INSERT INTO users (id, name) VALUES (1, 'Alice')", [])
        .unwrap();

    let mut columns = HashMap::new();
    columns.insert("id".to_string(), ColumnType::Number { optional: false });
    columns.insert("name".to_string(), ColumnType::String { optional: false });

    let mut source = TableSource::new(db.clone(), "users", columns, vec!["id".to_string()]);

    // Connect a pipeline
    let input = source.connect(None, None, None, None, None);
    let collector = Rc::new(RefCell::new(CollectOutput::new()));
    input
        .borrow_mut()
        .set_output(collector.clone() as OutputHandle);

    // Push an add
    let new_row = make_row(&[("id", Value::F64(2.0)), ("name", Value::Str("Bob".into()))]);
    source.push(rust_ivm::ivm::source::make_source_change_add(new_row));

    // Verify it was written to SQLite
    let count: i64 = db
        .borrow()
        .query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 2);

    let name: String = db
        .borrow()
        .query_row("SELECT name FROM users WHERE id = 2", [], |row| row.get(0))
        .unwrap();
    assert_eq!(name, "Bob");
}

#[test]
#[ignore = "write_change is intentionally a no-op in production"]
fn test_table_source_push_remove() {
    let db = create_db_with_table("users", &[("id", "INTEGER PRIMARY KEY"), ("name", "TEXT")]);
    db.borrow()
        .execute(
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
            [],
        )
        .unwrap();

    let mut columns = HashMap::new();
    columns.insert("id".to_string(), ColumnType::Number { optional: false });
    columns.insert("name".to_string(), ColumnType::String { optional: false });

    let mut source = TableSource::new(db.clone(), "users", columns, vec!["id".to_string()]);

    let input = source.connect(None, None, None, None, None);
    let collector = Rc::new(RefCell::new(CollectOutput::new()));
    input
        .borrow_mut()
        .set_output(collector.clone() as OutputHandle);

    // Push a remove
    let row = make_row(&[
        ("id", Value::F64(1.0)),
        ("name", Value::Str("Alice".into())),
    ]);
    source.push(rust_ivm::ivm::source::make_source_change_remove(row));

    // Verify it was deleted from SQLite
    let count: i64 = db
        .borrow()
        .query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
#[ignore = "write_change is intentionally a no-op in production"]
fn test_table_source_push_edit() {
    let db = create_db_with_table("users", &[("id", "INTEGER PRIMARY KEY"), ("name", "TEXT")]);
    db.borrow()
        .execute("INSERT INTO users (id, name) VALUES (1, 'Alice')", [])
        .unwrap();

    let mut columns = HashMap::new();
    columns.insert("id".to_string(), ColumnType::Number { optional: false });
    columns.insert("name".to_string(), ColumnType::String { optional: false });

    let mut source = TableSource::new(db.clone(), "users", columns, vec!["id".to_string()]);

    let input = source.connect(None, None, None, None, None);
    let collector = Rc::new(RefCell::new(CollectOutput::new()));
    input
        .borrow_mut()
        .set_output(collector.clone() as OutputHandle);

    // Push an edit (same PK, different name)
    let old_row = make_row(&[
        ("id", Value::F64(1.0)),
        ("name", Value::Str("Alice".into())),
    ]);
    let new_row = make_row(&[("id", Value::F64(1.0)), ("name", Value::Str("Bob".into()))]);
    source.push(rust_ivm::ivm::source::make_source_change_edit(
        new_row, old_row,
    ));

    // Verify the name was updated in SQLite
    let name: String = db
        .borrow()
        .query_row("SELECT name FROM users WHERE id = 1", [], |row| row.get(0))
        .unwrap();
    assert_eq!(name, "Bob");
}

#[test]
fn test_table_source_fetch_with_multi_constraint() {
    let db = create_db_with_table("users", &[("id", "INTEGER PRIMARY KEY"), ("name", "TEXT")]);
    db.borrow()
        .execute(
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol'), (4, 'Dave')",
            [],
        )
        .unwrap();

    let mut columns = HashMap::new();
    columns.insert("id".to_string(), ColumnType::Number { optional: false });
    columns.insert("name".to_string(), ColumnType::String { optional: false });

    let mut source = TableSource::new(db, "users", columns, vec!["id".to_string()]);
    let input = source.connect(None, None, None, None, None);

    // Fetch with multi-constraint: id IN (1, 3)
    let mc: rust_ivm::ivm::constraint::MultiConstraint = vec![
        {
            let mut c = rustc_hash::FxHashMap::default();
            c.insert("id".to_string(), Value::F64(1.0));
            c
        },
        {
            let mut c = rustc_hash::FxHashMap::default();
            c.insert("id".to_string(), Value::F64(3.0));
            c
        },
    ];

    let req = rust_ivm::ivm::operator::FetchRequest {
        multi_constraints: vec![mc],
        ..Default::default()
    };

    let stream = input.borrow().fetch(&req);
    let nodes: Vec<_> = rust_ivm::ivm::stream::skip_yields(stream).collect();

    assert_eq!(nodes.len(), 2);
    let _ids: Vec<f64> = nodes
        .iter()
        .map(|n| match n.row.get("id").cloned().unwrap_or(Value::Null) {
            Value::F64(f) => f,
            _ => -1.0,
        })
        .collect();
}
