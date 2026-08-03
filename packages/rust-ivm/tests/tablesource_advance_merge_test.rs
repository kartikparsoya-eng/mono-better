//! Regression guard for the TableSource same-advance fetch merge (G15 + its
//! symmetric edit/remove gaps). This is the SQLite-backed test the earlier
//! MemorySource probes could NOT be: the merge only runs on the SQLite fetch
//! path (db_conn set), reading the PREV snapshot. A MemorySource fetch reads
//! the in-memory Vec directly and can't exercise it.
//!
//! During advance the SQLite snapshot is PREV and is read-only. A re-entrant
//! fetch must still reflect every prior push in the same advance: adds appear,
//! edits show the NEW value, and removes disappear.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::change::{
    make_source_change_add, make_source_change_edit, make_source_change_remove,
};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::operator::{Basis, FetchRequest, Input, Start};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::stream::StreamItem;
use rust_ivm::sqlite::table_source::TableSource;

fn clean_db(path: &str) {
    for p in [
        path.to_string(),
        format!("{path}-wal"),
        format!("{path}-shm"),
    ] {
        let _ = std::fs::remove_file(p);
    }
}

fn row(id: i64, name: &str) -> Arc<FxHashMap<String, Value>> {
    let mut m = FxHashMap::default();
    m.insert("id".to_string(), Value::F64(id as f64));
    m.insert("name".to_string(), Value::Str(name.into()));
    Arc::new(m)
}

fn columns() -> HashMap<String, ColumnType> {
    let mut c = HashMap::new();
    c.insert("id".to_string(), ColumnType::Number { optional: false });
    c.insert("name".to_string(), ColumnType::String { optional: false });
    c
}

/// (id, name) pairs from a fetch stream, sorted by id.
fn fetched(input: &Rc<RefCell<dyn Input>>) -> Vec<(i64, String)> {
    fetched_with(input, &Default::default())
}

fn fetched_with(input: &Rc<RefCell<dyn Input>>, request: &FetchRequest) -> Vec<(i64, String)> {
    let stream = input.borrow().fetch(request);
    let mut out: Vec<(i64, String)> = stream
        .filter_map(|item| match item {
            StreamItem::Data(n) => {
                let id = match n.row.get("id") {
                    Some(Value::F64(f)) => *f as i64,
                    _ => return None,
                };
                let name = match n.row.get("name") {
                    Some(Value::Str(s)) => s.to_string(),
                    _ => String::new(),
                };
                Some((id, name))
            }
            StreamItem::Yield => None,
        })
        .collect();
    out.sort();
    out
}

#[test]
fn tablesource_advance_fetch_reflects_add_edit_remove() {
    let db_path = "/tmp/rust-ivm-tablesource-merge.db";
    clean_db(db_path);
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=wal;").unwrap();
    conn.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
        .unwrap();
    // PREV snapshot: Alice(1), Bob(2), Charlie(3).
    for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Charlie")] {
        conn.execute(
            "INSERT INTO users (id,name) VALUES (?,?)",
            rusqlite::params![id, name],
        )
        .unwrap();
    }
    let source = Rc::new(RefCell::new(TableSource::new(
        Rc::new(RefCell::new(conn)),
        "users",
        columns(),
        vec!["id".to_string()],
    )));
    let input = source.borrow_mut().connect(
        Some(Arc::new(vec![["id".to_string(), "asc".to_string()]])),
        None,
        None,
        None,
    );

    // Baseline: fetch reads PREV.
    assert_eq!(
        fetched(&input),
        vec![
            (1, "Alice".into()),
            (2, "Bob".into()),
            (3, "Charlie".into())
        ],
    );

    // Same-advance changes (as during advance_to_head_stream): ADD Frank(6),
    // EDIT Alice(1)->Alicia, REMOVE Bob(2). SQLite (PREV) is untouched.
    source
        .borrow_mut()
        .push(make_source_change_add(row(6, "Frank")));
    source
        .borrow_mut()
        .push(make_source_change_edit(row(1, "Alicia"), row(1, "Alice")));
    source
        .borrow_mut()
        .push(make_source_change_remove(row(2, "Bob")));
    source
        .borrow_mut()
        .push(make_source_change_add(row(4, "Dana")));

    // A re-entrant fetch must now reflect the current state:
    //   - Frank added, Bob gone, Alice shows the NEW value "Alicia".
    assert_eq!(
        fetched(&input),
        vec![
            (1, "Alicia".into()),
            (3, "Charlie".into()),
            (4, "Dana".into()),
            (6, "Frank".into())
        ],
        "TableSource fetch during advance must reflect same-advance add/edit/remove \
         (not the stale PREV snapshot)",
    );

    assert_eq!(
        fetched_with(
            &input,
            &FetchRequest {
                start: Some(Start {
                    row: row(4, "Dana"),
                    basis: Basis::After,
                }),
                ..Default::default()
            },
        ),
        vec![(6, "Frank".into())],
        "an accumulated overlay equal to an exclusive start must be filtered",
    );

    clean_db(db_path);
}
