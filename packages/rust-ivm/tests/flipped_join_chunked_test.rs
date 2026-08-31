//! Tests for FlippedJoin chunked multi-constraint fetch.
//! Port of TS `flipped-join.chunked.test.ts` (v1.7.0).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::data::{Node, Row, Value};
use rust_ivm::ivm::flipped_join::{
    FlippedJoin, FlippedJoinArgs, set_multi_constraint_chunk_size_for_test,
};
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::operator::{FetchRequest, Input};
use rust_ivm::ivm::schema::{ColumnType, System};

// `set_multi_constraint_chunk_size_for_test` mutates a process-global AtomicUsize.
// cargo runs the tests in this file on parallel threads, so every test that touches
// that global must serialize on this lock — otherwise one test's swap/restore chain
// races another's assertions (observed as a flaky `test_chunk_size_getter_setter`).
// Poison-safe: a panicking test still yields the guard to the next.
static CHUNK_SIZE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
fn chunk_serial() -> std::sync::MutexGuard<'static, ()> {
    CHUNK_SIZE_TEST_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

#[allow(dead_code)]
fn make_row(pairs: &[(&str, Value)]) -> Row {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Arc::new(map)
}

fn make_source(
    name: &str,
    pk: &[&str],
    columns: &[(&str, ColumnType)],
) -> Rc<RefCell<MemorySource>> {
    let cols: HashMap<String, ColumnType> = columns
        .iter()
        .map(|(n, t)| (n.to_string(), t.clone()))
        .collect();
    Rc::new(RefCell::new(MemorySource::new(
        name,
        cols,
        pk.iter().map(|s| s.to_string()).collect(),
    )))
}

fn add_row(source: &Rc<RefCell<MemorySource>>, pairs: &[(&str, Value)]) {
    let row_data: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    source.borrow_mut().add_row(row_data);
}

fn str_val(s: &str) -> Value {
    Value::Str(Arc::from(s))
}

#[test]
fn test_chunked_fetch_small_chunk_size() {
    let _serial = chunk_serial();
    let restore = set_multi_constraint_chunk_size_for_test(2);

    let parent = make_source(
        "parents",
        &["id"],
        &[
            ("id", ColumnType::Number { optional: false }),
            ("name", ColumnType::String { optional: false }),
        ],
    );
    let child = make_source(
        "children",
        &["id"],
        &[
            ("id", ColumnType::Number { optional: false }),
            ("parent_id", ColumnType::Number { optional: false }),
        ],
    );

    for i in 1..=6 {
        add_row(
            &parent,
            &[
                ("id", Value::F64(i as f64)),
                ("name", str_val(&format!("p{}", i))),
            ],
        );
    }
    for i in 1..=6 {
        add_row(
            &child,
            &[
                ("id", Value::F64(i as f64)),
                ("parent_id", Value::F64(i as f64)),
            ],
        );
    }

    let parent_input = parent.borrow_mut().connect(None, None, None, None);
    let child_input = child.borrow_mut().connect(None, None, None, None);

    let fj = FlippedJoin::new(FlippedJoinArgs {
        parent: parent_input,
        child: child_input,
        parent_key: vec!["id".to_string()],
        child_key: vec!["parent_id".to_string()],
        relationship_name: "children".to_string(),
        hidden: false,
        system: System::Client,
    });

    let stream = fj.borrow().fetch(&FetchRequest::default());
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(stream).collect();

    assert_eq!(nodes.len(), 6, "Should get 6 parent nodes with children");
    for (i, node) in nodes.iter().enumerate() {
        let id = node.row.get("id").cloned().unwrap_or(Value::Null);
        let expected = Value::F64((i + 1) as f64);
        assert_eq!(id, expected, "Parents should be in sorted order");
    }

    restore();
}

#[test]
fn test_chunked_fetch_preserves_order() {
    let _serial = chunk_serial();
    let restore = set_multi_constraint_chunk_size_for_test(3);

    let parent = make_source(
        "parents",
        &["id"],
        &[
            ("id", ColumnType::Number { optional: false }),
            ("name", ColumnType::String { optional: false }),
        ],
    );
    let child = make_source(
        "children",
        &["id"],
        &[
            ("id", ColumnType::Number { optional: false }),
            ("parent_id", ColumnType::Number { optional: false }),
        ],
    );

    for i in 1..=10 {
        add_row(
            &parent,
            &[
                ("id", Value::F64(i as f64)),
                ("name", str_val(&format!("p{}", i))),
            ],
        );
    }
    for i in 1..=10 {
        add_row(
            &child,
            &[
                ("id", Value::F64(i as f64)),
                ("parent_id", Value::F64((((i - 1) % 10) + 1) as f64)),
            ],
        );
    }

    let parent_input = parent.borrow_mut().connect(None, None, None, None);
    let child_input = child.borrow_mut().connect(None, None, None, None);

    let fj = FlippedJoin::new(FlippedJoinArgs {
        parent: parent_input,
        child: child_input,
        parent_key: vec!["id".to_string()],
        child_key: vec!["parent_id".to_string()],
        relationship_name: "children".to_string(),
        hidden: false,
        system: System::Client,
    });

    let stream = fj.borrow().fetch(&FetchRequest::default());
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(stream).collect();

    assert!(!nodes.is_empty(), "Should get results");
    for i in 1..nodes.len() {
        let prev = nodes[i - 1].row.get("id").cloned().unwrap_or(Value::Null);
        let curr = nodes[i].row.get("id").cloned().unwrap_or(Value::Null);
        if let (Value::F64(a), Value::F64(b)) = (prev, curr) {
            assert!(a <= b, "Parents should be sorted: {} <= {}", a, b)
        }
    }

    restore();
}

#[test]
fn test_chunked_fetch_single_chunk() {
    let parent = make_source(
        "parents",
        &["id"],
        &[("id", ColumnType::Number { optional: false })],
    );
    let child = make_source(
        "children",
        &["id"],
        &[
            ("id", ColumnType::Number { optional: false }),
            ("pid", ColumnType::Number { optional: false }),
        ],
    );

    add_row(&parent, &[("id", Value::F64(1.0))]);
    add_row(&child, &[("id", Value::F64(1.0)), ("pid", Value::F64(1.0))]);

    let parent_input = parent.borrow_mut().connect(None, None, None, None);
    let child_input = child.borrow_mut().connect(None, None, None, None);

    let fj = FlippedJoin::new(FlippedJoinArgs {
        parent: parent_input,
        child: child_input,
        parent_key: vec!["id".to_string()],
        child_key: vec!["pid".to_string()],
        relationship_name: "kids".to_string(),
        hidden: false,
        system: System::Client,
    });

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 1);
}

#[test]
fn test_chunked_fetch_empty_children() {
    let parent = make_source(
        "parents",
        &["id"],
        &[("id", ColumnType::Number { optional: false })],
    );
    let child = make_source(
        "children",
        &["id"],
        &[
            ("id", ColumnType::Number { optional: false }),
            ("pid", ColumnType::Number { optional: false }),
        ],
    );

    add_row(&parent, &[("id", Value::F64(1.0))]);

    let parent_input = parent.borrow_mut().connect(None, None, None, None);
    let child_input = child.borrow_mut().connect(None, None, None, None);

    let fj = FlippedJoin::new(FlippedJoinArgs {
        parent: parent_input,
        child: child_input,
        parent_key: vec!["id".to_string()],
        child_key: vec!["pid".to_string()],
        relationship_name: "kids".to_string(),
        hidden: false,
        system: System::Client,
    });

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(
        nodes.len(),
        0,
        "No parents should be returned when no children exist"
    );
}

#[test]
fn test_chunked_fetch_multiple_children_per_parent() {
    let _serial = chunk_serial();
    let restore = set_multi_constraint_chunk_size_for_test(2);

    let parent = make_source(
        "parents",
        &["id"],
        &[("id", ColumnType::Number { optional: false })],
    );
    let child = make_source(
        "children",
        &["id"],
        &[
            ("id", ColumnType::Number { optional: false }),
            ("pid", ColumnType::Number { optional: false }),
        ],
    );

    add_row(&parent, &[("id", Value::F64(1.0))]);
    add_row(&parent, &[("id", Value::F64(2.0))]);

    for i in 1..=4 {
        let pid = if i <= 2 { 1.0 } else { 2.0 };
        add_row(
            &child,
            &[("id", Value::F64(i as f64)), ("pid", Value::F64(pid))],
        );
    }

    let parent_input = parent.borrow_mut().connect(None, None, None, None);
    let child_input = child.borrow_mut().connect(None, None, None, None);

    let fj = FlippedJoin::new(FlippedJoinArgs {
        parent: parent_input,
        child: child_input,
        parent_key: vec!["id".to_string()],
        child_key: vec!["pid".to_string()],
        relationship_name: "kids".to_string(),
        hidden: false,
        system: System::Client,
    });

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 2, "Both parents should have children");

    restore();
}

#[test]
fn test_chunk_size_getter_setter() {
    let _serial = chunk_serial();
    let restore = set_multi_constraint_chunk_size_for_test(42);
    assert_eq!(
        rust_ivm::ivm::flipped_join::get_multi_constraint_chunk_size(),
        42
    );
    restore();
    assert_eq!(
        rust_ivm::ivm::flipped_join::get_multi_constraint_chunk_size(),
        256
    );
}
