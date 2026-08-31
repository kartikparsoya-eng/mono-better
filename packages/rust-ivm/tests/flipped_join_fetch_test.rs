//! Tests for FlippedJoin fetch operations.
//! Port of TS `flipped-join.fetch.test.ts` (v1.7.0).
//! Tests the inner join: fetch child first, then batched parent fetch.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::data::{Node, Row, Value};
use rust_ivm::ivm::flipped_join::{FlippedJoin, FlippedJoinArgs};
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::operator::{FetchRequest, Input};
use rust_ivm::ivm::schema::{ColumnType, System};

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

#[allow(clippy::type_complexity)]
fn setup_flipped_join(
    parent_data: &[Vec<(&str, Value)>],
    child_data: &[Vec<(&str, Value)>],
) -> (
    Rc<RefCell<MemorySource>>,
    Rc<RefCell<MemorySource>>,
    Rc<RefCell<FlippedJoin>>,
) {
    let parent = make_source(
        "issues",
        &["id"],
        &[("id", ColumnType::String { optional: false })],
    );
    let child = make_source(
        "comments",
        &["id"],
        &[
            ("id", ColumnType::String { optional: false }),
            ("issueID", ColumnType::String { optional: false }),
        ],
    );

    for row_data in parent_data {
        add_row(&parent, row_data);
    }
    for row_data in child_data {
        add_row(&child, row_data);
    }

    let parent_input = parent.borrow_mut().connect(None, None, None, None, None);
    let child_input = child.borrow_mut().connect(None, None, None, None, None);

    let fj = FlippedJoin::new(FlippedJoinArgs {
        parent: parent_input,
        child: child_input,
        parent_key: vec!["id".to_string()],
        child_key: vec!["issueID".to_string()],
        relationship_name: "comments".to_string(),
        hidden: false,
        system: System::Client,
    });

    (parent, child, fj)
}

fn get_rel_children(node: &Node, rel_name: &str) -> Vec<Node> {
    node.relationships
        .get(rel_name)
        .map(|f| rust_ivm::ivm::stream::skip_yields(f()).collect())
        .unwrap_or_default()
}

#[test]
fn test_fetch_no_data() {
    let (_, _, fj) = setup_flipped_join(&[], &[]);
    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 0, "No data should yield no results");
}

#[test]
fn test_fetch_no_parent() {
    let (_, _, fj) = setup_flipped_join(
        &[],
        &[vec![("id", str_val("c1")), ("issueID", str_val("i1"))]],
    );
    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 0, "No parent means no results (inner join)");
}

#[test]
fn test_fetch_parent_no_children() {
    let (_, _, fj) = setup_flipped_join(&[vec![("id", str_val("i1"))]], &[]);
    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(
        nodes.len(),
        0,
        "Parent with no children = inner join excludes it"
    );
}

#[test]
fn test_fetch_one_parent_one_child() {
    let (_, _, fj) = setup_flipped_join(
        &[vec![("id", str_val("i1"))]],
        &[vec![("id", str_val("c1")), ("issueID", str_val("i1"))]],
    );
    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 1);
    let children = get_rel_children(&nodes[0], "comments");
    assert_eq!(children.len(), 1);
    assert_eq!(
        children[0].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("c1")
    );
}

#[test]
fn test_fetch_one_parent_wrong_child() {
    let (_, _, fj) = setup_flipped_join(
        &[vec![("id", str_val("i1"))]],
        &[vec![("id", str_val("c1")), ("issueID", str_val("i2"))]],
    );
    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(
        nodes.len(),
        0,
        "Child pointing to non-existent parent = no results"
    );
}

#[test]
fn test_fetch_one_parent_one_child_one_wrong_child() {
    let (_, _, fj) = setup_flipped_join(
        &[vec![("id", str_val("i1"))]],
        &[
            vec![("id", str_val("c2")), ("issueID", str_val("i2"))],
            vec![("id", str_val("c1")), ("issueID", str_val("i1"))],
        ],
    );
    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 1);
    let children = get_rel_children(&nodes[0], "comments");
    assert_eq!(children.len(), 1);
    assert_eq!(
        children[0].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("c1")
    );
}

#[test]
fn test_fetch_two_parents_each_with_two_children() {
    let (_, _, fj) = setup_flipped_join(
        &[vec![("id", str_val("i2"))], vec![("id", str_val("i1"))]],
        &[
            vec![("id", str_val("c4")), ("issueID", str_val("i2"))],
            vec![("id", str_val("c3")), ("issueID", str_val("i2"))],
            vec![("id", str_val("c2")), ("issueID", str_val("i1"))],
            vec![("id", str_val("c1")), ("issueID", str_val("i1"))],
        ],
    );
    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 2, "Both parents should have children");

    assert_eq!(
        nodes[0].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("i1")
    );
    let children0 = get_rel_children(&nodes[0], "comments");
    assert_eq!(children0.len(), 2);
    assert_eq!(
        children0[0].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("c1")
    );
    assert_eq!(
        children0[1].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("c2")
    );

    assert_eq!(
        nodes[1].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("i2")
    );
    let children1 = get_rel_children(&nodes[1], "comments");
    assert_eq!(children1.len(), 2);
    assert_eq!(
        children1[0].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("c3")
    );
    assert_eq!(
        children1[1].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("c4")
    );
}

#[test]
fn test_fetch_with_constraint() {
    let (_, _, fj) = setup_flipped_join(
        &[vec![("id", str_val("i1"))], vec![("id", str_val("i2"))]],
        &[
            vec![("id", str_val("c1")), ("issueID", str_val("i1"))],
            vec![("id", str_val("c2")), ("issueID", str_val("i2"))],
        ],
    );

    let mut constraint = rust_ivm::ivm::constraint::Constraint::default();
    constraint.insert("id".to_string(), str_val("i2"));
    let req = FetchRequest {
        constraint: Some(constraint),
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("i2")
    );
}

#[test]
fn test_fetch_compound_key() {
    let parent = make_source(
        "parents",
        &["a", "b"],
        &[
            ("a", ColumnType::String { optional: false }),
            ("b", ColumnType::String { optional: false }),
        ],
    );
    let child = make_source(
        "children",
        &["id"],
        &[
            ("id", ColumnType::String { optional: false }),
            ("pa", ColumnType::String { optional: false }),
            ("pb", ColumnType::String { optional: false }),
        ],
    );

    add_row(&parent, &[("a", str_val("x")), ("b", str_val("1"))]);
    add_row(&parent, &[("a", str_val("y")), ("b", str_val("2"))]);
    add_row(
        &child,
        &[
            ("id", str_val("c1")),
            ("pa", str_val("x")),
            ("pb", str_val("1")),
        ],
    );
    add_row(
        &child,
        &[
            ("id", str_val("c2")),
            ("pa", str_val("y")),
            ("pb", str_val("2")),
        ],
    );

    let parent_input = parent.borrow_mut().connect(None, None, None, None, None);
    let child_input = child.borrow_mut().connect(None, None, None, None, None);

    let fj = FlippedJoin::new(FlippedJoinArgs {
        parent: parent_input,
        child: child_input,
        parent_key: vec!["a".to_string(), "b".to_string()],
        child_key: vec!["pa".to_string(), "pb".to_string()],
        relationship_name: "kids".to_string(),
        hidden: false,
        system: System::Client,
    });

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(
        nodes.len(),
        2,
        "Both parents should match with compound keys"
    );

    let children0 = get_rel_children(&nodes[0], "kids");
    assert_eq!(children0.len(), 1);
    assert_eq!(
        children0[0].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("c1")
    );
}

#[test]
fn test_fetch_reverse_order() {
    let (_, _, fj) = setup_flipped_join(
        &[
            vec![("id", str_val("i1"))],
            vec![("id", str_val("i2"))],
            vec![("id", str_val("i3"))],
        ],
        &[
            vec![("id", str_val("c1")), ("issueID", str_val("i1"))],
            vec![("id", str_val("c2")), ("issueID", str_val("i2"))],
            vec![("id", str_val("c3")), ("issueID", str_val("i3"))],
        ],
    );

    let req = FetchRequest {
        reverse: true,
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 3);
    assert_eq!(
        nodes[0].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("i3")
    );
    assert_eq!(
        nodes[1].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("i2")
    );
    assert_eq!(
        nodes[2].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("i1")
    );
}

#[test]
fn test_fetch_many_parents_chunked() {
    let restore = rust_ivm::ivm::flipped_join::set_multi_constraint_chunk_size_for_test(3);

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

    for i in 1..=10 {
        add_row(&parent, &[("id", Value::F64(i as f64))]);
    }
    for i in 1..=10 {
        add_row(
            &child,
            &[("id", Value::F64(i as f64)), ("pid", Value::F64(i as f64))],
        );
    }

    let parent_input = parent.borrow_mut().connect(None, None, None, None, None);
    let child_input = child.borrow_mut().connect(None, None, None, None, None);

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
    assert_eq!(nodes.len(), 10, "All 10 parents should have children");

    for (i, node) in nodes.iter().enumerate() {
        let id = node.row.get("id").cloned().unwrap_or(Value::Null);
        assert_eq!(id, Value::F64((i + 1) as f64), "Parents should be sorted");
    }

    restore();
}

#[test]
fn test_fetch_hidden_join() {
    let parent = make_source(
        "issues",
        &["id"],
        &[("id", ColumnType::String { optional: false })],
    );
    let child = make_source(
        "comments",
        &["id"],
        &[
            ("id", ColumnType::String { optional: false }),
            ("issueID", ColumnType::String { optional: false }),
        ],
    );

    add_row(&parent, &[("id", str_val("i1"))]);
    add_row(&child, &[("id", str_val("c1")), ("issueID", str_val("i1"))]);

    let parent_input = parent.borrow_mut().connect(None, None, None, None, None);
    let child_input = child.borrow_mut().connect(None, None, None, None, None);

    let fj = FlippedJoin::new(FlippedJoinArgs {
        parent: parent_input,
        child: child_input,
        parent_key: vec!["id".to_string()],
        child_key: vec!["issueID".to_string()],
        relationship_name: "comments".to_string(),
        hidden: true,
        system: System::Client,
    });

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(
        nodes.len(),
        1,
        "Hidden join should still return parent nodes"
    );
    assert_eq!(
        nodes[0].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("i1")
    );
}

#[test]
fn test_fetch_start_after() {
    let (_, _, fj) = setup_flipped_join(
        &[
            vec![("id", str_val("i1"))],
            vec![("id", str_val("i2"))],
            vec![("id", str_val("i3"))],
            vec![("id", str_val("i4"))],
        ],
        &[
            vec![("id", str_val("c1")), ("issueID", str_val("i1"))],
            vec![("id", str_val("c2")), ("issueID", str_val("i2"))],
            vec![("id", str_val("c3")), ("issueID", str_val("i3"))],
            vec![("id", str_val("c4")), ("issueID", str_val("i4"))],
        ],
    );

    let start_row: FxHashMap<String, Value> =
        FxHashMap::from_iter([("id".to_string(), str_val("i2"))]);
    let req = FetchRequest {
        start: Some(rust_ivm::ivm::operator::Start {
            row: Arc::new(start_row),
            basis: rust_ivm::ivm::operator::Basis::After,
        }),
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 2, "Start after i2 should yield i3 and i4");
    assert_eq!(
        nodes[0].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("i3")
    );
    assert_eq!(
        nodes[1].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("i4")
    );
}

// Start basis "at" (forward) — includes the start row
#[test]
fn test_fetch_start_at() {
    let (_, _, fj) = setup_flipped_join(
        &[
            vec![("id", str_val("i1"))],
            vec![("id", str_val("i2"))],
            vec![("id", str_val("i3"))],
        ],
        &[
            vec![("id", str_val("c1")), ("issueID", str_val("i1"))],
            vec![("id", str_val("c2")), ("issueID", str_val("i2"))],
            vec![("id", str_val("c3")), ("issueID", str_val("i3"))],
        ],
    );

    let start_row: FxHashMap<String, Value> =
        FxHashMap::from_iter([("id".to_string(), str_val("i2"))]);
    let req = FetchRequest {
        start: Some(rust_ivm::ivm::operator::Start {
            row: Arc::new(start_row),
            basis: rust_ivm::ivm::operator::Basis::At,
        }),
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 2, "Start at i2 should yield i2 and i3");
    assert_eq!(
        nodes[0].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("i2")
    );
    assert_eq!(
        nodes[1].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("i3")
    );
}

// Start basis "at" with reverse — TS: reverse from i2, includes i2, goes backwards → [i2, i1]
#[test]
fn test_fetch_start_at_reverse() {
    let (_, _, fj) = setup_flipped_join(
        &[
            vec![("id", str_val("i1"))],
            vec![("id", str_val("i2"))],
            vec![("id", str_val("i3"))],
        ],
        &[
            vec![("id", str_val("c1")), ("issueID", str_val("i1"))],
            vec![("id", str_val("c2")), ("issueID", str_val("i2"))],
            vec![("id", str_val("c3")), ("issueID", str_val("i3"))],
        ],
    );

    let start_row: FxHashMap<String, Value> =
        FxHashMap::from_iter([("id".to_string(), str_val("i2"))]);
    let req = FetchRequest {
        start: Some(rust_ivm::ivm::operator::Start {
            row: Arc::new(start_row),
            basis: rust_ivm::ivm::operator::Basis::At,
        }),
        reverse: true,
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 2);
    assert_eq!(
        nodes[0].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("i2")
    );
    assert_eq!(
        nodes[1].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("i1")
    );
}

// Start basis "after" with reverse — TS: reverse after i2, excludes i2, goes backwards → [i1]
#[test]
fn test_fetch_start_after_reverse() {
    let (_, _, fj) = setup_flipped_join(
        &[
            vec![("id", str_val("i1"))],
            vec![("id", str_val("i2"))],
            vec![("id", str_val("i3"))],
        ],
        &[
            vec![("id", str_val("c1")), ("issueID", str_val("i1"))],
            vec![("id", str_val("c2")), ("issueID", str_val("i2"))],
            vec![("id", str_val("c3")), ("issueID", str_val("i3"))],
        ],
    );

    let start_row: FxHashMap<String, Value> =
        FxHashMap::from_iter([("id".to_string(), str_val("i2"))]);
    let req = FetchRequest {
        start: Some(rust_ivm::ivm::operator::Start {
            row: Arc::new(start_row),
            basis: rust_ivm::ivm::operator::Basis::After,
        }),
        reverse: true,
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("i1")
    );
}

// ===========================================================================
// Stream merge tests — K-way merge via mergeSortedStreams
// ===========================================================================

fn setup_group_join(
    num_groups: usize,
    per_group: usize,
    children_per_group: usize,
) -> Rc<RefCell<FlippedJoin>> {
    let parent = make_source(
        "parents",
        &["id"],
        &[
            ("id", ColumnType::Number { optional: false }),
            ("groupId", ColumnType::Number { optional: false }),
        ],
    );
    let child = make_source(
        "children",
        &["id"],
        &[
            ("id", ColumnType::Number { optional: false }),
            ("parentGroupId", ColumnType::Number { optional: false }),
        ],
    );

    for i in 0..(num_groups * per_group) {
        add_row(
            &parent,
            &[
                ("id", Value::F64(i as f64)),
                ("groupId", Value::F64((i / per_group) as f64)),
            ],
        );
    }
    for g in 0..num_groups {
        for dup in 0..children_per_group {
            add_row(
                &child,
                &[
                    (
                        "id",
                        Value::F64((1000 + g * children_per_group + dup) as f64),
                    ),
                    ("parentGroupId", Value::F64(g as f64)),
                ],
            );
        }
    }

    let parent_input = parent.borrow_mut().connect(None, None, None, None, None);
    let child_input = child.borrow_mut().connect(None, None, None, None, None);

    FlippedJoin::new(FlippedJoinArgs {
        parent: parent_input,
        child: child_input,
        parent_key: vec!["groupId".to_string()],
        child_key: vec!["parentGroupId".to_string()],
        relationship_name: "children".to_string(),
        hidden: false,
        system: System::Client,
    })
}

// K=2 streams, reverse → [5, 4, 3, 2, 1, 0]
#[test]
fn test_stream_merge_k2_reverse() {
    let fj = setup_group_join(2, 3, 1);
    let req = FetchRequest {
        reverse: true,
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&req)).collect();
    let ids: Vec<f64> = nodes
        .iter()
        .map(|n| {
            n.row
                .get("id")
                .and_then(|v| match v {
                    Value::F64(n) => Some(*n),
                    _ => None,
                })
                .unwrap_or(-1.0)
        })
        .collect();
    assert_eq!(ids, vec![5.0, 4.0, 3.0, 2.0, 1.0, 0.0]);
}

// K=10 streams, forward → [0..49]
#[test]
fn test_stream_merge_k10_forward() {
    let fj = setup_group_join(10, 5, 1);
    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 50);
    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            node.row.get("id"),
            Some(&Value::F64(i as f64)),
            "Parent at index {} should have id={}",
            i,
            i
        );
    }
}

// K=10 streams, reverse → [49..0]
#[test]
fn test_stream_merge_k10_reverse() {
    let fj = setup_group_join(10, 5, 1);
    let req = FetchRequest {
        reverse: true,
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 50);
    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            node.row.get("id"),
            Some(&Value::F64((49 - i) as f64)),
            "Parent at index {} should have id={}",
            i,
            49 - i
        );
    }
}

// K=20 with shared parent-keys — dedup + heap merge
// 20 groups × 5 per group = 100 parents, 4 children per group = 80 children
#[test]
fn test_stream_merge_k20_dedup() {
    let fj = setup_group_join(20, 5, 4);
    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(fj.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 100);
    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            node.row.get("id"),
            Some(&Value::F64(i as f64)),
            "Parent at index {} should have id={}",
            i,
            i
        );
        let children = get_rel_children(node, "children");
        assert_eq!(children.len(), 4, "Each parent should have 4 children");
    }
}
