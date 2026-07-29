//! Tests for FlippedJoin sibling relationships — port of TS `flipped-join.sibling.test.ts` (v1.7.0).
//! Tests multiple FlippedJoins on the same parent source (sibling relationships).

use std::cell::RefCell;
use std::rc::Rc;
use std::collections::HashMap;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::change::{make_source_change_add, make_source_change_remove};
use rust_ivm::ivm::data::{Node, Row, Value};
use rust_ivm::ivm::flipped_join::{FlippedJoin, FlippedJoinArgs};
use rust_ivm::ivm::operator::{FetchRequest, Input, OutputHandle};
use rust_ivm::ivm::schema::{ColumnType, System};
use rust_ivm::ivm::source::{CollectOutput, MemorySource};

fn str_val(s: &str) -> Value {
    Value::Str(Arc::from(s))
}

fn make_source(name: &str, pk: &[&str], columns: &[(&str, ColumnType)]) -> Rc<RefCell<MemorySource>> {
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

fn get_rel_children(node: &Node, rel_name: &str) -> Vec<Node> {
    node.relationships
        .get(rel_name)
        .map(|f| rust_ivm::ivm::stream::skip_yields(f()).collect())
        .unwrap_or_default()
}

struct SiblingSetup {
    issues: Rc<RefCell<MemorySource>>,
    comments: Rc<RefCell<MemorySource>>,
    owners: Rc<RefCell<MemorySource>>,
    comments_join: Rc<RefCell<FlippedJoin>>,
    owners_join: Rc<RefCell<FlippedJoin>>,
    collector: Rc<RefCell<CollectOutput>>,
}

fn setup_siblings(
    issue_data: &[Vec<(&str, Value)>],
    comment_data: &[Vec<(&str, Value)>],
    owner_data: &[Vec<(&str, Value)>],
) -> SiblingSetup {
    let issues = make_source("issues", &["id"], &[
        ("id", ColumnType::String { optional: false }),
        ("ownerId", ColumnType::String { optional: false }),
    ]);
    let comments = make_source("comments", &["id"], &[
        ("id", ColumnType::String { optional: false }),
        ("issueId", ColumnType::String { optional: false }),
    ]);
    let owners = make_source("owners", &["id"], &[
        ("id", ColumnType::String { optional: false }),
    ]);

    for row in issue_data {
        add_row(&issues, row);
    }
    for row in comment_data {
        add_row(&comments, row);
    }
    for row in owner_data {
        add_row(&owners, row);
    }

    let issue_input_comments = issues.borrow_mut().connect(None, None, None, None);
    let comment_input = comments.borrow_mut().connect(None, None, None, None);

    let comments_join = FlippedJoin::new(FlippedJoinArgs {
        parent: issue_input_comments,
        child: comment_input,
        parent_key: vec!["id".to_string()],
        child_key: vec!["issueId".to_string()],
        relationship_name: "comments".to_string(),
        hidden: false,
        system: System::Client,
    });

    let issue_input_owners = issues.borrow_mut().connect(None, None, None, None);
    let owner_input = owners.borrow_mut().connect(None, None, None, None);

    let owners_join = FlippedJoin::new(FlippedJoinArgs {
        parent: issue_input_owners,
        child: owner_input,
        parent_key: vec!["ownerId".to_string()],
        child_key: vec!["id".to_string()],
        relationship_name: "owners".to_string(),
        hidden: false,
        system: System::Client,
    });

    let collector = Rc::new(RefCell::new(CollectOutput::new()));
    comments_join.borrow_mut().set_output(collector.clone() as OutputHandle);
    let owners_collector = Rc::new(RefCell::new(CollectOutput::new()));
    owners_join.borrow_mut().set_output(owners_collector.clone() as OutputHandle);

    SiblingSetup {
        issues,
        comments,
        owners,
        comments_join,
        owners_join,
        collector,
    }
}

#[test]
fn test_sibling_fetch_both_relationships() {
    let setup = setup_siblings(
        &[
            vec![("id", str_val("i1")), ("ownerId", str_val("o1"))],
            vec![("id", str_val("i2")), ("ownerId", str_val("o2"))],
        ],
        &[
            vec![("id", str_val("c1")), ("issueId", str_val("i1"))],
            vec![("id", str_val("c2")), ("issueId", str_val("i2"))],
        ],
        &[
            vec![("id", str_val("o1"))],
            vec![("id", str_val("o2"))],
        ],
    );

    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(setup.comments_join.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 2, "Both issues should have comments");

    for node in &nodes {
        let comments = get_rel_children(node, "comments");
        assert_eq!(comments.len(), 1, "Each issue should have 1 comment");
    }
}

#[test]
fn test_sibling_fetch_owners() {
    let setup = setup_siblings(
        &[
            vec![("id", str_val("i1")), ("ownerId", str_val("o1"))],
        ],
        &[
            vec![("id", str_val("c1")), ("issueId", str_val("i1"))],
        ],
        &[
            vec![("id", str_val("o1"))],
        ],
    );

    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(setup.owners_join.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 1, "One issue should have an owner");

    let owners = get_rel_children(&nodes[0], "owners");
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].row.get("id").cloned().unwrap_or(Value::Null), str_val("o1"));
}

#[test]
fn test_sibling_push_new_issue_existing_owner() {
    let setup = setup_siblings(
        &[
            vec![("id", str_val("i1")), ("ownerId", str_val("o1"))],
        ],
        &[
            vec![("id", str_val("c1")), ("issueId", str_val("i1"))],
        ],
        &[
            vec![("id", str_val("o1"))],
        ],
    );

    let change = make_source_change_add(
        Arc::new(FxHashMap::from_iter([
            ("id".to_string(), str_val("i2")),
            ("ownerId".to_string(), str_val("o1")),
        ])),
    );
    setup.issues.borrow_mut().push(change);

    let changes = setup.collector.borrow().changes.clone();
    assert!(changes.is_empty(), "New issue with no comments produces no output (inner join)");
}

#[test]
fn test_sibling_push_new_comment() {
    let setup = setup_siblings(
        &[
            vec![("id", str_val("i1")), ("ownerId", str_val("o1"))],
        ],
        &[
            vec![("id", str_val("c1")), ("issueId", str_val("i1"))],
        ],
        &[
            vec![("id", str_val("o1"))],
        ],
    );

    let change = make_source_change_add(
        Arc::new(FxHashMap::from_iter([
            ("id".to_string(), str_val("c2")),
            ("issueId".to_string(), str_val("i1")),
        ])),
    );
    setup.comments.borrow_mut().push(change);

    let changes = setup.collector.borrow().changes.clone();
    assert!(!changes.is_empty(), "Pushing a new comment should produce changes");
}

#[test]
fn test_sibling_push_new_owner() {
    let setup = setup_siblings(
        &[
            vec![("id", str_val("i1")), ("ownerId", str_val("o1"))],
        ],
        &[
            vec![("id", str_val("c1")), ("issueId", str_val("i1"))],
        ],
        &[
            vec![("id", str_val("o1"))],
        ],
    );

    let change = make_source_change_add(
        Arc::new(FxHashMap::from_iter([
            ("id".to_string(), str_val("o2")),
        ])),
    );
    setup.owners.borrow_mut().push(change);

    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(setup.owners_join.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 1);

    let owners = get_rel_children(&nodes[0], "owners");
    assert_eq!(owners.len(), 1, "Issue i1 should still have owner o1");
}

#[test]
fn test_sibling_two_owners_same_issue() {
    let setup = setup_siblings(
        &[
            vec![("id", str_val("i1")), ("ownerId", str_val("o1"))],
            vec![("id", str_val("i2")), ("ownerId", str_val("o2"))],
        ],
        &[
            vec![("id", str_val("c1")), ("issueId", str_val("i1"))],
            vec![("id", str_val("c2")), ("issueId", str_val("i2"))],
        ],
        &[
            vec![("id", str_val("o1"))],
            vec![("id", str_val("o2"))],
        ],
    );

    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(setup.owners_join.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 2);

    let owners0 = get_rel_children(&nodes[0], "owners");
    assert_eq!(owners0.len(), 1);
    assert_eq!(owners0[0].row.get("id").cloned().unwrap_or(Value::Null), str_val("o1"));

    let owners1 = get_rel_children(&nodes[1], "owners");
    assert_eq!(owners1.len(), 1);
    assert_eq!(owners1[0].row.get("id").cloned().unwrap_or(Value::Null), str_val("o2"));
}

#[test]
fn test_sibling_inner_join_no_owner() {
    let setup = setup_siblings(
        &[
            vec![("id", str_val("i1")), ("ownerId", str_val("o1"))],
            vec![("id", str_val("i2")), ("ownerId", str_val("o3"))],
        ],
        &[
            vec![("id", str_val("c1")), ("issueId", str_val("i1"))],
            vec![("id", str_val("c2")), ("issueId", str_val("i2"))],
        ],
        &[
            vec![("id", str_val("o1"))],
        ],
    );

    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(setup.owners_join.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 1, "Only issue i1 has a matching owner (inner join)");
    assert_eq!(nodes[0].row.get("id").cloned().unwrap_or(Value::Null), str_val("i1"));
}
