//! Tests for chained FlippedJoins — port of TS `flipped-join.more-fetch.test.ts` (v1.7.0).
//! Tests one:many:one chained flipped joins where parent constraints
//! are translated to child constraints via multiConstraints.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::data::{Node, Value};
use rust_ivm::ivm::flipped_join::{FlippedJoin, FlippedJoinArgs};
use rust_ivm::ivm::operator::{FetchRequest, Input};
use rust_ivm::ivm::schema::{ColumnType, System};
use rust_ivm::ivm::source::MemorySource;

fn str_val(s: &str) -> Value {
    Value::Str(Arc::from(s))
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

fn get_rel_children(node: &Node, rel_name: &str) -> Vec<Node> {
    node.relationships
        .get(rel_name)
        .map(|f| rust_ivm::ivm::stream::skip_yields(f()).collect())
        .unwrap_or_default()
}

#[allow(dead_code)]
struct ChainedSetup {
    #[allow(dead_code)]
    issues: Rc<RefCell<MemorySource>>,
    #[allow(dead_code)]
    issue_labels: Rc<RefCell<MemorySource>>,
    labels: Rc<RefCell<MemorySource>>,
    outer_join: Rc<RefCell<FlippedJoin>>,
    inner_join: Rc<RefCell<FlippedJoin>>,
}

fn setup_chained(
    issue_data: &[Vec<(&str, Value)>],
    issue_label_data: &[Vec<(&str, Value)>],
    label_data: &[Vec<(&str, Value)>],
) -> ChainedSetup {
    let issues = make_source(
        "issue",
        &["id"],
        &[("id", ColumnType::String { optional: false })],
    );
    let issue_labels = make_source(
        "issueLabel",
        &["issueID", "labelID"],
        &[
            ("issueID", ColumnType::String { optional: false }),
            ("labelID", ColumnType::String { optional: false }),
        ],
    );
    let labels = make_source(
        "label",
        &["id"],
        &[
            ("id", ColumnType::String { optional: false }),
            ("name", ColumnType::String { optional: false }),
        ],
    );

    for row in issue_data {
        add_row(&issues, row);
    }
    for row in issue_label_data {
        add_row(&issue_labels, row);
    }
    for row in label_data {
        add_row(&labels, row);
    }

    // Inner join: issueLabel ← label
    // parentKey: labelID (on issueLabel), childKey: id (on label)
    let il_input_parent = issue_labels.borrow_mut().connect(None, None, None, None);
    let label_input = labels.borrow_mut().connect(None, None, None, None);
    let inner_join = FlippedJoin::new(FlippedJoinArgs {
        parent: il_input_parent,
        child: label_input,
        parent_key: vec!["labelID".to_string()],
        child_key: vec!["id".to_string()],
        relationship_name: "labels".to_string(),
        hidden: false,
        system: System::Client,
    });

    // Outer join: issue ← inner_join (issueLabel with labels)
    // parentKey: id (on issue), childKey: issueID (on issueLabel)
    let issue_input = issues.borrow_mut().connect(None, None, None, None);
    let outer_join = FlippedJoin::new(FlippedJoinArgs {
        parent: issue_input,
        child: inner_join.clone(),
        parent_key: vec!["id".to_string()],
        child_key: vec!["issueID".to_string()],
        relationship_name: "issueLabels".to_string(),
        hidden: false,
        system: System::Client,
    });

    ChainedSetup {
        issues,
        issue_labels,
        labels,
        outer_join,
        inner_join,
    }
}

#[test]
fn test_chained_fetch_basic() {
    let setup = setup_chained(
        &[vec![("id", str_val("i1"))], vec![("id", str_val("i2"))]],
        &[
            vec![("issueID", str_val("i1")), ("labelID", str_val("l1"))],
            vec![("issueID", str_val("i2")), ("labelID", str_val("l2"))],
        ],
        &[
            vec![("id", str_val("l1")), ("name", str_val("label1"))],
            vec![("id", str_val("l2")), ("name", str_val("label2"))],
        ],
    );

    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(
        setup.outer_join.borrow().fetch(&FetchRequest::default()),
    )
    .collect();
    assert_eq!(nodes.len(), 2, "Both issues should have issueLabels");

    let issue_labels_0 = get_rel_children(&nodes[0], "issueLabels");
    assert_eq!(issue_labels_0.len(), 1, "Issue i1 should have 1 issueLabel");
}

#[test]
fn test_chained_fetch_no_labels() {
    let setup = setup_chained(
        &[vec![("id", str_val("i1"))]],
        &[vec![("issueID", str_val("i1")), ("labelID", str_val("l1"))]],
        &[],
    );

    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(
        setup.outer_join.borrow().fetch(&FetchRequest::default()),
    )
    .collect();
    assert_eq!(
        nodes.len(),
        0,
        "No labels means inner join excludes issueLabel"
    );
}

#[test]
fn test_chained_fetch_no_issue_labels() {
    let setup = setup_chained(
        &[vec![("id", str_val("i1"))]],
        &[],
        &[vec![("id", str_val("l1")), ("name", str_val("label1"))]],
    );

    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(
        setup.outer_join.borrow().fetch(&FetchRequest::default()),
    )
    .collect();
    assert_eq!(
        nodes.len(),
        0,
        "No issueLabels means inner join excludes issue"
    );
}

#[test]
fn test_chained_fetch_multiple_labels_per_issue() {
    let setup = setup_chained(
        &[vec![("id", str_val("i1"))]],
        &[
            vec![("issueID", str_val("i1")), ("labelID", str_val("l1"))],
            vec![("issueID", str_val("i1")), ("labelID", str_val("l2"))],
        ],
        &[
            vec![("id", str_val("l1")), ("name", str_val("label1"))],
            vec![("id", str_val("l2")), ("name", str_val("label2"))],
        ],
    );

    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(
        setup.outer_join.borrow().fetch(&FetchRequest::default()),
    )
    .collect();
    assert_eq!(nodes.len(), 1, "One issue with two labels");

    let issue_labels = get_rel_children(&nodes[0], "issueLabels");
    assert_eq!(issue_labels.len(), 2, "Issue should have 2 issueLabels");
}

#[test]
fn test_chained_fetch_with_constraint() {
    let setup = setup_chained(
        &[vec![("id", str_val("i1"))], vec![("id", str_val("i2"))]],
        &[
            vec![("issueID", str_val("i1")), ("labelID", str_val("l1"))],
            vec![("issueID", str_val("i2")), ("labelID", str_val("l2"))],
        ],
        &[
            vec![("id", str_val("l1")), ("name", str_val("label1"))],
            vec![("id", str_val("l2")), ("name", str_val("label2"))],
        ],
    );

    let mut constraint = rust_ivm::ivm::constraint::Constraint::default();
    constraint.insert("id".to_string(), str_val("i2"));
    let req = FetchRequest {
        constraint: Some(constraint),
        ..Default::default()
    };

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(setup.outer_join.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("i2")
    );
}

#[test]
fn test_chained_fetch_inner_join_semantics() {
    let setup = setup_chained(
        &[
            vec![("id", str_val("i1"))],
            vec![("id", str_val("i2"))],
            vec![("id", str_val("i3"))],
        ],
        &[
            vec![("issueID", str_val("i1")), ("labelID", str_val("l1"))],
            vec![("issueID", str_val("i3")), ("labelID", str_val("l2"))],
        ],
        &[
            vec![("id", str_val("l1")), ("name", str_val("label1"))],
            vec![("id", str_val("l2")), ("name", str_val("label2"))],
        ],
    );

    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(
        setup.outer_join.borrow().fetch(&FetchRequest::default()),
    )
    .collect();
    assert_eq!(
        nodes.len(),
        2,
        "Only issues with matching labels (i1, i3) should appear"
    );
    let ids: Vec<Value> = nodes
        .iter()
        .map(|n| n.row.get("id").cloned().unwrap_or(Value::Null))
        .collect();
    assert!(ids.contains(&str_val("i1")));
    assert!(ids.contains(&str_val("i3")));
    assert!(!ids.contains(&str_val("i2")));
}

#[test]
fn test_chained_fetch_compound_key() {
    let issues = make_source(
        "issue",
        &["id"],
        &[("id", ColumnType::String { optional: false })],
    );
    let junction = make_source(
        "junction",
        &["a", "b"],
        &[
            ("issueID", ColumnType::String { optional: false }),
            ("a", ColumnType::String { optional: false }),
            ("b", ColumnType::String { optional: false }),
        ],
    );
    let targets = make_source(
        "target",
        &["a", "b"],
        &[
            ("a", ColumnType::String { optional: false }),
            ("b", ColumnType::String { optional: false }),
            ("name", ColumnType::String { optional: false }),
        ],
    );

    add_row(&issues, &[("id", str_val("i1"))]);
    add_row(
        &junction,
        &[
            ("issueID", str_val("i1")),
            ("a", str_val("x")),
            ("b", str_val("1")),
        ],
    );
    add_row(
        &targets,
        &[
            ("a", str_val("x")),
            ("b", str_val("1")),
            ("name", str_val("target1")),
        ],
    );

    let j_input_parent = junction.borrow_mut().connect(None, None, None, None);
    let t_input = targets.borrow_mut().connect(None, None, None, None);
    let _inner = FlippedJoin::new(FlippedJoinArgs {
        parent: j_input_parent,
        child: t_input,
        parent_key: vec!["a".to_string(), "b".to_string()],
        child_key: vec!["a".to_string(), "b".to_string()],
        relationship_name: "targets".to_string(),
        hidden: false,
        system: System::Client,
    });

    let i_input = issues.borrow_mut().connect(None, None, None, None);
    let j_input_child = junction.borrow_mut().connect(None, None, None, None);
    let outer = FlippedJoin::new(FlippedJoinArgs {
        parent: i_input,
        child: j_input_child,
        parent_key: vec!["id".to_string()],
        child_key: vec!["issueID".to_string()],
        relationship_name: "junctions".to_string(),
        hidden: false,
        system: System::Client,
    });

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(outer.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(
        nodes.len(),
        1,
        "Issue with compound-key junction should match"
    );
}
