//! Tests for FlippedJoin push operations.
//! Port of TS `flipped-join.push.test.ts` (v1.7.0).
//! Tests incremental changes (add/remove/edit) through FlippedJoin.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::change::{
    make_source_change_add, make_source_change_edit, make_source_change_remove,
};
use rust_ivm::ivm::data::{Node, Row, Value};
use rust_ivm::ivm::flipped_join::{FlippedJoin, FlippedJoinArgs};
use rust_ivm::ivm::operator::{FetchRequest, Input, OutputHandle};
use rust_ivm::ivm::schema::{ColumnType, System};
use rust_ivm::ivm::source::{CollectOutput, MemorySource};

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

fn get_rel_children(node: &Node, rel_name: &str) -> Vec<Node> {
    node.relationships
        .get(rel_name)
        .map(|f| rust_ivm::ivm::stream::skip_yields(f()).collect())
        .unwrap_or_default()
}

struct PushSetup {
    parent: Rc<RefCell<MemorySource>>,
    child: Rc<RefCell<MemorySource>>,
    fj: Rc<RefCell<FlippedJoin>>,
    collector: Rc<RefCell<CollectOutput>>,
}

fn setup_flipped_join(
    parent_data: &[Vec<(&str, Value)>],
    child_data: &[Vec<(&str, Value)>],
) -> PushSetup {
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

    let parent_input = parent.borrow_mut().connect(None, None, None, None);
    let child_input = child.borrow_mut().connect(None, None, None, None);

    let fj = FlippedJoin::new(FlippedJoinArgs {
        parent: parent_input,
        child: child_input,
        parent_key: vec!["id".to_string()],
        child_key: vec!["issueID".to_string()],
        relationship_name: "comments".to_string(),
        hidden: false,
        system: System::Client,
    });

    let collector = Rc::new(RefCell::new(CollectOutput::new()));
    fj.borrow_mut()
        .set_output(collector.clone() as OutputHandle);

    PushSetup {
        parent,
        child,
        fj,
        collector,
    }
}

fn collected_changes(collector: &Rc<RefCell<CollectOutput>>) -> Vec<rust_ivm::ivm::change::Change> {
    collector.borrow().changes.clone()
}

#[test]
fn test_push_add_child_to_existing_parent() {
    let setup = setup_flipped_join(&[vec![("id", str_val("i1"))]], &[]);

    setup
        .child
        .borrow_mut()
        .push(make_source_change_add(make_row(&[
            ("id", str_val("c1")),
            ("issueID", str_val("i1")),
        ])));

    let changes = collected_changes(&setup.collector);
    assert_eq!(changes.len(), 1, "Should produce one change");
    assert!(matches!(changes[0], rust_ivm::ivm::change::Change::Add(_)));

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(setup.fj.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(nodes.len(), 1, "Parent should now have a child");
}

#[test]
fn test_push_remove_child_from_parent() {
    let setup = setup_flipped_join(
        &[vec![("id", str_val("i1"))]],
        &[vec![("id", str_val("c1")), ("issueID", str_val("i1"))]],
    );

    setup
        .child
        .borrow_mut()
        .push(make_source_change_remove(make_row(&[
            ("id", str_val("c1")),
            ("issueID", str_val("i1")),
        ])));

    let changes = collected_changes(&setup.collector);
    assert_eq!(changes.len(), 1, "Should produce one change");
    assert!(matches!(
        changes[0],
        rust_ivm::ivm::change::Change::Remove(_)
    ));

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(setup.fj.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(
        nodes.len(),
        0,
        "Parent with no children should not appear (inner join)"
    );
}

#[test]
fn test_push_add_parent_with_matching_child() {
    let setup = setup_flipped_join(
        &[],
        &[vec![("id", str_val("c1")), ("issueID", str_val("i1"))]],
    );

    setup
        .parent
        .borrow_mut()
        .push(make_source_change_add(make_row(&[("id", str_val("i1"))])));

    let changes = collected_changes(&setup.collector);
    assert_eq!(changes.len(), 1, "Should produce one change for parent add");
    assert!(matches!(changes[0], rust_ivm::ivm::change::Change::Add(_)));

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(setup.fj.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(
        nodes.len(),
        1,
        "New parent with matching child should appear"
    );
}

#[test]
fn test_push_add_parent_no_matching_child() {
    let setup = setup_flipped_join(
        &[],
        &[vec![("id", str_val("c1")), ("issueID", str_val("i2"))]],
    );

    setup
        .parent
        .borrow_mut()
        .push(make_source_change_add(make_row(&[("id", str_val("i1"))])));

    let changes = collected_changes(&setup.collector);
    assert_eq!(
        changes.len(),
        0,
        "No changes when parent has no matching child (inner join)"
    );

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(setup.fj.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(
        nodes.len(),
        0,
        "Parent without matching child should not appear"
    );
}

#[test]
fn test_push_remove_parent() {
    let setup = setup_flipped_join(
        &[vec![("id", str_val("i1"))]],
        &[vec![("id", str_val("c1")), ("issueID", str_val("i1"))]],
    );

    setup
        .parent
        .borrow_mut()
        .push(make_source_change_remove(make_row(&[(
            "id",
            str_val("i1"),
        )])));

    let changes = collected_changes(&setup.collector);
    assert_eq!(
        changes.len(),
        1,
        "Should produce one change for parent remove"
    );
    assert!(matches!(
        changes[0],
        rust_ivm::ivm::change::Change::Remove(_)
    ));

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(setup.fj.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(nodes.len(), 0, "Removed parent should not appear");
}

#[test]
fn test_push_edit_child_same_join_key() {
    let setup = setup_flipped_join(
        &[vec![("id", str_val("i1"))]],
        &[vec![("id", str_val("c1")), ("issueID", str_val("i1"))]],
    );

    setup.child.borrow_mut().push(make_source_change_edit(
        make_row(&[("id", str_val("c1")), ("issueID", str_val("i1"))]),
        make_row(&[("id", str_val("c1")), ("issueID", str_val("i1"))]),
    ));

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(setup.fj.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(nodes.len(), 1, "Parent should still have the edited child");
}

#[test]
fn test_push_add_child_to_parent_with_existing_child() {
    let setup = setup_flipped_join(
        &[vec![("id", str_val("i1"))]],
        &[vec![("id", str_val("c1")), ("issueID", str_val("i1"))]],
    );

    setup
        .child
        .borrow_mut()
        .push(make_source_change_add(make_row(&[
            ("id", str_val("c2")),
            ("issueID", str_val("i1")),
        ])));

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(setup.fj.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(nodes.len(), 1, "Should still have one parent");

    let children = get_rel_children(&nodes[0], "comments");
    assert_eq!(children.len(), 2, "Parent should now have 2 children");
}

#[test]
fn test_push_multiple_children_same_parent() {
    let setup = setup_flipped_join(&[vec![("id", str_val("i1"))]], &[]);

    setup
        .child
        .borrow_mut()
        .push(make_source_change_add(make_row(&[
            ("id", str_val("c1")),
            ("issueID", str_val("i1")),
        ])));
    setup
        .child
        .borrow_mut()
        .push(make_source_change_add(make_row(&[
            ("id", str_val("c2")),
            ("issueID", str_val("i1")),
        ])));
    setup
        .child
        .borrow_mut()
        .push(make_source_change_add(make_row(&[
            ("id", str_val("c3")),
            ("issueID", str_val("i1")),
        ])));

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(setup.fj.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(nodes.len(), 1);

    let children = get_rel_children(&nodes[0], "comments");
    assert_eq!(children.len(), 3, "Should have 3 children after 3 adds");
}

#[test]
fn test_push_remove_one_of_two_children() {
    let setup = setup_flipped_join(
        &[vec![("id", str_val("i1"))]],
        &[
            vec![("id", str_val("c1")), ("issueID", str_val("i1"))],
            vec![("id", str_val("c2")), ("issueID", str_val("i1"))],
        ],
    );

    setup
        .child
        .borrow_mut()
        .push(make_source_change_remove(make_row(&[
            ("id", str_val("c1")),
            ("issueID", str_val("i1")),
        ])));

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(setup.fj.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(
        nodes.len(),
        1,
        "Parent should still appear with remaining child"
    );

    let children = get_rel_children(&nodes[0], "comments");
    assert_eq!(children.len(), 1, "Should have 1 child after removing one");
    assert_eq!(
        children[0].row.get("id").cloned().unwrap_or(Value::Null),
        str_val("c2")
    );
}
