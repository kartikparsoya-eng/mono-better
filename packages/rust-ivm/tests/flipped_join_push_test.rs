//! Tests for FlippedJoin push operations.
//! Port of TS `flipped-join.push.test.ts` (v1.7.0).
//! Tests incremental changes (add/remove/edit) through FlippedJoin.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::data::{Node, Row, Value};
use rust_ivm::ivm::flipped_join::{FlippedJoin, FlippedJoinArgs};
use rust_ivm::ivm::operator::{FetchRequest, Input, OutputHandle};
use rust_ivm::ivm::schema::{ColumnType, System};
use rust_ivm::ivm::source::{CollectOutput, MemorySource};
use rust_ivm::ivm::source::{
    make_source_change_add, make_source_change_edit, make_source_change_remove,
};

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

/// NEW-6 regression: the remove overlay's re-apply must drop ONLY the node
/// spliced back in by `fetch` — TS filters by reference identity
/// (`n !== inprogressChildChange[NODE]`, flipped-join.ts:358-360; the Rust
/// twin is `Arc::ptr_eq` on the row). The old code matched by `child_key`
/// (the join key), so every SIBLING child sharing the key was dropped too and
/// parent i2 — which keeps child c2b — vanished from mid-push fetches
/// ([i1,i3] instead of TS's [i1,i2,i3]). Proven by temp-revert.
#[test]
fn test_fetch_during_remove_push_splices_removed_child_at_sorted_position() {
    use rust_ivm::ivm::change::Change;
    use rust_ivm::ivm::operator::Output;

    // Three parents, one child each; children c1<c2<c3 map to i1,i2,i3.
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
    for (i, c) in [("i1", "c1"), ("i2", "c2"), ("i3", "c3")] {
        add_row(&parent, &[("id", str_val(i))]);
        add_row(&child, &[("id", str_val(c)), ("issueID", str_val(i))]);
    }
    // i2 keeps a SECOND child so it survives the overlay's re-apply filter —
    // the emitted parent ORDER is then determined by the child-splice position.
    add_row(
        &child,
        &[("id", str_val("c2b")), ("issueID", str_val("i2"))],
    );
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

    // Downstream output that re-fetches THROUGH the FlippedJoin while the
    // push is in progress (what exists/take/skip do), recording parent order.
    struct FetchDuringPushOutput {
        fj: Rc<RefCell<FlippedJoin>>,
        orders: Rc<RefCell<Vec<Vec<Value>>>>,
    }
    impl Output for FetchDuringPushOutput {
        fn push(&mut self, _change: Change, _pusher: &dyn rust_ivm::ivm::operator::InputBase) {
            let ids: Vec<Value> = rust_ivm::ivm::stream::skip_yields(
                self.fj.borrow().fetch(&FetchRequest::default()),
            )
            .map(|n| n.row.get("id").cloned().unwrap_or(Value::Null))
            .collect();
            self.orders.borrow_mut().push(ids);
        }
    }
    let orders = Rc::new(RefCell::new(Vec::new()));
    let out = Rc::new(RefCell::new(FetchDuringPushOutput {
        fj: fj.clone(),
        orders: orders.clone(),
    }));
    fj.borrow_mut().set_output(out as OutputHandle);

    // Remove the MIDDLE child.
    child
        .borrow_mut()
        .push(make_source_change_remove(make_row(&[
            ("id", str_val("c2")),
            ("issueID", str_val("i2")),
        ])));

    let orders = orders.borrow();
    assert!(!orders.is_empty(), "the push must have reached the output");
    assert_eq!(
        orders[0],
        vec![str_val("i1"), str_val("i2"), str_val("i3")],
        "the re-apply must remove only the spliced change node (TS reference \
         identity), keeping i2 alive through its remaining child c2b"
    );
}

/// NEW-4 regression: `FlippedJoin::fetch`'s remove overlay splices the removed
/// node back into the refetched child list at its SORTED position (TS
/// flipped-join.ts:195-202: binarySearch = leftmost insertion point). The old
/// `partition_point(.. == Less)` predicate was inverted (true for the SUFFIX),
/// so a removed MIDDLE child spliced at index 0. Observable with a NON-UNIQUE
/// parent key: while the push is at parent p1 (position=p1), parent p2 has
/// not been pushed yet, so its group KEEPS the spliced node — at the splice
/// position. TS: [k1,k2,k3]; inverted predicate: [k2,k1,k3]. Proven by
/// temp-revert.
#[test]
fn test_remove_overlay_splice_position_for_unpushed_parent() {
    use rust_ivm::ivm::change::Change;
    use rust_ivm::ivm::operator::Output;

    // TWO parents share cat="x" (non-unique join key); three children k1<k2<k3
    // all join on cat="x".
    let parent = make_source(
        "issues",
        &["id"],
        &[
            ("id", ColumnType::String { optional: false }),
            ("cat", ColumnType::String { optional: false }),
        ],
    );
    let child = make_source(
        "comments",
        &["id"],
        &[
            ("id", ColumnType::String { optional: false }),
            ("cat", ColumnType::String { optional: false }),
        ],
    );
    add_row(&parent, &[("id", str_val("p1")), ("cat", str_val("x"))]);
    add_row(&parent, &[("id", str_val("p2")), ("cat", str_val("x"))]);
    for k in ["k1", "k2", "k3"] {
        add_row(&child, &[("id", str_val(k)), ("cat", str_val("x"))]);
    }
    let parent_input = parent.borrow_mut().connect(None, None, None, None);
    let child_input = child.borrow_mut().connect(None, None, None, None);
    let fj = FlippedJoin::new(FlippedJoinArgs {
        parent: parent_input,
        child: child_input,
        parent_key: vec!["cat".to_string()],
        child_key: vec!["cat".to_string()],
        relationship_name: "comments".to_string(),
        hidden: false,
        system: System::Client,
    });

    // On the FIRST push (position = p1), fetch through the FJ and record p2's
    // relationship order (p2 > position → overlay keeps the spliced node).
    struct RecordP2Output {
        fj: Rc<RefCell<FlippedJoin>>,
        p2_children: Rc<RefCell<Vec<Vec<Value>>>>,
    }
    impl Output for RecordP2Output {
        fn push(&mut self, _change: Change, _pusher: &dyn rust_ivm::ivm::operator::InputBase) {
            if !self.p2_children.borrow().is_empty() {
                return; // only the first (position = p1) push matters
            }
            for n in
                rust_ivm::ivm::stream::skip_yields(self.fj.borrow().fetch(&FetchRequest::default()))
            {
                if n.row.get("id") == Some(&str_val("p2")) {
                    let ids: Vec<Value> = get_rel_children(&n, "comments")
                        .iter()
                        .map(|c| c.row.get("id").cloned().unwrap_or(Value::Null))
                        .collect();
                    self.p2_children.borrow_mut().push(ids);
                }
            }
        }
    }
    let p2_children = Rc::new(RefCell::new(Vec::new()));
    let out = Rc::new(RefCell::new(RecordP2Output {
        fj: fj.clone(),
        p2_children: p2_children.clone(),
    }));
    fj.borrow_mut().set_output(out as OutputHandle);

    // Remove the MIDDLE child.
    child
        .borrow_mut()
        .push(make_source_change_remove(make_row(&[
            ("id", str_val("k2")),
            ("cat", str_val("x")),
        ])));

    let p2_children = p2_children.borrow();
    assert!(
        !p2_children.is_empty(),
        "the mid-push fetch must have seen p2"
    );
    assert_eq!(
        p2_children[0],
        vec![str_val("k1"), str_val("k2"), str_val("k3")],
        "for the not-yet-pushed parent, the removed child must appear at its \
         SORTED splice position (TS binarySearch), not at index 0"
    );
}
