//! TS-golden tests for the join-layer overlay generators — port of
//! `zql/src/ivm/join-utils.test.ts` (`generateWithOverlayUnordered` cases,
//! verbatim) plus cases for the ordered `generateWithOverlay` derived line by
//! line from join-utils.ts:19-125 (TS has no unit test for it; its semantics
//! are "undo the in-flight change for parents it has not reached yet": an ADD
//! overlay SUPPRESSES the matching node, a REMOVE overlay RE-INSERTS the node
//! at its sorted position, an EDIT re-inserts the old row and suppresses the
//! new one, and the generator asserts the overlay was applied).
//!
//! Non-vacuous: the previous rust generator had "apply" semantics (ADD
//! inserted a second copy of the node, REMOVE appended the removed node at the
//! end of the stream, nothing ever asserted) and dropped every `Yield`; all of
//! the cases below fail against it.

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rust_ivm::ivm::change::{
    ChildData, make_add_change, make_child_change, make_edit_change, make_remove_change,
};
use rust_ivm::ivm::data::{Node, SortOrder, Value};
use rust_ivm::ivm::join_utils::{
    generate_with_overlay, generate_with_overlay_no_yield_unordered,
    generate_with_overlay_unordered,
};
use rust_ivm::ivm::schema::{ColumnType, SourceSchema, System};
use rust_ivm::ivm::stream::{NodeStream, StreamItem, from_vec};
use rustc_hash::FxHashMap;

fn row(pairs: &[(&str, Value)]) -> rust_ivm::ivm::data::Row {
    let mut r = FxHashMap::default();
    for (k, v) in pairs {
        r.insert(k.to_string(), v.clone());
    }
    Arc::new(r)
}

fn n(id: i64) -> Node {
    Node::new(row(&[("id", Value::F64(id as f64))]))
}

fn nv(id: i64, val: &str) -> Node {
    Node::new(row(&[
        ("id", Value::F64(id as f64)),
        ("val", Value::Str(val.into())),
    ]))
}

fn make_schema(pk: &[&str]) -> SourceSchema {
    let sort: SortOrder = Arc::new(
        pk.iter()
            .map(|c| [c.to_string(), "asc".to_string()])
            .collect(),
    );
    SourceSchema {
        table_name: "test".into(),
        columns: HashMap::from([("id".to_string(), ColumnType::Number { optional: false })]),
        primary_key: pk.iter().map(|s| s.to_string()).collect(),
        relationships: HashMap::new(),
        relationship_order: vec![],
        compare_rows: rust_ivm::ivm::data::make_comparator(sort.clone(), false),
        is_hidden: false,
        sort: Some(sort),
        system: System::Client,
    }
}

fn with_rel(schema: SourceSchema, name: &str, child: SourceSchema) -> SourceSchema {
    let mut s = schema;
    s.relationships.insert(name.to_string(), child);
    s.relationship_order.push(name.to_string());
    s
}

/// A stream with explicit `Yield` markers (TS `'yield' as const`).
fn stream_with_yields(items: Vec<StreamItem<Node>>) -> NodeStream {
    Box::new(items.into_iter())
}

/// TS `collectRows`: rows only (asserts there are no yields).
fn collect_rows(stream: NodeStream) -> Vec<rust_ivm::ivm::data::Row> {
    stream
        .map(|i| match i {
            StreamItem::Data(node) => node.row,
            StreamItem::Yield => panic!("unexpected yield"),
        })
        .collect()
}

/// TS `collectNodes` rendered as strings: `id=<n>` for a node, `yield` for a
/// marker.
fn collect_tokens(stream: NodeStream) -> Vec<String> {
    stream
        .map(|i| match i {
            StreamItem::Data(node) => match node.row.get("id") {
                Some(Value::F64(f)) => format!("id={}", *f as i64),
                Some(Value::Str(s)) => format!("id={s}"),
                other => format!("id={other:?}"),
            },
            StreamItem::Yield => "yield".to_string(),
        })
        .collect()
}

fn rows(ids: &[i64]) -> Vec<rust_ivm::ivm::data::Row> {
    ids.iter().map(|i| n(*i).row).collect()
}

// ── generateWithOverlayUnordered (join-utils.test.ts:47-256, verbatim) ──────

#[test]
fn unordered_remove_yields_overlay_node_first_then_all_stream_nodes() {
    let schema = make_schema(&["id"]);
    let result = collect_rows(generate_with_overlay_unordered(
        from_vec(vec![n(1), n(2)]),
        make_remove_change(n(3)),
        &schema,
    ));
    assert_eq!(result, rows(&[3, 1, 2]));
}

#[test]
fn unordered_remove_does_not_assert_when_overlay_node_is_not_in_stream() {
    let schema = make_schema(&["id"]);
    let result = collect_rows(generate_with_overlay_unordered(
        from_vec(vec![]),
        make_remove_change(n(99)),
        &schema,
    ));
    assert_eq!(result, rows(&[99]));
}

#[test]
fn unordered_add_suppresses_matching_node_from_stream() {
    let schema = make_schema(&["id"]);
    let result = collect_rows(generate_with_overlay_unordered(
        from_vec(vec![n(1), n(2), n(3)]),
        make_add_change(n(2)),
        &schema,
    ));
    assert_eq!(result, rows(&[1, 3]));
}

#[test]
#[should_panic(expected = "overlayGenerator: overlay was never applied to any fetched node")]
fn unordered_add_asserts_if_no_matching_node_found_in_stream() {
    let schema = make_schema(&["id"]);
    let _ = collect_tokens(generate_with_overlay_unordered(
        from_vec(vec![n(1)]),
        make_add_change(n(99)),
        &schema,
    ));
}

#[test]
fn unordered_edit_yields_old_node_first_and_suppresses_matching_node() {
    let schema = make_schema(&["id"]);
    let result = collect_rows(generate_with_overlay_unordered(
        from_vec(vec![n(1), nv(2, "new")]),
        make_edit_change(nv(2, "new"), nv(2, "old")),
        &schema,
    ));
    assert_eq!(result, vec![nv(2, "old").row, n(1).row]);
}

#[test]
#[should_panic(expected = "overlayGenerator: overlay was never applied to any fetched node")]
fn unordered_edit_asserts_if_no_matching_node_found_in_stream() {
    let schema = make_schema(&["id"]);
    let _ = collect_tokens(generate_with_overlay_unordered(
        from_vec(vec![n(1)]),
        make_edit_change(n(99), n(99)),
        &schema,
    ));
}

#[test]
fn unordered_child_overlays_child_relationship_on_matching_node() {
    let schema = with_rel(make_schema(&["id"]), "items", make_schema(&["cid"]));
    let cid = |c: &str| Node::new(row(&[("cid", Value::Str(c.into()))]));
    let node2 = n(2).set_relationship(
        "items",
        Rc::new({
            let a = cid("a");
            let b = cid("b");
            move || from_vec(vec![a.clone(), b.clone()])
        }),
    );
    let overlay = make_child_change(
        n(2),
        ChildData {
            relationship_name: "items".into(),
            change: Box::new(make_add_change(cid("c"))),
        },
    );
    let result: Vec<Node> =
        generate_with_overlay_unordered(from_vec(vec![n(1), node2]), overlay, &schema)
            .map(|i| match i {
                StreamItem::Data(node) => node,
                StreamItem::Yield => panic!("unexpected yield"),
            })
            .collect();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].row, n(1).row);
    assert_eq!(result[1].row, n(2).row);
    assert!(
        result[1].relationships.contains_key("items"),
        "lazy overlaid relationship"
    );
}

#[test]
#[should_panic(expected = "overlayGenerator: overlay was never applied to any fetched node")]
fn unordered_child_asserts_if_no_matching_node_found_in_stream() {
    let schema = with_rel(make_schema(&["id"]), "items", make_schema(&["cid"]));
    let overlay = make_child_change(
        n(99),
        ChildData {
            relationship_name: "items".into(),
            change: Box::new(make_add_change(Node::new(row(&[(
                "cid",
                Value::Str("c".into()),
            )])))),
        },
    );
    let _ = collect_tokens(generate_with_overlay_unordered(
        from_vec(vec![n(1)]),
        overlay,
        &schema,
    ));
}

#[test]
fn unordered_compound_primary_key_matches_on_all_pk_columns() {
    let schema = make_schema(&["a", "b"]);
    let ab = |a: i64, b: i64, val: &str| {
        Node::new(row(&[
            ("a", Value::F64(a as f64)),
            ("b", Value::F64(b as f64)),
            ("val", Value::Str(val.into())),
        ]))
    };
    let overlay = make_add_change(Node::new(row(&[
        ("a", Value::F64(1.0)),
        ("b", Value::F64(2.0)),
    ])));
    let result = collect_rows(generate_with_overlay_unordered(
        from_vec(vec![ab(1, 1, "x"), ab(1, 2, "y"), ab(2, 1, "z")]),
        overlay,
        &schema,
    ));
    assert_eq!(result, vec![ab(1, 1, "x").row, ab(2, 1, "z").row]);
}

#[test]
#[should_panic(expected = "overlayGenerator: overlay was never applied to any fetched node")]
fn unordered_compound_primary_key_does_not_match_on_partial_pk() {
    let schema = make_schema(&["a", "b"]);
    let ab = |a: i64, b: i64| {
        Node::new(row(&[
            ("a", Value::F64(a as f64)),
            ("b", Value::F64(b as f64)),
        ]))
    };
    let _ = collect_tokens(generate_with_overlay_unordered(
        from_vec(vec![ab(1, 1), ab(1, 2)]),
        make_add_change(ab(1, 3)),
        &schema,
    ));
}

#[test]
fn unordered_passes_yield_markers_through_unchanged() {
    let schema = make_schema(&["id"]);
    let stream = stream_with_yields(vec![
        StreamItem::Data(n(1)),
        StreamItem::Yield,
        StreamItem::Data(n(2)),
        StreamItem::Yield,
        StreamItem::Data(n(3)),
    ]);
    let result = collect_tokens(generate_with_overlay_unordered(
        stream,
        make_add_change(n(2)),
        &schema,
    ));
    assert_eq!(result, ["id=1", "yield", "yield", "id=3"]);
}

#[test]
fn no_yield_unordered_strips_yield_markers_from_output() {
    let schema = make_schema(&["id"]);
    let result = collect_rows(generate_with_overlay_no_yield_unordered(
        from_vec(vec![n(1), n(2), n(3)]),
        make_add_change(n(2)),
        &schema,
    ));
    assert_eq!(result, rows(&[1, 3]));
}

// ── generateWithOverlay (ordered; join-utils.ts:19-125) ─────────────────────

#[test]
fn ordered_add_suppresses_the_matching_node_and_adds_nothing_at_the_end() {
    // join-utils.ts:34-42: ADD → `applied = true; yieldNode = false` on the
    // equal node; the trailing block (:104-118) handles only REMOVE / EDIT.
    let schema = make_schema(&["id"]);
    let result = collect_rows(generate_with_overlay(
        from_vec(vec![n(1), n(2), n(3)]),
        make_add_change(n(2)),
        &schema,
    ));
    assert_eq!(result, rows(&[1, 3]));
}

#[test]
#[should_panic(expected = "overlayGenerator: overlay was never applied to any fetched node")]
fn ordered_add_asserts_when_the_node_is_not_in_the_stream() {
    let schema = make_schema(&["id"]);
    let _ = collect_rows(generate_with_overlay(
        from_vec(vec![n(1), n(3)]),
        make_add_change(n(2)),
        &schema,
    ));
}

#[test]
fn ordered_remove_reinserts_the_removed_node_at_its_sorted_position() {
    // join-utils.ts:43-49: REMOVE → yield the overlay node before the first
    // node that sorts after it, then the node itself.
    let schema = make_schema(&["id"]);
    let result = collect_rows(generate_with_overlay(
        from_vec(vec![n(1), n(3)]),
        make_remove_change(n(2)),
        &schema,
    ));
    assert_eq!(result, rows(&[1, 2, 3]));
}

#[test]
fn ordered_remove_appends_the_removed_node_when_it_sorts_last_or_the_stream_is_empty() {
    // join-utils.ts:104-107: `if (!applied) { REMOVE → yield overlay node }`.
    let schema = make_schema(&["id"]);
    let result = collect_rows(generate_with_overlay(
        from_vec(vec![n(1)]),
        make_remove_change(n(2)),
        &schema,
    ));
    assert_eq!(result, rows(&[1, 2]));
    let result = collect_rows(generate_with_overlay(
        from_vec(vec![]),
        make_remove_change(n(2)),
        &schema,
    ));
    assert_eq!(result, rows(&[2]));
}

#[test]
fn ordered_edit_reinserts_the_old_row_in_order_and_suppresses_the_new_row() {
    // join-utils.ts:50-72: the new row (sorted at 2) is suppressed; the old
    // row is yielded before the first node sorting after it (3).
    let schema = make_schema(&["id"]);
    let result = collect_rows(generate_with_overlay(
        from_vec(vec![n(1), nv(2, "new"), n(3)]),
        make_edit_change(nv(2, "new"), nv(2, "old")),
        &schema,
    ));
    assert_eq!(result, vec![n(1).row, nv(2, "old").row, n(3).row]);
}

#[test]
fn ordered_edit_appends_the_old_row_when_it_sorts_last() {
    // join-utils.ts:108-116: at the end, EDIT with the new row applied yields
    // the old row.
    let schema = make_schema(&["id"]);
    let result = collect_rows(generate_with_overlay(
        from_vec(vec![n(1), nv(5, "new")]),
        make_edit_change(nv(5, "new"), nv(5, "old")),
        &schema,
    ));
    assert_eq!(result, vec![n(1).row, nv(5, "old").row]);
}

#[test]
#[should_panic(expected = "edit overlay: new node must be applied before old node")]
fn ordered_edit_asserts_when_the_new_row_was_never_seen() {
    let schema = make_schema(&["id"]);
    let _ = collect_rows(generate_with_overlay(
        from_vec(vec![n(1)]),
        make_edit_change(nv(5, "new"), nv(5, "old")),
        &schema,
    ));
}

#[test]
fn ordered_child_overlays_the_relationship_of_the_matching_node_lazily() {
    // join-utils.ts:73-96: the matching node is re-yielded with the named
    // relationship wrapped in `generateWithOverlay(children, childChange)`.
    let child_schema = make_schema(&["id"]);
    let schema = with_rel(make_schema(&["id"]), "items", child_schema);
    let node2 = n(2).set_relationship("items", Rc::new(|| from_vec(vec![n(10), n(11)])));
    let overlay = make_child_change(
        n(2),
        ChildData {
            relationship_name: "items".into(),
            change: Box::new(make_add_change(n(10))),
        },
    );
    let result: Vec<Node> =
        generate_with_overlay(from_vec(vec![n(1), node2, n(3)]), overlay, &schema)
            .map(|i| match i {
                StreamItem::Data(node) => node,
                StreamItem::Yield => panic!("unexpected yield"),
            })
            .collect();
    assert_eq!(
        result.iter().map(|x| x.row.clone()).collect::<Vec<_>>(),
        rows(&[1, 2, 3])
    );
    let items = collect_rows((result[1].relationships["items"])());
    assert_eq!(
        items,
        rows(&[11]),
        "the child ADD overlay suppresses child 10"
    );
}

#[test]
fn ordered_passes_yield_markers_through_unchanged() {
    // join-utils.ts:28-31.
    let schema = make_schema(&["id"]);
    let stream = stream_with_yields(vec![
        StreamItem::Data(n(1)),
        StreamItem::Yield,
        StreamItem::Data(n(2)),
        StreamItem::Yield,
        StreamItem::Data(n(3)),
    ]);
    let result = collect_tokens(generate_with_overlay(
        stream,
        make_add_change(n(2)),
        &schema,
    ));
    assert_eq!(result, ["id=1", "yield", "yield", "id=3"]);
}

#[test]
fn ordered_trailing_work_runs_only_when_the_consumer_exhausts_the_stream() {
    // A TS generator's post-loop code runs only if iterated to completion:
    // a consumer that stops early never sees the re-inserted REMOVE node and
    // never trips the assert.
    let schema = make_schema(&["id"]);
    let mut stream =
        generate_with_overlay(from_vec(vec![n(1), n(3)]), make_add_change(n(2)), &schema);
    assert!(matches!(stream.next(), Some(StreamItem::Data(_))));
    drop(stream); // no panic
}
