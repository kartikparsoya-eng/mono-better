//! Tests for data.rs utilities that were untested (triage targets):
//! `make_partial_bound_comparator` (TS data.ts makePartialBoundComparator — the
//! partial-key comparator used by Take/MemorySource resume) and `drain_streams`
//! (TS data.ts drainStreams — fully consumes a node's nested relationship
//! streams).

use std::cell::Cell;
use std::cmp::Ordering;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::data::{Node, SortOrder, Value, drain_streams, make_partial_bound_comparator};
use rust_ivm::ivm::stream::{RelStream, from_vec};

fn row(pairs: &[(&str, Value)]) -> FxHashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn sort(cols: &[(&str, &str)]) -> SortOrder {
    Arc::new(
        cols.iter()
            .map(|(c, d)| [c.to_string(), d.to_string()])
            .collect::<Vec<_>>(),
    )
}

// A partial bound (the `b` side missing a trailing sort field) compares Equal
// once the comparator reaches the absent field — the prefix-match semantics
// Take/MemorySource rely on to resume from a partial key.
#[test]
fn partial_bound_prefix_matches_when_field_absent() {
    let cmp = make_partial_bound_comparator(sort(&[("a", "asc"), ("b", "asc")]), false);
    let full = row(&[("a", Value::F64(1.0)), ("b", Value::F64(2.0))]);
    let partial = row(&[("a", Value::F64(1.0))]); // no "b"
    assert_eq!(
        cmp(&full, &partial),
        Ordering::Equal,
        "prefix (a) matches, b absent -> Equal"
    );
}

// With both fields present the comparator orders on the first differing field.
#[test]
fn full_bound_orders_on_first_differing_field() {
    let cmp = make_partial_bound_comparator(sort(&[("a", "asc"), ("b", "asc")]), false);
    let x = row(&[("a", Value::F64(1.0)), ("b", Value::F64(2.0))]);
    let y = row(&[("a", Value::F64(1.0)), ("b", Value::F64(3.0))]);
    assert_eq!(cmp(&x, &y), Ordering::Less, "a equal, 2 < 3 on b");
    assert_eq!(cmp(&y, &x), Ordering::Greater);
}

// The reverse flag flips the ordering.
#[test]
fn reverse_flag_flips_ordering() {
    let asc = make_partial_bound_comparator(sort(&[("a", "asc")]), false);
    let desc = make_partial_bound_comparator(sort(&[("a", "asc")]), true);
    let x = row(&[("a", Value::F64(1.0))]);
    let y = row(&[("a", Value::F64(2.0))]);
    assert_eq!(asc(&x, &y), Ordering::Less);
    assert_eq!(desc(&x, &y), Ordering::Greater);
}

// drain_streams fully consumes a node's relationship streams, recursing into
// nested children — proven by counting every yielded child.
#[test]
fn drain_streams_consumes_nested_relationship_streams() {
    let count = Rc::new(Cell::new(0usize));

    // grandchild stream (nested one level): yields 2 leaves.
    let gc_count = count.clone();
    let make_grandchildren: RelStream = Rc::new(move || {
        gc_count.set(gc_count.get() + 2);
        from_vec(vec![
            Node::new(Arc::new(row(&[("id", Value::F64(10.0))]))),
            Node::new(Arc::new(row(&[("id", Value::F64(11.0))]))),
        ])
    });

    // one child that itself carries a "gc" relationship.
    let child = Node::new(Arc::new(row(&[("id", Value::F64(1.0))])))
        .set_relationship("gc", make_grandchildren);

    let c_count = count.clone();
    let make_children: RelStream = Rc::new(move || {
        c_count.set(c_count.get() + 1);
        from_vec(vec![child.clone()])
    });

    let root = Node::new(Arc::new(row(&[("id", Value::F64(0.0))])))
        .set_relationship("children", make_children);

    drain_streams(&root);
    // 1 child + 2 grandchildren all consumed.
    assert_eq!(count.get(), 3);
}
