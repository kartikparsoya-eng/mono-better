//! Tests for the REAL `stream::{single_node, take, first, skip_yields,
//! count_data}` — ports of `zql/src/ivm/stream.ts` (`take` L10, `first` L21) and
//! `skip-yields.ts`. NOTE: stream_test.rs reimplements `take`/`first` as LOCAL
//! test helpers and never touches the crate functions, so the real `TakeStream`
//! (and its Yield-passthrough semantics) were uncovered. These call the crate
//! functions directly and pin the TS behavior that `take` passes `Yield` items
//! through WITHOUT counting them against the limit.

use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::data::{Node, Value};
use rust_ivm::ivm::stream::{StreamItem, count_data, first, single_node, skip_yields, take};

fn node(id: f64) -> Node {
    let mut r: FxHashMap<String, Value> = FxHashMap::default();
    r.insert("id".to_string(), Value::F64(id));
    Node::new(Arc::new(r))
}

fn id_of(n: &Node) -> f64 {
    match n.row.get("id") {
        Some(Value::F64(f)) => *f,
        _ => f64::NAN,
    }
}

fn data_ids(stream: rust_ivm::ivm::stream::NodeStream) -> Vec<f64> {
    skip_yields(stream).map(|n| id_of(&n)).collect()
}

fn stream_of(items: Vec<StreamItem<Node>>) -> rust_ivm::ivm::stream::NodeStream {
    Box::new(items.into_iter())
}

// Port of TS `singleNode`-style wrap: exactly one Data item, no yields.
#[test]
fn single_node_yields_exactly_one_data() {
    let ids = data_ids(single_node(node(7.0)));
    assert_eq!(ids, vec![7.0]);
}

// Port of TS `take`: caps the number of DATA items, but passes Yield items
// through and does NOT count them against the limit. Here limit=2 over
// [Yield, 1, Yield, 2, 3] yields [Yield, 1, Yield, 2] and drops 3.
#[test]
fn take_passes_yields_through_and_caps_data() {
    let s = stream_of(vec![
        StreamItem::Yield,
        StreamItem::Data(node(1.0)),
        StreamItem::Yield,
        StreamItem::Data(node(2.0)),
        StreamItem::Data(node(3.0)),
    ]);

    let out: Vec<StreamItem<Node>> = take(s, 2).collect();

    // Two Data (1,2) and the two interleaved Yields survived; Data(3) dropped.
    let yields = out
        .iter()
        .filter(|i| matches!(i, StreamItem::Yield))
        .count();
    let data: Vec<f64> = out
        .iter()
        .filter_map(|i| match i {
            StreamItem::Data(n) => Some(id_of(n)),
            StreamItem::Yield => None,
        })
        .collect();
    assert_eq!(yields, 2, "both interleaved yields passed through");
    assert_eq!(data, vec![1.0, 2.0], "capped at 2 data items");
}

// take(0) short-circuits to an empty stream (TS returns immediately).
#[test]
fn take_zero_is_empty() {
    let s = stream_of(vec![
        StreamItem::Data(node(1.0)),
        StreamItem::Data(node(2.0)),
    ]);
    assert!(take(s, 0).next().is_none(), "take(0) yields nothing");
}

// take beyond the stream length returns everything.
#[test]
fn take_more_than_available_returns_all() {
    let s = stream_of(vec![
        StreamItem::Data(node(1.0)),
        StreamItem::Data(node(2.0)),
    ]);
    assert_eq!(data_ids(take(s, 5)), vec![1.0, 2.0]);
}

// Port of TS `first`: skips leading Yields and returns the first Data.
#[test]
fn first_skips_yields_and_returns_first_data() {
    let s = stream_of(vec![
        StreamItem::Yield,
        StreamItem::Yield,
        StreamItem::Data(node(9.0)),
        StreamItem::Data(node(10.0)),
    ]);
    let got = first(s).expect("a data item exists past the yields");
    assert_eq!(id_of(&got), 9.0);
}

// `first` over a stream with no Data (only Yields) returns None.
#[test]
fn first_all_yields_is_none() {
    let s = stream_of(vec![StreamItem::Yield, StreamItem::Yield]);
    assert!(first(s).is_none());
}

// `count_data` counts Data items, ignoring Yields.
#[test]
fn count_data_ignores_yields() {
    let s = stream_of(vec![
        StreamItem::Yield,
        StreamItem::Data(node(1.0)),
        StreamItem::Yield,
        StreamItem::Data(node(2.0)),
        StreamItem::Data(node(3.0)),
    ]);
    assert_eq!(count_data(s), 3);
}
