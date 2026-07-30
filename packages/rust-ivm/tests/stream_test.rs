//! Tests for stream.ts — port of `zql/src/ivm/stream.test.ts`.
//!
//! Tests: take, first.

use rust_ivm::ivm::data::Node;
use rust_ivm::ivm::stream::from_vec;
use rustc_hash::FxHashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// take
// ---------------------------------------------------------------------------

fn node_stream(vals: &[i64]) -> Box<dyn Iterator<Item = Node>> {
    // Note: from_vec returns NodeStream (StreamItem<Node>), need to strip yields
    let nodes: Vec<Node> = vals
        .iter()
        .map(|v| {
            let mut m: FxHashMap<String, rust_ivm::ivm::data::Value> = FxHashMap::default();
            m.insert("n".to_string(), rust_ivm::ivm::data::Value::F64(*v as f64));
            Node::new(Arc::new(m))
        })
        .collect();
    Box::new(rust_ivm::ivm::stream::skip_yields(from_vec(nodes)))
}

fn take(stream: Box<dyn Iterator<Item = Node>>, limit: usize) -> Vec<i64> {
    stream
        .take(limit)
        .map(|n| match n.row.get("n") {
            Some(rust_ivm::ivm::data::Value::F64(v)) => *v as i64,
            _ => 0,
        })
        .collect()
}

#[test]
fn test_take_first_n_elements() {
    let stream = node_stream(&[1, 2, 3, 4, 5]);
    assert_eq!(take(stream, 3), vec![1, 2, 3]);
}

#[test]
fn test_take_zero_returns_empty() {
    let stream = node_stream(&[1, 2, 3, 4, 5]);
    assert_eq!(take(stream, 0), Vec::<i64>::new());
}

#[test]
fn test_take_greater_than_stream_returns_all() {
    let stream = node_stream(&[1, 2, 3]);
    assert_eq!(take(stream, 5), vec![1, 2, 3]);
}

// ---------------------------------------------------------------------------
// first
// ---------------------------------------------------------------------------

fn first(stream: Box<dyn Iterator<Item = Node>>) -> Option<i64> {
    let mut s = stream;
    s.next().map(|n| match n.row.get("n") {
        Some(rust_ivm::ivm::data::Value::F64(v)) => *v as i64,
        _ => 0,
    })
}

#[test]
fn test_first_returns_first_element() {
    let stream = node_stream(&[1, 2, 3, 4, 5]);
    assert_eq!(first(stream), Some(1));
}

#[test]
fn test_first_empty_stream_returns_none() {
    let stream: Box<dyn Iterator<Item = Node>> = Box::new(std::iter::empty());
    assert_eq!(first(stream), None);
}
