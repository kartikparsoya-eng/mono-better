//! Tests for `generate_with_start` — port of the `start` slicing in
//! `zql/src/ivm/memory-source.ts` (join_utils.rs:337). Given a sorted stream and
//! a `Start {row, basis}`, it yields the tail at/after the cursor: `At` keeps
//! rows `>= start` (cmp != Less), `After` keeps rows `> start` (cmp == Greater).

use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::data::{Node, Value, make_comparator};
use rust_ivm::ivm::join_utils::generate_with_start;
use rust_ivm::ivm::operator::{Basis, Start};
use rust_ivm::ivm::stream::{from_vec, skip_yields};

fn node(id: f64) -> Node {
    let mut r = FxHashMap::default();
    r.insert("id".to_string(), Value::F64(id));
    Node::new(Arc::new(r))
}

fn ids(stream: rust_ivm::ivm::stream::NodeStream) -> Vec<f64> {
    skip_yields(stream)
        .map(|n| match n.row.get("id") {
            Some(Value::F64(v)) => *v,
            _ => f64::NAN,
        })
        .collect()
}

fn asc_id_comparator() -> rust_ivm::ivm::data::Comparator {
    make_comparator(Arc::new(vec![["id".to_string(), "asc".to_string()]]), false)
}

fn start(id: f64, basis: Basis) -> Start {
    let mut r = FxHashMap::default();
    r.insert("id".to_string(), Value::F64(id));
    Start {
        row: Arc::new(r),
        basis,
    }
}

// `At` keeps the cursor row and everything after it (cmp != Less).
#[test]
fn start_at_includes_the_cursor_row() {
    let cmp = asc_id_comparator();
    let stream = from_vec(vec![node(1.0), node(2.0), node(3.0), node(4.0)]);
    let out = generate_with_start(stream, &start(2.0, Basis::At), &cmp);
    assert_eq!(ids(out), vec![2.0, 3.0, 4.0]);
}

// `After` excludes the cursor row (cmp == Greater only).
#[test]
fn start_after_excludes_the_cursor_row() {
    let cmp = asc_id_comparator();
    let stream = from_vec(vec![node(1.0), node(2.0), node(3.0), node(4.0)]);
    let out = generate_with_start(stream, &start(2.0, Basis::After), &cmp);
    assert_eq!(ids(out), vec![3.0, 4.0]);
}

// A cursor past the end yields nothing.
#[test]
fn start_after_last_yields_empty() {
    let cmp = asc_id_comparator();
    let stream = from_vec(vec![node(1.0), node(2.0)]);
    let out = generate_with_start(stream, &start(5.0, Basis::At), &cmp);
    assert_eq!(ids(out), Vec::<f64>::new());
}
