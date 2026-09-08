//! Direct test for `merge_fetches` — port of TS `mergeFetches`
//! (`zql/src/ivm/union-fan-in.ts:196`). Triage ivm #10: the fn has no in-crate
//! Rust caller today (the UnionFanIn fetch path is exercised via its operator),
//! so its k-way-merge + consecutive-dedup logic was untested. It stays 1:1 with
//! the TS twin, so this pins it directly: union (not union-all) semantics —
//! duplicates across branches collapse to one, output stays in comparator order.

use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::data::{Node, SortOrder, Value, make_comparator};
use rust_ivm::ivm::union_fan_in::merge_fetches;

fn node(id: &str) -> Node {
    let mut row: FxHashMap<String, Value> = FxHashMap::default();
    row.insert("id".to_string(), Value::Str(Arc::from(id)));
    Node::new(Arc::new(row))
}

fn ids(nodes: &[Node]) -> Vec<String> {
    nodes
        .iter()
        .map(|n| match n.row.get("id") {
            Some(Value::Str(s)) => s.to_string(),
            _ => String::new(),
        })
        .collect()
}

fn id_asc() -> impl Fn(&Node, &Node) -> std::cmp::Ordering {
    let sort: SortOrder = Arc::new(vec![["id".to_string(), "asc".to_string()]]);
    let row_cmp = make_comparator(sort, false);
    move |a: &Node, b: &Node| row_cmp(a.row.as_ref(), b.row.as_ref())
}

// Port of TS `mergeFetches`: k sorted branch streams merge into one sorted
// stream with consecutive-equal (cross-branch duplicate) rows collapsed to one.
#[test]
fn merges_and_dedupes_across_branches() {
    let cmp = id_asc();
    let f1 = vec![node("a"), node("c"), node("e")];
    let f2 = vec![node("b"), node("c"), node("d")]; // "c" duplicates f1
    let f3 = vec![node("a"), node("e")]; // "a"/"e" duplicate f1
    let merged = merge_fetches(vec![f1, f2, f3], &cmp);
    // Union, not union-all: each id appears exactly once, in comparator order.
    assert_eq!(ids(&merged), vec!["a", "b", "c", "d", "e"]);
}

// A single branch passes through unchanged (still deduping consecutive equals
// within it — TS dedupes by the comparator, not by branch identity).
#[test]
fn single_branch_dedupes_consecutive_equals() {
    let cmp = id_asc();
    let f1 = vec![node("a"), node("a"), node("b")];
    let merged = merge_fetches(vec![f1], &cmp);
    assert_eq!(ids(&merged), vec!["a", "b"]);
}

// Empty branches contribute nothing; an all-empty input yields an empty result.
#[test]
fn empty_inputs_yield_empty() {
    let cmp = id_asc();
    assert!(merge_fetches(vec![], &cmp).is_empty());
    assert!(merge_fetches(vec![vec![], vec![]], &cmp).is_empty());
    // An empty branch alongside a non-empty one is skipped cleanly.
    assert_eq!(
        ids(&merge_fetches(vec![vec![], vec![node("x")]], &cmp)),
        vec!["x"]
    );
}
