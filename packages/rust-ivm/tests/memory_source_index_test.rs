//! F-21: `MemorySource`'s primary index is kept sorted by the primary-key
//! comparator, so `add_row`'s replace-or-insert and `has`'s membership test
//! both BINARY SEARCH it rather than scanning.
//!
//! TS's `Index.data` is a `BTreeSet<Row>` (zql/src/ivm/memory-source.ts:71,
//! bulk-loaded via `BTreeSet.fromSorted`, :245), so add / has are O(log n)
//! there. Rust's index is a `Vec`, and `add_row` used to scan it with
//! `.iter().position(..)` before its O(log n) `partition_point` insert, making
//! a bulk load O(n^2) comparisons; `has` scanned with `.iter().any(..)`.
//! Because the vec is sorted by the PK comparator, a row with a given PK can
//! only sit at the insertion point — the equality predicate is unchanged, only
//! the search narrowed.
//!
//! These tests pin the BEHAVIOUR the narrowed search has to preserve, at every
//! position a binary search could get wrong: replacing the first row, a middle
//! row, the last row, inserting before everything, after everything, and into
//! the middle.
//!
//! NON-VACUOUS: introduce any off-by-one in either search — drop the
//! `pos < data.len()` guard (panics on the append cases), compare against
//! `data[pos - 1]`, or use `!= CmpOrdering::Greater` in the `partition_point`
//! predicate — and these fail. Restoring the full `.iter().position(..)` scan
//! also keeps them green, which is the point: they guard the refactor.

use std::collections::HashMap;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::schema::ColumnType;

fn source() -> MemorySource {
    let columns: HashMap<String, ColumnType> = [
        ("id".to_string(), ColumnType::String { optional: false }),
        ("name".to_string(), ColumnType::String { optional: false }),
    ]
    .into_iter()
    .collect();
    MemorySource::new("t", columns, vec!["id".to_string()])
}

fn row(id: &str, name: &str) -> FxHashMap<String, Value> {
    let mut m: FxHashMap<String, Value> = FxHashMap::default();
    m.insert("id".to_string(), Value::Str(id.into()));
    m.insert("name".to_string(), Value::Str(name.into()));
    m
}

/// `(id, name)` pairs in index order.
fn contents(s: &MemorySource) -> Vec<(String, String)> {
    s.all_rows()
        .iter()
        .map(|r| {
            let g = |k: &str| match r.get(k) {
                Some(Value::Str(v)) => v.to_string(),
                other => panic!("unexpected {k}: {other:?}"),
            };
            (g("id"), g("name"))
        })
        .collect()
}

#[test]
fn add_row_replaces_at_every_position_and_keeps_the_index_sorted() {
    let mut s = source();
    // Inserted out of order, so the sort is doing real work.
    for (id, name) in [("c", "c0"), ("a", "a0"), ("e", "e0"), ("b", "b0")] {
        s.add_row(row(id, name));
    }
    assert_eq!(
        contents(&s),
        vec![
            ("a".into(), "a0".into()),
            ("b".into(), "b0".into()),
            ("c".into(), "c0".into()),
            ("e".into(), "e0".into()),
        ],
        "the index must be sorted by primary key"
    );

    // Replace the FIRST row.
    s.add_row(row("a", "a1"));
    // Replace a MIDDLE row.
    s.add_row(row("c", "c1"));
    // Replace the LAST row.
    s.add_row(row("e", "e1"));
    assert_eq!(
        contents(&s),
        vec![
            ("a".into(), "a1".into()),
            ("b".into(), "b0".into()),
            ("c".into(), "c1".into()),
            ("e".into(), "e1".into()),
        ],
        "a same-PK add must REPLACE in place — never duplicate, never reorder"
    );
    assert_eq!(s.all_rows().len(), 4, "replacing must not grow the index");

    // Insert BEFORE everything, AFTER everything, and into the MIDDLE.
    s.add_row(row("A", "before"));
    s.add_row(row("z", "after"));
    s.add_row(row("d", "middle"));
    assert_eq!(
        contents(&s),
        vec![
            ("A".into(), "before".into()),
            ("a".into(), "a1".into()),
            ("b".into(), "b0".into()),
            ("c".into(), "c1".into()),
            ("d".into(), "middle".into()),
            ("e".into(), "e1".into()),
            ("z".into(), "after".into()),
        ],
        "inserts at the front, the back and the middle must all land in order"
    );
}

/// A bulk load in DESCENDING key order is the worst case for the old scan
/// (every add scanned the whole vec before inserting at the front). Pins that
/// the result is still a correctly sorted, duplicate-free index.
#[test]
fn a_reverse_order_bulk_load_produces_a_correct_index() {
    let mut s = source();
    const N: usize = 500;
    for i in (0..N).rev() {
        s.add_row(row(&format!("k{i:04}"), &format!("v{i}")));
    }
    let got = contents(&s);
    assert_eq!(
        got.len(),
        N,
        "every distinct key must be present exactly once"
    );
    let expected: Vec<(String, String)> = (0..N)
        .map(|i| (format!("k{i:04}"), format!("v{i}")))
        .collect();
    assert_eq!(got, expected, "the index must be fully sorted");

    // Re-adding every key must replace, not duplicate.
    for i in 0..N {
        s.add_row(row(&format!("k{i:04}"), &format!("w{i}")));
    }
    assert_eq!(
        s.all_rows().len(),
        N,
        "re-adding every key must replace in place, not double the index"
    );
    assert_eq!(
        contents(&s)[0],
        ("k0000".to_string(), "w0".to_string()),
        "the replacement value must be the one that survives"
    );
}
