//! Tests for `MemorySource::get_row` / `all_rows` / `gen_push` — ports of the
//! `getRow`, all-rows preload, and `genPush` paths in `zql/src/ivm/memory-
//! source.ts` (exercised by `memory-source.test.ts`). These accessors were
//! whole-untested (triage: source.rs get_row L274, all_rows L287, gen_push L360).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::change::{Change, ChangeType};
use rust_ivm::ivm::data::{SortOrder, Value};
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::operator::{InputBase, Output, OutputHandle};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::SourceChange;

fn str_val(s: &str) -> Value {
    Value::Str(Arc::from(s))
}

fn make_source() -> Rc<RefCell<MemorySource>> {
    let cols: HashMap<String, ColumnType> = HashMap::from([
        ("id".to_string(), ColumnType::String { optional: false }),
        ("n".to_string(), ColumnType::String { optional: false }),
    ]);
    Rc::new(RefCell::new(MemorySource::new(
        "t",
        cols,
        vec!["id".to_string()],
    )))
}

fn add_row(src: &Rc<RefCell<MemorySource>>, id: &str, n: &str) {
    let mut r: FxHashMap<String, Value> = FxHashMap::default();
    r.insert("id".to_string(), str_val(id));
    r.insert("n".to_string(), str_val(n));
    src.borrow_mut().add_row(r);
}

// Port of TS `getRow`: look a row up by primary key. Present PK returns the full
// stored row; an absent PK returns None (TS `undefined`).
#[test]
fn get_row_by_primary_key_hit_and_miss() {
    let src = make_source();
    add_row(&src, "a", "alpha");
    add_row(&src, "b", "beta");

    let got = src
        .borrow()
        .get_row(&[("id".to_string(), str_val("a"))])
        .expect("row a present");
    assert_eq!(got.get("n"), Some(&str_val("alpha")));

    // Missing PK => None.
    assert!(
        src.borrow()
            .get_row(&[("id".to_string(), str_val("zzz"))])
            .is_none(),
        "absent primary key returns None"
    );
}

// The Rust `get_row` matches on ALL provided (col,val) pairs, so a multi-column
// predicate that fails on a non-PK column also misses.
#[test]
fn get_row_requires_all_provided_columns_to_match() {
    let src = make_source();
    add_row(&src, "a", "alpha");

    // Correct id but wrong n => no match.
    assert!(
        src.borrow()
            .get_row(&[
                ("id".to_string(), str_val("a")),
                ("n".to_string(), str_val("WRONG")),
            ])
            .is_none()
    );
    // Both columns matching => hit.
    assert!(
        src.borrow()
            .get_row(&[
                ("id".to_string(), str_val("a")),
                ("n".to_string(), str_val("alpha")),
            ])
            .is_some()
    );
}

// `all_rows` returns every stored row (preload path).
#[test]
fn all_rows_returns_every_row() {
    let src = make_source();
    add_row(&src, "a", "alpha");
    add_row(&src, "b", "beta");
    add_row(&src, "c", "gamma");

    let rows = src.borrow().all_rows();
    assert_eq!(rows.len(), 3);
    let ids: Vec<String> = rows
        .iter()
        .filter_map(|r| match r.get("id") {
            Some(Value::Str(s)) => Some(s.to_string()),
            _ => None,
        })
        .collect();
    for want in ["a", "b", "c"] {
        assert!(ids.contains(&want.to_string()), "all_rows contains {want}");
    }
}

// Collector output downstream of a source connection.
struct Collector {
    seen: Rc<RefCell<Vec<ChangeType>>>,
}
impl Output for Collector {
    fn push(&mut self, change: Change, _pusher: &dyn InputBase) {
        self.seen.borrow_mut().push(change.change_type());
    }
}

fn id_sort() -> SortOrder {
    Arc::new(vec![["id".to_string(), "asc".to_string()]])
}

// Port of TS `genPush`: pushing a source change applies it to the store AND
// delivers the resulting change to every connected output. NOTE (divergence
// pinned): the Rust `gen_push`/`push` return value is a vestigial empty Vec —
// changes reach consumers via the output `push` callback, not the return (see
// source.rs push_internal `all_changes`), so we assert on the collector, not
// the return.
#[test]
fn gen_push_applies_change_and_delivers_to_output() {
    let src = make_source();
    let input = src
        .borrow_mut()
        .connect(Some(id_sort()), None, None, None, None);
    let seen = Rc::new(RefCell::new(Vec::new()));
    let collector: OutputHandle = Rc::new(RefCell::new(Collector { seen: seen.clone() }));
    input.borrow().set_output(collector);

    let mut r: FxHashMap<String, Value> = FxHashMap::default();
    r.insert("id".to_string(), str_val("x"));
    r.insert("n".to_string(), str_val("xray"));
    let produced: Vec<Change> = src
        .borrow_mut()
        .gen_push(SourceChange::Add { row: Arc::new(r) });

    // Return is the vestigial empty Vec; delivery happens via the output.
    assert!(produced.is_empty(), "gen_push return is vestigial/empty");
    assert_eq!(
        *seen.borrow(),
        vec![ChangeType::Add],
        "connected output receives exactly one Add"
    );

    // And the row is now retrievable via get_row.
    let got = src
        .borrow()
        .get_row(&[("id".to_string(), str_val("x"))])
        .expect("pushed row is present");
    assert_eq!(got.get("n"), Some(&str_val("xray")));
}
