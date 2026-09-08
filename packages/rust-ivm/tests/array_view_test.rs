//! Tests for ArrayView — port of TS `zql/src/ivm/array-view.test.ts` (v1.7.0).
//!
//! ArrayView materializes an operator's output into a `View` and notifies
//! listeners on `flush`. These port the TS `basics`, `single-format`, and
//! `hydrate-empty` cases 1:1 (same seed rows, same sort `[['b','asc'],
//! ['a','asc']]`, same expected entries + refCounts + callCounts).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::array_view::ArrayView;
use rust_ivm::ivm::data::{SortOrder, Value};
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::SourceChange;
use rust_ivm::ivm::view::{Format, View};

fn num(n: f64) -> Value {
    Value::F64(n)
}
fn str_val(s: &str) -> Value {
    Value::Str(Arc::from(s))
}

fn row(a: f64, b: &str) -> FxHashMap<String, Value> {
    let mut r = FxHashMap::default();
    r.insert("a".to_string(), num(a));
    r.insert("b".to_string(), str_val(b));
    r
}

fn table_source() -> Rc<RefCell<MemorySource>> {
    let cols: HashMap<String, ColumnType> = HashMap::from([
        ("a".to_string(), ColumnType::Number { optional: false }),
        ("b".to_string(), ColumnType::String { optional: false }),
    ]);
    Rc::new(RefCell::new(MemorySource::new(
        "table",
        cols,
        vec!["a".to_string()],
    )))
}

// The TS test connects with sort [['b','asc'],['a','asc']].
fn b_then_a_sort() -> SortOrder {
    Arc::new(vec![
        ["b".to_string(), "asc".to_string()],
        ["a".to_string(), "asc".to_string()],
    ])
}

fn list_format() -> Format {
    Format {
        singular: false,
        relationships: FxHashMap::default(),
    }
}

fn single_format() -> Format {
    Format {
        singular: true,
        relationships: FxHashMap::default(),
    }
}

/// Snapshot a list `View` into comparable `(a, b, ref_count)` triples.
fn snapshot_list(view: &View) -> Vec<(f64, String, usize)> {
    match view {
        View::List(entries) => entries
            .iter()
            .map(|e| {
                let a = match e.row.get("a") {
                    Some(Value::F64(n)) => *n,
                    _ => f64::NAN,
                };
                let b = match e.row.get("b") {
                    Some(Value::Str(s)) => s.to_string(),
                    _ => String::new(),
                };
                (a, b, e.ref_count)
            })
            .collect(),
        View::None => vec![],
        View::Single(_) => panic!("expected list view, got singular"),
    }
}

// Port of TS `basics`.
#[test]
fn basics() {
    let ms = table_source();
    // Seed a=1/b='a', a=2/b='b' before the view hydrates.
    ms.borrow_mut().add_row(row(1.0, "a"));
    ms.borrow_mut().add_row(row(2.0, "b"));

    let input = ms
        .borrow_mut()
        .connect(Some(b_then_a_sort()), None, None, None, None);
    let view = ArrayView::new(input, list_format());

    // Listener captures callCount + a snapshot of the entries it was handed.
    #[allow(clippy::type_complexity)] // test-only capture tuple
    let captured: Rc<RefCell<(usize, Vec<(f64, String, usize)>)>> =
        Rc::new(RefCell::new((0, vec![])));
    let cap = captured.clone();
    let listener: rust_ivm::ivm::array_view::Listener = Rc::new(move |v: &View| {
        let mut c = cap.borrow_mut();
        c.0 += 1;
        c.1 = snapshot_list(v);
    });
    view.borrow_mut().add_listener(listener);

    // Fires immediately with current (hydrated) data: sorted by b then a.
    assert_eq!(captured.borrow().0, 1, "listener fires once on add");
    assert_eq!(
        captured.borrow().1,
        vec![(1.0, "a".to_string(), 1), (2.0, "b".to_string(), 1)]
    );

    // A push does NOT notify until flush.
    ms.borrow_mut().push(SourceChange::Add {
        row: Arc::new(row(3.0, "c")),
    });
    assert_eq!(captured.borrow().0, 1, "no listener call before flush");

    view.borrow_mut().flush();
    assert_eq!(captured.borrow().0, 2);
    assert_eq!(
        captured.borrow().1,
        vec![
            (1.0, "a".to_string(), 1),
            (2.0, "b".to_string(), 1),
            (3.0, "c".to_string(), 1),
        ]
    );

    // Two removes, still no notify until flush.
    ms.borrow_mut().push(SourceChange::Remove {
        row: Arc::new(row(2.0, "b")),
    });
    assert_eq!(captured.borrow().0, 2);
    ms.borrow_mut().push(SourceChange::Remove {
        row: Arc::new(row(1.0, "a")),
    });
    assert_eq!(captured.borrow().0, 2);

    view.borrow_mut().flush();
    assert_eq!(captured.borrow().0, 3);
    assert_eq!(captured.borrow().1, vec![(3.0, "c".to_string(), 1)]);

    // After the LAST remove + flush, the live view is empty. (The Rust listener
    // is still registered — there is no unlisten handle — but the captured
    // snapshot is re-read from the live view, so it goes empty too.)
    ms.borrow_mut().push(SourceChange::Remove {
        row: Arc::new(row(3.0, "c")),
    });
    view.borrow_mut().flush();
    assert_eq!(captured.borrow().0, 4);
    assert_eq!(
        snapshot_list(view.borrow().data().unwrap()),
        Vec::<(f64, String, usize)>::new(),
        "live view is empty after all rows removed"
    );
}

// Port of TS `single-format` (the non-panicking half): singular view holds one
// entry; the LISTENER's captured value only updates on flush (the live view
// updates immediately — TS defers the notification, not the data).
#[test]
fn single_format_holds_one_then_none() {
    let ms = table_source();
    ms.borrow_mut().add_row(row(1.0, "a"));

    let input = ms
        .borrow_mut()
        .connect(Some(b_then_a_sort()), None, None, None, None);
    let view = ArrayView::new(input, single_format());

    // callCount + whether the last-notified value was a present single entry.
    #[allow(clippy::type_complexity)] // test-only capture tuple
    let captured: Rc<RefCell<(usize, Option<(f64, String)>)>> = Rc::new(RefCell::new((0, None)));
    let cap = captured.clone();
    let listener: rust_ivm::ivm::array_view::Listener = Rc::new(move |v: &View| {
        let mut c = cap.borrow_mut();
        c.0 += 1;
        c.1 = match v {
            View::Single(e) => {
                let a = match e.row.get("a") {
                    Some(Value::F64(n)) => *n,
                    _ => f64::NAN,
                };
                let b = match e.row.get("b") {
                    Some(Value::Str(s)) => s.to_string(),
                    _ => String::new(),
                };
                Some((a, b))
            }
            View::None | View::List(_) => None,
        };
    });
    view.borrow_mut().add_listener(listener);

    // Fires immediately with the single hydrated entry.
    assert_eq!(captured.borrow().0, 1);
    assert_eq!(captured.borrow().1, Some((1.0, "a".to_string())));

    // Remove the row: notification deferred until flush (captured value stays).
    ms.borrow_mut().push(SourceChange::Remove {
        row: Arc::new(row(1.0, "a")),
    });
    assert_eq!(captured.borrow().0, 1);
    assert_eq!(captured.borrow().1, Some((1.0, "a".to_string())));

    view.borrow_mut().flush();
    assert_eq!(captured.borrow().0, 2);
    assert_eq!(
        captured.borrow().1,
        None,
        "singular view is undefined after remove"
    );
    // Live view is also None/undefined.
    assert!(matches!(view.borrow().data(), Some(View::None) | None));
}

// Port of TS `single-format` throw: a singular relationship must never hold a
// second row. TS throws; the Rust port panics with the mirrored message.
#[test]
#[should_panic(expected = "Singular relationship should not have multiple rows")]
fn single_format_second_row_panics() {
    let ms = table_source();
    ms.borrow_mut().add_row(row(1.0, "a"));
    let input = ms
        .borrow_mut()
        .connect(Some(b_then_a_sort()), None, None, None, None);
    let _view = ArrayView::new(input, single_format());
    // Second row into a singular view: must panic.
    ms.borrow_mut().push(SourceChange::Add {
        row: Arc::new(row(2.0, "b")),
    });
}

// Port of TS `hydrate-empty`: an empty source yields an empty list view and the
// listener still fires exactly once on registration.
#[test]
fn hydrate_empty() {
    let ms = table_source();
    let input = ms
        .borrow_mut()
        .connect(Some(b_then_a_sort()), None, None, None, None);
    let view = ArrayView::new(input, list_format());

    let count = Rc::new(RefCell::new(0usize));
    let c = count.clone();
    let snap = Rc::new(RefCell::new(vec![(0.0, String::new(), 0usize)]));
    let s = snap.clone();
    let listener: rust_ivm::ivm::array_view::Listener = Rc::new(move |v: &View| {
        *c.borrow_mut() += 1;
        *s.borrow_mut() = snapshot_list(v);
    });
    view.borrow_mut().add_listener(listener);

    assert_eq!(*count.borrow(), 1);
    assert_eq!(*snap.borrow(), Vec::<(f64, String, usize)>::new());
}
