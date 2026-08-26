//! Tests for the plain `FanOut` / `FanIn` operators — ports of
//! `zql/src/ivm/fan-out.ts` and `fan-in.ts`, exercised by
//! `zql/src/ivm/fan-out-fan-in.test.ts`.
//!
//! NOTE (structural divergence, faithful test of what Rust implements): the TS
//! twins are `FilterOperator`s (beginFilter/endFilter/filter/push) wired via
//! `buildFilterPipeline`; the Rust `FanOut`/`FanIn` implement the push-only
//! `Input`/`Output` model instead, and the Rust *builder* uses `UnionFanOut`/
//! `UnionFanIn` for OR branches (builder.rs:381) rather than these. So the plain
//! FanOut/FanIn are ported-but-runtime-unused twins — genuinely uncovered. These
//! tests pin the behaviors the Rust code DOES implement, mirroring the TS test's
//! intent: (1) FanOut forwards every change to all branches in order, (2) the
//! missing-fan-in invariant panics (TS `must(...)`), (3) FanIn accumulates the
//! per-branch pushes and collapses them to a single downstream push (TS
//! "does not duplicate pushes").

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::change::{Change, ChangeType, SourceChange};
use rust_ivm::ivm::data::{SortOrder, Value};
use rust_ivm::ivm::fan_in::FanIn;
use rust_ivm::ivm::fan_out::FanOut;
use rust_ivm::ivm::operator::{Input, InputBase, Output, OutputHandle};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;

fn str_val(s: &str) -> Value {
    Value::Str(Arc::from(s))
}

fn num_val(n: f64) -> Value {
    Value::F64(n)
}

// table {a: number, b: string} pk [a] — mirrors the TS test's source.
fn make_source() -> Rc<RefCell<MemorySource>> {
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

fn a_sort() -> SortOrder {
    Arc::new(vec![["a".to_string(), "asc".to_string()]])
}

fn row_ab(a: f64, b: &str) -> FxHashMap<String, Value> {
    let mut r = FxHashMap::default();
    r.insert("a".to_string(), num_val(a));
    r.insert("b".to_string(), str_val(b));
    r
}

// Records every change it receives (change type + the row's `b` column).
struct Collector {
    seen: Rc<RefCell<Vec<(ChangeType, String)>>>,
}
impl Output for Collector {
    fn push(&mut self, change: Change, _pusher: &dyn InputBase) {
        let b = match change.node().row.get("b") {
            Some(Value::Str(s)) => s.to_string(),
            _ => String::new(),
        };
        self.seen.borrow_mut().push((change.change_type(), b));
    }
}

#[allow(clippy::type_complexity)] // test-only helper: (handle, recorded-changes) tuple
fn collector() -> (OutputHandle, Rc<RefCell<Vec<(ChangeType, String)>>>) {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let handle: OutputHandle = Rc::new(RefCell::new(Collector { seen: seen.clone() }));
    (handle, seen)
}

// Port of TS `fan-out pushes along all paths`: every branch sees add/edit/remove
// in order. The Rust FanOut does not self-register on its input (a divergence
// from TS `input.setFilterOutput(this)`), so we wire it as the connector output
// explicitly, then let the source push flow through.
#[test]
fn fan_out_pushes_along_all_paths() {
    let src = make_source();
    let input = src.borrow_mut().connect(Some(a_sort()), None, None, None);

    let fan_out = FanOut::new(input.clone());
    // Route the source connector's pushes into the fan-out.
    let fo_out: OutputHandle = fan_out.clone();
    input.borrow().set_output(fo_out);

    // Three downstream branches.
    let (c1, s1) = collector();
    let (c2, s2) = collector();
    let (c3, s3) = collector();
    fan_out.borrow().set_output(c1);
    fan_out.borrow().set_output(c2);
    fan_out.borrow().set_output(c3);

    // Dummy fan-in (no inputs) purely to satisfy the FanOut invariant, exactly
    // like the TS test's `new FanIn(fanOut, [])`.
    let schema = input.borrow().get_schema();
    let fan_in = FanIn::new(schema);
    fan_out.borrow().set_fan_in(fan_in);

    src.borrow_mut().push(SourceChange::Add {
        row: Arc::new(row_ab(1.0, "foo")),
    });
    src.borrow_mut().push(SourceChange::Edit {
        row: Arc::new(row_ab(1.0, "bar")),
        old_row: Arc::new(row_ab(1.0, "foo")),
    });
    src.borrow_mut().push(SourceChange::Remove {
        row: Arc::new(row_ab(1.0, "bar")),
    });

    let expected = vec![
        (ChangeType::Add, "foo".to_string()),
        (ChangeType::Edit, "bar".to_string()),
        (ChangeType::Remove, "bar".to_string()),
    ];
    assert_eq!(*s1.borrow(), expected, "branch 1 sees all changes in order");
    assert_eq!(*s2.borrow(), expected, "branch 2 sees all changes in order");
    assert_eq!(*s3.borrow(), expected, "branch 3 sees all changes in order");
}

// Port of TS `must(this.#fanIn, 'fan-out must have a corresponding fan-in
// set!')` (fan-out.ts:77): pushing through a FanOut with no fan-in wired is a
// graph-construction invariant violation and must panic with the exact message.
#[test]
#[should_panic(expected = "fan-out must have a corresponding fan-in set!")]
fn fan_out_push_without_fan_in_panics() {
    let src = make_source();
    let input = src.borrow_mut().connect(Some(a_sort()), None, None, None);

    let fan_out = FanOut::new(input.clone());
    let fo_out: OutputHandle = fan_out.clone();
    input.borrow().set_output(fo_out);

    let (c1, _s1) = collector();
    fan_out.borrow().set_output(c1);
    // Deliberately DO NOT call set_fan_in.

    src.borrow_mut().push(SourceChange::Add {
        row: Arc::new(row_ab(1.0, "foo")),
    });
}

// Port of TS `fan-out,fan-in pairing does not duplicate pushes`: the same change
// arriving on N branches is accumulated by FanIn and collapsed to a SINGLE
// downstream push. We simulate N converging branches by registering the fan-in
// as the fan-out's output N times; the source push then reaches fan_in N times,
// and `fan_out_done_pushing` collapses the N accumulated adds to one.
#[test]
fn fan_in_does_not_duplicate_pushes() {
    let src = make_source();
    let input = src.borrow_mut().connect(Some(a_sort()), None, None, None);

    let fan_out = FanOut::new(input.clone());
    let fo_out: OutputHandle = fan_out.clone();
    input.borrow().set_output(fo_out);

    let schema = input.borrow().get_schema();
    let fan_in = FanIn::new(schema);

    // Mark the fan-in as having branches (non-empty inputs => it collapses the
    // accumulated pushes rather than asserting emptiness). The concrete input
    // identity is irrelevant to the collapse path; the connector stands in for
    // "there are branches feeding this fan-in".
    fan_in.borrow_mut().add_input(input.clone());

    // Three converging branches all feed the same fan-in.
    let fi_out_a: OutputHandle = fan_in.clone();
    let fi_out_b: OutputHandle = fan_in.clone();
    let fi_out_c: OutputHandle = fan_in.clone();
    fan_out.borrow().set_output(fi_out_a);
    fan_out.borrow().set_output(fi_out_b);
    fan_out.borrow().set_output(fi_out_c);
    fan_out.borrow().set_fan_in(fan_in.clone());

    // Downstream of the fan-in: the single collector that must see NO duplicates.
    let (sink, seen) = collector();
    fan_in.borrow().set_output(sink);

    src.borrow_mut().push(SourceChange::Add {
        row: Arc::new(row_ab(1.0, "foo")),
    });

    assert_eq!(
        *seen.borrow(),
        vec![(ChangeType::Add, "foo".to_string())],
        "three converging branches collapse to a single downstream add"
    );
}

// Port of the FanIn empty-inputs invariant (fan-in.ts:77): a fan-in with no
// inputs must never receive pushes; `fan_out_done_pushing` on an empty
// accumulator is a no-op (asserted below by observing no downstream push).
#[test]
fn fan_in_with_no_inputs_is_a_noop() {
    let src = make_source();
    let input = src.borrow_mut().connect(Some(a_sort()), None, None, None);
    let schema = input.borrow().get_schema();
    let fan_in = FanIn::new(schema);

    let (sink, seen) = collector();
    fan_in.borrow().set_output(sink);

    // No inputs, no accumulated pushes: collapsing is a no-op, nothing forwarded.
    let fan_out = FanOut::new(input.clone());
    fan_in
        .borrow_mut()
        .fan_out_done_pushing(ChangeType::Add, &*fan_out.borrow());

    assert!(seen.borrow().is_empty(), "empty fan-in forwards nothing");
}
