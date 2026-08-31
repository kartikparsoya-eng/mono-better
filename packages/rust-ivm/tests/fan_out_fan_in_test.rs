//! Tests for `FanOut` / `FanIn` — ports of `zql/src/ivm/fan-out.ts` and
//! `fan-in.ts`, exercised per `zql/src/ivm/fan-out-fan-in.test.ts` through the
//! real filter sub-graph protocol (FilterStart → FanOut → branches → FanIn):
//! (1) FanOut forwards every change to all branches in order, (2) the
//! missing-fan-in invariant panics (TS `must(...)`), (3) N converging
//! branches collapse to a single downstream push ("does not duplicate
//! pushes").

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::change::{Change, ChangeType};
use rust_ivm::ivm::data::{Node, SortOrder, Value};
use rust_ivm::ivm::fan_in::FanIn;
use rust_ivm::ivm::fan_out::FanOut;
use rust_ivm::ivm::filter::Filter;
use rust_ivm::ivm::filter_operators::{
    FilterInputHandle, FilterOutput, FilterOutputHandle, FilterStart,
};
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::operator::InputBase;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::SourceChange;

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

// Records every change it receives (change type + the row's `b` column) —
// a terminal FilterOutput (accepts every node in filter loops).
struct Collector {
    seen: Rc<RefCell<Vec<(ChangeType, String)>>>,
}
impl FilterOutput for Collector {
    fn begin_filter(&self) {}
    fn end_filter(&self) {}
    fn filter(&self, _node: &Node) -> bool {
        true
    }
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        let b = match change.node().row.get("b") {
            Some(Value::Str(s)) => s.to_string(),
            _ => String::new(),
        };
        self.seen.borrow_mut().push((change.change_type(), b));
    }
}

#[allow(clippy::type_complexity)] // test-only helper: (handle, recorded-changes)
fn collector() -> (FilterOutputHandle, Rc<RefCell<Vec<(ChangeType, String)>>>) {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let handle: FilterOutputHandle = Rc::new(RefCell::new(Collector { seen: seen.clone() }));
    (handle, seen)
}

// Port of TS `fan-out pushes along all paths`: every branch sees
// add/edit/remove in order. TS wires branches via `setFilterOutput` (append)
// and satisfies the invariant with `new FanIn(fanOut, [])`.
#[test]
fn fan_out_pushes_along_all_paths() {
    let src = make_source();
    let input = src
        .borrow_mut()
        .connect(Some(a_sort()), None, None, None, None);

    let start = FilterStart::new(input);
    let start_fi: FilterInputHandle = start.clone();
    let fan_out = FanOut::new(start_fi);

    let (c1, s1) = collector();
    let (c2, s2) = collector();
    let (c3, s3) = collector();
    {
        let fo = fan_out.borrow();
        use rust_ivm::ivm::filter_operators::FilterInput;
        fo.set_filter_output(c1);
        fo.set_filter_output(c2);
        fo.set_filter_output(c3);
    }

    // TS: `new FanIn(fanOut, [])` — no converging branch inputs; purely
    // satisfies the fan-out invariant.
    let schema = fan_out.borrow().get_schema();
    let fan_in = FanIn::new(schema, Vec::new());
    fan_out.borrow_mut().set_fan_in(fan_in);

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
// graph-construction invariant violation and must panic with the message.
#[test]
#[should_panic(expected = "fan-out must have a corresponding fan-in set!")]
fn fan_out_push_without_fan_in_panics() {
    let src = make_source();
    let input = src
        .borrow_mut()
        .connect(Some(a_sort()), None, None, None, None);

    let start = FilterStart::new(input);
    let start_fi: FilterInputHandle = start.clone();
    let fan_out = FanOut::new(start_fi);

    let (c1, _s1) = collector();
    {
        use rust_ivm::ivm::filter_operators::FilterInput;
        fan_out.borrow().set_filter_output(c1);
    }
    // Deliberately DO NOT call set_fan_in.

    src.borrow_mut().push(SourceChange::Add {
        row: Arc::new(row_ab(1.0, "foo")),
    });
}

// Port of TS `fan-out,fan-in pairing does not duplicate pushes`: the same
// change flowing through N pass-through branches is accumulated by FanIn and
// collapsed to a SINGLE downstream push.
#[test]
fn fan_in_does_not_duplicate_pushes() {
    let src = make_source();
    let input = src
        .borrow_mut()
        .connect(Some(a_sort()), None, None, None, None);

    let start = FilterStart::new(input);
    let start_fi: FilterInputHandle = start.clone();
    let fan_out = FanOut::new(start_fi);

    // Three real pass-through branches (TS builds Filter branches).
    let branches: Vec<FilterInputHandle> = (0..3)
        .map(|_| {
            let fo: FilterInputHandle = fan_out.clone();
            let f: FilterInputHandle = Filter::new(fo, Arc::new(|_| true));
            f
        })
        .collect();

    let schema = fan_out.borrow().get_schema();
    let fan_in = FanIn::new(schema, branches);
    fan_out.borrow_mut().set_fan_in(fan_in.clone());

    // Downstream of the fan-in: the single collector that must see NO dupes.
    let (sink, seen) = collector();
    {
        use rust_ivm::ivm::filter_operators::FilterInput;
        fan_in.borrow().set_filter_output(sink);
    }

    src.borrow_mut().push(SourceChange::Add {
        row: Arc::new(row_ab(1.0, "foo")),
    });

    assert_eq!(
        *seen.borrow(),
        vec![(ChangeType::Add, "foo".to_string())],
        "three converging branches collapse to a single downstream add"
    );
}
