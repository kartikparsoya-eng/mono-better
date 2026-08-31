//! Tests for MeasurePushOperator — port of `zql/src/query/measure-push-operator.ts`.
//! The operator wraps an Input and, on each push, times the downstream push and
//! records a metric (name, elapsed_ms, query_id) via its MetricsDelegate while
//! forwarding the change unchanged. The whole operator was untested (triage).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::change::Change;
use rust_ivm::ivm::data::{SortOrder, Value};
use rust_ivm::ivm::operator::{Input, InputBase, Output, OutputHandle};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;
use rust_ivm::ivm::source::SourceChange;
use rust_ivm::query::measure_push_operator::{MeasurePushOperator, MetricsDelegate};

fn str_val(s: &str) -> Value {
    Value::Str(Arc::from(s))
}
fn id_row(id: &str) -> FxHashMap<String, Value> {
    let mut r = FxHashMap::default();
    r.insert("id".to_string(), str_val(id));
    r
}
fn make_source() -> Rc<RefCell<MemorySource>> {
    let cols: HashMap<String, ColumnType> =
        HashMap::from([("id".to_string(), ColumnType::String { optional: false })]);
    Rc::new(RefCell::new(MemorySource::new(
        "t",
        cols,
        vec!["id".to_string()],
    )))
}
fn id_sort() -> SortOrder {
    Arc::new(vec![["id".to_string(), "asc".to_string()]])
}

// Records every (name, query_id) the operator reports.
struct RecordingMetrics {
    entries: Rc<RefCell<Vec<(String, String)>>>,
}
impl MetricsDelegate for RecordingMetrics {
    fn add_metric(&self, name: &str, _ms: f64, query_id: &str) {
        self.entries
            .borrow_mut()
            .push((name.to_string(), query_id.to_string()));
    }
}

// Collector downstream of the operator.
struct Collector {
    pushes: Rc<RefCell<usize>>,
}
impl Output for Collector {
    fn push(&mut self, _change: Change, _pusher: &dyn InputBase) {
        *self.pushes.borrow_mut() += 1;
    }
}

#[test]
fn records_a_metric_per_push_and_forwards_downstream() {
    let src = make_source();
    let input = src.borrow_mut().connect(Some(id_sort()), None, None, None);

    let entries = Rc::new(RefCell::new(Vec::new()));
    let metrics = Rc::new(RecordingMetrics {
        entries: entries.clone(),
    });
    let mpo =
        MeasurePushOperator::new(input, "q1".to_string(), metrics, "push_latency".to_string());

    let pushes = Rc::new(RefCell::new(0usize));
    let collector: OutputHandle = Rc::new(RefCell::new(Collector {
        pushes: pushes.clone(),
    }));
    mpo.borrow().set_output(collector);

    src.borrow_mut().push(SourceChange::Add {
        row: Arc::new(id_row("a")),
    });
    src.borrow_mut().push(SourceChange::Add {
        row: Arc::new(id_row("b")),
    });

    // One metric recorded per push, each tagged with the configured name + query_id.
    let recorded = entries.borrow().clone();
    assert_eq!(recorded.len(), 2, "one metric per push");
    assert!(
        recorded
            .iter()
            .all(|(name, qid)| name == "push_latency" && qid == "q1")
    );
    // Each change forwarded downstream unchanged.
    assert_eq!(*pushes.borrow(), 2);
}
