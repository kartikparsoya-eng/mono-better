//! Tests for Snitch — port of `zql/src/ivm/snitch.ts` behavior (no dedicated TS
//! test file exists; Snitch is TS's observer operator, so these pin its logging
//! contract directly from the TS source: it records `fetch`, `fetchCount`, and
//! `push` messages gated by its `logTypes`, and re-emits fetched nodes + forwards
//! pushes to its downstream output unchanged.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::change::{Change, SourceChange};
use rust_ivm::ivm::data::{Node, Row, SortOrder, Value};
use rust_ivm::ivm::operator::{FetchRequest, Input, InputBase, Output, OutputHandle};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::snitch::{ChangeRecord, LogType, Snitch, SnitchMessage, to_change_record};
use rust_ivm::ivm::source::MemorySource;

fn str_val(s: &str) -> Value {
    Value::Str(Arc::from(s))
}

fn id_row(id: &str) -> FxHashMap<String, Value> {
    let mut r = FxHashMap::default();
    r.insert("id".to_string(), str_val(id));
    r
}

fn id_row_arc(id: &str) -> Row {
    Arc::new(id_row(id))
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

// Minimal downstream collector: records the changes Snitch forwards.
struct Collector {
    pushes: Rc<RefCell<Vec<Change>>>,
}
impl Output for Collector {
    fn push(&mut self, change: Change, _pusher: &dyn InputBase) {
        self.pushes.borrow_mut().push(change);
    }
}
fn collector() -> (OutputHandle, Rc<RefCell<Vec<Change>>>) {
    let pushes = Rc::new(RefCell::new(Vec::new()));
    let c: OutputHandle = Rc::new(RefCell::new(Collector {
        pushes: pushes.clone(),
    }));
    (c, pushes)
}

// A fetch through a Snitch with [Fetch, FetchCount] logs both a Fetch and a
// FetchCount (with the consumed node count) and STILL re-emits every node.
#[test]
fn fetch_logs_fetch_and_fetch_count_and_reemits() {
    let src = make_source();
    src.borrow_mut().add_row(id_row("a"));
    src.borrow_mut().add_row(id_row("b"));
    let input = src.borrow_mut().connect(Some(id_sort()), None, None, None);

    let snitch = Snitch::new(
        input,
        "s1".to_string(),
        vec![],
        vec![LogType::Fetch, LogType::FetchCount],
    );

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(snitch.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(nodes.len(), 2, "Snitch must re-emit all fetched nodes");

    let log = snitch.borrow().log.borrow().clone();
    assert_eq!(log.len(), 2);
    assert!(
        matches!(&log[0], SnitchMessage::Fetch { name, .. } if name == "s1"),
        "first message is Fetch, got {:?}",
        log[0]
    );
    match &log[1] {
        SnitchMessage::FetchCount { name, count, .. } => {
            assert_eq!(name, "s1");
            assert_eq!(*count, 2, "FetchCount reflects the consumed node count");
        }
        other => panic!("second message must be FetchCount, got {other:?}"),
    }
}

// A push through a Snitch with [Push] logs a Push ChangeRecord AND forwards the
// change unchanged to the downstream output.
#[test]
fn push_logs_change_record_and_forwards() {
    let src = make_source();
    let input = src.borrow_mut().connect(Some(id_sort()), None, None, None);
    let snitch = Snitch::new(input, "s2".to_string(), vec![], vec![LogType::Push]);

    let (out, pushes) = collector();
    snitch.borrow().set_output(out);

    src.borrow_mut().push(SourceChange::Add {
        row: id_row_arc("z"),
    });

    // Logged as a Push{Add{row}}.
    let log = snitch.borrow().log.borrow().clone();
    assert_eq!(log.len(), 1);
    match &log[0] {
        SnitchMessage::Push {
            name,
            change: ChangeRecord::Add { row },
        } => {
            assert_eq!(name, "s2");
            assert_eq!(row.get("id"), Some(&str_val("z")));
        }
        other => panic!("expected Push(Add), got {other:?}"),
    }
    // Forwarded downstream unchanged.
    assert_eq!(pushes.borrow().len(), 1);
    assert!(
        matches!(&pushes.borrow()[0], Change::Add(n) if n.row.get("id") == Some(&str_val("z")))
    );
}

// logTypes gates what is recorded: a Push-only Snitch logs nothing on fetch.
#[test]
fn log_type_filtering_push_only_ignores_fetch() {
    let src = make_source();
    src.borrow_mut().add_row(id_row("a"));
    let input = src.borrow_mut().connect(Some(id_sort()), None, None, None);
    let snitch = Snitch::new(input, "s3".to_string(), vec![], vec![LogType::Push]);

    let _n: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(snitch.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert!(
        snitch.borrow().log.borrow().is_empty(),
        "a Push-only Snitch must not log fetch/fetchCount"
    );
}

// to_change_record maps each Change variant to its logging record 1:1.
#[test]
fn to_change_record_maps_all_variants() {
    // Add
    match to_change_record(&Change::Add(Node::new(id_row_arc("a")))) {
        ChangeRecord::Add { row } => assert_eq!(row.get("id"), Some(&str_val("a"))),
        other => panic!("Add -> {other:?}"),
    }
    // Remove
    match to_change_record(&Change::Remove(Node::new(id_row_arc("b")))) {
        ChangeRecord::Remove { row } => assert_eq!(row.get("id"), Some(&str_val("b"))),
        other => panic!("Remove -> {other:?}"),
    }
    // Edit carries both new and old rows.
    let edit = Change::Edit {
        node: Node::new(id_row_arc("new")),
        old_node: Node::new(id_row_arc("old")),
    };
    match to_change_record(&edit) {
        ChangeRecord::Edit { row, old_row } => {
            assert_eq!(row.get("id"), Some(&str_val("new")));
            assert_eq!(old_row.get("id"), Some(&str_val("old")));
        }
        other => panic!("Edit -> {other:?}"),
    }
}
