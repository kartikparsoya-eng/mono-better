//! Additional source tests ported from TS v1.7.0 source.test.ts.
//! Covers: simple fetch, constraint null semantics, fetch-start reverse,
//! multiConstraints (IN lists), push errors, per-output sorts, JSON type.

use std::cell::RefCell;
use std::rc::Rc;
use std::collections::HashMap;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::change::{Change, SourceChange};
use rust_ivm::ivm::constraint::{Constraint, MultiConstraint};
use rust_ivm::ivm::data::{Node, Row, Value};
use rust_ivm::ivm::operator::{Basis, FetchRequest, Input, InputBase, Output, Start};
use rust_ivm::ivm::schema::{ColumnType, SourceSchema, System};
use rust_ivm::ivm::source::{MemorySource, CollectOutput, SourceInput, SharedOverlay};
use rust_ivm::ivm::stream::NodeStream;

fn make_row(pairs: &[(&str, Value)]) -> Row {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Arc::new(map)
}

fn make_source(name: &str, pk: &[&str], columns: &[(&str, ColumnType)]) -> Rc<RefCell<MemorySource>> {
    let cols: HashMap<String, ColumnType> = columns
        .iter()
        .map(|(n, t)| (n.to_string(), t.clone()))
        .collect();
    Rc::new(RefCell::new(MemorySource::new(
        name,
        cols,
        pk.iter().map(|s| s.to_string()).collect(),
    )))
}

fn add_row(source: &Rc<RefCell<MemorySource>>, pairs: &[(&str, Value)]) {
    let row_data: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    source.borrow_mut().add_row(row_data);
}

fn s(s: &str) -> Value {
    Value::Str(Arc::from(s))
}

fn s_owned(s: String) -> Value {
    Value::Str(Arc::from(s.as_str()))
}

fn num(n: f64) -> Value {
    Value::F64(n)
}

fn bool_val(b: bool) -> Value {
    Value::Bool(b)
}

fn sort_by(pk: &str) -> Option<rust_ivm::ivm::data::SortOrder> {
    Some(Arc::new(vec!([pk.to_string(), "asc".to_string()])))
}

fn fetch_all(input: &Rc<RefCell<dyn Input>>, req: &FetchRequest) -> Vec<Node> {
    rust_ivm::ivm::stream::skip_yields(input.borrow().fetch(req)).collect()
}

fn row_id(n: &Node) -> Value {
    n.row.get("id").cloned().unwrap_or(Value::Null)
}

// ===========================================================================
// Simple fetch — add rows, fetch sees them in order, remove rows
// ===========================================================================

#[test]
fn test_simple_fetch() {
    let source = make_source("table", &["a"], &[("a", ColumnType::Number { optional: false })]);
    let input = source.borrow_mut().connect(None, None, None, None);

    // Empty initially
    let nodes = fetch_all(&input, &FetchRequest::default());
    assert!(nodes.is_empty());

    // Add rows
    source.borrow_mut().push(SourceChange::Add { row: make_row(&[("a", num(3.0))]) });
    let nodes = fetch_all(&input, &FetchRequest::default());
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].row.get("a"), Some(&num(3.0)));

    source.borrow_mut().push(SourceChange::Add { row: make_row(&[("a", num(1.0))]) });
    source.borrow_mut().push(SourceChange::Add { row: make_row(&[("a", num(2.0))]) });
    let nodes = fetch_all(&input, &FetchRequest::default());
    assert_eq!(nodes.len(), 3);
    // Sorted by primary key (a): 1, 2, 3
    assert_eq!(nodes[0].row.get("a"), Some(&num(1.0)));
    assert_eq!(nodes[1].row.get("a"), Some(&num(2.0)));
    assert_eq!(nodes[2].row.get("a"), Some(&num(3.0)));

    // Remove
    source.borrow_mut().push(SourceChange::Remove { row: make_row(&[("a", num(1.0))]) });
    let nodes = fetch_all(&input, &FetchRequest::default());
    assert_eq!(nodes.len(), 2);

    source.borrow_mut().push(SourceChange::Remove { row: make_row(&[("a", num(2.0))]) });
    source.borrow_mut().push(SourceChange::Remove { row: make_row(&[("a", num(3.0))]) });
    let nodes = fetch_all(&input, &FetchRequest::default());
    assert!(nodes.is_empty());
}

// ===========================================================================
// Constraint null semantics — null !== null (join semantics)
// ===========================================================================

#[test]
fn test_constraint_null_semantics() {
    let source = make_source("table", &["a"], &[
        ("a", ColumnType::Number { optional: false }),
        ("b", ColumnType::Boolean { optional: false }),
        ("c", ColumnType::Number { optional: true }),
        ("d", ColumnType::String { optional: true }),
    ]);
    add_row(&source, &[("a", num(3.0)), ("b", bool_val(true)), ("c", num(1.0)), ("d", Value::Null)]);
    add_row(&source, &[("a", num(1.0)), ("b", bool_val(true)), ("c", num(2.0)), ("d", Value::Null)]);
    add_row(&source, &[("a", num(2.0)), ("b", bool_val(false)), ("c", Value::Null), ("d", Value::Null)]);

    let input = source.borrow_mut().connect(None, None, None, None);

    // Constraint b=true → rows with a=1, a=3
    let mut c = Constraint::default();
    c.insert("b".to_string(), bool_val(true));
    let req = FetchRequest { constraint: Some(c), ..Default::default() };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 2);

    // Constraint b=false → row with a=2
    let mut c = Constraint::default();
    c.insert("b".to_string(), bool_val(false));
    let req = FetchRequest { constraint: Some(c), ..Default::default() };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].row.get("a"), Some(&num(2.0)));

    // Constraint c=1 → row with a=3
    let mut c = Constraint::default();
    c.insert("c".to_string(), num(1.0));
    let req = FetchRequest { constraint: Some(c), ..Default::default() };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].row.get("a"), Some(&num(3.0)));

    // Constraint c=null → no rows (null !== null)
    let mut c = Constraint::default();
    c.insert("c".to_string(), Value::Null);
    let req = FetchRequest { constraint: Some(c), ..Default::default() };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 0);

    // Constraint c=0 → no rows
    let mut c = Constraint::default();
    c.insert("c".to_string(), num(0.0));
    let req = FetchRequest { constraint: Some(c), ..Default::default() };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 0);

    // Constraint b=true AND c=1 → row with a=3
    let mut c = Constraint::default();
    c.insert("b".to_string(), bool_val(true));
    c.insert("c".to_string(), num(1.0));
    let req = FetchRequest { constraint: Some(c), ..Default::default() };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].row.get("a"), Some(&num(3.0)));

    // Constraint b=true AND d=null → no rows (null !== null)
    let mut c = Constraint::default();
    c.insert("b".to_string(), bool_val(true));
    c.insert("d".to_string(), Value::Null);
    let req = FetchRequest { constraint: Some(c), ..Default::default() };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 0);
}

// ===========================================================================
// Fetch start reverse — start basis with reverse sort
// ===========================================================================

#[test]
fn test_fetch_start_reverse() {
    let source = make_source("table", &["a"], &[("a", ColumnType::Number { optional: false })]);
    add_row(&source, &[("a", num(2.0))]);
    add_row(&source, &[("a", num(3.0))]);

    let input = source.borrow_mut().connect(None, None, None, None);

    // start at a=2, reverse → [2] (TS: btree reverse from 2, includes 2, goes backwards)
    let req = FetchRequest {
        start: Some(Start { row: make_row(&[("a", num(2.0))]), basis: Basis::At }),
        reverse: true,
        ..Default::default()
    };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].row.get("a"), Some(&num(2.0)));

    // start after a=2, reverse → [] (TS: btree reverse after 2, nothing before 2)
    let req = FetchRequest {
        start: Some(Start { row: make_row(&[("a", num(2.0))]), basis: Basis::After }),
        reverse: true,
        ..Default::default()
    };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 0);
}

// ===========================================================================
// MultiConstraints (IN lists) — v1.7.0 feature
// ===========================================================================

fn setup_parents() -> Rc<RefCell<MemorySource>> {
    let source = make_source("parent", &["id"], &[
        ("id", ColumnType::Number { optional: false }),
        ("org", ColumnType::String { optional: false }),
        ("active", ColumnType::Boolean { optional: false }),
    ]);
    for i in 1..=5 {
        add_row(&source, &[
            ("id", num(i as f64)),
            ("org", s_owned(if i % 2 == 0 { "o-even".to_string() } else { "o-odd".to_string() })),
            ("active", bool_val(i != 2)),
        ]);
    }
    source
}

#[test]
fn test_mc_single_key_in_list() {
    let source = setup_parents();
    let input = source.borrow_mut().connect(None, None, None, None);

    let mut mc1 = Constraint::default(); mc1.insert("id".to_string(), num(4.0));
    let mut mc2 = Constraint::default(); mc2.insert("id".to_string(), num(1.0));
    let mut mc3 = Constraint::default(); mc3.insert("id".to_string(), num(3.0));
    let mc: MultiConstraint = vec![mc1, mc2, mc3];

    let req = FetchRequest { multi_constraints: vec![mc], ..Default::default() };
    let nodes = fetch_all(&input, &req);
    // Should return in sort order: 1, 3, 4
    assert_eq!(nodes.len(), 3);
    assert_eq!(nodes[0].row.get("id"), Some(&num(1.0)));
    assert_eq!(nodes[1].row.get("id"), Some(&num(3.0)));
    assert_eq!(nodes[2].row.get("id"), Some(&num(4.0)));
}

#[test]
fn test_mc_no_matches() {
    let source = setup_parents();
    let input = source.borrow_mut().connect(None, None, None, None);

    let mut mc1 = Constraint::default(); mc1.insert("id".to_string(), num(99.0));
    let mut mc2 = Constraint::default(); mc2.insert("id".to_string(), num(100.0));
    let mc: MultiConstraint = vec![mc1, mc2];

    let req = FetchRequest { multi_constraints: vec![mc], ..Default::default() };
    let nodes = fetch_all(&input, &req);
    assert!(nodes.is_empty());
}

#[test]
fn test_mc_empty_array_noop() {
    let source = setup_parents();
    let input = source.borrow_mut().connect(None, None, None, None);

    let req = FetchRequest { multi_constraints: vec![], ..Default::default() };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 5);
}

#[test]
fn test_mc_empty_entry_ignored() {
    let source = setup_parents();
    let input = source.borrow_mut().connect(None, None, None, None);

    // First MultiConstraint is empty (no entries) → skipped/ignored.
    // Second MultiConstraint has id=2 and id=4 → OR'd, matches those rows.
    let mc1: MultiConstraint = vec![];
    let mc2: MultiConstraint = vec![
        {
            let mut c = Constraint::default();
            c.insert("id".to_string(), num(2.0));
            c
        },
        {
            let mut c = Constraint::default();
            c.insert("id".to_string(), num(4.0));
            c
        },
    ];

    let req = FetchRequest { multi_constraints: vec![mc1, mc2], ..Default::default() };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].row.get("id"), Some(&num(2.0)));
    assert_eq!(nodes[1].row.get("id"), Some(&num(4.0)));
}

#[test]
fn test_mc_two_anded() {
    let source = setup_parents();
    let input = source.borrow_mut().connect(None, None, None, None);

    // id IN (1,2,3,4) AND org IN ('o-even') → 2, 4
    let mc1: MultiConstraint = (1..=4).map(|i| {
        let mut c = Constraint::default();
        c.insert("id".to_string(), num(i as f64));
        c
    }).collect();
    let mc2: MultiConstraint = {
        let mut c = Constraint::default();
        c.insert("org".to_string(), s("o-even"));
        vec![c]
    };

    let req = FetchRequest { multi_constraints: vec![mc1, mc2], ..Default::default() };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].row.get("id"), Some(&num(2.0)));
    assert_eq!(nodes[1].row.get("id"), Some(&num(4.0)));
}

#[test]
fn test_mc_with_constraint_anded() {
    let source = setup_parents();
    let input = source.borrow_mut().connect(None, None, None, None);

    // active=true AND id IN (1,2,3,4,5) → 1,3,4,5 (excludes 2)
    let mut constraint = Constraint::default();
    constraint.insert("active".to_string(), bool_val(true));
    let mc: MultiConstraint = (1..=5).map(|i| {
        let mut c = Constraint::default();
        c.insert("id".to_string(), num(i as f64));
        c
    }).collect();

    let req = FetchRequest {
        constraint: Some(constraint),
        multi_constraints: vec![mc],
        ..Default::default()
    };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 4);
    let ids: Vec<f64> = nodes.iter().map(|n| match n.row.get("id") {
        Some(Value::F64(v)) => *v, _ => 0.0
    }).collect();
    assert_eq!(ids, vec![1.0, 3.0, 4.0, 5.0]);
}

#[test]
fn test_mc_with_reverse() {
    let source = setup_parents();
    let input = source.borrow_mut().connect(None, None, None, None);

    let mc: MultiConstraint = vec![1, 3, 5].iter().map(|&i| {
        let mut c = Constraint::default();
        c.insert("id".to_string(), num(i as f64));
        c
    }).collect();

    let req = FetchRequest {
        multi_constraints: vec![mc],
        reverse: true,
        ..Default::default()
    };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 3);
    // Reverse order: 5, 3, 1
    assert_eq!(nodes[0].row.get("id"), Some(&num(5.0)));
    assert_eq!(nodes[1].row.get("id"), Some(&num(3.0)));
    assert_eq!(nodes[2].row.get("id"), Some(&num(1.0)));
}

#[test]
fn test_mc_with_start_after() {
    let source = setup_parents();
    let input = source.borrow_mut().connect(None, None, None, None);

    let mc: MultiConstraint = vec![1, 3, 5].iter().map(|&i| {
        let mut c = Constraint::default();
        c.insert("id".to_string(), num(i as f64));
        c
    }).collect();

    let req = FetchRequest {
        multi_constraints: vec![mc],
        start: Some(Start { row: make_row(&[("id", num(1.0))]), basis: Basis::After }),
        ..Default::default()
    };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].row.get("id"), Some(&num(3.0)));
    assert_eq!(nodes[1].row.get("id"), Some(&num(5.0)));
}

#[test]
fn test_mc_null_entries_never_match() {
    let source = setup_parents();
    let input = source.borrow_mut().connect(None, None, None, None);

    let mc: MultiConstraint = vec![{
        let mut c = Constraint::default();
        c.insert("id".to_string(), Value::Null);
        c
    }];

    let req = FetchRequest { multi_constraints: vec![mc], ..Default::default() };
    let nodes = fetch_all(&input, &req);
    assert!(nodes.is_empty(), "NULL entries in IN list should never match");
}

#[test]
fn test_mc_compound_key() {
    let source = make_source("items", &["a", "b"], &[
        ("a", ColumnType::Number { optional: false }),
        ("b", ColumnType::String { optional: false }),
    ]);
    for i in 1..=4 {
        add_row(&source, &[
            ("a", num(i as f64)),
            ("b", s_owned(format!("val{}", i))),
        ]);
    }
    let input = source.borrow_mut().connect(None, None, None, None);

    let mc: MultiConstraint = vec![
        {
            let mut c = Constraint::default();
            c.insert("a".to_string(), num(1.0));
            c.insert("b".to_string(), s("val1"));
            c
        },
        {
            let mut c = Constraint::default();
            c.insert("a".to_string(), num(3.0));
            c.insert("b".to_string(), s("val3"));
            c
        },
    ];

    let req = FetchRequest { multi_constraints: vec![mc], ..Default::default() };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 2);
}

// ===========================================================================
// Push — gracefully handles duplicate add and remove-missing
// ===========================================================================
// Source-drift assertions (TS parity)
//
// `MemorySource::push_internal` now panics on duplicate add, missing remove,
// and missing edit old-row, matching TS `memory-source.ts` dev assertions and
// `TableSource::validate_change`. SQLite-backed MemorySource (`db_path` set)
// skips this validation because the in-memory cache is only partial.
// ===========================================================================

#[test]
#[should_panic(expected = "source drift: Add duplicate row in table")]
fn test_push_duplicate_add_panics() {
    let source = make_source("table", &["a"], &[("a", ColumnType::Number { optional: false })]);
    add_row(&source, &[("a", num(1.0))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let _ = input; // keep alive

    source.borrow_mut().push(SourceChange::Add { row: make_row(&[("a", num(1.0))]) });
}

#[test]
#[should_panic(expected = "source drift: Remove missing row from table")]
fn test_push_remove_missing_panics() {
    let source = make_source("table", &["a"], &[("a", ColumnType::Number { optional: false })]);
    let input = source.borrow_mut().connect(None, None, None, None);
    let _ = input;

    source.borrow_mut().push(SourceChange::Remove { row: make_row(&[("a", num(99.0))]) });
}

// ===========================================================================
// Per-output sorts — different connections with different sort orders
// ===========================================================================

#[test]
fn test_per_output_sorts() {
    let source = make_source("table", &["id"], &[
        ("id", ColumnType::Number { optional: false }),
        ("name", ColumnType::String { optional: false }),
    ]);
    add_row(&source, &[("id", num(3.0)), ("name", s("Charlie"))]);
    add_row(&source, &[("id", num(1.0)), ("name", s("Alice"))]);
    add_row(&source, &[("id", num(2.0)), ("name", s("Bob"))]);

    // Connection 1: sort by id asc
    let sort_id = sort_by("id");
    let input1 = source.borrow_mut().connect(sort_id, None, None, None);

    // Connection 2: sort by name asc
    let sort_name: rust_ivm::ivm::data::SortOrder = Arc::new(vec![["name".to_string(), "asc".to_string()]]);
    let input2 = source.borrow_mut().connect(Some(sort_name), None, None, None);

    let nodes1 = fetch_all(&input1, &FetchRequest::default());
    assert_eq!(nodes1.len(), 3);
    assert_eq!(nodes1[0].row.get("id"), Some(&num(1.0)));
    assert_eq!(nodes1[1].row.get("id"), Some(&num(2.0)));
    assert_eq!(nodes1[2].row.get("id"), Some(&num(3.0)));

    let nodes2 = fetch_all(&input2, &FetchRequest::default());
    assert_eq!(nodes2.len(), 3);
    assert_eq!(nodes2[0].row.get("name"), Some(&s("Alice")));
    assert_eq!(nodes2[1].row.get("name"), Some(&s("Bob")));
    assert_eq!(nodes2[2].row.get("name"), Some(&s("Charlie")));
}

// ===========================================================================
// JSON type support
// ===========================================================================

#[test]
fn test_json_type_support() {
    let source = make_source("table", &["id"], &[
        ("id", ColumnType::Number { optional: false }),
        ("data", ColumnType::Json { optional: false }),
    ]);
    add_row(&source, &[
        ("id", num(1.0)),
        ("data", Value::Json(Arc::from(r#"{"key":"value"}"#))),
    ]);
    add_row(&source, &[
        ("id", num(2.0)),
        ("data", Value::Json(Arc::from(r#"[1,2,3]"#))),
    ]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let nodes = fetch_all(&input, &FetchRequest::default());
    assert_eq!(nodes.len(), 2);
    match nodes[0].row.get("data") {
        Some(Value::Json(j)) => assert!(j.contains("key")),
        _ => panic!("Expected JSON value"),
    }
    match nodes[1].row.get("data") {
        Some(Value::Json(j)) => assert!(j.contains("1")),
        _ => panic!("Expected JSON value"),
    }
}

// ===========================================================================
// Push with CollectOutput — verify changes are pushed to outputs
// ===========================================================================

#[test]
fn test_push_with_collect_output() {
    let source = make_source("table", &["a"], &[("a", ColumnType::Number { optional: false })]);
    add_row(&source, &[("a", num(1.0))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let output = Rc::new(RefCell::new(CollectOutput::new()));
    input.borrow_mut().set_output(Rc::clone(&output) as _);

    // Push add → output receives Add change
    source.borrow_mut().push(SourceChange::Add { row: make_row(&[("a", num(2.0))]) });
    assert_eq!(output.borrow().changes.len(), 1);
    match &output.borrow().changes[0] {
        rust_ivm::ivm::change::Change::Add(n) => assert_eq!(n.row.get("a"), Some(&num(2.0))),
        _ => panic!("Expected Add change"),
    }

    // Push remove → output receives Remove change
    source.borrow_mut().push(SourceChange::Remove { row: make_row(&[("a", num(1.0))]) });
    assert_eq!(output.borrow().changes.len(), 2);
    match &output.borrow().changes[1] {
        rust_ivm::ivm::change::Change::Remove(n) => assert_eq!(n.row.get("a"), Some(&num(1.0))),
        _ => panic!("Expected Remove change"),
    }
}

// ===========================================================================
// OverlaySpy — output that fetches from input during push to test overlay
// ===========================================================================

struct OverlaySpy {
    input: Rc<RefCell<dyn Input>>,
    fetch_req: FetchRequest,
    fetches: Vec<Vec<Node>>,
}

impl OverlaySpy {
    fn new(input: Rc<RefCell<dyn Input>>, fetch_req: FetchRequest) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(OverlaySpy { input, fetch_req, fetches: Vec::new() }))
    }
}

impl Output for OverlaySpy {
    fn push(&mut self, _change: Change, _pusher: &dyn InputBase) {
        let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(self.input.borrow().fetch(&self.fetch_req)).collect();
        self.fetches.push(nodes);
    }
}

fn make_source_ab() -> Rc<RefCell<MemorySource>> {
    make_source("table", &["a"], &[
        ("a", ColumnType::Number { optional: false }),
        ("b", ColumnType::Boolean { optional: false }),
    ])
}

fn fetch_vals(nodes: &[Node], col: &str) -> Vec<Value> {
    nodes.iter().map(|n| n.row.get(col).cloned().unwrap_or(Value::Null)).collect()
}

// ===========================================================================
// Overlay-vs-constraint — overlay during push filtered by constraint
// ===========================================================================

#[test]
fn test_overlay_vs_constraint_c1() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(2.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(4.0)), ("b", bool_val(true))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mut constraint = Constraint::default();
    constraint.insert("b".to_string(), bool_val(true));
    let req = FetchRequest { constraint: Some(constraint), ..Default::default() };
    let spy = OverlaySpy::new(input.clone(), req);
    input.borrow_mut().set_output(Rc::clone(&spy) as _);

    source.borrow_mut().push(SourceChange::Add { row: make_row(&[("a", num(1.0)), ("b", bool_val(true))]) });

    assert_eq!(spy.borrow().fetches.len(), 1);
    let vals = fetch_vals(&spy.borrow().fetches[0], "a");
    assert_eq!(vals, vec![num(1.0), num(4.0)]);
}

#[test]
fn test_overlay_vs_constraint_c2() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(2.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(4.0)), ("b", bool_val(true))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mut constraint = Constraint::default();
    constraint.insert("b".to_string(), bool_val(true));
    let req = FetchRequest { constraint: Some(constraint), ..Default::default() };
    let spy = OverlaySpy::new(input.clone(), req);
    input.borrow_mut().set_output(Rc::clone(&spy) as _);

    source.borrow_mut().push(SourceChange::Add { row: make_row(&[("a", num(1.0)), ("b", bool_val(false))]) });

    assert_eq!(spy.borrow().fetches.len(), 1);
    let vals = fetch_vals(&spy.borrow().fetches[0], "a");
    assert_eq!(vals, vec![num(4.0)]);
}

#[test]
fn test_overlay_vs_constraint_c3() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(2.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(4.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(5.0)), ("b", bool_val(true))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mut constraint = Constraint::default();
    constraint.insert("b".to_string(), bool_val(true));
    let req = FetchRequest { constraint: Some(constraint), ..Default::default() };
    let spy = OverlaySpy::new(input.clone(), req);
    input.borrow_mut().set_output(Rc::clone(&spy) as _);

    source.borrow_mut().push(SourceChange::Edit {
        row: make_row(&[("a", num(4.0)), ("b", bool_val(false))]),
        old_row: make_row(&[("a", num(4.0)), ("b", bool_val(true))]),
    });

    assert_eq!(spy.borrow().fetches.len(), 1);
    let vals = fetch_vals(&spy.borrow().fetches[0], "a");
    assert_eq!(vals, vec![num(5.0)]);
}

#[test]
fn test_overlay_vs_constraint_c4() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(2.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(4.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(5.0)), ("b", bool_val(true))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mut constraint = Constraint::default();
    constraint.insert("b".to_string(), bool_val(false));
    let req = FetchRequest { constraint: Some(constraint), ..Default::default() };
    let spy = OverlaySpy::new(input.clone(), req);
    input.borrow_mut().set_output(Rc::clone(&spy) as _);

    source.borrow_mut().push(SourceChange::Edit {
        row: make_row(&[("a", num(4.0)), ("b", bool_val(false))]),
        old_row: make_row(&[("a", num(4.0)), ("b", bool_val(true))]),
    });

    assert_eq!(spy.borrow().fetches.len(), 1);
    let vals = fetch_vals(&spy.borrow().fetches[0], "a");
    assert_eq!(vals, vec![num(2.0), num(4.0)]);
}

#[test]
fn test_overlay_vs_constraint_c5() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(2.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(4.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(5.0)), ("b", bool_val(true))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mut constraint = Constraint::default();
    constraint.insert("a".to_string(), num(4.0));
    constraint.insert("b".to_string(), bool_val(false));
    let req = FetchRequest { constraint: Some(constraint), ..Default::default() };
    let spy = OverlaySpy::new(input.clone(), req);
    input.borrow_mut().set_output(Rc::clone(&spy) as _);

    source.borrow_mut().push(SourceChange::Edit {
        row: make_row(&[("a", num(4.0)), ("b", bool_val(false))]),
        old_row: make_row(&[("a", num(4.0)), ("b", bool_val(true))]),
    });

    assert_eq!(spy.borrow().fetches.len(), 1);
    let vals = fetch_vals(&spy.borrow().fetches[0], "a");
    assert_eq!(vals, vec![num(4.0)]);
}

// ===========================================================================
// Overlay-vs-multiConstraints — overlay during push filtered by IN lists
// ===========================================================================

#[test]
fn test_overlay_vs_mc_add_matching() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(1.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(4.0)), ("b", bool_val(true))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mc: MultiConstraint = vec![
        { let mut c = Constraint::default(); c.insert("a".to_string(), num(1.0)); c },
        { let mut c = Constraint::default(); c.insert("a".to_string(), num(2.0)); c },
        { let mut c = Constraint::default(); c.insert("a".to_string(), num(3.0)); c },
    ];
    let req = FetchRequest { multi_constraints: vec![mc], ..Default::default() };
    let spy = OverlaySpy::new(input.clone(), req);
    input.borrow_mut().set_output(Rc::clone(&spy) as _);

    source.borrow_mut().push(SourceChange::Add { row: make_row(&[("a", num(2.0)), ("b", bool_val(true))]) });

    assert_eq!(spy.borrow().fetches.len(), 1);
    let vals = fetch_vals(&spy.borrow().fetches[0], "a");
    assert_eq!(vals, vec![num(1.0), num(2.0)]);
}

#[test]
fn test_overlay_vs_mc_add_outside_dropped() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(1.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(4.0)), ("b", bool_val(true))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mc: MultiConstraint = vec![
        { let mut c = Constraint::default(); c.insert("a".to_string(), num(1.0)); c },
        { let mut c = Constraint::default(); c.insert("a".to_string(), num(4.0)); c },
    ];
    let req = FetchRequest { multi_constraints: vec![mc], ..Default::default() };
    let spy = OverlaySpy::new(input.clone(), req);
    input.borrow_mut().set_output(Rc::clone(&spy) as _);

    source.borrow_mut().push(SourceChange::Add { row: make_row(&[("a", num(2.0)), ("b", bool_val(true))]) });

    assert_eq!(spy.borrow().fetches.len(), 1);
    let vals = fetch_vals(&spy.borrow().fetches[0], "a");
    assert_eq!(vals, vec![num(1.0), num(4.0)]);
}

#[test]
fn test_overlay_vs_mc_remove_matching() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(1.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(2.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(4.0)), ("b", bool_val(true))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mc: MultiConstraint = vec![
        { let mut c = Constraint::default(); c.insert("a".to_string(), num(1.0)); c },
        { let mut c = Constraint::default(); c.insert("a".to_string(), num(2.0)); c },
    ];
    let req = FetchRequest { multi_constraints: vec![mc], ..Default::default() };
    let spy = OverlaySpy::new(input.clone(), req);
    input.borrow_mut().set_output(Rc::clone(&spy) as _);

    source.borrow_mut().push(SourceChange::Remove { row: make_row(&[("a", num(2.0)), ("b", bool_val(true))]) });

    assert_eq!(spy.borrow().fetches.len(), 1);
    let vals = fetch_vals(&spy.borrow().fetches[0], "a");
    assert_eq!(vals, vec![num(1.0)]);
}

#[test]
fn test_overlay_vs_mc_edit_remove_in_add_out() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(1.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(5.0)), ("b", bool_val(true))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mc: MultiConstraint = vec![
        { let mut c = Constraint::default(); c.insert("a".to_string(), num(1.0)); c },
        { let mut c = Constraint::default(); c.insert("a".to_string(), num(5.0)); c },
    ];
    let req = FetchRequest { multi_constraints: vec![mc], ..Default::default() };
    let spy = OverlaySpy::new(input.clone(), req);
    input.borrow_mut().set_output(Rc::clone(&spy) as _);

    source.borrow_mut().push(SourceChange::Edit {
        row: make_row(&[("a", num(9.0)), ("b", bool_val(true))]),
        old_row: make_row(&[("a", num(5.0)), ("b", bool_val(true))]),
    });

    assert_eq!(spy.borrow().fetches.len(), 1);
    let vals = fetch_vals(&spy.borrow().fetches[0], "a");
    assert_eq!(vals, vec![num(1.0)]);
}

#[test]
fn test_overlay_vs_mc_two_anded() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(1.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(3.0)), ("b", bool_val(false))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mc1: MultiConstraint = vec![
        { let mut c = Constraint::default(); c.insert("a".to_string(), num(3.0)); c },
        { let mut c = Constraint::default(); c.insert("a".to_string(), num(4.0)); c },
        { let mut c = Constraint::default(); c.insert("a".to_string(), num(5.0)); c },
    ];
    let mc2: MultiConstraint = vec![
        { let mut c = Constraint::default(); c.insert("b".to_string(), bool_val(true)); c },
    ];
    let req = FetchRequest { multi_constraints: vec![mc1, mc2], ..Default::default() };
    let spy = OverlaySpy::new(input.clone(), req);
    input.borrow_mut().set_output(Rc::clone(&spy) as _);

    source.borrow_mut().push(SourceChange::Add { row: make_row(&[("a", num(4.0)), ("b", bool_val(true))]) });

    assert_eq!(spy.borrow().fetches.len(), 1);
    let vals = fetch_vals(&spy.borrow().fetches[0], "a");
    assert_eq!(vals, vec![num(4.0)]);
}

#[test]
fn test_overlay_vs_mc_empty_entry_ignored() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(1.0)), ("b", bool_val(true))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mc: MultiConstraint = vec![];
    let req = FetchRequest { multi_constraints: vec![mc], ..Default::default() };
    let spy = OverlaySpy::new(input.clone(), req);
    input.borrow_mut().set_output(Rc::clone(&spy) as _);

    source.borrow_mut().push(SourceChange::Add { row: make_row(&[("a", num(4.0)), ("b", bool_val(true))]) });

    assert_eq!(spy.borrow().fetches.len(), 1);
    let vals = fetch_vals(&spy.borrow().fetches[0], "a");
    assert_eq!(vals, vec![num(1.0), num(4.0)]);
}

// ===========================================================================
// Constraint tests (c1-c4) — constraint + start + reverse combinations
// ===========================================================================

#[test]
fn test_constraint_c1() {
    let source = make_source("table", &["a"], &[
        ("a", ColumnType::Number { optional: false }),
        ("b", ColumnType::String { optional: false }),
    ]);
    add_row(&source, &[("a", num(1.0)), ("b", s("1000"))]);
    add_row(&source, &[("a", num(2.0)), ("b", s("3000"))]);
    add_row(&source, &[("a", num(3.0)), ("b", s("2000"))]);
    add_row(&source, &[("a", num(5.0)), ("b", s("1000"))]);
    add_row(&source, &[("a", num(6.0)), ("b", s("4000"))]);
    add_row(&source, &[("a", num(7.0)), ("b", s("0000"))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mut constraint = Constraint::default();
    constraint.insert("b".to_string(), s("1000"));
    let req = FetchRequest {
        constraint: Some(constraint),
        start: Some(Start { row: make_row(&[("a", num(3.0)), ("b", s("2000"))]), basis: Basis::At }),
        ..Default::default()
    };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].row.get("a"), Some(&num(5.0)));
}

#[test]
fn test_constraint_c1_reverse() {
    let source = make_source("table", &["a"], &[
        ("a", ColumnType::Number { optional: false }),
        ("b", ColumnType::String { optional: false }),
    ]);
    add_row(&source, &[("a", num(1.0)), ("b", s("1000"))]);
    add_row(&source, &[("a", num(2.0)), ("b", s("3000"))]);
    add_row(&source, &[("a", num(3.0)), ("b", s("2000"))]);
    add_row(&source, &[("a", num(5.0)), ("b", s("1000"))]);
    add_row(&source, &[("a", num(6.0)), ("b", s("4000"))]);
    add_row(&source, &[("a", num(7.0)), ("b", s("0000"))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mut constraint = Constraint::default();
    constraint.insert("b".to_string(), s("1000"));
    let req = FetchRequest {
        constraint: Some(constraint),
        start: Some(Start { row: make_row(&[("a", num(3.0)), ("b", s("2000"))]), basis: Basis::At }),
        reverse: true,
        ..Default::default()
    };
    // TS: constraint b=1000 → [1, 5], reverse at a=3 → retain r<=3 → [1], reverse → [1]
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].row.get("a"), Some(&num(1.0)));
}

#[test]
fn test_constraint_c2() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(2.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(3.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(5.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(6.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(7.0)), ("b", bool_val(false))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mut constraint = Constraint::default();
    constraint.insert("b".to_string(), bool_val(false));
    let req = FetchRequest {
        constraint: Some(constraint),
        start: Some(Start { row: make_row(&[("a", num(6.0)), ("b", bool_val(false))]), basis: Basis::At }),
        ..Default::default()
    };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].row.get("a"), Some(&num(6.0)));
    assert_eq!(nodes[1].row.get("a"), Some(&num(7.0)));
}

#[test]
fn test_constraint_c2_reverse() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(2.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(3.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(5.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(6.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(7.0)), ("b", bool_val(false))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mut constraint = Constraint::default();
    constraint.insert("b".to_string(), bool_val(false));
    let req = FetchRequest {
        constraint: Some(constraint),
        start: Some(Start { row: make_row(&[("a", num(6.0)), ("b", bool_val(false))]), basis: Basis::At }),
        reverse: true,
        ..Default::default()
    };
    // TS: constraint b=false → [2,3,6,7], reverse at a=6 → retain r<=6 → [2,3,6], reverse → [6,3,2]
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 3);
    assert_eq!(nodes[0].row.get("a"), Some(&num(6.0)));
    assert_eq!(nodes[1].row.get("a"), Some(&num(3.0)));
    assert_eq!(nodes[2].row.get("a"), Some(&num(2.0)));
}

#[test]
fn test_constraint_c3() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(2.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(3.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(5.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(6.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(7.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(8.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(9.0)), ("b", bool_val(false))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mut constraint = Constraint::default();
    constraint.insert("b".to_string(), bool_val(false));
    let req = FetchRequest {
        constraint: Some(constraint),
        start: Some(Start { row: make_row(&[("a", num(6.0)), ("b", bool_val(false))]), basis: Basis::After }),
        ..Default::default()
    };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].row.get("a"), Some(&num(7.0)));
    assert_eq!(nodes[1].row.get("a"), Some(&num(9.0)));
}

#[test]
fn test_constraint_c3_reverse() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(2.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(3.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(5.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(6.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(7.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(8.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(9.0)), ("b", bool_val(false))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mut constraint = Constraint::default();
    constraint.insert("b".to_string(), bool_val(false));
    let req = FetchRequest {
        constraint: Some(constraint),
        start: Some(Start { row: make_row(&[("a", num(6.0)), ("b", bool_val(false))]), basis: Basis::After }),
        reverse: true,
        ..Default::default()
    };
    // TS: constraint b=false → [2,3,6,7,9], reverse after a=6 → retain r<6 → [2,3], reverse → [3,2]
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].row.get("a"), Some(&num(3.0)));
    assert_eq!(nodes[1].row.get("a"), Some(&num(2.0)));
}

#[test]
fn test_constraint_c4() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(2.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(3.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(5.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(6.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(7.0)), ("b", bool_val(false))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mut constraint = Constraint::default();
    constraint.insert("b".to_string(), bool_val(false));
    let req = FetchRequest {
        constraint: Some(constraint),
        start: Some(Start { row: make_row(&[("a", num(6.0)), ("b", bool_val(false))]), basis: Basis::After }),
        ..Default::default()
    };
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].row.get("a"), Some(&num(7.0)));
}

#[test]
fn test_constraint_c4_reverse() {
    let source = make_source_ab();
    add_row(&source, &[("a", num(2.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(3.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(5.0)), ("b", bool_val(true))]);
    add_row(&source, &[("a", num(6.0)), ("b", bool_val(false))]);
    add_row(&source, &[("a", num(7.0)), ("b", bool_val(false))]);

    let input = source.borrow_mut().connect(None, None, None, None);
    let mut constraint = Constraint::default();
    constraint.insert("b".to_string(), bool_val(false));
    let req = FetchRequest {
        constraint: Some(constraint),
        start: Some(Start { row: make_row(&[("a", num(6.0)), ("b", bool_val(false))]), basis: Basis::After }),
        reverse: true,
        ..Default::default()
    };
    // TS: constraint b=false → [2,3,6,7], reverse after a=6 → retain r<6 → [2,3], reverse → [3,2]
    let nodes = fetch_all(&input, &req);
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].row.get("a"), Some(&num(3.0)));
    assert_eq!(nodes[1].row.get("a"), Some(&num(2.0)));
}

// ===========================================================================
// Overlay-vs-fetch-start — overlay during push with start constraint
// Tests c9-c16 (add), c23-c30 (remove) from TS source.test.ts
// Now matches TS btree-based reverse behavior (reverse from start row, going backwards).
// ===========================================================================

fn make_source_a() -> Rc<RefCell<MemorySource>> {
    make_source("table", &["a"], &[("a", ColumnType::Number { optional: false })])
}

fn overlay_vs_fetch_start(
    start_data: &[f64],
    start_val: f64,
    basis: Basis,
    reverse: bool,
    change: SourceChange,
) -> Vec<Vec<Value>> {
    let source = make_source_a();
    for &v in start_data {
        add_row(&source, &[("a", num(v))]);
    }
    let input = source.borrow_mut().connect(sort_by("a"), None, None, None);
    let req = FetchRequest {
        start: Some(Start { row: make_row(&[("a", num(start_val))]), basis }),
        reverse,
        ..Default::default()
    };
    let spy = OverlaySpy::new(input.clone(), req);
    input.borrow_mut().set_output(Rc::clone(&spy) as _);
    source.borrow_mut().push(change);
    spy.borrow()
        .fetches
        .iter()
        .map(|nodes| fetch_vals(nodes, "a"))
        .collect()
}

// c9: start at a=2, add a=1 → forward [2,4] (overlay 1 before start)
#[test]
fn test_overlay_vs_start_c9() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::At, false,
        SourceChange::Add { row: make_row(&[("a", num(1.0))]) });
    assert_eq!(fetches, vec![vec![num(2.0), num(4.0)]]);
}

// c9 reverse: TS [2,1] (reverse from 2, overlay 1 appended)
#[test]
fn test_overlay_vs_start_c9_reverse() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::At, true,
        SourceChange::Add { row: make_row(&[("a", num(1.0))]) });
    assert_eq!(fetches, vec![vec![num(2.0), num(1.0)]]);
}

// c10: start at a=2, add a=3 → [2,3,4]
#[test]
fn test_overlay_vs_start_c10() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::At, false,
        SourceChange::Add { row: make_row(&[("a", num(3.0))]) });
    assert_eq!(fetches, vec![vec![num(2.0), num(3.0), num(4.0)]]);
}

// c10 reverse: TS [2] (overlay 3 > 2, dropped by reversed start filter)
#[test]
fn test_overlay_vs_start_c10_reverse() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::At, true,
        SourceChange::Add { row: make_row(&[("a", num(3.0))]) });
    assert_eq!(fetches, vec![vec![num(2.0)]]);
}

// c11: start at a=2, add a=5 → [2,4,5]
#[test]
fn test_overlay_vs_start_c11() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::At, false,
        SourceChange::Add { row: make_row(&[("a", num(5.0))]) });
    assert_eq!(fetches, vec![vec![num(2.0), num(4.0), num(5.0)]]);
}

// c11 reverse: TS [2] (overlay 5 > 2, dropped)
#[test]
fn test_overlay_vs_start_c11_reverse() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::At, true,
        SourceChange::Add { row: make_row(&[("a", num(5.0))]) });
    assert_eq!(fetches, vec![vec![num(2.0)]]);
}

// c12: start after a=2, add a=1 → [4]
#[test]
fn test_overlay_vs_start_c12() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::After, false,
        SourceChange::Add { row: make_row(&[("a", num(1.0))]) });
    assert_eq!(fetches, vec![vec![num(4.0)]]);
}

// c12 reverse: TS [1] (reverse after 2 → [], overlay 1 < 2 passes, appended)
#[test]
fn test_overlay_vs_start_c12_reverse() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::After, true,
        SourceChange::Add { row: make_row(&[("a", num(1.0))]) });
    assert_eq!(fetches, vec![vec![num(1.0)]]);
}

// c13: start after a=2, add a=3 → [3,4]
#[test]
fn test_overlay_vs_start_c13() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::After, false,
        SourceChange::Add { row: make_row(&[("a", num(3.0))]) });
    assert_eq!(fetches, vec![vec![num(3.0), num(4.0)]]);
}

// c13 reverse: TS [] (overlay 3 >= 2, dropped by reversed after filter)
#[test]
fn test_overlay_vs_start_c13_reverse() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::After, true,
        SourceChange::Add { row: make_row(&[("a", num(3.0))]) });
    assert_eq!(fetches, vec![vec![]]);
}

// c14: start after a=2, add a=5 → [4,5]
#[test]
fn test_overlay_vs_start_c14() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::After, false,
        SourceChange::Add { row: make_row(&[("a", num(5.0))]) });
    assert_eq!(fetches, vec![vec![num(4.0), num(5.0)]]);
}

// c14 reverse: TS [] (overlay 5 >= 2, dropped)
#[test]
fn test_overlay_vs_start_c14_reverse() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::After, true,
        SourceChange::Add { row: make_row(&[("a", num(5.0))]) });
    assert_eq!(fetches, vec![vec![]]);
}

// c15: start after a=4, add a=3 → []
#[test]
fn test_overlay_vs_start_c15() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 4.0, Basis::After, false,
        SourceChange::Add { row: make_row(&[("a", num(3.0))]) });
    assert_eq!(fetches, vec![vec![]]);
}

// c15 reverse: TS [3,2] (reverse after 4 → [2], overlay 3 < 4 passes, spliced before 2)
#[test]
fn test_overlay_vs_start_c15_reverse() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 4.0, Basis::After, true,
        SourceChange::Add { row: make_row(&[("a", num(3.0))]) });
    assert_eq!(fetches, vec![vec![num(3.0), num(2.0)]]);
}

// c16: start after a=4, add a=5 → [5]
#[test]
fn test_overlay_vs_start_c16() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 4.0, Basis::After, false,
        SourceChange::Add { row: make_row(&[("a", num(5.0))]) });
    assert_eq!(fetches, vec![vec![num(5.0)]]);
}

// c16 reverse: TS [2] (overlay 5 >= 4, dropped)
#[test]
fn test_overlay_vs_start_c16_reverse() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 4.0, Basis::After, true,
        SourceChange::Add { row: make_row(&[("a", num(5.0))]) });
    assert_eq!(fetches, vec![vec![num(2.0)]]);
}

// c23: start at a=2, remove a=2 → [4]
#[test]
fn test_overlay_vs_start_c23() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::At, false,
        SourceChange::Remove { row: make_row(&[("a", num(2.0))]) });
    assert_eq!(fetches, vec![vec![num(4.0)]]);
}

// c23 reverse: TS [] (reverse at 2 → [2], remove 2 → [])
#[test]
fn test_overlay_vs_start_c23_reverse() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::At, true,
        SourceChange::Remove { row: make_row(&[("a", num(2.0))]) });
    assert_eq!(fetches, vec![vec![]]);
}

// c24: start at a=2, remove a=4 → [2]
#[test]
fn test_overlay_vs_start_c24() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::At, false,
        SourceChange::Remove { row: make_row(&[("a", num(4.0))]) });
    assert_eq!(fetches, vec![vec![num(2.0)]]);
}

// c24 reverse: Rust gives [2] (matches TS — only one row)
#[test]
fn test_overlay_vs_start_c24_reverse() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::At, true,
        SourceChange::Remove { row: make_row(&[("a", num(4.0))]) });
    assert_eq!(fetches, vec![vec![num(2.0)]]);
}

// c25: start at a=4, remove a=2 → [4]
#[test]
fn test_overlay_vs_start_c25() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 4.0, Basis::At, false,
        SourceChange::Remove { row: make_row(&[("a", num(2.0))]) });
    assert_eq!(fetches, vec![vec![num(4.0)]]);
}

// c25 reverse: Rust gives [4] (matches TS — only one row)
#[test]
fn test_overlay_vs_start_c25_reverse() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 4.0, Basis::At, true,
        SourceChange::Remove { row: make_row(&[("a", num(2.0))]) });
    assert_eq!(fetches, vec![vec![num(4.0)]]);
}

// c26: start at a=4, remove a=4 → []
#[test]
fn test_overlay_vs_start_c26() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 4.0, Basis::At, false,
        SourceChange::Remove { row: make_row(&[("a", num(4.0))]) });
    assert_eq!(fetches, vec![vec![]]);
}

// c26 reverse: TS [2] (reverse at 4 → [4,2], remove 4 → [2])
#[test]
fn test_overlay_vs_start_c26_reverse() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 4.0, Basis::At, true,
        SourceChange::Remove { row: make_row(&[("a", num(4.0))]) });
    assert_eq!(fetches, vec![vec![num(2.0)]]);
}

// c27: start after a=2, remove a=2 → [4]
#[test]
fn test_overlay_vs_start_c27() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::After, false,
        SourceChange::Remove { row: make_row(&[("a", num(2.0))]) });
    assert_eq!(fetches, vec![vec![num(4.0)]]);
}

// c27 reverse: TS [] (reverse after 2 → [], remove 2 >= 2 dropped)
#[test]
fn test_overlay_vs_start_c27_reverse() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::After, true,
        SourceChange::Remove { row: make_row(&[("a", num(2.0))]) });
    assert_eq!(fetches, vec![vec![]]);
}

// c28: start after a=2, remove a=4 → []
#[test]
fn test_overlay_vs_start_c28() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 2.0, Basis::After, false,
        SourceChange::Remove { row: make_row(&[("a", num(4.0))]) });
    assert_eq!(fetches, vec![vec![]]);
}

// c29: start after a=4, remove a=2 → []
#[test]
fn test_overlay_vs_start_c29() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 4.0, Basis::After, false,
        SourceChange::Remove { row: make_row(&[("a", num(2.0))]) });
    assert_eq!(fetches, vec![vec![]]);
}

// c29 reverse: Rust gives [] (matches TS)
#[test]
fn test_overlay_vs_start_c29_reverse() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 4.0, Basis::After, true,
        SourceChange::Remove { row: make_row(&[("a", num(2.0))]) });
    assert_eq!(fetches, vec![vec![]]);
}

// c30: start after a=4, remove a=4 → []
#[test]
fn test_overlay_vs_start_c30() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 4.0, Basis::After, false,
        SourceChange::Remove { row: make_row(&[("a", num(4.0))]) });
    assert_eq!(fetches, vec![vec![]]);
}

// c30 reverse: TS [2] (reverse after 4 → [2], remove 4 >= 4 dropped)
#[test]
fn test_overlay_vs_start_c30_reverse() {
    let fetches = overlay_vs_fetch_start(&[2.0, 4.0], 4.0, Basis::After, true,
        SourceChange::Remove { row: make_row(&[("a", num(4.0))]) });
    assert_eq!(fetches, vec![vec![num(2.0)]]);
}

// ===========================================================================
// Overlay-vs-filter-predicate — overlay during push with filter_predicate
// Tests c5, c6 from TS per-output-sorts (OR filter — can't use constraint)
// ===========================================================================

fn overlay_vs_filter(
    start_data: &[(f64, bool)],
    predicate: Arc<dyn Fn(&Row) -> bool>,
    change: SourceChange,
) -> Vec<Vec<Value>> {
    let source = make_source_ab();
    for &(a, b) in start_data {
        add_row(&source, &[("a", num(a)), ("b", bool_val(b))]);
    }
    let input = source.borrow_mut().connect(sort_by("a"), None, Some(predicate), None);
    let spy = OverlaySpy::new(input.clone(), FetchRequest::default());
    input.borrow_mut().set_output(Rc::clone(&spy) as _);
    source.borrow_mut().push(change);
    spy.borrow()
        .fetches
        .iter()
        .map(|nodes| fetch_vals(nodes, "a"))
        .collect()
}

// c5: filter (a=4 OR b=false), add {a:1, b:false} → [1, 2, 4]
#[test]
fn test_overlay_vs_filter_c5() {
    let pred: Arc<dyn Fn(&Row) -> bool> = Arc::new(|row| {
        let a = row.get("a");
        let b = row.get("b");
        a == Some(&num(4.0)) || b == Some(&bool_val(false))
    });
    let fetches = overlay_vs_filter(
        &[(2.0, false), (4.0, true)],
        pred,
        SourceChange::Add { row: make_row(&[("a", num(1.0)), ("b", bool_val(false))]) },
    );
    assert_eq!(fetches, vec![vec![num(1.0), num(2.0), num(4.0)]]);
}

// c6: filter (a=4 OR b=false), add {a:1, b:false} → [1, 2, 4] (same as c5)
#[test]
fn test_overlay_vs_filter_c6() {
    let pred: Arc<dyn Fn(&Row) -> bool> = Arc::new(|row| {
        let a = row.get("a");
        let b = row.get("b");
        a == Some(&num(4.0)) || b == Some(&bool_val(false))
    });
    let fetches = overlay_vs_filter(
        &[(2.0, false), (4.0, true)],
        pred,
        SourceChange::Add { row: make_row(&[("a", num(1.0)), ("b", bool_val(false))]) },
    );
    assert_eq!(fetches, vec![vec![num(1.0), num(2.0), num(4.0)]]);
}
