//! Tests for UnionFanIn — port of TS `union-fan-in.test.ts` (v1.7.0).
//! Tests schema creation, fetch merge (sorted + dedup), and push propagation.

use std::cell::RefCell;
use std::rc::Rc;
use std::collections::HashMap;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::data::{Node, Row, SortOrder, Value};
use rust_ivm::ivm::operator::{
    FetchRequest, Input, InputBase, OutputHandle, Shared,
};
use rust_ivm::ivm::schema::{ColumnType, SourceSchema, System};
use rust_ivm::ivm::source::{MemorySource};
use rust_ivm::ivm::stream::{from_vec, NodeStream};
use rust_ivm::ivm::union_fan_in::UnionFanIn;

fn str_val(s: &str) -> Value {
    Value::Str(Arc::from(s))
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

fn pk_sort(pk: &[&str]) -> SortOrder {
    Arc::new(pk.iter().map(|s| [s.to_string(), "asc".to_string()]).collect::<Vec<_>>())
}

fn make_schema(name: &str, pk: &[&str], columns: &[(&str, ColumnType)]) -> SourceSchema {
    let cols: HashMap<String, ColumnType> = columns
        .iter()
        .map(|(n, t)| (n.to_string(), t.clone()))
        .collect();
    let pk_cols: Vec<String> = pk.iter().map(|s| s.to_string()).collect();
    let sort = pk_sort(pk);
    let comparator = rust_ivm::ivm::data::make_comparator(sort.clone(), false);
    SourceSchema {
        table_name: name.to_string(),
        columns: cols,
        primary_key: pk_cols,
        relationships: HashMap::new(),
        relationship_order: Vec::new(),
        compare_rows: comparator,
        is_hidden: false,
        sort: Some(sort),
        system: System::Client,
    }
}

fn connect_sorted(src: &Rc<RefCell<MemorySource>>, pk: &[&str]) -> Shared<dyn Input> {
    src.borrow_mut().connect(Some(pk_sort(pk)), None, None, None)
}

fn row_id(node: &Node) -> Value {
    node.row.get("id").cloned().unwrap_or(Value::Null)
}

// --- Mock Input for mismatch/conflict tests ---

struct MockInput {
    schema: SourceSchema,
    data: Vec<Node>,
    destroyed: Rc<RefCell<bool>>,
}

impl InputBase for MockInput {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }
    fn destroy(&mut self) {
        *self.destroyed.borrow_mut() = true;
    }
}

impl Input for MockInput {
    fn set_output(&self, _output: OutputHandle) {}
    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        let mut data = self.data.clone();
        if req.reverse {
            data.reverse();
        }
        from_vec(data)
    }
}

fn mock_input(schema: SourceSchema, data: Vec<Node>) -> (Shared<dyn Input>, Rc<RefCell<bool>>) {
    let destroyed = Rc::new(RefCell::new(false));
    let mock = MockInput {
        schema,
        data,
        destroyed: destroyed.clone(),
    };
    (Rc::new(RefCell::new(mock)), destroyed)
}

fn make_node(id: &str) -> Node {
    let mut row = FxHashMap::default();
    row.insert("id".to_string(), str_val(id));
    Node::new(Arc::new(row))
}

// === Schema creation tests ===

#[test]
fn test_fetch_empty_inputs() {
    let schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    let ufi = UnionFanIn::new(schema);
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(ufi.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 0);
}

#[test]
fn test_schema_preserved() {
    let schema = make_schema("custom", &["id", "name"], &[
        ("id", ColumnType::String { optional: false }),
        ("name", ColumnType::String { optional: false }),
    ]);
    let ufi = UnionFanIn::new(schema);
    let result = ufi.borrow().get_schema();
    assert_eq!(result.table_name, "custom");
    assert_eq!(result.primary_key, vec!["id", "name"]);
}

#[test]
fn test_schema_preserves_all_properties() {
    let sort = pk_sort(&["name"]);
    let comparator = rust_ivm::ivm::data::make_comparator(sort.clone(), false);
    let schema = SourceSchema {
        table_name: "custom".to_string(),
        columns: HashMap::from([("col1".to_string(), ColumnType::String { optional: false })]),
        primary_key: vec!["id".to_string(), "name".to_string()],
        relationships: HashMap::new(),
        relationship_order: Vec::new(),
        compare_rows: comparator,
        is_hidden: true,
        sort: Some(sort),
        system: System::Client,
    };
    let ufi = UnionFanIn::new(schema);
    let result = ufi.borrow().get_schema();
    assert_eq!(result.table_name, "custom");
    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.primary_key, vec!["id", "name"]);
    assert!(result.is_hidden);
    assert_eq!(result.system, System::Client);
}

// === Fetch tests ===

#[test]
fn test_fetch_single_input() {
    let schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    let src = make_source("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    add_row(&src, &[("id", str_val("a"))]);
    add_row(&src, &[("id", str_val("b"))]);
    let input = connect_sorted(&src, &["id"]);

    let ufi = UnionFanIn::new(schema);
    ufi.borrow_mut().add_input(input);

    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(ufi.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 2);
    assert_eq!(row_id(&nodes[0]), str_val("a"));
    assert_eq!(row_id(&nodes[1]), str_val("b"));
}

#[test]
fn test_fetch_merge_two_inputs_sorted() {
    let schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);

    let src1 = make_source("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    add_row(&src1, &[("id", str_val("a"))]);
    add_row(&src1, &[("id", str_val("c"))]);
    let input1 = connect_sorted(&src1, &["id"]);

    let src2 = make_source("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    add_row(&src2, &[("id", str_val("b"))]);
    add_row(&src2, &[("id", str_val("d"))]);
    let input2 = connect_sorted(&src2, &["id"]);

    let ufi = UnionFanIn::new(schema);
    ufi.borrow_mut().add_input(input1);
    ufi.borrow_mut().add_input(input2);

    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(ufi.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 4);
    assert_eq!(row_id(&nodes[0]), str_val("a"));
    assert_eq!(row_id(&nodes[1]), str_val("b"));
    assert_eq!(row_id(&nodes[2]), str_val("c"));
    assert_eq!(row_id(&nodes[3]), str_val("d"));
}

#[test]
fn test_fetch_merge_dedup() {
    let schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);

    let src1 = make_source("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    add_row(&src1, &[("id", str_val("a"))]);
    add_row(&src1, &[("id", str_val("b"))]);
    let input1 = connect_sorted(&src1, &["id"]);

    let src2 = make_source("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    add_row(&src2, &[("id", str_val("a"))]);
    add_row(&src2, &[("id", str_val("c"))]);
    let input2 = connect_sorted(&src2, &["id"]);

    let ufi = UnionFanIn::new(schema);
    ufi.borrow_mut().add_input(input1);
    ufi.borrow_mut().add_input(input2);

    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(ufi.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 3, "Duplicate 'a' should be deduplicated");
    assert_eq!(row_id(&nodes[0]), str_val("a"));
    assert_eq!(row_id(&nodes[1]), str_val("b"));
    assert_eq!(row_id(&nodes[2]), str_val("c"));
}

#[test]
fn test_fetch_merge_tiebreak_lower_index_wins() {
    // Regression for the flipped-path relationship leak (fuzzer seed 1900140):
    // when the same row appears in two UnionFanIn branches, the LOWER-index
    // branch's node must win. This matches TS `mergeFetches` (union-fan-in.ts),
    // whose linear reduce keeps the existing lower-index acc on equal rows
    // (`comparator(c, acc[0]) < 0` is strict, so equal preserves acc). Without
    // this, Rust's BinaryHeap tie-breaking is undefined, so a row matching both
    // the non-flipped and flipped OR branches may yield the flipped branch's
    // node first — leaking the flipped subquery's relationship into the output
    // where TS suppresses it (TS yields the non-flipped, relationship-less
    // node first and drops the flipped duplicate).
    let schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);

    // input1 (index 0): node 'a' with NO relationship (the non-flipped branch).
    let node_a_plain = make_node("a");
    let input1 = mock_input(schema.clone(), vec![node_a_plain]);

    // input2 (index 1): node 'a' WITH a subquery relationship (the flipped
    // branch — FlippedJoin attaches the matched child rows).
    let child = make_node("child");
    let rel: rust_ivm::ivm::stream::RelStream =
        Rc::new(move || from_vec(vec![child.clone()]));
    let node_a_with_rel = make_node("a").set_relationship("zsubq_t2_0", rel);
    let input2 = mock_input(schema.clone(), vec![node_a_with_rel]);

    let ufi = UnionFanIn::new(schema);
    ufi.borrow_mut().add_input(input1.0);
    ufi.borrow_mut().add_input(input2.0);

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(ufi.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(nodes.len(), 1, "duplicate 'a' should be deduplicated");
    assert_eq!(row_id(&nodes[0]), str_val("a"));
    assert!(
        nodes[0].relationships.is_empty(),
        "lower-index branch (no relationship) must win on ties; got relationships: {:?}",
        nodes[0].relationships.keys().collect::<Vec<_>>(),
    );
}

#[test]
fn test_fetch_reverse() {
    let schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);

    let src1 = make_source("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    add_row(&src1, &[("id", str_val("a"))]);
    add_row(&src1, &[("id", str_val("c"))]);
    let input1 = connect_sorted(&src1, &["id"]);

    let src2 = make_source("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    add_row(&src2, &[("id", str_val("b"))]);
    add_row(&src2, &[("id", str_val("d"))]);
    let input2 = connect_sorted(&src2, &["id"]);

    let ufi = UnionFanIn::new(schema);
    ufi.borrow_mut().add_input(input1);
    ufi.borrow_mut().add_input(input2);

    let req = FetchRequest {
        reverse: true,
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(ufi.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 4);
    assert_eq!(row_id(&nodes[0]), str_val("d"));
    assert_eq!(row_id(&nodes[1]), str_val("c"));
    assert_eq!(row_id(&nodes[2]), str_val("b"));
    assert_eq!(row_id(&nodes[3]), str_val("a"));
}

#[test]
fn test_fetch_reverse_with_overlap_dedup() {
    // Both branches contain id=2 and id=4. Reverse merge should dedup adjacent duplicates.
    let schema = make_schema("test", &["id"], &[("id", ColumnType::Number { optional: false })]);

    let src1 = make_source("test", &["id"], &[("id", ColumnType::Number { optional: false })]);
    add_row(&src1, &[("id", Value::F64(1.0))]);
    add_row(&src1, &[("id", Value::F64(2.0))]);
    add_row(&src1, &[("id", Value::F64(4.0))]);
    let input1 = connect_sorted(&src1, &["id"]);

    let src2 = make_source("test", &["id"], &[("id", ColumnType::Number { optional: false })]);
    add_row(&src2, &[("id", Value::F64(2.0))]);
    add_row(&src2, &[("id", Value::F64(3.0))]);
    add_row(&src2, &[("id", Value::F64(4.0))]);
    let input2 = connect_sorted(&src2, &["id"]);

    let ufi = UnionFanIn::new(schema);
    ufi.borrow_mut().add_input(input1);
    ufi.borrow_mut().add_input(input2);

    let req = FetchRequest {
        reverse: true,
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(ufi.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 4);
    let ids: Vec<Value> = nodes.iter().map(|n| n.row.get("id").cloned().unwrap_or(Value::Null)).collect();
    assert_eq!(ids, vec![Value::F64(4.0), Value::F64(3.0), Value::F64(2.0), Value::F64(1.0)]);
}

#[test]
fn test_fetch_with_constraint() {
    let schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);

    let src1 = make_source("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    add_row(&src1, &[("id", str_val("a"))]);
    add_row(&src1, &[("id", str_val("b"))]);
    let input1 = connect_sorted(&src1, &["id"]);

    let src2 = make_source("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    add_row(&src2, &[("id", str_val("b"))]);
    add_row(&src2, &[("id", str_val("c"))]);
    let input2 = connect_sorted(&src2, &["id"]);

    let ufi = UnionFanIn::new(schema);
    ufi.borrow_mut().add_input(input1);
    ufi.borrow_mut().add_input(input2);

    let mut constraint = rust_ivm::ivm::constraint::Constraint::default();
    constraint.insert("id".to_string(), str_val("b"));
    let req = FetchRequest {
        constraint: Some(constraint),
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(ufi.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 1, "Constraint should filter and dedup");
    assert_eq!(row_id(&nodes[0]), str_val("b"));
}

// === Schema mismatch validation tests ===

#[test]
#[should_panic(expected = "Table name mismatch")]
fn test_mismatch_table_name() {
    let schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    let mismatched = make_schema("different", &["id"], &[("id", ColumnType::String { optional: false })]);
    let (input, _) = mock_input(mismatched, vec![]);
    let ufi = UnionFanIn::new(schema);
    ufi.borrow_mut().add_input(input);
}

#[test]
#[should_panic(expected = "Primary key mismatch")]
fn test_mismatch_primary_key() {
    let schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    let mismatched = make_schema("test", &["id", "name"], &[
        ("id", ColumnType::String { optional: false }),
        ("name", ColumnType::String { optional: false }),
    ]);
    let (input, _) = mock_input(mismatched, vec![]);
    let ufi = UnionFanIn::new(schema);
    ufi.borrow_mut().add_input(input);
}

#[test]
#[should_panic(expected = "System mismatch")]
fn test_mismatch_system() {
    let schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    let mut mismatched = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    mismatched.system = System::Test;
    let (input, _) = mock_input(mismatched, vec![]);
    let ufi = UnionFanIn::new(schema);
    ufi.borrow_mut().add_input(input);
}

#[test]
#[should_panic(expected = "Sort mismatch")]
fn test_mismatch_sort() {
    let schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    let mut mismatched = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    mismatched.sort = Some(Arc::new(vec![["name".to_string(), "asc".to_string()]]));
    let (input, _) = mock_input(mismatched, vec![]);
    let ufi = UnionFanIn::new(schema);
    ufi.borrow_mut().add_input(input);
}

// === Relationship merging tests ===

#[test]
fn test_relationship_merging_from_inputs() {
    let schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);

    let mut input1_schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    let child_schema = make_schema("child", &["id"], &[("id", ColumnType::String { optional: false })]);
    input1_schema.relationships.insert("rel1".to_string(), child_schema.clone());
    input1_schema.relationship_order.push("rel1".to_string());

    let mut input2_schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    input2_schema.relationships.insert("rel2".to_string(), child_schema);
    input2_schema.relationship_order.push("rel2".to_string());

    let (input1, _) = mock_input(input1_schema, vec![]);
    let (input2, _) = mock_input(input2_schema, vec![]);

    let ufi = UnionFanIn::new(schema);
    ufi.borrow_mut().add_input(input1);
    ufi.borrow_mut().add_input(input2);

    let result = ufi.borrow().get_schema();
    assert!(result.relationships.contains_key("rel1"));
    assert!(result.relationships.contains_key("rel2"));
}

#[test]
#[should_panic(expected = "exists in multiple upstream inputs")]
fn test_relationship_conflict() {
    let schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);

    let child_schema = make_schema("child", &["id"], &[("id", ColumnType::String { optional: false })]);

    let mut input1_schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    input1_schema.relationships.insert("sharedRel".to_string(), child_schema.clone());
    input1_schema.relationship_order.push("sharedRel".to_string());

    let mut input2_schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    input2_schema.relationships.insert("sharedRel".to_string(), child_schema);
    input2_schema.relationship_order.push("sharedRel".to_string());

    let (input1, _) = mock_input(input1_schema, vec![]);
    let (input2, _) = mock_input(input2_schema, vec![]);

    let ufi = UnionFanIn::new(schema);
    ufi.borrow_mut().add_input(input1);
    ufi.borrow_mut().add_input(input2);
}

// === Destroy tests ===

#[test]
fn test_destroy_destroys_all_inputs() {
    let schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    let (input1, destroyed1) = mock_input(schema.clone(), vec![]);
    let (input2, destroyed2) = mock_input(schema, vec![]);

    let ufi = UnionFanIn::new(make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]));
    ufi.borrow_mut().add_input(input1);
    ufi.borrow_mut().add_input(input2);

    ufi.borrow_mut().destroy();

    assert!(*destroyed1.borrow(), "input1 should be destroyed");
    assert!(*destroyed2.borrow(), "input2 should be destroyed");
}

#[test]
fn test_destroy_empty_inputs() {
    let schema = make_schema("test", &["id"], &[("id", ColumnType::String { optional: false })]);
    let ufi = UnionFanIn::new(schema);
    ufi.borrow_mut().destroy();
}
