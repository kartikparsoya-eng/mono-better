//! Tests for MemorySource and merge_sorted_streams.
//! Port of TS `memory-source.test.ts` and `source.test.ts` (v1.7.0).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::constraint::{Constraint, MultiConstraint};
use rust_ivm::ivm::data::{Node, Row, Value};
use rust_ivm::ivm::memory_source::{MemorySource, merge_sorted_streams};
use rust_ivm::ivm::operator::{Basis, FetchRequest, Start};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::SourceChange;
use rust_ivm::ivm::stream::{NodeStream, from_vec};

fn make_row(pairs: &[(&str, Value)]) -> Row {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Arc::new(map)
}

fn make_source(
    name: &str,
    pk: &[&str],
    columns: &[(&str, ColumnType)],
) -> Rc<RefCell<MemorySource>> {
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

fn str_val(s: &str) -> Value {
    Value::Str(Arc::from(s))
}

fn str_val_owned(s: String) -> Value {
    Value::Str(Arc::from(s.as_str()))
}

#[test]
fn test_merge_sorted_streams_empty() {
    let streams: Vec<NodeStream> = vec![];
    let result: Vec<Node> = rust_ivm::ivm::stream::skip_yields(merge_sorted_streams(
        streams,
        Rc::new(
            |_: &rust_ivm::ivm::data::Node, _: &rust_ivm::ivm::data::Node| {
                std::cmp::Ordering::Equal
            },
        ),
    ))
    .collect();
    assert!(result.is_empty());
}

#[test]
fn test_merge_sorted_streams_single() {
    let nodes = vec![
        Node::new(make_row(&[("id", Value::F64(1.0))])),
        Node::new(make_row(&[("id", Value::F64(2.0))])),
        Node::new(make_row(&[("id", Value::F64(3.0))])),
    ];
    let stream = from_vec(nodes.clone());
    let result: Vec<Node> = rust_ivm::ivm::stream::skip_yields(merge_sorted_streams(
        vec![stream],
        Rc::new(
            |a: &rust_ivm::ivm::data::Node, b: &rust_ivm::ivm::data::Node| {
                let av = a
                    .row
                    .get("id")
                    .and_then(|v| match v {
                        Value::F64(n) => Some(*n),
                        _ => None,
                    })
                    .unwrap_or(0.0);
                let bv = b
                    .row
                    .get("id")
                    .and_then(|v| match v {
                        Value::F64(n) => Some(*n),
                        _ => None,
                    })
                    .unwrap_or(0.0);
                av.partial_cmp(&bv).unwrap()
            },
        ),
    ))
    .collect();
    assert_eq!(result.len(), 3);
}

#[test]
#[allow(clippy::needless_range_loop)]
fn test_merge_sorted_streams_two_streams() {
    let compare = |a: &Node, b: &Node| {
        let av = a
            .row
            .get("id")
            .and_then(|v| match v {
                Value::F64(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0.0);
        let bv = b
            .row
            .get("id")
            .and_then(|v| match v {
                Value::F64(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0.0);
        av.partial_cmp(&bv).unwrap()
    };

    let stream1 = from_vec(vec![
        Node::new(make_row(&[("id", Value::F64(1.0))])),
        Node::new(make_row(&[("id", Value::F64(3.0))])),
        Node::new(make_row(&[("id", Value::F64(5.0))])),
    ]);
    let stream2 = from_vec(vec![
        Node::new(make_row(&[("id", Value::F64(2.0))])),
        Node::new(make_row(&[("id", Value::F64(4.0))])),
        Node::new(make_row(&[("id", Value::F64(6.0))])),
    ]);

    let result: Vec<Node> = rust_ivm::ivm::stream::skip_yields(merge_sorted_streams(
        vec![stream1, stream2],
        Rc::new(compare),
    ))
    .collect();
    assert_eq!(result.len(), 6);
    for i in 0..result.len() {
        let id = result[i].row.get("id").cloned().unwrap_or(Value::Null);
        assert_eq!(id, Value::F64((i + 1) as f64), "Should be merged in order");
    }
}

#[test]
#[allow(clippy::needless_range_loop)]
fn test_merge_sorted_streams_unequal_length() {
    let compare = |a: &Node, b: &Node| {
        let av = a
            .row
            .get("id")
            .and_then(|v| match v {
                Value::F64(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0.0);
        let bv = b
            .row
            .get("id")
            .and_then(|v| match v {
                Value::F64(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0.0);
        av.partial_cmp(&bv).unwrap()
    };

    let stream1 = from_vec(vec![
        Node::new(make_row(&[("id", Value::F64(1.0))])),
        Node::new(make_row(&[("id", Value::F64(5.0))])),
    ]);
    let stream2 = from_vec(vec![
        Node::new(make_row(&[("id", Value::F64(2.0))])),
        Node::new(make_row(&[("id", Value::F64(3.0))])),
        Node::new(make_row(&[("id", Value::F64(4.0))])),
        Node::new(make_row(&[("id", Value::F64(6.0))])),
    ]);

    let result: Vec<Node> = rust_ivm::ivm::stream::skip_yields(merge_sorted_streams(
        vec![stream1, stream2],
        Rc::new(compare),
    ))
    .collect();
    assert_eq!(result.len(), 6);
    for i in 0..result.len() {
        let id = result[i].row.get("id").cloned().unwrap_or(Value::Null);
        assert_eq!(
            id,
            Value::F64((i + 1) as f64),
            "Should be merged in order despite unequal stream lengths"
        );
    }
}

#[test]
fn test_merge_sorted_streams_one_empty() {
    let compare = |a: &Node, b: &Node| {
        let av = a
            .row
            .get("id")
            .and_then(|v| match v {
                Value::F64(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0.0);
        let bv = b
            .row
            .get("id")
            .and_then(|v| match v {
                Value::F64(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0.0);
        av.partial_cmp(&bv).unwrap()
    };

    let stream1 = from_vec(vec![]);
    let stream2 = from_vec(vec![
        Node::new(make_row(&[("id", Value::F64(1.0))])),
        Node::new(make_row(&[("id", Value::F64(2.0))])),
    ]);

    let result: Vec<Node> = rust_ivm::ivm::stream::skip_yields(merge_sorted_streams(
        vec![stream1, stream2],
        Rc::new(compare),
    ))
    .collect();
    assert_eq!(result.len(), 2);
}

#[allow(clippy::needless_range_loop)]
#[test]
fn test_merge_sorted_streams_three_streams() {
    let compare = |a: &Node, b: &Node| {
        let av = a
            .row
            .get("id")
            .and_then(|v| match v {
                Value::F64(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0.0);
        let bv = b
            .row
            .get("id")
            .and_then(|v| match v {
                Value::F64(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0.0);
        av.partial_cmp(&bv).unwrap()
    };

    let stream1 = from_vec(vec![
        Node::new(make_row(&[("id", Value::F64(1.0))])),
        Node::new(make_row(&[("id", Value::F64(4.0))])),
    ]);
    let stream2 = from_vec(vec![
        Node::new(make_row(&[("id", Value::F64(2.0))])),
        Node::new(make_row(&[("id", Value::F64(5.0))])),
    ]);
    let stream3 = from_vec(vec![
        Node::new(make_row(&[("id", Value::F64(3.0))])),
        Node::new(make_row(&[("id", Value::F64(6.0))])),
    ]);

    let result: Vec<Node> = rust_ivm::ivm::stream::skip_yields(merge_sorted_streams(
        vec![stream1, stream2, stream3],
        Rc::new(compare),
    ))
    .collect();
    assert_eq!(result.len(), 6);
    for i in 0..result.len() {
        let id = result[i].row.get("id").cloned().unwrap_or(Value::Null);
        assert_eq!(
            id,
            Value::F64((i + 1) as f64),
            "Three streams should merge in order"
        );
    }
}

#[test]
fn test_memory_source_fetch_all() {
    let source = make_source(
        "users",
        &["id"],
        &[
            ("id", ColumnType::Number { optional: false }),
            ("name", ColumnType::String { optional: false }),
        ],
    );
    for i in 1..=5 {
        add_row(
            &source,
            &[
                ("id", Value::F64(i as f64)),
                ("name", str_val_owned(format!("user{}", i))),
            ],
        );
    }

    let input = source.borrow_mut().connect(None, None, None, None, None);
    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(input.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(nodes.len(), 5);
}

#[test]
fn test_memory_source_fetch_with_constraint() {
    let source = make_source(
        "users",
        &["id"],
        &[
            ("id", ColumnType::Number { optional: false }),
            ("name", ColumnType::String { optional: false }),
        ],
    );
    for i in 1..=5 {
        add_row(
            &source,
            &[
                ("id", Value::F64(i as f64)),
                ("name", str_val_owned(format!("user{}", i))),
            ],
        );
    }

    let input = source.borrow_mut().connect(None, None, None, None, None);
    let mut constraint = Constraint::default();
    constraint.insert("id".to_string(), Value::F64(3.0));
    let req = FetchRequest {
        constraint: Some(constraint),
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(input.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].row.get("id").cloned().unwrap_or(Value::Null),
        Value::F64(3.0)
    );
}

#[test]
fn test_memory_source_fetch_with_multi_constraints() {
    let source = make_source(
        "users",
        &["id"],
        &[("id", ColumnType::Number { optional: false })],
    );
    for i in 1..=10 {
        add_row(&source, &[("id", Value::F64(i as f64))]);
    }

    let input = source.borrow_mut().connect(None, None, None, None, None);

    let mut mc1 = Constraint::default();
    mc1.insert("id".to_string(), Value::F64(2.0));
    let mut mc2 = Constraint::default();
    mc2.insert("id".to_string(), Value::F64(5.0));
    let mut mc3 = Constraint::default();
    mc3.insert("id".to_string(), Value::F64(8.0));

    let mc: MultiConstraint = vec![mc1, mc2, mc3];

    let req = FetchRequest {
        multi_constraints: vec![mc],
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(input.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 3, "Should fetch 3 rows matching the IN clause");
}

#[test]
fn test_memory_source_push_add() {
    let source = make_source(
        "users",
        &["id"],
        &[("id", ColumnType::Number { optional: false })],
    );
    add_row(&source, &[("id", Value::F64(1.0))]);

    let input = source.borrow_mut().connect(None, None, None, None, None);

    source.borrow_mut().push(SourceChange::Add {
        row: make_row(&[("id", Value::F64(2.0))]),
    });

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(input.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(nodes.len(), 2, "Should have 2 rows after push add");
}

#[test]
fn test_memory_source_push_remove() {
    let source = make_source(
        "users",
        &["id"],
        &[("id", ColumnType::Number { optional: false })],
    );
    add_row(&source, &[("id", Value::F64(1.0))]);
    add_row(&source, &[("id", Value::F64(2.0))]);

    let input = source.borrow_mut().connect(None, None, None, None, None);

    source.borrow_mut().push(SourceChange::Remove {
        row: make_row(&[("id", Value::F64(1.0))]),
    });

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(input.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(nodes.len(), 1, "Should have 1 row after push remove");
    assert_eq!(
        nodes[0].row.get("id").cloned().unwrap_or(Value::Null),
        Value::F64(2.0)
    );
}

#[test]
fn test_memory_source_push_edit() {
    let source = make_source(
        "users",
        &["id"],
        &[
            ("id", ColumnType::Number { optional: false }),
            ("name", ColumnType::String { optional: false }),
        ],
    );
    add_row(
        &source,
        &[("id", Value::F64(1.0)), ("name", str_val("old"))],
    );

    let input = source.borrow_mut().connect(None, None, None, None, None);

    source.borrow_mut().push(SourceChange::Edit {
        old_row: make_row(&[("id", Value::F64(1.0)), ("name", str_val("old"))]),
        row: make_row(&[("id", Value::F64(1.0)), ("name", str_val("new"))]),
    });

    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(input.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].row.get("name").cloned().unwrap_or(Value::Null),
        str_val("new")
    );
}

// === Additional merge_sorted_streams tests ===

fn id_compare(a: &Node, b: &Node) -> std::cmp::Ordering {
    let av = a
        .row
        .get("id")
        .and_then(|v| match v {
            Value::F64(n) => Some(*n),
            _ => None,
        })
        .unwrap_or(0.0);
    let bv = b
        .row
        .get("id")
        .and_then(|v| match v {
            Value::F64(n) => Some(*n),
            _ => None,
        })
        .unwrap_or(0.0);
    av.partial_cmp(&bv).unwrap()
}

fn id_val(n: f64) -> Node {
    Node::new(make_row(&[("id", Value::F64(n))]))
}

#[test]
fn test_merge_sorted_streams_equal_keys() {
    let a = from_vec(vec![id_val(1.0), id_val(2.0)]);
    let b = from_vec(vec![id_val(1.0), id_val(2.0)]);
    let c = from_vec(vec![id_val(2.0), id_val(3.0)]);
    let result: Vec<Node> = rust_ivm::ivm::stream::skip_yields(merge_sorted_streams(
        vec![a, b, c],
        Rc::new(id_compare),
    ))
    .collect();
    let ids: Vec<f64> = result
        .iter()
        .map(|n| {
            n.row
                .get("id")
                .and_then(|v| match v {
                    Value::F64(n) => Some(*n),
                    _ => None,
                })
                .unwrap_or(0.0)
        })
        .collect();
    assert_eq!(ids, vec![1.0, 1.0, 2.0, 2.0, 2.0, 3.0]);
}

#[test]
fn test_merge_sorted_streams_reverse() {
    let a = from_vec(vec![id_val(5.0), id_val(3.0), id_val(1.0)]);
    let b = from_vec(vec![id_val(6.0), id_val(4.0), id_val(2.0)]);
    let reverse = |x: &Node, y: &Node| id_compare(y, x);
    let result: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(merge_sorted_streams(vec![a, b], Rc::new(reverse)))
            .collect();
    let ids: Vec<f64> = result
        .iter()
        .map(|n| {
            n.row
                .get("id")
                .and_then(|v| match v {
                    Value::F64(n) => Some(*n),
                    _ => None,
                })
                .unwrap_or(0.0)
        })
        .collect();
    assert_eq!(ids, vec![6.0, 5.0, 4.0, 3.0, 2.0, 1.0]);
}

#[test]
fn test_merge_sorted_streams_global_sort_invariant() {
    let data = [
        3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0, 5.0, 3.0, 5.0, 8.0, 9.0, 7.0, 9.0, 3.0,
    ];
    let mut buckets: Vec<Vec<Node>> = vec![vec![], vec![], vec![], vec![]];
    let num_buckets = buckets.len();
    for (i, &v) in data.iter().enumerate() {
        buckets[i % num_buckets].push(id_val(v));
    }
    let mut streams: Vec<NodeStream> = Vec::new();
    for bucket in buckets.iter_mut() {
        bucket.sort_by(id_compare);
        streams.push(from_vec(bucket.clone()));
    }
    let result: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(merge_sorted_streams(streams, Rc::new(id_compare)))
            .collect();
    assert_eq!(result.len(), data.len());
    for i in 1..result.len() {
        let prev = result[i - 1]
            .row
            .get("id")
            .and_then(|v| match v {
                Value::F64(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0.0);
        let curr = result[i]
            .row
            .get("id")
            .and_then(|v| match v {
                Value::F64(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0.0);
        assert!(
            curr >= prev,
            "Global sort invariant violated at index {}: {} < {}",
            i,
            curr,
            prev
        );
    }
}

// === Additional MemorySource fetch tests ===

#[test]
#[allow(clippy::needless_range_loop)]
fn test_memory_source_fetch_reverse() {
    let source = make_source(
        "users",
        &["id"],
        &[("id", ColumnType::Number { optional: false })],
    );
    for i in 1..=5 {
        add_row(&source, &[("id", Value::F64(i as f64))]);
    }
    let input = source.borrow_mut().connect(None, None, None, None, None);
    let req = FetchRequest {
        reverse: true,
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(input.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 5);
    for i in 0..5 {
        let id = nodes[i].row.get("id").cloned().unwrap_or(Value::Null);
        assert_eq!(id, Value::F64((5 - i) as f64));
    }
}

#[test]
fn test_memory_source_fetch_with_start_at() {
    let source = make_source(
        "users",
        &["id"],
        &[("id", ColumnType::Number { optional: false })],
    );
    for i in 1..=5 {
        add_row(&source, &[("id", Value::F64(i as f64))]);
    }
    let input = source.borrow_mut().connect(None, None, None, None, None);
    let req = FetchRequest {
        start: Some(Start {
            row: make_row(&[("id", Value::F64(3.0))]),
            basis: Basis::At,
        }),
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(input.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 3);
    assert_eq!(
        nodes[0].row.get("id").cloned().unwrap_or(Value::Null),
        Value::F64(3.0)
    );
    assert_eq!(
        nodes[1].row.get("id").cloned().unwrap_or(Value::Null),
        Value::F64(4.0)
    );
    assert_eq!(
        nodes[2].row.get("id").cloned().unwrap_or(Value::Null),
        Value::F64(5.0)
    );
}

#[test]
fn test_memory_source_fetch_with_start_after() {
    let source = make_source(
        "users",
        &["id"],
        &[("id", ColumnType::Number { optional: false })],
    );
    for i in 1..=5 {
        add_row(&source, &[("id", Value::F64(i as f64))]);
    }
    let input = source.borrow_mut().connect(None, None, None, None, None);
    let req = FetchRequest {
        start: Some(Start {
            row: make_row(&[("id", Value::F64(3.0))]),
            basis: Basis::After,
        }),
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(input.borrow().fetch(&req)).collect();
    assert_eq!(nodes.len(), 2);
    assert_eq!(
        nodes[0].row.get("id").cloned().unwrap_or(Value::Null),
        Value::F64(4.0)
    );
    assert_eq!(
        nodes[1].row.get("id").cloned().unwrap_or(Value::Null),
        Value::F64(5.0)
    );
}

#[test]
fn test_memory_source_fetch_with_filter_predicate() {
    let source = make_source(
        "users",
        &["id"],
        &[
            ("id", ColumnType::Number { optional: false }),
            ("name", ColumnType::String { optional: false }),
        ],
    );
    for i in 1..=5 {
        add_row(
            &source,
            &[
                ("id", Value::F64(i as f64)),
                ("name", str_val_owned(format!("user{}", i))),
            ],
        );
    }
    let predicate: Arc<dyn Fn(&Row) -> bool> = Arc::new(|row| {
        row.get("id")
            .and_then(|v| match v {
                Value::F64(n) => Some(*n > 2.0),
                _ => None,
            })
            .unwrap_or(false)
    });
    let input = source
        .borrow_mut()
        .connect(None, None, Some(predicate), None, None);
    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(input.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(nodes.len(), 3);
    assert_eq!(
        nodes[0].row.get("id").cloned().unwrap_or(Value::Null),
        Value::F64(3.0)
    );
    assert_eq!(
        nodes[1].row.get("id").cloned().unwrap_or(Value::Null),
        Value::F64(4.0)
    );
    assert_eq!(
        nodes[2].row.get("id").cloned().unwrap_or(Value::Null),
        Value::F64(5.0)
    );
}

#[test]
fn test_memory_source_fetch_with_multi_constraints_compound_key() {
    let source = make_source(
        "items",
        &["a", "b"],
        &[
            ("a", ColumnType::Number { optional: false }),
            ("b", ColumnType::String { optional: false }),
        ],
    );
    for i in 1..=4 {
        add_row(
            &source,
            &[
                ("id", Value::F64(i as f64)),
                ("a", Value::F64(i as f64)),
                ("b", str_val_owned(format!("val{}", i))),
            ],
        );
    }
    let input = source.borrow_mut().connect(None, None, None, None, None);

    let mut mc1 = Constraint::default();
    mc1.insert("a".to_string(), Value::F64(1.0));
    mc1.insert("b".to_string(), str_val("val1"));
    let mut mc2 = Constraint::default();
    mc2.insert("a".to_string(), Value::F64(3.0));
    mc2.insert("b".to_string(), str_val("val3"));

    let mc: MultiConstraint = vec![mc1, mc2];
    let req = FetchRequest {
        multi_constraints: vec![mc],
        ..Default::default()
    };
    let nodes: Vec<Node> = rust_ivm::ivm::stream::skip_yields(input.borrow().fetch(&req)).collect();
    assert_eq!(
        nodes.len(),
        2,
        "Compound key multi-constraint should match 2 rows"
    );
}

#[test]
fn test_memory_source_shared_data_visible_after_connect() {
    let source = make_source(
        "users",
        &["id"],
        &[("id", ColumnType::Number { optional: false })],
    );
    add_row(&source, &[("id", Value::F64(1.0))]);
    let input = source.borrow_mut().connect(None, None, None, None, None);
    add_row(&source, &[("id", Value::F64(2.0))]);
    let nodes: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(input.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(
        nodes.len(),
        2,
        "Row added after connect should be visible via shared data"
    );
}

#[test]
fn test_memory_source_multiple_connections_independent() {
    let source = make_source(
        "users",
        &["id"],
        &[("id", ColumnType::Number { optional: false })],
    );
    for i in 1..=3 {
        add_row(&source, &[("id", Value::F64(i as f64))]);
    }
    let input1 = source.borrow_mut().connect(None, None, None, None, None);
    let input2 = source.borrow_mut().connect(None, None, None, None, None);

    let nodes1: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(input1.borrow().fetch(&FetchRequest::default()))
            .collect();
    let nodes2: Vec<Node> =
        rust_ivm::ivm::stream::skip_yields(input2.borrow().fetch(&FetchRequest::default()))
            .collect();
    assert_eq!(nodes1.len(), 3);
    assert_eq!(nodes2.len(), 3);
}
