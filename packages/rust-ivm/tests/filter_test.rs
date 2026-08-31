//! Tests for ivm/filter.ts — port of `zql/src/ivm/filter.test.ts`.
//!
//! Tests: basics (add/remove through filter), edit transitions,
//!        beginFilter/endFilter forwarding.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::catch::Catch;
use rust_ivm::ivm::catch::CaughtChange;
use rust_ivm::ivm::data::{Row, Value};
use rust_ivm::ivm::filter::Filter;
use rust_ivm::ivm::filter_operators::build_filter_pipeline;
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::{
    make_source_change_add, make_source_change_edit, make_source_change_remove,
};

fn make_row(pairs: &[(&str, Value)]) -> Row {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Arc::new(map)
}

fn make_source(name: &str, cols: &[(&str, ColumnType)], pk: &[&str]) -> Rc<RefCell<MemorySource>> {
    let columns: HashMap<String, ColumnType> = cols
        .iter()
        .map(|(c, t)| (c.to_string(), t.clone()))
        .collect();
    Rc::new(RefCell::new(MemorySource::new(
        name,
        columns,
        pk.iter().map(|s| s.to_string()).collect(),
    )))
}

fn sort_order(parts: &[(&str, &str)]) -> rust_ivm::ivm::data::SortOrder {
    Arc::new(
        parts
            .iter()
            .map(|(c, d)| [c.to_string(), d.to_string()])
            .collect(),
    )
}

/// Get row values from caught changes (filter on Add/Remove).
fn push_types(pushes: &[CaughtChange]) -> Vec<&'static str> {
    pushes
        .iter()
        .map(|c| match c {
            CaughtChange::Add { .. } => "add",
            CaughtChange::Remove { .. } => "remove",
            CaughtChange::Edit { .. } => "edit",
            CaughtChange::Child { .. } => "child",
        })
        .collect()
}

// ---------------------------------------------------------------------------
// basics
// ---------------------------------------------------------------------------

#[test]
fn test_filter_basics_fetch() {
    let source = make_source(
        "table",
        &[
            ("a", ColumnType::Number { optional: false }),
            ("b", ColumnType::String { optional: false }),
        ],
        &["a"],
    );
    source.borrow_mut().add_row(
        [
            ("a".to_string(), Value::F64(3.0)),
            ("b".to_string(), Value::Str("foo".into())),
        ]
        .into_iter()
        .collect(),
    );
    source.borrow_mut().add_row(
        [
            ("a".to_string(), Value::F64(2.0)),
            ("b".to_string(), Value::Str("bar".into())),
        ]
        .into_iter()
        .collect(),
    );
    source.borrow_mut().add_row(
        [
            ("a".to_string(), Value::F64(1.0)),
            ("b".to_string(), Value::Str("foo".into())),
        ]
        .into_iter()
        .collect(),
    );

    let conn =
        source
            .borrow_mut()
            .connect(Some(sort_order(&[("a", "asc")])), None, None, None, None);
    let filter = build_filter_pipeline(conn, |fi| {
        let f: rust_ivm::ivm::filter_operators::FilterInputHandle = Filter::new(
            fi,
            Arc::new(|row| row.get("b") == Some(&Value::Str("foo".into()))),
        );
        f
    });

    let catch = Catch::new(filter, false);

    let fetched = catch.borrow().fetch(&Default::default());
    assert_eq!(fetched.len(), 2);
    let a_values: Vec<Value> = fetched
        .iter()
        .map(|n| n.row.get("a").cloned().unwrap_or(Value::Null))
        .collect();
    assert_eq!(a_values, vec![Value::F64(1.0), Value::F64(3.0)]);
}

#[test]
fn test_filter_basics_push() {
    let source = make_source(
        "table",
        &[
            ("a", ColumnType::Number { optional: false }),
            ("b", ColumnType::String { optional: false }),
        ],
        &["a"],
    );
    source.borrow_mut().add_row(
        [
            ("a".to_string(), Value::F64(3.0)),
            ("b".to_string(), Value::Str("foo".into())),
        ]
        .into_iter()
        .collect(),
    );
    source.borrow_mut().add_row(
        [
            ("a".to_string(), Value::F64(2.0)),
            ("b".to_string(), Value::Str("bar".into())),
        ]
        .into_iter()
        .collect(),
    );
    source.borrow_mut().add_row(
        [
            ("a".to_string(), Value::F64(1.0)),
            ("b".to_string(), Value::Str("foo".into())),
        ]
        .into_iter()
        .collect(),
    );

    let conn =
        source
            .borrow_mut()
            .connect(Some(sort_order(&[("a", "asc")])), None, None, None, None);
    let filter = build_filter_pipeline(conn, |fi| {
        let f: rust_ivm::ivm::filter_operators::FilterInputHandle = Filter::new(
            fi,
            Arc::new(|row| row.get("b") == Some(&Value::Str("foo".into()))),
        );
        f
    });
    let catch = Catch::new(filter, false);

    // Push some changes through the source
    let _ = source.borrow_mut().push(make_source_change_add(make_row(&[
        ("a", Value::F64(4.0)),
        ("b", Value::Str("bar".into())),
    ])));
    let _ = source.borrow_mut().push(make_source_change_add(make_row(&[
        ("a", Value::F64(5.0)),
        ("b", Value::Str("foo".into())),
    ])));
    let _ = source
        .borrow_mut()
        .push(make_source_change_remove(make_row(&[
            ("a", Value::F64(3.0)),
            ("b", Value::Str("foo".into())),
        ])));
    let _ = source
        .borrow_mut()
        .push(make_source_change_remove(make_row(&[
            ("a", Value::F64(2.0)),
            ("b", Value::Str("bar".into())),
        ])));

    let pushes = catch.borrow().pushes.clone();
    assert_eq!(push_types(&pushes), vec!["add", "remove"]);

    // First push should be the add of a=5 b=foo
    match &pushes[0] {
        CaughtChange::Add { node } => {
            assert_eq!(node.row.get("a"), Some(&Value::F64(5.0)));
            assert_eq!(node.row.get("b"), Some(&Value::Str("foo".into())));
        }
        _ => panic!("Expected Add"),
    }
    // Second push should be the remove of a=3 b=foo
    match &pushes[1] {
        CaughtChange::Remove { node } => {
            assert_eq!(node.row.get("a"), Some(&Value::F64(3.0)));
        }
        _ => panic!("Expected Remove"),
    }
}

// ---------------------------------------------------------------------------
// edit transitions
// ---------------------------------------------------------------------------

#[test]
fn test_filter_edit_add_passes_filter() {
    let source = make_source(
        "table",
        &[
            ("a", ColumnType::Number { optional: false }),
            ("x", ColumnType::Number { optional: false }),
        ],
        &["a"],
    );
    for (a, x) in [(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)] {
        source.borrow_mut().add_row(
            [
                ("a".to_string(), Value::F64(a)),
                ("x".to_string(), Value::F64(x)),
            ]
            .into_iter()
            .collect(),
        );
    }

    let conn =
        source
            .borrow_mut()
            .connect(Some(sort_order(&[("a", "asc")])), None, None, None, None);
    let filter = build_filter_pipeline(conn, |fi| {
        let f: rust_ivm::ivm::filter_operators::FilterInputHandle = Filter::new(
            fi,
            Arc::new(|row| match row.get("x") {
                Some(Value::F64(v)) => *v % 2.0 == 0.0,
                _ => false,
            }),
        );
        f
    });
    let catch = Catch::new(filter, false);

    // Initial fetch: only a=2, x=2 passes the filter
    let fetched = catch.borrow().fetch(&Default::default());
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].row.get("a"), Some(&Value::F64(2.0)));

    // Add a=4, x=4 — passes filter
    let _ = source.borrow_mut().push(make_source_change_add(make_row(&[
        ("a", Value::F64(4.0)),
        ("x", Value::F64(4.0)),
    ])));
    // Edit a=3: x 3→6 — was not passing, now passes (becomes Add)
    let _ = source.borrow_mut().push(make_source_change_edit(
        make_row(&[("a", Value::F64(3.0)), ("x", Value::F64(6.0))]),
        make_row(&[("a", Value::F64(3.0)), ("x", Value::F64(3.0))]),
    ));

    let pushes = catch.borrow().pushes.clone();
    assert_eq!(push_types(&pushes), vec!["add", "add"]);

    match &pushes[0] {
        CaughtChange::Add { node } => assert_eq!(node.row.get("a"), Some(&Value::F64(4.0))),
        _ => panic!("Expected Add"),
    }
    match &pushes[1] {
        CaughtChange::Add { node } => assert_eq!(node.row.get("a"), Some(&Value::F64(3.0))),
        _ => panic!("Expected Add"),
    }
}

#[test]
fn test_filter_edit_stops_passing_becomes_remove() {
    let source = make_source(
        "table",
        &[
            ("a", ColumnType::Number { optional: false }),
            ("x", ColumnType::Number { optional: false }),
        ],
        &["a"],
    );
    for (a, x) in [(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)] {
        source.borrow_mut().add_row(
            [
                ("a".to_string(), Value::F64(a)),
                ("x".to_string(), Value::F64(x)),
            ]
            .into_iter()
            .collect(),
        );
    }

    let conn =
        source
            .borrow_mut()
            .connect(Some(sort_order(&[("a", "asc")])), None, None, None, None);
    let filter = build_filter_pipeline(conn, |fi| {
        let f: rust_ivm::ivm::filter_operators::FilterInputHandle = Filter::new(
            fi,
            Arc::new(|row| match row.get("x") {
                Some(Value::F64(v)) => *v % 2.0 == 0.0,
                _ => false,
            }),
        );
        f
    });
    let catch = Catch::new(filter, false);

    // a=2, x=2 passes initially
    let _ = catch.borrow().fetch(&Default::default());

    // First edit: a=3 x 3→6 (becomes add, already tested above)
    let _ = source.borrow_mut().push(make_source_change_edit(
        make_row(&[("a", Value::F64(3.0)), ("x", Value::F64(6.0))]),
        make_row(&[("a", Value::F64(3.0)), ("x", Value::F64(3.0))]),
    ));
    catch.borrow_mut().reset();

    // Edit a=3 x 6→5: was passing, now doesn't → Remove
    let _ = source.borrow_mut().push(make_source_change_edit(
        make_row(&[("a", Value::F64(3.0)), ("x", Value::F64(5.0))]),
        make_row(&[("a", Value::F64(3.0)), ("x", Value::F64(6.0))]),
    ));

    let pushes = catch.borrow().pushes.clone();
    assert_eq!(push_types(&pushes), vec!["remove"]);
    match &pushes[0] {
        CaughtChange::Remove { node } => assert_eq!(node.row.get("a"), Some(&Value::F64(3.0))),
        _ => panic!("Expected Remove"),
    }
}

#[test]
fn test_filter_edit_neither_passes_is_noop() {
    let source = make_source(
        "table",
        &[
            ("a", ColumnType::Number { optional: false }),
            ("x", ColumnType::Number { optional: false }),
        ],
        &["a"],
    );
    for (a, x) in [(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)] {
        source.borrow_mut().add_row(
            [
                ("a".to_string(), Value::F64(a)),
                ("x".to_string(), Value::F64(x)),
            ]
            .into_iter()
            .collect(),
        );
    }

    let conn =
        source
            .borrow_mut()
            .connect(Some(sort_order(&[("a", "asc")])), None, None, None, None);
    let filter = build_filter_pipeline(conn, |fi| {
        let f: rust_ivm::ivm::filter_operators::FilterInputHandle = Filter::new(
            fi,
            Arc::new(|row| match row.get("x") {
                Some(Value::F64(v)) => *v % 2.0 == 0.0,
                _ => false,
            }),
        );
        f
    });
    let catch = Catch::new(filter, false);

    let _ = catch.borrow().fetch(&Default::default());
    // Edit a=3 x 5→7 — neither passes filter, no push
    let _ = source.borrow_mut().push(make_source_change_edit(
        make_row(&[("a", Value::F64(3.0)), ("x", Value::F64(7.0))]),
        make_row(&[("a", Value::F64(3.0)), ("x", Value::F64(5.0))]),
    ));

    assert!(catch.borrow().pushes.is_empty());
}

#[test]
fn test_filter_edit_both_pass_is_edit() {
    let source = make_source(
        "table",
        &[
            ("a", ColumnType::Number { optional: false }),
            ("x", ColumnType::Number { optional: false }),
        ],
        &["a"],
    );
    for (a, x) in [(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)] {
        source.borrow_mut().add_row(
            [
                ("a".to_string(), Value::F64(a)),
                ("x".to_string(), Value::F64(x)),
            ]
            .into_iter()
            .collect(),
        );
    }

    let conn =
        source
            .borrow_mut()
            .connect(Some(sort_order(&[("a", "asc")])), None, None, None, None);
    let filter = build_filter_pipeline(conn, |fi| {
        let f: rust_ivm::ivm::filter_operators::FilterInputHandle = Filter::new(
            fi,
            Arc::new(|row| match row.get("x") {
                Some(Value::F64(v)) => *v % 2.0 == 0.0,
                _ => false,
            }),
        );
        f
    });
    let catch = Catch::new(filter, false);

    let _ = catch.borrow().fetch(&Default::default());

    // Edit a=2 x 2→4: both pass filter → Edit
    let _ = source.borrow_mut().push(make_source_change_edit(
        make_row(&[("a", Value::F64(2.0)), ("x", Value::F64(4.0))]),
        make_row(&[("a", Value::F64(2.0)), ("x", Value::F64(2.0))]),
    ));

    let pushes = catch.borrow().pushes.clone();
    assert_eq!(push_types(&pushes), vec!["edit"]);
    match &pushes[0] {
        CaughtChange::Edit { old_row, row } => {
            assert_eq!(row.get("x"), Some(&Value::F64(4.0)));
            assert_eq!(old_row.get("x"), Some(&Value::F64(2.0)));
        }
        _ => panic!("Expected Edit"),
    }
}
