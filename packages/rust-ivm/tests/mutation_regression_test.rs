//! Regression tests for behaviours that mutation testing found **uncovered**.
//!
//! Both bugs below were introduced deliberately into the source, the entire
//! 533-test suite was run, and everything stayed green. Neither behaviour was
//! being exercised by anything.
//!
//! That is worth stating plainly: a passing suite is evidence about the tests
//! that exist, not about the code. These two tests exist so that these two
//! specific regressions cannot recur silently.
//!
//! Reproduce the check with `node tools/mutate.mjs`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{
    Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery, SimpleCondition, ValuePosition,
};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::{Row, Value};
use rust_ivm::ivm::join_utils::row_equals_for_compound_key;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::{MemorySource, Source};
use rust_ivm::streamer::RowChange;

// ---------------------------------------------------------------------------
// Mutation: join/compound-key-null
//
// `row_equals_for_compound_key` must use `compare_values` (where NULL == NULL)
// and NOT `values_equal` (join semantics, where NULL matches nothing).
//
// This is the null-semantics bug fixed in b6f8cc871. It had no regression test,
// so swapping the comparison back reintroduced the bug with a green suite.
//
// Why it matters: `Join::push_parent` / `push_child` assert with this function
// that an edit did not change the relationship. If it reports "not equal" for
// two rows that are both NULL on the key, a perfectly legal edit of a row with
// a NULL foreign key panics the engine — which at the napi boundary becomes a
// pipeline reset.
// ---------------------------------------------------------------------------

fn row(pairs: &[(&str, Value)]) -> Row {
    let mut m: FxHashMap<String, Value> = FxHashMap::default();
    for (k, v) in pairs {
        m.insert(k.to_string(), v.clone());
    }
    std::sync::Arc::new(m)
}

#[test]
fn compound_key_treats_null_as_equal_to_null() {
    let key = vec!["k".to_string()];

    let a = row(&[("id", Value::Str("a".into())), ("k", Value::Null)]);
    let b = row(&[("id", Value::Str("b".into())), ("k", Value::Null)]);
    assert!(
        row_equals_for_compound_key(&a, &b, &key),
        "NULL == NULL for compound-key equality (compare_values semantics). \
         Using values_equal here reintroduces the b6f8cc871 bug and panics \
         Join on a legal edit of a NULL-keyed row."
    );

    // A missing column is normalized to NULL, so it must match an explicit NULL.
    let c = row(&[("id", Value::Str("c".into()))]);
    assert!(
        row_equals_for_compound_key(&a, &c, &key),
        "absent key column normalizes to NULL and must equal an explicit NULL"
    );
}

#[test]
fn compound_key_still_distinguishes_real_values() {
    let key = vec!["k".to_string()];
    let a = row(&[("k", Value::Str("x".into()))]);
    let b = row(&[("k", Value::Str("y".into()))]);
    let n = row(&[("k", Value::Null)]);

    assert!(!row_equals_for_compound_key(&a, &b, &key));
    assert!(
        !row_equals_for_compound_key(&a, &n, &key),
        "NULL must not equal a non-NULL value"
    );
    assert!(row_equals_for_compound_key(&a, &a, &key));
}

// ---------------------------------------------------------------------------
// Mutation: exists/always-true
//
// `Exists::filter` computes `size > 0`. Mutating it to `size >= 0` makes
// `exists` unconditionally true, so an EXISTS query stops filtering: parents
// with no matching children leak into results, and NOT-EXISTS returns nothing.
//
// The suite was green with that mutation in place — i.e. nothing asserted that
// EXISTS actually excludes anything.
// ---------------------------------------------------------------------------

fn make_source(
    name: &str,
    columns: &[(&str, ColumnType)],
    pk: &[&str],
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
    let mut m: FxHashMap<String, Value> = FxHashMap::default();
    for (k, v) in pairs {
        m.insert(k.to_string(), v.clone());
    }
    source.borrow_mut().add_row(m);
}

fn exists_condition(rel: RelatedSubquery, not: bool) -> Condition {
    Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: rel,
        op: if not { "NOT EXISTS" } else { "EXISTS" }.to_string(),
        flip: Some(false),
        scalar: false,
        plan_id: None,
    })
}

fn child_subquery(where_clause: Option<Condition>) -> RelatedSubquery {
    RelatedSubquery {
        subquery: Box::new(Ast {
            schema: None,
            table: "child".to_string(),
            alias: Some("kids".to_string()),
            where_clause,
            related: Vec::new(),
            limit: None,
            order_by: None,
            start: None,
        }),
        relationship_name: "kids".to_string(),
        parent_key: vec!["id".to_string()],
        child_key: vec!["parentId".to_string()],
        hidden: false,
        system: None,
    }
}

/// Hydrate `ast` over a fixed two-table fixture and return the parent ids that
/// survived the filter.
fn hydrated_parent_ids(not: bool) -> Vec<String> {
    let parent = make_source(
        "parent",
        &[("id", ColumnType::String { optional: false })],
        &["id"],
    );
    let child = make_source(
        "child",
        &[
            ("id", ColumnType::String { optional: false }),
            ("parentId", ColumnType::String { optional: false }),
        ],
        &["id"],
    );

    // p1 has a child, p2 does not. That asymmetry is the whole test.
    add_row(&parent, &[("id", Value::Str("p1".into()))]);
    add_row(&parent, &[("id", Value::Str("p2".into()))]);
    add_row(
        &child,
        &[
            ("id", Value::Str("c1".into())),
            ("parentId", Value::Str("p1".into())),
        ],
    );

    let mut pks = HashMap::new();
    pks.insert("parent".to_string(), vec!["id".to_string()]);
    pks.insert("child".to_string(), vec!["id".to_string()]);
    let mut eng = Engine::new(pks);
    eng.register_source(parent as Rc<RefCell<dyn Source>>);
    eng.register_source(child as Rc<RefCell<dyn Source>>);

    let ast = Ast {
        schema: None,
        table: "parent".to_string(),
        alias: None,
        where_clause: Some(exists_condition(child_subquery(None), not)),
        related: Vec::new(),
        limit: None,
        order_by: None,
        start: None,
    };

    let mut ids: Vec<String> = Vec::new();
    eng.add_queries_streaming(
        &[QuerySpec {
            query_id: "q".to_string(),
            ast,
        }],
        |rc: &RowChange| {
            if rc.table == "parent" {
                if let Some(r) = &rc.row {
                    if let Some(Value::Str(s)) = r.get("id") {
                        ids.push(s.to_string());
                    }
                }
            }
        },
    );
    ids.sort();
    ids
}

#[test]
fn exists_excludes_parents_with_no_children() {
    assert_eq!(
        hydrated_parent_ids(false),
        vec!["p1".to_string()],
        "EXISTS must exclude p2, which has no children. If p2 appears, the \
         filter is passing everything (`size >= 0` instead of `size > 0`)."
    );
}

#[test]
fn not_exists_selects_only_parents_without_children() {
    assert_eq!(
        hydrated_parent_ids(true),
        vec!["p2".to_string()],
        "NOT EXISTS must select exactly the childless parent. An always-true \
         `exists` makes this empty."
    );
}

/// Guards the `simple` import path too — a filtered EXISTS must still exclude.
#[test]
fn exists_with_a_child_predicate_still_filters() {
    let parent = make_source(
        "parent",
        &[("id", ColumnType::String { optional: false })],
        &["id"],
    );
    let child = make_source(
        "child",
        &[
            ("id", ColumnType::String { optional: false }),
            ("parentId", ColumnType::String { optional: false }),
            ("kind", ColumnType::String { optional: false }),
        ],
        &["id"],
    );
    add_row(&parent, &[("id", Value::Str("p1".into()))]);
    add_row(
        &child,
        &[
            ("id", Value::Str("c1".into())),
            ("parentId", Value::Str("p1".into())),
            ("kind", Value::Str("other".into())),
        ],
    );

    let mut pks = HashMap::new();
    pks.insert("parent".to_string(), vec!["id".to_string()]);
    pks.insert("child".to_string(), vec!["id".to_string()]);
    let mut eng = Engine::new(pks);
    eng.register_source(parent as Rc<RefCell<dyn Source>>);
    eng.register_source(child as Rc<RefCell<dyn Source>>);

    // p1's only child has kind='other', so EXISTS(kind='wanted') is false.
    let cond = Condition::Simple(SimpleCondition {
        op: "=".to_string(),
        left: ValuePosition::Column {
            name: "kind".to_string(),
        },
        right: ValuePosition::Literal {
            value: Value::Str("wanted".into()),
        },
    });

    let ast = Ast {
        schema: None,
        table: "parent".to_string(),
        alias: None,
        where_clause: Some(exists_condition(child_subquery(Some(cond)), false)),
        related: Vec::new(),
        limit: None,
        order_by: None,
        start: None,
    };

    let mut count = 0usize;
    eng.add_queries_streaming(
        &[QuerySpec {
            query_id: "q".to_string(),
            ast,
        }],
        |rc: &RowChange| {
            if rc.table == "parent" {
                count += 1;
            }
        },
    );
    assert_eq!(
        count, 0,
        "no parent has a child with kind='wanted', so EXISTS must select none"
    );
}
