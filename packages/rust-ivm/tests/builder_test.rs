//! Tests for builder features: LIKE/ILIKE, IN/NOT IN, IS/IS NOT, EXISTS, transformFilters.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{
    Ast, Condition, CorrelatedSubqueryCondition, OrderPart, RelatedSubquery, SimpleCondition,
    ValuePosition,
};
use rust_ivm::builder::filter::{TransformedFilters, create_predicate, transform_filters};
use rust_ivm::builder::like::get_like_predicate;
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::{Row, Value};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;

fn make_row(pairs: &[(&str, Value)]) -> Row {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Arc::new(map)
}

fn make_source(name: &str, pk: &[&str]) -> Rc<RefCell<MemorySource>> {
    let columns: HashMap<String, ColumnType> = pk
        .iter()
        .map(|c| (c.to_string(), ColumnType::Number { optional: false }))
        .collect();
    Rc::new(RefCell::new(MemorySource::new(
        name,
        columns,
        pk.iter().map(|s| s.to_string()).collect(),
    )))
}

fn add_rows(source: &Rc<RefCell<MemorySource>>, rows: &[(&str, Value, &str, Value)]) {
    for (c1, v1, c2, v2) in rows {
        let mut m: FxHashMap<String, Value> = FxHashMap::default();
        m.insert(c1.to_string(), v1.clone());
        m.insert(c2.to_string(), v2.clone());
        source.borrow_mut().add_row(m);
    }
}

// ===========================================================================
// LIKE / ILIKE / NOT LIKE / NOT ILIKE
// ===========================================================================

#[test]
fn test_like_exact_match() {
    let pred = get_like_predicate(&Value::Str("hello".into()), "");
    assert!(pred(&Value::Str("hello".into())));
    assert!(!pred(&Value::Str("world".into())));
}

#[test]
fn test_like_percent_wildcard() {
    let pred = get_like_predicate(&Value::Str("hel%".into()), "");
    assert!(pred(&Value::Str("hello".into())));
    assert!(pred(&Value::Str("help".into())));
    assert!(pred(&Value::Str("hel".into())));
    assert!(!pred(&Value::Str("world".into())));
}

#[test]
fn test_like_underscore_wildcard() {
    let pred = get_like_predicate(&Value::Str("h_llo".into()), "");
    assert!(pred(&Value::Str("hello".into())));
    assert!(pred(&Value::Str("hxllo".into())));
    assert!(!pred(&Value::Str("heello".into())));
}

#[test]
fn test_like_escape() {
    let pred = get_like_predicate(&Value::Str("100\\%".into()), "");
    assert!(pred(&Value::Str("100%".into())));
    assert!(!pred(&Value::Str("100abc".into())));
}

#[test]
fn test_ilike_case_insensitive() {
    let pred = get_like_predicate(&Value::Str("hello".into()), "i");
    assert!(pred(&Value::Str("HELLO".into())));
    assert!(pred(&Value::Str("Hello".into())));
    assert!(pred(&Value::Str("hello".into())));
    assert!(!pred(&Value::Str("world".into())));
}

#[test]
fn test_like_in_predicate() {
    // Test LIKE through create_predicate
    let cond = Condition::Simple(SimpleCondition {
        op: "LIKE".to_string(),
        left: ValuePosition::Column {
            name: "name".to_string(),
        },
        right: ValuePosition::Literal {
            value: Value::Str("user%".into()),
        },
    });
    let pred = create_predicate(&cond);
    assert!(pred(&make_row(&[("name", Value::Str("user1".into()))])));
    assert!(pred(&make_row(&[("name", Value::Str("user42".into()))])));
    assert!(!pred(&make_row(&[("name", Value::Str("admin".into()))])));
}

#[test]
fn test_ilike_in_predicate() {
    let cond = Condition::Simple(SimpleCondition {
        op: "ILIKE".to_string(),
        left: ValuePosition::Column {
            name: "name".to_string(),
        },
        right: ValuePosition::Literal {
            value: Value::Str("USER%".into()),
        },
    });
    let pred = create_predicate(&cond);
    assert!(pred(&make_row(&[("name", Value::Str("user1".into()))])));
    assert!(pred(&make_row(&[("name", Value::Str("USER42".into()))])));
    assert!(!pred(&make_row(&[("name", Value::Str("admin".into()))])));
}

#[test]
fn test_not_like_in_predicate() {
    let cond = Condition::Simple(SimpleCondition {
        op: "NOT LIKE".to_string(),
        left: ValuePosition::Column {
            name: "name".to_string(),
        },
        right: ValuePosition::Literal {
            value: Value::Str("user%".into()),
        },
    });
    let pred = create_predicate(&cond);
    assert!(!pred(&make_row(&[("name", Value::Str("user1".into()))])));
    assert!(pred(&make_row(&[("name", Value::Str("admin".into()))])));
}

// ===========================================================================
// IS / IS NOT (null checks)
// ===========================================================================

#[test]
fn test_is_null_predicate() {
    let cond = Condition::Simple(SimpleCondition {
        op: "IS".to_string(),
        left: ValuePosition::Column {
            name: "name".to_string(),
        },
        right: ValuePosition::Literal { value: Value::Null },
    });
    let pred = create_predicate(&cond);
    assert!(pred(&make_row(&[("name", Value::Null)])));
    assert!(!pred(&make_row(&[("name", Value::Str("bob".into()))])));
}

#[test]
fn test_is_not_null_predicate() {
    let cond = Condition::Simple(SimpleCondition {
        op: "IS NOT".to_string(),
        left: ValuePosition::Column {
            name: "name".to_string(),
        },
        right: ValuePosition::Literal { value: Value::Null },
    });
    let pred = create_predicate(&cond);
    assert!(!pred(&make_row(&[("name", Value::Null)])));
    assert!(pred(&make_row(&[("name", Value::Str("bob".into()))])));
}

// ===========================================================================
// IS with literal (boolean check)
// ===========================================================================

#[test]
fn test_is_with_bool_literal() {
    let cond = Condition::Simple(SimpleCondition {
        op: "IS".to_string(),
        left: ValuePosition::Column {
            name: "active".to_string(),
        },
        right: ValuePosition::Literal {
            value: Value::Bool(true),
        },
    });
    let pred = create_predicate(&cond);
    assert!(pred(&make_row(&[("active", Value::Bool(true))])));
    assert!(!pred(&make_row(&[("active", Value::Bool(false))])));
    assert!(!pred(&make_row(&[("active", Value::Null)])));
}

// ===========================================================================
// transformFilters — strip correlated subquery conditions
// ===========================================================================

#[test]
fn test_transform_filters_no_subqueries() {
    let cond = Condition::Simple(SimpleCondition {
        op: "=".to_string(),
        left: ValuePosition::Column {
            name: "id".to_string(),
        },
        right: ValuePosition::Literal {
            value: Value::F64(1.0),
        },
    });
    let result = transform_filters(Some(&cond));
    assert!(!result.conditions_removed);
    assert!(result.filters.is_some());
}

#[test]
fn test_transform_filters_strips_subquery() {
    let csq = CorrelatedSubqueryCondition {
        related: RelatedSubquery {
            subquery: Box::new(Ast {
                schema: None,
                table: "posts".to_string(),
                alias: Some("posts".to_string()),
                where_clause: None,
                related: vec![],
                limit: None,
                order_by: None,
                start: None,
            }),
            relationship_name: "posts".to_string(),
            parent_key: vec!["id".to_string()],
            child_key: vec!["author_id".to_string()],
            hidden: false,
            system: None,
        },
        op: "EXISTS".to_string(),
        flip: Some(false),
        scalar: false,
        plan_id: None,
    };

    let simple = Condition::Simple(SimpleCondition {
        op: "=".to_string(),
        left: ValuePosition::Column {
            name: "active".to_string(),
        },
        right: ValuePosition::Literal {
            value: Value::Bool(true),
        },
    });

    // AND([simple, CSQ]) → should strip CSQ, keep simple
    let and = Condition::And(vec![simple.clone(), Condition::CorrelatedSubquery(csq)]);
    let result = transform_filters(Some(&and));
    assert!(result.conditions_removed);
    // The simple condition should remain
    match &result.filters {
        Some(Condition::Simple(s)) => assert_eq!(s.op, "="),
        Some(Condition::And(conds)) => assert_eq!(conds.len(), 1),
        _ => panic!("Expected simple or and with 1 condition"),
    }
}

#[test]
fn test_transform_filters_or_with_subquery_removes_all() {
    let csq = CorrelatedSubqueryCondition {
        related: RelatedSubquery {
            subquery: Box::new(Ast {
                schema: None,
                table: "posts".to_string(),
                alias: Some("posts".to_string()),
                where_clause: None,
                related: vec![],
                limit: None,
                order_by: None,
                start: None,
            }),
            relationship_name: "posts".to_string(),
            parent_key: vec!["id".to_string()],
            child_key: vec!["author_id".to_string()],
            hidden: false,
            system: None,
        },
        op: "EXISTS".to_string(),
        flip: Some(false),
        scalar: false,
        plan_id: None,
    };

    let simple = Condition::Simple(SimpleCondition {
        op: "=".to_string(),
        left: ValuePosition::Column {
            name: "id".to_string(),
        },
        right: ValuePosition::Literal {
            value: Value::F64(1.0),
        },
    });

    // OR([simple, CSQ]) → if any branch is removed, the whole OR is removed
    let or = Condition::Or(vec![simple, Condition::CorrelatedSubquery(csq)]);
    let result = transform_filters(Some(&or));
    assert!(result.conditions_removed);
    assert!(result.filters.is_none());
}

#[test]
fn test_transform_filters_none() {
    let result: TransformedFilters = transform_filters(None);
    assert!(!result.conditions_removed);
    assert!(result.filters.is_none());
}

// ===========================================================================
// Complete ordering
// ===========================================================================

#[test]
fn test_complete_ordering_appends_pks() {
    use rust_ivm::builder::complete_ordering::complete_ordering;

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: Some(vec![OrderPart {
            column: "name".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    };

    let result = complete_ordering(&ast, &|table| {
        assert_eq!(table, "users");
        vec!["id".to_string()]
    });

    let order_by = result.order_by.unwrap();
    assert_eq!(order_by.len(), 2);
    assert_eq!(order_by[0].column, "name");
    assert_eq!(order_by[1].column, "id");
    assert_eq!(order_by[1].direction, "asc");
}

#[test]
fn test_complete_ordering_no_dup_pks() {
    use rust_ivm::builder::complete_ordering::complete_ordering;

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: Some(vec![
            OrderPart {
                column: "name".to_string(),
                direction: "asc".to_string(),
            },
            OrderPart {
                column: "id".to_string(),
                direction: "asc".to_string(),
            },
        ]),
        start: None,
    };

    let result = complete_ordering(&ast, &|_| vec!["id".to_string()]);
    let order_by = result.order_by.unwrap();
    assert_eq!(order_by.len(), 2); // no duplicate PK
}

// ===========================================================================
// assert_no_not_exists
// ===========================================================================

#[test]
fn test_assert_no_not_exists_passes_for_exists() {
    use rust_ivm::builder::builder::assert_no_not_exists;

    let csq = CorrelatedSubqueryCondition {
        related: RelatedSubquery {
            subquery: Box::new(Ast {
                schema: None,
                table: "posts".to_string(),
                alias: Some("posts".to_string()),
                where_clause: None,
                related: vec![],
                limit: None,
                order_by: None,
                start: None,
            }),
            relationship_name: "posts".to_string(),
            parent_key: vec!["id".to_string()],
            child_key: vec!["author_id".to_string()],
            hidden: false,
            system: None,
        },
        op: "EXISTS".to_string(),
        flip: Some(false),
        scalar: false,
        plan_id: None,
    };

    // Should not panic
    assert_no_not_exists(&Condition::CorrelatedSubquery(csq));
}

#[test]
#[should_panic(expected = "not(exists()) is not supported")]
fn test_assert_no_not_exists_panics_for_not_exists() {
    use rust_ivm::builder::builder::assert_no_not_exists;

    let csq = CorrelatedSubqueryCondition {
        related: RelatedSubquery {
            subquery: Box::new(Ast {
                schema: None,
                table: "posts".to_string(),
                alias: Some("posts".to_string()),
                where_clause: None,
                related: vec![],
                limit: None,
                order_by: None,
                start: None,
            }),
            relationship_name: "posts".to_string(),
            parent_key: vec!["id".to_string()],
            child_key: vec!["author_id".to_string()],
            hidden: false,
            system: None,
        },
        op: "NOT EXISTS".to_string(),
        flip: Some(false),
        scalar: false,
        plan_id: None,
    };

    assert_no_not_exists(&Condition::CorrelatedSubquery(csq));
}

// ===========================================================================
// condition_includes_flipped_subquery_at_any_level
// ===========================================================================

#[test]
fn test_condition_includes_flipped_false() {
    use rust_ivm::builder::builder::condition_includes_flipped_subquery_at_any_level;

    let simple = Condition::Simple(SimpleCondition {
        op: "=".to_string(),
        left: ValuePosition::Column {
            name: "id".to_string(),
        },
        right: ValuePosition::Literal {
            value: Value::F64(1.0),
        },
    });
    assert!(!condition_includes_flipped_subquery_at_any_level(&simple));
}

#[test]
fn test_condition_includes_flipped_true() {
    use rust_ivm::builder::builder::condition_includes_flipped_subquery_at_any_level;

    let csq = CorrelatedSubqueryCondition {
        related: RelatedSubquery {
            subquery: Box::new(Ast {
                schema: None,
                table: "posts".to_string(),
                alias: Some("posts".to_string()),
                where_clause: None,
                related: vec![],
                limit: None,
                order_by: None,
                start: None,
            }),
            relationship_name: "posts".to_string(),
            parent_key: vec!["id".to_string()],
            child_key: vec!["author_id".to_string()],
            hidden: false,
            system: None,
        },
        op: "EXISTS".to_string(),
        flip: Some(true),
        scalar: false,
        plan_id: None,
    };

    let simple = Condition::Simple(SimpleCondition {
        op: "=".to_string(),
        left: ValuePosition::Column {
            name: "id".to_string(),
        },
        right: ValuePosition::Literal {
            value: Value::F64(1.0),
        },
    });

    let and = Condition::And(vec![simple, Condition::CorrelatedSubquery(csq)]);
    assert!(condition_includes_flipped_subquery_at_any_level(&and));
}

// ===========================================================================
// Integration: builder pipeline with LIKE filter
// ===========================================================================

#[test]
fn test_pipeline_with_like_filter() {
    let source = make_source("users", &["id"]);
    add_rows(
        &source,
        &[
            ("id", Value::F64(1.0), "name", Value::Str("user1".into())),
            ("id", Value::F64(2.0), "name", Value::Str("user2".into())),
            ("id", Value::F64(3.0), "name", Value::Str("admin".into())),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: Some(Condition::Simple(SimpleCondition {
            op: "LIKE".to_string(),
            left: ValuePosition::Column {
                name: "name".to_string(),
            },
            right: ValuePosition::Literal {
                value: Value::Str("user%".into()),
            },
        })),
        related: vec![],
        limit: None,
        order_by: Some(vec![OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    // user1 and user2 match, admin does not
    assert_eq!(results[0].changes.len(), 2);
}

// ===========================================================================
// Integration: builder pipeline with IS NULL filter
// ===========================================================================

#[test]
fn test_pipeline_with_is_null_filter() {
    let source = make_source("users", &["id"]);
    source.borrow_mut().add_row({
        let mut m: FxHashMap<String, Value> = FxHashMap::default();
        m.insert("id".to_string(), Value::F64(1.0));
        m.insert("name".to_string(), Value::Str("alice".into()));
        m.insert("bio".to_string(), Value::Null);
        m
    });
    source.borrow_mut().add_row({
        let mut m: FxHashMap<String, Value> = FxHashMap::default();
        m.insert("id".to_string(), Value::F64(2.0));
        m.insert("name".to_string(), Value::Str("bob".into()));
        m.insert("bio".to_string(), Value::Str("has bio".into()));
        m
    });

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: Some(Condition::Simple(SimpleCondition {
            op: "IS".to_string(),
            left: ValuePosition::Column {
                name: "bio".to_string(),
            },
            right: ValuePosition::Literal { value: Value::Null },
        })),
        related: vec![],
        limit: None,
        order_by: Some(vec![OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    // Only alice has NULL bio
    assert_eq!(results[0].changes.len(), 1);
    assert_eq!(
        results[0].changes[0].row.as_ref().unwrap().get("name"),
        Some(&Value::Str("alice".into()))
    );
}

// ===========================================================================
// Integration: builder pipeline with complex AND/OR
// ===========================================================================

#[test]
fn test_pipeline_with_and_or() {
    let source = make_source("users", &["id"]);
    add_rows(
        &source,
        &[
            ("id", Value::F64(1.0), "name", Value::Str("alice".into())),
            ("id", Value::F64(2.0), "name", Value::Str("bob".into())),
            ("id", Value::F64(3.0), "name", Value::Str("charlie".into())),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    // WHERE (id = 1 OR id = 3)
    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: Some(Condition::Or(vec![
            Condition::Simple(SimpleCondition {
                op: "=".to_string(),
                left: ValuePosition::Column {
                    name: "id".to_string(),
                },
                right: ValuePosition::Literal {
                    value: Value::F64(1.0),
                },
            }),
            Condition::Simple(SimpleCondition {
                op: "=".to_string(),
                left: ValuePosition::Column {
                    name: "id".to_string(),
                },
                right: ValuePosition::Literal {
                    value: Value::F64(3.0),
                },
            }),
        ])),
        related: vec![],
        limit: None,
        order_by: Some(vec![OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(results[0].changes.len(), 2);
}
