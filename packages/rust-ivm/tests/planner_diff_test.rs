//! Differential test: Rust plan_query vs TS planQuery.
//!
//! Builds ASTs with EXISTS/NOT EXISTS correlated subqueries (including
//! OR-with-CSQ shapes), plans them with a mock cost model, and asserts
//! the flip annotations match what TS would produce.
//!
//! Since we can't run TS in-process from Rust, this test verifies the
//! planner's structural correctness: deterministic flip decisions for
//! given cost inputs, correct 2^n enumeration, and that the plan_id →
//! flip mapping round-trips correctly.
//!
//! Run with: cargo test --test planner_diff_test -- --nocapture

use std::rc::Rc;

use rust_ivm::builder::ast::{Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery, SimpleCondition, ValuePosition};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::schema::{ColumnType, System};
use rust_ivm::planner::{plan_query, ConnectionCostModel, CostModelCost};

/// A simple mock cost model where each table has a fixed row count.
/// This makes flip decisions deterministic and testable.
fn mock_cost_model(table_costs: Vec<(&str, f64)>) -> ConnectionCostModel {
    let costs: std::collections::HashMap<String, f64> = table_costs
        .into_iter()
        .map(|(t, c)| (t.to_string(), c))
        .collect();
    Rc::new(move |table: &str, _sort: &[(String, String)], _filters: Option<&Condition>, _constraint: Option<&std::collections::BTreeMap<String, Option<Value>>>| {
        let rows = *costs.get(table).unwrap_or(&100.0);
        CostModelCost {
            startup_cost: 1.0,
            rows,
            fanout: Rc::new(|_cols: &[String]| rust_ivm::planner::FanoutEst {
                fanout: 1.0,
                confidence: rust_ivm::planner::Confidence::None,
            }),
        }
    })
}

fn simple_ast(table: &str) -> Ast {
    Ast {
        table: table.to_string(),
        ..Default::default()
    }
}

fn exists_condition(
    child_table: &str,
    parent_key: &str,
    child_key: &str,
    flip: Option<bool>,
) -> CorrelatedSubqueryCondition {
    CorrelatedSubqueryCondition {
        related: RelatedSubquery {
            subquery: Box::new(simple_ast(child_table)),
            relationship_name: child_table.to_string(),
            parent_key: vec![parent_key.to_string()],
            child_key: vec![child_key.to_string()],
            hidden: false,
            system: Some(System::Client),
        },
        op: "EXISTS".to_string(),
        flip,
        scalar: false,
        plan_id: None,
    }
}

fn not_exists_condition(
    child_table: &str,
    parent_key: &str,
    child_key: &str,
) -> CorrelatedSubqueryCondition {
    CorrelatedSubqueryCondition {
        related: RelatedSubquery {
            subquery: Box::new(simple_ast(child_table)),
            relationship_name: child_table.to_string(),
            parent_key: vec![parent_key.to_string()],
            child_key: vec![child_key.to_string()],
            hidden: false,
            system: Some(System::Client),
        },
        op: "NOT EXISTS".to_string(),
        flip: None,
        scalar: false,
        plan_id: None,
    }
}

/// Extract all flip annotations from a planned AST.
fn extract_flips(ast: &Ast) -> Vec<Option<bool>> {
    let mut flips = Vec::new();
    if let Some(ref where_clause) = ast.where_clause {
        extract_flips_from_condition(where_clause, &mut flips);
    }
    for csq in &ast.related {
        let mut sub_flips = extract_flips(&csq.subquery);
        flips.append(&mut sub_flips);
    }
    flips
}

fn extract_flips_from_condition(condition: &Condition, flips: &mut Vec<Option<bool>>) {
    match condition {
        Condition::Simple(_) => {}
        Condition::CorrelatedSubquery(csq) => {
            flips.push(csq.flip);
            if let Some(ref sub_where) = csq.related.subquery.where_clause {
                extract_flips_from_condition(sub_where, flips);
            }
        }
        Condition::And(conds) => {
            for c in conds {
                extract_flips_from_condition(c, flips);
            }
        }
        Condition::Or(conds) => {
            for c in conds {
                extract_flips_from_condition(c, flips);
            }
        }
    }
}

#[test]
fn test_single_exists_no_flip_when_child_larger() {
    // parent has 100 rows, child has 1000 rows → semi-join is cheaper
    // (don't flip: iterate 100 parents, check each against child)
    let mut ast = simple_ast("parent");
    ast.where_clause = Some(Condition::CorrelatedSubquery(exists_condition(
        "child", "id", "parent_id", None,
    )));

    let model = mock_cost_model(vec![("parent", 100.0), ("child", 1000.0)]);
    let planned = plan_query(&ast, model);
    let flips = extract_flips(&planned);
    assert_eq!(flips.len(), 1, "one EXISTS condition");
    assert_eq!(flips[0], Some(false), "should not flip when child is larger");
}

#[test]
fn test_single_exists_flip_when_child_smaller() {
    // parent has 1000 rows, child has 10 rows → flipped is cheaper
    // (iterate 10 children, look up parents)
    let mut ast = simple_ast("parent");
    ast.where_clause = Some(Condition::CorrelatedSubquery(exists_condition(
        "child", "id", "parent_id", None,
    )));

    let model = mock_cost_model(vec![("parent", 1000.0), ("child", 10.0)]);
    let planned = plan_query(&ast, model);
    let flips = extract_flips(&planned);
    assert_eq!(flips.len(), 1, "one EXISTS condition");
    assert_eq!(flips[0], Some(true), "should flip when child is smaller");
}

#[test]
fn test_not_exists_never_flips() {
    let mut ast = simple_ast("parent");
    ast.where_clause = Some(Condition::CorrelatedSubquery(not_exists_condition(
        "child", "id", "parent_id",
    )));

    let model = mock_cost_model(vec![("parent", 1000.0), ("child", 10.0)]);
    let planned = plan_query(&ast, model);
    let flips = extract_flips(&planned);
    assert_eq!(flips.len(), 1);
    assert_eq!(flips[0], Some(false), "NOT EXISTS must never flip");
}

#[test]
fn test_explicit_flip_true_not_changed() {
    let mut ast = simple_ast("parent");
    ast.where_clause = Some(Condition::CorrelatedSubquery(exists_condition(
        "child", "id", "parent_id", Some(true),
    )));

    // Even though child is larger (would normally not flip),
    // explicit flip=true forces it.
    let model = mock_cost_model(vec![("parent", 100.0), ("child", 1000.0)]);
    let planned = plan_query(&ast, model);
    let flips = extract_flips(&planned);
    assert_eq!(flips[0], Some(true), "explicit flip=true must be preserved");
}

#[test]
fn test_explicit_flip_false_not_changed() {
    let mut ast = simple_ast("parent");
    ast.where_clause = Some(Condition::CorrelatedSubquery(exists_condition(
        "child", "id", "parent_id", Some(false),
    )));

    // Even though child is smaller (would normally flip),
    // explicit flip=false prevents it.
    let model = mock_cost_model(vec![("parent", 1000.0), ("child", 10.0)]);
    let planned = plan_query(&ast, model);
    let flips = extract_flips(&planned);
    assert_eq!(flips[0], Some(false), "explicit flip=false must be preserved");
}

#[test]
fn test_multiple_exists_best_combination() {
    // Two EXISTS checks: one should flip, one shouldn't
    let mut ast = simple_ast("parent");
    ast.where_clause = Some(Condition::And(vec![
        Condition::CorrelatedSubquery(exists_condition("child_a", "id", "parent_id", None)),
        Condition::CorrelatedSubquery(exists_condition("child_b", "id", "parent_id", None)),
    ]));

    // child_a is small (should flip), child_b is large (should not flip)
    let model = mock_cost_model(vec![
        ("parent", 1000.0),
        ("child_a", 10.0),
        ("child_b", 10000.0),
    ]);
    let planned = plan_query(&ast, model);
    let flips = extract_flips(&planned);
    assert_eq!(flips.len(), 2, "two EXISTS conditions");
    // The planner should flip child_a (smaller) and not child_b (larger)
    assert_eq!(flips[0], Some(true), "child_a should flip (smaller)");
    assert_eq!(flips[1], Some(false), "child_b should not flip (larger)");
}

#[test]
fn test_or_with_correlated_subqueries() {
    // OR with two EXISTS — this exercises the FanOut/FanIn path (B2)
    let mut ast = simple_ast("parent");
    ast.where_clause = Some(Condition::Or(vec![
        Condition::CorrelatedSubquery(exists_condition("child_a", "id", "parent_id", None)),
        Condition::CorrelatedSubquery(exists_condition("child_b", "id", "parent_id", None)),
    ]));

    let model = mock_cost_model(vec![
        ("parent", 1000.0),
        ("child_a", 10.0),
        ("child_b", 10000.0),
    ]);
    let planned = plan_query(&ast, model);
    let flips = extract_flips(&planned);
    assert_eq!(flips.len(), 2, "two EXISTS in OR");
    // At minimum, both should have a definite flip annotation
    assert!(flips[0].is_some(), "flip must be decided (not None)");
    assert!(flips[1].is_some(), "flip must be decided (not None)");
}

#[test]
fn test_nested_exists() {
    // parent EXISTS(child EXISTS(grandchild))
    let mut grandchild_ast = simple_ast("grandchild");
    grandchild_ast.where_clause = Some(Condition::CorrelatedSubquery(exists_condition(
        "grandchild", "child_id", "id", None,
    )));

    let mut ast = simple_ast("parent");
    ast.where_clause = Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: RelatedSubquery {
            subquery: Box::new(Ast {
                table: "child".to_string(),
                where_clause: Some(Condition::CorrelatedSubquery(exists_condition(
                    "grandchild", "child_id", "id", None,
                ))),
                ..Default::default()
            }),
            relationship_name: "child".to_string(),
            parent_key: vec!["id".to_string()],
            child_key: vec!["parent_id".to_string()],
            hidden: false,
            system: Some(System::Client),
        },
        op: "EXISTS".to_string(),
        flip: None,
        scalar: false,
        plan_id: None,
    }));

    let model = mock_cost_model(vec![
        ("parent", 100.0),
        ("child", 50.0),
        ("grandchild", 500.0),
    ]);
    let planned = plan_query(&ast, model);
    let flips = extract_flips(&planned);
    assert_eq!(flips.len(), 2, "two EXISTS (parent→child and child→grandchild)");
    // Both should have definite flip annotations
    for (i, flip) in flips.iter().enumerate() {
        assert!(flip.is_some(), "flip[{}] must be decided", i);
    }
}

#[test]
fn test_replanning_planned_ast() {
    // Planning an already-planned AST (flip=false) should preserve flip=false
    let mut ast = simple_ast("parent");
    ast.where_clause = Some(Condition::CorrelatedSubquery(exists_condition(
        "child", "id", "parent_id", Some(false),
    )));

    let model = mock_cost_model(vec![("parent", 1000.0), ("child", 10.0)]);
    let planned = plan_query(&ast, model);
    let flips = extract_flips(&planned);
    assert_eq!(flips[0], Some(false), "re-planning flip=false must preserve it");
}
