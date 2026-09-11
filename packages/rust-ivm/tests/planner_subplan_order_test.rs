//! Sub-plans must be traversed in INSERTION order, matching TS.
//!
//! TS's `plans.subPlans` is a plain object (`{[key: string]: Plans}`,
//! planner-builder.ts:39,77) and `plan_recursively` walks it with
//! `Object.values(plans.subPlans)` (:305), which the language guarantees to be
//! insertion order for non-integer-like string keys. Rust used a
//! `std::collections::HashMap`, whose iteration order `RandomState` randomizes.
//!
//! That order is client-visible: `plan_recursively` plans each sub-graph in
//! traversal order, the shared `PlanDebugger` accumulates into an ordered
//! `Vec<Value>`, and those events reach clients through
//! `analyzeQuery --join-plans`. So the events came out in neither TS's order
//! nor a STABLE one — two rust pods analysing the same query disagreed, which
//! defeats diffing them. `IndexMap` fixes it: rust inserts sub-plans while
//! walking `ast.related` (planner_builder.rs), so insertion order IS
//! `ast.related` order, which is what TS walks.
//!
//! It does NOT change which joins flip — each sub-plan is an independent
//! `PlannerGraph` planned in isolation, the cost model's count cache is keyed
//! `(version, table)`, and `apply_plans_to_ast` looks sub-plans up BY KEY.
//! The assertions below are therefore about event ORDER only.
//!
//! Mutation test: restore `sub_plans: HashMap<String, Plans>` (and
//! `HashMap::new()` in `build_plan_graph`) and
//! `sub_plans_are_traversed_in_ast_related_order` fails. With 8 sub-plans a
//! randomized map would have to reproduce insertion order by chance, i.e.
//! 1/8! = 1/40320 — and `the_traversal_order_is_stable_across_plans` fails
//! independently, because two `HashMap`s built in the same thread get
//! different `RandomState` keys and so iterate differently.

use std::cell::RefCell;
use std::rc::Rc;

use rust_ivm::builder::ast::{Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery};
use rust_ivm::ivm::schema::System;
use rust_ivm::planner::{
    AccumulatorDebugger, ConnectionCostModel, CostModelCost, SharedPlanDebugger, plan_query,
};

const SUBPLAN_TABLES: [&str; 8] = ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8"];

fn flat_cost_model() -> ConnectionCostModel {
    Rc::new(
        move |_table: &str,
              _sort: &[(String, String)],
              _filters: Option<&Condition>,
              constraint: Option<&rust_ivm::planner::PlannerConstraint>| {
            CostModelCost {
                startup_cost: 1.0,
                rows: if constraint.is_some() { 1.0 } else { 100.0 },
                fanout: Rc::new(|_cols: &[String]| rust_ivm::planner::FanoutEst {
                    fanout: 1.0,
                    confidence: rust_ivm::planner::Confidence::None,
                }),
            }
        },
    )
}

/// A root query with one `related` subquery per table in `SUBPLAN_TABLES`, in
/// that order. Each subquery carries an `alias`, which is what
/// `build_plan_graph` keys the sub-plan by.
///
/// Each subquery also carries an EXISTS `where_clause`, and that is load-
/// bearing rather than decoration: `PlannerGraph::plan` computes
/// `num_patterns = if n == 0 { 0 } else { 1 << n }` over its FLIPPABLE joins
/// (planner_graph.rs), so a sub-plan with nothing to flip runs zero attempts
/// and emits no debugger events at all. Without the EXISTS, every assertion
/// here would be reading an empty event list — which is exactly what the
/// `!events.is_empty()` guard in `plan_once` caught.
fn multi_subplan_ast() -> Ast {
    let related: Vec<RelatedSubquery> = SUBPLAN_TABLES
        .iter()
        .enumerate()
        .map(|(i, table)| RelatedSubquery {
            subquery: Box::new(Ast {
                table: (*table).to_string(),
                alias: Some(format!("rel{i}")),
                // The flippable join that gives this sub-plan something to plan.
                where_clause: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
                    related: RelatedSubquery {
                        subquery: Box::new(Ast {
                            table: format!("{table}_inner"),
                            ..Default::default()
                        }),
                        relationship_name: format!("inner{i}"),
                        parent_key: vec!["id".to_string()],
                        child_key: vec!["outer_id".to_string()],
                        hidden: false,
                        system: Some(System::Client),
                    },
                    op: "EXISTS".to_string(),
                    flip: None,
                    scalar: false,
                    plan_id: None,
                })),
                ..Default::default()
            }),
            relationship_name: format!("rel{i}"),
            parent_key: vec!["id".to_string()],
            child_key: vec!["root_id".to_string()],
            hidden: false,
            system: Some(System::Client),
        })
        .collect();
    Ast {
        table: "root".to_string(),
        related,
        ..Default::default()
    }
}

/// The order each sub-plan's table is FIRST mentioned by a connection-node
/// cost event — i.e. the order `plan_recursively` planned the sub-graphs.
fn subplan_visit_order(events: &[serde_json::Value]) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    for e in events {
        if e.get("type").and_then(|t| t.as_str()) != Some("node-cost") {
            continue;
        }
        if e.get("nodeType").and_then(|t| t.as_str()) != Some("connection") {
            continue;
        }
        let Some(node) = e.get("node").and_then(|n| n.as_str()) else {
            continue;
        };
        if SUBPLAN_TABLES.contains(&node) && !seen.iter().any(|s| s == node) {
            seen.push(node.to_string());
        }
    }
    seen
}

fn plan_once() -> Vec<String> {
    let dbg = Rc::new(RefCell::new(AccumulatorDebugger::new()));
    let _planned = plan_query(
        &multi_subplan_ast(),
        flat_cost_model(),
        Some(dbg.clone() as SharedPlanDebugger),
    );
    let events = dbg.borrow().events.clone();
    assert!(
        !events.is_empty(),
        "the debugger must have received events — otherwise this test is vacuous"
    );
    subplan_visit_order(&events)
}

#[test]
fn sub_plans_are_traversed_in_ast_related_order() {
    let order = plan_once();
    let expected: Vec<String> = SUBPLAN_TABLES.iter().map(|t| t.to_string()).collect();
    assert_eq!(
        order.len(),
        SUBPLAN_TABLES.len(),
        "every sub-plan must be planned exactly once and emit a connection \
         cost event; got {order:?}"
    );
    assert_eq!(
        order, expected,
        "sub-plans must be planned in `ast.related` order, which is what TS's \
         `Object.values(plans.subPlans)` walks (planner-builder.ts:305)"
    );
}

#[test]
fn the_traversal_order_is_stable_across_plans() {
    let first = plan_once();
    let second = plan_once();
    assert_eq!(
        first, second,
        "two plans of the SAME query must emit the same sub-plan order — an \
         unstable order means two pods disagree on `analyzeQuery --join-plans` \
         output and cannot be diffed"
    );
}
