//! `planQuery`'s `planDebugger` parameter (planner-builder.ts:311) reaches the
//! planner's event emitters.
//!
//! TS threads `planDebugger` explicitly: `planQuery(ast, model, planDebugger)`
//! -> `planRecursively(plans, planDebugger)` -> `plans.plan.plan(planDebugger)`
//! -> every node's `estimateCost` / `propagateConstraints`. Rust threads the
//! same parameter through the first three and installs it as the node sink in
//! `PlannerGraph::plan` (see the `planner_debug` module doc).
//!
//! This is NON-VACUOUS for that threading: drop the parameter anywhere along
//! the chain (e.g. restore `plans.plan.plan()` / `plan_query(&ast, model)`) and
//! `events` is empty, so every assertion below fails. It is the only test that
//! covers the emitters at all — they were ported but had no caller-visible
//! assertion, which is how a silently-unthreaded debugger could have shipped.

use std::cell::RefCell;
use std::rc::Rc;

use rust_ivm::builder::ast::{Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery};
use rust_ivm::ivm::schema::System;
use rust_ivm::planner::{
    AccumulatorDebugger, ConnectionCostModel, CostModelCost, SharedPlanDebugger, plan_query,
    serialize_plan_debug_events,
};

/// Same deterministic mock as `planner_diff_test.rs`: fixed rows per table, and
/// a constrained read costs ~1 row (an indexed key seek), which is what lets a
/// flip ever win.
fn mock_cost_model(table_costs: Vec<(&str, f64)>) -> ConnectionCostModel {
    let costs: std::collections::HashMap<String, f64> = table_costs
        .into_iter()
        .map(|(t, c)| (t.to_string(), c))
        .collect();
    Rc::new(
        move |table: &str,
              _sort: &[(String, String)],
              _filters: Option<&Condition>,
              constraint: Option<&rust_ivm::planner::PlannerConstraint>| {
            let base = *costs.get(table).unwrap_or(&100.0);
            let rows = if constraint.is_some() { 1.0 } else { base };
            CostModelCost {
                startup_cost: 1.0,
                rows,
                fanout: Rc::new(|_cols: &[String]| rust_ivm::planner::FanoutEst {
                    fanout: 1.0,
                    confidence: rust_ivm::planner::Confidence::None,
                }),
            }
        },
    )
}

fn exists_ast() -> Ast {
    let mut ast = Ast {
        table: "parent".to_string(),
        ..Default::default()
    };
    ast.where_clause = Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: RelatedSubquery {
            subquery: Box::new(Ast {
                table: "child".to_string(),
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
    ast
}

fn types_of(events: &[serde_json::Value]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| e.get("type").and_then(|t| t.as_str()).map(str::to_string))
        .collect()
}

#[test]
fn plan_query_debugger_parameter_reaches_the_emitters() {
    let dbg = Rc::new(RefCell::new(AccumulatorDebugger::new()));
    let planned = plan_query(
        &exists_ast(),
        mock_cost_model(vec![("parent", 1000.0), ("child", 10.0)]),
        Some(dbg.clone() as SharedPlanDebugger),
    );
    // Sanity: the planner really ran (10-row child vs 1000-row parent flips).
    assert_eq!(
        rust_ivm::planner::flip_order(&planned),
        vec![Some(true)],
        "mock costs chosen so the EXISTS flips; if this changes the event \
         assertions below are still valid but no longer exercise a real flip"
    );

    let events = dbg.borrow().events.clone();
    let types = types_of(&events);
    assert!(
        !events.is_empty(),
        "debugger parameter never reached a sink"
    );

    // The four graph-level emissions TS makes per `plan` call
    // (planner-graph.ts:283 attempt-start, :308 constraints-propagated,
    // :340 plan-complete, :383 best-plan-selected) plus the per-node ones.
    for expected in [
        "attempt-start",
        "constraints-propagated",
        "plan-complete",
        "best-plan-selected",
        "node-cost",
        "node-constraint",
    ] {
        assert!(
            types.iter().any(|t| t == expected),
            "no `{expected}` event; got {types:?}"
        );
    }

    // 2^1 flip patterns for one flippable join => two attempts, numbered 0,1.
    let attempts: Vec<i64> = events
        .iter()
        .filter(|e| e["type"] == "attempt-start")
        .filter_map(|e| e["attemptNumber"].as_i64())
        .collect();
    assert_eq!(attempts, vec![0, 1], "2^1 patterns enumerated in order");

    // `AccumulatorDebugger.log` stamps node events with the current attempt
    // (planner-debug.ts:148) — so node events must carry one, and never a
    // number past the last attempt.
    let node_attempts: Vec<i64> = events
        .iter()
        .filter(|e| e["type"] == "node-cost" || e["type"] == "node-constraint")
        .map(|e| {
            e["attemptNumber"]
                .as_i64()
                .expect("node events are stamped with attemptNumber")
        })
        .collect();
    assert!(!node_attempts.is_empty());
    assert!(node_attempts.iter().all(|a| *a == 0 || *a == 1));

    // Connections and joins both report a cost estimate.
    let node_types: Vec<&str> = events
        .iter()
        .filter(|e| e["type"] == "node-cost")
        .filter_map(|e| e["nodeType"].as_str())
        .collect();
    assert!(node_types.contains(&"connection"), "{node_types:?}");
    assert!(node_types.contains(&"join"), "{node_types:?}");

    // `serializePlanDebugEvents` drops the internal `planSnapshot`
    // (planner-debug.ts:serializeEvent).
    for ev in serialize_plan_debug_events(&events) {
        assert!(
            ev.get("planSnapshot").is_none(),
            "planSnapshot leaked: {ev}"
        );
    }
}

#[test]
fn plan_query_without_a_debugger_emits_nothing_and_plans_the_same() {
    // No debugger => the hot path pays only the thread-local Option check, and
    // the plan is byte-identical to the instrumented one (TS: `planDebugger` is
    // diagnostic, never an input to a cost decision).
    let model = || mock_cost_model(vec![("parent", 1000.0), ("child", 10.0)]);
    let quiet = plan_query(&exists_ast(), model(), None);

    let dbg = Rc::new(RefCell::new(AccumulatorDebugger::new()));
    let loud = plan_query(
        &exists_ast(),
        model(),
        Some(dbg.clone() as SharedPlanDebugger),
    );

    assert_eq!(
        rust_ivm::planner::flip_order(&quiet),
        rust_ivm::planner::flip_order(&loud),
        "instrumenting the planner changed its decisions"
    );
    assert!(!dbg.borrow().events.is_empty());
}

#[test]
fn nested_subquery_plans_report_to_the_same_debugger() {
    // `planRecursively` (planner-builder.ts:300) recurses into every subPlan
    // WITH the debugger; a version that forgot to pass it down would emit only
    // the root graph's events.
    let mut ast = exists_ast();
    // parent -> related[child] -> where EXISTS(grandchild): the `related`
    // subquery gets its own Plans entry, planned by the recursion.
    ast.related = vec![RelatedSubquery {
        subquery: Box::new({
            let mut child = Ast {
                table: "child".to_string(),
                // `build_plan_graph` only builds a sub-plan for a `related`
                // subquery that has an alias (planner-builder.ts:335).
                alias: Some("child".to_string()),
                ..Default::default()
            };
            child.where_clause = Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
                related: RelatedSubquery {
                    subquery: Box::new(Ast {
                        table: "grandchild".to_string(),
                        ..Default::default()
                    }),
                    relationship_name: "grandchild".to_string(),
                    parent_key: vec!["id".to_string()],
                    child_key: vec!["child_id".to_string()],
                    hidden: false,
                    system: Some(System::Client),
                },
                op: "EXISTS".to_string(),
                flip: None,
                scalar: false,
                plan_id: None,
            }));
            child
        }),
        relationship_name: "child".to_string(),
        parent_key: vec!["id".to_string()],
        child_key: vec!["parent_id".to_string()],
        hidden: false,
        system: Some(System::Client),
    }];

    let dbg = Rc::new(RefCell::new(AccumulatorDebugger::new()));
    plan_query(
        &ast,
        mock_cost_model(vec![
            ("parent", 1000.0),
            ("child", 10.0),
            ("grandchild", 5.0),
        ]),
        Some(dbg.clone() as SharedPlanDebugger),
    );

    let events = dbg.borrow().events.clone();
    let tables: std::collections::HashSet<String> = events
        .iter()
        .filter(|e| e["type"] == "node-cost" && e["nodeType"] == "connection")
        .filter_map(|e| e["node"].as_str().map(str::to_string))
        .collect();
    // Node names are `<table>#<id>`; assert every table in the query reported.
    for table in ["parent", "child", "grandchild"] {
        assert!(
            tables.iter().any(|t| t.starts_with(table)),
            "sub-plan for `{table}` never reported to the debugger; got {tables:?}"
        );
    }
}
