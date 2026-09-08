//! Planner census regression — OWN test binary so the process-global
//! `live_count` counters aren't raced by unrelated tests (each integration
//! test file compiles to its own process).
//!
//! Pins two invariants end to end, including the PANIC path:
//!
//! 1. After any number of `plan_query` calls — completed or unwound mid-plan
//!    by a `CostProbeInterrupted` (the watchdog-interrupt path `plan_ast`
//!    catches in prod) — the live-instance census for planner graphs and
//!    planner nodes returns to zero. A nonzero census is exactly the
//!    "plan graph leaks per planAst" bug class, and the same counters are
//!    printed by the napi teardown census in prod logs (`pgraph=`/`pnode=`),
//!    so a future regression is visible in the field, not just here.
//! 2. The unwound plan also releases its strong refs to the snapshot
//!    connection (`Rc::strong_count` restored) — the conn-retention class
//!    that skips `Snapshot::drop`'s explicit close.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::Ordering;

use rust_ivm::live_count::{PLANNER_GRAPH, PLANNER_NODE};
use rust_ivm::planner::{Confidence, ConnectionCostModel, CostModelCost, FanoutEst, plan_query};
use rust_ivm::replay::json_to_ast;
use rust_ivm::sqlite::sqlite_cost_model::CostProbeInterrupted;

fn ast_json() -> serde_json::Value {
    serde_json::json!({
        "table": "parent",
        "where": {
            "type": "or",
            "conditions": [
                {"type": "correlatedSubquery", "op": "EXISTS", "related": {
                    "correlation": {"parentField": ["id"], "childField": ["parent_id"]},
                    "subquery": {"table": "child_a", "alias": "child_a"}
                }},
                {"type": "correlatedSubquery", "op": "EXISTS", "related": {
                    "correlation": {"parentField": ["id"], "childField": ["parent_id"]},
                    "subquery": {"table": "child_b", "alias": "child_b"}
                }}
            ]
        }
    })
}

/// Cost model that counts invocations; panics with the typed interrupt payload
/// once `panic_after` calls have been served (0 = never). Holds a strong clone
/// of `conn` like the legacy count model does, so the strong-count assertion
/// also covers closure-captured conns.
fn counting_model(
    conn: Rc<RefCell<rusqlite::Connection>>,
    calls: Rc<RefCell<usize>>,
    panic_after: usize,
) -> ConnectionCostModel {
    Rc::new(move |_table, _sort, _filters, constraint| {
        let _keepalive = &conn; // strong capture, released with the model
        let mut n = calls.borrow_mut();
        *n += 1;
        if panic_after > 0 && *n > panic_after {
            std::panic::panic_any(CostProbeInterrupted(
                "test: simulated watchdog interrupt mid-plan".to_string(),
            ));
        }
        CostModelCost {
            startup_cost: 1.0,
            rows: if constraint.is_some() { 1.0 } else { 100.0 },
            fanout: Rc::new(|_cols: &[String]| FanoutEst {
                fanout: 1.0,
                confidence: Confidence::None,
            }),
        }
    })
}

#[test]
fn census_returns_to_zero_after_completed_and_unwound_plans() {
    let ast = json_to_ast(&ast_json());
    let conn = Rc::new(RefCell::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    let baseline_strong = Rc::strong_count(&conn);

    // Completed plans.
    for _ in 0..10 {
        let calls = Rc::new(RefCell::new(0));
        let model = counting_model(conn.clone(), calls, 0);
        let planned = plan_query(&ast, model, None);
        assert!(planned.where_clause.is_some());
    }
    assert_eq!(
        PLANNER_GRAPH.load(Ordering::Relaxed),
        0,
        "graph census after completed plans"
    );
    assert_eq!(
        PLANNER_NODE.load(Ordering::Relaxed),
        0,
        "node census after completed plans"
    );
    assert_eq!(Rc::strong_count(&conn), baseline_strong);

    // Plans unwound mid-planning by the typed interrupt (prod watchdog path):
    // panic at increasing depths so teardown-under-unwind is exercised from
    // several borrow states.
    for panic_after in 1..8 {
        let calls = Rc::new(RefCell::new(0));
        let model = counting_model(conn.clone(), calls.clone(), panic_after);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            plan_query(&ast, model, None)
        }));
        if result.is_ok() {
            // Plan finished before reaching the trigger depth — fine.
            continue;
        }
        assert_eq!(
            PLANNER_GRAPH.load(Ordering::Relaxed),
            0,
            "graph census after unwind at depth {panic_after}"
        );
        assert_eq!(
            PLANNER_NODE.load(Ordering::Relaxed),
            0,
            "node census after unwind at depth {panic_after}"
        );
    }
    assert_eq!(
        Rc::strong_count(&conn),
        baseline_strong,
        "an unwound plan retained a strong ref to the snapshot connection"
    );
}
