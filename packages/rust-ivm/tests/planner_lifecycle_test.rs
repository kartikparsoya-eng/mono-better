//! Planner lifetime regression tests — the "bulletproof `Rc<RefCell>`" suite.
//!
//! The TS planner graph (planner-graph.ts et al.) is cyclic — every node holds
//! a strong upward `output` back-edge to its consumer — and TS relies on the
//! GC to reclaim the cycles once `planQuery` returns. The Rust port must free
//! the same graph the moment `Plans` drops, with no GC. These tests pin the
//! invariants that make that true, so any future edit that re-introduces a
//! strong cycle, retains a node, or leaves SQLite state behind fails CI:
//!
//! 1. every planner node (incl. sub-plans) is freed when `Plans` drops;
//! 2. an ESCAPED node subtree (not registered in the graph's Vecs — the class
//!    a Drop-based cycle-breaker cannot see) still frees, because back-edges
//!    are `Weak` structurally;
//! 3. planning holds NO strong ref to the snapshot connection afterwards, and
//!    leaves NO SQLite transaction open — a separate connection can
//!    `wal_checkpoint(TRUNCATE)` with busy=0, i.e. the planner path can never
//!    pin the WAL read-mark (the checkpoint-starvation / unbounded-WAL class);
//! 4. a dead snapshot conn degrades (default fanout / no flips) instead of
//!    panicking through the napi boundary.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};

use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::planner::{
    Confidence, ConnectionCostModel, CostModelCost, FanoutEst, Plans, build_plan_graph, plan_query,
};
use rust_ivm::replay::json_to_ast;
use rust_ivm::sqlite::sqlite_cost_model::{
    CostProbeInterrupted, create_sqlite_cost_model, scanstatus_available,
};
use rust_ivm::sqlite::sqlite_stat_fanout::{FanoutSource, SQLiteStatFanout};

/// A cost model that touches no database — isolates graph-topology tests from
/// SQLite entirely.
fn mock_model() -> ConnectionCostModel {
    Rc::new(|_table, _sort, _filters, constraint| CostModelCost {
        startup_cost: 1.0,
        rows: if constraint.is_some() { 1.0 } else { 100.0 },
        fanout: Rc::new(|_cols: &[String]| FanoutEst {
            fanout: 1.0,
            confidence: Confidence::None,
        }),
    })
}

/// Representative AST exercising every node type: root connection, AND, an OR
/// of two EXISTS (fan-out/fan-in), a nested EXISTS inside one branch, and a
/// `related` subquery (sub-plan recursion).
fn rich_ast_json() -> serde_json::Value {
    serde_json::json!({
        "table": "parent",
        "limit": 10,
        "orderBy": [["id", "asc"]],
        "where": {
            "type": "and",
            "conditions": [
                {"type": "simple", "op": "=", "left": {"type": "column", "name": "id"},
                 "right": {"type": "literal", "value": 1}},
                {"type": "or", "conditions": [
                    {"type": "correlatedSubquery", "op": "EXISTS", "related": {
                        "correlation": {"parentField": ["id"], "childField": ["parent_id"]},
                        "subquery": {"table": "child_a", "alias": "child_a", "where": {
                            "type": "correlatedSubquery", "op": "EXISTS", "related": {
                                "correlation": {"parentField": ["id"], "childField": ["a_id"]},
                                "subquery": {"table": "grandchild", "alias": "grandchild"}
                            }
                        }}
                    }},
                    {"type": "correlatedSubquery", "op": "EXISTS", "related": {
                        "correlation": {"parentField": ["id"], "childField": ["parent_id"]},
                        "subquery": {"table": "child_b", "alias": "child_b"}
                    }}
                ]}
            ]
        },
        "related": [
            {"correlation": {"parentField": ["id"], "childField": ["parent_id"]},
             "subquery": {"table": "child_a", "alias": "rel_a"}}
        ]
    })
}

/// Downgrade every node the graph owns (recursively through sub-plans) so the
/// test can prove they all free when `Plans` drops.
#[allow(clippy::type_complexity)]
fn collect_node_weaks(
    plans: &Plans,
    joins: &mut Vec<Weak<RefCell<rust_ivm::planner::join::PlannerJoin>>>,
    conns: &mut Vec<Weak<RefCell<rust_ivm::planner::connection::PlannerConnection>>>,
    fan_ins: &mut Vec<Weak<RefCell<rust_ivm::planner::fan_in::PlannerFanIn>>>,
    fan_outs: &mut Vec<Weak<RefCell<rust_ivm::planner::fan_out::PlannerFanOut>>>,
) {
    joins.extend(plans.plan.joins.iter().map(Rc::downgrade));
    conns.extend(plans.plan.connections.iter().map(Rc::downgrade));
    fan_ins.extend(plans.plan.fan_ins.iter().map(Rc::downgrade));
    fan_outs.extend(plans.plan.fan_outs.iter().map(Rc::downgrade));
    for sub in plans.sub_plans.values() {
        collect_node_weaks(sub, joins, conns, fan_ins, fan_outs);
    }
}

#[test]
fn every_planner_node_frees_when_plans_drop() {
    let mut ast = json_to_ast(&rich_ast_json());

    let mut joins = Vec::new();
    let mut conns = Vec::new();
    let mut fan_ins = Vec::new();
    let mut fan_outs = Vec::new();

    let mut plans = build_plan_graph(&mut ast, mock_model(), true, None);
    // Run the full planning pass so back-edges are exercised (FO→FI BFS,
    // constraint propagation, snapshot/restore) before teardown.
    fn plan_all(p: &mut Plans) {
        for sub in p.sub_plans.values_mut() {
            plan_all(sub);
        }
        p.plan.plan();
    }
    plan_all(&mut plans);

    collect_node_weaks(&plans, &mut joins, &mut conns, &mut fan_ins, &mut fan_outs);
    // The rich AST must actually produce each node type or the test is vacuous
    // (3 joins: the two OR-branch EXISTS + the nested EXISTS).
    assert!(joins.len() >= 3, "expected joins, got {}", joins.len());
    assert!(
        conns.len() >= 5,
        "expected connections, got {}",
        conns.len()
    );
    assert!(
        !fan_ins.is_empty() && !fan_outs.is_empty(),
        "expected OR fan nodes"
    );

    drop(plans);

    assert!(
        joins.iter().all(|w| w.upgrade().is_none()),
        "a PlannerJoin outlived its graph — strong cycle reintroduced"
    );
    assert!(
        conns.iter().all(|w| w.upgrade().is_none()),
        "a PlannerConnection outlived its graph — strong cycle reintroduced"
    );
    assert!(
        fan_ins.iter().all(|w| w.upgrade().is_none()),
        "a PlannerFanIn outlived its graph — strong cycle reintroduced"
    );
    assert!(
        fan_outs.iter().all(|w| w.upgrade().is_none()),
        "a PlannerFanOut outlived its graph — strong cycle reintroduced"
    );
}

/// The class the old Drop-based cycle-breaker could NOT handle: nodes wired to
/// each other but never registered in a `PlannerGraph`'s Vecs (a future builder
/// bug, or nodes escaping the graph's lifetime). With `Weak` back-edges the
/// parent→child edge is the only strong edge, so dropping the local Rcs frees
/// both nodes. With strong back-edges this test leaks (child.output keeps the
/// join alive, join.child keeps the connection alive → cycle).
#[test]
fn escaped_unregistered_nodes_cannot_form_a_cycle() {
    use rust_ivm::planner::connection::PlannerConnection;
    use rust_ivm::planner::join::PlannerJoin;
    use rust_ivm::planner::node::{JoinType, PlannerNode};

    let parent = Rc::new(RefCell::new(PlannerConnection::new(
        "parent",
        mock_model(),
        vec![],
        None,
        true,
        None,
        None,
    )));
    let child = Rc::new(RefCell::new(PlannerConnection::new(
        "child",
        mock_model(),
        vec![],
        None,
        false,
        None,
        Some(1),
    )));
    let join = Rc::new(RefCell::new(PlannerJoin::new(
        PlannerNode::Connection(parent.clone()),
        PlannerNode::Connection(child.clone()),
        [("id".to_string(), None)].into_iter().collect(),
        [("parent_id".to_string(), None)].into_iter().collect(),
        true,
        0,
        JoinType::Semi,
    )));
    // Wire the upward back-edges exactly as the builder does.
    parent
        .borrow_mut()
        .set_output(PlannerNode::Join(join.clone()));
    child
        .borrow_mut()
        .set_output(PlannerNode::Join(join.clone()));

    let wp = Rc::downgrade(&parent);
    let wc = Rc::downgrade(&child);
    let wj = Rc::downgrade(&join);

    drop(join);
    drop(parent);
    drop(child);

    assert!(wj.upgrade().is_none(), "escaped join leaked (cycle)");
    assert!(
        wp.upgrade().is_none(),
        "escaped parent connection leaked (cycle)"
    );
    assert!(
        wc.upgrade().is_none(),
        "escaped child connection leaked (cycle)"
    );
}

fn clean(path: &str) {
    for p in [
        path.to_string(),
        format!("{path}-wal"),
        format!("{path}-shm"),
    ] {
        let _ = std::fs::remove_file(p);
    }
}

/// (busy, log_frames, checkpointed_frames); busy == 1 means a reader is
/// pinning the WAL snapshot and the checkpoint could not complete.
fn checkpoint_truncate(conn: &rusqlite::Connection) -> (i64, i64, i64) {
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    })
    .unwrap()
}

fn specs() -> HashMap<String, HashMap<String, ColumnType>> {
    let mut cols = HashMap::new();
    cols.insert("id".to_string(), ColumnType::Number { optional: false });
    cols.insert(
        "parent_id".to_string(),
        ColumnType::Number { optional: false },
    );
    let mut t = HashMap::new();
    t.insert("parent".to_string(), cols.clone());
    t.insert("child_a".to_string(), cols.clone());
    t.insert("child_b".to_string(), cols.clone());
    t.insert("grandchild".to_string(), {
        let mut g = HashMap::new();
        g.insert("id".to_string(), ColumnType::Number { optional: false });
        g.insert("a_id".to_string(), ColumnType::Number { optional: false });
        g
    });
    t
}

/// THE WAL regression test: a full `plan_query` with the production scanstatus
/// cost model must (a) hold no strong conn ref afterwards, (b) leave the
/// connection in autocommit (no open read transaction), and (c) not block a
/// separate connection's `wal_checkpoint(TRUNCATE)` — i.e. the planner path
/// can never pin the WAL read-mark. This is the mechanism behind unbounded
/// WAL growth (checkpoint starvation), encoded as a test.
#[test]
fn planning_leaves_no_txn_and_never_blocks_checkpoint() {
    if !scanstatus_available() {
        eprintln!("skipping: SQLite lacks SQLITE_ENABLE_STMT_SCANSTATUS");
        return;
    }
    let db_path = "/tmp/rust-ivm-planner-wal-pin.db";
    clean(db_path);

    // Seed a WAL-mode db with the schema + stats the probes will read.
    {
        let c = rusqlite::Connection::open(db_path).unwrap();
        c.execute_batch(
            "PRAGMA journal_mode=wal;
             CREATE TABLE parent (id INTEGER PRIMARY KEY, parent_id INTEGER);
             CREATE TABLE child_a (id INTEGER PRIMARY KEY, parent_id INTEGER, a_id INTEGER);
             CREATE TABLE child_b (id INTEGER PRIMARY KEY, parent_id INTEGER);
             CREATE TABLE grandchild (id INTEGER PRIMARY KEY, a_id INTEGER);",
        )
        .unwrap();
        for i in 0..500i64 {
            c.execute(
                "INSERT INTO parent (id, parent_id) VALUES (?, ?)",
                rusqlite::params![i, i % 7],
            )
            .unwrap();
            c.execute(
                "INSERT INTO child_a (id, parent_id, a_id) VALUES (?, ?, ?)",
                rusqlite::params![i, i % 11, i % 5],
            )
            .unwrap();
        }
        c.execute_batch("ANALYZE;").unwrap();
        checkpoint_truncate(&c);
    }

    // The "snapshot" connection the planner probes run on.
    let conn = Rc::new(RefCell::new(rusqlite::Connection::open(db_path).unwrap()));
    let baseline_strong = Rc::strong_count(&conn);

    let mut ast = json_to_ast(&rich_ast_json());
    // Planner-shaped AST: strip the harness-only `related` (its child columns
    // aren't in this schema) — the WHERE tree is what drives probes.
    ast.related.clear();

    for _ in 0..20 {
        let model = create_sqlite_cost_model(conn.clone(), specs()).unwrap();
        let planned = plan_query(&ast, model);
        assert!(planned.where_clause.is_some());
    }

    // (a) no strong conn refs retained by planning
    assert_eq!(
        Rc::strong_count(&conn),
        baseline_strong,
        "planning retained a strong ref to the snapshot connection"
    );
    // (b) no transaction left open on the probe connection
    assert!(
        conn.borrow().is_autocommit(),
        "planning left a transaction open on the snapshot connection"
    );

    // (c) a separate writer can fully checkpoint while the probe conn is idle.
    let writer = rusqlite::Connection::open(db_path).unwrap();
    for i in 500..600i64 {
        writer
            .execute(
                "INSERT INTO parent (id, parent_id) VALUES (?, ?)",
                rusqlite::params![i, i % 7],
            )
            .unwrap();
    }
    let (busy, log, ckpt) = checkpoint_truncate(&writer);
    assert_eq!(
        busy, 0,
        "planner connection pinned the WAL read-mark (checkpoint starvation)"
    );
    assert_eq!(log, ckpt, "checkpoint could not copy all frames");

    drop(writer);
    drop(conn);
    clean(db_path);
}

/// Dead snapshot conn during a fanout estimate → default fanout, no panic.
#[test]
fn stat_fanout_degrades_to_default_when_conn_dropped() {
    let conn = Rc::new(RefCell::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    let fanout = SQLiteStatFanout::new(conn.clone());
    drop(conn); // snapshot rotated away and closed

    let r = fanout.get_fanout("t", &["a".to_string()]);
    assert_eq!(r.source, FanoutSource::Default);
}

/// Dead snapshot conn during a cost probe → typed `CostProbeInterrupted`
/// unwind (the payload `plan_ast` catches to degrade to "no flips"), never a
/// bare panic that would tear down the client group.
#[test]
fn cost_probe_on_dead_conn_unwinds_with_typed_payload() {
    if !scanstatus_available() {
        eprintln!("skipping: SQLite lacks SQLITE_ENABLE_STMT_SCANSTATUS");
        return;
    }
    let conn = Rc::new(RefCell::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    conn.borrow()
        .execute_batch("CREATE TABLE parent (id INTEGER PRIMARY KEY, parent_id INTEGER);")
        .unwrap();
    let model = create_sqlite_cost_model(conn.clone(), specs()).unwrap();
    drop(conn); // snapshot rotated away and closed

    let payload = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        model("parent", &[], None, None)
    })) {
        Ok(_) => panic!("probe on a dead conn must unwind"),
        Err(p) => p,
    };
    assert!(
        payload.downcast::<CostProbeInterrupted>().is_ok(),
        "unwind payload must be CostProbeInterrupted so plan_ast degrades to no flips"
    );
}
