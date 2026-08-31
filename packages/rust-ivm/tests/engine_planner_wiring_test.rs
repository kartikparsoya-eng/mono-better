//! Engine-level planner WIRING guard: `Engine::plan_ast` must plan with the
//! scanstatus cost model — what TS always plans with
//! (`PipelineDriver.#ensureCostModelExistsIfEnabled` →
//! `createSQLiteCostModel(db, #tableSpecs)` → `buildPipeline` → `planQuery`,
//! pipeline-driver.ts:430-436 / builder.ts:140) — not the legacy filter-blind
//! COUNT(*) escape hatch.
//!
//! The model-LEVEL divergence is pinned by
//! `sqlite_cost_model_test::selective_exists_flips_where_count_model_does_not`;
//! this test pins the ENGINE-level selection. The 2026-08-29 prod incident was
//! exactly this seam: the scanstatus port existed but `Engine::plan_ast` still
//! built the COUNT model, which prices a constrained fetch at ~1 row (fanout
//! 1.0), so the planner flipped a join whose real parent-side fanout was tens
//! of thousands of rows — 44.8s per flipped-join batch fetch on a limit-10
//! `tickets` query (144s total) that TS plans as an unflipped semi-join.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rust_ivm::engine::Engine;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::sqlite::sqlite_cost_model::scanstatus_available;

/// Same skewed shape as sqlite_cost_model_test: parent 100 rows; child 10_000
/// rows (~100 per parent bucket) with a high-cardinality indexed `email`
/// column. A selective `email = 'e42'` EXISTS matches ~5 child rows, so the
/// scanstatus model flips (scan 5 children, seek parents) while the
/// filter-blind COUNT model sees child=10_000 > parent=100 and refuses.
fn seed() -> Rc<RefCell<rusqlite::Connection>> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER,
            email TEXT
        );
        CREATE INDEX child_parent ON child (parent_id);
        CREATE INDEX child_email ON child (email);
        "#,
    )
    .unwrap();
    {
        let mut ins_p = conn
            .prepare("INSERT INTO parent (id, name) VALUES (?, ?)")
            .unwrap();
        for i in 0..100 {
            ins_p
                .execute(rusqlite::params![i, format!("p{i}")])
                .unwrap();
        }
        let mut ins_c = conn
            .prepare("INSERT INTO child (id, parent_id, email) VALUES (?, ?, ?)")
            .unwrap();
        for i in 0..10_000i64 {
            // ~5 rows per email value → selective equality.
            ins_c
                .execute(rusqlite::params![i, i % 100, format!("e{}", i / 5)])
                .unwrap();
        }
    }
    conn.execute_batch("ANALYZE;").unwrap();
    Rc::new(RefCell::new(conn))
}

fn specs() -> HashMap<String, HashMap<String, ColumnType>> {
    let s = |o| ColumnType::String { optional: o };
    let n = |o| ColumnType::Number { optional: o };
    HashMap::from([
        (
            "parent".to_string(),
            HashMap::from([("id".to_string(), n(false)), ("name".to_string(), s(true))]),
        ),
        (
            "child".to_string(),
            HashMap::from([
                ("id".to_string(), n(false)),
                ("parent_id".to_string(), n(true)),
                ("email".to_string(), s(true)),
            ]),
        ),
    ])
}

fn selective_exists_ast() -> serde_json::Value {
    serde_json::json!({
        "table": "parent",
        "where": {
            "type": "correlatedSubquery",
            "op": "EXISTS",
            "related": {
                "correlation": {"parentField": ["id"], "childField": ["parent_id"]},
                "subquery": {
                    "table": "child",
                    "alias": "child",
                    "where": {
                        "type": "simple",
                        "op": "=",
                        "left": {"type": "column", "name": "email"},
                        "right": {"type": "literal", "value": "e42"}
                    }
                }
            }
        }
    })
}

fn primary_keys() -> HashMap<String, Vec<String>> {
    HashMap::from([
        ("parent".to_string(), vec!["id".to_string()]),
        ("child".to_string(), vec!["id".to_string()]),
    ])
}

/// One test fn (not several) because it toggles a process-global env var —
/// cargo's threaded test runner must not interleave another planner test
/// between the set/remove.
#[test]
fn engine_plans_with_scanstatus_model_not_count() {
    if !scanstatus_available() {
        eprintln!(
            "SKIP: linked SQLite lacks SQLITE_ENABLE_STMT_SCANSTATUS; \
             engine wiring test needs the scanstatus model"
        );
        return;
    }
    let conn = seed();
    let ast = selective_exists_ast();

    // 1. Conn + specs (the production pipeline_driver wiring) → the engine
    //    must produce the SCANSTATUS decision: flip the selective EXISTS.
    //    Pre-wiring (COUNT model) this returns [Some(false)] — the exact
    //    divergence behind the 2026-08-29 144s tickets hydrate.
    let mut eng = Engine::new(primary_keys());
    eng.set_cost_model_conn(conn.clone());
    eng.set_cost_model_table_specs(specs());
    assert_eq!(
        eng.planned_flips_for_test(&ast),
        vec![Some(true)],
        "Engine::plan_ast must plan with the scanstatus cost model (TS \
         createSQLiteCostModel parity); [Some(false)] means the filter-blind \
         COUNT(*) model is deciding flips again"
    );

    // 2. Conn WITHOUT specs must run UNPLANNED — TS has no fallback cost model
    //    (builder.ts:140 `if (costModel)`), and the old COUNT(*) fallback that
    //    returned [Some(false)] here was the removed rust-only divergence
    //    (option-b, 2026-08-31). None = no flip assigned = unplanned, never a
    //    panic, never a different-cost mis-flip.
    let mut eng_nospecs = Engine::new(primary_keys());
    eng_nospecs.set_cost_model_conn(conn.clone());
    assert_eq!(
        eng_nospecs.planned_flips_for_test(&ast),
        vec![None],
        "conn without specs must run UNPLANNED (TS parity), not the COUNT model"
    );

    // 3. No conn at all → no planning (flips untouched: None).
    let mut eng_none = Engine::new(primary_keys());
    assert_eq!(
        eng_none.planned_flips_for_test(&ast),
        vec![None],
        "without a cost-model conn the AST must pass through unplanned"
    );
}
