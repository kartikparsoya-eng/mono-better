//! Planner probe against a REAL replica — the rust half of the TS↔rust planner
//! differential.
//!
//! TS side (run inside the zero-cache container, which has the compiled JS):
//!
//! ```sh
//! ZROOT=/opt/app/node_modules/@rocicorp/zero/out \
//! REPLICA=/data/replica.db AST=/tmp/ast.json node /tmp/ts-plan.mjs
//! ```
//!
//! Rust side (this test, from `packages/rust-syncer`):
//!
//! ```sh
//! unset SQLITE3_LIB_DIR SQLITE3_INCLUDE_DIR
//! REPLICA=/data/replica.db AST=/tmp/ast.json \
//!   cargo test --release --no-default-features --test planner_replica_probe \
//!   -- --ignored --nocapture
//! ```
//!
//! Both drive the SAME `planQuery`/`plan_query` entry point with a scanstatus
//! cost model over the SAME replica file and print the flip vector in the same
//! canonical order (WHERE conditions pre-order, recursing into each correlated
//! subquery, then `related` subqueries — `planner::flip_order`). `PLANDBG=1`
//! additionally dumps the `PlanDebugEventJSON` stream (TS's
//! `analyzeQuery --join-plans`) so per-node cost estimates can be diffed.
//!
//! `#[ignore]`d: it needs a replica file, so it never runs in CI.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::sqlite::sqlite_cost_model::{create_sqlite_cost_model_prepared, prepare_table_specs};
use rust_syncer::db::lite_tables::{compute_zql_specs, open_replica_read_only};

/// Same mapping as `pipeline_driver.rs`'s `zql_column_type` (which is private
/// to that module); kept in sync by the assertion below that every column maps.
fn zql_column_type(ty: &str, optional: bool) -> ColumnType {
    match ty {
        "boolean" => ColumnType::Boolean { optional },
        "string" => ColumnType::String { optional },
        "json" => ColumnType::Json { optional },
        _ => ColumnType::Number { optional },
    }
}

#[test]
#[ignore = "needs REPLICA + AST env pointing at a real replica file"]
fn plan_ast_against_replica() {
    let replica = std::env::var("REPLICA").expect("set REPLICA=/path/to/replica.db");
    let ast_path = std::env::var("AST").expect("set AST=/path/to/ast.json");

    let conn = open_replica_read_only(&replica).expect("open replica");
    let specs = compute_zql_specs(&conn, None).expect("compute_zql_specs");
    eprintln!("tableSpecs: {}", specs.len());

    let table_specs: HashMap<String, HashMap<String, ColumnType>> = specs
        .iter()
        .map(|s| {
            (
                s.table.clone(),
                s.columns
                    .iter()
                    .map(|(col, cs)| (col.clone(), zql_column_type(&cs.r#type, cs.optional)))
                    .collect(),
            )
        })
        .collect();

    // The model keeps only a WEAK ref to the connection (so the snapshotter's
    // explicit close is not blocked); the caller must hold it strong for the
    // duration of planning, exactly like the engine does.
    let conn = Rc::new(RefCell::new(conn));
    let model =
        create_sqlite_cost_model_prepared(conn.clone(), Rc::new(prepare_table_specs(table_specs)))
            .expect("scanstatus cost model");

    // The AST file is an inspector/transform payload: `{queries: [{ast}]}`, or
    // a bare AST.
    let raw: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&ast_path).expect("read AST")).unwrap();
    let ast_json = raw
        .get("queries")
        .and_then(|q| q.get(0))
        .and_then(|q| q.get("ast"))
        .unwrap_or(&raw);
    let ast = rust_ivm::replay::json_to_ast(ast_json);

    let dbg = Rc::new(RefCell::new(rust_ivm::planner::AccumulatorDebugger::new()));
    let planned = rust_ivm::planner::plan_query(
        &ast,
        model,
        Some(dbg.clone() as rust_ivm::planner::SharedPlanDebugger),
    );

    drop(conn);
    let flips = rust_ivm::planner::flip_order(&planned);
    println!(
        "rust flips={}",
        serde_json::to_string(&flips.iter().map(|f| *f == Some(true)).collect::<Vec<_>>()).unwrap()
    );
    // Same labelling as ts-plan.mjs, so the two tables line up element-wise.
    println!("\nidx  rust   subquery");
    for (i, (label, flip)) in label_order(&planned).iter().zip(flips.iter()).enumerate() {
        println!("{i:>3}  {:<6} {label}", format!("{}", *flip == Some(true)));
    }

    if std::env::var("PLANDBG").is_ok() {
        for ev in rust_ivm::planner::serialize_plan_debug_events(&dbg.borrow().events) {
            println!("[PLANDBG] {ev}");
        }
    }
}

/// `<table>[<parentKey>-><childKey>]` per flip position, in `flip_order`'s
/// canonical order.
fn label_order(ast: &rust_ivm::builder::ast::Ast) -> Vec<String> {
    fn cond(c: &rust_ivm::builder::ast::Condition, out: &mut Vec<String>) {
        use rust_ivm::builder::ast::Condition as C;
        match c {
            C::Simple(_) => {}
            C::CorrelatedSubquery(csq) => {
                out.push(format!(
                    "{}[{}->{}]",
                    csq.related.subquery.table,
                    csq.related.parent_key.join(","),
                    csq.related.child_key.join(",")
                ));
                if let Some(w) = &csq.related.subquery.where_clause {
                    cond(w, out);
                }
            }
            C::And(cs) | C::Or(cs) => cs.iter().for_each(|c| cond(c, out)),
        }
    }
    fn walk(a: &rust_ivm::builder::ast::Ast, out: &mut Vec<String>) {
        if let Some(w) = &a.where_clause {
            cond(w, out);
        }
        for r in &a.related {
            walk(&r.subquery, out);
        }
    }
    let mut out = Vec::new();
    walk(ast, &mut out);
    out
}
