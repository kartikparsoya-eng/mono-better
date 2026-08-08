//! Runtime planner entry (DESIGN: `#planAstForRust`, steps 3-4).
//!
//! `plan_ast_flips` runs the ported plan graph against a cost model backed by
//! the actor's pinned snapshot connection, and returns the ordered `flip`
//! decisions the TS driver applies to its own AST (no AST re-serialization).
//!
//! ## Cost models
//! The DEFAULT production model is the scanstatus/stat-fanout model
//! (`crate::sqlite::sqlite_cost_model::create_sqlite_cost_model`) — the exact
//! port of TS `createSQLiteCostModel`: filter-aware probe SQL prepared on the
//! snapshot connection, `SQLITE_SCANSTAT_EST` row estimates, stat4/stat1
//! fanout. It requires `SQLITE_ENABLE_STMT_SCANSTATUS` in the linked SQLite
//! (true for the prod image, the local wal2 build, and macOS system SQLite).
//!
//! `create_snapshot_cost_model` here is the LEGACY row-count model
//! (filter-blind `COUNT(*)`; constrained read ≈ 1 row; fanout 1.0/none),
//! selectable via `RUST_IVM_PLANNER_COST_MODEL=count` as an escape hatch and
//! still used by the mock-cost oracle tests.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use crate::builder::ast::{Ast, Condition};
use crate::ivm::data::Value;
use crate::planner::{Confidence, ConnectionCostModel, CostModelCost, FanoutEst, plan_query};

/// Version-keyed row-count cache: `(snapshot_version, table -> COUNT(*))`.
/// Shared across `plan_ast` calls so a connection-init burst of `addQuery`s (all
/// at the same snapshot version) reuses one `COUNT(*)` per table instead of
/// re-counting per query. Auto-invalidates when the version bumps (an advance
/// changed the data) — no explicit advance hook needed.
pub type PlanCountCache = Rc<RefCell<(String, HashMap<String, f64>)>>;

/// A cost model backed by a live SQLite connection (the pinned snapshot). Table
/// row counts are read once and memoised for the duration of one plan.
pub fn create_snapshot_cost_model(conn: Rc<RefCell<rusqlite::Connection>>) -> ConnectionCostModel {
    // Fresh per-call cache (tests + callers without a shared cache).
    cost_model_with_cache(conn, Rc::new(RefCell::new((String::new(), HashMap::new()))))
}

/// Like [`create_snapshot_cost_model`] but reuses `cache` across calls, keyed by
/// `version`. On a version change the cached counts are dropped (the advance
/// changed table sizes); within one version every `plan_ast` reuses them. Keeps
/// the planner's `COUNT(*)`s off the hot path when a client subscribes to many
/// queries at once: `COUNT(*)` runs at most once per (table, version) rather
/// than once per table per `addQuery`. Matters in prod where the planner is on.
pub fn create_snapshot_cost_model_cached(
    conn: Rc<RefCell<rusqlite::Connection>>,
    version: &str,
    cache: PlanCountCache,
) -> ConnectionCostModel {
    {
        let mut c = cache.borrow_mut();
        if c.0 != version {
            c.0 = version.to_string();
            c.1.clear();
        }
    }
    cost_model_with_cache(conn, cache)
}

fn cost_model_with_cache(
    conn: Rc<RefCell<rusqlite::Connection>>,
    cache: PlanCountCache,
) -> ConnectionCostModel {
    Rc::new(
        move |table: &str,
              _sort: &[(String, String)],
              _filters: Option<&Condition>,
              constraint: Option<&BTreeMap<String, Option<Value>>>| {
            let rows = if constraint.is_some() {
                // Indexed key seek — a handful of rows; model as ~1.
                1.0
            } else {
                let mut c = cache.borrow_mut();
                *c.1.entry(table.to_string())
                    .or_insert_with(|| row_count(&conn.borrow(), table).unwrap_or(1000.0))
            };
            CostModelCost {
                startup_cost: 1.0,
                rows,
                fanout: Rc::new(|_cols: &[String]| FanoutEst {
                    fanout: 1.0,
                    confidence: Confidence::None,
                }),
            }
        },
    )
}

fn row_count(conn: &rusqlite::Connection, table: &str) -> Option<f64> {
    let sql = format!("SELECT COUNT(*) FROM \"{}\"", table.replace('"', "\"\""));
    conn.query_row(&sql, [], |r| r.get::<_, i64>(0))
        .ok()
        .map(|n| n as f64)
}

/// Plan `ast_json` (TS-shape) with `cost_model` and return the ordered `flip`
/// decisions (canonical traversal — see [`flip_order`]). The TS driver walks its
/// own AST in the same order and sets `flip` per position.
pub fn plan_ast_flips(
    ast_json: &serde_json::Value,
    cost_model: ConnectionCostModel,
) -> Vec<Option<bool>> {
    let ast = crate::replay::json_to_ast(ast_json);
    let planned = plan_query(&ast, cost_model);
    flip_order(&planned)
}

/// Ordered `flip` extraction: WHERE conditions pre-order (recursing into each
/// correlated subquery's own where), then the `related` subqueries in order.
/// The TS driver's `applyFlips` MUST use this exact order.
pub fn flip_order(ast: &Ast) -> Vec<Option<bool>> {
    let mut flips = Vec::new();
    if let Some(ref where_clause) = ast.where_clause {
        flip_order_condition(where_clause, &mut flips);
    }
    for csq in &ast.related {
        flips.append(&mut flip_order(&csq.subquery));
    }
    flips
}

fn flip_order_condition(condition: &Condition, flips: &mut Vec<Option<bool>>) {
    match condition {
        Condition::Simple(_) => {}
        Condition::CorrelatedSubquery(csq) => {
            flips.push(csq.flip);
            if let Some(ref sub_where) = csq.related.subquery.where_clause {
                flip_order_condition(sub_where, flips);
            }
        }
        Condition::And(conds) | Condition::Or(conds) => {
            for c in conds {
                flip_order_condition(c, flips);
            }
        }
    }
}
