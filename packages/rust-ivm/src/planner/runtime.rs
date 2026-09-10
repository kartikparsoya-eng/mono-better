//! Runtime planner entry (DESIGN: `#planAstForRust`, steps 3-4).
//!
//! `plan_ast_flips` runs the ported plan graph against a cost model backed by
//! the actor's pinned snapshot connection, and returns the ordered `flip`
//! decisions the TS driver applies to its own AST (no AST re-serialization).
//!
//! ## Cost models
//! The production model — selected by `Engine::ensure_cost_model`
//! (engine/mod.rs), which is what the rust-syncer's `plan_ast` path actually
//! uses — is the scanstatus/stat-fanout model
//! (`crate::sqlite::sqlite_cost_model::create_sqlite_cost_model`) — the exact
//! port of TS `createSQLiteCostModel`: filter-aware probe SQL prepared on the
//! snapshot connection, `SQLITE_SCANSTAT_EST` row estimates, stat4/stat1
//! fanout. It requires `SQLITE_ENABLE_STMT_SCANSTATUS` in the linked SQLite
//! (true for the prod image, the local wal2 build, and macOS system SQLite).
//!
//! `create_snapshot_cost_model` here is the LEGACY row-count model
//! (filter-blind `COUNT(*)`; constrained read ≈ 1 row; fanout 1.0/none). As of
//! 2026-08-31 it is **test-only** (mock-cost oracle differentials): the engine
//! plans with the scanstatus model or runs UNPLANNED, mirroring TS. The
//! rust-only auto-fallback + `RUST_IVM_PLANNER_COST_MODEL=count` env that once
//! silently reached THIS model in prod — the cost-model gap behind the
//! 2026-08-29 144s flipped-join `tickets` hydrate — were removed (option-b).
//! `engine_planner_wiring_test.rs` pins the engine selection (scanstatus, or
//! unplanned when specs/scanstatus are unavailable).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::builder::ast::{Ast, Condition};
use crate::planner::{Confidence, ConnectionCostModel, CostModelCost, FanoutEst, plan_query};

/// Version-keyed row-count cache: `(snapshot_version, table -> COUNT(*))`.
/// Auto-invalidates when the version bumps (an advance changed the data) — no
/// explicit advance hook needed.
///
/// NOT actually shared between calls, despite what this comment used to claim
/// ("shared across `plan_ast` calls so a connection-init burst … reuses one
/// `COUNT(*)` per table"): the only constructor,
/// [`create_snapshot_cost_model`], builds a FRESH cache per call, and
/// `cost_model_with_cache` has no other caller. The sharing never happens.
/// Harmless because this whole model is test-only (see
/// [`create_snapshot_cost_model`]) — but a claim that describes behaviour the
/// code does not have is a stale claim, and this model's earlier PROD
/// reachability is the cost-model gap behind the 2026-08-29 144 s `tickets`
/// hydrate.
type PlanCountCache = Rc<RefCell<(String, HashMap<String, f64>)>>;

/// LEGACY filter-blind `COUNT(*)` cost model — **test-only reference**, NOT
/// wired into the engine. TS has no such model; prod plans with the scanstatus
/// model (`sqlite_cost_model.rs`) or runs unplanned when it's unavailable
/// (`Engine::ensure_cost_model`, mirroring TS `if (costModel)`). Retained only
/// so the differential tests (`sqlite_cost_model_test`, `planner_runtime_test`)
/// can prove the scanstatus model plans DIFFERENTLY from this one — i.e. pin
/// the 2026-08-29 `tickets` mis-flip fix. The rust-only auto-fallback + the
/// `RUST_IVM_PLANNER_COST_MODEL=count` env that once reached this in prod were
/// removed 2026-08-31 (option-b: no divergent prod cost model).
///
/// `#[doc(hidden)]` so the test-only status is machine-visible and not just
/// prose. (`#[cfg(test)]` would be wrong: the differential tests are
/// INTEGRATION tests, which link the lib as an ordinary dependency and so
/// cannot see `#[cfg(test)]` items.) Reachable today only from
/// `tests/sqlite_cost_model_test.rs` and `tests/planner_runtime_test.rs`.
#[doc(hidden)]
pub fn create_snapshot_cost_model(conn: Rc<RefCell<rusqlite::Connection>>) -> ConnectionCostModel {
    // Fresh per-call cache (tests + callers without a shared cache).
    cost_model_with_cache(conn, Rc::new(RefCell::new((String::new(), HashMap::new()))))
}

fn cost_model_with_cache(
    conn: Rc<RefCell<rusqlite::Connection>>,
    cache: PlanCountCache,
) -> ConnectionCostModel {
    Rc::new(
        move |table: &str,
              _sort: &[(String, String)],
              _filters: Option<&Condition>,
              constraint: Option<&crate::planner::planner_constraint::PlannerConstraint>| {
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
/// decisions (canonical traversal — see [`flip_order`]).
///
/// This used to say "the TS driver walks its own AST in the same order and sets
/// `flip` per position". That driver was the napi bridge, DELETED in
/// `a5e502ad9` — the contract had no counterparty left. Reachable today only
/// from `tests/sqlite_cost_model_test.rs` and `tests/planner_runtime_test.rs`,
/// which use it to prove the scanstatus model plans differently from the legacy
/// COUNT(*) one; the engine plans through `Engine::plan_ast` instead.
#[doc(hidden)]
pub fn plan_ast_flips(
    ast_json: &serde_json::Value,
    cost_model: ConnectionCostModel,
) -> Vec<Option<bool>> {
    let ast = crate::replay::json_to_ast(ast_json);
    let planned = plan_query(&ast, cost_model, None);
    flip_order(&planned)
}

/// Ordered `flip` extraction: WHERE conditions pre-order (recursing into each
/// correlated subquery's own where), then the `related` subqueries in order.
///
/// Unlike its callers above, this one IS live: `Engine::plan_ast` uses it for
/// the `[rust-ivm][PLAN] flips=…` trace line (engine/mod.rs:653, gated on
/// `RUST_IVM_PERF_TRACE`) and `Engine::planned_flips_for_test` returns it. The
/// order is therefore what those two report, and what the differential tests
/// compare positionally. It used to say "the TS driver's `applyFlips` MUST use
/// this exact order" — that driver was the deleted napi bridge, so the MUST had
/// no counterparty; the flips rust applies to the AST are set by the planner
/// itself, not handed to a TS caller.
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
