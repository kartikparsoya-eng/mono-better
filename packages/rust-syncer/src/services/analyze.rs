//! Port of `packages/zero-cache/src/services/analyze.ts` — `analyzeQuery`.
//!
//! Opens a fresh, read-only analysis engine over the replica (TS `new
//! Database(config.replica.file)` + per-table `TableSource`s), hydrates the AST
//! through [`run_ast`](super::run_ast::run_ast) with a `Debug` delegate, and
//! returns the [`AnalyzeQueryResult`]. The analysis engine is entirely separate
//! from the live view-syncer pipelines — a throwaway that is dropped when this
//! returns.
//!
//! This is a SYNC function that builds the `!Send` IVM engine on the calling
//! thread; the async inspect handler runs it via `spawn_blocking`.
//!
//! `createSQLiteCostModel` planner wiring IS in place — `IvmPipelines::init`
//! builds the engine with the SQLite cost model when `enable_query_planner` is
//! set (the default), so the analysis reflects the plan production executes
//! (mirrors analyze.ts:52 `config.enableQueryPlanner`).
//!
//! `explainQueries` substituted-binding fallback (analyze.ts:112-119) IS ported:
//! after `run_ast`, `sqlitePlans` is filled for any query SQLite prepared but did
//! not execute (so no scanstatus EXPLAIN was captured); execution-time plans win.
//!
//! `joinPlans` planner-debug serialization (B7) IS ported (see `run_ast` +
//! `rust_ivm::planner::planner_debug`). The whole `AnalyzeQueryResult` is pinned
//! to the real TS `analyzeQuery` by `tests/analyze_query_golden_test.rs`.

use crate::protocol::analyze_query_result::AnalyzeQueryResult;

use super::run_ast::{RunAstOptions, run_ast};
use super::view_syncer::pipeline_driver::IvmPipelines;

/// Port of TS `analyzeQuery` (analyze.ts:24). `synced_rows`/`vended_rows` are the
/// `body.options` flags (TS defaults: `syncedRows = true`, `vendedRows = false`).
///
/// `permissions` is the compiled permissions config loaded by the inspect
/// handler for legacy queries (TS `loadPermissions`); when present the AST is
/// transformed for read-permissions. `auth` is the decoded JWT claims of the
/// requesting connection (TS `ctx.auth?.type === 'jwt' ? ctx.auth : undefined`).
#[allow(clippy::too_many_arguments)]
pub fn analyze_query(
    replica_path: &str,
    app_id: &str,
    ast_json: &str,
    synced_rows: bool,
    vended_rows: bool,
    permissions: Option<serde_json::Value>,
    auth: Option<serde_json::Value>,
    join_plans: bool,
) -> Result<AnalyzeQueryResult, String> {
    // TS `computeZqlSpecs(lc, db, ...)` + building `TableSource`s per table.
    let specs = crate::compute_table_specs_from_path(replica_path)
        .map_err(|e| format!("analyze-query: reading replica specs: {e}"))?;

    let mut pipelines = IvmPipelines::new();
    pipelines
        .init(specs, Some(replica_path), app_id)
        .map_err(|e| format!("analyze-query: engine init: {e}"))?;

    // TS analyze.ts:65 — `applyPermissions: permissions !== undefined`.
    let options = RunAstOptions {
        apply_permissions: permissions.is_some(),
        auth: auth.as_ref(),
        permissions: permissions.as_ref(),
        synced_rows,
        vended_rows,
        join_plans,
    };
    let mut result = run_ast(&mut pipelines, ast_json, &options)?;

    // TS analyze.ts:112-119 — fill `sqlitePlans` for any query SQLite prepared
    // but did NOT execute (so scanStatus captured no EXPLAIN), using the
    // substituted-binding fallback. Execution-time plans (captured via
    // scanstatus in the fetch path) WIN; the fallback only fills gaps. Rust owns
    // the analysis engine's connections internally, so it opens a fresh read
    // handle over the same replica — the plan is identical (same schema + stat
    // tables). Keyed on the vended SQLs in `readRowCountsByQuery`.
    let read_counts = result.read_row_counts_by_query.clone().unwrap_or_default();
    if !read_counts.is_empty() {
        // `explain_queries` reads only the SQL keys; convert u64 → usize counts.
        let counts_for_explain: rust_ivm::sqlite::explain_queries::RowCountsBySource = read_counts
            .into_iter()
            .map(|(src, by_sql)| {
                (
                    src,
                    by_sql
                        .into_iter()
                        .map(|(sql, c)| (sql, c as usize))
                        .collect(),
                )
            })
            .collect();
        let db = rust_ivm::sqlite::db::Database::new(replica_path)
            .map_err(|e| format!("analyze-query: open replica for explain: {e}"))?;
        let db = std::rc::Rc::new(std::cell::RefCell::new(db));
        let fallback = rust_ivm::sqlite::explain_queries::explain_queries(&counts_for_explain, &db);
        result.sqlite_plans = Some(merge_explain_fallback(result.sqlite_plans.take(), fallback));
    }

    Ok(result)
}

/// Merge the `explainQueries` fallback plans into the captured plans, keeping the
/// captured (execution-time / scanstatus) plan for any query present in both.
/// Port of the merge loop in TS `analyze.ts:113-118` (`if (!captured[query])`).
fn merge_explain_fallback(
    captured: Option<std::collections::HashMap<String, Vec<String>>>,
    fallback: std::collections::HashMap<String, Vec<String>>,
) -> std::collections::HashMap<String, Vec<String>> {
    let mut captured = captured.unwrap_or_default();
    for (query, plan) in fallback {
        // Captured (execution-time) plans win; the fallback only fills gaps.
        captured.entry(query).or_insert(plan);
    }
    captured
}

#[cfg(test)]
mod tests {
    use super::merge_explain_fallback;
    use std::collections::HashMap;

    /// NON-VACUOUS: the fallback must FILL queries missing from the captured
    /// plans but must NOT overwrite a captured (execution-time) plan. Reverting
    /// the merge to `insert` (overwrite) flips the "captured wins" assertion;
    /// dropping the merge entirely drops the fallback-only query.
    #[test]
    fn merge_keeps_captured_and_fills_gaps() {
        let captured = HashMap::from([("q1".to_string(), vec!["CAPTURED".to_string()])]);
        let fallback = HashMap::from([
            ("q1".to_string(), vec!["FALLBACK".to_string()]),
            ("q2".to_string(), vec!["FALLBACK2".to_string()]),
        ]);
        let merged = merge_explain_fallback(Some(captured), fallback);
        // Captured plan for q1 is preserved (NOT overwritten by the fallback).
        assert_eq!(merged.get("q1"), Some(&vec!["CAPTURED".to_string()]));
        // The fallback fills q2, which the captured plans lacked.
        assert_eq!(merged.get("q2"), Some(&vec!["FALLBACK2".to_string()]));
    }

    /// With no captured plans (e.g. a build without SQLITE_ENABLE_STMT_SCANSTATUS),
    /// the fallback is the sole source of plans.
    #[test]
    fn merge_uses_fallback_when_no_captured_plans() {
        let fallback = HashMap::from([("q1".to_string(), vec!["FALLBACK".to_string()])]);
        let merged = merge_explain_fallback(None, fallback);
        assert_eq!(merged.get("q1"), Some(&vec!["FALLBACK".to_string()]));
    }
}
