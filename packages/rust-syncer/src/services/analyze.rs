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
//! DEFERRED vs TS `analyzeQuery` (labeled follow-ups):
//! - `explainQueries` substituted-binding fallback (analyze.ts:113-119):
//!   `sqlitePlans` already carries the execution-time scanstatus plans captured
//!   for scanned tables; the fallback (plans for prepared-but-unscanned queries)
//!   needs a fresh `Database` handle and is deferred (A5).
//! - `joinPlans` planner-debug serialization (B7, see `run_ast`).

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
pub fn analyze_query(
    replica_path: &str,
    app_id: &str,
    ast_json: &str,
    synced_rows: bool,
    vended_rows: bool,
    permissions: Option<serde_json::Value>,
    auth: Option<serde_json::Value>,
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
    };
    run_ast(&mut pipelines, ast_json, &options)
}
