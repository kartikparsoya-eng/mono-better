//! Port of `packages/zero-cache/src/services/run-ast.ts` — `runAst`.
//!
//! Hydrates a single query AST against a throwaway analysis engine with a
//! `Debug` delegate attached (TS `host.debug = new Debug()`), collecting the
//! synced rows and reading the vended-row / db-scan / plan stats back off the
//! delegate into an [`AnalyzeQueryResult`]. This is the consumer that gives
//! `record_nvisit`/`record_explain` (wired at the TableSource fetch path) a
//! purpose — `dbScansByQuery` / `sqlitePlans` come from it.
//!
//! `applyPermissions` (run-ast.ts:74-90) IS ported: when [`RunAstOptions::apply_permissions`]
//! is set, the AST is transformed for read-permissions (`transformAndHashQuery`)
//! before hydration, so the analysis reflects exactly the rows a client may read.
//!
//! DEFERRED vs TS `runAst` (labeled, tracked as follow-ups; each needs a
//! sub-port rust does not yet have):
//! - `afterPermissions` (B6): the `astToZQL`/`formatOutput` ZQL pretty-printer
//!   that renders the transformed AST back to ZQL text. The transform itself
//!   runs; only the human-readable rendering is pending.
//! - `joinPlans` (B7): the planner-debug event serializer (`AccumulatorDebugger`
//!   / `serializePlanDebugEvents`).
//!
//! NOT NEEDED on this path: client->server name mapping (`mapAST` when
//! `!isTransformed`). TS `analyzeQuery` is the sole caller and always passes
//! `isTransformed=true` (analyze.ts:62), so the `mapAST` branch is dead here.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::Instant;

use rust_ivm::builder::debug_delegate::{Debug, SharedDebug};
use rust_ivm::ivm::data::{Row, Value};
use rust_ivm::planner::{AccumulatorDebugger, serialize_plan_debug_events, with_plan_debugger};

use crate::auth::read_authorizer::transform_and_hash_query;
use crate::protocol::analyze_query_result::{AnalyzeQueryResult, RowsByQuery, RowsBySource};

use super::view_syncer::pipeline_driver::IvmPipelines;

/// Port of TS `RunAstOptions` (run-ast.ts:34). The rust analysis engine
/// (`IvmPipelines`) owns `db` / `host` / `tableSpecs` / `costModel` internally
/// (wired by `IvmPipelines::init`), so this struct carries only the fields
/// `run_ast` itself consumes: the read-permission transform inputs and the
/// row-return flags. There is no `clientToServerMapper` field — the sole caller
/// (`analyze_query`) always hands `run_ast` an already server-named AST (TS
/// `analyzeQuery` calls `runAst(..., isTransformed=true, ...)` unconditionally,
/// analyze.ts:62), so TS's `mapAST` branch is dead on this path.
#[derive(Default)]
pub struct RunAstOptions<'a> {
    /// TS `applyPermissions`: transform the AST for read-permissions before
    /// hydrating. Set iff `permissions` is present (analyze.ts:65).
    pub apply_permissions: bool,
    /// TS `auth`: the decoded JWT claims (`authData`) the permission rules bind
    /// their static parameters against. `None` when unauthenticated.
    pub auth: Option<&'a serde_json::Value>,
    /// TS `permissions`: the compiled permissions config.
    pub permissions: Option<&'a serde_json::Value>,
    /// TS `syncedRows`: collect the synced rows per table into the result.
    pub synced_rows: bool,
    /// TS `vendedRows`: collect the vended (scanned) rows per table.
    pub vended_rows: bool,
    /// TS `planDebugger` presence (analyze.ts:55 `joinPlans ? new
    /// AccumulatorDebugger() : undefined`): when set, an `AccumulatorDebugger` is
    /// installed for the plan pass and its serialized events fill `joinPlans`.
    pub join_plans: bool,
}

/// Convert an IVM `Value` to its JSON wire form — the analog of TS serializing a
/// `Row` value for `syncedRows`/`readRows`. Mirrors `ivm_value_to_json` in
/// `rust-ivm/src/bin/server.rs`.
pub(crate) fn ivm_value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::F64(n) => serde_json::Number::from_f64(*n)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Str(s) => serde_json::Value::String(s.to_string()),
        // A `json` column stores its text; re-parse it into a JSON value, falling
        // back to the raw string (matching server.rs).
        Value::Json(s) => {
            serde_json::from_str(s).unwrap_or_else(|_| serde_json::Value::String(s.to_string()))
        }
    }
}

/// Convert an IVM `Row` (`Arc<map>`) to a JSON object.
pub(crate) fn ivm_row_to_json(row: &Row) -> serde_json::Value {
    let mut map = serde_json::Map::with_capacity(row.len());
    for (k, v) in row.iter() {
        map.insert(k.clone(), ivm_value_to_json(v));
    }
    serde_json::Value::Object(map)
}

/// Convert the debug delegate's `RowsBySource` (IVM rows) into the JSON-valued
/// `RowsBySource` of the wire result.
fn rows_by_source_to_json(src: &rust_ivm::builder::debug_delegate::RowsBySource) -> RowsBySource {
    src.iter()
        .map(|(table, by_query)| {
            let by_query_json: RowsByQuery = by_query
                .iter()
                .map(|(sql, rows)| (sql.clone(), rows.iter().map(ivm_row_to_json).collect()))
                .collect();
            (table.clone(), by_query_json)
        })
        .collect()
}

/// Port of TS `runAst` (run-ast.ts:48). `pipelines` is a throwaway analysis
/// engine already `init`-ed over the replica (see `analyze_query`).
pub fn run_ast(
    pipelines: &mut IvmPipelines,
    ast_json: &str,
    options: &RunAstOptions,
) -> Result<AnalyzeQueryResult, String> {
    let mut warnings: Vec<String> = Vec::new();
    let mut after_permissions: Option<String> = None;

    // Port of run-ast.ts:74-90 — apply read-permissions to the AST before
    // hydrating so the analysis reflects exactly the rows a client is allowed to
    // read. `apply_permissions` is set iff permissions are present (TS
    // analyze.ts:65 `permissions !== undefined`).
    let transformed_ast_json: String;
    let hydrate_ast: &str = if options.apply_permissions {
        // TS: `const auth = options.auth; if (!auth) result.warnings.push(...)`.
        let auth_data: serde_json::Value = match options.auth {
            Some(a) => a.clone(),
            None => {
                warnings.push(
                    "No auth data provided. Permission rules will compare to `NULL` \
                     wherever an auth data field is referenced."
                        .to_string(),
                );
                serde_json::json!({})
            }
        };
        // TS `must(permissions)` — applyPermissions implies permissions present.
        let permissions = options
            .permissions
            .ok_or_else(|| "run_ast: applyPermissions set without permissions".to_string())?;
        let ast: serde_json::Value =
            serde_json::from_str(ast_json).map_err(|e| format!("run_ast: parse AST: {e}"))?;
        // read-authorizer.ts `transformAndHashQuery(..., internalQuery=false)`.
        let (transformed, _hash) = transform_and_hash_query(&ast, permissions, &auth_data, false);
        // TS run-ast.ts:89 — `result.afterPermissions = await formatOutput(
        //   ast.table + astToZQL(ast))`. `formatOutput` (oxfmt) is not ported;
        //   we use the raw `ast_to_zql` string (oxfmt's own on-error fallback).
        let table = transformed
            .get("table")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        after_permissions = Some(format!(
            "{table}{}",
            crate::ast_to_zql::ast_to_zql(&transformed)
        ));
        transformed_ast_json = serde_json::to_string(&transformed)
            .map_err(|e| format!("run_ast: serialize transformed AST: {e}"))?;
        &transformed_ast_json
    } else {
        ast_json
    };

    // TS `host.debug = new Debug()` — run_ast owns the delegate and reads it back.
    let debug: SharedDebug = Debug::new_shared();

    // TS analyze.ts:55 — `planDebugger = joinPlans ? new AccumulatorDebugger()
    // : undefined`, passed down to `plan`. Rust installs it via a thread-local
    // sink around the hydrate (which drives `plan_query`); see planner_debug.rs.
    let plan_debugger: Option<Rc<RefCell<AccumulatorDebugger>>> = if options.join_plans {
        Some(Rc::new(RefCell::new(AccumulatorDebugger::new())))
    } else {
        None
    };

    let start = Instant::now();
    let rows = match &plan_debugger {
        Some(dbg) => with_plan_debugger(dbg.clone(), || {
            pipelines.hydrate_analyze(hydrate_ast, debug.clone())
        })?,
        None => pipelines.hydrate_analyze(hydrate_ast, debug.clone())?,
    };
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    // TS analyze.ts:121-123 — `if (planDebugger) result.joinPlans =
    // serializePlanDebugEvents(planDebugger.events)`.
    let join_plans: Option<Vec<serde_json::Value>> =
        plan_debugger.map(|dbg| serialize_plan_debug_events(&dbg.borrow().events));

    // Dedup by table + row (TS `seenByTable`), counting synced rows and (when
    // requested) collecting them per table.
    let mut seen: HashSet<String> = HashSet::new();
    let mut synced_by_table: RowsByQuery = HashMap::new();
    let mut synced_row_count: u64 = 0;
    for (table, row) in &rows {
        let row_json = ivm_row_to_json(row);
        let key = format!(
            "{table}.{}",
            serde_json::to_string(&row_json).unwrap_or_default()
        );
        if seen.contains(&key) {
            continue; // skip duplicates (TS)
        }
        seen.insert(key);
        synced_row_count += 1;
        if options.synced_rows {
            synced_by_table
                .entry(table.clone())
                .or_default()
                .push(row_json);
        }
    }

    // Read the stats back off the delegate (TS run-ast.ts:181-193).
    let d = debug.borrow();
    let read_row_counts_by_query = d.get_vended_row_counts().clone();
    let read_row_count: u64 = read_row_counts_by_query
        .values()
        .flat_map(|by_sql| by_sql.values())
        .copied()
        .sum();
    let db_scans_by_query = d.get_nvisit_counts().clone();
    let sqlite_plans = d.get_sqlite_plans().clone();
    let read_rows = if options.vended_rows {
        Some(rows_by_source_to_json(d.get_vended_rows()))
    } else {
        None
    };
    drop(d);

    Ok(AnalyzeQueryResult {
        warnings,
        synced_rows: if options.synced_rows {
            Some(synced_by_table)
        } else {
            None
        },
        synced_row_count,
        // TS uses performance.now() absolutes; rust has no cheap wall clock here,
        // so start is 0 and end == elapsed. `elapsed` (the field clients use) is
        // exact.
        start: 0.0,
        end: elapsed,
        elapsed: Some(elapsed),
        after_permissions,
        vended_row_counts: None, // deprecated; TS runAst does not set it
        vended_rows: None,       // deprecated
        sqlite_plans: Some(sqlite_plans),
        read_rows,
        read_row_counts_by_query: Some(read_row_counts_by_query),
        read_row_count: Some(read_row_count),
        db_scans_by_query: Some(db_scans_by_query),
        join_plans,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Each IVM `Value` variant maps to the JSON shape the client expects for a
    /// `syncedRows`/`readRows` row value — mirrors server.rs `ivm_value_to_json`.
    /// This is the serialization every analyze row value flows through
    /// (`ivm_row_to_json` is a thin per-column wrapper over it).
    #[test]
    fn value_to_json_covers_every_variant() {
        assert_eq!(ivm_value_to_json(&Value::Null), serde_json::Value::Null);
        assert_eq!(
            ivm_value_to_json(&Value::Bool(true)),
            serde_json::json!(true)
        );
        assert_eq!(ivm_value_to_json(&Value::F64(1.5)), serde_json::json!(1.5));
        assert_eq!(
            ivm_value_to_json(&Value::Str(Arc::from("hi"))),
            serde_json::json!("hi")
        );
        // A `json` column's stored text re-parses into a JSON value.
        assert_eq!(
            ivm_value_to_json(&Value::Json(Arc::from(r#"{"a":1}"#))),
            serde_json::json!({"a": 1})
        );
    }
}
