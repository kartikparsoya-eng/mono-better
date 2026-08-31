//! Port of `packages/zero-cache/src/services/run-ast.ts` — `runAst`.
//!
//! Hydrates a single query AST against a throwaway analysis engine with a
//! `Debug` delegate attached (TS `host.debug = new Debug()`), collecting the
//! synced rows and reading the vended-row / db-scan / plan stats back off the
//! delegate into an [`AnalyzeQueryResult`]. This is the consumer that gives
//! `record_nvisit`/`record_explain` (wired at the TableSource fetch path) a
//! purpose — `dbScansByQuery` / `sqlitePlans` come from it.
//!
//! DEFERRED vs TS `runAst` (labeled, tracked as follow-ups; each needs a
//! sub-port rust does not yet have):
//! - `applyPermissions` / `afterPermissions`: the read-permission transform +
//!   `astToZQL`/`formatOutput` ZQL pretty-printer. Analyze currently runs on the
//!   AST as given; a `warnings` entry records this.
//! - `joinPlans`: the planner-debug event serializer (`AccumulatorDebugger` /
//!   `serializePlanDebugEvents`).
//! - client->server name mapping (`mapAST` when `!isTransformed`): the caller
//!   passes a server-named AST (named queries are already transformed).

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use rust_ivm::builder::debug_delegate::{Debug, SharedDebug};
use rust_ivm::ivm::data::{Row, Value};

use crate::protocol::analyze_query_result::{AnalyzeQueryResult, RowsByQuery, RowsBySource};

use super::view_syncer::pipeline_driver::IvmPipelines;

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
    synced_rows: bool,
    vended_rows: bool,
) -> Result<AnalyzeQueryResult, String> {
    // DEFERRED: read-permission transform (TS `applyPermissions`) — analyze runs
    // on the AST as given. Surface it the way TS surfaces a missing-auth analyze.
    let warnings: Vec<String> = vec![
        "Rust analyze-query does not yet apply read-permissions; results reflect \
         the query without permission filtering."
            .to_string(),
    ];

    // TS `host.debug = new Debug()` — run_ast owns the delegate and reads it back.
    let debug: SharedDebug = Debug::new_shared();

    let start = Instant::now();
    let rows = pipelines.hydrate_analyze(ast_json, debug.clone())?;
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;

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
        if synced_rows {
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
    let read_rows = if vended_rows {
        Some(rows_by_source_to_json(d.get_vended_rows()))
    } else {
        None
    };
    drop(d);

    Ok(AnalyzeQueryResult {
        warnings,
        synced_rows: if synced_rows {
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
        after_permissions: None,
        vended_row_counts: None, // deprecated; TS runAst does not set it
        vended_rows: None,       // deprecated
        sqlite_plans: Some(sqlite_plans),
        read_rows,
        read_row_counts_by_query: Some(read_row_counts_by_query),
        read_row_count: Some(read_row_count),
        db_scans_by_query: Some(db_scans_by_query),
        join_plans: None,
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
