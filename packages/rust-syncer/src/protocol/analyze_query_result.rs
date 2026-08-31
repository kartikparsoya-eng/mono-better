//! Port of `packages/zero-protocol/src/analyze-query-result.ts` — the
//! `analyze-query` inspector RPC result. serde equivalents of the valita
//! schemas (`analyzeQueryResultSchema` + the row-count/row maps it references).
//!
//! Serialized to JSON and sent to the client, which validates it against
//! `analyzeQueryResultSchema`; field names must match (camelCase) and every
//! `.optional()` field must be OMITTED when absent (not `null`), so the
//! optionals carry `skip_serializing_if = "Option::is_none"`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `Record<SQL, number>` — port of TS `RowCountsByQuery`
/// (analyze-query-result.ts:7).
pub type RowCountsByQuery = HashMap<String, u64>;
/// `Record<SourceName, RowCountsByQuery>` — port of TS `RowCountsBySource`
/// (analyze-query-result.ts:10).
pub type RowCountsBySource = HashMap<String, RowCountsByQuery>;
/// `Record<SQL, Row[]>` — port of TS `RowsByQuery` (analyze-query-result.ts:13).
/// A `Row` is a JSON object on the wire (`rowSchema`).
pub type RowsByQuery = HashMap<String, Vec<Value>>;
/// `Record<SourceName, RowsByQuery>` — port of TS `RowsBySource`
/// (analyze-query-result.ts:16).
pub type RowsBySource = HashMap<String, RowsByQuery>;

/// Port of TS `AnalyzeQueryResult` (`analyzeQueryResultSchema`,
/// analyze-query-result.ts:163). Field order and names mirror the schema.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyzeQueryResult {
    pub warnings: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub synced_rows: Option<RowsByQuery>,

    pub synced_row_count: u64,

    pub start: f64,
    /// @deprecated Use start + elapsed instead.
    pub end: f64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_permissions: Option<String>,

    /// @deprecated Use readRowCountsByQuery.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vended_row_counts: Option<RowCountsBySource>,

    /// @deprecated Use readRows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vended_rows: Option<RowsBySource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sqlite_plans: Option<HashMap<String, Vec<String>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_rows: Option<RowsBySource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_row_counts_by_query: Option<RowCountsBySource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_row_count: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_scans_by_query: Option<RowCountsBySource>,

    /// `PlanDebugEventJSON[]` — the planner-debug event stream (opt-in via
    /// `joinPlans`). Kept as raw JSON values; the planner-debug serializer is
    /// deferred (see run_ast).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub join_plans: Option<Vec<Value>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The schema requires: present required fields (`warnings`,
    /// `syncedRowCount`, `start`, `end`) always serialized; every `.optional()`
    /// field OMITTED (not `null`) when `None`. A `null` would fail the client's
    /// `analyzeQueryResultSchema` for a non-nullable optional.
    #[test]
    fn omits_absent_optionals_and_uses_camel_case() {
        let r = AnalyzeQueryResult {
            warnings: vec!["w".to_string()],
            synced_row_count: 2,
            start: 1.0,
            end: 3.0,
            read_row_count: Some(5),
            ..Default::default()
        };
        let json = serde_json::to_value(&r).unwrap();
        // Required fields present, camelCase.
        assert_eq!(json["syncedRowCount"], 2);
        assert_eq!(json["readRowCount"], 5);
        assert!(json.get("warnings").is_some());
        // Absent optionals are OMITTED, not null.
        assert!(
            json.get("syncedRows").is_none(),
            "absent optional must be omitted, not null: {json}"
        );
        assert!(json.get("afterPermissions").is_none());
        assert!(json.get("dbScansByQuery").is_none());
        assert!(json.get("sqlitePlans").is_none());
    }
}
