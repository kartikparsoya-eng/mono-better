//! Port of `packages/zero-cache/src/services/view-syncer/schema/cvr.ts` — the
//! CVR Postgres table row shapes (`InstancesRow`, `ClientsRow`, `QueriesRow`,
//! `DesiresRow`, `RowsRow`, `RowsVersionRow`) and the row<->record converters.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::schema::types::RowID;
use crate::schema::types::RowRecord;
use crate::schema::types::{maybe_version_string, version_string};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstancesRow {
    pub client_group_id: String,
    pub version: String,
    pub last_active: f64,
    pub ttl_clock: f64,
    pub replica_version: Option<String>,
    pub owner: Option<String>,
    pub granted_at: Option<f64>,
    pub client_schema: Option<Value>,
    pub profile_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientsRow {
    pub client_group_id: String,
    pub client_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueriesRow {
    pub client_group_id: String,
    pub query_hash: String,
    pub client_ast: Option<Value>,
    pub query_name: Option<String>,
    pub query_args: Option<Value>,
    pub patch_version: Option<String>,
    pub transformation_hash: Option<String>,
    pub transformation_version: Option<String>,
    pub internal: Option<bool>,
    pub deleted: Option<bool>,
    pub row_set_signature: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DesiresRow {
    pub client_group_id: String,
    pub client_id: String,
    pub query_hash: String,
    pub patch_version: String,
    /// TS `deleted: boolean | null` (schema/cvr.ts:164) — the DDL column is
    /// nullable BOOL; NULL reads as falsy (F-CVR-SCHEMA-1: a non-optional bool
    /// made a legacy NULL fail sqlx decode where TS treats it false).
    pub deleted: Option<bool>,
    pub ttl: Option<f64>,
    pub inactivated_at: Option<f64>,
}
/// Mirrors TS `RowsRow` from `schema/cvr.ts` — the DB row form.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowsRow {
    #[serde(rename = "clientGroupID")]
    pub client_group_id: String,
    pub schema: String,
    pub table: String,
    #[serde(rename = "rowKey")]
    pub row_key: serde_json::Value,
    #[serde(rename = "rowVersion")]
    pub row_version: String,
    #[serde(rename = "patchVersion")]
    pub patch_version: String,
    #[serde(rename = "refCounts")]
    pub ref_counts: Option<serde_json::Value>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowsVersionRow {
    #[serde(rename = "clientGroupID")]
    pub client_group_id: String,
    pub version: String,
}
/// Error from decoding a DB `RowsRow` into a `RowRecord`. All variants indicate
/// malformed data in the `rows` table; mapped to `sqlx::Error::Decode` at the
/// call site so a corrupt row fails the load recoverably instead of aborting the
/// task (matching TS, which throws on malformed shapes).
#[derive(Debug, thiserror::Error)]
pub enum RowRecordError {
    #[error("rowKey is not an object: {0:?}")]
    RowKeyNotObject(serde_json::Value),
    #[error("refCounts is not an object: {0:?}")]
    RefCountsNotObject(serde_json::Value),
    #[error("refCount value is not an integer: {0:?}")]
    RefCountNotInteger(serde_json::Value),
    #[error("invalid patchVersion: {0}")]
    Version(#[from] crate::schema::types::VersionError),
}
/// Converts a `RowsRow` (DB form) to a `RowRecord` (cache form).
/// Mirrors TS `rowsRowToRowRecord` from `schema/cvr.ts`.
pub fn rows_row_to_row_record(row: &RowsRow) -> Result<RowRecord, RowRecordError> {
    let row_key_map = match &row.row_key {
        serde_json::Value::Object(m) => m.clone(),
        other => return Err(RowRecordError::RowKeyNotObject(other.clone())),
    };
    let ref_counts = row
        .ref_counts
        .as_ref()
        .map(|v| match v {
            serde_json::Value::Object(m) => m
                .iter()
                .map(|(k, v)| {
                    v.as_i64()
                        .map(|n| (k.clone(), n))
                        .ok_or_else(|| RowRecordError::RefCountNotInteger(v.clone()))
                })
                .collect::<Result<_, _>>(),
            other => Err(RowRecordError::RefCountsNotObject(other.clone())),
        })
        .transpose()?;
    Ok(RowRecord {
        id: RowID {
            schema: row.schema.clone(),
            table: row.table.clone(),
            row_key: row_key_map,
        },
        row_version: row.row_version.clone(),
        patch_version: maybe_version_string(&row.patch_version)?,
        ref_counts,
    })
}
/// Converts a `RowRecord` (cache form) to a `RowsRow` (DB form).
/// Mirrors TS `rowRecordToRowsRow` from `schema/cvr.ts`.
pub fn row_record_to_rows_row(client_group_id: &str, record: &RowRecord) -> RowsRow {
    let ref_counts = record.ref_counts.as_ref().map(|rc| {
        let map: serde_json::Map<String, serde_json::Value> = rc
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::Number((*v).into())))
            .collect();
        serde_json::Value::Object(map)
    });

    RowsRow {
        client_group_id: client_group_id.to_string(),
        schema: record.id.schema.clone(),
        table: record.id.table.clone(),
        row_key: serde_json::Value::Object(record.id.row_key.clone()),
        row_version: record.row_version.clone(),
        patch_version: version_string(&record.patch_version),
        ref_counts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::{CVRVersion, RowID};

    /// The serialized batch's JSON keys ARE the contract with the flush SQL:
    /// every `INSERT … FROM json_to_recordset($1::json) AS x("clientGroupID"
    /// TEXT, "schema" TEXT, "table" TEXT, "rowKey" JSONB, "rowVersion" TEXT,
    /// "patchVersion" TEXT, "refCounts" JSONB)` in `cvr_store::flush_internal`
    /// and `row_record_cache::flush_one_iteration` names these exact columns.
    /// `json_to_recordset` matches by NAME and yields SQL NULL for a key it
    /// cannot find — so a renamed or dropped field does not error, it writes a
    /// NULL column. Same shape TS sends (postgres.js serializes the same
    /// camelCase object literal), which is why the keys are camelCase while the
    /// Rust fields are snake_case.
    ///
    /// This also pins the encoding the flush paths rely on after they moved
    /// from `serde_json::to_value(&inserts)` (a `Value` tree sqlx re-walked) to
    /// `serde_json::to_string(&inserts)` (text bound straight to `$1::json`):
    /// the bytes must be identical, and this is what says so.
    ///
    /// Mutation test: change any `#[serde(rename)]` on `RowsRow` — e.g.
    /// `clientGroupID` to `client_group_id` — and this fails. Before it existed
    /// such a rename produced an all-NULL insert with no error anywhere.
    #[test]
    fn rows_row_batch_serializes_to_the_columns_the_flush_sql_names() {
        let mut row_key = serde_json::Map::new();
        row_key.insert("id".to_string(), serde_json::json!("i1"));
        let record = RowRecord {
            id: RowID {
                schema: "public".to_string(),
                table: "issue".to_string(),
                row_key,
            },
            row_version: "01".to_string(),
            patch_version: CVRVersion {
                state_version: "02".to_string(),
                config_version: None,
            },
            ref_counts: Some([("q1".to_string(), 1)].into_iter().collect()),
        };
        let batch = vec![row_record_to_rows_row("cg1", &record)];

        assert_eq!(
            serde_json::to_string(&batch).expect("RowsRow is always serializable"),
            r#"[{"clientGroupID":"cg1","schema":"public","table":"issue","rowKey":{"id":"i1"},"rowVersion":"01","patchVersion":"02","refCounts":{"q1":1}}]"#,
            "the batch JSON must carry exactly the keys json_to_recordset selects"
        );
    }

    /// A tombstone (`refCounts: None`) must serialize `"refCounts":null`, NOT
    /// omit the key: the flush SQL selects `"refCounts" JSONB` and an absent key
    /// would also yield NULL, but the DELETE-vs-UPSERT distinction downstream
    /// (`execute_row_updates`: only a literal `None` RECORD is a delete, a
    /// refCounts-null record is upserted so catchup can emit its row DEL) means
    /// the null must survive the round trip explicitly.
    #[test]
    fn tombstone_rows_row_keeps_an_explicit_null_ref_counts() {
        let mut row_key = serde_json::Map::new();
        row_key.insert("id".to_string(), serde_json::json!("i2"));
        let record = RowRecord {
            id: RowID {
                schema: "public".to_string(),
                table: "issue".to_string(),
                row_key,
            },
            row_version: "01".to_string(),
            patch_version: CVRVersion {
                state_version: "02".to_string(),
                config_version: None,
            },
            ref_counts: None,
        };
        let json = serde_json::to_string(&vec![row_record_to_rows_row("cg1", &record)]).unwrap();
        assert!(
            json.contains(r#""refCounts":null"#),
            "a tombstone must keep an explicit null refCounts; got {json}"
        );
    }
}
