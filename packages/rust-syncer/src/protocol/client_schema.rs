//! Port of `packages/zero-protocol/src/client-schema.ts` — the valita shapes
//! `initConnectionBodySchema.clientSchema` is validated against
//! (`clientSchemaSchema.optional()`, connect.ts).
//!
//! The handler keeps consuming the client schema as a raw `serde_json::Value`
//! (`check_client_schema`, the CVR `clientSchema` column); these types exist so
//! the FRAME is accepted or rejected exactly where valita accepts or rejects it.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Port of TS `valueTypeSchema = v.literalUnion('string', 'number', 'boolean',
/// 'null', 'json')` (client-schema.ts:7-13).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ValueType {
    String,
    Number,
    Boolean,
    Null,
    Json,
}

/// Port of TS `columnSchemaSchema = v.object({type: valueTypeSchema})`
/// (client-schema.ts:15-17). valita `v.object` rejects unknown keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnSchema {
    pub r#type: ValueType,
}

/// Port of TS `tableSchemaSchema` (client-schema.ts:21-24).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TableSchema {
    pub columns: BTreeMap<String, ColumnSchema>,
    pub primary_key: Vec<String>,
}

/// Port of TS `clientSchemaSchema = v.object({tables: v.record(tableSchemaSchema)})`
/// (client-schema.ts:28-30).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientSchema {
    pub tables: BTreeMap<String, TableSchema>,
}

/// Deserialize `initConnectionBodySchema.clientSchema` —
/// `clientSchemaSchema.optional()` — as the raw `Value` the handler consumes,
/// VALIDATED against the ported shape.
///
/// `optional_no_null` alone is not enough for this field: it hands the input to
/// `T::deserialize`, and `serde_json::Value` deserializes `null` (and any other
/// JSON) happily, so `{"clientSchema": null}` came through as `Some(Null)` and
/// `{"clientSchema": {}}` as an object with no `tables` — both frames valita
/// REJECTS, closing the connection where rust silently served the client with
/// no schema (M13 corpus `wrongtype/initConnection.clientSchema/*`,
/// `clientschema/*`). Absent stays `None` via `#[serde(default)]`.
///
/// Rust-only helper (AGENTS.md rule 5): exists only to reproduce valita's
/// accept/reject boundary; the value that reaches the handler is the raw one,
/// byte-identical to what it was before.
pub fn optional_client_schema<'de, D>(d: D) -> Result<Option<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(d)?;
    ClientSchema::deserialize(&value).map_err(serde::de::Error::custom)?;
    Ok(Some(value))
}
