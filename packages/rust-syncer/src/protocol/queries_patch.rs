//! Port of `packages/zero-protocol/src/queries-patch.ts` — serde
//! equivalents of the valita schemas.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `upPutOpSchema` (queries-patch.ts:10-14): `putOpSchema` extended with the
/// upstream-only `ast` / `name` / `args`. Every object here is a valita
/// `v.object` — unknown keys rejected, `.optional()` never `null` — and the
/// `ast` value is validated against the ported `astSchema`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpQueriesPutOp {
    pub hash: String,
    #[serde(default, deserialize_with = "crate::protocol::optional_no_null")]
    pub ttl: Option<serde_json::Number>,
    #[serde(
        default,
        deserialize_with = "crate::protocol::ast::optional_strict_ast"
    )]
    pub ast: Option<Value>,
    #[serde(default, deserialize_with = "crate::protocol::optional_no_null")]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "crate::protocol::optional_no_null")]
    pub args: Option<Vec<Value>>,
}

/// `delOpSchema` (queries-patch.ts:16-19).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueriesDelOp {
    pub hash: String,
}

/// `clearOpSchema` (queries-patch.ts:21-23).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueriesClearOp {}

/// `upPatchOpSchema` (queries-patch.ts:26): put | del | clear, discriminated
/// by `op`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op")]
pub enum UpQueriesPatchOp {
    #[serde(rename = "put")]
    Put(UpQueriesPutOp),
    #[serde(rename = "del")]
    Del(QueriesDelOp),
    #[serde(rename = "clear")]
    Clear(QueriesClearOp),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueriesPatchOp {
    Put {
        op: String, // "put"
        hash: String,
        ttl: Option<i64>,
    },
    Del {
        op: String, // "del"
        hash: String,
    },
    Clear {
        op: String, // "clear"
    },
}

pub type QueriesPatch = Vec<QueriesPatchOp>;
/// `upQueriesPatchSchema` (queries-patch.ts:29). The syncer keeps the
/// entries as JSON (the view-syncer reads op/hash/ast/name/args/ttl off the
/// `Value`), so the field carries `strict_up_queries_patch` to validate each
/// entry against `upPatchOpSchema` at the boundary, as `valita.parse` does.
pub type UpQueriesPatch = Vec<Value>;

/// `deserialize_with` for an `UpQueriesPatch` field: every entry must be a
/// valid `UpQueriesPatchOp`; the JSON is handed back unchanged.
pub fn strict_up_queries_patch<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<UpQueriesPatch, D::Error> {
    let entries = Vec::<Value>::deserialize(d)?;
    for entry in &entries {
        UpQueriesPatchOp::deserialize(entry).map_err(serde::de::Error::custom)?;
    }
    Ok(entries)
}
