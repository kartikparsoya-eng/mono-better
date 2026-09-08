//! Port of `packages/zero-protocol/src/poke.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use super::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PokeStartBody {
    #[serde(rename = "pokeID")]
    pub poke_id: String,
    pub base_cookie: NullableVersion,
    pub schema_versions: Option<SchemaVersions>,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaVersions {
    pub min_supported_version: crate::protocol::JsNumber,
    pub max_supported_version: crate::protocol::JsNumber,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PokePartBody {
    #[serde(rename = "pokeID")]
    pub poke_id: String,
    #[serde(rename = "lastMutationIDChanges")]
    pub last_mutation_id_changes: Option<serde_json::Map<String, Value>>,
    #[serde(rename = "desiredQueriesPatches")]
    pub desired_queries_patches: Option<serde_json::Map<String, Value>>,
    #[serde(rename = "gotQueriesPatch")]
    pub got_queries_patch: Option<Value>,
    #[serde(rename = "rowsPatch")]
    pub rows_patch: Option<RowsPatch>,
    #[serde(rename = "mutationsPatch")]
    pub mutations_patch: Option<MutationsPatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PokeEndBody {
    #[serde(rename = "pokeID")]
    pub poke_id: String,
    pub cookie: Version,
    pub cancel: Option<bool>,
}
