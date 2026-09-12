//! Port of `zero-protocol/src/primary-key.ts` — serde equivalents of the
//! valita schemas. Parsed at the upstream boundary in valita's default
//! strict mode (the `crudOpSchema` members, mutation.ts:45-85, inside
//! `mutationSchema`, mutation.ts:116).

use serde::{Deserialize, Deserializer};
use std::collections::HashMap;

/// `primaryKeySchema` (primary-key.ts:3-6):
/// `v.tuple([v.string()]).concat(v.array(v.string()))` — at least one column.
#[derive(Debug, Clone, Deserialize)]
pub struct PrimaryKey(#[serde(deserialize_with = "non_empty_strings")] pub Vec<String>);

fn non_empty_strings<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    let key = Vec::<String>::deserialize(d)?;
    if key.is_empty() {
        return Err(serde::de::Error::invalid_length(0, &"at least 1 column"));
    }
    Ok(key)
}

/// `primaryKeyValueSchema` (primary-key.ts:9-13): `string | number | boolean`.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum PrimaryKeyValue {
    String(String),
    Number(serde_json::Number),
    Boolean(bool),
}

/// `primaryKeyValueRecordSchema` (primary-key.ts:16-18).
pub type PrimaryKeyValueRecord = HashMap<String, PrimaryKeyValue>;
