//! Port of `zero-protocol/src/data.ts` — the row/value schemas as serde types.
//! `valueSchema` is `v.union(jsonSchema, v.undefined())` (data.ts:4): any JSON
//! value; `undefined` cannot travel in JSON, so a `serde_json::Value` is the
//! whole set. `rowSchema` is `v.readonlyRecord(valueSchema)` (data.ts:6).

use serde_json::{Map, Value};

/// `rowSchema` (data.ts:6).
pub type Row = Map<String, Value>;
