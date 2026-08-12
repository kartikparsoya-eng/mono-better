//! TS-parity port of `packages/zero-cache/src/types/row-key.ts`.
//!
//! ## Contract
//!
//! `rowIDString(id)` == `JSON.stringify([id.schema, id.table, ...flatten(normalize(rowKey))])`
//! where `normalize(rowKey)` returns the row-key object with keys in lexicographic
//! ascending order (or passes the input through unchanged if already sorted), and
//! `JSON.stringify` is the `bigint-json` variant in TS (same as `JSON.stringify`
//! except BigInts become decimal strings).
//!
//! ## Known divergence (documented in the master plan)
//!
//! `bigint-json.stringify` uses `json-custom-numbers` which has subtle behavioral
//! differences from Rust's `serde_json::to_string` for edge cases:
//!
//! - **Bigints in TS > u64::MAX** are preserved as full-precision decimal strings.
//!   Rust `serde_json::Number` cannot represent integers outside u64/i64
//!   precisely. **Release blocker for Phase A:** none of the CVR tests today
//!   exercise bigint rowKeys (only safe-range i64), so this is gated on the Phase
//!   A fixture set not yet exercising it. Add deref-bigint-in-rowkey fixtures
//!   BEFORE Phase B lands.
//!
//! - **Number precision**: `serialize(1.1)` in JS emits `'1.1'`. Rust serde
//!   emits `'1.1'`. Both round-trip correctly for IEEE-754 doubles in safe
//!   integer range.
//!
//! - **Unicode escaping**: JS escapes surrogates U+D800-U+DFFF. Rust serde_json
//!   also escapes surrogates. Both emit the same output for valid UTF-8.
//!
//! Configuration is in place for parity: `serde_json` is imported with
//! `preserve_order`, and row-key normalization is done before serialization
//! so hash inputs are canonicalized upstream of any serde decision.

use crate::hash::h128;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::OnceLock;

pub type RowKey = Map<String, Value>;
pub type RowKeyType = serde_json::Map<String, Value>;

/// A RowID is the composite primary key used to identify a row across tables.
/// TS: `{schema: string, table: string, rowKey: RowKey}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RowID {
    pub schema: String,
    pub table: String,
    #[serde(rename = "rowKey")]
    pub row_key: RowKey,
}

/// Mirrors TS `normalizedKeyOrder(rowKey)`: if keys are already lex-sorted,
/// returns the input as-is; otherwise returns a new map with keys re-sorted.
///
/// In Rust, `serde_json::Map` with `preserve_order` is insertion-ordered but
/// lookups happen by key string anyway; the *order* matters only for the
/// subsequent `stringify`. We always re-sort into a Vec so the flatten step
/// gets a deterministic order.
pub fn normalized_key_order(key: &RowKey) -> Vec<(&String, &Value)> {
    let mut entries: Vec<(&String, &Value)> = key.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries
}

/// Mirrors TS `stringify(tuples(key))`.
///
/// The TS `tuples` flattens the sorted key-value pairs into `[k1, v1, k2, v2, ...]`
/// before stringification. Rust builds the same shape and serializes.
fn tuples_json(key: &RowKey) -> Value {
    let entries = normalized_key_order(key);
    let mut arr = Vec::with_capacity(entries.len() * 2);
    for (k, v) in entries {
        arr.push(Value::String(k.clone()));
        arr.push(v.clone());
    }
    Value::Array(arr)
}

/// Mirrors TS `rowIDString(id)` — canonical string for a RowID.
///
/// Note: TS caches per-object with a WeakMap. Rust uses a per-RowID OnceLock.
pub fn row_id_string(id: &RowID) -> String {
    // Build the [schema, table, k1, v1, ..., kn, vn] array as a JSON value.
    let mut arr = Vec::with_capacity(2 + id.row_key.len() * 2);
    arr.push(Value::String(id.schema.clone()));
    arr.push(Value::String(id.table.clone()));
    if let Value::Array(tuples) = tuples_json(&id.row_key) {
        arr.extend(tuples);
    }
    serde_json::to_string(&Value::Array(arr)).expect("rowIDString serialization failed")
}

/// A per-RowID cache to match the TS WeakMap behavior. This avoids recomputing
/// the string form (which involves sort + serialization) when the same RowID is
/// hashed/compared repeatedly.
///
/// # Memory lifecycle
///
/// Unlike TS's `WeakMap` which evicts entries when the RowID is GC'd, this
/// Rust cache is `static` and lives for the process. It will only grow
/// unboundedly if callers construct unique RowIDs on the fly. **Alert:** if
/// profiling shows this becomes a leak, switch to a `DashMap` keyed by the
/// computed hash itself (bounded by total unique RowIDs across the process).
pub static ROW_ID_STRING_CACHE: OnceLock<
    parking_lot::Mutex<std::collections::HashMap<RowID, String>>,
> = OnceLock::new();

/// Mirrors TS's memoized `rowIDString` using a thread-safe cache.
pub fn row_id_string_cached(id: &RowID) -> String {
    let cache = ROW_ID_STRING_CACHE
        .get_or_init(|| parking_lot::Mutex::new(std::collections::HashMap::new()));
    let mut guard = cache.lock();
    if let Some(s) = guard.get(id) {
        return s.clone();
    }
    let s = row_id_string(id);
    guard.insert(id.clone(), s.clone());
    s
}

/// Mirrors TS `rowIDHash(id) = h128(rowIDString(id)).toString(36)`.
pub fn row_id_hash(id: &RowID) -> String {
    let s = row_id_string_cached(id);
    let h = h128(&s);
    base36_encode(h)
}

/// Encodes a u128 in base36 (TS `BigInt(...).toString(36)` equivalent).
fn base36_encode(mut n: u128) -> String {
    if n == 0 {
        return "0".to_string();
    }
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut buf = Vec::new();
    while n > 0 {
        buf.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    buf.reverse();
    String::from_utf8(buf).expect("base36 encoding produced invalid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, json};

    fn make_row_id(schema: &str, table: &str, row_key_json: serde_json::Value) -> RowID {
        let row_key = row_key_json.as_object().unwrap().clone();
        RowID {
            schema: schema.to_string(),
            table: table.to_string(),
            row_key,
        }
    }

    #[test]
    fn test_normalized_key_order_already_sorted() {
        let mut m = Map::new();
        m.insert("a".to_string(), json!(1));
        m.insert("b".to_string(), json!(2));
        let entries = normalized_key_order(&m);
        let keys: Vec<&String> = entries.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn test_normalized_key_order_unsorted() {
        let mut m = Map::new();
        m.insert("z".to_string(), json!(1));
        m.insert("a".to_string(), json!(2));
        m.insert("k".to_string(), json!(3));
        let entries = normalized_key_order(&m);
        let keys: Vec<&String> = entries.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec!["a", "k", "z"]);
    }

    #[test]
    fn test_row_id_string_single_pk() {
        let id = make_row_id("public", "users", json!({"id": 42}));
        // Expected: JSON array ["public","users","id",42]
        assert_eq!(row_id_string(&id), r#"["public","users","id",42]"#);
    }

    #[test]
    fn test_row_id_string_multi_pk_sorted() {
        let id = make_row_id("public", "orders", json!({"userId": "u1", "id": 42}));
        assert_eq!(
            row_id_string(&id),
            r#"["public","orders","id",42,"userId","u1"]"#
        );
    }

    #[test]
    fn test_row_id_string_cached_idempotent() {
        let id = make_row_id("s", "t", json!({"k": "v"}));
        let a = row_id_string_cached(&id);
        let b = row_id_string_cached(&id);
        assert_eq!(a, b);
    }

    #[test]
    fn test_base36_encode() {
        assert_eq!(base36_encode(0), "0");
        assert_eq!(base36_encode(35), "z");
        assert_eq!(base36_encode(36), "10");
        assert_eq!(base36_encode(u128::MAX), "f5lxx1zz5pnorynqglhzmsp33");
    }

    #[test]
    fn test_row_id_hash_smoke() {
        let id = make_row_id("public", "users", json!({"id": 42}));
        let h = row_id_hash(&id);
        // Should be 25-26 base36 chars (128 bits max -> 25 chars).
        assert!(h.len() >= 20 && h.len() <= 26);
        assert!(h.chars().all(|c| c.is_ascii_alphanumeric()));
    }
}
