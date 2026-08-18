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

/// Mirrors TS `rowIDString(id)` — canonical string for a RowID.
///
/// Emits `["schema","table",k1,v1,...,kn,vn]` where the key/value pairs are in
/// `normalizedKeyOrder` (lexicographic). Rather than materialize an intermediate
/// `Value::Array` (which forced a clone of `schema`, `table`, and every key +
/// value), this streams the pieces straight into a byte buffer.
///
/// # Parity
///
/// Every actual JSON token — string escaping, number formatting — is still
/// produced by `serde_json`, and a compact serialize of `x` via `to_writer`
/// is byte-for-byte identical to that same `x` serialized as an element of a
/// `Value::Array`. So the output is identical to the previous
/// `serde_json::to_string(&Value::Array(...))` form the CVR keys are validated
/// against (see `parity_check.rs`). The only change is the elimination of the
/// intermediate `Value` allocations and clones.
///
/// Note: TS caches per-object with a WeakMap. Rust memoizes in
/// [`row_id_string_cached`].
pub fn row_id_string(id: &RowID) -> String {
    let entries = normalized_key_order(&id.row_key);
    // `[` + two strings + per-entry (`,"k",v`) + `]`. 32 + 16/entry is a rough
    // lower bound that avoids the first few reallocs for typical keys.
    let mut buf: Vec<u8> = Vec::with_capacity(32 + entries.len() * 16);
    buf.push(b'[');
    serde_json::to_writer(&mut buf, &id.schema).expect("rowIDString: schema");
    buf.push(b',');
    serde_json::to_writer(&mut buf, &id.table).expect("rowIDString: table");
    for (k, v) in entries {
        buf.push(b',');
        serde_json::to_writer(&mut buf, k).expect("rowIDString: key");
        buf.push(b',');
        serde_json::to_writer(&mut buf, v).expect("rowIDString: value");
    }
    buf.push(b']');
    // `serde_json` only ever emits valid UTF-8, so this never fails.
    String::from_utf8(buf).expect("rowIDString serialization produced invalid UTF-8")
}

/// Max live entries per generation. The cache holds at most `2 * CACHE_GEN_CAP`
/// entries before the oldest generation is dropped. 64Ki/gen (≤128Ki total)
/// comfortably covers a large client group's working set of distinct RowIDs
/// while bounding worst-case retention.
const CACHE_GEN_CAP: usize = 64 * 1024;

/// Two-generation ("hot"/"cold") bounded cache. A lookup checks `hot` then
/// `cold`, promoting a cold hit into `hot`. When `hot` fills, it rotates to
/// `cold` (dropping the previous `cold`) and a fresh `hot` starts. This gives
/// LRU-ish behavior with O(1) ops and no per-entry bookkeeping — and, crucially,
/// a hard memory bound.
struct RowIdStringCache {
    hot: std::collections::HashMap<RowID, String>,
    cold: std::collections::HashMap<RowID, String>,
}

impl RowIdStringCache {
    fn new() -> Self {
        Self {
            hot: std::collections::HashMap::new(),
            cold: std::collections::HashMap::new(),
        }
    }

    fn get(&mut self, id: &RowID) -> Option<String> {
        if let Some(s) = self.hot.get(id) {
            return Some(s.clone());
        }
        // Promote a cold hit into the hot generation so it survives the next
        // rotation. Remove from cold to keep total residency bounded.
        if let Some(s) = self.cold.remove(id) {
            self.insert(id.clone(), s.clone());
            return Some(s);
        }
        None
    }

    fn insert(&mut self, id: RowID, s: String) {
        if self.hot.len() >= CACHE_GEN_CAP {
            // Rotate: the previous cold generation is dropped here.
            std::mem::swap(&mut self.hot, &mut self.cold);
            self.hot.clear();
        }
        self.hot.insert(id, s);
    }
}

/// A per-RowID cache to match the TS WeakMap behavior. This avoids recomputing
/// the string form when the same RowID is hashed/compared repeatedly.
///
/// # Memory lifecycle
///
/// Unlike TS's `WeakMap` (which evicts when the RowID is GC'd), this cache is
/// `static` and lives for the process. To keep it from growing without bound as
/// callers construct unique RowIDs, it is a two-generation bounded cache (see
/// [`RowIdStringCache`]) capped at `2 * CACHE_GEN_CAP` entries. Eviction is
/// output-transparent: a miss simply recomputes the identical string.
static ROW_ID_STRING_CACHE: OnceLock<parking_lot::Mutex<RowIdStringCache>> = OnceLock::new();

/// Mirrors TS's memoized `rowIDString` using a thread-safe bounded cache.
pub fn row_id_string_cached(id: &RowID) -> String {
    let cache = ROW_ID_STRING_CACHE.get_or_init(|| parking_lot::Mutex::new(RowIdStringCache::new()));
    let mut guard = cache.lock();
    if let Some(s) = guard.get(id) {
        return s;
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

    /// The streaming `row_id_string` must be byte-identical to the reference
    /// form it replaced: `serde_json::to_string(&Value::Array([schema, table,
    /// k1, v1, ...]))`. Exercise the value shapes most likely to expose an
    /// encoding difference (floats, null, nested, unicode, quotes/backslashes).
    #[test]
    fn test_row_id_string_matches_value_array_reference() {
        fn reference(id: &RowID) -> String {
            let entries = normalized_key_order(&id.row_key);
            let mut arr = Vec::with_capacity(2 + entries.len() * 2);
            arr.push(Value::String(id.schema.clone()));
            arr.push(Value::String(id.table.clone()));
            for (k, v) in entries {
                arr.push(Value::String(k.clone()));
                arr.push(v.clone());
            }
            serde_json::to_string(&Value::Array(arr)).unwrap()
        }

        let cases = [
            json!({"id": 42}),
            json!({"userId": "u1", "id": 42}),
            json!({"f": 1.5, "g": -0.0, "big": 9007199254740991i64}),
            json!({"n": Value::Null, "s": "with \"quotes\" and \\backslash"}),
            json!({"uni": "café — 日本語 — 😀", "nested": {"a": [1, 2, {"b": null}]}}),
            json!({"z": 1, "a": 2, "m": 3}), // out-of-order keys → normalization
        ];
        for (i, c) in cases.iter().enumerate() {
            let id = make_row_id("public", "t", c.clone());
            assert_eq!(
                row_id_string(&id),
                reference(&id),
                "streaming row_id_string diverged from Value::Array reference for case {i}"
            );
        }
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
