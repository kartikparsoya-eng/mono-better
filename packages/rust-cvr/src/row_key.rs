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
use serde_json::{Map, Value};
use std::cell::RefCell;

use crate::schema::types::RowID;
use crate::shared::string_compare::string_compare;

pub type RowKey = Map<String, Value>;
pub type RowKeyType = serde_json::Map<String, Value>;

/// Mirrors TS `normalizedKeyOrder(rowKey)`: if keys are already lex-sorted,
/// returns the input as-is; otherwise returns a new map with keys re-sorted.
///
/// In Rust, `serde_json::Map` with `preserve_order` is insertion-ordered but
/// lookups happen by key string anyway; the *order* matters only for the
/// subsequent `stringify`. We always re-sort into a Vec so the flatten step
/// gets a deterministic order.
pub fn normalized_key_order(key: &RowKey) -> Vec<(&String, &Value)> {
    let mut entries: Vec<(&String, &Value)> = key.iter().collect();
    // TS row-key.ts:31 sorts with `a < b ? -1 : a > b ? 1 : 0` — JS string
    // order, i.e. `string_compare`, not byte order.
    entries.sort_by(|a, b| string_compare(a.0, b.0));
    entries
}

/// Append `v`'s compact JSON to `buf`.
///
/// `serde_json::to_writer` into a `Vec<u8>` cannot fail: the only two error
/// sources are the writer (an infallible `Vec` push) and a `Serialize` impl
/// that errors, which neither `String` nor `Value` does. The four call sites
/// below used to carry an `.expect("rowIDString: …")` each, which reads to the
/// next editor as a real error path worth handling. Concentrating the
/// impossibility here documents it once instead.
fn write_json<T: serde::Serialize + ?Sized>(buf: &mut Vec<u8>, v: &T) {
    serde_json::to_writer(buf, v).expect("serializing a String/Value into a Vec<u8> is infallible");
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
    write_json(&mut buf, &id.schema);
    buf.push(b',');
    write_json(&mut buf, &id.table);
    for (k, v) in entries {
        buf.push(b',');
        write_json(&mut buf, k);
        buf.push(b',');
        write_json(&mut buf, v);
    }
    buf.push(b']');
    // `serde_json` only ever emits valid UTF-8, so this never fails.
    String::from_utf8(buf).expect("rowIDString serialization produced invalid UTF-8")
}

/// Max live entries per generation, PER THREAD. The cache holds at most
/// `2 * CACHE_GEN_CAP` entries per thread before the oldest generation is
/// dropped.
///
/// This was 64Ki/gen when the cache was one process-global map. It is now
/// thread-local (see [`ROW_ID_STRING_CACHE`]), so the process-wide bound is
/// `threads * 2 * CACHE_GEN_CAP` — with ~200-1024 CG threads, keeping
/// 64Ki/gen would have raised the worst case from 128Ki entries to well over
/// 100M. 512/gen (1Ki/thread) keeps the aggregate in the same range as the
/// single shared cache it replaces (1Ki * 200 shards = 200Ki) while still
/// serving the memo's actual purpose: catching the same RowID hashed or
/// compared repeatedly within the window of rows currently being processed.
/// TS's `WeakMap` retains an entry only while the RowID OBJECT is alive
/// (row-key.ts:57), which is that same short window.
const CACHE_GEN_CAP: usize = 512;

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

thread_local! {
    /// A per-RowID cache to match the TS WeakMap behavior. This avoids recomputing
    /// the string form when the same RowID is hashed/compared repeatedly.
    ///
    /// # Why thread-local and not a shared `static`
    ///
    /// TS's memo is a module-level `WeakMap` (row-key.ts:57) — and zero-cache runs
    /// each sync worker as its own PROCESS, so that map is per-worker and reached
    /// from a single-threaded event loop. It needs no lock because there is no
    /// contention to resolve.
    ///
    /// Rust runs every client group's thread inside ONE process. A
    /// `static Mutex<RowIdStringCache>` therefore made every CG thread serialize
    /// on a shared lock at a point where TS has no synchronization at all — the
    /// "more serialized context than TS" divergence AGENTS rule 8 exists to catch,
    /// and on a path that is per-row when it is live. A `thread_local!` gives each
    /// thread the same private, lock-free memo TS's per-process WeakMap gives each
    /// worker.
    ///
    /// # Memory lifecycle
    ///
    /// Unlike TS's `WeakMap` (which evicts when the RowID is GC'd), this cache
    /// holds strong keys, so it is a two-generation bounded cache (see
    /// [`RowIdStringCache`]) capped at `2 * CACHE_GEN_CAP` entries per thread; the
    /// cap was lowered when the cache became per-thread so the process-wide bound
    /// did not multiply by the thread count. Eviction is output-transparent: a
    /// miss simply recomputes the identical string, so neither the cap nor the
    /// thread the call lands on can change what a caller observes.
    static ROW_ID_STRING_CACHE: RefCell<RowIdStringCache> =
        RefCell::new(RowIdStringCache::new());
}

/// Mirrors TS's memoized `rowIDString` using a per-thread bounded cache.
pub fn row_id_string_cached(id: &RowID) -> String {
    ROW_ID_STRING_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(s) = cache.get(id) {
            return s;
        }
        let s = row_id_string(id);
        cache.insert(id.clone(), s.clone());
        s
    })
}

/// TEST-ONLY: entries the CALLING THREAD's `rowIDString` memo currently holds.
///
/// The discriminator between a per-thread and a process-global cache: a freshly
/// spawned thread must see 0 here even after another thread has memoized the
/// same RowID. See `row_id_string_cache_is_per_thread`.
#[doc(hidden)]
pub fn __test_row_id_string_cache_len() -> usize {
    ROW_ID_STRING_CACHE.with(|cache| {
        let cache = cache.borrow();
        cache.hot.len() + cache.cold.len()
    })
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

    /// The `rowIDString` memo must be PER THREAD, not a process-global
    /// `Mutex`.
    ///
    /// TS's memo is a module-level `WeakMap` (row-key.ts:57) reached from a
    /// single-threaded event loop in a per-worker PROCESS — no lock, no
    /// cross-worker sharing. Rust runs every client group's thread in one
    /// process, so a `static Mutex<RowIdStringCache>` made all of them
    /// serialize on one lock where TS synchronizes nothing (AGENTS rule 8).
    ///
    /// The discriminator is cache RESIDENCY, not the returned string: the memo
    /// is output-transparent by design, so both shapes return identical text.
    /// A freshly spawned thread must therefore see an EMPTY cache even though
    /// this thread has already memoized the very same RowID.
    ///
    /// Mutation test: restore
    /// `static ROW_ID_STRING_CACHE: OnceLock<parking_lot::Mutex<..>>` with the
    /// `cache.lock()` body and `entries_seen_by_a_fresh_thread` comes back 1
    /// instead of 0 — the assertion that the spawned thread starts cold fails.
    #[test]
    fn row_id_string_cache_is_per_thread() {
        let id = make_row_id("public", "issue", json!({"id": "i1"}));
        let expected = row_id_string(&id);

        let before = __test_row_id_string_cache_len();
        assert_eq!(row_id_string_cached(&id), expected);
        assert_eq!(
            __test_row_id_string_cache_len(),
            before + 1,
            "the calling thread must have memoized the RowID"
        );
        // A second call is a hit: residency must not grow.
        assert_eq!(row_id_string_cached(&id), expected);
        assert_eq!(__test_row_id_string_cache_len(), before + 1);

        let id_for_thread = id.clone();
        let expected_for_thread = expected.clone();
        let (entries_seen_by_a_fresh_thread, text, after) = std::thread::spawn(move || {
            let seen = __test_row_id_string_cache_len();
            let text = row_id_string_cached(&id_for_thread);
            (seen, text, __test_row_id_string_cache_len())
        })
        .join()
        .expect("the probe thread must not panic");

        assert_eq!(
            entries_seen_by_a_fresh_thread, 0,
            "a fresh thread must start with its OWN empty memo; a \
             process-global cache would already hold the entry this thread \
             inserted, which is the shared lock this test exists to forbid"
        );
        assert_eq!(
            text, expected_for_thread,
            "the memo is output-transparent: which thread computes it, and \
             whether it hit or missed, cannot change the string"
        );
        assert_eq!(after, 1, "the probe thread memoized into its own cache");
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

    /// TS `normalizedKeyOrder` sorts with `a < b ? -1 : a > b ? 1 : 0`
    /// (row-key.ts:31) — JS UTF-16 order, so U+1F600 precedes U+FF01. Byte
    /// order (`str::cmp`) puts them the other way round.
    #[test]
    fn test_normalized_key_order_uses_js_string_order() {
        let mut key = RowKey::new();
        key.insert("\u{FF01}".to_string(), Value::from(1));
        key.insert("\u{1F600}".to_string(), Value::from(2));
        let ordered: Vec<&str> = normalized_key_order(&key)
            .into_iter()
            .map(|(k, _)| k.as_str())
            .collect();
        assert_eq!(ordered, vec!["\u{1F600}", "\u{FF01}"]);
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
