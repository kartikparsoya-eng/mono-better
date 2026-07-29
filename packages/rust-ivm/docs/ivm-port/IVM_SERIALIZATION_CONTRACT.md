# IVM Serialization Boundary Contract (Extraction #7)

Every type that crosses TS↔Rust (napi) or SQLite→Rust. Verified against
`rust-ivm/napi/src/lib.rs`, `rust-ivm/src/sqlite/table_source.rs`, and
`mono-v1.7/packages/zqlite/src/table-source.ts` (`zero/v1.7.0`).

## Type map (napi boundary)

| TS type | Wire (`NapiValue`) | Rust `Value` | Notes |
|---|---|---|---|
| `null` | `{kind:"null"}` | `Value::Null` | |
| `boolean` | `{kind:"bool", bool_val}` | `Value::Bool` | |
| `number` | `{kind:"f64", f64_val}` | `Value::F64` | all numbers are f64 both sides |
| `string` | `{kind:"str", str_val}` | `Value::Str(Arc<str>)` | |
| json | `{kind:"json", json_val}` | `Value::Json` | json_val is a JSON string |

`Row` = `HashMap<String, NapiValue>` ↔ `FxHashMap<String, Value>` (key order
irrelevant). `Change` carries a `change_type: i32` that must match the
`ChangeType` repr (ADD=0, REMOVE=1, EDIT=2, CHILD=3). AST crosses as a
camelCase JSON string parsed by serde.

**Rust `Value` has no integer variant — only `F64`.** The whole engine
represents numbers as f64. This matches TS, where `Value` is a JS `number`
(also f64) for numeric columns; neither side keeps a 64-bit integer.

## The SQLite integer conversion — corrected finding

**Locations (Rust):** `src/sqlite/table_source.rs:470` and `:593`
```rust
Ok(rusqlite::types::Value::Integer(n)) => Value::F64(n as f64),   // n: i64
```
Also `napi/src/lib.rs:266`: `n.as_i64() => Value::F64(i as f64)`.

`n as f64` silently loses precision for `|n| > 2^53` (e.g. a snowflake ID
`9007199254740993` becomes `...992`). **No error is raised.**

### What TS actually does (this corrects the extraction plan)
The plan states TS "uses `safeIntegers(true)` and handles bigint separately …
preserves precision." That is **not** what happens. `fromSQLiteType`
(`zqlite/table-source.ts:636-642`):
```ts
if (typeof v === 'bigint') {
  if (v > Number.MAX_SAFE_INTEGER || v < Number.MIN_SAFE_INTEGER) {
    throw new UnsupportedValueError(
      `value ${v} (in ${table}.${column}) is outside of supported bounds`);
  }
  return Number(v);   // within ±2^53: plain f64, same as Rust
}
```
- **TS does NOT preserve large integers.** It reads them as bigint (via
  `safeIntegers(true)`) purely to *detect* out-of-range values, then **throws
  `UnsupportedValueError`**.
- Within ±2^53, TS returns `Number(v)` — identical f64 to Rust.

### The real divergence
| Case | TS | Rust |
|---|---|---|
| `|n| ≤ 2^53` | `Number(n)` (f64) | `n as f64` | ✅ identical |
| `|n| > 2^53` | **throws `UnsupportedValueError`** | silent `n as f64` (corrupt) | 🔴 diverge |

So it is **hard-error vs. silent-corruption**, not precise-vs-imprecise.

### Correct fix (faithful port)
Do **not** add an i64/bigint `Value` variant (TS doesn't keep one). Instead add
a bounds check that errors, matching TS:
```rust
Ok(rusqlite::types::Value::Integer(n)) => {
    const MAX_SAFE: i64 = 9_007_199_254_740_991; // 2^53 - 1
    if n > MAX_SAFE || n < -MAX_SAFE {
        return Err(/* UnsupportedValueError equivalent */);
    }
    Value::F64(n as f64)
}
```
Apply at all three sites (`table_source.rs:470`, `:593`, `napi/src/lib.rs:266`).
Rows with IDs above 2^53 would then fail loudly on both engines instead of
silently diverging — which is also the signal that the app must use string IDs.

## Blob note
`Value::Str(String::from_utf8_lossy(...))` for SQLite Blob is lossy; TS keeps
blobs as-is. Low priority (IVM key/order columns are not blobs), but record it:
a non-UTF8 blob column would diverge (Rust mangles, TS preserves bytes).
