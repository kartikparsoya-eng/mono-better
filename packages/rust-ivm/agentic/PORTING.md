# PORTING.md — TS→Rust porting conventions for the rust-ivm engine

Rules-only reference. Read before editing any Rust file that ports a TS file from
`mono-v1.7/packages/zql/src/` or `mono-v1.7/packages/zqlite/src/`. The TS engine
under `mono-v1.7/` is the source of truth; the Rust engine must match its
behavior byte-for-byte. When fixing divergence, cite the TS source lines that
define the behavior.

## 1. File / module mapping (TS → Rust)

TS files live under `mono-v1.7/packages/zql/src/` and `.../zqlite/src/`.
Rust files live under `rust-ivm/src/`.

| TS path | Rust path | Notes |
|---|---|---|
| `zero-protocol/src/data.ts` | `ivm/data.rs` | `Value`/`Row`/`Node`/`compareValues` |
| `zero-protocol/src/ast.ts` | `builder/ast.rs` | `AST`/`Condition` (simplified, has serde) |
| `ivm/change.ts` | `ivm/change.rs` | `Change` enum + factories |
| `ivm/source.ts` + `ivm/memory-source.ts` | `ivm/source.rs` | **Two TS files → ONE Rust file** named `source.rs` (NOT `memory_source.rs`). No `trait Source` exists in Rust. |
| `ivm/operator.ts` | `ivm/operator.rs` | `Input`/`Output`/`InputBase` traits |
| `ivm/stream.ts` | `ivm/stream.rs` | `NodeStream`, `RelStream`, `from_vec` |
| `ivm/take.ts` | `ivm/take.rs` | |
| `ivm/join.ts` | `ivm/join.rs` | |
| `ivm/flipped-join.ts` | `ivm/flipped_join.rs` | |
| `ivm/exists.ts` | `ivm/exists.rs` | |
| `ivm/filter.ts` | `ivm/filter.rs` | |
| `ivm/filter-operators.ts` | `ivm/filter_operators.rs` | |
| `ivm/filter-push.ts` | `ivm/filter_push.rs` | |
| `ivm/fan-in.ts` / `fan-out.ts` | `ivm/fan_in.rs` / `fan_out.rs` | |
| `ivm/skip.ts` / `cap.ts` | `ivm/skip.rs` / `ivm/cap.rs` | |
| `ivm/union-fan-in.ts` / `union-fan-out.ts` | `ivm/union_fan_in.rs` / `union_fan_out.rs` | |
| `ivm/view-apply-change.ts` | `ivm/view.rs` | `applyChange`, `ExpandedNode` |
| `ivm/array-view.ts` | `ivm/array_view.rs` | |
| `ivm/catch.ts` | `ivm/catch.rs` | test output collector |
| `ivm/memory-storage.ts` | `ivm/memory_storage.rs` | |
| `ivm/constraint.ts` | `ivm/constraint.rs` | `Constraint`, `MultiConstraint` |
| `ivm/schema.ts` | `ivm/schema.rs` | `SourceSchema` |
| `builder/builder.ts` | `builder/builder.rs` | `buildPipeline`, `BuilderDelegate` trait |
| `builder/filter.ts` | `builder/filter.rs` | `createPredicate`, `transformFilters` |
| `builder/like.ts` | `builder/like.rs` | `getLikePredicate` (LIKE/ILIKE) |
| `query/expression.ts` | `builder/expression.rs` | `and`/`or`/`not`/`cmp`, `simplifyCondition` |
| `query/complete-ordering.ts` | `builder/complete_ordering.rs` | |
| `query/query-impl.ts` | `builder/query.rs` | `Query` builder |
| `zqlite/src/table-source.ts` | `sqlite/table_source.rs` | SQLite `TableSource` (stretch) |

camelCase → snake_case for all identifiers. PascalCase types are preserved
(`Value`, `Row`, `Node`, `Change`, `Condition`).

## 2. Type mappings

### `Value` (TS union → Rust enum)
TS `Value = null | boolean | number | string | ...` →
```rust
// ivm/data.rs
pub enum Value { Null, Bool(bool), F64(f64), Str(Arc<str>), Json(Arc<str>) }
```
- TS `undefined` AND `null` **both → `Value::Null`**. No `undefined` representation.
  TS `normalizeUndefined` (`v ?? null`) has no Rust counterpart — missing keys read
  as `Value::Null` via `row.get(col).cloned().unwrap_or(Value::Null)`.
- TS `number` → `F64(f64)` (no int/float split; integers stored as f64).
- TS `string` → `Str(Arc<str>)` (interned via `Arc`, not `String`).
- Rust adds `Json(Arc<str>)` variant (used for IN-clause arrays).
- Manual `PartialEq`/`Eq`: cross-variant `==` is `false`; `Null == Null` is `true`.

### `Row`
TS `Row = Record<string, Value>` → `pub type Row = Arc<FxHashMap<String, Value>>`
(`ivm/data.rs`). Immutable, shared via `Arc`; `FxHashMap` (rustc-hash) for perf.
Factory: `row(pairs)`.

### `Node`
```rust
// ivm/data.rs
pub struct Node {
    pub row: Row,
    pub relationships: HashMap<String, RelStream>,
    pub rel_order: Vec<String>,  // ADDED in Rust: preserves TS object key order
}
```
- TS `relationships: Record<string, () => Stream<Node | 'yield'>>` → `HashMap<String, RelStream>`.
- `rel_order: Vec<String>` is **Rust-only** (TS objects preserve insertion order; Rust HashMap does not). `Node::set_relationship(self, name, rel) -> Self` maintains it (builder pattern, takes `self` by value).

### `Change` (TS discriminated tuple union → Rust enum)
```rust
// ivm/change.rs  (#[repr(u8)] ChangeType: Add=0, Remove=1, Edit=2, Child=3)
pub enum Change {
    Add(Node),
    Remove(Node),
    Child { node: Node, child: ChildData },
    Edit { node: Node, old_node: Node },
}
pub struct ChildData { pub relationship_name: String, pub change: Box<Change> }  // Box breaks recursion
```
- TS tuple `[ChangeType.ADD, node, extra]` → Rust enum; the TS `extra: null` padding slot is **dropped**.
- Struct-variant syntax (`Child { node, child }`) mirrors TS field names.
- `SourceChange` (TS `source.ts`) is co-located in `change.rs` as a separate enum.

### `Ordering` rename
TS `Ordering` (from `ast.ts`) → Rust `SortOrder = Arc<Vec<[String; 2]>>`.
**Renamed to avoid clash with `std::cmp::Ordering`.**

### AST (`builder/ast.rs`)
```rust
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Ast { pub table: String, pub alias: Option<String>,
  pub where_clause: Option<Condition>, pub related: Vec<RelatedSubquery>,
  pub limit: Option<usize>, pub order_by: Option<Vec<OrderPart>>, pub start: Option<Bound> }
pub enum Condition { Simple(SimpleCondition), And(Vec<Condition>), Or(Vec<Condition>), CorrelatedSubquery(CorrelatedSubqueryCondition) }
pub struct SimpleCondition { pub op: String, pub left: ValuePosition, pub right: ValuePosition }
pub enum ValuePosition { Column { name: String }, Literal { value: Value } }
```
- The AST **has serde** — fixtures serialize/deserialize via serde_json. Field names are snake_case (`where_clause`, `order_by`) NOT the TS wire names (`where`, `orderBy`). When authoring fixtures, use the **Rust serde field names** (`table`, `where_clause`, `order_by`, `related`, `limit`, `start`).
- `op` is a `String` (not a typed enum) — values: `=`, `!=`, `<`, `<=`, `>`, `>=`, `IS`, `IS NOT`, `LIKE`, `NOT LIKE`, `ILIKE`, `NOT ILIKE`, `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS`.

## 3. `compareValues` / `valuesEqual` — comparison semantics

```rust
// ivm/data.rs
pub fn compare_values(a: &Value, b: &Value) -> CmpOrdering  // std::cmp::Ordering
pub fn values_equal(a: &Value, b: &Value) -> bool
```
- **Null ordering**: null sorts FIRST (`Value::Null` → `Less` vs any non-null). Matches TS.
- **Number compare**: `partial_cmp(f64).unwrap_or(Equal)` — **NaN collapses to `Equal`** (TS `a - b` can yield NaN; Rust does not propagate it). This is a deliberate divergence; treat NaN→Equal as the expected Rust behavior.
- **String compare**: `x.as_bytes().cmp(y.as_bytes())` — byte-order == UTF-8 byte order. Matches TS `compareUTF8`. No UTF-16 collation.
- **Cross-type compare**: `panic!("Cannot compare values of different types")` — TS `throw` → Rust `panic!` (NOT `Result`). Filter predicates guard with `lhs.is_null()` checks before calling, so cross-type panics only fire on genuinely mismatched-typed comparisons.
- **`values_equal`**: `null ≠ null` returns `false` (join semantics). `compare_values(Null, Null)` returns `Equal`. These DIFFER by design — use `values_equal` for join dedup, `compare_values` for ordering/range.

## 4. Ownership conventions

- **Single-threaded engine**: `Rc<RefCell<T>>` everywhere, NOT `Arc`/`Mutex`.
  `Shared<T> = Rc<RefCell<T>>` (`operator.rs`). `MemorySource.data: SharedData = Rc<RefCell<Vec<Row>>>`.
- **Strings**: `Arc<str>` for `Value::Str`/`Json`; `String` for struct fields.
- **Predicates / comparators**: `Rc<dyn Fn(&Row) -> bool + 'static>` (single-threaded, shareable). In `builder/filter.rs` the public type is `Predicate = Arc<dyn Fn(&Row) -> bool>`; the inner `create_predicate_impl` returns `Box<dyn Fn(&Value) -> bool>`. **Use `Rc` not `Box` at the row-predicate level** — a `Box<dyn Fn>` would move captured `RefCell` exclusively into one stream site, causing borrow conflicts when the predicate must be cloned/shared (this was a real bug; the audit records `filter_predicate` changed `Box`→`Rc`).
- **Relationship closures**: `RelStream = Rc<dyn Fn() -> NodeStream>` (`stream.rs`). Lazy thunk stored in `Node.relationships`, evaluated on demand when downstream iterates. Mirrors TS `relationships[name] = () => Stream`.
- **Recursive types**: `Box<Change>` inside `ChildData.change`.
- **SQLite connections**: `Rc<RefCell<Option<rusqlite::Connection>>>` (SQLite is synchronous; no async).
- `Arc` is used ONLY for immutable cloned data (`Arc<str>`, `Arc<FxHashMap>` for `Row`).

## 5. Generator / `'yield'` handling

- TS uses async generators `*fetch(): Stream<Node | 'yield'>` and `*push(): Stream<'yield'>`.
  The literal `'yield'` is a cooperative scheduling token.
- **Rust DROPS `'yield'` entirely.** `NodeStream = Box<dyn Iterator<Item = Node>>` yields plain `Node`.
  `push` returns `()` (void) — no `'yield'` relay.
- TS `yield*` delegation → Rust SHOULD return a lazy chained iterator
  (`.filter()`/`.take()`/`.map()` wrapped in `node_stream(...)`). Collecting
  into `Vec` then `from_vec(nodes)` is NOT lazy — the goal doc mandates lazy
  compute everywhere. The streaming-audit loop flags `from_vec`/`.collect()`
  in `fetch()` as violations; fix-streaming tasks rewrite them to lazy chains.
  EXCEPTION: operators that MUST sort all rows before emitting any (e.g.
  a merge-sorted source) are allowed to materialize — mark these
  `MATERIALIZATION-REQUIRED` with a one-line reason.
- Do NOT attempt to re-add a `'yield'` mechanism. It is intentionally absent.

## 6. Operator structure (`Input`/`Output` traits)

```rust
// ivm/operator.rs
pub trait InputBase { fn get_schema(&self) -> SourceSchema; fn destroy(&mut self); }
pub trait Input: InputBase { fn set_output(&self, output: OutputHandle); fn fetch(&self, req: &FetchRequest) -> NodeStream; }
pub trait Output { fn push(&mut self, change: Change, pusher: &dyn InputBase); }
pub type OutputHandle = Rc<RefCell<dyn Output>>;
pub type Shared<T> = Rc<RefCell<T>>;
```
- Each operator is a struct + `impl Input` + `impl Output` (Output::push is a **no-op stub**).
- **Adapter pattern**: the operator's constructor wires each upstream `Input` to a small adapter struct (`TakeOutput { take: Shared<Take> }`, `ParentOutput { join }`, `FilterOutputAdapter { filter }`) that implements `Output::push` by forwarding to a **private method** (`push_change`/`push_parent`/`push_child`) on the `Shared<Op>`. This replaces TS bound-method references (`parent.setOutput({ push: (c) => this.#pushParent(c) })`).
- **`&self` vs `&mut self` rule**: read-like trait methods (`get_schema`, `fetch`, `set_output`) take `&self` and mutate via `RefCell`; lifecycle/state methods (`destroy`, `push`, `connect`) take `&mut self`.
- `FetchRequest { constraint, multi_constraints, start, reverse, limit }`. `Start { row, basis: Basis::{At,After} }`.
- `Storage` trait (`get/set/del/scan` keyed `String → Value`) used by Take for per-partition `TakeState` and global `MAX_BOUND_KEY`.

## 7. Error / panic conventions

- **TS `throw new Error(...)` / `assert(cond, msg)` → Rust `panic!(...)` / `assert!(cond, "msg")`** for invariant violations. `fetch`/`push` are infallible signatures (no `Result`).
- **TS `unreachable(change)` → Rust `unreachable!()`** in exhaustive `match` arms (or the match is exhaustive by construction).
- **`throwOutput` → `ThrowOutput` with `panic!("Output not set")`** (`operator.rs`).
- **KEY DIVERGENCE — graceful skips**: some TS `assert`s become **silent `return;`** in Rust. In `join.rs` `push_parent`/`push_child` EDIT arms, a key-changing edit (which TS asserts cannot happen) becomes `if !row_equals_for_compound_key(...) { return; }` — "skip and let the next advance fix it" (Go IVM used `recover()` to tear down; Rust skips). When porting, preserve this: a TS assert about relationship-key stability may intentionally be a no-op skip in Rust.
- **Re-entrancy (exists.rs)**: TS uses `#inPush = true` + `try/finally`. Rust uses `try_borrow()` and **silently returns on re-entrant push** (`Err(_) => return`) instead of asserting. The `in_push: bool` field exists but is unused for guard duty.
- SQLite errors: `eprintln!` + swallow (return empty stream); never `Result`. (Acceptable for the in-memory fixture path; the SQLite `table` sourceKind is a stretch goal.)

## 8. Builder / predicate construction

### `create_predicate` (`builder/filter.rs`)
`create_predicate(condition: &Condition) -> Predicate` where `Predicate = Arc<dyn Fn(&Row) -> bool>`.
- `And` → all; `Or` → any; `CorrelatedSubquery` → pass-all (handled separately by `apply_correlated_subquery`).
- `Simple` with `Column { name }` vs `Literal { value }`:
  - `IS`/`IS NOT`: compare `lhs == rhs` (Null-aware via `==`).
  - non-IS with `rhs.is_null()` → always-false predicate (`Arc::new(|_| false)`).
  - non-IS with `lhs.is_null()` → `false` (then `impl(lhs)`).
- Operators and their Rust impl (`create_predicate_impl`):
  - `=` `!=` → `==`/`!=`
  - `<` `<=` `>` `>=` → `compare_values(lhs, rhs)` vs `Ordering` (NOT raw `<`)
  - `LIKE`/`NOT LIKE`/`ILIKE`/`NOT ILIKE` → `get_like_predicate(rhs, "")` / `get_like_predicate(rhs, "i")`, negated for NOT
  - `IN`/`NOT IN` → `Value::Json` array parsed; `set.iter().any(|v| v == lhs)`. NOTE: the current `parse_json_array` port is a STUB (returns single-element vec) — a real `serde_json` parse is needed for multi-element IN. Flag any IN-clause divergence for human review.
- `Literal = Literal` is evaluated at BUILD TIME into a constant predicate.
- `Column = Column` is **not supported** (`panic!("Only column = literal and literal = literal predicates supported")`).

### `transform_filters` (`builder/filter.rs`)
Strips `CorrelatedSubquery` from a `Condition` tree, returning a superset-matching condition. OR with any stripped branch → whole OR removed. AND with stripped branches → those branches dropped.

### `expression.rs` (`and`/`or`/`not`/`cmp`/`simplify_condition`/`negate_operator`)
- `and([])` = `TRUE` (`Condition::And(Vec::new())`); `or([])` = `FALSE` (`Condition::Or(Vec::new())`).
- `simplify_condition`: flattens nested same-type, collapses single-element, AND-with-false→FALSE, OR-with-true→TRUE.
- `negate_operator`: `=`↔`!=`, `<`↔`>=`, `>`↔`<=`, `IN`↔`NOT IN`, `LIKE`↔`NOT LIKE`, `ILIKE`↔`NOT ILIKE`, `IS`↔`IS NOT`, `EXISTS`↔`NOT EXISTS`. Unknown op → `panic!`.

## 9. Intentional deviations (from PORT-AUDIT.md) — do NOT "fix" these

- **`'yield'` dropped** (§5) — no cooperative scheduling token in Rust.
- **Planner layer (Layer 3) skipped** — optimization-only, not needed for core IVM.
- **Eager materialization** in in-memory `fetch` path (§5) — only `KWayMerge` is lazy.
- **NaN → `Equal`** in `compare_values` (§3).
- **Cross-type compare → `panic!`** not `Result` (§3, §7).
- **Key-changing edit in join push → silent skip** not assert (§7).
- **`IN`-clause JSON parse is a stub** — multi-element IN may diverge; flag for human.
- **`compareValues` null-first ordering** is INTENTIONAL and matches TS — do not change to null-last.
- ILIKE Unicode cases are IVM-only (SQLite without ICU doesn't handle Unicode `lower()`).

## 10. Naming quick-reference

| TS | Rust |
|---|---|
| `compareValues` | `compare_values` |
| `valuesEqual` | `values_equal` |
| `makeComparator` | `make_comparator` |
| `makeAddChange` | `make_add_change` |
| `normalizeUndefined` | *(dropped)* |
| `Ordering` (type) | `SortOrder` (renamed) |
| `relationshipName` | `relationship_name` |
| `oldNode` | `old_node` |
| `where` (AST field) | `where_clause` (serde) |
| `orderBy` (AST field) | `order_by` (serde) |
| `SourceChange` | `SourceChange` (co-located in `change.rs`) |
