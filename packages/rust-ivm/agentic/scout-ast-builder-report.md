# Code Context: rust-ivm AST + Builder Pipeline

## Environment
- Crate: `rust-ivm` (lib name `rust_ivm`), edition `2024` (`/Users/kartik.parsoya/Documents/Go-RS/rust-ivm/Cargo.toml:1-7`). Deps: `serde` w/ `derive`+`rc`, `serde_json`, `rustc-hash`, `rusqlite`, `tiny_http`.
- Crate root `src/lib.rs` re-exports `ivm::*`, `builder::*`, `engine::*`, `streamer::*`, `snapshotter::*`.

## ⚠️ CRITICAL: Rust AST serde does NOT match the fixture JSON shape

The Rust `Ast` (below) is a **simplified, snake_case** port. It cannot `serde_json::from_str` the TS fixture AST directly. Concrete mismatches:

| Fixture JSON (TS shape) | Rust `Ast` field | Problem |
|---|---|---|
| `"orderBy": [["n","desc"]]` (array of 2-tuples) | `order_by: Option<Vec<OrderPart{column,direction}>>` | field is snake_case; tuples vs struct |
| `"where": {...}` | `where_clause: Option<Condition>` | field renamed |
| `"related": [{"correlation":{parentField,childField},"subquery":{...}}]` | `related: Vec<RelatedSubquery{relationship_name, parent_key, child_key, subquery, hidden, system}>` | flat fields vs nested `correlation`; `alias` becomes `relationship_name`; `parentField`→`parent_key` |
| Literals: `1`, `"a"`, `true`, `null` (bare JSON) | `Value` enum (externally tagged) | serde default → `{"F64":1.0}`/`{"Str":"a"}`, NOT bare |
| Conditions `"and"/"or"/"cmp"` (TS tagged) | `Condition` enum (externally tagged) | default serde → `{"Simple":{...}}`/`{"And":[...]}`, NOT `"type":"and"` |

**There is NO `#[serde(rename_all)]`, `#[serde(tag=...)]`, or custom serializer anywhere in `src/`** (verified via grep — zero matches for `rename_all`/`tag =`/`deny_unknown_fields`).

### Solution path (already exists)
`src/bin/server.rs` contains **manual** JSON→Rust converters (NOT exported from the lib). The fixture replayer must reuse/copy these into the test file:
- `json_to_ast` (server.rs:~243), `json_to_condition` (~131), `json_to_simple_condition` (~109), `json_to_value_position` (~95), `json_to_related_subquery` (~151), `json_to_rust_value` (~36), `json_to_row` (~69), `row_to_json` (~76), `rust_value_to_json` (~50), `row_change_to_json` (~257), `change_type_str` (~248).
- Condition tagging: `v.get("type")` → `"simple"|"and"|"or"|"correlatedSubquery"` (server.rs:131-156). For related subqueries, `alias`→`relationship_name`, `correlation.parentField`→`parent_key`, `correlation.childField`→`child_key` (server.rs:159-179). `orderBy` tuples `[col,dir]`→`OrderPart` (server.rs:205-214). `where`→`where_clause` (server.rs:246).

---

## (1) AST structs & enums — `/Users/kartik.parsoya/Documents/Go-RS/rust-ivm/src/builder/ast.rs`

All derive `Clone, Debug, serde::Serialize, serde::Deserialize` with NO serde attributes (default external tagging, snake_case field names) unless noted.

### `Ast` (ast.rs:6-34) — the zero-protocol AST root
```
#[serde(default)]  // ← only attribute on any AST type
pub struct Ast {
    pub table: String,
    pub alias: Option<String>,
    pub where_clause: Option<Condition>,
    pub related: Vec<RelatedSubquery>,
    pub limit: Option<usize>,
    pub order_by: Option<Vec<OrderPart>>,
    pub start: Option<Bound>,
}
impl Default for Ast  // empty table, None fields, empty related
```

### `OrderPart` (ast.rs:37-41)
```
pub struct OrderPart { pub column: String, pub direction: String }  // direction ∈ {"asc","desc"}
```

### `Bound` (ast.rs:44-49) — pagination start
```
pub struct Bound { pub row: Row, pub exclusive: bool }   // Row = Arc<FxHashMap<String,Value>>
```

### `Condition` enum (ast.rs:51-57) — externally tagged, no `tag` attr
```
pub enum Condition {
    Simple(SimpleCondition),
    And(Vec<Condition>),
    Or(Vec<Condition>),
    CorrelatedSubquery(CorrelatedSubqueryCondition),
}
```

### `SimpleCondition` (ast.rs:59-64) — cmp / LIKE / ILIKE / IN / IS
```
pub struct SimpleCondition {
    pub op: String,           // "=" "!=" "<" "<=" ">" ">=" "LIKE" "NOT LIKE" "ILIKE" "NOT ILIKE" "IN" "NOT IN" "IS" "IS NOT"
    pub left: ValuePosition,
    pub right: ValuePosition,
}
```
Operators handled in `builder/filter.rs:78-149` (`create_predicate_impl`): `=, !=, <, <=, >, >=, LIKE, NOT LIKE, ILIKE, NOT ILIKE, IN, NOT IN`. `IS`/`IS NOT` handled at `filter.rs:55-65`. `IN` parses `Value::Json(string)` as a JSON array — **NOTE `parse_json_array` (filter.rs:151-157) is a stub that just wraps the string as a single `Value::Str`**, so IN-with-array is NOT actually implemented in the library.

### `ValuePosition` enum (ast.rs:66-71) — externally tagged
```
pub enum ValuePosition {
    Column { name: String },
    Literal { value: Value },
}
```

### `CorrelatedSubqueryCondition` (ast.rs:73-78) — EXISTS / NOT EXISTS
```
pub struct CorrelatedSubqueryCondition {
    pub related: RelatedSubquery,
    pub op: String,   // "EXISTS" | "NOT EXISTS"
    pub flip: bool,   // flipped-subquery → FlippedJoin path
}
```

### `RelatedSubquery` (ast.rs:80-89) — related subqueries w/ correlation
```
pub struct RelatedSubquery {
    pub subquery: Box<Ast>,
    pub relationship_name: String,     // ← comes from "alias" in fixture JSON
    pub parent_key: Vec<String>,       // ← fixture "correlation.parentField"
    pub child_key: Vec<String>,        // ← fixture "correlation.childField"
    pub hidden: bool,
    pub system: Option<System>,        // System enum (see below)
}
```

### Supporting types (not in ast.rs)
- `Value` — `src/ivm/data.rs:14-21`:
  ```
  pub enum Value { Null, Bool(bool), F64(f64), Str(Arc<str>), Json(Arc<str>) }
  ```
  Default serde → externally tagged (`{"F64":1.0}` etc.). `is_null()`, `PartialEq` strict per-variant, `Default = Null`.
- `Row` — `src/ivm/data.rs:60`: `pub type Row = Arc<FxHashMap<String, Value>>`. Helper `row(pairs)` (data.rs:63) builds one.
- `SortOrder` — `src/ivm/data.rs:66`: `pub type SortOrder = Arc<Vec<[String; 2]>>` (each entry `[column, "asc"|"desc"]). This is what `source.connect(sort, …)` expects; builder.rs:148-155 builds it from `OrderPart`s.
- `System` enum — `src/ivm/schema.rs:7-12`: `Permissions | Client | Test`. Derives serde (default externally tagged `{"Permissions":…}` — but server.rs maps string `"permissions"/"test"→enum` manually).
- `ColumnType` — `src/ivm/schema.rs:14-21`: `Boolean{optional} | Number{optional} | String{optional} | Json{optional}`. **No serde derives.** Fixture column specs are bare strings like `"string"`, `"number"`, `"string|null"` — the replayer must parse these to `ColumnType::{String{optional:true}, …}` itself (server.rs `handle_init` expects `{type, optional}` objects, also a mismatch with the bare-string fixture format).

---

## (2) buildPipeline equivalent — `/Users/kartik.parsoya/Documents/Go-RS/rust-ivm/src/builder/builder.rs`

### `BuilderDelegate` trait (builder.rs:43-59) — the source/table resolver delegate
```
pub trait BuilderDelegate {
    fn get_source(&self, table_name: &str) -> Option<Shared<MemorySource>>;
    fn enable_not_exists(&self) -> bool { false }
    fn create_storage(&mut self) -> Shared<dyn Storage> { Rc::new(RefCell::new(MemoryStorage::new())) }
}
```

### `build_pipeline` (builder.rs:62-65) — THE public entry point
```
pub fn build_pipeline(ast: &Ast, delegate: &mut dyn BuilderDelegate) -> Shared<dyn Input>
```
Returns `Shared<dyn Input>` = `Rc<RefCell<dyn Input>>` (`src/ivm/operator.rs:78`: `pub type Shared<T> = Rc<RefCell<T>>`).

Internally calls `build_pipeline_internal` (builder.rs:71-186), which:
1. `delegate.get_source(&ast.table)` → returns `EmptyInput` if None (builder.rs:80-84).
2. Validates NOT EXISTS if `!delegate.enable_not_exists()` (builder.rs:87-91).
3. Gathers correlated subquery conditions (`gather_correlated_subquery_query_conditions`, builder.rs:431-449).
4. Builds `sort: Option<SortOrder>` from `ast.order_by` (builder.rs:142-155) — unless `use_cap` (non-flipped EXISTS child w/o flipped subqueries).
5. `transform_filters(ast.where_clause)` (builder.rs:158-160) → strips CSQ conditions for source-level pushdown.
6. `source.borrow_mut().connect(sort, filter_condition, filter_predicate, split_keys)` (builder.rs:172-174) → returns `Shared<dyn Input>` (a `SourceInput`).
7. Applies `ast.start` → `Skip::new` (builder.rs:179-183).
8. Applies non-flipped EXISTS CSQs → `apply_correlated_subquery` (builder.rs:189-194 → 451-525) which builds child pipeline + `Join::new(JoinArgs{…})`.
9. Applies WHERE if needed → `apply_where` (builder.rs:198-208 → 214-228) → `apply_filter` (builder.rs:236-256) or `apply_filter_with_flips` (builder.rs:281-357).
10. Applies `ast.limit` → `Cap::new` (use_cap) or `Take::new` (builder.rs:213-232).
11. Applies `ast.related` (non-condition joins) → `apply_correlated_subquery_join` (builder.rs:243-251 → 527-552) → `Join::new`. Dedupes by `relationship_name` (last wins).

### `apply_filter` (builder.rs:236-256) — WHERE clause dispatch
- `And(conds)` → recurse fold.
- `Or(conds)` → `apply_or_filter` (builder.rs:262-291): if no subqueries → `Filter::new` with `create_predicate(Or(…))`; else `NodeFilter::new(input, build_node_predicate(conds))`.
- `CorrelatedSubquery(csq)` → `apply_csq_condition` (builder.rs:305-336) → `Exists::new(input, rel_name, parent_key, not)`.
- `Simple(s)` → `Filter::new(input, create_simple_predicate(s))`.

### `apply_filter_with_flips` (builder.rs:281-357) — flipped EXISTS path
- `Or` with flipped branches → `UnionFanOut::new` + per-branch `apply_filter_with_flips` + `UnionFanIn::new` (builder.rs:314-344).
- `CorrelatedSubquery` flipped → `build_pipeline_internal(subquery, …, Some(child_key), false)` + `FlippedJoin::new(FlippedJoinArgs{…})` (builder.rs:346-357).

### `complete_ordering` (builder.rs:595-598) — appends PK columns to orderBy
`pub fn complete_ordering_ast(ast, &dyn Fn(&str)->Vec<String>) -> Ast` — wraps `builder::complete_ordering::complete_ordering` (`src/builder/complete_ordering.rs:9-32`), which recurses into `related`/`where_clause` and appends missing PK cols as `{column:pk, direction:"asc"}`. The engine calls this BEFORE `build_pipeline` (engine.rs:233-237).

---

## (3) Source/table resolution (delegate equivalent)

`BuilderDelegate::get_source(&self, table_name: &str) -> Option<Shared<MemorySource>>` (builder.rs:47). The engine's in-tree impl is `EngineDelegate` (`src/engine/mod.rs:446-462`):
```
struct EngineDelegate<'a> { sources: &'a HashMap<String, Shared<MemorySource>>, enable_not_exists: bool }
impl BuilderDelegate for EngineDelegate<'a> {
    fn get_source(&self, table_name) -> Option<Shared<MemorySource>> { self.sources.get(table_name).cloned() }
    fn enable_not_exists(&self) -> bool { self.enable_not_exists }   // server-side: true
    fn create_storage(&mut self) -> Shared<dyn Storage> { Rc::new(RefCell::new(MemoryStorage::new())) }
}
```
The fixture replayer can either (a) reuse `Engine` (register sources via `engine.register_source`, then call `engine.add_queries(&[QuerySpec{query_id, ast}])` and `engine.advance(&[(table, SourceChange)])`), or (b) implement `BuilderDelegate` directly over its own `HashMap<String, Shared<MemorySource>>`. Option (a) is the simplest path and is what `tests/builder_test.rs` uses.

`MemorySource::new(table_name, columns: HashMap<String, ColumnType>, primary_key: Vec<String>)` — `src/ivm/source.rs:60-91`. Add rows via `source.borrow_mut().add_row(FxHashMap<String, Value>)` (source.rs:103-112). `add_row` keeps the Vec sorted by primary key comparator.

---

## (4) Operator structs/enums & wiring — `src/ivm/`

All operators implement `Input` trait (`src/ivm/operator.rs:46-50`: `set_output(OutputHandle)`, `fetch(&FetchRequest) -> NodeStream`) + `InputBase` (`get_schema() -> SourceSchema`, `destroy()`). They return `Shared<Self>` from `::new`.

| Operator | File:line (ctor) | Constructed in builder.rs at | Args |
|---|---|---|---|
| `SourceInput` (from `MemorySource::connect`) | source.rs:195-237 | 172-174 | `(sort, filter_condition, filter_predicate, split_edit_keys)` |
| `EmptyInput` | source.rs:419-422 | 80-84 (when source missing) | `EmptyInput::new()` |
| `Skip` | ivm/skip.rs | 180-183 | `Skip::new(input, Bound)` |
| `Filter` | ivm/filter.rs | 254 (Simple), 286 (OR no-sq) | `Filter::new(input, Predicate)` where `Predicate = Arc<dyn Fn(&Row)->bool>` |
| `Exists` | ivm/exists.rs | apply_csq_condition 330 | `Exists::new(input, rel_name, parent_key, not: bool)` |
| `Join` | ivm/join.rs:25-49 | 510 (CSQ), 548 (related) | `Join::new(JoinArgs{parent, child, parent_key, child_key, relationship_name, hidden, system})` |
| `FlippedJoin` | ivm/flipped_join.rs | 353 | `FlippedJoin::new(FlippedJoinArgs{parent, child, parent_key, child_key, relationship_name, hidden, system})` |
| `Take` (ordered limit) | ivm/take.rs:36-48 | 226 | `Take::new(input, storage: Shared<dyn Storage>, limit, partition_key: Option<Vec<String>>)` |
| `Cap` (unordered limit, EXISTS) | ivm/cap.rs | 218 | `Cap::new(input, Rc<RefCell<CapStorage>>, limit, partition_key)` |
| `UnionFanOut` | ivm/union_fan_out.rs | 316 | `UnionFanOut::new(input)` |
| `UnionFanIn` | ivm/union_fan_in.rs | 327 | `UnionFanIn::new(schema: SourceSchema)`; wire branches via `branch.borrow().set_output(ufi.clone())` |
| `NodeFilter` | ivm/node_filter.rs | 288 | `NodeFilter::new(input, build_node_predicate(conds))` |
| `CollectOutput` (sink) | source.rs:461-471 | engine.rs:239 | `CollectOutput::new()`; `Output` trait impl pushes into `changes: Vec<Change>` |

Builder constants: `EXISTS_LIMIT = 3` (builder.rs:38), `PERMISSIONS_EXISTS_LIMIT = 1` (builder.rs:40). When a CSQ's subquery has `limit == Some(0)`, short-circuits to always-true/false Filter (builder.rs:311-336, 467-482).

`Change` enum — `src/ivm/change.rs:23-37`: `Add(Node) | Remove(Node) | Child{node, child: ChildData} | Edit{node, old_node}`. `SourceChange` (change.rs:74-84): `Add{row} | Remove{row} | Edit{row, old_row}`. Factories `make_source_change_add/remove/edit` (change.rs:99-113).

`RowChange` (output of the engine, what fixtures' `pushChanges`/`hydrate`/`finalView` are built from) — `src/streamer/mod.rs:22-29`:
```
pub struct RowChange { pub change_type: ChangeType, pub query_id: String, pub table: String, pub row_key: Row, pub row: Option<Row> }
```
`ChangeType` — change.rs:10-15: `Add=0 | Remove=1 | Edit=2 | Child=3`. For REMOVE, `row: None` (streamer.rs:108-110 + 161-180). `Streamer::new(primary_keys, table_specs)` then `streamer.accumulate(qid, schema, &[Change])` + `streamer.stream() -> Vec<RowChange>`.

**Note on expected-output shape:** The fixtures serialize nodes as `{"node":{...},"type":"add"}` (a tuple `[node, type]`) and the view as `{row, relationships:{rel:[{row,relationships:{}}]}}`. The Rust `RowChange` has `{type, query_id, table, row_key, row}` — a different shape. The replayer must build the expected `{node, type}` / `{row, relationships}` JSON itself from `RowChange`s + the pipeline's `SourceSchema.relationships` (or by walking the actual `Node.relationships` during fetch). The `bin/server.rs` `row_change_to_json` (server.rs:257-269) outputs the `RowChange` shape, NOT the fixture's `node`-wrapped shape — so it's only a partial reference.

---

## (5) Exact import paths needed for the fixture replayer

```rust
// AST + builder (lib-re-exported via `rust_ivm::builder::*`)
use rust_ivm::builder::ast::{Ast, Bound, Condition, CorrelatedSubqueryCondition,
    OrderPart, RelatedSubquery, SimpleCondition, ValuePosition};
use rust_ivm::builder::builder::{build_pipeline, BuilderDelegate};   // if going delegate-direct
use rust_ivm::builder::complete_ordering::complete_ordering;
use rust_ivm::builder::filter::{create_predicate, create_simple_predicate, transform_filters};

// Engine (easiest harness — handles build_pipeline + hydration + advance + streaming)
use rust_ivm::engine::{Engine, QuerySpec, QueryResult};

// IVM data + schema + source + change + operator
use rust_ivm::ivm::data::{Value, Row, row as make_row, SortOrder, Node};
use rust_ivm::ivm::schema::{ColumnType, System, SourceSchema};
use rust_ivm::ivm::source::{MemorySource, CollectOutput, EmptyInput};
use rust_ivm::ivm::change::{Change, ChangeType, SourceChange,
    make_source_change_add, make_source_change_remove, make_source_change_edit,
    make_add_change, make_edit_change, make_remove_change, make_child_change};
use rust_ivm::ivm::operator::{Input, InputBase, Output, OutputHandle, Shared, Storage,
    FetchRequest, Basis, Start};

// Streamer (RowChange output)
use rust_ivm::streamer::{Streamer, RowChange, TableSpecInfo};

// For converting fixture JSON ↔ Rust Value/Row (copy these from src/bin/server.rs:36-90):
//   json_to_rust_value, rust_value_to_json, json_to_row, row_to_json
// For converting fixture AST JSON ↔ Rust Ast (copy from src/bin/server.rs:95-292):
//   json_to_ast, json_to_condition, json_to_simple_condition,
//   json_to_value_position, json_to_related_subquery

// External
use rustc_hash::FxHashMap;
use std::cell::RefCell; use std::rc::Rc; use std::sync::Arc;
use serde_json::Value as JsonValue;
```

`Shared<T> = Rc<RefCell<T>>` (`src/ivm/operator.rs:78`). `OutputHandle = Rc<RefCell<dyn Output>>` (operator.rs:18).

---

## Architecture / data flow

```
fixture .input.json
  ├─ tables.{name}: {columns, primaryKey, rows}
  │     └─ replayer builds MemorySource::new(name, ColumnType-map, pk),
  │        add_row() each row, engine.register_source(source)
  ├─ ast (TS shape) ── json_to_ast (copied from server.rs) ──> rust_ivm::builder::ast::Ast
  │     └─ engine.add_queries(&[QuerySpec{query_id, ast}])
  │          ├─ complete_ordering(&ast, |t| pks.get(t))           // appends PK to orderBy
  │          ├─ build_pipeline(&ast, &mut EngineDelegate)         // builds operator tree
  │          ├─ pipeline.set_output(CollectOutput)                // terminal sink
  │          └─ pipeline.fetch(default) → Streamer.accumulate → RowChanges  // hydrate
  └─ pushes[]: [{type, table, row, oldRow?}]
        └─ convert to (table, SourceChange) via make_source_change_{add,remove,edit}
           └─ engine.advance(&[(table, sc)])
                ├─ source.push_parallel(sc)  → propagates through pipeline
                └─ each pipeline's CollectOutput.changes → Streamer → RowChanges
```

The pipeline graph built by `build_pipeline`:
```
SourceInput ──[Skip if start]──┬──[Join×N for non-flipped EXISTS CSQs]──┬──[WHERE: Filter/NodeFilter/UnionFanOut+UnionFanIn/FlippedJoin]
                              └──[Join×N for related subqueries]        └──[Take or Cap if limit]──► CollectOutput
```
EXISTS conditions add BOTH a `Join` (attaches the relationship — `apply_correlated_subquery`, builder.rs:451-525) AND an `Exists` filter (checks relationship size — `apply_csq_condition`, builder.rs:305-336). Flipped EXISTS instead uses a single `FlippedJoin` (builder.rs:346-357) with no separate Exists.

## Start Here
1. **`/Users/kartik.parsoya/Documents/Go-RS/rust-ivm/src/builder/ast.rs`** — the AST types and (lack of) serde config. This is the contract the replayer must produce.
2. **`/Users/kartik.parsoya/Documents/Go-RS/rust-ivm/src/bin/server.rs:36-292`** — the existing JSON→Ast / JSON→Value / Row→JSON translators to copy into the test (since the lib doesn't export them and the Rust Ast's serde derives don't match the fixture JSON shape).
3. **`/Users/kartik.parsoya/Documents/Go-RS/rust-ivm/src/builder/builder.rs:43-65`** — `BuilderDelegate` + `build_pipeline` entry.
4. **`/Users/kartik.parsoya/Documents/Go-RS/rust-ivm/src/engine/mod.rs:225-262`** — `Engine::add_queries_streaming` shows the full hydrate pattern (complete_ordering → build_pipeline → set_output(CollectOutput) → fetch → Streamer); `engine.advance` (mod.rs:411-414) wraps `advance_streaming` (mod.rs:265-296) for pushes.
5. **`/Users/kartik.parsoya/Documents/Go-RS/rust-ivm/tests/builder_test.rs`** — existing test idiom for constructing `MemorySource`, `Engine`, `QuerySpec`, asserting on `results[0].changes`.

## Open questions / risks for the fixture replayer
- **Serde gap is the headline risk.** `serde_json::from_str::<Ast>(fixture_ast_json)` will FAIL on `orderBy`/`where`/`correlation.parentField`/bare literals. Must use the manual `json_to_*` converters (copy from server.rs) OR add serde attrs to ast.rs (forbidden — read-only). → Plan A: copy converters into the test.
- **Column type parsing**: fixtures use bare strings (`"string"`, `"string|null"`, `"number"`); server.rs `handle_init` expects `{type, optional}` objects. Replayer needs its own parser: split on `|null` → `optional=true`, map base → `ColumnType` variant.
- **`IN`/`NOT IN` is a stub** (`filter.rs:151-157` `parse_json_array` returns a single-element vec). Fixtures using IN may not work without fixing this in the library (out of scope for the test file).
- **Expected-output shape differs from `RowChange`**: fixtures use `{node:{row,relationships}, type}` (per-push, from `Change`s directly) and `{row, relationships:{rel:[…]}}` (view). The engine's `Streamer` produces `RowChange` (flat, `row: Option<Row>`). To match `expected.json` exactly the replayer may need to serialize `Node`+`Change` directly (before Streamer flattening) rather than `RowChange`. Inspect `e2e_test.rs` / `view_test.rs` for the node-serialization idiom.
- **`Value::F64` serde**: numbers serialize as `{"F64":1.0}`; fixture expects bare `1`. Use `rust_value_to_json` (server.rs:50-68) which emits integer JSON when `fract()==0`.
- `System` enum serde is externally tagged by default; server.rs maps strings manually. For fixtures (which never set `system`), `None` is fine.
