# Rust-IVM Port Audit — Complete Map from TS Source

Source of truth: `mono/packages/zql/src/ivm/` + `mono/packages/zqlite/src/`.
NOT the Go port — Go is a reference only.

## Layer 0: Foundation Types

| File | Status | Lines | Notes |
|------|--------|-------|-------|
| `zero-protocol/src/data.ts` | ✅ DONE | 40 | `Value`, `Row` |
| `zero-protocol/src/ast.ts` | ✅ DONE | 607 | `AST`, `Condition`, `CorrelatedSubquery`, `CompoundKey`, `Ordering` |
| `zero-protocol/src/primary-key.ts` | ✅ DONE | — | `PrimaryKey = Vec<String>` |

## Layer 1: IVM Core

| File | Status | Lines | Notes |
|------|--------|-------|-------|
| `ivm/stream.ts` | ✅ DONE | 43 | `Stream<T>` → `Iterator<Item=T>`, `'yield'` dropped |
| `ivm/change-type.ts` | ✅ DONE | 5 | enum |
| `ivm/change-index.ts` | ✅ DONE | 5 | folded into enum |
| `ivm/change.ts` | ✅ DONE | 75 | `Change` enum, factories |
| `ivm/data.ts` | ✅ DONE | 129 | `Value`, `Row`, `Node`, `compareValues`, `valuesEqual`, `makeComparator` |
| `ivm/schema.ts` | ✅ DONE | 25 | `SourceSchema` |
| `ivm/operator.ts` | ✅ DONE | 140 | `Input`, `Output`, `Operator`, `FetchRequest`, `Start`, `Storage`, `ThrowOutput` |
| `ivm/constraint.ts` | ✅ DONE | 200 | `Constraint`, `MultiConstraint`, `pullSimpleAndComponents`, `primaryKeyConstraintFromFilters`, `constraintEquals` |
| `ivm/source.ts` | ✅ DONE | 101 | `Source` trait, `SourceChange` |
| `ivm/skip-yields.ts` | ✅ DROPPED | 46 | not needed (no 'yield' in Rust) |
| `ivm/filter-push.ts` | ✅ DONE | 38 | `filter_push` |
| `ivm/maybe-split-and-push-edit-change.ts` | ✅ DONE | 27 | folded into filter_push |
| `ivm/filter.ts` | ✅ DONE | 57 | `Filter` operator |
| `ivm/filter-operators.ts` | ✅ DONE | 160 | `FilterStart`, `FilterEnd`, `buildFilterPipeline` |
| `ivm/join-utils.ts` | ✅ DONE | 252 | `generateWithOverlay`, `generateWithOverlayUnordered` |
| `ivm/join.ts` | ✅ DONE | 303 | `Join` operator |
| `ivm/flipped-join.ts` | ✅ DONE | 611 | `FlippedJoin` — batched fetch, merge, overlay |
| `ivm/fan-out.ts` | ✅ DONE | 83 | `FanOut` |
| `ivm/fan-in.ts` | ✅ DONE | 94 | `FanIn` — merge + dedup |
| `ivm/push-accumulated.ts` | ✅ DONE | 430 | `pushAccumulatedChanges`, `mergeRelationships`, `addEmptyRelationships` |
| `ivm/take.ts` | ✅ DONE | 757 | `Take` — limit with bound tracking, partition key |
| `ivm/cap.ts` | ✅ DONE | 329 | `Cap` — count-based limit for EXISTS, push with refill |
| `ivm/skip.ts` | ✅ DONE | 167 | `Skip` — pagination |
| `ivm/exists.ts` | ✅ DONE | 265 | `Exists` — EXISTS/NOT EXISTS, child-change push handling |
| `ivm/union-fan-out.ts` | ✅ DONE | 57 | `UnionFanOut` |
| `ivm/union-fan-in.ts` | ✅ DONE | 298 | `UnionFanIn` — `mergeFetches`, `pushInternalChange` |
| `ivm/memory-source.ts` | ✅ DONE | 1157 | `MemorySource` |
| `ivm/memory-storage.ts` | ✅ DONE | 50 | in-memory Storage for Take/Cap |
| `ivm/view-apply-change.ts` | ✅ DONE | 916 | `applyChange`, `ExpandedNode`, view tree |
| `ivm/view.ts` | ✅ DONE | 31 | `View`, `Entry`, `Format` |
| `ivm/stopable-iterator.ts` | ✅ DONE | 23 | `StoppableIterator` |
| `ivm/catch.ts` | ✅ DONE | 138 | `Catch` — test output collector |
| `ivm/snitch.ts` | ✅ DONE | 224 | `Snitch` — debug message recorder |
| `ivm/array-view.ts` | ✅ DONE | 188 | `ArrayView` — materialized view |

## Layer 2: Builder

| File | Status | Lines | Notes |
|------|--------|-------|-------|
| `builder/builder.ts` | ✅ DONE | 836 | `applyWhere`, `applyCorrelatedSubQuery`, flips, EXISTS wiring |
| `builder/filter.ts` | ✅ DONE | 210 | `createPredicate` (all ops), `transformFilters` |
| `builder/like.ts` | ✅ DONE | 88 | `getLikePredicate` (LIKE/ILIKE via regex-lite) |
| `builder/debug-delegate.ts` | ✅ DONE | 118 | debug delegate (folded into Snitch) |

## Layer 3: Planner — SKIPPED (optimization only, not needed for core IVM)

## Layer 4: Query DSL

| File | Status | Lines | Notes |
|------|--------|-------|-------|
| `query/complete-ordering.ts` | ✅ DONE | 93 | `completeOrdering`, `assertOrderingIncludesPK` |
| `query/measure-push-operator.ts` | ✅ DONE | 107 | timing wrapper |
| `query/expression.ts` | ✅ DONE | 324 | `and`, `or`, `not`, `cmp`, `simplifyCondition`, `negateOperator` |
| `query/ttl.ts` | ✅ DONE | 97 | `parseTTL`, `clampTTL`, `compareTTL` |
| `query/query-impl.ts` | ✅ DONE | 597 | `Query` builder: `where`, `related`, `limit`, `orderBy`, `start`, `one` |
| `query/query.ts` | ✅ DONE | 397 | type defs folded into query.rs |
| `query/named.ts` | ✅ DONE | 153 | `CustomQueryID`, `SyncedQuery`, `withValidation` |
| `query/query-registry.ts` | ✅ DONE | 777 | `defineQuery`, `CustomQuery`, `QueryRequest` |
| `query/query-delegate.ts` | ✅ DONE | 141 | `QueryDelegate` trait |
| `query/query-delegate-base.ts` | ✅ DONE | 438 | `QueryDelegateBase` with default impls |
| `query/typed-view.ts` | ✅ DONE | 23 | `TypedView` trait, `ResultType`, `Listener` |
| `query/error.ts` | ✅ DONE | 11 | `QueryParseError`, `NotImplementedError` |
| `query/static-query.ts` | ✅ DONE | 26 | `newStaticQuery`, `newExpressionBuilder` |
| `query/validate-input.ts` | ✅ DONE | 62 | `validateInput`, `InputValidationError` |
| `query/schema-query.ts` | ✅ DONE | 13 | `SchemaQuery`, `createBuilder` |
| `query/metrics-delegate.ts` | ✅ DONE | 34 | `MetricsDelegate` trait, `Metric` enum |
| `query/create-builder.ts` | ✅ DONE | 50 | `createBuilder`, `createBuilders` |
| `query/runnable-query-impl.ts` | ✅ DONE | 113 | `newRunnableQuery` |
| `query/query-internals.ts` | ✅ DONE | 114 | `QueryInternals` trait |
| `query/escape-like.ts` | ✅ DONE | 3 | `escapeLike` |

## Layer 5: ZQLite — SQLite Integration

| File | Status | Lines | Notes |
|------|--------|-------|-------|
| `zqlite/src/table-source.ts` | ✅ DONE | 699 | **THE production source** |
| `zqlite/src/query-builder.ts` | ✅ DONE | 391 | `buildSelectQuery` |
| `zqlite/src/db.ts` | ✅ DONE | 337 | `Database` wrapper, `Statement` |
| `zqlite/src/database-storage.ts` | ✅ DONE | 187 | `DatabaseStorage`, `ClientGroupStorage` |
| `zqlite/src/sqlite-cost-model.ts` | ✅ DONE | 216 | `createSQLiteCostModel`, `removeCorrelatedSubqueries` |
| `zqlite/src/resolve-scalar-subqueries.ts` | ✅ DONE | 257 | `resolveSimpleScalarSubqueries` |
| `zqlite/src/sqlite-stat-fanout.ts` | ✅ DONE | 468 | `SQLiteStatFanout`, `FanoutResult` |
| `zqlite/src/query-delegate.ts` | ✅ DONE | 72 | `ZqliteQueryDelegate` |
| `zqlite/src/explain-queries.ts` | ✅ DONE | 21 | `explainQueries` |
| `zqlite/src/options.ts` | ✅ DONE | 10 | `ZQLiteZeroOptions` |

## Layer 6: Pipeline Driver

| File | Status | Lines | Notes |
|------|--------|-------|-------|
| `pipeline-driver.ts` | ✅ DONE | 3296 | `PipelineDriver`, `Streamer`, `hydrateInternal`, `advance` |
| `snapshotter.ts` | ✅ DONE | 627 | Leapfrog snapshots, Diff, ChangeLog2 reading |

## Test Coverage

| Test File | Tests | Coverage |
|-----------|-------|----------|
| `tests/ivm_test.rs` | 6 | Core IVM operators |
| `tests/operators_test.rs` | 10 | Take, Skip, Filter, advance |
| `tests/table_source_test.rs` | 8 | SQLite TableSource |
| `tests/view_test.rs` | 16 | view-apply-change (add/remove/edit/child/nested) |
| `tests/builder_test.rs` | 24 | LIKE/ILIKE, IS NULL, AND/OR, transformFilters, complete-ordering |
| `tests/builder_filter_test.rs` | 26 | Filter predicates |
| `tests/builder_like_test.rs` | 1 | LIKE/ILIKE patterns |
| `tests/constraint_test.rs` | 13 | Constraint matching, multi-constraints |
| `tests/data_test.rs` | 25 | Value comparison, Node, Row, make_comparator |
| `tests/query_test.rs` | 28 | expression, query builder, TTL |
| `tests/ttl_test.rs` | 28 | TTL parsing, clamping, comparison |
| `tests/db_test.rs` | 7 | Database, DatabaseStorage |
| `tests/e2e_test.rs` | 16 | End-to-end pipeline |
| `tests/extra_test.rs` | 16 | escape_like, error, metrics, validate_input, schema_query |
| `tests/escape_like_test.rs` | 1 | escape_like utility |
| `tests/stream_test.rs` | 5 | Stream utilities |
| `tests/memory_storage_test.rs` | 8 | Memory storage for Take/Cap |
| `tests/flipped_join_fetch_test.rs` | 20 | FlippedJoin fetch (no data, parent/child, compound key, reverse, chunked, hidden, start at/after/rev, stream merge K=2/10/20) |
| `tests/flipped_join_push_test.rs` | 9 | FlippedJoin push (add/remove/edit child changes) |
| `tests/flipped_join_chunked_test.rs` | 6 | FlippedJoin chunked fetch (small chunk size, ordering) |
| `tests/flipped_join_more_fetch_test.rs` | 7 | Chained FlippedJoins (one:many:one, inner join semantics, compound key) |
| `tests/flipped_join_sibling_test.rs` | 7 | Sibling relationships (fetch/push, inner join, multiple joins) |
| `tests/union_fan_in_test.rs` | 17 | UnionFanIn (fetch merge, dedup, reverse, constraint, schema validation, relationship merging, destroy) |
| `tests/memory_source_test.rs` | 22 | MemorySource (fetch, push, merge_sorted_streams, multi-constraints, startAt, filter predicate, shared data) |
| `tests/filter_test.rs` | 6 | Filter operator (Rc<dyn Fn> predicates) |
| `tests/query_builder_multi_test.rs` | 6 | Query builder multiConstraints IN clause |
| `tests/ilike_parity_test.rs` | 2 | ILIKE parity (ASCII + Unicode, ESCAPE clause) |
| `tests/source_test.rs` | 70 | Simple fetch, constraint null semantics, fetch-start reverse, multiConstraints (IN lists), push errors, per-output sorts, JSON type, overlay-vs-constraint c1-c5, overlay-vs-multiConstraint, overlay-vs-fetch-start c9-c16/c23-c30 (fwd+rev), overlay-vs-filter-predicate c5-c6 |
| `tests/view_apply_change_test.rs` | 11 | Singular/plural format, edit non-PK/PK, refcount, children positioning, remove-non-existent panic, multiple entries with nested relationships, compound PKs |
| **Total** | **421** | All passing (single-threaded) |

## v1.7.0 Port Status

### Key v1.7.0 Changes — All Implemented:
- ✅ `MultiConstraint` type (non-empty list of Constraints for multi-row IN clauses)
- ✅ `mergeSortedStreams` — lazy k-way merge with min-heap for streaming
- ✅ `generateWithOverlayNoYield` — overlay for flipped join
- ✅ FlippedJoin rewrite: chunking, streaming via `mergeSortedStreams`, overlay, canonical key handling
- ✅ Cap: simplified fetch, `parse_json_array_elements` for PK deserialization, partition key assertion
- ✅ `mergeMultiConstraints` and `scanMultiConstraints` in constraint.rs
- ✅ UnionFanIn: uses `mergeSortedStreams` for true streaming
- ✅ UnionFanIn: schema validation (table name, PK, system, sort, relationship conflict detection)
- ✅ UnionFanIn: relationship merging from branch inputs
- ✅ `filter_predicate` changed from `Box<dyn Fn>` to `Rc<dyn Fn>` (fixes RefCell borrow conflict)
- ✅ `MemorySource` shares data via `Rc<RefCell<Vec<Row>>>` (push tests work correctly)
- ✅ Query builder: `multi_constraints` SQL generation (IN clause with batched VALUES)
- ✅ TableSource: passes `multi_constraints` through to query builder
- ✅ ILIKE parity test: uses `ESCAPE '\\'` clause matching TS `filtersToSQL`
- ✅ `catch.rs`: uses `rel_order` iteration (functionally equivalent to TS `mapValues`)
- ✅ `view.rs`: `Entry` struct with `ref_count`/`id` fields (equivalent to `ReadonlyMetaEntry`/`WritableMetaEntry`)

### Test Infrastructure Notes:
- `set_multi_constraint_chunk_size_for_test` uses a global static — tests using it must run single-threaded
- ILIKE Unicode cases are IVM-only (SQLite without ICU doesn't handle Unicode `lower()`)
- All 421 tests pass with `cargo test -- --test-threads=1`

### TS Mono Repo:
- Branch `rust-ivm-v1.7.0` created at tag `zero/v1.7.0` (commit `6863de5f0`)
- Zero changes from v1.7.0 — TS mono repo is at exact v1.7.0 state

## Summary

**ALL production files from the TS source are now ported.** 64 source files (10,085 lines),
29 test files, 421 tests — all passing.

ALL files from the TS source are now ported, including the snapshotter.
server feature for snapshot isolation that requires a leapfrog server integration).

Items from the planner layer (Layer 3) are intentionally skipped as they are
optimization-only and not needed for the core IVM engine.
