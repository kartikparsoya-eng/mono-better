# Structure & naming parity — `ivm` crate

_Companion to `parity/MAP-ivm.md` (Layer-1 symbol map). This documents the
2026-08-25 strict-1:1 refactor: **file structure, filenames, and function names
now mirror TS `zql/src/` 1:1** wherever a 1:1 mapping is meaningful, and every
place it is NOT is a documented, deliberate exception below — not an oversight._

Layer-2 body-differential coverage for this crate is the standing IVM oracle
(1822 golden fixtures, `tests/` + `parity_check`), which pins matched-fn bodies
to real-TS output; it is unaffected by this rename-only refactor (627/627 tests
green, run the CI way with the static wal2 SQLite lib).

## ✅ What is now 1:1

| Axis | Result |
|---|---|
| **Directory tree** | `builder/`, `ivm/`, `planner/`, `query/` mirror TS `zql/src/{builder,ivm,planner,query}`. The TS `builder/` vs `query/` split — previously fused into one Rust `builder/` — is restored: the fluent-API + delegates live in `src/query/`. |
| **Filenames** | Each ported file is the snake_case of its TS origin: `query/query_impl.rs` ⟵ `query-impl.ts`, `query/query_delegate_base.rs` ⟵ `query-delegate-base.ts`, `query/runnable_query_impl.rs` ⟵ `runnable-query-impl.ts`, `planner/planner_builder.rs` ⟵ `planner-builder.ts`, … (the planner `planner-*` prefix, previously dropped, is restored). |
| **Function names** | Already snake_case-exact for the overwhelming majority; 6 drifted names re-aligned to the exact TS name: `apply_or` (⟵`applyOr`, was `apply_or_filter`), `get_take_state_key` (⟵`getTakeStateKey`), `capture_planning_snapshot` / `restore_planning_snapshot` (⟵`capture/restorePlanningSnapshot`), `uniquify_correlated_subquery_condition_aliases` (⟵`uniquifyCorrelatedSubqueryConditionAliases`), `initialize_relationships_for_new_entry_if_any` (⟵`…IfAny`). |

## ⛔ Deliberate exceptions — 1:1 is NOT the contract here (documented, not a gap)

### 1. Symbol-level fusions (one Rust module fuses several tightly-coupled TS files)
A Rust struct lives in one file, so TS files whose classes are one Rust struct
cannot split into 1:1 files without un-fusing the struct (a rewrite). These show
as **MERGED**/**SPLIT** in `MAP-ivm.md` and are intentional:
- `ivm/view.rs` ⟵ `view.ts` + `view-apply-change.ts` (926 LOC) + `array-view.ts` + `view.ts` change types — the single `View` maintainer.
- `ivm/source.rs` ⟵ `source.ts` + `constraint.ts` + `skip-yields.ts` + `view-apply-change.ts` + `memory-source.ts` residue — the `Source` trait + change plumbing.
- `ivm/cap.rs` / `ivm/take.rs` / `ivm/stream.rs` ⟵ `take.ts` (757 LOC) split across the Cap operator, take-state, and the stream adaptor.
- `query/query_delegate_base.rs` ⟵ `query-delegate-base.ts` + the small `query-delegate.ts` residue (its `newQuery` runtime lives in `sqlite/query_delegate.rs`).
- `query/expression.rs` ⟵ `expression.ts` + `builder/filter.ts` predicate builders.
- `planner/planner_graph.rs` ⟵ `planner-graph.ts` + `planner-join.ts`; `planner/planner_fan_in.rs`/`planner_join.rs` also absorb `planner-node.ts` helpers.
- Tiny TS enum/format files (`change-index.ts`, `change-type.ts`, `*-enum.ts`, `default-format.ts`, `filter.ts`, `filter-push.ts`, `maybe-split-and-push-edit-change.ts`, `skip-yields.ts`) are inlined into their consumer (**DROPPED**/**MERGED**) — a Rust `enum`/`const`/closure, not a file.

### 2. Architectural divergence — `memory-source.ts` → the `sqlite/` subsystem
TS `memory-source.ts` (1180 LOC, an in-memory overlay + b-tree index) is
replaced by an entire SQLite-backed source: `sqlite/{table_source,db,
database_storage,query_builder,resolve_scalar_subqueries,sqlite_cost_model,
explain_queries,interrupt,…}.rs`. There is no 1:1 filename — the overlay
machinery (`computeOverlays`, `overlaysFor*`, `getIndexKeys`, `fork`, key
`stringify`) becomes SQLite transactions/indexes (see `MAP-ivm.md` aliases).
Forcing 1:1 filenames here would misrepresent the design.

### 3. Rust-only infra (no TS origin file)
`engine/`, `streamer/`, `snapshotter/{diff,spec,snapshotter}.rs`, `bin/`,
`advance_gate.rs` (fetch-budget gate), `credit.rs`, `otel_metrics.rs`,
`perf_trace.rs`, `live_count.rs`, `replay.rs`, `planner/runtime.rs` — transport,
observability, the actor snapshot runtime, and the SQLite-replica seam. These
have no single TS origin; they are the port's engine host.

### 4. Function names kept intentionally divergent
- `create_simple_predicate` (filter.rs) — the port of the narrow, private TS `createIsPredicate`, but broadened to take a whole `SimpleCondition`; the Rust name is the accurate one (`create_predicate`/`create_predicate_impl` are already 1:1).
- `make_partial_bound_comparator` (⟵`makeBoundComparator`) — carries the Rust partial-bounds (columnar) semantics the TS name lacks.
- `add_empty_relationships` (⟵`makeAddEmptyRelationships`) — TS returns a closure *factory*; Rust is the direct impl, so the `make`/factory prefix is dropped by idiom.

## Residual Layer-1 unresolved (out of engine remit)
The 32 behavioral-unresolved TS symbols in `MAP-ivm.md` are the `query/`
client-fluent + type-level API (`query-registry.ts` `defineQuery*`/`getQuery`,
`query-impl.ts` `isOneHop`/`isTwoHop`, `named.ts` `normalizeParser`/
`withValidation`, `ttl.ts` `normalizeTTL`, `validate-input.ts` `titleCase`, …):
TS type machinery / the client-side fluent builder factory, which have no engine
runtime to port. They are excluded from the engine remit, not missing behavior.
