# TS ⇄ Rust parity map — `ivm` crate

_Deterministic. File edges + symbol pairs are derived from **shared symbol content**, never filenames — so renamed files (e.g. `drain-coordinator.ts`→`drain.rs`) and renamed symbols (`cvrErrorKind`→`CVRStoreError`) still bind. Bodies are not compared; behavior drift needs Layer-2 body review._

- symbols: TS **457**, Rust **895** · resolved pairs **304** (exact 264 + fuzzy 40) + aliases 47
- 🟥 TS UNRESOLVED: **107** (**32** behavioral ⇒ investigate · 75 structural: zod/DDL/type-alias ⇒ serde/inline-SQL, expected) · 🟦 Rust-only ADDED: **591**

> ⚠️ **Behavioral TS symbols with no Rust resolution — check these:** `asQueryImpl` (query/query-impl.ts), `asQueryInternals` (query/query-internals.ts), `cmpLit` (query/expression.ts), `DeepMerge` (query/query-registry.ts), `defineQueries` (query/query-registry.ts), `defineQueriesWithType` (query/query-registry.ts), `defineQuery` (query/query-registry.ts), `defineQueryWithType` (query/query-registry.ts), `deserializePKToConstraint` (ivm/cap.ts), `eb` (query/expression.ts), `filterFalse` (query/expression.ts), `filterTrue` (query/expression.ts), `filterUndefined` (query/expression.ts), `getQuery` (query/query-registry.ts), `isCompoundKey` (query/query-impl.ts), `isOneHop` (query/query-impl.ts), `isParameterReference` (query/expression.ts), `isQuery` (query/query-registry.ts), `isQueryDefinition` (query/query-registry.ts), `isQueryRegistry` (query/query-registry.ts), `isTwoHop` (query/query-impl.ts), `materializeImpl` (query/query-delegate-base.ts), `mustGetQuery` (query/query-registry.ts), `newQuery` (query/query-delegate.ts), `newQueryImpl` (query/query-impl.ts), `normalizeParser` (query/named.ts), `normalizeTTL` (query/ttl.ts), `preloadImpl` (query/query-delegate-base.ts), `syncedQueryImpl` (query/named.ts), `throwQueryNotRunnable` (query/query-impl.ts), `titleCase` (query/validate-input.ts), `withValidation` (query/named.ts)

## 1 · File structure diff

TS origin files: **71**  ·  Rust files: **91** (26 new)

| TS file (LOC) | rel | Rust file(s) (shared syms) |
|---|---|---|
| `builder/builder.ts` (836) | **1:1** | `builder/builder.rs` (20), `permissions.rs` (3), `builder.rs` (2), `query/complete_ordering.rs` (1), `replay.rs` (1), `live_count.rs` (1) |
| `builder/filter.ts` (210) | **1:1** | `builder/filter.rs` (4), `query/expression.rs` (1), `builder/ast.rs` (1) |
| `builder/like-test-cases.ts` (200) | **DROPPED** | — |
| `builder/like.ts` (78) | **1:1** | `builder/like.rs` (3) |
| `ivm/array-view.ts` (188) | **MERGED** | `ivm/array_view.rs` (4), `engine/mod.rs` (2), `ivm/view.rs` (1), `query/typed_view.rs` (1) |
| `ivm/cap.ts` (329) | **MERGED** | `ivm/cap.rs` (7), `engine/mod.rs` (1) |
| `ivm/catch.ts` (138) | **1:1** | `ivm/catch.rs` (5), `ivm/cap.rs` (1), `engine/mod.rs` (1), `replay.rs` (1), `ivm/flipped_join.rs` (1) |
| `ivm/change-index-enum.ts` (9) | **MERGED** | `ivm/change.rs` (3) |
| `ivm/change-index.ts` (5) | **DROPPED** | — |
| `ivm/change-type-enum.ts` (9) | **MERGED** | `ivm/view.rs` (1), `ivm/source.rs` (1), `engine/mod.rs` (1) |
| `ivm/change-type.ts` (5) | **MERGED** | `ivm/change.rs` (1) |
| `ivm/change.ts` (75) | **SPLIT** | `ivm/change.rs` (5), `ivm/take.rs` (3), `ivm/view.rs` (1) |
| `ivm/constraint.ts` (200) | **MERGED** | `ivm/constraint.rs` (8), `ivm/source.rs` (1) |
| `ivm/data.ts` (129) | **MERGED** | `ivm/data.rs` (5) |
| `ivm/default-format.ts` (1) | **DROPPED** | — |
| `ivm/exists.ts` (265) | **MERGED** | `ivm/exists.rs` (4) |
| `ivm/fan-in.ts` (94) | **1:1** | `ivm/fan_in.rs` (1) |
| `ivm/fan-out.ts` (83) | **1:1** | `ivm/fan_out.rs` (2) |
| `ivm/filter-operators.ts` (160) | **MERGED** | `ivm/filter_operators.rs` (6), `ivm/exists.rs` (1) |
| `ivm/filter-push.ts` (38) | **DROPPED** | — |
| `ivm/filter.ts` (57) | **DROPPED** | — |
| `ivm/flipped-join.ts` (611) | **MERGED** | `ivm/flipped_join.rs` (6) |
| `ivm/join-utils.ts` (252) | **MERGED** | `ivm/join.rs` (3), `ivm/join_utils.rs` (2) |
| `ivm/join.ts` (303) | **MERGED** | `ivm/join.rs` (1) |
| `ivm/maybe-split-and-push-edit-change.ts` (27) | **DROPPED** | — |
| `ivm/memory-source.ts` (1180) | **SPLIT** | `sqlite/table_source.rs` (9), `ivm/source.rs` (5), `ivm/data.rs` (1), `ivm/view.rs` (1), `ivm/constraint.rs` (1) |
| `ivm/memory-storage.ts` (50) | **MERGED** | `ivm/memory_storage.rs` (2) |
| `ivm/operator.ts` (140) | **1:1** | `ivm/operator.rs` (7), `ivm/memory_storage.rs` (1), `sqlite/query_builder.rs` (1) |
| `ivm/push-accumulated.ts` (430) | **1:1** | `ivm/push_accumulated.rs` (2) |
| `ivm/schema.ts` (25) | **1:1** | `ivm/schema.rs` (1) |
| `ivm/skip-yields.ts` (46) | **MERGED** | `ivm/filter_operators.rs` (1), `ivm/stream.rs` (1) |
| `ivm/skip.ts` (167) | **MERGED** | `builder/ast.rs` (1), `ivm/skip.rs` (1) |
| `ivm/snitch.ts` (224) | **1:1** | `ivm/snitch.rs` (5), `sqlite/table_source.rs` (1) |
| `ivm/source-change-index-enum.ts` (7) | **MERGED** | `ivm/data.rs` (1) |
| `ivm/source-change-index.ts` (5) | **MERGED** | `replay.rs` (1) |
| `ivm/source.ts` (101) | **SPLIT** | `ivm/source.rs` (4), `ivm/change.rs` (4), `engine/mod.rs` (1) |
| `ivm/stopable-iterator.ts` (23) | **1:1** | `ivm/stopable_iterator.rs` (2) |
| `ivm/stream.ts` (43) | **1:1** | `streamer/mod.rs` (2), `ivm/stream.rs` (1) |
| `ivm/take.ts` (757) | **MERGED** | `ivm/take.rs` (3), `ivm/cap.rs` (1), `ivm/stream.rs` (1) |
| `ivm/union-fan-in.ts` (298) | **1:1** | `ivm/union_fan_in.rs` (2) |
| `ivm/union-fan-out.ts` (57) | **1:1** | `ivm/union_fan_out.rs` (1) |
| `ivm/view-apply-change.ts` (926) | **SPLIT** | `ivm/view.rs` (17), `array_view.rs` (6), `ivm/source.rs` (1) |
| `ivm/view.ts` (31) | **MERGED** | `ivm/view.rs` (2) |
| `planner/planner-builder.ts` (382) | **1:1** | `planner/planner_builder.rs` (13) |
| `planner/planner-connection.ts` (345) | **1:1** | `planner/planner_connection.rs` (9), `engine/mod.rs` (2) |
| `planner/planner-constraint.ts` (21) | **1:1** | `planner/planner_constraint.rs` (1) |
| `planner/planner-fan-in.ts` (241) | **MERGED** | `planner/planner_fan_in.rs` (2) |
| `planner/planner-fan-out.ts` (108) | **1:1** | `planner/planner_fan_out.rs` (4) |
| `planner/planner-graph.ts` (471) | **MERGED** | `planner/planner_graph.rs` (13), `live_count.rs` (1) |
| `planner/planner-join.ts` (473) | **MERGED** | `planner/planner_join.rs` (5), `planner/planner_graph.rs` (1) |
| `planner/planner-node.ts` (70) | **1:1** | `planner/planner_node.rs` (2), `planner/planner_join.rs` (1), `planner/planner_fan_in.rs` (1), `live_count.rs` (1), `sqlite/sqlite_stat_fanout.rs` (1) |
| `planner/planner-source.ts` (36) | **1:1** | `planner/planner_source.rs` (1) |
| `planner/planner-terminus.ts` (40) | **1:1** | `planner/planner_terminus.rs` (1), `planner/runtime.rs` (1) |
| `query/complete-ordering.ts` (93) | **MERGED** | `query/complete_ordering.rs` (3) |
| `query/error.ts` (11) | **1:1** | `query/error.rs` (1) |
| `query/escape-like.ts` (3) | **1:1** | `query/escape_like.rs` (1) |
| `query/expression.ts` (324) | **MERGED** | `query/expression.rs` (8), `ivm/source.rs` (1) |
| `query/measure-push-operator.ts` (62) | **MERGED** | `query/measure_push_operator.rs` (1) |
| `query/metrics-delegate.ts` (34) | **1:1** | `query/metrics_delegate.rs` (3), `query/measure_push_operator.rs` (2) |
| `query/named.ts` (153) | **1:1** | `query/named.rs` (3), `ivm/cap.rs` (1) |
| `query/query-delegate-base.ts` (442) | **MERGED** | `query/query_delegate_base.rs` (12), `ivm/array_view.rs` (1) |
| `query/query-delegate.ts` (141) | **MERGED** | `query/query_delegate_base.rs` (1), `sqlite/query_delegate.rs` (1) |
| `query/query-impl.ts` (597) | **MERGED** | `query/query_internals.rs` (2), `builder/ast.rs` (1) |
| `query/query-internals.ts` (114) | **MERGED** | `query/query_internals.rs` (3) |
| `query/query-registry.ts` (777) | **1:1** | `query/query_registry.rs` (2), `query/query_internals.rs` (1), `snapshotter/snapshotter.rs` (1) |
| `query/runnable-query-impl.ts` (113) | **MERGED** | `query/runnable_query_impl.rs` (1) |
| `query/schema-query.ts` (13) | **DROPPED** | — |
| `query/static-query.ts` (26) | **MERGED** | `query/runnable_query_impl.rs` (2) |
| `query/ttl.ts` (97) | **1:1** | `query/ttl.rs` (5), `credit.rs` (1) |
| `query/typed-view.ts` (23) | **MERGED** | `query/typed_view.rs` (2) |
| `query/validate-input.ts` (62) | **1:1** | `query/validate_input.rs` (2) |

**New Rust files (no TS origin — added in the port):**  `advance_gate.rs` (520), `bin/replay.rs` (18), `bin/server.rs` (881), `builder/mod.rs` (18), `ivm/filter.rs` (105), `ivm/filter_push.rs` (42), `ivm/mod.rs` (60), `ivm/trace.rs` (69), `lib.rs` (46), `otel_metrics.rs` (84), `perf_trace.rs` (146), `planner/mod.rs` (33), `query/mod.rs` (44), `query/query_impl.rs` (274), `query/schema_query.rs` (17), `snapshotter/diff.rs` (452), `snapshotter/mod.rs` (34), `snapshotter/spec.rs` (52), `sqlite/database_storage.rs` (188), `sqlite/db.rs` (235), `sqlite/explain_queries.rs` (47), `sqlite/interrupt.rs` (309), `sqlite/mod.rs` (35), `sqlite/options.rs` (17), `sqlite/resolve_scalar_subqueries.rs` (289), `sqlite/sqlite_cost_model.rs` (666)

**Merges (many TS → one Rust file):**
- `builder/ast.rs` ⟵ `builder/filter.ts`, `ivm/skip.ts`, `query/query-impl.ts`
- `engine/mod.rs` ⟵ `ivm/array-view.ts`, `ivm/cap.ts`, `ivm/catch.ts`, `ivm/change-type-enum.ts`, `ivm/source.ts`, `planner/planner-connection.ts`
- `ivm/array_view.rs` ⟵ `ivm/array-view.ts`, `query/query-delegate-base.ts`
- `ivm/cap.rs` ⟵ `ivm/cap.ts`, `ivm/catch.ts`, `ivm/take.ts`, `query/named.ts`
- `ivm/change.rs` ⟵ `ivm/change-index-enum.ts`, `ivm/change-type.ts`, `ivm/change.ts`, `ivm/source.ts`
- `ivm/constraint.rs` ⟵ `ivm/constraint.ts`, `ivm/memory-source.ts`
- `ivm/data.rs` ⟵ `ivm/data.ts`, `ivm/memory-source.ts`, `ivm/source-change-index-enum.ts`
- `ivm/exists.rs` ⟵ `ivm/exists.ts`, `ivm/filter-operators.ts`
- `ivm/filter_operators.rs` ⟵ `ivm/filter-operators.ts`, `ivm/skip-yields.ts`
- `ivm/flipped_join.rs` ⟵ `ivm/catch.ts`, `ivm/flipped-join.ts`
- `ivm/join.rs` ⟵ `ivm/join-utils.ts`, `ivm/join.ts`
- `ivm/memory_storage.rs` ⟵ `ivm/memory-storage.ts`, `ivm/operator.ts`
- `ivm/source.rs` ⟵ `ivm/change-type-enum.ts`, `ivm/constraint.ts`, `ivm/memory-source.ts`, `ivm/source.ts`, `ivm/view-apply-change.ts`, `query/expression.ts`
- `ivm/stream.rs` ⟵ `ivm/skip-yields.ts`, `ivm/stream.ts`, `ivm/take.ts`
- `ivm/take.rs` ⟵ `ivm/change.ts`, `ivm/take.ts`
- `ivm/view.rs` ⟵ `ivm/array-view.ts`, `ivm/change-type-enum.ts`, `ivm/change.ts`, `ivm/memory-source.ts`, `ivm/view-apply-change.ts`, `ivm/view.ts`
- `live_count.rs` ⟵ `builder/builder.ts`, `planner/planner-graph.ts`, `planner/planner-node.ts`
- `planner/planner_fan_in.rs` ⟵ `planner/planner-fan-in.ts`, `planner/planner-node.ts`
- `planner/planner_graph.rs` ⟵ `planner/planner-graph.ts`, `planner/planner-join.ts`
- `planner/planner_join.rs` ⟵ `planner/planner-join.ts`, `planner/planner-node.ts`
- `query/complete_ordering.rs` ⟵ `builder/builder.ts`, `query/complete-ordering.ts`
- `query/expression.rs` ⟵ `builder/filter.ts`, `query/expression.ts`
- `query/measure_push_operator.rs` ⟵ `query/measure-push-operator.ts`, `query/metrics-delegate.ts`
- `query/query_delegate_base.rs` ⟵ `query/query-delegate-base.ts`, `query/query-delegate.ts`
- `query/query_internals.rs` ⟵ `query/query-impl.ts`, `query/query-internals.ts`, `query/query-registry.ts`
- `query/runnable_query_impl.rs` ⟵ `query/runnable-query-impl.ts`, `query/static-query.ts`
- `query/typed_view.rs` ⟵ `ivm/array-view.ts`, `query/typed-view.ts`
- `replay.rs` ⟵ `builder/builder.ts`, `ivm/catch.ts`, `ivm/source-change-index.ts`
- `sqlite/table_source.rs` ⟵ `ivm/memory-source.ts`, `ivm/snitch.ts`

## 2 · Per-file functional divergence

### `advance_gate.rs`  ⟵  _(new)_


🟦 **Rust-only added here (34):** `ADVANCE_WALL_CLOCK_CEILING_MS`, `AdvanceGate`, `AdvanceReset`, `GateGuard`, `LATE_ADVANCEMENT_FINISH_PROGRESS`, `MAX_PROJECTED_ADVANCEMENT_SAMPLE_CHANGES`, `MIN_ADVANCEMENT_TIME_LIMIT_MS`, `MIN_PROJECTED_ADVANCEMENT_CHANGES`, `MIN_PROJECTED_ADVANCEMENT_SAMPLE_CHANGES`, `MIN_PROJECTED_ADVANCEMENT_SAMPLE_MS`, `PROJECTED_ADVANCEMENT_RESET_MULTIPLIER`, `PROJECTED_ADVANCEMENT_SAMPLE_FRACTION`, `advancement_reset_time_limit_ms`, `arm`, `budget_ms`, `clear_current_change`, `current_change_start_ms`, `drop`, `elapsed`, `elapsed_ms`, `exclude`, `exclude_current`, `new`, `over_budget`, `projected_advancement_time_ms`, `raw_elapsed_ms`, `set_current_change_start`, `set_pos`, `should_finish_late_advancement`, `should_reset_projected_advancement`, `should_reset_slow_current_change`, `should_stop_fetch`, `tripped`, `tripped_reset`

### `bin/replay.rs`  ⟵  _(new)_


🟦 **Rust-only added here (1):** `main`

### `bin/server.rs`  ⟵  _(new)_


🟦 **Rust-only added here (27):** `ServerState`, `change_type_str`, `error_response`, `handle_add_queries`, `handle_add_queries_stream`, `handle_add_row`, `handle_advance`, `handle_advance_stream`, `handle_destroy`, `handle_health`, `handle_init`, `handle_queries`, `handle_remove_query`, `handle_sources`, `handle_version`, `json_response`, `json_to_ast`, `json_to_condition`, `json_to_related_subquery`, `json_to_row`, `json_to_rust_value`, `json_to_simple_condition`, `json_to_value_position`, `read_body`, `row_change_to_json`, `row_to_json`, `rust_value_to_json`

### `builder/ast.rs`  ⟵  `builder/filter.ts`, `ivm/skip.ts`, `query/query-impl.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `ast` (query/query-impl.ts:565) | `Ast` (:10) | exact |
| `Bound` (ivm/skip.ts:24) | `Bound` (:29) | exact |
| `NoSubqueryCondition` (builder/filter.ts:16) | `CorrelatedSubqueryCondition` (:56) | fuzzy 0.50 |

🟦 **Rust-only added here (5):** `Condition`, `OrderPart`, `RelatedSubquery`, `SimpleCondition`, `ValuePosition`

### `builder/builder.rs`  ⟵  `builder/builder.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `applyAnd` (builder/builder.ts:541) | `apply_and` (:274) | exact |
| `applyCorrelatedSubQuery` (builder/builder.ts:650) | `apply_correlated_subquery` (:502) | exact |
| `applyCorrelatedSubqueryCondition` (builder/builder.ts:689) | `apply_correlated_subquery_condition` (:362) | exact |
| `applyFilter` (builder/builder.ts:523) | `apply_filter` (:257) | exact |
| `applyFilterWithFlips` (builder/builder.ts:414) | `apply_filter_with_flips` (:395) | exact |
| `applyOr` (builder/builder.ts:553) | `apply_or` (:289) | exact |
| `applySimpleCondition` (builder/builder.ts:625) | `apply_simple_condition` (:349) | exact |
| `applyWhere` (builder/builder.ts:399) | `apply_where` (:242) | exact |
| `assertNoNotExists` (builder/builder.ts:232) | `assert_no_not_exists` (:652) | exact |
| `BuilderDelegate` (builder/builder.ts:55) | `BuilderDelegate` (:39) | exact |
| `buildPipeline` (builder/builder.ts:126) | `build_pipeline` (:59) | exact |
| `buildPipelineInternal` (builder/builder.ts:256) | `build_pipeline_internal` (:65) | exact |
| `conditionIncludesFlippedSubqueryAtAnyLevel` (builder/builder.ts:807) | `condition_includes_flipped_subquery_at_any_level` (:619) | exact |
| `createStorage` (builder/builder.ts:83) | `create_storage` (:50) | exact |
| `gatherCorrelatedSubqueryQueryConditions` (builder/builder.ts:720) | `gather_correlated_subquery_query_conditions` (:593) | exact |
| `getSource` (builder/builder.ts:77) | `get_source` (:41) | exact |
| `groupSubqueryConditions` (builder/builder.ts:598) | `group_subquery_conditions` (:324) | exact |
| `isNotAndDoesNotContainSubquery` (builder/builder.ts:613) | `is_not_and_does_not_contain_subquery` (:338) | exact |
| `partitionBranches` (builder/builder.ts:822) | `partition_branches` (:631) | exact |
| `uniquifyCorrelatedSubqueryConditionAliases` (builder/builder.ts:763) | `uniquify_correlated_subquery_condition_aliases` (:678) | exact |

🟥 **TS symbols not resolved into this file (1):** `StaticQueryParameters`

🟦 **Rust-only added here (7):** `EXISTS_LIMIT`, `PERMISSIONS_EXISTS_LIMIT`, `apply_correlated_subquery_join`, `complete_ordering_ast`, `enable_not_exists`, `gather_csq_conditions`, `uniquify_condition`

### `builder/filter.rs`  ⟵  `builder/filter.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `createIsPredicate` (builder/filter.ts:97) | `create_simple_predicate` (:39) | fuzzy 0.67 |
| `createPredicate` (builder/filter.ts:27) | `create_predicate` (:17) | exact |
| `createPredicateImpl` (builder/filter.ts:109) | `create_predicate_impl` (:93) | exact |
| `transformFilters` (builder/filter.ts:171) | `transform_filters` (:181) | exact |

🟥 **TS symbols not resolved into this file (3):** `NonNullValue`, `SimplePredicate`, `SimplePredicateNoNull`

🟦 **Rust-only added here (3):** `TransformedFilters`, `json_to_value`, `parse_json_array`

### `builder/like.rs`  ⟵  `builder/like.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `getLikeOp` (builder/like.ts:17) | `get_like_op` (:31) | exact |
| `getLikePredicate` (builder/like.ts:4) | `get_like_predicate` (:15) | exact |

🟦 **Rust-only added here (2):** `is_special_regex_char`, `pattern_to_regex`

### `credit.rs`  ⟵  `query/ttl.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `DEFAULT_TTL` (query/ttl.ts:19) | `default` (:165) | fuzzy 0.50 |

🟦 **Rust-only added here (13):** `Inner`, `POLL`, `StreamCreditGate`, `StreamCreditGuard`, `acquire`, `begin`, `cancel_current`, `close`, `credit_snapshot`, `current_generation`, `gate`, `generation`, `grant`

### `engine/mod.rs`  ⟵  `ivm/array-view.ts`, `ivm/cap.ts`, `ivm/catch.ts`, `ivm/change-type-enum.ts`, `ivm/source.ts`, `planner/planner-connection.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `ConnectionCostModel` (planner/planner-connection.ts:340) | `ensure_cost_model` (:477) | fuzzy 0.50 |
| `destroy` (ivm/array-view.ts:136) | `destroy` (:1574) | exact |
| `FanoutCostModel` (planner/planner-connection.ts:333) | `set_cost_model_conn` (:446) | fuzzy 0.50 |
| `getSchema` (ivm/cap.ts:83) | `get_schema` (:1838) | exact |
| `push` (ivm/array-view.ts:159) | `push` (:294) | exact |
| `REMOVE` (ivm/change-type-enum.ts:2) | `remove_query` (:679) | fuzzy 0.50 |
| `reset` (ivm/catch.ts:84) | `reset` (:1545) | exact |
| `SourceChangeEdit` (ivm/source.ts:15) | `push_source_change` (:1661) | fuzzy 0.50 |

🟦 **Rust-only added here (56):** `AdvanceContext`, `AdvanceToHeadResult`, `Built`, `COLLECTOR_CAP_FLOOR`, `CompanionBuilt`, `CompanionOutput`, `CompanionPipeline`, `Engine`, `EngineDelegate`, `PipelineEntry`, `QueryResult`, `QuerySpec`, `ResetPipelinesSignal`, `ScalarResetError`, `ScalarResolveOut`, `UnusedPusher`, `__test_drop_primary_key`, `add_queries`, `add_queries_streaming`, `advance`, `advance_reset_error`, `advance_streaming`, `advance_to_head_stream`, `apply_client_primary_keys`, `cancel`, `cancellation_token`, `clear_and_cap`, `companion_value_change_records_reset_without_unwinding`, `fmt`, `get_row`, `inactive_source_skips_invalid_change`, `initialized`, `is_cancelled`, `js_scalar_string`, `pipeline_query_ids`, `plan_ast`, `planned_flips_for_test`, `register_source`, `resolve_scalar_subqueries`, `rollback_source_connections`, `row_set_signature`, `row_signature_unit`, `row_signature_unit_matches_ts_golden`, `scalar_values_equal`, `set_client_primary_keys`, `set_cost_model_table_specs`, `set_hydration_time_ms`, `set_table_spec`, `set_unique_keys`, `should_abort`, `source_connection_checkpoint`, `sources`, `sqlite_value_to_row`, `take_scalar_reset`, `total_hydration_time_ms`, `transformed_ast`

### `ivm/array_view.rs`  ⟵  `ivm/array-view.ts`, `query/query-delegate-base.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `addListener` (ivm/array-view.ts:115) | `add_listener` (:79) | exact |
| `ArrayView` (ivm/array-view.ts:50) | `ArrayView` (:21) | exact |
| `arrayViewFactory` (query/query-delegate-base.ts:420) | `ArrayViewOutput` (:126) | fuzzy 0.50 |
| `data` (ivm/array-view.ts:111) | `data` (:74) | exact |
| `flush` (ivm/array-view.ts:173) | `flush` (:88) | exact |

🟦 **Rust-only added here (1):** `hydrate`

### `ivm/cap.rs`  ⟵  `ivm/cap.ts`, `ivm/catch.ts`, `ivm/take.ts`, `query/named.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `Cap` (ivm/cap.ts:52) | `Cap` (:88) | exact |
| `del` (ivm/cap.ts:33) | `del` (:52) | exact |
| `fetch` (ivm/catch.ts:65) | `fetch` (:269) | exact |
| `get` (ivm/cap.ts:31) | `get` (:44) | exact |
| `getCapStateKey` (ivm/cap.ts:300) | `CapState` (:26) | fuzzy 0.67 |
| `getTakeStateKey` (ivm/take.ts:710) | `get_take_state_key` (:129) | exact |
| `ParseFn` (query/named.ts:140) | `parse_value` (:564) | fuzzy 0.50 |
| `serializePK` (ivm/cap.ts:315) | `serialize_pk` (:155) | exact |
| `set` (ivm/cap.ts:32) | `set` (:48) | exact |
| `setOutput` (ivm/cap.ts:79) | `set_output` (:265) | exact |

🟥 **TS symbols not resolved into this file (1):** `deserializePKToConstraint`

🟦 **Rust-only added here (11):** `CapInitialFetchGuard`, `CapOutput`, `CapStorage`, `initial_fetch`, `parse_json_array_elements`, `plain_string_pk_is_byte_identical_and_roundtrips`, `quoted_pk_does_not_break_array_element_split`, `roundtrip`, `string_pk_with_quote_and_backslash_roundtrips`, `unescape_json_string`, `value_to_string`

### `ivm/catch.rs`  ⟵  `ivm/catch.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `Catch` (ivm/catch.ts:53) | `Catch` (:41) | exact |
| `CaughtChange` (ivm/catch.ts:43) | `CaughtChange` (:22) | exact |
| `CaughtNode` (ivm/catch.ts:11) | `CaughtNode` (:15) | exact |
| `expandChange` (ivm/catch.ts:93) | `expand_change` (:119) | exact |
| `expandNode` (ivm/catch.ts:125) | `expand_node` (:142) | exact |

🟥 **TS symbols not resolved into this file (2):** `CaughtEditChange`, `CaughtRemoveChange`

🟦 **Rust-only added here (1):** `CatchOutput`

### `ivm/change.rs`  ⟵  `ivm/change-index-enum.ts`, `ivm/change-type.ts`, `ivm/change.ts`, `ivm/source.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `Change` (ivm/change.ts:12) | `Change` (:33) | exact |
| `ChangeType` (ivm/change-type.ts:5) | `ChangeType` (:12) | exact |
| `CHILD_DATA` (ivm/change-index-enum.ts:4) | `ChildData` (:21) | exact |
| `makeAddChange` (ivm/change.ts:61) | `make_add_change` (:81) | exact |
| `makeChildChange` (ivm/change.ts:69) | `make_child_change` (:89) | exact |
| `makeEditChange` (ivm/change.ts:73) | `make_edit_change` (:93) | exact |
| `makeRemoveChange` (ivm/change.ts:65) | `make_remove_change` (:85) | exact |
| `makeSourceChangeAdd` (ivm/source.ts:22) | `make_source_change_add` (:126) | exact |
| `makeSourceChangeEdit` (ivm/source.ts:30) | `make_source_change_edit` (:134) | exact |
| `makeSourceChangeRemove` (ivm/source.ts:26) | `make_source_change_remove` (:130) | exact |
| `NODE` (ivm/change-index-enum.ts:2) | `node` (:52) | exact |
| `OLD_NODE` (ivm/change-index-enum.ts:3) | `old_node` (:72) | exact |
| `SourceChange` (ivm/source.ts:17) | `SourceChange` (:102) | exact |

🟥 **TS symbols not resolved into this file (1):** `TYPE`

🟦 **Rust-only added here (1):** `node_mut`

### `ivm/constraint.rs`  ⟵  `ivm/constraint.ts`, `ivm/memory-source.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `constraintEquals` (ivm/constraint.ts:185) | `constraint_equals` (:154) | exact |
| `constraintMatchesPrimaryKey` (ivm/constraint.ts:46) | `constraint_matches_primary_key` (:39) | exact |
| `constraintMatchesRow` (ivm/constraint.ts:17) | `constraint_matches_row` (:15) | exact |
| `constraintsAreCompatible` (ivm/constraint.ts:34) | `constraints_are_compatible` (:27) | exact |
| `extractColumn` (ivm/constraint.ts:147) | `extract_column` (:112) | exact |
| `keyMatchesPrimaryKey` (ivm/constraint.ts:53) | `key_matches_primary_key` (:76) | exact |
| `primaryKeyConstraintFromFilters` (ivm/constraint.ts:114) | `primary_key_constraint_from_filters` (:125) | exact |
| `pullSimpleAndComponents` (ivm/constraint.ts:91) | `pull_simple_and_components` (:96) | exact |
| `rowMatchesPK` (ivm/memory-source.ts:976) | `row_matches_multi_constraints` (:59) | fuzzy 0.40 |

🟥 **TS symbols not resolved into this file (2):** `Constraint`, `SetOfConstraint`

### `ivm/data.rs`  ⟵  `ivm/data.ts`, `ivm/memory-source.ts`, `ivm/source-change-index-enum.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `compareValues` (ivm/data.ts:32) | `compare_values` (:131) | exact |
| `drainStreams` (ivm/data.ts:120) | `drain_streams` (:381) | exact |
| `makeBoundComparator` (ivm/memory-source.ts:997) | `make_partial_bound_comparator` (:318) | fuzzy 0.75 |
| `makeComparator` (ivm/data.ts:91) | `make_comparator` (:292) | exact |
| `ROW` (ivm/source-change-index-enum.ts:2) | `row` (:274) | exact |
| `valuesEqual` (ivm/data.ts:112) | `values_equal` (:199) | exact |

🟥 **TS symbols not resolved into this file (3):** `Comparator`, `NormalizedValue`, `OLD_ROW`

🟦 **Rust-only added here (15):** `MAX_SAFE`, `Value`, `cloned_json_preserves_reference_identity`, `comparator_errors_match_javascript_messages`, `deserialize`, `eq`, `independently_parsed_json_is_not_equal_or_orderable`, `is_null`, `js_json_string`, `js_stringify_value`, `js_stringify_value_matches_json_stringify`, `js_typeof`, `js_value_string`, `serialize`, `set_relationship`

### `ivm/exists.rs`  ⟵  `ivm/exists.ts`, `ivm/filter-operators.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `beginFilter` (ivm/exists.ts:71) | `begin_filter` (:159) | exact |
| `endFilter` (ivm/exists.ts:75) | `end_filter` (:166) | exact |
| `Exists` (ivm/exists.ts:21) | `Exists` (:36) | exact |
| `filter` (ivm/filter-operators.ts:37) | `filter` (:176) | exact |
| `setFilterOutput` (ivm/exists.ts:67) | `set_filter_output` (:153) | exact |

🟦 **Rust-only added here (7):** `InPushReset`, `fetch_exists`, `fetch_size`, `filter_inner`, `get_cache_key`, `push_to_output`, `push_with_filter`

### `ivm/fan_in.rs`  ⟵  `ivm/fan-in.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `FanIn` (ivm/fan-in.ts:30) | `FanIn` (:21) | exact |

🟦 **Rust-only added here (1):** `fan_out_done_pushing_to_all_branches`

### `ivm/fan_out.rs`  ⟵  `ivm/fan-out.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `FanOut` (ivm/fan-out.ts:17) | `FanOut` (:22) | exact |
| `setFanIn` (ivm/fan-out.ts:28) | `set_fan_in` (:48) | exact |

### `ivm/filter_operators.rs`  ⟵  `ivm/filter-operators.ts`, `ivm/skip-yields.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `buildFilterPipeline` (ivm/filter-operators.ts:148) | `build_filter_pipeline` (:275) | exact |
| `FilterEnd` (ivm/filter-operators.ts:106) | `FilterEnd` (:195) | exact |
| `FilterInput` (ivm/filter-operators.ts:27) | `FilterInput` (:28) | exact |
| `FilterOutput` (ivm/filter-operators.ts:32) | `FilterOutput` (:39) | exact |
| `FilterStart` (ivm/filter-operators.ts:61) | `FilterStart` (:52) | exact |
| `next` (ivm/skip-yields.ts:25) | `next` (:162) | exact |
| `throwFilterOutput` (ivm/filter-operators.ts:48) | `FilterOutputAsOutput` (:287) | fuzzy 0.67 |

🟥 **TS symbols not resolved into this file (1):** `FilterOperator`

🟦 **Rust-only added here (4):** `FilterChainPusher`, `FilterEndAsFilterOutput`, `FilterStartOutput`, `FilterStartStream`

### `ivm/filter_push.rs`  ⟵  _(new)_


🟦 **Rust-only added here (1):** `filter_push`

### `ivm/flipped_join.rs`  ⟵  `ivm/catch.ts`, `ivm/flipped-join.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `canonicalKey` (ivm/flipped-join.ts:585) | `canonical_key` (:578) | exact |
| `canonicalKeyForTest` (ivm/flipped-join.ts:572) | `canonical_key_row` (:573) | fuzzy 0.40 |
| `canonicalValue` (ivm/flipped-join.ts:600) | `canonical_value` (:593) | exact |
| `CaughtChildChange` (ivm/catch.ts:28) | `push_child_change` (:301) | fuzzy 0.50 |
| `FlippedJoin` (ivm/flipped-join.ts:93) | `FlippedJoin` (:62) | exact |
| `getMultiConstraintChunkSize` (ivm/flipped-join.ts:60) | `get_multi_constraint_chunk_size` (:40) | exact |
| `setMultiConstraintChunkSizeForTest` (ivm/flipped-join.ts:65) | `set_multi_constraint_chunk_size_for_test` (:44) | exact |

🟦 **Rust-only added here (8):** `ChildOutput`, `FlippedJoinArgs`, `InprogressGuard`, `MULTI_CONSTRAINT_CHUNK_SIZE`, `MULTI_CONSTRAINT_CHUNK_SIZE_TEST`, `ParentOutput`, `fetch_batched`, `push_parent_change`

### `ivm/join.rs`  ⟵  `ivm/join-utils.ts`, `ivm/join.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `buildJoinConstraint` (ivm/join-utils.ts:238) | `build_join_constraint` (:450) | exact |
| `isJoinMatch` (ivm/join-utils.ts:219) | `is_join_match` (:466) | exact |
| `Join` (ivm/join.ts:51) | `Join` (:33) | exact |
| `rowEqualsForCompoundKey` (ivm/join-utils.ts:206) | `row_equals_for_compound_key` (:482) | exact |

🟦 **Rust-only added here (5):** `JoinArgs`, `fetch_lazy`, `process_parent_node`, `push_child`, `push_parent`

### `ivm/join_utils.rs`  ⟵  `ivm/join-utils.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `generateWithOverlayNoYield` (ivm/join-utils.ts:11) | `generate_with_overlay_no_yield` (:368) | exact |
| `generateWithOverlayNoYieldUnordered` (ivm/join-utils.ts:126) | `generate_with_overlay_no_yield_unordered` (:378) | exact |

🟦 **Rust-only added here (3):** `generate_with_overlay`, `generate_with_overlay_unordered`, `generate_with_start`

### `ivm/memory_storage.rs`  ⟵  `ivm/memory-storage.ts`, `ivm/operator.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `MemoryStorage` (ivm/memory-storage.ts:17) | `MemoryStorage` (:13) | exact |
| `scan` (ivm/operator.ts:138) | `scan` (:44) | exact |

### `ivm/operator.rs`  ⟵  `ivm/operator.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `FetchRequest` (ivm/operator.ts:63) | `FetchRequest` (:24) | exact |
| `Input` (ivm/operator.ts:26) | `Input` (:49) | exact |
| `InputBase` (ivm/operator.ts:14) | `InputBase` (:44) | exact |
| `Output` (ivm/operator.ts:93) | `Output` (:54) | exact |
| `Start` (ivm/operator.ts:84) | `Start` (:33) | exact |
| `Storage` (ivm/operator.ts:132) | `Storage` (:58) | exact |
| `throwOutput` (ivm/operator.ts:114) | `ThrowOutput` (:68) | exact |

🟥 **TS symbols not resolved into this file (1):** `Operator`

🟦 **Rust-only added here (1):** `Basis`

### `ivm/push_accumulated.rs`  ⟵  `ivm/push-accumulated.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `makeAddEmptyRelationships` (ivm/push-accumulated.ts:369) | `add_empty_relationships` (:132) | fuzzy 0.75 |
| `mergeRelationships` (ivm/push-accumulated.ts:265) | `merge_relationships` (:23) | exact |

🟦 **Rust-only added here (1):** `push_accumulated_changes`

### `ivm/schema.rs`  ⟵  `ivm/schema.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `SourceSchema` (ivm/schema.ts:9) | `SourceSchema` (:24) | exact |

🟦 **Rust-only added here (3):** `ColumnType`, `System`, `with_relationship`

### `ivm/skip.rs`  ⟵  `ivm/skip.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `Skip` (ivm/skip.ts:33) | `Skip` (:18) | exact |

🟦 **Rust-only added here (2):** `SkipOutput`, `should_be_present`

### `ivm/snitch.rs`  ⟵  `ivm/snitch.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `ChangeRecord` (ivm/snitch.ts:194) | `ChangeRecord` (:24) | exact |
| `LogType` (ivm/snitch.ts:224) | `LogType` (:16) | exact |
| `Snitch` (ivm/snitch.ts:25) | `Snitch` (:50) | exact |
| `SnitchMessage` (ivm/snitch.ts:183) | `SnitchMessage` (:33) | exact |
| `toChangeRecord` (ivm/snitch.ts:94) | `to_change_record` (:189) | exact |

🟥 **TS symbols not resolved into this file (8):** `AddChangeRecord`, `ChildChangeRecord`, `EditChangeRecord`, `FetchMessage`, `FilterMessage`, `FilterSnitch`, `PushMessage`, `RemoveChangeRecord`

🟦 **Rust-only added here (3):** `SnitchOutput`, `clone_ref`, `log_message`

### `ivm/source.rs`  ⟵  `ivm/change-type-enum.ts`, `ivm/constraint.ts`, `ivm/memory-source.ts`, `ivm/source.ts`, `ivm/view-apply-change.ts`, `query/expression.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `applyChange` (ivm/view-apply-change.ts:185) | `apply_change` (:483) | exact |
| `applyMultiConstraintsToOverlays` (ivm/memory-source.ts:795) | `apply_source_overlays` (:1045) | fuzzy 0.40 |
| `cmp` (query/expression.ts:73) | `cmp` (:1481) | exact |
| `connect` (ivm/memory-source.ts:162) | `connect` (:53) | exact |
| `Connection` (ivm/memory-source.ts:75) | `Connection` (:104) | exact |
| `EDIT` (ivm/change-type-enum.ts:3) | `StableEdit` (:1287) | fuzzy 0.50 |
| `genPush` (ivm/source.ts:96) | `gen_push` (:65) | exact |
| `has` (ivm/constraint.ts:173) | `has` (:261) | exact |
| `MemorySource` (ivm/memory-source.ts:98) | `MemorySource` (:128) | exact |
| `Overlay` (ivm/memory-source.ts:59) | `OverlayGuard` (:117) | fuzzy 0.50 |
| `Source` (ivm/source.ts:54) | `Source` (:38) | exact |
| `SourceChangeAdd` (ivm/source.ts:9) | `source_change_to_change` (:539) | fuzzy 0.67 |
| `SourceInput` (ivm/source.ts:99) | `SourceInput` (:628) | exact |

🟥 **TS symbols not resolved into this file (1):** `SourceChangeRemove`

🟦 **Rust-only added here (34):** `CollectOutput`, `CollectStreamConfig`, `EmptyInput`, `HeapEntry`, `HistoricalOverlayContext`, `KWayMerge`, `SourcePusher`, `add_row`, `all_rows`, `apply_overlay_and_stream`, `apply_source_overlay`, `apply_source_overlay_impl`, `clear_advance_state`, `column_names`, `column_types`, `compute_index_compare`, `configure_streaming`, `connection_count`, `has_active_connections`, `has_db`, `historical_edit_with_unchanged_json_sort_key_replaces_in_place`, `merge_sorted_streams`, `partial_cmp`, `pk_key`, `primary_key`, `push_internal`, `rows_equal_on`, `rows_storage_equal_on`, `set_db_path`, `set_primary_key`, `set_snapshot_db`, `storage_values_equal`, `table_name`, `truncate_connections`

### `ivm/stopable_iterator.rs`  ⟵  `ivm/stopable-iterator.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `stop` (ivm/stopable-iterator.ts:20) | `stop` (:23) | exact |
| `StoppableIterator` (ivm/stopable-iterator.ts:5) | `StoppableIterator` (:10) | exact |

🟦 **Rust-only added here (1):** `is_stopped`

### `ivm/stream.rs`  ⟵  `ivm/skip-yields.ts`, `ivm/stream.ts`, `ivm/take.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `first` (ivm/stream.ts:23) | `first` (:102) | exact |
| `skipYields` (ivm/skip-yields.ts:44) | `skip_yields` (:56) | exact |
| `Take` (ivm/take.ts:55) | `take` (:65) | exact |

🟦 **Rust-only added here (9):** `StreamItem`, `TakeStream`, `count_data`, `empty_rel`, `empty_stream`, `from_vec`, `node_stream`, `rel_from_vec`, `single_node`

### `ivm/take.rs`  ⟵  `ivm/change.ts`, `ivm/take.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `AddChange` (ivm/change.ts:17) | `push_add_change` (:469) | fuzzy 0.67 |
| `constraintMatchesPartitionKey` (ivm/take.ts:727) | `constraint_matches_partition_key` (:1069) | exact |
| `EditChange` (ivm/change.ts:57) | `push_edit_change` (:686) | fuzzy 0.67 |
| `makePartitionKeyComparator` (ivm/take.ts:745) | `make_partition_key_comparator` (:1051) | exact |
| `PartitionKey` (ivm/take.ts:42) | `optional_constraint_matches_partition_key` (:1022) | fuzzy 0.40 |
| `RemoveChange` (ivm/change.ts:22) | `push_remove_change` (:581) | fuzzy 0.67 |

🟦 **Rust-only added here (17):** `HiddenRowGuard`, `InitialFetchGuard`, `MAX_BOUND_KEY`, `NoopOutput`, `TakeOutput`, `TakeState`, `TakeStorage`, `compare_rows`, `edit_on_empty_partition_panics_bound_should_be_set`, `get_state_and_constraint`, `mk_row`, `push_change`, `push_with_row_hidden_from_fetch`, `set_take_state`, `storage_round_trip_row`, `take_state_key_for_constraint`, `take_state_key_for_row`

### `ivm/trace.rs`  ⟵  _(new)_


🟦 **Rust-only added here (6):** `ENABLED`, `describe`, `emit`, `id`, `note`, `recv`

### `ivm/union_fan_in.rs`  ⟵  `ivm/union-fan-in.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `fanOutStartedPushing` (ivm/union-fan-in.ts:185) | `fan_out_started_pushing` (:109) | exact |
| `UnionFanIn` (ivm/union-fan-in.ts:25) | `UnionFanIn` (:30) | exact |

🟦 **Rust-only added here (6):** `UfiOutput`, `add_input`, `fan_out_done_pushing`, `merge_fetches`, `output_adapter`, `push_internal_change`

### `ivm/union_fan_out.rs`  ⟵  `ivm/union-fan-out.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `UnionFanOut` (ivm/union-fan-out.ts:11) | `UnionFanOut` (:15) | exact |

🟦 **Rust-only added here (1):** `UfoOutput`

### `ivm/view.rs`  ⟵  `ivm/array-view.ts`, `ivm/change-type-enum.ts`, `ivm/change.ts`, `ivm/memory-source.ts`, `ivm/view-apply-change.ts`, `ivm/view.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `applyChangeInternal` (ivm/view-apply-change.ts:213) | `apply_change_internal` (:237) | exact |
| `applyChanges` (ivm/view-apply-change.ts:555) | `apply_changes` (:212) | exact |
| `applyEdit` (ivm/view-apply-change.ts:579) | `apply_edit` (:741) | exact |
| `binarySearch` (ivm/view-apply-change.ts:767) | `binary_search` (:876) | exact |
| `changeToViewChange` (ivm/array-view.ts:15) | `change_to_view_change` (:996) | exact |
| `CHILD` (ivm/change-type-enum.ts:4) | `apply_child` (:571) | fuzzy 0.50 |
| `ChildChange` (ivm/change.ts:29) | `ChildViewChange` (:175) | fuzzy 0.67 |
| `compareBounds` (ivm/memory-source.ts:1023) | `compare` (:77) | fuzzy 0.50 |
| `decRefCount` (ivm/view-apply-change.ts:866) | `dec_ref_count` (:934) | exact |
| `Entry` (ivm/view.ts:11) | `Entry` (:54) | exact |
| `ExpandedNode` (ivm/view-apply-change.ts:45) | `ExpandedNode` (:91) | exact |
| `getChildEntryList` (ivm/view-apply-change.ts:821) | `get_child_entry_list` (:919) | exact |
| `getOptionalSingularEntry` (ivm/view-apply-change.ts:808) | `get_optional_singular_entry` (:908) | exact |
| `getSingularEntry` (ivm/view-apply-change.ts:797) | `get_singular_entry` (:900) | exact |
| `incRefCount` (ivm/view-apply-change.ts:859) | `inc_ref_count` (:927) | exact |
| `initializeRelationshipsForNewEntryIfAny` (ivm/view-apply-change.ts:625) | `initialize_relationships_for_new_entry_if_any` (:764) | exact |
| `makeID` (ivm/view-apply-change.ts:851) | `make_id` (:850) | exact |
| `makeNewMetaEntry` (ivm/view-apply-change.ts:831) | `make_new_meta_entry` (:841) | exact |
| `removeAndUpdateRefCount` (ivm/view-apply-change.ts:744) | `remove_and_update_ref_count` (:544) | exact |
| `RowOnlyNode` (ivm/view-apply-change.ts:68) | `RowOnlyNode` (:169) | exact |
| `View` (ivm/view.ts:9) | `View` (:45) | exact |
| `ViewChange` (ivm/view-apply-change.ts:62) | `ViewChange` (:149) | exact |
| `ViewNode` (ivm/view-apply-change.ts:55) | `ViewNode` (:99) | exact |

🟥 **TS symbols not resolved into this file (9):** `ADD`, `AddViewChange`, `AnyViewFactory`, `EntryList`, `RefCountMap`, `RemoveViewChange`, `ViewFactory`, `idSymbol`, `refCountSymbol`

🟦 **Rust-only added here (19):** `AddResult`, `Format`, `add_to_list`, `apply_add_plural`, `apply_add_singular`, `apply_change_hidden`, `apply_edit_plural`, `apply_edit_singular`, `apply_remove_plural`, `apply_remove_singular`, `children`, `default_format`, `empty_root_entry`, `entries_equal`, `relationship_names`, `set_relation`, `value_to_json_string`, `view_equal`, `views_equal`

### `live_count.rs`  ⟵  `builder/builder.ts`, `planner/planner-graph.ts`, `planner/planner-node.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `decorateSourceInput` (builder/builder.ts:91) | `TABLE_SOURCE_INPUT` (:12) | fuzzy 0.50 |
| `PlannerGraph` (planner/planner-graph.ts:42) | `PLANNER_GRAPH` (:21) | exact |
| `PlannerNode` (planner/planner-node.ts:11) | `PLANNER_NODE` (:23) | exact |

🟦 **Rust-only added here (5):** `TABLE_CONNECTION`, `TABLE_SOURCE`, `dec`, `inc`, `snapshot`

### `otel_metrics.rs`  ⟵  _(new)_


🟦 **Rust-only added here (5):** `LATENCY_BOUNDARIES_S`, `advance_time`, `conflict_rows_deleted`, `record_conflict_row_deleted`, `record_ivm_advance`

### `perf_trace.rs`  ⟵  _(new)_


🟦 **Rust-only added here (7):** `ON`, `STATS`, `Scope`, `VAL`, `env_value`, `report`, `report_residual`

### `planner/planner_builder.rs`  ⟵  `planner/planner-builder.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `applyPlansToAST` (planner/planner-builder.ts:357) | `apply_plans_to_ast` (:276) | exact |
| `applyToCondition` (planner/planner-builder.ts:322) | `apply_to_condition` (:306) | exact |
| `buildPlanGraph` (planner/planner-builder.ts:42) | `build_plan_graph` (:200) | exact |
| `extractConstraint` (planner/planner-builder.ts:293) | `extract_constraint` (:22) | exact |
| `hasCorrelatedSubquery` (planner/planner-builder.ts:282) | `has_correlated_subquery` (:26) | exact |
| `planQuery` (planner/planner-builder.ts:311) | `plan_query` (:269) | exact |
| `planRecursively` (planner/planner-builder.ts:300) | `plan_recursively` (:262) | exact |
| `Plans` (planner/planner-builder.ts:37) | `Plans` (:17) | exact |
| `processCondition` (planner/planner-builder.ts:100) | `process_condition` (:52) | exact |
| `processCorrelatedSubquery` (planner/planner-builder.ts:192) | `process_correlated_subquery` (:120) | exact |
| `wireOutput` (planner/planner-builder.ts:22) | `wire_output` (:34) | exact |

🟦 **Rust-only added here (1):** `order_to_tuples`

### `planner/planner_connection.rs`  ⟵  `planner/planner-connection.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `captureConstraints` (planner/planner-connection.ts:281) | `capture_constraints` (:182) | exact |
| `closestJoinOrSource` (planner/planner-connection.ts:148) | `closest_join_or_source` (:100) | exact |
| `CostModelCost` (planner/planner-connection.ts:335) | `CostModelCost` (:12) | exact |
| `estimateCost` (planner/planner-connection.ts:187) | `estimate_cost` (:119) | exact |
| `PlannerConnection` (planner/planner-connection.ts:59) | `PlannerConnection` (:27) | exact |
| `propagateConstraints` (planner/planner-connection.ts:166) | `propagate_constraints` (:104) | exact |
| `propagateUnlimitFromFlippedJoin` (planner/planner-connection.ts:266) | `propagate_unlimit_from_flipped_join` (:172) | exact |
| `restoreConstraints` (planner/planner-connection.ts:289) | `restore_constraints` (:186) | exact |
| `unlimit` (planner/planner-connection.ts:249) | `unlimit` (:165) | exact |

### `planner/planner_constraint.rs`  ⟵  `planner/planner-constraint.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `mergeConstraints` (planner/planner-constraint.ts:14) | `merge_constraints` (:19) | exact |

🟥 **TS symbols not resolved into this file (1):** `PlannerConstraint`

### `planner/planner_fan_in.rs`  ⟵  `planner/planner-fan-in.ts`, `planner/planner-node.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `convertToUFI` (planner/planner-fan-in.ts:60) | `convert_to_ufi` (:42) | exact |
| `NodeType` (planner/planner-node.ts:66) | `node_type` (:26) | exact |
| `PlannerFanIn` (planner/planner-fan-in.ts:28) | `PlannerFanIn` (:8) | exact |

### `planner/planner_fan_out.rs`  ⟵  `planner/planner-fan-out.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `addOutput` (planner/planner-fan-out.ts:26) | `add_output` (:30) | exact |
| `convertToUFO` (planner/planner-fan-out.ts:86) | `convert_to_ufo` (:64) | exact |
| `outputs` (planner/planner-fan-out.ts:30) | `outputs` (:37) | exact |
| `PlannerFanOut` (planner/planner-fan-out.ts:11) | `PlannerFanOut` (:8) | exact |

### `planner/planner_graph.rs`  ⟵  `planner/planner-graph.ts`, `planner/planner-join.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `addSource` (planner/planner-graph.ts:71) | `add_source` (:59) | exact |
| `buildFOFICache` (planner/planner-graph.ts:389) | `build_fofi_cache` (:233) | exact |
| `capturePlanningSnapshot` (planner/planner-graph.ts:136) | `capture_planning_snapshot` (:108) | exact |
| `checkAndConvertFOFI` (planner/planner-graph.ts:406) | `check_and_convert_fofi` (:298) | exact |
| `findFIAndJoins` (planner/planner-graph.ts:420) | `find_fi_and_joins` (:242) | exact |
| `getTotalCost` (planner/planner-graph.ts:122) | `get_total_cost` (:103) | exact |
| `hasSource` (planner/planner-graph.ts:93) | `has_source` (:55) | exact |
| `plan` (planner/planner-graph.ts:256) | `plan` (:154) | exact |
| `PlanState` (planner/planner-graph.ts:18) | `PlanState` (:19) | exact |
| `propagateUnlimit` (planner/planner-join.ts:186) | `propagate_unlimit` (:311) | exact |
| `resetPlanningState` (planner/planner-graph.ts:61) | `reset_planning_state` (:82) | exact |
| `restorePlanningSnapshot` (planner/planner-graph.ts:157) | `restore_planning_snapshot` (:130) | exact |
| `setTerminus` (planner/planner-graph.ts:101) | `set_terminus` (:78) | exact |

🟦 **Rust-only added here (3):** `FofiInfo`, `MAX_FLIPPABLE_JOINS`, `connect_source`

### `planner/planner_join.rs`  ⟵  `planner/planner-join.ts`, `planner/planner-node.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `flip` (planner/planner-join.ts:154) | `flip` (:84) | exact |
| `getName` (planner/planner-join.ts:427) | `get_name` (:211) | exact |
| `isFlippable` (planner/planner-join.ts:167) | `is_flippable` (:93) | exact |
| `JoinType` (planner/planner-node.ts:70) | `join_type` (:90) | exact |
| `PlannerJoin` (planner/planner-join.ts:96) | `PlannerJoin` (:37) | exact |
| `translateConstraintsForFlippedJoin` (planner/planner-join.ts:27) | `translate_constraints_for_flipped_join` (:8) | exact |

🟥 **TS symbols not resolved into this file (1):** `UnflippableJoinError`

🟦 **Rust-only added here (1):** `get_output`

### `planner/planner_node.rs`  ⟵  `planner/planner-node.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `CostEstimate` (planner/planner-node.ts:18) | `CostEstimate` (:19) | exact |
| `JoinOrConnection` (planner/planner-node.ts:68) | `JoinOrConnection` (:81) | exact |

🟦 **Rust-only added here (10):** `Confidence`, `FanInType`, `FanOutType`, `FanoutEst`, `NodeKind`, `PlannerNodeWeak`, `downgrade`, `kind`, `name`, `upgrade`

### `planner/planner_source.rs`  ⟵  `planner/planner-source.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `PlannerSource` (planner/planner-source.ts:10) | `PlannerSource` (:7) | exact |

### `planner/planner_terminus.rs`  ⟵  `planner/planner-terminus.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `PlannerTerminus` (planner/planner-terminus.ts:8) | `PlannerTerminus` (:5) | exact |

### `planner/runtime.rs`  ⟵  `planner/planner-terminus.ts`


🟦 **Rust-only added here (7):** `cost_model_with_cache`, `create_snapshot_cost_model`, `create_snapshot_cost_model_cached`, `flip_order`, `flip_order_condition`, `plan_ast_flips`, `row_count`

### `query/complete_ordering.rs`  ⟵  `builder/builder.ts`, `query/complete-ordering.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `addPrimaryKeys` (query/complete-ordering.ts:74) | `add_primary_keys` (:81) | exact |
| `assertOrderingIncludesPK` (builder/builder.ts:742) | `assert_ordering_includes_pk` (:39) | exact |
| `completeOrdering` (query/complete-ordering.ts:6) | `complete_ordering` (:10) | exact |
| `completeOrderingInCondition` (query/complete-ordering.ts:46) | `complete_ordering_in_condition` (:54) | exact |

### `query/error.rs`  ⟵  `query/error.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `QueryParseError` (query/error.ts:1) | `QueryParseError` (:8) | exact |

🟦 **Rust-only added here (1):** `NotImplementedError`

### `query/escape_like.rs`  ⟵  `query/escape-like.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `escapeLike` (query/escape-like.ts:1) | `escape_like` (:8) | exact |

### `query/expression.rs`  ⟵  `builder/filter.ts`, `query/expression.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `and` (query/expression.ts:134) | `and` (:11) | exact |
| `flatten` (query/expression.ts:269) | `flatten` (:134) | exact |
| `isAlwaysFalse` (query/expression.ts:245) | `is_always_false` (:194) | exact |
| `isAlwaysTrue` (query/expression.ts:241) | `is_always_true` (:190) | exact |
| `negateOperator` (query/expression.ts:308) | `negate_operator` (:158) | exact |
| `not` (builder/filter.ts:158) | `not` (:51) | exact |
| `or` (query/expression.ts:148) | `or` (:31) | exact |
| `simplifyCondition` (query/expression.ts:249) | `simplify_condition` (:104) | exact |
| `TRUE` (query/expression.ts:231) | `true_val` (:181) | fuzzy 0.50 |

🟥 **TS symbols not resolved into this file (9):** `ExpressionBuilder`, `ExpressionFactory`, `ParameterReference`, `cmpLit`, `eb`, `filterFalse`, `filterTrue`, `filterUndefined`, `isParameterReference`

🟦 **Rust-only added here (2):** `cmp_eq`, `false_val`

### `query/measure_push_operator.rs`  ⟵  `query/measure-push-operator.ts`, `query/metrics-delegate.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `addMetric` (query/metrics-delegate.ts:17) | `add_metric` (:16) | exact |
| `MeasurePushOperator` (query/measure-push-operator.ts:16) | `MeasurePushOperator` (:27) | exact |
| `MetricsDelegate` (query/metrics-delegate.ts:16) | `MetricsDelegate` (:15) | exact |

🟦 **Rust-only added here (2):** `MeasureOutput`, `NullMetricsDelegate`

### `query/metrics_delegate.rs`  ⟵  `query/metrics-delegate.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `isClientMetric` (query/metrics-delegate.ts:24) | `is_client_metric` (:29) | exact |
| `isServerMetric` (query/metrics-delegate.ts:30) | `is_server_metric` (:38) | exact |
| `MetricMap` (query/metrics-delegate.ts:14) | `Metric` (:10) | fuzzy 0.50 |

🟥 **TS symbols not resolved into this file (2):** `ClientMetricMap`, `ServerMetricMap`

### `query/named.rs`  ⟵  `query/named.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `CustomQueryID` (query/named.ts:150) | `CustomQueryID` (:14) | exact |
| `SyncedQuery` (query/named.ts:17) | `SyncedQuery` (:25) | exact |
| `syncedQueryWithContext` (query/named.ts:65) | `with_context` (:55) | fuzzy 0.50 |

🟥 **TS symbols not resolved into this file (6):** `HasParseFn`, `Parser`, `QueryFn`, `normalizeParser`, `syncedQueryImpl`, `withValidation`

🟦 **Rust-only added here (1):** `call`

### `query/query_delegate_base.rs`  ⟵  `query/query-delegate-base.ts`, `query/query-delegate.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `addCustomQuery` (query/query-delegate-base.ts:193) | `add_custom_query` (:59) | exact |
| `addServerQuery` (query/query-delegate-base.ts:185) | `add_server_query` (:53) | exact |
| `assertValidRunOptions` (query/query-delegate-base.ts:238) | `assert_valid_run_options` (:71) | exact |
| `batchViewUpdates` (query/query-delegate-base.ts:40) | `batch_view_updates` (:70) | exact |
| `flushQueryChanges` (query/query-delegate-base.ts:222) | `flush_query_changes` (:68) | exact |
| `materialize` (query/query-delegate-base.ts:56) | `materialize` (:74) | exact |
| `onTransactionCommit` (query/query-delegate-base.ts:230) | `on_transaction_commit` (:69) | exact |
| `preload` (query/query-delegate-base.ts:123) | `preload` (:80) | exact |
| `QueryDelegate` (query/query-delegate.ts:38) | `QueryDelegate` (:52) | exact |
| `QueryDelegateBase` (query/query-delegate-base.ts:35) | `QueryDelegateBase` (:91) | exact |
| `run` (query/query-delegate-base.ts:108) | `run` (:79) | exact |
| `updateCustomQuery` (query/query-delegate-base.ts:214) | `update_custom_query` (:67) | exact |
| `updateServerQuery` (query/query-delegate-base.ts:206) | `update_server_query` (:66) | exact |

🟥 **TS symbols not resolved into this file (5):** `CommitListener`, `GotCallback`, `materializeImpl`, `newQuery`, `preloadImpl`

🟦 **Rust-only added here (5):** `MaterializeOptions`, `PreloadOptions`, `RunOptions`, `RunResultType`, `default_query_complete`

### `query/query_impl.rs`  ⟵  _(new)_


🟦 **Rust-only added here (12):** `Cardinality`, `ExistsOptions`, `Query`, `RelationshipSpec`, `limit`, `one`, `order_by`, `related`, `where_cond`, `where_eq`, `where_exists`, `where_op`

### `query/query_internals.rs`  ⟵  `query/query-impl.ts`, `query/query-internals.ts`, `query/query-registry.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `AnyCustomQuery` (query/query-registry.ts:79) | `get_custom_query_id` (:16) | fuzzy 0.67 |
| `asQuery` (query/query-internals.ts:102) | `as_query` (:30) | exact |
| `hash` (query/query-impl.ts:173) | `hash` (:15) | exact |
| `isQueryInternals` (query/query-internals.ts:94) | `is_query_internals` (:22) | exact |
| `nameAndArgs` (query/query-impl.ts:157) | `name_and_args` (:17) | exact |
| `QueryInternals` (query/query-internals.ts:20) | `QueryInternals` (:12) | exact |

🟥 **TS symbols not resolved into this file (10):** `AnyQueryInternals`, `QueryImpl`, `asQueryImpl`, `asQueryInternals`, `isCompoundKey`, `isOneHop`, `isTwoHop`, `newQueryImpl`, `queryInternalsTag`, `throwQueryNotRunnable`

🟦 **Rust-only added here (2):** `get_ast`, `get_format`

### `query/query_registry.rs`  ⟵  `query/query-registry.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `CustomQuery` (query/query-registry.ts:43) | `CustomQuery` (:19) | exact |
| `QueryRequest` (query/query-registry.ts:108) | `QueryRequest` (:26) | exact |

🟥 **TS symbols not resolved into this file (26):** `AnyQueryDefinition`, `AnyQueryRegistry`, `AssertQueryDefinitions`, `CustomQueryTypes`, `DeepMerge`, `EnsureQueryDefinitions`, `FromQueryTree`, `QueryDefinition`, `QueryDefinitionFunction`, `QueryDefinitionTypes`, `QueryDefinitions`, `QueryExecutionFunction`, `QueryOrQueryRequest`, `QueryRegistry`, `QueryRegistryTypes`, `QueryRequestTypes`, `addContextToQuery`, `defineQueries`, `defineQueriesWithType`, `defineQuery`, `defineQueryWithType`, `getQuery`, `isQuery`, `isQueryDefinition`, `isQueryRegistry`, `mustGetQuery`

### `query/runnable_query_impl.rs`  ⟵  `query/runnable-query-impl.ts`, `query/static-query.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `newExpressionBuilder` (query/static-query.ts:20) | `new_expression_builder` (:32) | exact |
| `newRunnableQuery` (query/runnable-query-impl.ts:19) | `new_runnable_query` (:12) | exact |
| `newStaticQuery` (query/static-query.ts:6) | `new_static_query` (:21) | exact |

🟥 **TS symbols not resolved into this file (1):** `RunnableQueryImpl`

### `query/schema_query.rs`  ⟵  _(new)_


🟦 **Rust-only added here (1):** `create_builder`

### `query/ttl.rs`  ⟵  `query/ttl.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `clampTTL` (query/ttl.ts:89) | `clamp_ttl` (:42) | exact |
| `compareTTL` (query/ttl.ts:50) | `compare_ttl` (:53) | exact |
| `DEFAULT_TTL_MS` (query/ttl.ts:20) | `DEFAULT_TTL_MS` (:6) | exact |
| `MAX_TTL_MS` (query/ttl.ts:26) | `MAX_TTL_MS` (:8) | exact |
| `parseTTL` (query/ttl.ts:36) | `parse_ttl` (:12) | exact |

🟥 **TS symbols not resolved into this file (6):** `DEFAULT_PRELOAD_TTL`, `DEFAULT_PRELOAD_TTL_MS`, `MAX_TTL`, `TTL`, `TimeUnit`, `normalizeTTL`

🟦 **Rust-only added here (1):** `parse_and_clamp_agree_with_the_live_rust_cvr_impl`

### `query/typed_view.rs`  ⟵  `ivm/array-view.ts`, `query/typed-view.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `ResultType` (query/typed-view.ts:5) | `ResultType` (:10) | exact |
| `TypedView` (query/typed-view.ts:18) | `TypedView` (:22) | exact |
| `updateTTL` (ivm/array-view.ts:185) | `update_ttl` (:31) | exact |

🟥 **TS symbols not resolved into this file (1):** `Listener`

### `query/validate_input.rs`  ⟵  `query/validate-input.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `InputValidationError` (query/validate-input.ts:3) | `InputValidationError` (:10) | exact |
| `validateInput` (query/validate-input.ts:32) | `validate_input` (:30) | exact |

🟥 **TS symbols not resolved into this file (1):** `titleCase`

### `replay.rs`  ⟵  `builder/builder.ts`, `ivm/catch.ts`, `ivm/source-change-index.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `assert` (builder/builder.ts:421) | `assert_matches` (:703) | fuzzy 0.50 |
| `CaughtAddChange` (ivm/catch.ts:18) | `caught_change_to_json` (:352) | fuzzy 0.50 |
| `SourceChangeIndex` (ivm/source-change-index.ts:5) | `push_to_source_change` (:297) | fuzzy 0.50 |

🟦 **Rust-only added here (9):** `FixtureDelegate`, `canonicalize`, `caught_node_to_json`, `diff_path`, `json_deep_equal`, `parse_column_type`, `run_fixture`, `run_fixture_file`, `strip_empty_companion_rows`

### `snapshotter/diff.rs`  ⟵  _(new)_


🟦 **Rust-only added here (9):** `ChangeLogEntry`, `DiffError`, `check_valid`, `from`, `get_rows`, `iterate_diff`, `json_to_sqlite_value`, `parse_row_key`, `read_changelog`

### `snapshotter/mod.rs`  ⟵  _(new)_


🟦 **Rust-only added here (5):** `DEL_OP`, `RESET_OP`, `SET_OP`, `TRUNCATE_OP`, `ZERO_VERSION_COLUMN_NAME`

### `snapshotter/snapshotter.rs`  ⟵  `query/query-registry.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `createQuery` (query/query-registry.ts:520) | `create` (:484) | fuzzy 0.50 |

🟦 **Rust-only added here (30):** `DiffOwned`, `InvalidDiffError`, `REASON_PERMISSIONS_CHANGE`, `REASON_SCHEMA_CHANGE`, `REASON_TRUNCATION`, `SnapshotChange`, `Snapshotter`, `StalePinAction`, `StalePinTracker`, `advance_without_diff`, `begin_and_pin`, `changes`, `conn`, `curr_version`, `current_conn`, `current_version`, `destroyed`, `head_version`, `init`, `num_changes_since`, `observe`, `prev_conn`, `prev_version`, `publish_snapshot_interrupt_handles`, `repin_at_head`, `reset_to_head`, `set_snapshot_interrupt_registry`, `settle_statements`, `stale_for`, `version`

### `snapshotter/spec.rs`  ⟵  _(new)_


🟦 **Rust-only added here (6):** `ColumnSchema`, `LiteAndZqlSpec`, `TableSpec`, `cols`, `quote_ident`, `sorted_keys`

### `sqlite/database_storage.rs`  ⟵  _(new)_


🟦 **Rust-only added here (5):** `CREATE_STORAGE_TABLE`, `ClientGroupStorage`, `DatabaseStorage`, `create_database_storage`, `parse_json_value`

### `sqlite/db.rs`  ⟵  _(new)_


🟦 **Rust-only added here (11):** `Database`, `DatabaseInitError`, `Statement`, `all`, `compact`, `exec`, `in_memory`, `page_size`, `pragma_query_value_int`, `pragma_query_value_string`, `read_value_lossy`

### `sqlite/explain_queries.rs`  ⟵  _(new)_


🟦 **Rust-only added here (1):** `explain_queries`

### `sqlite/interrupt.rs`  ⟵  _(new)_


🟦 **Rust-only added here (8):** `JobWatchdog`, `WatchEntry`, `WatchGuard`, `WatchState`, `install_interrupt`, `monitor_loop`, `register`, `shutdown`

### `sqlite/options.rs`  ⟵  _(new)_


🟦 **Rust-only added here (1):** `ZQLiteZeroOptions`

### `sqlite/query_builder.rs`  ⟵  `ivm/operator.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `MultiConstraint` (ivm/operator.ts:61) | `multi_constraint_to_sql` (:159) | fuzzy 0.67 |

🟦 **Rust-only added here (22):** `SqlParam`, `SqlQuery`, `build_select_query`, `col`, `column_is_optional`, `column_left_unchanged`, `condition_to_sql`, `gather_start_constraints`, `json_start_values_are_stringified_like_typescript`, `lit`, `literal_left_binds_both_params`, `literal_left_like_and_in_balance`, `null_start_constraints_match_typescript_nullable_rules`, `nullable_aware_equality`, `nullable_aware_range_comparison`, `placeholders`, `simple_condition_to_sql`, `start_constraints_match_typescript_nullable_rules`, `to_sql`, `to_sqlite_column_value`, `to_sqlite_value`, `value_position_to_sql_param`

### `sqlite/query_delegate.rs`  ⟵  `query/query-delegate.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `NewQueryDelegate` (query/query-delegate.ts:21) | `ZqliteQueryDelegate` (:25) | fuzzy 0.50 |

### `sqlite/resolve_scalar_subqueries.rs`  ⟵  _(new)_


🟦 **Rust-only added here (10):** `CompanionSubquery`, `ResolveResult`, `TableSpecWithUniqueKeys`, `collect_constraints`, `extract_literal_equality_constraints`, `is_simple_subquery`, `resolve_ast_recursive`, `resolve_condition`, `resolve_scalar_subquery`, `resolve_simple_scalar_subqueries`

### `sqlite/sqlite_cost_model.rs`  ⟵  _(new)_


🟦 **Rust-only added here (21):** `AVAILABLE`, `CostProbeInterrupted`, `INTERRUPT_ERR_PREFIX`, `SQLITE_SCANSTAT_COMPLEX`, `SQLITE_SCANSTAT_EST`, `SQLITE_SCANSTAT_EXPLAIN`, `SQLITE_SCANSTAT_PARENTID`, `SQLITE_SCANSTAT_SELECTID`, `ScanstatusLoop`, `btree_cost`, `build_probe_sql`, `create_sqlite_cost_model`, `create_sqlite_cost_model_prepared`, `get_scanstatus_loops`, `inline_param`, `inline_sql`, `is_interrupt_error`, `prepare_table_specs`, `remove_correlated_subqueries`, `scanstatus_available`, `sqlite3_stmt_scanstatus_v2`

### `sqlite/sqlite_stat_fanout.rs`  ⟵  `planner/planner-node.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `omitFanout` (planner/planner-node.ts:61) | `get_fanout` (:115) | fuzzy 0.50 |

🟦 **Rust-only added here (14):** `DEFAULT_FANOUT`, `DecodedSample`, `FanoutResult`, `FanoutSource`, `IndexInfo`, `SQLiteStatFanout`, `clear_cache`, `decode_sample_is_null`, `fanout_from_stat1`, `fanout_from_stat4`, `find_index_for_columns`, `is_prefix_match`, `parse_int_js`, `with_default_fanout`

### `sqlite/table_source.rs`  ⟵  `ivm/memory-source.ts`, `ivm/snitch.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `FetchCountMessage` (ivm/snitch.ts:189) | `fetch_count` (:1398) | fuzzy 0.67 |
| `writeChange` (ivm/memory-source.ts:615) | `write_change` (:736) | exact |

🟥 **TS symbols not resolved into this file (1):** `Overlays`

🟦 **Rust-only added here (38):** `LazyRows`, `LazyRowsIter`, `NullInputBase`, `RowErr`, `_write_change_unused`, `applied_change_obeys_ts_sql_null_start_semantics`, `applied_changes_for_request`, `boolean_matches_ts_double_bang`, `check_exists`, `check_exists_failure_propagates_not_false`, `classify_row_error`, `conn_with_rows`, `conn_with_value`, `conv`, `existing_input_uses_replacement_snapshot_connection`, `fetch_reads_all_columns_and_values`, `fetch_resumes_all_rows_after_guard_drops`, `fetch_returns_all_rows_when_gate_under_floor`, `fetch_returns_all_rows_when_no_gate_armed`, `fetch_stops_when_gate_over_budget`, `integer_over_2_53_panics_like_ts`, `invalid_json_panics_like_ts`, `json_sqlite_text_to_ivm`, `number_string_passthrough`, `past_gate`, `push_body`, `read_error_panics_not_swallowed_to_null`, `set_db`, `sql_start_matches`, `sqlite_value_to_ivm`, `stream_query`, `stream_query_bind_failure_propagates_not_empty`, `stream_query_busy_propagates_not_empty`, `stream_query_prepare_failure_propagates_not_empty`, `table_source_get_row_reads_current_snapshot_with_types`, `try_new`, `valid_json_tagged`, `validate_change`

### `streamer/mod.rs`  ⟵  `ivm/stream.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `Stream` (ivm/stream.ts:8) | `stream` (:92) | exact |

🟦 **Rust-only added here (26):** `Chunker`, `CollectSink`, `NullSink`, `RowChange`, `SUBQ_JUNCTION_PREFIX`, `SUBQ_PREFIX`, `StreamFrame`, `StreamSink`, `Streamer`, `TableSpecInfo`, `absent_pk_column_does_not_yield_missing_key`, `accumulate`, `bump_row_version`, `done`, `empty_pk_list_does_not_yield_empty_key`, `error`, `flush_query`, `get_row_key`, `into_sink`, `is_exists_condition_rel`, `null_pk_value_does_not_yield_null_key`, `present_pk_yields_non_empty_key_with_column`, `push_row_change`, `send`, `stream_changes`, `stream_nodes`

## 3 · Flat one-to-one symbol map (every TS symbol resolved)

| TS symbol | origin | → Rust | status |
|---|---|---|---|
| `StaticQueryParameters` | builder/builder.ts:46 | — | 🟥 UNRESOLVED |
| `BuilderDelegate` | builder/builder.ts:55 | `BuilderDelegate` builder/builder.rs:39 | ✅ exact |
| `getSource` | builder/builder.ts:77 | `get_source` builder/builder.rs:41 | ✅ exact |
| `createStorage` | builder/builder.ts:83 | `create_storage` builder/builder.rs:50 | ✅ exact |
| `decorateInput` | builder/builder.ts:85 | N/A | 📌 debug-instrumentation decorator; not ported |
| `addEdge` | builder/builder.ts:87 | N/A | 📌 debug-instrumentation decorator; Rust wires Rc directly |
| `decorateFilterInput` | builder/builder.ts:89 | N/A | 📌 debug-instrumentation decorator; not ported |
| `decorateSourceInput` | builder/builder.ts:91 | `TABLE_SOURCE_INPUT` live_count.rs:12 | 🔁 rename 0.50 |
| `buildPipeline` | builder/builder.ts:126 | `build_pipeline` builder/builder.rs:59 | ✅ exact |
| `bindStaticParameters` | builder/builder.ts:146 | rust-syncer permissions.rs | 📌 relocated upstream (AST transform) |
| `resolveField` | builder/builder.ts:204 | rust-syncer permissions.rs resolve_field | 📌 relocated |
| `isParameter` | builder/builder.ts:220 | permissions.rs bind_value | 📌 inlined |
| `assertNoNotExists` | builder/builder.ts:232 | `assert_no_not_exists` builder/builder.rs:652 | ✅ exact |
| `buildPipelineInternal` | builder/builder.ts:256 | `build_pipeline_internal` builder/builder.rs:65 | ✅ exact |
| `applyWhere` | builder/builder.ts:399 | `apply_where` builder/builder.rs:242 | ✅ exact |
| `applyFilterWithFlips` | builder/builder.ts:414 | `apply_filter_with_flips` builder/builder.rs:395 | ✅ exact |
| `assert` | builder/builder.ts:421 | `assert_matches` replay.rs:703 | 🔁 rename 0.50 |
| `applyFilter` | builder/builder.ts:523 | `apply_filter` builder/builder.rs:257 | ✅ exact |
| `applyAnd` | builder/builder.ts:541 | `apply_and` builder/builder.rs:274 | ✅ exact |
| `applyOr` | builder/builder.ts:553 | `apply_or` builder/builder.rs:289 | ✅ exact |
| `groupSubqueryConditions` | builder/builder.ts:598 | `group_subquery_conditions` builder/builder.rs:324 | ✅ exact |
| `isNotAndDoesNotContainSubquery` | builder/builder.ts:613 | `is_not_and_does_not_contain_subquery` builder/builder.rs:338 | ✅ exact |
| `applySimpleCondition` | builder/builder.ts:625 | `apply_simple_condition` builder/builder.rs:349 | ✅ exact |
| `valuePosName` | builder/builder.ts:639 | builder.rs | 📌 inlined |
| `applyCorrelatedSubQuery` | builder/builder.ts:650 | `apply_correlated_subquery` builder/builder.rs:502 | ✅ exact |
| `applyCorrelatedSubqueryCondition` | builder/builder.ts:689 | `apply_correlated_subquery_condition` builder/builder.rs:362 | ✅ exact |
| `gatherCorrelatedSubqueryQueryConditions` | builder/builder.ts:720 | `gather_correlated_subquery_query_conditions` builder/builder.rs:593 | ✅ exact |
| `assertOrderingIncludesPK` | builder/builder.ts:742 | `assert_ordering_includes_pk` query/complete_ordering.rs:39 | ✅ exact |
| `uniquifyCorrelatedSubqueryConditionAliases` | builder/builder.ts:763 | `uniquify_correlated_subquery_condition_aliases` builder/builder.rs:678 | ✅ exact |
| `conditionIncludesFlippedSubqueryAtAnyLevel` | builder/builder.ts:807 | `condition_includes_flipped_subquery_at_any_level` builder/builder.rs:619 | ✅ exact |
| `partitionBranches` | builder/builder.ts:822 | `partition_branches` builder/builder.rs:631 | ✅ exact |
| `NonNullValue` | builder/filter.ts:12 | — | 🟥 UNRESOLVED |
| `SimplePredicate` | builder/filter.ts:13 | — | 🟥 UNRESOLVED |
| `SimplePredicateNoNull` | builder/filter.ts:14 | — | 🟥 UNRESOLVED |
| `NoSubqueryCondition` | builder/filter.ts:16 | `CorrelatedSubqueryCondition` builder/ast.rs:56 | 🔁 rename 0.50 |
| `createPredicate` | builder/filter.ts:27 | `create_predicate` builder/filter.rs:17 | ✅ exact |
| `createIsPredicate` | builder/filter.ts:97 | `create_simple_predicate` builder/filter.rs:39 | 🔁 rename 0.67 |
| `createPredicateImpl` | builder/filter.ts:109 | `create_predicate_impl` builder/filter.rs:93 | ✅ exact |
| `not` | builder/filter.ts:158 | `not` query/expression.rs:51 | ✅ exact |
| `transformFilters` | builder/filter.ts:171 | `transform_filters` builder/filter.rs:181 | ✅ exact |
| `cases` | builder/like-test-cases.ts:1 | — | 🟥 UNRESOLVED |
| `getLikePredicate` | builder/like.ts:4 | `get_like_predicate` builder/like.rs:15 | ✅ exact |
| `getLikeOp` | builder/like.ts:17 | `get_like_op` builder/like.rs:31 | ✅ exact |
| `patternToRegExp` | builder/like.ts:37 | builder/like.rs get_like_predicate | 📌 predicate closure, not regex |
| `changeToViewChange` | ivm/array-view.ts:15 | `change_to_view_change` ivm/view.rs:996 | ✅ exact |
| `ArrayView` | ivm/array-view.ts:50 | `ArrayView` ivm/array_view.rs:21 | ✅ exact |
| `data` | ivm/array-view.ts:111 | `data` ivm/array_view.rs:74 | ✅ exact |
| `addListener` | ivm/array-view.ts:115 | `add_listener` ivm/array_view.rs:79 | ✅ exact |
| `destroy` | ivm/array-view.ts:136 | `destroy` engine/mod.rs:1574 | ✅ exact |
| `push` | ivm/array-view.ts:159 | `push` engine/mod.rs:294 | ✅ exact |
| `flush` | ivm/array-view.ts:173 | `flush` ivm/array_view.rs:88 | ✅ exact |
| `updateTTL` | ivm/array-view.ts:185 | `update_ttl` query/typed_view.rs:31 | ✅ exact |
| `get` | ivm/cap.ts:31 | `get` ivm/cap.rs:44 | ✅ exact |
| `set` | ivm/cap.ts:32 | `set` ivm/cap.rs:48 | ✅ exact |
| `del` | ivm/cap.ts:33 | `del` ivm/cap.rs:52 | ✅ exact |
| `Cap` | ivm/cap.ts:52 | `Cap` ivm/cap.rs:88 | ✅ exact |
| `setOutput` | ivm/cap.ts:79 | `set_output` ivm/cap.rs:265 | ✅ exact |
| `getSchema` | ivm/cap.ts:83 | `get_schema` engine/mod.rs:1838 | ✅ exact |
| `getCapStateKey` | ivm/cap.ts:300 | `CapState` ivm/cap.rs:26 | 🔁 rename 0.67 |
| `serializePK` | ivm/cap.ts:315 | `serialize_pk` ivm/cap.rs:155 | ✅ exact |
| `deserializePKToConstraint` | ivm/cap.ts:319 | — | 🟥 UNRESOLVED |
| `CaughtNode` | ivm/catch.ts:11 | `CaughtNode` ivm/catch.rs:15 | ✅ exact |
| `CaughtAddChange` | ivm/catch.ts:18 | `caught_change_to_json` replay.rs:352 | 🔁 rename 0.50 |
| `CaughtRemoveChange` | ivm/catch.ts:23 | — | 🟥 UNRESOLVED |
| `CaughtChildChange` | ivm/catch.ts:28 | `push_child_change` ivm/flipped_join.rs:301 | 🔁 rename 0.50 |
| `CaughtEditChange` | ivm/catch.ts:37 | — | 🟥 UNRESOLVED |
| `CaughtChange` | ivm/catch.ts:43 | `CaughtChange` ivm/catch.rs:22 | ✅ exact |
| `Catch` | ivm/catch.ts:53 | `Catch` ivm/catch.rs:41 | ✅ exact |
| `fetch` | ivm/catch.ts:65 | `fetch` ivm/cap.rs:269 | ✅ exact |
| `reset` | ivm/catch.ts:84 | `reset` engine/mod.rs:1545 | ✅ exact |
| `expandChange` | ivm/catch.ts:93 | `expand_change` ivm/catch.rs:119 | ✅ exact |
| `expandNode` | ivm/catch.ts:125 | `expand_node` ivm/catch.rs:142 | ✅ exact |
| `TYPE` | ivm/change-index-enum.ts:1 | — | 🟥 UNRESOLVED |
| `NODE` | ivm/change-index-enum.ts:2 | `node` ivm/change.rs:52 | ✅ exact |
| `OLD_NODE` | ivm/change-index-enum.ts:3 | `old_node` ivm/change.rs:72 | ✅ exact |
| `CHILD_DATA` | ivm/change-index-enum.ts:4 | `ChildData` ivm/change.rs:21 | ✅ exact |
| `ChangeIndex` | ivm/change-index.ts:5 | — | 🟥 UNRESOLVED |
| `ADD` | ivm/change-type-enum.ts:1 | — | 🟥 UNRESOLVED |
| `REMOVE` | ivm/change-type-enum.ts:2 | `remove_query` engine/mod.rs:679 | 🔁 rename 0.50 |
| `EDIT` | ivm/change-type-enum.ts:3 | `StableEdit` ivm/source.rs:1287 | 🔁 rename 0.50 |
| `CHILD` | ivm/change-type-enum.ts:4 | `apply_child` ivm/view.rs:571 | 🔁 rename 0.50 |
| `ChangeType` | ivm/change-type.ts:5 | `ChangeType` ivm/change.rs:12 | ✅ exact |
| `Change` | ivm/change.ts:12 | `Change` ivm/change.rs:33 | ✅ exact |
| `AddChange` | ivm/change.ts:17 | `push_add_change` ivm/take.rs:469 | 🔁 rename 0.67 |
| `RemoveChange` | ivm/change.ts:22 | `push_remove_change` ivm/take.rs:581 | 🔁 rename 0.67 |
| `ChildChange` | ivm/change.ts:29 | `ChildViewChange` ivm/view.rs:175 | 🔁 rename 0.67 |
| `EditChange` | ivm/change.ts:57 | `push_edit_change` ivm/take.rs:686 | 🔁 rename 0.67 |
| `makeAddChange` | ivm/change.ts:61 | `make_add_change` ivm/change.rs:81 | ✅ exact |
| `makeRemoveChange` | ivm/change.ts:65 | `make_remove_change` ivm/change.rs:85 | ✅ exact |
| `makeChildChange` | ivm/change.ts:69 | `make_child_change` ivm/change.rs:89 | ✅ exact |
| `makeEditChange` | ivm/change.ts:73 | `make_edit_change` ivm/change.rs:93 | ✅ exact |
| `Constraint` | ivm/constraint.ts:13 | — | 🟥 UNRESOLVED |
| `constraintMatchesRow` | ivm/constraint.ts:17 | `constraint_matches_row` ivm/constraint.rs:15 | ✅ exact |
| `constraintsAreCompatible` | ivm/constraint.ts:34 | `constraints_are_compatible` ivm/constraint.rs:27 | ✅ exact |
| `constraintMatchesPrimaryKey` | ivm/constraint.ts:46 | `constraint_matches_primary_key` ivm/constraint.rs:39 | ✅ exact |
| `keyMatchesPrimaryKey` | ivm/constraint.ts:53 | `key_matches_primary_key` ivm/constraint.rs:76 | ✅ exact |
| `pullSimpleAndComponents` | ivm/constraint.ts:91 | `pull_simple_and_components` ivm/constraint.rs:96 | ✅ exact |
| `primaryKeyConstraintFromFilters` | ivm/constraint.ts:114 | `primary_key_constraint_from_filters` ivm/constraint.rs:125 | ✅ exact |
| `extractColumn` | ivm/constraint.ts:147 | `extract_column` ivm/constraint.rs:112 | ✅ exact |
| `SetOfConstraint` | ivm/constraint.ts:162 | — | 🟥 UNRESOLVED |
| `has` | ivm/constraint.ts:173 | `has` ivm/source.rs:261 | ✅ exact |
| `constraintEquals` | ivm/constraint.ts:185 | `constraint_equals` ivm/constraint.rs:154 | ✅ exact |
| `compareValues` | ivm/data.ts:32 | `compare_values` ivm/data.rs:131 | ✅ exact |
| `NormalizedValue` | ivm/data.ts:78 | — | 🟥 UNRESOLVED |
| `normalizeUndefined` | ivm/data.ts:85 | ivm/data.rs | 📌 inlined (undefined->null) |
| `Comparator` | ivm/data.ts:89 | — | 🟥 UNRESOLVED |
| `makeComparator` | ivm/data.ts:91 | `make_comparator` ivm/data.rs:292 | ✅ exact |
| `valuesEqual` | ivm/data.ts:112 | `values_equal` ivm/data.rs:199 | ✅ exact |
| `drainStreams` | ivm/data.ts:120 | `drain_streams` ivm/data.rs:381 | ✅ exact |
| `Exists` | ivm/exists.ts:21 | `Exists` ivm/exists.rs:36 | ✅ exact |
| `setFilterOutput` | ivm/exists.ts:67 | `set_filter_output` ivm/exists.rs:153 | ✅ exact |
| `beginFilter` | ivm/exists.ts:71 | `begin_filter` ivm/exists.rs:159 | ✅ exact |
| `endFilter` | ivm/exists.ts:75 | `end_filter` ivm/exists.rs:166 | ✅ exact |
| `FanIn` | ivm/fan-in.ts:30 | `FanIn` ivm/fan_in.rs:21 | ✅ exact |
| `FanOut` | ivm/fan-out.ts:17 | `FanOut` ivm/fan_out.rs:22 | ✅ exact |
| `setFanIn` | ivm/fan-out.ts:28 | `set_fan_in` ivm/fan_out.rs:48 | ✅ exact |
| `FilterInput` | ivm/filter-operators.ts:27 | `FilterInput` ivm/filter_operators.rs:28 | ✅ exact |
| `FilterOutput` | ivm/filter-operators.ts:32 | `FilterOutput` ivm/filter_operators.rs:39 | ✅ exact |
| `filter` | ivm/filter-operators.ts:37 | `filter` ivm/exists.rs:176 | ✅ exact |
| `FilterOperator` | ivm/filter-operators.ts:41 | — | 🟥 UNRESOLVED |
| `throwFilterOutput` | ivm/filter-operators.ts:48 | `FilterOutputAsOutput` ivm/filter_operators.rs:287 | 🔁 rename 0.67 |
| `FilterStart` | ivm/filter-operators.ts:61 | `FilterStart` ivm/filter_operators.rs:52 | ✅ exact |
| `FilterEnd` | ivm/filter-operators.ts:106 | `FilterEnd` ivm/filter_operators.rs:195 | ✅ exact |
| `buildFilterPipeline` | ivm/filter-operators.ts:148 | `build_filter_pipeline` ivm/filter_operators.rs:275 | ✅ exact |
| `getMultiConstraintChunkSize` | ivm/flipped-join.ts:60 | `get_multi_constraint_chunk_size` ivm/flipped_join.rs:40 | ✅ exact |
| `setMultiConstraintChunkSizeForTest` | ivm/flipped-join.ts:65 | `set_multi_constraint_chunk_size_for_test` ivm/flipped_join.rs:44 | ✅ exact |
| `FlippedJoin` | ivm/flipped-join.ts:93 | `FlippedJoin` ivm/flipped_join.rs:62 | ✅ exact |
| `canonicalKeyForTest` | ivm/flipped-join.ts:572 | `canonical_key_row` ivm/flipped_join.rs:573 | 🔁 rename 0.40 |
| `canonicalKey` | ivm/flipped-join.ts:585 | `canonical_key` ivm/flipped_join.rs:578 | ✅ exact |
| `canonicalValue` | ivm/flipped-join.ts:600 | `canonical_value` ivm/flipped_join.rs:593 | ✅ exact |
| `generateWithOverlayNoYield` | ivm/join-utils.ts:11 | `generate_with_overlay_no_yield` ivm/join_utils.rs:368 | ✅ exact |
| `generateWithOverlayNoYieldUnordered` | ivm/join-utils.ts:126 | `generate_with_overlay_no_yield_unordered` ivm/join_utils.rs:378 | ✅ exact |
| `rowEqualsForCompoundKey` | ivm/join-utils.ts:206 | `row_equals_for_compound_key` ivm/join.rs:482 | ✅ exact |
| `isJoinMatch` | ivm/join-utils.ts:219 | `is_join_match` ivm/join.rs:466 | ✅ exact |
| `buildJoinConstraint` | ivm/join-utils.ts:238 | `build_join_constraint` ivm/join.rs:450 | ✅ exact |
| `Join` | ivm/join.ts:51 | `Join` ivm/join.rs:33 | ✅ exact |
| `Overlay` | ivm/memory-source.ts:59 | `OverlayGuard` ivm/source.rs:117 | 🔁 rename 0.50 |
| `Overlays` | ivm/memory-source.ts:64 | — | 🟥 UNRESOLVED |
| `Connection` | ivm/memory-source.ts:75 | `Connection` ivm/source.rs:104 | ✅ exact |
| `MemorySource` | ivm/memory-source.ts:98 | `MemorySource` ivm/source.rs:128 | ✅ exact |
| `tableSchema` | ivm/memory-source.ts:127 | sqlite/table_source.rs | 📌 -> SQLite |
| `fork` | ivm/memory-source.ts:135 | N/A | 📌 TS memory-source fork; Rust source is SQLite-backed |
| `connect` | ivm/memory-source.ts:162 | `connect` ivm/source.rs:53 | ✅ exact |
| `getIndexKeys` | ivm/memory-source.ts:253 | sqlite/table_source.rs | 📌 -> SQLite index |
| `writeChange` | ivm/memory-source.ts:615 | `write_change` sqlite/table_source.rs:736 | ✅ exact |
| `setOverlay` | ivm/memory-source.ts:673 | sqlite/table_source.rs | 📌 -> SQLite |
| `computeOverlays` | ivm/memory-source.ts:745 | sqlite/table_source.rs | 📌 -> SQLite (overlays via SQLite tx) |
| `applyMultiConstraintsToOverlays` | ivm/memory-source.ts:795 | `apply_source_overlays` ivm/source.rs:1045 | 🔁 rename 0.40 |
| `overlaysForMultiConstraint` | ivm/memory-source.ts:810 | sqlite/table_source.rs | 📌 -> SQLite |
| `overlaysForStartAt` | ivm/memory-source.ts:829 | sqlite/table_source.rs | 📌 -> SQLite |
| `overlaysForConstraint` | ivm/memory-source.ts:844 | sqlite/table_source.rs | 📌 -> SQLite |
| `overlaysForFilterPredicate` | ivm/memory-source.ts:859 | sqlite/table_source.rs | 📌 -> SQLite |
| `rowMatchesPK` | ivm/memory-source.ts:976 | `row_matches_multi_constraints` ivm/constraint.rs:59 | 🔁 rename 0.40 |
| `makeBoundComparator` | ivm/memory-source.ts:997 | `make_partial_bound_comparator` ivm/data.rs:318 | 🔁 rename 0.75 |
| `compareBounds` | ivm/memory-source.ts:1023 | `compare` ivm/view.rs:77 | 🔁 rename 0.50 |
| `stringify` | ivm/memory-source.ts:1050 | N/A | 📌 TS memory-source key stringify; Rust uses SQLite keys |
| `MemoryStorage` | ivm/memory-storage.ts:17 | `MemoryStorage` ivm/memory_storage.rs:13 | ✅ exact |
| `cloneData` | ivm/memory-storage.ts:47 | ivm/memory_storage.rs | 📌 inlined clone |
| `InputBase` | ivm/operator.ts:14 | `InputBase` ivm/operator.rs:44 | ✅ exact |
| `Input` | ivm/operator.ts:26 | `Input` ivm/operator.rs:49 | ✅ exact |
| `MultiConstraint` | ivm/operator.ts:61 | `multi_constraint_to_sql` sqlite/query_builder.rs:159 | 🔁 rename 0.67 |
| `FetchRequest` | ivm/operator.ts:63 | `FetchRequest` ivm/operator.rs:24 | ✅ exact |
| `Start` | ivm/operator.ts:84 | `Start` ivm/operator.rs:33 | ✅ exact |
| `Output` | ivm/operator.ts:93 | `Output` ivm/operator.rs:54 | ✅ exact |
| `throwOutput` | ivm/operator.ts:114 | `ThrowOutput` ivm/operator.rs:68 | ✅ exact |
| `Operator` | ivm/operator.ts:126 | — | 🟥 UNRESOLVED |
| `Storage` | ivm/operator.ts:132 | `Storage` ivm/operator.rs:58 | ✅ exact |
| `scan` | ivm/operator.ts:138 | `scan` ivm/memory_storage.rs:44 | ✅ exact |
| `mergeRelationships` | ivm/push-accumulated.ts:265 | `merge_relationships` ivm/push_accumulated.rs:23 | ✅ exact |
| `unreachable` | ivm/push-accumulated.ts:366 | Rust unreachable!() macro | 📌 idiom |
| `makeAddEmptyRelationships` | ivm/push-accumulated.ts:369 | `add_empty_relationships` ivm/push_accumulated.rs:132 | 🔁 rename 0.75 |
| `mergeEmpty` | ivm/push-accumulated.ts:421 | ivm/push_accumulated logic | 📌 inlined |
| `SourceSchema` | ivm/schema.ts:9 | `SourceSchema` ivm/schema.rs:24 | ✅ exact |
| `next` | ivm/skip-yields.ts:25 | `next` ivm/filter_operators.rs:162 | ✅ exact |
| `skipYields` | ivm/skip-yields.ts:44 | `skip_yields` ivm/stream.rs:56 | ✅ exact |
| `Bound` | ivm/skip.ts:24 | `Bound` builder/ast.rs:29 | ✅ exact |
| `Skip` | ivm/skip.ts:33 | `Skip` ivm/skip.rs:18 | ✅ exact |
| `Snitch` | ivm/snitch.ts:25 | `Snitch` ivm/snitch.rs:50 | ✅ exact |
| `toChangeRecord` | ivm/snitch.ts:94 | `to_change_record` ivm/snitch.rs:189 | ✅ exact |
| `FilterSnitch` | ivm/snitch.ts:121 | — | 🟥 UNRESOLVED |
| `SnitchMessage` | ivm/snitch.ts:183 | `SnitchMessage` ivm/snitch.rs:33 | ✅ exact |
| `FetchCountMessage` | ivm/snitch.ts:189 | `fetch_count` sqlite/table_source.rs:1398 | 🔁 rename 0.67 |
| `FetchMessage` | ivm/snitch.ts:190 | — | 🟥 UNRESOLVED |
| `PushMessage` | ivm/snitch.ts:191 | — | 🟥 UNRESOLVED |
| `FilterMessage` | ivm/snitch.ts:192 | — | 🟥 UNRESOLVED |
| `ChangeRecord` | ivm/snitch.ts:194 | `ChangeRecord` ivm/snitch.rs:24 | ✅ exact |
| `AddChangeRecord` | ivm/snitch.ts:200 | — | 🟥 UNRESOLVED |
| `RemoveChangeRecord` | ivm/snitch.ts:207 | — | 🟥 UNRESOLVED |
| `ChildChangeRecord` | ivm/snitch.ts:212 | — | 🟥 UNRESOLVED |
| `EditChangeRecord` | ivm/snitch.ts:218 | — | 🟥 UNRESOLVED |
| `LogType` | ivm/snitch.ts:224 | `LogType` ivm/snitch.rs:16 | ✅ exact |
| `ROW` | ivm/source-change-index-enum.ts:2 | `row` ivm/data.rs:274 | ✅ exact |
| `OLD_ROW` | ivm/source-change-index-enum.ts:3 | — | 🟥 UNRESOLVED |
| `SourceChangeIndex` | ivm/source-change-index.ts:5 | `push_to_source_change` replay.rs:297 | 🔁 rename 0.50 |
| `SourceChangeAdd` | ivm/source.ts:9 | `source_change_to_change` ivm/source.rs:539 | 🔁 rename 0.67 |
| `SourceChangeRemove` | ivm/source.ts:10 | — | 🟥 UNRESOLVED |
| `SourceChangeEdit` | ivm/source.ts:15 | `push_source_change` engine/mod.rs:1661 | 🔁 rename 0.50 |
| `SourceChange` | ivm/source.ts:17 | `SourceChange` ivm/change.rs:102 | ✅ exact |
| `makeSourceChangeAdd` | ivm/source.ts:22 | `make_source_change_add` ivm/change.rs:126 | ✅ exact |
| `makeSourceChangeRemove` | ivm/source.ts:26 | `make_source_change_remove` ivm/change.rs:130 | ✅ exact |
| `makeSourceChangeEdit` | ivm/source.ts:30 | `make_source_change_edit` ivm/change.rs:134 | ✅ exact |
| `Source` | ivm/source.ts:54 | `Source` ivm/source.rs:38 | ✅ exact |
| `genPush` | ivm/source.ts:96 | `gen_push` ivm/source.rs:65 | ✅ exact |
| `SourceInput` | ivm/source.ts:99 | `SourceInput` ivm/source.rs:628 | ✅ exact |
| `StoppableIterator` | ivm/stopable-iterator.ts:5 | `StoppableIterator` ivm/stopable_iterator.rs:10 | ✅ exact |
| `stop` | ivm/stopable-iterator.ts:20 | `stop` ivm/stopable_iterator.rs:23 | ✅ exact |
| `Stream` | ivm/stream.ts:8 | `stream` streamer/mod.rs:92 | ✅ exact |
| `first` | ivm/stream.ts:23 | `first` ivm/stream.rs:102 | ✅ exact |
| `consume` | ivm/stream.ts:30 | streamer/mod.rs | 📌 -> Rust Iterator consume |
| `drainGenerator` | ivm/stream.ts:35 | N/A | 📌 TS generator drain -> Rust Iterator drop/for_each |
| `PartitionKey` | ivm/take.ts:42 | `optional_constraint_matches_partition_key` ivm/take.rs:1022 | 🔁 rename 0.40 |
| `Take` | ivm/take.ts:55 | `take` ivm/stream.rs:65 | ✅ exact |
| `getTakeStateKey` | ivm/take.ts:710 | `get_take_state_key` ivm/cap.rs:129 | ✅ exact |
| `constraintMatchesPartitionKey` | ivm/take.ts:727 | `constraint_matches_partition_key` ivm/take.rs:1069 | ✅ exact |
| `makePartitionKeyComparator` | ivm/take.ts:745 | `make_partition_key_comparator` ivm/take.rs:1051 | ✅ exact |
| `UnionFanIn` | ivm/union-fan-in.ts:25 | `UnionFanIn` ivm/union_fan_in.rs:30 | ✅ exact |
| `fanOutStartedPushing` | ivm/union-fan-in.ts:185 | `fan_out_started_pushing` ivm/union_fan_in.rs:109 | ✅ exact |
| `UnionFanOut` | ivm/union-fan-out.ts:11 | `UnionFanOut` ivm/union_fan_out.rs:15 | ✅ exact |
| `refCountSymbol` | ivm/view-apply-change.ts:16 | — | 🟥 UNRESOLVED |
| `idSymbol` | ivm/view-apply-change.ts:17 | — | 🟥 UNRESOLVED |
| `ExpandedNode` | ivm/view-apply-change.ts:45 | `ExpandedNode` ivm/view.rs:91 | ✅ exact |
| `ViewNode` | ivm/view-apply-change.ts:55 | `ViewNode` ivm/view.rs:99 | ✅ exact |
| `ViewChange` | ivm/view-apply-change.ts:62 | `ViewChange` ivm/view.rs:149 | ✅ exact |
| `RowOnlyNode` | ivm/view-apply-change.ts:68 | `RowOnlyNode` ivm/view.rs:169 | ✅ exact |
| `AddViewChange` | ivm/view-apply-change.ts:70 | — | 🟥 UNRESOLVED |
| `RemoveViewChange` | ivm/view-apply-change.ts:75 | — | 🟥 UNRESOLVED |
| `RefCountMap` | ivm/view-apply-change.ts:99 | — | 🟥 UNRESOLVED |
| `delete` | ivm/view-apply-change.ts:102 | array_view.rs Vec::remove | 📌 inlined |
| `owns` | ivm/view-apply-change.ts:156 | N/A | 📌 JS WeakSet COW -> Rust Rc::make_mut |
| `track` | ivm/view-apply-change.ts:161 | N/A | 📌 JS WeakSet COW -> Rust Rc::make_mut |
| `applyChange` | ivm/view-apply-change.ts:185 | `apply_change` ivm/source.rs:483 | ✅ exact |
| `applyChangeInternal` | ivm/view-apply-change.ts:213 | `apply_change_internal` ivm/view.rs:237 | ✅ exact |
| `applyChanges` | ivm/view-apply-change.ts:555 | `apply_changes` ivm/view.rs:212 | ✅ exact |
| `applyEdit` | ivm/view-apply-change.ts:579 | `apply_edit` ivm/view.rs:741 | ✅ exact |
| `initializeRelationshipsForNewEntryIfAny` | ivm/view-apply-change.ts:625 | `initialize_relationships_for_new_entry_if_any` ivm/view.rs:764 | ✅ exact |
| `insertAt` | ivm/view-apply-change.ts:719 | array_view.rs Vec::insert | 📌 inlined |
| `removeAt` | ivm/view-apply-change.ts:732 | array_view.rs Vec::remove | 📌 inlined |
| `removeAndUpdateRefCount` | ivm/view-apply-change.ts:744 | `remove_and_update_ref_count` ivm/view.rs:544 | ✅ exact |
| `binarySearch` | ivm/view-apply-change.ts:767 | `binary_search` ivm/view.rs:876 | ✅ exact |
| `assertMetaEntry` | ivm/view-apply-change.ts:790 | N/A | 📌 TS type-guard; Rust Entry struct |
| `assertNumber` | ivm/view-apply-change.ts:793 | N/A | 📌 TS type-guard; Rust ref_count:usize |
| `getSingularEntry` | ivm/view-apply-change.ts:797 | `get_singular_entry` ivm/view.rs:900 | ✅ exact |
| `getOptionalSingularEntry` | ivm/view-apply-change.ts:808 | `get_optional_singular_entry` ivm/view.rs:908 | ✅ exact |
| `getChildEntryList` | ivm/view-apply-change.ts:821 | `get_child_entry_list` ivm/view.rs:919 | ✅ exact |
| `assertArray` | ivm/view-apply-change.ts:826 | N/A | 📌 TS type-guard; Rust View enum |
| `makeNewMetaEntry` | ivm/view-apply-change.ts:831 | `make_new_meta_entry` ivm/view.rs:841 | ✅ exact |
| `makeID` | ivm/view-apply-change.ts:851 | `make_id` ivm/view.rs:850 | ✅ exact |
| `incRefCount` | ivm/view-apply-change.ts:859 | `inc_ref_count` ivm/view.rs:927 | ✅ exact |
| `decRefCount` | ivm/view-apply-change.ts:866 | `dec_ref_count` ivm/view.rs:934 | ✅ exact |
| `setRefCount` | ivm/view-apply-change.ts:873 | array_view.rs inc/dec_ref_count | 📌 inlined |
| `arrayWith` | ivm/view-apply-change.ts:888 | array_view.rs new_view[pos]=… | 📌 inlined |
| `setProperty` | ivm/view-apply-change.ts:901 | array_view.rs field assign | 📌 inlined |
| `View` | ivm/view.ts:9 | `View` ivm/view.rs:45 | ✅ exact |
| `EntryList` | ivm/view.ts:10 | — | 🟥 UNRESOLVED |
| `Entry` | ivm/view.ts:11 | `Entry` ivm/view.rs:54 | ✅ exact |
| `ViewFactory` | ivm/view.ts:15 | — | 🟥 UNRESOLVED |
| `AnyViewFactory` | ivm/view.ts:31 | — | 🟥 UNRESOLVED |
| `wireOutput` | planner/planner-builder.ts:22 | `wire_output` planner/planner_builder.rs:34 | ✅ exact |
| `Plans` | planner/planner-builder.ts:37 | `Plans` planner/planner_builder.rs:17 | ✅ exact |
| `buildPlanGraph` | planner/planner-builder.ts:42 | `build_plan_graph` planner/planner_builder.rs:200 | ✅ exact |
| `processCondition` | planner/planner-builder.ts:100 | `process_condition` planner/planner_builder.rs:52 | ✅ exact |
| `processAnd` | planner/planner-builder.ts:127 | planner/planner_builder.rs process_condition | 📌 inlined |
| `processOr` | planner/planner-builder.ts:149 | planner/planner_builder.rs process_condition | 📌 inlined |
| `processCorrelatedSubquery` | planner/planner-builder.ts:192 | `process_correlated_subquery` planner/planner_builder.rs:120 | ✅ exact |
| `hasCorrelatedSubquery` | planner/planner-builder.ts:282 | `has_correlated_subquery` planner/planner_builder.rs:26 | ✅ exact |
| `extractConstraint` | planner/planner-builder.ts:293 | `extract_constraint` planner/planner_builder.rs:22 | ✅ exact |
| `planRecursively` | planner/planner-builder.ts:300 | `plan_recursively` planner/planner_builder.rs:262 | ✅ exact |
| `planQuery` | planner/planner-builder.ts:311 | `plan_query` planner/planner_builder.rs:269 | ✅ exact |
| `applyToCondition` | planner/planner-builder.ts:322 | `apply_to_condition` planner/planner_builder.rs:306 | ✅ exact |
| `applyPlansToAST` | planner/planner-builder.ts:357 | `apply_plans_to_ast` planner/planner_builder.rs:276 | ✅ exact |
| `PlannerConnection` | planner/planner-connection.ts:59 | `PlannerConnection` planner/planner_connection.rs:27 | ✅ exact |
| `closestJoinOrSource` | planner/planner-connection.ts:148 | `closest_join_or_source` planner/planner_connection.rs:100 | ✅ exact |
| `propagateConstraints` | planner/planner-connection.ts:166 | `propagate_constraints` planner/planner_connection.rs:104 | ✅ exact |
| `estimateCost` | planner/planner-connection.ts:187 | `estimate_cost` planner/planner_connection.rs:119 | ✅ exact |
| `unlimit` | planner/planner-connection.ts:249 | `unlimit` planner/planner_connection.rs:165 | ✅ exact |
| `propagateUnlimitFromFlippedJoin` | planner/planner-connection.ts:266 | `propagate_unlimit_from_flipped_join` planner/planner_connection.rs:172 | ✅ exact |
| `captureConstraints` | planner/planner-connection.ts:281 | `capture_constraints` planner/planner_connection.rs:182 | ✅ exact |
| `restoreConstraints` | planner/planner-connection.ts:289 | `restore_constraints` planner/planner_connection.rs:186 | ✅ exact |
| `getConstraintsForDebug` | planner/planner-connection.ts:301 | N/A | 📌 debug introspection; not ported |
| `getFiltersForDebug` | planner/planner-connection.ts:310 | N/A | 📌 debug introspection; not ported |
| `getSortForDebug` | planner/planner-connection.ts:315 | N/A | 📌 debug introspection; not ported |
| `getConstraintCostsForDebug` | planner/planner-connection.ts:320 | N/A | 📌 debug introspection; not ported |
| `FanoutCostModel` | planner/planner-connection.ts:333 | `set_cost_model_conn` engine/mod.rs:446 | 🔁 rename 0.50 |
| `CostModelCost` | planner/planner-connection.ts:335 | `CostModelCost` planner/planner_connection.rs:12 | ✅ exact |
| `ConnectionCostModel` | planner/planner-connection.ts:340 | `ensure_cost_model` engine/mod.rs:477 | 🔁 rename 0.50 |
| `PlannerConstraint` | planner/planner-constraint.ts:8 | — | 🟥 UNRESOLVED |
| `mergeConstraints` | planner/planner-constraint.ts:14 | `merge_constraints` planner/planner_constraint.rs:19 | ✅ exact |
| `PlannerFanIn` | planner/planner-fan-in.ts:28 | `PlannerFanIn` planner/planner_fan_in.rs:8 | ✅ exact |
| `convertToUFI` | planner/planner-fan-in.ts:60 | `convert_to_ufi` planner/planner_fan_in.rs:42 | ✅ exact |
| `PlannerFanOut` | planner/planner-fan-out.ts:11 | `PlannerFanOut` planner/planner_fan_out.rs:8 | ✅ exact |
| `addOutput` | planner/planner-fan-out.ts:26 | `add_output` planner/planner_fan_out.rs:30 | ✅ exact |
| `outputs` | planner/planner-fan-out.ts:30 | `outputs` planner/planner_fan_out.rs:37 | ✅ exact |
| `convertToUFO` | planner/planner-fan-out.ts:86 | `convert_to_ufo` planner/planner_fan_out.rs:64 | ✅ exact |
| `PlanState` | planner/planner-graph.ts:18 | `PlanState` planner/planner_graph.rs:19 | ✅ exact |
| `PlannerGraph` | planner/planner-graph.ts:42 | `PLANNER_GRAPH` live_count.rs:21 | ✅ exact |
| `resetPlanningState` | planner/planner-graph.ts:61 | `reset_planning_state` planner/planner_graph.rs:82 | ✅ exact |
| `addSource` | planner/planner-graph.ts:71 | `add_source` planner/planner_graph.rs:59 | ✅ exact |
| `hasSource` | planner/planner-graph.ts:93 | `has_source` planner/planner_graph.rs:55 | ✅ exact |
| `setTerminus` | planner/planner-graph.ts:101 | `set_terminus` planner/planner_graph.rs:78 | ✅ exact |
| `getTotalCost` | planner/planner-graph.ts:122 | `get_total_cost` planner/planner_graph.rs:103 | ✅ exact |
| `capturePlanningSnapshot` | planner/planner-graph.ts:136 | `capture_planning_snapshot` planner/planner_graph.rs:108 | ✅ exact |
| `restorePlanningSnapshot` | planner/planner-graph.ts:157 | `restore_planning_snapshot` planner/planner_graph.rs:130 | ✅ exact |
| `plan` | planner/planner-graph.ts:256 | `plan` planner/planner_graph.rs:154 | ✅ exact |
| `buildFOFICache` | planner/planner-graph.ts:389 | `build_fofi_cache` planner/planner_graph.rs:233 | ✅ exact |
| `checkAndConvertFOFI` | planner/planner-graph.ts:406 | `check_and_convert_fofi` planner/planner_graph.rs:298 | ✅ exact |
| `findFIAndJoins` | planner/planner-graph.ts:420 | `find_fi_and_joins` planner/planner_graph.rs:242 | ✅ exact |
| `propagateUnlimitForFlippedJoins` | planner/planner-graph.ts:465 | planner/planner_graph.rs:298 | 📌 renamed |
| `translateConstraintsForFlippedJoin` | planner/planner-join.ts:27 | `translate_constraints_for_flipped_join` planner/planner_join.rs:8 | ✅ exact |
| `PlannerJoin` | planner/planner-join.ts:96 | `PlannerJoin` planner/planner_join.rs:37 | ✅ exact |
| `flipIfNeeded` | planner/planner-join.ts:143 | N/A | 📌 dead code in TS; planning calls flip() directly (Rust too) |
| `flip` | planner/planner-join.ts:154 | `flip` planner/planner_join.rs:84 | ✅ exact |
| `isFlippable` | planner/planner-join.ts:167 | `is_flippable` planner/planner_join.rs:93 | ✅ exact |
| `propagateUnlimit` | planner/planner-join.ts:186 | `propagate_unlimit` planner/planner_graph.rs:311 | ✅ exact |
| `getName` | planner/planner-join.ts:427 | `get_name` planner/planner_join.rs:211 | ✅ exact |
| `getDebugInfo` | planner/planner-join.ts:436 | N/A | 📌 debug introspection; not ported |
| `UnflippableJoinError` | planner/planner-join.ts:449 | — | 🟥 UNRESOLVED |
| `getNodeName` | planner/planner-join.ts:460 | N/A | 📌 debug introspection; not ported |
| `PlannerNode` | planner/planner-node.ts:11 | `PLANNER_NODE` live_count.rs:23 | ✅ exact |
| `CostEstimate` | planner/planner-node.ts:18 | `CostEstimate` planner/planner_node.rs:19 | ✅ exact |
| `omitFanout` | planner/planner-node.ts:61 | `get_fanout` sqlite/sqlite_stat_fanout.rs:115 | 🔁 rename 0.50 |
| `NodeType` | planner/planner-node.ts:66 | `node_type` planner/planner_fan_in.rs:26 | ✅ exact |
| `JoinOrConnection` | planner/planner-node.ts:68 | `JoinOrConnection` planner/planner_node.rs:81 | ✅ exact |
| `JoinType` | planner/planner-node.ts:70 | `join_type` planner/planner_join.rs:90 | ✅ exact |
| `PlannerSource` | planner/planner-source.ts:10 | `PlannerSource` planner/planner_source.rs:7 | ✅ exact |
| `PlannerTerminus` | planner/planner-terminus.ts:8 | `PlannerTerminus` planner/planner_terminus.rs:5 | ✅ exact |
| `pinned` | planner/planner-terminus.ts:16 | planner/runtime.rs | 📌 method |
| `completeOrdering` | query/complete-ordering.ts:6 | `complete_ordering` query/complete_ordering.rs:10 | ✅ exact |
| `completeOrderingInCondition` | query/complete-ordering.ts:46 | `complete_ordering_in_condition` query/complete_ordering.rs:54 | ✅ exact |
| `addPrimaryKeys` | query/complete-ordering.ts:74 | `add_primary_keys` query/complete_ordering.rs:81 | ✅ exact |
| `QueryParseError` | query/error.ts:1 | `QueryParseError` query/error.rs:8 | ✅ exact |
| `escapeLike` | query/escape-like.ts:1 | `escape_like` query/escape_like.rs:8 | ✅ exact |
| `ParameterReference` | query/expression.ts:21 | — | 🟥 UNRESOLVED |
| `ExpressionFactory` | query/expression.ts:41 | — | 🟥 UNRESOLVED |
| `ExpressionBuilder` | query/expression.ts:48 | — | 🟥 UNRESOLVED |
| `eb` | query/expression.ts:69 | — | 🟥 UNRESOLVED |
| `cmp` | query/expression.ts:73 | `cmp` ivm/source.rs:1481 | ✅ exact |
| `cmpLit` | query/expression.ts:104 | — | 🟥 UNRESOLVED |
| `and` | query/expression.ts:134 | `and` query/expression.rs:11 | ✅ exact |
| `or` | query/expression.ts:148 | `or` query/expression.rs:31 | ✅ exact |
| `isParameterReference` | query/expression.ts:220 | — | 🟥 UNRESOLVED |
| `TRUE` | query/expression.ts:231 | `true_val` query/expression.rs:181 | 🔁 rename 0.50 |
| `isAlwaysTrue` | query/expression.ts:241 | `is_always_true` query/expression.rs:190 | ✅ exact |
| `isAlwaysFalse` | query/expression.ts:245 | `is_always_false` query/expression.rs:194 | ✅ exact |
| `simplifyCondition` | query/expression.ts:249 | `simplify_condition` query/expression.rs:104 | ✅ exact |
| `flatten` | query/expression.ts:269 | `flatten` query/expression.rs:134 | ✅ exact |
| `negateOperator` | query/expression.ts:308 | `negate_operator` query/expression.rs:158 | ✅ exact |
| `filterUndefined` | query/expression.ts:314 | — | 🟥 UNRESOLVED |
| `filterTrue` | query/expression.ts:318 | — | 🟥 UNRESOLVED |
| `filterFalse` | query/expression.ts:322 | — | 🟥 UNRESOLVED |
| `MeasurePushOperator` | query/measure-push-operator.ts:16 | `MeasurePushOperator` query/measure_push_operator.rs:27 | ✅ exact |
| `ClientMetricMap` | query/metrics-delegate.ts:3 | — | 🟥 UNRESOLVED |
| `ServerMetricMap` | query/metrics-delegate.ts:9 | — | 🟥 UNRESOLVED |
| `MetricMap` | query/metrics-delegate.ts:14 | `Metric` query/metrics_delegate.rs:10 | 🔁 rename 0.50 |
| `MetricsDelegate` | query/metrics-delegate.ts:16 | `MetricsDelegate` query/measure_push_operator.rs:15 | ✅ exact |
| `addMetric` | query/metrics-delegate.ts:17 | `add_metric` query/measure_push_operator.rs:16 | ✅ exact |
| `isClientMetric` | query/metrics-delegate.ts:24 | `is_client_metric` query/metrics_delegate.rs:29 | ✅ exact |
| `isServerMetric` | query/metrics-delegate.ts:30 | `is_server_metric` query/metrics_delegate.rs:38 | ✅ exact |
| `QueryFn` | query/named.ts:7 | — | 🟥 UNRESOLVED |
| `SyncedQuery` | query/named.ts:17 | `SyncedQuery` query/named.rs:25 | ✅ exact |
| `normalizeParser` | query/named.ts:29 | — | 🟥 UNRESOLVED |
| `syncedQueryWithContext` | query/named.ts:65 | `with_context` query/named.rs:55 | 🔁 rename 0.50 |
| `syncedQueryImpl` | query/named.ts:85 | — | 🟥 UNRESOLVED |
| `withValidation` | query/named.ts:103 | — | 🟥 UNRESOLVED |
| `ParseFn` | query/named.ts:140 | `parse_value` ivm/cap.rs:564 | 🔁 rename 0.50 |
| `HasParseFn` | query/named.ts:143 | — | 🟥 UNRESOLVED |
| `Parser` | query/named.ts:148 | — | 🟥 UNRESOLVED |
| `CustomQueryID` | query/named.ts:150 | `CustomQueryID` query/named.rs:14 | ✅ exact |
| `QueryDelegateBase` | query/query-delegate-base.ts:35 | `QueryDelegateBase` query/query_delegate_base.rs:91 | ✅ exact |
| `batchViewUpdates` | query/query-delegate-base.ts:40 | `batch_view_updates` query/query_delegate_base.rs:70 | ✅ exact |
| `materialize` | query/query-delegate-base.ts:56 | `materialize` query/query_delegate_base.rs:74 | ✅ exact |
| `run` | query/query-delegate-base.ts:108 | `run` query/query_delegate_base.rs:79 | ✅ exact |
| `preload` | query/query-delegate-base.ts:123 | `preload` query/query_delegate_base.rs:80 | ✅ exact |
| `addServerQuery` | query/query-delegate-base.ts:185 | `add_server_query` query/query_delegate_base.rs:53 | ✅ exact |
| `addCustomQuery` | query/query-delegate-base.ts:193 | `add_custom_query` query/query_delegate_base.rs:59 | ✅ exact |
| `updateServerQuery` | query/query-delegate-base.ts:206 | `update_server_query` query/query_delegate_base.rs:66 | ✅ exact |
| `updateCustomQuery` | query/query-delegate-base.ts:214 | `update_custom_query` query/query_delegate_base.rs:67 | ✅ exact |
| `flushQueryChanges` | query/query-delegate-base.ts:222 | `flush_query_changes` query/query_delegate_base.rs:68 | ✅ exact |
| `onTransactionCommit` | query/query-delegate-base.ts:230 | `on_transaction_commit` query/query_delegate_base.rs:69 | ✅ exact |
| `assertValidRunOptions` | query/query-delegate-base.ts:238 | `assert_valid_run_options` query/query_delegate_base.rs:71 | ✅ exact |
| `preloadImpl` | query/query-delegate-base.ts:288 | — | 🟥 UNRESOLVED |
| `materializeImpl` | query/query-delegate-base.ts:327 | — | 🟥 UNRESOLVED |
| `arrayViewFactory` | query/query-delegate-base.ts:420 | `ArrayViewOutput` ivm/array_view.rs:126 | 🔁 rename 0.50 |
| `CommitListener` | query/query-delegate.ts:18 | — | 🟥 UNRESOLVED |
| `GotCallback` | query/query-delegate.ts:19 | — | 🟥 UNRESOLVED |
| `NewQueryDelegate` | query/query-delegate.ts:21 | `ZqliteQueryDelegate` sqlite/query_delegate.rs:25 | 🔁 rename 0.50 |
| `newQuery` | query/query-delegate.ts:22 | — | 🟥 UNRESOLVED |
| `QueryDelegate` | query/query-delegate.ts:38 | `QueryDelegate` query/query_delegate_base.rs:52 | ✅ exact |
| `newQueryImpl` | query/query-impl.ts:61 | — | 🟥 UNRESOLVED |
| `QueryImpl` | query/query-impl.ts:93 | — | 🟥 UNRESOLVED |
| `nameAndArgs` | query/query-impl.ts:157 | `name_and_args` query/query_internals.rs:17 | ✅ exact |
| `hash` | query/query-impl.ts:173 | `hash` query/query_internals.rs:15 | ✅ exact |
| `ast` | query/query-impl.ts:565 | `Ast` builder/ast.rs:10 | ✅ exact |
| `asQueryImpl` | query/query-impl.ts:574 | — | 🟥 UNRESOLVED |
| `throwQueryNotRunnable` | query/query-impl.ts:583 | — | 🟥 UNRESOLVED |
| `isCompoundKey` | query/query-impl.ts:587 | — | 🟥 UNRESOLVED |
| `isOneHop` | query/query-impl.ts:591 | — | 🟥 UNRESOLVED |
| `isTwoHop` | query/query-impl.ts:595 | — | 🟥 UNRESOLVED |
| `queryInternalsTag` | query/query-internals.ts:9 | — | 🟥 UNRESOLVED |
| `QueryInternals` | query/query-internals.ts:20 | `QueryInternals` query/query_internals.rs:12 | ✅ exact |
| `asQueryInternals` | query/query-internals.ts:80 | — | 🟥 UNRESOLVED |
| `isQueryInternals` | query/query-internals.ts:94 | `is_query_internals` query/query_internals.rs:22 | ✅ exact |
| `asQuery` | query/query-internals.ts:102 | `as_query` query/query_internals.rs:30 | ✅ exact |
| `AnyQueryInternals` | query/query-internals.ts:114 | — | 🟥 UNRESOLVED |
| `CustomQueryTypes` | query/query-registry.ts:25 | — | 🟥 UNRESOLVED |
| `CustomQuery` | query/query-registry.ts:43 | `CustomQuery` query/query_registry.rs:19 | ✅ exact |
| `AnyCustomQuery` | query/query-registry.ts:79 | `get_custom_query_id` query/query_internals.rs:16 | 🔁 rename 0.67 |
| `isQuery` | query/query-registry.ts:81 | — | 🟥 UNRESOLVED |
| `QueryRequestTypes` | query/query-registry.ts:92 | — | 🟥 UNRESOLVED |
| `QueryRequest` | query/query-registry.ts:108 | `QueryRequest` query/query_registry.rs:26 | ✅ exact |
| `QueryOrQueryRequest` | query/query-registry.ts:141 | — | 🟥 UNRESOLVED |
| `addContextToQuery` | query/query-registry.ts:159 | — | 🟥 UNRESOLVED |
| `isQueryRegistry` | query/query-registry.ts:183 | — | 🟥 UNRESOLVED |
| `QueryRegistryTypes` | query/query-registry.ts:191 | — | 🟥 UNRESOLVED |
| `QueryRegistry` | query/query-registry.ts:195 | — | 🟥 UNRESOLVED |
| `AnyQueryRegistry` | query/query-registry.ts:202 | — | 🟥 UNRESOLVED |
| `FromQueryTree` | query/query-registry.ts:223 | — | 🟥 UNRESOLVED |
| `QueryDefinitions` | query/query-registry.ts:239 | — | 🟥 UNRESOLVED |
| `QueryDefinitionTypes` | query/query-registry.ts:247 | — | 🟥 UNRESOLVED |
| `QueryDefinition` | query/query-registry.ts:264 | — | 🟥 UNRESOLVED |
| `AnyQueryDefinition` | query/query-registry.ts:283 | — | 🟥 UNRESOLVED |
| `isQueryDefinition` | query/query-registry.ts:285 | — | 🟥 UNRESOLVED |
| `QueryDefinitionFunction` | query/query-registry.ts:293 | — | 🟥 UNRESOLVED |
| `QueryExecutionFunction` | query/query-registry.ts:300 | — | 🟥 UNRESOLVED |
| `defineQuery` | query/query-registry.ts:366 | — | 🟥 UNRESOLVED |
| `defineQueryWithType` | query/query-registry.ts:469 | — | 🟥 UNRESOLVED |
| `createQuery` | query/query-registry.ts:520 | `create` snapshotter/snapshotter.rs:484 | 🔁 rename 0.50 |
| `defineQueries` | query/query-registry.ts:632 | — | 🟥 UNRESOLVED |
| `DeepMerge` | query/query-registry.ts:650 | — | 🟥 UNRESOLVED |
| `AssertQueryDefinitions` | query/query-registry.ts:709 | — | 🟥 UNRESOLVED |
| `EnsureQueryDefinitions` | query/query-registry.ts:713 | — | 🟥 UNRESOLVED |
| `defineQueriesWithType` | query/query-registry.ts:722 | — | 🟥 UNRESOLVED |
| `getQuery` | query/query-registry.ts:760 | — | 🟥 UNRESOLVED |
| `mustGetQuery` | query/query-registry.ts:768 | — | 🟥 UNRESOLVED |
| `newRunnableQuery` | query/runnable-query-impl.ts:19 | `new_runnable_query` query/runnable_query_impl.rs:12 | ✅ exact |
| `RunnableQueryImpl` | query/runnable-query-impl.ts:37 | — | 🟥 UNRESOLVED |
| `SchemaQuery` | query/schema-query.ts:8 | — | 🟥 UNRESOLVED |
| `ConditionalSchemaQuery` | query/schema-query.ts:12 | — | 🟥 UNRESOLVED |
| `newStaticQuery` | query/static-query.ts:6 | `new_static_query` query/runnable_query_impl.rs:21 | ✅ exact |
| `newExpressionBuilder` | query/static-query.ts:20 | `new_expression_builder` query/runnable_query_impl.rs:32 | ✅ exact |
| `TimeUnit` | query/ttl.ts:3 | — | 🟥 UNRESOLVED |
| `TTL` | query/ttl.ts:17 | — | 🟥 UNRESOLVED |
| `DEFAULT_TTL` | query/ttl.ts:19 | `default` credit.rs:165 | 🔁 rename 0.50 |
| `DEFAULT_TTL_MS` | query/ttl.ts:20 | `DEFAULT_TTL_MS` query/ttl.rs:6 | ✅ exact |
| `DEFAULT_PRELOAD_TTL` | query/ttl.ts:22 | — | 🟥 UNRESOLVED |
| `DEFAULT_PRELOAD_TTL_MS` | query/ttl.ts:23 | — | 🟥 UNRESOLVED |
| `MAX_TTL` | query/ttl.ts:25 | — | 🟥 UNRESOLVED |
| `MAX_TTL_MS` | query/ttl.ts:26 | `MAX_TTL_MS` query/ttl.rs:8 | ✅ exact |
| `parseTTL` | query/ttl.ts:36 | `parse_ttl` query/ttl.rs:12 | ✅ exact |
| `compareTTL` | query/ttl.ts:50 | `compare_ttl` query/ttl.rs:53 | ✅ exact |
| `normalizeTTL` | query/ttl.ts:62 | — | 🟥 UNRESOLVED |
| `clampTTL` | query/ttl.ts:89 | `clamp_ttl` query/ttl.rs:42 | ✅ exact |
| `ResultType` | query/typed-view.ts:5 | `ResultType` query/typed_view.rs:10 | ✅ exact |
| `Listener` | query/typed-view.ts:12 | — | 🟥 UNRESOLVED |
| `TypedView` | query/typed-view.ts:18 | `TypedView` query/typed_view.rs:22 | ✅ exact |
| `InputValidationError` | query/validate-input.ts:3 | `InputValidationError` query/validate_input.rs:10 | ✅ exact |
| `validateInput` | query/validate-input.ts:32 | `validate_input` query/validate_input.rs:30 | ✅ exact |
| `titleCase` | query/validate-input.ts:60 | — | 🟥 UNRESOLVED |
