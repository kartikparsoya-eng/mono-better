# Layer-8 traffic-driven path differential

_Same traffic at both sides; a pair is a ledger-resolved TS fn and its rust twin. `TS-HOT/RUST-COLD` = TS took the path, rust never entered its twin — the unwired-port class. `BOTH-COLD` = the traffic never exercised the pair (traffic gap, not divergence)._

## cvr — 76 fn-pairs: 45 BOTH-HOT, 10 TS-HOT/RUST-COLD, 3 RUST-HOT/TS-COLD, 18 BOTH-COLD (traffic exercised 45/76)

### ❌ TS-HOT/RUST-COLD

| TS symbol (file:line) | rust twin (file:line) | ts# | rust# | how |
|---|---|---|---|---|
| `rowIDSignatureUnit` (services/view-syncer/row-set-signature.ts:10) | `row_id_signature_unit` (row_set_signature.rs:17) | 325 | 0 | exact |
| `maxVersion` (services/view-syncer/schema/types.ts:72) | `max_version` (schema/types.rs:93) | 232 | 0 | exact |
| `versionToCookie` (services/view-syncer/schema/types.ts:76) | `version_to_cookie` (schema/types.rs:107) | 209 | 0 | exact |
| `clear` (services/view-syncer/row-record-cache.ts:334) | `clear` (row_record_cache.rs:420) | 121 | 0 | exact |
| `getTTLClock` (services/view-syncer/cvr-store.ts:569) | `get_ttl_clock` (cvr_store.rs:387) | 110 | 0 | exact |
| `versionToNullableCookie` (services/view-syncer/schema/types.ts:80) | `version_to_nullable_cookie` (schema/types.rs:111) | 53 | 0 | exact |
| `rowCount` (services/view-syncer/cvr-store.ts:1227) | `row_count` (cvr_store.rs:303) | 48 | 0 | exact |
| `executeRowUpdates` (services/view-syncer/row-record-cache.ts:414) | `execute_row_updates` (row_record_cache.rs:433) | 19 | 0 | exact |
| `cancel` (services/view-syncer/client-handler.ts:74) | `cancel` (client_handler.rs:104) | 17 | 0 | exact |
| `fail` (services/view-syncer/client-handler.ts:175) | `fail` (client_handler.rs:103) | 1 | 0 | exact |

### ⚠️ RUST-HOT/TS-COLD

| TS symbol (file:line) | rust twin (file:line) | ts# | rust# | how |
|---|---|---|---|---|
| `addPatch` (services/view-syncer/client-handler.ts:73) | `add_patch` (client_handler.rs:292) | 0 | 2474 | exact |
| `maybeVersionString` (services/view-syncer/schema/types.ts:392) | `maybe_version_string` (schema/types.rs:180) | 0 | 200 | exact |
| `RowSetSignatureProvider` (services/view-syncer/cvr.ts:544) | `record_row_set_signature_drift` (otel_metrics.rs:161) | 0 | 6 | fuzzy |

### ⚠️ count-ratio anomalies (≥100× — check topology)

| pair | ts# | rust# |
|---|---|---|
| `end` → `end` | 1 | 136 |

## ivm — 215 fn-pairs: 91 BOTH-HOT, 24 TS-HOT/RUST-COLD, 20 RUST-HOT/TS-COLD, 80 BOTH-COLD (traffic exercised 91/215)

### ❌ TS-HOT/RUST-COLD

| TS symbol (file:line) | rust twin (file:line) | ts# | rust# | how |
|---|---|---|---|---|
| `assert` (builder/builder.ts:421) | `assert_matches` (replay.rs:703) | 110520 | 0 | fuzzy |
| `run` (query/query-delegate-base.ts:108) | `run` (query/query_delegate_base.rs:79) | 25884 | 0 | exact |
| `flush` (ivm/array-view.ts:173) | `flush` (ivm/array_view.rs:88) | 613 | 0 | exact |
| `simplifyCondition` (query/expression.ts:249) | `simplify_condition` (query/expression.rs:104) | 450 | 0 | exact |
| `assertOrderingIncludesPK` (builder/builder.ts:742) | `assert_ordering_includes_pk` (query/complete_ordering.rs:39) | 337 | 0 | exact |
| `has` (ivm/constraint.ts:173) | `has` (ivm/source.rs:261) | 232 | 0 | exact |
| `isAlwaysFalse` (query/expression.ts:245) | `is_always_false` (query/expression.rs:194) | 163 | 0 | exact |
| `createStorage` (builder/builder.ts:83) | `create_storage` (builder/builder.rs:47) | 162 | 0 | exact |
| `addMetric` (query/metrics-delegate.ts:17) | `add_metric` (query/measure_push_operator.rs:16) | 160 | 0 | exact |
| `isServerMetric` (query/metrics-delegate.ts:30) | `is_server_metric` (query/metrics_delegate.rs:38) | 160 | 0 | exact |
| `serializePK` (ivm/cap.ts:315) | `serialize_pk` (ivm/cap.rs:155) | 142 | 0 | exact |
| `beginFilter` (ivm/exists.ts:71) | `begin_filter` (ivm/filter_operators.rs:26) | 138 | 0 | exact |
| `endFilter` (ivm/exists.ts:75) | `end_filter` (ivm/filter_operators.rs:28) | 138 | 0 | exact |
| `stop` (ivm/stopable-iterator.ts:20) | `stop` (ivm/stopable_iterator.rs:23) | 136 | 0 | exact |
| `setFilterOutput` (ivm/exists.ts:67) | `set_filter_output` (ivm/filter_operators.rs:21) | 121 | 0 | exact |
| `buildFilterPipeline` (ivm/filter-operators.ts:148) | `build_filter_pipeline` (ivm/filter_operators.rs:146) | 118 | 0 | exact |
| `flatten` (query/expression.ts:269) | `flatten` (query/expression.rs:134) | 108 | 0 | exact |
| `clampTTL` (query/ttl.ts:89) | `clamp_ttl` (query/ttl.rs:37) | 99 | 0 | exact |
| `parseTTL` (query/ttl.ts:36) | `parse_ttl` (query/ttl.rs:12) | 99 | 0 | exact |
| `genPush` (ivm/source.ts:96) | `gen_push` (ivm/source.rs:65) | 96 | 0 | exact |
| `valuesEqual` (ivm/data.ts:112) | `values_equal` (ivm/data.rs:199) | 90 | 0 | exact |
| `isAlwaysTrue` (query/expression.ts:241) | `is_always_true` (query/expression.rs:190) | 82 | 0 | exact |
| `applyAnd` (builder/builder.ts:541) | `apply_overlay_and_stream` (ivm/source.rs:903) | 82 | 0 | fuzzy |
| `assertNoNotExists` (builder/builder.ts:232) | `assert_no_not_exists` (builder/builder.rs:609) | 24 | 0 | exact |

### ⚠️ RUST-HOT/TS-COLD

| TS symbol (file:line) | rust twin (file:line) | ts# | rust# | how |
|---|---|---|---|---|
| `data` (ivm/array-view.ts:111) | `data` (ivm/array_view.rs:74) | 0 | 112594 | exact |
| `FanoutCostModel` (planner/planner-connection.ts:333) | `cost_model_with_cache` (planner/runtime.rs:62) | 0 | 5776 | fuzzy |
| `JoinType` (planner/planner-node.ts:70) | `join_type` (planner/planner_join.rs:90) | 0 | 3459 | exact |
| `NODE` (ivm/change-index-enum.ts:2) | `node` (ivm/change.rs:52) | 0 | 3352 | exact |
| `DEFAULT_TTL` (query/ttl.ts:19) | `default` (credit.rs:165) | 0 | 2130 | fuzzy |
| `cmp` (query/expression.ts:73) | `cmp` (ivm/source.rs:1477) | 0 | 1054 | exact |
| `canonicalKeyForTest` (ivm/flipped-join.ts:572) | `canonical_key_row` (ivm/flipped_join.rs:573) | 0 | 934 | fuzzy |
| `ROW` (ivm/source-change-index-enum.ts:2) | `row` (ivm/data.rs:274) | 0 | 384 | exact |
| `constraintMatchesPrimaryKey` (ivm/constraint.ts:46) | `constraint_matches_primary_key` (ivm/constraint.rs:39) | 0 | 205 | exact |
| `SourceChangeAdd` (ivm/source.ts:9) | `source_change_to_change` (ivm/source.rs:535) | 0 | 196 | fuzzy |
| `NodeType` (planner/planner-node.ts:66) | `node_type` (planner/planner_fan_in.rs:26) | 0 | 158 | exact |
| `SourceChangeEdit` (ivm/source.ts:15) | `push_source_change` (engine/mod.rs:1546) | 0 | 128 | fuzzy |
| `PartitionKey` (ivm/take.ts:42) | `optional_constraint_matches_partition_key` (ivm/take.rs:1022) | 0 | 45 | fuzzy |
| `ParseFn` (query/named.ts:140) | `parse_value` (ivm/cap.rs:564) | 0 | 38 | fuzzy |
| `CaughtChildChange` (ivm/catch.ts:28) | `push_child_change` (ivm/flipped_join.rs:301) | 0 | 23 | fuzzy |
| `MultiConstraint` (ivm/operator.ts:61) | `multi_constraint_to_sql` (sqlite/query_builder.rs:159) | 0 | 16 | fuzzy |
| `createQuery` (query/query-registry.ts:520) | `create` (snapshotter/snapshotter.rs:484) | 0 | 16 | fuzzy |
| `EditChange` (ivm/change.ts:57) | `push_edit_change` (ivm/take.rs:686) | 0 | 9 | fuzzy |
| `AddChange` (ivm/change.ts:17) | `push_add_change` (ivm/take.rs:469) | 0 | 4 | fuzzy |
| `ConnectionCostModel` (planner/planner-connection.ts:340) | `set_cost_model_conn` (engine/mod.rs:431) | 0 | 2 | fuzzy |

### ⚠️ count-ratio anomalies (≥100× — check topology)

| pair | ts# | rust# |
|---|---|---|
| `get` → `get` | 37934 | 367 |
| `next` → `next` | 16 | 7969 |
| `skipYields` → `skip_yields` | 8 | 1540 |

## syncer — 108 fn-pairs: 59 BOTH-HOT, 18 TS-HOT/RUST-COLD, 3 RUST-HOT/TS-COLD, 28 BOTH-COLD (traffic exercised 59/108)

### ❌ TS-HOT/RUST-COLD

| TS symbol (file:line) | rust twin (file:line) | ts# | rust# | how |
|---|---|---|---|---|
| `reset` (services/view-syncer/pipeline-driver.ts:343) | `record_reset` (metrics.rs:892) | 6609 | 0 | fuzzy |
| `initialized` (services/view-syncer/pipeline-driver.ts:334) | `initialized` (services/view_syncer/pipeline_driver.rs:209) | 309 | 0 | exact |
| `removeQuery` (services/view-syncer/pipeline-driver.ts:834) | `remove_query` (services/view_syncer/pipeline_driver.rs:390) | 103 | 0 | exact |
| `minDefined` (services/view-syncer/connection-context-manager.ts:858) | `min_defined` (services/view_syncer/connection_context_manager.rs:952) | 52 | 0 | exact |
| `planMaintenance` (services/view-syncer/connection-context-manager.ts:161) | `plan_maintenance` (services/view_syncer/connection_context_manager.rs:768) | 52 | 0 | exact |
| `shouldDrain` (services/view-syncer/drain-coordinator.ts:41) | `should_drain` (services/view_syncer/drain_coordinator.rs:57) | 52 | 0 | exact |
| `getRow` (services/view-syncer/pipeline-driver.ts:906) | `get_row` (services/view_syncer/pipeline_driver.rs:544) | 48 | 0 | exact |
| `getGroupState` (services/view-syncer/connection-context-manager.ts:159) | `get_group_state` (services/view_syncer/connection_context_manager.rs:762) | 6 | 0 | exact |
| `drainNextIn` (services/view-syncer/drain-coordinator.ts:45) | `drain_next_in` (services/view_syncer/drain_coordinator.rs:66) | 4 | 0 | exact |
| `setSharedRetransformReady` (services/view-syncer/connection-context-manager.ts:145) | `set_shared_retransform_ready` (services/view_syncer/connection_context_manager.rs:701) | 4 | 0 | exact |
| `transformAndHashQuery` (auth/read-authorizer.ts:24) | `transform_and_hash_query` (auth/read_authorizer.rs:63) | 4 | 0 | exact |
| `validateConnection` (services/view-syncer/connection-context-manager.ts:121) | `validate_connection` (services/view_syncer/connection_context_manager.rs:601) | 4 | 0 | exact |
| `destroy` (custom-queries/transform-query.ts:98) | `destroy` (services/view_syncer/pipeline_driver.rs:563) | 2 | 0 | exact |
| `literalArrayIncludes` (services/view-syncer/query-covering.ts:432) | `literal_array_includes` (services/view_syncer/query_covering.rs:442) | 2 | 0 | exact |
| `mustGetBackgroundConnectionContext` (services/view-syncer/connection-context-manager.ts:157) | `must_get_background_connection_context` (services/view_syncer/connection_context_manager.rs:754) | 2 | 0 | exact |
| `pickToken` (auth/auth.ts:126) | `pick_token` (services/view_syncer/connection_context_manager.rs:277) | 2 | 0 | exact |
| `draining` (services/view-syncer/drain-coordinator.ts:37) | `is_draining` (services/view_syncer/drain_coordinator.rs:112) | 2 | 0 | fuzzy |
| `boundsCoveredBy` (services/view-syncer/query-covering.ts:143) | `bounds_covered_by` (services/view_syncer/query_covering.rs:217) | 1 | 0 | exact |

### ⚠️ RUST-HOT/TS-COLD

| TS symbol (file:line) | rust twin (file:line) | ts# | rust# | how |
|---|---|---|---|---|
| `transformQuery` (auth/read-authorizer.ts:45) | `transform_query` (auth/read_authorizer.rs:79) | 0 | 6562 | exact |
| `RowChange` (services/view-syncer/pipeline-driver.ts:83) | `row_change_to_maps` (sync_engine.rs:1648) | 0 | 778 | fuzzy |
| `ViewSyncer` (services/view-syncer/view-syncer.ts:132) | `create_view_syncer` (main.rs:761) | 0 | 2 | fuzzy |

### ⚠️ count-ratio anomalies (≥100× — check topology)

| pair | ts# | rust# |
|---|---|---|
| `listIndexes` → `list_unique_indexes` | 13 | 5854 |


_full row set: parity/coverage/l8-rows.json_
