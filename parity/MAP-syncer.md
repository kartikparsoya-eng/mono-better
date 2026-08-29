# TS ⇄ Rust parity map — `syncer` crate

_Deterministic. File edges + symbol pairs are derived from **shared symbol content**, never filenames — so renamed files (e.g. `drain-coordinator.ts`→`drain.rs`) and renamed symbols (`cvrErrorKind`→`CVRStoreError`) still bind. Bodies are not compared; behavior drift needs Layer-2 body review._

- symbols: TS **286**, Rust **638** · resolved pairs **168** (exact 139 + fuzzy 29) + aliases 62
- 🟥 TS UNRESOLVED: **62** (**24** behavioral ⇒ investigate · 38 structural: zod/DDL/type-alias ⇒ serde/inline-SQL, expected) · 🟦 Rust-only ADDED: **470**

> ⚠️ **Behavioral TS symbols with no Rust resolution — check these:** `accumulate` (services/view-syncer/pipeline-driver.ts), `apiRequests` (custom/metrics.ts), `assertAreCompatiblePushes` (services/mutagen/pusher.ts), `assertNormalized` (config/zero-config.ts), `getMeter` (observability/metrics.ts), `getNormalizedZeroConfig` (config/zero-config.ts), `getOrCreateGauge` (observability/metrics.ts), `getOrCreateHistogram` (observability/metrics.ts), `getOrCreateLatencyHistogram` (observability/metrics.ts), `getOrCreateUpDownCounter` (observability/metrics.ts), `getServerVersion` (config/zero-config.ts), `getZeroConfig` (config/zero-config.ts), `hasExpiredQueries` (services/view-syncer/view-syncer.ts), `hasRefs` (services/mutagen/pusher.ts), `initEventSink` (server/syncer.ts), `metricsForProtocol` (services/view-syncer/inspect-handler.ts), `recordMs` (observability/metrics.ts), `ref` (services/mutagen/pusher.ts), `registerSQLiteCorruptionDiagnosticTarget` (server/syncer.ts), `resetWarnOnceState` (config/zero-config.ts), `rowSetSignature` (services/view-syncer/pipeline-driver.ts), `startAnonymousTelemetry` (server/syncer.ts), `unref` (services/mutagen/pusher.ts), `warnOnce` (config/zero-config.ts)

## 1 · File structure diff

TS origin files: **24**  ·  Rust files: **63** (37 new)

| TS file (LOC) | rel | Rust file(s) (shared syms) |
|---|---|---|
| `auth/auth.ts` (243) | **MERGED** | `services/view_syncer/connection_context_manager.rs` (6), `custom_queries/transform_query.rs` (1) |
| `auth/jwt.ts` (89) | **MERGED** | `auth/jwt.rs` (5) |
| `auth/load-permissions.ts` (100) | **1:1** | `auth/load_permissions.rs` (3) |
| `auth/read-authorizer.ts` (152) | **1:1** | `auth/read_authorizer.rs` (5) |
| `config/zero-config.ts` (1299) | **MERGED** | `services/view_syncer/inspect_handler.rs` (1) |
| `custom-queries/transform-query.ts` (290) | **MERGED** | `custom_queries/transform_query.rs` (5), `auth/jwt.rs` (1) |
| `custom/fetch.ts` (569) | **SPLIT** | `custom/fetch.rs` (3), `custom/metrics.rs` (3), `custom_queries/transform_query.rs` (3), `protocol/error_reason_enum.rs` (1), `protocol/error.rs` (1) |
| `custom/metrics.ts` (93) | **MERGED** | `custom/metrics.rs` (3) |
| `db/lite-tables.ts` (356) | **1:1** | `db/lite_tables.rs` (5), `services/view_syncer/pipeline_driver.rs` (1) |
| `observability/metrics.ts` (239) | **MERGED** | `custom_queries/transform_query.rs` (1), `workers/syncer.rs` (1), `observability/metrics.rs` (1), `server/otel_start.rs` (1) |
| `server/otel-start.ts` (107) | **MERGED** | `server/otel_start.rs` (2) |
| `server/syncer.ts` (295) | **MERGED** | `custom_queries/transform_query.rs` (1) |
| `services/mutagen/pusher.ts` (712) | **1:1** | `services/mutagen/pusher.rs` (6), `live_count.rs` (1) |
| `services/view-syncer/connection-context-manager.ts` (892) | **MERGED** | `services/view_syncer/connection_context_manager.rs` (29) |
| `services/view-syncer/drain-coordinator.ts` (76) | **1:1** | `services/view_syncer/drain_coordinator.rs` (6) |
| `services/view-syncer/e2e-serving-lag.ts` (82) | **1:1** | `services/view_syncer/e2e_serving_lag.rs` (6) |
| `services/view-syncer/inspect-handler.ts` (215) | **MERGED** | `services/view_syncer/inspect_handler.rs` (1) |
| `services/view-syncer/pipeline-driver.ts` (1558) | **MERGED** | `services/view_syncer/pipeline_driver.rs` (12), `ws_sink.rs` (1), `services/view_syncer/connection_context_manager.rs` (1), `protocol/error.rs` (1), `observability/metrics.rs` (1) |
| `services/view-syncer/query-covering.ts` (444) | **1:1** | `services/view_syncer/query_covering.rs` (25) |
| `services/view-syncer/view-syncer.ts` (3002) | **1:1** | `services/view_syncer/view_syncer.rs` (21), `protocol/error.rs` (1) |
| `workers/connect-params.ts` (100) | **1:1** | `workers/connect_params.rs` (2), `ws_server.rs` (1) |
| `workers/connection.ts` (485) | **1:1** | `workers/connection.rs` (13), `ws_sink.rs` (1) |
| `workers/syncer-ws-message-handler.ts` (283) | **1:1** | `workers/syncer_ws_message_handler.rs` (3) |
| `workers/syncer.ts` (759) | **MERGED** | `workers/syncer.rs` (15), `ws_server.rs` (1) |

**New Rust files (no TS origin — added in the port):**  `auth.rs` (6), `config.rs` (4), `config/zero_config.rs` (311), `custom.rs` (5), `custom_queries.rs` (4), `db.rs` (3), `http_server.rs` (513), `lib.rs` (84), `main.rs` (474), `observability.rs` (4), `protocol.rs` (116), `protocol/change_desired_queries.rs` (14), `protocol/connect.rs` (106), `protocol/delete_clients.rs` (14), `protocol/down.rs` (11), `protocol/error_kind_enum.rs` (30), `protocol/error_origin_enum.rs` (14), `protocol/inspect_up.rs` (41), `protocol/mutation_id.rs` (12), `protocol/mutations_patch.rs` (18), `protocol/poke.rs` (47), `protocol/pong.rs` (15), `protocol/protocol_version.rs` (9), `protocol/push.rs` (31), `protocol/queries_patch.rs` (55), `protocol/row_patch.rs` (33), `protocol/up.rs` (95), `protocol/update_auth.rs` (10), `protocol/version.rs` (8), `server.rs` (9), `server/syncer.rs` (166), `services.rs` (4), `services/mutagen.rs` (4), `services/view_syncer.rs` (17), `trace.rs` (30), `workers.rs` (10), `workers/cg_executor.rs` (340)

**Merges (many TS → one Rust file):**
- `auth/jwt.rs` ⟵ `auth/jwt.ts`, `custom-queries/transform-query.ts`
- `custom/metrics.rs` ⟵ `custom/fetch.ts`, `custom/metrics.ts`
- `custom_queries/transform_query.rs` ⟵ `auth/auth.ts`, `custom-queries/transform-query.ts`, `custom/fetch.ts`, `observability/metrics.ts`, `server/syncer.ts`
- `observability/metrics.rs` ⟵ `observability/metrics.ts`, `services/view-syncer/pipeline-driver.ts`
- `protocol/error.rs` ⟵ `custom/fetch.ts`, `services/view-syncer/pipeline-driver.ts`, `services/view-syncer/view-syncer.ts`
- `server/otel_start.rs` ⟵ `observability/metrics.ts`, `server/otel-start.ts`
- `services/view_syncer/connection_context_manager.rs` ⟵ `auth/auth.ts`, `services/view-syncer/connection-context-manager.ts`, `services/view-syncer/pipeline-driver.ts`
- `services/view_syncer/inspect_handler.rs` ⟵ `config/zero-config.ts`, `services/view-syncer/inspect-handler.ts`
- `services/view_syncer/pipeline_driver.rs` ⟵ `db/lite-tables.ts`, `services/view-syncer/pipeline-driver.ts`
- `workers/syncer.rs` ⟵ `observability/metrics.ts`, `workers/syncer.ts`
- `ws_server.rs` ⟵ `workers/connect-params.ts`, `workers/syncer.ts`
- `ws_sink.rs` ⟵ `services/view-syncer/pipeline-driver.ts`, `workers/connection.ts`

## 2 · Per-file functional divergence

### `auth/jwt.rs`  ⟵  `auth/jwt.ts`, `custom-queries/transform-query.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `validate` (custom-queries/transform-query.ts:111) | `validate_auth` (:381) | fuzzy 0.50 |

🟥 **TS symbols not resolved into this file (1):** `tokenConfigOptions`

🟦 **Rust-only added here (18):** `CachedJwks`, `Claims`, `JWKS_CACHE`, `JWKS_HTTP`, `JWKS_REFETCH_COOLDOWN`, `JWKS_TTL`, `JwtAuthValidator`, `apply_claim_validation`, `decode_jwt_claims`, `has_config`, `key_algorithm_to_signature_alg`, `lookup_cached_jwk`, `lookup_stale_cached_jwk`, `select_jwk`, `verify_sync`, `verify_with_jwk`, `verify_with_jwks`, `within_refetch_cooldown`

### `auth/load_permissions.rs`  ⟵  `auth/load-permissions.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `LoadedPermissions` (auth/load-permissions.ts:15) | `LoadedPermissions` (:52) | exact |
| `loadPermissions` (auth/load-permissions.ts:20) | `load_permissions` (:58) | exact |
| `reloadPermissionsIfChanged` (auth/load-permissions.ts:64) | `reload_permissions_if_changed` (:352) | exact |

🟦 **Rust-only added here (10):** `OPS`, `PermissionsReload`, `deny_all_permissions`, `resolve_permissions`, `validate_condition_value`, `validate_permission_asset`, `validate_permission_condition`, `validate_permissions_config`, `validate_policy`, `validate_related_subquery`

### `auth/read_authorizer.rs`  ⟵  `auth/read-authorizer.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `addRulesToWhere` (auth/read-authorizer.ts:105) | `add_rules_to_where` (:120) | exact |
| `transformAndHashQuery` (auth/read-authorizer.ts:24) | `transform_and_hash_query` (:36) | exact |
| `transformCondition` (auth/read-authorizer.ts:127) | `transform_condition` (:131) | exact |
| `transformQuery` (auth/read-authorizer.ts:45) | `transform_query` (:52) | exact |
| `transformQueryInternal` (auth/read-authorizer.ts:61) | `transform_query_internal` (:59) | exact |

🟥 **TS symbols not resolved into this file (1):** `TransformedAndHashed`

🟦 **Rust-only added here (24):** `DIGITS`, `base36`, `bind_condition`, `bind_static_parameters`, `bind_value`, `bind_visit`, `cmp_condition`, `cmp_optional_bool`, `cmp_related`, `compare_utf8_maybe_null`, `compare_value_position`, `ctype`, `flatten`, `flattened`, `hash_of_ast`, `insert_if_present`, `is_always_false`, `is_always_true`, `js_string`, `normalize_ast`, `normalize_related_entry`, `normalize_where`, `resolve_field`, `simplify_condition`

### `config/zero_config.rs`  ⟵  _(new)_


🟦 **Rust-only added here (8):** `SyncerConfig`, `cgroup_cpu_quota_cores`, `from_env`, `host_parallelism`, `parse_cpu_max`, `parse_cpu_max_quota_shapes`, `parse_query_config`, `warn_if_quota_capped`

### `custom/fetch.rs`  ⟵  `custom/fetch.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `getBackoffDelayMs` (custom/fetch.ts:407) | `get_backoff_delay_ms` (:38) | exact |
| `getBodyPreview` (custom/fetch.ts:62) | `BODY_PREVIEW_CAP` (:49) | fuzzy 0.67 |
| `urlMatch` (custom/fetch.ts:389) | `url_match` (:11) | exact |

🟥 **TS symbols not resolved into this file (1):** `FetchMetricsOptions`

🟦 **Rust-only added here (1):** `read_body_preview`

### `custom/metrics.rs`  ⟵  `custom/fetch.ts`, `custom/metrics.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `apiAttemptDuration` (custom/fetch.ts:568) | `API_DURATION_BOUNDARIES_S` (:22) | fuzzy 0.50 |
| `apiInFlight` (custom/fetch.ts:116) | `record_api_in_flight` (:116) | fuzzy 0.75 |
| `apiRequestDuration` (custom/metrics.ts:63) | `record_api_request_duration` (:74) | fuzzy 0.75 |
| `ApiRequestMetricAttrs` (custom/metrics.ts:31) | `api_request_metric_attrs` (:59) | exact |
| `ApiRequestResult` (custom/metrics.ts:15) | `record_api_request` (:67) | fuzzy 0.50 |
| `recordApiAttempt` (custom/fetch.ts:549) | `record_api_attempt` (:84) | exact |

🟥 **TS symbols not resolved into this file (6):** `ApiAttemptMetricAttrs`, `ApiAttemptResult`, `ApiCleanupType`, `ApiMetricBaseAttrs`, `ApiOperation`, `apiRequests`

🟦 **Rust-only added here (2):** `ApiOtel`, `INSTRUMENTS`

### `custom_queries/transform_query.rs`  ⟵  `auth/auth.ts`, `custom-queries/transform-query.ts`, `custom/fetch.ts`, `observability/metrics.ts`, `server/syncer.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `cache` (observability/metrics.ts:42) | `cache_get` (:531) | fuzzy 1.00 |
| `CustomQueryTransformer` (custom-queries/transform-query.ts:82) | `CustomQueryContext` (:49) | fuzzy 0.50 |
| `getCacheKey` (custom-queries/transform-query.ts:259) | `get_cache_key` (:521) | exact |
| `getCustomQueryConfig` (server/syncer.ts:53) | `CustomQuerySpec` (:117) | fuzzy 0.50 |
| `isAuthErrorBody` (auth/auth.ts:211) | `is_auth_error_body` (:270) | exact |
| `normalizedHeaders` (custom-queries/transform-query.ts:278) | `normalized_headers` (:514) | exact |
| `transform` (custom-queries/transform-query.ts:117) | `post_transform` (:304) | fuzzy 0.50 |

🟥 **TS symbols not resolved into this file (14):** `Category`, `HashedTransformResponse`, `LONG_DURATION_HISTOGRAM_BOUNDARIES_S`, `NATIVE_HISTOGRAM_INSTRUMENT_NAMES`, `TransformResponse`, `getMeter`, `getOrCreateGauge`, `getOrCreateHistogram`, `getOrCreateLatencyHistogram`, `getOrCreateUpDownCounter`, `initEventSink`, `recordMs`, `registerSQLiteCorruptionDiagnosticTarget`, `startAnonymousTelemetry`

🟦 **Rust-only added here (15):** `CACHE_TTL`, `CustomTransformed`, `FETCH_MAX_ATTEMPTS`, `HTTP_CLIENT`, `RESERVED_PARAMS`, `TRANSFORM_CACHE`, `TransformedQuery`, `cache_set`, `composed_headers`, `extract_transform_queries`, `post_transform_attempts`, `seed_transform_cache_for_test`, `set_header`, `transform_custom_queries`, `validate_custom_queries`

### `db/lite_tables.rs`  ⟵  `db/lite-tables.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `computeZqlSpecs` (db/lite-tables.ts:210) | `compute_zql_specs` (:79) | exact |
| `computeZqlSpecsFromLiteSpecs` (db/lite-tables.ts:227) | `compute_table_specs_from_path` (:73) | fuzzy 0.43 |
| `listIndexes` (db/lite-tables.ts:141) | `list_unique_indexes` (:200) | fuzzy 0.67 |
| `listTables` (db/lite-tables.ts:47) | `list_tables` (:292) | exact |

🟥 **TS symbols not resolved into this file (2):** `LiteTableSpecWithReplicationStatus`, `ZqlSpecOptions`

🟦 **Rust-only added here (13):** `NOT_NULL_ATTRIBUTE`, `ReplicaVersions`, `TEXT_ARRAY_ATTRIBUTE`, `TEXT_ENUM_ATTRIBUTE`, `lite_table_name`, `lite_type_to_zql_value_type`, `open_replica_read_only`, `read_min_row_versions`, `read_replica_versions`, `read_replica_versions_from_path`, `read_table_spec`, `validate_client_schema`, `zql_type_for_upstream`

### `http_server.rs`  ⟵  _(new)_


🟦 **Rust-only added here (14):** `HttpServerState`, `ServerStats`, `bind_http_listener`, `census_handler`, `check_admin_auth`, `check_notify_request`, `heapz_handler`, `metrics_handler`, `notify_broadcast_handler`, `notify_handler`, `readyz_handler`, `run_http_server`, `serve_http`, `statz_handler`

### `live_count.rs`  ⟵  `services/mutagen/pusher.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `Pusher` (services/mutagen/pusher.ts:40) | `PUSHER` (:33) | exact |

🟦 **Rust-only added here (10):** `CLIENT_GROUP`, `Guard`, `SYNC_ENGINE`, `WS_MESSAGE_HANDLER`, `dec`, `drop`, `drop_backtrace`, `inc`, `new`, `snapshot`

### `main.rs`  ⟵  _(new)_


🟦 **Rust-only added here (3):** `ALLOC`, `ShutdownSignal`, `main`

### `observability/metrics.rs`  ⟵  `observability/metrics.ts`, `services/view-syncer/pipeline-driver.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `LatencyHistogram` (observability/metrics.ts:91) | `Histogram` (:659) | fuzzy 0.50 |
| `reset` (services/view-syncer/pipeline-driver.ts:343) | `record_reset` (:779) | fuzzy 0.50 |

🟦 **Rust-only added here (60):** `C`, `CvrAttemptOtel`, `G`, `GAUGES`, `HIST_BOUNDS_SECS`, `I`, `INSTRUMENT`, `Metrics`, `OTEL_LATENCY_BOUNDARIES_S`, `Otel`, `QueryTransformOtel`, `ServingLagOtel`, `WS_QUEUED_BYTES`, `WS_QUEUED_FRAMES`, `active_clients`, `cvr_flush_failures`, `default`, `failed_client_groups`, `fmt`, `now_ms`, `observe_millis`, `observe_secs`, `proto_attr`, `record_active_client_delta`, `record_advance`, `record_cvr_flush_attempt`, `record_cvr_flush_failure`, `record_cvr_load_attempt`, `record_e2e_serving_lag`, `record_e2e_serving_lag_clamp`, `record_fail_group`, `record_hydration`, `record_query_transformation`, `record_query_transformation_hash_change`, `record_query_transformation_no_op`, `record_query_transformation_time`, `record_view_syncer_hydration`, `record_view_syncer_lag_ms`, `record_websocket_error`, `record_ws_connection_attempt`, `record_ws_connection_failure`, `record_ws_connection_success`, `record_ws_open_delta`, `record_ws_queued_bytes_delta`, `record_ws_queued_delta`, `record_ws_shed`, `register_cvr_pool_gauges`, `register_serving_lag_gauges`, `render`, `render_prometheus`, `view_syncer_hydration_otel`, `view_syncer_lag_otel`, `ws_connection_attempts`, `ws_connection_failures`, `ws_connection_successes`, `ws_errors`, `ws_open_connections`, `ws_queued_bytes_gauge`, `ws_queued_frames_gauge`, `ws_sheds`

### `protocol/change_desired_queries.rs`  ⟵  _(new)_


🟦 **Rust-only added here (1):** `ChangeDesiredQueriesBody`

### `protocol/connect.rs`  ⟵  _(new)_


🟦 **Rust-only added here (7):** `ConnectedBody`, `DecodeError`, `InitConnectionBody`, `InitConnectionMessage`, `SecProtocols`, `connected_message`, `decode_sec_protocols`

### `protocol/delete_clients.rs`  ⟵  _(new)_


🟦 **Rust-only added here (1):** `DeleteClientsBody`

### `protocol/down.rs`  ⟵  _(new)_


🟦 **Rust-only added here (1):** `downstream_message`

### `protocol/error.rs`  ⟵  `custom/fetch.ts`, `services/view-syncer/pipeline-driver.ts`, `services/view-syncer/view-syncer.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `apiFailedBody` (custom/fetch.ts:411) | `PushFailedHttpBody` (:44) | fuzzy 0.40 |
| `hydrateInternal` (services/view-syncer/pipeline-driver.ts:1505) | `internal` (:190) | fuzzy 0.50 |
| `isTransformFailedError` (services/view-syncer/view-syncer.ts:2897) | `TransformFailedHttpBody` (:82) | fuzzy 0.40 |

🟦 **Rust-only added here (17):** `BackoffBody`, `BasicErrorBody`, `ErrorBody`, `PushFailedServerBody`, `PushFailedZeroCacheBody`, `TransformFailedServerBody`, `TransformFailedZeroCacheBody`, `basic`, `client_not_found`, `error_message`, `invalid_message`, `invalid_push`, `kind`, `message`, `rehome`, `unauthorized`, `version_not_supported`

### `protocol/error_kind_enum.rs`  ⟵  _(new)_


🟦 **Rust-only added here (1):** `ErrorKind`

### `protocol/error_origin_enum.rs`  ⟵  _(new)_


🟦 **Rust-only added here (1):** `ErrorOrigin`

### `protocol/error_reason_enum.rs`  ⟵  `custom/fetch.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `legacyPushErrorReason` (custom/fetch.ts:484) | `ErrorReason` (:9) | fuzzy 0.50 |

### `protocol/inspect_up.rs`  ⟵  _(new)_


🟦 **Rust-only added here (2):** `AnalyzeQueryOptions`, `InspectUpBody`

### `protocol/mutation_id.rs`  ⟵  _(new)_


🟦 **Rust-only added here (1):** `MutationID`

### `protocol/mutations_patch.rs`  ⟵  _(new)_


🟦 **Rust-only added here (2):** `MutationPatchOp`, `MutationsPatch`

### `protocol/poke.rs`  ⟵  _(new)_


🟦 **Rust-only added here (4):** `PokeEndBody`, `PokePartBody`, `PokeStartBody`, `SchemaVersions`

### `protocol/pong.rs`  ⟵  _(new)_


🟦 **Rust-only added here (2):** `PongBody`, `pong_message`

### `protocol/protocol_version.rs`  ⟵  _(new)_


🟦 **Rust-only added here (2):** `MIN_SERVER_SUPPORTED_SYNC_PROTOCOL`, `PROTOCOL_VERSION`

### `protocol/push.rs`  ⟵  _(new)_


🟦 **Rust-only added here (2):** `AckMutationResponsesBody`, `PushBody`

### `protocol/queries_patch.rs`  ⟵  _(new)_


🟦 **Rust-only added here (6):** `QueriesClearOp`, `QueriesDelOp`, `QueriesPatch`, `QueriesPatchOp`, `QueriesPutOp`, `UpQueriesPatch`

### `protocol/row_patch.rs`  ⟵  _(new)_


🟦 **Rust-only added here (2):** `RowPatchOp`, `RowsPatch`

### `protocol/up.rs`  ⟵  _(new)_


🟦 **Rust-only added here (3):** `Upstream`, `parse_upstream`, `parse_upstream_array`

### `protocol/update_auth.rs`  ⟵  _(new)_


🟦 **Rust-only added here (1):** `UpdateAuthBody`

### `protocol/version.rs`  ⟵  _(new)_


🟦 **Rust-only added here (2):** `NullableVersion`, `Version`

### `server/otel_start.rs`  ⟵  `observability/metrics.ts`, `server/otel-start.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `getOrCreateNativeHistogram` (observability/metrics.ts:147) | `NATIVE_HISTOGRAM_INSTRUMENTS` (:99) | fuzzy 0.40 |

🟦 **Rust-only added here (2):** `init_metrics`, `metrics_enabled`

### `server/syncer.rs`  ⟵  _(new)_


🟦 **Rust-only added here (4):** `RealServicesFactory`, `create_mutagen`, `create_pusher`, `create_sync_engine_config`

### `services/mutagen/pusher.rs`  ⟵  `services/mutagen/pusher.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `ackMutationResponses` (services/mutagen/pusher.ts:43) | `ack_mutation_responses` (:517) | exact |
| `combinePushes` (services/mutagen/pusher.ts:626) | `combine_pushes` (:164) | exact |
| `deleteClientMutations` (services/mutagen/pusher.ts:47) | `delete_client_mutations` (:561) | exact |
| `enqueuePush` (services/mutagen/pusher.ts:42) | `enqueue_push` (:455) | exact |
| `initConnection` (services/mutagen/pusher.ts:41) | `init_connection` (:515) | exact |
| `PusherService` (services/mutagen/pusher.ts:68) | `PusherService` (:211) | exact |

🟥 **TS symbols not resolved into this file (4):** `assertAreCompatiblePushes`, `hasRefs`, `ref`, `unref`

🟦 **Rust-only added here (12):** `CLEANUP_RESULTS_MUTATION_NAME`, `DEFAULT_QUEUE_CAP`, `PushTarget`, `QueuedPush`, `RELAY_TIMEOUT`, `cleanup_push_body`, `combine_key_of`, `enqueue_payload`, `mutation_ids_of`, `queue_cap`, `relay_body`, `set_auth_fail_hook`

### `services/view_syncer/connection_context_manager.rs`  ⟵  `auth/auth.ts`, `services/view-syncer/connection-context-manager.ts`, `services/view-syncer/pipeline-driver.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `Auth` (auth/auth.ts:25) | `Auth` (:90) | exact |
| `authEquals` (auth/auth.ts:36) | `auth_equals` (:350) | exact |
| `closeConnection` (services/view-syncer/connection-context-manager.ts:136) | `close_connection` (:682) | exact |
| `compareByInsertionOrder` (services/view-syncer/connection-context-manager.ts:844) | `compare_by_insertion_order` (:968) | exact |
| `comparePreferredValidatedConnection` (services/view-syncer/connection-context-manager.ts:851) | `compare_preferred_validated_connection` (:975) | exact |
| `ConnectionContext` (services/view-syncer/connection-context-manager.ts:65) | `ConnectionContext` (:103) | exact |
| `ConnectionContextManager` (services/view-syncer/connection-context-manager.ts:104) | `ConnectionContextManager` (:404) | exact |
| `ConnectionFetchContext` (services/view-syncer/connection-context-manager.ts:54) | `ConnectionFetchContext` (:81) | exact |
| `ConnectionSelector` (services/view-syncer/connection-context-manager.ts:37) | `ConnectionSelector` (:56) | exact |
| `ConnectionState` (services/view-syncer/connection-context-manager.ts:17) | `ConnectionState` (:36) | exact |
| `ConnectionValidation` (services/view-syncer/connection-context-manager.ts:30) | `ConnectionValidation` (:49) | exact |
| `deferMaintenance` (services/view-syncer/connection-context-manager.ts:147) | `defer_maintenance` (:718) | exact |
| `failConnection` (services/view-syncer/connection-context-manager.ts:132) | `fail_connection` (:672) | exact |
| `fetch` (services/view-syncer/pipeline-driver.ts:1428) | `FetchConfig` (:146) | fuzzy 0.50 |
| `filterHeaders` (services/view-syncer/connection-context-manager.ts:875) | `filter_headers` (:372) | exact |
| `getBackgroundConnectionContext` (services/view-syncer/connection-context-manager.ts:156) | `get_background_connection_context` (:756) | exact |
| `getConnectionContext` (services/view-syncer/connection-context-manager.ts:149) | `get_connection_context` (:734) | exact |
| `getGroupState` (services/view-syncer/connection-context-manager.ts:159) | `get_group_state` (:769) | exact |
| `GroupAuthState` (services/view-syncer/connection-context-manager.ts:95) | `GroupAuthState` (:121) | exact |
| `HeaderOptions` (services/view-syncer/connection-context-manager.ts:44) | `HeaderOptions` (:70) | exact |
| `initConnection` (services/view-syncer/connection-context-manager.ts:111) | `init_connection` (:524) | exact |
| `markBackgroundRetransformSuccess` (services/view-syncer/connection-context-manager.ts:140) | `mark_background_retransform_success` (:688) | exact |
| `minDefined` (services/view-syncer/connection-context-manager.ts:858) | `min_defined` (:959) | exact |
| `mustGetBackgroundConnectionContext` (services/view-syncer/connection-context-manager.ts:157) | `must_get_background_connection_context` (:761) | exact |
| `mustGetConnectionContext` (services/view-syncer/connection-context-manager.ts:152) | `must_get_connection_context` (:745) | exact |
| `pickToken` (auth/auth.ts:126) | `pick_token` (:284) | exact |
| `planMaintenance` (services/view-syncer/connection-context-manager.ts:161) | `plan_maintenance` (:775) | exact |
| `registerConnection` (services/view-syncer/connection-context-manager.ts:105) | `register_connection` (:446) | exact |
| `resolveAuth` (auth/auth.ts:49) | `resolve_auth` (:230) | exact |
| `setSharedRetransformReady` (services/view-syncer/connection-context-manager.ts:145) | `set_shared_retransform_ready` (:708) | exact |
| `updateAuth` (services/view-syncer/connection-context-manager.ts:116) | `update_auth` (:573) | exact |
| `UserState` (services/view-syncer/connection-context-manager.ts:23) | `UserState` (:43) | exact |
| `validateConnection` (services/view-syncer/connection-context-manager.ts:121) | `validate_connection` (:608) | exact |
| `ValidateLegacyJWT` (auth/auth.ts:27) | `LegacyJwtValidator` (:224) | fuzzy 0.50 |

🟥 **TS symbols not resolved into this file (3):** `ConnectionContextManagerImpl`, `JWTAuth`, `OpaqueAuth`

🟦 **Rust-only added here (17):** `CCMError`, `ConnectParamsForRegistration`, `JwtPayload`, `MaintenanceKind`, `MaintenancePlan`, `ValidationResult`, `build_fetch_context`, `demote_connection`, `next_revalidate_at`, `now`, `raw`, `refresh_background_connection_context`, `remove_connection_internal`, `set_background_connection`, `store_connection`, `to_error_body`, `update_background_retransform_deadline`

### `services/view_syncer/drain_coordinator.rs`  ⟵  `services/view-syncer/drain-coordinator.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `DrainCoordinator` (services/view-syncer/drain-coordinator.ts:31) | `DrainCoordinator` (:39) | exact |
| `draining` (services/view-syncer/drain-coordinator.ts:37) | `is_draining` (:112) | fuzzy 1.00 |
| `drainNextIn` (services/view-syncer/drain-coordinator.ts:45) | `drain_next_in` (:66) | exact |
| `forceDrainTimeout` (services/view-syncer/drain-coordinator.ts:66) | `force_drain_timeout` (:92) | exact |
| `nextDrainTime` (services/view-syncer/drain-coordinator.ts:71) | `next_drain_time` (:117) | exact |
| `shouldDrain` (services/view-syncer/drain-coordinator.ts:41) | `should_drain` (:57) | exact |

🟦 **Rust-only added here (2):** `FORCE_DRAIN_PADDING`, `TARGET_UTILIZATION`

### `services/view_syncer/e2e_serving_lag.rs`  ⟵  `services/view-syncer/e2e-serving-lag.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `E2EServingLagTracker` (services/view-syncer/e2e-serving-lag.ts:19) | `E2EServingLagTracker` (:29) | exact |
| `Observation` (services/view-syncer/e2e-serving-lag.ts:77) | `Observation` (:21) | exact |
| `onVersionReady` (services/view-syncer/e2e-serving-lag.ts:35) | `on_version_ready` (:50) | exact |
| `onVersionServed` (services/view-syncer/e2e-serving-lag.ts:55) | `on_version_served` (:72) | exact |
| `pending` (services/view-syncer/e2e-serving-lag.ts:22) | `pending` (:38) | exact |
| `PendingUpstreamCommit` (services/view-syncer/e2e-serving-lag.ts:3) | `PendingUpstreamCommit` (:14) | exact |

### `services/view_syncer/inspect_handler.rs`  ⟵  `config/zero-config.ts`, `services/view-syncer/inspect-handler.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `handleInspect` (services/view-syncer/inspect-handler.ts:25) | `handle_inspect` (:41) | exact |
| `isAdminPasswordValid` (config/zero-config.ts:1242) | `is_admin_password_valid` (:23) | exact |

🟥 **TS symbols not resolved into this file (16):** `AuthConfig`, `LegacyJWTAuthConfig`, `RateLimit`, `ReplicaOptions`, `ZERO_ENV_VAR_PREFIX`, `ZeroConfig`, `appOptions`, `assertNormalized`, `getNormalizedZeroConfig`, `getServerVersion`, `getZeroConfig`, `metricsForProtocol`, `resetWarnOnceState`, `shardOptions`, `warnOnce`, `zeroOptions`

🟦 **Rust-only added here (1):** `inspect_queries_value`

### `services/view_syncer/pipeline_driver.rs`  ⟵  `db/lite-tables.ts`, `services/view-syncer/pipeline-driver.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `advance` (services/view-syncer/pipeline-driver.ts:923) | `advance` (:500) | exact |
| `buildPrimaryKeys` (services/view-syncer/pipeline-driver.ts:1520) | `set_client_primary_keys` (:416) | fuzzy 0.50 |
| `currentVersion` (services/view-syncer/pipeline-driver.ts:395) | `current_version` (:221) | exact |
| `destroy` (services/view-syncer/pipeline-driver.ts:447) | `destroy` (:597) | exact |
| `getRow` (services/view-syncer/pipeline-driver.ts:906) | `get_row` (:578) | exact |
| `hydrate` (services/view-syncer/pipeline-driver.ts:1491) | `hydrate` (:441) | exact |
| `init` (services/view-syncer/pipeline-driver.ts:325) | `init` (:233) | exact |
| `initialized` (services/view-syncer/pipeline-driver.ts:334) | `initialized` (:216) | exact |
| `mustGetTableSpec` (db/lite-tables.ts:326) | `IvmTableSpec` (:49) | fuzzy 0.50 |
| `removeQuery` (services/view-syncer/pipeline-driver.ts:834) | `remove_query` (:424) | exact |

🟥 **TS symbols not resolved into this file (9):** `PipelineDriver`, `PipelineHydrationReason`, `RowAdd`, `RowChange`, `RowEdit`, `RowRemove`, `Timer`, `accumulate`, `rowSetSignature`

🟦 **Rust-only added here (27):** `AdvanceOutcome`, `IvmColumnSchema`, `IvmPipelines`, `TsAst`, `TsBound`, `TsCondition`, `TsCorrelatedSubquery`, `TsCorrelation`, `TsValuePosition`, `active_query_ids`, `build_engine`, `column_schema`, `column_type`, `convert_ast`, `convert_condition`, `convert_csq`, `convert_value_position`, `has_query`, `init_from_connection`, `json_to_value`, `panic_message`, `parse_ts_ast`, `query_transformation_hash`, `running_queries`, `scalar_reset_message`, `set_query_transformation_hash`, `zql_column_type`

### `services/view_syncer/query_covering.rs`  ⟵  `services/view-syncer/query-covering.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `add` (services/view-syncer/query-covering.ts:67) | `add` (:141) | exact |
| `astCoveredBy` (services/view-syncer/query-covering.ts:129) | `ast_covered_by` (:202) | exact |
| `boundsCoveredBy` (services/view-syncer/query-covering.ts:143) | `bounds_covered_by` (:217) | exact |
| `columnLiteralParts` (services/view-syncer/query-covering.ts:399) | `ColumnLiteralParts` (:409) | exact |
| `conditionEquivalent` (services/view-syncer/query-covering.ts:192) | `condition_equivalent` (:259) | exact |
| `conditionImplies` (services/view-syncer/query-covering.ts:199) | `condition_implies` (:263) | exact |
| `correlatedConditionImplies` (services/view-syncer/query-covering.ts:237) | `correlated_condition_implies` (:303) | exact |
| `CoveringQuery` (services/view-syncer/query-covering.ts:21) | `CoveringQuery` (:33) | exact |
| `equalityImplies` (services/view-syncer/query-covering.ts:315) | `equality_implies` (:371) | exact |
| `findCoveringQuery` (services/view-syncer/query-covering.ts:44) | `find_covering_query` (:106) | exact |
| `isEqualityOp` (services/view-syncer/query-covering.ts:416) | `is_equality_op` (:430) | exact |
| `isNonNullScalarLiteralValue` (services/view-syncer/query-covering.ts:426) | `is_non_null_scalar_literal_value` (:438) | exact |
| `isNumericOrderOp` (services/view-syncer/query-covering.ts:420) | `is_numeric_order_op` (:434) | exact |
| `isQueryCoveredBy` (services/view-syncer/query-covering.ts:40) | `is_query_covered_by` (:97) | exact |
| `jsonEqual` (services/view-syncer/query-covering.ts:439) | `json_equal` (:462) | exact |
| `literalArrayIncludes` (services/view-syncer/query-covering.ts:432) | `literal_array_includes` (:442) | exact |
| `orderConditionImplies` (services/view-syncer/query-covering.ts:365) | `order_condition_implies` (:387) | exact |
| `QueryCoveringIndex` (services/view-syncer/query-covering.ts:55) | `QueryCoveringIndex` (:120) | exact |
| `relatedCoveredBy` (services/view-syncer/query-covering.ts:170) | `related_covered_by` (:242) | exact |
| `remove` (services/view-syncer/query-covering.ts:81) | `remove` (:158) | exact |
| `rootKey` (services/view-syncer/query-covering.ts:125) | `root_key` (:193) | exact |
| `RunningQuery` (services/view-syncer/query-covering.ts:15) | `RunningQuery` (:24) | exact |
| `sameRelatedEdge` (services/view-syncer/query-covering.ts:262) | `same_related_edge` (:326) | exact |
| `simpleConditionImplies` (services/view-syncer/query-covering.ts:274) | `simple_condition_implies` (:333) | exact |

🟦 **Rust-only added here (11):** `IndexedRunningQuery`, `QueryCoverageShadowHit`, `cmp_num`, `conditions`, `field_eq`, `json_eq`, `log_shadow_summary`, `num`, `present`, `related_of`, `subquery`

### `services/view_syncer/view_syncer.rs`  ⟵  `services/view-syncer/view-syncer.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `changeDesiredQueries` (services/view-syncer/view-syncer.ts:138) | `change_desired_queries` (:545) | exact |
| `checkClientAndCVRVersions` (services/view-syncer/view-syncer.ts:2875) | `check_client_and_cvr_versions` (:90) | exact |
| `deleteClients` (services/view-syncer/view-syncer.ts:143) | `delete_clients` (:574) | exact |
| `initConnection` (services/view-syncer/view-syncer.ts:133) | `init_connection` (:484) | exact |
| `inspect` (services/view-syncer/view-syncer.ts:148) | `inspect` (:603) | exact |
| `queryCount` (services/view-syncer/view-syncer.ts:658) | `query_count` (:1002) | exact |
| `rowCount` (services/view-syncer/view-syncer.ts:662) | `row_count` (:1008) | exact |
| `servedVersion` (services/view-syncer/view-syncer.ts:666) | `mark_version_served` (:2432) | fuzzy 0.67 |
| `servingLagEligible` (services/view-syncer/view-syncer.ts:670) | `serving_lag_eligible` (:996) | exact |
| `TTL_CLOCK_INTERVAL` (services/view-syncer/view-syncer.ts:202) | `TTL_CLOCK_INTERVAL` (:80) | exact |
| `TTL_TIMER_HYSTERESIS` (services/view-syncer/view-syncer.ts:210) | `TTL_TIMER_HYSTERESIS_MS` (:77) | fuzzy 0.75 |
| `updateAuth` (services/view-syncer/view-syncer.ts:149) | `update_auth` (:515) | exact |
| `ViewSyncer` (services/view-syncer/view-syncer.ts:132) | `CgViewSyncer` (:527) | fuzzy 0.67 |
| `ViewSyncerService` (services/view-syncer/view-syncer.ts:214) | `ViewSyncerService` (:617) | exact |

🟥 **TS symbols not resolved into this file (3):** `SyncContext`, `TimeSliceTimer`, `hasExpiredQueries`

🟦 **Rust-only added here (54):** `AuthValidator`, `CGServicesFactory`, `CG_KEEPALIVE_MS`, `CcmDispatchAdapter`, `CvrPgConfig`, `MAX_TTL_MS`, `SyncEngineConfig`, `T`, `apply_client_deletions`, `arm_auth_maintenance`, `arm_serving_lag`, `cg_event_loop`, `clients_to_delete`, `custom_query_context_from`, `decrement_active_client`, `decrement_nonzero`, `dispatch_cg_message`, `ensure_cvr`, `fail_group`, `fail_group_with_error`, `get_ttl_clock`, `handle_desired_queries`, `handle_update_auth`, `idle_shutdown_due`, `lock_unpoisoned`, `maybe_reload_permissions`, `merge_notifications`, `new_test`, `new_with_accepting`, `next_auth_maintenance_delay`, `next_expiry_delay`, `next_idle_shutdown_delay`, `next_ttl_clock_delay`, `older_replica_error`, `on_auth_maintenance_tick`, `on_connection_closed`, `on_expiry_tick`, `on_inbound`, `on_new_connection`, `on_notification`, `parse_desired_queries_patch`, `publish_serving_lag`, `query_context_for`, `reset_pipelines_and_rehydrate`, `schedule_expire_eviction`, `second_element`, `shard_for`, `shutdown`, `slow_hydrate_threshold_ms`, `start_ttl_clock_interval`, `stop_expire_timer`, `stop_ttl_clock_interval`, `str_array`, `update_ttl_clock_in_cvr_without_lock`

### `trace.rs`  ⟵  _(new)_


🟦 **Rust-only added here (2):** `ENABLED`, `note`

### `workers/cg_executor.rs`  ⟵  _(new)_


🟦 **Rust-only added here (11):** `CGHandle`, `CGMessage`, `CgMapCleanup`, `CgTaskContext`, `Executor`, `ExecutorCommand`, `connection_count`, `default_num_shards`, `executor_loop`, `forward_inbound`, `run_executor`

### `workers/connect_params.rs`  ⟵  `workers/connect-params.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `ConnectParams` (workers/connect-params.ts:9) | `ConnectParams` (:10) | exact |
| `getConnectParams` (workers/connect-params.ts:45) | `get_connect_params` (:63) | exact |

🟦 **Rust-only added here (7):** `ConnectParamsError`, `extract_protocol_version`, `get_boolean`, `get_integer`, `get_string`, `parse_js_integer`, `query_params_first_wins`

### `workers/connection.rs`  ⟵  `workers/connection.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `close` (workers/connection.ts:168) | `close` (:232) | exact |
| `Connection` (workers/connection.ts:78) | `Connection` (:59) | exact |
| `handleInitConnection` (workers/connection.ts:190) | `handle_init_connection` (:292) | exact |
| `handleMessage` (workers/connection.ts:52) | `handle_message` (:43) | exact |
| `HandlerResult` (workers/connection.ts:31) | `HandlerResult` (:23) | exact |
| `hasTransientSocketCode` (workers/connection.ts:466) | `has_transient_socket_code` (:342) | exact |
| `init` (workers/connection.ts:138) | `init` (:125) | exact |
| `isTransientSocketMessage` (workers/connection.ts:477) | `is_transient_socket_message` (:349) | exact |
| `MessageHandler` (workers/connection.ts:51) | `MessageHandler` (:40) | exact |
| `send` (workers/connection.ts:348) | `send` (:249) | exact |
| `sendError` (workers/connection.ts:356) | `send_error` (:257) | exact |

🟥 **TS symbols not resolved into this file (1):** `StreamResult`

🟦 **Rust-only added here (13):** `LogLevel`, `TRANSIENT_SOCKET_ERROR_CODES`, `TRANSIENT_SOCKET_MESSAGE_PATTERNS`, `WsState`, `classify_error_log_level`, `client_id`, `close_with_error`, `handle_close`, `handle_error`, `handle_inbound`, `handle_result`, `is_closed`, `ws_id`

### `workers/syncer.rs`  ⟵  `observability/metrics.ts`, `workers/syncer.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `boundReplicaReadyStates` (workers/syncer.ts:82) | `bound_replica_ready_states` (:83) | exact |
| `computeMaxServingLagMs` (workers/syncer.ts:247) | `compute_max_serving_lag_ms` (:232) | exact |
| `computeServingLagDistributionMs` (workers/syncer.ts:174) | `compute_serving_lag_distribution_ms` (:175) | exact |
| `computeServingLagStatsMs` (workers/syncer.ts:226) | `compute_serving_lag_stats_ms` (:223) | exact |
| `drain` (workers/syncer.ts:732) | `drain` (:1229) | exact |
| `findFirstUnservedIndex` (workers/syncer.ts:138) | `find_first_unserved_index` (:141) | exact |
| `getOrCreateCounter` (observability/metrics.ts:193) | `get_or_create_cg` (:1023) | fuzzy 0.50 |
| `lowerBoundReplicaReadyTimeMs` (workers/syncer.ts:104) | `lower_bound_replica_ready_time_ms` (:105) | exact |
| `MAX_REPLICA_READY_STATES` (workers/syncer.ts:76) | `MAX_REPLICA_READY_STATES` (:19) | exact |
| `percentileNearestRank` (workers/syncer.ts:160) | `percentile_nearest_rank` (:160) | exact |
| `pruneReplicaReadyStates` (workers/syncer.ts:93) | `prune_replica_ready_states` (:92) | exact |
| `ReplicaReadyState` (workers/syncer.ts:52) | `ReplicaReadyState` (:26) | exact |
| `ServingLagStats` (workers/syncer.ts:62) | `ServingLagStats` (:43) | exact |
| `ServingLagViewSyncer` (workers/syncer.ts:57) | `ServingLagViewSyncer` (:35) | exact |
| `Syncer` (workers/syncer.ts:288) | `Syncer` (:574) | exact |
| `upperBoundWatermark` (workers/syncer.ts:121) | `upper_bound_watermark` (:124) | exact |

🟥 **TS symbols not resolved into this file (1):** `SyncerWorkerData`

🟦 **Rust-only added here (28):** `CgServingSnapshot`, `ConnectionInfo`, `ConnectionSinks`, `DISTRIBUTION_CACHE_TTL_MS`, `MAX_DRAIN_MS`, `ServingLagDistribution`, `ServingLagRegistry`, `VIEW_SYNCER_LAG_SAMPLE_INTERVAL_MS`, `active_client_groups`, `broadcast_notification`, `cg_count`, `check_and_pin_user`, `compute_serving_lag_distribution`, `create_connection`, `insert_for_test`, `metrics_prometheus`, `metrics_snapshot`, `new_sharded`, `new_with_limit`, `place_cg`, `record_replica_ready_state`, `remove_view_syncer`, `send_error_if_current`, `send_notification`, `stats`, `total_queries`, `total_rows`, `upsert_view_syncer`

### `workers/syncer_ws_message_handler.rs`  ⟵  `workers/syncer-ws-message-handler.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `handleMessage` (workers/syncer-ws-message-handler.ts:72) | `handle_message` (:279) | exact |
| `SyncerWsMessageHandler` (workers/syncer-ws-message-handler.ts:36) | `SyncerWsMessageHandler` (:212) | exact |
| `withTraceparent` (workers/syncer-ws-message-handler.ts:28) | `with_traceparent` (:26) | exact |

🟦 **Rust-only added here (11):** `AuthFailHook`, `ConnContextInfo`, `ConnContextManagerDispatch`, `MutagenDispatch`, `PushOverride`, `PushRelayHeaders`, `PusherDispatch`, `ViewSyncerDispatch`, `handle_push`, `process_mutation`, `relay_headers_for`

### `ws_server.rs`  ⟵  `workers/connect-params.ts`, `workers/syncer.ts`


🟦 **Rust-only added here (23):** `DEFAULT_DOWNSTREAM_BYTE_HWM`, `DEFAULT_DOWNSTREAM_QUEUE_HWM`, `DEFAULT_LIVENESS_TIMEOUT_MS`, `DEFAULT_MAX_PAYLOAD_BYTES`, `DOWNSTREAM_MSG_INTERVAL_MS`, `KEEPALIVE_CHECK_INTERVAL_MS`, `NODE_SINGLETON_HEADERS`, `WsServerConfig`, `accept_connection`, `accept_connection_with_limit`, `bind_ws_listener`, `downstream_byte_hwm`, `downstream_queue_hwm`, `drain_until_peer_close`, `is_expected_disconnect`, `liveness_timeout_ms`, `now_epoch_ms`, `run_ws_reader`, `run_ws_server`, `run_ws_writer`, `send_error_and_close`, `serve_ws`, `serve_ws_with_config`

### `ws_sink.rs`  ⟵  `services/view-syncer/pipeline-driver.ts`, `workers/connection.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `push` (services/view-syncer/pipeline-driver.ts:1432) | `push` (:103) | exact |
| `WebSocketLike` (workers/connection.ts:361) | `DirectWebSocketSink` (:80) | fuzzy 0.40 |

🟦 **Rust-only added here (9):** `SinkLimits`, `WsCommand`, `cancel`, `close_with_code`, `count_shed_once`, `fail`, `push_sized`, `send_command`, `with_limits`

## 3 · Flat one-to-one symbol map (every TS symbol resolved)

| TS symbol | origin | → Rust | status |
|---|---|---|---|
| `JWTAuth` | auth/auth.ts:14 | — | 🟥 UNRESOLVED |
| `OpaqueAuth` | auth/auth.ts:20 | — | 🟥 UNRESOLVED |
| `Auth` | auth/auth.ts:25 | `Auth` services/view_syncer/connection_context_manager.rs:90 | ✅ exact |
| `ValidateLegacyJWT` | auth/auth.ts:27 | `LegacyJwtValidator` services/view_syncer/connection_context_manager.rs:224 | 🔁 rename 0.50 |
| `isProvidedAuth` | auth/auth.ts:32 | services/view_syncer/connection_context_manager.rs is_some_and non-empty | 📌 inlined |
| `authEquals` | auth/auth.ts:36 | `auth_equals` services/view_syncer/connection_context_manager.rs:350 | ✅ exact |
| `resolveAuth` | auth/auth.ts:49 | `resolve_auth` services/view_syncer/connection_context_manager.rs:230 | ✅ exact |
| `pickToken` | auth/auth.ts:126 | `pick_token` services/view_syncer/connection_context_manager.rs:284 | ✅ exact |
| `isAuthErrorBody` | auth/auth.ts:211 | `is_auth_error_body` custom_queries/transform_query.rs:270 | ✅ exact |
| `createJwkPair` | auth/jwt.ts:14 | N/A — JWK-pair GENERATION helper (tests/tooling) | 📌 rust only verifies tokens, never mints keys |
| `getRemoteKeyset` | auth/jwt.ts:32 | auth/jwt.rs JWKS_CACHE/lookup_cached_jwk | 📌 cached remote JWKS |
| `tokenConfigOptions` | auth/jwt.ts:41 | — | 🟥 UNRESOLVED |
| `verifyToken` | auth/jwt.ts:50 | auth/jwt.rs verify_with_jwks / verify_sync cluster | 📌 name-diverged verify path; 1:1 rename pending #163 |
| `loadJwk` | auth/jwt.ts:73 | auth/jwt.rs serde_json::from_str | 📌 parse JWK |
| `loadSecret` | auth/jwt.ts:77 | auth/jwt.rs DecodingKey::from_secret | 📌 secret key |
| `verifyTokenImpl` | auth/jwt.ts:81 | auth/jwt.rs verify_sync/verify_with_jwk(s) | 📌 JWT verify (split sync/async) |
| `LoadedPermissions` | auth/load-permissions.ts:15 | `LoadedPermissions` auth/load_permissions.rs:52 | ✅ exact |
| `loadPermissions` | auth/load-permissions.ts:20 | `load_permissions` auth/load_permissions.rs:58 | ✅ exact |
| `reloadPermissionsIfChanged` | auth/load-permissions.ts:64 | `reload_permissions_if_changed` auth/load_permissions.rs:352 | ✅ exact |
| `getSchema` | auth/load-permissions.ts:83 | rust-ivm operator get_schema | 📌 trait method (cross-crate) |
| `TransformedAndHashed` | auth/read-authorizer.ts:10 | — | 🟥 UNRESOLVED |
| `transformAndHashQuery` | auth/read-authorizer.ts:24 | `transform_and_hash_query` auth/read_authorizer.rs:36 | ✅ exact |
| `transformQuery` | auth/read-authorizer.ts:45 | `transform_query` auth/read_authorizer.rs:52 | ✅ exact |
| `transformQueryInternal` | auth/read-authorizer.ts:61 | `transform_query_internal` auth/read_authorizer.rs:59 | ✅ exact |
| `addRulesToWhere` | auth/read-authorizer.ts:105 | `add_rules_to_where` auth/read_authorizer.rs:120 | ✅ exact |
| `transformCondition` | auth/read-authorizer.ts:127 | `transform_condition` auth/read_authorizer.rs:131 | ✅ exact |
| `ZERO_ENV_VAR_PREFIX` | config/zero-config.ts:31 | — | 🟥 UNRESOLVED |
| `appOptions` | config/zero-config.ts:33 | — | 🟥 UNRESOLVED |
| `shardOptions` | config/zero-config.ts:82 | — | 🟥 UNRESOLVED |
| `ReplicaOptions` | config/zero-config.ts:129 | — | 🟥 UNRESOLVED |
| `RateLimit` | config/zero-config.ts:147 | — | 🟥 UNRESOLVED |
| `AuthConfig` | config/zero-config.ts:333 | — | 🟥 UNRESOLVED |
| `LegacyJWTAuthConfig` | config/zero-config.ts:336 | — | 🟥 UNRESOLVED |
| `zeroOptions` | config/zero-config.ts:344 | — | 🟥 UNRESOLVED |
| `ZeroConfig` | config/zero-config.ts:1199 | — | 🟥 UNRESOLVED |
| `getZeroConfig` | config/zero-config.ts:1203 | — | 🟥 UNRESOLVED |
| `getNormalizedZeroConfig` | config/zero-config.ts:1224 | — | 🟥 UNRESOLVED |
| `assertNormalized` | config/zero-config.ts:1228 | — | 🟥 UNRESOLVED |
| `getServerVersion` | config/zero-config.ts:1236 | — | 🟥 UNRESOLVED |
| `isAdminPasswordValid` | config/zero-config.ts:1242 | `is_admin_password_valid` services/view_syncer/inspect_handler.rs:23 | ✅ exact |
| `warnOnce` | config/zero-config.ts:1289 | — | 🟥 UNRESOLVED |
| `resetWarnOnceState` | config/zero-config.ts:1297 | — | 🟥 UNRESOLVED |
| `TransformResponse` | custom-queries/transform-query.ts:35 | — | 🟥 UNRESOLVED |
| `HashedTransformResponse` | custom-queries/transform-query.ts:43 | — | 🟥 UNRESOLVED |
| `CustomQueryTransformer` | custom-queries/transform-query.ts:82 | `CustomQueryContext` custom_queries/transform_query.rs:49 | 🔁 rename 0.50 |
| `destroy` | services/view-syncer/pipeline-driver.ts:447 | `destroy` services/view_syncer/pipeline_driver.rs:597 | ✅ exact |
| `validate` | custom-queries/transform-query.ts:111 | `validate_auth` auth/jwt.rs:381 | 🔁 rename 0.50 |
| `transform` | custom-queries/transform-query.ts:117 | `post_transform` custom_queries/transform_query.rs:304 | 🔁 rename 0.50 |
| `getCacheKey` | custom-queries/transform-query.ts:259 | `get_cache_key` custom_queries/transform_query.rs:521 | ✅ exact |
| `normalizedHeaders` | custom-queries/transform-query.ts:278 | `normalized_headers` custom_queries/transform_query.rs:514 | ✅ exact |
| `compileUrlPattern` | custom/fetch.ts:52 | N/A | 📌 no separate compile step; url_match matches the raw pattern inline |
| `getBodyPreview` | custom/fetch.ts:62 | `BODY_PREVIEW_CAP` custom/fetch.rs:49 | 🔁 rename 0.67 |
| `FetchMetricsOptions` | custom/fetch.ts:92 | — | 🟥 UNRESOLVED |
| `fetchFromAPIServer` | custom/fetch.ts:97 | custom_queries/transform_query.rs post_transform | 📌 push-class calls go via services/mutagen/pusher.rs relay POST (I-3) |
| `apiInFlight` | custom/fetch.ts:116 | `record_api_in_flight` custom/metrics.rs:116 | 🔁 rename 0.75 |
| `urlMatch` | custom/fetch.ts:389 | `url_match` custom/fetch.rs:11 | ✅ exact |
| `getBackoffDelayMs` | custom/fetch.ts:407 | `get_backoff_delay_ms` custom/fetch.rs:38 | ✅ exact |
| `apiFailedBody` | custom/fetch.ts:411 | `PushFailedHttpBody` protocol/error.rs:44 | 🔁 rename 0.40 |
| `apiErrorFromResult` | custom/fetch.ts:462 | custom_queries/transform_query.rs response validation | 📌 error extraction |
| `legacyPushErrorReason` | custom/fetch.ts:484 | `ErrorReason` protocol/error_reason_enum.rs:9 | 🔁 rename 0.50 |
| `ApiRequestMetricAttrs` | custom/metrics.ts:31 | `api_request_metric_attrs` custom/metrics.rs:59 | ✅ exact |
| `apiResponseErrorMetricAttrs` | custom/fetch.ts:528 | metrics.rs record_api_attempt attrs | 📌 status attrs |
| `recordApiAttempt` | custom/fetch.ts:549 | `record_api_attempt` custom/metrics.rs:84 | ✅ exact |
| `apiAttempts` | custom/fetch.ts:567 | metrics.rs record_api_attempt | 📌 OTel counter |
| `apiAttemptDuration` | custom/fetch.ts:568 | `API_DURATION_BOUNDARIES_S` custom/metrics.rs:22 | 🔁 rename 0.50 |
| `ApiOperation` | custom/metrics.ts:7 | — | 🟥 UNRESOLVED |
| `ApiCleanupType` | custom/metrics.ts:8 | — | 🟥 UNRESOLVED |
| `ApiMetricBaseAttrs` | custom/metrics.ts:10 | — | 🟥 UNRESOLVED |
| `ApiRequestResult` | custom/metrics.ts:15 | `record_api_request` custom/metrics.rs:67 | 🔁 rename 0.50 |
| `ApiAttemptResult` | custom/metrics.ts:24 | — | 🟥 UNRESOLVED |
| `ApiAttemptMetricAttrs` | custom/metrics.ts:40 | — | 🟥 UNRESOLVED |
| `apiRequests` | custom/metrics.ts:55 | — | 🟥 UNRESOLVED |
| `apiRequestDuration` | custom/metrics.ts:63 | `record_api_request_duration` custom/metrics.rs:74 | 🔁 rename 0.75 |
| `LiteTableSpecWithReplicationStatus` | db/lite-tables.ts:37 | — | 🟥 UNRESOLVED |
| `listTables` | db/lite-tables.ts:47 | `list_tables` db/lite_tables.rs:292 | ✅ exact |
| `listIndexes` | db/lite-tables.ts:141 | `list_unique_indexes` db/lite_tables.rs:200 | 🔁 rename 0.67 |
| `ZqlSpecOptions` | db/lite-tables.ts:184 | — | 🟥 UNRESOLVED |
| `computeZqlSpecs` | db/lite-tables.ts:210 | `compute_zql_specs` db/lite_tables.rs:79 | ✅ exact |
| `computeZqlSpecsFromLiteSpecs` | db/lite-tables.ts:227 | `compute_table_specs_from_path` db/lite_tables.rs:73 | 🔁 rename 0.43 |
| `mustGetTableSpec` | db/lite-tables.ts:326 | `IvmTableSpec` services/view_syncer/pipeline_driver.rs:49 | 🔁 rename 0.50 |
| `keyCmp` | db/lite-tables.ts:343 | db/lite_tables.rs sort_by len-then-lex | 📌 inlined key compare |
| `Category` | observability/metrics.ts:13 | — | 🟥 UNRESOLVED |
| `NATIVE_HISTOGRAM_INSTRUMENT_NAMES` | observability/metrics.ts:25 | — | 🟥 UNRESOLVED |
| `LONG_DURATION_HISTOGRAM_BOUNDARIES_S` | observability/metrics.ts:31 | — | 🟥 UNRESOLVED |
| `getMeter` | observability/metrics.ts:35 | — | 🟥 UNRESOLVED |
| `cache` | observability/metrics.ts:42 | `cache_get` custom_queries/transform_query.rs:531 | 🔁 rename 1.00 |
| `getOrCreateUpDownCounter` | observability/metrics.ts:61 | — | 🟥 UNRESOLVED |
| `LatencyHistogram` | observability/metrics.ts:91 | `Histogram` observability/metrics.rs:659 | 🔁 rename 0.50 |
| `recordMs` | observability/metrics.ts:99 | — | 🟥 UNRESOLVED |
| `getOrCreateHistogram` | observability/metrics.ts:123 | — | 🟥 UNRESOLVED |
| `getOrCreateNativeHistogram` | observability/metrics.ts:147 | `NATIVE_HISTOGRAM_INSTRUMENTS` server/otel_start.rs:99 | 🔁 rename 0.40 |
| `getOrCreateLatencyHistogram` | observability/metrics.ts:179 | — | 🟥 UNRESOLVED |
| `getOrCreateCounter` | observability/metrics.ts:193 | `get_or_create_cg` workers/syncer.rs:1023 | 🔁 rename 0.50 |
| `getOrCreateGauge` | observability/metrics.ts:218 | — | 🟥 UNRESOLVED |
| `getInstance` | server/otel-start.ts:23 | N/A — node OtelManager singleton wrapper | 📌 rust init is free fns in server/otel_start.rs |
| `startOtelAuto` | server/otel-start.ts:30 | server/otel_start.rs init_metrics/metrics_enabled | 📌 rust otel init path; node auto-instr has no rust twin |
| `randomID` | server/syncer.ts:49 | N/A | 📌 TS pipelineRunID debug-correlation id; not ported |
| `getCustomQueryConfig` | server/syncer.ts:53 | `CustomQuerySpec` custom_queries/transform_query.rs:117 | 🔁 rename 0.50 |
| `runWorker` | server/syncer.ts:74 | N/A — node worker bootstrap | 📌 rust process entry is the invented main/http_server pair |
| `assert` | server/syncer.ts:79 | Rust assert! macro | 📌 idiom |
| `initEventSink` | server/syncer.ts:90 | — | 🟥 UNRESOLVED |
| `registerSQLiteCorruptionDiagnosticTarget` | server/syncer.ts:95 | — | 🟥 UNRESOLVED |
| `startAnonymousTelemetry` | server/syncer.ts:282 | — | 🟥 UNRESOLVED |
| `Pusher` | services/mutagen/pusher.ts:40 | `PUSHER` live_count.rs:33 | ✅ exact |
| `initConnection` | services/mutagen/pusher.ts:41 | `init_connection` services/mutagen/pusher.rs:515 | ✅ exact |
| `enqueuePush` | services/mutagen/pusher.ts:42 | `enqueue_push` services/mutagen/pusher.rs:455 | ✅ exact |
| `ackMutationResponses` | services/mutagen/pusher.ts:43 | `ack_mutation_responses` services/mutagen/pusher.rs:517 | ✅ exact |
| `deleteClientMutations` | services/mutagen/pusher.ts:47 | `delete_client_mutations` services/mutagen/pusher.rs:561 | ✅ exact |
| `PusherService` | services/mutagen/pusher.ts:68 | `PusherService` services/mutagen/pusher.rs:211 | ✅ exact |
| `ref` | services/mutagen/pusher.ts:227 | — | 🟥 UNRESOLVED |
| `unref` | services/mutagen/pusher.ts:232 | — | 🟥 UNRESOLVED |
| `hasRefs` | services/mutagen/pusher.ts:240 | — | 🟥 UNRESOLVED |
| `run` | services/mutagen/pusher.ts:244 | view_syncer.rs cg_event_loop | 📌 per-CG async serving loop |
| `stop` | services/mutagen/pusher.ts:249 | view_syncer.rs shutdown() | 📌 per-CG drain + Rehome |
| `combinePushes` | services/mutagen/pusher.ts:626 | `combine_pushes` services/mutagen/pusher.rs:164 | ✅ exact |
| `assertAreCompatiblePushes` | services/mutagen/pusher.ts:669 | — | 🟥 UNRESOLVED |
| `ConnectionState` | services/view-syncer/connection-context-manager.ts:17 | `ConnectionState` services/view_syncer/connection_context_manager.rs:36 | ✅ exact |
| `UserState` | services/view-syncer/connection-context-manager.ts:23 | `UserState` services/view_syncer/connection_context_manager.rs:43 | ✅ exact |
| `ConnectionValidation` | services/view-syncer/connection-context-manager.ts:30 | `ConnectionValidation` services/view_syncer/connection_context_manager.rs:49 | ✅ exact |
| `ConnectionSelector` | services/view-syncer/connection-context-manager.ts:37 | `ConnectionSelector` services/view_syncer/connection_context_manager.rs:56 | ✅ exact |
| `HeaderOptions` | services/view-syncer/connection-context-manager.ts:44 | `HeaderOptions` services/view_syncer/connection_context_manager.rs:70 | ✅ exact |
| `ConnectionFetchContext` | services/view-syncer/connection-context-manager.ts:54 | `ConnectionFetchContext` services/view_syncer/connection_context_manager.rs:81 | ✅ exact |
| `ConnectionContext` | services/view-syncer/connection-context-manager.ts:65 | `ConnectionContext` services/view_syncer/connection_context_manager.rs:103 | ✅ exact |
| `GroupAuthState` | services/view-syncer/connection-context-manager.ts:95 | `GroupAuthState` services/view_syncer/connection_context_manager.rs:121 | ✅ exact |
| `ConnectionContextManager` | services/view-syncer/connection-context-manager.ts:104 | `ConnectionContextManager` services/view_syncer/connection_context_manager.rs:404 | ✅ exact |
| `registerConnection` | services/view-syncer/connection-context-manager.ts:105 | `register_connection` services/view_syncer/connection_context_manager.rs:446 | ✅ exact |
| `updateAuth` | services/view-syncer/connection-context-manager.ts:116 | `update_auth` services/view_syncer/connection_context_manager.rs:573 | ✅ exact |
| `validateConnection` | services/view-syncer/connection-context-manager.ts:121 | `validate_connection` services/view_syncer/connection_context_manager.rs:608 | ✅ exact |
| `failConnection` | services/view-syncer/connection-context-manager.ts:132 | `fail_connection` services/view_syncer/connection_context_manager.rs:672 | ✅ exact |
| `closeConnection` | services/view-syncer/connection-context-manager.ts:136 | `close_connection` services/view_syncer/connection_context_manager.rs:682 | ✅ exact |
| `markBackgroundRetransformSuccess` | services/view-syncer/connection-context-manager.ts:140 | `mark_background_retransform_success` services/view_syncer/connection_context_manager.rs:688 | ✅ exact |
| `setSharedRetransformReady` | services/view-syncer/connection-context-manager.ts:145 | `set_shared_retransform_ready` services/view_syncer/connection_context_manager.rs:708 | ✅ exact |
| `deferMaintenance` | services/view-syncer/connection-context-manager.ts:147 | `defer_maintenance` services/view_syncer/connection_context_manager.rs:718 | ✅ exact |
| `getConnectionContext` | services/view-syncer/connection-context-manager.ts:149 | `get_connection_context` services/view_syncer/connection_context_manager.rs:734 | ✅ exact |
| `mustGetConnectionContext` | services/view-syncer/connection-context-manager.ts:152 | `must_get_connection_context` services/view_syncer/connection_context_manager.rs:745 | ✅ exact |
| `getBackgroundConnectionContext` | services/view-syncer/connection-context-manager.ts:156 | `get_background_connection_context` services/view_syncer/connection_context_manager.rs:756 | ✅ exact |
| `mustGetBackgroundConnectionContext` | services/view-syncer/connection-context-manager.ts:157 | `must_get_background_connection_context` services/view_syncer/connection_context_manager.rs:761 | ✅ exact |
| `getGroupState` | services/view-syncer/connection-context-manager.ts:159 | `get_group_state` services/view_syncer/connection_context_manager.rs:769 | ✅ exact |
| `planMaintenance` | services/view-syncer/connection-context-manager.ts:161 | `plan_maintenance` services/view_syncer/connection_context_manager.rs:775 | ✅ exact |
| `ConnectionContextManagerImpl` | services/view-syncer/connection-context-manager.ts:176 | — | 🟥 UNRESOLVED |
| `compareByInsertionOrder` | services/view-syncer/connection-context-manager.ts:844 | `compare_by_insertion_order` services/view_syncer/connection_context_manager.rs:968 | ✅ exact |
| `comparePreferredValidatedConnection` | services/view-syncer/connection-context-manager.ts:851 | `compare_preferred_validated_connection` services/view_syncer/connection_context_manager.rs:975 | ✅ exact |
| `minDefined` | services/view-syncer/connection-context-manager.ts:858 | `min_defined` services/view_syncer/connection_context_manager.rs:959 | ✅ exact |
| `sameConnectionSelector` | services/view-syncer/connection-context-manager.ts:868 | services/view_syncer/connection_context_manager.rs set_background_connection | 📌 inlined tuple match |
| `filterHeaders` | services/view-syncer/connection-context-manager.ts:875 | `filter_headers` services/view_syncer/connection_context_manager.rs:372 | ✅ exact |
| `DrainCoordinator` | services/view-syncer/drain-coordinator.ts:31 | `DrainCoordinator` services/view_syncer/drain_coordinator.rs:39 | ✅ exact |
| `draining` | services/view-syncer/drain-coordinator.ts:37 | `is_draining` services/view_syncer/drain_coordinator.rs:112 | 🔁 rename 1.00 |
| `shouldDrain` | services/view-syncer/drain-coordinator.ts:41 | `should_drain` services/view_syncer/drain_coordinator.rs:57 | ✅ exact |
| `drainNextIn` | services/view-syncer/drain-coordinator.ts:45 | `drain_next_in` services/view_syncer/drain_coordinator.rs:66 | ✅ exact |
| `forceDrainTimeout` | services/view-syncer/drain-coordinator.ts:66 | `force_drain_timeout` services/view_syncer/drain_coordinator.rs:92 | ✅ exact |
| `nextDrainTime` | services/view-syncer/drain-coordinator.ts:71 | `next_drain_time` services/view_syncer/drain_coordinator.rs:117 | ✅ exact |
| `PendingUpstreamCommit` | services/view-syncer/e2e-serving-lag.ts:3 | `PendingUpstreamCommit` services/view_syncer/e2e_serving_lag.rs:14 | ✅ exact |
| `E2EServingLagTracker` | services/view-syncer/e2e-serving-lag.ts:19 | `E2EServingLagTracker` services/view_syncer/e2e_serving_lag.rs:29 | ✅ exact |
| `pending` | services/view-syncer/e2e-serving-lag.ts:22 | `pending` services/view_syncer/e2e_serving_lag.rs:38 | ✅ exact |
| `onVersionReady` | services/view-syncer/e2e-serving-lag.ts:35 | `on_version_ready` services/view_syncer/e2e_serving_lag.rs:50 | ✅ exact |
| `onVersionServed` | services/view-syncer/e2e-serving-lag.ts:55 | `on_version_served` services/view_syncer/e2e_serving_lag.rs:72 | ✅ exact |
| `Observation` | services/view-syncer/e2e-serving-lag.ts:77 | `Observation` services/view_syncer/e2e_serving_lag.rs:21 | ✅ exact |
| `handleInspect` | services/view-syncer/inspect-handler.ts:25 | `handle_inspect` services/view_syncer/inspect_handler.rs:41 | ✅ exact |
| `metricsForProtocol` | services/view-syncer/inspect-handler.ts:193 | — | 🟥 UNRESOLVED |
| `RowAdd` | services/view-syncer/pipeline-driver.ts:77 | — | 🟥 UNRESOLVED |
| `RowRemove` | services/view-syncer/pipeline-driver.ts:79 | — | 🟥 UNRESOLVED |
| `RowEdit` | services/view-syncer/pipeline-driver.ts:81 | — | 🟥 UNRESOLVED |
| `RowChange` | services/view-syncer/pipeline-driver.ts:83 | — | 🟥 UNRESOLVED |
| `PipelineHydrationReason` | services/view-syncer/pipeline-driver.ts:123 | — | 🟥 UNRESOLVED |
| `Timer` | services/view-syncer/pipeline-driver.ts:158 | — | 🟥 UNRESOLVED |
| `projectedAdvancementTimeMs` | services/view-syncer/pipeline-driver.ts:180 | rust-ivm advance_gate.rs | 📌 ported |
| `advancementResetTimeLimitMs` | services/view-syncer/pipeline-driver.ts:191 | rust-ivm advance_gate.rs | 📌 ported |
| `minProjectedAdvancementSampleChanges` | services/view-syncer/pipeline-driver.ts:195 | rust-ivm advance_gate.rs | 📌 ported |
| `shouldResetProjectedAdvancement` | services/view-syncer/pipeline-driver.ts:205 | rust-ivm advance_gate.rs | 📌 ported |
| `shouldFinishLateAdvancement` | services/view-syncer/pipeline-driver.ts:228 | rust-ivm advance_gate.rs | 📌 ported |
| `shouldResetSlowCurrentChange` | services/view-syncer/pipeline-driver.ts:238 | rust-ivm advance_gate.rs | 📌 ported |
| `PipelineDriver` | services/view-syncer/pipeline-driver.ts:251 | — | 🟥 UNRESOLVED |
| `init` | services/view-syncer/pipeline-driver.ts:325 | `init` services/view_syncer/pipeline_driver.rs:233 | ✅ exact |
| `initialized` | services/view-syncer/pipeline-driver.ts:334 | `initialized` services/view_syncer/pipeline_driver.rs:216 | ✅ exact |
| `reset` | services/view-syncer/pipeline-driver.ts:343 | `record_reset` observability/metrics.rs:779 | 🔁 rename 0.50 |
| `replicaVersion` | services/view-syncer/pipeline-driver.ts:386 | pipeline_driver.rs snapshotter current_version | 📌 field/getter |
| `currentVersion` | services/view-syncer/pipeline-driver.ts:395 | `current_version` services/view_syncer/pipeline_driver.rs:221 | ✅ exact |
| `currentPermissions` | services/view-syncer/pipeline-driver.ts:403 | view_syncer.rs/message_handler perms reload | 📌 perms hot-reload at CG dispatch |
| `advanceWithoutDiff` | services/view-syncer/pipeline-driver.ts:422 | pipeline_driver.rs advance_without_diff | 📌 ported |
| `queries` | services/view-syncer/pipeline-driver.ts:458 | pipeline_driver.rs running_queries/active_query_ids | 📌 split getters |
| `totalHydrationTimeMs` | services/view-syncer/pipeline-driver.ts:462 | rust-ivm engine total_hydration_time_ms | 📌 ported (cross-crate) |
| `addQuery` | services/view-syncer/pipeline-driver.ts:574 | rust-ivm engine add_queries | 📌 streaming add (cross-crate) |
| `removeQuery` | services/view-syncer/pipeline-driver.ts:834 | `remove_query` services/view_syncer/pipeline_driver.rs:424 | ✅ exact |
| `rowSetSignature` | services/view-syncer/pipeline-driver.ts:874 | — | 🟥 UNRESOLVED |
| `getRow` | services/view-syncer/pipeline-driver.ts:906 | `get_row` services/view_syncer/pipeline_driver.rs:578 | ✅ exact |
| `advance` | services/view-syncer/pipeline-driver.ts:923 | `advance` services/view_syncer/pipeline_driver.rs:500 | ✅ exact |
| `accumulate` | services/view-syncer/pipeline-driver.ts:1276 | — | 🟥 UNRESOLVED |
| `stream` | services/view-syncer/pipeline-driver.ts:1285 | CROSS-CRATE rust-ivm streamer/mod | 📌 RowChange streaming lives in the ivm crate's Streamer |
| `setOutput` | services/view-syncer/pipeline-driver.ts:1416 | rust-ivm operator set_output | 📌 trait method (cross-crate) |
| `fetch` | services/view-syncer/pipeline-driver.ts:1428 | `FetchConfig` services/view_syncer/connection_context_manager.rs:146 | 🔁 rename 0.50 |
| `push` | services/view-syncer/pipeline-driver.ts:1432 | `push` ws_sink.rs:103 | ✅ exact |
| `logQueryFailure` | services/view-syncer/pipeline-driver.ts:1451 | inlined | 📌 streamer error callback lives in rust-ivm; failures logged via tracing at the call sites |
| `toAdds` | services/view-syncer/pipeline-driver.ts:1472 | INLINED — rust-ivm engine hydrate emits Adds directly | 📌 no Node→AddChange adaptor needed |
| `getRowKey` | services/view-syncer/pipeline-driver.ts:1482 | rust-ivm streamer get_row_key | 📌 row-key extraction (cross-crate) |
| `hydrate` | services/view-syncer/pipeline-driver.ts:1491 | `hydrate` services/view_syncer/pipeline_driver.rs:441 | ✅ exact |
| `hydrateInternal` | services/view-syncer/pipeline-driver.ts:1505 | `internal` protocol/error.rs:190 | 🔁 rename 0.50 |
| `buildPrimaryKeys` | services/view-syncer/pipeline-driver.ts:1520 | `set_client_primary_keys` services/view_syncer/pipeline_driver.rs:416 | 🔁 rename 0.50 |
| `mustGetPrimaryKey` | services/view-syncer/pipeline-driver.ts:1530 | rust-ivm engine build | 📌 PK validated on build |
| `scalarValuesEqual` | services/view-syncer/pipeline-driver.ts:1553 | rust-ivm engine scalar_values_equal | 📌 ported (cross-crate) |
| `RunningQuery` | services/view-syncer/query-covering.ts:15 | `RunningQuery` services/view_syncer/query_covering.rs:24 | ✅ exact |
| `CoveringQuery` | services/view-syncer/query-covering.ts:21 | `CoveringQuery` services/view_syncer/query_covering.rs:33 | ✅ exact |
| `isQueryCoveredBy` | services/view-syncer/query-covering.ts:40 | `is_query_covered_by` services/view_syncer/query_covering.rs:97 | ✅ exact |
| `findCoveringQuery` | services/view-syncer/query-covering.ts:44 | `find_covering_query` services/view_syncer/query_covering.rs:106 | ✅ exact |
| `QueryCoveringIndex` | services/view-syncer/query-covering.ts:55 | `QueryCoveringIndex` services/view_syncer/query_covering.rs:120 | ✅ exact |
| `add` | services/view-syncer/query-covering.ts:67 | `add` services/view_syncer/query_covering.rs:141 | ✅ exact |
| `remove` | services/view-syncer/query-covering.ts:81 | `remove` services/view_syncer/query_covering.rs:158 | ✅ exact |
| `rootKey` | services/view-syncer/query-covering.ts:125 | `root_key` services/view_syncer/query_covering.rs:193 | ✅ exact |
| `astCoveredBy` | services/view-syncer/query-covering.ts:129 | `ast_covered_by` services/view_syncer/query_covering.rs:202 | ✅ exact |
| `boundsCoveredBy` | services/view-syncer/query-covering.ts:143 | `bounds_covered_by` services/view_syncer/query_covering.rs:217 | ✅ exact |
| `relatedCoveredBy` | services/view-syncer/query-covering.ts:170 | `related_covered_by` services/view_syncer/query_covering.rs:242 | ✅ exact |
| `conditionEquivalent` | services/view-syncer/query-covering.ts:192 | `condition_equivalent` services/view_syncer/query_covering.rs:259 | ✅ exact |
| `conditionImplies` | services/view-syncer/query-covering.ts:199 | `condition_implies` services/view_syncer/query_covering.rs:263 | ✅ exact |
| `correlatedConditionImplies` | services/view-syncer/query-covering.ts:237 | `correlated_condition_implies` services/view_syncer/query_covering.rs:303 | ✅ exact |
| `sameRelatedEdge` | services/view-syncer/query-covering.ts:262 | `same_related_edge` services/view_syncer/query_covering.rs:326 | ✅ exact |
| `simpleConditionImplies` | services/view-syncer/query-covering.ts:274 | `simple_condition_implies` services/view_syncer/query_covering.rs:333 | ✅ exact |
| `equalityImplies` | services/view-syncer/query-covering.ts:315 | `equality_implies` services/view_syncer/query_covering.rs:371 | ✅ exact |
| `orderConditionImplies` | services/view-syncer/query-covering.ts:365 | `order_condition_implies` services/view_syncer/query_covering.rs:387 | ✅ exact |
| `columnLiteralParts` | services/view-syncer/query-covering.ts:399 | `ColumnLiteralParts` services/view_syncer/query_covering.rs:409 | ✅ exact |
| `isEqualityOp` | services/view-syncer/query-covering.ts:416 | `is_equality_op` services/view_syncer/query_covering.rs:430 | ✅ exact |
| `isNumericOrderOp` | services/view-syncer/query-covering.ts:420 | `is_numeric_order_op` services/view_syncer/query_covering.rs:434 | ✅ exact |
| `isNonNullScalarLiteralValue` | services/view-syncer/query-covering.ts:426 | `is_non_null_scalar_literal_value` services/view_syncer/query_covering.rs:438 | ✅ exact |
| `literalArrayIncludes` | services/view-syncer/query-covering.ts:432 | `literal_array_includes` services/view_syncer/query_covering.rs:442 | ✅ exact |
| `jsonEqual` | services/view-syncer/query-covering.ts:439 | `json_equal` services/view_syncer/query_covering.rs:462 | ✅ exact |
| `ViewSyncer` | services/view-syncer/view-syncer.ts:132 | `CgViewSyncer` services/view_syncer/view_syncer.rs:527 | 🔁 rename 0.67 |
| `changeDesiredQueries` | services/view-syncer/view-syncer.ts:138 | `change_desired_queries` services/view_syncer/view_syncer.rs:545 | ✅ exact |
| `deleteClients` | services/view-syncer/view-syncer.ts:143 | `delete_clients` services/view_syncer/view_syncer.rs:574 | ✅ exact |
| `inspect` | services/view-syncer/view-syncer.ts:148 | `inspect` services/view_syncer/view_syncer.rs:603 | ✅ exact |
| `SyncContext` | services/view-syncer/view-syncer.ts:165 | — | 🟥 UNRESOLVED |
| `shutdownBeforeInitializationError` | services/view-syncer/view-syncer.ts:181 | view_syncer.rs init-fail path | 📌 error on terminal init failure |
| `TTL_CLOCK_INTERVAL` | services/view-syncer/view-syncer.ts:202 | `TTL_CLOCK_INTERVAL` services/view_syncer/view_syncer.rs:80 | ✅ exact |
| `TTL_TIMER_HYSTERESIS` | services/view-syncer/view-syncer.ts:210 | `TTL_TIMER_HYSTERESIS_MS` services/view_syncer/view_syncer.rs:77 | 🔁 rename 0.75 |
| `ViewSyncerService` | services/view-syncer/view-syncer.ts:214 | `ViewSyncerService` services/view_syncer/view_syncer.rs:617 | ✅ exact |
| `readyState` | services/view-syncer/view-syncer.ts:521 | view_syncer.rs ViewSyncerService/event loop | 📌 init/drain state flags |
| `queryCount` | services/view-syncer/view-syncer.ts:658 | `query_count` services/view_syncer/view_syncer.rs:1002 | ✅ exact |
| `rowCount` | services/view-syncer/view-syncer.ts:662 | `row_count` services/view_syncer/view_syncer.rs:1008 | ✅ exact |
| `servedVersion` | services/view-syncer/view-syncer.ts:666 | `mark_version_served` services/view_syncer/view_syncer.rs:2432 | 🔁 rename 0.67 |
| `servingLagEligible` | services/view-syncer/view-syncer.ts:670 | `serving_lag_eligible` services/view_syncer/view_syncer.rs:996 | ✅ exact |
| `keepalive` | services/view-syncer/view-syncer.ts:702 | view_syncer.rs ViewSyncerService.keepalive_until | 📌 field + next_idle_shutdown_delay |
| `markInitialized` | services/view-syncer/view-syncer.ts:2838 | view_syncer.rs ViewSyncerService.terminal | 📌 init-state flag; test helper dropped |
| `yieldProcess` | services/view-syncer/view-syncer.ts:2861 | N/A | 📌 tokio async yield; no global-lock setImmediate |
| `contentsAndVersion` | services/view-syncer/view-syncer.ts:2865 | view_syncer.rs engine seat (strip _0_version) | 📌 inlined |
| `checkClientAndCVRVersions` | services/view-syncer/view-syncer.ts:2875 | `check_client_and_cvr_versions` services/view_syncer/view_syncer.rs:90 | ✅ exact |
| `isTransformFailedError` | services/view-syncer/view-syncer.ts:2897 | `TransformFailedHttpBody` protocol/error.rs:82 | 🔁 rename 0.40 |
| `expired` | services/view-syncer/view-syncer.ts:2908 | view_syncer.rs remove_expired_queries | 📌 TTL/inactivation expiry |
| `hasExpiredQueries` | services/view-syncer/view-syncer.ts:2933 | — | 🟥 UNRESOLVED |
| `TimeSliceTimer` | services/view-syncer/view-syncer.ts:2943 | — | 🟥 UNRESOLVED |
| `start` | services/view-syncer/view-syncer.ts:2952 | view_syncer.rs ensure_cvr/ViewSyncerService init | 📌 CVR load + ttl seed |
| `startWithoutYielding` | services/view-syncer/view-syncer.ts:2959 | N/A | 📌 no setImmediate; sync Instant::now start |
| `elapsedLap` | services/view-syncer/view-syncer.ts:2976 | N/A | 📌 per-lap timing via Instant::elapsed() inline |
| `totalElapsed` | services/view-syncer/view-syncer.ts:2997 | N/A | 📌 inline Instant::elapsed accumulation |
| `ConnectParams` | workers/connect-params.ts:9 | `ConnectParams` workers/connect_params.rs:10 | ✅ exact |
| `normalizeHeaders` | workers/connect-params.ts:32 | ws_server.rs (dup-header join) | 📌 header normalization |
| `getConnectParams` | workers/connect-params.ts:45 | `get_connect_params` workers/connect_params.rs:63 | ✅ exact |
| `HandlerResult` | workers/connection.ts:31 | `HandlerResult` workers/connection.rs:23 | ✅ exact |
| `StreamResult` | workers/connection.ts:45 | — | 🟥 UNRESOLVED |
| `MessageHandler` | workers/connection.ts:51 | `MessageHandler` workers/connection.rs:40 | ✅ exact |
| `handleMessage` | workers/connection.ts:52 | `handle_message` workers/connection.rs:43 | ✅ exact |
| `Connection` | workers/connection.ts:78 | `Connection` workers/connection.rs:59 | ✅ exact |
| `close` | workers/connection.ts:168 | `close` workers/connection.rs:232 | ✅ exact |
| `handleInitConnection` | workers/connection.ts:190 | `handle_init_connection` workers/connection.rs:292 | ✅ exact |
| `send` | workers/connection.ts:348 | `send` workers/connection.rs:249 | ✅ exact |
| `sendError` | workers/connection.ts:356 | `send_error` workers/connection.rs:257 | ✅ exact |
| `WebSocketLike` | workers/connection.ts:361 | `DirectWebSocketSink` ws_sink.rs:80 | 🔁 rename 0.40 |
| `findProtocolError` | workers/connection.ts:433 | workers/connection.rs classify_error_log_level | 📌 protocol-error classify |
| `hasErrno` | workers/connection.ts:443 | N/A | 📌 Node `'errno' in e`; Rust WS stack has no errno |
| `hasTransientSocketCode` | workers/connection.ts:466 | `has_transient_socket_code` workers/connection.rs:342 | ✅ exact |
| `isTransientSocketMessage` | workers/connection.ts:477 | `is_transient_socket_message` workers/connection.rs:349 | ✅ exact |
| `withTraceparent` | workers/syncer-ws-message-handler.ts:28 | `with_traceparent` workers/syncer_ws_message_handler.rs:26 | ✅ exact |
| `SyncerWsMessageHandler` | workers/syncer-ws-message-handler.ts:36 | `SyncerWsMessageHandler` workers/syncer_ws_message_handler.rs:212 | ✅ exact |
| `SyncerWorkerData` | workers/syncer.ts:48 | — | 🟥 UNRESOLVED |
| `ReplicaReadyState` | workers/syncer.ts:52 | `ReplicaReadyState` workers/syncer.rs:26 | ✅ exact |
| `ServingLagViewSyncer` | workers/syncer.ts:57 | `ServingLagViewSyncer` workers/syncer.rs:35 | ✅ exact |
| `ServingLagStats` | workers/syncer.ts:62 | `ServingLagStats` workers/syncer.rs:43 | ✅ exact |
| `MAX_REPLICA_READY_STATES` | workers/syncer.ts:76 | `MAX_REPLICA_READY_STATES` workers/syncer.rs:19 | ✅ exact |
| `boundReplicaReadyStates` | workers/syncer.ts:82 | `bound_replica_ready_states` workers/syncer.rs:83 | ✅ exact |
| `pruneReplicaReadyStates` | workers/syncer.ts:93 | `prune_replica_ready_states` workers/syncer.rs:92 | ✅ exact |
| `lowerBoundReplicaReadyTimeMs` | workers/syncer.ts:104 | `lower_bound_replica_ready_time_ms` workers/syncer.rs:105 | ✅ exact |
| `upperBoundWatermark` | workers/syncer.ts:121 | `upper_bound_watermark` workers/syncer.rs:124 | ✅ exact |
| `findFirstUnservedIndex` | workers/syncer.ts:138 | `find_first_unserved_index` workers/syncer.rs:141 | ✅ exact |
| `percentileNearestRank` | workers/syncer.ts:160 | `percentile_nearest_rank` workers/syncer.rs:160 | ✅ exact |
| `computeServingLagDistributionMs` | workers/syncer.ts:174 | `compute_serving_lag_distribution_ms` workers/syncer.rs:175 | ✅ exact |
| `computeServingLagStatsMs` | workers/syncer.ts:226 | `compute_serving_lag_stats_ms` workers/syncer.rs:223 | ✅ exact |
| `computeMaxServingLagMs` | workers/syncer.ts:247 | `compute_max_serving_lag_ms` workers/syncer.rs:232 | ✅ exact |
| `getWebSocketServerOptions` | workers/syncer.ts:255 | ws_server.rs WebSocketConfig | 📌 compression opts |
| `Syncer` | workers/syncer.ts:288 | `Syncer` workers/syncer.rs:574 | ✅ exact |
| `drain` | workers/syncer.ts:732 | `drain` workers/syncer.rs:1229 | ✅ exact |
