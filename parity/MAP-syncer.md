# TS ⇄ Rust parity map — `syncer` crate

_Deterministic. File edges + symbol pairs are derived from **shared symbol content**, never filenames — so renamed files (e.g. `drain-coordinator.ts`→`drain.rs`) and renamed symbols (`cvrErrorKind`→`CVRStoreError`) still bind. Bodies are not compared; behavior drift needs Layer-2 body review._

- symbols: TS **218**, Rust **648** · resolved pairs **146** (exact 119 + fuzzy 27) + aliases 54
- 🟥 TS UNRESOLVED: **21** (**0** behavioral ⇒ investigate · 21 structural: zod/DDL/type-alias ⇒ serde/inline-SQL, expected) · 🟦 Rust-only ADDED: **502**

## 1 · File structure diff

TS origin files: **16**  ·  Rust files: **32** (10 new)

| TS file (LOC) | rel | Rust file(s) (shared syms) |
|---|---|---|
| `auth/auth.ts` (243) | **MERGED** | `services/view_syncer/connection_context_manager.rs` (4), `protocol.rs` (1) |
| `auth/jwt.ts` (89) | **MERGED** | `auth/jwt.rs` (4) |
| `auth/read-authorizer.ts` (152) | **1:1** | `auth/read_authorizer.rs` (5) |
| `custom-queries/transform-query.ts` (290) | **MERGED** | `custom_queries/transform_query.rs` (5), `services/view_syncer/pipeline_driver.rs` (1), `auth/jwt.rs` (1) |
| `custom/fetch.ts` (569) | **SPLIT** | `metrics.rs` (6), `custom_queries/transform_query.rs` (4), `protocol.rs` (2), `push_relay.rs` (1) |
| `db/lite-tables.ts` (356) | **1:1** | `db/lite_tables.rs` (5), `services/view_syncer/pipeline_driver.rs` (1) |
| `services/view-syncer/connection-context-manager.ts` (892) | **MERGED** | `services/view_syncer/connection_context_manager.rs` (23), `router.rs` (3), `main.rs` (3) |
| `services/view-syncer/drain-coordinator.ts` (76) | **1:1** | `services/view_syncer/drain_coordinator.rs` (6) |
| `services/view-syncer/e2e-serving-lag.ts` (82) | **1:1** | `services/view_syncer/e2e_serving_lag.rs` (6) |
| `services/view-syncer/pipeline-driver.ts` (1558) | **SPLIT** | `services/view_syncer/pipeline_driver.rs` (8), `advance_gate.rs` (6), `pipeline_driver.rs` (3), `sync_engine.rs` (2), `services/view_syncer/connection_context_manager.rs` (1), `metrics.rs` (1), `router.rs` (1) |
| `services/view-syncer/query-covering.ts` (444) | **1:1** | `services/view_syncer/query_covering.rs` (24), `metrics.rs` (1) |
| `services/view-syncer/view-syncer.ts` (3002) | **SPLIT** | `router.rs` (15), `main.rs` (5), `sync_engine.rs` (2), `protocol.rs` (1) |
| `workers/connect-params.ts` (100) | **1:1** | `workers/connect_params.rs` (2), `ws_server.rs` (1) |
| `workers/connection.ts` (485) | **1:1** | `workers/connection.rs` (8), `live_count.rs` (1), `router.rs` (1), `ws_sink.rs` (1) |
| `workers/syncer-ws-message-handler.ts` (283) | **1:1** | `workers/syncer_ws_message_handler.rs` (2) |
| `workers/syncer.ts` (759) | **1:1** | `workers/syncer.rs` (13), `router.rs` (1), `main.rs` (1), `ws_server.rs` (1) |

**New Rust files (no TS origin — added in the port):**  `auth.rs` (5), `custom_queries.rs` (4), `db.rs` (3), `http_server.rs` (454), `lib.rs` (81), `otel.rs` (131), `services.rs` (3), `services/view_syncer.rs` (9), `trace.rs` (30), `workers.rs` (8)

**Merges (many TS → one Rust file):**
- `auth/jwt.rs` ⟵ `auth/jwt.ts`, `custom-queries/transform-query.ts`
- `custom_queries/transform_query.rs` ⟵ `custom-queries/transform-query.ts`, `custom/fetch.ts`
- `main.rs` ⟵ `services/view-syncer/connection-context-manager.ts`, `services/view-syncer/view-syncer.ts`, `workers/syncer.ts`
- `metrics.rs` ⟵ `custom/fetch.ts`, `services/view-syncer/pipeline-driver.ts`, `services/view-syncer/query-covering.ts`
- `protocol.rs` ⟵ `auth/auth.ts`, `custom/fetch.ts`, `services/view-syncer/view-syncer.ts`
- `router.rs` ⟵ `services/view-syncer/connection-context-manager.ts`, `services/view-syncer/pipeline-driver.ts`, `services/view-syncer/view-syncer.ts`, `workers/connection.ts`, `workers/syncer.ts`
- `services/view_syncer/connection_context_manager.rs` ⟵ `auth/auth.ts`, `services/view-syncer/connection-context-manager.ts`, `services/view-syncer/pipeline-driver.ts`
- `services/view_syncer/pipeline_driver.rs` ⟵ `custom-queries/transform-query.ts`, `db/lite-tables.ts`, `services/view-syncer/pipeline-driver.ts`
- `sync_engine.rs` ⟵ `services/view-syncer/pipeline-driver.ts`, `services/view-syncer/view-syncer.ts`
- `ws_server.rs` ⟵ `workers/connect-params.ts`, `workers/syncer.ts`

## 2 · Per-file functional divergence

### `auth/jwt.rs`  ⟵  `auth/jwt.ts`, `custom-queries/transform-query.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `validate` (custom-queries/transform-query.ts:111) | `validate_auth` (:381) | fuzzy 0.50 |

🟥 **TS symbols not resolved into this file (1):** `tokenConfigOptions`

🟦 **Rust-only added here (18):** `CachedJwks`, `Claims`, `JWKS_CACHE`, `JWKS_HTTP`, `JWKS_REFETCH_COOLDOWN`, `JWKS_TTL`, `JwtAuthValidator`, `apply_claim_validation`, `decode_jwt_claims`, `has_config`, `key_algorithm_to_signature_alg`, `lookup_cached_jwk`, `lookup_stale_cached_jwk`, `select_jwk`, `verify_sync`, `verify_with_jwk`, `verify_with_jwks`, `within_refetch_cooldown`

### `auth/read_authorizer.rs`  ⟵  `auth/read-authorizer.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `addRulesToWhere` (auth/read-authorizer.ts:105) | `add_rules_to_where` (:135) | exact |
| `transformAndHashQuery` (auth/read-authorizer.ts:24) | `transform_and_hash_query` (:63) | exact |
| `transformCondition` (auth/read-authorizer.ts:127) | `transform_condition` (:146) | exact |
| `transformQuery` (auth/read-authorizer.ts:45) | `transform_query` (:79) | exact |
| `transformQueryInternal` (auth/read-authorizer.ts:61) | `transform_query_internal` (:86) | exact |

🟥 **TS symbols not resolved into this file (1):** `TransformedAndHashed`

🟦 **Rust-only added here (37):** `DIGITS`, `LoadedPermissions`, `OPS`, `PermissionsReload`, `base36`, `bind_condition`, `bind_static_parameters`, `bind_value`, `bind_visit`, `cmp_condition`, `cmp_optional_bool`, `cmp_related`, `compare_utf8_maybe_null`, `compare_value_position`, `ctype`, `deny_all_permissions`, `flatten`, `flattened`, `hash_of_ast`, `insert_if_present`, `is_always_false`, `is_always_true`, `js_string`, `load_permissions`, `normalize_ast`, `normalize_related_entry`, `normalize_where`, `reload_permissions_if_changed`, `resolve_field`, `resolve_permissions`, `simplify_condition`, `validate_condition_value`, `validate_permission_asset`, `validate_permission_condition`, `validate_permissions_config`, `validate_policy`, `validate_related_subquery`

### `custom_queries/transform_query.rs`  ⟵  `custom-queries/transform-query.ts`, `custom/fetch.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `CustomQueryTransformer` (custom-queries/transform-query.ts:82) | `CustomQueryContext` (:48) | fuzzy 0.50 |
| `getBackoffDelayMs` (custom/fetch.ts:407) | `get_backoff_delay_ms` (:261) | exact |
| `getCacheKey` (custom-queries/transform-query.ts:259) | `get_cache_key` (:482) | exact |
| `normalizedHeaders` (custom-queries/transform-query.ts:278) | `normalized_headers` (:475) | exact |
| `transform` (custom-queries/transform-query.ts:117) | `post_transform` (:278) | fuzzy 0.50 |
| `urlMatch` (custom/fetch.ts:389) | `url_match` (:121) | exact |

🟥 **TS symbols not resolved into this file (2):** `HashedTransformResponse`, `TransformResponse`

🟦 **Rust-only added here (16):** `CACHE_TTL`, `CustomQuerySpec`, `CustomTransformed`, `FETCH_MAX_ATTEMPTS`, `HTTP_CLIENT`, `RESERVED_PARAMS`, `TRANSFORM_CACHE`, `TransformedQuery`, `cache_get`, `cache_set`, `composed_headers`, `glob_match`, `post_transform_attempts`, `seed_transform_cache_for_test`, `set_header`, `transform_custom_queries`

### `db/lite_tables.rs`  ⟵  `db/lite-tables.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `computeZqlSpecs` (db/lite-tables.ts:210) | `compute_zql_specs` (:79) | exact |
| `computeZqlSpecsFromLiteSpecs` (db/lite-tables.ts:227) | `compute_table_specs_from_path` (:73) | fuzzy 0.43 |
| `listIndexes` (db/lite-tables.ts:141) | `list_unique_indexes` (:200) | fuzzy 0.67 |
| `listTables` (db/lite-tables.ts:47) | `list_tables` (:273) | exact |

🟥 **TS symbols not resolved into this file (2):** `LiteTableSpecWithReplicationStatus`, `ZqlSpecOptions`

🟦 **Rust-only added here (12):** `NOT_NULL_ATTRIBUTE`, `ReplicaVersions`, `TEXT_ARRAY_ATTRIBUTE`, `TEXT_ENUM_ATTRIBUTE`, `lite_type_to_zql_value_type`, `open_replica_read_only`, `read_min_row_versions`, `read_replica_versions`, `read_replica_versions_from_path`, `read_table_spec`, `validate_client_schema`, `zql_type_for_upstream`

### `http_server.rs`  ⟵  _(new)_


🟦 **Rust-only added here (14):** `HttpServerState`, `ServerStats`, `bind_http_listener`, `census_handler`, `check_admin_auth`, `check_notify_request`, `heapz_handler`, `metrics_handler`, `notify_broadcast_handler`, `notify_handler`, `readyz_handler`, `run_http_server`, `serve_http`, `statz_handler`

### `live_count.rs`  ⟵  `workers/connection.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `Connection` (workers/connection.ts:78) | `CONNECTION` (:29) | exact |

🟦 **Rust-only added here (11):** `CLIENT_GROUP`, `Guard`, `PUSHER`, `SYNC_ENGINE`, `WS_MESSAGE_HANDLER`, `dec`, `drop`, `drop_backtrace`, `inc`, `new`, `snapshot`

### `main.rs`  ⟵  `services/view-syncer/connection-context-manager.ts`, `services/view-syncer/view-syncer.ts`, `workers/syncer.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `changeDesiredQueries` (services/view-syncer/view-syncer.ts:138) | `change_desired_queries` (:852) | exact |
| `deleteClients` (services/view-syncer/view-syncer.ts:143) | `delete_clients` (:855) | exact |
| `initConnection` (services/view-syncer/connection-context-manager.ts:111) | `init_connection` (:862) | exact |
| `inspect` (services/view-syncer/view-syncer.ts:148) | `inspect` (:865) | exact |
| `mustGetConnectionContext` (services/view-syncer/connection-context-manager.ts:152) | `must_get_connection_context` (:871) | exact |
| `Syncer` (workers/syncer.ts:288) | `SyncerConfig` (:36) | fuzzy 0.50 |
| `updateAuth` (services/view-syncer/connection-context-manager.ts:116) | `update_auth` (:853) | exact |
| `ViewSyncer` (services/view-syncer/view-syncer.ts:132) | `create_view_syncer` (:725) | fuzzy 0.67 |
| `ViewSyncerService` (services/view-syncer/view-syncer.ts:214) | `PlaceholderViewSyncer` (:849) | fuzzy 0.50 |

🟦 **Rust-only added here (16):** `ALLOC`, `PlaceholderConnContextManager`, `RealServicesFactory`, `ShutdownSignal`, `cgroup_cpu_quota_cores`, `create_conn_context_manager`, `create_mutagen`, `create_pusher`, `create_sync_engine_config`, `from_env`, `host_parallelism`, `main`, `parse_cpu_max`, `parse_cpu_max_quota_shapes`, `parse_query_config`, `warn_if_quota_capped`

### `metrics.rs`  ⟵  `custom/fetch.ts`, `services/view-syncer/pipeline-driver.ts`, `services/view-syncer/query-covering.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `add` (services/view-syncer/query-covering.ts:67) | `add` (:839) | exact |
| `apiAttemptDuration` (custom/fetch.ts:568) | `API_DURATION_BOUNDARIES_S` (:361) | fuzzy 0.50 |
| `apiInFlight` (custom/fetch.ts:116) | `record_api_in_flight` (:455) | fuzzy 0.75 |
| `apiRequestMetricAttrs` (custom/fetch.ts:516) | `api_request_metric_attrs` (:398) | exact |
| `recordApiAttempt` (custom/fetch.ts:549) | `record_api_attempt` (:423) | exact |
| `reset` (services/view-syncer/pipeline-driver.ts:343) | `record_reset` (:865) | fuzzy 0.50 |

🟥 **TS symbols not resolved into this file (1):** `FetchMetricsOptions`

🟦 **Rust-only added here (63):** `ApiOtel`, `C`, `CvrAttemptOtel`, `G`, `GAUGES`, `HIST_BOUNDS_SECS`, `Histogram`, `I`, `INSTRUMENT`, `INSTRUMENTS`, `Metrics`, `OTEL_LATENCY_BOUNDARIES_S`, `Otel`, `QueryTransformOtel`, `ServingLagOtel`, `WS_QUEUED_BYTES`, `WS_QUEUED_FRAMES`, `active_clients`, `cvr_flush_failures`, `default`, `failed_client_groups`, `fmt`, `now_ms`, `observe_millis`, `observe_secs`, `proto_attr`, `record_active_client_delta`, `record_advance`, `record_api_request`, `record_api_request_duration`, `record_cvr_flush_attempt`, `record_cvr_flush_failure`, `record_cvr_load_attempt`, `record_e2e_serving_lag`, `record_e2e_serving_lag_clamp`, `record_fail_group`, `record_hydration`, `record_query_transformation`, `record_query_transformation_hash_change`, `record_query_transformation_no_op`, `record_query_transformation_time`, `record_view_syncer_hydration`, `record_view_syncer_lag_ms`, `record_ws_connection_attempt`, `record_ws_connection_failure`, `record_ws_connection_success`, `record_ws_open_delta`, `record_ws_queued_bytes_delta`, `record_ws_queued_delta`, `record_ws_shed`, `register_cvr_pool_gauges`, `register_serving_lag_gauges`, `render`, `render_prometheus`, `view_syncer_hydration_otel`, `view_syncer_lag_otel`, `ws_connection_attempts`, `ws_connection_failures`, `ws_connection_successes`, `ws_open_connections`, `ws_queued_bytes_gauge`, `ws_queued_frames_gauge`, `ws_sheds`

### `otel.rs`  ⟵  _(new)_


🟦 **Rust-only added here (3):** `NATIVE_HISTOGRAM_INSTRUMENTS`, `init_metrics`, `metrics_enabled`

### `protocol.rs`  ⟵  `auth/auth.ts`, `custom/fetch.ts`, `services/view-syncer/view-syncer.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `apiFailedBody` (custom/fetch.ts:411) | `PushFailedHttpBody` (:127) | fuzzy 0.40 |
| `isAuthErrorBody` (auth/auth.ts:211) | `ErrorBody` (:192) | fuzzy 0.67 |
| `isTransformFailedError` (services/view-syncer/view-syncer.ts:2897) | `TransformFailedHttpBody` (:165) | fuzzy 0.40 |
| `legacyPushErrorReason` (custom/fetch.ts:484) | `ErrorReason` (:64) | fuzzy 0.50 |

🟦 **Rust-only added here (51):** `AckMutationResponsesBody`, `AnalyzeQueryOptions`, `BackoffBody`, `BasicErrorBody`, `ChangeDesiredQueriesBody`, `ConnectedBody`, `ConnectedMessage`, `DecodeError`, `DeleteClientsBody`, `ErrorKind`, `ErrorMessage`, `ErrorOrigin`, `InitConnectionBody`, `InspectUpBody`, `MIN_SERVER_SUPPORTED_SYNC_PROTOCOL`, `MutationID`, `MutationPatchOp`, `PROTOCOL_VERSION`, `PokeEndBody`, `PokePartBody`, `PokeStartBody`, `PongBody`, `PongMessage`, `PushBody`, `PushFailedServerBody`, `PushFailedZeroCacheBody`, `QueriesClearOp`, `QueriesDelOp`, `QueriesPatchOp`, `QueriesPutOp`, `RowPatchOp`, `SchemaVersions`, `SecProtocols`, `TransformFailedServerBody`, `TransformFailedZeroCacheBody`, `UpdateAuthBody`, `Upstream`, `basic`, `client_not_found`, `decode_sec_protocols`, `downstream_message`, `internal`, `invalid_message`, `invalid_push`, `kind`, `message`, `parse_upstream`, `parse_upstream_array`, `rehome`, `unauthorized`, `version_not_supported`

### `push_relay.rs`  ⟵  `custom/fetch.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `getBodyPreview` (custom/fetch.ts:62) | `BODY_PREVIEW_CAP` (:40) | fuzzy 0.67 |

🟦 **Rust-only added here (15):** `CLEANUP_RESULTS_MUTATION_NAME`, `DEFAULT_QUEUE_CAP`, `HttpRelayPusher`, `PushTarget`, `QueuedPush`, `RELAY_TIMEOUT`, `ack_mutation_responses`, `cleanup_push_body`, `delete_client_mutations`, `enqueue_payload`, `enqueue_push`, `mutation_ids_of`, `queue_cap`, `read_body_preview`, `relay_body`

### `router.rs`  ⟵  `services/view-syncer/connection-context-manager.ts`, `services/view-syncer/pipeline-driver.ts`, `services/view-syncer/view-syncer.ts`, `workers/connection.ts`, `workers/syncer.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `checkClientAndCVRVersions` (services/view-syncer/view-syncer.ts:2875) | `check_client_and_cvr_versions` (:56) | exact |
| `closeConnection` (services/view-syncer/connection-context-manager.ts:136) | `close_connection` (:2848) | exact |
| `drain` (workers/syncer.ts:732) | `drain` (:1019) | exact |
| `GroupAuthState` (services/view-syncer/connection-context-manager.ts:95) | `GroupAuthState` (:372) | exact |
| `queryCount` (services/view-syncer/view-syncer.ts:658) | `query_count` (:1614) | exact |
| `rowCount` (services/view-syncer/view-syncer.ts:662) | `row_count` (:1620) | exact |
| `send` (workers/connection.ts:348) | `send` (:190) | exact |
| `servedVersion` (services/view-syncer/view-syncer.ts:666) | `mark_version_served` (:2915) | fuzzy 0.67 |
| `servingLagEligible` (services/view-syncer/view-syncer.ts:670) | `serving_lag_eligible` (:1608) | exact |
| `TTL_CLOCK_INTERVAL` (services/view-syncer/view-syncer.ts:202) | `get_ttl_clock` (:1644) | fuzzy 0.67 |
| `TTL_TIMER_HYSTERESIS` (services/view-syncer/view-syncer.ts:210) | `TTL_TIMER_HYSTERESIS_MS` (:46) | fuzzy 0.75 |

🟥 **TS symbols not resolved into this file (2):** `SyncContext`, `TimeSliceTimer`

🟦 **Rust-only added here (77):** `AuthValidator`, `CGHandle`, `CGMessage`, `CGServicesFactory`, `CG_KEEPALIVE_MS`, `CgMapCleanup`, `CgState`, `CgTaskContext`, `ConnectionInfo`, `ConnectionRouter`, `ConnectionSinks`, `CvrPgConfig`, `Executor`, `ExecutorCommand`, `MAX_DRAIN_MS`, `MAX_TTL_MS`, `SyncEngineConfig`, `T`, `apply_client_deletions`, `arm_auth_maintenance`, `arm_serving_lag`, `broadcast_notification`, `cg_count`, `cg_event_loop`, `check_and_pin_user`, `clients_to_delete`, `connection_count`, `decrement_active_client`, `decrement_nonzero`, `default_num_shards`, `default_query_context`, `dispatch_cg_message`, `drop_registration`, `ensure_cvr`, `executor_loop`, `fail_group`, `fail_group_with_error`, `filtered_query_headers`, `forward_inbound`, `get_or_create_cg`, `handle_connection`, `handle_desired_queries`, `handle_inspect`, `handle_update_auth`, `idle_shutdown_due`, `insert_for_test`, `inspect_queries_value`, `lock_unpoisoned`, `maybe_reload_permissions`, `merge_notifications`, `metrics_prometheus`, `metrics_snapshot`, `new_sharded`, `new_with_accepting`, `new_with_limit`, `next_auth_maintenance_delay`, `next_expiry_delay`, `next_idle_shutdown_delay`, `older_replica_error`, `on_auth_maintenance_tick`, `on_connection_closed`, `on_expiry_tick`, `on_inbound`, `on_new_connection`, `on_notification`, `parse_desired_queries_patch`, `place_cg`, `publish_serving_lag`, `reset_pipelines_and_rehydrate`, `run_executor`, `send_error_if_current`, `send_notification`, `serving_lag_registry`, `shard_for`, `shutdown`, `slow_hydrate_threshold_ms`, `str_array`

### `services/view_syncer/connection_context_manager.rs`  ⟵  `auth/auth.ts`, `services/view-syncer/connection-context-manager.ts`, `services/view-syncer/pipeline-driver.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `Auth` (auth/auth.ts:25) | `Auth` (:82) | exact |
| `authEquals` (auth/auth.ts:36) | `auth_equals` (:339) | exact |
| `compareByInsertionOrder` (services/view-syncer/connection-context-manager.ts:844) | `compare_by_insertion_order` (:900) | exact |
| `comparePreferredValidatedConnection` (services/view-syncer/connection-context-manager.ts:851) | `compare_preferred_validated_connection` (:907) | exact |
| `ConnectionContext` (services/view-syncer/connection-context-manager.ts:65) | `ConnectionContext` (:95) | exact |
| `ConnectionContextManager` (services/view-syncer/connection-context-manager.ts:104) | `ConnectionContextManager` (:368) | exact |
| `ConnectionFetchContext` (services/view-syncer/connection-context-manager.ts:54) | `ConnectionFetchContext` (:73) | exact |
| `ConnectionSelector` (services/view-syncer/connection-context-manager.ts:37) | `ConnectionSelector` (:49) | exact |
| `ConnectionState` (services/view-syncer/connection-context-manager.ts:17) | `ConnectionState` (:29) | exact |
| `ConnectionValidation` (services/view-syncer/connection-context-manager.ts:30) | `ConnectionValidation` (:42) | exact |
| `deferMaintenance` (services/view-syncer/connection-context-manager.ts:147) | `defer_maintenance` (:650) | exact |
| `failConnection` (services/view-syncer/connection-context-manager.ts:132) | `fail_connection` (:604) | exact |
| `fetch` (services/view-syncer/pipeline-driver.ts:1428) | `FetchConfig` (:135) | fuzzy 0.50 |
| `getBackgroundConnectionContext` (services/view-syncer/connection-context-manager.ts:156) | `get_background_connection_context` (:688) | exact |
| `getConnectionContext` (services/view-syncer/connection-context-manager.ts:149) | `get_connection_context` (:666) | exact |
| `getGroupState` (services/view-syncer/connection-context-manager.ts:159) | `get_group_state` (:701) | exact |
| `HeaderOptions` (services/view-syncer/connection-context-manager.ts:44) | `HeaderOptions` (:64) | exact |
| `markBackgroundRetransformSuccess` (services/view-syncer/connection-context-manager.ts:140) | `mark_background_retransform_success` (:620) | exact |
| `minDefined` (services/view-syncer/connection-context-manager.ts:858) | `min_defined` (:891) | exact |
| `mustGetBackgroundConnectionContext` (services/view-syncer/connection-context-manager.ts:157) | `must_get_background_connection_context` (:693) | exact |
| `pickToken` (auth/auth.ts:126) | `pick_token` (:273) | exact |
| `planMaintenance` (services/view-syncer/connection-context-manager.ts:161) | `plan_maintenance` (:707) | exact |
| `registerConnection` (services/view-syncer/connection-context-manager.ts:105) | `register_connection` (:410) | exact |
| `setSharedRetransformReady` (services/view-syncer/connection-context-manager.ts:145) | `set_shared_retransform_ready` (:640) | exact |
| `UserState` (services/view-syncer/connection-context-manager.ts:23) | `UserState` (:36) | exact |
| `validateConnection` (services/view-syncer/connection-context-manager.ts:121) | `validate_connection` (:540) | exact |

🟥 **TS symbols not resolved into this file (4):** `ConnectionContextManagerImpl`, `JWTAuth`, `OpaqueAuth`, `ValidateLegacyJWT`

🟦 **Rust-only added here (18):** `CCMError`, `ConnectParamsForRegistration`, `JwtPayload`, `MaintenanceKind`, `MaintenancePlan`, `ValidationResult`, `build_fetch_context`, `demote_connection`, `next_revalidate_at`, `now`, `raw`, `refresh_background_connection_context`, `remove_connection_internal`, `resolve_auth`, `set_background_connection`, `store_connection`, `to_error_body`, `update_background_retransform_deadline`

### `services/view_syncer/drain_coordinator.rs`  ⟵  `services/view-syncer/drain-coordinator.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `DrainCoordinator` (services/view-syncer/drain-coordinator.ts:31) | `DrainCoordinator` (:39) | exact |
| `draining` (services/view-syncer/drain-coordinator.ts:37) | `is_draining` (:105) | fuzzy 1.00 |
| `drainNextIn` (services/view-syncer/drain-coordinator.ts:45) | `drain_next_in` (:66) | exact |
| `forceDrainTimeout` (services/view-syncer/drain-coordinator.ts:66) | `force_drain_timeout` (:85) | exact |
| `nextDrainTime` (services/view-syncer/drain-coordinator.ts:71) | `next_drain_time` (:110) | exact |
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

### `services/view_syncer/pipeline_driver.rs`  ⟵  `custom-queries/transform-query.ts`, `db/lite-tables.ts`, `services/view-syncer/pipeline-driver.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `advance` (services/view-syncer/pipeline-driver.ts:923) | `advance` (:457) | exact |
| `buildPrimaryKeys` (services/view-syncer/pipeline-driver.ts:1520) | `set_client_primary_keys` (:373) | fuzzy 0.50 |
| `currentVersion` (services/view-syncer/pipeline-driver.ts:395) | `current_version` (:214) | exact |
| `destroy` (custom-queries/transform-query.ts:98) | `destroy` (:553) | exact |
| `getRow` (services/view-syncer/pipeline-driver.ts:906) | `get_row` (:535) | exact |
| `init` (services/view-syncer/pipeline-driver.ts:325) | `init` (:226) | exact |
| `initialized` (services/view-syncer/pipeline-driver.ts:334) | `initialized` (:209) | exact |
| `mustGetTableSpec` (db/lite-tables.ts:326) | `IvmTableSpec` (:49) | fuzzy 0.50 |
| `removeQuery` (services/view-syncer/pipeline-driver.ts:834) | `remove_query` (:381) | exact |
| `rowSetSignature` (services/view-syncer/pipeline-driver.ts:874) | `row_set_signature` (:541) | exact |

🟥 **TS symbols not resolved into this file (6):** `PipelineDriver`, `PipelineHydrationReason`, `RowAdd`, `RowEdit`, `RowRemove`, `Timer`

🟦 **Rust-only added here (27):** `AdvanceOutcome`, `IvmColumnSchema`, `IvmPipelines`, `TsAst`, `TsBound`, `TsCondition`, `TsCorrelatedSubquery`, `TsCorrelation`, `TsValuePosition`, `active_query_ids`, `build_engine`, `column_schema`, `column_type`, `convert_ast`, `convert_condition`, `convert_csq`, `convert_value_position`, `has_query`, `hydrate`, `init_from_connection`, `json_to_value`, `panic_message`, `parse_ts_ast`, `query_transformation_hash`, `running_queries`, `scalar_reset_message`, `set_query_transformation_hash`

### `services/view_syncer/query_covering.rs`  ⟵  `services/view-syncer/query-covering.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
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

### `sync_engine.rs`  ⟵  `services/view-syncer/pipeline-driver.ts`, `services/view-syncer/view-syncer.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `accumulate` (services/view-syncer/pipeline-driver.ts:1276) | `accumulate_signature` (:1621) | fuzzy 0.50 |
| `hasExpiredQueries` (services/view-syncer/view-syncer.ts:2933) | `remove_expired_queries` (:1401) | fuzzy 0.50 |
| `RowChange` (services/view-syncer/pipeline-driver.ts:83) | `row_change_to_maps` (:1586) | fuzzy 0.67 |

🟦 **Rust-only added here (38):** `LoadCvrError`, `MAX_FLUSH_ATTEMPTS`, `SyncResult`, `ZERO_VERSION_COLUMN`, `advance_and_sync`, `advance_poke_targets`, `catchup_clients`, `catchup_floor`, `client_primary_keys_from_schema`, `clients_for`, `config_and_hydrate`, `config_and_hydrate_with_profile`, `config_poke_targets`, `empty_cvr`, `existing_rows`, `fail_client`, `flush_ops_to_store`, `flush_to_store`, `gather_catchup_patches`, `hydrate_and_sync`, `inspect_queries`, `load_cvr`, `offload`, `parse_existing_rows`, `pipelines`, `query_name_of`, `register_client`, `row_op_is_noop`, `row_to_contents`, `seed_signatures_from_cvr`, `send_inspect_response`, `set_cvr_store`, `set_enable_query_covering`, `set_tokio_handle`, `signature_provider`, `sqlite_real_to_json`, `unregister_client`, `value_to_serde_json`

### `trace.rs`  ⟵  _(new)_


🟦 **Rust-only added here (2):** `ENABLED`, `note`

### `workers/connect_params.rs`  ⟵  `workers/connect-params.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `ConnectParams` (workers/connect-params.ts:9) | `ConnectParams` (:10) | exact |
| `getConnectParams` (workers/connect-params.ts:45) | `get_connect_params` (:50) | exact |

🟦 **Rust-only added here (6):** `ConnectParamsError`, `extract_protocol_version`, `get_boolean`, `get_integer`, `get_string`, `parse_js_integer`

### `workers/connection.rs`  ⟵  `workers/connection.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `close` (workers/connection.ts:168) | `close` (:231) | exact |
| `handleInitConnection` (workers/connection.ts:190) | `handle_init_connection` (:306) | exact |
| `handleMessage` (workers/connection.ts:52) | `handle_message` (:42) | exact |
| `HandlerResult` (workers/connection.ts:31) | `HandlerResult` (:26) | exact |
| `MessageHandler` (workers/connection.ts:51) | `MessageHandler` (:39) | exact |
| `sendError` (workers/connection.ts:356) | `send_error` (:256) | exact |

🟥 **TS symbols not resolved into this file (1):** `StreamResult`

🟦 **Rust-only added here (13):** `DOWNSTREAM_MSG_INTERVAL_MS`, `LogLevel`, `WsState`, `classify_error_log_level`, `client_id`, `close_with_error`, `handle_close`, `handle_error`, `handle_inbound`, `handle_result`, `is_closed`, `maybe_send_pong`, `ws_id`

### `workers/syncer.rs`  ⟵  `workers/syncer.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `boundReplicaReadyStates` (workers/syncer.ts:82) | `bound_replica_ready_states` (:83) | exact |
| `computeMaxServingLagMs` (workers/syncer.ts:247) | `compute_max_serving_lag_ms` (:232) | exact |
| `computeServingLagDistributionMs` (workers/syncer.ts:174) | `compute_serving_lag_distribution_ms` (:175) | exact |
| `computeServingLagStatsMs` (workers/syncer.ts:226) | `compute_serving_lag_stats_ms` (:223) | exact |
| `findFirstUnservedIndex` (workers/syncer.ts:138) | `find_first_unserved_index` (:141) | exact |
| `lowerBoundReplicaReadyTimeMs` (workers/syncer.ts:104) | `lower_bound_replica_ready_time_ms` (:105) | exact |
| `MAX_REPLICA_READY_STATES` (workers/syncer.ts:76) | `MAX_REPLICA_READY_STATES` (:19) | exact |
| `percentileNearestRank` (workers/syncer.ts:160) | `percentile_nearest_rank` (:160) | exact |
| `pruneReplicaReadyStates` (workers/syncer.ts:93) | `prune_replica_ready_states` (:92) | exact |
| `ReplicaReadyState` (workers/syncer.ts:52) | `ReplicaReadyState` (:26) | exact |
| `ServingLagStats` (workers/syncer.ts:62) | `ServingLagStats` (:43) | exact |
| `ServingLagViewSyncer` (workers/syncer.ts:57) | `ServingLagViewSyncer` (:35) | exact |
| `upperBoundWatermark` (workers/syncer.ts:121) | `upper_bound_watermark` (:124) | exact |

🟥 **TS symbols not resolved into this file (1):** `SyncerWorkerData`

🟦 **Rust-only added here (12):** `CgServingSnapshot`, `DISTRIBUTION_CACHE_TTL_MS`, `ServingLagDistribution`, `VIEW_SYNCER_LAG_SAMPLE_INTERVAL_MS`, `active_client_groups`, `compute_serving_lag_distribution`, `record_replica_ready_state`, `remove_view_syncer`, `stats`, `total_queries`, `total_rows`, `upsert_view_syncer`

### `workers/syncer_ws_message_handler.rs`  ⟵  `workers/syncer-ws-message-handler.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `SyncerWsMessageHandler` (workers/syncer-ws-message-handler.ts:36) | `SyncerWsMessageHandler` (:155) | exact |
| `withTraceparent` (workers/syncer-ws-message-handler.ts:28) | `with_traceparent` (:25) | exact |

🟦 **Rust-only added here (9):** `ConnContextInfo`, `ConnContextManagerDispatch`, `MutagenDispatch`, `PushOverride`, `PushRelayHeaders`, `PusherDispatch`, `ViewSyncerDispatch`, `handle_push`, `process_mutation`

### `ws_server.rs`  ⟵  `workers/connect-params.ts`, `workers/syncer.ts`


🟦 **Rust-only added here (21):** `DEFAULT_DOWNSTREAM_BYTE_HWM`, `DEFAULT_DOWNSTREAM_QUEUE_HWM`, `DEFAULT_LIVENESS_TIMEOUT_MS`, `DEFAULT_MAX_PAYLOAD_BYTES`, `KEEPALIVE_CHECK_INTERVAL_MS`, `NODE_SINGLETON_HEADERS`, `WsServerConfig`, `accept_connection`, `accept_connection_with_limit`, `bind_ws_listener`, `downstream_byte_hwm`, `downstream_queue_hwm`, `is_expected_disconnect`, `liveness_timeout_ms`, `now_epoch_ms`, `run_ws_reader`, `run_ws_server`, `run_ws_writer`, `send_error_and_close`, `serve_ws`, `serve_ws_with_config`

### `ws_sink.rs`  ⟵  `workers/connection.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `WebSocketLike` (workers/connection.ts:361) | `DirectWebSocketSink` (:77) | fuzzy 0.40 |

🟦 **Rust-only added here (10):** `SinkLimits`, `WsCommand`, `cancel`, `count_shed_once`, `fail`, `push`, `push_serializable`, `push_sized`, `send_command`, `with_limits`

## 3 · Flat one-to-one symbol map (every TS symbol resolved)

| TS symbol | origin | → Rust | status |
|---|---|---|---|
| `JWTAuth` | auth/auth.ts:14 | — | 🟥 UNRESOLVED |
| `OpaqueAuth` | auth/auth.ts:20 | — | 🟥 UNRESOLVED |
| `Auth` | auth/auth.ts:25 | `Auth` services/view_syncer/connection_context_manager.rs:82 | ✅ exact |
| `ValidateLegacyJWT` | auth/auth.ts:27 | — | 🟥 UNRESOLVED |
| `isProvidedAuth` | auth/auth.ts:32 | services/view_syncer/connection_context_manager.rs is_some_and non-empty | 📌 inlined |
| `authEquals` | auth/auth.ts:36 | `auth_equals` services/view_syncer/connection_context_manager.rs:339 | ✅ exact |
| `pickToken` | auth/auth.ts:126 | `pick_token` services/view_syncer/connection_context_manager.rs:273 | ✅ exact |
| `isAuthErrorBody` | auth/auth.ts:211 | `ErrorBody` protocol.rs:192 | 🔁 rename 0.67 |
| `getRemoteKeyset` | auth/jwt.ts:32 | auth/jwt.rs JWKS_CACHE/lookup_cached_jwk | 📌 cached remote JWKS |
| `tokenConfigOptions` | auth/jwt.ts:41 | — | 🟥 UNRESOLVED |
| `loadJwk` | auth/jwt.ts:73 | auth/jwt.rs serde_json::from_str | 📌 parse JWK |
| `loadSecret` | auth/jwt.ts:77 | auth/jwt.rs DecodingKey::from_secret | 📌 secret key |
| `verifyTokenImpl` | auth/jwt.ts:81 | auth/jwt.rs verify_sync/verify_with_jwk(s) | 📌 JWT verify (split sync/async) |
| `TransformedAndHashed` | auth/read-authorizer.ts:10 | — | 🟥 UNRESOLVED |
| `transformAndHashQuery` | auth/read-authorizer.ts:24 | `transform_and_hash_query` auth/read_authorizer.rs:63 | ✅ exact |
| `transformQuery` | auth/read-authorizer.ts:45 | `transform_query` auth/read_authorizer.rs:79 | ✅ exact |
| `transformQueryInternal` | auth/read-authorizer.ts:61 | `transform_query_internal` auth/read_authorizer.rs:86 | ✅ exact |
| `addRulesToWhere` | auth/read-authorizer.ts:105 | `add_rules_to_where` auth/read_authorizer.rs:135 | ✅ exact |
| `transformCondition` | auth/read-authorizer.ts:127 | `transform_condition` auth/read_authorizer.rs:146 | ✅ exact |
| `TransformResponse` | custom-queries/transform-query.ts:35 | — | 🟥 UNRESOLVED |
| `HashedTransformResponse` | custom-queries/transform-query.ts:43 | — | 🟥 UNRESOLVED |
| `CustomQueryTransformer` | custom-queries/transform-query.ts:82 | `CustomQueryContext` custom_queries/transform_query.rs:48 | 🔁 rename 0.50 |
| `destroy` | custom-queries/transform-query.ts:98 | `destroy` services/view_syncer/pipeline_driver.rs:553 | ✅ exact |
| `validate` | custom-queries/transform-query.ts:111 | `validate_auth` auth/jwt.rs:381 | 🔁 rename 0.50 |
| `transform` | custom-queries/transform-query.ts:117 | `post_transform` custom_queries/transform_query.rs:278 | 🔁 rename 0.50 |
| `getCacheKey` | custom-queries/transform-query.ts:259 | `get_cache_key` custom_queries/transform_query.rs:482 | ✅ exact |
| `normalizedHeaders` | custom-queries/transform-query.ts:278 | `normalized_headers` custom_queries/transform_query.rs:475 | ✅ exact |
| `compileUrlPattern` | custom/fetch.ts:52 | N/A | 📌 no separate compile step; url_match matches the raw pattern inline |
| `getBodyPreview` | custom/fetch.ts:62 | `BODY_PREVIEW_CAP` push_relay.rs:40 | 🔁 rename 0.67 |
| `FetchMetricsOptions` | custom/fetch.ts:92 | — | 🟥 UNRESOLVED |
| `apiInFlight` | custom/fetch.ts:116 | `record_api_in_flight` metrics.rs:455 | 🔁 rename 0.75 |
| `urlMatch` | custom/fetch.ts:389 | `url_match` custom_queries/transform_query.rs:121 | ✅ exact |
| `getBackoffDelayMs` | custom/fetch.ts:407 | `get_backoff_delay_ms` custom_queries/transform_query.rs:261 | ✅ exact |
| `apiFailedBody` | custom/fetch.ts:411 | `PushFailedHttpBody` protocol.rs:127 | 🔁 rename 0.40 |
| `apiErrorFromResult` | custom/fetch.ts:462 | custom_queries/transform_query.rs response validation | 📌 error extraction |
| `legacyPushErrorReason` | custom/fetch.ts:484 | `ErrorReason` protocol.rs:64 | 🔁 rename 0.50 |
| `apiRequestMetricAttrs` | custom/fetch.ts:516 | `api_request_metric_attrs` metrics.rs:398 | ✅ exact |
| `apiResponseErrorMetricAttrs` | custom/fetch.ts:528 | metrics.rs record_api_attempt attrs | 📌 status attrs |
| `recordApiAttempt` | custom/fetch.ts:549 | `record_api_attempt` metrics.rs:423 | ✅ exact |
| `apiAttempts` | custom/fetch.ts:567 | metrics.rs record_api_attempt | 📌 OTel counter |
| `apiAttemptDuration` | custom/fetch.ts:568 | `API_DURATION_BOUNDARIES_S` metrics.rs:361 | 🔁 rename 0.50 |
| `LiteTableSpecWithReplicationStatus` | db/lite-tables.ts:37 | — | 🟥 UNRESOLVED |
| `listTables` | db/lite-tables.ts:47 | `list_tables` db/lite_tables.rs:273 | ✅ exact |
| `listIndexes` | db/lite-tables.ts:141 | `list_unique_indexes` db/lite_tables.rs:200 | 🔁 rename 0.67 |
| `ZqlSpecOptions` | db/lite-tables.ts:184 | — | 🟥 UNRESOLVED |
| `computeZqlSpecs` | db/lite-tables.ts:210 | `compute_zql_specs` db/lite_tables.rs:79 | ✅ exact |
| `computeZqlSpecsFromLiteSpecs` | db/lite-tables.ts:227 | `compute_table_specs_from_path` db/lite_tables.rs:73 | 🔁 rename 0.43 |
| `mustGetTableSpec` | db/lite-tables.ts:326 | `IvmTableSpec` services/view_syncer/pipeline_driver.rs:49 | 🔁 rename 0.50 |
| `keyCmp` | db/lite-tables.ts:343 | db/lite_tables.rs sort_by len-then-lex | 📌 inlined key compare |
| `ConnectionState` | services/view-syncer/connection-context-manager.ts:17 | `ConnectionState` services/view_syncer/connection_context_manager.rs:29 | ✅ exact |
| `UserState` | services/view-syncer/connection-context-manager.ts:23 | `UserState` services/view_syncer/connection_context_manager.rs:36 | ✅ exact |
| `ConnectionValidation` | services/view-syncer/connection-context-manager.ts:30 | `ConnectionValidation` services/view_syncer/connection_context_manager.rs:42 | ✅ exact |
| `ConnectionSelector` | services/view-syncer/connection-context-manager.ts:37 | `ConnectionSelector` services/view_syncer/connection_context_manager.rs:49 | ✅ exact |
| `HeaderOptions` | services/view-syncer/connection-context-manager.ts:44 | `HeaderOptions` services/view_syncer/connection_context_manager.rs:64 | ✅ exact |
| `ConnectionFetchContext` | services/view-syncer/connection-context-manager.ts:54 | `ConnectionFetchContext` services/view_syncer/connection_context_manager.rs:73 | ✅ exact |
| `ConnectionContext` | services/view-syncer/connection-context-manager.ts:65 | `ConnectionContext` services/view_syncer/connection_context_manager.rs:95 | ✅ exact |
| `GroupAuthState` | services/view-syncer/connection-context-manager.ts:95 | `GroupAuthState` router.rs:372 | ✅ exact |
| `ConnectionContextManager` | services/view-syncer/connection-context-manager.ts:104 | `ConnectionContextManager` services/view_syncer/connection_context_manager.rs:368 | ✅ exact |
| `registerConnection` | services/view-syncer/connection-context-manager.ts:105 | `register_connection` services/view_syncer/connection_context_manager.rs:410 | ✅ exact |
| `initConnection` | services/view-syncer/connection-context-manager.ts:111 | `init_connection` main.rs:862 | ✅ exact |
| `updateAuth` | services/view-syncer/connection-context-manager.ts:116 | `update_auth` main.rs:853 | ✅ exact |
| `validateConnection` | services/view-syncer/connection-context-manager.ts:121 | `validate_connection` services/view_syncer/connection_context_manager.rs:540 | ✅ exact |
| `failConnection` | services/view-syncer/connection-context-manager.ts:132 | `fail_connection` services/view_syncer/connection_context_manager.rs:604 | ✅ exact |
| `closeConnection` | services/view-syncer/connection-context-manager.ts:136 | `close_connection` router.rs:2848 | ✅ exact |
| `markBackgroundRetransformSuccess` | services/view-syncer/connection-context-manager.ts:140 | `mark_background_retransform_success` services/view_syncer/connection_context_manager.rs:620 | ✅ exact |
| `setSharedRetransformReady` | services/view-syncer/connection-context-manager.ts:145 | `set_shared_retransform_ready` services/view_syncer/connection_context_manager.rs:640 | ✅ exact |
| `deferMaintenance` | services/view-syncer/connection-context-manager.ts:147 | `defer_maintenance` services/view_syncer/connection_context_manager.rs:650 | ✅ exact |
| `getConnectionContext` | services/view-syncer/connection-context-manager.ts:149 | `get_connection_context` services/view_syncer/connection_context_manager.rs:666 | ✅ exact |
| `mustGetConnectionContext` | services/view-syncer/connection-context-manager.ts:152 | `must_get_connection_context` main.rs:871 | ✅ exact |
| `getBackgroundConnectionContext` | services/view-syncer/connection-context-manager.ts:156 | `get_background_connection_context` services/view_syncer/connection_context_manager.rs:688 | ✅ exact |
| `mustGetBackgroundConnectionContext` | services/view-syncer/connection-context-manager.ts:157 | `must_get_background_connection_context` services/view_syncer/connection_context_manager.rs:693 | ✅ exact |
| `getGroupState` | services/view-syncer/connection-context-manager.ts:159 | `get_group_state` services/view_syncer/connection_context_manager.rs:701 | ✅ exact |
| `planMaintenance` | services/view-syncer/connection-context-manager.ts:161 | `plan_maintenance` services/view_syncer/connection_context_manager.rs:707 | ✅ exact |
| `ConnectionContextManagerImpl` | services/view-syncer/connection-context-manager.ts:176 | — | 🟥 UNRESOLVED |
| `compareByInsertionOrder` | services/view-syncer/connection-context-manager.ts:844 | `compare_by_insertion_order` services/view_syncer/connection_context_manager.rs:900 | ✅ exact |
| `comparePreferredValidatedConnection` | services/view-syncer/connection-context-manager.ts:851 | `compare_preferred_validated_connection` services/view_syncer/connection_context_manager.rs:907 | ✅ exact |
| `minDefined` | services/view-syncer/connection-context-manager.ts:858 | `min_defined` services/view_syncer/connection_context_manager.rs:891 | ✅ exact |
| `sameConnectionSelector` | services/view-syncer/connection-context-manager.ts:868 | services/view_syncer/connection_context_manager.rs set_background_connection | 📌 inlined tuple match |
| `filterHeaders` | services/view-syncer/connection-context-manager.ts:875 | router.rs filtered_query_headers | 📌 header allowlist |
| `DrainCoordinator` | services/view-syncer/drain-coordinator.ts:31 | `DrainCoordinator` services/view_syncer/drain_coordinator.rs:39 | ✅ exact |
| `draining` | services/view-syncer/drain-coordinator.ts:37 | `is_draining` services/view_syncer/drain_coordinator.rs:105 | 🔁 rename 1.00 |
| `shouldDrain` | services/view-syncer/drain-coordinator.ts:41 | `should_drain` services/view_syncer/drain_coordinator.rs:57 | ✅ exact |
| `drainNextIn` | services/view-syncer/drain-coordinator.ts:45 | `drain_next_in` services/view_syncer/drain_coordinator.rs:66 | ✅ exact |
| `forceDrainTimeout` | services/view-syncer/drain-coordinator.ts:66 | `force_drain_timeout` services/view_syncer/drain_coordinator.rs:85 | ✅ exact |
| `nextDrainTime` | services/view-syncer/drain-coordinator.ts:71 | `next_drain_time` services/view_syncer/drain_coordinator.rs:110 | ✅ exact |
| `PendingUpstreamCommit` | services/view-syncer/e2e-serving-lag.ts:3 | `PendingUpstreamCommit` services/view_syncer/e2e_serving_lag.rs:14 | ✅ exact |
| `E2EServingLagTracker` | services/view-syncer/e2e-serving-lag.ts:19 | `E2EServingLagTracker` services/view_syncer/e2e_serving_lag.rs:29 | ✅ exact |
| `pending` | services/view-syncer/e2e-serving-lag.ts:22 | `pending` services/view_syncer/e2e_serving_lag.rs:38 | ✅ exact |
| `onVersionReady` | services/view-syncer/e2e-serving-lag.ts:35 | `on_version_ready` services/view_syncer/e2e_serving_lag.rs:50 | ✅ exact |
| `onVersionServed` | services/view-syncer/e2e-serving-lag.ts:55 | `on_version_served` services/view_syncer/e2e_serving_lag.rs:72 | ✅ exact |
| `Observation` | services/view-syncer/e2e-serving-lag.ts:77 | `Observation` services/view_syncer/e2e_serving_lag.rs:21 | ✅ exact |
| `RowAdd` | services/view-syncer/pipeline-driver.ts:77 | — | 🟥 UNRESOLVED |
| `RowRemove` | services/view-syncer/pipeline-driver.ts:79 | — | 🟥 UNRESOLVED |
| `RowEdit` | services/view-syncer/pipeline-driver.ts:81 | — | 🟥 UNRESOLVED |
| `RowChange` | services/view-syncer/pipeline-driver.ts:83 | `row_change_to_maps` sync_engine.rs:1586 | 🔁 rename 0.67 |
| `PipelineHydrationReason` | services/view-syncer/pipeline-driver.ts:123 | — | 🟥 UNRESOLVED |
| `Timer` | services/view-syncer/pipeline-driver.ts:158 | — | 🟥 UNRESOLVED |
| `randomID` | services/view-syncer/pipeline-driver.ts:176 | N/A | 📌 TS pipelineRunID debug-correlation id; not ported |
| `projectedAdvancementTimeMs` | services/view-syncer/pipeline-driver.ts:180 | rust-ivm advance_gate.rs | 📌 ported |
| `advancementResetTimeLimitMs` | services/view-syncer/pipeline-driver.ts:191 | rust-ivm advance_gate.rs | 📌 ported |
| `minProjectedAdvancementSampleChanges` | services/view-syncer/pipeline-driver.ts:195 | rust-ivm advance_gate.rs | 📌 ported |
| `shouldResetProjectedAdvancement` | services/view-syncer/pipeline-driver.ts:205 | rust-ivm advance_gate.rs | 📌 ported |
| `shouldFinishLateAdvancement` | services/view-syncer/pipeline-driver.ts:228 | rust-ivm advance_gate.rs | 📌 ported |
| `shouldResetSlowCurrentChange` | services/view-syncer/pipeline-driver.ts:238 | rust-ivm advance_gate.rs | 📌 ported |
| `PipelineDriver` | services/view-syncer/pipeline-driver.ts:251 | — | 🟥 UNRESOLVED |
| `init` | services/view-syncer/pipeline-driver.ts:325 | `init` services/view_syncer/pipeline_driver.rs:226 | ✅ exact |
| `initialized` | services/view-syncer/pipeline-driver.ts:334 | `initialized` services/view_syncer/pipeline_driver.rs:209 | ✅ exact |
| `reset` | services/view-syncer/pipeline-driver.ts:343 | `record_reset` metrics.rs:865 | 🔁 rename 0.50 |
| `replicaVersion` | services/view-syncer/pipeline-driver.ts:386 | pipeline_driver.rs snapshotter current_version | 📌 field/getter |
| `currentVersion` | services/view-syncer/pipeline-driver.ts:395 | `current_version` services/view_syncer/pipeline_driver.rs:214 | ✅ exact |
| `currentPermissions` | services/view-syncer/pipeline-driver.ts:403 | router.rs/message_handler perms reload | 📌 perms hot-reload at CG dispatch |
| `advanceWithoutDiff` | services/view-syncer/pipeline-driver.ts:422 | pipeline_driver.rs advance_without_diff | 📌 ported |
| `queries` | services/view-syncer/pipeline-driver.ts:458 | pipeline_driver.rs running_queries/active_query_ids | 📌 split getters |
| `totalHydrationTimeMs` | services/view-syncer/pipeline-driver.ts:462 | rust-ivm engine total_hydration_time_ms | 📌 ported (cross-crate) |
| `addQuery` | services/view-syncer/pipeline-driver.ts:574 | rust-ivm engine add_queries | 📌 streaming add (cross-crate) |
| `removeQuery` | services/view-syncer/pipeline-driver.ts:834 | `remove_query` services/view_syncer/pipeline_driver.rs:381 | ✅ exact |
| `rowSetSignature` | services/view-syncer/pipeline-driver.ts:874 | `row_set_signature` services/view_syncer/pipeline_driver.rs:541 | ✅ exact |
| `getRow` | services/view-syncer/pipeline-driver.ts:906 | `get_row` services/view_syncer/pipeline_driver.rs:535 | ✅ exact |
| `advance` | services/view-syncer/pipeline-driver.ts:923 | `advance` services/view_syncer/pipeline_driver.rs:457 | ✅ exact |
| `accumulate` | services/view-syncer/pipeline-driver.ts:1276 | `accumulate_signature` sync_engine.rs:1621 | 🔁 rename 0.50 |
| `setOutput` | services/view-syncer/pipeline-driver.ts:1416 | rust-ivm operator set_output | 📌 trait method (cross-crate) |
| `getSchema` | services/view-syncer/pipeline-driver.ts:1420 | rust-ivm operator get_schema | 📌 trait method (cross-crate) |
| `fetch` | services/view-syncer/pipeline-driver.ts:1428 | `FetchConfig` services/view_syncer/connection_context_manager.rs:135 | 🔁 rename 0.50 |
| `logQueryFailure` | services/view-syncer/pipeline-driver.ts:1451 | inlined | 📌 streamer error callback lives in rust-ivm; failures logged via tracing at the call sites |
| `getRowKey` | services/view-syncer/pipeline-driver.ts:1482 | rust-ivm streamer get_row_key | 📌 row-key extraction (cross-crate) |
| `buildPrimaryKeys` | services/view-syncer/pipeline-driver.ts:1520 | `set_client_primary_keys` services/view_syncer/pipeline_driver.rs:373 | 🔁 rename 0.50 |
| `mustGetPrimaryKey` | services/view-syncer/pipeline-driver.ts:1530 | rust-ivm engine build | 📌 PK validated on build |
| `assert` | services/view-syncer/pipeline-driver.ts:1537 | Rust assert! macro | 📌 idiom |
| `scalarValuesEqual` | services/view-syncer/pipeline-driver.ts:1553 | rust-ivm engine scalar_values_equal | 📌 ported (cross-crate) |
| `RunningQuery` | services/view-syncer/query-covering.ts:15 | `RunningQuery` services/view_syncer/query_covering.rs:24 | ✅ exact |
| `CoveringQuery` | services/view-syncer/query-covering.ts:21 | `CoveringQuery` services/view_syncer/query_covering.rs:33 | ✅ exact |
| `isQueryCoveredBy` | services/view-syncer/query-covering.ts:40 | `is_query_covered_by` services/view_syncer/query_covering.rs:97 | ✅ exact |
| `findCoveringQuery` | services/view-syncer/query-covering.ts:44 | `find_covering_query` services/view_syncer/query_covering.rs:106 | ✅ exact |
| `QueryCoveringIndex` | services/view-syncer/query-covering.ts:55 | `QueryCoveringIndex` services/view_syncer/query_covering.rs:120 | ✅ exact |
| `add` | services/view-syncer/query-covering.ts:67 | `add` metrics.rs:839 | ✅ exact |
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
| `ViewSyncer` | services/view-syncer/view-syncer.ts:132 | `create_view_syncer` main.rs:725 | 🔁 rename 0.67 |
| `changeDesiredQueries` | services/view-syncer/view-syncer.ts:138 | `change_desired_queries` main.rs:852 | ✅ exact |
| `deleteClients` | services/view-syncer/view-syncer.ts:143 | `delete_clients` main.rs:855 | ✅ exact |
| `inspect` | services/view-syncer/view-syncer.ts:148 | `inspect` main.rs:865 | ✅ exact |
| `SyncContext` | services/view-syncer/view-syncer.ts:165 | — | 🟥 UNRESOLVED |
| `shutdownBeforeInitializationError` | services/view-syncer/view-syncer.ts:181 | router.rs init-fail path | 📌 error on terminal init failure |
| `TTL_CLOCK_INTERVAL` | services/view-syncer/view-syncer.ts:202 | `get_ttl_clock` router.rs:1644 | 🔁 rename 0.67 |
| `TTL_TIMER_HYSTERESIS` | services/view-syncer/view-syncer.ts:210 | `TTL_TIMER_HYSTERESIS_MS` router.rs:46 | 🔁 rename 0.75 |
| `ViewSyncerService` | services/view-syncer/view-syncer.ts:214 | `PlaceholderViewSyncer` main.rs:849 | 🔁 rename 0.50 |
| `readyState` | services/view-syncer/view-syncer.ts:521 | router.rs CgState/event loop | 📌 init/drain state flags |
| `run` | services/view-syncer/view-syncer.ts:528 | router.rs cg_event_loop | 📌 per-CG async serving loop |
| `queryCount` | services/view-syncer/view-syncer.ts:658 | `query_count` router.rs:1614 | ✅ exact |
| `rowCount` | services/view-syncer/view-syncer.ts:662 | `row_count` router.rs:1620 | ✅ exact |
| `servedVersion` | services/view-syncer/view-syncer.ts:666 | `mark_version_served` router.rs:2915 | 🔁 rename 0.67 |
| `servingLagEligible` | services/view-syncer/view-syncer.ts:670 | `serving_lag_eligible` router.rs:1608 | ✅ exact |
| `keepalive` | services/view-syncer/view-syncer.ts:702 | router.rs CgState.keepalive_until | 📌 field + next_idle_shutdown_delay |
| `stop` | services/view-syncer/view-syncer.ts:2802 | router.rs shutdown() | 📌 per-CG drain + Rehome |
| `markInitialized` | services/view-syncer/view-syncer.ts:2838 | router.rs CgState.terminal | 📌 init-state flag; test helper dropped |
| `yieldProcess` | services/view-syncer/view-syncer.ts:2861 | N/A | 📌 tokio async yield; no global-lock setImmediate |
| `contentsAndVersion` | services/view-syncer/view-syncer.ts:2865 | sync_engine.rs (strip _0_version) | 📌 inlined |
| `checkClientAndCVRVersions` | services/view-syncer/view-syncer.ts:2875 | `check_client_and_cvr_versions` router.rs:56 | ✅ exact |
| `isTransformFailedError` | services/view-syncer/view-syncer.ts:2897 | `TransformFailedHttpBody` protocol.rs:165 | 🔁 rename 0.40 |
| `expired` | services/view-syncer/view-syncer.ts:2908 | router.rs remove_expired_queries | 📌 TTL/inactivation expiry |
| `hasExpiredQueries` | services/view-syncer/view-syncer.ts:2933 | `remove_expired_queries` sync_engine.rs:1401 | 🔁 rename 0.50 |
| `TimeSliceTimer` | services/view-syncer/view-syncer.ts:2943 | — | 🟥 UNRESOLVED |
| `start` | services/view-syncer/view-syncer.ts:2952 | router.rs ensure_cvr/CgState init | 📌 CVR load + ttl seed |
| `startWithoutYielding` | services/view-syncer/view-syncer.ts:2959 | N/A | 📌 no setImmediate; sync Instant::now start |
| `elapsedLap` | services/view-syncer/view-syncer.ts:2976 | N/A | 📌 per-lap timing via Instant::elapsed() inline |
| `totalElapsed` | services/view-syncer/view-syncer.ts:2997 | N/A | 📌 inline Instant::elapsed accumulation |
| `ConnectParams` | workers/connect-params.ts:9 | `ConnectParams` workers/connect_params.rs:10 | ✅ exact |
| `normalizeHeaders` | workers/connect-params.ts:32 | ws_server.rs (dup-header join) | 📌 header normalization |
| `getConnectParams` | workers/connect-params.ts:45 | `get_connect_params` workers/connect_params.rs:50 | ✅ exact |
| `HandlerResult` | workers/connection.ts:31 | `HandlerResult` workers/connection.rs:26 | ✅ exact |
| `StreamResult` | workers/connection.ts:45 | — | 🟥 UNRESOLVED |
| `MessageHandler` | workers/connection.ts:51 | `MessageHandler` workers/connection.rs:39 | ✅ exact |
| `handleMessage` | workers/connection.ts:52 | `handle_message` workers/connection.rs:42 | ✅ exact |
| `Connection` | workers/connection.ts:78 | `CONNECTION` live_count.rs:29 | ✅ exact |
| `close` | workers/connection.ts:168 | `close` workers/connection.rs:231 | ✅ exact |
| `handleInitConnection` | workers/connection.ts:190 | `handle_init_connection` workers/connection.rs:306 | ✅ exact |
| `send` | workers/connection.ts:348 | `send` router.rs:190 | ✅ exact |
| `sendError` | workers/connection.ts:356 | `send_error` workers/connection.rs:256 | ✅ exact |
| `WebSocketLike` | workers/connection.ts:361 | `DirectWebSocketSink` ws_sink.rs:77 | 🔁 rename 0.40 |
| `findProtocolError` | workers/connection.ts:433 | workers/connection.rs classify_error_log_level | 📌 protocol-error classify |
| `hasErrno` | workers/connection.ts:443 | N/A | 📌 Node `'errno' in e`; Rust WS stack has no errno |
| `hasTransientSocketCode` | workers/connection.ts:466 | N/A | 📌 Node EPIPE/ECONNRESET/ECANCELED; no errno in tungstenite |
| `isTransientSocketMessage` | workers/connection.ts:477 | workers/connection.rs (message substring) | 📌 transient downgrade |
| `withTraceparent` | workers/syncer-ws-message-handler.ts:28 | `with_traceparent` workers/syncer_ws_message_handler.rs:25 | ✅ exact |
| `SyncerWsMessageHandler` | workers/syncer-ws-message-handler.ts:36 | `SyncerWsMessageHandler` workers/syncer_ws_message_handler.rs:155 | ✅ exact |
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
| `Syncer` | workers/syncer.ts:288 | `SyncerConfig` main.rs:36 | 🔁 rename 0.50 |
| `drain` | workers/syncer.ts:732 | `drain` router.rs:1019 | ✅ exact |
