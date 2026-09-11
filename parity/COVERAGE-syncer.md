# rust-syncer — Layer-2 (body-differential) coverage

_COVERED = reachable (transitive closure over the crate call graph, incl. fn-pointer edges like `.sort_by(cmp_condition)` / `.any(is_always_false)`) from a differential harness: the in-crate `*_parity_against_ts` fixtures (jwt / read-authorizer hash goldens / url_match / query_covering / serving_lag / e2e_serving_lag / parse_int) + the phase/rowkey/stage integration tests. Reachability ≠ every-branch-exercised._

- Rust fns total **650** · ✅ COVERED **585** · 🟥 GAP (pure, untested) **31** · ⚙️ IO (integration diff) **24** · ◻️ infra/metrics **7** · ◻️ documented n/a **3**
- Body-differential coverage of the **unit-testable pure surface**: **585/616 = 95%**

> ⚠️ **Highest-risk uncovered (build rowKeys/schemas / classify / mutate state — the corruption class):** `client_primary_keys_from_schema` (services/view_syncer/view_syncer.rs), `merge` (tdigest.rs), `optional_client_schema` (protocol/client_schema.rs)

## 🟥 GAP — pure & deterministic, NO differential fixture (build these) — 31

| fn | file | signature |
|---|---|---|
| `deserialize` | protocol.rs | `fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {` |
| `optional_no_null` | protocol.rs | `pub fn optional_no_null<'de, D, T>(d: D) -> Result<Option<T>, D::Error>` |
| `serialize` | protocol.rs | `fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {` |
| `optional_client_schema` | protocol/client_schema.rs | `pub fn optional_client_schema<'de, D>(d: D) -> Result<Option<Value>, D::Error>` |
| `expecting` | protocol/version.rs | `fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {` |
| `visit_none` | protocol/version.rs | `fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {` |
| `visit_str` | protocol/version.rs | `fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {` |
| `visit_unit` | protocol/version.rs | `fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {` |
| `create_log_context` | server/logging.rs | `pub fn create_log_context(worker: &str, worker_index: u32) {` |
| `format_event` | server/logging.rs | `fn format_event(` |
| `record_bool` | server/logging.rs | `fn record_bool(&mut self, field: &Field, value: bool) {` |
| `record_debug` | server/logging.rs | `fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {` |
| `record_error` | server/logging.rs | `fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {` |
| `record_f64` | server/logging.rs | `fn record_f64(&mut self, field: &Field, value: f64) {` |
| `record_i64` | server/logging.rs | `fn record_i64(&mut self, field: &Field, value: i64) {` |
| `record_str` | server/logging.rs | `fn record_str(&mut self, field: &Field, value: &str) {` |
| `record_u64` | server/logging.rs | `fn record_u64(&mut self, field: &Field, value: u64) {` |
| `init_metrics` | server/otel_start.rs | `pub fn init_metrics(service_version: &str) -> Option<SdkMeterProvider> {` |
| `metrics_enabled` | server/otel_start.rs | `fn metrics_enabled() -> bool {` |
| `client_primary_keys_from_schema` | services/view_syncer/view_syncer.rs | `fn client_primary_keys_from_schema(` |
| `fail_client` | services/view_syncer/view_syncer.rs | `pub fn fail_client(&self, ws_id: &str, msg: &str) -> bool {` |
| `set_enable_query_covering` | services/view_syncer/view_syncer.rs | `pub fn set_enable_query_covering(&mut self, enabled: bool) {` |
| `add_centroid_list` | tdigest.rs | `pub fn add_centroid_list(&mut self, centroid_list: Vec<Centroid>) {` |
| `byte_size_for_compression` | tdigest.rs | `pub fn byte_size_for_compression(comp: f64) -> f64 {` |
| `centroids` | tdigest.rs | `pub fn centroids(&mut self) -> Vec<Centroid> {` |
| `from_json` | tdigest.rs | `pub fn from_json(data: &[f64]) -> Result<Self, String> {` |
| `merge` | tdigest.rs | `pub fn merge(&mut self, t2: &mut TDigest) {` |
| `active_client_groups` | workers/syncer.rs | `pub fn active_client_groups(&self) -> usize {` |
| `metrics_snapshot` | workers/syncer.rs | `pub fn metrics_snapshot(&self) -> serde_json::Value {` |
| `send_notification` | workers/syncer.rs | `pub fn send_notification(&self, cg_id: &str, notification: serde_json::Value) -> bool {` |
| `total_client_groups` | workers/syncer.rs | `pub fn total_client_groups(&self) -> u64 {` |

## ◻️ NON-DIFFERENTIABLE — documented n/a (no un-pinned body) — 3

| fn | file | why not a body-differential |
|---|---|---|
| `compute_serving_lag_distribution` | workers/syncer.rs | gathers live registry snapshots then calls the already-pinned `compute_serving_lag_distribution_ms` (serving_lag_parity_against_ts); the wrapper reads DashMap state, no un-pinned math |
| `total_queries` | workers/syncer.rs | trivial getter — sums query counts over the registry snapshots |
| `total_rows` | workers/syncer.rs | trivial getter — sums row counts over the registry snapshots |

## ✅ COVERED — body pinned to TS fixture — 585

| fn | file | signature |
|---|---|---|
| `route_sqlite_malloc_through_mimalloc` | alloc.rs | `pub fn route_sqlite_malloc_through_mimalloc() -> Result<(), c_int> {` |
| `saturating_c_int` | alloc.rs | `fn saturating_c_int(n: usize) -> c_int {` |
| `as_str` | ast_to_zql.rs | `fn as_str(v: Option<&Value>) -> &str {` |
| `ast_to_zql` | ast_to_zql.rs | `pub fn ast_to_zql(ast: &Value) -> String {` |
| `extract_relationship_name` | ast_to_zql.rs | `fn extract_relationship_name(related: &Value) -> String {` |
| `get_next_exists_subquery` | ast_to_zql.rs | `fn get_next_exists_subquery(related: &Value) -> &Value {` |
| `has_sub_query_props` | ast_to_zql.rs | `fn has_sub_query_props(sub: &Value) -> bool {` |
| `transform_condition` | ast_to_zql.rs | `fn transform_condition(condition: &Value, prefix: &str, args: &mut BTreeSet<String>) ->…` |
| `transform_exists_condition` | ast_to_zql.rs | `fn transform_exists_condition(` |
| `transform_literal` | ast_to_zql.rs | `fn transform_literal(literal: &Value) -> String {` |
| `transform_logical_condition` | ast_to_zql.rs | `fn transform_logical_condition(` |
| `transform_order` | ast_to_zql.rs | `fn transform_order(order_by: &[Value]) -> String {` |
| `transform_parameter` | ast_to_zql.rs | `fn transform_parameter(param: &Value) -> String {` |
| `transform_related` | ast_to_zql.rs | `fn transform_related(related: &Value) -> String {` |
| `transform_simple_condition` | ast_to_zql.rs | `fn transform_simple_condition(condition: &Value, prefix: &str) -> String {` |
| `transform_value_position` | ast_to_zql.rs | `fn transform_value_position(value: &Value) -> String {` |
| `apply_claim_validation` | auth/jwt.rs | `fn apply_claim_validation(` |
| `decode_jwt_claims` | auth/jwt.rs | `pub fn decode_jwt_claims(token: &str) -> Value {` |
| `has_config` | auth/jwt.rs | `fn has_config(&self) -> bool {` |
| `key_algorithm_to_signature_alg` | auth/jwt.rs | `fn key_algorithm_to_signature_alg(` |
| `lookup_cached_jwk` | auth/jwt.rs | `fn lookup_cached_jwk(url: &str, kid: Option<&str>) -> Option<Jwk> {` |
| `lookup_stale_cached_jwk` | auth/jwt.rs | `fn lookup_stale_cached_jwk(url: &str, kid: Option<&str>) -> Option<Jwk> {` |
| `select_jwk` | auth/jwt.rs | `fn select_jwk<'a>(set: &'a JwkSet, kid: Option<&str>) -> Option<&'a Jwk> {` |
| `validate_auth` | auth/jwt.rs | `async fn validate_auth(` |
| `verify_sync` | auth/jwt.rs | `fn verify_sync(&self, token: &str, user_id: &str) -> Result<(), String> {` |
| `verify_with_jwk` | auth/jwt.rs | `fn verify_with_jwk(` |
| `verify_with_jwks` | auth/jwt.rs | `async fn verify_with_jwks(` |
| `within_refetch_cooldown` | auth/jwt.rs | `fn within_refetch_cooldown(url: &str) -> bool {` |
| `deny_all_permissions` | auth/load_permissions.rs | `pub fn deny_all_permissions() -> Value {` |
| `load_permissions` | auth/load_permissions.rs | `pub fn load_permissions(conn: &Connection, app_id: &str) -> Result<LoadedPermissions, S…` |
| `reload_permissions_if_changed` | auth/load_permissions.rs | `pub fn reload_permissions_if_changed(` |
| `resolve_permissions` | auth/load_permissions.rs | `pub fn resolve_permissions(loaded: Result<Option<Value>, String>) -> Option<Value> {` |
| `validate_condition_value` | auth/load_permissions.rs | `fn validate_condition_value(` |
| `validate_permission_asset` | auth/load_permissions.rs | `fn validate_permission_asset(value: &Value, path: &str) -> Result<(), String> {` |
| `validate_permission_condition` | auth/load_permissions.rs | `fn validate_permission_condition(value: &Value, path: &str) -> Result<(), String> {` |
| `validate_permissions_config` | auth/load_permissions.rs | `fn validate_permissions_config(value: &Value) -> Result<(), String> {` |
| `validate_policy` | auth/load_permissions.rs | `fn validate_policy(value: &Value, path: &str) -> Result<(), String> {` |
| `validate_related_subquery` | auth/load_permissions.rs | `fn validate_related_subquery(related: &Map<String, Value>, path: &str) -> Result<(), St…` |
| `add_rules_to_where` | auth/read_authorizer.rs | `fn add_rules_to_where(where_opt: Option<Value>, rule_conditions: Vec<Value>) -> Value {` |
| `base36` | auth/read_authorizer.rs | `fn base36(mut n: u64) -> String {` |
| `bind_condition` | auth/read_authorizer.rs | `fn bind_condition(cond: &Value, static_params: &Value) -> Value {` |
| `bind_static_parameters` | auth/read_authorizer.rs | `pub fn bind_static_parameters(ast: &Value, auth_data: &Value) -> Value {` |
| `bind_value` | auth/read_authorizer.rs | `fn bind_value(value: &Value, static_params: &Value) -> Value {` |
| `bind_visit` | auth/read_authorizer.rs | `fn bind_visit(ast: &Value, static_params: &Value) -> Value {` |
| `cmp_condition` | auth/read_authorizer.rs | `fn cmp_condition(a: &Value, b: &Value) -> std::cmp::Ordering {` |
| `cmp_optional_bool` | auth/read_authorizer.rs | `fn cmp_optional_bool(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {` |
| `cmp_related` | auth/read_authorizer.rs | `fn cmp_related(a: &Value, b: &Value) -> std::cmp::Ordering {` |
| `compare_utf8_maybe_null` | auth/read_authorizer.rs | `fn compare_utf8_maybe_null(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {` |
| `compare_value_position` | auth/read_authorizer.rs | `fn compare_value_position(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {` |
| `ctype` | auth/read_authorizer.rs | `fn ctype(c: &Value) -> &str {` |
| `flatten` | auth/read_authorizer.rs | `fn flatten(kind: &str, conditions: Vec<Value>) -> Vec<Value> {` |
| `flattened` | auth/read_authorizer.rs | `fn flattened(cond: &Value) -> Option<Value> {` |
| `hash_of_ast` | auth/read_authorizer.rs | `pub fn hash_of_ast(ast: &Value) -> String {` |
| `hash_of_name_and_args` | auth/read_authorizer.rs | `pub fn hash_of_name_and_args(name: &str, args: &[Value]) -> String {` |
| `insert_if_present` | auth/read_authorizer.rs | `fn insert_if_present(out: &mut Map<String, Value>, key: &str, v: Option<&Value>) {` |
| `is_always_false` | auth/read_authorizer.rs | `fn is_always_false(c: &Value) -> bool {` |
| `is_always_true` | auth/read_authorizer.rs | `fn is_always_true(c: &Value) -> bool {` |
| `js_string` | auth/read_authorizer.rs | `fn js_string(v: Option<&Value>) -> String {` |
| `normalize_ast` | auth/read_authorizer.rs | `pub fn normalize_ast(ast: &Value) -> Value {` |
| `normalize_related_entry` | auth/read_authorizer.rs | `fn normalize_related_entry(r: &Value) -> Value {` |
| `normalize_where` | auth/read_authorizer.rs | `fn normalize_where(cond: &Value) -> Value {` |
| `resolve_field` | auth/read_authorizer.rs | `fn resolve_field(anchor: Option<&Value>, field: Option<&Value>) -> Value {` |
| `simplify_condition` | auth/read_authorizer.rs | `pub fn simplify_condition(c: Value) -> Value {` |
| `transform_and_hash_query` | auth/read_authorizer.rs | `pub fn transform_and_hash_query(` |
| `transform_query` | auth/read_authorizer.rs | `pub fn transform_query(query: &Value, permissions: &Value, auth_data: &Value) -> Value {` |
| `transform_query_internal` | auth/read_authorizer.rs | `fn transform_query_internal(query: &Value, permissions: &Value) -> Value {` |
| `apply_runtime_debug_flags` | config/zero_config.rs | `pub fn apply_runtime_debug_flags(&self) {` |
| `cgroup_cpu_quota_cores` | config/zero_config.rs | `fn cgroup_cpu_quota_cores() -> Option<usize> {` |
| `from_env` | config/zero_config.rs | `pub fn from_env() -> Self {` |
| `host_parallelism` | config/zero_config.rs | `pub fn host_parallelism() -> usize {` |
| `is_admin_password_valid` | config/zero_config.rs | `pub fn is_admin_password_valid(` |
| `parse_cpu_max` | config/zero_config.rs | `pub(crate) fn parse_cpu_max(s: &str) -> Option<usize> {` |
| `parse_query_config` | config/zero_config.rs | `fn parse_query_config() -> Option<crate::FetchConfig> {` |
| `validated_app_id` | config/zero_config.rs | `fn validated_app_id(app_id: String) -> String {` |
| `warn_if_quota_capped` | config/zero_config.rs | `pub fn warn_if_quota_capped() {` |
| `get_backoff_delay_ms` | custom/fetch.rs | `pub(crate) fn get_backoff_delay_ms(attempt: u32) -> u64 {` |
| `read_body_preview` | custom/fetch.rs | `pub(crate) async fn read_body_preview(resp: reqwest::Response, cap: usize) -> Option<St…` |
| `url_match` | custom/fetch.rs | `pub fn url_match(pattern: &str, url: &str) -> bool {` |
| `api_attempt_attrs` | custom/metrics.rs | `pub fn api_attempt_attrs(` |
| `api_otel` | custom/metrics.rs | `fn api_otel() -> &'static ApiOtel {` |
| `api_request_attrs` | custom/metrics.rs | `pub fn api_request_attrs(` |
| `from_body` | custom/metrics.rs | `pub fn from_body(body: &str) -> Option<ApiErrorAttrs> {` |
| `push_response_error_attrs` | custom/metrics.rs | `fn push_response_error_attrs(` |
| `record_api_attempt` | custom/metrics.rs | `pub fn record_api_attempt(` |
| `record_api_in_flight` | custom/metrics.rs | `pub fn record_api_in_flight(delta: i64) {` |
| `record_api_request` | custom/metrics.rs | `pub fn record_api_request(` |
| `__test_backdate_transform_cache_sweep_clock` | custom_queries/transform_query.rs | `pub fn __test_backdate_transform_cache_sweep_clock(by: Duration) {` |
| `__test_cache_timings` | custom_queries/transform_query.rs | `pub fn __test_cache_timings() -> (Duration, Duration) {` |
| `__test_insert_aged` | custom_queries/transform_query.rs | `pub fn __test_insert_aged(ctx: &CustomQueryContext, id: &str, q: &TransformedQuery, age…` |
| `__test_reset_transform_cache` | custom_queries/transform_query.rs | `pub fn __test_reset_transform_cache() {` |
| `__test_transform_cache_state` | custom_queries/transform_query.rs | `pub fn __test_transform_cache_state() -> (usize, std::time::Duration) {` |
| `cache_get` | custom_queries/transform_query.rs | `fn cache_get(ctx: &CustomQueryContext, id: &str) -> Option<TransformedQuery> {` |
| `cache_set` | custom_queries/transform_query.rs | `fn cache_set(ctx: &CustomQueryContext, id: &str, q: &TransformedQuery) {` |
| `composed_headers` | custom_queries/transform_query.rs | `pub fn composed_headers(&self) -> Vec<(String, String)> {` |
| `extract_transform_queries` | custom_queries/transform_query.rs | `fn extract_transform_queries(response: &Value) -> Option<Vec<Value>> {` |
| `get_cache_key` | custom_queries/transform_query.rs | `fn get_cache_key(ctx: &CustomQueryContext, id: &str) -> String {` |
| `is_auth_error_body` | custom_queries/transform_query.rs | `pub fn is_auth_error_body(body: &Value) -> bool {` |
| `normalized_headers` | custom_queries/transform_query.rs | `fn normalized_headers(headers: &[(String, String)]) -> String {` |
| `post_transform_attempts` | custom_queries/transform_query.rs | `async fn post_transform_attempts(` |
| `request_transform` | custom_queries/transform_query.rs | `async fn request_transform(` |
| `seed_transform_cache_for_test` | custom_queries/transform_query.rs | `pub fn seed_transform_cache_for_test(ctx: &CustomQueryContext, id: &str, q: &Transforme…` |
| `set_header` | custom_queries/transform_query.rs | `fn set_header(headers: &mut Vec<(String, String)>, name: &str, value: String) {` |
| `transform` | custom_queries/transform_query.rs | `pub async fn transform(` |
| `validate` | custom_queries/transform_query.rs | `pub async fn validate(` |
| `validation_of` | custom_queries/transform_query.rs | `fn validation_of(response: &Value) -> ConnectionValidation {` |
| `compute_table_specs_from_path` | db/lite_tables.rs | `pub fn compute_table_specs_from_path(replica_path: &str) -> Result<Vec<IvmTableSpec>, S…` |
| `compute_zql_specs` | db/lite_tables.rs | `pub fn compute_zql_specs(` |
| `default` | db/lite_tables.rs | `fn default() -> Self {` |
| `list_tables` | db/lite_tables.rs | `fn list_tables(conn: &Connection) -> Result<Vec<String>, String> {` |
| `list_unique_indexes` | db/lite_tables.rs | `fn list_unique_indexes(conn: &Connection) -> Result<HashMap<String, Vec<Vec<String>>>, …` |
| `lite_table_name` | db/lite_tables.rs | `fn lite_table_name(schema: &str, table: &str) -> String {` |
| `lite_type_string` | db/lite_tables.rs | `pub fn lite_type_string(` |
| `lite_type_to_zql_value_type` | db/lite_tables.rs | `pub fn lite_type_to_zql_value_type(lite_type: &str) -> Option<&'static str> {` |
| `open_replica_read_only` | db/lite_tables.rs | `pub fn open_replica_read_only(replica_path: &str) -> Result<Connection, String> {` |
| `read_min_row_versions` | db/lite_tables.rs | `fn read_min_row_versions(conn: &Connection) -> Result<HashMap<String, String>, String> {` |
| `read_replica_versions` | db/lite_tables.rs | `pub fn read_replica_versions(conn: &Connection) -> Result<ReplicaVersions, String> {` |
| `read_replica_versions_from_path` | db/lite_tables.rs | `pub fn read_replica_versions_from_path(replica_path: &str) -> Result<ReplicaVersions, S…` |
| `read_table_spec` | db/lite_tables.rs | `fn read_table_spec(` |
| `zql_type_for_upstream` | db/lite_tables.rs | `fn zql_type_for_upstream(pg_type: &str) -> Option<&'static str> {` |
| `column` | db/specs.rs | `pub fn column(&self, name: &str) -> Option<&LiteColumnSpec> {` |
| `census_handler` | http_server.rs | `async fn census_handler() -> impl IntoResponse {` |
| `check_admin_auth` | http_server.rs | `fn check_admin_auth(` |
| `dec` | live_count.rs | `pub fn dec(c: &AtomicI64) {` |
| `drop` | live_count.rs | `fn drop(&mut self) {` |
| `drop_backtrace` | live_count.rs | `pub fn drop_backtrace(context: &str) {` |
| `inc` | live_count.rs | `pub fn inc(c: &AtomicI64) {` |
| `new` | live_count.rs | `pub fn new(counter: &'static AtomicI64) -> Self {` |
| `snapshot` | live_count.rs | `pub fn snapshot() -> String {` |
| `active_clients` | observability/metrics.rs | `fn active_clients() -> &'static UpDownCounter<i64> {` |
| `add` | observability/metrics.rs | `pub fn add(field: &AtomicU64, n: u64) {` |
| `cvr_flush_failures` | observability/metrics.rs | `fn cvr_flush_failures() -> &'static Counter<u64> {` |
| `failed_client_groups` | observability/metrics.rs | `fn failed_client_groups() -> &'static Counter<u64> {` |
| `fmt` | observability/metrics.rs | `fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {` |
| `lock_wait_time_otel` | observability/metrics.rs | `fn lock_wait_time_otel() -> &'static OtelHistogram<f64> {` |
| `mutation_otel` | observability/metrics.rs | `fn mutation_otel() -> &'static MutationOtel {` |
| `now_ms` | observability/metrics.rs | `fn now_ms() -> i64 {` |
| `proto_attr` | observability/metrics.rs | `fn proto_attr(protocol_version: u32) -> KeyValue {` |
| `query_transform_otel` | observability/metrics.rs | `fn query_transform_otel() -> &'static QueryTransformOtel {` |
| `record_active_client_delta` | observability/metrics.rs | `pub fn record_active_client_delta(delta: i64, protocol_version: u32) {` |
| `record_advance` | observability/metrics.rs | `pub fn record_advance(&self, process_time_ms: f64) {` |
| `record_client_protocol_version` | observability/metrics.rs | `pub fn record_client_protocol_version(protocol_version: u32) {` |
| `record_cvr_flush_failure` | observability/metrics.rs | `pub fn record_cvr_flush_failure() {` |
| `record_e2e_serving_lag` | observability/metrics.rs | `pub fn record_e2e_serving_lag(lag_ms: f64) {` |
| `record_e2e_serving_lag_clamp` | observability/metrics.rs | `pub fn record_e2e_serving_lag_clamp() {` |
| `record_fail_group` | observability/metrics.rs | `pub fn record_fail_group(reason: &'static str) {` |
| `record_hydration` | observability/metrics.rs | `pub fn record_hydration(&self, elapsed_ms: f64) {` |
| `record_lock_wait_ms` | observability/metrics.rs | `pub fn record_lock_wait_ms(elapsed_ms: f64) {` |
| `record_push` | observability/metrics.rs | `pub fn record_push(client_group_id: &str, mutation_count: u64) {` |
| `record_query_transformation` | observability/metrics.rs | `pub fn record_query_transformation(success: bool) {` |
| `record_query_transformation_hash_change` | observability/metrics.rs | `pub fn record_query_transformation_hash_change() {` |
| `record_query_transformation_no_op` | observability/metrics.rs | `pub fn record_query_transformation_no_op() {` |
| `record_query_transformation_time` | observability/metrics.rs | `pub fn record_query_transformation_time(elapsed_ms: f64) {` |
| `record_reset` | observability/metrics.rs | `pub fn record_reset(&self, reason: &str) {` |
| `record_same_hash_rehydration_version_bump` | observability/metrics.rs | `pub fn record_same_hash_rehydration_version_bump(reason: &'static str) {` |
| `record_websocket_error` | observability/metrics.rs | `pub fn record_websocket_error(event_type: &'static str, protocol_version: u32) {` |
| `record_ws_connection_failure` | observability/metrics.rs | `pub fn record_ws_connection_failure(protocol_version: u32, reason: ConnectionFailureRea…` |
| `record_ws_connection_success` | observability/metrics.rs | `pub fn record_ws_connection_success(protocol_version: u32) {` |
| `record_ws_open_delta` | observability/metrics.rs | `pub fn record_ws_open_delta(delta: i64, protocol_version: u32) {` |
| `record_ws_queued_bytes_delta` | observability/metrics.rs | `pub fn record_ws_queued_bytes_delta(delta: i64) {` |
| `record_ws_queued_delta` | observability/metrics.rs | `pub fn record_ws_queued_delta(delta: i64) {` |
| `record_ws_shed` | observability/metrics.rs | `pub fn record_ws_shed(reason: &'static str) {` |
| `serving_lag_otel` | observability/metrics.rs | `fn serving_lag_otel() -> &'static ServingLagOtel {` |
| `view_syncer_hydration_otel` | observability/metrics.rs | `fn view_syncer_hydration_otel() -> &'static OtelHistogram<f64> {` |
| `ws_connection_failures` | observability/metrics.rs | `fn ws_connection_failures() -> &'static Counter<u64> {` |
| `ws_connection_successes` | observability/metrics.rs | `fn ws_connection_successes() -> &'static Counter<u64> {` |
| `ws_errors` | observability/metrics.rs | `fn ws_errors() -> &'static Counter<u64> {` |
| `ws_open_connections` | observability/metrics.rs | `fn ws_open_connections() -> &'static UpDownCounter<i64> {` |
| `ws_queued_bytes_gauge` | observability/metrics.rs | `fn ws_queued_bytes_gauge() -> &'static opentelemetry::metrics::ObservableGauge<i64> {` |
| `ws_queued_frames_gauge` | observability/metrics.rs | `fn ws_queued_frames_gauge() -> &'static opentelemetry::metrics::ObservableGauge<i64> {` |
| `ws_sheds` | observability/metrics.rs | `fn ws_sheds() -> &'static Counter<u64> {` |
| `as_f64` | protocol.rs | `pub fn as_f64(self) -> f64 {` |
| `from` | protocol.rs | `fn from(v: f64) -> Self {` |
| `connected_message` | protocol/connect.rs | `pub fn connected_message(wsid: &str, app_id: &str, shard_num: u32) -> Value {` |
| `decode_sec_protocols` | protocol/connect.rs | `pub fn decode_sec_protocols(header: &str) -> Result<SecProtocols, DecodeError> {` |
| `downstream_message` | protocol/down.rs | `pub fn downstream_message(msg_type: &str, body: &impl Serialize) -> Value {` |
| `basic` | protocol/error.rs | `pub fn basic(kind: ErrorKind, message: String) -> Self {` |
| `client_not_found` | protocol/error.rs | `pub fn client_not_found(message: impl Into<String>) -> Self {` |
| `error_message` | protocol/error.rs | `pub fn error_message(body: &ErrorBody) -> Value {` |
| `internal` | protocol/error.rs | `pub fn internal(message: impl Into<String>) -> Self {` |
| `invalid_message` | protocol/error.rs | `pub fn invalid_message(message: impl Into<String>) -> Self {` |
| `invalid_push` | protocol/error.rs | `pub fn invalid_push(message: impl Into<String>) -> Self {` |
| `kind` | protocol/error.rs | `pub fn kind(&self) -> &ErrorKind {` |
| `message` | protocol/error.rs | `pub fn message(&self) -> &str {` |
| `rehome` | protocol/error.rs | `pub fn rehome(message: impl Into<String>) -> Self {` |
| `rehome_with_max_backoff_ms` | protocol/error.rs | `pub fn rehome_with_max_backoff_ms(message: impl Into<String>, max_backoff_ms: i64) -> S…` |
| `unauthorized` | protocol/error.rs | `pub fn unauthorized(message: impl Into<String>) -> Self {` |
| `version_not_supported` | protocol/error.rs | `pub fn version_not_supported(message: impl Into<String>) -> Self {` |
| `pong_message` | protocol/pong.rs | `pub fn pong_message() -> Value {` |
| `hex4` | protocol/up.rs | `fn hex4(b: &[u8], at: usize) -> Option<u32> {` |
| `parse_frame_json` | protocol/up.rs | `pub fn parse_frame_json(text: &str) -> Result<Vec<Value>, serde_json::Error> {` |
| `parse_upstream` | protocol/up.rs | `pub fn parse_upstream(text: &str) -> Result<Upstream, serde_json::Error> {` |
| `parse_upstream_array` | protocol/up.rs | `pub fn parse_upstream_array(arr: &[Value]) -> Result<Upstream, serde_json::Error> {` |
| `replace_unpaired_surrogate_escapes` | protocol/up.rs | `fn replace_unpaired_surrogate_escapes(text: &str) -> Option<String> {` |
| `add_metric` | server/inspector_delegate.rs | `pub fn add_metric(&mut self, metric: Metric, value: f64, query_id: &str) {` |
| `add_query` | server/inspector_delegate.rs | `pub fn add_query(&mut self, query_id: &str, ast: Value) {` |
| `get_ast_for_query` | server/inspector_delegate.rs | `pub fn get_ast_for_query(&self, query_id: &str) -> Option<&Value> {` |
| `get_metrics_json` | server/inspector_delegate.rs | `pub fn get_metrics_json(&mut self) -> Value {` |
| `get_metrics_json_for_query` | server/inspector_delegate.rs | `pub fn get_metrics_json_for_query(&mut self, query_id: &str) -> Option<Value> {` |
| `number_to_value` | server/inspector_delegate.rs | `fn number_to_value(n: f64) -> Value {` |
| `remove_query` | server/inspector_delegate.rs | `pub fn remove_query(&mut self, query_id: &str) {` |
| `put` | server/logging.rs | `fn put(&mut self, field: &Field, value: Value) {` |
| `is_priority_op_running` | server/priority_op.rs | `pub fn is_priority_op_running() -> bool {` |
| `run_priority_op` | server/priority_op.rs | `pub async fn run_priority_op<T, F: Future<Output = T>>(description: &str, op: F) -> T {` |
| `create_mutagen` | server/syncer.rs | `fn create_mutagen(&self, _cg_id: &str) -> Option<Arc<dyn crate::MutagenDispatch>> {` |
| `create_pusher` | server/syncer.rs | `fn create_pusher(&self, _cg_id: &str) -> Option<Arc<dyn crate::PusherDispatch>> {` |
| `create_sync_engine_config` | server/syncer.rs | `fn create_sync_engine_config(&self, cg_id: &str) -> crate::SyncEngineConfig {` |
| `analyze_query` | services/analyze.rs | `pub fn analyze_query(` |
| `merge_explain_fallback` | services/analyze.rs | `fn merge_explain_fallback(` |
| `ack_mutation_responses` | services/mutagen/pusher.rs | `fn ack_mutation_responses(` |
| `cleanup_push_body` | services/mutagen/pusher.rs | `fn cleanup_push_body(` |
| `combine_key_of` | services/mutagen/pusher.rs | `fn combine_key_of(` |
| `combine_pushes` | services/mutagen/pusher.rs | `fn combine_pushes(entries: Vec<QueuedPush>) -> Vec<QueuedPush> {` |
| `delete_client_mutations` | services/mutagen/pusher.rs | `fn delete_client_mutations(` |
| `enqueue_payload` | services/mutagen/pusher.rs | `fn enqueue_payload(&self, push: QueuedPush, what: &str) -> bool {` |
| `enqueue_push` | services/mutagen/pusher.rs | `fn enqueue_push(` |
| `fail_downstream` | services/mutagen/pusher.rs | `fn fail_downstream(` |
| `fan_out_responses` | services/mutagen/pusher.rs | `fn fan_out_responses(sinks: &ConnectionSinks, response: &serde_json::Value) {` |
| `group_by` | services/mutagen/pusher.rs | `fn group_by<T>(items: impl Iterator<Item = (String, T)>) -> Vec<(String, Vec<T>)> {` |
| `init_connection` | services/mutagen/pusher.rs | `fn init_connection(&self, _selector: &ConnectionSelector) {}` |
| `is_push_error_response` | services/mutagen/pusher.rs | `fn is_push_error_response(response: &serde_json::Value) -> bool {` |
| `mutation_ids_of` | services/mutagen/pusher.rs | `fn mutation_ids_of(push_body: &serde_json::Value) -> Vec<MutationID> {` |
| `queue_cap` | services/mutagen/pusher.rs | `fn queue_cap() -> i64 {` |
| `relay_body` | services/mutagen/pusher.rs | `fn relay_body(` |
| `set_auth_fail_hook` | services/mutagen/pusher.rs | `fn set_auth_fail_hook(&self, hook: AuthFailHook) {` |
| `set_validate_hook` | services/mutagen/pusher.rs | `fn set_validate_hook(&self, hook: ValidateHook) {` |
| `get_column` | services/replicator/schema/column_metadata.rs | `pub fn get_column(&self, table_name: &str, column_name: &str) -> Option<&ColumnMetadata> {` |
| `get_instance` | services/replicator/schema/column_metadata.rs | `pub fn get_instance(conn: &Connection) -> Result<Option<Self>, String> {` |
| `metadata_to_lite_type_string` | services/replicator/schema/column_metadata.rs | `pub fn metadata_to_lite_type_string(metadata: &ColumnMetadata) -> String {` |
| `ivm_row_to_json` | services/run_ast.rs | `pub(crate) fn ivm_row_to_json(row: &Row) -> serde_json::Value {` |
| `ivm_value_to_json` | services/run_ast.rs | `pub(crate) fn ivm_value_to_json(v: &Value) -> serde_json::Value {` |
| `rows_by_source_to_json` | services/run_ast.rs | `fn rows_by_source_to_json(src: &rust_ivm::builder::debug_delegate::RowsBySource) -> Row…` |
| `run_ast` | services/run_ast.rs | `pub fn run_ast(` |
| `check_client_schema` | services/view_syncer/client_schema.rs | `pub fn check_client_schema(` |
| `auth_equals` | services/view_syncer/connection_context_manager.rs | `pub fn auth_equals(a: Option<&Auth>, b: Option<&Auth>) -> bool {` |
| `build_fetch_context` | services/view_syncer/connection_context_manager.rs | `fn build_fetch_context(` |
| `close_connection` | services/view_syncer/connection_context_manager.rs | `pub fn close_connection(&mut self, selector: &ConnectionSelector) -> Option<ConnectionC…` |
| `compare_by_insertion_order` | services/view_syncer/connection_context_manager.rs | `fn compare_by_insertion_order(a: &ConnectionContext, b: &ConnectionContext) -> std::cmp…` |
| `compare_preferred_validated_connection` | services/view_syncer/connection_context_manager.rs | `fn compare_preferred_validated_connection(` |
| `defer_maintenance` | services/view_syncer/connection_context_manager.rs | `pub fn defer_maintenance(&mut self, kind: MaintenanceKind) {` |
| `demote_connection` | services/view_syncer/connection_context_manager.rs | `fn demote_connection(&mut self, connection: ConnectionContext) -> ConnectionContext {` |
| `fail_connection` | services/view_syncer/connection_context_manager.rs | `pub fn fail_connection(` |
| `filter_headers` | services/view_syncer/connection_context_manager.rs | `fn filter_headers(` |
| `get_background_connection_context` | services/view_syncer/connection_context_manager.rs | `pub fn get_background_connection_context(&self) -> Option<ConnectionContext> {` |
| `get_connection_context` | services/view_syncer/connection_context_manager.rs | `pub fn get_connection_context(` |
| `get_group_state` | services/view_syncer/connection_context_manager.rs | `pub fn get_group_state(&self) -> &GroupAuthState {` |
| `mark_background_retransform_success` | services/view_syncer/connection_context_manager.rs | `pub fn mark_background_retransform_success(` |
| `min_defined` | services/view_syncer/connection_context_manager.rs | `fn min_defined(a: Option<i64>, b: Option<i64>) -> Option<i64> {` |
| `must_get_background_connection_context` | services/view_syncer/connection_context_manager.rs | `pub fn must_get_background_connection_context(&self) -> Result<ConnectionContext, CCMEr…` |
| `must_get_connection_context` | services/view_syncer/connection_context_manager.rs | `pub fn must_get_connection_context(` |
| `next_revalidate_at` | services/view_syncer/connection_context_manager.rs | `fn next_revalidate_at(&self) -> Option<i64> {` |
| `now` | services/view_syncer/connection_context_manager.rs | `fn now(&self) -> i64 {` |
| `pick_token` | services/view_syncer/connection_context_manager.rs | `fn pick_token(previous: Option<&Auth>, new: &Auth) -> Result<Option<Auth>, CCMError> {` |
| `plan_maintenance` | services/view_syncer/connection_context_manager.rs | `pub fn plan_maintenance(&self) -> MaintenancePlan {` |
| `raw` | services/view_syncer/connection_context_manager.rs | `pub fn raw(&self) -> &str {` |
| `refresh_background_connection_context` | services/view_syncer/connection_context_manager.rs | `fn refresh_background_connection_context(&mut self, preferred: Option<&ConnectionContex…` |
| `register_connection` | services/view_syncer/connection_context_manager.rs | `pub fn register_connection(` |
| `remove_connection_internal` | services/view_syncer/connection_context_manager.rs | `fn remove_connection_internal(` |
| `resolve_auth` | services/view_syncer/connection_context_manager.rs | `pub fn resolve_auth(` |
| `set_background_connection` | services/view_syncer/connection_context_manager.rs | `fn set_background_connection(&mut self, bg: Option<ConnectionSelector>) {` |
| `set_shared_retransform_ready` | services/view_syncer/connection_context_manager.rs | `pub fn set_shared_retransform_ready(&mut self, ready: bool) {` |
| `store_connection` | services/view_syncer/connection_context_manager.rs | `fn store_connection(&mut self, connection: ConnectionContext) -> ConnectionContext {` |
| `to_error_body` | services/view_syncer/connection_context_manager.rs | `pub fn to_error_body(&self) -> ErrorBody {` |
| `update_auth` | services/view_syncer/connection_context_manager.rs | `pub fn update_auth(` |
| `update_background_retransform_deadline` | services/view_syncer/connection_context_manager.rs | `fn update_background_retransform_deadline(&mut self, reset: bool) {` |
| `validate_connection` | services/view_syncer/connection_context_manager.rs | `pub fn validate_connection(` |
| `drain_next_in` | services/view_syncer/drain_coordinator.rs | `pub fn drain_next_in(&self, interval_ms: u64) {` |
| `force_drain_timeout` | services/view_syncer/drain_coordinator.rs | `pub async fn force_drain_timeout(&self) {` |
| `is_draining` | services/view_syncer/drain_coordinator.rs | `pub fn is_draining(&self) -> bool {` |
| `next_drain_time` | services/view_syncer/drain_coordinator.rs | `pub fn next_drain_time(&self) -> i64 {` |
| `should_drain` | services/view_syncer/drain_coordinator.rs | `pub fn should_drain(&self) -> bool {` |
| `on_version_ready` | services/view_syncer/e2e_serving_lag.rs | `pub fn on_version_ready(` |
| `on_version_served` | services/view_syncer/e2e_serving_lag.rs | `pub fn on_version_served(&mut self, served_version: &str, now_ms: f64) -> Option<Observ…` |
| `pending` | services/view_syncer/e2e_serving_lag.rs | `pub fn pending(&self) -> Option<&PendingUpstreamCommit> {` |
| `analyze_query_op` | services/view_syncer/inspect_handler.rs | `async fn analyze_query_op(` |
| `handle_inspect` | services/view_syncer/inspect_handler.rs | `pub async fn handle_inspect(` |
| `inspect_queries_value` | services/view_syncer/inspect_handler.rs | `async fn inspect_queries_value(` |
| `load_legacy_analyze_permissions` | services/view_syncer/inspect_handler.rs | `fn load_legacy_analyze_permissions(` |
| `metrics_for_protocol` | services/view_syncer/inspect_handler.rs | `pub fn metrics_for_protocol(` |
| `resolve_analyze_ast` | services/view_syncer/inspect_handler.rs | `async fn resolve_analyze_ast(` |
| `active_query_ids` | services/view_syncer/pipeline_driver.rs | `pub fn active_query_ids(&self) -> Vec<String> {` |
| `advance` | services/view_syncer/pipeline_driver.rs | `pub fn advance(&mut self, timer: Rc<dyn Timer>) -> Result<AdvanceChanges<'_>, String> {` |
| `advance_panic_outcome` | services/view_syncer/pipeline_driver.rs | `fn advance_panic_outcome(` |
| `build_engine` | services/view_syncer/pipeline_driver.rs | `fn build_engine(&mut self, tables: &[IvmTableSpec], source_conn: Option<SharedConn>) {` |
| `column_schema` | services/view_syncer/pipeline_driver.rs | `fn column_schema(v: &IvmColumnSchema) -> ColumnSchema {` |
| `column_type` | services/view_syncer/pipeline_driver.rs | `fn column_type(type_str: &str, optional: bool) -> ColumnType {` |
| `convert_ast` | services/view_syncer/pipeline_driver.rs | `fn convert_ast(ts: TsAst) -> rust_ivm::builder::ast::Ast {` |
| `convert_condition` | services/view_syncer/pipeline_driver.rs | `fn convert_condition(c: TsCondition) -> rust_ivm::builder::ast::Condition {` |
| `convert_csq` | services/view_syncer/pipeline_driver.rs | `fn convert_csq(c: &TsCorrelatedSubquery) -> rust_ivm::builder::ast::RelatedSubquery {` |
| `convert_value_position` | services/view_syncer/pipeline_driver.rs | `fn convert_value_position(vp: TsValuePosition) -> rust_ivm::builder::ast::ValuePosition {` |
| `current_permissions` | services/view_syncer/pipeline_driver.rs | `pub fn current_permissions(` |
| `current_version` | services/view_syncer/pipeline_driver.rs | `pub fn current_version(&self) -> Option<String> {` |
| `destroy` | services/view_syncer/pipeline_driver.rs | `pub fn destroy(&mut self) {` |
| `destroy_pipeline` | services/view_syncer/pipeline_driver.rs | `fn destroy_pipeline(&mut self, query_id: &str, stop_reason: &'static str) {` |
| `elapsed_lap` | services/view_syncer/pipeline_driver.rs | `fn elapsed_lap(&self) -> f64;` |
| `finish` | services/view_syncer/pipeline_driver.rs | `pub fn finish(mut self) -> Result<(), JsError> {` |
| `finish_advance` | services/view_syncer/pipeline_driver.rs | `fn finish_advance(&mut self, stream: AdvanceStream) -> Result<AdvanceOutcome, String> {` |
| `finish_hydrate` | services/view_syncer/pipeline_driver.rs | `fn finish_hydrate(&mut self, stream: HydrateStream, queries: &[HydrateQuery]) {` |
| `for_pipeline` | services/view_syncer/pipeline_driver.rs | `fn for_pipeline(` |
| `get_row` | services/view_syncer/pipeline_driver.rs | `pub fn get_row(&self, table: &str, pk: &[(String, Value)]) -> Option<Row> {` |
| `has_query` | services/view_syncer/pipeline_driver.rs | `pub fn has_query(&self, query_id: &str) -> bool {` |
| `header` | services/view_syncer/pipeline_driver.rs | `pub fn header(&self) -> (&str, usize) {` |
| `hydrate` | services/view_syncer/pipeline_driver.rs | `pub fn hydrate<Q: Clone + Into<HydrateQuery>>(` |
| `hydrate_analyze` | services/view_syncer/pipeline_driver.rs | `pub fn hydrate_analyze(` |
| `hydrate_js_error` | services/view_syncer/pipeline_driver.rs | `fn hydrate_js_error(payload: &Box<dyn std::any::Any + Send>) -> JsError {` |
| `hydration_time_ms` | services/view_syncer/pipeline_driver.rs | `pub fn hydration_time_ms(&self, query_id: &str) -> Option<f64> {` |
| `init` | services/view_syncer/pipeline_driver.rs | `pub fn init(` |
| `init_and_reset_common` | services/view_syncer/pipeline_driver.rs | `pub fn init_and_reset_common(` |
| `init_from_connection` | services/view_syncer/pipeline_driver.rs | `pub fn init_from_connection(` |
| `initialized` | services/view_syncer/pipeline_driver.rs | `pub fn initialized(&self) -> bool {` |
| `json_to_value` | services/view_syncer/pipeline_driver.rs | `pub(crate) fn json_to_value(v: serde_json::Value) -> rust_ivm::ivm::data::Value {` |
| `log_query_failure` | services/view_syncer/pipeline_driver.rs | `fn log_query_failure(` |
| `log_query_pipeline_lifecycle` | services/view_syncer/pipeline_driver.rs | `fn log_query_pipeline_lifecycle(log: QueryPipelineLifecycleLog) {` |
| `log_vended_row_counts` | services/view_syncer/pipeline_driver.rs | `fn log_vended_row_counts(` |
| `next` | services/view_syncer/pipeline_driver.rs | `fn next(&mut self) -> Option<Self::Item> {` |
| `on_hydrate_panic` | services/view_syncer/pipeline_driver.rs | `fn on_hydrate_panic(` |
| `panic_message` | services/view_syncer/pipeline_driver.rs | `fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {` |
| `parse_ts_ast` | services/view_syncer/pipeline_driver.rs | `pub fn parse_ts_ast(json: &str) -> Result<rust_ivm::builder::ast::Ast, String> {` |
| `query_transformation_hash` | services/view_syncer/pipeline_driver.rs | `pub fn query_transformation_hash(&self, query_id: &str) -> Option<&str> {` |
| `random_id` | services/view_syncer/pipeline_driver.rs | `fn random_id() -> String {` |
| `running_queries` | services/view_syncer/pipeline_driver.rs | `pub fn running_queries(&self) -> Vec<(String, std::sync::Arc<str>, String)> {` |
| `scalar_reset_message` | services/view_syncer/pipeline_driver.rs | `fn scalar_reset_message(payload: &Box<dyn std::any::Any + Send>) -> Option<String> {` |
| `set_client_primary_keys` | services/view_syncer/pipeline_driver.rs | `pub fn set_client_primary_keys(&mut self, client_primary_keys: HashMap<String, Vec<Stri…` |
| `set_query_transformation_hash` | services/view_syncer/pipeline_driver.rs | `pub fn set_query_transformation_hash(&mut self, query_id: &str, hash: &str) {` |
| `set_yield_threshold_ms` | services/view_syncer/pipeline_driver.rs | `pub fn set_yield_threshold_ms(&mut self, yield_threshold_ms: Rc<dyn Fn() -> f64>) {` |
| `should_yield` | services/view_syncer/pipeline_driver.rs | `pub fn should_yield(&self) -> bool {` |
| `should_yield_hook` | services/view_syncer/pipeline_driver.rs | `fn should_yield_hook(&self) -> Rc<dyn Fn() -> bool> {` |
| `should_yield_with` | services/view_syncer/pipeline_driver.rs | `fn should_yield_with(` |
| `to_string_radix_36` | services/view_syncer/pipeline_driver.rs | `fn to_string_radix_36(mut n: u64) -> String {` |
| `total_elapsed` | services/view_syncer/pipeline_driver.rs | `fn total_elapsed(&self) -> f64;` |
| `vended_table_names` | services/view_syncer/pipeline_driver.rs | `fn vended_table_names(&self) -> Vec<String> {` |
| `zql_column_type` | services/view_syncer/pipeline_driver.rs | `fn zql_column_type(cs: &ColumnSchema) -> ColumnType {` |
| `ast_covered_by` | services/view_syncer/query_covering.rs | `fn ast_covered_by(covered: &Value, covering: &Value) -> bool {` |
| `bounds_covered_by` | services/view_syncer/query_covering.rs | `fn bounds_covered_by(covered: &Value, covering: &Value) -> bool {` |
| `cmp_num` | services/view_syncer/query_covering.rs | `fn cmp_num(a: &Value, b: &Value, f: impl Fn(f64, f64) -> bool) -> bool {` |
| `column_literal_parts` | services/view_syncer/query_covering.rs | `fn column_literal_parts(condition: &Value) -> Option<ColumnLiteralParts<'_>> {` |
| `condition_equivalent` | services/view_syncer/query_covering.rs | `fn condition_equivalent(a: Option<&Value>, b: Option<&Value>) -> bool {` |
| `condition_implies` | services/view_syncer/query_covering.rs | `fn condition_implies(covered: Option<&Value>, covering: Option<&Value>) -> bool {` |
| `conditions` | services/view_syncer/query_covering.rs | `fn conditions(v: &Value) -> &[Value] {` |
| `correlated_condition_implies` | services/view_syncer/query_covering.rs | `fn correlated_condition_implies(covered: &Value, covering: &Value) -> bool {` |
| `equality_implies` | services/view_syncer/query_covering.rs | `fn equality_implies(value: &Value, covering_op: &str, covering_value: &Value) -> bool {` |
| `field_eq` | services/view_syncer/query_covering.rs | `fn field_eq(a: &Value, b: &Value, key: &str) -> bool {` |
| `find_covering_query` | services/view_syncer/query_covering.rs | `pub fn find_covering_query(` |
| `is_equality_op` | services/view_syncer/query_covering.rs | `fn is_equality_op(op: &str) -> bool {` |
| `is_non_null_scalar_literal_value` | services/view_syncer/query_covering.rs | `fn is_non_null_scalar_literal_value(value: &Value) -> bool {` |
| `is_numeric_order_op` | services/view_syncer/query_covering.rs | `fn is_numeric_order_op(op: &str) -> bool {` |
| `is_query_covered_by` | services/view_syncer/query_covering.rs | `pub fn is_query_covered_by(covered: &Value, covering: &Value) -> bool {` |
| `json_eq` | services/view_syncer/query_covering.rs | `fn json_eq(a: Option<&Value>, b: Option<&Value>) -> bool {` |
| `json_equal` | services/view_syncer/query_covering.rs | `fn json_equal(a: &Value, b: &Value) -> bool {` |
| `literal_array_includes` | services/view_syncer/query_covering.rs | `fn literal_array_includes(values: &[Value], value: &Value) -> bool {` |
| `log_shadow_summary` | services/view_syncer/query_covering.rs | `pub fn log_shadow_summary(` |
| `num` | services/view_syncer/query_covering.rs | `fn num(v: &Value) -> Option<f64> {` |
| `order_condition_implies` | services/view_syncer/query_covering.rs | `fn order_condition_implies(` |
| `present` | services/view_syncer/query_covering.rs | `fn present(v: Option<&Value>) -> Option<&Value> {` |
| `related_covered_by` | services/view_syncer/query_covering.rs | `fn related_covered_by(covered: Option<&Vec<Value>>, covering: Option<&Vec<Value>>) -> b…` |
| `related_of` | services/view_syncer/query_covering.rs | `fn related_of(cond: &Value) -> &Value {` |
| `remove` | services/view_syncer/query_covering.rs | `pub fn remove(&mut self, query_id: &str) {` |
| `root_key` | services/view_syncer/query_covering.rs | `fn root_key(ast: &Value) -> String {` |
| `same_related_edge` | services/view_syncer/query_covering.rs | `fn same_related_edge(a: &Value, b: &Value) -> bool {` |
| `simple_condition_implies` | services/view_syncer/query_covering.rs | `fn simple_condition_implies(covered: &Value, covering: &Value) -> bool {` |
| `subquery` | services/view_syncer/query_covering.rs | `fn subquery(related: &Value) -> &Value {` |
| `accumulate_signature` | services/view_syncer/view_syncer.rs | `fn accumulate_signature(acc: &mut HashMap<String, u64>, rc: &rust_ivm::streamer::RowCha…` |
| `advance_and_sync` | services/view_syncer/view_syncer.rs | `pub async fn advance_and_sync(` |
| `advance_poke_targets` | services/view_syncer/view_syncer.rs | `fn advance_poke_targets(` |
| `app_id` | services/view_syncer/view_syncer.rs | `pub fn app_id(&self) -> &str {` |
| `apply_client_deletions` | services/view_syncer/view_syncer.rs | `async fn apply_client_deletions(` |
| `arm_serving_lag` | services/view_syncer/view_syncer.rs | `fn arm_serving_lag(&mut self, notification: &serde_json::Value) {` |
| `attempt_background_retransform` | services/view_syncer/view_syncer.rs | `async fn attempt_background_retransform(` |
| `catchup_clients` | services/view_syncer/view_syncer.rs | `pub async fn catchup_clients(` |
| `catchup_floor` | services/view_syncer/view_syncer.rs | `fn catchup_floor(` |
| `cg_event_loop` | services/view_syncer/view_syncer.rs | `pub(crate) async fn cg_event_loop(` |
| `change_desired_queries` | services/view_syncer/view_syncer.rs | `async fn change_desired_queries(&self, selector: &ConnectionSelector, msg: &str) {` |
| `check_client_and_cvr_versions` | services/view_syncer/view_syncer.rs | `fn check_client_and_cvr_versions(` |
| `check_for_thrashing` | services/view_syncer/view_syncer.rs | `fn check_for_thrashing(&mut self, query_id: &str) {` |
| `classify_retransform_failure` | services/view_syncer/view_syncer.rs | `fn classify_retransform_failure(failure: Option<serde_json::Value>) -> RetransformOutco…` |
| `client_handler_for` | services/view_syncer/view_syncer.rs | `fn client_handler_for(&self, client_id: &str) -> Option<Arc<ClientHandler>> {` |
| `clients_to_delete` | services/view_syncer/view_syncer.rs | `fn clients_to_delete(` |
| `cmd` | services/view_syncer/view_syncer.rs | `fn cmd(self) -> &'static str {` |
| `config_and_hydrate` | services/view_syncer/view_syncer.rs | `pub async fn config_and_hydrate(` |
| `config_and_hydrate_with_profile` | services/view_syncer/view_syncer.rs | `pub async fn config_and_hydrate_with_profile(` |
| `config_poke_targets` | services/view_syncer/view_syncer.rs | `fn config_poke_targets(` |
| `custom_query_context_from` | services/view_syncer/view_syncer.rs | `fn custom_query_context_from(ctx: &CcmConnectionContext) -> Option<CustomQueryContext> {` |
| `cvr_store_error_body` | services/view_syncer/view_syncer.rs | `fn cvr_store_error_body(error: &CVRStoreError) -> crate::protocol::ErrorBody {` |
| `cvr_store_error_thrown` | services/view_syncer/view_syncer.rs | `fn cvr_store_error_thrown<'a>(error: &CVRStoreError, message: &'a str) -> Thrown<'a> {` |
| `decrement_active_client` | services/view_syncer/view_syncer.rs | `fn decrement_active_client(&mut self, ws_id: &str) {` |
| `decrement_nonzero` | services/view_syncer/view_syncer.rs | `pub(crate) fn decrement_nonzero(count: &AtomicU64) {` |
| `delete_client_due_to_disconnect` | services/view_syncer/view_syncer.rs | `fn delete_client_due_to_disconnect(&mut self, client_id: &str, ws_id: &str) {` |
| `delete_clients` | services/view_syncer/view_syncer.rs | `async fn delete_clients(&self, selector: &ConnectionSelector, msg: &str) -> Vec<String> {` |
| `dispatch_cg_message` | services/view_syncer/view_syncer.rs | `async fn dispatch_cg_message(` |
| `empty_cvr` | services/view_syncer/view_syncer.rs | `pub fn empty_cvr(id: &str, replica_version: &str) -> CVR {` |
| `ensure_cvr` | services/view_syncer/view_syncer.rs | `async fn ensure_cvr(&mut self, allow_create: bool) -> Result<bool, LoadCvrError> {` |
| `existing_rows` | services/view_syncer/view_syncer.rs | `pub async fn existing_rows(&self) -> Result<Arc<RowRecordMap>, CVRStoreError> {` |
| `fail_group` | services/view_syncer/view_syncer.rs | `fn fail_group(&mut self, message: &str) {` |
| `fail_group_js` | services/view_syncer/view_syncer.rs | `fn fail_group_js(&mut self, error: &JsError) {` |
| `fail_group_with_error` | services/view_syncer/view_syncer.rs | `fn fail_group_with_error(` |
| `fail_maintenance_connection` | services/view_syncer/view_syncer.rs | `fn fail_maintenance_connection(` |
| `flush_ops_to_store` | services/view_syncer/view_syncer.rs | `async fn flush_ops_to_store(` |
| `flush_to_store` | services/view_syncer/view_syncer.rs | `async fn flush_to_store(` |
| `forces_config_pass` | services/view_syncer/view_syncer.rs | `fn forces_config_pass(self) -> bool {` |
| `format_transform_error_message` | services/view_syncer/view_syncer.rs | `fn format_transform_error_message(error: &serde_json::Value) -> String {` |
| `gather_catchup_patches` | services/view_syncer/view_syncer.rs | `async fn gather_catchup_patches(` |
| `get_clients` | services/view_syncer/view_syncer.rs | `fn get_clients(&self, ws_ids: &[String]) -> Vec<Arc<ClientHandler>> {` |
| `get_ttl_clock` | services/view_syncer/view_syncer.rs | `fn get_ttl_clock(&mut self, now: i64) -> TTLClock {` |
| `handle_config_update` | services/view_syncer/view_syncer.rs | `async fn handle_config_update(` |
| `handle_desired_queries` | services/view_syncer/view_syncer.rs | `async fn handle_desired_queries(` |
| `handle_update_auth` | services/view_syncer/view_syncer.rs | `async fn handle_update_auth(&mut self, client_id: &str, token: &str) {` |
| `has_client` | services/view_syncer/view_syncer.rs | `fn has_client(&self, client_id: &str) -> bool {` |
| `hydrate_and_sync` | services/view_syncer/view_syncer.rs | `pub async fn hydrate_and_sync(` |
| `hydrate_unchanged_queries` | services/view_syncer/view_syncer.rs | `async fn hydrate_unchanged_queries(` |
| `idle_shutdown_due` | services/view_syncer/view_syncer.rs | `fn idle_shutdown_due(&self) -> bool {` |
| `inspect` | services/view_syncer/view_syncer.rs | `async fn inspect(&self, selector: &ConnectionSelector, msg: &str) {` |
| `inspect_queries` | services/view_syncer/view_syncer.rs | `pub async fn inspect_queries(` |
| `inspector_delegate` | services/view_syncer/view_syncer.rs | `pub fn inspector_delegate(` |
| `is_init_connection` | services/view_syncer/view_syncer.rs | `fn is_init_connection(self) -> bool {` |
| `is_retryable_flush_error` | services/view_syncer/view_syncer.rs | `fn is_retryable_flush_error(e: &rust_cvr::cvr_store::CVRStoreError) -> bool {` |
| `load_cvr` | services/view_syncer/view_syncer.rs | `pub async fn load_cvr(&self, last_connect_time: f64) -> Result<Option<CVR>, LoadCvrErro…` |
| `lock_unpoisoned` | services/view_syncer/view_syncer.rs | `pub(crate) fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {` |
| `mark_version_served` | services/view_syncer/view_syncer.rs | `fn mark_version_served(&mut self, version: &CVRVersion) {` |
| `maybe_log_advance_summary` | services/view_syncer/view_syncer.rs | `fn maybe_log_advance_summary() {` |
| `merge_notifications` | services/view_syncer/view_syncer.rs | `fn merge_notifications(prev: serde_json::Value, next: serde_json::Value) -> serde_json:…` |
| `new_with_accepting` | services/view_syncer/view_syncer.rs | `fn new_with_accepting(` |
| `next_auth_maintenance_delay` | services/view_syncer/view_syncer.rs | `fn next_auth_maintenance_delay(&self) -> Option<Duration> {` |
| `next_expiry_delay` | services/view_syncer/view_syncer.rs | `fn next_expiry_delay(&self) -> Option<Duration> {` |
| `next_idle_shutdown_delay` | services/view_syncer/view_syncer.rs | `fn next_idle_shutdown_delay(&self) -> Option<Duration> {` |
| `next_ttl_clock_delay` | services/view_syncer/view_syncer.rs | `fn next_ttl_clock_delay(&self) -> Option<Duration> {` |
| `offload` | services/view_syncer/view_syncer.rs | `async fn offload<F, T>(&self, fut: F) -> T` |
| `older_replica_error` | services/view_syncer/view_syncer.rs | `fn older_replica_error(cvr: &CVR, replica_version: &str) -> Option<String> {` |
| `on_expiry_tick` | services/view_syncer/view_syncer.rs | `async fn on_expiry_tick(&mut self) {` |
| `on_inbound` | services/view_syncer/view_syncer.rs | `async fn on_inbound(` |
| `on_new_connection` | services/view_syncer/view_syncer.rs | `async fn on_new_connection(` |
| `on_notification` | services/view_syncer/view_syncer.rs | `async fn on_notification(&mut self, notification: serde_json::Value) {` |
| `parse_desired_queries_patch` | services/view_syncer/view_syncer.rs | `fn parse_desired_queries_patch(` |
| `pipelines` | services/view_syncer/view_syncer.rs | `pub fn pipelines(&mut self) -> &mut IvmPipelines {` |
| `process_clock_ms` | services/view_syncer/view_syncer.rs | `fn process_clock_ms() -> f64 {` |
| `protocol_version_for_ws` | services/view_syncer/view_syncer.rs | `pub fn protocol_version_for_ws(&self, ws_id: &str) -> u32 {` |
| `publish_serving_lag` | services/view_syncer/view_syncer.rs | `fn publish_serving_lag(&mut self) {` |
| `query_context_for` | services/view_syncer/view_syncer.rs | `fn query_context_for(&self, client_id: &str, ws_id: &str) -> Option<CustomQueryContext> {` |
| `query_count` | services/view_syncer/view_syncer.rs | `fn query_count(&mut self) -> usize {` |
| `query_name_of` | services/view_syncer/view_syncer.rs | `fn query_name_of(cvr: &CVR, qid: &str) -> Option<String> {` |
| `record_transform_error` | services/view_syncer/view_syncer.rs | `fn record_transform_error(error: serde_json::Value, transform_errors: &mut Vec<serde_js…` |
| `register_client` | services/view_syncer/view_syncer.rs | `pub fn register_client(` |
| `reject_queued_connection` | services/view_syncer/view_syncer.rs | `fn reject_queued_connection(` |
| `remove_expired_queries` | services/view_syncer/view_syncer.rs | `pub async fn remove_expired_queries(` |
| `replica_path` | services/view_syncer/view_syncer.rs | `pub fn replica_path(&self) -> Option<&str> {` |
| `reset_pipelines_and_rehydrate` | services/view_syncer/view_syncer.rs | `async fn reset_pipelines_and_rehydrate(&mut self, cvr: CVR, message: &str) {` |
| `row_cache_cow_copies` | services/view_syncer/view_syncer.rs | `pub async fn row_cache_cow_copies(&self) -> u64 {` |
| `row_change_to_maps` | services/view_syncer/view_syncer.rs | `fn row_change_to_maps(rc: &rust_ivm::streamer::RowChange) -> Option<RowChangeMaps> {` |
| `row_count` | services/view_syncer/view_syncer.rs | `fn row_count(&self) -> usize {` |
| `row_to_contents` | services/view_syncer/view_syncer.rs | `fn row_to_contents(row: &rust_ivm::ivm::data::Row) -> serde_json::Value {` |
| `run_auth_maintenance` | services/view_syncer/view_syncer.rs | `async fn run_auth_maintenance(&mut self) {` |
| `run_background_retransform` | services/view_syncer/view_syncer.rs | `async fn run_background_retransform(&mut self) {` |
| `run_in_lock_for_client` | services/view_syncer/view_syncer.rs | `fn run_in_lock_for_client(` |
| `same_hash_rehydration_bump_reason` | services/view_syncer/view_syncer.rs | `fn same_hash_rehydration_bump_reason(` |
| `schedule_auth_maintenance` | services/view_syncer/view_syncer.rs | `fn schedule_auth_maintenance(&mut self) {` |
| `schedule_expire_eviction` | services/view_syncer/view_syncer.rs | `fn schedule_expire_eviction(&mut self, cvr: &CVR) {` |
| `second_element` | services/view_syncer/view_syncer.rs | `fn second_element(msg: &str) -> serde_json::Value {` |
| `seed_signatures_from_cvr` | services/view_syncer/view_syncer.rs | `fn seed_signatures_from_cvr(cvr: &CVR) -> HashMap<String, u64> {` |
| `send_inspect_response` | services/view_syncer/view_syncer.rs | `pub fn send_inspect_response(&self, ws_id: &str, response: serde_json::Value) {` |
| `send_query_transform_error_to_clients` | services/view_syncer/view_syncer.rs | `fn send_query_transform_error_to_clients(` |
| `serving_lag_eligible` | services/view_syncer/view_syncer.rs | `fn serving_lag_eligible(&self) -> bool {` |
| `set_cvr_store` | services/view_syncer/view_syncer.rs | `pub fn set_cvr_store(` |
| `set_identity_for_tests` | services/view_syncer/view_syncer.rs | `pub fn set_identity_for_tests(&mut self, app_id: &str, shard: ShardID, cg_id: &str) {` |
| `set_tokio_handle` | services/view_syncer/view_syncer.rs | `pub fn set_tokio_handle(&mut self, handle: tokio::runtime::Handle) {` |
| `shard` | services/view_syncer/view_syncer.rs | `pub fn shard(&self) -> &ShardID {` |
| `shard_for` | services/view_syncer/view_syncer.rs | `pub(crate) fn shard_for(cg_id: &str, num_shards: usize) -> usize {` |
| `shutdown` | services/view_syncer/view_syncer.rs | `fn shutdown(&mut self) {` |
| `signature_provider` | services/view_syncer/view_syncer.rs | `fn signature_provider() -> (` |
| `slow_hydrate_threshold_from_env` | services/view_syncer/view_syncer.rs | `pub(crate) fn slow_hydrate_threshold_from_env(var: impl Fn(&str) -> Option<String>) -> …` |
| `slow_hydrate_threshold_ms` | services/view_syncer/view_syncer.rs | `pub(crate) fn slow_hydrate_threshold_ms() -> f64 {` |
| `sqlite_real_to_json` | services/view_syncer/view_syncer.rs | `fn sqlite_real_to_json(value: f64) -> serde_json::Value {` |
| `start` | services/view_syncer/view_syncer.rs | `pub async fn start(&self) {` |
| `start_lap` | services/view_syncer/view_syncer.rs | `fn start_lap(&self) {` |
| `start_ttl_clock_interval` | services/view_syncer/view_syncer.rs | `fn start_ttl_clock_interval(&mut self) {` |
| `start_without_yielding` | services/view_syncer/view_syncer.rs | `pub fn start_without_yielding(&self) {` |
| `stop` | services/view_syncer/view_syncer.rs | `pub fn stop(&self) -> f64 {` |
| `stop_expire_timer` | services/view_syncer/view_syncer.rs | `fn stop_expire_timer(&mut self) {` |
| `stop_lap` | services/view_syncer/view_syncer.rs | `fn stop_lap(&self) {` |
| `stop_ttl_clock_interval` | services/view_syncer/view_syncer.rs | `fn stop_ttl_clock_interval(&mut self) {` |
| `str_array` | services/view_syncer/view_syncer.rs | `fn str_array(v: Option<&serde_json::Value>) -> Vec<String> {` |
| `sync_query_pipeline_set` | services/view_syncer/view_syncer.rs | `async fn sync_query_pipeline_set(` |
| `sync_query_pipeline_set_inputs` | services/view_syncer/view_syncer.rs | `fn sync_query_pipeline_set_inputs(` |
| `transform_failure_message` | services/view_syncer/view_syncer.rs | `fn transform_failure_message(body: &serde_json::Value) -> String {` |
| `unregister_client` | services/view_syncer/view_syncer.rs | `pub fn unregister_client(&mut self, ws_id: &str) {` |
| `update_ttl_clock` | services/view_syncer/view_syncer.rs | `pub fn update_ttl_clock(&self, ttl_clock: rust_cvr::ttl_clock::TTLClock, last_active: i…` |
| `update_ttl_clock_in_cvr_without_lock` | services/view_syncer/view_syncer.rs | `fn update_ttl_clock_in_cvr_without_lock(&mut self) {` |
| `value_to_serde_json` | services/view_syncer/view_syncer.rs | `fn value_to_serde_json(v: &rust_ivm::ivm::data::Value) -> serde_json::Value {` |
| `wrap_with_protocol_error` | services/view_syncer/view_syncer.rs | `pub(crate) fn wrap_with_protocol_error(message: &str) -> crate::protocol::ErrorBody {` |
| `yield_process` | services/view_syncer/view_syncer.rs | `pub(crate) async fn yield_process() {` |
| `add_centroid` | tdigest.rs | `pub fn add_centroid(&mut self, c: Centroid) {` |
| `binary_search` | tdigest.rs | `fn binary_search(high: usize, compare: impl Fn(usize) -> f64) -> usize {` |
| `cdf` | tdigest.rs | `pub fn cdf(&mut self, x: f64) -> f64 {` |
| `count` | tdigest.rs | `pub fn count(&mut self) -> f64 {` |
| `integrated_location` | tdigest.rs | `fn integrated_location(&self, q: f64) -> f64 {` |
| `integrated_q` | tdigest.rs | `fn integrated_q(&self, k: f64) -> f64 {` |
| `process` | tdigest.rs | `fn process(&mut self) {` |
| `processed_size` | tdigest.rs | `fn processed_size(size: usize, compression: f64) -> usize {` |
| `quantile` | tdigest.rs | `pub fn quantile(&mut self, q: f64) -> f64 {` |
| `reset` | tdigest.rs | `pub fn reset(&mut self) {` |
| `sort_centroid_list` | tdigest.rs | `fn sort_centroid_list(centroids: &mut [Centroid]) {` |
| `to_json` | tdigest.rs | `pub fn to_json(&mut self) -> Vec<f64> {` |
| `to_json_value` | tdigest.rs | `pub fn to_json_value(&mut self) -> Value {` |
| `unprocessed_size` | tdigest.rs | `fn unprocessed_size(size: usize, compression: f64) -> usize {` |
| `update_cumulative` | tdigest.rs | `fn update_cumulative(&mut self) {` |
| `weighted_average` | tdigest.rs | `fn weighted_average(x1: f64, w1: f64, x2: f64, w2: f64) -> f64 {` |
| `weighted_average_sorted` | tdigest.rs | `fn weighted_average_sorted(x1: f64, w1: f64, x2: f64, w2: f64) -> f64 {` |
| `enabled` | trace.rs | `pub fn enabled() -> bool {` |
| `thread_cpu_ms` | trace.rs | `pub fn thread_cpu_ms() -> f64 {` |
| `connection_count` | workers/cg_executor.rs | `pub fn connection_count(&self) -> u64 {` |
| `default_num_shards` | workers/cg_executor.rs | `pub(crate) fn default_num_shards() -> usize {` |
| `executor_loop` | workers/cg_executor.rs | `async fn executor_loop(` |
| `forward_inbound` | workers/cg_executor.rs | `pub(crate) async fn forward_inbound(` |
| `run_executor` | workers/cg_executor.rs | `pub(crate) fn run_executor(` |
| `send` | workers/cg_executor.rs | `pub fn send(&self, msg: CGMessage) -> Result<(), mpsc::error::SendError<CGMessage>> {` |
| `extract_protocol_version` | workers/connect_params.rs | `pub fn extract_protocol_version(path: &str) -> Option<u32> {` |
| `get_boolean` | workers/connect_params.rs | `fn get_boolean(params: &HashMap<String, String>, name: &str) -> bool {` |
| `get_connect_params` | workers/connect_params.rs | `pub fn get_connect_params(` |
| `get_integer` | workers/connect_params.rs | `fn get_integer(` |
| `get_string` | workers/connect_params.rs | `fn get_string(` |
| `parse_js_integer` | workers/connect_params.rs | `fn parse_js_integer(value: &str) -> Option<i64> {` |
| `query_params_first_wins` | workers/connect_params.rs | `fn query_params_first_wins(parsed: &url::Url) -> HashMap<String, String> {` |
| `classify_error_log_level` | workers/connection.rs | `pub fn classify_error_log_level(error: &ErrorBody, thrown: Option<Thrown<'_>>) -> LogLe…` |
| `client_id` | workers/connection.rs | `pub fn client_id(&self) -> &str {` |
| `close` | workers/connection.rs | `pub fn close(&self, reason: &str) {` |
| `close_with_error` | workers/connection.rs | `pub fn close_with_error(&self, error: ErrorBody) {` |
| `close_with_error_thrown` | workers/connection.rs | `pub fn close_with_error_thrown(&self, error: ErrorBody, thrown: Option<Thrown<'_>>) {` |
| `fail` | workers/connection.rs | `pub fn fail(&self, error: ErrorBody, thrown: Option<Thrown<'_>>) {` |
| `get_log_level` | workers/connection.rs | `fn get_log_level(self) -> LogLevel {` |
| `handle_inbound` | workers/connection.rs | `pub async fn handle_inbound(&self, data: &str) -> bool {` |
| `handle_message` | workers/connection.rs | `async fn handle_message(&self, msg: &str) -> Vec<HandlerResult>;` |
| `handle_result` | workers/connection.rs | `fn handle_result(&self, result: HandlerResult) -> bool {` |
| `has_errno` | workers/connection.rs | `fn has_errno(msg_lower: &str) -> bool {` |
| `has_transient_socket_code` | workers/connection.rs | `fn has_transient_socket_code(msg_lower: &str) -> bool {` |
| `is_closed` | workers/connection.rs | `pub fn is_closed(&self) -> bool {` |
| `is_transient_socket_message` | workers/connection.rs | `fn is_transient_socket_message(msg_lower: &str) -> bool {` |
| `plain` | workers/connection.rs | `pub fn plain(message: impl Into<String>) -> Self {` |
| `protocol_version` | workers/connection.rs | `pub fn protocol_version(&self) -> u32 {` |
| `send_error` | workers/connection.rs | `pub fn send_error(&self, error: ErrorBody) {` |
| `send_error_with_thrown` | workers/connection.rs | `pub fn send_error_with_thrown(&self, error: ErrorBody, thrown: Option<Thrown<'_>>) {` |
| `sink` | workers/connection.rs | `pub fn sink(&self) -> &DirectWebSocketSink {` |
| `thrown` | workers/connection.rs | `pub fn thrown(&self) -> Thrown<'_> {` |
| `ws_id` | workers/connection.rs | `pub fn ws_id(&self) -> &str {` |
| `bound_replica_ready_states` | workers/syncer.rs | `pub fn bound_replica_ready_states(replica_ready_states: &mut Vec<ReplicaReadyState>) {` |
| `broadcast_notification` | workers/syncer.rs | `pub fn broadcast_notification(&self, notification: serde_json::Value) -> usize {` |
| `cg_count` | workers/syncer.rs | `pub fn cg_count(&self) -> usize {` |
| `check_and_pin_user` | workers/syncer.rs | `pub(crate) fn check_and_pin_user(group: &mut GroupAuthState, incoming: &str) -> Result<…` |
| `compute_max_serving_lag_ms` | workers/syncer.rs | `pub fn compute_max_serving_lag_ms<'a>(` |
| `compute_serving_lag_distribution_ms` | workers/syncer.rs | `pub fn compute_serving_lag_distribution_ms<'a>(` |
| `compute_serving_lag_stats_ms` | workers/syncer.rs | `pub fn compute_serving_lag_stats_ms<'a>(` |
| `create_connection` | workers/syncer.rs | `pub async fn create_connection(&self, ctx: ConnectionContext) {` |
| `drain` | workers/syncer.rs | `pub async fn drain(&self) {` |
| `fail_client_current` | workers/syncer.rs | `pub fn fail_client_current(&self, client_id: &str, error: &crate::protocol::ErrorBody) …` |
| `fail_if_current` | workers/syncer.rs | `pub fn fail_if_current(` |
| `find_first_unserved_index` | workers/syncer.rs | `pub fn find_first_unserved_index(` |
| `get_or_create_cg` | workers/syncer.rs | `pub(crate) fn get_or_create_cg(&self, client_group_id: &str) -> Result<Arc<CGHandle>, S…` |
| `lower_bound_replica_ready_time_ms` | workers/syncer.rs | `pub fn lower_bound_replica_ready_time_ms(` |
| `new_sharded` | workers/syncer.rs | `pub fn new_sharded(` |
| `new_with_limit` | workers/syncer.rs | `pub fn new_with_limit(` |
| `percentile_nearest_rank` | workers/syncer.rs | `pub fn percentile_nearest_rank(sorted_values: &[i64], percentile: f64) -> i64 {` |
| `place_cg` | workers/syncer.rs | `pub(crate) fn place_cg(&self, cg_id: &str) -> usize {` |
| `prune_replica_ready_states` | workers/syncer.rs | `pub fn prune_replica_ready_states(` |
| `record_replica_ready_state` | workers/syncer.rs | `pub fn record_replica_ready_state(&self, watermark: &str, replica_ready_time_ms: i64) {` |
| `remove_view_syncer` | workers/syncer.rs | `pub fn remove_view_syncer(&self, cg_id: &str) {` |
| `serving_lag_registry` | workers/syncer.rs | `pub fn serving_lag_registry(&self) -> Arc<crate::workers::syncer::ServingLagRegistry> {` |
| `stats` | workers/syncer.rs | `pub fn stats(&self) -> ServingLagStats {` |
| `upper_bound_watermark` | workers/syncer.rs | `pub fn upper_bound_watermark(replica_ready_states: &[ReplicaReadyState], watermark: &st…` |
| `upsert_view_syncer` | workers/syncer.rs | `pub fn upsert_view_syncer(&self, cg_id: &str, snapshot: CgServingSnapshot) {` |
| `handle_push` | workers/syncer_ws_message_handler.rs | `fn handle_push(` |
| `process_mutation` | workers/syncer_ws_message_handler.rs | `fn process_mutation(` |
| `relay_headers_for` | workers/syncer_ws_message_handler.rs | `fn relay_headers_for(` |
| `with_traceparent` | workers/syncer_ws_message_handler.rs | `fn with_traceparent<F, R>(traceparent: Option<&str>, f: F) -> R` |
| `drain_until_peer_close` | ws_server.rs | `async fn drain_until_peer_close(` |
| `elide` | ws_server.rs | `pub(crate) fn elide(val: &str, max_bytes: usize) -> String {` |
| `is_expected_disconnect` | ws_server.rs | `fn is_expected_disconnect(error: &WebSocketError) -> bool {` |
| `liveness_timeout_ms` | ws_server.rs | `fn liveness_timeout_ms() -> u64 {` |
| `now_epoch_ms` | ws_server.rs | `fn now_epoch_ms() -> i64 {` |
| `run_ws_reader` | ws_server.rs | `async fn run_ws_reader(` |
| `run_ws_writer` | ws_server.rs | `async fn run_ws_writer(` |
| `send_error_and_close` | ws_server.rs | `async fn send_error_and_close(` |
| `cancel` | ws_sink.rs | `fn cancel(&self) {` |
| `close_with_code` | ws_sink.rs | `pub fn close_with_code(&self, code: u16, reason: String) {` |
| `count_shed_once` | ws_sink.rs | `fn count_shed_once(limits: &SinkLimits, reason: &'static str) {` |
| `fail_with_code` | ws_sink.rs | `pub fn fail_with_code(&self, error: ErrorBody, code: Option<u16>) {` |
| `frame_value` | ws_sink.rs | `pub fn frame_value(&self) -> Option<Value> {` |
| `push` | ws_sink.rs | `pub fn push(&self, msg: Value) {` |
| `send_command` | ws_sink.rs | `fn send_command(&self, command: WsCommand) -> Result<(), String> {` |
| `with_limits` | ws_sink.rs | `pub fn with_limits(tx: mpsc::UnboundedSender<WsCommand>, limits: Arc<SinkLimits>) -> Se…` |

## ⚙️ IO — async/DB/actor/transport, use the integration diff — 24

| fn | file | signature |
|---|---|---|
| `bind_http_listener` | http_server.rs | `pub async fn bind_http_listener(addr: SocketAddr) -> tokio::net::TcpListener {` |
| `check_notify_request` | http_server.rs | `fn check_notify_request(` |
| `heapz_handler` | http_server.rs | `async fn heapz_handler(` |
| `notify_broadcast_handler` | http_server.rs | `async fn notify_broadcast_handler(` |
| `notify_handler` | http_server.rs | `async fn notify_handler(` |
| `readyz_handler` | http_server.rs | `async fn readyz_handler(State(state): State<Arc<HttpServerState>>) -> (StatusCode, Json…` |
| `run_http_server` | http_server.rs | `pub async fn run_http_server(addr: SocketAddr, router: Arc<Syncer>) {` |
| `serve_http` | http_server.rs | `pub async fn serve_http(` |
| `statz_handler` | http_server.rs | `async fn statz_handler(` |
| `main` | main.rs | `fn main() {` |
| `shutdown_signal` | main.rs | `async fn shutdown_signal() -> ShutdownSignal {` |
| `handle_close` | workers/connection.rs | `pub fn handle_close(&self, code: u16, reason: &str) {` |
| `handle_error` | workers/connection.rs | `pub fn handle_error(&self, message: &str) {` |
| `handle_init_connection` | workers/connection.rs | `pub async fn handle_init_connection(&self, init_msg_json: &str) -> bool {` |
| `accept_connection` | ws_server.rs | `pub async fn accept_connection(stream: tokio::net::TcpStream) -> Option<ConnectionConte…` |
| `accept_connection_with_limit` | ws_server.rs | `pub async fn accept_connection_with_limit(` |
| `bind_ws_listener` | ws_server.rs | `pub async fn bind_ws_listener(port: u16) -> Result<TcpListener, std::io::Error> {` |
| `downstream_byte_hwm` | ws_server.rs | `fn downstream_byte_hwm() -> i64 {` |
| `downstream_queue_hwm` | ws_server.rs | `fn downstream_queue_hwm() -> i64 {` |
| `run_ws_server` | ws_server.rs | `pub async fn run_ws_server<F>(config: WsServerConfig, handler: F) -> Result<(), std::io…` |
| `serve_ws` | ws_server.rs | `pub async fn serve_ws<F>(listener: TcpListener, handler: F) -> Result<(), std::io::Error>` |
| `serve_ws_with_config` | ws_server.rs | `pub async fn serve_ws_with_config<F>(` |
| `push_poke_part` | ws_sink.rs | `fn push_poke_part(` |
| `push_sized` | ws_sink.rs | `pub fn push_sized(&self, msg: Value, est_bytes: usize) {` |
