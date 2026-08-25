# rust-syncer — Layer-2 (body-differential) coverage

_COVERED = reachable (transitive closure over the crate call graph, incl. fn-pointer edges like `.sort_by(cmp_condition)` / `.any(is_always_false)`) from a differential harness: the in-crate `*_parity_against_ts` fixtures (jwt / read-authorizer hash goldens / url_match / query_covering / serving_lag / e2e_serving_lag / parse_int) + the phase/rowkey/stage integration tests. Reachability ≠ every-branch-exercised._

- Rust fns total **455** · ✅ COVERED **398** · 🟥 GAP (pure, untested) **0** · ⚙️ IO (integration diff) **41** · ◻️ infra/metrics **11** · ◻️ documented n/a **5**
- Body-differential coverage of the **unit-testable pure surface**: **398/398 = 100%**

## 🟥 GAP — pure & deterministic, NO differential fixture (build these) — 0

_none_

## ◻️ NON-DIFFERENTIABLE — documented n/a (no un-pinned body) — 5

| fn | file | why not a body-differential |
|---|---|---|
| `compute_serving_lag_distribution` | workers/syncer.rs | gathers live registry snapshots then calls the already-pinned `compute_serving_lag_distribution_ms` (serving_lag_parity_against_ts); the wrapper reads DashMap state, no un-pinned math |
| `row_set_signature` | services/view_syncer/pipeline_driver.rs | delegates to `rust_ivm engine.row_set_signature` (covered by the rust-ivm oracle); the persisted value is asserted by `stage_e_test` |
| `to_error_body` | services/view_syncer/connection_context_manager.rs | pure CCMError→wire-`ErrorBody` mapping; the wire shapes are pinned by `protocol_test` and the mapping is exercised by the phase2 error-path tests — no single TS `toErrorBody` fn to differentiate against |
| `total_queries` | workers/syncer.rs | trivial getter — sums query counts over the registry snapshots |
| `total_rows` | workers/syncer.rs | trivial getter — sums row counts over the registry snapshots |

## ✅ COVERED — body pinned to TS fixture — 398

| fn | file | signature |
|---|---|---|
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
| `deny_all_permissions` | auth/read_authorizer.rs | `pub fn deny_all_permissions() -> Value {` |
| `flatten` | auth/read_authorizer.rs | `fn flatten(kind: &str, conditions: Vec<Value>) -> Vec<Value> {` |
| `flattened` | auth/read_authorizer.rs | `fn flattened(cond: &Value) -> Option<Value> {` |
| `hash_of_ast` | auth/read_authorizer.rs | `pub fn hash_of_ast(ast: &Value) -> String {` |
| `insert_if_present` | auth/read_authorizer.rs | `fn insert_if_present(out: &mut Map<String, Value>, key: &str, v: Option<&Value>) {` |
| `is_always_false` | auth/read_authorizer.rs | `fn is_always_false(c: &Value) -> bool {` |
| `is_always_true` | auth/read_authorizer.rs | `fn is_always_true(c: &Value) -> bool {` |
| `js_string` | auth/read_authorizer.rs | `fn js_string(v: Option<&Value>) -> String {` |
| `load_permissions` | auth/read_authorizer.rs | `pub fn load_permissions(conn: &Connection, app_id: &str) -> Result<LoadedPermissions, S…` |
| `normalize_ast` | auth/read_authorizer.rs | `pub fn normalize_ast(ast: &Value) -> Value {` |
| `normalize_related_entry` | auth/read_authorizer.rs | `fn normalize_related_entry(r: &Value) -> Value {` |
| `normalize_where` | auth/read_authorizer.rs | `fn normalize_where(cond: &Value) -> Value {` |
| `reload_permissions_if_changed` | auth/read_authorizer.rs | `pub fn reload_permissions_if_changed(` |
| `resolve_field` | auth/read_authorizer.rs | `fn resolve_field(anchor: Option<&Value>, field: Option<&Value>) -> Value {` |
| `resolve_permissions` | auth/read_authorizer.rs | `pub fn resolve_permissions(loaded: Result<Option<Value>, String>) -> Option<Value> {` |
| `simplify_condition` | auth/read_authorizer.rs | `pub fn simplify_condition(c: Value) -> Value {` |
| `transform_and_hash_query` | auth/read_authorizer.rs | `pub fn transform_and_hash_query(` |
| `transform_condition` | auth/read_authorizer.rs | `fn transform_condition(cond: &Value, permissions: &Value) -> Value {` |
| `transform_query` | auth/read_authorizer.rs | `pub fn transform_query(query: &Value, permissions: &Value, auth_data: &Value) -> Value {` |
| `transform_query_internal` | auth/read_authorizer.rs | `fn transform_query_internal(query: &Value, permissions: &Value) -> Value {` |
| `validate_condition_value` | auth/read_authorizer.rs | `fn validate_condition_value(` |
| `validate_permission_asset` | auth/read_authorizer.rs | `fn validate_permission_asset(value: &Value, path: &str) -> Result<(), String> {` |
| `validate_permission_condition` | auth/read_authorizer.rs | `fn validate_permission_condition(value: &Value, path: &str) -> Result<(), String> {` |
| `validate_permissions_config` | auth/read_authorizer.rs | `fn validate_permissions_config(value: &Value) -> Result<(), String> {` |
| `validate_policy` | auth/read_authorizer.rs | `fn validate_policy(value: &Value, path: &str) -> Result<(), String> {` |
| `validate_related_subquery` | auth/read_authorizer.rs | `fn validate_related_subquery(related: &Map<String, Value>, path: &str) -> Result<(), St…` |
| `cache_get` | custom_queries/transform_query.rs | `fn cache_get(ctx: &CustomQueryContext, id: &str) -> Option<TransformedQuery> {` |
| `cache_set` | custom_queries/transform_query.rs | `fn cache_set(ctx: &CustomQueryContext, id: &str, q: &TransformedQuery) {` |
| `composed_headers` | custom_queries/transform_query.rs | `pub fn composed_headers(&self) -> Vec<(String, String)> {` |
| `get_backoff_delay_ms` | custom_queries/transform_query.rs | `fn get_backoff_delay_ms(attempt: u32) -> u64 {` |
| `get_cache_key` | custom_queries/transform_query.rs | `fn get_cache_key(ctx: &CustomQueryContext, id: &str) -> String {` |
| `glob_match` | custom_queries/transform_query.rs | `fn glob_match(p: &[u8], t: &[u8]) -> bool {` |
| `normalized_headers` | custom_queries/transform_query.rs | `fn normalized_headers(headers: &[(String, String)]) -> String {` |
| `post_transform` | custom_queries/transform_query.rs | `async fn post_transform(` |
| `post_transform_attempts` | custom_queries/transform_query.rs | `async fn post_transform_attempts(` |
| `seed_transform_cache_for_test` | custom_queries/transform_query.rs | `pub fn seed_transform_cache_for_test(ctx: &CustomQueryContext, id: &str, q: &Transforme…` |
| `set_header` | custom_queries/transform_query.rs | `fn set_header(headers: &mut Vec<(String, String)>, name: &str, value: String) {` |
| `transform_custom_queries` | custom_queries/transform_query.rs | `pub async fn transform_custom_queries(` |
| `url_match` | custom_queries/transform_query.rs | `pub fn url_match(pattern: &str, url: &str) -> bool {` |
| `compute_table_specs_from_path` | db/lite_tables.rs | `pub fn compute_table_specs_from_path(replica_path: &str) -> Result<Vec<IvmTableSpec>, S…` |
| `compute_zql_specs` | db/lite_tables.rs | `pub fn compute_zql_specs(conn: &Connection) -> Result<Vec<IvmTableSpec>, String> {` |
| `list_tables` | db/lite_tables.rs | `fn list_tables(conn: &Connection) -> Result<Vec<String>, String> {` |
| `list_unique_indexes` | db/lite_tables.rs | `fn list_unique_indexes(conn: &Connection) -> Result<HashMap<String, Vec<Vec<String>>>, …` |
| `lite_type_to_zql_value_type` | db/lite_tables.rs | `pub fn lite_type_to_zql_value_type(lite_type: &str) -> Option<&'static str> {` |
| `open_replica_read_only` | db/lite_tables.rs | `pub fn open_replica_read_only(replica_path: &str) -> Result<Connection, String> {` |
| `read_min_row_versions` | db/lite_tables.rs | `fn read_min_row_versions(conn: &Connection) -> Result<HashMap<String, String>, String> {` |
| `read_replica_versions` | db/lite_tables.rs | `pub fn read_replica_versions(conn: &Connection) -> Result<ReplicaVersions, String> {` |
| `read_replica_versions_from_path` | db/lite_tables.rs | `pub fn read_replica_versions_from_path(replica_path: &str) -> Result<ReplicaVersions, S…` |
| `read_table_spec` | db/lite_tables.rs | `fn read_table_spec(` |
| `validate_client_schema` | db/lite_tables.rs | `pub fn validate_client_schema(` |
| `zql_type_for_upstream` | db/lite_tables.rs | `fn zql_type_for_upstream(pg_type: &str) -> Option<&'static str> {` |
| `census_handler` | http_server.rs | `async fn census_handler() -> impl IntoResponse {` |
| `check_admin_auth` | http_server.rs | `fn check_admin_auth(` |
| `dec` | live_count.rs | `pub fn dec(c: &AtomicI64) {` |
| `drop` | live_count.rs | `fn drop(&mut self) {` |
| `drop_backtrace` | live_count.rs | `pub fn drop_backtrace(context: &str) {` |
| `inc` | live_count.rs | `pub fn inc(c: &AtomicI64) {` |
| `new` | live_count.rs | `pub fn new(counter: &'static AtomicI64) -> Self {` |
| `snapshot` | live_count.rs | `pub fn snapshot() -> String {` |
| `change_desired_queries` | main.rs | `fn change_desired_queries(&self, _selector: &rust_syncer::ConnectionSelector, _msg: &st…` |
| `create_conn_context_manager` | main.rs | `fn create_conn_context_manager(` |
| `create_mutagen` | main.rs | `fn create_mutagen(&self, _cg_id: &str) -> Option<Arc<dyn rust_syncer::MutagenDispatch>> {` |
| `create_pusher` | main.rs | `fn create_pusher(&self, _cg_id: &str) -> Option<Arc<dyn rust_syncer::PusherDispatch>> {` |
| `create_sync_engine_config` | main.rs | `fn create_sync_engine_config(&self, cg_id: &str) -> rust_syncer::SyncEngineConfig {` |
| `create_view_syncer` | main.rs | `fn create_view_syncer(&self, _cg_id: &str) -> Arc<dyn rust_syncer::ViewSyncerDispatch> {` |
| `delete_clients` | main.rs | `fn delete_clients(` |
| `init_connection` | main.rs | `fn init_connection(&self, _selector: &rust_syncer::ConnectionSelector, _msg: &str) -> b…` |
| `inspect` | main.rs | `fn inspect(&self, _selector: &rust_syncer::ConnectionSelector, _msg: &str) {}` |
| `must_get_connection_context` | main.rs | `fn must_get_connection_context(` |
| `parse_cpu_max` | main.rs | `fn parse_cpu_max(s: &str) -> Option<usize> {` |
| `update_auth` | main.rs | `fn update_auth(&self, _selector: &rust_syncer::ConnectionSelector, _msg: &str, _changed…` |
| `active_clients` | metrics.rs | `fn active_clients() -> &'static UpDownCounter<i64> {` |
| `add` | metrics.rs | `pub fn add(field: &AtomicU64, n: u64) {` |
| `api_otel` | metrics.rs | `fn api_otel() -> &'static ApiOtel {` |
| `api_request_metric_attrs` | metrics.rs | `fn api_request_metric_attrs(result: &'static str) -> [opentelemetry::KeyValue; 2] {` |
| `cvr_attempt_otel` | metrics.rs | `fn cvr_attempt_otel() -> &'static CvrAttemptOtel {` |
| `cvr_flush_failures` | metrics.rs | `fn cvr_flush_failures() -> &'static Counter<u64> {` |
| `default` | metrics.rs | `fn default() -> Self {` |
| `failed_client_groups` | metrics.rs | `fn failed_client_groups() -> &'static Counter<u64> {` |
| `now_ms` | metrics.rs | `fn now_ms() -> i64 {` |
| `observe_millis` | metrics.rs | `pub fn observe_millis(&self, ms: f64) {` |
| `observe_secs` | metrics.rs | `pub fn observe_secs(&self, v: f64) {` |
| `proto_attr` | metrics.rs | `fn proto_attr(protocol_version: u32) -> KeyValue {` |
| `query_transform_otel` | metrics.rs | `fn query_transform_otel() -> &'static QueryTransformOtel {` |
| `record_active_client_delta` | metrics.rs | `pub fn record_active_client_delta(delta: i64, protocol_version: u32) {` |
| `record_advance` | metrics.rs | `pub fn record_advance(&self, elapsed_ms: f64) {` |
| `record_api_attempt` | metrics.rs | `pub fn record_api_attempt(` |
| `record_api_in_flight` | metrics.rs | `pub fn record_api_in_flight(delta: i64) {` |
| `record_api_request` | metrics.rs | `pub fn record_api_request(result: &'static str) {` |
| `record_api_request_duration` | metrics.rs | `pub fn record_api_request_duration(elapsed_ms: f64) {` |
| `record_cvr_flush_attempt` | metrics.rs | `pub fn record_cvr_flush_attempt(success: bool) {` |
| `record_cvr_flush_failure` | metrics.rs | `pub fn record_cvr_flush_failure() {` |
| `record_cvr_load_attempt` | metrics.rs | `pub fn record_cvr_load_attempt(success: bool, elapsed_ms: f64) {` |
| `record_e2e_serving_lag` | metrics.rs | `pub fn record_e2e_serving_lag(lag_ms: f64) {` |
| `record_e2e_serving_lag_clamp` | metrics.rs | `pub fn record_e2e_serving_lag_clamp() {` |
| `record_fail_group` | metrics.rs | `pub fn record_fail_group(reason: &'static str) {` |
| `record_hydration` | metrics.rs | `pub fn record_hydration(&self, elapsed_ms: f64) {` |
| `record_query_transformation` | metrics.rs | `pub fn record_query_transformation(success: bool) {` |
| `record_query_transformation_hash_change` | metrics.rs | `pub fn record_query_transformation_hash_change() {` |
| `record_query_transformation_no_op` | metrics.rs | `pub fn record_query_transformation_no_op() {` |
| `record_query_transformation_time` | metrics.rs | `pub fn record_query_transformation_time(elapsed_ms: f64) {` |
| `record_reset` | metrics.rs | `pub fn record_reset(&self, reason: &str) {` |
| `record_view_syncer_hydration` | metrics.rs | `pub fn record_view_syncer_hydration(elapsed_ms: f64) {` |
| `record_ws_connection_failure` | metrics.rs | `pub fn record_ws_connection_failure(protocol_version: u32, reason: &str) {` |
| `record_ws_connection_success` | metrics.rs | `pub fn record_ws_connection_success(protocol_version: u32) {` |
| `record_ws_queued_bytes_delta` | metrics.rs | `pub fn record_ws_queued_bytes_delta(delta: i64) {` |
| `record_ws_queued_delta` | metrics.rs | `pub fn record_ws_queued_delta(delta: i64) {` |
| `record_ws_shed` | metrics.rs | `pub fn record_ws_shed(reason: &'static str) {` |
| `render` | metrics.rs | `fn render(&self, name: &str, help: &str, out: &mut String) {` |
| `render_prometheus` | metrics.rs | `pub fn render_prometheus(&self, active_client_groups: u64) -> String {` |
| `serving_lag_otel` | metrics.rs | `fn serving_lag_otel() -> &'static ServingLagOtel {` |
| `view_syncer_hydration_otel` | metrics.rs | `fn view_syncer_hydration_otel() -> &'static OtelHistogram<f64> {` |
| `ws_connection_failures` | metrics.rs | `fn ws_connection_failures() -> &'static Counter<u64> {` |
| `ws_connection_successes` | metrics.rs | `fn ws_connection_successes() -> &'static Counter<u64> {` |
| `ws_queued_bytes_gauge` | metrics.rs | `fn ws_queued_bytes_gauge() -> &'static opentelemetry::metrics::ObservableGauge<i64> {` |
| `ws_queued_frames_gauge` | metrics.rs | `fn ws_queued_frames_gauge() -> &'static opentelemetry::metrics::ObservableGauge<i64> {` |
| `ws_sheds` | metrics.rs | `fn ws_sheds() -> &'static Counter<u64> {` |
| `basic` | protocol.rs | `pub fn basic(kind: ErrorKind, message: String) -> Self {` |
| `client_not_found` | protocol.rs | `pub fn client_not_found(message: impl Into<String>) -> Self {` |
| `connected_message` | protocol.rs | `pub fn connected_message(wsid: &str, app_id: &str, shard_num: u32) -> Value {` |
| `decode_sec_protocols` | protocol.rs | `pub fn decode_sec_protocols(header: &str) -> Result<SecProtocols, DecodeError> {` |
| `downstream_message` | protocol.rs | `pub fn downstream_message(msg_type: &str, body: &impl Serialize) -> Value {` |
| `error_message` | protocol.rs | `pub fn error_message(body: &ErrorBody) -> Value {` |
| `internal` | protocol.rs | `pub fn internal(message: impl Into<String>) -> Self {` |
| `invalid_message` | protocol.rs | `pub fn invalid_message(message: impl Into<String>) -> Self {` |
| `invalid_push` | protocol.rs | `pub fn invalid_push(message: impl Into<String>) -> Self {` |
| `kind` | protocol.rs | `pub fn kind(&self) -> &ErrorKind {` |
| `message` | protocol.rs | `pub fn message(&self) -> &str {` |
| `parse_upstream` | protocol.rs | `pub fn parse_upstream(text: &str) -> Result<Upstream, serde_json::Error> {` |
| `parse_upstream_array` | protocol.rs | `pub fn parse_upstream_array(arr: &[Value]) -> Result<Upstream, serde_json::Error> {` |
| `pong_message` | protocol.rs | `pub fn pong_message() -> Value {` |
| `rehome` | protocol.rs | `pub fn rehome(message: impl Into<String>) -> Self {` |
| `unauthorized` | protocol.rs | `pub fn unauthorized(message: impl Into<String>) -> Self {` |
| `version_not_supported` | protocol.rs | `pub fn version_not_supported(message: impl Into<String>) -> Self {` |
| `ack_mutation_responses` | push_relay.rs | `fn ack_mutation_responses(` |
| `cleanup_push_body` | push_relay.rs | `fn cleanup_push_body(` |
| `delete_client_mutations` | push_relay.rs | `fn delete_client_mutations(` |
| `enqueue_payload` | push_relay.rs | `fn enqueue_payload(&self, push: QueuedPush, what: &str) -> bool {` |
| `enqueue_push` | push_relay.rs | `fn enqueue_push(` |
| `mutation_ids_of` | push_relay.rs | `fn mutation_ids_of(push_body: &serde_json::Value) -> Vec<MutationID> {` |
| `queue_cap` | push_relay.rs | `fn queue_cap() -> i64 {` |
| `read_body_preview` | push_relay.rs | `async fn read_body_preview(resp: reqwest::Response, cap: usize) -> Option<String> {` |
| `relay_body` | push_relay.rs | `fn relay_body(` |
| `apply_client_deletions` | router.rs | `async fn apply_client_deletions(` |
| `arm_auth_maintenance` | router.rs | `fn arm_auth_maintenance(&mut self) {` |
| `arm_serving_lag` | router.rs | `fn arm_serving_lag(&mut self, notification: &serde_json::Value) {` |
| `broadcast_notification` | router.rs | `pub fn broadcast_notification(&self, notification: serde_json::Value) -> usize {` |
| `cg_count` | router.rs | `pub fn cg_count(&self) -> usize {` |
| `cg_event_loop` | router.rs | `async fn cg_event_loop(` |
| `check_and_pin_user` | router.rs | `fn check_and_pin_user(group: &mut GroupAuthState, incoming: &str) -> Result<(), ()> {` |
| `check_client_and_cvr_versions` | router.rs | `fn check_client_and_cvr_versions(` |
| `clients_to_delete` | router.rs | `fn clients_to_delete(` |
| `close_connection` | router.rs | `fn close_connection(&mut self, client_id: &str, ws_id: &str) {` |
| `connection_count` | router.rs | `pub fn connection_count(&self) -> u64 {` |
| `decrement_active_client` | router.rs | `fn decrement_active_client(&mut self, ws_id: &str) {` |
| `decrement_nonzero` | router.rs | `fn decrement_nonzero(count: &AtomicU64) {` |
| `default_num_shards` | router.rs | `fn default_num_shards() -> usize {` |
| `default_query_context` | router.rs | `fn default_query_context(` |
| `dispatch_cg_message` | router.rs | `async fn dispatch_cg_message(` |
| `drain` | router.rs | `pub async fn drain(&self) {` |
| `drop_registration` | router.rs | `fn drop_registration(&mut self, client_id: &str, ws_id: &str) {` |
| `ensure_cvr` | router.rs | `async fn ensure_cvr(` |
| `executor_loop` | router.rs | `async fn executor_loop(` |
| `fail_group` | router.rs | `fn fail_group(&mut self, message: &str) {` |
| `fail_group_with_error` | router.rs | `fn fail_group_with_error(&mut self, error: crate::protocol::ErrorBody) {` |
| `filtered_query_headers` | router.rs | `fn filtered_query_headers(` |
| `forward_inbound` | router.rs | `async fn forward_inbound(` |
| `get_or_create_cg` | router.rs | `fn get_or_create_cg(&self, client_group_id: &str) -> Result<Arc<CGHandle>, String> {` |
| `get_ttl_clock` | router.rs | `fn get_ttl_clock(&mut self, now: i64) -> TTLClock {` |
| `handle_connection` | router.rs | `pub async fn handle_connection(&self, ctx: ConnectionContext) {` |
| `handle_desired_queries` | router.rs | `async fn handle_desired_queries(` |
| `handle_inspect` | router.rs | `async fn handle_inspect(&mut self, client_id: &str, body: &serde_json::Value) {` |
| `handle_update_auth` | router.rs | `async fn handle_update_auth(&mut self, client_id: &str, token: &str) {` |
| `idle_shutdown_due` | router.rs | `fn idle_shutdown_due(&self) -> bool {` |
| `inspect_queries_value` | router.rs | `async fn inspect_queries_value(` |
| `lock_unpoisoned` | router.rs | `pub(crate) fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {` |
| `mark_version_served` | router.rs | `fn mark_version_served(&mut self, version: &CVRVersion) {` |
| `maybe_reload_permissions` | router.rs | `fn maybe_reload_permissions(&mut self) -> bool {` |
| `merge_notifications` | router.rs | `fn merge_notifications(prev: serde_json::Value, next: serde_json::Value) -> serde_json:…` |
| `new_sharded` | router.rs | `pub fn new_sharded(` |
| `new_with_accepting` | router.rs | `fn new_with_accepting(` |
| `new_with_limit` | router.rs | `pub fn new_with_limit(` |
| `next_auth_maintenance_delay` | router.rs | `fn next_auth_maintenance_delay(&self) -> Option<Duration> {` |
| `next_expiry_delay` | router.rs | `fn next_expiry_delay(&self) -> Option<Duration> {` |
| `next_idle_shutdown_delay` | router.rs | `fn next_idle_shutdown_delay(&self) -> Option<Duration> {` |
| `older_replica_error` | router.rs | `fn older_replica_error(cvr: &CVR, replica_version: &str) -> Option<String> {` |
| `on_auth_maintenance_tick` | router.rs | `async fn on_auth_maintenance_tick(&mut self) {` |
| `on_connection_closed` | router.rs | `fn on_connection_closed(&mut self, client_id: &str, ws_id: &str) {` |
| `on_expiry_tick` | router.rs | `async fn on_expiry_tick(&mut self) {` |
| `on_inbound` | router.rs | `async fn on_inbound(&mut self, client_id: Arc<str>, ws_id: Arc<str>, text: String) {` |
| `on_new_connection` | router.rs | `async fn on_new_connection(&mut self, params: ConnectParams, sink: DirectWebSocketSink) {` |
| `on_notification` | router.rs | `async fn on_notification(&mut self, notification: serde_json::Value) {` |
| `parse_desired_queries_patch` | router.rs | `fn parse_desired_queries_patch(` |
| `place_cg` | router.rs | `fn place_cg(&self, cg_id: &str) -> usize {` |
| `publish_serving_lag` | router.rs | `fn publish_serving_lag(&mut self) {` |
| `query_count` | router.rs | `fn query_count(&mut self) -> usize {` |
| `reset_pipelines_and_rehydrate` | router.rs | `async fn reset_pipelines_and_rehydrate(&mut self, cvr: CVR, reason: &str) {` |
| `row_count` | router.rs | `fn row_count(&self) -> usize {` |
| `run_executor` | router.rs | `fn run_executor(` |
| `send` | router.rs | `pub fn send(&self, msg: CGMessage) -> Result<(), mpsc::error::SendError<CGMessage>> {` |
| `send_error_if_current` | router.rs | `pub fn send_error_if_current(` |
| `serving_lag_eligible` | router.rs | `fn serving_lag_eligible(&self) -> bool {` |
| `serving_lag_registry` | router.rs | `pub fn serving_lag_registry(&self) -> Arc<crate::workers::syncer::ServingLagRegistry> {` |
| `shard_for` | router.rs | `fn shard_for(cg_id: &str, num_shards: usize) -> usize {` |
| `shutdown` | router.rs | `pub fn shutdown(&mut self) {` |
| `slow_hydrate_threshold_ms` | router.rs | `fn slow_hydrate_threshold_ms() -> f64 {` |
| `str_array` | router.rs | `fn str_array(v: Option<&serde_json::Value>) -> Vec<String> {` |
| `auth_equals` | services/view_syncer/connection_context_manager.rs | `pub fn auth_equals(a: Option<&Auth>, b: Option<&Auth>) -> bool {` |
| `build_fetch_context` | services/view_syncer/connection_context_manager.rs | `fn build_fetch_context(` |
| `compare_by_insertion_order` | services/view_syncer/connection_context_manager.rs | `fn compare_by_insertion_order(a: &ConnectionContext, b: &ConnectionContext) -> std::cmp…` |
| `compare_preferred_validated_connection` | services/view_syncer/connection_context_manager.rs | `fn compare_preferred_validated_connection(` |
| `defer_maintenance` | services/view_syncer/connection_context_manager.rs | `pub fn defer_maintenance(&mut self, kind: MaintenanceKind) {` |
| `demote_connection` | services/view_syncer/connection_context_manager.rs | `fn demote_connection(&mut self, connection: ConnectionContext) -> ConnectionContext {` |
| `fail_connection` | services/view_syncer/connection_context_manager.rs | `pub fn fail_connection(` |
| `get_background_connection_context` | services/view_syncer/connection_context_manager.rs | `pub fn get_background_connection_context(&self) -> Option<ConnectionContext> {` |
| `get_connection_context` | services/view_syncer/connection_context_manager.rs | `pub fn get_connection_context(` |
| `get_group_state` | services/view_syncer/connection_context_manager.rs | `pub fn get_group_state(&self) -> &GroupAuthState {` |
| `mark_background_retransform_success` | services/view_syncer/connection_context_manager.rs | `pub fn mark_background_retransform_success(` |
| `min_defined` | services/view_syncer/connection_context_manager.rs | `fn min_defined(a: Option<i64>, b: Option<i64>) -> Option<i64> {` |
| `must_get_background_connection_context` | services/view_syncer/connection_context_manager.rs | `pub fn must_get_background_connection_context(&self) -> Result<ConnectionContext, CCMEr…` |
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
| `active_query_ids` | services/view_syncer/pipeline_driver.rs | `pub fn active_query_ids(&self) -> Vec<String> {` |
| `advance` | services/view_syncer/pipeline_driver.rs | `pub fn advance<H, F>(` |
| `build_engine` | services/view_syncer/pipeline_driver.rs | `fn build_engine(&mut self, tables: &[IvmTableSpec], source_conn: Option<SharedConn>) {` |
| `column_schema` | services/view_syncer/pipeline_driver.rs | `fn column_schema(v: &IvmColumnSchema) -> ColumnSchema {` |
| `column_type` | services/view_syncer/pipeline_driver.rs | `fn column_type(type_str: &str, optional: bool) -> ColumnType {` |
| `convert_ast` | services/view_syncer/pipeline_driver.rs | `fn convert_ast(ts: TsAst) -> rust_ivm::builder::ast::Ast {` |
| `convert_condition` | services/view_syncer/pipeline_driver.rs | `fn convert_condition(c: TsCondition) -> rust_ivm::builder::ast::Condition {` |
| `convert_csq` | services/view_syncer/pipeline_driver.rs | `fn convert_csq(c: &TsCorrelatedSubquery) -> rust_ivm::builder::ast::RelatedSubquery {` |
| `convert_value_position` | services/view_syncer/pipeline_driver.rs | `fn convert_value_position(vp: TsValuePosition) -> rust_ivm::builder::ast::ValuePosition {` |
| `current_version` | services/view_syncer/pipeline_driver.rs | `pub fn current_version(&self) -> Option<String> {` |
| `destroy` | services/view_syncer/pipeline_driver.rs | `pub fn destroy(&mut self) {` |
| `get_row` | services/view_syncer/pipeline_driver.rs | `pub fn get_row(&self, table: &str, pk: &[(String, Value)]) -> Option<Row> {` |
| `has_query` | services/view_syncer/pipeline_driver.rs | `pub fn has_query(&self, query_id: &str) -> bool {` |
| `hydrate` | services/view_syncer/pipeline_driver.rs | `pub fn hydrate<F: FnMut(&RowChange)>(` |
| `init` | services/view_syncer/pipeline_driver.rs | `pub fn init(` |
| `init_from_connection` | services/view_syncer/pipeline_driver.rs | `pub fn init_from_connection(` |
| `initialized` | services/view_syncer/pipeline_driver.rs | `pub fn initialized(&self) -> bool {` |
| `json_to_value` | services/view_syncer/pipeline_driver.rs | `pub(crate) fn json_to_value(v: serde_json::Value) -> rust_ivm::ivm::data::Value {` |
| `panic_message` | services/view_syncer/pipeline_driver.rs | `fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {` |
| `parse_ts_ast` | services/view_syncer/pipeline_driver.rs | `pub fn parse_ts_ast(json: &str) -> Result<rust_ivm::builder::ast::Ast, String> {` |
| `query_transformation_hash` | services/view_syncer/pipeline_driver.rs | `pub fn query_transformation_hash(&self, query_id: &str) -> Option<&str> {` |
| `remove_query` | services/view_syncer/pipeline_driver.rs | `pub fn remove_query(&mut self, query_id: &str) {` |
| `running_queries` | services/view_syncer/pipeline_driver.rs | `pub fn running_queries(&self) -> Vec<(String, std::sync::Arc<str>, String)> {` |
| `scalar_reset_message` | services/view_syncer/pipeline_driver.rs | `fn scalar_reset_message(payload: &Box<dyn std::any::Any + Send>) -> Option<String> {` |
| `set_client_primary_keys` | services/view_syncer/pipeline_driver.rs | `pub fn set_client_primary_keys(&mut self, client_primary_keys: HashMap<String, Vec<Stri…` |
| `set_query_transformation_hash` | services/view_syncer/pipeline_driver.rs | `pub fn set_query_transformation_hash(&mut self, query_id: &str, hash: &str) {` |
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
| `accumulate_signature` | sync_engine.rs | `fn accumulate_signature(acc: &mut HashMap<String, u64>, rc: &rust_ivm::streamer::RowCha…` |
| `advance_and_sync` | sync_engine.rs | `pub async fn advance_and_sync(` |
| `advance_poke_targets` | sync_engine.rs | `fn advance_poke_targets(` |
| `catchup_clients` | sync_engine.rs | `pub async fn catchup_clients(` |
| `catchup_floor` | sync_engine.rs | `fn catchup_floor(` |
| `client_primary_keys_from_schema` | sync_engine.rs | `fn client_primary_keys_from_schema(` |
| `clients_for` | sync_engine.rs | `fn clients_for(&self, ws_ids: &[String]) -> Vec<Arc<ClientHandler>> {` |
| `config_and_hydrate` | sync_engine.rs | `pub async fn config_and_hydrate(` |
| `config_and_hydrate_with_profile` | sync_engine.rs | `pub async fn config_and_hydrate_with_profile(` |
| `config_poke_targets` | sync_engine.rs | `fn config_poke_targets(` |
| `empty_cvr` | sync_engine.rs | `pub fn empty_cvr(id: &str, replica_version: &str) -> CVR {` |
| `existing_rows` | sync_engine.rs | `pub async fn existing_rows(&self) -> Arc<RowRecordMap> {` |
| `fail_client` | sync_engine.rs | `pub fn fail_client(&self, ws_id: &str, msg: &str) -> bool {` |
| `flush_ops_to_store` | sync_engine.rs | `async fn flush_ops_to_store(` |
| `flush_to_store` | sync_engine.rs | `async fn flush_to_store(` |
| `gather_catchup_patches` | sync_engine.rs | `async fn gather_catchup_patches(` |
| `hydrate_and_sync` | sync_engine.rs | `pub async fn hydrate_and_sync(` |
| `inspect_queries` | sync_engine.rs | `pub async fn inspect_queries(` |
| `load_cvr` | sync_engine.rs | `pub async fn load_cvr(&self, last_connect_time: f64) -> Result<Option<CVR>, LoadCvrErro…` |
| `offload` | sync_engine.rs | `async fn offload<F, T>(&self, fut: F) -> T` |
| `pipelines` | sync_engine.rs | `pub fn pipelines(&mut self) -> &mut IvmPipelines {` |
| `query_name_of` | sync_engine.rs | `fn query_name_of(cvr: &CVR, qid: &str) -> Option<String> {` |
| `register_client` | sync_engine.rs | `pub fn register_client(` |
| `remove_expired_queries` | sync_engine.rs | `pub async fn remove_expired_queries(` |
| `row_change_to_maps` | sync_engine.rs | `fn row_change_to_maps(rc: &rust_ivm::streamer::RowChange) -> Option<RowChangeMaps> {` |
| `row_op_is_noop` | sync_engine.rs | `fn row_op_is_noop(op: &StoreOp, existing: &RowRecordMap) -> bool {` |
| `row_to_contents` | sync_engine.rs | `fn row_to_contents(row: &rust_ivm::ivm::data::Row) -> serde_json::Value {` |
| `seed_signatures_from_cvr` | sync_engine.rs | `fn seed_signatures_from_cvr(cvr: &CVR) -> HashMap<String, u64> {` |
| `send_inspect_response` | sync_engine.rs | `pub fn send_inspect_response(&self, ws_id: &str, response: serde_json::Value) {` |
| `set_cvr_store` | sync_engine.rs | `pub fn set_cvr_store(` |
| `set_enable_query_covering` | sync_engine.rs | `pub fn set_enable_query_covering(&mut self, enabled: bool) {` |
| `set_tokio_handle` | sync_engine.rs | `pub fn set_tokio_handle(&mut self, handle: tokio::runtime::Handle) {` |
| `signature_provider` | sync_engine.rs | `fn signature_provider() -> (` |
| `sqlite_real_to_json` | sync_engine.rs | `fn sqlite_real_to_json(value: f64) -> serde_json::Value {` |
| `unregister_client` | sync_engine.rs | `pub fn unregister_client(&mut self, ws_id: &str) {` |
| `value_to_serde_json` | sync_engine.rs | `fn value_to_serde_json(v: &rust_ivm::ivm::data::Value) -> serde_json::Value {` |
| `enabled` | trace.rs | `pub fn enabled() -> bool {` |
| `note` | trace.rs | `pub fn note(op: &str, msg: &str) {` |
| `extract_protocol_version` | workers/connect_params.rs | `pub fn extract_protocol_version(path: &str) -> Option<u32> {` |
| `get_boolean` | workers/connect_params.rs | `fn get_boolean(params: &HashMap<String, String>, name: &str) -> bool {` |
| `get_connect_params` | workers/connect_params.rs | `pub fn get_connect_params(` |
| `get_integer` | workers/connect_params.rs | `fn get_integer(` |
| `get_string` | workers/connect_params.rs | `fn get_string(` |
| `parse_js_integer` | workers/connect_params.rs | `fn parse_js_integer(value: &str) -> Option<i64> {` |
| `classify_error_log_level` | workers/connection.rs | `pub fn classify_error_log_level(error: &ErrorBody) -> LogLevel {` |
| `client_id` | workers/connection.rs | `pub fn client_id(&self) -> &str {` |
| `close` | workers/connection.rs | `pub fn close(&self, reason: &str) {` |
| `close_with_error` | workers/connection.rs | `pub fn close_with_error(&self, error: ErrorBody) {` |
| `handle_inbound` | workers/connection.rs | `pub fn handle_inbound(&self, data: &str) -> bool {` |
| `handle_message` | workers/connection.rs | `fn handle_message(&self, msg: &str) -> Vec<HandlerResult>;` |
| `handle_result` | workers/connection.rs | `fn handle_result(&self, result: HandlerResult) -> bool {` |
| `send_error` | workers/connection.rs | `pub fn send_error(&self, error: ErrorBody) {` |
| `ws_id` | workers/connection.rs | `pub fn ws_id(&self) -> &str {` |
| `active_client_groups` | workers/syncer.rs | `pub fn active_client_groups(&self) -> usize {` |
| `bound_replica_ready_states` | workers/syncer.rs | `pub fn bound_replica_ready_states(replica_ready_states: &mut Vec<ReplicaReadyState>) {` |
| `compute_max_serving_lag_ms` | workers/syncer.rs | `pub fn compute_max_serving_lag_ms<'a>(` |
| `compute_serving_lag_distribution_ms` | workers/syncer.rs | `pub fn compute_serving_lag_distribution_ms<'a>(` |
| `compute_serving_lag_stats_ms` | workers/syncer.rs | `pub fn compute_serving_lag_stats_ms<'a>(` |
| `find_first_unserved_index` | workers/syncer.rs | `pub fn find_first_unserved_index(` |
| `lower_bound_replica_ready_time_ms` | workers/syncer.rs | `pub fn lower_bound_replica_ready_time_ms(` |
| `percentile_nearest_rank` | workers/syncer.rs | `pub fn percentile_nearest_rank(sorted_values: &[i64], percentile: f64) -> i64 {` |
| `prune_replica_ready_states` | workers/syncer.rs | `pub fn prune_replica_ready_states(` |
| `record_replica_ready_state` | workers/syncer.rs | `pub fn record_replica_ready_state(&self, watermark: &str, replica_ready_time_ms: i64) {` |
| `remove_view_syncer` | workers/syncer.rs | `pub fn remove_view_syncer(&self, cg_id: &str) {` |
| `stats` | workers/syncer.rs | `pub fn stats(&self) -> ServingLagStats {` |
| `upper_bound_watermark` | workers/syncer.rs | `pub fn upper_bound_watermark(replica_ready_states: &[ReplicaReadyState], watermark: &st…` |
| `upsert_view_syncer` | workers/syncer.rs | `pub fn upsert_view_syncer(&self, cg_id: &str, snapshot: CgServingSnapshot) {` |
| `handle_push` | workers/syncer_ws_message_handler.rs | `fn handle_push(` |
| `process_mutation` | workers/syncer_ws_message_handler.rs | `fn process_mutation(` |
| `with_traceparent` | workers/syncer_ws_message_handler.rs | `fn with_traceparent<F, R>(traceparent: Option<&str>, f: F) -> R` |
| `is_expected_disconnect` | ws_server.rs | `fn is_expected_disconnect(error: &WebSocketError) -> bool {` |
| `cancel` | ws_sink.rs | `fn cancel(&self) {` |
| `count_shed_once` | ws_sink.rs | `fn count_shed_once(limits: &SinkLimits, reason: &'static str) {` |
| `fail` | ws_sink.rs | `pub fn fail(&self, error: ErrorBody) {` |
| `push` | ws_sink.rs | `pub fn push(&self, msg: Value) {` |
| `send_command` | ws_sink.rs | `fn send_command(&self, command: WsCommand) -> Result<(), String> {` |
| `with_limits` | ws_sink.rs | `pub fn with_limits(tx: mpsc::UnboundedSender<WsCommand>, limits: Arc<SinkLimits>) -> Se…` |

## ⚙️ IO — async/DB/actor/transport, use the integration diff — 41

| fn | file | signature |
|---|---|---|
| `bind_http_listener` | http_server.rs | `pub async fn bind_http_listener(addr: SocketAddr) -> tokio::net::TcpListener {` |
| `check_notify_request` | http_server.rs | `fn check_notify_request(` |
| `heapz_handler` | http_server.rs | `async fn heapz_handler(` |
| `metrics_handler` | http_server.rs | `async fn metrics_handler(State(state): State<Arc<HttpServerState>>) -> impl IntoResponse {` |
| `notify_broadcast_handler` | http_server.rs | `async fn notify_broadcast_handler(` |
| `notify_handler` | http_server.rs | `async fn notify_handler(` |
| `readyz_handler` | http_server.rs | `async fn readyz_handler(State(state): State<Arc<HttpServerState>>) -> (StatusCode, Json…` |
| `run_http_server` | http_server.rs | `pub async fn run_http_server(addr: SocketAddr, router: Arc<ConnectionRouter>) {` |
| `serve_http` | http_server.rs | `pub async fn serve_http(` |
| `statz_handler` | http_server.rs | `async fn statz_handler(` |
| `cgroup_cpu_quota_cores` | main.rs | `fn cgroup_cpu_quota_cores() -> Option<usize> {` |
| `from_env` | main.rs | `pub fn from_env() -> Self {` |
| `host_parallelism` | main.rs | `fn host_parallelism() -> usize {` |
| `main` | main.rs | `fn main() {` |
| `parse_query_config` | main.rs | `fn parse_query_config() -> Option<rust_syncer::FetchConfig> {` |
| `shutdown_signal` | main.rs | `async fn shutdown_signal() -> ShutdownSignal {` |
| `warn_if_quota_capped` | main.rs | `fn warn_if_quota_capped() {` |
| `metrics_prometheus` | router.rs | `pub fn metrics_prometheus(&self) -> String {` |
| `metrics_snapshot` | router.rs | `pub fn metrics_snapshot(&self) -> serde_json::Value {` |
| `send_notification` | router.rs | `pub fn send_notification(&self, cg_id: &str, notification: serde_json::Value) -> bool {` |
| `parse_existing_rows` | sync_engine.rs | `pub fn parse_existing_rows(json: &str) -> Result<RowRecordMap, String> {` |
| `handle_close` | workers/connection.rs | `pub fn handle_close(&self, code: u16, reason: &str) {` |
| `handle_error` | workers/connection.rs | `pub fn handle_error(&self, message: &str) {` |
| `handle_init_connection` | workers/connection.rs | `pub fn handle_init_connection(&self, init_msg_json: &str) -> bool {` |
| `is_closed` | workers/connection.rs | `pub fn is_closed(&self) -> bool {` |
| `maybe_send_pong` | workers/connection.rs | `pub fn maybe_send_pong(&self) {` |
| `accept_connection` | ws_server.rs | `pub async fn accept_connection(stream: tokio::net::TcpStream) -> Option<ConnectionConte…` |
| `accept_connection_with_limit` | ws_server.rs | `pub async fn accept_connection_with_limit(` |
| `bind_ws_listener` | ws_server.rs | `pub async fn bind_ws_listener(port: u16) -> Result<TcpListener, std::io::Error> {` |
| `downstream_byte_hwm` | ws_server.rs | `fn downstream_byte_hwm() -> i64 {` |
| `downstream_queue_hwm` | ws_server.rs | `fn downstream_queue_hwm() -> i64 {` |
| `liveness_timeout_ms` | ws_server.rs | `fn liveness_timeout_ms() -> u64 {` |
| `now_epoch_ms` | ws_server.rs | `fn now_epoch_ms() -> i64 {` |
| `run_ws_reader` | ws_server.rs | `async fn run_ws_reader(` |
| `run_ws_server` | ws_server.rs | `pub async fn run_ws_server<F>(config: WsServerConfig, handler: F) -> Result<(), std::io…` |
| `run_ws_writer` | ws_server.rs | `async fn run_ws_writer(` |
| `send_error_and_close` | ws_server.rs | `async fn send_error_and_close(` |
| `serve_ws` | ws_server.rs | `pub async fn serve_ws<F>(listener: TcpListener, handler: F) -> Result<(), std::io::Error>` |
| `serve_ws_with_config` | ws_server.rs | `pub async fn serve_ws_with_config<F>(` |
| `push_serializable` | ws_sink.rs | `pub fn push_serializable(&self, msg: &impl Serialize) {` |
| `push_sized` | ws_sink.rs | `pub fn push_sized(&self, msg: Value, est_bytes: usize) {` |
