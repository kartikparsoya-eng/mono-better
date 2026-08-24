# rust-cvr — Layer-2 (body-differential) coverage

_Which Rust fns have their BODY pinned to TS output via `parity_check.rs`._

- Rust fns total **176** · ✅ COVERED **81** · 🟥 GAP (pure, untested) **55** · ⚙️ IO (integration diff) **25** · ◻️ infra/metrics (n/a) **15**
- Body-differential coverage of the **unit-testable pure surface**: **81/136 = 60%**

> ⚠️ **Highest-risk uncovered (emit patches / build rowKeys / mutate CVR — the corruption class):** `add_mutation_patch` (client_handler.rs), `ensure_client` (updater.rs), `estimate_row_patch_bytes` (client_handler.rs), `new_query_record` (cvr.rs), `patch_version` (types.rs), `patch_version_mut` (types.rs), `put_desired_query` (store.rs), `track_executed` (updater.rs), `track_removed` (updater.rs), `update_row_set_signature` (store.rs)

## 🟥 GAP — pure & deterministic, NO differential fixture (build these) — 55

| fn | file | signature |
|---|---|---|
| `acquire_chain` | client_handler.rs | `fn acquire_chain(&self, state: &mut PokeState) {` |
| `add_mutation_patch` | client_handler.rs | `fn add_mutation_patch(&self, state: &mut PokeState, patch: &RowPatch) -> Result<(), Str…` |
| `close` | client_handler.rs | `pub fn close(&self, reason: &str) {` |
| `ensure_body` | client_handler.rs | `fn ensure_body(&self, state: &mut PokeState) -> Result<(), String> {` |
| `ensure_safe_json` | client_handler.rs | `fn ensure_safe_json(contents: &Value) -> Result<(), String> {` |
| `estimate_json_bytes` | client_handler.rs | `pub fn estimate_json_bytes(v: &Value) -> usize {` |
| `estimate_row_patch_bytes` | client_handler.rs | `fn estimate_row_patch_bytes(rp: &RowPatch) -> usize {` |
| `flush_body` | client_handler.rs | `fn flush_body(&self, state: &mut PokeState) -> Result<(), String> {` |
| `go` | client_handler.rs | `fn go(v: &Value, depth: u32) -> usize {` |
| `normalize_mutation_result` | client_handler.rs | `fn normalize_mutation_result(row: &Value) -> Value {` |
| `poke_part_max_bytes` | client_handler.rs | `fn poke_part_max_bytes() -> usize {` |
| `push_sized` | client_handler.rs | `fn push_sized(&self, msg: Value, _est_bytes: usize) -> Result<(), String> {` |
| `release_chain` | client_handler.rs | `fn release_chain(&self, state: &mut PokeState) {` |
| `send_query_transform_failed_error` | client_handler.rs | `pub fn send_query_transform_failed_error(&self, error: &Value) {` |
| `update_lmids` | client_handler.rs | `fn update_lmids(&self, state: &mut PokeState, patch: &RowPatch) -> Result<(), String> {` |
| `upstream_schema` | client_handler.rs | `fn upstream_schema(shard: &ShardID) -> String {` |
| `version` | client_handler.rs | `pub fn version(&self) -> NullableCVRVersion {` |
| `assert_not_internal` | cvr.rs | `pub fn assert_not_internal(query: &QueryRecord) {` |
| `new_query_record` | cvr.rs | `pub fn new_query_record(` |
| `xxh32_seeded` | hash.rs | `fn xxh32_seeded(data: &[u8], seed: u32) -> u32 {` |
| `base36_encode` | row_key.rs | `fn base36_encode(mut n: u128) -> String {` |
| `apply_store_ops` | store.rs | `pub fn apply_store_ops(&mut self, ops: Vec<StoreOp>) {` |
| `catchup_reader` | store.rs | `pub fn catchup_reader(&self) -> CVRStoreCatchupReader {` |
| `del_row_record` | store.rs | `pub fn del_row_record(&mut self, id: &RowID) {` |
| `force_updates` | store.rs | `pub fn force_updates(&mut self, ids: &[RowID]) {` |
| `has_pending_writes` | store.rs | `pub fn has_pending_writes(&self) -> bool {` |
| `insert_client` | store.rs | `pub fn insert_client(&mut self, client: &ClientRecord) {` |
| `is_empty` | store.rs | `fn is_empty(&self) -> bool {` |
| `mark_query_as_deleted` | store.rs | `pub fn mark_query_as_deleted(&mut self, version: &CVRVersion, query_patch: &QueryPatch) {` |
| `put_desired_query` | store.rs | `pub fn put_desired_query(` |
| `put_instance` | store.rs | `pub fn put_instance(&mut self, cvr: &CVR) {` |
| `put_query` | store.rs | `pub fn put_query(&mut self, query: &QueryRecord) {` |
| `put_row_record` | store.rs | `pub fn put_row_record(&mut self, row: &RowRecord) {` |
| `row_count` | store.rs | `pub fn row_count(&self) -> usize {` |
| `update_query` | store.rs | `pub fn update_query(&mut self, query: &QueryRecord) {` |
| `update_row_set_signature` | store.rs | `pub fn update_row_set_signature(&mut self, query_hash: &str, signature: &str) {` |
| `base` | types.rs | `pub fn base(&self) -> &BaseQueryRecord {` |
| `base_mut` | types.rs | `pub fn base_mut(&mut self) -> &mut BaseQueryRecord {` |
| `client_state_mut` | types.rs | `pub fn client_state_mut(&mut self) -> Option<&mut BTreeMap<String, ClientState>> {` |
| `cvr_schema` | types.rs | `pub fn cvr_schema(shard: &ShardID) -> String {` |
| `id` | types.rs | `pub fn id(&self) -> &str {` |
| `is_internal` | types.rs | `pub fn is_internal(&self) -> bool {` |
| `patch_version` | types.rs | `pub fn patch_version(&self) -> Option<&CVRVersion> {` |
| `patch_version_mut` | types.rs | `pub fn patch_version_mut(&mut self) -> &mut Option<CVRVersion> {` |
| `assert_new_version` | updater.rs | `fn assert_new_version(&self) -> CVRVersion {` |
| `delete_queries` | updater.rs | `fn delete_queries(` |
| `ensure_client` | updater.rs | `pub fn ensure_client(&mut self, id: &str) -> &mut ClientRecord {` |
| `ensure_new_version` | updater.rs | `pub fn ensure_new_version(&mut self) -> CVRVersion {` |
| `set_version` | updater.rs | `pub fn set_version(&mut self, version: CVRVersion) -> CVRVersion {` |
| `track_executed` | updater.rs | `fn track_executed(&mut self, query_id: &str, transformation_hash: &str) -> Vec<Patch> {` |
| `track_removed` | updater.rs | `fn track_removed(&mut self, query_id: &str) -> Vec<Patch> {` |
| `updated_version` | updater.rs | `pub fn updated_version(&self) -> CVRVersion {` |
| `from_base36_u64` | version.rs | `fn from_base36_u64(s: &str) -> Result<u64, &'static str> {` |
| `to_base36_u64` | version.rs | `fn to_base36_u64(mut n: u64) -> String {` |
| `validate_state_version` | version.rs | `fn validate_state_version(ver: &str) -> Result<(), VersionError> {` |

## ✅ COVERED — body pinned to TS fixture — 81

| fn | file | signature |
|---|---|---|
| `new` | change_processor.rs | `pub fn new(updater: &'a mut CVRQueryDrivenUpdater, pokers: &'a MultiPoker) -> Self {` |
| `add_patch` | client_handler.rs | `pub fn add_patch(&self, patch_to_version: &PatchToVersion) -> Result<(), String> {` |
| `cancel` | client_handler.rs | `fn cancel(&self);` |
| `end` | client_handler.rs | `pub fn end(&self, final_version: CVRVersion) -> Result<(), String> {` |
| `fail` | client_handler.rs | `fn fail(&self, e: String);` |
| `make_row_patch` | client_handler.rs | `pub(crate) fn make_row_patch(patch: &RowPatch) -> Result<RowPatchOp, String> {` |
| `push` | client_handler.rs | `fn push(&self, msg: Value) -> Result<(), String>;` |
| `send_delete_clients` | client_handler.rs | `pub fn send_delete_clients(` |
| `send_inspect_response` | client_handler.rs | `pub fn send_inspect_response(&self, response: Value) {` |
| `send_query_transform_application_errors` | client_handler.rs | `pub fn send_query_transform_application_errors(` |
| `start_poke` | client_handler.rs | `pub fn start_poke(&self, tentative_version: CVRVersion) -> PokeHandler {` |
| `get_inactive_queries` | cvr.rs | `pub fn get_inactive_queries(cvr: &CVR) -> Vec<InactiveQuery> {` |
| `get_mutation_results_query` | cvr.rs | `pub fn get_mutation_results_query(` |
| `merge_ref_counts` | cvr.rs | `pub fn merge_ref_counts(` |
| `next_eviction_time` | cvr.rs | `pub fn next_eviction_time(cvr: &CVR) -> Option<TTLClock> {` |
| `h128` | hash.rs | `pub fn h128(s: &str) -> u128 {` |
| `h32` | hash.rs | `pub fn h32(s: &str) -> u32 {` |
| `h64` | hash.rs | `pub fn h64(s: &str) -> u64 {` |
| `base_cvr` | parity_check.rs | `fn base_cvr() -> CVR {` |
| `build_client_state` | parity_check.rs | `fn build_client_state(cs: &serde_json::Map<String, Value>) -> BTreeMap<String, ClientSt…` |
| `build_cvr_from_spec` | parity_check.rs | `fn build_cvr_from_spec(queries: &Value) -> CVR {` |
| `build_existing_rows` | parity_check.rs | `fn build_existing_rows(specs: &[Value]) -> HashMap<String, RowRecord> {` |
| `build_query_record_from_spec` | parity_check.rs | `fn build_query_record_from_spec(spec: &Value) -> QueryRecord {` |
| `build_received_rows` | parity_check.rs | `fn build_received_rows(specs: &[Value]) -> HashMap<String, (RowID, RowUpdate)> {` |
| `build_row_patch_from_spec` | parity_check.rs | `fn build_row_patch_from_spec(spec: &Value) -> RowPatch {` |
| `client_query_record` | parity_check.rs | `fn client_query_record(hash: &str, q: &Value) -> QueryRecord {` |
| `dummy_base` | parity_check.rs | `fn dummy_base(id: &str) -> BaseQueryRecord {` |
| `make_row_id_from_json` | parity_check.rs | `fn make_row_id_from_json(v: &Value) -> RowID {` |
| `norm_desire_state` | parity_check.rs | `fn norm_desire_state(cvr: &CVR) -> Value {` |
| `norm_patch` | parity_check.rs | `fn norm_patch(pv: &PatchToVersion) -> Value {` |
| `norm_put_desired_op` | parity_check.rs | `fn norm_put_desired_op(op: &StoreOp) -> Option<Value> {` |
| `parity_check` | parity_check.rs | `fn parity_check() {` |
| `parity_shard` | parity_check.rs | `fn parity_shard() -> ShardID {` |
| `parse_refcounts` | parity_check.rs | `fn parse_refcounts(v: &Value) -> Option<RefCounts> {` |
| `parse_u64` | parity_check.rs | `fn parse_u64(s: &str) -> u64 {` |
| `patch_sort_key` | parity_check.rs | `fn patch_sort_key(pv: &PatchToVersion) -> String {` |
| `patch_to_version_from_json` | parity_check.rs | `fn patch_to_version_from_json(v: &Value) -> PatchToVersion {` |
| `queries_row_from_json` | parity_check.rs | `fn queries_row_from_json(v: &Value) -> QueriesRow {` |
| `queries_row_to_json` | parity_check.rs | `fn queries_row_to_json(row: &QueriesRow) -> Value {` |
| `sorted_norm` | parity_check.rs | `fn sorted_norm(mut patches: Vec<PatchToVersion>) -> Value {` |
| `spec_from_json` | parity_check.rs | `fn spec_from_json(v: &Value) -> DesiredQuerySpec {` |
| `ttl_from_json` | parity_check.rs | `fn ttl_from_json(v: &Value) -> TTL {` |
| `get` | row_key.rs | `fn get(&mut self, id: &RowID) -> Option<String> {` |
| `insert` | row_key.rs | `fn insert(&mut self, id: RowID, s: String) {` |
| `normalized_key_order` | row_key.rs | `pub fn normalized_key_order(key: &RowKey) -> Vec<(&String, &Value)> {` |
| `row_id_hash` | row_key.rs | `pub fn row_id_hash(id: &RowID) -> String {` |
| `row_id_string` | row_key.rs | `pub fn row_id_string(id: &RowID) -> String {` |
| `row_id_string_cached` | row_key.rs | `pub fn row_id_string_cached(id: &RowID) -> String {` |
| `empty` | row_record_cache.rs | `fn empty() -> Self {` |
| `format_signature` | row_set_signature.rs | `pub fn format_signature(sig: u64) -> String {` |
| `parse_signature` | row_set_signature.rs | `pub fn parse_signature(hex: Option<&str>) -> Result<u64, std::num::ParseIntError> {` |
| `signature_unit` | row_set_signature.rs | `pub fn signature_unit(id: &RowID) -> u64 {` |
| `as_query` | store.rs | `pub fn as_query(row: &QueriesRow) -> Result<QueryRecord, VersionError> {` |
| `delete_client` | store.rs | `pub fn delete_client(&mut self, client_id: &str) {` |
| `query_record_to_query_row` | store.rs | `pub fn query_record_to_query_row(cvr_id: &str, query: &QueryRecord) -> QueriesRow {` |
| `clamp_ttl` | ttl.rs | `pub fn clamp_ttl(ttl: TTL) -> i64 {` |
| `compare_ttl` | ttl.rs | `pub fn compare_ttl(a: TTL, b: TTL) -> i64 {` |
| `parse_ttl` | ttl.rs | `pub fn parse_ttl(ttl: TTL) -> i64 {` |
| `parse_ttl_string` | ttl.rs | `pub fn parse_ttl_string(s: &str) -> TTL {` |
| `client_state` | types.rs | `pub fn client_state(&self) -> Option<&BTreeMap<String, ClientState>> {` |
| `clear_desired_queries` | updater.rs | `pub fn clear_desired_queries(&mut self, client_id: &str) -> Vec<PatchToVersion> {` |
| `delete_desired_queries` | updater.rs | `pub fn delete_desired_queries(` |
| `delete_unreferenced_rows` | updater.rs | `pub fn delete_unreferenced_rows<'a>(` |
| `drain_store_ops` | updater.rs | `pub fn drain_store_ops(&mut self) -> Vec<StoreOp> {` |
| `mark_desired_queries_as_inactive` | updater.rs | `pub fn mark_desired_queries_as_inactive(` |
| `put_desired_queries` | updater.rs | `pub fn put_desired_queries(` |
| `received` | updater.rs | `pub fn received(` |
| `set_client_schema` | updater.rs | `pub fn set_client_schema(&mut self, client_schema: ClientSchema) -> Result<(), String> {` |
| `set_profile_id` | updater.rs | `pub fn set_profile_id(&mut self, profile_id: &str) {` |
| `track_queries` | updater.rs | `pub fn track_queries(` |
| `cmp_cvr` | version.rs | `pub fn cmp_cvr(a: &CVRVersion, b: &CVRVersion) -> Ordering {` |
| `cmp_versions` | version.rs | `pub fn cmp_versions(a: &NullableCVRVersion, b: &NullableCVRVersion) -> Ordering {` |
| `max_version` | version.rs | `pub fn max_version(a: CVRVersion, b: Option<CVRVersion>) -> CVRVersion {` |
| `one_after` | version.rs | `pub fn one_after(v: &NullableCVRVersion) -> CVRVersion {` |
| `try_version_from_string` | version.rs | `pub fn try_version_from_string(s: &str) -> Result<CVRVersion, VersionError> {` |
| `version_from_lexi` | version.rs | `pub fn version_from_lexi(lexi_version: &str) -> Result<u128, &'static str> {` |
| `version_from_string` | version.rs | `pub fn version_from_string(s: &str) -> CVRVersion {` |
| `version_string` | version.rs | `pub fn version_string(v: &CVRVersion) -> String {` |
| `version_to_cookie` | version.rs | `pub fn version_to_cookie(v: &CVRVersion) -> String {` |
| `version_to_lexi` | version.rs | `pub fn version_to_lexi(v: u64) -> String {` |
| `version_to_nullable_cookie` | version.rs | `pub fn version_to_nullable_cookie(v: &NullableCVRVersion) -> Option<String> {` |

## ⚙️ IO — async/DB/actor, use the ART mirror not a unit fixture — 25

| fn | file | signature |
|---|---|---|
| `finish` | change_processor.rs | `pub fn finish(&mut self, existing_rows: &RowRecordMap) {` |
| `finish_received` | change_processor.rs | `pub fn finish_received(&mut self, existing_rows: &RowRecordMap) {` |
| `flush_batch` | change_processor.rs | `fn flush_batch(&mut self, existing_rows: &RowRecordMap) {` |
| `on_row_change` | change_processor.rs | `pub fn on_row_change(` |
| `total_processed` | change_processor.rs | `pub fn total_processed(&self) -> usize {` |
| `with_page_size` | change_processor.rs | `pub fn with_page_size(` |
| `apply` | row_record_cache.rs | `pub async fn apply(` |
| `catchup_row_patches` | row_record_cache.rs | `pub async fn catchup_row_patches(` |
| `catchup_task` | row_record_cache.rs | `async fn catchup_task(context: CatchupTaskContext) {` |
| `catchup_task_inner` | row_record_cache.rs | `async fn catchup_task_inner(context: &CatchupTaskContext) -> Result<(), String> {` |
| `clear` | row_record_cache.rs | `pub async fn clear(&self) {` |
| `execute_row_updates` | row_record_cache.rs | `pub fn execute_row_updates(` |
| `flush_loop` | row_record_cache.rs | `async fn flush_loop(context: FlushLoopContext) {` |
| `flush_one_iteration` | row_record_cache.rs | `async fn flush_one_iteration(` |
| `flushed` | row_record_cache.rs | `pub async fn flushed(&self) -> Result<(), String> {` |
| `from` | row_record_cache.rs | `fn from(db: RowsRowDb) -> Self {` |
| `get_row_records` | row_record_cache.rs | `pub async fn get_row_records(&self) -> Arc<HashMap<String, RowRecord>> {` |
| `has_pending_updates` | row_record_cache.rs | `pub async fn has_pending_updates(&self) -> bool {` |
| `load` | row_record_cache.rs | `pub async fn load(&self) -> Result<usize, sqlx::Error> {` |
| `next_page` | row_record_cache.rs | `pub async fn next_page(&mut self) -> Result<Option<Vec<RowsRow>>, String> {` |
| `row_record_to_rows_row` | row_record_cache.rs | `pub fn row_record_to_rows_row(client_group_id: &str, record: &RowRecord) -> RowsRow {` |
| `rows_row_to_row_record` | row_record_cache.rs | `pub fn rows_row_to_row_record(row: &RowsRow) -> Result<RowRecord, RowRecordError> {` |
| `catchup_config_patches` | store.rs | `pub async fn catchup_config_patches(` |
| `flush` | store.rs | `pub async fn flush(` |
| `load_once` | store.rs | `async fn load_once(&mut self, last_connect_time: f64) -> Result<LoadResult, CVRStoreErr…` |
