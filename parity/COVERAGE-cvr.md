# rust-cvr — Layer-2 (body-differential) coverage

_Which Rust fns have their BODY pinned to TS output. COVERED = reachable from a differential harness (parity_check.rs + the flush/inspect/catchup PG differentials + the sequence fuzzer via seq_replay.rs), taking the transitive closure over the crate call graph. Reachability ≠ every-branch-exercised, but the harnesses drive the real API over real-TS goldens with 150+ fuzzed programs + property tests._

- Rust fns total **186** · ✅ COVERED **162** · 🟥 GAP (pure, untested) **0** · ⚙️ IO (integration diff) **13** · ◻️ infra/metrics **4** · ◻️ documented n/a **7**
- Body-differential coverage of the **unit-testable pure surface**: **162/162 = 100%**

## 🟥 GAP — pure & deterministic, NO differential fixture (build these) — 0

_none_

## ◻️ NON-DIFFERENTIABLE — documented n/a (no un-pinned body) — 7

| fn | file | why not a body-differential |
|---|---|---|
| `catchup_reader` | cvr_store.rs | thin handle ctor (clones pool/schema/cvr_id); the reader's DB work is covered by the catchup PG differential |
| `close` | client_handler.rs | lifecycle side-effect (`eprintln!` + `downstream.cancel()`) — no differentiable output |
| `force_updates` | cvr_store.rs | set-insert of the already-pinned `row_id_string(id)`; no un-pinned logic of its own |
| `has_pending_writes` | cvr_store.rs | trivial getter — `!self.pending.is_empty()` |
| `row_count` | cvr_store.rs | trivial getter — returns `self.row_count` |
| `send_query_transform_failed_error` | client_handler.rs | documented TS↔Rust protocol divergence (TS `fail(ProtocolError)` channel vs Rust `['error', …]`); byte-parity is NOT the contract |
| `updated_version` | cvr.rs | trivial getter — returns `self.base.cvr.version` |

## ✅ COVERED — body pinned to TS fixture — 162

| fn | file | signature |
|---|---|---|
| `new` | change_processor.rs | `pub fn new(updater: &'a mut CVRQueryDrivenUpdater, pokers: &'a MultiPoker) -> Self {` |
| `with_page_size` | change_processor.rs | `pub fn with_page_size(` |
| `acquire_chain` | client_handler.rs | `fn acquire_chain(&self, state: &mut PokeState) {` |
| `add_mutation_patch` | client_handler.rs | `fn add_mutation_patch(&self, state: &mut PokeState, patch: &RowPatch) -> Result<(), Str…` |
| `add_patch` | client_handler.rs | `pub fn add_patch(&self, patch_to_version: &PatchToVersion) -> Result<(), String> {` |
| `cancel` | client_handler.rs | `fn cancel(&self);` |
| `drop` | client_handler.rs | `fn drop(&mut self) {` |
| `end` | client_handler.rs | `pub fn end(&self, final_version: CVRVersion) -> Result<(), String> {` |
| `ensure_body` | client_handler.rs | `fn ensure_body(&self, state: &mut PokeState) -> Result<(), String> {` |
| `ensure_safe_json` | client_handler.rs | `fn ensure_safe_json(contents: &Value) -> Result<(), String> {` |
| `estimate_json_bytes` | client_handler.rs | `pub fn estimate_json_bytes(v: &Value) -> usize {` |
| `estimate_row_patch_bytes` | client_handler.rs | `fn estimate_row_patch_bytes(rp: &RowPatch) -> usize {` |
| `fail` | client_handler.rs | `fn fail(&self, e: String);` |
| `flush_body` | client_handler.rs | `fn flush_body(&self, state: &mut PokeState) -> Result<(), String> {` |
| `go` | client_handler.rs | `fn go(v: &Value, depth: u32) -> usize {` |
| `make_row_patch` | client_handler.rs | `pub(crate) fn make_row_patch(patch: &RowPatch) -> Result<RowPatchOp, String> {` |
| `normalize_mutation_result` | client_handler.rs | `fn normalize_mutation_result(row: &Value) -> Value {` |
| `poke_part_max_bytes` | client_handler.rs | `fn poke_part_max_bytes() -> usize {` |
| `push` | client_handler.rs | `fn push(&self, msg: Value) -> Result<(), String>;` |
| `push_sized` | client_handler.rs | `fn push_sized(&self, msg: Value, _est_bytes: usize) -> Result<(), String> {` |
| `release_chain` | client_handler.rs | `fn release_chain(&self, state: &mut PokeState) {` |
| `send_delete_clients` | client_handler.rs | `pub fn send_delete_clients(` |
| `send_inspect_response` | client_handler.rs | `pub fn send_inspect_response(&self, response: Value) {` |
| `send_query_transform_application_errors` | client_handler.rs | `pub fn send_query_transform_application_errors(` |
| `start_poke` | client_handler.rs | `pub fn start_poke(&self, tentative_version: CVRVersion) -> PokeHandler {` |
| `update_lmids` | client_handler.rs | `fn update_lmids(&self, state: &mut PokeState, patch: &RowPatch) -> Result<(), String> {` |
| `upstream_schema` | client_handler.rs | `fn upstream_schema(shard: &ShardID) -> String {` |
| `version` | client_handler.rs | `pub fn version(&self) -> NullableCVRVersion {` |
| `assert_new_version` | cvr.rs | `fn assert_new_version(&self) -> CVRVersion {` |
| `assert_not_internal` | cvr.rs | `pub fn assert_not_internal(query: &QueryRecord) {` |
| `clear_desired_queries` | cvr.rs | `pub fn clear_desired_queries(&mut self, client_id: &str) -> Vec<PatchToVersion> {` |
| `delete_client` | cvr.rs | `pub fn delete_client(&mut self, client_id: &str, ttl_clock: TTLClock) -> Vec<PatchToVer…` |
| `delete_desired_queries` | cvr.rs | `pub fn delete_desired_queries(` |
| `delete_queries` | cvr.rs | `fn delete_queries(` |
| `delete_unreferenced_rows` | cvr.rs | `pub fn delete_unreferenced_rows<'a>(` |
| `drain_store_ops` | cvr.rs | `pub fn drain_store_ops(&mut self) -> Vec<StoreOp> {` |
| `ensure_client` | cvr.rs | `pub fn ensure_client(&mut self, id: &str) -> &mut ClientRecord {` |
| `ensure_new_version` | cvr.rs | `pub fn ensure_new_version(&mut self) -> CVRVersion {` |
| `flush` | cvr.rs | `pub fn flush(` |
| `get_inactive_queries` | cvr.rs | `pub fn get_inactive_queries(cvr: &CVR) -> Vec<InactiveQuery> {` |
| `get_mutation_results_query` | cvr.rs | `pub fn get_mutation_results_query(` |
| `mark_desired_queries_as_inactive` | cvr.rs | `pub fn mark_desired_queries_as_inactive(` |
| `merge_ref_counts` | cvr.rs | `pub fn merge_ref_counts(` |
| `new_query_record` | cvr.rs | `pub fn new_query_record(` |
| `next_eviction_time` | cvr.rs | `pub fn next_eviction_time(cvr: &CVR) -> Option<TTLClock> {` |
| `put_desired_queries` | cvr.rs | `pub fn put_desired_queries(` |
| `received` | cvr.rs | `pub fn received(` |
| `set_client_schema` | cvr.rs | `pub fn set_client_schema(&mut self, client_schema: ClientSchema) -> Result<(), String> {` |
| `set_profile_id` | cvr.rs | `pub fn set_profile_id(&mut self, profile_id: &str) {` |
| `set_version` | cvr.rs | `pub fn set_version(&mut self, version: CVRVersion) -> CVRVersion {` |
| `track_executed` | cvr.rs | `fn track_executed(&mut self, query_id: &str, transformation_hash: &str) -> Vec<Patch> {` |
| `track_queries` | cvr.rs | `pub fn track_queries(` |
| `track_removed` | cvr.rs | `fn track_removed(&mut self, query_id: &str) -> Vec<Patch> {` |
| `apply_store_ops` | cvr_store.rs | `pub fn apply_store_ops(&mut self, ops: Vec<StoreOp>) {` |
| `as_query` | cvr_store.rs | `pub fn as_query(row: &QueriesRow) -> Result<QueryRecord, VersionError> {` |
| `del_row_record` | cvr_store.rs | `pub fn del_row_record(&mut self, id: &RowID) {` |
| `from` | cvr_store.rs | `fn from(d: InspectQueryRowDb) -> Self {` |
| `insert_client` | cvr_store.rs | `pub fn insert_client(&mut self, client: &ClientRecord) {` |
| `inspect_queries` | cvr_store.rs | `pub async fn inspect_queries(` |
| `is_empty` | cvr_store.rs | `fn is_empty(&self) -> bool {` |
| `load` | cvr_store.rs | `pub async fn load(&mut self, last_connect_time: f64) -> Result<LoadResult, CVRStoreErro…` |
| `load_once` | cvr_store.rs | `async fn load_once(&mut self, last_connect_time: f64) -> Result<LoadResult, CVRStoreErr…` |
| `mark_query_as_deleted` | cvr_store.rs | `pub fn mark_query_as_deleted(&mut self, version: &CVRVersion, query_patch: &QueryPatch) {` |
| `put_desired_query` | cvr_store.rs | `pub fn put_desired_query(` |
| `put_instance` | cvr_store.rs | `pub fn put_instance(&mut self, cvr: &CVR) {` |
| `put_query` | cvr_store.rs | `pub fn put_query(&mut self, query: &QueryRecord) {` |
| `put_row_record` | cvr_store.rs | `pub fn put_row_record(&mut self, row: &RowRecord) {` |
| `update_query` | cvr_store.rs | `pub fn update_query(&mut self, query: &QueryRecord) {` |
| `update_row_set_signature` | cvr_store.rs | `pub fn update_row_set_signature(&mut self, query_hash: &str, signature: &str) {` |
| `h128` | hash.rs | `pub fn h128(s: &str) -> u128 {` |
| `h32` | hash.rs | `pub fn h32(s: &str) -> u32 {` |
| `h64` | hash.rs | `pub fn h64(s: &str) -> u64 {` |
| `xxh32_seeded` | hash.rs | `fn xxh32_seeded(data: &[u8], seed: u32) -> u32 {` |
| `drop_backtrace` | live_count.rs | `pub fn drop_backtrace(context: &str) {` |
| `snapshot` | live_count.rs | `pub fn snapshot() -> String {` |
| `instruments` | otel_metrics.rs | `fn instruments() -> &'static Instruments {` |
| `record_cvr_flush` | otel_metrics.rs | `pub fn record_cvr_flush(elapsed_ms: f64, rows: u64, flush_type: &'static str) {` |
| `record_poke` | otel_metrics.rs | `pub fn record_poke(elapsed_ms: f64) {` |
| `record_poked_row` | otel_metrics.rs | `pub fn record_poked_row() {` |
| `record_row_set_signature_drift` | otel_metrics.rs | `pub fn record_row_set_signature_drift() {` |
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
| `base36_encode` | row_key.rs | `fn base36_encode(mut n: u128) -> String {` |
| `get` | row_key.rs | `fn get(&mut self, id: &RowID) -> Option<String> {` |
| `insert` | row_key.rs | `fn insert(&mut self, id: RowID, s: String) {` |
| `normalized_key_order` | row_key.rs | `pub fn normalized_key_order(key: &RowKey) -> Vec<(&String, &Value)> {` |
| `row_id_hash` | row_key.rs | `pub fn row_id_hash(id: &RowID) -> String {` |
| `row_id_string` | row_key.rs | `pub fn row_id_string(id: &RowID) -> String {` |
| `row_id_string_cached` | row_key.rs | `pub fn row_id_string_cached(id: &RowID) -> String {` |
| `catchup_row_patches` | row_record_cache.rs | `pub async fn catchup_row_patches(` |
| `catchup_task` | row_record_cache.rs | `async fn catchup_task(context: CatchupTaskContext) {` |
| `catchup_task_inner` | row_record_cache.rs | `async fn catchup_task_inner(context: &CatchupTaskContext) -> Result<(), String> {` |
| `clear` | row_record_cache.rs | `pub async fn clear(&self) {` |
| `empty` | row_record_cache.rs | `fn empty() -> Self {` |
| `flushed` | row_record_cache.rs | `pub async fn flushed(&self) -> Result<(), String> {` |
| `next_page` | row_record_cache.rs | `pub async fn next_page(&mut self) -> Result<Option<Vec<RowsRow>>, String> {` |
| `format_signature` | row_set_signature.rs | `pub fn format_signature(sig: u64) -> String {` |
| `parse_signature` | row_set_signature.rs | `pub fn parse_signature(hex: Option<&str>) -> Result<u64, std::num::ParseIntError> {` |
| `row_id_signature_unit` | row_set_signature.rs | `pub fn row_id_signature_unit(id: &RowID) -> u64 {` |
| `row_record_to_rows_row` | schema/cvr.rs | `pub fn row_record_to_rows_row(client_group_id: &str, record: &RowRecord) -> RowsRow {` |
| `rows_row_to_row_record` | schema/cvr.rs | `pub fn rows_row_to_row_record(row: &RowsRow) -> Result<RowRecord, RowRecordError> {` |
| `base` | schema/types.rs | `pub fn base(&self) -> &BaseQueryRecord {` |
| `base_mut` | schema/types.rs | `pub fn base_mut(&mut self) -> &mut BaseQueryRecord {` |
| `client_state` | schema/types.rs | `pub fn client_state(&self) -> Option<&BTreeMap<String, ClientState>> {` |
| `client_state_mut` | schema/types.rs | `pub fn client_state_mut(&mut self) -> Option<&mut BTreeMap<String, ClientState>> {` |
| `cmp_cvr` | schema/types.rs | `pub fn cmp_cvr(a: &CVRVersion, b: &CVRVersion) -> Ordering {` |
| `cmp_versions` | schema/types.rs | `pub fn cmp_versions(a: &NullableCVRVersion, b: &NullableCVRVersion) -> Ordering {` |
| `from_base36_u64` | schema/types.rs | `fn from_base36_u64(s: &str) -> Result<u64, &'static str> {` |
| `id` | schema/types.rs | `pub fn id(&self) -> &str {` |
| `is_internal` | schema/types.rs | `pub fn is_internal(&self) -> bool {` |
| `max_version` | schema/types.rs | `pub fn max_version(a: CVRVersion, b: Option<CVRVersion>) -> CVRVersion {` |
| `maybe_version_string` | schema/types.rs | `pub fn maybe_version_string(s: &str) -> Result<CVRVersion, VersionError> {` |
| `one_after` | schema/types.rs | `pub fn one_after(v: &NullableCVRVersion) -> CVRVersion {` |
| `patch_version` | schema/types.rs | `pub fn patch_version(&self) -> Option<&CVRVersion> {` |
| `patch_version_mut` | schema/types.rs | `pub fn patch_version_mut(&mut self) -> &mut Option<CVRVersion> {` |
| `query_record_to_query_row` | schema/types.rs | `pub fn query_record_to_query_row(cvr_id: &str, query: &QueryRecord) -> QueriesRow {` |
| `to_base36_u64` | schema/types.rs | `fn to_base36_u64(mut n: u64) -> String {` |
| `validate_state_version` | schema/types.rs | `fn validate_state_version(ver: &str) -> Result<(), VersionError> {` |
| `version_from_lexi` | schema/types.rs | `pub fn version_from_lexi(lexi_version: &str) -> Result<u128, &'static str> {` |
| `version_from_string` | schema/types.rs | `pub fn version_from_string(s: &str) -> CVRVersion {` |
| `version_string` | schema/types.rs | `pub fn version_string(v: &CVRVersion) -> String {` |
| `version_to_cookie` | schema/types.rs | `pub fn version_to_cookie(v: &CVRVersion) -> String {` |
| `version_to_lexi` | schema/types.rs | `pub fn version_to_lexi(v: u64) -> String {` |
| `version_to_nullable_cookie` | schema/types.rs | `pub fn version_to_nullable_cookie(v: &NullableCVRVersion) -> Option<String> {` |
| `canon_patch` | seq_replay.rs | `fn canon_patch(p: &PatchToVersion) -> String {` |
| `canonicalize` | seq_replay.rs | `pub fn canonicalize(v: &Value) -> Value {` |
| `default_kind` | seq_replay.rs | `fn default_kind() -> String {` |
| `dump` | seq_replay.rs | `async fn dump(pool: &PgPool) -> Value {` |
| `load_existing_rows` | seq_replay.rs | `async fn load_existing_rows(pool: &PgPool, cvr_id: &str) -> RowRecordMap {` |
| `push_patches` | seq_replay.rs | `fn push_patches(acc: &mut Vec<String>, patches: Vec<PatchToVersion>) {` |
| `reset_schema` | seq_replay.rs | `pub async fn reset_schema(pool: &PgPool) {` |
| `run` | seq_replay.rs | `pub async fn run(pool: &PgPool, prog: &Program) -> Value {` |
| `cvr_schema` | shards.rs | `pub fn cvr_schema(shard: &ShardID) -> String {` |
| `enabled` | tracer.rs | `pub fn enabled() -> bool {` |
| `note` | tracer.rs | `pub fn note(op: &str, msg: &str) {` |
| `recv` | tracer.rs | `pub fn recv(op: &str, msg: &str) {` |
| `clamp_ttl` | ttl.rs | `pub fn clamp_ttl(ttl: TTL) -> i64 {` |
| `compare_ttl` | ttl.rs | `pub fn compare_ttl(a: TTL, b: TTL) -> i64 {` |
| `parse_ttl` | ttl.rs | `pub fn parse_ttl(ttl: TTL) -> i64 {` |
| `parse_ttl_string` | ttl.rs | `pub fn parse_ttl_string(s: &str) -> TTL {` |

## ⚙️ IO — async/DB/actor, use the ART mirror not a unit fixture — 13

| fn | file | signature |
|---|---|---|
| `main` | bin/cvr_seq_replay.rs | `async fn main() {` |
| `finish` | change_processor.rs | `pub fn finish(&mut self, existing_rows: &RowRecordMap) {` |
| `finish_received` | change_processor.rs | `pub fn finish_received(&mut self, existing_rows: &RowRecordMap) {` |
| `flush_batch` | change_processor.rs | `fn flush_batch(&mut self, existing_rows: &RowRecordMap) {` |
| `on_row_change` | change_processor.rs | `pub fn on_row_change(` |
| `total_processed` | change_processor.rs | `pub fn total_processed(&self) -> usize {` |
| `catchup_config_patches` | cvr_store.rs | `pub async fn catchup_config_patches(` |
| `apply` | row_record_cache.rs | `pub async fn apply(` |
| `execute_row_updates` | row_record_cache.rs | `pub fn execute_row_updates(` |
| `flush_loop` | row_record_cache.rs | `async fn flush_loop(context: FlushLoopContext) {` |
| `flush_one_iteration` | row_record_cache.rs | `async fn flush_one_iteration(` |
| `get_row_records` | row_record_cache.rs | `pub async fn get_row_records(&self) -> Arc<HashMap<String, RowRecord>> {` |
| `has_pending_updates` | row_record_cache.rs | `pub async fn has_pending_updates(&self) -> bool {` |
