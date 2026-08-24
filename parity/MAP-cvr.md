# TS ⇄ Rust parity map — `cvr` crate

_Deterministic. File edges + symbol pairs are derived from **shared symbol content**, never filenames — so renamed files (e.g. `drain-coordinator.ts`→`drain.rs`) and renamed symbols (`cvrErrorKind`→`CVRStoreError`) still bind. Bodies are not compared; behavior drift needs Layer-2 body review._

- symbols: TS **160**, Rust **254** · resolved pairs **101** (exact 96 + fuzzy 5) + aliases 9
- 🟥 TS UNRESOLVED: **50** (**0** behavioral ⇒ investigate · 50 structural: zod/DDL/type-alias ⇒ serde/inline-SQL, expected) · 🟦 Rust-only ADDED: **153**

## 1 · File structure diff

TS origin files: **8**  ·  Rust files: **17** (6 new)

| TS file (LOC) | rel | Rust file(s) (shared syms) |
|---|---|---|
| `client-handler.ts` (467) | **MERGED** | `client_handler.rs` (15), `types.rs` (3) |
| `cvr-store.ts` (1447) | **SPLIT** | `store.rs` (18), `row_record_cache.rs` (5), `live_count.rs` (1), `version.rs` (1), `client_handler.rs` (1) |
| `cvr.ts` (1197) | **SPLIT** | `updater.rs` (17), `cvr.rs` (6), `types.rs` (2), `store.rs` (2), `otel_metrics.rs` (1) |
| `row-record-cache.ts` (485) | **MERGED** | `row_record_cache.rs` (3), `live_count.rs` (1), `otel_metrics.rs` (1) |
| `row-set-signature.ts` (30) | **1:1** | `row_set_signature.rs` (3) |
| `schema/cvr.ts` (359) | **SPLIT** | `store.rs` (4), `row_record_cache.rs` (4) |
| `schema/types.ts` (393) | **SPLIT** | `version.rs` (10), `types.rs` (8), `store.rs` (1), `row_key.rs` (1) |
| `ttl-clock.ts` (15) | **MERGED** | `types.rs` (2) |

**New Rust files (no TS origin — added in the port):**  `change_processor.rs` (649), `hash.rs` (75), `lib.rs` (32), `parity_check.rs` (577), `trace.rs` (44), `ttl.rs` (137)

**Merges (many TS → one Rust file):**
- `client_handler.rs` ⟵ `client-handler.ts`, `cvr-store.ts`
- `live_count.rs` ⟵ `cvr-store.ts`, `row-record-cache.ts`
- `otel_metrics.rs` ⟵ `cvr.ts`, `row-record-cache.ts`
- `row_record_cache.rs` ⟵ `cvr-store.ts`, `row-record-cache.ts`, `schema/cvr.ts`
- `store.rs` ⟵ `cvr-store.ts`, `cvr.ts`, `schema/cvr.ts`, `schema/types.ts`
- `types.rs` ⟵ `client-handler.ts`, `cvr.ts`, `schema/types.ts`, `ttl-clock.ts`
- `version.rs` ⟵ `cvr-store.ts`, `schema/types.ts`

## 2 · Per-file functional divergence

### `change_processor.rs`  ⟵  _(new)_


🟦 **Rust-only added here (11):** `ChangeProcessor`, `DEFAULT_CURSOR_PAGE_SIZE`, `RowChangeType`, `ZERO_VERSION_COLUMN_NAME`, `finish`, `finish_received`, `flush_batch`, `new`, `on_row_change`, `total_processed`, `with_page_size`

### `client_handler.rs`  ⟵  `client-handler.ts`, `cvr-store.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `addPatch` (client-handler.ts:73) | `add_patch` (:255) | exact |
| `cancel` (client-handler.ts:74) | `cancel` (:67) | exact |
| `ClientHandler` (client-handler.ts:114) | `ClientHandler` (:693) | exact |
| `close` (client-handler.ts:183) | `close` (:764) | exact |
| `end` (client-handler.ts:75) | `end` (:392) | exact |
| `ensureSafeJSON` (client-handler.ts:449) | `ensure_safe_json` (:651) | exact |
| `fail` (client-handler.ts:175) | `fail` (:66) | exact |
| `makeRowPatch` (client-handler.ts:416) | `make_row_patch` (:671) | exact |
| `PokeHandler` (client-handler.ts:72) | `PokeHandler` (:222) | exact |
| `sendDeleteClients` (client-handler.ts:347) | `send_delete_clients` (:829) | exact |
| `sendInspectResponse` (client-handler.ts:371) | `send_inspect_response` (:859) | exact |
| `sendQueryTransformApplicationErrors` (client-handler.ts:363) | `send_query_transform_application_errors` (:851) | exact |
| `sendQueryTransformFailedError` (client-handler.ts:367) | `send_query_transform_failed_error` (:869) | exact |
| `startPoke` (client-handler.ts:85) | `start_poke` (:769) | exact |
| `version` (client-handler.ts:166) | `version` (:756) | exact |

🟥 **TS symbols not resolved into this file (3):** `ConfigPatch`, `DeleteRowPatch`, `PutRowPatch`

🟦 **Rust-only added here (32):** `DEFAULT_POKE_PART_MAX_BYTES`, `MAX_DEPTH`, `MAX_SAFE_INTEGER`, `MultiPoker`, `MutationPatchEntry`, `MutationPatchId`, `MutationPatchMutation`, `PART_COUNT_FLUSH_THRESHOLD`, `POKE_PART_ENVELOPE_EST`, `PokePartBody`, `PokeState`, `QueryPatchEntry`, `ROW_PATCH_ENVELOPE_EST`, `RowPatchOp`, `V`, `WebSocketSink`, `acquire_chain`, `add_mutation_patch`, `drop`, `ensure_body`, `estimate_json_bytes`, `estimate_row_patch_bytes`, `flush_body`, `go`, `normalize_mutation_result`, `poke_part_max_bytes`, `push`, `push_sized`, `release_chain`, `set_base_version_for_test`, `update_lmids`, `upstream_schema`

### `cvr.rs`  ⟵  `cvr.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `assertNotInternal` (cvr.ts:87) | `assert_not_internal` (:158) | exact |
| `getInactiveQueries` (cvr.ts:1087) | `get_inactive_queries` (:177) | exact |
| `getMutationResultsQuery` (cvr.ts:96) | `get_mutation_results_query` (:125) | exact |
| `mergeRefCounts` (cvr.ts:1049) | `merge_ref_counts` (:27) | exact |
| `newQueryRecord` (cvr.ts:1167) | `new_query_record` (:84) | exact |
| `nextEvictionTime` (cvr.ts:1156) | `next_eviction_time` (:243) | exact |

🟦 **Rust-only added here (1):** `InactiveQuery`

### `hash.rs`  ⟵  _(new)_


🟦 **Rust-only added here (4):** `h128`, `h32`, `h64`, `xxh32_seeded`

### `live_count.rs`  ⟵  `cvr-store.ts`, `row-record-cache.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `CVRStore` (cvr-store.ts:180) | `CVR_STORE` (:17) | exact |
| `RowRecordCache` (row-record-cache.ts:90) | `ROW_RECORD_CACHE` (:20) | exact |

🟦 **Rust-only added here (7):** `CONFIG_DRIVEN_UPDATER`, `Guard`, `QUERY_DRIVEN_UPDATER`, `dec`, `drop_backtrace`, `inc`, `snapshot`

### `otel_metrics.rs`  ⟵  `cvr.ts`, `row-record-cache.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `recordSyncFlushStats` (row-record-cache.ts:144) | `record_cvr_flush` (:96) | fuzzy 0.40 |
| `RowSetSignatureProvider` (cvr.ts:544) | `record_row_set_signature_drift` (:89) | fuzzy 0.40 |

🟦 **Rust-only added here (4):** `Instruments`, `LATENCY_BOUNDARIES_S`, `record_poke`, `record_poked_row`

### `parity_check.rs`  ⟵  _(new)_


🟦 **Rust-only added here (9):** `build_client_state`, `build_cvr_from_spec`, `build_query_record_from_spec`, `build_row_patch_from_spec`, `dummy_base`, `make_row_id_from_json`, `parity_check`, `parse_refcounts`, `parse_u64`

### `row_key.rs`  ⟵  `schema/types.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `RowID` (schema/types.ts:259) | `RowID` (:45) | exact |

🟦 **Rust-only added here (10):** `CACHE_GEN_CAP`, `DIGITS`, `RowIdStringCache`, `base36_encode`, `get`, `insert`, `normalized_key_order`, `row_id_hash`, `row_id_string`, `row_id_string_cached`

### `row_record_cache.rs`  ⟵  `cvr-store.ts`, `row-record-cache.ts`, `schema/cvr.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `apply` (row-record-cache.ts:234) | `apply` (:385) | exact |
| `catchupRowPatches` (cvr-store.ts:709) | `catchup_row_patches` (:587) | exact |
| `clear` (row-record-cache.ts:334) | `clear` (:514) | exact |
| `executeRowUpdates` (row-record-cache.ts:414) | `execute_row_updates` (:527) | exact |
| `flushed` (cvr-store.ts:1284) | `flushed` (:493) | exact |
| `getRowRecords` (cvr-store.ts:520) | `get_row_records` (:375) | exact |
| `hasPendingUpdates` (cvr-store.ts:1279) | `has_pending_updates` (:483) | exact |
| `load` (cvr-store.ts:274) | `load` (:335) | exact |
| `rowRecordToRowsRow` (schema/cvr.ts:238) | `row_record_to_rows_row` (:136) | exact |
| `RowsRow` (schema/cvr.ts:211) | `RowsRow` (:38) | exact |
| `rowsRowToRowRecord` (schema/cvr.ts:229) | `rows_row_to_row_record` (:102) | exact |
| `RowsVersionRow` (schema/cvr.ts:331) | `RowsVersionRow` (:181) | exact |

🟦 **Rust-only added here (20):** `CATCHUP_PAGE_SIZE`, `CacheState`, `CatchupCursor`, `CatchupTaskContext`, `DEFAULT_DEFERRED_THRESHOLD`, `ExecuteResult`, `FlushLoopContext`, `FlushMode`, `IDLE_TX_TIMEOUT_MS`, `RowKeyRef`, `RowRecordError`, `RowUpdateStatements`, `RowsRowDb`, `catchup_task`, `catchup_task_inner`, `empty`, `flush_loop`, `flush_one_iteration`, `from`, `next_page`

### `row_set_signature.rs`  ⟵  `row-set-signature.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `formatSignature` (row-set-signature.ts:28) | `format_signature` (:31) | exact |
| `parseSignature` (row-set-signature.ts:18) | `parse_signature` (:23) | exact |

🟦 **Rust-only added here (1):** `signature_unit`

### `store.rs`  ⟵  `cvr-store.ts`, `cvr.ts`, `schema/cvr.ts`, `schema/types.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `asQuery` (cvr-store.ts:119) | `as_query` (:1413) | exact |
| `catchupConfigPatches` (cvr-store.ts:725) | `catchup_config_patches` (:215) | exact |
| `ClientsRow` (schema/cvr.ts:76) | `ClientsRow` (:74) | exact |
| `CVRFlushStats` (cvr-store.ts:67) | `CVRFlushStats` (:146) | exact |
| `deleteClient` (cvr.ts:502) | `delete_client` (:318) | exact |
| `delRowRecord` (cvr-store.ts:536) | `del_row_record` (:413) | exact |
| `DesiresRow` (schema/cvr.ts:159) | `DesiresRow` (:121) | exact |
| `flush` (cvr.ts:183) | `flush` (:467) | exact |
| `forceUpdates` (cvr-store.ts:545) | `force_updates` (:420) | exact |
| `insertClient` (cvr-store.ts:662) | `insert_client` (:311) | exact |
| `InstancesRow` (schema/cvr.ts:31) | `InstancesRow` (:61) | exact |
| `markQueryAsDeleted` (cvr-store.ts:620) | `mark_query_as_deleted` (:357) | exact |
| `putDesiredQuery` (cvr-store.ts:684) | `put_desired_query` (:382) | exact |
| `putInstance` (cvr-store.ts:584) | `put_instance` (:297) | exact |
| `putQuery` (cvr-store.ts:629) | `put_query` (:324) | exact |
| `putRowRecord` (cvr-store.ts:524) | `put_row_record` (:406) | exact |
| `QueriesRow` (schema/cvr.ts:105) | `QueriesRow` (:80) | exact |
| `queryRecordToQueryRow` (schema/types.ts:342) | `query_record_to_query_row` (:1468) | exact |
| `rowCount` (cvr-store.ts:1227) | `row_count` (:291) | exact |
| `updateQuery` (cvr-store.ts:644) | `update_query` (:331) | exact |
| `updateRowSetSignature` (cvr-store.ts:658) | `update_row_set_signature` (:373) | exact |

🟥 **TS symbols not resolved into this file (20):** `ClientNotFoundError`, `ConcurrentModificationException`, `InvalidClientSchemaError`, `OwnershipError`, `compareClientsRows`, `compareDesiresRows`, `compareInstancesRows`, `compareQueriesRows`, `compareRowsRows`, `createClientsTable`, `createDesiresTable`, `createInstancesTable`, `createQueriesTable`, `createRowsTable`, `createRowsVersionTable`, `createSchema`, `createTables`, `rowsRowToRowID`, `schema`, `stringifySorted`

🟦 **Rust-only added here (13):** `CVRStoreCatchupReader`, `CVRStoreError`, `CVRStoreHandle`, `LOAD_ATTEMPT_INTERVAL_MS`, `LoadResult`, `MAX_LOAD_ATTEMPTS`, `PartialQueriesRow`, `PendingWrites`, `apply_store_ops`, `catchup_reader`, `has_pending_writes`, `is_empty`, `load_once`

### `trace.rs`  ⟵  _(new)_


🟦 **Rust-only added here (4):** `ENABLED`, `emit`, `note`, `recv`

### `ttl.rs`  ⟵  _(new)_


🟦 **Rust-only added here (12):** `DEFAULT_TTL_MS`, `MAX_TTL_MS`, `MULT_D`, `MULT_H`, `MULT_M`, `MULT_S`, `MULT_Y`, `TTL`, `clamp_ttl`, `compare_ttl`, `parse_ttl`, `parse_ttl_string`

### `types.rs`  ⟵  `client-handler.ts`, `cvr.ts`, `schema/types.ts`, `ttl-clock.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `baseQueryRecordSchema` (schema/types.ts:121) | `BaseQueryRecord` (:98) | fuzzy 0.75 |
| `ClientQueryRecord` (schema/types.ts:235) | `ClientQueryRecord` (:116) | exact |
| `ClientRecord` (schema/types.ts:119) | `ClientRecord` (:64) | exact |
| `CustomQueryRecord` (schema/types.ts:243) | `CustomQueryRecord` (:126) | exact |
| `CVR` (cvr.ts:58) | `CVR` (:201) | exact |
| `InternalQueryRecord` (schema/types.ts:188) | `InternalQueryRecord` (:109) | exact |
| `Patch` (client-handler.ts:65) | `Patch` (:216) | exact |
| `PatchToVersion` (client-handler.ts:67) | `PatchToVersion` (:254) | exact |
| `QueryPatch` (schema/types.ts:303) | `QueryPatch` (:237) | exact |
| `QueryRecord` (schema/types.ts:251) | `QueryRecord` (:87) | exact |
| `RowPatch` (client-handler.ts:62) | `RowPatch` (:225) | exact |
| `RowRecord` (schema/types.ts:269) | `RowRecord` (:39) | exact |
| `RowUpdate` (cvr.ts:51) | `RowUpdate` (:54) | exact |

🟥 **TS symbols not resolved into this file (2):** `TTLClock`, `ttlClockSchema`

🟦 **Rust-only added here (15):** `CLIENT_LMID_QUERY_ID`, `CLIENT_MUTATION_RESULTS_QUERY_ID`, `ClientState`, `DesiredQuerySpec`, `RowPatchInfo`, `ShardID`, `StoreOp`, `base`, `base_mut`, `client_state_mut`, `cvr_schema`, `id`, `is_internal`, `patch_version`, `patch_version_mut`

### `updater.rs`  ⟵  `cvr.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `_ensureNewVersion` (cvr.ts:176) | `ensure_new_version` (:65) | exact |
| `_setVersion` (cvr.ts:162) | `set_version` (:54) | exact |
| `clearDesiredQueries` (cvr.ts:497) | `clear_desired_queries` (:475) | exact |
| `CVRConfigDrivenUpdater` (cvr.ts:212) | `CVRConfigDrivenUpdater` (:105) | exact |
| `CVRQueryDrivenUpdater` (cvr.ts:560) | `CVRQueryDrivenUpdater` (:521) | exact |
| `CVRUpdater` (cvr.ts:141) | `CVRUpdater` (:36) | exact |
| `deleteDesiredQueries` (cvr.ts:422) | `delete_desired_queries` (:364) | exact |
| `deleteUnreferencedRows` (cvr.ts:959) | `delete_unreferenced_rows` (:855) | exact |
| `ensureClient` (cvr.ts:220) | `ensure_client` (:124) | exact |
| `markDesiredQueriesAsInactive` (cvr.ts:414) | `mark_desired_queries_as_inactive` (:354) | exact |
| `putDesiredQueries` (cvr.ts:317) | `put_desired_queries` (:230) | exact |
| `received` (cvr.ts:836) | `received` (:718) | exact |
| `setClientSchema` (cvr.ts:273) | `set_client_schema` (:194) | exact |
| `setProfileID` (cvr.ts:299) | `set_profile_id` (:214) | exact |
| `trackQueries` (cvr.ts:617) | `track_queries` (:590) | exact |
| `updatedVersion` (cvr.ts:789) | `updated_version` (:579) | exact |

🟥 **TS symbols not resolved into this file (3):** `CVRSnapshot`, `Column`, `RefCounts`

🟦 **Rust-only added here (5):** `assert_new_version`, `delete_queries`, `drain_store_ops`, `track_executed`, `track_removed`

### `version.rs`  ⟵  `cvr-store.ts`, `schema/types.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `cmpVersions` (schema/types.ts:55) | `cmp_versions` (:76) | exact |
| `CVRVersion` (schema/types.ts:38) | `CVRVersion` (:23) | exact |
| `EMPTY_CVR_VERSION` (schema/types.ts:40) | `EMPTY_CVR_VERSION` (:43) | exact |
| `maxVersion` (schema/types.ts:72) | `max_version` (:86) | exact |
| `maybeVersionString` (schema/types.ts:392) | `try_version_from_string` (:144) | fuzzy 0.40 |
| `oneAfter` (schema/types.ts:44) | `one_after` (:31) | exact |
| `RowsVersionBehindError` (cvr-store.ts:1437) | `VersionError` (:130) | fuzzy 0.50 |
| `versionFromString` (schema/types.ts:322) | `version_from_string` (:173) | exact |
| `versionString` (schema/types.ts:312) | `version_string` (:115) | exact |
| `versionToCookie` (schema/types.ts:76) | `version_to_cookie` (:100) | exact |
| `versionToNullableCookie` (schema/types.ts:80) | `version_to_nullable_cookie` (:104) | exact |

🟥 **TS symbols not resolved into this file (22):** `CvrID`, `DelQueryPatch`, `DelRowPatch`, `MetadataPatch`, `NullableCVRVersion`, `PutQueryPatch`, `clientQueryRecordSchema`, `clientRecordSchema`, `cookieToVersion`, `customQueryRecordSchema`, `cvrIDSchema`, `cvrVersionSchema`, `delRowPatchSchema`, `internalQueryRecordSchema`, `metadataPatchSchema`, `patchSchema`, `putRowPatchSchema`, `queryPatchSchema`, `queryRecordSchema`, `rowIDSchema`, `rowPatchSchema`, `rowRecordSchema`

🟦 **Rust-only added here (5):** `cmp_cvr`, `from_base36_u64`, `to_base36_u64`, `version_from_lexi`, `version_to_lexi`

## 3 · Flat one-to-one symbol map (every TS symbol resolved)

| TS symbol | origin | → Rust | status |
|---|---|---|---|
| `PutRowPatch` | client-handler.ts:49 | — | 🟥 UNRESOLVED |
| `DeleteRowPatch` | client-handler.ts:56 | — | 🟥 UNRESOLVED |
| `RowPatch` | client-handler.ts:62 | `RowPatch` types.rs:225 | ✅ exact |
| `ConfigPatch` | client-handler.ts:63 | — | 🟥 UNRESOLVED |
| `Patch` | client-handler.ts:65 | `Patch` types.rs:216 | ✅ exact |
| `PatchToVersion` | client-handler.ts:67 | `PatchToVersion` types.rs:254 | ✅ exact |
| `PokeHandler` | client-handler.ts:72 | `PokeHandler` client_handler.rs:222 | ✅ exact |
| `addPatch` | client-handler.ts:73 | `add_patch` client_handler.rs:255 | ✅ exact |
| `cancel` | client-handler.ts:74 | `cancel` client_handler.rs:67 | ✅ exact |
| `end` | client-handler.ts:75 | `end` client_handler.rs:392 | ✅ exact |
| `startPoke` | client-handler.ts:85 | `start_poke` client_handler.rs:769 | ✅ exact |
| `ClientHandler` | client-handler.ts:114 | `ClientHandler` client_handler.rs:693 | ✅ exact |
| `version` | client-handler.ts:166 | `version` client_handler.rs:756 | ✅ exact |
| `fail` | client-handler.ts:175 | `fail` client_handler.rs:66 | ✅ exact |
| `close` | client-handler.ts:183 | `close` client_handler.rs:764 | ✅ exact |
| `sendDeleteClients` | client-handler.ts:347 | `send_delete_clients` client_handler.rs:829 | ✅ exact |
| `sendQueryTransformApplicationErrors` | client-handler.ts:363 | `send_query_transform_application_errors` client_handler.rs:851 | ✅ exact |
| `sendQueryTransformFailedError` | client-handler.ts:367 | `send_query_transform_failed_error` client_handler.rs:869 | ✅ exact |
| `sendInspectResponse` | client-handler.ts:371 | `send_inspect_response` client_handler.rs:859 | ✅ exact |
| `makeRowPatch` | client-handler.ts:416 | `make_row_patch` client_handler.rs:671 | ✅ exact |
| `ensureSafeJSON` | client-handler.ts:449 | `ensure_safe_json` client_handler.rs:651 | ✅ exact |
| `CVRFlushStats` | cvr-store.ts:67 | `CVRFlushStats` store.rs:146 | ✅ exact |
| `convertTTLValues` | cvr-store.ts:88 | INLINED | 📌 store.rs upsert SQL: ttl/1000 + null-on-negative |
| `asQuery` | cvr-store.ts:119 | `as_query` store.rs:1413 | ✅ exact |
| `CVRStore` | cvr-store.ts:180 | `CVR_STORE` live_count.rs:17 | ✅ exact |
| `load` | cvr-store.ts:274 | `load` row_record_cache.rs:335 | ✅ exact |
| `getRowRecords` | cvr-store.ts:520 | `get_row_records` row_record_cache.rs:375 | ✅ exact |
| `putRowRecord` | cvr-store.ts:524 | `put_row_record` store.rs:406 | ✅ exact |
| `delRowRecord` | cvr-store.ts:536 | `del_row_record` store.rs:413 | ✅ exact |
| `forceUpdates` | cvr-store.ts:545 | `force_updates` store.rs:420 | ✅ exact |
| `updateTTLClock` | cvr-store.ts:556 | INLINED | 📌 store.rs:1260 UPDATE instances SET lastActive,ttlClock |
| `getTTLClock` | cvr-store.ts:569 | INLINED | 📌 store.rs SELECT instances."ttlClock" (load path) |
| `putInstance` | cvr-store.ts:584 | `put_instance` store.rs:297 | ✅ exact |
| `markQueryAsDeleted` | cvr-store.ts:620 | `mark_query_as_deleted` store.rs:357 | ✅ exact |
| `putQuery` | cvr-store.ts:629 | `put_query` store.rs:324 | ✅ exact |
| `updateQuery` | cvr-store.ts:644 | `update_query` store.rs:331 | ✅ exact |
| `updateRowSetSignature` | cvr-store.ts:658 | `update_row_set_signature` store.rs:373 | ✅ exact |
| `insertClient` | cvr-store.ts:662 | `insert_client` store.rs:311 | ✅ exact |
| `putDesiredQuery` | cvr-store.ts:684 | `put_desired_query` store.rs:382 | ✅ exact |
| `catchupRowPatches` | cvr-store.ts:709 | `catchup_row_patches` row_record_cache.rs:587 | ✅ exact |
| `catchupConfigPatches` | cvr-store.ts:725 | `catchup_config_patches` store.rs:215 | ✅ exact |
| `rowCount` | cvr-store.ts:1227 | `row_count` store.rs:291 | ✅ exact |
| `hasPendingUpdates` | cvr-store.ts:1279 | `has_pending_updates` row_record_cache.rs:483 | ✅ exact |
| `flushed` | cvr-store.ts:1284 | `flushed` row_record_cache.rs:493 | ✅ exact |
| `inspectQueries` | cvr-store.ts:1288 | send_inspect_response (client_handler.rs:859) | 📌 inspector path |
| `ClientNotFoundError` | cvr-store.ts:1354 | — | 🟥 UNRESOLVED |
| `ConcurrentModificationException` | cvr-store.ts:1367 | — | 🟥 UNRESOLVED |
| `OwnershipError` | cvr-store.ts:1382 | — | 🟥 UNRESOLVED |
| `InvalidClientSchemaError` | cvr-store.ts:1405 | — | 🟥 UNRESOLVED |
| `cvrErrorKind` | cvr-store.ts:1421 | CVRStoreError enum (store.rs:32) | 📌 fn→enum discriminant |
| `RowsVersionBehindError` | cvr-store.ts:1437 | `VersionError` version.rs:130 | 🔁 rename 0.50 |
| `RowUpdate` | cvr.ts:51 | `RowUpdate` types.rs:54 | ✅ exact |
| `CVR` | cvr.ts:58 | `CVR` types.rs:201 | ✅ exact |
| `CVRSnapshot` | cvr.ts:72 | — | 🟥 UNRESOLVED |
| `assertNotInternal` | cvr.ts:87 | `assert_not_internal` cvr.rs:158 | ✅ exact |
| `getMutationResultsQuery` | cvr.ts:96 | `get_mutation_results_query` cvr.rs:125 | ✅ exact |
| `CVRUpdater` | cvr.ts:141 | `CVRUpdater` updater.rs:36 | ✅ exact |
| `_setVersion` | cvr.ts:162 | `set_version` updater.rs:54 | ✅ exact |
| `_ensureNewVersion` | cvr.ts:176 | `ensure_new_version` updater.rs:65 | ✅ exact |
| `flush` | cvr.ts:183 | `flush` store.rs:467 | ✅ exact |
| `CVRConfigDrivenUpdater` | cvr.ts:212 | `CVRConfigDrivenUpdater` updater.rs:105 | ✅ exact |
| `ensureClient` | cvr.ts:220 | `ensure_client` updater.rs:124 | ✅ exact |
| `setClientSchema` | cvr.ts:273 | `set_client_schema` updater.rs:194 | ✅ exact |
| `setProfileID` | cvr.ts:299 | `set_profile_id` updater.rs:214 | ✅ exact |
| `putDesiredQueries` | cvr.ts:317 | `put_desired_queries` updater.rs:230 | ✅ exact |
| `markDesiredQueriesAsInactive` | cvr.ts:414 | `mark_desired_queries_as_inactive` updater.rs:354 | ✅ exact |
| `deleteDesiredQueries` | cvr.ts:422 | `delete_desired_queries` updater.rs:364 | ✅ exact |
| `clearDesiredQueries` | cvr.ts:497 | `clear_desired_queries` updater.rs:475 | ✅ exact |
| `deleteClient` | cvr.ts:502 | `delete_client` store.rs:318 | ✅ exact |
| `Column` | cvr.ts:530 | — | 🟥 UNRESOLVED |
| `RefCounts` | cvr.ts:531 | — | 🟥 UNRESOLVED |
| `RowSetSignatureProvider` | cvr.ts:544 | `record_row_set_signature_drift` otel_metrics.rs:89 | 🔁 rename 0.40 |
| `CVRQueryDrivenUpdater` | cvr.ts:560 | `CVRQueryDrivenUpdater` updater.rs:521 | ✅ exact |
| `trackQueries` | cvr.ts:617 | `track_queries` updater.rs:590 | ✅ exact |
| `updatedVersion` | cvr.ts:789 | `updated_version` updater.rs:579 | ✅ exact |
| `received` | cvr.ts:836 | `received` updater.rs:718 | ✅ exact |
| `deleteUnreferencedRows` | cvr.ts:959 | `delete_unreferenced_rows` updater.rs:855 | ✅ exact |
| `mergeRefCounts` | cvr.ts:1049 | `merge_ref_counts` cvr.rs:27 | ✅ exact |
| `getInactiveQueries` | cvr.ts:1087 | `get_inactive_queries` cvr.rs:177 | ✅ exact |
| `nextEvictionTime` | cvr.ts:1156 | `next_eviction_time` cvr.rs:243 | ✅ exact |
| `newQueryRecord` | cvr.ts:1167 | `new_query_record` cvr.rs:84 | ✅ exact |
| `assert` | cvr.ts:1186 | assert_new_version (updater.rs:704) | 📌 rename |
| `RowRecordCache` | row-record-cache.ts:90 | `ROW_RECORD_CACHE` live_count.rs:20 | ✅ exact |
| `recordSyncFlushStats` | row-record-cache.ts:144 | `record_cvr_flush` otel_metrics.rs:96 | 🔁 rename 0.40 |
| `apply` | row-record-cache.ts:234 | `apply` row_record_cache.rs:385 | ✅ exact |
| `clear` | row-record-cache.ts:334 | `clear` row_record_cache.rs:514 | ✅ exact |
| `executeRowUpdates` | row-record-cache.ts:414 | `execute_row_updates` row_record_cache.rs:527 | ✅ exact |
| `rowIDSignatureUnit` | row-set-signature.ts:10 | signature_unit (row_set_signature.rs:17) | 📌 rename |
| `parseSignature` | row-set-signature.ts:18 | `parse_signature` row_set_signature.rs:23 | ✅ exact |
| `formatSignature` | row-set-signature.ts:28 | `format_signature` row_set_signature.rs:31 | ✅ exact |
| `schema` | schema/cvr.ts:23 | — | 🟥 UNRESOLVED |
| `createSchema` | schema/cvr.ts:27 | — | 🟥 UNRESOLVED |
| `InstancesRow` | schema/cvr.ts:31 | `InstancesRow` store.rs:61 | ✅ exact |
| `createInstancesTable` | schema/cvr.ts:43 | — | 🟥 UNRESOLVED |
| `compareInstancesRows` | schema/cvr.ts:72 | — | 🟥 UNRESOLVED |
| `ClientsRow` | schema/cvr.ts:76 | `ClientsRow` store.rs:74 | ✅ exact |
| `createClientsTable` | schema/cvr.ts:81 | — | 🟥 UNRESOLVED |
| `compareClientsRows` | schema/cvr.ts:97 | — | 🟥 UNRESOLVED |
| `QueriesRow` | schema/cvr.ts:105 | `QueriesRow` store.rs:80 | ✅ exact |
| `createQueriesTable` | schema/cvr.ts:122 | — | 🟥 UNRESOLVED |
| `compareQueriesRows` | schema/cvr.ts:151 | — | 🟥 UNRESOLVED |
| `DesiresRow` | schema/cvr.ts:159 | `DesiresRow` store.rs:121 | ✅ exact |
| `createDesiresTable` | schema/cvr.ts:169 | — | 🟥 UNRESOLVED |
| `compareDesiresRows` | schema/cvr.ts:199 | — | 🟥 UNRESOLVED |
| `RowsRow` | schema/cvr.ts:211 | `RowsRow` row_record_cache.rs:38 | ✅ exact |
| `rowsRowToRowID` | schema/cvr.ts:221 | — | 🟥 UNRESOLVED |
| `rowsRowToRowRecord` | schema/cvr.ts:229 | `rows_row_to_row_record` row_record_cache.rs:102 | ✅ exact |
| `rowRecordToRowsRow` | schema/cvr.ts:238 | `row_record_to_rows_row` row_record_cache.rs:136 | ✅ exact |
| `compareRowsRows` | schema/cvr.ts:253 | — | 🟥 UNRESOLVED |
| `createRowsVersionTable` | schema/cvr.ts:287 | — | 🟥 UNRESOLVED |
| `createRowsTable` | schema/cvr.ts:301 | — | 🟥 UNRESOLVED |
| `RowsVersionRow` | schema/cvr.ts:331 | `RowsVersionRow` row_record_cache.rs:181 | ✅ exact |
| `createTables` | schema/cvr.ts:336 | — | 🟥 UNRESOLVED |
| `stringifySorted` | schema/cvr.ts:357 | — | 🟥 UNRESOLVED |
| `cvrVersionSchema` | schema/types.ts:13 | — | 🟥 UNRESOLVED |
| `CVRVersion` | schema/types.ts:38 | `CVRVersion` version.rs:23 | ✅ exact |
| `EMPTY_CVR_VERSION` | schema/types.ts:40 | `EMPTY_CVR_VERSION` version.rs:43 | ✅ exact |
| `oneAfter` | schema/types.ts:44 | `one_after` version.rs:31 | ✅ exact |
| `NullableCVRVersion` | schema/types.ts:53 | — | 🟥 UNRESOLVED |
| `cmpVersions` | schema/types.ts:55 | `cmp_versions` version.rs:76 | ✅ exact |
| `maxVersion` | schema/types.ts:72 | `max_version` version.rs:86 | ✅ exact |
| `versionToCookie` | schema/types.ts:76 | `version_to_cookie` version.rs:100 | ✅ exact |
| `versionToNullableCookie` | schema/types.ts:80 | `version_to_nullable_cookie` version.rs:104 | ✅ exact |
| `cookieToVersion` | schema/types.ts:84 | — | 🟥 UNRESOLVED |
| `cvrIDSchema` | schema/types.ts:93 | — | 🟥 UNRESOLVED |
| `CvrID` | schema/types.ts:94 | — | 🟥 UNRESOLVED |
| `clientRecordSchema` | schema/types.ts:111 | — | 🟥 UNRESOLVED |
| `ClientRecord` | schema/types.ts:119 | `ClientRecord` types.rs:64 | ✅ exact |
| `baseQueryRecordSchema` | schema/types.ts:121 | `BaseQueryRecord` types.rs:98 | 🔁 rename 0.75 |
| `internalQueryRecordSchema` | schema/types.ts:183 | — | 🟥 UNRESOLVED |
| `InternalQueryRecord` | schema/types.ts:188 | `InternalQueryRecord` types.rs:109 | ✅ exact |
| `clientQueryRecordSchema` | schema/types.ts:228 | — | 🟥 UNRESOLVED |
| `ClientQueryRecord` | schema/types.ts:235 | `ClientQueryRecord` types.rs:116 | ✅ exact |
| `customQueryRecordSchema` | schema/types.ts:237 | — | 🟥 UNRESOLVED |
| `CustomQueryRecord` | schema/types.ts:243 | `CustomQueryRecord` types.rs:126 | ✅ exact |
| `queryRecordSchema` | schema/types.ts:245 | — | 🟥 UNRESOLVED |
| `QueryRecord` | schema/types.ts:251 | `QueryRecord` types.rs:87 | ✅ exact |
| `rowIDSchema` | schema/types.ts:253 | — | 🟥 UNRESOLVED |
| `RowID` | schema/types.ts:259 | `RowID` row_key.rs:45 | ✅ exact |
| `rowRecordSchema` | schema/types.ts:261 | — | 🟥 UNRESOLVED |
| `RowRecord` | schema/types.ts:269 | `RowRecord` types.rs:39 | ✅ exact |
| `patchSchema` | schema/types.ts:271 | — | 🟥 UNRESOLVED |
| `putRowPatchSchema` | schema/types.ts:276 | — | 🟥 UNRESOLVED |
| `delRowPatchSchema` | schema/types.ts:285 | — | 🟥 UNRESOLVED |
| `DelRowPatch` | schema/types.ts:291 | — | 🟥 UNRESOLVED |
| `rowPatchSchema` | schema/types.ts:293 | — | 🟥 UNRESOLVED |
| `queryPatchSchema` | schema/types.ts:297 | — | 🟥 UNRESOLVED |
| `QueryPatch` | schema/types.ts:303 | `QueryPatch` types.rs:237 | ✅ exact |
| `PutQueryPatch` | schema/types.ts:305 | — | 🟥 UNRESOLVED |
| `DelQueryPatch` | schema/types.ts:306 | — | 🟥 UNRESOLVED |
| `metadataPatchSchema` | schema/types.ts:308 | — | 🟥 UNRESOLVED |
| `MetadataPatch` | schema/types.ts:310 | — | 🟥 UNRESOLVED |
| `versionString` | schema/types.ts:312 | `version_string` version.rs:115 | ✅ exact |
| `versionFromString` | schema/types.ts:322 | `version_from_string` version.rs:173 | ✅ exact |
| `queryRecordToQueryRow` | schema/types.ts:342 | `query_record_to_query_row` store.rs:1468 | ✅ exact |
| `maybeVersionString` | schema/types.ts:392 | `try_version_from_string` version.rs:144 | 🔁 rename 0.40 |
| `TTLClock` | ttl-clock.ts:5 | — | 🟥 UNRESOLVED |
| `ttlClockSchema` | ttl-clock.ts:7 | — | 🟥 UNRESOLVED |
| `ttlClockAsNumber` | ttl-clock.ts:9 | IDENTITY | 📌 TTLClock = i64 (types.rs:14); no conversion |
| `ttlClockFromNumber` | ttl-clock.ts:13 | IDENTITY | 📌 TTLClock = i64 (types.rs:14); no conversion |
