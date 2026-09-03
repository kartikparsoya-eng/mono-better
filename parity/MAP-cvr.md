# TS ⇄ Rust parity map — `cvr` crate

_Deterministic. File edges + symbol pairs are derived from **shared symbol content**, never filenames — so renamed files (e.g. `drain-coordinator.ts`→`drain.rs`) and renamed symbols (`cvrErrorKind`→`CVRStoreError`) still bind. Bodies are not compared; behavior drift needs Layer-2 body review._

- symbols: TS **178**, Rust **349** · resolved pairs **117** (exact 117 + fuzzy 0) + aliases 19
- 🟥 TS UNRESOLVED: **46** (**0** behavioral ⇒ investigate · 46 structural: zod/DDL/type-alias ⇒ serde/inline-SQL, expected) · 🟦 Rust-only ADDED: **232**

## 1 · File structure diff

TS origin files: **9**  ·  Rust files: **23** (10 new)

| TS file (LOC) | rel | Rust file(s) (shared syms) |
|---|---|---|
| `client-handler.ts` (467) | **1:1** | `client_handler.rs` (20) |
| `cvr-store.ts` (1447) | **1:1** | `cvr_store.rs` (35), `live_count.rs` (1), `otel_metrics.rs` (1) |
| `cvr.ts` (1197) | **1:1** | `cvr.rs` (34), `change_processor.rs` (1) |
| `row-record-cache.ts` (485) | **1:1** | `row_record_cache.rs` (9), `otel_metrics.rs` (2) |
| `row-set-signature.ts` (30) | **1:1** | `row_set_signature.rs` (3) |
| `schema/cvr.ts` (359) | **1:1** | `schema/cvr.rs` (8), `seq_replay.rs` (1) |
| `schema/types.ts` (393) | **1:1** | `schema/types.rs` (21) |
| `shared/string-compare.ts` (9) | **1:1** | `shared/string_compare.rs` (1) |
| `ttl-clock.ts` (15) | **1:1** | `ttl_clock.rs` (3) |

**New Rust files (no TS origin — added in the port):**  `bin/cvr_seq_replay.rs` (39), `hash.rs` (75), `lib.rs` (36), `parity_check.rs` (1657), `row_key.rs` (318), `schema/mod.rs` (6), `shards.rs` (28), `shared.rs` (4), `tracer.rs` (41), `ttl.rs` (145)

**Merges (many TS → one Rust file):**
- `otel_metrics.rs` ⟵ `cvr-store.ts`, `row-record-cache.ts`

## 2 · Per-file functional divergence

### `bin/cvr_seq_replay.rs`  ⟵  _(new)_


🟦 **Rust-only added here (1):** `main`

### `change_processor.rs`  ⟵  `cvr.ts`


🟦 **Rust-only added here (13):** `ChangeProcessor`, `DEFAULT_CURSOR_PAGE_SIZE`, `RowChangeType`, `ZERO_VERSION_COLUMN_NAME`, `cursor_page_size`, `cursor_page_size_from`, `finish`, `finish_received`, `flush_batch`, `new`, `on_row_change`, `total_processed`, `with_page_size`

### `client_handler.rs`  ⟵  `client-handler.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `#push` (client-handler.ts:170) | `push` (:94) | exact |
| `#updateLMIDs` (client-handler.ts:376) | `update_lmids` (:569) | exact |
| `addPatch` (client-handler.ts:73) | `add_patch` (:292) | exact |
| `cancel` (client-handler.ts:74) | `cancel` (:104) | exact |
| `ClientHandler` (client-handler.ts:114) | `ClientHandler` (:766) | exact |
| `close` (client-handler.ts:183) | `close` (:837) | exact |
| `end` (client-handler.ts:75) | `end` (:429) | exact |
| `ensureSafeJSON` (client-handler.ts:449) | `ensure_safe_json` (:709) | exact |
| `fail` (client-handler.ts:175) | `fail` (:103) | exact |
| `makeRowPatch` (client-handler.ts:416) | `make_row_patch` (:729) | exact |
| `Patch` (client-handler.ts:65) | `Patch` (:25) | exact |
| `PatchToVersion` (client-handler.ts:67) | `PatchToVersion` (:44) | exact |
| `PokeHandler` (client-handler.ts:72) | `PokeHandler` (:259) | exact |
| `RowPatch` (client-handler.ts:62) | `RowPatch` (:33) | exact |
| `sendDeleteClients` (client-handler.ts:347) | `send_delete_clients` (:902) | exact |
| `sendInspectResponse` (client-handler.ts:371) | `send_inspect_response` (:932) | exact |
| `sendQueryTransformApplicationErrors` (client-handler.ts:363) | `send_query_transform_application_errors` (:924) | exact |
| `sendQueryTransformFailedError` (client-handler.ts:367) | `send_query_transform_failed_error` (:942) | exact |
| `startPoke` (client-handler.ts:85) | `start_poke` (:842) | exact |
| `version` (client-handler.ts:166) | `version` (:829) | exact |

🟥 **TS symbols not resolved into this file (3):** `ConfigPatch`, `DeleteRowPatch`, `PutRowPatch`

🟦 **Rust-only added here (31):** `DEFAULT_POKE_PART_MAX_BYTES`, `MAX_DEPTH`, `MAX_SAFE_INTEGER`, `MultiPoker`, `MutationPatchEntry`, `MutationPatchId`, `MutationPatchMutation`, `PART_COUNT_FLUSH_THRESHOLD`, `POKE_PART_ENVELOPE_EST`, `PokePartBody`, `PokeState`, `QueryPatchEntry`, `ROW_PATCH_ENVELOPE_EST`, `RowPatchInfo`, `RowPatchOp`, `V`, `WebSocketSink`, `acquire_chain`, `add_mutation_patch`, `drop`, `ensure_body`, `estimate_json_bytes`, `estimate_row_patch_bytes`, `flush_body`, `go`, `normalize_mutation_result`, `poke_part_max_bytes`, `push_sized`, `release_chain`, `set_base_version_for_test`, `upstream_schema`

### `cvr.rs`  ⟵  `cvr.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `#assertNewVersion` (cvr.ts:769) | `assert_new_version` (:1031) | exact |
| `#deleteQueries` (cvr.ts:429) | `delete_queries` (:653) | exact |
| `#trackExecuted` (cvr.ts:701) | `track_executed` (:936) | exact |
| `#trackRemoved` (cvr.ts:742) | `track_removed` (:982) | exact |
| `_ensureNewVersion` (cvr.ts:176) | `ensure_new_version` (:313) | exact |
| `_setVersion` (cvr.ts:162) | `set_version` (:302) | exact |
| `assertNotInternal` (cvr.ts:87) | `assert_not_internal` (:172) | exact |
| `clearDesiredQueries` (cvr.ts:497) | `clear_desired_queries` (:786) | exact |
| `CVR` (cvr.ts:58) | `CVR` (:1355) | exact |
| `CVRConfigDrivenUpdater` (cvr.ts:212) | `CVRConfigDrivenUpdater` (:353) | exact |
| `CVRQueryDrivenUpdater` (cvr.ts:560) | `CVRQueryDrivenUpdater` (:832) | exact |
| `CVRUpdater` (cvr.ts:141) | `CVRUpdater` (:284) | exact |
| `deleteClient` (cvr.ts:502) | `delete_client` (:800) | exact |
| `deleteDesiredQueries` (cvr.ts:422) | `delete_desired_queries` (:645) | exact |
| `deleteUnreferencedRows` (cvr.ts:959) | `delete_unreferenced_rows` (:1190) | exact |
| `ensureClient` (cvr.ts:220) | `ensure_client` (:372) | exact |
| `flush` (cvr.ts:183) | `flush` (:333) | exact |
| `getInactiveQueries` (cvr.ts:1087) | `get_inactive_queries` (:191) | exact |
| `getMutationResultsQuery` (cvr.ts:96) | `get_mutation_results_query` (:139) | exact |
| `markDesiredQueriesAsInactive` (cvr.ts:414) | `mark_desired_queries_as_inactive` (:635) | exact |
| `mergeRefCounts` (cvr.ts:1049) | `merge_ref_counts` (:41) | exact |
| `newQueryRecord` (cvr.ts:1167) | `new_query_record` (:98) | exact |
| `nextEvictionTime` (cvr.ts:1156) | `next_eviction_time` (:265) | exact |
| `putDesiredQueries` (cvr.ts:317) | `put_desired_queries` (:489) | exact |
| `received` (cvr.ts:836) | `received` (:1045) | exact |
| `RefCounts` (cvr.ts:531) | `RefCounts` (:1338) | exact |
| `RowSetSignatureProvider` (cvr.ts:544) | `RowSetSignatureProvider` (:278) | exact |
| `RowUpdate` (cvr.ts:51) | `RowUpdate` (:1346) | exact |
| `setClientSchema` (cvr.ts:273) | `set_client_schema` (:445) | exact |
| `setProfileID` (cvr.ts:299) | `set_profile_id` (:471) | exact |
| `trackQueries` (cvr.ts:617) | `track_queries` (:905) | exact |
| `updatedVersion` (cvr.ts:789) | `updated_version` (:894) | exact |

🟥 **TS symbols not resolved into this file (2):** `CVRSnapshot`, `Column`

🟦 **Rust-only added here (36):** `CLIENT_LMID_QUERY_ID`, `CLIENT_MUTATION_RESULTS_QUERY_ID`, `DesiredQuerySpec`, `InactiveQuery`, `RowRecordMap`, `StoreOp`, `drain_store_ops`, `make_query_driven_updater`, `make_shard`, `make_test_cvr`, `test_clear_desired_queries`, `test_delete_client`, `test_delete_client_not_found`, `test_delete_desired_queries`, `test_delete_unreferenced_rows`, `test_ensure_client_creates_client_and_internal_queries`, `test_ensure_client_idempotent`, `test_flush_records_signature_drift_only_when_changed`, `test_flush_with_signature_provider`, `test_inactivate_missing_client_state_does_not_fabricate_entry`, `test_put_desired_queries_new`, `test_put_desired_queries_no_change`, `test_query_updater_bumps_version_on_new_state_version`, `test_query_updater_does_not_bump_on_same_state_version`, `test_received_new_row`, `test_received_no_bump_changed_row_returns_err_not_panic`, `test_received_null_then_reref_drops_stale_existing_refs`, `test_received_unref_row`, `test_set_client_schema_mismatch`, `test_set_client_schema_new`, `test_set_client_schema_same`, `test_set_profile_id`, `test_track_queries_executed`, `test_track_queries_removed`, `test_unref_empty_row_version_bumps_patch_version`, `test_updated_version_tracks_live_cvr_version`

### `cvr_store.rs`  ⟵  `cvr-store.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `#flush` (cvr-store.ts:1051) | `flush` (:656) | exact |
| `asQuery` (cvr-store.ts:119) | `as_query` (:1829) | exact |
| `catchupConfigPatches` (cvr-store.ts:725) | `catchup_config_patches` (:176) | exact |
| `catchupRowPatches` (cvr-store.ts:709) | `catchup_row_patches` (:353) | exact |
| `cvrErrorKind` (cvr-store.ts:1421) | `cvr_error_kind` (:1819) | exact |
| `CVRFlushStats` (cvr-store.ts:67) | `CVRFlushStats` (:102) | exact |
| `deleteClient` (cvr-store.ts:674) | `delete_client` (:496) | exact |
| `delRowRecord` (cvr-store.ts:536) | `del_row_record` (:598) | exact |
| `flushed` (cvr-store.ts:1284) | `flushed` (:347) | exact |
| `forceUpdates` (cvr-store.ts:545) | `force_updates` (:605) | exact |
| `getRowRecords` (cvr-store.ts:520) | `get_row_records` (:334) | exact |
| `getTTLClock` (cvr-store.ts:569) | `get_ttl_clock` (:461) | exact |
| `insertClient` (cvr-store.ts:662) | `insert_client` (:489) | exact |
| `inspectQueries` (cvr-store.ts:1288) | `inspect_queries` (:388) | exact |
| `load` (cvr-store.ts:274) | `load` (:1304) | exact |
| `markQueryAsDeleted` (cvr-store.ts:620) | `mark_query_as_deleted` (:535) | exact |
| `putDesiredQuery` (cvr-store.ts:684) | `put_desired_query` (:560) | exact |
| `putInstance` (cvr-store.ts:584) | `put_instance` (:475) | exact |
| `putQuery` (cvr-store.ts:629) | `put_query` (:502) | exact |
| `putRowRecord` (cvr-store.ts:524) | `put_row_record` (:591) | exact |
| `rowCount` (cvr-store.ts:1227) | `row_count` (:377) | exact |
| `updateQuery` (cvr-store.ts:644) | `update_query` (:509) | exact |
| `updateRowSetSignature` (cvr-store.ts:658) | `update_row_set_signature` (:551) | exact |
| `updateTTLClock` (cvr-store.ts:556) | `update_ttl_clock` (:438) | exact |

🟥 **TS symbols not resolved into this file (3):** `ConcurrentModificationException`, `InvalidClientSchemaError`, `OwnershipError`

🟦 **Rust-only added here (22):** `CVRStoreCatchupReader`, `CVRStoreError`, `CVRStoreHandle`, `DesireLoadRow`, `InspectQueryRow`, `InspectQueryRowDb`, `InstanceLoadRow`, `LOAD_ATTEMPT_INTERVAL_MS`, `LoadResult`, `MAX_LOAD_ATTEMPTS`, `PartialQueriesRow`, `PendingWrites`, `QueryLoadRow`, `apply_store_ops`, `catchup_reader`, `flush_internal`, `from`, `has_pending_writes`, `is_empty`, `load_once`, `load_with_retries`, `seed_row_cache_for_test`

### `hash.rs`  ⟵  _(new)_


🟦 **Rust-only added here (4):** `h128`, `h32`, `h64`, `xxh32_seeded`

### `live_count.rs`  ⟵  `cvr-store.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `CVRStore` (cvr-store.ts:180) | `CVR_STORE` (:17) | exact |

🟦 **Rust-only added here (5):** `CONFIG_DRIVEN_UPDATER`, `Guard`, `QUERY_DRIVEN_UPDATER`, `drop_backtrace`, `snapshot`

### `otel_metrics.rs`  ⟵  `cvr-store.ts`, `row-record-cache.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `#recordAsyncFlushStats` (row-record-cache.ts:153) | `record_async_flush_stats` (:181) | exact |
| `#recordLoad` (cvr-store.ts:308) | `record_load` (:118) | exact |
| `recordSyncFlushStats` (row-record-cache.ts:144) | `record_sync_flush_stats` (:169) | exact |

🟦 **Rust-only added here (7):** `Instruments`, `LATENCY_BOUNDARIES_S`, `record_flush_attempt`, `record_poke`, `record_poked_row`, `record_query`, `record_row_set_signature_drift`

### `parity_check.rs`  ⟵  _(new)_


🟦 **Rust-only added here (24):** `CaptureSink`, `base_cvr`, `build_client_state`, `build_cvr_from_spec`, `build_existing_rows`, `build_query_record_from_spec`, `build_received_rows`, `build_row_patch_from_spec`, `dummy_base`, `make_row_id_from_json`, `norm_desire_state`, `norm_patch`, `norm_put_desired_op`, `parity_check`, `parity_shard`, `parse_refcounts`, `parse_u64`, `patch_sort_key`, `patch_to_version_from_json`, `queries_row_from_json`, `queries_row_to_json`, `sorted_norm`, `spec_from_json`, `ttl_from_json`

### `row_key.rs`  ⟵  _(new)_


🟦 **Rust-only added here (12):** `CACHE_GEN_CAP`, `DIGITS`, `RowIdStringCache`, `RowKey`, `RowKeyType`, `base36_encode`, `get`, `insert`, `normalized_key_order`, `row_id_hash`, `row_id_string`, `row_id_string_cached`

### `row_record_cache.rs`  ⟵  `row-record-cache.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `apply` (row-record-cache.ts:234) | `apply` (:325) | exact |
| `catchupRowPatches` (row-record-cache.ts:341) | `catchup_row_patches` (:527) | exact |
| `clear` (row-record-cache.ts:334) | `clear` (:462) | exact |
| `executeRowUpdates` (row-record-cache.ts:414) | `execute_row_updates` (:475) | exact |
| `flushed` (row-record-cache.ts:326) | `flushed` (:441) | exact |
| `getRowRecords` (row-record-cache.ts:215) | `get_row_records` (:306) | exact |
| `hasPendingUpdates` (row-record-cache.ts:318) | `has_pending_updates` (:431) | exact |
| `RowRecordCache` (row-record-cache.ts:90) | `RowRecordCache` (:190) | exact |

🟦 **Rust-only added here (21):** `CATCHUP_PAGE_SIZE`, `CacheState`, `CatchupCursor`, `CatchupTaskContext`, `DEFAULT_DEFERRED_THRESHOLD`, `ExecuteResult`, `FailCallback`, `FlushLoopContext`, `FlushMode`, `IDLE_TX_TIMEOUT_MS`, `MetricsCallback`, `RowKeyRef`, `RowUpdateStatements`, `RowsRowDb`, `catchup_task`, `catchup_task_inner`, `empty`, `flush_loop`, `flush_one_iteration`, `next_page`, `seed_for_test`

### `row_set_signature.rs`  ⟵  `row-set-signature.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `formatSignature` (row-set-signature.ts:28) | `format_signature` (:31) | exact |
| `parseSignature` (row-set-signature.ts:18) | `parse_signature` (:23) | exact |
| `rowIDSignatureUnit` (row-set-signature.ts:10) | `row_id_signature_unit` (:17) | exact |

### `schema/cvr.rs`  ⟵  `schema/cvr.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `ClientsRow` (schema/cvr.ts:76) | `ClientsRow` (:25) | exact |
| `DesiresRow` (schema/cvr.ts:159) | `DesiresRow` (:44) | exact |
| `InstancesRow` (schema/cvr.ts:31) | `InstancesRow` (:13) | exact |
| `QueriesRow` (schema/cvr.ts:105) | `QueriesRow` (:30) | exact |
| `rowRecordToRowsRow` (schema/cvr.ts:238) | `row_record_to_rows_row` (:128) | exact |
| `RowsRow` (schema/cvr.ts:211) | `RowsRow` (:58) | exact |
| `rowsRowToRowRecord` (schema/cvr.ts:229) | `rows_row_to_row_record` (:95) | exact |
| `RowsVersionRow` (schema/cvr.ts:331) | `RowsVersionRow` (:73) | exact |

🟥 **TS symbols not resolved into this file (16):** `compareClientsRows`, `compareDesiresRows`, `compareInstancesRows`, `compareQueriesRows`, `compareRowsRows`, `createClientsTable`, `createDesiresTable`, `createInstancesTable`, `createQueriesTable`, `createRowsTable`, `createRowsVersionTable`, `createSchema`, `createTables`, `rowsRowToRowID`, `setupCVRTables`, `stringifySorted`

🟦 **Rust-only added here (1):** `RowRecordError`

### `schema/types.rs`  ⟵  `schema/types.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `ClientQueryRecord` (schema/types.ts:235) | `ClientQueryRecord` (:375) | exact |
| `ClientRecord` (schema/types.ts:119) | `ClientRecord` (:326) | exact |
| `cmpVersions` (schema/types.ts:55) | `cmp_versions` (:83) | exact |
| `CustomQueryRecord` (schema/types.ts:243) | `CustomQueryRecord` (:385) | exact |
| `CVRVersion` (schema/types.ts:38) | `CVRVersion` (:30) | exact |
| `EMPTY_CVR_VERSION` (schema/types.ts:40) | `EMPTY_CVR_VERSION` (:50) | exact |
| `InternalQueryRecord` (schema/types.ts:188) | `InternalQueryRecord` (:368) | exact |
| `maxVersion` (schema/types.ts:72) | `max_version` (:93) | exact |
| `maybeVersionString` (schema/types.ts:392) | `maybe_version_string` (:189) | exact |
| `NullableCVRVersion` (schema/types.ts:53) | `NullableCVRVersion` (:63) | exact |
| `oneAfter` (schema/types.ts:44) | `one_after` (:38) | exact |
| `QueryPatch` (schema/types.ts:303) | `QueryPatch` (:459) | exact |
| `QueryRecord` (schema/types.ts:251) | `QueryRecord` (:346) | exact |
| `queryRecordToQueryRow` (schema/types.ts:342) | `query_record_to_query_row` (:488) | exact |
| `RowID` (schema/types.ts:259) | `RowID` (:479) | exact |
| `RowRecord` (schema/types.ts:269) | `RowRecord` (:317) | exact |
| `versionFromString` (schema/types.ts:322) | `version_from_string` (:225) | exact |
| `versionString` (schema/types.ts:312) | `version_string` (:122) | exact |
| `versionToCookie` (schema/types.ts:76) | `version_to_cookie` (:107) | exact |
| `versionToNullableCookie` (schema/types.ts:80) | `version_to_nullable_cookie` (:111) | exact |

🟥 **TS symbols not resolved into this file (21):** `CvrID`, `DelQueryPatch`, `DelRowPatch`, `MetadataPatch`, `PutQueryPatch`, `clientQueryRecordSchema`, `clientRecordSchema`, `cookieToVersion`, `customQueryRecordSchema`, `cvrIDSchema`, `cvrVersionSchema`, `delRowPatchSchema`, `internalQueryRecordSchema`, `metadataPatchSchema`, `patchSchema`, `putRowPatchSchema`, `queryPatchSchema`, `queryRecordSchema`, `rowIDSchema`, `rowPatchSchema`, `rowRecordSchema`

🟦 **Rust-only added here (18):** `AST`, `BaseQueryRecord`, `ClientSchema`, `ClientState`, `VersionError`, `base`, `base_mut`, `client_state_mut`, `cmp_cvr`, `from_base36_u64`, `id`, `is_internal`, `patch_version`, `patch_version_mut`, `to_base36_u64`, `validate_state_version`, `version_from_lexi`, `version_to_lexi`

### `seq_replay.rs`  ⟵  `schema/cvr.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `schema` (schema/cvr.ts:23) | `SCHEMA` (:28) | exact |

🟦 **Rust-only added here (18):** `DDL`, `Op`, `Program`, `QSpec`, `ReceivedRow`, `RowIdJson`, `Shard`, `TASK_ID`, `Track`, `Txn`, `canon_patch`, `canonicalize`, `default_kind`, `dump`, `load_existing_rows`, `push_patches`, `reset_schema`, `run`

### `shards.rs`  ⟵  _(new)_


🟦 **Rust-only added here (3):** `ShardID`, `app_schema`, `cvr_schema`

### `shared/string_compare.rs`  ⟵  `shared/string-compare.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `stringCompare` (shared/string-compare.ts:1) | `string_compare` (:15) | exact |

🟦 **Rust-only added here (1):** `utf16_lead`

### `tracer.rs`  ⟵  _(new)_


🟦 **Rust-only added here (3):** `ENABLED`, `note`, `recv`

### `ttl.rs`  ⟵  _(new)_


🟦 **Rust-only added here (12):** `DEFAULT_TTL_MS`, `MAX_TTL_MS`, `MULT_D`, `MULT_H`, `MULT_M`, `MULT_S`, `MULT_Y`, `TTL`, `clamp_ttl`, `compare_ttl`, `parse_ttl`, `parse_ttl_string`

### `ttl_clock.rs`  ⟵  `ttl-clock.ts`

| TS symbol | Rust symbol | match |
|---|---|---|
| `TTLClock` (ttl-clock.ts:5) | `TTLClock` (:5) | exact |

🟥 **TS symbols not resolved into this file (1):** `ttlClockSchema`

## 3 · Flat one-to-one symbol map (every TS symbol resolved)

| TS symbol | origin | → Rust | status |
|---|---|---|---|
| `PutRowPatch` | client-handler.ts:49 | — | 🟥 UNRESOLVED |
| `DeleteRowPatch` | client-handler.ts:56 | — | 🟥 UNRESOLVED |
| `RowPatch` | client-handler.ts:62 | `RowPatch` client_handler.rs:33 | ✅ exact |
| `ConfigPatch` | client-handler.ts:63 | — | 🟥 UNRESOLVED |
| `Patch` | client-handler.ts:65 | `Patch` client_handler.rs:25 | ✅ exact |
| `PatchToVersion` | client-handler.ts:67 | `PatchToVersion` client_handler.rs:44 | ✅ exact |
| `PokeHandler` | client-handler.ts:72 | `PokeHandler` client_handler.rs:259 | ✅ exact |
| `addPatch` | client-handler.ts:73 | `add_patch` client_handler.rs:292 | ✅ exact |
| `cancel` | client-handler.ts:74 | `cancel` client_handler.rs:104 | ✅ exact |
| `end` | client-handler.ts:75 | `end` client_handler.rs:429 | ✅ exact |
| `startPoke` | client-handler.ts:85 | `start_poke` client_handler.rs:842 | ✅ exact |
| `ClientHandler` | client-handler.ts:114 | `ClientHandler` client_handler.rs:766 | ✅ exact |
| `version` | client-handler.ts:166 | `version` client_handler.rs:829 | ✅ exact |
| `#push` | client-handler.ts:170 | `push` client_handler.rs:94 | ✅ exact |
| `fail` | client-handler.ts:175 | `fail` client_handler.rs:103 | ✅ exact |
| `close` | client-handler.ts:183 | `close` client_handler.rs:837 | ✅ exact |
| `sendDeleteClients` | client-handler.ts:347 | `send_delete_clients` client_handler.rs:902 | ✅ exact |
| `sendQueryTransformApplicationErrors` | client-handler.ts:363 | `send_query_transform_application_errors` client_handler.rs:924 | ✅ exact |
| `sendQueryTransformFailedError` | client-handler.ts:367 | `send_query_transform_failed_error` client_handler.rs:942 | ✅ exact |
| `sendInspectResponse` | client-handler.ts:371 | `send_inspect_response` client_handler.rs:932 | ✅ exact |
| `#updateLMIDs` | client-handler.ts:376 | `update_lmids` client_handler.rs:569 | ✅ exact |
| `makeRowPatch` | client-handler.ts:416 | `make_row_patch` client_handler.rs:729 | ✅ exact |
| `ensureSafeJSON` | client-handler.ts:449 | `ensure_safe_json` client_handler.rs:709 | ✅ exact |
| `CVRFlushStats` | cvr-store.ts:67 | `CVRFlushStats` cvr_store.rs:102 | ✅ exact |
| `convertTTLValues` | cvr-store.ts:88 | INLINED | 📌 cvr_store.rs upsert SQL: ttl/1000 + null-on-negative |
| `asQuery` | cvr-store.ts:119 | `as_query` cvr_store.rs:1829 | ✅ exact |
| `CVRStore` | cvr-store.ts:180 | `CVR_STORE` live_count.rs:17 | ✅ exact |
| `CVR` | cvr.ts:58 | `CVR` cvr.rs:1355 | ✅ exact |
| `#updateQueryFields` | cvr-store.ts:263 | INLINED cvr_store.rs queries json_to_recordset upsert | 📌 patchVersion/transformationHash/-Version columns |
| `load` | cvr-store.ts:274 | `load` cvr_store.rs:1304 | ✅ exact |
| `#recordLoad` | cvr-store.ts:308 | `record_load` otel_metrics.rs:118 | ✅ exact |
| `getRowRecords` | cvr-store.ts:520 | `get_row_records` cvr_store.rs:334 | ✅ exact |
| `putRowRecord` | cvr-store.ts:524 | `put_row_record` cvr_store.rs:591 | ✅ exact |
| `delRowRecord` | cvr-store.ts:536 | `del_row_record` cvr_store.rs:598 | ✅ exact |
| `forceUpdates` | cvr-store.ts:545 | `force_updates` cvr_store.rs:605 | ✅ exact |
| `updateTTLClock` | cvr-store.ts:556 | `update_ttl_clock` cvr_store.rs:438 | ✅ exact |
| `getTTLClock` | cvr-store.ts:569 | `get_ttl_clock` cvr_store.rs:461 | ✅ exact |
| `putInstance` | cvr-store.ts:584 | `put_instance` cvr_store.rs:475 | ✅ exact |
| `markQueryAsDeleted` | cvr-store.ts:620 | `mark_query_as_deleted` cvr_store.rs:535 | ✅ exact |
| `putQuery` | cvr-store.ts:629 | `put_query` cvr_store.rs:502 | ✅ exact |
| `updateQuery` | cvr-store.ts:644 | `update_query` cvr_store.rs:509 | ✅ exact |
| `updateRowSetSignature` | cvr-store.ts:658 | `update_row_set_signature` cvr_store.rs:551 | ✅ exact |
| `insertClient` | cvr-store.ts:662 | `insert_client` cvr_store.rs:489 | ✅ exact |
| `deleteClient` | cvr-store.ts:674 | `delete_client` cvr_store.rs:496 | ✅ exact |
| `putDesiredQuery` | cvr-store.ts:684 | `put_desired_query` cvr_store.rs:560 | ✅ exact |
| `catchupRowPatches` | cvr-store.ts:709 | `catchup_row_patches` cvr_store.rs:353 | ✅ exact |
| `catchupConfigPatches` | cvr-store.ts:725 | `catchup_config_patches` cvr_store.rs:176 | ✅ exact |
| `#flushQueries` | cvr-store.ts:797 | INLINED cvr_store.rs flush_internal queries upsert (:835) | 📌 doc-cited |
| `#flushDesires` | cvr-store.ts:944 | INLINED cvr_store.rs flush_internal desires upsert (:959) | 📌 doc-cited |
| `#checkVersionAndOwnership` | cvr-store.ts:1018 | INLINED cvr_store.rs flush_internal (:700) | 📌 doc-cited version+ownership guard; Err rolls back tx |
| `#flush` | cvr-store.ts:1051 | `flush` cvr_store.rs:656 | ✅ exact |
| `rowCount` | cvr-store.ts:1227 | `row_count` cvr_store.rs:377 | ✅ exact |
| `hasPendingUpdates` | row-record-cache.ts:318 | `has_pending_updates` row_record_cache.rs:431 | ✅ exact |
| `flushed` | cvr-store.ts:1284 | `flushed` cvr_store.rs:347 | ✅ exact |
| `inspectQueries` | cvr-store.ts:1288 | `inspect_queries` cvr_store.rs:388 | ✅ exact |
| `checkVersion` | cvr-store.ts:1337 | INLINED cvr_store.rs catchup version guard | 📌 plain-SELECT re-check of instances.version vs `current` |
| `ClientNotFoundError` | cvr-store.ts:1354 | CVRStoreError::ClientNotFound (cvr_store.rs:47) | 📌 TS error class → Rust enum variant |
| `ConcurrentModificationException` | cvr-store.ts:1367 | — | 🟥 UNRESOLVED |
| `OwnershipError` | cvr-store.ts:1382 | — | 🟥 UNRESOLVED |
| `InvalidClientSchemaError` | cvr-store.ts:1405 | — | 🟥 UNRESOLVED |
| `cvrErrorKind` | cvr-store.ts:1421 | `cvr_error_kind` cvr_store.rs:1819 | ✅ exact |
| `RowsVersionBehindError` | cvr-store.ts:1437 | CVRStoreError::RowsVersionBehind (cvr_store.rs:49) | 📌 TS error class → Rust enum variant |
| `RowUpdate` | cvr.ts:51 | `RowUpdate` cvr.rs:1346 | ✅ exact |
| `CVRSnapshot` | cvr.ts:72 | — | 🟥 UNRESOLVED |
| `assertNotInternal` | cvr.ts:87 | `assert_not_internal` cvr.rs:172 | ✅ exact |
| `getMutationResultsQuery` | cvr.ts:96 | `get_mutation_results_query` cvr.rs:139 | ✅ exact |
| `CVRUpdater` | cvr.ts:141 | `CVRUpdater` cvr.rs:284 | ✅ exact |
| `_setVersion` | cvr.ts:162 | `set_version` cvr.rs:302 | ✅ exact |
| `_ensureNewVersion` | cvr.ts:176 | `ensure_new_version` cvr.rs:313 | ✅ exact |
| `CVRConfigDrivenUpdater` | cvr.ts:212 | `CVRConfigDrivenUpdater` cvr.rs:353 | ✅ exact |
| `ensureClient` | cvr.ts:220 | `ensure_client` cvr.rs:372 | ✅ exact |
| `setClientSchema` | cvr.ts:273 | `set_client_schema` cvr.rs:445 | ✅ exact |
| `setProfileID` | cvr.ts:299 | `set_profile_id` cvr.rs:471 | ✅ exact |
| `putDesiredQueries` | cvr.ts:317 | `put_desired_queries` cvr.rs:489 | ✅ exact |
| `markDesiredQueriesAsInactive` | cvr.ts:414 | `mark_desired_queries_as_inactive` cvr.rs:635 | ✅ exact |
| `deleteDesiredQueries` | cvr.ts:422 | `delete_desired_queries` cvr.rs:645 | ✅ exact |
| `#deleteQueries` | cvr.ts:429 | `delete_queries` cvr.rs:653 | ✅ exact |
| `clearDesiredQueries` | cvr.ts:497 | `clear_desired_queries` cvr.rs:786 | ✅ exact |
| `Column` | cvr.ts:530 | — | 🟥 UNRESOLVED |
| `RefCounts` | cvr.ts:531 | `RefCounts` cvr.rs:1338 | ✅ exact |
| `RowSetSignatureProvider` | cvr.ts:544 | `RowSetSignatureProvider` cvr.rs:278 | ✅ exact |
| `CVRQueryDrivenUpdater` | cvr.ts:560 | `CVRQueryDrivenUpdater` cvr.rs:832 | ✅ exact |
| `trackQueries` | cvr.ts:617 | `track_queries` cvr.rs:905 | ✅ exact |
| `#lookupRowsForExecutedAndRemovedQueries` | cvr.ts:652 | INLINED cvr.rs (:1199) | 📌 doc-cited |
| `#trackExecuted` | cvr.ts:701 | `track_executed` cvr.rs:936 | ✅ exact |
| `#trackRemoved` | cvr.ts:742 | `track_removed` cvr.rs:982 | ✅ exact |
| `#assertNewVersion` | cvr.ts:769 | `assert_new_version` cvr.rs:1031 | ✅ exact |
| `updatedVersion` | cvr.ts:789 | `updated_version` cvr.rs:894 | ✅ exact |
| `received` | cvr.ts:836 | `received` cvr.rs:1045 | ✅ exact |
| `deleteUnreferencedRows` | cvr.ts:959 | `delete_unreferenced_rows` cvr.rs:1190 | ✅ exact |
| `#deleteUnreferencedRow` | cvr.ts:1004 | change_processor.rs delete_unreferenced_rows | 📌 renamed plural + relocated (doc-cited :201) |
| `mergeRefCounts` | cvr.ts:1049 | `merge_ref_counts` cvr.rs:41 | ✅ exact |
| `getInactiveQueries` | cvr.ts:1087 | `get_inactive_queries` cvr.rs:191 | ✅ exact |
| `nextEvictionTime` | cvr.ts:1156 | `next_eviction_time` cvr.rs:265 | ✅ exact |
| `newQueryRecord` | cvr.ts:1167 | `new_query_record` cvr.rs:98 | ✅ exact |
| `assert` | cvr.ts:1186 | Rust assert! macro | 📌 idiom (D-16) |
| `RowRecordCache` | row-record-cache.ts:90 | `RowRecordCache` row_record_cache.rs:190 | ✅ exact |
| `recordSyncFlushStats` | row-record-cache.ts:144 | `record_sync_flush_stats` otel_metrics.rs:169 | ✅ exact |
| `#recordAsyncFlushStats` | row-record-cache.ts:153 | `record_async_flush_stats` otel_metrics.rs:181 | ✅ exact |
| `#ensureLoaded` | row-record-cache.ts:164 | INLINED row_record_cache.rs (:239) | 📌 doc-cited lazy load |
| `apply` | row-record-cache.ts:234 | `apply` row_record_cache.rs:325 | ✅ exact |
| `clear` | row-record-cache.ts:334 | `clear` row_record_cache.rs:462 | ✅ exact |
| `executeRowUpdates` | row-record-cache.ts:414 | `execute_row_updates` row_record_cache.rs:475 | ✅ exact |
| `rowIDSignatureUnit` | row-set-signature.ts:10 | `row_id_signature_unit` row_set_signature.rs:17 | ✅ exact |
| `parseSignature` | row-set-signature.ts:18 | `parse_signature` row_set_signature.rs:23 | ✅ exact |
| `formatSignature` | row-set-signature.ts:28 | `format_signature` row_set_signature.rs:31 | ✅ exact |
| `schema` | schema/cvr.ts:23 | `SCHEMA` seq_replay.rs:28 | ✅ exact |
| `createSchema` | schema/cvr.ts:27 | — | 🟥 UNRESOLVED |
| `InstancesRow` | schema/cvr.ts:31 | `InstancesRow` schema/cvr.rs:13 | ✅ exact |
| `createInstancesTable` | schema/cvr.ts:43 | — | 🟥 UNRESOLVED |
| `compareInstancesRows` | schema/cvr.ts:72 | — | 🟥 UNRESOLVED |
| `ClientsRow` | schema/cvr.ts:76 | `ClientsRow` schema/cvr.rs:25 | ✅ exact |
| `createClientsTable` | schema/cvr.ts:81 | — | 🟥 UNRESOLVED |
| `compareClientsRows` | schema/cvr.ts:97 | — | 🟥 UNRESOLVED |
| `QueriesRow` | schema/cvr.ts:105 | `QueriesRow` schema/cvr.rs:30 | ✅ exact |
| `createQueriesTable` | schema/cvr.ts:122 | — | 🟥 UNRESOLVED |
| `compareQueriesRows` | schema/cvr.ts:151 | — | 🟥 UNRESOLVED |
| `DesiresRow` | schema/cvr.ts:159 | `DesiresRow` schema/cvr.rs:44 | ✅ exact |
| `createDesiresTable` | schema/cvr.ts:169 | — | 🟥 UNRESOLVED |
| `compareDesiresRows` | schema/cvr.ts:199 | — | 🟥 UNRESOLVED |
| `RowsRow` | schema/cvr.ts:211 | `RowsRow` schema/cvr.rs:58 | ✅ exact |
| `rowsRowToRowID` | schema/cvr.ts:221 | — | 🟥 UNRESOLVED |
| `rowsRowToRowRecord` | schema/cvr.ts:229 | `rows_row_to_row_record` schema/cvr.rs:95 | ✅ exact |
| `rowRecordToRowsRow` | schema/cvr.ts:238 | `row_record_to_rows_row` schema/cvr.rs:128 | ✅ exact |
| `compareRowsRows` | schema/cvr.ts:253 | — | 🟥 UNRESOLVED |
| `createRowsVersionTable` | schema/cvr.ts:287 | — | 🟥 UNRESOLVED |
| `createRowsTable` | schema/cvr.ts:301 | — | 🟥 UNRESOLVED |
| `RowsVersionRow` | schema/cvr.ts:331 | `RowsVersionRow` schema/cvr.rs:73 | ✅ exact |
| `createTables` | schema/cvr.ts:336 | — | 🟥 UNRESOLVED |
| `setupCVRTables` | schema/cvr.ts:348 | — | 🟥 UNRESOLVED |
| `stringifySorted` | schema/cvr.ts:357 | — | 🟥 UNRESOLVED |
| `cvrVersionSchema` | schema/types.ts:13 | — | 🟥 UNRESOLVED |
| `CVRVersion` | schema/types.ts:38 | `CVRVersion` schema/types.rs:30 | ✅ exact |
| `EMPTY_CVR_VERSION` | schema/types.ts:40 | `EMPTY_CVR_VERSION` schema/types.rs:50 | ✅ exact |
| `oneAfter` | schema/types.ts:44 | `one_after` schema/types.rs:38 | ✅ exact |
| `NullableCVRVersion` | schema/types.ts:53 | `NullableCVRVersion` schema/types.rs:63 | ✅ exact |
| `cmpVersions` | schema/types.ts:55 | `cmp_versions` schema/types.rs:83 | ✅ exact |
| `maxVersion` | schema/types.ts:72 | `max_version` schema/types.rs:93 | ✅ exact |
| `versionToCookie` | schema/types.ts:76 | `version_to_cookie` schema/types.rs:107 | ✅ exact |
| `versionToNullableCookie` | schema/types.ts:80 | `version_to_nullable_cookie` schema/types.rs:111 | ✅ exact |
| `cookieToVersion` | schema/types.ts:84 | — | 🟥 UNRESOLVED |
| `cvrIDSchema` | schema/types.ts:93 | — | 🟥 UNRESOLVED |
| `CvrID` | schema/types.ts:94 | — | 🟥 UNRESOLVED |
| `clientRecordSchema` | schema/types.ts:111 | — | 🟥 UNRESOLVED |
| `ClientRecord` | schema/types.ts:119 | `ClientRecord` schema/types.rs:326 | ✅ exact |
| `baseQueryRecordSchema` | schema/types.ts:121 | BaseQueryRecord struct (schema/types.rs:338) | 📌 valita schema → serde struct |
| `internalQueryRecordSchema` | schema/types.ts:183 | — | 🟥 UNRESOLVED |
| `InternalQueryRecord` | schema/types.ts:188 | `InternalQueryRecord` schema/types.rs:368 | ✅ exact |
| `clientQueryRecordSchema` | schema/types.ts:228 | — | 🟥 UNRESOLVED |
| `ClientQueryRecord` | schema/types.ts:235 | `ClientQueryRecord` schema/types.rs:375 | ✅ exact |
| `customQueryRecordSchema` | schema/types.ts:237 | — | 🟥 UNRESOLVED |
| `CustomQueryRecord` | schema/types.ts:243 | `CustomQueryRecord` schema/types.rs:385 | ✅ exact |
| `queryRecordSchema` | schema/types.ts:245 | — | 🟥 UNRESOLVED |
| `QueryRecord` | schema/types.ts:251 | `QueryRecord` schema/types.rs:346 | ✅ exact |
| `rowIDSchema` | schema/types.ts:253 | — | 🟥 UNRESOLVED |
| `RowID` | schema/types.ts:259 | `RowID` schema/types.rs:479 | ✅ exact |
| `rowRecordSchema` | schema/types.ts:261 | — | 🟥 UNRESOLVED |
| `RowRecord` | schema/types.ts:269 | `RowRecord` schema/types.rs:317 | ✅ exact |
| `patchSchema` | schema/types.ts:271 | — | 🟥 UNRESOLVED |
| `putRowPatchSchema` | schema/types.ts:276 | — | 🟥 UNRESOLVED |
| `delRowPatchSchema` | schema/types.ts:285 | — | 🟥 UNRESOLVED |
| `DelRowPatch` | schema/types.ts:291 | — | 🟥 UNRESOLVED |
| `rowPatchSchema` | schema/types.ts:293 | — | 🟥 UNRESOLVED |
| `queryPatchSchema` | schema/types.ts:297 | — | 🟥 UNRESOLVED |
| `QueryPatch` | schema/types.ts:303 | `QueryPatch` schema/types.rs:459 | ✅ exact |
| `PutQueryPatch` | schema/types.ts:305 | — | 🟥 UNRESOLVED |
| `DelQueryPatch` | schema/types.ts:306 | — | 🟥 UNRESOLVED |
| `metadataPatchSchema` | schema/types.ts:308 | — | 🟥 UNRESOLVED |
| `MetadataPatch` | schema/types.ts:310 | — | 🟥 UNRESOLVED |
| `versionString` | schema/types.ts:312 | `version_string` schema/types.rs:122 | ✅ exact |
| `versionFromString` | schema/types.ts:322 | `version_from_string` schema/types.rs:225 | ✅ exact |
| `queryRecordToQueryRow` | schema/types.ts:342 | `query_record_to_query_row` schema/types.rs:488 | ✅ exact |
| `maybeVersionString` | schema/types.ts:392 | `maybe_version_string` schema/types.rs:189 | ✅ exact |
| `stringCompare` | shared/string-compare.ts:1 | `string_compare` shared/string_compare.rs:15 | ✅ exact |
| `TTLClock` | ttl-clock.ts:5 | `TTLClock` ttl_clock.rs:5 | ✅ exact |
| `ttlClockSchema` | ttl-clock.ts:7 | — | 🟥 UNRESOLVED |
| `ttlClockAsNumber` | ttl-clock.ts:9 | IDENTITY | 📌 TTLClock = i64 (ttl_clock.rs); no conversion |
| `ttlClockFromNumber` | ttl-clock.ts:13 | IDENTITY | 📌 TTLClock = i64 (ttl_clock.rs); no conversion |
