# TS ↔ Rust behavioral parity sweep — findings

Human fn-by-fn read of each 1:1-named TS↔rust file pair, hunting behavioral divergences
the differential-fixture harness can miss (Layer-2 "COVERED" = reachable, ≠ every-branch).
Complements `COVERAGE-*.md` (reachability) and `MAP-*.md` (structural 1:1).

Legend: ✅ equivalent · ⚠️ divergence-but-benign (traced to consumer) · 🟥 real divergence (fix) · ⏭️ trivial/skipped

**Fixed so far (all with fails-on-old regression tests, full crate 136/136 + clippy clean):**
1. `received()` null-vs-absent collapse — resurrected retracted refCounts across batches (`{qA,qB}`→`{qB}`).
2. `delete_queries()` fabricated a `clientState` entry when inactivating a desired-but-never-transformed query (TS guards it behind `if clientState !== undefined`).
3. `get_inactive_queries()` tie-break made an explicit total order (expire, then hash) — TS's own order is undefined (no ORDER BY on the query load), so this is the correct deterministic resolution.

---

## Crate: rust-cvr

### `cvr.ts` ↔ `cvr.rs`

| fn | verdict | notes |
|---|---|---|
| `mergeRefCounts` | ✅ | existing/received accumulation, removeHashes filters existing-only, zero-retention on the `!existing` raw copy, positive-check→null all match. BTreeMap vs JS-object key order is canonicalized downstream (refCounts compared as a map). |
| `newQueryRecord` | ✅ | ast-branch asserts name&args absent; else expects both. Same messages ("Cannot provide name or args with ast" / "Must provide name and args"). |
| `getMutationResultsQuery` | ✅ | identical internal AST (table `<schema>.mutations`, clientGroupID `=` filter, 3-key orderBy). |
| `assertNotInternal` | ✅ | panics "Query ID {id} is reserved for internal use". |
| `getInactiveQueries` | ⚠️→✅ FIXED | Tie-break order differed on equal `(inactivatedAt+ttl)`. Investigated: TS's `cvr.queries` load has **no ORDER BY** (cvr-store.ts:361), so TS's insertion-order tie-break is arbitrary PG heap order — no stable contract to match. Made Rust an explicit total order `(expire, then hash)`; robust + deterministic. Non-observable regardless (both consumers order-independent). |
| `nextEvictionTime` | ✅ | min of `(inactivatedAt+ttl)`; order-independent. |
| `putDesiredQueries` | ✅ | needed-set computation (new / reactivated / TTL-bump), sorted union for desiredQueryIDs, input-order emit w/ dedupe, clampTTL, PutQuery + PutDesiredQuery(deleted=false), query-put patch — all match. Telemetry `recordQuery` is a metrics side-effect (documented n/a). |
| `#deleteQueries` (`delete_queries`) | 🟥→✅ FIXED | **Fabricated clientState on inactivate.** Rust inserted `clientState[client_id]` unconditionally; TS (cvr.ts:463-476) only assigns inside `if (clientState !== undefined)`. For a desired-but-never-transformed query this created an in-memory entry TS never makes (skewing intra-pass getInactiveQueries/nextEvictionTime; DB desires-row write was already identical). Fixed to insert only when the entry pre-exists. Delete branch (`cs.remove`), intersection→remove, sorted-difference desiredQueryIDs, `deleted=true` for both delete+inactivate — all match. |
| `markDesiredQueriesAsInactive` / `deleteDesiredQueries` / `clearDesiredQueries` | ✅ | thin wrappers over `delete_queries` with `Some(ttlClock)` / `None` / `(desiredQueryIDs, None)` — match TS. |

| `received` | 🟥→✅ FIXED | **null-vs-absent collapse (reachable correctness bug).** cvr.rs:1008-1018 flattens `received_rows.get()` with `.and_then(\|o\|o.clone())` *before* branching, so a present-but-**null** entry (row merged to null in an earlier batch) is treated as **absent**. TS (cvr.ts:854) branches on `previouslyReceived !== undefined` → null takes the `mergeRefCounts(null, refCounts)` path (raw received, no existing, no removed-filter); Rust wrongly takes the `existing+filter` path. Cross-batch (a row shared across queries, collapsed then re-received) → divergent persisted refCounts + divergent client patch (TS `put` vs Rust `del`) = client-visible missing/extra row. Reachable: `received()` is called per-batch (view-syncer `processBatch` loop / change_processor flush), so `received_rows` accumulates nulls across a pass. Author handled the same distinction correctly in `delete_unreferenced_rows` (is_some_and rc.is_some) but missed it here. **Fix:** branch the merge on `received_rows.get(id_str)` *presence* (match `Some(prev_opt) => merge_ref_counts(prev_opt.as_ref(), …, None)`), keep the flattened value only for the line-1066 truthy check. Needs a targeted cross-batch differential test. |
| `deleteUnreferencedRows` / `#deleteUnreferencedRow` | ✅ | Correctly ported. Truthy `receivedRows` check matches (null≡absent here). Extra `references_relevant` early-skip (cvr.rs:1159) is a documented O(all-rows)→O(query-rows) optimization equivalent to TS's `#lookupRowsForExecutedAndRemovedQueries` pre-filter; the merge on a non-referencing row is an identity, so skipping is behavior-identical. Dedupe/`maxVersion`/patchVersion logic matches. |

| `trackQueries` / `#trackExecuted` / `#trackRemoved` | ✅ | executed-then-removed order, transformationHash-change guard, desired→gotten `patchVersion` set + query-put patch, `MarkQueryAsDeleted` sequencing (remove→bump→op) all match. The `#lookupRowsForExecutedAndRemovedQueries` prefilter is done caller-side in Rust via `delete_unreferenced_rows`'s `references_relevant` — architecturally different, behaviorally matched. |
| `ensureClient` | ✅ | The `_ensureNewVersion()`-before-`insertClient` order flips in Rust, but neither the InsertClient payload nor the version bump (no store-op) is order-sensitive → non-observable. Bare-`simple` lmids `where` AST preserved (vs `and`-wrapped mutation-results); both internal queries created once. |
| `setClientSchema` | ✅ | null→set+PutInstance; equal→noop; mismatch→error "Provided schema does not match previous schema" (Rust `Err(String)` mapped by caller to TS's `ProtocolError{InvalidConnectionRequest}`). |
| `setProfileID` | ✅ | change-guard + `!startsWith("cg")` warn (warn omitted, non-behavioral) + PutInstance. |
| `deleteClient` | ✅ | markInactive(desiredQueryIDs) → remove client → DeleteClient op, same order; empty-return when client absent. |
| `flush` (row-set-signature persist) | ✅ | provider-None→skip, `stored==sig`→skip, format+persist+`UpdateRowSetSignature` match. Rust's extra `record_row_set_signature_drift()` is telemetry-only (documented n/a). Iteration order is order-independent. |

**`cvr.ts ↔ cvr.rs`: SWEEP COMPLETE** — 3 fixed (2 real correctness + 1 hardening), ~20 ✅ equivalent, 0 open.

### `cvr-store.ts` ↔ `cvr_store.rs` (in progress)

| fn | verdict | notes |
|---|---|---|
| `asQuery` (`as_query`) | 🟥→✅ FIXED | **Different discriminator.** TS keys on `clientAST === null → custom` (then `internal ? internal : client`); Rust checked `internal` first and keyed custom off `query_name`. Agrees for all 3 schema-valid shapes, but a corrupt custom row (clientAST null, queryName null) silently became a **null-AST client query** in Rust vs TS's recoverable `assert(queryName && queryArgs)` throw. Reordered Rust to TS's discriminator + added `VersionError::MalformedQuery` (recoverable, `?`→`CVRStoreError::VersionParse`). Test `test_as_query_null_ast_missing_name_is_error`. |

| `put_desired_query` | 🟥→✅ FIXED (defensive) | TS `convertTTLValues`: `ttlMs = ttl < 0 ? null : ttl` — a negative ("forever") TTL persists as SQL NULL; Rust stored `Some(ttl)`. Unreachable today (`clamp_ttl` maps -1→MAX before callers), but a real function-contract gap; aligned defensively (`ttl < 0 → None`), a no-op for all `ttl >= 0`. Key/last-wins dedup (`client:query`) matches. |
| `load` / `load_once` | ✅ | CVR reconstruction matches: clientState set-condition `NOT(deleted && inactivatedAt===null)`, internal-skip via `client_state_mut()==None`, inactivatedAt/ttl/version. Rust `sort()+dedup()`s desiredQueryIDs vs TS's deliberate DB-order (cvr-store.ts:515) — benign determinism gain (downstream Set-consumed/re-sorted; matching TS would be strictly worse). Ownership grant, rows-behind detection, new-CVR putInstance match. |
| `catchupConfigPatches` | ✅ | Range `patchVersion > start AND <= end`, emit order queries→desires, Put/Del-on-deleted + clientID presence, `checkVersion`→`ConcurrentModification` all match. `ORDER BY patchVersion` (each entity appears once → non-observable) and `continue`-vs-`assert` on null patchVersion (unreachable via WHERE `> start`) both benign. |
| `apply_store_ops` | ✅ | 1:1 StoreOp→pending-write dispatch (Rust-internal replay of TS's inline cvrStore.* calls). |
| `put_query`/`update_query`/`mark_query_as_deleted`/`update_row_set_signature` | ✅ | full vs partial (`pending_query_partial_updates`) split matches TS; nested `Option<Option<>>` preserves undefined-vs-null clears for internal/untransformed queries. |
| `put_instance`/`insert_client`/`delete_client`/`put_row_record`/`del_row_record`/`force_updates` | ✅ | simple keyed pending buffers. |
| `flush` (530-line async) | ⚙️ IO | covered by the dedicated flush-PG differential fixture (flush-fixture.json); materiality guard + partial/full merge + ownership check verified structurally, not line-by-line. |

**`cvr-store.ts ↔ cvr_store.rs`: SWEEP COMPLETE** — 2 fixed (1 real + 1 defensive), ~13 ✅/⚙️, 0 open.

### Pure helper files

| file / fn | verdict | notes |
|---|---|---|
| `row_key.rs` — `normalized_key_order`, `row_id_string`, `row_id_string_cached`, `row_id_hash` | ✅ | lexicographic key sort; streamed rowIDString byte-identical to `Value::Array` form (validated parity_check.rs); bounded cache output-transparent vs TS WeakMap. |
| `hash.rs` — `h128`/`h64`/`h32`/`xxh32_seeded` | ✅ | canonical xxHash32 (`xxHash32("",0)=0x02cc5d05`); h128 = 4× seeded concat; pinned to TS goldens. |
| `row_key.rs` — `base36_encode` | ✅ | `0`→"0", `0-9a-z` = JS `toString(36)`. |
| `ttl.rs` — `parse_ttl`/`compare_ttl`/`clamp_ttl` | ✅ | compare/clamp match exactly; TS `NaN→0`/`!Finite→-1` structurally impossible in Rust `TTL::Ms(i64)`; unit multipliers match (incl. `y=365d`). |
| `ttl.rs` — `parse_ttl_string` | 🟥→✅ FIXED (defensive) | parsed numeric part as `i64` → fractional `"1.5h"` became `0`; TS `Number()` parses floats (5_400_000ms). Parity-harness/test-only entry (TTL reaches CVR pre-parsed), aligned to TS float semantics anyway. Tests for `"1.5h"`, `"0.5s"`. |

| `change_processor.rs` (`on_row_change`/`flush_batch`/`finish`) | ✅ | refCount deltas (ADD `+1`, EDIT ensure-key-no-change, REMOVE `-1`, no updateVersion on REMOVE), `_0_version` strip (contentsAndVersion), and **flush boundary** (`rows.len() % 10000` = TS `rows.size % CURSOR_PAGE_SIZE`, incl. dedup behavior) all match — batch boundaries identical, so the cross-batch `received()` state (post-fix) stays in parity. |
| `schema/cvr.rs` (`rows_row_to_row_record`/`row_record_to_rows_row`) | ✅ | refCounts `null↔None`, `{}↔Some(empty)`, `Object↔map`; row_key object-guard; version conversions all match. |
| `row_set_signature.rs`, `schema/types.rs` (version fns), `row_record_cache.rs` | ✅/⚙️ | format/parse + version fns pinned to TS goldens (schema/types already caught+fixed the `version_from_string` stateVersion bug); row_record_cache is async IO covered by the flush/catchup PG differentials. |

## ✅ rust-cvr: SWEEP COMPLETE — **6 divergences fixed**, ~45 functions verified equivalent, 137/137 tests + clippy clean, rust-syncer rebuilds clean.

Fixes: (1) `received()` null-vs-absent, (2) `delete_queries()` fabricated clientState, (3) `get_inactive_queries()` tie-break, (4) `as_query()` discriminator, (5) `put_desired_query()` negative-ttl, (6) `parse_ttl_string()` float parse. Recurring root cause: Rust collapsing a null/absent/empty tri-state that TS branches on precisely.

---

## Crate: rust-ivm

**Coverage note:** rust-ivm is the *standing IVM oracle* — its operators are differentially validated against TS output on fuzzed real ASTs (stronger than rust-cvr's fixed-fixture coverage, which is why rust-cvr surfaced 6 branch-divergences and rust-ivm's residual risk is low). Sweep prioritizes highest-divergence-risk operators.

| operator | verdict | notes |
|---|---|---|
| `exists.rs` (`push`, `fetch_size`) | ✅ | push branches match TS exactly (ADD/EDIT/REMOVE→filter; CHILD-add `size==1` flip w/ emptied rel; CHILD-remove `size==0` flip w/ removed-child included; NOT handling; else→filter(size>0)). `fetch_size` **fully drains** (`.count()`, no short-circuit — the reverted [[rust-exists-no-shortcircuit-invariant]] bug is absent). TS size-memoization vs Rust always-fresh yields identical values (oracle-validated). |
| `take.rs` (`fetch`) | ✅ | both branches match: constrained-to-partition (bound `<`row→stop, hidden-row skip) and unconstrained/max-bound (row`>`maxBound→stop, per-partition `bound>=row`→yield). Comparison directions all correct. Push boundary handlers (add/remove/child/edit) are oracle-covered. |

| `join.rs` (`push_parent`/`push_child`/`push_child_change`/`process_parent_node`) | ✅ | add/remove/child/edit parent push (w/ "must not change relationship" invariant assert), child-change inprogress-overlay + parent-fetch + per-parent push, child-stream constraint fetch + overlay when `matches && parent > position` (unordered-when-no-sort) — all match `join.ts`. |
| `filter.rs` + `filter_push.rs` | ✅ | fetch predicate-filters Data (keeps Yield); Edit-transition table (both→edit, old-only→remove(old), new-only→add(new), neither→drop) matches `filter-push.ts`. |
| `skip.rs` (`fetch`/push) | ✅ | `#getStart` forward/reverse + req.start interaction (w/ documented overlay-start parity fix); push Edit-transition table matches. |
| `cap.rs` (`push`: ADD/REMOVE/CHILD/EDIT) | ✅ (1 minor) | ADD (size<limit→append/push, else drop), REMOVE (`-1`→drop, splice, refill first-not-in-set, hide-during-forward + add-replacement), CHILD (`pkSet.has(pk)`→forward), EDIT (update PK if changed→push, else drop) all match `cap.ts`. **Minor:** rust omits TS's `assert(partitionKey unchanged)` in the Edit path (join/take have their equivalents) — guards an unreachable path (source splits key-changing edits), benign. |

| `fan_out.rs` | ✅→ +assert | push-to-all-outputs then signal fan-in matches `fan-out.ts`. **Added** TS's `must(fanIn, 'fan-out must have a corresponding fan-in set!')` (was `if let Some`). |
| `fan_in.rs` | ✅→ +assert | accumulate-on-push + collapse-on-done matches `fan-in.ts`. **Added** TS's no-inputs invariant: `if inputs empty, assert accumulated empty` ("If there are no inputs then fan-in should not receive any pushes."). |
| `cap.rs` (Edit assert) | +assert | **Added** the previously-noted TS `assert(partitionKey unchanged, 'Unexpected change of partition key')` to the Edit path — verified rust-syncer contains IVM panics per-CG (`pipeline_driver.rs:424/488` + `router.rs:3313` `catch_unwind`, unwind not abort → pipeline reset, matching TS's assert-throw→reset). |

| `union_fan_out.rs` | ✅→ +assert | mirrors fan_out + `fan_out_started_pushing()`; **added** TS's `must(unionFanIn)` for both started+done. |
| `union_fan_in.rs` (`fetch`/`mergeFetches`) | ✅ | fetch = k-way merge + **dedup consecutive-equal** (union, not union-all) — matches TS `mergeFetches` (`lastNodeYielded` + `comparator===0`→skip). **Structural diff (not fixable):** TS constructor asserts branch-schema consistency incl. `compareRows === inputSchema.compareRows` (function-reference equality) which Rust's `Arc<dyn Fn>` cannot express; the builder guarantees consistency. |
| `push_accumulated.rs` (`push_accumulated_changes`/`merge_relationships`) | ✅ | per-type collapse + per-fan-out-type resolution (Remove/Add single-type; Edit merge-or-synthesize-edit(add=new,remove=old); Child child-wins or add-xor-remove). Assert messages copied verbatim from TS. Oracle-covered. |

| `flipped_join.rs` (`push_child_change`) | ✅ | EXISTS-flip: per child-change fetch matching parents; `exists` seeded by Edit\|Child, set true if parent has *another* child (via CHILD-schema comparator — documented parity fix vs using the parent comparator); build child rel stream; push. Careful port, oracle-covered. |

**4 invariant asserts added** (fan_out, fan_in, cap, union_fan_out) — rust-ivm 69/69 + clippy clean.

### ✅ rust-ivm core dataflow: ALL 12 operators verified two-sided
exists, take, join, **flipped_join**, filter, skip, cap, fan_out, fan_in, union_fan_out, union_fan_in, push_accumulated — every operator with divergence-prone push/fetch logic. 0 behavioral divergences (the port is faithful — matching assert messages, documented parity fixes); 4 invariant asserts added to match TS's fail-loud.

| `source.rs` (`push_internal`) | ✅ | split-edit (key-changing Edit→Remove+Add, prevents Join panics), source-drift dev-assertions (Add-dup/Remove-missing/Edit-missing-old→panic = TS memory-source assertions), epoch+overlay for re-entrant fetch, per-connection push — match the reference. Oracle-covered. |

### ✅ rust-ivm behavioral-divergence surface: COMPLETE
All 12 dataflow operators + the source push path verified two-sided. **0 behavioral divergences** (faithful port — verbatim assert messages, documented parity fixes); **4 invariant asserts added** to match TS's fail-loud. Contrast with rust-cvr's 6 divergences: rust-ivm has the standing differential oracle (fuzzed real ASTs), rust-cvr had weaker fixed fixtures — the sweep confirms the oracle's strength.

**Remaining rust-ivm (oracle-validated plumbing/translation, low risk):** `source.rs` fetch/overlay internals, `view.rs`, `builder/*` + `planner/*` (AST→graph), data types (`change`/`data`/`schema`/`stream`/`constraint`/`node_filter`/`memory_storage`), debug helpers (`catch`/`snitch`/`array_view`/`credit`/`advance_gate`/`join_utils`/`stopable_iterator`).

## Crate: rust-syncer

Much of rust-syncer is rust-specific orchestration (event loop, WS, workers, metrics) with no direct TS twin. The function-by-function comparison applies to the TS-mirrored subset: `auth/read_authorizer.rs`, `custom_queries/transform_query.rs`, `services/view_syncer/*`.

| fn / file | verdict | notes |
|---|---|---|
| `read_authorizer.rs` (`resolve_permissions`, `transform_query_internal`, `add_rules_to_where`, `transform_condition`) | ✅ (security-verified; one verdict CORRECTED 2026-08-28) | Deny-by-default (no `row.select` rules → `[['allow', {or:[]}]]` always-false → 0 rows) matches TS exactly; rule application `where AND (OR allow-conds)` + recursive EXISTS/subquery transform match; `resolve_permissions` fail-CLOSED (`Err→deny_all`, safer than TS throw). **CORRECTION (2026-08-28): the original "`Ok(None)→pass-through` = TS intended warn+serve" verdict was WRONG — it audited `loadPermissions` but missed the CONSUMER: TS view-syncer.ts:1549/:1929 transform with `permissions ?? {tables: {}}`, so a null doc DENIES every client AST query (empty config → deny-by-default per table). Rust's `None→untransformed` branch in `sync_engine.rs` was a fail-OPEN data leak, caught live by ART G8 via the #158 channels rider (TS 0 rows, rust full table) and fixed by substituting the empty config at the same call site (regression test `pg_no_permissions_deployed_denies_client_ast_queries`, proven failing pre-fix). Lesson: audit the USE site, not just the load site — the L4 snapshot-freshness rule generalizes to "follow the value to consumption".** hash_of_ast/normalize_ast oracle-validated (result parity would break if the transform diverged). |

_Remaining rust-syncer TS-twinned: `transform_query.rs` (custom-query transform), `services/view_syncer/*` (connection_context_manager, pipeline_driver, query_covering, e2e_serving_lag) — the CVR-integration flow (sync_engine hydrate_and_sync) where rust-cvr-style tri-state bugs could recur._

### `transform-query.ts` ↔ `custom_queries/transform_query.rs` (2026-08-26)

| fn | verdict | notes |
|---|---|---|
| `transform` (`transform_custom_queries`) | ✅ | cache split, empty-batch short-circuit (`to_fetch.is_empty()` = TS `request.length===0`), per-query `'error' in q` (present-**null** counts as error on both — `q.get("error").is_some()` ≡ JS `in`), error-responses-not-cached (`continue` before `cache_set`), `hashOfAST` on the returned AST — all match. |
| result **order** | ⚠️→benign | TS returns `[...newResponses, ...cachedResponses]` (new-first); Rust pushes cached-first then fetched (`[cached, new]`). **Non-observable:** BOTH consumers key by `q.id` (TS view-syncer.ts:1538 `customQueries.get(q.id)`; rust sync_engine.rs:810 per-qid `executed` loop) — never index-positional. |
| missing-`ast` entry | ⚠️ divergence (documented, low-reach) | TS validates the whole response against `queryResponseSchema` inside `fetchFromAPIServer` → a non-error entry missing `ast` fails the **whole batch** (throw→`TransformFailed`, `result` non-array, consumer skips + retries all later). Rust has no response-schema validation (dynamic `serde_json::Value`) and degrades just that entry to a per-query `Errored`. Triggers only on a malformed API-server response (server bug); Rust is *more lenient* (keeps healthy siblings). Flagged not fixed — a faithful fix means porting `queryResponseSchema`; the divergence is unreachable with a spec-conformant server. |
| `validate` (`validate_custom_queries`) | ✅ | forces the empty `["transform", []]` POST (does NOT short-circuit like `transform`), opaque `Ok(())`; 401 → reason-http body classified auth-error. Stub-server tested. |
| `#requestTransform` `validation` thread | ⚠️ architectural | TS `#requestTransform` returns `ConnectionValidation` (`server-validated{userID}` vs `client-fallback`) consumed by `validateConnection` to re-pin the group userID. Rust's `transform_custom_queries` drops it; connection (re)validation is handled by the separate router.rs auth-revalidation path (jwt re-verify), not by threading the server's authoritative userID back from the transform response. Behaviorally equivalent for JWT auth (userID pinned from the token); a deployment that relies on the API server *rewriting* userID mid-session would diverge. Recorded as an open architectural item, not a quick fix. |
| `getCacheKey` (`get_cache_key`+`normalized_headers`) | ✅ | key encodes url+auth+userID+composed-headers-digest+id — TS includes token+cookie+origin+userID+customHeaders. `composed_headers` overwrite precedence (api-key→client→forwarded→Auth→Cookie→Origin) tested; process-wide cache with expiry-sweep is a labelled Rust-specific reclaim (TS per-connection `TimedCache`). |
| `urlMatch` (`url_match`) | ✅ (security) | real WHATWG `URLPattern` (urlpattern crate) — `url-match-fixture.json` differential vs native TS `URLPattern`; component-boundary-safe (F-FETCH-1 allowlist-bypass regression pinned). |
| `fetchFromAPIServer` (`post_transform*`) | ✅ | 4-attempt retry, 5xx/network retry with `min(1000, 100·2^(a-1)+jitter)` backoff (bounds test), 4xx immediate-fail, reserved-param guard, `?schema&appID` append, real-batch `queryIDs` on failure body (F-TQ-1 test) — all match. Timeout is a labelled Rust-specific addition (reqwest has no default; TS relies on undici's 300s). |
| `isAuthErrorBody` (`is_auth_error_body`) | ✅ | `{error:http,status:401\|403}` / `{kind:AuthInvalidated\|Unauthorized}` / `{kind:TransformFailed\|PushFailed,reason:http,status:401\|403}` — full table tested. |

**`transform-query.ts ↔ transform_query.rs`: SWEEP COMPLETE** — 0 real correctness divergences; 1 benign order diff (consumer-keyed), 1 documented low-reach leniency (missing-ast), 1 open architectural note (validation/userID thread). Strong existing test coverage incl. TS-golden url-match differential.

## Crate: rust-syncer — services/view_syncer/* (in progress)

### `pipeline-driver.ts` ↔ `pipeline_driver.rs` + `rust-ivm/advance_gate.rs` (2026-08-26)

`pipeline_driver.rs` is a *behavioral* bridge (engine-side of pipeline-driver.ts + the parity-tested napi `EngineState`); the row-streaming + operator logic lives in rust-ivm's `Engine` (already swept — 0 divergences). The divergence-prone piece unique to pipeline-driver.ts is the **smart load-shedding / advancement-timeout reset**, ported to `rust-ivm/advance_gate.rs`.

| item | verdict | notes |
|---|---|---|
| load-shed **constants** (`MIN_ADVANCEMENT_TIME_LIMIT_MS`, `MIN/MAX_PROJECTED_ADVANCEMENT_SAMPLE_CHANGES`, `PROJECTED_ADVANCEMENT_SAMPLE_FRACTION`, `MIN_PROJECTED_ADVANCEMENT_SAMPLE_MS`, `MIN_PROJECTED_ADVANCEMENT_CHANGES`, `PROJECTED_ADVANCEMENT_RESET_MULTIPLIER`, `LATE_ADVANCEMENT_FINISH_PROGRESS`) | ✅ | all 8 byte-identical to TS pipeline-driver.ts:167-174 (verified line-by-line). |
| `projectedAdvancementTimeMs` / `advancementResetTimeLimitMs` / `minProjectedAdvancementSampleChanges` / `shouldResetProjectedAdvancement` / `shouldFinishLateAdvancement` / `shouldResetSlowCurrentChange` | ✅ | each ported 1:1 (same name, same guards, same `>` vs `>=`). |
| `#shouldAdvanceYieldMaybeAbortAdvance` arm composition (`advance_reset`) | ✅ | arm order + gating identical: (1) slow-current-change always resets, (2) projected `&& !shouldFinish`, (3) economic timeout `!shouldFinish && elapsed>MIN && (elapsed>budget \|\| (elapsed>budget/2 && pos<=num/2))`. `budget` = `totalHydrationTimeMs`. **This is the B5/#145 "advancement-timeout" reset — a faithful 1:1 port, NOT a Rust-specific stall.** |
| `ADVANCE_WALL_CLOCK_CEILING_MS` (arm 0, 60s) | Rust-only (RULE #5, labeled) | Rust `exclude`s downstream-delivery time from the economic clock (TS doesn't pause its timer), so a slow consumer could hold the WAL snapshot indefinitely — the very resource the reset bounds. An exclusion-free absolute ceiling restores TS's implicit bound. Fires only at 60s (past every TS arm) → never changes TS-covered behavior. |
| per-row `should_stop_fetch` thread-local gate | Rust-only (RULE #5, labeled) | Rust IVM push is infallible (`Vec<Change>`, not `Result`) so it can't throw `ResetPipelinesSignal` mid-fetch like TS's `#shouldYield()` callback; a thread-local gate armed for the advance stops the row-read loop instead. RAII `GateGuard` clears on unwind so a later hydrate can't inherit a stale budget. |
| lifecycle (`init`/`hydrate`/`advance`/`destroy`, poison→Reset, scalar-subquery→Reset, panic classification) | ✅ | `scalar_reset_message` classifies only `ScalarResetError` → `scalar-subquery` reset; other panics poison→next-advance `schema-change` reset (TS teardown parity). Field-drop order is a labeled Rust-specific G6-leak fix. |
| AST conversion (`parse_ts_ast`/`convert_*`/`json_to_value`) | ✅ | ported verbatim from the parity-tested napi path; `static` value-position → `Null` and out-of-safe-range int → `f64` both match napi (labeled: never panic on client literals). |

**`pipeline-driver.ts ↔ pipeline_driver.rs`: SWEEP COMPLETE** — 0 behavioral divergences; the advancement-timeout reset is a faithful port with 1:1 constants (answers B5/#145 code-side); 2 labeled Rust-specific additions (wall-clock ceiling, per-row gate).

### `connection-context-manager.ts` ↔ `connection_context_manager.rs` (2026-08-26)

**Status: NOT WIRED — a tested reference impl** (production auth is the simplified per-CG `CgState` + router.rs revalidate/retransform tick). Parity still matters for a future promotion.

| fn | verdict | notes |
|---|---|---|
| `resolveAuth` (`resolve_auth`) | ✅ (security-verified) | line-by-line: no-token+prev→Unauthorized, no-token→None, token+null-userID→Unauthorized, legacy-validator→`pick_token`, prev-jwt→"cannot change JWT→opaque", prev-opaque+raw==wire→reuse, else new opaque — all exact. |
| `pickToken` (`pick_token`) | ✅ (security-verified) | prev-None→new; type-mismatch→"Token type cannot change" (Rust collapses TS's *dead* step-5 "opaque→JWT" message — unreachable in TS too, caught by the generic type-mismatch throw first); sub-mismatch→pinned-user error; iat tri-state (prev-None→new / new-None→error / `new_iat>prev_iat`→new else keep-prev) all match incl. the `<` vs `>` direction. |
| `validateConnection` | ✅ | stale-revision→None, server-validated userID match→Unauthorized-on-mismatch, pinned-user pin-on-first + agree-or-throw, revalidate-at refresh, background refresh — all match. |
| `updateAuth` 3-way branch | ⚠️→benign | TS's middle arm is `nextAuth === connection.auth` (JS **reference** equality — "did resolveAuth hand back the exact same object, skip store"); Rust uses `!=` **value** equality (`Auth: PartialEq`). Non-observable: `store_connection` is an idempotent map re-insert and the returned auth is `auth_equals`-equal either way. Documented — a Rust-can't-express-JS-ref-equality case. |
| `planMaintenance` | ✅ | `maintenanceNotBeforeAt` deferral returns empty + `max(earliest, notBefore)` ONLY when `notBefore>now && earliest.is_some()`; due-revalidations sorted ascending. Matches. |
| `compareByInsertionOrder` / `comparePreferredValidatedConnection` | ✅ | opposite directions preserved: due = **ascending** (`a-b`, ws_id asc); preferred-background = **descending** (`b-a`, ws_id desc). |
| `refreshBackgroundConnectionContext` / `updateBackgroundRetransformDeadline` | ✅ | sticky-background (preferred promoted only when no current validated bg), reset-vs-seed deadline semantics match. |
| `filterHeaders` / `build_fetch_context` | ⚠️ architectural (labeled) | outgoing request-header filtering (incl. #6144 forwarding) is done by router.rs; this port keeps the pre-#6144 `allowed_client_headers` shape and is not on the runtime fetch path (labeled in-file). |
| `to_error_body` | ✅ | Layer-2 differential vs TS `ErrorKind`/`ErrorOrigin` (`error-body-fixture.json`). |
| — | cleanup | removed a stale dangling comment ("Fix the pick_token function… simplifying here") that referred to no code and misdescribed the (correct) `pick_token`. |

### `drain-coordinator.ts` ↔ `drain_coordinator.rs` (2026-08-26) — WIRED (router.rs SIGTERM drain)

| item | verdict | notes |
|---|---|---|
| `TARGET_UTILIZATION=0.6`, `FORCE_DRAIN_PADDING=2` | ✅ | constants match. |
| `shouldDrain`/`drainNextIn`/`forceDrainTimeout`/`draining` | ✅ | interval `/0.6`, `nextDrainTime=now+adjusted`, force deadline `+PADDING`, coalescing push-forward — all match. `assert`→`debug_assert` (labeled: router upholds it by construction, avoids a prod panic the original port declined) and deadline-atomics-vs-`Notify` (labeled: can't lose a wakeup) are Rust-specific. |

### `e2e-serving-lag.ts` ↔ `e2e_serving_lag.rs` (2026-08-26) — WIRED (#6157/#6312)

| item | verdict | notes |
|---|---|---|
| `onVersionReady`/`onVersionServed` | ✅ (differential-tested) | both-fields-required guard, coalesce-keeps-**oldest**-commit-time + **newest**-watermark, `servedVersion < watermark`→None replay-guard (LexiVersion lexicographic==numeric), negative-lag clamp-to-0 + `clamped=true` flag — exact 1:1, pinned by a Layer-2 differential against the real TS tracker (`e2e-serving-lag-fixture.json`). |

### `query-covering.ts` ↔ `query_covering.rs` (2026-08-26) — SHADOW-MODE (logging only)

| fn | verdict | notes |
|---|---|---|
| `conditionImplies` | ✅ | branch order identical: covering-None→true, covered-None→false, json-equal→true, covered-`or`→all, covering-`or`→some, covering-`and`→all, covered-`and`→some, both-simple→simple, both-correlated→correlated, else false. |
| `simpleConditionImplies` / `equalityImplies` / `orderConditionImplies` | ✅ | full switch parity. `order_condition_implies` collapses TS's `>`/`<` two-branch disjunction into one `>=`/`<=` test (documented — both TS branches share the same threshold, semantically identical); `>=`/`<=` kept as-is. `equality_implies` NOT-IN/LIKE-family → false; `cmp_num`/`num` enforce TS's `typeof === 'number'` guard (booleans & strings → false, verified). |
| `boundsCoveredBy` | ✅ | covering-no-limit→(no-start→true / else start+orderBy eq); covered-no-limit or `covering.limit < covered.limit`→false; else `conditionEquivalent(where) && start eq && orderBy eq`. |
| `correlatedConditionImplies` | ✅ | op/scalar/edge match then EXISTS→covered⊆covering, NOT-EXISTS→**reversed** covering⊆covered. |
| `sameRelatedEdge` / `relatedCoveredBy` / `astCoveredBy` / `rootKey` / index add/remove/find | ✅ | correlation/hidden/system/alias edge match; every-covered-∈-some-covering; root-key bucketing + skip-self in find. |
| tri-state helpers (`present`/`json_eq`/`num`) | ✅ | `present` maps `Value::Null`→absent; `json_eq` treats both-absent as equal, present-vs-absent as unequal (matches TS `deepEqual(undefined, undefined)`); `num` rejects booleans. |

**`query-covering.ts ↔ query_covering.rs`: SWEEP COMPLETE** — 0 divergences (faithful, with a documented semantically-identical `order` collapse).

---

## ✅ rust-syncer TS-twinned subset: SWEEP COMPLETE (2026-08-26)

All TS-mirrored rust-syncer files swept: `transform_query.rs`, `pipeline_driver.rs` + `advance_gate.rs`, `connection_context_manager.rs`, `drain_coordinator.rs`, `e2e_serving_lag.rs`, `query_covering.rs`. **0 real correctness divergences.** Findings: 1 benign result-order (consumer-keyed), 1 documented low-reach leniency (transform missing-ast), several labeled Rust-specific additions (advance wall-clock ceiling + per-row gate, drain atomics, timeout), and 2 documented benign approximations in the not-wired connection-context-manager (ref-vs-value auth equality, header-filter architectural split). B5/#145 confirmed by-design. 1 stale comment removed. The rest of rust-syncer is Rust-specific orchestration (event loop, WS, workers, metrics) with no TS twin.
