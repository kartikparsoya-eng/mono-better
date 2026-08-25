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

**4 invariant asserts added** (fan_out, fan_in, cap, union_fan_out) — rust-ivm 69/69 + clippy clean. Verified two-sided: exists, take, join, filter, skip, cap, fan_out, fan_in, union_fan_out, union_fan_in, push_accumulated (11 modules — every OR/limit/filter/merge operator). Remaining: source/view (large, oracle-covered core), flipped_join, constraint, node_filter, memory_storage, catch, snitch, array_view, change/data/schema/stream, builder/*, planner/*._

## Crate: rust-syncer (after rust-ivm)
_pending_

## Crate: rust-syncer (after rust-ivm)
_pending_
