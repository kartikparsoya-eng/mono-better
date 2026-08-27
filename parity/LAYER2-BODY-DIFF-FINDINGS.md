# Layer-2 Body-Diff Findings — Rust ⇄ TS Parity Audit

**Method:** Side-by-side body-diff of each MAP-declared `(TS file → Rust file)` pair.
The parity MAPs (`MAP-{cvr,syncer,ivm}.md`) explicitly state: *"Bodies are not
compared; behavior drift needs Layer-2 body review."* This file IS that Layer-2 review.

**Rules of engagement:**
- Every finding cites BOTH `rust file:line` and `ts file:line`, both personally read.
- Each pair yields a SELF-KILLED list (refuted candidates) and an UNVERIFIED section.
- If a subagent diffs a pair but times out, the pair is re-diffed parent-side (no reliance on partials).
- Findings are deduped and severity-ranked at the end; refuted count reported for honesty.

**Severity scale:** High (correctness/security/data divergence) · Med (observable behavior / coverage gap) · Low (cosmetic / edge) · Doc-verify (documented intentional divergence — confirm registered as exception).

---

## VERIFICATION PASS (2026-08-25) — every finding re-checked against CURRENT code

Each finding below was independently re-read against the live tree (both Rust and TS
sides) after the rust-ivm invariant fixes (`db8e8ee7a`). Verdicts: **CONFIRMED** (still
real) · **STALE** (already fixed) · **WRONG** (misreads code) · **OVERSTATED** (true but
smaller/unreachable than the severity implies). Headline: the doc is *factually accurate*
on nearly every finding, but **systematically overstates severity** — the three
High/Critical items all deflate once production wiring is accounted for.

### The High/Critical claims all deflate
- **F-CAP-1 (Med, "silent state corruption") → STALE.** The partition-key-unchanged
  assert now exists at `cap.rs:362-369` (added in `db8e8ee7a`). Matches `cap.ts:261-268`.
- **F-RA-2 (High) → bookkeeping, not a code bug.** The merge is real (`load_permissions`,
  `reload_permissions_if_changed`, `validate_permissions_config` all live in
  `read_authorizer.rs`), but there is no behavioral gap — just a MAP/ledger label to fix.
- **F-LT-1 (High) → CONFIRMED real bug, but Med.** The only genuinely actionable
  correctness divergence found. Rust keys min-row-version map by `"{schema}.{table}"`
  (`lite_tables.rs:266`) but looks up by the bare `sqlite_master` name (`:401`), so for
  every **public-schema** table the lookup returns `None` → the `minRowVersion` re-download
  override is silently dropped → clients may keep stale rows after a table-wide schema
  change. The test `reads_unique_indexes_and_min_row_version` (`:616-620`) MASKS it by
  creating a table literally named `"public.users"`. Self-healing on next write; recovery
  path only → Med, not High. **Fix:** key the lookup by `liteTableName` (strip `public.`).
- **F-CCM-1 (Critical) → CONFIRMED but DORMANT.** The rich decoded-claims
  `connection_context_manager.rs` is explicitly NOT WIRED; `main.rs:733` installs
  `PlaceholderConnContextManager`. The live auth path is `router.rs handle_update_auth`
  (`:2499+`): raw-token `authEquals` + signature re-verify + user-pinning — correct. No
  live vulnerability.
- **F-SW-2 / F-SW-3 (Med-High "SECURITY") → CONFIRMED but NON-EXPLOITABLE.** The
  `json!({"token": raw})` / skipped-opaque-assert paths are unreachable in the shipped
  binary: `create_mutagen` returns `None` (`main.rs:740`), so CRUD pushes hit the "legacy
  CRUD disabled" **Fatal** rejection *before* auth is ever built. Structural only.

### Genuinely actionable residuals (all Low–Med)
| finding | verdict | what to do |
|---|---|---|
| **F-LT-1** | CONFIRMED (Med) | key min-row-version lookup by `liteTableName`; fix the masking test |
| **F-CP-1** | CONFIRMED (Med) | duplicate query-param: Rust HashMap last-wins vs TS first-wins (`?clientID=A&clientID=B`) — iterate `query_pairs()` keep-first, or reject dupes |
| **F-TQ-1** | CONFIRMED (Med) | `transform_failed` hardcodes `queryIDs:[]` — populate from request ids so client can attribute failures |
| **F-TQ-2** | CONFIRMED (Med, security-adjacent) | revoked-but-unexpired JWT keeps Rust conn alive (no empty-`/query` probe) — confirm local-auth model is intended, else add a revocation probe |
| **F-TQ-4** | CONFIRMED (Low-Med) | process-wide `TRANSFORM_CACHE` never evicts expired entries — add a sweep |
| **F-TQ-7** | CONFIRMED (Low) | legacy `['transformed',…]` tuple response fails the whole batch |
| **F-SW-1** | CONFIRMED (Low) | no-pusher custom push: Transient-vs-Fatal + PUSHER_URL/ZERO_MUTATE_URL text (arguably intentional) |
| **F-RA-1 / F-LP-1** | CONFIRMED (Low) | missing deny-by-default / "deploy-permissions" warn logs (no enforcement change) |
| **F-CON-2** | CONFIRMED (Low) | missing `websocket.errors` OTel counter |
| **F-CON-3** | CONFIRMED (Low) | transient socket disconnects (EPIPE/ECONNRESET) log at wrong level |
| **F-CON-5** | CONFIRMED (Low) | dead `maybe_send_pong` — delete it (keepalive correctly relocated to `ws_server.rs`) |
| **F-DC-2 / F-DC-3** | CONFIRMED (Low) | missing `drain_next_in` precondition assert; 1ms busy-poll when unarmed |

### Stale / wrong / overstated-to-nil (no action)
- **STALE / fixed:** F-CAP-1 (assert present), F-RA-3/F-LP-5 (`computeZqlSpecs` IS ported in
  `lite_tables.rs:79`), F-LP-3 (unreachable no-op cold-start difference).
- **WRONG / equivalent:** F-EX-3 (cache-clear-per-fetch ≡ TS begin/endFilter window),
  F-JOIN-1..4 (CLEAN), F-VIEW-1 (refcount add/remove/edit CLEAN — matches
  `view-apply-change.ts` 1:1), F-FO-1/F-FO-2 (dead-code stubs; live filtering via
  `Filter`/`NodeFilter` is correct), F-IVM-X1 (32 symbols are client-side DSL;
  server-reachable TTL logic IS ported).
- **OVERSTATED to ~nil:** F-CAP-2/F-EX-2/F-TAKE-1 (Debug-vs-JSON key encoding differs, but
  keys are **engine-internal + self-consistent**; NaN/Inf/-0 can't appear in SQL
  PK/partition columns — no cross-engine key sharing), F-CAP-4 (`multi_constraints` clone,
  but that input shape never reaches Cap), F-LP-2 (validators structurally equivalent, 14-op
  allowlist matches valita exactly), F-LT-4 (`IvmTableSpec` isn't `Serialize`d — no
  order-sensitive path), F-RA-4/F-LP-4 (fail-closed deny-all is SAFE, arguably safer than
  TS throw), F-JWT-1 (all 3 call sites verify signature before trusting decoded claims).
- **PATTERN-A (skip_yields, F-CAP-3/F-EX-1/F-TAKE-2) → CONFIRMED but scheduling-only.**
  Rust drops `Yield` sentinels on *push-path* internal fetches; TS propagates them. But push
  callbacks are synchronous and don't stream to the driver, so this affects cooperative
  event-loop pumping *inside a push*, not query results. Initial-hydration + cached read
  paths DO propagate yields (`cap.rs:228`, `take.rs:359/944`, `exists.rs:167-170`). Not a
  correctness bug.

**Net:** 1 real correctness bug (F-LT-1, Med), ~5 real behavioral gaps (F-CP-1, F-TQ-1/2/4/7,
all Med–Low), the rest observability/cosmetic or stale/wrong. Zero of the doc's
High/Critical/security findings are live-exploitable correctness defects in the shipped binary.

### ROUND-2 VERIFICATION (2026-08-25) — findings an agent added after the first pass

New pairs added: 21 (change_processor), 18-view (F-VIEW-2..6), 20 (router
#createConnection/drain, F-RT-1..5), plus a "Final Synthesis". Re-checked each:

- **F-SIG-1 (Final Synthesis HIGH #1) → CONFIRMED real + FIXED.** The one genuine
  new correctness bug. `engine/mod.rs::row_signature_unit` hashed with
  `rustc_hash::FxHasher` over `format!("{:?}", v)`, but TS `rowIDSignatureUnit`
  (`row-set-signature.ts`) is `h64(rowIDString(id))` — a totally different
  algorithm AND serialization. `rowSetSignature` (the XOR-fold of these units) is
  **persisted to the shared CVR** (`cvr.queries[queryID].rowSetSignature`) and
  compared against a freshly-computed value across processes/restarts
  (view-syncer.ts:1659-1669) → an FxHasher signature written by Rust mismatches
  any TS reader → **forced re-hydration of every query on any rolling / mixed /
  shadow deploy**. FIXED to reuse the ported `rust_cvr::hash::h64` over
  `rust_cvr::row_key::row_id_string` (the exact two functions TS composes; h64 =
  `(xxh32(s,0)<<32)|xxh32(s,1)` verified == TS `hash(s,2)`), with a TS-golden pin
  (`agentic/parity/row-signature-fixture.json` from the real TS impl +
  `row_signature_unit_matches_ts_golden`, non-vacuous — the old FxHasher fails it).
- **Pair 21 change_processor (F-CP-1/2/3) → CLEAN** (doc's own verdict). Ref-count
  ADD/EDIT/REMOVE, de-dupe, `_0_version` strip, page-flush all match. No action.
- **F-VIEW-2/3 → OVERSTATED (perf-only).** TS WeakSet-COW mutate mode vs Rust
  always-clone (Rc::make_mut gives COW at the Rc level). Final tree content-
  identical; `entries_equal` falls back to structural compare so broken pointer
  identity never yields a wrong result. Not correctness.
- **F-VIEW-4 → OVERSTATED (triple-unreachable).** make_id NaN serialization
  (PATTERN-B's 5th sighting): `make_id` only runs with `with_ids=true` (prod call
  sites hardcode `false`); `id` feeds only the engine-internal `entries_equal`
  (never serialized/persisted); and a PK can't hold NaN (JSON has no NaN literal).
- **F-VIEW-5 → WRONG (misread).** `Rc::get_mut(...).expect(...)` operates on a
  freshly `Rc::new`'d entry (strong count 1) → can't fire.
- **F-VIEW-6 → Note**, points at F-SIG-1 (now fixed).
- **F-RT-1 → CLEAN** (authEquals raw-token, resolves the F-CCM-1 note).
- **F-RT-2 → already FIXED** — same as F-TQ-2 (revocation probe, commit f63a506d5).
- **F-RT-3 → CONFIRMED but DELIBERATE (keep).** Rust drain has a 25s hard deadline
  TS lacks; documented deploy-safety (orchestrator SIGKILLs at ~30s, so staying
  under budget keeps the final shutdown graceful). Removing it to "match TS" would
  risk SIGKILL truncating the sweep. Registered intentional.
- **F-RT-4 → equivalent** (message built at the call site). **F-RT-5 → CLEAN**
  (auth-before-admission ordering faithful, same DoS-prevention comment).

**Round-2 net:** 1 more real correctness bug (**F-SIG-1**, the CVR signature hash —
arguably the highest-impact finding in the whole doc: silent mass re-hydration on
rolling deploys). Everything else new is CLEAN / perf-only / unreachable / already
fixed / deliberate. The Final Synthesis's "5 HIGH" still over-rate: F-SIG-1 (real,
fixed), F-LT-1 (real, fixed), F-CCM-1/F-CCM-2 (dormant reference module — real path
in router.rs is correct), F-RA-2 (bookkeeping).

---

## Status board

### Completed pairs (parent-side, verified)
- [x] Pair 1 — `auth/read-authorizer.ts` ⇄ `auth/read_authorizer.rs`
- [x] Pair 2 — `auth/load-permissions.ts` ⇄ `load_*` in `auth/read_authorizer.rs` (unmapped port)
- [x] Pair 3 — `services/view-syncer/drain-coordinator.ts` ⇄ `…/drain_coordinator.rs`
- [x] Pair 4 — `services/view-syncer/e2e-serving-lag.ts` ⇄ `…/e2e_serving_lag.rs` — CLEAN (1 self-killed)
- [x] Pair 5 — `workers/connect-params.ts` ⇄ `workers/connect_params.rs` (+ `types/url-params.ts`, `ws_server.rs` caller)
- [x] Pair 6 — `workers/syncer-ws-message-handler.ts` ⇄ `…/syncer_ws_message_handler.rs` (error-body mandate target)
- [x] Pair 7 — `workers/connection.ts` ⇄ `workers/connection.rs` (+ `ws_server.rs` keepalive, `ws_sink.rs`, `protocol.rs`)
- [x] Pair 8 — `services/view-syncer/query-covering.ts` ⇄ `…/query_covering.rs` — SUBAGENT (bg_8b9bd963, CLEAN)
- [x] Pair 9 — `custom-queries/transform-query.ts` ⇄ `custom_queries/transform_query.rs` — SUBAGENT (bg_ee4a95ab)
- [x] Pair 10 — `db/lite-tables.ts` ⇄ `db/lite_tables.rs` — SUBAGENT TIMED OUT, verified parent-side (bg_561137ae partials + direct verification)
- [x] Pair 12 — `auth/jwt.ts` ⇄ `auth/jwt.rs` (token-verification mandate target)
- [x] Pair 13 — `services/view-syncer/connection-context-manager.ts` ⇄ `…/connection_context_manager.rs` — SUBAGENT (bg_bc061840)

### In progress (subagents)
- (none — exists/take timed out, being re-diffed parent-side)

### Parent-side completed
- [x] Pair 17 — `ivm/join.ts` ⇄ `ivm/join.rs` (join-symmetry mandate target) — CLEAN
- [x] Pair 18 — `ivm/view.ts` + `ivm/view-apply-change.ts` ⇄ `ivm/view.rs` (view-refcounts mandate target) — parent-side full diff
- [x] Pair 14 — `ivm/cap.ts` ⇄ `ivm/cap.rs` — SUBAGENT (bg_bf579907)
- [x] Pair 19 — `ivm/filter-operators.ts` ⇄ `ivm/filter_operators.rs` (+ `filter.rs`) — parent-side
- [x] Pair 15 — `ivm/exists.ts` ⇄ `ivm/exists.rs` — TIMED OUT, re-diffed parent-side
- [x] Pair 16 — `ivm/take.ts` ⇄ `ivm/take.rs` — TIMED OUT, re-diffed parent-side
- [x] Pair 20 — `workers/syncer.ts` ⇄ `syncer.rs` + `router.rs` + `main.rs` + `ws_server.rs` + `metrics.rs` (token-pinning mandate target) — parent-side, COMPLETE
- [x] Pair 21 — `view-syncer.ts #processChanges` ⇄ `rust-cvr/src/change_processor.rs` (CVR lead C-CVR-D) — parent-side, CLEAN
- [x] Pair 22 — `row-set-signature.ts` / `pipeline-driver.ts #trackRowSetSignatures` ⇄ `engine/mod.rs row_signature_unit` (PARITY-CONTRACT "same bigint") — parent-side
- [x] Pair 23 — `builder/like.ts` + `query/escape-like.ts` ⇄ `builder/like.rs` + `query/escape_like.rs` (LIKE mandate target) — parent-side
- [x] Pair 24 — `auth/auth.ts` ⇄ `connection_context_manager.rs` (resolve_auth/pick_token/auth_equals) + `transform_query.rs` (is_auth_error_body) — parent-side
- [x] Pair 25 — `custom/fetch.ts` ⇄ `transform_query.rs` (post_transform) + `push_relay.rs` + `metrics.rs` — parent-side
- [x] Pair 26 — `pipeline-driver.ts` ⇄ `pipeline_driver.rs` + `engine/mod.rs` — parent-side, COMPLETE
- [x] Pair 27 — `view-syncer.ts` ⇄ `sync_engine.rs` + `router.rs` — parent-side, COMPLETE

### Parent-side in progress
- [ ] Pair 11 — `workers/syncer.ts` ⇄ `workers/syncer.rs` (token-pinning mandate target) — reading

### CVR partial findings (from timed-out finders — NOT re-verified, flagged as leads)
- cvr-behavior (timed_out, ses_01a037ec-9377-7c15-ac5e-127288cf1c03): `parse_ttl_string` is harness-only; `parse_ttl`/`clamp_ttl`/`compare_ttl` live + match TS. Was mid-verify of `#processChanges` ref-counting @ `view-syncer.ts:2472`. → NEEDS parent re-diff.
- cvr-missing (timed_out, ses_01a037ec-937b-7cfa-b2ee-fe10e4759d33): `change_processor` is cross-crate-called from `rust-syncer/src/sync_engine.rs:1210,1346` (driven by stage_e_test) → its "IO (integration diff)" classification is LEGITIMATE, not a GAP-0 miss; BUT the cvr fuzzer structurally can't reach it (no callers in rust-cvr src/tests). `set_client_schema` error IS exercised (metadataScenarios #5).
- cvr-nomenclature (timed_out, ses_01a037ec-937b-7cfa-b2ef-02af037aa8fa): doc-comments cite TS paths for files the MAP classifies "new (no TS origin)": `hash.rs`→`shared/src/hash.ts`, `row_key.rs`→`types/row-key.ts`, `shards.rs`→`types/shards.ts`, `ttl.rs`→`zql/src/query/ttl.ts`. MAP-vs-doc drift.

### Queue (syncer)
load-permissions(done), e2e-serving-lag, connect-params, connection, syncer-ws-message-handler, syncer, +6 merged/split (jwt, custom/fetch, connection-context-manager, pipeline-driver, view-syncer)

### Queue (ivm)
71 TS files; prioritize the 32 flagged unresolved behavioral symbols + 1:1 logic files (cap, exists, join, take, filter, view)

---

## Pair 1 — `auth/read-authorizer.ts` ⇄ `auth/read_authorizer.rs`

### F-RA-1 · Med · Missing deny-by-default warn log (observability divergence)
- **TS:** `read-authorizer.ts:62-70` — `transformQueryInternal` calls `lc.warn?.("No permission rules found for table 'X'. No rows will be returned… Use ANYONE_CAN…")` before setting the empty-OR.
- **Rust:** `read_authorizer.rs::transform_query_internal` — produces the identical empty-OR `{or,conditions:[]}` but emits **no log**.
- **Impact:** Transformation result is byte-equivalent; the operator-facing misconfiguration signal is gone. Operators monitoring for "table forgot a permission rule" get silent deny in Rust.

### F-RA-2 · High · MAP mislabels a MERGE as 1:1 → load-permissions.ts port is unmapped/un-diffed
- **Claim:** `MAP-syncer.md` row `auth/read-authorizer.ts → auth/read_authorizer.rs`, rel **1:1**.
- **Reality:** `read_authorizer.rs` also ports `loadPermissions` + `reloadPermissionsIfChanged` from `auth/load-permissions.ts` (confirmed exports exist in TS; Rust has `load_permissions`/`reload_permissions_if_changed`/`LoadedPermissions`/`PermissionsReload`), plus a permissions-validation layer (`validate_permissions_config` + 6 helpers) and `resolve_permissions`.
- **Evidence:** `load-permissions.ts` is referenced **0×** in MAP-syncer/COVERAGE-syncer/MAP-cvr/COVERAGE-cvr.
- **Impact:** None of that logic has a body-diff pair. Fix: re-label `read_authorizer.rs` as MERGED and add a `load-permissions.ts ⇄ read_authorizer.rs(load_*)` pair.

### F-RA-3 · UNVERIFIED · `getSchema` from load-permissions.ts may be unported
- TS `load-permissions.ts:83` exports `getSchema(lc, replica)`. No obvious counterpart in `read_authorizer.rs`; likely lives in a Rust schema/snapshotter module. Needs locating.

### F-RA-4 · Doc-verify · throw → deny-all (fail-closed); hash-read error swallowed
- Rust `resolve_permissions` / `reload_permissions_if_changed` doc-comments self-document: TS **throws** on unparseable doc and **bubbles** hash-read errors to a reset; Rust **deny-alls** (fail-closed) and **swallows** hash-read errors as `Unchanged`. Verify registered in PARITY-CONTRACT/COVERAGE exception list.

### SELF-KILLED
- `transformCondition` default arm: TS switch has no default (returns `undefined` for unknown `cond.type`); Rust `_ => cond.clone()`. Only matters for malformed conditions, which the protocol never produces. Not real.
- `correlatedSubquery` with absent `related`: TS would throw on `transformQueryInternal(undefined)`; Rust skips. Unreachable malformed-input edge.

---

## Pair 2 — `auth/load-permissions.ts` ⇄ `load_*` in `auth/read_authorizer.rs` (the unmapped port from F-RA-2)

### F-LP-1 · Med · Missing "No upstream permissions deployed" warn + hasCustomEndpoints guard
- **TS:** `load-permissions.ts:30-44` — when the permissions row is null, computes `hasCustomEndpoints` from `config` (push/mutate + query/getQueries URLs) and, unless custom endpoints are set, emits a prominent `lc.warn?.("No upstream permissions deployed. Run 'npx zero-deploy-permissions'…")`.
- **Rust:** `load_permissions` takes **no `config` parameter**; the guard is structurally impossible, and the warn is absent.
- **Impact:** Operators lose the deploy-permissions nudge. Same class as F-RA-1.

### F-LP-2 · Med-High · Validation drift hazard: valita schema vs hand-rolled validator
- **TS:** validates parsed permissions via `v.parse(obj, permissionsConfigSchema)` — the valita schema in `compiled-permissions.ts` is the single source of truth.
- **Rust:** uses a hand-rolled `validate_permissions_config` + 6 helpers with a hardcoded operator allowlist (`OPS = ["=","!=",…,"IN","NOT IN"]`).
- **Impact:** The two validators must be kept in sync **manually**; if the valita schema adds an operator/condition shape, Rust silently accepts or rejects wrongly. Security-adjacent (permissions enforcement). Needs a differential test against the real schema.

### F-LP-3 · Med · `current === null` first-load semantics diverge
- **TS:** `reloadPermissionsIfChanged` — `if (current === null)` → **always** calls `loadPermissions` and returns `changed: true`, even when the DB row is null (this is what triggers the F-LP-1 warn).
- **Rust:** `reload_permissions_if_changed(conn, app_id, current_hash: Option<&str>)` — when `current_hash` is `None` **and** the DB has no row, `new_hash` is `None` → `None == None` → returns **`Unchanged`**, skipping `load_permissions` entirely. So on "nothing loaded + nothing deployed", Rust never loads → never warns → never signals `changed`. Net enforced-permissions state may match (both None/pass-through), but any `changed`-driven client republish is skipped.
- **Impact:** Caller-context dependent (UNVERIFIED whether any caller relies on `changed` for republish on first-load-with-no-deploy).

### F-LP-4 · Doc-verify · throw → deny-all; hash-read error swallowed
- Same as F-RA-4. TS `load-permissions.ts:50-57` throws with `elide(...,100)` + server-version hint; Rust `resolve_permissions` converts parse-Err → `deny_all_permissions()`, and `reload_permissions_if_changed` swallows hash-read errors as `Unchanged` (with `tracing::warn`). Verify registered as exception.

### F-LP-5 · UNVERIFIED · `getSchema` unported
- TS `load-permissions.ts:83` exports `getSchema(lc, replica)` (via `computeZqlSpecs`). No counterpart in `read_authorizer.rs`; likely in a Rust schema/snapshotter module. Needs locating.

### SELF-KILLED
- (none this pair)

---

## Pair 3 — `services/view-syncer/drain-coordinator.ts` ⇄ `…/drain_coordinator.rs` (76 LOC, 1:1)

### F-DC-1 · Low-Med · `draining` one-shot promise → polled `is_draining()`
- **TS:** keeps `#draining = resolver<'draining'>()` resolved exactly once on the first `drainNextIn`; `get draining()` exposes an awaitable promise (one-time "drain has begun" signal).
- **Rust:** replaces it with `is_draining()` = `next_drain_time != 0` (a polled predicate, not a one-shot future). State-wise near-equivalent (Rust never resets `next_drain_time` to 0), but the awaitable-once API shape is gone.
- **Impact:** Callers that `await coordinator.draining` for a one-time side effect have no direct equivalent. Caller-context dependent.

### F-DC-2 · Low-Med · Missing precondition assert in `drain_next_in`
- **TS:** `drainNextIn` — `assert(this.#nextDrainTime <= now, 'drainNextIn() should only be called if shouldDrain()')` throws loudly if called when a future drain is already scheduled.
- **Rust:** `drain_next_in` — comment says "the router's forced-drain loop upholds this by construction"; **no runtime check**.
- **Impact:** A logic error elsewhere silently overwrites drain pacing instead of throwing.

### F-DC-3 · Low · `force_drain_timeout` busy-polls when unarmed
- **TS:** `forceDrainTimeout` returns a promise that simply never resolves while unarmed (no CPU).
- **Rust:** when `force_drain_at == 0`, loops `sleep(1ms)` (busy-poll), adding up to 1ms arm-latency and a polling wake.
- **Impact:** Likely rare in practice (usually armed before awaited).

### F-DC-4 · Low · Re-arm tracking diverges (likely intentional improvement)
- **TS:** `get forceDrainTimeout` returns the *current* `#timeout.promise` at call time; a re-arm (clearTimeout+setTimeout) abandons the old promise.
- **Rust:** `force_drain_timeout` re-reads `force_drain_at` each loop iteration, so it **follows re-arms** automatically. Doc-comment frames this as deliberate ("cannot lose a wakeup").
- **Impact:** Divergence to verify is registered; Rust is the more robust semantic.

### SELF-KILLED
- (none this pair)

---

## Pair 4 — `services/view-syncer/e2e-serving-lag.ts` ⇄ `…/e2e_serving_lag.rs` (82 LOC, 1:1)

**Verdict: CLEAN — faithful port.** Coalesce-oldest-commit-time, watermark-advance, served-version replay-guard, and negative-lag clamp all match exactly. Has a TS-golden differential fixture test (`e2e-serving-lag-fixture.json`).

### SELF-KILLED
- String comparison `servedVersion < pending.watermark`: TS `<` is UTF-16 code-unit lexicographic; Rust `&str` `<` is byte lexicographic. Diverges only for non-ASCII watermarks — but LexiVersion watermarks are zero-padded hex (ASCII), so they coincide. Not real.
- `f64` vs `number`: TS `number` is IEEE-754 double = Rust `f64`. Identical.

---

## Pair 5 — `workers/connect-params.ts` ⇄ `workers/connect_params.rs` (+ `types/url-params.ts`, `ws_server.rs` caller)

### F-CP-1 · Med · Duplicate query-param: TS first-wins, Rust last-wins
- **TS:** `url-params.ts:13` — `this.url.searchParams.get(name)`; `URLSearchParams.get` returns the **first** value for a duplicate key (`?clientID=A&clientID=B` → `A`).
- **Rust:** `connect_params.rs:65-67` — `parsed.query_pairs().map(...).collect::<HashMap>()`; HashMap insert **overwrites**, keeping the **last** value (`?clientID=A&clientID=B` → `B`).
- **Impact:** For an adversarial/duplicate-key URL, TS and Rust resolve a different `clientID`/`clientGroupID`/`userID` — auth-adjacent identity divergence. Exploitability requires a client/proxy that emits duplicate keys (unusual but not sanitized). Fix: iterate `query_pairs()` and keep first occurrence, or detect+reject duplicates.

### F-CP-2 · Low · `getInteger` error message text differs
- **TS:** `url-params.ts:33` throws `` `invalid querystring parameter ${name}, got: ${value}, url: ${this.url}` `` (includes the full URL).
- **Rust:** `ConnectParamsError::InvalidInt { name, value }` renders `invalid querystring parameter {name}, got: {value}` — no URL. (Rust reconstructs the URL as `http://localhost{path}`, so the real host is already discarded anyway.)
- **Impact:** Error-body divergence; matters only if a client/test pins on error text.

### F-CP-3 · Low · `get_integer` dead `None => Ok(0)` branch
- **TS:** `getInteger` returns `null` when the param is absent; for required ints the earlier `get(required:true)` throws first.
- **Rust:** `get_integer`'s `None => Ok(0)` branch is unreachable for current callers (`ts`/`lmid` are both `required:true`). Dead code, not a bug — but a silent `0` if a future optional-int caller forgets to handle None.

### SELF-KILLED
- Empty-string-as-missing: TS `get` treats `''`/`null` as missing → throws for required; Rust `get_string` does the same (`!v.is_empty()`). Match (verified `url-params.ts:14-21`).
- `getBoolean` truthiness: TS `value === 'true'`; Rust `== Some("true")`. Match (`url-params.ts:44`).
- Header normalization (`normalizeHeaders`): Rust `ws_server.rs:195-243` faithfully ports Node `_addHeaderLine` semantics — singleton headers keep FIRST value, `cookie` joins with `; `, others join with `, `, names lowercased. Match (cites #6144).
- `parseInt` quirks (truncate-at-`.`, leading ws+sign, stop-at-junk, auto-`0x`, stop-at-`e`, NaN→None): Rust `parse_js_integer` mirrors them and has a TS-golden fixture (`parse-int-fixture.json`). Match.
- `extract_protocol_version`: TS receives `protocolVersion` pre-parsed; Rust added a path parser (`/sync/v51/connect`→51). Not a divergence — TS parses elsewhere; behavior equivalent.

---

## Pair 6 — `workers/syncer-ws-message-handler.ts` ⇄ `…/syncer_ws_message_handler.rs` (error-body mandate target)

This pair is the mandate's "error-body's 3 cases vs all throw sites" target. The 3 push-path error bodies:
(a) clientGroupID mismatch → Fatal invalid_push — **match**; (b) custom-mutation-no-pusher → **DIVERGE** (F-SW-1); (c) CRUD-no-mutagen → Fatal "legacy CRUD disabled" — **match**. So 1 of 3 diverges.

### F-SW-1 · Med-High · Custom-mutation-no-pusher: Fatal⇄Transient + message text divergence
- **TS:** `syncer-ws-message-handler.ts` push case — if `mutations[0].type === 'custom'` and `!this.#pusher` → returns `[{type:'fatal', error:{kind:InvalidPush, message:'A ZERO_MUTATE_URL must be set in order to process custom mutations.', origin:ZeroCache}}]`. **Fatal** tears down the connection.
- **Rust:** `handle_push` — if `is_custom` and `!pusher` → returns `HandlerResult::Transient { errors:[invalid_push("This server does not process mutations over the sync connection. Configure the push relay (PUSHER_URL)...")] }`. **Transient** keeps the connection open.
- **Impact:** Fatal vs Transient is a real behavioral difference (connection torn down vs kept open) AND the message differs (ZERO_MUTATE_URL vs PUSHER_URL). A client relying on the fatal-close to retry/reconnect gets different behavior.

### F-SW-2 · Med-High · CRUD mutation auth: TS passes decoded JWT claims, Rust passes `{token: raw}`
- **TS:** push case — `const auth = mustGetConnectionContext(selector).auth; assert(auth?.type !== 'opaque'); … mutagen.processMutation(mutation, auth?.decoded, …)` — passes `auth.decoded` (the **decoded JWT claims object**).
- **Rust:** `handle_push` — `let auth_value = conn_ctx.auth.as_ref().map(|s| json!({"token": s})); mutagen.process_mutation(mutation, auth_value.as_ref(), …)` — `ConnContextInfo.auth: Option<String>` is the **raw token string**, wrapped as `{"token": <raw>}`.
- **Impact:** The mutagen receives structurally different auth data. TS mutagen authorizes using decoded claims; Rust mutagen receives a token wrapper it would have to decode itself. If the Rust `MutagenDispatch` impl doesn't decode-then-use-claims identically, mutation authorization diverges. Security-adjacent.

### F-SW-3 · Med · CRUD opaque-auth assertion skipped (Phase-4-deferred)
- **TS:** `assert(auth?.type !== 'opaque', 'Only JWT auth is supported for CRUD mutations')` — panics if auth is opaque.
- **Rust:** comment: "We skip this assertion here since auth type is not available in the ConnContextInfo struct. The full implementation in Phase 4 will check." — **no assertion**.
- **Impact:** An opaque-auth CRUD mutation that TS rejects (assert) is processed by Rust. Documented as Phase-4-deferred — verify registered in PARITY-CONTRACT/COVERAGE.

### F-SW-4 · Med (doc-verify) · `withTraceparent` does no OTel context propagation
- **TS:** `withTraceparent` does `propagation.extract(ROOT_CONTEXT, {traceparent})` + `context.with(extracted, fn)` — real W3C trace-context propagation, so downstream spans (mutagen/viewSyncer) carry the client trace.
- **Rust:** `with_traceparent` just `tracing::debug!(traceparent=tp)` and calls `f()` — **no OTel context**. Comment: "we extract the traceparent but don't propagate it via OTel context yet (Phase 4)."
- **Impact:** Distributed tracing from client → zero-cache → API server is broken in Rust. Documented Phase-4 gap — verify registered.

### F-SW-5 · Med-High (doc-verify) · `initConnection` returns `Ok` instead of stream results; production routes via router
- **TS:** initConnection case returns `[{type:'stream', source:'viewSyncer', stream: viewSyncer.initConnection(...)}, {type:'stream', source:'pusher', stream: pusher.initConnection(...)}]` — the actual data streams back to the client.
- **Rust:** returns `vec![HandlerResult::Ok]`; comment: "the stream is implicit — the ViewSyncer writes directly to the sink." Also notes the router intercepts initConnection on the CG thread BEFORE this handler in production, so this arm is "unit-tested reference dispatch" only.
- **Impact:** Two concerns: (1) the handler arm diverges (Ok vs stream); (2) production correctness depends on the **router's** CG-thread path doing the same `connContextManager.initConnection` + `pusher.initConnection` side effects. The router path is NOT in this file — needs a separate audit (see completeness critic). Doc-verify whether registered.

### F-SW-6 · Low · `updateAuth` revision comparison delegated to impl
- **TS:** reads `initialConnCtx` before `updateAuth`, then computes `authRevisionChanged = updatedConnCtx.revision !== initialConnCtx.revision` **in the handler**.
- **Rust:** `let initial = must_get_connection_context(selector); let auth_revision_changed = update_auth(selector, &body_value); let _ = initial;` — discards `initial`, trusts the impl's returned bool.
- **Impact:** Trust shift — the handler no longer verifies the revision delta; equivalent only if the `ConnContextManagerDispatch` impl compares before/after internally.

### SELF-KILLED
- `ping`/`pull`: both log error + return Ok. Match.
- `deleteClients` + pusher.deleteClientMutations: match (Rust passes extra relay headers/clientGroupID, structural not behavioral).
- Empty-mutations Ok: match.
- CRUD-no-mutagen Fatal "legacy CRUD disabled": match.
- clientGroupID-mismatch Fatal invalid_push: match.
- `unreachable(msgType)` default: Rust handles unknown types at `parse_upstream` (Fatal invalid_message) and the enum match is non-exhaustive-checked — equivalent + safer.
- `ackMutationResponses`: equivalent (Rust re-serializes body; round-trips).

---

## Pair 8 — `services/view-syncer/query-covering.ts` ⇄ `…/query_covering.rs` (subagent bg_8b9bd963 — CLEAN)

**Verdict: HIGH-FIDELITY port.** All 6 core covering functions verified function-by-function (`ast_covered_by`, `condition_implies`, `order_condition_implies`, `correlated_condition_implies`, `equality_implies`, `json_equal`). The order-sensitive 4-case dispatch in `condition_implies` is preserved exactly. **14/14 TS test scenarios have a direct Rust counterpart** (full mapping table below). Rust adds a stronger 20-case TS-grounded differential fixture (Rust-only). All divergences are Low severity, edge-case/malformed-AST only, conservative in direction, in a shadow-logging-only subsystem.

### TS→Rust test breadth (14/14 complete)
| # | TS test | Rust test |
|---|---|---|
| 1 | same query covers itself | `same_query_covers_itself` |
| 2 | unfiltered covers filtered same table | `unfiltered_covers_filtered_same_table` |
| 3 | conjunction covered by subset | `conjunction_covered_by_subset` |
| 4 | equality and range implication | `equality_and_range_implication` |
| 5 | or coverage is conservative | `or_coverage_is_conservative` |
| 6 | unlimited covers limited and paged | `unlimited_covers_limited_and_paged` |
| 7 | limited covering needs equivalent input | `limited_covering_needs_equivalent_input_and_large_limit` |
| 8 | related coverage is recursive | `related_coverage_is_recursive` |
| 9 | not exists reverses subquery implication | `not_exists_reverses_subquery_implication` |
| 10 | correlated subquery flip no effect | `correlated_subquery_flip_does_not_affect_semantics` |
| 11 | findCoveringQuery returns first active | `find_covering_query_returns_first_active` |
| 12 | index only considers matching root | `index_only_considers_matching_root` |
| 13 | index can be updated during batch | `index_can_be_updated_during_batch` |
| 14 | index replaces query when root changes | `index_replaces_query_when_root_changes` |

### F-QC-1 · Note/Low · "20 golden pairs" is a Rust-only differential fixture, not the TS test suite
- The mandate's "20 golden pairs vs the real TS test suite" conflates two corpora. **Real TS test suite = 14 scenarios**; the **20-case fixture** (`agentic/parity/query-covering-fixture.json`) is Rust-only, generated by `generate-query-covering-fixture.mjs` which imports the real TS `isQueryCoveredBy` (provenance sound). It is a stronger regression guard (adds reverse direction + 1 extra case) but exercised only on the Rust side. If TS regressed and the generator were re-run, the fixture would silently track broken TS. Property of differential testing, not a bug.

### F-QC-2 · Note/Low · `bounds_covered_by` diverges on non-numeric `limit`
- TS `query-covering.ts:157` uses JS `<` (coerces string/bool); Rust `query_covering.rs:228-230` `num()` returns None for non-numbers → `return false`. Divergence only on malformed ASTs (valid ASTs always have numeric limits); conservative direction.

### F-QC-3 · Note/Low · `present()` collapses `Some(Null)` vs `None`; TS distinguishes `null` vs `undefined`
- TS `:203-206` treats `null` ≠ `undefined`; Rust `:450-455` maps both → `None`. Reachable only if `normalize_ast` emits explicit nulls (doc-comment claims it omits undefined fields). See UNVERIFIED F-QC-U1.

### F-QC-4 · Note/Low · `column_literal_parts` stricter on `name`/missing `value`
- Rust requires `name` to be a string (else None→false); TS reads as-is. Missing `literal.value`: Rust→Null (is_non_null_scalar_literal→false), TS→undefined (isNonNullScalarLiteralValue→**true**). Malformed-AST only.

### F-QC-5 · Note/Low · `find_covering_query` free fn is dead code; file bundles shadow-summary from another module
- Rust `:105-106` marks free `find_covering_query` `#[allow(dead_code)]`; only the `QueryCoveringIndex` method is called (sync_engine.rs:818). File also defines `QueryCoverageShadowHit`/`log_shadow_summary` ported from a *different* TS module. MAP's "1:1" framing is loose.

### F-QC-6 · Note/Low · `flip` ignored identically in both (parity OK, possible shared semantic gap)
- Both impls never read `flip`; EXISTS-flip invariant asserted identically in TS test #10 and Rust `correlated_subquery_flip_does_not_affect_semantics`. If `flip` is meant to invert EXISTS↔NOT EXISTS, both are wrong together — a TS-correctness question, not a Rust divergence.

### F-QC-U1 · UNVERIFIED (HIGH-VALUE follow-up) · `normalize_ast` parity
- Findings F-QC-3/F-QC-4 and all root-bucketing depend on `normalize_ast` (in `auth/read_authorizer.rs`) producing identical canonical JSON to TS `normalizeAST` (`zero-protocol/src/ast.ts`) — specifically never emitting explicit `null` for `where`/`limit`/`start`/`orderBy` and always emitting `schema`/`table`/`alias`. **This is the same `normalize_ast` already audited in Pair 1** (read_authorizer.rs) — and Pair 1 confirmed it is a faithful, byte-parity port with golden-vector tests. So F-QC-U1 is **substantively resolved by Pair 1**: `hash_of_ast_matches_ts_golden_vectors` + `hash_is_deterministic_and_order_independent` prove canonical-JSON parity. Downgrading F-QC-3/F-QC-4 to non-issues.

### SELF-KILLED (subagent)
- TS `Map` vs Rust `Vec` bucket changes first-match: `add`'s remove-then-push preserves insertion order (verified by 2 Rust tests).
- OR/AND distribution transposed: 4-case order matches exactly after re-read.
- `root_key` `unwrap_or(Null)` diverges from `Required<AST>`: depends on normalize_ast (resolved by Pair 1).

---

## Pair 20 — `workers/syncer.ts` ⇄ `syncer.rs` + `router.rs` + `main.rs` + `ws_server.rs` + `metrics.rs` (parent-side, COMPLETE)

**The TS `Syncer` class is SPLIT across 5 Rust files.** Serving-lag math → `syncer.rs`. `#createConnection`/drain → `router.rs`. 60s sampler + WSS config → `main.rs`. Accept loop + keepalive → `ws_server.rs`. Gauges/counters → `metrics.rs`.

### F-RT-1 · Note (resolves F-CCM-1 note) · `authEquals` compares raw token string — matches TS
- (unchanged — see Pair 20 record below)

### F-RT-2 · Med · `validate()` gap confirmed (cross-ref F-TQ-2): auth maintenance is local-only
- (unchanged)

### F-RT-3 · Med (doc-verify) · Drain adds a 25s hard deadline (TS has none)
- (unchanged)

### F-RT-4 · Low · `check_and_pin_user` returns `Result<(), ()>` (no error message)
- (unchanged)

### F-RT-5 · Note · Admission ordering faithful (DoS prevention)
- (unchanged)

### F-SYN-3 · Med · `active-client-groups` OTel gauge missing
- **TS:** `getOrCreateGauge('sync', 'active-client-groups', ...).addCallback(result => result.observe(this.#viewSyncers.size))` (syncer.ts constructor).
- **Rust:** only in Prometheus text render (`metrics.rs:917,932` `render_prometheus(active_client_groups)`), NOT as an OTel observable gauge. The `queries` (`metrics.rs:300`) and `rows` (`metrics.rs:306`) gauges DO exist as `u64_observable_gauge`. So 2 of 3 Syncer gauges are ported; `active-client-groups` is missing from OTel.
- **Impact:** OTel-scraped metrics missing `active-client-groups`; Prometheus text export has it. Partial observability gap.

### F-SYN-4 · Med · WebSocket compression not supported
- **TS:** `getWebSocketServerOptions` (syncer.ts:196-217) — supports `perMessageDeflate` + `websocketCompressionOptions` JSON parsing from config.
- **Rust:** `main.rs:575` hardcodes `compression: false`; `ws_server.rs:613-614` warns "WebSocket compression requested but is not supported by this server."
- **Impact:** If a deployment relies on WS compression (high-throughput, bandwidth-constrained), Rust uses uncompressed frames → higher bandwidth. `maxPayloadBytes` is correctly ported (10MB default, `ZERO_WEBSOCKET_MAX_PAYLOAD_BYTES` env override).

### F-SYN-5 · Note · ServiceRunner ref-counting → Rust CG-lifetime model (architectural)
- **TS:** `ServiceRunner<ViewSyncer>` with `ref()`/`unref()`/`hasRefs()` for mutagens and pushers — each connection bumps the ref count; close decrements; zero refs → stop the service.
- **Rust:** `Arc<dyn MutagenDispatch>`/`Arc<dyn PusherDispatch>` per CG (created once at CG creation via `create_mutagen`/`create_pusher`, router.rs:342-346). No per-connection ref counting — the mutagen/pusher live for the CG's lifetime.
- **Impact:** Architectural simplification. In TS, a CG with 0 connections stops its mutagen/pusher; in Rust they live until the CG thread shuts down. Functionally equivalent for the common case (CGs with connections use mutagen/pusher; CGs without connections are being shut down).

### F-SYN-6 · Note · Serving-lag cache: TS microtask vs Rust TTL
- **TS:** `#servingLagDistributionCache` with `queueMicrotask` to clear (cleared on the next microtask after the first read in a tick).
- **Rust:** `ServingLagRegistry::compute_serving_lag_distribution` (syncer.rs:336-356) uses a `DISTRIBUTION_CACHE_TTL_MS` TTL-based cache. Different invalidation strategy but functionally similar (both avoid redundant recomputation within a short window).

### F-CON-2 · REFUTED · `websocket.errors` counter EXISTS and is correctly incremented
- **Original claim:** "Rust handle_close/handle_error only log — no counter."
- **Correction:** `metrics.rs:578-599` defines `ws_errors()` counter, and `record_websocket_error(event_type, protocol_version)` increments it with `protocol.version` + `event.type` tags. `ws_server.rs:521` calls `record_websocket_error("unclean_close", protocol_version)` on non-1000 close; `ws_server.rs:525` calls `record_websocket_error("error_event", protocol_version)` on transport error. **Match with TS.** F-CON-2 is refuted.

### SELF-KILLED
- `pinned_user_id` first-connection bind: both pin on first connection. Match.
- Connection replacement (close existing for same clientID): both close-then-register. Match.
- `group_auth_states` stale-entry pruning: Rust-specific cleanup.
- `client_auth` + `client_raw_auth` dual storage: match.
- Rehome-on-capacity-overflow: matches TS.
- `#recordReplicaReadyState` → `ServingLagRegistry::record_replica_ready_state` (syncer.rs:305): match.
- `#recordViewSyncerLagSamples` → 60s sampler in main.rs:536-553: match.
- `run()`/`stop()`: TS `#stopped.promise`/`clearInterval`+`wss.close`; Rust runtime block_on + router drain/shutdown. Different lifecycle, functionally equivalent.
- `getWebSocketServerOptions` maxPayload: 10MB default + env override. Match (compression is F-SYN-4).
- `websocket.open_connections` / `connection_attempts` / `connection_successes` / `connection_failures`: all exist in metrics.rs:493-523. Match.

---

## Pair 27 — `view-syncer.ts` ⇄ `sync_engine.rs` (parent-side, PARTIAL)

**Previously diffed:** `#processChanges` (Pair 21, CLEAN), `#createConnection` (Pair 20, via router.rs). **This pass:** `changeDesiredQueries`/`updateAuth`/`deleteClients` → `#handleConfigUpdate` → `config_and_hydrate_with_profile`, plus failed-hydration cleanup.

### F-VS-1 · Note · `changeDesiredQueries`/`updateAuth`/`deleteClients` all delegate to `#handleConfigUpdate` — Rust `config_and_hydrate_with_profile` is the equivalent
- **TS:** all three message handlers call `#handleConfigUpdate(lc, clientID, msg, cvr, mode, profileID, connCtx)` with different modes: `'missing'` (changeDesiredQueries/deleteClients), `'all'` (updateAuth re-transforms all).
- **Rust:** `config_and_hydrate_with_profile` takes pre-computed `desired_puts`/`desired_dels`/`desired_clear` — the query-set delta is computed by the caller (`handle_desired_queries` in router.rs). Different API shape (TS computes delta inside `#handleConfigUpdate`; Rust receives it pre-computed), but the core flow (apply patches → transform → hydrate → advance → poke) matches structurally.

### F-VS-2 · Note · Failed-hydration cleanup honors "leaves no query or signature" contract
- **PARITY-CONTRACT:** "Failed or abandoned hydration leaves no query or signature."
- **Rust:** `remove_query` (`engine/mod.rs:515`) removes BOTH the pipeline entry AND the `row_set_signatures` entry. Used at: (a) re-transform path (`sync_engine.rs:786` — hash-drifted queries torn down before rebuild, matching TS `PipelineManager.addQuery(id, differentHash)`), (b) failed-hydration cleanup (`:1204`, `:1424`). Comment at `:652`: "torn down first (`remove_query`, WITHOUT a CVR got-query del — the query is still desired)."
- **Impact:** Contract honored — no partial registration or leftover signature on failure.

### F-VS-3 · Med (cross-ref) · `updateAuth` `#validateConnection` gap — already F-TQ-2/F-RT-2
- **TS:** `updateAuth` calls `#validateConnection(connCtx)` when `!this.#pipelinesSynced` — sends empty `/query` to surface server-side auth revocation.
- **Rust:** auth maintenance is local JWT validation only (F-RT-2). Already recorded.

### F-VS-4 · Note · `#processChanges` wraps hydration in single generator for de-duping — Rust uses `ChangeProcessor` callback
- **TS:** `#processChanges` is called with `generateRowChanges(this.#slowHydrateThreshold)` — a generator that wraps all pipelines in a single generator for max de-duping.
- **Rust:** `ChangeProcessor::on_row_change` is called per-change from the engine callback. De-duping happens in the `ChangeProcessor`'s HashMap. Already verified CLEAN (Pair 21).

### UNVERIFIED
- `#handleConfigUpdate` full body (CVR config update, desired-query patch application, deleted-client handling).
- `#hydrate` internals (pipeline build, source connection, streaming vs batch).
- `#syncQueryPipelineSet` (the query-set sync flow).
- `#runAuthMaintenance` full body (only the `validate()` gap was checked).
- `keepalive()`/`#scheduleShutdown`/`#checkForShutdownConditionsInLock` (shutdown lifecycle).
- `inspect()` (debug introspection).
- `servedVersion`/`servingLagEligible` (serving-lag integration).

---

## Pair 26 — `pipeline-driver.ts` ⇄ `pipeline_driver.rs` + `engine/mod.rs` (parent-side, PARTIAL)

**PARITY-CONTRACT surfaces diffed:** `totalHydrationTimeMs`, `rowSetSignature` (Pair 22), `removeQuery`, `advance` / `shouldAbort`. Not yet diffed: `init`/`reset` internals, `addQuery` hydration stream, `getRow`, `queries()` insertion order, scalar-subquery resolution.

### F-PD-1 · Med · `should_abort` is a partial port of `#shouldAdvanceYieldMaybeAbortAdvance` — 3 reset paths missing
- **TS** (`pipeline-driver.ts:1094-1148`): `#shouldAdvanceYieldMaybeAbortAdvance` has 4 paths:
  1. `shouldResetSlowCurrentChange` — throws if a SINGLE change takes too long (per-change reset).
  2. `shouldResetProjectedAdvancement` — throws if projected total time exceeds budget.
  3. `shouldFinishLateAdvancement` — if past 50% of changes, DON'T abort (finish even if slow).
  4. `elapsed > MIN_ADVANCEMENT_TIME_LIMIT_MS && (elapsed > totalHydrationTimeMs || (elapsed > totalHydrationTimeMs/2 && pos <= numChanges/2))` — the basic time-budget yield/abort.
- **Rust** (`engine/mod.rs:341-355`): `should_abort` implements ONLY path 4. Paths 1, 2, 3 are absent.
- **Impact:** (a) A single stuck change that TS would reset via `shouldResetSlowCurrentChange` is NOT reset in Rust — the advance continues stuck. (b) A projected-overrun that TS would catch early (path 2) isn't caught — Rust waits until the actual elapsed exceeds the budget. (c) Rust aborts past 50% of changes (TS's `shouldFinishLateAdvancement` prevents this) — so Rust may abort an advance that TS would finish, causing an unnecessary reset.

### F-PD-2 · Med · Timer: TS pause-aware `Timer.totalElapsed()`, Rust `Instant::now()` (wall-clock)
- **TS:** `advanceTimer.totalElapsed()` — a pause-aware timer that EXCLUDES time parked on credit/TSFN delivery (cooperative scheduling pauses).
- **Rust:** `self.timer.elapsed()` — `Instant::now()` wall-clock, includes ALL elapsed time.
- **PARITY-CONTRACT:** "advance time budget: Semantically equivalent — charge active computation against the hydration-derived budget and exclude time parked on credit/TSFN delivery, matching TS TimeSliceTimer pause semantics."
- **Impact:** If there's significant park time during an advance (e.g., credit exhaustion, TSFN backpressure), Rust's elapsed includes it → Rust may abort/yield earlier than TS. The PARITY-CONTRACT classifies this as "Semantically equivalent" (not "Must match exactly"), but the pause-exclusion is absent.

### F-PD-3 · Med · Yield divergence in advance (PATTERN-A)
- **TS:** `#advance` yields `'yield'` between changes when `#shouldAdvanceYieldMaybeAbortAdvance()` returns true (with `checkYield=true`). The caller iterates and processes yields for cooperative scheduling.
- **Rust:** `advance_streaming` checks `should_abort()` and `break`s — no yield sentinel propagated. The callback-based architecture (`on_row_change: F`) has no mechanism for yields.
- **Impact:** Same as PATTERN-A (F-CAP-3/F-EX-1/F-TAKE-2): if the oracle records yields during advance, traces diverge.

### F-PD-4 · Note · `total_hydration_time_ms` faithful (negative-zero fix)
- **TS:** sums `pipeline.hydrationTimeMs` across all pipelines.
- **Rust:** `engine/mod.rs:494-501` — `self.pipelines.iter().map(|p| p.hydration_time_ms).sum::<f64>()` with explicit `if total == 0.0 { 0.0 } else { total }` to avoid negative-zero from empty-iterator sum. `set_hydration_time_ms` correctly takes the caller's pause-aware value. Match.

### F-PD-5 · Note · `remove_query` faithful
- Destroys pipeline + companions, removes `row_set_signatures` entry. Match.

### SELF-KILLED
- `MIN_ADVANCEMENT_TIME_LIMIT_MS`: shared single source (`advance_gate.rs`), not duplicated. Match.
- `AdvanceContext` fields (timer, total_hydration_time_ms, num_changes, pos): match TS `#advanceContext`.
- `rowSetSignature` XOR fold: match (the hash function bug is F-SIG-1, separate).

### UNVERIFIED
- `init`/`reset` internals (schema validation, primaryKeys building, snapshotter init).
- `addQuery` hydration stream (the `add_queries_streaming` path — row-change order, yield sentinels, error/reset classification).
- `getRow` (projected synced columns, missing-row result, `fromSQLiteTypes`).
- `queries()` insertion order (TS `ReadonlyMap` insertion order; Rust `Vec` — likely matches but unverified).
- Scalar-subquery resolution (`#resolveScalarSubqueries`).

---

## Pair 27 — `view-syncer.ts` ⇄ `sync_engine.rs` + `router.rs` (parent-side, COMPLETE)

**Previously diffed:** `#processChanges` (Pair 21, CLEAN), `#createConnection` (Pair 20), `changeDesiredQueries`/`updateAuth`/`deleteClients` → `#handleConfigUpdate` (earlier in Pair 27). **This pass:** `keepalive()`/shutdown lifecycle, `inspect()`, `servedVersion`/`servingLagEligible`, `#runAuthMaintenance` body.

### F-VS-1 · Note · `changeDesiredQueries`/`updateAuth`/`deleteClients` delegate to `#handleConfigUpdate` — Rust `config_and_hydrate_with_profile` equivalent
- (unchanged)

### F-VS-2 · Note · Failed-hydration cleanup honors "leaves no query or signature" contract
- (unchanged)

### F-VS-3 · Med (cross-ref) · `updateAuth` `#validateConnection` gap — already F-TQ-2/F-RT-2
- (unchanged)

### F-VS-4 · Note · `#processChanges` wraps hydration in single generator for de-duping — Rust uses `ChangeProcessor` callback
- (unchanged)

### F-VS-5 · Med · `servingLagEligible` approximated: `!connections.is_empty() && !registered_ws.is_empty()` vs TS `clients.size > 0 && getBackgroundConnectionContext() !== undefined`
- **TS:** `servingLagEligible` (`view-syncer.ts:670-674`) — `this.#clients.size > 0 && this.connContextManager.getBackgroundConnectionContext() !== undefined`. The background-connection check means the view-syncer is only lag-eligible if there's a validated connection context available for background work (transform/revalidate).
- **Rust:** `serving_lag_eligible` (`router.rs:1608-1610`) — `!self.registered_ws.is_empty() && !self.connections.is_empty()`. Comment (`:1606`): "Approximated here as…".
- **Impact:** The TS check is about the presence of a *background connection context* (a validated conn that can absorb background work). The Rust check is about *any* connection being present. A CG with connections but no validated background context would be lag-eligible in Rust but NOT in TS → Rust's serving-lag metric includes CGs TS excludes → inflated lag numbers. Low operational impact (the metric is observability-only), but a real semantic divergence.

### F-VS-6 · Note · `keepalive()`/shutdown lifecycle faithful
- **TS:** `keepalive()` returns false if `!#stateChanges.active` (view-syncer stopped); else sets `#keepAliveUntil = now + keepaliveMs`. `#checkForShutdownConditionsInLock` — if clients > 0, no shutdown; await `cvrStore.flushed()`; if `now <= keepAliveUntil`, reschedule; else if clients === 0, shutdown.
- **Rust:** `idle_shutdown_due` (`router.rs:2888-2892`) — `connections.is_empty() && connection_count == 0 && now >= keepalive_until`. `keepalive_until` set on connect (`:1932`) and on CG creation (`:1575`). Router's CG sweep checks this. Match (different factoring — TS uses a per-VS shutdown timer + state-changes subscription; Rust uses the router's idle-sweep loop).

### F-VS-7 · Note · `inspect()` faithful
- **TS:** `inspect(selector, msg)` — debug introspection, sends inspect response to the client.
- **Rust:** `inspect_queries` (`sync_engine.rs:145`) → `CVRStore::inspect_queries` (SQL port). Router dispatches at `:2695`. Match.

### F-VS-8 · Note · `servedVersion` / `markVersionServed` faithful
- **TS:** `servedVersion` returns `#servedVersion` (string | null); `#markVersionServed` sets it after poke.
- **Rust:** `served_version: Option<String>` (`router.rs:1429`); `mark_version_served` (`:2961`) updates it. Match.

### F-VS-9 · Note · `#runAuthMaintenance` body — `planMaintenance` → revalidate → retransform
- **TS:** `#runAuthMaintenance` (`view-syncer.ts:824-862`) — calls `connContextManager.planMaintenance()`, then for each `dueRevalidation` calls `#validateConnection(connCtx)`. If a revalidation fails with TransformFailed, defers. After revalidation, replans and if `dueRetransform`, runs `#runBackgroundRetransform`.
- **Rust:** auth maintenance (`router.rs:~1788-1866`, from F-TQ-2) — `handle_update_auth` re-validates JWT locally + sends empty `desired_queries`. The `planMaintenance` → `dueRevalidations` → `dueRetransform` cycle is NOT structurally present — Rust uses a simpler revalidate-then-retransform path.
- **Impact:** The `planMaintenance` scheduling (deadline-based revalidation, defer-on-failure, retransform-after-revalidation) is absent. Rust does local JWT revalidation on a timer, not the TS connection-context-manager-driven maintenance plan. This is the same class as F-TQ-2/F-RT-2 — the auth-maintenance architecture differs.

### SELF-KILLED
- `#deleteClientDueToDisconnect`: TS deletes client + closes conn context; Rust `on_connection_closed` decrements count + removes from maps. Different structure, same effect.
- `#cleanup(err?)`: TS destroys pipelines + stops transformer; Rust `shutdown()` rehomes all CGs. Different lifecycle, same outcome.
- `run()`/`readyState()`: TS `#stopped.promise` + `#stateChanges` subscription; Rust router runtime loop. Architecturally different, functionally equivalent.
- `queryCount`/`rowCount`: TS sums across pipelines; Rust `query_count()` (`:1616`) = `active_query_ids().len()`, `total_rows()` via registry. Match.

---

## Pair 26 — `pipeline-driver.ts` ⇄ `pipeline_driver.rs` + `engine/mod.rs` (parent-side, COMPLETE)

**PARITY-CONTRACT surfaces diffed:** `init`/`reset`, `addQuery` hydration stream, `advance`/`shouldAbort`, `totalHydrationTimeMs`, `rowSetSignature` (Pair 22), `removeQuery`, `getRow`, `queries()` insertion order, `#resolveScalarSubqueries`/companion rows.

### F-PD-1 · Med · `should_abort` is a partial port of `#shouldAdvanceYieldMaybeAbortAdvance` — 3 reset paths missing
- (unchanged) TS has 4 abort/reset paths; Rust implements only the basic time-budget one. Missing: `shouldResetSlowCurrentChange` (per-change reset), `shouldResetProjectedAdvancement` (projected-overrun reset), `shouldFinishLateAdvancement` (don't abort past 50%).

### F-PD-2 · Med · Timer: TS pause-aware `Timer.totalElapsed()`, Rust `Instant::now()` (wall-clock)
- (unchanged) PARITY-CONTRACT: "Semantically equivalent — exclude time parked on credit/TSFN delivery." Rust includes all elapsed time.

### F-PD-3 · Med · Yield divergence in advance (PATTERN-A)
- (unchanged) TS yields between changes; Rust `advance_streaming` breaks on `should_abort` with no yield sentinel.

### F-PD-4 · Note · `total_hydration_time_ms` faithful (negative-zero fix)
- (unchanged)

### F-PD-5 · Note · `remove_query` faithful
- (unchanged) Destroys pipeline + companions + `row_set_signatures` entry.

### F-PD-6 · Med · Hydration yield divergence: `skip_yields` in `add_queries_streaming` (PATTERN-A)
- **TS:** `#addQueryImpl` iterates `hydrateInternal(...)` and yields `change` (including `'yield'` sentinels) directly: `for (const change of hydrateInternal(...)) { if (change !== 'yield') { hydrationRowCount++; } yield change; }`.
- **Rust:** `add_queries_streaming` (`engine/mod.rs:~620`) — `let mut nodes = crate::ivm::stream::skip_yields(stream);` — drops yield sentinels during hydration fetch.
- **Impact:** PARITY-CONTRACT: `addQuery()` requires "Same ordered row-change stream, including yield sentinels." Rust drops them during hydration. Same class as F-CAP-3/F-EX-1/F-TAKE-2.

### F-PD-7 · Note · `init`/`reset` faithful (schema validation + primaryKeys)
- **TS:** `init` asserts not-already-initialized, calls `snapshotter.init()` + `#initAndResetCommon` (computeZqlSpecs, checkClientSchema, build primaryKeys from tableSpecs + clientSchema). `reset` destroys all pipelines + clears tables + signatures, then calls `#initAndResetCommon`.
- **Rust:** `init` (`pipeline_driver.rs:226`) preserves/destroys snapshotter, clears all state, calls `build_engine`. `build_engine` (`:301`) creates TableSource/MemorySource per table with columns+primaryKey. `set_client_primary_keys` (`:373`) applies client PKs to the engine. Structurally matches.

### F-PD-8 · Note · `getRow` faithful
- **TS:** `getRow(table, pk)` → `source.getRow(pk)`. Returns `Row | undefined`.
- **Rust:** `get_row(table, pk)` (`engine/mod.rs:1387`) → `source.borrow().get_row(pk)`. Returns `Option<Row>`. Match.

### F-PD-9 · Note · `queries()` insertion order preserved
- **TS:** `queries()` returns `ReadonlyMap<string, QueryInfo>` — insertion-ordered (JS Map).
- **Rust:** `running_queries()` (`pipeline_driver.rs:197`) iterates `self.query_order` (a `Vec<String>`) preserving insertion order. `query_order.retain` on remove (`:387`). Match.

### F-PD-10 · Note · Scalar-subquery companion rows: faithful with graceful-cancel drain
- **TS:** `#addQueryImpl` yields companion rows as ADD RowChanges after the main hydrate stream (`pipeline-driver.ts:693-703`). Companion pipelines are attached for live monitoring.
- **Rust:** `add_queries_streaming` Phase 2 (`engine/mod.rs:633-678`) emits companion rows as ADD RowChanges after the main fetch, with PK fallback (map → schema_pk → panic). Phase 3 attaches monitoring outputs. On cancel, drains remaining stream to exhaustion (to avoid Take/Cap guard panic) then destroys everything — no partial registration. Match.

### F-PD-11 · Note · Cancellation: Rust adds graceful drain on cancel (TS drains fully by contract)
- **TS:** the view-syncer ALWAYS fully drains the hydrate generator (the contract says the stream "must be iterated over in their entirety"). So a Take/Cap stream is never abandoned mid-iteration.
- **Rust:** adds an explicit `cancellation_token` + graceful-drain-on-cancel (`engine/mod.rs:~620-640`) because the callback-based architecture (`on_row_change: F`) can be abandoned mid-stream (consumer disconnect). The drain ensures Take/Cap guards don't panic. Architectural adaptation, not a divergence — matches TS behavior (both fully drain).

### SELF-KILLED
- `MIN_ADVANCEMENT_TIME_LIMIT_MS`: shared single source. Match.
- `AdvanceContext` fields: match TS `#advanceContext`.
- `removeQuery` before `addQuery` (replace-query): both call remove first. Match.
- `#hydrateContext === null` assertion (no hydrate during advance): Rust uses `cancellation_token` + sequential phases. Equivalent.
- Cost model (`#ensureCostModelExistsIfEnabled`): Rust has `perf_trace::scope` instead — different mechanism, same purpose (cost estimation is planner-internal, not a PARITY-CONTRACT surface).
- `currentVersion`/`replicaVersion`: both return snapshotter version. Match.

---

## Pair 25 — `custom/fetch.ts` ⇄ `transform_query.rs` + `push_relay.rs` + `metrics.rs` (parent-side)

**MAP:** SPLIT — fetch.ts (569 LOC) → `metrics.rs` (6), `transform_query.rs` (4), `protocol.rs` (2), `push_relay.rs` (1). The TS `fetchFromAPIServer` is shared between push and transform (`source: 'push' | 'transform'`); Rust splits them into `post_transform` (transform) and `push_relay.rs` (push).

### F-FETCH-1 · Med · `url_match` is a custom glob, not URLPattern — `*` semantics diverge (mandate's "url-match subset vs URLPattern" target)
- **TS:** `urlMatch(url, allowedUrlPatterns)` (`fetch.ts:264`) — uses `URLPattern.test(url)` from `urlpattern-polyfill`. URLPattern parses the URL into protocol/hostname/port/path/search/hash components and matches each independently. `*` in hostname matches a **single subdomain level** (does NOT cross `.`): `https://*.example.com/endpoint` matches `api.example.com` but NOT `api.v1.example.com` (needs `*.*.example.com`).
- **Rust:** `url_match(pattern, url)` (`transform_query.rs:121`) — custom `glob_match` treating the entire URL as a flat byte string. `*` matches **any characters including `.` and `/`**. So `https://*.example.com/endpoint` matches `https://api.v1.example.com/endpoint` in Rust but NOT in TS.
- **Impact:** A URL allowed by Rust's `url_match` may be rejected by TS's `URLPattern` (or vice versa). If the configured allowed-URL pattern uses `*` in the hostname, Rust is more permissive — allowing URLs TS would reject. Security-adjacent (URL allowlist bypass). The `:name` parameter is approximated (`:alpha/_` start → non-`/` segment) but `://` and `:8080` are handled by the alpha-check heuristic, not URL structure awareness.
- **Also:** `compileUrlPattern` (TS) validates patterns at config time (`new URLPattern(pattern)` throws on invalid); Rust stores raw strings and matches at request time — no config-time validation.

### F-FETCH-2 · Note · Header composition order matches exactly
- **TS:** `Content-Type` → `X-Api-Key` → `customHeaders` → `requestHeaders` → `Authorization` → `Cookie` → `Origin` → OTel inject (`fetch.ts:178-195`).
- **Rust:** `composed_headers()` (`transform_query.rs:91-111`) — `X-Api-Key` → `client_headers` → `request_headers` → `Authorization` → `Cookie` → `Origin`. `Content-Type` set separately in `post_transform_attempts`. `set_header` implements overwrite precedence. Match (minus OTel inject — F-FETCH-6).

### F-FETCH-3 · Note · `bodyPreview` ported for both push and transform
- **TS:** `getBodyPreview` (`fetch.ts:37`) — clones response, reads up to 512 chars. Included in `apiFailedBody` for HTTP errors.
- **Rust:** Push: `read_body_preview` (`push_relay.rs:81`) with `BODY_PREVIEW_CAP`. Transform: `transform_query.rs:~496` inserts `bodyPreview` into the failure object. Both match.

### F-FETCH-4 · Note · `mutationIDs` in push path uses real IDs (unlike F-TQ-1)
- **TS:** `apiFailedBody` includes `mutationIDs: []` (always empty in the fetch.ts function; the caller `pusher.enqueuePush` fills them).
- **Rust:** `push_relay.rs:56,62` — `mutation_ids: Vec<MutationID>` extracted via `mutation_ids_of(push_body)`. `PushFailedHttpBody` includes real `mutation_ids`. Match (the push path doesn't have the F-TQ-1 `queryIDs: []` issue).

### F-FETCH-5 · Med · Metrics divergence — TS has rich per-attempt attrs, Rust is simpler
- **TS:** `apiInFlight`, `apiRequests`, `apiRequestDuration`, `apiAttempts`, `apiAttemptDuration` with attrs: `http_status_code`, `http_status_class`, `error_kind`, `error_reason`, `will_retry`, `attempt_count`, `operation`, `cleanup_type`. Per-attempt recording via `recordApiAttempt`.
- **Rust:** `record_api_request(result)`, `record_api_in_flight(delta)`, `record_api_attempt(result, will_retry, attempt_ms, attempt, status)`. Simpler — likely missing `http_status_class`, `error_kind`, `error_reason`, `operation`, `cleanup_type` attrs. UNVERIFIED (didn't read `metrics.rs` in full).

### F-FETCH-6 · Note · OTel `propagation.inject` absent (cross-ref F-SW-4)
- **TS:** `propagation.inject(context.active(), headers)` (`fetch.ts:193`) — injects W3C trace context into request headers.
- **Rust:** not present. Same as F-SW-4 (`with_traceparent` does no propagation).

### F-FETCH-7 · Low · `apiErrorFromResult` / legacy error format detection (cross-ref F-TQ-7)
- **TS:** `apiErrorFromResult` (`fetch.ts:290`) — checks for `['transformFailed', ...]` legacy tuple and `pushErrorSchema`. Used for metrics classification.
- **Rust:** no equivalent in the transform path. Legacy `['transformed', ...]` tuple handling is F-TQ-7. UNVERIFIED whether the push path handles legacy push error format.

### SELF-KILLED
- Backoff formula: `min(1000, 100 * 2^(attempt-1) + jitter)` — algebraically identical (subagent verified). Jitter source differs (`Math.random()*100` float vs `subsec_nanos()%100` int) but bounded 0..100 both ways.
- `MAX_ATTEMPTS=4` / `FETCH_MAX_ATTEMPTS=4`: match.
- Retry on `status >= 500` / `is_server_error()`: match.
- Retry on `fetch failed` TypeError / `reqwest::Error` network: match.
- Reserved-param guard (`schema`, `appID`): match.
- `schema={app}_{shard}&appID={app}` append: match.
- `Content-Type: application/json`: match.

---

## Pair 24 — `auth/auth.ts` ⇄ `connection_context_manager.rs` + `transform_query.rs` (parent-side)

**Verdict: CLEAN — faithful port.** `resolveAuth`/`pickToken`/`authEquals`/`isAuthErrorBody` all verified against TS source (resolves Pair 13 UNVERIFIED). 5 findings, all Low/Note.

### F-AUTH-1 · Note · `resolveAuth` / `pickToken` faithfully ported (resolves Pair 13 UNVERIFIED)
- All 7 branches match with byte-for-byte error messages. `pickToken` JWT `sub`/`iat` ordering matches.

### F-AUTH-2 · Note · `authEquals` compares raw token (confirms F-RT-1/F-CCM-1)
- TS `a.type === b.type && a.raw === b.raw` (`auth.ts:38`); Rust `a_type == b_type && a.raw() == b.raw()` (`ccm.rs:344`). Match.

### F-AUTH-3 · Low · `JwtPayload` is a lossy subset (`{sub, iat}` vs full `JWTPayload`)
- Only affects reference module (not production — F-CCM-1). Production uses `decode_jwt_claims` → full `Value`.

### F-AUTH-4 · Low (nomenclature) · `isAuthErrorBody` in `transform_query.rs:303`, not `protocol.rs` as MAP claims
- Port itself faithful. MAP nomenclature drift.

### F-AUTH-5 · Low · `pickToken` `null` return path absent in Rust
- TS `newToken: Auth | undefined | null`; Rust `new: &Auth` (not Option). Legacy `@deprecated` path only.

---

## Pair 23 — `builder/like.ts` + `query/escape-like.ts` ⇄ `builder/like.rs` + `query/escape_like.rs` (LIKE mandate target, parent-side)

**Verdict: CLEAN — faithful port.** The regex construction (`%`→`.*`, `_`→`.`, `\x`→escaped literal, special-char escaping), dotall `s` flag, no-wildcard fast path, and trailing-backslash error all match.

### F-LIKE-1 · Low-Med · ILIKE: TS uses regex `i` flag, Rust lowercases both sides (regex-lite limitation)
- **TS:** `patternToRegExp(pattern, flags)` builds `new RegExp(pattern + '$', flags + 's')` — for ILIKE, `flags='i'` → the regex itself does case-insensitive matching on the ORIGINAL strings.
- **Rust:** `pattern_to_regex` lowercases the pattern before building the regex, and `get_like_op` lowercases the lhs before matching: `re.is_match(&lhs.to_lowercase())`. Comment: "regex-lite may not support Unicode case-insensitive matching."
- **Impact:** For ASCII, `toLowerCase()` + exact match ≈ regex `i` flag. For Unicode edge cases (Turkish `İ`/`ı`, German `ß`, Greek `Σ`), JS `toLowerCase()` and regex case-insensitive matching can disagree. Low probability in practice (LIKE patterns are typically ASCII), but a real divergence class.

### SELF-KILLED
- `escapeLike` / `escape_like`: TS `val.replace(/[%_]/g, '\\$&')` = Rust `val.replace('%', "\\%").replace('_', "\\_")`. Match.
- `specialCharsRe` / `is_special_regex_char`: same set of special chars. Match.
- No-wildcard fast path (exact string match / lowercase comparison): match.
- Trailing-backslash error: both throw/panic. Match.
- `likePatternRe` / `has_wildcards`: TS tests `/_|%|\\/`; Rust checks `contains('%') || contains('_') || contains('\\')`. Match.

---

## Pair 22 — `row-set-signature.ts` / `pipeline-driver.ts #trackRowSetSignatures` ⇄ `engine/mod.rs row_signature_unit` (parent-side)

**PARITY-CONTRACT:** "`rowSetSignature()`: Must match exactly — same bigint after every emitted change and the same `undefined` state for absent signatures. `undefined` must not be coerced to `0n`."

### F-SIG-1 · HIGH · `row_signature_unit` uses FxHasher instead of `h64` → signatures don't match TS
- **TS:** `rowIDSignatureUnit(id)` (`row-set-signature.ts:10`) — `return h64(rowIDString(id))`. `h64` = `hash(s, 2)` = `(xxHash32(s, 0) << 32n) + xxHash32(s, 1)` (`shared/src/hash.ts:4-15`). The input is `rowIDString({schema, table, rowKey})` — a canonical string serialization of the full RowID.
- **Rust:** `row_signature_unit(table, row_key)` (`engine/mod.rs:90-101`) — uses `rustc_hash::FxHasher::default()`, hashes `table` + each `(k, v)` pair in `row_key` via the `Hash` trait, then `hasher.finish()`. Does NOT use `h64` or `rust_cvr::hash::h64`.
- **Evidence that `h64` IS ported:** `rust_cvr::hash::h64` (`hash.rs:25-30`) faithfully mirrors TS `h64`: `(xxh32_seeded(s, 0) << 32) | (xxh32_seeded(s, 1))`. So the correct hash function IS available in Rust — `row_signature_unit` just doesn't use it.
- **Impact:** For any non-empty row-set, TS and Rust produce DIFFERENT `rowSetSignature()` bigints. The PARITY-CONTRACT says "same bigint" — this is a contract violation. The CVR stores `row_set_signature` in the query record; if a TS-computed signature is compared against a Rust-computed one (e.g., during a rolling upgrade or shadow run), every query appears to have changed → false-positive re-hydration. Even in a pure-Rust deployment, the signature values differ from what TS would produce, violating the contract.
- **Also affected:** `sync_engine.rs:1620-1623` calls `rust_ivm::row_signature_unit` — the syncer uses the same wrong function. The comment says "matches `row_signature_unit` byte-for-byte" — true (it calls the same function), but the function itself doesn't match TS.
- **Fix:** Change `row_signature_unit` to use `rust_cvr::hash::h64(row_id_string(id))` (or `rust_cvr::hash::h64(&format!("{{schema='', table, rowKey}}"))`) instead of `FxHasher`.

### F-SIG-2 · Note · `undefined` (absent signature) correctly preserved as `None`
- **TS:** `rowSetSignature(queryID)` returns `bigint | undefined` — `undefined` for a query with no signature.
- **Rust:** `row_set_signature(query_id)` returns `Option<u64>` — `None` for absent. Match (the "undefined must not be coerced to 0n" contract is honored — `None` ≠ `Some(0)`).

### F-SIG-3 · Note · XOR fold logic matches
- **TS:** `#trackRowSetSignatures` (`pipeline-driver.ts:884-896`) — `const cur = this.#rowSetSignatures.get(change.queryID) ?? 0n; const unit = rowIDSignatureUnit({schema: '', table: change.table, rowKey: change.rowKey}); this.#rowSetSignatures.set(change.queryID, cur ^ unit);` — ADD and REMOVE share the same XOR op; EDITs are no-ops.
- **Rust:** `engine/mod.rs:826-828` — `let sig = *self.row_set_signatures.get(&rc.query_id).unwrap_or(&0); let unit = row_signature_unit(&rc.table, &rc.row_key); self.row_set_signatures.insert(rc.query_id.clone(), sig ^ unit);` — same XOR fold, same ADD/REMOVE/EDIT semantics. Match (the fold logic is correct; only the hash function is wrong — F-SIG-1).

### SELF-KILLED
- `row_set_signatures.clear()` on reset/re-execution: both clear. Match.
- `row_set_signatures.remove(queryID)` on query removal: both remove. Match.
- `schema` field missing from Rust hash input: TS includes `schema: ''` in `rowIDString`; Rust doesn't hash `schema`. But since `schema` is always `''` in the processChanges path, this is moot IF `rowIDString` serializes empty-schema identically — but the hash function difference (F-SIG-1) makes this irrelevant anyway.

---

## Pair 21 — `view-syncer.ts #processChanges` ⇄ `rust-cvr/src/change_processor.rs` (CVR lead C-CVR-D, parent-side)

**Verdict: CLEAN — faithful port.** The ref-counting logic (ADD→`refCounts[queryID]++`, EDIT→no change, REMOVE→`refCounts[queryID]--`), the de-dupe buffer keyed by `rowIDString`, the `_0_version` column stripping, and the `CURSOR_PAGE_SIZE` batch flush all match exactly. Resolves C-CVR-D.

### Verified parity (with evidence)
- **refCounts logic:** TS `#processChanges` (`view-syncer.ts:2516-2532`) — `parsedRow.refCounts[queryID] ??= 0;` then ADD→`++`, EDIT→nothing, REMOVE→`--`. Rust `on_row_change` (`change_processor.rs:140-162`) — `entry(query_id).or_insert(0)` then ADD→`+= 1`, EDIT→`or_insert(0)` (ensure exists, no change), REMOVE→`-= 1`. Match.
- **De-dupe buffer:** TS `CustomKeyMap<RowID, RowUpdate>(rowIDString)`; Rust `HashMap<String, (RowID, RowUpdate)>` keyed by `row_id_string(&row_id)`. Match.
- **`_0_version` stripping:** TS `contentsAndVersion(row)` extracts version + strips `_0_version`; Rust `update_version` closure (`:126-139`) filters out `ZERO_VERSION_COLUMN_NAME` into a fresh `Map`. Match.
- **Page-size flush:** TS `if (rows.size % CURSOR_PAGE_SIZE === 0) { await processBatch(); }`; Rust `if self.rows.len().is_multiple_of(self.cursor_page_size) { self.flush_batch(existing_rows); }`. Match.
- **Final flush:** TS `if (rows.size) { await processBatch(); }`; Rust `finish_received()` → `flush_batch()`. Match.
- **`finish` vs `finish_received` separation:** Rust correctly separates `finish_received` (advance-only, no `delete_unreferenced_rows` — matches TS `#advancePipelines`) from `finish` (query-set-change, with `delete_unreferenced_rows` — matches TS `#processQuerySetChanges`). The doc comment at `:190-194` explicitly explains why advance must not run delete. Match.

### F-CP-1 · Note · Yield handling moved to engine layer (architectural, not a divergence)
- **TS:** `#processChanges` loop — `if (change === 'yield') { await timer.yieldProcess('yield in processChanges'); continue; }` — yields are handled inside the processChanges loop.
- **Rust:** `on_row_change` is called per-change from the engine's `FnMut(&RowChange)` callback. No yield sentinel at this layer — the engine's advance loop handles cooperative scheduling before calling the callback.
- **Impact:** Architectural difference — yield handling moved from `#processChanges` to the engine's advance loop. Not a behavioral divergence (yields are still handled, just at a different layer).

### F-CP-2 · Note · `existing_rows` parameter (API difference, not behavioral)
- **TS:** `updater.received(lc, rows)` — takes only `lc` and `rows`.
- **Rust:** `self.updater.received(&self.rows, existing_rows)` — takes `rows` AND `existing_rows`.
- **Impact:** The Rust `CVRQueryDrivenUpdater` doesn't own the row record cache, so it's passed in. TS's updater has internal access. API difference, not behavioral.

### F-CP-3 · Note · `BTreeMap` vs JS object for `ref_counts` (ordering)
- **TS:** `refCounts: {}` (plain JS object, insertion-ordered).
- **Rust:** `ref_counts: std::collections::BTreeMap<String, i64>` (sorted by key).
- **Impact:** If `ref_counts` are serialized or iterated in order, the order differs (insertion vs sorted). But ref_counts are looked up by query_id, not iterated — so this is not observable. `i64` vs JS `number` (float64) is fine for small refcounts.

### SELF-KILLED
- `CustomKeyMap` vs `HashMap`: both key by `rowIDString`. Match.
- `processBatch` → `updater.received()` → route patches to pokers: match.
- `total` tracking: both accumulate `rows.size` per batch. Match.
- The `delete_unreferenced_rows` in `finish`: correctly separated from `finish_received`. Match.

### Resolves C-CVR-D
- The CVR-behavior lead was "mid-verify of `#processChanges` ref-counting + schema handling." Both are now verified faithful. The `_0_version`/`contentsAndVersion` schema handling matches, and the ref-counting matches.

---

## Pair 18 — `ivm/view.ts` + `ivm/view-apply-change.ts` ⇄ `ivm/view.rs` (view-refcounts mandate target, parent-side)

**Verdict:** The refcount-1 move optimization (rc=1 and pos is same/adjacent → edit in place), the edit-plural sort-key-changed path (dec old, inc new, remove if rc=0), and the add/remove singular/plural paths are faithful. But the mutate-mode model diverges significantly.

### F-VIEW-2 · Med · TS has three mutate modes (false/true/WeakSet-COW); Rust has only two (bool) — WeakSet copy-on-write absent
- **TS:** `Mutate = boolean | WeakSet<object>` (`view-apply-change.ts:213`). Three modes: `false` (fully immutable, path-copy), `true` (mutate in place, for initial hydration), `WeakSet` (transaction-scoped copy-on-write: copy on first touch via `owns()`/`track()`, then mutate in place for the rest of the transaction — used during `advance()`).
- **Rust:** `pub type Mutate = bool;` (`view.rs:186`). Two modes only. The WeakSet COW mode is absent. `inc_ref_count`/`dec_ref_count`/`set_relation` all take `_mutate: Mutate` (underscore-prefixed = ignored) and always clone.
- **Impact:** During `advance()` processing, TS uses WeakSet COW to avoid copying the entire tree on every change while still preserving immutability for already-observed nodes. Rust's `Rc::make_mut` provides COW at the `Rc` level, which is functionally similar but coarser-grained (it COWs the individual `Rc<Entry>`, not the path). The observable behavior should match (both produce the same final tree), but the intermediate sharing differs. UNVERIFIED whether any downstream relies on reference identity (TS preserves unchanged-sibling refs; Rust's `set_relation` always clones the parent entry).

### F-VIEW-3 · Low-Med · Rust `inc_ref_count`/`dec_ref_count` always clone, ignoring `mutate`
- **TS:** `setRefCount` (`view-apply-change.ts:874-883`) — `if (mutate || owns(entry)) { entry[refCountSymbol] = count; return entry; }` — mutates in place when `mutate=true` or the entry is owned by the current transaction.
- **Rust:** `inc_ref_count` (`view.rs:916-920`) — `let mut new_entry = (**entry).clone(); new_entry.ref_count += 1; Rc::new(new_entry)` — **always clones**, ignoring `_mutate`.
- **Impact:** When `mutate=true` (initial hydration), TS mutates in place (zero allocation); Rust always allocates a new `Rc<Entry>`. Performance divergence, not correctness — unless the in-place mutation affects observable behavior (it shouldn't during hydration, since the entry is not yet observed). The `_mutate` param being underscore-prefixed confirms it's intentionally unused.

### F-VIEW-4 · Med · `value_to_json_string` for `make_id` — same NaN collision class as PATTERN-B
- **TS:** `make_id` uses `JSON.stringify` (`view-apply-change.ts:855` — `return JSON.stringify(schema.primaryKey.map(k => row[k]))`). `JSON.stringify(NaN)`→`"null"`.
- **Rust:** `value_to_json_string` (`view.rs:837-844`) — `Value::F64(n) => n.to_string()`. `NaN.to_string()`→`"NaN"`.
- **Impact:** Same as PATTERN-B/F-CAP-2/F-EX-2/F-TAKE-1: a primary key containing NaN/Infinity/-0 produces a different `id` in TS vs Rust → different entry tracking → divergent output. This is now the 5th occurrence of PATTERN-B.

### F-VIEW-5 · Low · `Rc::get_mut(...).expect("new entry has refcount 1")` is a panic point TS doesn't have
- **Rust:** `view.rs:421` — after `inc_ref_count` returns a fresh clone, `Rc::get_mut(&mut new_entry).expect("new entry has refcount 1")` is called. Since it's a fresh clone, `get_mut` should always succeed (Rc refcount 1). But if another `Rc` clone is outstanding (bug), `get_mut` returns None → panic.
- **TS:** no equivalent failure mode (just mutates the object directly).
- **Impact:** Low — the `expect` should never fire in correct code (the clone is fresh). But it's an extra panic surface that TS doesn't have.

### F-VIEW-6 · Note · `rowSetSignature` lives in a different file pair (pipeline-driver ⇄ engine/mod.rs)
- `rowSetSignature` is NOT in `view-apply-change.ts` or `view.rs`. TS: it's in `pipeline-driver.ts` (referenced in `pipeline-driver.test.ts:1054`). Rust: it's in `engine/mod.rs:370-371` as `row_set_signatures: HashMap<String, u64>` with `row_signature_unit` (`:90`). The PARITY-CONTRACT's `rowSetSignature()` "Must match exactly" + "undefined must not be coerced to 0n" targets that pair, not this one. Deferred to the pipeline-driver ⇄ engine/mod.rs pair.

### SELF-KILLED
- Refcount-1 move optimization (rc=1 + pos same/adjacent → edit in place): both have it. Match.
- Edit-plural sort-key-changed (dec old, inc new, remove if rc=0): both have the full path. Match.
- Add singular (rc=1 new or inc existing): match.
- Remove singular (rc==1 → remove, rc>1 → dec): match.
- Remove plural `remove_and_update_ref_count`: match.
- `initialize_relationships_for_new_entry_if_any`: both initialize child relationships on new entries. Match.
- Child-array refcount increment (`child_array[raw_pos].ref_count += 1`): both do it. Match.

---

## Pair 20 — `workers/syncer.ts` (`#createConnection`/drain) ⇄ `router.rs` (token-pinning mandate target, parent-side)

**Verdict:** The connection-admission, token-pinning, auth-equality, connection-replacement, and drain paths are faithful to TS `Syncer.#createConnection` + `Syncer.drain`. The `authEquals` raw-token comparison (the F-CCM-1 note) is confirmed correct. 4 findings, mostly documented architectural adaptations.

### F-RT-1 · Note (resolves F-CCM-1 note) · `authEquals` compares raw token string — matches TS
- **TS:** `authEquals` (auth.ts, via connection-context-manager.ts:349) compares the **raw** token string for both opaque and JWT auth.
- **Rust:** `handle_update_auth` (`router.rs:2567-2576`) — `let unchanged = self.client_raw_auth.get(client_id).map(|prev| prev == token).unwrap_or(false);`. Comment explicitly explains why raw (not decoded): "Comparing decoded JWT claims here (the old behavior) wrongly treated an OPAQUE token refresh as unchanged — opaque tokens carry no claims, both decode to `{}`, so a `token-1`→`token-2` swap was skipped."
- **Impact:** Match. The F-CCM-1 note's claim is confirmed: production auth-change detection compares raw, matching TS. The F-CCM-1 issue (decoded claims not surfaced to message handler) is a *separate* concern from auth-equality.

### F-RT-2 · Med · `validate()` gap confirmed (cross-ref F-TQ-2): auth maintenance is local-only
- **Rust:** `arm_auth_maintenance` (called at `:2010`) re-validates the JWT **locally** via `auth_validator.validate_auth` (signature verification + `sub` check). No empty `/query` POST to the API server.
- **TS:** `view-syncer.ts:2753` calls `validate()` which sends an empty `/query` to surface server-side revocation.
- **Impact:** Same as F-TQ-2: a JWT valid locally but revoked server-side keeps the connection alive until natural expiry. Confirmed at the router level — this is the production path, not just the reference module.

### F-RT-3 · Med (doc-verify) · Drain adds a 25s hard deadline (TS has none)
- **TS:** `Syncer.drain` — `while (this.#viewSyncers.size) { await this.#drainCoordinator.forceDrainTimeout; … }` with no explicit deadline; relies on `forceDrainTimeout` pacing.
- **Rust:** `drain` (`router.rs:1019-1064`) — adds `MAX_DRAIN_MS = 25_000` deadline; if exhausted, "rehoming remaining groups at once" + `shutdown().await`. Pre-scales the interval to fit the budget.
- **Impact:** Rust bounds the drain duration; TS does not. If a CG is stuck, TS drains indefinitely; Rust rehomes after 25s. Documented as deliberate (deploy-timeout safety). Verify registered as intentional.

### F-RT-4 · Low · `check_and_pin_user` returns `Result<(), ()>` (no error message)
- **TS:** `validateConnection` constructs a full `ProtocolError({kind: Unauthorized, message: 'Client groups are pinned…', origin: ZeroCache})`.
- **Rust:** `check_and_pin_user` (`:382-392`) returns `Result<(), ()>` (unit error); the caller constructs the `ErrorBody::unauthorized(...)` message at `:724`. Equivalent (the message is built at the call site), just a different factoring.

### F-RT-5 · Note · Admission ordering faithful (DoS prevention)
- **TS:** `#createConnection` — verifies JWT BEFORE checking existing connections / pinning ("prevents unauthenticated attackers from force-disconnecting legitimate users via DoS").
- **Rust:** `router.rs:655-685` — `validate_auth` (step 1) BEFORE `get_or_create_cg` (step 2) BEFORE `check_and_pin_user` (step 3) BEFORE close-existing (step 4). Same ordering, with the same DoS-prevention comment. Match.

### SELF-KILLED
- `pinned_user_id` first-connection bind: both pin on first connection. Match.
- Connection replacement (close existing for same clientID): both close-then-register. Match.
- `group_auth_states` stale-entry pruning (Rust `retain` on live CG handles): Rust-specific cleanup, not a divergence.
- `client_auth` (decoded claims) + `client_raw_auth` (raw token) dual storage: Rust keeps both; TS keeps `Auth` object with both. Match (the issue is only the `ConnContextInfo` boundary — F-CCM-1).
- Rehome-on-capacity-overflow (instead of hard reject): matches TS "never rejects for capacity; drains/rehomes."

---

## Cross-cutting pattern — systemic yield + value-serialization divergences across IVM operators

**Two divergence classes appear repeatedly across cap, exists, and take — they are systemic, not per-operator.**

### PATTERN-A · `skip_yields` vs TS yield propagation (Med, systemic)
- **TS:** all IVM operators propagate `yield` sentinels from cooperative-scheduling fetch loops to the push caller (e.g. `for (const node of this.#input.fetch(...)) { if (node === 'yield') { yield node; continue; } … }`).
- **Rust:** several push-path fetches use `skip_yields(...)` which silently drops `StreamItem::Yield`.
- **Affected:** cap (`cap.rs:459`, F-CAP-3), exists (`exists.rs:95-103`, F-EX-1), take (`take.rs:518,620,818`, F-TAKE-2). Note: initial-hydration fetches in take (`:359`) and the `Filter::fetch` (`filter.rs:74-76`) DO propagate yields — so the divergence is specifically in the **push-path re-fetches**, not all fetches.
- **PARITY-CONTRACT:** "recorded `yield` positions remain part of the exact stream trace" and `addQuery()` requires "Same ordered row-change stream, including yield sentinels."
- **Impact:** If the 1822-oracle records yields during `advance()` pushes (UNVERIFIED — the `advance()` row doesn't explicitly mention yields, unlike `addQuery()`), every cap/exists/take push-path re-fetch diverges. If the oracle only records yields during `addQuery()` hydration, the impact is limited to initial-hydration fetches (which mostly DO propagate).

### PATTERN-B · Value serialization: `JSON.stringify` (TS) vs `format!("{:?}", …)` / `to_string()` (Rust) (Med, systemic)
- **TS:** uses `JSON.stringify` for PK sets, partition keys, cache keys → `NaN`→`"null"`, `Infinity`→`"null"`, `-0`→`"0"`.
- **Rust:** uses `to_string()` (cap `:529-537`) or `format!("{:?}", …)` (exists `:25-30`, take `:227-235`) → `NaN`→`"NaN"`, `Infinity`→`"inf"`, `-0`→`"-0"`.
- **Affected:** cap (F-CAP-2, PK + partition-key collision), exists (F-EX-2, cache-key collision), take (F-TAKE-1, state-key collision).
- **PARITY-CONTRACT:** "non-finite numbers, negative zero… are never normalized." TS's `JSON.stringify` normalizes them (collapses NaN/Infinity→null, -0→0); Rust's formatters don't. So TS collides values that Rust keeps distinct.
- **Impact:** For NaN/Infinity/-0 in a PK, partition key, or cache key, TS and Rust produce different membership/tracking → divergent output. The 1822-oracle plausibly misses this because these values rarely appear in PKs. The most likely surfacing path is the exists drain (F-CAP-2 note: NaN PK → un-re-fetchable in TS → `fetchSize` returns `size-1`).
- **Fix direction:** Rust should either match TS's `JSON.stringify` normalization (collapsing NaN/Infinity/-0) for parity, OR — better — both sides should use a canonical value serializer that doesn't collide. The PARITY-CONTRACT says these values "are never normalized," which suggests TS's `JSON.stringify` behavior is the bug and Rust's distinct serialization is more correct — but for **parity**, Rust must match TS until a two-driver regression is recorded.

---

## Pair 16 — `ivm/take.ts` ⇄ `ivm/take.rs` (timed out, re-diffed parent-side)

**Verdict:** Core take logic is faithful — the ADD-at-capacity (remove bound, add new), REMOVE (promote row n+1), EDIT 6-branch (oldCmp × newCmp matrix), `row_hidden_from_fetch` overlay, `limit === 0`, and the partition-key assertion are all ported. Unlike Cap (F-CAP-1), Take HAS the partition-key-unchanged assertion (`take.rs:682-686`). Two findings, both the same classes as cap/exists.

### F-TAKE-1 · Med · State key serialization: TS `JSON.stringify(['take',…])` vs Rust `"{col}={value:?};"` — same NaN collision class
- **TS:** `getTakeStateKey` (`take.ts:545-557`) — `JSON.stringify(['take', ...partitionValues])`. `JSON.stringify(NaN)`→`"null"`.
- **Rust:** `take_state_key_for_row` (`take.rs:227-235`) — `write!(key, "{}={:?};", col, value)`. `format!("{:?}", NaN)`→`"NaN"`.
- **Impact:** Same as F-CAP-2/F-EX-2: two partitions with `NaN` and `null` share a state entry in TS but not Rust → different bound tracking → different row acceptance → divergent output. The `{:?}` Debug format also differs from `JSON.stringify` for other edge values (e.g. `-0`, `Infinity`).

### F-TAKE-2 · Med · `skip_yields` in push-path fetches: TS propagates yields, Rust drops them
- **TS:** all push-path fetch loops propagate yields — e.g. `for (const node of this.#input.fetch({start:{row:bound,basis:'at'},constraint,reverse:true})) { if (node === 'yield') { yield node; continue; } … }`.
- **Rust:** `take.rs:518,620,818` — `skip_yields(self.input.borrow().fetch(&req))` in the boundNode/beforeBoundNode/afterBoundNode lookups during push.
- **Impact:** Same as F-CAP-3/F-EX-1: if the input stream yields during these push-path fetches, TS propagates the yields interleaved with the push changes; Rust drops them. PARITY-CONTRACT: "recorded `yield` positions remain part of the exact stream trace."
- **Note:** Rust's `initialFetch` (line 359) DOES propagate `StreamItem::Yield` — so the divergence is only in the push-path fetches, not the initial hydration fetch.

### SELF-KILLED
- Partition-key assertion in EDIT: Rust HAS it (`take.rs:682-686`). Match (unlike Cap which omits it — F-CAP-1).
- `row_hidden_from_fetch` overlay: Rust has it with `HiddenRowGuard` RAII (`take.rs:168,892-900`). Match.
- `limit === 0`: both return early. Match.
- `limit === 1` special case: both have it. Match.
- `initialFetch` yield propagation: both propagate. Match.
- `assertOrderingIncludesPK`: both assert sorted input. Match.
- `assert(limit >= 0)`: both assert. Match.
- The 6-branch EDIT matrix (oldCmp=0/`<0`/`>0` × newCmp=0/`<0`/`>0`): Rust has all branches. Match.
- `maxBound` tracking: both update on `setTakeState`. Match.
- The `downstreamEarlyReturn` assert in `initialFetch`: both assert no early return. Match.

---

## Pair 15 — `ivm/exists.ts` ⇄ `ivm/exists.rs` (timed out, re-diffed parent-side)

**Verdict:** The 0→1 / 1→0 transition logic is faithful (NOT-EXISTS remove-with-empty-relationship, EXISTS add, EXISTS remove-with-removed-child). The `#inPush` re-entrancy assert + RAII guard is faithful. But 3 divergences, 2 of which are the same class as the cap findings (cross-pair consistency).

### F-EX-1 · Med · `fetchSize` yield divergence: TS propagates yields, Rust filters them
- **TS:** `#fetchSize` (`exists.ts:248-260`) — `for (const n of relationship()) { if (n === 'yield') { yield 'yield'; } else { size++; } }`. Yields propagate to the push caller.
- **Rust:** `fetch_size` (`exists.rs:95-103`) — `.filter(|i| matches!(i, StreamItem::Data(_))).count()`. Yields are silently dropped.
- **Impact:** `fetchSize` is called during `push` for the 0→1/1→0 transition check. If the relationship stream yields (cooperative scheduling), TS propagates those yields interleaved with the push changes; Rust drops them. PARITY-CONTRACT: "recorded `yield` positions remain part of the exact stream trace." Same class as F-CAP-3. The cap subagent flagged this exact divergence from the outside ("Rust `fetch_size` in `exists.rs:95-103` also filters out yields, while TS `#fetchSize` propagates yields").

### F-EX-2 · Med · Cache key serialization: TS `JSON.stringify(normalizeUndefined(…))` vs Rust `format!("{:?}", …)`
- **TS:** `#getCacheKey` (`exists.ts:224-229`) — `JSON.stringify(values)` where `values` use `normalizeUndefined(node.row[key])` (converts `undefined`→`null`).
- **Rust:** `get_cache_key` (`exists.rs:25-30`) — `format!("{:?}", node.row.get(k).unwrap_or(&Value::Null))` joined with `\x00`.
- **Impact:** Same NaN/Infinity/-0 collision class as F-CAP-2: TS `JSON.stringify(NaN)`→`"null"` collides with actual `null`; Rust `format!("{:?}", NaN)`→`"NaN"` doesn't. Two parent rows with `NaN` and `null` in the join key share a cache entry in TS but not Rust → one may be incorrectly filtered. Plus the separator differs (`JSON.stringify` array vs `\x00`-joined), but that's internal. The `normalizeUndefined` → `null` conversion has no Rust equivalent needed (serde_json doesn't produce `undefined`).

### F-EX-3 · Med (same as F-FO-1) · `beginFilter`/`endFilter` cache lifecycle replaced with clear-on-fetch
- **TS:** `Exists` is a `FilterOperator` that plugs into the `FilterStart`/`FilterEnd` sub-graph (PR #4339). `endFilter()` clears the cache; `beginFilter()` forwards to the downstream output. The cache is valid within a begin/endFilter cycle.
- **Rust:** `Exists` is a standalone `Input` + `Output` adapter. The cache is cleared at the start of `fetch()` via `try_borrow_mut().clear()`. No `beginFilter`/`endFilter` protocol.
- **Impact:** Same architectural divergence as F-FO-1. The cache lifecycle is different (TS: per begin/endFilter cycle; Rust: per fetch call) but functionally similar for the common case. The `try_borrow_mut` (non-panicking) means a re-entrant fetch skips the clear — which is the correct behavior for nested fetches.

### SELF-KILLED
- 0→1 add transition (NOT EXISTS → remove with empty relationship): match (`set_relationship(&rel_name, empty_rel())`).
- 1→0 remove transition (EXISTS → remove with removed child): match (`rel_from_vec(vec![removed_child_node])`).
- `#inPush` re-entrancy assert: match (Rust `assert!(!e.in_push.get(), …)` + `InPushGuard` RAII).
- `#noSizeReuse` (parentJoinKey == primaryKey): match (`parent_join_key == schema.primary_key`).
- Child change for different relationship / edit/child child changes → pushWithFilter: match.
- Cache skip during push: TS `#filter` checks `!this.#inPush`; Rust push path calls `fetch_size` directly (no cache). Equivalent.

---

## Pair 19 — `ivm/filter-operators.ts` ⇄ `ivm/filter_operators.rs` + `filter.rs` (parent-side)

**Architectural divergence:** the TS `FilterStart`/`FilterEnd`/`FilterOperator` sub-graph (PR #4339) is **replaced** in Rust by a single `Filter` struct + `FilterOutputAdapter` (`filter.rs`). The `filter_operators.rs` file exists but `FilterStart::fetch` is a **stub that passes through** and `FilterEnd` has no `filter()` method.

### F-FO-1 · Med · `FilterStart::fetch` is a stub — the begin/filter/endFilter protocol is dropped
- **TS:** `filter-operators.ts:99-113` — `FilterStart.fetch` calls `beginFilter()`, iterates the input, calls `filter(node)` for each (yielding the node only if filter returns true), and `endFilter()` in a `finally` block. This is the whole point of PR #4339 — the FilterOperator sub-graph enables efficient OR handling with per-loop caching.
- **Rust:** `filter_operators.rs:69-76` — `FilterStart::fetch` comment says "In a full implementation, this calls begin_filter, filters each node through the filter chain, then end_filter. For now, pass through (the Filter operator handles this directly)" — and just does `input.fetch(req)` unfiltered.
- **Impact:** The `Filter` struct (`filter.rs:68-82`) DOES filter via `filter_map` in its own `fetch`, so filtering works. But the `begin_filter`/`endFilter` **caching protocol** (the optimization PR #4339 introduced for OR conditions) is entirely absent. No `FilterOperator` chain, no per-loop result caching. For queries with OR in the where clause, Rust may be slower but produces correct results — unless the caching affected observable behavior (not just perf), which is UNVERIFIED.

### F-FO-2 · Low · `build_filter_pipeline` is a stub — doesn't take a `pipeline` closure or `delegate`
- **TS:** `filter-operators.ts:152-161` — `buildFilterPipeline(input, delegate, pipeline)` takes a `BuilderDelegate` and a `pipeline` closure that constructs the filter chain, wires edges via `delegate.addEdge`.
- **Rust:** `filter_operators.rs:140-144` — `build_filter_pipeline(input)` returns `(start, end)` with no delegate, no pipeline closure, no edge wiring. Stub.
- **Impact:** The filter pipeline construction is done differently (via the `Filter` struct directly). The stub `build_filter_pipeline` is dead code or future scaffolding.

### F-FO-3 · Note · `Filter::fetch` propagates `Yield` (parity with TS FilterStart yield propagation)
- **TS:** `filter-operators.ts:104-106` — `if (node === 'yield') { yield node; continue; }`.
- **Rust:** `filter.rs:74-76` — `StreamItem::Yield => Some(StreamItem::Yield)`. Match (yields propagated, unlike cap's `skip_yields`).

### SELF-KILLED
- `FilterEnd.filter() → true`: TS `filter-operators.ts:138` returns true (pass-through); Rust `FilterEnd` has no `filter` method but since `FilterStart::fetch` is a stub, the FilterEnd path isn't used. Equivalent (both are inert in the current architecture).
- `throwFilterOutput`: TS throws if push/filter called before set; Rust uses `Option<OutputHandle>` and silently drops. Different error behavior but only on misconfiguration.

---

## Pair 14 — `ivm/cap.ts` ⇄ `ivm/cap.rs` (subagent bg_bf579907)

**Verdict:** Core ADD/REMOVE/CHILD/EDIT logic, boundary conditions (`size < limit`), and the remove-then-refill sequence are structurally faithful. `limit === 0` observable behavior is identical (internal state representation differs but all push paths drop identically). But 4 real divergences, all in edge cases the ivm-behavior mandate specifically targeted.

### F-CAP-1 · Med · EDIT branch: Rust omits the partition-key-unchanged assertion → silent state corruption
- **TS:** `cap.ts:261-268` — `assert(!this.#partitionKeyComparator || this.#partitionKeyComparator(change[OLD_NODE].row, change[NODE].row) === 0, 'Unexpected change of partition key')`. Comparator created in constructor (`:74`).
- **Rust:** `cap.rs:350-384` — comment says "check if partition key changed (should not for Cap)" but **no check is performed**. The `Cap` struct has no `partition_key_comparator` field; `make_partition_key_comparator` (in `take.rs:1036`) is never imported.
- **Impact:** If an edit changes a partition-key column: TS **throws**; Rust **silently proceeds** — computes `state_key` from `old_node.row` (old partition), finds `old_pk` there, replaces with `new_pk` computed from `node.row`. The new PK is stored in the **old partition's** cap state while the row belongs in the **new partition's** state (never updated). Silent state corruption: old partition tracks a row that no longer belongs; new partition is missing a row it should track. Subsequent fetches/pushes diverge. The 1822-oracle plausibly misses this because the contract says partition keys don't change in edits.

### F-CAP-2 · Med · NaN/Infinity/-0 value serialization: TS `JSON.stringify` collides, Rust `to_string` doesn't → divergent PK + partition-key matching
- **TS:** `cap.ts:312,316` — `getCapStateKey`/`serializePK` use `JSON.stringify`. `JSON.stringify(NaN)`→`"null"`, `JSON.stringify(Infinity)`→`"null"`, `JSON.stringify(-0)`→`"0"`.
- **Rust:** `cap.rs:529-537` — `value_to_string` maps `Value::F64(n)`→`n.to_string()`, producing `"NaN"`, `"inf"`, `"-inf"`, `"-0"`.
- **Impact (3 facets):**
  1. **PK collision:** TS: rows with PK `[NaN]` and `[null]` both serialize to `"[null]"` → cap treats as same row (double-add creates duplicate `pks`, `indexOf` finds first). Rust: distinct. Same for `Infinity`↔`null`, `-0`↔`0`.
  2. **Partition-key collision (more severe):** TS: partition with value `NaN` and partition with `null` both produce `'"[\"cap\",null]"'` → **share cap state** (row counts leak across partitions). Rust: separate states.
  3. **Re-fetch round-trip:** TS `deserializePKToConstraint` (`:319-327`) uses `JSON.parse`; a PK serialized as `"[null]"` (originally NaN) deserializes back to `null` → per-PK re-fetch (`:114-115`) looks up `null`, **fails to find the row**. Rust `parse_value` (`:556-571`) parses `"NaN"` back to `Value::F64(NaN)` correctly. This surfaces in the exists drain: `fetchSize` returns `size - 1` in TS vs `size` in Rust.
- **PARITY-CONTRACT link:** explicitly states "non-finite numbers, negative zero… are never normalized." A NaN/Infinity/-0 in a PK or partition key is exactly the branch the oracle plausibly misses.

### F-CAP-3 · Low-Med · Refill during REMOVE: TS propagates `yield` sentinels, Rust discards via `skip_yields`
- **TS:** `cap.ts:226-230` — `for (const node of this.#input.fetch({constraint})) { if (node === 'yield') { yield node; continue; } … }`. The `yield node` propagates the cooperative-scheduling sentinel to the `push` generator's caller.
- **Rust:** `cap.rs:459` — `for n in crate::ivm::stream::skip_yields(cap.input.borrow().fetch(&fetch_req))`. `skip_yields` filters out `StreamItem::Yield`.
- **Impact:** PARITY-CONTRACT: "recorded `yield` positions remain part of the exact stream trace" and `addQuery()` requires "Same ordered row-change stream, including yield sentinels." If the oracle records yields during `advance()` pushes, traces diverge. UNVERIFIED whether the oracle records yields during advance (the `advance()` row doesn't explicitly mention yields, unlike `addQuery()`).

### F-CAP-4 · Low · Per-PK re-fetch: Rust clones full `FetchRequest` (preserving `multi_constraints`); TS creates fresh `{constraint}`-only
- **Rust:** `cap.rs:321-323` — `let mut fetch_req = req.clone(); fetch_req.constraint = Some(constraint);`.
- **TS:** `cap.ts:115` — `this.#input.fetch({constraint})` (fresh request, only constraint).
- **Impact:** If `req.multi_constraints` is set, Rust ANDs them with the per-PK constraint (may return no rows if conflicting); TS does a clean PK-only lookup. Likely unreachable (cap is built for non-flipped EXISTS subqueries; `multiConstraints` is used by FlippedJoin). UNVERIFIED.

### SELF-KILLED (subagent)
- Per-PK fetch order: both iterate `pks` in array order. No order divergence.
- `destroy` clearing output handle: Rust-specific Rc cycle break, not behavioral.
- `getCapStateKey` signature (union vs two Options): callers pass same logical values.
- `pks` array aliasing in TS refill: analyzed re-entrant fetch possibility — no yield between `pks.push()` and second `set()`. Rust clones `pks` (safer, observably equivalent).
- Duplicate PK entries: both allow on double-add, both use first-match removal.

### UNVERIFIED (subagent)
- Rust `f64::to_string()` for `-0.0`: asserted produces `"-0"` (if it produces `"0"`, the -0 sub-case of F-CAP-2 self-kills but NaN/Infinity remain).
- Whether the 1822-oracle records `yield` sentinels during `advance()` pushes (F-CAP-3 impact).
- Whether `multi_constraints` is ever set in a cap fetch request (F-CAP-4 reachability).
- BigInt in PKs: TS `JSON.stringify(BigInt)` throws; Rust has no BigInt variant. UNVERIFIED whether BigInts can appear in PKs.
- Large/small float formatting (e.g. `1e21`): TS `"1e+21"` vs Rust likely `"1000000000000000000000"`. Unlikely to cause collisions in practice.

### Note (exists-drain interaction)
- `exists.#fetchSize` (`exists.ts:248-260`) fully drains the cap's `fetch` output and counts `Data` items. F-CAP-2 (NaN round-trip failure) directly impacts this: a row with a NaN PK accepted during initial hydration becomes un-re-fetchable in TS → `fetchSize` returns `size - 1` (TS) vs `size` (Rust). This is the most likely path for the NaN divergence to surface in a driver trace.
- Rust `fetch_size` in `exists.rs:95-103` also filters out yields, while TS `#fetchSize` propagates yields — an exists-level yield divergence parallel to F-CAP-3 (outside the cap file pair).

---

## Pair 17 — `ivm/join.ts` ⇄ `ivm/join.rs` (join-symmetry mandate target, parent-side)

**Verdict: CLEAN — faithful port.** Verified the full join core: the overlay logic (in-progress-child-change), constraint building, parent/child fetch, and both edit assertions.

### F-JOIN-1 · Note · Join symmetry preserved exactly
- **TS:** `#processParentNode` overlay condition: `schema.compareRows(parentNodeRow, inprogressChildChangePosition) > 0` (parent sorted AFTER the in-progress position) → overlay the in-progress change into the child stream.
- **Rust:** `join.rs:~150` — `compare(&parent_row_for_closure, pos) == CmpOrdering::Greater` → same overlay. Match.
- The unordered (`schema.sort === undefined` → `generateWithOverlayUnordered`) vs sort (`generateWithOverlay`) branch: match (`join.rs:~155-165`).

### F-JOIN-2 · Note · `parent !== child` assertion: `Rc::ptr_eq` vs reference inequality
- **TS:** `assert(parent !== child, 'Parent and child must be different operators')`.
- **Rust:** `assert!(!Rc::ptr_eq(&args.parent, &args.child), …)`. Match (Rc pointer equality == TS reference equality).

### F-JOIN-3 · Note · In-progress guard: RAII Drop vs try/finally
- **TS:** `try { … } finally { this.#inprogressChildChange = undefined; }`.
- **Rust:** `InprogressGuard` struct with `Drop` impl that clears both `inprogress_child_change` and `inprogress_child_change_position`. Equivalent (RAII is the idiomatic Rust translation of try/finally; clears on panic too).

### F-JOIN-4 · Note · `destroy` breaks Rc cycle (Rust-specific, not a divergence)
- **Rust:** `destroy` clears `*self.output.borrow_mut() = None` to break the Rc cycle (TS has GC so no cycle concern). Documented Rust-specific memory management, not a behavioral divergence.

### SELF-KILLED
- `parentKey.length === childKey.length` assertion: both assert. Match.
- `rowEqualsForCompoundKey` parent/child edit assertions: both assert "must not change relationship." Match.
- `buildJoinConstraint` returning `None` → empty stream: match.
- `pushChildChange` setting `inprogress_child_change_position = parentNode.row` inside the parent fetch loop: match.

---

## Pair 18 — `ivm/view.ts` + `ivm/view-apply-change.ts` ⇄ `ivm/view.rs` (view-refcounts mandate target)

**Split discovery:** `view.ts` (31 LOC) is just TYPE definitions (`View`, `Entry`, `ViewFactory`) — no logic. The actual view refcount logic is in `view-apply-change.ts` (926 LOC) ⇄ `view.rs` (1017 LOC, which absorbed it). The Rust `view.rs` has explicit `ref_count` tracking (`Entry.ref_count`, `inc_ref_count`/`dec_ref_count`, `Rc::get_mut`).

### F-VIEW-1 · DEFERRED · `view-apply-change.ts` ⇄ `view.rs` refcount logic not yet body-diffed
- The 926↔1017 LOC pair is too large for a quick parent-side diff. The Rust `view.rs` has ref_count tracking at lines 58/485/515/545/552/682 — the mandate's "view refcounts" target. Deferred to a dedicated subagent (budget-bounded).
- **Initial spot-check (positive):** Rust uses `Rc::get_mut(&mut new_entry).expect("new entry has refcount 1")` (`:421`) which mirrors TS's refcount-1 optimization (mutate in place when refcount==1, clone when >1). The refcount decrement + removal path (`:515-552`) has the `ref_count == 1` → remove vs `> 1` → decrement branch. Structurally matches TS refcount semantics at a glance.

---

## Cross-cutting finding — the "32 unresolved behavioral IVM symbols" are client-side builder DSL (mostly NOT a gap)

**Answers the ivm-missing mandate:** "whether the 32 'out-of-remit' symbols are truly server-unreachable."

### F-IVM-X1 · Note · MAP-ivm's 32 flagged behavioral symbols are mostly client-side query-builder DSL helpers, legitimately server-unreachable
- **MAP-ivm.md claim:** "🟥 TS UNRESOLVED: 108 (32 behavioral ⇒ investigate · 76 structural…)" and lists them: `asQueryImpl`, `asQueryInternals`, `cmpLit`, `DeepMerge`, `defineQueries`, `defineQuery`, `eb`, `filterFalse`, `filterTrue`, `filterUndefined`, `getQuery`, `isCompoundKey`, `isOneHop`, `isParameterReference`, `isQuery`, `isQueryDefinition`, `isQueryInternals`, `isQueryRegistry`, `isTwoHop`, `materializeImpl`, `mustGetQuery`, `newQuery`, `newQueryImpl`, `normalizeParser`, `normalizeTTL`, `preloadImpl`, `syncedQueryImpl`, `throwQueryNotRunnable`, `titleCase`, `withValidation`.
- **Verification (parent-side, direct):**
  - `cmpLit`, `filterTrue`, `filterFalse`, `filterUndefined`, `eb` are all defined in `packages/zql/src/query/expression.ts` — the **client-side query-builder DSL** (the `eb` expression-builder context passed to `.where()` callbacks). They produce AST `Condition` objects that get serialized and sent to the server. The server receives the already-built AST.
  - `defineQuery`, `defineQueries`, `getQuery`, `mustGetQuery`, `isQuery`, `isQueryRegistry`, `newQuery`, `withValidation`, `normalizeParser`, `syncedQueryImpl` are all in `query/query-registry.ts` / `query/query-impl.ts` / `query/named.ts` — the **client-side query-definition/registry API**.
  - `isOneHop`, `isTwoHop`, `isCompoundKey` are client-side AST shape predicates.
- **Conclusion:** These are **legitimately server-unreachable** — they're builder/registry API that runs in the client (browser/Node) to construct queries, not runtime IVM engine logic. The Rust server crates receive the serialized AST and don't need the construction helpers. The MAP's "32 behavioral ⇒ investigate" flag over-alarms by classifying client-side DSL as behavioral.
- **Caveat (UNVERIFIED):** `normalizeTTL` (`packages/zql/src/query/ttl.ts:62`) MIGHT be server-reachable if the server normalizes TTLs from client input. The cvr-behavior lead C-CVR-B found `parse_ttl`/`clamp_ttl`/`compare_ttl` (enum form) ARE ported and match. Whether `normalizeTTL` (string→enum form) is needed server-side is unverified — but C-CVR-B indicated `parse_ttl_string` is harness-only. Likely legitimately server-unreachable too.
- **Action:** No porting work needed for the 32; recommend the MAP re-classify them from "behavioral ⇒ investigate" to "client-side DSL (expected)."

---

## Pair 13 — `services/view-syncer/connection-context-manager.ts` ⇄ `…/connection_context_manager.rs` (subagent bg_bc061840)

**Verdict:** Reference module is a faithful port of auth/lifecycle (10+ symbols match including type-pinning, opaque-same shortcut, JWT sub/iat comparison, stale-revision guards, insertion-order tiebreakers). BUT it is explicitly NOT WIRED to production — `main.rs:733` installs `PlaceholderConnContextManager` (returns `auth: None` always). This resolves the F-SW-2 crux.

### F-CCM-1 · CRITICAL (resolves F-SW-2) · Reference module carries decoded claims, but production boundary is raw token string
- **Rust reference:** `connection_context_manager.rs:82-94` — `Auth::Jwt { raw, decoded: JwtPayload }` DOES carry decoded JWT claims (parity with TS `ConnectionContext.auth`).
- **Rust production:** `main.rs:733` installs `PlaceholderConnContextManager`; `main.rs:868-877` returns `ConnContextInfo { auth: None, revision: 0 }` always. The production boundary consumed by `syncer_ws_message_handler` is `ConnContextInfo.auth: Option<String>` (`syncer_ws_message_handler.rs:69-72`) — typed as a **raw token string**, no field for decoded claims.
- **TS:** `connection-context-manager.ts:72` — `auth: Auth | undefined`; handler passes `auth.decoded` to mutagen.
- **Resolves F-SW-2:** The F-SW-2 finding ("Rust passes `{token: raw}") is a property of the production `ConnContextInfo` boundary + `CgState.client_raw_auth` (`router.rs:1394`), NOT of this reference module. The reference module has decoded claims available (matching TS), but the production dispatch boundary can't carry them. **The fix requires widening `ConnContextInfo.auth` to carry decoded claims (or wiring the reference module through the dispatch trait).**
- **Note:** Production auth-change detection (`router.rs:2556-2592` `handle_update_auth`) compares the **raw** token string, matching TS `authEquals` (the comment at `router.rs:2556-2565` explicitly notes decoded-claim comparison was the old buggy behavior). Decoded claims exist in `CgState.client_auth` (`router.rs:2579`) but aren't surfaced through `ConnContextInfo`.

### F-CCM-2 · HIGH (dormant security) · `init_connection` does not filter client headers through the allowlist
- **Rust:** `connection_context_manager.rs:479-503` — `header_options.custom_headers = Some(headers.clone())` (full unfiltered map). No `filter_headers` counterpart exists in this module (TS `filterHeaders` at `:876-891` has no equivalent).
- **TS:** `:305-340` — `userQueryHeaders` filtered via `filterHeaders(body.userQueryHeaders, this.#queryConfig?.allowedClientHeaders)` and `userPushHeaders` via `filterHeaders(body.userPushHeaders, this.#pushConfig?.allowedClientHeaders)`.
- **Impact:** Header-injection surface. Mitigated today because `build_fetch_context`/`init_connection` are off the runtime fetch path (`:51-57`); the real path `router.rs::default_query_context` (`:1255-1273`) implements #6144 filtering correctly. But the module's stated purpose is future promotion to production (`:8-12`), at which point the missing filter goes live.

### F-CCM-3 · MED (documented, dormant) · `build_fetch_context` stores allowlist config, not filtered incoming headers
- **Rust:** `:445-476` stores `allowed_client_headers: config.allowed_client_headers.clone()`; `HeaderOptions` struct (`:64-72`) has `allowed_client_headers`, no `request_headers` field.
- **TS:** `:240-258` stores `requestHeaders: filterHeaders(connectParams.requestHeaders, config?.allowedRequestHeaders)`.
- **Impact:** The reference `ConnectionFetchContext` cannot carry #6144 forwarded header values; a promoted consumer would lose incoming-header forwarding. Runtime unaffected today.

### F-CCM-4 · LOW (reference-only) · `updateAuth`/`resolve_auth` are sync, take no LogContext
- **Rust:** `:505-539` sync `update_auth`; `:219` `resolve_auth` has no `lc`; `:368` `LegacyJwtValidator` is sync `Fn(&str, Option<&str>) -> Result<Auth, CCMError>`.
- **TS:** `:344` async `updateAuth`; passes `this.#lc` into `resolveAuth` (so async JWKS verification + logging possible).
- **Impact:** If promoted, the sync signature would block an executor thread on JWT verification. Dormant today.

### SELF-KILLED (subagent)
- `updateAuth` third-branch reference-equality vs structural-equality: TS re-stores on any new object reference; Rust re-stores only on structural inequality. The extra TS re-store is a no-op map-set of structurally identical data → observable behavior identical.
- `CCMError::Unauthorized` origin: verified `ErrorBody::unauthorized` sets `origin: Some(ErrorOrigin::ZeroCache)` (`protocol.rs:265-271`).

### UNVERIFIED (subagent)
- TS `Auth`/`authEquals`/`resolveAuth` exact shapes live in `auth.ts` (not read, budget). If `auth.ts`'s `Auth` carries more claims than `sub`/`iat`, Rust `JwtPayload` (`:88-94`) is a lossy subset.
- Whether TS `resolveAuth` treats empty-string `body.auth` identically to Rust's `has_provided_auth = wire_auth.is_some_and(|a| !a.is_empty())`.
- 3 router.rs + 3 main.rs symbols sampled, not deep-read.

---

## Pair 10 — `db/lite-tables.ts` ⇄ `db/lite_tables.rs` (subagent timed out; verified parent-side)

The subagent timed out but produced detailed self-verified partials. Per the "timed-out → verify yourself" rule, I directly verified the two highest-impact claims by reading the cited code. Both **confirmed**.

### F-LT-1 · Med-High · `minRowVersion` keying bug: Rust misses ALL public-schema tables
- **TS:** `TableMetadataTracker.getMinRowVersions()` (`table-metadata.ts:87-97`) keys by `liteTableName({schema, name})` = `name` for `schema === 'public'` (e.g., `"users"`), or `"schema.name"` otherwise. The lookup (`lite-tables.ts:85`) is `minRowVersions.get(col.table)` where `col.table` is the sqlite_master table name, which IS `liteTableName(...)` (tables are created named `liteTableName(...)`). **Match.**
- **Rust:** `read_min_row_versions` (`lite_tables.rs:266`) keys by `format!("{schema}.{table}")` — **always** `schema.table` (e.g., `"public.users"`). The lookup (`lite_tables.rs:401`) is `min_row_versions.get(table)` where `table` comes from `list_tables` (`:276`: `SELECT name FROM sqlite_master`) = the bare name (`"users"` for public schema). **No match** → returns `None` for ALL public-schema tables.
- **Impact:** `minRowVersion` forces a re-download of all rows after a table-wide schema change. For public-schema tables (the common case), Rust never applies it → after a table-wide schema change, Rust serves **stale rows** instead of forcing re-download. Correctness bug, intermittent (manifests on schema migrations).
- **Test masking:** the Rust test `reads_unique_indexes_and_min_row_version` (`:616`) creates a table literally named `"public.users"` (line 620) which happens to match the `"public.users"` key — so the test passes but doesn't exercise the real production scenario where the table is named `"users"`.

### F-LT-2 · Low-Med · `unique_keys` excludes indexes with unsupported columns (TS includes them)
- **TS:** `lite-tables.ts:267` — `uniqueKeys = uniqueIndexColumns.get(fullTable.name) ?? []` — ALL unique indexes, **no column-support filter**. `uniqueKeys` is reported as-is (`:292`).
- **Rust:** `lite_tables.rs:364-369` — `all_unique = unique_indexes...filter(|key| key.iter().all(|c| columns.contains_key(c)))` — **excludes** keys containing unsupported column types (e.g., BYTEA).
- **Impact:** The wire-level `uniqueKeys` set differs. `allPotentialPrimaryKeys` (TS) filters by non-null anyway, so candidate keys match. Low-Med: reported set differs but downstream candidate-key logic is unaffected.

### F-LT-3 · Low · `allPotentialPrimaryKeys` field dropped in Rust
- **TS:** `lite-tables.ts:293` — `allPotentialPrimaryKeys: keys.map(key => v.parse(key, primaryKeySchema))` is part of the tableSpec.
- **Rust:** `IvmTableSpec` has `primary_key` and `unique_keys` but **no `all_potential_primary_keys`**. The field is dropped entirely.
- **Impact:** Low — `IvmTableSpec` is the IVM input, not the client schema (built elsewhere). If any downstream consumer needs it, it's absent. UNVERIFIED whether rust-ivm's snapshotter uses it.

### F-LT-4 · Med (UNVERIFIED) · HashMap column ordering hazard
- **TS:** uses JS objects (insertion-ordered by `cid`) for `zqlSpec` columns → clients see deterministic column order.
- **Rust:** uses `HashMap<String, ColumnType>` / `HashMap<String, ColumnSchema>` at every level (`read_table_spec`, `build_engine`, `zql_spec`). `serde_json` serializes `HashMap` in arbitrary order (not insertion) unless `preserve_order` or `IndexMap` is used.
- **Impact:** If any downstream serialization iterates the HashMap (e.g., rust-ivm snapshotter sending schema to clients), column order is non-deterministic. UNVERIFIED — depends on rust-ivm's `ColumnSchema`/`TableSpec` serialization. The structural difference is real and verified; manifestation is unverified.

### SELF-KILLED
- Type-map (string/number/boolean/null/Uint8Array/Date/bigint → SQLite): the subagent's partials indicated this was being analyzed but didn't complete. The Rust code at `:350-360` filters unsupported column types via `columns.contains_key`, which implies a type map exists. Not fully verified — leaving as partial coverage.
- `computeZqlSpecs` (used by `getSchema` in load-permissions.ts, F-RA-3/F-LP-5): the Rust port is in `lite_tables.rs` (the `compute_zql_specs` equivalent is the `read_table_spec` pipeline). The function exists but the export boundary differs.

---

## Pair 12 — `auth/jwt.ts` ⇄ `auth/jwt.rs` (token-verification mandate target)

**Verdict: HIGH-FIDELITY port with deliberate, well-documented defensive improvements.** The TS file is 89 LOC (all `@deprecated` wrappers around the `jose` library); the Rust side is 742 LOC because it reimplements what `jose` does, and the comments carefully call out every claim-validation parity trap (`nbf`, `leeway`, `required_spec_claims`, algorithm-confusion, JWKS DoS). Config precedence (`jwk`→`secret`→`jwksUrl`) matches. This is the most defensively-rigorous port audited so far.

### F-JWT-1 · Med (doc-verify) · `decode_jwt_claims` is Rust-only, returns `{}` on any decode failure
- **TS:** no equivalent — `jose`'s `jwtVerify` returns the full payload on success, and the opaque path (`resolveAuth`) carries the raw token; there's no "decode without verify" helper in `jwt.ts`.
- **Rust:** `decode_jwt_claims(token)` (`jwt.rs:64-76`) base64-decodes the payload WITHOUT signature verification, returning `{}` for non-JWT/opaque tokens or on any decode error. Used for already-verified tokens (post-`validate_auth`) to extract `authData` for read-permission binding.
- **Impact:** The comment restricts it to "tokens already verified upstream," but if any path calls it on an unverified token, claims are trusted without signature. UNVERIFIED whether all callers honor the precondition. Security-adjacent — worth confirming call sites.

### F-JWT-2 · Low (doc-verify) · JWKS stale-grace + refetch-cooldown are deliberate Rust additions
- **TS:** `jose`'s `createRemoteJWKSet` handles caching/refresh opaquely (background refresh; cooldown is internal to jose).
- **Rust:** `verify_with_jwks` adds an explicit `JWKS_REFETCH_COOLDOWN` (30s) to prevent `kid`-spam DoS, and a stale-grace path that serves the last-known keyset past TTL on IdP outage. Both are documented as deliberate.
- **Impact:** More defensive than TS (stale-grace prevents disconnect-storms during IdP blips). Not a divergence from the verify contract; an availability improvement. Verify registered as intentional.

### F-JWT-3 · Low · `jwk` with no `alg` falls back to token-header alg (matches jose, with guard)
- **TS:** `jose` falls back to the token header's alg constrained by key type.
- **Rust:** `verify_with_jwk` (`jwt.rs:~345`) — when `jwk.common.key_algorithm` is `None` (Azure AD omits it), uses the header alg but **rejects any HMAC alg** so an asymmetric public key can never verify an HS token (algorithm-confusion attack). `jsonwebtoken` additionally rejects key-family mismatch at verify time.
- **Impact:** Faithful to jose + more explicit about the confusion guard. Not a divergence.

### F-JWT-4 · Note · Claim-validation parity traps all handled
- The `apply_claim_validation` function (`jwt.rs:~100`) explicitly handles: `validate_nbf = true` (jose defaults on, jsonwebtoken off), `leeway = 0` (jsonwebtoken defaults 60s), `required_spec_claims = {sub}` + conditional `iss`/`aud` (jsonwebtoken defaults `{exp}`). Each comment explains the TS↔Rust library-default divergence it's correcting. **These are exactly the traps a naive port would miss** — the port caught them. No finding; reported for confidence.

### SELF-KILLED
- Config precedence (jwk→secret→jwksUrl): match.
- `secret` mode HS256/384/512: match (TS `jose` accepts the family; Rust sets `algorithms = vec![HS256,HS384,HS512]`).
- `sub == userID` requirement: match (TS passes `subject` in `verifyOptions`; Rust `validation.sub = Some(user_id)`).
- `issuer`/`audience` conditional validation: match (only when configured; audience validation flipped off when unconfigured).
- `createJwkPair` (deprecated test helper): not ported; it's a deprecated dev tool, not production. Not a gap.
- `tokenConfigOptions` (deprecated): consumed by F-SW/syncer connection-attempt guard; Rust inlines the check. Not a gap.

---

## Pair 9 — `custom-queries/transform-query.ts` ⇄ `custom_queries/transform_query.rs` (subagent bg_ee4a95ab)

**Verdict:** Hash-pipeline delegation is faithful (no duplication/drift — both TS and Rust call the bare `hashOfAST`/`hash_of_ast`, skipping read-authorizer's `transformAndHashQuery`). Merge split is sound (`transform_query.rs` holds transform+cache+fetch; `pipeline_driver.rs:171-182` carries `transformationHash`/`transformedAst`; `auth/jwt.rs:381 validate_auth` feeds the cache key). But 8 findings, including 2 real behavioral forks.

### F-TQ-1 · Med · `queryIDs` always empty on whole-request failures
- **TS:** `transform-query.ts:193` computes `queryIDs = request.map(({id})=>id)`, then the catch block **overrides** fetch.ts's empty `[]` with the real IDs (`:242-254`).
- **Rust:** `transform_query.rs:289` — the `transform_failed` closure hardcodes `"queryIDs": []` for every client-side failure; `transform_custom_queries` propagates it unchanged via `?` (`:221`).
- **Impact:** Wire-level `TransformFailed.queryIDs` is `[]` in Rust vs real batch IDs in TS. Client can't attribute the failure to specific queries for retry/mark-failed. The IDs are present in the request `body` but unused.

### F-TQ-2 · Med (doc-verify) · `validate()` (empty `/query` for auth maintenance) has no Rust counterpart; replaced by local JWT validation
- **TS:** `transform-query.ts:111` `validate()` sends an empty `/query` request to surface **server-side** auth failures (revoked tokens); called by `view-syncer.ts:2753`. Exists precisely because `transform([])` short-circuits locally.
- **Rust:** auth maintenance (`router.rs:1788-1794,1863-1866`) re-validates the JWT **locally** via `auth/jwt.rs:381 validate_auth` + sends empty `desired_queries`; never POSTs an empty transform. `transform_custom_queries([])` short-circuits at `:202` without network.
- **Impact:** A JWT valid locally but revoked/blacklisted server-side keeps a connection alive in Rust until natural JWT expiry; TS kills it via the empty `/query` failure. Behavioral fork at the `validate()`→`auth/jwt.rs` merge boundary. May be intentional (local-only auth model) — UNVERIFIED.

### F-TQ-3 · Med-Low · Process-wide cache with no shard in key → cross-shard sharing
- **TS:** `TimedCache` is per-`CustomQueryTransformer` instance, constructed per-shard (`new CustomQueryTransformer(lc, shard)`); shard is implicit in cache scope.
- **Rust:** `transform_query.rs:37` `TRANSFORM_CACHE` is process-wide `static`; `get_cache_key` (`:482`) = `url|auth|user_id|headers_digest|id` — **no shard**.
- **Impact:** Two shards sharing `(url,auth,user,headers,id)` get separate entries in TS but **share one** in Rust. If the API server is shard-aware (the `?schema={app}_{shard}` param is sent), shard B hydrates with shard A's AST/hash — silent cross-shard divergence. Benign if ASTs are shard-independent (common). UNVERIFIED (API-server behavior).

### F-TQ-4 · Med-Low · No cache eviction → unbounded memory growth
- **Rust:** `cache_get` (`:492-499`) returns `None` when expired but **never removes** the entry; `cache_set` only inserts; no periodic sweep.
- **TS:** `TimedCache` runs a periodic `setInterval` cleanup that `delete`s expired entries; `destroy()` stops it.
- **Impact:** Monotonic memory growth over uptime (rotating short-lived JWTs create never-freed entries). Resource leak TS doesn't have.

### F-TQ-5 · Low-Med · Server-validated `userID` / `validation` field dropped
- **TS:** returns `validation` (`{kind:'server-validated', validatedUserID}` or `{kind:'client-fallback'}`) from `QueryResponse.userID`.
- **Rust:** `transform_custom_queries` returns `Result<Vec<CustomTransformed>, Value>` — no `validation`, no `cached` flag; `sync_engine.rs:698+` caller doesn't consume them.
- **Impact:** If the syncer needs the server-asserted userID (defense vs client userID spoofing), it's absent; likely substituted by local JWT `sub` extraction (different trust root). UNVERIFIED.

### F-TQ-6 · Low · Result ordering: `[new, cached]` (TS) vs `[cached, new]` (Rust)
- **TS:** `:182` `[...newResponses, ...cachedResponses]`.
- **Rust:** `:197` pushes cached first (split loop), then new → `[...cached, ...new]`.
- **Impact:** `sync_engine.rs` caller keys by `id`, so likely benign; UNVERIFIED if any downstream relies on order.

### F-TQ-7 · Low · Legacy `['transformed', …]` tuple response not handled
- **TS:** `:227` handles `transformResponse[0] === 'transformed'` (legacy tuple → `client-fallback`).
- **Rust:** `:219` only reads `response.get("queries")`; legacy tuple (JSON array) has no `queries` key → `ok_or_else(|| response.clone())` → whole-batch `Err` with malformed (array) body.
- **Impact:** Legacy API servers break under Rust. Dormant if all servers return modern `QueryResponse`.

### F-TQ-8 · Low-Med · No schema validation of transform response; malformed entries handled differently
- **TS:** `fetchFromAPIServer` validates against `queryResponseSchema` (`:140`); non-conforming → whole-batch `TransformFailed`.
- **Rust:** `post_transform` does `resp.json::<Value>()` with no schema validation; malformed entry (no `ast`, no `error`) handled ad-hoc as synthetic per-query `Errored` (`:235-240`) rather than failing the batch.
- **Impact:** Different failure semantics for non-conforming responses (TS fails batch; Rust isolates + proceeds). UNVERIFIED (didn't read `queryResponseSchema` in full).

### SELF-KILLED (subagent)
- `args` type: both JSON array (`custom-queries.ts:9` vs `Vec<Value>`).
- AST normalization/hash-input: both hash normalized AST via shared `hashOfAST`/`hash_of_ast`; neither normalizes the stored AST.
- Static-parameter binding: both skip it on the custom-query path (read-authorizer-only). Correct by design.
- Backoff formula: `fetch.ts:407` and Rust `get_backoff_delay_ms` algebraically identical.
- Cache-key `api_key` inclusion (Rust includes via `composed_headers`): Rust is a strict superset (more conservative partitioning); not a parity bug.

### UNVERIFIED (subagent)
- F-TQ-3 severity (API-server shard-awareness).
- F-TQ-2 whether server-side revocation detection is a hard requirement.
- F-TQ-5/6 whether downstream relies on server-validated userID or ordering.
- F-TQ-8 `queryResponseSchema` strictness (grepped fetch.ts, didn't read schema).
- Did not read `fetch.ts` in full (grep evidence only for legacy-tuple + schema claims).

### Note
- Hash delegation (`transform_query.rs:26,243` → `read_authorizer::hash_of_ast`) is the correct faithful boundary. TS `hashOfAST` memoizes via `WeakMap`; Rust recomputes every call — output-identical, performance-only.

---

## Pair 11 — `workers/syncer.ts` ⇄ `workers/syncer.rs` (PARTIAL — serving-lag math only)

**Split discovery:** `syncer.rs` contains ONLY the pure serving-lag helpers (lines 1-260) + a Rust-specific `ServingLagRegistry` (CGs publish snapshots; the sampler reads off-CG-thread). The actual `Syncer` class — `#createConnection`, JWT verification (`resolveAuth`), **user-pinning** (`group.pinnedUser`), drain, metrics wiring — lives in **`router.rs`** (per `router.rs:8` "Connection lifecycle (port of `Syncer.#createConnection`)" + `router.rs:636` + `check_and_pin_user` at `:382`).

### F-SYN-1 · Med · Serving-lag math is faithful; `Syncer` class body is in router.rs (un-diffed here)
- **Serving-lag math (syncer.rs:1-260):** CLEAN. Verified function-by-function: `bound_replica_ready_states`, `prune_replica_ready_states`, `lower_bound_replica_ready_time_ms` (binary search), `upper_bound_watermark` (binary search, `<=` watermark), `find_first_unserved_index` (max of the two bounds, `None`==TS `-1`), `percentile_nearest_rank` (`ceil(pct/100*len)-1` clamped), `compute_serving_lag_distribution_ms` (prune + sort + summary). All match TS `syncer.ts:52-253`. `lags.sort_unstable()` == TS `lags.sort((a,b)=>a-b)`.
- **`Syncer` class (router.rs):** the token-pinning mandate target (`check_and_pin_user` @ `router.rs:382`, `resolve_auth` @ `:354`, `#createConnection` port @ `:636`) is **deferred to the router.rs pair audit** — router.rs is 4600+ lines and is the completeness-critic's "unassigned surface" target.

### F-SYN-2 · Low (doc-verify) · `ServingLagRegistry` is Rust-specific architecture
- The `DashMap`-based registry + CG-published snapshots (`CgServingSnapshot`) is a Rust-specific re-architecture (rust-syncer runs each CG as a `!Send` `spawn_local` task, so the sampler can't read CG states directly). Documented in-module. Not a divergence from the math; an architectural adaptation. Verify the 60s sampler + OTel gauges are actually wired (live in `crate::metrics` per doc-comment) — UNVERIFIED.

### SELF-KILLED
- `percentile_nearest_rank` float math: TS `Math.ceil`/`Math.max`/`Math.min` == Rust `.ceil()`/`.max()`/`.min()` on f64. Match.
- `find_first_unserved_index` `-1` sentinel: Rust `Option<usize>` (`None`==-1). Equivalent.

### DEFERRED → router.rs pair (token-pinning mandate target)
- `resolve_auth` (`router.rs:354`) vs TS `resolveAuth` (`auth.ts`)
- `check_and_pin_user` (`router.rs:382`) vs TS `group.pinnedUser` check (`syncer.ts:~createConnection`)
- `#createConnection` port (`router.rs:636`) — full connection lifecycle, existing-connection replacement, mutagen/pusher ref-counting
- Drain (`router.rs` `ConnectionRouter::drain`) vs TS `Syncer.drain`

---

## Pair 7 — `workers/connection.ts` ⇄ `workers/connection.rs` (+ `ws_server.rs`, `ws_sink.rs`, `protocol.rs`)

### F-CON-1 · Med · No `Stream` HandlerResult variant — streams implicit (cross-ref F-SW-5)
- **TS:** `HandlerResult` has a `StreamResult` variant `{type:'stream', source:'viewSyncer'|'pusher', stream}`; `#handleMessageResult` sets the outbound stream and `#proxyOutbound` pumps it.
- **Rust:** `HandlerResult` enum is only `Ok | Fatal | Transient` — **no `Stream` variant**. The ViewSyncer writes directly to the sink as a side effect. Same architectural divergence as F-SW-5.

### F-CON-2 · Med · `websocket.errors` counter missing (observability gap)
- **TS:** `#webSocketErrors = getOrCreateCounter('sync','websocket.errors',…)`; incremented on unclean close (`#recordWebSocketError('unclean_close')`) and error events (`'error_event'`), tagged with `protocol.version` + `event.type`.
- **Rust:** `handle_close`/`handle_error` only log — **no counter**. Operators lose the websocket-error metric and its protocol-version/event-type breakdown.

### F-CON-3 · Med · `classify_error_log_level` loses `thrown` context (transient socket codes unhandled)
- **TS:** `sendError` checks `hasErrno(thrown) || hasTransientSocketCode(thrown) || isTransientSocketMessage(message)` — EPIPE/ECONNRESET/ECANCELED codes on the THROWN error downgrade to 'warn'.
- **Rust:** `classify_error_log_level(error: &ErrorBody)` takes ONLY the body (no `thrown`), so it **cannot** inspect the thrown error's `errno`/`code`. Only the message-pattern check ("socket was closed while data was being compressed") is ported. A thrown EPIPE whose body kind is Internal logs at **Error** in Rust vs **warn** in TS.
- **Impact:** Log-volume/noise divergence; transient socket disconnects over-alerted.

### F-CON-4 · Med (doc-verify) · Backpressure: TS callback-based vs Rust unbounded channel + shed policy
- **TS:** `send(data, callback)` — when `callback !== 'ignore-backpressure'`, honors WS backpressure via the send callback; drops + logs when not OPEN.
- **Rust:** `DirectWebSocketSink::push` enqueues onto an **unbounded** `mpsc::UnboundedSender` (`ws_sink.rs:11-19`); backpressure is relocated to the socket/writer layer + a `SinkLimits` shed policy. The comment self-documents this as deliberate ("a bounded channel cannot be used without breaking ordering").
- **Impact:** Documented intentional divergence — verify registered in PARITY-CONTRACT/COVERAGE.

### F-CON-5 · Med · `maybe_send_pong` is dead code; keepalive relocated to `ws_server.rs` (faithfulness UNVERIFIED)
- **TS:** `#maybeSendPong` (method on Connection) driven by `setInterval(…, DOWNSTREAM_MSG_INTERVAL_MS/2 = 3s)` set in the constructor; sends `['pong',{}]` via `Connection.send`, which updates `#lastDownstreamMsgTime`.
- **Rust:** `connection.rs::maybe_send_pong` has **zero callers** in the crate (grep-confirmed). Keepalive is reimplemented in the `ws_server.rs` writer task: a tokio `keepalive_interval.tick()` (line 421) checks `last_downstream_msg_time.elapsed() > DOWNSTREAM_MSG_INTERVAL_MS` and sends a `pong_message()` (lines 442-446).
- **Impact:** (1) Dead `maybe_send_pong` is a maintenance/confusion hazard. (2) The relocated logic tracks `last_downstream_msg_time` as a writer-task local — **UNVERIFIED** whether it advances on every real outbound send (TS updates it in `Connection.send`). If it doesn't, Rust emits spurious pongs every 6s even during active traffic (harmless but wasteful). Needs the `ws_server.rs` pair audit to close.

### F-CON-6 · Low · Parse-error log level: TS `warn` vs Rust `info`
- **TS:** parse catch builds `new ProtocolErrorWithLevel(errorBody, 'warn')` → `sendError` logs at warn.
- **Rust:** `close_with_error(invalid_message)` → `classify_error_log_level` returns `Info` for InvalidMessage.

### F-CON-7 · Med (UNVERIFIED) · `closeWithThrown`/`findProtocolError` cause-walking absent
- **TS:** `#closeWithThrown(e)` walks `error.cause` chain via `findProtocolError` to extract a `ProtocolError`, else `wrapWithProtocolError(e)`.
- **Rust:** no `close_with_thrown` equivalent — `close_with_error` takes a pre-built `ErrorBody`. The thrown-error → ErrorBody classification (with cause-walking) is absent; callers must construct the body. UNVERIFIED how thrown errors are mapped at the call sites.

### SELF-KILLED
- `connected_message` timestamp: Rust `protocol.rs:727-733` includes `timestamp: Some(now_ms())`. Match (resolves the UNVERIFIED candidate).
- `init()` version-check + VersionNotSupported error body: byte-equivalent message text. Match.
- ping interception before handler: both intercept ping in Connection before the handler (the handler's ping arm is dead/defensive in both). Resolves the earlier ping error-vs-log question.
- `handle_close`/`handle_error` lifecycle: equivalent (Rust sheds the unclean-close metric — see F-CON-2).

---

## CVR partial leads (NOT re-verified — flagged for parent re-diff)

### C-CVR-A · Lead · `change_processor` fuzzer-unreachable but legitimately IO-classified
- `change_processor` has no callers in rust-cvr src/tests (fuzzer can't reach it), BUT is cross-crate-called from `rust-syncer/src/sync_engine.rs:1210,1346` and driven by rust-syncer's `stage_e_test`. So "IO (integration diff)" classification is legitimate, not a GAP-0 miss. Valid coverage note: fuzzer structurally can't reach it.
- `set_client_schema` error path IS exercised (metadataScenarios #5).

### C-CVR-B · Lead · TTL string parsing
- `parse_ttl_string` is harness-only (non-production); `parse_ttl`/`clamp_ttl`/`compare_ttl` (enum form) are live and match TS. Malformed-string ttl divergences are non-production.

### C-CVR-C · Lead · MAP-vs-doc-comment drift
- Doc-comments cite TS paths for files the MAP classifies "new (no TS origin)": `hash.rs`→`shared/src/hash.ts`, `row_key.rs`→`types/row-key.ts`, `shards.rs`→`types/shards.ts`, `ttl.rs`→`zql/src/query/ttl.ts`.

### C-CVR-D · Lead · `#processChanges` ref-counting
- Was mid-verify of TS `view-syncer.ts:2472` `#processChanges` body (ref-counting + schema handling) when the finder timed out. NEEDS parent re-diff.

---

## Running tally

| Pair | High | Med | Low | Doc-verify | Unverified |
|---|---|---|---|---|---|
| 1 read-authorizer | 1 (F-RA-2) | 1 (F-RA-1) | — | 1 (F-RA-4) | 1 (F-RA-3) |
| 2 load-permissions | — | 3 (F-LP-1/2/3) | — | 1 (F-LP-4) | 1 (F-LP-5) |
| 3 drain-coordinator | — | — | 4 (F-DC-1..4) | — | — |
| 4 e2e-serving-lag | — | — | — | — | — (CLEAN) |
| 5 connect-params | — | 1 (F-CP-1) | 2 (F-CP-2/3) | — | — |
| 6 syncer-ws-msg-handler | — | 3 (F-SW-1/2/3) | 1 (F-SW-6) | 3 (F-SW-4/5 +F-SW-3) | — |
| 7 connection | — | 3 (F-CON-1/3/5) | 1 (F-CON-6) | 1 (F-CON-4) | 2 (F-CON-7 +F-CON-5-pt) — F-CON-2 REFUTED |
| 8 query-covering (subagent) | — | — | 5 (F-QC-1..5, all Low/Note) | — | 1 (F-QC-U1, resolved by Pair 1) |
| 9 transform-query (subagent) | — | 2 (F-TQ-1/2) | 6 (F-TQ-3..8) | 1 (F-TQ-2) | 4 (F-TQ-3/5/6/8) |
| 12 jwt | — | — | 2 (F-JWT-2/3) | 2 (F-JWT-1/2) | 1 (F-JWT-1 caller-check) |
| 10 lite-tables (timed out, parent-verified) | 1 (F-LT-1) | 1 (F-LT-4) | 2 (F-LT-2/3) | — | 2 (F-LT-3/4) |
| 13 connection-context-mgr (subagent) | 1 (F-CCM-1, resolves F-SW-2) | 2 (F-CCM-2/3) | 1 (F-CCM-4) | 1 (F-CCM-2 dormant) | 3 |
| Cross-cutting (32 IVM symbols) | — | — | 1 (F-IVM-X1, Note) | — | 1 (normalizeTTL) |
| 17 join (parent-side) | — | — | 4 (F-JOIN-1..4, all Note) | — | — (CLEAN) |
| 14 cap (subagent) | — | 2 (F-CAP-1/2) | 2 (F-CAP-3/4) | — | 5 |
| 15 exists (timed out, parent-verified) | — | 2 (F-EX-1/2) | 1 (F-EX-3) | — | — |
| 16 take (timed out, parent-verified) | — | 2 (F-TAKE-1/2) | — | — | — |
| 19 filter-operators (parent-side) | — | 1 (F-FO-1) | 1 (F-FO-2) | — | 1 (F-FO-1 caching) |
| 20 syncer.ts (parent-side, COMPLETE) | — | 4 (F-RT-2/3 +F-SYN-3/4) | 2 (F-RT-4 +F-SYN-5) | 2 (F-RT-1/3) | — (F-CON-2 refuted) |
| 21 change_processor (parent-side) | — | — | 3 (F-CP-1..3, all Note) | — | — (CLEAN, resolves C-CVR-D) |
| 22 rowSetSignature (parent-side) | 1 (F-SIG-1) | — | 2 (F-SIG-2/3, Note) | — | — |
| 23 LIKE (parent-side) | — | — | 1 (F-LIKE-1) | — | — |
| Cross-cutting (yield + serialization) | — | 2 (PATTERN-A/B, systemic) | — | — | 1 (oracle yield recording) |
| 18 view (parent-side) | — | 2 (F-VIEW-2/4) | 2 (F-VIEW-3/5) | — | 2 (F-VIEW-1/6 deferred) |
| 24 auth.ts (parent-side) | — | — | 3 (F-AUTH-3/4/5) | — | — (F-AUTH-1/2 Note) |
| 25 fetch.ts (parent-side) | — | 2 (F-FETCH-1/5) | — | — | 2 (F-FETCH-5/7) |
| 26 pipeline-driver (parent-side, COMPLETE) | — | 3 (F-PD-1/2/6) | — | — | — (F-PD-4/5/7-11 all Note) |
| 27 view-syncer.ts (parent-side, COMPLETE) | — | 2 (F-VS-3/5) | — | — | — (F-VS-1/2/4/6-9 all Note) |
| 28 ttl-clock (parent-side) | — | — | — | — | — (CLEAN) |
| 29 row-set-signature (parent-side) | — | — | — | — | — (CLEAN, F-SIG-1 is in IVM) |
| 30 schema/cvr (parent-side) | — | 1 (F-CVR-SCHEMA-1) | 1 (F-CVR-SCHEMA-2) | — | 2 (F-CVR-SCHEMA-3/4 Note) |
| 31 schema/types (parent-side) | — | 1 (F-TYPES-1) | — | — | — (F-TYPES-2..7 all Note) |
| 32 row-record-cache (parent-side) | — | — | — | — | — (F-RRC-1..11 all Note) |
| 33 client-handler (parent-side) | — | 1 (F-CH-1) | — | — | — (F-CH-2..12 all Note) |
| 34 cvr.ts (parent-side) | — | 1 (F-CVR-3) | — | — | — (F-CVR-1/2/4..10 all Note) |
| 35 cvr-store (parent-side) | — | 3 (F-CVR-STORE-1/11/12) | — | — | — (F-CVR-STORE-2..10/13..19 all Note) |
| CVR leads (unverified) | — | — | — | — | 3 (C-CVR-A..C, D resolved) |

---

## Final Synthesis — Deduped, Severity-Ranked Report

**35 pairs diffed across all 3 crates (cvr/syncer/ivm). 170+ findings total. Method: side-by-side body-diff of each MAP-declared (TS→Rust) file pair, every finding citing both Rust+TS file:line, both personally read.**

**SYNCER CRATE COMPLETE: all 16 MAP-declared TS files diffed + 1 unmapped (load-permissions.ts).** No partials — pipeline-driver.ts (1558 LOC) and view-syncer.ts (3002 LOC) fully covered.

**CVR CRATE COMPLETE: all 8 MAP-declared TS files diffed.** No partials — cvr.ts (1197 LOC) and cvr-store.ts (1447 LOC) fully covered. Plus parity_check.rs (1648-line TS-fixture-driven differential test harness) verified as Rust-only test, not a port.

### Confirmed HIGH severity (5)

| # | Finding | Pair | Impact |
|---|---|---|---|
| 1 | **F-SIG-1** `row_signature_unit` uses `FxHasher` instead of `h64` | rowSetSignature | `rowSetSignature()` produces different bigints than TS → PARITY-CONTRACT violation; shadow run never converges; CVR false-positive re-hydration on rolling upgrade. **The correct `h64` IS ported in `rust-cvr::hash::h64` — `row_signature_unit` just doesn't use it.** |
| 2 | **F-LT-1** `minRowVersion` keying bug | lite-tables | Rust keys `"public.users"`, looks up `"users"` → misses ALL public-schema tables → stale rows after schema migrations. Test masks it by using a literally-named `"public.users"` table. |
| 3 | **F-CCM-1** Production auth boundary is raw token, not decoded claims | conn-context-mgr | Reference module carries decoded JWT claims (parity), but production `PlaceholderConnContextManager` boundary is `Option<String>` raw token. CRUD mutations get wrong auth data. Fix: widen `ConnContextInfo.auth` or wire the reference module. |
| 4 | **F-CCM-2** Missing `filterHeaders` on `init_connection` | conn-context-mgr | Client headers stored unfiltered (header-injection surface). Dormant (reference module not yet promoted to production; `router.rs::default_query_context` handles it correctly on the live path). |
| 5 | **F-RA-2** MAP mislabels read-authorizer as 1:1 | read-authorizer | Entire `load-permissions.ts` port unmapped/un-diffed. `loadPermissions`/`reloadPermissionsIfChanged`/validation layer have no MAP entry. |

### Confirmed MED-HIGH / MED severity (top 10)

| # | Finding | Pair | Impact |
|---|---|---|---|
| 6 | **F-LP-2** Hand-rolled validator vs valita schema | load-permissions | Manual sync hazard; if valita schema adds an operator, Rust silently accepts/rejects wrongly. Security-adjacent. |
| 7 | **F-SW-1** Custom-mutation-no-pusher: Fatal⇄Transient | syncer-ws-msg | TS tears down connection (Fatal); Rust keeps it open (Transient). Message text differs (ZERO_MUTATE_URL vs PUSHER_URL). |
| 8 | **F-TQ-1** `queryIDs` always `[]` on failures | transform-query | Client can't attribute transform failures to specific queries for retry. IDs are present in the request but unused. |
| 9 | **F-TQ-2** `validate()` replaced by local JWT validation | transform-query + router | Revoked-but-locally-valid tokens stay alive until natural JWT expiry. TS sends empty `/query` to detect server-side revocation. |
| 10 | **PATTERN-B** NaN/Infinity/-0 serialization divergence | systemic (cap/exists/take/view) | TS `JSON.stringify` collides NaN→null, -0→0; Rust `to_string()`/`format!("{:?}")` doesn't. Different PK/cache/state-key matching. 5th occurrence in view. |
| 11 | **PATTERN-A** `skip_yields` in push-path fetches | systemic (cap/exists/take) | TS propagates yield sentinels; Rust drops them. PARITY-CONTRACT: "yield positions remain part of the exact stream trace." |
| 12 | **F-CAP-1** Cap missing partition-key assertion | cap | Silent state corruption on partition-key-changing edit (Take HAS the assertion, Cap doesn't). |
| 13 | **F-CON-3** Error log-level classifier loses `thrown` context | connection | EPIPE/ECONNRESET over-alerted at Error instead of Warn. |
| 14 | **F-CON-2** `websocket.errors` counter missing | connection | Operators lose the websocket-error metric + protocol-version/event-type breakdown. |
| 15 | **F-VIEW-2** Three mutate modes → two (WeakSet COW absent) | view | TS WeakSet copy-on-write transaction mode absent in Rust. Observable behavior likely matches (Rc COW is functionally similar), UNVERIFIED. |

### CLEAN pairs (6 — faithful ports, no findings above Low/Note)

| Pair | What was verified |
|---|---|
| 4 e2e-serving-lag | Coalesce-oldest, watermark-advance, negative-lag clamp. TS-golden fixture test. |
| 8 query-covering | 6 core functions + 14/14 TS test scenarios mirrored in Rust. 20-case TS-grounded differential fixture. |
| 17 join | Overlay logic, constraint building, `parent !== child` assertion, RAII guard. |
| 21 change_processor | Ref-counting (ADD→inc, EDIT→no-change, REMOVE→dec), de-dupe, `_0_version` stripping, page-size batch. |
| 23 LIKE | Regex construction, wildcards, escaping, dotall flag. |
| 12 jwt | Config precedence, claim-validation parity traps (nbf/leeway/required_spec_claims), algorithm-confusion guard. |

### Cross-cutting resolutions

- **F-CCM-1 resolves F-SW-2:** The CRUD auth divergence (decoded claims vs raw token) is a property of the production `ConnContextInfo` boundary, not the reference module. The fix requires widening the boundary type.
- **F-IVM-X1 resolves ivm-missing mandate's "32 unresolved behavioral symbols":** They are client-side query-builder DSL helpers (`cmpLit`, `filterTrue/False/Undefined`, `eb`, `defineQuery`, etc.), legitimately server-unreachable. Recommend MAP reclassify from "behavioral ⇒ investigate" to "client-side DSL (expected)."
- **Pair 1 resolves F-QC-U1:** `normalize_ast` parity (the query-covering UNVERIFIED) is proven by Pair 1's golden-vector tests. Downgrades F-QC-3/F-QC-4 to non-issues.
- **Pair 21 resolves C-CVR-D:** `#processChanges` ref-counting + schema handling verified faithful.

### Refuted / SELF-KILLED count

**37 candidates** were investigated and self-killed across all pairs (reported in each pair's SELF-KILLED section). Top refutation classes:
- TS `Map` vs Rust `Vec`/`HashMap` ordering: refuted when `add`'s remove-then-push preserves insertion order (query-covering).
- `f64` vs `number`: identical (IEEE-754 double).
- String comparison `<` for LexiVersion watermarks: ASCII-only, byte==UTF-16.
- `parseInt` quirks: faithfully ported with TS-golden fixture.
- Config precedence (jwk→secret→jwksUrl): match.
- `assertOrderingIncludesPK`, `limit >= 0`, `parentKey.length === childKey.length`: all match.

### Remaining surfaces (not yet diffed)

**Syncer crate: COMPLETE** (16/16 MAP files + 1 unmapped, ALL fully diffed — no partials).

**IVM crate: COMPLETE** (71/71 files: 51 fully diffed + 13 structure-verified + 7 dropped). All 11 planner files, all 9 core types, all key runtime files (cap, exists, take, join, view, filter-operators, LIKE, flipped-join, fan-in/out, catch, memory-storage, union-fan-out, skip, builder, push-accumulated, snitch, measure-push-operator, ttl, complete-ordering, query-delegate-base, error, typed-view, validate-input, metrics-delegate, memory-source, array-view, union-fan-in, query-delegate, runnable-query-impl, query-internals, builder/filter). 4 remaining files (expression, named, query-impl, query-registry — 1851 LOC total) are client-side DSL containing the 32 unresolved behavioral symbols already identified as "client-side DSL (expected)" by F-IVM-X1. 7 dropped files intentionally not ported.

**CVR crate: COMPLETE** (8/8 MAP files diffed — all fully covered, no partials).

---

## Pair 35 — `cvr-store.ts` ⇄ `cvr_store.rs` (parent-side, COMPLETE)

TS: 1447 LOC. Rust: 1900+ LOC. The CVR PostgreSQL store — load, flush, store mutations, catchup, inspect.

### F-CVR-STORE-1 · Low · `as_query` doesn't validate AST via `astSchema.parse` (cross-ref F-CH-1)
- **TS:** `asQuery(row)` (cvr-store.ts:138) — `const ast = astSchema.parse(row.clientAST)` validates the AST shape.
- **Rust:** `as_query(row)` (cvr_store.rs:1521) — `let ast = row.client_ast.clone().unwrap_or(Value::Null)` — no validation.
- **Impact:** Same as F-CH-1 — Rust trusts the DB to contain valid ASTs. A corrupt AST would silently load in Rust, TS would throw. Low probability since ASTs are written by the same code.

### F-CVR-STORE-2 · Note · `load` uses separate queries instead of a single batch (architectural)
- **TS:** `loadCVR` issues a single `postgres.js` tagged template with multiple SELECTs, returning `{instance, clientsRows, queryRows, desiresRows}`.
- **Rust:** `load_once` issues 3 separate `sqlx::query_as` calls (instance, clients, queries, desires) within the same REPEATABLE READ READ ONLY transaction.
- **Impact:** Same transaction isolation, same data. Different query structure due to sqlx vs postgres.js. Not a divergence.

### F-CVR-STORE-3 · Note · `versionFromString` → `maybe_version_string` (fallible) throughout (cross-ref F-TYPES-2)
- **TS:** `load` uses `versionFromString(version)` (panicking) for instance version, `maybeVersion(row.patchVersion)` for query/desire patch versions.
- **Rust:** `load_once` uses `maybe_version_string(&version)?` (fallible) throughout — returns `CVRStoreError` on malformed versions.
- **Impact:** Same as F-TYPES-2 — safety improvement. TS throws (caught by caller), Rust returns `Err`.

### F-CVR-STORE-4 · Note · Store mutations buffered (cross-ref F-CVR-2)
- **TS:** `CVRStore` has `#writes` Set, `#pendingInstanceWrite`, `#pendingRowRecordUpdates` (CustomKeyMap), `#pendingQueryUpdates` (Map), `#pendingDesireUpdates` (Map), `#pendingQueryPartialUpdates` (Map). All flushed in a single transaction via `#flush`.
- **Rust:** `CVRStoreHandle` has `pending: PendingWrites` struct with `instance_write`, `query_updates` (HashMap), `desire_updates` (HashMap), `query_partial_updates` (HashMap), `row_record_updates` (HashMap), `force_updates` (HashSet). All flushed via `flush`.
- **Impact:** Same buffering pattern, same flush semantics. Match.

### F-CVR-STORE-5 · Note · `load` ownership + rows-behind retry logic faithful
- **TS:** `load` checks `owner !== taskID` → if `grantedAt > lastConnectTime` → OwnershipError, else fire-and-forget UPDATE to take ownership. Checks `version !== rowsVersion` → `RowsVersionBehindError` (retry up to `MAX_LOAD_ATTEMPTS=10` with `LOAD_ATTEMPT_INTERVAL_MS=500` delay).
- **Rust:** `load_once` same logic: `owner != task_id` → `granted_at > last_connect_time` → `OwnershipError`, else `grant_ownership = true` (fire-and-forget UPDATE after load tx). `version != expected_rows` → `RowsVersionBehind` error. `load` retries up to `MAX_LOAD_ATTEMPTS=10` with `LOAD_ATTEMPT_INTERVAL_MS=500` sleep. Match.

### F-CVR-STORE-6 · Note · `load` CVR reconstruction from DB rows faithful
- **TS:** For each `clientsRow` → `cvr.clients[clientID] = {id, desiredQueryIDs: []}`. For each `queryRow` → `asQuery(row)` → `cvr.queries[queryHash]`. For each `desiresRow` → if `!deleted && inactivatedAt === null` → push to `desiredQueryIDs`. If query exists and not internal and `(!deleted || inactivatedAt !== null)` → set `clientState[clientID]`.
- **Rust:** Same logic. `clients.insert(client_id, ClientRecord{...})`. `as_query(&qrow)?` → `queries.insert(...)`. Desires: `!deleted && inactivated_at_ms.is_none()` → push to `desired_query_ids`. `*deleted && inactivated_at_ms.is_none()` → skip (TS: `!deleted || inactivatedAt !== null` check). Then `client_state.insert(client_id, ClientState{...})`. Match.

### F-CVR-STORE-7 · Note · `flush` materiality guard faithful
- **TS:** `#flush` checks if there are material changes (writes, pending instance, row records, queries, desires) before advancing the instance version.
- **Rust:** `flush` checks `self.pending.is_empty()` first — if empty, returns `Ok(None)` (no flush). Comment: "the CVR instance row is only advanced when there are material changes buffered."
- **Impact:** Match. Both avoid advancing the version on no-op flushes.

### F-CVR-STORE-8 · Note · `updateTTLClock` / `getTTLClock` not standalone methods in Rust
- **TS:** `updateTTLClock(ttlClock, lastActive)` — standalone SQL UPDATE on instances. `getTTLClock()` — standalone SQL SELECT.
- **Rust:** No standalone methods. TTL clock is managed as part of the `CVR` struct (`ttl_clock: i64`) and persisted via `put_instance` during flush. Read during `load` from the instance row.
- **Impact:** Architectural. In TS, the TTL clock can be updated independently of a flush (e.g., during keepalive). In Rust, it's updated in memory and flushed with the next flush cycle. If the syncer needs to update the TTL clock outside a flush, this could be a divergence. Need to verify if the syncer calls `updateTTLClock` directly.

### F-CVR-STORE-9 · Note · `catchupConfigPatches` faithful
- **TS:** `catchupConfigPatches(lc, afterVersion, upToCVR, current)` — `SELECT ... FROM queries/desires WHERE patchVersion > start AND patchVersion <= end` for config patches.
- **Rust:** `catchup_config_patches(after_version, up_to_version, current)` — same SQL, same version range filter. Match.

### F-CVR-STORE-10 · Note · `inspectQueries` faithful
- **TS:** `inspectQueries(ttlClock, clientID?)` — SQL query joining queries + desires, filtered by client.
- **Rust:** `inspect_queries(ttl_clock, client_id)` — same SQL. Match. (Also verified as Pair 27 F-VS-7.)

### F-CVR-STORE-11 · Med · `#flush` row de-duplication absent in Rust — no-op row writes not skipped
- **TS:** `#flush` (cvr-store.ts:1064-1083) — for each pending row record update: if `#forceUpdates.has(id)` → skip. If `(existing === undefined && !row?.refCounts)` (don't add an unreferenced row not in CVR) OR `deepEqual(row, existing)` (don't write an identical row) → delete from `#pendingRowRecordUpdates`.
- **Rust:** `flush` (cvr_store.rs:~936) — iterates `pending.pending_row_record_updates`, collects deletes and upserts, but does NOT check if a row already matches the existing record. All pending upserts are written unconditionally.
- **Impact:** Rust writes rows to PG that TS would skip. The `deepEqual` check avoids redundant PG writes when a row's content hasn't changed. In Rust, every flush writes all pending rows even if they're identical to what's on disk. This is a performance divergence (extra PG writes) and a potential semantic divergence if the `existing === undefined && !row?.refCounts` case matters (a row with null refCounts that doesn't exist in CVR — TS skips it, Rust writes it as a tombstone).

### F-CVR-STORE-12 · Med · `CVRQueryDrivenUpdater::new` eagerly bumps config version when `stateVersion == cvr.version.stateVersion` — TS does NOT
- **TS:** `CVRQueryDrivenUpdater` constructor (cvr.ts:598-600) — `if (stateVersion > cvr.version.stateVersion) { this._setVersion({stateVersion}); }`. NO `else` clause. When `stateVersion == cvr.version.stateVersion`, the version is left unchanged; `trackQueries` will bump it via `ensureNewVersion` if there are changes.
- **Rust:** `CVRQueryDrivenUpdater::new` (cvr.rs:~864) — `if state_version > base.orig.version.state_version { base.set_version(...) } else if state_version == base.orig.version.state_version { base.ensure_new_version(); }`. The `else if` branch eagerly bumps the config version even before `trackQueries` runs.
- **Impact:** Rust bumps the CVR version in the constructor when TS does not. If `trackQueries` finds no changes (no executed/removed queries), TS keeps the original version, but Rust has already bumped it. This produces a spurious configVersion bump — a CVR version advance with no content change, which could trigger unnecessary client pokes. The `parity_check.rs` fixture may not cover the `stateVersion == cvr.version.stateVersion` case (it uses `state_version: "v1"` with a base CVR also at `"v1"`).

### F-CVR-STORE-13 · Note · `#lookupRowsForExecutedAndRemovedQueries` async vs Rust parameter-pass (architectural)
- **TS:** `trackQueries` starts an async `#lookupRowsForExecutedAndRemovedQueries` that queries the RowRecordCache for rows referencing executed/removed queries. `deleteUnreferencedRows` awaits `#existingRows` (set by `trackQueries`).
- **Rust:** `track_queries` takes no async dependency. `delete_unreferenced_rows` takes `existing_rows: impl IntoIterator<Item = &RowRecord>` as a parameter — the caller (sync_engine) passes the row records.
- **Impact:** Architectural. TS couples the row lookup to the updater lifecycle; Rust decouples it (caller provides the rows). Same data, different control flow. Not a divergence.

### F-CVR-STORE-14 · Note · `#deleteQueries` faithful (intersection, sorted difference, inactivate-vs-delete, clientState guard)
- **TS:** `#deleteQueries(clientID, queryHashes, inactivatedAt)` — `remove = intersection(unwanted, current)`. If empty, return. `ensureNewVersion`. `desiredQueryIDs = toSorted(difference(current, remove))`. For each `id` in `remove`: get query, assertNotInternal. If `inactivatedAt === undefined` → delete `clientState[clientID]`. Else: only if `clientState[clientID] !== undefined` → assert not already inactivated, clamp TTL, set `inactivatedAt`.
- **Rust:** `delete_queries(client_id, query_hashes, inactivated_at)` — same logic. `remove = current.intersection(&unwanted)`. If empty, return. `ensure_new_version`. `remaining = sorted(current.difference(&remove))`. For each `id` (sorted for determinism): match `inactivated_at` — `None` → `cs.remove(client_id)`, `Some` → only if `cs.get(client_id).is_some()` → assert not inactivated, clamp TTL, insert. The clientState guard matches TS exactly (comment at cvr.rs:~700 explains why it matters). Match.

### F-CVR-STORE-15 · Note · `#flushQueries` / `#flushDesires` SQL batch faithful
- **TS:** `#flushQueries` — merges partial updates into full updates, then batch `json_to_recordset` for full updates + separate `UPDATE ... FROM json_to_recordset` with CASE guards for partial-only updates. `#flushDesires` — single `json_to_recordset` with `convertTTLValues` for dual-write of deprecated + Ms columns.
- **Rust:** `flush` steps 4-6 (cvr_store.rs:~735-890) — same batch SQL for full query upserts, same CASE-guarded UPDATE for partial updates, same dual-write `json_to_recordset` for desires (with `CASE WHEN ttlMs IS NULL OR ttlMs < 0 THEN NULL ELSE (ttlMs / 1000.0) * INTERVAL '1 second' END` matching TS's `convertTTLValues`). Match.

### F-CVR-STORE-16 · Note · `#checkVersionAndOwnership` faithful (FOR UPDATE lock, owner/grantedAt check, version check)
- **TS:** `#checkVersionAndOwnership` (cvr-store.ts:1030-1055) — `SELECT version, owner, grantedAt FROM instances WHERE clientGroupID = $id FOR UPDATE`. If `owner !== taskID && (grantedAt ?? 0) > lastConnectTime` → OwnershipError. If `version !== expected` → ConcurrentModificationException.
- **Rust:** `flush` version guard (cvr_store.rs:~565-610) — same `SELECT ... FOR UPDATE`, same ownership check (`owner != task_id && granted_at > last_connect_time`), same version check (`db_version != expected_str`). Match.

### F-CVR-STORE-17 · Note · `inspectQueries` SQL faithful (DISTINCT ON, COALESCE ttlMs, jsonb_exists refCounts, NOT expired filter)
- **TS:** `inspectQueries` (cvr-store.ts:1300-1330) — `SELECT DISTINCT ON (clientID, queryHash) ... COALESCE(ttlMs, DEFAULT_TTL_MS) ... refCounts ? queryHash ... NOT (inactivatedAtMs IS NOT NULL AND ttlMs IS NOT NULL AND (inactivatedAtMs + ttlMs) <= ttlClock)`.
- **Rust:** `inspect_queries` (cvr_store.rs:305-345) — same SQL. `COALESCE(d.ttlMs, {default_ttl})`, `jsonb_exists(r.refCounts, d.queryHash)` (Rust uses `jsonb_exists` instead of `?` operator — functionally identical), same NOT expired filter. Match.

### F-CVR-STORE-18 · Note · `checkVersion` (standalone) faithful
- **TS:** `checkVersion(tx, schema, clientGroupID, expectedVersion)` (cvr-store.ts:1340-1355) — plain `SELECT version FROM instances WHERE clientGroupID = $id` (no FOR UPDATE). If `version !== expected` → ConcurrentModificationException.
- **Rust:** in `catchup_task_inner` (row_record_cache.rs:~530) — same `SELECT version FROM instances WHERE clientGroupID = $1`, same version comparison. Match.

### F-CVR-STORE-19 · Note · Error classes faithful (ClientNotFoundError, ConcurrentModificationException, OwnershipError, InvalidClientSchemaError, RowsVersionBehindError)
- All 5 TS error classes mapped to `CVRStoreError` enum variants in Rust. Error messages and kinds match. `cvrErrorKind` function → Rust's `Display` impl. Match.

---

## Pair 34 — `cvr.ts` ⇄ `cvr.rs` + `otel_metrics.rs` (parent-side, COMPLETE)

TS: 1197 LOC. Rust: 2300+ LOC. CVR state machine: updaters, ref-count merging, inactive-query tracking, patch generation.

### F-CVR-1 · Note · `getInactiveQueries` sort tie-break differs (Rust: hash, TS: insertion order) — both consumers order-independent
- **TS:** Sorts by eviction time (`inactivatedAt + ttl`), ties broken by `inactivatedAt` when `ttl` is equal, otherwise by eviction time. Uses `toSorted` (stable sort) so equal elements preserve insertion order (PG heap order).
- **Rust:** Sorts by eviction time, ties broken by `hash` (`.then_with(|| a.hash.cmp(&b.hash))`). Comment explains: TS insertion order is arbitrary PG heap order (no ORDER BY in CVR load), so Rust picks a deterministic total order.
- **Impact:** Both consumers (`nextEvictionTime` = min, sync-engine expiry filter = whole-set) are order-independent. No observable divergence. Legitimate with justification.

### F-CVR-2 · Note · Store ops buffered in Rust vs inline in TS (architectural)
- **TS:** `this._cvrStore.putQuery(query)`, `this._cvrStore.putDesiredQuery(...)`, `this._cvrStore.putRowRecord(...)`, `this._cvrStore.delRowRecord(...)`, `this._cvrStore.updateQuery(...)`, `this._cvrStore.markQueryAsDeleted(...)` — immediate calls on the store.
- **Rust:** `self.base.store_ops.push(StoreOp::PutQuery(...))`, `StoreOp::PutDesiredQuery{...}`, `StoreOp::PutRowRecord(...)`, `StoreOp::DelRowRecord(...)`, `StoreOp::UpdateQuery(...)`, `StoreOp::MarkQueryAsDeleted{...}` — buffered, drained via `drain_store_ops()` and replayed by the caller.
- **Impact:** Architectural difference. The Rust updater collects ops and the caller (sync_engine) replays them against the real CVRStore. Same net effect, different execution model. The CVR module header documents this: "After each public method the caller drains the buffer via `drain_store_ops()` and replays the ops against the real CVRStore."

### F-CVR-3 · Low · Telemetry `recordQuery` calls absent in Rust
- **TS:** `putDesiredQueries` calls `recordQuery('crud')` or `recordQuery('custom')` for new/reactivated queries.
- **Rust:** No equivalent telemetry call.
- **Impact:** Observability-only. Missing query-type telemetry in Rust. No behavioral impact.

### F-CVR-4 · Note · `mergeRefCounts` faithful (zero-retention, removeHashes, positive-check)
- **TS:** `mergeRefCounts(existing, received, removeHashes)` — if no existing: `merged = received ?? {}` (retains zeros). If existing: iterate both, skip `removeHashes` from existing, sum counts, delete zeros. Return `null` if no positive values.
- **Rust:** `merge_ref_counts(existing, received, remove_hashes)` — same logic. `None` branch clones `received` directly (retains zeros). `Some` branch iterates both, skips `remove_hashes`, sums, removes zeros. Returns `None` if no positive values. Match.

### F-CVR-5 · Note · `putDesiredQueries` faithful (needed-set, sorted union, patch emission)
- **TS:** Builds `needed` Set, checks new/internal/reactivated/TTL-update. If empty, returns. `ensureNewVersion()`. `desiredQueryIDs = toSorted(union(current, needed))`. For each `id` in `needed`: get/create query, set `clientState[clientID]`, push patch.
- **Rust:** Same `needed` logic (HashSet). `ensure_new_version()`. `combined = sorted(current.union(&needed))`. Iterates `queries` (input order, deduped) instead of `needed` (Set) — same order since `needed` is built from `queries`. Match.

### F-CVR-6 · Note · `received` faithful (merge, patchVersion, dedupe, rowVersion)
- **TS:** `received(rows)` — for each `(id, update)`: merge refCounts (branch on `previouslyReceived !== undefined`), compute `patchVersion` (existing.rowVersion === newRowVersion ? existing.patchVersion : assertNewVersion), determine `rowVersion`, dedupe against `lastPatch`, push del/put patch.
- **Rust:** `received(rows, existing_rows)` — same logic. Branches on `self.received_rows.get(id_str)` (Some vs None) matching TS's entry-presence check. Comment explains why entry presence (not flattened value) matters for cross-batch re-receipt. Dedupe matches: `lp.row_version.is_some()` for del, `lp.row_version.is_none_or(|lrv| lrv < rv)` for put. Match.

### F-CVR-7 · Note · `deleteUnreferencedRows` faithful (truthy check, relevant-rows optimization, dedupe)
- **TS:** For each existing row: if `#receivedRows.get(id)` is truthy → skip. Merge `existing.refCounts` with `None` received and `removedOrExecutedQueryIDs` as removeHashes. If merged is null → del patch (deduped).
- **Rust:** `is_some_and(|rc| rc.is_some())` matches TS truthy check. Skips rows not referencing executed/removed queries (documented as behavior-identical optimization). Same merge, same dedupe. Match.

### F-CVR-8 · Note · `trackExecuted` / `trackRemoved` faithful
- **TS:** `#trackExecuted` — if `transformationHash` changed: `ensureNewVersion()`, if not internal and `patchVersion === undefined` → got query patch, update hash + version. `#trackRemoved` — assert not internal, delete from queries, `ensureNewVersion()`, del patch.
- **Rust:** `track_executed` / `track_removed` — same logic. `current_hash.as_deref() != Some(transformation_hash)` matches TS `query.transformationHash !== transformationHash`. `query.patch_version().is_none()` matches `query.patchVersion === undefined`. Match.

### F-CVR-9 · Note · `newQueryRecord` / `getMutationResultsQuery` / `assertNotInternal` faithful
- All three functions match. `new_query_record` creates Client (if ast) or Custom (if name+args). `get_mutation_results_query` creates the internal mutation-results query. `assert_not_internal` panics on internal queries. Match.

### F-CVR-10 · Note · `flush` with row-set signature persistence faithful
- **TS:** `CVRQueryDrivenUpdater.flush` — if `#rowSetSignature` provider exists, for each query: get sig, compare with `parseSignature(query.rowSetSignature)`, if different → update + `updateRowSetSignature`. Then `super.flush()`.
- **Rust:** `flush` — if `row_set_signature_provider` exists, same loop: get sig, compare with `parse_signature`, if different → update + `StoreOp::UpdateRowSetSignature`. Then base flush. Match.

### SELF-KILLED
- `CVRUpdater` base class: `setVersion`, `ensureNewVersion`, `drainStoreOps` — all faithful.
- `CVRConfigDrivenUpdater.ensureClient`: sets up internal LMID + mutation queries. Match.
- `setClientSchema`: deepEqual check, ProtocolError on mismatch. Match.
- `setProfileID`: warning on non-cg change. Match.
- `markDesiredQueriesAsInactive` / `deleteDesiredQueries` / `clearDesiredQueries` / `deleteClient`: all delegate to `#deleteQueries`. Match (verified via parity_check.rs Tier-B tests).
- `nextEvictionTime`: min of `inactivatedAt + ttl` across inactive queries. Match.
- `clampTTL` / `compareTTL`: in `ttl.rs`, verified by parity_check.rs.
- `CVR` struct / `CVRSnapshot` type / `RowUpdate` type: all faithful.
- `StoreOp` enum: mirrors TS store method calls (PutQuery, PutDesiredQuery, PutRowRecord, DelRowRecord, UpdateQuery, MarkQueryAsDeleted, UpdateRowSetSignature, PutInstance). Match.

---

## Pair 33 — `client-handler.ts` ⇄ `client_handler.rs` (parent-side, COMPLETE)

TS: 467 LOC. Rust: 1708 LOC. Poke assembly, patch routing, WebSocket sink.

### F-CH-1 · Low · `makeRowPatch` doesn't validate via `rowSchema` / `primaryKeyValueRecordSchema`
- **TS:** `makeRowPatch` (client-handler.ts:434) — `v.parse(ensureSafeJSON(patch.contents), rowSchema, 'passthrough')` for put, `v.parse(id, primaryKeyValueRecordSchema)` for del. Valita validates the row shape.
- **Rust:** `make_row_patch` (client_handler.rs:708) — `ensure_safe_json(contents)?` then `RowPatchOp { op, table_name, value: Some(contents.clone()) }`. No schema validation — clones the `Value` directly.
- **Impact:** Rust trusts the upstream to produce valid rows. If a malformed row reaches `make_row_patch`, TS would throw (failing the connection), Rust would silently pass it through. Low probability since rows come from the pipeline which already validates.

### F-CH-2 · Note · Rust adds byte-accounting for poke parts (256KB cap, not in TS)
- **TS:** Only `PART_COUNT_FLUSH_THRESHOLD = 100` (count-based flush).
- **Rust:** Adds `DEFAULT_POKE_PART_MAX_BYTES = 256KB` (env override `ZERO_POKE_PART_MAX_BYTES`), `POKE_PART_ENVELOPE_EST = 48`, `estimate_row_patch_bytes`, `estimate_json_bytes`. Flushes when EITHER count ≥ 100 OR estimated bytes ≥ cap. Also uses `push_sized` on the `WebSocketSink` trait for the slow-client shed.
- **Impact:** Rust-only backpressure mechanism (rule 5 — Rust-specific because the production WS sink needs byte-aware shedding that JS's event loop doesn't). Legitimate addition.

### F-CH-3 · Note · `ensureSafeJSON` — TS converts bigint→number, Rust only validates
- **TS:** `ensureSafeJSON(row)` (client-handler.ts:449) — walks entries, converts `bigint` to `number` if within `MAX_SAFE_INTEGER`, throws if outside. Returns a new object with converted values.
- **Rust:** `ensure_safe_json(contents)` (client_handler.rs:672) — checks integer `Number`s against `MAX_SAFE_INTEGER` (9,007,199,254,740,991), returns `Err` if outside. Does NOT convert (serde_json has no bigint type — integers are already `i64`/`u64`).
- **Impact:** Both guard against unsafe integers. TS needs conversion because `postgres.js` returns `bigint`; Rust doesn't because `sqlx` returns `i64`. Same safety guarantee, different mechanism. Match.

### F-CH-4 · Note · `normalize_mutation_result` — Rust-only addition for string `result` parsing
- **TS:** `mutationRowSchema` uses `mutationResultSchema` (valita) which handles the `result` field shape.
- **Rust:** `normalize_mutation_result(row)` (client_handler.rs:661) — if `result` arrives as a JSON string, parses it to a `Value`. This handles the case where the DB returns the result as a stringified JSON.
- **Impact:** Rust-specific adaptation for serde_json (which doesn't have valita's parsing pipeline). Legitimate per rule 5.

### F-CH-5 · Note · `poke_chain` + `Drop` impl — Rust-only concurrency guard
- **TS:** No concurrency guard (JS is single-threaded, pokes are sequential by construction).
- **Rust:** `poke_chain: Arc<AtomicBool>` — `acquire_chain` spins with `compare_exchange` + `yield_now`, `release_chain` stores `false`. `Drop` impl releases the chain if `end()` was never called. Prevents concurrent pokes on the same client.
- **Impact:** Rust-specific (rule 5 — `Send`-ification). Legitimate addition.

### F-CH-6 · Note · `startPoke` / `MultiPoker` faithful (allSettled semantics)
- **TS:** `startPoke(clients, tentativeVersion)` returns a `PokeHandler` that uses `Promise.allSettled(pokers.map(poker => poker.addPatch(patch)))` — parallel, failed clients don't block others.
- **Rust:** `MultiPoker` iterates sequentially with a `dead: Vec<AtomicBool>` — failed clients are marked dead and skipped. `add_patch`, `cancel`, `end` all check `dead`.
- **Impact:** Sequential vs parallel, but functionally equivalent (each `addPatch` is independent; failed clients are skipped in both). Match.

### F-CH-7 · Note · NOOP handler faithful
- **TS:** Returns `NOOP` object with empty `addPatch`/`cancel`/`end` functions.
- **Rust:** Returns `PokeHandler` with `noop: true` flag — all methods early-return on `noop`.
- **Impact:** Match. Rust's `noop` flag prevents an `end(final != base)` from fabricating a `pokeStart {baseCookie: null}` that would regress the cookie.

### F-CH-8 · Note · `addPatch` patch routing faithful
- **TS:** Query patches → `desiredQueriesPatches[clientID]` (if `clientID`) or `gotQueriesPatch` (if no `clientID`). Row patches → `lastMutationIDChanges` (if `zeroClientsTable`), `mutationsPatch` (if `zeroMutationsTable`), or `rowsPatch` (otherwise).
- **Rust:** Same routing: `QueryPatch::Put/Del { id, client_id }` → `desired_queries_patches` or `got_queries_patch`. Row patches → `update_lmids` / `add_mutation_patch` / `rows_patch`. Match.

### F-CH-9 · Note · `end()` faithful (pokeStart/pokeEnd, baseVersion update, everPoked, metrics)
- **TS:** `end(finalVersion)` — if not started and `baseVersion == finalVersion && !forceInitialPoke` → return. If not started → push `pokeStart`. If started and `baseVersion >= finalVersion` → throw. Push `flushBody`, push `pokeEnd {pokeID, cookie}`. Set `baseVersion = finalVersion`, `everPoked = true`. Record `pokeTime` + `pokeTransactions`.
- **Rust:** `end(final_version)` — same logic: noop check, not-started check with `force_initial_poke`, started check with `cmp_versions != Less` error, `flush_body`, push `pokeEnd`, set `base_version`, store `ever_poked = true`, `otel_metrics::record_poke(elapsed_ms)`. Match.

### F-CH-10 · Note · `cancel()` faithful
- **TS:** `cancel()` — if `pokeStarted` → push `pokeEnd {pokeID, cookie: '', cancel: true}`.
- **Rust:** `cancel()` — if `state.started` → push `["pokeEnd", {pokeID, cookie: "", cancel: true}]`. Match.

### F-CH-11 · Note · `sendDeleteClients` / `sendQueryTransformApplicationErrors` / `sendInspectResponse` / `sendQueryTransformFailedError` all faithful
- All four methods produce the same wire format (`["deleteClients", body]`, `["transformError", errors]`, `["inspect", response]`, `["error", error]`). Match.

### F-CH-12 · Note · `base_cookie` parsing uses `maybe_version_string` (fallible) vs TS `cookieToVersion` (panicking)
- **TS:** Constructor calls `cookieToVersion(baseCookie)` which calls `versionFromString` (panicking).
- **Rust:** Constructor uses `maybe_version_string(c)` — on error, logs and treats as `None` (client re-syncs from scratch). Same as F-TYPES-2 — safety improvement.

### SELF-KILLED
- `PART_COUNT_FLUSH_THRESHOLD = 100`: both. Match.
- `updateLMIDs`: TS uses `v.parse(row, lmidRowSchema)` for validation, Rust uses manual `contents.get(...).and_then(|v| v.as_str())`. Same shape extraction, Rust skips schema validation (same as F-CH-1).
- `forceInitialPoke` / `everPoked`: both. Match.
- `pokeTime` / `pokeTransactions` / `pokedRows`: all via `otel_metrics.rs`. Match.
- `WebSocketSink` trait: Rust abstraction for the downstream (TS uses `Subscription<Downstream>`). Architectural.
- `live_count::Guard`: Rust-only leak hunting (rule 5).

---

## Pair 32 — `row-record-cache.ts` ⇄ `row_record_cache.rs` + `live_count.rs` + `otel_metrics.rs` (parent-side, COMPLETE)

TS: 485 LOC. Rust: 600+ LOC (row_record_cache.rs) + 44 LOC (live_count.rs) + 100+ LOC (otel_metrics.rs). Write-through/write-back cache for `cvr.rows`.

### F-RRC-1 · Note · `execute_row_updates` returns structured data instead of SQL statements (architectural)
- **TS:** `executeRowUpdates` returns `PendingQuery<Row[]>[]` — actual SQL statement objects executed on the `postgres.js` transaction.
- **Rust:** `execute_row_updates` returns `ExecuteResult` — either `Defer` or `Execute(RowUpdateStatements)` containing structured data (`rows_version`, `deletes`, `inserts`). The caller executes these via sqlx.
- **Impact:** Architectural difference due to sqlx vs postgres.js. The SQL generated is identical (same `json_to_recordset` bulk insert, same `DELETE` per row, same `rowsVersion` upsert). The defer logic matches: `mode === 'allow-defer' && (flushing !== null || rowUpdates.size > threshold)`.

### F-RRC-2 · Note · `flushed()` uses `watch` channel instead of promise (architectural)
- **TS:** `flushed(lc)` returns `this.#flushing.promise` (or `promiseVoid` if not flushing). The promise resolves when `#flush()` completes.
- **Rust:** `flushed()` uses a `watch::Sender<Option<CVRVersion>>` channel. It loops checking `pending_rows_version == flushed_rows_version`, then `rx.changed().await`. Also surfaces `flush_error` (TS calls `failService` which tears down the whole service; Rust returns `Err` to awaiters).
- **Impact:** Different mechanism, same semantics. Rust's error handling is more graceful (surfaces error to awaiters instead of killing the service). Architectural, not a divergence.

### F-RRC-3 · Note · `live_count.rs` is Rust-only leak hunting (rule 5)
- `live_count.rs` is a process-global atomic counter for `CVRStore`, `RowRecordCache`, `ClientHandler`, `PokeHandler`, `CVRQueryDrivenUpdater`, `CVRConfigDrivenUpdater`. RAII `Guard` inc/dec. Env-gated backtrace on suspicious drop. No TS equivalent (JS has GC). Legitimate Rust-only addition per rule 5.

### F-RRC-4 · Note · `otel_metrics.rs` faithfully ports all TS OTel instruments
- **TS:** `cvr.flush-time` (histogram), `cvr.rows-flushed` (counter), `cvr.flush_attempts` (counter), `poke.time` (histogram), `poke.transactions` (counter), `poke.rows` (counter).
- **Rust:** All present in `otel_metrics.rs` with same names/types/units. `LATENCY_BOUNDARIES_S` matches TS `LATENCY_HISTOGRAM_BOUNDARIES_S`. `recordSyncFlushStats` → `otel_metrics.rs:93` `record_cvr_flush(flush_type="sync")`, called from `cvr_store.rs:546,1031`. `#recordAsyncFlushStats` → `metrics_callback` in `flush_loop`. Match.

### F-RRC-5 · Note · `load()` / `#ensureLoaded` faithful (cursor 5000, refCounts IS NOT NULL)
- **TS:** `#ensureLoaded` streams rows via `.cursor(5000)`, loads only `WHERE refCounts IS NOT NULL`, keys by `rowIDString`.
- **Rust:** `load()` uses `sqlx::query_as` + `fetch` (streaming), same SQL, same `refCounts IS NOT NULL` filter, keys by `row_id_string`. Match.

### F-RRC-6 · Note · `apply()` faithful (null/refCounts-null → delete, pending tracking, flush trigger)
- **TS:** `apply(rowRecords, rowsVersion, flushed)` — for each `(id, row)`: if `row === null || row.refCounts === null` → `cache.delete(id)`, else `cache.set(id, row)`. If `!flushed` → `pending.set(id, row)`. Sets `pendingRowsVersion`. If `!flushed && flushing === null` → spawn flush.
- **Rust:** `apply(row_records, rows_version, flushed)` — for each `(id, row)`: `None` or `ref_counts.is_none()` → `cache.remove`, else `cache.insert`. If `!flushed` → `pending.insert`. Sets `pending_rows_version`. If `flushed` → also sets `flushed_rows_version` (avoids deadlock). If `!flushed && !is_flushing` → spawn flush. Match.

### F-RRC-7 · Note · `#flush()` loop faithful (READ_COMMITTED, clear pending, advance flushed version)
- **TS:** `#flush()` loops while `pendingRowsVersion !== flushedRowsVersion`. Each iteration: `runTx(READ_COMMITTED)` → `executeRowUpdates(tx, version, pending, 'force')` → `pending.clear()` → `flushedRowsVersion = rowsVersion`. On error: `failService(e)`.
- **Rust:** `flush_loop` loops while `pending_rows_version != flushed_rows_version`. Each iteration: `pool.begin()` + `SET LOCAL statement_timeout=0` + `SET LOCAL idle_in_transaction_session_timeout=60000` → `flush_one_iteration` (same SQL) → `pending.clear()` → `flushed_rows_version = version`. On error: `fail_service(err)` + `flush_error = Some(err)`. Match.

### F-RRC-8 · Note · `catchupRowPatches` faithful (REPEATABLE READ READ ONLY, cursor 10000, excludeQueryHashes)
- **TS:** `catchupRowPatches` — `await flushed()`, `checkVersion`, then `SELECT ... WHERE patchVersion > start AND patchVersion <= end [AND (refCounts IS NULL OR NOT refCounts ?| excludeQueryHashes)]` via `.cursor(10000)`.
- **Rust:** `catchup_row_patches` — `flushed()`, then `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` + `SET LOCAL` + `checkVersion` (compares `version` from `instances`), then same SQL via `fetch` with page size 10000. Match.

### F-RRC-9 · Note · `executeRowUpdates` defer logic matches exactly
- **TS:** `mode === 'allow-defer' && (this.#flushing !== null || rowUpdates.size > this.#deferredRowFlushThreshold)` → return `[]`.
- **Rust:** `mode == AllowDefer && (is_flushing.load() || row_updates.len() > deferred_threshold)` → return `Defer`. Match (default threshold 100 in both).

### F-RRC-10 · Note · `clear()` faithful (cache only, pending preserved)
- **TS:** `clear()` — `this.#cache = undefined` (pending preserved).
- **Rust:** `clear()` — `state.cache = None` (pending preserved). Match.

### F-RRC-11 · Note · `recordSyncFlushStats` faithful (via otel_metrics.rs)
- **TS:** `recordSyncFlushStats(stats, elapsedMs)` — records `cvrFlushTime` with `flush.type=sync`, `cvrRowsFlushed` if `rowsDeferred === 0`.
- **Rust:** `cvr_store.rs:546,1031` calls `otel_metrics::record_cvr_flush("sync", ...)` — same instrument, same attribute. Match.

### SELF-KILLED
- `CustomKeyMap` vs `HashMap<String, RowRecord>`: different data structure, same keying (`rowIDString`). Not a divergence.
- `Drop` impl with pending-write warning: Rust-only (rule 5, no JS GC equivalent). Legitimate.
- `hasPendingUpdates`: TS `#flushing !== null`, Rust `state.flushing`. Match.
- `getRowRecords`: TS returns `Promise<ReadonlyMap>`, Rust returns `Arc<HashMap>` snapshot (O(1) refcount bump). Architectural optimization.
- `#cvr(table)` helper: inlined in Rust. Not a divergence.
- `promiseVoid` (resolved void promise): Rust returns `Ok(())`. Match.

_(Verifier + completeness-critic phases pending.)_

---

## Pair 28 — `ttl-clock.ts` ⇄ `ttl_clock.rs` (parent-side, COMPLETE, CLEAN)

TS: 15 LOC. Rust: 2 LOC. TTLClock is an opaque branded `number` in TS; Rust uses `i64` directly. Trivial identity type — no behavioral surface. CLEAN.

### SELF-KILLED
- `ttlClockAsNumber` / `ttlClockFromNumber`: TS brand-casting helpers; Rust has no equivalent (i64 IS the number). Not a divergence.

---

## Pair 29 — `row-set-signature.ts` ⇄ `row_set_signature.rs` (parent-side, COMPLETE, CLEAN)

Already covered as Pair 22 (F-SIG-1 HIGH in `engine/mod.rs:row_signature_unit` using FxHasher). The CVR crate's `row_set_signature.rs` is a SEPARATE, CORRECT port that uses `h64` (the faithful chained-xxHash32). `hash.rs` is a byte-faithful port of `shared/src/hash.ts`.

### F-CVR-SIG-1 · Note · CVR `row_id_signature_unit` is correct (uses `h64`) — F-SIG-1 bug is in the IVM crate's `row_signature_unit`
- **TS:** `rowIDSignatureUnit(id)` = `h64(rowIDString(id))` (row-set-signature.ts:10).
- **Rust (CVR):** `row_id_signature_unit(id)` (`row_set_signature.rs:16`) = `h64(&row_id_string_cached(id))`. **Match.**
- **Rust (IVM):** `row_signature_unit(table, row_key)` (`engine/mod.rs:96`) = `FxHasher` (WRONG — F-SIG-1).
- **Impact:** The CVR crate's signature computation is correct. The syncer/IVM crate uses the WRONG function. See F-SIG-1 for details.

### F-CVR-SIG-2 · Note · `parse_signature` faithful (None/empty → 0)
- **TS:** `parseSignature(hex)` — `if (!hex) return 0n; return BigInt('0x' + hex)`.
- **Rust:** `parse_signature(hex: Option<&str>)` — `None | Some("") → Ok(0); Some(s) → u64::from_str_radix(s, 16)`. Match (TS `BigInt('0x'+hex)` = Rust `from_str_radix(s, 16)`).

### F-CVR-SIG-3 · Note · `format_signature` faithful
- **TS:** `formatSignature(sig)` = `sig.toString(16)` (lowercase hex).
- **Rust:** `format_signature(sig)` = `format!("{:x}", sig)` (lowercase hex). Match.

### F-CVR-SIG-4 · Note · `row_id_string` / `row_id_string_cached` faithful (lexicographic sort, JSON array format)
- **TS:** `rowIDString(id)` = `stringify([id.schema, id.table, ...tuples(id.rowKey)])` where `tuples` = `Object.entries(normalizedKeyOrder(key)).flat()` (lexicographic sort). Cached via `WeakMap`.
- **Rust:** `row_id_string(id)` = manual JSON buffer `["schema","table",k1,v1,...]` via `serde_json::to_writer`, `normalized_key_order` sorts by `String::cmp` (lexicographic). `row_id_string_cached` uses a `parking_lot::Mutex<RowIdStringCache>` (generational, 64Ki cap).
- **Match.** Both sort lexicographically, both produce the same JSON array, both cache. The `serde_json::to_writer` output is byte-identical to `JSON.stringify` for the same inputs (modulo PATTERN-B number formatting — NaN/Infinity/-0).

### SELF-KILLED
- `h64` / `h32` / `h128` in `hash.rs`: faithful chained-xxHash32 port. `h64 = (xxh32(s,0)<<32) | (xxh32(s,1))` matches TS `hash(s, 2) = (xxh32(s,0)<<32n) + xxh32(s,1)`. `+` vs `|` is equivalent (low 32 bits of `hi<<32` are zero). Match.
- `rowIDHash` / `row_id_hash`: not diffed here (not in this file's scope, deferred to row-key.ts pair).
- TS `WeakMap` vs Rust generational mutex cache: different GC mechanism but same cache semantics. Not a divergence.

---

## Pair 30 — `schema/cvr.ts` ⇄ `schema/cvr.rs` + `seq_replay.rs` (parent-side, COMPLETE)

TS: 359 LOC (row types + SQL DDL + compare functions + row↔record converters). Rust: 135 LOC (row structs + converters only). `seq_replay.rs` is a Rust-only replay binary (no TS twin). The SQL DDL and compare functions are NOT in the Rust file.

### F-CVR-SCHEMA-1 · Med · `DesiresRow.deleted` is `bool` in Rust, `boolean | null` in TS — NULL would crash Rust deserialization
- **TS:** `DesiresRow.deleted: boolean | null` (schema/cvr.ts:148). The SQL DDL has `"deleted" BOOL` with no NOT NULL constraint — NULL is allowed.
- **Rust:** `DesiresRow.deleted: bool` (schema/cvr.rs:49). NOT `Option<bool>`. If the DB column is NULL, sqlx/serde deserialization will fail.
- **Impact:** In practice, the code always sets `deleted` to `true` or `false` on insert, so NULL may never occur. But the schema allows it, TS handles it, and Rust would crash. Low probability but a real type divergence.
- **Contrast:** `QueriesRow.deleted` IS `Option<bool>` in Rust (schema/cvr.rs:40), correctly matching TS `boolean | null`. So the `DesiresRow` case is an oversight.

### F-CVR-SCHEMA-2 · Low · TS `compareRowsRows` has a copy-paste bug (`b.table` vs `b.table`) — not reproduced in Rust (function absent)
- **TS:** `compareRowsRows` (schema/cvr.ts:262) — `const tableComp = stringCompare(b.table, b.table);` — compares `b.table` with ITSELF, always returns 0. The table comparison is a no-op. Should be `stringCompare(a.table, b.table)`.
- **Rust:** No `compareRowsRows` function exists. Rows are likely sorted at the SQL level or via BTreeMap.
- **Impact:** The TS bug means rows with the same `clientGroupID` + `schema` but different `table` are sorted by `rowKey` only, not by `table` first. Since Rust doesn't have this function, it doesn't reproduce the bug. But per Rule 1 ("TS behavior is the spec"), if Rust ever needs to sort rows the same way, it should reproduce the bug faithfully. Currently moot since the function is absent.

### F-CVR-SCHEMA-3 · Note · SQL DDL not ported to Rust (migration-managed)
- **TS:** `createInstancesTable`, `createClientsTable`, `createQueriesTable`, `createDesiresTable`, `createRowsVersionTable`, `createRowsTable`, `createTables`, `setupCVRTables` — all inline SQL DDL strings.
- **Rust:** None of these exist in `schema/cvr.rs` or anywhere in `rust-cvr/src/`. No `CREATE TABLE` for CVR tables found in the Rust codebase.
- **Impact:** The CVR tables are PostgreSQL tables. In the Rust port, the DDL is likely managed by a migration system (Drizzle) or assumed to exist. The `cvr_store.rs` queries against these tables but doesn't create them. This is an architectural difference — TS creates tables inline, Rust relies on external migration. Not a behavioral divergence in the ported functions.

### F-CVR-SCHEMA-4 · Note · `rowsRowToRowRecord` uses `maybe_version_string` (fallible) vs TS `versionFromString` (panicking)
- **TS:** `rowsRowToRowRecord` (schema/cvr.ts:237) — `patchVersion: versionFromString(rowsRow.patchVersion)` — panicking.
- **Rust:** `rows_row_to_row_record` (schema/cvr.rs:97) — `patch_version: maybe_version_string(&row.patch_version)?` — fallible, returns `RowRecordError::Version`.
- **Impact:** Same as F-TYPES-2 — Rust replaces panicking with fallible. Safety improvement, not a divergence.

### F-CVR-SCHEMA-5 · Note · `rowRecordToRowsRow` faithful
- **TS:** `rowRecordToRowsRow` (schema/cvr.ts:245) — maps `RowRecord` fields to `RowsRow`, converts `patchVersion` via `versionString`, passes `refCounts` through.
- **Rust:** `row_record_to_rows_row` (schema/cvr.rs:112) — same field mapping, `version_string(&record.patch_version)`, `ref_counts` converted from `BTreeMap<String, i64>` to `serde_json::Map<String, Value>` with `Value::Number`. Match.

### F-CVR-SCHEMA-6 · Note · All row structs faithful (InstancesRow, ClientsRow, QueriesRow, DesiresRow, RowsRow, RowsVersionRow)
- **InstancesRow:** TS `{clientGroupID, version, lastActive, ttlClock, replicaVersion, owner, grantedAt, clientSchema, profileID}` → Rust `{client_group_id, version, last_active: f64, ttl_clock: f64, replica_version: Option, owner: Option, granted_at: Option<f64>, client_schema: Option<Value>, profile_id: Option}`. Match.
- **QueriesRow:** TS `{..., internal: boolean|null, deleted: boolean|null, rowSetSignature?: string|null}` → Rust `{..., internal: Option<bool>, deleted: Option<bool>, row_set_signature: Option<String>}`. Match.
- **RowsRow:** TS `{clientGroupID, schema, table, rowKey: JSONObject, rowVersion, patchVersion, refCounts: {...}|null}` → Rust with `#[serde(rename = ...)]` for camelCase. Match.

### F-CVR-SCHEMA-7 · Note · `seq_replay.rs` is a Rust-only replay binary (no TS twin)
- 39-line `main()` that reads a program JSON, connects to PG, and runs `rust_cvr::seq_replay::run`. Companion to `agentic/parity/seq/run-ts.mjs`. This is a test tool, not a port. The actual replay engine is in `rust_cvr::seq_replay` (shared with CI gate `tests/seq_diff_pg_test.rs`).

### SELF-KILLED
- `compareInstancesRows` / `compareClientsRows` / `compareQueriesRows` / `compareDesiresRows`: all absent in Rust. Sorting is done at SQL level or via BTreeMap. Not a divergence.
- `rowsRowToRowID`: inlined in `rows_row_to_row_record` in Rust. Match.
- `stringifySorted`: absent in Rust (uses `serde_json` which preserves insertion order; `normalizedKeyOrder` handles sorting). Not needed.
- `InstancesRow` missing `deleted` column in both TS type and Rust struct (though SQL DDL has it). Match.
- `DesiresRow.ttl: Option<f64>` vs TS `number | null`: match (f64 = JS number).
- `DesiresRow.inactivated_at: Option<f64>` vs TS `TTLClock | null`: match (TTLClock is i64/number).

---

## Pair 31 — `schema/types.ts` ⇄ `schema/types.rs` + `parity_check.rs` (parent-side, COMPLETE)

TS: 393 LOC. Rust: 582 LOC (types.rs) + 1648 LOC (parity_check.rs test harness). All version functions, struct types, and `queryRecordToQueryRow` diffed.

### F-TYPES-1 · Low · `maybe_version_string` configVersion bounds check: Rust uses `u32::MAX` (2^32-1), TS uses `Number.MAX_SAFE_INTEGER` (2^53-1)
- **TS:** `versionFromString` (schema/types.ts:330) — `if (configVersion > BigInt(Number.MAX_SAFE_INTEGER))` (2^53-1 = 9,007,199,254,740,991). Throws `minorVersion exceeds max safe integer`.
- **Rust:** `maybe_version_string` (schema/types.rs:~185) — `if (config_version > u64::from(u32::MAX) as u128)` (2^32-1 = 4,294,967,295). Returns `VersionError::ConfigTooLarge`.
- **Impact:** Rust rejects configVersions above 2^32-1 that TS would accept (up to 2^53-1). In practice, configVersions are tiny (incremented per config change), so this is unreachable. But it IS a bounds divergence — Rust is stricter.

### F-TYPES-2 · Note · `cookie_to_version` intentionally absent in Rust (replaced by fallible `maybe_version_string`)
- **TS:** `cookieToVersion(cookie: string | null)` calls `versionFromString(cookie)` which THROWS on malformed input.
- **Rust:** Comment at schema/types.rs:115: "there is deliberately no `cookie_to_version` here. It used to wrap the PANICKING `version_from_string`... All cookie parsing must go through the fallible `maybe_version_string`."
- **Impact:** Rust replaces a panicking function with a fallible one. This is a Rust-specific safety improvement (commented as such per rule 5). The behavior is equivalent for valid cookies; for invalid cookies, TS throws (caught by caller) and Rust returns `Err` (handled by caller). Not a divergence.

### F-TYPES-3 · Note · `version_string` faithful — `configVersion == 0` is falsy in TS, Rust handles `Some(0)` correctly
- **TS:** `versionString(v)` (schema/types.ts:312) — `v.configVersion ? \`${stateVersion}:${versionToLexi(configVersion)}\` : stateVersion`. JS truthiness: `0` is falsy → `configVersion: 0` serializes as bare `stateVersion`.
- **Rust:** `version_string(v)` (schema/types.rs:~125) — `match v.config_version { Some(cv) if cv != 0 => format!("{}:{}", ...), _ => state_version.clone() }`. Explicitly handles `Some(0)` as bare stateVersion. **Match.** Comment documents this explicitly.

### F-TYPES-4 · Note · `one_after` / `cmp_versions` / `max_version` all faithful
- **TS:** `oneAfter(null)` → `EMPTY_CVR_VERSION` (`{stateVersion: majorVersionToString(0)}` = `"00"`). `oneAfter(v)` → bumps `configVersion`.
- **Rust:** `one_after(None)` → `EMPTY_CVR_VERSION` (`state_version: "00"`). `one_after(Some(v))` → `config_version: Some(v.config_version.unwrap_or(0) + 1)`. Match.
- `cmp_versions`: TS null-handling (null < non-null, null == null) → Rust `match (None, None) => Equal, (None, _) => Less, (_, None) => Greater`. Match.
- `cmp_cvr`: TS `(a.configVersion ?? 0) - (b.configVersion ?? 0)` → Rust `a.config_version.unwrap_or(0).cmp(&b.config_version.unwrap_or(0))`. Match (both treat absent as 0).
- `max_version`: TS `!b ? a : cmpVersions(b, a) > 0 ? b : a` → Rust `match b { None => a, Some(b) => if cmp_cvr(&b, &a) == Greater { b } else { a } }`. Match.

### F-TYPES-5 · Note · All struct types faithful (QueryRecord, RowRecord, ClientRecord, ClientState, BaseQueryRecord)
- **QueryRecord:** TS discriminated union (`internal` | `client` | `custom`) → Rust `#[serde(tag = "type")] enum QueryRecord`. Match.
- **RowRecord:** TS `{id, rowVersion, patchVersion, refCounts: Record<string,number> | null}` → Rust `{id, row_version, patch_version, ref_counts: Option<RefCounts>}` where `RefCounts = BTreeMap<String, i64>`. Match (number → i64 is safe since refcounts are always integers).
- **ClientState:** TS `{inactivatedAt?: TTLClock, ttl: number, version: CVRVersion}` → Rust `{inactivated_at: Option<TTLClock>, ttl: i64, version: CVRVersion}`. Match.
- **BaseQueryRecord:** TS `{id, transformationHash?, transformationVersion?, rowSetSignature?}` → Rust `{id, transformation_hash: Option, transformation_version: Option, row_set_signature: Option}` with `skip_serializing_if = Option::is_none`. Match.

### F-TYPES-6 · Note · `query_record_to_query_row` faithful (all 3 variants)
- **TS:** `queryRecordToQueryRow(clientGroupID, query)` (schema/types.ts:345) — 3-way switch: internal → `internal: true, clientAST: ast`; client → `internal: null, clientAST: ast`; custom → `internal: null, clientAST: null, queryName: name, queryArgs: args`.
- **Rust:** `query_record_to_query_row(cvr_id, query)` (schema/types.rs:~460) — same 3-way match. Internal → `internal: Some(true), client_ast: Some(ast)`; Client → `internal: None, client_ast: Some(ast)`; Custom → `internal: None, client_ast: None, query_name: Some(name), query_args: Some(Array(args))`. Match.
- **Note:** Rust uses `Option<bool>` for `internal` (Some(true)/None) vs TS `true | null`. serde_json serializes `None` as `null` (with `skip_serializing_if` NOT set for `internal`). Match.

### F-TYPES-7 · Note · `parity_check.rs` is a 1648-line TS-fixture-driven differential test harness (Rust-only, not a port)
- Covers: hash (h32/h64/h128), rowIDString/hash/signature, LexiVersion round-trip, versionString/versionFromString/cmpVersions, getInactiveQueries, mergeRefCounts, queryRecordToQueryRow, TTL parse/clamp/compare, normalizedKeyOrder, oneAfter, maxVersion, versionToCookie, cmp_cvr, maybe_version_string, CVRConfigDrivenUpdater (putDesiredQueries/markInactive/delete/clear/deleteClient), CVRQueryDrivenUpdater (trackQueries/received/deleteUnreferencedRows), makeRowPatch, ClientHandler poke assembly.
- Reads `agentic/parity/parity-fixture.json` generated by running the real TS implementations. This is the gold standard for parity validation.
- **Not a divergence** — it's a test, not a port. But it provides strong evidence that the functions it covers ARE byte-faithful.

### SELF-KILLED
- `versionToLexi` / `versionFromLexi`: faithful base-36 encoding with length-prefix char. Match (parity_check.rs validates round-trip).
- `EMPTY_CVR_VERSION`: TS `{stateVersion: majorVersionToString(0)}` = `"00"`; Rust `CVRVersion::empty()` = `state_version: "00"`. Match.
- `cvrRecordSchema` (patchVersion field): used in RowRecord. Match.
- `rowIDSchema` / `RowID` struct: TS `{schema, table, rowKey}` → Rust `{schema, table, row_key}` with `#[serde(rename = "rowKey")]`. Match.
- `patchSchema` / `QueryPatch`: TS `{type: 'query', op: 'put'|'del', id, clientID?}` → Rust `#[serde(tag = "op")] enum QueryPatch { Put{id, client_id?}, Del{id, client_id?} }`. Match.
- `PutRowPatch` / `DelRowPatch` / `RowPatch`: TS union → Rust `enum RowPatch { Put{id, contents}, Del{id} }`. Match.
- `maybeVersionString`: TS `(v) => v ? versionString(v) : null` → Rust `v.as_ref().map(version_string)`. Match.

---

## IVM Crate — Pairs 36+ (parent-side, in progress)

### Pair 36 — Core IVM types: `change.ts` + `change-type-enum.ts` + `change-type.ts` + `change-index-enum.ts` + `source-change-index-enum.ts` + `source-change-index.ts` ⇄ `change.rs` + `data.rs` + `stream.rs` + `schema.rs` + `operator.rs` + `constraint.rs`

**All CLEAN — faithful 1:1 ports.**

### F-IVM-CORE-1 · Note · `Change` tuple → enum adaptation faithful
- TS `Change = AddChange | RemoveChange | ChildChange | EditChange` (tuple types `[ChangeType, Node, extra]`) → Rust `enum Change { Add(Node), Remove(Node), Child{node, child}, Edit{node, old_node} }`. Factory functions (`makeAddChange` etc.) match. `ChangeType` enum values (ADD=0, REMOVE=1, EDIT=2, CHILD=3) match.

### F-IVM-CORE-2 · Note · `ChangeIndex` / `SourceChangeIndex` inlined into Rust enums (no tuple indexing)
- TS uses `ChangeIndex.TYPE=0, NODE=1, OLD_NODE=2` and `SourceChangeIndex.TYPE=0, ROW=1, OLD_ROW=2` to access tuple elements by index. Rust's enum variants directly carry the data — no index access needed. Legitimate structural adaptation.

### F-IVM-CORE-3 · Note · `Value` type: TS JSON value → Rust custom enum with reference-identity for JSON
- TS `Value` is `zero-protocol/src/data.ts` JSON value. Rust uses `enum Value { Null, Bool, F64, Str, Json(Arc<str>) }`. `Json` variant uses `Arc::ptr_eq` for equality (matching JS `===` for objects). `js_stringify_value` faithfully implements `JSON.stringify` semantics (NaN/Infinity → "null", -0 → "0") — this IS the PATTERN-B fix.

### F-IVM-CORE-4 · Note · `compareValues` faithful (UTF-8 string comparison)
- TS uses `compareUTF8(a, b)` for strings. Rust uses `x.as_bytes().cmp(y.as_bytes())` — byte comparison is equivalent to UTF-8 comparison since Rust strings are UTF-8. Match.

### F-IVM-CORE-5 · Note · `Stream<T> = Iterable<T>` → Rust `Iterator<Item = StreamItem<T>>`
- TS `'yield'` sentinel string → Rust `StreamItem::Yield` enum variant. `skipYields` → `skip_yields` (filter_map). `take` → `TakeStream` with yield passthrough. `first` → `first` (skips yields). Match.

### F-IVM-CORE-6 · Note · `operator.ts` `push` returns void in Rust (PATTERN-A)
- TS `Output.push(change): Stream<'yield'>` returns a stream that can yield. Rust `Output::push(&mut self, change, pusher)` returns `()` — no yield support. This IS PATTERN-A (yield divergence in push paths). Already documented systemically.
- `Storage` trait: TS uses `JSONValue`, Rust uses `Value`. `scan` returns `Vec` instead of `Stream` (eager vs lazy). Minor architectural difference.

### F-IVM-CORE-7 · Note · `constraint.ts` faithful (all functions match)
- `constraintMatchesRow`, `constraintsAreCompatible`, `constraintMatchesPrimaryKey`, `keyMatchesPrimaryKey`, `pullSimpleAndComponents`, `primaryKeyConstraintFromFilters`, `extractColumn`, `constraintEquals` — all match. `SetOfConstraint` is test-only (TS `assertTesting`). Rust adds `row_matches_multi_constraints` (Go-origin, not TS).

### F-IVM-CORE-8 · Note · `schema.ts` `SourceSchema` faithful
- All fields match. Rust adds `relationship_order: Vec<String>` (TS uses JS object insertion order). `System` enum (Permissions/Client/Test) matches. `ColumnType` enum matches `SchemaValue`.

### F-IVM-CORE-9 · Note · `data.ts` `Node` faithful (row + relationships + rel_order)
- TS `Node = {row, relationships: Record<string, () => Stream<Node|'yield'>>}` → Rust `Node = {row: Row, relationships: HashMap<String, RelStream>, rel_order: Vec<String>}`. `rel_order` preserves insertion order (TS uses JS object). `set_relationship` matches TS spread. `drain_streams` matches.

### Pair 37 — Planner core: `planner-node.ts` + `planner-source.ts` + `planner-terminus.ts` + `planner-constraint.ts` ⇄ Rust counterparts

### F-PLANNER-1 · Note · `PlanDebugger` entirely absent in Rust
- TS planner methods (`propagateConstraints`, `estimateCost`, `plan`) take optional `PlanDebugger` parameter that logs structured events (attempt-start, constraints-propagated, plan-complete, best-plan-selected, node-constraint, node-cost). Rust omits `PlanDebugger` entirely — no debug events.
- **Impact:** Debug/observability only. No behavioral impact on plan selection.

### F-PLANNER-2 · Note · `PlannerNodeWeak` — Rust-only cycle breaking (rule 5)
- TS stores strong upward back-edges (`#output`) and relies on GC. Rust uses `Weak<RefCell<...>>` for back-edges (`PlannerNodeWeak`) to prevent Rc cycles. Legitimate Rust-only addition per rule 5.

### F-PLANNER-3 · Note · `PlannerConstraint` type: TS `Record<string, undefined>` → Rust `BTreeMap<String, Option<Value>>`
- TS constraint values are always `undefined` (just marking column names). Rust uses `Option<Value>` (more general). `merge_constraints` matches (last-wins). `BTreeMap` gives deterministic order. Match with adaptation.

### F-PLANNER-4 · Note · `planner-terminus.ts` `pinned` property absent in Rust
- TS has `get pinned(): boolean { return true; }`. Rust omits it. Not used in planning logic (only in debug output). No impact.

### Pair 38 — Planner graph + join: `planner-graph.ts` + `planner-join.ts` ⇄ `planner_graph.rs` + `planner_join.rs`

### F-PLANNER-5 · Note · `plan()` algorithm faithful (exhaustive 2^n flip enumeration)
- TS: `flippableJoins.filter(j => j.isFlippable())`, `MAX_FLIPPABLE_JOINS = 9`, `2 ** n` patterns, reset → apply bitmask flips → `checkAndConvertFOFI` → `propagateUnlimitForFlippedJoins` → `propagateConstraints` → `getTotalCost` → track best → restore best.
- Rust: Same algorithm, same constant (9), same steps. Match.

### F-PLANNER-6 · Low · FOFI cache rebuilt per-pattern in Rust (TS builds once)
- TS: `buildFOFICache(this)` called ONCE before the pattern loop, passed to `checkAndConvertFOFI(fofiCache)` inside the loop.
- Rust: `check_and_convert_fofi(graph)` calls `build_fofi_cache(graph)` internally — rebuilt on every pattern iteration (2^n times).
- **Impact:** Performance regression (2^n × BFS instead of 1 × BFS + 2^n × lookups). For 9 flippable joins = 512 BFS traversals instead of 1. No behavioral impact — same plan selected.

### F-PLANNER-7 · Note · `estimate_cost` faithful (semi + flipped join cost models)
- Semi: `cost = parent.cost + parent.scan_est * (child.startup_cost + child.cost + child.scan_est)`. Flipped: `cost = child.cost + ceil(child.scan_est / chunk_size) * parent.startup_cost + child.scan_est * (parent.cost + parent.scan_est)`. Both match exactly.
- `scaledChildSelectivity = 1 - (1 - child.selectivity).powf(fanout)` — same. Parent DCS: Semi → `scaled * dcs`, Flipped → `1.0 * dcs` — same.
- `scan_est`, `returned_rows`, `selectivity`, `limit`, `fanout` — all match.

### F-PLANNER-8 · Note · `propagate_constraints` faithful (semi + flipped)
- Semi: child gets `childConstraint`, parent gets forwarded `constraint`. Flipped: child gets `translateConstraintsForFlippedJoin(constraint, parentConstraint, childConstraint)`, parent gets `mergeConstraints(constraint, parentConstraint)`. Both match.

### F-PLANNER-9 · Note · `flip()` faithful, `flipIfNeeded` absent (test-only in TS)
- TS `flip()`: assert semi, assert flippable, set to 'flipped'. Rust `flip()`: `assert!(join_type == Semi)`, `assert!(flippable)`, set to `Flipped`. Match.
- TS `flipIfNeeded(input)`: flips if `input === child`. Only used in tests. Rust omits. No impact.
- TS throws `UnflippableJoinError` when flipping non-flippable. Rust `assert!` panics. Same effect.

### SELF-KILLED (planner)
- `MAX_FLIPPABLE_JOINS = 9`: both. Match.
- `resetPlanningState` / `reset_planning_state`: both reset joins, fanOuts, fanIns, connections. Match.
- `capturePlanningSnapshot` / `capture_planning_snapshot`: both capture limit, join types, fan types, constraints. Match.
- `restorePlanningSnapshot` / `restore_planning_snapshot`: both validate shape, restore connections/joins/fanNodes. Match.
- `propagateUnlimit` / `propagate_unlimit`: flipped join propagates unlimit to child. Match.
- `propagateUnlimitFromFlippedJoin`: propagates to parent. Match.
- `closestJoinOrSource`: join returns 'join', terminus delegates to input. Match.
- `getName` / `get_name`: `"parent ⋈ child"` format. Match.
- `live_count::Guard` on planner nodes: Rust-only leak hunting (rule 5).

### Pair 39 — Planner connection + fan-in + fan-out + builder: `planner-connection.ts` + `planner-fan-in.ts` + `planner-fan-out.ts` + `planner-builder.ts` ⇄ Rust counterparts

### F-PLANNER-10 · Note · `planner-connection.ts` `estimate_cost` faithful (cache, mergedConstraint, scanEst, unlimit)
- TS: caches per branch-pattern key, merges base+propagated constraints, calls model, computes `scanEst = limit === undefined ? rows : Math.min(rows, limit / dcs)`. `unlimit()` clears limit (root connections exempt).
- Rust: Same cache, same merge, same model call, `scan_est = limit.map(|l| rows.min(l as f64 / dcs.max(1e-10))).unwrap_or(rows)`. `.max(1e-10)` prevents division by zero — same result since join guards dcs=0 before reaching connection. `unlimit()` same (root exempt). Match.

### F-PLANNER-11 · Note · `planner-fan-in.ts` `estimate_cost` + `propagateConstraints` faithful (FI: max+same-pattern, UFI: sum+unique-pattern)
- FI: `[0, ...branchPattern]` for all inputs, max of rows/cost/startup/scan, `selectivity = 1 - noMatchProb`. UFI: `[i, ...branchPattern]` per input, sum of rows/cost/scan/startup, same selectivity formula. Both match.
- `propagateConstraints`: FI → same pattern for all, UFI → unique pattern per input. Match.

### F-PLANNER-12 · Note · `planner-builder.ts` `planQuery` / `applyPlansToAST` / `applyToCondition` faithful
- `planQuery`: build plan graph → plan recursively → apply to AST. Match (Rust omits `planDebugger` and `lc` params).
- `applyPlansToAST`: collect flipped join planIds → recursively set `flip` on correlatedSubquery conditions. Match.
- `applyToCondition`: simple → passthrough, correlatedSubquery → set flip + recurse into subquery.where, and/or → recurse. Match.
- TS uses `planIdSymbol` (Symbol key); Rust uses `csq.plan_id` (struct field). Structural adaptation.

### Pair 40 — Remaining IVM runtime files

### F-IVM-RT-1 · Note · `fan-in.ts` / `fan-out.ts` faithful (filter operator pair, ref-counted destroy)
- FanOut: forks stream to multiple outputs, ref-counted destroy (destroys input when all outputs destroyed). FanIn: merges streams, deduplicates, `pushAccumulatedChanges`. Both match.

### F-IVM-RT-2 · Note · `catch.ts` faithful (test-only output collector)
- Collects pushes into arrays, optional fetch-on-push. `expand_change` / `expand_node` convert Change/Node to CaughtChange/CaughtNode. Test-only operator. Match.

### F-IVM-RT-3 · Note · `memory-storage.ts` faithful (BTreeMap instead of BTreeSet)
- TS uses `BTreeSet<Entry>` with UTF-8 comparator. Rust uses `BTreeMap<String, Value>` (natural String ordering = UTF-8). `scan` returns `Vec` instead of `Stream` (eager). Match.

### F-IVM-RT-4 · Note · `union-fan-out.ts` faithful (UfoOutput adapter for re-entrancy)
- TS: generator `*push(change): Stream<'yield'>` calls `fanOutStartedPushing` → push to all outputs → `fanOutDonePushing`. Rust: `UfoOutput` adapter delegates to `push_parent_change`/`push_child_change` (re-entrancy fix, rule 5). Ref-counted destroy matches.

### F-IVM-RT-5 · Note · `flipped-join.ts` `fetch` faithful (constraint translation, child fetch, batched parent fetch)
- TS: translates parent constraint to child constraint (index-based key mapping), fetches child nodes, handles in-progress child change, calls `#fetchBatched`. Rust: same logic. `MULTI_CONSTRAINT_CHUNK_SIZE = 256` in both. `getMultiConstraintChunkSize` / `setMultiConstraintChunkSizeForTest` match.
- Push: TS uses generators with yield. Rust uses `ParentOutput`/`ChildOutput` adapter pattern (rule 5 re-entrancy). PATTERN-A applies to push path.

### F-IVM-RT-6 · Note · `query/ttl.ts` faithful (parseTTL, compareTTL, clampTTL)
- `parseTTL`: number→ms (NaN=0, Inf/neg=-1), 'none'=0, 'forever'=-1, string→parse unit. `clampTTL`: -1 or >10min → 10min. `compareTTL`: forever-aware. All match.
- `normalizeTTL`: one of 32 unresolved behavioral symbols — client-side DSL, not ported. Expected.
- TS `TTL` type includes `number`; Rust takes `&str` only. Numbers converted to strings before reaching Rust. Structural adaptation.

### F-IVM-RT-7 · Note · `query/complete-ordering.ts` faithful
- `completeOrdering`: recursively adds PK columns to orderBy. `assertOrderingIncludesPK`: checks all PK fields present. `addPrimaryKeys`: appends missing PK fields as `[key, 'asc']`. All match.

### F-IVM-RT-8 · Note · `query/error.ts`, `query/typed-view.ts`, `query/validate-input.ts`, `query/metrics-delegate.ts` — client-side types
- `QueryParseError`: error class for query argument parsing. Ported as struct.
- `TypedView`: client-side view interface. Ported as trait.
- `validateInput` + `titleCase`: 2 of 32 unresolved behavioral symbols — client-side DSL (StandardSchema validation). Not ported. Expected.
- `MetricsDelegate`: interface with client/server metric maps. Ported as trait.

### REMAINING IVM FILES (structure-checked, not full line-by-line)

### F-IVM-RT-9 · Note · `push-accumulated.ts` structure matches (merge_relationships, add_empty_relationships, push_accumulated_changes)
- TS: complex function handling accumulated changes from fan-out/fan-in OR sub-graphs (child/add/remove/edit deduplication). Rust: `push_accumulated.rs` has `merge_relationships`, `add_empty_relationships`, `push_accumulated_changes` — same function set. Structure match.

### F-IVM-RT-10 · Note · `snitch.ts` faithful (debug logging operator)
- TS: logs all fetch/push messages. Rust: `Snitch` struct with `to_change_record`. Test-only operator. Structure match.

### F-IVM-RT-11 · Note · `measure-push-operator.ts` faithful (metrics wrapper)
- TS: wraps operator, measures push time via MetricsDelegate. Rust: `MeasurePushOperator` struct + `NullMetricsDelegate`. Match.

### F-IVM-RT-12 · Note · `builder/builder.ts` `buildPipeline` faithful
- TS: `buildPipeline` → `buildPipelineInternal` (recursive): get source → validate NOT EXISTS → uniquify CSQ aliases → gather CSQ conditions → collect split edit keys → determine use_cap → build sort → transform filters → connect source → apply Skip → apply non-flipped EXISTS → apply WHERE → apply limit (Cap/Take) → apply related subqueries.
- Rust: `build_pipeline` → `build_pipeline_internal` — same structure, same steps. `use_cap` logic matches (Cap when no flipped subqueries, Take otherwise). `apply_where` → `apply_filter` or `apply_filter_with_flips`. Related subquery deduplication by relationship_name matches. Match.

### F-IVM-RT-13 · Note · `query/query-delegate-base.ts` structure matches
- TS: `QueryDelegateBase` class with batch view updates, storage creation, transaction commit callbacks. Rust: `QueryDelegate` trait + `QueryDelegateBase` struct with `batch_view_updates`, `create_storage`, `on_transaction_commit`. Structure match.

### F-IVM-RT-14 · Note · `memory-source.ts` → `Source` trait + `MemorySource` + `TableSource` (architectural split)
- TS: single `MemorySource` class implementing `Source` interface (in-memory row store + connect/fetch/push).
- Rust: `Source` trait in `ivm/source.rs` with server-specific methods (`set_db_path`, `set_snapshot_db`, `clear_advance_state`, `column_types`, `set_primary_key` — rule 5). `MemorySource` for tests, `TableSource` in `sqlite/table_source.rs` for production (SQLite-backed). Core methods match: `connect`, `push`, `gen_push`, `get_row`, `table_name`, `primary_key`.

### F-IVM-RT-15 · Note · `array-view.ts` faithful (ArrayView + changeToViewChange + applyChange)
- TS: `ArrayView` implements Output + TypedView, uses `applyChange` from view-apply-change.ts, `changeToViewChange` converts Change→ViewChange. Rust: `ArrayView` struct + `ArrayViewOutput` adapter (re-entrancy, rule 5). Match.

### F-IVM-RT-16 · Note · `builder/filter.ts` `createPredicate` faithful
- TS: recursive AND/OR, simple condition with operators (=, !=, <, >, etc.), LIKE predicate via `getLikePredicate`. Rust: `create_predicate` in `builder/filter.rs`, `build_predicate` in `builder/builder.rs`. Match.

### F-IVM-RT-17 · Note · `query-delegate.ts` → `ZqliteQueryDelegate` (server-side subset)
- TS: `QueryDelegate` interface extends `BuilderDelegate + MetricsDelegate` with `addServerQuery`, `addCustomQuery`, `materialize`, `run`, `preload`, `batchViewUpdates`, `flushQueryChanges`, `onTransactionCommit`. Rust: `ZqliteQueryDelegate` in `sqlite/query_delegate.rs` implements the server-side subset. Client-only methods (materialize, run, preload) are not ported. Expected.

### F-IVM-RT-18 · Note · `union-fan-in.ts` faithful (fanOutStartedPushing / fanOutDonePushing / merge_fetches)
- Rust: `UnionFanIn` struct with `fan_out_started_pushing`, `fan_out_done_pushing`, `merge_fetches`, `output_adapter` (re-entrancy). Match.

### F-IVM-RT-19 · Note · `skip.ts` faithful (getStart logic, maybeSplitAndPushEditChange inlined)
- TS: `Skip` operator with `fetch` (computes effective start from bound + req.start) and `push` (filters by `shouldBePresent`, splits edits via `maybeSplitAndPushEditChange` from dropped file). Rust: `Skip` in `ivm/skip.rs` with same `getStart` logic, `SkipOutput` adapter inlines the edit-split logic. Match.

### F-IVM-RT-20 · Note · `runnable-query-impl.ts` / `query-internals.ts` — client-side DSL
- `RunnableQueryImpl`: extends `QueryImpl` with `run`/`preload`/`materialize` — client-side lifecycle. Rust: `new_runnable_query` is a simple factory (server doesn't materialize client views). Expected.
- `QueryInternals`: Symbol-tagged interface (`queryInternalsTag`). Rust: `QueryInternals` trait. `as_query` does unsafe pointer cast (Rust-specific). `is_query_internals` always returns true (trait-based, not Symbol-based). 3 of 32 unresolved DSL symbols. Expected.

### REMAINING: Client-side DSL files (4 files, 32 unresolved symbols — F-IVM-X1)
- `query/expression.ts` (324 LOC) — `cmpLit`, `eb`, `filterTrue`, `filterFalse`, `filterUndefined`, `isParameterReference`, `titleCase`
- `query/named.ts` (153 LOC) — `normalizeParser`, `syncedQueryImpl`, `withValidation`
- `query/query-impl.ts` (597 LOC) — `asQueryImpl`, `isCompoundKey`, `isOneHop`, `isTwoHop`, `newQueryImpl`
- `query/query-registry.ts` (777 LOC) — `DeepMerge`, `defineQueries`, `defineQuery`, `getQuery`, `isQuery`, `isQueryRegistry`, `mustGetQuery`

These 4 files define the client-side query builder DSL (`defineQuery`, `newQuery`, expression builder, etc.). The server-side Rust port receives already-constructed ASTs from the client, so these DSL helpers are not ported. F-IVM-X1 already identified all 32 symbols as "client-side DSL (expected)" — recommend MAP reclassify from "behavioral ⇒ investigate" to "client-side DSL (expected)." These files are NOT divergences.
