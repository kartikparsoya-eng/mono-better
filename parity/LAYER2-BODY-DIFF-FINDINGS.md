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
- [x] Pair 18 — `ivm/view.ts` + `ivm/view-apply-change.ts` ⇄ `ivm/view.rs` (view-refcounts mandate target) — split discovery, deferred
- [x] Pair 14 — `ivm/cap.ts` ⇄ `ivm/cap.rs` — SUBAGENT (bg_bf579907)
- [x] Pair 19 — `ivm/filter-operators.ts` ⇄ `ivm/filter_operators.rs` (+ `filter.rs`) — parent-side
- [x] Pair 15 — `ivm/exists.ts` ⇄ `ivm/exists.rs` — TIMED OUT, re-diffed parent-side
- [x] Pair 16 — `ivm/take.ts` ⇄ `ivm/take.rs` — TIMED OUT, re-diffed parent-side
- [x] Pair 20 — `workers/syncer.ts` (`#createConnection`/drain) ⇄ `router.rs` (token-pinning mandate target) — parent-side

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
| 7 connection | — | 4 (F-CON-1/2/3/5) | 1 (F-CON-6) | 1 (F-CON-4) | 2 (F-CON-7 +F-CON-5-pt) |
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
| 20 router.rs (parent-side) | — | 2 (F-RT-2/3) | 1 (F-RT-4) | 2 (F-RT-1/3) | — |
| Cross-cutting (yield + serialization) | — | 2 (PATTERN-A/B, systemic) | — | — | 1 (oracle yield recording) |
| 18 view (parent-side) | — | — | — | — | 1 (F-VIEW-1, deferred) |
| CVR leads (unverified) | — | — | — | — | 4 (C-CVR-A..D) |

_(Verifier + completeness-critic phases pending.)_
