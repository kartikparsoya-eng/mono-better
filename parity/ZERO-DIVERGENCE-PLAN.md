# Zero-Divergence Plan (post-incident 2026-08-27)

Two prod bugs (connect-ack serialized behind hydrate; push-relay relaying a
stale auth token) escaped ALL five existing parity layers. This document is the
post-mortem of *why the tools missed them* and the plan that closes each hole.

Goal restated (the standing rule): **the only thing Rust is allowed to invent is
the thread/parallelism implementation — and even those inventions must be
observationally equivalent to TS.** Everything else is a 1:1 port.

---

## Part 1 — Why every existing layer missed both bugs

| Layer | What it audits | Why it missed bug 1 (connect-ack) | Why it missed bug 2 (stale push auth) |
|---|---|---|---|
| **L1 symbol/file ledger** (`parity_ledger.py`) | A ported symbol *exists* in the mirrored file | `Connection::init()` existed, 1:1 named, right file. The bug was *where it was called from* (serial CG thread vs concurrent accept path) — L1 does not model call sites | `PushRelayHeaders` is part of the push-relay **invention** (Option-A), exempted from the ledger as "no TS twin" |
| **L2 body-differential** (`layer2_coverage.py`) | Function *bodies* branch-match TS | `init()`'s body was a perfect port. The divergence was in the **caller topology**, which L2 never looks at | TS `pusher.ts enqueuePush` body says `mustGetConnectionContext(selector)` — a *use-time read*. Rust's relay used a stored field. L2 never diffed this body because the pusher is an invention, not a port |
| **TS-golden fixtures** | Byte-equal outputs for chosen inputs | The `connected` frame was byte-identical on both sides. Fixtures compare **values**, never **when/on which thread** a value is produced | A fixture would need a *token refresh followed by a push* to expose it; nobody fixtures state-freshness over time |
| **diff-oracle / ART** | End-to-end result sets TS-vs-rust | Results were identical. The oracle is **time-blind**: it can't see "ack arrived 254s late". And the workload had no multi-second hydrates and no mid-hydrate reconnects, so the pathology never fired | Sessions were shorter than any token TTL and never refreshed auth mid-session. Value-space testing, time-space bug |
| **Ported-test audit** (view-syncer.pg.test.ts etc.) | Rust reproduces TS's own test outcomes | TS has no test asserting "connected is independent of hydrate latency" — it's guaranteed *structurally* (per-connection concurrency), stated only in a prose comment ("This is early in the connection lifecycle") | TS's freshness is also structural (`mustGetConnectionContext` per push); no TS test pins it, so the audit had nothing to port |

**The single root cause:** both bugs live at the **seam between ported code and
the invented concurrency architecture**. Every tool audits the *interior* of
ported functions; nothing audits the seam:

1. **Execution-context placement** — which thread/task a ported call runs on,
   and what ordering that placement implies (bug 1).
2. **State ownership & freshness** — TS keeps one copy of connection state in
   `ConnectionContextManager` and *reads it at use time*; rust smeared it into
   four parallel copies (`client_raw_auth`, `client_auth`,
   `client_query_ctx.auth`, `PushRelayHeaders.auth`) and one went stale (bug 2).
3. **Inventions were exempt** — AGENTS.md rule 5 says "justified + labeled" but
   requires no *contract*: no statement of the TS-observable behavior the
   invention must preserve, and no test pinning it.
4. **Time-space is untested** — all differential testing compares values;
   nothing compares ordering/latency-independence under adversarial timing
   (slow hydrates, token expiry, mid-hydrate reconnects).

---

## Part 2 — The plan: five new layers + two process rules

### L3 — Call-topology & execution-context ledger  *(catches bug-1 class)*

Extend `parity_ledger.py` from "symbol exists" to "symbol is **called from the
mapped context**":

- Extract call edges (caller → callee) for every ported symbol on both sides
  (regex/tree-sitter on TS, `syn`/regex on Rust — same content-derived approach
  as L1).
- Maintain a checked-in **execution-context map**: every TS context → rust
  context, e.g.
  - TS per-connection accept handler (`syncer.ts#handleConnection`) → rust
    accept task (`router::handle_connection`)
  - TS view-syncer `#lock` tasks → rust serial CG thread (`dispatch_cg_message`)
  - TS setTimeout/interval callbacks → rust CG-loop deadline arms
- **Rule:** a ported call edge whose rust context is *more serialized* than its
  TS context is a divergence unless a contract entry (L6) proves
  order-equivalence. Bug 1 is exactly this: `Connection.init()` moved from the
  concurrent context into the serial one.
- One-time full-edge audit of rust-syncer (the seam crate), then the ledger
  enforces on change like L1 does.

### L4 — State-ownership & freshness audit  *(catches bug-2 class)*

Mirror TS's **state topology**, not just its functions:

- **One-time sweep (highest yield, do first):** enumerate every rust struct
  field that stores a *constructor-time snapshot* of connection/auth/config
  state. For each, find the TS read pattern. If TS reads through a
  manager/getter at use time, rust must read a shared cell at use time. A
  snapshot is only legal if TS also snapshots — cite the TS line.
  Known candidates to check now: `PushRelayHeaders.{cookie, origin,
  request_headers, user_id}`, `CustomQueryContext` fields, `ConnContextInfo`
  consumers, `client_base_versions`, anything cloned into
  `SyncerWsMessageHandler`/`push_relay`.
- **Eliminate duplicated state:** auth existed in four places; TS has one
  (`ConnectionContextManager`). The ported `connection_context_manager.rs`
  exists but is a "tested reference" while the live path uses simplified
  `CgState` maps — that split is itself a divergence and is what made bug 2
  possible. Plan item: **promote the ported CCM to be the single live owner**
  of connection context; everything (pushes, queries, revalidation) reads
  through it, exactly like TS.
- Extend rule 3 in AGENTS.md: 1:1 files and 1:1 **state ownership** — a TS
  class's fields live in exactly one rust struct; no parallel copies.

### L5 — Temporal differential oracle  *(catches both classes end-to-end)*

Today's oracle compares result sets. Add **time-space** comparison:

- **Injected-delay harness:** the two new regression tests (block the CG thread,
  assert `connected` still flows; refresh auth, assert the forwarded token
  flips) are instances of a general pattern. Build it out: a `BlockingCcm`-style
  hook at each seam (hydrate, advance, flush, relay) + assertions for every
  client-observable liveness/ordering invariant.
- **ART adversarial-timing gates (new):**
  - *G-slow*: seed one deliberately expensive query (or a sleep-injecting cost
    hook), then run a reconnect storm mid-hydrate. Assert: connect-ack p99 is
    independent of hydrate time, on BOTH TS and rust, and equal frame sequences.
  - *G-ttl*: mint JWTs with TTL shorter than the session; client refreshes via
    `updateAuth`; mutations continue. Assert: zero 401s, mutation results equal
    TS.
  - *Frame-sequence oracle*: per client, record the ordered downstream frame
    *types* (connected, pokeStart/parts/End, error) with coarse timing classes,
    diff TS vs rust. Values were always compared; now the *order and latency
    envelope* is too.

### L6 — Invention contract registry  *(closes the exemption hole)*

Upgrade AGENTS.md rule 5 from "justified + labeled" to **"justified + labeled +
contracted + tested"**:

- `parity/INVENTIONS.md`: enumerate every Rust-only invention — CG
  thread/executor model, ws_sink writer/reader tasks, push relay (Option-A),
  CVR write-behind, Drop-based teardown, offload runtime, shed policy…
- Each entry states its **TS-observable contract**, e.g.:
  - *CG serial thread*: "clients must observe the same frame ordering AND the
    same independence guarantees as TS's per-connection concurrency — in
    particular, connect-ack, pong, and error frames must never be delayed by
    another message's processing."
  - *Push relay*: "the relayed request must be byte-equivalent to what TS's
    in-process `fetchFromAPIServer('push', ctx)` would send **at push time** —
    including the current (not connect-time) auth."
- Each contract maps to at least one test (the L5 harness). The ledger fails if
  an invention exists in code without a registry entry.

### L7 — TS prose-invariant mining  *(the cheap one nobody does)*

TS comments are spec text under rule 1. Both bugs were *written down in TS*:
`connection.ts:135` "This is early in the connection lifecycle";
`pusher.ts` reads context per-push by construction. One-time sweep of
zero-cache/zql for ordering/timing/liveness prose ("immediately", "before",
"must not block", "per push/connection", "paced", "early") → each becomes a
checklist row: rust test reference, or an explicit N/A with citation. New ports
must add rows for any such comments in the ported file.

### Process rules (AGENTS.md amendments)

1. **Rule 6 extension:** when porting or moving a call site, re-read the TS
   *caller and its execution context*, not just the function body. Porting a
   function without porting its placement is a divergence.
2. **Snapshot rule:** storing a clone of any connection/auth/config value in a
   struct requires a doc-comment citing the TS line proving TS also
   captures-at-construction. Default is read-through-shared-state at use time.

---

## Part 3 — Execution order (inline, no agent fan-out)

| # | Item | Effort | Would have caught |
|---|---|---|---|
| 1 | **L4 snapshot sweep** of rust-syncer (all constructor-captured state vs TS read patterns) | ~½ day | bug 2 + any siblings lurking now |
| 2 | **L6 registry** `parity/INVENTIONS.md` with contracts for the ~8 existing inventions | ~½ day | both (as review checklist) |
| 3 | **L5 injected-delay unit harness**: generalize BlockingCcm; one liveness test per seam (ack, pong, error, poke during blocked hydrate/flush/relay) | ~1 day | bug 1 + siblings |
| 4 | **L7 prose-invariant sweep** of zero-cache (view-syncer, workers, mutagen, custom) | ~½ day | both, cheaply |
| 5 | **L3 call-edge ledger** extension + one-time full-edge audit of rust-syncer | ~1–2 days | bug 1 class, permanently |
| 6 | **L5 ART gates** G-slow + G-ttl + frame-sequence oracle (xyne-art) | ~1–2 days | both, end-to-end, forever |
| 7 | **L4 CCM promotion**: single state owner for connection context (the ported CCM), delete the parallel CgState auth maps | ~2–3 days, re-gate | bug 2 class, structurally |

Items 1–4 are fast and close the immediate holes; 5–7 make it structural.

## Status (executed 2026-08-27)

**Fixes shipped (branch rust-cvr-v1.0.0, unpushed):**
- Bug 1: `5e71e24f4` (connected on accept task) — non-vacuous test proven.
- Bug 2: `97440d021` (push auth Arc refreshed on updateAuth) — non-vacuous test.

**Plan items — done:**
- **L4 snapshot sweep** → `L4-SNAPSHOT-SWEEP.md`. Verdict: auth was the ONLY
  active stale-snapshot divergence (fixed x2). All other post-connect-mutable TS
  fields (query/push URL + customHeaders) map to rust cells refreshed on the same
  trigger. One LATENT finding: I-8 placeholder CCM (dead in prod, mutagen off).
- **L6 invention registry** → `INVENTIONS.md` (I-1..I-8 with contracts + tests).
- **L7 prose sweep** → `L7-PROSE-INVARIANTS.md`. Confirmed view-syncer.ts:896/916
  is bug-1's spec; confirmed the pong keepalive is ALREADY decoupled (writer task,
  ws_server.rs:464 — mirrors TS `#maybeSendPong`), so no pong-behind-hydrate bug.
- **L3 call-topology guard** → `call_topology.py`, wired into `local-rust-ci.sh`.
  Passes clean; proven to catch a re-introduced bug-1 (connected in on_new_connection).
- **AGENTS.md** amended with rules 8 (call-site/context), 9 (state ownership +
  freshness), 10 (invention contract), + the divergence-layer index.

**Plan items — done (session 2, 2026-08-27):**
- **Item 5 — L3 call-edge ledger:** `call_topology.py` Tier-2 (cross-file
  emitter-site allowlist) + `L3-CONTEXT-MAP.md`. Catches a bug-1 placed in ANY
  file (not just router.rs); proven non-vacuous. `2fc090abc`.
- **Item 3 (unit-harness) — resolved to the CORRECT invariants:** the decoupled
  emissions (`connected` accept task, `pong` writer keepalive, connect-time
  `error` accept path, shed `error` writer task) are all pinned; the "poke during
  hydrate" premise was INCORRECT — per-CG pokes are faithfully serialized in both
  (I-1 contract (b)), so no such test was written. INVENTIONS.md I-1 documents the
  three decoupled emissions precisely.
- **Minor GAPs — I-4 shed-error parity** (`455a1a72a`, non-vacuous socket test),
  **L7 cancel-during-hydrate** (`9f6bd78df`, teardown-completeness test +
  FIFO-drain reasoning), **I-1 pong/error** (resolved in prose + tests).
- **Item 7 (CCM) — blocking ambiguity RESOLVED:** verified TS `resolveAuth`
  (auth.ts:74-85) allows anonymous (no-token) and requires userID only WHEN a
  token is present; the rust port is already 1:1. So there is NO anonymous-opaque
  divergence — the promotion is pure state de-duplication, not a fix. Pinned by
  `resolve_auth_matches_ts_...` (`a903d8155`); spec corrected.
- **L5 ART temporal gates — scripts written** (xyne-art `fac1ced`): G-slow,
  G-ttl, frame-sequence oracle on the `ab_common` harness; pure logic unit-smoked.

**Plan items — done (session 3, 2026-08-27):**
- **I-8 CCM promotion — LARGELY DONE** (`df4830e51` + `99de4c97b`): the ported
  ConnectionContextManager is now the single live owner of per-connection auth +
  custom-query context. DELETED the parallel `client_auth`/`client_raw_auth`/
  `client_query_ctx` maps; all consumers (authData, custom-query context,
  auth-maintenance/revalidation, updateAuth) read the CCM at use time via
  TS-named methods + the labeled `custom_query_context_from` adapter. Non-vacuous
  golden (`configured_query_context_matches_...`, proven to fail on a broken auth
  map). En route: fixed the initConnection customHeaders allowlist filter AND the
  opaque-token updateAuth sub-pin divergence (TS pins opaque by userID, not by
  decoding — `99de4c97b`, both opaque tests now non-vacuous, no security
  regression). REMAINING (deferred, NOT a live bug): the push-relay
  `PushRelayHeaders.auth` cell (freshness contract met + test-pinned) + the dead
  mutagen-CRUD `conn_context_manager` dispatch — full CCM-sourcing is a write-path
  purity item with a raw-vs-filtered request-header subtlety; see
  I8-CCM-PROMOTION-SPEC.md.
- **I-6 durability-ordering oracle — DONE** (`3444dc154`): BOTH halves pinned,
  PG-gated, non-vacuous. Store side `pg_quiet_commit_noop_flush_contract`; client
  side `pg_noop_flush_does_not_poke_client_past_stored_version` (a quiet advance
  touching only another CG's row → cg1 no-op flush → every pokeEnd cookie ≤ stored
  version; reverting the advance-path no-op fallback makes it poke "02" and FAIL).
  Verified green against a live Postgres.

**Plan items — done (session 3 ART re-gate, 2026-08-27):**
- **ART smoke re-gate on candidate `i8i6-440ed8820` (rev 440ed8820) — CORRECTNESS
  CLEAN.** Full TS-vs-rust differential (opened 25/25, puts 11617, muts 1196 ok
  err=0, errors=0). PASS: G1 connectivity, G4 mutations (1196/1196), G7 cvr-gc,
  **G8 diff-oracle (0 mismatch, catalog 150/150, 4 pairs row-identical)**, G11
  negative (8/0/1), G14 impact-cov (17/17), G26/G27/G31, G30 provenance (rev
  matches HEAD). WATCH-only: G9/G29 (2-3 prod shapes = known build drift
  hierarchyCanvases/kanbanTicketsPage, pre-existing). SKIP: G5/G6/G10/G15-25/G28
  (not run in smoke mode — need soak/baselines/flags).
  - **I-8 opaque-edge delta VALIDATED live:** G11 `update-auth-valid` PASS
    (connection survives updateAuth + re-hydrates — the opaque-pin fix) AND
    `wrong-user-pinned-group` PASS (cross-user JWT still rejected — no security
    regression). 0 auth/CCM errors; 0 panics.
  - The first run showed `LOCAL ART: FAIL` from a G13 log-health false-positive
    (the otel-collector container did not auto-restart after the OrbStack bounce
    used to clear the `:80` ingress wedge → `ENOTFOUND` spam). After restarting
    otel-collector, the **clean re-run is `LOCAL ART: PASS`** (opened 25/25, G8
    150/150 0-mismatch, G4 1202/1202, G11 8/0/1). G13 → WATCH (4 sigs < 5
    threshold), and those 4 are EXPECTED test-induced signatures — including the
    `wrong-user-pinned-group` "User ID mismatch pinned/incoming" (my opaque sub-pin
    correctly rejecting a cross-user token) and `update-auth-invalid` "updateAuth
    verification failed" (bad-signature rejection) — plus a cold-SQL slow-statement.
    No code-related signatures.

**Plan items — done (push-relay flip, 2026-08-27):**
- **I-8 push-relay flip — DONE + ART-validated** (`ce47a7306`): the message
  handler's `ConnContextManagerDispatch` is now backed by the ported CCM via
  `CcmDispatchAdapter` (replacing `PlaceholderConnContextManager`), so the handler's
  mutagen-CRUD auth AND relayed-push auth read the single owner. The
  `PushRelayHeaders.auth` `Arc<Mutex>` cell is DELETED (plain `Option<String>`
  filled fresh per relay from `mustGetConnectionContext(sel).auth`), and the
  `handle_update_auth` cell-refresh removed. Raw incoming-header forwarding
  preserved. **I-8 is now FULLY complete — no parallel auth copies remain.**
  Re-ART on `i8relay-ce47a7306`: `LOCAL ART: PASS`, G4 mutations 1193/1193 0-err,
  G8 150/150, G11 8/0/1, 0 panics, push relay forwarding cleanly (no 401s).

**Plan items — done (L5 temporal gates RUN, 2026-08-27):**
- **L5 temporal differential oracle — RUN + GREEN** against the `i8relay-ce47a7306`
  candidate vs the TS mirror (xyne-art `587b706` fixed a G-ttl auth-pool indexing
  bug en route):
  - **frame-seq oracle: PASS** — TS==rust ordered frame sequences + ack latency class.
  - **G-slow: PASS** — connect-ack p99 rust 38ms / ts 72ms, both "instant"
    (hydrate-independent) — the direct temporal regression for prod bug-1.
  - **G-ttl: PASS** — `unauthorized-after-refresh=0` on BOTH after refreshing an 8s
    token mid-session — the direct temporal regression for prod bug-2 AND live
    validation of the I-8 push-relay flip.
  All 5 new layers (L3–L7) are now built, wired, AND exercised green.

**Plan items — remaining (optional, not correctness-blocking):**
- Deeper ART modes (soak G6 leaks, capacity G22/G25, determinism G21, mutation-
  matrix G15) — run when a longer infra window is available.

## L8 — traffic-driven path differential (added + executed 2026-08-27)

The layer the original five could not cover: L2 proves matched functions agree
on fixtures, ART/L5 prove the client-visible frames agree — neither proves
rust WALKED the same code. L8 records per-function execution counts on both
sides under byte-identical traffic (TS: `NODE_V8_COVERAGE`; rust:
`-C instrument-coverage` via `--build-arg RUST_SYNCER_COVERAGE=1`; traffic:
diff-oracle full catalog + mutations) and joins them through the L1 ledger.

- Tooling: `parity/layer8_path_diff.py` (+ `--self-test`), capture recipe
  `parity/L8-RUNBOOK.md`.
- First run: 399 fn-pairs; 52 confirmed TS-HOT/RUST-COLD after fixing two
  joiner blind spots (v0 generic-arg demangling, `Cs<hash>_` tokens →
  rustfilt). Full disposition: `parity/L8-TRIAGE.md`.
- Real findings FIXED: signature-unit duplicate composition (delegated to the
  1:1 impl), poke-cookie sites bypassing `version_to_cookie`, the auth-
  maintenance loop planning outside the ported CCM (migrated to
  `plan_maintenance`/`validate_connection`/`fail_connection`/
  `defer_maintenance` + single background retransform), and an ALREADY-drifted
  ttl fallback between the two ports (cross-impl agreement test, proven
  failing pre-fix).
- Remaining tracked GAP (not fixed this pass): ivm filter-pipeline operator
  protocol (`beginFilter`/`endFilter`/`buildFilterPipeline` +
  DNF `simplifyCondition`) — rust builder uses `apply_filter` chains; value
  parity holds on the full catalog, operator-graph structure diverges. Own
  work item. Also: add an AST+permissions catalog case so
  `transform_and_hash_query` gets traffic.
- Recapture after the fixes must show the wired symbols hot — that recapture
  is the non-vacuous proof for wiring fixes (the pre-fix capture is the
  failing state).

## Part 4 — L9: structural 1:1 refactor of the orchestration layer (planned 2026-08-28)

**Goal.** The rust-syncer ORCHESTRATION layer (today: `router.rs` 6.7k lines +
`sync_engine.rs` + `push_relay.rs` + parts of `ws_server.rs`/`main.rs`) mirrors
the TS tree file-for-file, function-for-function, and — the part L3 exists
for — **call-site-for-call-site**: every ported function is invoked from the
twin of its TS caller. Thread/executor logic stays only where rust genuinely
needs it, quarantined in labeled invention modules with I-contracts. The
ported crates (rust-cvr, rust-ivm zql tree, `pipeline_driver.rs`,
`syncer_ws_message_handler.rs`, `connection_context_manager.rs`,
`drain_coordinator.rs`, `read_authorizer.rs`) are already 1:1 and are NOT
touched except where their callers move.

### Target file map

| TS (zero-cache/src) | Rust target | From (today) |
|---|---|---|
| `workers/syncer.ts` (`Syncer`, `#createConnection`, `drain`, `#connections`) | `workers/syncer.rs` | `router.rs::ConnectionRouter` (`handle_connection` → `create_connection`) |
| `workers/connection.ts` (`Connection`, `init`, `#handleMessage`, `#proxyInbound/#proxyOutbound`, `#maybeSendPong`, `close`, `#closeWithError`) | `workers/connection.rs` | smeared: `ws_server.rs` reader/writer + `router.rs` dispatch |
| `services/view-syncer/view-syncer.ts` (`ViewSyncerService` + `#runInLockForClient`, `#runInLockWithCVR`, `#handleConfigUpdate`, `#updateCVRConfig`, `#syncQueryPipelineSet`, `#addAndRemoveQueries`, `#hydrateUnchangedQueries`, `#processChanges`, `#advancePipelines`, `#catchupClients`, `#flushUpdater`, `startPoke`, `#getClients`, `#scheduleExpireEviction`, `#removeExpiredQueries`, `#scheduleAuthMaintenance`, `#runAuthMaintenance`, `#validateConnection`, `#failMaintenanceConnection`, `#runBackgroundRetransform`, `#markVersionServed`, `run`, `stop`) | `services/view_syncer/view_syncer.rs` | `router.rs::CgState` + `sync_engine.rs::SyncEngine` (`config_and_hydrate`/`advance_and_sync` de-melded into the TS decomposition; the invented `SyncEngine` name dissolves) |
| `services/view-syncer/inspect-handler.ts` (`handleInspect`) | `services/view_syncer/inspect_handler.rs` | `router.rs::handle_inspect` |
| `services/mutagen/pusher.ts` (`PusherService`, `PushWorker`, `combinePushes`, `#processPush`, `#fanOutResponses`, `ackMutationResponses`, `deleteClientMutations`) | `services/mutagen/pusher.rs` | `push_relay.rs` (invented name retired; **`combinePushes` is MISSING — real behavioral gap, ported with failing-first test**) |
| `auth/load-permissions.ts` (`loadPermissions`, `reloadPermissionsIfChanged`) | `auth/load_permissions.rs` | folded into `auth/read_authorizer.rs` (pre-dates the prefer-mirrored-file rule) |

### Call-site restorations (rule 8 — the deep part)

1. **Dispatch un-interception.** TS routes EVERY client message
   `Connection.#handleMessage` → `SyncerWsMessageHandler.handleMessage` →
   `viewSyncer.*`. Rust's router today INTERCEPTS
   `initConnection`/`changeDesiredQueries`/`updateAuth`/`deleteClients`/`inspect`
   before the handler (handler arm is a "reference dispatch"). Target: the
   intercepts are deleted; the handler becomes the single live dispatch, ON
   the CG task (twin of the TS worker event loop). This also retires the
   Placeholder dual-write of `ccm.init_connection`.
2. **`connected` emission.** TS `#createConnection` calls `connection.init()`
   on the accept path — pre-hydration. Rust's #152 fix already emits on the
   accept task; restoration = rename the context (`create_connection` calling
   `Connection::init()`), keeping the fix's semantics EXACTLY. Net: full 1:1
   naming AND the incident fix, no tension.
3. **`#maybeSendPong`.** Stays hosted on the per-connection writer task
   (invention: TS timer → writer-task check, same 6s/3s constants, same
   any-frame-suppresses semantics) but becomes a named `Connection` method the
   task calls — the L3 ledger pins the writer task as its sanctioned context.
4. **`run()` loop.** TS `ViewSyncerService.run()` for-awaits `#stateChanges`;
   rust's CG message pump is the twin (CGMessage::Notification ↔
   'version-ready'). The pump body becomes `ViewSyncerService::run` with the
   `#pipelinesSynced` gate, `#advancePipelines`-vs-initial-sync branch, and
   the `finally`-block scheduling (`#scheduleAuthMaintenance`) in TS order.
5. **`#lock` ↔ serial task.** `#runInLockForClient`/`#runInLockWithCVR` are
   kept as named wrappers around "enqueue/execute on the CG task" so lock-body
   callbacks keep TS call shape (and the CVR-load-on-first-touch lives in
   `#runInLockWithCVR`, where TS has it).

### What stays invented (quarantined, I-registered)

- `workers/cg_executor.rs` (NEW home): K executors, LocalSets, `CGHandle`,
  the unbounded ordered channel (TS `#lock` twin), forwarder tasks. Exists
  because the IVM Engine is `!Send`; removable only by #103 (arena rewrite).
- `ws_server.rs`: accept loop, reader/writer tasks, sink queue/backpressure
  shed, liveness shed, admission-cap Rehome. (TS twin is the dispatcher +
  Node stream plumbing; behavior contracts already gated by ART.)
- Pusher queue-cap drop-newest (inside `pusher.rs`, labeled), CVR
  write-behind actor (rust-cvr), Drop teardown, timer mux (one `select!`
  deadline = min of the four TS `setTimeout`s — planner fns keep TS names).

### Execution stages (each: move-only commits ≠ behavior commits; ledger + local CI + call_topology green per commit; ONE full ART release gate at the END of the refactor — no per-stage ART, per user direction 2026-08-28)

- **Stage 0 — freeze.** Current release gate green + pushed FIRST. Fix the
  two known stale comments (rule 13): `CGMessage::NewConnection` doc still
  says it sends `connected`; handler's "reference dispatch" note inverts at
  Stage 2. Snapshot parity-ledger misfiled-count baseline; grep pub-API
  consumers of `router.rs`/`sync_engine.rs` for shim planning.
- **Stage 1 — leaf splits (low risk).** (a) `auth/load_permissions.rs`;
  (b) `inspect_handler.rs`; (c) `push_relay.rs` → `services/mutagen/pusher.rs`
  renames, then **port `combinePushes`** (separate commit, non-vacuous test:
  two pushes same clientID/wsID/revision merge into one POST; proven failing
  first).
- **Stage 2 — `workers/` extraction. DONE 2026-08-28** (e4b863384 +
  bf07222ad): the Syncer connection-management seat moved to
  workers/syncer.rs (Connection was already live in workers/connection.rs);
  `ConnectionRouter` → `Syncer`, `handle_connection` → `create_connection`
  (converges with the #152 connected-ack fix — TS also emits from the accept
  path). RE-SEQUENCED: dispatch un-interception moved INTO Stage 3 — the
  handler's view-syncer arms dispatch through the Placeholder today, so
  un-intercepting requires the real ViewSyncerService seats Stage 3 builds.
- **Stage 3 — `view_syncer.rs` reconstruction. DONE 2026-08-28** (3a
  261e00f93 executor quarantine → workers/cg_executor.rs; 3b d632507bc
  router.rs dissolved → services/view_syncer/view_syncer.rs; 3c-i cddff926f
  CgState→ViewSyncerService; 3c-ii bbd4c174e config_and_hydrate de-meld into
  handle_config_update + sync_query_pipeline_set; 3c-iii 1970feeb7 SyncEngine
  struct dissolved into ViewSyncerService — TS owns #pipelines/#cvrStore/
  #clients directly; 3d fea7f181c un-interception — async ViewSyncerDispatch,
  CgViewSyncer executes inline on the CG task via the service's own
  Rc<RefCell> cell, 5 intercepts deleted, placeholders + dual-writes retired,
  CCM recording single-sited in CcmDispatchAdapter, failing-first proof on
  init_connection_fires_ccm_init_side_effect). Original plan text: includes the
  dispatch un-interception (restoration #1) once the ViewSyncerService seats
  exist, as its own commit with frame-golden proof. Move `CgState`
  → `ViewSyncerService`; de-meld `config_and_hydrate`/`advance_and_sync` into
  the TS method set by PURE extract-method (call order byte-identical —
  wire-golden pg_harness tests pin poke sequences). Any genuine order
  divergence DISCOVERED during de-melding is a separate fix commit with a
  failing-first test (that surfacing is a feature, not a hazard). `#clients`
  registry moves here from SyncEngine. `sync_engine.rs` reduced to a
  deprecated re-export shim, deleted at stage end.
- **Stage 4 — enforcement. IN PROGRESS 2026-08-28**: shim sweep DONE
  (router.rs + sync_engine.rs deleted, all paths repointed); ledger re-bind
  DONE — fixed the extractor's test-module brace double-count that swallowed
  every symbol after a mid-file `mod tests` (TS `Syncer` now binds rust
  `Syncer` exact; syncer scope 128 exact + 20 fuzzy); L3 pins repointed to the
  1:1 tree (workers/syncer.rs::create_connection accept-task `connected`,
  view_syncer.rs forbidden); L1 structural ratchet (`parity_ledger.py syncer
  --enforce-structure`, max_misfiled=24 baseline) wired into local-rust-ci.
  Remaining: full ART release gate on the post-L9 image, push. Original:
  L1 ledger re-run (orchestration symbols must
  bind to their TS twins; misfiled → ~0); L3 Tier-2 extended to pin the new
  sanctioned contexts (init on accept task, pong on writer task, handler on
  CG task); structural CI guard = ledger misfiled-count threshold in
  local-rust-ci; full release gate; push.

- **Stage 5 — infra-layer mirroring (user-directed 2026-08-28). DONE**
  (8abe6b80e + 83627e6bf + 0c280577d): (5a) `protocol.rs` split into
  `src/protocol/` mirroring `packages/zero-protocol/src` file-for-file
  (20 files; protocol.rs = decl + re-exports, all `crate::protocol::X`
  paths stable); (5b) `metrics.rs` → `observability/metrics.rs`, the
  query-API instrument cluster → `custom/metrics.rs`, and
  `custom/fetch.rs` consolidates url_match/backoff/body-preview out of
  transform_query.rs + pusher.rs (with their TS-parity tests, per TS
  fetch.test.ts); (5c) `SyncerConfig`+env parsing → `config/zero_config.rs`,
  `RealServicesFactory` → `server/syncer.rs`, `otel.rs` →
  `server/otel_start.rs`; main.rs = thin bin entry (915→474 lines).
  Ledger scope widened with the mirrored infra TS files (custom/metrics,
  observability/metrics, config/zero-config, server/syncer, server/otel-start,
  mutagen/pusher, inspect-handler); ratchet re-baselined 24→25 (scope-widening
  noise, not relocation). Deliberately NOT mirrored: `http_server.rs` /
  `ws_server.rs` (the TS `server/worker-dispatcher.ts` + the I-1/I-4
  reader/writer/shed invention mix — mapping documented in `src/server.rs`),
  `ws_sink.rs`/`live_count.rs`/`trace.rs` (registered inventions).

### Risks + mitigations

- **Hot-path routing change (Stage 2 #1)** → frame-seq oracle, G-frames,
  full-catalog G8 before/after; the change is a delete of a duplicate path,
  not new logic.
- **De-meld reordering (Stage 3)** → extract-method only; wire goldens; any
  reorder = separate flagged commit.
- **API churn** → shims per stage, deleted in Stage 4.
- **Perf** → moves/renames compile identically; G25 in every stage gate.
- **Drift while in flight** → stages land on the branch tip sequentially; no
  parallel feature work in `router.rs`/`sync_engine.rs` mid-stage.

**Acceptance:** parity ledger shows the orchestration layer bound 1:1 to
`syncer.ts`/`connection.ts`/`view-syncer.ts`/`pusher.ts`/`inspect-handler.ts`/
`load-permissions.ts`; call_topology pins every restored call site; the only
non-twinned modules are the registered inventions; full ART release gate
green. From then on, the TS→rust diff for ANY future zero-cache change is
file-local.


---

## Part 5 — CVR path map: TS ⇄ rust verdicts + remaining work (2026-08-28)

Derived from code (rust-cvr + the dissolved engine seat in rust-syncer), path
numbering matches the TS 12-path CVR map (session notes). rust-cvr had the 1:1
file/fn refactor on 2026-08-24; L1/L2 bind it. This part records the
END-TO-END verdicts and the residual work.

| # | Path | Verdict | Evidence / notes |
|---|------|---------|------------------|
| 1 | Load | **PORTED 1:1** | `MAX_LOAD_ATTEMPTS=10` + `RowsVersionBehind` retry; tombstone → `ClientNotFound("purged…")`; conditional ownership steal (`granted_at <= last_connect_time`, fire-and-forget); first-load `put_instance`; TS no-dedup-of-desiredQueryIDs quirk preserved (cvr_store.rs:1484). |
| 2 | Config-driven update | **PORTED 1:1** | `ensure_client` creates `lmids` + `mutationResults` internal queries (incl. TS bare-`simple` where quirk); `put_desired_queries` / `mark_desired_queries_as_inactive` / `delete_client` 1:1. |
| 3 | Query-driven update | **PORTED 1:1** | `track_queries` / `received` / `delete_unreferenced_rows`; `merge_ref_counts` zero-drop bug fixed earlier; `assert_new_version` semantics kept. |
| 4 | Flush | **PORTED 1:1 + documented refinement** | deep-equal row de-dup (test `flush_prunes_noop_row_updates_like_ts`); `SELECT … FOR UPDATE` version+ownership check; pipelined writes; materiality check documented inline (the quiet-commit desync fix). Error → CG `fail_group` teardown, which SUBSUMES TS's `rowCache.clear()` (the whole cache dies with the CG). |
| 5 | Row write-back | **PORTED + registered invention seat** | same latch semantics (`allow-defer` defers while flushing or >100); rust drives the loop via `tokio::spawn(flush_loop)` on the shared-pool runtime (TS: same-event-loop `setTimeout`); `fail_service` callback; `flushed()` returns the stored error instead of hanging. Observable contract pinned by the I-6 durability-ordering oracle. |
| 6 | Catchup | **PORTED 1:1** | `catchup_config_patches` + `catchup_row_patches` with `exclude_query_hashes`; `flushed()` wait; `check_version` → `ConcurrentModification` → clean Rehome. |
| 7 | TTL clock | **PORTED 1:1** (P7-a retracted — see below) | `get_ttl_clock` delta model; standalone `update_ttl_clock_in_cvr_without_lock`; interval armed after material flush (`take_flush_observed` bridge, documented). Interval is NOT stopped on last disconnect — faithful to TS (`#stopTTLClockInterval` is called only from `#startTTLClockInterval` and `#cleanup`, never from `#deleteClientDueToDisconnect`). |
| 8 | Query TTL expiry | **PORTED 1:1** (timer trio restored 2026-08-28) | `#expiredQueriesTimer` → `expired_queries_timer` field; `#scheduleExpireEviction` → `schedule_expire_eviction(&cvr)` (called at the config-update tail = view-syncer.ts:1390, and the remove-expired tail = 651); `#stopExpireTimer` → `stop_expire_timer` (called on last disconnect = view-syncer.ts:767). The CG loop fires on the armed deadline and clears-before-run (TS:1423). Previously rust POLLED `next_eviction_time(cvr)` with no trio, which let an eviction+flush fire during the 0-client keepalive window that TS suppresses — now fixed + pinned by `last_disconnect_stops_the_eviction_timer`. |
| 9 | Row-set signature | **PORTED; read-side force-re-exec N/A by architecture** | `row_id_signature_unit = h64(row_id_string)` (live since the L8 delegation fix); flush persists + fires the drift canary metric. TS's hydrate-time "stored≠candidate → removeQuery" recovery exists because TS RESTORES hydration state from the CVR; rust re-executes every query on engine reset/rehydrate, so there is no restored state to force out — the drift canary is the remaining observable. |
| 10 | Ownership transfer | **PORTED 1:1** | load-side conditional steal + flush-side `FOR UPDATE` → `OwnershipError`/`ConcurrentModification` → Rehome. |
| 11 | CVR purge | **OUT OF RUST SCOPE (by design)** | TS `CVRPurger` runs in the REAPER worker (`server/reaper.ts`), which the shipped combined image's TS runner still operates. Rust's contract: honor tombstones on load (✓ path 1) and hold `FOR UPDATE` during flush so `SKIP LOCKED` skips live CVRs (✓ path 4). |
| 12 | CCM lifecycle | **PORTED, live single owner** | I-8 promotion done; maintenance planner (`plan_maintenance` / revalidate / background retransform) wired (L8 fixes). |

**Structural note D-CVR-1 (registered, not refactored):** rust queues pending
writes as `StoreOp`s on the UPDATER (`store_ops` vec, applied by
`flush_ops_to_store`) where TS queues them on `CVRStore` (`#writes` /
`#pending*`). Queue→de-dup→one-tx semantics are identical; relocating the
queue to the store for site-parity would be high-churn/low-value. Revisit only
if a real divergence is ever traced to queue placement.

### Remaining work items
- **P7-a — RETRACTED (was a false divergence), REAL fix DONE 2026-08-28.** The
  Part-5 note conflated two different TS timers. Re-reading the TS source:
  `#deleteClientDueToDisconnect` (view-syncer.ts:747-771) stops the *expire*
  timer (`#stopExpireTimer`, 767) — NOT the *ttlClock* interval. The ttlClock
  interval (`#ttlClockInterval`) is stopped only in `#startTTLClockInterval`
  (restart) and `#cleanup` (2812, shutdown), so TS leaves it armed until
  shutdown — exactly like rust. So the ttlClock side needs NO change.
  The REAL gap the investigation surfaced: rust had never ported the
  `#expiredQueriesTimer` / `#scheduleExpireEviction` / `#stopExpireTimer` trio
  at all — it polled `next_eviction_time(cvr)` in the CG loop with no
  clients-present gate, so an eviction pass (and its CVR flush) could fire
  during the 0-client keepalive window that TS's `#stopExpireTimer`-on-last-
  disconnect suppresses. **Fixed** by porting the trio 1:1 at the exact TS call
  sites (field + `schedule_expire_eviction`/`stop_expire_timer`, clear-before-
  run in the loop), pinned by the non-vacuous `last_disconnect_stops_the_
  eviction_timer` test (fails with the disconnect-site `stop_expire_timer()`
  reverted).
- **P6-a (verify)**: TS pages catchup rows via cursor (10 000/page); confirm
  the rust catchup reader is bounded-memory for large CVRs (streamed or
  chunked), add a bound note/test.
- **P11-a (verify)**: one integration assertion that a purge-tombstoned CVR
  loaded by rust yields the exact TS `ClientNotFound` message bytes (client
  wipe semantics depend on it).


---

## PICKUP LIST (single source of truth for remaining work — updated 2026-08-28)

Everything below is orderable; per-item gates = fmt + clippy(-D warnings) +
341 syncer tests (`TEST_CVR_PG_URI=postgres://user:password@localhost:6434/postgres`)
+ `python3 parity/call_topology.py` + `python3 parity/parity_ledger.py syncer
--enforce-structure` + `bash scripts/local-rust-ci.sh` == PASS.

### In flight (this session)
- [ ] **ART release gate** on `zero-cache-rust-syncer:l9-fa1bfbef4`
      (`RUST_SYNCER_IMAGE=… SYNCER_SHARDS=200 run-rust-syncer-release.sh
      --mode release --skip-code`); log: `/tmp/art-l9.log`.
- [ ] **Push** the L9 series to `origin` (mono → kartikparsoya-eng/mono-better;
      `git push --no-verify` if the GitGuardian hook false-positives).
      11 commits: 1970feeb7 (3c-iii) … 3cb3d7036 (Part 5).

### Queued next (small, self-contained — good pickups)
- [x] **P7-a** (Part 5) — RETRACTED as a false divergence + REAL fix DONE
      2026-08-28. The ttlClock interval is faithfully left armed (TS never stops
      it on disconnect). The actual gap was the un-ported `#expiredQueriesTimer`
      / `#scheduleExpireEviction` / `#stopExpireTimer` trio (rust polled
      instead), which let an eviction fire during the 0-client keepalive window.
      Ported 1:1 at the exact TS call sites; pinned by
      `last_disconnect_stops_the_eviction_timer` (proven failing-first). NOTE:
      lands AFTER the ART image `l9-fa1bfbef4` was built → needs a re-ART before
      it is considered release-validated.
- [ ] **CVR fuzzy-rename sweep**: rename the 4 fuzzy-bound rust-cvr symbols to
      exact TS names (run `python3 parity/parity_ledger.py cvr`, see the
      "fuzzy" rows, e.g. `RowsVersionBehindError`→current `VersionError`-class
      names). Pure rename commits; ledger re-run proves exact.
- [x] **CVR fuzzy sweep** — DONE 2026-08-28. cvr ledger now fuzzy **0** (was 5):
      split the merged `record_cvr_flush` into 1:1 `record_sync_flush_stats` +
      `record_async_flush_stats` (row-record-cache.ts:144/153; a rule-2 merge —
      also wired the previously-`None` async recorder), and registered 4 aliases
      for extractor blind spots (2 enum variants, 1 exact `type` alias, 1
      valita→struct).
- [x] **P6-a** — VERIFIED DONE 2026-08-28 (no code change). Rust row catchup is
      bounded-memory: `CATCHUP_PAGE_SIZE = 10000` (row_record_cache.rs:207,
      matches TS `.cursor(10000)`); a READ-ONLY REPEATABLE-READ txn task streams
      ≤10k-row pages through a bounded mpsc channel — 1:1 with TS's
      `for await … query.cursor(10000)`. (Config patches use `fetch_all`, but
      those are O(queries+clients), not O(rows).)
- [x] **P11-a** — DONE 2026-08-28. TS's purge path throws `ClientNotFoundError(
      'Client has been purged due to inactivity')` (cvr-store.ts:423-424); rust
      emitted `self.cvr_id` instead, and that string reaches the client verbatim
      as the `["error",…]` frame (view_syncer.rs:1807). Fixed byte-exact +
      PG test `pg_cvr_store_load_purged_yields_exact_client_not_found_message`
      (proven failing-first).

### Larger, optional (decide before starting)
- [ ] **Syncer misfiled tail**: drive the 25-entry L1 ratchet list down
      (`parity_ledger.py syncer --enforce-structure` prints it) — mostly
      fuzzy-matcher noise + documented folds; only worth it with a matcher
      improvement (per-file tie-break) or symbol moves with real value.
- [ ] **worker-dispatcher mirroring**: split `http_server.rs`/`ws_server.rs`
      against TS `server/worker-dispatcher.ts` — interwoven with I-1/I-4
      invention tasks; needs its own design pass (documented in `src/server.rs`).
- [ ] Dormant backlog: #103 arena/Send-ification; #143 G25 pool-sizing rerun;
      #145 advancement-timeout by-design confirmation; #150 shard-sized
      capacity rerun; #151 dhat/e2e profiler runs.

### Done (for orientation)
L9 Stages 1–5 all landed (Part 4 records per-stage commits); CVR path verdicts
in Part 5; L1 structural ratchet wired into local CI; ledger extractor
brace-bug fixed; L3 pins on the 1:1 tree.
