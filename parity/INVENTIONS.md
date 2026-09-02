# Rust-Only Inventions — Contract Registry (L6)

AGENTS.md rule 5 upgraded: a Rust-only construct is legal **only** if it appears
here with (a) why it has no TS twin, (b) its **TS-observable contract** — the
client-visible behavior it MUST preserve — and (c) the test(s) that pin the
contract. The L1 ledger fails if an invention exists in code without a row here.

An invention may change *how* work is scheduled/stored; it may **never** change
*what* a client observes (frame content, frame ordering, latency-independence
guarantees, error semantics) versus TS.

---

## I-1 — CG serial thread + executor model
- **Files (post-L9):** `workers/cg_executor.rs` (`run_executor`, `SpawnCg`, `spawn_local`), `workers/syncer.rs` (dispatch/routing, `CGHandle` map), `services/view_syncer/view_syncer.rs` (`cg_event_loop`), `config/zero_config.rs` + `main.rs` runtime sizing.
- **No TS twin:** TS runs one `ViewSyncerService` per client group with a single
  async `#lock`; JS is single-threaded with an event loop. Rust replaces the
  event loop + lock with one OS thread per CG hash-shard running a serial message
  loop.
- **Contract:** clients must observe the SAME frame ordering and the SAME
  independence guarantees as TS. Specifically: (a) `connected`, `pong`, and
  `error` frames MUST NOT be delayed by another message's `config_and_hydrate`
  (TS returns `downstream` immediately and runs hydration via `void
  #runInLockForClient` — view-syncer.ts:896,916); (b) per-CG hydration IS
  serialized in both (TS one `#lock` == rust one thread) — that serialization is
  faithful; (c) poke frame order per client is preserved.
- **The three decoupled emissions — how each `(a)` frame stays live off the CG thread:**
  - `connected`: emitted on the accept task (`handle_connection`), never the CG
    thread — see I-2. Pinned by `connected_ack_is_decoupled_from_a_blocked_cg_hydrate`
    and enforced by the L3 guard.
  - `pong` **liveness**: guaranteed by the writer-task keepalive
    (`run_ws_writer`, ws_server.rs:474 — sends `["pong",{}]` every
    `DOWNSTREAM_MSG_INTERVAL_MS` if no downstream frame went out), a separate
    tokio task that does NOT touch the CG thread. This is the 1:1 mirror of TS
    `#maybeSendPong` (connection.ts:341, a `setInterval` off the view-syncer
    lock), which TS documents fires exactly when "the inbound stream is backed up
    ... pongs will be manually sent" (connection.ts:58-61). The client-initiated
    `["ping"]→["pong"]` fast-path (connection.rs:163, TS connection.ts:220) runs
    on the CG thread in rust, so under a blocked hydrate the *explicit* ping-reply
    can be delayed — but the writer keepalive is precisely TS's backed-up path, so
    the client still observes a pong within the liveness window. Contract holds
    via the keepalive, not the ping-reply. Pinned by
    `on_inbound_ping_answers_pong` (reply correctness) + the keepalive code path.
  - `error` (connect-time): version-gate + malformed-params errors are emitted on
    the accept path (`accept_connection` / `send_error_and_close`), never the CG
    thread. Pinned by `send_error_and_close_sends_error_frame_then_close_3000` and
    `malformed_base_cookie_closes_with_internal_error`. A shed error (slow-client)
    is emitted from the writer task — pinned by
    `slow_client_shed_closes_with_rehome_error_then_close_3000` (see I-4).
    Message-*processing* errors (a throw inside `handleMessage`) ARE serialized on
    the CG thread — faithful, because TS also runs `handleMessage` before it can
    throw, and the throw only closes the SAME client's connection (per-client, not
    cross-client).
- **Tests:** `router::tests::connected_ack_is_decoupled_from_a_blocked_cg_hydrate`
  (ack independence), `on_inbound_ping_answers_pong` (pong reply),
  `slow_client_shed_closes_with_rehome_error_then_close_3000` (shed error frame),
  `send_error_and_close_sends_error_frame_then_close_3000` (connect-time error
  ordering). Pong keepalive liveness is structural (writer task, ws_server.rs:474)
  — the L7 prose-invariant checklist carries its citation.
- **History:** violated by bug-1 (connect-ack was on the serial path). Fixed
  `5e71e24f4`.

## I-2 — `Connection::init()` effects applied on the accept path
- **Files:** `router::handle_connection` (`connected_message` push),
  `ws_server::accept_connection` (version gate), `Connection::init` (the 1:1
  port, exercised by connection.rs unit tests).
- **No TS twin (rule 5):** TS calls `connection.init()` (version gate + send
  `connected`) on the per-connection accept handler (`syncer.ts#handleConnection`,
  after `new Connection`). Rust builds `Connection` on the serial CG thread
  (its handler binds CG-local dispatch services), so `init()` cannot run on the
  accept task. `init()` is kept as the faithful 1:1 port; its TWO observable
  effects are produced on the accept path: the version gate in
  `accept_connection` (byte-identical `VersionNotSupported` message), and the
  `connected` frame in `handle_connection` via the 1:1 `connected_message()`
  builder. NO invented split function — the earlier `check_version()` was
  removed.
- **Contract:** `connected` is emitted after auth/user-pin validation and before
  any hydration, on a context not serialized behind another client's hydrate —
  byte-identical body to TS (`{wsid, timestamp, appID, shardNum}`); an
  out-of-range version is rejected with TS `init()`'s exact message.
- **Tests:** `connected_ack_is_decoupled_from_a_blocked_cg_hydrate`,
  `cg_state_connection_lifecycle_and_notification` (CG thread must NOT emit it),
  `malformed_base_cookie_closes_with_internal_error` (ordering),
  connection.rs `init_out_of_range_closes_with_exact_version_not_supported_message`
  (version-gate message 1:1).

## I-3 — Push relay (Option-A write path)
- **Files:** `push_relay.rs`, `PushRelayHeaders`, `server/rust-push-relay.ts` (TS loopback).
- **No TS twin:** rust runs zero mutation logic; a custom push is relayed to a TS
  loopback that runs the real `fetchFromAPIServer('push', ctx)`. TS processes it
  in-process.
- **Contract:** the relayed request must be byte-equivalent to what TS's
  in-process `fetchFromAPIServer('push', ctx)` sends **at push time**, including
  the **current** (not connect-time) auth token and the current
  `userPushURL`/`userPushHeaders`. TS reads `mustGetConnectionContext(selector)`
  per push (pusher.ts:107).
- **Contract (must-get + auth-failure invalidation, 2026-08-29):** a push from
  a connection with NO registered context is an ERROR to that client (TS
  `mustGetConnectionContext` throw, `InvalidConnectionRequest`) — the relay
  must NEVER fire an Authorization-less POST (the prod "No token provided"
  401s). A 401/403 relay response must `failConnection(selector, revision)` at
  the enqueue-time revision (TS pusher.ts:539 `isAuthErrorBody` →
  `#connContextManager.failConnection`), so the client's next message
  must-fails and it reconnects with fresh auth instead of retrying a dead
  token (the 2026-08-29 401 storm). The TS loopback hop must be
  status-TRANSPARENT for auth rejections: an upstream 401/403 keeps its status
  on the relay response (rust-push-relay.ts) — collapsing it to 502 renders
  the rust failConnection branch inert (observed in prod 2026-08-29:
  backend 401 → relay 502 → 0 invalidations; pinned by
  rust-push-relay.test.ts).
- **Tests:** `update_auth_refreshes_the_forwarded_push_relay_token`,
  `relay_body_carries_user_push_overrides`,
  `ccm_dispatch_adapter_surfaces_real_connection_auth` (must-get: missing
  context ⇒ `InvalidConnectionRequest`, never a defaulted `auth: None`),
  `auth_failure_relay_response_fires_fail_connection_hook` (401 fires
  `fail_connection` at the captured revision; 500 does not).
- **History:** violated by bug-2 (auth was a connect-time snapshot). Fixed
  `97440d021` (auth → shared `Arc<Mutex>` refreshed in `handle_update_auth`).
  Violated again 2026-08-29 (adapter defaulted a missing context to
  `auth: None` → headerless relays; no failConnection on 401 → dead-token
  retry storm) — both fixed with the tests above.
- **Contract note (non-auth fields):** `cookie`/`origin`/`request_headers`/
  `user_id` are connect-time in BOTH — TS sets them once in the initial context
  and neither `updateAuth` nor `initConnection` mutate them (connection-context-
  manager.ts:242-260, 290-337 mutate only auth + query/push URL + customHeaders).
  So snapshotting them is faithful. `userPushHeaders` (customHeaders) ARE
  refreshable and live in the shared `push_override` cell.

## I-4 — ws_sink writer/reader tasks + slow-client shed
- **Files:** `ws_server.rs` (`run_ws_writer`/`run_ws_reader`), `ws_sink.rs`.
- **No TS twin:** TS `ws.send` + backpressure via the runtime; rust splits the
  socket into tokio tasks with a bounded queue and a HWM kill.
- **Contract:** frame order out equals enqueue order; a shed closes with the SAME
  error TS emits for a connection it can no longer serve (Rehome,
  view-syncer.ts:473 / cvr-store.ts:1373) — an `["error",{kind:"Rehome"}]` frame
  FIRST, then close 3000. No frame reordering vs the sync push path.
- **Tests:** `ws_server` frame-order tests +
  `ws_server::tests::slow_client_shed_closes_with_rehome_error_then_close_3000`
  (shed → Rehome error frame then close 3000; non-vacuous — a bare close fails it).

## I-5 — Drop-based teardown (Engine `destroy`)
- **Files:** `rust-ivm` Engine `Drop` (`engine/mod.rs:1717`), CG idle reap in `workers/syncer.rs`.
- **No TS twin:** breaks Rc cycles / releases SQLite conns deterministically
  (TS relies on GC). Fires on idle-keepalive reap.
- **Contract:** teardown is observationally a no-op to a still-connected client;
  reap timing matches TS `DEFAULT_KEEPALIVE_MS` (5000). A reconnect after reap
  produces the SAME cold-rehydrate result TS produces for a fresh ViewSyncer.
- **Tests:** teardown_gate_test, `rust-g6-leak-hunt` census. **NOTE:** the reap
  makes a cold rebuild expensive — this is faithful (TS also rebuilds), but it
  is the amplifier that made bug-1 catastrophic; keep I-1's ack contract intact.

## I-6 — CVR write-behind / offload runtime
- **Files:** `services/view_syncer/view_syncer.rs` (`offload`, `flush_to_store`/`flush_ops_to_store`), `rust-cvr` `row_record_cache.rs` write-behind.
- **No TS twin:** rust offloads CVR I/O to the pool runtime; TS awaits inline.
- **Contract:** a poke is not sent to a client before the CVR state it reflects
  is durable to the SAME degree TS guarantees (no client observes a version the
  CVR hasn't recorded). Flush ordering per CG preserved.
- **Enforcement point (located):** the version a client is poked TO must equal
  the version the store actually PERSISTED. `flush_ops_to_store` returns whether
  the store *materially* flushed (`flush_ops_to_store` → `store_flushed`,
  view_syncer.rs:6787); every caller that pokes gates the poked cookie on it:
  `cfg_cvr = if store_flushed { bumped } else { cfg.base.orig.clone() }` then
  `pokers.end(cfg_cvr.version)` (view_syncer.rs). This is the 1:1 port of
  TS `CVRUpdater.flush`'s `if (!flushed) return {cvr: this._orig}` (cvr.ts) —
  cited at view_syncer.rs:6754. Adopting the bumped CVR on a no-op flush would
  advance client cookies past the stored version (the exact "poke to a
  non-durable version" divergence) AND fail the next material flush's version
  guard (`ConcurrentModification`).
- **Tests (DONE):** BOTH halves of the durability-ordering oracle now pinned,
  PG-gated (`TEST_CVR_PG_URI`), non-vacuous:
  - STORE side — `pg_quiet_commit_noop_flush_contract`: a no-op flush returns
    `None`, does NOT advance the stored version, and the next material flush passes
    `orig.version` and succeeds; the counter-factual (adopting the bumped version)
    dies on `ConcurrentModification`.
  - CLIENT side — `pg_noop_flush_does_not_poke_client_past_stored_version`
    (2026-08-27): drive a quiet ADVANCE (the "02" commit touches only a different
    client group's `clients` row, so cg1's lmids query sees 0 changes → no-op
    flush), then assert every `pokeEnd` cookie ≤ the PG stored version and none
    reaches the never-persisted "02". Proven to FAIL when the advance-path no-op
    fallback (the advance-path no-op branch in view_syncer.rs) is reverted — the poke then carries the
    bumped "02" cookie: "client poked to non-durable version 02; store is at 01".

## I-7 — Cost-model / flip-planner COUNT(*) caching
- **Files:** `rust-ivm` planner cache, `engine::plan_ast`.
- **No TS twin:** batch-shared `PlanCountCache` per hydration.
- **Contract:** the plan chosen is identical to TS `planQuery(ast, costModel)` for
  the same replica state; the cache only avoids recomputation, never changes the
  plan.
- **Tests:** `g8_mychannelparticipations_real_ast`, diff-oracle full-catalog.

## I-8 — Promote the ported ConnectionContextManager to single live owner
- **Files (post-L9):** `services/view_syncer/connection_context_manager.rs` (the
  ported CCM), now owned by `ViewSyncerService.ccm: Arc<Mutex<ConnectionContextManager>>`
  (`services/view_syncer/view_syncer.rs:432`); connection routing in
  `workers/syncer.rs`; dispatch adapter (`CcmDispatchAdapter`) in `server/syncer.rs`.
- **Status (2026-08-27): LARGELY DONE.** The ported CCM is now the single live
  owner of per-connection auth + custom-query context. DELETED the parallel
  `CgState` maps `client_auth`, `client_raw_auth`, and `client_query_ctx` (plus
  `default_query_context`/`filtered_query_headers`/the dead `query_config` field).
  All consumers read the CCM at use time:
  - authData → `decode(must_get_connection_context(sel).auth.raw())` at hydrate.
  - custom-query context → `custom_query_context_from(must_get_connection_context(
    sel))` (rust-only adapter; TS `transform-query.ts` reads `ctx.queryContext`
    inline) at the 3 config_and_hydrate/revalidation sites.
  - auth-maintenance / revalidation / updateAuth-unchanged → the CCM.
  - Register now precedes arm (TS order); a failing test pinned the ordering.
- **Fixed en route:** the `initConnection` `customHeaders` allowlist filter (TS
  :306/:324) and the opaque-token updateAuth sub-pin (only JWTs carry a `sub`).
- **Push-relay + dispatch consolidation — DONE (2026-08-27):** the message
  handler's `ConnContextManagerDispatch` is now backed by the ported CCM via the
  `CcmDispatchAdapter` (`server/syncer.rs`) instead of the former
  `PlaceholderConnContextManager` (which returned `auth:None`; now deleted). So
  BOTH the handler's live reads consolidate onto
  the single owner: (a) the mutagen-CRUD auth (`syncer_ws_message_handler.rs`) now
  sees real auth (the placeholder divergence is gone), and (b) the relayed-push
  auth is read FRESH per relay from `must_get_connection_context(sel).auth`
  (handler `relay_headers_for`; router deleteClients cleanup) — TS pusher.ts:107.
  The parallel `PushRelayHeaders.auth` `Arc<Mutex<Option<String>>>` CELL is DELETED
  (now a plain `Option<String>` filled per relay), and the `handle_update_auth`
  cell-refresh is removed. The raw-header forwarding semantics are preserved
  (`request_headers` still RAW; only `auth` moved to the CCM). Tests:
  `ccm_dispatch_adapter_surfaces_real_connection_auth` (non-vacuous: fails when the
  adapter returns the placeholder `None`) + the repurposed
  `update_auth_refreshes_the_forwarded_push_relay_token`.
- **Tests:** `configured_query_context_matches_typescript_defaults_and_header_filtering`,
  `forwards_allowlisted_incoming_request_headers` (Step-2 golden, non-vacuous),
  `connection_context_manager_tracks_register_update_and_close`,
  `authdata_reads_from_connection_context_manager`,
  `auth_maintenance_reads_token_from_the_connection_context_manager`,
  `a_close_fully_tears_down_all_per_client_state`, the 7 `update_auth_*` tests.

## I-9 — L8-surfaced coverage of invented layers (2026-08-27)

The Layer-8 traffic-driven path differential (parity/L8-PATH-DIFF.md,
parity/L8-TRIAGE.md) confirmed which TS-hot symbols are intentionally cold in
rust because an invention replaces their layer. Contracts already registered
above; this entry records the explicit coverage so L8 cold rows bind to it:

- **I-6 (CVR write-behind)** covers `row-record-cache.ts` `clear` /
  `executeRowUpdates` — the flush actor persists byte-identical CVR state
  (flush PG differential) without the TS write-path helpers.
- **Pokers/ws_sink model (I-1/I-4)** covers `client-handler.ts`
  `close`/`cancel`/`fail` — lifecycle owned by the poker + writer-task model;
  error/close semantics pinned by G36 + shed/Rehome tests.
- **SQLite-backed engine (architecture)** covers the TS client-engine
  machinery zero-cache runs server-side (`memory-source` gen_push/overlays,
  `array-view` flush, `stopable-iterator`, per-query metrics delegate):
  value parity is pinned by the G8 diff-oracle + ART, not per-function twins.
- **Staggered SIGTERM drain + idle reaper** covers `drain-coordinator.ts`'s
  elective drain consult (`shouldDrain`/`drainNextIn` after hydrate): rust
  drains on signal with staggered deadlines and reaps idle CGs; the TS
  keepalive-driven elective drain is not wired. Client-observable contract
  (no mid-work connection loss without Rehome semantics) unchanged.

Formerly-open gap, now CLOSED (task #157, 2026-09-01 verified): the ivm
filter-pipeline operator protocol (`begin_filter`/`end_filter`/
`build_filter_pipeline`/`set_filter_output` + builder DNF simplification) is
ported and wired at the builder + operator call sites — `builder/builder.rs`
and `ivm/{filter,filter_operators,exists,fan_in,fan_out}.rs`.

## I-10 — Inspector server metrics on the per-CG (not per-worker) delegate; `query-update-server` seam
- **Files:** `server/inspector_delegate.rs` (the ported `InspectorDelegate`
  metrics + AST store), `tdigest.rs`, `services/view_syncer/view_syncer.rs`
  (recording sites + the per-CG `inspector_delegate` field),
  `services/view_syncer/inspect_handler.rs` (`metrics`/`queries` ops).
- **No TS twin (scope):** TS constructs ONE `InspectorDelegate` per Syncer
  worker (server/syncer.ts:207) shared across every `ViewSyncerService`, so
  `getMetricsJSON()` is a worker-global aggregate. Rust runs each client group
  on its own `!Send` CG thread (I-1/I-2) with no shared mutable worker object,
  so the delegate is per-CG.
- **Contract (metrics/queries ops):** the `queries` op's per-query rows are
  observationally identical to TS — they are keyed by the caller's own queryIDs,
  which live in this CG. The `metrics` op returns THIS client group's aggregate
  rather than a cross-CG one; since the inspector is a diagnostic scoped to the
  connecting client, the per-CG aggregate is a strict subset (the caller's own
  queries), never wrong data for another group.
- **`query-update-server` seam (NOT wired — documented gap):** TS wraps every
  per-query source connection in a `MeasurePushOperator` (pipeline-driver.ts:650)
  that times each incremental push and reports `query-update-server`. Rust's
  advance is a shared-source fan-out: one `source.push(change)` propagates to ALL
  subscribed pipelines at once (`engine::advance_streaming`, the batched-advance
  invention), so there is no per-query per-push seam to attribute update timing
  to without wrapping each pipeline's source connection and distorting the hot
  path. The `query-update-server` digest is therefore always empty (`[1000]`),
  and `query-materialization-server` (fed from the engine's existing per-query
  `hydration_time_ms`, no new timer) is fully populated. Client-observable
  effect: a query that has received incremental updates shows a populated
  materialization/hydration metric but an empty update digest. Wiring it 1:1
  would require the arena/Send-ification rewrite (#103) that gives each pipeline
  an isolable source input.
- **Tests:** `tdigest::tests::ts_golden_matches_real_tdigest` (byte-exact vs the
  real TS TDigest), the `server::inspector_delegate::tests::*` unit suite
  (metrics wire shapes + `metrics_for_protocol`),
  `inspect_metrics_returns_delegate_global_aggregates` (op wiring, non-vacuous),
  `hydrate_and_sync_records_inspector_materialization_and_ast` (recording,
  non-vacuous).

## I-11 — Per-row mid-fetch advancement gate (thread-local bridge for TS's `ResetPipelinesSignal`)
- **Files:** `rust-ivm/src/advance_gate.rs` (the `AdvanceGate` struct + economic
  budget math + the thread-local gate `arm`/`GateGuard`/`should_stop_fetch`);
  called from `engine/mod.rs` `advance_to_head_stream` (arms the gate; per-change
  `advance_reset()` check) and `sqlite/table_source.rs` `LazyRowsIter::next`
  (per-row `should_stop_fetch()` poll, every 64th row).
- **No TS twin (mechanism, not logic):** the ECONOMIC-BUDGET LOGIC is a 1:1 port
  of `pipeline-driver.ts` `#shouldAdvanceYieldMaybeAbortAdvance` / `AdvanceContext`
  (#6206) — every fn cites its TS name (`projectedAdvancementTimeMs`,
  `shouldResetProjectedAdvancement`, `shouldResetSlowCurrentChange`,
  `shouldFinishLateAdvancement`), and the per-change check lives in `engine/mod.rs`
  (the `pipeline-driver.ts` twin), exactly like TS. What has NO TS twin is the
  DELIVERY MECHANISM: TS re-checks the budget mid-fetch and `throw`s a
  `ResetPipelinesSignal` from deep inside the `TableSource` fetch, unwinding to the
  PipelineDriver. Rust IVM push is INFALLIBLE (operators return `Vec<Change>`, not
  `Result`), so a leaf fetch cannot throw across it. Instead a **thread-local gate**
  (advance is single-threaded on the actor thread) is armed by the engine for the
  advance's duration; the leaf fetch polls `should_stop_fetch()` between rows and,
  when the budget blows, returns `None` (a normal short-input end-of-stream) and
  latches `tripped`; the engine checks `tripped_reset()` after the push and resets.
  A RAII `GateGuard` disarms on scope exit or panic so a later hydrate on the thread
  can never inherit a stale budget. Housed in its own dependency-free leaf module so
  BOTH `engine/` (root) and `sqlite/table_source.rs` (leaf) can depend on it without
  a `sqlite → engine` layering inversion.
- **Contract (TS-observable):** a mid-fetch budget abort is client-indistinguishable
  from TS's `ResetPipelinesSignal` — same reset reasons (slow-current-change /
  projected / timeout / wall-clock ceiling), same thresholds
  (`MIN_ADVANCEMENT_TIME_LIMIT_MS` = 50, the #6206 projection tunables — the SAME
  const the per-change arm imports, so per-row and per-change can never trip on
  different thresholds), and a trip discards the truncated push and rehydrates
  (`advance_reset_error` → the same `advancement-timeout` reset the per-change arm
  emits). The gate is inert outside a production advance: hydrate and worker-thread
  fetches see `None` and read every row (`should_stop_fetch()` == false when
  unarmed). Rust-only additions beyond TS: the `WallClockCeiling` arm (an absolute
  exclusion-free bound — TS's budget keeps ticking through delivery, Rust excludes
  delivery time via `exclude`, so a slow consumer could otherwise hold the WAL
  snapshot open indefinitely) is a divergence-guard, not a behavior change on the
  hot path.
- **Tests:** `advance_gate::tests::*` (the budget-math arms:
  `slow_current_change_trips_regardless_of_late_finish`, `projected_batch_cost_trips`,
  `late_advancement_finishes_instead_of_resetting`, `delivery_wait_is_excluded_from_budget`,
  `should_stop_fetch_is_false_when_unarmed`) + the fetch-integration suite
  `sqlite::table_source` `advance_gate_fetch_tests`
  (`fetch_returns_all_rows_when_no_gate_armed`, `fetch_stops_when_gate_over_budget`
  — non-vacuous: the fetch actually short-circuits mid-stream —,
  `fetch_resumes_all_rows_after_guard_drops` — the RAII disarm proven).
