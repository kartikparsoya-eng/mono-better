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

- **Failure semantics (2026-09-03):** a relay failure (`PushFailedHttp` non-2xx,
  `PushFailedZeroCache` network error) FAILS the client's downstream exactly like
  TS `#failDownstream` (pusher.ts:612 → `downstream.fail` → `closeWithError`,
  types/streams.ts:88): error frame, then close. Before this, rust only sent the
  frame and left the socket open, so the client's next push died with
  `InvalidConnectionRequest` (frame-capture #3). The only rust addition is the
  `ws_id` guard in `ConnectionSinks::fail_if_current` (a superseded socket's
  failure never closes the replacement). Pinned by pusher.rs
  `drainer_surfaces_push_failed_http_on_non_2xx` /
  `drainer_surfaces_push_failed_zerocache_on_network_error` (assert
  `WsCommand::Fail`, not `Send`) and view_syncer.rs
  `connection_sinks_deliver_only_to_current_socket`.

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
- **No TS twin — what is STILL invented here (2026-09-02, after the write-back
  fix below):**
  1. `offload`: the whole PG-touching section (apply ops → store flush → cache
     apply) runs on the shared multi-thread runtime via `handle.spawn(fut).await`
     because the CG executor's single-threaded reactor does not drive the shared
     CVR pool's connections. TS awaits inline in the `#lock`. The CG task still
     AWAITS it, so this is a thread move, not a concurrency change: per-CG flush
     ordering is unchanged.
  2. Bounded flush retry (`MAX_FLUSH_ATTEMPTS = 3`, jittered backoff). TS has no
     retry — postgres.js queues, so CVR saturation degrades to latency rather
     than a `fail_group` → rehydrate storm. The retry approximates that with a
     BOUND, so a genuinely dead CVR still fails the group.
  2b. **Failure scope (2026-09-03).** When a client-initiated lock op fails
     (initConnection / changeDesiredQueries / deleteClients → rust
     `config_and_hydrate` / `delete_clients`), TS fails ONLY that client
     (`#runInLockForClient` catch → `failConnection` + `client.fail(e)`,
     view-syncer.ts:1237-1250) and keeps serving the group. Rust tears the
     whole group down (`fail_group`) because the write-behind may already have
     served a version the store never recorded; continuing would let the next
     notification skip that batch. Contract: the ERROR every client receives is
     the TS one — `wrapWithProtocolError` → `{kind: Internal, message:
     <underlying error>, origin: zeroCache}` (never Rehome; Rehome stays
     reserved for I-4 shed and TS's own Rehome sites). Pinned by
     `store_failure_fails_clients_with_internal_like_ts_wrap_with_protocol_error`.
     Failures inside the advance loop / timer ops fail every client in TS too
     (`#cleanup(err)`), so those sites are 1:1.
  3. ~~The store does not OWN the `RowRecordCache`~~ — **CLOSED 2026-09-02.**
     `CVRStoreHandle` now holds `row_cache` like TS's `CVRStore.#rowCache`
     (cvr-store.ts:246), builds it in `new()` with TS's default arguments, and
     is the only thing that touches it: `get_row_records()` for the flush's
     no-op prune (TS :1067), `execute_row_updates()` for the write-or-defer
     decision (TS :1166), `apply()` after commit (TS :1218), plus the
     `flushed()` and `catchup_row_patches()` delegates (TS :709). `flush()` is
     back to TS's parameter list, the syncer's `row_cache` field is gone, and
     `execute_row_updates_forced` — which only existed for cache-less callers —
     is deleted.
  4. Row DELETEs/upserts are batched into ONE `json_to_recordset` statement each
     instead of TS's per-row statements. TS pipelines its through postgres.js, so
     the round-trip count matches; sqlx cannot pipeline, and sequential awaits
     were the flush-convoy driver behind the capacity cliff.
- **2026-09-02 — the "single atomic PG writer" part of this invention is GONE,
  and so is the cache-ownership split (3 above). What remains is `offload`,
  the bounded flush retry, and statement batching.**
  Until this commit the store wrote instance + `rows` + `rowsVersion` in ONE
  transaction on the serving path and called `RowRecordCache::apply(..., true)`,
  so the ported `execute_row_updates` / `FlushMode::AllowDefer` / background
  `flush_loop` were dead code. TS defers any batch over
  `deferredRowFlushThreshold` (100) — `executeRowUpdates` returns `[]`, the
  transaction commits only CVR metadata, `pokeEnd` fires, and `apply(...,
  flushed=false)` flushes the rows in the background (row-record-cache.ts:46-60,
  418-427, 234-260). Rust now asks the cache at the same call site
  (cvr-store.ts:1166) and honours `Defer`, so a large hydrate's ~1900 row
  upserts no longer sit in front of the client's poke. Measured shape of the
  divergence: `getUsersV2` (1,923 rows) server hydration 16.8ms vs TS 17.2ms,
  but client-observed 271ms vs 53ms.
- **Durability boundary (now identical to TS):** the poke is gated on the
  *instance* version being committed, NOT on `cvr.rows`. `rowsVersion` may lag
  `instances.version` while rows are pending; `CVRStore::load` reconciles by
  retrying `MAX_LOAD_ATTEMPTS` (10) × `LOAD_ATTEMPT_INTERVAL_MS` (500) on
  `RowsVersionBehind` — the 1:1 port of TS's load loop.
- **Contract:** a poke is not sent to a client before the CVR state it reflects
  is durable to the SAME degree TS guarantees (no client observes a version the
  CVR hasn't recorded). Flush ordering per CG preserved.
- **Write-back scheduling (2026-09-03, D2):** TS schedules the row write-back
  with `this.#setTimeout(() => this.#flush(), 0)` on the client group's OWN
  event loop (row-record-cache.ts:258); rust `tokio::spawn`s `flush_loop` on
  the shared runtime (`row_record_cache.rs`), where it runs genuinely
  concurrently with the group's foreground work. That is a scheduling move
  (sanctioned: it is the thread approach), so its TS-observable contract is
  pinned explicitly — everything TS gets for free from single-threading:
  1. The pending-row snapshot is ATOMIC with respect to `apply()`: the batch a
     loop iteration writes is exactly what was pending at snapshot time, and
     rows applied while that transaction is in flight are written by the next
     iteration, never lost (TS `#flush`'s synchronous `runTx` block,
     row-record-cache.ts:270-284 — snapshot, `executeRowUpdates`, `#pending.
     clear()` with nothing able to interleave). Rust: `std::mem::take` under
     the state lock. **Pinned by** `write_back_keeps_rows_applied_while_a_
     flush_transaction_is_in_flight` (PG; proven to fail on the old
     clone-then-clear loop: `["a","b"]` vs `["a","b","c","d"]`).
  2. `apply()` never waits on an in-flight write-back transaction — the state
     lock is held only for the snapshot / the version bookkeeping, never across
     PG I/O (TS: the tx awaits are async; `apply()` runs between them).
  3. `flushed()` resolves only when `pending_rows_version == flushed_rows_
     version`, and is awaited before a catchup read (row-record-cache.ts:361 ↔
     `catchup_row_patches`) and before idle shutdown (view-syncer.ts:736).
  4. `'allow-defer'` defers while a write-back is in flight (`is_flushing`),
     so the foreground flush commits only CVR metadata and `pokeEnd` is not
     behind the row commit (row-record-cache.ts:418-427). Pinned by
     `execute_row_updates_defers_over_threshold_and_while_flushing`.
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

## I-12 — IVM time slicing on the shard runtime (`yield_process` ↔ `setImmediate`)
- **Files:** `services/view_syncer/view_syncer.rs` (`TimeSliceTimer`,
  `yield_process`, `TIME_SLICE_QUEUE`, the `StreamItem::Yield` arms in
  `hydrate_and_sync` / `hydrate_unchanged_queries`), `services/view_syncer/
  pipeline_driver.rs` (`Timer`, `HydrateContext`/`AdvanceContext`,
  `should_yield`, `HydrateChanges`, `AdvanceChanges`), `server/priority_op.rs`,
  `rust-ivm` `engine::HydrateStream` / `engine::AdvanceStream` (+
  `snapshotter::diff::DiffIter`, `advance_gate::arm` per pull) +
  `sqlite/table_source.rs` `generate_with_yields`.
- **What is ported 1:1 (not invented):** every name, threshold and check site —
  `yieldThresholdMs` (zero-config.ts:534, default 10), the two derived
  thresholds and the priority-op selector (server/syncer.ts:209-213/230-233),
  `#shouldYield` (pipeline-driver.ts:1080), `generateWithYields` per row
  between overlay and start (zqlite table-source.ts:314-337/692), the yield
  before the first slice (view-syncer.ts:2259), the `'yield'` arm of
  `#processChanges` (:2510) and `#hydrateUnchangedQueries` (:1629), `#advance`
  as a generator with the between-change yield arm of
  `#shouldAdvanceYieldMaybeAbortAdvance` (pipeline-driver.ts:948-1000,
  :975-977, :1156) consumed by `#advancePipelines` (view-syncer.ts:2596), and
  `TimeSliceTimer` (:2943-3010) as the process-time clock per-query hydration
  time is recorded in (pipeline-driver.ts:703).
- **What has no TS twin (the invention):**
  1. `new Promise(setImmediate)` → `tokio::task::yield_now()`. tokio 1.53
     defers the yielding task until after the runtime polls its I/O driver
     (tokio `task/yield_now.rs`), which is the `setImmediate` semantics TS
     relies on (view-syncer.ts:2845-2857) on a `current_thread` runtime.
  2. TS's module-global `timeSliceQueue` / `runningPriorityOpCounter` are per
     sync-worker PROCESS (one event loop); rust's are `thread_local!` per shard
     (one `current_thread` runtime = one event loop). The scope is the same
     object on both sides: "the client groups sharing this event loop".
  3. TS brackets each query with `timer.startWithoutYielding()` / `timer.stop()`
     inside `generateRowChanges`; rust's batched hydrate runs ONE lap across the
     batch and the engine takes per-query deltas of the same clock
     (`HydrateClock`), yielding the same per-query numbers.
  4. The advance economic budget's per-fetch arm (I-11 `advance_gate`) is a
     thread-local; TS keeps it in `#advanceContext` on the driver. Because a
     suspended `AdvanceStream` shares its shard thread with other client
     groups, the gate is armed only for the duration of each `next()` and
     DISARMED at every `Yield` (`advance_gate::arm` guard per pull); consumer
     time between pulls is excluded from the budget, as the callback path's
     synchronous row delivery was.
- **Contract:** (a) a hydrate whose lap exceeds the threshold hands the shard's
  event loop to the other ready tasks (other client groups' frames /
  notifications, timers) before its next slice — a co-located client group is
  never frozen for the length of a neighbour's hydrate; (b) yields are control
  flow only: the row set, row order and poke content are identical at any
  threshold; (c) `hydration_time_ms` (hence `total_hydration_time_ms`, the
  advance economic budget) excludes yielded time; (d) an advance suspended at a
  yield leaves its shard thread with NO armed advance gate, so a neighbouring
  client group's hydrate on the same thread can never be truncated by this
  advance's budget, and a hydrate started after the advance finishes sees no
  advance context (TS `#advanceContext = null` in `finally`, :1049).
- **Tests:** `tests/time_slice_yield_test.rs` —
  `hydrate_surfaces_a_yield_per_row_when_the_slice_threshold_is_exceeded` (b +
  the sentinel round-trip: 0 yields on the pre-port shape),
  `a_fresh_lap_under_the_threshold_does_not_yield` (comparison direction),
  `yield_process_lets_a_co_scheduled_task_run_before_the_slice_owner_finishes`
  (a; fails with a no-op yield), `time_slice_timer_excludes_time_spent_yielded`
  (c; fails with a wall-clock timer),
  `advance_surfaces_a_yield_per_change_when_the_slice_threshold_is_exceeded`
  (a+b for advance; 0 yields when the driver passes no hook),
  `a_hydrate_after_a_finished_advance_uses_its_own_slice_context` (d; panics
  "Cannot hydrate while advance is in progress" if the context is not
  cleared); `server::priority_op::tests::
  priority_op_is_running_only_while_the_op_is_in_flight`; rust-ivm
  `tests/advance_yield_test.rs` — one `Yield` before every change and the hook
  asked exactly once per change, `None` → 0 yields, identical rows with and
  without yields, and `the_advance_gate_is_disarmed_while_the_stream_is_
  suspended_at_a_yield` (d; fails if the gate guard outlives `next()`).
- **Fetch-path forwarding (D1 phase 3):** the sentinel is forwarded through
  every operator's FETCH exactly as TS: `FlippedJoin.fetch` /
  `#fetchBatched` (flipped-join.ts:180/289 — lazy `FlippedJoinFetch`),
  `mergeSortedStreams` (memory-source.ts:1117-1136 — `KWayMerge` primes and
  refills through a yield-forwarding `pending_pull`), the filter chain's
  `filter(node): Generator<'yield', boolean>` (filter-operators.ts:37 —
  `FilterResult`, with `FilterStartStream` holding the in-flight generator,
  `FanOutFilter`, `filter_and`, and `Exists::fetch_size_stream`, exists.ts:
  246-262), the join-layer overlays (join-utils.ts:28/149), and the
  `Streamer` (pipeline-driver.ts:1285-1385 — `StreamerStream`, an explicit-
  stack walk of `#streamChanges`/`#streamNodes` forwarding child-relationship
  yields). ART run 20260903-012323 showed `yields=0` on every hydrate of the
  flipped-EXISTS / EXISTS / related shapes (a 33 s one froze its shard);
  `rust-ivm/tests/hydrate_yield_prod_shapes_test.rs` pins each shape against
  a yielding `TableSource`, `tests/flipped_join_chunked_test.rs` carries the
  TS chunked-yield test + the merge order, all proven on the pre-port sources.
- **Known gap (intra-push yields):** TS's `TableSource.push` → `genPush`
  also yields INSIDE a single change's fetches (the `'yield'` sentinels the
  sources produce during a push). Rust's operator `push` is eager and drains
  those sentinels (`skip_yields` / `resolve_filter` / `Streamer::stream_rows`),
  so one change always runs to completion; the ported abort arms (I-11
  `advance_gate`, checked per row) bound a pathological change. The
  between-change yield (D1 phase 2) is ported.

## I-13 — mimalloc as the process-wide Rust allocator (`GLOBAL_ALLOCATOR`)
- **Files:** `rust-syncer/src/alloc.rs` (`GLOBAL_ALLOCATOR`,
  `route_sqlite_malloc_through_mimalloc` = SQLite `sqlite3_mem_methods` over
  mimalloc via `SQLITE_CONFIG_MALLOC`), `rust-syncer/src/main.rs` (the hook is
  the first statement of `main`; `malloc-trim` thread: `mi_collect(true)` +
  glibc `malloc_trim`), `rust-syncer/Cargo.toml` (`mimalloc`,
  `libmimalloc-sys` with `extended`).
- **What is ported 1:1 (not invented):** nothing — allocation is not part of
  the TS spec; no TS symbol is involved.
- **What has no TS twin (the invention):** the whole item. One TS syncer
  worker is one PROCESS (one V8 heap, one `mm`), so its allocations never
  contend with another client group's. rust runs ~1000 client-group threads
  in ONE process: glibc malloc serves the large allocations of a hydrate (row
  buffers, change vectors) with `mmap`/`munmap`, and every page fault and
  every `munmap` takes the per-process mmap lock (plus TLB shootdowns to all
  the process's CPUs). Concurrent hydrates therefore serialize on the kernel,
  not on IVM work. Measured on the xyne ART 5m trace, image 7f38dffd6, 1024
  shards (2026-09-03): the same 20K-row query hydrated in 1.2-1.4s when it ran
  alone (TS: 1.25-1.57s, parity) and in 9-18s (5-10s of it on-CPU) when five
  ran at once; the process spent 28% of its CPU in the kernel (TS workers:
  ~9%), the thread of the 640K-row hydrate 33s system vs 11s user with 675K
  minor faults. `perf` (same image, rust-only storm replay) attributed 48% of
  the process's samples to glibc malloc/free and its arena lock
  (`__lll_lock_wait_private`, `pthread_mutex_lock`), 31% to the kernel futex /
  wakeup paths those locks take, 21% to actual work — so the mechanism is the
  glibc arena locks (plus their futex traffic), not IVM cost. mimalloc keeps
  per-thread heaps with a lock-free fast path. SQLite is C and allocates
  through its own malloc (glibc; compiled `SQLITE_DEFAULT_MEMSTATUS=0`, so no
  SQLite-side global mutex) and showed up in the same profile, so it is
  pointed at mimalloc too through `SQLITE_CONFIG_MALLOC`, which SQLite only
  accepts before its first `sqlite3_initialize` — hence the hook is the first
  statement of `main`.
- **Contract:** (a) nothing a client observes changes — frames, row sets,
  ordering and error semantics are allocation-independent; (b) memory freed by
  query-TTL expiry / CG teardown is still returned to the OS on the `malloc-trim`
  cadence (the G6 leak gate keeps its meaning): `mi_collect(true)` for Rust
  allocations, `malloc_trim` for SQLite's; (c) the profiling `dhat-heap` build
  installs dhat's allocator instead (and leaves SQLite on glibc) and never both.
- **Tests:** `rust-syncer/tests/global_allocator_test.rs` —
  `rust_allocations_come_from_mimalloc_heaps_for_small_and_large_sizes` and
  `allocations_made_on_other_threads_also_come_from_mimalloc` (fail with the
  `#[global_allocator]` line removed: `mi_is_in_heap_region` is then false for
  every Rust pointer), `sqlite_allocations_come_from_mimalloc_after_the_config_hook`
  (fails with the hook a no-op: `sqlite3_malloc` then returns system memory).
  (b) is measured by ART G6, not unit-tested.
- **A/B (same box, same 5m trace, 1024 shards, glibc image 7f38dffd6 → this):**
  steady p50/p95/p99 54.1/523/3540 → 21.4/245/693 ms (TS in the same windows:
  42.3/293/1635 and 41.5/261/1465); initial p50/p95 218/3801 → 101/616 (TS
  153/527 and 153/465); the 23K-row query 1.2 s alone / 9-18 s concurrent →
  0.61-0.95 s always with cpu ≈ wall; the 640K-row hydrate 37.2 s → 5.9 s (TS
  11.9 s); per-pass `changeDesiredQueries` handle p50 20.6 → 10.4 ms, queue
  wait p50 88.7 → 29.5 ms; process CPU for the replay 155 s user + 61 s sys →
  53 s + 11 s. Pokes/dedup_puts unchanged (3275/9664).
- **Known gap:** initial-connect p95 is still 1.33× TS (616 vs 465 ms) — the
  per-pass PG/HTTP round-trip gap (I-12/I-13 do not touch it).

## I-14 — opt-in server-side liveness close of idle clients (`ZERO_WS_LIVENESS_TIMEOUT_MS`)
- **Files:** `rust-syncer/src/ws_server.rs` (`DEFAULT_LIVENESS_TIMEOUT_MS`,
  `liveness_timeout_ms`, the `keepalive_interval` arm that sends close 1001
  "liveness timeout"), `rust-syncer/OPERATIONS.md` (env table).
- **What is ported 1:1 (not invented):** the 6s downstream `pong` keepalive
  (`DOWNSTREAM_MSG_INTERVAL_MS`, workers/connection.ts:57-67) — the only
  liveness mechanism TS runs on a client socket.
- **What has no TS twin (the invention):** closing a client that has sent no
  frame for a configured time. TS applies its heartbeat-terminate
  (`sendPingsForLiveness`, types/ws.ts:26) only to internal streams
  (types/streams.ts:155/264); `connection.ts` / `syncer.ts` never close an
  idle client. Before 2026-09-03 rust shipped this ON at 60s and unregistered:
  the xyne ART 5m replay (clients without app-level pings) lost 50/344
  sessions to code 1001 and delivered 3275 pokes vs TS 4854, 1826 puts vs
  2172 — a client-observable divergence hidden behind `errors=0` because the
  harness only counts error frames.
- **Contract:** (a) with the default configuration a client that sends
  nothing is never closed by the server — exactly TS; (b) the close exists
  only when an operator sets `ZERO_WS_LIVENESS_TIMEOUT_MS>0`, and then a
  client sending any frame within the window is never closed; (c) the
  `pong` keepalive cadence is unaffected either way.
- **Tests:** `ws_server.rs`
  `liveness_close_is_disabled_by_default_and_opt_in_via_env` (fails with the
  old 60_000 default). (a) end-to-end is measured by the ART head-to-head:
  rust pokes/puts must equal TS's for the same trace.
- **Known gap:** none.

## I-15 — eager first pipeline sync inside the config pass (vs TS's state-loop first sync)
- **Files:** `rust-syncer/src/services/view_syncer/view_syncer.rs`
  (`config_and_hydrate_with_profile`: `handle_config_update` →
  `sync_query_pipeline_set` unconditionally; `on_notification` →
  `advance_and_sync` only advances).
- **What is ported 1:1 (not invented):** `#handleConfigUpdate` →
  `#syncQueryPipelineSet` once pipelines are synced (view-syncer.ts:1163),
  `#advancePipelines` on `version-ready` (:569), the config poke / sync poke
  contents and order within a pass.
- **What has no TS twin (the invention):** WHEN the very first sync of a
  client group runs. TS's `#handleConfigUpdate` skips the sync while
  `#pipelinesSynced` is false; the first init + `#hydrateUnchangedQueries` +
  `#syncQueryPipelineSet('missing')` happens in the `#stateChanges` loop
  (view-syncer.ts:538-606) on the replica state the subscription replays at
  start, which RACES the client's next frames through the same lock. rust
  runs that first sync synchronously at the end of the initConnection pass.
  Client-visible consequence (2026-09-03 frame capture, xyne ART harness whose
  initConnection carries an EMPTY desired set): rust sends one extra empty
  poke (pokeStart/pokeEnd, no part) at the replica version right after the
  initial 00:01 poke and its first config poke is numbered from that state
  version; TS (in that harness timing) sends the config poke first and folds
  the state-version jump into the sync poke. With a real zero-client the
  initConnection carries the desired set and both sides emit the same two
  pokes (config, then sync). Porting the laziness would make a client's first
  hydration depend on a notification that must then be guaranteed at spawn
  — a rule-8 execution-order change for a harness-only difference — so it is
  registered instead of ported.
- **Contract:** (a) the set and order of pokes per pass is TS's; (b) the only
  permitted deviation is at most ONE extra empty poke directly after the
  initial poke of a connection whose initConnection carried no queries; (c) a
  connection whose initConnection carries queries produces frames identical
  to TS.
- **Tests:** xyne-art `tools/frameseq_gate.py` counts this exact shape as
  `known(K1)` and fails on anything else; `hydrate_real_rows_produces_row_pokes`
  (stage_e) pins (c)'s poke contents.
- **Known gap:** none beyond (b).
