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
- **Files:** `router.rs` (`dispatch_cg_message`, `run_cg_thread`, executors), `main.rs` runtime sizing.
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
- **Tests:** `update_auth_refreshes_the_forwarded_push_relay_token`,
  `relay_body_carries_user_push_overrides`.
- **History:** violated by bug-2 (auth was a connect-time snapshot). Fixed
  `97440d021` (auth → shared `Arc<Mutex>` refreshed in `handle_update_auth`).
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
- **Files:** `rust-ivm` Engine `Drop`, CG idle reap in `router.rs`.
- **No TS twin:** breaks Rc cycles / releases SQLite conns deterministically
  (TS relies on GC). Fires on idle-keepalive reap.
- **Contract:** teardown is observationally a no-op to a still-connected client;
  reap timing matches TS `DEFAULT_KEEPALIVE_MS` (5000). A reconnect after reap
  produces the SAME cold-rehydrate result TS produces for a fresh ViewSyncer.
- **Tests:** teardown_gate_test, `rust-g6-leak-hunt` census. **NOTE:** the reap
  makes a cold rebuild expensive — this is faithful (TS also rebuilds), but it
  is the amplifier that made bug-1 catastrophic; keep I-1's ack contract intact.

## I-6 — CVR write-behind / offload runtime
- **Files:** `sync_engine.rs` offload, CVR flush actor.
- **No TS twin:** rust offloads CVR I/O to the pool runtime; TS awaits inline.
- **Contract:** a poke is not sent to a client before the CVR state it reflects
  is durable to the SAME degree TS guarantees (no client observes a version the
  CVR hasn't recorded). Flush ordering per CG preserved.
- **Tests:** cvr flush ordering tests. **GAP:** durability-ordering oracle.

## I-7 — Cost-model / flip-planner COUNT(*) caching
- **Files:** `rust-ivm` planner cache, `engine::plan_ast`.
- **No TS twin:** batch-shared `PlanCountCache` per hydration.
- **Contract:** the plan chosen is identical to TS `planQuery(ast, costModel)` for
  the same replica state; the cache only avoids recomputation, never changes the
  plan.
- **Tests:** `g8_mychannelparticipations_real_ast`, diff-oracle full-catalog.

## I-8 — PlaceholderConnContextManager (LATENT DIVERGENCE — do not ship new consumers)
- **Files:** `main.rs` `PlaceholderConnContextManager`.
- **Status:** the prod CCM dispatch returns `auth:None, revision:0` always; the
  LIVE connection/auth state lives in `CgState` maps (`client_raw_auth`,
  `client_auth`, `client_query_ctx`, `PushRelayHeaders`). TS keeps ONE owner
  (`ConnectionContextManager`).
- **Why latent, not active:** the only reader of the placeholder is the mutagen
  CRUD path (`syncer_ws_message_handler.rs:407,421`), and `create_mutagen`
  returns `None` in prod, so that branch is dead. If CRUD mutagen is ever
  enabled, it would read `auth:None` → a bug of the SAME class as bug-2.
- **Contract owed:** promote the ported `connection_context_manager.rs` to be the
  single live owner (plan item 7), or, minimally, feed the placeholder from the
  live `CgState` auth so any future reader is correct.
- **Tests:** NONE yet — this is the structural work remaining.
