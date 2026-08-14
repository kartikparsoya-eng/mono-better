# 90 — Phase 7: Real Wiring Plan (end-to-end Rust syncer)

**Status:** Execution plan. Follows doc 89 (Full Rust Syncer spec).
**Goal:** Turn the compiling `rust-syncer` _shell_ into a working end-to-end
syncer by replacing the placeholder trait implementations with real bridges to
`rust-cvr` (CVR/pokes) and `rust-ivm` (query engine), then prove parity against
the TS syncer.

---

## ⚠️ STATUS UPDATE (2026-08-14) — the body below is largely historical

Stages A–D are effectively **done**: the pure `rust-syncer`
(`ZERO_SYNCER=rust`) runs real end-to-end — real `PipelineDriver`→`rust-ivm`,
real `CVRStoreOps`+poke emission→`rust-cvr`, notification loop, auth,
mutations. The old `view_syncer.rs` placeholder is gone; `sync_engine.rs`,
`pipeline_driver.rs`, `cvr_store`, `auth.rs`, `permissions.rs` are the real
impls. A live capacity ladder held **~93 concurrent connections** (vs TS ~65 at
the same 4-core cap) — you cannot serve that without the full brain wired.

Changed since this plan was written:

- **NAPI rust-IVM hybrid REMOVED on this branch** (commit `a5e502ad9`). The napi
  crates (`rust-ivm/napi`, `rust-cvr/napi`) and all TS hybrid wiring
  (`rust-ivm-driver.ts`, `rust-cvr-addon.ts`, `USE_RUST_IVM`, the differential
  harness/tests) are deleted; the TS view-syncer is restored to `zero/v1.7.0`.
  The napi/rust-IVM work continues on a **separate branch** — it is no longer
  this branch's fallback. This supersedes the "napi crates stay until parity
  green" decision below.
- **Exactly one flag** now selects the engine: `ZERO_SYNCER=rust` (rust-syncer)
  vs unset/`ts` (pure TS, upstream 1.7.0 behavior). Rust-specific tuning flags
  may still exist, but there is no second engine toggle.

**Remaining to ship (unchanged in spirit — Stage E):** full ART gate on the
current image (G4/G8/G13; G22 capacity already passes), multi-day zero-diff
shadow-parity soak, then flip the default to `ZERO_SYNCER=rust`. Port any
remaining TS view-syncer tests (gap #9).

---

## Why this doc exists

Doc 89's phases 1–6 were completed as commits, but "Phase 6: Process manager
integration" wired the process manager to **placeholder** services, not the real
engine. The current `rust-syncer` binary accepts a WebSocket, sends `connected`,
and does nothing real — it never opens the replica, connects to Postgres, runs
the engine, or emits a real poke.

This is by design: doc 89 established clean trait seams
(`PipelineDriver`, `CVRStoreOps`, `AuthValidator`, `CGServicesFactory`,
`ViewSyncerDispatch`) so the shell and the brain could be built separately. The
shell is done. This doc covers building and wiring the brain.

## Current state (verified)

| Crate         | State          | Notes                                                                                                                                                                                                                                                                     |
| ------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `rust-ivm`    | ✅ ~production | Pure-Rust `Engine`: `new`, `register_source`, `add_queries_streaming` (mod.rs:466), `advance_to_head_stream` (:774), `get_row` (:1235), `row_set_signature` (:425). SQLite via `Snapshotter`/`TableSource`. Parity-locked, 421 tests. napi kept as TS fallback.           |
| `rust-cvr`    | ✅ ~95–100%    | Real PG store (sqlx), updaters producing poke patches, `client_handler` (`start_poke` :495, `send_query_transform_failed_error` :575, `PokeHandler` Drop :394), row-record cache. Defines its own `WebSocketSink` trait (client_handler.rs:30). napi kept as TS fallback. |
| `rust-syncer` | ⚠️ ~25% real   | Shell compiles (`cargo check` green). Real: WS server, Connection, MessageHandler, ConnectionContextManager, DrainCoordinator, TTLClock, HTTP server, dispatch-loop skeleton. Brain: all behind placeholders.                                                             |

### Placeholder / stub inventory (what Phase 7 replaces)

- `main.rs:133-204` — `PlaceholderServicesFactory`, `PlaceholderViewSyncer`,
  `PlaceholderConnContextManager`, `PlaceholderAuthValidator` (all no-ops).
- `router.rs:444-451` — `CGMessage::Notification` logs but is **not** wired to
  `view_syncer.state_changes_rx`.
- `router.rs:458-521` — `process_connection_messages` is a "simplified version"
  that handles only **one connection at a time** per CG thread.
- `view_syncer.rs:596-597` — `queries_to_add` / `queries_to_remove` empty
  (CVR↔desired diff not computed). **CRITICAL.**
- `view_syncer.rs:783,789,798` — config-update patch / client-schema / delete
  not applied to CVR store. **CRITICAL.**
- `view_syncer.rs:561` — `hydrate_unchanged_queries` doesn't call the engine.
- `view_syncer.rs:506,600` — `auth: None` (not sourced from background conn).
- `view_syncer.rs:415,733,744,897` — low-sev: last_connect_time, delete_clients
  extraction, inspect body, hydration timing.
- Missing deps: `jsonwebtoken` (auth), `opentelemetry` (metrics), `sqlx`/
  `rusqlite` are transitive via cvr/ivm (fine).

### Known architectural glue points

- `DirectWebSocketSink` (ws_sink.rs) is a **struct**, not an impl of
  `rust_cvr::client_handler::WebSocketSink`. Stage B must implement that trait
  for the sink (or an adapter).
- The CG thread (router.rs) is a plain OS thread with no tokio runtime; PG I/O
  from `rust-cvr` (async sqlx) must cross via `Handle::block_on` on that thread —
  matches doc 89's threading model ("CVRStore flush: CG thread → block_on(tokio)").

---

## Decisions locked

- **napi crates stay** (`rust-cvr/napi`, `rust-ivm/napi`) until the pure-Rust
  path passes shadow parity. They are the `ZERO_SYNCER=ts` fallback. Deleted in
  Stage E only after parity is green.
- Trait-seam architecture is kept. Phase 7 writes concrete impls, not rewrites.

---

## Stages

Estimated ~18–23 working days. Stages A and B touch different seams and can
overlap; the critical path is **B → C** (pokes must be wired before the
notification loop is meaningful), then **E**.

### Stage A — Concrete `PipelineDriver` (bridge to rust-ivm) · ~3–4d

Implement a real driver held on the CG thread, replacing the
`PipelineDriver` placeholder.

- [ ] New `src/pipeline_driver.rs` implementing `view_syncer::PipelineDriver`.
- [ ] Hold `rust_ivm::Engine` + `Snapshotter` + registered `TableSource`s.
- [ ] Open `REPLICA_FILE` (config) via `Snapshotter::new` + `init`.
- [ ] Add/expose a `TableSource::new(db_path, schema, primary_key)` factory in
      `rust-ivm` (pattern already exists in `rust-ivm/napi/src/lib.rs`).
- [ ] `hydrate_and_sync` → `engine.add_queries_streaming` (collect RowChanges).
- [ ] `advance_and_sync` → `engine.advance_to_head_stream` (diff from snapshotter).
- [ ] `row_set_signature` → `engine.row_set_signature` (u64 → String).
- [ ] `get_row` exposed for catchup.
- [ ] `init` / `reset` / `replica_version` / `advance_without_diff` / `destroy`.
- [ ] Unit test: hydrate a query against a fixture replica, assert rows.

**Risk:** signature is currently maintained on the hydrate path but not emitted
during streaming advance (see doc 89 gap #4 / rust-ivm assessment). Confirm the
TS driver recomputes post-advance and match that behavior.

### Stage B — Concrete `CVRStoreOps` + poke emission (bridge to rust-cvr) · ~4–5d

This is where a real poke first reaches a client.

- [ ] New `src/cvr_store.rs` implementing `view_syncer::CVRStoreOps`.
- [ ] Build `sqlx::PgPool` from `CVR_PG_URI`; wire `load` / `update_ttl_clock` /
      `flushed` / `wait_flushed` to `rust_cvr::store`.
- [ ] Implement `rust_cvr::client_handler::WebSocketSink` for
      `DirectWebSocketSink` (or an adapter type). Wire `ClientHandler::new`.
- [ ] Close CRITICAL view_syncer TODOs:
  - [ ] `view_syncer.rs:596-597` — compute `queries_to_add`/`queries_to_remove`
        from the CVR↔desired-queries diff.
  - [ ] `view_syncer.rs:783` — apply desiredQueriesPatch to the updater.
  - [ ] `view_syncer.rs:789` — set client schema in CVR store.
  - [ ] `view_syncer.rs:798` — delete client from CVR store.
- [ ] Route updater patches → `ClientHandler::start_poke` → `PokeHandler`
      (pokeStart/Part/End) → `DirectWebSocketSink`.
- [ ] Wire `send_query_transform_failed_error` path.
- [ ] Integration test (against a test PG): init connection → desired query →
      assert pokeStart/pokePart/pokeEnd frames on the sink.

### Stage C — CG-thread services + notification loop · ~3d

Make replication changes actually drive pokes.

- [ ] Replace `PlaceholderServicesFactory` with a real `CGServicesFactory`
      (main.rs) that, per client group, spawns one dedicated OS thread running
      `RustViewSyncer::run()` holding the Stage A driver + Stage B store.
- [ ] Provide the CG thread a tokio `Handle` for `block_on` PG/replica I/O.
- [ ] Wire `router.rs:450` `CGMessage::Notification` → `state_changes_rx`.
- [ ] Wire HTTP `/notify/:cg_id` (http_server.rs) → the CG channel.
- [ ] Fix `process_connection_messages` (router.rs:458) to handle multiple
      connections per CG concurrently (currently one-at-a-time).
- [ ] Test: POST a notification, assert the run loop advances + pokes clients.

### Stage D — Auth + mutations + observability · ~3–4d

- [ ] Add `jsonwebtoken` dep; real `AuthValidator` (main.rs) using
      `AUTH_JWK` / `AUTH_JWKS_URL` / `AUTH_SECRET`; JWKS fetch on tokio.
- [ ] Implement `create_mutagen` / `create_pusher` (router.rs factory) for
      mutation forwarding to `MUTAGEN_URL` / `PUSHER_URL`.
- [ ] Add `opentelemetry` metrics; real `/statz` + `/heapz` (http_server.rs).
- [ ] Fill low-sev TODOs: hydration timing (:897), last_connect_time (:415),
      inspect body (:744), delete_clients extraction (:733), auth on
      advance/hydrate params (:506,:600).

### Stage E — End-to-end + parity, then cleanup · ~5–7d

- [ ] Boot `ZERO_SYNCER=rust` against zbugs; verify full lifecycle
      (connect → desired queries → hydrate → mutate → poke → advance).
- [ ] Port remaining TS view-syncer tests to Rust integration tests
      (doc 89 gap #9).
- [ ] Shadow parity run: `ZERO_SYNCER=rust` vs `ZERO_SYNCER=ts` (reuse
      rust-ivm's differential-oracle harness) — target zero-diff.
- [ ] After parity green: delete `rust-cvr/napi` + `rust-ivm/napi` (doc 89
      gaps #1, #2, #10). Keep TS syncer as flagged fallback.

---

## Definition of done

- `ZERO_SYNCER=rust` runs zbugs end-to-end with no TS syncer in the path.
- Multi-day zero-diff shadow parity vs `ZERO_SYNCER=ts`.
- napi hot-path boundary removed for the Rust path.
- All doc 89 gap-closure items (#1–#10) satisfied.

## Out of scope (unchanged)

Change-streamer, replicator, reaper (separate TS processes), zero-client, wire
protocol, CVR PG schema, SQLite replica format. See doc 89 "What Does NOT Change".
