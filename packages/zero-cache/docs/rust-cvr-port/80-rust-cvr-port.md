# Rust CVR Port — Master Plan

**Status:** Draft plan. Branch: `rust-cvr-v1.0.0`. Depends on `rust-ivm-v1.7.0` (engine + snapshotter + planner, all already ported and prod-tested).

## Why this port exists

The end goal stated months ago: **completely get out of the event loop for the syncer**. The engine (`rust-ivm-v1.6/v1.7`) removed query evaluation from the JS event loop. But the syncer still runs full CVR mutation in JS: every `advance()` runs CVRUpdater logic, computes refCounts, builds the per-connection poke bodies, and pushes JSON to WebSockets — all single-threaded Node.

Moving CVR + client-handler into Rust closes the loop: hydrate → advance → diff → body assembly → socket frame → flush — everything runs on the OS thread that already owns the engine (Rust `EngineHandle::spawn()` per CG). TS becomes a dispatch shell.

## What stays in TS

- The WebSocket server (`syncer.ts`) and HTTP route handling
- Auth / cookie plumbing / `initConnection` handshake (trivial)
- The high-level `ViewSyncerService` driver that responds to view-syncer lock, change-streamer notifications, and decides *when* to advance/catchup/hydrate — but the per-decision work is delegated to Rust
- Broadcasts (change-streamer side) (`broadcast.ts`, `change-streamer-http.ts`)
- Replicator, schema migrations, copy-pipelines

## What goes in

| Layer | TS LOC | Rust equivalent | Notes |
|---|---|---|---|
| `cvr-store.ts` | 1382 | `packages/rust-cvr/src/store.rs` | Direct DB writes via `sqlx::PgConnection`. TS caller gets opaque handle. |
| `cvr.ts` | 1194 | `packages/rust-cvr/src/cvr.rs` + `updater.rs` | Two updater classes follow one-to-one (ConfigDriven, QueryDriven). |
| `client-handler.ts` | 523 | `packages/rust-cvr/src/client_handler.rs` | Per-connection poke chain + body assembly + push. Need a Rust-side WS sink trait. |
| `row-record-cache.ts` | 469 | `packages/rust-cvr/src/row_record_cache.rs` | LRU + write-back mode + cursor-based `catchupRowPatches`. |
| `schema/types.ts` | ~380 | `packages/rust-cvr/src/schema.rs` | Serde types + invariants (currently enforced by valita on the JS side). |
| `schema/cvr.ts` | ~330 | `packages/rust-cvr/src/ddl.rs` | Static SQL strings; run verbatim. |
| `row-set-signature.ts` | 29 | `packages/rust-cvr/src/row_set_signature.rs` | Pure function; reuse `xxh3_64` (rust already depends on `xxhash-rust`). |

**Total egress: ~4300 TS LOC, ~1500-2500 Rust LOC.** The Rust implementation will be smaller because:
- No valita schema parsing en route (invariants enforced at compile time + serde derive)
- No `structuredClone` / deep-equal (Rust borrows)
- No async/await ceremony for non-async methods (`flush()` on Updater is sync except for the CVRStore call)

## Four-phase rollout

| Phase | Ships | What TS does | What Rust does |
|---|---|---|---|
| **A. Row-set signature + helpers** | `RUST_CVR_SIGNATURE` | drives existing helper code paths | hashes, format/parse |
| **B. Row-record-cache** | `RUST_CVR_ROW_CACHE` | wraps Rust handle, calls `apply()` and `catchupRowPatches()` via napi | cache + write-back + read-path |
| **C. Updaters (Config+Query driven)** | `RUST_CVR_UPDATERS` | constructs updaters via napi, passes them to handler | patch generation, refCount math, version logic |
| **D. ClientHandler + PokeHandler** | `RUST_CVR_POKER` | calls `startPoke(version) -> PokeHandle`, forwards rows, no body building | poke chain + body assembly + socket frame construction |
| **E. CVRStore (final)** | `RUST_CVR_STORE` | legacy fallback while `!USE_RUST_IVM` | full DB writer |

Each step is independently toggleable via env-var + napi hook. Production runs stock TS until `USE_RUST_CVR=1`.

## Two paused design decisions (unblocking required before phase B)

1. **Void-flush semantics.** `RowRecordCache.apply()` in TS currently does `void` on the initialize-ownership UPDATE if the ownership signal fails (`cvr-store.ts:395`). Should Rust match that fire-and-forget behavior, or return a proper error? **Recommendation:** match the TS behavior for byte-for-byte parity, log-and-fail-visible. Failing to signal ownership only delays the load-retry, doesn't corrupt state.

2. **Poke-backpressure exactness.** `client-handler.startPoke` clears `#pokeTail` ordering. Porting it means committing to a Rust-side ordering guarantee. **Recommendation:** keep the same gate semantics (new poke doesn't open until previous completes) — this is not negotiable per the existing zero-poke-handler.ts contract.

## Interaction with the existing rust-ivm driver

The current `rust-ivm-driver.ts` already talks to a Rust `EngineHandle` via napi for hydrate/advance/cancel. Adding the CVR port extends the handle's surface — it doesn't create a second channel. The CVR port can reuse `packages/rust-ivm/src/sqlite/` for row encoding and `packages/rust-ivm/src/advance_gate.rs` for cadence if both sides need the same semantics. **Do NOT create a separate rust-cvr-thread-per-CG.** One CG = one Rust thread = engine + cvr + poke handler all sharing it.

## Rollback plan

Each phase ships behind its own env flag, with the **TS implementation retained**. Rolling back is `unset` + restart. If we ever reach phase E, the TS CVR code path remains in `zero-cache/src/services/view-syncer/ts-cvr-legacy/` for one major release, then deletes.

## Reading order for the detailed docs

1. `81-cvr-store-port.md` — the DB writer (largest chunk)
2. `82-cvr-updater-port.md` — the in-memory version/tracker state machine
3. `83-row-record-cache-port.md` — the write-back LRU + cursor reader
4. `84-client-handler-port.md` — the per-connection poke serializer
5. `85-open-decisions.md` — void-flush + backpressure, with tradeoffs
6. `86-migration-plan.md` — phase-by-phase rollout with flag names and rollback triggers
