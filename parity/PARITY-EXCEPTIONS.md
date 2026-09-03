# Registered Parity Exceptions — Deliberate Rust ⇄ TS Divergences

Per the project's HARD RULES (AGENTS.md): the Rust crates are a strict 1:1 port of
TS, and **only STALE (already fixed) or WRONG (finding misreads code) justify not
matching TS**. Everything else is fixed to match TS.

The exception is a **deliberate, justified Rust-only divergence** — one that solves
a genuinely Rust-specific problem (memory management, the threaded-CG architecture,
a runtime-library limitation) where "matching TS" is impossible or would reintroduce
a real defect. Those are NOT fixed; they are **registered here** so the divergence is
auditable and intentional rather than accidental drift.

Anything not listed here and not STALE/WRONG must match TS.

---

## D-1 · Drain hard deadline (`MAX_DRAIN_MS = 25s`) — F-RT-3

- **TS** (`workers/syncer.ts` `Syncer.drain`): `while (this.#viewSyncers.size) { await forceDrainTimeout }` — drains indefinitely, paced only by `forceDrainTimeout`, with no wall-clock bound.
- **Rust** (`workers/syncer.rs` `drain`, `MAX_DRAIN_MS = 25_000`): caps the total drain, then "rehomes remaining groups at once" + `shutdown()`.
- **Why kept:** deploy orchestrators SIGKILL after a ~30s stop-grace period. Draining indefinitely (TS behavior) risks the orchestrator hard-killing the process mid-sweep, truncating the graceful `shutdown()` + executor join and orphaning in-flight work. The 25s cap keeps the final shutdown graceful. This is a deployment-safety property, not a behavioral choice — matching TS here would reintroduce the hard-kill risk.
- **Scope:** only observable if a CG is stuck > 25s during drain (TS keeps draining; Rust rehomes). Documented at `workers/syncer.rs:1229` (`drain`; post-L9 home of the old `router.rs` drain).

## D-2 · View entry copy-on-write: `Rc::make_mut` vs TS `WeakSet` — F-VIEW-2

- **TS** (`ivm/view-apply-change.ts`): `Mutate = boolean | WeakSet<object>`; a transaction-scoped `WeakSet` tracks which entries the current transaction owns, so already-observed nodes stay immutable while freshly-created ones are mutated in place. This is explicitly a **JS-GC allocation optimization**.
- **Rust** (`ivm/view.rs`): `Mutate = bool`; structural sharing comes from `Rc`, and `Rc::make_mut` provides copy-on-write at the `Rc<Entry>` level (mutate in place when uniquely owned, clone when shared).
- **Why kept:** `WeakSet` is a GC mechanism with no Rust equivalent; `Rc` COW is the idiomatic Rust realization of the same intent and produces a **content-identical** final tree (`entries_equal` falls back to structural comparison, so reference-identity differences never yield a wrong result). This is the HARD RULE #5 memory-management exception.
- **Covers F-VIEW-3** (`inc_ref_count`/`dec_ref_count` take `_mutate` and always clone): the `mutate` flag is TS's per-transaction allocation hint, subsumed here by `Rc` ownership tracking — the same mechanism, so the ignored param is part of this exception, not a separate divergence. Output is identical; only intermediate allocation differs.

## D-3 · Push-path fetch yields dropped (`skip_yields`) — PATTERN-A (F-CAP-3/F-EX-1/F-TAKE-2/F-PD-3)

- **TS**: push is a generator; a fetch during `push` propagates `'yield'` sentinels (`for (const n of fetch(...)) { if (n === 'yield') { yield n; continue } … }`) so the cooperative scheduler can interleave.
- **Rust**: push is a synchronous callback (`Output::push` → `FnMut(&RowChange)`), with no generator/scheduler to yield to mid-push. The push-path re-fetches use `skip_yields(...)`.
- **Why kept:** a `yield` is a cooperative-scheduling sentinel, not a row change. In a synchronous push there is no yield point, so dropping them cannot change the output. The **initial-hydration / read-path** fetches — the ones the differential oracle actually records into the trace — DO propagate yields (`cap.rs:228`, `take.rs:359/944`, `exists.rs:167`). The 1822-fixture oracle passes with push-path yields dropped, confirming the output row-change stream is identical. Matching TS here would require re-architecting the entire push path from synchronous callbacks to generators for zero observable gain — the HARD RULE #5 architecture exception.

## D-4 · `advance_streaming` simplified `should_abort` — F-PD-1/F-PD-2 (non-production only)

- **Context:** the PRODUCTION advance path (`advance_to_head_stream`, used by rust-syncer `pipeline_driver.rs` + `services/view_syncer/view_syncer.rs`) already implements the FULL TS `#shouldAdvanceYieldMaybeAbortAdvance` via `AdvanceGate` (`advance_gate.rs`, invention I-11) — all four arms + pause-aware timing. That part is faithful; F-PD-1/F-PD-2 were WRONG about it.
- **The residual:** `Engine::advance_streaming` (and the `Engine::advance()` wrapper) carry a simplified `AdvanceContext::should_abort` (the basic time-budget arm only). These have **no TS twin** — TS has no "apply this explicit change list" advance separate from the snapshotter-driven one; it's a Rust-only convenience used solely by the ART test harness `bin/server.rs` and unit tests, never by the syncer.
- **Why kept:** on a no-TS-twin Rust helper, the abort is a best-effort safety net; it is time-based (never part of the deterministic row-change trace), so it does not affect oracle parity or any production behavior. Reconciling it to the full `AdvanceGate` would only touch the test/dev path.

## D-5 · ConnectionContextManager — RETRACTED (2026-09-01: promoted to the single live owner in #155)

- **No longer a divergence.** When first written, `services/view_syncer/connection_context_manager.rs` was an UNWIRED reference port and production ran `PlaceholderConnContextManager` with a parallel per-CG auth model. Task **#155 (invention I-8)** promoted the ported CCM to the **single live owner** of per-connection auth + custom-query context: `PlaceholderConnContextManager` and the parallel maps (`client_auth`/`client_raw_auth`/`client_query_ctx`) were **DELETED**, and every consumer now reads `must_get_connection_context(...)` at use time (`services/view_syncer/view_syncer.rs`). The live model matches TS `auth.ts` / `transform-query.ts`, so there is nothing to register.
- **Residual (not a divergence):** the decoded-claims boundary (former F-CCM-1) widens to carry decoded claims only if the CRUD mutagen is wired; today CRUD is Fatal-rejected (`create_mutagen → None`) and custom mutations relay via the push path, so there is no local consumer to reconcile against. See `parity/INVENTIONS.md` **I-8**.

## D-6 · Row-patch emission order: `HashMap` iteration vs TS `Map` insertion order — F-CVR-6 (caveat)

- **TS** (`cvr.ts` `received()`): iterates the received-rows `Map` in insertion order, so row patches inside a poke come out in the order the pipeline produced them.
- **Rust** (`cvr.rs` `received(&rows: &HashMap<…>)`): iterates a `HashMap`, so the row-patch order inside a poke is arbitrary (and run-to-run nondeterministic).
- **Why kept:** row patches are key-addressed and independently versioned — no consumer is order-sensitive, and the parity harness itself institutionalizes this (generate-fixture.mjs: "Row patches come out in HashMap order on the Rust side, so both sides sort by rowIDString before comparing"). Matching TS byte-order would require threading insertion-ordered maps (`IndexMap`) through the entire engine→updater row path for zero semantic gain. Wire BYTES differ per poke; wire CONTENT is identical.

## D-7 · Dead write-back/defer machinery + async-flush metrics — F-RRC-1/F-RRC-9 (caveat)

- **TS** (`row-record-cache.ts`): `executeRowUpdates` supports `allow-defer` — a flush can defer row writes to a background task, with `#recordAsyncFlushStats` counting `cvr.flush_attempts{flush.type=async}`.
- **Rust**: `execute_row_updates` and the defer latch are a faithful port but have **no production caller** — `CVRStoreHandle::flush` writes rows inline in one atomic PG transaction (single-atomic-writer design, documented at the flush row-write section), so `rows_deferred` is always 0 and no `flush.type=async` counts are ever emitted (`cvr.flush_attempts` carries sync|noop only).
- **Why kept:** the single-writer flush is the Rust architecture's answer to the same problem (verified equivalent through the flush/seq TS-golden differentials); the deferred path would add a second writer and re-introduce the write-behind gap class of bugs. The ported-but-unwired code stays as the reference for a future write-behind actor (see SYNC-ENGINE-SCALABILITY-PROPOSAL).

## D-8 · IVM planner/runtime deliberate adaptations (Pairs 36-40 verification)

- **`PlanDebugger` absent** (F-PLANNER-1): TS's `PlanDebugger` (planner-debug.ts) is a pure event sink — nothing in `plan()`/`estimateCost` reads back from it, and its only production consumers are the analyze/inspector tools (a known deferred subsystem). Omission cannot change plan selection.
- **`planner-terminus.ts` `pinned` absent** (F-PLANNER-4): dead code in TS — `get pinned() { return true }` has zero readers (the `pinned` field in `ConnectionCostsEvent` belongs to an event that is never emitted; legacy of the old greedy planner).
- **`flipIfNeeded` absent** (F-PLANNER-9): test-only in TS (only callers are planner-join.test.ts).
- **UnionFanIn push forwarding via all-branch match-count** instead of TS's pusher-identity skip: equivalent under the invariant that the pusher branch's post-change fetch reflects its own change (add→found, remove→absent); pinned by the union-fan-in tests + the differential oracle. Also: single-input fetch skips the merge-dedup (unobservable for sorted-unique branches).

## D-9 · Malformed-baseCookie rejection TIMING (connect vs first init)

- **TS**: the baseCookie from the connect URL is parsed lazily, when the
  ClientHandler is constructed while processing the first `initConnection`
  (client-handler.ts `cookieToVersion` → schema/types.ts `versionFromString`
  throw → `wrapWithProtocolError` → fatal `Internal` error frame + close).
- **Rust**: the same validation runs at connection REGISTRATION
  (`workers/syncer.rs` `on_new_connection`, right after `connected` is sent) —
  registration materializes `client_base_versions`, so deferring the throw to
  init handling would let a poke race an invalid base version.
- **Observable difference**: identical frames (`connected` → `["error",
  {kind: Internal, message: <versionFromString error>}]` → close 1000); the
  only divergence is that Rust emits them even if the client never sends an
  `initConnection`, and before (rather than after) consuming one. Real zero
  clients always send init immediately, so the orderings are indistinguishable
  in practice; the xyne-art G36 harness tolerates both (open_side send guard).

## D-10 · Push-ack latency +~20ms (Option-A relay hop)

- **TS**: a custom-mutation push is fetched ONCE from the syncer's pusher to
  the app's mutate endpoint (services/mutagen/pusher.ts → custom/fetch.ts).
- **Rust**: pushes deliberately relay through a TS loopback endpoint
  (push_relay.rs → server/rust-push-relay.ts → second fetch to the app) so
  ZERO mutation logic lives in rust (the Option-A write-path design). The
  extra loopback POST + body rebuild adds ~20ms to the steady-state push
  ack (G42 `push`: rust ~66ms vs TS ~43ms p50; commit→ack legs identical).
- **Gate treatment**: G42 gives the `push` class caps of 2.0×/3.0× (vs the
  default 1.5×/2.0×) citing this entry; the number still prints every run.
  The once-per-CG `push_first` cost is NOT part of this exception — it is
  tracked as an open item (task #127), not a design cost.

## D-12 · API-metric recorders folded into `custom/metrics.rs` (OTel idiom)

- **TS**: `custom/metrics.ts` exports lazy instrument ACCESSORS (`apiAttempts()`,
  `apiAttemptDuration()`, `apiInFlight()`); `custom/fetch.ts` defines the
  RECORDERS that call them (`recordApiAttempt`, `apiRequestMetricAttrs`).
- **Rust**: `custom/metrics.rs` holds the instruments in a static `api_otel()`
  and the recorders (`record_api_attempt`, `api_request_metric_attrs`) live
  beside them; `custom/fetch.rs` calls `record_api_attempt(...)`.
- **Why (rule 5, OTel idiom)**: rust OTel holds instruments once and records
  through them, so recorder-beside-instrument is the natural cohesion. Splitting
  `record_api_attempt` back into `fetch.rs` (to mirror fetch.ts) would force it
  to reach into `metrics.rs`'s `api_otel()` internals — worse, not more 1:1.
  This is the only remaining `custom/fetch.ts` "split" contributor; the rest of
  fetch.ts is 1:1 in `custom/fetch.rs`.

## D-11 · `CVR_CURSOR_PAGE_SIZE` env knob (default = TS 10000) — POST-MORTEM of a retracted divergence

- **TS** (`view-syncer.ts:2844`): `#processChanges` flushes the accumulated row
  batch to the CVR updater + pokers every **10000** rows, hardcoded.
- **Rust** (`rust-cvr/src/change_processor.rs`): same 10000 default, plus a
  rust-only **`CVR_CURSOR_PAGE_SIZE`** env for experiments (invalid/0 → 10000).
- **RETRACTED divergence (2026-08-29)**: the default was briefly lowered to 100
  on the theory that the boundary only changes WHEN patches flush. **That theory
  was wrong and prod falsified it within 25 minutes**: CG `kggpbcl9ths15umnnr`
  panicked at `cvr.rs:1009` ("Expected CVR version to have been bumped above
  original"). The batch is the **de-dupe window** — same-row churn nets out
  inside one batch, and `received()` skips the version stamp only for rows whose
  state MATCHES the CVR record. A smaller batch flushes rows MID-churn, whose
  transient state differs from the CVR, in passes that legitimately never bump
  the version (the poke-start cookie contract, TS `#assertNewVersion`
  cvr.ts:764-776). The value participates in correctness; rule 1 applies.
- **Pinned by**: `cursor_page_size_env_resolution` +
  `new_reads_env_and_defaults_to_ts_page_size` (default = 10000) +
  `small_flush_boundary_splits_churn_and_trips_version_assert` (the prod panic
  reproduced at page size 1, `#[should_panic]`). Do NOT lower the env below a
  pass's churn window; it exists for controlled experiments only.

## D-13 · WebSocket per-message compression not offered (`ZERO_WEBSOCKET_COMPRESSION`)

- **TS** (`workers/syncer.ts` `getWebSocketServerOptions`): when `websocketCompression`
  is enabled (default **false**, zero-config.ts:818) the `ws` server negotiates
  `permessage-deflate` with the client (optionally with `websocketCompressionOptions`).
- **Rust** (`ws_server.rs`): tokio-tungstenite 0.24 has no permessage-deflate
  implementation, so the extension is never offered; a configured
  `ZERO_WEBSOCKET_COMPRESSION=true` logs "WebSocket compression requested but is not
  supported by this server" and serves uncompressed.
- **Why kept (2026-09-03):** library gap, not a port choice. Client-observable only
  as wire BYTES (frame content/order identical); the prod TS deployment does not
  enable it. Revisit when tungstenite ships the extension.

## D-14 · Debug / introspection / observability-only TS helpers not ported

- **TS**: debug decorators and introspection getters on the IVM graph, lap timers,
  pipeline-lifecycle debug logs, the `pipelineRunID` correlation id.
- **Rust**: none of it exists; the equivalent visibility comes from `tracing`
  fields at the call sites and the `zero.*` metrics.
- **Why kept (2026-09-03):** nothing a client can observe; ledger members (cite
  this id in their alias note): `addedge`, `decorateinput`, `decoratefilterinput`, `getconstraintsfordebug`, `getfiltersfordebug`, `getsortfordebug`, `getconstraintcostsfordebug`, `getdebuginfo`, `getnodename`, `elapsedlap`, `totalelapsed`, `randomid`, `logquerypipelinelifecycle`.

## D-15 · Node-runtime-only helpers (no rust twin possible)

- **TS**: `errno`-based socket-error classification, `setImmediate` yields, the
  worker bootstrap, JWK-pair minting for tests.
- **Rust**: tungstenite/tokio have no errno objects (message-based classification,
  `has_transient_socket_code` / `is_transient_socket_message`), the executor
  yields via tokio, the process entry is `main.rs`, keys are only verified.
- **Why kept (2026-09-03):** platform-bound; members: `startwithoutyielding`, `yieldprocess`, `haserrno`, `hastransientsocketcode`, `createjwkpair`, `runworker`.

## D-16 · Language-idiom mappings (generators, type guards, COW, asserts)

- **TS**: generator drains, `assert*` type guards, WeakSet copy-on-write tracking,
  memory-source forks, `unreachable`/`assert`.
- **Rust**: iterators, the type system, `Rc::make_mut`, SQLite-backed sources,
  `unreachable!()`/`assert!()`.
- **Why kept (2026-09-03):** same observable behavior by construction; members:
  `assertarray`, `assertnumber`, `assertmetaentry`, `track`, `owns`, `flipifneeded`, `fork`, `stringify`, `draingenerator`, `unreachable`, `assert`, `logqueryfailure`.

## Minor notes (log/observability-only, not behavior)

- **Error message texts** (F-CVR-STORE-19): `CVRStoreError` kinds map 1:1 to the TS error classes (and `cvr_error_kind` labels match TS `cvrErrorKind` exactly), but two `Display` strings differ — `OwnershipError` prints raw epoch ms where TS prints ISO dates, and `ClientNotFound` carries a "Client not found:" prefix. Log-only.
- **`TTLClock` precision**: Rust `i64` ms vs TS double over a `DOUBLE PRECISION` column — sub-millisecond truncation on load (`ttl_clock as i64`). Same for the inspect query's `::bigint` casts on `ttl`/`inactivatedAt` (F-CVR-STORE-10).
- **Anonymous-telemetry counters** (F-CVR-3): TS `recordQuery` lives on the separate opt-out anonymous-telemetry meter (`anonymous-otel-start.ts`); Rust has no anonymous-telemetry subsystem, so `zero.crud_queries_processed` / `zero.custom_queries_processed` are registered on the shared meter under the same names.

---

_Add an entry here (with the TS source, the Rust divergence, and the justification)
whenever a finding is resolved as "deliberate" rather than fixed._
