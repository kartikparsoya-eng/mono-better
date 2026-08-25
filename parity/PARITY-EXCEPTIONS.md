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
- **Rust** (`router.rs` `drain`, `MAX_DRAIN_MS = 25_000`): caps the total drain, then "rehomes remaining groups at once" + `shutdown()`.
- **Why kept:** deploy orchestrators SIGKILL after a ~30s stop-grace period. Draining indefinitely (TS behavior) risks the orchestrator hard-killing the process mid-sweep, truncating the graceful `shutdown()` + executor join and orphaning in-flight work. The 25s cap keeps the final shutdown graceful. This is a deployment-safety property, not a behavioral choice — matching TS here would reintroduce the hard-kill risk.
- **Scope:** only observable if a CG is stuck > 25s during drain (TS keeps draining; Rust rehomes). Documented at `router.rs:1011-1018`.

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

- **Context:** the PRODUCTION advance path (`advance_to_head_stream`, used by rust-syncer `pipeline_driver.rs` + `sync_engine.rs`) already implements the FULL TS `#shouldAdvanceYieldMaybeAbortAdvance` via `AdvanceGate` (`advance_gate.rs`) — all four arms + pause-aware timing. That part is faithful; F-PD-1/F-PD-2 were WRONG about it.
- **The residual:** `Engine::advance_streaming` (and the `Engine::advance()` wrapper) carry a simplified `AdvanceContext::should_abort` (the basic time-budget arm only). These have **no TS twin** — TS has no "apply this explicit change list" advance separate from the snapshotter-driven one; it's a Rust-only convenience used solely by the ART test harness `bin/server.rs` and unit tests, never by the syncer.
- **Why kept:** on a no-TS-twin Rust helper, the abort is a best-effort safety net; it is time-based (never part of the deterministic row-change trace), so it does not affect oracle parity or any production behavior. Reconciling it to the full `AdvanceGate` would only touch the test/dev path.

## D-5 · ConnectionContextManager reference module — F-CCM-1/2/3/4

- **Status:** `services/view_syncer/connection_context_manager.rs` is an explicitly **UNWIRED reference port** (its own header: "NOT WIRED INTO PRODUCTION — behavior changes belong in `router.rs`, NOT here"). Production installs `PlaceholderConnContextManager`; the live auth model is the per-CG state in `router.rs`.
- **Production matches TS** on the paths that matter: header filtering via `router.rs::filtered_query_headers` (#6144 `filterHeaders` — F-CCM-2's "real path is correct"); raw-token `authEquals` + signature revalidation + user-pinning in `handle_update_auth`.
- **F-CCM-1 (decoded-claims boundary) is unreachable:** the consumer is the CRUD mutagen, which is disabled (`create_mutagen` → `None`) so CRUD is Fatal-rejected before auth; custom mutations go through the push relay (`userPushURL`), not a local mutagen. The `ConnContextInfo.auth: Option<String>` raw-token boundary will be widened to carry decoded claims when the mutagen is wired (Phase 4) — there is no consumer to match TS against today.
- **Why kept:** the divergences are confined to unwired reference code; the live behavior already matches TS. The reference module is reconciled to TS on promotion, per its own header.

## D-6 · Row-patch emission order: `HashMap` iteration vs TS `Map` insertion order — F-CVR-6 (caveat)

- **TS** (`cvr.ts` `received()`): iterates the received-rows `Map` in insertion order, so row patches inside a poke come out in the order the pipeline produced them.
- **Rust** (`cvr.rs` `received(&rows: &HashMap<…>)`): iterates a `HashMap`, so the row-patch order inside a poke is arbitrary (and run-to-run nondeterministic).
- **Why kept:** row patches are key-addressed and independently versioned — no consumer is order-sensitive, and the parity harness itself institutionalizes this (generate-fixture.mjs: "Row patches come out in HashMap order on the Rust side, so both sides sort by rowIDString before comparing"). Matching TS byte-order would require threading insertion-ordered maps (`IndexMap`) through the entire engine→updater row path for zero semantic gain. Wire BYTES differ per poke; wire CONTENT is identical.

## D-7 · Dead write-back/defer machinery + async-flush metrics — F-RRC-1/F-RRC-9 (caveat)

- **TS** (`row-record-cache.ts`): `executeRowUpdates` supports `allow-defer` — a flush can defer row writes to a background task, with `#recordAsyncFlushStats` counting `cvr.flush_attempts{flush.type=async}`.
- **Rust**: `execute_row_updates` and the defer latch are a faithful port but have **no production caller** — `CVRStoreHandle::flush` writes rows inline in one atomic PG transaction (single-atomic-writer design, documented at the flush row-write section), so `rows_deferred` is always 0 and no `flush.type=async` counts are ever emitted (`cvr.flush_attempts` carries sync|noop only).
- **Why kept:** the single-writer flush is the Rust architecture's answer to the same problem (verified equivalent through the flush/seq TS-golden differentials); the deferred path would add a second writer and re-introduce the write-behind gap class of bugs. The ported-but-unwired code stays as the reference for a future write-behind actor (see SYNC-ENGINE-SCALABILITY-PROPOSAL).

## Minor notes (log/observability-only, not behavior)

- **Error message texts** (F-CVR-STORE-19): `CVRStoreError` kinds map 1:1 to the TS error classes (and `cvr_error_kind` labels match TS `cvrErrorKind` exactly), but two `Display` strings differ — `OwnershipError` prints raw epoch ms where TS prints ISO dates, and `ClientNotFound` carries a "Client not found:" prefix. Log-only.
- **`TTLClock` precision**: Rust `i64` ms vs TS double over a `DOUBLE PRECISION` column — sub-millisecond truncation on load (`ttl_clock as i64`). Same for the inspect query's `::bigint` casts on `ttl`/`inactivatedAt` (F-CVR-STORE-10).
- **Anonymous-telemetry counters** (F-CVR-3): TS `recordQuery` lives on the separate opt-out anonymous-telemetry meter (`anonymous-otel-start.ts`); Rust has no anonymous-telemetry subsystem, so `zero.crud_queries_processed` / `zero.custom_queries_processed` are registered on the shared meter under the same names.

---

_Add an entry here (with the TS source, the Rust divergence, and the justification)
whenever a finding is resolved as "deliberate" rather than fixed._
