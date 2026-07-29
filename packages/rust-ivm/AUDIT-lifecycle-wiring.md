# Phase 3 — Lifecycle + Wiring Audit (Rust IVM ↔ TS view-syncer)

Systematic pass over the reset/teardown/recovery/cleanup contract, deriving
each invariant from the TS source (`view-syncer.ts`, `pipeline-driver.ts`,
`snapshotter.ts`) and verifying the Rust port + napi driver honour it. This is
the "invariant checklist" — not reactive bug-chasing.

Legend: ✅ verified-correct · 🔧 divergence found + fixed · ⚠️ known gap (documented)

## A. Overlay / fetch wiring (the source-drift family)

- 🔧 **A1 — Source-level vs join-level overlay.** `TableSource.fetch` /
  `TableSourceInput.fetch` used `join_utils::generate_with_overlay*`
  (suppress-add + "overlay never applied" assert) instead of the source-level
  overlay from `memory-source.ts` (INJECT add, SUPPRESS remove, no assert).
  Also gated on `last_pushed_epoch < epoch` (inverted). Against the read-only
  PREV snapshot the added row is legitimately absent → assert tripped whenever
  a Join/EXISTS re-fetched a source mid-advance (fuzzer seeds 335/762/934).
  Fixed by extracting MemorySource's correct logic into shared
  `apply_source_overlay` + `>= epoch` gate. **Commit a7771b5.**
- 🔧 **A2 — Join child overlay ordered/unordered.** `join.rs` used ordered
  `generate_with_overlay` unconditionally; TS `join.ts:279-290` picks the
  UNORDERED variant when `childSchema.sort === undefined`. Fixed to branch on
  `schema.sort.is_none()`. **Commit 1d0862e.**
- ✅ **A3 — Join operators keep join-utils overlay.** join.rs:145/335 correctly
  use join-utils (the fetched child stream contains the change there). Only the
  *source* fetch was wrong. `generate_with_overlay_join` (join.rs:425) is dead.

## B. Reset-reason contract (advance → ResetPipelinesSignal vs teardown)

TS `ResetPipelinesReason` = {advancement-timeout, scalar-subquery,
schema-change, truncation, permissions-change}.

- ✅ **B1 — schema-change / truncation / permissions-change** emitted by
  `diff.rs` (REASON_* constants) → -2 reset row → in-place reset. Matches TS.
- ✅ **B2 — advancement-timeout** emitted by `engine/mod.rs` (same
  MIN_ADVANCEMENT_TIME_LIMIT + half-budget test as pipeline-driver.ts:857).
  See D1 for the partial-commit safety proof.
- ✅ **B3 — scalar-subquery IMPLEMENTED (full port, not a guard).** TS
  pre-resolves a `scalar`-flagged correlated subquery to a literal at hydrate
  (`resolveSimpleScalarSubqueries`), ships the matched row as a companion, and
  runs a live companion pipeline that throws `ResetPipelinesSignal('scalar-
  subquery')` when the resolved value changes on advance (pipeline-driver.ts
  :353/543). Ported end-to-end, mirroring go-ivm:
  - **AST**: added `scalar: bool` to `CorrelatedSubqueryCondition` (+ napi/
    replay/server deserialization); the resolver now gates on `csq.scalar`,
    not the old arity heuristic (which wrongly resolved ordinary single-field
    EXISTS).
  - **Executor** (`engine::resolve_scalar_subqueries`): builds + fetches the
    subquery pipeline against live sources, returns value + `matched`, and
    retains the built pipeline as a live companion. Wired into
    `add_queries_streaming` (which previously never called resolve at all);
    companion rows are emitted as ADD `RowChange`s post-hydrate.
  - **Monitoring**: `CompanionOutput` recomputes the scalar on each push
    (ADD/EDIT→row[childField], REMOVE→undefined, CHILD→ignore) and raises
    `ScalarResetError` (via `panic_any`) when `scalar_values_equal` is false;
    else it streams the companion change under the owning query.
  - **Boundary**: the napi advance catch_unwind downcasts `ScalarResetError`
    and emits a `-2` reset row with reason `scalar-subquery` (transparent
    in-place reset, the twin of Go's `-32105`), NOT a teardown error.
  - **Tests**: `tests/scalar_subquery_test.rs` (resolve+companion, live reset
    with correct payload, no-match ALWAYS_FALSE, and non-scalar-EXISTS-is-not-
    reset for the gating fix). Full suite green.

## C. Error contract (raw error → teardown vs signal → in-place reset)

- ✅ **C1 — engine panic (source-drift asserts).** napi `EngineHandle::call`
  catch_unwinds each job; an advance panic is surfaced as a THROWN NapiError
  (Err), NOT a -2 reset row → `#advancePipelines` re-throws → view-syncer
  teardown → client reconnect. Matches TS raw-assert-throw semantics. Engine +
  snapshotter are restored before returning so the actor thread survives.
  (napi lib.rs:751-767). Verified earlier session; re-confirmed.
- ✅ **C2 — only `advance_result.reset_reason` maps to -2** (in-place reset).
  The two paths (throw vs signal) are kept distinct exactly as TS.

## D. CVR / recovery — no silent delta drops (go-ivm finding #4 class)

- ✅ **D1 — advancement-timeout does NOT commit partial changes.** The napi
  layer appends the -2 reset row at the END of the row array, so the driver
  yields the partial changes THEN throws `ResetPipelinesSignal`. Verified in
  `view-syncer.ts:2343-2352`: on a thrown ResetPipelinesSignal the view-syncer
  `await pokers.cancel()` + returns the signal **without flushing the updater**
  (flush is success-only, line 2352). Partial changes are discarded, the CVR is
  NOT committed at the new version, pipelines reset+rehydrate at curr. The
  scary path is transactionally safe — NOT a finding.
- ✅ **D2 — replicaVersion on abort.** Set to curr (engine reports the advanced
  version); the subsequent reset rehydrates at curr, consistent with the
  snapshotter's leapfrog. CVR still at old version → next poke is a full
  hydrate diff. Correct.

## E. Teardown / actor-thread + resource cleanup on CG drop

- 🔧 **E1 — engine.destroy() was a silent no-op.** rust-ivm-driver.destroy()
  called `this.#engine.destroy?.()` but the napi engine had no such method →
  engine graph + SQLite reader fds + snapshotter replica handles held until GC.
  Added `#[napi] destroy()` that runs `eng.destroy()` + resets EngineState on
  the actor thread → prompt release, matching TS PipelineDriver.destroy().
  **Commit 4451d3b.**
- ✅ **E2 — actor thread exit.** `EngineHandle::spawn` loop exits + calls
  `eng.destroy()` when the last `Sender` drops (JS object GC'd). Thread blocked
  on recv is cheap; no permanent leak. E1 handles the memory/fd urgency.
- ✅ **E3 — reset() rehydrate.** `reset()` clears engine pipelines + all
  EngineState source maps (napi lib.rs:586) before re-init; rehydrate rebuilds
  at head. Matches pipeline-driver reset.

## Open items
- B3 validation at the napi/differential layer: the engine-level tests pass,
  but extend the napi/TableSource differential fuzzer to generate `scalar`-
  flagged subqueries (SQLite-backed) so resolution + companion monitoring are
  exercised over the real addon, not just MemorySource.
- Validation: image rebuild + ART re-run to confirm A1/A2/E1 + B3 live
  (source-drift still 0; scalar resets classified as reset-not-teardown;
  CG-churn fd/mem flat).
