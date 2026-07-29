# Phase 2 — True streaming across the napi boundary (Rust IVM)

## Problem
Both hot paths eager-materialize the entire RowChange stream into one
`Vec<NapiRowChange>` and return it as a single JS array:
- hydrate: `add_queries_streaming(..., |rc| rows.push(...))` (napi lib.rs:655)
- advance: `advance_to_head_stream(..., |rc| rows.push(...))` (napi lib.rs:700-727)

For a whale query (e.g. ticketsQueryV2 ≈ 13 K rows) that means holding all
13 K NapiRowChange structs — each with a `HashMap<String,NapiValue>` row — in
Rust memory AND then a second full copy as a JS array, before the TS driver
processes the first row. Peak memory ∝ result size; GC spikes; the actor
thread is blocked building the whole Vec before JS sees anything.

TS's native PipelineDriver is a generator (`Stream<RowChange>`) — the
view-syncer's `for await` pulls one change at a time with backpressure. Our
driver already consumes an *iterable* (`#advanceToHeadRows` is a generator),
so the TS side is streaming-ready; only the Rust→JS boundary is eager.

## Goal
Stream RowChanges across the boundary **ROW BY ROW, exactly like TS** — one
RowChange at a time, backpressured, O(1) rows in flight (not O(result), not
O(window)). TS's view-syncer does `for await (const change of changes)` over a
`Stream<RowChange>`; we mirror that: one row crosses the boundary, is fully
processed by the CVR updater, then the next row is produced. Keep the actor
model. No behavior change to the emitted sequence (differential stays green).
Ship DARK behind a flag; validate whale hydration with flat memory first.

## Mechanism (napi-rs v2, napi8 → ThreadsafeFunction available)
The engine ALREADY emits per-row via a callback:
`advance_to_head_stream(..., |rc| rows.push(...))` (lib.rs:727) and
`add_queries_streaming(..., |rc| rows.push(...))` (lib.rs:655). Row-by-row
streaming is a change to *only that callback* — the engine, snapshotter,
overlay, and reset code are untouched.

1. Add `advanceToHeadStreamingRows(onRow: (row) => Promise<void>)` and
   `addQueriesStreamingRows(onRow)`. `onRow` is wrapped as a
   `ThreadsafeFunction<NapiRowChange, Blocking>`.
2. The per-row callback becomes `tsfn.call(rc, Blocking)` instead of
   `rows.push(rc)` — each RowChange is handed to JS the instant it is produced.
   Blocking mode parks the actor thread until the JS callback resolves, so at
   most ONE row is in flight (O(1)) — the faithful TS row-by-row semantic, not
   a buffered window.
3. Header row (changeType=-1) is emitted FIRST (before any change rows); the -2
   reset row (if any) is emitted LAST — same ordering the driver relies on, so
   the Phase-3 D1 partial-commit / reset-throw invariant is preserved verbatim.
4. No final flush needed — there is no buffer.

### Backpressure correctness
- Blocking TSFN per row = the actor thread cannot produce row N+1 until JS has
  finished row N (which awaits `#processChanges` → the CVR updater). True
  end-to-end, one-row backpressure — identical to TS pulling the generator.
- The cancel token (lib.rs:510) is checked between rows (the engine loop
  already does per-change abort checks), so advancement-timeout/abort stops
  promptly and emits the -2 row as the last emission (D1 preserved).

### Cost note (accepted tradeoff)
Row-by-row = one FFI crossing per RowChange (TS's generator is in-process, no
FFI). For a 13 K-row whale that is 13 K TSFN calls. This is the fidelity +
memory cost the user chose over chunking. If per-row FFI overhead measures too
high in the soak, revisit — but do NOT pre-optimize into chunks; ship row-by-
row first and measure.

## TS driver changes (rust-ivm-driver.ts)
- `advance()` becomes: kick off the chunked call; expose `changes` as an async
  generator that yields rows as chunks arrive (a bounded queue fed by
  `onChunk`, drained by the generator). The header (row 0 of chunk 0) is still
  consumed by `advance()` before returning; rest flow through
  `#advanceToHeadRows` unchanged (it already handles -2 → ResetPipelinesSignal
  and the `% 100` yield).
- `#advanceToHeadRows` already an iterator → minimal change (accept
  AsyncIterable; view-syncer.ts:2312 already casts to
  `Iterable | AsyncIterable`).

## Rollout (flagged, dark-first)
- `RUST_IVM_STREAM_ROWS` flag, default OFF → keep the eager array path byte-
  identical. Flag ON routes through the row-by-row TSFN API.
- Phase 0 (safe, self-contained): land the row-by-row napi methods + TS async-
  gen consumption behind the OFF flag, with a unit test that the streamed row
  sequence is identical to the eager path for N seeds. Ships without changing
  production behavior.
- Phase 1: flip the flag in a shadow-soak; gate = whale-query (13 K-row)
  hydration with flat RSS (O(window)) + differential still green + no new
  resets.

## Validation gates
1. Differential parity: run `verify-adv-seeds.mjs` + hydrate differential with
   the flag ON — must be byte-identical to OFF.
2. Memory: hydrate ticketsQueryV2-scale under both flags; RSS delta under ON
   must be ~flat vs result size (the whole point).
3. Advance reset paths: force advancement-timeout + schema-change with flag ON;
   confirm D1 (partial discarded, in-place reset) still holds.
4. Full cargo suite + sustained advance fuzzer, flag ON.

## Risk notes
- ThreadsafeFunction blocking-mode + the single-threaded actor: the actor
  thread parks in `tsfn.call`, so no other job runs on that engine during a
  stream — same as today (the whole advance already occupies the actor). Inter-
  CG parallelism is unaffected (separate actor threads per engine).
- Do NOT start emitting chunks before the header/version is known, or the
  driver can't set `#replicaVersion` — keep header as chunk-0 row-0.
- This is a boundary-only change: the IVM engine, snapshotter, overlay, and
  reset semantics are untouched. Keep it that way (Phase-3 invariants hold).

## Status
NOT STARTED — design only. Net-new for Rust (the Go duplex-streaming design is
a separate codebase). Sequenced AFTER the Phase-3 fixes (overlay a7771b5 /
1d0862e, destroy 4451d3b) which are the correctness floor this builds on.
