# Rust IVM streaming boundary

## Production contract

Hydration and advance use only the row-streaming NAPI methods:

- `addQueriesStreamingRows`
- `advanceToHeadStreamingRows`

The TypeScript driver exposes those callbacks as `AsyncIterable<RowChange>` so
the view-syncer consumes them with the same `for await` contract as
`PipelineDriver`. The old eager driver path and its rollout flag were removed.
Native array-returning methods remain test helpers and are not reachable from
the production driver.

## Ordering

Advance emits:

1. one `changeType=-1` header containing `version` and `numChanges`;
2. zero or more data changes in engine order;
3. an optional terminal `changeType=-2` reset signal;
4. an internal `changeType=-3` drain sentinel, which is never exposed as a
   `RowChange`.

The driver waits for the header before returning from `advance()`. The changes
stream preserves data order and converts a reset signal to
`ResetPipelinesSignal`, matching the TypeScript pipeline lifecycle.

## Backpressure and cancellation

Each stream has a monotonically increasing ID and a `StreamCreditGate`. The
producer acquires one credit before every data-row callback; the TypeScript
consumer grants one credit as it removes that row from its queue. Grants for a
stale stream ID are ignored. The gate closes on success, cancellation, panic,
or guard drop.

`RUST_IVM_TSFN_QUEUE` bounds the NAPI callback queue.
`RUST_IVM_STREAM_CREDIT` bounds total producer run-ahead. The production image
sets both to 64. `cancel()` and `cancelStream(streamID)` are out-of-band so they
can unpark the actor even while its normal job queue is occupied.

The native task sends a drain sentinel and waits until JS executes its callback
before resolving. This prevents the Promise continuation from closing the
TypeScript queue ahead of the final data row.

## Failure rules

- SQLite, conversion, and engine failures reject the operation; they are never
  converted to an empty stream.
- A failed or abandoned hydration removes the native pipeline and does not add
  public query metadata.
- A consumer that abandons a stream cancels the engine, closes the exact stream
  generation, and waits for the native task to settle before issuing another
  synchronous actor call.
- A post-header reset is thrown from the changes iterable so the view-syncer
  discards partial output using the same reset path as `PipelineDriver`.

## Required validation

- exact stateful differential corpus against `PipelineDriver`;
- slow-consumer bound and early-abandonment tests;
- SQLite failure and reset propagation tests;
- repeated teardown/cancel soak;
- production ART liveness, determinism, and Rust-vs-TS mutation parity.
