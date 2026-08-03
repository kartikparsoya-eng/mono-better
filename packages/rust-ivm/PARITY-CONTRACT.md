# Rust IVM Driver Parity Contract

## Authority and Scope

`PipelineDriver` is the executable correctness specification until the Rust
driver becomes authoritative. This contract covers behavior observable through
the public driver interface and the row-change streams consumed by view-syncer.
Performance may differ. A Rust behavior is not accepted merely because it is
already implemented or faster.

The driver-level oracle is
`packages/zero-cache/src/services/view-syncer/driver-parity-trace.ts`. It runs
the same operation sequence against both public drivers and records inputs,
results, errors, versions, reset signals, query metadata, row-set signatures,
and stream events. Object property order is canonicalized because it is not a
data-model guarantee. Array and stream order, missing properties, `undefined`,
`null`, JS scalar types, bigint, non-finite numbers, negative zero, and bytes
are never normalized.

## Classification

- **Must match exactly**: values and state must have the same canonical trace.
- **Semantically equivalent**: the precise value may differ, but the invariant
  below must hold and receives a dedicated assertion.
- **Intentional divergence**: reviewed behavior required by a Rust-specific
  constraint. It must be documented and tested on both sides.

## Observable Operations

| Surface                                              | Classification                           | Required behavior                                                                                                                                                                                                               |
| ---------------------------------------------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `init()` / `initialized()`                           | Must match exactly                       | Same success or error, initialized state, replica version, current version, schema validation, and repeat-init behavior.                                                                                                        |
| `reset()`                                            | Must match exactly                       | Same schema validation and resulting empty query/signature state; subsequent hydration observes the same snapshot and rows.                                                                                                     |
| `addQuery()`                                         | Must match exactly                       | Same ordered row-change stream, including yield sentinels, transformed AST metadata, transformation hash/name, and registration state. Failed or abandoned hydration leaves no query or signature.                           |
| duplicate query ID / re-add                          | Must match exactly                       | Existing query is removed first; the new query replaces its metadata, signature, rows, and hydration-time contribution.                                                                                                         |
| `removeQuery()`                                      | Must match exactly                       | Same no-op behavior for an unknown ID and complete removal of query metadata, signature, and hydration-time contribution.                                                                                                       |
| hydration rows                                       | Must match exactly                       | Same event order, change type, query ID, table, row key, row fields, missing fields, and JS value types.                                                                                                                        |
| `advance()` header                                   | Must match exactly                       | Same `version` and `numChanges`; current version changes at the same observable point.                                                                                                                                          |
| advance changes                                      | Must match exactly                       | Same ordered stream and add/edit/remove semantics, including rows produced before a reset/error.                                                                                                                                |
| `getRow()`                                           | Must match exactly                       | Same projected synced columns, missing-row result, row key interpretation, errors, and `fromSQLiteTypes` values.                                                                                                                |
| `rowSetSignature()`                                  | Must match exactly                       | Same bigint after every emitted change and the same `undefined` state for absent signatures. `undefined` must not be coerced to `0n`.                                                                                           |
| `queries()`                                          | Must match exactly                       | Same query IDs in insertion order and same public `QueryInfo` fields. Private pipeline objects are not part of the contract.                                                                                                    |
| schema drift / truncate / permissions / scalar reset | Must match exactly                       | Same reset-versus-error classification, reason, message, partial-stream behavior, and cleared/rebuilt state.                                                                                                                    |
| cancellation / early stream return                   | Must match exactly for outcome and state | Same cancellation/error/reset category and no partial registration or later stale output. Exact completion time is not compared.                                                                                                |
| `destroy()`                                          | Must match exactly for outcome and state | No queued callback may mutate state after destruction; repeated teardown follows the TS contract. Exact completion time is not compared.                                                                                        |
| error propagation                                    | Must match exactly                       | Prepare, bind, iteration, row conversion, and existence-check failures retain error name/class, code, reason, and message. No failure becomes an empty result or `false`.                                                       |
| `totalHydrationTimeMs()`                             | Semantically equivalent                  | Sum only successful active-query hydration durations; replace on re-add; delete on remove; clear on reset. Values are finite, nonnegative, and positive zero for an empty set. Wall-clock values need not be numerically equal. |
| cooperative scheduling latency                       | Semantically equivalent                  | Yield/callback scheduling may take different wall time, but recorded `yield` positions remain part of the exact stream trace while the API exposes them.                                                                        |
| planner choice                                       | Semantically equivalent                  | Plans and internal tree shapes may differ; public rows, ordering, changes, versions, state, and errors may not.                                                                                                                 |
| stale/invalid snapshot diff                         | Must match exactly                       | Propagate `InvalidDiffError` like TS. Do not convert it into a Rust-only reset reason or silently retry against a different snapshot.                                                                                         |

## No Accepted Waivers

The previous start-cursor whitelist and `undefined -> 0n` signature
normalization are not part of this contract. Cursor boundaries, ordering,
aliases, replica-versus-client primary keys, and signature absence are public
behavior and must be compared. A corpus mismatch remains unresolved until the
Rust implementation matches TS or this document records a reviewed product
decision with a two-driver regression.

## Required Scenario Trace

Every deterministic and fuzz-generated scenario should record:

1. Initial state after initialization.
2. Each operation and its complete input.
3. Ordered output events or structured error/reset.
4. State after the operation: versions, public query info, and signatures.
5. `getRow()` observations for affected keys where applicable.
6. Final state after remove/reset/destroy.

Persist minimized differential failures as checked-in regression fixtures. Do
not compare raw Rust engine trees with TS caught-node trees when the production
contract is `RowChange`; both sides must pass through their public drivers.

## Release Gates

- All known parity findings fixed with regressions.
- Zero unexplained deterministic or fuzz-corpus trace differences.
- Boundary matrix and fault/lifecycle suites pass through the public drivers.
- Slow-consumer tests demonstrate bounded queue and memory.
- Generated NAPI types contain no `any` at the driver boundary.
- Production shadowing is isolated from CVR state and retains every mismatch as
  a replayable fixture.
- Multi-day zero-difference shadow run, then canary with immediate TS fallback.
