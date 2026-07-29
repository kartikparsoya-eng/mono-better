# ResetPipelinesSignal Recovery Paths (Extraction #5)

`ResetPipelinesSignal` (snapshotter.ts:265) is thrown during diff iteration /
advance to force a full rebuild. It carries a `reason: ResetPipelinesReason`
(snapshotter.ts:258). All refs are `mono-v1.7`, `zero/v1.7.0`.

## The 5 reasons — trigger, message, source

| Reason | Triggered when | Message | Site |
|---|---|---|---|
| `schema-change` | changelog row `op === RESET_OP` — a table's schema changed | `schema for table {t} has changed` | snapshotter.ts:446 |
| `truncation` | changelog row `op === TRUNCATE_OP` | `table {t} has been truncated` | snapshotter.ts:453 |
| `permissions-change` | permissions table row's `permissions` column differs prev→next | `Permissions have changed …` | snapshotter.ts:515 |
| `scalar-subquery` | a resolved scalar-subquery value changed during change processing | `Scalar subquery value changed for {table}: {old} -> {new}` | pipeline-driver.ts:567 |
| `advancement-timeout` | advance is too slow (see algorithm) | `Advancement exceeded timeout at {pos} of {n} changes after {ms} ms…` | pipeline-driver.ts:861 |

## Recovery (uniform across reasons)
All five recover the **same way**: the signal aborts the current advance and the
caller **re-hydrates all pipelines at the current SQLite state** ("Truncates /
schema changes are processed by rehydrating pipelines at current" —
snapshotter.ts comments; view-syncer.ts:2299-2310 catches the signal and
re-initializes). Sources/connections are torn down and rebuilt from `current`.
The `reason` drives **logging/metrics and breaker classification**, not a
different recovery mechanism — but losing a *trigger* means the reset never
fires → stale results.

## The advancement-timeout algorithm (port verbatim) — pipeline-driver.ts:855
```ts
const MIN_ADVANCEMENT_TIME_LIMIT_MS = 50;               // :131
const elapsed = advanceTimer.totalElapsed();
if (
  elapsed > MIN_ADVANCEMENT_TIME_LIMIT_MS &&
  (elapsed > totalHydrationTimeMs ||
   (elapsed > totalHydrationTimeMs / 2 && pos <= numChanges / 2))
) {
  throw new ResetPipelinesSignal(/* advancement-timeout */);
}
```
Read: always allow ≥ 50 ms. Then abort if **either** (a) advance already took
longer than the *total original hydration time*, **or** (b) it's past *half* the
hydration-time budget while still in the *first half* of the changes (i.e. on
pace to blow the budget). `totalHydrationTimeMs` = Σ per-pipeline
`hydrationTimeMs` (pipeline-driver.ts:345).

## Rust port coverage — corrected

The plan says "the Rust port … doesn't differentiate reasons." **It partially
does.** `rust-ivm/src/snapshotter/`:
- `ResetPipelinesSignal { reason: &'static str, msg }` (snapshotter.rs:345).
- Emits **3 of 5** from the diff: `schema-change` (diff.rs:296),
  `truncation` (diff.rs:303), `permissions-change` (diff.rs:386).

Gap / where the other two live:
- **`advancement-timeout`** — correctly *not* in the Rust engine: it's a timing
  concern of the driver. The TS driver wrapper (`rust-ivm-driver.ts`) tracks
  `totalHydrationTimeMs` (:305) and must apply the algorithm above around the
  Rust advance RPC. **Verify the wrapper actually enforces it** (the constant
  `MIN_ADVANCEMENT_TIME_LIMIT_MS=50` and the two-branch check must be mirrored
  driver-side).
- **`scalar-subquery`** — the Rust side *resolves* scalar subqueries and tracks
  companion values (`sqlite/resolve_scalar_subqueries.rs`), but no
  `REASON_SCALAR_SUBQUERY` constant is emitted. **Action:** confirm the Rust
  advance path detects a resolved-scalar-value change and emits a
  `scalar-subquery` reset; if not, that reset silently never fires → stale
  results after a scalar-subquery value change (the exact failure mode the plan
  warns about). This is the one genuine gap.

## Reset-signal wire path (napi)
The Rust engine signals a reset by emitting a row with `changeType === -2`
carrying `rowKey.reason` and `rowKey.msg`; `rust-ivm-driver.ts:434` re-throws it
as a `ResetPipelinesSignal`. So any reason the Rust engine wants to raise must be
written into that reset row's `reason` field.

## Action items
1. Add `REASON_SCALAR_SUBQUERY` and emit it from the Rust advance path on a
   resolved-scalar value change (highest-value gap).
2. Confirm the driver enforces the advancement-timeout algorithm (constant +
   both branches) around the Rust advance.
3. Keep the reason string flowing through the `changeType=-2` reset row so
   breaker classification (see tiered-reset-breaker work) stays reason-aware.
