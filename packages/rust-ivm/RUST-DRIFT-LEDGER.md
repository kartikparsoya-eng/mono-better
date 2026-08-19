# Rust-IVM Drift Ledger

This file records intentional Rust-IVM behavior that differs from the TypeScript
reference engine. Entries should either become the shared spec later, or be
removed when Rust and TS are aligned.

## R1: Source Trait Abstraction

Status: Intentional Rust divergence (structural).

Rust introduces a `Source` trait (`src/ivm/source.rs`) that abstracts over
`MemorySource` and `TableSource`. TS has no equivalent trait — it uses
duck-typing / structural interfaces. The `BuilderDelegate::get_source` returns
`Shared<dyn Source>` in Rust vs `Source | undefined` in TS.

Reason: Rust requires explicit trait objects for dynamic dispatch. The trait
matches TS's `Source` interface semantics exactly.

Required follow-up: none. The trait is a structural mapping, not a semantic
divergence.

## R2: Rc-Cycle Break via destroy() (not Weak)

Status: Intentional Rust divergence (mechanism).

Rust breaks the operator-graph Rc cycle by clearing `Connection.output` in
`destroy()` (strong Rc, cleared on teardown). TS relies on GC to collect cycles.

An alternative (Weak back-edges) was attempted and reverted: it required every
operator to hold a strong ref to its OutputAdapter, which was too invasive for
the mechanical-fidelity stance. The destroy-clear approach is minimal and
matches the existing cascade-destroy pattern.

Reason: Rc cycles don't self-collect in Rust. The destroy-clear is called
on every pipeline teardown (removeQuery, reset, destroy), so the cycle is
broken before the Rc strong count drops to 0.

Required follow-up: none. The 1000-iteration leak test verifies no leak.

## R3: Chunked Wire Streaming (StreamSink + Chunker)

Status: Intentional Rust addition (no TS equivalent).

Rust introduces `StreamSink` trait + `Chunker` for transport-agnostic streaming
output. TS streams via async generators (`AsyncIterable<RowChange | 'yield'>`).
The Rust abstraction batches RowChanges into bounded `StreamFrame`s (Partial /
Final / Done / Error) with monotonic `chunkIndex`.

The HTTP server exposes this as NDJSON (`/add-queries-stream`, `/advance-stream`).
The napi addon uses the pull-based `NapiStreamIterator` (existing).

Reason: Rust has no built-in async generators. The `FnMut(&RowChange)` callback
in `add_queries_streaming` / `advance_streaming` is the engine's streaming
interface; the Chunker wraps it for wire-level framing.

Required follow-up: none. The chunk invariants are unit-tested.

## R4: TableSource SQLite Error Handling

Status: Intentional Rust hardening.

Rust's `TableSource::write_change` returns `Result<(), rusqlite::Error>` and
logs errors instead of panicking. `check_exists` returns `false` on prepare
failure. `query_map` failures return an empty stream. TS uses try-catch with
similar graceful degradation.

The previous Rust implementation had 15+ `unwrap()` calls that would panic on
SQLite errors. These are now error-handled.

Reason: A production server must not crash on a transient SQLite error (locked
DB, disk full, etc.). The error is logged and the operation degrades gracefully.

Required follow-up: none.

## R5: Planner scanstatus cost model — cosmetic probe-SQL differences

Status: Intentional Rust divergence (cosmetic only; decisions identical).

The default planner cost model (`sqlite/sqlite_cost_model.rs`) is the faithful
port of TS `createSQLiteCostModel` (scanstatus EST + stat4/stat1 fanout,
filter-inlined probe SQL, boolean-constraint `= 0` quirk included). Two
non-semantic differences from the TS-built probe:

1. SELECT-list column ORDER: TS uses zqlSpec insertion order; Rust sorts
   column names for determinism. The SELECT list does not affect the query
   plan or `SQLITE_SCANSTAT_EST`.
2. Constraint-column ORDER in WHERE: TS uses `Object.entries` insertion order
   of the merged constraint; Rust's `PlannerConstraint` is a `BTreeMap`
   (sorted). `a = ? AND b = ?` vs `b = ? AND a = ?` plan identically.

Also: when the engine has a snapshotter but no initialized sources
(harness-only path, e.g. rust-ivm-driver.planner.test.ts's bare engine), table
specs fall back to `pragma_table_info` with string-typed columns; TS `must()`
would throw there. Production always initializes sources, using the same
zqlSpec column set as TS.

Decision parity is asserted by rust-ivm-planner-parity.test.ts (PLANNER_PARITY=1)
across 9 AST shapes at multiple scales. Escape hatch: the legacy filter-blind
COUNT(*) model remains behind `RUST_IVM_PLANNER_COST_MODEL=count`.

Required follow-up: none.

## R?: LIKE/ILIKE type-mismatch handling (from six-front review, 2026-08-19)

Status: Intentional Rust divergence (benign; reachable only via a mistyped query).

TS `getLikePredicate` (`zql/src/builder/like.ts`) calls `assertString(lhs)`,
which THROWS if the filtered column value isn't a string, and coerces a
non-string pattern via `String(pattern)`. Rust (`builder/like.rs`):

1. A non-string **lhs** value returns `false` (row excluded) rather than
   throwing. This is only reachable when a `LIKE`/`ILIKE` predicate is applied
   to a non-text column value — a mistyped query. `return false` is strictly
   safer than TS's throw (which would fail the whole query on one bad row), and
   on a well-typed schema (LIKE only on TEXT columns) neither path fires. We
   deliberately keep the safer behavior rather than introduce a per-row throw.
2. A non-string **pattern** panics ("LIKE pattern must be a string"). Patterns
   are string literals in every real query; a non-string pattern is a malformed
   AST. The panic is caught by the per-CG `catch_unwind` (fail-op), consistent
   with the other malformed-input panics in the conversion layer.

Also: wildcard ILIKE case-folds via `to_lowercase()` + `regex_lite` rather than
a native Unicode `i`-flag RegExp. Agrees with TS/SQLite for ASCII and common
Unicode; a handful of code points (final sigma ς/σ, Kelvin K U+212A, Turkish
İ/ı) can fold differently ONLY when combined with wildcards. No known real-world
schema depends on this; revisit with a Unicode-folding regex if a mismatch is
ever observed.

Required follow-up: none (behavior is safe and only affects malformed queries).

## R?: `as_query` trait-object cast (from six-front review, 2026-08-19)

Status: Documented safety invariant (sound today).

`builder/query_internals.rs::as_query` casts `&dyn QueryInternals` to `&Query`
via a raw pointer. This is sound because **`Query` is the SOLE implementor of
`QueryInternals`** (it's `Query`'s own internals trait, mirroring TS `asQuery`).
The invariant to preserve: never implement `QueryInternals` for any other type.
If a second implementor is ever needed, replace the cast with an `Any`-downcast
or fold the trait into an enum before doing so. Not converted now because the
trait has multiple methods and the cast is a hot-path no-op; the invariant is
narrow and easy to hold.

Required follow-up: none while `Query` is the only `QueryInternals` impl.
