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

---

_Add an entry here (with the TS source, the Rust divergence, and the justification)
whenever a finding is resolved as "deliberate" rather than fixed._
