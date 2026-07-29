# DESIGN: Intra-CG Parallel Hydration + Lifecycle Hardening

Status: proposal. Goal: parallelize hydration *within* a single CG without
re-importing the Go-IVM deadlock/wedge/lifecycle bug class, and close the
lifecycle gaps that exist *today* regardless of parallelism.

---

## 0. The one invariant everything hangs on

**Single-writer engine graph.** The `Rc/RefCell` operator graph (`!Send`) is
owned and mutated by exactly one thread — the CG's actor thread
(`napi/src/lib.rs:54`, `EngineHandle::spawn`). Parallelism is **read-only work
over immutable `BEGIN CONCURRENT` snapshots**; workers hand back `Send` owned
data; the actor does all graph mutation serially.

Why this is the whole game: every Go deadlock/wedge came from *shared mutable
state + locks* (`writeMu`, `group.mu`, `hydrateReaders`). Rust's current safety
is *not* magic — it's this invariant, enforced at compile time by `!Send`. Keep
it and the bug class is impossible. Break it (`Arc<Mutex>` the graph) and you
have rebuilt Go. The `!Send` graph is the guardrail: a worker that tries to
touch the graph **will not compile**.

## 1. Non-negotiable safety rules (kill the class from the start)

- **S1 — No shared mutable engine state.** Workers never hold a reference to the
  graph. They receive `Send`-only task specs + their own `rusqlite::Connection`,
  and return owned `Vec<Row>`/serialized nodes.
- **S2 — Co-read only, never converge-to-head.** Workers read one pinned
  `stateVersion`; they never advance or mutate `curr`. (This is the exact rule
  the Go warm-hydrate co-read pool landed on.)
- **S3 — Bounded pool + bounded buffers.** Fixed worker count (≤ cores, config
  cap); results flow through a bounded channel with backpressure. Never
  unbounded (the Go mattn per-`Next` goroutine explosion, and the `.all()` OOM).
- **S4 — Deterministic teardown.** RAII on snapshots/connections; `catch_unwind`
  per worker; first-error-wins abort; any failure → one clean reset, no partial
  commit.
- **S5 — Interruptible.** Cancellation checked per-row AND enforced by a
  cross-thread SQLite interrupt (see §1a); per-job watchdog deadline. No
  uninterruptible operation.

### Interrupt primitive (the composability decision)

Use **`rusqlite::Connection::get_interrupt_handle() -> InterruptHandle`**
(rusqlite 0.32, not behind any feature flag). The handle is **`Send + Sync`**:
hold it on one thread, call `.interrupt()` from another to abort a query running
on that connection (returns `SQLITE_INTERRUPT`). This is deliberately chosen over
`progress_handler`, which is a *same-thread* cooperative callback — the
interrupt handle is the *cross-thread hard abort* that both the watchdog and the
parallel workers need, so the exact same plumbing serves the single actor
connection today and every pooled worker connection later.

Build these three seams NOW so Phase 0 is strictly additive to parallel hydrate
(no rework):

1. **Connection-generic setup** — a helper
   `install_interrupt(conn) -> InterruptHandle` run at connection open, used by
   the actor connection today and every pooled worker connection later. Never
   special-case "the actor's connection".
2. **Job-scoped cancel token** — already true: `CancellationToken` is per-engine
   and reset per hydrate/advance (`engine/mod.rs:390/556`). In parallel all
   workers of a job share it, so one cancel/timeout cascades to all. No change.
3. **Single monitor thread with a deadline registry** — not thread-per-job. It
   iterates a set of `(deadline, InterruptHandle[])`. For a serial job the set
   has one handle; for a parallel job it has the job's N worker handles. Same
   loop.

The only thing that would *hamper* parallel hydrate is the opposite mistake —
hard-wiring the interrupt/watchdog to a singleton connection. Seams 1–3 prevent
that.

## 2. Architecture

On a parallel-hydrate request the actor thread:

1. **Pins** a `BEGIN CONCURRENT` snapshot at the target `stateVersion` via the
   snapshotter (`src/snapshotter/snapshotter.rs` already holds these).
2. **Splits** work into independent read tasks:
   - *coarse*: one task per pipeline in the CG (share-nothing at the pipeline
     boundary), or
   - *fine*: one task per heavy correlated-EXISTS/child subquery of a pipeline
     (the N+1 fetches pprof pins as the cold-hydrate cost).
3. **Dispatches** to a bounded worker pool. Each worker owns a
   `rusqlite::Connection` (Connection is `!Sync` → one per thread) pinned to the
   snapshot's version, plus a `Send` task spec.
4. **Collects** owned results over a bounded channel and **merges** them into the
   graph single-threaded, then builds views.

Workers see only `Send` data + their own connection. They physically cannot
reach the graph — enforced by types, not discipline.

**Connection pool:** reuse validated connections (the wal2 validated-stateVersion
read pool). Before a worker reads, **assert its connection pins the wanted
version**; if the replica moved and the pin can't hold, that task fails →
serial fallback. This is the fix for the Go pin-race (the 43↔144s flicker was
the reader pool binding non-deterministically and silently reading the wrong /
serialized frame — *never read the wrong frame silently*).

## 3. Lifecycle guards to build in on day one

- **L1 — `SnapshotGuard` (RAII):** releases the `BEGIN CONCURRENT` read tx and
  returns the connection to the pool on drop, even on panic. Without it a
  panic leaks the connection and pins the WAL (blocks checkpoint → unbounded
  growth).
- **L2 — `WorkerScope`, first-error-wins:** shared abort `AtomicBool`; every
  worker checks it and runs under `catch_unwind`; on any worker error/panic set
  abort, drain, actor emits **one** reset. No partial results reach the graph.
- **L3 — Cancellation propagation:** pass the CG's `CancellationToken`
  (`engine/mod.rs:1083`, `Arc<AtomicBool>`) clone to each worker for cooperative
  between-rows checks, **and** register each worker connection's
  `InterruptHandle` (§1a) with the monitor so a long query aborts mid-flight via
  a cross-thread `.interrupt()`.
- **L4 — Per-job watchdog:** actor arms a deadline on dispatch (registers the
  job's interrupt handles with the single monitor, §1a seam 3); if the pool
  doesn't finish by it, the monitor flips the cancel token **and** `.interrupt()`s
  the handles; past a hard bound, abandon the workers, rebuild/poison their
  connections, emit an advancement-timeout-style reset. Closes the W3 "no bound
  for stuck-in-cgo" gap.
- **L5 — Backpressure:** workers block when the merge queue is full → bounded
  memory.
- **L6 — Version-pin validation:** see §2; fail-to-serial, never wrong-frame.

## 4. Fixes to land NOW (independent of parallel hydrate)

These harden the *current* single-threaded engine and are prerequisites for the
parallel path anyway:

- **N1 — Cross-thread SQLite interrupt** via `install_interrupt(conn)`
  (§1a seam 1) on the actor's connection(s); the cancel path calls
  `.interrupt()`. Closes the single open wedge class: today cancel is only
  checked *between rows* (`engine/mod.rs:454`), so one runaway SQLite query
  wedges the actor thread uninterruptibly. Built connection-generic so the
  worker pool reuses it verbatim.
- **N2 — Single monitor thread + deadline registry** (§1a seam 3) around
  `EngineHandle::call` (`napi/src/lib.rs`): on deadline it flips the cancel token
  and `.interrupt()`s the job's handles; past a hard bound surfaces a stuck-actor
  signal. Closes the no-watchdog gap and is the same monitor the parallel jobs
  register with.
- **N3 — (shared TS driver) reset/delta contract audit:** ensure every engine
  reset carries `ResetPipelinesSignal` so the driver never commits CVR past a
  never-delivered gap (the Go "recovery drops deltas" root cause). Engine-side is
  clean; this is driver work — flag, don't port.

## 5. Phasing

- **Phase 0 (now):** N1 + N2. Small, high-value, de-risks the wedge class before
  any parallelism.
- **Phase 1:** `SnapshotGuard` + validated read-pool + **coarse** per-pipeline
  parallel hydrate behind a flag (default off), co-read only. Guards L1–L6.
- **Phase 2:** **fine-grained** parallel correlated-EXISTS/child fetches.
- **Phase 3:** shadow-soak at CG scale + the differential below.

## 6. Validation strategy

- **Parallel ≡ serial (the strongest oracle):** parallel hydrate MUST produce
  **byte-identical** output to serial hydrate (read-only ⇒ result-preserving).
  Add a replay mode that runs both and diffs; wire it into the fixture oracle
  and the differential fuzzer (run each seed through serial *and* parallel
  replay, assert equal). This reuses everything already built.
- **Leak/contention soak:** N-CG × parallel hydrate; assert 0 connection leaks
  (SnapshotGuard), bounded WAL growth, pin-rate 100%-or-serial-fallback (never
  wrong-frame), heap/goroutine-equivalent Δ ≈ 0 across runs.
- **Interruptibility test:** issue a cancel mid-parallel-hydrate and a
  deliberately-slow query; assert both abort < deadline (validates N1/L3/L4).
- **`-race` / `loom`** on the worker↔merge channel boundary.

## 7. What this explicitly does NOT do

- Does **not** make the graph `Send`/shared. No `Arc<Mutex>` on operators.
- Does **not** let workers advance or mutate `curr`.
- Does **not** add an unbounded pool or unbounded buffers.

If a future change needs any of those three, stop — that is the moment the Go
bug class comes back, and it should be an explicit, reviewed decision, not a
drift.
