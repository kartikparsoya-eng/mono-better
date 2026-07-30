# Read-level parallel hydrate (supersedes DESIGN-parallel-hydrate.md)

## Why the old design was removed

`DESIGN-parallel-hydrate.md` describes **coarse per-pipeline parallelism**: one
throwaway worker pipeline per query, fetched on a worker thread for output, then
the actor re-fetches its own pipelines serially to warm advance state (Phase 2b).

That design is **architecturally incapable of beating serial**, and it was
default-ON:

- The actor's operator state (join/take/exists caches) is `!Send` `Rc<RefCell>`
  and cannot move off the worker thread, so after the workers stream output the
  actor **must** re-fetch every pipeline serially to warm state. That serial
  warming pass costs ≈ a full serial hydrate. The parallel worker fetch is
  therefore **pure additive work** → wall = serial + parallel overhead.
- Workers opened **fresh, unpinned** SQLite connections (`WorkerDelegate`),
  bypassing the snapshot version guarantee → the Go pin-race, reintroduced.
- Worker connections were never interrupt-registered → uncancellable (N1 gap).

**Phase 1 (DONE, verified)** removed it. Hydrate is now a single fetch on the
actor's pinned snapshot connection (`Engine::add_queries_streaming`): one fetch
per pipeline warms operator state AND emits output in the same pass. This fixed
P1 (unpinned conns), P2 (double fetch) and N1 (uncancellable conns) at the root.
Verified: full suite green + 3283 differential fuzz seeds, 0 findings.

The parallelism is re-introduced **below the source, at the SQL-read level**, so
the `!Send` graph stays single-writer and is fetched exactly once.

## Invariant

The engine graph (`Rc<RefCell>` operators) is owned by the actor thread and is
**never** touched by a worker. Parallel work is pure `Send` SQL: a worker gets a
`&rusqlite::Connection` (pinned) and returns `Send` rows. Node construction and
all graph mutation happen back on the actor thread.

## Phase 2a — parallel read primitive (DONE, tested)

`ReadPool::parallel_read(target_version, workers, interrupt_handles, tasks)`
(`src/snapshotter/read_pool.rs`):

- Each worker thread acquires ONE `SnapshotGuard` pinned+validated to
  `target_version` and runs a slice of the `Send` tasks on that one connection —
  every read in the batch observes the SAME frame, across all workers.
- **Serial fallback, never mix frames:** if ANY worker cannot pin exactly
  `target_version` (pool exhausted / open failure / head advanced past target),
  `parallel_read` returns `Err` and the caller reads serially on the actor's own
  pinned connection.
- Results returned strictly in input order → byte-identical to serial.
- Each worker connection is interrupt-registered (N1) so cancel()/watchdog can
  hard-abort a slow read on any of them.

Tests: in-order results, wrong-version→Err, task-error-wins, empty, no leaks.

## Phase 2b — co-pin the pool at the snapshot frame (THE integration, not yet wired)

**Critical constraint (wal2 / BEGIN CONCURRENT).** Each `Snapshot` pins ONE
connection with an open `BEGIN CONCURRENT` read tx at a specific frame
(`snapshotter.rs`). Under active replication, **head advances past that frame**,
but the snapshot holds it. A pool connection opened *lazily at fetch time* would
`BEGIN` at **head** (newer) → `parallel_read`'s version check fails → serial
fallback every time. Safe but useless. `sqlite3_snapshot_open` is dead on wal2
(see memory `project_wal2_blocks_snapshot_pool`), so we cannot re-open an old
frame after the fact.

**Therefore the pool MUST be co-pinned with the snapshot, at the moment its frame
is head:**

- The snapshotter re-pins `curr` to head only when `PipelineCount == 0` (cold
  hydrate; "curr draggable to head" rule). At that instant, `head == curr
  version`. Open the pool's N connections THEN and `BEGIN CONCURRENT` each at the
  same frame. Now all pool connections + the actor's snapshot share the frame.
- Own the pool on the `Snapshot` (or the snapshotter), lifetime-tied to the
  current frame; drop/re-pin it on the next re-pin. Ephemeral, cold-hydrate only.
- **Warm hydrate** (existing pipelines pin an older frame): fresh pool
  connections can't reach that frame → `parallel_read` Errs → serial fallback.
  This matches the "incremental adds stay serial" rule. Correct by construction.

Wiring:
- Add `Source::set_read_pool(Arc<ReadPool>)` (default no-op; `TableSource`
  stores it and passes it to `TableSourceInput` on `connect`).
- Snapshotter exposes the co-pinned pool + `current_version()`; the engine sets
  it on every source at cold-hydrate pin time.
- `parallel_read` is called with `target_version = current_version()`.

## Phase 2c — parallelize the reads that matter

**2c-1 — TableSourceInput multi-constraint fan-out.** When a fetch carries
`multi_constraints` (a batch of independent key-reads) above a threshold, split
them across the pinned pool via `parallel_read` and merge-sort, instead of one
serial connection. Directly speeds FlippedJoin / OR-subquery hydration. Lower
risk (the `multi_constraints` path already exists and is exercised).

**2c-2 — Join hydrate batching (the common EXISTS N+1).** `Join::fetch` today
maps each parent to a lazy `child_stream` doing a single-constraint child fetch
→ N serial reads driven by `Exists`/the Streamer. In the **hydrate** path only
(`inprogress_child_change == None`), buffer parent rows, gather their child-key
constraints, issue ONE `child_input.fetch` with `multi_constraints = all` (which
fans out in parallel via 2c-1), build a `key → Vec<child_row>` index, and serve
each parent's `child_stream` from the index. Keep the lazy per-parent path for
push/advance (inprogress change present) and for `MemorySource`.
- Buffering parents changes streaming *latency* but not output *order* → still
  byte-identical (the fuzzer's oracle). Overlay branches in the closures are
  inert during hydrate (`inprogress` is None), which is what makes this safe.
- Nested joins compose: a `Join` receiving `multi_constraints` forwards them to
  its parent source (already does, `join.rs:295`), so batching applies per level.

## Verification gate (must pass before default-ON)

1. `cargo test` (full suite) green.
2. Differential fuzzer with the read-parallel path ON — **byte-identical** to the
   TS oracle over ≥50k seeds (hydrate + advance).
3. A cold-hydrate microbench showing parallel < serial wall time (else the
   feature is off by default — no repeat of the old default-ON regression).
4. Soak: `ReadPool::live_count()==0` after each hydrate (no connection/WAL-pin
   leak), goroutine/heap Δ 0.

## Status

- Phase 1: DONE + verified (suite + 3283 fuzz seeds).
- Phase 2a: DONE + tested (primitive).
- Phase 2b/2c: specified above; the pool co-pin integration (2b) is the
  prerequisite and must land before 2c gives any real (non-serial-fallback)
  parallelism.
