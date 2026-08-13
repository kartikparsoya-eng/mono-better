# 91 — Sharded async executor model for the Rust syncer

**Status:** Proposed (design review)
**Author:** rust-syncer capacity work
**Supersedes the execution model in:** [89-full-rust-syncer.md](./89-full-rust-syncer.md) (thread-per-client-group)
**Scope:** the *execution harness* of `rust-syncer` (`router.rs` CG scheduling + the `block_on` I/O edges). The `SyncEngine`, CVR store/updater, and `rust-ivm` engine are **out of scope** — they are preserved unchanged.

---

## 1. Goal (why we moved to Rust at all)

The original mandate for the Rust syncer was two things:

1. **Increase throughput** of the read path (hydrate / advance / poke).
2. **Remove TypeScript event-loop contention** — in TS, one Node event loop per sync worker runs *all* CPU-bound sync work (IVM diff, poke building, JSON encode) *and* all I/O, so one heavy client group stalls every other client group on that worker. TS scales only by running more *processes* (`numSyncWorkers`), each duplicating the replica, snapshotter, and memory.

This document argues that the **current** Rust execution model does not meet either goal, explains why, and proposes the model that does.

---

## 2. Where we are today, and the evidence it doesn't work

Today `router.rs` runs **one dedicated OS thread per client group** (`run_cg_thread`, spawned from `get_or_create_cg`). Each CG thread:

- owns its `!Send` `SyncEngine` (correct — the engine cannot move between threads),
- runs a blocking `crossbeam` receive loop,
- performs **all CVR Postgres I/O via `handle.block_on(...)`** on that thread (load, flush), plus JWKS verification and custom-query HTTP.

### The measured failure (ART capacity ladder, 4-CPU container)

After the shared-CVR-pool fix (which removed connection *exhaustion*):

| conns | p95 | opened / failed_open / errors |
|------:|----:|-------------------------------|
| 10 | 529 ms ✅ | 10 / 0 / 0 |
| 25 | 8,860 ms | 25 / 0 / 0 |
| 50 | 67,710 ms | 50 / 0 / 0 |
| 100 | 42,044 ms | 100 / 0 / 0 |

`errors=0` and `failed_open=0` throughout — nothing *fails*, but p95 latency explodes under concurrency. Capacity cliff = 10 conns; the blessed TS baseline is 50.

### Root cause

The current model conflates two unrelated concerns:

- **Compute** — the `!Send` IVM engine (hydrate/advance diff, poke building). Genuinely must live on a thread; *should* run in parallel across cores. This is the part worth keeping.
- **I/O** — CVR Postgres load/flush, JWKS, custom-query HTTP. `Send`, async, and should **never occupy a compute thread**.

By using `block_on` for I/O, a CG thread is **frozen** for the duration of every Postgres round-trip: it processes no further messages for that group and cannot even send pokes. And by spawning **one thread per client group**, N active CGs become N threads competing for K cores. At 50 CGs on 4 cores that is ~12:1 oversubscription; every `block_on` parks a thread that must then be *rescheduled* to wake on I/O completion, producing a scheduler-latency storm.

**We did not remove contention — we relocated event-loop contention into scheduler contention, which is worse.** Neither goal is met.

---

## 3. The design: K single-threaded async executors, CGs hash-sharded

> Run a **bounded pool of `K ≈ num_cores` single-threaded async executors**. Each executor hosts a **hash-shard of client groups** and their `!Send` engines. **All I/O is `.await`ed, never `block_on`'d.** CPU-bound IVM compute runs inline on the executor thread but is spread across `K` cores.

### Mechanics

- **Executors.** Spawn `K` OS threads at startup (default `K = available_cores`, configurable via e.g. `ZERO_SYNCER_SHARDS`). Each thread runs a **`tokio::runtime::Builder::new_current_thread()`** runtime driven with a **`LocalSet`**. A current-thread runtime can drive `!Send` futures (`spawn_local`), so the IVM engine lives there without ever crossing threads.
- **Placement.** A client group is assigned to a shard by `shard = hash(client_group_id) % K`, computed once. The assignment is stable for the CG's lifetime, so its `!Send` `SyncEngine` is **pinned to exactly one executor thread** — the `!Send` invariant is upheld by construction.
- **Multiplexing.** Within a shard, many CGs run as concurrent async tasks (or a single `select!` loop over their inbound channels). CVR flush/load, JWKS, and custom-query HTTP are `.await`ed. While CG-A awaits Postgres, the executor runs CG-B, CG-C — no frozen threads, no wasted core.
- **Compute.** IVM hydrate/advance runs inline on the shard thread (mandatory — `!Send`). With `K` shards on `K` cores this is **true multi-core compute parallelism**.
- **Notifications / fan-out.** A replica commit notification is dispatched to each executor once; the executor advances the CGs it owns. The WS accept loop routes an incoming connection to `hash(cg) % K`.

### Why the `!Send` constraint is fully satisfied

The engine is created on its shard thread and never leaves it. Nothing `.await`s across a thread boundary with engine state held. `sqlx::PgPool` is `Send + Sync` and is awaited fine on a `LocalSet`. This is the standard "single-threaded runtime hosting `!Send` state" pattern.

---

## 4. Why this meets the goal (and beats both TS and the current port)

| Dimension | TS | Current Rust | **Sharded executors** |
|---|---|---|---|
| Compute lanes | P processes × 1 loop | N CG threads (thrash) | **K executors = K cores** |
| I/O model | async (non-blocking) | `block_on` (freezes thread) | **async (non-blocking)** |
| Threads under load | few (P) | **N** (oversubscribed) | **K** (never oversubscribed) |
| Replica / snapshotter / memory | duplicated × P | 1 shared | **1 shared** |
| IVM speed | JS | native | **native** |
| Intra-lane fairness | cooperative yield | thread frozen | **cooperative yield** |

- **Throughput.** `K` native compute lanes over *one* shared replica/snapshotter/memory ≈ TS running `K` worker processes, but with no per-process memory duplication, no IPC, and native IVM instead of JS. This is the concrete throughput win.
- **Contention.** Within a lane it is TS's cooperative-async model (no `block_on` freeze); across lanes it is real parallelism; thread count is pinned to cores, so no scheduler thrash. This removes *both* TS's event-loop contention *and* the current port's scheduler contention rather than trading one for the other.

---

## 5. The one hazard: intra-shard head-of-line blocking

Because a shard multiplexes many CGs on one thread, a large CPU-bound hydrate on CG-A stalls the other CGs sharing that executor until it yields.

**TS has the identical hazard and already solves it** by chunking hydrate/advance and yielding between chunks — this is exactly what the ART `yield-during-hydrate` / `yield-during-advance` gates verify. Our streaming `RowChange` path is already chunked; the fix is to make the chunk boundaries `.await`/yield so a big hydrate cooperatively lets the shard's other CGs run. We mirror TS's chunk cadence (`hydrateChunkSize` / `advanceChunkSize`) so behavior and fairness match the baseline.

Residual risk is bounded: worst case a shard behaves like *one* TS event loop, which is the current TS baseline — never worse than TS, and `K×` better in aggregate.

---

## 6. Migration plan (refactor the harness, not the engine)

The tested core — `SyncEngine`, CVR store/updater, `rust-ivm` — does not change. Only the execution shell changes, in three measurable phases, each validated against the ART capacity ladder.

1. **Phase 1 — de-`block_on` the CG loop.** Convert `run_cg_thread`'s blocking `crossbeam` loop to a `current_thread` runtime + `LocalSet` with async message receive; replace every `handle.block_on(...)` with `.await`. (Prerequisite; also independently removes the "frozen thread during I/O" symptom.) *Expected:* modest p95 improvement; still one thread per CG.
2. **Phase 2 — shard CGs onto K executors.** Replace one-thread-per-CG (`get_or_create_cg` spawn) with a fixed pool of `K` executor threads; route a CG to `hash % K`; host its engine + async task there. *Expected:* the capacity cliff moves from 10 toward 50+ as oversubscription disappears — this is the phase that delivers the throughput.
3. **Phase 3 — cooperative yield** at streaming hydrate/advance chunk boundaries so intra-shard head-of-line is bounded to TS's cadence.

### Success metric

Capacity ladder cliff ≥ 50 conns (matches the blessed TS baseline) with p95 < 5,000 ms at 50, `errors=0`, `failed_open=0`, and G6 leak slope flat — validated by `run-rust-syncer-release.sh --mode release`.

---

## 7. Alternatives considered

- **Just enlarge the CVR pool / add CPUs.** Treats a symptom. More CPUs help thread-per-CG only until CG count exceeds cores again; the `block_on` freeze and scheduler thrash remain. Rejected as a primary fix.
- **Make the IVM engine `Send` and use tokio's multi-threaded work-stealing runtime with a `Mutex` per CG.** Would allow per-CG async tasks on a shared pool. But making the engine `Send` is a deep change (SQLite handles, `Rc`/`Weak` planner graph per the review) and a per-CG `Mutex` reintroduces serialization plus lock contention. High risk, unclear win. Rejected.
- **Cap the number of CG threads with a queue (bounded thread pool, thread reused across CGs).** A `!Send` engine cannot be reused across CGs on a shared thread without re-pinning; effectively this *is* the sharded-executor model once you pin CGs to threads. This design is that, done deliberately.

---

## 8. Open questions

- **`K` default and tuning.** `num_cores` vs `num_cores − 1` (leave a core for the accept loop / tokio I/O reaper / HTTP). Needs measurement.
- **Shard rebalancing.** Static `hash % K` can hot-spot if a few CGs dominate. Start static (matches TS's dispatcher hashing); revisit only if a shard hot-spots in the ART profile.
- **CVR pool sizing per shard.** One shared pool of `maxConns` across `K` shards vs a sub-pool per shard. Start shared (already implemented); measure acquire contention.
- **Accept loop / HTTP notify.** Keep on the main tokio runtime; only the CG execution moves to the `K` executors.

---

## 9. Recommendation

Adopt the sharded single-thread-async-executor model. It is the only shape that fulfills the original mandate — *more throughput than TS while removing contention rather than relocating it* — and it is a bounded, well-scoped refactor of the execution harness that leaves the risky, already-tested IVM/CVR core untouched. Proceed Phase 1 → 2 → 3, proving each step on the capacity ladder.
