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
- **CVR pool per executor (critical — see §5.1).** Each executor owns **its own `PgPool`, created on and driven by that executor's runtime**, sized `maxConns / K`. `K` pools × `maxConns/K` keeps the total connection budget bounded (the P0 fix) while ensuring every pool's connections are driven by the same runtime that `.await`s them. A single global pool created on a *different* runtime does **not** work with current-thread executors (proven below). This matches TS, where each sync-worker process has its own `cvrDB` pool.
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

## 5.1. Validated finding: current-thread executors REQUIRE per-executor pools

Phase 1 (async CG loop, still one thread per CG) was implemented and measured on the ART capacity ladder against commit `353ff2488`. It **regressed** capacity hard: p95 at 10 conns went `529 ms → 44,546 ms`, and at 100 conns `100/100 opened → 0/100 opened` (cliff `10 → 0`). Container logs showed the cause directly:

```
sqlx::pool::acquire: acquired connection, but time to acquire exceeded slow threshold — 4–9 s
config_and_hydrate failed: store flush: pool timed out while waiting for an open connection
```

**Root cause — cross-runtime I/O.** The shared `PgPool` was created on the main multi-thread runtime; its TCP connections' readiness is driven by that runtime's reactor. When a CG on its **own** `current_thread` runtime `.await`s `pool.acquire()`/a query, it polls connections whose readiness the CG's reactor never receives — a tokio "resource driven across runtimes" antipattern — so acquires stall for seconds and time out. The pre-Phase-1 `block_on` path was fast *precisely because* `handle.block_on(fut)` drove the future on the pool's own (main) runtime.

**Consequences, now baked into the design:**
- A current-thread executor must own **all** the I/O resources it awaits. → **per-executor pool** (§3), created on that executor's runtime.
- **Phase 1 in isolation is a dead end** and was reverted from the branch. A `current_thread`-per-CG runtime cannot use a shared main-runtime pool (cross-runtime starvation) and cannot use per-CG pools (reinstates the P0 exhaustion, `N × maxConns`). Only the bounded `K`-executor model with `K` pools of `maxConns/K` resolves both.
- Corollary: Phase 1 and Phase 2 are **not separable**. The async loop only pays off, and only becomes *correct*, once it runs on a bounded executor that owns its pool.

## 5. The one hazard: intra-shard head-of-line blocking

Because a shard multiplexes many CGs on one thread, a large CPU-bound hydrate on CG-A stalls the other CGs sharing that executor until it yields.

**TS has the identical hazard and already solves it** by chunking hydrate/advance and yielding between chunks — this is exactly what the ART `yield-during-hydrate` / `yield-during-advance` gates verify. Our streaming `RowChange` path is already chunked; the fix is to make the chunk boundaries `.await`/yield so a big hydrate cooperatively lets the shard's other CGs run. We mirror TS's chunk cadence (`hydrateChunkSize` / `advanceChunkSize`) so behavior and fairness match the baseline.

Residual risk is bounded: worst case a shard behaves like *one* TS event loop, which is the current TS baseline — never worse than TS, and `K×` better in aggregate.

---

## 6. Migration plan (refactor the harness, not the engine)

The tested core — `SyncEngine`, CVR store/updater, `rust-ivm` — does not change. Only the execution shell changes, in three measurable phases, each validated against the ART capacity ladder.

1. **Phase 1 — de-`block_on` the CG loop.** Convert `run_cg_thread`'s blocking `crossbeam` loop to a `current_thread` runtime + `LocalSet` with async message receive; replace every `handle.block_on(...)` with `.await`. **Done and measured — REVERTED.** In isolation (one thread per CG, shared main-runtime pool) it *regressed* the ladder (cliff `10 → 0`) via cross-runtime pool starvation (§5.1). Kept in history at `353ff2488` / branch `phase1-async-cg-loop` as the async foundation; **must not ship without Phase 2 + per-executor pool.**
2. **Phase 2 — shard CGs onto K executors, each owning its pool.** Replace one-thread-per-CG (`get_or_create_cg` spawn) with a fixed pool of `K` executor threads; route a CG to `hash % K`; host its engine + async task there; **create one `PgPool` per executor on that executor's runtime, sized `maxConns/K`** (§3, §5.1). This is the phase that delivers the throughput *and* makes the async loop correct. *Expected:* the cliff moves from 10 toward 50+ as oversubscription and cross-runtime starvation both disappear. **Do Phase 1's async conversion and Phase 2 together** — they are inseparable.
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
