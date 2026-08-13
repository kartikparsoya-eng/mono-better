# 91 — Sharded async executor model for the Rust syncer

**Status:** ✅ Implemented and validated on the ART capacity ladder (see §5.1, §6).
**Author:** rust-syncer capacity work
**Supersedes the execution model in:** [89-full-rust-syncer.md](./89-full-rust-syncer.md) (thread-per-client-group)
**Scope:** the *execution harness* of `rust-syncer` (`router.rs` CG scheduling + the `block_on` I/O edges) **and one dispatcher change** — `numSyncWorkers=1` for the rust path (§3, `server/main.ts`). The `SyncEngine`, CVR store/updater, and `rust-ivm` engine are **out of scope** — they are preserved unchanged.

> **TL;DR of the validated design (read this first).** One rust-syncer process (`numSyncWorkers=1`) hosts `K ≈ cores` single-threaded async executors, each hosting a hash-shard of client groups and their `!Send` `SyncEngine`s. There is **one shared CVR `PgPool`** on the process's main multi-thread runtime; executors **offload** every CVR I/O future onto that runtime (`SyncEngine::offload` → `Handle::spawn`), so the pool's connections are always polled by the reactor that owns them (no cross-runtime starvation) *and* the whole `cvr_max_conns` budget is one shared pool, not fragmented per executor. Result: the capacity cliff moved **10 → past 50** (blessed baseline), 50-conn steady p95 **67,710 ms → 119 ms**, `errors=0`.
>
> **Note on §3/§5.1 below:** the original proposal called for a *per-executor* pool (§3, bullet "CVR pool per executor"). That was implemented first and measured — it works but *fragments* a small per-worker budget and stalls under load (§5.1). The shipped design replaces it with the shared-pool + spawn-offload above. §5.1 records the full journey.

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
- **CVR pool (see §5.1 for the full journey — this bullet is the *superseded* proposal).** *Originally proposed:* each executor owns its own `PgPool` sized `maxConns/K`. *Shipped instead:* **one shared `PgPool`** on the process's main multi-thread runtime, with each executor **offloading** its CVR I/O futures onto that runtime (`SyncEngine::offload` → `Handle::spawn(fut).await`). The offload runs the I/O on the runtime that owns the connections (so no cross-runtime starvation, §5.1-A) while keeping the whole budget in one pool that any CG on any executor can draw from (so no per-executor fragmentation, §5.1-B). Requires `numSyncWorkers=1` for the rust path so the single process holds the whole budget over one replica.
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

## 5.1. The validated journey: three measured iterations to a passing cliff

The design was proven by iterating on the ART capacity ladder. Three distinct execution shells were built and measured; the third ships.

### Iteration A — async CG loop, one thread per CG, shared main-runtime pool (REVERTED)

Phase 1 alone (`current_thread` runtime per CG, still one thread per CG, awaiting the shared main-runtime `PgPool` directly) **regressed** capacity: p95 at 10 conns `529 ms → 44,546 ms`, cliff `10 → 0`. Logs:

```
sqlx::pool::acquire: acquired connection, but time to acquire exceeded slow threshold — 4–9 s
config_and_hydrate failed: store flush: pool timed out while waiting for an open connection
```

**Root cause — cross-runtime I/O.** The `PgPool` lives on the main multi-thread runtime; its TCP connections' readiness is driven by that runtime's reactor. A CG on its **own** `current_thread` runtime that `.await`s `pool.acquire()` polls connections whose readiness its reactor never receives (tokio "resource driven across runtimes"), so acquires stall and time out. The pre-Phase-1 `block_on` path was fast precisely because `handle.block_on(fut)` drove the future on the pool's own runtime. **A current-thread executor cannot directly await a pool owned by a different runtime.**

### Iteration B — K executors, each with its OWN pool sized `maxConns/K` (works, but fragments)

This is what §3 originally prescribed: give each executor a pool built on its own runtime, so it awaits only resources it owns. Measured: cliff `10 → 50` for the blessed *standalone* 50-conn run (p95 **67,710 ms → 251 ms**), `50/50` opened, `errors=0`. **The executor model itself works.**

But two problems surfaced under the *full G22 sweep* (10→25→50→100→200 against one warm container):

1. **Budget fragmentation.** Each executor's pool needs ≥1 connection, so `K` executors need ≥`K` connections. With `K = cores` and a small per-worker budget, `maxConns/K` rounds down to 1–2 connections per executor. Under sustained load a *hot* executor (several active CGs) queues on its 1–2 connections for **2–4 s** (`slow threshold` warnings ×891) while *idle* executors' connections sit unused — the shared budget is stranded. Sweep-50 p95 was **19,758 ms** (fails) even though standalone-50 was 251 ms.
2. **Process × executor thread blow-up.** The dispatcher spawns `numSyncWorkers` (= cores) rust-syncer processes, each of which then spawned `K = cores` executors → `cores²` threads (112 on a 14-core box) and split the CVR budget into per-process slices (7) too small to subdivide.

### Iteration C — one syncer, K executors, ONE shared pool, spawn-offload (SHIPS)

Two changes fix both problems:

- **`numSyncWorkers = 1` for the rust path** (`server/main.ts`). The Rust syncer scales *within* a process, so running one — handed the whole CVR/upstream budget, over one replica — removes the `cores²` thread blow-up and the per-process budget fragmentation. (TS syncers keep the multi-process model.)
- **One shared CVR `PgPool` + spawn-offload** (`SyncEngine::offload`). The pool stays on the main multi-thread runtime; each executor, instead of awaiting the pool directly (Iteration A's bug) or owning a slice of it (Iteration B's fragmentation), **spawns each CVR I/O future onto the pool's runtime via `Handle::spawn` and awaits the `JoinHandle`**. The I/O future is polled by the runtime that owns the connections (fixing §5.1-A cross-runtime starvation), the executor is freed to run its other CGs while it waits, and — crucially — *any* of the pool's `cvr_max_conns` connections can serve *any* CG on *any* executor (fixing §5.1-B fragmentation). This is exactly TS's one-shared-`cvrDB`-pool-per-worker behavior, now with native async compute fanned across `K` cores.

  `JoinHandle` is awaited across runtimes safely (its waker fires regardless of which runtime awaits it). The offloaded futures touch only `Send` state — `Arc<Mutex<CVRStoreHandle>>` and a cloned `RowRecordCache` — never the `!Send` IVM engine, so nothing `!Send` crosses a thread.

**Measured (spawn-offload, full G22 sweep, single 14-core box):**

| conns | steady p95 | opened / errors | verdict |
|------:|-----------:|-----------------|---------|
| 10 | 123 ms | 10/10 / 0 | ✅ |
| 25 | 75 ms | 25/25 / 0 | ✅ |
| **50** | **119 ms** | **50/50 / 0** | ✅ **blessed baseline** |
| 100 | 70,177 ms | 100/100 / 0 | cliff (budget-bound) |
| 200 | 57,837 ms | 200/200 / 9,650 | breakdown |

Zero `slow threshold` warnings at ≤50 conns. The 100/200 degradation is the genuine 30-shared-connection budget ceiling (100+ CGs ≫ 30 conns) — TS hits the same wall; its blessed capacity target is **50**, which this passes with **42× p95 headroom** (119 ms vs 5,000 ms). Raising the cliff past 100 is a connection-budget/`cvr_max_conns` tuning question, not an execution-model one.

## 5. The one hazard: intra-shard head-of-line blocking

Because a shard multiplexes many CGs on one thread, a large CPU-bound hydrate on CG-A stalls the other CGs sharing that executor until it yields.

**TS has the identical hazard and already solves it** by chunking hydrate/advance and yielding between chunks — this is exactly what the ART `yield-during-hydrate` / `yield-during-advance` gates verify. Our streaming `RowChange` path is already chunked; the fix is to make the chunk boundaries `.await`/yield so a big hydrate cooperatively lets the shard's other CGs run. We mirror TS's chunk cadence (`hydrateChunkSize` / `advanceChunkSize`) so behavior and fairness match the baseline.

Residual risk is bounded: worst case a shard behaves like *one* TS event loop, which is the current TS baseline — never worse than TS, and `K×` better in aggregate.

---

## 6. Migration plan (refactor the harness, not the engine)

The tested core — `SyncEngine`, CVR store/updater, `rust-ivm` — does not change. Only the execution shell changes, in three measurable phases, each validated against the ART capacity ladder.

1. **Phase 1 — de-`block_on` the CG loop.** ✅ Convert `run_cg_thread`'s blocking loop to a `current_thread` runtime + `LocalSet` with async message receive; replace every `handle.block_on(...)` with `.await`. Async foundation; kept at `353ff2488` / branch `phase1-async-cg-loop`. Not shippable alone (§5.1-A).
2. **Phase 2 — shard CGs onto K executors.** ✅ Replace one-thread-per-CG (`get_or_create_cg` spawn) with a fixed pool of `K` executor threads; route a CG to `hash % K`; host its `!Send` engine + async task there. This is the compute-parallelism win.
3. **Pooling — one shared pool + spawn-offload (replaces "per-executor pool").** ✅ The `maxConns/K`-per-executor pool of the original plan fragments a small budget (§5.1-B); shipped a single shared pool on the main runtime with `SyncEngine::offload` moving CVR I/O onto it. Requires `numSyncWorkers=1` for the rust path (`server/main.ts`).
4. **Phase 3 — cooperative yield** at streaming hydrate/advance chunk boundaries so intra-shard head-of-line is bounded to TS's cadence. *Not yet needed:* the blessed 50-conn sweep passes at 119 ms p95 without it; revisit only if a hot shard shows up in a future profile.

### Success metric — ✅ ACHIEVED

Capacity ladder cliff ≥ 50 conns (blessed TS baseline) with p95 < 5,000 ms at 50, `errors=0`, `failed_open=0`. **Measured: cliff past 50; sweep-50 steady p95 = 119 ms; 10/25/50 all pass; `errors=0`, `failed_open=0`.** G6 leak slope + the remaining release matrix to be confirmed via `run-rust-syncer-release.sh --mode release`.

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
