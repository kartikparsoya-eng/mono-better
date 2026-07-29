# Redirection Prompt: rust-ivm napi binding — service → value

**Hand this to the implementing agent. It rewrites only the napi binding layer, not the engine core.**

---

## Task

Rewrite the rust-ivm napi binding from a worker-thread **service** into a JS-thread-confined **value**. Keep the engine core untouched. Preserve inter-CG parallelism and the read-only intra-CG hydrate parallelism; drop only intra-CG parallel *advance* (which is a behavioral divergence and already forbidden).

## Context

- `rust-ivm/` is a single-threaded IVM engine (`Rc<RefCell<Engine>>`, `!Send`, NodeStream lazy fetch) that faithfully ports the TS Zero engine. `Row = Arc<FxHashMap<…>>` is `Send + Sync`.
- It's exposed to zero-cache via a napi-rs addon in `rust-ivm/napi/`. Mono integration is on branch `rust-ivm-v1.7.0` (worktree `mono-v1.7`).
- Validation is ART running the **complete Rust zero-cache image** against the differential G8 oracle (0 mismatches, 0 connect errors).

## The problem to fix

`rust-ivm/napi/src/lib.rs` currently:
- spawns a worker OS thread in `RustIvmEngine::new()` (`thread::spawn(worker_main)`),
- makes every method an `async fn` that sends a `Command` (`Init`/`Destroy`/`Reset`/`Shutdown`/`Cancel`) over an `mpsc` channel and awaits a `oneshot`,
- pulls in `tokio`, and streams via `StreamState` + `Notify` + `async next()`.

Despite being an in-process addon, this reproduces all four lifecycle breaks that cause the Go sidecar's bug class (out-of-thread, async, independent lifetime, cross-thread coordination). The engine being `!Send` was "solved" by exiling it to a worker thread — **that is the anti-pattern to remove.**

## Target architecture — the engine is a value, not a service

1. **Hold the engine directly on the JS thread.** `#[napi] pub struct RustIvmEngine { inner: RefCell<Engine> }` (or `Rc<RefCell<Engine>>`). **No worker thread. No `mpsc`/`oneshot`/`Command` enum. No tokio.** Delete `worker_main` and the whole command-dispatch layer.
2. **Methods are synchronous** `#[napi] fn` (not `async`). `init`, `add_queries`, `advance_to_head`, `remove_query` borrow `self.inner` and run inline on the JS thread, returning results directly. Mirrors how the TS ViewSyncer drives its engine synchronously under a lock. Blocking the worker thread during a long advance/hydrate is correct and desired — that thread serves one CG (or a small shard), so it blocks only its own work, exactly like TS under its lock.
3. **Streaming is a synchronous pull-iterator.** Replace `StreamState`/`Notify`/`ThreadsafeFunction`/`async next()` with a `#[napi]` iterator object holding a Rust `Iterator` (the existing NodeStream). Its `next()` is a **synchronous** `#[napi] fn` returning `Option<NapiRowChange>`. JS drains it in a loop — exactly how TS drains a generator. No worker, no notify, no await.
4. **Lifetime is JS-owned via `Drop`.** Implement `Drop for RustIvmEngine` to close the DB and free the engine. **Delete the `destroy()`/`Shutdown`/`Reset` RPC surface.** "Reset" = JS drops the object and constructs a new one. One JS ViewSyncer owns exactly one engine, 1:1, lifetime nested in the ViewSyncer. This eliminates the destroy↔re-init race family by construction — there is no shared, recycled, or independently-lifecycled engine to coordinate.
5. **Resolve `!Send` by thread-confinement, not exile.** A napi class instance is bound to the Environment (JS thread) that created it and is never shared across threads unless handed to a ThreadsafeFunction — which this design never does. If napi-rs's bounds require it, add a **narrow, documented `unsafe impl Send for RustIvmEngine {}`** with the invariant "only ever accessed from the JS thread that constructed it; never captured by a ThreadsafeFunction." Do **not** convert the engine to `Arc<Mutex<…>>` (adds the locking the single-threaded model exists to avoid). Do **not** reintroduce a worker thread.

## Parallelism model (preserve inter-CG + read-only intra-CG hydrate)

This is the part the blunt "no threading" rule got wrong. The value model keeps every axis of parallelism that is actually allowed:

6a. **Inter-CG parallelism — the primary axis, fully preserved and cleaner.** Parallelism across client groups = multiple Node **worker threads**, each with its own JS Environment, each owning **one** single-threaded `RustIvmEngine` confined to that thread. N workers → N engines truly parallel on N cores, zero shared mutable state, zero cross-CG locks. This is the existing zero-cache syncer-sharding model. **Rust upside: no shared GC** — unlike Go (whose 16-CG scaling was GC/allocation-bound, 2.6×→4.9× only via GOGC tuning), each Rust engine allocates/drops in its own thread, so inter-CG scaling can exceed Go's ceiling. Remaining ceiling is the shared SQLite replica file + cores (identical for both, below the engine).

6b. **Intra-CG parallel ADVANCE — do NOT implement. Keep advance serial.** `Rc<RefCell>` is `!Send` so you can't `par_iter` pipelines sharing the engine — but this is not a loss: parallel advance is a behavioral divergence (shadow-only in Go, prod bakes `GO_IVM_PARALLEL_ADVANCE=false`), because the differential oracle requires deterministic emission order and TS advances serially. Serial advance is the faithful, oracle-safe design.

6c. **Intra-CG parallel HYDRATE — allowed and encouraged for the whale case, via self-contained lanes.** Cold hydrate is read-only over a frozen replica snapshot, so it can be parallelized **without touching the shared live engine**. Structure each lane self-contained:
   - own SQLite reader connection,
   - own **thread-local** `Rc` operator graph built from `Send` inputs (the query AST),
   - producing `Send` outputs (`Row = Arc<FxHashMap>` is `Send + Sync`),
   - gathered back on the JS thread and installed into the engine **serially**.

   The value invariant holds because the parallel phase is a **pure read that feeds the engine, never concurrent mutation of it**; the `Rc`-ness stays confined inside each lane. This is Go's reader-pool / co-read pattern re-expressed. Use a scoped mechanism (rayon scope / scoped threads) so no `Rc` or engine reference escapes a lane. **Gate it on low concurrency**: enable when active CGs < cores (one whale CG cold-hydrating alone); disable under high CG concurrency where inter-CG parallelism already saturates cores (spawning lanes there just oversubscribes). Advance path stays single-threaded regardless.

   Constraint: sources/AST/config consumed by a lane must be reachable as `Send` inputs or rebuilt per-lane. If a source is shared via `Rc`, either re-create it per-lane from `Send` config or snapshot the needed read-only data into a `Send` form before spawning. **Never** share the live `Rc<RefCell>` engine graph across lanes.

## Keep untouched

The entire `rust-ivm/src/ivm/`, `snapshotter/`, `sqlite/`, `builder/` engine core. This is a binding-layer rewrite only. `src/bin/server.rs` (tiny_http) stays as an ART/test harness but is **not** the production transport — the napi addon is the only production path.

## Validation gates (all must pass)

- **ART green** (0 mismatches, 0 connect errors) against the complete Rust zero-cache image with the new synchronous napi engine.
- **Lifecycle stress test**: rapid construct → init → advance → drop → re-construct for the same client group, in a tight JS loop. Must pass **without any epoch/generation/restart-gate/stale-response machinery** on either the Rust or mono side. If you need an epoch guard, the lifetime model is still wrong — the engine must be a value whose validity is guaranteed by its own existence.
- **Inter-CG scaling test**: M worker threads each with their own engine, concurrent load; confirm near-linear scaling up to core count (bounded only by the shared SQLite file), with no cross-engine locks.
- **Intra-CG hydrate parallelism test**: one large CG cold-hydrating with lanes on; confirm speedup vs serial AND identical materialized result (oracle green) — proving the parallel read produces the same rows as serial.
- No `tokio`, no spawned long-lived worker thread, no `mpsc`/`oneshot` in `rust-ivm/napi/src/lib.rs`. (Scoped hydrate lanes that join before returning are fine — they are not a persistent service thread.)

## The principle to hold throughout

In TS the engine is a `this.#engine` field — synchronous, single-threaded, same-lifetime, owned. Every deviation from that is a lifecycle bug waiting to happen. napi-rs lets Rust be a value in exactly this sense; the current binding threw that away. Put it back — **without** giving up the parallelism that is actually allowed: inter-CG (workers) and read-only intra-CG hydrate (self-contained lanes). Only intra-CG parallel *advance* is dropped, and it was never shippable under match-TS.

---

## Note on scope

This supersedes the "Phase 2 (napi in-process)" step of the earlier full implementation prompt — that phase was implemented as a service; this is the correction. Other phases (Source-trait extraction, Rc-leak Weak+destroy fix, wire streaming) are unaffected. A mono-side (`rust-ivm-v1.7.0`) counterpart should follow: the ViewSyncer holds the napi engine as a plain owned field with **no** `#restartGate`/`initEpoch`/`#withReinitRetry` — that machinery only exists because Go's engine is a service. The mono integration for a value engine is *simpler* than the Go one, not more complex.
