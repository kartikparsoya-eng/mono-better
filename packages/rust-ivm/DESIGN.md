# Rust-IVM Design Document

## Overview

Rust-IVM is a Rust reimplementation of Zero's IVM query engine. It mirrors the
TypeScript reference engine's semantics exactly while choosing Rust-idiomatic
mechanisms (ownership, RAII, trait objects).

## Architecture Decisions

### 1. Source Trait (Phase 0)

A `Source` trait abstracts over `MemorySource` (in-memory/test) and `TableSource`
(SQLite/production), matching TS's `Source` interface. The builder, engine, and
all delegates use `Shared<dyn Source>` (= `Rc<RefCell<dyn Source>>`), enabling
the engine to run real SQLite queries via `TableSource` without code changes.

### 2. Rc-Cycle Leak Prevention (Phase 0.5)

The operator graph uses `Rc<RefCell<T>>` for shared ownership (matching TS's GC'd
references). Cycles form: SourceInput → Connection.output → Operator → SourceInput.

Fix: `SourceInput::destroy()` and `TableSourceInput::destroy()` clear the
`Connection.output` back-edge. All operators cascade `destroy()` to children, so
calling `destroy()` on the pipeline root breaks all cycles. Verified with a
1000-iteration add/remove leak test.

### 3. Wire Streaming (Phase 1)

A `StreamSink` trait + `Chunker` provide transport-agnostic streaming:
- **StreamFrame** enum: `Partial` (batch of RowChanges), `Final` (per-query end),
  `Done` (terminal), `Error`.
- Monotonic `chunk_index` across all frames.
- Bounded frame size (configurable `chunk_size`).
- Query-switch detection: flushing + Final when the query ID changes.

Both transports feed this abstraction:
- **HTTP**: `/add-queries-stream` and `/advance-stream` endpoints return NDJSON
  (one frame per line, `Content-Type: application/x-ndjson`).
- **napi-rs**: the `StreamState` queue + `NapiStreamIterator` deliver frames
  to the JS event loop via pull-based `next()`.

### 4. Per-CG Threading Model (Phase 2–3)

Each client-group (CG) engine runs on a dedicated OS thread (the napi worker
thread). All RPCs for a CG run one at a time via an mpsc channel. This respects
`Rc<RefCell>` (each engine stays single-threaded) while enabling cross-CG
parallelism (multiple CGs on different threads).

### 5. Cancellation (Phase 4)

`CancellationToken` (`Arc<AtomicBool>`) + SQLite `progress_handler` +
`interrupt()`. Economic-abort budget via the progress handler (opcode/CPU gas
meter → "advancement-timeout" reset). RAII Drop cleans up connections — no
manual free/UAF handling (unlike Go's `atomic.Uintptr` + `unsafe.Pointer`).

## Value / Coercion Fidelity

- `Value` mirrors TS's union: `Null | Bool | F64(f64) | Str(Arc<str>) | Json(Arc<str>)`.
- Integers → f64 (JS Number), with MAX_SAFE_INTEGER guard.
- `FromSQLiteType` matches TS `table-source.ts`: boolean = JS truthiness, number
  bigint → bounds-checked f64, json = JSON.parse-or-error.
- Fail-closed AST: operator whitelist + reject unknown condition types
  (DRIFT-LEDGER D2/D3 aligned).

## Do NOT Port (Go scars)

- mattn per-Next goroutine tax — rusqlite is sync.
- cancel-flag `atomic.Uintptr` / `unsafe.Pointer` UAF — use CancellationToken + RAII.
- GC tuning knobs (GOGC/GOMEMLIMIT) — no GC.
- C step-rows batching shim — rusqlite iterates efficiently.
- libgoivm C ABI + addon.c two-layer bridge — napi-rs replaces both.

## Phase 2 — napi-rs In-Process Transport

The napi addon (`napi/src/lib.rs`) provides in-process engine access:
- `RustIvmEngine` class: constructor spawns a dedicated worker thread
- `init(tables, dbPath, appId)`: registers sources + opens SQLite
- `addQueriesStreaming(queries)`: returns `NapiStreamIterator` (pull-based)
- `advanceToHeadStreaming()`: returns `NapiStreamIterator` (pull-based)
- `cancel()`: cancels in-progress advance/hydrate
- `removeQuery(queryId)`, `destroy()`, `reset()`, `ping()`

Backpressure: credit-based bounded queue (1024 items). Producer parks when
credits exhausted; consumer returns credits on each `next()` call.

The TS driver (`rust-ivm-driver.ts`) loads `rust-ivm/napi/rust-ivm.node` via
`createRequire` and calls it in-process behind `USE_RUST_IVM=true`.

## Phase 3 — Cross-CG Parallel Hydrate

Each `RustIvmEngine` = one worker thread = one client group. Multiple CGs
run on separate threads, giving cross-CG parallelism. Measured 3.78x speedup
with 4 CGs (vs sequential) on the cross-CG benchmark.

## Phase 4 — Cancellation

- `CancellationToken` (`Arc<AtomicBool>`) on the `Engine`, checked in the
  advance loop and hydrate loop.
- `RustIvmEngine.cancel()` sends a `Cancel` command to the worker, which calls
  `eng.cancel()`.
- `NapiStreamIterator.cancel()` sets `StreamState.cancelled`, which stops
  `push()` from accepting new rows (cooperative cancellation).
- Economic abort: `AdvanceContext::should_abort()` budget check (existing).
- RAII: connections cleaned up by `Drop` — no manual free/UAF handling.

## Build & Run

### Dev (HTTP server)

```bash
cd rust-ivm
cargo build --release --bin rust-ivm-server
PORT=8080 ./target/release/rust-ivm-server
```

### napi addon

```bash
cd rust-ivm/napi
cargo build --release
# The .node file is at target/release/librust_ivm_napi.so (Linux) or .dylib (macOS)
```

### Docker (dev image)

```bash
docker build -t rust-ivm-server .
docker run -p 8080:8080 rust-ivm-server
```

### Tests

```bash
cargo test -- --test-threads=1  # 455 tests
```
