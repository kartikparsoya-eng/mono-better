# 88 — Unified Rust Architecture: Eliminating the Napi Boundary in the Hot Path

**Status:** Design document. Supersedes the phased napi approach in docs 80-86
for the hot path. The existing `rust-cvr` crate's **core logic** (updater state
machine, store buffer, client handler body assembly, row record cache) is
reusable as-is. What changes is **where the core logic runs** and **how it's
wired to the engine**.

## Problem statement

Docs 80-86 prescribe: "hydrate → advance → diff → body assembly → socket frame
→ flush — everything runs on the OS thread." "Extends the handle's surface — it
doesn't create a second channel. One CG = one Rust thread = engine + cvr + poke
handler all sharing it."

What was built instead: a **separate** `rust-cvr` napi crate with its own napi
channel. Both hydration and advance cross the napi boundary **4 times** for row
data:

```
Rust IVM engine → TSFN → TS AsyncQueue → TS #processChanges
  → updater.received() → napi → Rust CVR updater → patches back → napi → TS
  → pokers.addPatch() → napi → Rust ClientHandler → WS push
```

## What's reusable (no changes needed)

The following Rust core logic is correct and reusable. Only its **wiring**
changes:

| Component | File | LOC | What it does | Reusable? |
|---|---|---|---|---|
| `CVRUpdater` | `updater.rs` | ~1000 | State machine: trackQueries, received, deleteUnreferencedRows, version math, refCounts merge | ✅ Core logic yes. Remove `store_ops` buffer, call store directly. |
| `CVRStoreHandle` | `store.rs` | ~800 | PendingWrites buffer, apply_store_ops, flush to PG | ✅ Core logic yes. Sync buffer (std::sync::Mutex), async flush. |
| `ClientHandler` | `client_handler.rs` | ~880 | Poke chain, body assembly, flushBody, LMIDs, mutations | ✅ Core logic yes. Currently uses tokio::Mutex — needs to work on actor thread. |
| `PokeHandler` | `client_handler.rs` | (in same file) | Per-poke state, addPatch, end, cancel | ✅ Same as above. |
| `RowRecordCache` | `row_record_cache.rs` | ~900 | LRU cache, write-back, catchup cursor | ✅ No changes — stays on tokio runtime (async PG reads). |
| `RowSetSignature` | `row_set_signature.rs` | ~100 | xxHash32 signature | ✅ No changes. |
| Types | `types.rs` | ~250 | StoreOp, Patch, RowPatch, QueryPatch, CVR, etc. | ✅ No changes. |
| TTL | `ttl.rs` | ~120 | TTL clamp/compare | ✅ No changes. |

## What changes

### 1. `rust-ivm` depends on `rust-cvr`

```toml
# packages/rust-ivm/Cargo.toml
[dependencies]
rust-cvr = { path = "../rust-cvr" }
```

The `rust-cvr` crate becomes a library dependency of `rust-ivm`, not a separate
napi addon. The IVM engine's actor thread holds CVR components directly.

### 2. `EngineState` holds CVR components

```rust
// packages/rust-ivm/napi/src/lib.rs

pub struct EngineState {
    // Existing IVM fields (unchanged)
    engine: Option<Engine>,
    snapshotter: Option<Snapshotter>,
    sources: HashMap<String, Rc<RefCell<TableSource>>>,
    primary_keys: HashMap<String, Vec<String>>,
    table_specs: HashMap<String, TableSpecInfo>,
    syncable_tables: HashMap<String, LiteAndZqlSpec>,
    all_table_names: HashSet<String>,
    poisoned: bool,
    should_exit: bool,
    plan_count_cache: PlanCountCache,

    // NEW: CVR components, held directly on the actor thread
    cvr_store: Option<CVRStoreHandle>,        // sync buffer, async flush
    row_record_cache: Option<RowRecordCache>,  // stays on tokio (async PG)
    client_handlers: HashMap<String, ClientHandler>,  // per-connection
}
```

**Threading model**: `EngineState` is `!Send` (uses `Rc<RefCell>` for the
engine graph). The CVR sync components (pending writes buffer, poke body
assembly, updater state) are pure computation — they don't need tokio. Only the
**edges** (PG flush, WS push) need async:

- `CVRStoreHandle`'s `PendingWrites` buffer: `std::sync::Mutex` (no async
  needed — just a Vec)
- `CVRStoreHandle::flush()`: spawns onto tokio runtime (async PG write)
- `ClientHandler`'s poke state: `std::sync::Mutex` (body assembly is pure CPU)
- `WebSocketSink::push()`: `ThreadsafeFunction` to JS (the only boundary cross)
- `RowRecordCache`: stays on tokio runtime (async PG reads for catchup)

### 3. New napi methods on `RustIvmEngine`

The engine's napi surface gains CVR-aware methods. TS calls ONE method, and the
entire pipeline runs on the actor thread:

```rust
#[napi]
impl RustIvmEngine {
    /// Hydrate queries AND apply to CVR + push to clients — all on the actor thread.
    /// Returns version + summary. Row data never crosses the boundary.
    #[napi(ts_return_type = "Promise<HydrateAndSyncResult>")]
    pub fn hydrate_and_sync(
        &self,
        queries: Vec<NapiQuerySpec>,
        cvr_json: serde_json::Value,           // CVR snapshot
        state_version: String,
        replica_version: String,
        add_query_ids: Vec<String>,            // for trackQueries
        remove_query_ids: Vec<String>,
        client_ids: Vec<String>,               // clients to poke
        last_connect_time: f64,
        last_active: f64,
        ttl_clock: f64,
    ) -> AsyncTask<HydrateAndSyncTask> { ... }

    /// Advance to head AND apply to CVR + push to clients — all on the actor thread.
    #[napi(ts_return_type = "Promise<AdvanceAndSyncResult>")]
    pub fn advance_and_sync(
        &self,
        cvr_json: serde_json::Value,
        replica_version: String,
        client_ids: Vec<String>,               // clients at cvr.version to poke
        last_connect_time: f64,
        last_active: f64,
        ttl_clock: f64,
    ) -> AsyncTask<AdvanceAndSyncTask> { ... }
}
```

Return types (cross the boundary ONCE — just the summary):

```typescript
interface HydrateAndSyncResult {
  version: string;            // new CVR version
  cvr: unknown;               // updated CVR snapshot
  flushed: CVRFlushStats | false;
  queryPatches: PatchToVersion[];   // config patches (for catchup)
  numChanges: number;
  resetReason?: string;       // if advance/hydrate triggered a reset
}

interface AdvanceAndSyncResult {
  version: string;
  cvr: unknown;
  flushed: CVRFlushStats | false;
  numChanges: number;
  resetReason?: string;
}
```

### 4. The `#processChanges` logic moves into Rust

The entire TS `#processChanges` loop (view-syncer.ts:2217-2300) — de-duping,
refCount merging, batching at CURSOR_PAGE_SIZE, calling `received()` + routing
patches to `addPatch()` — becomes a Rust function that runs inside the engine's
`on_row_change` callback:

```rust
// Inside HydrateAndSyncTask::compute() or AdvanceAndSyncTask::compute()

/// The in-Rust equivalent of TS #processChanges.
/// Called from the engine's on_row_change callback — same thread, zero crossing.
struct ChangeProcessor<'a> {
    updater: &'a mut CVRQueryDrivenUpdater,
    pokers: &'a [ClientHandler],         // all clients to poke
    rows: HashMap<String, (RowID, RowUpdate)>,  // de-dupe buffer
    cursor_page_size: usize,
    total: usize,
}

impl<'a> ChangeProcessor<'a> {
    fn on_row_change(&mut self, rc: &RowChange) {
        // 1. Convert RowChange → RowUpdate entry (de-dupe by rowIDString)
        let id = RowID { schema: "", table: rc.table.clone(), row_key: ... };
        let id_str = row_id_string(&id);
        let entry = self.rows.entry(id_str).or_insert_with(|| {
            (id.clone(), RowUpdate { version: None, contents: None, ref_counts: BTreeMap::new() })
        });

        // 2. Strip _0_version (TS contentsAndVersion)
        match rc.change_type {
            ChangeType::Add => {
                let (version, contents) = strip_version(rc.row);
                entry.1.version = Some(version);
                entry.1.contents = Some(contents);
                *entry.1.ref_counts.entry(rc.query_id.clone()).or_insert(0) += 1;
            }
            ChangeType::Edit => {
                let (version, contents) = strip_version(rc.row);
                entry.1.version = Some(version);
                entry.1.contents = Some(contents);
                // No refCount change for EDIT
            }
            ChangeType::Remove => {
                *entry.1.ref_counts.entry(rc.query_id.clone()).or_insert(0) -= 1;
            }
        }

        // 3. Batch at CURSOR_PAGE_SIZE
        if self.rows.len() % self.cursor_page_size == 0 {
            self.flush_batch();
        }
    }

    fn flush_batch(&mut self) {
        if self.rows.is_empty() { return; }
        // 4. Call updater.received() — direct call, zero boundary crossing
        let existing_rows = self.updater.existing_rows();  // from RowRecordCache
        let patches = self.updater.received(&self.rows, &existing_rows);
        self.rows.clear();

        // 5. Route patches to all client handlers — direct call, zero crossing
        for patch in patches {
            for poker in self.pokers {
                poker.add_patch(&patch);  // sync body assembly, zero crossing
            }
        }
    }

    fn finish(&mut self) {
        self.flush_batch();
        // delete_unreferenced_rows
        let patches = self.updater.delete_unreferenced_rows(...);
        for patch in patches {
            for poker in self.pokers {
                poker.add_patch(&patch);
            }
        }
    }
}
```

### 5. The engine callback becomes the integration point

Currently, `add_queries_streaming` and `advance_to_head_stream` take
`F: FnMut(&RowChange)` closures. In the napi layer, these closures call
`tsfn.call()` to push rows to JS. In the new architecture, the closure calls
`ChangeProcessor::on_row_change()` instead:

```rust
// In HydrateAndSyncTask::compute()
let mut processor = ChangeProcessor::new(&mut updater, &pokers, cursor_page_size);

eng.add_queries_streaming(&specs, |rc| {
    processor.on_row_change(rc);
    // Also update row-set signature (currently done in TS handleRow)
    update_row_set_signature(&mut row_set_signatures, rc);
});

processor.finish();  // flush remaining batch + delete unreferenced
```

```rust
// In AdvanceAndSyncTask::compute()
let mut processor = ChangeProcessor::new(&mut updater, &pokers, cursor_page_size);

eng.advance_to_head_stream(&mut snapshotter, &syncable_tables, &all_table_names,
    |version, num_changes| { /* header: set version */ },
    |rc| {
        processor.on_row_change(rc);
        update_row_set_signature(&mut row_set_signatures, rc);
    },
);

processor.finish();
```

### 6. The poke chain — sync on the actor thread

Currently `ClientHandler` and `PokeHandler` use `tokio::sync::Mutex` for poke
state and `base_version`. On the actor thread (single-threaded by
construction), we can use `std::sync::Mutex` or even `RefCell`:

```rust
pub struct ClientHandler {
    // ... fields ...
    base_version: std::sync::Mutex<NullableCVRVersion>,
    poke_chain: AtomicBool,  // already AtomicBool in current code
    downstream: Arc<dyn WebSocketSink>,
}
```

The `WebSocketSink::push()` trait method becomes the ONLY boundary crossing in
the hot path. It's async, but on the actor thread we can't `.await`. Two
options:

**Option A (preferred): `push()` is synchronous, uses TSFN non-blocking.**

```rust
pub trait WebSocketSink: Send + Sync {
    fn push(&self, msg: Value);  // sync, non-blocking TSFN call
    fn fail(&self, e: String);
    fn cancel(&self);
}
```

The TSFN `call(NonBlocking)` returns immediately. If the JS side can't keep up,
the TSFN queue fills up and `call` returns `Closing`/`QueueFull`. At that point
the actor thread stops producing — natural backpressure, same as TS's
`#pokeTail` stall. The only difference: the stall happens in Rust, not JS.

**Risk**: `NonBlocking` drops frames if the queue is full. For poke frames,
this would corrupt the client state. Need to use `Blocking` instead — but
`Blocking` on the actor thread blocks the entire CG.

**Option B: `push()` is async, actor yields.**

The actor thread is not a tokio runtime — it's a `std::thread::spawn` with a
channel. It can't `.await`. But `Blocking` TSFN calls do a blocking wait that
yields the OS thread, which is fine — it's the same as what happens in TS today
(the WS send blocks the event loop).

**Recommendation: Option B.** `push()` uses `ThreadsafeFunctionCallMode::Blocking`.
This blocks the actor thread until JS drains the frame — identical backpressure
to TS. The actor thread is a dedicated OS thread per CG, so blocking it doesn't
affect other CGs (unlike blocking the Node event loop, which is the current TS
problem).

### 7. `CVRStoreHandle::flush()` — async edge

The store's `flush()` writes to PG via `sqlx`. This is async. On the actor
thread, we can't `.await`. Options:

**Option A: `tokio::runtime::Handle::block_on()`.** The napi process has a
tokio runtime. The actor thread can borrow its handle and block_on the flush.

**Option B: Channel to a tokio task.** The store sends its `PendingWrites`
buffer through a channel to a tokio task that does the actual PG write.

**Option C: `spawn_blocking` + `oneshot`.** Spawn a blocking task that runs the
async flush, wait for the result via a oneshot channel.

**Recommendation: Option A.** `block_on` is the simplest. The napi tokio
runtime is already running. The actor thread borrows the `Handle` and calls
`handle.block_on(store.flush())`. This blocks the actor thread until PG
completes — same as TS (where `await cvrStore.flush()` blocks the event loop).

### 8. RowRecordCache — stays on tokio

The `RowRecordCache` does async PG reads for `catchupRowPatches` and
`getRowRecords`. These are NOT on the hot path (they happen during catchup, not
during advance/hydrate). The cache can stay on the tokio runtime and be
accessed via `block_on` from the actor thread when needed.

### 9. The `#catchupClients` flow

`#catchupClients` is a separate flow that reads row patches from PG (via
`catchupRowPatches`) and sends them to clients. This flow also crosses the
boundary unnecessarily in the current architecture. In the new architecture:

- `catchupRowPatches` stays as a Rust async method (PG read)
- The patch → poke body assembly → WS push happens in Rust
- TS just calls `engine.catchupClients(clientIDs, fromVersion, toVersion)` and
  gets back a summary

This can be a separate napi method or part of `hydrate_and_sync`.

## Data flow after the refactor

### Hydration

```
TS: engine.hydrateAndSync(queries, cvr, addQueries, removeQueries, clientIDs, ...)
  → ONE napi call, enters actor thread
    → engine.add_queries_streaming(specs, |rc| {
        processor.on_row_change(rc)    ← same thread
          → de-dupe, strip _0_version, merge refCounts
          → batch at CURSOR_PAGE_SIZE
          → updater.received(rows)     ← same thread, direct call
          → for each patch: poker.add_patch(patch)  ← same thread, direct call
            → body assembly, flushBody at 100 parts
            → WebSocketSink::push(frame)  ← THE ONLY BOUNDARY CROSS (TSFN)
      })
    → processor.finish()
    → updater.flush() → store.flush() → PG write (block_on)
    → pokers.end(finalVersion) → WS push (TSFN)
  → return { version, cvr, queryPatches, flushed }  ← summary crosses ONCE
```

**Boundary crossings: 1 (WS push via TSFN) + 1 (PG flush via block_on) + 1
(return value).** Row data, refCounts, patches, and body assembly NEVER cross.

### Advance

```
TS: engine.advanceAndSync(cvr, clientIDs, ...)
  → ONE napi call, enters actor thread
    → engine.advance_to_head_stream(snap, tables, |version, n| {...}, |rc| {
        processor.on_row_change(rc)    ← same thread
      })
    → processor.finish()
    → updater.flush() → store.flush() → PG write
    → pokers.end(finalVersion) → WS push
  → return { version, cvr, flushed, numChanges }
```

Same pattern. **1 WS crossing + 1 PG crossing + 1 return value.**

## What TS becomes (dispatch shell)

```typescript
// #syncQueryPipelineSet (simplified)
async #syncQueryPipelineSet(lc, cvr, reason, ..., addQueries, removeQueries) {
  if (isRustCvrEnabled()) {
    const result = await this.#pipelines.hydrateAndSync(
      addQueries.map(q => ({ queryId: q.id, astJson: JSON.stringify(q.ast) })),
      cvr,
      stateVersion,
      this.#pipelines.replicaVersion,
      addQueries.map(q => q.id),
      removeQueries.map(q => q.id),
      this.#getClientIDs(),
      lastConnectTime,
      lastActive,
      ttlClock,
    );
    this.#cvr = result.cvr;
    // catchup clients that were behind (separate call if needed)
    return;
  }
  // ... existing TS path ...
}

// #advancePipelines (simplified)
async #advancePipelines(lc, cvr) {
  if (isRustCvrEnabled()) {
    const result = await this.#pipelines.advanceAndSync(
      cvr,
      this.#pipelines.replicaVersion,
      this.#getClientIDsAtVersion(cvr.version),
      lastConnectTime,
      lastActive,
      ttlClock,
    );
    this.#cvr = result.cvr;
    return result.resetReason ? new ResetPipelinesSignal(...) : 'success';
  }
  // ... existing TS path ...
}
```

The TS `#processChanges`, `generateRowChanges`, `contentsAndVersion`,
`AsyncQueue`, `deferClose`, the entire `CustomKeyMap` de-dupe loop, and the
patch-to-poker fanout — all of that code is **dead when `RUST_CVR=1`**. It
remains for the fallback path only.

## What gets removed from the `rust-cvr` napi crate

The `rust-cvr/napi/` crate shrinks dramatically. The following napi handles are
**removed** because they're now internal to the engine:

- `CVRConfigDrivenUpdaterHandle` → internal, called from actor thread
- `CVRQueryDrivenUpdaterHandle` → internal, called from actor thread
- `ClientHandlerHandle` → internal, called from actor thread
- `PokeHandlerHandle` → internal, called from actor thread
- `CVRStoreNapiHandle` → internal, called from actor thread

What **stays** in `rust-cvr/napi/`:
- `RowRecordCacheHandle` — still a separate napi handle (async PG reads, not
  on the actor thread). Or moves to `rust-ivm/napi` if we want one addon.
- Phase A signature functions — still pure functions exposed via napi.

Alternatively, **merge everything into `rust-ivm/napi`** as a single addon.
This avoids two `.node` files and matches the doc: "extends the handle's
surface."

## Threading summary

| Component | Thread | Locking | Why |
|---|---|---|---|
| Engine graph (pipelines, sources) | Actor thread (pinned) | `Rc<RefCell>` (single-threaded) | Existing — unchanged |
| Snapshotter (SQLite) | Actor thread | `Rc<RefCell>` | Existing — unchanged |
| CVR updater (state machine) | Actor thread | `&mut self` (borrowed) | Pure computation, no locking |
| CVRStore pending writes | Actor thread | `&mut self` | Buffer is append-only, flushed once |
| CVRStore flush (PG write) | Actor thread → tokio | `block_on(async flush)` | Edge: PG I/O |
| ClientHandler poke state | Actor thread | `std::sync::Mutex` | Single thread, but poke chain needs guard |
| PokeHandler body assembly | Actor thread | `&mut self` | Pure computation |
| WebSocketSink::push | Actor thread → JS | TSFN Blocking | Edge: WS I/O (the only hot-path crossing) |
| RowRecordCache (catchup reads) | Tokio runtime | `tokio::sync::Mutex` | Async PG reads, not on hot path |

## Implementation plan

### Step 1: `rust-ivm` depends on `rust-cvr` (1 day)

- Add `rust-cvr = { path = "../rust-cvr" }` to `rust-ivm/Cargo.toml`
- Add `rust-cvr = { path = "../rust-cvr" }` to `rust-ivm/napi/Cargo.toml`
- Verify `cargo build` passes for both crates
- No code changes yet, just the dependency

### Step 2: CVR components work on the actor thread (2-3 days)

- Change `CVRStoreHandle` to use `std::sync::Mutex` for `PendingWrites` (it
  already takes `&mut self`, so this is trivial)
- Change `ClientHandler` and `PokeHandler` to use `std::sync::Mutex` instead of
  `tokio::sync::Mutex`
- Change `WebSocketSink::push()` from `async` to sync (TSFN Blocking call)
- Change `PokeHandler::add_patch()` from `async` to sync
- Change `PokeHandler::end()` from `async` to sync
- Change `PokeHandler::cancel()` from `async` to sync
- Update all 10 client_handler tests + 15 store tests
- Verify `cargo test --lib` passes

### Step 3: `ChangeProcessor` — the `#processChanges` port (2-3 days)

- Port `contentsAndVersion` (strip `_0_version`) to Rust
- Port the de-dupe/accumulate/batch loop to `ChangeProcessor`
- Port the refCount merging (ADD/EDIT/REMOVE) to `ChangeProcessor::on_row_change`
- Port `flush_batch` (call `received()` + route patches to `add_patch()`)
- Port `finish` (flush remaining + `delete_unreferenced_rows`)
- Unit tests with fixture RowChanges
- Verify parity against TS `#processChanges` test cases

### Step 4: Wire into engine streaming callbacks (2-3 days)

- Add `cvr_store`, `row_record_cache`, `client_handlers` to `EngineState`
- Add `HydrateAndSyncTask` that uses `ChangeProcessor` inside
  `add_queries_streaming` callback
- Add `AdvanceAndSyncTask` that uses `ChangeProcessor` inside
  `advance_to_head_stream` callback
- Both tasks call `store.flush()` via `block_on` after processing
- Both tasks call `pokers.end()` after flush
- Both tasks return summary (version, cvr, queryPatches, flushed)
- Handle `ResetPipelinesSignal` (return reset reason in summary)
- Handle cancellation (same `StreamCreditGuard` pattern, but consumer is
  in-Rust, not JS — so credit/backpressure is moot; the `ChangeProcessor` is
  always ready to consume)

### Step 5: napi surface + TS dispatch shell (2-3 days)

- Add `hydrate_and_sync()` and `advance_and_sync()` napi methods on
  `RustIvmEngine`
- Add `NapiWebSocketSink` implementation (TSFN proxy to TS WS)
- Add napi methods for `ClientHandler` lifecycle: `register_client(wsID,
  clientID, baseCookie, pushCallback, failCallback, cancelCallback)` and
  `unregister_client(wsID)`
- Update `view-syncer.ts`: `#syncQueryPipelineSet` and `#advancePipelines` call
  the new methods when `RUST_CVR=1`
- Remove `replayStoreOps`, `drainStoreOps`, separate CVR napi handles from TS
- Verify `npx tsc --noEmit` clean

### Step 6: Catchup flow (1-2 days)

- Port `#catchupClients` to a Rust method: reads row patches from
  `RowRecordCache`, assembles poke bodies, pushes to WS
- Add `catchup_clients()` napi method
- Wire into `view-syncer.ts`

### Step 7: Config-driven updater integration (1-2 days)

- Wire `CVRConfigDrivenUpdater` into the actor thread
- Add `update_config()` napi method (ensureClient, setClientSchema,
  setProfileID, putDesiredQueries, deleteDesiredQueries, deleteClient, flush)
- Wire into `view-syncer.ts` `#updateCVRConfig`

### Step 8: Testing + verification (3-5 days)

- `cargo test --lib` for all Rust crates (existing 98 tests + new
  ChangeProcessor tests + new integration tests)
- `napi build` clean
- `npx tsc --noEmit` clean
- Parity tests: run `cvr.pg.test.ts` with `RUST_CVR=1` and `RUST_CVR=0`,
  assert identical results
- Body-shape byte equality: same poke bodies from TS and Rust paths
- Poke chain interleave regression test: two rapid advances, verify no frame
  interleaving

## What does NOT change

- The `rust-cvr` crate's **core logic** (updater state machine, store buffer,
  client handler body assembly, row record cache) — these are correct ports of
  the TS logic. They just need different locking (std vs tokio) and different
  callers (engine actor thread instead of napi async tasks).
- The `rust-ivm` engine's streaming mechanism (`add_queries_streaming`,
  `advance_to_head_stream` with `FnMut(&RowChange)` callbacks) — this is already
  the right abstraction. The callback just gets a different consumer.
- The TS fallback path — all existing TS code stays for `RUST_CVR=0`.
- The RowRecordCache — stays on tokio runtime for async PG reads.
- The CVR docs (80-87) — this doc (88) supplements them with the corrected
  wiring. The core port docs remain accurate.
