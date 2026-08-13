# 83 — RowRecordCache Port

**Source:** `packages/zero-cache/src/services/view-syncer/row-record-cache.ts` (469 LOC)
**Target:** `packages/rust-cvr/src/row_record_cache.rs`

## What this class actually is

The doc comment at the top of the file buries the lead. This is NOT a cache in the "evict-on-lru" sense. It's a **write-through-or-write-back adapter** that sits in front of `cvr.rows` and `cvr.rowsVersion`. Its invariants:

1. The in-memory `#cache` is only refreshed once per CG (on `load`), then kept in sync via `apply()`.
2. `#pending` holds deferred writes that haven't hit Postgres yet.
3. `#pendingRowsVersion` is the CVR version those pending rows bring us to.
4. `#flushedRowsVersion` is the CVR version actually on disk.
5. When `flushedRowsVersion != pendingRowsVersion`, a background `#flush()` task pushes the rows and updates `rowsVersion`.

## The deferred mode decision

The trigger is **exactly one** condition in `executeRowUpdates`:

```ts
if (
  mode === 'allow-defer' &&
  (this.#flushing !== null || rowUpdates.size > this.#deferredRowFlushThreshold)
) {
  return []; // don't write now; cache will write later
}
```

Default threshold: **100 rows**. The sequencer: an already-active flush forces ALL subsequent flushes to defer until the current one finishes — that's the write-back switch-on-latch pattern.

Rust equivalent:

```rust
pub struct RowRecordCache {
    db: sqlx::PgPool,
    cvr_id: String,
    schema: String,
    cache: OnceCell<HashMap<String, RowRecord>>,          // keyed by rowIDString()
    pending: HashMap<String, Option<RowRecord>>,          // pending writes
    pending_rows_version: Option<CVRVersion>,
    flushed_rows_version: Option<CVRVersion>,
    flushing: Option<JoinHandle<()>>,
    deferred_threshold: usize,
}

impl RowRecordCache {
    pub async fn load(&mut self) -> Result<usize>;
    pub async fn get_row_records(&self) -> &HashMap<String, RowRecord>;
    pub fn has_pending_updates(&self) -> bool;
    pub async fn flushed(&self) -> Result<()>;
    pub async fn apply(&mut self, rows: HashMap<String, Option<RowRecord>>, version: CVRVersion, flushed: bool);
    pub fn execute_row_updates(&self, tx: &mut PgTransaction, version: &CVRVersion, rows: &HashMap<String, Option<RowRecord>>, mode: FlushMode) -> Vec<postgres::Statement>;
    pub async fn flush_pending(&mut self);
    pub async fn catchup_row_patches(&mut self, after_version: NullableCVRVersion, up_to: &CVRSnapshot, current: CVRVersion, exclude_query_hashes: &[String]) -> impl Stream<Item = Result<RowsRow, Error>>;
}
```

## Write-back mode: Rust mechanics

The TS `#flush()` runs in a `setTimeout(0)` task. In Rust this becomes `tokio::task::spawn` of a `flush_pending(this)` future. `this` needs `Arc<Mutex<RowRecordCache>>` or a `mpsc::UnboundedChannel<FlushRequest>` back to the owner so it remains Send across CG threads.

**Pick the channel:** The Rust CG is already on a single OS thread. Have the engine post a `FlushRequest` to its own event queue. This keeps the cache without a mutex and lets the same queue handle cancels/invalidations uniformly.

## The `#flushing` resolver dance

TS:

```ts
if (!flushed && this.#flushing === null) {
  this.#flushing = resolver();
  this.#flushing.promise.catch(() => {});
  this.#setTimeout(() => this.#flush(), 0);
}
```

The `#flushing.promise` is handed to callers via `flushed(lc)` so they can wait on completion. In Rust, use `tokio::sync::watch::Receiver<CVRVersion>` — the "current flushed version" channel. `flushed()` becomes:

```rust
async fn flushed(&self) -> Result<()> {
    let target = self.pending_rows_version;
    let mut rx = self.flushed_version.subscribe();
    while *rx.borrow() != target {
        rx.changed().await?;
    }
    Ok(())
}
```

## Loading — the 5000-row cursor

TS:

```ts
for await (const rows of this.#db<RowsRow[]>`
  SELECT "clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts"
  FROM ${this.#cvr(`rows`)}
    WHERE "clientGroupID" = ${this.#cvrID} AND "refCounts" IS NOT NULL`
  .cursor(5000)) { ... }
```

Postgres-cursor page size: 5000. Rust equivalent uses `sqlx::query_as::<_, RowsRow>(...).fetch()` with `.chunks(5000)` plus per-chunk commits via `PgCursor`. Note: **`sqlx`'s raw fetch isn't cursor-transaction-bound**; it just pulls. If we want true server-side cursors we issue `DECLARE CURSOR` explicitly via `sqlx::raw_sql`. The simpler path: use a single `fetch()` with a high-water channel and let the driver stream — at 5k row pages we'll arrive at the same memory profile.

**Decision:** match page size, don't replicate the explicit `DECLARE CURSOR`. Document the divergence.

## `catchupRowPatches` — async generator + `processReadTask`

TS wraps the read in `TransactionPool(lc, {mode: Mode.READONLY})` and runs `processReadTask` twice. Required because the stream iterates lazily and we need the SAME Postgres connection (and transaction snapshot) for both `checkVersion` and the row stream — otherwise the version check is meaningless.

Rust:

```rust
pub async fn catchup_row_patches(
    &self,
    after_version: Option<CVRVersion>,
    up_to: &CVRSnapshot,
    current: CVRVersion,
    exclude_query_hashes: &[String],
) -> Result<impl Stream<Item = Result<RowsRow>>> {
    let mut tx = self.db.begin_with("BEGIN READ ONLY").await?;

    check_version(&mut tx, &self.schema, &self.cvr_id, &current).await?;
    let rows = fetch_rows(&mut tx, &self.schema, &self.cvr_id, after_version, up_to.version, exclude_query_hashes)
        .await?;
    Ok(rows)  // stream borrows `tx` until done
}
```

The borrow threading via sqlx + fetch-stream on the same `Transaction` requires `stream` lifetime parameterization; stock sqlx supports `fetch_borrowed` for exactly this. Test the lifetime tradeoffs early — if it fights us, fall back to buffering the stream into a `Vec<RowsRow>` and returning `stream::iter(vec)`. **Document this** as the only intentionally-lossy divergence.

## The `apply()` invariants

```rust
async fn apply(&mut self, rows: HashMap<String, Option<RowRecord>>, version: CVRVersion, flushed: bool) {
    let cache = self.cache.get_mut().await;
    for (id, row) in rows {
        match row {
            None => { cache.remove(&id); }
            Some(r) if r.ref_counts.is_none() => { cache.remove(&id); }
            Some(r) => { cache.insert(id, r); }
        }
        if !flushed {
            self.pending.insert(id, row);
        }
    }
    self.pending_rows_version = Some(version);
    if !flushed && self.flushed_version.borrow() != self.pending_rows_version {
        self.spawn_flush_if_needed();
    }
}
```

## Edge case: crash before flush completes

TS doc comment says:

> Of course, there is the pathological situation in which a view-syncer process crashes before the pending row updates are flushed. In this case, the wait timeout will elapse and the CVR considered invalid.

This invariant is **preserved by construction**: if Rust's flush task dies, the `flushed_version` channel never advances, the load-retry loop times out, and the CVR is considered stale just as in TS.

## One place Rust differs legitimately — failures

TS's `#flush()` catches errors with `this.#lc.info?.(\`row record flush failed\`)`and calls`this.#failService(e)`. The Rust port should route errors to the same channel that panics the whole ViewSyncer (not just the cache). **Do not silently swallow** — the TS behavior is already lose-the-CVR; the Rust one is lose-the-CVR-too, with a louder trace.

## Testing surface

`row-record-cache` has no dedicated test file — it's covered indirectly by `cvr-store.pg.test.ts` and `view-syncer.pg.test.ts`. Port by writing **fresh** Rust tests in `packages/rust-cvr/tests/row_record_cache_test.rs`:

- `load-empty-cache`
- `apply-small-batch-sync-flush` (rows < 100)
- `apply-large-batch-async-flush` (rows > 100, verify the latch)
- `apply-while-flushing-defers-next-batch` (write-back latch preserved)
- `catchup-row-patches-empty`
- `catchup-row-patches-after-partial-flush`
- `flushed-promise-resolves-on-complete-flush`
- `flushed-promise-blocks-pending-rows`
- `apply-null-row-deletes-cache-entry`
- `apply-zero-refcounts-deletes-cache-entry`

## The keying decision (revisited from doc 82)

Same as in `updater.rs`: use `rowIDString`-stringified keys in `HashMap<String, ...>`. Container internals must be stable and match TS byte-for-byte.
