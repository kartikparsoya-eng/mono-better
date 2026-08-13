# 81 — CVRStore Port

**Source:** `packages/zero-cache/src/services/view-syncer/cvr-store.ts` (1382 LOC)
**Target:** `packages/rust-cvr/src/store.rs`

## Responsibility

The CVRStore is the only piece that **writes to Postgres**. Everything else is in-memory bookkeeping or per-connection wiring. The surface it exposes to the callers is deliberately small — only the data that has durable meaning.

```ts
// TS interface (cvr-store.ts public surface)
class CVRStore {
  constructor(lc, db, shard, cvrID, taskID, failService, loadAttemptIntervalMs, ownershipTimeoutMs);
  load(lc, lastConnectTime): Promise<LoadedCVR>;
  flush(lc, lastUpdate, newCVR, lastConnectTime): Promise<CVRFlushStats | false>;
  catchupConfigPatches(lc, afterVersion, upToCVR, current, excludeQueryHashes?): AsyncGenerator<ConfigPatch[], void, undefined>;
  catchupRowPatches(lc, afterVersion, upToCVR, current, excludeQueryHashes?): AsyncGenerator<RowsRow[], void, undefined>;
  insertClient(client: ClientRecord): void;
  putQuery(query: QueryRecord): void;
  updateQuery(query: QueryRecord): void;
  putDesiredQuery(version, query, client, deleted, inactivatedAt, ttl): void;
  delDesiredQuery(...): void;
  markQueryAsDeleted(version, patch): void;
  putRowRecord(row: RowRecord): void;
  delRowRecord(id: RowID): void;
  setQueryRowSetSignature(queryHash, signature): void;
  updateInstanceFields(fields): void;
  updateTTLClock(ttlClock, lastActive): Promise<void>;
  updateReplicaVersion(replicaVersion): Promise<void>;
  updateClientSchema(schema): Promise<void>;
  updateProfileID(profileID): Promise<void>;
  release(): Promise<void>;
}
```

**Everything except `load` and `flush` is a buffered-queue enqueue.** The `flush()` method commits the queue atomically inside a single Postgres transaction against `cvr.instances`, `cvr.clients`, `cvr.queries`, `cvr.desires`, `cvr.rows`, `cvr.rowsVersion`.

## Schema

Postgres tables (`schema/cvr.ts` DDL) — verbatim mirror:

- `cvr.instances` — (clientGroupID, version, replicaVersion, lastActive, ttlClock, clientSchema, profileID, owner, grantedAt, deleted)
- `cvr.clients` — (clientGroupID, clientID, patchVersion, deleted)
- `cvr.queries` — (clientGroupID, queryHash, clientAST, queryName, queryArgs, patchVersion, transformationHash, transformationVersion, internal, deleted, rowSetSignature)
- `cvr.desires` — (clientGroupID, clientID, queryHash, patchVersion, inactivatedAt, ttl, deleted)
- `cvr.rows` — (clientGroupID, schema, table, rowKey, rowVersion, patchVersion, refCounts)
- `cvr.rowsVersion` — (clientGroupID, version) — separate so metadata flush can complete without row flush (defer optimisation)

## Translation strategy

The TS code builds the queue in two layers:

1. A `PendingQuery<Row[]>[]` array of pending SQL expressions (from the `postgres` driver's tagged-template)
2. An `execute()` at flush-time which serializes them all into a single transaction

Rust port replaces both with a simple struct:

```rust
pub struct CVRStoreHandle {
    pool: sqlx::PgPool,
    cvr_id: String,
    task_id: String,
    schema: String,
    // Pending write queue for the next flush()
    pending: PendingWrites,
}

#[derive(Default)]
struct PendingWrites {
    instances: Option<InstanceRow>,
    clients_insert: Vec<ClientsRow>,
    clients_del: Vec<String>,
    queries_upsert: Vec<QueriesRow>,
    queries_del: Vec<(String, String)>, // (cvr_id, query_hash)
    desires_upsert: Vec<DesiresRow>,
    desires_del: Vec<(String, String, String)>, // (cvr_id, client_id, query_hash)
    rows_upsert: Vec<RowsRow>,
    rows_del: Vec<(String, String, String, serde_json::Value)>, // rowKey is JSONB
    rows_version: Option<Versioned<String>>,
}

pub struct LoadResult {
    instance: Option<InstanceRow>,
    clients: Vec<ClientsRow>,
    queries: Vec<QueriesRow>,
    desires: Vec<DesiresRow>,
}
```

`flush(lc, expected_loaded_version, new_cvr, last_connect_time) -> Result<CVRFlushStats, OwnershipError>` becomes a single `sqlx::Transaction` block. The `sqlx-postgres` crate handles the wire protocol; we avoid the `postgres` npm-driver queue pattern entirely.

## The ownership check — keep semantics exact

TS:

```ts
// cvr-store.ts:388
if (owner !== this.#taskID) {
  if ((grantedAt ?? 0) > lastConnectTime) {
    throw new OwnershipError(owner, grantedAt, lastConnectTime);
  } else {
    // Fire-and-forget UPDATE ... WHERE grantedAt <= lastConnectTime (older owner)
    void this.#db`UPDATE cvr.instances SET owner=..., grantedAt=... ...`;
  }
}
```

**This is a void-write.** The UPDATE runs in the background; if it fails, the current owner won't stop and the next load() will retry. The Rust port must replicate this **without dropping the error** — fire-and-forget with `.catch(this.#failService)` semantics.

In Rust, a `tokio::spawn(update_ownership_signal())` future that returns `Result<()>` and logs to the same OTEL context. **Decision deferred: see doc 85.**

## The deferred-rows path

The `flush()` method has a branch on `rowsRegardless` (rows to flush) vs `deferred` (rows pushed to the RowRecordCache's write-back queue instead). The threshold is `deferredRowFlushThreshold = 100` by default.

```ts
const rows = rowUpdates.size > this.#deferredRowFlushThreshold
  ? []  // defer to cache
  : this.#rowRecordCache.executeRowUpdates(...)
```

In Rust, the CVRStore and the RowRecordCache live on the same thread, but the call must remain explicit so the threshold is testable. Keep this branch byte-compatible.

## Catchup readers

Both `catchupConfigPatches` and `catchupRowPatches` are async generators streaming PaginatedQueryResults. Rust side:

- Use `sqlx::query_as::<_, RowsRow>(...).fetch(&mut conn)` to get a `Stream`
- Each `yield` from the generator becomes a `poll_next` on the stream adapter returned to TS via napi's `AsyncIterator`

The `async iterator` napi feature (`napi::AsyncIterator`) directly mirrors TS's async generator protocol.

## Testing surface

Same tests that exist in `cvr-store.pg.test.ts` (1500+ LOC, uses `pg-mem`/`testcontainers`). Port them with the same names:

- `test-load-empty-cvr`
- `test-load-existing-cvr`
- `test-flush-instance-only`
- `test-flush-instance-and-clients`
- `test-flush-instance-and-queries`
- `test-flush-instance-and-desires`
- `test-flush-instance-and-rows-small` (below 100)
- `test-flush-instance-and-rows-large-deferred`
- `test-flush-fails-when-version-mismatch`
- `test-catchup-config-patches-empty`
- `test-catchup-row-patches-empty`
- `test-catchup-row-patches-after-flush`
- `test-ownership-handoff-success`
- `test-ownership-refused-when-grantedAt-too-new`

## Open questions remaining

- None that block. Phase B of the master plan can start here.
