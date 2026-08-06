# 82 — CVR Updaters Port

**Source:** `packages/zero-cache/src/services/view-syncer/cvr.ts` (1194 LOC)
**Target:** `packages/rust-cvr/src/cvr.rs` + `updater.rs`

## The two updater classes

TS splits mutation responsibilities:

| Class | Used when | What it does |
|---|---|---|
| `CVRUpdater` | Any flush that just bumps `lastActive` / `ttlClock` | Base class with `flush()` and `_ensureNewVersion()` |
| `CVRConfigDrivenUpdater` | Client connects, changes desired queries, adds/removes profileID, disconnects | Maintains `clients[]`, `queries[]` metadata; generates `Patch { type: 'query' }` for each desired/got change |
| `CVRQueryDrivenUpdater` | Data actually arrives from the replicator; queries execute | Runs `trackQueries`, `received`, `deleteUnreferencedRows`, `flush`; generates `Patch { type: 'row' }` and handles refCounts |

Both are mutable (unlike the immutable `CVRSnapshot`), so the Rust port shouldn't try to make them immutable. Use a clean `&mut` API.

## State shape (one struct, no deep clone)

```rust
pub struct CVR {
    pub id: String,
    pub version: CVRVersion,                    // (stateVersion, configVersion)
    pub last_active: i64,
    pub ttl_clock: TTLClock,
    pub replica_version: Option<String>,
    pub clients: HashMap<String, ClientRecord>,
    pub queries: HashMap<String, QueryRecord>,
    pub client_schema: Option<ClientSchema>,
    pub profile_id: Option<String>,
}

pub enum QueryRecord {
    Internal { ast, transformation_hash, ... },
    Client   { ast, client_state: HashMap<ClientID, ClientState>, ... },
    Custom   { name, args: Vec<JSONValue>, client_state: ... },
}

pub struct ClientState {
    inactivated_at: Option<TTLClock>,
    ttl: i64,
    version: CVRVersion,
}
```

TS keeps the original `CVRSnapshot` for comparison: `_orig`. Rust borrows it. The mutable `_cvr` struct is the owned one.

## Key Rust API (one-to-one with TS)

```rust
pub struct CVRConfigDrivenUpdater<'a> {
    store: &'a CVRStoreHandle,
    orig: &'a CVRSnapshot,
    cvr: CVR,                     // owned, mutable
    shard: ShardID,
}

impl<'a> CVRConfigDrivenUpdater<'a> {
    pub fn ensureClient(&mut self, id: &str) -> &mut ClientRecord;
    pub fn setClientSchema(&mut self, lc: &LogContext, schema: ClientSchema) -> Result<()>;
    pub fn setProfileID(&mut self, lc: &LogContext, id: &str) -> Result<()>;
    pub fn putDesiredQueries(&mut self, client_id: &str, queries: Vec<DesiredQuerySpec>) -> Vec<PatchToVersion>;
    pub fn deleteDesiredQueries(&mut self, client_id: &str, hashes: Vec<String>) -> Vec<PatchToVersion>;
    pub fn deleteClient(&mut self, client_id: &str, ttl_clock: TTLClock) -> Vec<PatchToVersion>;
    pub fn clearDesiredQueries(&mut self, client_id: &str) -> Vec<PatchToVersion>;
    pub fn flush(self, lc: &LogContext, last_connect: i64, last_active: i64, ttl: TTLClock)
        -> Result<(CVRSnapshot, CVRFlushStats), FlushFailure>;
}
```

```rust
pub struct CVRQueryDrivenUpdater<'a> {
    store: &'a CVRStoreHandle,
    orig: &'a CVRSnapshot,
    cvr: CVR,
    state_version: LexiVersion,

    // private mutable state
    removed_or_executed_query_ids: HashSet<String>,
    received_rows: HashMap<RowID, RefCountsOrNull>,   // CustomKeyMap, see below
    last_patches: HashMap<RowID, RowPatchInfo>,       // CustomKeyMap
    row_set_signature_provider: Option<Box<dyn Fn(&str) -> Option<u64>>>,

    // lazy — populated by trackQueries
    existing_rows: Option<Box<dyn Stream<Item = RowRecord>>>,
}
```

`trackQueries`, `received`, `deleteUnreferencedRows`, `flush` become `async fn` on this struct.

## The two CustomKeyMap classes

`#receivedRows` and `#lastPatches` are keyed by `RowID` (composite: `{schema, table, rowKey: JSONB}`). TS uses a `CustomKeyMap<RowID, V>` that hashes with `rowIDString()`. In **Rust** we get two options:

1. Define `impl Hash for RowID` that walks the JSONB rowKey fields in a deterministic order, plus `impl Eq` matching it. **Danger:** key order inside JSONB maps is not stable in serde_json unless you use `preserve_order`.
2. Use the same `rowIDString()` stringification as the key and `HashMap<String, (RowID, V)>` — mirrors TS exactly. **Preferred for parity.**

Pick option 2. `rowIDString()` is already a stable canonicalization (sorts keys, handles nested values consistently).

## The refCounts math

`mergeRefCounts` (TS line ~1041) is the only non-trivial combinator of the whole updater:

```ts
function mergeRefCounts(
  existing: RefCounts | null | undefined,
  received: RefCounts | null | undefined,
  removeHashes?: Set<string>,
): RefCounts | null {
  // Returns `null` if no positive refs remain.
  // Skips counting `existing` refs from `removeHashes`.
  // Drops zero entries inline.
}
```

This is a pure function of three inputs. Move it verbatim into `updater.rs`. Property: `mergeRefCounts(null, null, _) == null`, and `mergeRefCounts(x, null, ∅) == x` after normalizing zeros.

## The `flush()` ordering — preserved exactly

The TS `CVRQueryDrivenUpdater.flush()` has one **critical side-band write**:

```ts
// Persist each query's rowSetSignature IF the pipeline driver's signature
// differs from what's on disk.
for (const [queryID, query] of Object.entries(this._cvr.queries)) {
  const sig = this.#rowSetSignature?.(queryID);
  if (sig === undefined) continue;
  if (parseSignature(query.rowSetSignature) === sig) continue;
  query.rowSetSignature = formatSignature(sig);
  this._cvrStore.updateRowSetSignature(queryID, sig);
}
return super.flush(...);
```

Rust:

```rust
impl CVRQueryDrivenUpdater<'_> {
    async fn flush(mut self, lc: &LogContext, ...) -> Result<...> {
        if let Some(provider) = &self.row_set_signature_provider {
            for (id, q) in self.cvr.queries.iter_mut() {
                if let Some(sig) = provider(&id) {
                    if parseSignature(q.row_set_signature) == sig { continue; }
                    q.row_set_signature = Some(formatSignature(sig));
                    self.store.update_row_set_signature(&id, sig);
                }
            }
        }
        self.into_base().flush(lc, ...).await
    }
}
```

Must run **before** the base `flush()` because the base one snapshots `_cvr`. Order of side-effect operations: (1) signatures, (2) `updateTTLClock` / `updateReplicaVersion` inside base `flush()`.

## The internal-query special cases

Two query IDs are reserved and managed by `ensureClient`:

- `CLIENT_LMID_QUERY_ID = "lmids"` — tracks `${zero_schema}.clients` per clientGroupID
- `CLIENT_MUTATION_RESULTS_QUERY_ID = "mutationResults"` — tracks `${zero_schema}.mutations` per clientGroupID

TS throws `Error('Query ID ${query.id} is reserved for internal use')` if a client ever tries to register a query with those IDs. **Keep the same error string.** This message appears in client logs and change-streamer emails.

## Testing surface

One-to-one port of `cvr.pg.test.ts` (~3300 LOC, larger than the src itself). Direct mapping:

- `CVRConfigDrivenUpdater` test suite (40 tests) → `#[tokio::test] mod config_driven_updater`
- `CVRQueryDrivenUpdater` suite (60+ tests) → `mod query_driven_updater`
- `mergeRefCounts` property tests (already quickcheck-style in TS) → `proptest!` in Rust

## What's intentionally NOT ported

- `assertNotInternal` / TypeScript's structural type discrimination — Rust's enum Discriminant is already exhaustive; runtime checks disappear.
- The `startSpan` OTEL wrappers — Rust has `tracing::info_span!` and they are usually free; the API surface doesn't need to propagate span handles.

## Risk register

| Risk | Impact | Mitigation |
|---|---|---|
| `rowIDString`-as-key drifts from TS | HKMap lookups silently miss | Unit-test the canonicalization with the same JSON vectors TS uses |
| `mergeRefCounts` property violations | RefCounts leak rows | Proptest invariants; byte-compare 10000 random cases TS-vs-Rust |
| Flush ordering (signatures before base flush) | Signature drift; spurious re-execution | Explicit docstring + integration test with simulated `rowSetSignatureProvider` returning drift |
