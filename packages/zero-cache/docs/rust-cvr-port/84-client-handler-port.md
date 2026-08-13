# 84 — ClientHandler Port

**Source:** `packages/zero-cache/src/services/view-syncer/client-handler.ts` (523 LOC)
**Target:** `packages/rust-cvr/src/client_handler.rs`

## What this class actually is

The bridge between CVR state changes and a WebSocket. One instance per WebSocket connection. Holds a private `#pokeTail: Promise<void>` chain that serializes pokes so a hydrate poke can't interleave with an advance poke's frames — this is the load-bearing invariant that was bug-prone in Rust until `f64f7e435` (rust-ivm-v1.7.0).

## Why porting this is delicate

The TS code's `startPoke(tentativeVersion)` returns a `PokeHandler` whose three methods (`addPatch`, `cancel`, `end`) MUST respect the single-flight-per-connection rule no matter which thread the call arrives from. The trust chain:

1. Client-side `poke-handler.ts` clears state and reconnects if a `pokeStart` arrives while another poke is in flight. (Source: `packages/zero-client/src/client/poke-handler.ts` — single-flight invariant.)
2. Server-side this invariant is currently upheld by the `#pokeTail` promise chain.

In Rust the same chain becomes a `tokio::sync::Mutex<PokeState>` per connection. The `flushBody()` bit (semi-arbitrary 100-row threshold) must NOT lock — it's an inner-interval batching, not a serialization boundary. Lock only on `pokeStart`, `pokeEnd`, `cancel`, and the call sequence boundaries.

## API shape

```rust
pub struct ClientHandler {
    client_group_id: String,
    client_id: String,
    ws_id: String,
    zero_clients_table: String,
    zero_mutations_table: String,
    downstream: WebSocketSink,           // abstract; real impl talks to napi->WS or direct WS
    base_version: Arc<Mutex<NullableCVRVersion>>,
    poke_tail: Arc<Mutex<()>>,           // token-style guard
    metrics_start: Option<Instant>,
}

impl ClientHandler {
    pub async fn version(&self) -> NullableCVRVersion;
    pub async fn fail(&self, e: Box<dyn Error>);
    pub async fn close(&self, reason: String);
    pub async fn start_poke(self: Arc<Self>, tentative_version: CVRVersion) -> PokeHandler;
    pub async fn send_delete_clients(&self, client_ids: Vec<String>, cgs: Vec<String>);
    pub async fn send_query_transform_application_errors(&self, errs: Vec<ErroredQuery>);
    pub async fn send_query_transform_failed_error(&self, e: TransformFailedBody);
    pub async fn send_inspect_response(&self, body: InspectDownBody);
}

pub struct PokeHandler {
    // owns an Arc<Mutex<PokeState>>
}

impl PokeHandler {
    pub async fn add_patch(&self, patch: PatchToVersion) -> Result<()>;
    pub async fn cancel(&self) -> Result<()>;
    pub async fn end(&self, final_version: CVRVersion) -> Result<()>;
}

pub struct PokeState {
    poke_id: String,
    base_cookie: Option<String>,
    cookie: String,
    started: bool,
    body: Option<PokePartBody>,          // the JSON body being built up
    part_count: usize,
    awaited_prior: bool,
}
```

## The WebSocket sink — the hardest bit

TS's `downstream` is `Subscription<Downstream>`, which is the zero protocol's sink for pushing JSON frames to the actual WebSocket. In Rust the equivalent is:

```rust
#[async_trait]
pub trait WebSocketSink: Send + Sync {
    async fn push(&self, msg: &Downstream) -> Result<()>;
    fn fail(&self, e: Box<dyn Error>);
    fn cancel(&self);
}
```

Two implementations:

1. `NapiWebSocketSink` — proxies to the TS-side WebSocket via napi's `ThreadsafeFunction::call(Ok::<(), Error>(msg))`. Used during the transition period when the WS server still lives in TS.
2. `RustWebSocketSink` — the end-state where `tokio-tungstenite` (or similar) hosts the WS directly inside the syncer. Not for the CVR port itself; future work.

## The body assembly — keep semantics exactly

`ensureBody()` lazily constructs `{pokeID}` once on the first patch, then `flushBody()` empties it when `partCount >= 100`. Behaviors to preserve:

- The floor: if `baseVersion >= tentativeVersion` at `startPoke` time, return a NOOP handler. **Never emit a `pokeStart` for a no-op poke.**
- Flush on 100 parts. `partCount` resets after every `flushBody`.
- `end()` checks `started == false && baseVersion == finalVersion` and skips pushing entirely, allowing a no-op poke to remain truly silent on the wire.
- `end()` calls `flushBody()` before pushing `pokeEnd` — the LAST partial batch must be on the wire before the cookie.

## The big-table special cases

`addPatch` for `type: 'row'` walks:

- If `patch.id.table == this.#zeroClientsTable` → merge into `body.lastMutationIDChanges`, do NOT queue to `rowsPatch`.
- Else if `patch.id.table == this.#zeroMutationsTable` → queue to `body.mutationsPatch` (special shape with `(clientID, mutationID)` and a `result` object). Includes the `normalizeMutationResult` defense-in-depth JSON.parse if `result` is a string.
- Else → `body.rowsPatch.push(makeRowPatch(patch))`.

These **must** be in Rust because they intercept patches based on table names — the routing lives next to the body assembly.

## The `#updateLMIDs` interception

```ts
// client-handler.ts:298
if (patch.op === 'put') {
  const {clientGroupID, clientID, lastMutationID} = v.parse(...);
  if (clientGroupID !== this.#clientGroupID) { lc.error?.(`Received clients row for wrong clientGroupID. Ignoring.`); }
  else { lmids[clientID] = lastMutationID; }
} else { /* constrain/del are ignored */ }
```

Rust: identical logic, with the **explicit `lc.error` log line preserved**. This log is load-bearing production telemetry for diagnosing cross-CG leaks.

## The row-schema validation — defer

TS `makeRowPatch` and `ensureSafeJSON` validate the row JSON (bigint-safety, assert safe JSON values, etc.). The Rust engine has `packages/rust-ivm/src/ivm/data.rs` with `Row`, `Value`, `CompoundKey`. The server-side row is already serde-compatible. **Do not re-validate** — Rust's `serde_json::to_value` is the source of truth; valita's `v.parse` is duplicative. Note as a divergence: **byte-for-byte equality of the JSON body may differ in bigint handling**. Run the diff test below.

## The poke chain in Rust

TS:

```ts
#pokeTail: Promise<void> = Promise.resolve();

startPoke(...) {
  const priorPoke = this.#pokeTail;
  let releasePoke!: () => void;
  const pokeDone = new Promise<void>(resolve => (releasePoke = resolve));
  this.#pokeTail = priorPoke.then(() => pokeDone);
  ...
}
```

Rust:

```rust
pub struct PokeChain {
    tail: Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
}

impl PokeChain {
    async fn chain_after(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let mut tail = self.tail.lock().await;
        let (tx, rx) = oneshot::channel();
        *tail = Some(rx);
        (prev_rx_or_ready, tx)
    }
}
```

`start_poke` returns a `PokeHandler` whose first call inside `add_patch` does `prev_rx.await` before pushing `pokeStart`. The next poke's `chain_after` uses the current `Sender` as "previous" — this is the same chain semantics.

## Testing surface

`client-handler.test.ts` is ~1900 LOC. Direct port priorities:

- `test-noop-poke-sends-nothing`
- `test-empty-poke-sends-only-start-and-end`
- `test-poke-flushes-at-100-parts`
- `test-poke-lmids-interception`
- `test-mutations-patch-shape`
- `test-cancel-does-not-leak-poke-chain`
- `test-serialized-pokes-never-interleave` ← CRITICAL REGRESSION TEST
- `test-end-after-no-start-does-not-push`
- `test-baseVersion-after-end-advances`

## Metrics parity

TS uses three OpenTelemetry metrics in this file: `sync.poke.time`, `sync.poke.transactions`, `sync.poke.rows`. Rust uses the opentelemetry crate with the same names. Attribute labels must match.

## The `normalizeMutationResult` defense

TS parses `result` as JSON if it's a string. Rust: keep an explicit `normalize_mutation_result` step. This was added as a bugfix — dropping it reintroduces conn-killing panics for lawful failed-mutation rows. **Add to the port; do not delete as "dead code".**

## Risk register

| Risk                                          | Impact                              | Mitigation                                                                                                         |
| --------------------------------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Bigint payload byte-diff                      | Valid rows rejected by client       | Diff-test TS vs Rust bodies against the same `RowPatchOp` fixtures; assert equality on every existing test fixture |
| Poke chain leak (forget to release on cancel) | Next poke hangs forever             | Use `Drop` on `PokeHandler` to auto-release the chain if it goes out of scope without `end`/`cancel`               |
| Body flush threshold off-by-one               | Valid but differently-shaped bodies | Exact `>=` comparison + unit test at boundary (99, 100, 101)                                                       |
| Row-schema validation difference              | Silent schema leakage               | Snapshot the JSON bodies for 1000 fixture rows, assert identity between TS and Rust outputs                        |
