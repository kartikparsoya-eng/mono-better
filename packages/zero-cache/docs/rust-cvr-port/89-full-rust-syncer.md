# 89 — Full Rust Syncer: Complete Behavior Specification

**Status:** Design document. Supersedes doc 88's "dispatch shell" approach.
The entire syncer process — WebSocket server, connection routing, dispatch
loop, ViewSyncer, CVR, pokes — runs as a single Rust binary. No Node.js, no
napi, no TSFN, no event loop. TS code is kept as dead code behind a flag.

## Problem statement

Doc 88 prescribed a "TS dispatch shell" — TS keeps the WebSocket server and
dispatch loop, calling into Rust via napi for the hot path. This was built and
works, but it creates 10 gaps (two separate Rust stores, two poke paths, cache
desync, config-driven path still in TS, catchup still in TS, the entire
`rust-cvr/napi/` crate still needed, etc.). The partial approach is the worst
of both worlds — the complexity of two code paths with none of the
boundary-elimination benefits.

## What we're building

A single Rust binary (`rust-syncer`) that replaces 16 TS files (~10,089 LOC):

| #   | TS File                                              | LOC  | Replaced by                              |
| --- | ---------------------------------------------------- | ---- | ---------------------------------------- |
| 1   | `workers/syncer.ts`                                  | 382  | `ws_server.rs` + `router.rs`             |
| 2   | `workers/connection.ts`                              | 457  | `connection.rs`                          |
| 3   | `workers/syncer-ws-message-handler.ts`               | 283  | `message_handler.rs`                     |
| 4   | `services/view-syncer/view-syncer.ts`                | 2940 | `view_syncer.rs`                         |
| 5   | `services/view-syncer/connection-context-manager.ts` | 874  | `connection_context.rs`                  |
| 6   | `services/view-syncer/drain-coordinator.ts`          | 76   | `drain.rs`                               |
| 7   | `services/view-syncer/cvr-store.ts`                  | 1492 | Already ported (`store.rs`)              |
| 8   | `services/view-syncer/cvr.ts`                        | 1399 | Already ported (`cvr.rs` + `updater.rs`) |
| 9   | `services/view-syncer/client-handler.ts`             | 623  | Already ported (`client_handler.rs`)     |
| 10  | `services/view-syncer/row-record-cache.ts`           | 686  | Already ported (`row_record_cache.rs`)   |
| 11  | `server/inspector-delegate.ts`                       | 171  | `inspector.rs`                           |
| 12  | `services/view-syncer/inspect-handler.ts`            | 215  | `inspect_handler.rs`                     |
| 13  | `types/websocket-handoff.ts`                         | 173  | Not needed (direct accept)               |
| 14  | `server/runner/zero-dispatcher.ts`                   | 52   | `http_server.rs`                         |
| 15  | `server/worker-dispatcher.ts`                        | 186  | Not needed (single process)              |
| 16  | `workers/connect-params.ts`                          | 80   | `connect_params.rs`                      |

**Already ported (reuse directly):** 7-10 (~4200 LOC of Rust already exists).

**New Rust code to write:** Files 1-6, 11-12, 14, 16 (~2800 LOC TS → ~2000 LOC Rust).

**Flag:** `ZERO_SYNCER=rust` — process manager launches `rust-syncer` binary.
`ZERO_SYNCER=ts` (default) — existing TS syncer runs unchanged.

---

## Component 1: WebSocket Server (`ws_server.rs`)

**Ports:** `syncer.ts` (Syncer class), `zero-dispatcher.ts`, `websocket-handoff.ts`

### Behaviors to preserve exactly

#### 1.1 Server initialization (syncer.ts:46-70)

- `WebSocketServer` with `noServer: true` (handoff model)
- `maxPayload` from `config.websocketMaxPayloadBytes`
- Per-message deflate compression when `config.websocketCompression` is true
  - If `config.websocketCompressionOptions` is set, parse as JSON and use as
    `perMessageDeflate` options
  - If parsing fails, throw error with message about
    `ZERO_WEBSOCKET_COMPRESSION_OPTIONS`
- **Rust:** `tokio-tungstenite` with `WebSocketConfig::max_message_size` and
  compression via `tokio-tungstenite`'s deflate feature.

#### 1.2 WebSocket handoff (websocket-handoff.ts)

- The dispatcher (parent process) accepts HTTP upgrade requests and sends the
  raw TCP socket to the syncer worker via IPC.
- The syncer worker receives the socket via `installWebSocketReceiver` and
  calls `server.handleUpgrade(message, socket, head, callback)`.
- Guard: if WebSocket is closed during handoff (CLOSED/CLOSING), skip receive
  and log warning.
- **Rust:** In the full Rust binary, the WebSocket server accepts directly —
  no handoff needed. The `tokio-tungstenite` `accept_async` handles the
  upgrade. This eliminates the entire handoff mechanism.

#### 1.3 HTTP endpoints (zero-dispatcher.ts:30-43)

- `GET /statz` — server statistics (active connections, memory, etc.)
- `GET /heapz` — heap profiling snapshot
- **Rust:** `axum` router with `/statz` and `/heapz` routes.

#### 1.4 Drain mechanism (syncer.ts:340-370, drain-coordinator.ts)

- Two drain types:
  1. **Elective drain:** ViewSyncer checks `shouldDrain()` before processing
     a replication event. If true, exits its run loop and calls
     `drainNextIn(hydrationTime)`.
  2. **Force drain:** `Syncer.drain()` picks a random ViewSyncer, calls
     `stop()`, waits for `forceDrainTimeout`, repeats.
- `TARGET_UTILIZATION = 0.6` — drain interval is divided by 0.6 to give the
  receiving server breathing room.
- `FORCE_DRAIN_PADDING = 2` ms — extra padding on force drain timeout.
- `drainNextIn(interval)`: sets `nextDrainTime = now + interval / 0.6`,
  clears existing timeout, sets new timeout for `interval + 2` ms.
- **Rust:** `DrainCoordinator` struct with `AtomicI64` for `next_drain_time`,
  `std::thread::spawn` for the force-drain timer. Same constants.

---

## Component 2: Connection (`connection.rs`)

**Ports:** `connection.ts` (Connection class, ~457 LOC)

### Behaviors to preserve exactly

#### 2.1 Connection construction (connection.ts:48-116)

- Store `ws`, `wsID`, `protocolVersion`, `lc`, `onClose`, `messageHandler`.
- Add `close` and `error` event listeners on the WebSocket.
- Start proxying inbound messages immediately via `#proxyInbound()`.
- Start `#downstreamMsgTimer` interval at `DOWNSTREAM_MSG_INTERVAL_MS / 2`
  (3000ms).
- `DOWNSTREAM_MSG_INTERVAL_MS = 6000` — slightly longer than client's 5s
  PING_INTERVAL.

#### 2.2 Protocol version check + `connected` message (connection.ts:118-135)

- `init()` checks `protocolVersion` against `PROTOCOL_VERSION` and
  `MIN_SERVER_SUPPORTED_SYNC_PROTOCOL`.
- If too high or too low: close with `VersionNotSupported` error.
- If OK: send `['connected', {wsid, timestamp: Date.now()}]` with
  `'ignore-backpressure'` flag, return `true`.
- **Rust:** Compare version constants, send `Connected` message via
  `DirectWebSocketSink`.

#### 2.3 Inbound message handling (connection.ts:137-170)

- Parse incoming WS message as JSON.
- Parse with `upstreamSchema` (valita union). On parse error: close with
  `InvalidMessage` error.
- If `msgType === 'ping'`: respond with `['pong', {}]` immediately,
  `'ignore-backpressure'`. Return.
- Otherwise: call `messageHandler.handleMessage(msg)`.
- For each `HandlerResult` in the array, call `#handleMessageResult(r)`.
- On exception: call `#closeWithThrown(e)`.
- **Rust:** `serde_json::from_str` → match on tag → dispatch.

#### 2.4 HandlerResult processing (connection.ts:172-207)

- `'fatal'`: close with error.
- `'ok'`: no-op.
- `'stream'` with `source: 'viewSyncer'`: store as `viewSyncerOutboundStream`,
  start proxying outbound.
- `'stream'` with `source: 'pusher'`: store as `pusherOutboundStream`,
  start proxying outbound.
- `'transient'`: send each error via `sendError()`.
- Assert: only one viewSyncer and one pusher outbound stream per connection.

#### 2.5 Outbound proxying (connection.ts:234-250)

- Pipe the outbound `Source<Downstream>` to the WebSocket via `send()`.
- On error in the pipeline: `#closeWithThrown(e)`.
- On normal close: `close('downstream closed by ViewSyncer')`.
- **Rust:** The `DirectWebSocketSink` is the outbound channel. The CG thread
  writes poke frames to it directly.

#### 2.6 Keepalive pong (connection.ts:289-296)

- Every 3s (`DOWNSTREAM_MSG_INTERVAL_MS / 2`), check if
  `Date.now() - lastDownstreamMsgTime > 6000`.
- If true: send `['pong', {}]` with `'ignore-backpressure'`.
- **Rust:** `tokio::time::interval` with 3s period, check last send time.

#### 2.7 Send function (connection.ts:298-326)

- If `ws.readyState === OPEN`: `ws.send(JSON.stringify(data))`.
- If callback is `'ignore-backpressure'`: don't pass callback (fire-and-forget).
- If callback is a function: pass it to `ws.send()` for backpressure.
- If not OPEN: log dropped message, if callback is not `'ignore-backpressure'`,
  call callback with `Internal: WebSocket closed` error.
- **Rust:** Check WS state, serialize to JSON, send via `tokio_tungstenite`.

#### 2.8 Error sending (connection.ts:328-378)

- `sendError(lc, ws, errorBody, thrown?)`:
  - Determine log level:
    - If `thrown instanceof ProtocolErrorWithLevel`: use its `logLevel`.
    - If error has `errno` or transient socket code (`EPIPE`, `ECONNRESET`,
      `ECANCELED`): `'warn'`.
    - If message contains `'socket was closed while data was being compressed'`: `'warn'`.
    - If `errorBody.kind === ClientNotFound || TransformFailed`: `'warn'`.
    - Otherwise: `getLogLevel(thrown)` or `'info'`.
  - Log at determined level.
  - Send `['error', errorBody]` with `'ignore-backpressure'`.
- **Rust:** Match the same error classification logic. Use `tracing` crate
  for leveled logging.

#### 2.9 Close handling (connection.ts:142-160, 209-218)

- `close(reason, ...args)`:
  - If already closed: return.
  - Set `#closed = true`.
  - Log: `'closing connection: ${reason}'` at info level.
  - Remove event listeners.
  - Cancel `viewSyncerOutboundStream` and `pusherOutboundStream`.
  - Call `onClose()`.
  - If WS not already CLOSED: `ws.close()`.
  - Clear `downstreamMsgTimer`.
- `#handleClose`: extract `{code, reason, wasClean}` from CloseEvent, call
  `close('WebSocket close event', {code, reason, wasClean})`.
- `#handleError`: log at warn level.

#### 2.10 `handleInitConnection` (connection.ts:162-164)

- Takes a string (JSON), calls `#handleMessage({data: initConnectionMsg})`.
- Used when `initConnection` is piggybacked in the `sec-websocket-protocol`
  header during WS handshake.
- **Rust:** Parse the `sec-websocket-protocol` header during accept, extract
  the initConnection message, process it before entering the message loop.

---

## Component 3: Connect Params (`connect_params.rs`)

**Ports:** `connect-params.ts` (~80 LOC)

### Behaviors to preserve exactly

#### 3.1 URL parameter parsing

- `clientID` — required string
- `clientGroupID` — required string
- `profileID` — optional string
- `baseCookie` — optional string
- `ts` — required integer (timestamp)
- `lmID` — required integer (last mutation ID)
- `wsID` — optional string, defaults to `''`
- `userID` — optional string, defaults to `undefined`
- `debugPerf` — optional boolean

#### 3.2 `sec-websocket-protocol` header decoding

- `decodeSecProtocols(header)`:
  - `decodeURIComponent(header)` → `atob()` → UTF-8 decode → `JSON.parse()`.
  - Returns `{initConnectionMessage, authToken}`.
  - `initConnectionMessage` is an `InitConnectionMessage` or `undefined`.
  - `authToken` is a string or `undefined`.

#### 3.3 Other headers

- `cookie` — HTTP cookie header
- `origin` — HTTP origin header

---

## Component 4: Message Handler (`message_handler.rs`)

**Ports:** `syncer-ws-message-handler.ts` (~283 LOC)

### Behaviors to preserve exactly

#### 4.1 Construction

- Takes `lc`, `connectParams`, `connContextManager`, `viewSyncer`, `mutagen?`,
  `pusher?`.
- Creates a per-connection `Lock` for mutation ordering.
- Stores `clientGroupID`, `connectionSelector = {clientID, wsID}`.

#### 4.2 Message routing (handleMessage)

Routes by `msg[0]` (message type):

**`'ping'`**: Log error `'Ping is not supported at this layer by Zero'`. Break.

**`'pull'`**: Log error `'Pull is not supported by Zero'`. Break.

**`'push'`**:

1. Extract `traceparent` from `msg[1]`.
2. Validate `clientGroupID` in mutation matches connection's `clientGroupID`.
   If mismatch: return `[{type: 'fatal', error: {kind: InvalidPush, ...}}]`.
3. If `mutations.length === 0`: return `[{type: 'ok'}]`.
4. If `mutations[0].type === 'custom'`:
   - If no `pusher`: return fatal `InvalidPush` error.
   - Otherwise: return `[pusher.enqueuePush(selector, msg[1])]`.
5. If CRUD mutation:
   - If no `mutagen`: return fatal `InvalidPush` error.
   - Assert auth is JWT (not opaque).
   - Acquire `mutationLock`, process each mutation via `mutagen.processMutation()`.
   - Collect errors. Return `{type: 'transient', errors}` or `{type: 'ok'}`.

**`'changeDesiredQueries'`**:

1. Extract `traceparent`.
2. Call `viewSyncer.changeDesiredQueries(selector, msg)`.
3. Return `[{type: 'ok'}]`.

**`'updateAuth'`**:

1. Get initial `connCtx` via `mustGetConnectionContext`.
2. Call `connContextManager.updateAuth(selector, msg[1])`.
3. Check if `authRevisionChanged`.
4. Call `viewSyncer.updateAuth(selector, msg, authRevisionChanged)`.
5. Return `[{type: 'ok'}]`.

**`'deleteClients'`**:

1. Call `viewSyncer.deleteClients(selector, msg)`.
2. Get `deletedClientIDs` from result.
3. If `pusher` and `deletedClientIDs.length > 0`: call
   `pusher.deleteClientMutations(selector, deletedClientIDs)`.
4. Return `[{type: 'ok'}]`.

**`'initConnection'`**:

1. Call `connContextManager.initConnection(selector, msg[1])`.
2. Extract `traceparent`.
3. Return `[{type: 'stream', source: 'viewSyncer', stream:
viewSyncer.initConnection(selector, msg)}]`.
4. If `pusher` is configured: also return
   `{type: 'stream', source: 'pusher', stream: pusher.initConnection(selector)}`.

**`'closeConnection'`**: Deprecated, no-op. Break.

**`'inspect'`**:

1. Call `viewSyncer.inspect(selector, msg)`.
2. Return `[{type: 'ok'}]`.

**`'ackMutationResponses'`**:

1. If `pusher`: call `pusher.ackMutationResponses(selector, msg[1])`.
2. Return `[{type: 'ok'}]`.

**Default**: `unreachable(msgType)` — throws error for unknown message types.

#### 4.3 Traceparent propagation

- `withTraceparent(traceparent, fn)`: extracts W3C traceparent header into
  OTel context, runs `fn` within that context.
- If no traceparent: just run `fn()`.
- **Rust:** Use `opentelemetry` crate's context propagation.

---

## Component 5: Connection Context Manager (`connection_context.rs`)

**Ports:** `connection-context-manager.ts` (~874 LOC)

This is a critical state machine. Every behavior must be preserved.

### 5.1 Types

```rust
pub enum ConnectionState { Provisional, Validated }

pub struct UserState { id: Option<String> }  // None = logged out

pub enum ConnectionValidation {
    ClientFallback,
    ServerValidated { validated_user_id: Option<String> },
}

pub struct ConnectionContext {
    state: ConnectionState,
    client_id: String,
    ws_id: String,
    user: UserState,
    auth: Option<Auth>,
    profile_id: Option<String>,
    base_cookie: Option<String>,
    protocol_version: u32,
    revision: u32,
    revalidate_at: Option<i64>,
    insertion_order: u32,
    query_context: ConnectionFetchContext,
    mutate_context: ConnectionFetchContext,
}

pub struct GroupAuthState {
    pinned_user: Option<UserState>,
    background_connection: Option<ConnectionSelector>,
    retransform_at: Option<i64>,
    maintenance_not_before_at: Option<i64>,
}
```

### 5.2 `registerConnection(selector, params, auth?)`

1. Remove any existing connection for this `clientID` (via `#removeConnection`).
2. Build `queryContext` and `mutateContext` from config:
   - `url`: from `config.query.url[0]` or `config.push.url[0]`
   - `allowedUrlPatterns`: compiled from config URLs
   - `headerOptions`: `customHeaders` (undefined initially), `origin` from
     params, `apiKey` from config, `allowedClientHeaders` from config,
     `cookie` from params if `forwardCookies` is enabled.
3. Create connection with `state: 'provisional'`, `revision: 0`,
   `revalidateAt: undefined`, `insertionOrder: ++counter`.
4. Store connection.
5. `refreshBackgroundConnectionContext()` (no preferred).
6. `updateBackgroundRetransformDeadline(false)`.
7. Return connection.

### 5.3 `initConnection(selector, body)`

1. Get existing connection (must exist).
2. Update `queryContext.url` if `body.userQueryURL` is set.
3. Update `queryContext.headerOptions.customHeaders` if `body.userQueryHeaders` is set.
4. Update `mutateContext.url` if `body.userPushURL` is set.
5. Update `mutateContext.headerOptions.customHeaders` if `body.userPushHeaders` is set.
6. Increment `revision`.
7. Demote connection (set `state: 'provisional'`, `revalidateAt: undefined`).
8. Return demoted connection.

### 5.4 `updateAuth(selector, body)` — async

1. Get existing connection (must exist).
2. Call `resolveAuth(lc, currentAuth, userId, body.auth, validateLegacyJWT)`.
3. If auth changed: demote connection (set new auth, increment revision).
4. If auth is the same object: return unchanged.
5. If auth is different but equal: store new auth (no demotion).
6. Return connection.

### 5.5 `validateConnection(selector, revision, validation)`

1. Get connection. If not found: return `None`.
2. If `connection.revision !== revision`: log debug, return `None` (stale).
3. If `validation.kind === 'server-validated'`:
   - Check `connection.user.id === validation.validatedUserID`.
   - If mismatch: throw `ProtocolError(Unauthorized, 'Connection userID does
not match validated server userID.')`.
4. Determine `incomingUserState`:
   - Server-validated: use `validatedUserID`.
   - Client-fallback: use `connection.user`.
5. If `group.pinnedUser` is set and `pinnedUser.id !== incomingUserState.id`:
   - Throw `ProtocolError(Unauthorized, 'Client groups are pinned...')`.
6. If `group.pinnedUser` is unset: set it to `incomingUserState`.
7. Set connection `state: 'validated'`, `revalidateAt: now + interval`.
8. `refreshBackgroundConnectionContext(validatedConnection)`.
9. `updateBackgroundRetransformDeadline(false)`.
10. Return `{connection, group}`.

### 5.6 `failConnection(selector, revision)`

1. Call `#removeConnection(selector, revision)`.
2. If revision mismatch: log debug, return `None`.
3. Remove connection, refresh background, update retransform deadline.
4. Return removed connection.

### 5.7 `closeConnection(selector)`

1. Call `#removeConnection(selector)` (no revision check).
2. Return removed connection.

### 5.8 `markBackgroundRetransformSuccess(selector, revision)`

1. Get background connection. If none: return.
2. If `backgroundConnection.clientID/wsID/revision` doesn't match: return.
3. `updateBackgroundRetransformDeadline(true)` (reset).

### 5.9 `setSharedRetransformReady(ready)`

1. If unchanged: return.
2. Set `sharedRetransformReady = ready`.
3. `updateBackgroundRetransformDeadline(true)`.

### 5.10 `deferMaintenance(kind)`

1. Get interval for kind (revalidate or retransform).
2. If no interval configured: return.
3. Set `maintenanceNotBeforeAt = max(current, now + interval)`.

### 5.11 `planMaintenance()`

1. Initialize `dueRevalidations = []`, `earliestDeadlineAt = retransformAt`.
2. For each validated connection with `revalidateAt`:
   - If `revalidateAt <= now`: add to `dueRevalidations`.
   - `earliestDeadlineAt = min(earliestDeadlineAt, revalidateAt)`.
3. `dueRetransform = retransformAt !== undefined && retransformAt <= now`.
4. If `maintenanceNotBeforeAt > now && earliestDeadlineAt !== undefined`:
   - Return empty due lists, `earliestDeadlineAt = max(earliestDeadlineAt, maintenanceNotBeforeAt)`.
5. Sort `dueRevalidations` by `insertionOrder` (ascending), then `wsID`
   (ascending).
6. Return `{dueRevalidations, dueRetransform, earliestDeadlineAt}`.

### 5.12 Background connection selection (`#refreshBackgroundConnectionContext`)

- If a `preferred` validated connection is provided:
  - If it's already the background: return.
  - If there's already a validated background: return (sticky).
  - Otherwise: promote preferred to background.
- If no preferred (or preferred is not validated):
  - If current background is still validated: return.
  - Otherwise: find the newest validated connection (by `insertionOrder`
    descending, then `wsID` descending) and promote it.
  - If none found: clear background.

### 5.13 Retransform deadline (`#updateBackgroundRetransformDeadline(reset)`)

- If no background connection, or no retransform interval, or
  `!sharedRetransformReady`: clear `retransformAt`.
- If `reset || retransformAt === undefined`: set
  `retransformAt = now + retransformIntervalMs`.
- Otherwise: preserve existing deadline.

### 5.14 Helper: `#removeConnection(selector, revision?)`

1. Get connection. If not found: return `None`.
2. If revision is specified and doesn't match: return `None`.
3. Delete from map.
4. `refreshBackgroundConnectionContext()` (no preferred).
5. `updateBackgroundRetransformDeadline(false)`.
6. Return removed connection.

### 5.15 Helper: `#demoteConnection(connection)`

1. Set `state: 'provisional'`, `revalidateAt: undefined`.
2. Store.
3. `refreshBackgroundConnectionContext()`.
4. `updateBackgroundRetransformDeadline(false)`.
5. Return demoted connection.

### 5.16 Comparison functions

- `compareByInsertionOrder`: ascending by `insertionOrder`, then `wsID`
  ascending.
- `comparePreferredValidatedConnection`: descending by `insertionOrder`,
  then `wsID` descending.

---

## Component 6: ViewSyncer (`view_syncer.rs`)

**Ports:** `view-syncer.ts` (~2940 LOC) — the core.

This is the largest component. Every method and behavior is documented below.

### 6.1 Constructor (lines 344-404)

Parameters:

- `config: NormalizedZeroConfig`
- `lc: LogContext`
- `shard: ShardID`
- `taskID: string`
- `clientGroupID: string`
- `cvrDb: PostgresDB`
- `pipelineDriver: PipelineDriverLike` (RustIVMDriver or PipelineDriver)
- `versionChanges: Subscription<ReplicaState>`
- `drainCoordinator: DrainCoordinator`
- `slowHydrateThreshold: number`
- `inspectorDelegate: InspectorDelegate`
- `connContextManager: ConnectionContextManager`
- `customQueryTransformer: CustomQueryTransformer | undefined`
- `runPriorityOp: <T>(lc, description, op) => Promise<T>`
- `keepaliveMs = 5000`
- `setTimeoutFn = setTimeout`
- `pgUri?: string`

Initialization:

- Create `CVRStore` with `failService = () => stateChanges.cancel()`.
- If `RUST_CVR && RustIVMDriver && pgUri`: call `engine.setCvrStore(pgUri,
cvrSchema(shard), clientGroupID, taskID)`.
- Call `keepalive()`.

**Rust:** The `RustViewSyncer` struct holds all of these. `CVRStoreHandle` is
created directly (no TS CVRStore). `engine` is the `rust-ivm` Engine.
`connContextManager` is `ConnectionContextManager` (Rust). No lock needed —
the CG thread is single-threaded.

### 6.2 Metrics (lines 265-330)

All metrics use `getOrCreateCounter` / `getOrCreateLatencyHistogram` /
`getOrCreateUpDownCounter` with these names:

- `sync/active-clients` (UpDownCounter) — tagged with `protocol.version`
- `sync/hydration` (Counter)
- `sync/hydration-time` (LatencyHistogram)
- `sync/advance-time` (LatencyHistogram)
- `sync/query.transformations` (Counter)
- `sync/query.transformation-time` (LatencyHistogram)
- `sync/lock-wait-time` (LatencyHistogram)
- `sync/pipeline-resets` (Counter) — tagged with `reason`
- `sync/query.row-set-signature-drifts` (Counter)
- `sync/active-client-groups` (Gauge)

**Rust:** Use `opentelemetry` crate with the same metric names and types.

### 6.3 `#runInLockWithCVR(fn)` (lines 411-475)

1. Record lock wait time.
2. Acquire `#lock`.
3. If `!stateChanges.active`: clear expired queries timer, throw
   `ProtocolError(Rehome, 'Reconnect required')` at `'info'` level.
4. Check shutdown conditions (`#checkForShutdownConditionsInLock`):
   - If clients > 0: return false (common case).
   - If clients === 0: wait for `cvrStore.flushed()`, then check keepalive.
   - If still no clients: return true (shutdown).
   - If shutting down: reject `#initialized`, cancel `#stateChanges`, return
     from `fn`.
5. If `#cvr` is undefined: load CVR from store, set `#ttlClock` and
   `#ttlClockBase`.
6. If `#cvr` is defined: update `ttlClock` via `#getTTLClock(now)`.
7. Call `fn(lc, cvr)`.
8. On error: clear `#cvr = undefined`, rethrow.
9. In `finally`: `#scheduleAuthMaintenance(lc)`.

**Rust:** No lock needed — CG thread is single-threaded. The "lock" is
implicit. `stateChanges` is a channel. CVR load via `block_on`.

### 6.4 `run()` (lines 484-575)

1. Wait for `readyState()` — race between `#initialized.promise` and
   `drainCoordinator.draining`.
2. If draining: call `stop()`, return.
3. Loop: `for await (const {state} of stateChanges)`:
   a. If `drainCoordinator.shouldDrain()`: break (elective drain).
   b. Assert `state === 'version-ready'`.
   c. `#runInLockWithCVR`:
   - If `!pipelines.initialized()`: `pipelines.init(clientSchema)`.
   - If `cvr.replicaVersion > pipelines.replicaVersion` and
     `cvr.version.stateVersion !== '00'`: throw `ClientNotFoundError`.
   - If `pipelinesSynced`:
     - `result = #advancePipelines(lc, cvr)`.
     - If `'success'`: return.
     - If `ResetPipelinesSignal`: record `pipelineResets.add(1, {reason})`,
       `pipelines.reset(clientSchema)`, `pipelinesSynced = false`,
       `setSharedRetransformReady(false)`.
   - `version = pipelines.advanceWithoutDiff()`.
   - If `version < cvr.version.stateVersion`: log, return (wait).
   - `driftedQueryIDs = #hydrateUnchangedQueries(lc, cvr)`.
   - `#syncQueryPipelineSet(lc, cvr, 'missing', undefined, driftedQueryIDs)`.
   - `pipelinesSynced = true`.
   - `setSharedRetransformReady(true)`.
4. After loop (drained): `drainCoordinator.drainNextIn(totalHydrationTimeMs())`.
5. `#cleanup()`.
6. On error: log, `#cleanup(e)`.
7. In `finally`: wait for `cvrStore.flushed()`, resolve `#stopped`.

**Rust:** The CG thread's main loop. `stateChanges` is the channel from
`router.rs`. Each `AdvanceNotification` triggers the advance/init logic.

### 6.5 `initConnection(selector, msg)` (lines 799-920)

1. Get `connCtx` from `connContextManager.mustGetConnectionContext`.
2. Create `downstream` Subscription with cleanup:
   - On error: log.
   - On clean: log.
   - Call `#deleteClientDueToDisconnect(clientID, newClient)`.
   - `activeClients.add(-1, {protocol.version})`.
3. `activeClients.add(1, {protocol.version})`.
4. If first connection: set `#ttlClockBase = now`.
5. Create `ClientHandler(lc, id, clientID, wsID, shard, baseCookie, downstream)`.
6. If existing client for this `clientID`: close it
   (`'replaced by wsID: ...'`).
7. Store new client in `#clients`.
8. If `RUST_CVR && RustIVMDriver`:
   - `engine.registerClient(clientID, wsID, id, shard, baseCookie,
pushFn, failFn, cancelFn)`.
   - `pushFn`: `downstream.push(msg)` with catch.
   - `failFn`: `downstream.fail(wrapWithProtocolError(new Error(err)))`.
   - `cancelFn`: `downstream.cancel()`.
9. Async (not blocking return):
   - `#runInLockForClient(selector, msg, fn, newClient)`: - If `cvr.clientSchema === null && !msg.clientSchema`: throw
     `ProtocolError(InvalidConnectionRequest, 'must include client schema')`. - `#validateConnection(connCtx)` — if fails, return. - `#handleConfigUpdate(lc, clientID, msg, cvr, 'all',
profileID ?? 'cg${id}', connCtx)`. - Resolve `#initialized`.
10. Return `downstream` (the Subscription/Source) synchronously.

**Rust:** `ClientHandler` created directly (Rust struct). `DirectWebSocketSink`
replaces the TSFN. The `downstream` is a channel. `registerClient` is not
needed — the `ClientHandler` is directly accessible on the CG thread.

### 6.6 `changeDesiredQueries(selector, msg)` (lines 921-933)

1. `#runInLockForClient(selector, msg, (lc, clientID, msg, cvr) =>
#handleConfigUpdate(lc, clientID, msg, cvr, 'missing', undefined,
connContextManager.mustGetConnectionContext(selector)))`.

### 6.7 `updateAuth(selector, msg, authRevisionChanged)` (lines 935-970)

1. `#runInLockForClient(selector, msg, async (lc, clientID, _, cvr) => ...)`:
   - If `!authRevisionChanged`: log debug, return.
   - Get `connCtx`.
   - If `!pipelinesSynced`: `#validateConnection(connCtx)` — if fails, return.
   - `#handleConfigUpdate(lc, clientID, {}, cvr, 'all', undefined, connCtx)`.

### 6.8 `deleteClients(selector, msg)` (lines 972-986)

1. `#runInLockForClient(selector, [msg[0], {deleted: msg[1]}], (lc, clientID,
msg, cvr) => #handleConfigUpdate(lc, clientID, msg, cvr, 'missing',
undefined, connContextManager.mustGetConnectionContext(selector)))`.
2. Return `deletedClientIDs ?? []`.

### 6.9 `#runInLockForClient(selector, msg, fn, newClient?)` (lines 1155-1240)

1. If `newClient || !clients.has(clientID)`: update `lastConnectTime`.
2. Acquire lock via `#runInLockWithCVR`:
   a. Get `client = clients.get(clientID)`.
   b. If `client?.wsID !== wsID`: log, return (mismatched wsID).
   c. Get `connCtx = connContextManager.getConnectionContext(selector)`.
   d. If `newClient`: assert `newClient === client`, call
   `checkClientAndCVRVersions(client.version(), cvr.version)`.
   e. If no client: log warn.
   f. Call `fn(lc, clientID, body, cvr)`.
3. On error:
   - Log at `getLogLevel(e)`.
   - `connContextManager.failConnection(selector, connCtx.revision)`.
   - If `client`: `client.fail(e)`.
   - If no client: rethrow.

### 6.10 `#handleConfigUpdate` (lines 1234-1400)

This is an arrow function property (not a method) — preserves `this` binding.

1. `deletedClientIDs = []`, `deletedClientGroupIDs = []`.
2. `cvr = #updateCVRConfig(lc, cvr, clientID, customQueryTransformMode,
connCtx, async updater => { ... })`.
3. In the updater callback:
   a. If `clientSchema`: `updater.setClientSchema(lc, clientSchema)`.
   b. If `profileID`: `updater.setProfileID(lc, profileID)`.
   c. For each `desiredQueriesPatch`:
   - `'put'`: `updater.putDesiredQueries(clientID, [patch])`.
   - `'del'`: `updater.markDesiredQueriesAsInactive(clientID, [hash], ttlClock)`.
   - `'clear'`: `updater.clearDesiredQueries(clientID)`.
     d. If `activeClients`: find clients not in active set, mark for deletion.
     e. If `deleted.clientIDs`: add to deletion set.
     f. For each `cid` to delete: `updater.deleteClient(cid, ttlClock)`,
     collect patches.
     g. If `deleted.clientGroupIDs`: log debug (deprecated, ignored).
     h. Return patches.
4. After `#updateCVRConfig` returns:
   a. If `cmpVersions(cvr.version, newCVR.version) < 0`:
   - Poke clients at `cvr.version` with config patches.
   - `startPoke(getClients(cvr.version), newCVR.version)`.
   - For each patch: `pokers.addPatch(patch)`.
   - `pokers.end(newCVR.version)`.
     b. If `pipelinesSynced`: `#syncQueryPipelineSet(lc, newCVR,
customQueryTransformMode, connCtx)`.
5. If `deletedClientIDs.length > 0`:
   - For each deleted client: if `clients.has(cid)`: `client.close(...)`,
     `clients.delete(cid)`, `inspectorDelegate.removeQuery(...)` for internal
     queries, `queryReplacements.delete(...)`.
   - `activeClients.add(-deletedClientIDs.length, {protocol.version})`.
6. Return `deletedClientIDs`.

### 6.11 `#updateCVRConfig` (lines 1089-1130)

1. Create `CVRConfigDrivenUpdater(cvrStore, cvr, shard)`.
2. `updater.ensureClient(clientID)`.
3. `patches = fn(updater)` (the callback from #handleConfigUpdate).
4. `#cvr = #flushUpdater(lc, updater)`.
5. If version bumped: poke clients at old version with config patches.
6. If `pipelinesSynced`: `#syncQueryPipelineSet(...)`.
7. Return `#cvr`.

### 6.12 `#syncQueryPipelineSet` (lines 1722-2260)

This is the hydrate path. Already partially ported as `HydrateAndSyncTask`.

Full TS behavior:

1. Start span.
2. If `ttlClock === undefined`: set from `cvr.ttlClock`.
3. Compute `ttlClock = #getTTLClock(now)`.
4. Group CVR queries: custom queries vs everything else.
5. Transform custom queries if `customQueryTransformMode === 'all'` or
   if any are missing from pipelines:
   - Call `customQueryTransformer.transform(connCtx, customQueries)`.
   - Handle `TransformFailed` errors.
   - Handle application errors (send to client via poke).
   - Record `queryTransformations.add(transformed.length)`,
     `queryTransformationTime.recordMs(elapsed)`.
   - For each transformed query: store in `transformedCustomQueries`.
6. Compute `gotQueries` (existing in pipelines) vs `desiredQueries` (from CVR).
7. Compute `addQueries` (desired but not in pipelines) and `removeQueries`
   (in pipelines but not desired, or hash changed).
8. Thrashing detection: if a query was recently removed and is being re-added
   within 60s, log warning.
9. For `removeQueries`: `pipelines.removeQuery(q.id)`,
   `inspectorDelegate.removeQuery(q.id)`, `queryReplacements.delete(q.id)`.
10. **RUST_CVR path** (if enabled): call `engine.hydrateAndSync(...)`, process
    result (TTL, cache, metrics, catchup), return.
11. **TS fallback path**:
    a. Create `CVRQueryDrivenUpdater` with `rowSetSignatureProvider`.
    b. If drifted queries: force `configVersion` bump.
    c. `trackQueries()`, `startPoke()`.
    d. Stream row changes through `#processChanges`.
    e. `#flushUpdater()`, `#catchupClients()`, `pokers.end()`.
    f. Record metrics per query.

### 6.13 `#advancePipelines` (lines 2444-2570)

Already partially ported as `AdvanceAndSyncTask`.

Full TS behavior:

1. Start span, record start time.
2. **RUST_CVR path**: call `engine.advanceAndSync(...)`, process result,
   return `'success'` or `ResetPipelinesSignal`.
3. **TS fallback path**:
   a. `advanceResult = pipelines.advance(timer)`.
   b. If `ResetPipelinesSignal`: return it.
   c. Create `CVRQueryDrivenUpdater` with `rowSetSignatureProvider`.
   d. `startPoke(getClients(cvr.version), version)`.
   e. `#processChanges(...)`.
   f. `#catchupClients(...)`.
   g. `#flushUpdater()`.
   h. `pokers.end(finalVersion)`.
   i. Record `transactionAdvanceTime.recordMs(totalProcessTime)`.
   j. Return `'success'`.

### 6.14 `#hydrateUnchangedQueries` (lines 1414-1621)

Runs at init when `pipelinesSynced === false` and `version >= cvr.version.stateVersion`.

1. Get `gotQueries` from CVR (all non-deleted queries).
2. Transform custom queries (all mode).
3. For each got query:
   a. If already in pipelines: skip.
   b. Add to pipelines via `pipelines.addQuery(...)`.
   c. Consume the iterable (count rows, yield for time slicing).
   d. Record `hydrations.add(1)`, `hydrationTime.recordMs(elapsed)`.
   e. `addQueryMaterializationServerMetric(queryID, elapsed)`.
   f. `inspectorDelegate.addQuery(queryID, ast)`.
   g. **Drift detection**: compare `pipelines.rowSetSignature(queryID)` with
   `cvr.queries[queryID].rowSetSignature`.
   - If mismatch: `rowSetSignatureDrifts.add(1)`,
     `pipelines.removeQuery(queryID)`, add to `driftedQueryIDs`.
4. Return `driftedQueryIDs`.

### 6.15 `#catchupClients` (lines 2267-2350)

1. Get all clients, `startPoke(clients, cvr.version)`.
2. Compute `catchupFrom = min(client versions, cvr.version)`.
3. `rowPatches = cvrStore.catchupRowPatches(lc, catchupFrom, cvr, current, excludeHashes)`.
4. `configPatches = cvrStore.catchupConfigPatches(lc, catchupFrom, cvr, current)`.
5. Add error handler to `configPatches` promise.
6. For each row patch:
   - If no refCounts: `{type: 'row', op: 'del', id}`.
   - If refCounts: get row from `pipelines.getRow(table, rowKey)`,
     extract `contentsAndVersion`, `{type: 'row', op: 'put', id, contents}`.
   - `pokers.addPatch({patch, toVersion})`.
7. Await `configPatches`, for each: `pokers.addPatch(patch)`.
8. If no external `usePokers`: `pokers.end(cvr.version)`.

### 6.16 `#processChanges` (lines 2185-2260)

Batched de-duplication of row changes. Already ported as `ChangeProcessor`.

1. Batch up to `CURSOR_PAGE_SIZE` (10000) changes.
2. De-dupe by row key (last write wins).
3. Strip `_0_version` from row contents.
4. Merge refCounts (ADD=+1, EDIT=0, REMOVE=-1).
5. Call `updater.received(batch, existingRows)`.
6. For each patch from `received()`: `pokers.addPatch(patch)`.
7. Yield for time slicing between batches.

### 6.17 TTL Clock (lines 1006-1085)

- `#getTTLClock(now)`: Must be **synchronous** (no await). Computes
  `delta = now - ttlClockBase`, `ttlClock += delta`, `ttlClockBase = now`.
  Asserts `ttlClock <= now`.
- `#startTTLClockInterval(lc)`: Stop existing, start new timer at
  `TTL_CLOCK_INTERVAL` (60000ms). On fire: `#updateTTLClockInCVRWithoutLock`,
  restart interval.
- `#updateTTLClockInCVRWithoutLock(lc)`: Call `#getTTLClock(now)`, then
  `cvrStore.updateTTLClock(ttlClock, now)` (fire-and-forget promise).
- `#flushUpdater`: Gets TTL clock, calls `updater.flush(lc, lastConnectTime,
now, ttlClock)`. If flushed: restart TTL interval.

### 6.18 Auth Maintenance (lines 710-800)

- `#scheduleAuthMaintenance(lc)`: Stop existing timer. Call
  `connContextManager.planMaintenance()`. If `earliestDeadlineAt` is set,
  schedule timer at `max(0, deadline - now)`.
- `#runAuthMaintenance(lc, cvr)`:
  1. `plan = planMaintenance()`.
  2. If no due work: log, return.
  3. For each `dueRevalidations`: `#validateConnection(connCtx)`.
     - On `TransformFailed`: `deferMaintenance('revalidate')`, return.
  4. Re-plan. If `dueRetransform`: `#runBackgroundRetransform(lc)`.
- `#runBackgroundRetransform(lc)`:
  1. Get background connection. If none: return.
  2. Attempt retransform via `#syncQueryPipelineSet(lc, cvr, 'all', connCtx)`.
  3. On auth error: fail connection, find replacement, retry.
  4. On TransformFailed: `deferMaintenance('retransform')`, return.
- `#validateConnection(connCtx)`:
  1. If `customQueryTransformer`: call `validate(connCtx)`.
  2. If TransformFailed: throw.
  3. Call `connContextManager.validateConnection(...)`.
  4. On auth error: `#failMaintenanceConnection(connCtx, e)`, return false.
  5. Return true.

### 6.19 Expired Query Eviction (lines 595-612, 2760-2790)

- `#removeExpiredQueries(lc, cvr)`:
  1. If `hasExpiredQueries(cvr)`: `#syncQueryPipelineSet(lc, cvr, 'missing')`.
  2. `#scheduleExpireEviction(lc, cvr)`.
- `#scheduleExpireEviction(lc, cvr)`:
  1. Stop existing timer.
  2. Compute `nextEvictionTime` from all queries' TTL + inactivatedAt.
  3. If no eviction needed: return.
  4. Schedule timer at `nextEvictionTime - now`.
  5. On fire: `#runInLockWithCVR(#removeExpiredQueries)`.
- `expired(ttlClock, q)`: A query is expired when ALL clients have
  `inactivatedAt` set AND `inactivatedAt + clampTTL(ttl) <= ttlClock` for all.
  Internal queries never expire.
- `clampTTL(ttl)`: `Math.min(Math.max(ttl, 0), MAX_TTL_MS)` where
  `MAX_TTL_MS = 5_000_000`.

### 6.20 Shutdown / Cleanup (lines 631-700, 2710-2740)

- `keepalive()`: If `!stateChanges.active`: return false. Set
  `keepAliveUntil = now + keepaliveMs`. Return true.
- `#scheduleShutdown(delayMs)`: Set timer (if not already set) to queue empty
  lock task (triggers shutdown check).
- `#checkForShutdownConditionsInLock()`:
  1. If `clients.size > 0`: return false.
  2. `await cvrStore.flushed(lc)`.
  3. If `now <= keepAliveUntil`: schedule shutdown, return false.
  4. Return `clients.size === 0`.
- `#deleteClientDueToDisconnect(clientID, client)`:
  1. `connContextManager.closeConnection({clientID, wsID})`.
  2. If `clients.get(clientID) === client`: delete, unregister from Rust
     engine, if no clients left: update TTL clock, stop expire timer,
     schedule shutdown.
- `stop()`: `setSharedRetransformReady(false)`, reject `#initialized`,
  cancel `#stateChanges`. Return `#stopped.promise`.
- `#cleanup(err?)`:
  1. `setSharedRetransformReady(false)`.
  2. Stop TTL, expire, auth maintenance timers.
  3. For each client: `fail(err)` or `close(reason)`.
  4. `await lock.withLock(() => {})` (wait for pending work).
  5. `await pipelines.destroy()`.
- In `run()` finally: `await cvrStore.flushed(lc)`, resolve `#stopped`.

### 6.21 `inspect(selector, msg)` (lines 2750-2800)

- `handleInspect(lc, msg, inspectorDelegate, pipelines, connCtx, id)`.
- Checks authentication via `inspectorDelegate.isAuthenticated(id)`.
- Returns query metrics, ASTs, server metrics.

### 6.22 `#getClients(atVersion?)` (lines 1245-1252)

- If `atVersion`: filter clients where `client.version() === atVersion`.
- If no version: return all clients.

### 6.23 `checkClientAndCVRVersions` (lines 2820-2840)

- If `cvr === {stateVersion: '00'}` and `client > {stateVersion: '00'}`:
  throw `ClientNotFoundError('Client not found')`.
- If `client > cvr`: throw `ProtocolError(InvalidConnectionRequestBaseCookie)`.

### 6.24 TimeSliceTimer (lines 2850-2940)

- `start()`: yield, then `startWithoutYielding()`.
- `startWithoutYielding()`: reset total, start lap.
- `yieldProcess(msg?)`: stop lap, yield (setImmediate via timeSliceQueue),
  start lap.
- `stop()`: stop lap, return total.
- `totalElapsed()`: total + current lap if running.
- `elapsedLap()`: current lap elapsed.
- `yieldProcess` uses a global `Lock` as a queue — one time slice per event
  loop iteration.
- **Rust:** Not needed — the CG thread doesn't yield. Time slicing is
  implicit in the streaming callback.

---

## Component 7: Inspector Delegate (`inspector.rs`)

**Ports:** `inspector-delegate.ts` (~171 LOC)

### Behaviors to preserve exactly

- `ServerMetrics`: TDigest for `query-materialization-server` and
  `query-update-server`.
- `addMetric(metric, value, queryID)`:
  - `query-materialization-server`: store as `perQueryHydrateMs[queryID] = value`.
  - `query-update-server`: create/append to TDigest per query.
  - Add to global metrics.
- `getMetricsJSONForQuery(queryID)`: return `{query-hydration-server-ms,
query-update-server}` or null.
- `getMetricsJSON()`: global metrics as JSON.
- `getASTForQuery(queryID)`: return stored AST.
- `removeQuery(queryID)`: delete all per-query data.
- `addQuery(queryID, ast)`: store AST.
- `isAuthenticated(clientGroupID)`: true if dev mode or in
  `authenticatedClientGroupIDs` set.
- `setAuthenticated(clientGroupID)`: add to set.
- `clearAuthenticated(clientGroupID)`: remove from set.
- `transformCustomQuery(name, args, ctx)`: call customQueryTransformer with
  a single query, return transformed AST.

**Rust:** TDigest implementation needed (or approximate). The
`authenticatedClientGroupIDs` set is process-global (shared across CGs).

---

## Component 8: Inspect Handler (`inspect_handler.rs`)

**Ports:** `inspect-handler.ts` (~215 LOC)

- `handleInspect(lc, msg, inspectorDelegate, pipelines, connCtx, id)`:
  - Parse inspect request.
  - Check authentication.
  - Return query list, metrics, ASTs.
  - Support `transformCustomQuery` for single-query inspection.

---

## Component 9: Mutagen / Pusher Integration

**Stays in TS** as a separate process. The Rust syncer forwards `push`
messages to the mutagen service via HTTP.

### Behaviors to preserve

- Per-connection mutation lock (ordering within a connection).
- `mutations[0].type === 'custom'` → forward to pusher.
- CRUD mutations → forward to mutagen.
- `ackMutationResponses` → forward to pusher.
- `deleteClients` → notify pusher of deleted client IDs.
- Mutagen/Pusher ref counting per CG (created on first connection, destroyed
  when ref count hits 0).
- **Rust:** HTTP client to the mutagen/pusher service. Or embed mutagen in
  Rust (future). For now, HTTP forwarding.

---

## Component 10: DirectWebSocketSink (`ws_sink.rs`)

Replaces `NapiWebSocketSink`. Direct write to the WebSocket.

```rust
pub struct DirectWebSocketSink {
    tx: tokio::sync::mpsc::Sender<tungstenite::Message>,
}

impl WebSocketSink for DirectWebSocketSink {
    fn push(&self, msg: serde_json::Value) {
        let text = serde_json::to_string(&msg).unwrap();
        let _ = self.tx.blocking_send(Message::Text(text));
    }
    fn fail(&self, err: String) {
        let _ = self.tx.blocking_send(Message::Close(...));
    }
    fn cancel(&self) {
        let _ = self.tx.close();
    }
}
```

The tokio runtime drives the WS I/O. The CG thread writes to the channel.
Backpressure is natural (bounded channel).

---

## Crate Structure

```
packages/rust-syncer/
├── Cargo.toml
├── src/
│   ├── main.rs                  # binary entry point, config parsing
│   ├── ws_server.rs             # Component 1: WebSocket server
│   ├── connection.rs            # Component 2: Connection
│   ├── connect_params.rs        # Component 3: Connect params
│   ├── message_handler.rs       # Component 4: Message routing
│   ├── connection_context.rs    # Component 5: Context manager
│   ├── view_syncer.rs           # Component 6: ViewSyncer dispatch loop
│   ├── drain.rs                 # Drain coordinator
│   ├── inspector.rs             # Component 7: Inspector delegate
│   ├── inspect_handler.rs       # Component 8: Inspect handler
│   ├── ws_sink.rs               # Component 10: DirectWebSocketSink
│   ├── protocol.rs              # Zero protocol serde types
│   ├── auth.rs                  # JWT validation
│   ├── notify.rs                # Change-streamer notification endpoint
│   ├── http_server.rs           # axum /statz, /heapz
│   ├── metrics.rs               # OpenTelemetry metrics
│   └── config.rs                # Parse zero config from env/args
└── tests/
    └── integration_test.rs
```

**Dependencies:**

- `rust-ivm` (engine, snapshotter, planner)
- `rust-cvr` (CVR, updater, store, client handler, change processor)
- `tokio` (async runtime)
- `tokio-tungstenite` (WebSocket)
- `axum` (HTTP server)
- `sqlx` (Postgres)
- `rusqlite` (SQLite replica)
- `jsonwebtoken` (JWT)
- `opentelemetry` (metrics)
- `serde` / `serde_json` (protocol)
- `tracing` (logging)

---

## Threading Model

| Component              | Thread                         | Why                          |
| ---------------------- | ------------------------------ | ---------------------------- |
| WS accept              | Tokio runtime                  | I/O multiplexing             |
| WS read                | Tokio runtime                  | I/O                          |
| WS write               | Tokio runtime                  | I/O                          |
| HTTP server            | Tokio runtime                  | /statz, /heapz, /notify      |
| CG dispatch loop       | Dedicated OS thread            | No event loop, no GC         |
| Engine graph           | CG thread                      | Single-threaded (Rc/RefCell) |
| CVR updater            | CG thread                      | Pure computation             |
| CVRStore flush         | CG thread → block_on(tokio)    | PG I/O edge                  |
| RowRecordCache         | CG thread → block_on(tokio)    | PG I/O edge                  |
| Poke body assembly     | CG thread                      | Same thread as engine        |
| WS push (poke frames)  | CG thread → channel → tokio    | WS I/O edge                  |
| Change notification    | Tokio → channel → CG thread    | Edge: HTTP recv              |
| Auth JWT validation    | Tokio runtime                  | HTTP/JWKS fetch              |
| TTL timer              | CG thread (std::thread::sleep) | No event loop                |
| Expire timer           | CG thread                      | No event loop                |
| Auth maintenance timer | CG thread                      | No event loop                |

**Zero napi crossings. Zero TSFN calls. Zero event loop involvement.**

---

## Process Model

```
main.ts (ProcessManager) — still TS
├── change-streamer (TS) — notifies via HTTP POST /notify/:cg_id
├── replicator (TS)
├── reaper (TS)
└── syncer
    ├── ZERO_SYNCER=ts (default): TS syncer worker
    └── ZERO_SYNCER=rust: rust-syncer binary
```

The Rust syncer binary receives:

- Port to listen on
- Replica SQLite file path
- CVR PG connection string
- Auth config (JWK/JWKS/secret)
- Task ID
- Shard config
- Mutagen/pusher URLs (for mutation forwarding)
- Change-streamer notification URL

It sends `['ready', {ready: true}]` to parent when initialized.

---

## Gap Closure Summary

| Gap                                            | How closed                                                                                               |
| ---------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| 1. Two Rust stores                             | One `CVRStoreHandle` per CG, on the CG thread                                                            |
| 2. TS CVRStore used                            | `CVRStoreHandle` is the only store; TS `CVRStore` is dead code                                           |
| 3. Config-driven not on actor thread           | `handle_config_update()` on CG thread, same store + clients                                              |
| 4. Signature provider not wired                | `engine.row_set_signature()` passed as provider                                                          |
| 5. `#hydrateUnchangedQueries` not ported       | `hydrate_unchanged()` on CG thread                                                                       |
| 6. Catchup not ported                          | `catchup_clients()` on CG thread, `engine.get_row()` direct                                              |
| 7. PokeHandler Drop missing                    | Add `impl Drop for PokeHandler`                                                                          |
| 8. `send_query_transform_failed_error` missing | Add method to `ClientHandler`                                                                            |
| 9. TS tests not ported                         | Port as Rust integration tests                                                                           |
| 10. `rust-cvr/napi/` not cleaned up            | ✅ DONE (a5e502ad9): `rust-cvr/napi` + `rust-ivm/napi` deleted; TS hybrid wiring reverted to zero/v1.7.0 |

---

## What Does NOT Change

- `rust-cvr` crate core logic (updater, store, client handler, etc.)
- `rust-ivm` engine
- TS code (kept as dead code behind `ZERO_SYNCER=ts` flag)
- Change-streamer, replicator, reaper (separate TS processes)
- Zero-client library
- Zero protocol (wire format unchanged)
- CVR PG schema
- SQLite replica format

---

## Implementation Phases

### Phase 1: Protocol + WebSocket + Connect Params (3-4 days)

- Port zero-protocol schemas to `protocol.rs`
- Port `connect-params.ts` to `connect_params.rs`
- Implement `WsServer` (tokio-tungstenite)
- Implement `DirectWebSocketSink`
- Test: accept connection, parse params, send `connected`

### Phase 2: Connection + Message Handler (3-4 days)

- Implement `Connection` (message parsing, keepalive pong, error handling)
- Implement `MessageHandler` (all message types)
- Test: round-trip messages

### Phase 3: Connection Context Manager (3-4 days)

- Port full state machine (provisional → validated, group auth, maintenance)
- Port `DrainCoordinator`
- Test: connection lifecycle, auth validation, background selection

### Phase 4: ViewSyncer Dispatch Loop (7-10 days)

- Port `#runInLockWithCVR` → channel recv loop
- Port CVR load
- Port `#handleConfigUpdate` / `#updateCVRConfig` → `handle_config_update()`
- Port `#hydrateUnchangedQueries` → `hydrate_unchanged()`
- Port `#catchupClients` → `catchup_clients()`
- Wire `row_set_signature_provider`
- Port TTL clock, expire timer, auth maintenance
- Port metrics recording
- Port shutdown/cleanup
- Port inspector delegate + inspect handler
- Test: full dispatch loop

### Phase 5: HTTP + Notification + Process Integration (3-4 days)

- axum HTTP server (/statz, /heapz, /notify)
- Change-streamer notification → CG thread
- Process manager integration (launch binary, ready message)
- Graceful shutdown
- Test: end-to-end

### Phase 6: Cleanup + Testing (5-7 days)

- `impl Drop for PokeHandler`
- `send_query_transform_failed_error`
- Delete `rust-cvr/napi/` — ✅ DONE (a5e502ad9); also deleted `rust-ivm/napi` and reverted the TS rust-IVM hybrid wiring to zero/v1.7.0 (napi/rust-IVM lives on a separate branch)
- Port TS tests to Rust integration tests
- Parity testing: `ZERO_SYNCER=rust` vs `ZERO_SYNCER=ts`
