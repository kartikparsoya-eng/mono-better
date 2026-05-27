# Rocicorp Monorepo Instructions

## Architecture Overview

This monorepo contains **Zero** (real-time sync platform) and **Replicache** (client-side data layer), built as complementary technologies for building reactive, sync-enabled applications.

### Repo Structure

```
mono/
├── packages/          # 29 core packages (libraries and engines)
│   ├── zero-client    # Main Zero client (uses Replicache)
│   ├── zero-cache     # Server-side cache and sync engine
│   ├── zero-server    # Server-side mutations/queries
│   ├── zero-schema    # Schema definition builder
│   ├── zql            # IVM (Incremental View Maintenance) query engine and language
│   ├── replicache     # Core client-side sync library
│   └── shared         # Shared utilities and testing helpers
├── apps/              # 3 applications
│   ├── zbugs          # Reference app (React + Wouter + Zero + PostgreSQL)
│   ├── otel-proxy     # OpenTelemetry proxy
│   └── zql-viz        # Query visualization tool
├── tools/             # 5 development tools
└── prod/              # Production deployment (SST/Pulumi)
```

### Data Flow Architecture

Zero follows a **sync-first** model: client queries are reactive and automatically update when server data changes. ZQL queries are transformed to SQL on the server and results are incrementally maintained.

## Development Workflow

### Essential Commands

```bash
# Install and build everything
npm install && npm run build

# Run tests (uses vitest)
npm run test              # All tests
npm run test:watch        # Watch mode

# Type checking and linting
npm run check-types       # TypeScript across all packages
npm run lint              # oxlint with type-awareness
npm run format            # oxfmt formatting
```

**Always run `lint`, `format` and `check-types` after every change.**

### Package-Level Commands

Prefer package-level commands when possible. Each package supports: `test`, `check-types`, `lint`, `format`, `build`. e.g.:

```bash
npm --workspace=zero-client run format
npm --workspace=zero-cache run lint
npm --workspace=zero-server run check-types

# Run with coverage (prefer using this flag when possible)
npm --workspace=zero-client run test -- --coverage

# Run specific test file
npm --workspace=zero-client run test -- zero.test
```

### Zero Cache Development

```bash
# Start Zero cache server for local development
npm run start-zero-cache

# In zbugs app - start Zero cache with schema hot-reload
npm run zero-cache-dev
```

## Code Conventions

### TypeScript Patterns

- **Optional fields**: Always explicitly typed as `type | undefined` (not just `type?`)

  ```typescript
  // Correct
  interface User {
    name?: string | undefined;
  }

  // Incorrect
  interface User {
    name?: string;
  }
  ```

### Zero Schema Definition

Zero schemas use a builder pattern with method chaining:

```typescript
const user = table('user')
  .columns({
    id: string(),
    name: string().optional(),
    role: enumeration<Role>(),
  })
  .primaryKey('id');
```

### Testing Patterns

- Use **vitest** for all testing
- Tests are co-located with source files using environment-specific naming:
  - `.test.ts` - Standard tests (Node.js environment)
  - `.node.test.ts` - Node-specific tests (Replicache)
  - `.web.test.ts` - Browser tests (Replicache)
  - `.pg.test.ts` - PostgreSQL integration tests
- Multiple vitest configs for different environments (e.g., `vitest.config.pg-16.ts` for PostgreSQL tests)
- Test files automatically discovered by the root vitest config
- Prefer `test` over `it` for consistency
- Coverage is run with `v8` - use the `--coverage` flag to help write tests

### Import Patterns

- **DO NOT import from `mod.ts`**: Use direct relative paths instead

  ```typescript
  // Correct - use relative path
  import {helper} from './helper.ts';

  // Incorrect - don't import from mod.ts
  import {helper} from './mod.ts';
  ```

- **DO NOT use `import()` in type expressions**: Always use `import type` at the top of the file

  ```typescript
  // Correct - import type at the top
  import type {AST} from '../../../zero-protocol/src/ast.ts';
  import type {TTL} from './ttl.ts';

  abstract addServerQuery(ast: AST, ttl: TTL): void;

  // Incorrect - don't use import() in type expressions
  abstract addServerQuery(
    ast: import('../../../zero-protocol/src/ast.ts').AST,
    ttl: import('./ttl.ts').TTL,
  ): void;
  ```

- **DO NOT use dynamic imports (`await import()`) unless necessary**: Use standard static imports

  ```typescript
  // Correct - static import
  import {createBuilder} from '../../../zql/src/query/named.ts';

  // Incorrect - unnecessary dynamic import
  const {createBuilder} = await import('../../../zql/src/query/named.ts');
  ```

  Dynamic imports are only needed for:
  - Lazy-loading heavy modules
  - Conditional imports based on runtime conditions

- **AVOID re-exports that create cycles**: Re-exports can introduce circular dependencies between packages

  ```typescript
  // Incorrect - re-exporting from higher-level package
  // In zero-types/src/schema.ts:
  export type {Schema} from '../zero-schema/src/builder/schema-builder.ts';

  // Correct - import directly from the source
  // In your code:
  import type {Schema} from '../zero-types/src/schema.ts';
  ```

  **Package dependency hierarchy** (lower packages should not depend on higher ones):
  - `shared`, `zero-protocol`, `zero-types` (lowest level - pure types/utilities)
  - `zql`, `zero-schema` (mid level - can use types packages)
  - `zero-client`, `zero-server`, `zero-cache` (higher level - can use zql/schema)
  - `zero` (highest - re-exports for convenience, user-facing only)

- Re-exports are acceptable in **user-facing packages** for convenience (e.g., `packages/zero/src/mod.ts` → exports from `zero-client`, `zero-server`), but avoid re-exports between internal packages

## Database

### Zero + PostgreSQL

Zero is a streaming database:

- **PostgreSQL**: Source of truth for data
- **SQLite**: Server-side replica managed by `zero-cache`
- **Replicache**: Client-side store managed by `zero-client` and `replicache`, in IndexedDB by default

### Schema Migrations

- Use Drizzle for PostgreSQL schema management (`db-migrate`, `db-seed`)
- Zero schema definitions are separate from PostgreSQL schema
- Apps like zbugs demonstrate the connection between PostgreSQL tables and Zero schemas

## Go IVM Sidecar (experimental)

`zero-cache` can optionally offload IVM compute (advance + hydrate) to a
companion Go process called the "Go IVM sidecar". When enabled, the
PipelineDriver dispatches the hot path to the sidecar over a Unix socket
using MessagePack-RPC instead of running the TS operator tree inline.

### Configuration

All sidecar settings live under the `goSidecar` group of `zero-config.ts`
and are validated by the same valita schema as the rest of zero-cache.

- `ZERO_GO_SIDECAR_ENABLED=true` — enable the sidecar.
- `ZERO_GO_SIDECAR_SHADOW_MODE=true` — run both TS and Go paths and
  compare results. TS is source of truth; mismatches are logged at
  `error` level. Requires `ZERO_GO_SIDECAR_ENABLED=true`. Used to
  validate the sidecar before flipping to Go-primary.
- `ZERO_GO_SIDECAR_SHADOW_VERBOSE=true` — include full row contents in
  shadow-mode mismatch logs (default: redacted to type + queryID +
  rowKey for PII safety).
- `ZERO_GO_SIDECAR_BINARY_PATH=/path/to/go-ivm-sidecar` — path to the
  compiled binary. Default: `go-ivm-sidecar` (PATH lookup).
- `ZERO_GO_SIDECAR_DRIFT_AUDIT_INTERVAL_MS=60000` — in Go-primary mode
  (enabled=true, shadowMode=false), how often each PipelineDriver runs a
  sampled-shadow drift audit. The audit picks one random active query
  per interval, re-hydrates it on both TS and Go from the current
  snapshot, and compares. Mismatches are logged at error level and
  surfaced via the `ivm.drift-audit-mismatches` metric (paired with
  `ivm.drift-audit-runs` and `ivm.drift-audit-skips`). Set to `0` to
  disable. Has no effect in shadow mode (which audits every query).
  Audit `ok` events log at `debug` level — to verify the audit is
  actually firing (and not just `enabled`), bump `ZERO_LOG_LEVEL=debug`
  and look for `[shadow] drift-audit (queryID): TS and Go match`.
- `ZERO_GO_SIDECAR_DRIFT_AUDIT_SQL_GROUND_TRUTH=true` — within each
  drift-audit cycle, also run a raw SQL query on the snapshot's SQLite
  replica as a third opinion (in addition to the TS-audit comparison).
  Catches Go-vs-SQL set and content drift directly, which is more
  trustworthy than Go-vs-TS-audit alone (the TS audit pipeline has
  known boundary-drop edges). Defaults to `true`; set to `false` to
  skip the SQL re-query if it shows measurable replica load — the
  audit then falls back to the legacy TS-vs-Go set comparison only.
  Has no effect when the audit itself is disabled.
- `ZERO_GO_SIDECAR_EXTERNALLY_MANAGED=true` — opt into shared-sidecar
  mode (see "Shared sidecar mode" below). When true, the worker's
  `SidecarManager` skips spawn and binary-existence checks and just
  connects to `goSidecar.socketPath`. Requires `goSidecar.socketPath`
  to be set.
- `ZERO_GO_SIDECAR_SOCKET_PATH=/tmp/go-ivm-shared.sock` — explicit
  Unix-socket path for the sidecar. Used together with
  `externallyManaged=true`. Without it, each worker spawns its own
  sidecar at `/tmp/go-ivm-<pid>.sock`.

#### Sidecar-side env (read by the `go-ivm-sidecar` binary itself)

- `GO_IVM_PARALLEL_THRESHOLD=2` — min connection count per MemorySource
  at which `genPushAndWriteParallel` (per-pipeline goroutine fan-out)
  kicks in for an advance push. The engine default is **2** (lowered
  from the historical 4 because dashboard-shaped workloads typically
  had 2-3 queries per source per cg and never crossed the old
  threshold). Set higher to suppress fan-out when goroutine spawn cost
  dominates the actual push work (rarely useful — measure first).
  Empty / non-numeric values keep the engine default.
- `GO_IVM_PPROF_ADDR=127.0.0.1:6060` — when set, the sidecar opens a
  `net/http/pprof` endpoint at this address with block + mutex
  profiling enabled at sample rate 1. ~5% overhead; leave unset in
  prod. Used to capture CPU / alloc / block / mutex / heap profiles
  via `go tool pprof http://addr/debug/pprof/<type>`. See
  `go-ivm/PERF-REVIEW.md` § "How to reproduce the profile" for the
  capture recipe.

### Decoder / Row representation

The sidecar's `ivm.Row` is `map[string]Value` — faithful port of TS's
`Record<string, Value>` (`zero-protocol/src/data.ts`). Numeric coercion
happens at msgpack decode time via a custom `Row.DecodeMsgpack` that
coerces integer column values to `float64` inline. This replaced the
legacy post-decode reflection-walk normalize for Row data
(`walkForNumericNormalize` in `cmd/sidecar/main.go`), measurably cutting
total allocations by ~41% on the live-load profile while preserving
TS↔Go equivalence (verified by 96+ drift audits and two 30-minute
sustained-load soaks with 0 mismatches).

The walk is still applied to non-Row payloads (e.g.,
`builder.ValuePos.Value` AST literals) — those positions can carry
int* values from clients but don't dominate the alloc profile.

OTel traces from the sidecar use the standard OTLP env vars
(`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`,
`OTEL_SERVICE_NAME`). Tracing is disabled when no endpoint is set.

### Shared sidecar mode (`externallyManaged`)

By default each `zero-cache` syncer worker spawns its own sidecar
process. With `ZERO_NUM_SYNC_WORKERS=N`, you get N sidecars. Each
sidecar holds the engines for whichever client groups happen to be
routed to its worker, so as N rises the average client-groups-per-
sidecar drops — shrinking the inter-cg parallelism each sidecar can
sustain and adding N replicator subscriptions / restart machineries.

Shared-sidecar mode collapses all workers onto one external sidecar
process so all client groups colocate. Wire-up:

1. The deployment owner (typically the container entrypoint) spawns
   `/path/to/go-ivm-sidecar /tmp/go-ivm-shared.sock` as a background
   process and waits for the socket to appear.
2. Each zero-cache worker sets:
   ```
   ZERO_GO_SIDECAR_ENABLED=true
   ZERO_GO_SIDECAR_EXTERNALLY_MANAGED=true
   ZERO_GO_SIDECAR_SOCKET_PATH=/tmp/go-ivm-shared.sock
   ```
3. The `SidecarManager` in each worker connects (instead of spawning),
   verifies the protocol revision, and installs a 2-second
   `isConnected()` health-check ticker. On disconnect (the external
   sidecar crashed or restarted), the ticker routes through the same
   `#handleRestartTrigger` pipeline as the spawned-process path —
   incrementing the sliding-window failure counter, queuing
   `waitForRunning()` callers, and firing `onRestart` listeners when
   the reconnect succeeds.
4. The external owner is responsible for actually keeping the sidecar
   process alive; `SidecarManager` only reconnects.

`SidecarManager.stop()` in externallyManaged mode tears down the
client and the health-check ticker but **does not** kill the sidecar
process or unlink the socket — the owner does that.

Workers=1 + shared sidecar is the recommended pairing for Go-primary:
no replicator duplication, no V8 GC overhead from extra workers, and
no fragmentation of cg state across sidecars.

### Sidecar restart + fallback behavior

If the binary doesn't exist or fails its initial spawn, the
PipelineDriver dispatch falls through to the TS-native path. The Go
path is always optional; user-facing correctness depends only on the
TS path.

Once running, an in-process sidecar crash (SIGKILL, OOM, panic) is
recovered transparently to clients:

- `SidecarManager` respawns the binary under a sliding-window failure
  cap (`maxRestartsInWindow` over `restartWindowMs`, defaults 5 / 60s).
- `GoComputeBackend.waitForRunning()` queues in-flight RPCs across the
  restart window instead of failing them.
- The `onRestart` listener re-runs `init` + chunked `loadRows` + query
  re-registration before any further advance/hydrate touches Go.
- `advance()` calls in flight at the moment of the crash do NOT replay
  their diff after re-init (Go's new state already reflects it); they
  return empty `{changes, timings}` and clients miss exactly ONE delta
  from the IVM stream. Subsequent advances are correct.
- `#doInit` deduplicates concurrent calls so the onRestart listener and
  any retry path don't race on `loadRows` (which previously caused
  doubled row sets).
- Hydrate / `addQueriesStream` calls retry against the re-initialized
  engine after re-init completes.
- Mid-handshake spawn failure (e.g., `#waitForReady` times out, ping or
  version RPC fails after the process spawned) advances the state
  machine instead of wedging. In spawned mode the wedged process is
  SIGKILLed so its `'exit'` handler re-routes; in externally-managed
  mode `#handleRestartTrigger` is re-invoked explicitly (no
  `proc.on('exit')` exists to fall back on). Either path eventually
  trips the failure cap.
- Node's `'error'` event on the spawned `ChildProcess` is explicitly
  caught (defense against an unhandled-error worker crash on spawn-time
  failures like ENOENT / EACCES / wrong arch).

If the failure cap is exceeded (`'failed'` terminal state),
`waitForRunning()` rejects and dispatch falls through to TS for the
remainder of the process lifetime.

The drift audit (when running) catches state divergence with a triple
guard — Snapshotter version + `#tableSourcesVersion` (TableSource DB
binding) + `GoComputeBackend.epoch` (manager restart counter) — so a
restart that lands mid-audit is detected and the audit is skipped
rather than reporting a false-positive mismatch.

### Wire protocol

Length-prefixed (4-byte big-endian) MessagePack frames over Unix socket.
RPC methods: `ping`, `version`, `init`, `loadRows`, `addQuery`,
`addQueries`, `addQueriesStream`, `removeQuery`, `advance`,
`advanceStream`, `destroy`.

`addQueriesStream` (hydrate) and `advanceStream` (advance) are streaming
variants. Each emits one OR MORE partial frames (same request id)
followed by exactly one `"done"` terminal frame. The TS client routes
partial frames to an `onPartial` callback; the call promise resolves on
`"done"`.

**Chunking contract** (protocol rev 3+): partial frames carry
`chunkIndex` (monotonically increasing) and `final` (bool, true on the
last frame). The Go side chunks at 10,000 RowChanges per frame
(`hydrateChunkSize` / `advanceChunkSize`, matching the view-syncer's
`CURSOR_PAGE_SIZE`). For `addQueriesStream`, chunks are scoped per
`queryID`: each query may produce multiple frames, exactly one per
query has `final=true`, and the TS client accumulates them per `queryID`
before invoking its caller's `onResult`. For `advanceStream`, the whole
call produces one chunk sequence with cumulative `timings` only on the
final frame.

The view-syncer uses `addQueriesStream` so fast queries' RowChanges
reach clients before slow queries in the same batch finish, and
`advanceStream` so large snapshot diffs (e.g., from bulk imports) don't
buffer as one giant msgpack frame on the Go side.

Defensive invariants enforced by the TS client (throws on violation):
- `chunkIndex` arrives in monotonic order — wire-level bug if not
- every `addQueriesStream` `queryID` ends with `final=true` before "done"
- every `advanceStream` call sees at least one `final=true` before "done"

The TS client refuses to talk to a sidecar reporting a different
`protocolRev` than its constant in `sidecar-manager.ts` (currently 3).

### Operational concerns

- **Sidecar topology** — default is one sidecar per `zero-cache` syncer
  worker, shared across the ViewSyncers in that worker via per-cg FIFO
  queues on the Go side. Set `goSidecar.externallyManaged=true` +
  `goSidecar.socketPath=...` to switch to one sidecar per zero-cache
  process (shared across all workers); the deployment owner is then
  responsible for spawning the sidecar before workers start.
- **Worker-count tuning** — with default (per-worker) sidecars,
  bumping `ZERO_NUM_SYNC_WORKERS` shards client groups across more
  sidecars, shrinking per-sidecar batches and lowering inter-cg
  parallelism. Empirically measured (5-min, 10-user soak) p99
  regression 70ms → 180ms when going 2→4 workers in per-worker mode.
  In shared-sidecar mode worker count no longer affects sidecar
  concurrency, so the recommended default is `ZERO_NUM_SYNC_WORKERS=1`
  + shared sidecar.
- **Parallel-push threshold** — the sidecar engine fans out per-source
  push across pipeline connections when a source has ≥
  `GO_IVM_PARALLEL_THRESHOLD` connections (default 2, env-tunable).
  Lower this in workloads with low per-source query counts; raise it
  if goroutine spawn cost is visible in profiles.
- Sidecar memory grows linearly with active client groups × table data.
  Groups idle for 30+ minutes are auto-evicted on the Go side.
- Sidecar restart triggers per-`GoComputeBackend` reinit (init + chunked
  loadRows + re-register queries) before advance/hydrate calls resume.
  In externallyManaged mode "restart" is detected by the connection
  health-check ticker (default 2s); same reinit path runs.
- See `go-ivm/REVIEW-final.md` for the current production-readiness gate
  list.

## Known Gotchas

This section documents surprising behaviors and hard-won lessons. If you discover something non-obvious that caused significant debugging pain, consider adding it here.

### SQLite: NULL + OR = Full Table Scan

When building OR queries with bound parameters in SQLite, if **any** branch involves a NULL value, SQLite abandons its MULTI-INDEX OR optimization and falls back to a full table scan.

```sql
-- Even this simple query becomes a full table scan if ? is NULL:
SELECT * FROM users WHERE id = ? OR email = ?;

-- If email is NULL, SQLite won't use MULTI-INDEX OR, even for the valid id branch
EXPLAIN QUERY PLAN → "SCAN users" (not "SEARCH users USING INDEX")
```

**Why it matters**: This caused 320x slowdowns on tables with nullable unique columns. A query that should take <1ms was taking 320ms.

**Fix**: Filter out conditions where the value is NULL before building OR queries. NULL values can't violate uniqueness constraints anyway (NULL ≠ NULL in SQL).

```typescript
// Filter out keys where any column is NULL
const validKeys = keys.filter(key =>
  key.every(column => row[column] !== null && row[column] !== undefined),
);
```

See: https://github.com/rocicorp/mono/pull/5542

## Git Conventions

### Commit Messages

Follow conventional commits format:

```
type(scope): description
```

- `feat(zero-client): add support for custom mutations`
- `fix(zero-cache): resolve memory leak in connection pool`
- `chore(deps): update vitest to 3.2.4`

### Cherry-picking

Always use the `-x` flag when cherry-picking to record the source commit hash:

```bash
git cherry-pick -x <commit>
```

## Debugging and Development

### Zero Cache Debugging

```bash
# Debug Zero cache with breakpoints
npm run zero-brk

# Transform/run queries for debugging
npm run transform-query
npm run run-query
```

### Docker Development

Many apps include Docker Compose for local PostgreSQL:

```bash
npm run db-up    # Start PostgreSQL
npm run db-down  # Stop PostgreSQL
```

## Package Dependencies

### Core Dependencies

- **@rocicorp/\*** packages are internal utilities (logger, lock, resolver)
- **vitest**: Primary testing framework
- **oxlint**: TypeScript-aware linting
- **turbo**: Monorepo task running and caching

### Zero-Specific

- Clients depend on `replicache` for local data management
- Server components use `fastify` for HTTP/WebSocket handling
- OpenTelemetry integration for observability

## Critical Files to Understand

- `turbo.json`: Task dependencies and caching configuration
- `vitest.config.ts`: Multi-project test discovery and configuration
- `apps/zbugs/shared/schema.ts`: Reference Zero schema implementation
- `packages/zero-client/src/mod.ts`: Main Zero client API surface

## Running zbugs Locally

zbugs is the reference Zero application. To run it locally:

### Prerequisites

1. **Docker must be running** - Start Docker Desktop before running `db-up`

2. If you've made changes to any Zero packages (`zero-client`, `zero-cache`, `zero-protocol`, etc.), you must first rebuild:

```bash
npm --workspace=@rocicorp/zero run build
```

### Starting the Services

From `apps/zbugs`, start these three services (in background for AI, separate tabs for humans):

```bash
cd apps/zbugs

# 1. Start PostgreSQL (Docker) - must complete before others
npm run db-up

# 2. Start zero-cache with hot-reload
npm run zero-cache-dev

# 3. Start the Vite dev server
npm run dev
```

**For AI assistants**: Run `db-up` in background, wait for PostgreSQL to be ready, then run `zero-cache-dev` and `dev` in background. Use `run_in_background` parameter or `&` suffix. Check logs with `tail` on the output files.

### First-Time Setup

If the database is empty or schema has changed:

```bash
cd apps/zbugs
npm run db-migrate  # Apply schema migrations
npm run db-seed     # Seed with test data
```

### Troubleshooting

- **Port conflicts**: If `zero-cache-dev` fails with port in use, find and kill the process: `lsof -i :4848 | grep LISTEN` then `kill <PID>`
- **Schema changes**: If you modify `apps/zbugs/shared/schema.ts`, restart `zero-cache-dev`
- **Client changes**: Vite hot-reloads automatically, but for Zero client changes you may need to refresh the browser

See `apps/zbugs/README.md` for additional setup details and configuration options.
