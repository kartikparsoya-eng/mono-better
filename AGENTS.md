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
pnpm install && pnpm run build

# Run tests (uses vitest)
pnpm run test              # All tests
pnpm run test:watch        # Watch mode

# Type checking and linting
pnpm run check-types       # TypeScript across all packages
pnpm run lint              # oxlint with type-awareness
pnpm run format            # oxfmt formatting
```

**Always run `lint`, `format` and `check-types` after every change.**

### Package-Level Commands

Prefer package-level commands when possible. Each package supports: `test`, `check-types`, `lint`, `format`, `build`. e.g.:

```bash
pnpm --filter zero-client run format
pnpm --filter zero-cache run lint
pnpm --filter zero-server run check-types

# Run with coverage (prefer using this flag when possible)
pnpm --filter zero-client run test --coverage

# Run specific test file
pnpm --filter zero-client run test zero.test
```

### Zero Cache Development

```bash
# Start Zero cache server for local development
pnpm run start-zero-cache

# In zbugs app - start Zero cache with schema hot-reload
pnpm run zero-cache-dev
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

## Go IVM (in-process engine)

`zero-cache` can optionally offload IVM compute (advance + hydrate) to a
Go engine running **in-process** via a NAPI c-shared library
(`libgoivm.so`). Each zero-cache syncer worker dlopens the library at
startup via `ZERO_GO_SIDECAR_NAPI_LIB_PATH`. There is no socket
transport, no separate sidecar process, and no spawn/fallback machinery.
When enabled, the PipelineDriver dispatches the hot path to the Go engine
via direct ABI calls using MessagePack-RPC framing over a `net.Pipe`
instead of running the TS operator tree inline.

### Configuration

Sidecar settings live under the `goSidecar` group of `zero-config.ts`:

- `ZERO_GO_SIDECAR_ENABLED=true` — enable the Go IVM in-process engine.
- `ZERO_GO_SIDECAR_NAPI_LIB_PATH=/opt/go-ivm/libgoivm.so` — path to the
  c-shared library. Default: `libgoivm.so` (PATH lookup).
- `ZERO_GO_SIDECAR_PULL_WINDOW` — pull-mode hydration credit window
  (ABI v3 demand-gated streaming). Default is set in `zero-config.ts`.

### Engine-side env (read by the Go library at startup)

- `GO_IVM_GOGC=200` — GC target percent (`debug.SetGCPercent`). The engine is
  allocation-heavy (~8.6k objects per ~1k-row hydrate, dominated by the per-row
  `Row` map). At the Go default `GOGC=100` the GC caps multi-CG parallel scaling:
  cross-CG parallel speedup measured 2.6× at 16 CGs at GOGC=100 vs 4.9× at
  GOGC=800 (`engine/tablesource_bench_test.go` `TableSourceMulti`). The engine
  defaults to `GOGC=200`. Set `GO_IVM_GOGC=off` to disable GC. The standard
  `GOGC` env takes precedence when `GO_IVM_GOGC` is unset.
- `GO_IVM_GOMEMLIMIT=<bytes>` — soft memory cap (`debug.SetMemoryLimit`). Set
  to the container's memory budget and run a high/off `GOGC` so GC fires near
  the cap.
- `GO_IVM_GOMEMLIMIT_PERCENT=40` — alternative: set as a percentage of
  container memory (Docker default). Mutually exclusive with
  `GO_IVM_GOMEMLIMIT`.
- `GO_IVM_PARALLEL_THRESHOLD=2` — min connection count per source at which
  parallel push fan-out kicks in. Default 2.
- `GO_IVM_HYDRATE_PARALLELISM=4` — hydrate lane count (readers = 2× lanes).
- `GO_IVM_ADVANCE_PARALLELISM=4` — advance fanout workers (default ON;
  `false` disables).
- `GO_IVM_MAX_OPEN_CONNS=1024` — per-worker SQLite connection pool ceiling.
- `GO_IVM_DELIVER_TIMEOUT_SEC=55` — row-plane park deadline. Default 55s,
  kept below the 60s advance budget.
- `GO_IVM_ADVANCE_BUDGET_MS=60000` — wall-clock advance backstop.
- `GO_IVM_WEDGE_WATCHDOG_SEC=90` — CG handler wedge detection threshold.
- `GO_IVM_CHUNK_SIZE=100` — streaming chunk size (both hydrate and advance).
- `GO_IVM_PPROF_ADDR=127.0.0.1:6060` — when set, opens a `net/http/pprof`
  endpoint. ~5% overhead; leave unset in prod.

### Decoder / Row representation

The Go engine's `ivm.Row` is `map[string]Value` — faithful port of TS's
`Record<string, Value>` (`zero-protocol/src/data.ts`). Numeric coercion
happens at msgpack decode time via a custom `Row.DecodeMsgpack` that
coerces integer column values to `float64` inline, cutting total
allocations by ~41% on the live-load profile while preserving TS↔Go
equivalence. A reflection-walk normalize is still applied to non-Row
payloads (e.g., `builder.ValuePos.Value` AST literals).

OTel traces from the sidecar use the standard OTLP env vars
(`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`,
`OTEL_SERVICE_NAME`). Tracing is disabled when no endpoint is set.

### Engine reinit and error handling

If the NAPI library fails to load (missing file, wrong arch, dlopen
error), the PipelineDriver dispatch falls through to the TS-native
path. The Go path is always optional; user-facing correctness depends
only on the TS path.

**NAPI in-process transport (current):** the engine is dlopen'd into the
syncer worker, so it CANNOT be restarted in-process. A post-start host
death — `goivm_send` returning rc != 0 (the Go receive loop is gone) —
fires `onFatal` → `SidecarManager.#handleFatal`, which marks the manager
`failed` (`initFailureCounter` reason `napi-no-restart`), rejects the
running promise, and calls `fatalExit()` (`process.exit(1)`). The
supervisor then restores a working state with a fresh worker + fresh
dlopen. TS fallback is deliberately NOT attempted for a post-start crash:
Go-owned client groups hold Go-owned stub pipelines, so silently serving
them from TS would double-emit. Crash-don't-degrade is the design.

The graceful in-process reinit machinery below (`onRestart`,
`waitForRunning` queueing across a reinit window, "miss exactly ONE
delta") describes the OBSOLETE out-of-process socket-sidecar transport
and does NOT apply to the napi transport — those code paths are inert
post-start here (`sidecar-manager.ts` `#handleFatal` is terminal):

- ~~`SidecarManager` re-initializes the engine under a failure cap.~~
- ~~`GoComputeBackend.waitForRunning()` queues in-flight RPCs across
  the reinit window.~~
- ~~The `onRestart` listener re-runs `init` + query re-registration.~~
- ~~`advance()` in flight at the crash returns empty `{changes, timings}`;
  clients miss exactly ONE delta.~~

The LOAD-TIME fallback above still holds: if the library fails to dlopen
at startup, dispatch runs entirely on the TS-native path.

### Reset circuit breaker

The view-syncer implements a reason-tiered reset circuit breaker to
prevent cascading CG teardowns:

- **Deterministic** (watermark-regression): fast trip — 2 resets in 20s.
- **Transient** (go-primary-unavailable, go-primary-drop): loose trip
  — 6 resets in 60s.
- **Economic** (advancement-timeout): never trips — abort/reset cycles
  are handled by the suppressAbort escalation instead.
- **Lawful** (schema-change, truncation, permissions-change,
  scalar-subquery): never trips.

Per-class sliding windows are cleared on every successful advance
(half-open semantics). The trip metric carries a class label.

### suppressAbort escalation

After 3 consecutive abort-doubling cycles (`SUPPRESS_ABORT_AFTER_STREAK`),
the next `advanceToHeadStream` sends `suppressAbort=true` for an
un-abortable catch-up. The streak resets on success. Go's 60s wall
clock backstop still bounds the advance. This makes the economic
class's never-trip breaker safe: an unbounded abort→reset loop is
structurally impossible.

### NAPI ABI

The Go IVM engine exposes a c-shared ABI (`cmd/sidecar/napi_lib.go`)
called via the NAPI addon (`go-sidecar/napi/`). RPC methods: `ping`,
`version`, `init`, `addQueriesStream`, `removeQuery`,
`advanceToHeadStream`, `destroy`.

`addQueriesStream` (hydrate) and `advanceToHeadStream` (advance) are
streaming methods. Each emits one OR MORE partial frames (same request
id) followed by exactly one `"done"` terminal frame. The TS client
routes partial frames to an `onPartial` callback; the call promise
resolves on `"done"`.

**Chunking contract** (protocol rev 12): partial frames carry
`chunkIndex` (monotonically increasing) and `final` (bool, true on the
last frame). The Go side chunks at 10,000 RowChanges per frame. For
`addQueriesStream`, chunks are scoped per `queryID`: each query may
produce multiple frames, exactly one per query has `final=true`. For
`advanceToHeadStream`, the whole call produces one chunk sequence with
cumulative `timings` only on the final frame.

Defensive invariants enforced by the TS client (throws on violation):
- `chunkIndex` arrives in monotonic order
- every `addQueriesStream` `queryID` ends with `final=true` before "done"
- every `advanceToHeadStream` call sees at least one `final=true` before "done"

The TS client refuses to talk to a sidecar reporting a different
`protocolRev` than its constant in `sidecar-manager.ts` (currently 12).

### Operational concerns

- **Engine topology** — each `zero-cache` syncer worker hosts its own
  Go engine instance via NAPI in-process. Per-cg FIFO queues on the Go
  side serialize work within each engine.
- **Worker-count tuning** — `ZERO_NUM_SYNC_WORKERS` controls V8
  parallelism and the number of Go engine instances. Each worker's
  engine has its own SQLite connection pool. The default is 8 workers.
- **Parallel advance** — `GO_IVM_ADVANCE_PARALLELISM` controls advance
  fanout workers (default 4, default ON). Set to 1 for serial/TS-faithful
  fanout.
- **Hydrate parallelism** — `GO_IVM_HYDRATE_PARALLELISM` controls hydrate
  lane count (default 4, readers = 2× lanes).
- Engine memory grows linearly with active client groups × table data.
  Groups idle for 30+ minutes are auto-evicted on the Go side.
- Engine reinit triggers per-`GoComputeBackend` reinit (init +
  re-register queries) before advance/hydrate calls resume.
- See `go-ivm/PROD-PATH.md` for the production configuration map.

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
pnpm run zero-brk

# Transform/run queries for debugging
pnpm run transform-query
pnpm run run-query
```

### Docker Development

Many apps include Docker Compose for local PostgreSQL:

```bash
pnpm run db-up    # Start PostgreSQL
pnpm run db-down  # Stop PostgreSQL
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

1. **Docker must be running** - Start Docker Desktop before running `db-up`.
   Inside a dev container there is no Docker: use the zbugs dev container
   profile (`.devcontainer/zbugs/`) instead, where Postgres already runs as
   sibling containers and `db-up`/`db-down` are not needed. See
   `.devcontainer/README.md`.

2. If you've made changes to any Zero packages (`zero-client`, `zero-cache`, `zero-protocol`, etc.), you must first rebuild:

```bash
pnpm --filter @rocicorp/zero run build
```

### Starting the Services

From `apps/zbugs`, start these three services (in background for AI, separate tabs for humans):

```bash
cd apps/zbugs

# 1. Start PostgreSQL (Docker) - must complete before others
pnpm run db-up

# 2. Start zero-cache with hot-reload
pnpm run zero-cache-dev

# 3. Start the Vite dev server
pnpm run dev
```

**For AI assistants**: Run `db-up` in background, wait for PostgreSQL to be ready, then run `zero-cache-dev` and `dev` in background. Use `run_in_background` parameter or `&` suffix. Check logs with `tail` on the output files.

### First-Time Setup

If the database is empty or schema has changed:

```bash
cd apps/zbugs
pnpm run db-migrate  # Apply schema migrations
pnpm run db-seed     # Seed with test data
```

### Troubleshooting

- **Port conflicts**: If `zero-cache-dev` fails with port in use, find and kill the process: `lsof -i :4848 | grep LISTEN` then `kill <PID>`
- **Schema changes**: If you modify `apps/zbugs/shared/schema.ts`, restart `zero-cache-dev`
- **Client changes**: Vite hot-reloads automatically, but for Zero client changes you may need to refresh the browser

See `apps/zbugs/README.md` for additional setup details and configuration options.
