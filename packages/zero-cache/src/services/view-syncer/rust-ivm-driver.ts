import type {LogContext} from '@rocicorp/logger';
import type {
  NapiRowChange,
  NapiTableSpec,
  RustIvmEngine,
} from '../../../../rust-ivm/napi/index.js';
import {assert} from '../../../../shared/src/asserts.ts';
export type {
  NapiQuerySpec,
  NapiRowChange,
  NapiTableSpec,
} from '../../../../rust-ivm/napi/index.js';

// Terminal sentinel changeType — the LAST element of the final delivery chunk
// (see drain_barrier_chunk in napi/src/lib.rs). It carries no data; consumers
// skip it. Its only purpose is to keep the addon's actor thread blocked until
// every real row has been delivered to JS before the hydrate/advance promise
// resolves — so the driver's queue-close cannot race ahead and drop the last
// row (seed 308).
const END_STREAM_SENTINEL = -3;

import {must} from '../../../../shared/src/must.ts';
import type {AST, Condition} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import {completeOrdering} from '../../../../zql/src/query/complete-ordering.ts';
import type {ClientGroupStorage} from '../../../../zqlite/src/database-storage.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {
  fromSQLiteTypes,
  toSQLiteTypes,
} from '../../../../zqlite/src/table-source.ts';
import {
  reloadPermissionsIfChanged,
  type LoadedPermissions,
} from '../../auth/load-permissions.ts';
import type {LogConfig, ZeroConfig} from '../../config/zero-config.ts';
import {computeZqlSpecs} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../../db/specs.ts';
import type {StatementRunner} from '../../db/statements.ts';
import type {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {type RowKey} from '../../types/row-key.ts';
import {type ShardID} from '../../types/shards.ts';
import {getSubscriptionState} from '../replicator/schema/replication-state.ts';
import {checkClientSchema} from './client-schema.ts';
import {rowIDSignatureUnit} from './row-set-signature.ts';
import {ResetPipelinesSignal} from './snapshotter.ts';

// Try to load the native addon (use createRequire for ESM compatibility with tsx)
import {createRequire} from 'node:module';
const nodeRequire = createRequire(import.meta.url);
let RustIvmEngineClass: (new () => RustIvmEngine) | null = null;
const addonPath =
  process.env['RUST_IVM_ADDON_PATH'] ??
  '../../../../packages/rust-ivm/napi/rust-ivm.node';
try {
  RustIvmEngineClass = (
    nodeRequire(addonPath) as {RustIvmEngine: new () => RustIvmEngine}
  ).RustIvmEngine;
} catch (e) {
  console.error(
    '[rust-ivm-driver] Failed to load addon from',
    addonPath,
    ':',
    (e as Error).message,
  );
}

export type {Timer} from './pipeline-driver.ts';
import {MIN_ADVANCEMENT_TIME_LIMIT_MS} from './pipeline-driver.ts';
import type {Timer} from './pipeline-driver.ts';

export type RowChange = {
  readonly type: number;
  readonly queryID: string;
  readonly table: string;
  readonly rowKey: RowKey;
  readonly row: Row | undefined;
};

type QueryInfo = {
  readonly transformedAst: AST;
  readonly transformationHash: string;
  readonly queryName?: string | undefined;
};

// napiToRow and fromNapiValue removed — row data is now JSON strings.

// Integer handling matches TS exactly: TS's `fromSQLiteType` uses
// `safeIntegers(true)` and THROWS `UnsupportedValueError` for any integer
// outside ±(2^53-1) — it does NOT keep bigint (Zero's `Value` type is
// `null|bool|number|string|JSON`, no bigint). The Rust addon mirrors this: it
// rejects >2^53 at `sqlite_value_to_ivm` (panic → rethrown across napi), and all
// in-bounds integers cross as JSON numbers and parse to JS `number`. So there is
// no bigint path on EITHER side — a future column that needs >2^53 (nanosecond
// timestamps, snowflake IDs) requires a lossless int design in both engines.
function reviveNativeSQLiteValue(_key: string, value: unknown): unknown {
  if (
    value !== null &&
    typeof value === 'object' &&
    Object.keys(value).length === 1
  ) {
    const tagged = value as Record<string, unknown>;
    const integer = tagged['__rustIvmSqliteInteger'];
    if (typeof integer === 'string') {
      return BigInt(integer);
    }
    const real = tagged['__rustIvmSqliteReal'];
    if (real === 'NaN') {
      return Number.NaN;
    }
    if (real === 'Infinity') {
      return Number.POSITIVE_INFINITY;
    }
    if (real === '-Infinity') {
      return Number.NEGATIVE_INFINITY;
    }
  }
  return value;
}

function parseJSONObject(json: string, label: string): Record<string, unknown> {
  const value: unknown = JSON.parse(json);
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`${label} must be a JSON object`);
  }
  return value as Record<string, unknown>;
}

function reviveNativeRow(
  value: Record<string, unknown>,
  table: string,
  tableSpecs: ReadonlyMap<string, LiteAndZqlSpec>,
): Record<string, unknown> {
  const columns = tableSpecs.get(table)?.zqlSpec;
  for (const [column, cell] of Object.entries(value)) {
    // JSON columns can legitimately contain an object identical to a native
    // transport tag. Only revive top-level non-JSON cells; readQuery returns
    // raw JSON text and uses its own reviver.
    if (columns?.[column]?.type !== 'json') {
      value[column] = reviveNativeSQLiteValue(column, cell);
    }
  }
  return value;
}

// Mirror of the Rust RUST_IVM_PERF_TRACE gate: JS-side per-row costs
// (JSON.parse / revive / queue push). Zero overhead when the env is unset
// (checked once at module load).
const PERF_TRACE_JS = !!process.env['RUST_IVM_PERF_TRACE'];
const perfJS = {
  parse: {ms: 0, hits: 0},
  revive: {ms: 0, hits: 0},
  queue: {ms: 0, hits: 0},
};
function perfJSReport(lc: LogContext, op: string): void {
  if (!PERF_TRACE_JS) {
    return;
  }
  const f = (b: {ms: number; hits: number}) =>
    `${b.ms.toFixed(1)}ms/${b.hits}h`;
  const line = `[rust-ivm][PERF-JS] ${op} parse=${f(perfJS.parse)} revive=${f(perfJS.revive)} queue=${f(perfJS.queue)}`;
  lc.info?.(line);
  // Write directly to fd 2: vitest's `silent: 'passed-only'` swallows
  // intercepted console output for passing tests, but not raw stream writes
  // (matching the native addon's eprintln! lines).
  process.stderr.write(`${line}\n`);
  for (const b of [perfJS.parse, perfJS.revive, perfJS.queue]) {
    b.ms = 0;
    b.hits = 0;
  }
}

function napiToRowChange(
  c: NapiRowChange,
  tableSpecs: ReadonlyMap<string, LiteAndZqlSpec>,
): RowChange {
  if (PERF_TRACE_JS) {
    const t0 = performance.now();
    const rowKeyObj = parseJSONObject(c.rowKey, 'native row key');
    const rowObj = c.row ? parseJSONObject(c.row, 'native row') : undefined;
    const t1 = performance.now();
    const rowKey = reviveNativeRow(rowKeyObj, c.table, tableSpecs) as RowKey;
    const row = rowObj
      ? (reviveNativeRow(rowObj, c.table, tableSpecs) as Row)
      : undefined;
    const t2 = performance.now();
    perfJS.parse.ms += t1 - t0;
    perfJS.parse.hits++;
    perfJS.revive.ms += t2 - t1;
    perfJS.revive.hits++;
    return {
      type: c.changeType,
      queryID: c.queryId,
      table: c.table,
      rowKey,
      row,
    };
  }
  return {
    type: c.changeType,
    queryID: c.queryId,
    table: c.table,
    rowKey: reviveNativeRow(
      parseJSONObject(c.rowKey, 'native row key'),
      c.table,
      tableSpecs,
    ) as RowKey,
    row: c.row
      ? (reviveNativeRow(
          parseJSONObject(c.row, 'native row'),
          c.table,
          tableSpecs,
        ) as Row)
      : undefined,
  };
}

const ENGINE_ADVANCE_PANIC_PREFIX = 'engine advance panic: ';

/** Remove the napi transport wrapper around an engine error.
 *
 * PipelineDriver exposes IVM/SQLite failures as ordinary Errors. The native
 * worker catches the same panic so it can cross the napi boundary, which adds
 * a prefix and a GenericFailure code. Neither is part of the driver contract.
 */
function normalizeNativeAdvanceError(error: unknown): unknown {
  if (
    error instanceof Error &&
    error.message.startsWith(ENGINE_ADVANCE_PANIC_PREFIX)
  ) {
    return new Error(error.message.slice(ENGINE_ADVANCE_PANIC_PREFIX.length));
  }
  return error;
}

function normalizeNativeHydrateError(error: unknown): unknown {
  // `napi::Error::from_reason` adds `code: "GenericFailure"`. PipelineDriver's
  // corresponding builder/SQLite/comparator failures are plain Errors, and the
  // transport code is not part of the PipelineDriver contract.
  return error instanceof Error ? new Error(error.message) : error;
}

type NativeStatement = {
  all: (...args: unknown[]) => Record<string, unknown>[];
  get: (...args: unknown[]) => Record<string, unknown> | undefined;
  iterate: (...args: unknown[]) => Generator<Record<string, unknown>>;
  raw: (...args: unknown[]) => Generator<unknown[]>;
};

type NativeStatementRunner = {
  get: (sql: string, ...args: unknown[]) => Record<string, unknown> | undefined;
  all: (sql: string, ...args: unknown[]) => Record<string, unknown>[];
  prepare: (sql: string) => NativeStatement;
  modify: () => never;
};

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}
function buildPrimaryKeys(
  clientSchema: ClientSchema,
  primaryKeys: Map<string, PrimaryKey> = new Map<string, PrimaryKey>(),
) {
  for (const [tableName, {primaryKey}] of Object.entries(clientSchema.tables)) {
    primaryKeys.set(tableName, primaryKey as unknown as PrimaryKey);
  }
  return primaryKeys;
}

/**
 * Build the per-table specs handed to the napi engine.
 *
 * Extracted as a pure function so both the driver and tests/fuzzers exercise the
 * exact same derivation — the class of bug where the engine is keyed differently
 * from the client (rowKey missing the client PK column → toPrimaryKeyString "Got
 * undefined") only manifests HERE, in the glue between the LiteSpec and the
 * client schema, so this is the seam the differential harness must drive.
 *
 * Invariant this encodes (matching TS): a table's rowKey is keyed by its
 * CLIENT-schema primaryKey (from `primaryKeys`, populated by buildPrimaryKeys),
 * NOT the raw LiteSpec/replica primaryKey. `uniqueKeys` still come from the
 * LiteSpec (they drive scalar-EXISTS resolution, not the emitted rowKey).
 */
export function buildNapiTableSpecs(
  tableSpecs: ReadonlyMap<string, LiteAndZqlSpec>,
  primaryKeys: ReadonlyMap<string, PrimaryKey>,
): NapiTableSpec[] {
  const out: NapiTableSpec[] = [];
  for (const [table, spec] of tableSpecs.entries()) {
    const columns: Record<string, {type: string; optional: boolean}> = {};
    for (const [col, schemaValue] of Object.entries(spec.zqlSpec)) {
      columns[col] = {
        type: schemaValue.type,
        optional: schemaValue.optional ?? false,
      };
    }
    out.push({
      table,
      columns,
      // CLIENT-schema PK (what the client uses in toPrimaryKeyString), not the
      // raw LiteSpec/replica PK. These differ for tables whose Zero primaryKey
      // column != the replica PK column (messages -> messageId, conversations ->
      // conversationId); shipping the LiteSpec PK emits rowKeys missing the
      // client PK column and crashes the client with "Got undefined".
      primaryKey: [...(primaryKeys.get(table) ?? spec.tableSpec.primaryKey)],
      // All unique keys (PK plus secondary unique indexes) — drives scalar-EXISTS
      // resolution keyed on a non-PK unique index (e.g. channel_participants
      // (channelId,userId) in the conversation ACL); see the G8 diff-oracle gap.
      uniqueKeys: spec.tableSpec.uniqueKeys.map(key => [...key]),
      ...(spec.tableSpec.minRowVersion && {
        minRowVersion: spec.tableSpec.minRowVersion,
      }),
    });
  }
  return out;
}

// ---------------------------------------------------------------------------
// AsyncQueue — bounded bridge between TSFN callbacks and async generators
// ---------------------------------------------------------------------------

/**
 * Bounded bridge between TSFN callbacks and async generators.
 *
 * The TSFN callback calls `push()` synchronously; the async generator drains
 * via `for await`. The queue is capped at `maxBuffer` items — when full,
 * `push()` returns false, signaling the caller to pause production. With the
 * bounded TSFN (max_queue_size=1), at most maxBuffer+1 rows are in flight
 * per client group. This caps JS heap usage under concurrent load (50 CGs ×
 * maxBuffer items × ~1KB/row = bounded, not O(result-set)).
 */
export class AsyncQueue<T> implements AsyncIterable<T> {
  #items: T[] = [];
  #waiters: {
    resolve: (r: IteratorResult<T>) => void;
    reject: (e: unknown) => void;
  }[] = [];
  #done = false;
  #error: unknown = null;
  #maxBuffer: number;
  #drainWaiters: (() => void)[] = [];

  constructor(maxBuffer = 256) {
    this.#maxBuffer = maxBuffer;
  }

  /**
   * Push an item. Returns true if the queue has room for more, false if the
   * buffer is full (producer should pause). The TSFN callback can check this
   * to apply backpressure. Production backpressure is enforced by the native
   * credit gate; this return value remains useful to direct queue users.
   */
  push(item: T): boolean {
    if (this.#done) return false;
    const waiter = this.#waiters.shift();
    if (waiter) {
      waiter.resolve({value: item, done: false});
      return this.#items.length < this.#maxBuffer;
    }
    this.#items.push(item);
    return this.#items.length < this.#maxBuffer;
  }

  /**
   * Wait until the buffer drains below maxBuffer. Called by the TSFN
   * callback path when push() returns false.
   */
  async waitForDrain(): Promise<void> {
    if (this.#items.length < this.#maxBuffer) return;
    return new Promise(resolve => {
      this.#drainWaiters.push(resolve);
    });
  }

  #notifyDrainWaiters(): void {
    if (this.#items.length < this.#maxBuffer) {
      const waiters = this.#drainWaiters;
      this.#drainWaiters = [];
      for (const w of waiters) w();
    }
  }

  close(): void {
    this.#done = true;
    for (const w of this.#waiters) {
      if (this.#error) {
        w.reject(this.#error);
      } else {
        w.resolve({value: undefined, done: true});
      }
    }
    this.#waiters = [];
    for (const w of this.#drainWaiters) w();
    this.#drainWaiters = [];
  }

  error(e: unknown): void {
    this.#error = e;
    this.close();
  }

  async next(): Promise<IteratorResult<T>> {
    if (this.#items.length > 0) {
      const item = this.#items.shift()!;
      this.#notifyDrainWaiters();
      return {value: item, done: false};
    }
    if (this.#done) {
      if (this.#error) throw this.#error;
      return {value: undefined, done: true};
    }
    return new Promise((resolve, reject) => {
      this.#waiters.push({resolve, reject});
    });
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {next: () => this.next()};
  }
}

/**
 * Close an AsyncQueue on the next macrotask instead of immediately.
 *
 * The napi streaming methods signal completion two ways: (1) each row arrives
 * via a TSFN callback (a libuv macrotask), and (2) the returned Promise
 * resolves when `compute()` finishes on the worker thread. These are separate
 * channels, so the Promise can resolve while the final row's TSFN callback is
 * still queued. If we called `queue.close()` directly in `.then()` (a
 * microtask), `#done` would be set before that callback ran and its `push()`
 * would silently drop the row.
 *
 * The addon also emits an explicit end-of-stream sentinel and waits for its JS
 * callback, which is the correctness barrier. Deferring close remains a small
 * ordering cushion between that callback and the native Promise continuation.
 */
export function deferClose<T>(queue: AsyncQueue<T>): void {
  setImmediate(() => queue.close());
}

// ---------------------------------------------------------------------------
// RustIVMDriver
// ---------------------------------------------------------------------------

export class RustIVMDriver {
  readonly #engine: RustIvmEngine;
  // Monotonic per-driver stream id (#3 backpressure). Each streaming hydrate/
  // advance mints the next id and passes it to the native call + every
  // grant/cancel so credit is always tagged to the exact stream it belongs to.
  #nextStreamId = 1;
  readonly #lc: LogContext;
  readonly #shardID: ShardID;
  readonly #storage: ClientGroupStorage;
  readonly #config: ZeroConfig | undefined;
  readonly #replicaFile: string;
  readonly #yieldThresholdMs: () => number;
  readonly #planEnabled: boolean = false;
  readonly #tableSpecs = new Map<string, LiteAndZqlSpec>();
  readonly #allTableNames = new Set<string>();
  readonly #primaryKeys = new Map<string, PrimaryKey>();
  #replicaVersion: string | null = null;
  #currentVersion: string | null = null;
  #permissions: LoadedPermissions | null = null;
  #permissionsVersion: string | null = null;
  #queryInfo = new Map<string, QueryInfo>();
  readonly #rowSetSignatures = new Map<string, bigint>();
  #initialized = false;
  #destroyPromise: Promise<void> | undefined;

  constructor(
    lc: LogContext,
    _logConfig: LogConfig,
    shardID: ShardID,
    storage: ClientGroupStorage,
    clientGroupID: string,
    _inspectorDelegate: InspectorDelegate,
    yieldThresholdMs: () => number,
    enablePlanner?: boolean,
    config?: ZeroConfig,
    replicaFile?: string,
  ) {
    assert(RustIvmEngineClass, 'Rust IVM NAPI addon not loaded');
    this.#lc = lc.withContext('clientGroupID', clientGroupID);
    this.#engine = new RustIvmEngineClass();
    // Fail loud on an addon that predates the backpressure/cancel wiring. These
    // are called out-of-band while a producer is parked on stream credit; if the
    // loaded addon lacks them, an optional-chained no-op would silently wedge the
    // stream until the 600s watchdog instead of surfacing the version skew here.
    for (const method of [
      'grantStreamCredit',
      'cancelStream',
      'cancel',
    ] as const) {
      assert(
        typeof (this.#engine as unknown as Record<string, unknown>)[method] ===
          'function',
        `Rust IVM NAPI addon is missing '${method}' — rebuild the addon (version skew)`,
      );
    }
    this.#shardID = shardID;
    this.#storage = storage;
    this.#config = config;
    this.#replicaFile = replicaFile ?? '';
    this.#yieldThresholdMs = yieldThresholdMs;
    // Query planner: runs the Rust-ported plan graph over a
    // cost model backed by the Rust-owned snapshot connection, annotating
    // `flip` on OR-with-CSQ conditions (parity with zero 1.7's default-on
    // planner). Match PipelineDriver: the constructor flag is the only gate.
    this.#planEnabled = enablePlanner === true;
  }

  // Single-owner design: the Rust engine exclusively owns replica.db's
  // connection. All TS reads go through NAPI methods on the Rust side.
  // This minimal StatementRunner adapter lets computeZqlSpecs and
  // getSubscriptionState run unchanged against the Rust-owned connection.
  // Bind params are passed as a JSON array to engine.readQuery.
  #runner(): NativeStatementRunner {
    const engine = this.#engine;
    const readQuery = (
      sql: string,
      params?: unknown[],
    ): Record<string, unknown>[] =>
      JSON.parse(
        engine.readQuery(sql, params ? JSON.stringify(params) : null),
        reviveNativeSQLiteValue,
      ) as Record<string, unknown>[];
    return {
      get: (sql: string, ...args: unknown[]) => readQuery(sql, args)[0],
      all: (sql: string, ...args: unknown[]) => readQuery(sql, args),
      prepare: (sql: string) => {
        return {
          all: (...args: unknown[]) => readQuery(sql, args),
          get: (...args: unknown[]) => readQuery(sql, args)[0],
          iterate: function* (
            ...args: unknown[]
          ): Generator<Record<string, unknown>> {
            yield* readQuery(sql, args);
          },
          raw: function* (...args: unknown[]): Generator<unknown[]> {
            for (const row of readQuery(sql, args)) yield Object.values(row);
          },
        };
      },
      modify: () => {
        throw new Error('read-only');
      },
    };
  }

  init(clientSchema: ClientSchema) {
    assert(!this.#initialized, 'Already initialized');
    this.#initialized = true;
    void this.#initAndResetCommon(clientSchema);
  }

  initialized(): boolean {
    return this.#initialized;
  }

  reset(clientSchema: ClientSchema) {
    assert(this.initialized(), 'Not yet initialized');
    this.#engine.reset();
    this.#queryInfo.clear();
    this.#rowSetSignatures.clear();
    void this.#initAndResetCommon(clientSchema);
  }

  #initAndResetCommon(clientSchema: ClientSchema) {
    // Initialize the Rust snapshotter FIRST so read_query works for
    // computeZqlSpecs and getSubscriptionState below. This solves the
    // chicken-and-egg: init() needs table specs from the schema, but
    // reading the schema needs the snapshotter.
    if (this.#replicaFile) {
      this.#engine.initSnapshotter(this.#replicaFile, this.#shardID.appID);
    }

    const runner = this.#runner();
    const fullTables = new Map<string, LiteTableSpec>();
    computeZqlSpecs(
      this.#lc,
      runner as unknown as Database,
      {includeBackfillingColumns: false},
      this.#tableSpecs,
      fullTables,
    );
    checkClientSchema(
      this.#shardID,
      clientSchema,
      this.#tableSpecs,
      fullTables,
    );
    this.#allTableNames.clear();
    for (const table of fullTables.keys()) {
      this.#allTableNames.add(table);
    }
    this.#primaryKeys.clear();
    for (const [table, spec] of this.#tableSpecs.entries()) {
      this.#primaryKeys.set(table, spec.tableSpec.primaryKey);
    }
    buildPrimaryKeys(clientSchema, this.#primaryKeys);

    const tableSpecs = buildNapiTableSpecs(this.#tableSpecs, this.#primaryKeys);
    // Pass db_path to init() so sources + SQLite path are set atomically on
    // the worker thread (avoids race where setDatabasePath runs before Init).
    this.#engine.init(
      tableSpecs,
      this.#replicaFile || null,
      this.#shardID.appID,
    );
    this.#lc.info?.(`RustIVMDriver: init complete, db=${this.#replicaFile}`);

    // replicaVersion is the IMMUTABLE base stamped at replica creation.
    // watermark (= _zero.replicationState.stateVersion) is the LIVE head the
    // snapshot is pinned at. These diverge on any replica that has advanced
    // past its creation stamp (i.e. every restored replica). currentVersion()
    // must return the live head, matching TS PipelineDriver.currentVersion()
    // which reads `this.#snapshotter.current().version` (pipeline-driver.ts).
    // Seeding #currentVersion from replicaVersion (the base) floors the CVR at
    // the base version; a later real row version then trips cvr.ts
    // #assertNewVersion ("Expected CVR version to have been bumped above
    // original") and crash-loops the client group.
    const {replicaVersion, watermark} = getSubscriptionState(
      this.#runner() as unknown as StatementRunner,
    );
    this.#replicaVersion = replicaVersion;
    this.#currentVersion = watermark;
  }

  get replicaVersion(): string {
    return must(this.#replicaVersion, 'Not yet initialized');
  }

  currentVersion(): string {
    assert(this.initialized(), 'Not yet initialized');
    return must(this.#currentVersion, 'Not yet initialized');
  }

  currentPermissions(): LoadedPermissions | null {
    assert(this.initialized(), 'Not yet initialized');
    // Cache: only re-query permissions when the replica version changes.
    // In TS this is a direct SQLite call (microseconds), but in the Rust
    // single-owner path each read_query goes through the actor thread
    // (synchronous NAPI call). Without caching, this fires on every
    // query batch — blocking the JS event loop dozens of times per
    // hydration cycle.
    if (
      this.#permissions !== null &&
      this.#permissionsVersion === this.#currentVersion
    ) {
      return this.#permissions;
    }
    const res = reloadPermissionsIfChanged(
      this.#lc,
      this.#runner() as unknown as StatementRunner,
      this.#shardID.appID,
      this.#permissions,
      this.#config,
    );
    if (res.changed) {
      this.#permissions = res.permissions;
    }
    this.#permissionsVersion = this.#currentVersion;
    return this.#permissions;
  }

  advanceWithoutDiff(): string {
    // Single-owner: the Rust engine advances its own snapshotter.
    const version = this.#engine.advanceWithoutDiff();
    this.#currentVersion = version;
    return version;
  }

  async destroy(): Promise<void> {
    if (this.#destroyPromise) {
      return this.#destroyPromise;
    }
    this.#destroyPromise = this.#destroyOnce();
    return this.#destroyPromise;
  }

  async #destroyOnce(): Promise<void> {
    // Single-owner teardown: the Rust engine owns every SQLite connection.
    // Cancel out-of-band before queueing destroy. A producer may be parked on
    // credit while an abandoned iterator no longer grants it; destroy itself is
    // actor-queued and cannot release that park site.
    this.#engine.cancel?.();
    await this.#engine.destroy();
    this.#storage.destroy();
  }

  queries(): ReadonlyMap<string, QueryInfo> {
    return this.#queryInfo;
  }

  totalHydrationTimeMs(): number {
    return this.#engine.totalHydrationTimeMs();
  }

  removeQuery(queryID: string) {
    this.#engine.removeQuery(queryID);
    this.#queryInfo.delete(queryID);
    this.#rowSetSignatures.delete(queryID);
  }

  rowSetSignature(queryID: string): bigint | undefined {
    return this.#rowSetSignatures.get(queryID);
  }

  getRow(table: string, pk: RowKey): Row | undefined {
    assert(this.initialized(), 'Not yet initialized');
    must(this.#primaryKeys.get(table));
    const spec = must(this.#tableSpecs.get(table));
    // Match the TS TableSource.getRow() value contract (zqlite/table-source.ts):
    // project ONLY the syncable columns (never `SELECT *`, which leaks the
    // replica-internal _0_version and other unsynced columns), convert the key
    // params via toSQLiteTypes, and run the result through fromSQLiteTypes so
    // booleans (0/1 -> true/false) and json (string -> parsed) match the
    // pipeline's value semantics instead of the raw SQLite serialization.
    const columns = spec.zqlSpec;
    const keyCols = Object.keys(pk);
    const selectCols = Object.keys(columns)
      .map(c => quoteIdent(c))
      .join(', ');
    const where = keyCols.map(c => `${quoteIdent(c)} = ?`).join(' AND ');
    const sql = `SELECT ${selectCols} FROM ${quoteIdent(table)} WHERE ${where}`;
    const params = toSQLiteTypes(keyCols, pk as Row, columns);
    const row = this.#runner().get(sql, ...params) as Row | undefined;
    return row ? fromSQLiteTypes(columns, row, table) : undefined;
  }

  addQuery(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
    queryName?: string,
  ): AsyncIterable<RowChange | 'yield'> {
    return this.#addQueryImpl(
      transformationHash,
      queryID,
      query,
      timer,
      queryName,
    );
  }

  #planAst(ast: AST): AST {
    const ordered = completeOrdering(ast, tableName =>
      must(
        this.#primaryKeys.get(tableName),
        `no primary key for table '${tableName}'`,
      ),
    );
    if (!this.#planEnabled) {
      return ordered;
    }
    // Ask the Rust engine for the flip decisions (cost model on the snapshot
    // connection), then apply them in the same canonical order. PipelineDriver
    // does not swallow cost-model failures, so native planning failures must
    // propagate rather than silently changing the physical plan.
    const flips: (boolean | null)[] = JSON.parse(
      this.#engine.planAst(JSON.stringify(ordered)),
    );
    const i = {n: 0};
    const flipped = applyFlips(ordered, flips, i);
    assert(i.n === flips.length, 'Native planner returned an invalid flip set');
    return flipped;
  }

  async *#addQueryImpl(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
    queryName?: string,
  ): AsyncIterable<RowChange | 'yield'> {
    assert(
      this.initialized(),
      'Pipeline driver must be initialized before adding queries',
    );
    this.removeQuery(queryID);

    const planned = this.#planAst(query);

    let hydrated = false;
    let transformedAst = queryInfoAst(query, planned);
    try {
      yield* this.#addQueryStreaming(queryID, planned, timer);
      assert(
        this.#engine.setHydrationTimeMs(queryID, timer.totalElapsed()),
        `Missing hydrated query '${queryID}'`,
      );
      const resolved = this.#engine.queryTransformedAst(queryID);
      assert(resolved, `Missing transformed AST for query '${queryID}'`);
      transformedAst = queryInfoAst(query, JSON.parse(resolved) as AST);
      hydrated = true;
    } finally {
      if (hydrated) {
        // TS registers query metadata only after the hydrate generator has
        // completed. Failed or abandoned hydrations must never appear active.
        this.#queryInfo.set(queryID, {
          // `planned` is an engine-only AST (completeOrdering plus optional
          // Rust planner flips). PipelineDriver exposes the transformed query,
          // not its internal physical ordering, through queries(). Keep the
          // caller-visible AST here so planning does not leak into public state.
          transformedAst,
          transformationHash,
          ...(queryName !== undefined && {queryName}),
        });
      } else {
        // The streaming generator waits for its native task before returning
        // here, so this synchronous cleanup cannot race a blocking TSFN call.
        this.#engine.removeQuery(queryID);
        this.#rowSetSignatures.delete(queryID);
      }
    }
  }

  async *#addQueryStreaming(
    queryID: string,
    query: AST,
    timer: Timer,
  ): AsyncIterable<RowChange | 'yield'> {
    const queue = new AsyncQueue<RowChange | 'yield'>();
    const streamId = this.#nextStreamId++;

    const handleRow = (row: NapiRowChange) => {
      // changeType -3 is the end-of-stream barrier sentinel (see drain_barrier in
      // napi/src/lib.rs); it carries no data and must not be pushed as a row.
      if (row.changeType === END_STREAM_SENTINEL) {
        return;
      }
      try {
        const change = napiToRowChange(row, this.#tableSpecs);
        if (change.type !== ChangeType.EDIT) {
          const cur = this.#rowSetSignatures.get(change.queryID) ?? 0n;
          const unit = rowIDSignatureUnit({
            schema: '',
            table: change.table,
            rowKey: change.rowKey as RowKey,
          });
          this.#rowSetSignatures.set(change.queryID, cur ^ unit);
        }
        if (PERF_TRACE_JS) {
          const t0 = performance.now();
          queue.push(change);
          perfJS.queue.ms += performance.now() - t0;
          perfJS.queue.hits++;
        } else {
          queue.push(change);
        }
      } catch (error) {
        // Exceptions thrown from a raw TSFN callback otherwise escape outside
        // the async task promise and can terminate the whole syncer worker.
        queue.error(error);
        this.#engine.cancel?.();
        this.#engine.cancelStream?.(streamId);
      }
    };

    const spec = [{queryId: queryID, astJson: JSON.stringify(query)}];
    // The addon delivers CHUNKS: one TSFN callback carries an ordered array of
    // NapiRowChanges (up to RUST_IVM_DELIVERY_CHUNK; the final chunk ends with
    // the -3 END sentinel). Iterating here preserves the exact per-row order.
    const hydrated = this.#engine.addQueriesStreamingRows(
      spec,
      (_err: unknown, rows: NapiRowChange[]) => {
        for (const row of rows) {
          handleRow(row);
        }
      },
      streamId,
    );

    hydrated
      .then(() => deferClose(queue))
      .catch((e: unknown) => queue.error(normalizeNativeHydrateError(e)));

    // If the consumer abandons this stream early (break / thrown error /
    // client teardown), cancel the engine so the actor stops producing and
    // close the queue so any further pushed rows are dropped — otherwise the
    // engine materializes the whole result into a queue nobody drains
    // (O(result) memory + wasted compute). cancel() is out-of-band and
    // non-blocking; destroy() is fire-and-forget, so neither can wedge here.
    let count = 0;
    let completed = false;
    try {
      for await (const change of queue) {
        // #3 backpressure: this row has left the buffer, so return one credit
        // to the producer BEFORE we hand it downstream — the producer may be
        // parked waiting for exactly this credit.
        this.#engine.grantStreamCredit?.(streamId, 1);
        yield change;
        count++;
        if (timer.elapsedLap() > this.#yieldThresholdMs()) {
          yield 'yield';
        }
      }
      completed = true;
      perfJSReport(this.#lc, 'hydrate');
    } finally {
      // Only cancel on EARLY exit; on normal completion the engine already
      // finished and the token resets at the next op anyway. close() is
      // idempotent (deferClose already ran on success).
      if (!completed) {
        // Hard-cancel FIRST: flips the cancel token (so the producer sees
        // cancellation and skips the drain barrier) and closes the current
        // credit gate — then cancelStream closes THIS stream precisely. Order
        // matters: the token must be set before the unparked producer reaches
        // the barrier, else it blocks on the barrier's timeout (#3).
        this.#engine.cancel?.();
        this.#engine.cancelStream?.(streamId);
      }
      queue.close();
      if (!completed) {
        // Cancellation can race with a producer that already acquired credit
        // and entered a blocking TSFN call. Keep the JS event loop available
        // until that native task settles; otherwise the next synchronous actor
        // call (notably removeQuery during a re-add) can deadlock with the TSFN
        // callback that only this event loop can dispatch.
        await hydrated.catch(() => {});
      }
    }
  }

  async advance(timer: Timer): Promise<
    | {
        version: string;
        numChanges: number;
        changes:
          | Iterable<RowChange | 'yield'>
          | AsyncIterable<RowChange | 'yield'>;
      }
    | ResetPipelinesSignal
  > {
    assert(
      this.initialized(),
      'Pipeline driver must be initialized before advancing',
    );

    return this.#advanceStreaming(timer);
  }

  async #advanceStreaming(timer: Timer): Promise<
    | {
        version: string;
        numChanges: number;
        changes: AsyncIterable<RowChange | 'yield'>;
      }
    | ResetPipelinesSignal
  > {
    const queue = new AsyncQueue<NapiRowChange>();
    let headerResolve: ((row: NapiRowChange) => void) | null = null;
    let headerReject: ((e: unknown) => void) | null = null;
    const headerPromise = new Promise<NapiRowChange>((resolve, reject) => {
      headerResolve = resolve;
      headerReject = reject;
    });

    const streamId = this.#nextStreamId++;
    // Captured BEFORE the advance job starts: totalHydrationTimeMs is a
    // synchronous actor call, and once the streaming job is live on the actor
    // a queued sync call would deadlock against the credit gate (we would
    // block the event loop the producer needs for credit grants).
    const totalHydrationTimeMs = this.totalHydrationTimeMs();
    // Chunked delivery: each TSFN callback carries an ordered array. The first
    // element of the first chunk is the header (-1) — or a reset (-2) when the
    // advance resets before producing a header — and the final chunk ends with
    // the -3 END sentinel (skipped by the drain loop).
    const advancing = this.#engine
      .advanceToHeadStreamingRows((_err: unknown, rows: NapiRowChange[]) => {
        for (const row of rows) {
          if (headerResolve) {
            headerResolve(row);
            headerResolve = null;
            headerReject = null;
          } else if (PERF_TRACE_JS) {
            const t0 = performance.now();
            queue.push(row);
            perfJS.queue.ms += performance.now() - t0;
            perfJS.queue.hits++;
          } else {
            queue.push(row);
          }
        }
      }, streamId)
      .then(() => deferClose(queue))
      .catch((e: unknown) => {
        // The engine can fail BEFORE emitting the header (snapshotter advance
        // error, or engine/snapshotter not initialized): no callback fires, so
        // `await headerPromise` below would hang forever. Reject it as well as
        // erroring the queue, so the failure propagates as a teardown like TS.
        // If the header already arrived, headerReject is null (no-op).
        const error = normalizeNativeAdvanceError(e);
        queue.error(error);
        headerReject?.(error);
      });

    const header = await headerPromise;
    if (header.changeType === -2) {
      const hk = JSON.parse(header.rowKey);
      const reason = hk['reason'] ?? 'schema-change';
      const msg = hk['msg'] ?? 'advance reset';
      return new ResetPipelinesSignal(
        msg,
        reason as ResetPipelinesSignal['reason'],
      );
    }
    if (header.changeType !== -1) {
      throw new Error(
        'advanceToHeadStreaming expected header row (changeType=-1) as first row',
      );
    }
    const hk = JSON.parse(header.rowKey);
    const version = hk['version'] ?? '';
    const numChanges = hk['numChanges'] ?? 0;
    const aborted = hk['aborted'] ?? false;
    this.#lc.debug?.(
      `advanceToHead: version=${version} numChanges=${numChanges} aborted=${aborted}`,
    );

    if (version) {
      this.#currentVersion = version;
    }

    return {
      version,
      numChanges,
      changes: this.#advanceToHeadRowsStreaming(
        queue,
        aborted,
        streamId,
        advancing,
        timer,
        numChanges,
        totalHydrationTimeMs,
      ),
    };
  }

  async *#advanceToHeadRowsStreaming(
    queue: AsyncQueue<NapiRowChange>,
    aborted: boolean,
    streamId: number,
    advancing: Promise<unknown>,
    timer: Timer,
    numChanges: number,
    totalHydrationTimeMs: number,
  ): AsyncIterable<RowChange | 'yield'> {
    let count = 0;
    let completed = false;

    // Advancement-timeout breaker — port of PipelineDriver
    // #shouldAdvanceYieldMaybeAbortAdvance (pipeline-driver.ts). The stock
    // driver documents this as BOTH a circuit breaker for very large
    // transactions AND "a bound on the amount of time the previous connection
    // locks the inactive WAL file ... which can make the WAL grow continuously"
    // — i.e. it is a WAL-checkpoint-starvation bound, and the rust path must
    // enforce it too (the engine watchdog only bounds a LIVE engine job at its
    // 600s hard deadline; this scales the limit to the CG's hydration time).
    const maybeAbortAdvance = (pos: number) => {
      const elapsed = timer.totalElapsed();
      if (
        elapsed > MIN_ADVANCEMENT_TIME_LIMIT_MS &&
        (elapsed > totalHydrationTimeMs ||
          (elapsed > totalHydrationTimeMs / 2 && pos <= numChanges / 2))
      ) {
        throw new ResetPipelinesSignal(
          `Advancement exceeded timeout at ${pos} of ${numChanges} changes ` +
            `after ${elapsed} ms. Advancement time limited based on total ` +
            `hydration time of ${totalHydrationTimeMs} ms.`,
          'advancement-timeout',
        );
      }
    };

    // A thrown ResetPipelinesSignal (or the consumer abandoning early) exits
    // this generator mid-stream. On early exit, cancel the engine + close the
    // queue so the actor stops producing and further pushes are dropped, rather
    // than materializing the remaining changes into an undrained queue.
    try {
      for await (const row of queue) {
        // End-of-stream barrier sentinel — carries no change (see drain_barrier).
        if (row.changeType === END_STREAM_SENTINEL) {
          continue;
        }
        if (row.changeType === -2) {
          const rk = JSON.parse(row.rowKey);
          const reason = rk['reason'] ?? 'schema-change';
          const msg = rk['msg'] ?? 'advance reset';
          throw new ResetPipelinesSignal(
            msg,
            reason as ResetPipelinesSignal['reason'],
          );
        }

        // Stock parity: checked before each change is processed. (`count` is
        // emitted rows — a proxy for the stock driver's change position; rows
        // can exceed changes, which only makes the mid-point clause stricter
        // later, never laxer.)
        maybeAbortAdvance(count);

        // #3 backpressure: this DATA row left the buffer — return the one
        // credit the producer acquired for it (the header/-2 rows are not
        // credit-gated on the producer side, so we don't grant for them).
        this.#engine.grantStreamCredit?.(streamId, 1);
        const change = napiToRowChange(row, this.#tableSpecs);
        // The #queryInfo guard: a removeQuery racing rows still buffered in
        // this queue would otherwise resurrect the just-deleted signature
        // entry, orphaning it until the next remove/reset for that id.
        if (
          change.type !== ChangeType.EDIT &&
          this.#queryInfo.has(change.queryID)
        ) {
          const cur = this.#rowSetSignatures.get(change.queryID) ?? 0n;
          const unit = rowIDSignatureUnit({
            schema: '',
            table: change.table,
            rowKey: change.rowKey as RowKey,
          });
          this.#rowSetSignatures.set(change.queryID, cur ^ unit);
        }
        yield change;
        count++;
        if (timer.elapsedLap() > this.#yieldThresholdMs()) {
          yield 'yield';
        }
      }
      completed = true;
      perfJSReport(this.#lc, 'advance');
    } finally {
      if (!completed) {
        // See #addQueryStreaming: cancel() (token + gate) before cancelStream.
        this.#engine.cancel?.();
        this.#engine.cancelStream?.(streamId);
      }
      queue.close();
      if (!completed) {
        // See #addQueryStreaming: do not let a subsequent synchronous actor
        // call block the event loop while an already-credited TSFN delivery is
        // still completing.
        await advancing.catch(() => {});
      }
    }

    if (aborted) {
      this.#lc.warn?.(
        `advanceToHead: aborted after ${count} changes (budget exceeded)`,
      );
    }
  }
}

// Match the object shape produced by resolveSimpleScalarSubqueries for normal
// related/non-scalar queries. That resolver recursively rebuilds every AST with
// a `related` array and materializes `where: undefined` on the rebuilt parent.
// The Rust engine performs the equivalent graph transformation internally, so
// keep its public QueryInfo shape aligned without leaking the physical plan.
function queryInfoAst(original: AST, resolved: AST): AST {
  const where =
    original.where && resolved.where
      ? queryInfoCondition(original.where, resolved.where)
      : original.where;
  const related = original.related?.map((r, i) => ({
    ...r,
    subquery: queryInfoAst(
      r.subquery,
      resolved.related?.[i]?.subquery ?? r.subquery,
    ),
  }));
  return where !== original.where || related !== original.related
    ? {...original, where, related}
    : original;
}

function queryInfoCondition(
  original: Condition,
  resolved: Condition,
): Condition {
  if (original.type === 'correlatedSubquery') {
    // A simple scalar is replaced by a literal condition. This is the actual
    // native resolution result, including the resolved value/ALWAYS_FALSE.
    if (resolved.type !== 'correlatedSubquery') {
      return resolved;
    }
    const subquery = queryInfoAst(
      original.related.subquery,
      resolved.related.subquery,
    );
    return subquery === original.related.subquery
      ? original
      : {
          ...original,
          related: {...original.related, subquery},
        };
  }
  if (
    (original.type === 'and' || original.type === 'or') &&
    resolved.type === original.type
  ) {
    const conditions = original.conditions.map((condition, i) =>
      queryInfoCondition(condition, resolved.conditions[i] ?? condition),
    );
    return conditions.every((value, i) => value === original.conditions[i])
      ? original
      : {type: original.type, conditions};
  }
  return original;
}

// -- Rust planner flip application -----------------------------------------
// Applies the Rust planner's ordered `flip` decisions to an AST, walking in the
// SAME order the Rust side emits them (see planner::flip_order): WHERE
// conditions pre-order (recursing into each correlated subquery's own where),
// then `related` subqueries. `i.n` is a shared cursor into the flip list.

type PlanCond = NonNullable<AST['where']>;

function applyFlips(ast: AST, flips: (boolean | null)[], i: {n: number}): AST {
  const where = ast.where ? applyFlipsCond(ast.where, flips, i) : ast.where;
  const related = ast.related?.map(csq => ({
    ...csq,
    subquery: applyFlips(csq.subquery, flips, i),
  }));
  return {...ast, where, related};
}

function applyFlipsCond(
  cond: PlanCond,
  flips: (boolean | null)[],
  i: {n: number},
): PlanCond {
  switch (cond.type) {
    case 'simple':
      return cond;
    case 'correlatedSubquery': {
      const flip = flips[i.n++];
      const subWhere = cond.related.subquery.where;
      const subquery = subWhere
        ? {...cond.related.subquery, where: applyFlipsCond(subWhere, flips, i)}
        : cond.related.subquery;
      return {
        ...cond,
        flip: flip === null ? undefined : flip,
        related: {...cond.related, subquery},
      };
    }
    case 'and':
    case 'or':
      return {
        ...cond,
        conditions: cond.conditions.map(c => applyFlipsCond(c, flips, i)),
      };
    default:
      return cond;
  }
}
