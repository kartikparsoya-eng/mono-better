import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';

// Stream rows one-at-a-time via ThreadsafeFunction instead of materializing
// the full result array. O(1) JS objects in flight vs O(result). Default ON
// — matches TS's per-row streaming invariant. Set RUST_IVM_STREAM_ROWS=0 to
// fall back to the eager array path (for debugging/diffing only).
const STREAM_ROWS = process.env['RUST_IVM_STREAM_ROWS'] !== '0';

import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import {must} from '../../../../shared/src/must.ts';
import {type RowKey} from '../../types/row-key.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {LogConfig, ZeroConfig} from '../../config/zero-config.ts';
import type {ClientGroupStorage} from '../../../../zqlite/src/database-storage.ts';
import {completeOrdering} from '../../../../zql/src/query/complete-ordering.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {computeZqlSpecs} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../db/specs.ts';
import type {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {type ShardID} from '../../types/shards.ts';
import {
  getSubscriptionState,
} from '../replicator/schema/replication-state.ts';
import {checkClientSchema} from './client-schema.ts';
import {ResetPipelinesSignal} from './snapshotter.ts';
import {
  reloadPermissionsIfChanged,
  type LoadedPermissions,
} from '../../auth/load-permissions.ts';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import {
  rowIDSignatureUnit,
} from './row-set-signature.ts';
import type {StatementRunner} from '../../db/statements.ts';

// NAPI addon types
// NapiValue interface retained for NapiTableSpec columns that still use it.
export interface NapiValue {
  kind: string;
  boolVal?: boolean;
  f64Val?: number;
  strVal?: string;
  jsonVal?: string;
}

export interface NapiRowChange {
  changeType: number;
  queryId: string;
  table: string;
  rowKey: string;
  row?: string;
  isHidden: boolean;
}

export interface NapiQuerySpec {
  queryId: string;
  astJson: string;
}

export interface NapiQueryResult {
  queryId: string;
  changes: NapiRowChange[];
}

export interface NapiSourceChange {
  table: string;
  changeType: string;
  row: Record<string, NapiValue>;
  oldRow?: Record<string, NapiValue>;
}

export interface NapiTableSchema {
  columns: Record<string, {type: string; optional: boolean}>;
  primaryKey: string[];
  uniqueKeys?: string[][];
  minRowVersion?: string;
}

export interface NapiTableSpec {
  table: string;
  columns: Record<string, {type: string; optional: boolean}>;
  primaryKey: string[];
  // All unique keys (PK plus secondary unique indexes). Drives scalar-EXISTS
  // subquery resolution in the engine; omitting them degrades scalar subqueries
  // keyed on a non-PK unique index to a live per-parent Exists (see G8 gap).
  uniqueKeys?: string[][];
  minRowVersion?: string;
}

// Try to load the native addon (use createRequire for ESM compatibility with tsx)
import {createRequire} from 'node:module';
const nodeRequire = createRequire(import.meta.url);
let RustIvmEngineClass: unknown = null;
const addonPath = process.env['RUST_IVM_ADDON_PATH'] ?? '../../../../packages/rust-ivm/napi/rust-ivm.node';
try {
  RustIvmEngineClass = (nodeRequire(addonPath) as {RustIvmEngine: new () => unknown}).RustIvmEngine;
} catch (e) {
  console.error('[rust-ivm-driver] Failed to load addon from', addonPath, ':', (e as Error).message);
}

export type {Timer} from './pipeline-driver.ts';
import type {Timer} from './pipeline-driver.ts';

export type RowChange = {
  readonly type: number;
  readonly queryID: string;
  readonly table: string;
  readonly rowKey: Row;
  readonly row: Row | undefined;
};

type QueryInfo = {
  readonly transformedAst: AST;
  readonly transformationHash: string;
  readonly queryName?: string | undefined;
};

// napiToRow and fromNapiValue removed — row data is now JSON strings.

function napiToRowChange(c: NapiRowChange): RowChange {
  return {
    type: c.changeType,
    queryID: c.queryId,
    table: c.table,
    rowKey: JSON.parse(c.rowKey) as any,
    row: c.row ? (JSON.parse(c.row) as any) : undefined,
  };
}

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

// ---------------------------------------------------------------------------
// AsyncQueue — bounded bridge between TSFN callbacks and async generators
// ---------------------------------------------------------------------------

/**
 * A simple async queue that bridges the napi ThreadsafeFunction (push model)
 * with the view-syncer's `for await` consumption (pull model).
 *
 * The TSFN callback calls `push()` synchronously; the async generator drains
 * via `for await`. With TSFN Blocking mode + the actor thread parking on a
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
   * to apply backpressure — though with max_queue_size=1 the TSFN itself
   * already blocks the actor thread, so this is a second safety net for the
   * JS-side buffer.
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
 * `setImmediate` defers close to the check phase, which libuv runs AFTER the
 * poll phase where the TSFN callback is dispatched. Because the TSFN is bounded
 * (`max_queue_size=1`, Blocking) there is at most one undelivered row when the
 * Promise resolves, and no row is ever enqueued after it — so one deferral is
 * sufficient for the queue to drain before it closes.
 *
 * NOTE: this correctness argument depends on the TSFN staying small-bounded. If
 * `max_queue_size` is ever raised for throughput, prefer an explicit
 * end-of-stream sentinel over event-loop-phase timing.
 */
export function deferClose<T>(queue: AsyncQueue<T>): void {
  setImmediate(() => queue.close());
}

// ---------------------------------------------------------------------------
// RustIVMDriver
// ---------------------------------------------------------------------------

export class RustIVMDriver {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly #engine: any;
  readonly #lc: LogContext;
  readonly #shardID: ShardID;
  readonly #storage: ClientGroupStorage;
  readonly #config: ZeroConfig | undefined;
  readonly #replicaFile: string;
  readonly #planEnabled: boolean = false;
  readonly #tableSpecs = new Map<string, LiteAndZqlSpec>();
  readonly #allTableNames = new Set<string>();
  readonly #primaryKeys = new Map<string, PrimaryKey>();
  // Cost model disabled for single-owner: no TS Database to prepare
  // statements against. completeOrdering alone is correct (slower on
  // OR-with-CSQ). TODO: expose cost model via Rust NAPI.
  #replicaVersion: string | null = null;
  #permissions: LoadedPermissions | null = null;
  #permissionsVersion: string | null = null;
  #queryInfo = new Map<string, QueryInfo>();
  readonly #rowSetSignatures = new Map<string, bigint>();
  #totalHydrationTimeMs = 0;
  #initialized = false;

  constructor(
    lc: LogContext,
    _logConfig: LogConfig,
    shardID: ShardID,
    storage: ClientGroupStorage,
    clientGroupID: string,
    _inspectorDelegate: InspectorDelegate,
    _yieldThresholdMs: () => number,
    enablePlanner?: boolean,
    config?: ZeroConfig,
    replicaFile?: string,
  ) {
    assert(RustIvmEngineClass, 'Rust IVM NAPI addon not loaded');
    this.#lc = lc.withContext('clientGroupID', clientGroupID);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    this.#engine = new (RustIvmEngineClass as any)();
    this.#shardID = shardID;
    this.#storage = storage;
    this.#config = config;
    this.#replicaFile = replicaFile ?? '';
    // Query planner (#planAstForRust): runs the Rust-ported plan graph over a
    // cost model backed by the Rust-owned snapshot connection, annotating
    // `flip` on OR-with-CSQ conditions (parity with zero 1.7's default-on
    // planner). Ships DARK: requires config.enableQueryPlanner AND an explicit
    // env opt-in until validated in ART.
    this.#planEnabled = enablePlanner === true && process.env.RUST_IVM_PLANNER === '1';
  }

  // Single-owner design: the Rust engine exclusively owns replica.db's
  // connection. All TS reads go through NAPI methods on the Rust side.
  // This minimal StatementRunner adapter lets computeZqlSpecs and
  // getSubscriptionState run unchanged against the Rust-owned connection.
  // Bind params are passed as a JSON array to engine.readQuery.
  #runner(): object {
    const engine = this.#engine;
    const readQuery = (sql: string, params?: unknown[]): Record<string, unknown>[] =>
      JSON.parse(
        engine.readQuery(sql, params ? JSON.stringify(params) : null),
      ) as Record<string, unknown>[];
    return {
      get: (sql: string, ...args: unknown[]) => readQuery(sql, args)[0],
      all: (sql: string, ...args: unknown[]) => readQuery(sql, args),
      prepare: (sql: string) => {
        return {
          all: (...args: unknown[]) => readQuery(sql, args),
          get: (...args: unknown[]) => readQuery(sql, args)[0],
          iterate: function* (...args: unknown[]): Generator<Record<string, unknown>> {
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
    const fullTables = new Map<string, unknown>();
    computeZqlSpecs(
      this.#lc,
      runner as unknown as Database,
      {includeBackfillingColumns: false},
      this.#tableSpecs,
      fullTables as any,
    );
    checkClientSchema(
      this.#shardID,
      clientSchema,
      this.#tableSpecs,
      fullTables as any,
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

    const tableSpecs: NapiTableSpec[] = [];
    for (const [table, spec] of this.#tableSpecs.entries()) {
      const columns: Record<string, {type: string; optional: boolean}> = {};
      for (const [col, schemaValue] of Object.entries(spec.zqlSpec)) {
        columns[col] = {
          type: schemaValue.type,
          optional: schemaValue.optional ?? false,
        };
      }
      tableSpecs.push({
        table,
        columns,
        primaryKey: [...spec.tableSpec.primaryKey],
        // Pass ALL unique keys (PK plus secondary unique indexes), not just the
        // PK. The engine uses these to resolve scalar EXISTS subqueries keyed on
        // a non-PK unique index (e.g. channel_participants(channelId,userId) in
        // the conversation ACL). Without them the scalar can't be pre-resolved,
        // degrades to a live per-parent Exists, and the matched row is only
        // streamed as a hidden companion — diverging from TS (missing rows in
        // the client store; see the G8 diff-oracle gap).
        uniqueKeys: spec.tableSpec.uniqueKeys.map(key => [...key]),
        ...(spec.tableSpec.minRowVersion && {minRowVersion: spec.tableSpec.minRowVersion}),
      });
    }
    // Pass db_path to init() so sources + SQLite path are set atomically on
    // the worker thread (avoids race where setDatabasePath runs before Init).
    this.#engine.init(
      tableSpecs,
      this.#replicaFile || null,
      this.#shardID.appID,
    );
    this.#lc.info?.(`RustIVMDriver: init complete, db=${this.#replicaFile}`);

    const {replicaVersion} = getSubscriptionState(
      this.#runner() as unknown as StatementRunner,
    );
    this.#replicaVersion = replicaVersion;
  }

  get replicaVersion(): string {
    return must(this.#replicaVersion, 'Not yet initialized');
  }

  currentVersion(): string {
    // #replicaVersion is updated by init/advance/advanceWithoutDiff.
    // Single-owner: only the Rust engine has a replica connection.
    assert(this.initialized(), 'Not yet initialized');
    return must(this.#replicaVersion, 'Not yet initialized');
  }

  currentPermissions(): LoadedPermissions | null {
    assert(this.initialized(), 'Not yet initialized');
    // Cache: only re-query permissions when the replica version changes.
    // In TS this is a direct SQLite call (microseconds), but in the Rust
    // single-owner path each read_query goes through the actor thread
    // (synchronous NAPI call). Without caching, this fires on every
    // query batch — blocking the JS event loop dozens of times per
    // hydration cycle.
    if (this.#permissions !== null && this.#permissionsVersion === this.#replicaVersion) {
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
    this.#permissionsVersion = this.#replicaVersion;
    return this.#permissions;
  }

  advanceWithoutDiff(): string {
    // Single-owner: the Rust engine advances its own snapshotter.
    const version = this.#engine.advanceWithoutDiff();
    this.#replicaVersion = version;
    return version;
  }

  async destroy(): Promise<void> {
    // Single-owner teardown: the Rust engine owns every SQLite connection.
    // Await its destroy() so the Promise only resolves once the actor thread
    // confirms all connections are closed + engine graph freed. No TS
    // connection exists to race with.
    await this.#engine.destroy();
    this.#storage.destroy();
  }

  queries(): ReadonlyMap<string, QueryInfo> {
    return this.#queryInfo;
  }

  totalHydrationTimeMs(): number {
    return this.#totalHydrationTimeMs;
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
    const primaryKey = this.#primaryKeys.get(table);
    if (!primaryKey) {
      return undefined;
    }
    const cols = Object.keys(pk);
    const where = cols.map(c => `${quoteIdent(c)} = ?`).join(' AND ');
    const sql = `SELECT * FROM ${quoteIdent(table)} WHERE ${where}`;
    const params = cols.map(c => pk[c]);
    const row = (this.#runner() as any).get(sql, ...params);
    return row as Row | undefined;
  }

  addQuery(
    transformationHash: string,
    queryID: string,
    query: AST,
    _timer: Timer,
    queryName?: string,
  ): AsyncIterable<RowChange | 'yield'> {
    return this.#addQueryImpl(transformationHash, queryID, query, queryName);
  }

  #planAst(ast: AST): AST {
    const ordered = completeOrdering(
      ast,
      tableName =>
        must(this.#primaryKeys.get(tableName), `no primary key for table '${tableName}'`),
    );
    if (!this.#planEnabled) {
      return ordered;
    }
    // Ask the Rust engine for the flip decisions (cost model on the snapshot
    // connection), then apply them to our AST in the SAME canonical order the
    // Rust side used (WHERE pre-order, recursing into each subquery's where,
    // then `related`). Never fail hydration on a planner hiccup — fall back to
    // the un-flipped ordering.
    try {
      const flips: (boolean | null)[] = JSON.parse(
        this.#engine.planAst(JSON.stringify(ordered)),
      );
      const i = {n: 0};
      const flipped = applyFlips(ordered, flips, i);
      return i.n === flips.length ? flipped : ordered;
    } catch (e) {
      this.#lc.warn?.('rust planner failed; using unplanned AST', e);
      return ordered;
    }
  }

  async *#addQueryImpl(
    transformationHash: string,
    queryID: string,
    query: AST,
    queryName?: string,
  ): AsyncIterable<RowChange | 'yield'> {
    assert(this.initialized(), 'Pipeline driver must be initialized before adding queries');
    this.removeQuery(queryID);

    const planned = this.#planAst(query);

    this.#queryInfo.set(queryID, {
      transformedAst: planned,
      transformationHash,
      ...(queryName !== undefined && {queryName}),
    });
    this.#rowSetSignatures.set(queryID, 0n);

    if (STREAM_ROWS) {
      yield* this.#addQueryStreaming(queryID, planned);
    } else {
      yield* this.#addQueryEager(queryID, planned);
    }
  }

  async *#addQueryEager(
    queryID: string,
    query: AST,
  ): AsyncIterable<RowChange | 'yield'> {
    const out = await this.#engine.addQueriesStreaming([
      {queryId: queryID, astJson: JSON.stringify(query)},
    ]);

    let count = 0;
    for (const row of out) {
      const change = napiToRowChange(row);
      if (change.type !== ChangeType.EDIT) {
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
      if (count % 100 === 0) {
        yield 'yield';
      }
    }
  }

  async *#addQueryStreaming(
    queryID: string,
    query: AST,
  ): AsyncIterable<RowChange | 'yield'> {
    const queue = new AsyncQueue<RowChange | 'yield'>();

    this.#engine
      .addQueriesStreamingRows(
        [{queryId: queryID, astJson: JSON.stringify(query)}],
        (_err: unknown, row: NapiRowChange) => {
          const change = napiToRowChange(row);
          if (change.type !== ChangeType.EDIT) {
            const cur = this.#rowSetSignatures.get(change.queryID) ?? 0n;
            const unit = rowIDSignatureUnit({
              schema: '',
              table: change.table,
              rowKey: change.rowKey as RowKey,
            });
            this.#rowSetSignatures.set(change.queryID, cur ^ unit);
          }
          queue.push(change);
        },
      )
      .then(() => deferClose(queue))
      .catch((e: unknown) => queue.error(e));

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
        yield change;
        count++;
        if (count % 100 === 0) {
          yield 'yield';
        }
      }
      completed = true;
    } finally {
      // Only cancel on EARLY exit; on normal completion the engine already
      // finished and the token resets at the next op anyway. close() is
      // idempotent (deferClose already ran on success).
      if (!completed) {
        this.#engine.cancel?.();
      }
      queue.close();
    }
  }

  async advance(_timer: Timer): Promise<
    | {version: string; numChanges: number; changes: Iterable<RowChange | 'yield'> | AsyncIterable<RowChange | 'yield'>}
    | ResetPipelinesSignal
  > {
    assert(this.initialized(), 'Pipeline driver must be initialized before advancing');

    if (STREAM_ROWS) {
      return this.#advanceStreaming();
    }
    return this.#advanceEager();
  }

  async #advanceEager(): Promise<
    | {version: string; numChanges: number; changes: Iterable<RowChange | 'yield'>}
    | ResetPipelinesSignal
  > {
    const rows = await this.#engine.advanceToHeadStreaming();

    const headerRow = rows[0];
    if (headerRow === null || headerRow === undefined) {
      throw new Error('advanceToHeadStreaming returned no rows');
    }
    if (headerRow.changeType === -2) {
      const reason = headerRow.rowKey['reason']?.strVal ?? 'schema-change';
      const msg = headerRow.rowKey['msg']?.strVal ?? 'advance reset';
      return new ResetPipelinesSignal(msg, reason as ResetPipelinesSignal['reason']);
    }
    if (headerRow.changeType !== -1) {
      throw new Error('advanceToHeadStreaming expected header row (changeType=-1) as first row');
    }
    const version = headerRow.rowKey['version']?.strVal ?? '';
    const numChanges = headerRow.rowKey['numChanges']?.f64Val ?? 0;
    const aborted = headerRow.rowKey['aborted']?.boolVal ?? false;
    this.#lc.debug?.(`advanceToHead: version=${version} numChanges=${numChanges} aborted=${aborted}`);

    if (version) {
      this.#replicaVersion = version;
    }

    return {
      version,
      numChanges,
      changes: this.#advanceToHeadRows(rows, aborted),
    };
  }

  async #advanceStreaming(): Promise<
    | {version: string; numChanges: number; changes: AsyncIterable<RowChange | 'yield'>}
    | ResetPipelinesSignal
  > {
    const queue = new AsyncQueue<NapiRowChange>();
    let headerResolve: ((row: NapiRowChange) => void) | null = null;
    let headerReject: ((e: unknown) => void) | null = null;
    const headerPromise = new Promise<NapiRowChange>((resolve, reject) => {
      headerResolve = resolve;
      headerReject = reject;
    });

    this.#engine
      .advanceToHeadStreamingRows((_err: unknown, row: NapiRowChange) => {
        if (headerResolve) {
          headerResolve(row);
          headerResolve = null;
          headerReject = null;
        } else {
          queue.push(row);
        }
      })
      .then(() => deferClose(queue))
      .catch((e: unknown) => {
        // The engine can fail BEFORE emitting the header (snapshotter advance
        // error, or engine/snapshotter not initialized): no callback fires, so
        // `await headerPromise` below would hang forever. Reject it as well as
        // erroring the queue, so the failure propagates as a teardown like TS.
        // If the header already arrived, headerReject is null (no-op).
        queue.error(e);
        headerReject?.(e);
      });

    const header = await headerPromise;
    if (header.changeType === -2) {
      const hk = JSON.parse(header.rowKey);
      const reason = hk['reason'] ?? 'schema-change';
      const msg = hk['msg'] ?? 'advance reset';
      return new ResetPipelinesSignal(msg, reason as ResetPipelinesSignal['reason']);
    }
    if (header.changeType !== -1) {
      throw new Error('advanceToHeadStreaming expected header row (changeType=-1) as first row');
    }
    const hk = JSON.parse(header.rowKey);
    const version = hk['version'] ?? '';
    const numChanges = hk['numChanges'] ?? 0;
    const aborted = hk['aborted'] ?? false;
    this.#lc.debug?.(`advanceToHead: version=${version} numChanges=${numChanges} aborted=${aborted}`);

    if (version) {
      this.#replicaVersion = version;
    }

    return {
      version,
      numChanges,
      changes: this.#advanceToHeadRowsStreaming(queue, aborted),
    };
  }

  *#advanceToHeadRows(
    rows: NapiRowChange[],
    aborted: boolean,
  ): Iterable<RowChange | 'yield'> {
    let count = 0;

    // Skip index 0 (the header row consumed by advance()).
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];

      if (row.changeType === -2) {
        const rk = JSON.parse(row.rowKey); const reason = rk['reason'] ?? 'schema-change';
        const msg = rk['msg'] ?? 'advance reset';
        throw new ResetPipelinesSignal(msg, reason as ResetPipelinesSignal['reason']);
      }

      const change = napiToRowChange(row);
      if (change.type !== ChangeType.EDIT) {
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
      if (count % 100 === 0) {
        yield 'yield';
      }
    }

    if (aborted) {
      this.#lc.warn?.(`advanceToHead: aborted after ${count} changes (budget exceeded)`);
    }
  }

  async *#advanceToHeadRowsStreaming(
    queue: AsyncQueue<NapiRowChange>,
    aborted: boolean,
  ): AsyncIterable<RowChange | 'yield'> {
    let count = 0;
    let completed = false;

    // A thrown ResetPipelinesSignal (or the consumer abandoning early) exits
    // this generator mid-stream. On early exit, cancel the engine + close the
    // queue so the actor stops producing and further pushes are dropped, rather
    // than materializing the remaining changes into an undrained queue.
    try {
      for await (const row of queue) {
        if (row.changeType === -2) {
          const rk = JSON.parse(row.rowKey); const reason = rk['reason'] ?? 'schema-change';
          const msg = rk['msg'] ?? 'advance reset';
          throw new ResetPipelinesSignal(msg, reason as ResetPipelinesSignal['reason']);
        }

        const change = napiToRowChange(row);
        if (change.type !== ChangeType.EDIT) {
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
        if (count % 100 === 0) {
          yield 'yield';
        }
      }
      completed = true;
    } finally {
      if (!completed) {
        this.#engine.cancel?.();
      }
      queue.close();
    }

    if (aborted) {
      this.#lc.warn?.(`advanceToHead: aborted after ${count} changes (budget exceeded)`);
    }
  }
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
