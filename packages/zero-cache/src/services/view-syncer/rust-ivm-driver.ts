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
import {computeZqlSpecs} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../db/specs.ts';
import type {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {type ShardID} from '../../types/shards.ts';
import {
  getSubscriptionState,
} from '../replicator/schema/replication-state.ts';
import {checkClientSchema} from './client-schema.ts';
import type {Snapshotter} from './snapshotter.ts';
import {ResetPipelinesSignal} from './snapshotter.ts';
import {
  reloadPermissionsIfChanged,
  type LoadedPermissions,
} from '../../auth/load-permissions.ts';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import {
  rowIDSignatureUnit,
} from './row-set-signature.ts';

// NAPI addon types
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
  rowKey: Record<string, NapiValue>;
  row?: Record<string, NapiValue>;
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
  minRowVersion?: string;
}

// Try to load the native addon (use createRequire for ESM compatibility with tsx)
import {createRequire} from 'node:module';
const nodeRequire = createRequire(import.meta.url);
let RustIvmEngineClass: unknown = null;
const addonPath = process.env['RUST_IVM_ADDON_PATH'] ?? '../../../../../../rust-ivm/napi/rust-ivm.node';
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

function fromNapiValue(val: NapiValue): unknown {
  switch (val.kind) {
    case 'null':
      return null;
    case 'bool':
      return val.boolVal ?? false;
    case 'f64':
      return val.f64Val ?? 0;
    case 'str':
      return val.strVal ?? '';
    case 'json':
      return JSON.parse(val.jsonVal ?? 'null');
    default:
      return null;
  }
}

function napiToRow(row: Record<string, NapiValue> | undefined): Row | undefined {
  if (!row) return undefined;
  return Object.fromEntries(
    Object.entries(row).map(([k, v]) => [k, fromNapiValue(v)]),
  ) as Row;
}

function napiToRowChange(c: NapiRowChange): RowChange {
  return {
    type: c.changeType,
    queryID: c.queryId,
    table: c.table,
    rowKey: napiToRow(c.rowKey) ?? {},
    row: napiToRow(c.row),
  };
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
 * full TSFN queue, at most O(1) rows are in flight.
 */
class AsyncQueue<T> implements AsyncIterable<T> {
  #items: T[] = [];
  #waiters: {
    resolve: (r: IteratorResult<T>) => void;
    reject: (e: unknown) => void;
  }[] = [];
  #done = false;
  #error: unknown = null;

  push(item: T): void {
    if (this.#done) return;
    const waiter = this.#waiters.shift();
    if (waiter) {
      waiter.resolve({value: item, done: false});
    } else {
      this.#items.push(item);
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
  }

  error(e: unknown): void {
    this.#error = e;
    this.close();
  }

  async next(): Promise<IteratorResult<T>> {
    if (this.#items.length > 0) {
      return {value: this.#items.shift()!, done: false};
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

// ---------------------------------------------------------------------------
// RustIVMDriver
// ---------------------------------------------------------------------------

export class RustIVMDriver {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly #engine: any;
  readonly #lc: LogContext;
  readonly #snapshotter: Snapshotter;
  readonly #shardID: ShardID;
  readonly #storage: ClientGroupStorage;
  readonly #config: ZeroConfig | undefined;
  readonly #replicaFile: string;
  readonly #tableSpecs = new Map<string, LiteAndZqlSpec>();
  readonly #allTableNames = new Set<string>();
  readonly #primaryKeys = new Map<string, PrimaryKey>();
  #replicaVersion: string | null = null;
  #permissions: LoadedPermissions | null = null;
  #queryInfo = new Map<string, QueryInfo>();
  readonly #rowSetSignatures = new Map<string, bigint>();
  #totalHydrationTimeMs = 0;

  constructor(
    lc: LogContext,
    _logConfig: LogConfig,
    snapshotter: Snapshotter,
    shardID: ShardID,
    storage: ClientGroupStorage,
    clientGroupID: string,
    _inspectorDelegate: InspectorDelegate,
    _yieldThresholdMs: () => number,
    _enablePlanner?: boolean,
    config?: ZeroConfig,
    replicaFile?: string,
  ) {
    assert(RustIvmEngineClass, 'Rust IVM NAPI addon not loaded');
    this.#lc = lc.withContext('clientGroupID', clientGroupID);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    this.#engine = new (RustIvmEngineClass as any)();
    this.#snapshotter = snapshotter;
    this.#shardID = shardID;
    this.#storage = storage;
    this.#config = config;
    this.#replicaFile = replicaFile ?? '';
  }

  init(clientSchema: ClientSchema) {
    assert(!this.#snapshotter.initialized(), 'Already initialized');
    this.#snapshotter.init();
    void this.#initAndResetCommon(clientSchema);
  }

  initialized(): boolean {
    return this.#snapshotter.initialized();
  }

  reset(clientSchema: ClientSchema) {
    this.#engine.reset();
    this.#queryInfo.clear();
    this.#rowSetSignatures.clear();
    void this.#initAndResetCommon(clientSchema);
  }

  #initAndResetCommon(clientSchema: ClientSchema) {
    const {db} = this.#snapshotter.current();
    const fullTables = new Map<string, unknown>();
    computeZqlSpecs(
      this.#lc,
      db.db,
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

    const {replicaVersion} = getSubscriptionState(db);
    this.#replicaVersion = replicaVersion;
  }

  get replicaVersion(): string {
    return must(this.#replicaVersion, 'Not yet initialized');
  }

  currentVersion(): string {
    assert(this.initialized(), 'Not yet initialized');
    return this.#snapshotter.current().version;
  }

  currentPermissions(): LoadedPermissions | null {
    assert(this.initialized(), 'Not yet initialized');
    const res = reloadPermissionsIfChanged(
      this.#lc,
      this.#snapshotter.current().db,
      this.#shardID.appID,
      this.#permissions,
      this.#config,
    );
    if (res.changed) {
      this.#permissions = res.permissions;
    }
    return this.#permissions;
  }

  advanceWithoutDiff(): string {
    const {version} = this.#snapshotter.advanceWithoutDiff().curr;
    return version;
  }

  destroy() {
    // The engine is a JS-owned value — it's cleaned up by GC/Drop.
    // We still call destroy() for explicit cleanup (clears pipelines/sources).
    this.#engine.destroy?.();
    this.#storage.destroy();
    this.#snapshotter.destroy();
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

  getRow(_table: string, _pk: RowKey): Row | undefined {
    return undefined;
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

  async *#addQueryImpl(
    transformationHash: string,
    queryID: string,
    query: AST,
    queryName?: string,
  ): AsyncIterable<RowChange | 'yield'> {
    assert(this.initialized(), 'Pipeline driver must be initialized before adding queries');
    this.removeQuery(queryID);

    this.#queryInfo.set(queryID, {
      transformedAst: query,
      transformationHash,
      ...(queryName !== undefined && {queryName}),
    });
    this.#rowSetSignatures.set(queryID, 0n);

    if (STREAM_ROWS) {
      yield* this.#addQueryStreaming(queryID, query);
    } else {
      yield* this.#addQueryEager(queryID, query);
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
      .then(() => queue.close())
      .catch((e: unknown) => queue.error(e));

    let count = 0;
    for await (const change of queue) {
      yield change;
      count++;
      if (count % 100 === 0) {
        yield 'yield';
      }
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
    const headerPromise = new Promise<NapiRowChange>(r => {
      headerResolve = r;
    });

    this.#engine
      .advanceToHeadStreamingRows((_err: unknown, row: NapiRowChange) => {
        if (headerResolve) {
          headerResolve(row);
          headerResolve = null;
        } else {
          queue.push(row);
        }
      })
      .then(() => queue.close())
      .catch((e: unknown) => queue.error(e));

    const header = await headerPromise;
    if (header.changeType === -2) {
      const reason = header.rowKey['reason']?.strVal ?? 'schema-change';
      const msg = header.rowKey['msg']?.strVal ?? 'advance reset';
      return new ResetPipelinesSignal(msg, reason as ResetPipelinesSignal['reason']);
    }
    if (header.changeType !== -1) {
      throw new Error('advanceToHeadStreaming expected header row (changeType=-1) as first row');
    }
    const version = header.rowKey['version']?.strVal ?? '';
    const numChanges = header.rowKey['numChanges']?.f64Val ?? 0;
    const aborted = header.rowKey['aborted']?.boolVal ?? false;
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
        const reason = row.rowKey['reason']?.strVal ?? 'schema-change';
        const msg = row.rowKey['msg']?.strVal ?? 'advance reset';
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

    for await (const row of queue) {
      if (row.changeType === -2) {
        const reason = row.rowKey['reason']?.strVal ?? 'schema-change';
        const msg = row.rowKey['msg']?.strVal ?? 'advance reset';
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
}
