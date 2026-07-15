import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import {must} from '../../../../shared/src/must.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import type {LogConfig, ZeroConfig} from '../../config/zero-config.ts';
import type {ClientGroupStorage} from '../../../../zqlite/src/database-storage.ts';
import {computeZqlSpecs} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../db/specs.ts';
import type {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {type RowKey} from '../../types/row-key.ts';
import {type ShardID} from '../../types/shards.ts';
import {
  getSubscriptionState,
} from '../replicator/schema/replication-state.ts';
import {checkClientSchema} from './client-schema.ts';
import type {Snapshotter} from './snapshotter.ts';
import {type SnapshotDiff} from './snapshotter.ts';
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
}

// Try to load the native addon (use createRequire for ESM compatibility with tsx)
import {createRequire} from 'node:module';
const nodeRequire = createRequire(import.meta.url);
let RustIvmEngineClass: unknown = null;
try {
  RustIvmEngineClass = (nodeRequire('../../../../../rust-ivm/napi/rust-ivm.node') as {RustIvmEngine: new () => unknown}).RustIvmEngine;
} catch {
  // Addon not available - will be caught at construction time
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

function toNapiValue(val: unknown): NapiValue {
  if (val === null || val === undefined) {
    return {kind: 'null'};
  }
  if (typeof val === 'boolean') {
    return {kind: 'bool', boolVal: val};
  }
  if (typeof val === 'number') {
    return {kind: 'f64', f64Val: val};
  }
  if (typeof val === 'string') {
    return {kind: 'str', strVal: val};
  }
  return {kind: 'json', jsonVal: JSON.stringify(val)};
}

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

function rowToNapi(row: Row): Record<string, NapiValue> {
  const result: Record<string, NapiValue> = {};
  for (const [k, v] of Object.entries(row)) {
    result[k] = toNapiValue(v);
  }
  return result;
}

function napiToRow(row: Record<string, NapiValue> | undefined): Row | undefined {
  if (!row) return undefined;
  const result: Row = {};
  for (const [k, v] of Object.entries(row)) {
    result[k] = fromNapiValue(v);
  }
  return result;
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

function getRowKey(cols: PrimaryKey, row: Row): RowKey {
  return Object.fromEntries(cols.map(col => [col, must(row[col])] as const));
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
    void this.#engine.reset();
    this.#queryInfo.clear();
    this.#rowSetSignatures.clear();
    void this.#initAndResetCommon(clientSchema);
  }

  #initPromise: Promise<void> | null = null;

  async #initAndResetCommon(clientSchema: ClientSchema) {
    const {db} = this.#snapshotter.current();
    const fullTables = new Map<string, unknown>();
    computeZqlSpecs(
      this.#lc,
      db.db,
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
        primaryKey: spec.tableSpec.primaryKey,
      });
      if (spec.tableSpec.minRowVersion) {
        this.#engine.setTableSpec(table, spec.tableSpec.minRowVersion);
      }
    }
    // Pass db_path to init() so sources + SQLite path are set atomically on
    // the worker thread (avoids race where setDatabasePath runs before Init).
    this.#initPromise = this.#engine.init(
      tableSpecs,
      this.#replicaFile || null,
    );
    try {
      await this.#initPromise;
      this.#lc.info?.(`RustIVMDriver: init complete, db=${this.#replicaFile}`);
    } catch (e) {
      this.#lc.error?.(`RustIVMDriver: init failed: ${(e as Error).message}`);
    }

    const {replicaVersion} = getSubscriptionState(db);
    this.#replicaVersion = replicaVersion;
  }

  gotComplete(queryID: string): boolean {
    return this.#queryInfo.has(queryID);
  }

  async awaitGoInit(): Promise<void> {
    if (this.#initPromise) {
      await this.#initPromise;
    }
  }

  get canBatchHydrate(): boolean {
    return false;
  }

  hydrateVersion(): string {
    return this.currentVersion();
  }

  async *goHydrateBatchStream(): AsyncGenerator<never, void, unknown> {
    throw new Error('goHydrateBatchStream not supported by RustIVMDriver');
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
    this.#engine.destroy();
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
    void this.#engine.removeQuery(queryID);
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

    // PULL-BASED: get a stream iterator from the worker thread.
    // Worker pushes rows to a queue in the background; we pull via next().
    // This matches TS: `for await (const change of await pipelines.addQuery(...))`.
    const iterator = await this.#engine.addQueriesStreaming([
      {queryId: queryID, ast: query},
    ]);

    let count = 0;
    // Pull rows one at a time — true streaming, no collecting.
    while (true) {
      const row = await iterator.next();
      if (row === null || row === undefined) break;
      const change = napiToRowChange(row);
      // Track rowSetSignature locally (XOR for ADD/REMOVE, skip EDIT).
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

  advance(timer: Timer): {
    version: string;
    numChanges: number;
    changes: AsyncIterable<RowChange | 'yield'>;
  } {
    assert(this.initialized(), 'Pipeline driver must be initialized before advancing');
    const diff = this.#snapshotter.advance(
      this.#tableSpecs,
      this.#allTableNames,
    );
    const {prev, curr, changes: numChanges} = diff;
    this.#lc.debug?.(`advance ${prev.version} => ${curr.version}: ${numChanges} changes`);

    return {
      version: curr.version,
      numChanges,
      changes: this.#advance(diff, timer, numChanges),
    };
  }

  async *#advance(
    diff: SnapshotDiff,
    _timer: Timer,
    _numChanges: number,
  ): AsyncIterable<RowChange | 'yield'> {
    // Push changes per table to the worker thread (push-based for advance).
    // Worker pushes results to a queue; we pull via iterator.next() (pull-based boundary).
    for (const {table, prevValues, nextValue} of diff) {
      const primaryKey = this.#primaryKeys.get(table);
      if (!primaryKey) continue;

      const sourceChanges: NapiSourceChange[] = [];
      let editOldRow: Row | undefined = undefined;
      for (const prevValue of prevValues) {
        if (
          nextValue &&
          JSON.stringify(getRowKey(primaryKey, prevValue)) ===
            JSON.stringify(getRowKey(primaryKey, nextValue))
        ) {
          editOldRow = prevValue;
        } else {
          sourceChanges.push({
            table,
            changeType: 'remove',
            row: rowToNapi(prevValue),
          });
        }
      }
      if (nextValue) {
        if (editOldRow) {
          sourceChanges.push({
            table,
            changeType: 'edit',
            row: rowToNapi(nextValue),
            oldRow: rowToNapi(editOldRow),
          });
        } else {
          sourceChanges.push({
            table,
            changeType: 'add',
            row: rowToNapi(nextValue),
          });
        }
      }

      if (sourceChanges.length > 0) {
        // PULL-BASED: get iterator, pull rows one at a time.
        const iterator = await this.#engine.advanceWithDiffStreaming(sourceChanges);
        while (true) {
          const row = await iterator.next();
          if (row === null || row === undefined) break;
          const change = napiToRowChange(row);
          // Track rowSetSignature locally (XOR for ADD/REMOVE, skip EDIT).
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
        }
        yield 'yield';
      }
    }
  }
}
