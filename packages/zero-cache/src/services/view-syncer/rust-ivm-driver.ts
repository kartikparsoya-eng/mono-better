import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
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

    // ASYNC: hydration runs on this engine's actor thread (off the JS event
    // loop), so concurrent client groups hydrate in parallel. Resolves to the
    // full row array (the addon already materialised it).
    const out = await this.#engine.addQueriesStreaming([
      {queryId: queryID, astJson: JSON.stringify(query)},
    ]);

    let count = 0;
    for (const row of out) {
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

  async advance(_timer: Timer): Promise<
    | {version: string; numChanges: number; changes: Iterable<RowChange | 'yield'>}
    | ResetPipelinesSignal
  > {
    assert(this.initialized(), 'Pipeline driver must be initialized before advancing');

    // Rust derives its own diff from the snapshotter. ASYNC: the advance runs
    // on this engine's actor thread (off the JS event loop), so advances for
    // concurrent client groups run in parallel. Resolves to [header(-1), ...rows]
    // (with a trailing -2 row iff the engine reported a legit reset_reason).
    //
    // ERROR CONTRACT (matches TS): an engine PANIC (e.g. a source-drift assert
    // "Add duplicate row") is NOT turned into a reset — the addon rejects this
    // promise, so `await` throws a raw Error that propagates out of advance() →
    // #advancePipelines re-throws → the view-syncer tears down and the client
    // reconnects, exactly as the TS PipelineDriver does when its source asserts
    // throw. Only advance_result.reset_reason maps to a ResetPipelinesSignal.
    const rows = await this.#engine.advanceToHeadStreaming();

    const headerRow = rows[0];
    if (headerRow === null || headerRow === undefined) {
      throw new Error('advanceToHeadStreaming returned no rows');
    }
    // A legit reset that emitted no header (reset_reason set before the version
    // was known) arrives as a lone -2 row → ResetPipelinesSignal (in-place
    // reset). Panics do NOT reach here — they reject the promise (above).
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

    // Update replicaVersion immediately — the version is now known.
    if (version) {
      this.#replicaVersion = version;
    }

    return {
      version,
      numChanges,
      changes: this.#advanceToHeadRows(rows, aborted),
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

      // Reset signal row (changeType=-2): throw ResetPipelinesSignal.
      if (row.changeType === -2) {
        const reason = row.rowKey['reason']?.strVal ?? 'schema-change';
        const msg = row.rowKey['msg']?.strVal ?? 'advance reset';
        throw new ResetPipelinesSignal(msg, reason as ResetPipelinesSignal['reason']);
      }

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

    if (aborted) {
      this.#lc.warn?.(`advanceToHead: aborted after ${count} changes (budget exceeded)`);
    }
  }
}
