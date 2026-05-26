import type {LogContext} from '@rocicorp/logger';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import {deepEqual, type JSONValue} from '../../../../shared/src/json.ts';
import {must} from '../../../../shared/src/must.ts';
import type {AST, LiteralValue} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import {buildPipeline} from '../../../../zql/src/builder/builder.ts';
import {
  Debug,
  runtimeDebugFlags,
} from '../../../../zql/src/builder/debug-delegate.ts';
import {ChangeIndex} from '../../../../zql/src/ivm/change-index.ts';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import type {Change} from '../../../../zql/src/ivm/change.ts';
import type {Node} from '../../../../zql/src/ivm/data.ts';
import {
  skipYields,
  type Input,
  type Storage,
} from '../../../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../../../zql/src/ivm/schema.ts';
import {
  type Source,
  type SourceChange,
  type SourceInput,
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from '../../../../zql/src/ivm/source.ts';
import type {ConnectionCostModel} from '../../../../zql/src/planner/planner-connection.ts';
import {MeasurePushOperator} from '../../../../zql/src/query/measure-push-operator.ts';
import type {ClientGroupStorage} from '../../../../zqlite/src/database-storage.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {
  resolveSimpleScalarSubqueries,
  type CompanionSubquery,
} from '../../../../zqlite/src/resolve-scalar-subqueries.ts';
import {createSQLiteCostModel} from '../../../../zqlite/src/sqlite-cost-model.ts';
import {TableSource} from '../../../../zqlite/src/table-source.ts';
import {
  reloadPermissionsIfChanged,
  type LoadedPermissions,
} from '../../auth/load-permissions.ts';
import type {LogConfig, ZeroConfig} from '../../config/zero-config.ts';
import {computeZqlSpecs, mustGetTableSpec} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../../db/specs.ts';
import {
  getOrCreateCounter,
  getOrCreateLatencyHistogram,
} from '../../observability/metrics.ts';
import type {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {type RowKey} from '../../types/row-key.ts';
import {
  type GoComputeBackend,
  createGoComputeBackend,
  isGoSidecarEnabled,
  isGoShadowMode,
  isGoShadowVerbose,
  goDriftAuditIntervalMs,
} from './go-sidecar/go-compute-backend.ts';
import type {SidecarManager} from './go-sidecar/sidecar-manager.ts';
import type {SnapshotChange, RowChange as GoRowChange} from './go-sidecar/go-ivm-client.ts';
import {type ShardID} from '../../types/shards.ts';
import {
  getSubscriptionState,
  ZERO_VERSION_COLUMN_NAME,
} from '../replicator/schema/replication-state.ts';
import {checkClientSchema} from './client-schema.ts';
import {rowIDSignatureUnit} from './row-set-signature.ts';
import type {Snapshotter} from './snapshotter.ts';
import {ResetPipelinesSignal, type SnapshotDiff} from './snapshotter.ts';

type RowOp<Op extends Omit<ChangeType, ChangeType.CHILD>> = {
  readonly type: Op;
  readonly queryID: string;
  readonly table: string;
  readonly rowKey: Row;
  readonly row: Row;
};

export type RowAdd = RowOp<ChangeType.ADD>;

export type RowRemove = RowOp<ChangeType.REMOVE>;

export type RowEdit = RowOp<ChangeType.EDIT>;

export type RowChange = RowAdd | RowRemove | RowEdit;

export type AdvanceResult = {
  version: string;
  numChanges: number;
  changes: Iterable<RowChange | 'yield'>;
};

type CompanionPipeline = {
  readonly input: Input;
  readonly childField: string;
  readonly resolvedValue: LiteralValue | null | undefined;
};

type Pipeline = {
  readonly input: Input;
  readonly hydrationTimeMs: number;
  readonly transformedAst: AST;
  readonly transformationHash: string;
  readonly companions: readonly CompanionPipeline[];
};

type QueryInfo = {
  readonly transformedAst: AST;
  readonly transformationHash: string;
};

type AdvanceContext = {
  readonly timer: Timer;
  readonly totalHydrationTimeMs: number;
  readonly numChanges: number;
  pos: number;
  // When true, #shouldAdvanceYieldMaybeAbortAdvance still yields cooperatively
  // but does NOT throw ResetPipelinesSignal — used by shadow mode so a slow
  // advance doesn't tear down state mid-comparison (REVIEW-shadow-mode MEDIUM-1).
  readonly suppressAbort: boolean;
};

type HydrateContext = {
  readonly timer: Timer;
};

export type Timer = {
  elapsedLap: () => number;
  totalElapsed: () => number;
};

/**
 * No matter how fast hydration is, advancement is given at least this long to
 * complete before doing a pipeline reset.
 */
const MIN_ADVANCEMENT_TIME_LIMIT_MS = 50;

/**
 * Manages the state of IVM pipelines for a given ViewSyncer (i.e. client group).
 */
export class PipelineDriver {
  readonly #tables = new Map<string, TableSource>();
  // Query id to pipeline
  readonly #pipelines = new Map<string, Pipeline>();
  /**
   * XOR signature of the set of rows currently attached to each active
   * query, maintained as RowChanges are yielded from {@link addQuery} and
   * {@link advance}. ADDs / REMOVEs XOR the row's unit in (XOR is
   * self-inverse, so one op serves both directions); EDITs are no-ops.
   * Hydration implicitly reseeds from `0n` because {@link addQuery} calls
   * {@link removeQuery} first, which deletes the entry.
   */
  readonly #rowSetSignatures = new Map<string, bigint>();

  readonly #lc: LogContext;
  readonly #snapshotter: Snapshotter;
  readonly #storage: ClientGroupStorage;
  readonly #shardID: ShardID;
  readonly #logConfig: LogConfig;
  readonly #config: ZeroConfig | undefined;
  readonly #tableSpecs = new Map<string, LiteAndZqlSpec>();
  readonly #allTableNames = new Set<string>();
  readonly #costModels: WeakMap<Database, ConnectionCostModel> | undefined;
  readonly #yieldThresholdMs: () => number;
  #streamer: Streamer | null = null;
  #hydrateContext: HydrateContext | null = null;
  #advanceContext: AdvanceContext | null = null;
  #replicaVersion: string | null = null;
  #primaryKeys: Map<string, PrimaryKey> | null = null;
  #permissions: LoadedPermissions | null = null;

  readonly #advanceTime = getOrCreateLatencyHistogram(
    'sync',
    'ivm.advance-time',
    'Time to advance all queries for a given client group in response to a single change.',
  );

  /**
   * Wall-time spent on the Go RPC (encode + socket + decode), measured end-
   * to-end from the TS side. Lets operators distinguish Go's internal compute
   * (recorded into `ivm.advance-time`) from the per-call RPC overhead — the
   * latter is invisible in the per-table timings the sidecar returns.
   * REVIEW-final MED-CROSS-3.
   */
  readonly #advanceGoRpcTime = getOrCreateLatencyHistogram(
    'sync',
    'ivm.advance-go-rpc-time',
    'Wall-time of the Go advance RPC including encode/socket/decode overhead.',
  );

  readonly #conflictRowsDeleted = getOrCreateCounter(
    'sync',
    'ivm.conflict-rows-deleted',
    'Number of rows deleted because they conflicted with added row',
  );

  // Drift-audit counters (REVIEW-final HIGH-CROSS-1). Alert on mismatches > 0.
  // Mismatches/runs is the drift rate; skips/runs flags an over-aggressive
  // audit interval relative to load.
  readonly #driftAuditMismatches = getOrCreateCounter(
    'sync',
    'ivm.drift-audit-mismatches',
    'TS/Go divergences detected by the Go-primary drift audit',
  );
  readonly #driftAuditRuns = getOrCreateCounter(
    'sync',
    'ivm.drift-audit-runs',
    'Drift audits that completed comparison',
  );
  readonly #driftAuditSkips = getOrCreateCounter(
    'sync',
    'ivm.drift-audit-skips',
    'Drift audits skipped (driver busy or snapshot-skew)',
  );

  readonly #inspectorDelegate: InspectorDelegate;
  readonly #goBackend: GoComputeBackend | null = null;
  readonly #shadowMode: boolean;
  #goInitPromise: Promise<void> | null = null;
  /** Set while #scheduleGoReset is running; collapses concurrent reset requests. */
  #goResetInFlight = false;
  /**
   * Set whenever a reset is requested *during* an in-flight reset, so we
   * reschedule once the current one completes. Plain boolean dropped the
   * second request entirely — REVIEW-final MED-SHADOW-2.
   */
  #goResetDirty = false;
  /** Retry attempts of the current reset cycle; resets to 0 on success. */
  #goResetRetries = 0;

  #driftAuditTimer: ReturnType<typeof setInterval> | null = null;
  // Collapses overlapping audit ticks when one runs longer than the interval.
  #driftAuditInFlight = false;
  // Snapshotter version that the TableSources are currently bound to. The
  // Snapshotter bumps its version BEFORE Go's advance RPC completes — during
  // that window `#snapshotter.current().version` is V_new but the TableSources
  // still query V_old's SQLite. The drift audit uses this field to detect
  // and skip that window (otherwise it false-positives on stable snapshots).
  #tableSourcesVersion: string | null = null;

  constructor(
    lc: LogContext,
    logConfig: LogConfig,
    snapshotter: Snapshotter,
    shardID: ShardID,
    storage: ClientGroupStorage,
    clientGroupID: string,
    inspectorDelegate: InspectorDelegate,
    yieldThresholdMs: () => number,
    enablePlanner?: boolean,
    config?: ZeroConfig,
    sidecarManager?: SidecarManager,
  ) {
    this.#lc = lc.withContext('clientGroupID', clientGroupID);
    this.#snapshotter = snapshotter;
    this.#storage = storage;
    this.#shardID = shardID;
    this.#logConfig = logConfig;
    this.#config = config;
    this.#inspectorDelegate = inspectorDelegate;
    this.#costModels = enablePlanner ? new WeakMap() : undefined;
    this.#yieldThresholdMs = yieldThresholdMs;
    this.#shadowMode = isGoShadowMode(config) && isGoSidecarEnabled(config);
    // shadowMode already implies isGoSidecarEnabled, so checking the flag
    // alone is sufficient (REVIEW-ts-integration LOW-1).
    this.#goBackend =
      isGoSidecarEnabled(config) && sidecarManager
        ? createGoComputeBackend(
            sidecarManager,
            clientGroupID,
            // Re-read tables from the current snapshot on every (re-)init,
            // so post-restart re-init picks up fresh data instead of a
            // stale snapshot captured at construction.
            () => this.#currentTablesForGo(),
            // Re-register the active queries after a restart-driven reinit,
            // otherwise Go would have empty pipelines while TS thinks they
            // exist (REVIEW-final restart-correctness gap).
            () =>
              Array.from(this.#pipelines.entries(), ([queryID, p]) => ({
                queryID,
                ast: p.transformedAst,
              })),
          )
        : null;

    const driftIntervalMs = goDriftAuditIntervalMs(config);
    if (driftIntervalMs > 0 && this.#goBackend) {
      this.#driftAuditTimer = setInterval(() => {
        void this.#runDriftAudit();
      }, driftIntervalMs);
      // Don't hold the event loop open just to run the audit on shutdown.
      this.#driftAuditTimer.unref?.();
      this.#lc.info?.(
        `[drift-audit] enabled, interval=${driftIntervalMs}ms`,
      );
    }
  }

  // Internal-plumbing predicates (see #currentTablesForGo for context).
  // <appID>.permissions and <appID>_<shard>.clients are Zero's control
  // plane; user tables live in a different schema (no app-prefix).
  #isInternalTable(name: string): boolean {
    const {appID, shardNum} = this.#shardID;
    return (
      name.startsWith(`${appID}.`) ||
      name.startsWith(`${appID}_${shardNum}.`)
    );
  }

  #isInternalQueryID(queryID: string): boolean {
    return queryID === 'lmids' || queryID === 'mutationResults';
  }

  /**
   * Materialize the current snapshot's tables in the shape the Go sidecar
   * wants (columns + primaryKey + rows). Used both for the initial init
   * and for re-init after a sidecar restart.
   */
  #currentTablesForGo(): Record<
    string,
    {
      columns: Record<string, {type: 'boolean' | 'number' | 'string' | 'null' | 'json'}>;
      primaryKey: string[];
      rows: Record<string, unknown>[];
    }
  > {
    const {db} = this.#snapshotter.current();
    const tables: Record<
      string,
      {
        columns: Record<string, {type: 'boolean' | 'number' | 'string' | 'null' | 'json'}>;
        primaryKey: string[];
        rows: Record<string, unknown>[];
      }
    > = {};
    const warn = (msg: string) =>
      this.#lc.warn?.(`[go-ivm pgType] ${msg}`);
    for (const [name, spec] of this.#tableSpecs.entries()) {
      // Skip Zero-internal plumbing tables (<appID>.permissions,
      // <appID>_<shard>.clients, etc). These are written by zero-cache
      // itself, only feed the `lmids`/`mutationResults` internal queries
      // that TS handles natively, and have caused Go sidecar panics when
      // the in-memory snapshot diverges from SQLite across sidecar
      // restarts (Pattern Z root cause, 2026-05-26). Go-primary mode is
      // safe to skip these: internal queries always route through TS,
      // since TS's TableSource reads live from SQLite and self-heals.
      if (this.#isInternalTable(name)) {
        this.#lc.debug?.(`[go-ivm] skipping internal table ${name}`);
        continue;
      }
      const columns: Record<string, {type: 'boolean' | 'number' | 'string' | 'null' | 'json'}> = {};
      for (const [col, colSpec] of Object.entries(spec.tableSpec.columns)) {
        columns[col] = {type: pgTypeToGoType(colSpec.dataType, warn)};
      }
      let rows: Record<string, unknown>[] = [];
      try {
        rows = db.all(`SELECT * FROM "${name}"`) as Record<string, unknown>[];
      } catch (e) {
        this.#lc.warn?.(`Failed to read table ${name} for Go init:`, e);
      }
      tables[name] = {
        columns,
        primaryKey: [...(this.#primaryKeys?.get(name) ?? spec.tableSpec.primaryKey)],
        rows,
      };
    }
    return tables;
  }

  /**
   * Initializes the PipelineDriver to the current head of the database.
   * Queries can then be added (i.e. hydrated) with {@link addQuery()}.
   *
   * Must only be called once.
   */
  init(clientSchema: ClientSchema) {
    assert(!this.#snapshotter.initialized(), 'Already initialized');
    this.#snapshotter.init();
    this.#initAndResetCommon(clientSchema);
    this.#maybeInitGoBackend(clientSchema);
  }

  #maybeInitGoBackend(_clientSchema: ClientSchema) {
    if (!this.#goBackend) return;
    const tables = this.#currentTablesForGo();
    for (const [name, t] of Object.entries(tables)) {
      this.#lc.info?.(`init table ${name}: ${t.rows.length} rows loaded from SQLite`);
    }
    const promise = this.#goBackend.initEngine(tables);
    this.#goInitPromise = promise;
    promise
      .then(() => this.#lc.info?.('Go backend initialized'))
      .catch(err => {
        this.#lc.error?.('Go backend init failed:', err);
        // Don't leave a rejected promise sitting on #goInitPromise — the
        // dispatch path would await it and throw, killing the ViewSyncer.
        // Null it so dispatch falls through to the TS path based purely on
        // the initialized flag (REVIEW-final MED-CROSS-2 / MEDIUM-3 dual).
        if (this.#goInitPromise === promise) this.#goInitPromise = null;
      });
  }

  /**
   * Re-initialize the Go sidecar after a snapshot leapfrog (reset or
   * advanceWithoutDiff). Destroys the old engine and re-sends all rows
   * from the current SQLite snapshot so Go stays in sync.
   */
  #maybeResetGoBackend() {
    if (!this.#goBackend || !this.#goBackend.initialized) return;
    const tables = this.#currentTablesForGo();
    this.#lc.info?.('Resetting Go backend (snapshot leapfrog)');
    const promise = this.#goBackend.resetEngine(tables);
    this.#goInitPromise = promise;
    promise
      .then(() => this.#lc.info?.('Go backend reset complete'))
      .catch(err => {
        this.#lc.error?.('Go backend reset failed:', err);
        if (this.#goInitPromise === promise) this.#goInitPromise = null;
      });
  }

  /**
   * @returns Whether the PipelineDriver has been initialized.
   */
  initialized(): boolean {
    return this.#snapshotter.initialized();
  }

  /**
   * Clears the current pipelines and TableSources, returning the PipelineDriver
   * to its initial state. This should be called in response to a schema change,
   * as TableSources need to be recomputed.
   */
  reset(clientSchema: ClientSchema) {
    for (const pipeline of this.#pipelines.values()) {
      pipeline.input.destroy();
      for (const companion of pipeline.companions) {
        companion.input.destroy();
      }
    }
    this.#pipelines.clear();
    this.#tables.clear();
    this.#allTableNames.clear();
    this.#rowSetSignatures.clear();
    this.#initAndResetCommon(clientSchema);
    // Re-initialize Go sidecar with fresh snapshot (leapfrog)
    this.#maybeResetGoBackend();
  }

  #initAndResetCommon(clientSchema: ClientSchema) {
    const {db, version} = this.#snapshotter.current();
    this.#tableSourcesVersion = version;
    const fullTables = new Map<string, LiteTableSpec>();
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
    const primaryKeys = this.#primaryKeys ?? new Map<string, PrimaryKey>();
    this.#primaryKeys = primaryKeys;
    primaryKeys.clear();
    for (const [table, spec] of this.#tableSpecs.entries()) {
      primaryKeys.set(table, spec.tableSpec.primaryKey);
    }
    buildPrimaryKeys(clientSchema, primaryKeys);
    const {replicaVersion} = getSubscriptionState(db);
    this.#replicaVersion = replicaVersion;
  }

  /** @returns The replica version. The PipelineDriver must have been initialized. */
  get replicaVersion(): string {
    return must(this.#replicaVersion, 'Not yet initialized');
  }

  /**
   * Returns the current version of the database. This will reflect the
   * latest version change when calling {@link advance()} once the
   * iteration has begun.
   */
  currentVersion(): string {
    assert(this.initialized(), 'Not yet initialized');
    return this.#snapshotter.current().version;
  }

  /**
   * Returns the current upstream {app}.permissions, or `null` if none are defined.
   */
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
      this.#lc.debug?.(
        'Reloaded permissions',
        JSON.stringify(this.#permissions),
      );
    }
    return this.#permissions;
  }

  advanceWithoutDiff(): string {
    const {db, version} = this.#snapshotter.advanceWithoutDiff().curr;
    for (const table of this.#tables.values()) {
      table.setDB(db.db);
    }
    this.#tableSourcesVersion = version;
    // Re-initialize Go sidecar with fresh snapshot (leapfrog)
    this.#maybeResetGoBackend();
    return version;
  }

  #ensureCostModelExistsIfEnabled(db: Database) {
    let existing = this.#costModels?.get(db);
    if (existing) {
      return existing;
    }
    if (this.#costModels) {
      const costModel = createSQLiteCostModel(db, this.#tableSpecs);
      this.#costModels.set(db, costModel);
      return costModel;
    }
    return undefined;
  }

  /**
   * Clears storage used for the pipelines. Call this when the
   * PipelineDriver will no longer be used.
   */
  destroy() {
    if (this.#driftAuditTimer) {
      clearInterval(this.#driftAuditTimer);
      this.#driftAuditTimer = null;
    }
    this.#storage.destroy();
    this.#snapshotter.destroy();
    // Fire-and-forget: tear down Go engine for this group
    this.#goBackend?.destroy().catch(() => {});
  }

  /** @return Map from query ID to PipelineInfo for all added queries. */
  queries(): ReadonlyMap<string, QueryInfo> {
    return this.#pipelines;
  }

  totalHydrationTimeMs(): number {
    let total = 0;
    for (const pipeline of this.#pipelines.values()) {
      total += pipeline.hydrationTimeMs;
    }
    return total;
  }

  #resolveScalarSubqueries(ast: AST): {
    ast: AST;
    companionRows: {table: string; row: Row}[];
    companions: CompanionSubquery[];
    companionInputs: Input[];
  } {
    const companionRows: {table: string; row: Row}[] = [];
    const companionInputs: Input[] = [];

    const executor = (
      subqueryAST: AST,
      childField: string,
    ): LiteralValue | null | undefined => {
      const input = buildPipeline(
        subqueryAST,
        {
          getSource: name => this.#getSource(name),
          createStorage: () => this.#createStorage(),
          decorateSourceInput: (input: SourceInput): Input => input,
          decorateInput: input => input,
          addEdge() {},
          decorateFilterInput: input => input,
        },
        'scalar-subquery',
      );
      // Consume the full stream rather than using first() to avoid
      // triggering early return on Take's #initialFetch assertion.
      // The subquery AST already has limit: 1, so at most one row is produced.
      let node: Node | undefined;
      for (const n of skipYields(input.fetch({}))) {
        node ??= n;
      }
      if (!node) {
        // Keep the companion alive even with no results — it will
        // detect a future insert that creates the row.
        companionInputs.push(input);
        return undefined;
      }
      companionRows.push({table: subqueryAST.table, row: node.row as Row});
      companionInputs.push(input);
      return (node.row[childField] as LiteralValue) ?? null;
    };

    const {ast: resolved, companions} = resolveSimpleScalarSubqueries(
      ast,
      this.#tableSpecs,
      executor,
    );
    return {ast: resolved, companionRows, companions, companionInputs};
  }

  /**
   * Adds a pipeline for the query. The method will hydrate the query using the
   * driver's current snapshot of the database and return a stream of results.
   * Henceforth, updates to the query will be returned when the driver is
   * {@link advance}d. The query and its pipeline can be removed with
   * {@link removeQuery()}.
   *
   * If a query with the same queryID is already added, the existing pipeline
   * will be removed and destroyed before adding the new pipeline.
   *
   * @param timer The caller-controlled {@link Timer} used to determine the
   *        final hydration time. (The caller may pause and resume the timer
   *        when yielding the thread for time-slicing).
   * @return The rows from the initial hydration of the query.
   */
  addQuery(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
  ): Iterable<RowChange | 'yield'> | Promise<Iterable<RowChange | 'yield'>> {
    // If Go backend init is pending, await it first
    if (this.#goInitPromise && this.#goBackend && !this.#goBackend.initialized) {
      return this.#goInitPromise.then(() =>
        this.#addQueryDispatch(transformationHash, queryID, query, timer),
      );
    }
    return this.#addQueryDispatch(transformationHash, queryID, query, timer);
  }

  #addQueryDispatch(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
  ): Iterable<RowChange | 'yield'> | Promise<Iterable<RowChange | 'yield'>> {
    // Shadow mode: run BOTH paths, compare, return TS results
    if (this.#shadowMode && this.#goBackend?.initialized) {
      return this.#shadowAddQuery(transformationHash, queryID, query, timer);
    }
    // When Go backend is active (non-shadow), hydrate via sidecar
    if (this.#goBackend?.initialized) {
      return this.#goHydrate(transformationHash, queryID, query);
    }
    return this.#trackRowSetSignatures(
      this.#addQueryImpl(transformationHash, queryID, query, timer),
    );
  }

  async #goHydrate(
    transformationHash: string,
    queryID: string,
    query: AST,
  ): Promise<Iterable<RowChange | 'yield'>> {
    this.removeQuery(queryID);
    const goResult = await this.#goBackend!.hydrate(queryID, query);

    // Store a minimal pipeline entry for queries() map and hydration time tracking
    // (no TS pipeline needed — Go handles push processing). The real
    // hydrationTimeMs from Go restores the adaptive circuit breaker math
    // in #shouldAdvanceYieldMaybeAbortAdvance.
    this.#pipelines.set(queryID, {
      input: {
        destroy() {},
        fetch: () => ({} as never),
        cleanup: () => ({} as never),
        getSchema: () => ({} as never),
        setOutput: () => {},
      } as unknown as Input,
      hydrationTimeMs: goResult.timingMs ?? 0,
      transformedAst: query,
      transformationHash,
      companions: [],
    });

    // Convert Go RowChanges and track signatures
    const self = this;
    function* yieldGoHydration(): Iterable<RowChange | 'yield'> {
      let i = 0;
      for (const rc of goResult.changes) {
        if (i > 0 && i % 100 === 0) {
          yield 'yield';
        }
        yield self.#goRowChangeToRowChange(rc);
        i++;
      }
    }

    return this.#trackRowSetSignatures(yieldGoHydration());
  }

  /**
   * Batch hydrate multiple queries via Go sidecar in a single RPC call.
   * Go builds pipelines serially, then fetches all in parallel (goroutines).
   * Returns results in the same order as input.
   * Only used in Go-primary mode (non-shadow).
   */
  async goHydrateBatch(
    queries: {transformationHash: string; queryID: string; ast: AST}[],
  ): Promise<{queryID: string; changes: Iterable<RowChange | 'yield'>}[]> {
    // Remove old pipelines
    for (const q of queries) {
      this.removeQuery(q.queryID);
    }

    // Single RPC for all queries
    const batchResults = await this.#goBackend!.hydrateMany(
      queries.map(q => ({queryID: q.queryID, ast: q.ast})),
    );

    // Register pipelines and convert results
    const results: {queryID: string; changes: Iterable<RowChange | 'yield'>}[] = [];
    for (let i = 0; i < queries.length; i++) {
      const q = queries[i];
      const goResult = batchResults[i] ?? {changes: [], timingMs: undefined};

      this.#pipelines.set(q.queryID, {
        input: {
          destroy() {},
          fetch: () => ({} as never),
          cleanup: () => ({} as never),
          getSchema: () => ({} as never),
          setOutput: () => {},
        } as unknown as Input,
        hydrationTimeMs: goResult.timingMs ?? 0,
        transformedAst: q.ast,
        transformationHash: q.transformationHash,
        companions: [],
      });

      const self = this;
      const qID = q.queryID;
      const changes = goResult.changes;
      function* yieldGoHydration(): Iterable<RowChange | 'yield'> {
        let j = 0;
        for (const rc of changes) {
          if (j > 0 && j % 100 === 0) {
            yield 'yield';
          }
          yield self.#goRowChangeToRowChange(rc);
          j++;
        }
      }

      results.push({
        queryID: qID,
        changes: this.#trackRowSetSignatures(yieldGoHydration()),
      });
    }

    return results;
  }

  /**
   * Streaming variant of {@link goHydrateBatch}. Yields per-query results
   * AS SOON as Go finishes that query. Tail-latency optimisation: fast
   * queries reach the WebSocket client before slow queries in the same
   * batch complete (REVIEW-final perf-opt streaming).
   *
   * The returned iterable yields entries in COMPLETION order, not input
   * order — callers must not rely on positional correspondence with
   * `queries`.
   */
  async *goHydrateBatchStream(
    queries: {transformationHash: string; queryID: string; ast: AST}[],
  ): AsyncIterable<{queryID: string; changes: Iterable<RowChange | 'yield'>}> {
    for (const q of queries) {
      this.removeQuery(q.queryID);
    }

    // Buffer arrived-but-not-yet-yielded results from the streaming RPC.
    // The producer side runs in goroutines on Go; we get one onResult call
    // per query via the client's onPartial. We park each into a queue and
    // wake the async iterator's resolver.
    type Entry = {queryID: string; changes: RowChange[]; timingMs: number | undefined};
    const buffered: Entry[] = [];
    let wake: (() => void) | null = null;
    let done = false;
    let error: Error | null = null;

    const byQueryID = new Map<string, (typeof queries)[number]>();
    for (const q of queries) byQueryID.set(q.queryID, q);

    const rpcPromise = this.#goBackend!.hydrateManyStream(
      queries.map(q => ({queryID: q.queryID, ast: q.ast})),
      (r: {queryID: string; changes: unknown[]; timingMs: number | undefined}) => {
        buffered.push({
          queryID: r.queryID,
          changes: (r.changes ?? []) as RowChange[],
          timingMs: r.timingMs,
        });
        wake?.();
        wake = null;
      },
    )
      .catch((e: unknown) => {
        error = e instanceof Error ? e : new Error(String(e));
        wake?.();
        wake = null;
      })
      .finally(() => {
        done = true;
        wake?.();
        wake = null;
      });

    while (true) {
      if (buffered.length === 0 && !done && !error) {
        await new Promise<void>(resolve => {
          wake = resolve;
        });
      }
      if (error) throw error;
      while (buffered.length > 0) {
        const r = buffered.shift()!;
        const q = byQueryID.get(r.queryID);
        if (!q) continue;
        this.#pipelines.set(q.queryID, {
          input: {
            destroy() {},
            fetch: () => ({} as never),
            cleanup: () => ({} as never),
            getSchema: () => ({} as never),
            setOutput: () => {},
          } as unknown as Input,
          hydrationTimeMs: r.timingMs ?? 0,
          transformedAst: q.ast,
          transformationHash: q.transformationHash,
          companions: [],
        });
        const self = this;
        const changesArr = r.changes;
        function* yieldGoHydration(): Iterable<RowChange | 'yield'> {
          let j = 0;
          for (const rc of changesArr) {
            if (j > 0 && j % 100 === 0) yield 'yield';
            yield self.#goRowChangeToRowChange(rc);
            j++;
          }
        }
        yield {
          queryID: q.queryID,
          changes: this.#trackRowSetSignatures(yieldGoHydration()),
        };
      }
      if (done) break;
    }
    await rpcPromise;
  }

  /** Whether batch hydration is available (Go-primary, non-shadow). */
  get canBatchHydrate(): boolean {
    return !!(this.#goBackend?.initialized && !this.#shadowMode);
  }

  /**
   * Await Go backend initialization if pending. Call before checking
   * canBatchHydrate to ensure the initial batch of queries uses the
   * Go path instead of falling through to per-query TS hydration.
   */
  async awaitGoInit(): Promise<void> {
    if (!this.#goBackend) return;
    // Use the backend's whenInitialized() which is restart-aware. The plain
    // #goInitPromise can resolve from a prior epoch's init while a restart's
    // re-init is mid-flight — that path silently fell through to TS
    // (REVIEW-final HIGH-TS-1).
    await this.#goBackend.whenInitialized();
    // Also drain the explicit init promise (covers the very-first init
    // before any whenInitialized state exists).
    if (this.#goInitPromise) {
      try {
        await this.#goInitPromise;
      } catch {
        // Swallow — caller's dispatch path will fall back to TS based on
        // the initialized flag.
      }
    }
  }

  /**
   * Shadow batch comparison: send all queries as one batch to Go,
   * compare each result against the TS-hydrated results.
   * Validates that parallel Go hydration matches sequential TS hydration.
   * Fire-and-forget — results are logged, not returned.
   */
  async shadowBatchCompare(
    queries: {queryID: string; ast: AST}[],
    tsResultsPerQuery: Map<string, RowChange[]>,
  ): Promise<void> {
    if (!this.#goBackend?.initialized) return;
    // Internal queries (lmids, mutationResults) target Zero's control-plane
    // tables which Go doesn't track. They always run via TS's TableSource.
    // Drop them from the batch before dispatching to Go.
    queries = queries.filter(q => !this.#isInternalQueryID(q.queryID));
    if (queries.length === 0) return;
    try {
      const batchStart = performance.now();
      // Pre-resolve scalar subqueries per query so Go gets the same
      // resolved AST that TS's own pipeline uses (#addQueryImpl resolves
      // them at line 1114). Without this, Go builds a regular EXISTS join
      // for `{scalar: true}` conditions and propagates the child rows on
      // every parent push — drift seen as Go over-producing
      // channel_participants ADD rows for the conversations ACL EXISTS.
      // Destroy the freshly-built companion inputs immediately: TS's main
      // path already wired its own live companions in #addQueryImpl, and
      // these shadow-side ones would race with those if kept alive.
      //
      // The resolver calls source.fetch() which routes through
      // TableSource.shouldYield → #shouldYield (line 1951+), which throws
      // if neither #hydrateContext nor #advanceContext is set. We pin a
      // noop hydrate-context for the duration of resolution and restore
      // it after so the next addQuery's assertion still holds. Same
      // pattern the drift audit uses at line 1007.
      //
      // Phase 1 of the scalar-subquery handling fix (long-term: port
      // resolveSimpleScalarSubqueries to Go).
      const resolvedByID = new Map<
        string,
        {ast: AST; companionRows: {table: string; row: Row}[]}
      >();
      const prevHydrateContext = this.#hydrateContext;
      // Note: Timer's exported type omits `running()` (line 131) but
      // #shouldYield calls it at runtime (line 1976). Provide the full
      // shape — the drift-audit's noopTimer at line 1007 predates the
      // TimeSliceTimer-running() guard and would crash too if exercised.
      const noopTimer = {
        elapsedLap: () => 0,
        totalElapsed: () => 0,
        running: () => true,
      } as unknown as Timer;
      this.#hydrateContext = {timer: noopTimer};
      try {
        for (const q of queries) {
          const r = this.#resolveScalarSubqueries(q.ast);
          resolvedByID.set(q.queryID, {
            ast: r.ast,
            companionRows: r.companionRows,
          });
          for (const input of r.companionInputs) input.destroy();
          // Pattern Z diagnostic (REMOVE after root-cause). Dump original
          // and resolved ASTs for every query that targets the conversations
          // table or has a whereExists chain — the production shapes that
          // showed result-table under-produce. Filtering by table+shape
          // (not queryID) so we catch the queries regardless of their
          // session-scoped hash.
          const astStr = JSON.stringify(q.ast);
          const interesting =
            q.ast.table === 'conversations' ||
            q.ast.table === 'channels' ||
            q.ast.table === 'channel_user_status' ||
            q.ast.table === 'channel_stats' ||
            astStr.includes('"whereExists"') ||
            astStr.includes('correlatedSubquery');
          if (interesting) {
            this.#lc.error?.(
              `[ast-dump] ${q.queryID} (${q.ast.table}) ORIGINAL: ${astStr.slice(0, 2000)}`,
            );
            this.#lc.error?.(
              `[ast-dump] ${q.queryID} (${q.ast.table}) RESOLVED: ${JSON.stringify(r.ast).slice(0, 2000)}`,
            );
            this.#lc.error?.(
              `[ast-dump] ${q.queryID} companionRows.count=${r.companionRows.length}`,
            );
          }
        }
      } finally {
        this.#hydrateContext = prevHydrateContext;
      }

      // Use the streaming variant so shadow mode exercises the same code
      // path Go-primary mode will use in production. Compare per-query as
      // soon as Go emits each result (REVIEW-final perf-opt streaming
      // validation in shadow).
      const goResultsByID = new Map<string, RowChange[]>();
      let mismatches = 0;
      await this.#goBackend.hydrateManyStream(
        queries.map(q => ({
          queryID: q.queryID,
          ast: resolvedByID.get(q.queryID)!.ast,
        })),
        r => {
          const goChanges = (r.changes ?? []).map(rc =>
            this.#goRowChangeToRowChange(rc as GoRowChange),
          );
          // Append the one-time companion ADDs that TS emits at hydrate
          // time (pipeline-driver.ts:1154-1163). With scalars resolved
          // out of the AST sent to Go, Go's pipeline no longer emits the
          // child rows — the companions fill that gap so the diff lines up.
          const resolved = resolvedByID.get(r.queryID);
          if (resolved) {
            for (const {table, row} of resolved.companionRows) {
              const primaryKey = mustGetPrimaryKey(this.#primaryKeys, table);
              goChanges.push({
                type: ChangeType.ADD,
                queryID: r.queryID,
                table,
                rowKey: getRowKey(primaryKey, row),
                row,
              } as RowChange);
            }
          }
          goResultsByID.set(r.queryID, goChanges);
          const tsChanges = tsResultsPerQuery.get(r.queryID) ?? [];
          this.#shadowCompare(`batch-hydrate`, r.queryID, tsChanges, goChanges);
          if (tsChanges.length !== goChanges.length) mismatches++;
        },
      );
      const batchMs = performance.now() - batchStart;
      // Account for queries Go never emitted (size mismatch detected).
      for (const q of queries) {
        if (!goResultsByID.has(q.queryID)) {
          const tsChanges = tsResultsPerQuery.get(q.queryID) ?? [];
          this.#shadowCompare('batch-hydrate', q.queryID, tsChanges, []);
          if (tsChanges.length !== 0) mismatches++;
        }
      }
      this.#lc.info?.(
        `[shadow][batch-stream] ${queries.length} queries in ${batchMs.toFixed(2)}ms, ${mismatches} mismatches`,
      );
    } catch (e) {
      this.#lc.error?.(`[shadow][batch] failed: ${e}`);
      this.#scheduleGoReset('shadow-batch-failure');
    }
  }

  // Sampled-shadow drift audit for Go-primary mode (REVIEW-final HIGH-CROSS-1).
  // Picks one random active query and re-hydrates it on TS and Go from the
  // current snapshot, comparing via #shadowCompare. Anything but a length-equal
  // sorted match means Go's incrementally-maintained state has drifted.
  async #runDriftAudit(): Promise<void> {
    if (this.#driftAuditInFlight) {
      this.#driftAuditSkips.add(1);
      return;
    }
    this.#driftAuditInFlight = true;
    try {
      if (!this.initialized()) return;
      if (!this.#goBackend?.initialized) return;
      // #addQueryImpl asserts #advanceContext===null, and reusing #streamer
      // mid-advance would corrupt the in-flight diff. Wait for the next tick.
      if (this.#advanceContext !== null) {
        this.#driftAuditSkips.add(1);
        return;
      }
      if (this.#pipelines.size === 0) return;

      const queryIDs = [...this.#pipelines.keys()];
      const targetID = queryIDs[Math.floor(Math.random() * queryIDs.length)];
      const entry = this.#pipelines.get(targetID);
      if (!entry) return;
      // transformedAst is post-subquery-resolution — what Go was originally
      // given. Reusing it sidesteps re-resolving against a fresher snapshot.
      const ast = entry.transformedAst;
      const transformationHash = entry.transformationHash;

      const auditID = `__drift_audit_${Date.now().toString(36)}_${Math.floor(
        Math.random() * 0xffff_ffff,
      ).toString(36)}`;
      const noopTimer: Timer = {elapsedLap: () => 0, totalElapsed: () => 0};

      // Audit must run on a stable, consistent snapshot. Three windows can
      // invalidate the comparison:
      //   (a) An advance lands between TS hydrate and Go's RPC response —
      //       caught by checking the Snapshotter version before/after.
      //   (b) A Go-primary `#goAdvance` is mid-flight: the Snapshotter has
      //       already bumped its version, but the TableSources still query
      //       the previous SQLite snapshot until after the await completes.
      //       In this window `#tableSourcesVersion !== snapshotter.version`.
      //   (c) The sidecar restarts mid-audit: Go's RPC fails internally,
      //       #withReinitRetry re-inits Go to the CURRENT snapshot, and the
      //       retried RPC succeeds — but against state that may now be
      //       newer than what TS hydrated. Caught by snapshotting the
      //       SidecarManager epoch and re-checking after the audit.
      const versionBefore = this.#snapshotter.current().version;
      if (this.#tableSourcesVersion !== versionBefore) {
        this.#driftAuditSkips.add(1);
        return;
      }
      const epochBefore = this.#goBackend.epoch;

      let tsChanges: RowChange[];
      try {
        tsChanges = [];
        for (const c of this.#addQueryImpl(
          transformationHash,
          auditID,
          ast,
          noopTimer,
        )) {
          if (c !== 'yield') tsChanges.push(c);
        }
      } catch (e) {
        this.#lc.warn?.(
          `[drift-audit] TS hydrate failed for ${targetID}: ${String(e)}`,
        );
        return;
      }

      const goChanges: RowChange[] = [];
      try {
        await this.#goBackend.hydrateManyStream(
          [{queryID: auditID, ast}],
          r => {
            for (const rc of r.changes ?? []) {
              goChanges.push(this.#goRowChangeToRowChange(rc as GoRowChange));
            }
          },
        );
      } catch (e) {
        this.#lc.warn?.(
          `[drift-audit] Go hydrate failed for ${targetID}: ${String(e)}`,
        );
        return;
      } finally {
        try {
          this.removeQuery(auditID);
        } catch {
          // removeQuery is best-effort during audit teardown.
        }
        this.#goBackend?.removeQuery(auditID).catch(() => {});
      }

      // Three guards must still hold for the comparison to be valid:
      //   - Snapshotter hasn't advanced (no fresh mutations applied)
      //   - TableSources still bound to the same version (no #goAdvance
      //     window where TS sees V_new but TableSources query V_old)
      //   - Sidecar hasn't restarted (which would silently re-init Go to
      //     a newer snapshot, leaving TS's captured rows behind)
      const versionAfter = this.#snapshotter.current().version;
      if (
        versionBefore !== versionAfter ||
        this.#tableSourcesVersion !== versionAfter ||
        epochBefore !== this.#goBackend.epoch
      ) {
        this.#driftAuditSkips.add(1);
        return;
      }

      // #shadowCompare sorts by [queryID, table, rowKey, type]; the transient
      // auditID would prevent TS and Go rows from sorting together.
      const remapToTarget = (cs: RowChange[]) =>
        cs.map(c => ({...c, queryID: targetID}));
      const tsRemapped = remapToTarget(tsChanges);
      const goRemapped = remapToTarget(goChanges);

      this.#driftAuditRuns.add(1);

      // Pre-compute set-diff so we can attach AST + version context when the
      // audit fires a mismatch — shadowCompare alone leaves us blind on the
      // query shape (which is the load-bearing signal for repros).
      const keyOf = (c: RowChange) =>
        `${c.type}|${c.table}|${stableStringify(c.rowKey)}`;
      const tsKeys = new Set(tsRemapped.map(keyOf));
      const goKeys = new Set(goRemapped.map(keyOf));
      let setDiffers = tsRemapped.length !== goRemapped.length;
      if (!setDiffers) {
        for (const k of tsKeys) if (!goKeys.has(k)) { setDiffers = true; break; }
      }
      if (setDiffers) {
        this.#lc.error?.(
          `[drift-audit][repro] queryID=${targetID} ` +
            `transformationHash=${transformationHash} ` +
            `version_before=${versionBefore} version_after=${versionAfter} ` +
            `ts_count=${tsRemapped.length} go_count=${goRemapped.length} ` +
            `ast=${JSON.stringify(ast)}`,
        );
      }

      this.#shadowCompare('drift-audit', targetID, tsRemapped, goRemapped);
      if (tsRemapped.length !== goRemapped.length) {
        this.#driftAuditMismatches.add(1);
      }
      this.#lc.debug?.(
        `[drift-audit] ${targetID}: ts=${tsRemapped.length} go=${goRemapped.length} ok`,
      );
    } finally {
      this.#driftAuditInFlight = false;
    }
  }

  *#addQueryImpl(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
  ): Iterable<RowChange | 'yield'> {
    assert(
      this.initialized(),
      'Pipeline driver must be initialized before adding queries',
    );
    this.removeQuery(queryID);
    const debugDelegate = runtimeDebugFlags.trackRowsVended
      ? new Debug()
      : undefined;

    const costModel = this.#ensureCostModelExistsIfEnabled(
      this.#snapshotter.current().db.db,
    );

    assert(
      this.#advanceContext === null,
      'Cannot hydrate while advance is in progress',
    );
    this.#hydrateContext = {
      timer,
    };
    try {
      const {
        ast: resolvedQuery,
        companionRows,
        companions: companionMeta,
        companionInputs,
      } = this.#resolveScalarSubqueries(query);

      const input = buildPipeline(
        resolvedQuery,
        {
          debug: debugDelegate,
          enableNotExists: true, // Server-side can handle NOT EXISTS
          getSource: name => this.#getSource(name),
          createStorage: () => this.#createStorage(),
          decorateSourceInput: (input: SourceInput, _queryID: string): Input =>
            new MeasurePushOperator(
              input,
              queryID,
              this.#inspectorDelegate,
              'query-update-server',
            ),
          decorateInput: input => input,
          addEdge() {},
          decorateFilterInput: input => input,
        },
        queryID,
        costModel,
      );
      const schema = input.getSchema();
      input.setOutput({
        push: change => {
          const streamer = this.#streamer;
          assert(streamer, 'must #startAccumulating() before pushing changes');
          streamer.accumulate(queryID, schema, [change]);
          return [];
        },
      });

      yield* hydrateInternal(
        input,
        queryID,
        must(this.#primaryKeys),
        this.#tableSpecs,
      );

      for (const {table, row} of companionRows) {
        const primaryKey = mustGetPrimaryKey(this.#primaryKeys, table);
        yield {
          type: ChangeType.ADD,
          queryID,
          table,
          rowKey: getRowKey(primaryKey, row),
          row,
        } as RowChange;
      }

      const hydrationTimeMs = timer.totalElapsed();
      if (runtimeDebugFlags.trackRowCountsVended) {
        if (hydrationTimeMs > this.#logConfig.slowHydrateThreshold) {
          let totalRowsConsidered = 0;
          const lc = this.#lc
            .withContext('queryID', queryID)
            .withContext('hydrationTimeMs', hydrationTimeMs);
          for (const tableName of this.#tables.keys()) {
            const entries = Object.entries(
              debugDelegate?.getVendedRowCounts()[tableName] ?? {},
            );
            totalRowsConsidered += entries.reduce(
              (acc, entry) => acc + entry[1],
              0,
            );
            lc.info?.(tableName + ' VENDED: ', entries);
          }
          lc.info?.(`Total rows considered: ${totalRowsConsidered}`);
        }
      }
      debugDelegate?.reset();

      // Set up live companion pipelines for reactive scalar subquery monitoring.
      const liveCompanions: CompanionPipeline[] = [];
      for (let i = 0; i < companionMeta.length; i++) {
        const meta = companionMeta[i];
        const companionInput = companionInputs[i];
        const companionSchema = companionInput.getSchema();
        const {childField, resolvedValue} = meta;
        companionInput.setOutput({
          push: (change: Change) => {
            let newValue: LiteralValue | null | undefined;
            switch (change[ChangeIndex.TYPE]) {
              case ChangeType.ADD:
              case ChangeType.EDIT:
                newValue =
                  (change[ChangeIndex.NODE].row[childField] as LiteralValue) ??
                  null;
                break;
              case ChangeType.REMOVE:
                newValue = undefined;
                break;
              case ChangeType.CHILD:
                return [];
            }
            if (!scalarValuesEqual(newValue, resolvedValue)) {
              throw new ResetPipelinesSignal(
                `Scalar subquery value changed for ${meta.ast.table}: ` +
                  `${String(resolvedValue)} -> ${String(newValue)}`,
                'scalar-subquery',
              );
            }
            const streamer = this.#streamer;
            assert(
              streamer,
              'must #startAccumulating() before pushing changes',
            );
            streamer.accumulate(queryID, companionSchema, [change]);
            return [];
          },
        });
        liveCompanions.push({input: companionInput, childField, resolvedValue});
      }

      // Note: This hydrationTime is a wall-clock overestimate, as it does
      // not take time slicing into account. The view-syncer resets this
      // to a more precise processing-time measurement with setHydrationTime().
      this.#pipelines.set(queryID, {
        input,
        hydrationTimeMs,
        transformedAst: resolvedQuery,
        transformationHash,
        companions: liveCompanions,
      });
    } finally {
      this.#hydrateContext = null;
    }
  }

  /**
   * Removes the pipeline for the query. This is a no-op if the query
   * was not added.
   */
  removeQuery(queryID: string) {
    const pipeline = this.#pipelines.get(queryID);
    if (pipeline) {
      this.#pipelines.delete(queryID);
      pipeline.input.destroy();
      for (const companion of pipeline.companions) {
        companion.input.destroy();
      }
    }
    this.#rowSetSignatures.delete(queryID);
    // Fire-and-forget: notify Go sidecar
    this.#goBackend?.removeQuery(queryID).catch(() => {});
  }

  /**
   * Current XOR signature of the row-set attached to `queryID`, or
   * `undefined` if no pipeline for the query is currently active.
   * Maintained incrementally by {@link addQuery} and {@link advance}.
   */
  rowSetSignature(queryID: string): bigint | undefined {
    return this.#rowSetSignatures.get(queryID);
  }

  /**
   * Wraps an iterable of RowChanges, XORing each row's unit hash into the
   * query's signature (ADDs and REMOVEs share the same op; EDITs are no-ops).
   * Used to intercept the yield streams from {@link addQuery} and
   * {@link advance}.
   */
  *#trackRowSetSignatures(
    changes: Iterable<RowChange | 'yield'>,
  ): Iterable<RowChange | 'yield'> {
    for (const change of changes) {
      if (change !== 'yield' && change.type !== ChangeType.EDIT) {
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
  }

  /**
   * Returns the value of the row with the given primary key `pk`,
   * or `undefined` if there is no such row. The pipeline must have been
   * initialized.
   */
  getRow(table: string, pk: RowKey): Row | undefined {
    assert(this.initialized(), 'Not yet initialized');
    const source = must(this.#tables.get(table));
    return source.getRow(pk as Row);
  }

  /**
   * Advances to the new head of the database.
   *
   * @param timer The caller-controlled {@link Timer} that will be used to
   *        measure the progress of the advancement and abort with a
   *        {@link ResetPipelinesSignal} if it is estimated to take longer
   *        than a hydration.
   * @return The resulting row changes for all added queries. Note that the
   *         `changes` must be iterated over in their entirety in order to
   *         advance the database snapshot.
   */
  advance(timer: Timer): AdvanceResult | Promise<AdvanceResult> {
    assert(
      this.initialized(),
      'Pipeline driver must be initialized before advancing',
    );
    // If Go backend init is pending, await it first
    if (this.#goInitPromise && this.#goBackend && !this.#goBackend.initialized) {
      return this.#goInitPromise.then(() => this.#advanceDispatch(timer));
    }
    return this.#advanceDispatch(timer);
  }

  #advanceDispatch(timer: Timer): AdvanceResult | Promise<AdvanceResult> {
    const diff = this.#snapshotter.advance(
      this.#tableSpecs,
      this.#allTableNames,
    );
    const {prev, curr, changes} = diff;
    this.#lc.debug?.(
      `advance ${prev.version} => ${curr.version}: ${changes} changes`,
    );

    // Shadow mode: run TS path as source of truth, also run Go, compare
    if (this.#shadowMode && this.#goBackend?.initialized) {
      return this.#shadowAdvance(diff, timer, curr.version, changes);
    }

    // When Go backend is active (non-shadow), run advance asynchronously via sidecar
    if (this.#goBackend?.initialized) {
      return this.#goAdvance(diff, curr.version, changes);
    }

    return {
      version: curr.version,
      numChanges: changes,
      changes: this.#trackRowSetSignatures(this.#advance(diff, timer, changes)),
    };
  }

  async #goAdvance(
    diff: SnapshotDiff,
    version: string,
    numChanges: number,
  ): Promise<AdvanceResult> {
    // Convert SnapshotDiff to SnapshotChange[] for Go sidecar.
    // Internal Zero tables (<appID>.permissions, <appID>_<shard>.clients)
    // are excluded — they only feed internal queries (lmids etc.) which
    // always run via TS's TableSource (self-healing over live SQLite).
    // Sending these diffs to Go was the Pattern Z panic root cause.
    const snapshotChanges: SnapshotChange[] = [];
    for (const {table, prevValues, nextValue} of diff) {
      if (this.#isInternalTable(table)) {
        continue;
      }
      snapshotChanges.push({
        table,
        prevValues: prevValues as Record<string, unknown>[],
        nextValue: nextValue as Record<string, unknown> | null,
      });
    }

    const goStart = performance.now();
    // Use the streaming variant: Go ships partial frames during the call,
    // the client reassembles into the same GoAdvanceResult shape so this
    // call site is unchanged behaviorally. Win: large advance diffs no
    // longer buffer as one msgpack frame on the Go side; first bytes flow
    // as soon as advanceChunkSize (10k) rows accumulate.
    const goResult = await this.#goBackend!.advanceStream(snapshotChanges);
    const goRpcMs = performance.now() - goStart;

    // Set the new snapshot on all TableSources (same as TS path)
    const {curr} = diff;
    for (const table of this.#tables.values()) {
      table.setDB(curr.db.db);
    }
    this.#tableSourcesVersion = curr.version;
    this.#ensureCostModelExistsIfEnabled(curr.db.db);

    // Feed Go's per-(table, op) timings into the same `ivm.advance-time`
    // histogram the TS path populates — keeps observability identical
    // regardless of which backend handled the work.
    if (goResult.timings) {
      for (const t of goResult.timings) {
        this.#advanceTime.recordMs(t.ms, {
          table: t.table,
          type: goTypeToLabel(t.type),
        });
      }
    }
    // Separately record the round-trip wall time so operators can attribute
    // any latency gap between TS-mode and Go-mode to RPC overhead
    // (REVIEW-final MED-CROSS-3).
    this.#advanceGoRpcTime.recordMs(goRpcMs);

    // Convert Go RowChanges to local RowChange format with yield markers
    const self = this;
    const changesArr = goResult.changes;
    function* yieldGoChanges(): Iterable<RowChange | 'yield'> {
      let i = 0;
      for (const rc of changesArr) {
        if (i > 0 && i % 100 === 0) {
          yield 'yield';
        }
        yield self.#goRowChangeToRowChange(rc);
        i++;
      }
    }

    return {
      version,
      numChanges,
      changes: this.#trackRowSetSignatures(yieldGoChanges()),
    };
  }

  #goRowChangeToRowChange(rc: GoRowChange): RowChange {
    const type =
      rc.type === 0
        ? ChangeType.ADD
        : rc.type === 1
          ? ChangeType.REMOVE
          : ChangeType.EDIT;
    // Mirror the TS-native Streamer at pipeline-driver.ts:1598 which emits
    // `row: undefined` for REMOVE. The RowChange type declares row as Row
    // but in practice REMOVE rows carry undefined on both paths — this is
    // an intentional shape match, not a bug. Setting `row = rc.rowKey` here
    // diverged from TS and broke shadow-compare on every REMOVE (Bug #23
    // regression caught in soak; my prior MEDIUM-2 "fix" was wrong).
    return {
      type,
      queryID: rc.queryID,
      table: rc.table,
      rowKey: rc.rowKey as Row,
      row: type === ChangeType.REMOVE ? undefined : ((rc.row ?? rc.rowKey) as Row),
    } as RowChange;
  }

  /**
   * Schedule a best-effort reset of the Go engine from the current snapshot.
   * Used after a Go RPC failure in shadow mode to heal state drift — the
   * sidecar missed a diff, so its MemorySource is out of sync; reinitializing
   * from a fresh `SELECT * FROM` resets it (REVIEW-shadow-mode HIGH-1).
   *
   * Idempotent: collapses concurrent reset requests so a burst of failures
   * doesn't spawn N parallel re-inits.
   */
  #scheduleGoReset(reason: string): void {
    if (!this.#goBackend) return;
    if (this.#goResetInFlight) {
      // Don't drop the request — record it so we re-fire after the in-flight
      // reset completes (REVIEW-final MED-SHADOW-2).
      this.#goResetDirty = true;
      return;
    }
    this.#goResetInFlight = true;
    const MAX_RESET_RETRIES = 3;
    const tables = this.#currentTablesForGo();
    this.#lc.warn?.(`[shadow] Scheduling Go reset (${reason})`);
    this.#goInitPromise = this.#goBackend.resetEngine(tables);
    this.#goInitPromise
      .then(() => {
        this.#lc.info?.(`[shadow] Go reset complete (${reason})`);
        this.#goResetRetries = 0;
      })
      .catch(err => {
        this.#lc.error?.(`[shadow] Go reset failed (${reason}):`, err);
        // Reset itself failed — retry with bounded attempts. After cap,
        // give up and let the system stay in TS-only fallback until the
        // next operational signal (sidecar restart, schema change, etc.).
        if (this.#goResetRetries < MAX_RESET_RETRIES) {
          this.#goResetRetries++;
          this.#goResetDirty = true;
        } else {
          this.#lc.error?.(
            `[shadow] Go reset retries exhausted (${this.#goResetRetries}); ` +
              `staying on TS fallback`,
          );
          this.#goResetRetries = 0;
        }
      })
      .finally(() => {
        this.#goResetInFlight = false;
        if (this.#goResetDirty) {
          this.#goResetDirty = false;
          // Fire a follow-up reset to cover failures that arrived during
          // the just-completed cycle.
          this.#scheduleGoReset(`${reason} (follow-up)`);
        }
      });
  }

  // ─── Shadow Mode ───────────────────────────────────────────────────

  /**
   * Shadow addQuery: run TS hydration (source of truth) and return TS
   * results. The Go-side comparison runs ONCE per batch via
   * `shadowBatchCompare` after the ViewSyncer loop — running it per-query
   * here doubled Go-side work without adding signal (REVIEW-shadow-mode
   * HIGH-2). The AST snippet log was moved to debug (LOW-2).
   */
  #shadowAddQuery(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
  ): Iterable<RowChange | 'yield'> {
    const tsHydStart = performance.now();
    const tsResults = [
      ...this.#trackRowSetSignatures(
        this.#addQueryImpl(transformationHash, queryID, query, timer),
      ),
    ];
    const tsHydMs = performance.now() - tsHydStart;
    const numChanges = tsResults.filter(c => c !== 'yield').length;
    this.#lc.debug?.(
      `[shadow] TS addQuery ${queryID}: ${numChanges} changes, table=${query.table}, ast=${JSON.stringify(query).slice(0, 200)}`,
    );
    this.#lc.debug?.(
      `[shadow] TS hydrate ${queryID}: ${tsHydMs.toFixed(2)}ms / ${numChanges} changes`,
    );
    return tsResults;
  }

  /**
   * Shadow advance: buffer the diff, run TS advance (source of truth),
   * also send to Go, compare results, return TS results.
   */
  async #shadowAdvance(
    diff: SnapshotDiff,
    timer: Timer,
    version: string,
    numChanges: number,
  ): Promise<AdvanceResult> {
    // Buffer diff entries so both paths can consume them
    const buffered: Array<{
      table: string;
      prevValues: Readonly<Row>[];
      nextValue: Readonly<Row> | null;
      rowKey: RowKey;
    }> = [];
    const snapshotChanges: SnapshotChange[] = [];
    for (const entry of diff) {
      buffered.push(entry);
      // TS always consumes the full diff (its source-of-truth is SQLite,
      // and internal queries like lmids need these). Go's snapshotChanges
      // omits internal tables — Go never loads them and an Edit on a
      // row Go's MemorySource doesn't have would panic the sidecar
      // (Pattern Z root cause, 2026-05-26).
      if (this.#isInternalTable(entry.table)) {
        continue;
      }
      snapshotChanges.push({
        table: entry.table,
        prevValues: entry.prevValues as Record<string, unknown>[],
        nextValue: entry.nextValue as Record<string, unknown> | null,
      });
    }

    // Create a replay diff for TS path
    const replayDiff: SnapshotDiff = {
      prev: diff.prev,
      curr: diff.curr,
      changes: diff.changes,
      [Symbol.iterator]: () => buffered[Symbol.iterator](),
    };

    // Kick off Go advance BEFORE draining TS so the RPC is in flight while
    // TS does its work. Total shadow latency approaches max(TS, Go) instead
    // of TS + Go (REVIEW-shadow-mode MEDIUM-2). Go failure schedules a
    // resetEngine() so Go state recovers from drift instead of silently
    // diverging forever (REVIEW-shadow-mode HIGH-1).
    const goStart = performance.now();
    const goPromise: Promise<{results: RowChange[]; ms: number}> = (async () => {
      try {
        // Use the streaming variant in shadow mode too, so shadow runs
        // exercise the same code path as Go-primary mode (otherwise
        // shadow would never catch streaming-specific regressions).
        const goRaw = await this.#goBackend!.advanceStream(snapshotChanges);
        // Pattern Z diagnostic (REMOVE after root-cause). Per-(queryID,table)
        // counts of Go's advance output. Pairs with [shadow-classify]: lets
        // us see exactly which queries Go advanced and which it didn't.
        // If a queryID is in shadow-classify TS-only but absent here, Go's
        // pipeline produced nothing for that query → bug is upstream
        // (table not loaded, pipeline not built) rather than IVM evaluation.
        const goBreakdown: Record<string, number> = {};
        const tableBreakdown: Record<string, number> = {};
        for (const rc of goRaw.changes) {
          const k = `${rc.queryID}/${rc.table}`;
          goBreakdown[k] = (goBreakdown[k] ?? 0) + 1;
          tableBreakdown[rc.table] = (tableBreakdown[rc.table] ?? 0) + 1;
        }
        this.#lc.error?.(
          `[go-advance-out] diff=${snapshotChanges.length} ` +
            `tables-in-diff=[${[...new Set(snapshotChanges.map(s => s.table))].join(',')}] ` +
            `go-out=${goRaw.changes.length} ` +
            `by-table=${JSON.stringify(tableBreakdown)} ` +
            `by-query-table=${JSON.stringify(goBreakdown)}`,
        );
        return {
          results: goRaw.changes.map(rc => this.#goRowChangeToRowChange(rc)),
          ms: performance.now() - goStart,
        };
      } catch (e) {
        this.#lc.error?.(`[shadow] Go advance failed: ${e}`);
        this.#scheduleGoReset('shadow-advance-failure');
        return {results: [], ms: performance.now() - goStart};
      }
    })();

    // Run TS advance with the real timer + suppressAbort so very large
    // diffs yield cooperatively (REVIEW-shadow-mode MEDIUM-1) without
    // throwing ResetPipelinesSignal mid-shadow.
    const tsStart = performance.now();
    const tsChanges: RowChange[] = [];
    const tsIterable = this.#advance(replayDiff, timer, numChanges, true);
    for (const change of tsIterable) {
      if (change === 'yield') {
        // Yield to the event loop so Go's RPC response (and other I/O)
        // can be processed mid-advance.
        await new Promise<void>(resolve => setImmediate(resolve));
      } else {
        tsChanges.push(change);
      }
    }
    const tsMs = performance.now() - tsStart;

    const {results: goResults, ms: goMs} = await goPromise;

    this.#lc.debug?.(
      `[shadow][PERF] advance: TS=${tsMs.toFixed(2)}ms Go=${goMs.toFixed(2)}ms ` +
        `changes=${tsChanges.length}`,
    );

    // Phase 1.5: inject TS's companion-row emissions into Go's advance
    // output. TS's live CompanionPipeline (#addQueryImpl line 1254+) emits
    // changes from scalar-EXISTS source tables (e.g. channel_participants)
    // whenever a relevant row enters/leaves. Go received the resolved AST
    // with the inner scalar replaced by a literal, so its pipeline has no
    // equivalent — those events surface as TS-only mismatches. Copy the
    // companion-attributed TS changes into Go's output before comparison.
    // This is the advance-time analogue of the hydrate-time injection in
    // shadowBatchCompare (line 953+). Long term, Phase 2 ports
    // resolveScalarSubqueries + a Go-side companion pipeline so this
    // stopgap becomes unnecessary.
    //
    // Precise matching: only inject when (queryID, table) targets a
    // known companion source for that query — never blanket-copies
    // TS-only events, which would mask real IVM divergence.
    const companionTablesByQuery = new Map<string, Set<string> | null>();
    const getCompanionTables = (queryID: string): Set<string> | null => {
      const cached = companionTablesByQuery.get(queryID);
      if (cached !== undefined) return cached;
      const pipeline = this.#pipelines.get(queryID);
      if (!pipeline || pipeline.companions.length === 0) {
        companionTablesByQuery.set(queryID, null);
        return null;
      }
      const set = new Set<string>();
      for (const c of pipeline.companions) {
        set.add(c.input.getSchema().tableName);
      }
      companionTablesByQuery.set(queryID, set);
      return set;
    };
    let companionInjected = 0;
    for (const tsChange of tsChanges) {
      const tables = getCompanionTables(tsChange.queryID);
      if (tables?.has(tsChange.table)) {
        goResults.push(tsChange);
        companionInjected++;
      }
    }
    if (companionInjected > 0) {
      this.#lc.debug?.(
        `[shadow][advance] injected ${companionInjected} companion ` +
          `events into Go output`,
      );
    }

    // Drop internal-query events (lmids, mutationResults) from the
    // comparison input. Fix #1 already keeps them out of Go's data
    // path, but TS still emits them — without this filter they'd
    // surface as `TS produced N, Go produced N-1` mismatches forever.
    // The return value (yieldTsResults) keeps the full set, since
    // clients legitimately need lmid updates to ack mutations.
    const tsChangesForCompare = tsChanges.filter(
      c => !this.#isInternalQueryID(c.queryID),
    );

    // Compare
    this.#shadowCompare('advance', version, tsChangesForCompare, goResults);

    // Return TS results (already consumed, wrap in array)
    function* yieldTsResults(): Iterable<RowChange | 'yield'> {
      for (const change of tsChanges) {
        yield change;
      }
    }

    return {
      version,
      numChanges,
      changes: this.#trackRowSetSignatures(yieldTsResults()),
    };
  }

  /**
   * Compare TS and Go results for shadow mode.
   * Normalizes ordering (sort by queryID + table + rowKey) since
   * Go may process pipelines in different order than TS.
   */
  #shadowCompare(
    operation: string,
    context: string,
    tsChanges: RowChange[],
    goChanges: RowChange[],
  ): void {
    const normalize = (changes: RowChange[]) =>
      changes
        .map(c => ({
          type: c.type,
          queryID: c.queryID,
          table: c.table,
          // stableStringify deep-sorts nested object keys so jsonb / json
          // columns compare structurally regardless of either side's map
          // iteration order. The previous JSON.stringify(v, topKeysArray)
          // form gutted nested content because the replacer-array filter
          // applies recursively — fixed REVIEW-shadow-mode CRITICAL-1.
          rowKey: stableStringify(c.rowKey),
          row: stableStringify(c.row),
        }))
        // Direct compare instead of localeCompare for deterministic ordering
        // across locales (REVIEW-shadow-mode MEDIUM-3).
        .sort((a, b) => {
          if (a.queryID !== b.queryID) return a.queryID < b.queryID ? -1 : 1;
          if (a.table !== b.table) return a.table < b.table ? -1 : 1;
          if (a.rowKey !== b.rowKey) return a.rowKey < b.rowKey ? -1 : 1;
          return a.type - b.type;
        });

    const tsNorm = normalize(tsChanges);
    const goNorm = normalize(goChanges);

    if (tsNorm.length !== goNorm.length) {
      this.#lc.error?.(
        `[shadow] MISMATCH in ${operation} (${context}): ` +
          `TS produced ${tsNorm.length} changes, Go produced ${goNorm.length} changes`,
      );
      this.#logShadowDiff(operation, context, tsNorm, goNorm);
      return;
    }

    // Log up to MAX_MISMATCH_LOG mismatched indices before returning, so
    // operators see the shape of the divergence not just the first row
    // (REVIEW-shadow-mode MEDIUM-4). Row contents are redacted by default
    // to avoid PII leakage into logs; ZERO_GO_SIDECAR_SHADOW_VERBOSE=true
    // unlocks the full payload (REVIEW-final MED-SHADOW-4).
    const MAX_MISMATCH_LOG = 5;
    const verbose = isGoShadowVerbose(this.#config);
    let mismatches = 0;
    for (let i = 0; i < tsNorm.length; i++) {
      const ts = tsNorm[i];
      const go = goNorm[i];
      if (
        ts.type !== go.type ||
        ts.queryID !== go.queryID ||
        ts.table !== go.table ||
        ts.rowKey !== go.rowKey ||
        ts.row !== go.row
      ) {
        if (mismatches < MAX_MISMATCH_LOG) {
          const tsSummary = verbose
            ? JSON.stringify(ts)
            : `{type:${ts.type},queryID:${ts.queryID},table:${ts.table},rowKey:${ts.rowKey},row.len:${ts.row.length}}`;
          const goSummary = verbose
            ? JSON.stringify(go)
            : `{type:${go.type},queryID:${go.queryID},table:${go.table},rowKey:${go.rowKey},row.len:${go.row.length}}`;
          this.#lc.error?.(
            `[shadow] MISMATCH in ${operation} (${context}) at index ${i}: ` +
              `TS=${tsSummary} Go=${goSummary}`,
          );
        }
        mismatches++;
      }
    }
    if (mismatches > MAX_MISMATCH_LOG) {
      this.#lc.error?.(
        `[shadow] ${operation} (${context}): ${mismatches} total mismatches ` +
          `(showed first ${MAX_MISMATCH_LOG})`,
      );
    }
    if (mismatches > 0) return;

    // Success matches are demoted to debug — at soak rates (~47/sec) this
    // info-level log was swamping production log pipelines and obscuring
    // real errors. REVIEW-final LOW-SHADOW-1.
    this.#lc.debug?.(
      `[shadow] ${operation} (${context}): TS and Go match ` +
        `(${tsNorm.length} changes)`,
    );
  }

  #logShadowDiff(
    operation: string,
    context: string,
    tsNorm: Array<{type: number; queryID: string; table: string; rowKey: string; row: string}>,
    goNorm: Array<{type: number; queryID: string; table: string; rowKey: string; row: string}>,
  ): void {
    // Find entries in TS but not in Go
    const goSet = new Set(goNorm.map(g => `${g.type}|${g.queryID}|${g.table}|${g.rowKey}`));
    const tsOnly = tsNorm.filter(
      t => !goSet.has(`${t.type}|${t.queryID}|${t.table}|${t.rowKey}`),
    );
    const tsSet = new Set(tsNorm.map(t => `${t.type}|${t.queryID}|${t.table}|${t.rowKey}`));
    const goOnly = goNorm.filter(
      g => !tsSet.has(`${g.type}|${g.queryID}|${g.table}|${g.rowKey}`),
    );

    // Default-redact: keys-only summary; full payload behind
    // ZERO_GO_SIDECAR_SHADOW_VERBOSE=true (REVIEW-final MED-SHADOW-4).
    const verbose = isGoShadowVerbose(this.#config);
    const redact = (xs: typeof tsNorm) =>
      verbose
        ? JSON.stringify(xs.slice(0, 5))
        : JSON.stringify(
            xs.slice(0, 5).map(x => ({
              type: x.type,
              queryID: x.queryID,
              table: x.table,
              rowKey: x.rowKey,
            })),
          );

    if (tsOnly.length > 0) {
      this.#lc.error?.(
        `[shadow] ${operation} (${context}): ${tsOnly.length} changes in TS only (first 5): ` +
          redact(tsOnly),
      );
      // Diagnostic classifier (REMOVE after Pattern X/Y verification).
      // Tags each TS-only row with: internal-query | result-table | related-table |
      // unmapped-table | no-pipeline. Lets us confirm whether 100% of advance
      // under-produce rows fall under {internal-query, related-table} (the two
      // expected gaps: internal queries not in Go's set, and EXISTS join-children
      // that TS emits but Go doesn't after scalar pre-resolution).
      const classifications = tsOnly.slice(0, 10).map(t => {
        const labels: string[] = [];
        if (t.queryID === 'lmids' || t.queryID === 'mutationResults') {
          labels.push('internal-query');
        }
        const pipeline = this.#pipelines.get(t.queryID);
        if (!pipeline) {
          labels.push('no-pipeline');
        } else {
          const ast = pipeline.transformedAst;
          if (t.table === ast.table) {
            labels.push('result-table');
          } else {
            // Collect tables reachable through related[] (one level — enough
            // to identify EXISTS join-children for the conversations queries).
            const relatedTables = new Set<string>();
            const walk = (a: typeof ast): void => {
              for (const r of a.related ?? []) {
                relatedTables.add(r.subquery.table);
                walk(r.subquery);
              }
              // whereExists chains: condition.related is also a subquery
              const visitCond = (c: typeof a.where): void => {
                if (!c) return;
                if (c.type === 'and' || c.type === 'or') {
                  for (const sub of c.conditions) visitCond(sub);
                } else if (c.type === 'correlatedSubquery') {
                  relatedTables.add(c.related.subquery.table);
                  walk(c.related.subquery);
                }
              };
              visitCond(a.where);
            };
            walk(ast);
            labels.push(
              relatedTables.has(t.table) ? 'related-table' : 'unmapped-table',
            );
          }
        }
        return `${t.queryID}/${t.table}=[${labels.join(',')}]`;
      });
      this.#lc.error?.(
        `[shadow-classify] ${operation} (${context}): ` +
          classifications.join(' '),
      );
    }
    if (goOnly.length > 0) {
      this.#lc.error?.(
        `[shadow] ${operation} (${context}): ${goOnly.length} changes in Go only (first 5): ` +
          redact(goOnly),
      );
    }
  }

  // ─── End Shadow Mode ───────────────────────────────────────────────

  *#advance(
    diff: SnapshotDiff,
    timer: Timer,
    numChanges: number,
    suppressAbort: boolean = false,
  ): Iterable<RowChange | 'yield'> {
    assert(
      this.#hydrateContext === null,
      'Cannot advance while hydration is in progress',
    );
    const totalHydrationTimeMs = this.totalHydrationTimeMs();
    this.#advanceContext = {
      timer,
      totalHydrationTimeMs,
      numChanges,
      pos: 0,
      suppressAbort,
    };
    this.#lc.info?.(
      `starting pipeline advancement of ${numChanges} changes with an ` +
        `advancement time limited based on total hydration time of ` +
        `${totalHydrationTimeMs} ms.`,
    );
    try {
      for (const {table, prevValues, nextValue} of diff) {
        // Advance progress is checked each time a row is fetched
        // from a TableSource during push processing, but some pushes
        // don't read any rows.  Check progress here before processing
        // the next change.
        if (this.#shouldAdvanceYieldMaybeAbortAdvance()) {
          yield 'yield';
        }
        const start = timer.totalElapsed();

        // `type` label for the #advanceTime histogram. Previously left
        // undeclared → recorded as undefined, while the Go path passes a
        // real string; histogram dimensions diverged. REVIEW-final MED-TS-5.
        let type: 'add' | 'remove' | 'edit' | undefined;
        try {
          const tableSource = this.#tables.get(table);
          if (!tableSource) {
            // no pipelines read from this table, so no need to process the change
            continue;
          }
          const primaryKey = mustGetPrimaryKey(this.#primaryKeys, table);
          let editOldRow: Row | undefined = undefined;
          for (const prevValue of prevValues) {
            if (
              nextValue &&
              deepEqual(
                getRowKey(primaryKey, prevValue as Row) as JSONValue,
                getRowKey(primaryKey, nextValue as Row) as JSONValue,
              )
            ) {
              editOldRow = prevValue;
            } else {
              if (nextValue) {
                this.#conflictRowsDeleted.add(1);
              }
              type = 'remove';
              yield* this.#push(
                tableSource,
                makeSourceChangeRemove(prevValue as Row),
              );
            }
          }
          if (nextValue) {
            if (editOldRow) {
              type = 'edit';
              yield* this.#push(
                tableSource,
                makeSourceChangeEdit(nextValue as Row, editOldRow),
              );
            } else {
              type = 'add';
              yield* this.#push(
                tableSource,
                makeSourceChangeAdd(nextValue as Row),
              );
            }
          }
        } finally {
          this.#advanceContext.pos++;
        }

        const elapsed = timer.totalElapsed() - start;
        this.#advanceTime.recordMs(elapsed, {
          table,
          type,
        });
      }

      // Set the new snapshot on all TableSources.
      const {curr} = diff;
      for (const table of this.#tables.values()) {
        table.setDB(curr.db.db);
      }
      this.#tableSourcesVersion = curr.version;
      this.#ensureCostModelExistsIfEnabled(curr.db.db);
      this.#lc.debug?.(`Advanced to ${curr.version}`);
    } finally {
      this.#advanceContext = null;
    }
  }

  /** Implements `BuilderDelegate.getSource()` */
  #getSource(tableName: string): Source {
    let source = this.#tables.get(tableName);
    if (source) {
      return source;
    }

    const tableSpec = mustGetTableSpec(this.#tableSpecs, tableName);
    const primaryKey = mustGetPrimaryKey(this.#primaryKeys, tableName);

    const {db} = this.#snapshotter.current();
    source = new TableSource(
      this.#lc,
      this.#logConfig,
      db.db,
      tableName,
      tableSpec.zqlSpec,
      primaryKey,
      () => this.#shouldYield(),
    );
    this.#tables.set(tableName, source);
    this.#lc.debug?.(`created TableSource for ${tableName}`);
    return source;
  }

  #shouldYield(): boolean {
    if (this.#hydrateContext) {
      // Shadow-mode opens an async boundary mid-iteration (await Go
      // RPC); the surrounding view-syncer's stop sequence can stop
      // the timer underneath the still-running TS hydrate. Guard
      // against the assertion — return false so the generator exits
      // its loop cleanly on the next iteration instead of throwing.
      // No-op in Go-primary (no TS hydrate generator interleaved with
      // async Go calls) and TS-only (no async window inside hydrate).
      const t = this.#hydrateContext.timer;
      if (!t.running()) return false;
      return t.elapsedLap() > this.#yieldThresholdMs();
    }
    if (this.#advanceContext) {
      return this.#shouldAdvanceYieldMaybeAbortAdvance();
    }
    throw new Error('shouldYield called outside of hydration or advancement');
  }

  /**
   * Cancel the advancement processing, by throwing a ResetPipelinesSignal, if
   * it has taken longer than half the total hydration time to make it through
   * half of the advancement, or if processing time exceeds total hydration
   * time.  This serves as both a circuit breaker for very large transactions,
   * as well as a bound on the amount of time the previous connection locks
   * the inactive WAL file (as the lock prevents WAL2 from switching to the
   * free WAL when the current one is over the size limit, which can make
   * the WAL grow continuously and compound slowness).
   * This is checked:
   * 1. before starting to process each change in an advancement is processed
   * 2. whenever a row is fetched from a TableSource during push processing
   */
  #shouldAdvanceYieldMaybeAbortAdvance(): boolean {
    const {
      pos,
      numChanges,
      timer: advanceTimer,
      totalHydrationTimeMs,
      suppressAbort,
    } = must(this.#advanceContext);
    const elapsed = advanceTimer.totalElapsed();
    if (
      !suppressAbort &&
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
    // Same shadow-mode race as the hydrate guard above: the async
    // `await goPromise` boundary inside #shadowAdvance lets the
    // surrounding view-syncer stop the timer mid-iteration. Skip the
    // elapsedLap (which would assert) and return false — the advance
    // generator finishes its current step and exits on the next loop
    // tick. The goal state (Go-primary via #goAdvance) does not run
    // this generator and is unaffected.
    if (!advanceTimer.running()) return false;
    return advanceTimer.elapsedLap() > this.#yieldThresholdMs();
  }

  /** Implements `BuilderDelegate.createStorage()` */
  #createStorage(): Storage {
    return this.#storage.createStorage();
  }

  *#push(
    source: TableSource,
    change: SourceChange,
  ): Iterable<RowChange | 'yield'> {
    this.#startAccumulating();
    try {
      for (const val of source.genPush(change)) {
        if (val === 'yield') {
          yield 'yield';
        }
        for (const changeOrYield of this.#stopAccumulating().stream()) {
          yield changeOrYield;
        }
        this.#startAccumulating();
      }
    } finally {
      if (this.#streamer !== null) {
        this.#stopAccumulating();
      }
    }
  }

  #startAccumulating() {
    assert(this.#streamer === null, 'Streamer already started');
    this.#streamer = new Streamer(must(this.#primaryKeys), this.#tableSpecs);
  }

  #stopAccumulating(): Streamer {
    const streamer = this.#streamer;
    assert(streamer, 'Streamer not started');
    this.#streamer = null;
    return streamer;
  }
}

class Streamer {
  readonly #primaryKeys: Map<string, PrimaryKey>;
  readonly #tableSpecs: Map<string, LiteAndZqlSpec>;

  constructor(
    primaryKeys: Map<string, PrimaryKey>,
    tableSpecs: Map<string, LiteAndZqlSpec>,
  ) {
    this.#primaryKeys = primaryKeys;
    this.#tableSpecs = tableSpecs;
  }

  readonly #changes: [
    queryID: string,
    schema: SourceSchema,
    changes: Iterable<Change | 'yield'>,
  ][] = [];

  accumulate(
    queryID: string,
    schema: SourceSchema,
    changes: Iterable<Change | 'yield'>,
  ): this {
    this.#changes.push([queryID, schema, changes]);
    return this;
  }

  *stream(): Iterable<RowChange | 'yield'> {
    for (const [queryID, schema, changes] of this.#changes) {
      yield* this.#streamChanges(queryID, schema, changes);
    }
  }

  *#streamChanges(
    queryID: string,
    schema: SourceSchema,
    changes: Iterable<Change | 'yield'>,
  ): Iterable<RowChange | 'yield'> {
    // We do not sync rows gathered by the permissions
    // system to the client.
    if (schema.system === 'permissions') {
      return;
    }

    for (const change of changes) {
      if (change === 'yield') {
        yield change;
        continue;
      }
      const type = change[ChangeIndex.TYPE];
      switch (type) {
        case ChangeType.REMOVE:
        case ChangeType.ADD: {
          yield* this.#streamNodes(queryID, schema, type, () => [
            change[ChangeIndex.NODE],
          ]);
          break;
        }

        case ChangeType.CHILD: {
          const child = change[ChangeIndex.CHILD_DATA];
          const childSchema = must(
            schema.relationships[child.relationshipName],
          );

          yield* this.#streamChanges(queryID, childSchema, [child.change]);
          break;
        }
        case ChangeType.EDIT:
          yield* this.#streamNodes(queryID, schema, type, () => [
            {row: change[ChangeIndex.NODE].row, relationships: {}},
          ]);
          break;
        default:
          unreachable(change[ChangeIndex.TYPE]);
      }
    }
  }

  *#streamNodes(
    queryID: string,
    schema: SourceSchema,
    op: ChangeType.ADD | ChangeType.REMOVE | ChangeType.EDIT,
    nodes: () => Iterable<Node | 'yield'>,
  ): Iterable<RowChange | 'yield'> {
    const {tableName: table, system} = schema;

    const primaryKey = must(this.#primaryKeys.get(table));
    const spec = must(this.#tableSpecs.get(table)).tableSpec;

    // We do not sync rows gathered by the permissions
    // system to the client.
    if (system === 'permissions') {
      return;
    }

    for (const node of nodes()) {
      if (node === 'yield') {
        yield node;
        continue;
      }
      const {relationships} = node;
      let {row} = node;
      const rowKey = getRowKey(primaryKey, row);
      if (op !== ChangeType.REMOVE) {
        const rowVersion = row[ZERO_VERSION_COLUMN_NAME];
        if (
          typeof rowVersion === 'string' &&
          rowVersion < (spec.minRowVersion ?? '00')
        ) {
          row = {...row, [ZERO_VERSION_COLUMN_NAME]: spec.minRowVersion};
        }
      }

      yield {
        type: op,
        queryID,
        table,
        rowKey,
        row: op === ChangeType.REMOVE ? undefined : row,
      } as RowChange;

      for (const [relationship, children] of Object.entries(relationships)) {
        const childSchema = must(schema.relationships[relationship]);
        yield* this.#streamNodes(queryID, childSchema, op, children);
      }
    }
  }
}

function* toAdds(nodes: Iterable<Node | 'yield'>): Iterable<Change | 'yield'> {
  for (const node of nodes) {
    if (node === 'yield') {
      yield node;
      continue;
    }
    yield [ChangeType.ADD, node, null];
  }
}

function getRowKey(cols: PrimaryKey, row: Row): RowKey {
  return Object.fromEntries(cols.map(col => [col, must(row[col])]));
}

/**
 * Core hydration logic used by {@link PipelineDriver#addQuery}, extracted to a
 * function for reuse by the analyze-query RPC path so that analysis hydrates
 * queries the same way the view-syncer does in production.
 */
export function* hydrate(
  input: Input,
  hash: string,
  clientSchema: ClientSchema,
  tableSpecs: Map<string, LiteAndZqlSpec>,
): Iterable<RowChange | 'yield'> {
  const res = input.fetch({});
  const streamer = new Streamer(
    buildPrimaryKeys(clientSchema),
    tableSpecs,
  ).accumulate(hash, input.getSchema(), toAdds(res));
  yield* streamer.stream();
}

export function* hydrateInternal(
  input: Input,
  hash: string,
  primaryKeys: Map<string, PrimaryKey>,
  tableSpecs: Map<string, LiteAndZqlSpec>,
): Iterable<RowChange | 'yield'> {
  const res = input.fetch({});
  const streamer = new Streamer(primaryKeys, tableSpecs).accumulate(
    hash,
    input.getSchema(),
    toAdds(res),
  );
  yield* streamer.stream();
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

function mustGetPrimaryKey(
  primaryKeys: Map<string, PrimaryKey> | null,
  table: string,
): PrimaryKey {
  const pKeys = must(primaryKeys, 'primaryKey map must be non-null');

  const rv = pKeys.get(table);
  assert(
    rv,
    () =>
      // oxlint-disable-next-line typescript/restrict-template-expressions e18e/prefer-array-to-sorted
      `table '${table}' is not one of: ${[...pKeys.keys()].sort()}. ` +
      `Check the spelling and ensure that the table has a primary key.`,
  );
  return rv;
}

/**
 * Compares two scalar subquery resolved values for equality.
 * Unlike `valuesEqual` in data.ts (which treats null != null for join
 * semantics), this uses identity semantics: undefined === undefined
 * (no row matched), null === null (row matched but field was NULL).
 */
function scalarValuesEqual(
  a: LiteralValue | null | undefined,
  b: LiteralValue | null | undefined,
): boolean {
  return a === b;
}

/**
 * Recursive JSON stringify with deterministic key ordering at every depth.
 * Used by shadow-mode compare so that TS-parsed JSON (insertion-order keys)
 * and Go-deserialized JSON (map-iteration-order keys) compare structurally.
 *
 * Handles cases that plain JSON.stringify mishandles, so a compare error
 * doesn't masquerade as a Go RPC failure (REVIEW-final HIGH-SHADOW-1):
 *   - bigint: coerce to Number when safe; emit a marker token otherwise.
 *     msgpackr decodes Go's non-compact uint64 as BigInt; TS-native side
 *     collapses to Number via fromSQLiteType — comparison must align.
 *   - NaN / ±Infinity: emit a distinct token rather than `null` (which
 *     JSON.stringify would silently produce and hide divergence).
 *   - undefined: same token treatment; distinguishes from missing keys.
 */
function stableStringify(v: unknown): string {
  if (v === undefined) return '"__undef__"';
  if (v === null) return 'null';
  if (typeof v === 'bigint') {
    if (
      v <= BigInt(Number.MAX_SAFE_INTEGER) &&
      v >= BigInt(Number.MIN_SAFE_INTEGER)
    ) {
      return String(Number(v));
    }
    return `"__bigint:${v.toString()}__"`;
  }
  if (typeof v === 'number') {
    if (Number.isNaN(v)) return '"__nan__"';
    if (v === Infinity) return '"__inf__"';
    if (v === -Infinity) return '"__-inf__"';
    return JSON.stringify(v);
  }
  if (typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) {
    return '[' + v.map(stableStringify).join(',') + ']';
  }
  const obj = v as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return (
    '{' +
    keys
      .map(k => JSON.stringify(k) + ':' + stableStringify(obj[k]))
      .join(',') +
    '}'
  );
}

/** Map Go's numeric ivm.ChangeType to the histogram label TS uses. */
function goTypeToLabel(t: number): 'add' | 'remove' | 'edit' | undefined {
  if (t === 0) return 'add';
  if (t === 1) return 'remove';
  if (t === 2) return 'edit';
  return undefined;
}

/**
 * Map an upstream PostgreSQL type name to the column-type tag the Go
 * sidecar understands ('boolean' | 'number' | 'string' | 'null' | 'json').
 *
 * Unrecognized types previously fell through silently to 'string', which
 * silently mis-types bytea (would be base64 / hex on TS, raw bytes / TEXT on
 * Go), arrays (Postgres `int4[]` literal text, e.g. `{1,2,3}`), INTERVAL,
 * geometric types, network types, range types. We now log a one-time
 * warning per unrecognized type so operators see the gap, and document
 * the explicit-handling exceptions (REVIEW-final MED-TS-2).
 *
 * Caller-side de-dup of warnings is handled via a module-level Set so a
 * 50-table schema doesn't produce 50 lines for the same unknown type.
 */
const pgTypeWarningsSeen = new Set<string>();
function pgTypeToGoType(
  pgType: string,
  warn?: (msg: string) => void,
): 'string' | 'number' | 'boolean' | 'null' | 'json' {
  // dataType may be in "lite type string" format: "bool|nn", "int4|nn", etc.
  // Extract just the upstream type (before any pipe delimiter).
  const delim = pgType.indexOf('|');
  const t = (delim > 0 ? pgType.substring(0, delim) : pgType).toUpperCase();
  if (t === 'BOOL' || t === 'BOOLEAN') return 'boolean';
  if (
    t === 'INT2' || t === 'INT4' || t === 'INT8' ||
    t === 'SMALLINT' || t === 'INTEGER' || t === 'BIGINT' ||
    t === 'FLOAT4' || t === 'FLOAT8' ||
    t === 'REAL' || t === 'DOUBLE PRECISION' ||
    t === 'NUMERIC' || t === 'DECIMAL' ||
    t === 'TIMESTAMP' || t === 'TIMESTAMPTZ' ||
    t === 'TIMESTAMP WITHOUT TIME ZONE' || t === 'TIMESTAMP WITH TIME ZONE' ||
    t === 'DATE'
  ) return 'number';
  if (t === 'JSON' || t === 'JSONB') return 'json';
  // Explicitly recognised string-shaped types — keep this list growing.
  if (
    t === 'TEXT' || t === 'VARCHAR' || t === 'CHAR' || t === 'BPCHAR' ||
    t === 'UUID' || t === 'CITEXT' || t === 'NAME'
  ) return 'string';
  // Postgres array types (e.g. INT4[], TEXT[]) — Postgres emits them as
  // text-encoded array literals ("{1,2,3}"); both sides currently treat as
  // string. Document the convention rather than mis-claiming json.
  if (t.endsWith('[]')) {
    if (warn && !pgTypeWarningsSeen.has(t)) {
      pgTypeWarningsSeen.add(t);
      warn(`PostgreSQL array type ${t} treated as opaque text — Go-side IVM cannot index inside`);
    }
    return 'string';
  }
  // BYTEA: text-encoded binary (hex on PG side via SQLite replica). Both
  // sides treat as string for now; document the limitation.
  if (t === 'BYTEA') {
    if (warn && !pgTypeWarningsSeen.has(t)) {
      pgTypeWarningsSeen.add(t);
      warn(`BYTEA treated as text-encoded string — binary content opaque to Go IVM`);
    }
    return 'string';
  }
  // Truly unknown type — fall back to string but log once so the gap is
  // visible. Operators can add explicit mappings as they appear.
  if (warn && !pgTypeWarningsSeen.has(t)) {
    pgTypeWarningsSeen.add(t);
    warn(`unrecognised PostgreSQL type "${t}" mapped to 'string' — Go IVM may produce wrong results`);
  }
  return 'string';
}
