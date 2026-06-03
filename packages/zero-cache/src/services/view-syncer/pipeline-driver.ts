import type {LogContext} from '@rocicorp/logger';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import {deepEqual, type JSONValue} from '../../../../shared/src/json.ts';
import {must} from '../../../../shared/src/must.ts';
import type {AST, LiteralValue} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import {buildPipeline} from '../../../../zql/src/builder/builder.ts';
import {planQuery} from '../../../../zql/src/planner/planner-builder.ts';
import {completeOrdering} from '../../../../zql/src/query/complete-ordering.ts';
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
import type {Condition} from '../../../../zero-protocol/src/ast.ts';
import {createSQLiteCostModel} from '../../../../zqlite/src/sqlite-cost-model.ts';
import {TableSource, fromSQLiteTypes} from '../../../../zqlite/src/table-source.ts';
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
  goDriftAuditSqlGroundTruth,
} from './go-sidecar/go-compute-backend.ts';
import type {SidecarManager} from './go-sidecar/sidecar-manager.ts';
import type {SnapshotChange, RowChange as GoRowChange} from './go-sidecar/go-ivm-client.ts';
import {DriftError, StaleInitEpochError} from './go-sidecar/go-ivm-client.ts';
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

/**
 * Convert an audit AST into a parameterized SQL string + values array
 * suitable for `better-sqlite3.prepare(text).all(...values)`. Unlike
 * zqlite's `buildSelectQuery`, this version handles correlated EXISTS /
 * NOT EXISTS subqueries by emitting nested `EXISTS (SELECT 1 FROM ...)`
 * with the correlation expressed via outer-alias-qualified columns.
 *
 * Scope: simple/and/or/correlatedSubquery in WHERE. Drops AST.related
 * (output-only joins, no filter effect). Preserves cursor + orderBy + limit.
 *
 * Designed for the drift audit's SQL ground-truth comparator — produces
 * the SAME row-key set the IVM pipeline should emit.
 */
function buildAuditSQL(
  ast: AST,
  tableSpecs: Map<string, LiteAndZqlSpec>,
): {text: string; values: unknown[]} {
  const values: unknown[] = [];
  let aliasCounter = 0;
  const nextAlias = () => `t${aliasCounter++}`;
  const outerAlias = nextAlias();
  const outerSpec = tableSpecs.get(ast.table);
  if (!outerSpec) throw new Error(`no spec for table ${ast.table}`);

  // Quote SQLite identifier — same approach as @databases/sql.
  const q = (s: string) => `"${s.replace(/"/g, '""')}"`;

  function valuePosToSQL(
    vp: {type: 'column'; name: string} | {type: 'literal'; value: unknown} | {type: string},
    tableAlias: string,
  ): string {
    const t = vp as {type: string; name?: string; value?: unknown};
    if (t.type === 'column') {
      return `${tableAlias}.${q(t.name as string)}`;
    }
    if (t.type === 'literal') {
      // Detect type at runtime — literals don't carry type info in the AST.
      // SQLite can only bind number / string / bigint / Buffer / null, so
      // convert booleans → 0/1 and other compound values to their JSON form.
      const v = t.value;
      if (typeof v === 'boolean') {
        values.push(v ? 1 : 0);
      } else if (v === null || v === undefined) {
        values.push(null);
      } else if (typeof v === 'number' || typeof v === 'string' || typeof v === 'bigint') {
        values.push(v);
      } else {
        // arrays/objects — caller handles arrays via IN-path above; otherwise stringify.
        values.push(JSON.stringify(v));
      }
      return '?';
    }
    throw new Error(`unsupported value position type: ${t.type}`);
  }

  function condToSQL(
    cond: Condition,
    tableAlias: string,
  ): string {
    switch (cond.type) {
      case 'simple': {
        const left = valuePosToSQL(cond.left, tableAlias);
        const right = valuePosToSQL(cond.right, tableAlias);
        // Map ZQL ops to SQLite ops. ILIKE → LIKE (SQLite LIKE is
        // case-insensitive by default).
        const op =
          cond.op === 'ILIKE' ? 'LIKE' :
          cond.op === 'NOT ILIKE' ? 'NOT LIKE' :
          cond.op === 'IN' ? 'IN' :
          cond.op === 'NOT IN' ? 'NOT IN' :
          cond.op;
        // IN/NOT IN with literal array → expand via json_each
        if ((cond.op === 'IN' || cond.op === 'NOT IN') && cond.right.type === 'literal') {
          // Use json_each because the literal is an array.
          // Replace the last pushed `?` value with the JSON-encoded array.
          values[values.length - 1] = JSON.stringify(cond.right.value);
          return `${left} ${op} (SELECT value FROM json_each(?))`;
        }
        return `${left} ${op} ${right}`;
      }
      case 'and': {
        if (cond.conditions.length === 0) return '1';
        return '(' + cond.conditions.map(c => condToSQL(c, tableAlias)).join(' AND ') + ')';
      }
      case 'or': {
        if (cond.conditions.length === 0) return '0';
        return '(' + cond.conditions.map(c => condToSQL(c, tableAlias)).join(' OR ') + ')';
      }
      case 'correlatedSubquery': {
        const sub = cond.related.subquery;
        const corr = cond.related.correlation;
        const subAlias = nextAlias();
        const subSpec = tableSpecs.get(sub.table);
        if (!subSpec) throw new Error(`no spec for subquery table ${sub.table}`);

        // Correlation: childField on inner = parentField on outer.
        const corrClauses: string[] = [];
        for (let i = 0; i < corr.childField.length; i++) {
          corrClauses.push(`${subAlias}.${q(corr.childField[i])} = ${tableAlias}.${q(corr.parentField[i])}`);
        }
        const corrJoin = corrClauses.join(' AND ');

        const subWhere = sub.where ? condToSQL(sub.where, subAlias) : null;
        const wherePart = subWhere ? `${corrJoin} AND ${subWhere}` : corrJoin;
        const op = cond.op === 'EXISTS' ? 'EXISTS' : 'NOT EXISTS';
        return `${op} (SELECT 1 FROM ${q(sub.table)} ${subAlias} WHERE ${wherePart})`;
      }
      default:
        throw new Error(`unsupported condition type: ${(cond as {type: string}).type}`);
    }
  }

  // WHERE clause assembly: main where + cursor predicate
  const wherePieces: string[] = [];
  if (ast.where) {
    wherePieces.push(condToSQL(ast.where, outerAlias));
  }
  // Cursor: WHERE (sortField < cursor) OR (basis='at' AND sortField = cursor)
  // For multi-field orderBy, produces lexicographic comparison.
  if (ast.start && ast.orderBy && ast.orderBy.length > 0) {
    const cursorRow = ast.start.row;
    const inclusive = !ast.start.exclusive;
    const ranges: string[] = [];
    for (let i = 0; i < ast.orderBy.length; i++) {
      const group: string[] = [];
      for (let j = 0; j <= i; j++) {
        const [field, dir] = ast.orderBy[j];
        if (j === i) {
          const op = dir === 'asc' ? '>' : '<';
          values.push(cursorRow[field]);
          group.push(`${outerAlias}.${q(field)} ${op} ?`);
        } else {
          values.push(cursorRow[field]);
          group.push(`${outerAlias}.${q(ast.orderBy[j][0])} = ?`);
        }
      }
      ranges.push('(' + group.join(' AND ') + ')');
    }
    if (inclusive) {
      const eqs: string[] = [];
      for (const [field] of ast.orderBy) {
        values.push(cursorRow[field]);
        eqs.push(`${outerAlias}.${q(field)} = ?`);
      }
      ranges.push('(' + eqs.join(' AND ') + ')');
    }
    wherePieces.push('(' + ranges.join(' OR ') + ')');
  }

  const cols = Object.keys(outerSpec.tableSpec.columns)
    .map(c => `${outerAlias}.${q(c)}`)
    .join(', ');
  let sql = `SELECT ${cols} FROM ${q(ast.table)} ${outerAlias}`;
  if (wherePieces.length > 0) sql += ` WHERE ${wherePieces.join(' AND ')}`;
  if (ast.orderBy && ast.orderBy.length > 0) {
    const parts = ast.orderBy.map(([f, d]) => `${outerAlias}.${q(f)} ${d.toUpperCase()}`);
    sql += ` ORDER BY ${parts.join(', ')}`;
  }
  if (ast.limit !== undefined) sql += ` LIMIT ${ast.limit}`;
  return {text: sql, values};
}

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
  /**
   * True iff a lap is currently in progress. Called by TableSource.#shouldYield
   * at runtime to decide whether elapsedLap is safe to invoke. Was omitted
   * from the exported type, forcing every noop-timer site to `as unknown as
   * Timer` to keep tsc happy. Declaring it here means tsc verifies new
   * Timer-typed values include it — preventing the silent runtime
   * `TypeError: t.running is not a function` we hit on the drift-audit path.
   */
  running: () => boolean;
};

/**
 * No matter how fast hydration is, advancement is given at least this long to
 * complete before doing a pipeline reset.
 */
const MIN_ADVANCEMENT_TIME_LIMIT_MS = 50;

/**
 * Minimum gap between drift-audit "heartbeat OK" INFO lines per driver.
 * Successful audits log at DEBUG (high cadence + low signal); a periodic
 * INFO heartbeat gives operators a "yes the audit is firing" signal
 * without bumping ZERO_LOG_LEVEL=debug for the whole syncer.
 */
const DRIFT_AUDIT_HEARTBEAT_MS = 5 * 60_000;

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
  // Incremented at the top of #runDriftAudit before any guards, so a flat
  // #driftAuditRuns counter (no comparisons happening) can be distinguished
  // from a never-firing timer. Without this, an idle CG and a broken-timer
  // CG looked identical in metrics (both at zero) — the exact visibility
  // gap that masked the prod incident where the Go sidecar refused to
  // start and drift-audit was silently off for hours.
  readonly #driftAuditTicks = getOrCreateCounter(
    'sync',
    'ivm.drift-audit-ticks',
    'Drift audit timer fires (regardless of skip/run outcome)',
  );
  // Incremented when the TS-vs-Go pipeline-count cross-check finds Go
  // has fewer pipelines than TS expects. This is the freeze signal:
  // per-CG recovery (drift, sidecar-unavail, engine-not-init) dropped
  // pipeline state somewhere, so every advance returns empty changes
  // and the client view diverges silently. Triggers self-heal via
  // resetEngine.
  readonly #driftAuditFreezes = getOrCreateCounter(
    'sync',
    'ivm.drift-audit-freezes',
    'Audit detected Go has fewer pipelines than TS (post-C2 defense-in-depth)',
  );
  // Bucketed counter for advance-dropped events in Go-primary mode. Pre-C5
  // fix, ALL errors from Go's advanceStream were silently swallowed as
  // empty changes — the CVR committed the empty diff and the client view
  // diverged with no operator-visible signal. The forensics gap was total:
  // a wire-protocol violation (chunk-order, missing terminal frame),
  // a stale-epoch error, and a sidecar restart all looked identical.
  // Each reason gets its own counter so dashboards can distinguish them.
  readonly #advanceDroppedProtocol = getOrCreateCounter(
    'sync',
    'ivm.advance-dropped-protocol',
    'Go-primary advance dropped due to wire-protocol violation (escalated to restart)',
  );
  readonly #advanceDroppedSidecar = getOrCreateCounter(
    'sync',
    'ivm.advance-dropped-sidecar',
    'Go-primary advance dropped due to sidecar unavailability / restart',
  );
  readonly #advanceDroppedStaleEpoch = getOrCreateCounter(
    'sync',
    'ivm.advance-dropped-stale-epoch',
    'Go-primary advance dropped due to stale initEpoch (torn-down view-syncer)',
  );
  readonly #advanceDroppedOther = getOrCreateCounter(
    'sync',
    'ivm.advance-dropped-other',
    'Go-primary advance dropped due to unclassified Go-side error',
  );
  /**
   * D11: per-reason counter for #scheduleGoReset. Pre-fix every reset
   * looked the same in metrics — a burst of shadow-batch-failure resets
   * was indistinguishable from a drift-audit-pipeline-count-mismatch
   * burst. Now each call increments with a {reason} attribute so dashboards
   * can attribute restarts to the trigger.
   */
  readonly #goResetScheduled = getOrCreateCounter(
    'sync',
    'ivm.go-reset-scheduled',
    'Go engine resets scheduled (label: reason)',
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
  // Timestamp of the last drift-audit INFO heartbeat. Each successful audit-OK
  // path is debug-only (high noise), so without this heartbeat operators have
  // no INFO-level signal that the audit machinery is actually firing — just
  // metrics they have to know to look at. With a 5-minute heartbeat, the
  // "[drift-audit] heartbeat OK" line at INFO confirms liveness without
  // flooding logs.
  #driftAuditLastHeartbeatMs = 0;
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
            //
            // Filter out internal control-plane queries (permissions /
            // clients / mutations). Go never registers a Source for those
            // tables (#currentTablesForGo skips them), so re-registering an
            // internal query during reset makes engine.AddQueries panic
            // "no source for table <appID>_<shard>.clients" → resetEngine
            // throws → the drift recovery cascades into client-connection
            // failures. Every other dispatch site already applies this
            // filter (see #goHydrate, addQueries, the drift-audit picker);
            // the reset re-register callback was the one that missed it.
            () =>
              Array.from(this.#pipelines.entries())
                .filter(
                  ([queryID, p]) =>
                    !this.#isInternalQueryID(queryID) &&
                    !this.#isInternalTable(p.transformedAst.table),
                )
                .map(([queryID, p]) => ({
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
      uniqueKeys?: string[][] | undefined;
      minRowVersion?: string | null | undefined;
      rows: Record<string, unknown>[];
    }
  > {
    const {db} = this.#snapshotter.current();
    const tables: Record<
      string,
      {
        columns: Record<string, {type: 'boolean' | 'number' | 'string' | 'null' | 'json'}>;
        primaryKey: string[];
        uniqueKeys?: string[][] | undefined;
        minRowVersion?: string | null | undefined;
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
      // uniqueKeys: forward all unique-index column sets to Go so its
      // scalar-subquery resolver can detect at-most-one-row subqueries
      // (the Phase 2 port of resolveSimpleScalarSubqueries). Falls back to
      // [primaryKey] when liteTableSpec didn't capture uniqueKeys, so the
      // Go resolver still has something useful for the common pk-only
      // case rather than treating the table as having no unique keys.
      const tableSpec = spec.tableSpec as unknown as {uniqueKeys?: string[][]};
      const uniqueKeys: string[][] =
        tableSpec.uniqueKeys && tableSpec.uniqueKeys.length > 0
          ? tableSpec.uniqueKeys.map(k => [...k])
          : [[...spec.tableSpec.primaryKey]];
      tables[name] = {
        columns,
        primaryKey: [...(this.#primaryKeys?.get(name) ?? spec.tableSpec.primaryKey)],
        uniqueKeys,
        // Forward minRowVersion so the Go streamer can bump emitted rows'
        // _0_version when below it (audit item K).
        minRowVersion: spec.tableSpec.minRowVersion,
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

  // Plans an AST through the same completeOrdering + cost-model planner pass
  // that buildPipeline applies internally for TS. Used to keep the AST sent
  // to Go in sync with the AST TS materializes against — without this the
  // planner's `flip: true` decorations (which route OR-with-CSQ through
  // FlippedJoin + UnionFanIn merge-with-dedup) never reach Go, so Go's plain
  // Join + applyOr path attaches the inner-CSQ relationship and the streamer
  // over-emits it (H18-cont).
  #planAstForGo(ast: AST): AST {
    const planned = completeOrdering(ast, tableName =>
      must(this.#getSource(tableName)).tableSchema.primaryKey,
    );
    if (!this.#costModels) {
      return planned;
    }
    const db = this.#snapshotter.current().db.db;
    const costModel = this.#ensureCostModelExistsIfEnabled(db);
    if (!costModel) {
      return planned;
    }
    return planQuery(planned, costModel);
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
    // If a per-CG recovery is in flight (drift, sidecar-restart, etc.),
    // wait for it to finish before deciding TS vs Go. Pre-fix the
    // dispatch checked initialized synchronously and fell through to
    // TS-native during the recovery window — that created a phantom
    // TS pipeline (rooted at a user-table TableSource) that persisted
    // beyond the recovery and emitted duplicate RowChanges on every
    // subsequent advance. The new query sees up to ~tens of ms extra
    // latency in exchange for routing to Go correctly.
    if (this.#goBackend) {
      return this.#goBackend
        .whenRecovered()
        .then(() =>
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
    // Internal queries (lmids, mutationResults, queries rooted at the
    // <appID>.permissions or <appID>_<shard>.clients tables) always
    // run via TS — their source tables are excluded from Go's data
    // path (Fix #1), and TS's TableSource over live SQLite self-heals
    // any state drift. Routes regardless of mode so Go-primary doesn't
    // break mutation acks (which depend on the lmids subscription).
    if (
      this.#isInternalQueryID(queryID) ||
      this.#isInternalTable(query.table)
    ) {
      return this.#trackRowSetSignatures(
        this.#addQueryImpl(transformationHash, queryID, query, timer),
      );
    }
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
    // Plan the AST the same way the batch path does so Go's pipeline
    // gets the planner's flip:true annotation and the side-effect of
    // creating TableSources in this.#tables (via #planAstForGo →
    // completeOrdering → #getSource). Without this, single-query
    // hydrate fed Go a raw AST → H18-class over-emit on OR-with-CSQ
    // shapes, and post-reconnect getRow() panicked because the
    // TableSource was never created. (Audit fix F.)
    const planned = this.#planAstForGo(query);
    const goResult = await this.#goBackend!.hydrate(queryID, planned);

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
      transformedAst: planned,
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

    // Single RPC for all queries. Plan the AST so Go gets the same
    // flip-aware AST TS materializes against (H18-cont).
    const batchResults = await this.#goBackend!.hydrateMany(
      queries.map(q => ({queryID: q.queryID, ast: this.#planAstForGo(q.ast)})),
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

    // Split internal queries (lmids, mutationResults, control-plane tables)
    // off the batch — they must run via TS's #addQueryImpl since their
    // source tables are excluded from Go's data path (the same invariant
    // enforced by #addQueryDispatch for the per-query path). Without this
    // split, the Go sidecar panics with `no source for table` on the
    // <appID>.clients / <appID>_<shard>.permissions queries that show up
    // in every first-time hydrate batch.
    const internalQueries: typeof queries = [];
    const userQueries: typeof queries = [];
    for (const q of queries) {
      if (this.#isInternalQueryID(q.queryID) || this.#isInternalTable(q.ast.table)) {
        internalQueries.push(q);
      } else {
        userQueries.push(q);
      }
    }

    // Run internal queries through TS first. Their results yield with the
    // same {queryID, changes} shape the Go side does, so the caller is
    // mode-agnostic.
    //
    // Yield-token caveat: #addQueryImpl emits 'yield' tokens for
    // cooperative time-slicing, but in Go-primary the view-syncer's
    // batch consumer never started the TimeSliceTimer — calling
    // `timer.yieldProcess()` on a 'yield' would trip `not running`
    // assert and crash the ViewSyncer. Internal queries are tiny
    // (lmids/mutationResults/clients/permissions are O(rows = active
    // connections)), so dropping the cooperative yields here is safe
    // and contained.
    const noopTimer = {
      elapsedLap: () => 0,
      totalElapsed: () => 0,
      running: () => true,
    } as unknown as Timer;
    for (const q of internalQueries) {
      const raw = this.#trackRowSetSignatures(
        this.#addQueryImpl(q.transformationHash, q.queryID, q.ast, noopTimer),
      );
      function* dropYields(): Iterable<RowChange | 'yield'> {
        for (const c of raw) {
          if (c !== 'yield') yield c;
        }
      }
      yield {queryID: q.queryID, changes: dropYields()};
    }

    if (userQueries.length === 0) {
      return;
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
    for (const q of userQueries) byQueryID.set(q.queryID, q);

    const rpcPromise = this.#goBackend!.hydrateManyStream(
      userQueries.map(q => ({queryID: q.queryID, ast: this.#planAstForGo(q.ast)})),
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
        // No 'yield' tokens in Go-primary batch hydrate: the view-syncer
        // batch consumer path never starts the TimeSliceTimer, so a
        // 'yield' would trip `not running` in TimeSliceTimer.#stopLap
        // and tear down the ViewSyncer. The 'yield' tokens existed for
        // cooperative scheduling against the timer; the batch path
        // doesn't need them (rows are already chunked at the Go side via
        // hydrateChunkSize, so we never accumulate enough in one
        // generator to starve the event loop).
        function* yieldGoHydration(): Iterable<RowChange | 'yield'> {
          for (const rc of changesArr) {
            yield self.#goRowChangeToRowChange(rc);
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
      // Phase 2: send the ORIGINAL AST to Go. Go's own scalar-subquery
      // resolver (go-ivm/builder/resolve_scalar.go) walks the AST,
      // identifies subqueries whose WHERE fully covers a unique key,
      // executes them against the MemorySource, and replaces the EXISTS
      // with a literal — same algorithm as TS's resolveSimpleScalarSubqueries
      // line-for-line. It also builds a companion sub-pipeline per resolved
      // scalar that holds a live Connection to the subquery source, so on
      // advance the companion's source.Push fans out to it and emits the
      // child row deltas under the parent queryID. That's what the Phase
      // 1.5 stopgap was doing manually; Phase 2 makes Go do it natively
      // so shadow comparator sees identical output on both sides without
      // injection.
      //
      // Use the streaming variant so shadow mode exercises the same code
      // path Go-primary mode will use in production. Compare per-query as
      // soon as Go emits each result (REVIEW-final perf-opt streaming
      // validation in shadow).
      const goResultsByID = new Map<string, RowChange[]>();
      let mismatches = 0;
      await this.#goBackend.hydrateManyStream(
        queries.map(q => ({queryID: q.queryID, ast: this.#planAstForGo(q.ast)})),
        r => {
          const goChanges = (r.changes ?? []).map(rc =>
            this.#goRowChangeToRowChange(rc as GoRowChange),
          );
          // Phase 2: no TS-side companion injection here. Go's resolver
          // emits companion rows itself if any scalar subquery resolved.
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
  //
  // Open issue (2026-05-26 multi-CG JWT soak — pickup TODO):
  //   Rare false-positive drift on paginated `conversations` queries with
  //   `EXISTS(channels)` + `start.exclusive: false` + cursor row whose
  //   createdAt EXACTLY matches `start.row.createdAt`. The audit reports
  //   ts=N go=N MISMATCH; the [drift-audit][rowdiff] log shows TS missing
  //   exactly the boundary row and including one extra older row. Direct
  //   `SELECT … WHERE createdAt <= cursor ORDER BY createdAt DESC LIMIT N`
  //   on the replica.db returns the boundary row at position 0, so the
  //   bug is between SQL and the audit's RowChange emission — NOT in
  //   Go's MemorySource (Go gets the boundary right). Ruled out:
  //     - Resolver asymmetry (drift fires whether scalar EXISTS resolved
  //       or not — confirmed by [scalar-resolver] log instrumentation)
  //     - View-syncer instance restart (none of the drifting CGs had
  //       multiple instances)
  //     - createdAt ties (verified zero ties at cursor for each repro)
  //     - Snapshot skew (versionBefore === versionAfter on every repro)
  //   Suspect: Skip → Exists → Take interaction at the audit's
  //   re-hydrate path, possibly companion-pipeline state from the
  //   executor in #resolveScalarSubqueries holding a stale Connection
  //   on the conversations TableSource. Repro via 5-CG × 360s JWT soak
  //   + drive-traffic.sh; fires ~1–2% of audit runs. The
  //   [drift-audit][rowdiff] log line points to the divergent row.
  async #runDriftAudit(): Promise<void> {
    // Unconditional tick counter — fires before any guards so operators
    // can distinguish "audit timer never fired" (broken sidecar config,
    // missed setInterval, etc.) from "audit fired but skipped" (legitimate
    // busy / idle CG). Without this, an audit that's silently disabled
    // looks identical in metrics to a healthy one running against an idle
    // CG (both stuck at runs=0). The prod incident on the morning of
    // 2026-06-01 was exactly this gap: the Go sidecar refused to start,
    // drift-audit hadn't run for hours, and nothing in INFO logs surfaced
    // it. Increment this BEFORE the InFlight guard so even
    // back-to-back-fire scenarios show ticks > runs.
    this.#driftAuditTicks.add(1);

    if (this.#driftAuditInFlight) {
      this.#driftAuditSkips.add(1);
      return;
    }
    this.#driftAuditInFlight = true;
    try {
      if (!this.initialized()) return;
      if (!this.#goBackend?.initialized) return;

      // Cross-validate Go's pipeline state against TS's expected count.
      // C2 (per-CG recovery missing query re-registration) is now fixed,
      // but this probe is defense-in-depth: if any future regression or
      // unanticipated code path leaves Go with fewer pipelines than TS
      // thinks are registered, advances would silently return empty and
      // the client view would freeze. The audit's normal hydrate path
      // CANNOT detect this — it registers a fresh auditID query in Go
      // that succeeds in isolation, so set comparison shows match. The
      // count probe is independent of any audit-registered query.
      //
      // Self-heal on detection: kick off resetEngine asynchronously
      // (don't block this audit cycle) — it rebuilds from current
      // tables AND re-registers queries via the centralized recovery
      // helper. The audit itself returns without comparing for this
      // cycle since the state is known-divergent.
      let goCount: number;
      try {
        goCount = await this.#goBackend.pipelineCount();
      } catch (e) {
        this.#lc.debug?.(
          `[drift-audit] pipelineCount probe failed (continuing): ${String(e)}`,
        );
        goCount = -1; // unknown; skip the cross-check this cycle
      }
      const tsExpected = [...this.#pipelines.keys()].filter(
        qid => !this.#isInternalQueryID(qid),
      ).length;
      if (goCount >= 0 && goCount < tsExpected) {
        this.#driftAuditFreezes.add(1);
        this.#lc.error?.(
          `[drift-audit] FREEZE detected: TS=${tsExpected} queries registered, ` +
            `Go=${goCount} pipelines. Per-CG recovery dropped pipeline state. ` +
            `Triggering resetEngine to self-heal.`,
        );
        // Fire-and-forget. resetEngine handles its own concurrency via
        // the gate; if it's already running, this is a no-op via the
        // gate await in #reinitPerCGAndRegisterQueries.
        void this.#scheduleGoReset('drift-audit-pipeline-count-mismatch');
        return;
      }
      // #addQueryImpl asserts #advanceContext===null, and reusing #streamer
      // mid-advance would corrupt the in-flight diff. Wait for the next tick.
      if (this.#advanceContext !== null) {
        this.#driftAuditSkips.add(1);
        return;
      }
      if (this.#pipelines.size === 0) return;

      // Internal queries (lmids/mutationResults) and queries rooted at
      // internal tables can't be audited against Go — those tables are
      // excluded from Go's data path (Fix #1), so sending the AST to
      // Go's `hydrateManyStream` would panic with `no source for table`
      // and crash the sidecar under load. Filter them out of the audit
      // target pool entirely; if nothing else is hydrated this cycle,
      // skip the audit.
      const auditableIDs = [...this.#pipelines.entries()]
        .filter(([qid, entry]) =>
          !this.#isInternalQueryID(qid) &&
          !this.#isInternalTable(entry.transformedAst.table),
        )
        .map(([qid]) => qid);
      if (auditableIDs.length === 0) return;

      const targetID = auditableIDs[Math.floor(Math.random() * auditableIDs.length)];
      const entry = this.#pipelines.get(targetID);
      if (!entry) return;
      // transformedAst is post-subquery-resolution — what Go was originally
      // given. Reusing it sidesteps re-resolving against a fresher snapshot.
      const ast = entry.transformedAst;
      const transformationHash = entry.transformationHash;

      const auditID = `__drift_audit_${Date.now().toString(36)}_${Math.floor(
        Math.random() * 0xffff_ffff,
      ).toString(36)}`;
      const noopTimer = {
        elapsedLap: () => 0,
        totalElapsed: () => 0,
        running: () => true, // TableSource.#shouldYield calls this at runtime; exported Timer type omits it
      } as unknown as Timer;

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
          true, // auditMode — no-op setOutput; see #addQueryImpl
        )) {
          if (c !== 'yield') tsChanges.push(c);
        }
      } catch (e) {
        this.#lc.warn?.(
          `[drift-audit] TS hydrate failed for ${targetID}: ${String(e)}`,
        );
        return;
      }

      // Snapshot alignment: roll the Go leaf's pinned read tx (Phase 4) so
      // its hydrate reads at the same WAL frame the TS audit / SQL ground-
      // truth comparator will read. Without this, Go's tx stays pinned to
      // the last Push's snapshot and during sustained writes the audit
      // compares stale-vs-current frames — surfaces as transient
      // go-vs-sql set differences that aren't real drift.
      //
      // Best-effort: a failure here (e.g., sidecar restart mid-audit) is
      // caught by the existing epoch/version checks below. No-op on
      // MemorySource backends so this is safe to call unconditionally.
      try {
        await this.#goBackend.refreshSnapshot();
      } catch (e) {
        this.#lc.debug?.(
          `[drift-audit] refreshSnapshot failed (continuing): ${String(e)}`,
        );
      }

      const goChanges: RowChange[] = [];
      try {
        await this.#goBackend.hydrateManyStream(
          [{queryID: auditID, ast: this.#planAstForGo(ast)}],
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
        // Diagnostic: dump the symmetric difference of rowKeys so we can
        // see exactly which row(s) each side has that the other doesn't.
        // Avoids the indirection through #shadowCompare's sort positions.
        const tsOnly: string[] = [];
        const goOnly: string[] = [];
        for (const k of tsKeys) if (!goKeys.has(k)) tsOnly.push(k);
        for (const k of goKeys) if (!tsKeys.has(k)) goOnly.push(k);
        this.#lc.error?.(
          `[drift-audit][rowdiff] queryID=${targetID} ` +
            `ts_only=${tsOnly.slice(0, 5).join(' | ')} (${tsOnly.length} total) ` +
            `go_only=${goOnly.slice(0, 5).join(' | ')} (${goOnly.length} total)`,
        );
      }

      this.#shadowCompare('drift-audit', targetID, tsRemapped, goRemapped);

      // Third opinion: run raw SQL on the snapshot's SQLite replica as
      // ground truth and compare against Go's main-table rows. Only
      // mark MISMATCH when Go disagrees with SQL — that's the real
      // drift signal. Go-vs-TS-audit divergence with SQL agreeing
      // with Go is a TS-audit-only bug (Bug #3) — demote to info.
      const sqlVerdict = this.#sqlGroundTruthCompare(ast, goRemapped);

      if (sqlVerdict.kind === 'go-vs-sql-drift') {
        // IVM boundary-semantics exception: paginated queries (limit +
        // start cursor) define their boundary row at result position 0
        // by IVM convention. Both Go IVM and TS IVM apply the cursor
        // through the Take operator's input stream, which can include
        // the cursor row PLUS LIMIT more (so go_count = sql_count + 1
        // is expected on a forward-paginated query whose cursor lands
        // on a real row). If TS IVM and Go IVM agree but SQL disagrees,
        // SQL is the outlier — same Bug #3 class as ts-audit-only,
        // demote to info instead of flagging REAL DRIFT.
        //
        // Two boundary shapes — pre-fix the classifier only matched the
        // first, sending the second to "REAL DRIFT" and paging on-call
        // for what was a known-benign semantic difference. Reproduced in
        // prod 2026-06-01 with ts=51 go=51 sql=50 (asymmetric, goOnly=1,
        // sqlOnly=0).
        //
        //   1. Asymmetric: IVM includes the cursor row, SQL excludes it
        //      (or vice versa). One side has exactly 1 extra unique row.
        //   2. Symmetric: IVM and SQL both have the cursor position
        //      filled but with different rows (e.g., a row mutation
        //      crossed the cursor). One row on each side disagrees.
        const goOnly = sqlVerdict.goOnly.length;
        const sqlOnly = sqlVerdict.sqlOnly.length;
        const asymmetricBoundary = goOnly + sqlOnly === 1;
        const symmetricBoundary = goOnly === 1 && sqlOnly === 1;
        if (
          !setDiffers &&
          ast.limit !== undefined &&
          ast.start !== undefined &&
          (asymmetricBoundary || symmetricBoundary)
        ) {
          this.#lc.info?.(
            `[drift-audit] ${targetID}: ivm-boundary divergence ` +
              `(TS IVM and Go IVM agree, SQL alone disagrees by ${goOnly + sqlOnly} on ` +
              `limit+cursor query — pagination boundary semantics, ` +
              `${asymmetricBoundary ? 'asymmetric' : 'symmetric'}). ` +
              `ts=${tsRemapped.length} go=${goRemapped.length} sql=${sqlVerdict.sqlCount}`,
          );
        } else {
          this.#driftAuditMismatches.add(1);
          this.#lc.error?.(
            `[drift-audit] ${targetID}: REAL DRIFT — Go disagrees with SQL (set). ` +
              `go_count=${goRemapped.length} sql_count=${sqlVerdict.sqlCount} ` +
              `go_only=${sqlVerdict.goOnly.slice(0, 3).join(' | ')} ` +
              `(${sqlVerdict.goOnly.length} total) ` +
              `sql_only=${sqlVerdict.sqlOnly.slice(0, 3).join(' | ')} ` +
              `(${sqlVerdict.sqlOnly.length} total)`,
          );
        }
      } else if (sqlVerdict.kind === 'go-vs-sql-content-drift') {
        // Same PKs but row contents differ — Go has stale/wrong values
        // for one or more rows. This is the most insidious bug class
        // (users see wrong data without any visible error), so escalate.
        this.#driftAuditMismatches.add(1);
        const sample = sqlVerdict.contentMismatches[0];
        this.#lc.error?.(
          `[drift-audit] ${targetID}: REAL DRIFT — Go disagrees with SQL (content). ` +
            `sql_count=${sqlVerdict.sqlCount} ` +
            `mismatched_rows=${sqlVerdict.contentMismatches.length} ` +
            `first_pk=${sample.pk} ` +
            `sql_row=${sample.sqlRow.slice(0, 300)} ` +
            `go_row=${sample.goRow.slice(0, 300)}`,
        );
      } else if (setDiffers && sqlVerdict.kind === 'confirmed') {
        // TS-audit disagrees but Go matches the SQL ground truth.
        // Known TS-audit bug (Bug #3, see #runDriftAudit doc) — info-level only.
        this.#lc.info?.(
          `[drift-audit] ${targetID}: ts-audit-only divergence (Go matches SQL). ` +
            `ts=${tsRemapped.length} go=${goRemapped.length} sql=${sqlVerdict.sqlCount} — see [rowdiff]`,
        );
      } else if (sqlVerdict.kind === 'skipped') {
        // Couldn't build SQL (e.g. AST has unresolvable subqueries) —
        // fall back to original count-based signal.
        if (setDiffers) {
          this.#driftAuditMismatches.add(1);
          this.#lc.error?.(
            `[drift-audit] ${targetID}: ts=${tsRemapped.length} go=${goRemapped.length} ` +
              `MISMATCH (sql-truth unavailable: ${sqlVerdict.reason})`,
          );
        } else {
          this.#lc.debug?.(
            `[drift-audit] ${targetID}: ts=${tsRemapped.length} go=${goRemapped.length} ok (sql skipped: ${sqlVerdict.reason})`,
          );
          this.#emitDriftAuditHeartbeat('sql-skipped');
        }
      } else {
        this.#lc.debug?.(
          `[drift-audit] ${targetID}: ts=${tsRemapped.length} go=${goRemapped.length} ok (sql confirmed)`,
        );
        this.#emitDriftAuditHeartbeat('sql-confirmed');
      }
    } finally {
      this.#driftAuditInFlight = false;
    }
  }

  /**
   * Emit an INFO heartbeat once per {@link DRIFT_AUDIT_HEARTBEAT_MS} per
   * driver after a successful audit run. Reason field describes which OK
   * path landed (sql-confirmed / sql-skipped / count-only) so operators
   * can confirm the ground-truth comparator is active. No-op until enough
   * time has passed since the previous heartbeat.
   */
  #emitDriftAuditHeartbeat(reason: string): void {
    const now = Date.now();
    if (now - this.#driftAuditLastHeartbeatMs < DRIFT_AUDIT_HEARTBEAT_MS) {
      return;
    }
    this.#driftAuditLastHeartbeatMs = now;
    // cgID is on this.#lc's context already (constructor's lc.withContext),
    // so JSON-format loggers emit it as a field. Inline cgID would duplicate.
    this.#lc.info?.(`[drift-audit] heartbeat OK (reason=${reason})`);
  }

  /**
   * SQL ground-truth comparator: builds a flat SELECT from the AST's main
   * filter + cursor + orderBy + limit, runs it against the snapshot's
   * SQLite replica, and compares the resulting rowKey set against Go's
   * main-table changes. Returns one of:
   *   - `confirmed`: Go matches SQL exactly → real source of truth agrees
   *   - `go-matches-sql`: Go matches SQL but caller sees a divergent
   *     TS-audit output → TS-audit-only bug (Bug #3 noise)
   *   - `go-vs-sql-drift`: Go disagrees with SQL → REAL Go-side drift
   *   - `skipped`: AST has constructs we can't SQL-ify (unresolved
   *     correlatedSubquery in WHERE, etc.) — falls back to the legacy
   *     ts-vs-go count comparison
   */
  #sqlGroundTruthCompare(
    ast: AST,
    goRemapped: RowChange[],
  ):
    | {kind: 'confirmed'; sqlCount: number}
    | {kind: 'go-vs-sql-drift'; sqlCount: number; goOnly: string[]; sqlOnly: string[]}
    | {
        kind: 'go-vs-sql-content-drift';
        sqlCount: number;
        contentMismatches: {pk: string; sqlRow: string; goRow: string}[];
      }
    | {kind: 'skipped'; reason: string} {
    // Operator opt-out: when goSidecar.driftAuditSqlGroundTruth is false,
    // skip the SQL re-query entirely. The caller's skip-handling path then
    // falls back to the TS-vs-Go set comparison as the only drift signal.
    if (!goDriftAuditSqlGroundTruth(this.#config)) {
      return {kind: 'skipped', reason: 'disabled'};
    }
    // The caller passes entry.transformedAst which is ALREADY post-resolution.
    // Re-running #resolveScalarSubqueries here is redundant AND trips
    // `shouldYield called outside of hydration` because the executor's
    // buildPipeline fetches from a TableSource outside any hydrate context.
    // buildAuditSQL handles correlatedSubqueries natively, so we don't need
    // resolution either way.
    const resolved = ast;
    const spec = this.#tableSpecs.get(resolved.table);
    if (!spec) return {kind: 'skipped', reason: 'no-table-spec'};

    // Walk AST → flat SQL string with `?` placeholders. Handles correlated
    // subqueries (EXISTS / NOT EXISTS) as nested SELECT 1 with correlation
    // expressed via outer-alias qualified columns. Hand-rolled because
    // zqlite's buildSelectQuery rejects correlatedSubquery in NoSubqueryCondition.
    let limitedText: string;
    let values: unknown[];
    try {
      const result = buildAuditSQL(resolved, this.#tableSpecs);
      limitedText = result.text;
      values = result.values;
    } catch (e) {
      return {kind: 'skipped', reason: `build-sql-failed: ${String(e)}`};
    }

    let sqlRows: Record<string, unknown>[];
    try {
      sqlRows = this.#snapshotter
        .current()
        .db.db.prepare(limitedText)
        .all(...values) as Record<string, unknown>[];
    } catch (e) {
      return {kind: 'skipped', reason: `sql-exec-failed: ${String(e)}`};
    }

    // Normalize SQL rows (BigInt→Number for INTEGER columns, JSON parse,
    // boolean coerce) so they match Go's row encoding shape.
    let normalizedSqlRows: Record<string, unknown>[];
    try {
      normalizedSqlRows = sqlRows.map(
        row => fromSQLiteTypes(spec.zqlSpec, row as Row, resolved.table) as Record<string, unknown>,
      );
    } catch (e) {
      return {kind: 'skipped', reason: `normalize-failed: ${String(e)}`};
    }

    // Build PK→full-row maps so we can do both set diff AND content diff.
    const pk = spec.tableSpec.primaryKey;
    const pkOf = (row: Record<string, unknown>): string => {
      const rowKey: Record<string, unknown> = {};
      for (const col of pk) rowKey[col] = row[col];
      return stableStringify(rowKey);
    };

    const sqlByPK = new Map<string, Record<string, unknown>>();
    for (const row of normalizedSqlRows) sqlByPK.set(pkOf(row), row);

    const goByPK = new Map<string, Record<string, unknown>>();
    for (const c of goRemapped) {
      if (c.table === resolved.table) {
        goByPK.set(stableStringify(c.rowKey), c.row);
      }
    }

    // Step 1: set diff (PK presence)
    const goOnly: string[] = [];
    const sqlOnly: string[] = [];
    for (const k of goByPK.keys()) if (!sqlByPK.has(k)) goOnly.push(k);
    for (const k of sqlByPK.keys()) if (!goByPK.has(k)) sqlOnly.push(k);

    if (goOnly.length > 0 || sqlOnly.length > 0) {
      return {kind: 'go-vs-sql-drift', sqlCount: sqlByPK.size, goOnly, sqlOnly};
    }

    // Step 2: content diff for rows present on both sides
    const contentMismatches: {pk: string; sqlRow: string; goRow: string}[] = [];
    for (const [pkKey, sqlRow] of sqlByPK) {
      const goRow = goByPK.get(pkKey);
      if (!goRow) continue;
      // Project Go's row to the schema columns only — Go may carry extra
      // bookkeeping fields (e.g., _0_version) that aren't in the zql spec.
      const goRowProjected: Record<string, unknown> = {};
      for (const col of Object.keys(spec.zqlSpec)) goRowProjected[col] = goRow[col];
      const sqlStr = stableStringify(sqlRow);
      const goStr = stableStringify(goRowProjected);
      if (sqlStr !== goStr) {
        contentMismatches.push({pk: pkKey, sqlRow: sqlStr, goRow: goStr});
      }
    }

    if (contentMismatches.length > 0) {
      return {
        kind: 'go-vs-sql-content-drift',
        sqlCount: sqlByPK.size,
        contentMismatches,
      };
    }

    return {kind: 'confirmed', sqlCount: sqlByPK.size};
  }

  *#addQueryImpl(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
    /**
     * When true, the pipeline's setOutput is a no-op — source pushes still
     * flow through the operator chain (correctness preserved), but nothing
     * is written to the production #streamer. Used by the drift audit so
     * its transient audit pipeline cannot leak RowChanges tagged with the
     * synthetic auditID into a concurrent advance's output. Pre-fix, an
     * advance landing during the audit's `await goBackend.hydrateManyStream`
     * would fan out source changes to ALL connected pipelines including
     * the audit's, whose setOutput wrote to #streamer with queryID=auditID
     * — those changes either silently dropped at the view-syncer or
     * surfaced as phantom rows in the CVR patch.
     */
    auditMode = false,
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
      if (auditMode) {
        // Audit-mode setOutput: source pushes still traverse the
        // operator chain (some operators have side-effects on push
        // that the next fetch depends on), but the change is dropped
        // at this terminal sink instead of leaking into #streamer
        // under the synthetic auditID.
        input.setOutput({
          push: () => [],
        });
      } else {
        input.setOutput({
          push: change => {
            const streamer = this.#streamer;
            assert(streamer, 'must #startAccumulating() before pushing changes');
            streamer.accumulate(queryID, schema, [change]);
            return [];
          },
        });
      }

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

      // Note: hydrationTimeMs is a wall-clock measurement taken across the
      // (time-sliced, yield-punctuated) hydrate, so it can overestimate pure
      // processing time. It is the value stored on the pipeline and used by
      // the adaptive hydrate circuit-breaker math and the slow-hydrate
      // warning; there is no separate precise-reset pass.
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
    // Include the table name in the error message so the bare-must() failure
    // surfaced by 14-CG soak (4 CGs hit "Unexpected undefined value" during
    // CVR catchup → ViewSyncer's outer must wraps with "Missing row ..." but
    // the inner must fires first and obscures which table) tells us which
    // table source went missing. Suspect: query removed mid-CVR-catchup
    // while view-syncer still has a refCount for one of its rows.
    const source = must(
      this.#tables.get(table),
      `pipelineDriver.getRow: no TableSource for table=${table} ` +
        `(pk=${JSON.stringify(pk)}). Known tables: ${[...this.#tables.keys()].join(',')}`,
    );
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

    // Go-primary: dual-run TS + Go on disjoint table sets. Go gets the
    // diff with internal tables filtered out and handles user queries.
    // TS handles internal queries (lmids, mutationResults) via its real
    // #addQueryImpl pipelines — those pipelines were registered through
    // the internal-query branch of #addQueryDispatch. User-query
    // pipelines in TS are stubs (no setOutput callback), so TS's
    // #advance walks the full diff but only emits for internal queries.
    // Merging is safe because the two sets are table-disjoint.
    if (this.#goBackend?.initialized) {
      return this.#goPrimaryAdvance(diff, timer, curr.version, changes);
    }

    return {
      version: curr.version,
      numChanges: changes,
      changes: this.#trackRowSetSignatures(this.#advance(diff, timer, changes)),
    };
  }

  /**
   * Go-primary advance: runs Go's advanceStream for user queries and
   * TS's #advance for internal queries in parallel, then concatenates
   * the results. Same internal-table filter as #shadowAdvance so the
   * Go side never sees control-plane diffs.
   */
  async #goPrimaryAdvance(
    diff: SnapshotDiff,
    timer: Timer,
    version: string,
    numChanges: number,
  ): Promise<AdvanceResult> {
    // Buffer the diff so both TS and Go can consume it. Filter the Go
    // side to drop internal tables (Fix #1 invariant — those rows go
    // through TS's TableSource which self-heals against live SQLite).
    const buffered: Array<{
      table: string;
      prevValues: Readonly<Row>[];
      nextValue: Readonly<Row> | null;
      rowKey: RowKey;
    }> = [];
    const snapshotChanges: SnapshotChange[] = [];
    for (const entry of diff) {
      buffered.push(entry);
      if (this.#isInternalTable(entry.table)) continue;
      snapshotChanges.push({
        table: entry.table,
        prevValues: entry.prevValues as Record<string, unknown>[],
        nextValue: entry.nextValue as Record<string, unknown> | null,
      });
    }
    const replayDiff: SnapshotDiff = {
      prev: diff.prev,
      curr: diff.curr,
      changes: diff.changes,
      [Symbol.iterator]: () => buffered[Symbol.iterator](),
    };

    // Kick Go RPC in flight while TS does its work.
    //
    // Error classification — pre-fix this catch was a silent swallow of
    // ALL errors, indistinguishable from a legitimately-empty advance.
    // We now bucket by error class so dashboards can detect each
    // failure mode and operators can take the right action:
    //
    //   1. Protocol violation ("chunk order violation", "finished
    //      without a final chunk", decode failures): wire-level bug or
    //      data corruption. Can't trust Go's state. Re-throw so the
    //      caller's error path surfaces it instead of silently committing
    //      an empty CVR diff. Hard escalation — a recovery reset won't
    //      fix a wire bug.
    //
    //   2. StaleInitEpochError: this view-syncer instance was torn
    //      down and a successor's epoch took over. Continuing to retry
    //      against the stale instance is futile (-32101 will fire on
    //      every call). Drop the advance and signal upstream caller via
    //      throw so the instance lifecycle teardown completes.
    //
    //   3. Sidecar unavailable / restart-related: drop this advance
    //      (the "miss exactly one delta" contract). The recovery
    //      machinery in #scheduleGoReset → #reinitPerCGAndRegisterQueries
    //      will rebuild Go's state from the next valid advance onward.
    //
    //   4. Other unclassified: log + drop + reset (preserves the old
    //      best-effort behavior) but COUNT separately so we can tell
    //      from metrics whether we're seeing many of these (signal of
    //      an error class we should explicitly handle).
    const goPromise = this.#goBackend!.advanceStream(snapshotChanges)
      .then(r => {
        // Record Go-side per-(table,op) advance timings into the same
        // #advanceTime histogram the TS-native path populates
        // (pipeline-driver.ts:2914), so Go-primary has table/op timing
        // attribution parity instead of dropping r.timings on the floor.
        if (r.timings) {
          for (const t of r.timings) {
            this.#advanceTime.recordMs(t.ms, {
              table: t.table,
              type: t.type === 0 ? 'add' : t.type === 1 ? 'remove' : 'edit',
            });
          }
        }
        return r.changes.map(rc => this.#goRowChangeToRowChange(rc));
      })
      .catch(e => {
        const msg = e instanceof Error ? e.message : String(e);

        if (
          msg.includes('chunk order violation') ||
          msg.includes('finished without a final chunk') ||
          msg.includes('Frame too large') ||
          msg.includes('protocolRev mismatch')
        ) {
          this.#advanceDroppedProtocol.add(1);
          this.#lc.error?.(
            `[go-primary] Go advance failed with PROTOCOL VIOLATION (escalating): ${msg}`,
          );
          // Hard escalation: re-throw. The caller's error path will
          // surface this instead of letting an empty diff commit. A
          // protocol violation means we cannot trust Go's state OR the
          // wire — recovery via resetEngine won't fix it without a
          // sidecar process restart.
          throw e;
        }

        if (e instanceof StaleInitEpochError) {
          this.#advanceDroppedStaleEpoch.add(1);
          this.#lc.warn?.(
            `[go-primary] Go advance rejected by sidecar (stale initEpoch); ` +
              `this view-syncer instance is torn down: ${msg}`,
          );
          throw e;
        }

        const sidecarUnavailable =
          msg.includes('Sidecar is not running') ||
          msg.includes('Connection closed') ||
          msg.includes('engine not initialized');
        if (sidecarUnavailable) {
          this.#advanceDroppedSidecar.add(1);
          this.#lc.warn?.(
            `[go-primary] Go advance dropped (sidecar restart in flight): ${msg}`,
          );
          this.#scheduleGoReset('go-primary-advance-sidecar-restart');
          return [] as RowChange[];
        }

        this.#advanceDroppedOther.add(1);
        this.#lc.error?.(`[go-primary] Go advance failed (unclassified): ${msg}`);
        this.#scheduleGoReset('go-primary-advance-failure');
        return [] as RowChange[];
      });

    // Run TS's #advance with the full diff. Only internal-query pipelines
    // are connected, so user-table pushes are no-ops on TS — the iterator
    // emits nothing for those, only events for internal queries.
    const tsChanges: RowChange[] = [];
    for (const change of this.#advance(replayDiff, timer, numChanges, true)) {
      if (change === 'yield') {
        await new Promise<void>(resolve => setImmediate(resolve));
      } else {
        tsChanges.push(change);
      }
    }

    const goResults = await goPromise;

    // Merge: TS internal-query events + Go user-query events. The two
    // sets are table-disjoint by construction (internal tables filtered
    // out of Go; user tables have stub pipelines in TS that don't emit).
    function* yieldMerged(): Iterable<RowChange | 'yield'> {
      for (const c of tsChanges) yield c;
      for (const c of goResults) yield c;
    }

    return {
      version,
      numChanges,
      changes: this.#trackRowSetSignatures(yieldMerged()),
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
    let row: Row | undefined;
    if (type === ChangeType.REMOVE) {
      row = undefined;
    } else if (rc.row == null) {
      // ADD/EDIT MUST carry a full row — Go's streamNodes sets rc.Row =
      // node.Row for every non-REMOVE change. A missing row here is a
      // Go-side wire/serialization bug. Previously we silently substituted
      // rc.rowKey, shipping a PK-only row that corrupts the client view
      // invisibly. Surface it at error level (the rowKey fallback is kept
      // only so one wire glitch doesn't crash the merge), so the bug is
      // diagnosable instead of hidden.
      this.#lc.error?.(
        `[go-primary] ${type === ChangeType.ADD ? 'ADD' : 'EDIT'} RowChange ` +
          `missing row for ${rc.table} ${JSON.stringify(rc.rowKey)} — Go wire ` +
          `bug; falling back to rowKey (PK-only row)`,
      );
      row = rc.rowKey as Row;
    } else {
      row = rc.row as Row;
    }
    return {
      type,
      queryID: rc.queryID,
      table: rc.table,
      rowKey: rc.rowKey as Row,
      row,
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
    // Record EVERY caller (even ones we coalesce with #goResetDirty) so the
    // metric reflects the real trigger rate, not just the post-dedup
    // executed-resets count. Dashboard queries that want executed count can
    // sum minus dirty-coalesced count separately.
    this.#goResetScheduled.add(1, {reason});
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
        // Per-(queryID,table) Go advance breakdown. Demoted to debug —
        // useful when chasing a divergence, noise at error in steady-state.
        if (this.#lc.debug) {
          const goBreakdown: Record<string, number> = {};
          const tableBreakdown: Record<string, number> = {};
          for (const rc of goRaw.changes) {
            const k = `${rc.queryID}/${rc.table}`;
            goBreakdown[k] = (goBreakdown[k] ?? 0) + 1;
            tableBreakdown[rc.table] = (tableBreakdown[rc.table] ?? 0) + 1;
          }
          this.#lc.debug?.(
            `[go-advance-out] diff=${snapshotChanges.length} ` +
              `go-out=${goRaw.changes.length} ` +
              `by-table=${JSON.stringify(tableBreakdown)}`,
          );
        }
        return {
          results: goRaw.changes.map(rc => this.#goRowChangeToRowChange(rc)),
          ms: performance.now() - goStart,
        };
      } catch (e) {
        this.#lc.error?.(`[shadow] Go advance failed: ${e}`);
        this.#scheduleGoReset('shadow-advance-failure');
        // If the failure was a DriftError carrying partial RowChanges from
        // Pushes that completed before the panic, surface them to the
        // shadow comparator so its diff against TS reflects the actual
        // mid-advance divergence (which row/op caused the drift) instead
        // of "TS=N rows, Go=0 rows" (every row of the drift'd advance
        // appearing as a Go-side miss). Matches the post-drift partial-
        // emit semantics now shipped end-to-end (Go engine.Advance →
        // RPC drift data → DriftError.partialChanges).
        if (e instanceof DriftError && e.partialChanges.length > 0) {
          return {
            results: e.partialChanges.map((rc: GoRowChange) => this.#goRowChangeToRowChange(rc)),
            ms: performance.now() - goStart,
          };
        }
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

    // Phase 2: no TS-side companion injection at advance time either.
    // Go's companion sub-pipelines (built by buildAndRegisterLocked in
    // go-ivm/engine/engine.go) hold live Connections to the scalar
    // subquery's source. When the diff has a change on that source,
    // source.Push fans out to the companion's Connection and emits the
    // change under the PARENT queryID via the wired pipelineOutput — same
    // observable behavior as TS's CompanionPipeline (pipeline-driver.ts
    // line 1254+) without manual reconciliation.
    //
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
      // If the advance loop threw (ResetPipelinesSignal or unexpected
      // error in #push), the success-path setDB + version update above
      // never ran — TableSources stay bound to the old snapshot while
      // the snapshotter has already moved forward. The drift audit's
      // first guard (#tableSourcesVersion !== snapshotter.version)
      // then skips EVERY subsequent cycle silently until the next
      // successful advance happens to land. Without this finally that
      // could be hours of effectively-disabled audit, with the metric
      // counter idling at zero — operators would think the audit is
      // healthy when it's actually offline.
      //
      // Realign now: bind TableSources to the current snapshot and
      // sync the version field. The advance's diff wasn't fully
      // applied, but the post-advance state is still the snapshotter's
      // current snapshot — TableSources reading at that frame is
      // correct (their next fetch sees the same point-in-time the
      // snapshotter exposes). The caller's restart machinery is
      // responsible for rebuilding any operator state that depends
      // on the dropped diff.
      const {curr} = diff;
      if (this.#tableSourcesVersion !== curr.version) {
        for (const table of this.#tables.values()) {
          table.setDB(curr.db.db);
        }
        this.#tableSourcesVersion = curr.version;
      }
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
