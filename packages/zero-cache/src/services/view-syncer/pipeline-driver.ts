import type {LogContext} from '@rocicorp/logger';
import {mkdir, writeFile} from 'node:fs/promises';
import {join} from 'node:path';
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
  goDivergenceCaptureDir,  isGoDerivedDiff,
  isGoAdvanceDrive,
  isGoPrimaryTrigger,
  isGoLeanPrimary,
  goNapiRowMode,
  goDriftAuditIntervalMs,
  goDriftAuditSqlGroundTruth,
} from './go-sidecar/go-compute-backend.ts';
import {min as minLexiVersion} from '../../types/lexi-version.ts';
import {isEnum as isLiteEnum, isArray} from '../../types/lite.ts';
import type {SidecarManager} from './go-sidecar/sidecar-manager.ts';
import type {
  SnapshotChange,
  RowChange as GoRowChange,
  AdvanceToHeadResult,
  TableTiming,
} from './go-sidecar/go-ivm-client.ts';
import {
  DriftError,
  PermanentDataError,
  StaleInitEpochError,
} from './go-sidecar/go-ivm-client.ts';
import {type ShardID} from '../../types/shards.ts';
import {
  getSubscriptionState,
  ZERO_VERSION_COLUMN_NAME,
} from '../replicator/schema/replication-state.ts';
import {checkClientSchema} from './client-schema.ts';
import {rowIDSignatureUnit} from './row-set-signature.ts';
import type {Snapshotter} from './snapshotter.ts';
import {ResetPipelinesSignal, type SnapshotDiff} from './snapshotter.ts';
import type {StatementRunner} from '../../db/statements.ts';

type RowOp<Op extends Omit<ChangeType, ChangeType.CHILD>> = {
  readonly type: Op;
  readonly queryID: string;
  readonly table: string;
  readonly rowKey: Row;
  readonly row: Row;
};

/**
 * Hard cap on rows the SQL ground-truth oracle will materialize. The audit
 * SQL only carries a LIMIT when the query AST has one, so an unlimited query
 * over a large table would otherwise read the entire table into the JS heap.
 * Past the cap the oracle returns `skipped` (row-cap-exceeded) and the caller
 * falls back to the TS-vs-Go set comparison. 20k rows ≈ tens of MB worst
 * case — large enough for every sandbox-scale query, small enough to never
 * threaten the heap.
 */
const SQL_ORACLE_ROW_CAP = 20_000;

/**
 * Hard cap on TS hydrate rows retained per query for the shadow batch
 * comparison. Past the cap the view-syncer keeps counting but stops
 * buffering, and {@link PipelineDriver.shadowBatchCompare} degrades that
 * query to a count-only compare. Without this, shadow mode retained every
 * CG's entire hydrate result set (TS side AND Go side) in the JS heap for
 * the duration of the batch RPC — a reconnect-storm OOM at production
 * scale (hydration p95 ~0.9s ⇒ result sets are large).
 */
export const SHADOW_COMPARE_ROW_CAP = 20_000;

/**
 * Per-query TS hydrate results collected for {@link PipelineDriver.shadowBatchCompare}.
 * `changes` is capped at {@link SHADOW_COMPARE_ROW_CAP}; `total` is the true
 * row count (so `total > changes.length` marks a truncated entry).
 */
export type ShadowHydrateResult = {changes: RowChange[]; total: number};

/**
 * Queries per hydrateManyStream RPC in the Go-primary batch-hydrate path
 * ({@link PipelineDriver.goHydrateBatchStream}). The socket delivers Go's
 * results with no backpressure while the consumer drains into (slow) CVR
 * flushes, so the sub-batch size bounds how many queries' full result sets
 * can sit buffered in the JS heap at once. Drive mode serializes per-CG
 * hydrates on the snapshotter's single conn, so small sub-batches cost
 * little parallelism.
 */
const GO_HYDRATE_SUB_BATCH = 8;

/**
 * Per-chunk streaming hydrate. When on (production DEFAULT — the Go path
 * streams by default; GO_IVM_PERCHUNK_HYDRATE=false reverts),
 * goHydrateBatchStream requests chunked delivery from the Go sidecar and
 * yields each chunk to the view-syncer as it arrives — so query#1's poke
 * delivery overlaps the remaining chunks' Go compute (intra-query pipelining),
 * extending the existing inter-query overlap. When false the
 * accumulator buffers each query to its terminal frame and yields once,
 * byte-identical to the pre-experiment path. Safe on cold hydrate: every
 * change is an ADD and #trackRowSetSignatures XORs per row (associative), so
 * splitting a query across chunk-boundaried onResult calls produces the
 * identical final signature. Chunk granularity is the Go side's
 * GO_IVM_CHUNK_SIZE (default 100; per-row on the NAPI rowMode plane).
 */
const GO_PERCHUNK_HYDRATE = process.env.GO_IVM_PERCHUNK_HYDRATE !== 'false';

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

/**
 * Multiset (bag) diff over a string key. Returns the keys whose occurrence
 * COUNT differs between the two lists, with each side's count. Unlike a
 * Set-based diff this catches MULTIPLICITY divergence — e.g. a join fan-out
 * that emits a row N times on one side and M on the other — which every
 * PK-keyed (deduping) comparison in the audit is structurally blind to.
 */
export function multisetDiff(
  a: readonly string[],
  b: readonly string[],
): {key: string; aCount: number; bCount: number}[] {
  const count = (xs: readonly string[]) => {
    const m = new Map<string, number>();
    for (const x of xs) m.set(x, (m.get(x) ?? 0) + 1);
    return m;
  };
  const ca = count(a);
  const cb = count(b);
  const out: {key: string; aCount: number; bCount: number}[] = [];
  for (const k of new Set([...ca.keys(), ...cb.keys()])) {
    const av = ca.get(k) ?? 0;
    const bv = cb.get(k) ?? 0;
    if (av !== bv) out.push({key: k, aCount: av, bCount: bv});
  }
  return out;
}

/**
 * Op-kind parity: for each (table,rowKey) touched by an advance, the SET of
 * change types each engine emitted. A row that TS expresses as a single
 * `edit` while Go expresses as `remove`+`add` (or vice versa) yields the SAME
 * final row — so #shadowCompare's positional row compare and every PK-set
 * check pass — but is a genuinely different client-visible wire sequence
 * (edit patches in place; remove+add flickers the row out and back). Returns
 * the rowKeys whose type-multiset differs between the two sides.
 */
export function opKindDiff(
  tsChanges: readonly RowChange[],
  goChanges: readonly RowChange[],
): {key: string; tsTypes: number[]; goTypes: number[]}[] {
  const byKey = (changes: readonly RowChange[]) => {
    const m = new Map<string, number[]>();
    for (const c of changes) {
      const k = `${c.table}|${stableStringify(c.rowKey)}`;
      let arr = m.get(k);
      if (!arr) {
        arr = [];
        m.set(k, arr);
      }
      arr.push(c.type);
    }
    for (const v of m.values()) v.sort((x, y) => x - y);
    return m;
  };
  const ts = byKey(tsChanges);
  const go = byKey(goChanges);
  const out: {key: string; tsTypes: number[]; goTypes: number[]}[] = [];
  for (const k of new Set([...ts.keys(), ...go.keys()])) {
    const tt = ts.get(k) ?? [];
    const gt = go.get(k) ?? [];
    if (tt.length !== gt.length || tt.some((t, i) => t !== gt[i])) {
      out.push({key: k, tsTypes: tt, goTypes: gt});
    }
  }
  return out;
}

/**
 * True when a TS-vs-Go set difference is a benign LIMIT tie-member swap rather
 * than a real divergence (used by shadow mode to suppress nondeterministic
 * MISMATCHes). TS-vs-TS-window — no oracle. An ordered + LIMITed query whose
 * sort lacks a unique tiebreaker has nondeterministic window membership when a
 * tie group straddles the boundary, so TS and Go can legitimately keep
 * different members of that tie.
 *
 * Conservative by construction — returns false (keeps the MISMATCH) on: no
 * orderBy / no limit; any child/related-table rows present (a root tie-swap
 * drags children we can't reason about, and single-root-table keeps the diff
 * PK-unique with no multiplicity ambiguity); any common row whose content or
 * change-kind differs (real value drift); any differing row that is NOT a
 * boundary tie member (its ORDER BY key value present on BOTH sides). So it
 * never suppresses a genuine row/value/multiplicity divergence.
 */
export function isShadowTieWindow(
  ast: AST | undefined,
  tsChanges: readonly RowChange[],
  goChanges: readonly RowChange[],
): boolean {
  if (!ast?.orderBy?.length || ast.limit === undefined) return false;
  // Symmetric swaps only: a clean tie-window cut leaves both windows FULL at the
  // same row count (a tie group straddling the boundary, different members
  // kept). Unequal counts mean one side dropped/added a row — not a boundary
  // swap — so keep it as a MISMATCH (also avoids masking a real row-drop).
  if (tsChanges.length !== goChanges.length) return false;
  const rootTable = ast.table;
  if (
    tsChanges.some(c => c.table !== rootTable) ||
    goChanges.some(c => c.table !== rootTable)
  ) {
    return false;
  }
  const orderFields = ast.orderBy.map(([fld]) => fld);
  const keyTuple = (row: Record<string, unknown>) =>
    stableStringify(orderFields.map(fld => row[fld]));
  const index = (changes: readonly RowChange[]) => {
    const m = new Map<string, RowChange>();
    for (const c of changes) m.set(stableStringify(c.rowKey), c);
    return m;
  };
  const ts = index(tsChanges);
  const go = index(goChanges);
  const tsKeys = new Set(Array.from(ts.values(), c => keyTuple(c.row)));
  const goKeys = new Set(Array.from(go.values(), c => keyTuple(c.row)));
  const isTie = (c: RowChange) => {
    const k = keyTuple(c.row);
    return tsKeys.has(k) && goKeys.has(k);
  };
  let anySetDiff = false;
  for (const k of new Set([...ts.keys(), ...go.keys()])) {
    const t = ts.get(k);
    const g = go.get(k);
    if (t && g) {
      if (t.type !== g.type || stableStringify(t.row) !== stableStringify(g.row)) {
        return false; // real content / op-kind drift, not a tie swap
      }
    } else {
      const c = t ?? g;
      if (!c || !isTie(c)) return false; // a non-tie set difference is real
      anySetDiff = true;
    }
  }
  return anySetDiff;
}

/**
 * Decides whether an `advance`-path TS-vs-Go set difference is a BENIGN
 * cross-batch frame-skew split (Go's snapshotter and TS's snapshotter placed
 * the same logical changes in different advance batches) vs a REAL divergence
 * that must stay a [shadow] MISMATCH.
 *
 * The advance path has no single AST (it spans many queries' incremental
 * deltas), so the SQL ground-truth classifier that adjudicates `batch-hydrate`
 * cannot run here — a raw advance MISMATCH otherwise surfaces unattributed, and
 * a frame-skew split can be hundreds of rows (the go-primary soak confirmed a
 * 588-row channel_participants fan-out that landed entirely in TS's batch and
 * not Go's for that one advance window). That scale of false alarm is exactly
 * where a genuine 1-row Go bug would hide, so suppressing the benign shape
 * restores the advance MISMATCH as a trustworthy signal rather than reducing
 * noise for its own sake.
 *
 * Mechanism, proven deterministically by the go-ivm advance_drift_shadow_mismatch
 * repro tests: both engines process every change correctly when they receive it
 * and converge at head; they only disagree on which advance batch carries a
 * given edit (independently-pinned WAL frames). Within one #shadowCompare call
 * that manifests as a clean PARTITION — the TS-only and Go-only sides are
 * disjoint on rowKey, no rowKey appears on both sides with differing change kind
 * or content, each (queryID, table, rowKey) appears at most once per side, and
 * both sides carry at least one exclusive row. A real Go drift, by contrast,
 * either changes a row's content or op-kind on a shared key (value/op drift) or
 * emits a row the other side never emits at any batch (asymmetric drop) —
 * neither is a clean partition.
 *
 * Conservative by construction — returns false (keeps the MISMATCH) on: no
 * divergence at all; any rowKey present on BOTH sides whose change kind OR row
 * content differs (real value/op drift — this is the case that must never be
 * masked, since a real 1-row Go bug under a 588-row false alarm would look like
 * exactly this); any (queryID, table, rowKey) tuple appearing more than once
 * on a side (real multiplicity/fan-out divergence, not a one-per-key partition);
 * an empty side with a non-empty other (a pure drop/add, not a split). So it
 * never suppresses a genuine row, value, op-kind, or multiplicity divergence.
 * Unlike isShadowTieWindow it needs no AST — the partition signature is
 * structural and applies uniformly across all advance queries, ordered or not.
 */
export function isAdvanceFrameSkew(
  tsChanges: readonly RowChange[],
  goChanges: readonly RowChange[],
): boolean {
  // No divergence → nothing to suppress (and a clean partition of two equal
  // sets is the no-op case, not a frame-skew split).
  if (tsChanges.length === goChanges.length) {
    let any = false;
    for (let i = 0; i < tsChanges.length; i++) {
      if (
        tsChanges[i].type !== goChanges[i].type ||
        tsChanges[i].queryID !== goChanges[i].queryID ||
        tsChanges[i].table !== goChanges[i].table ||
        stableStringify(tsChanges[i].rowKey) !==
          stableStringify(goChanges[i].rowKey) ||
        stableStringify(tsChanges[i].row) !== stableStringify(goChanges[i].row)
      ) {
        any = true;
        break;
      }
    }
    if (!any) return false;
  }

  const keyOf = (c: RowChange) =>
    stableStringify({
      q: c.queryID,
      t: c.table,
      k: c.rowKey,
    });
  const tsKeys = new Map<string, RowChange>();
  const goKeys = new Map<string, RowChange>();
  for (const c of tsChanges) {
    const k = keyOf(c);
    // A duplicate tuple on the TS side alone is a real multiplicity divergence
    // (the clean-partition invariant requires each key exactly once per side).
    if (tsKeys.has(k)) return false;
    tsKeys.set(k, c);
  }
  for (const c of goChanges) {
    const k = keyOf(c);
    if (goKeys.has(k)) return false;
    goKeys.set(k, c);
  }

  // Any rowKey present on BOTH sides must agree on BOTH change kind and content
  // — a same-row ADD-vs-REMOVE (op drift) or a same-key content difference
  // (value drift) is a real divergence, not a batch split. Keying on rowKey
  // alone (not type) is what catches the ADD/REMOVE pair: they collide on the
  // same key and fail this check.
  for (const [k, tc] of tsKeys) {
    const gc = goKeys.get(k);
    if (
      gc &&
      (tc.type !== gc.type || stableStringify(tc.row) !== stableStringify(gc.row))
    ) {
      return false;
    }
  }

  // A clean cross-batch partition requires BOTH sides to carry rows the other
  // lacks (each engine's batch got some of the split, none of it shared). A
  // fully-shared set with no exclusive rows is handled above; a one-sided-only
  // set (empty other) is a pure drop/add, not a split — keep it as a MISMATCH.
  let tsExclusive = 0;
  let goExclusive = 0;
  for (const k of tsKeys.keys()) if (!goKeys.has(k)) tsExclusive++;
  for (const k of goKeys.keys()) if (!tsKeys.has(k)) goExclusive++;
  return tsExclusive > 0 && goExclusive > 0;
}

/**
 * Cross-batch frame-skew suppression for the EMPTY-SIDE case.
 * `isAdvanceFrameSkew` only suppresses a CLEAN PARTITION where BOTH sides carry
 * exclusive rows (:534-542) — so it KEEPs (as a MISMATCH) any advance batch
 * where one engine's side is empty and the other's is not. But the same WAL
 * frame-skew that produces a both-sides-exclusive split can also place ALL of
 * a logical change in one engine's batch and NONE in the other's, with the
 * missing rows appearing on the OTHER engine in the adjacent advance batch
 * (live-proven 2026-06-22: frames 81b3tyfhi0 TS=1/Go=4 and 81b3tyh9ug TS=3/Go=0,
 * byte-identical rows — the intra-frame classifier ran but the empty-side guard
 * at :534-542 blocked suppression). This classifier closes that gap by looking
 * at the poke-paired NEIGHBOR advance: if this batch has one empty side and the
 * other side's rows appear byte-identical on the OPPOSITE engine in the
 * neighbor batch, it's the same cross-batch frame-skew split — suppress.
 *
 * Strict invariants (mirroring isAdvanceFrameSkew :4163-4168) so a REAL
 * one-sided drop is never silenced:
 *   - This batch must be one-sided (one side empty, the other non-empty).
 *   - Every row on the non-empty side must appear on the OPPOSITE engine's side
 *     in the neighbor batch, byte-identical (full RowChange: type + queryID +
 *     table + rowKey + row — NOT just PK; this is what distinguishes "rows
 *     moved to the neighbor frame" from "rows genuinely dropped").
 *   - No (queryID, table, rowKey, type) tuple may appear more than once across
 *     the union of this batch's non-empty side and the neighbor's matched side
 *     (no multiplicity divergence hiding behind the match).
 *   - The match is full-content. A same-PK different-content row in the
 *     neighbor is NOT a match — falls through to MISMATCH (the real value-drift
 *     case that must always survive).
 *
 * `neighbor` is the prior advance batch's (ts, go); the caller threads a
 * 1-deep rolling buffer so only the immediately-adjacent batch is consulted —
 * a stale match two batches back cannot trigger suppression.
 */
export function isAdvanceFrameSkewCrossBatch(
  tsChanges: readonly RowChange[],
  goChanges: readonly RowChange[],
  neighbor: {ts: readonly RowChange[]; go: readonly RowChange[]} | null,
): boolean {
  // No neighbor → can't look across batches; defer to isAdvanceFrameSkew.
  // Also bail when the current batch is itself a clean both-sides partition —
  // the intra-frame classifier already handles that, and running the cross-batch
  // check would be redundant (and could double-suppress).
  if (!neighbor) return false;
  if (tsChanges.length > 0 && goChanges.length > 0) return false;
  if (tsChanges.length === 0 && goChanges.length === 0) return false;

  // This batch is one-sided. Determine which engine has the rows here, and
  // which engine's neighbor side they must match against.
  const tsEmpty = tsChanges.length === 0;
  const hereChanges = tsEmpty ? goChanges : tsChanges;
  // Rows present HERE on engine X must appear on engine Y (the OTHER engine)
  // in the NEIGHBOR batch — the frame-skew split put them in X's batch this
  // advance and Y's batch the adjacent advance.
  const neighborOtherSide = tsEmpty ? neighbor.ts : neighbor.go;

  if (neighborOtherSide.length === 0) return false;

  // Full-content key: byte-identical match requires type + queryID + table +
  // rowKey + row to all agree. This is the false-negative guard — a real drop
  // whose PK happens to match a neighbor re-emit but with different content
  // must NOT suppress.
  const fullKey = (c: RowChange) =>
    stableStringify({
      type: c.type,
      queryID: c.queryID,
      table: c.table,
      rowKey: c.rowKey,
      row: c.row,
    });
  const hereKeys = new Map<string, RowChange>();
  for (const c of hereChanges) {
    const k = fullKey(c);
    // A duplicate on the non-empty side is a multiplicity divergence — keep.
    if (hereKeys.has(k)) return false;
    hereKeys.set(k, c);
  }
  const neighborKeys = new Set<string>();
  for (const c of neighborOtherSide) {
    const k = fullKey(c);
    if (neighborKeys.has(k)) return false; // multiplicity in the neighbor — keep.
    neighborKeys.add(k);
  }
  // Every row here must be present byte-identical on the other engine's
  // neighbor side. Any missing (or content-different) row → real drop → keep.
  for (const k of hereKeys.keys()) {
    if (!neighborKeys.has(k)) return false;
  }
  return true;
}

/**
 * Verdict shape for the advance-path SQL ground-truth oracle. Mirrors
 * {@link #sqlGroundTruthCompare}'s hydrate verdict EXCEPT: no `go-vs-sql-
 * tie-window` (a delta bag has no LIMIT window — order is meaningless for
 * ADD/EDIT/REMOVE) and an added `oracle-blind` (the divergence is entirely
 * off-table fan-out for this queryID — no main-table delta to adjudicate).
 * The classify logic in {@link #shadowCompare}'s advance branch consumes
 * `confirmed | go-vs-sql-drift | go-vs-sql-content-drift | oracle-blind |
 * skipped` and reuses the SAME counters as the hydrate oracle.
 */
export type AdvanceSqlOracleVerdict =
  | {kind: 'confirmed'; sqlCount: number}
  | {kind: 'go-vs-sql-drift'; sqlCount: number; goOnly: string[]; sqlOnly: string[]}
  | {
      kind: 'go-vs-sql-content-drift';
      sqlCount: number;
      contentMismatches: {pk: string; sqlRow: string; goRow: string}[];
    }
  | {kind: 'oracle-blind'; sqlCount: number}
  | {kind: 'skipped'; reason: string};

/**
 * Pure core of the advance-path SQL ground-truth oracle. Given the ALREADY-
 * QUERIED + NORMALIZED rows from the snapshotter's prev/curr snapshots for
 * one query's main table, derive the expected prev→curr delta (ADD / REMOVE
 * / EDIT) and compare it to Go's emitted changes for that query (filtered to
 * the main table). Returns the verdict; the SQL I/O + normalization wrapper
 * {@link #sqlGroundTruthAdvanceCompare} delegates here.
 *
 * Extracted as an exported pure function (mirroring {@link isAdvanceFrameSkew})
 * so the delta-derivation + comparison logic is unit-testable without a live
 * SQLite replica — the test feeds `prevRows` / `currRows` directly.
 *
 * `pk` is the table's primary-key columns; `zqlColumns` is the schema column
 * names used to project Go's row (Go may carry extra bookkeeping fields like
 * `_0_version` that the oracle must drop — same projection as the hydrate
 * oracle's `:2969-2970`). `sqlCount` in the verdict is `currRows.length`
 * (the post-snapshot main-table row count — matches the hydrate oracle's
 * `sqlByPK.size`).
 */
export function compareAdvanceDeltaToSqlDelta(
  table: string,
  pk: readonly string[],
  zqlColumns: readonly string[],
  prevRows: Record<string, unknown>[],
  currRows: Record<string, unknown>[],
  goChanges: RowChange[],
): AdvanceSqlOracleVerdict {
  const pkOf = (row: Record<string, unknown>): string => {
    const rowKey: Record<string, unknown> = {};
    for (const col of pk) rowKey[col] = row[col];
    return stableStringify(rowKey);
  };

  const prevByPK = new Map<string, Record<string, unknown>>();
  for (const row of prevRows) prevByPK.set(pkOf(row), row);
  const currByPK = new Map<string, Record<string, unknown>>();
  for (const row of currRows) currByPK.set(pkOf(row), row);

  // Derive the EXPECTED delta from prev→curr. A row in curr not in prev is an
  // ADD; a row in prev not in curr is a REMOVE; a row in both whose content
  // differs is an EDIT. StableStringify (deep-sorted keys) for content
  // comparison matches #sqlGroundTruthCompare's :2971-2972.
  type ExpectedEntry = {type: 'add' | 'remove' | 'edit'; pk: string; rowStr: string};
  const expected = new Map<string, ExpectedEntry>(); // pk → entry
  for (const [pkKey, currRow] of currByPK) {
    const prevRow = prevByPK.get(pkKey);
    if (!prevRow) {
      expected.set(pkKey, {type: 'add', pk: pkKey, rowStr: stableStringify(currRow)});
    } else {
      const prevStr = stableStringify(prevRow);
      const currStr = stableStringify(currRow);
      if (prevStr !== currStr) {
        expected.set(pkKey, {type: 'edit', pk: pkKey, rowStr: currStr});
      }
      // else: unchanged — no delta, not in `expected`.
    }
  }
  for (const [pkKey] of prevByPK) {
    if (!currByPK.has(pkKey)) {
      expected.set(pkKey, {type: 'remove', pk: pkKey, rowStr: ''});
    }
  }

  // Go's RowChange.type → the expected map's stringified type. ChangeType.ADD
  // = 0, REMOVE = 1, EDIT = 2 (CHILD = 3 extends ChangeType but advances
  // never emit CHILD for the main table; the spec filter + c.table === ast.table
  // already excludes fan-out children, and a CHILD here would be an oracle
  // miss, not a drift we can adjudicate → null → treated as go-vs-sql-drift.)
  const goTypeStr = (t: number): 'add' | 'remove' | 'edit' | null => {
    switch (t) {
      case ChangeType.ADD:
        return 'add';
      case ChangeType.REMOVE:
        return 'remove';
      case ChangeType.EDIT:
        return 'edit';
      default:
        return null;
    }
  };

  const goByPK = new Map<string, {type: 'add' | 'remove' | 'edit' | null; rowStr: string}>();
  for (const c of goChanges) {
    if (c.table !== table) continue;
    const pkKey = stableStringify(c.rowKey);
    const goRowProjected: Record<string, unknown> = {};
    for (const col of zqlColumns) goRowProjected[col] = c.row[col];
    goByPK.set(pkKey, {
      type: goTypeStr(c.type),
      rowStr: c.type === ChangeType.REMOVE ? '' : stableStringify(goRowProjected),
    });
  }

  // If Go has NO main-table changes for this query AND the expected delta is
  // empty, the divergence lives entirely off-table (fan-out) — oracle-blind.
  // (Only reached when bagsDiffer already held in #shadowCompare, so an
  // all-empty main-table is oracle-blind, not a true `confirmed`.)
  const sqlCount = currByPK.size;
  if (goByPK.size === 0 && expected.size === 0) {
    return {kind: 'oracle-blind', sqlCount};
  }

  // Set + type + content compare: every expected entry must be matched by a
  // Go entry of the same type+content, and vice versa.
  const goOnly: string[] = [];
  const sqlOnly: string[] = [];
  const contentMismatches: {pk: string; sqlRow: string; goRow: string}[] = [];

  for (const [pkKey, exp] of expected) {
    const go = goByPK.get(pkKey);
    if (!go) {
      sqlOnly.push(pkKey); // SQL expects a delta here, Go emitted none.
      continue;
    }
    if (go.type !== exp.type) {
      // Same PK, different op kind (e.g. SQL says edit, Go says add) — a real
      // divergence on the main table.
      contentMismatches.push({
        pk: pkKey,
        sqlRow: `${exp.type}:${exp.rowStr}`,
        goRow: `${go.type}:${go.rowStr}`,
      });
      continue;
    }
    if (exp.type !== 'remove' && go.rowStr !== exp.rowStr) {
      contentMismatches.push({pk: pkKey, sqlRow: exp.rowStr, goRow: go.rowStr});
    }
  }
  for (const [pkKey] of goByPK) {
    if (!expected.has(pkKey)) goOnly.push(pkKey); // Go emitted a delta SQL doesn't expect.
  }

  if (goOnly.length > 0 || sqlOnly.length > 0) {
    return {kind: 'go-vs-sql-drift', sqlCount, goOnly, sqlOnly};
  }
  if (contentMismatches.length > 0) {
    return {kind: 'go-vs-sql-content-drift', sqlCount, contentMismatches};
  }
  return {kind: 'confirmed', sqlCount};
}

export type RowAdd = RowOp<ChangeType.ADD>;

export type RowRemove = RowOp<ChangeType.REMOVE>;

export type RowEdit = RowOp<ChangeType.EDIT>;

export type RowChange = RowAdd | RowRemove | RowEdit;

export type AdvanceResult = {
  version: string;
  numChanges: number;
  changes: Iterable<RowChange | 'yield'>;
  // P2c observability: when Go-primary serves user queries via advanceToHead,
  // `version` above is the RECONCILED watermark min(tsVersion, goVersion). These
  // expose the two un-reconciled authorities so the view-syncer can assert
  // monotonicity / log the split. Both undefined on the TS-only and push paths
  // (where the watermark is simply TS's version).
  tsVersion?: string | undefined;
  goVersion?: string | undefined;
};

/**
 * P2c watermark reconciliation (DESIGN-snapshotter-port.md §10). In Go-primary
 * trigger mode the user-query data is at Go's `goVersion` (V_go) while
 * internal/control-plane data is at TS's `tsVersion` (V_ts). The CVR
 * stateVersion is a COMPLETENESS FLOOR — client catchup/poke keys off the
 * `patchVersion` stamped at the committed CVR version, not the row's own
 * `_0_version` — so it may only be committed at a version BOTH authorities have
 * crossed: min(V_ts, V_go). Under-claiming is safe (the ahead side's extra rows
 * are an idempotent superset, re-delivered next cycle at the committed
 * patchVersion); over-claiming would let a client miss a change in the gap.
 *
 * In push mode `goVersion` is undefined (Go applied TS's diff, so its version is
 * TS's by construction) and the watermark is simply V_ts.
 *
 * `min` is monotone in each argument and each authority only advances, so the
 * reconciled watermark is non-decreasing — the CVR monotonicity invariant holds.
 */
export function reconcileGoPrimaryWatermark(
  tsVersion: string,
  goVersion: string | undefined,
): {version: string; tsVersion: string; goVersion: string | undefined} {
  // Treat empty string same as undefined — Go omitting `version` on the
  // final frame decodes as '' via `v.version ?? ''` in go-ivm-client.ts.
  // Without this guard, '' < "00" in lexi-version min() regresses the
  // CVR watermark to '' causing full re-hydration for all clients.
  if (goVersion === undefined || goVersion === '') {
    return {version: tsVersion, tsVersion, goVersion: undefined};
  }
  return {version: minLexiVersion(tsVersion, goVersion), tsVersion, goVersion};
}

/**
 * F1 advance-dispatch decision (non-shadow Go-primary mode only). Given the LIVE
 * Go backend availability and the mode the CURRENT user-query pipelines were
 * built in (#goUserPipelineMode), decide how advance() must proceed. The two
 * reset outcomes make advance() RETURN a ResetPipelinesSignal (never throw it)
 * so the view-syncer rebuilds the pipelines for the live Go state before the
 * next advance.
 *
 *   'go-advance'      Go UP and the user pipelines are Go-owned stubs (or none
 *                     yet) → run the Go-primary advance (Go owns user queries).
 *   'reset-recovered' Go UP but the pipelines were degraded to real TS while Go
 *                     was down → rebuild as Go-owned stubs first, else real TS
 *                     pipelines AND Go would both emit user rows (double-count).
 *   'reset-degrade'   Go DOWN and the pipelines are Go-owned stubs → the
 *                     TS-native advance emits NOTHING for them: a silent client
 *                     freeze with the cookie advancing past the gap (watermark
 *                     over-claim). Reset so re-registration builds REAL TS
 *                     pipelines (graceful degradation to TS-serving). This fires
 *                     on routine sidecar restarts and the drift-breaker cooldown,
 *                     not just terminal failure.
 *   'ts-native'       Go DOWN and the pipelines are already real TS (or none) →
 *                     the TS-native advance serves correctly; do NOT reset (that
 *                     would loop every advance for the whole outage / cooldown).
 */
export type GoPrimaryDispatchDecision =
  | 'go-advance'
  | 'reset-recovered'
  | 'reset-degrade'
  | 'ts-native';

export function decideGoPrimaryDispatch(
  goInitialized: boolean,
  pipelineMode: 'go' | 'ts' | undefined,
): GoPrimaryDispatchDecision {
  if (goInitialized) {
    return pipelineMode === 'ts' ? 'reset-recovered' : 'go-advance';
  }
  return pipelineMode === 'go' ? 'reset-degrade' : 'ts-native';
}

/**
 * F2 classification of a Go-primary advance RPC failure (advanceStream OR
 * advanceToHead) as a PURE decision — the metric counters and logging live in
 * the #classifyGoPrimaryAdvanceError method that wraps this.
 *
 *   'protocol'     wire-level violation/corruption (chunk order, missing final
 *                  chunk, oversized frame, protocolRev mismatch) — RE-THROW; a
 *                  reset can't fix a wire bug without a sidecar process restart.
 *   'stale-epoch'  this instance was superseded by a successor's initEpoch —
 *                  RE-THROW so teardown completes.
 *   'data-error'   permanent, non-retryable bad replica data (ivm.DataError /
 *                  RPC_CODE_DATA_ERROR) — RE-THROW (teardown), NEVER reset: a
 *                  reset re-reads the same bad row and loops forever.
 *   'sidecar'      sidecar unavailable / restart in flight — DROP → reset.
 *   'unclassified' anything else (incl. RPC timeouts under load) — DROP → reset.
 *
 * The two DROP buckets escalate to a ResetPipelinesSignal (full re-hydrate)
 * instead of the legacy "return [] + #scheduleGoReset", which left a permanent
 * (prev→head] gap: the async reset rebuilt Go at head and DISCARDED its hydrate
 * output, so the dropped user delta was never delivered. The order matters —
 * protocol message patterns are checked before the stale-epoch instance so a
 * protocol violation that also happens to be a stale-epoch error escalates as a
 * protocol violation (both re-throw, so the buckets are observably distinct but
 * behaviourally identical here).
 */
export type GoAdvanceErrorClass =
  | 'protocol'
  | 'stale-epoch'
  | 'data-error'
  | 'sidecar'
  | 'unclassified';

export function classifyGoPrimaryAdvanceError(e: unknown): GoAdvanceErrorClass {
  const msg = e instanceof Error ? e.message : String(e);
  if (
    msg.includes('chunk order violation') ||
    msg.includes('finished without a final chunk') ||
    msg.includes('Frame too large') ||
    msg.includes('protocolRev mismatch')
  ) {
    return 'protocol';
  }
  if (e instanceof StaleInitEpochError) {
    return 'stale-epoch';
  }
  // Permanent data error (RPC_CODE_DATA_ERROR → PermanentDataError): bad
  // replica data the sidecar can't represent. Checked before the 'sidecar' /
  // 'unclassified' DROP buckets so a poison row tears down ONCE instead of
  // reset-looping. The `instanceof` is the robust path; the message fallback
  // catches any DataError that reached us as a plain Error (defense in depth).
  if (
    e instanceof PermanentDataError ||
    msg.includes('FromSQLiteType') ||
    msg.includes('cannot compare values of different types')
  ) {
    return 'data-error';
  }
  if (
    msg.includes('Sidecar is not running') ||
    msg.includes('Connection closed') ||
    msg.includes('Not connected') ||
    msg.includes('engine not initialized')
  ) {
    return 'sidecar';
  }
  return 'unclassified';
}

/**
 * F3 (keystone): eagerly drain a snapshotter diff, invoking `onEntry` for each
 * change, but CATCH the ResetPipelinesSignal the diff iterator throws on a
 * truncate / schema change and RETURN it instead of letting it propagate. The
 * Go-primary advance paths buffer the diff eagerly (so both TS and Go can
 * consume it); without this catch the throw escapes #advancePipelines and lands
 * in run()'s outer catch — a full client-group teardown (all clients
 * disconnected) on every truncate or schema change. Returning the signal routes
 * through the view-syncer's graceful reset + re-hydrate (the same self-heal the
 * TS-native lazy path gets). Any non-reset error propagates unchanged.
 */
export function drainDiffCatchingReset<T>(
  diff: Iterable<T>,
  onEntry: (entry: T) => void,
): ResetPipelinesSignal | undefined {
  try {
    for (const entry of diff) {
      onEntry(entry);
    }
    return undefined;
  } catch (e) {
    if (e instanceof ResetPipelinesSignal) {
      return e;
    }
    throw e;
  }
}

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
 * Consecutive drift-audit cycles a Go-pipeline-count shortfall must persist
 * before the audit fires a (full, expensive) resetEngine. The check is racy —
 * Go registers queries via async addQueriesStream that can take hundreds of ms
 * under load, so an audit firing mid-registration sees a transient shortfall.
 * 2 means "seen on two consecutive audits" (≈ two audit intervals apart), which
 * a registration lag cannot survive; a genuine freeze persists and heals one
 * cycle later. See {@link PipelineDriver.#driftCountMismatchStreak}.
 */
const DRIFT_COUNT_MISMATCH_GRACE = 2;

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
  // Per-class breakout: ORDER divergences (Go returns the right ROWS in the
  // WRONG sequence vs the SQL ORDER BY oracle). Invisible to the set/content
  // checks and to #shadowCompare (which sorts by rowKey). Counted separately
  // from #driftAuditMismatches so dashboards can distinguish "wrong rows/values"
  // from "wrong order" (e.g. enum definition-order vs TEXT collation).
  readonly #driftAuditOrderMismatches = getOrCreateCounter(
    'sync',
    'ivm.drift-audit-order-mismatches',
    'Row-order divergences (Go vs SQL ORDER BY) detected by the drift audit',
  );
  // Multiplicity divergence: same DISTINCT rows on both sides but a different
  // emission COUNT for some (table,rowKey) — a join fan-out cardinality bug
  // the PK-keyed set/content checks can't see.
  readonly #driftAuditMultiplicityMismatches = getOrCreateCounter(
    'sync',
    'ivm.drift-audit-multiplicity-mismatches',
    'TS/Go row-multiplicity divergences detected by the drift audit',
  );
  // Op-kind divergence (advance only): TS and Go agree on the final row but
  // disagree on the change TYPE (e.g. edit vs remove+add). Client-visible wire
  // difference invisible to the row/value compare.
  readonly #shadowOpKindMismatches = getOrCreateCounter(
    'sync',
    'ivm.shadow-opkind-mismatches',
    'TS/Go change-kind (edit vs remove+add) divergences in shadow advance',
  );
  // Result-ORDER divergence between TS and Go for an ordered hydrate. The set/
  // value compare in #shadowCompare sorts both sides by rowKey, so it's blind to
  // emission order — this is the only check that verifies Go reproduces TS's
  // wire ORDER (what clients render). Tie-aware: reorderings within an equal
  // ORDER BY key-value group don't count.
  readonly #shadowOrderMismatches = getOrCreateCounter(
    'sync',
    'ivm.shadow-order-mismatches',
    'TS/Go result-order divergences detected in shadow-mode hydrate',
  );
  // Shadow MISMATCHes suppressed as benign tie-window (single-root-table
  // ordered+LIMITed result where TS and Go picked different members of a tie
  // group at the boundary — nondeterministic, not a Go bug). Counted so the
  // suppression rate is visible rather than silent.
  readonly #shadowTieWindows = getOrCreateCounter(
    'sync',
    'ivm.shadow-tie-windows',
    'Shadow hydrate set-diffs suppressed as benign LIMIT tie-member swaps',
  );
  // Advance-path set-diff suppressed as benign cross-batch frame-skew: Go's
  // snapshotter and TS's placed the same logical changes in different advance
  // batches (independently-pinned WAL frames). The advance path has no single
  // AST so the SQL oracle can't adjudicate it; without this, a frame-skew split
  // surfaces as an unattributed MISMATCH (the go-primary soak saw 588 rows in
  // one such split). Proven benign by the go-ivm advance_drift_shadow_mismatch
  // repro tests — both engines converge at head. Counted so the suppression
  // rate stays visible; isAdvanceFrameSkew never suppresses a same-key value
  // drift or a multiplicity divergence.
  readonly #shadowAdvanceFrameSkew = getOrCreateCounter(
    'sync',
    'ivm.shadow-advance-frame-skew',
    'Shadow advance set-diffs suppressed as benign cross-batch frame-skew',
  );
  // Cross-batch (empty-side) frame-skew: the adjacent-batch variant where one
  // engine's side of an advance batch is empty and the rows appear on the
  // other engine in the poke-paired neighbor batch. Counted separately so the
  // empty-side suppression rate is observable independently of the
  // both-sides-exclusive case above (the two are different shapes of the same
  // underlying WAL frame-skew, but only the empty-side one was uncaught before).
  readonly #shadowAdvanceFrameSkewCrossBatch = getOrCreateCounter(
    'sync',
    'ivm.shadow-advance-frame-skew-cross-batch',
    'Shadow advance empty-side set-diffs suppressed as benign cross-batch frame-skew',
  );
  // A shadow MISMATCH the SQL oracle attributed to TS (Go matched SQL). TS is
  // NOT always right — it has known IVM pagination-boundary bugs — so without
  // this, a TS bug is mislabeled as a Go drift. Demoted to info, not a Go fault.
  readonly #shadowTsOnlyDivergences = getOrCreateCounter(
    'sync',
    'ivm.shadow-ts-only-divergences',
    'Shadow MISMATCHes where Go matches the SQL oracle and TS is the outlier',
  );
  // A shadow MISMATCH the SQL oracle confirmed as a REAL Go drift (Go disagrees
  // with SQL). This is the signal that actually matters.
  readonly #shadowConfirmedGoDrift = getOrCreateCounter(
    'sync',
    'ivm.shadow-confirmed-go-drift',
    'Shadow MISMATCHes confirmed as real Go drift by the SQL oracle',
  );
  // A shadow MISMATCH the SQL oracle could NOT adjudicate at all — buildAuditSQL
  // couldn't translate the query shape (verdict 'skipped'), so we fall through to
  // the raw TS-vs-Go MISMATCH. Counted so the unadjudicable rate stays visible
  // (and the buildAuditSQL coverage gap can be closed); NOT itself a Go-fault
  // signal. (Was previously incremented for the SQL=0-both-engines-have-rows
  // case, until replica ground-truth proved SQL=0 is authoritative there.)
  readonly #shadowSqlUnreliable = getOrCreateCounter(
    'sync',
    'ivm.shadow-sql-unreliable',
    'Shadow MISMATCHes the SQL oracle could not adjudicate (unbuildable SQL)',
  );
  // The advance path's SQL ground-truth oracle (#sqlGroundTruthAdvanceCompare)
  // was actually run — i.e. a diverged advance per-queryID had a recoverable
  // AST + a live SnapshotDiff, so we could re-query prev/curr to derive the
  // expected delta and adjudicate. Counted separately from the hydrate oracle
  // runs so advance-oracle coverage is observable on its own; the verdict
  // breakdown (confirmed / go-vs-sql-drift / oracle-blind / skipped) flows
  // through the SAME counters as the hydrate oracle (#shadowTsOnlyDivergences,
  // #shadowConfirmedGoDrift, #shadowSqlUnreliable), reusing the existing
  // classification — this counter is purely "the advance oracle fired".
  readonly #shadowAdvanceSqlOracleRuns = getOrCreateCounter(
    'sync',
    'ivm.shadow-advance-sql-oracle-runs',
    'Shadow advance per-queryID divergences the SQL ground-truth oracle adjudicated',
  );
  // Incremental-path divergence: Go's accumulated advance deltas for a query
  // disagree with a fresh SQL re-hydrate of the same query — i.e. some advance
  // emitted a wrong delta even though a full re-materialization would be right.
  // This is the only check that validates the INCREMENTAL path; every other
  // layer compares point-in-time materialized state.
  readonly #driftAuditIncrementalMismatches = getOrCreateCounter(
    'sync',
    'ivm.drift-audit-incremental-mismatches',
    'Go advance-delta accumulation vs SQL re-hydrate divergences',
  );
  // #4b: a same-PK content drift that persists between audit cycles with stable
  // membership — the gap the PK-set-only accumulator missed. Surfaced by the
  // content-aware reconcile (same PK, differing stored content vs the fresh
  // hydrate row). The per-batch #shadowCompare catches content drift WITHIN an
  // advance; this catches it ACROSS the audit-cycle boundary when no ADD/REMOVE
  // touched the PK (so membership stayed stable and the old accumulator saw no
  // change). Defense-in-depth; cheap (the accumulator already stores the row).
  readonly #driftAuditIncrementalContentMismatches = getOrCreateCounter(
    'sync',
    'ivm.drift-audit-incremental-content-mismatches',
    'Go advance-delta same-PK content drift across audit cycles (between-cycle window)',
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
  readonly #advanceDroppedDataError = getOrCreateCounter(
    'sync',
    'ivm.advance-dropped-data-error',
    'Go-primary advance hit permanent bad replica data (CG torn down, NOT reset — prevents reset storm)',
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
  // When true (and shadowMode), each shadow advance also asks Go to derive its
  // own snapshot diff (advanceToHead) and compares it to TS's. Snapshotter-in-Go
  // P1 fidelity gate.
  readonly #goDerivedDiff: boolean;
  // P2: drive Go's engine from its own derived diff (advanceToHead) instead of
  // shipping it the TS diff, and compare its RowChanges to TS's.
  readonly #goAdvanceDrive: boolean;
  // P2c: in Go-PRIMARY mode, source the user-query advance via advanceToHead
  // (trigger) instead of advanceStream (push), making Go self-consistent and
  // stamping the CVR at min(V_ts, V_go). Only ever true when !shadowMode.
  readonly #goPrimaryTrigger: boolean;
  // P3: in Go-PRIMARY mode, skip walking USER-table changes in TS's #advance
  // (TS holds only stub user pipelines; user TableSources stay current via
  // snapshot setDB, not these pushes). Only ever true when !shadowMode.
  readonly #goLeanPrimary: boolean;
  #goInitPromise: Promise<void> | null = null;
  /**
   * F1: how the CURRENT user-query pipelines were built, so the advance
   * dispatch can detect a Go-availability flip and rebuild in the right mode.
   *   'go'      — Go-owned stubs (TS emits nothing for user queries).
   *   'ts'      — real TS pipelines (degraded because Go was unavailable at
   *               build time, or a pure-TS deployment).
   *   undefined — no user pipelines built yet (or only internal queries).
   * Only consulted in non-shadow Go-primary mode. A mismatch — Go DOWN with
   * 'go' stubs (would silently freeze + over-claim the watermark), or Go UP
   * with 'ts' pipelines (real TS + Go would both emit → double-count) — makes
   * #advanceDispatch return a ResetPipelinesSignal so re-registration rebuilds
   * for the live Go state. Tracking the mode (rather than resetting on every
   * Go-down advance) is what keeps the degradation a ONE-TIME event instead of
   * a reset loop for the whole outage / drift-breaker cooldown.
   */
  #goUserPipelineMode: 'go' | 'ts' | undefined = undefined;
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
  /**
   * Consecutive drift-audit cycles where Go's pipelineCount < tsExpected. The
   * count check is racy: Go registers queries via async addQueriesStream
   * (hundreds of ms under load), so an audit firing mid-registration sees a
   * transient shortfall that self-resolves by the next cycle. Resetting on the
   * FIRST shortfall churns needless full re-registrations in Go-primary mode,
   * so we require the shortfall to PERSIST across
   * `DRIFT_COUNT_MISMATCH_GRACE` consecutive audits (cycles are ~auditInterval
   * apart, so a registration lag can't survive the grace) before resetting; a
   * genuine freeze persists and still self-heals, just one cycle later. Resets
   * to 0 the moment goCount catches up.
   */
  #driftCountMismatchStreak = 0;

  #driftAuditTimer: ReturnType<typeof setInterval> | null = null;
  // Collapses overlapping audit ticks when one runs longer than the interval.
  #driftAuditInFlight = false;
  /**
   * Set by #healConfirmedDrift (Go-primary only): the detached drift-audit
   * timer cannot return a ResetPipelinesSignal itself, so it parks the heal
   * request here and the next advance() returns the signal — the view-syncer
   * then runs the full pipelines.reset → re-hydrate → CVR-diff path that
   * corrects rows already delivered to clients. Cleared on consumption and
   * on reset/init (a fresh hydrate makes the pending heal moot).
   */
  #pendingClientResetReason: string | null = null;
  // Round-robin cursor over the sorted auditable queries, so the audit covers
  // every active query within N cycles instead of sampling one at random.
  #driftAuditCursor = 0;

  // Incremental-correctness accumulator (item #2). Per queryID, the main-table
  // state as built by APPLYING Go's emitted advance deltas: a Map from PK →
  // `stableStringify(projectedRow)` (the projected row content, schema columns
  // only — same projection as the SQL oracle). ADD inserts, REMOVE deletes,
  // EDIT updates the stored content. Seeded from each drift audit's SQL hydrate,
  // then reconciled against the next SQL hydrate of the same query — so a wrong
  // advance delta surfaces even when a full re-hydrate is correct, AND (since
  // #4b stores content, not just PK membership) a same-PK content drift that
  // persists between audit cycles with stable membership is caught too — closing
  // the window the PK-set-only accumulator missed (#shadowCompare already
  // catches content drift per-batch; this catches the between-cycle case).
  // Self-healing: re-seeded every audit cycle, so any false desync clears in one
  // cycle. `#goDeltaAccumDirty` marks queries whose stream was interrupted
  // (reset / dropped advance / eviction) so the incremental check is skipped
  // until the next clean seed. Each query's Map is capped at
  // SQL_ORACLE_ROW_CAP entries with LRU eviction (dirty-on-evict: evicting a
  // row we can no longer reconcile taints the query so the next audit re-seeds
  // cleanly rather than reconciling against a truncated accumulator).
  readonly #goDeltaAccum = new Map<string, Map<string, string>>();
  readonly #goDeltaAccumDirty = new Set<string>();

  // 1-deep rolling buffer of the prior advance batch's (ts, go) changes for
  // cross-batch frame-skew suppression (isAdvanceFrameSkewCrossBatch). Only the
  // immediately-adjacent batch is kept on purpose: a stale match from two
  // batches back must not trigger suppression (a real drop that re-emits later
  // would otherwise be silenced). Overwritten each advance so it never grows.
  #advanceFrameSkewNeighbor: {ts: RowChange[]; go: RowChange[]} | null = null;

  // #2: divergence-capture rate cap. Map from capture-key (operation+queryID+
  // drift-kind) → last-fire timestamp (ms). One capture per key per minute —
  // VACUUM INTO copies the whole replica on the divergence hot path, so even
  // with the config gate ON we bound the cost under a sustained divergence
  // storm. Patterned on #driftAuditInFlight (collapses overlapping work) +
  // the drift-audit heartbeat rate-limit (#driftAuditLastHeartbeatMs).
  readonly #divergenceCaptureLastFire = new Map<string, number>();

  // Schema-type coverage (item #5): the set of column pgTypes that audited
  // queries have actually SORTED by and FILTERED on. A comparator bug for one
  // type (bytea / numeric / enum / array / timestamp …) only surfaces if some
  // audited query exercises it; this tracks what's been hit so gaps are
  // visible rather than silently untested.
  readonly #auditTypesSorted = new Set<string>();
  readonly #auditTypesFiltered = new Set<string>();
  #auditTypeCovLastReportMs = 0;
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
    // P2 drive: source the shadow Go advance via advanceToHead. Implies (and
    // supersedes) the P1 derive-only compare.
    this.#goAdvanceDrive = this.#shadowMode && isGoAdvanceDrive(config);
    // advanceToHead diff-shadow only runs inside the shadow advance path.
    // In drive mode the engine is already driven via advanceToHead, so the
    // separate derive-only compare is redundant — skip it.
    this.#goDerivedDiff =
      this.#shadowMode && isGoDerivedDiff(config) && !this.#goAdvanceDrive;
    // P2c: Go-primary trigger is mutually exclusive with shadow mode — it only
    // applies when Go's output is actually committed (enabled && !shadowMode).
    this.#goPrimaryTrigger =
      !this.#shadowMode &&
      isGoSidecarEnabled(config) &&
      isGoPrimaryTrigger(config);
    // P3 lean primary: only in Go-primary mode (TS authoritative in shadow).
    this.#goLeanPrimary =
      !this.#shadowMode && isGoSidecarEnabled(config) && isGoLeanPrimary(config);
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
              [...this.#pipelines.entries()]
                .filter(
                  ([queryID, p]) =>
                    !this.#isInternalQueryID(queryID) &&
                    !this.#isInternalTable(p.transformedAst.table),
                )
                .map(([queryID, p]) => ({
                  queryID,
                  ast: p.transformedAst,
                })),
            // O2: make the shard's appID authoritative on the advanceToHead
            // wire so the sidecar watches the right permissions table even if
            // its GO_IVM_APP_ID env was set inconsistently (externally-managed).
            // rowMode: per-row delivery on the in-process (napi) transport —
            // the client degrades it to frames when a socket came up instead.
            {appID: this.#shardID.appID, rowMode: goNapiRowMode(config)},
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
      columns: Record<string, {type: 'boolean' | 'number' | 'string' | 'null' | 'json'; optional?: boolean}>;
      primaryKey: string[];
      uniqueKeys?: string[][] | undefined;
      minRowVersion?: string | null | undefined;
      rows: Record<string, unknown>[];
    }
  > {
    // MED-8 (dispatch) invariant: this method is called from the Go backend's
    // (re-)init callback, which can run from the restart handler OUTSIDE the
    // ViewSyncer lock. It is safe ONLY because it is fully SYNCHRONOUS — the
    // snapshot reference captured here is read consistently through to the end
    // with no intervening `await`, so a concurrent advance (which can only run
    // between awaits, JS being single-threaded) cannot swap the snapshot
    // mid-build. DO NOT introduce an `await` into this method; if row reads
    // ever need to go async, snapshot `db`/`version` first and pin them, or
    // hoist the call back under the lock.
    //
    // Bail fast if the snapshotter was torn down (CG eviction / worker
    // reassignment): current() still returns the old Snapshot (destroy() closes
    // the connection but doesn't clear #curr), so reads below would throw
    // "database connection is not open" once per table. One typed throw lets the
    // caller (resetEngine / #scheduleGoReset) abandon the reset cleanly.
    if (this.#snapshotter.destroyed) {
      throw new Error('snapshotter destroyed — CG torn down; aborting Go (re-)init');
    }
    // Table mode: the sidecar's leaves read SQLite directly and its loadRows
    // is a no-op, so shipping row contents is pure waste — and materializing
    // every user table via `SELECT *` .all() in one synchronous pass OOMs the
    // syncer worker on real datasets (the buffered .all() can exhaust the heap
    // inside Statement::JS_all). Schemas/PKs/uniqueKeys still ship; rows stay
    // empty.
    const skipRows = this.#goBackend?.sidecarSourceMode === 'table';
    const {db} = this.#snapshotter.current();
    const tables: Record<
      string,
      {
        columns: Record<string, {type: 'boolean' | 'number' | 'string' | 'null' | 'json'; optional?: boolean}>;
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
      // restarts. Go-primary mode is
      // safe to skip these: internal queries always route through TS,
      // since TS's TableSource reads live from SQLite and self-heals.
      if (this.#isInternalTable(name)) {
        this.#lc.debug?.(`[go-ivm] skipping internal table ${name}`);
        continue;
      }
      const columns: Record<
        string,
        {type: 'boolean' | 'number' | 'string' | 'null' | 'json'; optional?: boolean}
      > = {};
      for (const [col, colSpec] of Object.entries(spec.tableSpec.columns)) {
        // HIGH-1: forward nullability so Go's nullable-aware SQL (IS NULL /
        // (? IS NULL OR field > ?)) fires for cursor pagination through a NULL
        // on a nullable order column — otherwise Go yields 0 rows where TS
        // yields N. notNull may be true/false/null/undefined; only an explicit
        // true means non-nullable.
        columns[col] = {
          type: pgTypeToGoType(colSpec.dataType, warn),
          optional: colSpec.notNull !== true,
        };
      }
      let rows: Record<string, unknown>[] = [];
      if (!skipRows) {
        try {
          rows = db.all(`SELECT * FROM "${name}"`) as Record<string, unknown>[];
        } catch (e) {
          this.#lc.warn?.(`Failed to read table ${name} for Go init:`, e);
        }
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
    if (this.#goBackend.sidecarSourceMode === 'table') {
      this.#lc.info?.(
        `init ${Object.keys(tables).length} tables (schemas only — ` +
          `table-mode sidecar reads rows from SQLite directly)`,
      );
    } else {
      for (const [name, t] of Object.entries(tables)) {
        this.#lc.info?.(`init table ${name}: ${t.rows.length} rows loaded from SQLite`);
      }
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
    this.#lc.info?.('Resetting Go backend (snapshot leapfrog)');
    // CRIT-5: resetEngine reads the snapshot itself at reinit time (after
    // its destroy await) — do not pre-capture here.
    const promise = this.#goBackend.resetEngine();
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
    // F1: pipelines are gone; the next (re-)registration sets the mode afresh
    // for the live Go state.
    this.#goUserPipelineMode = undefined;
    this.#initAndResetCommon(clientSchema);
    // Re-initialize Go sidecar with fresh snapshot (leapfrog)
    this.#maybeResetGoBackend();
  }

  #initAndResetCommon(clientSchema: ClientSchema) {
    // A (re)init re-hydrates everything — any parked drift-audit heal is
    // moot, and carrying it over would force a needless second reset.
    this.#pendingClientResetReason = null;
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
    // MED-10 (dispatch): cost-model planning is an optimisation, not a
    // correctness requirement — the ordering-completed `planned` AST already
    // runs correctly on Go. A planner fault on a skewed/edge-case schema
    // previously threw out of here and killed the whole hydrate/advance
    // dispatch. Degrade gracefully to the unplanned AST instead, warning once
    // so the gap stays visible.
    try {
      return planQuery(planned, costModel);
    } catch (e) {
      this.#lc.warn?.(
        `[go-ivm] cost-model planning failed; falling back to unplanned ` +
          `ordering for this query`,
        e,
      );
      return planned;
    }
  }

  /**
   * Clears storage used for the pipelines. Call this when the
   * PipelineDriver will no longer be used.
   */
  destroy(): Promise<void> {
    if (this.#driftAuditTimer) {
      clearInterval(this.#driftAuditTimer);
      this.#driftAuditTimer = null;
    }
    this.#storage.destroy();
    this.#snapshotter.destroy();
    // MED-5 (dispatch): await the Go engine teardown rather than fire-and-
    // forget. On a shared sidecar a rapid recycle — a new ViewSyncer for the
    // SAME client group starting before this one's teardown lands — could
    // otherwise race this group's destroy RPC against the new engine's init,
    // tearing down freshly-initialised state. Awaiting serialises destroy
    // before any recreate. Errors are swallowed (teardown is best-effort and
    // the Go side evicts idle groups regardless).
    return this.#goBackend?.destroy().catch(() => {}) ?? Promise.resolve();
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
    // When Go backend is active (non-shadow), hydrate via sidecar (Go-owned
    // stub pipeline). F1: record the build mode so a later Go-availability flip
    // triggers a rebuild instead of a silent freeze / double-emit.
    if (this.#goBackend?.initialized) {
      this.#goUserPipelineMode = 'go';
      return this.#goHydrate(transformationHash, queryID, query);
    }
    // Real TS pipeline. In a Go-primary deployment this is the DEGRADED path
    // (Go unavailable at build time); mark it so advance() serves TS-native and
    // rebuilds Go-owned stubs once Go recovers (F1).
    if (this.#goBackend && !this.#shadowMode) {
      this.#goUserPipelineMode = 'ts';
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
   * Batch hydrate multiple queries via the Go sidecar (Go-primary mode),
   * streaming per-query results AS SOON as Go finishes that query.
   * Tail-latency optimisation: fast queries reach the WebSocket client
   * before slow queries in the same batch complete (REVIEW-final perf-opt
   * streaming).
   *
   * The returned iterable yields entries in COMPLETION order, not input
   * order — callers must not rely on positional correspondence with
   * `queries`.
   */
  async *goHydrateBatchStream(
    queries: {transformationHash: string; queryID: string; ast: AST}[],
  ): AsyncIterable<{
    queryID: string;
    changes: Iterable<RowChange | 'yield'>;
    // Go's per-query engine COMPUTE wall-time (engine.go hydrateEntry,
    // `time.Since(start)` — fetch+materialize, excludes RPC/serialize).
    // Surfaced so the view-syncer records the engine-compute span into
    // hydration_time (apples-to-apples with TS) instead of the TS-side
    // consumption of already-computed rows. `undefined` for internal
    // queries (lmids/clients/permissions) that run through TS's
    // #addQueryImpl — their compute is lazy in the yielded generator, so
    // the consumer times it during drain.
    timingMs: number | undefined;
    // In per-chunk mode, true only on a query's terminal chunk; the
    // view-syncer gates once-per-query metric recording on it. Always true in
    // the default (accumulate-to-final) mode and for internal TS queries.
    final: boolean;
  }> {
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
      yield {queryID: q.queryID, changes: dropYields(), timingMs: undefined, final: true};
    }

    if (userQueries.length === 0) {
      return;
    }
    // F1: about to register Go-owned user stubs — record the build mode.
    this.#goUserPipelineMode = 'go';

    // Buffer arrived-but-not-yet-yielded results from the streaming RPC.
    // The producer side runs in goroutines on Go; we get one onResult call
    // per query via the client's onPartial. We park each into a queue and
    // wake the async iterator's resolver.
    //
    // BACKPRESSURE: the socket delivers results as fast as Go produces
    // them, while this iterator's consumer drains one query at a time into
    // CVR flushes — far slower. A single hydrateManyStream over the whole
    // query set would buffer the CG's ENTIRE hydrate result set in heap
    // (the Go-primary twin of the shadow-collection OOM). Sub-batch the
    // RPC instead: at most GO_HYDRATE_SUB_BATCH queries' results are
    // buffered at once, and the next sub-batch isn't requested until the
    // previous one is fully consumed. Costs little parallelism: in drive
    // mode the per-CG hydrate is serialized on the snapshotter's single
    // conn anyway.
    type Entry = {queryID: string; changes: RowChange[]; timingMs: number | undefined; final: boolean};

    const byQueryID = new Map<string, (typeof queries)[number]>();
    for (const q of userQueries) byQueryID.set(q.queryID, q);

    for (
      let batchStart = 0;
      batchStart < userQueries.length;
      batchStart += GO_HYDRATE_SUB_BATCH
    ) {
      const subBatch = userQueries.slice(
        batchStart,
        batchStart + GO_HYDRATE_SUB_BATCH,
      );
      const buffered: Entry[] = [];
      let wake: (() => void) | null = null;
      let done = false;
      let error: Error | null = null;

      const rpcPromise = this.#goBackend!.hydrateManyStream(
        subBatch.map(q => ({queryID: q.queryID, ast: this.#planAstForGo(q.ast)})),
        (r: {queryID: string; changes: unknown[]; timingMs: number | undefined; final?: boolean; chunkIndex?: number}) => {
          buffered.push({
            queryID: r.queryID,
            changes: (r.changes ?? []) as RowChange[],
            timingMs: r.timingMs,
            // Default (non-chunked) mode fires onResult once per query with
            // `final` omitted → treat as terminal. Chunked mode carries the
            // real per-frame flag so the consumer can gate metrics.
            final: r.final ?? true,
          });
          wake?.();
          wake = null;
        },
        {chunked: GO_PERCHUNK_HYDRATE},
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
          // Register the Go-owned stub once per query so it exists while the
          // query's chunks stream, then overwrite with the real engine-compute
          // time on the terminal chunk (only the final frame carries timingMs).
          // In the default (non-chunked) path a query has exactly one entry
          // with final=true, so this collapses to the original single set().
          if (!this.#pipelines.has(q.queryID) || r.final) {
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
          }
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
            timingMs: r.timingMs,
            final: r.final,
          };
        }
        if (done) break;
      }
      await rpcPromise;
    }
  }

  /** Whether batch hydration is available (Go-primary, non-shadow). */
  get canBatchHydrate(): boolean {
    return !!(this.#goBackend?.initialized && !this.#shadowMode);
  }

  /**
   * Whether the view-syncer should collect per-query TS hydrate results
   * for {@link shadowBatchCompare}. False in Go-primary, Go-disabled, or
   * non-shadow deployments — collecting unconditionally retained every
   * CG's ENTIRE hydrate result set in the JS heap on each (re)connect,
   * which is a reconnect-storm OOM at production scale.
   */
  get shadowCompareActive(): boolean {
    return this.#shadowMode && this.#goBackend !== null;
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
    tsResultsPerQuery: Map<string, ShadowHydrateResult>,
  ): Promise<void> {
    // HIGH-5: this is a SHADOW-only comparison. It previously returned only on
    // !initialized, so in Go-primary it ran a second full hydrateManyStream
    // against Go (Go-vs-Go, always matches) — wasted work. It also used to be
    // load-bearing because its #planAstForGo created the TableSources that the
    // per-query #goHydrate path skipped (CRIT-3); that's fixed (#goHydrate now
    // plans the AST itself), so gating on shadow mode no longer re-exposes the
    // getRow panic. In Go-primary, Go IS the source of truth — nothing to
    // compare against.
    if (!this.#shadowMode || !this.#goBackend?.initialized) return;
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
      // Retain only per-query COUNTS for Go results — the full change arrays
      // were previously kept for the whole batch ("goResultsByID") even
      // though they were only read inside the per-query callback. At
      // production result-set sizes that doubled the batch's heap footprint
      // for no benefit.
      const goCountsByID = new Map<string, number>();
      // queryID → original (pre-planForGo) AST, so #shadowCompare's result-ORDER
      // check has the orderBy without depending on #pipelines registration.
      const astByID = new Map(queries.map(q => [q.queryID, q.ast]));
      let mismatches = 0;
      await this.#goBackend.hydrateManyStream(
        queries.map(q => ({queryID: q.queryID, ast: this.#planAstForGo(q.ast)})),
        r => {
          const goChanges = (r.changes ?? []).map(rc =>
            this.#goRowChangeToRowChange(rc as GoRowChange),
          );
          // Phase 2: no TS-side companion injection here. Go's resolver
          // emits companion rows itself if any scalar subquery resolved.
          goCountsByID.set(r.queryID, goChanges.length);
          const ts = tsResultsPerQuery.get(r.queryID) ?? {changes: [], total: 0};
          // Free the TS-side buffer as soon as this query is compared —
          // no need to hold the whole batch's results until the RPC ends.
          tsResultsPerQuery.delete(r.queryID);
          if (ts.total > ts.changes.length) {
            // TS side was truncated at SHADOW_COMPARE_ROW_CAP — a content
            // compare is impossible; degrade to a count compare.
            if (ts.total !== goChanges.length) {
              this.#lc.error?.(
                `[shadow] batch-hydrate (${r.queryID}): COUNT mismatch on ` +
                  `capped result (ts=${ts.total} go=${goChanges.length}, ` +
                  `cap=${SHADOW_COMPARE_ROW_CAP})`,
              );
              mismatches++;
            } else {
              this.#lc.info?.(
                `[shadow] batch-hydrate (${r.queryID}): count-only compare ` +
                  `(${ts.total} rows > cap ${SHADOW_COMPARE_ROW_CAP}) — counts match`,
              );
            }
            return;
          }
          this.#shadowCompare(
            `batch-hydrate`,
            r.queryID,
            ts.changes,
            goChanges,
            astByID.get(r.queryID),
          );
          if (ts.changes.length !== goChanges.length) mismatches++;
        },
      );
      const batchMs = performance.now() - batchStart;
      // Account for queries Go never emitted (size mismatch detected).
      for (const q of queries) {
        if (!goCountsByID.has(q.queryID)) {
          const ts = tsResultsPerQuery.get(q.queryID) ?? {changes: [], total: 0};
          tsResultsPerQuery.delete(q.queryID);
          this.#shadowCompare('batch-hydrate', q.queryID, ts.changes, [], q.ast);
          if (ts.total !== 0) mismatches++;
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
  // Known false-positive:
  //   Paginated `conversations` queries with `EXISTS(channels)` +
  //   `start.exclusive: false` + a cursor row whose createdAt EXACTLY
  //   matches `start.row.createdAt` can report ts=N go=N MISMATCH, with
  //   the [drift-audit][rowdiff] log showing TS missing the boundary row
  //   and including one extra older row. A direct
  //   `SELECT … WHERE createdAt <= cursor ORDER BY createdAt DESC LIMIT N`
  //   on the replica returns the boundary row at position 0, so the
  //   divergence is between SQL and the audit's RowChange emission — NOT
  //   in Go's MemorySource (Go gets the boundary right). Ruled out:
  //     - Resolver asymmetry (drift fires whether or not scalar EXISTS
  //       resolved)
  //     - View-syncer instance restart (drifting CGs had a single instance)
  //     - createdAt ties (none present at the cursor)
  //     - Snapshot skew (versionBefore === versionAfter)
  //   Suspect: the Skip → Exists → Take interaction on the audit's
  //   re-hydrate path, possibly companion-pipeline state from the executor
  //   in #resolveScalarSubqueries holding a stale Connection on the
  //   conversations TableSource. The [drift-audit][rowdiff] log line points
  //   to the divergent row.
  async #runDriftAudit(): Promise<void> {
    // Unconditional tick counter — fires before any guards so operators
    // can distinguish "audit timer never fired" (broken sidecar config,
    // missed setInterval, etc.) from "audit fired but skipped" (legitimate
    // busy / idle CG). Without this, an audit that's silently disabled
    // looks identical in metrics to a healthy one running against an idle
    // CG (both stuck at runs=0) — e.g. a sidecar that refused to start
    // leaves drift-audit silent for hours with nothing in INFO logs to
    // surface it. Increment this BEFORE the InFlight guard so even
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
      // MED-7 (dispatch): count ONLY the queries Go actually registers a
      // pipeline for. Queries rooted at an internal table always run via TS
      // and are never sent to Go (see #goHydrate / addQueries gate above), so
      // Go's pipelineCount() excludes them. The old filter dropped only
      // internal *query IDs* — an internal-table-rooted query then inflated
      // tsExpected past goCount and tripped a FALSE freeze, firing a spurious
      // resetEngine (the CRIT-5 drift-loop amplifier). Match the exact
      // predicate auditableIDs uses below.
      const tsExpected = [...this.#pipelines.entries()].filter(
        ([qid, entry]) =>
          !this.#isInternalQueryID(qid) &&
          !this.#isInternalTable(entry.transformedAst.table),
      ).length;
      if (goCount >= 0 && goCount < tsExpected) {
        // Grace: a shortfall on a SINGLE audit is almost always async
        // registration lag (addQueriesStream in flight), which self-resolves by
        // the next cycle. Only a shortfall that PERSISTS across
        // DRIFT_COUNT_MISMATCH_GRACE consecutive audits is a real freeze worth a
        // full resetEngine. This avoids needless re-registrations on
        // transient single-cycle shortfalls while still self-healing a
        // genuine freeze one cycle later.
        this.#driftCountMismatchStreak++;
        if (this.#driftCountMismatchStreak < DRIFT_COUNT_MISMATCH_GRACE) {
          this.#lc.debug?.(
            `[drift-audit] pipeline-count shortfall TS=${tsExpected} Go=${goCount} ` +
              `(streak ${this.#driftCountMismatchStreak}/${DRIFT_COUNT_MISMATCH_GRACE}) — ` +
              `likely registration lag; deferring reset`,
          );
          return;
        }
        this.#driftCountMismatchStreak = 0;
        this.#driftAuditFreezes.add(1);
        this.#lc.error?.(
          `[drift-audit] FREEZE detected: TS=${tsExpected} queries registered, ` +
            `Go=${goCount} pipelines for ${DRIFT_COUNT_MISMATCH_GRACE} consecutive ` +
            `audits. Per-CG recovery dropped pipeline state. ` +
            `Triggering resetEngine to self-heal.`,
        );
        // Fire-and-forget. resetEngine handles its own concurrency via
        // the gate; if it's already running, this is a no-op via the
        // gate await in #reinitPerCGAndRegisterQueries.
        void this.#scheduleGoReset('drift-audit-pipeline-count-mismatch');
        return;
      }
      // Go's count caught up (or overtook) — clear any pending shortfall streak
      // so a fresh transient lag later starts its grace window from zero. Only
      // on a SUCCESSFUL probe (goCount >= 0); a failed probe (-1) is "unknown"
      // and must not clear a genuine persisting shortfall.
      if (goCount >= 0) {
        this.#driftCountMismatchStreak = 0;
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
        .map(([qid]) => qid)
        // Stable order so the round-robin cursor maps to a consistent query
        // across cycles even as the pipeline set changes.
        .sort();
      if (auditableIDs.length === 0) return;

      // Round-robin instead of random pick: a uniform-random choice leaves most
      // queries un-audited for long stretches (coupon-collector: ~N·lnN cycles
      // to hit all N), so a query-specific divergence can hide for many minutes.
      // Cycling the cursor guarantees every active query is audited within
      // auditableIDs.length cycles. (`%` re-bounds when the set shrank.)
      const targetID = auditableIDs[this.#driftAuditCursor % auditableIDs.length];
      this.#driftAuditCursor++;
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

      this.#shadowCompare('drift-audit', targetID, tsRemapped, goRemapped, ast);

      // Item #5: record which column types this audited shape sorts/filters on,
      // and periodically log coverage gaps.
      this.#recordAuditTypeCoverage(ast);

      // Item #1: multiplicity (multiset) compare. #shadowCompare and the SQL
      // checks are PK/rowKey-keyed; a fan-out that emits a (table,rowKey) a
      // different NUMBER of times on each side nets out. Compare the full bags
      // with TS as the multiplicity oracle (the single-table SQL can't model
      // join cardinality).
      //
      // Restricted to TRUE cardinality divergences (a row emitted >=2x on one
      // side): a 1-vs-0 difference is plain SET membership, already covered by
      // the set check + SQL classifier — and it's how the TS conversations
      // boundary-drop shows up here (TS x1 Go x0), which is a TS bug, not a Go
      // cardinality drift. Without this gate the metric is inflated by TS's own
      // pagination bug.
      {
        const bagKey = (c: RowChange) => `${c.table}|${stableStringify(c.rowKey)}`;
        const multDiffs = multisetDiff(
          tsRemapped.map(bagKey),
          goRemapped.map(bagKey),
        ).filter(d => Math.max(d.aCount, d.bCount) >= 2);
        if (multDiffs.length > 0) {
          this.#driftAuditMultiplicityMismatches.add(multDiffs.length);
          const sample = multDiffs
            .slice(0, 5)
            .map(d => `${d.key}: TS×${d.aCount} Go×${d.bCount}`)
            .join(' ; ');
          this.#lc.error?.(
            `[drift-audit][multiplicity] ${targetID}: ${multDiffs.length} row(s) ` +
              `with divergent emission count — ${sample}`,
          );
        }
      }

      // Item #2: reconcile Go's ACCUMULATED advance deltas against this fresh
      // full hydrate (goRemapped is all-ADD, so its main-table PK→content map is
      // Go's current full materialization). A divergence means some advance
      // emitted a wrong delta that a from-scratch re-hydrate masks — the
      // incremental operator path disagreeing with the hydrate path. The existing
      // SQL check separately ties this hydrate to ground truth, so
      // accum==hydrate==SQL gives accum==SQL transitively. Skip when tainted;
      // always RE-SEED after.
      //
      // #4b: the accumulator stores CONTENT (PK→projected row), not just PK
      // membership, so the reconcile diffs BOTH — `accumOnly`/`hydrateOnly`
      // (membership, the original item-#2 check) AND `contentMismatch` (same PK,
      // differing stored content vs the fresh hydrate row). The content check
      // closes the between-cycle window: a same-PK content drift that persists
      // across audit cycles with no ADD/REMOVE touching the PK kept stable
      // membership in the old PK-set accumulator and slipped through; #shadowCompare
      // catches content drift per-batch but only WITHIN an advance.
      {
        const spec = this.#tableSpecs.get(ast.table);
        // Seed: PK → projected row content (schema columns only, matching the
        // accumulator's #accumulateGoDelta projection).
        const goHydrateByPK = new Map<string, string>();
        for (const c of goRemapped) {
          if (c.table !== ast.table) continue;
          const pk = stableStringify(c.rowKey);
          let rowStr: string;
          if (spec) {
            const projected: Record<string, unknown> = {};
            for (const col of Object.keys(spec.zqlSpec)) projected[col] = c.row[col];
            rowStr = stableStringify(projected);
          } else {
            rowStr = stableStringify(c.row);
          }
          goHydrateByPK.set(pk, rowStr);
        }
        const accum = this.#goDeltaAccum.get(targetID);
        if (accum && !this.#goDeltaAccumDirty.has(targetID)) {
          const accumOnly: string[] = [];
          const hydrateOnly: string[] = [];
          const contentMismatch: {pk: string; accum: string; hydrate: string}[] = [];
          for (const [k, accumRow] of accum) {
            const hydrateRow = goHydrateByPK.get(k);
            if (hydrateRow === undefined) {
              accumOnly.push(k);
            } else if (hydrateRow !== accumRow) {
              // Same PK, differing content — the between-cycle content drift
              // the PK-set-only accumulator missed (#4b).
              contentMismatch.push({pk: k, accum: accumRow, hydrate: hydrateRow});
            }
          }
          for (const k of goHydrateByPK.keys()) if (!accum.has(k)) hydrateOnly.push(k);
          if (accumOnly.length > 0 || hydrateOnly.length > 0) {
            this.#driftAuditIncrementalMismatches.add(1);
            this.#lc.error?.(
              `[drift-audit][incremental] ${targetID}: REAL DRIFT — Go advance-delta ` +
                `accumulation diverged from a fresh hydrate. ` +
                `accum_only=${accumOnly.slice(0, 3).join(' | ')} (${accumOnly.length}) ` +
                `hydrate_only=${hydrateOnly.slice(0, 3).join(' | ')} (${hydrateOnly.length}) ` +
                `version=${versionAfter} ast=${JSON.stringify(ast)}`,
            );
          }
          if (contentMismatch.length > 0) {
            this.#driftAuditIncrementalContentMismatches.add(contentMismatch.length);
            const sample = contentMismatch
              .slice(0, 3)
              .map(m => `pk=${m.pk.slice(0, 40)}`)
              .join(' ; ');
            this.#lc.error?.(
              `[drift-audit][incremental-content] ${targetID}: REAL DRIFT — Go ` +
                `advance-delta accumulated content diverged from a fresh hydrate on ` +
                `${contentMismatch.length} same-PK row(s) (${sample}) — between-cycle ` +
                `content drift with stable membership; version=${versionAfter}`,
            );
          }
        }
        this.#goDeltaAccum.set(targetID, goHydrateByPK);
        this.#goDeltaAccumDirty.delete(targetID);
      }

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
        // Two boundary shapes — a classifier that only matches the first
        // sends the second to "REAL DRIFT" and pages on-call for what is a
        // known-benign semantic difference (e.g. ts=51 go=51 sql=50,
        // asymmetric with goOnly=1, sqlOnly=0).
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
        if (!setDiffers) {
          // Both IVM engines agree (TS == Go) but SQL alone disagrees.
          // The single-table SQL oracle can't model IVM's
          // EXISTS+cursor+LIMIT boundary semantics exactly (it has its own
          // predicate boundaries), so any divergence here is an oracle
          // limitation, NOT a Go drift. Demote unconditionally — the
          // previous narrower gate (asymmetric/symmetric 1-off) missed
          // cases where IVM's EXISTS filter shifted the boundary by >1 row
          // (e.g., ts=101 go=101 sql=50 on conversations with
          // EXISTS(channels) + exclusive cursor). Bug #3 class.
          this.#lc.info?.(
            `[drift-audit] ${targetID}: ivm-boundary divergence ` +
              `(TS IVM and Go IVM agree, SQL alone disagrees by ` +
              `${goOnly}+${sqlOnly} rows). ` +
              `ts=${tsRemapped.length} go=${goRemapped.length} sql=${sqlVerdict.sqlCount}`,
          );
        } else if (
          ast.limit !== undefined &&
          ast.start !== undefined &&
          (asymmetricBoundary || symmetricBoundary)
        ) {
          // TS and Go disagree, but the SQL divergence is a 1-row
          // boundary swap on a paginated query — pagination semantics,
          // not real drift.
          this.#lc.info?.(
            `[drift-audit] ${targetID}: ivm-boundary divergence ` +
              `(paginated query, ${asymmetricBoundary ? 'asymmetric' : 'symmetric'} ` +
              `1-off). ts=${tsRemapped.length} go=${goRemapped.length} ` +
              `sql=${sqlVerdict.sqlCount}`,
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
          this.#healConfirmedDrift('drift-audit-confirmed-set-drift');
        }
      } else if (sqlVerdict.kind === 'go-vs-sql-content-drift') {
        // Same PKs but row contents differ — Go has stale/wrong values
        // for one or more rows. This is the most insidious bug class
        // (users see wrong data without any visible error), so escalate.
        if (!setDiffers) {
          // Both IVM engines have the same PK set — overwhelmingly likely
          // they also agree on content (same pipeline, same snapshot). The
          // SQL oracle's simple SELECT can disagree on value encoding (e.g.,
          // timestamp epoch vs ISO, boolean int vs bool) or on IVM-specific
          // transforms that the flat oracle doesn't replicate. Demote.
          this.#lc.info?.(
            `[drift-audit] ${targetID}: ivm-boundary divergence (content) ` +
              `(TS IVM and Go IVM agree on PK set, SQL alone disagrees on ` +
              `${sqlVerdict.contentMismatches.length} row value(s)). ` +
              `sql_count=${sqlVerdict.sqlCount}`,
          );
        } else {
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
          this.#healConfirmedDrift('drift-audit-confirmed-content-drift');
        }
      } else if (sqlVerdict.kind === 'go-vs-sql-order-drift') {
        // Right rows, WRONG ORDER vs the SQL ORDER BY oracle. Invisible to the
        // set/content checks and to #shadowCompare — and a real client-visible
        // bug for ordered queries (the user's list is in the wrong sequence).
        // Counted in its own metric so order bugs are distinguishable from
        // row/value drift on dashboards.
        if (!setDiffers) {
          // Both IVM engines have the same PK set — their cursor-influenced
          // pipeline ordering is identical. The SQL oracle's ORDER BY can
          // disagree because IVM pipelines produce rows in cursor-relative
          // order (e.g., appended rows after the cursor position) while the
          // flat SQL ORDER BY re-sorts the full result. Demote.
          const at = sqlVerdict.orderDiffAt;
          this.#lc.info?.(
            `[drift-audit] ${targetID}: ivm-boundary divergence (order) ` +
              `(TS IVM and Go IVM agree on PK set, SQL alone disagrees on ` +
              `order at position ${at}). sql_count=${sqlVerdict.sqlCount}`,
          );
        } else {
          this.#driftAuditOrderMismatches.add(1);
          const at = sqlVerdict.orderDiffAt;
          const window = (seq: string[]) =>
            seq.slice(Math.max(0, at - 1), at + 2).join(' > ');
          this.#lc.error?.(
            `[drift-audit] ${targetID}: REAL DRIFT — Go row ORDER disagrees with ` +
              `SQL ORDER BY at position ${at} (sql_count=${sqlVerdict.sqlCount}). ` +
              `sql_seq=[…${window(sqlVerdict.sqlSeq)}…] ` +
              `go_seq=[…${window(sqlVerdict.goSeq)}…] ast=${JSON.stringify(ast.orderBy)}`,
          );
          this.#healConfirmedDrift('drift-audit-confirmed-order-drift');
        }
      } else if (sqlVerdict.kind === 'go-vs-sql-tie-window') {
        // Ordered+limited query whose ORDER BY lacks a unique tiebreaker: Go
        // and SQL picked different members of a tie group straddling the LIMIT
        // boundary, but both windows carry the SAME ordered key VALUES. Not a
        // bug — nondeterministic window membership (real Zero appends the PK so
        // this can't happen in prod; raw audit ASTs may omit it). Info-level.
        this.#lc.info?.(
          `[drift-audit] ${targetID}: tie-window (nondeterministic LIMIT boundary, ` +
            `same ordered key values, ${sqlVerdict.goOnly.length} tie members swapped). ` +
            `go=${goRemapped.length} sql=${sqlVerdict.sqlCount} ast=${JSON.stringify(ast.orderBy)}`,
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
    | {kind: 'go-vs-sql-tie-window'; sqlCount: number; goOnly: string[]; sqlOnly: string[]}
    | {
        kind: 'go-vs-sql-content-drift';
        sqlCount: number;
        contentMismatches: {pk: string; sqlRow: string; goRow: string}[];
      }
    | {
        kind: 'go-vs-sql-order-drift';
        sqlCount: number;
        orderDiffAt: number;
        sqlSeq: string[];
        goSeq: string[];
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

    // Stream rows with a hard cap instead of .all(): buildAuditSQL only
    // emits a LIMIT when the AST has one, so an unlimited query over a
    // production-sized table would otherwise materialize the whole table
    // as JS objects and can exhaust the heap inside Statement::JS_all.
    // Overflow returns the existing `skipped` shape; callers fall back to
    // the TS-vs-Go set comparison.
    let sqlRows: Record<string, unknown>[];
    try {
      sqlRows = [];
      const stmt = this.#snapshotter.current().db.db.prepare(limitedText);
      for (const row of stmt.iterate(...values)) {
        if (sqlRows.length >= SQL_ORACLE_ROW_CAP) {
          // Early return triggers the iterator's return() — better-sqlite3
          // resets the statement, so no open-cursor leak.
          return {
            kind: 'skipped',
            reason: `row-cap-exceeded: >${SQL_ORACLE_ROW_CAP} rows`,
          };
        }
        sqlRows.push(row as Record<string, unknown>);
      }
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

    // ORDER BY key-tuple of a row (used by the tie-window set check and the
    // tie-aware order check). Real Zero appends the PK so the sort is total;
    // raw audit ASTs may not, so equal key tuples = an unordered tie group.
    const orderFields = (ast.orderBy ?? []).map(([fld]) => fld);
    const keyTupleOf = (row: Record<string, unknown>) =>
      stableStringify(orderFields.map(fld => row[fld]));

    // Step 1: set diff (PK presence)
    const goOnly: string[] = [];
    const sqlOnly: string[] = [];
    for (const k of goByPK.keys()) if (!sqlByPK.has(k)) goOnly.push(k);
    for (const k of sqlByPK.keys()) if (!goByPK.has(k)) sqlOnly.push(k);

    if (goOnly.length > 0 || sqlOnly.length > 0) {
      // Tie-window benign case: an ordered + LIMITED query whose ORDER BY lacks
      // a unique tiebreaker has NONDETERMINISTIC window membership when a tie
      // group straddles the LIMIT boundary — Go and SQL legitimately pick
      // different members of that tie. It's benign IFF every differing row is a
      // tie member: its ORDER BY key-VALUE appears in BOTH windows (so the row
      // is interchangeable with one the other side kept). With NO orderBy the
      // whole limited result is one unordered "tie" (every row's empty key
      // tuple matches), so a no-orderBy LIMIT diff is benign too. The LIMIT
      // guard is load-bearing: without a limit both engines return the full
      // filtered set, so any set diff there is a REAL bug, not a tie. (Real Zero
      // appends the PK making the sort total; raw audit ASTs may omit it.)
      if (resolved.limit !== undefined) {
        const sqlKeysPresent = new Set(normalizedSqlRows.map(keyTupleOf));
        const goKeysPresent = new Set(Array.from(goByPK.values(), keyTupleOf));
        const isTieMember = (row: Record<string, unknown> | undefined) => {
          if (!row) return false;
          const k = keyTupleOf(row);
          return sqlKeysPresent.has(k) && goKeysPresent.has(k);
        };
        const allTies =
          goOnly.every(pk => isTieMember(goByPK.get(pk))) &&
          sqlOnly.every(pk => isTieMember(sqlByPK.get(pk)));
        if (allTies) {
          return {kind: 'go-vs-sql-tie-window', sqlCount: sqlByPK.size, goOnly, sqlOnly};
        }
      }
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

    // Step 3: ORDER diff. The set + content checks above are PK-keyed, so they
    // pass even when Go returns the right rows in the WRONG sequence — e.g. an
    // enum column sorted by TEXT collation instead of PG definition order, a
    // NULL-ordering or multi-key tie-break divergence. #shadowCompare can't see
    // it either (it sorts both sides by rowKey). `normalizedSqlRows` is already
    // in the AST's ORDER BY order (the true oracle); `goRemapped` is Go's
    // emission order. Compare the main-table PK sequences positionally.
    //
    // Gate: only when the query has an EXPLICIT orderBy (otherwise result order
    // is semantically undefined and both engines' arbitrary orders are valid).
    // Sets are known EQUAL here (we'd have returned go-vs-sql-drift otherwise),
    // so cursor-paginated queries (`ast.start`) ARE checked too (item #4):
    // buildAuditSQL applies the same cursor predicate + ORDER BY + LIMIT, so
    // `normalizedSqlRows` is the correct ordered WINDOW, and the off-by-one
    // boundary case the caller special-cases only arises when sets DIFFER —
    // which never reaches here. The length-equal guard below keeps it sound if
    // an IVM boundary row still slips one side's count.
    if (orderFields.length > 0) {
      // Compare the ORDER BY key-VALUE sequences, NOT the PK sequences: a
      // reordering WITHIN a tie group (equal key tuples) is a semantically
      // valid permutation and must not flag. Only a position where the actual
      // key VALUES differ is a real sort violation (enum/collation/NULL-order/
      // direction bug). This is what makes the order check sound on sorts that
      // lack a unique tiebreaker.
      const sqlSeq = normalizedSqlRows.map(keyTupleOf);
      const goSeq = goRemapped
        .filter(c => c.table === resolved.table)
        .map(c => keyTupleOf(c.row));
      if (sqlSeq.length === goSeq.length) {
        for (let i = 0; i < sqlSeq.length; i++) {
          if (sqlSeq[i] !== goSeq[i]) {
            return {
              kind: 'go-vs-sql-order-drift',
              sqlCount: sqlByPK.size,
              orderDiffAt: i,
              sqlSeq,
              goSeq,
            };
          }
        }
      }
    }

    return {kind: 'confirmed', sqlCount: sqlByPK.size};
  }

  /**
   * SQL ground-truth oracle for the ADVANCE path — the delta-derived analog of
   * {@link #sqlGroundTruthCompare}. The hydrate oracle compares Go's point-in-
   * time materialization to a single SQL SELECT on the replica; the advance
   * path compares a DELTA (Go's emitted RowChange[] for one advance) to the
   * SQL-derived delta between the snapshotter's `prev` and `curr` snapshots
   * (both pinned via BEGIN CONCURRENT and live through #shadowAdvance — see
   * snapshotter.ts:177, valid "until the next call to advance()").
   *
   * For one query (caller groups Go's advance changes by queryID and looks up
   * the AST via #pipelines), this:
   *   1. buildAuditSQL(ast) → flat SELECT (reused unchanged — handles EXISTS).
   *   2. Runs it on BOTH prevDb and currDb (streamed + SQL_ORACLE_ROW_CAP).
   *   3. Normalizes via fromSQLiteTypes, builds prevByPK / currByPK.
   *   4. Derives the expected delta: curr\prev → ADD, prev\curr → REMOVE,
   *      intersect → content-diff → EDIT.
   *   5. Compares to Go's main-table changes for this query (filter
   *      c.table === ast.table, same as #sqlGroundTruthCompare :2914).
   *
   * Returns the SAME verdict shape as #sqlGroundTruthCompare so the existing
   * classify logic (ts-only / oracle-blind / go-vs-sql-drift / skipped) in
   * #shadowCompare's advance branch reuses it verbatim. Order is MEANINGLESS
   * for a delta bag (a RowChange[] is not a result sequence — ADD/EDIT/REMOVE
   * is a set), so unlike the hydrate oracle this skips the ORDER stage; the
   * advance op-kind check at :~4514 already catches edit-vs-add+remove wire
   * differences separately. MAIN-TABLE-ONLY caveat is inherited from the
   * hydrate oracle (buildAuditSQL returns outer-table rows only); the
   * `oracle-blind` fall-through handles off-table (fan-out) divergences.
   */
  #sqlGroundTruthAdvanceCompare(
    ast: AST,
    prevDb: StatementRunner,
    currDb: StatementRunner,
    goChanges: RowChange[],
  ): AdvanceSqlOracleVerdict {
    if (!goDriftAuditSqlGroundTruth(this.#config)) {
      return {kind: 'skipped', reason: 'disabled'};
    }
    const spec = this.#tableSpecs.get(ast.table);
    if (!spec) return {kind: 'skipped', reason: 'no-table-spec'};

    // Same buildAuditSQL path as the hydrate oracle.
    let text: string;
    let values: unknown[];
    try {
      const result = buildAuditSQL(ast, this.#tableSpecs);
      text = result.text;
      values = result.values;
    } catch (e) {
      return {kind: 'skipped', reason: `build-sql-failed: ${String(e)}`};
    }

    // Stream rows from BOTH endpoints with the same cap + early-return guard
    // as the hydrate oracle (:2872-2888). prevDb/currDb are pinned BEGIN
    // CONCURRENT handles (snapshotter.ts:248) — readable concurrently with
    // the advance that produced them.
    const queryRows = (db: StatementRunner): {
      rows: Record<string, unknown>[] | null;
      reason?: string;
    } => {
      const rows: Record<string, unknown>[] = [];
      try {
        const stmt = db.db.prepare(text);
        for (const row of stmt.iterate(...values)) {
          if (rows.length >= SQL_ORACLE_ROW_CAP) {
            return {rows: null, reason: `row-cap-exceeded: >${SQL_ORACLE_ROW_CAP} rows`};
          }
          rows.push(row as Record<string, unknown>);
        }
      } catch (e) {
        return {rows: null, reason: `sql-exec-failed: ${String(e)}`};
      }
      return {rows};
    };

    const prevQ = queryRows(prevDb);
    if (prevQ.rows === null) return {kind: 'skipped', reason: `prev: ${prevQ.reason}`};
    const currQ = queryRows(currDb);
    if (currQ.rows === null) return {kind: 'skipped', reason: `curr: ${currQ.reason}`};

    // Normalize to Go's row encoding shape (BigInt→Number, JSON parse, etc.).
    let prevRows: Record<string, unknown>[];
    let currRows: Record<string, unknown>[];
    try {
      prevRows = prevQ.rows.map(
        row => fromSQLiteTypes(spec.zqlSpec, row as Row, ast.table) as Record<string, unknown>,
      );
      currRows = currQ.rows.map(
        row => fromSQLiteTypes(spec.zqlSpec, row as Row, ast.table) as Record<string, unknown>,
      );
    } catch (e) {
      return {kind: 'skipped', reason: `normalize-failed: ${String(e)}`};
    }

    // Delegate the delta-derivation + comparison to the exported pure core
    // (unit-tested directly via compareAdvanceDeltaToSqlDelta). The wrapper
    // above owns the SQL I/O + normalization; this owns the verdict logic.
    return compareAdvanceDeltaToSqlDelta(
      ast.table,
      spec.tableSpec.primaryKey,
      Object.keys(spec.zqlSpec),
      prevRows,
      currRows,
      goChanges,
    );
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
    // Include the table name in the error message. Without it a bare-must()
    // failure during CVR catchup ("Unexpected undefined value") fires before
    // ViewSyncer's outer must can wrap it with "Missing row ...", obscuring
    // which table source went missing. Suspect: query removed mid-CVR-catchup
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
  advance(
    timer: Timer,
  ):
    | AdvanceResult
    | ResetPipelinesSignal
    | Promise<AdvanceResult | ResetPipelinesSignal> {
    assert(
      this.initialized(),
      'Pipeline driver must be initialized before advancing',
    );
    // Drift-audit heal, part 2 (see #healConfirmedDrift): the audit runs in
    // a detached setInterval and structurally cannot return a
    // ResetPipelinesSignal itself, so it parks the request here and the
    // next advance returns it — driving the view-syncer's proven reset path
    // (pipelines.reset → hydrateUnchangedQueries → CVR updater) that
    // corrects rows already delivered to clients.
    if (this.#pendingClientResetReason !== null) {
      const why = this.#pendingClientResetReason;
      this.#pendingClientResetReason = null;
      return new ResetPipelinesSignal(
        `drift-audit confirmed client-visible drift (${why}) — ` +
          `full re-hydrate to heal delivered rows`,
        'drift-audit-heal',
      );
    }
    // If Go backend init is pending, await it first
    if (this.#goInitPromise && this.#goBackend && !this.#goBackend.initialized) {
      return this.#goInitPromise.then(() => this.#advanceDispatch(timer));
    }
    return this.#advanceDispatch(timer);
  }

  #advanceDispatch(
    timer: Timer,
  ):
    | AdvanceResult
    | ResetPipelinesSignal
    | Promise<AdvanceResult | ResetPipelinesSignal> {
    const diff = this.#snapshotter.advance(
      this.#tableSpecs,
      this.#allTableNames,
    );
    const {prev, curr, changes} = diff;
    this.#lc.debug?.(
      `advance ${prev.version} => ${curr.version}: ${changes} changes`,
    );

    // Shadow mode: run TS path as source of truth, also run Go, compare.
    // Shadow always builds real TS user pipelines (#shadowAddQuery) and serves
    // via #shadowAdvance, so the Go-primary mode reconciliation below does not
    // apply to it.
    if (this.#shadowMode && this.#goBackend?.initialized) {
      return this.#shadowAdvance(diff, timer, curr.version, changes);
    }

    // Non-shadow Go-primary: reconcile the LIVE Go availability against the
    // mode the current user pipelines were built in (#goUserPipelineMode). A
    // flip in either direction must rebuild the pipelines (via a returned
    // ResetPipelinesSignal) before we can safely advance. See
    // decideGoPrimaryDispatch for the full decision matrix and rationale.
    if (this.#goBackend && !this.#shadowMode) {
      switch (
        decideGoPrimaryDispatch(
          this.#goBackend.initialized,
          this.#goUserPipelineMode,
        )
      ) {
        case 'go-advance':
          // Go-primary: dual-run TS + Go on disjoint table sets. Go gets the
          // diff with internal tables filtered out and handles user queries.
          // TS handles internal queries (lmids, mutationResults) via its real
          // #addQueryImpl pipelines. User-query pipelines in TS are stubs (no
          // setOutput callback), so TS's #advance walks the full diff but only
          // emits for internal queries. Merging is safe (table-disjoint sets).
          return this.#goPrimaryAdvance(diff, timer, curr.version, changes);
        case 'reset-recovered':
          // Go recovered but user pipelines were degraded to real TS while it
          // was down — running #goPrimaryAdvance now would DOUBLE-emit user
          // rows (TS's real pipelines AND Go both emit). Rebuild as stubs.
          return new ResetPipelinesSignal(
            'Go backend recovered; rebuilding Go-owned pipelines ' +
              '(were degraded to TS while Go was unavailable)',
            'go-primary-unavailable',
          );
        case 'reset-degrade':
          // Go is DOWN in primary mode with Go-owned STUB pipelines; the
          // TS-native advance below would emit NOTHING for them — a silent
          // freeze with the cookie advancing past the gap (watermark
          // over-claim). Reset so re-registration (which checks `initialized`)
          // rebuilds REAL TS pipelines → graceful TS-serving.
          return new ResetPipelinesSignal(
            'Go backend unavailable in primary mode at advance time; ' +
              'rebuilding TS pipelines (avoids silent watermark over-claim)',
            'go-primary-unavailable',
          );
        case 'ts-native':
          // Pipelines are already real TS (or none) — the TS-native advance
          // below serves correctly; do NOT reset (that would loop every advance
          // for the whole outage / drift-breaker cooldown).
          break;
      }
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
  ): Promise<AdvanceResult | ResetPipelinesSignal> {
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
    // F3 (keystone): consume the diff EAGERLY to buffer it for both engines, but
    // route through drainDiffCatchingReset so the ResetPipelinesSignal the diff
    // iterator throws on a truncate / schema change is RETURNED (graceful
    // view-syncer reset + re-hydrate) instead of escaping #advancePipelines into
    // run()'s outer catch (full client-group teardown on every truncate).
    const resetSignal = drainDiffCatchingReset(diff, entry => {
      if (this.#isInternalTable(entry.table)) {
        // TS advances its real internal-query pipelines from these
        // (lmids / mutationResults); always replay them.
        buffered.push(entry);
        return;
      }
      // User-table change. P3 lean primary: TS holds only STUB user pipelines
      // (Go owns them) and keeps its user TableSources current via the snapshot
      // setDB in #advance — NOT via these pushes — so replaying user changes on
      // TS is pure redundant work. When lean, drop them from TS's replay
      // buffer. When NOT lean, keep the historical behaviour (replay the full
      // diff; user pushes are no-ops against stub pipelines).
      if (!this.#goLeanPrimary) {
        buffered.push(entry);
      }
      // Push mode still ships user changes to Go via advanceStream. Trigger
      // mode derives its own diff, so Go needs nothing here.
      if (!this.#goPrimaryTrigger) {
        snapshotChanges.push({
          table: entry.table,
          prevValues: entry.prevValues as Record<string, unknown>[],
          nextValue: entry.nextValue as Record<string, unknown> | null,
        });
      }
    });
    if (resetSignal) {
      return resetSignal;
    }
    const replayDiff: SnapshotDiff = {
      prev: diff.prev,
      curr: diff.curr,
      changes: diff.changes,
      [Symbol.iterator]: () => buffered[Symbol.iterator](),
    };

    // Kick the Go RPC in flight while TS does its work. Two sourcing modes:
    //
    //  - PUSH (default): advanceStream(snapshotChanges) — Go applies the
    //    TS-derived diff. Go's version is TS's version by construction, so the
    //    CVR stamps at `version` (= curr.version) unchanged.
    //
    //  - TRIGGER (P2c, #goPrimaryTrigger): advanceToHead() — Go independently
    //    leapfrogs its OWN Snapshotter, derives its OWN diff, and drives its OWN
    //    engine frame-coordinated. Go is self-consistent (no frame-timing
    //    drift). Go reports its own watermark V_go, which we reconcile with TS's
    //    V_ts below (§10: CVR stamps at min(V_ts, V_go)).
    //
    // Error classification is shared via #classifyGoPrimaryAdvanceError: bucket
    // by failure mode so each is observable; protocol/stale-epoch escalate
    // (re-throw), restart/unclassified drop the delta + schedule a reset (the
    // "miss exactly one delta" contract; recovery rebuilds Go's state).
    const goPromise: Promise<
      | {changes: RowChange[]; goVersion: string | undefined}
      | {reset: ResetPipelinesSignal}
    > = this.#goPrimaryTrigger
      ? this.#goBackend!.advanceToHeadStream()
          .then(goDerived => {
          if (goDerived.reset) {
            // F2: a Go-reported reset (truncate / permissions change / engine
              // reset) means Go's user pipelines must full-re-hydrate. The
              // legacy path (#scheduleGoReset + stamp prev) floored THIS cycle's
              // watermark safely, but the async Go reset rebuilds at head and
              // DISCARDS its hydrate output — so the (prev→head] user delta is
              // never delivered. For a TRUNCATE that means the deleted rows stay
              // on every client's screen indefinitely (TS-native does a full
              // re-hydrate for the same signal). Escalate to a full pipeline
              // reset so the view-syncer re-hydrates at head, re-delivering the
              // current state as an idempotent superset.
              this.#lc.info?.(
                `[go-primary] Go reported reset ${goDerived.reset.reason} ` +
                  `(${goDerived.reset.msg}); escalating to pipeline reset`,
              );
              return {
                reset: new ResetPipelinesSignal(
                  `Go reported reset ${goDerived.reset.reason} ` +
                    `(${goDerived.reset.msg})`,
                  'go-primary-drop',
                ),
              };
            }
            this.#recordGoPrimaryAdvanceTimings(goDerived.timings);
            return {
              changes: goDerived.rowChanges.map(rc =>
                this.#goRowChangeToRowChange(rc),
              ),
              goVersion: goDerived.version,
            };
          })
          .catch(e => {
            // F2: a dropped advance (sidecar restart / engine not initialized /
            // RPC timeout / unclassified) escalates to a full pipeline reset
            // rather than committing an empty delta floored at prev — the
            // latter permanently skips the (prev→head] user changes because the
            // scheduled Go reset rebuilds at head and discards its hydrate
            // output (see #classifyGoPrimaryAdvanceError). Protocol/stale errors
            // still reject goPromise (re-thrown) and never reach here.
            const classified = this.#classifyGoPrimaryAdvanceError(e);
            return classified instanceof ResetPipelinesSignal
              ? {reset: classified}
              : {changes: classified, goVersion: diff.prev.version};
          })
      : this.#goBackend!.advanceStream(snapshotChanges)
          .then(r => {
            this.#recordGoPrimaryAdvanceTimings(r.timings);
            return {
              changes: r.changes.map(rc => this.#goRowChangeToRowChange(rc)),
              goVersion: undefined,
            };
          })
          .catch(e => {
            const classified = this.#classifyGoPrimaryAdvanceError(e);
            return classified instanceof ResetPipelinesSignal
              ? {reset: classified}
              : {changes: classified, goVersion: undefined};
          });

    // Run TS's #advance over the replay buffer. Only internal-query pipelines
    // are connected, so user-table pushes are no-ops on TS — the iterator
    // emits nothing for those, only events for internal queries. When lean,
    // the buffer already excludes user changes, so pass its actual length for
    // the advance-time heuristic instead of the full diff's numChanges.
    const tsChanges: RowChange[] = [];
    for (const change of this.#advance(replayDiff, timer, buffered.length, true)) {
      if (change === 'yield') {
        await new Promise<void>(resolve => setImmediate(resolve));
      } else {
        tsChanges.push(change);
      }
    }

    const goOutcome = await goPromise;
    if ('reset' in goOutcome) {
      // F2: the Go advance dropped a user delta or reported a truncate/reset.
      // Return the signal so the view-syncer re-hydrates every query at head
      // (the gap heals as an idempotent superset). The TS internal-query work
      // above is discarded — the reset re-establishes it. Correct over fast.
      return goOutcome.reset;
    }
    const {changes: goResults, goVersion} = goOutcome;
    let goVersionFinal = goVersion;
    let goResultsFinal = goResults;

    // P2c clamp (inverted-edge guard): if Go came back BEHIND TS
    // (V_go < V_ts — rare; Go re-init/lag left its snapshotter pinned behind),
    // committing now stamps the CVR at min=V_go while TS's internal changes
    // (lmid / mutationResults) already reflect V_ts > V_go — an lmid ack AHEAD
    // of its user data, i.e. a torn cross-engine view. We can't clamp the
    // internal changes DOWN to min (they're already derived, version-less, and
    // would be LOST — the snapshotter consumed to V_ts), so instead LIFT Go UP
    // to V_ts: re-run advanceToHead (Go leapfrogs further toward head, which is
    // ≥ V_ts since the replica head only grows) and accumulate its extra user
    // RowChanges, until V_go ≥ V_ts. Bounded; only fires in the inverted edge,
    // so the common path (V_go ≥ V_ts) is untouched. If Go still can't catch up
    // (genuinely wedged), we fall through to min() — the watermark stays safe,
    // the torn window persists one cycle until the breaker/audit heals Go.
    if (
      this.#goPrimaryTrigger &&
      goVersionFinal !== undefined &&
      goVersionFinal < version
    ) {
      const MAX_CATCHUP = 3;
      for (let i = 0; i < MAX_CATCHUP && goVersionFinal < version; i++) {
        let next: AdvanceToHeadResult;
        try {
          next = await this.#goBackend!.advanceToHeadStream();
        } catch (e) {
          this.#lc.warn?.(
            `[go-primary] catch-up advanceToHead failed: ${String(e)}; ` +
              `committing at min`,
          );
          break;
        }
        if (next.reset) {
          // F2: a reset during catch-up (truncate/permissions) leaves the same
          // permanent gap as the main reset branch — the async Go reset rebuilds
          // at head and discards its hydrate output. Escalate to a full pipeline
          // reset so the view-syncer re-hydrates at head.
          this.#lc.info?.(
            `[go-primary] Go reported reset during catch-up ` +
              `(${next.reset.reason}); escalating to pipeline reset`,
          );
          return new ResetPipelinesSignal(
            `Go reported reset during catch-up ${next.reset.reason} ` +
              `(${next.reset.msg})`,
            'go-primary-drop',
          );
        }
        this.#recordGoPrimaryAdvanceTimings(next.timings);
        goResultsFinal = [
          ...goResultsFinal,
          ...next.rowChanges.map(rc => this.#goRowChangeToRowChange(rc)),
        ];
        goVersionFinal = next.version;
      }
      if (goVersionFinal < version) {
        this.#lc.warn?.(
          `[go-primary] Go still behind TS after catch-up ` +
            `(V_ts=${version}, V_go=${goVersionFinal}); committing at min — ` +
            `rare transient, control-plane ack may lead data by one cycle`,
        );
        // Go's user-data delta is incomplete this cycle — taint the
        // incremental accumulators so the reconcile waits for a clean re-seed.
        this.#markAllDeltaAccumDirty();
      }
    }

    // Item #2: fold Go's emitted advance deltas into the per-query incremental
    // accumulator (Go-primary path — the mirror of the shadow-advance feed).
    // The drift audit reconciles this against a fresh hydrate to catch a wrong
    // delta that a full re-materialization would mask.
    this.#accumulateGoDelta(goResultsFinal);

    // P2c reconciliation: the CVR stateVersion is a completeness floor, so it
    // may only be committed at a version BOTH authorities have crossed. TS's
    // internal data is at `version` (V_ts); Go's user data is at `goVersionFinal`
    // (V_go, post-catch-up). Stamp at min(V_ts, V_go) — under-claiming is safe
    // (the ahead side's extra rows are an idempotent superset delivered at the
    // committed patchVersion), over-claiming risks a client missing a change in
    // the gap. In push mode `goVersion` is undefined and the watermark is V_ts.
    const reconciled = reconcileGoPrimaryWatermark(version, goVersionFinal);

    // Merge: TS internal-query events + Go user-query events. The two
    // sets are table-disjoint by construction (internal tables filtered
    // out of Go; user tables have stub pipelines in TS that don't emit).
    function* yieldMerged(): Iterable<RowChange | 'yield'> {
      for (const c of tsChanges) yield c;
      for (const c of goResultsFinal) yield c;
    }

    return {
      version: reconciled.version,
      numChanges,
      changes: this.#trackRowSetSignatures(yieldMerged()),
      tsVersion: reconciled.tsVersion,
      goVersion: reconciled.goVersion,
    };
  }

  /**
   * Record Go-side per-(table,op) advance timings into the same #advanceTime
   * histogram the TS-native path populates, so Go-primary has table/op timing
   * attribution parity instead of dropping `timings` on the floor. Also feeds
   * the per-query `query-update-server` inspector metric (the TS-native path
   * populates it via MeasurePushOperator, which can't exist across the RPC
   * boundary): Go only reports per-(table, op) timings, so attribute each
   * table's time to every user query whose top-level table matches. This is an
   * APPROXIMATION — a query reading a table only through a related/CSQ subquery
   * isn't attributed, and a table feeding N queries credits all N — but it makes
   * the metric non-zero and directionally useful instead of permanently empty.
   */
  #recordGoPrimaryAdvanceTimings(timings: TableTiming[] | undefined): void {
    if (!timings) return;
    const tableToQueries = new Map<string, string[]>();
    for (const [qid, p] of this.#pipelines) {
      if (this.#isInternalQueryID(qid)) continue;
      const t = p.transformedAst.table;
      const list = tableToQueries.get(t);
      if (list) list.push(qid);
      else tableToQueries.set(t, [qid]);
    }
    for (const t of timings) {
      this.#advanceTime.recordMs(t.ms, {
        table: t.table,
        type: t.type === 0 ? 'add' : t.type === 1 ? 'remove' : 'edit',
      });
      for (const qid of tableToQueries.get(t.table) ?? []) {
        this.#inspectorDelegate.addMetric('query-update-server', t.ms, qid);
      }
    }
  }

  /**
   * Classify a Go-primary advance RPC failure (advanceStream OR advanceToHead).
   * Pre-fix the catch was a silent swallow of ALL errors, indistinguishable
   * from a legitimately-empty advance. Bucket by class so dashboards detect each
   * failure mode and operators take the right action:
   *
   *   1. Protocol violation (chunk order, missing final chunk, decode/frame):
   *      wire-level bug or corruption — can't trust Go's state. RE-THROW so the
   *      caller surfaces it instead of committing an empty CVR diff. A reset
   *      won't fix a wire bug without a sidecar process restart.
   *   2. StaleInitEpochError: this instance was torn down and a successor's
   *      epoch took over. Retrying is futile — RE-THROW so teardown completes.
   *   3. Sidecar unavailable / restart-related: the advance can't be trusted.
   *      F2: return a ResetPipelinesSignal so the view-syncer re-hydrates all
   *      pipelines at head. The legacy "return [] + #scheduleGoReset" path
   *      floored this cycle's watermark at prev but the async Go reset rebuilt
   *      at head with its hydrate output DISCARDED — the (prev→head] user delta
   *      was never delivered (permanent gap). A full re-hydrate heals it.
   *   4. Other unclassified (incl. RPC timeouts under load): same F2 escalation.
   *
   * Returns a ResetPipelinesSignal for the drop cases (caller RETURNS it from
   * #goPrimaryAdvance → graceful re-hydrate); throws for the escalation cases
   * (protocol/stale → caller surfaces it → teardown + reconnect).
   */
  #classifyGoPrimaryAdvanceError(e: unknown): RowChange[] | ResetPipelinesSignal {
    const msg = e instanceof Error ? e.message : String(e);

    switch (classifyGoPrimaryAdvanceError(e)) {
      case 'protocol':
        this.#advanceDroppedProtocol.add(1);
        this.#lc.error?.(
          `[go-primary] Go advance failed with PROTOCOL VIOLATION (escalating): ${msg}`,
        );
        throw e;
      case 'stale-epoch':
        this.#advanceDroppedStaleEpoch.add(1);
        this.#lc.warn?.(
          `[go-primary] Go advance rejected by sidecar (stale initEpoch); ` +
            `this view-syncer instance is torn down: ${msg}`,
        );
        throw e;
      case 'data-error':
        // Permanent bad replica data — re-throw (CG teardown) like
        // TS-native's UnsupportedValueError. NEVER reset: a reset re-reads the
        // same row and re-panics, looping forever AND re-paying every CG's
        // hydrate (the sustained 5–8s p99 reset storm). One poison row drops
        // ONE CG, not the whole pod.
        this.#advanceDroppedDataError.add(1);
        this.#lc.error?.(
          `[go-primary] Go advance hit PERMANENT data error (tearing down CG, ` +
            `NOT resetting — bad replica data, retry cannot fix): ${msg}`,
        );
        throw e;
      case 'sidecar':
        this.#advanceDroppedSidecar.add(1);
        this.#lc.warn?.(
          `[go-primary] Go advance dropped (sidecar restart in flight); ` +
            `escalating to pipeline reset: ${msg}`,
        );
        return new ResetPipelinesSignal(
          `Go advance dropped (sidecar restart): ${msg}`,
          'go-primary-drop',
        );
      case 'unclassified':
        this.#advanceDroppedOther.add(1);
        this.#lc.error?.(
          `[go-primary] Go advance failed (unclassified); escalating to pipeline ` +
            `reset: ${msg}`,
        );
        return new ResetPipelinesSignal(
          `Go advance failed (unclassified): ${msg}`,
          'go-primary-drop',
        );
    }
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
    // diverges from TS and breaks shadow-compare on every REMOVE.
    let row: Row | undefined;
    if (type === ChangeType.REMOVE) {
      row = undefined;
    } else if (rc.row === null || rc.row === undefined) {
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
   * Heal a SQL-oracle-CONFIRMED drift found by the sampled drift audit.
   * In Go-primary mode Go is SERVING the drifted rows — leaving this
   * log-only means clients keep wrong data until manual action, while the
   * count-freeze probe already self-heals via reset. Escalate confirmed
   * set/content/order drift the same way. In shadow mode TS is
   * authoritative (nothing wrong reaches clients), so stay log-only and
   * keep the drift signal observable across audits instead of wiping the
   * evidence with a reset.
   *
   * Safe against oracle false-positives: a reset rebuilds Go from SQLite
   * truth, so a spurious trigger costs one re-init and the next audit
   * passes; repeated resets are bounded by #scheduleGoReset's retry cap
   * and the drift-loop breaker.
   */
  #healConfirmedDrift(reason: string): void {
    if (this.#shadowMode) return;
    this.#lc.warn?.(
      `[drift-audit] Go-primary confirmed drift → engine reset now + ` +
        `client re-hydrate on next advance (${reason})`,
    );
    // Two-part heal. The engine reset fixes Go's pipelines IMMEDIATELY so
    // drift stops compounding into subsequent advances — but resetEngine
    // discards its hydrate output (go-compute-backend: "TS already owns the
    // client view"), so rows ALREADY DELIVERED to connected clients stay
    // wrong. The pending flag makes the next advance() return a
    // ResetPipelinesSignal, driving the proven F2 machinery
    // (pipelines.reset → hydrateUnchangedQueries → CVR updater → correcting
    // patches poked to clients) — the only path that converges the client
    // view.
    //
    // No double engine-init: the F2 reset() path re-inits Go via
    // #maybeResetGoBackend, which no-ops while this reset is mid-reinit
    // (its `!initialized` guard — #reinitPerCGAndRegisterQueries flips
    // initialized=false for the duration). The re-hydrate's addQueries then
    // serialize behind this reset via the backend's #restartGate /
    // #currentInitPromise. (NOT #scheduleGoReset's in-flight flag — reset()
    // bypasses #scheduleGoReset entirely.)
    this.#scheduleGoReset(reason);
    this.#pendingClientResetReason ??= reason;
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
    // A Go reset rebuilds engine state from scratch, so any in-flight
    // incremental accumulation is no longer continuous — taint it so the
    // incremental reconcile waits for a clean re-seed (item #2).
    this.#markAllDeltaAccumDirty();
    // If the snapshotter has been torn down (CG eviction / worker reassignment),
    // there is nothing to reset — resetEngine would re-read a CLOSED snapshot
    // connection in #currentTablesForGo and fail-loop ("database connection is
    // not open" per table, ×N, then a failed reset that retries). The CG is
    // gone; skip cleanly. (More likely under the P2c trigger path, whose longer
    // advanceToHead window widens the reset-vs-teardown overlap.)
    if (this.#snapshotter.destroyed) {
      this.#lc.debug?.(`[go-reset] snapshotter torn down; skipping reset (${reason})`);
      return;
    }
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
    this.#lc.warn?.(`[shadow] Scheduling Go reset (${reason})`);
    // CRIT-5: resetEngine reads the snapshot at reinit time (after its
    // destroy await), not now — pre-capturing here loaded a stale snapshot
    // and amplified drift into a reset loop.
    this.#goInitPromise = this.#goBackend.resetEngine();
    this.#goInitPromise
      .then(() => {
        this.#lc.info?.(`[shadow] Go reset complete (${reason})`);
        this.#goResetRetries = 0;
      })
      .catch(err => {
        // If the snapshotter was torn down DURING the reset (destroy raced the
        // resetEngine destroy-await → #currentTablesForGo read a closed conn),
        // the CG is gone — abandon cleanly instead of logging an error and
        // retrying into the same closed connection.
        if (this.#snapshotter.destroyed) {
          this.#lc.debug?.(
            `[go-reset] snapshotter torn down during reset; abandoning (${reason})`,
          );
          this.#goResetRetries = 0;
          this.#goResetDirty = false;
          return;
        }
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
  ): Promise<AdvanceResult | ResetPipelinesSignal> {
    // Buffer diff entries so both paths can consume them
    const buffered: Array<{
      table: string;
      prevValues: Readonly<Row>[];
      nextValue: Readonly<Row> | null;
      rowKey: RowKey;
    }> = [];
    const snapshotChanges: SnapshotChange[] = [];
    // F3: same eager-buffer hazard as #goPrimaryAdvance — route through
    // drainDiffCatchingReset so a truncate / schema-change ResetPipelinesSignal
    // is RETURNED (graceful reset + re-hydrate) instead of escaping into the
    // outer-catch teardown.
    const resetSignal = drainDiffCatchingReset(diff, entry => {
      buffered.push(entry);
      // TS always consumes the full diff (its source-of-truth is SQLite,
      // and internal queries like lmids need these). Go's snapshotChanges
      // omits internal tables — Go never loads them and an Edit on a
      // row Go's MemorySource doesn't have would panic the sidecar.
      if (this.#isInternalTable(entry.table)) {
        return;
      }
      snapshotChanges.push({
        table: entry.table,
        prevValues: entry.prevValues as Record<string, unknown>[],
        nextValue: entry.nextValue as Record<string, unknown> | null,
      });
    });
    if (resetSignal) {
      return resetSignal;
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
        // P2 drive: source the Go advance from Go's OWN derived diff
        // (advanceToHead, frame-coordinated) instead of shipping it the TS
        // diff. Otherwise (P1/legacy) ship the diff via the streaming advance
        // so shadow exercises the same path as Go-primary.
        let goChanges: GoRowChange[];
        if (this.#goAdvanceDrive) {
          const goDerived = await this.#goBackend!.advanceToHead();
          if (goDerived.reset) {
            this.#lc.info?.(
              `[go-drive-shadow] Go reported reset ${goDerived.reset.reason} ` +
                `(${goDerived.reset.msg}); scheduling Go reset, skipping compare`,
            );
            this.#scheduleGoReset('advanceToHead-reset');
            return {results: [], ms: performance.now() - goStart};
          }
          goChanges = goDerived.rowChanges;
        } else {
          // Use the streaming variant in shadow mode too, so shadow runs
          // exercise the same code path as Go-primary mode (otherwise
          // shadow would never catch streaming-specific regressions).
          const goRaw = await this.#goBackend!.advanceStream(snapshotChanges);
          goChanges = goRaw.changes;
        }
        // Per-(queryID,table) Go advance breakdown. Demoted to debug —
        // useful when chasing a divergence, noise at error in steady-state.
        if (this.#lc.debug) {
          const tableBreakdown: Record<string, number> = {};
          for (const rc of goChanges) {
            tableBreakdown[rc.table] = (tableBreakdown[rc.table] ?? 0) + 1;
          }
          this.#lc.debug?.(
            `[go-advance-out] diff=${snapshotChanges.length} ` +
              `go-out=${goChanges.length} drive=${this.#goAdvanceDrive} ` +
              `by-table=${JSON.stringify(tableBreakdown)}`,
          );
        }
        return {
          results: goChanges.map(rc => this.#goRowChangeToRowChange(rc)),
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

    // Compare — try/catch because stableStringify inside #shadowCompare can
    // throw on malformed row data; must not kill the advance pipeline when
    // both TS and Go results are already computed and ready to yield.
    try {
      this.#shadowCompare('advance', version, tsChangesForCompare, goResults, undefined, diff);
    } catch (e) {
      this.#lc.error?.(`[shadow] advance compare threw: ${e}`);
    }

    // Roll the 1-deep neighbor buffer: this advance's compare-filtered changes
    // become the NEXT advance's neighbor for isAdvanceFrameSkewCrossBatch.
    // Kept 1-deep (overwrite, no append) so a stale match from >1 batch back
    // can never trigger suppression. Internal-query events are already filtered
    // out of both sides (tsChangesForCompare / goResults are what the classifier
    // saw), keeping the neighbor consistent with the compare's view.
    this.#advanceFrameSkewNeighbor = {ts: tsChangesForCompare, go: goResults};

    // Item #2: fold Go's emitted advance deltas into the per-query incremental
    // accumulator so the next drift audit can reconcile the accumulated stream
    // against a fresh SQL hydrate (catches a wrong delta that a full
    // re-materialization would mask). If this cycle's Go output is already
    // suspect (count diverged from TS, or Go produced nothing while TS did),
    // taint all accumulators — they'll re-seed cleanly on the next audit
    // rather than reconcile against a known-incomplete stream.
    if (
      tsChangesForCompare.length !== goResults.length ||
      (tsChangesForCompare.length > 0 && goResults.length === 0)
    ) {
      this.#markAllDeltaAccumDirty();
    }
    this.#accumulateGoDelta(goResults);

    // Snapshotter-in-Go P1: also have Go derive its OWN diff (advanceToHead)
    // and compare it to TS's computed snapshotChanges for this same advance.
    // Best-effort + isolated from the RowChange comparison above; a failure
    // here never affects the returned (TS) results.
    if (this.#goDerivedDiff && this.#goBackend?.initialized) {
      try {
        const goDerived = await this.#goBackend.advanceToHead();
        this.#compareGoDerivedDiff(version, snapshotChanges, goDerived);
      } catch (e) {
        this.#lc.warn?.(`[go-diff-shadow] advanceToHead failed: ${e}`);
      }
    }

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
   * #2: capture a divergence to disk for offline replay. Gated by
   * `goSidecar.divergenceCaptureDir` (off by default) and rate-capped at one
   * capture per (operation+queryID+kind) per minute. Writes:
   *   <dir>/<timestamp>_<op>_<queryID>_<kind>_<hash>.db  — VACUUM INTO copy of
   *     the snapshotter's CURRENT replica (the post state; for advance the
   *     caller also passes `diff` so prev/curr are both available offline).
   *   <dir>/<...>.json — {ast, queryID, operation, kind, tsChanges, goChanges,
   *     sqlVerdict, stateVersion}.
   *
   * MUST be called BEFORE the divergence branch returns, while the snapshotter's
   * prev/curr are still live (snapshotter only advances on the next advance()).
   * Best-effort: any I/O error is logged + swallowed — capture must never kill
   * the compare or affect the returned (TS) results. The fire-and-forget writes
   * are NOT awaited on the compare hot path (the JSON write is a microtask; the
   * VACUUM INTO is synchronous against the replica but non-blocking to readers).
   */
  #captureDivergence(
    kind: string,
    operation: string,
    context: string,
    ast: AST | undefined,
    tsChanges: RowChange[],
    goChanges: RowChange[],
    sqlVerdict: unknown,
    diff: SnapshotDiff | undefined,
  ): void {
    const dir = goDivergenceCaptureDir(this.#config);
    if (!dir) return; // capture off (default).
    // Rate cap: one per (op+queryID+kind) per minute. Under a divergence storm
    // VACUUM INTO would copy the whole DB on every MISMATCH; this bounds it.
    const capKey = `${operation}|${context}|${kind}`;
    const now = Date.now();
    const last = this.#divergenceCaptureLastFire.get(capKey) ?? 0;
    if (now - last < 60_000) return;
    this.#divergenceCaptureLastFire.set(capKey, now);

    // Best-effort: a thrown error here must not escape into the compare.
    try {
      // The snapshot to freeze: for advance, `diff.curr` is the post-advance
      // snapshot (prev is `diff.prev`); for hydrate/drift-audit, the
      // snapshotter's current snapshot is the right post state. Both expose
      // `.db` (StatementRunner) and `.version`.
      const snap = diff?.curr ?? this.#snapshotter.current();
      const stateVersion = snap.version;
      // Hash the inputs for a short, collision-likely-unique filename. Use the
      // normalized lengths + a sample of row keys (NOT the full content — PII).
      const tsSample = tsChanges.slice(0, 3).map(c => `${c.table}:${stableStringify(c.rowKey)}`).join(',');
      const goSample = goChanges.slice(0, 3).map(c => `${c.table}:${stableStringify(c.rowKey)}`).join(',');
      const hash = (str: string): string => {
        let h = 0;
        for (let i = 0; i < str.length; i++) {
          h = (Math.imul(31, h) + str.charCodeAt(i)) | 0;
        }
        return (h >>> 0).toString(36);
      };
      const fileHash = hash(`${operation}|${context}|${stateVersion}|${tsChanges.length}|${goChanges.length}|${tsSample}|${goSample}`);
      const safeContext = context.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 32);
      const safeKind = kind.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 32);
      const base = `${now.toString(36)}_${operation}_${safeContext}_${safeKind}_${fileHash}`;
      const dbPath = join(dir, `${base}.db`);
      const jsonPath = join(dir, `${base}.json`);

      // VACUUM INTO freezes a point-in-time copy. Non-blocking to readers of
      // the source replica. Synchronous but bounded by DB size — the rate cap
      // + off-by-default gate contain it.
      snap.db.db.prepare(`VACUUM INTO ?`).run(dbPath);

      // Fire-and-forget the JSON write (do NOT block the compare on disk I/O).
      // The metadata carries everything the offline oracle + Go harness need to
      // replay: the AST (JSON-serializable), both sides' changes, the SQL
      // verdict, and the state version. For advance, `diff.prev.version` /
      // `diff.curr.version` give both endpoints; the .db above is the post copy.
      void (async () => {
        try {
          await mkdir(dir, {recursive: true});
          const payload = {
            ast,
            queryID: context,
            operation,
            kind,
            tsChanges,
            goChanges,
            sqlVerdict,
            stateVersion,
            prevVersion: diff?.prev.version,
            currVersion: diff?.curr.version,
            snapshotFile: `${base}.db`,
            capturedAt: new Date(now).toISOString(),
          };
          await writeFile(jsonPath, JSON.stringify(payload));
        } catch (e) {
          this.#lc.warn?.(`[divergence-capture] json write failed for ${base}: ${String(e)}`);
        }
      })();
      this.#lc.info?.(
        `[divergence-capture] captured ${kind} (${operation} ${context}) ` +
          `→ ${base}.{db,json} (stateVersion=${stateVersion})`,
      );
    } catch (e) {
      this.#lc.warn?.(`[divergence-capture] failed (${operation} ${context} ${kind}): ${String(e)}`);
    }
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
    // Optional AST for the query under comparison — supplied by callers that
    // have it inline (batch-hydrate, drift-audit) so the result-ORDER check
    // doesn't depend on `context` being registered in #pipelines yet (it may
    // not be at hydrate time). Falls back to the #pipelines lookup.
    ast?: AST | undefined,
    // Optional SnapshotDiff — supplied ONLY by the advance caller
    // (#shadowAdvance). Carries the pinned prev/curr StatementRunners the
    // advance SQL oracle (#sqlGroundTruthAdvanceCompare) re-queries to derive
    // the expected delta. The hydrate/drift-audit callers pass nothing — the
    // advance oracle is gated on `diff` being present AND `operation === 'advance'`,
    // so this stays a pure additive param for the non-advance paths.
    diff?: SnapshotDiff | undefined,
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

    // Tie-window suppression. Before reporting ANY divergence, check whether the
    // bags differ ONLY by a benign tie-member swap in a single-root-table
    // ordered+LIMITed result (Go and TS legitimately pick different members of a
    // tie group at the LIMIT boundary; real Zero appends the PK so the sort is
    // total, raw test ASTs may not). Suppressing keeps shadow MISMATCH a
    // trustworthy signal. Only runs when the bags actually diverge.
    const bagsDiffer =
      tsNorm.length !== goNorm.length ||
      tsNorm.some((t, i) => {
        const g = goNorm[i];
        return (
          t.type !== g.type ||
          t.queryID !== g.queryID ||
          t.table !== g.table ||
          t.rowKey !== g.rowKey ||
          t.row !== g.row
        );
      });
    if (
      bagsDiffer &&
      isShadowTieWindow(
        ast ?? this.#pipelines.get(context)?.transformedAst,
        tsChanges,
        goChanges,
      )
    ) {
      this.#shadowTieWindows.add(1);
      this.#lc.debug?.(
        `[shadow] tie-window (${operation} ${context}): benign LIMIT tie-member ` +
          `swap — suppressed (ts=${tsNorm.length} go=${goNorm.length})`,
      );
      return;
    }

    // Cross-batch frame-skew suppression (advance path primarily). The advance
    // path has no single AST, so the SQL oracle below can't adjudicate it — a
    // frame-skew split (same logical changes placed in different advance
    // batches by the two engines' independently-pinned snapshots) would otherwise
    // surface as an unattributed raw MISMATCH, and the split can be hundreds of
    // rows (the go-primary soak confirmed a 588-row channel_participants
    // fan-out that landed entirely in TS's batch for that one advance window).
    // isAdvanceFrameSkew only suppresses a CLEAN PARTITION: TS-only and Go-only
    // sides disjoint on rowKey, no same-key content/op drift, each
    // (queryID,table,rowKey) at most once per side, and BOTH sides exclusive. A
    // real value drift (same key, differing content) or a real
    // multiplicity/drop divergence is kept. Proven benign by the go-ivm
    // advance_drift_shadow_mismatch repro tests — both engines converge at head.
    if (bagsDiffer && isAdvanceFrameSkew(tsChanges, goChanges)) {
      this.#shadowAdvanceFrameSkew.add(1);
      this.#lc.debug?.(
        `[shadow] advance-frame-skew (${operation} ${context}): benign ` +
          `cross-batch frame-skew split — suppressed ` +
          `(ts=${tsNorm.length} go=${goNorm.length})`,
      );
      return;
    }

    // Cross-batch frame-skew, EMPTY-SIDE variant (advance path only). The
    // intra-frame classifier above requires BOTH sides to carry exclusive rows
    // (:4271-4274), so a one-sided batch (one engine empty) falls through as a
    // raw MISMATCH. But the same WAL frame-skew can put ALL of a logical change
    // in one engine's batch here and NONE in the other's, with the missing rows
    // appearing on the OTHER engine in the adjacent advance batch (live-proven
    // 2026-06-22: frames 81b3tyfhi0 / 81b3tyh9ug, byte-identical rows). The
    // 1-deep neighbor buffer (#advanceFrameSkewNeighbor, updated at the end of
    // each #shadowAdvance) supplies that adjacent batch; the classifier
    // suppresses ONLY when the non-empty side's rows appear byte-identical on
    // the opposite engine in the neighbor (full RowChange content equality — a
    // same-PK different-content neighbor row is NOT a match, so a real drop is
    // kept). See isAdvanceFrameSkewCrossBatch for the invariants.
    if (
      bagsDiffer &&
      operation === 'advance' &&
      isAdvanceFrameSkewCrossBatch(
        tsChanges,
        goChanges,
        this.#advanceFrameSkewNeighbor,
      )
    ) {
      this.#shadowAdvanceFrameSkewCrossBatch.add(1);
      this.#lc.debug?.(
        `[shadow] advance-frame-skew-cross-batch (${operation} ${context}): ` +
          `benign empty-side cross-batch frame-skew split — suppressed ` +
          `(ts=${tsNorm.length} go=${goNorm.length})`,
      );
      return;
    }

    // SQL ground-truth CLASSIFIER. A bare TS-vs-Go MISMATCH is ambiguous: TS is
    // NOT axiomatically the oracle — it can be the wrong side (suspected IVM
    // pagination-boundary divergences, e.g. the conversations boundary-drop;
    // unverified upstream, so treated as a hypothesis, not an established fact).
    // When we have the query's AST (a single-query hydrate), adjudicate against
    // raw SQL on the replica. CAVEAT — this oracle is MAIN-TABLE-ONLY:
    // buildAuditSQL SELECTs FROM the outer table (related tables appear only as
    // EXISTS filters, never as returned rows) and #sqlGroundTruthCompare filters
    // changes to `c.table === ast.table`, so it can confirm MAIN-table parity
    // but is structurally BLIND to related-table (join fan-out) rows. Therefore
    // "Go matches SQL" does not by itself convict TS: we verify TS against the
    // SAME oracle too (below), and attribute the divergence to TS only when Go
    // matches SQL and TS does NOT. Runs ONLY on an actual divergence, so it adds
    // no load to matching comparisons, and only for batch-hydrate (the audit
    // path runs its own SQL check; advance has no single AST). This respects
    // "TS is the bar" — SQL only breaks ties that already exist.
    if (bagsDiffer && operation === 'batch-hydrate') {
      const classifyAst = ast ?? this.#pipelines.get(context)?.transformedAst;
      if (classifyAst) {
        const verdict = this.#sqlGroundTruthCompare(classifyAst, goChanges);
        if (
          verdict.kind === 'confirmed' ||
          verdict.kind === 'go-vs-sql-tie-window'
        ) {
          // Go matches the SQL oracle — but ONLY on the main table (goByPK is
          // filtered to c.table === ast.table; buildAuditSQL returns outer-table
          // rows only). That alone does NOT make TS the outlier: the ts-vs-go
          // count gap is dominated by related-table (join fan-out) rows the
          // oracle never sees. Before demoting to "TS differs", verify TS against
          // the SAME oracle — symmetric with the Go-disagrees branch below, which
          // added this exact check because the old code asserted TS parity in a
          // comment without testing it. One extra SQL re-query on an already-rare
          // mismatch path.
          const tsVerdict = this.#sqlGroundTruthCompare(classifyAst, tsChanges);
          if (
            tsVerdict.kind === 'confirmed' ||
            tsVerdict.kind === 'go-vs-sql-tie-window'
          ) {
            // BOTH engines match SQL on the main table. The ts-vs-go divergence
            // therefore lives in a dimension this single-table oracle CANNOT see
            // (related-table fan-out multiplicity) — it is NOT attributable to
            // TS, and Go (the heavier side) is at least as suspect. Count it as
            // unadjudicable and FALL THROUGH (no return) so the raw TS-vs-Go
            // MISMATCH detail below surfaces the off-table rows for triage.
            this.#shadowSqlUnreliable.add(1);
            this.#lc.info?.(
              `[shadow] oracle-blind divergence (${operation} ${context}): ` +
                `BOTH engines match SQL on the main table; ts-vs-go differs ` +
                `off-table (likely related-table join fan-out) (go=${verdict.kind}, ` +
                `ts=${tsVerdict.kind}, ts=${tsNorm.length} go=${goNorm.length} ` +
                `sql=${verdict.sqlCount}) — NOT attributable to TS; raw detail below`,
            );
            // fall through to the raw mismatch detail below (do NOT return).
          } else {
            // Go matches SQL, TS does NOT → TS is genuinely the divergent side.
            this.#shadowTsOnlyDivergences.add(1);
            this.#lc.info?.(
              `[shadow] ts-only divergence (${operation} ${context}): Go ` +
                `matches SQL, TS differs from it (go=${verdict.kind}, ` +
                `ts=${tsVerdict.kind}, ts=${tsNorm.length} go=${goNorm.length} ` +
                `sql=${verdict.sqlCount}) — NOT a Go drift`,
            );
            this.#captureDivergence(
              'ts-only',
              operation,
              context,
              classifyAst,
              tsChanges,
              goChanges,
              {go: verdict, ts: tsVerdict},
              diff,
            );
            return;
          }
        } else if (verdict.kind === 'skipped') {
          // Unbuildable SQL — can't adjudicate. Count the rate, then fall
          // through to the raw TS-vs-Go MISMATCH so it's still surfaced.
          this.#shadowSqlUnreliable.add(1);
        } else if (verdict.sqlCount === 0 && tsNorm.length > 0) {
          // EXCLUSIVE-CURSOR BOUNDARY-SKEW family. Go disagrees
          // with SQL, SQL=0, AND TS ALSO has rows. Traced to the conversations
          // list with an exclusive cursor (createdAt ASC, exclusive) whose
          // cursor row is the NEWEST: the steady-state correct page is EMPTY
          // (`createdAt > cursor` ⇒ 0, replica-verified via a direct
          // `@rocicorp/zero-sqlite3` query). Go's cursor+scalar-EXISTS+Take
          // logic for exactly this shape is VERIFIED CORRECT offline — see
          // go-ivm/testharness `testcase_exclusive_cursor_{boundary,advance}`:
          // hydrate ⇒ 0 rows, and an insert-churn advance ⇒ only the strictly-
          // after row, on a clean snapshot (source-level, full hydrate, and
          // advance paths all pass). So a LIVE divergence where BOTH engines
          // over-read an empty exclusive page is a transient WAL-frame snapshot
          // skew (Go's pinned prev-tx leaf sits one frame ahead of the SQL/TS
          // read), NOT a logic drift — Go just over-reads the freshly-written
          // boundary rows by one more frame than TS. Self-heals on re-hydrate.
          // Demote to info; do NOT alarm. (The Go-OUTLIER case — SQL=0 AND
          // TS=0, only Go has rows — falls to the else below and stays
          // CONFIRMED: Go diverging from BOTH TS and SQL is genuinely suspect.)
          this.#shadowSqlUnreliable.add(1);
          this.#lc.info?.(
            `[shadow] boundary-skew-suspect (${operation} ${context}): both ` +
              `engines over-read an empty exclusive-cursor page (ts=${tsNorm.length} ` +
              `go=${goNorm.length} sql=0) — Go cursor logic verified correct ` +
              `offline, transient snapshot skew, NOT a Go drift`,
          );
          return;
        } else {
          // Go disagrees with SQL. Before declaring Go the LONE offender,
          // verify that TS actually matches SQL — the previous code asserted
          // "TS matches SQL" in a comment without checking, so the known
          // buildAuditSQL under-count families (main-table-only counting,
          // its own EXISTS+limit boundary handling) produced CONFIRMED
          // verdicts when the oracle in fact disagreed with BOTH engines.
          // One extra SQL re-query, only on this already-rare mismatch path.
          const tsVerdict = this.#sqlGroundTruthCompare(classifyAst, tsChanges);
          if (
            tsVerdict.kind === 'confirmed' ||
            tsVerdict.kind === 'go-vs-sql-tie-window'
          ) {
            // TS matches SQL, Go doesn't: Go is genuinely the lone offender.
            this.#shadowConfirmedGoDrift.add(1);
            this.#lc.error?.(
              `[shadow] CONFIRMED Go drift (${operation} ${context}): Go disagrees ` +
                `with SQL while TS matches it (${verdict.kind}, ts=${tsNorm.length} ` +
                `go=${goNorm.length} sql=${verdict.sqlCount}) — detail below`,
            );
            this.#captureDivergence(
              'go-vs-sql-drift',
              operation,
              context,
              classifyAst,
              tsChanges,
              goChanges,
              {go: verdict, ts: tsVerdict},
              diff,
            );
          } else {
            // The SQL oracle disagrees with BOTH engines (or can't adjudicate
            // TS). That's an oracle/audit-SQL divergence, not a confirmed Go
            // drift — demote to info and fall through so the raw TS-vs-Go
            // MISMATCH detail below still surfaces the difference.
            this.#shadowSqlUnreliable.add(1);
            this.#lc.info?.(
              `[shadow] ts/sql-oracle-divergence (${operation} ${context}): SQL ` +
                `disagrees with BOTH engines (go=${verdict.kind}, ts=${tsVerdict.kind}, ` +
                `ts=${tsNorm.length} go=${goNorm.length} sql=${verdict.sqlCount}) — ` +
                `NOT a confirmed Go drift; raw mismatch detail below`,
            );
          }
        }
      }
    }

    // SQL ground-truth oracle for the ADVANCE path (#1). The hydrate block
    // above adjudicates a SINGLE query whose AST is in scope; an advance has
    // no single AST — it carries changes for MANY queryIDs at once. So we
    // group the diverged changes by queryID, look up each query's AST via
    // #pipelines, and run #sqlGroundTruthAdvanceCompare (which re-queries the
    // snapshotter's pinned prev/curr to derive the expected delta per query)
    // — one per diverged queryID that has a recoverable AST.
    //
    // Reuses the SAME verdict-classification shape as the hydrate oracle
    // (confirmed / go-vs-sql-drift / oracle-blind / skipped), and the SAME
    // counters (#shadowTsOnlyDivergences, #shadowConfirmedGoDrift,
    // #shadowSqlUnreliable) so advance + hydrate drift rates aggregate
    // naturally. The advance path's verdict has no `go-vs-sql-tie-window` (a
    // delta has no LIMIT window) and adds `oracle-blind` (the divergence is
    // entirely off-table fan-out for this queryID). The TS-vs-SQL cross-check
    // is symmetric with hydrate: Go confirmed alone is not a TS fault (the
    // gap may be fan-out the oracle can't see), so we re-query TS against the
    // same derived delta before demoting to ts-only.
    //
    // Gating: runs ONLY when (a) bagsDiffer, (b) operation === 'advance',
    // (c) `diff` is present (only #shadowAdvance passes it — hydrate/drift-
    // audit callers don't), and (d) at least one diverged queryID has a
    // registered AST. Zero cost on matching advances (bagsDiffer short-
    // circuits). 2 SQL re-queries (prev + curr) per diverged queryID.
    if (bagsDiffer && operation === 'advance' && diff) {
      // Partition the diverged changes by queryID on BOTH sides so we can
      // cross-check TS against the same derived delta before attributing.
      const byQueryID = (changes: RowChange[]): Map<string, RowChange[]> => {
        const m = new Map<string, RowChange[]>();
        for (const c of changes) {
          let bucket = m.get(c.queryID);
          if (!bucket) {
            bucket = [];
            m.set(c.queryID, bucket);
          }
          bucket.push(c);
        }
        return m;
      };
      const goByQuery = byQueryID(goChanges);
      // Only queryIDs that actually diverge warrant an oracle run. A queryID
      // present on only one side, or present on both with differing content,
      // is diverged; a queryID whose TS+Go changes are byte-identical is not.
      const tsByQuery = byQueryID(tsChanges);
      const divergedIDs = new Set<string>();
      const allIDs = new Set<string>([...goByQuery.keys(), ...tsByQuery.keys()]);
      for (const id of allIDs) {
        const tsQ = tsByQuery.get(id) ?? [];
        const goQ = goByQuery.get(id) ?? [];
        if (tsQ.length !== goQ.length) {
          divergedIDs.add(id);
          continue;
        }
        // Same-length: compare as sorted strings (cheap; reuse the normalize
        // shape — type/queryID/table/rowKey/row). A divergence on any row
        // marks this queryID diverged.
        const sig = (arr: RowChange[]) =>
          arr
            .map(c =>
              stableStringify({
                type: c.type,
                table: c.table,
                rowKey: c.rowKey,
                row: c.row,
              }),
            )
            .sort();
        const tsSig = sig(tsQ);
        const goSig = sig(goQ);
        if (tsSig.some((s, i) => s !== goSig[i])) divergedIDs.add(id);
      }

      for (const qid of divergedIDs) {
        const pipeline = this.#pipelines.get(qid);
        if (!pipeline?.transformedAst) continue;
        this.#shadowAdvanceSqlOracleRuns.add(1);
        const goQ = goByQuery.get(qid) ?? [];
        const tsQ = tsByQuery.get(qid) ?? [];
        let verdict;
        try {
          verdict = this.#sqlGroundTruthAdvanceCompare(
            pipeline.transformedAst,
            diff.prev.db,
            diff.curr.db,
            goQ,
          );
        } catch (e) {
          // The oracle must never kill the advance compare — a thrown error
          // (e.g. a stale db handle) degrades to oracle-blind / fall-through.
          this.#shadowSqlUnreliable.add(1);
          this.#lc.info?.(
            `[shadow] advance-sql-oracle threw (${operation} ${qid}): ${String(e)} — ` +
              `falling through to raw mismatch detail`,
          );
          continue;
        }
        if (verdict.kind === 'confirmed') {
          // Go's main-table delta matches the SQL-derived delta. Before
          // attributing to TS, cross-check TS against the same delta — the
          // ts-vs-go gap for this queryID may be fan-out the oracle can't see.
          let tsVerdict;
          try {
            tsVerdict = this.#sqlGroundTruthAdvanceCompare(
              pipeline.transformedAst,
              diff.prev.db,
              diff.curr.db,
              tsQ,
            );
          } catch {
            tsVerdict = {kind: 'skipped' as const, reason: 'ts-oracle-threw'};
          }
          if (tsVerdict.kind === 'confirmed') {
            // Both engines match SQL on the main table → divergence is off-
            // table (fan-out); NOT attributable to TS. Fall through to raw
            // mismatch detail so the off-table rows still surface.
            this.#shadowSqlUnreliable.add(1);
            this.#lc.info?.(
              `[shadow] oracle-blind divergence (${operation} ${qid}): both ` +
                `engines' deltas match SQL on the main table; ts-vs-go differs ` +
                `off-table (go=${verdict.kind}, ts=${tsVerdict.kind}, ` +
                `ts=${tsQ.length} go=${goQ.length} sql=${verdict.sqlCount}) — ` +
                `NOT attributable to TS; raw detail below`,
            );
            // fall through (do NOT return).
          } else {
            // Go matches the SQL delta, TS does NOT → TS is the divergent side.
            this.#shadowTsOnlyDivergences.add(1);
            this.#lc.info?.(
              `[shadow] ts-only divergence (${operation} ${qid}): Go delta ` +
                `matches SQL, TS delta differs (go=${verdict.kind}, ` +
                `ts=${tsVerdict.kind}, ts=${tsQ.length} go=${goQ.length} ` +
                `sql=${verdict.sqlCount}) — NOT a Go drift`,
            );
            this.#captureDivergence(
              'ts-only',
              operation,
              qid,
              pipeline.transformedAst,
              tsQ,
              goQ,
              {go: verdict, ts: tsVerdict},
              diff,
            );
            return;
          }
        } else if (verdict.kind === 'oracle-blind') {
          // This queryID's divergence is entirely off-table (no main-table
          // delta on either side). Can't adjudicate; fall through to raw detail.
          this.#shadowSqlUnreliable.add(1);
          this.#lc.info?.(
            `[shadow] oracle-blind divergence (${operation} ${qid}): no ` +
              `main-table delta for this queryID (ts=${tsQ.length} go=${goQ.length} ` +
              `sql=${verdict.sqlCount}) — divergence is off-table; raw detail below`,
          );
          // fall through.
        } else if (verdict.kind === 'skipped') {
          this.#shadowSqlUnreliable.add(1);
          // fall through.
        } else {
          // go-vs-sql-drift or go-vs-sql-content-drift: Go's delta disagrees
          // with SQL. Cross-check TS before convicting Go (symmetric with the
          // hydrate branch) — if TS ALSO disagrees with SQL, it's an oracle/
          // audit-SQL issue, not a lone Go fault.
          let tsVerdict;
          try {
            tsVerdict = this.#sqlGroundTruthAdvanceCompare(
              pipeline.transformedAst,
              diff.prev.db,
              diff.curr.db,
              tsQ,
            );
          } catch {
            tsVerdict = {kind: 'skipped' as const, reason: 'ts-oracle-threw'};
          }
          if (tsVerdict.kind === 'confirmed') {
            this.#shadowConfirmedGoDrift.add(1);
            this.#lc.error?.(
              `[shadow] CONFIRMED Go drift (${operation} ${qid}): Go delta ` +
                `disagrees with SQL while TS matches it (${verdict.kind}, ` +
                `ts=${tsQ.length} go=${goQ.length} sql=${verdict.sqlCount}) — detail below`,
            );
            this.#captureDivergence(
              'go-vs-sql-drift',
              operation,
              qid,
              pipeline.transformedAst,
              tsQ,
              goQ,
              {go: verdict, ts: tsVerdict},
              diff,
            );
          } else {
            this.#shadowSqlUnreliable.add(1);
            this.#lc.info?.(
              `[shadow] ts/sql-oracle-divergence (${operation} ${qid}): SQL ` +
                `delta disagrees with BOTH engines (go=${verdict.kind}, ` +
                `ts=${tsVerdict.kind}, ts=${tsQ.length} go=${goQ.length} ` +
                `sql=${verdict.sqlCount}) — NOT a confirmed Go drift; raw detail below`,
            );
          }
        }
      }
    }

    if (tsNorm.length !== goNorm.length) {
      this.#lc.error?.(
        `[shadow] MISMATCH in ${operation} (${context}): ` +
          `TS produced ${tsNorm.length} changes, Go produced ${goNorm.length} changes`,
      );
      this.#logShadowDiff(operation, context, tsNorm, goNorm);
      this.#captureDivergence(
        'raw-mismatch',
        operation,
        context,
        ast ?? this.#pipelines.get(context)?.transformedAst,
        tsChanges,
        goChanges,
        undefined,
        diff,
      );
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
    if (mismatches > 0) {
      this.#captureDivergence(
        'raw-mismatch',
        operation,
        context,
        ast ?? this.#pipelines.get(context)?.transformedAst,
        tsChanges,
        goChanges,
        undefined,
        diff,
      );
      return;
    }

    // Op-kind parity (item #3). The row compare above is order-blind and keyed
    // by (type,queryID,table,rowKey) — so a row TS emits as `edit` while Go
    // emits as `remove`+`add` nets to the same final row and passes. That's a
    // real client-visible wire difference. Only meaningful for advances (a
    // hydrate is all-ADD on both sides, so opKindDiff is empty and this is a
    // cheap no-op).
    if (operation === 'advance') {
      const opDiffs = opKindDiff(tsChanges, goChanges);
      if (opDiffs.length > 0) {
        this.#shadowOpKindMismatches.add(opDiffs.length);
        const sample = opDiffs
          .slice(0, MAX_MISMATCH_LOG)
          .map(
            d => `${d.key}: TS[${d.tsTypes.join(',')}] Go[${d.goTypes.join(',')}]`,
          )
          .join(' ; ');
        this.#lc.error?.(
          `[shadow][opkind] ${operation} (${context}): ${opDiffs.length} row(s) ` +
            `with same final value but divergent change-kind — ${sample}`,
        );
      }
    }

    // Result-ORDER parity (TS reference, hydrate only). The set/value compare
    // above sorts both sides by rowKey, so it CANNOT see whether Go emits the
    // query's rows in the same ORDER TS does — which is exactly what the client
    // renders. Only meaningful for a hydrate (emission order == result order);
    // advance deltas have no result order, so skip them. `context` is the
    // queryID for hydrate ops, so the ORDER BY comes from the live pipeline.
    // Tie-aware: compare ORDER BY key-VALUE tuples of the ROOT-table rows, so a
    // permutation within an equal-key tie group (valid, and unspecified when the
    // sort lacks a PK tiebreak) does NOT fire. Reached only when the set/value
    // compare already matched, so a divergence here is purely positional.
    if (operation !== 'advance') {
      const orderAst = ast ?? this.#pipelines.get(context)?.transformedAst;
      if (orderAst?.orderBy && orderAst.orderBy.length > 0) {
        const orderFields = orderAst.orderBy.map(([fld]) => fld);
        const keyTuple = (c: RowChange) =>
          stableStringify(orderFields.map(fld => c.row[fld]));
        const tsSeq = tsChanges.filter(c => c.table === orderAst.table).map(keyTuple);
        const goSeq = goChanges.filter(c => c.table === orderAst.table).map(keyTuple);
        if (tsSeq.length === goSeq.length) {
          for (let i = 0; i < tsSeq.length; i++) {
            if (tsSeq[i] !== goSeq[i]) {
              this.#shadowOrderMismatches.add(1);
              const w = (s: string[]) =>
                s.slice(Math.max(0, i - 1), i + 2).join(' > ');
              this.#lc.error?.(
                `[shadow][order] ${operation} (${context}): Go result ORDER ` +
                  `diverges from TS at position ${i} — ts=[…${w(tsSeq)}…] ` +
                  `go=[…${w(goSeq)}…] orderBy=${JSON.stringify(orderAst.orderBy)}`,
              );
              break;
            }
          }
        }
      }
    }

    // Success matches are demoted to debug — at soak rates (~47/sec) this
    // info-level log was swamping production log pipelines and obscuring
    // real errors. REVIEW-final LOW-SHADOW-1.
    this.#lc.debug?.(
      `[shadow] ${operation} (${context}): TS and Go match ` +
        `(${tsNorm.length} changes)`,
    );
  }

  /**
   * Item #2 helper: apply Go's emitted advance deltas to the per-query
   * incremental accumulator. Tracks MAIN-table PK→projectedRow content (matching
   * buildAuditSQL's single-table oracle) and only for queries the audit has
   * already SEEDED (no entry ⇒ skip; the audit seeds from SQL). A dirty entry
   * (stream interrupted) is left untouched until the next clean reseed.
   *
   * #4b: ADD stores the projected row content (re-deleting + re-inserting the PK
   * to move it to MRU for LRU recency), EDIT updates the stored content, REMOVE
   * deletes. The Map is capped at {@link SQL_ORACLE_ROW_CAP} per query with LRU
   * eviction — evicting the OLDEST entry on overflow taints the query (dirty-on-
   * evict) so the next audit re-seeds cleanly rather than reconciling against a
   * truncated accumulator (a truncated membership/content set would produce false
   * `accumOnly` drift signals).
   */
  #accumulateGoDelta(goChanges: RowChange[]): void {
    for (const c of goChanges) {
      const qid = c.queryID;
      const map = this.#goDeltaAccum.get(qid);
      if (!map || this.#goDeltaAccumDirty.has(qid)) continue;
      const entry = this.#pipelines.get(qid);
      if (!entry || c.table !== entry.transformedAst.table) continue;
      const pk = stableStringify(c.rowKey);
      const spec = this.#tableSpecs.get(entry.transformedAst.table);
      if (c.type === ChangeType.ADD || c.type === ChangeType.EDIT) {
        // Project to schema columns (drop Go bookkeeping like _0_version) —
        // same projection as the SQL oracle. REMOVE carries no row content.
        let rowStr: string;
        if (spec) {
          const projected: Record<string, unknown> = {};
          for (const col of Object.keys(spec.zqlSpec)) projected[col] = c.row[col];
          rowStr = stableStringify(projected);
        } else {
          rowStr = stableStringify(c.row);
        }
        // LRU recency: re-insert at MRU. Map preserves insertion order, and
        // delete-then-set moves an existing key to the end (newest).
        map.delete(pk);
        map.set(pk, rowStr);
        // Cap with LRU eviction: evict the OLDEST entry on overflow.
        if (map.size > SQL_ORACLE_ROW_CAP) {
          // The first key is the least-recently-used.
          const oldest = map.keys().next().value;
          if (oldest !== undefined) {
            map.delete(oldest);
            // Dirty-on-evict: a truncated accumulator can't reconcile correctly
            // (the evicted row would later appear as a spurious `accumOnly` or
            // `contentMismatch`). Taint so the next audit re-seeds cleanly.
            this.#goDeltaAccumDirty.add(qid);
          }
        }
      } else if (c.type === ChangeType.REMOVE) {
        map.delete(pk);
      }
      // CHILD: not a main-table delta; ignored (the c.table === ast.table guard
      // already filters fan-out children, which carry the PARENT queryID).
    }
  }

  /**
   * Taint every incremental accumulator. Called when a cycle's Go output is
   * untrustworthy (advance mismatch, Go reset) so the incremental reconcile is
   * skipped until each query is cleanly re-seeded from SQL on its next audit.
   */
  #markAllDeltaAccumDirty(): void {
    for (const qid of this.#goDeltaAccum.keys()) this.#goDeltaAccumDirty.add(qid);
  }

  /**
   * Item #5 helper: record which column pgTypes an audited query SORTED by and
   * FILTERED on, then periodically log the coverage so untested type×operation
   * combinations are visible rather than silently never exercised.
   */
  #recordAuditTypeCoverage(ast: AST): void {
    const spec = this.#tableSpecs.get(ast.table);
    if (!spec) return;
    const typeOf = (col: string): string | undefined =>
      (spec.tableSpec.columns[col] as {dataType?: string} | undefined)?.dataType;
    for (const [field] of ast.orderBy ?? []) {
      const t = typeOf(field);
      if (t) this.#auditTypesSorted.add(t);
    }
    const walkWhere = (cond: Condition | undefined): void => {
      if (!cond) return;
      if (cond.type === 'simple') {
        if (cond.left.type === 'column') {
          const t = typeOf(cond.left.name);
          if (t) this.#auditTypesFiltered.add(t);
        }
      } else if (cond.type === 'and' || cond.type === 'or') {
        for (const c of cond.conditions) walkWhere(c);
      }
    };
    walkWhere(ast.where);

    const now = Date.now();
    if (now - this.#auditTypeCovLastReportMs < DRIFT_AUDIT_HEARTBEAT_MS) return;
    this.#auditTypeCovLastReportMs = now;
    const allTypes = new Set<string>();
    for (const col of Object.values(spec.tableSpec.columns)) {
      const t = (col as {dataType?: string}).dataType;
      if (t) allTypes.add(t);
    }
    const sortGaps = [...allTypes].filter(t => !this.#auditTypesSorted.has(t));
    const filterGaps = [...allTypes].filter(t => !this.#auditTypesFiltered.has(t));
    this.#lc.info?.(
      `[drift-audit][type-coverage] sorted={${[...this.#auditTypesSorted].join(',')}} ` +
        `filtered={${[...this.#auditTypesFiltered].join(',')}} ` +
        `UNSORTED_GAPS={${sortGaps.join(',')}} UNFILTERED_GAPS={${filterGaps.join(',')}}`,
    );
  }

  /**
   * Snapshotter-in-Go P1: compare the diff Go DERIVED itself (advanceToHead)
   * against the diff TS computed for the same advance (`tsChanges`, already
   * filtered to non-internal tables). This is a DIFF-level comparison (table +
   * prevValues + nextValue), distinct from #shadowCompare's RowChange-level one.
   *
   * Each change is reduced to an order-independent signature so the comparison
   * is insensitive to emission order and to prevValues ordering. Mismatches are
   * logged at error under [go-diff-shadow]; matches at debug. A Go-side reset
   * (schema/truncate/permissions) is logged and the comparison skipped — TS's
   * shadow advance suppresses resets, so the two aren't comparable that cycle.
   */
  #compareGoDerivedDiff(
    context: string,
    tsChanges: SnapshotChange[],
    goDerived: AdvanceToHeadResult,
  ): void {
    if (goDerived.reset) {
      this.#lc.info?.(
        `[go-diff-shadow] (${context}): Go reported reset ` +
          `${goDerived.reset.reason} (${goDerived.reset.msg}) — skipping diff compare`,
      );
      return;
    }

    const sigOf = (
      table: string,
      prevValues: Record<string, unknown>[],
      nextValue: Record<string, unknown> | null,
    ): string => {
      const prev = (prevValues ?? [])
        .map(r => stableStringify(r))
        .sort()
        .join(',');
      return `${table}|next=${stableStringify(nextValue ?? null)}|prev=[${prev}]`;
    };

    const tsSigs = tsChanges
      .map(c => sigOf(c.table, c.prevValues, c.nextValue))
      .sort();
    // Filter Go's changes to non-internal tables to match tsChanges (which the
    // caller already filtered). Go normally never sees internal tables, but
    // filtering defensively keeps the comparison apples-to-apples.
    const goSigs = goDerived.changes
      .filter(c => !this.#isInternalTable(c.table))
      .map(c => sigOf(c.table, c.prevValues, c.nextValue))
      .sort();

    if (tsSigs.length !== goSigs.length) {
      this.#lc.error?.(
        `[go-diff-shadow] MISMATCH (${context}): TS derived ${tsSigs.length} ` +
          `changes, Go derived ${goSigs.length} (ver=${goDerived.version}, ` +
          `rawChangeLog=${goDerived.numChanges})`,
      );
      this.#logGoDiffMismatch(tsSigs, goSigs);
      return;
    }
    for (let i = 0; i < tsSigs.length; i++) {
      if (tsSigs[i] !== goSigs[i]) {
        this.#lc.error?.(
          `[go-diff-shadow] MISMATCH (${context}) at #${i}: ` +
            `TS=${tsSigs[i]} Go=${goSigs[i]}`,
        );
        return;
      }
    }
    this.#lc.debug?.(
      `[go-diff-shadow] (${context}): TS and Go derived identical diffs ` +
        `(${tsSigs.length} changes)`,
    );
  }

  #logGoDiffMismatch(tsSigs: string[], goSigs: string[]): void {
    const tsSet = new Set(tsSigs);
    const goSet = new Set(goSigs);
    const onlyTS = tsSigs.filter(s => !goSet.has(s)).slice(0, 5);
    const onlyGo = goSigs.filter(s => !tsSet.has(s)).slice(0, 5);
    if (onlyTS.length) {
      this.#lc.error?.(`[go-diff-shadow] in TS not Go: ${JSON.stringify(onlyTS)}`);
    }
    if (onlyGo.length) {
      this.#lc.error?.(`[go-diff-shadow] in Go not TS: ${JSON.stringify(onlyGo)}`);
    }
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
    this.#lc.debug?.(
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
      // oxlint-disable-next-line e18e/prefer-array-to-sorted
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
export function pgTypeToGoType(
  pgType: string,
  warn?: (msg: string) => void,
): 'string' | 'number' | 'boolean' | 'null' | 'json' {
  // dataType may be in "lite type string" format: "bool|nn", "int4|nn",
  // "varchar(255)|nn" etc. Extract the upstream type (before any pipe
  // delimiter), strip any "(N)" args (e.g. char(32) → char), and lowercase —
  // exactly mirroring `formatTypeForLookup` in types/pg-data-type.ts so this
  // Go-dispatch mapping stays in lock-step with the canonical
  // `pgToZqlTypeMap`. MED-5/6/7/9: the previous hand-rolled list was a
  // divergent copy that dropped TIME/TIMETZ, bare INT, the SERIAL family,
  // bare FLOAT, and never stripped `(N)` (so `varchar(255)` fell through to
  // the unknown→string warn path). Keep this list byte-for-byte aligned with
  // pgToZqlTypeMap — if a type is added there, add it here too.
  const delim = pgType.indexOf('|');
  const upstream = delim > 0 ? pgType.substring(0, delim) : pgType;
  const argStart = upstream.indexOf('(');
  const t = (argStart > 0 ? upstream.substring(0, argStart) : upstream)
    .trim()
    .toUpperCase();
  // Arrays (including enum-arrays) map to 'json': the replicator stores ALL
  // array columns as JSON.stringify'd text in SQLite, and both sides must
  // JSON.parse them into real arrays. This MUST be checked before the enum
  // check below — an enum-ARRAY (e.g. `TicketPriority[]|TEXT_ENUM|TEXT_ARRAY`)
  // carries BOTH the |TEXT_ENUM and |TEXT_ARRAY attributes (plus `[]`), but
  // the CONTAINER is JSON, so the array property takes precedence over the
  // enum-ness (which only affects how individual elements are compared).
  // Without this, enum-arrays hit isLiteEnum → 'string', causing Go's
  // FromSQLiteType('string', ...) to skip JSON parsing → a +2-byte drift
  // per enum-array column (two extra `\"` chars from JSON.stringify on the
  // Go side vs a bare array on the TS side). isArray also catches the legacy
  // `|TEXT_ARRAY[]` form that the plain `t.endsWith('[]')` missed.
  if (isArray(pgType)) return 'json';
  // Enums: the LiteTypeString carries a `|TEXT_ENUM` attribute (e.g.
  // `TicketPriority|NOT_NULL|TEXT_ENUM`) that the upstream-name extraction above
  // strips, so a user-defined enum name fell through to the "unrecognised type"
  // warning. Enums are TEXT-backed and compared as their string labels on BOTH
  // sides (the SQLite replica stores them as TEXT); the canonical TS mapper
  // (`dataTypeToZqlValueType`) likewise returns 'string' for enums. Map to
  // 'string' WITHOUT a warning — this is correct, not a gap. Checked before the
  // name lookups so an enum named like a builtin can't be mis-typed.
  if (isLiteEnum(pgType)) return 'string';
  if (t === 'BOOL' || t === 'BOOLEAN') return 'boolean';
  if (
    // Integer + serial families (PG rewrites SERIAL → INTEGER, but the
    // declared type may still surface as serial in a lite type string).
    t === 'SMALLINT' || t === 'INTEGER' || t === 'INT' ||
    t === 'INT2' || t === 'INT4' || t === 'INT8' || t === 'BIGINT' ||
    t === 'SMALLSERIAL' || t === 'SERIAL' ||
    t === 'SERIAL2' || t === 'SERIAL4' || t === 'SERIAL8' ||
    t === 'BIGSERIAL' ||
    // Real / floating / fixed-point.
    t === 'REAL' || t === 'DOUBLE PRECISION' ||
    t === 'FLOAT' || t === 'FLOAT4' || t === 'FLOAT8' ||
    t === 'NUMERIC' || t === 'DECIMAL' ||
    // Date / time — all mapped to number (epoch-ish) like the canonical map.
    t === 'DATE' ||
    t === 'TIME' || t === 'TIMETZ' ||
    t === 'TIME WITH TIME ZONE' || t === 'TIME WITHOUT TIME ZONE' ||
    t === 'TIMESTAMP' || t === 'TIMESTAMPTZ' ||
    t === 'TIMESTAMP WITH TIME ZONE' ||
    t === 'TIMESTAMP WITHOUT TIME ZONE'
  ) return 'number';
  if (t === 'JSON' || t === 'JSONB') return 'json';
  // Explicitly recognised string-shaped types — keep this list growing.
  if (
    t === 'TEXT' || t === 'VARCHAR' || t === 'CHARACTER VARYING' ||
    t === 'CHAR' || t === 'CHARACTER' || t === 'BPCHAR' ||
    t === 'UUID' || t === 'CITEXT' || t === 'NAME'
  ) return 'string';
  // Postgres array types (e.g. INT4[], TEXT[]) are handled by the early
  // `isArray` check above (which also catches enum-arrays and the legacy
  // |TEXT_ARRAY[] form). The previous `t.endsWith('[]')` here was redundant
  // and missed the enum-array case (isLiteEnum fired first → 'string').
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
