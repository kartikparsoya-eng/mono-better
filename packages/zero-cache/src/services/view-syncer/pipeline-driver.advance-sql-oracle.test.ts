import {describe, expect, test} from 'vitest';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import {
  compareAdvanceDeltaToSqlDelta,
  type RowChange,
} from './pipeline-driver.ts';

// compareAdvanceDeltaToSqlDelta is the pure core of the advance-path SQL
// ground-truth oracle (#1). Given the snapshotter's prev/curr main-table rows
// for ONE query (already queried + normalized) and Go's emitted advance
// changes for that query, it derives the expected prev→curr delta (ADD /
// REMOVE / EDIT) and compares it to Go's delta — returning the SAME verdict
// shape the hydrate oracle uses (confirmed / go-vs-sql-drift / go-vs-sql-
// content-drift / oracle-blind / skipped) so #shadowCompare's advance branch
// can reuse the existing classify + counters.
//
// Extracted as an exported pure function (mirroring isAdvanceFrameSkew) so the
// delta-derivation + comparison is unit-testable without a live SQLite
// replica. Every test below pins one verdict boundary deterministically.

const TABLE = 'issues';
const PK = ['id'];
// Schema columns the oracle projects Go's row to (drops Go bookkeeping like
// _0_version). Matches the hydrate oracle's :2969-2970 projection.
const ZQL_COLUMNS = ['id', 'closed'];

const add = (id: string, closed: boolean): RowChange =>
  ({
    type: ChangeType.ADD,
    queryID: 'q',
    table: TABLE,
    rowKey: {id},
    row: {id, closed, _0_version: '123'},
  }) as unknown as RowChange;

const edit = (id: string, closed: boolean): RowChange =>
  ({
    type: ChangeType.EDIT,
    queryID: 'q',
    table: TABLE,
    rowKey: {id},
    row: {id, closed, _0_version: '456'},
  }) as unknown as RowChange;

const remove = (id: string): RowChange =>
  ({
    type: ChangeType.REMOVE,
    queryID: 'q',
    table: TABLE,
    rowKey: {id},
    row: {id},
  }) as unknown as RowChange;

// SQL-side rows as the oracle reads them from the replica (already normalized
// — no _0_version, the column set is exactly ZQL_COLUMNS).
const sqlRow = (id: string, closed: boolean): Record<string, unknown> => ({
  id,
  closed,
});

describe('view-syncer/pipeline-driver: compareAdvanceDeltaToSqlDelta (advance SQL oracle core)', () => {
  test('confirmed: ADD matches — Go adds a row SQL also newly has', () => {
    const prev = [sqlRow('1', false)];
    const curr = [sqlRow('1', false), sqlRow('2', true)];
    const go = [add('2', true)];
    expect(compareAdvanceDeltaToSqlDelta(TABLE, PK, ZQL_COLUMNS, prev, curr, go)).toEqual({
      kind: 'confirmed',
      sqlCount: 2,
    });
  });

  test('confirmed: EDIT matches — Go edits a row SQL also changed', () => {
    const prev = [sqlRow('1', false)];
    const curr = [sqlRow('1', true)];
    const go = [edit('1', true)];
    expect(compareAdvanceDeltaToSqlDelta(TABLE, PK, ZQL_COLUMNS, prev, curr, go)).toEqual({
      kind: 'confirmed',
      sqlCount: 1,
    });
  });

  test('confirmed: REMOVE matches — Go removes a row SQL also dropped', () => {
    const prev = [sqlRow('1', false), sqlRow('2', true)];
    const curr = [sqlRow('1', false)];
    const go = [remove('2')];
    expect(compareAdvanceDeltaToSqlDelta(TABLE, PK, ZQL_COLUMNS, prev, curr, go)).toEqual({
      kind: 'confirmed',
      sqlCount: 1,
    });
  });

  test('confirmed: mixed ADD+EDIT+REMOVE all match', () => {
    const prev = [sqlRow('1', false), sqlRow('2', true)];
    const curr = [sqlRow('1', true), sqlRow('3', false)];
    // SQL delta: edit 1 (false→true), remove 2, add 3.
    const go = [edit('1', true), remove('2'), add('3', false)];
    expect(compareAdvanceDeltaToSqlDelta(TABLE, PK, ZQL_COLUMNS, prev, curr, go)).toEqual({
      kind: 'confirmed',
      sqlCount: 2,
    });
  });

  test('oracle-blind: prev==curr (no SQL delta) AND Go idle on main table', () => {
    // No SQL delta (prev==curr) AND Go emitted nothing for this query's main
    // table. The pure core returns `oracle-blind` here: this combination only
    // reaches the oracle when #shadowCompare already saw bagsDiffer on this
    // queryID — meaning the divergence is entirely off-table (fan-out), which
    // the main-table oracle cannot adjudicate. (A TRUE confirmed no-op — both
    // engines idle on a query that didn't diverge — never reaches the oracle,
    // because bagsDiffer gates the whole advance-oracle block.)
    const prev = [sqlRow('1', false), sqlRow('2', true)];
    const curr = [sqlRow('1', false), sqlRow('2', true)];
    expect(compareAdvanceDeltaToSqlDelta(TABLE, PK, ZQL_COLUMNS, prev, curr, [])).toEqual({
      kind: 'oracle-blind',
      sqlCount: 2,
    });
  });

  test('go-vs-sql-drift: Go emits a wrong PK (add a row SQL does not have)', () => {
    const prev = [sqlRow('1', false)];
    const curr = [sqlRow('1', false), sqlRow('2', true)];
    // SQL expects add(2); Go adds a DIFFERENT row (3) instead.
    const go = [add('3', true)];
    const verdict = compareAdvanceDeltaToSqlDelta(TABLE, PK, ZQL_COLUMNS, prev, curr, go);
    expect(verdict.kind).toBe('go-vs-sql-drift');
    if (verdict.kind === 'go-vs-sql-drift') {
      // Go emitted add(3) which SQL doesn't expect → goOnly.
      expect(verdict.goOnly).toEqual([stableRowKey('3')]);
      // SQL expected add(2) which Go didn't emit → sqlOnly.
      expect(verdict.sqlOnly).toEqual([stableRowKey('2')]);
      expect(verdict.sqlCount).toBe(2);
    }
  });

  test('go-vs-sql-drift: SQL expects a REMOVE Go never emitted', () => {
    const prev = [sqlRow('1', false), sqlRow('2', true)];
    const curr = [sqlRow('1', false)];
    // SQL expects remove(2); Go emits nothing.
    const go: RowChange[] = [];
    const verdict = compareAdvanceDeltaToSqlDelta(TABLE, PK, ZQL_COLUMNS, prev, curr, go);
    expect(verdict.kind).toBe('go-vs-sql-drift');
    if (verdict.kind === 'go-vs-sql-drift') {
      expect(verdict.sqlOnly).toEqual([stableRowKey('2')]);
      expect(verdict.goOnly).toEqual([]);
    }
  });

  test('go-vs-sql-content-drift: same PK, wrong content on an ADD', () => {
    const prev = [sqlRow('1', false)];
    const curr = [sqlRow('1', false), sqlRow('2', true)];
    // SQL expects add(2, true); Go adds(2, false) — same PK, wrong content.
    const go = [add('2', false)];
    const verdict = compareAdvanceDeltaToSqlDelta(TABLE, PK, ZQL_COLUMNS, prev, curr, go);
    expect(verdict.kind).toBe('go-vs-sql-content-drift');
    if (verdict.kind === 'go-vs-sql-content-drift') {
      expect(verdict.contentMismatches).toHaveLength(1);
      expect(verdict.contentMismatches[0].pk).toBe(stableRowKey('2'));
      expect(verdict.contentMismatches[0].sqlRow).toBe(stableStringify({id: '2', closed: true}));
      expect(verdict.contentMismatches[0].goRow).toBe(stableStringify({id: '2', closed: false}));
    }
  });

  test('go-vs-sql-content-drift: same PK, wrong content on an EDIT', () => {
    const prev = [sqlRow('1', false)];
    const curr = [sqlRow('1', true)];
    // SQL expects edit(1, true); Go edits(1, false) — same PK, wrong new content.
    const go = [edit('1', false)];
    const verdict = compareAdvanceDeltaToSqlDelta(TABLE, PK, ZQL_COLUMNS, prev, curr, go);
    expect(verdict.kind).toBe('go-vs-sql-content-drift');
    if (verdict.kind === 'go-vs-sql-content-drift') {
      expect(verdict.contentMismatches).toHaveLength(1);
      expect(verdict.contentMismatches[0].pk).toBe(stableRowKey('1'));
      // SQL curr content is {id:1, closed:true}; Go projected row is {id:1, closed:false}.
      expect(verdict.contentMismatches[0].sqlRow).toBe(stableStringify({id: '1', closed: true}));
      expect(verdict.contentMismatches[0].goRow).toBe(stableStringify({id: '1', closed: false}));
    }
  });

  test('go-vs-sql-content-drift: same PK, different op kind (SQL=edit, Go=add)', () => {
    // SQL sees row 1 change false→true (an EDIT); Go emits it as an ADD (as if
    // the row were new). Same PK, different op kind → a real wire divergence
    // the content-mismatch branch surfaces (prefixed with the type).
    const prev = [sqlRow('1', false)];
    const curr = [sqlRow('1', true)];
    const go = [add('1', true)];
    const verdict = compareAdvanceDeltaToSqlDelta(TABLE, PK, ZQL_COLUMNS, prev, curr, go);
    expect(verdict.kind).toBe('go-vs-sql-content-drift');
    if (verdict.kind === 'go-vs-sql-content-drift') {
      expect(verdict.contentMismatches).toHaveLength(1);
      // sqlRow is "edit:<content>"; goRow is "add:<content>" — the type prefix
      // distinguishes op-kind mismatch from same-kind content mismatch.
      expect(verdict.contentMismatches[0].sqlRow).toMatch(/^edit:/);
      expect(verdict.contentMismatches[0].goRow).toMatch(/^add:/);
    }
  });

  test('oracle-blind: divergence entirely off-table — no main-table delta on either side', () => {
    // prev==curr on the main table (no SQL delta), AND Go emitted no main-table
    // changes for this query (only off-table fan-out, filtered out by the
    // c.table === ast.table guard). The oracle cannot adjudicate fan-out.
    const prev = [sqlRow('1', false)];
    const curr = [sqlRow('1', false)];
    // Go's changes are all on a DIFFERENT (related) table — filtered out.
    const go: RowChange[] = [
      {
        type: ChangeType.ADD,
        queryID: 'q',
        table: 'comments',
        rowKey: {id: 'c1'},
        row: {id: 'c1', issueID: '1'},
      } as unknown as RowChange,
    ];
    const verdict = compareAdvanceDeltaToSqlDelta(TABLE, PK, ZQL_COLUMNS, prev, curr, go);
    expect(verdict).toEqual({kind: 'oracle-blind', sqlCount: 1});
  });

  test('main-table filter: Go changes on other tables are ignored (confirmed on main)', () => {
    // SQL delta = add(2). Go emits add(2) on issues + fan-out on comments.
    // The comments changes must be filtered out; main-table delta matches.
    const prev = [sqlRow('1', false)];
    const curr = [sqlRow('1', false), sqlRow('2', true)];
    const go: RowChange[] = [
      add('2', true),
      {
        type: ChangeType.ADD,
        queryID: 'q',
        table: 'comments',
        rowKey: {id: 'c1'},
        row: {id: 'c1', issueID: '2'},
      } as unknown as RowChange,
    ];
    expect(compareAdvanceDeltaToSqlDelta(TABLE, PK, ZQL_COLUMNS, prev, curr, go)).toEqual({
      kind: 'confirmed',
      sqlCount: 2,
    });
  });

  test('projection drops Go bookkeeping columns (_0_version) before content compare', () => {
    // Go's row carries _0_version='999' which is NOT in ZQL_COLUMNS. SQL's row
    // has no _0_version. The projection must drop it so the ADD still confirms
    // (otherwise the content strings would differ on the bookkeeping field).
    const prev: Record<string, unknown>[] = [];
    const curr = [sqlRow('1', false)]; // SQL row: {id:1, closed:false}
    const go: RowChange[] = [
      {
        type: ChangeType.ADD,
        queryID: 'q',
        table: TABLE,
        rowKey: {id: '1'},
        row: {id: '1', closed: false, _0_version: '999'},
      } as unknown as RowChange,
    ];
    expect(compareAdvanceDeltaToSqlDelta(TABLE, PK, ZQL_COLUMNS, prev, curr, go)).toEqual({
      kind: 'confirmed',
      sqlCount: 1,
    });
  });
});

// Helpers — replicate stableStringify's deep-sorted-key shape so the test can
// assert exact content-mismatch strings. (stableStringify is private to the
// module; we reconstruct the same canonical form: JSON.stringify with sorted
// top-level keys, recursively sorted.) Keep these local + minimal.
function stableStringify(v: unknown): string {
  return JSON.stringify(sortDeep(v));
}

function sortDeep(v: unknown): unknown {
  if (Array.isArray(v)) return v.map(sortDeep);
  if (v && typeof v === 'object') {
    const o = v as Record<string, unknown>;
    return Object.keys(o)
      .sort()
      .reduce<Record<string, unknown>>((acc, k) => {
        acc[k] = sortDeep(o[k]);
        return acc;
      }, {});
  }
  return v;
}

function stableRowKey(id: string): string {
  // pkOf in the oracle: stableStringify({id}).
  return stableStringify({id});
}
