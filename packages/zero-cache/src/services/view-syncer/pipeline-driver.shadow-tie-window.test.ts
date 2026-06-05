import {describe, expect, test} from 'vitest';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {isShadowTieWindow, type RowChange} from './pipeline-driver.ts';

// isShadowTieWindow decides whether a TS-vs-Go set difference in shadow mode is
// a BENIGN LIMIT tie-member swap (Go and TS kept different members of a tie
// group straddling the window boundary — nondeterministic, not a Go bug) vs a
// REAL divergence that must stay a [shadow] MISMATCH. The soak can only hit the
// tie condition probabilistically, so these pin the decision boundaries
// deterministically. Reproduces the exact shape the go-primary soak confirmed
// (many tickets sharing one updatedAt after a bulk mutation).

const add = (
  table: string,
  id: string,
  updatedAt: number,
  extra: Record<string, unknown> = {},
): RowChange =>
  ({
    type: ChangeType.ADD,
    queryID: 'q',
    table,
    rowKey: {id},
    row: {id, updatedAt, ...extra},
  }) as unknown as RowChange;

const ast = (over: Partial<AST> = {}): AST =>
  ({
    table: 'tickets',
    orderBy: [['updatedAt', 'desc']],
    limit: 3,
    ...over,
  }) as unknown as AST;

describe('view-syncer/pipeline-driver: isShadowTieWindow', () => {
  test('benign: pure tie-member swap in the LIMIT window → suppressed', () => {
    // All rows share updatedAt=100 (one big tie); TS kept C, Go kept D.
    const ts = [add('tickets', 'A', 100), add('tickets', 'B', 100), add('tickets', 'C', 100)];
    const go = [add('tickets', 'A', 100), add('tickets', 'B', 100), add('tickets', 'D', 100)];
    expect(isShadowTieWindow(ast(), ts, go)).toBe(true);
  });

  test('benign: distinct head rows match, only the tied tail swaps', () => {
    // H is the unambiguous top (updatedAt=200); A,B,C tie at 100.
    const ts = [add('tickets', 'H', 200), add('tickets', 'A', 100), add('tickets', 'B', 100)];
    const go = [add('tickets', 'H', 200), add('tickets', 'A', 100), add('tickets', 'C', 100)];
    expect(isShadowTieWindow(ast(), ts, go)).toBe(true);
  });

  test('REAL: a differing row whose sort key is NOT a boundary tie → kept', () => {
    // Go fetched D with updatedAt=50, a value absent from TS's window — that is
    // a row past the boundary, not a tie swap. Must stay a MISMATCH.
    const ts = [add('tickets', 'A', 100), add('tickets', 'B', 100), add('tickets', 'C', 100)];
    const go = [add('tickets', 'A', 100), add('tickets', 'B', 100), add('tickets', 'D', 50)];
    expect(isShadowTieWindow(ast(), ts, go)).toBe(false);
  });

  test('REAL: same rowKey but different content (value drift) → kept', () => {
    const ts = [add('tickets', 'A', 100, {status: 'OPEN'})];
    const go = [add('tickets', 'A', 100, {status: 'CLOSED'})];
    expect(isShadowTieWindow(ast({limit: 1}), ts, go)).toBe(false);
  });

  test('REAL: no orderBy → a set diff must match exactly', () => {
    const ts = [add('tickets', 'A', 100), add('tickets', 'B', 100)];
    const go = [add('tickets', 'A', 100), add('tickets', 'C', 100)];
    expect(isShadowTieWindow(ast({orderBy: undefined}), ts, go)).toBe(false);
  });

  test('REAL: no limit → full result, any set diff is real', () => {
    const ts = [add('tickets', 'A', 100), add('tickets', 'B', 100)];
    const go = [add('tickets', 'A', 100), add('tickets', 'C', 100)];
    expect(isShadowTieWindow(ast({limit: undefined}), ts, go)).toBe(false);
  });

  test('REAL: child/related-table rows present → not classified (conservative)', () => {
    // A root tie-swap drags child rows we can't reason about, so any non-root
    // table present disqualifies suppression entirely.
    const ts = [add('tickets', 'A', 100), add('ticket_assignments', 'x', 100)];
    const go = [add('tickets', 'B', 100), add('ticket_assignments', 'y', 100)];
    expect(isShadowTieWindow(ast(), ts, go)).toBe(false);
  });

  test('no divergence at all (identical sets) → false (nothing to suppress)', () => {
    const rows = [add('tickets', 'A', 100), add('tickets', 'B', 100)];
    expect(isShadowTieWindow(ast(), rows, rows)).toBe(false);
  });

  test('REAL: asymmetric counts (one side has an extra row) → kept', () => {
    // Unequal counts are NOT a clean boundary swap — could be a real row drop.
    // Conservatively keep it as a MISMATCH rather than suppress.
    const ts = [add('tickets', 'A', 100), add('tickets', 'B', 100)];
    const go = [add('tickets', 'A', 100), add('tickets', 'B', 100), add('tickets', 'D', 100)];
    expect(isShadowTieWindow(ast(), ts, go)).toBe(false);
  });
});
