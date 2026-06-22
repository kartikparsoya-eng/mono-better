import {describe, expect, test} from 'vitest';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import {
  isAdvanceFrameSkew,
  type RowChange,
} from './pipeline-driver.ts';

// isAdvanceFrameSkew decides whether an `advance`-path TS-vs-Go set difference
// is a BENIGN cross-batch frame-skew split (Go's snapshotter and TS's placed the
// same logical changes in different advance batches — independently-pinned WAL
// frames) vs a REAL divergence that must stay a [shadow] MISMATCH. The advance
// path has no single AST, so the SQL oracle that adjudicates batch-hydrate can't
// run here, and a frame-skew split can be hundreds of rows (the go-primary soak
// confirmed a 588-row channel_participants fan-out that landed entirely in TS's
// batch for one advance window). That scale of false alarm is exactly where a
// genuine 1-row Go bug would hide, so the classifier restores the advance
// MISMATCH as a trustworthy signal.
//
// The benign shape is a CLEAN PARTITION: TS-only and Go-only sides are disjoint
// on rowKey, no rowKey appears on both sides with differing type or content,
// each (queryID, table, rowKey, type) tuple appears at most once across the
// union, and BOTH sides carry exclusive rows. Every test below pins one
// boundary of that decision deterministically. Reproduces the shape the
// go-primary soak + the go-ivm advance_drift_shadow_mismatch repro tests
// confirmed (the ae0f0bc7 channel_user_status edit flipping Go-only in advance 1
// → TS-only in advance 2; the 588-row channel_participants fan-out split).

const add = (
  table: string,
  id: string,
  extra: Record<string, unknown> = {},
): RowChange =>
  ({
    type: ChangeType.ADD,
    queryID: 'q',
    table,
    rowKey: {id},
    row: {id, ...extra},
  }) as unknown as RowChange;

const edit = (
  table: string,
  id: string,
  row: Record<string, unknown>,
): RowChange =>
  ({
    type: ChangeType.EDIT,
    queryID: 'q',
    table,
    rowKey: {id},
    row,
  }) as unknown as RowChange;

const remove = (table: string, id: string): RowChange =>
  ({
    type: ChangeType.REMOVE,
    queryID: 'q',
    table,
    rowKey: {id},
    row: {id},
  }) as unknown as RowChange;

describe('view-syncer/pipeline-driver: isAdvanceFrameSkew', () => {
  test('benign: disjoint ADD fan-out split across two batches → suppressed', () => {
    // TS's batch carried A,B,C; Go's carried D,E,F — same logical fan-out, split
    // across adjacent advance batches (the 588-row channel_participants shape).
    const ts = [add('cp', 'A'), add('cp', 'B'), add('cp', 'C')];
    const go = [add('cp', 'D'), add('cp', 'E'), add('cp', 'F')];
    expect(isAdvanceFrameSkew(ts, go)).toBe(true);
  });

  test('benign: same rowKey on both sides, identical content, plus exclusive rows → suppressed', () => {
    // Shared rows (B) agree exactly; the divergence is only the exclusive split
    // (A on TS, C on Go). A clean partition can include shared agreeing rows.
    const ts = [add('cp', 'A'), add('cp', 'B', {v: 1})];
    const go = [add('cp', 'B', {v: 1}), add('cp', 'C')];
    expect(isAdvanceFrameSkew(ts, go)).toBe(true);
  });

  test('benign: the ae0f0bc7 advance flip — one row exclusive per side → suppressed', () => {
    // Advance 1 had ae0f0bc7 Go-only; advance 2 had it TS-only. Each single
    // advance comparison sees the row on exactly one side — a 1-row split.
    const ts = [edit('cus', 'ae0f0bc7', {seen: 100})];
    const go = [edit('cus', 'bf10cafe', {seen: 100})];
    expect(isAdvanceFrameSkew(ts, go)).toBe(true);
  });

  test('benign: mixed ADD/EDIT/REMOVE across tables, clean partition → suppressed', () => {
    // The partition signature is structural — no AST needed, works across
    // change kinds and tables in one advance window.
    const ts = [add('cp', 'A'), edit('cus', 'B', {v: 2}), remove('msg', 'C')];
    const go = [add('cp', 'D'), edit('cus', 'E', {v: 2}), remove('msg', 'F')];
    expect(isAdvanceFrameSkew(ts, go)).toBe(true);
  });

  test('benign: different queryIDs, disjoint rowKeys → suppressed', () => {
    // A partition where the split also falls along queryID boundaries (Go's
    // batch advanced one query, TS's another) is still a clean partition.
    const ts = [
      {...add('cp', 'A'), queryID: 'q1'} as unknown as RowChange,
      {...add('cp', 'B'), queryID: 'q1'} as unknown as RowChange,
    ];
    const go = [
      {...add('cp', 'C'), queryID: 'q2'} as unknown as RowChange,
      {...add('cp', 'D'), queryID: 'q2'} as unknown as RowChange,
    ];
    expect(isAdvanceFrameSkew(ts, go)).toBe(true);
  });

  test('REAL: same rowKey on both sides with differing content → kept', () => {
    // This is the case that matters most — a genuine value drift on a shared
    // key must NEVER be suppressed. (A real 1-row Go bug hiding under a 588-row
    // false alarm would look exactly like this and must survive.)
    const ts = [edit('cus', 'ae0f0bc7', {seen: 100})];
    const go = [edit('cus', 'ae0f0bc7', {seen: 200})];
    expect(isAdvanceFrameSkew(ts, go)).toBe(false);
  });

  test('REAL: same rowKey on both sides with differing change kind → kept', () => {
    // ADD on one side, REMOVE on the other for the same key — not a batch split.
    const ts = [add('cp', 'A')];
    const go = [remove('cp', 'A')];
    expect(isAdvanceFrameSkew(ts, go)).toBe(false);
  });

  test('REAL: one side empty (pure drop/add) → kept', () => {
    // A clean partition requires BOTH sides to carry exclusive rows. An empty
    // other side is a pure drop (or pure add), not a split — could be a real
    // row-drop, so keep it as a MISMATCH.
    const ts = [add('cp', 'A'), add('cp', 'B')];
    const go: RowChange[] = [];
    expect(isAdvanceFrameSkew(ts, go)).toBe(false);
  });

  test('REAL: duplicate tuple on one side (multiplicity divergence) → kept', () => {
    // The clean-partition invariant needs each (queryID,table,rowKey,type) at
    // most once per side. A duplicate is a fan-out/multiplicity divergence.
    const ts = [add('cp', 'A'), add('cp', 'A')];
    const go = [add('cp', 'B'), add('cp', 'C')];
    expect(isAdvanceFrameSkew(ts, go)).toBe(false);
  });

  test('REAL: asymmetric counts with a shared agreeing row + one exclusive → kept', () => {
    // Counts differ (3 vs 2), one row is shared-and-agreeing, but only ONE side
    // has an exclusive row — the other's extra is the shared one. Not a clean
    // both-sides-exclusive partition, so keep it.
    const ts = [add('cp', 'A'), add('cp', 'B'), add('cp', 'C')];
    const go = [add('cp', 'A'), add('cp', 'B')];
    expect(isAdvanceFrameSkew(ts, go)).toBe(false);
  });

  test('no divergence (identical sets) → false (nothing to suppress)', () => {
    const rows = [add('cp', 'A'), add('cp', 'B')];
    expect(isAdvanceFrameSkew(rows, rows)).toBe(false);
  });

  test('no divergence across mixed kinds (identical) → false', () => {
    const rows = [add('cp', 'A'), edit('cus', 'B', {v: 1}), remove('msg', 'C')];
    expect(isAdvanceFrameSkew(rows, rows)).toBe(false);
  });
});
