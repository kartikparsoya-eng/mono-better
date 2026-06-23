import {describe, expect, test} from 'vitest';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import {
  isAdvanceFrameSkew,
  isAdvanceFrameSkewCrossBatch,
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

  test('REAL: one side empty, no neighbor (pure drop/add) → kept', () => {
    // A clean partition requires BOTH sides to carry exclusive rows. An empty
    // other side is a pure drop (or pure add), not a split — could be a real
    // row-drop, so keep it as a MISMATCH. The intra-frame classifier has no
    // neighbor to consult, so this stays a MISMATCH. (The cross-batch
    // classifier below handles the case WHERE a neighbor carries the match.)
    const ts = [add('cp', 'A'), add('cp', 'B')];
    const go: RowChange[] = [];
    expect(isAdvanceFrameSkew(ts, go)).toBe(false);
    // And the cross-batch classifier with no neighbor also keeps it.
    expect(isAdvanceFrameSkewCrossBatch(ts, go, null)).toBe(false);
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

// isAdvanceFrameSkewCrossBatch closes the empty-side gap in isAdvanceFrameSkew.
// The intra-frame classifier (above) only suppresses a CLEAN PARTITION where
// BOTH sides carry exclusive rows — so a one-sided advance batch (one engine
// empty, the other not) falls through as a raw MISMATCH. But the same WAL
// frame-skew that produces a both-sides split can also place ALL of a logical
// change in one engine's batch here and NONE in the other's, with the missing
// rows appearing on the OTHER engine in the adjacent (poke-paired) advance
// batch. Live-proven 2026-06-22: frames 81b3tyfhi0 (TS=1/Go=4) and 81b3tyh9ug
// (TS=3/Go=0), byte-identical rows — the intra-frame classifier ran but the
// empty-side guard blocked suppression. This classifier consults a 1-deep
// neighbor buffer and suppresses ONLY when the non-empty side's rows appear
// byte-identical on the OPPOSITE engine in the neighbor. Every test pins one
// boundary of that decision; the false-negative guards (full-content equality,
// 1-deep buffer, no multiplicity) are each asserted explicitly.
describe('view-syncer/pipeline-driver: isAdvanceFrameSkewCrossBatch', () => {
  test('benign: TS empty here, Go-only rows match TS-only in neighbor → suppressed', () => {
    // This batch: Go has A,B; TS empty. Neighbor (prior advance): TS has A,B;
    // Go empty. The frame-skew split put A,B in Go's batch this advance and
    // TS's batch the adjacent advance — byte-identical, so suppress.
    const here = {ts: [] as RowChange[], go: [add('cp', 'A'), add('cp', 'B')]};
    const neighbor = {ts: [add('cp', 'A'), add('cp', 'B')], go: [] as RowChange[]};
    expect(isAdvanceFrameSkewCrossBatch(here.ts, here.go, neighbor)).toBe(true);
  });

  test('benign: Go empty here, TS-only rows match Go-only in neighbor → suppressed', () => {
    // Symmetric: the rows are on TS this advance and on Go in the neighbor.
    const here = {ts: [edit('cus', 'ae0f0bc7', {seen: 100})], go: [] as RowChange[]};
    const neighbor = {ts: [] as RowChange[], go: [edit('cus', 'ae0f0bc7', {seen: 100})]};
    expect(isAdvanceFrameSkewCrossBatch(here.ts, here.go, neighbor)).toBe(true);
  });

  test('benign: the 2026-06-22 live shape (frames 81b3tyfhi0 / 81b3tyh9ug) → suppressed', () => {
    // Frame 81b3tyfhi0: TS=1, Go=4. Frame 81b3tyh9ug (the neighbor): TS=3, Go=0.
    // The TS row in fhi0 is byte-identical to one of the TS rows in h9ug's
    // neighbor side; the Go rows in fhi0 are byte-identical to Go rows in the
    // h9ug side. One-sided each batch, matching the opposite engine across the
    // pair — the exact cross-batch empty-side split.
    const fhi0 = {ts: [add('cp', 'X')], go: [add('cp', 'A'), add('cp', 'B'), add('cp', 'C'), add('cp', 'D')]};
    // Neighbor as seen from fhi0: its go-side rows must match fhi0's go-side;
    // its ts-side (the OTHER engine) must carry fhi0's ts-side row.
    const neighbor = {ts: [add('cp', 'X')], go: [add('cp', 'A'), add('cp', 'B'), add('cp', 'C'), add('cp', 'D')]};
    // Here fhi0 is NOT one-sided (both sides non-empty), so cross-batch does
    // NOT fire — the intra-frame classifier handles both-sides splits. Assert
    // cross-batch correctly defers (returns false) for non-one-sided batches.
    expect(isAdvanceFrameSkewCrossBatch(fhi0.ts, fhi0.go, neighbor)).toBe(false);
    // The actual one-sided live case: take fhi0's TS row alone (TS=1, Go=0)
    // matching the neighbor's Go side (Go carries it there).
    const oneSided = {ts: [add('cp', 'X')], go: [] as RowChange[]};
    const nbr = {ts: [] as RowChange[], go: [add('cp', 'X')]};
    expect(isAdvanceFrameSkewCrossBatch(oneSided.ts, oneSided.go, nbr)).toBe(true);
  });

  test('REAL: no neighbor → kept (cannot look across batches)', () => {
    const here = {ts: [] as RowChange[], go: [add('cp', 'A'), add('cp', 'B')]};
    expect(isAdvanceFrameSkewCrossBatch(here.ts, here.go, null)).toBe(false);
  });

  test('REAL: neighbor opposite-side empty → kept (no match to consult)', () => {
    // Here: Go has rows, TS empty. Neighbor's TS side (the opposite engine) is
    // ALSO empty — no match possible. Keep as MISMATCH (could be a real drop).
    const here = {ts: [] as RowChange[], go: [add('cp', 'A')]};
    const neighbor = {ts: [] as RowChange[], go: [add('cp', 'Z')]};
    expect(isAdvanceFrameSkewCrossBatch(here.ts, here.go, neighbor)).toBe(false);
  });

  test('REAL: neighbor has same PK but DIFFERENT content → kept (false-negative guard)', () => {
    // The decisive guard: a real drop whose PK happens to match a neighbor
    // re-emit, but with different content, must NOT suppress. Full RowChange
    // content equality (not just PK) is what keeps this as a MISMATCH.
    const here = {ts: [] as RowChange[], go: [edit('cus', 'ae0f0bc7', {seen: 100})]};
    const neighbor = {ts: [edit('cus', 'ae0f0bc7', {seen: 999})], go: [] as RowChange[]};
    expect(isAdvanceFrameSkewCrossBatch(here.ts, here.go, neighbor)).toBe(false);
  });

  test('REAL: neighbor has same PK but DIFFERENT change kind → kept', () => {
    // Here: Go ADDs A. Neighbor: TS REMOVEs A. Same key, different op — not a
    // frame-skew split (a split would preserve the op kind). Keep.
    const here = {ts: [] as RowChange[], go: [add('cp', 'A')]};
    const neighbor = {ts: [remove('cp', 'A')], go: [] as RowChange[]};
    expect(isAdvanceFrameSkewCrossBatch(here.ts, here.go, neighbor)).toBe(false);
  });

  test('REAL: only SOME of the non-empty rows match the neighbor → kept', () => {
    // Here: Go has A,B. Neighbor TS has A but not B. B is a real drop (no
    // byte-identical match in the neighbor) → the whole batch must stay a
    // MISMATCH; partial suppression would hide B's drop.
    const here = {ts: [] as RowChange[], go: [add('cp', 'A'), add('cp', 'B')]};
    const neighbor = {ts: [add('cp', 'A')], go: [] as RowChange[]};
    expect(isAdvanceFrameSkewCrossBatch(here.ts, here.go, neighbor)).toBe(false);
  });

  test('REAL: duplicate on the non-empty side → kept (multiplicity divergence)', () => {
    const here = {ts: [] as RowChange[], go: [add('cp', 'A'), add('cp', 'A')]};
    const neighbor = {ts: [add('cp', 'A'), add('cp', 'A')], go: [] as RowChange[]};
    expect(isAdvanceFrameSkewCrossBatch(here.ts, here.go, neighbor)).toBe(false);
  });

  test('REAL: duplicate in the neighbor opposite side → kept', () => {
    const here = {ts: [] as RowChange[], go: [add('cp', 'A')]};
    const neighbor = {ts: [add('cp', 'A'), add('cp', 'A')], go: [] as RowChange[]};
    expect(isAdvanceFrameSkewCrossBatch(here.ts, here.go, neighbor)).toBe(false);
  });

  test('defers to intra-frame when BOTH sides non-empty → false (not one-sided)', () => {
    // A both-sides batch is the intra-frame classifier's job. Cross-batch must
    // NOT fire here even if a neighbor exists — avoids double-suppression and
    // keeps the two classifiers' responsibilities disjoint.
    const here = {ts: [add('cp', 'A')], go: [add('cp', 'B')]};
    const neighbor = {ts: [add('cp', 'B')], go: [add('cp', 'A')]};
    expect(isAdvanceFrameSkewCrossBatch(here.ts, here.go, neighbor)).toBe(false);
  });

  test('stale neighbor (2 batches back) does not match → 1-deep buffer guard', () => {
    // Simulates the 1-deep buffer: the row that WOULD match is no longer in
    // the neighbor (it's 2 batches back, already overwritten). The current
    // neighbor carries an unrelated row → no match → keep as MISMATCH. This is
    // the guard against silencing a real drop that re-emits much later.
    const here = {ts: [] as RowChange[], go: [add('cp', 'A')]};
    // Neighbor's TS side has only 'Z', not 'A' — 'A' was 2 batches back.
    const neighbor = {ts: [add('cp', 'Z')], go: [] as RowChange[]};
    expect(isAdvanceFrameSkewCrossBatch(here.ts, here.go, neighbor)).toBe(false);
  });
});
