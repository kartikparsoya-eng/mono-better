import {describe, expect, test} from 'vitest';
import {versionToLexi} from '../../types/lexi-version.ts';
import {reconcileGoPrimaryWatermark} from './pipeline-driver.ts';

// P2c (DESIGN-snapshotter-port.md §10): the CVR stateVersion is a completeness
// floor, so in Go-primary trigger mode (user data at V_go, internal at V_ts) it
// must be stamped at min(V_ts, V_go). These tests pin that rule and its
// monotonicity, using real LexiVersions so we exercise the same lexicographic
// ordering the comparator (and SQLite/CVR) rely on.
describe('view-syncer/pipeline-driver: P2c watermark reconciliation', () => {
  const v = (n: number) => versionToLexi(n);

  test('push mode (goVersion undefined) → watermark is V_ts, goVersion stays undefined', () => {
    const r = reconcileGoPrimaryWatermark(v(7), undefined);
    expect(r.version).toBe(v(7));
    expect(r.tsVersion).toBe(v(7));
    expect(r.goVersion).toBeUndefined();
  });

  test('Go ahead (V_go > V_ts) → floor lands on V_ts (the common path)', () => {
    // TS reads head before Go each cycle, so a commit landing in between gives
    // V_go > V_ts. The floor holds the CVR at V_ts; Go's extra user rows up to
    // V_go are an idempotent superset re-delivered next cycle.
    const r = reconcileGoPrimaryWatermark(v(10), v(13));
    expect(r.version).toBe(v(10));
    expect(r.tsVersion).toBe(v(10));
    expect(r.goVersion).toBe(v(13));
  });

  test('Go behind (V_go < V_ts) → floor lands on V_go (guards the inverted edge)', () => {
    // e.g. Go re-init left its Snapshotter pinned at an older head. Stamping at
    // V_ts would over-claim user completeness; min correctly holds at V_go.
    const r = reconcileGoPrimaryWatermark(v(20), v(16));
    expect(r.version).toBe(v(16));
    expect(r.tsVersion).toBe(v(20));
    expect(r.goVersion).toBe(v(16));
  });

  test('equal versions → watermark equals both', () => {
    const r = reconcileGoPrimaryWatermark(v(5), v(5));
    expect(r.version).toBe(v(5));
    expect(r.tsVersion).toBe(v(5));
    expect(r.goVersion).toBe(v(5));
  });

  test('reconciled watermark never exceeds either authority (the floor invariant)', () => {
    for (const [ts, go] of [
      [1, 1],
      [1, 2],
      [2, 1],
      [100, 99],
      [99, 100],
      [12345, 67890],
    ] as const) {
      const r = reconcileGoPrimaryWatermark(v(ts), v(go));
      // version <= min(both) — the assertion view-syncer enforces before stamp.
      expect(r.version <= v(ts)).toBe(true);
      expect(r.version <= v(go)).toBe(true);
      // and it IS the larger floor both crossed, not something lower.
      expect(r.version).toBe(v(Math.min(ts, go)));
    }
  });

  test('monotonicity: as both authorities advance, the watermark is non-decreasing', () => {
    // min is monotone in each argument; each authority only moves forward, so
    // the committed CVR watermark must never go backward across cycles.
    const cycles: Array<[number, number]> = [
      [1, 1],
      [1, 3], // TS frozen this tick, Go ahead → floor stays at 1
      [3, 3], // TS catches up
      [3, 5],
      [6, 5], // Go briefly behind → floor at 5 (>= prior 3)
      [7, 9],
    ];
    let prev = '';
    for (const [ts, go] of cycles) {
      const {version} = reconcileGoPrimaryWatermark(v(ts), v(go));
      expect(version >= prev).toBe(true);
      prev = version;
    }
  });
});
