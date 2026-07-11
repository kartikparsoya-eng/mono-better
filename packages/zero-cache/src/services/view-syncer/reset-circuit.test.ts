import {describe, expect, test} from 'vitest';
import type {ResetPipelinesReason} from './snapshotter.ts';
import {
  classifyResetReason,
  emptyResetCircuitBuckets,
  RESET_CIRCUIT_LIMIT,
  RESET_CIRCUIT_WINDOW_MS,
  resetCircuitDecision,
  resetCircuitDecisionByReason,
  TRANSIENT_RESET_CIRCUIT_LIMIT,
  TRANSIENT_RESET_CIRCUIT_WINDOW_MS,
  type ResetCircuitBuckets,
} from './view-syncer.ts';

describe('reset circuit breaker', () => {
  test('defaults are 2 resets / 20s', () => {
    expect(RESET_CIRCUIT_LIMIT).toBe(2);
    expect(RESET_CIRCUIT_WINDOW_MS).toBe(20_000);
  });

  test('first two resets in the window do NOT trip', () => {
    const now = 1_000_000;
    // No prior resets.
    expect(resetCircuitDecision([], now).trip).toBe(false);
    // One prior reset, still under the limit.
    expect(resetCircuitDecision([now - 5_000], now).trip).toBe(false);
  });

  test('the third reset within the window trips (teardown)', () => {
    const now = 1_000_000;
    const twoRecent = [now - 10_000, now - 3_000];
    const {trip, pruned} = resetCircuitDecision(twoRecent, now);
    expect(trip).toBe(true);
    expect(pruned).toEqual(twoRecent); // both still in-window
  });

  test('resets older than the window are pruned and do not count', () => {
    const now = 1_000_000;
    // Two old (just outside 20s) + nothing recent → should NOT trip.
    const old = [now - 25_000, now - 21_000];
    const {trip, pruned} = resetCircuitDecision(old, now);
    expect(trip).toBe(false);
    expect(pruned).toEqual([]); // both pruned
  });

  test('mixed old + recent: only in-window resets count', () => {
    const now = 1_000_000;
    // One expired, two in-window → trips on the in-window pair.
    const {trip, pruned} = resetCircuitDecision(
      [now - 30_000, now - 15_000, now - 1_000],
      now,
    );
    expect(trip).toBe(true);
    expect(pruned).toEqual([now - 15_000, now - 1_000]);
  });

  test('the window boundary is exclusive (exactly windowMs old is pruned)', () => {
    const now = 1_000_000;
    const {pruned} = resetCircuitDecision(
      [now - RESET_CIRCUIT_WINDOW_MS, now - (RESET_CIRCUIT_WINDOW_MS - 1)],
      now,
    );
    // The exactly-windowMs-old entry is pruned; the 1ms-younger one stays.
    expect(pruned).toEqual([now - (RESET_CIRCUIT_WINDOW_MS - 1)]);
  });

  test('custom limit/window are honored', () => {
    const now = 1_000_000;
    // limit 1: a single prior reset trips.
    expect(resetCircuitDecision([now - 100], now, 1, 1_000).trip).toBe(true);
    // window 1s: a 2s-old reset is pruned → no trip even at limit 1.
    expect(resetCircuitDecision([now - 2_000], now, 1, 1_000).trip).toBe(false);
  });
});

describe('classifyResetReason (which repeats imply a reset-proof loop)', () => {
  test('full taxonomy is covered', () => {
    const expected: Record<ResetPipelinesReason, string> = {
      'watermark-regression': 'deterministic',
      'go-primary-unavailable': 'transient',
      'go-primary-drop': 'transient',
      'advancement-timeout': 'economic',
      'schema-change': 'lawful',
      'truncation': 'lawful',
      'permissions-change': 'lawful',
      'scalar-subquery': 'lawful',
    };
    for (const [reason, cls] of Object.entries(expected)) {
      expect(classifyResetReason(reason as ResetPipelinesReason)).toBe(cls);
    }
  });
});

describe('resetCircuitDecisionByReason (tiered breaker)', () => {
  const now = 1_000_000;

  /** Feed n resets of `reason` at 1s spacing ending at `now`; return the
   * final decision. */
  function feed(reason: ResetPipelinesReason, n: number) {
    let buckets: ResetCircuitBuckets = emptyResetCircuitBuckets();
    let last;
    for (let i = 0; i < n; i++) {
      last = resetCircuitDecisionByReason(
        reason,
        buckets,
        now - (n - 1 - i) * 1_000,
      );
      buckets = last.buckets;
    }
    return last!;
  }

  test('economic (advancement-timeout) NEVER trips — the G13 teardown-storm regression', () => {
    // The oracle-suite pathology: sustained write load lawfully resets every
    // large batch; the reason-blind 2/20s breaker read that as a reset-proof
    // loop and tore down 10 CGs. Convergence for this class is owned by the
    // budget escalation + suppressAbort catch-up, not by teardown.
    const {cls, trip, buckets} = feed('advancement-timeout', 50);
    expect(cls).toBe('economic');
    expect(trip).toBe(false);
    // Never recorded either: economic resets must not poison other buckets.
    expect(buckets).toEqual(emptyResetCircuitBuckets());
  });

  test('lawful structural resets NEVER trip (migration touching 3 tables = 3 schema-change resets)', () => {
    for (const reason of [
      'schema-change',
      'truncation',
      'permissions-change',
      'scalar-subquery',
    ] as const) {
      const {trip, buckets} = feed(reason, 10);
      expect(trip, reason).toBe(false);
      expect(buckets, reason).toEqual(emptyResetCircuitBuckets());
    }
  });

  test('deterministic keeps the fast trip: 3rd watermark-regression in 20s tears down', () => {
    expect(feed('watermark-regression', RESET_CIRCUIT_LIMIT).trip).toBe(false);
    expect(feed('watermark-regression', RESET_CIRCUIT_LIMIT + 1).trip).toBe(
      true,
    );
  });

  test('transient tolerates a sidecar-restart burst that would trip the deterministic limit', () => {
    // 3 resets in 3 seconds — a normal restart cycle (drop, degrade,
    // recover). Deterministic would tear down; transient must not.
    expect(feed('go-primary-drop', 3).trip).toBe(false);
  });

  test('transient still catches genuine flapping within its window', () => {
    expect(
      feed('go-primary-unavailable', TRANSIENT_RESET_CIRCUIT_LIMIT).trip,
    ).toBe(false);
    expect(
      feed('go-primary-unavailable', TRANSIENT_RESET_CIRCUIT_LIMIT + 1).trip,
    ).toBe(true);
  });

  test('classes do not cross-contaminate', () => {
    // 5 transient resets then 2 deterministic: the deterministic bucket
    // counts only its own class.
    let buckets: ResetCircuitBuckets = emptyResetCircuitBuckets();
    for (let i = 0; i < 5; i++) {
      buckets = resetCircuitDecisionByReason(
        'go-primary-drop',
        buckets,
        now + i,
      ).buckets;
    }
    // First two deterministic resets pass; the third trips.
    let d = resetCircuitDecisionByReason(
      'watermark-regression',
      buckets,
      now + 10,
    );
    expect(d.trip).toBe(false);
    d = resetCircuitDecisionByReason(
      'watermark-regression',
      d.buckets,
      now + 11,
    );
    expect(d.trip).toBe(false);
    d = resetCircuitDecisionByReason(
      'watermark-regression',
      d.buckets,
      now + 12,
    );
    expect(d.trip).toBe(true);
  });

  test('per-class windows prune independently', () => {
    // Two deterministic resets older than the 20s window + one recent → no
    // trip (pruned); the same shape inside the window → trip.
    const stale: ResetCircuitBuckets = {
      deterministic: [
        now - RESET_CIRCUIT_WINDOW_MS - 2,
        now - RESET_CIRCUIT_WINDOW_MS - 1,
      ],
      transient: [now - TRANSIENT_RESET_CIRCUIT_WINDOW_MS - 1],
    };
    const d = resetCircuitDecisionByReason('watermark-regression', stale, now);
    expect(d.trip).toBe(false);
    expect(d.buckets.deterministic).toEqual([now]); // stale pruned, now recorded
    expect(d.buckets.transient).toEqual([]); // stale pruned
  });

  test('a trip does not record the tripping reset (the CG is being torn down)', () => {
    const full: ResetCircuitBuckets = {
      deterministic: [now - 2_000, now - 1_000],
      transient: [],
    };
    const d = resetCircuitDecisionByReason('watermark-regression', full, now);
    expect(d.trip).toBe(true);
    expect(d.buckets.deterministic).toEqual([now - 2_000, now - 1_000]);
  });
});
