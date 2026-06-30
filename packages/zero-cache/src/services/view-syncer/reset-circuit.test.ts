import {describe, expect, test} from 'vitest';
import {
  RESET_CIRCUIT_LIMIT,
  RESET_CIRCUIT_WINDOW_MS,
  resetCircuitDecision,
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
