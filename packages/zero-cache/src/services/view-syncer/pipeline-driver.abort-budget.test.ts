import {describe, expect, test} from 'vitest';
import {
  escalatedAbortBudgetMs,
  GO_ADVANCE_ABORT_BUDGET_FLOOR_MS,
  goAdvanceAbortBudgetMs,
  goHydrationCostMs,
  shouldSuppressAbort,
  SUPPRESS_ABORT_AFTER_STREAK,
} from './pipeline-driver.ts';

// Gen-5 (2026-07-07 abort-loop forensics) — pins for the two pure inputs
// that keep Go's economic advancement-abort CONVERGENT under sustained
// writes. The observed pathology these prevent: the abort budget was Go's
// engine-internal hydrate time (4–54ms) — ~100× under the true reset cost —
// so it floor-pinned at Go's 50ms minimum while a ~150-change backlog costs
// >50ms CPU. Unlike TS-native (whose budget input self-heals: slow system →
// slow re-hydrate → bigger next budget), the underpriced budget never grew:
// abort → reset → the backlog re-accumulated during the seconds-long reset →
// abort at the SAME position, 11–14× consecutively, tripping breakers and
// cascading CVR-version errors (16/50 clients lost at ~14 writes/s where
// stock TS at identical load had zero aborts, zero losses).
describe('goHydrationCostMs (budget unit-parity with TS-native)', () => {
  test('records the TS-observed wall when it exceeds the engine time (the fix)', () => {
    // The regression shape: Go hydrates in ~15ms engine time, but the
    // TS-observed hydrate wall — the honest floor on what a reset re-pays —
    // is ~1.2s. Storing 15 priced resets at ~100× below cost.
    expect(goHydrationCostMs(15, 1200)).toBe(1200);
  });

  test('keeps the engine time as the floor when wall is (degenerately) smaller', () => {
    // Wall ≥ engine in practice (the engine runs inside the awaited call);
    // max() guards clock-skew / instant-await degenerates.
    expect(goHydrationCostMs(1200, 500)).toBe(1200);
  });

  test('undefined engine timing (non-terminal / older Go) → wall alone', () => {
    expect(goHydrationCostMs(undefined, 640)).toBe(640);
  });

  test('both zero → zero (Go floors the budget at its 50ms minimum, not here)', () => {
    expect(goHydrationCostMs(undefined, 0)).toBe(0);
    expect(goHydrationCostMs(0, 0)).toBe(0);
  });
});

describe('escalatedAbortBudgetMs (consecutive-abort convergence backstop)', () => {
  test('no abort streak → base budget unchanged (the common path)', () => {
    expect(escalatedAbortBudgetMs(1000, 0)).toBe(1000);
  });

  test('doubles per consecutive abort: 2×, 4×, 8×', () => {
    expect(escalatedAbortBudgetMs(1000, 1)).toBe(2000);
    expect(escalatedAbortBudgetMs(1000, 2)).toBe(4000);
    expect(escalatedAbortBudgetMs(1000, 3)).toBe(8000);
  });

  test('caps at 8× — the abort must stay meaningful as a circuit breaker', () => {
    // Beyond the cap the GO_IVM_ADVANCE_BUDGET_MS wall backstop (60s)
    // remains the runaway/WAL-pin guard; an unbounded multiplier would
    // effectively delete the economic abort after a few streaks.
    expect(escalatedAbortBudgetMs(1000, 4)).toBe(8000);
    expect(escalatedAbortBudgetMs(1000, 50)).toBe(8000);
  });

  test('negative streak (impossible by construction) clamps to 1×', () => {
    expect(escalatedAbortBudgetMs(1000, -3)).toBe(1000);
  });

  test('zero base stays zero — the FLOOR (goAdvanceAbortBudgetMs), not this fn, prices the reset minimum', () => {
    expect(escalatedAbortBudgetMs(0, 3)).toBe(0);
  });

  test('division of labor: escalation cannot rescue a 100×-underpriced base — the floor does', () => {
    // The field pathology in numbers: true per-advance work ~60ms CPU,
    // budget stored at ~4ms (engine time). Go aborts when
    // elapsed > max(50ms gate, budget) — even the capped 8× escalation
    // (32ms) stays under the 50ms gate and the advance still aborts:
    const storedBudget = 4;
    const trueWorkMs = 60;
    expect(
      Math.max(escalatedAbortBudgetMs(storedBudget, 3), 50), // abort threshold
    ).toBeLessThan(trueWorkMs);
    // …which is why the SENT budget goes through goAdvanceAbortBudgetMs:
    // the reset-cost floor clears the gap regardless of streak state.
    expect(goAdvanceAbortBudgetMs(storedBudget, 0)).toBeGreaterThan(trueWorkMs);
  });
});

describe('goAdvanceAbortBudgetMs (the budget actually sent to Go)', () => {
  test('structurally-zero budgets are floored at the reset cost (the 50/77 zero-budget abort class)', () => {
    // Internal-only CGs (noopTimer entries), TTL-expired reconnects (empty
    // map), and mid-registration stubs all price the base at ~0 — but a Go
    // reset is never remotely free (destroy + per-table re-init + full
    // re-hydrate + re-transform round-trips), and the source-maintenance
    // CPU an abort "saves" is re-paid by the reset's own leapfrog anyway.
    expect(goAdvanceAbortBudgetMs(0, 0)).toBe(GO_ADVANCE_ABORT_BUDGET_FLOOR_MS);
    expect(goAdvanceAbortBudgetMs(0, 3)).toBe(GO_ADVANCE_ABORT_BUDGET_FLOOR_MS);
  });

  test('sub-floor bases are floored even after full escalation', () => {
    // 8ms × 8 = 64ms < floor → floor wins.
    expect(goAdvanceAbortBudgetMs(8, 3)).toBe(GO_ADVANCE_ABORT_BUDGET_FLOOR_MS);
  });

  test('above-floor bases pass through untouched and still escalate', () => {
    // Honest wall-priced budgets dominate the floor — the floor must never
    // shrink a real budget, and escalation still applies on streaks.
    expect(goAdvanceAbortBudgetMs(400, 0)).toBe(400);
    expect(goAdvanceAbortBudgetMs(400, 1)).toBe(800);
  });

  test('the floor sits above Go-side per-burst source-maintenance cost, below genuine-abort scale', () => {
    // ~150-change bursts cost ~50-110ms CPU of unavoidable prev-tx replay —
    // must NOT abort (a reset re-pays that work). Genuinely huge
    // transactions cost seconds — must still abort.
    expect(GO_ADVANCE_ABORT_BUDGET_FLOOR_MS).toBeGreaterThan(110);
    expect(GO_ADVANCE_ABORT_BUDGET_FLOOR_MS).toBeLessThan(1000);
  });
});

describe('shouldSuppressAbort (terminal escalation for abort streaks)', () => {
  test('no suppression while the budget-escalation lever still has headroom', () => {
    expect(shouldSuppressAbort(0)).toBe(false);
    expect(shouldSuppressAbort(1)).toBe(false);
    expect(shouldSuppressAbort(2)).toBe(false);
  });

  test('suppresses once the maxed budget has itself aborted', () => {
    expect(shouldSuppressAbort(SUPPRESS_ABORT_AFTER_STREAK)).toBe(true);
    expect(shouldSuppressAbort(SUPPRESS_ABORT_AFTER_STREAK + 5)).toBe(true);
  });

  test('the threshold aligns with the escalation cap — suppression fires only when doubling is exhausted', () => {
    // By the time suppression fires, further streak growth cannot raise the
    // budget any more (the exp cap in escalatedAbortBudgetMs is saturated) —
    // suppression is a terminal lever, never a substitute for escalation
    // that still had headroom.
    const base = 1000;
    expect(escalatedAbortBudgetMs(base, SUPPRESS_ABORT_AFTER_STREAK)).toBe(
      escalatedAbortBudgetMs(base, SUPPRESS_ABORT_AFTER_STREAK + 1),
    );
  });
});
