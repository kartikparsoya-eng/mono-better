import {describe, expect, test} from 'vitest';
import {StaleInitEpochError} from './go-sidecar/go-ivm-client.ts';
import {
  classifyGoPrimaryAdvanceError,
  decideGoPrimaryDispatch,
  drainDiffCatchingReset,
  type GoAdvanceErrorClass,
  type GoPrimaryDispatchDecision,
} from './pipeline-driver.ts';
import {ResetPipelinesSignal} from './snapshotter.ts';

// Findings 1–3 (the CRITICAL/HIGH Go-primary advance-path bugs) all converge on
// one mechanism: a recovery condition must make advance() RETURN a
// ResetPipelinesSignal (→ view-syncer heals the CVR via reset + re-hydrate) and
// must NEVER let it escape as a throw past advance() (→ run()'s outer catch →
// full client-group teardown). The advance path itself needs a live Pipeline
// driver + sidecar, so these pin the three pure decision functions the path
// delegates to. Pre-fix there was ZERO coverage of any drop path — consistent
// with how F1–F3 survived the soaks (soaks don't kill the sidecar, truncate
// tables, or time out RPCs).
describe('view-syncer/pipeline-driver: Go-primary drop-path decisions', () => {
  // F1 — TS-fallback freeze + watermark over-claim. The dispatch must reconcile
  // the LIVE Go availability against the mode the current user pipelines were
  // built in, so a Go-availability flip rebuilds the pipelines (returned signal)
  // instead of silently freezing (Go-owned stubs while Go is down) or
  // double-emitting (real TS + Go after recovery).
  describe('F1: decideGoPrimaryDispatch', () => {
    test('Go UP + Go-owned stubs → go-advance (the steady-state primary path)', () => {
      expect(decideGoPrimaryDispatch(true, 'go')).toBe('go-advance');
    });

    test('Go UP + no pipelines yet (undefined) → go-advance', () => {
      // First advance after init before any user query registered: nothing to
      // rebuild, Go owns user queries.
      expect(decideGoPrimaryDispatch(true, undefined)).toBe('go-advance');
    });

    test('Go UP + degraded-to-TS pipelines → reset-recovered (avoid double-emit)', () => {
      // Go recovered after an outage that degraded the pipelines to real TS.
      // Running the Go-primary advance now would emit user rows from BOTH the
      // real TS pipelines and Go. Rebuild as stubs first.
      expect(decideGoPrimaryDispatch(true, 'ts')).toBe('reset-recovered');
    });

    test('Go DOWN + Go-owned stubs → reset-degrade (avoid silent freeze)', () => {
      // The keystone F1 case: a TS-native advance over STUB user pipelines emits
      // nothing for them yet commits the CVR at full version — a silent freeze
      // with the cookie advancing past the gap. Reset so re-registration builds
      // REAL TS pipelines (graceful degradation to TS-serving).
      expect(decideGoPrimaryDispatch(false, 'go')).toBe('reset-degrade');
    });

    test('Go DOWN + already-degraded TS pipelines → ts-native (no reset loop)', () => {
      // Already serving real TS; resetting again would loop every advance for
      // the whole outage / drift-breaker cooldown.
      expect(decideGoPrimaryDispatch(false, 'ts')).toBe('ts-native');
    });

    test('Go DOWN + no pipelines yet (undefined) → ts-native', () => {
      expect(decideGoPrimaryDispatch(false, undefined)).toBe('ts-native');
    });

    test('full matrix is exhaustive and never silently advances over stubs while Go is down', () => {
      const matrix: Array<
        [boolean, 'go' | 'ts' | undefined, GoPrimaryDispatchDecision]
      > = [
        [true, 'go', 'go-advance'],
        [true, undefined, 'go-advance'],
        [true, 'ts', 'reset-recovered'],
        [false, 'go', 'reset-degrade'],
        [false, 'ts', 'ts-native'],
        [false, undefined, 'ts-native'],
      ];
      for (const [up, mode, expected] of matrix) {
        expect(decideGoPrimaryDispatch(up, mode)).toBe(expected);
      }
      // The dangerous combination — Go DOWN with Go-owned stubs falling through
      // to a TS-native advance — must be the ONLY one that never resolves to a
      // plain advance. Pin that it is a reset, not 'go-advance'/'ts-native'.
      expect(decideGoPrimaryDispatch(false, 'go')).not.toBe('ts-native');
      expect(decideGoPrimaryDispatch(false, 'go')).not.toBe('go-advance');
    });
  });

  // F2 — dropped user deltas leave permanent gaps. The classifier buckets an
  // advance RPC failure; protocol/stale-epoch ESCALATE (re-throw → teardown +
  // reconnect), sidecar/unclassified DROP → reset (the caller turns these into a
  // ResetPipelinesSignal so the view-syncer re-hydrates the gap as an idempotent
  // superset rather than committing the watermark past a never-delivered delta).
  describe('F2: classifyGoPrimaryAdvanceError', () => {
    const RETHROW: ReadonlySet<GoAdvanceErrorClass> = new Set([
      'protocol',
      'stale-epoch',
    ]);

    test('protocol violations → protocol (escalate, a reset cannot fix a wire bug)', () => {
      for (const msg of [
        'chunk order violation: got 3 expected 2',
        'stream finished without a final chunk',
        'Frame too large: 80000000 bytes',
        'protocolRev mismatch: client 4 server 3',
      ]) {
        expect(classifyGoPrimaryAdvanceError(new Error(msg))).toBe('protocol');
      }
    });

    test('StaleInitEpochError instance → stale-epoch (this instance was superseded)', () => {
      const e = new StaleInitEpochError('initEpoch 7 < current 8');
      expect(classifyGoPrimaryAdvanceError(e)).toBe('stale-epoch');
    });

    test('sidecar-unavailable messages → sidecar (drop → reset)', () => {
      for (const msg of [
        'Sidecar is not running',
        'Connection closed before response',
        'engine not initialized',
      ]) {
        expect(classifyGoPrimaryAdvanceError(new Error(msg))).toBe('sidecar');
      }
    });

    test('RPC timeout under load → unclassified (drop → reset, NOT silently swallowed)', () => {
      // The exact class that the soaks never produced and that — pre-F2 —
      // returned [] and floored the watermark at prev, permanently skipping the
      // (prev→head] delta. Now it escalates to a reset.
      expect(
        classifyGoPrimaryAdvanceError(new Error('RPC timed out after 30000ms')),
      ).toBe('unclassified');
    });

    test('non-Error values are classified by their string form', () => {
      expect(classifyGoPrimaryAdvanceError('engine not initialized')).toBe(
        'sidecar',
      );
      expect(classifyGoPrimaryAdvanceError({weird: true})).toBe('unclassified');
    });

    test('protocol patterns win over the stale-epoch instance check (order pinned)', () => {
      // A StaleInitEpochError whose message also matches a protocol pattern is
      // classified protocol (checked first). Both re-throw, so behaviour is
      // identical — this pins the documented precedence so a future refactor
      // does not accidentally reorder it into a DROP bucket.
      const e = new StaleInitEpochError('chunk order violation during teardown');
      expect(classifyGoPrimaryAdvanceError(e)).toBe('protocol');
    });

    test('escalate-vs-drop contract: only protocol/stale-epoch re-throw', () => {
      const classes: GoAdvanceErrorClass[] = [
        'protocol',
        'stale-epoch',
        'sidecar',
        'unclassified',
      ];
      // sidecar + unclassified are the DROP buckets that must escalate to a
      // ResetPipelinesSignal rather than re-throw — the heart of the F2 fix.
      expect(classes.filter(c => !RETHROW.has(c))).toEqual([
        'sidecar',
        'unclassified',
      ]);
    });
  });

  // F3 — the truncate throw escapes the handled path. The Go-primary advance
  // paths buffer the snapshotter diff EAGERLY; the diff iterator throws a
  // ResetPipelinesSignal on truncate / schema change. drainDiffCatchingReset
  // must RETURN that signal (graceful reset) instead of letting it propagate
  // into the outer-catch teardown, while letting any other error through.
  describe('F3: drainDiffCatchingReset', () => {
    test('clean diff → undefined, onEntry invoked for every entry in order', () => {
      const seen: number[] = [];
      const signal = drainDiffCatchingReset([1, 2, 3], e => seen.push(e));
      expect(signal).toBeUndefined();
      expect(seen).toEqual([1, 2, 3]);
    });

    test('iterator throws ResetPipelinesSignal partway → RETURNS that exact signal', () => {
      const seen: number[] = [];
      const thrown = new ResetPipelinesSignal('table truncated', 'truncation');
      function* truncatingDiff(): Generator<number> {
        yield 1;
        yield 2;
        throw thrown;
      }
      const signal = drainDiffCatchingReset(truncatingDiff(), e => seen.push(e));
      // Same instance (reason preserved) — the caller returns it unchanged so
      // the view-syncer's reason metric/log is accurate.
      expect(signal).toBe(thrown);
      expect(signal?.reason).toBe('truncation');
      // Entries before the throw were still buffered (the throw is mid-stream).
      expect(seen).toEqual([1, 2]);
    });

    test('iterator throws a non-reset error → propagates (must NOT be swallowed)', () => {
      function* boom(): Generator<number> {
        yield 1;
        throw new Error('genuine bug');
      }
      // A real bug must surface, not be silently turned into a reset.
      expect(() => drainDiffCatchingReset(boom(), () => {})).toThrow(
        'genuine bug',
      );
    });

    test('onEntry throwing a ResetPipelinesSignal is also caught and returned', () => {
      // Defensive: the signal can originate from onEntry-side processing too.
      const thrown = new ResetPipelinesSignal('schema changed', 'schema-change');
      const signal = drainDiffCatchingReset([1, 2, 3], e => {
        if (e === 2) {
          throw thrown;
        }
      });
      expect(signal).toBe(thrown);
    });

    test('empty diff → undefined, onEntry never called', () => {
      let calls = 0;
      const signal = drainDiffCatchingReset([], () => calls++);
      expect(signal).toBeUndefined();
      expect(calls).toBe(0);
    });
  });
});
