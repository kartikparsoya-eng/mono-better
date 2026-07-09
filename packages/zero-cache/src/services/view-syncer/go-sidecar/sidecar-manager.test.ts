import {describe, expect, test} from 'vitest';
import {
  divideGoConnCeilingsForWorkers,
  isProtocolMismatchError,
  isVersionMethodNotFoundError,
} from './sidecar-manager.ts';

// The version handshake (SidecarManager #start) wraps the version RPC in a
// try/catch that intentionally SWALLOWS a "method not found" error — a sidecar
// predating the version RPC must stay usable — but MUST re-throw a genuine
// protocol-revision mismatch, because accepting an incompatible wire protocol
// silently corrupts every subsequent RPC. Pre-fix the catch swallowed BOTH,
// so a mismatched sidecar was accepted and ran with a wrong protocol rev.
// isProtocolMismatchError is the predicate that splits the two cases.
describe('view-syncer/go-sidecar/sidecar-manager: protocol-mismatch gate', () => {
  test('the exact mismatch error the handshake throws → re-throw (true)', () => {
    // Constructed identically to the throw site (sidecar-manager.ts:495-497).
    const err = new Error(
      'Sidecar protocol revision mismatch: client expects 9, ' +
        'sidecar (v1.2.3) is at 8. Refusing to use this sidecar.',
    );
    expect(isProtocolMismatchError(err)).toBe(true);
  });

  test('"method not found" from an older sidecar → swallow (false)', () => {
    // The ONLY case the catch is allowed to absorb: a pre-version-RPC sidecar.
    expect(
      isProtocolMismatchError(new Error('method not found: version')),
    ).toBe(false);
    expect(
      isVersionMethodNotFoundError(new Error('method not found: version')),
    ).toBe(true);
  });

  test('an unrelated transport error → not a compatibility fallback', () => {
    // A dropped transport during version() means the wire rev is unverified;
    // startup must fail instead of marking the manager running.
    expect(
      isProtocolMismatchError(new Error('Connection closed before response')),
    ).toBe(false);
    expect(
      isVersionMethodNotFoundError(
        new Error('Connection closed before response'),
      ),
    ).toBe(false);
  });

  test('non-Error values → false (the catch requires a real Error to escalate)', () => {
    // Mirrors the `err instanceof Error &&` guard: a bare string or plain
    // object that merely CONTAINS the phrase must not be treated as a mismatch.
    expect(isProtocolMismatchError('protocol revision mismatch')).toBe(false);
    expect(
      isProtocolMismatchError({message: 'protocol revision mismatch'}),
    ).toBe(false);
    expect(isProtocolMismatchError(undefined)).toBe(false);
    expect(isProtocolMismatchError(null)).toBe(false);
  });
});

// M8 (napi review): the container-wide SQLite conn ceilings must be divided
// across napi workers — each worker runs its OWN engine + replica pools, so
// N workers × GO_IVM_MAX_OPEN_CONNS × ~1MB page cache is N× the intended
// C-side budget, invisible to GOMEMLIMIT. Pure-function coverage of the
// #startNapi division (companion to the GOMEMLIMIT_PERCENT division).
describe('divideGoConnCeilingsForWorkers', () => {
  const noopLogger = () => {};

  test('divides both ceilings when set (image defaults, 8 workers)', () => {
    const env: Record<string, string | undefined> = {
      GO_IVM_MAX_OPEN_CONNS: '1024',
      GO_IVM_MAX_IDLE_CONNS: '128',
    };
    divideGoConnCeilingsForWorkers(env, 8, noopLogger);
    expect(env.GO_IVM_MAX_OPEN_CONNS).toBe('128');
    expect(env.GO_IVM_MAX_IDLE_CONNS).toBe('16');
  });

  test('single worker is a no-op (socket topology / W=1)', () => {
    const env: Record<string, string | undefined> = {
      GO_IVM_MAX_OPEN_CONNS: '1024',
    };
    divideGoConnCeilingsForWorkers(env, 1, noopLogger);
    expect(env.GO_IVM_MAX_OPEN_CONNS).toBe('1024');
  });

  test('floors prevent starving a worker at high worker counts', () => {
    const env: Record<string, string | undefined> = {
      GO_IVM_MAX_OPEN_CONNS: '32',
      GO_IVM_MAX_IDLE_CONNS: '4',
    };
    divideGoConnCeilingsForWorkers(env, 24, noopLogger);
    expect(env.GO_IVM_MAX_OPEN_CONNS).toBe('8'); // ceil(32/24)=2 → floor 8
    expect(env.GO_IVM_MAX_IDLE_CONNS).toBe('2'); // ceil(4/24)=1 → floor 2
  });

  test('unset ceilings warn (Go default applies PER worker) and stay unset', () => {
    const warnings: string[] = [];
    const env: Record<string, string | undefined> = {};
    divideGoConnCeilingsForWorkers(env, 4, (level, msg) => {
      if (level === 'warn') warnings.push(msg);
    });
    expect(env.GO_IVM_MAX_OPEN_CONNS).toBeUndefined();
    expect(warnings.some(w => w.includes('GO_IVM_MAX_OPEN_CONNS'))).toBe(true);
  });

  test('invalid values are left as-is with a warning', () => {
    const env: Record<string, string | undefined> = {
      GO_IVM_MAX_OPEN_CONNS: 'lots',
    };
    divideGoConnCeilingsForWorkers(env, 4, noopLogger);
    expect(env.GO_IVM_MAX_OPEN_CONNS).toBe('lots');
  });
});
