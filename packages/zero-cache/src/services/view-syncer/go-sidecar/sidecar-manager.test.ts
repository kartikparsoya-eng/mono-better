import {describe, expect, test} from 'vitest';
import {isProtocolMismatchError} from './sidecar-manager.ts';

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
    expect(isProtocolMismatchError(new Error('method not found: version'))).toBe(
      false,
    );
  });

  test('an unrelated transport error → swallow (false), not mis-escalated', () => {
    // A dropped socket mid-handshake is not a protocol incompatibility; it must
    // fall through to the warn-and-continue path, not the re-throw.
    expect(
      isProtocolMismatchError(new Error('Connection closed before response')),
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
