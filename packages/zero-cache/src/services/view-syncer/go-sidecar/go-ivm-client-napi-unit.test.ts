// Unit tests for the NAPI-transport failure path (A2) and per-group
// fairness routing (A1) in GoIVMClient — scale-review regressions. These use
// a FAKE GoNapiAddon (no real .node / libgoivm), so they run everywhere,
// unlike the artifact-gated napi-transport.test.ts E2E.

import {describe, expect, test} from 'vitest';
import {GoIVMClient} from './go-ivm-client.ts';
import type {GoNapiAddon} from './napi/index.ts';

function fakeAddon(send: (payload: Buffer) => number): GoNapiAddon {
  return {
    start: () => {},
    send,
    shutdown: () => {},
    abiVersion: () => 1,
  };
}

describe('GoIVMClient napi fatal path (A2)', () => {
  test('send rc!=0 rejects the call, sweeps pending, and fires onFatal once', async () => {
    // First send succeeds (RPC parks pending forever — the fake never
    // responds); second send reports the host dead.
    let sends = 0;
    const fatals: Error[] = [];
    const client = new GoIVMClient('napi:test', {
      onFatal: err => fatals.push(err),
    });
    client.connectNapi(fakeAddon(() => (++sends === 1 ? 0 : 1)));

    // Call A: sent OK, pending. Pre-fix it stayed pending after the host
    // died — burning its full timeout while the manager stayed 'running'.
    const callA = client.pipelineCount('cg-a', {timeoutMs: 60_000});
    const callAErr = callA.catch((e: Error) => e);

    // Call B: send returns rc=1 → the transport is dead.
    await expect(client.pipelineCount('cg-b', {timeoutMs: 60_000})).rejects.toThrow(
      /goivm_send failed: rc=1/,
    );

    // A2: the whole transport latches dead — call A rejects PROMPTLY (the
    // sweep), not via its 60s timer (pre-fix this await hung → test timeout).
    expect(await callAErr).toBeInstanceOf(Error);
    // ...and the embedder was notified exactly once.
    expect(fatals).toHaveLength(1);
    expect(fatals[0].message).toMatch(/goivm_send failed/);

    // Latched: subsequent calls fail fast without re-firing onFatal.
    await expect(client.ping()).rejects.toThrow(/Not connected/);
    expect(fatals).toHaveLength(1);
  });
});

describe('GoIVMClient per-group fairness (A1)', () => {
  test('one saturated group does not head-block other groups', async () => {
    // Fake addon: every send succeeds but never gets a response, so RPCs
    // stay in-flight and hold their fairness slots.
    const wire: Buffer[] = [];
    const client = new GoIVMClient('napi:test');
    client.connectNapi(
      fakeAddon(payload => {
        wire.push(payload);
        return 0;
      }),
    );

    // Saturate cg-hog's per-group cap (MAX_IN_FLIGHT_PER_GROUP = 16) with
    // CG-scoped wrapper calls. Swallow the eventual timeout rejections.
    const hogs: Promise<unknown>[] = [];
    for (let i = 0; i < 16; i++) {
      hogs.push(client.pipelineCount('cg-hog', {timeoutMs: 2_000}).catch(() => {}));
    }
    // All 16 must have hit the wire (they only contend within their group).
    await new Promise(r => setTimeout(r, 10));
    expect(wire.length).toBe(16);

    // A different group must still get through. Pre-fix, ALL wrappers
    // bucketed under GLOBAL_KEY, so cg-hog's 16 in-flight RPCs consumed the
    // ONE shared 16-slot bucket and this call parked in #acquireSlot —
    // never reaching the wire (worker-wide head-blocking).
    const other = client.pipelineCount('cg-other', {timeoutMs: 2_000}).catch(() => {});
    await new Promise(r => setTimeout(r, 10));
    expect(wire.length).toBe(17);

    // And a 17th call from the HOG group still parks (the per-group cap is
    // enforced where it should be): wire count stays 17.
    const hog17 = client.pipelineCount('cg-hog', {timeoutMs: 1_000}).catch(() => {});
    await new Promise(r => setTimeout(r, 10));
    expect(wire.length).toBe(17);

    await Promise.allSettled([...hogs, other, hog17]);
  });
});
