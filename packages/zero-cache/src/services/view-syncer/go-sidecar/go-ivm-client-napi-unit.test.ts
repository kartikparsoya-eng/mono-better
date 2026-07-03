// Unit tests for the NAPI-transport failure path (A2) and per-group
// fairness routing (A1) in GoIVMClient — scale-review regressions. These use
// a FAKE GoNapiAddon (no real .node / libgoivm), so they run everywhere,
// unlike the artifact-gated napi-transport.test.ts E2E.

import {createServer, type Socket as NetSocket} from 'node:net';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {describe, expect, test} from 'vitest';
import {Packr} from 'msgpackr';
import {DriftError, GoIVMClient} from './go-ivm-client.ts';
import type {GoNapiAddon} from './napi/index.ts';

function fakeAddon(send: (payload: Buffer) => number): GoNapiAddon {
  return {
    start: () => {},
    send,
    abiVersion: () => 2,
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

describe('GoIVMClient host-death delivery (A3)', () => {
  test('kind-4 delivery sweeps pending RPCs and fires onFatal with the reason', async () => {
    // The Go side's death watcher (abi.go) emits ONE kind-4 record when the
    // in-process pump dies unexpectedly. Pre-fix the client warn-logged
    // "unknown NAPI delivery kind 4" and every pending RPC burned its full
    // timeout — this test hung 60s and failed.
    const fatals: Error[] = [];
    const client = new GoIVMClient('napi:test', {
      onFatal: err => fatals.push(err),
    });
    client.connectNapi(fakeAddon(() => 0)); // sends succeed, never respond

    const callA = client.pipelineCount('cg-a', {timeoutMs: 60_000});
    const callAErr = callA.catch((e: Error) => e);
    await new Promise(r => setTimeout(r, 5)); // let the send hit the wire

    // Go host dies: the addon delivers the death record (UTF-8 reason —
    // deliberately NOT a reqID-prefixed record; it must bypass the
    // late-record guard that drops payloads with unknown reqIDs).
    client.handleNapiDelivery(
      4,
      Buffer.from('goivm host pump terminated: io: read/write on closed pipe', 'utf8'),
    );

    // Pending RPC rejects promptly with the host-death reason.
    const err = await callAErr;
    expect(err).toBeInstanceOf(Error);
    expect((err as Error).message).toMatch(/host died.*pump terminated/);

    // Embedder notified exactly once (→ fatalExit / worker restart).
    expect(fatals).toHaveLength(1);
    expect(fatals[0].message).toMatch(/go-ivm in-process host died/);

    // Latched dead: new calls fail fast; no duplicate onFatal.
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

describe('kind-1 dispatch containment (scale review)', () => {
  test('DriftError construction is BigInt-safe', () => {
    // msgpackr decodes Go int64/uint64 PK values >2^53 as BigInt;
    // JSON.stringify throws on BigInt. Pre-fix this constructor threw.
    const err = new DriftError({
      table: 't',
      op: 'Add',
      pk: {id: 9007199254740993n},
      hasCount: 1,
    });
    expect(err.message).toContain('9007199254740993');
  });

  test('a drift frame with a BigInt PK and no message rejects the RPC instead of throwing', async () => {
    const client = new GoIVMClient('napi:test');
    client.connectNapi(fakeAddon(() => 0)); // send OK, never responds

    const call = client.pipelineCount('cg-a', {timeoutMs: 60_000}); // id 1
    const callErr = call.catch((e: Error) => e);
    await new Promise(r => setTimeout(r, 5));

    // Drift error frame: no error.message (so DriftError builds its own via
    // JSON.stringify(pk)) and a BigInt PK (2^53+1 — msgpackr decodes int64
    // beyond Number-safe range as BigInt).
    const frame = new Packr().pack({
      jsonrpc: '2.0',
      id: 1,
      error: {
        code: -32100, // RPC_CODE_DRIFT
        data: {table: 't', op: 'Add', pk: {id: 9007199254740993n}, hasCount: 1},
      },
    });

    // Pre-fix: the DriftError constructor's stringify threw and the throw
    // ESCAPED handleNapiDelivery — an uncaughtException from the TSFN
    // callback in production (worker crash for one bad frame).
    expect(() => client.handleNapiDelivery(1, frame)).not.toThrow();

    const err = await callErr;
    expect(err).toBeInstanceOf(DriftError);
    expect((err as Error).message).toContain('9007199254740993');
  });
});

describe('drain-gate release on socket death (A5)', () => {
  test('callers parked on byte-level backpressure fail fast when the socket dies', async () => {
    const sockPath = join(tmpdir(), `goivm-a5-${process.pid}-${Date.now()}.sock`);
    let serverConn: NetSocket | null = null;
    const server = createServer(conn => {
      serverConn = conn;
      conn.pause(); // never consume — forces socket.write() === false
    });
    await new Promise<void>(r => server.listen(sockPath, () => r()));

    const client = new GoIVMClient(sockPath);
    await client.connect();

    // Call A: one ~8MB frame against a paused reader → write() returns
    // false → the #drainPromise gate arms.
    const big = 'x'.repeat(8 * 1024 * 1024);
    const callA = client
      .loadRows('cg-a', 't', [{id: 1, blob: big}], 1, {timeoutMs: 60_000})
      .catch((e: Error) => e);
    await new Promise(r => setTimeout(r, 50));

    // Call B parks on the drain gate INSIDE #acquireSlot — BEFORE its
    // timeout timer exists, so the pre-fix hang was timerless and permanent
    // (and a dead socket never emits 'drain').
    const callB = client.ping().catch((e: Error) => e);
    await new Promise(r => setTimeout(r, 20));

    // The sidecar dies mid-backpressure.
    serverConn!.destroy();

    // Post-fix: both settle promptly (A rejected by the close sweep, B
    // released from the gate and failing the transport check). Pre-fix:
    // callB never settles — this await trips the vitest test timeout.
    const [a, b] = await Promise.all([callA, callB]);
    expect(a).toBeInstanceOf(Error);
    expect(b).toBeInstanceOf(Error);

    client.close();
    await new Promise<void>(r => server.close(() => r()));
  });
});
