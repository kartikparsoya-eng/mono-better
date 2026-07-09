// Unit tests for the advance-path follow-TS failure contract against a FAKE
// GoNapiAddon (no real .node / libgoivm needed):
//
//   1. computeBoundTimeoutMs — compute-bound RPCs run WITHOUT a wall-clock
//      timeout on the in-process transport (the fixed timeouts were the
//      reset-storm fuel; TS's own compute has no deadline either).
//   2. advanceToHeadStream ships the economic-abort budget
//      (totalHydrationTimeMs / suppressAbort) as additive request params.
//   3. RPC_CODE_ADVANCE_ABORTED / RPC_CODE_ADVANCE_CLEAN_RETRYABLE /
//      RPC_CODE_SCALAR_RESET map to typed errors (AdvanceAbortedError →
//      'advancement-timeout' reset; RetryableAdvanceError → in-place retry
//      in GoComputeBackend; ScalarResetError → 'scalar-subquery' reset).

import {afterEach, describe, expect, test, vi} from 'vitest';
import {Packr} from 'msgpackr';
import {
  AdvanceAbortedError,
  computeBoundTimeoutMs,
  GoIVMClient,
  RetryableAdvanceError,
  type RowChange,
  ScalarResetError,
} from './go-ivm-client.ts';
import type {GoNapiAddon} from './napi/index.ts';

const packr = new Packr({
  useRecords: false,
  encodeUndefinedAsNil: true,
  mapsAsObjects: true,
  useBigIntExtension: false,
});

type CapturedReq = {
  id: number;
  method: string;
  params: Record<string, unknown>;
};

const posFrame = (ids: string[], queryID = 'q1') => ({
  d: [{q: queryID, t: 't', c: ['id'], k: ['id']}],
  r: ids.map(id => [0, 0, id] as unknown[]),
});

// Minimal fake host: captures every request; the TEST decides when/what to
// deliver back (no autonomous pump — advance frames are test-driven).
function makeAdvanceFakeHost(client: GoIVMClient) {
  const reqs: CapturedReq[] = [];
  const addon: GoNapiAddon = {
    start: () => {},
    abiVersion: () => 3,
    send: (payload: Buffer) => {
      reqs.push(packr.unpack(payload) as CapturedReq);
      return 0;
    },
    streamCredit: () => {},
    streamCancel: () => {},
  };
  const deliver = (id: number, result: unknown, error?: {code: number; message: string}) => {
    const frame = error
      ? {jsonrpc: '2.0', error, id}
      : {jsonrpc: '2.0', result, id};
    client.handleNapiDelivery(1, packr.pack(frame) as Buffer);
  };
  const deliverSuccess = (id: number) => {
    deliver(id, {chunkIndex: 0, final: true, version: '0000000002', numChanges: 0});
    deliver(id, 'done');
  };
  return {reqs, addon, deliver, deliverSuccess};
}

afterEach(() => {
  vi.useRealTimers();
});

// #call acquires its in-flight slot through (resolved) promises before the
// payload reaches addon.send — drain a few microtask turns so captured
// requests are visible. Pure microtasks: unaffected by vi.useFakeTimers.
async function flush(): Promise<void> {
  for (let i = 0; i < 8; i++) {
    await Promise.resolve();
  }
}

describe('computeBoundTimeoutMs', () => {
  test('in-process → 0 (no timeout)', () => {
    expect(computeBoundTimeoutMs()).toBe(0);
  });

  test('explicit override wins — including an explicit 0', () => {
    expect(computeBoundTimeoutMs(5_000)).toBe(5_000);
    expect(computeBoundTimeoutMs(0)).toBe(0);
  });
});

describe('GoIVMClient.advanceToHeadStream (follow-TS failure contract)', () => {
  test('abortBudget rides the request as additive params', async () => {
    const client = new GoIVMClient();
    const host = makeAdvanceFakeHost(client);
    client.connectNapi(host.addon);

    const p = client.advanceToHeadStream('cg', 1, 'app', {
      abortBudget: {totalHydrationTimeMs: 123.5},
    });
    await flush();
    expect(host.reqs).toHaveLength(1);
    const req = host.reqs[0];
    expect(req.method).toBe('advanceToHeadStream');
    expect(req.params.totalHydrationTimeMs).toBe(123.5);
    // suppressAbort omitted (additive; Go treats absent as false).
    expect('suppressAbort' in req.params).toBe(false);
    host.deliverSuccess(req.id);
    const result = await p;
    expect(result.version).toBe('0000000002');
  });

  test('no abortBudget → no budget fields (old-server pairs see the rev-9 shape)', async () => {
    const client = new GoIVMClient();
    const host = makeAdvanceFakeHost(client);
    client.connectNapi(host.addon);

    const p = client.advanceToHeadStream('cg', 1, 'app');
    await flush();
    const req = host.reqs[0];
    expect('totalHydrationTimeMs' in req.params).toBe(false);
    expect('suppressAbort' in req.params).toBe(false);
    host.deliverSuccess(req.id);
    await p;
  });

  test('RPC_CODE_ADVANCE_ABORTED (-32103) rejects as AdvanceAbortedError with the TS message verbatim', async () => {
    const client = new GoIVMClient();
    const host = makeAdvanceFakeHost(client);
    client.connectNapi(host.addon);

    const msg =
      'Advancement exceeded timeout at 1499 of 30000 changes after 234.56789 ms. ' +
      'Advancement time limited based on total hydration time of 120.5 ms.';
    const p = client.advanceToHeadStream('cg', 1, 'app', {
      abortBudget: {totalHydrationTimeMs: 120.5},
    });
    await flush();
    host.deliver(host.reqs[0].id, undefined, {code: -32103, message: msg});
    await expect(p).rejects.toSatisfy(
      (e: unknown) => e instanceof AdvanceAbortedError && e.message === msg,
    );
  });

  test('RPC_CODE_ADVANCE_CLEAN_RETRYABLE (-32104) rejects as RetryableAdvanceError', async () => {
    const client = new GoIVMClient();
    const host = makeAdvanceFakeHost(client);
    client.connectNapi(host.addon);

    const p = client.advanceToHeadStream('cg', 1, 'app');
    await flush();
    host.deliver(host.reqs[0].id, undefined, {
      code: -32104,
      message: 'advanceToHeadStream advance: snapshotter: acquire conn (30s timeout): busy',
    });
    await expect(p).rejects.toBeInstanceOf(RetryableAdvanceError);
  });

  test('RPC_CODE_SCALAR_RESET (-32105) rejects as ScalarResetError with the TS message verbatim', async () => {
    // Pre-fix, -32105 fell through the ladder to the generic Error branch
    // (`RPC error -32105: ...`) → classifier 'unclassified' → CG teardown.
    // The Go message mirrors TS's ResetPipelinesSignal('scalar-subquery')
    // text; the typed error is what routes it to the same transparent reset.
    const client = new GoIVMClient();
    const host = makeAdvanceFakeHost(client);
    client.connectNapi(host.addon);

    const msg = 'Scalar subquery value changed for users: alice -> bob';
    const p = client.advanceToHeadStream('cg', 1, 'app');
    await flush();
    host.deliver(host.reqs[0].id, undefined, {code: -32105, message: msg});
    await expect(p).rejects.toSatisfy(
      (e: unknown) => e instanceof ScalarResetError && e.message === msg,
    );
  });

  test('NO wall-clock timeout in-process: still pending far past the old 120s default', async () => {
    vi.useFakeTimers();
    const client = new GoIVMClient();
    const host = makeAdvanceFakeHost(client);
    client.connectNapi(host.addon);

    let settled: 'resolved' | 'rejected' | undefined;
    const p = client
      .advanceToHeadStream('cg', 1, 'app', {abortBudget: {totalHydrationTimeMs: 1}})
      .then(
        () => (settled = 'resolved'),
        () => (settled = 'rejected'),
      );
    // 10 minutes — the pre-fix 120s TimeoutError would have fired 5x over.
    // The bound on a slow advance is Go's ECONOMIC abort (the budget riding
    // the request), not TS wall-clock.
    await flush();
    expect(host.reqs).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(600_000);
    expect(settled).toBeUndefined();
    host.deliverSuccess(host.reqs[0].id);
    await p;
    expect(settled).toBe('resolved');
  });

  test('chunk iterator yields row chunks before the final frame arrives', async () => {
    const client = new GoIVMClient();
    const host = makeAdvanceFakeHost(client);
    client.connectNapi(host.addon);

    const it = client.advanceToHeadStreamChunks('cg', 1, 'app');
    await flush();
    const req = host.reqs[0];

    host.deliver(req.id, {
      chunkIndex: 0,
      final: false,
      version: '0000000002',
      numChanges: 1,
      header: true,
    });
    host.deliver(req.id, {
      ...posFrame(['early']),
      chunkIndex: 0,
      final: false,
    });

    const header = await it.next();
    expect(header.done).toBe(false);
    expect(header.value).toMatchObject({
      changes: [],
      final: false,
      header: true,
      version: '0000000002',
      numChanges: 1,
    });

    const first = await it.next();
    expect(first.done).toBe(false);
    expect((first.value.changes[0] as RowChange).rowKey.id).toBe('early');

    host.deliver(req.id, {
      chunkIndex: 1,
      final: true,
      version: '0000000002',
      numChanges: 1,
    });
    host.deliver(req.id, 'done');
    const final = await it.next();
    expect(final.done).toBe(false);
    expect(final.value).toMatchObject({
      changes: [],
      final: true,
      version: '0000000002',
      numChanges: 1,
    });
    await expect(it.next()).resolves.toEqual({
      value: undefined,
      done: true,
    });
  });

  test('chunk iterator throws if done arrives without final frame', async () => {
    const client = new GoIVMClient();
    const host = makeAdvanceFakeHost(client);
    client.connectNapi(host.addon);

    const it = client.advanceToHeadStreamChunks('cg', 1, 'app');
    await flush();
    const req = host.reqs[0];

    host.deliver(req.id, {
      chunkIndex: 0,
      final: false,
      version: '0000000002',
      numChanges: 1,
      header: true,
    });
    await expect(it.next()).resolves.toMatchObject({
      done: false,
      value: {header: true},
    });

    host.deliver(req.id, 'done');
    await expect(it.next()).rejects.toThrow(
      /advanceToHeadStream finished without a final chunk/,
    );
  });
});
