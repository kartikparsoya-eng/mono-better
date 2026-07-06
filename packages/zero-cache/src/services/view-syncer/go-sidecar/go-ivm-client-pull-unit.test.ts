// Unit tests for GoIVMClient.addQueriesStreamPull (ABI v3 pull hydration,
// DESIGN-duplex-streaming) against a FAKE GoNapiAddon that emulates the Go
// side's demand gate: it "produces" a row-bearing frame only while it holds
// credit (opening window = params.pullWindow, top-ups via streamCredit) and
// unwinds on streamCancel with a terminal error frame — exactly the sidecar
// contract pinned by go-ivm's cmd/sidecar/pullmode_test.go. No real .node /
// libgoivm needed, so these run everywhere.

import {describe, expect, test} from 'vitest';
import {Packr} from 'msgpackr';
import {GoIVMClient, type RowChange} from './go-ivm-client.ts';
import type {GoNapiAddon} from './napi/index.ts';

// Mirror the client codec's settings (useRecords: false, mapsAsObjects) so
// the fake host decodes requests to plain objects and packs frames the
// client's Unpackr accepts — same shape as the Go side's vmihailenco codec.
const packr = new Packr({
  useRecords: false,
  encodeUndefinedAsNil: true,
  mapsAsObjects: true,
  useBigIntExtension: false,
});

type FakeHost = {
  addon: GoNapiAddon;
  /** Rows the fake will produce for the single query 'q1'. */
  rows: number;
  /** Max produced-minus-granted observed (must stay ≤ 0: never over-produce). */
  overProduction: number;
  /** Total credits received (opening window + top-ups). */
  granted: number;
  produced: number;
  cancelled: boolean;
  /** When true, deliver an error frame after `errorAfter` rows. */
  errorAfter?: number;
  /** When true, skip the final frame before "done" (orphan-guard test). */
  omitFinal?: boolean;
};

// makeFakeHost wires a GoIVMClient to an in-memory pull-aware host. The
// client must be created first so deliveries route to it.
function makeFakeHost(client: GoIVMClient, rows: number, opts?: {errorAfter?: number; omitFinal?: boolean}): FakeHost {
  const host: FakeHost = {
    addon: undefined as unknown as GoNapiAddon,
    rows,
    overProduction: 0,
    granted: 0,
    produced: 0,
    cancelled: false,
    ...(opts?.errorAfter !== undefined ? {errorAfter: opts.errorAfter} : {}),
    ...(opts?.omitFinal ? {omitFinal: true} : {}),
  };

  let reqID = 0;
  let settled = false;

  const deliver = (result: unknown, error?: {code: number; message: string}) => {
    const frame = error
      ? {jsonrpc: '2.0', error, id: reqID}
      : {jsonrpc: '2.0', result, id: reqID};
    client.handleNapiDelivery(1, packr.pack(frame) as Buffer);
  };

  const settle = (error?: {code: number; message: string}) => {
    if (settled) return;
    settled = true;
    if (error) {
      deliver(undefined, error);
    } else {
      if (!host.omitFinal) {
        deliver({queryID: 'q1', chunkIndex: host.produced, final: true, timingMs: 1.5});
      }
      deliver('done');
    }
  };

  // pump produces while credit allows — asynchronously, like the real TSFN.
  const pump = () => {
    queueMicrotask(() => {
      if (settled || host.cancelled) return;
      while (host.produced < host.rows && host.granted - host.produced > 0) {
        if (host.errorAfter !== undefined && host.produced >= host.errorAfter) {
          settle({code: -32000, message: 'synthetic mid-stream failure'});
          return;
        }
        const i = host.produced++;
        if (host.produced - host.granted > 0) {
          host.overProduction = Math.max(host.overProduction, host.produced - host.granted);
        }
        deliver({
          queryID: 'q1',
          changes: [
            {type: 0, queryID: 'q1', table: 't', rowKey: {id: `r${i}`}, row: {id: `r${i}`}},
          ],
          chunkIndex: i,
          final: false,
        });
      }
      if (host.produced >= host.rows) {
        if (host.errorAfter !== undefined && host.produced >= host.errorAfter) {
          settle({code: -32000, message: 'synthetic mid-stream failure'});
          return;
        }
        settle();
      }
    });
  };

  host.addon = {
    start: () => {},
    abiVersion: () => 3,
    send: (payload: Buffer) => {
      const req = packr.unpack(payload) as {
        id: number;
        method: string;
        params: {pullMode?: boolean; pullWindow?: number};
      };
      if (req.method !== 'addQueriesStream' || req.params.pullMode !== true) {
        throw new Error(`fake host: unexpected request ${req.method}`);
      }
      reqID = req.id;
      host.granted = req.params.pullWindow ?? 0; // opening window rides the request
      pump();
      return 0;
    },
    streamCredit: (id: number, n: number) => {
      expect(id).toBe(reqID);
      host.granted += n;
      pump();
    },
    streamCancel: (id: number) => {
      expect(id).toBe(reqID);
      host.cancelled = true;
      // Go settles the RPC with the bookkeeping error frame (I9: -32000).
      settle({code: -32000, message: 'addQueriesStream: hydrate stream cancelled by consumer'});
    },
  };
  return host;
}

describe('GoIVMClient.addQueriesStreamPull', () => {
  test('delivers all rows in order; host never produces past the granted window', async () => {
    const client = new GoIVMClient('napi:test');
    const host = makeFakeHost(client, 10);
    client.connectNapi(host.addon);

    const seen: string[] = [];
    let finals = 0;
    for await (const entry of client.addQueriesStreamPull('cg', [{queryID: 'q1', ast: {}}], 1, {window: 4})) {
      if (entry.final) {
        finals++;
        expect(entry.timingMs).toBe(1.5);
        continue;
      }
      expect(entry.changes).toHaveLength(1);
      seen.push((entry.changes[0] as RowChange).rowKey.id as string);
    }
    expect(seen).toEqual(Array.from({length: 10}, (_, i) => `r${i}`));
    expect(finals).toBe(1);
    // The gate held: the fake never produced a row it had no credit for.
    expect(host.overProduction).toBe(0);
    // Grants are bounded: opening window + top-ups never exceed rows + W.
    expect(host.granted).toBeLessThanOrEqual(10 + 4);
  });

  test('W=1 is strict lockstep: produced never exceeds consumed + 1', async () => {
    const client = new GoIVMClient('napi:test');
    const host = makeFakeHost(client, 5);
    client.connectNapi(host.addon);

    let consumed = 0;
    for await (const entry of client.addQueriesStreamPull('cg', [{queryID: 'q1', ast: {}}], 1, {window: 1})) {
      if (!entry.final) {
        consumed++;
        // I6 at W=1: at the moment a row is served, the host can be at
        // most ONE delivery ahead of consumption.
        expect(host.produced).toBeLessThanOrEqual(consumed + 1);
      }
    }
    expect(consumed).toBe(5);
  });

  test('return() cancels the Go producer and settles quietly', async () => {
    const client = new GoIVMClient('napi:test');
    const host = makeFakeHost(client, 100);
    client.connectNapi(host.addon);

    const it = client.addQueriesStreamPull('cg', [{queryID: 'q1', ast: {}}], 1, {window: 2});
    let rows = 0;
    for await (const entry of it) {
      if (!entry.final) rows++;
      if (rows === 3) break; // for-await break → it.return() → streamCancel
    }
    expect(host.cancelled).toBe(true);
    // Production stopped near the cancel point (bounded by the window),
    // nowhere near the full 100 rows.
    expect(host.produced).toBeLessThanOrEqual(3 + 2 + 1);
    // The RPC's bookkeeping rejection was swallowed (no unhandled
    // rejection — vitest would fail the test file otherwise). Give the
    // settle microtasks a tick to flush.
    await new Promise(r => setTimeout(r, 10));
  });

  test('mid-stream Go error surfaces as a throw from next()', async () => {
    const client = new GoIVMClient('napi:test');
    const host = makeFakeHost(client, 10, {errorAfter: 2});
    client.connectNapi(host.addon);

    const seen: string[] = [];
    await expect(async () => {
      for await (const entry of client.addQueriesStreamPull('cg', [{queryID: 'q1', ast: {}}], 1, {window: 8})) {
        if (!entry.final) seen.push((entry.changes[0] as RowChange).rowKey.id as string);
      }
    }).rejects.toThrow(/synthetic mid-stream failure/);
    expect(seen).toEqual(['r0', 'r1']);
    expect(host.cancelled).toBe(false); // Go failed on its own; no client cancel
  });

  test('done without a final chunk trips the orphan guard', async () => {
    const client = new GoIVMClient('napi:test');
    const host = makeFakeHost(client, 2, {omitFinal: true});
    client.connectNapi(host.addon);

    await expect(async () => {
      for await (const _ of client.addQueriesStreamPull('cg', [{queryID: 'q1', ast: {}}], 1, {window: 4})) {
        // drain
      }
    }).rejects.toThrow(/never received a final chunk/);
  });

  test('requires the NAPI transport', () => {
    const client = new GoIVMClient('/tmp/nonexistent.sock');
    expect(() => client.addQueriesStreamPull('cg', [{queryID: 'q1', ast: {}}], 1)).toThrow(
      /requires the NAPI transport/,
    );
  });
});
