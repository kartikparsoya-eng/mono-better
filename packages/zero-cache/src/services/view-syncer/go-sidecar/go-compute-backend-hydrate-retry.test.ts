// Regression for scale-review finding 9: a hydrateManyStream whose stream
// died MID-DELIVERY (socket restart) must NOT be replayed by the
// reinit-retry wrapper. The consumer (pipeline-driver) XORs every yielded
// row into per-query row-set signatures; replaying attempt 1's chunks
// double-XORs them (self-cancelling — permanent signature corruption →
// spurious drift resets) and duplicates CVR rows, and the retry hydrates
// against the restarted sidecar's FRESH snapshot so the replayed chunks may
// not even match. Uses a fake manager + client (no real sidecar).

import {describe, expect, test} from 'vitest';
import {GoComputeBackend} from './go-compute-backend.ts';
import type {SidecarManager} from './sidecar-manager.ts';

function fakeManager(client: unknown): SidecarManager {
  return {
    status: 'running',
    epoch: 1,
    sidecarSourceMode: 'table',
    onRestart: () => () => {},
    waitForRunning: () => Promise.resolve(),
    withInitSlot: <T>(fn: () => Promise<T>) => fn(),
    getClient: () => client,
  } as unknown as SidecarManager;
}

describe('hydrateManyStream restart retry (finding 9)', () => {
  test('a partially delivered stream is NOT replayed on restart retry', async () => {
    let attempts = 0;
    const client = {
      init: () => Promise.resolve({initEpoch: 1}),
      addQueriesStream: (
        _cg: string,
        _queries: unknown[],
        _epoch: number,
        onResult: (r: {
          queryID: string;
          changes: unknown[];
          timingMs: number | undefined;
          final?: boolean;
        }) => void,
      ) => {
        attempts++;
        // Two NON-final chunks reach the consumer, then the socket dies —
        // the mid-stream restart shape.
        onResult({queryID: 'q1', changes: [{}], timingMs: undefined, final: false});
        onResult({queryID: 'q1', changes: [{}], timingMs: undefined, final: false});
        return Promise.reject(new Error('Connection closed'));
      },
    };
    const backend = new GoComputeBackend(
      fakeManager(client),
      'cg-f9',
      () => ({}), // getCurrentTables: schema-only init, no loadRows
      () => [], // getCurrentQueries: nothing to re-register
    );
    await backend.initEngine({}); // initialized=true at manager epoch 1

    let received = 0;
    await expect(
      backend.hydrateManyStream([{queryID: 'q1', ast: {table: 't'} as never}], () => {
        received++;
      }),
    ).rejects.toThrow(/not retryable|Connection closed/);

    // Pre-fix: the reinit-retry silently re-ran the whole stream —
    // attempts === 2 and the consumer received attempt 1's chunks TWICE
    // (double-XORed signatures, duplicate CVR rows).
    expect(attempts).toBe(1);
    expect(received).toBe(2);
  });

  test('a failure BEFORE any chunk is still retried (retry stays useful)', async () => {
    let attempts = 0;
    const client = {
      init: () => Promise.resolve({initEpoch: 1}),
      addQueriesStream: (
        _cg: string,
        _queries: unknown[],
        _epoch: number,
        onResult: (r: {
          queryID: string;
          changes: unknown[];
          timingMs: number | undefined;
          final?: boolean;
        }) => void,
      ) => {
        attempts++;
        if (attempts === 1) {
          // Dies before ANY chunk — the classic instant restart failure.
          return Promise.reject(new Error('Connection closed'));
        }
        onResult({queryID: 'q1', changes: [{}], timingMs: undefined, final: true});
        return Promise.resolve();
      },
    };
    const backend = new GoComputeBackend(
      fakeManager(client),
      'cg-f9b',
      () => ({}),
      () => [],
    );
    await backend.initEngine({});

    let received = 0;
    await backend.hydrateManyStream(
      [{queryID: 'q1', ast: {table: 't'} as never}],
      () => {
        received++;
      },
    );
    expect(attempts).toBe(2); // zero-chunk failure retried once
    expect(received).toBe(1); // and delivered exactly once
  });
});
