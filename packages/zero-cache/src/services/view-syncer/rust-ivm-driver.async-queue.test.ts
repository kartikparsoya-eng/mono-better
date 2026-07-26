import {describe, expect, test} from 'vitest';
import {AsyncQueue, deferClose} from './rust-ivm-driver.ts';

// Regression guard for the streaming 1-row-drop bug.
//
// The napi streaming methods signal completion on a DIFFERENT channel than the
// rows: rows arrive via TSFN callbacks (macrotasks) while the returned Promise
// resolves when the worker's `compute()` finishes. The Promise can therefore
// resolve while the final row's TSFN callback is still queued. Closing the
// AsyncQueue directly in `.then()` (a microtask) sets `#done` before that
// callback runs, and `push()` silently drops the row. `deferClose` fixes this
// by deferring `close()` to the check phase (after the poll-phase callbacks).
//
// These tests model that exact race: schedule N pushes as pending macrotasks,
// then resolve a completion Promise (microtask) that closes the queue — and
// assert every row still drains (N in -> N out).

/**
 * Simulate the napi producer: enqueue N row deliveries as macrotasks (like TSFN
 * callbacks), then resolve a completion Promise whose `.then` closes the queue
 * via `close`. Returns the completion Promise.
 */
function simulateNapiStream(
  queue: AsyncQueue<number>,
  n: number,
  close: (q: AsyncQueue<number>) => void,
): Promise<void> {
  for (let i = 0; i < n; i++) {
    setImmediate(() => queue.push(i));
  }
  // Resolves NOW — before the scheduled push macrotasks fire — exactly like the
  // napi AsyncTask resolving while its TSFN row callbacks are still queued.
  return Promise.resolve().then(() => close(queue));
}

async function drain(queue: AsyncQueue<number>): Promise<number[]> {
  const got: number[] = [];
  for await (const x of queue) {
    got.push(x);
  }
  return got;
}

describe('rust-ivm-driver AsyncQueue completion race', () => {
  // The exact boundary counts that matter: empty, single (the minimal drop
  // case), and either side of the streaming yield boundary (every 100 rows).
  for (const n of [0, 1, 100, 101]) {
    test(`deferClose preserves all rows: ${n} in -> ${n} out`, async () => {
      const queue = new AsyncQueue<number>();
      const completion = simulateNapiStream(queue, n, deferClose);

      const got = await drain(queue);
      await completion;

      expect(got.length).toBe(n);
      expect(got).toEqual(Array.from({length: n}, (_, i) => i));
    });
  }

  // Proves the test has teeth: the pre-fix behavior (closing immediately in the
  // completion microtask) drops every row that was still a pending macrotask.
  // If someone reverts deferClose to a direct close(), the tests above fail —
  // this documents why.
  test('immediate close (pre-fix) drops pending rows', async () => {
    const queue = new AsyncQueue<number>();
    const completion = simulateNapiStream(queue, 5, q => q.close());

    const got = await drain(queue);
    await completion;

    expect(got.length).toBe(0);
  });
});
