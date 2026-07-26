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

  // Once closed, further pushes are dropped (return false) — this is what bounds
  // memory when the driver's finally closes the queue on early consumer exit
  // (Fix 2a): the engine may still push a few rows before it observes cancel.
  test('push after close is dropped, not buffered', () => {
    const queue = new AsyncQueue<number>();
    expect(queue.push(1)).toBe(true);
    queue.close();
    expect(queue.push(2)).toBe(false);
    expect(queue.push(3)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// #advanceStreaming header handling (Fix 1: reject, don't hang).
//
// The advance streaming path awaits a separate `headerPromise` that the FIRST
// TSFN callback resolves. The engine can fail BEFORE emitting any row (the
// snapshotter's advance() errors, or engine/snapshotter not initialized): then
// no callback fires and the producer Promise rejects. These tests model that
// wiring and assert the header promise must REJECT (propagate as teardown),
// never hang. The pre-fix code captured only `resolve`, so the await hung.
// ---------------------------------------------------------------------------

/** Model of the driver's header-promise wiring around a producer promise. */
function awaitHeaderLike<T>(
  producer: Promise<void>,
  deliver: (cb: (row: T) => void) => void,
): Promise<T> {
  let headerResolve: ((row: T) => void) | null = null;
  let headerReject: ((e: unknown) => void) | null = null;
  const headerPromise = new Promise<T>((resolve, reject) => {
    headerResolve = resolve;
    headerReject = reject;
  });
  deliver(row => {
    if (headerResolve) {
      headerResolve(row);
      headerResolve = null;
      headerReject = null;
    }
  });
  producer.then(() => {}).catch((e: unknown) => headerReject?.(e)); // Fix 1: reject on pre-header error
  return headerPromise;
}

describe('rust-ivm-driver #advanceStreaming header handling', () => {
  test('rejects when the producer errors before the first row (no hang)', async () => {
    // Producer rejects without ever delivering a row.
    const producer = Promise.reject(new Error('snapshotter advance failed'));
    const header = awaitHeaderLike<number>(producer, () => {
      /* no callback — engine failed before emitting the header */
    });
    await expect(header).rejects.toThrow('snapshotter advance failed');
  });

  test('resolves with the first row when the engine emits a header', async () => {
    let cb: ((row: number) => void) | null = null;
    const producer = new Promise<void>(resolve => {
      // Deliver the header row, then complete.
      setImmediate(() => {
        cb?.(-1);
        resolve();
      });
    });
    const header = await awaitHeaderLike<number>(producer, c => {
      cb = c;
    });
    expect(header).toBe(-1);
  });

  test('a post-header producer error does not double-settle the header', async () => {
    // Header arrives first; then the producer rejects (mid-stream error). The
    // header promise already resolved, so headerReject is null — the reject is
    // a no-op, not an unhandled rejection or double-settle.
    let cb: ((row: number) => void) | null = null;
    let rejectProducer!: (e: unknown) => void;
    const producer = new Promise<void>((_res, rej) => {
      rejectProducer = rej;
    });
    const headerP = awaitHeaderLike<number>(producer, c => {
      cb = c;
    });
    cb!(-1); // header delivered synchronously via deliver()
    const header = await headerP;
    expect(header).toBe(-1);
    rejectProducer(new Error('mid-stream'));
    await expect(producer).rejects.toThrow('mid-stream'); // handled, no crash
  });
});
