/**
 * Stalled-connection wedge — repro + regression for the frozen-read-mark /
 * unbounded-WAL class.
 *
 * The poke path awaits each downstream push's `result`, which settles only
 * when the outbound ws pipeline consumes the message. A stalled-but-OPEN
 * socket — a silently dead peer before the kernel TCP timeout (~15-25 min),
 * or a suspended/backgrounded tab whose kernel keeps ACKing into a TCP
 * zero-window (which never times out) — leaves that await pending forever.
 * The server sends pongs but enforces NO inbound liveness on client sockets,
 * and the advancement-timeout breaker only runs while rows are flowing, so
 * nothing else bounds the stall.
 *
 * Because pokes run inside the view-syncer lock (view-syncer.ts
 * #advancePipelines → pokers.addPatch/end, and initConnection catchup), ONE
 * such client freezes advances for the ENTIRE client group: the snapshotter's
 * pinned read-marks stop moving, wal2 checkpointing starves behind them, and
 * the replica WAL grows at the write rate — from a healthy-looking,
 * epoll-idle process (the observed prod wedge).
 *
 * The fix (client-handler.ts PUSH_CONSUME_TIMEOUT_MS): a push unconsumed for
 * 60s fails THAT connection — Subscription cleanup settles every pending
 * push, the poke chain (#pokeTail) releases, sibling clients keep advancing,
 * and the failed client reconnects and catches up. Without the fix, every
 * await in this file hangs forever.
 */
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {Subscription} from '../../types/subscription.ts';
import {ClientHandler, startPoke} from './client-handler.ts';

const SHARD = {appID: 'zapp', shardNum: 6};
const PUSH_TIMEOUT_MS = 60_000; // default PUSH_CONSUME_TIMEOUT_MS

describe('view-syncer/client-handler stalled connection', () => {
  const lc = createSilentLogContext();

  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  /** A downstream whose consumer never runs — the stalled-but-open socket. */
  function stalledSubscription() {
    const unconsumed: Downstream[] = [];
    let cleanupErr: Error | undefined;
    const subscription = Subscription.create<Downstream>({
      cleanup: (msgs, err) => {
        unconsumed.push(...msgs);
        cleanupErr = err;
      },
    });
    return {subscription, unconsumed, err: () => cleanupErr};
  }

  /** A downstream drained by a live consumer — a healthy socket. */
  function healthySubscription() {
    const received: Downstream[] = [];
    const subscription = Subscription.create<Downstream>({});
    void (async () => {
      try {
        for await (const msg of subscription) {
          received.push(msg);
        }
      } catch {
        // failed/cancelled — fine for these tests.
      }
    })();
    return {subscription, received};
  }

  function newHandler(clientID: string, downstream: Subscription<Downstream>) {
    return new ClientHandler(
      lc,
      'g1',
      clientID,
      `ws-${clientID}`,
      SHARD,
      '121',
      downstream,
    );
  }

  test('a poke to a stalled connection settles at the push timeout and fails the connection', async () => {
    const {subscription, err} = stalledSubscription();
    const handler = newHandler('stalled', subscription);

    const poker = handler.startPoke({stateVersion: '123'});
    let settled = false;
    const done = poker.end({stateVersion: '123'}).then(() => {
      settled = true;
    });

    // The wedge: nothing consumes the downstream; without the timeout this
    // promise NEVER settles (and in prod holds the view-syncer lock forever).
    await vi.advanceTimersByTimeAsync(PUSH_TIMEOUT_MS - 1_000);
    expect(settled).toBe(false);

    await vi.advanceTimersByTimeAsync(2_000);
    await done;
    expect(settled).toBe(true);

    // The connection was failed (client will reconnect and catch up).
    expect(err()?.message).toMatch(/not consuming pokes/);
  });

  test('the poke chain (#pokeTail) is released — subsequent pokes settle immediately', async () => {
    const {subscription} = stalledSubscription();
    const handler = newHandler('stalled', subscription);

    const first = handler.startPoke({stateVersion: '123'}).end({
      stateVersion: '123',
    });
    await vi.advanceTimersByTimeAsync(PUSH_TIMEOUT_MS + 1_000);
    await first;

    // The connection is failed: later pokes must not wedge behind the chain
    // (pushes to a terminated subscription settle 'unconsumed' instantly).
    let settled = false;
    const second = handler.startPoke({stateVersion: '125'}).end({
      stateVersion: '125',
    });
    void second.then(() => {
      settled = true;
    });
    await vi.advanceTimersByTimeAsync(1);
    expect(settled).toBe(true);
    await second;
  });

  test('one stalled client cannot block the client group: siblings still get their pokes', async () => {
    const stalled = stalledSubscription();
    const healthy = healthySubscription();
    const a = newHandler('stalled', stalled.subscription);
    const b = newHandler('healthy', healthy.subscription);

    // The client-group-wide poker used by #advancePipelines.
    const pokers = startPoke([a, b], {stateVersion: '123'});
    let settled = false;
    const done = pokers.end({stateVersion: '123'}).then(() => (settled = true));

    // Promise.allSettled tolerates FAILED clients but not never-settling
    // ones: before the fix this hung forever (freezing the whole CG's
    // advances). With the fix, the stalled client is failed at the timeout
    // and the group settles.
    await vi.advanceTimersByTimeAsync(PUSH_TIMEOUT_MS + 1_000);
    await done;
    expect(settled).toBe(true);

    expect(stalled.err()?.message).toMatch(/not consuming pokes/);
    // The healthy sibling received its poke.
    expect(healthy.received.map(m => m[0])).toEqual(['pokeStart', 'pokeEnd']);
  });

  test('a consumed connection never trips the timeout', async () => {
    const {subscription, received} = healthySubscription();
    const handler = newHandler('healthy', subscription);

    const poker = handler.startPoke({stateVersion: '123'});
    // No timer advancement needed: consumption settles the pushes.
    await poker.end({stateVersion: '123'});
    expect(received.map(m => m[0])).toEqual(['pokeStart', 'pokeEnd']);
  });
});
