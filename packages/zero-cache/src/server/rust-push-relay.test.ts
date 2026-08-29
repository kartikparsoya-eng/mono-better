import {createServer, type Server} from 'node:http';
import type {AddressInfo} from 'node:net';
import {afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {startRustPushRelay} from './rust-push-relay.ts';

/**
 * The relay must be status-TRANSPARENT for upstream AUTH rejections: the rust
 * drainer's failConnection branch (the port of pusher.ts `isAuthErrorBody` →
 * `failConnection`) keys on the relay response being 401/403. The 2026-08-29
 * prod incident: backend answered 401 ("Invalid or expired token"), the relay
 * collapsed it to 502, so rust never invalidated the connection and the
 * dead-token client retried forever. Non-auth upstream failures must STAY 502
 * (relay-hop semantics). Proven failing against the flat-502 catch-all.
 */
describe('rust-push-relay upstream status propagation', () => {
  const servers: Server[] = [];

  afterEach(async () => {
    for (const s of servers) {
      await new Promise(resolve => s.close(resolve));
    }
    servers.length = 0;
  });

  function stubAPI(status: number, body: string): Promise<string> {
    const api = createServer((_req, res) => {
      res.writeHead(status, {'content-type': 'application/json'});
      res.end(body);
    });
    servers.push(api);
    return new Promise(resolve =>
      api.listen(0, '127.0.0.1', () =>
        resolve(`http://127.0.0.1:${(api.address() as AddressInfo).port}/push`),
      ),
    );
  }

  async function relayStatusFor(
    upstreamStatus: number,
    upstreamBody: string,
  ): Promise<number> {
    const apiURL = await stubAPI(upstreamStatus, upstreamBody);
    const lc = createSilentLogContext();
    const config = {
      push: {url: [apiURL]},
      mutate: {},
      app: {id: 'zero'},
      shard: {num: 0},
    } as unknown as Parameters<typeof startRustPushRelay>[1];
    const started = await startRustPushRelay(lc, config, 'relay-token');
    expect(started).toBeDefined();
    servers.push(started!.server);

    const res = await fetch(started!.url, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-relay-auth': 'relay-token',
      },
      body: JSON.stringify({
        clientGroupID: 'cg1',
        clientID: 'c1',
        auth: 'expired-token',
        push: {
          clientGroupID: 'cg1',
          mutations: [],
          pushVersion: 1,
          timestamp: 1,
          requestID: 'r1',
        },
      }),
    });
    await res.text();
    return res.status;
  }

  test('upstream 401 (auth rejection) propagates as 401', async () => {
    expect(
      await relayStatusFor(
        401,
        JSON.stringify({
          error: 'Invalid or expired token',
          message: 'Token verification failed',
        }),
      ),
    ).toBe(401);
  });

  test('upstream 500 (non-auth failure) stays 502', async () => {
    expect(await relayStatusFor(500, JSON.stringify({error: 'boom'}))).toBe(
      502,
    );
  });
});
