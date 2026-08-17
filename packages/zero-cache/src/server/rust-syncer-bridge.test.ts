import {
  createServer as createHttpServer,
  type Server as HttpServer,
} from 'node:http';
import {connect, createServer, type Server, type Socket} from 'node:net';
import {resolver} from '@rocicorp/resolver';
import {afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {
  notifyRustSyncers,
  proxyUpgradeToRust,
  rebuildUpgradeRequest,
  rustSyncerEnv,
  type RustSyncerConfig,
  type UpgradeHandoff,
} from './rust-syncer-bridge.ts';

const lc = createSilentLogContext();

describe('rebuildUpgradeRequest', () => {
  test('reconstructs the request line + headers from rawHeaders', () => {
    const req = rebuildUpgradeRequest({
      method: 'GET',
      url: '/sync/v1/connect?clientGroupID=cg1&foo=bar',
      httpVersion: '1.1',
      rawHeaders: [
        'Host',
        'example.com',
        'Upgrade',
        'websocket',
        'Connection',
        'Upgrade',
        'Sec-WebSocket-Key',
        'dGhlIHNhbXBsZSBub25jZQ==',
        'Sec-WebSocket-Version',
        '13',
      ],
      headers: {},
    } as never);

    expect(req).toBe(
      'GET /sync/v1/connect?clientGroupID=cg1&foo=bar HTTP/1.1\r\n' +
        'Host: example.com\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n' +
        'Sec-WebSocket-Version: 13\r\n' +
        '\r\n',
    );
  });

  test('preserves duplicate headers and defaults method/version', () => {
    const req = rebuildUpgradeRequest({
      url: '/x',
      rawHeaders: ['Cookie', 'a=1', 'Cookie', 'b=2'],
      headers: {},
    } as never);
    expect(req).toBe('GET /x HTTP/1.1\r\nCookie: a=1\r\nCookie: b=2\r\n\r\n');
  });
});

describe('rustSyncerEnv', () => {
  const base: RustSyncerConfig = {
    replica: {file: '/data/replica.db'},
    cvr: {db: 'postgres://cvr'},
    upstream: {db: 'postgres://upstream'},
    taskID: 'task-7',
    app: {id: 'myapp'},
    shard: {num: 2},
    auth: {secret: 's3cret'},
    query: {
      url: ['https://api.example/query'],
      apiKey: 'query-key',
      allowedClientHeaders: ['x-request-id'],
      allowedRequestHeaders: ['x-forwarded-for'],
      forwardCookies: true,
    },
  };

  test('maps resolved config onto the env names main.rs reads', () => {
    const env = rustSyncerEnv(base, 'serving', 3100, 3200, 15);
    expect(env).toEqual({
      PORT: '3100',
      HTTP_PORT: '3200',
      CVR_PG_URI: 'postgres://cvr',
      CVR_MAX_CONNS: '15',
      REPLICA_FILE: '/data/replica.db',
      TASK_ID: 'task-7',
      ZERO_APP_ID: 'myapp',
      SHARD: '2',
      AUTH_SECRET: 's3cret',
      QUERY_URLS_JSON: '["https://api.example/query"]',
      QUERY_API_KEY: 'query-key',
      QUERY_ALLOWED_CLIENT_HEADERS_JSON: '["x-request-id"]',
      QUERY_ALLOWED_REQUEST_HEADERS_JSON: '["x-forwarded-for"]',
      QUERY_FORWARD_COOKIES: 'true',
    });
  });

  test('applies the serving-copy suffix to the replica file', () => {
    const env = rustSyncerEnv(base, 'serving-copy', 3100, 3200, 15);
    expect(env.REPLICA_FILE).toBe('/data/replica.db-serving-copy');
  });

  test('falls back to upstream.db when cvr.db is unset', () => {
    const env = rustSyncerEnv(
      {...base, cvr: {db: undefined}},
      'serving',
      3100,
      3200,
      15,
    );
    expect(env.CVR_PG_URI).toBe('postgres://upstream');
  });

  test('forwards jwk / jwksUrl auth and omits unset auth keys', () => {
    const env = rustSyncerEnv(
      {...base, auth: {jwk: '{"kty":"oct"}', jwksUrl: 'https://jwks'}},
      'serving',
      3100,
      3200,
      15,
    );
    expect(env.AUTH_JWK).toBe('{"kty":"oct"}');
    expect(env.AUTH_JWKS_URL).toBe('https://jwks');
    expect(env.AUTH_SECRET).toBeUndefined();
  });

  test('uses the legacy getQueries config when query has no URL', () => {
    const env = rustSyncerEnv(
      {
        ...base,
        query: {url: undefined},
        getQueries: {url: ['https://legacy.example/query']},
      },
      'serving',
      3100,
      3200,
      15,
    );
    expect(env.QUERY_URLS_JSON).toBe('["https://legacy.example/query"]');
  });

  test('omits ENABLE_QUERY_COVERING unless explicitly disabled', () => {
    // Default / unset: Rust defaults to true, so no env is forwarded.
    expect(
      rustSyncerEnv(base, 'serving', 3100, 3200, 15).ENABLE_QUERY_COVERING,
    ).toBeUndefined();
    expect(
      rustSyncerEnv(
        {...base, enableQueryCovering: true},
        'serving',
        3100,
        3200,
        15,
      ).ENABLE_QUERY_COVERING,
    ).toBeUndefined();
    // Explicit opt-out is forwarded.
    expect(
      rustSyncerEnv(
        {...base, enableQueryCovering: false},
        'serving',
        3100,
        3200,
        15,
      ).ENABLE_QUERY_COVERING,
    ).toBe('false');
  });
});

describe('proxyUpgradeToRust', () => {
  const servers: (Server | HttpServer)[] = [];
  const sockets: Socket[] = [];

  afterEach(() => {
    for (const s of sockets) s.destroy();
    sockets.length = 0;
    for (const s of servers) s.close();
    servers.length = 0;
  });

  function listen(server: Server): Promise<number> {
    servers.push(server);
    const {promise, resolve} = resolver<number>();
    server.listen(0, '127.0.0.1', () => {
      resolve((server.address() as {port: number}).port);
    });
    return promise;
  }

  test('replays the upgrade request + head, then pipes both directions', async () => {
    // Fake rust-syncer: collect bytes, and echo a canned 101 response back.
    const rustRecv = resolver<Buffer>();
    const upstreamChunks: Buffer[] = [];
    let frameSeen = false;
    const {promise: frameP, resolve: frameResolve} = resolver<void>();
    const rustServer = createServer(sock => {
      sockets.push(sock);
      sock.on('data', chunk => {
        upstreamChunks.push(chunk);
        const all = Buffer.concat(upstreamChunks).toString();
        // Once the request + head arrived, send a response back downstream.
        if (all.includes('\r\n\r\nHEAD') && !frameSeen) {
          sock.write('HTTP/1.1 101 Switching Protocols\r\n\r\nPONG');
          rustRecv.resolve(Buffer.concat(upstreamChunks));
        }
        if (all.includes('CLIENTFRAME')) {
          frameSeen = true;
          frameResolve();
        }
      });
    });
    const rustPort = await listen(rustServer);

    // A connected socket pair: `clientSocket` is the dispatcher-held end.
    const clientSocketP = resolver<Socket>();
    const browserServer = createServer(sock => {
      sockets.push(sock);
      clientSocketP.resolve(sock);
    });
    const browserPort = await listen(browserServer);
    const browserEnd = connect(browserPort, '127.0.0.1');
    sockets.push(browserEnd);
    const clientSocket = await clientSocketP.promise;

    const handoff: UpgradeHandoff = [
      'handoff',
      {
        message: {
          method: 'GET',
          url: '/sync/v1/connect?clientGroupID=cg1',
          httpVersion: '1.1',
          rawHeaders: ['Upgrade', 'websocket', 'Connection', 'Upgrade'],
          headers: {},
        } as never,
        head: new Uint8Array([0x48, 0x45, 0x41, 0x44]).buffer, // "HEAD"
      },
    ];

    proxyUpgradeToRust(lc, handoff, clientSocket, rustPort);

    // Upstream received the reconstructed request followed by the head bytes.
    const received = (await rustRecv.promise).toString();
    expect(received).toBe(
      'GET /sync/v1/connect?clientGroupID=cg1 HTTP/1.1\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        '\r\n' +
        'HEAD',
    );

    // rust → browser: the canned response is piped back to the browser end.
    const downstream = await new Promise<string>(res => {
      browserEnd.on('data', d => res(d.toString()));
    });
    expect(downstream).toContain('PONG');

    // browser → rust: a subsequent client frame reaches the upstream.
    browserEnd.write('CLIENTFRAME');
    await frameP;
    expect(frameSeen).toBe(true);
  });

  test('cleans up the client socket when the upstream is unreachable', async () => {
    const clientSocketP = resolver<Socket>();
    const browserServer = createServer(sock => {
      sockets.push(sock);
      clientSocketP.resolve(sock);
    });
    const browserPort = await listen(browserServer);
    const browserEnd = connect(browserPort, '127.0.0.1');
    sockets.push(browserEnd);
    const clientSocket = await clientSocketP.promise;

    const closedP = new Promise<void>(res =>
      clientSocket.on('close', () => res()),
    );

    // Port 1 is (essentially) always refused.
    proxyUpgradeToRust(
      lc,
      [
        'handoff',
        {
          message: {url: '/x', rawHeaders: [], headers: {}} as never,
          head: new ArrayBuffer(0),
        },
      ],
      clientSocket,
      1,
    );

    // The client socket is destroyed on upstream failure (no hang).
    await closedP;
    expect(clientSocket.destroyed).toBe(true);
  });
});

describe('notifyRustSyncers', () => {
  const servers: HttpServer[] = [];
  afterEach(() => {
    for (const s of servers) s.close();
    servers.length = 0;
  });

  test('POSTs /notify version-ready to every port; a dead port does not throw', async () => {
    const bodies: string[] = [];
    const paths: string[] = [];
    const {promise, resolve} = resolver<void>();
    const server = createHttpServer((req, res) => {
      paths.push(req.url ?? '');
      let body = '';
      req.on('data', c => (body += c));
      req.on('end', () => {
        bodies.push(body);
        res.writeHead(200, {'content-type': 'application/json'});
        res.end('{"ok":true}');
        resolve();
      });
    });
    servers.push(server);
    const port = await new Promise<number>(res => {
      server.listen(0, '127.0.0.1', () =>
        res((server.address() as {port: number}).port),
      );
    });

    // One live port + one refused port (1). Must resolve without throwing.
    await notifyRustSyncers(lc, [port, 1]);
    await promise;

    expect(paths).toEqual(['/notify']);
    expect(JSON.parse(bodies[0])).toEqual({state: 'version-ready'});
  });

  test('retries a transient failure until it is delivered', async () => {
    // Fail the first two requests (503), then succeed — the notification must
    // still be delivered, matching the TS Notifier's eventually-delivered
    // guarantee rather than being dropped on the first failure.
    let attempts = 0;
    const {promise: delivered, resolve} = resolver<void>();
    const server = createHttpServer((req, res) => {
      attempts++;
      req.on('data', () => {});
      req.on('end', () => {
        if (attempts < 3) {
          res.writeHead(503);
          res.end();
          return;
        }
        res.writeHead(200, {'content-type': 'application/json'});
        res.end('{"ok":true}');
        resolve();
      });
    });
    servers.push(server);
    const port = await new Promise<number>(res => {
      server.listen(0, '127.0.0.1', () =>
        res((server.address() as {port: number}).port),
      );
    });

    await notifyRustSyncers(lc, [port]);
    await delivered;

    expect(attempts).toBe(3);
  });
});
