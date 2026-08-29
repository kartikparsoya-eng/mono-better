import {createServer, type Server} from 'node:http';
import type {AddressInfo} from 'node:net';
import type {LogContext} from '@rocicorp/logger';
import {isProtocolError} from '../../../zero-protocol/src/error.ts';
import {mutateResponseSchema} from '../../../zero-protocol/src/mutate-server.ts';
import {isAuthErrorBody} from '../auth/auth.ts';
import type {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {compileUrlPattern, fetchFromAPIServer} from '../custom/fetch.ts';
import type {
  ConnectionContext,
  ConnectionFetchContext,
} from '../services/view-syncer/connection-context-manager.ts';
import {getShardID} from '../types/shards.ts';

type NormalizedZeroConfig = ReturnType<typeof getNormalizedZeroConfig>;

/**
 * The JSON body the Rust syncer POSTs when it relays a custom push. The Rust
 * syncer runs zero mutation logic — it forwards the raw push body plus this
 * connection's auth/header material, and THIS endpoint rebuilds the
 * `userPushURL` request via the same `fetchFromAPIServer('push', …)` path the
 * in-process pusher uses. So the write path stays entirely in TS.
 */
type RelayedPush = {
  clientGroupID: string;
  clientID: string;
  push: unknown;
  auth?: string | null;
  cookie?: string | null;
  origin?: string | null;
  /** `[[name, value], …]` — raw incoming request headers (unfiltered). */
  requestHeaders?: [string, string][] | null;
  userID?: string | null;
  /**
   * Client-supplied push overrides from `initConnection` (the in-process
   * pusher honors these per connection via ConnectionContextManager). The
   * URL must still match the configured push-URL allowlist —
   * `fetchFromAPIServer` enforces that — and the headers pass through the
   * `allowedClientHeaders` filter below.
   */
  userPushURL?: string | null;
  userPushHeaders?: Record<string, string> | null;
};

/** Port of the private `filterHeaders` in connection-context-manager.ts. */
function filterHeaders(
  headers: Record<string, string> | undefined,
  allowedHeaders: readonly string[] | undefined,
): Record<string, string> | undefined {
  if (!headers || !allowedHeaders || allowedHeaders.length === 0) {
    return undefined;
  }
  const allowed = new Set(allowedHeaders.map(h => h.toLowerCase()));
  let filtered: Record<string, string> | undefined;
  for (const [key, value] of Object.entries(headers)) {
    if (allowed.has(key.toLowerCase())) {
      filtered ??= {};
      filtered[key] = value;
    }
  }
  return filtered;
}

/**
 * Starts the HTTP endpoint the Rust syncer relays custom pushes to. Bound to
 * loopback and gated by a shared token (`x-relay-auth`). Returns the server and
 * the URL the Rust syncer should POST to (handed to it via `PUSHER_URL`).
 *
 * Returns `undefined` when no push/mutate URL is configured — with nothing to
 * forward to, the Rust syncer keeps rejecting stray WS pushes as before.
 */
export function startRustPushRelay(
  lc: LogContext,
  config: NormalizedZeroConfig,
  token: string,
): Promise<{server: Server; url: string} | undefined> {
  // `push.url`/`mutate.url` are string arrays (URL allowlists). The first entry
  // is the URL we POST to; all entries form the allowed-URL patterns.
  const urls = config.push.url ?? config.mutate.url;
  if (!urls || urls.length === 0) {
    lc.info?.('rust push relay: no push/mutate URL configured; not started');
    return Promise.resolve(undefined);
  }
  const pushURL = urls[0];
  // Resolve push config the same way server/syncer.ts does (push wins, mutate
  // fallback), so the forwarded request matches the in-process pusher exactly.
  const pushConfig = {
    ...config.push,
    ...config.mutate,
  };
  const allowedUrlPatterns = urls.map(compileUrlPattern);
  const shard = getShardID(config);

  lc = lc.withContext('component', 'rust-push-relay');

  const server = createServer((req, res) => {
    const fail = (status: number, message: string) => {
      res.writeHead(status, {'content-type': 'application/json'});
      res.end(JSON.stringify({error: message}));
    };
    if (req.method !== 'POST') {
      return fail(405, 'method not allowed');
    }
    if (req.headers['x-relay-auth'] !== token) {
      return fail(403, 'forbidden');
    }
    const chunks: Buffer[] = [];
    req.on('data', c => chunks.push(c as Buffer));
    req.on('end', () => {
      void (async () => {
        let relayed: RelayedPush;
        try {
          relayed = JSON.parse(Buffer.concat(chunks).toString('utf8'));
        } catch {
          return fail(400, 'invalid JSON body');
        }

        const requestHeaders: Record<string, string> = {};
        for (const [k, v] of relayed.requestHeaders ?? []) {
          requestHeaders[k] = v;
        }

        const mutateContext: ConnectionFetchContext = {
          // The client's userPushURL override wins, exactly like the
          // in-process pusher (connection-context-manager.ts). An override
          // outside the allowlist fails THIS push inside fetchFromAPIServer,
          // not the connection.
          url: relayed.userPushURL ?? pushURL,
          allowedUrlPatterns,
          headerOptions: {
            apiKey: pushConfig.apiKey,
            customHeaders: filterHeaders(
              relayed.userPushHeaders ?? undefined,
              pushConfig.allowedClientHeaders,
            ),
            requestHeaders: filterHeaders(
              requestHeaders,
              pushConfig.allowedRequestHeaders,
            ),
            cookie: pushConfig.forwardCookies
              ? (relayed.cookie ?? undefined)
              : undefined,
            origin: relayed.origin ?? undefined,
          },
        };

        // fetchFromAPIServer only reads ctx.mutateContext + ctx.auth?.raw.
        const ctx = {
          mutateContext,
          auth: relayed.auth
            ? {type: 'jwt', raw: relayed.auth, decoded: {}}
            : undefined,
          user: {id: relayed.userID ?? null},
          clientID: relayed.clientID,
        } as unknown as ConnectionContext;

        try {
          const response = await fetchFromAPIServer(
            mutateResponseSchema,
            'push',
            lc,
            ctx,
            {appID: shard.appID, shardNum: shard.shardNum},
            relayed.push as Parameters<typeof fetchFromAPIServer>[5],
            {operation: 'mutate'},
          );
          res.writeHead(200, {'content-type': 'application/json'});
          res.end(JSON.stringify(response));
        } catch (e) {
          // The mutation was not applied. The client's lastMutationID won't
          // advance, so it re-pushes on its next attempt.
          lc.warn?.(
            `push relay to ${relayed.userPushURL ?? pushURL} failed for client ${relayed.clientID}: ${String(e)}`,
          );
          // An upstream AUTH rejection must keep its real status: the rust
          // drainer's failConnection branch (the port of pusher.ts
          // `isAuthErrorBody(response)` → `failConnection`) keys on the relay
          // response being 401/403. Collapsing it to 502 left the dead-token
          // client retrying forever (2026-08-29 prod: backend 401 → relay 502
          // → 0 invalidations). Everything else stays 502 so the Rust syncer
          // logs it as a relay-hop failure.
          if (isProtocolError(e) && isAuthErrorBody(e.errorBody)) {
            const status =
              'status' in e.errorBody && typeof e.errorBody.status === 'number'
                ? e.errorBody.status
                : 401;
            return fail(status, `push forward failed: ${String(e)}`);
          }
          return fail(502, `push forward failed: ${String(e)}`);
        }
      })();
    });
    req.on('error', () => fail(400, 'request error'));
  });

  return new Promise(resolve => {
    // Loopback only: the endpoint is internal to the zero-cache node.
    server.listen(0, '127.0.0.1', () => {
      const port = (server.address() as AddressInfo).port;
      const url = `http://127.0.0.1:${port}/push`;
      lc.info?.(`rust push relay listening at ${url} → ${pushURL}`);
      resolve({server, url});
    });
  });
}
