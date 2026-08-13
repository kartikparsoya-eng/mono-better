import net, {type Socket} from 'node:net';
import type {LogContext} from '@rocicorp/logger';
import {must} from '../../../shared/src/must.ts';
import type {IncomingMessageSubset} from '../types/http.ts';
import {getShardID} from '../types/shards.ts';
import {replicaFileName, type ReplicaFileMode} from '../workers/replicator.ts';

/**
 * The subset of the normalized zero-cache config that rust-syncer needs. TS is
 * the single source of truth for config resolution — rather than re-deriving
 * env vars in Rust (which would drift from `getNormalizedZeroConfig`), the
 * dispatcher resolves everything here and hands rust the concrete values under
 * the env names its `main.rs` reads.
 */
export type RustSyncerConfig = {
  replica: {file: string};
  cvr: {db?: string | undefined};
  upstream: {db?: string | undefined};
  taskID: string;
  app: {id: string};
  shard: {num: number};
  auth?:
    | {
        secret?: string | undefined;
        jwk?: string | undefined;
        jwksUrl?: string | undefined;
      }
    | undefined;
  query?: RustSyncerFetchConfig | undefined;
  /** @deprecated Legacy name retained by normalized 1.7 configs. */
  getQueries?: RustSyncerFetchConfig | undefined;
};

type RustSyncerFetchConfig = {
  url?: readonly string[] | undefined;
  apiKey?: string | undefined;
  allowedClientHeaders?: readonly string[] | undefined;
  forwardCookies?: boolean | undefined;
};

/**
 * Builds the environment for a spawned rust-syncer process from the normalized
 * TS config. Applies `replicaFileName(file, mode)` (so `serving-copy` resolves
 * to the `-serving-copy` file, matching the TS syncer worker) and maps the
 * resolved shard / cvr-db / auth values onto the env names `main.rs` reads.
 */
export function rustSyncerEnv(
  config: RustSyncerConfig,
  fileMode: ReplicaFileMode,
  wsPort: number,
  httpPort: number,
  cvrMaxConns: number,
): Record<string, string> {
  const shard = getShardID(config);
  const out: Record<string, string> = {
    PORT: String(wsPort),
    HTTP_PORT: String(httpPort),
    // `cvr.db` is already defaulted to `upstream.db` by `normalize`, but keep
    // the fallback defensive.
    CVR_PG_URI: must(
      config.cvr.db ?? config.upstream.db,
      'no cvr.db / upstream.db configured for rust-syncer',
    ),
    CVR_MAX_CONNS: String(cvrMaxConns),
    REPLICA_FILE: replicaFileName(config.replica.file, fileMode),
    TASK_ID: config.taskID,
    ZERO_APP_ID: shard.appID,
    SHARD: String(shard.shardNum),
  };
  const {secret, jwk, jwksUrl} = config.auth ?? {};
  if (secret) {
    out.AUTH_SECRET = secret;
  }
  if (jwk) {
    out.AUTH_JWK = jwk;
  }
  if (jwksUrl) {
    out.AUTH_JWKS_URL = jwksUrl;
  }
  // Match syncer.ts#getCustomQueryConfig: the modern `query` URL wins, with
  // `getQueries` retained as the 1.7 compatibility fallback. Serialize arrays
  // explicitly so Rust receives the normalized TS values rather than reparsing
  // the original process environment.
  const query = config.query?.url ? config.query : config.getQueries;
  if (query?.url?.length) {
    out.QUERY_URLS_JSON = JSON.stringify(query.url);
    if (query.apiKey) {
      out.QUERY_API_KEY = query.apiKey;
    }
    if (query.allowedClientHeaders) {
      out.QUERY_ALLOWED_CLIENT_HEADERS_JSON = JSON.stringify(
        query.allowedClientHeaders,
      );
    }
    out.QUERY_FORWARD_COOKIES = String(query.forwardCookies ?? false);
  }
  return out;
}

/**
 * The handoff tuple the dispatcher passes to a worker's `send(msg, socket)`:
 * `['handoff', {message, head, payload}]`. rust-syncer only needs the request
 * `message` (to rebuild the upgrade) and any buffered `head` bytes.
 */
export type UpgradeHandoff = readonly [
  unknown,
  {message: IncomingMessageSubset; head: ArrayBuffer},
];

/**
 * Rebuilds the raw HTTP upgrade request bytes from a serialized
 * {@link IncomingMessageSubset}. Uses `rawHeaders` (the flat `[k, v, k, v, …]`
 * array) so header casing and duplicates are preserved exactly — important for
 * the WebSocket handshake (`Sec-WebSocket-Key`, etc.).
 */
export function rebuildUpgradeRequest(message: IncomingMessageSubset): string {
  const method = message.method ?? 'GET';
  const httpVersion = message.httpVersion ?? '1.1';
  const raw = message.rawHeaders ?? [];
  let out = `${method} ${message.url ?? '/'} HTTP/${httpVersion}\r\n`;
  for (let i = 0; i + 1 < raw.length; i += 2) {
    out += `${raw[i]}: ${raw[i + 1]}\r\n`;
  }
  out += '\r\n';
  return out;
}

/**
 * Reverse-proxies a raw WebSocket upgrade socket (received by the dispatcher
 * from its parent) to a rust-syncer process listening on `127.0.0.1:<wsPort>`.
 *
 * The dispatcher's handoff normally passes the socket fd to a sibling Node
 * worker over IPC. rust-syncer is a separate process, so instead we open a TCP
 * connection to its WS port, replay the original HTTP upgrade request, forward
 * any buffered `head` bytes, then pipe the two sockets together. The upgrade
 * handshake is completed by rust-syncer's own WebSocket server, not here.
 */
export function proxyUpgradeToRust(
  lc: LogContext,
  handoff: UpgradeHandoff,
  clientSocket: Socket,
  wsPort: number,
): void {
  const [, {message, head}] = handoff;
  const upstream = net.connect(wsPort, '127.0.0.1');

  let cleanedUp = false;
  const cleanup = (err?: unknown) => {
    if (cleanedUp) {
      return;
    }
    cleanedUp = true;
    if (err) {
      lc.warn?.(`rust-syncer upgrade proxy (:${wsPort}) error: ${String(err)}`);
    }
    clientSocket.destroy();
    upstream.destroy();
  };

  upstream.on('connect', () => {
    upstream.write(rebuildUpgradeRequest(message));
    if (head && head.byteLength > 0) {
      upstream.write(Buffer.from(head));
    }
    // Bidirectional pipe. The client socket was paused since the `upgrade`
    // event, so any buffered frames are flushed by `pipe()`.
    clientSocket.pipe(upstream);
    upstream.pipe(clientSocket);
  });

  upstream.on('error', cleanup);
  clientSocket.on('error', cleanup);
  upstream.on('close', () => cleanup());
  clientSocket.on('close', () => cleanup());
}

/**
 * POSTs a `version-ready` notification to each rust-syncer's HTTP `/notify`
 * endpoint. rust-syncer has no IPC channel, so replica commit notifications are
 * relayed over HTTP (the analog of the in-process `Subscription<ReplicaState>`
 * the TS syncer consumes). Failures are logged, not thrown — a single
 * unreachable syncer must not stall the others.
 */
export async function notifyRustSyncers(
  lc: LogContext,
  httpPorts: readonly number[],
): Promise<void> {
  await Promise.all(
    httpPorts.map(port =>
      fetch(`http://127.0.0.1:${port}/notify`, {
        method: 'POST',
        headers: {'content-type': 'application/json'},
        body: JSON.stringify({state: 'version-ready'}),
      }).catch(e =>
        lc.warn?.(`failed to notify rust-syncer :${port}: ${String(e)}`),
      ),
    ),
  );
}
