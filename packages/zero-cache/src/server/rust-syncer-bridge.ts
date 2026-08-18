import {randomUUID} from 'node:crypto';
import net, {type Socket} from 'node:net';
import type {LogContext} from '@rocicorp/logger';
import {must} from '../../../shared/src/must.ts';
import {sleep} from '../../../shared/src/sleep.ts';
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
        issuer?: string | undefined;
        audience?: string | undefined;
        revalidateIntervalSeconds?: number | undefined;
      }
    | undefined;
  query?: RustSyncerFetchConfig | undefined;
  /** @deprecated Legacy name retained by normalized 1.7 configs. */
  getQueries?: RustSyncerFetchConfig | undefined;
  /**
   * Shadow-mode query-covering detection during hydration (zero-config
   * `enableQueryCovering`, default true). Log-only; forwarded so the Rust
   * syncer's coverage logging matches the TS syncer's.
   */
  enableQueryCovering?: boolean | undefined;
};

type RustSyncerFetchConfig = {
  url?: readonly string[] | undefined;
  apiKey?: string | undefined;
  allowedClientHeaders?: readonly string[] | undefined;
  allowedRequestHeaders?: readonly string[] | undefined;
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
  const {secret, jwk, jwksUrl, issuer, audience, revalidateIntervalSeconds} =
    config.auth ?? {};
  if (secret) {
    out.AUTH_SECRET = secret;
  }
  if (jwk) {
    out.AUTH_JWK = jwk;
  }
  if (jwksUrl) {
    out.AUTH_JWKS_URL = jwksUrl;
  }
  // iss/aud pinning and the revalidation cadence must reach rust, or JWT
  // validation silently degrades to signature+sub-only under the rust syncer
  // while the operator believes issuer/audience are enforced.
  if (issuer) {
    out.AUTH_ISSUER = issuer;
  }
  if (audience) {
    out.AUTH_AUDIENCE = audience;
  }
  if (revalidateIntervalSeconds !== undefined) {
    out.AUTH_REVALIDATE_INTERVAL_SECONDS = String(revalidateIntervalSeconds);
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
    if (query.allowedRequestHeaders) {
      out.QUERY_ALLOWED_REQUEST_HEADERS_JSON = JSON.stringify(
        query.allowedRequestHeaders,
      );
    }
    out.QUERY_FORWARD_COOKIES = String(query.forwardCookies ?? false);
  }
  // Only forward an explicit opt-out; Rust defaults the flag to true, matching
  // the zero-config default.
  if (config.enableQueryCovering === false) {
    out.ENABLE_QUERY_COVERING = 'false';
  }
  // Shared secret gating the rust /notify endpoints (see notifyAuthToken).
  out.NOTIFY_AUTH_TOKEN = notifyAuthToken;
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
 * Bounded retry schedule for a single `/notify` POST. The in-process TS
 * `Notifier` never permanently loses a `version-ready` — a busy subscriber
 * coalesces to the latest state but always eventually observes it. A single
 * fire-and-forget POST does not give that guarantee: a transient failure (the
 * rust HTTP server briefly unavailable across a restart / GC pause, a dropped
 * connection) would silently drop the notification, leaving the syncer stale
 * until the *next* commit happens to fire another POST. Retrying restores the
 * "eventually delivered" guarantee. Retries are delivery-safe: rust advances
 * to replica head idempotently, coalesces queued notifications per client
 * group, and drops a redelivered watermark it has already served (its
 * serving-lag replay guard). Exponential backoff: 50, 100, 200, 400ms.
 *
 * Each attempt carries a hard timeout: without one, a rust process that
 * accepts the TCP connection but never responds (wedged executor, full accept
 * queue) blocks this promise forever — and since `notifyRustSyncers` awaits
 * all ports before the subscription loop pulls the next state, one wedged
 * syncer would permanently stall version-ready fan-out to EVERY syncer.
 */
const NOTIFY_MAX_ATTEMPTS = 5;
const NOTIFY_RETRY_BASE_MS = 50;
const NOTIFY_ATTEMPT_TIMEOUT_MS = 5_000;

/**
 * Per-dispatcher shared secret for `/notify`. The rust HTTP port is bound on
 * all interfaces (its metrics endpoint is scraped externally), which would
 * otherwise leave `/notify` open to any reachable peer — one POST triggers a
 * full advance cycle on every hosted client group, and a forged
 * watermark/commit-time poisons the serving-lag histogram. The token is
 * generated once per dispatcher process, handed to each spawned rust-syncer
 * via `NOTIFY_AUTH_TOKEN`, and attached to every notify request.
 */
const notifyAuthToken = randomUUID();

async function notifyOne(
  lc: LogContext,
  port: number,
  body: string,
): Promise<void> {
  for (let attempt = 1; attempt <= NOTIFY_MAX_ATTEMPTS; attempt++) {
    try {
      const resp = await fetch(`http://127.0.0.1:${port}/notify`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-notify-auth': notifyAuthToken,
        },
        body,
        signal: AbortSignal.timeout(NOTIFY_ATTEMPT_TIMEOUT_MS),
      });
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      return;
    } catch (e) {
      if (attempt === NOTIFY_MAX_ATTEMPTS) {
        lc.warn?.(
          `failed to notify rust-syncer :${port} after ${attempt} attempts: ${String(e)}`,
        );
        return;
      }
      await sleep(NOTIFY_RETRY_BASE_MS * 2 ** (attempt - 1));
    }
  }
}

/**
 * POSTs a `version-ready` notification to each rust-syncer's HTTP `/notify`
 * endpoint. rust-syncer has no IPC channel, so replica commit notifications are
 * relayed over HTTP (the analog of the in-process `Subscription<ReplicaState>`
 * the TS syncer consumes). Each port is retried with bounded backoff (see
 * {@link notifyOne}) so a transient failure does not permanently drop the
 * notification — the cross-process analog of the TS Notifier's coalesce-to-
 * latest delivery guarantee. Failures are logged, not thrown — a single
 * unreachable syncer must not stall the others.
 */
export async function notifyRustSyncers(
  lc: LogContext,
  httpPorts: readonly number[],
  state?:
    | {
        watermark?: string | undefined;
        upstreamCommitTimeMs?: number | undefined;
      }
    | undefined,
): Promise<void> {
  // Carry the version-ready watermark + upstream commit time so the rust-syncer
  // can compute end-to-end serving lag (zero/v1.9.0 #6157). Omitted fields are
  // simply absent — the rust tracker ignores a notification missing either.
  const body = JSON.stringify({
    state: 'version-ready',
    ...(state?.watermark !== undefined ? {watermark: state.watermark} : {}),
    ...(state?.upstreamCommitTimeMs !== undefined
      ? {upstreamCommitTimeMs: state.upstreamCommitTimeMs}
      : {}),
  });
  await Promise.all(httpPorts.map(port => notifyOne(lc, port, body)));
}
