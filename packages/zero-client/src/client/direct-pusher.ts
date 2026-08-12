import type {PushBody} from '../../../zero-protocol/src/push.ts';

/**
 * Direct mutation pushing.
 *
 * By default the client tunnels mutations to zero-cache over the sync
 * WebSocket, and zero-cache relays them to the app's mutate endpoint. When
 * direct mutations are enabled (and the server advertises its `appID`/`shardNum`
 * in the `connected` message) the client POSTs mutations to the mutate endpoint
 * itself, byte-for-byte identically to how zero-cache would relay them — same
 * `PushBody`, same `?schema=…&appID=…` query params, same `Authorization`
 * header. This keeps the (read-only) view-syncer entirely out of the write path;
 * mutation results still return to the client through the normal sync path
 * (the `mutationResults`/lmid queries poked down and applied to the local store).
 *
 * The app's mutate endpoint therefore receives an identical request whether it
 * was relayed by zero-cache or sent directly, so no app backend change is
 * required. (The browser now makes a cross-origin request, so the endpoint must
 * allow CORS; and it authenticates the caller with the user's JWT rather than a
 * zero-cache API key.)
 */

/** The server identity advertised in the `connected` message. */
export type MutateServerInfo = {
  readonly appID: string;
  readonly shardNum: number;
};

const RESERVED_QUERY_PARAMS = ['schema', 'appID'] as const;

/**
 * Builds the mutate-endpoint URL, appending `schema` (`{appID}_{shardNum}`) and
 * `appID` query params exactly as zero-cache's `fetchFromAPIServer` does, while
 * preserving any query params already present on `mutateURL`. A relative
 * `mutateURL` is resolved against `base` (the app origin).
 */
export function buildMutateURL(
  mutateURL: string,
  server: MutateServerInfo,
  base?: string | undefined,
): string {
  const url = new URL(mutateURL, base);
  for (const reserved of RESERVED_QUERY_PARAMS) {
    if (url.searchParams.has(reserved)) {
      throw new Error(
        `mutateURL cannot contain the reserved query param "${reserved}"`,
      );
    }
  }
  url.searchParams.append('schema', `${server.appID}_${server.shardNum}`);
  url.searchParams.append('appID', server.appID);
  return url.toString();
}

/**
 * Builds the request headers for a direct push: JSON content type, the user's
 * bearer token (when present), and any app-configured custom headers.
 */
export function buildMutateHeaders(
  auth: string | undefined,
  customHeaders: Readonly<Record<string, string>> | undefined,
): Record<string, string> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...customHeaders,
  };
  if (auth) {
    headers['Authorization'] = `Bearer ${auth}`;
  }
  return headers;
}

/** The outcome of a direct push POST. */
export type DirectPushResult = {
  /** The HTTP status code (0 if the request never completed). */
  readonly httpStatusCode: number;
  /** An error message when the request failed to complete, else ''. */
  readonly errorMessage: string;
  /** The parsed JSON response body, when the response was JSON. */
  readonly body: unknown;
};

/**
 * POSTs a {@link PushBody} to the mutate endpoint. Never throws — a transport
 * failure is reported as `httpStatusCode: 0` with an `errorMessage`, so the
 * caller (a Replicache `Pusher`) can surface it as a retryable push failure.
 */
export async function postDirectPush(
  url: string,
  headers: Record<string, string>,
  body: PushBody,
  fetchFn: typeof fetch = fetch,
): Promise<DirectPushResult> {
  let response: Response;
  try {
    response = await fetchFn(url, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
    });
  } catch (e) {
    return {
      httpStatusCode: 0,
      errorMessage: e instanceof Error ? e.message : String(e),
      body: undefined,
    };
  }

  let parsed: unknown;
  try {
    parsed = await response.json();
  } catch {
    parsed = undefined;
  }
  return {
    httpStatusCode: response.status,
    errorMessage: response.ok ? '' : `HTTP ${response.status}`,
    body: parsed,
  };
}
