import type {LogContext} from '@rocicorp/logger';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import type {JSONObject} from '../../../../shared/src/bigint-json.ts';
import {
  assertJSONValue,
  type JSONObject as SafeJSONObject,
} from '../../../../shared/src/json.ts';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import * as v from '../../../../shared/src/valita.ts';
import type {Writable} from '../../../../shared/src/writable.ts';
import type {ErroredQuery} from '../../../../zero-protocol/src/custom-queries.ts';
import {rowSchema} from '../../../../zero-protocol/src/data.ts';
import type {DeleteClientsBody} from '../../../../zero-protocol/src/delete-clients.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {
  ProtocolError,
  type TransformFailedBody,
} from '../../../../zero-protocol/src/error.ts';
import type {InspectDownBody} from '../../../../zero-protocol/src/inspect-down.ts';
import {mutationResultSchema} from '../../../../zero-protocol/src/mutation.ts';
import type {
  PokePartBody,
  PokeStartBody,
} from '../../../../zero-protocol/src/poke.ts';
import {primaryKeyValueRecordSchema} from '../../../../zero-protocol/src/primary-key.ts';
import type {RowPatchOp} from '../../../../zero-protocol/src/row-patch.ts';
import {
  getOrCreateCounter,
  getOrCreateLatencyHistogram,
} from '../../observability/metrics.ts';
import {
  getLogLevel,
  wrapWithProtocolError,
} from '../../types/error-with-level.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import type {Subscription} from '../../types/subscription.ts';
import {getRustCvrAddon, isRustCvrEnabled} from './rust-cvr-addon.ts';
import {
  cmpVersions,
  cookieToVersion,
  versionToCookie,
  versionToNullableCookie,
  type CVRVersion,
  type DelQueryPatch,
  type NullableCVRVersion,
  type PutQueryPatch,
  type RowID,
} from './schema/types.ts';

interface RustPokeHandlerHandle {
  addPatch(patch: unknown): Promise<void>;
  cancel(): Promise<void>;
  end(finalVersion: unknown): Promise<void>;
}

interface RustClientHandlerHandle {
  version(): Promise<unknown>;
  fail(e: string): void;
  close(reason: string): void;
  startPoke(tentativeVersion: unknown): RustPokeHandlerHandle;
  sendDeleteClients(
    clientIDs: string[],
    clientGroupIDs: string[],
  ): Promise<void>;
  sendQueryTransformApplicationErrors(errors: unknown[]): Promise<void>;
  sendInspectResponse(response: unknown): void;
}

export type PutRowPatch = {
  type: 'row';
  op: 'put';
  id: RowID;
  contents: JSONObject;
};

export type DeleteRowPatch = {
  type: 'row';
  op: 'del';
  id: RowID;
};

export type RowPatch = PutRowPatch | DeleteRowPatch;
export type ConfigPatch = DelQueryPatch | PutQueryPatch;

export type Patch = ConfigPatch | RowPatch;

export type PatchToVersion = {
  patch: Patch;
  toVersion: CVRVersion;
};

export interface PokeHandler {
  addPatch(patch: PatchToVersion): Promise<void>;
  cancel(): Promise<void>;
  end(finalVersion: CVRVersion): Promise<void>;
}

const NOOP: PokeHandler = {
  addPatch: () => promiseVoid,
  cancel: () => promiseVoid,
  end: () => promiseVoid,
};

/** Wraps PokeHandlers for multiple clients in a single PokeHandler. */
export function startPoke(
  clients: ClientHandler[],
  tentativeVersion: CVRVersion,
): PokeHandler {
  const pokers = clients.map(c => c.startPoke(tentativeVersion));

  // Promise.allSettled() ensures that a failed (e.g. disconnected) client
  // does not prevent other clients from receiving the pokes. However, the
  // rate (per client group) will be limited by the slowest connection.
  return {
    addPatch: async patch => {
      await Promise.allSettled(pokers.map(poker => poker.addPatch(patch)));
    },
    cancel: async () => {
      await Promise.allSettled(pokers.map(poker => poker.cancel()));
    },
    end: async finalVersion => {
      await Promise.allSettled(pokers.map(poker => poker.end(finalVersion)));
    },
  };
}

// Semi-arbitrary threshold at which poke body parts are flushed.
// When row size is being computed, that should be used as a threshold instead.
const PART_COUNT_FLUSH_THRESHOLD = 100;

// Upper bound on how long a single downstream push may remain unconsumed
// before the connection is failed (closed) as unrecoverably slow.
//
// The poke path awaits each push's `result`, which resolves only when the
// outbound ws pipeline consumes the message. A stalled-but-open socket — a
// silently dead peer before the kernel's TCP timeout (~15-25 min), or a
// suspended/backgrounded tab whose kernel keeps ACKing into a zero-window
// (which never times out) — leaves that await pending indefinitely. Because
// pokes run inside the view-syncer lock (`#advancePipelines` →
// `pokers.addPatch`/`end`, and initConnection catchup), ONE such client
// freezes advances for the entire client group: the snapshotter's pinned
// read-marks stop moving, wal2 checkpointing is starved behind them, and the
// replica WAL grows at the write rate, unbounded. The server sends pongs but
// enforces NO inbound liveness on client sockets, and the advancement-timeout
// breaker only runs while rows are flowing — nothing else bounds this stall.
//
// Failing the one stalled connection settles every pending push (Subscription
// cleanup resolves them 'unconsumed'), releases the poke chain (#pokeTail),
// and lets the rest of the client group advance; the client reconnects and
// catches up normally.
const PUSH_CONSUME_TIMEOUT_MS = (() => {
  const v = Number(process.env['ZERO_PUSH_CONSUME_TIMEOUT_MS']);
  return Number.isFinite(v) && v > 0 ? v : 60_000;
})();

/**
 * Handles a single `ViewSyncer` connection.
 */
export class ClientHandler {
  readonly #clientGroupID: string;
  readonly clientID: string;
  readonly wsID: string;
  readonly #zeroClientsTable: string;
  readonly #zeroMutationsTable: string;
  readonly #lc: LogContext;
  readonly #downstream: Subscription<Downstream>;
  #baseVersion: NullableCVRVersion;
  // Tail of the per-connection poke chain. Each poke transaction gates its
  // first frame on the previous transaction's completion so that pokes to this
  // connection never interleave. See startPoke() for why.
  #pokeTail: Promise<void> = Promise.resolve();
  readonly #rust: RustClientHandlerHandle | null = null;

  readonly #pokeTime = getOrCreateLatencyHistogram(
    'sync',
    'poke.time',
    'Time elapsed for each poke transaction. Canceled / noop pokes are excluded.',
  );

  readonly #pokeTransactions = getOrCreateCounter(
    'sync',
    'poke.transactions',
    'Count of poke transactions.',
  );

  readonly #pokedRows = getOrCreateCounter(
    'sync',
    'poke.rows',
    'Count of poked rows.',
  );

  constructor(
    lc: LogContext,
    clientGroupID: string,
    clientID: string,
    wsID: string,
    shard: ShardID,
    baseCookie: string | null,
    downstream: Subscription<Downstream>,
  ) {
    lc.debug?.('new client handler');
    this.#clientGroupID = clientGroupID;
    this.clientID = clientID;
    this.wsID = wsID;
    this.#zeroClientsTable = `${upstreamSchema(shard)}.clients`;
    this.#zeroMutationsTable = `${upstreamSchema(shard)}.mutations`;
    this.#lc = lc;
    this.#downstream = downstream;
    this.#baseVersion = cookieToVersion(baseCookie);

    if (isRustCvrEnabled()) {
      const addon = getRustCvrAddon<Record<string, unknown>>();
      const RustClientHandlerHandle = addon?.ClientHandlerHandle as
        | (new (
            clientGroupID: string,
            clientID: string,
            wsID: string,
            shard: unknown,
            baseCookie: string | null,
            pushFn: (msg: unknown) => void,
            failFn: (err: string) => void,
            cancelFn: () => void,
          ) => RustClientHandlerHandle)
        | undefined;
      if (RustClientHandlerHandle) {
        this.#rust = new RustClientHandlerHandle(
          clientGroupID,
          clientID,
          wsID,
          shard,
          baseCookie,
          (msg: unknown) => {
            // Fire-and-forget push to WS
            const {result} = downstream.push(msg as Downstream);
            result.catch(() => {});
          },
          (err: string) => {
            lc.error?.(`rust client handler error: ${err}`);
            downstream.fail(wrapWithProtocolError(new Error(err)));
          },
          () => downstream.cancel(),
        );
      }
    }
  }

  version(): NullableCVRVersion {
    if (this.#rust) {
      // Rust version() is async but TS version() is sync.
      // Return the cached baseVersion — it's updated after each poke end().
      return this.#baseVersion;
    }
    return this.#baseVersion;
  }

  async #push(msg: Downstream): Promise<void> {
    const {result} = this.#downstream.push(msg);
    // Bound the wait (see PUSH_CONSUME_TIMEOUT_MS): a stalled-but-open
    // connection must fail rather than hold the view-syncer lock forever.
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<'push-timeout'>(resolve => {
      timer = setTimeout(resolve, PUSH_CONSUME_TIMEOUT_MS, 'push-timeout');
      // Don't let a pending poke keep the process alive.
      timer.unref?.();
    });
    try {
      const won = await Promise.race([result, timeout]);
      if (won === 'push-timeout') {
        this.fail(
          new Error(
            `client not consuming pokes for ${PUSH_CONSUME_TIMEOUT_MS}ms ` +
              `(stalled connection); closing to unblock the client group`,
          ),
        );
      }
    } finally {
      clearTimeout(timer);
    }
  }

  fail(e: unknown) {
    if (this.#rust) {
      this.#rust.fail(String(e));
      return;
    }
    this.#lc[getLogLevel(e)]?.(
      `view-syncer closing connection with error: ${String(e)}`,
      e,
    );
    this.#downstream.fail(wrapWithProtocolError(e));
  }

  close(reason: string) {
    if (this.#rust) {
      this.#rust.close(reason);
      return;
    }
    this.#lc.debug?.(`view-syncer closing connection: ${reason}`);
    this.#downstream.cancel();
  }

  startPoke(tentativeVersion: CVRVersion): PokeHandler {
    if (this.#rust) {
      const rust = this.#rust;
      const rustPoke = rust.startPoke(tentativeVersion);
      return {
        addPatch: async patch => {
          await rustPoke.addPatch(patch);
        },
        cancel: async () => {
          await rustPoke.cancel();
        },
        end: async finalVersion => {
          await rustPoke.end(finalVersion);
          this.#baseVersion = finalVersion;
        },
      };
    }
    const pokeID = versionToCookie(tentativeVersion);
    const lc = this.#lc.withContext('pokeID', pokeID);

    if (cmpVersions(this.#baseVersion, tentativeVersion) >= 0) {
      lc.info?.(`already caught up, not sending poke.`);
      return NOOP;
    }

    const baseCookie = versionToNullableCookie(this.#baseVersion);
    const cookie = versionToCookie(tentativeVersion);
    lc.debug?.(`starting poke from ${baseCookie} to ${cookie}`);

    const start = performance.now();

    const pokeStart: PokeStartBody = {pokeID, baseCookie};

    // Serialize poke transactions to this connection. The client
    // (zero-poke-handler.ts) permits only ONE in-flight poke: a `pokeStart`
    // arriving while another poke is still streaming makes it clear its state
    // and reconnect (surfaced to users as "Connection Lost"). Stock TS upheld
    // this implicitly because hydration was a *synchronous* generator, so a
    // poke opened and closed without yielding. The rust-ivm driver streams
    // rows across async TSFN macrotask boundaries
    // (rust-ivm-driver.ts#addQueryStreaming), so a following poke's frames can
    // otherwise interleave with a hydrate poke still draining. Gate this poke's
    // first frame on the previous poke's completion, and chain the tail so the
    // next poke waits for us — even if we send nothing.
    const priorPoke = this.#pokeTail;
    let releasePoke!: () => void;
    const pokeDone = new Promise<void>(resolve => (releasePoke = resolve));
    this.#pokeTail = priorPoke.then(() => pokeDone);
    let pokeReleased = false;
    const endPoke = () => {
      if (!pokeReleased) {
        pokeReleased = true;
        releasePoke();
      }
    };
    let awaitedPrior = false;
    const awaitPrior = async () => {
      if (!awaitedPrior) {
        awaitedPrior = true;
        await priorPoke;
      }
    };

    let pokeStarted = false;
    let body: PokePartBody | undefined;
    let partCount = 0;
    const ensureBody = async () => {
      if (!pokeStarted) {
        await awaitPrior();
        await this.#push(['pokeStart', pokeStart]);
        pokeStarted = true;
      }
      return (body ??= {pokeID});
    };
    const flushBody = async () => {
      if (body) {
        await this.#push(['pokePart', body]);
        body = undefined;
        partCount = 0;
      }
    };

    const addPatch = async (patchToVersion: PatchToVersion) => {
      const {patch, toVersion} = patchToVersion;
      if (cmpVersions(toVersion, this.#baseVersion) <= 0) {
        return;
      }
      const body = await ensureBody();

      const {type, op} = patch;
      switch (type) {
        case 'query': {
          const patches = patch.clientID
            ? ((body.desiredQueriesPatches ??= {})[patch.clientID] ??= [])
            : (body.gotQueriesPatch ??= []);
          if (op === 'put') {
            patches.push({op, hash: patch.id});
          } else {
            patches.push({op, hash: patch.id});
          }
          break;
        }
        case 'row':
          if (patch.id.table === this.#zeroClientsTable) {
            this.#updateLMIDs((body.lastMutationIDChanges ??= {}), patch);
          } else if (patch.id.table === this.#zeroMutationsTable) {
            const patches = (body.mutationsPatch ??= []);
            if (op === 'put') {
              const row = v.parse(
                normalizeMutationResult(ensureSafeJSON(patch.contents)),
                mutationRowSchema,
                'passthrough',
              );
              patches.push({
                op: 'put',
                mutation: {
                  id: {
                    clientID: row.clientID,
                    id: row.mutationID,
                  },
                  result: row.result,
                },
              });
            } else {
              const {clientID, mutationID} = patch.id.rowKey;
              assert(
                typeof clientID === 'string',
                'client id must be a string',
              );
              const id = Number(mutationID);
              assert(
                !Number.isNaN(id) && Number.isFinite(id) && id >= 0,
                'mutation id must be a finite number',
              );
              patches.push({
                op: 'del',
                id: {
                  clientID,
                  id,
                },
              });
            }
          } else {
            (body.rowsPatch ??= []).push(makeRowPatch(patch));
          }
          break;
        default:
          unreachable(patch);
      }

      if (++partCount >= PART_COUNT_FLUSH_THRESHOLD) {
        await flushBody();
      }
    };

    return {
      addPatch: async (patchToVersion: PatchToVersion) => {
        try {
          await addPatch(patchToVersion);
          if (patchToVersion.patch.type === 'row') {
            this.#pokedRows.add(1);
          }
        } catch (e) {
          this.#downstream.fail(wrapWithProtocolError(e));
        }
      },

      cancel: async () => {
        try {
          if (pokeStarted) {
            await this.#push(['pokeEnd', {pokeID, cookie: '', cancel: true}]);
          }
        } finally {
          endPoke();
        }
      },

      end: async (finalVersion: CVRVersion) => {
        try {
          const cookie = versionToCookie(finalVersion);
          if (!pokeStarted) {
            if (cmpVersions(this.#baseVersion, finalVersion) === 0) {
              return; // Nothing changed and nothing was sent.
            }
            await awaitPrior();
            await this.#push(['pokeStart', pokeStart]);
          } else if (cmpVersions(this.#baseVersion, finalVersion) >= 0) {
            // Sanity check: If the poke was started, the finalVersion
            // must be > #baseVersion.
            throw new Error(
              `Patches were sent but finalVersion ${finalVersion} is ` +
                `not greater than baseVersion ${this.#baseVersion}`,
            );
          }
          await flushBody();
          await this.#push(['pokeEnd', {pokeID, cookie}]);
          this.#baseVersion = finalVersion;

          const elapsed = performance.now() - start;
          this.#pokeTransactions.add(1);
          this.#pokeTime.recordMs(elapsed);
        } finally {
          endPoke();
        }
      },
    };
  }

  async sendDeleteClients(
    lc: LogContext,
    deletedClientIDs: string[],
    deletedClientGroupIDs: string[],
  ) {
    if (this.#rust) {
      await this.#rust.sendDeleteClients(
        deletedClientIDs,
        deletedClientGroupIDs,
      );
      return;
    }
    const deleteClientsBody: Writable<DeleteClientsBody> = {};
    if (deletedClientIDs.length > 0) {
      deleteClientsBody.clientIDs = deletedClientIDs;
    }
    if (deletedClientGroupIDs.length > 0) {
      deleteClientsBody.clientGroupIDs = deletedClientGroupIDs;
    }
    lc.debug?.('sending deleteClients', deleteClientsBody);
    await this.#push(['deleteClients', deleteClientsBody]);
  }

  sendQueryTransformApplicationErrors(errors: ErroredQuery[]) {
    if (this.#rust) {
      void this.#rust.sendQueryTransformApplicationErrors(errors);
      return;
    }
    void this.#push(['transformError', errors]);
  }

  sendQueryTransformFailedError(error: TransformFailedBody) {
    this.fail(new ProtocolError(error));
  }

  sendInspectResponse(lc: LogContext, response: InspectDownBody): void {
    if (this.#rust) {
      this.#rust.sendInspectResponse(response);
      return;
    }
    lc.debug?.('sending inspect response', response);
    this.#downstream.push(['inspect', response]);
  }

  #updateLMIDs(lmids: Record<string, number>, patch: RowPatch) {
    if (patch.op === 'put') {
      const row = ensureSafeJSON(patch.contents);
      const {clientGroupID, clientID, lastMutationID} = v.parse(
        row,
        lmidRowSchema,
        'passthrough',
      );
      if (clientGroupID !== this.#clientGroupID) {
        this.#lc.error?.(
          `Received clients row for wrong clientGroupID. Ignoring.`,
          clientGroupID,
        );
      } else {
        lmids[clientID] = lastMutationID;
      }
    } else {
      // The 'constrain' and 'del' ops for clients can be ignored.
      patch.op satisfies 'constrain' | 'del';
    }
  }
}

// Note: The {APP_ID}_{SHARD_ID}.clients table is set up in replicator/initial-sync.ts.
const lmidRowSchema = v.object({
  clientGroupID: v.string(),
  clientID: v.string(),
  lastMutationID: v.number(), // Actually returned as a bigint, but converted by ensureSafeJSON().
});

const mutationRowSchema = v.object({
  clientGroupID: v.string(),
  clientID: v.string(),
  mutationID: v.number(),
  result: mutationResultSchema,
});

/**
 * Defense-in-depth: the `{app}_{shard}.mutations.result` column is Postgres
 * type JSON, stored in the SQLite replica as stringified text, and must be
 * re-parsed to an OBJECT on read (see zqlite `fromSQLiteTypes` →
 * `case 'json': JSON.parse(v)`). `mutationRowSchema` REQUIRES `result` to be an
 * object and never parses it itself. If the engine ever emits `result` as a
 * JSON string (an encoding slip in the source column typing), `v.parse` below
 * would throw a fatal `ProtocolError` that tears down the WebSocket connection
 * — even for a LAWFUL failed-mutation result (e.g. a `MutationACLError` app
 * error). A lawful app error must NEVER fatal the connection. So mirror
 * `fromSQLiteTypes` here: if `result` arrives as a string, JSON.parse it back
 * to an object before validation, degrading a slip to a normal failed mutation.
 */
function normalizeMutationResult(row: SafeJSONObject): SafeJSONObject {
  const {result} = row;
  if (typeof result !== 'string') {
    return row;
  }
  try {
    return {...row, result: JSON.parse(result)};
  } catch {
    // Not valid JSON — leave as-is and let v.parse produce its normal error.
    return row;
  }
}

export function makeRowPatch(patch: RowPatch): RowPatchOp {
  const {
    op,
    id: {table: tableName, rowKey: id},
  } = patch;

  switch (op) {
    case 'put':
      return {
        op: 'put',
        tableName,
        value: v.parse(ensureSafeJSON(patch.contents), rowSchema),
      };

    case 'del':
      return {
        op,
        tableName,
        id: v.parse(id, primaryKeyValueRecordSchema),
      };

    default:
      unreachable(op);
  }
}

/**
 * Column values of type INT8 are returned as the `bigint` from the
 * Postgres library. These are converted to `number` if they are within
 * the safe Number range, allowing the protocol to support numbers larger
 * than 32-bits. Values outside of the safe number range (e.g. > 2^53) will
 * result in an Error.
 */
export function ensureSafeJSON(row: JSONObject): SafeJSONObject {
  const modified = Object.entries(row)
    .filter(([k, v]) => {
      if (typeof v === 'bigint') {
        if (v >= Number.MIN_SAFE_INTEGER && v <= Number.MAX_SAFE_INTEGER) {
          return true; // send this entry onto the next map() step.
        }
        throw new Error(`Value of "${k}" exceeds safe Number range (${v})`);
      } else if (typeof v === 'object') {
        assertJSONValue(v);
      }
      return false;
    })
    .map(([k, v]) => [k, Number(v)]);

  return modified.length
    ? {...row, ...Object.fromEntries(modified)}
    : (row as SafeJSONObject);
}
