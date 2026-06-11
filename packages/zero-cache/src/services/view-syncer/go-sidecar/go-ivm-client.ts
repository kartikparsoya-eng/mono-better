// MessagePack-RPC client for the Go IVM sidecar over a Unix socket.
// Wire format: 4-byte big-endian length prefix, then a MessagePack payload.
// Each clientGroupID maps to its own Engine in the sidecar; groups run in
// parallel and the client multiplexes a single socket across them.

import {createConnection, type Socket} from 'net';
import {Packr, Unpackr} from 'msgpackr';
import {trace} from '@opentelemetry/api';

/**
 * Build a W3C traceparent string from the currently-active OTel span, or
 * undefined if none. Failures are silent — tracing is best-effort.
 */
function getActiveTraceparent(): string | undefined {
  try {
    const span = trace.getActiveSpan();
    if (!span) return undefined;
    const ctx = span.spanContext();
    if (!ctx.traceId || !ctx.spanId) return undefined;
    // version-format: 00-{traceId}-{spanId}-{traceFlags hex}
    const flags = (ctx.traceFlags & 0xff).toString(16).padStart(2, '0');
    return `00-${ctx.traceId}-${ctx.spanId}-${flags}`;
  } catch {
    return undefined;
  }
}

// msgpackr emits several JS-specific extensions by default that Go's
// vmihailenco/msgpack decoder cannot understand. We opt out of all of them.
const codec = {
  packr: new Packr({
    useRecords: false,
    encodeUndefinedAsNil: true,
    mapsAsObjects: true,
    useBigIntExtension: false,
  }),
  unpackr: new Unpackr({
    useRecords: false,
    mapsAsObjects: true,
    useBigIntExtension: false,
  }),
};
const pack = (v: unknown) => codec.packr.pack(v);
const unpack = (buf: Buffer | Uint8Array) => codec.unpackr.unpack(buf as Buffer);

// --- Types ---

export type ColumnSchema = {
  type: 'boolean' | 'number' | 'string' | 'null' | 'json';
  optional?: boolean;
};

export type TableSchema = {
  columns: Record<string, ColumnSchema>;
  primaryKey: string[];
  /**
   * All column sets that have a unique index on this table (includes
   * primary key). Forwarded to the Go scalar-subquery resolver so it can
   * detect subqueries returning at most one row. Optional for backward
   * compat — when omitted, the Go resolver treats the table as having no
   * known unique keys and leaves EXISTS rewrites unmodified.
   */
  uniqueKeys?: string[][] | undefined;
};

export type TableData = {
  columns: Record<string, ColumnSchema>;
  primaryKey: string[];
  uniqueKeys?: string[][] | undefined;
  /**
   * Table's minRowVersion (liteTableSpec.minRowVersion), set after a RESET
   * during incremental catchup. Forwarded so the Go streamer can bump an
   * emitted row's _0_version up to it when below — port of streamNodes
   * (pipeline-driver.ts:3172-3178). Omitted/undefined means no bump.
   */
  minRowVersion?: string | null | undefined;
  rows: Record<string, unknown>[];
};

export type InitParams = {
  dbPath?: string;
  storagePath?: string;
  tables: Record<string, TableData>;
};

export type SnapshotChange = {
  table: string;
  prevValues: Record<string, unknown>[];
  nextValue: Record<string, unknown> | null;
};

/**
 * A SnapshotChange the Go sidecar DERIVED itself (advanceToHead), carrying the
 * rowKey TS uses to align it with its own diff. Mirrors snapshotChangeWire on
 * the Go side (cmd/sidecar/advance_to_head.go).
 */
export type DerivedSnapshotChange = SnapshotChange & {
  rowKey: Record<string, unknown>;
};

/**
 * Result of the advanceToHead RPC: the Go-derived diff + Go's new stateVersion.
 * `reset` is set when the diff aborted on a reset/truncate/permissions-change,
 * in which case `changes` is empty and the caller re-hydrates at `version`.
 */
export type AdvanceToHeadResult = {
  // P1 (derive-only): the Go-derived diff for the TS-vs-Go shadow compare.
  changes: DerivedSnapshotChange[];
  version: string;
  numChanges: number;
  // P2 (drive): the engine RowChanges produced by applying Go's OWN derived
  // diff to Go's engine (frame-coordinated). Empty in derive-only mode.
  rowChanges: RowChange[];
  timings?: TableTiming[] | undefined;
  reset?: {reason: string; msg: string} | undefined;
};

export type RowChange = {
  type: 0 | 1 | 2; // add, remove, edit
  queryID: string;
  table: string;
  rowKey: Record<string, unknown>;
  row: Record<string, unknown> | null;
};

export type CallOptions = {
  /** Per-call timeout in ms. 0 or Infinity disables. Default DEFAULT_TIMEOUT_MS. */
  timeoutMs?: number;
  /**
   * Optional clientGroupID for per-group fairness in the in-flight semaphore.
   * Without this, a single group flooding requests starves all other groups
   * sharing the same `GoIVMClient`. REVIEW-final CRITICAL-CROSS-2.
   */
  clientGroupID?: string;
  /**
   * Streaming callback. When set, intermediate response frames for this
   * id route here; the call promise resolves on the final terminal frame.
   * Used by `addQueriesStream`.
   */
  onPartial?: (value: unknown) => void;
};

/** Per-(table, op) timing reported by Go for an advance call. */
export type TableTiming = {
  table: string;
  /** ivm.ChangeType: 0=add, 1=remove, 2=edit. */
  type: number;
  ms: number;
};

/** Hydrate result, with optional per-query wall-time. */
export type HydrateResult = {
  changes: RowChange[];
  timingMs: number | undefined;
};

/** Advance result, with optional per-(table, op) wall-times. */
export type AdvanceResult = {
  changes: RowChange[];
  timings: TableTiming[] | undefined;
};

// --- Streaming accumulators ---
//
// Both `addQueriesStream` and `advanceStream` receive partial frames from
// Go that need to be reassembled into the same result shape their
// non-streaming counterparts return. These factory functions own the
// reassembly logic so it can be unit-tested in isolation — without
// having to spin up a real Unix socket and fake sidecar.
//
// Defensive invariants enforced here (all throw on violation):
//   - ChunkIndex arrives strictly in monotonic order (0, 1, 2, ...) — a
//     gap or duplicate means a wire-level reordering bug.
//   - Every queryID (hydrate) / every call (advance) ends with Final=true
//     before the terminal "done" — a missing Final means Go finished
//     without signaling completion for that query/call.

/**
 * Accumulator for `addQueriesStream` partial frames. Reassembles per
 * `queryID` and invokes `onResult` exactly once per query, on the frame
 * carrying `final=true`. After the streaming RPC resolves, call
 * `finish()` to verify no queryID is left orphaned (Go sent "done"
 * without a final for it).
 *
 * Returned object is stateful — do not reuse across calls.
 */
export function createHydrateStreamAccumulator(
  onResult: (r: {
    queryID: string;
    changes: RowChange[];
    timingMs: number | undefined;
  }) => void,
): {
  onFrame: (value: unknown) => void;
  finish: () => void;
} {
  const acc = new Map<
    string,
    {changes: RowChange[]; expectedNextIndex: number}
  >();
  // Tracks queryIDs that already saw final=true. Without this, a duplicate
  // final frame for the same queryID would resurrect a fresh entry (since
  // we delete from `acc` on first final) and re-invoke onResult — silently
  // double-resolving the caller's promise / re-firing dispatch handlers.
  // A wire bug or a replayed frame should fail loud here, not silently
  // mis-dispatch.
  const finalized = new Set<string>();

  return {
    onFrame: (value: unknown) => {
      const v = value as {
        queryID: string;
        changes?: RowChange[];
        chunkIndex?: number;
        final?: boolean;
        timingMs?: number;
      };
      const chunkIndex = v.chunkIndex ?? 0;
      const final = v.final ?? true; // older sidecars sent unchunked frames
      const chunk = v.changes ?? [];

      if (finalized.has(v.queryID)) {
        throw new Error(
          `addQueriesStream duplicate frame after final for queryID=${v.queryID} ` +
            `(chunkIndex=${chunkIndex}, final=${final}): Go re-emitted post-terminal`,
        );
      }

      let entry = acc.get(v.queryID);
      if (!entry) {
        entry = {changes: [], expectedNextIndex: 0};
        acc.set(v.queryID, entry);
      }

      // Per-query chunk ordering is guaranteed by the Go sender (single
      // goroutine per query calls onResult sequentially) and the RPC
      // transport preserves frame order within an id. A gap or duplicate
      // means a wire-level bug — fail loudly rather than silently
      // delivering misordered or partial results.
      if (chunkIndex !== entry.expectedNextIndex) {
        throw new Error(
          `addQueriesStream chunk order violation for queryID=${v.queryID}: ` +
            `expected chunkIndex=${entry.expectedNextIndex}, got ${chunkIndex}`,
        );
      }
      entry.expectedNextIndex++;

      if (chunk.length > 0) {
        // Push instead of concat: concat allocates a new array each call,
        // multiplying transient memory on multi-chunk queries.
        for (const rc of chunk) entry.changes.push(rc);
      }

      if (final) {
        acc.delete(v.queryID);
        finalized.add(v.queryID);
        onResult({
          queryID: v.queryID,
          changes: entry.changes,
          timingMs: v.timingMs,
        });
      }
    },

    finish: () => {
      // Any queries left in `acc` after the call resolves mean Go finished
      // (sent "done") without emitting a final chunk for them — caller
      // would silently miss the result. Surface explicitly.
      if (acc.size > 0) {
        const missing = [...acc.keys()].join(',');
        throw new Error(
          `addQueriesStream finished but ${acc.size} queries never received a final chunk: ${missing}`,
        );
      }
    },
  };
}

/**
 * Accumulator for `advanceStream` partial frames. Reassembles into one
 * `AdvanceResult`. After the streaming RPC resolves, call `finish()` to
 * collect the reassembled result; it throws if no frame carried
 * `final=true` before "done".
 *
 * Returned object is stateful — do not reuse across calls.
 */
export function createAdvanceStreamAccumulator(): {
  onFrame: (value: unknown) => void;
  finish: () => AdvanceResult;
} {
  const acc: RowChange[] = [];
  let timings: TableTiming[] | undefined;
  let expectedNextIndex = 0;
  let gotFinal = false;
  let drift: DriftError | undefined;

  return {
    onFrame: (value: unknown) => {
      const v = value as {
        changes?: RowChange[];
        chunkIndex?: number;
        final?: boolean;
        timings?: TableTiming[];
        drift?: {
          table?: string;
          op?: string;
          pk?: Record<string, unknown>;
          hasCount?: number;
        };
      };
      const chunkIndex = v.chunkIndex ?? 0;
      const final = v.final ?? true; // belt-and-braces for older sidecars
      const chunk = v.changes ?? [];

      // Single sender goroutine on the Go side → strict in-order delivery.
      // A gap is a wire-level bug; fail loud rather than silently
      // delivering misordered or partial advance results to the view-syncer.
      if (chunkIndex !== expectedNextIndex) {
        throw new Error(
          `advanceStream chunk order violation: ` +
            `expected chunkIndex=${expectedNextIndex}, got ${chunkIndex}`,
        );
      }
      expectedNextIndex++;

      for (const rc of chunk) acc.push(rc);

      // Timings + drift only travel on the final frame (Go-side invariant).
      if (final) {
        timings = v.timings;
        gotFinal = true;
        if (v.drift) {
          drift = new DriftError({
            table: v.drift.table,
            op: v.drift.op,
            pk: v.drift.pk,
            hasCount: v.drift.hasCount,
          });
        }
      }
    },

    finish: (): AdvanceResult => {
      if (!gotFinal) {
        // `done` arrived without any frame carrying final=true. Indicates
        // a Go-side bug (AdvanceStream MUST always emit a terminal frame,
        // even for empty advances). Surface so we don't silently return a
        // partial AdvanceResult that would corrupt the CVR.
        throw new Error('advanceStream finished without a final chunk');
      }
      if (drift) {
        // Drift on Final — attach the partial-output the Go sidecar
        // produced from Pushes that succeeded BEFORE the panic to the
        // DriftError. GoComputeBackend's recovery path forwards them
        // alongside the re-init so the view-syncer + clients see the
        // same partial-emit-then-rehydrate sequence TS's assert-and-
        // throw path produces. Without this, Go silently dropped
        // RowChanges that TS would have emitted — the divergence
        // observed in shadow soaks.
        drift.partialChanges = acc;
        drift.partialTimings = timings;
        throw drift;
      }
      return {changes: acc, timings};
    },
  };
}

/**
 * Accumulator for `advanceToHeadStream` partial frames (the DRIVE-mode
 * streaming variant of advanceToHead). Reassembles into one
 * {@link AdvanceToHeadResult}. Mirrors {@link createAdvanceStreamAccumulator}
 * for the chunked RowChanges (→ `rowChanges`) but additionally captures the
 * advanceToHead-specific `version` + `numChanges` + `reset`, which ride the
 * final frame only.
 *
 * The derive-only `changes` field is always `[]` here — the streaming variant
 * only carries the engine's RowChanges (drive mode); the P1 derive-only diff
 * uses the non-streaming `advanceToHead`.
 *
 * Returned object is stateful — do not reuse across calls.
 */
export function createAdvanceToHeadStreamAccumulator(): {
  onFrame: (value: unknown) => void;
  finish: () => AdvanceToHeadResult;
} {
  const acc: RowChange[] = [];
  let timings: TableTiming[] | undefined;
  let expectedNextIndex = 0;
  let gotFinal = false;
  let drift: DriftError | undefined;
  let version = '';
  let numChanges = 0;
  let reset: {reason: string; msg: string} | undefined;

  return {
    onFrame: (value: unknown) => {
      const v = value as {
        changes?: RowChange[];
        chunkIndex?: number;
        final?: boolean;
        timings?: TableTiming[];
        drift?: {
          table?: string;
          op?: string;
          pk?: Record<string, unknown>;
          hasCount?: number;
        };
        version?: string;
        numChanges?: number;
        reset?: {reason: string; msg: string};
      };
      const chunkIndex = v.chunkIndex ?? 0;
      const final = v.final ?? true; // belt-and-braces for older sidecars
      const chunk = v.changes ?? [];

      // Single sender goroutine on the Go side → strict in-order delivery.
      // A gap is a wire-level bug; fail loud rather than silently committing
      // a partial advance to the CVR.
      if (chunkIndex !== expectedNextIndex) {
        throw new Error(
          `advanceToHeadStream chunk order violation: ` +
            `expected chunkIndex=${expectedNextIndex}, got ${chunkIndex}`,
        );
      }
      expectedNextIndex++;

      for (const rc of chunk) acc.push(rc);

      // version + numChanges + timings + reset + drift travel on the final
      // frame only (Go-side invariant).
      if (final) {
        timings = v.timings;
        version = v.version ?? '';
        numChanges = v.numChanges ?? 0;
        reset = v.reset;
        gotFinal = true;
        if (v.drift) {
          drift = new DriftError({
            table: v.drift.table,
            op: v.drift.op,
            pk: v.drift.pk,
            hasCount: v.drift.hasCount,
          });
        }
      }
    },

    finish: (): AdvanceToHeadResult => {
      if (!gotFinal) {
        // `done` arrived without any frame carrying final=true — a Go-side bug
        // (AdvanceStream MUST always emit a terminal frame). Surface so we
        // don't silently return a partial result that would corrupt the CVR.
        throw new Error('advanceToHeadStream finished without a final chunk');
      }
      if (drift) {
        // Same contract as createAdvanceStreamAccumulator: attach the
        // partial-output the sidecar produced before the drift panic so
        // GoComputeBackend's recovery forwards it alongside the re-init.
        drift.partialChanges = acc;
        drift.partialTimings = timings;
        throw drift;
      }
      return {
        // Derive-only diff is never streamed (drive mode carries RowChanges).
        changes: [],
        version,
        numChanges,
        rowChanges: acc,
        timings,
        reset,
      };
    },
  };
}

// --- RPC (msgpack) ---

type RPCRequest = {
  jsonrpc: '2.0';
  method: string;
  params?: unknown;
  id: number;
  /**
   * Optional W3C traceparent header for cross-process trace correlation.
   * Forwarded as-is by Go; logged on slow handlers. Full Go-side OTel SDK
   * integration is a separate feature (REVIEW-final MED-CROSS-4).
   */
  traceparent?: string;
};

type RPCResponse = {
  jsonrpc: '2.0';
  result?: unknown;
  error?: {code: number; message: string};
  id: number;
};

const MAX_FRAME_SIZE = 64 * 1024 * 1024; // 64MB — must match Go side
const DEFAULT_TIMEOUT_MS = 30_000;
const MAX_IN_FLIGHT = 1024; // global semaphore: caps concurrent pending RPCs
const MAX_IN_FLIGHT_PER_GROUP = 16; // per-clientGroupID fairness cap
const GLOBAL_KEY = '__global__'; // bucket for ping / unscoped calls
// IDs are uint32 to stay JS-safe regardless of how long the client lives.
// Pending map dedupes inflight collisions; with MAX_IN_FLIGHT=1024 the
// chance of an in-flight collision in a uint32 space is essentially zero.
const MAX_ID = 0xffff_ffff;

export class TimeoutError extends Error {
  constructor(method: string, ms: number) {
    super(`RPC ${method} timed out after ${ms}ms`);
    this.name = 'TimeoutError';
  }
}

/**
 * RPC error code Go uses to signal source drift — MemorySource detected
 * an Edit/Remove against a missing row, or a duplicate Add. Surface for
 * GoComputeBackend so it can branch on the drift recovery path instead
 * of escalating to a generic RPC failure.
 */
export const RPC_CODE_DRIFT = -32100;

/**
 * Terminal sentinel for streaming RPCs (addQueriesStream / advanceStream).
 * Go emits this as a plain-string Result on the final frame; everything
 * else is a partial. Reserved — handlers MUST NOT emit a string-valued
 * partial that collides with this constant. See D6 collision defense
 * in #onData below.
 */
const STREAM_DONE_SENTINEL = 'done';

/**
 * RPC error code Go uses when a mutating call (loadRows / addQuery* /
 * advance*) carries an initEpoch that doesn't match the cgID's current
 * epoch on the sidecar. The caller is a torn-down view-syncer instance
 * whose RPC raced past the next instance's init for the same cgID; without
 * this guard, Go would silently mutate the new engine's state with stale
 * data. Surface so GoComputeBackend can no-op stale calls instead of
 * treating them as protocol errors.
 */
export const RPC_CODE_STALE_INIT_EPOCH = -32101;

export class StaleInitEpochError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'StaleInitEpochError';
  }
}

/**
 * Source-drift signal from the Go sidecar. Carries the offending table +
 * op + PK so the caller can log specifics before re-init. Thrown by
 * {@link GoIVMClient.advance} and surfaced via accumulator on
 * {@link GoIVMClient.advanceStream} when the Final frame has drift set.
 *
 * partialChanges + partialTimings carry the output produced by Push calls
 * that successfully completed BEFORE the panic-and-drift fired in this
 * advance batch. Matches TS view-syncer behavior: assert-and-throw streams
 * whatever was already pushed to the output before snapshot revert +
 * re-hydrate. Previously Go's drift recovery discarded these (the comment
 * at the discard site cited "Replaying them now would risk double-
 * application" — but partial emission + re-hydrate is precisely TS's path,
 * so dropping the partial only widened the TS/Go output divergence
 * observed in shadow soaks).
 */
export class DriftError extends Error {
  readonly table: string;
  readonly op: string;
  readonly pk: Record<string, unknown>;
  readonly hasCount: number;
  partialChanges: RowChange[] = [];
  partialTimings: TableTiming[] | undefined = undefined;
  constructor(data: {
    table?: string | undefined;
    op?: string | undefined;
    pk?: Record<string, unknown> | undefined;
    hasCount?: number | undefined;
    message?: string | undefined;
  }) {
    super(data.message ?? `source drift: table=${data.table} op=${data.op} pk=${JSON.stringify(data.pk)}`);
    this.name = 'DriftError';
    this.table = data.table ?? '';
    this.op = data.op ?? '';
    this.pk = data.pk ?? {};
    this.hasCount = data.hasCount ?? 0;
  }
}

// --- Client ---

export class GoIVMClient {
  readonly #socketPath: string;
  readonly #onLog:
    | ((level: 'info' | 'warn' | 'error', msg: string, err?: unknown) => void)
    | undefined;
  #socket: Socket | null = null;
  #nextID = 1;
  #pending = new Map<
    number,
    {
      method: string;
      resolve: (v: unknown) => void;
      reject: (e: Error) => void;
      timer: NodeJS.Timeout | null;
      /**
       * For streaming RPCs: invoked on each non-terminal response frame.
       * The final frame (Result === "done" by convention, or any frame
       * without `partial`-shape data) resolves the promise normally.
       */
      onPartial?: (value: unknown) => void;
    }
  >();

  // Frame parser state: buffer + read offset. Avoids quadratic Buffer.concat
  // by retaining a single underlying buffer until at least half is consumed,
  // then compacting once.
  #recvBuf: Buffer = Buffer.alloc(0);
  #recvOffset = 0;
  // Bytes remaining to discard as part of skipping past an oversized frame.
  // When > 0, #onData consumes incoming bytes without parsing instead of
  // closing the entire connection (audit H8). The orphaned RPC for the
  // bad frame times out naturally; healthy CGs on the same connection
  // keep flowing.
  #skipRemaining = 0;

  // Backpressure (global cap): awaiters waiting for any in-flight slot.
  #slotWaiters: Array<() => void> = [];
  // Per-clientGroupID in-flight counters and waiter queues. Prevents one
  // rogue group from monopolizing the global MAX_IN_FLIGHT pool and
  // starving every other ViewSyncer (REVIEW-final CRITICAL-CROSS-2).
  #perGroupInFlight = new Map<string, number>();
  #perGroupWaiters = new Map<string, Array<() => void>>();
  // Byte-level backpressure: when socket.write returns false, all subsequent
  // #call invocations await this promise before writing. Set null when the
  // socket is writable (REVIEW-final MED-TS-1).
  #drainPromise: Promise<void> | null = null;
  /**
   * IDs that recently timed out. Late responses for them must be dropped,
   * not routed to a freshly-issued same-ID call (REVIEW-final LOW-TS-2).
   * Bounded eviction in #nextId so the set doesn't grow unbounded.
   */
  #recentlyTimedOut = new Set<number>();

  constructor(
    socketPath = '/tmp/go-ivm.sock',
    options?: {onLog?: (level: 'info' | 'warn' | 'error', msg: string, err?: unknown) => void},
  ) {
    this.#socketPath = socketPath;
    this.#onLog = options?.onLog;
  }

  /** Connect to the Go sidecar. Resolves when connected. */
  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const socket = createConnection(this.#socketPath, () => {
        this.#socket = socket;
        socket.on('data', chunk => this.#onData(chunk));
        resolve();
      });
      socket.on('error', err => {
        if (!this.#socket) {
          reject(err);
          return;
        }
        this.#log('error', `socket error: ${(err as Error).message}`, err);
        // Treat as terminal — the 'close' handler will run next and clean up.
      });
      socket.on('close', () => {
        // Mark socket dead BEFORE rejecting so concurrent #call invocations
        // fail fast instead of writing into a destroyed Socket.
        this.#socket = null;
        const err = new Error('Connection closed');
        for (const [, p] of this.#pending) {
          if (p.timer) clearTimeout(p.timer);
          p.reject(err);
        }
        this.#pending.clear();
        // Release any backpressure waiters (global + per-group + drain) so they can fail fast.
        const globalWaiters = this.#slotWaiters;
        this.#slotWaiters = [];
        for (const w of globalWaiters) w();
        const perGroup = this.#perGroupWaiters;
        this.#perGroupWaiters = new Map();
        for (const waiters of perGroup.values()) for (const w of waiters) w();
        this.#perGroupInFlight.clear();
        this.#drainPromise = null;
      });
    });
  }

  /** Close the connection. */
  close(): void {
    const socket = this.#socket;
    this.#socket = null;
    if (socket) {
      socket.destroy();
    }
    this.#recvBuf = Buffer.alloc(0);
    this.#recvOffset = 0;
    this.#skipRemaining = 0;
    // Reject anything still pending so callers don't hang.
    const err = new Error('Client closed');
    for (const [, p] of this.#pending) {
      if (p.timer) clearTimeout(p.timer);
      p.reject(err);
    }
    this.#pending.clear();
    const waiters = this.#slotWaiters;
    this.#slotWaiters = [];
    for (const w of waiters) w();
    const perGroup = this.#perGroupWaiters;
    this.#perGroupWaiters = new Map();
    for (const ws of perGroup.values()) for (const w of ws) w();
    this.#perGroupInFlight.clear();
  }

  isConnected(): boolean {
    return this.#socket !== null;
  }

  /**
   * Initialize an engine for a client group. Returns the new initEpoch
   * which subsequent mutating calls (loadRows / addQuery* / advance*) MUST
   * pass back so the sidecar can reject calls from a torn-down caller
   * whose RPC raced past a fresh init for the same cgID.
   */
  async init(clientGroupID: string, params: InitParams, opts?: CallOptions): Promise<{initEpoch: number}> {
    // init can be slow on first start; allow longer default.
    const result = (await this.#call(
      'init',
      {clientGroupID, ...params},
      {timeoutMs: opts?.timeoutMs ?? 120_000},
    )) as {status?: string; initEpoch?: number} | 'ok';
    if (typeof result !== 'object' || typeof result.initEpoch !== 'number') {
      throw new Error('init: sidecar did not return initEpoch — protocol mismatch');
    }
    return {initEpoch: result.initEpoch};
  }

  /**
   * Append a batch of rows to a previously-init'd table.
   * Chunking is the caller's responsibility — pick batch sizes that fit
   * comfortably under the 64MB frame cap (typical: 1–10MB encoded).
   */
  async loadRows(
    clientGroupID: string,
    table: string,
    rows: Record<string, unknown>[],
    initEpoch: number,
    opts?: CallOptions,
  ): Promise<void> {
    await this.#call(
      'loadRows',
      {clientGroupID, table, rows, initEpoch},
      {timeoutMs: opts?.timeoutMs ?? 120_000},
    );
  }

  // Hydrates a new query — initial state encoded as ADDs. `timingMs` feeds
  // TS's adaptive advance circuit-breaker.
  async addQuery(
    clientGroupID: string,
    queryID: string,
    ast: unknown,
    initEpoch: number,
    opts?: CallOptions,
  ): Promise<HydrateResult> {
    const result = (await this.#call(
      'addQuery',
      {clientGroupID, queryID, ast, initEpoch},
      {timeoutMs: opts?.timeoutMs ?? 60_000},
    )) as {changes: RowChange[]; timingMs?: number};
    return {changes: result.changes ?? [], timingMs: result.timingMs};
  }

  // Builds all pipelines, hydrates them in parallel goroutines, returns
  // results in input order with per-query `timingMs`.
  async addQueries(
    clientGroupID: string,
    queries: {queryID: string; ast: unknown}[],
    initEpoch: number,
    opts?: CallOptions,
  ): Promise<HydrateResult[]> {
    const result = (await this.#call(
      'addQueries',
      {clientGroupID, queries, initEpoch},
      {timeoutMs: opts?.timeoutMs ?? 120_000},
    )) as {results: {changes: RowChange[]; timingMs?: number}[]};
    return (result.results ?? []).map(r => ({
      changes: r.changes ?? [],
      timingMs: r.timingMs,
    }));
  }

  // Like {@link addQueries} but `onResult` fires per query as each
  // Go goroutine finishes (in completion order, not input order).
  // Resolves on the terminal "done" frame. Cuts tail latency on batches
  // with uneven hydration costs.
  //
  // Wire-level chunking (protocol rev 3+): Go may emit multiple partial
  // frames per query, each carrying up to `hydrateChunkSize` (10k) rows
  // with monotonically increasing `chunkIndex`. The frame with
  // `final: true` is the per-query completion marker. This client
  // accumulates chunks per queryID and invokes the caller's `onResult`
  // exactly once per query — preserving the existing per-query contract
  // so view-syncer code doesn't need to change.
  //
  // The win is operationally: Go-side memory pressure is relieved (large
  // results no longer buffered as one big msgpack frame), and the wire
  // gets first bytes of large hydrations sooner. The TS-side memory
  // footprint here is unchanged from the per-query contract — bounding
  // TS-side memory would require pushing chunks through to the
  // view-syncer (a separate change).
  async addQueriesStream(
    clientGroupID: string,
    queries: {queryID: string; ast: unknown}[],
    initEpoch: number,
    onResult: (r: {queryID: string; changes: RowChange[]; timingMs: number | undefined}) => void,
    opts?: CallOptions,
  ): Promise<void> {
    const handler = createHydrateStreamAccumulator(onResult);
    await this.#call(
      'addQueriesStream',
      {clientGroupID, queries, initEpoch},
      {
        timeoutMs: opts?.timeoutMs ?? 120_000,
        onPartial: handler.onFrame,
      },
    );
    handler.finish();
  }

  /** Remove a query pipeline from a client group's engine. */
  async removeQuery(clientGroupID: string, queryID: string, initEpoch: number, opts?: CallOptions): Promise<void> {
    await this.#call('removeQuery', {clientGroupID, queryID, initEpoch}, opts);
  }

  // Push mode: ship a SnapshotChange diff to Go, which applies it and
  // streams the resulting RowChanges back as one or more partial frames
  // (chunked at advanceChunkSize on the Go side). This method accumulates
  // them and returns an {@link AdvanceResult}, including per-(table, op)
  // wall-times so TS's `ivm.advance-time` histogram keeps native-path
  // granularity.
  //
  // Streaming wins: Go-side memory pressure relieved on large diffs (each
  // chunk is encoded + flushed + freed before the next accumulates instead
  // of being buffered into a single msgpack frame); first chunk hits the
  // socket as soon as advanceChunkSize rows accumulate, so TTFB on big
  // advances drops.
  //
  // Same defensive invariants as {@link addQueriesStream}: chunk-order
  // gaps throw; missing terminal `final: true` throws.
  async advanceStream(
    clientGroupID: string,
    changes: SnapshotChange[],
    initEpoch: number,
    opts?: CallOptions,
  ): Promise<AdvanceResult> {
    const handler = createAdvanceStreamAccumulator();
    await this.#call(
      'advanceStream',
      {clientGroupID, changes, initEpoch},
      {
        timeoutMs: opts?.timeoutMs ?? 120_000,
        onPartial: handler.onFrame,
      },
    );
    return handler.finish();
  }

  /**
   * advanceToHead: the Go sidecar derives its OWN snapshot diff from the
   * replica's changeLog2 (via internal/snapshotter) instead of consuming a
   * TS-shipped SnapshotChange[]. Carries NO changes payload — only the cgID +
   * epoch. Returns Go's derived diff + its new stateVersion. Used (in P1) to
   * shadow-compare Go's diff against TS's own computed one. Requires a
   * protocolRev>=7 sidecar with GO_IVM_ADVANCE_TO_HEAD=true + table mode.
   */
  async advanceToHead(
    clientGroupID: string,
    initEpoch: number,
    appID: string,
    opts?: CallOptions,
  ): Promise<AdvanceToHeadResult> {
    // O2: forward the shard's appID so the sidecar's snapshotter watches the
    // correct `<appID>.permissions` table. Omitted when empty so the Go side
    // keeps its GO_IVM_APP_ID env fallback (p.AppID is `omitempty`).
    const result = (await this.#call(
      'advanceToHead',
      appID ? {clientGroupID, initEpoch, appID} : {clientGroupID, initEpoch},
      opts,
    )) as Partial<AdvanceToHeadResult>;
    return {
      changes: result.changes ?? [],
      version: result.version ?? '',
      numChanges: result.numChanges ?? 0,
      rowChanges: result.rowChanges ?? [],
      timings: result.timings,
      reset: result.reset,
    };
  }

  /**
   * Streaming variant of {@link advanceToHead} for DRIVE mode (P2 / Go-primary
   * trigger). Go derives its own diff, drives its engine, and emits the
   * resulting RowChanges as chunked partial frames (at `advanceChunkSize` on
   * the Go side) instead of one msgpack frame. This method reassembles them
   * into the same {@link AdvanceToHeadResult} shape `advanceToHead` returns, so
   * the caller is unaware of chunking.
   *
   * Why this exists (finding F5): the non-streaming `advanceToHead` carries all
   * drive-mode RowChanges in a single frame under the 64MB cap. A bulk backfill
   * or mass UPDATE blows the cap; the TS receive loop SKIPS the oversized frame,
   * orphaning the RPC until it times out. Streaming bounds per-frame size the
   * same way push-mode already uses {@link advanceStream}.
   *
   * Requires a protocolRev>=8 sidecar with GO_IVM_ADVANCE_TO_HEAD=true,
   * GO_IVM_ADVANCE_DRIVE=true, + table mode. Same defensive invariants as
   * {@link advanceStream}: chunk-order gaps throw; missing terminal `final:true`
   * throws; drift on the final frame re-throws as a {@link DriftError}.
   */
  async advanceToHeadStream(
    clientGroupID: string,
    initEpoch: number,
    appID: string,
    opts?: CallOptions,
  ): Promise<AdvanceToHeadResult> {
    const handler = createAdvanceToHeadStreamAccumulator();
    await this.#call(
      'advanceToHeadStream',
      appID ? {clientGroupID, initEpoch, appID} : {clientGroupID, initEpoch},
      {
        timeoutMs: opts?.timeoutMs ?? 120_000,
        // Forward the group for in-flight fairness (advanceStream omits this;
        // we keep it so a single group can't starve others — CROSS-2).
        clientGroupID: opts?.clientGroupID ?? clientGroupID,
        onPartial: handler.onFrame,
      },
    );
    return handler.finish();
  }

  /**
   * Destroy a client group's engine.
   * Call when the client disconnects to free memory.
   */
  async destroy(clientGroupID: string, opts?: CallOptions): Promise<void> {
    await this.#call('destroy', {clientGroupID}, opts);
  }

  /**
   * Roll the engine's leaf sources to a fresh snapshot of underlying
   * storage. No-op for MemorySource (it has no pinned snapshot); for
   * tablesource.Source it commits the current read tx and opens a new
   * one at the current WAL frame. Used by the drift audit so its
   * comparison reads on Go see the same point-in-time as the SQL
   * ground-truth read (avoids transient snapshot skew). protocolRev 6+.
   */
  async refreshSnapshot(
    clientGroupID: string,
    initEpoch: number,
    opts?: CallOptions,
  ): Promise<void> {
    await this.#call(
      'refreshSnapshot',
      {clientGroupID, initEpoch},
      opts,
    );
  }

  /**
   * Returns the number of queries currently registered on the given CG's
   * engine. Drift audit uses this as a cross-validation health probe: if
   * TS thinks N queries are active but Go reports 0, per-CG recovery has
   * silently dropped pipeline state (the C2 freeze). Returns 0 cleanly
   * when the engine isn't initialized — the absence-of-engine is the
   * answer. No initEpoch param: we want this probe to succeed even
   * after a stale view-syncer instance was torn down.
   */
  async pipelineCount(
    clientGroupID: string,
    opts?: CallOptions,
  ): Promise<number> {
    const r = await this.#call('pipelineCount', {clientGroupID}, opts);
    return typeof r === 'number' ? r : 0;
  }

  /** Ping the sidecar. */
  async ping(opts?: CallOptions): Promise<string> {
    return (await this.#call('ping', undefined, {timeoutMs: opts?.timeoutMs ?? 5_000})) as string;
  }

  /**
   * Query sidecar version and protocol revision. Used by `SidecarManager`
   * to refuse to talk to an incompatible build during rolling deploys
   * (REVIEW-final MED-CROSS-5).
   *
   * `sourceMode` ('memory' | 'table') is reported by newer sidecars so the
   * init path can skip shipping row contents when the sidecar reads SQLite
   * directly (loadRows is a no-op in table mode). Absent on older builds —
   * callers must treat undefined as memory mode (ship rows).
   */
  async version(
    opts?: CallOptions,
  ): Promise<{version: string; protocolRev: number; sourceMode?: string}> {
    return (await this.#call('version', undefined, {
      timeoutMs: opts?.timeoutMs ?? 5_000,
    })) as {version: string; protocolRev: number; sourceMode?: string};
  }

  // --- Private ---

  #log(level: 'info' | 'warn' | 'error', msg: string, err?: unknown): void {
    // No console fallback — callers without a logger get silence by design.
    // SidecarManager wires its own logger through to here.
    this.#onLog?.(level, msg, err);
  }

  async #acquireSlot(cgID: string): Promise<void> {
    // Byte-level backpressure — wait for socket drain before issuing more
    // writes (REVIEW-final MED-TS-1).
    if (this.#drainPromise) {
      await this.#drainPromise;
    }
    // Global cap first — a hard safety bound on aggregate in-flight RPCs.
    if (this.#pending.size >= MAX_IN_FLIGHT) {
      await new Promise<void>(resolve => this.#slotWaiters.push(resolve));
    }
    // Then per-group cap so one client group can't starve others
    // (REVIEW-final CRITICAL-CROSS-2).
    while ((this.#perGroupInFlight.get(cgID) ?? 0) >= MAX_IN_FLIGHT_PER_GROUP) {
      let waiters = this.#perGroupWaiters.get(cgID);
      if (!waiters) {
        waiters = [];
        this.#perGroupWaiters.set(cgID, waiters);
      }
      await new Promise<void>(resolve => waiters!.push(resolve));
    }
    this.#perGroupInFlight.set(cgID, (this.#perGroupInFlight.get(cgID) ?? 0) + 1);
  }

  #releaseSlot(cgID: string): void {
    const cur = this.#perGroupInFlight.get(cgID) ?? 1;
    if (cur <= 1) {
      this.#perGroupInFlight.delete(cgID);
    } else {
      this.#perGroupInFlight.set(cgID, cur - 1);
    }
    const groupWaiters = this.#perGroupWaiters.get(cgID);
    if (groupWaiters && groupWaiters.length > 0) {
      const next = groupWaiters.shift();
      if (next) next();
      if (groupWaiters.length === 0) this.#perGroupWaiters.delete(cgID);
    }
    const globalNext = this.#slotWaiters.shift();
    if (globalNext) globalNext();
  }

  /** Bounded record of recently-timed-out IDs; oldest evicted when full. */
  #recordTimeout(id: number): void {
    const MAX_RECENT = 4096;
    this.#recentlyTimedOut.add(id);
    if (this.#recentlyTimedOut.size > MAX_RECENT) {
      // Evict the oldest (Set preserves insertion order in JS).
      const first = this.#recentlyTimedOut.values().next().value;
      if (first !== undefined) this.#recentlyTimedOut.delete(first);
    }
  }

  #nextId(): number {
    // Wrap before exhausting JS-safe range. With MAX_IN_FLIGHT cap, in-flight
    // collisions in uint32 space are practically impossible. We also skip:
    //   - IDs currently in #pending (a wrap-around collision)
    //   - IDs recently timed-out (a delayed response could route to the new
    //     pending entry); REVIEW-final LOW-TS-2.
    let id = this.#nextID;
    for (let tries = 0; tries < 32; tries++) {
      this.#nextID = id >= MAX_ID ? 1 : id + 1;
      if (!this.#pending.has(id) && !this.#recentlyTimedOut.has(id)) return id;
      id = this.#nextID;
    }
    // Extremely unlikely; bubble up rather than risk wrong-response routing.
    throw new Error('Could not allocate unused RPC id');
  }

  async #call(method: string, params: unknown, opts?: CallOptions): Promise<unknown> {
    const cgID = opts?.clientGroupID ?? GLOBAL_KEY;
    // Backpressure: cap in-flight RPCs globally and per-group.
    await this.#acquireSlot(cgID);

    const socket = this.#socket;
    if (!socket) {
      this.#releaseSlot(cgID);
      throw new Error('Not connected');
    }

    const id = this.#nextId();
    const timeoutMs = opts?.timeoutMs ?? DEFAULT_TIMEOUT_MS;

    return new Promise<unknown>((resolve, reject) => {
      const cleanup = () => {
        const entry = this.#pending.get(id);
        if (entry?.timer) clearTimeout(entry.timer);
        this.#pending.delete(id);
        this.#releaseSlot(cgID);
      };
      const wrappedResolve = (v: unknown) => {
        cleanup();
        resolve(v);
      };
      const wrappedReject = (e: Error) => {
        cleanup();
        reject(e);
      };

      let timer: NodeJS.Timeout | null = null;
      if (timeoutMs > 0 && timeoutMs !== Infinity) {
        timer = setTimeout(() => {
          // Remember this ID so a late-arriving response can't route to a
          // freshly-issued same-ID call. Bounded eviction below caps the set.
          this.#recordTimeout(id);
          wrappedReject(new TimeoutError(method, timeoutMs));
        }, timeoutMs);
        // Don't keep the event loop alive for an in-flight RPC.
        timer.unref?.();
      }

      const entry: {
        method: string;
        resolve: (v: unknown) => void;
        reject: (e: Error) => void;
        timer: NodeJS.Timeout | null;
        onPartial?: (value: unknown) => void;
      } = {
        method,
        resolve: wrappedResolve,
        reject: wrappedReject,
        timer,
      };
      if (opts?.onPartial) entry.onPartial = opts.onPartial;
      this.#pending.set(id, entry);

      const req: RPCRequest = {jsonrpc: '2.0', method, params, id};
      // Attach the active W3C traceparent if available — Go-side can log
      // it for slow handlers and a future Go OTel SDK can resume the
      // trace (REVIEW-final MED-CROSS-4).
      const tp = getActiveTraceparent();
      if (tp) req.traceparent = tp;
      let payload: Buffer;
      try {
        payload = pack(req);
      } catch (err) {
        wrappedReject(err as Error);
        return;
      }
      if (payload.length > MAX_FRAME_SIZE) {
        wrappedReject(
          new Error(`Payload too large for method ${method}: ${payload.length} > ${MAX_FRAME_SIZE}`),
        );
        return;
      }
      const frame = Buffer.allocUnsafe(4 + payload.length);
      frame.writeUInt32BE(payload.length, 0);
      payload.copy(frame, 4);

      const ok = socket.write(frame);
      if (!ok && !this.#drainPromise) {
        // Byte-level backpressure: gate subsequent writes until the kernel
        // buffer drains. Subsequent #call invocations await this promise
        // in #acquireSlot (REVIEW-final MED-TS-1).
        this.#drainPromise = new Promise<void>(resolve => {
          socket.once('drain', () => {
            this.#drainPromise = null;
            resolve();
          });
        });
      }
    });
  }

  #onData(chunk: Buffer): void {
    // Skip-mode (H8): when a previous frame was rejected as too large
    // we discard its remaining bytes from the stream instead of closing
    // the entire connection. Other CGs' RPCs continue normally; the
    // orphaned RPC times out via its own timer.
    if (this.#skipRemaining > 0) {
      const discard = Math.min(chunk.length, this.#skipRemaining);
      this.#skipRemaining -= discard;
      if (discard === chunk.length) return;
      chunk = chunk.subarray(discard);
    }
    // Append: if we have unconsumed data, splice it together with the new
    // chunk. Optimisation for the case where we already know the next
    // frame's length and it's larger than the new chunk: pre-allocate a
    // buffer sized for the full frame so subsequent chunks copy into it
    // in O(chunkSize) instead of O(everything-so-far). Avoids the O(N²)
    // accumulation that hit very large frames split across many chunks
    // (REVIEW-final LOW-TS-1).
    if (this.#recvOffset === this.#recvBuf.length) {
      this.#recvBuf = chunk;
      this.#recvOffset = 0;
    } else {
      const remaining = this.#recvBuf.length - this.#recvOffset;
      // If we have a length-prefix and know the target frame is bigger
      // than what we currently hold, grow once to the full frame size.
      let cap = remaining + chunk.length;
      if (remaining >= 4) {
        const announcedLen = this.#recvBuf.readUInt32BE(this.#recvOffset);
        if (announcedLen <= MAX_FRAME_SIZE && 4 + announcedLen > cap) {
          cap = 4 + announcedLen;
        }
      }
      const combined = Buffer.allocUnsafe(cap);
      this.#recvBuf.copy(combined, 0, this.#recvOffset);
      chunk.copy(combined, remaining);
      // Track logical filled length separately from buffer capacity. The
      // existing length checks below use `this.#recvBuf.length`; keep that
      // semantic by trimming if we over-allocated.
      this.#recvBuf =
        remaining + chunk.length === cap
          ? combined
          : combined.subarray(0, remaining + chunk.length);
      this.#recvOffset = 0;
    }

    while (this.#recvBuf.length - this.#recvOffset >= 4) {
      const len = this.#recvBuf.readUInt32BE(this.#recvOffset);
      // D7: minimum frame size. Wire never legitimately carries len=0
      // (no valid msgpack response is 0 bytes). Pre-fix this would slide
      // past the 4-byte prefix, hand an empty payload to unpack(), warn,
      // and continue — turning a corrupted stream of `\x00\x00\x00\x00`
      // into a fast log-flood loop. Now we close the connection (returning
      // here breaks out; subsequent #onData calls re-enter against a
      // buffer that still starts with the same len-0 prefix, but the
      // outer caller closes on persistent decode failure).
      if (len < 1) {
        this.#log(
          'error',
          `Frame too small: ${len} bytes (protocol violation) — closing connection`,
        );
        // Force the connection closed so the SidecarManager's restart
        // machinery takes over; the stream is desynchronized.
        try {
          this.#socket?.destroy(new Error(`protocol violation: zero-length frame`));
        } catch {
          // best-effort
        }
        return;
      }
      if (len > MAX_FRAME_SIZE) {
        // H8 fix: don't close the connection — that kills every CG on
        // this socket for one bad query. The frame's body is too long
        // to parse (we can't know the RPC id without decoding), so we
        // can't selectively reject the offending pending entry. Skip-
        // read its bytes instead; the orphaned RPC times out via its
        // own timer while other CGs' traffic flows uninterrupted.
        this.#log(
          'error',
          `Frame too large: ${len} bytes — skipping past it (orphan RPC will time out)`,
        );
        // Step past the 4-byte length prefix. Any frame body bytes
        // already buffered get discarded inline; remaining bytes are
        // discarded on subsequent #onData chunks via #skipRemaining.
        this.#recvOffset += 4;
        const bufferedBody = this.#recvBuf.length - this.#recvOffset;
        if (bufferedBody >= len) {
          // Whole body already in buffer — advance past it.
          this.#recvOffset += len;
        } else {
          // Discard the buffered portion now; consume the rest from
          // upcoming chunks.
          this.#skipRemaining = len - bufferedBody;
          this.#recvBuf = Buffer.alloc(0);
          this.#recvOffset = 0;
        }
        continue;
      }
      if (this.#recvBuf.length - this.#recvOffset < 4 + len) {
        // Wait for more bytes; preserve the partial frame.
        return;
      }

      const payload = this.#recvBuf.subarray(this.#recvOffset + 4, this.#recvOffset + 4 + len);
      this.#recvOffset += 4 + len;

      let resp: RPCResponse;
      try {
        resp = unpack(payload) as RPCResponse;
      } catch (e) {
        this.#log('warn', `failed to decode response frame: ${(e as Error).message}`);
        continue;
      }

      // Coerce id to Number: msgpackr decodes Go's uint64 (9-byte non-compact
      // encoding) as BigInt, which won't match the Number key stored in
      // #pending when the request was sent.
      const respId = typeof resp.id === 'bigint' ? Number(resp.id) : resp.id;
      const pending = this.#pending.get(respId);
      if (!pending) continue;

      if (resp.error) {
        if (resp.error.code === RPC_CODE_DRIFT) {
          // Drift signal: parse the structured payload Go ships in
          // error.data (table/op/pk/hasCount) so callers don't have to
          // string-match the message. Caller is GoComputeBackend, which
          // catches DriftError and triggers re-init.
          // partialChanges + partialTimings carry the RowChanges produced
          // by Pushes that completed BEFORE the panic; the recovery path
          // emits them to clients matching TS's "stream-pre-throw-output-
          // then-revert" behavior.
          const d = (resp.error as {data?: unknown}).data as {
            table?: string;
            op?: string;
            pk?: Record<string, unknown>;
            hasCount?: number;
            partialChanges?: RowChange[];
            partialTimings?: TableTiming[];
          } | undefined;
          const drift = new DriftError({
            table: d?.table,
            op: d?.op,
            pk: d?.pk,
            hasCount: d?.hasCount,
            message: resp.error.message,
          });
          drift.partialChanges = d?.partialChanges ?? [];
          drift.partialTimings = d?.partialTimings;
          pending.reject(drift);
        } else if (resp.error.code === RPC_CODE_STALE_INIT_EPOCH) {
          // Stale-epoch signal: the sidecar is rejecting our call because
          // this view-syncer instance's initEpoch is behind a successor's.
          // Surface as StaleInitEpochError so callers (GoComputeBackend,
          // PipelineDriver's #goPrimaryAdvance catch) can branch on
          // instanceof instead of string-matching. Pre-fix this fell
          // through to the generic Error path and the type existed but
          // was never instantiated.
          pending.reject(new StaleInitEpochError(resp.error.message));
        } else {
          pending.reject(new Error(`RPC error ${resp.error.code}: ${resp.error.message}`));
        }
        continue;
      }
      // Streaming: deliver per-frame value to onPartial unless this is the
      // terminal "done" sentinel. Go emits "done" as a plain string Result;
      // any other Result shape is a partial.
      //
      // Sentinel collision defense (D6): partial values are ALWAYS objects
      // (chunk-metadata records); the literal string "done" is reserved as
      // the terminal sentinel. If a partial were ever emitted as the string
      // "done" (Go-side bug or replay), the equality check below would
      // silently terminate the stream — caller's onResult never fires for
      // the missing query, and the accumulator's `finish()` then throws
      // "queries never received a final chunk", obscuring the real cause.
      // We assert partial shape here so the failure surfaces with the
      // useful error.
      //
      // try/catch around onPartial is load-bearing: accumulators (e.g.
      // createAdvanceStreamAccumulator) throw on chunk-order violations
      // or missing-final invariants. A synchronous throw from this
      // socket data handler propagates to Node's EventEmitter, which
      // emits 'error' on the socket — without an explicit listener
      // that triggers uncaughtException and crashes the worker.
      // Pre-fix this took down the entire view-syncer process on any
      // protocol bug. Now we reject the offending RPC with the throw
      // and continue draining other pending entries; the connection
      // stays up and other CGs' RPCs flow normally.
      const isStreamTerminal = resp.result === STREAM_DONE_SENTINEL;
      if (pending.onPartial && !isStreamTerminal) {
        if (typeof resp.result !== 'object' || resp.result === null) {
          if (pending.timer) clearTimeout(pending.timer);
          this.#pending.delete(respId);
          pending.reject(
            new Error(
              `Streaming RPC received non-object partial: ` +
                `typeof=${typeof resp.result} value=${JSON.stringify(resp.result)}; ` +
                `partials must be records, "${STREAM_DONE_SENTINEL}" is reserved as terminal sentinel`,
            ),
          );
          continue;
        }
        try {
          pending.onPartial(resp.result);
        } catch (err) {
          if (pending.timer) clearTimeout(pending.timer);
          this.#pending.delete(respId);
          pending.reject(
            err instanceof Error
              ? err
              : new Error(`onPartial threw: ${String(err)}`),
          );
        }
        continue;
      }
      pending.resolve(resp.result);
    }

    // Compact periodically so the underlying chunk's memory can be released
    // once we've consumed at least half of it.
    if (this.#recvOffset > 0 && this.#recvOffset * 2 >= this.#recvBuf.length) {
      this.#recvBuf = this.#recvBuf.subarray(this.#recvOffset);
      this.#recvOffset = 0;
    }
  }
}
