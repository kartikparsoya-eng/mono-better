// MessagePack-RPC client for the Go IVM sidecar over a Unix socket.
// Wire format: 4-byte big-endian length prefix, then a MessagePack payload.
// Each clientGroupID maps to its own Engine in the sidecar; groups run in
// parallel and the client multiplexes a single socket across them.

import {createConnection, type Socket} from 'net';
import {Packr, Unpackr} from 'msgpackr';
import {
  DELIVERY_KIND_FRAME,
  DELIVERY_KIND_GROUP_DEF,
  DELIVERY_KIND_HOST_DEATH,
  DELIVERY_KIND_ROW,
  RowGroupRegistry,
} from './napi-records.ts';
import type {GoNapiAddon} from './napi/index.ts';
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
// Exported as a test seam: the wire-contract test (go-ivm-client.test.ts)
// decodes REAL Go-emitted msgpack bytes through this exact production Unpackr,
// pinning the vmihailenco-encode → msgpackr-decode boundary that the
// object-level frame tests never cross. Not intended for other callers.
export const unpack = (buf: Buffer | Uint8Array) =>
  codec.unpackr.unpack(buf as Buffer);

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
  /**
   * Row-plane callback (NAPI transport only). When set, per-row records
   * (delivery kinds 2/3) for this RPC id are decoded and routed here as
   * assembled RowChange objects — bypassing msgpack entirely. Partial
   * FRAMES for the same id (fallback rows + the terminal final frame)
   * still route through onPartial. See napi-records.ts.
   */
  onRow?: (change: RowChange) => void;
  /**
   * Invoked synchronously with the allocated RPC id just before the
   * request is sent. Pull-mode streams (ABI v3) use it to learn the reqID
   * for goivm_stream_credit/goivm_stream_cancel top-up and cancel calls.
   * (The OPENING window never uses this — it rides the request params as
   * pullWindow, because a credit call racing ahead of Go-side gate
   * registration is a silent no-op and would strand the stream.)
   */
  onStreamOpen?: (reqID: number) => void;
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

// --- Positional (protocolRev 9) RowChange decoding ---
//
// Streamed RowChange chunks arrive in the positional wire form (see the Go
// side's positional.go): column-name keys are sent ONCE per (queryID,table)
// group in a dictionary, and each row is a value-only array referencing its
// group. This decodes that frame back into the RowChange[] the rest of the
// view-syncer consumes — identical objects to the legacy map-keyed decode.

type PositionalDictEntry = {
  q: string; // queryID
  t: string; // table
  c: string[]; // column order for add/edit rows
  k: string[]; // primary-key column names
};

function decodePositionalChanges(
  dict: PositionalDictEntry[],
  rows: unknown[][],
): RowChange[] {
  const out: RowChange[] = [];
  for (let i = 0; i < rows.length; i++) {
    const arr = rows[i];
    const e = dict[arr[0] as number];
    const type = arr[1] as 0 | 1 | 2;
    if (type === 1 /* remove */) {
      const rowKey: Record<string, unknown> = {};
      for (let j = 0; j < e.k.length; j++) rowKey[e.k[j]] = arr[2 + j];
      // A remove carries no row — matches the legacy decode where the omitted
      // `row` field left rc.row undefined (downstream keys removes by rowKey).
      out[i] = {type, queryID: e.q, table: e.t, rowKey} as unknown as RowChange;
    } else {
      const row: Record<string, unknown> = {};
      for (let j = 0; j < e.c.length; j++) row[e.c[j]] = arr[2 + j];
      // rowKey is derived from the row's PK columns; the Go side's pkValue is a
      // pure lookup so rowKey[pk] === row[pk].
      const rowKey: Record<string, unknown> = {};
      for (let j = 0; j < e.k.length; j++) rowKey[e.k[j]] = row[e.k[j]];
      out[i] = {type, queryID: e.q, table: e.t, rowKey, row};
    }
  }
  return out;
}

/**
 * Extract a stream frame's RowChanges. Tolerant of both encodings: positional
 * (`{d, r}`, protocolRev 9) when present, else the legacy `changes` array.
 * Empty frames (no `r` and no `changes`) yield `[]`.
 */
function extractChanges(value: unknown): RowChange[] {
  const v = value as {
    changes?: RowChange[];
    d?: PositionalDictEntry[];
    r?: unknown[][];
  };
  // Guard BOTH undefined and null (not just `!== undefined`): a wire-encoded
  // msgpack-null `r` must also be treated as "no positional payload". This
  // defends the decode against a future Go-side drop of `omitempty` on the
  // Rows field (nil -> msgpack-null on the wire): without the null check,
  // `null !== undefined` is true and we'd reach
  // decodePositionalChanges(…, null) -> null.length -> TypeError. The Go side
  // currently emits `r,omitempty` (positional.go / *StreamPartial structs), so
  // empty frames already omit `r`; this is belt-and-suspenders.
  if (v.r !== undefined && v.r !== null) {
    return decodePositionalChanges(v.d ?? [], v.r);
  }
  return v.changes ?? [];
}

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
    final?: boolean;
    chunkIndex?: number;
  }) => void,
  opts?: {chunked?: boolean; rowMode?: boolean},
): {
  onFrame: (value: unknown) => void;
  onRow: (change: RowChange) => void;
  finish: () => void;
} {
  // When true, deliver each partial frame straight to onResult (carrying its
  // own final/chunkIndex) instead of buffering per queryID until final. Lets
  // the caller start poke delivery on the FIRST chunk instead of waiting for
  // the whole query. The ordering + duplicate-final guards below still apply.
  // Default false = original accumulate-until-final behavior (unit tests and
  // all non-streaming callers rely on the exact {queryID,changes,timingMs}
  // shape emitted in that branch).
  const chunked = opts?.chunked === true;
  // Row mode (NAPI transport): rows arrive individually via onRow (kind-3
  // records, routed per queryID); FRAMES carry only fallback rows + each
  // query's terminal final. chunkIndex continuity is relaxed to
  // non-decreasing (row-bearing partials produce no frame). See the
  // advance accumulator's rowMode note.
  const rowMode = opts?.rowMode === true;
  const acc = new Map<
    string,
    {changes: RowChange[]; expectedNextIndex: number; rowChunkIndex: number}
  >();
  // Tracks queryIDs that already saw final=true. Without this, a duplicate
  // final frame for the same queryID would resurrect a fresh entry (since
  // we delete from `acc` on first final) and re-invoke onResult — silently
  // double-resolving the caller's promise / re-firing dispatch handlers.
  // A wire bug or a replayed frame should fail loud here, not silently
  // mis-dispatch.
  const finalized = new Set<string>();

  const entryFor = (queryID: string) => {
    let entry = acc.get(queryID);
    if (!entry) {
      entry = {changes: [], expectedNextIndex: 0, rowChunkIndex: 0};
      acc.set(queryID, entry);
    }
    return entry;
  };

  return {
    onRow: (change: RowChange) => {
      if (finalized.has(change.queryID)) {
        throw new Error(
          `addQueriesStream row record after final for queryID=${change.queryID}`,
        );
      }
      const entry = entryFor(change.queryID);
      if (chunked) {
        // Per-chunk consumers get each row as its own 1-row non-final
        // delivery with a synthetic per-query chunk counter (records don't
        // carry chunkIndex; the addon queue guarantees order). NOTE: this
        // synthetic counter shares a numeric space with the ENGINE's frame
        // chunkIndex (fallback frames + the terminal final) but counts
        // different things — (queryID, chunkIndex) is NOT a unique key
        // across the two planes and must never be used as one. Consumers
        // key on `final` only (view-syncer gates once-per-query metrics on
        // it); the wire-order guarantee comes from the addon queue.
        onResult({
          queryID: change.queryID,
          changes: [change],
          timingMs: undefined,
          final: false,
          chunkIndex: entry.rowChunkIndex++,
        });
        return;
      }
      entry.changes.push(change);
    },
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
      const chunk = extractChanges(value);

      if (finalized.has(v.queryID)) {
        throw new Error(
          `addQueriesStream duplicate frame after final for queryID=${v.queryID} ` +
            `(chunkIndex=${chunkIndex}, final=${final}): Go re-emitted post-terminal`,
        );
      }

      let entry = acc.get(v.queryID);
      if (!entry) {
        entry = {changes: [], expectedNextIndex: 0, rowChunkIndex: 0};
        acc.set(v.queryID, entry);
      }

      // Per-query chunk ordering is guaranteed by the Go sender (single
      // goroutine per query calls onResult sequentially) and the RPC
      // transport preserves frame order within an id. A gap or duplicate
      // means a wire-level bug — fail loudly rather than silently
      // delivering misordered or partial results. Row mode: gaps expected
      // (row-bearing partials ship as records); backwards = reordering.
      if (rowMode ? chunkIndex < entry.expectedNextIndex : chunkIndex !== entry.expectedNextIndex) {
        throw new Error(
          `addQueriesStream chunk order violation for queryID=${v.queryID}: ` +
            `expected chunkIndex=${entry.expectedNextIndex}, got ${chunkIndex}`,
        );
      }
      entry.expectedNextIndex = chunkIndex + 1;

      if (chunked) {
        // Per-chunk delivery: hand this frame straight through (no per-queryID
        // accumulation). timingMs is only meaningful on the terminal frame
        // (Go sets it there — see engine.go flush()), so pass it only when
        // final; non-final chunks carry undefined.
        if (final) {
          acc.delete(v.queryID);
          finalized.add(v.queryID);
        }
        onResult({
          queryID: v.queryID,
          changes: chunk,
          timingMs: final ? v.timingMs : undefined,
          final,
          chunkIndex,
        });
        return;
      }

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
export function createAdvanceStreamAccumulator(opts?: {rowMode?: boolean}): {
  onFrame: (value: unknown) => void;
  onRow: (change: RowChange) => void;
  finish: () => AdvanceResult;
} {
  // Row mode (NAPI transport): rows arrive individually via onRow (kind-3
  // records) while FRAMES carry only fallback rows + the terminal final.
  // Ordering across both planes is guaranteed by the addon's single
  // delivery queue, but chunkIndex continuity is NOT observable
  // frame-to-frame (row-bearing partials produce no frame at all), so the
  // strict monotonicity check is relaxed to "non-decreasing" in row mode.
  const rowMode = opts?.rowMode === true;
  const acc: RowChange[] = [];
  let timings: TableTiming[] | undefined;
  let expectedNextIndex = 0;
  let gotFinal = false;
  let drift: DriftError | undefined;

  return {
    onRow: (change: RowChange) => {
      if (gotFinal) {
        throw new Error('advanceStream received row record after final frame');
      }
      acc.push(change);
    },
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
      const chunk = extractChanges(value);

      // Single sender goroutine on the Go side → strict in-order delivery.
      // A gap is a wire-level bug; fail loud rather than silently
      // delivering misordered or partial advance results to the view-syncer.
      // Row mode: index GAPS are expected (row-bearing partials ship as
      // records, not frames), but going backwards still means reordering.
      if (rowMode ? chunkIndex < expectedNextIndex : chunkIndex !== expectedNextIndex) {
        throw new Error(
          `advanceStream chunk order violation: ` +
            `expected chunkIndex=${expectedNextIndex}, got ${chunkIndex}`,
        );
      }
      expectedNextIndex = chunkIndex + 1;

      // Reject chunks arriving after the terminal frame — a Go-side wire bug
      // that would silently corrupt the accumulated result.
      if (gotFinal) {
        throw new Error(
          `advanceStream received chunk (index=${chunkIndex}) after final frame`,
        );
      }

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
export function createAdvanceToHeadStreamAccumulator(opts?: {
  rowMode?: boolean;
}): {
  onFrame: (value: unknown) => void;
  onRow: (change: RowChange) => void;
  finish: () => AdvanceToHeadResult;
} {
  // Row mode (NAPI transport): same two-plane contract as
  // createAdvanceStreamAccumulator — rows arrive individually via onRow
  // (kind-3 records) while FRAMES carry only fallback rows + the terminal
  // final (which here also carries version/numChanges). Ordering across
  // both planes is guaranteed by the addon's single delivery queue, but
  // chunkIndex continuity is NOT observable frame-to-frame (row-bearing
  // partials produce no frame at all), so the strict monotonicity check
  // is relaxed to "non-decreasing" in row mode.
  const rowMode = opts?.rowMode === true;
  const acc: RowChange[] = [];
  let timings: TableTiming[] | undefined;
  let expectedNextIndex = 0;
  let gotFinal = false;
  let drift: DriftError | undefined;
  let version = '';
  let numChanges = 0;
  let reset: {reason: string; msg: string} | undefined;

  return {
    onRow: (change: RowChange) => {
      if (gotFinal) {
        throw new Error(
          'advanceToHeadStream received row record after final frame',
        );
      }
      acc.push(change);
    },
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
      const chunk = extractChanges(value);

      // Single sender goroutine on the Go side → strict in-order delivery.
      // A gap is a wire-level bug; fail loud rather than silently committing
      // a partial advance to the CVR.
      // Row mode: index GAPS are expected (row-bearing partials ship as
      // records, not frames), but going backwards still means reordering.
      if (rowMode ? chunkIndex < expectedNextIndex : chunkIndex !== expectedNextIndex) {
        throw new Error(
          `advanceToHeadStream chunk order violation: ` +
            `expected chunkIndex=${expectedNextIndex}, got ${chunkIndex}`,
        );
      }
      expectedNextIndex = chunkIndex + 1;

      // Reject chunks arriving after the terminal frame — a Go-side wire bug
      // that would silently corrupt the accumulated result.
      if (gotFinal) {
        throw new Error(
          `advanceToHeadStream received chunk (index=${chunkIndex}) after final frame`,
        );
      }

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
 * RPC error code Go uses when a panic was a DETERMINISTIC, NON-RETRYABLE data
 * (or schema) error — bad replica data the sidecar cannot represent in the JS
 * value model: a non-JSON string in a json/array column, an integer beyond JS
 * MAX_SAFE_INTEGER, or a cross-type comparison (ivm.DataError, recovered in the
 * sidecar's panic handler). Surface so #classifyGoPrimaryAdvanceError can tear
 * down the CG — matching TS-native's UnsupportedValueError throw — instead of
 * escalating to a pipeline reset. Reset CANNOT fix bad data: it re-hydrates,
 * re-reads the same row, re-panics, resets again → an infinite reset storm
 * that also re-pays every CG's hydrate cost (the 5–8s p99 incident).
 */
export const RPC_CODE_DATA_ERROR = -32102;

export class PermanentDataError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PermanentDataError';
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
    // BigInt-safe stringify (scale review): msgpackr decodes Go's int64/
    // uint64 PK values as BigInt, and JSON.stringify THROWS on BigInt — a
    // drift error over a bigint PK would crash the dispatch path instead of
    // rejecting one RPC.
    super(
      data.message ??
        `source drift: table=${data.table} op=${data.op} pk=${JSON.stringify(
          data.pk,
          (_k, v: unknown) => (typeof v === 'bigint' ? v.toString() : v),
        )}`,
    );
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
  /**
   * A2 (scale review): invoked ONCE when the in-process (NAPI) transport is
   * detected dead (goivm_send rc != 0 — the pump/host has exited). The
   * embedder (SidecarManager) routes this to its fatal-exit path: a dead
   * in-process engine cannot be restarted (dlopen is once-per-process) and
   * TS fallback is unsound for Go-owned client groups, so the worker must
   * crash for the supervisor to restore a working state. Without this hook
   * the send failure only rejected the ONE RPC — the manager stayed
   * 'running' and every CG spun in per-CG reset loops forever.
   */
  readonly #onFatal: ((err: Error) => void) | undefined;
  #socket: Socket | null = null;
  // In-process (NAPI) transport. Mutually exclusive with #socket: exactly
  // one of connect() / connectNapi() is used per client instance. When set,
  // #call sends request payloads through the addon (no length prefix, no
  // kernel round-trip) and complete response frames arrive via
  // #handleDelivery — no reassembly, #onData never runs.
  #napi: GoNapiAddon | null = null;
  // Row-plane group registry (NAPI row mode): (reqID, groupID) → column/PK
  // metadata, populated by kind-2 deliveries and cleared when the owning
  // RPC settles.
  readonly #rowGroups = new RowGroupRegistry();
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
      /** NAPI row plane: invoked per decoded row record for this id. */
      onRow?: (change: RowChange) => void;
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
  // socket is writable (REVIEW-final MED-TS-1). #drainResolve is the escape
  // hatch (A5, scale review): a socket that DIES under backpressure never
  // emits 'drain', so transport teardown must resolve the promise manually
  // — otherwise callers already parked on it hang forever, and a stale
  // non-null #drainPromise after close()+reconnect gated every future call
  // on a promise nothing could ever resolve (a total client wedge that
  // SURVIVED sidecar restart).
  #drainPromise: Promise<void> | null = null;
  #drainResolve: (() => void) | null = null;
  /**
   * IDs that recently timed out. Late responses for them must be dropped,
   * not routed to a freshly-issued same-ID call (REVIEW-final LOW-TS-2).
   * Bounded eviction in #nextId so the set doesn't grow unbounded.
   */
  #recentlyTimedOut = new Set<number>();

  constructor(
    socketPath = '/tmp/go-ivm.sock',
    options?: {
      onLog?: (level: 'info' | 'warn' | 'error', msg: string, err?: unknown) => void;
      onFatal?: (err: Error) => void;
    },
  ) {
    this.#socketPath = socketPath;
    this.#onLog = options?.onLog;
    this.#onFatal = options?.onFatal;
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
        this.#releaseDrainWaiters();
      });
    });
  }

  /**
   * Attach the in-process (NAPI) transport instead of a socket. The caller
   * owns the addon lifecycle (start/shutdown — typically SidecarManager in
   * 'napi' mode); this just wires deliveries into the client's dispatch.
   * Call the addon's start() with this client's {@link handleNapiDelivery}
   * BEFORE issuing RPCs.
   */
  connectNapi(addon: GoNapiAddon): void {
    if (this.#socket) {
      throw new Error('connectNapi: socket transport already connected');
    }
    this.#napi = addon;
  }

  /**
   * Release callers parked on the byte-level backpressure gate (A5): the
   * socket died (or the client closed) before 'drain' fired, so the promise
   * must be resolved manually — woken callers then fail fast at the
   * transport checks. Clears the field FIRST so a woken caller re-entering
   * #acquireSlot doesn't re-await a dead gate.
   */
  #releaseDrainWaiters(): void {
    const resolve = this.#drainResolve;
    this.#drainPromise = null;
    this.#drainResolve = null;
    resolve?.();
  }

  /**
   * Terminal in-process transport failure (A2). Mirrors the socket 'close'
   * cleanup: mark the transport dead FIRST so concurrent #call invocations
   * fail fast, reject every pending RPC (their responses can never arrive —
   * without this they'd each burn a full timeout during the fatal-exit
   * window), release all backpressure waiters, then notify the embedder
   * exactly once via onFatal.
   */
  #napiFatal(err: Error): void {
    if (!this.#napi) return; // already dead (latch) or never connected
    this.#napi = null;
    for (const [, p] of this.#pending) {
      if (p.timer) clearTimeout(p.timer);
      p.reject(err);
    }
    this.#pending.clear();
    const globalWaiters = this.#slotWaiters;
    this.#slotWaiters = [];
    for (const w of globalWaiters) w();
    const perGroup = this.#perGroupWaiters;
    this.#perGroupWaiters = new Map();
    for (const waiters of perGroup.values()) for (const w of waiters) w();
    this.#perGroupInFlight.clear();
    this.#log('error', `napi transport fatal: ${err.message}`, err);
    this.#onFatal?.(err);
  }

  /**
   * The delivery callback to register with the addon's start(). Exposed as
   * a bound method (not wired inside connectNapi) because goivm_start must
   * be called exactly once per process while GoIVMClient instances may be
   * recreated — the owner re-points the live callback at the current client.
   */
  get handleNapiDelivery(): (kind: number, payload: Buffer) => void {
    return (kind, payload) => this.#handleDelivery(kind, payload);
  }

  /** Close the connection. */
  close(): void {
    const socket = this.#socket;
    this.#socket = null;
    this.#napi = null;
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
    // A5: without this, a close() during byte-level backpressure left a
    // stale #drainPromise that gated EVERY call after reconnect — forever.
    this.#releaseDrainWaiters();
  }

  isConnected(): boolean {
    return this.#socket !== null || this.#napi !== null;
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
      // A1: forward the group so heavyweight RPCs take PER-GROUP fairness
      // slots. Pre-fix every wrapper except advanceToHeadStream bucketed
      // under GLOBAL_KEY — ONE shared 16-slot cap for all CGs' init/
      // hydrate/advance traffic, so 16 slow CGs head-blocked every other
      // CG on the worker (the per-group cap existed but was dead code).
      {timeoutMs: opts?.timeoutMs ?? 120_000, clientGroupID: opts?.clientGroupID ?? clientGroupID},
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
      {timeoutMs: opts?.timeoutMs ?? 120_000, clientGroupID: opts?.clientGroupID ?? clientGroupID},
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
      {timeoutMs: opts?.timeoutMs ?? 60_000, clientGroupID: opts?.clientGroupID ?? clientGroupID},
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
      {timeoutMs: opts?.timeoutMs ?? 120_000, clientGroupID: opts?.clientGroupID ?? clientGroupID},
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
    onResult: (r: {queryID: string; changes: RowChange[]; timingMs: number | undefined; final?: boolean; chunkIndex?: number}) => void,
    opts?: CallOptions & {chunked?: boolean; rowMode?: boolean},
  ): Promise<void> {
    // Row mode requires the in-process transport; degrades to frames on
    // the socket (see advanceStream).
    const rowMode = opts?.rowMode === true && this.#napi !== null;
    const handler = createHydrateStreamAccumulator(onResult, {
      chunked: opts?.chunked ?? false,
      rowMode,
    });
    await this.#call(
      'addQueriesStream',
      rowMode
        ? {clientGroupID, queries, initEpoch, rowMode: true}
        : {clientGroupID, queries, initEpoch},
      {
        timeoutMs: opts?.timeoutMs ?? 120_000,
        clientGroupID: opts?.clientGroupID ?? clientGroupID, // A1: per-group fairness
        onPartial: handler.onFrame,
        ...(rowMode ? {onRow: handler.onRow} : {}),
      },
    );
    handler.finish();
  }

  /**
   * Pull-mode batch hydrate (ABI v3, DESIGN-duplex-streaming): returns an
   * AsyncIterableIterator over per-delivery entries — one row-bearing entry
   * per Go-side gated delivery (chunkSize=1 ⇒ one row), plus each query's
   * ungated terminal `final` entry. Go produces ONLY as this iterator is
   * consumed: the opening window W rides the request (`pullWindow`), the
   * iterator grants top-ups at the low-water mark (W/2) as entries are
   * served, and `return()`/`throw()` cancels the Go producer mid-stream
   * (cursor close, pool-reader release) via goivm_stream_cancel.
   *
   * Requires the NAPI transport (throws otherwise — callers gate on
   * goPullHydrate(config), which implies transport=napi + rowMode).
   *
   * Timeout: pull duration is CONSUMER-driven, so the blanket RPC timeout
   * defaults OFF (a legit slow consumer must not fire it). Liveness is
   * bounded on the Go side instead: no grants for
   * GO_IVM_PULL_IDLE_TIMEOUT_SEC (60s default) auto-cancels the stream
   * (terminal error frame → this iterator throws), and a host death
   * sweeps every pending RPC (#napiFatal).
   *
   * Error semantics (I3 all-or-nothing): any RPC rejection — including the
   * Go-side idle-timeout cancel — surfaces as a throw from next(); the
   * caller abandons the whole hydrate exactly like an addQueriesStream
   * rejection today. After the CONSUMER cancels via return()/throw(), the
   * RPC's own bookkeeping rejection ("cancelled by consumer") is swallowed:
   * the consumer already knows.
   */
  addQueriesStreamPull(
    clientGroupID: string,
    queries: {queryID: string; ast: unknown}[],
    initEpoch: number,
    opts?: CallOptions & {window?: number},
  ): AsyncIterableIterator<{
    queryID: string;
    changes: RowChange[];
    timingMs: number | undefined;
    final: boolean;
    chunkIndex?: number | undefined;
  }> {
    const napi = this.#napi;
    if (!napi) {
      throw new Error('addQueriesStreamPull requires the NAPI transport');
    }
    const window = Math.max(1, Math.floor(opts?.window ?? 64));
    const lowWater = Math.max(1, Math.floor(window / 2));

    type Entry = {
      queryID: string;
      changes: RowChange[];
      timingMs: number | undefined;
      final: boolean;
      chunkIndex?: number | undefined;
    };
    const buffered: Entry[] = [];
    let wake: (() => void) | null = null;
    let done = false;
    let error: Error | null = null;
    let reqID: number | null = null;
    // Credit accounting in DELIVERY units (one row-bearing delivery == one
    // Go-side gate acquire — a fallback frame carrying rows costs one, just
    // like a row record; final/empty frames are free on both sides).
    let granted = window; // the opening window rides the request params
    let consumed = 0;
    let closed = false; // consumer called return()/throw()

    const notify = () => {
      const w = wake;
      wake = null;
      w?.();
    };

    // Reuse the accumulator in chunked+rowMode: every record and every
    // frame becomes exactly one onResult entry, preserving wire order and
    // the chunk-order/duplicate-final guards.
    const handler = createHydrateStreamAccumulator(
      r => {
        buffered.push({
          queryID: r.queryID,
          changes: r.changes,
          timingMs: r.timingMs,
          final: r.final ?? true,
          chunkIndex: r.chunkIndex,
        });
        notify();
      },
      {chunked: true, rowMode: true},
    );

    void this.#call(
      'addQueriesStream',
      {clientGroupID, queries, initEpoch, rowMode: true, pullMode: true, pullWindow: window},
      {
        timeoutMs: opts?.timeoutMs ?? 0,
        clientGroupID: opts?.clientGroupID ?? clientGroupID, // A1: per-group fairness
        onPartial: handler.onFrame,
        onRow: handler.onRow,
        onStreamOpen: id => {
          reqID = id;
        },
      },
    ).then(
      () => {
        // Preserve the accumulator's orphan guard: "done" with a query
        // that never saw its final chunk is a wire bug, same as the
        // non-pull path (handler.finish() throws → iterator throws).
        try {
          handler.finish();
          done = true;
        } catch (e) {
          if (!closed) {
            error = e instanceof Error ? e : new Error(String(e));
          } else {
            done = true;
          }
        }
        notify();
      },
      (e: unknown) => {
        if (closed) {
          // Consumer already cancelled; the rejection is Go's bookkeeping
          // error frame for OUR cancel — not news. Settle quietly.
          done = true;
        } else {
          error = e instanceof Error ? e : new Error(String(e));
        }
        notify();
      },
    );

    const cancel = () => {
      if (closed) {
        return;
      }
      closed = true;
      buffered.length = 0;
      if (reqID !== null) {
        // Unparks the Go producer → fetch-range break → operator unwind.
        // Idempotent; unknown reqID (RPC already settled) is a no-op.
        napi.streamCancel(reqID);
      }
    };

    const iterator: AsyncIterableIterator<Entry> = {
      [Symbol.asyncIterator]() {
        return this;
      },
      next: async (): Promise<IteratorResult<Entry>> => {
        for (;;) {
          if (closed) {
            return {value: undefined, done: true};
          }
          const entry = buffered.shift();
          if (entry !== undefined) {
            if (entry.changes.length > 0) {
              consumed += 1;
              const outstanding = granted - consumed;
              if (outstanding <= lowWater && reqID !== null && !done && error === null) {
                const topUp = window - outstanding;
                granted += topUp;
                napi.streamCredit(reqID, topUp);
              }
            }
            return {value: entry, done: false};
          }
          if (error !== null) {
            const e = error;
            error = null;
            closed = true;
            throw e;
          }
          if (done) {
            closed = true;
            return {value: undefined, done: true};
          }
          await new Promise<void>(resolve => {
            wake = resolve;
          });
        }
      },
      return: (): Promise<IteratorResult<Entry>> => {
        cancel();
        return Promise.resolve({value: undefined, done: true});
      },
      throw: (e?: unknown): Promise<IteratorResult<Entry>> => {
        cancel();
        return Promise.reject(e instanceof Error ? e : new Error(String(e)));
      },
    };
    return iterator;
  }

  // Single-query hydrate over the STREAMING path. Identical return contract to
  // {@link addQuery}, but routes through addQueriesStream so the result is
  // chunked on the Go side (byte-aware, softChunkBytes ~8MB) instead of shipped
  // as one unbounded msgpack frame. A wide-result query (e.g. allTickets) can
  // encode to >MAX_FRAME_SIZE, which the receive loop SKIPS — orphaning the RPC
  // into a 60s timeout that freezes the client group. Non-streaming addQuery
  // has no such bound; this is its safe replacement for the hydrate hot path.
  async addQueryStream(
    clientGroupID: string,
    queryID: string,
    ast: unknown,
    initEpoch: number,
    opts?: CallOptions & {rowMode?: boolean},
  ): Promise<HydrateResult> {
    let result: HydrateResult | undefined;
    await this.addQueriesStream(
      clientGroupID,
      [{queryID, ast}],
      initEpoch,
      r => {
        result = {changes: r.changes, timingMs: r.timingMs};
      },
      opts,
    );
    if (result === undefined) {
      // The Go engine always emits a terminal Final frame per query (even
      // empty), so onResult must have fired exactly once. Defensive.
      throw new Error(`addQueryStream: no result frame for query ${queryID}`);
    }
    return result;
  }

  /** Remove a query pipeline from a client group's engine. */
  async removeQuery(clientGroupID: string, queryID: string, initEpoch: number, opts?: CallOptions): Promise<void> {
    await this.#call('removeQuery', {clientGroupID, queryID, initEpoch}, {
      ...opts,
      clientGroupID: opts?.clientGroupID ?? clientGroupID,
    });
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
    opts?: CallOptions & {rowMode?: boolean},
  ): Promise<AdvanceResult> {
    // Row mode requires the in-process transport (per-row records ride the
    // addon's delivery queue); on the socket it silently degrades to the
    // ordinary frame path — same result, chunked framing.
    const rowMode = opts?.rowMode === true && this.#napi !== null;
    const handler = createAdvanceStreamAccumulator({rowMode});
    await this.#call(
      'advanceStream',
      rowMode
        ? {clientGroupID, changes, initEpoch, rowMode: true}
        : {clientGroupID, changes, initEpoch},
      {
        timeoutMs: opts?.timeoutMs ?? 120_000,
        clientGroupID: opts?.clientGroupID ?? clientGroupID, // A1: per-group fairness
        onPartial: handler.onFrame,
        ...(rowMode ? {onRow: handler.onRow} : {}),
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
   * protocolRev 9 sidecar (exact-match enforced at connect — sidecar-manager
   * EXPECTED_PROTOCOL_REV; the RPC itself dates to rev 7) with
   * GO_IVM_ADVANCE_TO_HEAD=true + table mode.
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
      {...opts, clientGroupID: opts?.clientGroupID ?? clientGroupID},
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
   * Requires a protocolRev 9 sidecar (exact-match enforced at connect —
   * sidecar-manager EXPECTED_PROTOCOL_REV; this RPC dates to rev 8) with
   * GO_IVM_ADVANCE_TO_HEAD=true, GO_IVM_ADVANCE_DRIVE=true, + table mode.
   * Same defensive invariants as {@link advanceStream}: chunk-order gaps
   * throw; missing terminal `final:true` throws; drift on the final frame
   * re-throws as a {@link DriftError}.
   */
  async advanceToHeadStream(
    clientGroupID: string,
    initEpoch: number,
    appID: string,
    opts?: CallOptions & {rowMode?: boolean},
  ): Promise<AdvanceToHeadResult> {
    // Row mode requires the in-process transport (per-row records ride the
    // addon's delivery queue); on the socket it silently degrades to the
    // ordinary frame path — same result, chunked framing.
    const rowMode = opts?.rowMode === true && this.#napi !== null;
    const handler = createAdvanceToHeadStreamAccumulator({rowMode});
    const base = appID
      ? {clientGroupID, initEpoch, appID}
      : {clientGroupID, initEpoch};
    await this.#call(
      'advanceToHeadStream',
      rowMode ? {...base, rowMode: true} : base,
      {
        timeoutMs: opts?.timeoutMs ?? 120_000,
        // Forward the group for in-flight fairness (every CG-scoped wrapper
        // does this since A1 — one group can't starve others — CROSS-2).
        clientGroupID: opts?.clientGroupID ?? clientGroupID,
        onPartial: handler.onFrame,
        ...(rowMode ? {onRow: handler.onRow} : {}),
      },
    );
    return handler.finish();
  }

  /**
   * Destroy a client group's engine.
   * Call when the client disconnects to free memory.
   *
   * initEpoch must match the sidecar's current epoch for the cgID — a
   * stale destroy from a torn-down view-syncer must not tear down the
   * live successor's engine (D2 fix, protocolRev 9+). The sidecar
   * rejects with rpcCodeStaleInitEpoch; callers already catch and
   * best-effort-ignore errors from destroy.
   */
  async destroy(
    clientGroupID: string,
    initEpoch: number,
    opts?: CallOptions,
  ): Promise<void> {
    await this.#call('destroy', {clientGroupID, initEpoch}, {
      ...opts,
      clientGroupID: opts?.clientGroupID ?? clientGroupID,
    });
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
      {...opts, clientGroupID: opts?.clientGroupID ?? clientGroupID},
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
    const r = await this.#call('pipelineCount', {clientGroupID}, {
      ...opts,
      clientGroupID: opts?.clientGroupID ?? clientGroupID,
    });
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
    const napi = this.#napi;
    if (!socket && !napi) {
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
        // Row-plane group metadata is per-RPC; free it when the RPC settles
        // (group ids are only unique within a request).
        this.#rowGroups.clearRequest(id);
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
        onRow?: (change: RowChange) => void;
      } = {
        method,
        resolve: wrappedResolve,
        reject: wrappedReject,
        timer,
      };
      if (opts?.onPartial) entry.onPartial = opts.onPartial;
      if (opts?.onRow) entry.onRow = opts.onRow;
      this.#pending.set(id, entry);
      // Pull streams learn their reqID here (top-up credits + cancel).
      // After #pending.set so a synchronous throw inside the callback
      // cannot orphan the entry; before send so the consumer can never
      // observe a delivery for an id it doesn't know yet.
      opts?.onStreamOpen?.(id);

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
      // NAPI transport: hand the raw payload to the addon — no length
      // prefix (goivm_send frames internally), no kernel buffer, no drain
      // machinery (the Go-side send queue is unbounded like Node's
      // userspace socket buffering; memory is bounded by the in-flight
      // slot caps above).
      if (napi) {
        const rc = napi.send(payload);
        if (rc !== 0) {
          // rc != 0 means the Go host's receive loop is gone (markClosed) —
          // the engine is dead for EVERY pending and future RPC, not just
          // this one. Reject this call with the specific error, then latch
          // the whole transport dead and notify the embedder (→ fatalExit;
          // see #napiFatal / A2).
          const err = new Error(`goivm_send failed: rc=${rc} (host closed?)`);
          wrappedReject(err);
          this.#napiFatal(err);
        }
        return;
      }
      // Socket transport. Re-check under the closure: TS can't narrow the
      // outer `socket || napi` guard across the Promise executor.
      if (!socket) {
        wrappedReject(new Error('Not connected'));
        return;
      }
      const frame = Buffer.allocUnsafe(4 + payload.length);
      frame.writeUInt32BE(payload.length, 0);
      payload.copy(frame, 4);

      const ok = socket.write(frame);
      if (!ok && !this.#drainPromise) {
        // Byte-level backpressure: gate subsequent writes until the kernel
        // buffer drains. Subsequent #call invocations await this promise
        // in #acquireSlot (REVIEW-final MED-TS-1). #drainResolve lets the
        // teardown paths release parked callers when the socket dies
        // instead of draining (A5).
        this.#drainPromise = new Promise<void>(resolve => {
          this.#drainResolve = resolve;
          socket.once('drain', () => {
            this.#drainPromise = null;
            this.#drainResolve = null;
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
      this.#dispatchResponsePayload(payload);
    }

    // Compact periodically so the underlying chunk's memory can be released
    // once we've consumed at least half of it.
    if (this.#recvOffset > 0 && this.#recvOffset * 2 >= this.#recvBuf.length) {
      this.#recvBuf = this.#recvBuf.subarray(this.#recvOffset);
      this.#recvOffset = 0;
    }
  }

  // Decode + route ONE complete response frame payload. Shared by both
  // transports: #onData (socket) calls it per reassembled frame; the NAPI
  // path calls it per kind-1 delivery (frames arrive whole — no prefix, no
  // reassembly). Extracted verbatim from the #onData loop body; `return`
  // here corresponds to the loop's `continue`.
  #dispatchResponsePayload(payload: Buffer): void {
      let resp: RPCResponse;
      try {
        resp = unpack(payload) as RPCResponse;
      } catch (e) {
        this.#log('warn', `failed to decode response frame: ${(e as Error).message}`);
        return;
      }

      // Coerce id to Number: msgpackr decodes Go's uint64 (9-byte non-compact
      // encoding) as BigInt, which won't match the Number key stored in
      // #pending when the request was sent.
      const respId = typeof resp.id === 'bigint' ? Number(resp.id) : resp.id;
      const pending = this.#pending.get(respId);
      if (!pending) return;

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
        } else if (resp.error.code === RPC_CODE_DATA_ERROR) {
          // Permanent data error: the sidecar hit bad replica data it cannot
          // represent (non-JSON in a json/array column, int beyond
          // MAX_SAFE_INTEGER, cross-type compare). Surface as
          // PermanentDataError so #classifyGoPrimaryAdvanceError TEARS DOWN
          // the CG (like TS-native's UnsupportedValueError throw) instead of
          // escalating to a pipeline reset — a reset re-reads the same bad
          // row and re-panics forever (reset storm).
          pending.reject(new PermanentDataError(resp.error.message));
        } else {
          pending.reject(new Error(`RPC error ${resp.error.code}: ${resp.error.message}`));
        }
        return;
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
          return;
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
        return;
      }
      pending.resolve(resp.result);
  }

  // Route one delivery from the NAPI addon's ordered queue. Kind 1 =
  // complete msgpack response frame; kinds 2/3 = row-plane records; kind 4
  // = host death (the Go pump died — fail everything, fatal the worker).
  // The try/catch mirrors #dispatchResponsePayload's onPartial guard: a
  // record-decode or onRow throw rejects ONLY the owning RPC, never the
  // process (the addon invokes this from the TSFN callback — an escaped
  // exception would be an uncaughtException).
  #handleDelivery(kind: number, payload: Buffer): void {
    if (kind === DELIVERY_KIND_FRAME) {
      // Containment (scale review): #dispatchResponsePayload guards its
      // msgpack decode internally, but a post-decode throw (e.g. an error
      // branch stringifying exotic values) escaped this TSFN callback as an
      // uncaughtException — a worker crash for one bad frame. There may be
      // no attributable RPC (the reqID lives INSIDE the body), so log and
      // drop; the owning RPC, if any, fails via its timeout instead of
      // taking the worker down.
      try {
        this.#dispatchResponsePayload(payload);
      } catch (err) {
        this.#log(
          'error',
          `NAPI frame dispatch error: ${(err as Error).message}`,
          err,
        );
      }
      return;
    }
    // Host death (A3): the in-process host's pump terminated unexpectedly
    // — no response can ever arrive again. Sweep pending RPCs and notify
    // the embedder (→ fatalExit; a dead host cannot be restarted
    // in-process). MUST be checked BEFORE the late-record reqID guard
    // below: the payload is a UTF-8 reason string, not a record — its
    // first 8 bytes are not a reqID, so the guard would silently drop it.
    if (kind === DELIVERY_KIND_HOST_DEATH) {
      const reason =
        payload.length > 0 ? payload.toString('utf8') : 'no reason given';
      this.#napiFatal(new Error(`go-ivm in-process host died: ${reason}`));
      return;
    }
    // Late-record guard (REVIEW-napi-transport MED): records for an RPC that
    // already settled (timeout/close cleared #pending AND the group registry)
    // must drop SILENTLY, BEFORE touching the registry. Two failure modes
    // otherwise: a late kind-3 decode throws "unknown group" → one error log
    // per late row on the JS thread — thousands of logs during the exact
    // overload that caused the timeout; and a late kind-2 def RE-ADDS a
    // registry entry after clearRequest already ran — a permanent per-request
    // leak (nothing ever clears it again). Every record's first 8 bytes are
    // the f64 reqID; Buffer.readDoubleLE reads it without allocating a
    // DataView (perf #3 — this runs on EVERY record, the hot path).
    if (payload.length >= 8) {
      const reqID = payload.readDoubleLE(0);
      if (!this.#pending.has(reqID)) return;
    }
    try {
      if (kind === DELIVERY_KIND_GROUP_DEF) {
        this.#rowGroups.addGroupDef(payload);
        return;
      }
      if (kind === DELIVERY_KIND_ROW) {
        const {reqID, change} = this.#rowGroups.decodeRow(payload);
        const pending = this.#pending.get(reqID);
        if (!pending) return; // timed-out / settled RPC: drop late rows
        if (!pending.onRow) {
          throw new Error(
            `row record for RPC ${reqID} (${pending.method}) which did not opt into rowMode`,
          );
        }
        pending.onRow(change);
        return;
      }
      this.#log('warn', `unknown NAPI delivery kind ${kind} (${payload.length} bytes)`);
    } catch (err) {
      // Attribute to the owning RPC when identifiable (first 8 bytes of
      // every record are the f64 reqID); otherwise just log.
      let reqID: number | undefined;
      if (payload.length >= 8) {
        reqID = payload.readDoubleLE(0);
      }
      const pending = reqID !== undefined ? this.#pending.get(reqID) : undefined;
      if (pending && reqID !== undefined) {
        if (pending.timer) clearTimeout(pending.timer);
        this.#pending.delete(reqID);
        pending.reject(
          err instanceof Error ? err : new Error(`row delivery failed: ${String(err)}`),
        );
      } else {
        this.#log('error', `NAPI delivery error (kind=${kind}): ${(err as Error).message}`, err);
      }
    }
  }
}
