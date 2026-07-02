// Row-plane record parsing for the in-process (NAPI) Go transport.
//
// When a streaming RPC opts into rowMode, the Go side ships each RowChange
// as ONE flat little-endian binary record (kind 2 = groupDef, kind 3 = row)
// through the addon's ordered delivery queue, instead of batching rows into
// msgpack partial frames. This module decodes those records with a DataView
// and assembles the SAME RowChange objects decodePositionalChanges produces
// — no msgpackr on the row hot path.
//
// The layout is defined (and locked by round-trip tests) on the Go side in
// go-ivm cmd/sidecar/rowrecord.go; its decode mirror lives in
// rowrecord_test.go. Any change must update both in lockstep:
//
//	groupDef: [f64 reqID][u32 groupID][str queryID][str table]
//	          [u16 ncols]([str col])*[u16 npk]([u16 pkIdx][str pkCol])*
//	row:      [f64 reqID][u32 groupID][u8 changeType][values...]
//	          changeType 1 (remove): npk values in PK order
//	          else:                  ncols values in groupDef column order
//	value:    [u8 tag] 0=null 1=false 2=true 3=f64(8B) 4=i64(8B)
//	          5=string(u32+bytes) 6=msgpack blob(u32+bytes)
//	str:      u16 len + UTF-8 bytes (identifiers)
//
// All integers little-endian. reqID is f64 (RPC ids are JS numbers).

import type {RowChange} from './go-ivm-client.ts';
import {unpack} from './go-ivm-client.ts';

/** Delivery kinds on the addon's single ordered queue. */
export const DELIVERY_KIND_FRAME = 1;
export const DELIVERY_KIND_GROUP_DEF = 2;
export const DELIVERY_KIND_ROW = 3;

const VAL_NULL = 0;
const VAL_FALSE = 1;
const VAL_TRUE = 2;
const VAL_F64 = 3;
const VAL_I64 = 4;
const VAL_STR = 5;
const VAL_BLOB = 6;

const utf8 = new TextDecoder();

export type RowGroupDef = {
  reqID: number;
  groupID: number;
  queryID: string;
  table: string;
  cols: string[];
  pk: string[];
};

class RecordReader {
  readonly #view: DataView;
  readonly #bytes: Uint8Array;
  off = 0;

  constructor(buf: Buffer) {
    this.#bytes = buf;
    this.#view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  }

  get length(): number {
    return this.#bytes.length;
  }

  f64(): number {
    const v = this.#view.getFloat64(this.off, true);
    this.off += 8;
    return v;
  }

  u32(): number {
    const v = this.#view.getUint32(this.off, true);
    this.off += 4;
    return v;
  }

  u16(): number {
    const v = this.#view.getUint16(this.off, true);
    this.off += 2;
    return v;
  }

  u8(): number {
    return this.#view.getUint8(this.off++);
  }

  shortStr(): string {
    const n = this.u16();
    const s = utf8.decode(this.#bytes.subarray(this.off, this.off + n));
    this.off += n;
    return s;
  }

  longBytes(): Uint8Array {
    const n = this.u32();
    const b = this.#bytes.subarray(this.off, this.off + n);
    this.off += n;
    return b;
  }

  i64(): number | bigint {
    const v = this.#view.getBigInt64(this.off, true);
    this.off += 8;
    // Downcast to Number when exact — matches msgpackr's int64 handling
    // (useBigIntExtension:false decodes safe ints as numbers), so row-plane
    // values are indistinguishable from frame-decoded ones downstream.
    return v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(v)
      : v;
  }
}

/** Decode a kind-2 groupDef record. */
export function decodeGroupDef(payload: Buffer): RowGroupDef {
  const r = new RecordReader(payload);
  const reqID = r.f64();
  const groupID = r.u32();
  const queryID = r.shortStr();
  const table = r.shortStr();
  const ncols = r.u16();
  const cols: string[] = [];
  for (let i = 0; i < ncols; i++) cols.push(r.shortStr());
  const npk = r.u16();
  const pk: string[] = [];
  for (let i = 0; i < npk; i++) {
    r.u16(); // pk column index (0xFFFF when not a column reference) — names follow
    pk.push(r.shortStr());
  }
  if (r.off !== r.length) {
    throw new Error(`groupDef record: ${r.length - r.off} trailing bytes`);
  }
  return {reqID, groupID, queryID, table, cols, pk};
}

/**
 * Decode a kind-3 row record against its groupDef and assemble the
 * RowChange — identical object shape to decodePositionalChanges: add/edit
 * derive rowKey from the row's PK columns; remove carries rowKey only.
 * Returns the reqID alongside so the client can route to the pending RPC.
 */
export function decodeRowRecord(
  payload: Buffer,
  groupOf: (reqID: number, groupID: number) => RowGroupDef | undefined,
): {reqID: number; change: RowChange} {
  const r = new RecordReader(payload);
  const reqID = r.f64();
  const groupID = r.u32();
  const type = r.u8();
  const def = groupOf(reqID, groupID);
  if (!def) {
    throw new Error(
      `row record references unknown group (reqID=${reqID}, groupID=${groupID}) — ` +
        `groupDef must precede its rows on the delivery queue`,
    );
  }

  const readValue = (): unknown => {
    const tag = r.u8();
    switch (tag) {
      case VAL_NULL:
        return null;
      case VAL_FALSE:
        return false;
      case VAL_TRUE:
        return true;
      case VAL_F64:
        return r.f64();
      case VAL_I64:
        return r.i64();
      case VAL_STR:
        return utf8.decode(r.longBytes());
      case VAL_BLOB:
        return unpack(r.longBytes());
      default:
        throw new Error(`row record: unknown value tag ${tag} at offset ${r.off - 1}`);
    }
  };

  if (type === 1 /* remove */) {
    const rowKey: Record<string, unknown> = {};
    for (const pkCol of def.pk) rowKey[pkCol] = readValue();
    if (r.off !== r.length) {
      throw new Error(`row record (remove): ${r.length - r.off} trailing bytes`);
    }
    return {
      reqID,
      change: {type, queryID: def.queryID, table: def.table, rowKey} as RowChange,
    };
  }

  const row: Record<string, unknown> = {};
  for (const col of def.cols) row[col] = readValue();
  if (r.off !== r.length) {
    throw new Error(`row record: ${r.length - r.off} trailing bytes`);
  }
  const rowKey: Record<string, unknown> = {};
  for (const pkCol of def.pk) rowKey[pkCol] = row[pkCol];
  return {
    reqID,
    change: {type, queryID: def.queryID, table: def.table, rowKey, row} as RowChange,
  };
}

/**
 * Per-connection registry of row groups, keyed (reqID, groupID). The client
 * clears a request's groups when the RPC settles (frees the interned column
 * arrays; group ids are only unique per request).
 */
export class RowGroupRegistry {
  readonly #byReq = new Map<number, Map<number, RowGroupDef>>();

  addGroupDef(payload: Buffer): RowGroupDef {
    const def = decodeGroupDef(payload);
    let groups = this.#byReq.get(def.reqID);
    if (!groups) {
      groups = new Map();
      this.#byReq.set(def.reqID, groups);
    }
    if (groups.has(def.groupID)) {
      throw new Error(
        `duplicate groupDef (reqID=${def.reqID}, groupID=${def.groupID}) — Go must intern`,
      );
    }
    groups.set(def.groupID, def);
    return def;
  }

  decodeRow(payload: Buffer): {reqID: number; change: RowChange} {
    return decodeRowRecord(payload, (reqID, groupID) =>
      this.#byReq.get(reqID)?.get(groupID),
    );
  }

  clearRequest(reqID: number): void {
    this.#byReq.delete(reqID);
  }

  /** Number of requests with live group state (leak check in tests). */
  get size(): number {
    return this.#byReq.size;
  }
}
