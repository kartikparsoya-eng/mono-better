// Pure unit tests for the row-record decoder (napi-records.ts) — no addon,
// no dylib, runs everywhere. Buffers are hand-crafted to the layout locked
// by go-ivm cmd/sidecar/rowrecord.go, covering the paths a HEALTHY Go side
// never produces (so the E2E suites can't reach them): the i64 tag's
// BigInt-vs-Number split, malformed records (trailing bytes, unknown tag),
// protocol violations (row before def, duplicate def), and registry
// lifecycle (clearRequest).

import {describe, expect, test} from 'vitest';
import {RowGroupRegistry, decodeGroupDef, decodeRowRecord} from './napi-records.ts';

class Writer {
  #chunks: number[] = [];

  f64(v: number): this {
    const b = new ArrayBuffer(8);
    new DataView(b).setFloat64(0, v, true);
    this.#chunks.push(...new Uint8Array(b));
    return this;
  }

  i64(v: bigint): this {
    const b = new ArrayBuffer(8);
    new DataView(b).setBigInt64(0, v, true);
    this.#chunks.push(...new Uint8Array(b));
    return this;
  }

  u32(v: number): this {
    this.#chunks.push(v & 0xff, (v >>> 8) & 0xff, (v >>> 16) & 0xff, (v >>> 24) & 0xff);
    return this;
  }

  u16(v: number): this {
    this.#chunks.push(v & 0xff, (v >>> 8) & 0xff);
    return this;
  }

  u8(v: number): this {
    this.#chunks.push(v & 0xff);
    return this;
  }

  shortStr(s: string): this {
    const bytes = new TextEncoder().encode(s);
    this.u16(bytes.length);
    this.#chunks.push(...bytes);
    return this;
  }

  buf(): Buffer {
    return Buffer.from(this.#chunks);
  }
}

// groupDef: [f64 reqID][u32 groupID][str queryID][str table]
//           [u16 ncols]([str col])*[u16 npk]([u16 pkIdx][str pkCol])*
function makeDef(reqID: number, groupID: number, cols: string[], pk: string[]): Buffer {
  const w = new Writer().f64(reqID).u32(groupID).shortStr('q1').shortStr('t1');
  w.u16(cols.length);
  for (const c of cols) w.shortStr(c);
  w.u16(pk.length);
  for (const p of pk) {
    w.u16(Math.max(cols.indexOf(p), 0xffff & -1) === -1 ? 0xffff : cols.indexOf(p));
    w.shortStr(p);
  }
  return w.buf();
}

describe('napi-records decoder (pure)', () => {
  test('i64 tag: safe values downcast to Number, unsafe stay BigInt', () => {
    const reg = new RowGroupRegistry();
    reg.addGroupDef(makeDef(7, 0, ['id', 'big'], ['id']));
    const mk = (v: bigint) =>
      new Writer()
        .f64(7)
        .u32(0)
        .u8(0) // add
        .u8(5) // str tag for id
        .u32(2)
        .u8(0x61)
        .u8(0x31) // "a1"
        .u8(4) // i64 tag
        .i64(v)
        .buf();

    // 2^53-1: exact in f64 → must come out as a plain number, exactly what
    // the msgpack frame plane would deliver.
    const safe = reg.decodeRow(mk(9007199254740991n));
    expect(safe.change.row).toEqual({id: 'a1', big: 9007199254740991});
    expect(typeof (safe.change.row as {big: unknown}).big).toBe('number');

    // 2^53+1: NOT exact in f64 → preserved as BigInt (no silent aliasing
    // to the adjacent even integer).
    const unsafe = reg.decodeRow(mk(9007199254740993n));
    expect((unsafe.change.row as {big: unknown}).big).toBe(9007199254740993n);

    expect(reg.decodeRow(mk(-9007199254740993n)).change.row).toMatchObject({
      big: -9007199254740993n,
    });
  });

  test('row record with trailing bytes throws (truncation/corruption guard)', () => {
    const reg = new RowGroupRegistry();
    reg.addGroupDef(makeDef(3, 0, ['id'], ['id']));
    const w = new Writer().f64(3).u32(0).u8(0).u8(0 /* null id */).u8(0xee); // extra byte
    expect(() => reg.decodeRow(w.buf())).toThrow(/trailing bytes/);
  });

  test('unknown value tag throws with offset context', () => {
    const reg = new RowGroupRegistry();
    reg.addGroupDef(makeDef(4, 0, ['id'], ['id']));
    const w = new Writer().f64(4).u32(0).u8(0).u8(99); // tag 99 undefined
    expect(() => reg.decodeRow(w.buf())).toThrow(/unknown value tag 99/);
  });

  test('row before its groupDef throws (delivery-order violation)', () => {
    const reg = new RowGroupRegistry();
    const w = new Writer().f64(5).u32(0).u8(0).u8(0);
    expect(() => reg.decodeRow(w.buf())).toThrow(/unknown group/);
  });

  test('duplicate groupDef throws (Go must intern)', () => {
    const reg = new RowGroupRegistry();
    reg.addGroupDef(makeDef(6, 0, ['id'], ['id']));
    expect(() => reg.addGroupDef(makeDef(6, 0, ['id'], ['id']))).toThrow(/duplicate groupDef/);
  });

  test('remove record: PK values in PK order, rowKey only', () => {
    const reg = new RowGroupRegistry();
    reg.addGroupDef(makeDef(8, 1, ['id', 'n'], ['id']));
    const w = new Writer().f64(8).u32(1).u8(1 /* remove */).u8(5).u32(2).u8(0x7a).u8(0x39); // "z9"
    const {change} = reg.decodeRow(w.buf());
    expect(change).toMatchObject({type: 1, queryID: 'q1', table: 't1', rowKey: {id: 'z9'}});
    expect((change as {row?: unknown}).row).toBeUndefined();
  });

  test('groupIDs are scoped per reqID; clearRequest frees exactly one request', () => {
    const reg = new RowGroupRegistry();
    reg.addGroupDef(makeDef(10, 0, ['id'], ['id']));
    reg.addGroupDef(makeDef(11, 0, ['id'], ['id'])); // same groupID, other req — legal
    expect(reg.size).toBe(2);
    reg.clearRequest(10);
    expect(reg.size).toBe(1);
    // Req 10's group is gone; a late row for it now throws (and the client
    // drops it via the no-pending path before ever reaching decode).
    const w = new Writer().f64(10).u32(0).u8(0).u8(0);
    expect(() => reg.decodeRow(w.buf())).toThrow(/unknown group/);
    // Req 11 unaffected.
    const ok = new Writer().f64(11).u32(0).u8(0).u8(2 /* true */);
    expect(reg.decodeRow(ok.buf()).change.row).toEqual({id: true});
  });

  test('decodeGroupDef rejects trailing bytes', () => {
    const good = makeDef(12, 0, ['id'], ['id']);
    expect(decodeGroupDef(good).queryID).toBe('q1');
    const bad = Buffer.concat([good, Buffer.from([0x00])]);
    expect(() => decodeGroupDef(bad)).toThrow(/trailing bytes/);
  });

  test('decodeRowRecord: unicode column values survive u32-length strings', () => {
    const reg = new RowGroupRegistry();
    reg.addGroupDef(makeDef(13, 0, ['id', 'label'], ['id']));
    const label = '🎯-héllo-\u0000-🚀';
    const labelBytes = new TextEncoder().encode(label);
    const w = new Writer().f64(13).u32(0).u8(0).u8(5).u32(1).u8(0x78); // id "x"
    w.u8(5).u32(labelBytes.length);
    const rec = Buffer.concat([w.buf(), Buffer.from(labelBytes)]);
    const {change} = decodeRowRecord(rec, (r, g) =>
      r === 13 && g === 0
        ? {reqID: 13, groupID: 0, queryID: 'q1', table: 't1', cols: ['id', 'label'], pk: ['id']}
        : undefined,
    );
    expect((change.row as {label: string}).label).toBe(label);
  });
});
