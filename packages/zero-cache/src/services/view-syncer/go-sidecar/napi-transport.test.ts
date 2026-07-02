// End-to-end test for the in-process (NAPI) Go transport: GoIVMClient →
// goivm_napi addon → libgoivm (Go c-shared) → engine → TSFN deliveries →
// client dispatch, covering BOTH planes:
//
//   - frame plane: init / loadRows / ping round-trips (msgpack, kind 1)
//   - row plane:   addQueriesStream + advanceStream with rowMode (kinds 2/3)
//
// GATED: requires the out-of-band build artifacts —
//
//   addon:  cd .../go-sidecar/napi && npx node-gyp rebuild
//   dylib:  (go-ivm repo) go build -tags napilib -buildmode=c-shared \
//             -o /tmp/libgoivm.dylib ./cmd/sidecar
//           (override path via GOIVM_TEST_LIB)
//
// Skips cleanly when either is missing. The Go host can only start ONCE per
// process (Go runtimes cannot be unloaded), so all tests share one bridge.

import {existsSync} from 'node:fs';
import {afterAll, describe, expect, test} from 'vitest';
import {GoIVMClient} from './go-ivm-client.ts';
import type {RowChange} from './go-ivm-client.ts';
import {isGoNapiAddonAvailable, loadGoNapiAddon} from './napi/index.ts';

const LIB_PATH =
  process.env.GOIVM_TEST_LIB ??
  (process.platform === 'darwin' ? '/tmp/libgoivm.dylib' : '/tmp/libgoivm.so');

const available = isGoNapiAddonAvailable() && existsSync(LIB_PATH);

describe.skipIf(!available)('NAPI transport (in-process Go engine)', () => {
  const client = new GoIVMClient('unused-socket-path', undefined);
  let started = false;

  function ensureStarted(): GoIVMClient {
    if (!started) {
      const addon = loadGoNapiAddon();
      addon.start(LIB_PATH, client.handleNapiDelivery);
      client.connectNapi(addon);
      started = true;
    }
    return client;
  }

  afterAll(() => {
    // Deliberately NOT calling addon.shutdown(): the vitest worker exits
    // after this file; tearing down the Go host mid-flush can race the
    // TSFN drain. Process exit reclaims everything.
    client.close();
  });

  test('frame plane: init + loadRows round-trip', async () => {
    const c = ensureStarted();
    const {initEpoch} = await c.init('cg-napi', {
      storage: `/tmp/goivm-napi-test-${process.pid}.db`,
      tables: {
        users: {
          columns: {
            id: {type: 'string'},
            name: {type: 'string'},
            age: {type: 'number'},
          },
          primaryKey: ['id'],
        },
      },
    });
    expect(initEpoch).toBeGreaterThan(0);

    await c.loadRows(
      'cg-napi',
      'users',
      [
        {id: 'u1', name: 'alice', age: 30},
        {id: 'u2', name: 'bob', age: 25},
        {id: 'u3', name: 'carol', age: 35},
      ],
      initEpoch,
    );
  });

  test('row plane: addQueriesStream rowMode delivers per-row records', async () => {
    const c = ensureStarted();
    const results: {queryID: string; changes: RowChange[]; final?: boolean}[] = [];
    await c.addQueriesStream(
      'cg-napi',
      [{queryID: 'q-all', ast: {table: 'users', orderBy: [['id', 'asc']]}}],
      1,
      r => results.push(r),
      {rowMode: true},
    );
    // Non-chunked accumulate-until-final: exactly one onResult per query.
    expect(results).toHaveLength(1);
    const r = results[0];
    expect(r.queryID).toBe('q-all');
    expect(r.changes).toHaveLength(3);
    expect(r.changes.map(ch => (ch.row as {name: string}).name)).toEqual([
      'alice',
      'bob',
      'carol',
    ]);
    // Row-plane assembly matches decodePositionalChanges' shape.
    expect(r.changes[0].rowKey).toEqual({id: 'u1'});
    expect(r.changes[0].table).toBe('users');
    expect(r.changes[0].type).toBe(0);
    // Full row content survives the record round-trip (f64 + string tags).
    expect(r.changes[0].row).toEqual({id: 'u1', name: 'alice', age: 30});
  });

  test('row plane: chunked rowMode streams one delivery per row', async () => {
    const c = ensureStarted();
    const deliveries: {changes: RowChange[]; final?: boolean; chunkIndex?: number}[] = [];
    await c.addQueriesStream(
      'cg-napi',
      [{queryID: 'q-chunked', ast: {table: 'users', orderBy: [['id', 'asc']]}}],
      1,
      r => deliveries.push(r),
      {rowMode: true, chunked: true},
    );
    // 3 per-row deliveries + 1 terminal final (empty, carries timing).
    const nonFinal = deliveries.filter(d => !d.final);
    const finals = deliveries.filter(d => d.final);
    expect(nonFinal).toHaveLength(3);
    expect(nonFinal.every(d => d.changes.length === 1)).toBe(true);
    expect(finals).toHaveLength(1);
  });

  test('row plane: advanceStream rowMode delivers the pushed change', async () => {
    const c = ensureStarted();
    const result = await c.advanceStream(
      'cg-napi',
      [
        {
          table: 'users',
          nextValue: {id: 'u9', name: 'zed', age: 99},
          rowKey: {id: 'u9'},
        },
      ],
      1,
      {rowMode: true},
    );
    // Two live queries (q-all, q-chunked) over the same table → the add
    // fans out to both pipelines: 2 RowChanges.
    expect(result.changes.length).toBe(2);
    const qids = result.changes.map(ch => ch.queryID).sort();
    expect(qids).toEqual(['q-all', 'q-chunked']);
    for (const ch of result.changes) {
      expect(ch.type).toBe(0);
      expect(ch.rowKey).toEqual({id: 'u9'});
      expect((ch.row as {name: string}).name).toBe('zed');
    }
    expect(result.timings?.length).toBeGreaterThan(0);
  });

  test('frame plane: advanceStream WITHOUT rowMode still works over NAPI', async () => {
    const c = ensureStarted();
    const result = await c.advanceStream(
      'cg-napi',
      [
        {
          table: 'users',
          nextValue: {id: 'u10', name: 'yara', age: 41},
          rowKey: {id: 'u10'},
        },
      ],
      1,
    );
    expect(result.changes.length).toBe(2);
    for (const ch of result.changes) {
      expect((ch.row as {name: string}).name).toBe('yara');
    }
  });

  test('concurrent rowMode hydrates keep rows correctly routed per RPC', async () => {
    const c = ensureStarted();
    const resA: RowChange[] = [];
    const resB: RowChange[] = [];
    await Promise.all([
      c.addQueriesStream(
        'cg-napi',
        [{queryID: 'q-conc-a', ast: {table: 'users', orderBy: [['id', 'asc']]}}],
        1,
        r => resA.push(...r.changes),
        {rowMode: true},
      ),
      c.addQueriesStream(
        'cg-napi',
        [{queryID: 'q-conc-b', ast: {table: 'users', orderBy: [['id', 'asc']]}}],
        1,
        r => resB.push(...r.changes),
        {rowMode: true},
      ),
    ]);
    // 5 rows each (u1,u2,u3,u9,u10), tagged with their own queryID.
    expect(resA).toHaveLength(5);
    expect(resB).toHaveLength(5);
    expect(resA.every(ch => ch.queryID === 'q-conc-a')).toBe(true);
    expect(resB.every(ch => ch.queryID === 'q-conc-b')).toBe(true);
  });
});

if (!available) {
  test('NAPI transport E2E skipped (build artifacts missing)', () => {
    // Visibility: this suite is a no-op until the addon + dylib are built.
    expect(available).toBe(false);
  });
}
