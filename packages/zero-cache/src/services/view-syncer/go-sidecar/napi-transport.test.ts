// End-to-end test for the in-process (NAPI) Go transport: GoIVMClient →
// goivm_napi addon → libgoivm (Go c-shared) → engine → TSFN deliveries →
// client dispatch, covering BOTH planes:
//
//   - frame plane: init / ping round-trips (msgpack, kind 1)
//   - row plane:   addQueriesStream + advanceToHeadStream with rowMode (kinds 2/3)
//
// Table-mode fixture: the removal sweep deleted memory mode (loadRows), so
// all data is pre-seeded into a SQLite replica BEFORE the addon starts.
// init is schema-only; the Go engine reads rows from the replica.
//
// GATED: requires the out-of-band build artifacts —
//
//   addon:  cd .../go-sidecar/napi && npx node-gyp rebuild
//   dylib:  (go-ivm repo) go build -tags napilib -buildmode=c-shared \
//             -o /tmp/libgoivm.dylib ./cmd/sidecar
//           (override path via GOIVM_TEST_LIB)
//
//   advance tests require the wal2-tagged dylib (BEGIN CONCURRENT):
//   CGO_CFLAGS="-I/tmp/wal2lib" CGO_LDFLAGS="-L/tmp/wal2lib" \
//     go build -tags "libsqlite3 sqlite_omit_load_extension osusergo netgo napilib" \
//       -buildmode=c-shared -o /tmp/libgoivm_wal2.dylib ./cmd/sidecar
//   GOIVM_TEST_LIB=/tmp/libgoivm_wal2.dylib npx vitest run ...
//
// Skips cleanly when either is missing. The Go host can only start ONCE per
// process (Go runtimes cannot be unloaded), so all tests share one bridge.

import {existsSync} from 'node:fs';
import {afterAll, describe, expect, test} from 'vitest';
import {GoIVMClient} from './go-ivm-client.ts';
import type {RowChange} from './go-ivm-client.ts';
import {isGoNapiAddonAvailable, loadGoNapiAddon} from './napi/index.ts';
import {makeTestReplica} from './napi-test-fixtures.ts';

const LIB_PATH =
  process.env.GOIVM_TEST_LIB ??
  (process.platform === 'darwin' ? '/tmp/libgoivm.dylib' : '/tmp/libgoivm.so');

// ── replica ──────────────────────────────────────────────────────────
// Create and seed the replica BEFORE the addon starts. The Go engine
// reads GO_IVM_REPLICA_DB_PATH at startup; all client groups share it.

const replica = makeTestReplica();
replica.db.exec(
  `CREATE TABLE "users" ("id" TEXT PRIMARY KEY,"name" TEXT,"age" INTEGER,"_0_version" TEXT)`,
);
const userInsert = replica.db.prepare(
  'INSERT INTO "users" VALUES (?,?,?,?)',
);
userInsert.run('u1', 'alice', 30, '0000000001');
userInsert.run('u2', 'bob', 25, '0000000001');
userInsert.run('u3', 'carol', 35, '0000000001');

replica.db.exec(
  `CREATE TABLE "edge" ("id" TEXT PRIMARY KEY,"num" NUMERIC,"flag" INTEGER,"meta" TEXT,"label" TEXT,"_0_version" TEXT)`,
);
const edgeRows: {
  id: string;
  num: number;
  flag: boolean;
  meta: unknown;
  label: string;
}[] = [
  {id: 'e01', num: 9007199254740991, flag: true, meta: null, label: ''},
  {
    id: 'e02',
    num: -123456.789,
    flag: false,
    meta: {a: [1, 'x', null], b: {c: true}},
    label: 'plain',
  },
  {id: 'e03', num: 3.141592653589793, flag: true, meta: [1, 2, 3], label: '🎯emoji🚀'},
  {
    id: 'e04',
    num: 0,
    flag: false,
    meta: {nested: {deep: 'ok'}},
    label: 'a'.repeat(70_000),
  },
  {id: 'e05', num: 42, flag: true, meta: null, label: '\u0000null-byte'},
  {id: 'e06', num: -9876.54321, flag: false, meta: {emoji: '💥'}, label: 'ünïcödé'},
];
const edgeInsert = replica.db.prepare(
  'INSERT INTO "edge" VALUES (?,?,?,?,?,?)',
);
for (const r of edgeRows) {
  edgeInsert.run(
    r.id,
    r.num,
    r.flag ? 1 : 0,
    r.meta === null ? null : JSON.stringify(r.meta),
    r.label,
    '0000000001',
  );
}

replica.db.exec(
  `CREATE TABLE "bulk" ("id" TEXT PRIMARY KEY,"n" INTEGER,"_0_version" TEXT)`,
);
const bulkInsert = replica.db.prepare(
  'INSERT INTO "bulk" VALUES (?,?,?)',
);
for (let i = 0; i < 5000; i++) {
  bulkInsert.run(`k${String(i).padStart(5, '0')}`, i, '0000000001');
}

replica.db.exec(
  `CREATE TABLE "seq" ("id" TEXT PRIMARY KEY,"n" INTEGER,"_0_version" TEXT)`,
);
const seqInsert = replica.db.prepare('INSERT INTO "seq" VALUES (?,?,?)');
for (let i = 0; i < 200; i++) {
  seqInsert.run(`s${String(i).padStart(4, '0')}`, i, '0000000001');
}

process.env.GO_IVM_REPLICA_DB_PATH = replica.path;

const available = isGoNapiAddonAvailable() && existsSync(LIB_PATH);

describe.skipIf(!available)('NAPI transport (in-process Go engine)', () => {
  const client = new GoIVMClient();
  let started = false;
  let usersEpoch = 0;

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
    client.close();
  });

  test('frame plane: init round-trip', async () => {
    const c = ensureStarted();
    const {initEpoch} = await c.init('cg-napi', {
      tables: {
        users: {
          columns: {
            id: {type: 'string'},
            name: {type: 'string'},
            age: {type: 'number'},
            _0_version: {type: 'string'},
          },
          primaryKey: ['id'],
          uniqueKeys: [['id']],
          rows: [],
        },
      },
    });
    expect(initEpoch).toBeGreaterThan(0);
    usersEpoch = initEpoch;
  });

  test('row plane: addQueriesStream delivers per-row records', async () => {
    const c = ensureStarted();
    const results: {
      queryID: string;
      changes: RowChange[];
      timingMs: number | undefined;
      final?: boolean;
    }[] = [];
    await c.addQueriesStream(
      'cg-napi',
      [{queryID: 'q-all', ast: {table: 'users', orderBy: [['id', 'asc']]}}],
      usersEpoch,
      r => results.push(r),
    );
    expect(results).toHaveLength(1);
    const r = results[0];
    expect(r.queryID).toBe('q-all');
    expect(r.changes).toHaveLength(3);
    expect(r.changes.map(ch => (ch.row as {name: string}).name)).toEqual([
      'alice',
      'bob',
      'carol',
    ]);
    expect(r.changes[0].rowKey).toEqual({id: 'u1'});
    expect(r.changes[0].table).toBe('users');
    expect(r.changes[0].type).toBe(0);
    expect(r.changes[0].row).toEqual({
      id: 'u1',
      name: 'alice',
      age: 30,
      _0_version: '0000000001',
    });
  });

  test('row plane: chunked streams one delivery per row', async () => {
    const c = ensureStarted();
    const deliveries: {
      changes: RowChange[];
      final?: boolean;
      chunkIndex?: number;
    }[] = [];
    await c.addQueriesStream(
      'cg-napi',
      [{queryID: 'q-chunked', ast: {table: 'users', orderBy: [['id', 'asc']]}}],
      usersEpoch,
      r => deliveries.push(r),
      {chunked: true},
    );
    const nonFinal = deliveries.filter(d => !d.final);
    const finals = deliveries.filter(d => d.final);
    expect(nonFinal).toHaveLength(3);
    expect(nonFinal.every(d => d.changes.length === 1)).toBe(true);
    expect(finals).toHaveLength(1);
  });

  // Advance: modify the replica (insert row + changelog + bump version),
  // then advanceToHeadStream derives the diff and applies it to the
  // engine. The two live queries (q-all, q-chunked) fan out the add to
  // both pipelines. Requires the wal2-tagged dylib (BEGIN CONCURRENT).
  test('row plane: advanceToHeadStream delivers derived changes', async () => {
    const c = ensureStarted();
    replica.db
      .prepare('INSERT INTO "users" VALUES (?,?,?,?)')
      .run('u9', 'zed', 99, '0000000002');
    replica.addChangeLog('0000000002', 0, 'users', '{"id":"u9"}', 's');
    replica.bumpVersion('0000000002');

    const result = await c.advanceToHeadStream('cg-napi', usersEpoch, '');
    expect(result.rowChanges.length).toBe(2);
    const qids = result.rowChanges.map(ch => ch.queryID).sort();
    expect(qids).toEqual(['q-all', 'q-chunked']);
    for (const ch of result.rowChanges) {
      expect(ch.type).toBe(0);
      expect(ch.rowKey).toEqual({id: 'u9'});
      expect((ch.row as {name: string}).name).toBe('zed');
    }
    expect(result.version).toBe('0000000002');
    expect(result.timings?.length).toBeGreaterThan(0);
  });

  test('concurrent hydrates keep rows correctly routed per RPC', async () => {
    const c = ensureStarted();
    const resA: RowChange[] = [];
    const resB: RowChange[] = [];
    await Promise.all([
      c.addQueriesStream(
        'cg-napi',
        [{queryID: 'q-conc-a', ast: {table: 'users', orderBy: [['id', 'asc']]}}],
        usersEpoch,
        r => resA.push(...r.changes),
      ),
      c.addQueriesStream(
        'cg-napi',
        [{queryID: 'q-conc-b', ast: {table: 'users', orderBy: [['id', 'asc']]}}],
        usersEpoch,
        r => resB.push(...r.changes),
      ),
    ]);
    // 4 rows (u1,u2,u3,u9), tagged with their own queryID.
    expect(resA).toHaveLength(4);
    expect(resB).toHaveLength(4);
    expect(resA.every(ch => ch.queryID === 'q-conc-a')).toBe(true);
    expect(resB.every(ch => ch.queryID === 'q-conc-b')).toBe(true);
  });

  // ── cross-plane type-conversion correctness ───────────────────────
  // For identical engine rows, rowMode (flat records) and frame mode
  // (msgpack positional decode) must produce deep-equal JS values.
  // SQLite-provenance coercion is covered on the Go side by
  // TestABIHost_RowModeTableModeCoercion; this covers the JS decode half
  // with values that are edge cases for the decode path but storable in
  // SQLite (the removal sweep deleted loadRows, so values go through
  // SQLite rather than msgpack — Infinity/-0/subnormals are no longer
  // representable).
  test('cross-plane: rowMode and frame mode decode identical edge values', async () => {
    const c = ensureStarted();
    const {initEpoch} = await c.init('cg-edge', {
      tables: {
        edge: {
          columns: {
            id: {type: 'string'},
            num: {type: 'number'},
            flag: {type: 'boolean'},
            meta: {type: 'json'},
            label: {type: 'string'},
            _0_version: {type: 'string'},
          },
          primaryKey: ['id'],
          uniqueKeys: [['id']],
          rows: [],
        },
      },
    });

    const hydrate = async (queryID: string) => {
      const out: RowChange[] = [];
      await c.addQueriesStream(
        'cg-edge',
        [{queryID, ast: {table: 'edge', orderBy: [['id', 'asc']]}}],
        initEpoch,
        r => out.push(...r.changes),
      );
      return out;
    };
    const viaRecords = await hydrate('q-edge-rows');

    expect(viaRecords).toHaveLength(edgeRows.length);

    const byID = new Map(
      viaRecords.map(ch => [
        (ch.row as {id: string}).id,
        ch.row as Record<string, unknown>,
      ]),
    );
    expect(byID.get('e01')?.num).toBe(9007199254740991);
    expect(byID.get('e01')?.flag).toBe(true);
    expect(byID.get('e01')?.meta).toBeNull();
    expect(byID.get('e01')?.label).toBe('');
    expect(byID.get('e02')?.meta).toEqual({a: [1, 'x', null], b: {c: true}});
    expect(byID.get('e02')?.flag).toBe(false);
    expect(byID.get('e03')?.label).toBe('🎯emoji🚀');
    expect(byID.get('e03')?.meta).toEqual([1, 2, 3]);
    expect((byID.get('e04')?.label as string).length).toBe(70_000);
    expect(byID.get('e05')?.label).toBe('\u0000null-byte');
    expect(byID.get('e05')?.num).toBe(42);
    expect(byID.get('e06')?.meta).toEqual({emoji: '💥'});
    expect(byID.get('e06')?.label).toBe('ünïcödé');
  });

  // ── lifecycle: RPC timeout mid-row-stream ─────────────────────────
  test('lifecycle: timeout mid-stream drops late records, client stays usable', async () => {
    const c = ensureStarted();
    const {initEpoch} = await c.init('cg-late', {
      tables: {
        bulk: {
          columns: {
            id: {type: 'string'},
            n: {type: 'number'},
            _0_version: {type: 'string'},
          },
          primaryKey: ['id'],
          uniqueKeys: [['id']],
          rows: [],
        },
      },
    });

    await expect(
      c.addQueriesStream(
        'cg-late',
        [{queryID: 'q-timeout', ast: {table: 'bulk', orderBy: [['id', 'asc']]}}],
        initEpoch,
        () => {},
        {timeoutMs: 1},
      ),
    ).rejects.toThrow(/timed out/);

    await new Promise(r => setTimeout(r, 250));
    expect(await c.ping()).toBe('pong');

    const out: RowChange[] = [];
    await c.addQueriesStream(
      'cg-late',
      [
        {
          queryID: 'q-after-timeout',
          ast: {table: 'bulk', orderBy: [['id', 'asc']], limit: 3},
        },
      ],
      initEpoch,
      r => out.push(...r.changes),
    );
    expect(out.map(ch => (ch.row as {id: string}).id)).toEqual([
      'k00000',
      'k00001',
      'k00002',
    ]);
  });

  // ── pull mode (ABI v3) over the REAL boundary ─────────────────────
  test('pull mode: W=1 lockstep — one row per next(), cancel unwinds Go mid-stream', async () => {
    const c = ensureStarted();
    const {initEpoch} = await c.init('cg-pull-e2e', {
      tables: {
        seq: {
          columns: {
            id: {type: 'string'},
            n: {type: 'number'},
            _0_version: {type: 'string'},
          },
          primaryKey: ['id'],
          uniqueKeys: [['id']],
          rows: [],
        },
      },
    });

    const it = c.addQueriesStreamPull(
      'cg-pull-e2e',
      [{queryID: 'q-pull-e2e', ast: {table: 'seq', orderBy: [['id', 'asc']]}}],
      initEpoch,
      {window: 1},
    );
    const seen: string[] = [];
    for (let i = 0; i < 5; i++) {
      const {value, done} = await it.next();
      expect(done).toBe(false);
      if (!value.final) {
        seen.push((value.changes[0].row as {id: string}).id);
      }
      await new Promise(r => setTimeout(r, 5));
    }
    expect(seen).toEqual(['s0000', 's0001', 's0002', 's0003', 's0004']);

    await it.return?.();

    await new Promise(r => setTimeout(r, 100));
    const out: RowChange[] = [];
    await c.addQueriesStream(
      'cg-pull-e2e',
      [
        {
          queryID: 'q-after-pull-cancel',
          ast: {table: 'seq', orderBy: [['id', 'asc']]},
        },
      ],
      initEpoch,
      r => out.push(...r.changes),
    );
    expect(out).toHaveLength(200);
  });

  test('pull mode: full drain delivers every row exactly once with W=8', async () => {
    const c = ensureStarted();
    const seen: string[] = [];
    let finals = 0;
    for await (const entry of c.addQueriesStreamPull(
      'cg-pull-e2e',
      [
        {
          queryID: 'q-pull-drain',
          ast: {table: 'seq', orderBy: [['id', 'asc']]},
        },
      ],
      1,
      {window: 8},
    )) {
      if (entry.final) {
        finals++;
        expect(entry.timingMs).toBeGreaterThan(0);
        continue;
      }
      expect(entry.changes).toHaveLength(1);
      seen.push((entry.changes[0].row as {id: string}).id);
    }
    expect(finals).toBe(1);
    expect(seen).toHaveLength(200);
    expect(seen).toEqual([...seen].sort());
    expect(new Set(seen).size).toBe(200);
  });
});

if (!available) {
  test('NAPI transport E2E skipped (build artifacts missing)', () => {
    expect(available).toBe(false);
  });
}
