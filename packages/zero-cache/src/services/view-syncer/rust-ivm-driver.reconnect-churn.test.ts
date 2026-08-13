/* oxlint-disable typescript/no-explicit-any -- optional exposed-gc and storage internals are diagnostic test hooks */
import './rust-ivm-addon-setup.ts'; // MUST be first: guarantees the wal2 addon.
import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import {DatabaseStorage} from '../../../../zqlite/src/database-storage.ts';
import type {Database as DB} from '../../../../zqlite/src/db.ts';
import {listTables} from '../../db/lite-tables.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import {populateFromExistingTables} from '../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {RustIVMDriver} from './rust-ivm-driver.ts';

// ---------------------------------------------------------------------------
// RECONNECT-CHURN BISECT — the full-stack Playwright soak proved a ~13.7MB/
// reconnect RETAINED native leak in the view-syncer teardown, above the engine
// (engine create/destroy loop was flat). This isolates the DRIVER+STORAGE
// layer: loop create RustIVMDriver (real DatabaseStorage) -> hydrate -> destroy
// (engine.destroy + storage.destroy). storage.destroy = DELETE rows, NO
// incremental_vacuum. If native climbs -> the leak is here (storage free-list /
// driver). If flat -> it's ABOVE the driver (CVR / client / view-syncer).
// VACUUM=1 adds PRAGMA incremental_vacuum after each destroy to test the fix.
//   NODE_OPTIONS=--expose-gc pnpm --filter zero-cache test rust-ivm-driver.reconnect-churn --run
// ---------------------------------------------------------------------------

const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
const BASE = '8400bivbkg';
const NO_TIMER = {elapsedLap: () => 0, totalElapsed: () => 0} as any;

const issues = table('issues')
  .columns({id: string(), kind: string(), owner: string()})
  .primaryKey('id');
const comments = table('comments')
  .columns({id: string(), issueID: string(), body: string()})
  .primaryKey('id');
const CS = createSchema({tables: [issues, comments]});

async function drain(it: AsyncIterable<unknown> | Iterable<unknown>) {
  for await (const _ of it as AsyncIterable<unknown>) {
    /* consume */
  }
}

describe('rust-ivm reconnect-churn bisect', () => {
  let dbFile: DbFile;
  let db: DB;
  let lc: LogContext;

  beforeEach(() => {
    lc = new LogContext('error', undefined, new TestLogSink());
    dbFile = new DbFile('rust_ivm_reconnect');
    dbFile.connect(lc).pragma('journal_mode = wal2');
  });
  afterEach(() => dbFile.delete());

  function seed(nIssues: number, nComments: number) {
    db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], BASE);
    db.exec(/*sql*/ `
      CREATE TABLE "${mutationsTableName}" (
        "clientGroupID" TEXT, "clientID" TEXT, "mutationID" INTEGER,
        "result" TEXT, _0_version TEXT NOT NULL,
        PRIMARY KEY ("clientGroupID","clientID","mutationID"));
      CREATE TABLE issues (id TEXT PRIMARY KEY, kind TEXT, owner TEXT, _0_version TEXT NOT NULL);
      CREATE TABLE comments (id TEXT PRIMARY KEY, issueID TEXT, body TEXT, _0_version TEXT NOT NULL);
      CREATE INDEX comments_issueID ON comments (issueID);
    `);
    const ins = db.prepare(`INSERT INTO issues VALUES (?, ?, ?, '${BASE}')`);
    for (let i = 0; i < nIssues; i++) {
      ins.run(`i${i}`, i % 2 ? 'public' : 'private', `owner${i % 20}`);
    }
    const insC = db.prepare(`INSERT INTO comments VALUES (?, ?, ?, '${BASE}')`);
    for (let i = 0; i < nComments; i++) {
      insC.run(`c${i}`, `i${i % nIssues}`, 'x'.repeat(80));
    }
    populateFromExistingTables(db, listTables(db, false));
  }

  function queryFor(owner: number): AST {
    return {
      table: 'issues',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'owner'},
        op: '=',
        right: {type: 'literal', value: `owner${owner % 20}`},
      },
      related: [
        {
          system: 'client',
          correlation: {parentField: ['id'], childField: ['issueID']},
          subquery: {table: 'comments', alias: 'comments'},
        },
      ],
      orderBy: [['id', 'asc']],
    } as unknown as AST;
  }

  test('reconnect churn: create -> hydrate -> destroy, watch native RSS', async () => {
    seed(1000, 3000);
    const ROUNDS = Number(process.env.ROUNDS || 800);
    const LIVE = Number(process.env.LIVE || 10);
    const VACUUM = process.env.VACUUM === '1';

    // ONE shared per-worker storage db (prod: each worker owns one DatabaseStorage,
    // a spillable file, auto_vacuum=INCREMENTAL, journal=OFF — via the factory).
    const {tmpdir} = await import('node:os');
    const {join} = await import('node:path');
    const storagePath = join(tmpdir(), `recon-storage-${process.pid}.db`);
    const storage = DatabaseStorage.create(lc, storagePath);
    const storageDb = (storage as any).db ?? undefined; // for the VACUUM toggle

    const {appendFileSync} = await import('node:fs');
    const OUT = '/tmp/reconnect-mem.log';
    const gc = () => (globalThis as any).gc?.();
    const snap = (label: string): number => {
      gc();
      gc();
      const m = process.memoryUsage();
      const nativeMB = (m.rss - m.heapTotal - m.external) / 2 ** 20;
      appendFileSync(
        OUT,
        `${label.padStart(6)}  rss=${(m.rss / 2 ** 20).toFixed(0)}MB  ` +
          `v8=${(m.heapUsed / 2 ** 20).toFixed(0)}MB  native=${nativeMB.toFixed(1)}MB  vacuum=${VACUUM}\n`,
      );
      return nativeMB;
    };
    snap('t0');
    let halfNativeMB: number | undefined;

    for (let r = 1; r <= ROUNDS; r++) {
      const cg = `cg-${r}`;
      const d = new RustIVMDriver(
        lc,
        testLogConfig,
        shardID,
        storage.createClientGroupStorage(cg),
        cg,
        new InspectorDelegate(undefined),
        () => 200,
        true /* planner on */,
        undefined,
        dbFile.path,
      );
      d.init(CS);
      for (let k = 0; k < LIVE; k++) {
        await drain(d.addQuery('h', `q${k}`, queryFor(r * 100 + k), NO_TIMER));
      }
      await d.destroy(); // engine.destroy() + storage.destroy()
      if (VACUUM && storageDb) storageDb.pragma('incremental_vacuum');
      if (r === Math.floor(ROUNDS / 2)) halfNativeMB = snap(`r${r}`);
      else if (r % 50 === 0) snap(`r${r}`);
    }
    const endNativeMB = snap('END');
    storage.close();

    // SELF-GATING (not just a measurement harness): per-reconnect retained
    // native growth over the steady-state HALF of the run (the first half is
    // warmup — page cache, allocator arenas — same ramp shape as prod cold
    // start). The historical bug this hunts was ~13.7MB/reconnect retained in
    // view-syncer teardown; measured healthy noise is ~14KB/round, so 512KB/
    // round is ~27x above noise and ~27x below the bug signature. Skipped for
    // short debug runs (ROUNDS < 200) where two GC'd samples are all noise.
    if (ROUNDS >= 200 && halfNativeMB !== undefined) {
      const tailRounds = ROUNDS - Math.floor(ROUNDS / 2);
      const perRoundKB = ((endNativeMB - halfNativeMB) * 1024) / tailRounds;
      expect(
        perRoundKB,
        `retained native grew ${perRoundKB.toFixed(1)}KB/reconnect over the ` +
          `steady-state ${tailRounds} rounds (${halfNativeMB.toFixed(1)} -> ` +
          `${endNativeMB.toFixed(1)}MB) — reconnect teardown is leaking`,
      ).toBeLessThan(512);
    }
  }, 600_000);
});
