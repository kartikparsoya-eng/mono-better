import './rust-ivm-addon-setup.ts'; // MUST be first: guarantees the wal2 addon.
import {LogContext} from '@rocicorp/logger';
import {beforeEach, afterEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import type {Database as DB} from '../../../../zqlite/src/db.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {listTables} from '../../db/lite-tables.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import {populateFromExistingTables} from '../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {RustIVMDriver} from './rust-ivm-driver.ts';
import {ResetPipelinesSignal} from './snapshotter.ts';

// -----------------------------------------------------------------------------
// STEADY-STATE CHURN LEAK REPRO — the path prod runs at FLAT CG that neither
// the oracle (bare engine) nor the sandbox (teardown/reconnect churn) tested:
//
//   * REAL RustIVMDriver (the rust-specific TS wrapper — stock TS uses
//     PipelineDriver, which prod shows does NOT leak).
//   * Clients STAY (no driver teardown) while queries are CONSTANTLY
//     added/removed  (= changeDesiredQueries / syncQueryPipelineSet churn).
//   * A replication advance stream flowing between churns.
//
// Instrumented with process.memoryUsage() AFTER a forced GC, so it separates
//   V8 retained heap (heapUsed) from native (rss - heapTotal - external).
// A monotonic climb in EITHER, across GCs, over thousands of iterations, at a
// stable live-query count = the prod flat-CG leak. Run:
//   NODE_OPTIONS=--expose-gc pnpm --filter zero-cache test rust-ivm-driver.leak-churn --run
// -----------------------------------------------------------------------------

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
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const _ of it as AsyncIterable<unknown>) {
    /* consume — must fully drain the stream like the view-syncer does */
  }
}

describe('rust-ivm steady-state churn leak repro', () => {
  let dbFile: DbFile;
  let db: DB;
  let lc: LogContext;

  beforeEach(() => {
    lc = new LogContext('error', undefined, new TestLogSink());
    dbFile = new DbFile('rust_ivm_leak_churn');
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
      insC.run(`c${i}`, `i${i % nIssues}`, 'x'.repeat(50));
    }
    populateFromExistingTables(db, listTables(db, false));
  }

  function newStorage() {
    const s = new Database(lc, ':memory:');
    s.prepare(CREATE_STORAGE_TABLE).run();
    return new DatabaseStorage(s);
  }

  function makeDriver(cg: string): RustIVMDriver {
    const d = new RustIVMDriver(
      lc,
      testLogConfig,
      shardID,
      newStorage().createClientGroupStorage(cg),
      cg,
      new InspectorDelegate(undefined),
      () => 200,
      true, // planner ON — match prod
      undefined,
      dbFile.path,
    );
    d.init(CS);
    return d;
  }

  // A varied query: filter on owner so each distinct value builds a distinct
  // pipeline (like real clients' distinct desired queries). Includes a related
  // subquery (comments) so hydrate does real join work.
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

  let ver = 8500000000;
  let writeCursor = 0;
  function advanceWrites(nWrites: number) {
    const v = String(++ver);
    // Distinct ids per batch (cursor walks the 1000-row space) → no duplicate
    // rowKeys in one changeLog2 commit. Each is an UPDATE (row edit) — the
    // common prod advance shape.
    const upd: string[] = [];
    const log: string[] = [];
    for (let i = 0; i < nWrites; i++) {
      const id = `i${writeCursor % 1000}`;
      writeCursor++;
      upd.push(
        `UPDATE issues SET owner='owner${writeCursor % 20}', _0_version='${v}' WHERE id='${id}';`,
      );
      log.push(`('${v}', ${i}, 'issues', json('{"id":"${id}"}'), 's', '{}')`);
    }
    db.exec(/*sql*/ `
      ${upd.join('\n')}
      INSERT OR REPLACE INTO "_zero.changeLog2" VALUES ${log.join(',')};
      UPDATE "_zero.replicationState" SET stateVersion = '${v}';
    `);
  }

  async function drainAdvance(d: RustIVMDriver) {
    try {
      const res = await d.advance(NO_TIMER);
      if (res instanceof ResetPipelinesSignal) return;
      await drain(res.changes);
    } catch (e) {
      if (!(e instanceof ResetPipelinesSignal)) throw e;
    }
  }

  test('churn: add/remove queries + advance at flat live-query count', async () => {
    seed(1000, 3000);
    const CGS = Number(process.env.CGS || 8);
    const drivers = Array.from({length: CGS}, (_, i) => makeDriver(`cg-${i}`));
    // Each driver holds a stable set of LIVE queries; we churn a fraction each
    // round (add new, remove old) keeping the live count flat — the prod
    // changeDesiredQueries pattern.
    const LIVE = 15; // queries held per driver (flat)
    const CHURN = 5; // added+removed per round
    const ROUNDS = Number(process.env.ROUNDS || 400);
    const qid = new Array(CGS).fill(0);
    const live: Set<string>[] = drivers.map(() => new Set());

    // prime: hydrate LIVE queries per driver
    for (let c = 0; c < CGS; c++) {
      for (let k = 0; k < LIVE; k++) {
        const id = `q${qid[c]++}`;
        await drain(drivers[c].addQuery('h', id, queryFor(qid[c]), NO_TIMER));
        live[c].add(id);
      }
    }

    const {appendFileSync} = await import('node:fs');
    const OUT = '/tmp/churn-mem.log';
    const gc = () => (globalThis as any).gc?.();
    const snap = (label: string): number => {
      gc();
      gc();
      const m = process.memoryUsage();
      const nativeMB = (m.rss - m.heapTotal - m.external) / 2 ** 20;
      const line =
        `${label.padStart(6)}  rss=${(m.rss / 2 ** 20).toFixed(0)}MB  ` +
        `v8Heap=${(m.heapUsed / 2 ** 20).toFixed(0)}MB  ` +
        `heapTotal=${(m.heapTotal / 2 ** 20).toFixed(0)}MB  ` +
        `external=${(m.external / 2 ** 20).toFixed(0)}MB  ` +
        `native≈${nativeMB.toFixed(0)}MB` +
        `  gc=${(globalThis as any).gc ? 'on' : 'OFF'}\n`;
      appendFileSync(OUT, line);
      return nativeMB;
    };

    snap('t0');
    let halfNativeMB: number | undefined;
    for (let round = 1; round <= ROUNDS; round++) {
      // BISECT toggles: CHURN_ONLY skips advance; ADVANCE_ONLY skips churn.
      const noChurn = process.env.ADVANCE_ONLY === '1';
      const noAdvance = process.env.CHURN_ONLY === '1';
      // 1. churn each driver's desired-query set (add/remove, flat live count)
      if (!noChurn) {
        for (let c = 0; c < CGS; c++) {
          const ids = [...live[c]];
          for (let k = 0; k < CHURN && ids.length; k++) {
            drivers[c].removeQuery(ids[k]);
            live[c].delete(ids[k]);
          }
          for (let k = 0; k < CHURN; k++) {
            const id = `q${qid[c]++}`;
            await drain(
              drivers[c].addQuery('h', id, queryFor(qid[c]), NO_TIMER),
            );
            live[c].add(id);
          }
        }
      }
      // 2. ONE replication commit this round (bump db to head once)
      advanceWrites(30);
      // 3. every driver advances to the new head (the version-ready fan-out)
      if (!noAdvance) {
        for (let c = 0; c < CGS; c++) {
          await drainAdvance(drivers[c]);
        }
      }
      if (round === Math.floor(ROUNDS / 2)) halfNativeMB = snap(`r${round}`);
      else if (round % 25 === 0) snap(`r${round}`);
    }
    const endNativeMB = snap('END');
    for (const d of drivers) await d.destroy();

    // SELF-GATING: retained native growth per round over the steady-state
    // half (first half = warmup ramp; see the reconnect-churn gate for the
    // rationale and thresholds). A live-pipeline churn+advance leak class
    // (per-advance state, planner graphs, cursors) shows up here as a
    // per-round slope; healthy noise measured ~10s of KB/round.
    if (ROUNDS >= 200 && halfNativeMB !== undefined) {
      const tailRounds = ROUNDS - Math.floor(ROUNDS / 2);
      const perRoundKB = ((endNativeMB - halfNativeMB) * 1024) / tailRounds;
      expect(
        perRoundKB,
        `retained native grew ${perRoundKB.toFixed(1)}KB/round over the ` +
          `steady-state ${tailRounds} churn+advance rounds ` +
          `(${halfNativeMB.toFixed(1)} -> ${endNativeMB.toFixed(1)}MB)`,
      ).toBeLessThan(512);
    }
  }, 600_000);
});
