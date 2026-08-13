/* oxlint-disable typescript/no-explicit-any -- benchmark adapters intentionally accept both sync and async driver shapes */
/**
 * ADVANCE FIXED-OVERHEAD BENCH — reproduce the prod finding locally.
 *
 * Prod profiles show ~5ms/advance of time unattributed by engine spans
 * (86% of all advance time), vs TS advance p50 0.6ms. Hypothesis: rust-only
 * boundary machinery (header TSFN, blocking drain barrier, JS queue hop).
 *
 * This bench: tiny table, one trivial query, then N sequential single-change
 * advances through the REAL streaming path. Reports wall-clock per advance
 * (p50/p95/mean) for RustIVMDriver vs PipelineDriver. With
 * RUST_IVM_PERF_TRACE=/tmp/adv-overhead-trace.txt the rust engine-side totals
 * land in the trace file: wall - engineTotal = JS/boundary share.
 *
 * Run:
 *   cd packages/zero-cache && ADV_OVERHEAD=1 \
 *     RUST_IVM_PERF_TRACE=/tmp/adv-overhead-trace.txt \
 *     npx vitest run rust-ivm-advance-overhead
 * Results appended to /tmp/adv-overhead-results.txt
 */
import './rust-ivm-addon-setup.ts';
import {appendFileSync, readFileSync, writeFileSync} from 'node:fs';
import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {listTables} from '../../db/lite-tables.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import {populateFromExistingTables} from '../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {PipelineDriver} from './pipeline-driver.ts';
import {RustIVMDriver} from './rust-ivm-driver.ts';
import {Snapshotter} from './snapshotter.ts';
const ADDON_PATH = process.env['RUST_IVM_ADDON_PATH'];
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import {ResetPipelinesSignal} from './snapshotter.ts';

const BIG_TIMER = {elapsedLap: () => 0, totalElapsed: () => 10_000_000} as any;
const NO_TIMER = {elapsedLap: () => 0, totalElapsed: () => 0} as any;

const RUN = !!process.env.ADV_OVERHEAD;
const N_ADVANCES = 200;
const TRACE_FILE = process.env['RUST_IVM_PERF_TRACE'];

describe.skipIf(!ADDON_PATH || !RUN)('advance fixed overhead bench', () => {
  const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
  const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
  const BASE = '8400bivbkg';

  let dbFile: DbFile;
  let db: Database;
  let lc: LogContext;

  beforeEach(() => {
    lc = new LogContext('error', undefined, new TestLogSink());
    dbFile = new DbFile('rust_ivm_adv_overhead');
    dbFile.connect(lc).pragma('journal_mode = wal2');
  });
  afterEach(() => dbFile.delete());

  const items = table('items')
    .columns({id: string(), grp: string(), name: string()})
    .primaryKey('id');
  const CS = createSchema({tables: [items]});

  function seed() {
    db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], BASE);
    db.pragma('wal_autocheckpoint = 0');
    const stmts: string[] = [
      `CREATE TABLE "${mutationsTableName}" (
        "clientGroupID" TEXT, "clientID" TEXT, "mutationID" INTEGER,
        "result" TEXT, _0_version TEXT NOT NULL,
        PRIMARY KEY ("clientGroupID","clientID","mutationID"));`,
      `CREATE TABLE items (id TEXT PRIMARY KEY, grp "text|NOT_NULL", name TEXT, _0_version TEXT NOT NULL);`,
      `CREATE INDEX items_grp ON items (grp);`,
    ];
    for (let i = 0; i < 1000; i++) {
      stmts.push(
        `INSERT INTO items VALUES ('i${String(i).padStart(5, '0')}','g${i % 10}','item ${i}','${BASE}');`,
      );
    }
    db.exec(stmts.join('\n'));
    db.exec('ANALYZE;');
    populateFromExistingTables(db, listTables(db, false));
  }

  // Trivial query: 100 rows of one group, no related, no exists.
  const AST_SHAPE: AST = {
    table: 'items',
    orderBy: [['id', 'asc']],
    where: {
      type: 'simple',
      left: {type: 'column', name: 'grp'},
      op: '=',
      right: {type: 'literal', value: 'g1'},
    },
  };

  function newStorage() {
    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    return new DatabaseStorage(storage);
  }
  function makeRust() {
    const d = new RustIVMDriver(
      lc,
      testLogConfig,
      shardID,
      newStorage().createClientGroupStorage('cg-rust'),
      'cg-rust',
      new InspectorDelegate(undefined),
      () => 200,
      false,
      undefined,
      dbFile.path,
    );
    d.init(CS);
    return d;
  }
  function makeTs() {
    const d = new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
      shardID,
      newStorage().createClientGroupStorage('cg-ts'),
      'cg-ts',
      new InspectorDelegate(undefined),
      () => 200,
      false,
    );
    d.init(CS);
    return d;
  }

  async function drain(it: any): Promise<any[]> {
    const out: any[] = [];
    for await (const c of it) if (c !== 'yield') out.push(c);
    return out;
  }
  async function timedAdvance(d: RustIVMDriver | PipelineDriver) {
    const t0 = performance.now();
    let n = 0;
    try {
      const res = await d.advance(NO_TIMER);
      if (!(res instanceof ResetPipelinesSignal)) {
        n = (await drain(res.changes)).length;
      }
    } catch (e) {
      if (!(e instanceof ResetPipelinesSignal)) throw e;
    }
    return {ms: performance.now() - t0, n};
  }

  let vCounter = 0;
  // One single-row change per advance. Alternates in/out of the query's group
  // so pipeline work exists but is tiny (one indexed probe).
  function commitOne() {
    const v = `85000${String(vCounter++).padStart(5, '0')}`;
    const i = vCounter % 1000;
    const id = `i${String(i).padStart(5, '0')}`;
    db.exec(`UPDATE items SET name='n${v}', _0_version='${v}' WHERE id='${id}';
      INSERT OR REPLACE INTO "_zero.changeLog2" VALUES ('${v}',0,'items',json('{"id":"${id}"}'),'s','{}');
      UPDATE "_zero.replicationState" SET stateVersion='${v}';`);
  }

  function stats(xs: number[]) {
    const s = xs.toSorted((a, b) => a - b);
    const q = (p: number) =>
      s[Math.min(s.length - 1, Math.floor(p * s.length))];
    const mean = s.reduce((a, b) => a + b, 0) / s.length;
    return `mean=${mean.toFixed(2)}ms p50=${q(0.5).toFixed(2)}ms p95=${q(0.95).toFixed(2)}ms max=${q(1).toFixed(2)}ms`;
  }

  test(`fixed overhead x${N_ADVANCES}`, async () => {
    const out: string[] = [];
    // CONTEND=1: simulate a busy prod event loop (CVR flush / poke serialize
    // bursts) — 2ms of sync work every ~4ms. Rust advances need 3-4 loop
    // turnarounds (header/rows/END/drain-ack); TS needs the loop once.
    let spinner: ReturnType<typeof setInterval> | undefined;
    if (process.env.CONTEND === '1') {
      spinner = setInterval(() => {
        const end = performance.now() + 2;
        while (performance.now() < end) {
          /* busy */
        }
      }, 4);
      out.push('(contended: 2ms sync bursts every 4ms on the JS loop)');
    }
    try {
      for (const [name, mk] of [
        ['rust', makeRust],
        ['ts  ', makeTs],
      ] as const) {
        dbFile.delete();
        dbFile = new DbFile('rust_ivm_adv_overhead');
        dbFile.connect(lc).pragma('journal_mode = wal2');
        vCounter = 0;
        seed();
        if (name === 'rust' && TRACE_FILE) writeFileSync(TRACE_FILE, '');
        const d = mk();
        try {
          await drain(d.addQuery('h', 'q', AST_SHAPE, BIG_TIMER));
          // warmup
          for (let w = 0; w < 10; w++) {
            commitOne();
            await timedAdvance(d);
          }
          const walls: number[] = [];
          for (let k = 0; k < N_ADVANCES; k++) {
            commitOne();
            const a = await timedAdvance(d);
            walls.push(a.ms);
          }
          out.push(
            `${name} wall/advance: ${stats(walls)}  (n=${N_ADVANCES}, single-change advances)`,
          );
          if (name === 'rust' && TRACE_FILE) {
            // engine-side totals from the trace file → boundary share = wall - engine
            const lines = readFileSync(TRACE_FILE, 'utf8')
              .split('\n')
              .filter(l => l.includes('PERF] advance total='));
            const engTotals = lines
              .map(l =>
                parseFloat(l.match(/advance total=([0-9.]+)ms/)?.[1] ?? '0'),
              )
              .filter(x => x > 0)
              .slice(-N_ADVANCES);
            if (engTotals.length) {
              out.push(
                `rust engine-side total: ${stats(engTotals)}  (from PERF trace, n=${engTotals.length})`,
              );
            }
          }
        } finally {
          try {
            d.removeQuery('q');
          } catch {}
          try {
            await (d as any).destroy?.();
          } catch {}
        }
      }
    } finally {
      if (spinner) clearInterval(spinner);
    }
    appendFileSync(
      '/tmp/adv-overhead-results.txt',
      `\n=== ${new Date().toISOString()}${process.env.CONTEND === '1' ? ' CONTENDED' : ''} ===\n${out.join('\n')}\n`,
    );
    expect(out.length).toBeGreaterThanOrEqual(2);
  }, 300_000);
});
