/**
 * FLIP-ADVANCE A/B EXPERIMENT — is FlippedJoin advance amplification
 * engine-independent?
 *
 * Question: with the query planner ON, is the planner-flipped OR-with-EXISTS
 * (prod `userAllChannels` shape) advance amplification EQUALLY expensive in the
 * rust engine (RustIVMDriver) and the TS engine (PipelineDriver)? Prod context:
 * ~10s/advance-change in rust (fjoin.batch_fetch ~1s×4, ~500K row steps per
 * change); stock TS shows 17.7s max advances. If the amplification factor is
 * engine-independent (within ~2x), re-enabling the planner for rust is
 * justified once breaker semantics match TS.
 *
 * Matrix: {rust, ts} × {planner ON, planner OFF, FORCED flip}; FRESH DbFile +
 * driver per config so no mutation carry-over between configs (the old
 * advance-cost harness reused one DB across configs, which confounded its row
 * counts).
 *
 * WHY the FORCED configs: they isolate the FlippedJoin advance amplification
 * independent of the planner. FORCED = planner OFF + `flip: true`
 * pre-annotated on the CSQ condition in the AST, which BOTH builders honor
 * independent of the planner (zql builder.ts applyWhere; rust builder.rs
 * csq_condition.flip). NOTE: the rust planner's default cost model is now the
 * scanstatus/stat-fanout port of TS createSQLiteCostModel (napi plan_ast;
 * sqlite/sqlite_cost_model.rs) — "rust ON" therefore makes the SAME flip
 * decision as "ts ON" on this shape (decision parity is asserted by
 * rust-ivm-planner-parity.test.ts). The legacy filter-blind COUNT(*) model
 * remains behind RUST_IVM_PLANNER_COST_MODEL=count.
 *
 * Run:
 *   cd packages/zero-cache && FLIP_ADVANCE=1 pnpm vitest run rust-ivm-flip-advance
 *
 * Output: /tmp/flip-advance-results.txt (+ rust perf trace /tmp/flip-trace.txt)
 */
// Must be set BEFORE the addon loads (rust perf_trace reads env via OnceLock on
// first use). '/'-prefixed value → spans also appended to that file.
process.env['RUST_IVM_PERF_TRACE'] = '/tmp/flip-trace.txt';
// Prod-representative SQLite page cache, per bench spec. NOTE: verified that
// nothing in the current tree consumes this env (napi passes page_cache=None;
// TS Snapshotter takes pageCacheSizeKib as a ctor arg we don't pass) — set for
// forward-compat/documentation; both engines run at SQLite default cache,
// which is at least SYMMETRIC.
process.env['RUST_IVM_PAGE_CACHE_KIB'] = '16000';
import './rust-ivm-addon-setup.ts';
import {appendFileSync, existsSync, readFileSync, statSync} from 'node:fs';
import {LogContext} from '@rocicorp/logger';
import {afterEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {AST, CompoundKey} from '../../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
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
import type {Timer} from './pipeline-driver.ts';
import {RustIVMDriver} from './rust-ivm-driver.ts';
import {Snapshotter} from './snapshotter.ts';
import {ResetPipelinesSignal} from './snapshotter.ts';

const ADDON_PATH = process.env['RUST_IVM_ADDON_PATH'];
const RUN = !!process.env['FLIP_ADVANCE'];
const TRACE_FILE = '/tmp/flip-trace.txt';
const RESULTS_FILE = '/tmp/flip-advance-results.txt';

// Huge hydration-time timer → advance budget effectively unlimited → the
// economic breaker never fires and we measure true advance cost.
const BIG_TIMER = {
  elapsedLap: () => 0,
  totalElapsed: () => 10_000_000,
} as unknown as Timer;
const NO_TIMER = {
  elapsedLap: () => 0,
  totalElapsed: () => 0,
} as unknown as Timer;

const S = Number(process.env['FLIP_S'] ?? 100_000); // channels
const P = 3; // participants per channel (u0..u2)
const REPS = 5;
// 'me' membership density: member of i%ME_MOD==0 (PRIVATE) and i%ME_MOD==1
// (PUBLIC) channels. Default per bench spec: 1000 → 100+100 memberships at
// S=100K. Lower (e.g. 10) to scale the flipped-branch driving side up.
const ME_MOD = Number(process.env['FLIP_ME_MOD'] ?? 1000);
// Optional top-level LIMIT. WHY: without a Take above the UnionFanIn, every
// advance push through the flipped OR graph is key-constrained (UFI dedup
// fetches by primary key; FlippedJoin maps parent-key constraints to child
// constraints) → O(1) per change, NO amplification at any scale. A LIMIT makes
// removes inside the window backfill via an UNCONSTRAINED fetch through the
// FlippedJoin (full driving-side child scan + chunked multi-constraint parent
// batch fetch = the prod fjoin.batch_fetch signature).
const LIMIT = process.env['FLIP_LIMIT']
  ? Number(process.env['FLIP_LIMIT'])
  : undefined;

describe.skipIf(!ADDON_PATH || !RUN)('flip advance A/B', () => {
  const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
  const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
  const BASE = '8400bivbkg';

  const lc = new LogContext('error', undefined, new TestLogSink());
  let dbFile: DbFile | undefined;
  afterEach(() => {
    try {
      dbFile?.delete();
    } catch {
      /* mid-config failure cleanup */
    }
    dbFile = undefined;
  });

  const channels = table('channels')
    .columns({
      id: string(),
      workspaceId: string(),
      visibility: string(),
      name: string(),
    })
    .primaryKey('id');
  const parts = table('channel_participants')
    .columns({id: string(), channelId: string(), userId: string()})
    .primaryKey('id');
  const CS = createSchema({tables: [channels, parts]});

  const cid = (i: number) => `c${String(i).padStart(6, '0')}`;

  // Seeding (per bench spec, deterministic):
  //  - HALF the channels PRIVATE (i%2==0) so the EXISTS branch matters.
  //  - P=3 filler participants per channel.
  //  - 'me' participates in i%1000==0 channels (100, all PRIVATE → in-result
  //    via the EXISTS branch only) AND i%1000==1 channels (100, all PUBLIC →
  //    discriminator for the old "ts planner=ON row count" anomaly: these are
  //    EXISTS-children of parents already admitted by the PUBLIC branch).
  function seed(): Database {
    assert(dbFile);
    const db = dbFile.connect(lc);
    db.pragma('journal_mode = wal2');
    db.pragma('wal_autocheckpoint = 0');
    initReplicationState(db, ['zero_data'], BASE);
    db.exec(`
      CREATE TABLE "${mutationsTableName}" (
        "clientGroupID" TEXT, "clientID" TEXT, "mutationID" INTEGER,
        "result" TEXT, _0_version TEXT NOT NULL,
        PRIMARY KEY ("clientGroupID","clientID","mutationID"));
      CREATE TABLE channels (id TEXT PRIMARY KEY, workspaceId "text|NOT_NULL",
        visibility "text|NOT_NULL", name TEXT, _0_version TEXT NOT NULL);
      CREATE TABLE channel_participants (id TEXT PRIMARY KEY,
        channelId "text|NOT_NULL", userId "text|NOT_NULL", _0_version TEXT NOT NULL);
      CREATE INDEX cp_chan ON channel_participants (channelId);
      CREATE INDEX cp_user ON channel_participants (userId);
    `);
    const insCh = db.prepare('INSERT INTO channels VALUES (?,?,?,?,?)');
    const insCp = db.prepare(
      'INSERT INTO channel_participants VALUES (?,?,?,?)',
    );
    db.exec('BEGIN');
    for (let i = 0; i < S; i++) {
      const c = cid(i);
      insCh.run(c, 'w1', i % 2 === 0 ? 'PRIVATE' : 'PUBLIC', `chan ${i}`, BASE);
      for (let p = 0; p < P; p++) {
        insCp.run(`${c}_u${p}`, c, `u${p}`, BASE);
      }
      if (i % ME_MOD === 0 || i % ME_MOD === 1) {
        insCp.run(`${c}_me`, c, 'me', BASE);
      }
    }
    db.exec('COMMIT');
    db.exec('ANALYZE;');
    populateFromExistingTables(db, listTables(db, false));
    return db;
  }

  // prod userAllChannels shape: channels in workspace, OR(PUBLIC, EXISTS me).
  // `flip` pre-annotates the EXISTS condition (FORCED configs): both builders
  // honor it independent of the planner.
  const makeAst = (flip: boolean): AST => ({
    table: 'channels',
    orderBy: [['id', 'asc']],
    ...(LIMIT !== undefined ? {limit: LIMIT} : {}),
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'workspaceId'},
          op: '=',
          right: {type: 'literal', value: 'w1'},
        },
        {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              left: {type: 'column', name: 'visibility'},
              op: '=',
              right: {type: 'literal', value: 'PUBLIC'},
            },
            {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              ...(flip ? {flip: true} : {}),
              related: {
                system: 'client',
                correlation: {
                  parentField: ['id'] as CompoundKey,
                  childField: ['channelId'] as CompoundKey,
                },
                subquery: {
                  table: 'channel_participants',
                  alias: 'zsubq_participants',
                  orderBy: [['id', 'asc']],
                  where: {
                    type: 'simple',
                    left: {type: 'column', name: 'userId'},
                    op: '=',
                    right: {type: 'literal', value: 'me'},
                  },
                },
              },
            },
          ],
        },
      ],
    },
  });
  const AST_SHAPE = makeAst(false);
  const AST_FLIPPED = makeAst(true);

  function newStorage() {
    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    return new DatabaseStorage(storage);
  }
  function makeRust(planner: boolean) {
    assert(dbFile);
    const d = new RustIVMDriver(
      lc,
      testLogConfig,
      shardID,
      newStorage().createClientGroupStorage('cg-rust'),
      'cg-rust',
      new InspectorDelegate(undefined),
      () => 200,
      planner,
      undefined,
      dbFile.path,
    );
    d.init(CS);
    return d;
  }
  function makeTs(planner: boolean) {
    assert(dbFile);
    const d = new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
      shardID,
      newStorage().createClientGroupStorage('cg-ts'),
      'cg-ts',
      new InspectorDelegate(undefined),
      () => 200,
      planner,
    );
    d.init(CS);
    return d;
  }

  type RowChangeLite = {
    readonly table: string;
    readonly rowKey: Readonly<Record<string, unknown>>;
  };
  async function drain(
    it:
      | AsyncIterable<RowChangeLite | 'yield'>
      | Iterable<RowChangeLite | 'yield'>,
  ): Promise<RowChangeLite[]> {
    const out: RowChangeLite[] = [];
    for await (const c of it) if (c !== 'yield') out.push(c);
    return out;
  }
  async function timedAdvance(d: RustIVMDriver | PipelineDriver) {
    const t0 = performance.now();
    let n = 0;
    let reset = false;
    try {
      const res = await d.advance(NO_TIMER);
      if (res instanceof ResetPipelinesSignal) reset = true;
      else n = (await drain(res.changes)).length;
    } catch (e) {
      if (e instanceof ResetPipelinesSignal) reset = true;
      else throw e;
    }
    return {ms: performance.now() - t0, n, reset};
  }

  // One replicated change = data DML + changeLog2 entry + stateVersion bump.
  // Deterministic version sequence, reset per config (fresh DB each config →
  // every config replays the IDENTICAL change sequence).
  let vCounter = 0;
  function commit(
    db: Database,
    dml: string,
    logTable: string,
    rowKeyJson: string,
    op: 's' | 'd',
  ) {
    const v = `85000000${String(vCounter++).padStart(2, '0')}`;
    db.exec(
      `${dml.replaceAll('$V', v)}
       INSERT OR REPLACE INTO "_zero.changeLog2" VALUES ('${v}',0,'${logTable}',json('${rowKeyJson}'),'${op}','{}');
       UPDATE "_zero.replicationState" SET stateVersion='${v}';`,
    );
  }

  // Change kinds (r = rep 0..4; targets disjoint & identical across configs):
  //  k1_pub_rename    UPDATE a PUBLIC channel's name (in result via filter branch)
  //  k2_priv_rename   UPDATE a PRIVATE non-member channel (outside result)
  //  k3_me_join_priv  INSERT 'me' participant on a PRIVATE channel (adds result row)
  //  k4_u9_join       INSERT 'u9' participant (no result change)
  //  k5_me_leave      DELETE one of 'me''s seeded rows (removes result row)
  //  k6_member_rename UPDATE a PRIVATE channel 'me' participates in (in result
  //                   via the FLIPPED branch only — the flip-hot parent edit)
  const KINDS = [
    'k1_pub_rename',
    'k2_priv_rename',
    'k3_me_join_priv',
    'k4_u9_join',
    'k5_me_leave',
    'k6_member_rename',
  ] as const;
  type Kind = (typeof KINDS)[number];

  // Predicate-based target selection so targets stay valid for any S/ME_MOD.
  const isPriv = (i: number) => i % 2 === 0;
  const isMe = (i: number) => i % ME_MOD === 0 || i % ME_MOD === 1;
  function nth(pred: (i: number) => boolean, n: number): number {
    let c = 0;
    for (let i = 0; i < S; i++) {
      if (pred(i)) {
        if (c === n) return i;
        c++;
      }
    }
    throw new Error(`nth: not enough matches (wanted #${n})`);
  }

  function applyChange(db: Database, kind: Kind, r: number) {
    switch (kind) {
      case 'k1_pub_rename': {
        const c = cid(nth(i => !isPriv(i) && !isMe(i), r)); // PUBLIC non-member
        commit(
          db,
          `UPDATE channels SET name='k1 $V', _0_version='$V' WHERE id='${c}';`,
          'channels',
          `{"id":"${c}"}`,
          's',
        );
        break;
      }
      case 'k2_priv_rename': {
        const c = cid(nth(i => isPriv(i) && !isMe(i), 50 + r)); // PRIVATE non-member
        commit(
          db,
          `UPDATE channels SET name='k2 $V', _0_version='$V' WHERE id='${c}';`,
          'channels',
          `{"id":"${c}"}`,
          's',
        );
        break;
      }
      case 'k3_me_join_priv': {
        const c = cid(nth(i => isPriv(i) && !isMe(i), 100 + r)); // PRIVATE, me not seeded
        commit(
          db,
          `INSERT INTO channel_participants VALUES ('${c}_me','${c}','me','$V');`,
          'channel_participants',
          `{"id":"${c}_me"}`,
          's',
        );
        break;
      }
      case 'k4_u9_join': {
        const c = cid(nth(i => !isMe(i), 300 + r)); // any non-me channel
        commit(
          db,
          `INSERT INTO channel_participants VALUES ('${c}_u9','${c}','u9','$V');`,
          'channel_participants',
          `{"id":"${c}_u9"}`,
          's',
        );
        break;
      }
      case 'k5_me_leave': {
        const c = cid(nth(i => isPriv(i) && isMe(i), 1 + r)); // seeded PRIVATE me-member
        commit(
          db,
          `DELETE FROM channel_participants WHERE id='${c}_me';`,
          'channel_participants',
          `{"id":"${c}_me"}`,
          'd',
        );
        break;
      }
      case 'k6_member_rename': {
        // PRIVATE me-member, disjoint from k5's removals (k5 used members 1..5)
        const c = cid(nth(i => isPriv(i) && isMe(i), 6 + r));
        commit(
          db,
          `UPDATE channels SET name='k6 $V', _0_version='$V' WHERE id='${c}';`,
          'channels',
          `{"id":"${c}"}`,
          's',
        );
        break;
      }
    }
  }

  function traceLen(): number {
    try {
      return existsSync(TRACE_FILE) ? statSync(TRACE_FILE).size : 0;
    } catch {
      return 0;
    }
  }
  function traceSegment(from: number): string {
    try {
      return existsSync(TRACE_FILE)
        ? readFileSync(TRACE_FILE, 'utf8').slice(from)
        : '';
    } catch {
      return '';
    }
  }

  function assert(v: unknown): asserts v {
    if (!v) throw new Error('assertion failed');
  }

  type ConfigResult = {
    name: string;
    engine: 'rust' | 'ts';
    mode: 'on' | 'off' | 'forced';
    hydrateMs: number;
    rowsTotal: number;
    rowsChannels: number;
    rowsParts: number;
    channelIds: Set<string>;
    partIds: Set<string>;
    kinds: Record<Kind, {ms: number[]; n: number[]; resets: number}>;
    fjoinSpans: number; // fjoin.batch_fetch hits observed in this config's trace segment
    traceSample: string;
  };

  test(`S=${S} P=${P} ME_MOD=${ME_MOD} reps=${REPS} — rust/ts × ON/OFF/FORCED`, async () => {
    const configs: ConfigResult[] = [];
    for (const [name, engine, mode] of [
      ['rust ON    ', 'rust', 'on'],
      ['rust OFF   ', 'rust', 'off'],
      ['rust FORCED', 'rust', 'forced'],
      ['ts   ON    ', 'ts', 'on'],
      ['ts   OFF   ', 'ts', 'off'],
      ['ts   FORCED', 'ts', 'forced'],
    ] as const) {
      dbFile = new DbFile(`flip_advance_${engine}_${mode}`);
      const db = seed();
      vCounter = 0;
      const planner = mode === 'on';
      const ast = mode === 'forced' ? AST_FLIPPED : AST_SHAPE;
      const d = engine === 'rust' ? makeRust(planner) : makeTs(planner);
      const traceFrom = traceLen();
      try {
        const h0 = performance.now();
        const rows = await drain(d.addQuery('h', 'q', ast, BIG_TIMER));
        const hydrateMs = performance.now() - h0;
        const channelIds = new Set<string>();
        const partIds = new Set<string>();
        for (const c of rows) {
          if (c.table === 'channels') channelIds.add(String(c.rowKey.id));
          else if (c.table === 'channel_participants') {
            partIds.add(String(c.rowKey.id));
          }
        }
        const kinds = {} as ConfigResult['kinds'];
        for (const kind of KINDS) {
          const ms: number[] = [];
          const n: number[] = [];
          let resets = 0;
          for (let r = 0; r < REPS; r++) {
            applyChange(db, kind, r);
            const a = await timedAdvance(d);
            ms.push(a.ms);
            n.push(a.n);
            if (a.reset) resets++;
          }
          kinds[kind] = {ms, n, resets};
        }
        const seg = traceSegment(traceFrom);
        const fjoinSpans = (seg.match(/fjoin\.batch_fetch=/g) ?? []).length;
        const traceSample =
          seg.split('\n').find(l => l.includes('fjoin.batch_fetch')) ?? '';
        configs.push({
          name,
          engine,
          mode,
          hydrateMs,
          rowsTotal: rows.length,
          rowsChannels: channelIds.size,
          rowsParts: partIds.size,
          channelIds,
          partIds,
          kinds,
          fjoinSpans,
          traceSample,
        });
      } finally {
        try {
          d.removeQuery('q');
        } catch {
          /* best-effort */
        }
        try {
          await (d as {destroy?: () => Promise<void> | void}).destroy?.();
        } catch {
          /* best-effort */
        }
        try {
          dbFile.delete();
        } catch {
          /* best-effort */
        }
        dbFile = undefined;
      }
    }

    // ---- Report ----------------------------------------------------------
    const mean = (a: number[]) => a.reduce((x, y) => x + y, 0) / a.length;
    const out: string[] = [];
    out.push(
      `\n=== FLIP-ADVANCE S=${S} P=${P} ME_MOD=${ME_MOD} LIMIT=${LIMIT ?? 'none'} reps=${REPS} @${new Date().toISOString()} ===`,
    );
    out.push('config       hydrate_ms  rows_total  channels  participants');
    for (const c of configs) {
      out.push(
        `${c.name}  ${c.hydrateMs.toFixed(0).padStart(9)}  ${String(c.rowsTotal).padStart(10)}  ` +
          `${String(c.rowsChannels).padStart(8)}  ${String(c.rowsParts).padStart(12)}`,
      );
    }
    // Row-count parity analysis.
    const [rustOn, rustOff, rustForced, tsOn, tsOff, tsForced] = configs;
    const parityChannels = new Set(configs.map(c => c.rowsChannels)).size === 1;
    const parityTotal = new Set(configs.map(c => c.rowsTotal)).size === 1;
    out.push(
      `parity: channels ${parityChannels ? 'EQUAL' : 'DIVERGENT'}, ` +
        `total ${parityTotal ? 'EQUAL' : 'DIVERGENT'}`,
    );
    if (!parityTotal || !parityChannels) {
      for (const [a, b] of [
        [rustOn, tsOn],
        [rustOff, tsOff],
        [rustForced, tsForced],
        [tsOn, tsOff],
        [tsForced, tsOff],
        [rustForced, rustOff],
      ] as const) {
        const chanOnly = [...a.channelIds].filter(x => !b.channelIds.has(x));
        const chanMiss = [...b.channelIds].filter(x => !a.channelIds.has(x));
        const partOnly = [...a.partIds].filter(x => !b.partIds.has(x));
        const partMiss = [...b.partIds].filter(x => !a.partIds.has(x));
        out.push(
          `  diff ${a.name.trim()} vs ${b.name.trim()}: ` +
            `channels +${chanOnly.length}/-${chanMiss.length} ` +
            `(e.g. +[${chanOnly.slice(0, 3)}] -[${chanMiss.slice(0, 3)}]) ` +
            `participants +${partOnly.length}/-${partMiss.length} ` +
            `(e.g. +[${partOnly.slice(0, 3)}] -[${partMiss.slice(0, 3)}])`,
        );
      }
    }
    // Flip-active evidence.
    for (const c of configs.filter(c => c.engine === 'rust')) {
      out.push(
        `trace ${c.name.trim()}: fjoin.batch_fetch spans=${c.fjoinSpans}` +
          (c.traceSample ? `  sample: ${c.traceSample.slice(0, 220)}` : ''),
      );
    }
    // Per-kind table.
    out.push('');
    out.push(
      'kind              rust_ON  rust_OFF  rust_FRC    ts_ON   ts_OFF   ts_FRC  ' +
        '| rustAmp(FRC/OFF)  tsAmp(FRC/OFF)  tsAmp(ON/OFF)  rust/ts@FRC',
    );
    for (const kind of KINDS) {
      const m = (c: ConfigResult) => mean(c.kinds[kind].ms);
      const resets = configs
        .map(c => c.kinds[kind].resets)
        .reduce((a, b) => a + b, 0);
      out.push(
        `${kind.padEnd(16)}  ${m(rustOn).toFixed(2).padStart(7)}  ${m(rustOff).toFixed(2).padStart(8)}  ` +
          `${m(rustForced).toFixed(2).padStart(8)}  ${m(tsOn).toFixed(2).padStart(7)}  ` +
          `${m(tsOff).toFixed(2).padStart(7)}  ${m(tsForced).toFixed(2).padStart(7)}  ` +
          `| ${(m(rustForced) / m(rustOff)).toFixed(2).padStart(16)}  ` +
          `${(m(tsForced) / m(tsOff)).toFixed(2).padStart(14)}  ` +
          `${(m(tsOn) / m(tsOff)).toFixed(2).padStart(13)}  ` +
          `${(m(rustForced) / m(tsForced)).toFixed(2).padStart(11)}` +
          (resets ? `  RESETS=${resets}` : ''),
      );
    }
    out.push('');
    out.push(
      'n changes emitted per rep (per kind, config order rON/rOFF/rFRC/tON/tOFF/tFRC):',
    );
    for (const kind of KINDS) {
      out.push(
        `  ${kind.padEnd(16)} ` +
          configs.map(c => `[${c.kinds[kind].n.join(',')}]`).join(' '),
      );
    }
    const report = out.join('\n') + '\n';
    // eslint-disable-next-line no-console
    console.log(report);
    appendFileSync(RESULTS_FILE, report);

    // FlippedJoin must actually be active in the rust FORCED config (trace
    // evidence) and inactive with planner OFF.
    expect(rustForced.fjoinSpans).toBeGreaterThan(0);
    expect(rustOff.fjoinSpans).toBe(0);
    // Decision parity: with the scanstatus cost model (the default), "rust ON"
    // makes the SAME flip decision as "ts ON" for any S/ME_MOD — whether that
    // decision is flip (selective 'me', e.g. ME_MOD=1000) or no-flip
    // (non-selective 'me', e.g. ME_MOD=4). Same decision ⇒ same emission mode
    // ⇒ identical row sets between the two planner-ON configs.
    expect(rustOn.rowsTotal).toBe(tsOn.rowsTotal);
    expect([...rustOn.channelIds].toSorted()).toEqual(
      [...tsOn.channelIds].toSorted(),
    );
    expect([...rustOn.partIds].toSorted()).toEqual(
      [...tsOn.partIds].toSorted(),
    );
    expect(configs.length).toBe(6);
  }, 900_000);
});
