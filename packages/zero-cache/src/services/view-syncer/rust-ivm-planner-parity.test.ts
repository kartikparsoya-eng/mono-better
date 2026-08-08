/**
 * PLANNER COST-MODEL DECISION PARITY — rust vs TS on ONE real wal2 replica.
 *
 * The rust planner's graph algorithm is already proven identical to TS
 * (planner_oracle_test on mock costs). This test closes the remaining seam:
 * the COST MODEL. It seeds a single wal2 replica (channels /
 * channel_participants, ANALYZE'd so stat1+stat4 exist), then for a battery
 * of AST shapes obtains
 *
 *   (i)  TS flip decisions:   planQuery + createSQLiteCostModel on the replica
 *   (ii) rust flip decisions: the native engine's planAst (scanstatus cost
 *        model on the snapshot connection — the production default)
 *
 * and asserts the flip vectors are IDENTICAL per AST (canonical order: WHERE
 * pre-order recursing into subquery wheres, then `related` in order — the
 * same order RustIVMDriver#planAst applies flips in).
 *
 * It also proves the old COUNT(*) model (env escape hatch
 * RUST_IVM_PLANNER_COST_MODEL=count) DISAGREES on the selective-EXISTS shape
 * — guarding against a silent regression to the filter-blind model.
 *
 * Run:
 *   bash packages/rust-ivm/scripts/build-local-wal2.sh   # wal2 + SCANSTATUS + STAT4
 *   cd packages/zero-cache && PLANNER_PARITY=1 pnpm vitest run rust-ivm-planner-parity
 *
 * Knobs: PARITY_S (channels, default 5000), PARITY_ME_MOD (default 100).
 */
import './rust-ivm-addon-setup.ts'; // MUST be first: guarantees the wal2 addon.
import {createRequire} from 'node:module';
import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {
  AST,
  Condition,
  CompoundKey,
} from '../../../../zero-protocol/src/ast.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import {planQuery} from '../../../../zql/src/planner/planner-builder.ts';
import {completeOrdering} from '../../../../zql/src/query/complete-ordering.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {createSQLiteCostModel} from '../../../../zqlite/src/sqlite-cost-model.ts';
import {computeZqlSpecs, listTables} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../../db/specs.ts';
import {DbFile} from '../../test/lite.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import {populateFromExistingTables} from '../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {buildNapiTableSpecs} from './rust-ivm-driver.ts';

const ADDON_PATH = process.env['RUST_IVM_ADDON_PATH'];
const RUN = !!process.env['PLANNER_PARITY'];
const require = createRequire(import.meta.url);

const S = Number(process.env['PARITY_S'] ?? 5000); // channels
const P = 3; // filler participants per channel
const ME_MOD = Number(process.env['PARITY_ME_MOD'] ?? 100);

type NativeEngine = {
  initSnapshotter(dbPath: string, appID: string): void;
  init(
    tableSpecs: ReturnType<typeof buildNapiTableSpecs>,
    dbPath: string | null,
    appID: string,
  ): void;
  planAst(astJSON: string): string;
  destroy(): Promise<void>;
};

/**
 * Canonical flip extraction — MUST match rust `flip_order`
 * (planner/runtime.rs) and RustIVMDriver applyFlips: WHERE conditions
 * pre-order (recursing into each correlated subquery's own where), then the
 * `related` subqueries in order.
 */
function extractFlips(ast: AST): (boolean | null)[] {
  const flips: (boolean | null)[] = [];
  const walkCond = (cond: Condition): void => {
    switch (cond.type) {
      case 'simple':
        return;
      case 'correlatedSubquery': {
        flips.push(cond.flip ?? null);
        const subWhere = cond.related.subquery.where;
        if (subWhere) {
          walkCond(subWhere);
        }
        return;
      }
      case 'and':
      case 'or':
        cond.conditions.forEach(walkCond);
        return;
    }
  };
  if (ast.where) {
    walkCond(ast.where);
  }
  for (const r of ast.related ?? []) {
    flips.push(...extractFlips(r.subquery));
  }
  return flips;
}

describe.skipIf(!ADDON_PATH || !RUN)(
  'rust-ivm planner cost-model parity',
  () => {
    const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
    const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
    const BASE = '8400bivbkg';

    const lc = new LogContext('error', undefined, new TestLogSink());
    let dbFile: DbFile;
    let db: Database;
    let engine: NativeEngine | undefined;
    let priorCostModelEnv: string | undefined;

    const cid = (i: number) => `c${String(i).padStart(6, '0')}`;

    beforeEach(() => {
      priorCostModelEnv = process.env['RUST_IVM_PLANNER_COST_MODEL'];
      delete process.env['RUST_IVM_PLANNER_COST_MODEL'];
      dbFile = new DbFile('planner_parity');
      db = dbFile.connect(lc);
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
        insCh.run(
          c,
          'w1',
          i % 2 === 0 ? 'PRIVATE' : 'PUBLIC',
          `chan ${i}`,
          BASE,
        );
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
    });

    afterEach(async () => {
      if (priorCostModelEnv === undefined) {
        delete process.env['RUST_IVM_PLANNER_COST_MODEL'];
      } else {
        process.env['RUST_IVM_PLANNER_COST_MODEL'] = priorCostModelEnv;
      }
      try {
        await engine?.destroy();
      } catch {
        /* best-effort */
      }
      engine = undefined;
      try {
        db.close();
      } catch {
        /* best-effort */
      }
      try {
        dbFile.delete();
      } catch {
        /* best-effort */
      }
    });

    // ---- AST battery -------------------------------------------------------

    const exists = (
      childWhere?: Condition,
      op: 'EXISTS' | 'NOT EXISTS' = 'EXISTS',
      flip?: boolean,
    ): Condition => ({
      type: 'correlatedSubquery',
      op,
      ...(flip !== undefined ? {flip} : {}),
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
          ...(childWhere ? {where: childWhere} : {}),
        },
      },
    });

    const userEq = (userId: string): Condition => ({
      type: 'simple',
      left: {type: 'column', name: 'userId'},
      op: '=',
      right: {type: 'literal', value: userId},
    });

    const visibilityEq = (v: string): Condition => ({
      type: 'simple',
      left: {type: 'column', name: 'visibility'},
      op: '=',
      right: {type: 'literal', value: v},
    });

    const channels = (extra: Partial<AST>): AST => ({
      table: 'channels',
      orderBy: [['id', 'asc']],
      ...extra,
    });

    const CASES: {name: string; ast: AST}[] = [
      {
        name: 'plain-exists',
        ast: channels({where: exists()}),
      },
      {
        name: 'selective-exists (userId=me on big child)',
        ast: channels({where: exists(userEq('me'))}),
      },
      {
        name: 'or-public-exists (prod userAllChannels shape)',
        ast: channels({
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
                conditions: [visibilityEq('PUBLIC'), exists(userEq('me'))],
              },
            ],
          },
        }),
      },
      {
        name: 'nested-exists',
        ast: channels({
          where: exists({
            type: 'and',
            conditions: [
              userEq('me'),
              {
                type: 'correlatedSubquery',
                op: 'EXISTS',
                related: {
                  system: 'client',
                  correlation: {
                    parentField: ['channelId'] as CompoundKey,
                    childField: ['id'] as CompoundKey,
                  },
                  subquery: {
                    table: 'channels',
                    alias: 'zsubq_channel',
                    orderBy: [['id', 'asc']],
                    where: visibilityEq('PUBLIC'),
                  },
                },
              },
            ],
          }),
        }),
      },
      {
        name: 'not-exists',
        ast: channels({where: exists(userEq('me'), 'NOT EXISTS')}),
      },
      {
        name: 'manual-flip-true',
        ast: channels({where: exists(userEq('me'), 'EXISTS', true)}),
      },
      {
        name: 'manual-flip-false',
        ast: channels({where: exists(userEq('me'), 'EXISTS', false)}),
      },
      {
        name: 'or-two-exists',
        ast: channels({
          where: {
            type: 'or',
            conditions: [exists(userEq('me')), exists(userEq('u0'))],
          },
        }),
      },
      {
        name: 'limit+related (paginated shape)',
        ast: channels({
          limit: 100,
          where: exists(userEq('me')),
          related: [
            {
              system: 'client',
              correlation: {
                parentField: ['id'] as CompoundKey,
                childField: ['channelId'] as CompoundKey,
              },
              subquery: {
                table: 'channel_participants',
                alias: 'participants',
                orderBy: [['id', 'asc']],
              },
            },
          ],
        }),
      },
    ];

    test(`decision parity on S=${S} ME_MOD=${ME_MOD}`, () => {
      // ---- TS side ---------------------------------------------------------
      const tableSpecs = new Map<string, LiteAndZqlSpec>();
      const fullTables = new Map<string, LiteTableSpec>();
      computeZqlSpecs(
        lc,
        db,
        {includeBackfillingColumns: false},
        tableSpecs,
        fullTables,
      );
      const primaryKeys = new Map<string, PrimaryKey>();
      for (const [table, spec] of tableSpecs.entries()) {
        primaryKeys.set(table, spec.tableSpec.primaryKey as PrimaryKey);
      }
      const getPK = (t: string): PrimaryKey => {
        const pk = primaryKeys.get(t);
        if (!pk) throw new Error(`no PK for ${t}`);
        return pk;
      };
      const costModel = createSQLiteCostModel(db, tableSpecs);

      // ---- rust side -------------------------------------------------------
      const {RustIvmEngine} = require(ADDON_PATH!) as {
        RustIvmEngine: new () => NativeEngine;
      };
      engine = new RustIvmEngine();
      engine.initSnapshotter(dbFile.path, shardID.appID);
      // Same table-spec path as production RustIVMDriver init.
      engine.init(
        buildNapiTableSpecs(tableSpecs, primaryKeys),
        dbFile.path,
        shardID.appID,
      );

      const report: string[] = [];
      for (const {name, ast} of CASES) {
        const ordered = completeOrdering(ast, getPK);
        const tsFlips = extractFlips(planQuery(ordered, costModel));
        const rustFlips = JSON.parse(
          engine.planAst(JSON.stringify(ordered)),
        ) as (boolean | null)[];

        report.push(`${name}: ts=[${tsFlips}] rust=[${rustFlips}]`);
        expect(rustFlips, `case '${name}' flip vector`).toEqual(tsFlips);
      }
      // eslint-disable-next-line no-console
      console.log(
        `\nPLANNER PARITY (S=${S}, ME_MOD=${ME_MOD}):\n  ${report.join('\n  ')}\n`,
      );

      // ---- COUNT-model regression guard ------------------------------------
      // The selective EXISTS on the (bigger) child table is the shape the old
      // filter-blind COUNT(*) model provably refuses to flip while the
      // scanstatus model (and TS) flip it. If this stops disagreeing, either
      // the escape hatch broke or the default silently regressed to COUNT.
      const selective = completeOrdering(CASES[1].ast, getPK);
      const tsSelective = extractFlips(planQuery(selective, costModel));
      expect(tsSelective, 'TS must flip the selective-EXISTS shape').toEqual([
        true,
      ]);

      process.env['RUST_IVM_PLANNER_COST_MODEL'] = 'count';
      try {
        const countFlips = JSON.parse(
          engine.planAst(JSON.stringify(selective)),
        ) as (boolean | null)[];
        expect(
          countFlips,
          'the COUNT(*) escape-hatch model must NOT flip this shape (it is filter-blind)',
        ).toEqual([false]);
      } finally {
        delete process.env['RUST_IVM_PLANNER_COST_MODEL'];
      }
      const defaultFlips = JSON.parse(
        engine.planAst(JSON.stringify(selective)),
      ) as (boolean | null)[];
      expect(defaultFlips, 'default model must match TS again').toEqual(
        tsSelective,
      );
    }, 300_000);
  },
);
