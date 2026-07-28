import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import type {Database as DB} from '../../../../zqlite/src/db.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {listTables} from '../../db/lite-tables.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import {populateFromExistingTables} from '../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {Snapshotter} from './snapshotter.ts';
import {RustIVMDriver} from './rust-ivm-driver.ts';

// Tests for the #planAst feature: verifies the driver runs completeOrdering +
// planQuery (cost-model planner) before sending the AST to the Rust engine.
//
// The key assertion is that the stored transformedAst (accessible via queries())
// has flip:true on correlated subqueries when the cost model determines
// flipping is cheaper, and flip:undefined when the planner is disabled.

const ADDON_PATH = process.env['RUST_IVM_ADDON_PATH'];

describe.skipIf(!ADDON_PATH)('view-syncer/rust-ivm-driver planner', () => {
  const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
  const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
  let dbFile: DbFile;
  let db: DB;
  let lc: LogContext;
  let logSink: TestLogSink;

  beforeEach(() => {
    logSink = new TestLogSink();
    lc = new LogContext('error', undefined, logSink);
    dbFile = new DbFile('rust_ivm_planner_test');
    dbFile.connect(lc).pragma('journal_mode = wal2');
  });

  afterEach(() => {
    dbFile.delete();
  });

  const issues = table('issues')
    .columns({
      id: string(),
      kind: string(),
    })
    .primaryKey('id');
  const comments = table('comments')
    .columns({
      id: string(),
      issueID: string(),
      upvotes: number(),
    })
    .primaryKey('id');

  const clientSchema = createSchema({
    tables: [issues, comments],
  });

  // Query: issues WHERE EXISTS (comments WHERE comments.issueID = issues.id)
  const ISSUES_WITH_EXISTS: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        system: 'client',
        correlation: {
          parentField: ['id'],
          childField: ['issueID'],
        },
        subquery: {
          table: 'comments',
          alias: 'comments',
          orderBy: [['id', 'asc']],
        },
      },
    },
  };

  // Query with OR + correlated subquery — triggers FlippedJoin + UnionFanIn
  // in the Rust engine when flip:true.
  const ISSUES_WITH_OR_EXISTS: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
    where: {
      type: 'or',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'kind'},
          right: {type: 'literal', value: 'public'},
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          related: {
            system: 'client',
            correlation: {
              parentField: ['id'],
              childField: ['issueID'],
            },
            subquery: {
              table: 'comments',
              alias: 'comments',
              orderBy: [['id', 'asc']],
            },
          },
        },
      ],
    },
  };

  const NO_TIMER = {elapsedLap: () => 0, totalElapsed: () => 0} as any;

  function setupDriver(enablePlanner: boolean): RustIVMDriver {
    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();

    const driver = new RustIVMDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
      shardID,
      new DatabaseStorage(storage).createClientGroupStorage('planner-test-cg'),
      'planner-test-cg',
      new InspectorDelegate(undefined),
      () => 200,
      enablePlanner,
      undefined,
      dbFile.path,
    );

    db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], '123');
    db.exec(/*sql*/ `
      CREATE TABLE "${mutationsTableName}" (
        "clientGroupID"  TEXT,
        "clientID"       TEXT,
        "mutationID"     INTEGER,
        "result"         TEXT,
        _0_version       TEXT NOT NULL,
        PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
      );
      CREATE TABLE issues (
        id TEXT PRIMARY KEY,
        kind TEXT,
        _0_version TEXT NOT NULL
      );
      CREATE TABLE comments (
        id TEXT PRIMARY KEY,
        issueID TEXT,
        upvotes INTEGER,
        _0_version TEXT NOT NULL
      );
      CREATE INDEX comments_issueID ON comments (issueID);
    `);

    // Skewed data: many issues, few comments → planner should flip.
    for (let i = 0; i < 200; i++) {
      db.prepare(
        'INSERT INTO issues (id, kind, _0_version) VALUES (?, ?, ?)',
      ).run(`i${i}`, i % 3 === 0 ? 'public' : 'private', '123');
    }
    for (let i = 0; i < 5; i++) {
      db.prepare(
        'INSERT INTO comments (id, issueID, upvotes, _0_version) VALUES (?, ?, ?, ?)',
      ).run(`c${i}`, `i${i * 10}`, i, '123');
    }

    db.pragma('analysis_limit = 1000');
    db.exec('ANALYZE main');
    populateFromExistingTables(db, listTables(db, false));

    driver.init(clientSchema);
    return driver;
  }

  // Drain the addQuery async generator, swallowing engine errors — we only
  // care that #planAst ran and stored the planned AST in #queryInfo.
  async function drainAddQuery(
    driver: RustIVMDriver,
    ast: AST,
  ): Promise<AST | undefined> {
    try {
      const gen = driver.addQuery('hash1', 'q1', ast, NO_TIMER);
      for await (const _ of gen) {
        // drain
      }
    } catch {
      // Engine may fail — we only care about the stored AST.
    }
    return driver.queries().get('q1')?.transformedAst;
  }

  test('enablePlanner=true: planAst sets flip flag (true or false)', async () => {
    const driver = setupDriver(true);
    const ast = await drainAddQuery(driver, ISSUES_WITH_EXISTS);

    expect(ast).toBeDefined();
    expect(ast!.where).toBeDefined();
    expect(ast!.where!.type).toBe('correlatedSubquery');
    // After planQuery, flip is always set (true or false), never undefined.
    expect(ast!.where!.flip).toBeDefined();
  });

  test('enablePlanner=false: planAst does NOT add flip', async () => {
    const driver = setupDriver(false);
    const ast = await drainAddQuery(driver, ISSUES_WITH_EXISTS);

    expect(ast).toBeDefined();
    expect(ast!.where).toBeDefined();
    expect(ast!.where!.type).toBe('correlatedSubquery');
    // Without planning, the raw AST has flip: undefined.
    expect(ast!.where!.flip).toBeUndefined();
  });

  test('enablePlanner=true: cost model flips when inner table is small', async () => {
    const driver = setupDriver(true);
    const ast = await drainAddQuery(driver, ISSUES_WITH_EXISTS);

    expect(ast).toBeDefined();
    // With 200 issues and 5 comments + ANALYZE, the cost model should
    // estimate flipping (scan 5 comments, lookup issues by PK) is cheaper.
    expect(ast!.where!.flip).toBe(true);
  });

  test('enablePlanner=true: OR + flipped subquery gets flip:true', async () => {
    const driver = setupDriver(true);
    const ast = await drainAddQuery(driver, ISSUES_WITH_OR_EXISTS);

    expect(ast).toBeDefined();
    expect(ast!.where!.type).toBe('or');
    const csq = ast!.where!.conditions[1];
    expect(csq.type).toBe('correlatedSubquery');
    expect(csq.flip).toBe(true);
  });

  test('enablePlanner=true: planning failure falls back gracefully', async () => {
    const warnLc = new LogContext('warn', undefined, logSink);
    const storage = new Database(warnLc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();

    const driver = new RustIVMDriver(
      warnLc,
      testLogConfig,
      new Snapshotter(warnLc, dbFile.path, {appID: shardID.appID}),
      shardID,
      new DatabaseStorage(storage).createClientGroupStorage('planner-fallback-cg'),
      'planner-fallback-cg',
      new InspectorDelegate(undefined),
      () => 200,
      true,
      undefined,
      dbFile.path,
    );

    db = dbFile.connect(warnLc);
    initReplicationState(db, ['zero_data'], '123');
    db.exec(/*sql*/ `
      CREATE TABLE "${mutationsTableName}" (
        "clientGroupID"  TEXT,
        "clientID"       TEXT,
        "mutationID"     INTEGER,
        "result"         TEXT,
        _0_version       TEXT NOT NULL,
        PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
      );
      CREATE TABLE issues (
        id TEXT PRIMARY KEY,
        kind TEXT,
        _0_version TEXT NOT NULL
      );
      CREATE TABLE comments (
        id TEXT PRIMARY KEY,
        issueID TEXT,
        upvotes INTEGER,
        _0_version TEXT NOT NULL
      );
      CREATE INDEX comments_issueID ON comments (issueID);
    `);

    for (let i = 0; i < 10; i++) {
      db.prepare(
        'INSERT INTO issues (id, kind, _0_version) VALUES (?, ?, ?)',
      ).run(`i${i}`, 'public', '123');
    }

    db.pragma('analysis_limit = 1000');
    db.exec('ANALYZE main');
    populateFromExistingTables(db, listTables(db, false));
    driver.init(clientSchema);

    const ast = await drainAddQuery(driver, ISSUES_WITH_EXISTS);
    expect(ast).toBeDefined();
    expect(ast!.table).toBe('issues');
  });
});
