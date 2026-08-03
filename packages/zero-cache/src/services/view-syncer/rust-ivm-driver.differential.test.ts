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
import {DriverParityTrace} from './driver-parity-trace.ts';
import {PipelineDriver} from './pipeline-driver.ts';
import {RustIVMDriver} from './rust-ivm-driver.ts';
import {ResetPipelinesSignal, Snapshotter} from './snapshotter.ts';

// -----------------------------------------------------------------------------
// DRIVER-LEVEL DIFFERENTIAL: RustIVMDriver (the Rust port) vs PipelineDriver
// (the TS reference it is a drop-in replacement for), driven with identical
// input against the SAME wal2 replica.
//
// WHY this exists: the existing agentic fuzzer compares the raw napi ENGINE
// (engine.init directly) against a MemorySource oracle — it BYPASSES the driver
// glue where the real prod bugs have lived (buildNapiTableSpecs client-PK-vs-
// LiteSpec-PK rowKey derivation, #planAst, replicaVersion/currentVersion seeding,
// the streaming/queue plumbing). This test exercises exactly that seam: whatever
// the two drivers disagree on, byte-for-byte, is a divergence in the production
// path. The `messages -> messageId` PK-divergence case is the driver-level
// regression for the live disconnect bug (client PK != replica PK).
//
// Comparison is order-independent: neither driver guarantees emission order
// (both keep an XOR rowSetSignature precisely because of that), so we compare a
// multiset keyed by (table, canonical rowKey) AND cross-check rowSetSignature.
// -----------------------------------------------------------------------------

const ADDON_PATH = process.env['RUST_IVM_ADDON_PATH'];

const NO_TIMER = {elapsedLap: () => 0, totalElapsed: () => 0} as any;

/** Deterministic JSON with sorted keys (rowKey/row column order may differ). */
function stable(v: unknown): string {
  if (v === null || typeof v !== 'object') {
    return JSON.stringify(v);
  }
  const o = v as Record<string, unknown>;
  return `{${Object.keys(o)
    .sort()
    .map(k => `${JSON.stringify(k)}:${stable(o[k])}`)
    .join(',')}}`;
}

type Change = {
  type: number;
  queryID: string;
  table: string;
  rowKey: unknown;
  row: unknown;
};

/** Drain a sync OR async iterable of `RowChange | 'yield'`, dropping sentinels. */
async function drain(
  it: Iterable<unknown> | AsyncIterable<unknown>,
): Promise<Change[]> {
  const out: Change[] = [];
  for await (const c of it as AsyncIterable<unknown>) {
    if (c === 'yield') {
      continue;
    }
    out.push(c as Change);
  }
  return out;
}

/** Multiset keyed by (table, canonical rowKey) → sorted list of {type,row}. */
function multiset(changes: Change[]): Map<string, string[]> {
  const m = new Map<string, string[]>();
  for (const c of changes) {
    const key = `${c.table}\u0000${stable(c.rowKey)}`;
    const val = `${c.type}\u0000${stable(c.row ?? null)}`;
    const arr = m.get(key);
    if (arr) {
      arr.push(val);
    } else {
      m.set(key, [val]);
    }
  }
  for (const arr of m.values()) {
    arr.sort();
  }
  return m;
}

/** Assert two change streams are equal as multisets; returns nothing, throws on diff. */
function expectSameChanges(rust: Change[], ts: Change[], label: string) {
  const rm = multiset(rust);
  const tm = multiset(ts);
  // Symmetric diff for a readable failure message.
  const onlyRust: string[] = [];
  const onlyTs: string[] = [];
  for (const [k, rv] of rm) {
    const tv = tm.get(k);
    if (!tv || stable(rv) !== stable(tv)) {
      onlyRust.push(
        `${k} => ${JSON.stringify(rv)} (ts: ${JSON.stringify(tv)})`,
      );
    }
  }
  for (const [k, tv] of tm) {
    if (!rm.has(k)) {
      onlyTs.push(`${k} => ${JSON.stringify(tv)}`);
    }
  }
  expect(
    {label, onlyInRust: onlyRust, onlyInTs: onlyTs},
    `driver divergence [${label}]`,
  ).toEqual({label, onlyInRust: [], onlyInTs: []});
}

describe.skipIf(!ADDON_PATH)('view-syncer/rust-ivm-driver differential', () => {
  const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
  const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
  const BASE = '8400bivbkg';

  let dbFile: DbFile;
  let db: DB;
  let lc: LogContext;

  beforeEach(() => {
    lc = new LogContext('error', undefined, new TestLogSink());
    dbFile = new DbFile('rust_ivm_differential_test');
    dbFile.connect(lc).pragma('journal_mode = wal2');
  });

  afterEach(() => {
    dbFile.delete();
  });

  /** Seed the wal2 replica: mutations table + caller-supplied DDL/DML. */
  function seedReplica(ddlAndDml: string) {
    db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], BASE);
    db.exec(/*sql*/ `
      CREATE TABLE "${mutationsTableName}" (
        "clientGroupID"  TEXT,
        "clientID"       TEXT,
        "mutationID"     INTEGER,
        "result"         TEXT,
        _0_version       TEXT NOT NULL,
        PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
      );
    `);
    db.exec(ddlAndDml);
    populateFromExistingTables(db, listTables(db, false));
  }

  function newStorage() {
    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    return new DatabaseStorage(storage);
  }

  function makeRust(
    clientSchema: Parameters<PipelineDriver['init']>[0],
  ): RustIVMDriver {
    const d = new RustIVMDriver(
      lc,
      testLogConfig,
      shardID,
      newStorage().createClientGroupStorage('diff-rust-cg'),
      'diff-rust-cg',
      new InspectorDelegate(undefined),
      () => 200,
      false, // planner off — results must match regardless of flip
      undefined,
      dbFile.path,
    );
    d.init(clientSchema);
    return d;
  }

  function makeTs(
    clientSchema: Parameters<PipelineDriver['init']>[0],
  ): PipelineDriver {
    const d = new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
      shardID,
      newStorage().createClientGroupStorage('diff-ts-cg'),
      'diff-ts-cg',
      new InspectorDelegate(undefined),
      () => 200,
      false,
    );
    d.init(clientSchema);
    return d;
  }

  /** Hydrate the same query through both drivers and assert parity. */
  async function assertHydrateParity(
    clientSchema: Parameters<PipelineDriver['init']>[0],
    ast: AST,
    label: string,
  ) {
    const rust = makeRust(clientSchema);
    const ts = makeTs(clientSchema);
    try {
      // Cheap pre-check: both must agree on the version they hydrate at.
      expect(rust.replicaVersion, `${label}: replicaVersion`).toBe(
        ts.replicaVersion,
      );
      expect(rust.currentVersion(), `${label}: currentVersion`).toBe(
        ts.currentVersion(),
      );

      const rustChanges = await drain(rust.addQuery('h', 'q', ast, NO_TIMER));
      const tsChanges = await drain(ts.addQuery('h', 'q', ast, NO_TIMER));

      expectSameChanges(rustChanges, tsChanges, label);
      // The drivers' own order-independent XOR oracle must also agree.
      expect(rust.rowSetSignature('q'), `${label}: rowSetSignature`).toBe(
        ts.rowSetSignature('q'),
      );
    } finally {
      rust.removeQuery('q');
      ts.removeQuery('q');
    }
  }

  // --- Schemas ---------------------------------------------------------------

  const issues = table('issues')
    .columns({id: string(), kind: string()})
    .primaryKey('id');
  const comments = table('comments')
    .columns({id: string(), issueID: string(), body: string()})
    .primaryKey('id');
  const issuesCommentsCS = createSchema({tables: [issues, comments]});

  // PK-DIVERGENCE schema: the client primary key (`messageId`) is NOT the
  // SQLite table's PRIMARY KEY (`id`). This is the exact seam of the live
  // disconnect bug — the engine must key rowKeys by the CLIENT pk.
  const messages = table('messages')
    .columns({id: string(), messageId: string(), body: string()})
    .primaryKey('messageId');
  const messagesCS = createSchema({tables: [messages]});

  function seedIssuesComments() {
    seedReplica(/*sql*/ `
      CREATE TABLE issues (
        id TEXT PRIMARY KEY, kind TEXT, _0_version TEXT NOT NULL
      );
      CREATE TABLE comments (
        id TEXT PRIMARY KEY, issueID TEXT, body TEXT, _0_version TEXT NOT NULL
      );
      CREATE INDEX comments_issueID ON comments (issueID);
      INSERT INTO issues VALUES
        ('i1','public','${BASE}'), ('i2','private','${BASE}'), ('i3','public','${BASE}');
      INSERT INTO comments VALUES
        ('c1','i1','hello','${BASE}'), ('c2','i1','world','${BASE}'), ('c3','i3','hi','${BASE}');
    `);
  }

  function seedMessages() {
    // The SQLite/lite PRIMARY KEY is `id`; the CLIENT primary key is `messageId`
    // (a separate non-null unique index). This is the exact PK-divergence shape
    // of the live disconnect bug — spec.tableSpec.primaryKey resolves to `id`
    // while the client keys rowKeys by `messageId`. Lite nullability is carried
    // in the column TYPE string (`|NOT_NULL`), not SQLite's NOT NULL constraint,
    // so both id and messageId must be declared with the marker to qualify as
    // potential primary keys (see lite-tables.ts notNullColumns / client-schema
    // checkClientSchema allPotentialPrimaryKeys).
    seedReplica(/*sql*/ `
      CREATE TABLE messages (
        id        "text|NOT_NULL" PRIMARY KEY,
        messageId "text|NOT_NULL",
        body      "text",
        _0_version TEXT NOT NULL
      );
      CREATE UNIQUE INDEX messages_messageId ON messages (messageId);
      INSERT INTO messages VALUES
        ('row-1','m-100','a','${BASE}'),
        ('row-2','m-200','b','${BASE}'),
        ('row-3','m-300','c','${BASE}');
    `);
  }

  // --- Hydrate parity --------------------------------------------------------

  test('hydrate: simple filtered select', async () => {
    seedIssuesComments();
    const ast: AST = {
      table: 'issues',
      orderBy: [['id', 'asc']],
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'kind'},
        right: {type: 'literal', value: 'public'},
      },
    };
    await assertHydrateParity(issuesCommentsCS, ast, 'simple-filter');
  });

  test('hydrate: empty query preserves undefined row-set signature', async () => {
    seedIssuesComments();
    const ast: AST = {
      table: 'issues',
      orderBy: [['id', 'asc']],
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'kind'},
        right: {type: 'literal', value: 'missing'},
      },
    };
    const rust = makeRust(issuesCommentsCS);
    const ts = makeTs(issuesCommentsCS);
    try {
      expect(await drain(rust.addQuery('h', 'q', ast, NO_TIMER))).toEqual([]);
      expect(await drain(ts.addQuery('h', 'q', ast, NO_TIMER))).toEqual([]);
      expect(rust.rowSetSignature('q')).toBeUndefined();
      expect(rust.rowSetSignature('q')).toBe(ts.rowSetSignature('q'));
    } finally {
      rust.removeQuery('q');
      ts.removeQuery('q');
      await rust.destroy();
      ts.destroy();
    }
  });

  test('hydrate: PK divergence — rowKey must be the CLIENT pk (messageId)', async () => {
    seedMessages();
    const ast: AST = {table: 'messages', orderBy: [['messageId', 'asc']]};

    // Parity vs the reference driver.
    await assertHydrateParity(messagesCS, ast, 'pk-divergence');

    // And an explicit assertion of the actual invariant: rowKeys are keyed by
    // messageId (client pk), NOT id (replica pk). This is what the client's
    // toPrimaryKeyString validates; keying by `id` is the live disconnect bug.
    const rust = makeRust(messagesCS);
    try {
      const changes = await drain(rust.addQuery('h', 'q', ast, NO_TIMER));
      expect(changes.length).toBe(3);
      for (const c of changes) {
        expect(Object.keys(c.rowKey as object)).toEqual(['messageId']);
        expect((c.rowKey as any).id).toBeUndefined();
      }
    } finally {
      rust.removeQuery('q');
    }
  });

  test('hydrate: EXISTS correlated subquery', async () => {
    seedIssuesComments();
    const ast: AST = {
      table: 'issues',
      orderBy: [['id', 'asc']],
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          system: 'client',
          correlation: {parentField: ['id'], childField: ['issueID']},
          subquery: {
            table: 'comments',
            alias: 'comments',
            orderBy: [['id', 'asc']],
          },
        },
      },
    };
    await assertHydrateParity(issuesCommentsCS, ast, 'exists');
  });

  test('hydrate: query with related children (companion rows)', async () => {
    seedIssuesComments();
    const ast: AST = {
      table: 'issues',
      orderBy: [['id', 'asc']],
      related: [
        {
          system: 'client',
          correlation: {parentField: ['id'], childField: ['issueID']},
          subquery: {
            table: 'comments',
            alias: 'comments',
            orderBy: [['id', 'asc']],
          },
        },
      ],
    };
    await assertHydrateParity(issuesCommentsCS, ast, 'related-children');
  });

  // --- Advance / push parity -------------------------------------------------

  /** Commit a replica write at `version` and bump the head watermark. */
  function commitAt(version: string, sql: string) {
    db.exec(/*sql*/ `
      ${sql}
      UPDATE "_zero.replicationState" SET stateVersion = '${version}';
    `);
  }

  async function drainAdvance(
    d: RustIVMDriver | PipelineDriver,
  ): Promise<{reset: boolean; changes: Change[]}> {
    try {
      const res = await d.advance(NO_TIMER);
      if (res instanceof ResetPipelinesSignal) {
        return {reset: true, changes: []};
      }
      return {reset: false, changes: await drain(res.changes)};
    } catch (e) {
      if (e instanceof ResetPipelinesSignal) {
        return {reset: true, changes: []};
      }
      throw e;
    }
  }

  test('advance: insert + delete propagate identically', async () => {
    seedIssuesComments();
    const ast: AST = {table: 'issues', orderBy: [['id', 'asc']]};
    const rust = makeRust(issuesCommentsCS);
    const ts = makeTs(issuesCommentsCS);
    try {
      // Hydrate both first (advance diffs against the hydrated state).
      const h = 'h';
      await drain(rust.addQuery(h, 'q', ast, NO_TIMER));
      await drain(ts.addQuery(h, 'q', ast, NO_TIMER));

      const V1 = '8500000001';
      commitAt(
        V1,
        /*sql*/ `
        INSERT INTO issues VALUES ('i4','public','${V1}');
        DELETE FROM issues WHERE id = 'i2';
        INSERT INTO "_zero.changeLog2" VALUES
          ('${V1}', 0, 'issues', json('{"id":"i4"}'), 's', '{}'),
          ('${V1}', 1, 'issues', json('{"id":"i2"}'), 'd', '{}');
      `,
      );

      const r = await drainAdvance(rust);
      const t = await drainAdvance(ts);

      expect(r.reset, 'reset agreement').toBe(t.reset);
      if (!r.reset) {
        expect(r.changes.length).toBeGreaterThan(0);
        expectSameChanges(r.changes, t.changes, 'advance-insert-delete');
      }
      expect(rust.rowSetSignature('q'), 'post-advance rowSetSignature').toBe(
        ts.rowSetSignature('q'),
      );
      expect(rust.currentVersion(), 'post-advance currentVersion').toBe(
        ts.currentVersion(),
      );
    } finally {
      rust.removeQuery('q');
      ts.removeQuery('q');
    }
  });

  test('advance: PK-divergence table push keys removes by client pk', async () => {
    seedMessages();
    const ast: AST = {table: 'messages', orderBy: [['messageId', 'asc']]};
    const rust = makeRust(messagesCS);
    const ts = makeTs(messagesCS);
    try {
      await drain(rust.addQuery('h', 'q', ast, NO_TIMER));
      await drain(ts.addQuery('h', 'q', ast, NO_TIMER));

      const V1 = '8500000001';
      commitAt(
        V1,
        /*sql*/ `
        INSERT INTO messages VALUES ('row-4','m-400','d','${V1}');
        DELETE FROM messages WHERE id = 'row-2';
        INSERT INTO "_zero.changeLog2" VALUES
          ('${V1}', 0, 'messages', json('{"id":"row-4"}'), 's', '{}'),
          ('${V1}', 1, 'messages', json('{"id":"row-2"}'), 'd', '{}');
      `,
      );

      const r = await drainAdvance(rust);
      const t = await drainAdvance(ts);

      expect(r.reset).toBe(t.reset);
      if (!r.reset) {
        expect(r.changes.length).toBeGreaterThan(0);
        expectSameChanges(r.changes, t.changes, 'advance-pk-divergence');
        // Every emitted rowKey must be a messageId, never an id.
        for (const c of r.changes) {
          expect(Object.keys(c.rowKey as object)).toEqual(['messageId']);
        }
      }
      expect(rust.rowSetSignature('q')).toBe(ts.rowSetSignature('q'));
    } finally {
      rust.removeQuery('q');
      ts.removeQuery('q');
    }
  });

  test('public trace: init, hydrate, getRow, advance, remove, and re-add match exactly', async () => {
    seedMessages();
    const ast: AST = {table: 'messages', orderBy: [['messageId', 'asc']]};
    const rust = makeRust(messagesCS);
    const ts = makeTs(messagesCS);
    const rustTrace = new DriverParityTrace(rust);
    const tsTrace = new DriverParityTrace(ts);

    try {
      rustTrace.recordState('initialized');
      tsTrace.recordState('initialized');

      await rustTrace.addQuery('transform-v1', 'q', ast, NO_TIMER, 'messages');
      await tsTrace.addQuery('transform-v1', 'q', ast, NO_TIMER, 'messages');
      expect(rustTrace.hydrationTimeMs()).toBeGreaterThanOrEqual(0);
      expect(tsTrace.hydrationTimeMs()).toBeGreaterThanOrEqual(0);

      await rustTrace.getRow('messages', {messageId: 'm-200'});
      await tsTrace.getRow('messages', {messageId: 'm-200'});
      await rustTrace.getRow('messages', {messageId: 'missing'});
      await tsTrace.getRow('messages', {messageId: 'missing'});

      const V1 = '8500000001';
      commitAt(
        V1,
        /*sql*/ `
        INSERT INTO messages VALUES ('row-4','m-400','d','${V1}');
        DELETE FROM messages WHERE id = 'row-2';
        INSERT INTO "_zero.changeLog2" VALUES
          ('${V1}', 0, 'messages', json('{"id":"row-4"}'), 's', '{}'),
          ('${V1}', 1, 'messages', json('{"id":"row-2"}'), 'd', '{}');
      `,
      );
      await rustTrace.advance(NO_TIMER);
      await tsTrace.advance(NO_TIMER);

      await rustTrace.removeQuery('q');
      await tsTrace.removeQuery('q');
      expect(rustTrace.hydrationTimeMs()).toBe(0);
      expect(tsTrace.hydrationTimeMs()).toBe(0);

      await rustTrace.addQuery(
        'transform-v2',
        'q',
        ast,
        NO_TIMER,
        'messages-v2',
      );
      await tsTrace.addQuery('transform-v2', 'q', ast, NO_TIMER, 'messages-v2');

      // Adding the same query ID is a replacement, including its metadata,
      // row-set signature, native pipeline, and hydration-time contribution.
      await rustTrace.addQuery(
        'transform-v3',
        'q',
        ast,
        NO_TIMER,
        'messages-v3',
      );
      await tsTrace.addQuery('transform-v3', 'q', ast, NO_TIMER, 'messages-v3');

      await rustTrace.reset(messagesCS);
      await tsTrace.reset(messagesCS);
      expect(rustTrace.hydrationTimeMs()).toBe(0);
      expect(tsTrace.hydrationTimeMs()).toBe(0);
      expect(Object.is(rustTrace.hydrationTimeMs(), -0)).toBe(false);

      await rustTrace.addQuery(
        'transform-v4',
        'q',
        ast,
        NO_TIMER,
        'after-reset',
      );
      await tsTrace.addQuery('transform-v4', 'q', ast, NO_TIMER, 'after-reset');
      await rustTrace.removeQuery('q');
      await tsTrace.removeQuery('q');

      expect(rustTrace.events()).toEqual(tsTrace.events());
    } finally {
      rust.removeQuery('q');
      ts.removeQuery('q');
      await rust.destroy();
      ts.destroy();
    }
  });
});
