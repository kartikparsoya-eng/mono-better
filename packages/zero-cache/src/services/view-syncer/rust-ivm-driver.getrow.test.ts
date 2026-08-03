import './rust-ivm-addon-setup.ts'; // MUST be first: guarantees the wal2 addon.
import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  json,
  number,
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
import {canonicalValue, errorTrace} from './driver-parity-trace.ts';
import {PipelineDriver} from './pipeline-driver.ts';
import {drain} from './rust-ivm-differential-harness.ts';
import {RustIVMDriver} from './rust-ivm-driver.ts';
import {Snapshotter} from './snapshotter.ts';

// Regression for review finding #1: getRow() must apply the SAME column
// projection and fromSQLiteType semantics as the TS PipelineDriver/TableSource,
// NOT a raw `SELECT *` with direct SQLite value serialization (booleans as 0/1,
// json as a string, unsynced columns leaked). The TS PipelineDriver is the
// executable spec: RustIVMDriver.getRow MUST return byte-identical results.
const ADDON_PATH = process.env['RUST_IVM_ADDON_PATH'];

describe.skipIf(!ADDON_PATH)('view-syncer/rust-ivm-driver getRow', () => {
  const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
  const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
  const BASE = '8400bivbkg';
  let dbFile: DbFile;
  let db: DB;
  let lc: LogContext;

  const widgets = table('widgets')
    .columns({
      id: string(),
      active: boolean().optional(),
      count: number().optional(),
      payload: json().optional(),
      label: string().optional(),
    })
    .primaryKey('id');
  const clientSchema = createSchema({tables: [widgets]});

  beforeEach(() => {
    lc = new LogContext('error', undefined, new TestLogSink());
    dbFile = new DbFile('rust_ivm_getrow_test');
    dbFile.connect(lc).pragma('journal_mode = wal2');
  });
  afterEach(() => dbFile.delete());

  function newStorage(name: string) {
    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    return new DatabaseStorage(storage).createClientGroupStorage(name);
  }

  function setup(): {rust: RustIVMDriver; ts: PipelineDriver} {
    const rust = new RustIVMDriver(
      lc,
      testLogConfig,
      shardID,
      newStorage('getrow-rust'),
      'getrow-rust',
      new InspectorDelegate(undefined),
      () => 200,
      false,
      undefined,
      dbFile.path,
    );
    const ts = new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
      shardID,
      newStorage('getrow-ts'),
      'getrow-ts',
      new InspectorDelegate(undefined),
      () => 200,
      false,
    );
    db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], BASE);
    db.exec(/*sql*/ `
      CREATE TABLE "${mutationsTableName}" (
        "clientGroupID" TEXT, "clientID" TEXT, "mutationID" INTEGER,
        "result" TEXT, _0_version TEXT NOT NULL,
        PRIMARY KEY ("clientGroupID","clientID","mutationID")
      );
      CREATE TABLE widgets (
        id      "text|NOT_NULL" PRIMARY KEY,
        active  "bool",
        count   "int",
        payload "json",
        label   "text",
        server_only "text",
        unsynced_blob BLOB DEFAULT X'0102',
        _0_version "text|NOT_NULL"
      );
      INSERT INTO widgets
        (id, active, count, payload, label, server_only, _0_version) VALUES
        ('w1', 1, 1, '{"x":1,"y":[2,3]}', 'base', 'hidden', '${BASE}'),
        ('nulls', NULL, NULL, NULL, NULL, 'hidden', '${BASE}'),
        ('bool-zero', 0, 9007199254740991, 'true', 'zero', 'hidden', '${BASE}'),
        ('bool-other', 2, -9007199254740991, '42', 'other', 'hidden', '${BASE}'),
        ('bool-real', 0.5, 1.5, '[1,"two",null]', 'real', 'hidden', '${BASE}'),
        ('bool-text-empty', CAST('' AS TEXT), 0, '"scalar"', 'empty', 'hidden', '${BASE}'),
        ('bool-text', CAST('false' AS TEXT), 0, json_object('escaped', 'quote''slash\\'),
          char(0) || 'nul-हैलो-世界', 'hidden', '${BASE}'),
        ('bool-blob', X'00FF', 0, '{}', 'blob', 'hidden', '${BASE}'),
        ('number-high', 1, 9007199254740992, '{}', 'high', 'hidden', '${BASE}'),
        ('number-low', 1, -9007199254740992, '{}', 'low', 'hidden', '${BASE}'),
        ('invalid-json', 1, 0, '{bad', 'bad', 'hidden', '${BASE}');
    `);
    populateFromExistingTables(db, listTables(db, false));
    rust.init(clientSchema);
    ts.init(clientSchema);
    return {rust, ts};
  }

  const NO_TIMER = {elapsedLap: () => 0, totalElapsed: () => 0} as any;

  const astForID = (id: string) =>
    ({
      table: 'widgets',
      orderBy: [['id', 'asc']],
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'id'},
        right: {type: 'literal', value: id},
      },
    }) as any;

  async function observeHydrate(
    driver: RustIVMDriver | PipelineDriver,
    id: string,
  ) {
    try {
      return {
        status: 'ok',
        value: canonicalValue(
          await drain(
            driver.addQuery(`hydrate-${id}`, 'q', astForID(id), NO_TIMER),
          ),
        ),
      } as const;
    } catch (error) {
      return {status: 'error', error: errorTrace(error)} as const;
    }
  }

  async function observeAdvance(driver: RustIVMDriver | PipelineDriver) {
    try {
      const result = await driver.advance(NO_TIMER);
      if (result instanceof Error) {
        return {status: 'reset', error: errorTrace(result)} as const;
      }
      return {
        status: 'ok',
        value: canonicalValue({
          version: result.version,
          numChanges: result.numChanges,
          changes: await drain(result.changes),
        }),
      } as const;
    } catch (error) {
      return {status: 'error', error: errorTrace(error)} as const;
    }
  }

  test('getRow matches PipelineDriver (projection + fromSQLiteType)', async () => {
    const {rust, ts} = setup();
    // getRow requires the table's source to exist — add+drain a query first.
    const ast = {
      table: 'widgets',
      orderBy: [['id', 'asc']],
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'id'},
        right: {type: 'literal', value: 'w1'},
      },
    } as any;
    for (const _ of ts.addQuery('h', 'q', ast, NO_TIMER)) {
      /* drain */
    }
    for await (const _ of rust.addQuery('h', 'q', ast, NO_TIMER)) {
      /* drain */
    }

    const rustRow = rust.getRow('widgets', {id: 'w1'});
    const tsRow = ts.getRow('widgets', {id: 'w1'});

    // Executable-spec parity: rust must equal the TS reference exactly.
    expect(rustRow).toEqual(tsRow);
    // And the value semantics the raw SELECT * path got wrong:
    expect(rustRow!.active).toBe(tsRow!.active);
    expect(rustRow!.active).toBe(true); // boolean, not 0/1
    expect(rustRow!.payload).toEqual({x: 1, y: [2, 3]}); // parsed json, not a string
    // Missing row parity.
    expect(rust.getRow('widgets', {id: 'nope'})).toEqual(
      ts.getRow('widgets', {id: 'nope'}),
    );
  });

  test('getRow boundary matrix matches exact TS values and errors', async () => {
    const {rust, ts} = setup();
    const ast = {
      table: 'widgets',
      orderBy: [['id', 'asc']],
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'id'},
        right: {type: 'literal', value: 'w1'},
      },
    } as any;
    for (const _ of ts.addQuery('h', 'q', ast, NO_TIMER)) {
      /* drain */
    }
    for await (const _ of rust.addQuery('h', 'q', ast, NO_TIMER)) {
      /* drain */
    }

    const observe = (read: () => unknown) => {
      try {
        return {status: 'ok', value: canonicalValue(read())};
      } catch (error) {
        return {status: 'error', error: errorTrace(error)};
      }
    };
    const ids = [
      'nulls',
      'bool-zero',
      'bool-other',
      'bool-real',
      'bool-text-empty',
      'bool-text',
      'bool-blob',
      'number-high',
      'number-low',
      'invalid-json',
    ];
    for (const id of ids) {
      expect(
        observe(() => rust.getRow('widgets', {id})),
        `getRow boundary mismatch for ${id}`,
      ).toEqual(observe(() => ts.getRow('widgets', {id})));
    }

    expect(rust.getRow('widgets', {id: 'nulls'})).toMatchObject({
      id: 'nulls',
      active: null,
      count: null,
      payload: null,
      label: null,
    });
    expect(rust.getRow('widgets', {id: 'bool-zero'})?.active).toBe(false);
    expect(rust.getRow('widgets', {id: 'bool-other'})?.active).toBe(true);
    expect(rust.getRow('widgets', {id: 'bool-real'})?.active).toBe(true);
    expect(rust.getRow('widgets', {id: 'bool-text-empty'})?.active).toBe(false);
    expect(rust.getRow('widgets', {id: 'bool-text'})?.active).toBe(true);
    expect(rust.getRow('widgets', {id: 'bool-blob'})?.active).toBe(true);
    expect(rust.getRow('widgets', {id: 'bool-text'})?.label).toBe(
      '\0nul-हैलो-世界',
    );
    expect(rust.getRow('widgets', {id: 'bool-text'})).toHaveProperty(
      'server_only',
      'hidden',
    );
    expect(rust.getRow('widgets', {id: 'bool-text'})).not.toHaveProperty(
      'unsynced_blob',
    );
  });

  test('hydration boundary matrix matches TS values and failure state', async () => {
    const {rust, ts} = setup();
    try {
      for (const id of [
        'nulls',
        'bool-zero',
        'bool-other',
        'bool-real',
        'bool-text-empty',
        'bool-text',
        'bool-blob',
      ]) {
        expect(
          await observeHydrate(rust, id),
          `hydrate boundary mismatch for ${id}`,
        ).toEqual(await observeHydrate(ts, id));
      }

      for (const id of ['number-high', 'number-low', 'invalid-json']) {
        const rustResult = await observeHydrate(rust, id);
        const tsResult = await observeHydrate(ts, id);
        expect(rustResult.status, `Rust must reject ${id}`).toBe('error');
        expect(tsResult.status, `TS must reject ${id}`).toBe('error');
        expect(rust.queries().has('q')).toBe(false);
        expect(ts.queries().has('q')).toBe(false);
        expect(rust.rowSetSignature('q')).toBeUndefined();
        expect(ts.rowSetSignature('q')).toBeUndefined();
      }
    } finally {
      await rust.destroy();
      ts.destroy();
    }
  });

  test('advance converts edited boundary values identically', async () => {
    const {rust, ts} = setup();
    try {
      expect(await observeHydrate(rust, 'w1')).toEqual(
        await observeHydrate(ts, 'w1'),
      );

      const version = '8500000001';
      db.exec(/*sql*/ `
        UPDATE widgets SET
          active = 2,
          count = 9007199254740991,
          payload = json_object('array', json_array(1, 'two', NULL)),
          label = char(0) || 'advance-हैलो-世界',
          _0_version = '${version}'
        WHERE id = 'w1';
        INSERT INTO "_zero.changeLog2" VALUES
          ('${version}', 0, 'widgets', json('{"id":"w1"}'), 's', '{}');
        UPDATE "_zero.replicationState" SET stateVersion = '${version}';
      `);

      expect(await observeAdvance(rust)).toEqual(await observeAdvance(ts));
      expect(rust.getRow('widgets', {id: 'w1'})).toEqual(
        ts.getRow('widgets', {id: 'w1'}),
      );
    } finally {
      await rust.destroy();
      ts.destroy();
    }
  });

  test('advance conversion failure rejects without partial query state', async () => {
    const {rust, ts} = setup();
    try {
      expect(await observeHydrate(rust, 'w1')).toEqual(
        await observeHydrate(ts, 'w1'),
      );
      const beforeSignature = rust.rowSetSignature('q');
      expect(beforeSignature).toBe(ts.rowSetSignature('q'));

      const version = '8500000002';
      db.exec(/*sql*/ `
        UPDATE widgets SET payload = '{bad', _0_version = '${version}'
        WHERE id = 'w1';
        INSERT INTO "_zero.changeLog2" VALUES
          ('${version}', 0, 'widgets', json('{"id":"w1"}'), 's', '{}');
        UPDATE "_zero.replicationState" SET stateVersion = '${version}';
      `);

      expect((await observeAdvance(rust)).status).toBe('error');
      expect((await observeAdvance(ts)).status).toBe('error');
      expect(rust.queries().has('q')).toBe(ts.queries().has('q'));
      expect(rust.rowSetSignature('q')).toBe(beforeSignature);
      expect(ts.rowSetSignature('q')).toBe(beforeSignature);
      expect(rust.currentVersion()).toBe(ts.currentVersion());
    } finally {
      await rust.destroy();
      ts.destroy();
    }
  });
});
