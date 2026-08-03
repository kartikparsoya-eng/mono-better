import './rust-ivm-addon-setup.ts'; // MUST be first: guarantees the wal2 addon.
import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  json,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import type {Database as DB} from '../../../../zqlite/src/db.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {listTables} from '../../db/lite-tables.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import {populateFromExistingTables} from '../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {PipelineDriver} from './pipeline-driver.ts';
import {Snapshotter} from './snapshotter.ts';
import {RustIVMDriver} from './rust-ivm-driver.ts';

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
    .columns({id: string(), active: boolean(), payload: json()})
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
      lc, testLogConfig, shardID,
      newStorage('getrow-rust'),
      'getrow-rust', new InspectorDelegate(undefined), () => 200,
      false, undefined, dbFile.path,
    );
    const ts = new PipelineDriver(
      lc, testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
      shardID, newStorage('getrow-ts'),
      'getrow-ts', new InspectorDelegate(undefined), () => 200, false,
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
        payload "json",
        _0_version "text|NOT_NULL"
      );
      INSERT INTO widgets VALUES ('w1', 1, '{"x":1,"y":[2,3]}', '${BASE}');
    `);
    populateFromExistingTables(db, listTables(db, false));
    rust.init(clientSchema);
    ts.init(clientSchema);
    return {rust, ts};
  }

  const NO_TIMER = {elapsedLap: () => 0, totalElapsed: () => 0} as any;

  test('getRow matches PipelineDriver (projection + fromSQLiteType)', async () => {
    const {rust, ts} = setup();
    // getRow requires the table's source to exist — add+drain a query first.
    const ast = {table: 'widgets', orderBy: [['id', 'asc']]} as any;
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
});
