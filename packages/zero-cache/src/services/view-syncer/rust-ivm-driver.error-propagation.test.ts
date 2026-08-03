import './rust-ivm-addon-setup.ts'; // MUST be first: guarantees the wal2 addon.
import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  json,
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
import {PipelineDriver} from './pipeline-driver.ts';
import {drain} from './rust-ivm-differential-harness.ts';
import {RustIVMDriver} from './rust-ivm-driver.ts';
import {Snapshotter} from './snapshotter.ts';

// End-to-end regression LOCKING review finding #2 (and its Phase-1 fix
// cb19237ad): a failure encountered DURING streaming hydration must reject the
// consumer / tear the pipeline down — it must NEVER masquerade as an empty or
// partial result.
//
// The propagation mechanism is shared by every mid-stream failure (SQLite
// prepare/bind in stream_query, check_exists, and value-decode in
// sqlite_value_to_ivm): the panic fires lazily while `add_queries_streaming`
// pulls rows INSIDE `EngineHandle::call`'s closure, is caught by that call's
// std::panic::catch_unwind (napi/src/lib.rs:234), returned as a NapiError
// (lib.rs:240) so the AsyncTask REJECTS, which makes the driver's `for await`
// over addQuery throw. (A `DROP TABLE` prepare-failure would race the pinned
// wal2 read snapshot and be non-deterministic, so we induce a deterministic
// decode failure instead — invalid JSON text in a `json` column, a real
// restored-replica corruption mode — which travels the identical path.)
//
// This is a DIFFERENTIAL lock: the TS PipelineDriver is the executable spec and
// ALSO throws (zqlite/table-source.ts:651 UnsupportedValueError). "Altered
// errors" is a forbidden divergence — both engines must reject, neither may
// swallow the row into an empty stream.
const ADDON_PATH = process.env['RUST_IVM_ADDON_PATH'];

describe.skipIf(!ADDON_PATH)(
  'view-syncer/rust-ivm-driver error propagation',
  () => {
    const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
    const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
    const BASE = '8400bivbkg';
    let dbFile: DbFile;
    let db: DB;
    let lc: LogContext;

    const widgets = table('widgets')
      .columns({id: string(), payload: json()})
      .primaryKey('id');
    // A clean sibling table used to prove the actor survives the panic.
    const gadgets = table('gadgets')
      .columns({id: string(), name: string()})
      .primaryKey('id');
    const clientSchema = createSchema({tables: [widgets, gadgets]});

    beforeEach(() => {
      lc = new LogContext('error', undefined, new TestLogSink());
      dbFile = new DbFile('rust_ivm_errprop_test');
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
        newStorage('errprop-rust'),
        'errprop-rust',
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
        newStorage('errprop-ts'),
        'errprop-ts',
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
        payload "json",
        _0_version "text|NOT_NULL"
      );
      CREATE TABLE gadgets (
        id      "text|NOT_NULL" PRIMARY KEY,
        name    "text",
        _0_version "text|NOT_NULL"
      );
      -- Deterministic corruption: payload declared json but holds a bareword
      -- that JSON.parse (TS) / assert_valid_json (Rust) both reject.
      INSERT INTO widgets VALUES ('w1', 'not valid json ][', '${BASE}');
      INSERT INTO gadgets VALUES ('g1', 'clean', '${BASE}');
    `);
      populateFromExistingTables(db, listTables(db, false));
      rust.init(clientSchema);
      ts.init(clientSchema);
      return {rust, ts};
    }

    const NO_TIMER = {elapsedLap: () => 0, totalElapsed: () => 0} as any;

    test('mid-stream decode failure rejects both drivers (no empty result)', async () => {
      const {rust, ts} = setup();
      const ast = {table: 'widgets', orderBy: [['id', 'asc']]} as any;

      // TS reference (executable spec): hydration MUST throw, not yield empty.
      await expect(
        drain(ts.addQuery('h', 'q', ast, NO_TIMER)),
      ).rejects.toThrow();

      // Rust MUST match: reject, never swallow the bad row into an empty stream.
      await expect(
        drain(rust.addQuery('h', 'q', ast, NO_TIMER)),
      ).rejects.toThrow();
      expect(rust.queries().has('q')).toBe(false);
      expect(rust.rowSetSignature('q')).toBeUndefined();
    });

    test('actor survives the panic — a valid query still hydrates afterward', async () => {
      // Prove the failure is contained (catch_unwind), not fatal to the engine:
      // after a rejected hydration, a subsequent VALID query on the SAME driver
      // still works. (Guards against a panic that poisons/kills the actor thread.)
      const {rust} = setup();
      const badAst = {table: 'widgets', orderBy: [['id', 'asc']]} as any;
      await expect(
        drain(rust.addQuery('bad', 'q', badAst, NO_TIMER)),
      ).rejects.toThrow();
      expect(rust.queries().has('q')).toBe(false);
      expect(rust.rowSetSignature('q')).toBeUndefined();

      // The engine reads a snapshot pinned at BASE, so the corrupt widgets row
      // can't be "repaired" in place. Instead hydrate a CLEAN sibling table from
      // the same pinned snapshot: it must succeed, proving the panic was
      // contained (catch_unwind) and did not kill/poison the actor thread.
      const cleanAst = {table: 'gadgets', orderBy: [['id', 'asc']]} as any;
      const good = await drain(rust.addQuery('good', 'q2', cleanAst, NO_TIMER));
      expect(good.length).toBeGreaterThan(0);
      expect(good.some(c => (c.row as any)?.name === 'clean')).toBe(true);
    });
  },
);
