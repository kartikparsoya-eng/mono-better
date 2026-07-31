import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../../zero-schema/src/builder/table-builder.ts';
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
import {RustIVMDriver} from './rust-ivm-driver.ts';

// Regression guard for the "Cannot sync from older replica" reset-LOOP bug.
//
// `replicaVersion` is the IMMUTABLE base stamped at replica creation (stored in
// `_zero.replicationConfig`); `currentVersion()` is the ADVANCING head (the
// `_zero.replicationState.stateVersion` the snapshotter is pinned at). TS's
// PipelineDriver keeps these strictly separate (pipeline-driver.ts:270 sets
// `#replicaVersion` once and never reassigns it; `currentVersion()` reads the
// snapshotter).
//
// The Rust driver originally CONFLATED them into one field, overwriting
// `#replicaVersion` with the advancing head on every advance
// (advanceWithoutDiff/advance/advanceToHead). The ViewSyncer stamps the CVR's
// replicaVersion from `#pipelines.replicaVersion` (view-syncer.ts:1964/2323),
// so post-advance a CVR got stamped at the HEAD. On the next connect a fresh
// pipeline init re-reads the true BASE, and the older-replica guard
// (view-syncer.ts:494, `base < cvr.head`) self-fired -> ClientNotFound -> reset
// -> re-hydrate -> CVR re-stamped at head -> reset ... an infinite loop that
// manifested as "Messages are loading..." forever for affected client groups.
//
// This test locks in the fix: `replicaVersion` MUST stay == the init base
// across any number of advances, while `currentVersion()` tracks the head.

const ADDON_PATH = process.env['RUST_IVM_ADDON_PATH'];

describe.skipIf(!ADDON_PATH)(
  'view-syncer/rust-ivm-driver replicaVersion immutability',
  () => {
    const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
    const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
    const BASE = '8400bivbkg'; // the immutable replicaVersion (base)

    let dbFile: DbFile;
    let db: DB;
    let lc: LogContext;

    const issues = table('issues')
      .columns({id: string(), kind: string()})
      .primaryKey('id');
    const clientSchema = createSchema({tables: [issues]});

    beforeEach(() => {
      lc = new LogContext('error', undefined, new TestLogSink());
      dbFile = new DbFile('rust_ivm_replica_version_test');
      dbFile.connect(lc).pragma('journal_mode = wal2');
    });

    afterEach(() => {
      dbFile.delete();
    });

    function setupDriver(): RustIVMDriver {
      const storage = new Database(lc, ':memory:');
      storage.prepare(CREATE_STORAGE_TABLE).run();

      const driver = new RustIVMDriver(
        lc,
        testLogConfig,
        shardID,
        new DatabaseStorage(storage).createClientGroupStorage('rv-test-cg'),
        'rv-test-cg',
        new InspectorDelegate(undefined),
        () => 200,
        false, // planner off
        undefined,
        dbFile.path,
      );

      db = dbFile.connect(lc);
      // Stamp the immutable base into _zero.replicationConfig + set head == base.
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
        CREATE TABLE issues (
          id        TEXT PRIMARY KEY,
          kind      TEXT,
          _0_version TEXT NOT NULL
        );
      `);
      db.prepare(
        'INSERT INTO issues (id, kind, _0_version) VALUES (?, ?, ?)',
      ).run('i0', 'public', BASE);
      populateFromExistingTables(db, listTables(db, false));

      driver.init(clientSchema);
      return driver;
    }

    /** Advance the replica head (what the replicator does) to `version`. */
    function bumpHead(version: string) {
      db.prepare('UPDATE "_zero.replicationState" SET stateVersion = ?').run(
        version,
      );
    }

    test('replicaVersion stays immutable across advances; currentVersion tracks head', () => {
      const driver = setupDriver();

      // At init both equal the base.
      expect(driver.replicaVersion).toBe(BASE);
      expect(driver.currentVersion()).toBe(BASE);

      // Replicator advances head past the base (simulates WAL catch-up).
      bumpHead('846700000a');
      const v1 = driver.advanceWithoutDiff();
      expect(v1).toBe('846700000a');

      // THE REGRESSION ASSERT: base is immutable, head advanced.
      // Pre-fix, replicaVersion would now read '846700000a' — the value the
      // CVR got stamped with, which then self-fires the older-replica guard.
      expect(driver.replicaVersion).toBe(BASE);
      expect(driver.currentVersion()).toBe('846700000a');

      // A second advance must not budge the base either.
      bumpHead('846700000b');
      const v2 = driver.advanceWithoutDiff();
      expect(v2).toBe('846700000b');
      expect(driver.replicaVersion).toBe(BASE);
      expect(driver.currentVersion()).toBe('846700000b');

      // The CVR guard's invariant (view-syncer.ts:494): a CVR stamped from
      // #pipelines.replicaVersion must NOT be greater than a freshly-read base
      // — i.e. base-vs-base never trips "older replica".
      expect(driver.replicaVersion < driver.currentVersion()).toBe(true);
      expect(driver.replicaVersion < driver.replicaVersion).toBe(false);
    });
  },
);
