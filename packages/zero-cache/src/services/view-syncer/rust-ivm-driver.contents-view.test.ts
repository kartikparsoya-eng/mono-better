import './rust-ivm-addon-setup.ts'; // MUST be first: guarantees the wal2 addon.
import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../../zero-schema/src/builder/table-builder.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import type {Database as DB} from '../../../../zqlite/src/db.ts';
import {listTables} from '../../db/lite-tables.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import {stringify} from '../../../../shared/src/bigint-json.ts';
import {Subscription} from '../../types/subscription.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {ClientHandler} from './client-handler.ts';

import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import {populateFromExistingTables} from '../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {RustIVMDriver} from './rust-ivm-driver.ts';

// `contents` (the wire form, without `_0_version`) and `row` (the parity form
// the TS PipelineDriver produces, with it) are served from ONE parse. They must
// not alias: `contents` is handed out by reference and held by the CVR until
// the poke is flushed, so building `row` must never write `_0_version` back
// onto the shared object.
const ADDON_PATH = process.env['RUST_IVM_ADDON_PATH'];

describe.skipIf(!ADDON_PATH)('rust-ivm-driver rowKey canonical parity', () => {
  const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
  const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
  const BASE = '8400bivbkg';
  let dbFile: DbFile;
  let db: DB;
  let lc: LogContext;

  const items = table('items')
    .columns({id: string(), label: string().optional()})
    .primaryKey('id');
  const clientSchema = createSchema({tables: [items]});

  beforeEach(() => {
    lc = new LogContext('error', undefined, new TestLogSink());
    dbFile = new DbFile('rust_ivm_rowkey_canonical');
    dbFile.connect(lc).pragma('journal_mode = wal2');
  });
  afterEach(() => dbFile.delete());

  function newStorage(name: string) {
    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    return new DatabaseStorage(storage).createClientGroupStorage(name);
  }

  // A spread of row shapes, including values that need reviving.
  const IDS = [
    'plain',
    'has "quote"',
    'has\\backslash',
    'has\nnewline\tand\ttabs',
    'héllo-世界',
    '😀 astral',
    '',
    'trailing-space ',
    'a/b?c=d&e',
  ];

  function setup(): RustIVMDriver {
    const rust = new RustIVMDriver(
      lc,
      testLogConfig,
      shardID,
      newStorage('rowkey-canonical'),
      'rowkey-canonical',
      new InspectorDelegate(undefined),
      () => 200,
      false,
      undefined,
      dbFile.path,
    );
    db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], BASE);
    db.exec(/*sql*/ `
      CREATE TABLE "${mutationsTableName}" (
        "clientGroupID" TEXT, "clientID" TEXT, "mutationID" INTEGER,
        "result" TEXT, _0_version TEXT NOT NULL,
        PRIMARY KEY ("clientGroupID","clientID","mutationID")
      );
      CREATE TABLE items (
        id "text|NOT_NULL" PRIMARY KEY,
        label "text",
        _0_version "text|NOT_NULL"
      );
    `);
    const ins = db.prepare(
      `INSERT INTO items (id, label, _0_version) VALUES (?, ?, '${BASE}')`,
    );
    for (const id of IDS) {
      ins.run(id, 'x');
    }
    populateFromExistingTables(db, listTables(db, false));
    rust.init(clientSchema);
    return rust;
  }

  const NO_TIMER = {elapsedLap: () => 0, totalElapsed: () => 0} as never;

  /**
   * End-to-end over the seam that actually broke: engine row -> driver
   * `contents` -> row patch -> poke -> the structured-clone hop the downstream
   * makes -> serialized wire bytes.
   *
   * The unit tests either stop at the driver or start from hand-made contents.
   * Neither covers the join between them, which is where a `{"json":"..."}`
   * row and a leaked `_0_version` would both show up.
   */
  test('engine rows reach the wire correctly through a poke', async () => {
    const rust = setup();
    const ast = {table: 'items', orderBy: [['id', 'asc']]} as never;

    const received: Downstream[] = [];
    const subscription = Subscription.create<Downstream>({});
    void (async () => {
      for await (const msg of subscription) {
        received.push(msg);
      }
    })();

    const handler = new ClientHandler(
      lc,
      'g1',
      'c1',
      'ws1',
      shardID,
      '100',
      subscription,
    );
    const poker = handler.startPoke({stateVersion: '120'});

    let sent = 0;
    for await (const change of rust.addQuery('h', 'q', ast, NO_TIMER)) {
      if (change === 'yield' || change.row === undefined) {
        continue;
      }
      // Exactly what view-syncer's updateVersion does for the Rust path.
      expect(typeof change.version).toBe('string');
      const contents = change.contents;
      expect(contents).toBeDefined();
      await poker.addPatch({
        toVersion: {stateVersion: '120'},
        patch: {
          type: 'row',
          op: 'put',
          id: {schema: '', table: change.table, rowKey: change.rowKey},
          contents: contents as never,
        },
      });
      sent++;
    }
    await poker.end({stateVersion: '120'});
    subscription.cancel();
    rust.destroy?.();
    expect(sent).toBe(IDS.length);

    // Serialize the way the outbound stream does, AFTER the process hop.
    const wire = stringify(structuredClone(received));
    expect(wire).toBe(stringify(received));

    // No wrapper leakage, and the replica-internal column never ships.
    expect(wire).not.toContain('"json":');
    expect(wire).not.toContain('_0_version');
    // The real columns are present.
    expect(wire).toContain('"label":"x"');
  });

  test('reading row never leaks _0_version into contents', async () => {
    const rust = setup();
    const ast = {table: 'items', orderBy: [['id', 'asc']]} as never;
    let checked = 0;
    for await (const change of rust.addQuery('h', 'q', ast, NO_TIMER)) {
      if (change === 'yield' || change.row === undefined) {
        continue;
      }
      // Grab contents FIRST, as the view-syncer does, then force `row`.
      const contents = change.contents;
      expect(contents).toBeDefined();
      expect('_0_version' in (contents as object)).toBe(false);

      const row = change.row;
      expect(row).toBeDefined();
      expect('_0_version' in (row as object)).toBe(true);

      // The previously-handed-out contents must be untouched.
      expect('_0_version' in (contents as object)).toBe(false);
      expect(change.contents).toBe(contents);
      checked++;
    }
    rust.destroy?.();
    expect(checked).toBe(IDS.length);
  });
});
