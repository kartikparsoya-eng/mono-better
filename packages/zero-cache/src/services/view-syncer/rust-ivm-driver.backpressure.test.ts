// #3 backpressure — real NAPI integration test (reviewer requirement: not only
// the Condvar unit test). Sets a TINY credit window so a slow consumer forces
// the native producer to PARK on credit, then verifies correctness is preserved
// under backpressure:
//   - a slow consumer yields the EXACT same ordered rows as a fast consumer
//     (the credit gate must not drop, duplicate, or reorder rows);
//   - both match the TS PipelineDriver (executable spec) as a multiset;
//   - a SECOND operation succeeds after an abandoned (early-break) hydration —
//     proving the parked producer is released and the actor is reusable.
//
// The tiny window is set/restored via beforeAll/afterAll (the native
// `RUST_IVM_STREAM_CREDIT` is read fresh at each streaming compute(), so setting
// it before the tests run — not at import — is sufficient, and restoring it
// avoids leaking a 4-row window into sibling test files sharing this worker).
import './rust-ivm-addon-setup.ts'; // MUST be first: guarantees wal2 addon.
import {createRequire} from 'node:module';
import {LogContext} from '@rocicorp/logger';
import {
  afterAll,
  beforeAll,
  beforeEach,
  afterEach,
  describe,
  expect,
  test,
} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
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
import {PipelineDriver} from './pipeline-driver.ts';
import {
  drain,
  multiset,
  stable,
  type Change,
} from './rust-ivm-differential-harness.ts';
import {RustIVMDriver} from './rust-ivm-driver.ts';
import {ResetPipelinesSignal, Snapshotter} from './snapshotter.ts';

const ADDON_PATH = process.env['RUST_IVM_ADDON_PATH'];
const ROWS = (() => {
  const configured = Number(process.env['RUST_IVM_SOAK_ROWS'] ?? '40');
  return Number.isSafeInteger(configured) && configured >= 8 ? configured : 40;
})(); // >> the credit window (4) so the producer parks repeatedly
const nodeRequire = createRequire(import.meta.url);

describe.skipIf(!ADDON_PATH)('view-syncer/rust-ivm-driver backpressure', () => {
  const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
  const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
  const BASE = '8400bivbkg';
  let dbFile: DbFile;
  let db: DB;
  let lc: LogContext;

  const items = table('items')
    .columns({id: string(), n: number()})
    .primaryKey('id');
  const clientSchema = createSchema({tables: [items]});

  let prevCredit: string | undefined;
  let prevQueue: string | undefined;
  beforeAll(() => {
    prevCredit = process.env['RUST_IVM_STREAM_CREDIT'];
    prevQueue = process.env['RUST_IVM_TSFN_QUEUE'];
    process.env['RUST_IVM_STREAM_CREDIT'] ??= '4';
    process.env['RUST_IVM_TSFN_QUEUE'] ??= '4';
  });
  afterAll(() => {
    if (prevCredit === undefined) {
      delete process.env['RUST_IVM_STREAM_CREDIT'];
    } else {
      process.env['RUST_IVM_STREAM_CREDIT'] = prevCredit;
    }
    if (prevQueue === undefined) {
      delete process.env['RUST_IVM_TSFN_QUEUE'];
    } else {
      process.env['RUST_IVM_TSFN_QUEUE'] = prevQueue;
    }
  });

  beforeEach(() => {
    lc = new LogContext('error', undefined, new TestLogSink());
    dbFile = new DbFile('rust_ivm_backpressure_test');
    dbFile.connect(lc).pragma('journal_mode = wal2');
  });
  afterEach(() => dbFile.delete());

  function newStorage(name: string) {
    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    return new DatabaseStorage(storage).createClientGroupStorage(name);
  }

  function newRust(name: string) {
    return new RustIVMDriver(
      lc,
      testLogConfig,
      shardID,
      newStorage(name),
      name,
      new InspectorDelegate(undefined),
      () => 200,
      false,
      undefined,
      dbFile.path,
    );
  }

  function setup(): {rust: RustIVMDriver; ts: PipelineDriver} {
    const rust = newRust('bp-rust');
    const ts = new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
      shardID,
      newStorage('bp-ts'),
      'bp-ts',
      new InspectorDelegate(undefined),
      () => 200,
      false,
    );
    db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], BASE);
    let inserts = '';
    for (let i = 0; i < ROWS; i++) {
      const id = `i${String(i).padStart(3, '0')}`;
      inserts += `INSERT INTO items VALUES ('${id}', ${i}, '${BASE}');\n`;
    }
    db.exec(/*sql*/ `
      CREATE TABLE "${mutationsTableName}" (
        "clientGroupID" TEXT, "clientID" TEXT, "mutationID" INTEGER,
        "result" TEXT, _0_version TEXT NOT NULL,
        PRIMARY KEY ("clientGroupID","clientID","mutationID")
      );
      CREATE TABLE items (
        id "text|NOT_NULL" PRIMARY KEY,
        n  "int",
        _0_version "text|NOT_NULL"
      );
      ${inserts}
    `);
    populateFromExistingTables(db, listTables(db, false));
    rust.init(clientSchema);
    ts.init(clientSchema);
    return {rust, ts};
  }

  const NO_TIMER = {elapsedLap: () => 0, totalElapsed: () => 0} as any;
  const TIMER_17 = {elapsedLap: () => 17, totalElapsed: () => 17} as any;
  const TIMER_500 = {elapsedLap: () => 500, totalElapsed: () => 500} as any;
  const LARGE_TIMER = {
    elapsedLap: () => 0,
    totalElapsed: () => 60_000,
  } as any;
  const AST = {table: 'items', orderBy: [['id', 'asc']]} as any;

  function editEveryItem(version: string) {
    const changes = Array.from({length: ROWS}, (_, i) => {
      const id = `i${String(i).padStart(3, '0')}`;
      return `INSERT INTO "_zero.changeLog2" VALUES
        ('${version}', ${i}, 'items', json('{"id":"${id}"}'), 's', '{}');`;
    }).join('\n');
    db.exec(/*sql*/ `
      UPDATE items SET n = n + 1000, _0_version = '${version}';
      ${changes}
      UPDATE "_zero.replicationState" SET stateVersion = '${version}';
    `);
  }

  /** Drain, awaiting a macrotask between rows to make the consumer SLOW so the
   * native producer parks on the credit window. */
  async function drainSlow(it: AsyncIterable<unknown>): Promise<Change[]> {
    const out: Change[] = [];
    for await (const c of it) {
      if (c === 'yield') {
        continue;
      }
      out.push(c as Change);
      await new Promise(r => setTimeout(r, 1)); // slower than the producer
    }
    return out;
  }

  async function drainVerySlow(
    it: AsyncIterable<unknown> | Iterable<unknown>,
  ): Promise<Change[]> {
    const out: Change[] = [];
    for await (const c of it) {
      if (c === 'yield') {
        continue;
      }
      out.push(c as Change);
      await new Promise(resolve => setTimeout(resolve, 3));
    }
    return out;
  }

  test(
    'slow consumer preserves rows/order and matches TS',
    async () => {
      const {rust, ts} = setup();

      const slow = await drainSlow(rust.addQuery('h', 'q', AST, NO_TIMER));
      // A fresh driver, fast consumer — order must be byte-identical to the slow
      // run (backpressure must not drop/dup/reorder).
      const rustFast = newRust('bp-rust-fast');
      rustFast.init(clientSchema);
      const fast = await drain(rustFast.addQuery('h', 'q', AST, NO_TIMER));
      const tsRows = await drain(ts.addQuery('h', 'q', AST, NO_TIMER));

      expect(slow.length).toBe(ROWS);
      // Exact ordered equality slow-vs-fast (same engine, credit must be transparent).
      expect(slow.map(c => stable(c.row))).toEqual(
        fast.map(c => stable(c.row)),
      );
      // Multiset parity vs the TS executable spec.
      expect(multiset(slow)).toEqual(multiset(tsRows));

      await rust.destroy();
      await rustFast.destroy();
    },
    Math.max(10_000, ROWS * 10),
  );

  test('native producer cannot run past the credit window', async () => {
    const {rust, ts} = setup();
    const {RustIvmEngine} = nodeRequire(ADDON_PATH!) as {
      RustIvmEngine: new () => {
        init: (specs: unknown[], dbPath: string, appID: string) => void;
        addQueriesStreamingRows: (
          specs: unknown[],
          callback: (error: unknown, rows: {changeType: number}[]) => void,
          streamID: number,
        ) => Promise<void>;
        grantStreamCredit: (streamID: number, permits: number) => void;
        cancelStream: (streamID: number) => void;
        cancel: () => void;
        destroy: () => Promise<void> | void;
      };
    };
    const engine = new RustIvmEngine();
    engine.init(
      [
        {
          table: 'items',
          columns: {
            id: {type: 'string', optional: false},
            n: {type: 'number', optional: true},
            _0_version: {type: 'string', optional: false},
          },
          primaryKey: ['id'],
          minRowVersion: BASE,
        },
      ],
      dbFile.path,
      shardID.appID,
    );

    const streamID = 9_001;
    let delivered = 0;
    const done = engine.addQueriesStreamingRows(
      [{queryId: 'native-bound', astJson: JSON.stringify(AST)}],
      (_error, rows) => {
        for (const row of rows) {
          if (row.changeType >= 0) {
            delivered++;
          }
        }
      },
      streamID,
    );

    const waitFor = async (expected: number) => {
      const deadline = Date.now() + 2_000;
      while (delivered < expected && Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, 5));
      }
      expect(delivered).toBe(expected);
    };

    // Chunked delivery: the effective chunk is min(RUST_IVM_DELIVERY_CHUNK,
    // credit) and the credit window is min(credit, queue * chunk) rows. With
    // the tiny test window (credit=4 ≤ default chunk) both collapse to
    // `credit` rows = exactly one chunk in flight; with
    // RUST_IVM_DELIVERY_CHUNK=1 this reproduces the old per-row shape
    // (window = min(credit, queue), grant-1-releases-1).
    const credit = Number(process.env['RUST_IVM_STREAM_CREDIT']);
    const queue = Number(process.env['RUST_IVM_TSFN_QUEUE']);
    const chunk = Math.min(
      Number(process.env['RUST_IVM_DELIVERY_CHUNK'] ?? '64'),
      credit,
    );
    const window = Math.min(credit, queue * chunk);
    await waitFor(window);
    await new Promise(resolve => setTimeout(resolve, 25));
    expect(delivered).toBe(window);

    // A PARTIAL grant (< one chunk of credit) must NOT release the next chunk
    // — credit is acquired per chunk, so the producer stays parked. In the
    // chunk=1 per-row shape, one grant releases exactly one row.
    engine.grantStreamCredit(streamID, 1);
    await new Promise(resolve => setTimeout(resolve, 25));
    expect(delivered).toBe(chunk > 1 ? window : window + 1);

    // Topping up to one full chunk's worth of credit releases exactly one more
    // chunk (grant of 0 is a no-op in the chunk=1 shape).
    engine.grantStreamCredit(streamID, chunk - 1);
    await waitFor(window + chunk);
    await new Promise(resolve => setTimeout(resolve, 25));
    expect(delivered).toBe(window + chunk);

    engine.cancel();
    engine.cancelStream(streamID);
    await done;
    await engine.destroy();
    await rust.destroy();
    ts.destroy();
  });

  test('credit is clamped to a smaller TSFN queue', async () => {
    const {rust, ts} = setup();
    const {RustIvmEngine} = nodeRequire(ADDON_PATH!) as {
      RustIvmEngine: new () => {
        init: (specs: unknown[], dbPath: string, appID: string) => void;
        addQueriesStreamingRows: (
          specs: unknown[],
          callback: (error: unknown, rows: {changeType: number}[]) => void,
          streamID: number,
        ) => Promise<void>;
        cancel: () => void;
        destroy: () => Promise<void> | void;
      };
    };

    const configuredQueue = process.env['RUST_IVM_TSFN_QUEUE'];
    const configuredCredit = process.env['RUST_IVM_STREAM_CREDIT'];
    process.env['RUST_IVM_TSFN_QUEUE'] = '1';
    process.env['RUST_IVM_STREAM_CREDIT'] = '4';
    // Chunked delivery: chunk = min(DELIVERY_CHUNK, credit), window =
    // min(credit, queue * chunk) rows = at most ONE chunk on the depth-1
    // queue. The producer must deliver exactly one chunk and then park
    // interruptibly on credit — never block uninterruptibly in tsfn.call.
    // (With RUST_IVM_DELIVERY_CHUNK=1 this is the old min(credit, queue) = 1.)
    const clampChunk = Math.min(
      Number(process.env['RUST_IVM_DELIVERY_CHUNK'] ?? '64'),
      4,
    );
    const expectedWindow = Math.min(4, 1 * clampChunk);
    const engine = new RustIvmEngine();
    engine.init(
      [
        {
          table: 'items',
          columns: {
            id: {type: 'string', optional: false},
            n: {type: 'number', optional: true},
            _0_version: {type: 'string', optional: false},
          },
          primaryKey: ['id'],
          minRowVersion: BASE,
        },
      ],
      dbFile.path,
      shardID.appID,
    );

    let delivered = 0;
    const done = engine.addQueriesStreamingRows(
      [{queryId: 'queue-clamp', astJson: JSON.stringify(AST)}],
      (_error, rows) => {
        for (const row of rows) {
          if (row.changeType >= 0) {
            delivered++;
          }
        }
      },
      9_002,
    );
    process.env['RUST_IVM_TSFN_QUEUE'] = configuredQueue;
    process.env['RUST_IVM_STREAM_CREDIT'] = configuredCredit;
    await new Promise(resolve => setTimeout(resolve, 50));
    expect(delivered).toBe(expectedWindow);

    engine.cancel();
    await done;
    await engine.destroy();
    await rust.destroy();
    ts.destroy();
  });

  test('watchdog cancellation rejects an undrained advance', async () => {
    const {rust, ts} = setup();
    await drain(rust.addQuery('h', 'q', AST, LARGE_TIMER));
    editEveryItem('8500000100');

    const prevWarn = process.env['RUST_IVM_WATCHDOG_WARN_MS'];
    const prevAbort = process.env['RUST_IVM_WATCHDOG_ABORT_MS'];
    process.env['RUST_IVM_WATCHDOG_WARN_MS'] = '10';
    process.env['RUST_IVM_WATCHDOG_ABORT_MS'] = '10';
    try {
      const result = await rust.advance(LARGE_TIMER);
      if (result instanceof ResetPipelinesSignal) {
        throw result;
      }
      await new Promise(resolve => setTimeout(resolve, 50));
      await expect(drain(result.changes)).rejects.toThrow(
        /advance failed|cancelled|interrupt/i,
      );
    } finally {
      if (prevWarn === undefined) {
        delete process.env['RUST_IVM_WATCHDOG_WARN_MS'];
      } else {
        process.env['RUST_IVM_WATCHDOG_WARN_MS'] = prevWarn;
      }
      if (prevAbort === undefined) {
        delete process.env['RUST_IVM_WATCHDOG_ABORT_MS'];
      } else {
        process.env['RUST_IVM_WATCHDOG_ABORT_MS'] = prevAbort;
      }
      await rust.destroy();
      ts.destroy();
    }
  });

  test('destroy cancels an abandoned advance before queueing teardown', async () => {
    const {rust, ts} = setup();
    await drain(rust.addQuery('h', 'q', AST, LARGE_TIMER));
    editEveryItem('8500000101');
    await rust.advance(LARGE_TIMER);
    await new Promise(resolve => setTimeout(resolve, 25));

    await expect(
      Promise.race([
        rust.destroy().then(() => 'destroyed'),
        new Promise<string>(resolve =>
          setTimeout(() => resolve('timed-out'), 1_000),
        ),
      ]),
    ).resolves.toBe('destroyed');
    ts.destroy();
  });

  test('slow delivery time does not consume the advancement budget', async () => {
    const {rust, ts} = setup();
    await drain(rust.addQuery('h', 'q', AST, TIMER_500));
    editEveryItem('8500000102');

    const result = await rust.advance(TIMER_500);
    if (result instanceof ResetPipelinesSignal) {
      throw result;
    }
    expect(await drainVerySlow(result.changes)).toHaveLength(ROWS);

    await rust.destroy();
    ts.destroy();
  });

  test('second hydration succeeds after an abandoned (early-break) one', async () => {
    const {rust} = setup();

    // Abandon after a few rows: this triggers the generator `finally` →
    // cancelStream + cancel, which must release the parked producer.
    let seen = 0;
    for await (const c of rust.addQuery('h', 'q', AST, NO_TIMER)) {
      if (c === 'yield') {
        continue;
      }
      if (++seen >= 3) {
        break; // early exit while the producer is still parked on credit
      }
    }
    expect(seen).toBe(3);
    expect(rust.queries().has('q')).toBe(false);
    expect(rust.rowSetSignature('q')).toBeUndefined();

    // The actor must be reusable: a full hydration now completes correctly.
    const full = await drain(rust.addQuery('h2', 'q2', AST, TIMER_17));
    expect(full.length).toBe(ROWS);
    expect(rust.queries().has('q2')).toBe(true);
    expect(rust.totalHydrationTimeMs()).toBe(17);
    rust.removeQuery('q2');
    expect(rust.totalHydrationTimeMs()).toBe(0);

    await rust.destroy();
  });
});
