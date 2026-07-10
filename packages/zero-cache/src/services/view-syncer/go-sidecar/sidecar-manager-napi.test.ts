// E2E for SidecarManager's napi transport mode — the production glue that
// syncer.ts uses when goSidecar.enabled. Distinct from napi-transport.test.ts
// (which drives GoIVMClient + addon directly): this exercises the manager's
// #startNapi path — addon load, goivm_start, handshake (ping + version +
// protocol-rev gate), status machine, and the rowMode plumbing production
// actually runs through manager.getClient().
//
// MUST be a separate file from napi-transport.test.ts: one Go host per
// process (goivm_start refuses a second start), and vitest isolates per
// test file, giving this suite its own process.
//
// Table-mode fixture: the replica is pre-seeded before the manager starts
// (spawnEnv carries GO_IVM_REPLICA_DB_PATH into the Go host's env before
// dlopen). GATED on the same out-of-band artifacts (addon .node + libgoivm);
// skips cleanly when missing.

import {existsSync} from 'node:fs';
import {afterAll, describe, expect, test} from 'vitest';
import type {RowChange} from './go-ivm-client.ts';
import {makeTestReplica} from './napi-test-fixtures.ts';
import {isGoNapiAddonAvailable} from './napi/index.ts';
import {SidecarManager} from './sidecar-manager.ts';

const LIB_PATH =
  process.env.GOIVM_TEST_LIB ??
  (process.platform === 'darwin' ? '/tmp/libgoivm.dylib' : '/tmp/libgoivm.so');

// ── replica ──────────────────────────────────────────────────────────
const replica = makeTestReplica();
replica.db.exec(
  `CREATE TABLE "items" ("id" TEXT PRIMARY KEY,"label" TEXT,"_0_version" TEXT)`,
);
replica.db
  .prepare('INSERT INTO "items" VALUES (?,?,?)')
  .run('i1', 'one', '0000000001');
replica.db
  .prepare('INSERT INTO "items" VALUES (?,?,?)')
  .run('i2', 'two', '0000000001');

const available = isGoNapiAddonAvailable() && existsSync(LIB_PATH);

if (!available && process.env.CI === 'true') {
  throw new Error(
    `SidecarManager NAPI E2E tests cannot run: build artifacts missing in CI. ` +
      `Addon: ${isGoNapiAddonAvailable() ? 'present' : 'missing'}, ` +
      `Lib: ${existsSync(LIB_PATH) ? 'present' : 'missing'} (${LIB_PATH}).`,
  );
}

describe.skipIf(!available)('SidecarManager (napi transport)', () => {
  const manager = new SidecarManager({
    napiLibPath: LIB_PATH,
    spawnEnv: {
      GO_IVM_REPLICA_DB_PATH: replica.path,
    },
  });

  afterAll(async () => {
    await manager.stop();
  });

  test('start() loads the in-process engine and completes the handshake', async () => {
    await manager.start();
    expect(manager.status).toBe('running');
    expect(manager.epoch).toBe(1);
    expect(await manager.getClient().ping()).toBe('pong');
  });

  test('second start() is idempotent while running', async () => {
    await manager.start();
    expect(manager.status).toBe('running');
    expect(manager.epoch).toBe(1);
  });

  test('hydrate flows end-to-end through the manager-owned client', async () => {
    const client = manager.getClient();
    const {initEpoch} = await client.init('cg-mgr-napi', {
      tables: {
        items: {
          columns: {
            id: {type: 'string'},
            label: {type: 'string'},
            _0_version: {type: 'string'},
          },
          primaryKey: ['id'],
          uniqueKeys: [['id']],
          rows: [],
        },
      },
    });
    const changes: RowChange[] = [];
    for await (const entry of client.addQueriesStreamPull(
      'cg-mgr-napi',
      [{queryID: 'q-mgr', ast: {table: 'items', orderBy: [['id', 'asc']]}}],
      initEpoch,
    )) {
      changes.push(...entry.changes);
    }
    expect(changes.map(ch => (ch.row as {label: string}).label)).toEqual([
      'one',
      'two',
    ]);
  });

  test('stop() reaches terminal state and getClient refuses', async () => {
    await manager.stop();
    expect(manager.status).toBe('stopped');
    expect(() => manager.getClient()).toThrow(/not running/);
  });
});

if (!available) {
  test('SidecarManager napi E2E skipped (build artifacts missing)', () => {
    expect(available).toBe(false);
  });
}
