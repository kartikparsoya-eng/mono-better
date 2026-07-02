// E2E for SidecarManager's napi transport mode — the production glue that
// syncer.ts uses when goSidecar.transport=napi. Distinct from
// napi-transport.test.ts (which drives GoIVMClient + addon directly): this
// exercises the manager's #startNapi path — addon load, goivm_start,
// handshake (ping + version + protocol-rev gate), status machine, and the
// rowMode plumbing production actually runs through manager.getClient().
//
// MUST be a separate file from napi-transport.test.ts: one Go host per
// process (goivm_start refuses a second start), and vitest isolates per
// test file, giving this suite its own process.
//
// GATED on the same out-of-band artifacts (addon .node + libgoivm); skips
// cleanly when missing.

import {existsSync} from 'node:fs';
import {afterAll, describe, expect, test} from 'vitest';
import type {RowChange} from './go-ivm-client.ts';
import {isGoNapiAddonAvailable} from './napi/index.ts';
import {SidecarManager} from './sidecar-manager.ts';

const LIB_PATH =
  process.env.GOIVM_TEST_LIB ??
  (process.platform === 'darwin' ? '/tmp/libgoivm.dylib' : '/tmp/libgoivm.so');

const available = isGoNapiAddonAvailable() && existsSync(LIB_PATH);

describe.skipIf(!available)('SidecarManager (napi transport)', () => {
  const manager = new SidecarManager({
    binaryPath: 'unused-in-napi-mode',
    transport: 'napi',
    napiLibPath: LIB_PATH,
  });

  afterAll(async () => {
    // stop() in napi mode closes the client but deliberately does NOT tear
    // down the Go host (TSFN drain race; see sidecar-manager.ts stop()).
    // Process exit reclaims it.
    await manager.stop();
  });

  test('start() loads the in-process engine and completes the handshake', async () => {
    await manager.start();
    expect(manager.status).toBe('running');
    expect(manager.epoch).toBe(1);
    // The version handshake ran over the frame plane; default env (no
    // GO_IVM_SOURCE_MODE) means memory mode.
    expect(manager.sidecarSourceMode).toBe('memory');
    expect(await manager.getClient().ping()).toBe('pong');
  });

  test('second start() is idempotent while running', async () => {
    await manager.start();
    expect(manager.status).toBe('running');
    expect(manager.epoch).toBe(1); // no re-establishment happened
  });

  test('rowMode hydrate flows end-to-end through the manager-owned client', async () => {
    const client = manager.getClient();
    const {initEpoch} = await client.init('cg-mgr-napi', {
      tables: {
        items: {
          columns: {id: {type: 'string'}, label: {type: 'string'}},
          primaryKey: ['id'],
          rows: [],
        },
      },
    });
    await client.loadRows(
      'cg-mgr-napi',
      'items',
      [
        {id: 'i1', label: 'one'},
        {id: 'i2', label: 'two'},
      ],
      initEpoch,
    );
    const results: {queryID: string; changes: RowChange[]}[] = [];
    await client.addQueriesStream(
      'cg-mgr-napi',
      [{queryID: 'q-mgr', ast: {table: 'items', orderBy: [['id', 'asc']]}}],
      initEpoch,
      r => results.push(r),
      {rowMode: true},
    );
    expect(results).toHaveLength(1);
    expect(results[0].changes.map(ch => (ch.row as {label: string}).label)).toEqual([
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
