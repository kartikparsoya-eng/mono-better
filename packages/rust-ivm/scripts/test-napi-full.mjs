#!/usr/bin/env node
/**
 * Production-facing NAPI smoke test for the Rust IVM driver contract.
 *
 * This intentionally uses the same WAL2 SQLite implementation, table-spec
 * shape, astJson boundary, and credit-gated callbacks as RustIVMDriver.
 */

import {copyFileSync, existsSync, rmSync} from 'node:fs';
import {createRequire} from 'node:module';
import {tmpdir} from 'node:os';
import {dirname, join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const zqliteRequire = createRequire(
  resolve(scriptDir, '..', '..', 'zqlite', 'package.json'),
);
const SQLiteDatabase = zqliteRequire('@rocicorp/zero-sqlite3');
const napiDir = resolve(scriptDir, '..', 'napi');
const candidates = process.env.RUST_IVM_ADDON
  ? [process.env.RUST_IVM_ADDON]
  : process.platform === 'darwin'
    ? [
        resolve(napiDir, 'target/release/librust_ivm_napi.dylib'),
        resolve(napiDir, 'rust-ivm.node'),
      ]
    : [
        resolve(napiDir, 'rust-ivm.node'),
        resolve(napiDir, 'target/release/librust_ivm_napi.so'),
      ];
const addonPath = candidates.find(existsSync);
if (!addonPath) {
  throw new Error(`NAPI addon not found; tried:\n  ${candidates.join('\n  ')}`);
}

let loadPath = addonPath;
let copiedAddon;
if (!loadPath.endsWith('.node')) {
  copiedAddon = join(tmpdir(), `rust-ivm-napi-${process.pid}.node`);
  copyFileSync(loadPath, copiedAddon);
  loadPath = copiedAddon;
}
const {RustIvmEngine} = require(loadPath);

let assertions = 0;
function assert(condition, message) {
  assertions++;
  if (!condition) throw new Error(message);
}

function clean(path) {
  for (const suffix of ['', '-wal', '-shm', '-wal2', '-shm2']) {
    rmSync(path + suffix, {force: true});
  }
}

function createReplica(path) {
  clean(path);
  const db = new SQLiteDatabase(path);
  db.pragma('journal_mode = wal2');
  db.exec(`
    CREATE TABLE "_zero.replicationState" (
      stateVersion TEXT NOT NULL,
      lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock = 1)
    );
    INSERT INTO "_zero.replicationState" (stateVersion) VALUES ('01');
    CREATE TABLE "_zero.replicationConfig" (
      lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock = 1),
      replicaVersion TEXT NOT NULL
    );
    INSERT INTO "_zero.replicationConfig" (replicaVersion) VALUES ('01');
    CREATE TABLE "_zero.changeLog2" (
      stateVersion TEXT NOT NULL,
      pos INTEGER NOT NULL,
      "table" TEXT NOT NULL,
      rowKey TEXT NOT NULL,
      op TEXT NOT NULL,
      PRIMARY KEY (stateVersion, pos),
      UNIQUE ("table", rowKey)
    );
    CREATE TABLE users (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      active INTEGER NOT NULL,
      "_0_version" TEXT NOT NULL
    );
    INSERT INTO users VALUES ('u1', 'Alice', 1, '01');
    INSERT INTO users VALUES ('u2', 'Bob', 0, '01');
  `);
  return db;
}

const tableSpecs = [
  {
    table: 'users',
    columns: {
      id: {type: 'string', optional: false},
      name: {type: 'string', optional: false},
      active: {type: 'boolean', optional: false},
    },
    primaryKey: ['id'],
    uniqueKeys: [['id']],
  },
];

const allUsers = {
  table: 'users',
  orderBy: [{column: 'id', direction: 'asc'}],
};

function dataRows(changes) {
  return changes.filter(change => change.changeType >= 0);
}

function decodedRow(change) {
  return change.row === undefined ? undefined : JSON.parse(change.row);
}

async function expectReject(promise, message) {
  try {
    await promise;
  } catch {
    assertions++;
    return;
  }
  throw new Error(message);
}

async function main() {
  const dbPath = join(tmpdir(), `rust-ivm-napi-full-${process.pid}.db`);
  const keeper = createReplica(dbPath);
  const engine = new RustIvmEngine();

  try {
    assert(engine.ping() === 'pong', 'ping must return pong');
    engine.init(tableSpecs, dbPath, 'napi-full-test');

    const subscription = JSON.parse(
      engine.getSubscriptionState('napi-full-test'),
    );
    assert(subscription.replicaVersion === '01', 'replicaVersion mismatch');
    assert(subscription.watermark === '01', 'watermark mismatch');

    const readRows = JSON.parse(
      engine.readQuery('SELECT id, name FROM users ORDER BY id', null),
    );
    assert(readRows.length === 2, 'readQuery must return both rows');

    const hydrate = await engine.addQueriesStreaming([
      {queryId: 'buffered', astJson: JSON.stringify(allUsers)},
    ]);
    const hydratedRows = dataRows(hydrate);
    assert(hydratedRows.length === 2, 'buffered hydrate lost rows');
    assert(decodedRow(hydratedRows[0]).name === 'Alice', 'row decode mismatch');
    assert(
      engine.queryTransformedAst('buffered') !== null,
      'transformed AST must be retained',
    );
    assert(
      engine.setHydrationTimeMs('buffered', 12.5),
      'hydration timing update must find query',
    );
    assert(engine.totalHydrationTimeMs() === 12.5, 'hydration timing mismatch');

    const streamed = [];
    const hydrateStreamID = 101;
    await engine.addQueriesStreamingRows(
      [{queryId: 'streamed', astJson: JSON.stringify(allUsers)}],
      (error, change) => {
        if (error) throw error;
        engine.grantStreamCredit(hydrateStreamID, 1);
        if (change?.changeType >= 0) streamed.push(change);
      },
      hydrateStreamID,
    );
    assert(streamed.length === 2, 'credit-gated hydrate lost rows');

    keeper.exec(`
      INSERT INTO users VALUES ('u3', 'Charlie', 1, '02');
      INSERT INTO "_zero.changeLog2"
        (stateVersion, pos, "table", rowKey, op)
        VALUES ('02', 0, 'users', '{"id":"u3"}', 's');
      UPDATE "_zero.replicationState" SET stateVersion = '02';
    `);

    const advanced = [];
    const advanceStreamID = 102;
    await engine.advanceToHeadStreamingRows((error, change) => {
      if (error) throw error;
      engine.grantStreamCredit(advanceStreamID, 1);
      if (change?.changeType >= 0) advanced.push(change);
    }, advanceStreamID);
    assert(advanced.length === 2, 'advance must fan out to both live queries');
    assert(
      advanced.every(change => decodedRow(change).id === 'u3'),
      'advance emitted the wrong row',
    );

    engine.removeQuery('buffered');
    assert(
      engine.queryTransformedAst('buffered') === null,
      'removeQuery must remove pipeline metadata',
    );

    const uninitialized = new RustIvmEngine();
    await expectReject(
      uninitialized.addQueriesStreaming([
        {queryId: 'bad', astJson: JSON.stringify(allUsers)},
      ]),
      'uninitialized hydrate must reject',
    );
    await uninitialized.destroy();

    await engine.destroy();
    await expectReject(
      engine.addQueriesStreaming([
        {queryId: 'after-destroy', astJson: JSON.stringify(allUsers)},
      ]),
      'use after destroy must reject',
    );

    process.stdout.write(
      `Rust IVM NAPI full smoke: PASS (${assertions} assertions)\n`,
    );
  } finally {
    keeper.close();
    clean(dbPath);
    if (copiedAddon) rmSync(copiedAddon, {force: true});
  }
}

await main();
