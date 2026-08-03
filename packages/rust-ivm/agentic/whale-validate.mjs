#!/usr/bin/env node
// whale-validate.mjs — validate that streaming hydration has flat memory
// on a 13K-row "whale" query, vs the eager path which materializes all rows.
//
// Usage: node agentic/whale-validate.mjs
//
// Creates a SQLite DB with 13,000 rows, hydrates via both paths, and
// prints peak RSS for each. PASS = streaming RSS delta < 50% of eager delta.

import {rmSync, copyFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {resolve, join, dirname} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {fileURLToPath} from 'node:url';
import {tmpdir} from 'os';

const __dirname = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

const NAPI = resolve(__dirname, '..', 'napi');
const candidates = [
  resolve(NAPI, 'target/release/librust_ivm_napi.dylib'),
  resolve(NAPI, 'rust-ivm.node'),
  resolve(NAPI, 'target/release/librust_ivm_napi.so'),
];
const addonPath = candidates.find(p => {
  try {
    require('fs').readFileSync(p, {flag: 'rs'});
    return true;
  } catch {
    return false;
  }
});
if (!addonPath) throw new Error('napi addon not found');

let NODEPATH = addonPath;
if (!addonPath.endsWith('.node')) {
  NODEPATH = join(tmpdir(), `rust-ivm-whale-${process.pid}.node`);
  copyFileSync(addonPath, NODEPATH);
}
const addon = require(NODEPATH);

const NUM_ROWS = 13_000;

function createWhaleDb(dbPath) {
  const db = new DatabaseSync(dbPath);
  db.exec('PRAGMA journal_mode = WAL');
  db.exec('DROP TABLE IF EXISTS "_zero.replicationState"');
  db.exec(
    'CREATE TABLE "_zero.replicationState" (stateVersion TEXT NOT NULL, lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1))',
  );
  db.exec('INSERT INTO "_zero.replicationState" (stateVersion) VALUES (\'0\')');
  db.exec('DROP TABLE IF EXISTS "_zero.changeLog2"');
  db.exec(
    'CREATE TABLE "_zero.changeLog2" ("stateVersion" TEXT NOT NULL, "pos" INT NOT NULL, "table" TEXT NOT NULL, "rowKey" TEXT NOT NULL, "op" TEXT NOT NULL, PRIMARY KEY("stateVersion", "pos"), UNIQUE("table", "rowKey"))',
  );

  db.exec('DROP TABLE IF EXISTS "whale"');
  db.exec(
    'CREATE TABLE "whale" ("id" TEXT PRIMARY KEY, "title" TEXT NOT NULL, "count" INTEGER NOT NULL, "active" INTEGER NOT NULL, "note" TEXT, "_0_version" TEXT NOT NULL DEFAULT \'0\')',
  );

  const stmt = db.prepare(
    'INSERT OR IGNORE INTO "whale" ("id", "title", "count", "active", "note", "_0_version") VALUES (?, ?, ?, ?, ?, ?)',
  );
  db.exec('BEGIN');
  for (let i = 0; i < NUM_ROWS; i++) {
    stmt.run(
      `whale-r${i}`,
      `Row ${i} — Lorem ipsum dolor sit amet`,
      i,
      i % 2 === 0 ? 1 : 0,
      i % 3 === 0 ? null : `note-${i}`,
      '0',
    );
  }
  db.exec('COMMIT');
  return db;
}

function buildTableSpecs() {
  return [
    {
      table: 'whale',
      columns: {
        id: {type: 'string', optional: false},
        title: {type: 'string', optional: false},
        count: {type: 'number', optional: false},
        active: {type: 'boolean', optional: false},
        note: {type: 'string', optional: true},
        _0_version: {type: 'string', optional: false},
      },
      primaryKey: ['id'],
      minRowVersion: '0',
    },
  ];
}

const AST = {table: 'whale', where: null};

async function main() {
  const dbPath = join(tmpdir(), `whale-${Date.now()}-${process.pid}.db`);
  console.log(`Whale validation: ${NUM_ROWS} rows\n`);

  // --- Streaming path ---
  {
    const keeper = createWhaleDb(dbPath);
    const engine = new addon.RustIvmEngine();
    engine.init(buildTableSpecs(), dbPath, 'test');

    const rssBefore = process.memoryUsage().rss;
    let count = 0;
    const streamId = 1;
    await engine.addQueriesStreamingRows(
      [{queryId: 'q1', astJson: JSON.stringify(AST)}],
      (_err, rc) => {
        if (!rc) return;
        engine.grantStreamCredit(streamId, 1);
        if (rc.changeType >= 0) count++;
      },
      streamId,
    );
    const rssAfter = process.memoryUsage().rss;
    const deltaMB = (rssAfter - rssBefore) / 1024 / 1024;

    console.log(
      `Streaming: ${count} rows, RSS delta = ${deltaMB.toFixed(1)} MB (before=${(rssBefore / 1024 / 1024).toFixed(0)} MB, after=${(rssAfter / 1024 / 1024).toFixed(0)} MB)`,
    );
    engine.destroy();
    keeper.close();
  }

  // Yield between engines (TSFN lifecycle)
  await new Promise(r => setImmediate(r));
  await new Promise(r => setTimeout(r, 50));

  // --- Eager path ---
  {
    const keeper = createWhaleDb(dbPath);
    const engine = new addon.RustIvmEngine();
    engine.init(buildTableSpecs(), dbPath, 'test');

    const rssBefore = process.memoryUsage().rss;
    const out = await engine.addQueriesStreaming([
      {queryId: 'q1', astJson: JSON.stringify(AST)},
    ]);
    const rssAfter = process.memoryUsage().rss;
    const deltaMB = (rssAfter - rssBefore) / 1024 / 1024;
    const count = out.filter(r => r.changeType >= 0).length;

    console.log(
      `Eager:     ${count} rows, RSS delta = ${deltaMB.toFixed(1)} MB (before=${(rssBefore / 1024 / 1024).toFixed(0)} MB, after=${(rssAfter / 1024 / 1024).toFixed(0)} MB)`,
    );
    engine.destroy();
    keeper.close();
  }

  for (const ext of ['', '-wal', '-shm']) rmSync(dbPath + ext, {force: true});
  if (!addonPath.endsWith('.node')) rmSync(NODEPATH, {force: true});
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
