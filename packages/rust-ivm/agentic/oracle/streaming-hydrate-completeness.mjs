#!/usr/bin/env node
// oracle/streaming-hydrate-completeness.mjs
//
// ROOT invariant for the take-bound divergence ("make it as rare as TS").
//
// The take operator's `bound == None` (empty-hydrated partition) + an
// incremental Edit is the take.rs:670 reset. That contradictory state can ONLY
// arise legitimately from a GENUINELY EMPTY partition — OR illegitimately from
// the STREAMING HYDRATE DROPPING A ROW (the TSFN drain-barrier class, which
// historically dropped the LAST hydrate row ~50% of the time). If the streaming
// hydrate is complete, a take partition can never be seen as empty when it isn't,
// so the divergence can't be manufactured — matching TS, whose synchronous
// generator hydrate is complete by construction.
//
// This gate exercises the REAL production path — addQueriesStreamingRows over a
// wal2 replica with per-row credit (the exact TSFN boundary where a drop lives)
// — and asserts EVERY run emits EXACTLY the expected row count, for both a
// take/limit query and a full-table query, across many repetitions (a
// probabilistic drop must fail here, not "be observed in prod").
//
// Exit 0 = every run complete. Exit 1 = a drop was detected (with the offending
// config + run index). Run: node agentic/oracle/streaming-hydrate-completeness.mjs

import {rmSync, readFileSync, copyFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {tmpdir} from 'node:os';
import {resolve, join, dirname} from 'node:path';
import {fileURLToPath} from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const zqliteRequire = createRequire(
  resolve(__dirname, '..', '..', '..', 'zqlite', 'package.json'),
);
const SQLiteDatabase = zqliteRequire('@rocicorp/zero-sqlite3');

const NAPI = resolve(__dirname, '..', '..', 'napi');
const fileExists = p => {
  try {
    readFileSync(p, {flag: 'rs'});
    return true;
  } catch {
    return false;
  }
};
const candidates = process.env.RUST_IVM_ADDON
  ? [process.env.RUST_IVM_ADDON]
  : process.platform === 'darwin'
    ? [
        resolve(NAPI, 'target/release/librust_ivm_napi.dylib'),
        resolve(NAPI, 'rust-ivm.node'),
      ]
    : [
        resolve(NAPI, 'rust-ivm.node'),
        resolve(NAPI, 'target/release/librust_ivm_napi.so'),
      ];
const addonPath = candidates.find(fileExists);
if (!addonPath) {
  throw new Error(`napi addon not found. tried:\n  ${candidates.join('\n  ')}`);
}
let NODEPATH = addonPath;
if (!addonPath.endsWith('.node')) {
  NODEPATH = join(tmpdir(), `rust-ivm-completeness-${process.pid}.node`);
  copyFileSync(addonPath, NODEPATH);
}
const addon = require(NODEPATH);

function createDb(dbPath, numRows) {
  const db = new SQLiteDatabase(dbPath);
  db.pragma('journal_mode = wal2');
  db.exec('DROP TABLE IF EXISTS "_zero.replicationState"');
  db.exec(
    'CREATE TABLE "_zero.replicationState" (stateVersion TEXT NOT NULL, lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1))',
  );
  db.exec('INSERT INTO "_zero.replicationState" (stateVersion) VALUES (\'0\')');
  db.exec('DROP TABLE IF EXISTS "_zero.changeLog2"');
  db.exec(
    'CREATE TABLE "_zero.changeLog2" ("stateVersion" TEXT NOT NULL, "pos" INT NOT NULL, "table" TEXT NOT NULL, "rowKey" TEXT NOT NULL, "op" TEXT NOT NULL, PRIMARY KEY("stateVersion", "pos"), UNIQUE("table", "rowKey"))',
  );
  db.exec('DROP TABLE IF EXISTS "items"');
  db.exec(
    'CREATE TABLE "items" ("id" INTEGER PRIMARY KEY, "part" TEXT NOT NULL, "n" INTEGER NOT NULL, "_0_version" TEXT NOT NULL DEFAULT \'0\')',
  );
  const stmt = db.prepare(
    'INSERT INTO "items" ("id", "part", "n", "_0_version") VALUES (?, ?, ?, ?)',
  );
  db.exec('BEGIN');
  for (let i = 0; i < numRows; i++) {
    // Single partition ("p0") so a limit query builds ONE take partition of
    // known size — exactly the shape whose empty-hydrate produces bound=None.
    stmt.run(i, 'p0', i, '0');
  }
  db.exec('COMMIT');
  return db;
}

const TABLE_SPEC = [
  {
    table: 'items',
    columns: {
      id: {type: 'number', optional: false},
      part: {type: 'string', optional: false},
      n: {type: 'number', optional: false},
      _0_version: {type: 'string', optional: false},
    },
    primaryKey: ['id'],
    minRowVersion: '0',
  },
];

// One streaming hydrate; returns the count of real (changeType>=0) rows.
async function hydrateCount(dbPath, astJson) {
  const keeper = createDb(dbPath, hydrateCount._numRows);
  const engine = new addon.RustIvmEngine();
  engine.init(TABLE_SPEC, dbPath, 'test');
  let count = 0;
  const streamId = 1;
  await engine.addQueriesStreamingRows(
    [{queryId: 'q1', astJson}],
    (err, chunk) => {
      if (err) throw err;
      if (!chunk) return;
      for (const rc of Array.isArray(chunk) ? chunk : [chunk]) {
        engine.grantStreamCredit(streamId, 1);
        if (rc.changeType >= 0) count++;
      }
    },
    streamId,
  );
  await engine.destroy();
  keeper.close();
  for (const ext of ['', '-wal', '-wal2', '-shm'])
    rmSync(dbPath + ext, {force: true});
  return count;
}

async function main() {
  const REPS = Number(process.env.COMPLETENESS_REPS || 120);
  // (numRows, limit|null, expected). limit=null => full table.
  const CONFIGS = [
    {numRows: 1, limit: 3, label: 'take limit=3 over 1 row'},
    {numRows: 3, limit: 3, label: 'take limit=3 over 3 rows (boundary)'},
    {numRows: 5, limit: 3, label: 'take limit=3 over 5 rows'},
    {numRows: 200, limit: 50, label: 'take limit=50 over 200 rows'},
    {numRows: 1, limit: null, label: 'full over 1 row (last-row-drop bait)'},
    {numRows: 200, limit: null, label: 'full over 200 rows'},
  ];

  let failures = 0;
  for (const cfg of CONFIGS) {
    const expected =
      cfg.limit === null ? cfg.numRows : Math.min(cfg.numRows, cfg.limit);
    const ast = {table: 'items', orderBy: [['id', 'asc']]};
    if (cfg.limit !== null) ast.limit = cfg.limit;
    const astJson = JSON.stringify(ast);
    hydrateCount._numRows = cfg.numRows;

    const bad = [];
    for (let r = 0; r < REPS; r++) {
      const dbPath = join(
        tmpdir(),
        `completeness-${process.pid}-${cfg.numRows}-${r}.db`,
      );
      // eslint-disable-next-line no-await-in-loop
      const got = await hydrateCount(dbPath, astJson);
      if (got !== expected) bad.push({run: r, got});
      // Yield so the previous task's TSFN fully releases (napi lifecycle).
      // eslint-disable-next-line no-await-in-loop
      await new Promise(res => setImmediate(res));
    }

    if (bad.length === 0) {
      console.log(`PASS  ${cfg.label} — ${REPS}/${REPS} runs emitted ${expected} rows`);
    } else {
      failures++;
      const sample = bad
        .slice(0, 5)
        .map(b => `run#${b.run} got ${b.got}`)
        .join(', ');
      console.error(
        `FAIL  ${cfg.label} — expected ${expected}, ${bad.length}/${REPS} runs DROPPED rows: ${sample}${bad.length > 5 ? ', …' : ''}`,
      );
    }
  }

  if (!addonPath.endsWith('.node')) rmSync(NODEPATH, {force: true});

  if (failures > 0) {
    console.error(
      `\nSTREAMING HYDRATE INCOMPLETE: ${failures} config(s) dropped rows. ` +
        `A dropped row manufactures the take.rs bound=None divergence — ` +
        `this is the root, not the -2 recovery. Fix the drain-barrier path.`,
    );
    process.exit(1);
  }
  console.log('\nAll streaming hydrates complete — bound=None can only mean a genuinely empty partition.');
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
