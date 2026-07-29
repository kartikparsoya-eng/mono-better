#!/usr/bin/env node
// scripts/parallelism-test.mjs — proves inter-CG parallelism at the napi layer.
//
// Each RustIvmEngine now runs its engine on its own OS thread (actor), and
// addQueriesStreaming/advanceToHeadStreaming are async (AsyncTask off the JS
// loop). So N engines (= N client groups) hydrating concurrently should finish
// in ~max(individual) wall time, not sum(individual).
//
// We build N independent SQLite DBs with a CPU-heavy correlated-EXISTS query,
// then time: (a) sequential (await each in turn) vs (b) parallel (Promise.all).
// If the actor+async wiring is real, parallel wall << sequential wall on a
// multi-core machine.
//
// Usage: node scripts/parallelism-test.mjs [engines] [t0rows] [t1rows]

import { DatabaseSync } from 'node:sqlite';
import { createRequire } from 'node:module';
import { resolve, join, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { readFileSync, rmSync, copyFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { availableParallelism } from 'node:os';

const __dirname = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

// --- addon resolution (same as napi-sqlite-runner) ---
const NAPI = resolve(__dirname, '..', 'napi');
const exists = (p) => { try { readFileSync(p, { flag: 'rs' }); return true; } catch { return false; } };
const candidates = process.env.RUST_IVM_ADDON
  ? [process.env.RUST_IVM_ADDON]
  : process.platform === 'darwin'
    ? [resolve(NAPI, 'target/release/librust_ivm_napi.dylib'), resolve(NAPI, 'rust-ivm.node')]
    : [resolve(NAPI, 'rust-ivm.node'), resolve(NAPI, 'target/release/librust_ivm_napi.so')];
const addonPath = candidates.find(exists);
if (!addonPath) throw new Error(`napi addon not found:\n  ${candidates.join('\n  ')}`);
let NODEPATH = addonPath;
if (!addonPath.endsWith('.node')) {
  NODEPATH = join(tmpdir(), `rust-ivm-par-${process.pid}.node`);
  copyFileSync(addonPath, NODEPATH);
}
const addon = require(NODEPATH);

const N = Number(process.argv[2] || Math.min(4, availableParallelism()));
const T0 = Number(process.argv[3] || 1200);
const T1 = Number(process.argv[4] || 6000);

console.log(`cores=${availableParallelism()} engines=${N} t0rows=${T0} t1rows=${T1}`);

// --- build a heavy DB: t0 with correlated EXISTS into t1 (CPU-bound hydrate) ---
function makeDb(i) {
  const path = join(tmpdir(), `par-test-${process.pid}-${i}.db`);
  for (const ext of ['', '-wal', '-shm']) rmSync(path + ext, { force: true });
  const db = new DatabaseSync(path);
  db.exec('PRAGMA journal_mode = DELETE');
  db.exec('CREATE TABLE "t0" ("id" TEXT PRIMARY KEY, "c1" INTEGER)');
  db.exec('CREATE TABLE "t1" ("id" TEXT PRIMARY KEY, "fk" TEXT, "c1" INTEGER)');
  const s0 = db.prepare('INSERT INTO "t0" ("id","c1") VALUES (?,?)');
  for (let r = 0; r < T0; r++) s0.run(`t0-${r}`, r % 100);
  const s1 = db.prepare('INSERT INTO "t1" ("id","fk","c1") VALUES (?,?,?)');
  for (let r = 0; r < T1; r++) s1.run(`t1-${r}`, `t0-${r % T0}`, r % 50);
  db.close();
  return path;
}

const tableSpecs = [
  { table: 't0', columns: { id: { type: 'string', optional: false }, c1: { type: 'number', optional: false } }, primaryKey: ['id'] },
  { table: 't1', columns: { id: { type: 'string', optional: false }, fk: { type: 'string', optional: false }, c1: { type: 'number', optional: false } }, primaryKey: ['id'] },
];

// query: t0 WHERE EXISTS (t1 where t1.fk = t0.id AND t1.c1 > 10) — correlated,
// per-node EXISTS work = the known CPU-heavy cold-hydrate path.
const ast = {
  table: 't0',
  orderBy: [['id', 'asc']],
  where: {
    type: 'correlatedSubquery',
    op: 'EXISTS',
    related: {
      correlation: { parentField: ['id'], childField: ['fk'] },
      subquery: {
        table: 't1', alias: 'zsubq_t1', orderBy: [['id', 'asc']],
        where: { type: 'simple', op: '>', left: { type: 'column', name: 'c1' }, right: { type: 'literal', value: 10 } },
      },
    },
  },
};

function makeEngine(path) {
  const e = new addon.RustIvmEngine();
  e.init(tableSpecs, path, 'test');
  return e;
}
async function hydrate(engine) {
  const out = await engine.addQueriesStreaming([{ queryId: 'q1', astJson: JSON.stringify(ast) }]);
  return out.filter(r => r.changeType >= 0).length;
}

const dbs = Array.from({ length: N }, (_, i) => makeDb(i));
const engines = dbs.map(makeEngine);

// warm once (JIT / first-touch) — one hydrate, discard timing
await hydrate(engines[0]);

// --- sequential ---
let seqRows = 0;
const seqStart = performance.now();
for (const e of engines) seqRows += await hydrate(e);
const seqMs = performance.now() - seqStart;

// --- parallel (all engines at once) ---
const parStart = performance.now();
const parCounts = await Promise.all(engines.map(hydrate));
const parMs = performance.now() - parStart;
const parRows = parCounts.reduce((a, b) => a + b, 0);

const speedup = seqMs / parMs;
console.log(`rows/engine=${parCounts[0]} (seq total=${seqRows}, par total=${parRows})`);
console.log(`sequential: ${seqMs.toFixed(1)}ms   parallel: ${parMs.toFixed(1)}ms   speedup: ${speedup.toFixed(2)}x`);

// cleanup
for (const p of dbs) for (const ext of ['', '-wal', '-shm']) rmSync(p + ext, { force: true });

// verdict: with real inter-CG parallelism, parallel should be meaningfully
// faster than sequential (allow for overhead; require >1.5x on >=2 cores).
const threshold = Math.min(1.5, Math.max(1.2, N * 0.5));
if (availableParallelism() >= 2 && N >= 2) {
  if (speedup >= threshold) {
    console.log(`PASS — inter-CG parallelism confirmed (>=${threshold.toFixed(2)}x)`);
    process.exit(0);
  } else {
    console.log(`FAIL — expected >=${threshold.toFixed(2)}x speedup, got ${speedup.toFixed(2)}x (engines serialized?)`);
    process.exit(1);
  }
}
console.log('SKIP verdict (need >=2 cores and >=2 engines)');
