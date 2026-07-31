// NAPI boundary micro-experiment: decompose the per-row delivery cost.
//
// Same compute, two transports:
//   - addQueriesStreaming(q)          -> Promise<NapiRowChange[]>  : 1 crossing for N rows (eager)
//   - addQueriesStreamingRows(q, onRow) -> per-row TSFN (max_queue_size=1, Blocking): N crossings
//
// Three arms isolate the split:
//   eager            = compute + 1 batched crossing         => per-row compute floor
//   stream + no-op   = compute + N crossings, empty callback => + boundary/scheduling tax
//   stream + real    = compute + N crossings + JSON.parse    => + JS marshal cost
//
// Decomposition (per row):
//   compute  ~= eager / N
//   boundary ~= (stream_noop - eager) / N      <- the TSFN round-trip; chunking kills this
//   marshal  ~= (stream_real - stream_noop) / N
import { DatabaseSync } from 'node:sqlite';
import { createRequire } from 'node:module';
import { resolve, join } from 'node:path';
import { tmpdir } from 'node:os';
import { rmSync, copyFileSync } from 'node:fs';

const require = createRequire(import.meta.url);
const SRC = process.env.RUST_IVM_ADDON ||
  resolve(import.meta.dirname, '..', 'napi', 'target', 'release', 'librust_ivm_napi.dylib');
const NODEPATH = join(tmpdir(), `rust-ivm-boundary-${process.pid}.node`);
copyFileSync(SRC, NODEPATH);
const addon = require(NODEPATH);

const N = Number(process.env.N || 20000);
const REPS = Number(process.env.REPS || 4); // 1 warmup + rest measured

// ---- build a plain SQLite table with N rows -------------------------------
const dbPath = join(tmpdir(), `boundary-${Date.now()}.db`);
for (const ext of ['', '-wal', '-shm']) rmSync(dbPath + ext, { force: true });
const db = new DatabaseSync(dbPath);
db.exec(`PRAGMA journal_mode = WAL`);
db.exec(`PRAGMA synchronous = NORMAL`);
db.exec(`CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, email TEXT, age INTEGER, active INTEGER)`);
const ins = db.prepare(`INSERT INTO users VALUES (?,?,?,?,?)`);
db.exec('BEGIN');
for (let i = 0; i < N; i++) {
  const s = String(i).padStart(7, '0');
  ins.run(`u-${s}`, `name-${s}`, `user${s}@example.com`, 20 + (i % 60), i % 2);
}
db.exec('COMMIT');
db.close();
console.log(`seeded ${N} users -> ${dbPath}`);

const SCHEMA = [{
  table: 'users',
  columns: {
    id: { type: 'string', optional: false },
    name: { type: 'string', optional: false },
    email: { type: 'string', optional: false },
    age: { type: 'number', optional: false },
    active: { type: 'number', optional: false },
  },
  primaryKey: ['id'],
}];
const AST = JSON.stringify({ table: 'users', orderBy: [['id', 'asc']] });

function freshEngine() {
  const e = new addon.RustIvmEngine();
  e.init(SCHEMA, dbPath, 'bench');
  return e;
}

function median(xs) { const s = [...xs].sort((a, b) => a - b); return s[Math.floor(s.length / 2)]; }
const ms = (ns) => (ns / 1e6);

async function timeArm(name, fn) {
  const times = [];
  let rows = 0;
  for (let r = 0; r < REPS; r++) {
    const e = freshEngine();
    const t0 = process.hrtime.bigint();
    rows = await fn(e);
    const t1 = process.hrtime.bigint();
    if (r > 0) times.push(Number(t1 - t0)); // drop warmup
    try { e.destroy?.(); } catch {}
  }
  const med = median(times);
  const res = { name, rows, medMs: ms(med), perRowUs: (med / rows) / 1000, all: times.map(ms) };
  console.log(`  [done] ${res.name.padEnd(42)} rows=${res.rows} total=${res.medMs.toFixed(1)}ms per-row=${res.perRowUs.toFixed(3)}µs`);
  return res;
}

const arms = {};

// Arm A: eager — 1 crossing
arms.eager = await timeArm('eager (1 crossing)', async (e) => {
  const rows = await e.addQueriesStreaming([{ queryId: 'qE', astJson: AST }]);
  return rows.length;
});

// Arm B: streaming, no-op onRow — N crossings, empty callback
arms.noop = await timeArm('stream + no-op onRow (N crossings)', async (e) => {
  let n = 0;
  await e.addQueriesStreamingRows([{ queryId: 'qN', astJson: AST }], () => { n++; });
  return n;
});

// Arm C: streaming, real marshal — N crossings + JSON.parse x2. Callback is
// (err, row) — CalleeHandled TSFN passes two args (matches rust-ivm-driver.ts).
arms.real = await timeArm('stream + real onRow (N crossings + parse)', async (e) => {
  let n = 0;
  const sink = [];
  await e.addQueriesStreamingRows([{ queryId: 'qR', astJson: AST }], (_err, c) => {
    if (!c || c.changeType < 0) return;
    JSON.parse(c.rowKey);
    if (c.row) JSON.parse(c.row);
    sink.push(c.changeType);
    n++;
  });
  return n;
});

// ---- concurrency sweep: reproduce the single-thread delivery cliff ---------
// All streams' onRow callbacks funnel through the ONE main JS thread. Run C
// concurrent hydrations and watch per-row wall time balloon as C rises.
async function concSweep(C) {
  const engines = Array.from({ length: C }, () => freshEngine());
  const t0 = process.hrtime.bigint();
  let total = 0;
  await Promise.all(engines.map((e, i) =>
    e.addQueriesStreamingRows([{ queryId: `qc${i}`, astJson: AST }], (_err, c) => {
      if (!c || c.changeType < 0) return;
      JSON.parse(c.rowKey);
      if (c.row) JSON.parse(c.row);
      total++;
    })));
  const t1 = process.hrtime.bigint();
  for (const e of engines) { try { e.destroy?.(); } catch {} }
  const wallMs = Number(t1 - t0) / 1e6;
  const perRowUs = (Number(t1 - t0) / total) / 1000;
  console.log(`  [conc C=${String(C).padStart(2)}] streams=${C} rows=${total} wall=${wallMs.toFixed(1)}ms per-row=${perRowUs.toFixed(3)}µs`);
  return { C, wallMs, perRowUs, total };
}
console.log(`\n--- concurrency sweep (C concurrent streaming hydrations, real onRow) ---`);
const conc = [];
for (const C of [1, 2, 4, 8]) conc.push(await concSweep(C));

// ---- busy-main-thread arm: the real production mechanism -------------------
// With max_queue_size=1 Blocking, the actor parks after EVERY row until the
// main thread drains it. If the main thread is busy (CVR flush / poke serialize
// / WS writes / other CGs), each row's delivery waits behind that work. Simulate
// with a self-rescheduling CPU burst on the event loop during one hydration.
async function busyMainArm(burstMs) {
  const e = freshEngine();
  let stop = false;
  // event-loop hog: every setImmediate, spin the CPU for burstMs
  const hog = () => {
    if (stop) return;
    if (burstMs > 0) { const end = Number(process.hrtime.bigint()) + burstMs * 1e6; while (Number(process.hrtime.bigint()) < end) {} }
    setImmediate(hog);
  };
  setImmediate(hog);
  let n = 0;
  const t0 = process.hrtime.bigint();
  await e.addQueriesStreamingRows([{ queryId: 'qBusy', astJson: AST }], (_err, c) => {
    if (!c || c.changeType < 0) return;
    JSON.parse(c.rowKey); if (c.row) JSON.parse(c.row); n++;
  });
  const t1 = process.hrtime.bigint();
  stop = true;
  try { e.destroy?.(); } catch {}
  const wallMs = Number(t1 - t0) / 1e6;
  console.log(`  [busy burst=${burstMs}ms] rows=${n} wall=${wallMs.toFixed(1)}ms per-row=${((Number(t1 - t0) / n) / 1000).toFixed(3)}µs`);
  return { burstMs, wallMs, perRowUs: (Number(t1 - t0) / n) / 1000 };
}
console.log(`\n--- busy-main-thread arm (main loop occupied during hydration; queue=1) ---`);
const busy = [];
for (const b of [0, 0.5, 2, 5]) busy.push(await busyMainArm(b));

// ---- correctness: streaming (current queue depth) must byte-match eager -----
{
  const e1 = freshEngine();
  const eager = await e1.addQueriesStreaming([{ queryId: 'qGT', astJson: AST }]);
  try { e1.destroy?.(); } catch {}
  const e2 = freshEngine();
  const streamed = [];
  await e2.addQueriesStreamingRows([{ queryId: 'qST', astJson: AST }], (_err, c) => { if (c) streamed.push(c); });
  try { e2.destroy?.(); } catch {}
  const key = (r) => `${r.changeType}|${r.table}|${r.rowKey}|${r.row ?? ''}`;
  let mism = 0;
  if (eager.length !== streamed.length) mism = -1;
  else for (let i = 0; i < eager.length; i++) if (key(eager[i]) !== key(streamed[i])) { mism++; }
  const q = process.env.RUST_IVM_TSFN_QUEUE || '1';
  console.log(`\n--- correctness (queue=${q}): eager=${eager.length} streamed=${streamed.length} mismatches=${mism === -1 ? 'LENGTH-DIFF' : mism} -> ${mism === 0 ? 'IDENTICAL ✓' : 'DIVERGED ✗'}`);
}

for (const ext of ['', '-wal', '-shm']) rmSync(dbPath + ext, { force: true });
rmSync(NODEPATH, { force: true });

// ---- report ----------------------------------------------------------------
console.log(`\n=== NAPI boundary decomposition (N=${N}, ${REPS - 1} measured reps, median) ===`);
for (const k of ['eager', 'noop', 'real']) {
  const a = arms[k];
  console.log(`  ${a.name.padEnd(42)} rows=${a.rows}  total=${a.medMs.toFixed(1)}ms  per-row=${a.perRowUs.toFixed(3)}µs   [${a.all.map(x => x.toFixed(0)).join(',')}]`);
}
const compute = arms.eager.perRowUs;
const boundary = arms.noop.perRowUs - arms.eager.perRowUs;
const marshal = arms.real.perRowUs - arms.noop.perRowUs;
console.log(`\n  per-row breakdown:`);
console.log(`    compute  (eager/N)           = ${compute.toFixed(3)} µs`);
console.log(`    boundary (noop-eager)/N      = ${boundary.toFixed(3)} µs   <- TSFN round-trip (chunking removes this)`);
console.log(`    marshal  (real-noop)/N       = ${marshal.toFixed(3)} µs   <- JSON.parse x2 + push`);
const deliveryTotal = boundary + marshal;
console.log(`\n  streaming per-row total        = ${arms.real.perRowUs.toFixed(3)} µs  (compute ${(100*compute/arms.real.perRowUs).toFixed(0)}% | boundary ${(100*boundary/arms.real.perRowUs).toFixed(0)}% | marshal ${(100*marshal/arms.real.perRowUs).toFixed(0)}%)`);
console.log(`  eager vs streaming speedup     = ${(arms.real.medMs / arms.eager.medMs).toFixed(1)}x  (what perfect chunking approaches)`);
const verdict = boundary > marshal
  ? 'isolated: BOUNDARY(scheduling)-DOMINATED'
  : 'isolated: MARSHAL(CPU)-DOMINATED';
console.log(`\n  ISOLATED VERDICT: ${verdict}`);

const c1 = conc.find(c => c.C === 1).perRowUs;
const c8 = conc.find(c => c.C === 8).perRowUs;
console.log(`\n  concurrency scaling: per-row ${c1.toFixed(2)}µs @C=1 -> ${c8.toFixed(2)}µs @C=8  (${(c8 / c1).toFixed(1)}x)`);
console.log(`  If per-row balloons with C, the single main JS thread is saturating on delivery`);
console.log(`  (the production cliff) -> lever A (chunk transport = fewer main-thread callbacks) is primary.`);
