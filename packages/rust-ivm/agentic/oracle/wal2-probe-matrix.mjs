#!/usr/bin/env node
// Empirical matrix: which wal_checkpoint mode detects a STALE read-mark (pinned
// below head) while staying clean with only AT-HEAD read-marks — on the real
// wal2 zero-sqlite3. Decides the probe mode for ART detector #1b.
import {rmSync, statSync} from 'node:fs';
import {createRequire} from 'node:module';
import {tmpdir} from 'node:os';
import {resolve, join, dirname} from 'node:path';
import {fileURLToPath} from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const zqliteRequire = createRequire(
  resolve(__dirname, '..', '..', '..', 'zqlite', 'package.json'),
);
const SQLiteDatabase = zqliteRequire('@rocicorp/zero-sqlite3');

function fresh(tag) {
  const p = join(tmpdir(), `wal2-probe-${process.pid}-${tag}.db`);
  for (const ext of ['', '-wal', '-wal2', '-shm']) rmSync(p + ext, {force: true});
  const db = new SQLiteDatabase(p);
  db.pragma('journal_mode = wal2');
  // Small rotation target so writes actually SWITCH wal files — without a
  // switch, wal2 checkpoints nothing and every probe is trivially clean.
  db.pragma('journal_size_limit = 4096');
  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)');
  db.exec("INSERT INTO t VALUES (1, 'a')");
  return {p, db};
}

function pin(p) {
  const c = new SQLiteDatabase(p);
  c.exec('BEGIN CONCURRENT');
  c.prepare('SELECT count(*) FROM t').get();
  return c;
}

function walSizes(p) {
  const sz = f => {
    try {
      return statSync(f).size;
    } catch {
      return 0;
    }
  };
  return `wal=${sz(p + '-wal')} wal2=${sz(p + '-wal2')}`;
}

function probeAll(p, label) {
  const out = [];
  for (const mode of ['PASSIVE', 'FULL', 'RESTART', 'TRUNCATE']) {
    const c = new SQLiteDatabase(p);
    const r = c.pragma(`wal_checkpoint(${mode})`);
    const row = Array.isArray(r) ? r[0] : r;
    out.push(
      `${mode}: busy=${row.busy} log=${row.log} ckpt=${row.checkpointed} | after: ${walSizes(p)}`,
    );
    c.close();
  }
  console.log(`${label}  [before: ${walSizes(p)}]\n  ${out.join('\n  ')}`);
}

// Scenario A: STALE pin — pinned, then 200 writes on another conn.
{
  const {p, db} = fresh('stale');
  const stale = pin(p);
  for (let i = 2; i < 202; i++) db.exec(`INSERT INTO t VALUES (${i}, 'x')`);
  probeAll(p, 'A: stale pin (pinned below head)  — want busy=1');
  stale.close();
  db.close();
}

// Scenario B: AT-HEAD pins only — 200 writes first, then two pins (like the
// engine's curr+prev right after an advance), no writes after.
{
  const {p, db} = fresh('head');
  for (let i = 2; i < 202; i++) db.exec(`INSERT INTO t VALUES (${i}, 'x')`);
  const a = pin(p);
  const b = pin(p);
  probeAll(p, 'B: two at-head pins (healthy engine) — want busy=0');
  a.close();
  b.close();
  db.close();
}

// Scenario C: no pins at all (control).
{
  const {p, db} = fresh('none');
  for (let i = 2; i < 202; i++) db.exec(`INSERT INTO t VALUES (${i}, 'x')`);
  probeAll(p, 'C: no pins (control)              — want busy=0');
  db.close();
}

// Scenario D: the actual ART probe flow — at-head pins exist, then the PROBE
// ITSELF writes (forcing switches) before checkpointing. The engine pins are
// below the probe's own writes; must still read clean or the probe is useless.
{
  const {p, db} = fresh('probe-writes');
  for (let i = 2; i < 202; i++) db.exec(`INSERT INTO t VALUES (${i}, 'x')`);
  const a = pin(p);
  const b = pin(p);
  for (let i = 300; i < 500; i++) db.exec(`INSERT INTO t VALUES (${i}, 'y')`);
  probeAll(p, 'D: at-head pins + probe writes    — want busy=0 (or a stable non-signal)');
  a.close();
  b.close();
  db.close();
}

// v2 probe: one tiny write (switches the big active file to inactive), then
// checkpoint it. Repeat ×2 so both files get a turn.
function probeV2(p, label) {
  const c = new SQLiteDatabase(p);
  // MUST be >0: wal2's walRestartLog only honours mxWalSize>0 (0/-1 fall back
  // to WAL_DEFAULT_WALSIZE frames, so small fixtures never switch).
  c.pragma('journal_size_limit = 4096');
  c.exec('CREATE TABLE IF NOT EXISTS "_art_probe" (k INTEGER PRIMARY KEY, v)');
  const lines = [];
  for (let round = 1; round <= 2; round++) {
    c.exec(`INSERT INTO "_art_probe" (v) VALUES (${round})`);
    const r = c.pragma('wal_checkpoint(PASSIVE)');
    const row = Array.isArray(r) ? r[0] : r;
    lines.push(
      `round${round}: busy=${row.busy} log=${row.log} ckpt=${row.checkpointed} | ${walSizes(p)}`,
    );
  }
  c.close();
  console.log(`${label}\n  ${lines.join('\n  ')}`);
}

{
  const {p, db} = fresh('v2-stale');
  const stale = pin(p);
  for (let i = 2; i < 202; i++) db.exec(`INSERT INTO t VALUES (${i}, 'x')`);
  probeV2(p, 'E: v2 probe, STALE pin      — want a signal (busy/size)');
  stale.close();
  db.close();
}
{
  const {p, db} = fresh('v2-head');
  for (let i = 2; i < 202; i++) db.exec(`INSERT INTO t VALUES (${i}, 'x')`);
  const a = pin(p);
  const b = pin(p);
  probeV2(p, 'F: v2 probe, at-head pins   — want CLEAN');
  a.close();
  b.close();
  db.close();
}
{
  const {p, db} = fresh('v2-none');
  for (let i = 2; i < 202; i++) db.exec(`INSERT INTO t VALUES (${i}, 'x')`);
  probeV2(p, 'G: v2 probe, no pins        — want CLEAN');
  db.close();
}
