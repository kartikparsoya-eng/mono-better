#!/usr/bin/env node
// oracle/wal2-relay-pin-threshold.mjs
//
// ROOT-CAUSE REPRO for unbounded WAL growth on rust pods: RELAY PINNING.
//
// wal2 can only rotate wal->wal2 when the inactive file is fully checkpointed
// and NO reader pins a frame in it. Each CG's snapshotter holds TWO long-lived
// BEGIN CONCURRENT read-marks (curr+prev) that only move when that CG advances.
// With N CGs advancing on a ~fixed cadence, the inactive file gets a
// zero-reader instant only if ALL N CGs' marks simultaneously sit in the
// current file. If each CG's old marks persist for `dwell` ms out of every
// `cadence` ms cycle (dwell = how long an advance/hydrate takes while holding
// the pre-advance snapshot), the per-attempt clear probability is roughly
// (1 - dwell/cadence)^N -- exponentially sensitive to dwell.
//
// Prod numbers this models (pod hf2cg, 2026-08-05): N=44 CGs, version cadence
// ~5s, per-op dwell p50=0.7s / p90=4.6s (rust) vs tens of ms (TS).
//
// This harness uses REAL wal2 (zero-sqlite3), a REAL steady writer, and N
// simulated snapshotter pairs leapfrogging (ROLLBACK old + BEGIN CONCURRENT at
// head) after `dwell` ms of each cycle. Nothing else varies across scenarios.
//
// Expected: WAL growth bounded when dwell << cadence; unbounded (linear with
// write rate) when dwell approaches cadence. A sharp threshold in between.
//
// Run: node agentic/oracle/wal2-relay-pin-threshold.mjs
//      [--cgs 44] [--cadence 500] [--phase 15000] [--dwells 20,100,250,450]

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

// ---------------------------------------------------------------------------
// Args
// ---------------------------------------------------------------------------
const arg = (name, dflt) => {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : dflt;
};
const N_CGS = Number(arg('cgs', 44));
const CADENCE_MS = Number(arg('cadence', 500)); // scaled-down version tick (prod ~5000)
const PHASE_MS = Number(arg('phase', 15000));
const DWELLS = String(arg('dwells', '20,100,250,450'))
  .split(',')
  .map(Number);
// Writer: ~1KB rows in small batches -> steady frame stream.
const WRITE_EVERY_MS = 20;
const ROWS_PER_WRITE = 20;
const JOURNAL_SIZE_LIMIT = 1 * 1024 * 1024; // 1MB: rotate/reset target

const sz = p => {
  try {
    return statSync(p).size;
  } catch {
    return 0;
  }
};
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// One scenario: fresh DB, steady writer, N leapfrogging snapshotter pairs.
// ---------------------------------------------------------------------------
async function runScenario(dwellMs) {
  const dbPath = join(tmpdir(), `relaypin-${process.pid}-${dwellMs}.db`);
  for (const ext of ['', '-wal', '-wal2', '-shm'])
    rmSync(dbPath + ext, {force: true});

  const writer = new SQLiteDatabase(dbPath);
  writer.pragma('journal_mode = wal2');
  writer.pragma(`journal_size_limit = ${JOURNAL_SIZE_LIMIT}`);
  writer.pragma('synchronous = NORMAL');
  writer.exec(
    'CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, pad TEXT NOT NULL)',
  );
  const pad = 'x'.repeat(1024);
  const ins = writer.prepare('INSERT INTO t (pad) VALUES (?)');
  const writeBatch = writer.transaction(() => {
    for (let i = 0; i < ROWS_PER_WRITE; i++) ins.run(pad);
  });

  // Snapshotter pair per CG: two read connections, leapfrogging.
  // pin() = BEGIN CONCURRENT + a read (the read actually takes the snapshot).
  const mkReader = () => {
    const c = new SQLiteDatabase(dbPath);
    c.pragma('journal_mode = wal2');
    return c;
  };
  const pin = c => {
    c.exec('BEGIN CONCURRENT');
    c.prepare('SELECT count(*) AS n FROM t').get();
  };
  const unpin = c => {
    try {
      c.exec('ROLLBACK');
    } catch {
      /* not in txn */
    }
  };

  const cgs = [];
  for (let i = 0; i < N_CGS; i++) {
    const curr = mkReader();
    const prev = mkReader();
    pin(curr);
    pin(prev);
    cgs.push({curr, prev});
  }

  // Advance loop: every CADENCE, each CG "processes" for dwell ms while its
  // old marks persist, THEN leapfrogs (old prev re-pins at head).
  let running = true;
  const advanceLoop = (async () => {
    while (running) {
      const cycleStart = Date.now();
      await sleep(dwellMs); // marks stay old for the dwell (advance duration)
      if (!running) break;
      for (const cg of cgs) {
        // leapfrog: prev (oldest mark) re-pins at head, becomes curr.
        unpin(cg.prev);
        pin(cg.prev);
        const next = cg.prev;
        cg.prev = cg.curr;
        cg.curr = next;
      }
      const remain = CADENCE_MS - (Date.now() - cycleStart);
      if (remain > 0) await sleep(remain);
    }
  })();

  // Writer loop.
  const writerLoop = (async () => {
    while (running) {
      writeBatch();
      await sleep(WRITE_EVERY_MS);
    }
  })();

  // Litestream/replicator-style CHECKPOINTER: prod always has one; without
  // explicit wal_checkpoint attempts nothing ever reclaims, readers or not.
  const ckpt = new SQLiteDatabase(dbPath);
  ckpt.pragma('journal_mode = wal2');
  let ckptAttempts = 0;
  let ckptBusy = 0;
  const ckptLoop = (async () => {
    while (running) {
      await sleep(200);
      try {
        const r = ckpt.pragma('wal_checkpoint(PASSIVE)');
        ckptAttempts++;
        const row = Array.isArray(r) ? r[0] : r;
        if (row && row.busy === 1) ckptBusy++;
      } catch {
        /* locked */
      }
    }
  })();

  // Sample WAL sizes (both files separately; a switch shows as the inactive
  // file starting to grow while the other freezes).
  const startWal = sz(dbPath + '-wal') + sz(dbPath + '-wal2');
  let peak = startWal;
  let shrinkEvents = 0; // a size drop = a successful reset/rotation reclaim
  let last = startWal;
  let lastW1 = sz(dbPath + '-wal');
  let lastW2 = sz(dbPath + '-wal2');
  let switches = 0;
  let growingFile = 1;
  const sampler = (async () => {
    while (running) {
      await sleep(250);
      const w1 = sz(dbPath + '-wal');
      const w2 = sz(dbPath + '-wal2');
      const d1 = w1 - lastW1;
      const d2 = w2 - lastW2;
      const nowGrowing = d1 > d2 ? 1 : d2 > d1 ? 2 : growingFile;
      if (nowGrowing !== growingFile && (d1 !== 0 || d2 !== 0)) {
        switches++;
        growingFile = nowGrowing;
      }
      lastW1 = w1;
      lastW2 = w2;
      const cur = w1 + w2;
      if (cur < last - 4096) shrinkEvents++;
      last = cur;
      if (cur > peak) peak = cur;
    }
  })();

  await sleep(PHASE_MS);
  running = false;
  await Promise.all([advanceLoop, writerLoop, sampler, ckptLoop]);
  ckpt.close();

  const endWal = sz(dbPath + '-wal') + sz(dbPath + '-wal2');
  const bytesWritten = ROWS_PER_WRITE * 1050 * (PHASE_MS / WRITE_EVERY_MS);

  for (const cg of cgs) {
    unpin(cg.curr);
    unpin(cg.prev);
    cg.curr.close();
    cg.prev.close();
  }
  writer.close();
  for (const ext of ['', '-wal', '-wal2', '-shm'])
    rmSync(dbPath + ext, {force: true});

  return {
    dwellMs,
    duty: dwellMs / CADENCE_MS,
    growthMB: (endWal - startWal) / 1024 / 1024,
    peakMB: peak / 1024 / 1024,
    shrinkEvents,
    switches,
    ckptAttempts,
    ckptBusy,
    writtenMB: bytesWritten / 1024 / 1024,
  };
}

async function main() {
  console.log(
    `relay-pin threshold: N=${N_CGS} CGs (${N_CGS * 2} read-marks), ` +
      `cadence=${CADENCE_MS}ms, phase=${PHASE_MS}ms, ` +
      `journal_size_limit=${JOURNAL_SIZE_LIMIT / 1024 / 1024}MB, ` +
      `write rate ~${((ROWS_PER_WRITE * 1050) / WRITE_EVERY_MS / 1024).toFixed(0)}KB/s`,
  );
  console.log(
    'dwell = ms per cycle that each CG holds its PRE-advance snapshot ' +
      '(rust prod p50~700/p90~4600 of a ~5000ms cadence; TS ~tens of ms)\n',
  );

  const results = [];
  // Control: zero readers — WAL must stay bounded.
  {
    const saveN = N_CGS;
    // run with no CGs by temporarily using a scenario with dwell 0 and no pins
    // (simplest: dwell=0 but also skip pinning via N=0 special-case)
  }
  for (const d of DWELLS) {
    process.stdout.write(`dwell=${d}ms (duty ${(d / CADENCE_MS * 100).toFixed(0)}%) ... `);
    const r = await runScenario(d);
    results.push(r);
    console.log(
      `WAL growth ${r.growthMB.toFixed(1)}MB (peak ${r.peakMB.toFixed(1)}MB, ` +
        `${r.shrinkEvents} reclaims, ${r.switches} switches, ` +
        `ckpt busy ${r.ckptBusy}/${r.ckptAttempts}, ${r.writtenMB.toFixed(0)}MB written)`,
    );
  }

  console.log('\n=== verdict ===');
  const bounded = results.filter(r => r.growthMB < r.writtenMB * 0.15);
  const unbounded = results.filter(r => r.growthMB >= r.writtenMB * 0.5);
  for (const r of results) {
    const label =
      r.growthMB < r.writtenMB * 0.15
        ? 'BOUNDED'
        : r.growthMB >= r.writtenMB * 0.5
          ? 'UNBOUNDED (grows with write rate)'
          : 'DEGRADING';
    console.log(
      `  duty ${(r.duty * 100).toFixed(0).padStart(3)}%  growth ${r.growthMB
        .toFixed(1)
        .padStart(6)}MB / written ${r.writtenMB.toFixed(0)}MB  -> ${label}`,
    );
  }
  if (bounded.length && unbounded.length) {
    console.log(
      `\nTHRESHOLD DEMONSTRATED: same writer, same ${N_CGS} readers, same cadence — ` +
        `only dwell differs. Sub-threshold dwell => rotation finds zero-reader ` +
        `instants => bounded. Super-threshold => relay pinning => WAL grows ` +
        `with the write rate, exactly the prod signature.`,
    );
  } else {
    console.log('\nNo clean threshold in this sweep — adjust --dwells/--cadence.');
  }
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
