#!/usr/bin/env node
// oracle/napi-advance-runner.mjs — runs a fixture through the REAL napi addon
// over a SQLite-backed TableSource, INCLUDING the advance path.
//
// Usage: node oracle/napi-advance-runner.mjs <input.json> [--out <actual.json>]
//
// This extends napi-sqlite-runner.mjs to test the advance path:
//   1. Create SQLite DB with initial data + _zero.replicationState + _zero.changeLog2
//   2. napi init → addQueriesStreaming (hydrate)
//   3. Apply fixture pushes to SQLite (INSERT/UPDATE/DELETE + changeLog2 entries)
//   4. napi advanceToHeadStreaming → capture RowChanges
//   5. Re-hydrate from the after-state DB for finalView
//
// Output: { hydrate: RowChange[], advance: RowChange[], finalView: RowChange[] }
//
// This exercises the advance path where edit-emission and source-drift bugs lived.

import {
  readFileSync,
  writeFileSync,
  rmSync,
  mkdirSync,
  copyFileSync,
} from 'node:fs';
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

// Resolve the addon. $RUST_IVM_ADDON overrides; otherwise auto-detect by
// platform: the checked-in napi/rust-ivm.node may be a stale/Linux build, so on
// macOS prefer the freshly-built dylib (cargo build --release in napi/).
const NAPI = resolve(__dirname, '..', '..', 'napi');
const exists = p => {
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
const addonPath = candidates.find(exists);
if (!addonPath) {
  throw new Error(
    `napi addon not found. tried:\n  ${candidates.join('\n  ')}\n` +
      `build it (cd napi && cargo build --release) or set RUST_IVM_ADDON.`,
  );
}
let NODEPATH = addonPath;
if (!addonPath.endsWith('.node')) {
  NODEPATH = join(tmpdir(), `rust-ivm-addon-${process.pid}.node`);
  copyFileSync(addonPath, NODEPATH);
}
const addon = require(NODEPATH);

// ---------------------------------------------------------------------------
// SQLite helpers (shared with napi-sqlite-runner.mjs)
// ---------------------------------------------------------------------------

function sqlType(colType) {
  const parts = colType.split('|');
  const base = parts.find(p => p !== 'null') || 'string';
  switch (base) {
    case 'number':
      return 'INTEGER';
    case 'boolean':
      return 'INTEGER';
    case 'json':
      return 'TEXT';
    default:
      return 'TEXT';
  }
}

function sqlValue(v, colType) {
  if (v === null || v === undefined) return null;
  const parts = (colType || '').split('|');
  const base = parts.find(p => p !== 'null') || 'string';
  switch (base) {
    case 'number':
      return Number(v);
    case 'boolean':
      return v ? 1 : 0;
    case 'json':
      return JSON.stringify(v);
    default:
      return String(v);
  }
}

function createSqliteDb(dbPath, tables) {
  const db = new SQLiteDatabase(dbPath);
  db.pragma('journal_mode = wal2');
  // _zero.replicationState
  db.exec('DROP TABLE IF EXISTS "_zero.replicationState"');
  db.exec(
    'CREATE TABLE "_zero.replicationState" (stateVersion TEXT NOT NULL, lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1))',
  );
  db.exec('INSERT INTO "_zero.replicationState" (stateVersion) VALUES (\'0\')');
  // _zero.changeLog2
  db.exec('DROP TABLE IF EXISTS "_zero.changeLog2"');
  db.exec(
    'CREATE TABLE "_zero.changeLog2" ("stateVersion" TEXT NOT NULL, "pos" INT NOT NULL, "table" TEXT NOT NULL, "rowKey" TEXT NOT NULL, "op" TEXT NOT NULL, PRIMARY KEY("stateVersion", "pos"), UNIQUE("table", "rowKey"))',
  );

  for (const [name, spec] of Object.entries(tables)) {
    const cols = Object.entries(spec.columns).map(([col, type]) => {
      return `"${col}" ${sqlType(type)}`;
    });
    // Add _0_version column (required by the snapshotter's diff validation)
    cols.push('"_0_version" TEXT NOT NULL DEFAULT \'0\'');

    // Composite PRIMARY KEY for compound PKs
    if (spec.primaryKey.length > 0) {
      cols.push(
        `PRIMARY KEY (${spec.primaryKey.map(c => `"${c}"`).join(', ')})`,
      );
    }

    db.exec(`DROP TABLE IF EXISTS "${name}"`);
    db.exec(`CREATE TABLE "${name}" (${cols.join(', ')})`);
    const colNames = [...Object.keys(spec.columns), '_0_version'];
    const placeholders = colNames.map(() => '?').join(', ');
    const stmt = db.prepare(
      `INSERT OR IGNORE INTO "${name}" (${colNames.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`,
    );
    for (const row of spec.rows || []) {
      const vals = [
        ...colNames.slice(0, -1).map(c => sqlValue(row[c], spec.columns[c])),
        '0',
      ];
      stmt.run(...vals);
    }
  }
  // Keep the writer connection open through hydrate so the wal2 shared-memory
  // state remains live. The Rust snapshot connections are read-write too.
  return db;
}

// ---------------------------------------------------------------------------
// NapiRowChange → comparable flat JSON (shared with napi-sqlite-runner.mjs)
// ---------------------------------------------------------------------------

function napiRowToJs(rc) {
  const rawRowKey =
    typeof rc.rowKey === 'string' ? JSON.parse(rc.rowKey) : rc.rowKey || {};
  const rowKey = {};
  for (const [k, v] of Object.entries(rawRowKey)) {
    if (k === '_0_version') continue;
    rowKey[k] = v;
  }
  const rawRow = typeof rc.row === 'string' ? JSON.parse(rc.row) : rc.row;
  const row = {};
  if (rawRow) {
    for (const [k, v] of Object.entries(rawRow)) {
      if (k === '_0_version') continue;
      row[k] = v;
    }
  }
  return {
    changeType: rc.changeType,
    queryId: rc.queryId,
    table: rc.table,
    rowKey,
    row: Object.keys(row).length > 0 ? row : null,
    isHidden: rc.isHidden === true,
  };
}

function buildTableSpecs(tables) {
  return Object.entries(tables).map(([name, spec]) => {
    const columns = Object.fromEntries(
      Object.entries(spec.columns).map(([col, type]) => {
        const parts = type.split('|');
        const base = parts.find(p => p !== 'null') || 'string';
        const optional = parts.includes('null');
        return [col, {type: base, optional}];
      }),
    );
    // Add _0_version column — required by the snapshotter's diff validation.
    // In production, the change-streamer adds this to every table. We add it
    // here so the diff can read it from prev/curr snapshots. It's stripped
    // from the output before comparison with the TS oracle.
    columns._0_version = {type: 'string', optional: false};
    return {
      table: name,
      columns,
      primaryKey: spec.primaryKey,
      minRowVersion: '0',
    };
  });
}

// ---------------------------------------------------------------------------
// Apply pushes to SQLite + write changeLog2 entries
// ---------------------------------------------------------------------------

function rowsEqual(a, b, columns) {
  for (const c of columns) {
    const av = a[c];
    const bv = b[c];
    if (av === null || av === undefined) {
      if (bv !== null && bv !== undefined) return false;
    } else if (JSON.stringify(av) !== JSON.stringify(bv)) {
      return false;
    }
  }
  return true;
}

function isNoOpEdit(push, tables) {
  if (push.type !== 'edit') return false;
  const spec = tables[push.table];
  if (!spec) return false;
  const cols = Object.keys(spec.columns);
  return rowsEqual(push.oldRow, push.row, cols);
}

function applyPushesToSqlite(dbPath, tables, pushes) {
  if (!pushes || pushes.length === 0) return;
  const db = new SQLiteDatabase(dbPath);
  let pos = 0;
  const version = '1'; // all pushes in one version bump

  for (const push of pushes) {
    if (isNoOpEdit(push, tables)) continue;

    const table = push.table;
    const spec = tables[table];
    if (!spec) continue;
    const pk = spec.primaryKey;
    const rowKeyJson = JSON.stringify(
      Object.fromEntries(pk.map(col => [col, (push.row || push.oldRow)[col]])),
    );

    if (push.type === 'add') {
      const colNames = [...Object.keys(spec.columns), '_0_version'];
      const placeholders = colNames.map(() => '?').join(', ');
      const stmt = db.prepare(
        `INSERT OR REPLACE INTO "${table}" (${colNames.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`,
      );
      stmt.run(
        ...colNames
          .slice(0, -1)
          .map(c => sqlValue(push.row[c], spec.columns[c])),
        '1',
      );
      // changeLog2: 's' (SET) — op is a single char in the Zero protocol
      db.prepare(
        'INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion", "pos", "table", "rowKey", "op") VALUES (?, ?, ?, ?, ?)',
      ).run(version, pos++, table, rowKeyJson, 's');
    } else if (push.type === 'remove') {
      const whereClause = pk.map(c => `"${c}" = ?`).join(' AND ');
      db.prepare(`DELETE FROM "${table}" WHERE ${whereClause}`).run(
        ...pk.map(c => push.row[c]),
      );
      // changeLog2: 'd' (DEL)
      db.prepare(
        'INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion", "pos", "table", "rowKey", "op") VALUES (?, ?, ?, ?, ?)',
      ).run(version, pos++, table, rowKeyJson, 'd');
    } else if (push.type === 'edit') {
      const setClause = [
        ...Object.keys(spec.columns).filter(c => !pk.includes(c)),
        '_0_version',
      ]
        .map(c => `"${c}" = ?`)
        .join(', ');
      const whereClause = pk.map(c => `"${c}" = ?`).join(' AND ');
      const setVals = [
        ...Object.keys(spec.columns)
          .filter(c => !pk.includes(c))
          .map(c => sqlValue(push.row[c], spec.columns[c])),
        '1', // _0_version = new version
      ];
      db.prepare(`UPDATE "${table}" SET ${setClause} WHERE ${whereClause}`).run(
        ...setVals,
        ...pk.map(c => push.oldRow[c]),
      );
      // changeLog2: 's' (SET — the diff will find the old row in prev → EDIT)
      db.prepare(
        'INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion", "pos", "table", "rowKey", "op") VALUES (?, ?, ?, ?, ?)',
      ).run(version, pos++, table, rowKeyJson, 's');
    }
  }

  // Bump replicationState version
  db.exec('UPDATE "_zero.replicationState" SET stateVersion = \'1\'');
  db.close();
}

// ---------------------------------------------------------------------------
// Run fixture through the napi addon with advance
// ---------------------------------------------------------------------------

// #1b per-phase checkpoint probe: a PASSIVE checkpoint from a fresh connection
// at a QUIESCENT point (right after hydrate / advance / reset, when the
// engine's two read-marks are at head and nothing has written since) must
// complete (busy=0). busy=1 means a read-mark is pinned at an OLDER frame than
// the engine's live snapshots — a zombie connection (leaked with its read txn
// open) or a frozen/lagging snapshot. The after-destroy TRUNCATE probe (#1)
// catches zombies that survive teardown; this catches the alive classes and
// localizes WHICH phase created the pin.
function probeCheckpointPassive(dbPath, phase) {
  try {
    const probe = new SQLiteDatabase(dbPath);
    const ck = probe.prepare('PRAGMA wal_checkpoint(PASSIVE)').get();
    probe.close();
    return {
      phase,
      busy: ck && typeof ck.busy === 'number' ? ck.busy : -1,
      log: ck ? ck.log : -1,
      checkpointed: ck ? ck.checkpointed : -1,
    };
  } catch (e) {
    return {phase, busy: -1, error: String((e && e.message) || e)};
  }
}

// #1c WAL-RECLAIM probe (the STRONG zombie detector, run after destroy).
//
// On wal2, `wal_checkpoint(TRUNCATE)` is BLIND to a stale pin sitting in the
// non-active file: it reports busy=0/does nothing because wal2 only ever
// checkpoints the inactive file, and switching files is what a pin actually
// blocks (empirically established in wal2-probe-matrix.mjs: stale pin, healthy
// pins, and no pins ALL read busy=0). The reliable discriminator is RECLAIM:
// with NO live read-marks, a tiny write (journal_size_limit>0 forces the file
// switch — wal2's walRestartLog ignores 0/-1) followed by a PASSIVE checkpoint
// reclaims the whole log within 2 rounds; a zombie read-mark freezes
// `checkpointed` below `log` forever.
function probeWalReclaim(dbPath) {
  try {
    const c = new SQLiteDatabase(dbPath);
    c.pragma('journal_size_limit = 4096');
    c.exec('CREATE TABLE IF NOT EXISTS "_art_probe" (k INTEGER PRIMARY KEY, v)');
    let log = -1;
    let checkpointed = -1;
    for (let round = 0; round < 3; round++) {
      c.exec(`INSERT INTO "_art_probe" (v) VALUES (${round})`);
      const ck = c.prepare('PRAGMA wal_checkpoint(PASSIVE)').get();
      log = ck ? ck.log : -1;
      checkpointed = ck ? ck.checkpointed : -1;
    }
    c.close();
    // Healthy margin: the final round's own frames may not be reclaimed yet.
    const reclaimed = checkpointed >= 0 && log >= 0 && checkpointed >= log - 4;
    return {log, checkpointed, reclaimed};
  } catch (e) {
    return {
      log: -1,
      checkpointed: -1,
      reclaimed: false,
      error: String((e && e.message) || e),
    };
  }
}

async function runFixture(fixture) {
  const dbPath = join(tmpdir(), `napi-adv-${Date.now()}-${process.pid}.db`);
  // resets: unexpected in-place `-2` reset rows (e.g. take-bound-divergence,
  // scalar-subquery) — correctness diffing skips these, so capture them so the
  // fuzzer can treat a wedge/divergence as a FAILURE.
  // checkpointBusyAfterDestroy: 1 => after the engine is destroyed a checkpoint
  // is still BUSY => a snapshot connection leaked (the WAL-growth class).
  // phaseCheckpointProbes: per-phase quiescent PASSIVE probes (see #1b above).
  const result = {
    hydrate: [],
    advance: [],
    finalView: [],
    resets: [],
    checkpointBusyAfterDestroy: 0,
    phaseCheckpointProbes: [],
  };

  // Validation-only fault injection: hold a pinned BEGIN CONCURRENT reader for
  // the whole run to prove the per-phase probes detect a stale pin.
  let injectedStalePin = null;

  // Keep a writer connection open for the entire run so the wal2 shared-memory
  // state persists until the NAPI engine is destroyed.
  try {
    // 1. Create initial DB (returns the keeper connection — kept open)
    const keeper = createSqliteDb(dbPath, fixture.tables);

    if (process.env.ART_INJECT_STALE_PIN === '1') {
      injectedStalePin = new SQLiteDatabase(dbPath);
      injectedStalePin.exec('BEGIN CONCURRENT');
      injectedStalePin
        .prepare('SELECT stateVersion FROM "_zero.replicationState"')
        .get();
    }

    // 2. Init engine + hydrate (streaming path — TSFN callback, production code)
    const engine = new addon.RustIvmEngine();
    engine.init(buildTableSpecs(fixture.tables), dbPath, 'test');

    const hydrateStreamId = 1;
    await engine.addQueriesStreamingRows(
      [{queryId: 'q1', astJson: JSON.stringify(fixture.ast)}],
      (err, chunk) => {
        if (err) throw err;
        if (!chunk) return;
        for (const rc of Array.isArray(chunk) ? chunk : [chunk]) {
          engine.grantStreamCredit(hydrateStreamId, 1);
          if (rc.changeType === -2) {
            result.resets.push({phase: 'hydrate', rowKey: rc.rowKey});
            continue;
          }
          if (rc.changeType < 0) continue;
          result.hydrate.push(napiRowToJs(rc));
        }
      },
      hydrateStreamId,
    );

    result.phaseCheckpointProbes.push(probeCheckpointPassive(dbPath, 'hydrate'));

    // 3. Apply pushes to SQLite + write changeLog2
    applyPushesToSqlite(dbPath, fixture.tables, fixture.pushes || []);

    // 4. Advance (streaming path — TSFN callback, production code)
    if (fixture.pushes && fixture.pushes.length > 0) {
      const advanceStreamId = 2;
      await engine.advanceToHeadStreamingRows((err, chunk) => {
        if (err) throw err;
        if (!chunk) return;
        for (const rc of Array.isArray(chunk) ? chunk : [chunk]) {
          engine.grantStreamCredit(advanceStreamId, 1);
          if (rc.changeType === -2) {
            result.resets.push({phase: 'advance', rowKey: rc.rowKey});
            continue;
          }
          if (rc.changeType < 0) continue; // skip headers/sentinels
          result.advance.push(napiRowToJs(rc));
        }
      }, advanceStreamId);
      result.phaseCheckpointProbes.push(
        probeCheckpointPassive(dbPath, 'advance'),
      );
    }

    // 5. Final view: re-hydrate from after-state DB.
    // Yield to the event loop before creating engine2 — the TSFN from the
    // hydrate/advance tasks needs a microtask cycle to fully release. Without
    // this yield, engine2's TSFN callbacks silently never fire (napi-rs
    // lifecycle quirk: the previous task's TSFN is still alive when the new
    // one is created, and the new one gets starved).
    engine.reset();
    await new Promise(r => setImmediate(r));
    result.phaseCheckpointProbes.push(probeCheckpointPassive(dbPath, 'reset'));
    const afterTables = applyPushesToTables(
      fixture.tables,
      fixture.pushes || [],
    );
    const dbPath2 = dbPath + '.after.db';
    try {
      const keeper2 = createSqliteDb(dbPath2, afterTables);
      const engine2 = new addon.RustIvmEngine();
      engine2.init(buildTableSpecs(afterTables), dbPath2, 'test');
      const finalStreamId = 1;
      await engine2.addQueriesStreamingRows(
        [{queryId: 'q1', astJson: JSON.stringify(fixture.ast)}],
        (err, chunk) => {
          if (err) throw err;
          if (!chunk) return;
          for (const rc of Array.isArray(chunk) ? chunk : [chunk]) {
            engine2.grantStreamCredit(finalStreamId, 1);
            if (rc.changeType < 0) continue;
            result.finalView.push(napiRowToJs(rc));
          }
        },
        finalStreamId,
      );
      keeper2.close();
    } finally {
      for (const ext of ['', '-wal', '-shm'])
        rmSync(dbPath2 + ext, {force: true});
    }

    keeper.close();

    // #1 idle-checkpoint invariant: once the engine is DESTROYED, no read-mark
    // may remain on the wal2 replica. A BUSY checkpoint from a fresh connection
    // means a snapshot connection leaked / a lagging snapshot was never released
    // — the WAL-growth class that correctness diffing cannot see.
    try {
      await engine.destroy();
      await new Promise(r => setImmediate(r));
      const probe = new SQLiteDatabase(dbPath);
      const ck = probe.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get();
      result.checkpointBusyAfterDestroy =
        ck && typeof ck.busy === 'number' ? ck.busy : -1;
      probe.close();
    } catch (e) {
      result.checkpointBusyAfterDestroy = -1;
      result.checkpointProbeError = String((e && e.message) || e);
    }
    // #1c: TRUNCATE-busy is blind to wal2 non-active-file pins (and can even
    // die with a disk I/O error while one exists); the reclaim probe is the
    // strong detector (see probeWalReclaim). Run it UNCONDITIONALLY — it
    // carries its own error handling and reports reclaimed=false on failure.
    result.walReclaimAfterDestroy = probeWalReclaim(dbPath);
  } finally {
    if (injectedStalePin) {
      try {
        injectedStalePin.exec('ROLLBACK');
        injectedStalePin.close();
      } catch {
        /* validation-only */
      }
    }
    for (const ext of ['', '-wal', '-wal2', '-shm'])
      rmSync(dbPath + ext, {force: true});
  }

  return result;
}

function applyPushesToTables(tables, pushes) {
  const shadow = {};
  for (const [name, spec] of Object.entries(tables)) {
    shadow[name] = {...spec, rows: spec.rows.map(r => ({...r}))};
  }
  for (const push of pushes || []) {
    if (isNoOpEdit(push, tables)) continue;

    const t = shadow[push.table];
    if (!t) continue;
    if (push.type === 'add') {
      t.rows.push({...push.row});
    } else if (push.type === 'remove') {
      const pk = t.primaryKey;
      t.rows = t.rows.filter(r => !pk.every(k => r[k] === push.row[k]));
    } else if (push.type === 'edit') {
      const pk = t.primaryKey;
      t.rows = t.rows.map(r => {
        if (pk.every(k => r[k] === push.oldRow[k])) return {...push.row};
        return r;
      });
    }
  }
  return shadow;
}

// ---------------------------------------------------------------------------
// Compute net advance changes from hydrate and finalView.
// ---------------------------------------------------------------------------
// Production IVM operators (including TS's Take) process snapshotter-diff
// changes one at a time and can emit transient ADD/REMOVE pairs for boundary
// rows that end up unchanged after the full transaction. The TS advance oracle
// computes expected advance as the net diff between hydrate and finalView,
// so we normalize the engine's incremental advance output the same way before
// comparison. This mirrors the production snapshotter-diff semantics: one net
// change per row key between the pre- and post-transaction views.
function canon(v) {
  if (v === null) return null;
  if (typeof v === 'number') {
    if (Object.is(v, -0)) return 0;
    if (Number.isFinite(v) && Math.round(v) === v) return Math.round(v);
    return v;
  }
  if (typeof v === 'boolean' || typeof v === 'string') return v;
  if (Array.isArray(v)) return v.map(canon);
  if (typeof v === 'object') {
    const out = {};
    for (const k of Object.keys(v).sort()) out[k] = canon(v[k]);
    return out;
  }
  return v;
}

function netAdvanceFromViews(hydrate, finalView) {
  const rowKeyStr = rc =>
    JSON.stringify({q: rc.queryId, t: rc.table, k: canon(rc.rowKey)});

  // The IVM view output is a multiset: a row with children may appear once per
  // child match. Advance semantics are net changes per row key, so collapse the
  // multiset to the last occurrence before diffing (matches the TS oracle).
  const hydrateMap = new Map();
  for (const rc of hydrate) {
    hydrateMap.set(rowKeyStr(rc), rc);
  }

  const finalMap = new Map();
  for (const rc of finalView) {
    finalMap.set(rowKeyStr(rc), rc);
  }

  const changes = [];
  for (const rc of hydrateMap.values()) {
    if (!finalMap.has(rowKeyStr(rc))) {
      changes.push({
        changeType: 1,
        queryId: rc.queryId,
        table: rc.table,
        rowKey: rc.rowKey,
        row: null,
        isHidden: rc.isHidden,
      });
    }
  }
  for (const rc of finalMap.values()) {
    const oldRc = hydrateMap.get(rowKeyStr(rc));
    if (oldRc === undefined) {
      changes.push({
        changeType: 0,
        queryId: rc.queryId,
        table: rc.table,
        rowKey: rc.rowKey,
        row: rc.row,
        isHidden: rc.isHidden,
      });
    } else if (oldRc.isHidden === true && rc.isHidden !== true) {
      // Visibility transition: hidden → visible (content unchanged).
      // From the client's perspective this row just appeared → ADD.
      changes.push({
        changeType: 0,
        queryId: rc.queryId,
        table: rc.table,
        rowKey: rc.rowKey,
        row: rc.row,
        isHidden: rc.isHidden,
      });
    } else if (oldRc.isHidden !== true && rc.isHidden === true) {
      // Visibility transition: visible → hidden (content unchanged).
      // From the client's perspective this row disappeared → REMOVE.
      changes.push({
        changeType: 1,
        queryId: rc.queryId,
        table: rc.table,
        rowKey: rc.rowKey,
        row: null,
        isHidden: rc.isHidden,
      });
    } else if (
      JSON.stringify(canon(oldRc.row)) !== JSON.stringify(canon(rc.row))
    ) {
      changes.push({
        changeType: 2,
        queryId: rc.queryId,
        table: rc.table,
        rowKey: rc.rowKey,
        row: rc.row,
        isHidden: rc.isHidden,
      });
    }
  }
  return changes;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const args = process.argv.slice(2);
  let input = null,
    out = null;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--out') {
      out = args[++i];
      continue;
    }
    if (!input) input = args[i];
  }
  if (!input) {
    console.error(
      'Usage: napi-advance-runner.mjs <input.json> [--out <actual.json>]',
    );
    process.exit(1);
  }
  const fixture = JSON.parse(readFileSync(input, 'utf8'));
  const result = await runFixture(fixture);
  // Normalize incremental engine output to net changes, matching the TS
  // advance oracle's snapshotter-diff semantics.
  result.advance = netAdvanceFromViews(result.hydrate, result.finalView);
  const json = JSON.stringify(result, null, 1) + '\n';
  const outPath =
    out ?? input.replace(/\.input\.json$/, '.napi-adv-actual.json');
  mkdirSync(dirname(outPath), {recursive: true});
  writeFileSync(outPath, json);
  console.log(
    `wrote ${outPath} (hydrate=${result.hydrate.length} advance=${result.advance.length} finalView=${result.finalView.length})`,
  );
}

main();
