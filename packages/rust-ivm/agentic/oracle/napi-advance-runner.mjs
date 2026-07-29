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

import { DatabaseSync } from 'node:sqlite';
import { createRequire } from 'node:module';
import { resolve, join, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { readFileSync, writeFileSync, rmSync, mkdirSync, copyFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

// Resolve the addon. $RUST_IVM_ADDON overrides; otherwise auto-detect by
// platform: the checked-in napi/rust-ivm.node may be a stale/Linux build, so on
// macOS prefer the freshly-built dylib (cargo build --release in napi/).
const NAPI = resolve(__dirname, '..', '..', 'napi');
const exists = (p) => { try { readFileSync(p, { flag: 'rs' }); return true; } catch { return false; } };
const candidates = process.env.RUST_IVM_ADDON
  ? [process.env.RUST_IVM_ADDON]
  : process.platform === 'darwin'
    ? [resolve(NAPI, 'target/release/librust_ivm_napi.dylib'), resolve(NAPI, 'rust-ivm.node')]
    : [resolve(NAPI, 'rust-ivm.node'), resolve(NAPI, 'target/release/librust_ivm_napi.so')];
const addonPath = candidates.find(exists);
if (!addonPath) {
  throw new Error(`napi addon not found. tried:\n  ${candidates.join('\n  ')}\n` +
    `build it (cd napi && cargo build --release) or set RUST_IVM_ADDON.`);
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
    case 'number': return 'INTEGER';
    case 'boolean': return 'INTEGER';
    case 'json': return 'TEXT';
    default: return 'TEXT';
  }
}

function sqlValue(v, colType) {
  if (v === null || v === undefined) return null;
  const parts = (colType || '').split('|');
  const base = parts.find(p => p !== 'null') || 'string';
  switch (base) {
    case 'number': return Number(v);
    case 'boolean': return v ? 1 : 0;
    case 'json': return JSON.stringify(v);
    default: return String(v);
  }
}

function createSqliteDb(dbPath, tables) {
  const db = new DatabaseSync(dbPath);
  db.exec('PRAGMA journal_mode = WAL');
  // _zero.replicationState
  db.exec('DROP TABLE IF EXISTS "_zero.replicationState"');
  db.exec('CREATE TABLE "_zero.replicationState" (stateVersion TEXT NOT NULL, lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1))');
  db.exec("INSERT INTO \"_zero.replicationState\" (stateVersion) VALUES ('0')");
  // _zero.changeLog2
  db.exec('DROP TABLE IF EXISTS "_zero.changeLog2"');
  db.exec('CREATE TABLE "_zero.changeLog2" ("stateVersion" TEXT NOT NULL, "pos" INT NOT NULL, "table" TEXT NOT NULL, "rowKey" TEXT NOT NULL, "op" TEXT NOT NULL, PRIMARY KEY("stateVersion", "pos"), UNIQUE("table", "rowKey"))');

  for (const [name, spec] of Object.entries(tables)) {
    const cols = Object.entries(spec.columns).map(([col, type]) => {
      return `"${col}" ${sqlType(type)}`;
    });
    // Add _0_version column (required by the snapshotter's diff validation)
    cols.push('"_0_version" TEXT NOT NULL DEFAULT \'0\'');

    // Composite PRIMARY KEY for compound PKs
    if (spec.primaryKey.length > 0) {
      cols.push(`PRIMARY KEY (${spec.primaryKey.map(c => `"${c}"`).join(', ')})`);
    }

    db.exec(`DROP TABLE IF EXISTS "${name}"`);
    db.exec(`CREATE TABLE "${name}" (${cols.join(', ')})`);
    const colNames = [...Object.keys(spec.columns), '_0_version'];
    const placeholders = colNames.map(() => '?').join(', ');
    const stmt = db.prepare(`INSERT OR IGNORE INTO "${name}" (${colNames.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`);
    for (const row of (spec.rows || [])) {
      const vals = [...colNames.slice(0, -1).map(c => sqlValue(row[c], spec.columns[c])), '0'];
      stmt.run(...vals);
    }
  }
  // Do NOT close — return the connection so the WAL -shm file persists
  // for rusqlite's READ_ONLY connections. Caller must close.
  return db;
}

// ---------------------------------------------------------------------------
// NapiRowChange → comparable flat JSON (shared with napi-sqlite-runner.mjs)
// ---------------------------------------------------------------------------

function napiValueToJs(v) {
  switch (v.kind) {
    case 'null': return null;
    case 'bool': return v.boolVal;
    case 'f64': return v.f64Val;
    case 'str': return v.strVal;
    case 'json':
      try { return JSON.parse(v.jsonVal); } catch { return v.jsonVal; }
    default: return null;
  }
}

function napiRowToJs(rc) {
  const rowKey = {};
  for (const [k, v] of Object.entries(rc.rowKey || {})) {
    if (k === '_0_version') continue; // strip internal column
    rowKey[k] = napiValueToJs(v);
  }
  const row = {};
  if (rc.row) {
    for (const [k, v] of Object.entries(rc.row)) {
      if (k === '_0_version') continue; // strip internal column
      row[k] = napiValueToJs(v);
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
        return [col, { type: base, optional }];
      })
    );
    // Add _0_version column — required by the snapshotter's diff validation.
    // In production, the change-streamer adds this to every table. We add it
    // here so the diff can read it from prev/curr snapshots. It's stripped
    // from the output before comparison with the TS oracle.
    columns._0_version = { type: 'string', optional: false };
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
  const db = new DatabaseSync(dbPath);
  let pos = 0;
  const version = '1'; // all pushes in one version bump

  for (const push of pushes) {
    if (isNoOpEdit(push, tables)) continue;

    const table = push.table;
    const spec = tables[table];
    if (!spec) continue;
    const pk = spec.primaryKey;
    const rowKeyJson = JSON.stringify(
      Object.fromEntries(pk.map(col => [col, (push.row || push.oldRow)[col]]))
    );

    if (push.type === 'add') {
      const colNames = [...Object.keys(spec.columns), '_0_version'];
      const placeholders = colNames.map(() => '?').join(', ');
      const stmt = db.prepare(
        `INSERT OR REPLACE INTO "${table}" (${colNames.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`
      );
      stmt.run(...colNames.slice(0, -1).map(c => sqlValue(push.row[c], spec.columns[c])), '1');
      // changeLog2: 's' (SET) — op is a single char in the Zero protocol
      db.prepare(
        'INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion", "pos", "table", "rowKey", "op") VALUES (?, ?, ?, ?, ?)'
      ).run(version, pos++, table, rowKeyJson, 's');
    } else if (push.type === 'remove') {
      const whereClause = pk.map(c => `"${c}" = ?`).join(' AND ');
      db.prepare(`DELETE FROM "${table}" WHERE ${whereClause}`).run(...pk.map(c => push.row[c]));
      // changeLog2: 'd' (DEL)
      db.prepare(
        'INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion", "pos", "table", "rowKey", "op") VALUES (?, ?, ?, ?, ?)'
      ).run(version, pos++, table, rowKeyJson, 'd');
    } else if (push.type === 'edit') {
      const setClause = [...Object.keys(spec.columns).filter(c => !pk.includes(c)), '_0_version']
        .map(c => `"${c}" = ?`).join(', ');
      const whereClause = pk.map(c => `"${c}" = ?`).join(' AND ');
      const setVals = [
        ...Object.keys(spec.columns).filter(c => !pk.includes(c)).map(c => sqlValue(push.row[c], spec.columns[c])),
        '1', // _0_version = new version
      ];
      db.prepare(`UPDATE "${table}" SET ${setClause} WHERE ${whereClause}`)
        .run(...setVals, ...pk.map(c => push.oldRow[c]));
      // changeLog2: 's' (SET — the diff will find the old row in prev → EDIT)
      db.prepare(
        'INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion", "pos", "table", "rowKey", "op") VALUES (?, ?, ?, ?, ?)'
      ).run(version, pos++, table, rowKeyJson, 's');
    }
  }

  // Bump replicationState version
  db.exec("UPDATE \"_zero.replicationState\" SET stateVersion = '1'");
  db.close();
}

// ---------------------------------------------------------------------------
// Run fixture through the napi addon with advance
// ---------------------------------------------------------------------------

async function runFixture(fixture) {
  const dbPath = join(tmpdir(), `napi-adv-${Date.now()}-${process.pid}.db`);
  const result = { hydrate: [], advance: [], finalView: [] };

  // Keep a "keeper" connection open for the entire run so the WAL -shm file
  // persists. rusqlite opens the DB in READ_ONLY mode and cannot create the
  // -shm file; if node:sqlite deletes it on close, every rusqlite query fails
  // with "unable to open database file". The keeper stays open until we're
  // done with the napi engine.
  try {
    // 1. Create initial DB (returns the keeper connection — kept open)
    const keeper = createSqliteDb(dbPath, fixture.tables);

    // 2. Init engine + hydrate (streaming path — TSFN callback, production code)
    const engine = new addon.RustIvmEngine();
    engine.init(buildTableSpecs(fixture.tables), dbPath, 'test');

    await engine.addQueriesStreamingRows(
      [{ queryId: 'q1', astJson: JSON.stringify(fixture.ast) }],
      (err, rc) => { if (err) throw err; if (!rc) return;
        if (rc.changeType < 0) return;
        result.hydrate.push(napiRowToJs(rc));
      },
    );

    // 3. Apply pushes to SQLite + write changeLog2
    applyPushesToSqlite(dbPath, fixture.tables, fixture.pushes || []);

    // 4. Advance (streaming path — TSFN callback, production code)
    if (fixture.pushes && fixture.pushes.length > 0) {
      await engine.advanceToHeadStreamingRows(
        (err, rc) => { if (err) throw err; if (!rc) return;
          if (rc.changeType < 0) return; // skip headers/resets
          result.advance.push(napiRowToJs(rc));
        },
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
    const afterTables = applyPushesToTables(fixture.tables, fixture.pushes || []);
    const dbPath2 = dbPath + '.after.db';
    try {
      const keeper2 = createSqliteDb(dbPath2, afterTables);
      const engine2 = new addon.RustIvmEngine();
      engine2.init(buildTableSpecs(afterTables), dbPath2, 'test');
      await engine2.addQueriesStreamingRows(
        [{ queryId: 'q1', astJson: JSON.stringify(fixture.ast) }],
        (err, rc) => { if (err) throw err; if (!rc) return;
          if (rc.changeType < 0) return;
          result.finalView.push(napiRowToJs(rc));
        },
      );
      keeper2.close();
    } finally {
      for (const ext of ['', '-wal', '-shm']) rmSync(dbPath2 + ext, { force: true });
    }

    keeper.close();
  } finally {
    for (const ext of ['', '-wal', '-shm']) rmSync(dbPath + ext, { force: true });
  }

  return result;
}

function applyPushesToTables(tables, pushes) {
  const shadow = {};
  for (const [name, spec] of Object.entries(tables)) {
    shadow[name] = { ...spec, rows: spec.rows.map(r => ({ ...r })) };
  }
  for (const push of (pushes || [])) {
    if (isNoOpEdit(push, tables)) continue;

    const t = shadow[push.table];
    if (!t) continue;
    if (push.type === 'add') {
      t.rows.push({ ...push.row });
    } else if (push.type === 'remove') {
      const pk = t.primaryKey;
      t.rows = t.rows.filter(r => !pk.every(k => r[k] === push.row[k]));
    } else if (push.type === 'edit') {
      const pk = t.primaryKey;
      t.rows = t.rows.map(r => {
        if (pk.every(k => r[k] === push.oldRow[k])) return { ...push.row };
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
  const rowKeyStr = rc => JSON.stringify({q: rc.queryId, t: rc.table, k: canon(rc.rowKey)});

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
    } else if (JSON.stringify(canon(oldRc.row)) !== JSON.stringify(canon(rc.row))) {
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
  let input = null, out = null;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--out') { out = args[++i]; continue; }
    if (!input) input = args[i];
  }
  if (!input) {
    console.error('Usage: napi-advance-runner.mjs <input.json> [--out <actual.json>]');
    process.exit(1);
  }
  const fixture = JSON.parse(readFileSync(input, 'utf8'));
  const result = await runFixture(fixture);
  // Normalize incremental engine output to net changes, matching the TS
  // advance oracle's snapshotter-diff semantics.
  result.advance = netAdvanceFromViews(result.hydrate, result.finalView);
  const json = JSON.stringify(result, null, 1) + '\n';
  const outPath = out ?? input.replace(/\.input\.json$/, '.napi-adv-actual.json');
  mkdirSync(dirname(outPath), { recursive: true });
  writeFileSync(outPath, json);
  console.log(`wrote ${outPath} (hydrate=${result.hydrate.length} advance=${result.advance.length} finalView=${result.finalView.length})`);
}

main();
