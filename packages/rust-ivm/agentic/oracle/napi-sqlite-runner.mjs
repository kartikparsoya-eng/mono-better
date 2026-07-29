#!/usr/bin/env node
// oracle/napi-sqlite-runner.mjs — runs a fixture through the REAL napi addon
// over a SQLite-backed TableSource (the production code path).
//
// Usage: node oracle/napi-sqlite-runner.mjs <input.json> [--out <actual.json>] [--phase hydrate|final|both]
//
// This is the counterpart to ts-runner.mjs. Instead of MemorySource, it:
//   1. Creates a SQLite DB from the fixture's tables/rows
//   2. Calls engine.init() with db_path → creates TableSource instances
//   3. Calls engine.addQueriesStreaming() → drains NapiRowChanges (hydration)
//   4. For "final" phase: applies pushes to SQLite, re-inits, re-hydrates
//   5. Emits a flat RowChange list comparable to the TS oracle's CaughtNode tree
//      (after flattening via the same logic as the Streamer)
//
// This exercises the napi/TableSource boundary where all 3 real bugs lived:
//   - json_to_value (AST literal deserialization — the IN operator bug)
//   - TableSource::fetch (SQLite-backed reads)
//   - value_to_napi (output serialization)
//   - parse_ts_ast / convert_ast (the TS AST adapter)

import { DatabaseSync } from 'node:sqlite';
import { createRequire } from 'node:module';
import { resolve, join, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { readFileSync, writeFileSync, rmSync, mkdirSync, copyFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

// Resolve the addon. $RUST_IVM_ADDON overrides; otherwise auto-detect by
// platform: the checked-in napi/rust-ivm.node is a LINUX build, so on macOS
// prefer the locally-built dylib (cargo build --release in napi/).
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
// napi needs a .node extension to dlopen. Copy to a temp .node if needed.
let NODEPATH = addonPath;
if (!addonPath.endsWith('.node')) {
  NODEPATH = join(tmpdir(), `rust-ivm-addon-${process.pid}.node`);
  copyFileSync(addonPath, NODEPATH);
}
const addon = require(NODEPATH);

// ---------------------------------------------------------------------------
// SQLite DB creation from fixture
// ---------------------------------------------------------------------------

function sqlType(colType) {
  // colType is like "string", "number", "boolean", "number|null", etc.
  const parts = colType.split('|');
  const base = parts.find(p => p !== 'null') || 'string';
  switch (base) {
    case 'number': return 'INTEGER';
    case 'boolean': return 'INTEGER'; // SQLite has no native BOOL
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
    case 'boolean': return v ? 1 : 0; // SQLite boolean as 0/1
    case 'json': return JSON.stringify(v);
    default: return String(v);
  }
}

function createSqliteDb(dbPath, tables) {
  const db = new DatabaseSync(dbPath);
  db.exec(`PRAGMA journal_mode = DELETE`);
  for (const [name, spec] of Object.entries(tables)) {
    const cols = Object.entries(spec.columns).map(([col, type]) => {
      return `"${col}" ${sqlType(type)}`;
    });
    // Composite PRIMARY KEY for compound PKs
    if (spec.primaryKey.length > 0) {
      cols.push(`PRIMARY KEY (${spec.primaryKey.map(c => `"${c}"`).join(', ')})`);
    }
    db.exec(`CREATE TABLE "${name}" (${cols.join(', ')})`);
    const placeholders = Object.keys(spec.columns).map(() => '?').join(', ');
    const colNames = Object.keys(spec.columns);
    const stmt = db.prepare(`INSERT OR IGNORE INTO "${name}" (${colNames.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`);
    for (const row of (spec.rows || [])) {
      const vals = colNames.map(c => sqlValue(row[c], spec.columns[c]));
      stmt.run(...vals);
    }
  }
  db.close();
}

// ---------------------------------------------------------------------------
// NapiRowChange → comparable flat JSON
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
  for (const [k, v] of Object.entries(rc.rowKey || {})) rowKey[k] = napiValueToJs(v);
  const row = {};
  if (rc.row) {
    for (const [k, v] of Object.entries(rc.row)) row[k] = napiValueToJs(v);
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

// ---------------------------------------------------------------------------
// Run fixture through the napi addon
// ---------------------------------------------------------------------------

function buildTableSpecs(tables) {
  return Object.entries(tables).map(([name, spec]) => ({
    table: name,
    columns: Object.fromEntries(
      Object.entries(spec.columns).map(([col, type]) => {
        const parts = type.split('|');
        const base = parts.find(p => p !== 'null') || 'string';
        const optional = parts.includes('null');
        return [col, { type: base, optional }];
      })
    ),
    primaryKey: spec.primaryKey,
  }));
}

async function runHydration(dbPath, tables, ast, queryId = 'q1') {
  const engine = new addon.RustIvmEngine();
  engine.init(buildTableSpecs(tables), dbPath, 'test');
  // Use the streaming path (addQueriesStreamingRows + TSFN callback) — this
  // is the production code path now that RUST_IVM_STREAM_ROWS is default ON.
  const rows = [];
  await engine.addQueriesStreamingRows(
    [{ queryId, astJson: JSON.stringify(ast) }],
    (err, rc) => { if (err) throw err; if (!rc) return;
      if (rc.changeType < 0) return; // skip control rows (headers, resets)
      rows.push(napiRowToJs(rc));
    },
  );
  return rows;
}

function applyPushesToTables(tables, pushes) {
  // Apply fixture pushes to the in-memory table data to produce the "after" state.
  const shadow = {};
  for (const [name, spec] of Object.entries(tables)) {
    shadow[name] = { ...spec, rows: spec.rows.map(r => ({ ...r })) };
  }
  for (const push of (pushes || [])) {
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

async function runFixture(fixture) {
  const phase = process.env.NAPI_DIFF_PHASE || 'both';
  const dbPath = join(tmpdir(), `napi-diff-${Date.now()}-${process.pid}.db`);
  const result = { hydrate: [], finalView: [] };

  try {
    // Phase 1: hydration from initial data
    createSqliteDb(dbPath, fixture.tables);
    result.hydrate = await runHydration(dbPath, fixture.tables, fixture.ast);

    if (phase === 'both' || phase === 'final') {
      // Yield to the event loop before creating a second engine — the TSFN
      // from the first hydrate needs a microtask cycle to fully release.
      // Without this yield, the second engine's TSFN callbacks silently
      // never fire (napi-rs lifecycle quirk).
      await new Promise(r => setImmediate(r));
      // Phase 2: apply pushes, create new DB with after-state, re-hydrate
      const afterTables = applyPushesToTables(fixture.tables, fixture.pushes || []);
      const dbPath2 = dbPath + '.after.db';
      try {
        createSqliteDb(dbPath2, afterTables);
        result.finalView = await runHydration(dbPath2, afterTables, fixture.ast);
      } finally {
        for (const ext of ['', '-wal', '-shm']) rmSync(dbPath2 + ext, { force: true });
      }
    }
  } finally {
    for (const ext of ['', '-wal', '-shm']) rmSync(dbPath + ext, { force: true });
  }

  return result;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const args = argv.slice(2);
  let input = null, out = null;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--out') { out = args[++i]; continue; }
    if (!input) input = args[i];
  }
  if (!input) {
    console.error('Usage: napi-sqlite-runner.mjs <input.json> [--out <actual.json>]');
    process.exit(1);
  }
  return { input, out };
}

async function main() {
  const { input, out } = parseArgs(process.argv);
  const fixture = JSON.parse(readFileSync(input, 'utf8'));
  const result = await runFixture(fixture);
  const json = JSON.stringify(result, null, 1) + '\n';
  const outPath = out ?? input.replace(/\.input\.json$/, '.napi-actual.json');
  mkdirSync(dirname(outPath), { recursive: true });
  writeFileSync(outPath, json);
  console.log(`wrote ${outPath} (hydrate=${result.hydrate.length} finalView=${result.finalView.length})`);
}

main();
