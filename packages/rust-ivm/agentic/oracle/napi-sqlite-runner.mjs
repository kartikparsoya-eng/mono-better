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
// Use the wal2 build of SQLite (@rocicorp/zero-sqlite3), NOT node:sqlite's
// DatabaseSync: the snapshotter hard-requires wal2, and node:sqlite has no wal2
// support, so a DatabaseSync-created replica is rejected ("must be in wal2 mode
// (current: delete)") — which had silently disabled this entire hydrate fuzzer.
const zqliteRequire = createRequire(
  resolve(__dirname, '..', '..', '..', 'zqlite', 'package.json'),
);
const SQLiteDatabase = zqliteRequire('@rocicorp/zero-sqlite3');

// Resolve the addon. $RUST_IVM_ADDON overrides; otherwise auto-detect by
// platform: the checked-in napi/rust-ivm.node is a LINUX build, so on macOS
// prefer the locally-built dylib (cargo build --release in napi/).
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
    case 'number':
      return 'INTEGER';
    case 'boolean':
      return 'INTEGER'; // SQLite has no native BOOL
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
      return v ? 1 : 0; // SQLite boolean as 0/1
    case 'json':
      return JSON.stringify(v);
    default:
      return String(v);
  }
}

// Create a wal2 replica the snapshotter accepts. Mirrors the advance runner:
// wal2 journal mode + _zero.replicationState + _zero.changeLog2 + a _0_version
// column on every table (all required by the snapshotter's diff validation).
// Returns the keeper connection — it MUST stay open through hydration so the
// wal2 shared-memory (-shm) state stays live.
function createSqliteDb(dbPath, tables) {
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
  for (const [name, spec] of Object.entries(tables)) {
    const cols = Object.entries(spec.columns).map(([col, type]) => {
      return `"${col}" ${sqlType(type)}`;
    });
    cols.push('"_0_version" TEXT NOT NULL DEFAULT \'0\'');
    // The SQLite REPLICA is keyed by replicaPrimaryKey (defaults to primaryKey).
    // For PK-divergent tables this differs from the client/engine primaryKey, so
    // the engine must emit rowKeys by the client PK while reading a table whose
    // SQLite PK is different — the exact seam that was untested.
    const replicaPK = spec.replicaPrimaryKey ?? spec.primaryKey;
    if (replicaPK.length > 0) {
      cols.push(`PRIMARY KEY (${replicaPK.map(c => `"${c}"`).join(', ')})`);
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
        ...Object.keys(spec.columns).map(c => sqlValue(row[c], spec.columns[c])),
        '0',
      ];
      stmt.run(...vals);
    }
  }
  return db;
}

// ---------------------------------------------------------------------------
// NapiRowChange → comparable flat JSON
// ---------------------------------------------------------------------------

function napiRowToJs(rc) {
  const rowKey =
    typeof rc.rowKey === 'string' ? JSON.parse(rc.rowKey) : rc.rowKey || {};
  const row = typeof rc.row === 'string' ? JSON.parse(rc.row) : rc.row;
  return {
    changeType: rc.changeType,
    queryId: rc.queryId,
    table: rc.table,
    rowKey,
    row: row || null,
    isHidden: rc.isHidden === true,
  };
}

// ---------------------------------------------------------------------------
// Run fixture through the napi addon
// ---------------------------------------------------------------------------

function buildTableSpecs(tables) {
  return Object.entries(tables).map(([name, spec]) => {
    // Mirror rust-ivm-driver.buildNapiTableSpecs: the engine is keyed by the
    // CLIENT primaryKey (spec.primaryKey), and uniqueKeys carries BOTH the client
    // PK and the replica PK (they drive scalar-EXISTS resolution). A wrong emitted
    // rowKey (keyed by the replica PK instead of the client PK) then diverges from
    // the TS oracle, which keys its MemorySource by spec.primaryKey.
    const clientPK = spec.primaryKey;
    const replicaPK = spec.replicaPrimaryKey ?? spec.primaryKey;
    const uniqueKeys = [clientPK];
    if (JSON.stringify(replicaPK) !== JSON.stringify(clientPK)) {
      uniqueKeys.push(replicaPK);
    }
    return {
      table: name,
      columns: Object.fromEntries(
        Object.entries(spec.columns).map(([col, type]) => {
          const parts = type.split('|');
          const base = parts.find(p => p !== 'null') || 'string';
          const optional = parts.includes('null');
          return [col, {type: base, optional}];
        }),
      ),
      primaryKey: clientPK,
      uniqueKeys,
    };
  });
}

async function runHydration(dbPath, tables, ast, queryId = 'q1') {
  const keeper = createSqliteDb(dbPath, tables);
  // Validation-only fault injection: a pinned BEGIN CONCURRENT reader held past
  // destroy proves the #1c reclaim probe detects a zombie read-mark.
  let injectedStalePin = null;
  if (process.env.ART_INJECT_STALE_PIN === '1') {
    injectedStalePin = new SQLiteDatabase(dbPath);
    injectedStalePin.exec('BEGIN CONCURRENT');
    injectedStalePin
      .prepare('SELECT stateVersion FROM "_zero.replicationState"')
      .get();
  }
  const engine = new addon.RustIvmEngine();
  engine.init(buildTableSpecs(tables), dbPath, 'test');
  // Use the streaming path (addQueriesStreamingRows + TSFN callback), which is
  // the production driver's only hydration path.
  const rows = [];
  const resets = [];
  const streamId = 1;
  await engine.addQueriesStreamingRows(
    [{queryId, astJson: JSON.stringify(ast)}],
    (err, chunk) => {
      if (err) throw err;
      if (!chunk) return;
      // Chunked delivery: each callback carries an ordered array of rows.
      for (const rc of Array.isArray(chunk) ? chunk : [chunk]) {
        engine.grantStreamCredit(streamId, 1);
        if (rc.changeType === -2) {
          // Unexpected in-place reset — capture (correctness diffing skips these).
          resets.push({phase: 'hydrate', rowKey: rc.rowKey});
          continue;
        }
        if (rc.changeType < 0) continue; // skip control rows (headers, sentinels)
        rows.push(napiRowToJs(rc));
      }
    },
    streamId,
  );

  // #1 idle-checkpoint invariant: after the engine is destroyed, a checkpoint
  // from a fresh connection must NOT be BUSY — else a snapshot connection leaked
  // (lagging-snapshot / WAL-growth class).
  // Close the writer BEFORE the checkpoint probe so only the engine's snapshot
  // connections (if leaked) can hold a read-mark.
  keeper.close();
  let checkpointBusy = 0;
  try {
    await engine.destroy();
    await new Promise(r => setImmediate(r));
    const probe = new SQLiteDatabase(dbPath);
    const ck = probe.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get();
    checkpointBusy = ck && typeof ck.busy === 'number' ? ck.busy : -1;
    probe.close();
  } catch {
    checkpointBusy = -1;
  }
  // #1c the STRONG zombie detector: TRUNCATE-busy is blind to a stale pin in
  // the non-active wal2 file (wal2 only checkpoints the inactive file; a pin
  // blocks the SWITCH, not the pragma — empirically wal2-probe-matrix.mjs).
  // With no live read-marks, a write+PASSIVE loop must reclaim the whole log;
  // a zombie freezes `checkpointed` below `log` forever.
  const walReclaim = probeWalReclaim(dbPath);
  if (injectedStalePin) {
    try {
      injectedStalePin.exec('ROLLBACK');
      injectedStalePin.close();
    } catch {
      /* validation-only */
    }
  }
  return {rows, resets, checkpointBusy, walReclaim};
}

function probeWalReclaim(dbPath) {
  try {
    const c = new SQLiteDatabase(dbPath);
    // MUST be >0: wal2's walRestartLog ignores 0/-1 (falls back to 1000 frames)
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

function applyPushesToTables(tables, pushes) {
  // Apply fixture pushes to the in-memory table data to produce the "after" state.
  const shadow = {};
  for (const [name, spec] of Object.entries(tables)) {
    shadow[name] = {...spec, rows: spec.rows.map(r => ({...r}))};
  }
  for (const push of pushes || []) {
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

async function runFixture(fixture) {
  const phase = process.env.NAPI_DIFF_PHASE || 'both';
  const dbPath = join(tmpdir(), `napi-diff-${Date.now()}-${process.pid}.db`);
  const result = {
    hydrate: [],
    finalView: [],
    resets: [],
    checkpointBusyAfterDestroy: 0,
  };
  const absorb = h => {
    result.resets.push(...h.resets);
    if (h.checkpointBusy === 1) {
      result.checkpointBusyAfterDestroy = 1;
    }
    // Worst-case wins: any engine run leaving an unreclaimable wal is a FAIL.
    if (
      h.walReclaim &&
      (!result.walReclaimAfterDestroy ||
        result.walReclaimAfterDestroy.reclaimed !== false)
    ) {
      result.walReclaimAfterDestroy = h.walReclaim;
    }
    return h.rows;
  };

  try {
    // Phase 1: hydration (runHydration builds the wal2 replica + keeper)
    result.hydrate = absorb(
      await runHydration(dbPath, fixture.tables, fixture.ast),
    );

    if (phase === 'both' || phase === 'final') {
      // Yield to the event loop before creating a second engine — the TSFN
      // from the first hydrate needs a microtask cycle to fully release.
      // Without this yield, the second engine's TSFN callbacks silently
      // never fire (napi-rs lifecycle quirk).
      await new Promise(r => setImmediate(r));
      // Phase 2: apply pushes, create new DB with after-state, re-hydrate
      const afterTables = applyPushesToTables(
        fixture.tables,
        fixture.pushes || [],
      );
      const dbPath2 = dbPath + '.after.db';
      try {
        result.finalView = absorb(
          await runHydration(dbPath2, afterTables, fixture.ast),
        );
      } finally {
        for (const ext of ['', '-wal', '-wal2', '-shm'])
          rmSync(dbPath2 + ext, {force: true});
      }
    }
  } finally {
    for (const ext of ['', '-wal', '-wal2', '-shm']) rmSync(dbPath + ext, {force: true});
  }

  return result;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const args = argv.slice(2);
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
      'Usage: napi-sqlite-runner.mjs <input.json> [--out <actual.json>]',
    );
    process.exit(1);
  }
  return {input, out};
}

async function main() {
  const {input, out} = parseArgs(process.argv);
  const fixture = JSON.parse(readFileSync(input, 'utf8'));
  const result = await runFixture(fixture);
  const json = JSON.stringify(result, null, 1) + '\n';
  const outPath = out ?? input.replace(/\.input\.json$/, '.napi-actual.json');
  mkdirSync(dirname(outPath), {recursive: true});
  writeFileSync(outPath, json);
  console.log(
    `wrote ${outPath} (hydrate=${result.hydrate.length} finalView=${result.finalView.length})`,
  );
}

main();
