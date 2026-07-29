#!/usr/bin/env node
/**
 * Local NAPI addon test — no Docker, no ART, no tsx needed.
 *
 * Creates a regular SQLite database with test data, loads the Rust IVM
 * NAPI addon, and exercises the full pipeline:
 *   init → setDatabasePath → addQueries → advance
 *
 * Usage:
 *   node rust-ivm/scripts/test-napi.mjs
 *   node rust-ivm/scripts/test-napi.mjs --wal2  # also test WAL2 mode
 *
 * Exits 0 on success, 1 on failure.
 */

import { createRequire } from 'node:module';
import { DatabaseSync as Database } from 'node:sqlite';
import { writeFileSync, unlinkSync, existsSync } from 'node:fs';
import { join } from 'node:path';

const require = createRequire(import.meta.url);

const TEST_DB = join(import.meta.dirname, 'test-napi.db');
const WAL2_DB = join(import.meta.dirname, 'test-napi-wal2.db');
const ADDON_PATH = join(import.meta.dirname, '..', 'napi', 'rust-ivm.node');

let passed = 0;
let failed = 0;

function assert(condition, msg) {
  if (condition) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg}`);
  }
}

function assertEqual(actual, expected, msg) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg}`);
    console.error(`    expected: ${e}`);
    console.error(`    actual:   ${a}`);
  }
}

function section(name) {
  console.log(`\n=== ${name} ===`);
}

// ---------------------------------------------------------------------------
// 1. Create test SQLite database with regular WAL mode
// ---------------------------------------------------------------------------
function createTestDB(path, walMode = 'wal') {
  if (existsSync(path)) unlinkSync(path);
  for (const ext of ['-wal', '-shm', '-journal']) {
    if (existsSync(path + ext)) unlinkSync(path + ext);
  }

  const db = new Database(path);
  db.exec(`PRAGMA journal_mode = ${walMode}`);
  db.exec(`PRAGMA synchronous = NORMAL`);

  // Create tables
  db.exec(`
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      age INTEGER,
      active INTEGER DEFAULT 1
    );
    CREATE TABLE posts (
      id INTEGER PRIMARY KEY,
      userId INTEGER NOT NULL,
      title TEXT NOT NULL,
      body TEXT,
      createdAt TEXT DEFAULT (datetime('now'))
    );
  `);

  // Insert test data
  const insertUser = db.prepare('INSERT INTO users (id, name, email, age, active) VALUES (?, ?, ?, ?, ?)');
  insertUser.run(1, 'Alice', 'alice@example.com', 30, 1);
  insertUser.run(2, 'Bob', 'bob@example.com', 25, 1);
  insertUser.run(3, 'Charlie', 'charlie@example.com', 35, 0);
  insertUser.run(4, 'Diana', 'diana@example.com', 28, 1);
  insertUser.run(5, 'Eve', 'eve@example.com', 40, 1);

  const insertPost = db.prepare('INSERT INTO posts (id, userId, title, body) VALUES (?, ?, ?, ?)');
  insertPost.run(1, 1, 'Hello World', 'My first post');
  insertPost.run(2, 1, 'Second Post', 'Another post');
  insertPost.run(3, 2, 'Bob Post', 'Bob content');
  insertPost.run(4, 3, 'Charlie Post', 'Charlie content');
  insertPost.run(5, 1, 'Third Post', 'Alice third');

  db.close();
  console.log(`  Created ${path} with ${walMode} mode, 5 users, 5 posts`);
}

// ---------------------------------------------------------------------------
// 2. Load NAPI addon
// ---------------------------------------------------------------------------
function loadAddon() {
  if (!existsSync(ADDON_PATH)) {
    console.error(`  NAPI addon not found at ${ADDON_PATH}`);
    console.error(`  Build it first: cd rust-ivm/napi && napi build --release`);
    return null;
  }

  try {
    const addon = require(ADDON_PATH);
    const className = addon.RustIvmEngine ? 'RustIvmEngine' : (addon.RustIvmEngine ? 'RustIVMEngine' : null);
    assert(className, 'RustIVMEngine or RustIvmEngine export exists');
  console.log(`  Loaded NAPI addon from ${ADDON_PATH}, export: ${className}`);
  return addon;
  } catch (e) {
    console.error(`  Failed to load NAPI addon: ${e.message}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// 3. Test basic engine operations
// ---------------------------------------------------------------------------
function testBasicEngine(addon) {
  section('Basic Engine');

  const engine = new addon.RustIvmEngine();
  assert(engine, 'Engine created');

  // Test ping
  const ping = engine.ping();
  assertEqual(ping, 'pong', 'ping() returns "pong"');

  // Test version
  const ver = engine.version();
  assert(ver.version, 'version() returns version string');
  assert(typeof ver.protocolRev === 'number', 'version() returns protocolRev');

  // Test initialized (should be false before init)
  const initBefore = engine.initialized();
  assertEqual(initBefore, false, 'initialized() false before init');
}

// ---------------------------------------------------------------------------
// 4. Test init with table schemas
// ---------------------------------------------------------------------------
function testInit(addon) {
  section('Init');

  const engine = new addon.RustIvmEngine();
  engine.init({
    users: {
      columns: {
        id: { type: 'number', optional: false },
        name: { type: 'string', optional: false },
        email: { type: 'string', optional: false },
        age: { type: 'number', optional: false },
        active: { type: 'number', optional: false },
      },
      primaryKey: ['id'],
    },
    posts: {
      columns: {
        id: { type: 'number', optional: false },
        userId: { type: 'number', optional: false },
        title: { type: 'string', optional: false },
        body: { type: 'string', optional: false },
        createdAt: { type: 'string', optional: false },
      },
      primaryKey: ['id'],
    },
  });

  const initAfter = engine.initialized();
  assertEqual(initAfter, true, 'initialized() true after init');
  console.log('  init() succeeded with 2 tables');
}

// ---------------------------------------------------------------------------
// 5. Test setDatabasePath + addQueries (the critical path)
// ---------------------------------------------------------------------------
function testSetDatabasePathAndQuery(addon, dbPath) {
  section(`setDatabasePath + addQueries (${dbPath})`);

  const engine = new addon.RustIvmEngine();
  engine.init({
    users: {
      columns: {
        id: { type: 'number', optional: false },
        name: { type: 'string', optional: false },
        email: { type: 'string', optional: false },
        age: { type: 'number', optional: false },
        active: { type: 'number', optional: false },
      },
      primaryKey: ['id'],
    },
    posts: {
      columns: {
        id: { type: 'number', optional: false },
        userId: { type: 'number', optional: false },
        title: { type: 'string', optional: false },
        body: { type: 'string', optional: false },
        createdAt: { type: 'string', optional: false },
      },
      primaryKey: ['id'],
    },
  });

  // Set the database path — this is where WAL2 issues show up
  let dbError = null;
  try {
    engine.setDatabasePath(dbPath);
    console.log('  setDatabasePath() succeeded');
  } catch (e) {
    dbError = e;
    console.error(`  setDatabasePath() FAILED: ${e.message}`);
  }
  assert(!dbError, 'setDatabasePath() does not throw');

  // Simple query: SELECT * FROM users ORDER BY id
  const ast = {
    table: 'users',
    orderBy: [['id', 'asc']],
  };

  let results = null;
  try {
    results = engine.addQueries([{ queryId: 'q1', ast }]);
    console.log(`  addQueries() returned ${results?.length} result(s)`);
  } catch (e) {
    console.error(`  addQueries() FAILED: ${e.message}`);
  }
  assert(results && results.length === 1, 'addQueries returns 1 result');

  if (results && results[0]) {
    const changes = results[0].changes;
    console.log(`  Query q1 returned ${changes.length} row changes`);
    assert(changes.length === 5, `Query returns 5 rows (got ${changes.length})`);

    // Check first row
    if (changes.length > 0) {
      const first = changes[0];
      assertEqual(first.changeType, 0, 'First change is add (0)');
      assertEqual(first.queryId, 'q1', 'First change queryId is q1');
      assertEqual(first.table, 'users', 'First change table is users');

      // Check row data
      const row = first.row;
      if (row) {
        assertEqual(row.id?.f64Val, 1, 'First row id = 1');
        assertEqual(row.name?.strVal, 'Alice', 'First row name = Alice');
        assertEqual(row.email?.strVal, 'alice@example.com', 'First row email');
      }
    }
  }

  // Test with WHERE clause
  section('Query with WHERE');

  const astWithWhere = {
    table: 'users',
    where: {
      type: 'simple',
      op: '=',
      left: { type: 'column', name: 'active' },
      right: { type: 'literal', value: 1 },
    },
    orderBy: [['id', 'asc']],
  };

  let whereResults = null;
  try {
    whereResults = engine.addQueries([{ queryId: 'q2', ast: astWithWhere }]);
  } catch (e) {
    console.error(`  addQueries with WHERE FAILED: ${e.message}`);
  }
  assert(whereResults && whereResults[0], 'WHERE query returns results');
  if (whereResults && whereResults[0]) {
    const activeCount = whereResults[0].changes.length;
    console.log(`  Active users query returned ${activeCount} rows`);
    assertEqual(activeCount, 4, '4 active users (Alice, Bob, Diana, Eve)');
  }

  // Test with LIMIT
  section('Query with LIMIT');

  const astWithLimit = {
    table: 'users',
    orderBy: [['id', 'asc']],
    limit: 2,
  };

  let limitResults = null;
  try {
    limitResults = engine.addQueries([{ queryId: 'q3', ast: astWithLimit }]);
  } catch (e) {
    console.error(`  addQueries with LIMIT FAILED: ${e.message}`);
  }
  assert(limitResults && limitResults[0], 'LIMIT query returns results');
  if (limitResults && limitResults[0]) {
    const limitCount = limitResults[0].changes.length;
    console.log(`  LIMIT 2 query returned ${limitCount} rows`);
    assertEqual(limitCount, 2, 'LIMIT returns 2 rows');
  }

  // Test row set signature
  section('Row Set Signature');

  const sig = engine.rowSetSignature('q3');
  assert(sig !== null && sig !== undefined, 'rowSetSignature returns value');
  console.log(`  Signature for q3: ${sig}`);

  // Test queries() listing
  section('Query Listing');

  engine.removeQuery('q1');
  engine.removeQuery('q2');
  engine.removeQuery('q3');
  const queries = engine.queries();
  assertEqual(queries.length, 0, 'All queries removed');

  // Test getRow — returns null in SQLite mode (reads in-memory, which is empty)
  section('Get Row');
  engine.addQueries([{ queryId: 'q4', ast: { table: 'users', orderBy: [['id', 'asc']] } }]);
  const row = engine.getRow('users', { id: { kind: 'f64', f64Val: 1 } });
  console.log(`  getRow in SQLite mode returned: ${row ? 'row' : 'null'} (expected null — in-memory is empty)`);
  assert(row === null || row === undefined, 'getRow returns null in SQLite mode (in-memory empty)');

  // Test getAllRows
  section('Get All Rows');

  // NOTE: getAllRows reads from in-memory data, which is empty in SQLite mode.
  // This is expected — on-demand fetch means rows are only read via queries.
  const allRows = engine.getAllRows('users');
  console.log(`  getAllRows returned ${allRows.length} rows (expected 0 in SQLite mode)`);
  // In SQLite mode, in-memory data is empty — this is correct behavior.

  engine.destroy();
}

// ---------------------------------------------------------------------------
// 6. Test advance (source changes)
// ---------------------------------------------------------------------------
function testAdvance(addon, dbPath) {
  section('Advance');

  const engine = new addon.RustIvmEngine();
  engine.init({
    users: {
      columns: {
        id: { type: 'number', optional: false },
        name: { type: 'string', optional: false },
        email: { type: 'string', optional: false },
        age: { type: 'number', optional: false },
        active: { type: 'number', optional: false },
      },
      primaryKey: ['id'],
    },
    posts: {
      columns: {
        id: { type: 'number', optional: false },
        userId: { type: 'number', optional: false },
        title: { type: 'string', optional: false },
        body: { type: 'string', optional: false },
        createdAt: { type: 'string', optional: false },
      },
      primaryKey: ['id'],
    },
  });

  engine.setDatabasePath(dbPath);

  // Add a query
  engine.addQueries([{ queryId: 'q1', ast: { table: 'users', orderBy: [['id', 'asc']] } }]);

  // Advance with an ADD change
  let advanceResults = null;
  try {
    advanceResults = engine.advanceWithDiff([
      {
        table: 'users',
        changeType: 'add',
        row: {
          id: { kind: 'f64', f64Val: 6 },
          name: { kind: 'str', strVal: 'Frank' },
          email: { kind: 'str', strVal: 'frank@example.com' },
          age: { kind: 'f64', f64Val: 50 },
          active: { kind: 'f64', f64Val: 1 },
        },
      },
    ]);
  } catch (e) {
    console.error(`  advanceWithDiff FAILED: ${e.message}`);
  }
  assert(advanceResults !== null, 'advanceWithDiff does not throw');
  if (advanceResults) {
    console.log(`  advanceWithDiff returned ${advanceResults.length} changes`);
    // Should produce at least 1 add change for the new user
    assert(advanceResults.length > 0, 'advance returns changes for new user');
  }

  // Advance with an EDIT change
  let editResults = null;
  try {
    editResults = engine.advanceWithDiff([
      {
        table: 'users',
        changeType: 'edit',
        row: {
          id: { kind: 'f64', f64Val: 1 },
          name: { kind: 'str', strVal: 'Alice Updated' },
          email: { kind: 'str', strVal: 'alice@example.com' },
          age: { kind: 'f64', f64Val: 31 },
          active: { kind: 'f64', f64Val: 1 },
        },
        oldRow: {
          id: { kind: 'f64', f64Val: 1 },
          name: { kind: 'str', strVal: 'Alice' },
          email: { kind: 'str', strVal: 'alice@example.com' },
          age: { kind: 'f64', f64Val: 30 },
          active: { kind: 'f64', f64Val: 1 },
        },
      },
    ]);
  } catch (e) {
    console.error(`  advanceWithDiff (edit) FAILED: ${e.message}`);
  }
  assert(editResults !== null, 'advanceWithDiff (edit) does not throw');

  // Advance with a REMOVE change
  let removeResults = null;
  try {
    removeResults = engine.advanceWithDiff([
      {
        table: 'users',
        changeType: 'remove',
        row: {
          id: { kind: 'f64', f64Val: 3 },
          name: { kind: 'str', strVal: 'Charlie' },
          email: { kind: 'str', strVal: 'charlie@example.com' },
          age: { kind: 'f64', f64Val: 35 },
          active: { kind: 'f64', f64Val: 0 },
        },
      },
    ]);
  } catch (e) {
    console.error(`  advanceWithDiff (remove) FAILED: ${e.message}`);
  }
  assert(removeResults !== null, 'advanceWithDiff (remove) does not throw');

  engine.destroy();
}

// ---------------------------------------------------------------------------
// 7. Test error handling
// ---------------------------------------------------------------------------
function testErrorHandling(addon, dbPath) {
  section('Error Handling');

  const engine = new addon.RustIvmEngine();

  // Test setDatabasePath before init (should handle gracefully)
  let threw = false;
  try {
    engine.setDatabasePath('/nonexistent/path/db.sqlite');
  } catch (e) {
    threw = true;
    console.log(`  setDatabasePath with bad path threw: ${e.message}`);
  }
  // This may or may not throw depending on rusqlite behavior — just log it

  // Test init with empty tables (initialized() checks sources, not init() call)
  engine.init({});
  console.log(`  initialized() after init({}): ${engine.initialized()}`);

  // Test querying a nonexistent table — should now return 0 rows (no panic)
  engine.init({
    users: {
      columns: { id: { type: 'number', optional: false }, name: { type: 'string', optional: false } },
      primaryKey: ['id'],
    },
  });
  engine.setDatabasePath(dbPath);

  // Query for a table that was never registered — should return 0 rows
  let badResults = null;
  try {
    badResults = engine.addQueries([{ queryId: 'bad-table', ast: { table: 'nonexistent', orderBy: [['id', 'asc']] } }]);
    assert(badResults && badResults[0].changes.length === 0, 'Query on nonexistent table returns 0 rows (no panic)');
  } catch (e) {
    assert(false, `Query on nonexistent table should not throw: ${e.message}`);
  }

  engine.destroy();
}

// ---------------------------------------------------------------------------
// 8. Test stress (many tables, many queries)
// ---------------------------------------------------------------------------
function testStress(addon, dbPath) {
  section('Stress Test');

  const engine = new addon.RustIvmEngine();

  // Init with many tables — also register users so we can query it
  const tables = {
    users: {
      columns: {
        id: { type: 'number', optional: false },
        name: { type: 'string', optional: false },
        email: { type: 'string', optional: false },
        age: { type: 'number', optional: false },
        active: { type: 'number', optional: false },
      },
      primaryKey: ['id'],
    },
  };
  for (let i = 0; i < 50; i++) {
    tables[`table_${i}`] = {
      columns: {
        id: { type: 'number', optional: false },
        data: { type: 'string', optional: false },
      },
      primaryKey: ['id'],
    };
  }
  engine.init(tables);
  engine.setDatabasePath(dbPath);

  // Add many queries
  const queries = [];
  for (let i = 0; i < 20; i++) {
    queries.push({
      queryId: `stress_q_${i}`,
      ast: { table: 'users', orderBy: [['id', 'asc']] },
    });
  }

  let stressResults = null;
  try {
    stressResults = engine.addQueries(queries);
    console.log(`  Added ${queries.length} queries, got ${stressResults.length} results`);
  } catch (e) {
    console.error(`  Stress test FAILED: ${e.message}`);
  }
  assert(stressResults && stressResults.length === 20, 'Stress: 20 queries return 20 results');

  if (stressResults) {
    for (const r of stressResults) {
      assert(r.changes.length === 5, `Stress: query ${r.queryId} returns 5 rows`);
    }
  }

  // Remove all
  for (let i = 0; i < 20; i++) {
    engine.removeQuery(`stress_q_${i}`);
  }
  assertEqual(engine.queries().length, 0, 'All stress queries removed');

  engine.destroy();
}

// ---------------------------------------------------------------------------
// 9. Test totalHydrationTimeMs
// ---------------------------------------------------------------------------
function testHydrationTime(addon, dbPath) {
  section('Hydration Time');

  const engine = new addon.RustIvmEngine();
  engine.init({
    users: {
      columns: {
        id: { type: 'number', optional: false },
        name: { type: 'string', optional: false },
        email: { type: 'string', optional: false },
        age: { type: 'number', optional: false },
        active: { type: 'number', optional: false },
      },
      primaryKey: ['id'],
    },
  });
  engine.setDatabasePath(dbPath);

  const before = engine.totalHydrationTimeMs();
  assertEqual(before, 0, 'Hydration time 0 before queries');

  engine.addQueries([{ queryId: 'q1', ast: { table: 'users', orderBy: [['id', 'asc']] } }]);

  const after = engine.totalHydrationTimeMs();
  assert(after > 0, `Hydration time > 0 after queries (got ${after}ms)`);

  engine.destroy();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
function main() {
  console.log('Rust IVM NAPI Local Test Suite');
  console.log('================================');
  console.log(`Addon path: ${ADDON_PATH}`);

  // 1. Load addon
  const addon = loadAddon();
  if (!addon) {
    console.error('\nFATAL: Cannot load NAPI addon. Build it first:');
    console.error('  cd rust-ivm/napi && napi build --release');
    process.exit(1);
  }

  // 2. Basic tests
  testBasicEngine(addon);
  testInit(addon);

  // 3. Create test database
  section('Create Test Database');
  createTestDB(TEST_DB, 'wal');

  // 4. Full pipeline tests with WAL database
  testSetDatabasePathAndQuery(addon, TEST_DB);
  testAdvance(addon, TEST_DB);
  testErrorHandling(addon, TEST_DB);
  testStress(addon, TEST_DB);
  testHydrationTime(addon, TEST_DB);

  // 5. Test with WAL2 mode if requested
  if (process.argv.includes('--wal2')) {
    section('WAL2 Mode Test');
    try {
      createTestDB(WAL2_DB, 'wal2');
      testSetDatabasePathAndQuery(addon, WAL2_DB);
    } catch (e) {
      console.log(`  WAL2 mode test skipped: ${e.message}`);
      console.log('  (WAL2 requires rocicorp-patched SQLite — only available in Docker)');
    }
  }

  // 6. Summary
  console.log('\n================================');
  console.log(`Results: ${passed} passed, ${failed} failed`);
  console.log('================================');

  // Cleanup
  for (const f of [TEST_DB, WAL2_DB]) {
    if (existsSync(f)) unlinkSync(f);
    for (const ext of ['-wal', '-shm', '-journal']) {
      if (existsSync(f + ext)) unlinkSync(f + ext);
    }
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
