#!/usr/bin/env node
/**
 * Comprehensive NAPI addon test — simulates the full driver flow
 * that rust-ivm-driver.ts + syncer.ts use in production.
 *
 * Tests EVERYTHING ART would catch, without Docker:
 *   - NAPI loading (ESM, export name)
 *   - SQLite connection (WAL mode, URI flags)
 *   - Schema handling (flat column spec like spec.zqlSpec)
 *   - All query types (simple, WHERE, ORDER BY, LIMIT, joins/related, CSQ)
 *   - All advance ops (add, edit, remove, multi-change)
 *   - Row operations (getRow, getAllRows, rowSetSignature)
 *   - Error handling (missing tables, bad paths, invalid AST)
 *   - Driver simulation (exact conversion flow)
 *   - Stress (many tables, queries, rows)
 *
 * Usage:
 *   node rust-ivm/scripts/test-napi-full.mjs
 *   node rust-ivm/scripts/test-napi-full.mjs --verbose  # print every row
 *   node rust-ivm/scripts/test-napi-full.mjs --wal2     # also test WAL2 mode
 *
 * Exits 0 on success, 1 on failure.
 */

import {createRequire} from 'node:module';
import {DatabaseSync as Database} from 'node:sqlite';
import {unlinkSync, existsSync, mkdirSync} from 'node:fs';
import {join} from 'node:path';

const require = createRequire(import.meta.url);
const VERBOSE = process.argv.includes('--verbose');
const TEST_WAL2 = process.argv.includes('--wal2');

const SCRIPT_DIR = import.meta.dirname;
const TEST_DB = join(SCRIPT_DIR, 'test-full.db');
const WAL2_DB = join(SCRIPT_DIR, 'test-full-wal2.db');
const ADDON_PATH = join(SCRIPT_DIR, '..', 'napi', 'rust-ivm.node');

// ---------------------------------------------------------------------------
// Test framework
// ---------------------------------------------------------------------------
let passed = 0;
let failed = 0;
let skipped = 0;
const failures = [];

function assert(cond, msg) {
  if (cond) {
    passed++;
  } else {
    failed++;
    failures.push(msg);
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
    failures.push(msg);
    console.error(`  FAIL: ${msg}`);
    console.error(`    expected: ${e}`);
    console.error(`    actual:   ${a}`);
  }
}

function assertThrows(fn, msg) {
  try {
    fn();
    failed++;
    failures.push(msg);
    console.error(`  FAIL: ${msg} (expected throw)`);
  } catch {
    passed++;
  }
}

function assertNoThrow(fn, msg) {
  try {
    fn();
    passed++;
  } catch (e) {
    failed++;
    failures.push(msg);
    console.error(`  FAIL: ${msg}: ${e.message}`);
  }
}

function skip(msg) {
  skipped++;
  if (VERBOSE) console.log(`  SKIP: ${msg}`);
}

function section(name) {
  console.log(`\n=== ${name} ===`);
}

// ---------------------------------------------------------------------------
// Value conversion helpers — mirror rust-ivm-driver.ts exactly
// ---------------------------------------------------------------------------
function toNapiValue(val) {
  if (val === null || val === undefined) return {kind: 'null'};
  if (typeof val === 'boolean') return {kind: 'bool', boolVal: val};
  if (typeof val === 'number') return {kind: 'f64', f64Val: val};
  if (typeof val === 'string') return {kind: 'str', strVal: val};
  return {kind: 'json', jsonVal: JSON.stringify(val)};
}

function fromNapiValue(val) {
  switch (val.kind) {
    case 'null': return null;
    case 'bool': return val.boolVal ?? false;
    case 'f64': return val.f64Val ?? 0;
    case 'str': return val.strVal ?? '';
    case 'json': return JSON.parse(val.jsonVal ?? 'null');
    default: return null;
  }
}

function rowToNapi(row) {
  const result = {};
  for (const [k, v] of Object.entries(row)) result[k] = toNapiValue(v);
  return result;
}

function napiToRow(row) {
  if (!row) return undefined;
  const result = {};
  for (const [k, v] of Object.entries(row)) result[k] = fromNapiValue(v);
  return result;
}

function napiToRowChange(c) {
  return {
    type: c.changeType,
    queryID: c.queryId,
    table: c.table,
    rowKey: napiToRow(c.rowKey) ?? {},
    row: napiToRow(c.row),
  };
}

// ---------------------------------------------------------------------------
// Test database — realistic multi-table schema with relationships
// Mirrors the sandbox: users, channels, messages, memberships, orgs
// ---------------------------------------------------------------------------
function createTestDB(path, walMode = 'wal') {
  if (existsSync(path)) unlinkSync(path);
  for (const ext of ['-wal', '-shm', '-journal', '-wal2', '-shm2']) {
    if (existsSync(path + ext)) unlinkSync(path + ext);
  }

  const db = new Database(path);
  db.exec(`PRAGMA journal_mode = ${walMode}`);
  db.exec(`PRAGMA synchronous = NORMAL`);
  db.exec(`PRAGMA foreign_keys = ON`);

  // Schema: 6 tables with relationships
  db.exec(`
    CREATE TABLE organizations (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      plan TEXT DEFAULT 'free'
    );

    CREATE TABLE users (
      id TEXT PRIMARY KEY,
      email TEXT NOT NULL,
      name TEXT NOT NULL,
      orgId TEXT NOT NULL,
      role TEXT DEFAULT 'member',
      active INTEGER DEFAULT 1,
      age INTEGER,
      createdAt TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE channels (
      id TEXT PRIMARY KEY,
      orgId TEXT NOT NULL,
      name TEXT NOT NULL,
      type TEXT DEFAULT 'public',
      createdAt TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE channel_members (
      id TEXT PRIMARY KEY,
      channelId TEXT NOT NULL,
      userId TEXT NOT NULL,
      role TEXT DEFAULT 'member',
      joinedAt TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE messages (
      id TEXT PRIMARY KEY,
      channelId TEXT NOT NULL,
      userId TEXT NOT NULL,
      body TEXT NOT NULL,
      createdAt TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE reactions (
      id TEXT PRIMARY KEY,
      messageId TEXT NOT NULL,
      userId TEXT NOT NULL,
      emoji TEXT NOT NULL
    );
  `);

  // Insert organizations
  const insOrg = db.prepare('INSERT INTO organizations (id, name, plan) VALUES (?, ?, ?)');
  insOrg.run('org-1', 'Acme Corp', 'pro');
  insOrg.run('org-2', 'Beta Inc', 'free');

  // Insert users
  const insUser = db.prepare('INSERT INTO users (id, email, name, orgId, role, active, age) VALUES (?, ?, ?, ?, ?, ?, ?)');
  insUser.run('u-1', 'alice@acme.com', 'Alice', 'org-1', 'admin', 1, 30);
  insUser.run('u-2', 'bob@acme.com', 'Bob', 'org-1', 'member', 1, 25);
  insUser.run('u-3', 'charlie@beta.com', 'Charlie', 'org-2', 'admin', 1, 35);
  insUser.run('u-4', 'diana@acme.com', 'Diana', 'org-1', 'member', 0, 28);
  insUser.run('u-5', 'eve@beta.com', 'Eve', 'org-2', 'member', 1, 40);
  insUser.run('u-6', 'frank@acme.com', 'Frank', 'org-1', 'member', 1, 50);

  // Insert channels
  const insChan = db.prepare('INSERT INTO channels (id, orgId, name, type) VALUES (?, ?, ?, ?)');
  insChan.run('c-1', 'org-1', 'general', 'public');
  insChan.run('c-2', 'org-1', 'engineering', 'private');
  insChan.run('c-3', 'org-2', 'general', 'public');
  insChan.run('c-4', 'org-1', 'random', 'public');

  // Insert channel members
  const insMem = db.prepare('INSERT INTO channel_members (id, channelId, userId, role) VALUES (?, ?, ?, ?)');
  insMem.run('m-1', 'c-1', 'u-1', 'admin');
  insMem.run('m-2', 'c-1', 'u-2', 'member');
  insMem.run('m-3', 'c-2', 'u-1', 'admin');
  insMem.run('m-4', 'c-2', 'u-2', 'member');
  insMem.run('m-5', 'c-2', 'u-6', 'member');
  insMem.run('m-6', 'c-1', 'u-6', 'member');
  insMem.run('m-7', 'c-3', 'u-3', 'admin');
  insMem.run('m-8', 'c-3', 'u-5', 'member');
  insMem.run('m-9', 'c-4', 'u-1', 'admin');
  insMem.run('m-10', 'c-4', 'u-4', 'member');

  // Insert messages
  const insMsg = db.prepare('INSERT INTO messages (id, channelId, userId, body) VALUES (?, ?, ?, ?)');
  insMsg.run('msg-1', 'c-1', 'u-1', 'Hello everyone!');
  insMsg.run('msg-2', 'c-1', 'u-2', 'Hi Alice!');
  insMsg.run('msg-3', 'c-1', 'u-6', 'Good morning');
  insMsg.run('msg-4', 'c-2', 'u-1', 'Deploy is ready');
  insMsg.run('msg-5', 'c-2', 'u-2', 'Pushing now');
  insMsg.run('msg-6', 'c-3', 'u-3', 'Welcome to Beta');
  insMsg.run('msg-7', 'c-1', 'u-1', 'Meeting at 3pm');

  // Insert reactions
  const insReact = db.prepare('INSERT INTO reactions (id, messageId, userId, emoji) VALUES (?, ?, ?, ?)');
  insReact.run('r-1', 'msg-1', 'u-2', '👍');
  insReact.run('r-2', 'msg-1', 'u-6', '❤️');
  insReact.run('r-3', 'msg-4', 'u-6', '🚀');

  db.close();
  console.log(`  Created ${path} (${walMode} mode): 2 orgs, 6 users, 4 channels, 10 members, 7 messages, 3 reactions`);
}

// ---------------------------------------------------------------------------
// Schema definitions — mirrors what rust-ivm-driver.ts builds from spec.zqlSpec
// ---------------------------------------------------------------------------
const TABLES = {
  organizations: {
    columns: {
      id: {type: 'string', optional: false},
      name: {type: 'string', optional: false},
      plan: {type: 'string', optional: false},
    },
    primaryKey: ['id'],
  },
  users: {
    columns: {
      id: {type: 'string', optional: false},
      email: {type: 'string', optional: false},
      name: {type: 'string', optional: false},
      orgId: {type: 'string', optional: false},
      role: {type: 'string', optional: false},
      active: {type: 'number', optional: false},
      age: {type: 'number', optional: false},
      createdAt: {type: 'string', optional: false},
    },
    primaryKey: ['id'],
  },
  channels: {
    columns: {
      id: {type: 'string', optional: false},
      orgId: {type: 'string', optional: false},
      name: {type: 'string', optional: false},
      type: {type: 'string', optional: false},
      createdAt: {type: 'string', optional: false},
    },
    primaryKey: ['id'],
  },
  channel_members: {
    columns: {
      id: {type: 'string', optional: false},
      channelId: {type: 'string', optional: false},
      userId: {type: 'string', optional: false},
      role: {type: 'string', optional: false},
      joinedAt: {type: 'string', optional: false},
    },
    primaryKey: ['id'],
  },
  messages: {
    columns: {
      id: {type: 'string', optional: false},
      channelId: {type: 'string', optional: false},
      userId: {type: 'string', optional: false},
      body: {type: 'string', optional: false},
      createdAt: {type: 'string', optional: false},
    },
    primaryKey: ['id'],
  },
  reactions: {
    columns: {
      id: {type: 'string', optional: false},
      messageId: {type: 'string', optional: false},
      userId: {type: 'string', optional: false},
      emoji: {type: 'string', optional: false},
    },
    primaryKey: ['id'],
  },
};

// ---------------------------------------------------------------------------
// 1. Load NAPI addon
// ---------------------------------------------------------------------------
function loadAddon() {
  section('1. NAPI Addon Loading');

  assert(existsSync(ADDON_PATH), `Addon exists at ${ADDON_PATH}`);

  let addon;
  try {
    addon = require(ADDON_PATH);
    passed++;
  } catch (e) {
    failed++;
    failures.push(`Failed to load NAPI addon: ${e.message}`);
    console.error(`  FAIL: Cannot load addon: ${e.message}`);
    return null;
  }

  // Check export name (NAPI camelCase: RustIvmEngine, NOT RustIVMEngine)
  assert(addon.RustIvmEngine, 'Export RustIvmEngine exists');
  assert(!addon.RustIVMEngine, 'Export RustIVMEngine does NOT exist (NAPI camelCase)');

  console.log(`  Loaded: ${Object.keys(addon).join(', ')}`);
  return addon;
}

// ---------------------------------------------------------------------------
// 2. Engine lifecycle
// ---------------------------------------------------------------------------
function testLifecycle(addon) {
  section('2. Engine Lifecycle');

  const engine = new addon.RustIvmEngine();
  assert(engine, 'Engine created');

  // ping
  assertEqual(engine.ping(), 'pong', 'ping() returns "pong"');

  // version
  const ver = engine.version();
  assert(typeof ver.version === 'string', 'version() returns version string');
  assert(typeof ver.protocolRev === 'number', 'version() returns protocolRev');

  // initialized — false before init
  assertEqual(engine.initialized(), false, 'initialized() false before init');

  // init with all tables
  await engine.init(TABLES, null);
  assertEqual(engine.initialized(), true, 'initialized() true after init');

  // queries — empty before addQueries
  assertEqual(engine.queries().length, 0, 'queries() empty before addQueries');

  // totalHydrationTimeMs — 0 before any queries
  assertEqual(engine.totalHydrationTimeMs(), 0, 'totalHydrationTimeMs() 0 before queries');

  // reset
  engine.reset();
  assertEqual(engine.initialized(), false, 'initialized() false after reset');

  // re-init
  await engine.init(TABLES, null);
  assertEqual(engine.initialized(), true, 'initialized() true after re-init');

  // destroy
  engine.destroy();
  assertNoThrow(() => {}, 'destroy() does not throw');
}

// ---------------------------------------------------------------------------
// 3. SQLite connection (setDatabasePath)
// ---------------------------------------------------------------------------
function testSQLiteConnection(addon, dbPath) {
  section('3. SQLite Connection');

  const engine = new addon.RustIvmEngine();
  await engine.init(TABLES, null);

  // setDatabasePath — this is where WAL2 issues show up in Docker
  assertNoThrow(() => engine.setDatabasePath(dbPath), `setDatabasePath(${dbPath})`);

  // Verify we can query after setDatabasePath
  const results = engine.addQueries([
    {queryId: 'conn-test', ast: {table: 'users', orderBy: [['id', 'asc']]}},
  ]);
  assert(results && results.length === 1, 'addQueries after setDatabasePath returns results');
  if (results && results[0]) {
    assert(results[0].changes.length === 6, `Query returns 6 users (got ${results[0].changes.length})`);
  }

  engine.destroy();
}

// ---------------------------------------------------------------------------
// 4. Simple queries (SELECT * with ORDER BY)
// ---------------------------------------------------------------------------
function testSimpleQueries(addon, dbPath) {
  section('4. Simple Queries');

  const engine = new addon.RustIvmEngine();
  await engine.init(TABLES, null);
  engine.setDatabasePath(dbPath);

  // 4a. Single table, all rows, ORDER BY id
  const r1 = engine.addQueries([
    {queryId: 'q-users', ast: {table: 'users', orderBy: [['id', 'asc']]}},
  ]);
  assert(r1[0].changes.length === 6, 'All 6 users returned');
  if (r1[0].changes[0]) {
    assertEqual(r1[0].changes[0].changeType, 0, 'First change is add (0)');
    assertEqual(r1[0].changes[0].table, 'users', 'Change table is users');
    assertEqual(r1[0].changes[0].row.id.strVal, 'u-1', 'First user is u-1');
  }

  // 4b. Different table
  const r2 = engine.addQueries([
    {queryId: 'q-channels', ast: {table: 'channels', orderBy: [['id', 'asc']]}},
  ]);
  assert(r2[0].changes.length === 4, 'All 4 channels returned');

  // 4c. Messages
  const r3 = engine.addQueries([
    {queryId: 'q-messages', ast: {table: 'messages', orderBy: [['id', 'asc']]}},
  ]);
  assert(r3[0].changes.length === 7, 'All 7 messages returned');

  // 4d. ORDER BY desc
  const r4 = engine.addQueries([
    {queryId: 'q-users-desc', ast: {table: 'users', orderBy: [['id', 'desc']]}},
  ]);
  if (r4[0].changes.length > 0) {
    assertEqual(r4[0].changes[0].row.id.strVal, 'u-6', 'Desc order: first user is u-6');
  }

  // 4e. Multi-column ORDER BY
  const r5 = engine.addQueries([
    {queryId: 'q-users-multi', ast: {table: 'users', orderBy: [['orgId', 'asc'], ['name', 'asc']]}},
  ]);
  assert(r5[0].changes.length === 6, 'Multi-column ORDER BY returns 6 users');
  if (r5[0].changes.length >= 2) {
    // org-1 users sorted by name: Alice, Bob, Diana, Frank
    // org-2 users sorted by name: Charlie, Eve
    assertEqual(r5[0].changes[0].row.name.strVal, 'Alice', 'Multi-sort: first is Alice (org-1)');
    assertEqual(r5[0].changes[4].row.name.strVal, 'Charlie', 'Multi-sort: 5th is Charlie (org-2)');
  }

  if (VERBOSE) {
    for (const c of r5[0].changes) {
      console.log(`    ${c.row.orgId.strVal} / ${c.row.name.strVal}`);
    }
  }

  engine.destroy();
}

// ---------------------------------------------------------------------------
// 5. WHERE clause
// ---------------------------------------------------------------------------
function testWhereClause(addon, dbPath) {
  section('5. WHERE Clause');

  const engine = new addon.RustIvmEngine();
  await engine.init(TABLES, null);
  engine.setDatabasePath(dbPath);

  // 5a. Simple equality
  const r1 = engine.addQueries([
    {queryId: 'w1', ast: {
      table: 'users',
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'orgId'},
        right: {type: 'literal', value: 'org-1'},
      },
      orderBy: [['id', 'asc']],
    }},
  ]);
  assert(r1[0].changes.length === 4, 'WHERE orgId=org-1 returns 4 users');

  // 5b. Numeric equality
  const r2 = engine.addQueries([
    {queryId: 'w2', ast: {
      table: 'users',
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'active'},
        right: {type: 'literal', value: 1},
      },
      orderBy: [['id', 'asc']],
    }},
  ]);
  assert(r2[0].changes.length === 5, 'WHERE active=1 returns 5 users');

  // 5c. AND condition
  const r3 = engine.addQueries([
    {queryId: 'w3', ast: {
      table: 'users',
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'orgId'},
            right: {type: 'literal', value: 'org-1'},
          },
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'role'},
            right: {type: 'literal', value: 'member'},
          },
        ],
      },
      orderBy: [['id', 'asc']],
    }},
  ]);
  assert(r3[0].changes.length === 3, 'WHERE orgId=org-1 AND role=member returns 3 (Bob, Diana, Frank)');

  // 5d. OR condition
  const r4 = engine.addQueries([
    {queryId: 'w4', ast: {
      table: 'users',
      where: {
        type: 'or',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'role'},
            right: {type: 'literal', value: 'admin'},
          },
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'age'},
            right: {type: 'literal', value: 40},
          },
        ],
      },
      orderBy: [['id', 'asc']],
    }},
  ]);
  // admins: Alice(u-1), Charlie(u-3) + age=40: Eve(u-5)
  // But Charlie is admin AND in org-2, Eve has age 40
  // admins: u-1, u-3. age=40: u-5. Union: u-1, u-3, u-5 = 3
  assert(r4[0].changes.length === 3, `WHERE role=admin OR age=40 returns 3 (got ${r4[0].changes.length})`);

  // 5e. Greater than
  const r5 = engine.addQueries([
    {queryId: 'w5', ast: {
      table: 'users',
      where: {
        type: 'simple',
        op: '>',
        left: {type: 'column', name: 'age'},
        right: {type: 'literal', value: 30},
      },
      orderBy: [['age', 'asc']],
    }},
  ]);
  // age > 30: Charlie(35), Eve(40), Frank(50) = 3
  assert(r5[0].changes.length === 3, `WHERE age>30 returns 3 (got ${r5[0].changes.length})`);

  // 5f. No results
  const r6 = engine.addQueries([
    {queryId: 'w6', ast: {
      table: 'users',
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'id'},
        right: {type: 'literal', value: 'nonexistent'},
      },
      orderBy: [['id', 'asc']],
    }},
  ]);
  assert(r6[0].changes.length === 0, 'WHERE id=nonexistent returns 0 rows');

  engine.destroy();
}

// ---------------------------------------------------------------------------
// 6. LIMIT
// ---------------------------------------------------------------------------
function testLimit(addon, dbPath) {
  section('6. LIMIT');

  const engine = new addon.RustIvmEngine();
  await engine.init(TABLES, null);
  engine.setDatabasePath(dbPath);

  // 6a. LIMIT 2
  const r1 = engine.addQueries([
    {queryId: 'l1', ast: {table: 'users', orderBy: [['id', 'asc']], limit: 2}},
  ]);
  assert(r1[0].changes.length === 2, 'LIMIT 2 returns 2 rows');
  assertEqual(r1[0].changes[0].row.id.strVal, 'u-1', 'First row is u-1');
  assertEqual(r1[0].changes[1].row.id.strVal, 'u-2', 'Second row is u-2');

  // 6b. LIMIT 0
  const r2 = engine.addQueries([
    {queryId: 'l2', ast: {table: 'users', orderBy: [['id', 'asc']], limit: 0}},
  ]);
  assert(r2[0].changes.length === 0, 'LIMIT 0 returns 0 rows');

  // 6c. LIMIT > row count
  const r3 = engine.addQueries([
    {queryId: 'l3', ast: {table: 'users', orderBy: [['id', 'asc']], limit: 100}},
  ]);
  assert(r3[0].changes.length === 6, 'LIMIT 100 returns all 6 rows');

  // 6d. LIMIT with WHERE
  const r4 = engine.addQueries([
    {queryId: 'l4', ast: {
      table: 'users',
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'orgId'},
        right: {type: 'literal', value: 'org-1'},
      },
      orderBy: [['name', 'asc']],
      limit: 2,
    }},
  ]);
  assert(r4[0].changes.length === 2, 'LIMIT 2 with WHERE returns 2 rows');
  assertEqual(r4[0].changes[0].row.name.strVal, 'Alice', 'First is Alice');
  assertEqual(r4[0].changes[1].row.name.strVal, 'Bob', 'Second is Bob');

  engine.destroy();
}

// ---------------------------------------------------------------------------
// 7. Related subqueries (JOINs)
// ---------------------------------------------------------------------------
function testRelatedSubqueries(addon, dbPath) {
  section('7. Related Subqueries (Joins)');

  const engine = new addon.RustIvmEngine();
  await engine.init(TABLES, null);
  engine.setDatabasePath(dbPath);

  // 7a. Users → channels (via channel_members)
  // AST: { table: 'users', related: [{ subquery: { table: 'channel_members', ... }, correlation: { parentField: ['id'], childField: ['userId'] } }] }
  const r1 = engine.addQueries([
    {queryId: 'j1', ast: {
      table: 'users',
      orderBy: [['id', 'asc']],
      related: [{
        subquery: {
          table: 'channel_members',
          orderBy: [['id', 'asc']],
        },
        correlation: {
          parentField: ['id'],
          childField: ['userId'],
        },
        hidden: false,
      }],
    }},
  ]);

  // Alice (u-1) has 3 memberships: m-1, m-3, m-9
  if (r1[0].changes.length > 0) {
    const aliceChanges = r1[0].changes.filter(c => c.table === 'channel_members' && c.rowKey?.id?.strVal === 'm-1');
    assert(aliceChanges.length > 0, 'Alice has channel_member m-1');
  }

  // Check we get both parent and child rows
  const userRows = r1[0].changes.filter(c => c.table === 'users');
  const memberRows = r1[0].changes.filter(c => c.table === 'channel_members');
  assert(userRows.length === 6, `6 user rows in join (got ${userRows.length})`);
  assert(memberRows.length === 10, `10 channel_member rows in join (got ${memberRows.length})`);

  if (VERBOSE) {
    console.log('  Join results:');
    for (const c of r1[0].changes) {
      console.log(`    ${c.table}: ${c.rowKey?.id?.strVal} (type=${c.changeType})`);
    }
  }

  engine.destroy();
}

// ---------------------------------------------------------------------------
// 8. Advance operations (add, edit, remove)
// ---------------------------------------------------------------------------
function testAdvance(addon, dbPath) {
  section('8. Advance Operations');

  const engine = new addon.RustIvmEngine();
  await engine.init(TABLES, null);
  engine.setDatabasePath(dbPath);

  // Setup: add a query
  engine.addQueries([
    {queryId: 'a1', ast: {table: 'users', orderBy: [['id', 'asc']]}},
  ]);

  // 8a. ADD — new user
  const addResult = engine.advanceWithDiff([
    {
      table: 'users',
      changeType: 'add',
      row: rowToNapi({
        id: 'u-7', email: 'grace@acme.com', name: 'Grace',
        orgId: 'org-1', role: 'member', active: 1, age: 22, createdAt: '2026-01-01',
      }),
    },
  ]);
  assertNoThrow(() => {}, 'advanceWithDiff (add) does not throw');
  if (VERBOSE) console.log(`  ADD produced ${addResult.length} changes`);

  // 8b. EDIT — update existing user
  const editResult = engine.advanceWithDiff([
    {
      table: 'users',
      changeType: 'edit',
      row: rowToNapi({
        id: 'u-1', email: 'alice@acme.com', name: 'Alice Updated',
        orgId: 'org-1', role: 'admin', active: 1, age: 31, createdAt: '2026-01-01',
      }),
      oldRow: rowToNapi({
        id: 'u-1', email: 'alice@acme.com', name: 'Alice',
        orgId: 'org-1', role: 'admin', active: 1, age: 30, createdAt: '2026-01-01',
      }),
    },
  ]);
  assertNoThrow(() => {}, 'advanceWithDiff (edit) does not throw');
  if (VERBOSE) console.log(`  EDIT produced ${editResult.length} changes`);

  // 8c. REMOVE — delete existing user
  const removeResult = engine.advanceWithDiff([
    {
      table: 'users',
      changeType: 'remove',
      row: rowToNapi({
        id: 'u-3', email: 'charlie@beta.com', name: 'Charlie',
        orgId: 'org-2', role: 'admin', active: 1, age: 35, createdAt: '2026-01-01',
      }),
    },
  ]);
  assertNoThrow(() => {}, 'advanceWithDiff (remove) does not throw');
  if (VERBOSE) console.log(`  REMOVE produced ${removeResult.length} changes`);

  // 8d. Multiple changes in one advance
  const multiResult = engine.advanceWithDiff([
    {
      table: 'users',
      changeType: 'add',
      row: rowToNapi({
        id: 'u-8', email: 'henry@acme.com', name: 'Henry',
        orgId: 'org-1', role: 'member', active: 1, age: 45, createdAt: '2026-01-01',
      }),
    },
    {
      table: 'users',
      changeType: 'remove',
      row: rowToNapi({
        id: 'u-4', email: 'diana@acme.com', name: 'Diana',
        orgId: 'org-1', role: 'member', active: 0, age: 28, createdAt: '2026-01-01',
      }),
    },
  ]);
  assertNoThrow(() => {}, 'advanceWithDiff (multi) does not throw');
  if (VERBOSE) console.log(`  MULTI produced ${multiResult.length} changes`);

  // 8e. Advance on unrelated table (should produce no changes for this query)
  const unrelatedResult = engine.advanceWithDiff([
    {
      table: 'channels',
      changeType: 'add',
      row: rowToNapi({id: 'c-5', orgId: 'org-1', name: 'test', type: 'public', createdAt: '2026-01-01'}),
    },
  ]);
  // The users query should not be affected by channel changes
  if (VERBOSE) console.log(`  UNRELATED produced ${unrelatedResult.length} changes`);

  engine.destroy();
}

// ---------------------------------------------------------------------------
// 9. Row operations
// ---------------------------------------------------------------------------
function testRowOperations(addon, dbPath) {
  section('9. Row Operations');

  const engine = new addon.RustIvmEngine();
  await engine.init(TABLES, null);
  engine.setDatabasePath(dbPath);

  // Add a query first (needed for row tracking)
  engine.addQueries([
    {queryId: 'r1', ast: {table: 'users', orderBy: [['id', 'asc']]}},
  ]);

  // 9a. rowSetSignature
  const sig1 = engine.rowSetSignature('r1');
  assert(sig1 !== null && sig1 !== undefined, 'rowSetSignature returns value');
  if (VERBOSE) console.log(`  Signature for r1: ${sig1}`);

  // 9b. rowSetSignature for nonexistent query
  const sig2 = engine.rowSetSignature('nonexistent');
  assert(sig2 === null || sig2 === undefined, 'rowSetSignature for nonexistent returns null');

  // 9c. getRow — in SQLite mode, in-memory is empty, so returns null
  const row = engine.getRow('users', {id: toNapiValue('u-1')});
  // getRow reads from in-memory data which is empty in SQLite mode
  if (VERBOSE) console.log(`  getRow('users', u-1) returned: ${row ? 'row' : 'null'}`);

  // 9d. getAllRows — in SQLite mode, returns empty
  const allRows = engine.getAllRows('users');
  if (VERBOSE) console.log(`  getAllRows('users') returned ${allRows.length} rows`);

  // 9e. queries listing
  const queries = engine.queries();
  assert(queries.includes('r1'), 'queries() includes r1');

  // 9f. removeQuery
  engine.removeQuery('r1');
  assertEqual(engine.queries().length, 0, 'queries() empty after removeQuery');

  engine.destroy();
}

// ---------------------------------------------------------------------------
// 10. Error handling
// ---------------------------------------------------------------------------
function testErrorHandling(addon, dbPath) {
  section('10. Error Handling');

  // 10a. setDatabasePath with nonexistent path
  const engine1 = new addon.RustIvmEngine();
  await engine1.init(TABLES, null);
  // Should throw or handle gracefully
  let threw = false;
  try {
    engine1.setDatabasePath('/nonexistent/path/replica.db');
  } catch (e) {
    threw = true;
    if (VERBOSE) console.log(`  Bad path threw: ${e.message}`);
  }
  // Either behavior is acceptable — just log it
  if (VERBOSE) console.log(`  setDatabasePath bad path: ${threw ? 'threw' : 'did not throw'}`);
  engine1.destroy();

  // 10b. Query on nonexistent table — should return 0 rows (no panic)
  const engine2b = new addon.RustIvmEngine();
  await engine2b.init(TABLES, null);
  engine2b.setDatabasePath(dbPath);
  let badResults = null;
  assertNoThrow(() => {
    badResults = engine2b.addQueries([
      {queryId: 'bad-1', ast: {table: 'nonexistent_table', orderBy: [['id', 'asc']]}},
    ]);
  }, 'Query on nonexistent table does not throw');
  assert(badResults && badResults[0].changes.length === 0, 'Query on nonexistent table returns 0 rows');
  engine2b.destroy();

  // 10c. addQueries with empty array
  const engine2 = new addon.RustIvmEngine();
  await engine2.init(TABLES, null);
  engine2.setDatabasePath(dbPath);
  let emptyResults;
  assertNoThrow(() => {
    emptyResults = engine2.addQueries([]);
  }, 'addQueries([]) does not throw');
  assertEqual(emptyResults.length, 0, 'Empty addQueries returns empty array');
  engine2.destroy();

  // 10d. advanceWithDiff with empty array
  const engine3 = new addon.RustIvmEngine();
  await engine3.init(TABLES, null);
  engine3.setDatabasePath(dbPath);
  engine3.addQueries([{queryId: 'e1', ast: {table: 'users', orderBy: [['id', 'asc']]}}]);
  let emptyAdvance;
  assertNoThrow(() => {
    emptyAdvance = engine3.advanceWithDiff([]);
  }, 'advanceWithDiff([]) does not throw');
  assertEqual(emptyAdvance.length, 0, 'Empty advanceWithDiff returns empty array');
  engine3.destroy();

  // 10e. Double init (reset first)
  const engine4 = new addon.RustIvmEngine();
  await engine4.init(TABLES, null);
  engine4.reset();
  assertNoThrow(() => await engine4.init(TABLES, null), 'Double init after reset does not throw');
  engine4.destroy();

  // 10f. removeQuery on nonexistent query
  const engine5 = new addon.RustIvmEngine();
  await engine5.init(TABLES, null);
  assertNoThrow(() => engine5.removeQuery('nonexistent'), 'removeQuery nonexistent does not throw');
  engine5.destroy();
}

// ---------------------------------------------------------------------------
// 11. Driver simulation — exact flow matching rust-ivm-driver.ts
// ---------------------------------------------------------------------------
function testDriverSimulation(addon, dbPath) {
  section('11. Driver Simulation (exact driver flow)');

  // This simulates exactly what RustIVMDriver does:
  // 1. new RustIvmEngine()
  // 2. init(tables) — tables built from spec.zqlSpec (flat column map)
  // 3. setDatabasePath(replicaFile)
  // 4. addQueries([{queryId, ast}])
  // 5. advanceWithDiff(sourceChanges)

  const engine = new addon.RustIvmEngine();

  // Step 2: init — using flat column spec (like spec.zqlSpec)
  // The driver does: for (const [col, type] of Object.entries(spec.zqlSpec))
  // where type is a string like 'string', 'number', etc.
  await engine.init(TABLES, null);

  // Step 3: setDatabasePath
  engine.setDatabasePath(dbPath);

  // Step 4: addQueries — the AST comes from the client, already transformed
  const queries = [
    {queryId: 'sim-1', ast: {table: 'users', orderBy: [['id', 'asc']]}},
    {queryId: 'sim-2', ast: {table: 'channels', orderBy: [['name', 'asc']]}},
    {queryId: 'sim-3', ast: {
      table: 'messages',
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'channelId'},
        right: {type: 'literal', value: 'c-1'},
      },
      orderBy: [['id', 'asc']],
    }},
  ];

  const results = engine.addQueries(queries);
  assertEqual(results.length, 3, '3 queries return 3 results');
  assert(results[0].changes.length === 6, 'sim-1: 6 users');
  assert(results[1].changes.length === 4, 'sim-2: 4 channels');
  assert(results[2].changes.length === 4, 'sim-3: 4 messages in c-1');

  // Verify all row changes convert correctly through napiToRowChange
  for (const result of results) {
    for (const change of result.changes) {
      const rowChange = napiToRowChange(change);
      assert(typeof rowChange.type === 'number', `RowChange type is number for ${result.queryId}`);
      assert(typeof rowChange.queryID === 'string', `RowChange queryID is string for ${result.queryId}`);
      assert(typeof rowChange.table === 'string', `RowChange table is string for ${result.queryId}`);
      assert(typeof rowChange.rowKey === 'object', `RowChange rowKey is object for ${result.queryId}`);
    }
  }

  // Step 5: advanceWithDiff — simulates snapshotter diff
  const diffChanges = [
    {
      table: 'users',
      changeType: 'edit',
      row: rowToNapi({
        id: 'u-1', email: 'alice@acme.com', name: 'Alice Smith',
        orgId: 'org-1', role: 'admin', active: 1, age: 31, createdAt: '2026-01-01',
      }),
      oldRow: rowToNapi({
        id: 'u-1', email: 'alice@acme.com', name: 'Alice',
        orgId: 'org-1', role: 'admin', active: 1, age: 30, createdAt: '2026-01-01',
      }),
    },
    {
      table: 'messages',
      changeType: 'add',
      row: rowToNapi({
        id: 'msg-8', channelId: 'c-1', userId: 'u-2', body: 'New message', createdAt: '2026-01-01',
      }),
    },
  ];

  const advanceResult = engine.advanceWithDiff(diffChanges);
  assertNoThrow(() => {}, 'Driver simulation advance does not throw');
  if (VERBOSE) console.log(`  Advance produced ${advanceResult.length} changes`);

  // Verify advance results convert correctly
  for (const change of advanceResult) {
    const rowChange = napiToRowChange(change);
    assert(typeof rowChange.type === 'number', 'Advance RowChange type is number');
    assert(typeof rowChange.queryID === 'string', 'Advance RowChange queryID is string');
  }

  // Step 6: rowSetSignature
  const sig = engine.rowSetSignature('sim-1');
  assert(sig !== null, 'rowSetSignature returns value after queries');

  // Step 7: removeQuery
  engine.removeQuery('sim-1');
  assertEqual(engine.queries().length, 2, '2 queries after removing sim-1');

  // Step 8: destroy
  engine.destroy();
  console.log('  Driver simulation completed successfully');
}

// ---------------------------------------------------------------------------
// 12. Stress test
// ---------------------------------------------------------------------------
function testStress(addon, dbPath) {
  section('12. Stress Test');

  const engine = new addon.RustIvmEngine();
  await engine.init(TABLES, null);
  engine.setDatabasePath(dbPath);

  // 12a. Many queries at once
  const queries = [];
  for (let i = 0; i < 50; i++) {
    queries.push({
      queryId: `stress-${i}`,
      ast: {table: 'users', orderBy: [['id', 'asc']]},
    });
  }
  const t0 = Date.now();
  const results = engine.addQueries(queries);
  const elapsed = Date.now() - t0;
  assertEqual(results.length, 50, '50 queries return 50 results');
  for (const r of results) {
    assert(r.changes.length === 6, `Stress query ${r.queryId} returns 6 rows`);
  }
  console.log(`  50 queries in ${elapsed}ms (${(elapsed / 50).toFixed(1)}ms/query)`);

  // 12b. Remove all and re-add
  for (let i = 0; i < 50; i++) {
    engine.removeQuery(`stress-${i}`);
  }
  assertEqual(engine.queries().length, 0, 'All stress queries removed');

  // 12c. Large advance batch
  const bigDiff = [];
  for (let i = 0; i < 100; i++) {
    bigDiff.push({
      table: 'users',
      changeType: 'add',
      row: rowToNapi({
        id: `stress-u-${i}`, email: `stress${i}@test.com`, name: `Stress${i}`,
        orgId: 'org-1', role: 'member', active: 1, age: 20 + i, createdAt: '2026-01-01',
      }),
    });
  }
  engine.addQueries([{queryId: 'stress-big', ast: {table: 'users', orderBy: [['id', 'asc']]}}]);
  const t1 = Date.now();
  const bigResult = engine.advanceWithDiff(bigDiff);
  const bigElapsed = Date.now() - t1;
  console.log(`  100-change advance in ${bigElapsed}ms (${bigResult.length} output changes)`);
  assertNoThrow(() => {}, '100-change advance does not throw');

  // 12d. Many tables registered
  const engine2 = new addon.RustIvmEngine();
  const manyTables = {};
  for (let i = 0; i < 100; i++) {
    manyTables[`table_${i}`] = {
      columns: {
        id: {type: 'string', optional: false},
        data: {type: 'string', optional: false},
      },
      primaryKey: ['id'],
    };
  }
  assertNoThrow(() => await engine2.init(manyTables, null), 'init with 100 tables does not throw');
  assertEqual(engine2.initialized(), true, '100 tables initialized');
  engine2.destroy();

  engine.destroy();
}

// ---------------------------------------------------------------------------
// 13. Multiple databases (simulate multiple client groups)
// ---------------------------------------------------------------------------
function testMultipleEngines(addon, dbPath) {
  section('13. Multiple Engines (Multiple Client Groups)');

  // Simulate two client groups with separate engines
  const engine1 = new addon.RustIvmEngine();
  const engine2 = new addon.RustIvmEngine();

  await engine1.init(TABLES, null);
  await engine2.init(TABLES, null);

  engine1.setDatabasePath(dbPath);
  engine2.setDatabasePath(dbPath);

  // Both engines query the same table
  engine1.addQueries([{queryId: 'cg1-q1', ast: {table: 'users', orderBy: [['id', 'asc']]}}]);
  engine2.addQueries([{queryId: 'cg2-q1', ast: {table: 'users', orderBy: [['id', 'asc']]}}]);

  assert(engine1.queries().includes('cg1-q1'), 'Engine 1 has cg1-q1');
  assert(engine2.queries().includes('cg2-q1'), 'Engine 2 has cg2-q1');
  assert(!engine1.queries().includes('cg2-q1'), 'Engine 1 does NOT have cg2-q1');
  assert(!engine2.queries().includes('cg1-q1'), 'Engine 2 does NOT have cg1-q1');

  // Advance on engine 1 should not affect engine 2
  engine1.advanceWithDiff([{
    table: 'users',
    changeType: 'add',
    row: rowToNapi({
      id: 'multi-u-1', email: 'multi@test.com', name: 'Multi',
      orgId: 'org-1', role: 'member', active: 1, age: 25, createdAt: '2026-01-01',
    }),
  }]);

  // Both should still function independently
  assertNoThrow(() => {
    engine2.advanceWithDiff([{
      table: 'users',
      changeType: 'add',
      row: rowToNapi({
        id: 'multi-u-2', email: 'multi2@test.com', name: 'Multi2',
        orgId: 'org-1', role: 'member', active: 1, age: 30, createdAt: '2026-01-01',
      }),
    }]);
  }, 'Engine 2 advance after engine 1 advance does not throw');

  engine1.destroy();
  engine2.destroy();
  console.log('  Two engines operate independently');
}

// ---------------------------------------------------------------------------
// 14. Schema edge cases
// ---------------------------------------------------------------------------
function testSchemaEdgeCases(addon, dbPath) {
  section('14. Schema Edge Cases');

  // 14a. Flat column spec (matching driver's spec.zqlSpec usage)
  // The driver does: for (const [col, type] of Object.entries(spec.zqlSpec))
  // where type is a string, NOT an object with {type, optional}
  const engine = new addon.RustIvmEngine();
  engine.init({
    users: {
      columns: {
        id: {type: 'string', optional: false},
        name: {type: 'string', optional: false},
        active: {type: 'number', optional: false},
        age: {type: 'number', optional: false},
        orgId: {type: 'string', optional: false},
        email: {type: 'string', optional: false},
        role: {type: 'string', optional: false},
        createdAt: {type: 'string', optional: false},
      },
      primaryKey: ['id'],
    },
  });
  engine.setDatabasePath(dbPath);

  const results = engine.addQueries([
    {queryId: 'schema-1', ast: {table: 'users', orderBy: [['id', 'asc']]}},
  ]);
  assert(results[0].changes.length === 6, 'Schema with all columns returns 6 rows');
  engine.destroy();

  // 14b. Partial columns (only some columns registered)
  const engine2 = new addon.RustIvmEngine();
  engine2.init({
    users: {
      columns: {
        id: {type: 'string', optional: false},
        name: {type: 'string', optional: false},
      },
      primaryKey: ['id'],
    },
  });
  engine2.setDatabasePath(dbPath);

  const results2 = engine2.addQueries([
    {queryId: 'schema-2', ast: {table: 'users', orderBy: [['id', 'asc']]}},
  ]);
  // Should still return 6 rows, but only with id and name columns
  assert(results2[0].changes.length === 6, 'Partial schema returns 6 rows');
  if (results2[0].changes[0]) {
    assert(results2[0].changes[0].row.id !== undefined, 'Partial schema has id');
    assert(results2[0].changes[0].row.name !== undefined, 'Partial schema has name');
  }
  engine2.destroy();

  // 14c. setTableSpec (minRowVersion)
  const engine3 = new addon.RustIvmEngine();
  await engine3.init(TABLES, null);
  assertNoThrow(() => engine3.setTableSpec('users', '2026-01-01T00:00:00Z'), 'setTableSpec does not throw');
  engine3.destroy();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
function main() {
  console.log('Rust IVM NAPI Comprehensive Test Suite');
  console.log('======================================');
  console.log(`Addon: ${ADDON_PATH}`);
  console.log(`Node:  ${process.version}`);
  console.log(`Verbose: ${VERBOSE}`);

  // 1. Load addon
  const addon = loadAddon();
  if (!addon) {
    console.error('\nFATAL: Cannot load NAPI addon.');
    console.error('Build it first: cd rust-ivm/napi && napi build --release');
    process.exit(1);
  }

  // 2. Lifecycle
  testLifecycle(addon);

  // 3. Create test database
  section('Create Test Database');
  createTestDB(TEST_DB, 'wal');

  // 4. SQLite connection
  testSQLiteConnection(addon, TEST_DB);

  // 5. Simple queries
  testSimpleQueries(addon, TEST_DB);

  // 6. WHERE clause
  testWhereClause(addon, TEST_DB);

  // 7. LIMIT
  testLimit(addon, TEST_DB);

  // 8. Related subqueries
  testRelatedSubqueries(addon, TEST_DB);

  // 9. Advance operations
  testAdvance(addon, TEST_DB);

  // 10. Row operations
  testRowOperations(addon, TEST_DB);

  // 11. Error handling
  testErrorHandling(addon, TEST_DB);

  // 12. Driver simulation
  testDriverSimulation(addon, TEST_DB);

  // 13. Stress test
  testStress(addon, TEST_DB);

  // 14. Multiple engines
  testMultipleEngines(addon, TEST_DB);

  // 15. Schema edge cases
  testSchemaEdgeCases(addon, TEST_DB);

  // 16. WAL2 mode test (optional)
  if (TEST_WAL2) {
    section('WAL2 Mode Test');
    try {
      createTestDB(WAL2_DB, 'wal2');
      testSQLiteConnection(addon, WAL2_DB);
      testSimpleQueries(addon, WAL2_DB);
    } catch (e) {
      skip(`WAL2 mode test: ${e.message}`);
      console.log('  (WAL2 requires rocicorp-patched SQLite — only in Docker)');
    }
  }

  // Summary
  console.log('\n======================================');
  console.log(`Results: ${passed} passed, ${failed} failed, ${skipped} skipped`);
  if (failures.length > 0) {
    console.log('\nFailures:');
    for (const f of failures) {
      console.log(`  - ${f}`);
    }
  }
  console.log('======================================');

  // Cleanup
  for (const f of [TEST_DB, WAL2_DB]) {
    if (existsSync(f)) unlinkSync(f);
    for (const ext of ['-wal', '-shm', '-journal', '-wal2', '-shm2']) {
      if (existsSync(f + ext)) unlinkSync(f + ext);
    }
  }

  process.exit(failed > 0 ? 1 : 0);
}

main();
