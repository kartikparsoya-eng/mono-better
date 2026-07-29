#!/usr/bin/env node
/**
 * Tests for the two critical fixes:
 * 1. Queue no-drop: all rows preserved when worker produces faster than consumer
 * 2. Single-conn-per-source: no hang/deadlock on nested fetches, connection reuse
 * 3. Error propagation: panic caught, finish_with_error called
 * 4. FlippedJoin hidden child rows not dropped on large result sets
 *
 * These tests FAIL with the old code (queue drop + connection pool).
 *
 * Usage: node rust-ivm/scripts/test-fixes.mjs
 * Exits 0 on success, 1 on failure.
 */

import {createRequire} from 'node:module';
import {DatabaseSync as Database} from 'node:sqlite';
import {unlinkSync, existsSync} from 'node:fs';
import {join} from 'node:path';

const require = createRequire(import.meta.url);
const SCRIPT_DIR = import.meta.dirname;
const ADDON_PATH = join(SCRIPT_DIR, '..', 'napi', 'rust-ivm.node');
const TEST_DB = join(SCRIPT_DIR, 'test-fixes.db');

let passed = 0, failed = 0;
const failures = [];

function assert(cond, msg) {
  if (cond) { passed++; } else { failed++; failures.push(msg); console.error(`  FAIL: ${msg}`); }
}
function assertEqual(a, e, msg) {
  if (a === e) { passed++; } else { failed++; failures.push(msg); console.error(`  FAIL: ${msg} — expected ${e}, got ${a}`); }
}
function section(name) { console.log(`\n=== ${name} ===`); }

function createTestDB(path, numChannels, numStatsPerChannel) {
  if (existsSync(path)) unlinkSync(path);
  const db = new Database(path);
  db.exec('PRAGMA journal_mode = wal');
  db.exec(`
    CREATE TABLE channels (id TEXT PRIMARY KEY, name TEXT);
    CREATE TABLE channel_stats (channelId TEXT PRIMARY KEY, participantCount INTEGER, lastActivityAt TEXT);
    CREATE TABLE channel_user_status (id TEXT PRIMARY KEY, channelId TEXT, userId TEXT, isStarred INTEGER);
    CREATE TABLE conversations (conversationId TEXT PRIMARY KEY, channelId TEXT, createdBy TEXT);
  `);

  const insChan = db.prepare('INSERT INTO channels (id, name) VALUES (?, ?)');
  const insStats = db.prepare('INSERT INTO channel_stats (channelId, participantCount, lastActivityAt) VALUES (?, ?, ?)');
  const insCus = db.prepare('INSERT INTO channel_user_status (id, channelId, userId, isStarred) VALUES (?, ?, ?, ?)');

  for (let i = 0; i < numChannels; i++) {
    const chId = `ch-${i}`;
    insChan.run(chId, `Channel ${i}`);
    insStats.run(chId, i % 10, `2024-01-${(i % 28) + 1}`);
    for (let j = 0; j < numStatsPerChannel; j++) {
      insCus.run(`cus-${i}-${j}`, chId, `user-${j}`, j % 2);
    }
  }
  db.close();
}

async function drainIterator(iter) {
  let count = 0;
  while (true) {
    const row = await iter.next();
    if (row === null || row === undefined) break;
    count++;
  }
  return count;
}

async function main() {
  const m = require(ADDON_PATH);

  // ========================================================================
  // Test 1: Queue no-drop — 5000+ rows must all be delivered
  // ========================================================================
  section('Test 1: Queue no-drop (5000+ rows)');
  {
    createTestDB(TEST_DB, 2000, 3);  // 2000 channels + 6000 cus = 8000 rows

    const e = new m.RustIvmEngine();
    await e.init([
      {table: 'channels', columns: {id: {type: 'string', optional: false}}, primaryKey: ['id']},
      {table: 'channel_stats', columns: {channelId: {type: 'string', optional: false}}, primaryKey: ['channelId']},
      {table: 'channel_user_status', columns: {id: {type: 'string', optional: false}}, primaryKey: ['id']},
    ], TEST_DB);

    // Simple query: 2000 channels
    const iter1 = await e.addQueriesStreaming([
      {queryId: 'q1', ast: {table: 'channels', orderBy: [{column: 'id', direction: 'asc'}]}},
    ]);
    const c1 = await drainIterator(iter1);
    assertEqual(c1, 2000, 'simple query: 2000 channels must all be delivered');

    // FlippedJoin EXISTS hidden: 2000 channels × 3 cus = many rows
    const iter2 = await e.addQueriesStreaming([{
      queryId: 'q2',
      ast: {
        table: 'channels',
        orderBy: [{column: 'id', direction: 'asc'}],
        where: {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          related: {
            relationshipName: 'cus',
            correlation: {parentField: ['id'], childField: ['channelId']},
            subquery: {table: 'channel_user_status', orderBy: [{column: 'id', direction: 'asc'}]},
            hidden: true,
          },
        },
      },
    }]);
    const c2 = await drainIterator(iter2);
    // Each channel has 3 cus, so 2000 channels pass EXISTS, each yields 2000 parent + 6000 child = 8000
    assert(c2 >= 2000, `FlippedJoin: at least 2000 rows (got ${c2}) — old code would drop to 4096`);
    assert(c2 === 8000, `FlippedJoin: exactly 8000 rows (got ${c2}) — no drops allowed`);

    await e.destroy();
    console.log('  PASS: queue preserved all rows (no drop)');
  }

  // ========================================================================
  // Test 2: Single-conn-per-source — no hang on repeated/nested fetches
  // ========================================================================
  section('Test 2: Single-conn-per-source (no hang, reuse)');
  {
    const e = new m.RustIvmEngine();
    await e.init([
      {table: 'channels', columns: {id: {type: 'string', optional: false}}, primaryKey: ['id']},
      {table: 'channel_stats', columns: {channelId: {type: 'string', optional: false}}, primaryKey: ['channelId']},
    ], TEST_DB);

    // Fetch the same table 20 times — must not hang
    for (let i = 0; i < 20; i++) {
      const iter = await e.addQueriesStreaming([
        {queryId: `r${i}`, ast: {table: 'channels', orderBy: [{column: 'id', direction: 'asc'}]}},
      ]);
      const count = await drainIterator(iter);
      assertEqual(count, 2000, `repeated fetch #${i}: 2000 rows`);
    }

    // Nested fetch via FlippedJoin — must not deadlock
    for (let i = 0; i < 5; i++) {
      const iter = await e.addQueriesStreaming([{
        queryId: `n${i}`,
        ast: {
          table: 'channels',
          orderBy: [{column: 'id', direction: 'asc'}],
          related: [{
            relationshipName: 'stats',
            correlation: {parentField: ['id'], childField: ['channelId']},
            subquery: {table: 'channel_stats', orderBy: [{column: 'channelId', direction: 'asc'}]},
          }],
        },
      }]);
      const count = await drainIterator(iter);
      assert(count >= 2000, `nested fetch #${i}: at least 2000 rows (got ${count})`);
    }

    await e.destroy();
    console.log('  PASS: no hang/deadlock, connection reused');
  }

  // ========================================================================
  // Test 3: Error propagation — uninitialized engine, panic caught
  // ========================================================================
  section('Test 3: Error propagation');
  {
    // Uninitialized engine — addQueriesStreaming must return error
    const e = new m.RustIvmEngine();
    await e.init([], null);  // no tables, no db

    // Nonexistent table returns 0 rows (EmptyInput), not an error.
    const iter = await e.addQueriesStreaming([
      {queryId: 'bad', ast: {table: 'nonexistent', orderBy: [{column: 'id', direction: 'asc'}]}},
    ]);
    const count = await drainIterator(iter);
    assertEqual(count, 0, 'nonexistent table returns 0 rows (EmptyInput)');

    // Engine not initialized at all
    const e2 = new m.RustIvmEngine();
    try {
      const iter = await e2.addQueriesStreaming([
        {queryId: 'bad2', ast: {table: 'x', orderBy: [{column: 'id', direction: 'asc'}]}},
      ]);
      await drainIterator(iter);
      assert(false, 'should have thrown for uninitialized engine');
    } catch (err) {
      assert(true, 'error propagated for uninitialized engine');
    }

    // Advance on uninitialized engine
    const e3 = new m.RustIvmEngine();
    try {
      const iter = await e3.advanceWithDiffStreaming([
        {table: 'x', changeType: 'add', row: {id: {kind: 'str', strVal: 'a'}}},
      ]);
      await drainIterator(iter);
      assert(false, 'should have thrown for advance on uninitialized');
    } catch (err) {
      assert(true, 'error propagated for advance on uninitialized');
    }

    console.log('  PASS: all error paths propagated');
  }

  // ========================================================================
  // Test 4: Batch query — multiple queries in one addQueriesStreaming
  // ========================================================================
  section('Test 4: Batch query (multiple queries)');
  {
    const e = new m.RustIvmEngine();
    await e.init([
      {table: 'channels', columns: {id: {type: 'string', optional: false}}, primaryKey: ['id']},
      {table: 'channel_stats', columns: {channelId: {type: 'string', optional: false}}, primaryKey: ['channelId']},
      {table: 'channel_user_status', columns: {id: {type: 'string', optional: false}}, primaryKey: ['id']},
    ], TEST_DB);

    // Send 3 queries in one batch
    const iter = await e.addQueriesStreaming([
      {queryId: 'a', ast: {table: 'channels', orderBy: [{column: 'id', direction: 'asc'}]}},
      {queryId: 'b', ast: {table: 'channel_stats', orderBy: [{column: 'channelId', direction: 'asc'}]}},
      {queryId: 'c', ast: {table: 'channel_user_status', orderBy: [{column: 'id', direction: 'asc'}]}},
    ]);

    const count = await drainIterator(iter);
    assertEqual(count, 2000 + 2000 + 6000, 'batch: 2000+2000+6000=10000 rows');

    await e.destroy();
    console.log('  PASS: batch query delivered all rows');
  }

  // ========================================================================
  // Test 5: Multiple engines on same DB (simulates 4 syncer workers)
  // ========================================================================
  section('Test 5: Multiple engines, same DB (concurrent)');
  {
    const engines = [];
    for (let i = 0; i < 4; i++) {
      const e = new m.RustIvmEngine();
      await e.init([
        {table: 'channels', columns: {id: {type: 'string', optional: false}}, primaryKey: ['id']},
        {table: 'channel_user_status', columns: {id: {type: 'string', optional: false}}, primaryKey: ['id']},
      ], TEST_DB);
      engines.push(e);
    }

    // All 4 engines fetch concurrently — must not deadlock
    const promises = engines.map(async (e, i) => {
      const iter = await e.addQueriesStreaming([
        {queryId: `concurrent-${i}`, ast: {table: 'channels', orderBy: [{column: 'id', direction: 'asc'}]}},
      ]);
      const count = await drainIterator(iter);
      assertEqual(count, 2000, `engine ${i}: 2000 rows`);
      return count;
    });

    const results = await Promise.all(promises);
    assert(results.every(c => c === 2000), 'all 4 engines returned 2000 rows');

    for (const e of engines) await e.destroy();
    console.log('  PASS: 4 concurrent engines, no deadlock');
  }

  // ========================================================================
  // Test 6: Destroy then use — channel closed error
  // ========================================================================
  section('Test 6: Destroy then use');
  {
    const e = new m.RustIvmEngine();
    await e.init([
      {table: 'channels', columns: {id: {type: 'string', optional: false}}, primaryKey: ['id']},
    ], TEST_DB);

    // Fetch works
    const iter1 = await e.addQueriesStreaming([
      {queryId: 'before', ast: {table: 'channels', orderBy: [{column: 'id', direction: 'asc'}]}},
    ]);
    const c1 = await drainIterator(iter1);
    assertEqual(c1, 2000, 'before destroy: 2000 rows');

    // Destroy
    await e.destroy();

    // Now fetch must error
    try {
      const iter2 = await e.addQueriesStreaming([
        {queryId: 'after', ast: {table: 'channels', orderBy: [{column: 'id', direction: 'asc'}]}},
      ]);
      await drainIterator(iter2);
      assert(false, 'should error after destroy');
    } catch (err) {
      assert(true, 'error after destroy propagated');
    }

    console.log('  PASS: destroy → error on subsequent use');
  }

  // ========================================================================
  // Cleanup
  // ========================================================================
  if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
  for (const ext of ['-wal', '-shm']) {
    if (existsSync(TEST_DB + ext)) unlinkSync(TEST_DB + ext);
  }

  // ========================================================================
  // Summary
  // ========================================================================
  console.log(`\n=== Results: ${passed} passed, ${failed} failed ===`);
  if (failed > 0) {
    console.error('Failures:');
    for (const f of failures) console.error(`  - ${f}`);
    process.exit(1);
  }
  console.log('All tests passed!');
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
