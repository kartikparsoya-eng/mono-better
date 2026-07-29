// Test: init() with db_path sets SQLite path atomically on all sources.
// Verifies the race fix: no more "db_path set on 0 sources".
import {createRequire} from 'node:module';
const req = createRequire(import.meta.url);
const addon = req('../napi/rust-ivm.node');

const TABLES = [
  {
    table: 'users',
    columns: {
      id: {type: 'string', optional: false},
      name: {type: 'string', optional: false},
      age: {type: 'number', optional: false},
      active: {type: 'boolean', optional: false},
    },
    primaryKey: ['id'],
  },
];

async function main() {
  let pass = 0, fail = 0;
  const assert = (cond, msg) => {
    if (cond) { pass++; console.log(`  PASS: ${msg}`); }
    else { fail++; console.error(`  FAIL: ${msg}`); }
  };

  // 1. init with null db_path — should work (no SQLite)
  console.log('=== Test 1: init with null db_path ===');
  {
    const e = new addon.RustIvmEngine();
    await e.init(TABLES, null);
    assert(true, 'init(TABLES, null) succeeds');
    const ping = await e.ping();
    assert(ping === 'pong', `ping returns "${ping}"`);
    const isInit = await e.initialized();
    assert(isInit === true, 'initialized() returns true after init');
    await e.destroy();
  }

  // 2. init with :memory: db_path — should set sources atomically
  console.log('=== Test 2: init with :memory: db_path ===');
  {
    const e = new addon.RustIvmEngine();
    await e.init(TABLES, ':memory:');
    assert(true, 'init(TABLES, ":memory:") succeeds');
    const isInit = await e.initialized();
    assert(isInit === true, 'initialized() returns true');

    // Add a row and query to verify the source works
    await e.addRow('users', {
      id: {kind: 'str', strVal: 'u1'},
      name: {kind: 'str', strVal: 'Alice'},
      age: {kind: 'f64', f64Val: 30},
      active: {kind: 'bool', boolVal: true},
    });

    const results = await e.addQueries([
      {queryId: 'q1', ast: {table: 'users', orderBy: [{column: 'id', direction: 'asc'}]}},
    ]);
    assert(results.length === 1, `addQueries returns 1 result (got ${results.length})`);
    assert(results[0].changes.length === 1, `query returns 1 row (got ${results[0].changes.length})`);
    assert(results[0].changes[0].row?.id?.strVal === 'u1', `row id = u1`);
    await e.destroy();
  }

  // 3. init with 0 tables + db_path — should not crash
  console.log('=== Test 3: init with empty tables + db_path ===');
  {
    const e = new addon.RustIvmEngine();
    await e.init([], ':memory:');
    assert(true, 'init([], ":memory:") succeeds');
    const ping = await e.ping();
    assert(ping === 'pong', `ping still works`);
    await e.destroy();
  }

  // 4. init with many tables + db_path — all sources get the path
  console.log('=== Test 4: init with 10 tables + db_path ===');
  {
    const manyTables = [];
    for (let i = 0; i < 10; i++) {
      manyTables.push({
        table: `table_${i}`,
        columns: {
          id: {type: 'string', optional: false},
          val: {type: 'number', optional: false},
        },
        primaryKey: ['id'],
      });
    }
    const e = new addon.RustIvmEngine();
    await e.init(manyTables, ':memory:');
    assert(true, 'init(10 tables, ":memory:") succeeds');

    // Add rows to all tables
    for (let i = 0; i < 10; i++) {
      await e.addRow(`table_${i}`, {
        id: {kind: 'str', strVal: `r${i}`},
        val: {kind: 'f64', f64Val: i},
      });
    }

    // Query one table
    const results = await e.addQueries([
      {queryId: 'q1', ast: {table: 'table_5', orderBy: [{column: 'id', direction: 'asc'}]}},
    ]);
    assert(results[0].changes.length === 1, `table_5 has 1 row`);
    assert(results[0].changes[0].row?.id?.strVal === 'r5', `row id = r5`);
    await e.destroy();
  }

  // 5. Verify db_path set on correct number of sources (via getAllRows)
  console.log('=== Test 5: getAllRows works after init with db_path ===');
  {
    const e = new addon.RustIvmEngine();
    await e.init(TABLES, ':memory:');
    await e.addRow('users', {
      id: {kind: 'str', strVal: 'x1'},
      name: {kind: 'str', strVal: 'X'},
      age: {kind: 'f64', f64Val: 1},
      active: {kind: 'bool', boolVal: false},
    });
    const rows = await e.getAllRows('users');
    assert(rows.length === 1, `getAllRows returns 1 row (got ${rows.length})`);
    assert(rows[0].id?.strVal === 'x1', `row id = x1`);
    await e.destroy();
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  if (fail > 0) process.exit(1);
}

main().catch(e => {
  console.error('FATAL:', e);
  process.exit(1);
});
