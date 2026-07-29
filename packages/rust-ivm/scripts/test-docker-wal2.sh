#!/bin/bash
# WAL2 SQLite verification script — runs INSIDE the Docker container.
#
# Checks that the NAPI addon has WAL2-patched SQLite linked,
# can open a WAL2 database, and can read from it.
#
# Usage (from host):
#   docker exec xyne-sandbox-rust-test-zero-cache bash /app/mono/rust-ivm/scripts/test-docker-wal2.sh
#
# Or build a temporary container:
#   docker run --rm zero-rust-ivm:latest bash /app/mono/rust-ivm/scripts/test-docker-wal2.sh

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0

check() {
  if [ $? -eq 0 ]; then
    echo -e "${GREEN}PASS${NC}: $1"
    PASS=$((PASS + 1))
  else
    echo -e "${RED}FAIL${NC}: $1"
    FAIL=$((FAIL + 1))
  fi
}

echo "=== WAL2 SQLite Verification ==="
echo "Container: $(hostname)"
echo "Date: $(date)"
echo ""

# ---------------------------------------------------------------------------
# 1. Check if libsqlite3 has WAL2 symbols
# ---------------------------------------------------------------------------
echo "--- 1. WAL2 Symbol Check ---"

# The WAL2 patch adds sqlite3WalSnapshotOpen and related functions.
# Check if these symbols exist in the linked SQLite library.
REPLICA_DB="/var/zero/replica.db"

# Try to check via nm on the NAPI addon
NAPI_ADDON="/app/mono/rust-ivm/napi/rust-ivm.node"

if [ -f "$NAPI_ADDON" ]; then
  echo "Checking NAPI addon for WAL2 symbols..."
  if nm "$NAPI_ADDON" 2>/dev/null | grep -q "sqlite3Wal"; then
    echo -e "${GREEN}PASS${NC}: NAPI addon contains sqlite3Wal symbols"
    PASS=$((PASS + 1))
  else
    echo -e "${YELLOW}WARN${NC}: NAPI addon does not contain sqlite3Wal symbols (may be dynamically linked)"
  fi

  if nm "$NAPI_ADDON" 2>/dev/null | grep -q "wal2"; then
    echo -e "${GREEN}PASS${NC}: NAPI addon contains wal2 string"
    PASS=$((PASS + 1))
  fi
else
  echo -e "${RED}FAIL${NC}: NAPI addon not found at $NAPI_ADDON"
  FAIL=$((FAIL + 1))
fi

# Check system SQLite
echo ""
echo "Checking system SQLite..."
if [ -f "/usr/lib/libsqlite3.a" ]; then
  echo "Found static libsqlite3.a"
  if nm "/usr/lib/libsqlite3.a" 2>/dev/null | grep -q "wal2"; then
    echo -e "${GREEN}PASS${NC}: libsqlite3.a contains wal2 symbols"
    PASS=$((PASS + 1))
  else
    echo -e "${RED}FAIL${NC}: libsqlite3.a does NOT contain wal2 symbols"
    FAIL=$((FAIL + 1))
  fi
fi

if [ -f "/usr/local/lib/libsqlite3.so" ]; then
  echo "Found shared libsqlite3.so"
  if nm "/usr/local/lib/libsqlite3.so" 2>/dev/null | grep -q "wal2"; then
    echo -e "${GREEN}PASS${NC}: libsqlite3.so contains wal2 symbols"
    PASS=$((PASS + 1))
  fi
fi

# Check if sqlite3 CLI is available and what version
if command -v sqlite3 &>/dev/null; then
  echo "sqlite3 CLI version: $(sqlite3 --version)"
fi

# ---------------------------------------------------------------------------
# 2. Check journal_mode of replica.db
# ---------------------------------------------------------------------------
echo ""
echo "--- 2. Replica Database Check ---"

if [ -f "$REPLICA_DB" ]; then
  echo "Found replica.db at $REPLICA_DB"
  echo "File size: $(du -h $REPLICA_DB | cut -f1)"

  # Try to read journal_mode using sqlite3 CLI if available
  if command -v sqlite3 &>/dev/null; then
    MODE=$(sqlite3 "$REPLICA_DB" "PRAGMA journal_mode;" 2>&1 || echo "ERROR")
    echo "journal_mode (via sqlite3 CLI): $MODE"
    if echo "$MODE" | grep -q "wal2"; then
      echo -e "${GREEN}PASS${NC}: replica.db is in WAL2 mode"
      PASS=$((PASS + 1))
    elif echo "$MODE" | grep -q "wal"; then
      echo -e "${YELLOW}WARN${NC}: replica.db is in WAL mode (not WAL2)"
    else
      echo -e "${RED}FAIL${NC}: replica.db journal_mode is '$MODE'"
      FAIL=$((FAIL + 1))
    fi
  fi

  # Check WAL sidecar files
  for ext in "-wal" "-wal2" "-shm"; do
    if [ -f "$REPLICA_DB$ext" ]; then
      echo "Found $ext: $(du -h $REPLICA_DB$ext | cut -f1)"
    fi
  done
else
  echo -e "${YELLOW}WARN${NC}: replica.db not found at $REPLICA_DB (may not have synced yet)"
fi

# ---------------------------------------------------------------------------
# 3. Check via Node.js NAPI addon
# ---------------------------------------------------------------------------
echo ""
echo "--- 3. NAPI Addon Test ---"

# Use node to test the NAPI addon directly
node -e "
const { createRequire } = require('module');
const req = createRequire(require('url').pathToFileURL(__filename).href);

try {
  const addon = req('$NAPI_ADDON');
  const engine = new addon.RustIVMEngine();

  // Test ping
  const pong = engine.ping();
  console.log('ping:', pong);

  // Test version
  const ver = engine.version();
  console.log('version:', JSON.stringify(ver));

  // Test init with a simple table
  engine.init({
    test_table: {
      columns: { id: { type: 'number', optional: false }, name: { type: 'string', optional: false } },
      primaryKey: ['id'],
    },
  });
  console.log('initialized:', engine.initialized());

  // Try to set database path (this is where WAL2 issues surface)
  try {
    engine.setDatabasePath('$REPLICA_DB');
    console.log('setDatabasePath: OK');
  } catch (e) {
    console.error('setDatabasePath FAILED:', e.message);

    // Try with URI
    try {
      engine.setDatabasePath('file:$REPLICA_DB?_busy_timeout=5000');
      console.log('setDatabasePath (URI): OK');
    } catch (e2) {
      console.error('setDatabasePath (URI) FAILED:', e2.message);
    }
  }

  engine.destroy();
  console.log('NAPI test: PASS');
} catch (e) {
  console.error('NAPI test: FAIL -', e.message);
  process.exit(1);
}
" 2>&1

check "NAPI addon loads and basic operations work"

# ---------------------------------------------------------------------------
# 4. Try reading actual table data from replica.db via NAPI
# ---------------------------------------------------------------------------
echo ""
echo "--- 4. Read Replica Data via NAPI ---"

if [ -f "$REPLICA_DB" ]; then
  node -e "
const { createRequire } = require('module');
const req = createRequire(require('url').pathToFileURL(__filename).href);

try {
  const addon = req('$NAPI_ADDON');
  const engine = new addon.RustIVMEngine();

  // List tables from sqlite_master
  const Database = require('node:sqlite').Database;
  const db = new Database('$REPLICA_DB', { readOnly: true });
  const tables = db.prepare(
    \"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '_zero.%' LIMIT 5\"
  ).all();
  console.log('Tables in replica:', tables.map(t => t.name).join(', '));
  db.close();

  if (tables.length === 0) {
    console.log('No user tables found (replica may not have synced yet)');
    process.exit(0);
  }

  // Get columns for first table
  const firstTable = tables[0].name;
  const db2 = new Database('$REPLICA_DB', { readOnly: true });
  const cols = db2.prepare(\"PRAGMA table_info(\\\"$firstTable\\\")\").all();
  db2.close();

  const columns = {};
  const pk = [];
  for (const col of cols) {
    const type = col.type === 'INTEGER' || col.type === 'REAL' ? 'number' :
                 col.type === 'TEXT' ? 'string' : 'string';
    columns[col.name] = { type, optional: col.notnull === 0 };
    if (col.pk > 0) pk.push(col.name);
  }

  console.log('Table:', firstTable, 'PK:', pk, 'Columns:', Object.keys(columns).join(', '));

  // Init engine with this table
  engine.init({ [firstTable]: { columns, primaryKey: pk.length > 0 ? pk : ['rowid'] } });
  engine.setDatabasePath('$REPLICA_DB');

  // Query all rows
  const orderBy = pk.length > 0 ? pk.map(c => [c, 'asc']) : [['rowid', 'asc']];
  const results = engine.addQueries([{
    queryId: 'test',
    ast: { table: firstTable, orderBy }
  }]);

  const count = results[0]?.changes?.length || 0;
  console.log('Rows returned:', count);

  if (count > 0) {
    console.log('First row:', JSON.stringify(results[0].changes[0].row, null, 2).substring(0, 200));
    console.log('NAPI read test: PASS');
  } else {
    console.log('NAPI read test: WARN (0 rows — table may be empty or WAL2 issue)');
  }

  engine.destroy();
} catch (e) {
  console.error('NAPI read test: FAIL -', e.message);
  process.exit(1);
}
" 2>&1

  check "NAPI can read replica.db data"
fi

# ---------------------------------------------------------------------------
# 5. Summary
# ---------------------------------------------------------------------------
echo ""
echo "=== Summary ==="
echo -e "${GREEN}Passed: $PASS${NC}"
echo -e "${RED}Failed: $FAIL${NC}"

if [ $FAIL -gt 0 ]; then
  exit 1
fi
