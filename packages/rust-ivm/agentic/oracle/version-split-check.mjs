// Verifies, through the REAL napi addon, that the engine splits `_0_version`
// out of row contents and reports it in `version`.
//
// The advance fixtures cannot cover this: they pass `_0_version` to the engine
// but their comparison deliberately ignores that column, so they confirm the
// OTHER columns are unaffected and nothing more.
//
// Run: node agentic/oracle/version-split-check.mjs

import {createRequire} from 'node:module';
import {existsSync, copyFileSync, rmSync} from 'node:fs';
import {resolve, join} from 'node:path';
import {tmpdir} from 'node:os';

const require = createRequire(import.meta.url);
// The replica must be in `wal2` mode, which Node's bundled SQLite cannot do —
// use the same wal2-linked build the other oracle runners use.
const zqliteRequire = createRequire(
  resolve(import.meta.dirname, '../../../zqlite/package.json'),
);
const SQLiteDatabase = zqliteRequire('@rocicorp/zero-sqlite3');
const NAPI = resolve(import.meta.dirname, '../../napi');
const candidates =
  process.platform === 'darwin'
    ? [
        resolve(NAPI, 'target/release/librust_ivm_napi.dylib'),
        resolve(NAPI, 'rust-ivm.node'),
      ]
    : [
        resolve(NAPI, 'rust-ivm.node'),
        resolve(NAPI, 'target/release/librust_ivm_napi.so'),
      ];
const addonPath = candidates.find(existsSync);
if (!addonPath) {
  // Exit 2 means "cannot run here", distinct from exit 1 ("check failed").
  // The cargo test wrapper turns this into a skip so a checkout without a
  // built addon does not report a false failure.
  console.error(
    `napi addon not found; build it with: cd napi && cargo build --release\ntried:\n  ${candidates.join('\n  ')}`,
  );
  process.exit(2);
}
let nodePath = addonPath;
if (!addonPath.endsWith('.node')) {
  nodePath = join(tmpdir(), `rust-ivm-vsplit-${process.pid}.node`);
  copyFileSync(addonPath, nodePath);
}
const addon = require(nodePath);

const dbPath = join(tmpdir(), `rust-ivm-vsplit-${process.pid}.db`);
for (const s of ['', '-wal', '-wal2', '-shm']) rmSync(dbPath + s, {force: true});

const db = new SQLiteDatabase(dbPath);
db.pragma('journal_mode = wal2');
db.exec(
  'CREATE TABLE "_zero.replicationState" (stateVersion TEXT NOT NULL, lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1))',
);
db.exec("INSERT INTO \"_zero.replicationState\" (stateVersion) VALUES ('0')");
db.exec(
  'CREATE TABLE "_zero.changeLog2" ("stateVersion" TEXT NOT NULL, "pos" INT NOT NULL, "table" TEXT NOT NULL, "rowKey" TEXT NOT NULL, "op" TEXT NOT NULL, PRIMARY KEY("stateVersion","pos"), UNIQUE("table","rowKey"))',
);
db.exec(
  'CREATE TABLE issue ("id" TEXT PRIMARY KEY, "title" TEXT, "num" INTEGER, "_0_version" TEXT NOT NULL)',
);
const ins = db.prepare(
  'INSERT INTO issue (id,title,num,_0_version) VALUES (?,?,?,?)',
);
ins.run('i1', 'first', 1, '01aa');
ins.run('i2', 'second', 2, '01bb');
db.close();

const specs = [
  {
    table: 'issue',
    columns: {
      id: {type: 'string', optional: false},
      title: {type: 'string', optional: true},
      num: {type: 'number', optional: true},
      _0_version: {type: 'string', optional: false},
    },
    primaryKey: ['id'],
    minRowVersion: '0',
  },
];

const engine = new addon.RustIvmEngine();
engine.init(specs, dbPath, 'vsplit');

const got = [];
// Credit-based backpressure: grant one credit per delivered row, as the
// production driver and the other oracle runners do.
const streamId = 1;
await engine.addQueriesStreamingRows(
  [{queryId: 'q1', astJson: JSON.stringify({table: 'issue'})}],
  (err, rc) => {
    if (err) throw err;
    if (!rc) return;
    engine.grantStreamCredit(streamId, 1);
    if (rc.changeType < 0) return;
    got.push(rc);
  },
  streamId,
);

let failures = 0;
const check = (cond, msg) => {
  if (!cond) {
    console.error('  FAIL:', msg);
    failures++;
  }
};

check(got.length === 2, `expected 2 rows, got ${got.length}`);
const byVersion = {i1: '01aa', i2: '01bb'};
for (const rc of got) {
  const key = JSON.parse(rc.rowKey).id;
  const row = JSON.parse(rc.row);
  check(
    rc.version === byVersion[key],
    `${key}: version should be ${byVersion[key]}, got ${JSON.stringify(rc.version)}`,
  );
  check(
    !('_0_version' in row),
    `${key}: _0_version must not remain in contents, got ${rc.row}`,
  );
  check(row.id === key, `${key}: id column preserved`);
  check(
    row.title === (key === 'i1' ? 'first' : 'second'),
    `${key}: title preserved, got ${JSON.stringify(row.title)}`,
  );
  check(row.num === (key === 'i1' ? 1 : 2), `${key}: num preserved`);
}

engine.destroy?.();
for (const s of ['', '-wal', '-wal2', '-shm']) rmSync(dbPath + s, {force: true});

if (failures) {
  console.error(`\nversion-split-check: ${failures} failure(s)`);
  process.exit(1);
}
console.log(
  `version-split-check: OK — ${got.length} rows, _0_version lifted into \`version\`, contents intact`,
);
