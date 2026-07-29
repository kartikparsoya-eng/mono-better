// Repro: does the REAL addon (Engine over TableSource/SQLite) under-emit rows
// for an EXISTS whose subquery filters on boolean columns? Models the sandbox
// query channels.whereExists('participants', p => isClosed=false, isDeleted=false).
// Booleans stored as 0/1 integers in SQLite, declared as `boolean` columns so
// the SQLite->Value coercion (fix 5ef6b4a) is exercised.
import { DatabaseSync } from 'node:sqlite';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { rmSync } from 'node:fs';
import { resolve } from 'node:path';
const require = createRequire(import.meta.url);
// macOS: point at the locally-built dylib (cargo build --release in napi/).
// Override with RUST_IVM_ADDON if needed.
const ADDON = process.env.RUST_IVM_ADDON ||
  resolve(import.meta.dirname, '..', 'napi', 'target', 'release', 'librust_ivm_napi.dylib');
const addon = require(ADDON);

const dbPath = join(tmpdir(), `repro-bool-exists-${Date.now()}.db`);
rmSync(dbPath, { force: true });
const db = new DatabaseSync(dbPath);
db.exec(`PRAGMA journal_mode = DELETE`);
db.exec(`CREATE TABLE channels (id TEXT PRIMARY KEY, name TEXT)`);
db.exec(`CREATE TABLE participants (id TEXT PRIMARY KEY, fk TEXT, isClosed INTEGER, isDeleted INTEGER)`);
const ic = db.prepare(`INSERT INTO channels (id,name) VALUES (?,?)`);
for (const id of ['t0-a', 't0-b', 't0-c', 't0-d']) ic.run(id, `chan ${id}`);
const ip = db.prepare(`INSERT INTO participants (id,fk,isClosed,isDeleted) VALUES (?,?,?,?)`);
ip.run('p1', 't0-a', 0, 0); // a: open+undeleted -> MATCH
ip.run('p2', 't0-b', 1, 0); // b: closed        -> no
ip.run('p3', 't0-c', 0, 1); // c: deleted       -> no
ip.run('p4', 't0-d', 0, 0); // d: open+undeleted -> MATCH
db.close();

const engine = new addon.RustIvmEngine();
engine.init(
  [
    { table: 'channels', columns: { id: { type: 'string', optional: false }, name: { type: 'string', optional: false } }, primaryKey: ['id'] },
    { table: 'participants', columns: { id: { type: 'string', optional: false }, fk: { type: 'string', optional: false }, isClosed: { type: 'boolean', optional: false }, isDeleted: { type: 'boolean', optional: false } }, primaryKey: ['id'] },
  ],
  dbPath,
  'test',
);
if (engine.setDatabasePath) { try { engine.setDatabasePath(dbPath); } catch {} }

const ast = {
  table: 'channels',
  orderBy: [['id', 'asc']],
  where: {
    type: 'correlatedSubquery',
    op: 'EXISTS',
    related: {
      correlation: { parentField: ['id'], childField: ['fk'] },
      subquery: {
        table: 'participants',
        alias: 'zsubq_participants',
        orderBy: [['id', 'asc']],
        where: {
          type: 'and',
          conditions: [
            { type: 'simple', op: '=', left: { type: 'column', name: 'isClosed' }, right: { type: 'literal', value: false } },
            { type: 'simple', op: '=', left: { type: 'column', name: 'isDeleted' }, right: { type: 'literal', value: false } },
          ],
        },
      },
    },
  },
};

const iter = engine.addQueriesStreaming([{ queryId: 'q1', astJson: JSON.stringify(ast) }]);
const ids = [];
let row;
while ((row = iter.next()) != null) {
  if (row.changeType < 0) continue; // header/reset rows
  if (row.table === 'channels' || !row.table) {
    const id = row.rowKey?.id?.strVal ?? row.row?.id?.strVal;
    if (id) ids.push(id);
  }
}
console.log('addon emitted channel ids:', ids.sort());
console.log('EXPECTED (TS oracle):        [ t0-a, t0-d ]');
console.log(ids.length === 2 && ids.includes('t0-a') && ids.includes('t0-d')
  ? 'RESULT: MATCH — boolean-EXISTS on TableSource is correct (bug NOT here)'
  : `RESULT: DIVERGE — reproduced under-emission on TableSource boolean-EXISTS! got=${JSON.stringify(ids)}`);
for (const ext of ['', '-wal', '-shm']) rmSync(dbPath + ext, { force: true });
