// Reproduce ticketsByIds = tickets.where(id IN [ticketIds]) against real data.
// The ART attribution says this query under-emits seed-tkt-199 / seed-tkt-4.
import { DatabaseSync } from 'node:sqlite';
import { createRequire } from 'node:module';
import { resolve, join } from 'node:path';
import { tmpdir } from 'node:os';
import { readFileSync, rmSync, copyFileSync } from 'node:fs';
const require = createRequire(import.meta.url);
const SRC = process.env.RUST_IVM_ADDON ||
  resolve(import.meta.dirname, '..', 'napi', 'target', 'release', 'librust_ivm_napi.dylib');
const NODEPATH = join(tmpdir(), `rust-ivm-addon-${process.pid}.node`);
copyFileSync(SRC, NODEPATH);
const addon = require(NODEPATH);

const dbPath = join(tmpdir(), `repro-in-${Date.now()}.db`);
rmSync(dbPath, { force: true });
const db = new DatabaseSync(dbPath);
db.exec(`PRAGMA journal_mode = DELETE`);
db.exec(`CREATE TABLE tickets (id TEXT PRIMARY KEY, ticketType TEXT, workspaceId TEXT, channelId TEXT, createdAt INTEGER)`);
const allIds = [];
const stmt = db.prepare(`INSERT OR IGNORE INTO tickets VALUES (?,?,?,?,?)`);
for (const line of readFileSync('/tmp/rd_tickets.tsv', 'utf8').trimEnd().split('\n')) {
  const c = line.split('\t');
  stmt.run(c[0], c[1] === '__NULL__' ? null : c[1], c[2], c[3], Number(c[4]));
  allIds.push(c[0]);
}
db.close();
console.log(`loaded ${allIds.length} tickets`);

const engine = new addon.RustIvmEngine();
engine.init(
  [{ table: 'tickets', columns: { id: { type: 'string', optional: false }, ticketType: { type: 'string', optional: true }, workspaceId: { type: 'string', optional: false }, channelId: { type: 'string', optional: false }, createdAt: { type: 'number', optional: false } }, primaryKey: ['id'] }],
  dbPath, 'test',
);

function runIN(idList, label) {
  const ast = { table: 'tickets', orderBy: [['id', 'asc']], where: { type: 'simple', op: 'IN', left: { type: 'column', name: 'id' }, right: { type: 'literal', value: idList } } };
  const iter = engine.addQueriesStreaming([{ queryId: 'q_' + label, astJson: JSON.stringify(ast) }]);
  const got = new Set();
  let row;
  while ((row = iter.next()) != null) {
    if (row.changeType < 0) continue;
    const id = row.rowKey?.id?.strVal ?? row.row?.id?.strVal;
    if (id) got.add(id);
  }
  const want = idList.filter((x) => allIds.includes(x));
  const missing = want.filter((x) => !got.has(x));
  console.log(`[${label}] IN(${idList.length} ids) -> emitted ${got.size}; want ${want.length}; MISSING ${missing.length}${missing.length ? ': ' + missing.slice(0, 6).join(',') : ''}`);
  return missing;
}

// 1) small array, just the two targets
runIN(['seed-tkt-199', 'seed-tkt-4'], 'small');
// 2) the two targets + many other real ids (like the ART harness sends a big batch)
const batch = ['seed-tkt-199', 'seed-tkt-4', ...allIds.filter((x) => x.startsWith('seed-tkt-')).slice(0, 200)];
runIN([...new Set(batch)], 'seed-batch');
// 3) large mixed batch across the id space
runIN([...new Set(['seed-tkt-199', 'seed-tkt-4', ...allIds.slice(0, 500)])], 'large-mixed');
for (const ext of ['', '-wal', '-shm']) rmSync(dbPath + ext, { force: true });
