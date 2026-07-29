// Replay the EXACT ticketsQuery AST against the REAL sandbox data (dumped from
// postgres to /tmp/rd_*.tsv) at full scale. seed-tkt-199/seed-tkt-4 have null
// ticketType + the real workspaceId + a public/participated channel, so
// ticketsQuery (no status filter) MUST emit them. If the addon doesn't -> the
// real bug reproduced locally.
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

const dbPath = join(tmpdir(), `repro-realdata-${Date.now()}.db`);
rmSync(dbPath, { force: true });
const db = new DatabaseSync(dbPath);
db.exec(`PRAGMA journal_mode = DELETE`);
db.exec(`CREATE TABLE tickets (id TEXT PRIMARY KEY, ticketType TEXT, workspaceId TEXT, channelId TEXT, createdAt INTEGER)`);
db.exec(`CREATE TABLE channels (id TEXT PRIMARY KEY, visibility TEXT)`);
db.exec(`CREATE TABLE channel_participants (id TEXT PRIMARY KEY, channelId TEXT, userId TEXT)`);
db.exec(`CREATE TABLE ticket_assignments (id TEXT PRIMARY KEY, ticketId TEXT)`);
db.exec(`CREATE TABLE ticket_stage_eta (id TEXT PRIMARY KEY, ticketId TEXT)`);

const load = (file, sql, map) => {
  const stmt = db.prepare(sql);
  const txt = readFileSync(`/tmp/${file}`, 'utf8').trimEnd();
  if (!txt) return 0;
  let n = 0;
  for (const line of txt.split('\n')) { stmt.run(...map(line.split('\t'))); n++; }
  return n;
};
const N = (v) => (v === '__NULL__' ? null : v);
const t = load('rd_tickets.tsv', `INSERT OR IGNORE INTO tickets VALUES (?,?,?,?,?)`, (c) => [c[0], N(c[1]), c[2], c[3], Number(c[4])]);
const ch = load('rd_channels.tsv', `INSERT OR IGNORE INTO channels VALUES (?,?)`, (c) => [c[0], c[1]]);
const p = load('rd_participants.tsv', `INSERT OR IGNORE INTO channel_participants VALUES (?,?,?)`, (c) => [c[0], c[1], c[2]]);
const a = load('rd_assignments.tsv', `INSERT OR IGNORE INTO ticket_assignments VALUES (?,?)`, (c) => [c[0], c[1]]);
const e = load('rd_stageeta.tsv', `INSERT OR IGNORE INTO ticket_stage_eta VALUES (?,?)`, (c) => [c[0], c[1]]);
console.log(`loaded: tickets=${t} channels=${ch} participants=${p} assignments=${a} stageEta=${e}`);
db.close();

const engine = new addon.RustIvmEngine();
engine.init(
  [
    { table: 'tickets', columns: { id: { type: 'string', optional: false }, ticketType: { type: 'string', optional: true }, workspaceId: { type: 'string', optional: false }, channelId: { type: 'string', optional: false }, createdAt: { type: 'number', optional: false } }, primaryKey: ['id'] },
    { table: 'channels', columns: { id: { type: 'string', optional: false }, visibility: { type: 'string', optional: false } }, primaryKey: ['id'] },
    { table: 'channel_participants', columns: { id: { type: 'string', optional: false }, channelId: { type: 'string', optional: false }, userId: { type: 'string', optional: false } }, primaryKey: ['id'] },
    { table: 'ticket_assignments', columns: { id: { type: 'string', optional: false }, ticketId: { type: 'string', optional: false } }, primaryKey: ['id'] },
    { table: 'ticket_stage_eta', columns: { id: { type: 'string', optional: false }, ticketId: { type: 'string', optional: false } }, primaryKey: ['id'] },
  ],
  dbPath,
  'test',
);

const ast = JSON.parse(readFileSync('/tmp/ticketsQuery-ast.json', 'utf8'));
const iter = engine.addQueriesStreaming([{ queryId: 'q1', astJson: JSON.stringify(ast) }]);
const emitted = new Set();
let row, total = 0;
while ((row = iter.next()) != null) {
  if (row.changeType < 0) continue;
  if (row.table === 'tickets' || !row.table) {
    const id = row.rowKey?.id?.strVal ?? row.row?.id?.strVal;
    if (id) { emitted.add(id); total++; }
  }
}
console.log(`addon emitted ${total} tickets`);
for (const tk of ['seed-tkt-199', 'seed-tkt-4']) {
  console.log(`  ${tk}: ${emitted.has(tk) ? 'EMITTED (ok)' : 'MISSING <<< reproduced under-emission'}`);
}
for (const ext of ['', '-wal', '-shm']) rmSync(dbPath + ext, { force: true });
