// Repro of the ART residual: tickets WHERE EXISTS(channels ch WHERE ch.id=fk AND
// (ch.visibility='PUBLIC' OR EXISTS(channel_participants cp WHERE cp.channelId=ch.id
// AND cp.userId=U))). This is the ticketsQuery discriminator — a nested
// EXISTS-within-OR-within-EXISTS (the shape simple repros lacked). Under SQL truth
// every ticket whose channel is PUBLIC (or user-participated) must be emitted.
import { DatabaseSync } from 'node:sqlite';
import { createRequire } from 'node:module';
import { resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { rmSync, copyFileSync } from 'node:fs';
const require = createRequire(import.meta.url);
const SRC = process.env.RUST_IVM_ADDON ||
  resolve(import.meta.dirname, '..', 'napi', 'target', 'release', 'librust_ivm_napi.dylib');
// require needs a .node extension to treat it as a native addon.
const NODEPATH = join(tmpdir(), `rust-ivm-addon-${process.pid}.node`);
copyFileSync(SRC, NODEPATH);
const addon = require(NODEPATH);

const dbPath = join(tmpdir(), `repro-nested-${Date.now()}.db`);
rmSync(dbPath, { force: true });
const db = new DatabaseSync(dbPath);
db.exec(`PRAGMA journal_mode = DELETE`);
db.exec(`CREATE TABLE tickets (id TEXT PRIMARY KEY, channelId TEXT, workspaceId TEXT, ticketType TEXT)`);
db.exec(`CREATE TABLE channels (id TEXT PRIMARY KEY, visibility TEXT)`);
db.exec(`CREATE TABLE channel_participants (id TEXT PRIMARY KEY, channelId TEXT, userId TEXT)`);
db.exec(`CREATE TABLE ticket_assignments (id TEXT PRIMARY KEY, ticketId TEXT)`);
db.exec(`CREATE TABLE ticket_stage_eta (id TEXT PRIMARY KEY, ticketId TEXT)`);
// Scale: ~2000 channels, 200 tickets, many participants — matching the sandbox.
const insCh = db.prepare(`INSERT INTO channels VALUES (?,?)`);
const insCp = db.prepare(`INSERT INTO channel_participants VALUES (?,?,?)`);
for (let i = 0; i < 2000; i++) {
  insCh.run(`bulk-ch-${i}`, i % 2 ? 'PRIVATE' : 'PUBLIC');
  if (i % 3 === 0) insCp.run(`bulk-cp-${i}`, `bulk-ch-${i}`, `other-${i}`);
}
const insTk = db.prepare(`INSERT INTO tickets VALUES (?,?,?,?)`);
for (let i = 0; i < 197; i++) insTk.run(`bulk-tk-${i}`, `bulk-ch-${i}`, 'WS', null);
// The three targets (like seed-tkt-199: real ws, public channel):
insTk.run('tk-pub', 'ch-pub', 'WS', null);   // public + right ws -> MATCH
insTk.run('tk-priv', 'ch-priv', 'WS', null);  // private+participant -> MATCH
insTk.run('tk-none', 'ch-none', 'WS', null);  // no access -> no
insCh.run('ch-pub', 'PUBLIC');
insCh.run('ch-priv', 'PRIVATE');
insCh.run('ch-none', 'PRIVATE');
insCp.run('cp-1', 'ch-priv', 'U');
db.close();

const engine = new addon.RustIvmEngine();
engine.init(
  [
    { table: 'tickets', columns: { id: { type: 'string', optional: false }, channelId: { type: 'string', optional: false }, workspaceId: { type: 'string', optional: false }, ticketType: { type: 'string', optional: true } }, primaryKey: ['id'] },
    { table: 'channels', columns: { id: { type: 'string', optional: false }, visibility: { type: 'string', optional: false } }, primaryKey: ['id'] },
    { table: 'channel_participants', columns: { id: { type: 'string', optional: false }, channelId: { type: 'string', optional: false }, userId: { type: 'string', optional: false } }, primaryKey: ['id'] },
    { table: 'ticket_assignments', columns: { id: { type: 'string', optional: false }, ticketId: { type: 'string', optional: false } }, primaryKey: ['id'] },
    { table: 'ticket_stage_eta', columns: { id: { type: 'string', optional: false }, ticketId: { type: 'string', optional: false } }, primaryKey: ['id'] },
  ],
  dbPath,
  'test',
);

const ast = {
  table: 'tickets',
  orderBy: [['id', 'asc']],
  related: [
    { system: 'client', correlation: { parentField: ['id'], childField: ['ticketId'] }, subquery: { table: 'ticket_assignments', alias: 'assignments', orderBy: [['id', 'asc']] } },
    { system: 'client', correlation: { parentField: ['id'], childField: ['ticketId'] }, subquery: { table: 'ticket_stage_eta', alias: 'stageEtaEntries', orderBy: [['id', 'asc']] } },
  ],
  where: {
    type: 'and',
    conditions: [
      { type: 'or', conditions: [
        { type: 'simple', op: 'IS', left: { type: 'column', name: 'ticketType' }, right: { type: 'literal', value: null } },
        { type: 'simple', op: '!=', left: { type: 'column', name: 'ticketType' }, right: { type: 'literal', value: 'Support' } },
      ] },
      { type: 'simple', op: '=', left: { type: 'column', name: 'workspaceId' }, right: { type: 'literal', value: 'WS' } },
      {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          system: 'client',
          correlation: { parentField: ['channelId'], childField: ['id'] },
          subquery: {
            table: 'channels',
            alias: 'zsubq_channel',
            orderBy: [['id', 'asc']],
            where: {
              type: 'or',
              conditions: [
                { type: 'simple', op: '=', left: { type: 'column', name: 'visibility' }, right: { type: 'literal', value: 'PUBLIC' } },
                {
                  type: 'correlatedSubquery',
                  op: 'EXISTS',
                  related: {
                    system: 'client',
                    correlation: { parentField: ['id'], childField: ['channelId'] },
                    subquery: {
                      table: 'channel_participants',
                      alias: 'zsubq_participants',
                      orderBy: [['id', 'asc']],
                      where: { type: 'simple', op: '=', left: { type: 'column', name: 'userId' }, right: { type: 'literal', value: 'U' } },
                    },
                  },
                },
              ],
            },
          },
        },
      },
    ],
  },
};

// MULTI-QUERY: like the sandbox (~50 queries/client sharing sources). Several
// other queries that also read channels / channel_participants (shared sources)
// are built in the SAME call BEFORE q1 hydrates (Phase 1 builds all, then
// Phase 2 hydrates). If building them clobbers q1's shared source wiring, q1
// under-emits.
const channelsQ = { table: 'channels', orderBy: [['id', 'asc']] };
const participantsQ = { table: 'channel_participants', orderBy: [['id', 'asc']] };
const channelsExistsQ = {
  table: 'channels', orderBy: [['id', 'asc']],
  where: {
    type: 'correlatedSubquery', op: 'EXISTS',
    related: { system: 'client', correlation: { parentField: ['id'], childField: ['channelId'] },
      subquery: { table: 'channel_participants', alias: 'zsubq_participants', orderBy: [['id', 'asc']],
        where: { type: 'simple', op: '=', left: { type: 'column', name: 'userId' }, right: { type: 'literal', value: 'U' } } } },
  },
};
const queries = [
  { queryId: 'chans', astJson: JSON.stringify(channelsQ) },
  { queryId: 'parts', astJson: JSON.stringify(participantsQ) },
  { queryId: 'chanExists', astJson: JSON.stringify(channelsExistsQ) },
  { queryId: 'q1', astJson: JSON.stringify(ast) },        // target ticketsQuery
  { queryId: 'chans2', astJson: JSON.stringify(channelsQ) },
  { queryId: 'chanExists2', astJson: JSON.stringify(channelsExistsQ) },
];
const iter = engine.addQueriesStreaming(queries);
const ids = [];
let row;
while ((row = iter.next()) != null) {
  if (row.changeType < 0) continue;
  if (row.queryId === 'q1' && (row.table === 'tickets' || !row.table)) {
    const id = row.rowKey?.id?.strVal ?? row.row?.id?.strVal;
    if (id) ids.push(id);
  }
}
const targets = ['tk-pub', 'tk-priv'];
const missing = targets.filter((x) => !ids.includes(x));
const wrongInclude = ids.includes('tk-none');
console.log(`addon emitted ${ids.length} tickets; targets present: ${targets.filter(x=>ids.includes(x))}`);
console.log('EXPECTED: tk-pub + tk-priv present (public/participated channels), tk-none absent');
console.log(missing.length === 0 && !wrongInclude
  ? 'RESULT: MATCH — correct at scale (bug NOT here)'
  : `RESULT: DIVERGE — reproduced! missing=${JSON.stringify(missing)} wrongTkNone=${wrongInclude}`);
for (const ext of ['', '-wal', '-shm']) rmSync(dbPath + ext, { force: true });
