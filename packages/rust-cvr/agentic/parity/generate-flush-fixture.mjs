#!/usr/bin/env node
/**
 * Live-Postgres flush differential: drives the REAL TS CVRStore + CVRQueryDrivenUpdater
 * through trackQueries -> received -> flush against TEST_CVR_PG_URI, then dumps the
 * persisted CVR tables (rows / queries / instances / rowsVersion) as the golden.
 * flush_pg_test.rs replays the same operations through the Rust store and asserts the
 * persisted rows match — pinning the DB-row serialization (rowKey / refCounts / ttl /
 * versions), the layer where the prior poisoned-rowKey-in-PG corruption happened.
 *
 * Also captures the exact TS schema DDL (via createTables, intercepted through a fake
 * db.unsafe) into flush-schema.sql so the Rust test builds a byte-identical schema.
 *
 * Usage: TEST_CVR_PG_URI=... npx tsx generate-flush-fixture.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import postgres from '../../../zero-cache/node_modules/postgres/src/index.js';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {CustomKeyMap} from '../../../shared/src/custom-key-map.ts';
import {rowIDString} from '../../../zero-cache/src/types/row-key.ts';
import {CVRStore} from '../../../zero-cache/src/services/view-syncer/cvr-store.ts';
import {setupCVRTables} from '../../../zero-cache/src/services/view-syncer/schema/cvr.ts';
import {CVRQueryDrivenUpdater} from '../../../zero-cache/src/services/view-syncer/cvr.ts';

const URI = process.env.TEST_CVR_PG_URI;
if (!URI) {
  console.error('TEST_CVR_PG_URI unset');
  process.exit(1);
}
const dir = path.dirname(fileURLToPath(import.meta.url));
const lc = createSilentLogContext();

const SHARD = {appID: 'roze', shardNum: 1};
const SCHEMA = 'roze_1/cvr';
const CVR_ID = 'cg-flush';
const TASK_ID = 'flush-task';
const CONNECT_TIME = Date.UTC(2024, 8, 4);
const NOW = Date.UTC(2024, 8, 5);
const TTL_CLOCK = NOW;

// Capture the exact DDL createTables() emits (setupCVRTables -> db.unsafe(ddl)).
let capturedDDL = '';
await setupCVRTables(lc, {unsafe: sql => ((capturedDDL = sql), Promise.resolve())}, SHARD);
fs.writeFileSync(path.join(dir, 'flush-schema.sql'), capturedDDL);

const db = postgres(URI, {onnotice: () => {}});

async function reset() {
  await db.unsafe(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`);
  await db.unsafe(capturedDDL);
  // base instance (owned-by-nobody, version 01) + a query to execute + rowsVersion.
  await db.unsafe(`
    INSERT INTO "${SCHEMA}".instances ("clientGroupID", version, "lastActive", "ttlClock", "replicaVersion")
      VALUES ('${CVR_ID}', '01', to_timestamp(${CONNECT_TIME} / 1000.0), ${CONNECT_TIME}, '01');
    INSERT INTO "${SCHEMA}"."rowsVersion" ("clientGroupID", version) VALUES ('${CVR_ID}', '01');
    INSERT INTO "${SCHEMA}".queries ("clientGroupID", "queryHash", "clientAST", "patchVersion", "transformationHash", "transformationVersion")
      VALUES ('${CVR_ID}', 'foo', '{"table":"issues"}', '01', 'foo-t', '01');
  `);
}

// The received rows: single-col, multi-col, and a POISONED rowKey (non-PK column).
const RECEIVED = [
  {id: {schema: 'public', table: 'issues', rowKey: {id: '1'}}, contents: {id: '1'}, refCounts: {foo: 1}},
  {id: {schema: 'public', table: 'labels', rowKey: {issueID: '1', labelID: '2'}}, contents: {a: 1}, refCounts: {foo: 1}},
  {id: {schema: 'public', table: 'issues', rowKey: {id: '3', _leaked: 'oops'}}, contents: {id: '3'}, refCounts: {foo: 1}},
];

await reset();
const store = new CVRStore(lc, db, SHARD, TASK_ID, CVR_ID, e => {
  throw e;
});
const cvr = await store.load(lc, CONNECT_TIME);
const updater = new CVRQueryDrivenUpdater(store, cvr, '02', '01');
updater.trackQueries(lc, [{id: 'foo', transformationHash: 'foo-t'}], []);
const rows = new CustomKeyMap(rowIDString);
for (const r of RECEIVED) rows.set(r.id, {version: '02', contents: r.contents, refCounts: r.refCounts});
await updater.received(lc, rows);
await updater.deleteUnreferencedRows(lc);
await updater.flush(lc, CONNECT_TIME, NOW, TTL_CLOCK);

const dump = async sql => JSON.parse(JSON.stringify(await db.unsafe(sql)));
const fixture = {
  received: RECEIVED,
  rows: await dump(
    `SELECT "schema","table","rowKey","rowVersion","patchVersion","refCounts"
     FROM "${SCHEMA}".rows ORDER BY "table","rowKey"::text`,
  ),
  queries: await dump(
    `SELECT "queryHash","patchVersion","transformationHash","deleted"
     FROM "${SCHEMA}".queries ORDER BY "queryHash"`,
  ),
  instances: await dump(
    `SELECT version,"replicaVersion","ttlClock" FROM "${SCHEMA}".instances`,
  ),
  rowsVersion: await dump(`SELECT version FROM "${SCHEMA}"."rowsVersion"`),
};

fs.writeFileSync(path.join(dir, 'flush-fixture.json'), JSON.stringify(fixture, null, 2) + '\n');
console.log('wrote flush-fixture.json + flush-schema.sql');
console.log('rows:', JSON.stringify(fixture.rows));
await db.end();
