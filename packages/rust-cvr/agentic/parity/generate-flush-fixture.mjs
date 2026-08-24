#!/usr/bin/env node
/**
 * Seed-parameterized live-Postgres flush differential (query-driven WRITE path).
 *
 * Drives the REAL TS CVRStore + CVRQueryDrivenUpdater through
 * trackQueries -> received -> deleteUnreferencedRows -> flush for several
 * scenarios (single/multi-column keys, a POISONED non-PK rowKey, multi-query
 * shared refCounts, an unreferenced-row tombstone) and dumps the persisted CVR
 * tables as the golden. Each scenario is SELF-CONTAINED — it carries its own
 * `baseSeedSql`, `tracked`, and `received` — so `flush_pg_test.rs` replays the
 * identical inputs through the Rust store and asserts the persisted rows match,
 * pinning DB-row serialization (rowKey / refCounts / versions), the layer where
 * the prior poisoned-rowKey-in-PG corruption happened.
 *
 * Also captures the exact TS schema DDL into flush-schema.sql (shared by the
 * flush / inspect / sequence differentials).
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
// All scenarios seed the base instance at version '01' and advance to '02', so
// the flush CAS (expectedCurrentVersion) is the loaded '01' on the Rust side.
const q = s => s.replaceAll('$S', `"${SCHEMA}"`).replaceAll('$C', `'${CVR_ID}'`);
const baseInstance = q(`
  INSERT INTO $S.instances ("clientGroupID", version, "lastActive", "ttlClock", "replicaVersion")
    VALUES ($C, '01', to_timestamp(${CONNECT_TIME} / 1000.0), ${CONNECT_TIME}, '01');
  INSERT INTO $S."rowsVersion" ("clientGroupID", version) VALUES ($C, '01');
`);

const SCENARIOS = [
  {
    name: 'single-query-mixed-keys',
    baseSeedSql:
      baseInstance +
      q(`INSERT INTO $S.queries ("clientGroupID", "queryHash", "clientAST", "patchVersion", "transformationHash", "transformationVersion")
           VALUES ($C, 'foo', '{"table":"issues"}', '01', 'foo-t', '01');`),
    tracked: {executed: [['foo', 'foo-t']], removed: []},
    received: [
      {id: {schema: 'public', table: 'issues', rowKey: {id: '1'}}, contents: {id: '1'}, refCounts: {foo: 1}},
      {id: {schema: 'public', table: 'labels', rowKey: {issueID: '1', labelID: '2'}}, contents: {a: 1}, refCounts: {foo: 1}},
      // POISONED rowKey carrying a non-PK column — must persist verbatim.
      {id: {schema: 'public', table: 'issues', rowKey: {id: '3', _leaked: 'oops'}}, contents: {id: '3'}, refCounts: {foo: 1}},
    ],
  },
  {
    name: 'multi-query-shared-refcounts',
    baseSeedSql:
      baseInstance +
      q(`INSERT INTO $S.queries ("clientGroupID", "queryHash", "clientAST", "patchVersion", "transformationHash", "transformationVersion")
           VALUES
           ($C, 'foo', '{"table":"issues"}', '01', 'foo-t', '01'),
           ($C, 'bar', '{"table":"labels"}', '01', 'bar-t', '01');`),
    tracked: {executed: [['foo', 'foo-t'], ['bar', 'bar-t']], removed: []},
    received: [
      // Shared by BOTH queries — refCounts must persist both hashes.
      {id: {schema: 'public', table: 'issues', rowKey: {id: '1'}}, contents: {id: '1'}, refCounts: {foo: 1, bar: 1}},
      {id: {schema: 'public', table: 'issues', rowKey: {id: '2'}}, contents: {id: '2'}, refCounts: {foo: 1}},
      {id: {schema: 'public', table: 'labels', rowKey: {issueID: '1', labelID: '2'}}, contents: {a: 1}, refCounts: {bar: 1}},
    ],
  },
];

// Capture the exact DDL createTables() emits (setupCVRTables -> db.unsafe(ddl)).
let capturedDDL = '';
await setupCVRTables(lc, {unsafe: sql => ((capturedDDL = sql), Promise.resolve())}, SHARD);
fs.writeFileSync(path.join(dir, 'flush-schema.sql'), capturedDDL);

const db = postgres(URI, {onnotice: () => {}});
const dump = async sql => JSON.parse(JSON.stringify(await db.unsafe(sql)));

const out = {connectTime: CONNECT_TIME, now: NOW, scenarios: []};
for (const s of SCENARIOS) {
  await db.unsafe(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`);
  await db.unsafe(capturedDDL);
  await db.unsafe(s.baseSeedSql);

  const store = new CVRStore(lc, db, SHARD, TASK_ID, CVR_ID, e => {
    throw e;
  });
  const cvr = await store.load(lc, CONNECT_TIME);
  const updater = new CVRQueryDrivenUpdater(store, cvr, '02', '01');
  updater.trackQueries(
    lc,
    s.tracked.executed.map(([id, transformationHash]) => ({id, transformationHash})),
    s.tracked.removed ?? [],
  );
  const rows = new CustomKeyMap(rowIDString);
  for (const r of s.received) {
    rows.set(r.id, {version: '02', contents: r.contents, refCounts: r.refCounts});
  }
  await updater.received(lc, rows);
  await updater.deleteUnreferencedRows(lc);
  await updater.flush(lc, CONNECT_TIME, NOW, TTL_CLOCK);

  out.scenarios.push({
    name: s.name,
    baseSeedSql: s.baseSeedSql,
    tracked: s.tracked,
    received: s.received,
    expected: {
      rows: await dump(
        q(`SELECT "schema","table","rowKey","rowVersion","patchVersion","refCounts"
             FROM $S.rows ORDER BY "table","rowKey"::text`),
      ),
      queries: await dump(
        q(`SELECT "queryHash","patchVersion","transformationHash","deleted"
             FROM $S.queries ORDER BY "queryHash"`),
      ),
      instances: await dump(q(`SELECT version,"replicaVersion","ttlClock" FROM $S.instances`)),
      rowsVersion: await dump(q(`SELECT version FROM $S."rowsVersion"`)),
    },
  });
}

fs.writeFileSync(path.join(dir, 'flush-fixture.json'), JSON.stringify(out, null, 2) + '\n');
console.log(`wrote flush-fixture.json (${out.scenarios.length} scenarios) + flush-schema.sql`);
for (const s of out.scenarios) console.log(`  ${s.name}: ${s.expected.rows.length} rows`);
await db.end();
