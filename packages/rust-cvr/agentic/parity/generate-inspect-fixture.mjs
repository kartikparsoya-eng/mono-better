#!/usr/bin/env node
/**
 * TS-vs-Rust differential for `CVRStore.inspectQueries`: drives the REAL TS
 * `CVRStore.inspectQueries` against a seeded desires/queries/rows set and dumps
 * the result as the golden (`inspect-fixture.json`). `tests/inspect_pg_test.rs`
 * seeds the byte-identical data, runs the Rust `CVRStore::inspect_queries`, and
 * asserts its output matches this golden — pinning the SQL semantics (LEFT JOIN,
 * rowCount via `refCounts ? queryHash`, got flag, COALESCE(ttlMs), TTL-expiry
 * filter, client filter, ordering) against the actual TS implementation.
 *
 * The seed here MUST stay identical to tests/inspect_pg_test.rs.
 *
 * Usage: TEST_CVR_PG_URI=... npx tsx generate-inspect-fixture.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import postgres from '../../../zero-cache/node_modules/postgres/src/index.js';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {CVRStore} from '../../../zero-cache/src/services/view-syncer/cvr-store.ts';
import {setupCVRTables} from '../../../zero-cache/src/services/view-syncer/schema/cvr.ts';
import {ttlClockFromNumber} from '../../../zero-cache/src/services/view-syncer/ttl-clock.ts';

const URI = process.env.TEST_CVR_PG_URI;
if (!URI) {
  console.error('TEST_CVR_PG_URI unset');
  process.exit(1);
}
const dir = path.dirname(fileURLToPath(import.meta.url));
const lc = createSilentLogContext();

const SHARD = {appID: 'roze', shardNum: 1};
const SCHEMA = 'roze_1/cvr';
const CVR_ID = 'cg-inspect';
const TASK_ID = 'inspect-task';
const TTL_CLOCK = 5_000;

// The exact TS DDL (same createTables the flush differential captures).
let capturedDDL = '';
await setupCVRTables(lc, {unsafe: sql => ((capturedDDL = sql), Promise.resolve())}, SHARD);

const db = postgres(URI, {onnotice: () => {}});

await db.unsafe(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`);
await db.unsafe(capturedDDL);

// Seed instances → rowsVersion → queries → desires → rows (byte-identical to
// tests/inspect_pg_test.rs).
await db.unsafe(`
  INSERT INTO "${SCHEMA}".instances ("clientGroupID", version, "lastActive", "ttlClock", "replicaVersion")
    VALUES ('${CVR_ID}', '01', to_timestamp(0), 0, '01');
  INSERT INTO "${SCHEMA}"."rowsVersion" ("clientGroupID", version) VALUES ('${CVR_ID}', '01');

  INSERT INTO "${SCHEMA}".queries ("clientGroupID", "queryHash", "clientAST", "queryName", "queryArgs", "patchVersion", internal, deleted)
    VALUES
    ('${CVR_ID}', 'q1', '{"table":"issues"}', NULL, NULL, '01', false, false),
    ('${CVR_ID}', 'q2', NULL, 'myQuery', '[42]', NULL, false, false),
    ('${CVR_ID}', 'q3', '{"table":"labels"}', NULL, NULL, NULL, false, false);

  INSERT INTO "${SCHEMA}".desires ("clientGroupID", "clientID", "queryHash", "patchVersion", deleted, "ttlMs", "inactivatedAtMs")
    VALUES
    ('${CVR_ID}', 'c1', 'q1', '01', false, 300000, NULL),
    ('${CVR_ID}', 'c1', 'q2', '01', false, 2000,   4000),
    ('${CVR_ID}', 'c1', 'q3', '01', false, 1000,   1000),
    ('${CVR_ID}', 'c2', 'q1', '01', false, 300000, NULL);

  INSERT INTO "${SCHEMA}".rows ("clientGroupID", schema, "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
    VALUES
    ('${CVR_ID}', 'public', 'issues', '{"id":"1"}', '01', '01', '{"q1":1}'),
    ('${CVR_ID}', 'public', 'issues', '{"id":"2"}', '01', '01', '{"q1":1}');
`);

const store = new CVRStore(lc, db, SHARD, TASK_ID, CVR_ID, e => {
  throw e;
});
const ttlClock = ttlClockFromNumber(TTL_CLOCK);

const norm = v => JSON.parse(JSON.stringify(v));
const all = norm(await store.inspectQueries(lc, ttlClock, undefined));
const filtered = norm(await store.inspectQueries(lc, ttlClock, 'c2'));

fs.writeFileSync(
  path.join(dir, 'inspect-fixture.json'),
  JSON.stringify({ttlClock: TTL_CLOCK, all, filtered}, null, 2) + '\n',
);
console.log('wrote inspect-fixture.json');
console.log('all:', JSON.stringify(all));
console.log('filtered:', JSON.stringify(filtered));
await db.end();
