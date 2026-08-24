#!/usr/bin/env node
/**
 * Seed-parameterized TS-vs-Rust differential for `CVRStore.inspectQueries`.
 *
 * Defines several scenarios (varying TTL-expiry boundaries, custom vs crud
 * queries, got/not-got, multiple clients, client filters) and drives the REAL TS
 * `CVRStore.inspectQueries` over each, emitting a SELF-CONTAINED golden: every
 * scenario carries its own `seedSql` plus the expected results per client filter.
 * `tests/inspect_pg_test.rs` replays the embedded `seedSql` verbatim and runs the
 * Rust `inspect_queries`, so the seed data lives in exactly ONE place (here) and
 * cannot drift between the two languages.
 *
 * Regenerate with:
 *   TEST_CVR_PG_URI=... npx tsx generate-inspect-fixture.mjs
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

const q = s => s.replaceAll('$S', `"${SCHEMA}"`).replaceAll('$C', `'${CVR_ID}'`);

// Each scenario: a seed (applied to a freshly-created schema) + the TTL clock +
// the client filters to inspect under (null = all clients). Filters use the
// string 'null' key in results for the unfiltered pass.
const SCENARIOS = [
  {
    // Base: a got crud query, a not-got custom query, and a TTL-expired query,
    // plus a second client to exercise the client filter.
    name: 'base',
    ttlClock: 5_000,
    filters: [null, 'c2'],
    seedSql: q(`
      INSERT INTO $S.instances ("clientGroupID", version, "lastActive", "ttlClock", "replicaVersion")
        VALUES ($C, '01', to_timestamp(0), 0, '01');
      INSERT INTO $S."rowsVersion" ("clientGroupID", version) VALUES ($C, '01');
      INSERT INTO $S.queries ("clientGroupID", "queryHash", "clientAST", "queryName", "queryArgs", "patchVersion", internal, deleted)
        VALUES
        ($C, 'q1', '{"table":"issues"}', NULL, NULL, '01', false, false),
        ($C, 'q2', NULL, 'myQuery', '[42]', NULL, false, false),
        ($C, 'q3', '{"table":"labels"}', NULL, NULL, NULL, false, false);
      INSERT INTO $S.desires ("clientGroupID", "clientID", "queryHash", "patchVersion", deleted, "ttlMs", "inactivatedAtMs")
        VALUES
        ($C, 'c1', 'q1', '01', false, 300000, NULL),
        ($C, 'c1', 'q2', '01', false, 2000,   4000),
        ($C, 'c1', 'q3', '01', false, 1000,   1000),
        ($C, 'c2', 'q1', '01', false, 300000, NULL);
      INSERT INTO $S.rows ("clientGroupID", schema, "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
        VALUES
        ($C, 'public', 'issues', '{"id":"1"}', '01', '01', '{"q1":1}'),
        ($C, 'public', 'issues', '{"id":"2"}', '01', '01', '{"q1":1}');
    `),
  },
  {
    // TTL-expiry boundary: three inactivated desires whose (inactivatedAtMs +
    // ttlMs) lands just below, exactly at, and just above the TTL clock — pinning
    // the `<=` vs `<` boundary in the expiry filter.
    name: 'ttl-boundary',
    ttlClock: 6_000,
    filters: [null],
    seedSql: q(`
      INSERT INTO $S.instances ("clientGroupID", version, "lastActive", "ttlClock", "replicaVersion")
        VALUES ($C, '01', to_timestamp(0), 0, '01');
      INSERT INTO $S."rowsVersion" ("clientGroupID", version) VALUES ($C, '01');
      INSERT INTO $S.queries ("clientGroupID", "queryHash", "clientAST", "queryName", "queryArgs", "patchVersion", internal, deleted)
        VALUES
        ($C, 'below', '{"table":"a"}', NULL, NULL, '01', false, false),
        ($C, 'at',    '{"table":"b"}', NULL, NULL, '01', false, false),
        ($C, 'above', '{"table":"c"}', NULL, NULL, '01', false, false),
        ($C, 'active','{"table":"d"}', NULL, NULL, '01', false, false);
      INSERT INTO $S.desires ("clientGroupID", "clientID", "queryHash", "patchVersion", deleted, "ttlMs", "inactivatedAtMs")
        VALUES
        ($C, 'c1', 'below',  '01', false, 1000, 4000),   -- 5000 <  6000 -> filtered
        ($C, 'c1', 'at',     '01', false, 1000, 5000),   -- 6000 <= 6000 -> filtered
        ($C, 'c1', 'above',  '01', false, 1000, 5500),   -- 6500 >  6000 -> kept
        ($C, 'c1', 'active', '01', false, 300000, NULL); -- never inactivated -> kept
    `),
  },
  {
    // Custom-vs-crud + multiple clients + a client with no rows. Also a
    // client-filter that matches nothing (empty result).
    name: 'custom-crud-multi',
    ttlClock: 5_000,
    filters: [null, 'c1', 'nobody'],
    seedSql: q(`
      INSERT INTO $S.instances ("clientGroupID", version, "lastActive", "ttlClock", "replicaVersion")
        VALUES ($C, '01', to_timestamp(0), 0, '01');
      INSERT INTO $S."rowsVersion" ("clientGroupID", version) VALUES ($C, '01');
      INSERT INTO $S.queries ("clientGroupID", "queryHash", "clientAST", "queryName", "queryArgs", "patchVersion", internal, deleted)
        VALUES
        ($C, 'crudGot',   '{"table":"issues"}', NULL,       NULL,        '02', false, false),
        ($C, 'crudUngot', '{"table":"labels"}', NULL,       NULL,        NULL, false, false),
        ($C, 'customGot', NULL,                 'namedA',   '[1,"x"]',   '02', false, false),
        ($C, 'customUngot', NULL,               'namedB',   '[]',        NULL, false, false),
        ($C, 'internalQ', '{"table":"z"}',      NULL,       NULL,        '01', true,  false);
      INSERT INTO $S.desires ("clientGroupID", "clientID", "queryHash", "patchVersion", deleted, "ttlMs", "inactivatedAtMs")
        VALUES
        ($C, 'c1', 'crudGot',     '02', false, NULL,   NULL),
        ($C, 'c1', 'customGot',   '02', false, 100000, NULL),
        ($C, 'c2', 'crudUngot',   '01', false, 100000, NULL),
        ($C, 'c2', 'customUngot', '01', false, NULL,   NULL);
      INSERT INTO $S.rows ("clientGroupID", schema, "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
        VALUES
        ($C, 'public', 'issues', '{"id":"1"}', '02', '02', '{"crudGot":1}'),
        ($C, 'public', 'mutations', '{"id":"m1"}', '02', '02', '{"customGot":2}');
    `),
  },
];

// Capture the exact TS DDL (createTables) once.
let ddl = '';
await setupCVRTables(lc, {unsafe: sql => ((ddl = sql), Promise.resolve())}, SHARD);

const db = postgres(URI, {onnotice: () => {}});
const norm = v => JSON.parse(JSON.stringify(v));

const out = {scenarios: []};
for (const s of SCENARIOS) {
  await db.unsafe(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`);
  await db.unsafe(ddl);
  await db.unsafe(s.seedSql);

  const store = new CVRStore(lc, db, SHARD, TASK_ID, CVR_ID, e => {
    throw e;
  });
  const ttlClock = ttlClockFromNumber(s.ttlClock);

  const results = {};
  for (const f of s.filters) {
    results[f === null ? 'null' : f] = norm(
      await store.inspectQueries(lc, ttlClock, f ?? undefined),
    );
  }
  out.scenarios.push({
    name: s.name,
    ttlClock: s.ttlClock,
    filters: s.filters,
    seedSql: s.seedSql,
    results,
  });
}

fs.writeFileSync(
  path.join(dir, 'inspect-fixture.json'),
  JSON.stringify(out, null, 2) + '\n',
);
console.log(`wrote inspect-fixture.json (${out.scenarios.length} scenarios)`);
for (const s of out.scenarios) {
  console.log(`  ${s.name}: ${s.filters.length} filter(s)`);
}
await db.end();
