#!/usr/bin/env node
/**
 * Live-Postgres golden generator for the catchup differential.
 *
 * Seeds a disposable Postgres with a known CVR row-set (multiple patch
 * versions, a POISONED rowKey carrying a non-PK column, a tombstone) and runs
 * the VERBATIM TS catchupRowPatches SQL (from row-record-cache.ts) for a set of
 * scenarios, capturing the emitted rows as the golden. The Rust pg-test seeds
 * the same SQL and asserts catchup_row_patches yields the identical set.
 *
 * Usage:
 *   TEST_CVR_PG_URI=postgres://... \
 *     npx tsx packages/rust-cvr/agentic/parity/generate-catchup-fixture.mjs \
 *       > packages/rust-cvr/agentic/parity/catchup-fixture.json
 */
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
// `postgres` lives in zero-cache's node_modules; node resolves bare specifiers
// from the file location, so reference the ESM entry explicitly.
import postgres from '../../../zero-cache/node_modules/postgres/src/index.js';

const URI = process.env.TEST_CVR_PG_URI;
if (!URI) {
  console.error('TEST_CVR_PG_URI is required');
  process.exit(2);
}
const dir = path.dirname(fileURLToPath(import.meta.url));
const seed = fs.readFileSync(path.join(dir, 'catchup-seed.sql'), 'utf8');

const CVRID = 'cg1';
const sql = postgres(URI, {onnotice: () => {}});

// Verbatim TS catchupRowPatches query (row-record-cache.ts). No ORDER BY, so
// the emitted set — not order — is what must match.
async function catchup(start, end, exclude) {
  if (exclude.length === 0) {
    return sql`SELECT "clientGroupID","schema","table","rowKey","rowVersion","patchVersion","refCounts"
               FROM cvr_parity.rows
               WHERE "clientGroupID" = ${CVRID}
                 AND "patchVersion" > ${start}
                 AND "patchVersion" <= ${end}`;
  }
  return sql`SELECT "clientGroupID","schema","table","rowKey","rowVersion","patchVersion","refCounts"
             FROM cvr_parity.rows
             WHERE "clientGroupID" = ${CVRID}
               AND "patchVersion" > ${start}
               AND "patchVersion" <= ${end}
               AND ("refCounts" IS NULL OR NOT "refCounts" ?| ${exclude})`;
}

// after=null => start '' (matches versionString(null) path). Versions here are
// bare stateVersions, so versionString is identity.
const SCENARIOS = [
  {name: 'full', after: null, upTo: '0c', current: '0c', exclude: []},
  {name: 'partial_base_lt_head', after: '04', upTo: '08', current: '0c', exclude: []},
  {name: 'exclude_qB', after: null, upTo: '0c', current: '0c', exclude: ['qB']},
  {name: 'version_mismatch', after: null, upTo: '0c', current: '0b', exclude: [], expectError: true},
];

const canonRow = r => ({
  clientGroupID: r.clientGroupID,
  schema: r.schema,
  table: r.table,
  rowKey: r.rowKey,
  rowVersion: r.rowVersion,
  patchVersion: r.patchVersion,
  refCounts: r.refCounts ?? null,
});
const sortKey = r => JSON.stringify([r.patchVersion, r.rowKey]);

await sql.unsafe(seed);

const out = {scenarios: []};
for (const s of SCENARIOS) {
  const rec = {...s};
  if (!s.expectError) {
    const start = s.after === null ? '' : s.after;
    const rows = (await catchup(start, s.upTo, s.exclude)).map(canonRow);
    rows.sort((a, b) => (sortKey(a) < sortKey(b) ? -1 : 1));
    rec.rows = rows;
  }
  out.scenarios.push(rec);
}

await sql.end();
console.log(JSON.stringify(out, null, 2));
