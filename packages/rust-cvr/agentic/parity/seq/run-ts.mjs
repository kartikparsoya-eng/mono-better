#!/usr/bin/env node
/**
 * TS replay driver for the CVR sequence differential.
 *
 * Reads a program (gen.mjs format) and replays it against the REAL TS CVRStore +
 * CVRConfigDrivenUpdater over TEST_CVR_PG_URI: fresh schema, then per transaction
 * load -> apply ops -> flush. Emits a canonical trace (per-txn returned patches +
 * flushed flag + resulting version + full DB dump) as JSON on stdout.
 *
 * The Rust `cvr_seq_replay` binary emits the byte-compatible trace for the same
 * program; diff.mjs / seq_diff_pg_test.rs assert they match.
 *
 * Usage: TEST_CVR_PG_URI=... npx tsx run-ts.mjs <program.json>
 *        (program on argv[2], or on stdin if omitted)
 */
import fs from 'node:fs';
import postgres from '../../../../zero-cache/node_modules/postgres/src/index.js';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {CVRStore} from '../../../../zero-cache/src/services/view-syncer/cvr-store.ts';
import {setupCVRTables} from '../../../../zero-cache/src/services/view-syncer/schema/cvr.ts';
import {ttlClockFromNumber} from '../../../../zero-cache/src/services/view-syncer/ttl-clock.ts';
import {
  CVRConfigDrivenUpdater,
  CVRQueryDrivenUpdater,
} from '../../../../zero-cache/src/services/view-syncer/cvr.ts';
import {versionString} from '../../../../zero-cache/src/services/view-syncer/schema/types.ts';
import {CustomKeyMap} from '../../../../shared/src/custom-key-map.ts';
import {rowIDString} from '../../../../zero-cache/src/types/row-key.ts';

const URI = process.env.TEST_CVR_PG_URI;
if (!URI) {
  console.error('TEST_CVR_PG_URI unset');
  process.exit(1);
}

const progText = process.argv[2]
  ? fs.readFileSync(process.argv[2], 'utf8')
  : fs.readFileSync(0, 'utf8');
const prog = JSON.parse(progText);

const lc = createSilentLogContext();
const SHARD = prog.shard;
const SCHEMA = `${SHARD.appID}_${SHARD.shardNum}/cvr`;
const CVR_ID = prog.cvrId;
const TASK_ID = 'seq-task';

// Capture the exact TS DDL (setupCVRTables -> db.unsafe(ddl)).
let ddl = '';
await setupCVRTables(lc, {unsafe: sql => ((ddl = sql), Promise.resolve())}, SHARD);

const db = postgres(URI, {onnotice: () => {}});
await db.unsafe(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`);
await db.unsafe(ddl);

const norm = v => JSON.parse(JSON.stringify(v));

// Canonical, language-neutral rendering of a returned patch. `PatchToVersion` and
// its inner `Patch` are INTERNAL types on both sides (the Rust port serializes
// them snake_case; the actual wire DTO is `QueryPatchEntry`, built separately), so
// comparing raw serde would flag naming, not semantics. This captures the
// client-facing MEANING — kind:op:id:clientID@version — which both drivers must
// agree on.
const sortKeys = v =>
  Array.isArray(v)
    ? v.map(sortKeys)
    : v && typeof v === 'object'
      ? Object.fromEntries(Object.keys(v).sort().map(k => [k, sortKeys(v[k])]))
      : v;
const canonPatch = ({patch, toVersion}) => {
  const v = versionString(toVersion);
  // id is a string (query patch) or a RowID object (row patch); sort keys so
  // object key order can't diverge from the Rust side.
  const id = patch.id === undefined ? '' : JSON.stringify(sortKeys(patch.id));
  const cid = patch.clientID ?? '';
  return `${patch.type}:${patch.op}:${id}:${cid}@${v}`;
};

// Canonical DB dump — the shared, language-neutral oracle. Both drivers read the
// same tables; diff canonicalizes arrays so ORDER BY need not match exactly.
async function dump() {
  const q = s => db.unsafe(`${s.replaceAll('$S', `"${SCHEMA}"`)}`);
  return {
    instances: norm(
      await q(
        `SELECT version, "replicaVersion", "ttlClock", "clientSchema", "profileID"
         FROM $S.instances ORDER BY version`,
      ),
    ),
    clients: norm(await q(`SELECT "clientID" FROM $S.clients ORDER BY "clientID"`)),
    queries: norm(
      await q(
        `SELECT "queryHash","clientAST","queryName","queryArgs","patchVersion",
                "transformationHash","transformationVersion","internal","deleted"
         FROM $S.queries ORDER BY "queryHash"`,
      ),
    ),
    desires: norm(
      await q(
        `SELECT "clientID","queryHash","patchVersion","deleted","ttlMs","inactivatedAtMs"
         FROM $S.desires ORDER BY "clientID","queryHash"`,
      ),
    ),
    rows: norm(
      await q(
        `SELECT "schema","table","rowKey","rowVersion","patchVersion","refCounts"
         FROM $S.rows ORDER BY "table","rowKey"::text`,
      ),
    ),
  };
}

const trace = {cvrId: CVR_ID, transactions: []};

for (const tx of prog.transactions) {
  // Fresh store per transaction: a clean load -> mutate -> flush cycle against PG
  // (mirrors the Rust binary's loop; avoids in-memory row-cache carryover).
  const store = new CVRStore(lc, db, SHARD, TASK_ID, CVR_ID, e => {
    throw e;
  });
  const cvr = await store.load(lc, prog.connectTime);
  const ttlClock = ttlClockFromNumber(tx.ttlClock);
  const patches = [];
  let result;

  if (tx.kind === 'query') {
    // ── query-driven: trackQueries -> received -> deleteUnreferencedRows ──
    const updater = new CVRQueryDrivenUpdater(
      store,
      cvr,
      tx.stateVersion,
      tx.replicaVersion ?? '01',
    );
    const {queryPatches} = updater.trackQueries(
      lc,
      tx.track.executed.map(([id, transformationHash]) => ({id, transformationHash})),
      (tx.track.removed ?? []).map(id => ({id})),
    );
    patches.push(...queryPatches);
    const rows = new CustomKeyMap(rowIDString);
    for (const r of tx.received) {
      rows.set(r.id, {version: r.version, contents: r.contents, refCounts: r.refCounts});
    }
    patches.push(...(await updater.received(lc, rows)));
    if (tx.deleteUnreferenced) {
      patches.push(...(await updater.deleteUnreferencedRows(lc)));
    }
    result = await updater.flush(lc, prog.connectTime, tx.lastActive, ttlClock);
  } else {
    // ── config-driven ──
    const updater = new CVRConfigDrivenUpdater(store, cvr, SHARD);
    for (const op of tx.ops) {
      switch (op.op) {
        case 'ensureClient':
          updater.ensureClient(op.clientID);
          break;
        case 'putDesiredQueries':
          patches.push(...updater.putDesiredQueries(op.clientID, op.queries));
          break;
        case 'markDesiredInactive':
          patches.push(
            ...updater.markDesiredQueriesAsInactive(op.clientID, op.hashes, ttlClock),
          );
          break;
        case 'deleteDesired':
          patches.push(...updater.deleteDesiredQueries(op.clientID, op.hashes));
          break;
        case 'clearDesired':
          patches.push(...updater.clearDesiredQueries(op.clientID));
          break;
        case 'deleteClient':
          patches.push(...updater.deleteClient(op.clientID, ttlClock));
          break;
        default:
          throw new Error(`unknown op ${op.op}`);
      }
    }
    result = await updater.flush(lc, prog.connectTime, tx.lastActive, ttlClock);
  }

  trace.transactions.push({
    patches: patches.map(canonPatch),
    flushed: !!result.flushed,
    version: versionString(result.cvr.version),
    db: await dump(),
  });
}

process.stdout.write(JSON.stringify(trace, null, 2) + '\n');
await db.end();
