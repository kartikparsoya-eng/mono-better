#!/usr/bin/env node
/**
 * Deterministic program generator for the CVR *sequence* differential.
 *
 * A "program" is a language-neutral JSON description of a sequence of
 * config-driven CVR transactions. Both replay drivers — run-ts.mjs (real TS
 * CVRStore + CVRConfigDrivenUpdater) and the Rust `cvr_seq_replay` binary (real
 * Rust store + updater) — replay the SAME program against a fresh Postgres
 * schema and emit an identical trace. diff.mjs / seq_diff_pg_test.rs then assert
 * the two traces match, pinning the *stateful* surface (version + configVersion
 * transitions, per-client desired-query sets, TTL inactivation, deleteClient)
 * across many interleaved transactions — the space the fixed-scenario fixtures
 * never reach.
 *
 * Randomness lives ONLY here (a seeded mulberry32 PRNG). The drivers are pure
 * deterministic replays, so a program fully reproduces on both sides.
 *
 * Usage:
 *   node gen.mjs <seed>              # print one program to stdout
 *   node gen.mjs --corpus <N>        # (re)generate seq/corpus/prog-000.json..
 */
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const dir = path.dirname(fileURLToPath(import.meta.url));

// ── seeded PRNG (mulberry32) — identical stream for a given seed ──
function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const TABLES = ['issues', 'labels', 'comments', 'users'];
const TTLS = [undefined, 1000, 60000, 300000, -1]; // -1 = forever (TS DEFAULT semantics differ; exercise both)

// A query hash maps deterministically to a stable AST so the same hash always
// carries the same clientAST (TS keys queries by hash; a hash reused across
// clients/txns must resolve to the identical query row).
function astFor(hash) {
  const t = TABLES[[...hash].reduce((a, c) => a + c.charCodeAt(0), 0) % TABLES.length];
  // Minimal valid AST — `asQuery` validates the stored clientAST on load, so keep
  // it to the shape the fixed fixtures use ({table}); a hash's table is stable.
  return {table: t};
}

export function generate(seed) {
  const rnd = mulberry32(seed);
  const pick = arr => arr[Math.floor(rnd() * arr.length)];
  const chance = p => rnd() < p;

  // Model of the CVR we are building, so we mostly emit *valid, interesting*
  // ops (referring to live clients / their desired queries) with a minority of
  // fresh ids to exercise create paths.
  const clients = new Map(); // clientID -> Set<queryHash> (currently desired, active or inactive)
  let hashCounter = 0;
  const freshHash = () => `q${hashCounter++}`;
  let clientCounter = 0;
  const freshClient = () => `c${clientCounter++}`;

  const knownClient = () =>
    clients.size && chance(0.75)
      ? pick([...clients.keys()])
      : freshClient();

  const connectTime = 1_725_408_000_000; // Date.UTC(2024, 8, 4) — matches other fixtures
  const nTx = 2 + Math.floor(rnd() * 6); // 2..7 transactions
  const transactions = [];

  for (let i = 0; i < nTx; i++) {
    const ttlClock = connectTime + (i + 1) * 3600_000; // +1h per txn, monotonic
    const nOps = 1 + Math.floor(rnd() * 4); // 1..4 ops
    const ops = [];

    for (let j = 0; j < nOps; j++) {
      const kind = pick([
        'ensureClient',
        'putDesiredQueries',
        'putDesiredQueries', // weight query churn higher
        'markDesiredInactive',
        'deleteDesired',
        'clearDesired',
        'deleteClient',
      ]);

      if (kind === 'ensureClient') {
        const c = knownClient();
        if (!clients.has(c)) clients.set(c, new Set());
        ops.push({op: 'ensureClient', clientID: c});
      } else if (kind === 'putDesiredQueries') {
        const c = knownClient();
        if (!clients.has(c)) clients.set(c, new Set());
        const set = clients.get(c);
        const n = 1 + Math.floor(rnd() * 3);
        const queries = [];
        for (let k = 0; k < n; k++) {
          // Reuse an existing hash sometimes (reactivate / keep), else fresh.
          const reuse = set.size && chance(0.5);
          const hash = reuse ? pick([...set]) : freshHash();
          set.add(hash);
          queries.push({hash, ast: astFor(hash), ttl: pick(TTLS)});
        }
        ops.push({op: 'putDesiredQueries', clientID: c, queries});
      } else if (kind === 'markDesiredInactive') {
        const c = knownClient();
        const set = clients.get(c);
        const hashes = set && set.size ? subset(set, rnd) : [];
        ops.push({op: 'markDesiredInactive', clientID: c, hashes});
      } else if (kind === 'deleteDesired') {
        const c = knownClient();
        const set = clients.get(c);
        const hashes = set && set.size ? subset(set, rnd) : [];
        for (const h of hashes) set.delete(h);
        ops.push({op: 'deleteDesired', clientID: c, hashes});
      } else if (kind === 'clearDesired') {
        const c = knownClient();
        if (clients.has(c)) clients.get(c).clear();
        ops.push({op: 'clearDesired', clientID: c});
      } else if (kind === 'deleteClient') {
        const c = knownClient();
        clients.delete(c);
        ops.push({op: 'deleteClient', clientID: c});
      }
    }

    transactions.push({lastActive: ttlClock, ttlClock, ops});
  }

  return {
    seed,
    cvrId: `cg-seq-${String(seed).padStart(6, '0')}`,
    shard: {appID: 'roze', shardNum: 1},
    connectTime,
    transactions,
  };
}

function subset(set, rnd) {
  return [...set].filter(() => rnd() < 0.5);
}

// LexiVersion encoder (mirror of schema/types.rs `version_to_lexi`): base36(v)
// prefixed with base36(len-1). For 1..35 this is '01'..'0z'. Used for the
// query-path stateVersion / rowVersion, which must be VALID lexi (parsed on read).
function versionToLexi(v) {
  const b = v.toString(36);
  return (b.length - 1).toString(36) + b;
}

// ── Query-driven (received-rows) program generator ──
//
// A config PRELUDE creates a client with K desired queries, then a sequence of
// QUERY transactions execute subsets of them (trackQueries) and receive rows
// (received) referencing the executed queries, with deleteUnreferencedRows. This
// exercises the query-driven WRITE path across many stateVersions: desired->gotten
// transitions, refCount merge against rows loaded from PG, row versioning, and the
// unreferenced-row GC (a re-executed query whose row is NOT re-received is deleted).
const ROW_POOL = [
  {schema: 'public', table: 'issues', rowKey: {id: '0'}},
  {schema: 'public', table: 'issues', rowKey: {id: '1'}},
  {schema: 'public', table: 'issues', rowKey: {id: '2'}},
  {schema: 'public', table: 'labels', rowKey: {issueID: '1', labelID: '0'}},
  {schema: 'public', table: 'labels', rowKey: {issueID: '1', labelID: '1'}},
  {schema: 'public', table: 'comments', rowKey: {id: 'x'}},
];

export function generateQuery(seed) {
  // Distinct PRNG stream from the config generator so the same seed yields a
  // different program in each corpus.
  const rnd = mulberry32(seed + 0x9e3779b9);
  const pick = arr => arr[Math.floor(rnd() * arr.length)];
  const chance = p => rnd() < p;

  const connectTime = 1_725_408_000_000;
  const K = 3 + Math.floor(rnd() * 3); // 3..5 queries
  const queries = Array.from({length: K}, (_, i) => `q${i}`);

  // Prelude: one config txn that desires all K queries (so they exist to execute).
  const transactions = [
    {
      kind: 'config',
      lastActive: connectTime + 3600_000,
      ttlClock: connectTime + 3600_000,
      ops: [
        {op: 'ensureClient', clientID: 'c0'},
        {
          op: 'putDesiredQueries',
          clientID: 'c0',
          queries: queries.map(h => ({hash: h, ast: astFor(h), ttl: 300000})),
        },
      ],
    },
  ];

  let stateCounter = 0;
  const nTx = 2 + Math.floor(rnd() * 5); // 2..6 query transactions
  for (let i = 0; i < nTx; i++) {
    stateCounter += 1;
    const stateVersion = versionToLexi(stateCounter);
    const ttlClock = connectTime + (i + 2) * 3600_000;

    // Execute a non-empty distinct subset of the queries.
    const executed = [...new Set(queries.filter(() => rnd() < 0.6))];
    if (executed.length === 0) executed.push(pick(queries));
    // transformationHash: sometimes stable (idempotent re-exec), sometimes new.
    const track = {
      executed: executed.map(h => [h, `${h}-t${chance(0.5) ? i : 0}`]),
      removed: [],
    };

    // Receive a subset of rows, each referencing a non-empty subset of executed.
    const received = [];
    for (const row of ROW_POOL) {
      if (!chance(0.55)) continue;
      const refs = executed.filter(() => rnd() < 0.6);
      if (refs.length === 0) continue;
      const refCounts = {};
      for (const q of refs) refCounts[q] = 1;
      received.push({
        id: row,
        contents: {...row.rowKey, v: i},
        version: stateVersion,
        refCounts,
      });
    }

    transactions.push({
      kind: 'query',
      stateVersion,
      replicaVersion: '01',
      lastActive: ttlClock,
      ttlClock,
      track,
      received,
      deleteUnreferenced: true,
    });
  }

  return {
    seed,
    cvrId: `cg-seqq-${String(seed).padStart(6, '0')}`,
    shard: {appID: 'roze', shardNum: 1},
    connectTime,
    transactions,
  };
}

// ── CLI ──
const args = process.argv.slice(2);
function writeCorpus(prefix, fn, n) {
  const outDir = path.join(dir, 'corpus');
  fs.mkdirSync(outDir, {recursive: true});
  for (let s = 0; s < n; s++) {
    fs.writeFileSync(
      path.join(outDir, `${prefix}-${String(s).padStart(3, '0')}.json`),
      JSON.stringify(fn(s), null, 2) + '\n',
    );
  }
  console.error(`wrote ${n} ${prefix} programs to ${outDir}`);
}

if (args[0] === '--corpus') {
  writeCorpus('prog', generate, Number(args[1] || 40));
} else if (args[0] === '--qcorpus') {
  writeCorpus('qprog', generateQuery, Number(args[1] || 30));
} else if (args[0] === '--q' && !isNaN(Number(args[1]))) {
  process.stdout.write(JSON.stringify(generateQuery(Number(args[1])), null, 2) + '\n');
} else if (args.length && !isNaN(Number(args[0]))) {
  process.stdout.write(JSON.stringify(generate(Number(args[0])), null, 2) + '\n');
} else if (import.meta.url === `file://${process.argv[1]}`) {
  console.error(
    'usage: node gen.mjs <seed> | --q <seed> | --corpus <N> | --qcorpus <N>',
  );
  process.exit(2);
}
