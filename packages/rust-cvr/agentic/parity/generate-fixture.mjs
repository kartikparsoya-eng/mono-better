#!/usr/bin/env node
/**
 * Generates the TS-vs-Rust parity fixture for Phase A.
 *
 * Runs the TS implementations of h64/h128/rowIDString/rowIDHash/rowIDSignatureUnit/parseSignature/formatSignature
 * against a fixed battery of inputs, writes the results as JSON, and is intended to be
 * consumed by `packages/rust-cvr/agentic/parity/verify-fixture.rs`.
 *
 * Usage:
 *   npx tsx packages/rust-cvr/agentic/parity/generate-fixture.mjs > packages/rust-cvr/agentic/parity/parity-fixture.json
 *
 * (tsx, not bare node: cvr.ts's transitive imports remap .js->.ts extensions.)
 */

import {h32, h64, h128} from '../../../shared/src/hash.ts';
import {
  rowIDHash as rowIDHashTs,
  rowIDString as rowIDStringTs,
  normalizedKeyOrder,
} from '../../../zero-cache/src/types/row-key.ts';
import {rowIDSignatureUnit} from '../../../zero-cache/src/services/view-syncer/row-set-signature.ts';
import {
  versionToLexi,
  versionFromLexi,
} from '../../../zero-cache/src/types/lexi-version.ts';
import {
  cmpVersions,
  versionString,
  versionFromString,
  queryRecordToQueryRow,
  oneAfter,
  maxVersion,
  versionToCookie,
  versionToNullableCookie,
} from '../../../zero-cache/src/services/view-syncer/schema/types.ts';
import {parseTTL, compareTTL, clampTTL} from '../../../zql/src/query/ttl.ts';
import {
  getInactiveQueries,
  mergeRefCounts,
  getMutationResultsQuery,
  nextEvictionTime,
} from '../../../zero-cache/src/services/view-syncer/cvr.ts';
import {makeRowPatch} from '../../../zero-cache/src/services/view-syncer/client-handler.ts';
import {
  CVRConfigDrivenUpdater,
  CVRQueryDrivenUpdater,
} from '../../../zero-cache/src/services/view-syncer/cvr.ts';
import {CustomKeyMap} from '../../../shared/src/custom-key-map.ts';
import {ClientHandler} from '../../../zero-cache/src/services/view-syncer/client-handler.ts';

// Handpicked inputs that cover the interesting space:
// - Empty/short strings for the hash functions
// - Realistic rowIds with different schema/table/key shapes
// - Multi-column rowKeys with mixed types (string, number, boolean, null)

const STRINGS = [
  '',
  'a',
  'hello',
  'world',
  'foo bar baz',
  'unicode-émoji-🚀',
  'multi\nline\nstring',
  'larger test string with some padding'.repeat(20),
];

const ROW_IDS = [
  {schema: 'public', table: 'users', rowKey: {id: 42}},
  {schema: 'public', table: 'users', rowKey: {id: 'user-abc'}},
  {schema: 'public', table: 'orders', rowKey: {id: 1, userId: 'u1'}},
  {schema: 'zero_0', table: '__zero_internal', rowKey: {mutationID: 5, clientID: 'client-123'}},
  {schema: 'app', table: 'items', rowKey: {b: true, a: null, c: 3.14}},
  // Nested object values in the rowKey (used by some custom queries).
  {
    schema: 'public',
    table: 'compound',
    rowKey: {id: {nested: {x: 1, y: 'text'}}, tag: 'z'},
  },
];

const SIGNATURE_VALUES = [
  0n,
  1n,
  0xffn,
  0x7fffffffn, // i32 max
  0xffffffffffffn, // arbitrary mid-range
  0xffffffffffffffffn, // u64 max
];

// --- ② semantic layer: CVR version encoding + comparison ------------------
// LexiVersion round-trip. Includes the exact vectors from lexi-version.test.ts
// plus boundaries that exercise multi-char length prefixes. Passed as decimal
// strings and hydrated with BigInt so large u64 values survive JSON.
const LEXI_VALUES = [
  '0', '1', '10', '35', '36', '37', '46655', '46656',
  '4294967296', // 2^32
  '9007199254740991', // Number.MAX_SAFE_INTEGER
  '9223372036854775808', // 2^63 (large u64)
];

// CVRVersion -> versionString/cookie. configVersion is deliberately varied:
// absent, present, and 0 — TS treats configVersion as FALSY (`v.configVersion
// ? ... : stateVersion`), so 0 must serialize as bare stateVersion. This pins
// whether the Rust port replicates that falsy-zero contract.
const CVR_VERSIONS = [
  {stateVersion: '00'},
  {stateVersion: '1a9'},
  {stateVersion: '1a9', configVersion: 1},
  {stateVersion: '1a9', configVersion: 2},
  {stateVersion: '72ioz88c0', configVersion: 23},
  {stateVersion: '1a9', configVersion: 0}, // falsy-zero contract probe
];

// Cookie strings -> versionFromString round-trip.
const COOKIES = ['00', '1a9', '1a9:01', '72ioz88c0:0n'];

// cmpVersions ordering — nullable pairs covering every branch + config tie-break.
const CMP_PAIRS = [
  [null, null],
  [null, {stateVersion: '00'}],
  [{stateVersion: '00'}, null],
  [{stateVersion: '1a9'}, {stateVersion: '1a9'}],
  [{stateVersion: '1a9'}, {stateVersion: '1aa'}],
  [{stateVersion: '1aa'}, {stateVersion: '1a9'}],
  [{stateVersion: '1a9', configVersion: 1}, {stateVersion: '1a9', configVersion: 2}],
  [{stateVersion: '1a9', configVersion: 2}, {stateVersion: '1a9', configVersion: 1}],
  [{stateVersion: '1a9'}, {stateVersion: '1a9', configVersion: 1}], // undefined config == 0
  [{stateVersion: '1a9', configVersion: 0}, {stateVersion: '1a9'}], // 0 vs undefined -> equal
];

const sign = n => (n < 0 ? -1 : n > 0 ? 1 : 0);

// getInactiveQueries accumulation + sort. Compact spec (inactivatedAt: null
// means "still active for that client" => undefined in TS). Exercises: single
// inactive client, active-client exclusion, max-eviction-per-query across
// clients, clampTTL (-1 and > MAX_TTL), internal exclusion, and multi-query
// eviction-time ordering incl. an equal-eviction-time tie.
const MAX_TTL = 10 * 60 * 1000;
const INACTIVE_CASES = [
  {
    desc: 'single inactive client -> included',
    queries: {q1: {type: 'client', clientState: {c1: {inactivatedAt: 100, ttl: 5000}}}},
  },
  {
    desc: 'active for one client (undefined) -> excluded even if inactive for another',
    queries: {
      q1: {
        type: 'client',
        clientState: {c1: {inactivatedAt: 100, ttl: 5000}, c2: {inactivatedAt: null, ttl: 5000}},
      },
    },
  },
  {
    desc: 'multi-client inactive -> take furthest-future eviction',
    queries: {
      q1: {
        type: 'client',
        clientState: {
          c1: {inactivatedAt: 100, ttl: 1000}, // evict 1100
          c2: {inactivatedAt: 200, ttl: 5000}, // evict 5200 (winner)
        },
      },
    },
  },
  {
    desc: 'clampTTL: -1 and > MAX_TTL both clamp to MAX_TTL',
    queries: {
      qNeg: {type: 'client', clientState: {c1: {inactivatedAt: 0, ttl: -1}}},
      qBig: {type: 'custom', clientState: {c1: {inactivatedAt: 0, ttl: 24 * 60 * 60 * 1000}}},
    },
  },
  {
    desc: 'internal query excluded',
    queries: {
      qi: {type: 'internal'},
      qc: {type: 'client', clientState: {c1: {inactivatedAt: 50, ttl: 2000}}},
    },
  },
  {
    desc: 'multi-query eviction ordering + equal-eviction tie',
    queries: {
      qLate: {type: 'client', clientState: {c1: {inactivatedAt: 1000, ttl: 9000}}}, // 10000
      qEarly: {type: 'client', clientState: {c1: {inactivatedAt: 100, ttl: 400}}}, // 500
      qTieA: {type: 'client', clientState: {c1: {inactivatedAt: 300, ttl: 700}}}, // 1000
      qTieB: {type: 'client', clientState: {c1: {inactivatedAt: 600, ttl: 400}}}, // 1000 (tie)
    },
  },
];

// mergeRefCounts: existing + received refcount maps, with optional removeHashes
// applied to `existing` only. Probes the no-existing branch's zero-entry
// handling, negative counts cancelling to zero, and the all-zero -> null rule.
const REFCOUNT_MERGES = [
  {existing: null, received: null},
  {existing: null, received: {a: 2}},
  {existing: null, received: {a: 0, b: 5}}, // no-existing zero-retention probe
  {existing: null, received: {a: 0}}, // all-zero -> null
  {existing: {a: 2}, received: {a: -2}}, // cancels to null
  {existing: {a: 2, b: 1}, received: {a: -2}}, // -> {b:1}
  {existing: {a: 2}, received: {b: 3}}, // -> {a:2,b:3}
  {existing: {a: 5}, received: {a: -3}}, // -> {a:2}
  {existing: {a: 2, b: 3}, received: null, removeHashes: ['a']}, // -> {b:3}
  {existing: {a: 2}, received: null, removeHashes: ['a']}, // -> null
  {existing: {a: 0, b: 5}, received: {a: 3}}, // -> {b:5,a:3}
];

// queryRecordToQueryRow: QueryRecord -> QueriesRow (DB row). Pins the subtle
// field mapping: internal=true vs null (NOT false) for client/custom, clientAST
// null for custom, queryArgs passthrough, and patchVersion via maybeVersionString
// (which routes through versionString, incl. the falsy configVersion==0 case).
// makeRowPatch: RowPatch -> rowsPatch wire op (the poke serialization the
// change_processor feeds). Pins put value passthrough (incl. nested/null/bool,
// max-safe int), del id shape (simple/composite), and poisoned-rowKey del
// (non-PK column passed through, matching the catchup finding).
const RP = (schema, table, rowKey) => ({schema, table, rowKey});
const ROW_PATCH_CASES = [
  {desc: 'put simple', patch: {op: 'put', id: RP('public', 'issue', {id: '1'}),
    contents: {id: '1', title: 'a', n: 5}}},
  {desc: 'put nested/null/bool', patch: {op: 'put', id: RP('public', 'issue', {id: '2'}),
    contents: {id: '2', meta: {x: [1, 2]}, flag: true, opt: null}}},
  {desc: 'put max-safe int', patch: {op: 'put', id: RP('public', 'issue', {id: '3'}),
    contents: {id: '3', big: 9007199254740991}}},
  {desc: 'del simple', patch: {op: 'del', id: RP('public', 'issue', {id: '4'})}},
  {desc: 'del composite key', patch: {op: 'del', id: RP('public', 'issue', {a: 'x', b: 2})}},
  {desc: 'del poisoned rowKey (non-PK col)', patch: {op: 'del',
    id: RP('public', 'issue', {id: '5', _leaked: 'oops'})}},
];

const CVRID = 'cg-parity';
const QR_CASES = [
  {desc: 'internal', spec: {type: 'internal', id: 'q1', ast: {table: 't'},
    transformationHash: 'th1'}},
  {desc: 'client with patchVersion', spec: {type: 'client', id: 'q2', ast: {table: 'u'},
    patchVersion: {stateVersion: '1a9', configVersion: 2}}},
  {desc: 'custom with args', spec: {type: 'custom', id: 'q3', name: 'myQuery',
    args: [1, 'x', null, {k: true}], patchVersion: {stateVersion: '1aa', configVersion: 1}}},
  {desc: 'custom patchVersion configVersion 0 -> bare stateVersion', spec: {type: 'custom',
    id: 'q4', name: 'n', args: [], patchVersion: {stateVersion: '1b0', configVersion: 0}}},
  {desc: 'client full fields', spec: {type: 'client', id: 'q5', ast: {table: 'v'},
    patchVersion: {stateVersion: '2zzz'},
    transformationHash: 'th5', transformationVersion: {stateVersion: '2zzz', configVersion: 3},
    rowSetSignature: 'deadbeef'}},
];
function buildQR(s) {
  const base = {id: s.id};
  if (s.transformationHash !== undefined) base.transformationHash = s.transformationHash;
  if (s.transformationVersion !== undefined) base.transformationVersion = s.transformationVersion;
  if (s.rowSetSignature !== undefined) base.rowSetSignature = s.rowSetSignature;
  if (s.type === 'internal') return {...base, type: 'internal', ast: s.ast};
  if (s.type === 'custom')
    return {...base, type: 'custom', name: s.name, args: s.args, patchVersion: s.patchVersion};
  return {...base, type: 'client', ast: s.ast, patchVersion: s.patchVersion};
}

function buildClientState(cs) {
  const out = {};
  for (const [cid, s] of Object.entries(cs)) {
    // Omit inactivatedAt entirely (=> undefined) for the active case.
    out[cid] = s.inactivatedAt === null ? {ttl: s.ttl} : {inactivatedAt: s.inactivatedAt, ttl: s.ttl};
  }
  return out;
}
function buildCVR(queries) {
  const q = {};
  for (const [hash, spec] of Object.entries(queries)) {
    q[hash] =
      spec.type === 'internal'
        ? {type: 'internal'}
        : {type: spec.type, clientState: buildClientState(spec.clientState)};
  }
  return {queries: q};
}

// --- ③ Tier-A leaf gaps: TTL semantics, key normalization, version helpers ---

// TTL (zql/src/query/ttl.ts). Numbers (incl. negative = forever, > MAX) and the
// string forms (`5m`, `1h`, `forever`, `none`). Pins parseTTL / clampTTL and the
// forever-aware compareTTL ordering.
const TTL_VALUES = [
  0, 5000, 300000, 599999, 600000, 600001, 3600000, -1, -5000,
  '30s', '5m', '10m', '1h', '2d', '1y', 'forever', 'none',
];
const TTL_PAIRS = [
  [5000, 10000], [300000, '5m'], ['forever', 5000], ['none', '5m'],
  [5000, 5000], ['1h', '60m'], [600001, '10m'], ['forever', 'forever'],
  ['none', 'none'], [-1, 5000],
];

// normalizedKeyOrder (types/row-key.ts): single-col, already-ordered, and
// out-of-order multi-col keys (the sort branch), plus null/mixed values.
const NORMALIZED_KEYS = [
  {id: 5},
  {a: 1, b: 2},
  {b: 1, a: 2, c: 3},
  {z: 'x', a: null, m: true},
  {id: 'u1', userId: 'u2'},
];

// CVRVersion / NullableCVRVersion inputs for the version helper family. Includes
// the falsy configVersion==0 contract on every path.
const NULLABLE_VERSIONS = [
  null,
  {stateVersion: '00'},
  {stateVersion: '1a9'},
  {stateVersion: '1a9', configVersion: 2},
  {stateVersion: '1a9', configVersion: 0},
];
const NONNULL_VERSIONS = NULLABLE_VERSIONS.filter(v => v !== null);
const MAX_PAIRS = [
  [{stateVersion: '1a9'}, {stateVersion: '1aa'}],
  [{stateVersion: '1aa'}, undefined],
  [{stateVersion: '1a9', configVersion: 1}, {stateVersion: '1a9', configVersion: 3}],
  [{stateVersion: '1a9', configVersion: 0}, {stateVersion: '1a9'}],
];
// cmpCvr: non-null CVRVersion pairs exercising state + config tie-break.
const CMP_CVR_PAIRS = [
  [{stateVersion: '1a9'}, {stateVersion: '1a9'}],
  [{stateVersion: '1a9'}, {stateVersion: '1aa'}],
  [{stateVersion: '1a9', configVersion: 1}, {stateVersion: '1a9', configVersion: 2}],
  [{stateVersion: '1a9', configVersion: 0}, {stateVersion: '1a9'}],
];
// tryVersionFromString: valid cookies + malformed strings (TS throws => invalid).
const VERSION_PARSE_STRINGS = ['00', '1a9', '1a9:01', '', 'zz:zz', '1a9:'];
function tsVersionParse(str) {
  try {
    return {str, valid: true, versionString: versionString(versionFromString(str))};
  } catch {
    return {str, valid: false};
  }
}

// --- ④ Tier-B: CVRConfigDrivenUpdater.putDesiredQueries state-transition diff -
// Drive the REAL TS updater with a stub CVRStore that captures the queued
// writes, then capture {returned patches, putDesiredQuery ops, resulting desire
// state}. The Rust test replays each scenario through put_desired_queries and
// asserts all three match. Pins the query-desire algorithm (client tracking,
// version bumps, ttl/inactivatedAt handling) that feeds row-key construction.

function makeStubStore(existingRows = new Map()) {
  const ops = [];
  return {
    ops,
    getRowRecords: async () => existingRows,
    insertClient: client => ops.push({op: 'insertClient', id: client.id}),
    putQuery: query => ops.push({op: 'putQuery', id: query.id, kind: query.type}),
    updateQuery: query => ops.push({op: 'updateQuery', id: query.id, kind: query.type}),
    putRowRecord: row => ops.push({op: 'putRowRecord', id: row.id}),
    delRowRecord: id => ops.push({op: 'delRowRecord', id}),
    putInstance: () => ops.push({op: 'putInstance'}),
    putDesiredQuery: (v, q, c, deleted, inactivatedAt, ttl) =>
      ops.push({
        op: 'putDesiredQuery',
        version: versionString(v),
        queryID: q.id,
        clientID: c.id,
        deleted,
        inactivatedAt: inactivatedAt ?? null,
        ttl,
      }),
    markQueryAsDeleted: (v, qp) =>
      ops.push({op: 'markQueryAsDeleted', version: versionString(v), patch: qp}),
    updateRowSetSignature: (qh, sig) =>
      ops.push({op: 'updateRowSetSignature', queryHash: qh, signature: sig}),
    deleteClient: cid => ops.push({op: 'deleteClient', clientID: cid}),
    flush: async () => {},
  };
}

// A minimal starting CVR (snapshot). Callers may pre-seed clients/queries.
function baseCVR(overrides = {}) {
  return {
    id: 'cg-parity',
    version: {stateVersion: 'v1'},
    lastActive: 0,
    ttlClock: 0,
    replicaVersion: 'r1',
    clients: {},
    queries: {},
    clientSchema: null,
    profileID: null,
    ...overrides,
  };
}

const SHARD = {appID: 'test', shardNum: 0};

// Canonical patch form both sides normalize to (Rust serializes to_version /
// externally-tagged enums; TS uses toVersion / {type,op}). Keyed by kind.
function normPatch(pv) {
  const p = pv.patch;
  const toVersion = versionString(pv.toVersion);
  if (p.type === 'query') {
    return {kind: 'query', op: p.op, id: p.id, clientID: p.clientID ?? null, toVersion};
  }
  return p.op === 'put'
    ? {kind: 'row', op: 'put', id: p.id, contents: p.contents, toVersion}
    : {kind: 'row', op: 'del', id: p.id, toVersion};
}

// Resulting desire state: per-client desiredQueryIDs + per (non-internal) query
// clientState. Captures what the desire algorithm mutated.
function normDesireState(cvr) {
  const clients = {};
  for (const [cid, c] of Object.entries(cvr.clients)) {
    clients[cid] = [...c.desiredQueryIDs].sort();
  }
  const queries = {};
  for (const [qh, q] of Object.entries(cvr.queries)) {
    if (q.type === 'internal') continue;
    const cs = {};
    for (const [cid, s] of Object.entries(q.clientState ?? {})) {
      cs[cid] = {inactivatedAt: s.inactivatedAt ?? null, ttl: s.ttl ?? null};
    }
    queries[qh] = {type: q.type, clientState: cs};
  }
  return {clients, queries};
}

// Each scenario = a sequence of putDesiredQueries calls against one updater.
const CONFIG_SCENARIOS = [
  {
    desc: 'fresh client desires one crud query',
    calls: [{clientID: 'c1', queries: [{hash: 'q1', ast: {table: 'issue'}}]}],
  },
  {
    desc: 'client desires crud + custom query',
    calls: [
      {
        clientID: 'c1',
        queries: [
          {hash: 'q1', ast: {table: 'issue'}},
          {hash: 'q2', name: 'myQuery', args: [1, 'x']},
        ],
      },
    ],
  },
  {
    desc: 'explicit ttl is carried into the desire',
    calls: [{clientID: 'c1', queries: [{hash: 'q1', ast: {table: 'issue'}, ttl: 60000}]}],
  },
  {
    desc: 're-desire same query is a no-op (no new patch)',
    calls: [
      {clientID: 'c1', queries: [{hash: 'q1', ast: {table: 'issue'}}]},
      {clientID: 'c1', queries: [{hash: 'q1', ast: {table: 'issue'}}]},
    ],
  },
  {
    desc: 'two clients desire the same query',
    calls: [
      {clientID: 'c1', queries: [{hash: 'q1', ast: {table: 'issue'}}]},
      {clientID: 'c2', queries: [{hash: 'q1', ast: {table: 'issue'}}]},
    ],
  },
  {
    desc: 'ttl over MAX is clamped to MAX (both sides)',
    calls: [
      {clientID: 'c1', queries: [{hash: 'q1', ast: {table: 'issue'}, ttl: 24 * 60 * 60 * 1000}]},
    ],
  },
  {
    desc: 'input order preserved with 3 queries (nondeterminism probe)',
    calls: [
      {
        clientID: 'c1',
        queries: [
          {hash: 'zzz', ast: {table: 'a'}},
          {hash: 'aaa', ast: {table: 'b'}},
          {hash: 'mmm', name: 'n', args: []},
        ],
      },
    ],
  },
];

function runConfigScenario(sc) {
  const store = makeStubStore();
  const updater = new CVRConfigDrivenUpdater(store, baseCVR(), SHARD);
  const callResults = sc.calls.map(call => {
    const patches = updater.putDesiredQueries(call.clientID, call.queries);
    return {clientID: call.clientID, patches: patches.map(normPatch)};
  });
  return {
    desc: sc.desc,
    calls: sc.calls,
    callResults,
    desiredOps: store.ops.filter(o => o.op === 'putDesiredQuery'),
    finalState: normDesireState(updater._cvr),
    finalVersion: versionString(updater._cvr.version),
  };
}

// --- Tier-B: CVRConfigDrivenUpdater lifecycle ops (markInactive/delete/clear/deleteClient)
// A sequence of config ops on one updater; captures per-op patches + final state.
const CONFIG_OP_SCENARIOS = [
  {
    desc: 'desire then mark inactive',
    ops: [
      {fn: 'putDesiredQueries', clientID: 'c1', queries: [{hash: 'q1', ast: {table: 't'}}]},
      {fn: 'markDesiredQueriesAsInactive', clientID: 'c1', queryHashes: ['q1'], ttlClock: 1000},
    ],
  },
  {
    desc: 'desire then delete desired',
    ops: [
      {fn: 'putDesiredQueries', clientID: 'c1', queries: [{hash: 'q1', ast: {table: 't'}}]},
      {fn: 'deleteDesiredQueries', clientID: 'c1', queryHashes: ['q1']},
    ],
  },
  {
    desc: 'desire two then clear',
    ops: [
      {
        fn: 'putDesiredQueries',
        clientID: 'c1',
        queries: [{hash: 'q1', ast: {table: 't'}}, {hash: 'q2', ast: {table: 'u'}}],
      },
      {fn: 'clearDesiredQueries', clientID: 'c1'},
    ],
  },
  {
    desc: 'desire then delete client',
    ops: [
      {fn: 'putDesiredQueries', clientID: 'c1', queries: [{hash: 'q1', ast: {table: 't'}}]},
      {fn: 'deleteClient', clientID: 'c1', ttlClock: 2000},
    ],
  },
];

function runConfigOpScenario(sc) {
  const store = makeStubStore();
  const updater = new CVRConfigDrivenUpdater(store, baseCVR(), SHARD);
  const opResults = sc.ops.map(op => {
    let patches;
    switch (op.fn) {
      case 'putDesiredQueries':
        patches = updater.putDesiredQueries(op.clientID, op.queries);
        break;
      case 'markDesiredQueriesAsInactive':
        patches = updater.markDesiredQueriesAsInactive(op.clientID, op.queryHashes, op.ttlClock);
        break;
      case 'deleteDesiredQueries':
        patches = updater.deleteDesiredQueries(op.clientID, op.queryHashes);
        break;
      case 'clearDesiredQueries':
        patches = updater.clearDesiredQueries(op.clientID);
        break;
      case 'deleteClient':
        patches = updater.deleteClient(op.clientID, op.ttlClock);
        break;
      default:
        throw new Error(`unknown op ${op.fn}`);
    }
    return {fn: op.fn, patches: patches.slice().sort(byKey).map(normPatch)};
  });
  return {
    desc: sc.desc,
    ops: sc.ops,
    opResults,
    finalState: normDesireState(updater._cvr),
    finalVersion: versionString(updater._cvr.version),
  };
}

// --- Tier-B: CVRConfigDrivenUpdater metadata setters (setProfileID/setClientSchema)
const METADATA_SCENARIOS = [
  {desc: 'set profileID', ops: [{fn: 'setProfileID', profileID: 'user-1'}]},
  {
    desc: 'set profileID twice same (idempotent)',
    ops: [{fn: 'setProfileID', profileID: 'u'}, {fn: 'setProfileID', profileID: 'u'}],
  },
  {
    desc: 'change profileID from cg-backfill',
    ops: [{fn: 'setProfileID', profileID: 'cg-old'}, {fn: 'setProfileID', profileID: 'new'}],
  },
  {
    desc: 'set clientSchema first time',
    ops: [{fn: 'setClientSchema', schema: {tables: {issue: {columns: {id: 'string'}}}}}],
  },
  {
    desc: 'set clientSchema same twice ok',
    ops: [
      {fn: 'setClientSchema', schema: {tables: {}}},
      {fn: 'setClientSchema', schema: {tables: {}}},
    ],
  },
  {
    desc: 'set clientSchema mismatch errors',
    ops: [
      {fn: 'setClientSchema', schema: {tables: {a: 1}}},
      {fn: 'setClientSchema', schema: {tables: {b: 2}}},
    ],
  },
];

function runMetadataScenario(sc) {
  const store = makeStubStore();
  const updater = new CVRConfigDrivenUpdater(store, baseCVR(), SHARD);
  const opResults = sc.ops.map(op => {
    try {
      if (op.fn === 'setProfileID') updater.setProfileID(LC, op.profileID);
      else if (op.fn === 'setClientSchema') updater.setClientSchema(LC, op.schema);
      else throw new Error(`unknown ${op.fn}`);
      return {fn: op.fn, ok: true};
    } catch {
      return {fn: op.fn, ok: false};
    }
  });
  return {
    desc: sc.desc,
    ops: sc.ops,
    opResults,
    clientSchema: updater._cvr.clientSchema ?? null,
    profileID: updater._cvr.profileID ?? null,
  };
}

// --- ⑤ Tier-B: CVRQueryDrivenUpdater trackQueries -> received -> deleteUnreferencedRows
// The rowKey-construction path. received/deleteUnreferencedRows are async and
// read existing rows from the store, so the stub returns preset rows and the
// driver awaits. Row patches come out in HashMap order on the Rust side, so both
// sides sort by rowIDString before comparing (row patches are key-addressed).

const LC = {
  info: () => {},
  debug: () => {},
  warn: () => {},
  error: () => {},
  withContext() {
    return LC;
  },
};

function clientQueryRecord(hash, q) {
  return {
    id: hash,
    type: 'client',
    ast: q.ast,
    clientState: {},
    transformationHash: q.transformationHash,
    transformationVersion: undefined,
    patchVersion: q.patchVersion,
    rowSetSignature: undefined,
  };
}

function buildExistingRows(specs) {
  // getRowRecords() returns a CustomKeyMap keyed by rowIDString — NOT a plain Map
  // (whose object-reference keying would make received()'s `existingRows.get(id)`
  // silently miss and spuriously treat every row as new).
  const m = new CustomKeyMap(rowIDStringTs);
  for (const s of specs) {
    m.set(s.id, {
      id: s.id,
      rowVersion: s.rowVersion,
      patchVersion: s.patchVersion,
      refCounts: s.refCounts,
    });
  }
  return m;
}

// Stable sort key so TS (Map insertion order) and Rust (HashMap order) agree.
function patchSortKey(pv) {
  const p = pv.patch;
  return p.type === 'row' ? `row:${rowIDStringTs(p.id)}` : `query:${p.id}`;
}
const byKey = (a, b) => (patchSortKey(a) < patchSortKey(b) ? -1 : 1);

const QUERY_SCENARIOS = [
  {
    desc: 'execute query, receive 2 new rows (single + multi-col key)',
    stateVersion: 'v2',
    queries: {hash1: {ast: {table: 'issue'}}},
    executed: [{id: 'hash1', transformationHash: 'th1'}],
    removed: [],
    existingRows: [],
    receivedRows: [
      {
        id: {schema: 'public', table: 'issue', rowKey: {id: '1'}},
        contents: {id: '1', title: 'a'},
        version: 'rv1',
        refCounts: {hash1: 1},
      },
      {
        id: {schema: 'public', table: 'label', rowKey: {issueID: '1', labelID: '2'}},
        contents: {issueID: '1', labelID: '2'},
        version: 'rv1',
        refCounts: {hash1: 1},
      },
    ],
  },
  {
    desc: 'query-less config update: trackQueries([],[]) then no rows',
    stateVersion: 'v2',
    queries: {},
    executed: [],
    removed: [],
    existingRows: [],
    receivedRows: [],
  },
  {
    desc: 'remove query -> its sole existing row is deleted',
    stateVersion: 'v2',
    queries: {
      hash1: {ast: {table: 'issue'}, transformationHash: 'th1', patchVersion: {stateVersion: 'v1'}},
    },
    executed: [],
    removed: [{id: 'hash1'}],
    existingRows: [
      {
        id: {schema: 'public', table: 'issue', rowKey: {id: '1'}},
        rowVersion: 'rv0',
        patchVersion: {stateVersion: 'v1'},
        refCounts: {hash1: 1},
      },
    ],
    receivedRows: [],
  },
  {
    desc: 're-execute query; unchanged existing row is re-received at same rowVersion',
    stateVersion: 'v2',
    queries: {hash1: {ast: {table: 'issue'}, transformationHash: 'th0', patchVersion: {stateVersion: 'v1'}}},
    executed: [{id: 'hash1', transformationHash: 'th1'}],
    removed: [],
    existingRows: [
      {
        id: {schema: 'public', table: 'issue', rowKey: {id: '1'}},
        rowVersion: 'rv1',
        patchVersion: {stateVersion: 'v1'},
        refCounts: {hash1: 1},
      },
    ],
    receivedRows: [
      {
        id: {schema: 'public', table: 'issue', rowKey: {id: '1'}},
        contents: {id: '1', title: 'a'},
        version: 'rv1',
        refCounts: {hash1: 1},
      },
    ],
  },
  {
    desc: 'received row with a poisoned rowKey (extra non-PK column) passes through',
    stateVersion: 'v2',
    queries: {hash1: {ast: {table: 'issue'}}},
    executed: [{id: 'hash1', transformationHash: 'th1'}],
    removed: [],
    existingRows: [],
    receivedRows: [
      {
        id: {schema: 'public', table: 'issue', rowKey: {id: '5', _leaked: 'oops'}},
        contents: {id: '5', title: 'x'},
        version: 'rv1',
        refCounts: {hash1: 1},
      },
    ],
  },
];

async function runQueryScenario(sc) {
  const store = makeStubStore(buildExistingRows(sc.existingRows));
  const queries = {};
  for (const [h, q] of Object.entries(sc.queries)) queries[h] = clientQueryRecord(h, q);
  const updater = new CVRQueryDrivenUpdater(
    store,
    baseCVR({queries}),
    sc.stateVersion,
    'r1',
    undefined,
  );

  const {queryPatches} = updater.trackQueries(LC, sc.executed, sc.removed);

  const rowsMap = new Map();
  for (const r of sc.receivedRows) {
    rowsMap.set(r.id, {contents: r.contents, version: r.version, refCounts: r.refCounts});
  }
  const receivedPatches = await updater.received(LC, rowsMap);
  const deletePatches = await updater.deleteUnreferencedRows(LC);

  return {
    desc: sc.desc,
    stateVersion: sc.stateVersion,
    queries: sc.queries,
    executed: sc.executed,
    removed: sc.removed,
    existingRows: sc.existingRows,
    receivedRows: sc.receivedRows,
    trackPatches: queryPatches.slice().sort(byKey).map(normPatch),
    receivedPatches: receivedPatches.slice().sort(byKey).map(normPatch),
    deletePatches: deletePatches.slice().sort(byKey).map(normPatch),
    finalVersion: versionString(updater._cvr.version),
  };
}

// --- ⑨ Tier-D: ClientHandler poke assembly (patches -> pokeStart/pokePart/pokeEnd)
// The client wire format. Drive the real TS ClientHandler with a capturing sink
// (Subscription stub) and compare the emitted messages to the Rust ClientHandler
// driven with an equivalent WebSocketSink. baseCookie strings must be valid lexi
// (the constructor parses them); tentative/final are CVRVersion objects (not parsed).
function makeCHSink() {
  const messages = [];
  return {
    messages,
    push: msg => {
      messages.push(msg);
      return {result: Promise.resolve()};
    },
    fail: () => {},
    cancel: () => {},
  };
}

const POKE_SCENARIOS = [
  {
    desc: 'empty initial poke (forceInitial): start + end only',
    baseCookie: null,
    tentative: {stateVersion: 'v2'},
    patches: [],
    final: {stateVersion: 'v2', configVersion: 1},
  },
  {
    desc: 'got + desired query patches',
    baseCookie: null,
    tentative: {stateVersion: 'v2'},
    patches: [
      {patch: {type: 'query', op: 'put', id: 'q1', clientID: 'client1'}, toVersion: {stateVersion: 'v2'}},
      {patch: {type: 'query', op: 'put', id: 'q2'}, toVersion: {stateVersion: 'v2'}},
    ],
    final: {stateVersion: 'v2', configVersion: 1},
  },
  {
    desc: 'row put + del patches (rowsPatch via makeRowPatch)',
    baseCookie: null,
    tentative: {stateVersion: 'v2'},
    patches: [
      {
        patch: {type: 'row', op: 'put', id: {schema: 'public', table: 'issue', rowKey: {id: '1'}}, contents: {id: '1', title: 'a'}},
        toVersion: {stateVersion: 'v2'},
      },
      {
        patch: {type: 'row', op: 'del', id: {schema: 'public', table: 'issue', rowKey: {id: '2'}}},
        toVersion: {stateVersion: 'v2'},
      },
    ],
    final: {stateVersion: 'v2', configVersion: 1},
  },
  {
    desc: 'patch at/below baseVersion is dropped',
    baseCookie: '1a9',
    tentative: {stateVersion: '1aa'},
    patches: [
      {patch: {type: 'query', op: 'put', id: 'qOld'}, toVersion: {stateVersion: '1a9'}},
      {patch: {type: 'query', op: 'put', id: 'qNew'}, toVersion: {stateVersion: '1aa'}},
    ],
    final: {stateVersion: '1aa'},
  },
];

async function runPokeScenario(sc) {
  const sink = makeCHSink();
  const handler = new ClientHandler(LC, 'cg', 'client1', 'ws1', SHARD, sc.baseCookie ?? null, sink);
  const poke = handler.startPoke(sc.tentative);
  for (const p of sc.patches) await poke.addPatch(p);
  await poke.end(sc.final);
  return {
    desc: sc.desc,
    baseCookie: sc.baseCookie ?? null,
    tentative: sc.tentative,
    patches: sc.patches,
    final: sc.final,
    messages: sink.messages,
  };
}

const queryScenarioResults = await Promise.all(QUERY_SCENARIOS.map(runQueryScenario));
const pokeScenarioResults = await Promise.all(POKE_SCENARIOS.map(runPokeScenario));

const fixture = {
  hashes: STRINGS.map(s => ({
    input: s,
    h32: h32(s).toString(),
    h64: h64(s).toString(),
    h128: h128(s).toString(),
  })),
  rowIds: ROW_IDS.map(r => ({
    input: r,
    rowIDString: rowIDStringTs(r),
    rowIDHash: rowIDHashTs(r),
    rowIDSignatureUnit: rowIDSignatureUnit(r).toString(),
  })),
  signatures: SIGNATURE_VALUES.map(v => ({
    sig: v.toString(),
    hex: v.toString(16),
  })),
  lexiVersions: LEXI_VALUES.map(dec => ({
    value: dec,
    lexi: versionToLexi(BigInt(dec)),
    roundTrip: versionFromLexi(versionToLexi(BigInt(dec))).toString(),
  })),
  cvrVersions: CVR_VERSIONS.map(v => ({
    input: v,
    versionString: versionString(v),
  })),
  cookies: COOKIES.map(c => ({
    cookie: c,
    version: versionFromString(c),
  })),
  cmp: CMP_PAIRS.map(([a, b]) => ({
    a,
    b,
    sign: sign(cmpVersions(a, b)),
  })),
  inactiveQueries: INACTIVE_CASES.map(c => ({
    desc: c.desc,
    queries: c.queries,
    expected: getInactiveQueries(buildCVR(c.queries)),
    nextEvictionTime: nextEvictionTime(buildCVR(c.queries)) ?? null,
  })),
  mutationResultsQueries: [
    {upstreamSchema: 'zero_0', clientGroupID: 'cg-1'},
    {upstreamSchema: 'app_2', clientGroupID: 'cg-abc'},
  ].map(i => {
    const q = getMutationResultsQuery(i.upstreamSchema, i.clientGroupID);
    return {...i, id: q.id, ast: q.ast};
  }),
  rowPatches: ROW_PATCH_CASES.map(c => ({
    desc: c.desc,
    patch: c.patch,
    expected: makeRowPatch(c.patch),
  })),
  queryRows: QR_CASES.map(c => ({
    desc: c.desc,
    spec: c.spec,
    expected: queryRecordToQueryRow(CVRID, buildQR(c.spec)),
  })),
  refCountMerges: REFCOUNT_MERGES.map(c => ({
    existing: c.existing ?? null,
    received: c.received ?? null,
    removeHashes: c.removeHashes ?? null,
    expected: mergeRefCounts(
      c.existing,
      c.received,
      c.removeHashes ? new Set(c.removeHashes) : undefined,
    ),
  })),
  ttls: TTL_VALUES.map(v => ({
    input: v,
    parse: parseTTL(v),
    clamp: clampTTL(v),
  })),
  ttlCompares: TTL_PAIRS.map(([a, b]) => ({a, b, cmp: compareTTL(a, b)})),
  normalizedKeys: NORMALIZED_KEYS.map(k => ({
    input: k,
    order: Object.entries(normalizedKeyOrder(k)),
  })),
  oneAfter: NULLABLE_VERSIONS.map(v => ({
    input: v,
    versionString: versionString(oneAfter(v)),
  })),
  maxVersions: MAX_PAIRS.map(([a, b]) => ({
    a,
    b: b ?? null,
    versionString: versionString(maxVersion(a, b)),
  })),
  versionCookies: NONNULL_VERSIONS.map(v => ({
    input: v,
    cookie: versionToCookie(v),
  })),
  nullableVersionCookies: NULLABLE_VERSIONS.map(v => ({
    input: v,
    cookie: versionToNullableCookie(v),
  })),
  cmpCvr: CMP_CVR_PAIRS.map(([a, b]) => ({a, b, sign: sign(cmpVersions(a, b))})),
  versionParses: VERSION_PARSE_STRINGS.map(tsVersionParse),
  configScenarios: CONFIG_SCENARIOS.map(runConfigScenario),
  configOpScenarios: CONFIG_OP_SCENARIOS.map(runConfigOpScenario),
  metadataScenarios: METADATA_SCENARIOS.map(runMetadataScenario),
  queryScenarios: queryScenarioResults,
  pokeScenarios: pokeScenarioResults,
};

console.log(JSON.stringify(fixture, null, 2));
