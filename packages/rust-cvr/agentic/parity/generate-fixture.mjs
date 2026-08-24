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
} from '../../../zero-cache/src/services/view-syncer/cvr.ts';
import {makeRowPatch} from '../../../zero-cache/src/services/view-syncer/client-handler.ts';

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
    patchVersion: {stateVersion: '2z0'},
    transformationHash: 'th5', transformationVersion: {stateVersion: '2z0', configVersion: 3},
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
  })),
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
};

console.log(JSON.stringify(fixture, null, 2));
