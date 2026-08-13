#!/usr/bin/env node
/**
 * Generates the TS-vs-Rust parity fixture for Phase A.
 *
 * Runs the TS implementations of h64/h128/rowIDString/rowIDHash/rowIDSignatureUnit/parseSignature/formatSignature
 * against a fixed battery of inputs, writes the results as JSON, and is intended to be
 * consumed by `packages/rust-cvr/agentic/parity/verify-fixture.rs`.
 *
 * Usage:
 *   node packages/rust-cvr/agentic/parity/generate-fixture.mjs > packages/rust-cvr/agentic/parity/parity-fixture.json
 */

import {h32, h64, h128} from '../../../shared/src/hash.ts';
import {
  rowIDHash as rowIDHashTs,
  rowIDString as rowIDStringTs,
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
} from '../../../zero-cache/src/services/view-syncer/schema/types.ts';

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
};

console.log(JSON.stringify(fixture, null, 2));
