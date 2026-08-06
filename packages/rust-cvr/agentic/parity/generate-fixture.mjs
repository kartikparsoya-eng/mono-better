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
};

console.log(JSON.stringify(fixture, null, 2));
