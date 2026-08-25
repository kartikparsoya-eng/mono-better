// Generates TS-golden `rowIDSignatureUnit` values (the unit XOR-folded into a
// query's rowSetSignature, PERSISTED to the CVR) so the Rust port can be pinned
// byte-for-byte. Drives the REAL TS impl. Run: npx tsx generate-row-signature-fixture.mjs
import {rowIDSignatureUnit} from '../../../zero-cache/src/services/view-syncer/row-set-signature.ts';

// Each case: {schema:'', table, rowKey} exactly as pipeline-driver.ts folds.
const cases = [
  {table: 'issue', rowKey: {id: 'abc'}},
  {table: 'issue', rowKey: {id: 1}},
  {table: 'user', rowKey: {id: 42, org: 'acme'}},   // multi-col
  {table: 'user', rowKey: {org: 'acme', id: 42}},   // same, unsorted → must equal ^
  {table: 't', rowKey: {b: 2, a: 1}},               // unsorted keys
  {table: 't', rowKey: {flag: true}},
  {table: 't', rowKey: {val: null}},
  {table: 'issue', rowKey: {id: 'abc', sub: 'x'}},  // distinct from case 0
];

const out = cases.map(c => ({
  schema: '',
  table: c.table,
  rowKey: c.rowKey,
  // bigint → decimal string (JSON can't hold u64/bigint)
  unit: rowIDSignatureUnit({schema: '', table: c.table, rowKey: c.rowKey}).toString(),
}));

console.log(JSON.stringify({cases: out}, null, 2));
