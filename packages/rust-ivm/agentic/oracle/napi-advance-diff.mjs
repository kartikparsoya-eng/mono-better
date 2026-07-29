#!/usr/bin/env node
// oracle/napi-advance-diff.mjs — compare TS advance-oracle output vs
// napi-advance-runner output across hydrate, advance, and finalView.
//
// Usage: node napi-advance-diff.mjs <expected.json> <actual.json>
//   expected.json = ts-advance-runner.mjs output
//   actual.json   = napi-advance-runner.mjs output

import {readFileSync} from 'node:fs';

const META_FIELDS = new Set(['queryId', 'isHidden']);

function stripMeta(rc) {
  const out = {};
  for (const k of Object.keys(rc)) {
    if (!META_FIELDS.has(k)) out[k] = rc[k];
  }
  return out;
}

function canon(v) {
  if (v === null) return null;
  if (typeof v === 'number') {
    if (Object.is(v, -0)) return 0;
    if (Number.isFinite(v) && Math.round(v) === v) return Math.round(v);
    return v;
  }
  if (typeof v === 'boolean' || typeof v === 'string') return v;
  if (Array.isArray(v)) return v.map(canon);
  if (typeof v === 'object') {
    const out = {};
    for (const k of Object.keys(v).sort()) out[k] = canon(v[k]);
    return out;
  }
  return v;
}

function deepEqual(a, b) {
  if (a === b) return true;
  if (typeof a !== typeof b) return false;
  if (a === null || b === null) return a === b;
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    return a.every((x, i) => deepEqual(x, b[i]));
  }
  if (typeof a === 'object' && typeof b === 'object') {
    const ka = Object.keys(a), kb = Object.keys(b);
    if (ka.length !== kb.length) return false;
    return ka.every(k => deepEqual(a[k], b[k]));
  }
  return false;
}

function rowChangeKey(rc) {
  const rowKeyStr = JSON.stringify(canon(rc.rowKey));
  const rowStr = rc.row ? JSON.stringify(canon(rc.row)) : 'null';
  return `${rc.changeType ?? 0}|${rc.table}|${rowKeyStr}|${rowStr}`;
}

function rowChangeCmp(a, b) {
  const ka = rowChangeKey(a), kb = rowChangeKey(b);
  return ka < kb ? -1 : ka > kb ? 1 : 0;
}

function compareRowChangeSets(expected, actual) {
  const dropHidden = rows => rows.filter(r => r.isHidden !== true).map(({isHidden, ...rest}) => rest);
  const exp = dropHidden(expected).map(stripMeta).map(canon);
  const act = dropHidden(actual).map(stripMeta).map(canon);
  const expSorted = [...exp].sort(rowChangeCmp);
  const actSorted = [...act].sort(rowChangeCmp);

  if (expSorted.length !== actSorted.length) {
    const expSet = new Set(expSorted.map(rowChangeKey));
    const actSet = new Set(actSorted.map(rowChangeKey));
    const missing = expSorted.filter(r => !actSet.has(rowChangeKey(r))).slice(0, 5);
    const extra = actSorted.filter(r => !expSet.has(rowChangeKey(r))).slice(0, 5);
    return {
      path: `length (expected=${expSorted.length} actual=${actSorted.length})`,
      missing: missing.map(r => JSON.stringify({table: r.table, rowKey: r.rowKey, row: r.row})),
      extra: extra.map(r => JSON.stringify({table: r.table, rowKey: r.rowKey, row: r.row})),
    };
  }

  for (let i = 0; i < expSorted.length; i++) {
    if (!deepEqual(expSorted[i], actSorted[i])) {
      return {path: `row[${i}]`, expected: expSorted[i], actual: actSorted[i]};
    }
  }
  return null;
}

function main() {
  const [expectedPath, actualPath] = process.argv.slice(2);
  if (!expectedPath || !actualPath) {
    console.error('Usage: napi-advance-diff.mjs <expected.json> <actual.json>');
    process.exit(2);
  }

  const expected = JSON.parse(readFileSync(expectedPath, 'utf8'));
  const actual = JSON.parse(readFileSync(actualPath, 'utf8'));

  for (const phase of ['hydrate', 'advance', 'finalView']) {
    const diff = compareRowChangeSets(expected[phase] || [], actual[phase] || []);
    if (diff) {
      console.error(`${phase.toUpperCase()} DIFF: ${diff.path}`);
      if (diff.missing) {
        console.error(`  missing from napi: ${diff.missing.join(', ')}`);
        console.error(`  extra in napi: ${diff.extra.join(', ')}`);
      } else {
        console.error(`  expected: ${JSON.stringify(diff.expected)}`);
        console.error(`  actual:   ${JSON.stringify(diff.actual)}`);
      }
      process.exit(1);
    }
  }
  console.log('EQUAL');
}

main();
