#!/usr/bin/env node
// oracle/diff.mjs — canonical JSON diff of expected vs actual.
//
// Usage: node oracle/diff.mjs <a.json> <b.json>
// Exit 0 if semantically equal (after canonicalization: key order, -0/0,
// integer-valued floats). Else prints a minimal path-based diff and exits 1.
//
// Numeric comparison is otherwise EXACT — precision differences are real bugs
// (Bun hit exactly this class). Do not add epsilon tolerance.

import {readFileSync} from 'node:fs';

function readJSON(p) {
  return JSON.parse(readFileSync(p, 'utf8'));
}

// Canonicalize a JS value for comparison:
//  - object keys sorted (deep)
//  - -0 normalized to 0
//  - integer-valued floats normalized (1.0 -> 1) so 1.0 === 1
//  - arrays compared element-wise
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

function diffPath(a, b, path) {
  if (deepEqual(a, b)) return null;
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) {
      return {path: `${path}.length`, a: a.length, b: b.length};
    }
    for (let i = 0; i < a.length; i++) {
      const d = diffPath(a[i], b[i], `${path}[${i}]`);
      if (d) return d;
    }
    return null;
  }
  if (a && b && typeof a === 'object' && typeof b === 'object') {
    const ka = Object.keys(a), kb = Object.keys(b);
    for (const k of [...new Set([...ka, ...kb])].sort()) {
      if (!(k in a)) return {path: `${path}.${k}`, a: undefined, b: b[k]};
      if (!(k in b)) return {path: `${path}.${k}`, a: a[k], b: undefined};
      const d = diffPath(a[k], b[k], `${path}.${k}`);
      if (d) return d;
    }
    return null;
  }
  return {path: path || '<root>', a, b};
}

function main() {
  const [aPath, bPath] = process.argv.slice(2);
  if (!aPath || !bPath) {
    console.error('Usage: diff.mjs <a.json> <b.json>');
    process.exit(2);
  }
  const a = canon(readJSON(aPath));
  const b = canon(readJSON(bPath));
  if (deepEqual(a, b)) {
    console.log('EQUAL');
    process.exit(0);
  }
  const d = diffPath(a, b, '');
  if (d) {
    console.error(`DIFF at ${d.path}`);
    console.error(`  expected: ${JSON.stringify(d.a)}`);
    console.error(`  actual:   ${JSON.stringify(d.b)}`);
  } else {
    console.error('DIFF (structural mismatch)');
  }
  process.exit(1);
}

main();
