#!/usr/bin/env node
/**
 * Runs both replay drivers on one program and structurally diffs their traces.
 *
 * TS trace  <- npx tsx run-ts.mjs <prog>
 * Rust trace <- cvr_seq_replay <prog>   (prebuilt binary; path via CVR_SEQ_REPLAY
 *               or the default rust-cvr target/debug location)
 *
 * Canonicalization sorts object keys (JSON key order is not semantic) and coerces
 * integer-valued floats (Rust emits `-1.0`, TS `-1`) so only *real* divergences
 * surface. Array ORDER is preserved — both drivers ORDER BY identically and emit
 * patches in op order, so an order mismatch is itself a finding.
 *
 * Exit 0 = identical, 1 = divergence (prints the first differing path), 2 = error.
 *
 * Usage: TEST_CVR_PG_URI=... node diff.mjs <program.json|seed>
 */
import {execFileSync} from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {fileURLToPath} from 'node:url';
import {generate} from './gen.mjs';

const dir = path.dirname(fileURLToPath(import.meta.url));
const RUST_BIN =
  process.env.CVR_SEQ_REPLAY ||
  path.join(dir, '../../../target/debug/cvr_seq_replay');

export function canon(v) {
  if (Array.isArray(v)) {
    // Sort arrays by canonical content. Both the DB dumps (ORDER BY'd row *sets*)
    // and the returned patch lists are order-INDEPENDENT here: rows are a set, and
    // TS's patch order is a size-based `intersection` optimization artifact, not a
    // contract. Sorting a multiset of full objects still catches any content
    // difference — it only ignores ordering.
    const a = v.map(canon);
    a.sort((x, y) => {
      const sx = JSON.stringify(x);
      const sy = JSON.stringify(y);
      return sx < sy ? -1 : sx > sy ? 1 : 0;
    });
    return a;
  }
  if (v && typeof v === 'object') {
    const o = {};
    for (const k of Object.keys(v).sort()) o[k] = canon(v[k]);
    return o;
  }
  if (typeof v === 'number' && Number.isInteger(v)) return v; // -1.0 -> -1 via JSON.parse already
  return v;
}

// First differing path between two canonicalized values (or null if equal).
export function firstDiff(a, b, p = '') {
  if (typeof a !== typeof b) return `${p}: type ${typeof a} vs ${typeof b}`;
  if (a === null || b === null || typeof a !== 'object') {
    return Object.is(a, b) || a === b ? null : `${p}: ${JSON.stringify(a)} vs ${JSON.stringify(b)}`;
  }
  if (Array.isArray(a) !== Array.isArray(b)) return `${p}: array/object mismatch`;
  if (Array.isArray(a)) {
    if (a.length !== b.length) return `${p}: length ${a.length} vs ${b.length}`;
    for (let i = 0; i < a.length; i++) {
      const d = firstDiff(a[i], b[i], `${p}[${i}]`);
      if (d) return d;
    }
    return null;
  }
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  for (const k of keys) {
    if (!(k in a)) return `${p}.${k}: missing on TS side`;
    if (!(k in b)) return `${p}.${k}: missing on Rust side`;
    const d = firstDiff(a[k], b[k], `${p}.${k}`);
    if (d) return d;
  }
  return null;
}

export function runTs(progPath) {
  const out = execFileSync('npx', ['tsx', path.join(dir, 'run-ts.mjs'), progPath], {
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  return JSON.parse(out);
}

export function runRust(progPath) {
  const out = execFileSync(RUST_BIN, [progPath], {
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  return JSON.parse(out);
}

// Returns {ok, diff, ts, rust}. Throws only on driver crash.
export function diffProgram(progPath) {
  const ts = canon(runTs(progPath));
  const rust = canon(runRust(progPath));
  const diff = firstDiff(ts, rust, 'trace');
  return {ok: diff === null, diff, ts, rust};
}

// ── CLI ──
if (import.meta.url === `file://${process.argv[1]}`) {
  if (!process.env.TEST_CVR_PG_URI) {
    console.error('TEST_CVR_PG_URI unset');
    process.exit(2);
  }
  const arg = process.argv[2];
  if (!arg) {
    console.error('usage: node diff.mjs <program.json|seed>');
    process.exit(2);
  }
  let progPath = arg;
  if (!fs.existsSync(arg)) {
    // Treat as a seed: generate to a temp file.
    const prog = generate(Number(arg));
    progPath = path.join(os.tmpdir(), `cvr-seq-${arg}.json`);
    fs.writeFileSync(progPath, JSON.stringify(prog, null, 2));
  }
  const {ok, diff} = diffProgram(progPath);
  if (ok) {
    console.log(`OK — traces identical for ${path.basename(progPath)}`);
    process.exit(0);
  }
  console.error(`DIVERGENCE in ${path.basename(progPath)}:\n  ${diff}`);
  process.exit(1);
}
