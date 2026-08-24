#!/usr/bin/env node
/**
 * Sequence-differential fuzz loop with delta-debugging shrinker.
 *
 * For each seed in a range, generates a program and diffs the TS vs Rust replay
 * traces (diff.mjs). On the first divergence it ddmin-shrinks the program —
 * dropping whole transactions, then individual ops — while the divergence
 * persists, and writes the MINIMAL reproducer + its TS golden trace to
 * `regressions/` so it can be promoted into the CI corpus.
 *
 * This is the dev-time driver; the checked-in corpus + CI gate (seq_diff_pg_test.rs)
 * replays the frozen goldens without needing tsx.
 *
 * Usage:
 *   TEST_CVR_PG_URI=... node fuzz.mjs [--from N] [--to M] [--stop-on-fail]
 */
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {fileURLToPath} from 'node:url';
import {generate} from './gen.mjs';
import {diffProgram, runTs} from './diff.mjs';

const dir = path.dirname(fileURLToPath(import.meta.url));
const regDir = path.join(dir, 'regressions');

function argN(flag, dflt) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? Number(process.argv[i + 1]) : dflt;
}
const FROM = argN('--from', 0);
const TO = argN('--to', 200);
const STOP = process.argv.includes('--stop-on-fail');

const tmp = p => {
  const f = path.join(os.tmpdir(), `cvr-seq-fuzz-${process.pid}.json`);
  fs.writeFileSync(f, JSON.stringify(p));
  return f;
};

// Does this program still diverge? (drivers throwing counts as a divergence too —
// a crash on one side is a finding.)
function diverges(prog) {
  try {
    return !diffProgram(tmp(prog)).ok;
  } catch (e) {
    return true;
  }
}

// Delta-debug: drop transactions, then ops within transactions, keeping only what
// is needed to still diverge.
function shrink(prog) {
  let cur = structuredClone(prog);

  // 1. Minimize transactions.
  let changed = true;
  while (changed) {
    changed = false;
    for (let i = 0; i < cur.transactions.length; i++) {
      const cand = structuredClone(cur);
      cand.transactions.splice(i, 1);
      if (cand.transactions.length && diverges(cand)) {
        cur = cand;
        changed = true;
        break;
      }
    }
  }

  // 2. Minimize ops within each surviving transaction.
  changed = true;
  while (changed) {
    changed = false;
    for (let t = 0; t < cur.transactions.length; t++) {
      const ops = cur.transactions[t].ops;
      for (let j = 0; j < ops.length; j++) {
        const cand = structuredClone(cur);
        cand.transactions[t].ops.splice(j, 1);
        // Keep transactions non-empty (an empty-op txn is a valid no-op, but drop
        // the whole txn instead via step 1 semantics).
        if (cand.transactions[t].ops.length === 0) continue;
        if (diverges(cand)) {
          cur = cand;
          changed = true;
          break;
        }
      }
      if (changed) break;
    }
  }
  return cur;
}

if (!process.env.TEST_CVR_PG_URI) {
  console.error('TEST_CVR_PG_URI unset');
  process.exit(2);
}

let found = 0;
for (let s = FROM; s < TO; s++) {
  const prog = generate(s);
  let res;
  try {
    res = diffProgram(tmp(prog));
  } catch (e) {
    console.error(`seed ${s}: driver crash — ${e.message.split('\n')[0]}`);
    res = {ok: false, diff: `driver crash: ${e.message.split('\n')[0]}`};
  }
  if (res.ok) {
    process.stdout.write(`\rseed ${s}: OK    `);
    continue;
  }
  found++;
  console.error(`\nseed ${s}: DIVERGENCE — ${res.diff}`);
  console.error(`  shrinking…`);
  const min = shrink(prog);
  const nTx = min.transactions.length;
  const nOps = min.transactions.reduce((a, t) => a + t.ops.length, 0);
  fs.mkdirSync(regDir, {recursive: true});
  const base = `seed-${s}`;
  fs.writeFileSync(path.join(regDir, `${base}.json`), JSON.stringify(min, null, 2) + '\n');
  // Freeze the TS golden trace for the minimized program (the CI gate replays it).
  const golden = runTs(path.join(regDir, `${base}.json`));
  fs.writeFileSync(
    path.join(regDir, `${base}.trace.json`),
    JSON.stringify(golden, null, 2) + '\n',
  );
  console.error(
    `  minimized to ${nTx} txn / ${nOps} ops → regressions/${base}.json (+ .trace.json)`,
  );
  if (STOP) break;
}
process.stdout.write('\n');
console.error(found ? `${found} divergence(s) found` : 'no divergences');
process.exit(found ? 1 : 0);
