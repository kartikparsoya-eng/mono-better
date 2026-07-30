#!/usr/bin/env node
// oracle/planner-ts-runner.mjs — runs the corpus ASTs through the TS query
// planner (the oracle) and emits the flip annotations Rust must match.
//
// Usage:
//   node --experimental-strip-types agentic/oracle/planner-ts-runner.mjs \
//        agentic/oracle/planner-corpus.json --out agentic/oracle/planner-expected.json
//
// For each case { name, tableCosts, ast } it builds a constraint-aware mock
// ConnectionCostModel (identical to the Rust test's mock), runs TS `planQuery`,
// and extracts the ordered flip list. Expected flips are produced ONLY here —
// never hand-written — so a green Rust run proves parity with zero 1.7.

import {readFileSync, writeFileSync} from 'node:fs';
import {dirname, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {existsSync} from 'node:fs';

const __dirname = dirname(fileURLToPath(import.meta.url));
function findMono(fromDir) {
  let dir = fromDir;
  for (let i = 0; i < 8; i++) {
    if (existsSync(`${dir}/packages/zql/src/planner/planner-builder.ts`)) return dir;
    dir = dirname(dir);
  }
  return resolve(fromDir, '..', '..', '..', '..');
}
const MONO = findMono(__dirname);
const {planQuery} = await import(`${MONO}/packages/zql/src/planner/planner-builder.ts`);

// A constrained read is an indexed key seek (~1 row); an unconstrained read is a
// full scan. This MUST match the Rust test's mock_cost_model exactly.
function mockCostModel(tableCosts) {
  return (table, _sort, _filters, constraint) => ({
    startupCost: 1,
    rows: constraint != null ? 1 : (tableCosts[table] ?? 100),
    fanout: (_cols) => ({fanout: 1, confidence: 'none'}),
  });
}

// Ordered flip extraction — MUST match the Rust test's extract_flips:
// WHERE conditions (pre-order DFS, recursing into each subquery's where),
// then the `related` subqueries in order.
function extractFlips(ast) {
  const flips = [];
  if (ast.where) extractFromCondition(ast.where, flips);
  for (const csq of ast.related ?? []) {
    flips.push(...extractFlips(csq.subquery));
  }
  return flips;
}
function extractFromCondition(cond, flips) {
  switch (cond.type) {
    case 'simple':
      break;
    case 'correlatedSubquery':
      flips.push(cond.flip ?? null);
      if (cond.related?.subquery?.where) {
        extractFromCondition(cond.related.subquery.where, flips);
      }
      break;
    case 'and':
    case 'or':
      for (const c of cond.conditions) extractFromCondition(c, flips);
      break;
    default:
      throw new Error(`unknown condition type: ${cond.type}`);
  }
}

const corpusPath = process.argv[2];
const outIdx = process.argv.indexOf('--out');
const outPath = outIdx >= 0 ? process.argv[outIdx + 1] : undefined;
if (!corpusPath) {
  console.error('usage: planner-ts-runner.mjs <corpus.json> [--out <expected.json>]');
  process.exit(2);
}

const corpus = JSON.parse(readFileSync(corpusPath, 'utf8'));
const expected = corpus.map(({name, tableCosts, ast}) => {
  const planned = planQuery(structuredClone(ast), mockCostModel(tableCosts ?? {}));
  return {name, flips: extractFlips(planned)};
});

const json = JSON.stringify(expected, null, 2);
if (outPath) {
  writeFileSync(outPath, json + '\n');
  console.error(`wrote ${expected.length} cases -> ${outPath}`);
} else {
  process.stdout.write(json + '\n');
}
