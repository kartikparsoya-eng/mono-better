#!/usr/bin/env node
// fuzz/fuzz-napi-loop.mjs — napi/TableSource differential fuzzer.
//
// For each seed: gen → ts-runner (oracle) → napi-sqlite-runner (real addon)
// → napi-sqlite-diff. This exercises the napi/TableSource boundary where
// all 3 real bugs lived (serialization, IN operator, panic-abort).
//
// The MemorySource fuzzer (fuzz-loop.mjs) is orthogonal — it catches engine
// logic bugs. This fuzzer catches boundary bugs that only manifest when
// the real napi addon deserializes the AST via json_to_value and fetches
// from SQLite via TableSource.
//
// Usage: node fuzz-napi-loop.mjs --minutes M [--start-seed N] [--max-findings K]
// Start: nohup node agentic/fuzz/fuzz-napi-loop.mjs --minutes 10 >> logs/fuzz-napi.out 2>&1 &

import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { availableParallelism } from 'node:os';
import { existsSync, mkdirSync, readFileSync, writeFileSync, appendFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { genFixture } from './gen.mjs';
import { appendTask } from '../lib/queue.mjs';

const pexec = promisify(execFile);
const MAX_BUF = 64 * 1024 * 1024;

const AG = dirname(dirname(fileURLToPath(import.meta.url)));
const ROOT = dirname(dirname(AG));
const RUST = join(ROOT, 'rust-ivm');
const TS_RUNNER = join(AG, 'oracle', 'ts-runner.mjs');
const NAPI_RUNNER = join(AG, 'oracle', 'napi-sqlite-runner.mjs');
const NAPI_DIFF = join(AG, 'oracle', 'napi-sqlite-diff.mjs');
const REGRESSIONS = join(AG, 'fixtures', 'regressions');
const TMP = join(AG, 'logs', 'fuzz-tmp');
const LOG = join(AG, 'logs', 'fuzz-napi.log');

mkdirSync(REGRESSIONS, { recursive: true });
mkdirSync(TMP, { recursive: true });

function log(msg) {
  const line = `${new Date().toISOString()} ${msg}`;
  console.log(line);
  appendFileSync(LOG, line + '\n');
}

async function runOracle(inputPath, expectedPath) {
  try {
    await pexec('node', ['--experimental-strip-types', TS_RUNNER, inputPath, '--out', expectedPath],
      { timeout: 120_000, maxBuffer: MAX_BUF });
    return { ok: true };
  } catch (e) {
    return { ok: false, err: `${e.code ?? e.signal}: ${e.stderr?.toString().slice(0, 500)}` };
  }
}

async function runNapi(inputPath, actualPath) {
  try {
    await pexec('node', [NAPI_RUNNER, inputPath, '--out', actualPath],
      { timeout: 60_000, maxBuffer: MAX_BUF });
    return { ok: true };
  } catch (e) {
    return { ok: false, err: `napi runner failed (${e.code ?? e.signal}): ${e.stderr?.toString().slice(0, 500)}` };
  }
}

async function diffFiles(expectedPath, actualPath) {
  try {
    await pexec('node', [NAPI_DIFF, expectedPath, actualPath],
      { timeout: 30_000, maxBuffer: MAX_BUF });
    return { equal: true };
  } catch (e) {
    return { equal: false, diff: e.stdout?.toString().slice(0, 2000) ?? e.stderr?.toString().slice(0, 500) ?? '(no diff output)' };
  }
}

// One full differential run for a fixture object.
async function tryFixture(fixture, tag) {
  const inputPath = join(TMP, `napi-${tag}.input.json`);
  const expectedPath = join(TMP, `napi-${tag}.expected.json`);
  const actualPath = join(TMP, `napi-${tag}.napi-actual.json`);
  writeFileSync(inputPath, JSON.stringify(fixture, null, 1) + '\n');

  const oracle = await runOracle(inputPath, expectedPath);
  if (!oracle.ok) return { verdict: 'invalid', detail: oracle.err };

  const napi = await runNapi(inputPath, actualPath);
  if (!napi.ok) return { verdict: 'napi-crash', detail: napi.err };

  const d = await diffFiles(expectedPath, actualPath);
  if (d.equal) return { verdict: 'equal' };
  return { verdict: 'diverged', detail: d.diff };
}

// Greedy minimization (same strategy as fuzz-loop.mjs)
async function minimize(fixture, tag) {
  let current = structuredClone(fixture);
  let budget = 40;
  const stillDiverges = async f => {
    if (budget-- <= 0) return null;
    const r = await tryFixture(f, `napi-${tag}-min`);
    return r.verdict === 'diverged' || r.verdict === 'napi-crash';
  };

  for (const chunk of [8, 4, 1]) {
    let i = current.pushes.length - chunk;
    while (i >= 0 && budget > 0) {
      const cand = structuredClone(current);
      cand.pushes.splice(i, chunk);
      const v = await stillDiverges(cand);
      if (v === null) break;
      if (v) current = cand; else i -= chunk;
    }
  }
  for (const key of ['start', 'related', 'where', 'limit']) {
    if (current.ast[key] === undefined || budget <= 0) continue;
    const cand = structuredClone(current);
    delete cand.ast[key];
    if (await stillDiverges(cand)) current = cand;
  }
  for (const tname of Object.keys(current.tables)) {
    for (const frac of [0.5, 0.25]) {
      if (budget <= 0) break;
      const cand = structuredClone(current);
      const rows = cand.tables[tname].rows;
      const n = Math.max(1, Math.floor(rows.length * frac));
      cand.tables[tname].rows = rows.slice(0, rows.length - n);
      if (await stillDiverges(cand)) current = cand;
    }
    let i = (current.tables[tname].rows.length) - 1;
    while (i >= 0 && budget > 0) {
      const cand = structuredClone(current);
      cand.tables[tname].rows.splice(i, 1);
      if (await stillDiverges(cand)) current = cand;
      i--;
    }
  }
  return current;
}

async function handleDivergence(seed, fixture, r, state) {
  if (state.findings >= state.maxFindings) {
    log(`seed ${seed}: divergence found but maxFindings reached — skipping save`);
    return;
  }
  state.findings++;
  log(`seed ${seed}: NAPI DIVERGENCE (${r.verdict}) — minimizing…`);
  const min = await minimize(fixture, `seed-${seed}`);
  const base = join(REGRESSIONS, `napi-seed-${seed}`);
  writeFileSync(`${base}.input.json`, JSON.stringify(min, null, 1) + '\n');
  await runOracle(`${base}.input.json`, `${base}.expected.json`);
  await appendTask({
    id: `napi-divergence-seed-${seed}`,
    type: 'fix-divergence',
    source: `agentic/fixtures/regressions/napi-seed-${seed}.input.json`,
    instructions: `NAPI/TableSource differential fuzzer found a divergence (seed ${seed}, kind ${r.verdict}). This is at the napi boundary, NOT the core engine. Repro: agentic/fixtures/regressions/napi-seed-${seed}.input.json vs .expected.json. First diff: ${r.detail?.split('\n').slice(0, 3).join(' | ')}`,
  });
  log(`seed ${seed}: saved napi regression + queued napi-divergence-seed-${seed}`);
  try {
    await pexec('git', ['-C', RUST, 'add', `${base}.input.json`, `${base}.expected.json`]);
    await pexec('git', ['-C', RUST, 'commit', '--no-verify', '-m', `chore(fuzz): napi regression seed-${seed}`]);
    log(`seed ${seed}: committed napi regression files`);
  } catch (e) {
    log(`seed ${seed}: WARN — could not commit: ${e.message?.slice(0, 100)}`);
  }
}

async function main() {
  const args = process.argv.slice(2);
  let minutes = 5, startSeed = null, maxFindings = 3;
  let workers = Number(process.env.FUZZ_WORKERS) ||
    Math.max(1, Math.min(4, availableParallelism() - 2)); // napi is heavier, use fewer workers
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--minutes') minutes = Number(args[++i]);
    else if (args[i] === '--start-seed') startSeed = Number(args[++i]);
    else if (args[i] === '--max-findings') maxFindings = Number(args[++i]);
    else if (args[i] === '--workers') workers = Number(args[++i]);
  }

  const cursorPath = join(AG, 'fuzz', '.napi-seed-cursor');
  const start = startSeed ?? (existsSync(cursorPath) ? Number(readFileSync(cursorPath, 'utf8')) : 1);
  const deadline = Date.now() + minutes * 60_000;
  const state = { n: 0, invalid: 0, findings: 0, maxFindings };
  let nextSeed = start, stop = false;

  let chain = Promise.resolve();
  const serialize = fn => (chain = chain.then(fn, fn));

  log(`napi fuzz start: seed=${start} minutes=${minutes} maxFindings=${maxFindings} workers=${workers}`);

  async function worker() {
    while (!stop && Date.now() < deadline) {
      const seed = nextSeed++;
      writeFileSync(cursorPath, String(nextSeed));
      const fixture = genFixture(seed);
      const r = await tryFixture(fixture, `seed-${seed}`);
      state.n++;
      if (r.verdict === 'invalid') {
        state.invalid++;
        if (state.invalid <= 10) log(`seed ${seed}: oracle rejected: ${r.detail?.split('\n')[0]}`);
        continue;
      }
      if (r.verdict === 'equal') continue;
      // confirm
      const confirm = await tryFixture(fixture, `seed-${seed}-confirm`);
      if (confirm.verdict === 'equal') {
        log(`seed ${seed}: UNSTABLE napi divergence (did not confirm) — investigate`);
        continue;
      }
      await serialize(() => handleDivergence(seed, fixture, r, state));
      if (state.findings >= maxFindings) stop = true;
    }
  }

  await Promise.all(Array.from({ length: workers }, () => worker()));
  await chain;
  log(`napi fuzz done: ${state.n} seeds, ${state.invalid} invalid, ${state.findings} findings (next seed ${nextSeed})`);
}

main();
