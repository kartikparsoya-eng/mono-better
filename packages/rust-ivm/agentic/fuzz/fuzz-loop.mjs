#!/usr/bin/env node
// fuzz/fuzz-loop.mjs — differential fuzzer driver.
// Usage: node fuzz-loop.mjs --minutes M [--start-seed N] [--max-findings K]
// Loop: seed++ -> gen -> ts-runner (oracle) -> rust replay -> diff.
// On divergence: re-run to confirm, greedily minimize (drop pushes/rows/
// clauses while the divergence persists), save the minimized pair to
// fixtures/regressions/seed-N.*, append a fix-divergence task to the queue.
// Oracle-rejected fixtures are generator bugs -> logged + skipped.
// No AI inside. Deterministic given the seed sequence.

import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {availableParallelism} from 'node:os';
import {existsSync, mkdirSync, readFileSync, writeFileSync, appendFileSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {genFixture} from './gen.mjs';
import {appendTask} from '../lib/queue.mjs';

const pexec = promisify(execFile);
const MAX_BUF = 64 * 1024 * 1024; // fixtures can emit large JSON

const AG = dirname(dirname(fileURLToPath(import.meta.url)));
const ROOT = dirname(dirname(AG)); // Go-RS
const RUST = join(ROOT, 'rust-ivm');
const REPLAY_BIN = join(RUST, 'target', 'debug', 'replay');
const TS_RUNNER = join(AG, 'oracle', 'ts-runner.mjs');
const DIFF = join(AG, 'oracle', 'diff.mjs');
const REGRESSIONS = join(AG, 'fixtures', 'regressions');
const TMP = join(AG, 'logs', 'fuzz-tmp');
const LOG = join(AG, 'logs', 'fuzz.log');

mkdirSync(REGRESSIONS, {recursive: true});
mkdirSync(TMP, {recursive: true});

function log(msg) {
  const line = `${new Date().toISOString()} ${msg}`;
  console.log(line);
  appendFileSync(LOG, line + '\n');
}

async function runOracle(inputPath, expectedPath) {
  try {
    await pexec('node', ['--experimental-strip-types', TS_RUNNER, inputPath, '--out', expectedPath],
      {timeout: 120_000, maxBuffer: MAX_BUF});
    return {ok: true};
  } catch (e) {
    return {ok: false, err: `${e.code ?? e.signal}: ${e.stderr?.toString().slice(0, 500)}`};
  }
}

async function runRust(inputPath, actualPath) {
  try {
    const {stdout} = await pexec(REPLAY_BIN, [inputPath],
      {timeout: 60_000, maxBuffer: MAX_BUF, encoding: 'buffer'});
    writeFileSync(actualPath, stdout);
    return {ok: true};
  } catch (e) {
    return {ok: false, err: `rust replay failed (${e.code ?? e.signal}): ${e.stderr?.toString().slice(0, 500)}`};
  }
}

async function diffFiles(expectedPath, actualPath) {
  try {
    await pexec('node', [DIFF, expectedPath, actualPath], {timeout: 30_000, maxBuffer: MAX_BUF});
    return {equal: true};
  } catch (e) {
    return {equal: false, diff: e.stdout?.toString().slice(0, 1000) ?? '(no diff output)'};
  }
}

// One full differential run for a fixture object. Returns
// {verdict: 'equal'|'diverged'|'invalid'|'rust-crash', detail}
// `tag` must be unique per concurrent call — it names the scratch files, so
// distinct seeds/roles never collide even when many run in parallel.
async function tryFixture(fixture, tag) {
  const inputPath = join(TMP, `${tag}.input.json`);
  const expectedPath = join(TMP, `${tag}.expected.json`);
  const actualPath = join(TMP, `${tag}.actual.json`);
  writeFileSync(inputPath, JSON.stringify(fixture, null, 1) + '\n');
  const oracle = await runOracle(inputPath, expectedPath);
  if (!oracle.ok) return {verdict: 'invalid', detail: oracle.err};
  const rust = await runRust(inputPath, actualPath);
  if (!rust.ok) return {verdict: 'rust-crash', detail: rust.err};
  const d = await diffFiles(expectedPath, actualPath);
  if (d.equal) return {verdict: 'equal'};
  return {verdict: 'diverged', detail: d.diff};
}

// Greedy minimization: keep the divergence while shrinking. Budget-bounded.
// Runs serially (only ever called from the serialized divergence handler).
async function minimize(fixture, tag) {
  let current = structuredClone(fixture);
  let budget = 60;
  const stillDiverges = async f => {
    if (budget-- <= 0) return null;
    const r = await tryFixture(f, `${tag}-min`);
    return r.verdict === 'diverged' || r.verdict === 'rust-crash';
  };

  // 1. drop pushes (batches from the end, then singles)
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
  // 2. drop whole AST clauses
  for (const key of ['start', 'related', 'where', 'limit']) {
    if (current.ast[key] === undefined || budget <= 0) continue;
    const cand = structuredClone(current);
    delete cand.ast[key];
    if (await stillDiverges(cand)) current = cand;
  }
  // 3. drop rows per table (halves, then singles)
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

// Persist + commit a confirmed divergence. Runs under the serialize() lock so
// minimize, oracle regeneration, git add/commit, and the shared findings
// counter never interleave across workers.
async function handleDivergence(seed, fixture, r, state) {
  if (state.findings >= state.maxFindings) {
    log(`seed ${seed}: divergence found but maxFindings reached — skipping save`);
    return;
  }
  state.findings++;
  log(`seed ${seed}: DIVERGENCE (${r.verdict}) — minimizing…`);
  const min = await minimize(fixture, `seed-${seed}`);
  const base = join(REGRESSIONS, `seed-${seed}`);
  writeFileSync(`${base}.input.json`, JSON.stringify(min, null, 1) + '\n');
  await runOracle(`${base}.input.json`, `${base}.expected.json`);
  await appendTask({
    id: `divergence-seed-${seed}`,
    type: 'fix-divergence',
    source: `agentic/fixtures/regressions/seed-${seed}.input.json`,
    instructions: `Differential fuzzer found a Rust-vs-TS divergence (seed ${seed}, kind ${r.verdict}). Repro: agentic/fixtures/regressions/seed-${seed}.input.json vs .expected.json (TS oracle output). Fix the Rust engine to match TS behavior; cite the TS source lines that define the behavior. Never change the fixture or expected file. When fixed, move both files into agentic/fixtures/ so it becomes a permanent regression test. First diff: ${r.detail?.split('\n').slice(0, 3).join(' | ')}`,
  });
  log(`seed ${seed}: saved regression + queued divergence-seed-${seed}`);
  // Commit regression files so worktrees created by the orchestrator can access
  // them. Without this, fix-divergence tasks fail at gate B because the
  // regression .input.json is missing from the worktree.
  try {
    await pexec('git', ['-C', RUST, 'add', `${base}.input.json`, `${base}.expected.json`]);
    await pexec('git', ['-C', RUST, 'commit', '--no-verify', '-m', `chore(fuzz): regression seed-${seed}`]);
    log(`seed ${seed}: committed regression files`);
  } catch (e) {
    log(`seed ${seed}: WARN — could not commit regression files: ${e.message?.slice(0, 100)}`);
  }
}

async function main() {
  const args = process.argv.slice(2);
  let minutes = 5, startSeed = null, maxFindings = 3;
  let workers = Number(process.env.FUZZ_WORKERS) ||
    Math.max(2, Math.min(8, availableParallelism()));
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--minutes') minutes = Number(args[++i]);
    else if (args[i] === '--start-seed') startSeed = Number(args[++i]);
    else if (args[i] === '--max-findings') maxFindings = Number(args[++i]);
    else if (args[i] === '--workers') workers = Number(args[++i]);
  }
  if (!existsSync(REPLAY_BIN)) {
    log(`FATAL: replay binary missing at ${REPLAY_BIN} — run: cargo build --bin replay`);
    process.exit(2);
  }
  // persist the seed cursor across runs so each cycle explores new seeds
  const cursorPath = join(AG, 'fuzz', '.seed-cursor');
  const start = startSeed ?? (existsSync(cursorPath) ? Number(readFileSync(cursorPath, 'utf8')) : 1);
  const deadline = Date.now() + minutes * 60_000;
  const state = {n: 0, invalid: 0, findings: 0, maxFindings};
  // Seeds are dispatched in order; JS is single-threaded so `nextSeed++` hands
  // each worker a distinct seed without a lock. Distinct seeds => distinct file
  // tags => no scratch-file collisions across the pool.
  let nextSeed = start, stop = false;

  // Serialize the (rare) divergence-handling path: chain each call after the
  // previous so minimize/oracle/git/counter mutations never interleave.
  let chain = Promise.resolve();
  const serialize = fn => (chain = chain.then(fn, fn));

  log(`fuzz start: seed=${start} minutes=${minutes} maxFindings=${maxFindings} workers=${workers}`);

  async function worker() {
    while (!stop && Date.now() < deadline) {
      const seed = nextSeed++;
      writeFileSync(cursorPath, String(nextSeed)); // monotonic watermark
      const fixture = genFixture(seed);
      const r = await tryFixture(fixture, `seed-${seed}`);
      state.n++;
      if (r.verdict === 'invalid') {
        state.invalid++;
        if (state.invalid <= 10) log(`seed ${seed}: oracle rejected (gen bug?): ${r.detail?.split('\n')[0]}`);
        continue;
      }
      if (r.verdict === 'equal') continue;
      // confirm on a fresh run before treating it as a real divergence
      const confirm = await tryFixture(fixture, `seed-${seed}-confirm`);
      if (confirm.verdict === 'equal') {
        log(`seed ${seed}: UNSTABLE divergence (did not confirm) — flaky, investigate`);
        continue;
      }
      await serialize(() => handleDivergence(seed, fixture, r, state));
      if (state.findings >= maxFindings) stop = true; // drain the pool
    }
  }

  await Promise.all(Array.from({length: workers}, () => worker()));
  await chain; // let any in-flight divergence handler finish
  log(`fuzz done: ${state.n} seeds, ${state.invalid} invalid, ${state.findings} findings (next seed ${nextSeed})`);
}

main();
