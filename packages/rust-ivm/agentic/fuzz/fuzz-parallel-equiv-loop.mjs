#!/usr/bin/env node
// fuzz/fuzz-parallel-equiv-loop.mjs — parallel-hydrate equivalence fuzzer.
//
// For each seed: gen → rust replay (parallel=1) vs rust replay (parallel=0).
// Any divergence is a parallel-hydrate bug. This is separate from the TS-oracle
// fuzzer because parallel hydrate is supposed to be byte-identical to serial.
//
// Usage: node fuzz-parallel-equiv-loop.mjs --minutes M [--start-seed N] [--max-findings K]

import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {availableParallelism} from 'node:os';
import {existsSync, mkdirSync, readFileSync, writeFileSync, appendFileSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {genFixture} from './gen.mjs';
import {appendTask} from '../lib/queue.mjs';

const pexec = promisify(execFile);
const MAX_BUF = 64 * 1024 * 1024;

const AG = dirname(dirname(fileURLToPath(import.meta.url)));
const ROOT = dirname(dirname(AG));
const RUST = join(ROOT, 'rust-ivm');
const REPLAY_BIN = join(RUST, 'target', 'debug', 'replay');
const DIFF = join(AG, 'oracle', 'diff.mjs');
const REGRESSIONS = join(AG, 'fixtures', 'regressions');
const TMP = join(AG, 'logs', 'fuzz-tmp');
const LOG = join(AG, 'logs', 'fuzz-parallel-equiv.log');

mkdirSync(REGRESSIONS, {recursive: true});
mkdirSync(TMP, {recursive: true});

function log(msg) {
  const line = `${new Date().toISOString()} ${msg}`;
  console.log(line);
  appendFileSync(LOG, line + '\n');
}

async function runReplay(inputPath, actualPath, parallel) {
  try {
    const env = {...process.env, RUST_IVM_PARALLEL_HYDRATE: parallel ? '1' : '0'};
    const {stdout} = await pexec(REPLAY_BIN, [inputPath],
      {timeout: 60_000, maxBuffer: MAX_BUF, encoding: 'buffer', env});
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

async function tryFixture(fixture, tag) {
  const inputPath = join(TMP, `${tag}.input.json`);
  const serialPath = join(TMP, `${tag}.serial.json`);
  const parallelPath = join(TMP, `${tag}.parallel.json`);
  writeFileSync(inputPath, JSON.stringify(fixture, null, 1) + '\n');

  const serial = await runReplay(inputPath, serialPath, false);
  if (!serial.ok) return {verdict: 'serial-crash', detail: serial.err};

  const parallel = await runReplay(inputPath, parallelPath, true);
  if (!parallel.ok) return {verdict: 'parallel-crash', detail: parallel.err};

  const d = await diffFiles(serialPath, parallelPath);
  if (d.equal) return {verdict: 'equal'};
  return {verdict: 'diverged', detail: d.diff};
}

async function minimize(fixture, tag) {
  let current = structuredClone(fixture);
  let budget = 40;
  const stillDiverges = async f => {
    if (budget-- <= 0) return null;
    const r = await tryFixture(f, `${tag}-min`);
    return r.verdict === 'diverged';
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
  log(`seed ${seed}: PARALLEL DIVERGENCE — minimizing…`);
  const min = await minimize(fixture, `seed-${seed}`);
  const base = join(REGRESSIONS, `parallel-seed-${seed}`);
  writeFileSync(`${base}.input.json`, JSON.stringify(min, null, 1) + '\n');

  // Generate expected output from the SERIAL path (the oracle for parallel).
  const serialExpected = `${base}.expected.json`;
  const inputPath = `${base}.input.json`;
  const serialOk = await runReplay(inputPath, serialExpected, false);
  if (!serialOk.ok) {
    log(`seed ${seed}: WARN — serial replay failed for minimized fixture`);
    return;
  }

  // Append a fix task so the queue knows this is a parallel-hydrate bug.
  appendTask({
    source: `${base}.input.json`,
    instructions: `Parallel-hydrate equivalence fuzzer found a divergence (seed ${seed}). Serial replay output is in ${base}.expected.json. Fix the Rust parallel-hydrate path so RUST_IVM_PARALLEL_HYDRATE=1 replay matches RUST_IVM_PARALLEL_HYDRATE=0 replay.`,
  });
  log(`seed ${seed}: saved parallel regression`);
  try {
    await pexec('git', ['-C', RUST, 'add', `${base}.input.json`, `${base}.expected.json`]);
    await pexec('git', ['-C', RUST, 'commit', '--no-verify', '-m', `chore(fuzz): parallel-hydrate regression seed-${seed}`]);
    log(`seed ${seed}: committed`);
  } catch (e) {
    log(`seed ${seed}: WARN — could not commit: ${e.message?.slice(0, 100)}`);
  }
}

async function main() {
  const args = process.argv.slice(2);
  let minutes = 5, startSeed = null, maxFindings = 5;
  let workers = Number(process.env.FUZZ_WORKERS) ||
    Math.max(1, Math.min(4, availableParallelism() - 2));
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--minutes') minutes = Number(args[++i]);
    else if (args[i] === '--start-seed') startSeed = Number(args[++i]);
    else if (args[i] === '--max-findings') maxFindings = Number(args[++i]);
    else if (args[i] === '--workers') workers = Number(args[++i]);
  }

  const cursorPath = join(AG, 'fuzz', '.parallel-seed-cursor');
  const start = startSeed ?? (existsSync(cursorPath) ? Number(readFileSync(cursorPath, 'utf8')) : 1);
  const deadline = Date.now() + minutes * 60_000;
  const state = {n: 0, findings: 0, maxFindings};
  let nextSeed = start, stop = false;

  let chain = Promise.resolve();
  const serialize = fn => (chain = chain.then(fn, fn));

  log(`parallel-equiv fuzz start: seed=${start} minutes=${minutes} maxFindings=${maxFindings} workers=${workers}`);

  async function worker() {
    while (!stop && Date.now() < deadline) {
      const seed = nextSeed++;
      writeFileSync(cursorPath, String(nextSeed));
      const fixture = genFixture(seed);
      const r = await tryFixture(fixture, `seed-${seed}`);
      state.n++;
      if (r.verdict === 'equal') continue;
      const confirm = await tryFixture(fixture, `seed-${seed}-confirm`);
      if (confirm.verdict === 'equal') {
        log(`seed ${seed}: UNSTABLE parallel divergence (did not confirm) — investigate`);
        continue;
      }
      await serialize(() => handleDivergence(seed, fixture, r, state));
      if (state.findings >= maxFindings) stop = true;
    }
  }

  await Promise.all(Array.from({length: workers}, () => worker()));
  await chain;
  log(`parallel-equiv fuzz done: ${state.n} seeds, ${state.findings} findings (next seed ${nextSeed})`);
}

main();
