#!/usr/bin/env node
// replay-seed.mjs — hammer a single seed through the napi differential N times.
// The napi streaming path is nondeterministic (seed 308 dropped the last row
// ~50% of runs before the drain_barrier fix). Oracle is deterministic, so we
// compute expected once and re-run the napi side N times, counting divergences.
//
// Usage: node replay-seed.mjs <seed> <iterations>
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdirSync, writeFileSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {genFixture} from './gen.mjs';

const pexec = promisify(execFile);
const MAX_BUF = 64 * 1024 * 1024;
const AG = dirname(dirname(fileURLToPath(import.meta.url)));
const TS_RUNNER = join(AG, 'oracle', 'ts-runner.mjs');
const NAPI_RUNNER = join(AG, 'oracle', 'napi-sqlite-runner.mjs');
const NAPI_DIFF = join(AG, 'oracle', 'napi-sqlite-diff.mjs');
const TMP = join(AG, 'logs', 'fuzz-tmp');
mkdirSync(TMP, {recursive: true});

const seed = Number(process.argv[2] ?? 308);
const iters = Number(process.argv[3] ?? 30);

const input = join(TMP, `replay-${seed}.input.json`);
const expected = join(TMP, `replay-${seed}.expected.json`);
const actual = join(TMP, `replay-${seed}.actual.json`);

writeFileSync(input, JSON.stringify(genFixture(seed)));
await pexec('node', ['--experimental-strip-types', TS_RUNNER, input, '--out', expected],
  {timeout: 120_000, maxBuffer: MAX_BUF});

let diverged = 0;
for (let i = 0; i < iters; i++) {
  await pexec('node', [NAPI_RUNNER, input, '--out', actual], {timeout: 120_000, maxBuffer: MAX_BUF});
  try {
    await pexec('node', [NAPI_DIFF, expected, actual], {timeout: 60_000, maxBuffer: MAX_BUF});
  } catch (e) {
    diverged++;
    const d = e.stdout?.toString().slice(0, 400) ?? '(no diff)';
    console.log(`  iter ${i}: DIVERGED\n${d}`);
  }
}
console.log(`seed ${seed}: ${iters - diverged}/${iters} clean, ${diverged} diverged`);
process.exit(diverged > 0 ? 1 : 0);
