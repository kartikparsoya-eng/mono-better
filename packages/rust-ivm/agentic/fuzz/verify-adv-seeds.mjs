#!/usr/bin/env node
// Verify specific advance-fuzzer seeds (ad-hoc regression check).
// Usage: node verify-adv-seeds.mjs 335 762 934
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { genFixture } from './gen.mjs';

const pexec = promisify(execFile);
const MAX_BUF = 64 * 1024 * 1024;
const AG = dirname(dirname(fileURLToPath(import.meta.url)));
const TS_RUNNER = join(AG, 'oracle', 'ts-runner.mjs');
const NAPI_RUNNER = join(AG, 'oracle', 'napi-advance-runner.mjs');
const NAPI_DIFF = join(AG, 'oracle', 'napi-sqlite-diff.mjs');
const TMP = join(AG, 'logs', 'fuzz-tmp');
mkdirSync(TMP, { recursive: true });

async function tryFixture(fixture, tag) {
  const inputPath = join(TMP, `verify-${tag}.input.json`);
  const expectedPath = join(TMP, `verify-${tag}.expected.json`);
  const actualPath = join(TMP, `verify-${tag}.napi-actual.json`);
  writeFileSync(inputPath, JSON.stringify(fixture, null, 1) + '\n');
  try {
    await pexec('node', ['--experimental-strip-types', TS_RUNNER, inputPath, '--out', expectedPath],
      { timeout: 120_000, maxBuffer: MAX_BUF });
  } catch (e) { return { verdict: 'invalid', detail: `${e.code ?? e.signal}: ${e.stderr?.toString().slice(0, 300)}` }; }
  try {
    await pexec('node', [NAPI_RUNNER, inputPath, '--out', actualPath], { timeout: 60_000, maxBuffer: MAX_BUF });
  } catch (e) { return { verdict: 'napi-crash', detail: `${e.code ?? e.signal}: ${e.stderr?.toString().slice(0, 600)}` }; }
  try {
    await pexec('node', [NAPI_DIFF, expectedPath, actualPath], { timeout: 30_000, maxBuffer: MAX_BUF });
    return { verdict: 'equal' };
  } catch (e) { return { verdict: 'diverged', detail: e.stdout?.toString().slice(0, 1200) ?? '(no diff)' }; }
}

const seeds = process.argv.slice(2).map(Number);
let fail = 0;
for (const seed of seeds) {
  const r = await tryFixture(genFixture(seed), `seed-${seed}`);
  const ok = r.verdict === 'equal' || r.verdict === 'invalid';
  if (!ok) fail++;
  console.log(`seed ${seed}: ${r.verdict}${r.detail ? ' — ' + r.detail.split('\n')[0] : ''}`);
}
console.log(fail === 0 ? '\nALL CLEAN' : `\n${fail} FAILED`);
process.exit(fail === 0 ? 0 : 1);
