#!/usr/bin/env node
// agentic/triage-regressions.mjs — classify pending regression fixtures.
//
// Uses the correct oracle runner + diff tool per fixture kind:
//   - seed-*       : replay binary + oracle/diff.mjs (memory-source path)
//   - napi-seed-*  : napi-sqlite-runner.mjs + oracle/napi-sqlite-diff.mjs
//   - adv-seed-*   : napi-advance-runner.mjs + oracle/napi-sqlite-diff.mjs
//                    (hydrate/finalView parity only; advance field lacks a
//                     TS oracle and is reported separately)
//
// Usage: node agentic/triage-regressions.mjs
// Output: agentic/logs/regression-triage.json

import {readFileSync, writeFileSync, mkdirSync} from 'node:fs';
import {execSync} from 'node:child_process';
import {dirname, basename} from 'node:path';
import {fileURLToPath} from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = dirname(__dirname);
const REGRESSIONS = `${ROOT}/agentic/fixtures/regressions`;
const ADVANCE = `${ROOT}/agentic/fixtures/advance`;
const LOGS = `${ROOT}/agentic/logs`;

mkdirSync(LOGS, {recursive: true});

function exec(cmd, opts = {}) {
  try {
    return {ok: true, stdout: execSync(cmd, {encoding: 'utf8', stdio: 'pipe', ...opts}).trim()};
  } catch (e) {
    return {ok: false, stdout: e.stdout?.toString?.() || '', stderr: e.stderr?.toString?.() || '', code: e.status};
  }
}

function listFixtures() {
  const regs = execSync(`find ${REGRESSIONS} -name '*.input.json' 2>/dev/null || true`, {encoding: 'utf8'}).trim();
  const adv = execSync(`find ${ADVANCE} -name '*.input.json' 2>/dev/null || true`, {encoding: 'utf8'}).trim();
  return [...regs.split('\n'), ...adv.split('\n')].filter(Boolean).sort();
}

function classify(name) {
  if (name.startsWith('adv-seed-')) return 'advance';
  if (name.startsWith('napi-seed-')) return 'napi';
  if (name.startsWith('seed-')) return 'memory';
  return 'unknown';
}

function runMemory(input, expected) {
  const actual = `${LOGS}/${basename(input).replace('.input.json', '.actual.json')}`;
  const replay = exec(`cd ${ROOT} && cargo run --quiet --bin replay -- "${input}" > "${actual}" 2>${LOGS}/replay-err.log`);
  if (!replay.ok) return {runner: 'replay', error: replay.stderr || 'replay failed'};
  const diff = exec(`node ${ROOT}/agentic/oracle/diff.mjs "${expected}" "${actual}"`);
  return parseDiff(diff, 'replay');
}

function runNapi(input, expected) {
  const actual = `${LOGS}/${basename(input).replace('.input.json', '.napi-actual.json')}`;
  const runner = exec(`node ${ROOT}/agentic/oracle/napi-sqlite-runner.mjs "${input}" --out "${actual}" 2>${LOGS}/napi-err.log`);
  if (!runner.ok) return {runner: 'napi-sqlite', error: runner.stderr || 'napi runner failed'};
  const diff = exec(`node ${ROOT}/agentic/oracle/napi-sqlite-diff.mjs "${expected}" "${actual}"`);
  return parseDiff(diff, 'napi-sqlite');
}

function runAdvance(input, expected) {
  const actual = `${LOGS}/${basename(input).replace('.input.json', '.adv-actual.json')}`;
  const runner = exec(`node ${ROOT}/agentic/oracle/napi-advance-runner.mjs "${input}" --out "${actual}" 2>${LOGS}/advance-err.log`);
  if (!runner.ok) return {runner: 'napi-advance', error: runner.stderr || 'advance runner failed'};

  const diff = exec(`node ${ROOT}/agentic/oracle/napi-advance-diff.mjs "${expected}" "${actual}"`);
  return parseDiff(diff, 'napi-advance');
}

function parseDiff(diff, runner) {
  if (diff.ok) return {runner, status: 'equal'};
  const lines = diff.stderr.split('\n').filter(Boolean);
  const pathLine = lines.find(l => l.startsWith('DIFF at') || l.startsWith('HYDRATE DIFF:') || l.startsWith('FINAL VIEW DIFF:'));
  const expectedLine = lines.find(l => l.includes('expected:') || l.includes('missing from napi'));
  const actualLine = lines.find(l => l.includes('actual:') || l.includes('extra in napi'));
  const path = pathLine
    ? pathLine.replace('DIFF at ', '').replace('HYDRATE DIFF: ', '').replace('FINAL VIEW DIFF: ', '').trim()
    : '';
  return {
    runner,
    status: 'diverged',
    path,
    expected: expectedLine ? expectedLine.trim() : '',
    actual: actualLine ? actualLine.trim() : '',
    raw: diff.stderr,
  };
}

function main() {
  const fixtures = listFixtures();
  const results = [];
  let equalCount = 0;
  let errorCount = 0;

  for (const input of fixtures) {
    const name = basename(input, '.input.json');
    const expected = input.replace('.input.json', '.expected.json');
    const kind = classify(name);

    let res;
    if (kind === 'memory') res = runMemory(input, expected);
    else if (kind === 'napi') res = runNapi(input, expected);
    else if (kind === 'advance') res = runAdvance(input, expected);
    else res = {runner: 'unknown', error: 'unknown fixture prefix'};

    if (res.status === 'equal') equalCount++;
    if (res.error) errorCount++;

    results.push({name, kind, input, ...res});
  }

  // Group divergences by runner + first path token
  const groups = {};
  for (const r of results.filter(x => x.status === 'diverged' && !x.error)) {
    const token = r.path.split(/[\[\.]/).filter(Boolean)[0] || 'other';
    const key = `${r.kind}|${token}|${r.runner}`;
    groups[key] = groups[key] || {key, kind: r.kind, runner: r.runner, token, count: 0, fixtures: []};
    groups[key].count++;
    groups[key].fixtures.push({name: r.name, path: r.path, expected: r.expected, actual: r.actual});
  }

  const report = {
    total: fixtures.length,
    equal: equalCount,
    errored: errorCount,
    diverged: fixtures.length - equalCount - errorCount,
    results,
    groups: Object.values(groups).sort((a, b) => b.count - a.count),
  };

  const outPath = `${LOGS}/regression-triage.json`;
  writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(`Triage complete: ${report.total} fixtures`);
  console.log(`  equal:    ${report.equal}`);
  console.log(`  diverged: ${report.diverged}`);
  console.log(`  errored:  ${report.errored}`);
  console.log(`Groups:`);
  for (const g of report.groups) {
    console.log(`  ${g.count} x ${g.kind}/${g.token} (${g.runner})`);
  }
  console.log(`Wrote ${outPath}`);
}

main();
