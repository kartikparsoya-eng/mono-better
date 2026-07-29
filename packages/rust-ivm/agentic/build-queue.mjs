#!/usr/bin/env node
// build-queue.mjs — enumerate TS test files and build queue/tasks.json.
// Deterministic, no AI. Chunks files with >25 cases into contiguous slices with
// the exact case-name list in each task's instructions. Core operators first.
// Existing tasks (by id) are preserved; fix-divergence tasks always outrank
// port-fixtures when claimed (see lib/queue.mjs).

import {readFileSync, readdirSync} from 'node:fs';
import {basename, dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {readQueue, withQueueLock, writeQueue} from './lib/queue.mjs';

const AG = dirname(fileURLToPath(import.meta.url));
const ROOT = dirname(dirname(AG)); // Go-RS
const MONO = join(ROOT, 'mono-v1.7');
const GROUPS = [
  'packages/zql/src/ivm',
  'packages/zql/src/builder',
  'packages/zqlite/src',
];
// Queue order: core operators first (mission step 8).
const ORDER = ['memory-source', 'take', 'join', 'exists', 'filter', 'fan-out-fan-in',
  'fan-in', 'push', 'skip', 'cap', 'union-fan-in', 'flipped-join'];
const MAX_CASES = 25;

// Enumerate case names: test('name'|it('name'; test.each blocks noted as one unit.
function listCases(path) {
  const src = readFileSync(path, 'utf8');
  const cases = [];
  const re = /^\s*(?:test|it)(?:\.each\([^)]*\))?\s*\(\s*(['"`])((?:\\.|(?!\1).)*)\1/gm;
  let m;
  while ((m = re.exec(src)) !== null) cases.push(m[2]);
  const eachBlocks = (src.match(/(?:test|it)\.each/g) ?? []).length;
  return {cases, eachBlocks};
}

function rankOf(file) {
  const base = basename(file);
  for (let i = 0; i < ORDER.length; i++) {
    if (base.startsWith(ORDER[i])) return i;
  }
  return ORDER.length + (file.includes('/builder/') ? 0 : 1);
}

function slugOf(file) {
  const grp = file.includes('/builder/') ? 'builder-' : file.includes('zqlite') ? 'zqlite-' : '';
  return grp + basename(file).replace(/\.test\.ts$/, '').replace(/[^a-z0-9.-]/gi, '-');
}

function buildTasks() {
  const files = [];
  for (const g of GROUPS) {
    const dir = join(MONO, g);
    for (const f of readdirSync(dir)) {
      if (!f.endsWith('.test.ts')) continue;
      if (f.includes('.perf.')) continue; // perf tests are not behavior fixtures
      files.push(join(g, f));
    }
  }
  files.sort((a, b) => rankOf(a) - rankOf(b) || a.localeCompare(b));

  const tasks = [];
  for (const rel of files) {
    const abs = join(MONO, rel);
    const {cases, eachBlocks} = listCases(abs);
    if (cases.length === 0) continue;
    const slug = slugOf(rel);
    const chunks = [];
    for (let i = 0; i < cases.length; i += MAX_CASES) chunks.push(cases.slice(i, i + MAX_CASES));
    chunks.forEach((chunk, ci) => {
      const part = chunks.length > 1 ? `-${ci + 1}of${chunks.length}` : '';
      const caseList = chunk.map((c, i) => ` ${i + 1}. ${c}`).join('\n');
      tasks.push({
        id: `fixtures-${slug}${part}`,
        type: 'port-fixtures',
        source: `mono-v1.7/${rel}`,
        instructions:
          `Translate the following ${chunk.length} test case(s) from ` +
          `/Users/kartik.parsoya/Documents/Go-RS/mono-v1.7/${rel} into fixture ` +
          `.input.json files under agentic/fixtures/, named ${slug}.<case-slug>.input.json. ` +
          `EXACT case list for this slice:\n${caseList}\n` +
          (eachBlocks > 0 ? `NOTE: file has ${eachBlocks} test.each block(s); expand each parameter row into its own fixture or SKIP with reason if not expressible.\n` : '') +
          `Cases outside the fixture schema's expressible range (timers, TTL wall-clock, ` +
          `debug/snitch message assertions, non-memory sources): list them as SKIPPED ` +
          `with one-line reasons instead of forcing them.`,
        state: 'pending',
        attempts: 0,
      });
    });
  }
  return tasks;
}

const tasks = buildTasks();
await withQueueLock(() => {
  const q = readQueue();
  const existing = new Set(q.tasks.map(t => t.id));
  let added = 0;
  for (const t of tasks) {
    if (!existing.has(t.id)) { q.tasks.push(t); added++; }
  }
  writeQueue(q);
  console.log(`queue: ${q.tasks.length} tasks (${added} added)`);
});
