#!/usr/bin/env node
// status.mjs — one-screen loop status from queue + logs.
import {existsSync, readFileSync, readdirSync, statSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {readQueue} from './lib/queue.mjs';

const AG = dirname(fileURLToPath(import.meta.url));
const q = readQueue();
const by = {};
for (const t of q.tasks) by[t.state] = (by[t.state] ?? 0) + 1;
console.log('=== rust-ivm agentic loop status ===');
console.log('queue:', Object.entries(by).map(([k, v]) => `${k}=${v}`).join('  '), `(total ${q.tasks.length})`);
for (const t of q.tasks.filter(t => t.state === 'in_progress')) {
  console.log(`  in_progress: ${t.id} worker=${t.worker} attempts=${t.attempts} since=${t.updatedAt}`);
}
const reg = join(AG, 'fixtures', 'regressions');
const regs = existsSync(reg) ? readdirSync(reg).filter(f => f.endsWith('.input.json')) : [];
console.log(`regressions pending: ${regs.length}`, regs.slice(0, 8).join(', '));
const fixtures = readdirSync(join(AG, 'fixtures')).filter(f => f.endsWith('.input.json'));
console.log(`fixtures (passing): ${fixtures.length}`);
const nh = join(AG, 'needs-human.md');
if (existsSync(nh)) {
  const entries = (readFileSync(nh, 'utf8').match(/^## /gm) ?? []).length;
  console.log(`needs-human.md entries: ${entries}`);
}
for (const f of ['loop.out', 'fuzz.log']) {
  const p = join(AG, 'logs', f);
  if (!existsSync(p)) continue;
  const lines = readFileSync(p, 'utf8').trimEnd().split('\n');
  const age = ((Date.now() - statSync(p).mtimeMs) / 60000).toFixed(1);
  console.log(`--- ${f} (last write ${age} min ago) ---`);
  for (const l of lines.slice(-4)) console.log(' ', l);
}
