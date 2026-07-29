// lib/queue.mjs — shared task-queue access with a simple mkdir lockfile.
// Used by build-queue.mjs, orchestrate.mjs, fuzz/fuzz-loop.mjs, status.mjs.
// tasks.json shape: {"tasks": [{id, type, source?, instructions, state,
//   attempts, worker?, updatedAt?}]}
// states: pending | in_progress | done | failed | divergence-pending
// (divergence *tasks* have type "fix-divergence" and jump the queue).

import {existsSync, mkdirSync, readFileSync, rmdirSync, writeFileSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';

const AG = dirname(dirname(fileURLToPath(import.meta.url)));
export const QUEUE_PATH = join(AG, 'queue', 'tasks.json');
const LOCK_DIR = join(AG, 'queue', '.lock');

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

export async function withQueueLock(fn) {
  const deadline = Date.now() + 60_000;
  for (;;) {
    try {
      mkdirSync(LOCK_DIR);
      break;
    } catch {
      if (Date.now() > deadline) throw new Error('queue lock timeout (stale .lock?)');
      await sleep(100 + Math.random() * 150);
    }
  }
  try {
    return await fn();
  } finally {
    try { rmdirSync(LOCK_DIR); } catch {}
  }
}

export function readQueue() {
  if (!existsSync(QUEUE_PATH)) return {tasks: []};
  return JSON.parse(readFileSync(QUEUE_PATH, 'utf8'));
}

export function writeQueue(q) {
  mkdirSync(dirname(QUEUE_PATH), {recursive: true});
  const tmp = QUEUE_PATH + '.tmp';
  writeFileSync(tmp, JSON.stringify(q, null, 1) + '\n');
  // atomic-enough rename on the same fs
  writeFileSync(QUEUE_PATH, readFileSync(tmp));
}

export async function appendTask(task) {
  return withQueueLock(() => {
    const q = readQueue();
    if (q.tasks.some(t => t.id === task.id)) return false;
    q.tasks.push({state: 'pending', attempts: 0, ...task});
    writeQueue(q);
    return true;
  });
}

export async function updateTask(id, patch) {
  return withQueueLock(() => {
    const q = readQueue();
    const t = q.tasks.find(t => t.id === id);
    if (!t) return false;
    Object.assign(t, patch, {updatedAt: new Date().toISOString()});
    writeQueue(q);
    return true;
  });
}

// Claim the next runnable task: fix-divergence tasks first, then queue order.
export async function claimNextTask(worker, taskId = null) {
  return withQueueLock(() => {
    const q = readQueue();
    const pending = q.tasks.filter(t => t.state === 'pending');
    const next = taskId
      ? pending.find(t => t.id === taskId)
      : pending[0];
    if (!next) return null;
    next.state = 'in_progress';
    next.worker = worker;
    next.updatedAt = new Date().toISOString();
    writeQueue(q);
    return structuredClone(next);
  });
}
