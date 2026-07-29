#!/usr/bin/env node
// orchestrate.mjs — THE loop driver. Deterministic, no AI logic inside.
// Usage: node orchestrate.mjs [--workers N] [--once]
// Lifecycle per task: claim -> worktree -> IMPLEMENT (xyne) -> GATES ->
// 2x REVIEW (xyne) -> commit | retry(<3) | failed/divergence -> needs-human.
// Safety: max 2 workers, disk>10GB + load<10 checks, SIGINT-safe, idempotent.
// Clippy gate SKIPPED: baseline has 215 warnings (recorded in SETUP-REPORT.md).

import {execFileSync, spawn} from 'node:child_process';
import {appendFileSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync} from 'node:fs';
import os from 'node:os';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {claimNextTask, readQueue, updateTask, appendTask} from './lib/queue.mjs';

const AG = dirname(fileURLToPath(import.meta.url));
const RUST = dirname(AG);              // rust-ivm main checkout
const ROOT = dirname(RUST);            // Go-RS
const LOGS = join(AG, 'logs');
const NEEDS_HUMAN = join(AG, 'needs-human.md');
const KEYS = JSON.parse(readFileSync(join(AG, 'keys.json'), 'utf8'));

const IMPLEMENT_TIMEOUT_MS = 30 * 60_000;
const REVIEW_TIMEOUT_MS = 15 * 60_000;
const MAX_ATTEMPTS = 3;

mkdirSync(LOGS, {recursive: true});

function log(msg) {
  const line = `${new Date().toISOString()} [orch] ${msg}`;
  console.log(line);
  appendFileSync(join(LOGS, 'loop.out'), line + '\n');
}

function sh(args, opts = {}) {
  return execFileSync(args[0], args.slice(1), {
    encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'],
    maxBuffer: 64 * 1024 * 1024, ...opts,
  });
}

// ---- per-process key slots (step 8.5): concurrently-running xyne processes
// always hold distinct key indexes; index (never the key) is logged.
const busySlots = new Set();
function takeSlot() {
  let s = 0;
  while (busySlots.has(s)) s++;
  busySlots.add(s);
  return s;
}
function releaseSlot(s) { busySlots.delete(s); }

function xyneEnv(slot) {
  const key = KEYS.keys[slot % KEYS.keys.length];
  return {
    ...process.env,
    LITE_LLM: '1',
    LITE_LLM_URL: KEYS.url,
    LITE_LLM_API_KEY: key,
    LITE_LLM_MODEL: KEYS.model,
  };
}

// Hard tool allowlists (xyne --tools is a verified hard allowlist). The xyne
// write/edit tools crash the whole agent process on .rs files (observed 4x),
// and subagent carries the full default toolset — never expose them.
const IMPLEMENTER_TOOLS = 'bash,read,grep,find,ls,todo';
const REVIEWER_TOOLS = 'read,grep,find,ls';

// Spawn one xyne prompt; capture full output; enforce timeout in-process
// (this Mac has no `timeout` binary). Returns {code, output, timedOut}.
function runXyne({prompt, systemPrompt, cwd, stdinText, timeoutMs, logFile, slot, extraEnv = {}, tools = IMPLEMENTER_TOOLS}) {
  return new Promise(resolve => {
    const args = ['prompt', prompt, '--yolo', `--tools=${tools}`];
    if (systemPrompt) args.push(`--system-prompt=${systemPrompt}`);
    const child = spawn('xyne', args, {cwd, env: {...xyneEnv(slot), ...extraEnv}, stdio: ['pipe', 'pipe', 'pipe']});
    let out = '';
    const onData = d => {
      out += d.toString();
      try { appendFileSync(logFile, d); } catch {}
    };
    child.stdout.on('data', onData);
    child.stderr.on('data', onData);
    if (stdinText) child.stdin.write(stdinText);
    child.stdin.end();
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; child.kill('SIGKILL'); }, timeoutMs);
    child.on('close', code => {
      clearTimeout(timer);
      resolve({code, output: out, timedOut});
    });
    child.on('error', err => {
      clearTimeout(timer);
      resolve({code: -1, output: out + `\nSPAWN-ERROR: ${err.message}`, timedOut});
    });
  });
}

// Transient network failure (does NOT count against task attempts).
function isTransient(res) {
  if (res.timedOut) return false;
  const t = res.output;
  if (res.code !== 0 && t.trim() === '') return true;
  return /ECONNREFUSED|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|ECONNRESET|socket hang up|gateway|HTTP 5\d\d|502|503|504/i.test(t) && res.code !== 0;
}
function isRateLimited(res) {
  return /429|rate.?limit|too many requests|concurrent/i.test(res.output) && res.code !== 0;
}

async function probeUrlUntilUp() {
  for (;;) {
    try {
      await fetch(KEYS.url, {signal: AbortSignal.timeout(10_000)});
      return;
    } catch {
      log(`LITE_LLM_URL unreachable — pausing 10 min`);
      await new Promise(r => setTimeout(r, 10 * 60_000));
    }
  }
}

// ---- worktree management -------------------------------------------------
function cleanStaleWorktrees() {
  try {
    for (const d of readdirSync(ROOT)) {
      if (d.startsWith('wt-')) {
        try { sh(['git', '-C', RUST, 'worktree', 'remove', '--force', join(ROOT, d)]); }
        catch { try { rmSync(join(ROOT, d), {recursive: true, force: true}); } catch {} }
      }
    }
    sh(['git', '-C', RUST, 'worktree', 'prune']);
  } catch (e) { log(`stale worktree cleanup: ${e.message}`); }
}

function makeWorktree(taskId) {
  const wt = join(ROOT, `wt-${taskId}`);
  sh(['git', '-C', RUST, 'worktree', 'add', wt, 'HEAD']);
  return wt;
}
function removeWorktree(wt) {
  try { sh(['git', '-C', RUST, 'worktree', 'remove', '--force', wt]); }
  catch { try { rmSync(wt, {recursive: true, force: true}); } catch {} }
  try { sh(['git', '-C', RUST, 'worktree', 'prune']); } catch {}
}

// ---- gates -----------------------------------------------------------------
// Gate A: only allowed paths changed. port-fixtures: ONLY new (untracked/added)
// *.input.json / *.expected.json under agentic/fixtures/. fix-divergence: also
// modifications under src/**, and deletions limited to agentic/fixtures/regressions/
// (promoting a fixed regression pair). Everything else = TAMPER (no retry).
function gateA(wt, task) {
  const porcelain = sh(['git', '-C', wt, 'status', '--porcelain']).trimEnd();
  if (porcelain === '') return {ok: false, tamper: false, detail: 'no changes produced'};
  const violations = [];
  for (const line of porcelain.split('\n')) {
    const st = line.slice(0, 2);
    let rawPath = line.slice(3).trim();
    let oldPath = null;
    if (rawPath.includes(' -> ')) {
      const parts = rawPath.split(' -> ');
      oldPath = parts[0].trim();
      rawPath = parts[1].trim();
    }
    const path = rawPath;
    const isNew = st === '??' || st.trim() === 'A';
    const isFixture = /^agentic\/fixtures\/.*\.(input|expected)\.json$/.test(path);
    // The implementer prompt instructs workers to record schema-inexpressible
    // cases (timers/TTL/non-memory sources) as SKIPPED with reasons. Allow that
    // summary file — it is not a tamper vector (no .expected.json, no src).
    const isSkipSummary = /^agentic\/fixtures\/.*\.SKIPPED\.md$/.test(path);
    const isStreamingReport = path === 'streaming-issues.md';
    const isRegression = path.startsWith('agentic/fixtures/regressions/');
    const wasRegression = oldPath && oldPath.startsWith('agentic/fixtures/regressions/');
    const isSrc = /^src\/.*\.rs$/.test(path) || path === 'Cargo.toml';
    if (task.type === 'port-fixtures') {
      if (isNew && (isFixture || isSkipSummary)) continue;
      violations.push(line);
    } else if (task.type === 'streaming-audit') {
      // audit touches ONLY streaming-issues.md
      if ((isNew || st.includes('M')) && isStreamingReport) continue;
      violations.push(line);
    } else { // fix-divergence, fix-streaming
      if (isNew && (isFixture || isSrc)) continue;
      if (st.includes('M') && isSrc) continue;
      if ((st.includes('D') || st.includes('R')) && isRegression) continue;
      // Allow promoting regression files to fixtures (rename from regressions/ to fixtures/)
      if (st.includes('R') && isFixture && wasRegression) continue;
      if (st.includes('M') && isFixture && isRegression) continue;
      if (st.includes('M') && isStreamingReport) continue;
      violations.push(line);
    }
  }
  if (violations.length > 0) {
    return {ok: false, tamper: true, detail: `disallowed changes:\n${violations.join('\n')}`};
  }
  return {ok: true, detail: porcelain};
}

// Gate B: regenerate every new/changed expected.json from its input via the TS
// oracle (neutralizes hand-written expectations).
function gateB(wt) {
  const porcelain = sh(['git', '-C', wt, 'status', '--porcelain']).trimEnd();
  const inputs = porcelain.split('\n')
    .map(l => l.slice(3).trim().split(' -> ').pop())
    .filter(p => /^agentic\/fixtures\/.*\.input\.json$/.test(p));
  for (const rel of inputs) {
    try {
      sh(['node', '--experimental-strip-types', join(wt, 'agentic/oracle/ts-runner.mjs'),
          join(wt, rel), '--out', join(wt, rel.replace('.input.json', '.expected.json'))],
         {timeout: 180_000});
    } catch (e) {
      return {ok: false, detail: `oracle rejected ${rel}:\n${(e.stderr ?? e.message ?? '').toString().slice(0, 2000)}`};
    }
  }
  return {ok: true, detail: `regenerated ${inputs.length} expected file(s)`};
}

// Gate C: full test suite in the worktree (includes fixture_replay).
function gateC(wt, workerIdx) {
  try {
    const out = sh(['cargo', 'test', '--', '--test-threads=1'], {
      cwd: wt, timeout: 30 * 60_000,
      env: {...process.env, CARGO_TARGET_DIR: join(RUST, `.shared-target-w${workerIdx}`)},
    });
    return {ok: true, detail: out.split('\n').filter(l => l.startsWith('test result')).join('\n')};
  } catch (e) {
    const out = `${e.stdout ?? ''}\n${e.stderr ?? ''}`;
    return {ok: false, detail: out.slice(-6000),
            divergence: /Rust-vs-TS divergence/.test(out)};
  }
}

// ---- review ---------------------------------------------------------------
function collectDiff(wt) {
  let diff = sh(['git', '-C', wt, 'diff']);
  const untracked = sh(['git', '-C', wt, 'status', '--porcelain']).trimEnd().split('\n')
    .filter(l => l.startsWith('??')).map(l => l.slice(3).trim());
  for (const p of untracked) {
    try {
      const content = readFileSync(join(wt, p), 'utf8');
      diff += `\n===== NEW FILE: ${p} =====\n${content.slice(0, 100_000)}\n`;
    } catch {}
  }
  return diff;
}

function parseVerdict(output) {
  const lines = output.trim().split('\n').map(l => l.trim()).filter(Boolean);
  for (let i = lines.length - 1; i >= Math.max(0, lines.length - 10); i--) {
    const l = lines[i].toUpperCase();
    if (/VERDICT:?\s*APPROVE/.test(l) || /APPROVE/.test(l) && i >= lines.length - 3) return 'APPROVE';
    if (/VERDICT:?\s*REJECT/.test(l) || /REJECT/.test(l) && i >= lines.length - 3) return 'REJECT';
  }
  // Also check for markdown-style verdict headers with approval language
  const fullOut = output.toUpperCase();
  if (/NO ISSUES FOUND|NO.*BLOCKING.*ISSUE/.test(fullOut) && !/REJECT/.test(fullOut)) return 'APPROVE';
  return 'UNPARSEABLE';
}

async function review(task, wt, attemptDir, n) {
  const diff = collectDiff(wt);
  const slot = takeSlot();
  try {
    log(`task ${task.id}: reviewer ${n} (key index ${slot % KEYS.keys.length})`);
    const res = await runXyne({
      prompt: `Review this diff for task "${task.id}". Task description: ${task.instructions}\n\nThe diff (+ new file contents) follows below:\n\n--- BEGIN DIFF ---\n${diff.slice(0, 200_000)}\n--- END DIFF ---`,
      systemPrompt: readFileSync(join(AG, 'prompts/reviewer.md'), 'utf8'),
      cwd: RUST,
      timeoutMs: REVIEW_TIMEOUT_MS,
      logFile: join(attemptDir, `review-${n}.log`),
      slot,
      tools: REVIEWER_TOOLS,
    });
    if (isTransient(res)) return {verdict: 'TRANSIENT', res};
    if (isRateLimited(res)) {
      log(`task ${task.id}: reviewer ${n} rate-limited — 30s wait, retry on next key`);
      await new Promise(r => setTimeout(r, 30_000));
      const res2 = await runXyne({
        prompt: `Review this diff for task "${task.id}". Task description: ${task.instructions}\n\nThe diff (+ new file contents) follows below:\n\n--- BEGIN DIFF ---\n${diff.slice(0, 200_000)}\n--- END DIFF ---`,
        systemPrompt: readFileSync(join(AG, 'prompts/reviewer.md'), 'utf8'),
        cwd: RUST, timeoutMs: REVIEW_TIMEOUT_MS,
        logFile: join(attemptDir, `review-${n}-retry.log`), slot: slot + 1,
        tools: REVIEWER_TOOLS,
      });
      return {verdict: parseVerdict(res2.output), res: res2};
    }
    return {verdict: parseVerdict(res.output), res};
  } finally { releaseSlot(slot); }
}

// ---- commit (serialized across workers) ------------------------------------
let commitChain = Promise.resolve();
function commitTask(task, wt) {
  const run = async () => {
    sh(['git', '-C', wt, 'add', '-A', 'agentic/fixtures']);
    if (task.type === 'fix-divergence' || task.type === 'fix-streaming') {
      try { sh(['git', '-C', wt, 'add', '-A', 'src', 'Cargo.toml']); } catch {}
    }
    if (task.type === 'streaming-audit' || task.type === 'fix-streaming') {
      try { sh(['git', '-C', wt, 'add', 'streaming-issues.md']); } catch {}
    }
    sh(['git', '-C', wt, 'commit', '--no-verify', '-m', `agentic(${task.id}): ${task.type}`]);
    const sha = sh(['git', '-C', wt, 'rev-parse', 'HEAD']).trim();
    try {
      sh(['git', '-C', RUST, 'merge', '--ff-only', sha]);
    } catch {
      sh(['git', '-C', RUST, 'cherry-pick', sha]);
    }
    return sha;
  };
  const p = commitChain.then(run, run);
  commitChain = p.catch(() => {});
  return p;
}

function needsHuman(task, reason, attemptDir) {
  const entry = `\n## ${new Date().toISOString()} — task ${task.id} (${task.type})\n- ${reason}\n- logs: ${attemptDir}\n`;
  appendFileSync(NEEDS_HUMAN, entry);
}

// ---- resource safety --------------------------------------------------------
function resourcesOk() {
  try {
    const df = sh(['df', '-k', ROOT]).split('\n')[1].split(/\s+/);
    const availGB = Number(df[3]) / (1024 * 1024);
    const load = os.loadavg()[0];
    if (availGB < 10) { log(`low disk (${availGB.toFixed(1)}GB) — pausing 10 min`); return false; }
    if (load > 10) { log(`high load (${load.toFixed(1)}) — pausing 10 min`); return false; }
    return true;
  } catch { return true; }
}

// ---- task lifecycle ---------------------------------------------------------
async function runTask(task, workerIdx) {
  const taskLogDir = join(LOGS, task.id);
  let attempt = (task.attempts ?? 0) + 1;
  let failureContext = '';

  while (attempt <= MAX_ATTEMPTS) {
    const attemptDir = join(taskLogDir, `attempt-${attempt}`);
    mkdirSync(attemptDir, {recursive: true});
    const wt = makeWorktree(task.id);
    try {
      // IMPLEMENT
      const slot = takeSlot();
      let impl;
      try {
        log(`task ${task.id}: attempt ${attempt} implement (worker ${workerIdx}, key index ${slot % KEYS.keys.length})`);
        impl = await runXyne({
          prompt: `${task.instructions}\n\nWorking directory (a git worktree of rust-ivm): ${wt}\nOracle: node --experimental-strip-types ${wt}/agentic/oracle/ts-runner.mjs <input.json>\nReplay: cargo run --bin replay -- <input.json>  (env CARGO_TARGET_DIR=${join(RUST, `.shared-target-w${workerIdx}`)})\nDiff: node ${wt}/agentic/oracle/diff.mjs <expected.json> <actual.json>\ncargo test ALWAYS with -- --test-threads=1\n${failureContext ? `\nPREVIOUS ATTEMPT FAILED. Gate/review output:\n${failureContext.slice(0, 4000)}\n` : ''}`,
          systemPrompt: readFileSync(join(AG, 'prompts/implementer.md'), 'utf8'),
          cwd: wt,
          timeoutMs: IMPLEMENT_TIMEOUT_MS,
          logFile: join(attemptDir, 'implementer.log'),
          slot,
          extraEnv: {CARGO_TARGET_DIR: join(RUST, `.shared-target-w${workerIdx}`)},
        });
      } finally { releaseSlot(slot); }

      if (isTransient(impl)) {
        log(`task ${task.id}: transient network failure — probing, will resume same attempt`);
        removeWorktree(wt);
        await probeUrlUntilUp();
        continue; // same attempt, does not count
      }
      if (/TASK-BLOCKED:/.test(impl.output)) {
        const reason = impl.output.match(/TASK-BLOCKED:[^\n]*/)[0];
        await updateTask(task.id, {state: 'failed', attempts: attempt});
        needsHuman(task, `implementer declared ${reason}`, attemptDir);
        return;
      }

      // GATES
      const a = gateA(wt, task);
      writeFileSync(join(attemptDir, 'gates.log'), `GATE A: ${a.ok ? 'PASS' : 'FAIL'}\n${a.detail}\n`);
      if (!a.ok && a.tamper) {
        await updateTask(task.id, {state: 'failed', attempts: attempt});
        needsHuman(task, `TAMPER: ${a.detail.split('\n')[0]}`, attemptDir);
        log(`task ${task.id}: TAMPER — straight to needs-human`);
        return;
      }
      let gateFail = a.ok ? null : `GATE A (allowed paths): ${a.detail}`;
      if (!gateFail) {
        const b = gateB(wt);
        appendFileSync(join(attemptDir, 'gates.log'), `GATE B: ${b.ok ? 'PASS' : 'FAIL'}\n${b.detail}\n`);
        if (!b.ok) gateFail = `GATE B (oracle regeneration): ${b.detail}`;
      }
      let divergence = false;
      if (!gateFail) {
        const c = gateC(wt, workerIdx);
        appendFileSync(join(attemptDir, 'gates.log'), `GATE C: ${c.ok ? 'PASS' : 'FAIL'}\n${c.detail}\n`);
        if (!c.ok) { gateFail = `GATE C (cargo test): ${c.detail}`; divergence = !!c.divergence; }
      }

      // REVIEW (both must approve)
      if (!gateFail) {
        const r1 = await review(task, wt, attemptDir, 1);
        const r2 = await review(task, wt, attemptDir, 2);
        if (r1.verdict === 'TRANSIENT' || r2.verdict === 'TRANSIENT') {
          removeWorktree(wt);
          await probeUrlUntilUp();
          continue;
        }
        if (r1.verdict !== 'APPROVE' || r2.verdict !== 'APPROVE') {
          gateFail = `REVIEW: reviewer1=${r1.verdict} reviewer2=${r2.verdict}\n` +
            `--- reviewer 1 tail ---\n${r1.res.output.slice(-1500)}\n--- reviewer 2 tail ---\n${r2.res.output.slice(-1500)}`;
        }
      }

      if (!gateFail) {
        const sha = await commitTask(task, wt);

        // Close the loop: if a port-fixtures task moved divergent fixtures into
        // regressions/, auto-queue a fix-divergence task for each (same pattern
        // as the fuzzer). This makes the loop self-healing.
        if (task.type === 'port-fixtures') {
          const diffNames = sh(['git', '-C', RUST, 'diff', '--name-only',
            '--diff-filter=A', `${sha}^`, sha, '--',
            'agentic/fixtures/regressions/']).trim();
          for (const f of diffNames.split('\n').filter(Boolean)) {
            const m = f.match(/^agentic\/fixtures\/regressions\/(.+)\.input\.json$/);
            if (!m) continue;
            const base = m[1];
            const added = await appendTask({
              id: `divergence-${base}`,
              type: 'fix-divergence',
              source: `agentic/fixtures/regressions/${base}.input.json`,
              instructions: `A port-fixtures task found a Rust-vs-TS divergence in case \`${base}\`. Repro: agentic/fixtures/regressions/${base}.input.json vs .expected.json (TS oracle output). Fix the Rust engine (src/**) to match TS behavior; cite the TS source lines that define the behavior. Never change the fixture or expected file. When fixed, move both files into agentic/fixtures/ so it becomes a permanent regression test.`,
            });
            if (added) log(`task ${task.id}: auto-queued fix-divergence for ${base}`);
          }
        }

        // Close the loop: if a streaming-audit found violations in
        // streaming-issues.md, auto-queue a fix-streaming task for each.
        if (task.type === 'streaming-audit') {
          const report = readFileSync(join(RUST, 'streaming-issues.md'), 'utf8');
          const matches = [...report.matchAll(/^## (.+?) — (VIOLATION)/gm)];
          for (const m of matches) {
            const opName = m[1].trim();
            const fileId = opName.toLowerCase().replace(/[^a-z0-9]+/g, '-');
            const added = await appendTask({
              id: `fix-streaming-${fileId}`,
              type: 'fix-streaming',
              source: `src/ivm/${opName}.rs`,
              instructions: `Streaming audit found that \`${opName}.rs\` uses collect-then-stream (from_vec/.collect) in its fetch() where the TS equivalent is a lazy generator. Rewrite fetch() to be truly lazy (node_stream with .filter/.map/.skip/.take chaining). The output must remain byte-identical — all existing fixture replay tests must still pass. See streaming-issues.md for the exact location. If materialization is genuinely required (e.g. sorting), mark MATERIALIZATION-REQUIRED with reason instead.`,
            });
            if (added) log(`task ${task.id}: auto-queued fix-streaming for ${opName}`);
          }
        }

        removeWorktree(wt);
        await updateTask(task.id, {state: 'done', attempts: attempt, commit: sha});
        log(`task ${task.id}: DONE (${sha.slice(0, 8)})`);
        return;
      }

      // failure path
      appendFileSync(join(attemptDir, 'gates.log'), `\nATTEMPT ${attempt} FAILED:\n${gateFail}\n`);
      removeWorktree(wt);
      failureContext = gateFail;
      attempt++;
      await updateTask(task.id, {attempts: attempt - 1});
      if (attempt > MAX_ATTEMPTS) {
        const finalState = divergence ? 'divergence-pending' : 'failed';
        await updateTask(task.id, {state: finalState});
        needsHuman(task, `${MAX_ATTEMPTS} attempts exhausted. Last failure: ${gateFail.slice(0, 300)}`, taskLogDir);
        log(`task ${task.id}: ${finalState} after ${MAX_ATTEMPTS} attempts`);
        return;
      }
    } catch (e) {
      removeWorktree(wt);
      log(`task ${task.id}: orchestrator error: ${e.message}`);
      await updateTask(task.id, {state: 'pending', attempts: attempt - 1});
      throw e;
    }
  }
}

// ---- main -------------------------------------------------------------------
const claimed = new Set();
async function workerLoop(workerIdx, once, taskId = null) {
  for (;;) {
    while (!resourcesOk()) await new Promise(r => setTimeout(r, 10 * 60_000));
    const task = await claimNextTask(`w${workerIdx}`, taskId);
    if (!task) { log(`worker ${workerIdx}: queue empty — exiting`); return; }
    claimed.add(task.id);
    try { await runTask(task, workerIdx); }
    catch (e) { log(`worker ${workerIdx}: task ${task.id} errored: ${e.message}`); }
    claimed.delete(task.id);
    if (once) return;
  }
}

async function main() {
  const args = process.argv.slice(2);
  let workers = 1, once = false, taskId = null;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--workers') workers = Math.min(2, Number(args[++i]));
    if (args[i] === '--once') once = true;
    if (args[i] === '--task') { taskId = args[++i]; once = true; }
  }
  process.on('SIGINT', async () => {
    log('SIGINT — releasing in_progress tasks back to pending');
    for (const id of claimed) await updateTask(id, {state: 'pending'});
    process.exit(130);
  });
  cleanStaleWorktrees();
  log(`orchestrator start: workers=${workers} once=${once} queue=${readQueue().tasks.filter(t => t.state === 'pending').length} pending`);
  await Promise.all(Array.from({length: workers}, (_, i) => workerLoop(i, once, taskId)));
  log('orchestrator exit');
}

main();
