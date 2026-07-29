#!/usr/bin/env node
// agentic/fuzz/watch-fuzzers.mjs
// Monitors the four background fuzzers by tailing their logs. When any one
// reports it hit --max-findings ("maxFindings reached" or "fuzz done"), stop
// the other three, run triage, and write trigger files so the next assistant
// turn can start root-cause fixing immediately.

import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { setInterval, clearInterval } from 'node:timers';

const FUZZERS = [
  { name: 'fuzz-serial', script: 'agentic/fuzz/fuzz-loop.mjs' },
  { name: 'fuzz-napi', script: 'agentic/fuzz/fuzz-napi-loop.mjs' },
  { name: 'fuzz-adv', script: 'agentic/fuzz/fuzz-adv-loop.mjs' },
  { name: 'fuzz-par-equiv', script: 'agentic/fuzz/fuzz-parallel-equiv-loop.mjs' },
];

const LOG_DIR = '/Users/kartik.parsoya/.xyne/agent/background/global';
const TRIGGER = 'agentic/fuzz/.fix-triggered';
const PROMPT = 'agentic/fuzz/.fix-prompt.md';
const POLL_MS = 5 * 60 * 1000; // 5 minutes

function log(msg) {
  const line = `${new Date().toISOString()} ${msg}`;
  console.log(line);
}

function readLogTail(name, lines = 12) {
  try {
    const log = readFileSync(`${LOG_DIR}/${name}.log`, 'utf8');
    return log.split('\n').filter(Boolean).slice(-lines);
  } catch {
    return [];
  }
}

function hasCompleted(tail) {
  return tail.some(line =>
    line.includes('maxFindings reached') ||
    line.includes('fuzz done')
  );
}

function getPids(script) {
  try {
    const out = execFileSync('pgrep', ['-f', `${script} --minutes`], { encoding: 'utf8' });
    return out.trim().split('\n').filter(Boolean).map(Number);
  } catch {
    return [];
  }
}

function killFuzzer(script) {
  for (const pid of getPids(script)) {
    try {
      process.kill(pid, 'SIGTERM');
      log(`sent SIGTERM to ${script} pid ${pid}`);
    } catch (e) {
      log(`failed to kill ${script} pid ${pid}: ${e.message}`);
    }
  }
}

function killOthers(exceptScript) {
  for (const { script } of FUZZERS) {
    if (script === exceptScript) continue;
    killFuzzer(script);
  }
}

function runTriage() {
  try {
    return execFileSync('node', ['agentic/triage-regressions.mjs'], {
      cwd: '/Users/kartik.parsoya/Documents/Go-RS/rust-ivm',
      encoding: 'utf8',
      timeout: 120_000,
    });
  } catch (e) {
    return `triage failed: ${e.message}\n${e.stdout || ''}\n${e.stderr || ''}`;
  }
}

function writePrompt(triggerName, tail, triageOutput) {
  const content = `# Auto-generated fix prompt

A fuzzer hit --max-findings and the others were stopped.

- Trigger: ${triggerName}
- Stopped at: ${new Date().toISOString()}

## Tail of triggering log

\`\`\`
${tail.join('\n')}
\`\`\`

## Triage output

\`\`\`
${triageOutput}
\`\`\`

## Next steps

1. Inspect \`agentic/fixtures/regressions/\` for new divergences.
2. Root-cause the smallest divergence.
3. Fix it in Rust sources only (do not edit TS oracle / .expected.json).
4. Verify with \`cargo test -- --test-threads=1\`, fixture replay, and triage.
5. Promote fixed fixtures and commit.
`;
  writeFileSync(PROMPT, content);
}

function main() {
  if (existsSync(TRIGGER)) {
    log(`trigger file ${TRIGGER} already exists; refusing to start a second watcher`);
    process.exit(1);
  }

  log('watching fuzzers every 5min: ' + FUZZERS.map(f => f.name).join(', '));

  const timer = setInterval(() => {
    log('polling fuzzer logs...');
    for (const { name, script } of FUZZERS) {
      const tail = readLogTail(name);
      if (hasCompleted(tail)) {
        log(`${name} completed (hit max-findings)`);
        killOthers(script);

        log('running triage...');
        const triageOutput = runTriage();

        writeFileSync(TRIGGER, JSON.stringify({
          stoppedAt: new Date().toISOString(),
          trigger: name,
          logTail: tail,
        }, null, 2) + '\n');

        writePrompt(name, tail, triageOutput);

        log(`wrote ${TRIGGER} and ${PROMPT}; stopping watcher`);
        clearInterval(timer);
        process.exit(0);
      }
    }
  }, POLL_MS);
}

main();
