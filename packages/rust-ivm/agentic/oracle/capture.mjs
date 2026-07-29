#!/usr/bin/env node
// capture.mjs — extract .input.json fixtures from TS *.test.ts files by
// executing them with vitest stubbed and runPushTest/runFetchTest wrapped to
// serialize their arguments (see capture-loader.mjs).
//
// Usage:
//   node --experimental-strip-types \
//     agentic/oracle/capture.mjs <testFile.ts> [<testFile.ts> ...] [--out <dir>]
//
// Writes <name>.input.json into --out (default: agentic/fixtures/_captured).
// Does NOT generate expected output — run ts-runner.mjs on each input next.

import { register } from 'node:module';
import { pathToFileURL } from 'node:url';
import { dirname, basename, resolve as pathResolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));

const argv = process.argv.slice(2);
let outDir = pathResolve(__dirname, '..', 'fixtures', '_captured');
const files = [];
for (let i = 0; i < argv.length; i++) {
  if (argv[i] === '--out') { outDir = pathResolve(argv[++i]); continue; }
  files.push(argv[i]);
}
if (files.length === 0) {
  console.error('Usage: capture.mjs <testFile.ts> [...] [--out <dir>]');
  process.exit(1);
}

globalThis.__CAP__ = { stack: [], counter: {}, written: [], outDir, prefix: '' };

register('./capture-loader.mjs', import.meta.url);

const summary = [];
for (const f of files) {
  const abs = pathResolve(f);
  const prefix = basename(abs).replace(/\.test\.ts$/, '').replace(/\./g, '-');
  globalThis.__CAP__.prefix = prefix;
  const before = globalThis.__CAP__.written.length;
  try {
    await import(pathToFileURL(abs).href);
  } catch (e) {
    console.error(`ERROR importing ${f}: ${e && e.stack ? e.stack : e}`);
  }
  const n = globalThis.__CAP__.written.length - before;
  summary.push(`${prefix}: ${n} fixtures`);
}

console.log(`\nWrote ${globalThis.__CAP__.written.length} fixtures to ${outDir}`);
for (const s of summary) console.log('  ' + s);
