#!/usr/bin/env node
// oracle/ts-runner.mjs — runs a fixture through the TS engine (the oracle).
//
// Usage: node --experimental-strip-types agentic/oracle/ts-runner.mjs <input.json> [--out <expected.json>]
//
// Reads an inputs-only fixture (.input.json), builds MemorySources + pipeline
// via the SAME builder path the TS test suite uses (buildPipeline +
// TestBuilderDelegate), hydrates into a Catch, applies pushes one at a time,
// and emits <name>.expected.json:
//   { "hydrate": <CaughtNode[]>, "pushChanges": <CaughtChange[][]>, "finalView": <CaughtNode[]> }
//
// Expected outputs are produced ONLY here — never hand-written. The Rust engine
// replays the same fixture and must match byte-for-byte (after canonicalization).

import {readFileSync, writeFileSync, mkdirSync, existsSync} from 'node:fs';
import {dirname, join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
// mono root: pwd-based fallback, then walk up for a dir with packages/zql
function findMono(fromDir) {
  let dir = fromDir;
  for (let i = 0; i < 6; i++) {
    if (existsSync(`${dir}/packages/zql/src`)) return dir;
    dir = dirname(dir);
  }
  return resolve(fromDir, '..', '..', '..', 'mono-v1.7');
}
const MONO = findMono(__dirname);
const ZQL = `${MONO}/packages/zql/src`;
const ZQLITE = `${MONO}/packages/zqlite/src`;
const SHARED = `${MONO}/packages/shared/src`;
const OTEL = `${MONO}/packages/otel/src`;

let lc;
async function loadDeps() {
  const [{buildPipeline}, {TestBuilderDelegate}, {MemorySource}, {Catch}, srcMod, {consume}, {skipYields}, {resolveSimpleScalarSubqueries}, {createSilentLogContext}, {testLogConfig}] =
    await Promise.all([
      import(`${ZQL}/builder/builder.ts`),
      import(`${ZQL}/builder/test-builder-delegate.ts`),
      import(`${ZQL}/ivm/memory-source.ts`),
      import(`${ZQL}/ivm/catch.ts`),
      import(`${ZQL}/ivm/source.ts`),
      import(`${ZQL}/ivm/stream.ts`),
      import(`${ZQL}/ivm/skip-yields.ts`),
      import(`${ZQLITE}/resolve-scalar-subqueries.ts`),
      import(`${SHARED}/logging-test-utils.ts`),
      import(`${OTEL}/test-log-config.ts`),
    ]);
  return {buildPipeline, TestBuilderDelegate, MemorySource, Catch,
    makeSourceChangeAdd: srcMod.makeSourceChangeAdd,
    makeSourceChangeEdit: srcMod.makeSourceChangeEdit,
    makeSourceChangeRemove: srcMod.makeSourceChangeRemove,
    consume, skipYields, resolveSimpleScalarSubqueries, testLogConfig, createSilentLogContext};
}

function parseArgs(argv) {
  const args = argv.slice(2);
  let input = null;
  let out = null;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--out') { out = args[++i]; continue; }
    if (!input) input = args[i];
  }
  if (!input) {
    console.error('Usage: ts-runner.mjs <input.json> [--out <expected.json>]');
    process.exit(1);
  }
  return {input, out};
}

// Convert fixture column type string → SchemaValue object expected by MemorySource.
function toSchemaColumns(cols) {
  const out = {};
  for (const [name, type] of Object.entries(cols)) {
    out[name] = typeof type === 'string' ? {type} : type;
  }
  return out;
}

function makeSource(deps, table, spec) {
  const source = new deps.MemorySource(
    table,
    toSchemaColumns(spec.columns),
    spec.primaryKey,
  );
  for (const row of (spec.rows ?? [])) {
    deps.consume(source.push(deps.makeSourceChangeAdd(row)));
  }
  return source;
}

function toSourceChange(deps, push) {
  switch (push.type) {
    case 'add': return deps.makeSourceChangeAdd(push.row);
    case 'remove': return deps.makeSourceChangeRemove(push.row);
    case 'edit': return deps.makeSourceChangeEdit(push.row, push.oldRow);
    default: throw new Error(`Unknown push type: ${push.type}`);
  }
}

// Canonicalize JSON: sorted keys, stable number formatting, no trailing whitespace.
function canonicalize(value) {
  // JSON.stringify with a replacer that sorts object keys.
  return JSON.stringify(value, (key, val) => {
    if (val && typeof val === 'object' && !Array.isArray(val)) {
      const sorted = {};
      for (const k of Object.keys(val).sort()) sorted[k] = val[k];
      return sorted;
    }
    return val;
  }, 0);
}

async function main() {
  const {input, out} = parseArgs(process.argv);
  const fixture = JSON.parse(readFileSync(input, 'utf8'));

  const deps = await loadDeps();
  lc = deps.createSilentLogContext();

  const sources = {};
  for (const [name, spec] of Object.entries(fixture.tables)) {
    sources[name] = makeSource(deps, name, spec);
  }

  const delegate = new deps.TestBuilderDelegate(sources, false, fixture.enableNotExists ?? false);

  // Resolve scalar subqueries the same way pipeline-driver does: build+fetch
  // the (at-most-one-row) subquery, bake the value as a literal, and emit the
  // matched subquery row as a companion. Mirrors #resolveScalarSubqueries so
  // the oracle matches the addon (which resolves scalars in-engine). Fixtures
  // with scalar subqueries carry no pushes, so this runs once for hydrate ==
  // finalView.
  const tableSpecs = new Map(
    Object.entries(fixture.tables).map(([name, spec]) => {
      // Match the engine's uniqueKeys (client PK + replica PK when they diverge)
      // so scalar-EXISTS resolution keys identically on both sides.
      const uniqueKeys = [spec.primaryKey];
      const replicaPK = spec.replicaPrimaryKey ?? spec.primaryKey;
      if (JSON.stringify(replicaPK) !== JSON.stringify(spec.primaryKey)) {
        uniqueKeys.push(replicaPK);
      }
      return [name, {tableSpec: {uniqueKeys}}];
    }),
  );
  const companionRows = [];
  const executor = (subqueryAST, childField) => {
    const input = deps.buildPipeline(subqueryAST, delegate, 'scalar-subquery');
    let node;
    for (const n of deps.skipYields(input.fetch({}))) node ??= n;
    if (!node) return undefined;
    companionRows.push({table: subqueryAST.table, row: {...node.row}});
    return node.row[childField] ?? null;
  };
  const {ast: resolvedAst} = deps.resolveSimpleScalarSubqueries(
    fixture.ast,
    tableSpecs,
    executor,
  );

  const pipeline = deps.buildPipeline(resolvedAst, delegate, 'query-id');

  const sink = new deps.Catch(pipeline);

  // Hydrate: materialized view after initial source load, before pushes.
  const hydrate = sink.fetch();

  // Apply pushes one at a time; capture the CaughtChange[] delta per push.
  const pushChanges = [];
  for (const push of (fixture.pushes ?? [])) {
    const src = delegate.getSource(push.table);
    if (!src) throw new Error(`Unknown source for push: ${push.table}`);
    const before = sink.pushes.length;
    deps.consume(src.push(toSourceChange(deps, push)));
    pushChanges.push(sink.pushes.slice(before));
  }

  // Final view: re-fetch the full pipeline state after all pushes.
  const finalView = sink.fetch();

  const result = {hydrate, pushChanges, finalView, companionRows};
  const canonical = canonicalize(result) + '\n';

  const outPath = out ?? input.replace(/\.input\.json$/, '.expected.json');
  mkdirSync(dirname(outPath), {recursive: true});
  writeFileSync(outPath, canonical);
  console.log(`wrote ${outPath}`);
}

main();
