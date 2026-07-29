#!/usr/bin/env node
// oracle/ts-advance-runner.mjs — TS advance-path oracle.
//
// Counterpart to napi-advance-runner.mjs. Runs the fixture through TS
// MemorySource + pipeline + Catch, applies all pushes, and emits the same
// flat RowChange shape the addon emits:
//   { hydrate: RowChange[], advance: RowChange[], finalView: RowChange[] }
//
// Usage: node --experimental-strip-types agentic/oracle/ts-advance-runner.mjs \
//              <input.json> [--out <expected.json>]

import {readFileSync, writeFileSync, mkdirSync} from 'node:fs';
import {dirname, join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const MONO = resolve(__dirname, '..', '..', '..', 'mono-v1.7');
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
    console.error('Usage: ts-advance-runner.mjs <input.json> [--out <expected.json>]');
    process.exit(1);
  }
  return {input, out};
}

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

// Change types matching Rust ChangeType
const ADD = 0, REMOVE = 1, EDIT = 2;

function typeToCt(type) {
  switch (type) {
    case 'add': return ADD;
    case 'remove': return REMOVE;
    case 'edit': return EDIT;
    default: throw new Error(`Unknown change type: ${type}`);
  }
}

function rowKey(row, pk) {
  const out = {};
  for (const col of pk) out[col] = row[col] ?? null;
  return out;
}

// ---------------------------------------------------------------------------
// Hidden EXISTS-alias detection (mirrors napi-sqlite-diff.mjs)
// ---------------------------------------------------------------------------

function buildRelInfo(fixture) {
  const tables = fixture.tables || {};
  const ast = fixture.ast || {};
  const relToTable = new Map();
  const hiddenAliases = new Set();
  const hiddenTables = new Set();
  const visibleTables = new Set();

  function walkAst(node, visible) {
    if (!node) return;
    if (visible && node.table) visibleTables.add(node.table);
    for (const rel of (node.related || [])) {
      const sub = rel.subquery || rel;
      const alias = sub.alias || rel.relationship_name;
      const table = sub.table;
      const pk = (tables[table] && tables[table].primaryKey) || ['id'];
      if (alias && table) relToTable.set(alias, {table, pk});
      walkAst(sub, true);
    }
    (function scanConditions(cond) {
      if (!cond) return;
      if (cond.type === 'correlatedSubquery') {
        const sub = cond.related && cond.related.subquery;
        if (sub && sub.alias && sub.table) {
          const pk = (tables[sub.table] && tables[sub.table].primaryKey) || ['id'];
          relToTable.set(sub.alias, {table: sub.table, pk});
          hiddenAliases.add(sub.alias);
          hiddenTables.add(sub.table);
          walkAst(sub, false);
        }
      }
      if (cond.conditions) for (const c of cond.conditions) scanConditions(c);
    })(node.where);
  }
  walkAst(ast, true);

  for (let i = 0; i < 10; i++) {
    for (const [alias, info] of relToTable) {
      relToTable.set(`${alias}_${i}`, info);
    }
    for (const a of hiddenAliases) hiddenAliases.add(`${a}_${i}`);
  }

  const hiddenOnlyTables = new Set([...hiddenTables].filter(t => !visibleTables.has(t)));
  return {relToTable, hiddenAliases, hiddenOnlyTables};
}

// ---------------------------------------------------------------------------
// Flatten CaughtNode / CaughtChange trees → RowChange list
// ---------------------------------------------------------------------------

function flattenNode(node, table, pk, rels, changeType, out) {
  if (!node || !node.row) return;
  out.push({
    changeType,
    queryId: 'q1',
    table,
    rowKey: rowKey(node.row, pk),
    row: changeType === REMOVE ? null : {...node.row},
  });
  if (node.relationships) {
    for (const [relName, children] of Object.entries(node.relationships)) {
      if (rels.hiddenAliases.has(relName)) continue;
      const childInfo = rels.relToTable.get(relName);
      if (!childInfo) continue;
      for (const child of children) {
        flattenNode(child, childInfo.table, childInfo.pk, rels, changeType, out);
      }
    }
  }
}

function flattenCaughtChange(cc, rootTable, rootPk, rels, out) {
  const ct = typeToCt(cc.type);
  if (ct === EDIT) {
    // Edit for the root only; children are not represented as a single node.
    // Push the edit row change directly.
    out.push({
      changeType: EDIT,
      queryId: 'q1',
      table: rootTable,
      rowKey: rowKey(cc.row, rootPk),
      row: {...cc.row},
    });
    return;
  }
  const node = cc.node;
  flattenNode(node, rootTable, rootPk, rels, ct, out);
}

function flattenNodes(nodes, table, pk, rels) {
  const out = [];
  for (const node of nodes) flattenNode(node, table, pk, rels, ADD, out);
  return out;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

function canonicalize(value) {
  return JSON.stringify(value, (key, val) => canonicalReplacer(val), 1) + '\n';
}

function canonicalReplacer(val) {
  if (val && typeof val === 'object' && !Array.isArray(val)) {
    const sorted = {};
    for (const k of Object.keys(val).sort()) sorted[k] = canonicalReplacer(val[k]);
    return sorted;
  }
  return val;
}

function canonical(v) {
  if (v && typeof v === 'object' && !Array.isArray(v)) {
    const sorted = {};
    for (const k of Object.keys(v).sort()) sorted[k] = canonical(v[k]);
    return sorted;
  }
  return v;
}

function rcKey(rc) {
  return `${rc.table}|${JSON.stringify(canonical(rc.rowKey))}`;
}

function rowsEqual(a, b) {
  return JSON.stringify(canonical(a)) === JSON.stringify(canonical(b));
}

/// Compute the production-style advance diff between two flat RowChange sets.
/// This mirrors what the snapshotter diff → IVM advance emits: one change per
/// row key summarizing the net change between the before-state (hydrate) and
/// after-state (finalView), not per-push deltas.
function diffRowChangeSets(before, after) {
  const beforeMap = new Map();
  for (const rc of before) beforeMap.set(rcKey(rc), rc);
  const afterMap = new Map();
  for (const rc of after) afterMap.set(rcKey(rc), rc);

  const advance = [];
  for (const [key, afterRc] of afterMap) {
    const beforeRc = beforeMap.get(key);
    if (!beforeRc) {
      advance.push({changeType: ADD, queryId: 'q1', table: afterRc.table, rowKey: afterRc.rowKey, row: afterRc.row});
    } else if (!rowsEqual(beforeRc.row, afterRc.row)) {
      advance.push({changeType: EDIT, queryId: 'q1', table: afterRc.table, rowKey: afterRc.rowKey, row: afterRc.row});
    }
  }
  for (const [key, beforeRc] of beforeMap) {
    if (!afterMap.has(key)) {
      advance.push({changeType: REMOVE, queryId: 'q1', table: beforeRc.table, rowKey: beforeRc.rowKey, row: null});
    }
  }
  return advance;
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

  const tableSpecs = new Map(
    Object.entries(fixture.tables).map(([name, spec]) => [
      name,
      {tableSpec: {uniqueKeys: [spec.primaryKey]}},
    ]),
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

  const rels = buildRelInfo(fixture);
  const rootTable = resolvedAst.table;
  const rootPk = (fixture.tables[rootTable] && fixture.tables[rootTable].primaryKey) || ['id'];

  let hydrate = flattenNodes(sink.fetch(), rootTable, rootPk, rels);

  // Scalar-subquery companions are emitted at hydrate time, so fold them into
  // hydrate/finalView like the napi side does.
  const tables = fixture.tables || {};
  const companionChanges = companionRows.map(({table, row}) => {
    const pk = (tables[table] && tables[table].primaryKey) || ['id'];
    return {changeType: ADD, queryId: 'q1', table, rowKey: rowKey(row, pk), row: {...row}};
  });
  hydrate = hydrate.concat(companionChanges);

  // Apply all pushes to the in-memory sources to reach the after-state.
  for (const push of (fixture.pushes || [])) {
    const src = delegate.getSource(push.table);
    if (!src) throw new Error(`Unknown source for push: ${push.table}`);
    deps.consume(src.push(toSourceChange(deps, push)));
  }

  let finalView = flattenNodes(sink.fetch(), rootTable, rootPk, rels);
  finalView = finalView.concat(companionChanges);

  // Advance output = net diff between hydrate and finalView, matching the
  // production snapshotter-diff semantics (not per-push deltas).
  const advance = diffRowChangeSets(hydrate, finalView);

  const result = {hydrate, advance, finalView};
  const outPath = out ?? input.replace(/\.input\.json$/, '.expected.json');
  mkdirSync(dirname(outPath), {recursive: true});
  writeFileSync(outPath, canonicalize(result));
  console.log(`wrote ${outPath} (hydrate=${hydrate.length} advance=${advance.length} finalView=${finalView.length})`);
}

main();
