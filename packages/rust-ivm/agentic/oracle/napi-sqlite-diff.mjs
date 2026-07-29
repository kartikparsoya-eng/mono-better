#!/usr/bin/env node
// oracle/napi-sqlite-diff.mjs — compare TS oracle output (CaughtNode tree)
// vs napi addon output (flat RowChanges) by flattening both to a common format.
//
// Usage: node oracle/napi-sqlite-diff.mjs <expected.json> <actual.json>
//   expected.json = TS oracle output ({hydrate, pushChanges, finalView})
//   actual.json   = napi addon output ({hydrate, finalView})
//
// Exit 0 if semantically equal, exit 1 with a diff on mismatch.
//
// The napi addon flattens the nested CaughtNode tree into individual RowChanges
// via the Streamer (one RowChange per row, recursing into relationships).
// To compare, we flatten the TS oracle's CaughtNode tree the same way:
//   - Each node → {changeType: ADD(0), table, rowKey, row}
//   - Recurse into relationships → child rows
//
// The table name for the root is derived from the AST's `table` field.
// Child table names are derived from the relationship's alias → subquery.table.
// This mirrors how the Streamer resolves table names from the schema.

import { readFileSync } from 'node:fs';

// Change types matching Rust ChangeType
const ADD = 0, REMOVE = 1, EDIT = 2, CHILD = 3;

// ---------------------------------------------------------------------------
// Flatten TS oracle CaughtNode tree → flat RowChange list
// ---------------------------------------------------------------------------

/**
 * Flatten a CaughtNode into RowChange objects.
 * @param {object} node - CaughtNode { row, relationships }
 * @param {string} table - the table name for this node
 * @param {string[]} pk - primary key columns for this table
 * @param {Map} relToTable - maps relationship name → { table, pk } for child tables
 * @param {number} changeType - ADD/REMOVE/EDIT
 * @param {object[]} out - accumulator
 */
function baseAlias(name) {
  return name.replace(/_\d+$/, '');
}

function flattenNode(node, table, pk, rels, changeType, out) {
  if (!node || !node.row) return;

  const rowKey = {};
  for (const col of pk) {
    rowKey[col] = node.row[col] ?? null;
  }

  out.push({
    changeType,
    queryId: 'q1',
    table,
    rowKey,
    row: changeType === REMOVE ? null : { ...node.row },
  });

  // Recurse into relationships, skipping hidden EXISTS aliases whose children
  // are client-invisible and source-order-dependent (see buildRelToTable).
  if (node.relationships) {
    for (const [relName, children] of Object.entries(node.relationships)) {
      if (rels.hiddenAliases.has(relName) || rels.hiddenAliases.has(baseAlias(relName))) continue;
      const childInfo = rels.relToTable.get(relName) || rels.relToTable.get(baseAlias(relName));
      if (!childInfo) continue;
      for (const child of children) {
        flattenNode(child, childInfo.table, childInfo.pk, rels, changeType, out);
      }
    }
  }
}

/**
 * Build a mapping from relationship name → { table, pk } from the fixture's AST.
 * The AST has `related` (array of correlated subqueries) and `where` (which may
 * contain correlatedSubquery conditions). Each has a `subquery` with `alias`
 * and `table`.
 */
function buildRelToTable(fixture) {
  const relToTable = new Map();
  const tables = fixture.tables || {};
  const ast = fixture.ast || {};

  // Aliases coming from `where` correlatedSubquery conditions are HIDDEN
  // EXISTS relationships. Their child rows are internal maintenance rows:
  //   1. explicitly UNORDERED by design (the Cap optimization — TS builder
  //      passes `undefined` orderBy so SQLite chooses order; see builder.ts
  //      "exists only needs the first row"), so which children survive the
  //      EXISTS limit is source-order-dependent (MemorySource PK order vs
  //      TableSource rowid order) — a legitimate, benign difference;
  //   2. discarded by the client (view-apply-change.ts isHidden), so never
  //      user-visible.
  // We therefore EXCLUDE hidden EXISTS children from the differential:
  //   - the oracle flatten does not recurse into hidden aliases, and
  //   - napi rows whose table is reachable ONLY via a hidden alias are dropped.
  const hiddenAliases = new Set();
  const hiddenTables = new Set();
  const visibleTables = new Set();

  // Collect visible tables (root + `related` join subqueries, recursively) and
  // hidden EXISTS tables/aliases (from `where`), walking nested subqueries.
  function walkAst(node, visible) {
    if (!node) return;
    if (visible && node.table) visibleTables.add(node.table);
    // related (visible joins)
    for (const rel of (node.related || [])) {
      const sub = rel.subquery || rel;
      const alias = sub.alias || rel.relationship_name;
      const table = sub.table;
      const pk = (tables[table] && tables[table].primaryKey) || ['id'];
      if (alias && table) relToTable.set(alias, { table, pk });
      walkAst(sub, true);
    }
    // where correlatedSubquery (hidden EXISTS)
    (function scanConditions(cond) {
      if (!cond) return;
      if (cond.type === 'correlatedSubquery') {
        const sub = cond.related && cond.related.subquery;
        if (sub && sub.alias && sub.table) {
          const pk = (tables[sub.table] && tables[sub.table].primaryKey) || ['id'];
          relToTable.set(sub.alias, { table: sub.table, pk });
          hiddenAliases.add(sub.alias);
          hiddenTables.add(sub.table);
          walkAst(sub, false); // hidden subtree is not client-visible
        }
      }
      if (cond.conditions) for (const c of cond.conditions) scanConditions(c);
    })(node.where);
  }
  walkAst(ast, true);

  // Tables reachable ONLY through a hidden EXISTS alias (never as root/related).
  const hiddenOnlyTables = new Set(
    [...hiddenTables].filter(t => !visibleTables.has(t)),
  );

  return { relToTable, hiddenAliases, hiddenOnlyTables };
}

/**
 * Convert TS oracle output to flat RowChange list for a given phase.
 */
function flattenOracle(output, fixture, phase, rels) {
  const ast = fixture.ast || {};
  const rootTable = ast.table;
  const rootPk = (fixture.tables && fixture.tables[rootTable] && fixture.tables[rootTable].primaryKey) || ['id'];
  const nodes = output[phase] || [];
  const out = [];
  for (const node of nodes) {
    flattenNode(node, rootTable, rootPk, rels, ADD, out);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Canonical comparison (same logic as diff.mjs)
// ---------------------------------------------------------------------------

const META_FIELDS = new Set(['queryId', 'isHidden']);

function stripMeta(rc) {
  const out = {};
  for (const k of Object.keys(rc)) {
    if (!META_FIELDS.has(k)) out[k] = rc[k];
  }
  return out;
}

function canon(v) {
  if (v === null) return null;
  if (typeof v === 'number') {
    if (Object.is(v, -0)) return 0;
    if (Number.isFinite(v) && Math.round(v) === v) return Math.round(v);
    return v;
  }
  if (typeof v === 'boolean' || typeof v === 'string') return v;
  if (Array.isArray(v)) return v.map(canon);
  if (typeof v === 'object') {
    const out = {};
    for (const k of Object.keys(v).sort()) out[k] = canon(v[k]);
    return out;
  }
  return v;
}

function deepEqual(a, b) {
  if (a === b) return true;
  if (typeof a !== typeof b) return false;
  if (a === null || b === null) return a === b;
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    return a.every((x, i) => deepEqual(x, b[i]));
  }
  if (typeof a === 'object' && typeof b === 'object') {
    const ka = Object.keys(a), kb = Object.keys(b);
    if (ka.length !== kb.length) return false;
    return ka.every(k => deepEqual(a[k], b[k]));
  }
  return false;
}

function diffPath(a, b, path) {
  if (deepEqual(a, b)) return null;
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) {
      return { path: `${path}.length`, a: a.length, b: b.length };
    }
    for (let i = 0; i < a.length; i++) {
      const d = diffPath(a[i], b[i], `${path}[${i}]`);
      if (d) return d;
    }
    return null;
  }
  if (a && b && typeof a === 'object' && typeof b === 'object') {
    for (const k of [...new Set([...Object.keys(a), ...Object.keys(b)])].sort()) {
      if (!(k in a)) return { path: `${path}.${k}`, a: undefined, b: b[k] };
      if (!(k in b)) return { path: `${path}.${k}`, a: a[k], b: undefined };
      const d = diffPath(a[k], b[k], `${path}.${k}`);
      if (d) return d;
    }
    return null;
  }
  return { path: path || '<root>', a, b };
}

// ---------------------------------------------------------------------------
// Comparison key for a RowChange — used for set-based comparison.
// The napi addon may emit rows in a different order than the TS oracle
// (SQLite query order vs memory-source order). We compare as sorted sets.
// ---------------------------------------------------------------------------

function rowChangeKey(rc) {
  const rowKeyStr = JSON.stringify(canon(rc.rowKey));
  const rowStr = rc.row ? JSON.stringify(canon(rc.row)) : 'null';
  const ct = rc.changeType ?? 0;
  return `${ct}|${rc.table}|${rowKeyStr}|${rowStr}`;
}

function rowChangeCmp(a, b) {
  const ka = rowChangeKey(a), kb = rowChangeKey(b);
  return ka < kb ? -1 : ka > kb ? 1 : 0;
}

/**
 * Compare two RowChange lists as sorted sets (order-independent).
 * Returns null if equal, or { path, expected, actual } on mismatch.
 */
function compareRowChangeSets(expected, actual) {
  const expSorted = [...expected].sort(rowChangeCmp);
  const actSorted = [...actual].sort(rowChangeCmp);

  if (expSorted.length !== actSorted.length) {
    // Find the first mismatch for a readable diff
    const expSet = new Set(expSorted.map(rowChangeKey));
    const actSet = new Set(actSorted.map(rowChangeKey));
    const missing = expSorted.filter(r => !actSet.has(rowChangeKey(r)));
    const extra = actSorted.filter(r => !expSet.has(rowChangeKey(r)));
    return {
      path: `length (expected=${expSorted.length} actual=${actSorted.length})`,
      missing: missing.slice(0, 5).map(r => JSON.stringify({ table: r.table, rowKey: r.rowKey, row: r.row })),
      extra: extra.slice(0, 5).map(r => JSON.stringify({ table: r.table, rowKey: r.rowKey, row: r.row })),
    };
  }

  for (let i = 0; i < expSorted.length; i++) {
    if (!deepEqual(canon(stripMeta(expSorted[i])), canon(stripMeta(actSorted[i])))) {
      const d = diffPath(canon(stripMeta(expSorted[i])), canon(stripMeta(actSorted[i])), `row[${i}]`);
      return d || { path: `row[${i}]`, expected: expSorted[i], actual: actSorted[i] };
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

function main() {
  const [expectedPath, actualPath] = process.argv.slice(2);
  if (!expectedPath || !actualPath) {
    console.error('Usage: napi-sqlite-diff.mjs <expected.json> <actual.json>');
    console.error('  expected.json = TS oracle output ({hydrate, pushChanges, finalView})');
    console.error('  actual.json   = napi addon output ({hydrate, finalView})');
    process.exit(2);
  }

  // We need the fixture to build the relToTable mapping.
  // The expected.json file was produced from a fixture; we need the fixture's
  // AST + table schema. Look for a sibling .input.json file.
  const fixturePath = expectedPath.replace(/\.expected\.json$/, '.input.json');
  let fixture = null;
  try {
    fixture = JSON.parse(readFileSync(fixturePath, 'utf8'));
  } catch {
    // If we can't find the fixture, do a simpler comparison without table mapping
    fixture = null;
  }

  const expected = JSON.parse(readFileSync(expectedPath, 'utf8'));
  const actual = JSON.parse(readFileSync(actualPath, 'utf8'));

  // Flatten TS oracle output. Hidden EXISTS-subquery children are excluded on
  // both sides — they are client-invisible and source-order-dependent (see
  // buildRelToTable), so comparing MemorySource-order vs TableSource-order there
  // would be a false positive.
  const rels = fixture ? buildRelToTable(fixture) : null;
  const expHydrate = rels
    ? flattenOracle(expected, fixture, 'hydrate', rels)
    : (expected.hydrate || []).map(n => ({ changeType: ADD, table: '?', rowKey: {}, row: n.row }));
  const expFinal = rels
    ? flattenOracle(expected, fixture, 'finalView', rels)
    : (expected.finalView || []).map(n => ({ changeType: ADD, table: '?', rowKey: {}, row: n.row }));

  // Scalar-subquery companion rows: pipeline-driver (and the addon) emit the
  // matched subquery row as a client-visible ADD alongside hydration. The
  // oracle reports them in `companionRows` ({table, row}); flatten to the same
  // RowChange shape and fold into both phases (scalar fixtures carry no pushes,
  // so hydrate == finalView).
  const tables = (fixture && fixture.tables) || {};
  const companionChanges = (expected.companionRows || []).map(({ table, row }) => {
    const pk = (tables[table] && tables[table].primaryKey) || ['id'];
    const rowKey = {};
    for (const col of pk) rowKey[col] = row[col] ?? null;
    return { changeType: ADD, queryId: 'q1', table, rowKey, row: { ...row } };
  });
  expHydrate.push(...companionChanges);
  expFinal.push(...companionChanges);

  // Napi addon output is already flat. Drop rows tagged is_hidden by the
  // streamer (children of a hidden EXISTS relationship) — mirrors the
  // oracle-side exclusion above. This is precise even when a table is BOTH a
  // visible related join AND a hidden EXISTS target (the flag distinguishes the
  // two copies, which a table-name heuristic cannot).
  // Filter out hidden rows, then strip the `isHidden` tag itself (it is harness
  // metadata, not row content — the oracle side has no such field).
  const dropHidden = (rows) =>
    rows.filter(r => r.isHidden !== true).map(({ isHidden, ...rest }) => rest);
  const actHydrate = dropHidden(actual.hydrate || []);
  const actFinal = dropHidden(actual.finalView || []);

  // Compare hydrate
  const hydrateDiff = compareRowChangeSets(expHydrate, actHydrate);
  if (hydrateDiff) {
    console.error(`HYDRATE DIFF: ${hydrateDiff.path}`);
    if (hydrateDiff.missing) {
      console.error(`  missing from napi (in TS but not addon): ${hydrateDiff.missing.join(', ')}`);
      console.error(`  extra in napi (in addon but not TS): ${hydrateDiff.extra.join(', ')}`);
    } else {
      console.error(`  expected: ${JSON.stringify(hydrateDiff.expected || hydrateDiff.a)}`);
      console.error(`  actual:   ${JSON.stringify(hydrateDiff.actual || hydrateDiff.b)}`);
    }
    process.exit(1);
  }

  // Compare finalView (only if napi produced it)
  if (actFinal.length > 0 || expFinal.length > 0) {
    const finalDiff = compareRowChangeSets(expFinal, actFinal);
    if (finalDiff) {
      console.error(`FINAL VIEW DIFF: ${finalDiff.path}`);
      if (finalDiff.missing) {
        console.error(`  missing from napi: ${finalDiff.missing.join(', ')}`);
        console.error(`  extra in napi: ${finalDiff.extra.join(', ')}`);
      } else {
        console.error(`  expected: ${JSON.stringify(finalDiff.expected || finalDiff.a)}`);
        console.error(`  actual:   ${JSON.stringify(finalDiff.actual || finalDiff.b)}`);
      }
      process.exit(1);
    }
  }

  console.log('EQUAL');
  process.exit(0);
}

// Use top-level await for the dynamic import
await main();
