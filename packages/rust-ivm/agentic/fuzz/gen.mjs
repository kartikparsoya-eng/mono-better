#!/usr/bin/env node
// fuzz/gen.mjs — seeded random fixture generator (reproducible).
// Usage: node gen.mjs --seed N [--out path.input.json]
// Emits an inputs-only fixture from the supported grammar: 1-3 tables, 5-50
// rows, where (and/or/simple: = != < <= > >= LIKE ILIKE IN IS/IS NOT),
// orderBy multi-column asc/desc, limit, start (inclusive/exclusive), one level
// of related, EXISTS / NOT EXISTS, 0-20 pushes (add/edit/remove; edits that
// cross limit boundaries; boundary-tie values). Expected outputs come ONLY
// from oracle/ts-runner.mjs — never from here.

import {writeFileSync} from 'node:fs';

function mulberry32(seed) {
  let a = seed >>> 0;
  return function () {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function makeRng(seed) {
  const rand = mulberry32(seed);
  return {
    rand,
    int: (lo, hi) => lo + Math.floor(rand() * (hi - lo + 1)), // inclusive
    pick: arr => arr[Math.floor(rand() * arr.length)],
    chance: p => rand() < p,
    shuffle: arr => {
      const a = arr.slice();
      for (let i = a.length - 1; i > 0; i--) {
        const j = Math.floor(rand() * (i + 1));
        [a[i], a[j]] = [a[j], a[i]];
      }
      return a;
    },
  };
}

// Nasty value pools. -0 is intentionally absent: JSON.stringify(-0) === "0",
// so it cannot survive the fixture file round-trip (canonicalization equates
// them anyway).
const STRINGS = ['', 'a', 'A', 'b', 'abc', 'ZZZ', 'héllo', '🦀rust', '%', '_', '0', 'null', 'a b', 'ﬀ', 'true', 'false', 'null', '{}', '[]', 'x'.repeat(1000), '🎉🎊', 'café', '日本語', '\t\n', '"quoted"', "'apos'"];
const NUMBERS = [0, 1, -1, 2, 10, -10, 2.5, -3.75, 0.1, 1e6, -1e6, 123456789, 0.30000000000000004, Number.MAX_SAFE_INTEGER, Number.MIN_SAFE_INTEGER, -0, 3.14159265358979, 1e-10, 1e10, 1e100, -1e100, 0.5, -0.5];
const JSON_VALUES = [null, true, false, 0, '', [], {}, [1, 'a'], {x: 1}, {nested: {deep: true}}, [null, true, 's']];

function randValue(rng, type, optional) {
  if (optional && rng.chance(0.25)) return null;
  switch (type) {
    case 'number': return rng.pick(NUMBERS);
    case 'boolean': return rng.chance(0.5);
    case 'json': return rng.pick(JSON_VALUES);
    default: return rng.pick(STRINGS);
  }
}

export function genTables(rng) {
  const nTables = rng.int(1, 3);
  const tables = {};
  const names = ['t0', 't1', 't2'].slice(0, nTables);
  for (let ti = 0; ti < nTables; ti++) {
    const name = names[ti];
    // 10% chance: compound PK (id + id2)
    const compoundPK = ti > 0 && rng.chance(0.1);
    const columns = {id: 'string'};
    const colTypes = {id: ['string', false]};
    if (compoundPK) { columns.id2 = 'string'; colTypes.id2 = ['string', false]; }
    const nCols = rng.int(2, 4);
    for (let c = 0; c < nCols; c++) {
      const cname = `c${c}`;
      const base = rng.pick(['number', 'string', 'boolean', 'number', 'string']);
      const optional = rng.chance(0.4);
      columns[cname] = optional ? `${base}|null` : base;
      colTypes[cname] = [base, optional];
    }
    if (ti > 0) { columns.fk = 'string'; colTypes.fk = ['string', false]; }
    const nRows = rng.chance(0.1) ? 0 : rng.int(5, 50);
    const rows = [];
    for (let r = 0; r < nRows; r++) {
      const row = {id: `${name}-r${r}`};
      if (compoundPK) row.id2 = `v${r % 3}`;
      for (const [cname, [base, optional]] of Object.entries(colTypes)) {
        if (cname === 'id' || cname === 'id2') continue;
        if (cname === 'fk') { row.fk = `t0-r${rng.int(0, 60)}`; continue; }
        row[cname] = randValue(rng, base, optional);
      }
      rows.push(row);
    }
    tables[name] = {columns, primaryKey: compoundPK ? ['id', 'id2'] : ['id'], rows, _colTypes: colTypes};
  }
  return tables;
}

const CMP_OPS = ['=', '!=', '<', '<=', '>', '>='];

function colRef(name) { return {type: 'column', name}; }
function lit(value) { return {type: 'literal', value}; }

function randLiteralFor(rng, table, cname) {
  // Bias toward values that actually occur (boundary ties), else random pool.
  if (rng.chance(0.6) && table.rows.length > 0) {
    const row = rng.pick(table.rows);
    if (row[cname] !== undefined && row[cname] !== null) return row[cname];
  }
  const [base] = table._colTypes[cname] ?? ['string'];
  const v = randValue(rng, base, false);
  return v;
}

function genSimpleCondition(rng, table) {
  const cols = Object.keys(table._colTypes).filter(c => c !== 'fk');
  const cname = rng.pick(cols);
  const [base] = table._colTypes[cname];
  const roll = rng.rand();
  if (roll < 0.10) {
    // IS / IS NOT NULL (safe on all types)
    return {type: 'simple', op: rng.chance(0.5) ? 'IS' : 'IS NOT',
            left: colRef(cname), right: lit(null)};
  }
  if (roll < 0.16) {
    // Literal vs literal comparison (1=1, 'a'='a', etc.) — exercises the
    // build-time evaluation path (the TableSource bug the scalar fuzzer
    // found where 1=1 returned 0 rows). Only = and != are safe here.
    const val1 = randLiteralFor(rng, table, cname);
    const val2 = rng.chance(0.5) ? val1 : randLiteralFor(rng, table, cname);
    return {type: 'simple', op: rng.pick(['=', '!=']),
            left: lit(val1), right: lit(val2)};
  }
  if (roll < 0.3 && base === 'string') {
    const pat = rng.pick(['a%', '%b%', '_', '%', 'h_llo', 'A%', '%🦀%', 'a b', '', '%null%', '_%', '___', '%_%', 'a%c', 'Z%', '%Z', 'a_b_c']);
    const likeOps = ['LIKE', 'ILIKE', 'NOT LIKE', 'NOT ILIKE'];
    return {type: 'simple', op: rng.pick(likeOps),
            left: colRef(cname), right: lit(pat)};
  }
  if (roll < 0.45) {
    // 5% chance: empty IN list (matches nothing)
    const n = rng.chance(0.05) ? 0 : rng.int(1, 4);
    const vals = [];
    for (let i = 0; i < n; i++) vals.push(randLiteralFor(rng, table, cname));
    return {type: 'simple', op: rng.chance(0.3) ? 'NOT IN' : 'IN', left: colRef(cname), right: lit(vals)};
  }
  return {type: 'simple', op: rng.pick(CMP_OPS),
          left: colRef(cname), right: lit(randLiteralFor(rng, table, cname))};
}

function genExistsCondition(rng, rootName, tables, allowNotExists) {
  const others = Object.keys(tables).filter(t => t !== rootName && tables[t].columns.fk);
  if (rootName !== 't0' || others.length === 0) return null;
  const child = rng.pick(others);
  const op = allowNotExists && rng.chance(0.35) ? 'NOT EXISTS' : 'EXISTS';
  const sub = {table: child, alias: `zsubq_${child}`, orderBy: [['id', 'asc']]};
  if (rng.chance(0.4)) sub.where = genSimpleCondition(rng, tables[child]);
  const cond = {
    type: 'correlatedSubquery',
    related: {
      correlation: {parentField: ['id'], childField: ['fk']},
      subquery: sub,
    },
    op,
  };
  // Coverage: exercise the flipped-subquery pipeline — FlippedJoin when the
  // condition stands alone / under AND, and UnionFanOut/UnionFanIn when it sits
  // inside an OR (genCondition wraps conditions in OR ~15% of the time). Only
  // positive EXISTS is flipped; NOT EXISTS + flip is not a supported plan shape.
  if (op === 'EXISTS' && rng.chance(0.4)) cond.flip = true;
  return cond;
}

// A scalar-flagged EXISTS: `whereExists(rel, q, {scalar: true})` where q pins
// the subquery table's unique key (PK 'id'). resolveSimpleScalarSubqueries
// bakes it to a literal `parentField = value` (or ALWAYS_FALSE) and ships the
// matched row as a companion. Correlation parentField is the root PK so at
// most one parent matches (a clean 1:1 with the companion).
function genScalarExistsCondition(rng, rootName, tables) {
  const others = Object.keys(tables).filter(t => t !== rootName && tables[t].columns.fk);
  if (others.length === 0) return null;
  const child = rng.pick(others);
  const rows = tables[child].rows;
  // Bias toward an existing child row (→ match + companion); sometimes a
  // missing id (→ resolved undefined → ALWAYS_FALSE, no companion).
  const pinId = (rows.length > 0 && rng.chance(0.8))
    ? rng.pick(rows).id
    : `${child}-x${rng.int(0, 99)}`;
  const sub = {
    table: child, alias: `zsubq_${child}`, orderBy: [['id', 'asc']],
    where: {type: 'simple', op: '=', left: colRef('id'), right: lit(pinId)},
  };
  return {
    type: 'correlatedSubquery',
    op: 'EXISTS',
    scalar: true,
    related: {correlation: {parentField: ['id'], childField: ['fk']}, subquery: sub},
  };
}

function genCondition(rng, rootName, tables, depth, allowNotExists) {
  const table = tables[rootName];
  const roll = rng.rand();
  if (depth < 2 && roll < 0.3) {
    const kind = rng.chance(0.5) ? 'and' : 'or';
    const n = rng.int(2, 3);
    const conditions = [];
    for (let i = 0; i < n; i++) {
      conditions.push(genCondition(rng, rootName, tables, depth + 1, allowNotExists));
    }
    return {type: kind, conditions};
  }
  if (roll < 0.42) {
    // ~25% of the EXISTS budget generates the scalar variant.
    const c = rng.chance(0.25)
      ? genScalarExistsCondition(rng, rootName, tables)
      : genExistsCondition(rng, rootName, tables, allowNotExists);
    if (c) return c;
  }
  return genSimpleCondition(rng, table);
}

// Does the AST contain a scalar-flagged subquery anywhere? Scalar fixtures run
// with no pushes: the napi harness re-hydrates for finalView (so companion
// MONITORING is not exercised here — that path is covered by the engine-level
// unit tests), and the oracle resolves once, so hydrate must equal finalView.
function astHasScalar(node) {
  if (!node) return false;
  const scanCond = c => {
    if (!c) return false;
    if (c.type === 'correlatedSubquery') {
      if (c.scalar) return true;
      return astHasScalar(c.related && c.related.subquery);
    }
    if (c.conditions) return c.conditions.some(scanCond);
    return false;
  };
  if (scanCond(node.where)) return true;
  return (node.related || []).some(r => astHasScalar(r.subquery));
}

export function genFixture(seed) {
  const rng = makeRng(seed);
  const tables = genTables(rng);
  const rootName = rng.pick(Object.keys(tables));
  const root = tables[rootName];
  const enableNotExists = rng.chance(0.3);

  const ast = {table: rootName};
  // orderBy: 0-2 non-PK columns asc/desc, always ending in the PK tiebreaker.
  const allSortCols = Object.keys(root._colTypes).filter(c => c !== 'id' && c !== 'id2');
  const sortCols = rng.shuffle(allSortCols)
    .slice(0, rng.int(0, Math.min(allSortCols.length, 3)));
  ast.orderBy = [...sortCols.map(c => [c, rng.chance(0.5) ? 'asc' : 'desc']),
                 ['id', rng.chance(0.85) ? 'asc' : 'desc']];
  if (rng.chance(0.55)) ast.where = genCondition(rng, rootName, tables, 0, enableNotExists);
  if (rng.chance(0.5)) ast.limit = rng.int(1, 12);
  if (rng.chance(0.25) && root.rows.length > 0) {
    // start: use a real row from the table (ensures the PK columns match)
    ast.start = {row: rng.pick(root.rows), exclusive: rng.chance(0.5)};
  }
  const relatedTargets = Object.keys(tables).filter(t => t !== rootName && tables[t].columns.fk);
  if (rootName === 't0' && relatedTargets.length > 0 && rng.chance(0.55)) {
    ast.related = [];
    const nRelated = rng.int(1, Math.min(2, relatedTargets.length));
    for (let ri = 0; ri < nRelated; ri++) {
      const child = relatedTargets[ri];
      const sub = {table: child, alias: `rel${ri}`, orderBy: [['id', 'asc']]};
      if (rng.chance(0.3)) sub.limit = rng.int(1, 5);
      if (rng.chance(0.35)) sub.where = genSimpleCondition(rng, tables[child]);
      // 10% chance: nested related in the subquery (2-level deep join)
      if (rng.chance(0.1) && rootName === 't0' && child !== 't0' && tables.t0) {
        const nestedSub = {table: 't0', alias: `nested${ri}`, orderBy: [['id', 'asc']]};
        if (rng.chance(0.3)) nestedSub.limit = rng.int(1, 3);
        sub.related = [{correlation: {parentField: ['fk'], childField: ['id']}, subquery: nestedSub}];
      }
      // 15% chance of a hidden junction edge (invisible to client but children visible)
      const hidden = rng.chance(0.15);
      ast.related.push({correlation: {parentField: ['id'], childField: ['fk']}, subquery: sub, ...(hidden && {hidden: true})});
    }
  }

  // Pushes: maintain shadow state so edit/remove always reference live rows.
  // Scalar-subquery fixtures carry no pushes (see astHasScalar).
  const shadow = {};
  for (const [name, spec] of Object.entries(tables)) shadow[name] = spec.rows.map(r => ({...r}));
  const pushes = [];
  const nPushes = astHasScalar(ast) ? 0 : rng.int(0, 25);
  let nextId = rng.int(1, 1000);
  for (let i = 0; i < nPushes; i++) {
    const tname = rng.pick(Object.keys(tables));
    const t = tables[tname];
    const live = shadow[tname];
    const kind = rng.pick(live.length > 0 ? ['add', 'edit', 'edit', 'remove'] : ['add']);
    if (kind === 'add') {
      const row = {id: `${tname}-p${nextId++}`};
      for (const [cname, [base, optional]] of Object.entries(t._colTypes)) {
        if (cname === 'id') continue;
        if (cname === 'fk') { row.fk = `t0-r${rng.int(0, 60)}`; continue; }
        row[cname] = randValue(rng, base, optional);
      }
      live.push(row);
      pushes.push({type: 'add', table: tname, row});
    } else if (kind === 'edit') {
      const idx = rng.int(0, live.length - 1);
      const oldRow = {...live[idx]};
      const row = {...oldRow};
      const editable = Object.keys(t._colTypes).filter(c => c !== 'id' && c !== 'id2' && c !== 'fk');
      const cname = editable.length > 0 && rng.chance(0.85) ? rng.pick(editable) : null;
      if (cname) {
        const [base, optional] = t._colTypes[cname];
        row[cname] = randValue(rng, base, optional);
      } else if (t._colTypes.fk) {
        row.fk = `t0-r${rng.int(0, 60)}`;
      }
      // 5% chance: edit that crosses a sort boundary (changes an orderBy column
      // to a value at the extreme of the range — exercises re-sort + re-emit)
      if (rng.chance(0.05) && editable.length > 0) {
        const sortCol = rng.pick(editable);
        const [base] = t._colTypes[sortCol];
        if (base === 'number') row[sortCol] = rng.pick([1e6, -1e6, 0]);
        else if (base === 'boolean') row[sortCol] = rng.chance(0.5);
        else row[sortCol] = rng.pick(['ZZZ', '', '🦀rust']);
      }
      // Skip no-op edits (oldRow === row). Production change-streams never
      // emit them, and they create false-positive advance divergences.
      const cols = Object.keys(t._colTypes);
      const changed = cols.some(c => JSON.stringify(oldRow[c]) !== JSON.stringify(row[c]));
      if (changed) {
        live[idx] = {...row};
        pushes.push({type: 'edit', table: tname, oldRow, row});
      }
    } else {
      const idx = rng.int(0, live.length - 1);
      const row = {...live[idx]};
      live.splice(idx, 1);
      pushes.push({type: 'remove', table: tname, row});
    }
  }

  const cleanTables = {};
  for (const [name, spec] of Object.entries(tables)) {
    cleanTables[name] = {columns: spec.columns, primaryKey: spec.primaryKey, rows: spec.rows};
  }
  const fixture = {
    name: `fuzz.seed-${seed}`,
    sourceKind: 'memory',
    tables: cleanTables,
    ast,
    format: {singular: false, relationships: {}},
    pushes,
  };
  if (enableNotExists) fixture.enableNotExists = true;
  return fixture;
}

// CLI
if (import.meta.url === `file://${process.argv[1]}`) {
  let seed = null, out = null;
  const args = process.argv.slice(2);
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--seed') seed = Number(args[++i]);
    else if (args[i] === '--out') out = args[++i];
  }
  if (seed === null || !Number.isFinite(seed)) {
    console.error('Usage: gen.mjs --seed N [--out path.input.json]');
    process.exit(1);
  }
  const fixture = genFixture(seed);
  const json = JSON.stringify(fixture, null, 1) + '\n';
  if (out) { writeFileSync(out, json); console.log(`wrote ${out}`); }
  else process.stdout.write(json);
}
