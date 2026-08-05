#!/usr/bin/env node
// agentic/fuzz/minimize-fixture.mjs — greedy delta-debugging for driver-fuzz
// fixtures. Shrinks a fixture while a predicate regex still matches the
// differential fuzz test's output (run via DRIVER_FUZZ_FIXTURE).
//
// Usage:
//   node agentic/fuzz/minimize-fixture.mjs <fixture.json> <out.json> \
//        '<predicate-regex>'
// Example (seed-636 take bound=None):
//   node agentic/fuzz/minimize-fixture.mjs /tmp/fixture-636.json \
//        /tmp/fixture-636.min.json 'ts=.*Bound should be set'

import {execFileSync} from 'node:child_process';
import {readFileSync, writeFileSync} from 'node:fs';
import {dirname, join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {tmpdir} from 'node:os';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ZERO_CACHE = resolve(__dirname, '..', '..', '..', 'zero-cache');
const [fixturePath, outPath, predicateSrc] = process.argv.slice(2);
if (!fixturePath || !outPath || !predicateSrc) {
  console.error(
    'Usage: minimize-fixture.mjs <fixture.json> <out.json> <predicate-regex>',
  );
  process.exit(2);
}
const predicate = new RegExp(predicateSrc);
const candPath = join(tmpdir(), `minimize-cand-${process.pid}.json`);

let runs = 0;
function holds(fixture) {
  runs++;
  writeFileSync(candPath, JSON.stringify(fixture));
  let out = '';
  try {
    out = execFileSync(
      'npx',
      [
        'vitest',
        'run',
        'src/services/view-syncer/rust-ivm-driver.fuzz.test.ts',
      ],
      {
        cwd: ZERO_CACHE,
        env: {
          ...process.env,
          DRIVER_FUZZ_FIXTURE: candPath,
          DRIVER_FUZZ_START: '0',
          DRIVER_FUZZ_SEEDS: '1',
        },
        timeout: 180_000,
        maxBuffer: 64 * 1024 * 1024,
        stdio: ['ignore', 'pipe', 'pipe'],
      },
    ).toString();
  } catch (e) {
    out =
      (e.stdout ? e.stdout.toString() : '') +
      (e.stderr ? e.stderr.toString() : '');
  }
  return predicate.test(out);
}

const clone = f => JSON.parse(JSON.stringify(f));

let fixture = JSON.parse(readFileSync(fixturePath, 'utf8'));
if (!holds(fixture)) {
  console.error('predicate does not hold on the ORIGINAL fixture — aborting');
  process.exit(1);
}
console.log('predicate holds on original; minimizing…');

let changed = true;
while (changed) {
  changed = false;

  // 1. Drop whole tables (and their pushes) other than the query root.
  const root = fixture.ast && fixture.ast.table;
  for (const t of Object.keys(fixture.tables)) {
    if (t === root) continue;
    const cand = clone(fixture);
    delete cand.tables[t];
    cand.pushes = (cand.pushes || []).filter(p => p.table !== t);
    if (holds(cand)) {
      fixture = cand;
      changed = true;
      console.log(`dropped table ${t}`);
    }
  }

  // 2. Drop pushes, one at a time (from the end — later pushes often depend
  // on earlier ones, so removing tail-first converges faster).
  for (let i = (fixture.pushes || []).length - 1; i >= 0; i--) {
    const cand = clone(fixture);
    cand.pushes.splice(i, 1);
    if (holds(cand)) {
      fixture = cand;
      changed = true;
      console.log(`dropped push[${i}] (${cand.pushes.length} left)`);
    }
  }

  // 3. Drop initial rows, one at a time.
  for (const t of Object.keys(fixture.tables)) {
    for (let i = fixture.tables[t].rows.length - 1; i >= 0; i--) {
      const cand = clone(fixture);
      cand.tables[t].rows.splice(i, 1);
      if (holds(cand)) {
        fixture = cand;
        changed = true;
        console.log(`dropped ${t}.rows[${i}] (${cand.tables[t].rows.length} left)`);
      }
    }
  }

  // 4. Simplify the AST: drop where / related / orderBy columns (never the
  // trailing pk tiebreaker), and try smaller limits.
  {
    const cand = clone(fixture);
    if (cand.ast && cand.ast.where) {
      delete cand.ast.where;
      if (holds(cand)) {
        fixture = cand;
        changed = true;
        console.log('dropped ast.where');
      }
    }
  }
  if (fixture.ast && Array.isArray(fixture.ast.orderBy)) {
    for (let i = fixture.ast.orderBy.length - 2; i >= 0; i--) {
      const cand = clone(fixture);
      cand.ast.orderBy.splice(i, 1);
      if (holds(cand)) {
        fixture = cand;
        changed = true;
        console.log(`dropped orderBy[${i}]`);
      }
    }
  }
  if (fixture.ast && typeof fixture.ast.limit === 'number') {
    for (const lim of [1, fixture.ast.limit - 1]) {
      if (lim >= 1 && lim < fixture.ast.limit) {
        const cand = clone(fixture);
        cand.ast.limit = lim;
        if (holds(cand)) {
          fixture = cand;
          changed = true;
          console.log(`limit -> ${lim}`);
          break;
        }
      }
    }
  }

  // 5. Drop unused columns from the root table (never pk/orderBy columns).
  {
    const root = fixture.ast && fixture.ast.table;
    const spec = root && fixture.tables[root];
    if (spec) {
      const keep = new Set([
        ...(spec.primaryKey || ['id']),
        ...((fixture.ast.orderBy || []).map(o => o[0])),
      ]);
      for (const col of Object.keys(spec.columns)) {
        if (keep.has(col)) continue;
        const cand = clone(fixture);
        delete cand.tables[root].columns[col];
        for (const r of cand.tables[root].rows) delete r[col];
        for (const p of cand.pushes || []) {
          if (p.row) delete p.row[col];
          if (p.oldRow) delete p.oldRow[col];
        }
        if (holds(cand)) {
          fixture = cand;
          changed = true;
          console.log(`dropped column ${root}.${col}`);
        }
      }
    }
  }
}

writeFileSync(outPath, JSON.stringify(fixture, null, 1) + '\n');
console.log(
  `MINIMAL (${runs} runs): tables=${Object.keys(fixture.tables)
    .map(t => `${t}:${fixture.tables[t].rows.length}rows`)
    .join(',')} pushes=${(fixture.pushes || []).length} -> ${outPath}`,
);
