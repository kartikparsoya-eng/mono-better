// TS-golden PLAN fixture: drives the REAL TS planner (zql planQuery) over a
// set of ASTs with a deterministic, data-driven mock cost model and records
// the PLANNED AST (the `flip` annotations on every correlated subquery).
//
// Why: plan-CHOICE divergences are invisible to the row-output oracle — a
// semi and a flipped plan return the same rows. The 2026-08 sweep found three
// Rust planner bugs (chunk-size 500-vs-256, BTreeMap breaking multi-column
// constraint pairing, a never-written connection cost cache) that only a plan
// comparison can pin. The Rust twin (`tests/planner_ts_golden_test.rs`)
// replays the same ASTs through `plan_query` with the IDENTICAL mock model
// and asserts the flip maps match.
//
// Regenerate: npx tsx generate-planner-fixture.mjs > planner-fixture.json
import {planQuery} from '../../../zql/src/planner/planner-builder.ts';

// ─── Mock cost model ─────────────────────────────────────────────────────
// MUST stay semantically identical to `mock_model` in
// tests/planner_ts_golden_test.rs:
//   rows      = constraint ? (constrained[constraintColsInIterationOrder
//                             .join(',')] ?? constrainedDefault ?? 1)
//             : filters    ? (filtered ?? rows)
//             : rows
//   startup   = startup ?? 1
//   fanout(_) = {fanout: fanout ?? 1, confidence: 'none'}
// The constraint branch is keyed by WHICH columns are constrained so that
// wrong column pairing (the NEW-2 class) changes the cost.
function makeModel(tables) {
  return (table, _sort, filters, constraint) => {
    const cfg = tables[table] ?? {};
    let rows;
    if (constraint) {
      // NATURAL iteration order (TS Record = insertion order) — a real cost
      // model observes the constraint exactly this way, so a Rust map type
      // that re-sorts keys (the NEW-2 BTreeMap class) produces a different
      // key and a different cost.
      const key = Object.keys(constraint).join(',');
      rows = cfg.constrained?.[key] ?? cfg.constrainedDefault ?? 1;
    } else if (filters) {
      rows = cfg.filtered ?? cfg.rows ?? 100;
    } else {
      rows = cfg.rows ?? 100;
    }
    return {
      startupCost: cfg.startup ?? 1,
      rows,
      fanout: _cols => ({fanout: cfg.fanout ?? 1, confidence: 'none'}),
    };
  };
}

// ─── AST helpers (zero-protocol wire shape) ──────────────────────────────
const asc = cols => cols.map(c => [c, 'asc']);

function exists(childTable, parentField, childField, opts = {}) {
  return {
    type: 'correlatedSubquery',
    op: opts.op ?? 'EXISTS',
    related: {
      correlation: {parentField, childField},
      subquery: {
        table: childTable,
        alias: opts.alias ?? `zsubq_${childTable}`,
        orderBy: asc(opts.orderBy ?? ['id']),
        ...(opts.where ? {where: opts.where} : {}),
        ...(opts.limit !== undefined ? {limit: opts.limit} : {}),
      },
      system: 'client',
    },
  };
}

const flagFilter = {
  type: 'simple',
  op: '=',
  left: {type: 'column', name: 'flag'},
  right: {type: 'literal', value: 1},
};

// ─── Scenarios ───────────────────────────────────────────────────────────
const SCENARIOS = [
  {
    name: 'single-exists-semi-wins',
    // Cheap constrained child probe per parent row → semi wins.
    tables: {
      issue: {rows: 50},
      comments: {rows: 10_000, constrainedDefault: 2},
    },
    ast: {
      table: 'issue',
      orderBy: asc(['id']),
      where: exists('comments', ['id'], ['issueID']),
    },
  },
  {
    name: 'single-exists-flipped-wins',
    // Tiny child, huge parent scan, cheap constrained parent seek → flip wins.
    tables: {
      issue: {rows: 100_000, constrainedDefault: 1},
      comments: {rows: 5, constrainedDefault: 5},
    },
    ast: {
      table: 'issue',
      orderBy: asc(['id']),
      where: exists('comments', ['id'], ['issueID']),
    },
  },
  {
    name: 'chunk-boundary-sensitive',
    // Tuned (empirically, via the Rust chunk-override probe) so the
    // flipped-join chunk count decides the plan: with the TS chunk size 256
    // SEMI wins; with the divergent 500 the planner FLIPS — the NEW-1
    // regression this scenario exists to catch (proven by temp-revert).
    tables: {
      issue: {rows: 500, constrainedDefault: 1, startup: 200},
      comments: {rows: 600, constrainedDefault: 5, startup: 1},
    },
    ast: {
      table: 'issue',
      orderBy: asc(['id']),
      where: exists('comments', ['id'], ['issueID']),
    },
  },
  {
    name: 'not-exists-unflippable',
    tables: {
      issue: {rows: 100_000, constrainedDefault: 1},
      comments: {rows: 5},
    },
    ast: {
      table: 'issue',
      orderBy: asc(['id']),
      where: exists('comments', ['id'], ['issueID'], {op: 'NOT EXISTS'}),
    },
  },
  {
    name: 'or-two-exists-fanin',
    // OR of two EXISTS → FanOut/FanIn. Each EXISTS subquery carries its own
    // limit + filter, so each child connection gets a selectivity < 1 and the
    // two branches revisit the SHARED root connection with DIFFERENT
    // downstream child selectivities. TS's cost cache is keyed by branch
    // pattern ONLY, so the second branch reuses the FIRST branch's scanEst —
    // that staleness is part of the golden, and a Rust planner that
    // recomputes per-dcs (the NEW-3 never-written-cache regression) picks a
    // different plan here (proven by cache-write-toggle probe + temp-revert:
    // cached → [false,false], uncached → [true,true]).
    tables: {
      issue: {rows: 2_000, filtered: 100, constrainedDefault: 1},
      comments: {rows: 30, filtered: 6, constrainedDefault: 4, fanout: 8},
      labels: {rows: 2_000, filtered: 100, constrainedDefault: 2, fanout: 1},
    },
    ast: {
      table: 'issue',
      orderBy: asc(['id']),
      limit: 10,
      where: {
        type: 'and',
        conditions: [
          flagFilter,
          {
            type: 'or',
            conditions: [
              exists('comments', ['id'], ['issueID'], {
                where: flagFilter,
                limit: 3,
              }),
              exists('labels', ['id'], ['issueID'], {
                where: flagFilter,
                limit: 7,
              }),
            ],
          },
        ],
      },
    },
  },
  {
    name: 'nested-exists',
    tables: {
      issue: {rows: 2_000, constrainedDefault: 1},
      comments: {rows: 50, constrainedDefault: 3},
      reactions: {rows: 10, constrainedDefault: 2},
    },
    ast: {
      table: 'issue',
      orderBy: asc(['id']),
      where: exists('comments', ['id'], ['issueID'], {
        where: exists('reactions', ['id'], ['commentID']),
      }),
    },
  },
  {
    name: 'multi-col-correlation-pairing',
    // parentField ['b','a'] ↔ childField ['x','y'] with strongly asymmetric
    // per-column constrained costs on the child. Correct positional pairing
    // (b→x, a→y — Record insertion order) vs alphabetical re-sorting (b→y)
    // produces different constraint keys → different costs → the NEW-2
    // regression this scenario exists to catch.
    // parentField ['b','a'] is deliberately ANTI-alphabetical: under a
    // FLIPPED join the parent's merged constraint key is 'b,a' in insertion
    // order (TS Record) but 'a,b' under a re-sorting map — the latter misses
    // `constrained` and falls back to the expensive constrainedDefault, so
    // flipped loses and the plan changes. (The SEMI child cost is
    // insensitive: an EXISTS probe stops at the first row, so the child's
    // constrained scan never enters the semi total — found empirically.)
    // Tuned: semi = 3000*2 = 6001; flipped(insertion 'b,a'→1) ≈ 4017 WINS;
    // flipped(sorted 'a,b'→5000) ≈ 20M loses.
    tables: {
      issue: {rows: 3_000, constrainedDefault: 5_000, constrained: {'b,a': 1}},
      links: {rows: 4_000, constrainedDefault: 2},
    },
    ast: {
      table: 'issue',
      orderBy: asc(['id']),
      where: exists('links', ['b', 'a'], ['y', 'x'], {alias: 'zsubq_links'}),
    },
  },
  {
    name: 'related-subquery-recursion',
    // `related` gets its own sub-plan (planQuery recurses via subPlans).
    tables: {
      issue: {rows: 300, constrainedDefault: 1},
      comments: {rows: 40_000, constrainedDefault: 2},
      reactions: {rows: 12, constrainedDefault: 6},
    },
    ast: {
      table: 'issue',
      orderBy: asc(['id']),
      related: [
        {
          correlation: {parentField: ['id'], childField: ['issueID']},
          subquery: {
            table: 'comments',
            alias: 'comments',
            orderBy: asc(['id']),
            where: exists('reactions', ['id'], ['commentID']),
          },
          system: 'client',
        },
      ],
    },
  },
];

// ─── Run ─────────────────────────────────────────────────────────────────
const out = SCENARIOS.map(sc => {
  // astTables lets a scenario override table cfg (merged over `tables`).
  const tables = {...sc.tables};
  for (const [t, extra] of Object.entries(sc.astTables ?? {})) {
    tables[t] = {...tables[t], ...extra};
  }
  const planned = planQuery(structuredClone(sc.ast), makeModel(tables));
  return {name: sc.name, tables, ast: sc.ast, plannedAst: planned};
});

console.log(JSON.stringify({scenarios: out}, null, 2));
