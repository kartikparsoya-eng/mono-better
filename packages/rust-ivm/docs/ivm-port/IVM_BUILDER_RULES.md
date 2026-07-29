# Builder AST → Operator-Tree Rules (Extraction #4)

Verified against `mono-v1.7/packages/zql/src/builder/builder.ts` (`zero/v1.7.0`).
Getting these wrong = wrong pipeline *shape*; no operator-level correctness
fixes that. Line refs are TS.

## Constants
| Name | Value | Where |
|---|---|---|
| `EXISTS_LIMIT` | `3` | builder.ts:224 — client-side EXISTS child limit |
| `PERMISSIONS_EXISTS_LIMIT` | `1` | builder.ts:225 — EXISTS limit under permissions system |

## Operator selection (in build order)

| Condition | Operator | Rule / line |
|---|---|---|
| `ast.start !== undefined` | `Skip` | always, wraps source (builder.ts:324) |
| correlated subquery in `where`, `flip:false` | `Join` (hidden) | non-flipped EXISTS child (builder.ts:331) |
| correlated subquery, `flip:true` | `FlippedJoin` | via `applyWhere` (builder.ts:498) |
| `ast.limit !== undefined` **and `useCap`** | `Cap` | unordered limiter (builder.ts:362) |
| `ast.limit !== undefined` **and !`useCap`** | `Take` | ordered limiter (builder.ts:374) |
| `ast.related[alias]` (limit>0 or not fromCondition) | `Join` | relationship join (builder.ts:675) |
| `where` OR with **non-flipped** subqueries | `FanOut` + `FanIn` | OR expansion (builder.ts:574/590) |
| `where` OR with **flipped** subqueries | `UnionFanOut` + `UnionFanIn` | union expansion (builder.ts:455/480) |
| EXISTS/NOT EXISTS, `subquery.limit === 0` | `Filter(() => false/true)` | dead query (builder.ts:699) |
| EXISTS/NOT EXISTS otherwise | `Exists` | filter operator (builder.ts:710) |

## The Cap-vs-Take decision (most error-prone) — builder.ts:306
```ts
const useCap =
  isNonFlippedExistsChild &&
  !(ast.where && conditionIncludesFlippedSubqueryAtAnyLevel(ast.where));
```
- **Cap** ⇔ the pipeline is a *non-flipped EXISTS child* **and** its `where`
  contains **no flipped subquery at any nesting level**.
- **Take** otherwise (any top-level query, any query whose `where` has a flipped
  subquery anywhere, any non-exists-child).

Consequence in the limiter itself (builder.ts:313):
```ts
useCap ? undefined : must(ast.orderBy)   // Cap gets NO orderBy; Take REQUIRES it
```
- **Cap** is unordered — it lets SQLite pick the scan order (avoids a temp
  b-tree; EXISTS only needs the first row).
- **Take** is ordered — `ast.orderBy` must be present (`must(...)` throws if not).

**Port note:** the Rust builder must compute `isNonFlippedExistsChild` and the
recursive `conditionIncludesFlippedSubqueryAtAnyLevel` identically. Getting
`useCap` wrong swaps Cap↔Take → wrong ordering semantics and wrong SQL plan.

## The EXISTS limit-0 dead query — builder.ts:699
```ts
if (condition.related.subquery.limit === 0) {
  if (condition.op === 'EXISTS')      return new Filter(input, () => false); // never matches
  /* NOT EXISTS */                    return new Filter(input, () => true);  // always matches
}
```
A `limit(0)` EXISTS subquery is statically decidable: `EXISTS` → constant
`false`, `NOT EXISTS` → constant `true`. No Exists/Join operators are built.

## EXISTS subquery preconditions (asserts to preserve) — builder.ts:294
- `ast.start === undefined` — EXISTS subqueries must not have `start`.
- `ast.related` empty — EXISTS subqueries must not have `related`.
(These are two of the 85 in the assertion catalog; keep them in the Rust builder.)

## related limit-0 → empty array — builder.ts:660
`if (sq.subquery.limit === 0 && fromCondition)` → produce an **empty array** for
that relationship (a `limit(0)` related field materializes as `[]`, not omitted).

## FlippedJoin OR path — builder.ts:421..480
- `applyWhere` asserts `condition.type !== 'simple'` before flips (builder.ts:421).
- An OR that contains flipped subqueries builds `UnionFanOut(end)` →
  per-branch `FlippedJoin` → `UnionFanIn(ufo, branches)`, and asserts
  `withFlipped.length > 0` (builder.ts:442/453).
- An OR without flips builds `FanOut` → branches → `FanIn` (builder.ts:574/590).

## Suggested Rust verification
Extract these into `builder` table-driven tests: assert the operator tree shape
(via the fixture replay `log`/structure) for one representative AST per row
above — especially the four `useCap` quadrants and both limit-0 dead queries.
