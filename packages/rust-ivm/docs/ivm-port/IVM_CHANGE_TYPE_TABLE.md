# Per-Operator Change-Type Transformation Table (Extraction #3)

The "push protocol": how each operator maps an incoming `Change`
(ADD/REMOVE/EDIT/CHILD) to its output change(s). Verified against
`mono-v1.7/packages/zql/src/ivm/*.ts` (`zero/v1.7.0`). ChangeType: ADD=0,
REMOVE=1, EDIT=2, CHILD=3.

## Matrix

| Operator | ADD in | REMOVE in | EDIT in | CHILD in |
|---|---|---|---|---|
| **Filter** | ADD if pred | REMOVE if pred | split: old&!new→REMOVE, !old&new→ADD, both→EDIT | CHILD if pred |
| **Skip** | ADD if in range | REMOVE if in range | split on range (same as Filter) | CHILD if in range |
| **Take** | ADD, or REMOVE+ADD if full & new < bound | REMOVE, or REMOVE+ADD (refill from beyond bound) | **12-case machine** (`#pushEditChange` take.ts:432) | CHILD if ≤ bound |
| **Cap** | ADD if under limit | REMOVE, or REMOVE+ADD (refill) | EDIT if pk in tracked set | CHILD if pk in tracked set |
| **Join** (parent side) | ADD (+ child rel) | REMOVE (+ child rel) | EDIT (assert parentKey unchanged, join.ts:167) | CHILD (+ child rel) |
| **Join** (child side) | CHILD to each matching parent | CHILD to each matching parent | CHILD (assert childKey unchanged, join.ts:208) | CHILD nested |
| **FlippedJoin** (parent) | ADD if has child | REMOVE if has child | EDIT (assert parentKey unchanged) | CHILD if has child |
| **FlippedJoin** (child) | ADD or CHILD (per cardinality) | REMOVE or CHILD | CHILD (assert childKey unchanged, flipped-join.ts:392) | CHILD nested |
| **Exists** | passthrough if pred | passthrough if pred | passthrough if pred | **0↔1 boundary → parent ADD/REMOVE** (exists.ts:139) |
| **FanOut** | fan to all branches | fan to all branches | fan to all branches | fan to all branches |
| **FanIn** | accumulate → collapse | accumulate → collapse | accumulate → collapse | accumulate → collapse |
| **UnionFanIn** | accumulate; internal add fwd iff in ≤1 branch | internal remove fwd iff in 0 branches | accumulate | always forward (branches unique) |

## The three EDIT-splitting operators (most error-prone)

1. **Filter / Skip** — an EDIT where predicate membership changes is split:
   `old_present && !new_present → REMOVE`; `!old_present && new_present → ADD`;
   both present → `EDIT`; neither → dropped.
2. **Take** — `#pushEditChange` (take.ts:432) is the single most complex control
   flow in IVM: **4 top-level position cases × 3 actions = 12 paths**, output
   is one of `EDIT` / `REMOVE+ADD` / `REMOVE+EDIT` depending on where old-row and
   new-row sit relative to the take bound. Extract as an explicit transition
   table when porting; do not infer from control flow.

## Exists CHILD transition (exists.ts:139-185) — the 0↔1 boundary
Exists passes ADD/EDIT/REMOVE through iff the predicate holds. The subtle part
is **CHILD** changes, which move the child count across the 0↔1 boundary:

| child change | size after | EXISTS emits | NOT EXISTS emits |
|---|---|---|---|
| child ADD | becomes 1 (0→1) | parent **ADD** (exists.ts:158) | parent **REMOVE** (exists.ts:147) |
| child ADD | stays ≥2 | — (passthrough) | — |
| child REMOVE | becomes 0 (1→0) | parent **REMOVE** (exists.ts:180) | parent **ADD** (exists.ts:172) |
| child REMOVE | stays ≥1 | — | — |

The `#not` flag flips the polarity. This boundary is where the Rust port
currently diverges (see below).

## Cross-link: where the Rust port diverges on these transitions
From the fixture oracle (`IVM_FIXTURE_ORACLE.md`), the 34 quarantined
divergences map **directly** onto specific cells of this table — porting these
cells is where to look:

- **Exists CHILD 0↔1** (row above) — 10 exists-push divergences (`child add
  resulting in 1 child`, `child remove resulting in 0 children`, correlation
  edits, NOT-EXISTS mirrors).
- **Cap CHILD / refill** — 10 cap-push divergences (child add/edit/remove,
  tracked-pk-set on pk-changing edit, compound-pk refill).
- **Take EDIT / remove-at-window-start** — 6 take-push divergences (the
  `#pushEditChange` machine + remove-at-start refill).
- **FlippedJoin child cardinality EDIT** — 6 flipped-join divergences
  (assignee none↔one↔many).

So this table doubles as the fix-order guide: each diverging cluster is one
row/cell here, and each has ready fixtures in `agentic/fixtures/regressions/`.
