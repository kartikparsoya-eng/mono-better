# push-accumulated — Skipped Cases

All 25 test cases from `push-accumulated.test.ts` are outside the fixture schema's
expressible range and are skipped.

**Root cause:** Every test is a direct unit test of one of three internal IVM
functions — `pushAccumulatedChanges` (cases 1–15), `mergeRelationships`
(cases 16–22), or `makeAddEmptyRelationships` (cases 23–25) — that are called
in isolation with hand-crafted `Change` objects. The fixture schema
(`ts-runner.mjs`) is a pipeline-level integration framework: it builds a
pipeline from `tables` + `ast`, drives it with source-level pushes
(`add`/`remove`/`edit` carrying only `row`/`oldRow`), and captures output as
`CaughtChange[]`. There is no mechanism to invoke these internal functions
directly, supply a specific `fanOutChangeType`, choose the
`mergeRelationships`/`addEmptyRelationships` callback pair, push `child`
changes from a source, supply function-valued relationships, or assert on
internal `Change` tuple structure (`ChangeIndex.TYPE`, relationship key sets,
function-reference identity, throw expectations).

| # | Case | Reason |
|---|------|--------|
| 1 | single add change passes through | Direct unit test of `pushAccumulatedChanges` with `mergeRelationships`+`identity` callbacks; no pipeline operator uses this callback pair (FanIn uses identity/identity, UnionFanIn uses mergeRelationships/makeAddEmptyRelationships), and the fixture schema cannot invoke the function in isolation or control `fanOutChangeType`. |
| 2 | multiple add changes collapse to single add | Direct unit test supplying two hand-crafted `makeAddChange` with function-valued relationships (`rel1: () => []`); source-level pushes only carry `row` data, not relationship functions, and the `mergeRelationships`+`identity` callback pair is unavailable in any pipeline operator. |
| 3 | no changes when all branches filter out add | Direct unit test passing an empty `Change[]` to `pushAccumulatedChanges` with `fanOutChangeType=ADD`; the fixture schema cannot invoke the function in isolation or assert on zero-output from a specific internal call. |
| 4 | single remove change passes through | Direct unit test of `pushAccumulatedChanges` with `fanOutChangeType=REMOVE` and `mergeRelationships`+`identity` callbacks; no pipeline operator uses this callback pair and `fanOutChangeType` cannot be independently set. |
| 5 | multiple remove changes collapse to single remove | Direct unit test supplying two hand-crafted `makeRemoveChange` with function-valued relationships; source pushes cannot supply relationship functions and the callback pair is unavailable in any pipeline operator. |
| 6 | edit preserved as edit | Direct unit test of `pushAccumulatedChanges` with `fanOutChangeType=EDIT` and `mergeRelationships`+`identity`; no pipeline operator uses this callback pair and `fanOutChangeType` cannot be independently controlled. |
| 7 | edit converted to add only | Direct unit test passing a single `makeAddChange` with `fanOutChangeType=EDIT`; the fixture schema cannot set `fanOutChangeType` independently of the incoming change type or invoke the function in isolation. |
| 8 | edit converted to remove only | Direct unit test passing a single `makeRemoveChange` with `fanOutChangeType=EDIT`; `fanOutChangeType` cannot be independently controlled through the fixture schema. |
| 9 | edit split into add and remove recombines to edit | Direct unit test passing hand-crafted `makeAddChange`+`makeRemoveChange` with `fanOutChangeType=EDIT` and `mergeRelationships`+`identity`; the fixture schema cannot supply specific change-type pairs to the fan-in or use the `identity` addEmptyRelationships callback. |
| 10 | edit supersedes add and remove when all three present | Direct unit test passing hand-crafted edit+add+remove with function-valued relationships and asserting on `ChangeIndex.NODE`/`OLD_NODE` relationship key sets; `CaughtEditChange` only captures `{type, oldRow, row}` (no relationships), so the edit-side relationship assertions are unobservable in fixture output. |
| 11 | child preserved as child takes precedence | Direct unit test passing a `makeChildChange` with `fanOutChangeType=CHILD`; `child` changes cannot be pushed from a `MemorySource` (source pushes only support add/remove/edit) and `fanOutChangeType=CHILD` cannot be independently set. |
| 12 | child converted to add only | Direct unit test passing a single `makeAddChange` with `fanOutChangeType=CHILD`; `fanOutChangeType` cannot be independently set to a value different from the incoming change type through the fixture schema. |
| 13 | child converted to remove only | Direct unit test passing a single `makeRemoveChange` with `fanOutChangeType=CHILD`; `fanOutChangeType` cannot be independently controlled through the fixture schema. |
| 14 | child takes precedence over add/remove when present | Direct unit test passing a `makeChildChange`+`makeAddChange` with `fanOutChangeType=CHILD`; `child` changes cannot be pushed from a source and the fixture schema cannot supply specific change-type combinations to the fan-in. |
| 15 | child ensures at most one add or remove (not both) | Direct unit test asserting `toThrow('Fan-in:child expected either add or remove, not both')`; the fixture schema has no mechanism for expecting throws/assertion failures from internal operators. |
| 16 | merges relationships from add changes | Direct unit test calling `mergeRelationships(left, right)` with two hand-crafted `makeAddChange` objects; `mergeRelationships` is an internal function never called in isolation by any pipeline operator, and the fixture schema cannot invoke it directly. |
| 17 | merges relationships from remove changes | Direct unit test calling `mergeRelationships` with two hand-crafted `makeRemoveChange` objects; the function cannot be invoked in isolation through the fixture schema. |
| 18 | merges relationships from edit changes | Direct unit test calling `mergeRelationships` with two hand-crafted `makeEditChange` objects and asserting on both `NODE` and `OLD_NODE` relationship key sets; `CaughtEditChange` captures no relationships, making the assertions unobservable. |
| 19 | left takes precedence when same relationship exists | Direct unit test asserting `result[ChangeIndex.NODE].relationships.rel1` is the exact same function reference as `rel1Left` (`toBe(rel1Left)`); `CaughtNode` expands relationship functions to arrays of child nodes, destroying function-reference identity. |
| 20 | merges edit with add | Direct unit test calling `mergeRelationships(makeEditChange(...), makeAddChange(...))` and asserting on `NODE` relationship keys; the function cannot be invoked in isolation and edit-side relationship keys are not captured in `CaughtEditChange`. |
| 21 | merges edit with remove | Direct unit test calling `mergeRelationships(makeEditChange(...), makeRemoveChange(...))` and asserting on `OLD_NODE` relationship keys; the function cannot be invoked in isolation and edit-side relationship keys are not captured in `CaughtEditChange`. |
| 22 | merges relationships from child changes | Direct unit test calling `mergeRelationships` with two hand-crafted `makeChildChange` objects; `child` changes cannot be pushed from a source and `mergeRelationships` cannot be invoked in isolation through the fixture schema. |
| 23 | adds empty relationships for add change | Direct unit test calling `makeAddEmptyRelationships(schema)(change)` with a hand-crafted `makeAddChange` and asserting on `relationships.rel1?.()` returning `[]`; the function cannot be invoked in isolation through the fixture schema (only called internally by `UnionFanIn` with a different callback pair than the test's `identity`). |
| 24 | adds empty relationships for remove change | Direct unit test calling `makeAddEmptyRelationships(schema)(change)` with a hand-crafted `makeRemoveChange`; the function cannot be invoked in isolation through the fixture schema. |
| 25 | adds empty relationships for edit change | Direct unit test calling `makeAddEmptyRelationships(schema)(change)` with a hand-crafted `makeEditChange` and asserting on both `NODE` and `OLD_NODE` relationship keys; the function cannot be invoked in isolation and `CaughtEditChange` captures no relationships. |
