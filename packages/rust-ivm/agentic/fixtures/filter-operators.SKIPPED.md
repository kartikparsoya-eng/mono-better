# filter-operators — Skipped Cases

The following test case from `filter-operators.test.ts` is outside the fixture schema's expressible range and is skipped:

| # | Case | Reason |
|---|------|--------|
| 1 | fetch calls endFilter even if stream is not fully consumed | Unit test that mocks `FilterOutput` with `vi.fn()`, iterates `FilterStart.fetch()` and `break`s after 1 of 3 nodes, then asserts `beginFilter`/`endFilter` were each called exactly once — the fixture schema drives full hydration via `Catch.fetch()` (so the partial-consumption `break` scenario is inexpressible), only captures pipeline-level output rather than internal method-call counts, and `FilterEnd.endFilter()` is a no-op so the call is unobservable in output regardless. |
