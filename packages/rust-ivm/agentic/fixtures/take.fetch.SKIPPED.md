# take.fetch — Skipped Cases

The following test cases from `take.fetch.test.ts` are outside the fixture schema's expressible range and are skipped:

| # | Case | Reason |
|---|------|--------|
| 7 | exception during hydrate | Tests that `Take.fetch` re-throws when the upstream `Snitch.fetch` throws — the fixture schema assumes successful hydration and cannot express exception/throw assertions. |
| 8 | early return during hydrate | Tests that `Take.fetch` asserts on downstream early-return (partial iteration) during hydration — the fixture schema drives full hydration via `Catch.fetch()` and cannot express early-return/assertion behavior. |
