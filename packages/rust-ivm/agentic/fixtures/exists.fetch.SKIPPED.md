# exists.fetch — Skipped Cases

The following test case from `exists.fetch.test.ts` is outside the fixture schema's expressible range and is skipped:

| # | Case | Reason |
|---|------|--------|
| 17 | Exists forwards beginFilter/endFilter | Unit test that mocks `FilterInput`/output with `vi.fn()` and asserts the `Exists` class forwards `beginFilter`/`endFilter` method calls to its filter output — the fixture schema only captures pipeline-level behavior (hydrate, pushChanges, finalView) and cannot express method-call-forwarding assertions on internal operator wiring. |
