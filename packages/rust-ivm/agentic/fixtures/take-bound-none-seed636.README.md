# take "Bound should be set" — minimal deterministic trigger (from fuzz seed 636)

`take-bound-none-seed636.min.json` is the greedy-minimized (34-run,
`agentic/fuzz/minimize-fixture.mjs`) form of pre-json-constraint fuzz seed 636:
**3 rows, 3 pushes, limit 1** — the smallest known input that makes the STOCK
TS `PipelineDriver` throw `Bound should be set` (take.ts:445) during a single
advance batch. Rust dies earlier on the same input with `Cannot compare values
of different types: string and object` (json compare) — same class, different
first casualty.

Reproduce:

    cd packages/zero-cache
    DRIVER_FUZZ_FIXTURE=../rust-ivm/agentic/fixtures/take-bound-none-seed636.min.json \
    DRIVER_FUZZ_START=0 DRIVER_FUZZ_SEEDS=1 \
    npx vitest run src/services/view-syncer/rust-ivm-driver.fuzz.test.ts

Anatomy (t0: pk id, json column c0; query `ORDER BY c0 asc, id asc LIMIT 1`):

    rows:   r0(c0=null)  r3(c0="")  r4(c0={nested:{deep:true}})
    batch:  remove r4 → remove r0 (empties the take window)
            → edit r3 ("" → {x:1})  →  TS: "Bound should be set"

Mechanism: the leading ORDER BY key is a **json column** whose values are
heterogeneously typed (null / "" / object). SQLite (TableSource fetch/cursor
SQL) and the IVM comparator order these differently, so during the collapsed
3-change batch the take operator's replacement fetch disagrees with its
tracked bound state → `push_remove` writes `{size-1, bound: None}` →
the same-batch edit hits the `Bound should be set` assert. This is the exact
writer + assert pair behind prod's take.rs:670 panic.

HONEST CAVEAT: this minimal trigger's *skew source* (json in ORDER BY) is the
non-production-representative class deliberately excluded from the fuzzer in
9c95fa68a (prod never sorts on a json payload). Prod's panics reach the SAME
writer via storm-batch frame skew (fetches at partially-applied state) on
timestamp/string sort keys — the writer and assert are identical, the road to
them differs. Upstream-reachable either way: stock TS throws on this input.

Behavioral contract (deliberate, TS parity): the assert stays a raw
panic → thrown error → view-syncer teardown, exactly like TS (see take.rs
NOTE + `bound_none_edit_tests`).
