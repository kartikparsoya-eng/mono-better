# CVR sequence differential (Layer-2, stateful)

A TS-vs-Rust differential that exercises the **stateful** CVR surface across many
interleaved transactions and version/configVersion transitions. Where the
fixed-scenario fixtures pin one call each, this replays random *operation
sequences* against the real TS and real Rust CVR updaters over Postgres and
asserts the resulting CVR state (persisted DB rows) + returned patches match.

Two program families (both in `corpus/`):

- **`prog-*` — config-driven** (`CVRConfigDrivenUpdater`): ensureClient /
  putDesiredQueries / markDesiredInactive / deleteDesired / clearDesired /
  deleteClient.
- **`qprog-*` — query-driven / received-rows** (`CVRQueryDrivenUpdater`): a config
  prelude desires K queries, then query transactions execute subsets
  (trackQueries), receive rows referencing them (received, merged against rows
  loaded from PG), and deleteUnreferencedRows — exercising desired→gotten
  transitions, refCount merge, row versioning, and unreferenced-row GC
  (tombstones) across many stateVersions.

On introduction the config path caught four real port divergences: the `lmids`
internal-query AST `and`-wrapper (TS uses a bare `simple`), a missing first-sight
instance write on `load`, an inactivated-desire `deleted` flag (TS writes
`deleted=true`), and nondeterministic `HashSet` patch ordering. The query path
matched TS across 120 programs (no divergences).

## Pieces

| file | role |
|------|------|
| `gen.mjs` | deterministic (seeded) program generator — the ONLY source of randomness |
| `run-ts.mjs` | replays a program through the **real TS** updaters → canonical trace |
| `../../../src/bin/cvr_seq_replay.rs` | replays through the **real Rust** updaters → same trace (`rust_cvr::seq_replay`) |
| `diff.mjs` | runs both on one program, canonicalizes, reports the first divergence |
| `fuzz.mjs` | loops seeds; on divergence ddmin-shrinks to a minimal reproducer in `regressions/` |
| `corpus/` | 40 frozen programs + their TS golden traces (the CI gate replays these) |
| `../../../tests/seq_diff_pg_test.rs` | CI gate: Rust trace == frozen TS golden, per corpus program |

## Run

```bash
export TEST_CVR_PG_URI=postgres://postgres:postgres@localhost:55432/postgres
# build the Rust replay driver
( cd ../../.. && cargo build --no-default-features --bin cvr_seq_replay )
export CVR_SEQ_REPLAY=$(cd ../../.. && pwd)/target/debug/cvr_seq_replay

node diff.mjs 42                  # diff one config seed (or a program path)
node gen.mjs --q 42 | node diff.mjs /dev/stdin   # diff one query seed
node fuzz.mjs --from 0 --to 500          # fuzz the config path
node fuzz.mjs --query --from 0 --to 500  # fuzz the query (received-rows) path

# regenerate the checked-in corpus + goldens (config + query)
node gen.mjs --corpus 40
node gen.mjs --qcorpus 30
./refresh-goldens.sh
```

The CI gate (`cargo test --no-default-features --test seq_diff_pg_test`, gated on
`TEST_CVR_PG_URI`) needs no tsx — it replays the frozen goldens.
