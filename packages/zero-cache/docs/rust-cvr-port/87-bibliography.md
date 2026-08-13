# 87 — Bibliography & Verification Anchors

Every claim in these docs is anchored to an existing TS source line or a pinned SQL statement. When implementing, cross-check against these before running tests.

## Source files (pin these commits)

- `packages/zero-cache/src/services/view-syncer/cvr-store.ts` (1382 LOC) — post-phase-E delete candidate; verify with `git log -p --follow` since multiple authors
- `packages/zero-cache/src/services/view-syncer/cvr.ts` (1194 LOC) — two classes; the mergeRefCounts at line ~1041
- `packages/zero-cache/src/services/view-syncer/client-handler.ts` (523 LOC) — fire-and-forget ownership signal at lines 388-405
- `packages/zero-cache/src/services/view-syncer/row-record-cache.ts` (469 LOC)
- `packages/zero-cache/src/services/view-syncer/schema/types.ts` (~380 LOC) — full CVR type surface
- `packages/zero-cache/src/services/view-syncer/schema/cvr.ts` (~330 LOC) — DDL
- `packages/zero-cache/src/services/view-syncer/row-set-signature.ts` (29 LOC)
- `packages/zero-cache/src/services/view-syncer/view-syncer.ts` (2748 LOC) — for orchestration points

## Specific anchors

| Doc | Claim                                                           | Anchor                                                    |
| --- | --------------------------------------------------------------- | --------------------------------------------------------- |
| 81  | Tables: instances, clients, queries, desires, rows, rowsVersion | `schema/cvr.ts:45`, `:83`, `:124`, `:171`, `:289`, `:303` |
| 81  | `void` UPDATE ownership signal                                  | `cvr-store.ts:395`                                        |
| 81  | Defer threshold = 100                                           | `cvr-store.ts:~35` (default param)                        |
| 82  | `mergeRefCounts` property                                       | `cvr.ts:1041-1075`                                        |
| 82  | Internal query IDs `lmids`, `mutationResults`                   | `cvr.ts:75-76`                                            |
| 83  | Page size 5000 for cache load                                   | `row-record-cache.ts:~85`                                 |
| 83  | Deferred threshold default 100                                  | `row-record-cache.ts`:constructor                         |
| 84  | Poke chain `#pokeTail`                                          | `client-handler.ts:297`                                   |
| 84  | Filter at 100 parts                                             | `client-handler.ts:104`                                   |
| 84  | Special-table interception                                      | `client-handler.ts:221-270`                               |
| 85  | Void-flush code                                                 | `cvr-store.ts:388-405`                                    |

## Existing tests to mirror

- `packages/zero-cache/src/services/view-syncer/cvr.pg.test.ts` (~3300 LOC) — CVRUpdater + CVRQueryDrivenUpdater integration
- `packages/zero-cache/src/services/view-syncer/cvr-store.pg.test.ts` (~1500 LOC) — flush, ownership, catchup
- `packages/zero-cache/src/services/view-syncer/row-set-signature.test.ts` (small)
- `packages/zero-cache/src/services/view-syncer/view-syncer.pg.test.ts` — end-to-end
- `packages/zero-cache/src/services/view-syncer/view-syncer.yield-during-advance.pg.test.ts` — backpressure timing

## Design references

- `packages/zero-cache/src/services/view-syncer/pipeline-driver.ts` (1233 LOC) — the engine driver; CVR's calls into it
- `packages/rust-ivm/DESIGN-wal2-snapshot-isolation.md` — for snapshotter context
- `packages/rust-ivm/PARITY-CONTRACT.md` — for the "byte-for-byte fidelity" model these docs assume

## Open production-data references

The following facts are anchored in `agenting` notes from the `rust-ivm-v1.7.0` branch and verified during the g8mns sandbox incident RCA:

- **Per-CG Rust overhead:** ~12.6 GB mem at 2-3 CGs vs ~200 MB/CG stock. Rootcause identified as WAL sizes, not CVR loss. **Not addressed by this port** — relevant for scale-of-deployment planning only.
- **Poke chain serialization regression** (`f64f7e435` on `rust-ivm-v1.7.0`) — proves the in-place invariant matters; lock down the Rust equivalent with a dedicated regression test.
- **The `-2` reset row shim** (`TakeBoundResetError` → client-reset row in poke) — CVR doesn't directly know about this; the poke-format carries it indistinguishable from any other poke Part. No CVR impact.
