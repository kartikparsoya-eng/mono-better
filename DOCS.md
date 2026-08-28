# Documentation index — the single source of doc status

Every `.md` in this repo (rust-port scope) is classified here. See AGENTS.md
"Docs discipline" for the rules. If a doc is not listed, treat it as ARCHIVED
until classified.

**LIVING** — maintained; must be correct; update in the same commit as the
change that invalidates it.
**GENERATED** — owned by a tool; regenerate, never hand-edit.
**ARCHIVED** — frozen snapshot of finished work; do not update; status/dates
inside reflect their time of writing, not the present.

## LIVING

| Doc | Purpose |
|---|---|
| `AGENTS.md` | Project instructions + HARD RULES (parity, tests, docs) |
| `DOCS.md` | This index |
| `parity/ZERO-DIVERGENCE-PLAN.md` | Divergence-layer system status ledger (L1–L8) |
| `parity/INVENTIONS.md` | L6: rust-only inventions — contracts + pinning tests |
| `parity/PARITY-EXCEPTIONS.md` | Sanctioned TS↔rust deltas with justification |
| `parity/L7-PROSE-INVARIANTS.md` | L7: prose invariants checklist |
| `parity/L3-CONTEXT-MAP.md` | L3: execution-context map for ordering-sensitive emissions |
| `parity/L4-SNAPSHOT-SWEEP.md` | L4: snapshot-freshness sweep (state read at use time) |
| `parity/L8-RUNBOOK.md` | L8: how to run the traffic-driven path differential |
| `parity/BEHAVIORAL-SWEEP-FINDINGS.md` | Audit log — append corrections; never restructure |
| `packages/rust-syncer/OPERATIONS.md` | Prod runbook: rollback, drain, profiling, known sharp edges |
| `packages/rust-ivm/RUST-DRIFT-LEDGER.md` | Known drift ledger for the ivm crate |
| `RUST-SYNCER-ARCHITECTURE.md` | Current architecture overview (threads, CG model, offload) |
| `PROFILING.md` | How to profile + coverage recipes |

## GENERATED (tool-owned)

| Doc | Generator |
|---|---|
| `parity/MAP-cvr.md`, `parity/MAP-ivm.md`, `parity/MAP-syncer.md` | `parity/parity_ledger.py` |
| `parity/COVERAGE-cvr.md`, `parity/COVERAGE-ivm.md`, `parity/COVERAGE-syncer.md` | `parity/layer2_coverage.py` |

## ARCHIVED (frozen; superseded by the parity/ layer system or completed)

- `parity/LAYER2-BODY-DIFF-FINDINGS.md` — L2 sweep findings (completed sweeps)
- `parity/L8-TRIAGE.md` — first/second L8 run dispositions (completed)
- `parity/L8-PATH-DIFF.md` — L8 design note (runbook is the living half)
- `parity/I8-CCM-PROMOTION-SPEC.md` — CCM promotion spec (DONE, task #155)
- `RUST-SYNCER-TS-PARITY.md` — early parity notes; superseded by `parity/`
- `RUST-SYNCER-DEEP-DIVE.md`, `RUST-CVR-DEEP-DIVE.md`, `RUST-SYNCER-DB-AND-OFFLOAD.md` — one-shot deep dives
- `RUST-SYNCER-VS-HYPERSWITCH.md` — one-shot comparison
- `ZERO_QUERY_FUZZER_EXPANSION_DESIGN.md`, `ZERO_THROUGHPUT_DESIGN.md` — one-shot designs
- `SYNC-ENGINE-EFFICIENCY-AUDIT.md`, `SYNC-ENGINE-SCALABILITY-PROPOSAL.md` — 2026-08 audit pair
- `packages/rust-ivm/PORT-AUDIT.md`, `packages/rust-ivm/AUDIT-lifecycle-wiring.md` — completed audits
- `packages/rust-ivm/DESIGN.md`, `packages/rust-ivm/DESIGN-wal2-snapshot-isolation.md`, `packages/rust-ivm/ARENA-DESIGN.md` — design notes (arena work dormant, task #103)
- `packages/rust-syncer/GATE-OBSERVATIONS.md` — ART observation backlog snapshot
