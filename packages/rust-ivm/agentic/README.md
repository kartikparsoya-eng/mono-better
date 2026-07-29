# rust-ivm agentic verification loop

Phase 1 of `/Users/kartik.parsoya/Documents/Go-RS/RUST-IVM-GOAL.md`, built per
`/Users/kartik.parsoya/Documents/Go-RS/AGENT-SETUP-PROMPT.md`. Ports the TS
test suites into language-independent JSON fixtures (expected outputs generated
ONLY by the TS engine) and differentially fuzzes the Rust engine against it.

## Run / stop / monitor

```bash
cd /Users/kartik.parsoya/Documents/Go-RS/rust-ivm

# status (one screen)
node agentic/status.mjs

# start the loop (2 workers) — keeps the Mac awake
caffeinate -is node agentic/orchestrate.mjs --workers 2 >> agentic/logs/loop.out 2>&1 &
# start the fuzzer cycle (5 min fuzz every 30 min)
nohup bash agentic/fuzz/fuzz-cycle.sh >> agentic/logs/fuzz-cycle.out 2>&1 &

# stop everything
pkill -f orchestrate.mjs; pkill -f fuzz-cycle.sh; pkill -f fuzz-loop.mjs
pkill -f supervisor.sh        # the 15-min self-healing watchdog (Go-RS/supervisor.sh)

# run exactly one task (trial mode)
node agentic/orchestrate.mjs --once
```

Logs: `agentic/logs/<task-id>/attempt-N/{implementer.log,gates.log,review-*.log}`,
loop-level `agentic/logs/loop.out`, fuzzer `agentic/logs/fuzz.log`.
Failed/triaged work lands in `agentic/needs-human.md` (append-only).

## File map

- `oracle/ts-runner.mjs` — runs a fixture through the TS engine (mono-v1.7
  worktree), writes `<name>.expected.json`. THE only producer of expectations.
  Invoke: `node --experimental-strip-types agentic/oracle/ts-runner.mjs <input.json>`
- `oracle/diff.mjs` — canonical JSON diff (sorted keys, -0/0, 1.0/1); exit 0 = equal.
- `src/replay.rs` + `cargo run --bin replay -- <input.json>` — Rust replayer
  (same {hydrate, pushChanges, finalView} shape).
- `tests/fixture_replay_test.rs` — replays every `agentic/fixtures/*.input.json`
  (top level only; `regressions/` = pending divergences, promoted after fix).
- `fuzz/gen.mjs --seed N` — seeded random fixture; `fuzz/fuzz-loop.mjs --minutes M`
  — differential loop; minimized findings land in `fixtures/regressions/` and
  auto-queue `fix-divergence` tasks (front of queue).
- `build-queue.mjs` — enumerates TS test files → `queue/tasks.json` (idempotent).
- `orchestrate.mjs` — the driver (no AI inside). Gates: A allowed-paths (TAMPER
  → needs-human, no retry), B oracle-regenerate expected, C `cargo test --
  --test-threads=1`. Clippy gate skipped (baseline has 215 warnings). Then 2×
  adversarial review (both must APPROVE). 3 attempts → failed/divergence-pending.
- `keys.json` (gitignored) — 3 grid API keys; every xyne spawn gets a distinct
  key index (5-concurrent limit per key; one session ≈ 1 in-flight request).
- `prompts/` — implementer / reviewer / process-fixer system prompts.

## Fixture schema (inputs only — see prompts/implementer.md for the full spec)

```jsonc
{
  "name": "take.push.backfill-on-remove",
  "sourceKind": "memory",
  "tables": {"issue": {"columns": {"id": "string", "n": "number"},
              "primaryKey": ["id"], "rows": [{"id": "i1", "n": 1}]}},
  "ast": { /* zero-protocol AST: table, orderBy [["col","asc"]], where, limit,
              start {row, exclusive}, related [{correlation, subquery}] */ },
  "format": {"singular": false, "relationships": {}},
  "enableNotExists": false,
  "pushes": [{"type": "add", "table": "issue", "row": {"id": "i9", "n": 9}}]
}
```

Invariant: agents author `.input.json` ONLY. `.expected.json` comes from the
oracle; the orchestrator regenerates it from every new input (gate B), so a
hand-written expectation can never survive.

## Executing TS from mono-v1.7

Node 24's `--experimental-strip-types` runs the zql TS sources directly
(no build step). mono-v1.7 has `pnpm install` already run; the oracle imports
`packages/zql/src/builder/builder.ts`, `ivm/memory-source.ts`, `ivm/catch.ts`
and `test-builder-delegate.ts` — the engine's own test surface.

## Known state at handoff (2026-07-17)

- 448 Rust tests green (`cargo test -- --test-threads=1`).
- 3 pending divergences in `fixtures/regressions/` (queued first):
  Take push path is a STUB (take.limit-desc + seed-2), and a filter-push
  remove-drop class (seed-5). See needs-human.md and SETUP-REPORT.md.
- Loop-agent model: xyne → litellm → glm-latest. xyne write/edit tools CRASH on
  .rs files — all prompts mandate bash-heredoc writes (section 0 of the mission).
