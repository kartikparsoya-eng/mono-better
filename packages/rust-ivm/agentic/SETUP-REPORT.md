# SETUP-REPORT.md — rust-ivm agentic verification loop

(Reconstructed by recovery supervisor; the xyne setup agent never wrote its
per-step checkpoints here despite the mission requiring them.)

## RECOVERY — 2026-07-17 14:30 IST (supervisor recovery attempt 2, Claude Code)

**Why the pipeline stopped:** the xyne setup agent (GLM via litellm) crashed at
14:18 IST while invoking the xyne `edit` tool on `rust-ivm/src/replay.rs` — the
exact KNOWN RUNTIME BUG that mission section 0 forbids (write/edit tools on .rs
files crash the whole agent process). The final transcript event
(~/.xyne/agent/sessions/.../2026-07-17T08-12-34-544Z_*.jsonl) is that edit
toolCall with no result after it. The edit never applied: `cargo check` failed
with the exact two E0308 errors (replay.rs:213/218) the edit was meant to fix.
This was at least the 4th crash today (4 transcripts 12:22–13:42 IST, ~30 min
apart) — the GLM agent repeatedly ignored the section-0 chunked-bash-write rule.

**Recovery attempt 1 (13:41 IST) was a no-op:** its `claude -p` died instantly
to "You've hit your session limit · resets 2:20pm" (supervisor-recovery-1.log).
It burned a recovery slot without doing anything; `.recoveries` reset 2→1 to
reflect one real recovery in flight (this one).

**State found on disk (verified):**
- Step 1.0 done: rust-ivm git repo, 1 baseline commit fb46c7b; mono-v1.7
  worktree on branch rust-ivm-v1.7.0.
- Step 2 done: agentic/PORTING.md (15.7KB).
- Step 3 done: oracle/ts-runner.mjs + 2 hand fixtures with .expected.json
  (take.limit-desc, join.related-owner-push).
- Step 4 done: oracle/diff.mjs.
- Step 5 IN PROGRESS at crash: src/replay.rs (469 lines, wired in lib.rs),
  no replay bin, no tests/fixture_replay_test.rs yet.
- Steps 6–10 not started: no fuzz/, prompts/, queue/, orchestrate.mjs,
  status.mjs, README.md, needs-human.md.
- agentic/keys.json present (3 keys). agentic/scout-ast-builder-report.md is a
  scout-subagent artifact (AST/builder notes), kept.

**Fix applied:** patched replay.rs:211-225 with the agent's intended change
(borrow-vs-owned Map on json_to_row; empty-row fallback). cargo check now
0 errors.

**Decision:** per recovery instruction 3, completing remaining steps 5–10
directly in this session (Claude) rather than relaunching the xyne setup agent,
which has crashed 4× on the same bug class. Checkpoints below.

2026-07-17 14:55 — step 5 done (recovery): replay.rs compile-fixed; replay bin +
tests/fixture_replay_test.rs added; found+fixed 2 bugs (in-memory source ignored
connection sort — real engine gap vs TS memory-source per-sort index; replayer
read alias from wrong AST level); found 1 REAL DIVERGENCE (Take push path is a
stub — see needs-human.md, moved to fixtures/regressions/, divergence task will
lead the queue). Validation: join.related-owner-push + select.orderby-limit both
MATCH the TS oracle. Full suite: 448 passed / 0 failed (--test-threads=1).

2026-07-17 15:20 — steps 6-8.5 done (recovery): fuzz/gen.mjs + fuzz-loop.mjs
(smoke: 319 seeds/2min, 0 invalid, 2 minimized findings auto-queued — seed-2 =
known Take-push class; seed-5 = NEW second class: filter push path drops a
matching remove while hydrate is correct). prompts/{implementer,reviewer,
process-fixer}.md written. build-queue.mjs → 64 tasks (3 divergence first,
then memory-source/take/join/... per mission order; >25-case files chunked
with exact case lists). keys.json: 3 keys; PONG validated per-key incl. 3
simultaneous distinct-key calls. NOTE: no `timeout` binary on this Mac —
orchestrator implements timeouts in Node.
