#!/usr/bin/env python3
r"""
M2 — call-guard parity (parity/ layer; the static half of the M0 matrix).

Every divergence found on 2026-09-02 had the same shape: TS gates a call on a
lifecycle flag (`if (this.#pipelinesSynced) { ... }`) and the rust port makes the
call UNCONDITIONALLY. Symbols match, bodies match, the ledger is green — and the
behavior is wrong. L1/L2 cannot see it because nothing is missing; only the
*guard* is.

This check extracts those guards from the TS source MECHANICALLY (so a newly
added TS guard shows up on its own) and, for each, asserts the rust twin's call
sites sit inside an equivalent guard.

  TS side  : scan for `if (<cond mentioning this.#...>) { ... }` and collect the
             `this.#method(` calls inside the block.
  Mapping  : MAP below binds (ts_flag_expr, ts_callee) -> rust (flag, callee,
             file) with a status.
  Rust side: for status `enforced`, every `self.<callee>(` call site must be
             lexically inside an `if` whose condition mentions `self.<flag>`.

Statuses:
  enforced      — checked as above; a dropped guard FAILS the build.
  orchestration — rust's call graph legitimately differs (note REQUIRED, and it
                  must cite where the equivalent gate lives).
  n/a           — the TS guard has no rust twin at all (note REQUIRED).

An extracted TS guard with no MAP entry FAILS: that is the ratchet. When TS adds
a guard, someone must classify it here.
"""
from __future__ import annotations
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TS_VIEW_SYNCER = "packages/zero-cache/src/services/view-syncer/view-syncer.ts"
RS_VIEW_SYNCER = "packages/rust-syncer/src/services/view_syncer/view_syncer.rs"

# (ts_cond_substring, ts_callee) -> (status, rust_flag, rust_caller, rust_callee, rust_file, note)
# `rust_caller` scopes the check to ONE rust function, because TS gates a call inside a
# specific method and deliberately leaves the same callee ungated elsewhere (e.g.
# `#validateConnection` is gated at view-syncer.ts:1012 but not at :838 / :942).
# `ts_cond_substring` is matched against the extracted condition text, so it is
# stable against reformatting of the rest of the expression.
MAP = {
    ("this.#pipelinesSynced", "syncQueryPipelineSet", "removeExpiredQueries"): (
        "enforced", "pipelines_synced", "on_expiry_tick", "remove_expired_queries", RS_VIEW_SYNCER,
        "TS #removeExpiredQueries (view-syncer.ts:643) and #updateCVRConfig (:1163) both gate the "
        "pipeline sync on #pipelinesSynced; rust's expiry twin is on_expiry_tick -> "
        "remove_expired_queries, gated 2026-09-02",
    ),
    ("!(await this.#validateConnection(connCtx))", "validateConnection", "initConnection"): (
        "result-checked", None, "handle_desired_queries", "validate_connection", RS_VIEW_SYNCER,
        "TS validates BEFORE any data is sent and RETURNS on failure "
        "(view-syncer.ts:936-944); a cached transform cannot stand in for it. rust "
        "`handle_desired_queries` must branch on `validate_connection`'s result, not "
        "discard it. Pinned by `init_connection_validates_before_serving_any_data`.",
    ),
    ("await this.#checkForShutdownConditionsInLock()", "checkForShutdownConditionsInLock",
     "runInLockWithCVR"): (
        "orchestration", None, None, None, None,
        "TS re-checks shutdown conditions inside every locked op; rust's idle shutdown is "
        "driven by the CG executor's keepalive timer (`idle_shutdown_due`), which is "
        "evaluated on the same serial task between messages. Pinned by "
        "`idle_shutdown_requires_both_keepalive_expiry_and_zero_admissions`.",
    ),
    ("this.#pipelinesSynced", "syncQueryPipelineSet", "updateCVRConfig"): (
        "orchestration", "pipelines_synced", None, "sync_query_pipeline_set", RS_VIEW_SYNCER,
        "TS `#updateCVRConfig` syncs the pipeline set only once pipelines are synced "
        "(view-syncer.ts:1163) because BEFORE that the run loop's init block owns the first "
        "hydrate (view-syncer.ts:592-606). Rust has no separate run-loop hydrate: "
        "`config_and_hydrate_with_profile` IS the first hydrate, so it must call "
        "`sync_query_pipeline_set` unconditionally — the same work, from the one place rust "
        "does it. What TS's flag protects (not re-hydrating everything per config change) is "
        "preserved instead by the `pipelines_synced` gate on `hydrate_unchanged_queries` and "
        "by CustomQueryTransformMode::Missing. Pinned by "
        "`hydrate_unchanged_runs_once_per_pipeline_init`.",
    ),
    ("!this.#pipelinesSynced", "validateConnection", "updateAuth"): (
        "enforced", "pipelines_synced", "handle_update_auth", "validate_connection", RS_VIEW_SYNCER,
        "TS updateAuth validates immediately ONLY when pipelines are unsynced "
        "(view-syncer.ts:1011); gated 2026-09-02",
    ),
    ("this.#pipelinesSynced", "advancePipelines", "run"): (
        "orchestration", "pipelines_synced", None, "advance_and_sync", RS_VIEW_SYNCER,
        "TS's run loop only advances once pipelines are synced (view-syncer.ts:568). Rust's "
        "advance path is driven by the replication subscription and cannot start before "
        "`pipelines().init()`, which is the same precondition expressed structurally; "
        "`advance_gate.rs` is the tested gate.",
    ),
    ("!this.#cvr", "getTTLClock", "runInLockWithCVR"): (
        "orchestration", "cvr", None, "get_ttl_clock", RS_VIEW_SYNCER,
        "TS lazily creates the CVR when absent; rust's `ensure_cvr` performs the same "
        "load-or-create before any caller reaches the ttlClock.",
    ),
    ("!this.#cvr", "runPriorityOp", "runInLockWithCVR"): (
        "n/a", None, None, None, None,
        "`#runPriorityOp` is TS's #lock priority queue — rust's serial CG task is the "
        "registered invention I-1 (parity/INVENTIONS.md); there is no rust twin to gate.",
    ),
    ("this.#drainCoordinator.shouldDrain()", "totalHydrationTimeMs", "run"): (
        "orchestration", "drain_coordinator", None, "total_hydration_time_ms", RS_VIEW_SYNCER,
        "Drain scheduling lives in the syncer worker's drain path, not the CG service; "
        "the hydration-time read is inside that same branch there.",
    ),
    ("Date.now() <= this.#keepAliveUntil", "scheduleShutdown", "checkForShutdownConditionsInLock"): (
        "orchestration", "keep_alive_until", None, "schedule_shutdown", RS_VIEW_SYNCER,
        "rust's idle shutdown compares the keepalive deadline inside `schedule_shutdown` "
        "itself (CG_KEEPALIVE_MS); pinned by "
        "`idle_shutdown_requires_both_keepalive_expiry_and_zero_admissions`.",
    ),
    ("this.#clients.size === 0", "scheduleShutdown", "deleteClientDueToDisconnect"): (
        "orchestration", "registered_ws", None, "schedule_shutdown", RS_VIEW_SYNCER,
        "same zero-clients precondition, checked inside rust's shutdown scheduling; pinned by "
        "`idle_shutdown_requires_both_keepalive_expiry_and_zero_admissions`.",
    ),
    ("this.#clients.size === 0", "stopExpireTimer", "deleteClientDueToDisconnect"): (
        "orchestration", "registered_ws", None, "stop_expire_timer", RS_VIEW_SYNCER,
        "timer teardown runs from rust's CG teardown path, which only runs at zero clients.",
    ),
    ("this.#clients.size === 0", "updateTTLClockInCVRWithoutLock", "deleteClientDueToDisconnect"): (
        "orchestration", "registered_ws", None, "update_ttl_clock_in_cvr_without_lock", RS_VIEW_SYNCER,
        "final ttlClock persist happens in the same zero-clients teardown path.",
    ),
    ("cmpVersions(cvr.version, this.#cvr.version) < 0", "getClients", "updateCVRConfig"): (
        "orchestration", "expected_current_version", None, "config_poke_targets", RS_VIEW_SYNCER,
        "rust `handle_config_update` pokes via `config_poke_targets(clients, "
        "&expected_current_version)`, which applies the same version comparison per client "
        "instead of around the whole call.",
    ),
}


def read(rel: str) -> list[str]:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as fh:
        return fh.read().split("\n")


MEMBER_RE = re.compile(
    r"^  (?:readonly )?(?:async )?#?(\w+)\s*(?:=|\()"
)


def enclosing_member(lines: list[str], idx: int) -> str:
    """The class member a line sits in — `#removeExpiredQueries`, `updateAuth`, ...

    Two different responsibilities can share a guard SHAPE (`#pipelinesSynced` ->
    `#syncQueryPipelineSet` appears at view-syncer.ts:643 AND :1163). Keying on
    the shape alone silently classifies one and skips the other.
    """
    for k in range(idx, -1, -1):
        m = MEMBER_RE.match(lines[k])
        if m:
            return m.group(1)
    return "?"


def extract_ts_guards(rel: str) -> list[tuple[int, str, str, list[str]]]:
    """`if (<cond mentioning this.#...>) { ... }` -> the `this.#method(` calls inside."""
    lines = read(rel)
    out: list[tuple[int, str, str, list[str]]] = []
    i = 0
    while i < len(lines):
        m = re.match(r"\s*(?:\} else )?if \((.*)\)\s*\{\s*$", lines[i])
        if m and "this.#" in m.group(1):
            cond = m.group(1).strip()
            depth = 0
            j = i
            body: list[str] = []
            while j < len(lines):
                depth += lines[j].count("{") - lines[j].count("}")
                if j > i:
                    body.append(lines[j])
                if depth <= 0 and j > i:
                    break
                j += 1
            # Calls in the CONDITION count too: TS writes guards both ways —
            # `if (flag) { this.#f() }` AND `if (!(await this.#f())) return;`.
            # Reading only the body missed `#validateConnection` entirely, which
            # is one of the calls this check exists to police.
            calls = sorted(set(re.findall(r"this\.#(\w+)\(", cond + "\n" + "\n".join(body))))
            if calls:
                out.append((i + 1, cond, enclosing_member(lines, i), calls))
            i = j
        i += 1
    return out


def rust_calls_are_guarded(
    rust_file: str, caller: str, callee: str, flag: str
) -> tuple[bool, list[int], bool]:
    """Inside rust fn `caller`, every `self.<callee>(` must sit in an `if` mentioning `self.<flag>`.

    Returns (ok, unguarded_lines, caller_found). Walks the caller's body tracking
    brace depth and remembers, per depth level, whether the block that opened it
    tested the flag — so a call is guarded iff any enclosing block did.
    """
    lines = read(rust_file)
    start = None
    fn_re = re.compile(r"^\s*(pub(\(\w+\))? )?(async )?fn %s\s*[(<]" % re.escape(caller))
    for idx, line in enumerate(lines):
        if fn_re.match(line):
            start = idx
            break
    if start is None:
        return (False, [], False)

    call_re = re.compile(r"\bself\.%s\s*\(" % re.escape(callee))
    flag_re = re.compile(r"\bself\.%s\b" % re.escape(flag))
    unguarded: list[int] = []
    calls_found = 0
    pending_self = False
    depth = 0
    entered = False
    # guard_at_depth[d] is True when the block opened at depth d tested the flag.
    guard_at_depth: dict[int, bool] = {}
    # Whether the block opened at depth d exits the function (`return`/`continue`).
    block_returns: dict[int, bool] = {}
    early_return_guard = False
    # A condition can span lines; keep a small rolling window of recent text.
    window: list[str] = []
    for idx in range(start, len(lines)):
        line = lines[idx]
        code = line.split("//", 1)[0]
        window.append(code)
        if len(window) > 4:
            window.pop(0)
        opens = code.count("{")
        closes = code.count("}")
        # A call can be split as `self\n    .callee(` — rustfmt does this whenever
        # the receiver expression is long. Missing that spelling silently found
        # ZERO call sites and reported a dropped guard as OK.
        is_call = bool(call_re.search(code)) or (
            pending_self and re.match(r"\s*\.%s\s*\(" % re.escape(callee), code)
        )
        if is_call:
            calls_found += 1
            if not early_return_guard and not any(
                guard_at_depth.get(d) for d in range(0, depth + 1)
            ):
                unguarded.append(idx + 1)
        stripped = code.strip()
        if stripped:
            pending_self = stripped.endswith("self") or (
                pending_self and stripped.startswith(".") and not stripped.endswith(";")
            )
        for _ in range(opens):
            depth += 1
            recent = "\n".join(window)
            guard_at_depth[depth] = bool(
                re.search(r"\bif\b", recent) and flag_re.search(recent)
            )
            block_returns[depth] = False
            entered = True
        if re.search(r"\b(return|continue)\b", code):
            for d in range(1, depth + 1):
                block_returns[d] = True
        for _ in range(closes):
            # A rust GUARD CLAUSE — `if !flag { ...; return; }` — is the same
            # branch as TS's enclosing `if (flag) { ... }`, just inverted, and it
            # is how rust normally spells it. Once such a block has closed,
            # everything after it in the function runs only when the flag holds.
            if guard_at_depth.get(depth) and block_returns.get(depth):
                early_return_guard = True
            guard_at_depth.pop(depth, None)
            block_returns.pop(depth, None)
            depth -= 1
        if entered and depth <= 0:
            break
    # No call site at all means the MAP entry is stale (renamed callee, moved
    # call). Reporting that as "guarded" is exactly the false-negative that let a
    # multi-line `self\n.callee(` slip through, so treat it as a failure.
    if calls_found == 0:
        return (False, [], False)
    return (not unguarded, unguarded, True)


def rust_call_result_is_checked(rust_file: str, caller: str, callee: str) -> tuple[bool, str]:
    """Inside rust fn `caller`, is `self.<callee>(..)`'s result BRANCHED ON?

    TS's `if (!(await this.#f(x))) return;` is a guard whose whole content is the
    result test. The rust twin satisfies it only by matching / testing the value —
    a bare `self.f(..).await;` statement or `let _ = self.f(..)` ports the CALL
    while dropping the GUARD, which reads as done and is not.
    """
    lines = read(rust_file)
    fn_re = re.compile(r"^\s*(pub(\(\w+\))? )?(async )?fn %s\s*[(<]" % re.escape(caller))
    start = next((i for i, l in enumerate(lines) if fn_re.match(l)), None)
    if start is None:
        return (False, f"has no fn `{caller}`")
    call_re = re.compile(r"\bself\.%s\s*\(" % re.escape(callee))
    depth, entered, seen = 0, False, False
    for idx in range(start, len(lines)):
        code = lines[idx].split("//", 1)[0]
        if call_re.search(code):
            seen = True
            # the statement usually begins on this line or the one above
            ctx = "\n".join(lines[max(start, idx - 2): idx + 1])
            if re.search(r"let\s+_\s*=", ctx):
                return (False, f"DISCARDS the result (`let _ = self.{callee}(..)`)")
            if not re.search(r"\b(match|if|while|let|return)\b|\?", ctx):
                return (False, f"calls `self.{callee}(..)` as a bare statement "
                               f"without testing the result")
        depth += code.count("{") - code.count("}")
        if code.count("{"):
            entered = True
        if entered and depth <= 0:
            break
    if not seen:
        return (False, f"never calls `self.{callee}(..)`")
    return (True, "")


def main() -> int:
    rc = 0
    guards = extract_ts_guards(TS_VIEW_SYNCER)
    print(f"== M2 call-guard parity ({len(guards)} guarded TS call sites in view-syncer.ts) ==")
    seen: set[tuple[str, str]] = set()
    for ln, cond, member, calls in guards:
        for callee in calls:
            key = None
            for (cond_sub, ts_callee, ts_member) in MAP:
                if ts_callee == callee and cond_sub in cond and ts_member == member:
                    key = (cond_sub, ts_callee, ts_member)
                    break
            if key is None:
                print(f"  [UNMAPPED] view-syncer.ts:{ln}  in {member}: "
                      f"if ({cond}) -> #{callee}()")
                print(f"             A TS guard with no rust classification. Add a MAP entry "
                      f'keyed ("{cond}", "{callee}", "{member}") '
                      f"— enforced / orchestration / n/a — after checking the rust twin.")
                rc = 1
                continue
            if key in seen:
                continue
            seen.add(key)
            status, flag, rs_caller, rs_callee, rs_file, note = MAP[key]
            if status == "enforced":
                ok, bad, found = rust_calls_are_guarded(rs_file, rs_caller, rs_callee, flag)
                if not found:
                    rc = 1
                    print(f"  [FAIL] no `self.{rs_callee}(` call site inside rust `{rs_caller}` in "
                          f"{rs_file} — the MAP entry for #{callee} is stale (renamed? moved?)")
                elif ok:
                    print(f"  [OK ] #{callee} gated on {cond} -> rust {rs_caller}() gates "
                          f"{rs_callee}() on self.{flag}")
                else:
                    rc = 1
                    print(f"  [FAIL] #{callee} is gated on `{cond}` in TS (view-syncer.ts:{ln}), but "
                          f"rust `self.{rs_callee}(` is UNGUARDED inside `{rs_caller}` at "
                          f"{rs_file}:{bad}")
                    print(f"         expected an enclosing `if` testing `self.{flag}` — {note}")
            elif status == "result-checked":
                ok, why = rust_call_result_is_checked(rs_file, rs_caller, rs_callee)
                if ok:
                    print(f"  [OK ] #{callee} result-checked in TS ({cond}) -> rust "
                          f"{rs_caller}() branches on {rs_callee}()")
                else:
                    rc = 1
                    print(f"  [FAIL] TS branches on #{callee} (view-syncer.ts:{ln}) but rust "
                          f"`{rs_caller}` {why}")
                    print(f"         {note}")
            else:
                print(f"  [{status.upper()}] #{callee} ({cond}) — {note}")
                if not note:
                    rc = 1
    print("\nM2 call-guard parity:", "PASS" if rc == 0 else "FAIL")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
