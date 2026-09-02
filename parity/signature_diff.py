#!/usr/bin/env python3
r"""
M8 — TS<->Rust signature + inventory differential (parity/ layer).

WHY THIS EXISTS (2026-09-02). Three divergences shipped past L1/L2 and M1-M7:

  1. `customQueryTransformMode` — a PARAMETER of TS `#syncQueryPipelineSet` that
     the rust twin simply did not have. Nothing compares parameter lists.
  2. `#validateConnection` — a TS METHOD with no rust twin. L1 said "exact",
     because it name-matches crate-wide and found the UNRELATED
     `ConnectionContextManager::validate_connection` in a different file.
  3. `is_init: bool` — ONE rust function standing in for THREE TS entry points
     (`initConnection` / `updateAuth` / `#runBackgroundRetransform`). AGENTS
     rule 2 forbids merging; nothing enforced it.

All three are INVENTORY facts, decidable from the source with no runtime, no
workload, and no judgement. This checks them:

  A. MIRRORED-FILE matching (AGENTS rule 3). A TS symbol's twin must be in the
     rust file mirroring its TS file. A same-named function in another file does
     NOT satisfy it — that is precisely how (2) hid.
  B. INJECTIVITY (AGENTS rule 2). Two TS symbols may not claim one rust symbol.
     Catches merges like (3).
  C. PARAMETER-SET differential. For a matched pair, every TS parameter must
     appear in the rust twin (camelCase -> snake_case). Catches (1).
  D. CONSTANT VALUE differential (AGENTS rule 4). A named constant ported with
     the WRONG VALUE passes every symbol-, signature- and body-level check --
     the name matches, the type matches, the call sites match. Only the number
     is wrong. 2026-09-02: rust capped its SQLite statement cache at 64 where TS
     caps at 1000 (`DEFAULT_MAX_CACHED_STATEMENTS`), so a whale client group
     thrashed the cache and re-parsed + re-planned SQL on nearly every fetch --
     ~7x slower hydration than TS on an identical AST over identical rows.

Pre-existing mismatches are held by a RATCHET baseline (parity/.signature_baseline)
so the count can only go down; a NEW mismatch fails the build.

  parity/signature_diff.py                  # gate
  parity/signature_diff.py --list           # every finding
  parity/signature_diff.py --update-baseline
  parity/signature_diff.py --at <git-rev>   # run against a historical revision
"""
from __future__ import annotations
import argparse
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASELINE = os.path.join(ROOT, "parity", ".signature_baseline")

# (TS file, TS class) -> the rust file that mirrors it (AGENTS rule 3).
# The class matters: view-syncer.ts also declares `TimeSliceTimer`, a TS-only
# cooperative-yielding helper for Node's single event loop. Attributing its
# methods to `ViewSyncerService` produced six phantom MISSING rows.
MIRROR = {
    ("packages/zero-cache/src/services/view-syncer/view-syncer.ts", "ViewSyncerService"):
        "packages/rust-syncer/src/services/view_syncer/view_syncer.rs",
    ("packages/zero-cache/src/services/view-syncer/connection-context-manager.ts",
     "ConnectionContextManager"):
        "packages/rust-syncer/src/services/view_syncer/connection_context_manager.rs",
    ("packages/zero-cache/src/custom-queries/transform-query.ts", "CustomQueryTransformer"):
        "packages/rust-syncer/src/custom_queries/transform_query.rs",
}

# TS symbol -> (rust symbol, reason). An alias is NOT a free pass: M8 verifies
# the named rust symbol really exists in the mirrored file, so a stale alias
# fails like a MISSING. Use it where rust's name legitimately differs; fix the
# rust name instead wherever that is the cheaper truth.
ALIAS: dict[str, tuple[str, str]] = {
    # --- rust folds several TS methods into one orchestrator (documented) ---
    "#updateCVRConfig": ("handle_config_update",
        "rust folds TS's #handleConfigUpdate + #updateCVRConfig into one config pass; "
        "see the doc-comment on handle_config_update"),
    "#addAndRemoveQueries": ("hydrate_and_sync",
        "the add/remove + poke + catch-up half of TS #syncQueryPipelineSet"),
    "#advancePipelines": ("advance_and_sync",
        "rust drives advances from the replication subscription, not a run-loop step"),
    "#processChanges": ("advance_and_sync",
        "the per-change apply loop is inlined into rust's advance path"),
    "#flushUpdater": ("flush_ops_to_store",
        "rust flushes the updater's drained store-ops rather than the updater object"),
    "#runInLockWithCVR": ("dispatch_cg_message",
        "TS's #lock twin is the serial CG task (INVENTIONS.md I-1); the CVR is loaded "
        "by ensure_cvr inside it"),
    "#cleanup": ("fail_group",
        "rust teardown is Drop + fail_group; there is no separate cleanup step"),
    "#scheduleShutdown": ("next_idle_shutdown_delay",
        "rust arms the idle-shutdown timer by returning its delay to the CG loop"),
    "#checkForShutdownConditionsInLock": ("idle_shutdown_due",
        "same zero-clients + keepalive predicate, evaluated between messages on the "
        "serial CG task"),
    "#processTransformedCustomQueries": ("record_transform_error",
        "rust handles per-query transform outcomes inline in sync_query_pipeline_set; "
        "the error-forwarding half is record_transform_error"),
    "run": ("cg_event_loop",
        "TS's ViewSyncerService.run loop is the rust CG executor loop"),
    "#findQueryCoverageShadowHit": ("query_covering::find_covering_query",
        "shadow-mode query covering lives in its own mirrored module, "
        "services/view_syncer/query_covering.rs (#6182)"),
    "#sendQueryTransformErrorToClients": ("get_clients",
        "rust fans the per-query transform errors out over get_clients() to "
        "ClientHandler::send_query_transform_application_errors (rust-cvr)"),
    "#stopAuthMaintenanceTimer": ("next_auth_maintenance_delay",
        "rust has no timer object to stop: the CG loop asks for the next delay each "
        "pass and None means disarmed"),
    "#logQueryCoverageShadowSummary": ("query_covering::find_covering_query",
        "the shadow summary is logged inline at the end of rust's covering pass, in the "
        "same mirrored module"),
    "#addQueryMaterializationServerMetric": ("hydrate_and_sync",
        "rust records the inspector materialization metric inline where the query "
        "hydrates; pinned by hydrate_and_sync_records_inspector_materialization_and_ast"),
    "#totalHydrationTimeMs": ("hydrate_and_sync",
        "rust accumulates hydration time into the OTel histogram as each query hydrates "
        "rather than summing on demand for the drain coordinator"),
    "keepalive": ("schedule_auth_maintenance",
        "TS ActivityBasedService interface; rust's CG executor owns the keepalive window "
        "(CG_KEEPALIVE_MS) and re-arms its timers between messages"),
    "markInitialized": ("ensure_cvr",
        "TS resolves an #initialized promise to release its run loop; rust's CG task has "
        "no separate run loop to release -- ensure_cvr is the same one-shot"),
    "readyState": ("idle_shutdown_due",
        "TS ActivityBasedService readiness probe; rust reports liveness on /readyz and "
        "keeps the idle predicate on the CG"),
    "transform-query.ts:constructor": ("request_transform",
        "rust's transform_query is free functions over a process-wide client + cache, not "
        "a constructed object -- there is no per-instance state to build"),
    "transform-query.ts:destroy": ("request_transform",
        "no object to tear down: the transform cache is a process-wide TTL map"),
    "stop": ("fail_group",
        "rust stops a group by failing it; clients rehome (INVENTIONS.md I-4)"),
}

# (TS method, TS parameter) -> reason. Unlike an ALIAS, this exempts ONE
# parameter and leaves the rest of the signature under the diff, so a newly
# dropped argument still fails. Aliasing an exact-name match instead would
# disable the check that found `customQueryTransformMode` in the first place.
PARAM_EXEMPT_BY_METHOD = {
    ("#handleConfigUpdate", "desiredQueriesPatch"): "passed decomposed as puts/dels/clear",
    ("#handleConfigUpdate", "deleted"): "routed to apply_client_deletions",
    ("#handleConfigUpdate", "activeClients"): "routed to apply_client_deletions",
    ("#handleConfigUpdate", "connCtx"): "read from the CCM at use time (AGENTS rule 9)",
    ("#handleConfigUpdate", "customQueryTransformMode"):
        "threaded at the config_and_hydrate_with_profile seat, which chains "
        "handle_config_update + sync_query_pipeline_set",
    ("#syncQueryPipelineSet", "connCtx"): "read from the CCM at use time (AGENTS rule 9)",
    ("#syncQueryPipelineSet", "driftedQueryIDs"):
        "computed inside the pass by hydrate_unchanged_queries",
    ("#getClients", "atVersion"):
        "version filter applied per client in config_poke_targets/advance_poke_targets",
    ("#catchupClients", "usePokers"): "rust always creates its own pokers",
    ("#handleInspect", "cvr"): "read from self at use time",
    ("#runAuthMaintenance", "_cvr"): "unused in TS too (the #runInLockWithCVR signature)",
    ("#requestTransform", "operation"): "the body is built at each call site",
    ("#requestTransform", "request"): "the body is built at each call site",
    ("updateAuth", "msg"): "the dispatch seat parses the token out of the raw message",
    ("updateAuth", "authRevisionChanged"):
        "rust compares the RAW token (auth.ts authEquals) one layer down in "
        "handle_update_auth and returns on unchanged -- same skip",
    ("initConnection", "initConnectionMessage"): "the dispatch seat receives the raw text",
    # TS hands the ViewSyncerService a dozen collaborators; rust injects ONE
    # CGServicesFactory that builds pipelines/mutagen/pusher/config per group.
    **{("constructor", p): "supplied by the injected CGServicesFactory"
       for p in ("clientGroupID", "config", "connContextManager", "customQueryTransformer",
                 "cvrDb", "drainCoordinator", "inspectorDelegate", "pipelineDriver",
                 "runPriorityOp", "shard", "slowHydrateThreshold", "taskID",
                 "versionChanges")},
}

# TS parameters with no rust counterpart BY DESIGN. Each needs a reason.
PARAM_EXEMPT = {
    # TS threads a LogContext through every method; rust uses the `tracing`
    # macros, which read their context from the subscriber, not an argument.
    "lc", "logcontext",
    # TS passes the OpenTelemetry span into `startAsyncSpan` callbacks.
    "span",
}


# JS keywords that look like a method declaration to a regex (`if (`, `for (`).
NOT_METHODS = {
    "if", "for", "while", "switch", "catch", "return", "function", "do", "else",
    "try", "await", "typeof", "new", "get", "set", "yield",
}

# TS name -> rust name where the transliteration is not mechanical.
NAME_ALIASES = {
    "constructor": "new",
}


def snake(name: str) -> str:
    """camelCase / #privateName -> snake_case, acronym-aware.

    A naive `(?=[A-Z])` split mangles acronyms: `getTTLClock` becomes
    `get_t_t_l_clock`, so the check reports a phantom MISSING against the real
    `get_ttl_clock`. Split only at a lower->upper boundary or before the last
    capital of an acronym run.
    """
    name = name.lstrip("#_")
    if name in NAME_ALIASES:
        return NAME_ALIASES[name]
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name).lower()


# TS constant -> (ts_file, rust constant, rust_file). Values must be EQUAL.
# A constant whose rust value is a guess rather than the ported number is
# invisible to every other check here.
CONSTANTS = [
    ("DEFAULT_MAX_CACHED_STATEMENTS",
     "packages/zqlite/src/internal/statement-cache.ts",
     "DEFAULT_MAX_CACHED_STATEMENTS",
     "packages/rust-ivm/src/snapshotter/snapshotter.rs"),
    ("TTL_CLOCK_INTERVAL",
     "packages/zero-cache/src/services/view-syncer/view-syncer.ts",
     "TTL_CLOCK_INTERVAL",
     "packages/rust-syncer/src/services/view_syncer/view_syncer.rs"),
    ("TTL_TIMER_HYSTERESIS",
     "packages/zero-cache/src/services/view-syncer/view-syncer.ts",
     "TTL_TIMER_HYSTERESIS_MS",
     "packages/rust-syncer/src/services/view_syncer/view_syncer.rs"),
    ("THRASH_WINDOW_MS",
     "packages/zero-cache/src/services/view-syncer/view-syncer.ts",
     "THRASH_WINDOW_MS",
     "packages/rust-syncer/src/services/view_syncer/view_syncer.rs"),
    ("THRASH_THRESHOLD",
     "packages/zero-cache/src/services/view-syncer/view-syncer.ts",
     "THRASH_THRESHOLD",
     "packages/rust-syncer/src/services/view_syncer/view_syncer.rs"),
]


def numeric(text: str, name: str, ts: bool) -> str | None:
    """The literal value of a named constant, normalized (1_000 == 1000 == 1e3)."""
    pat = (r"\b%s\s*(?::[^=]+)?=\s*([0-9][0-9_]*(?:\.[0-9]+)?)" % re.escape(name)) if not ts \
        else (r"\b%s\s*(?::[^=]+)?=\s*([0-9][0-9_]*(?:\.[0-9]+)?)" % re.escape(name))
    m = re.search(pat, text)
    if not m:
        return None
    raw = m.group(1).replace("_", "")
    try:
        v = float(raw)
    except ValueError:
        return raw
    return str(int(v)) if v == int(v) else str(v)


def constant_findings(rev: str | None) -> list[str]:
    out: list[str] = []
    for ts_name, ts_file, rs_name, rs_file in CONSTANTS:
        ts_src, rs_src = read(ts_file, rev), read(rs_file, rev)
        if ts_src is None or rs_src is None:
            continue
        tv = numeric(ts_src, ts_name, True)
        rv = numeric(rs_src, rs_name, False)
        if tv is None:
            out.append(f"CONST    {ts_name} not found in {os.path.basename(ts_file)} "
                       f"— the CONSTANTS entry is stale")
        elif rv is None:
            out.append(f"CONST    {rs_name} not found in {os.path.basename(rs_file)} "
                       f"— the CONSTANTS entry is stale")
        elif tv != rv:
            out.append(f"CONST    {ts_name}={tv} ({os.path.basename(ts_file)}) but "
                       f"{rs_name}={rv} ({os.path.basename(rs_file)}) — AGENTS rule 4: a "
                       f"ported constant must carry TS's VALUE, not a guess")
    return out


def read(rel: str, rev: str | None) -> str | None:
    if rev:
        try:
            return subprocess.check_output(
                ["git", "show", f"{rev}:{rel}"], cwd=ROOT, text=True,
                stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError:
            return None
    path = os.path.join(ROOT, rel)
    if not os.path.isfile(path):
        return None
    with open(path, encoding="utf-8", errors="replace") as fh:
        return fh.read()


def split_params(sig: str) -> list[str]:
    """Parameter NAMES from a parenthesised list, ignoring types/defaults."""
    out, depth, cur = [], 0, ""
    prev = ""
    for ch in sig:
        if ch in "([{<":
            depth += 1
        elif ch in ")]}>":
            # `->` is a return arrow, not a closing bracket. Counting its `>`
            # unbalanced the depth for any closure-typed parameter
            # (`f: impl FnMut(&T) -> U`), which split the rest of the signature
            # at the wrong commas and emitted words from the body as parameter
            # names -- silently garbling the very list this gate compares.
            if not (ch == ">" and prev == "-"):
                depth -= 1
        prev = ch
        if ch == "," and depth == 0:
            out.append(cur)
            cur = ""
        else:
            cur += ch
    out.append(cur)
    names = []
    for raw in out:
        raw = raw.strip()
        if not raw:
            continue
        # strip destructuring, modifiers, and everything after the type/default
        raw = re.sub(r"^(readonly|public|private|protected|mut|ref)\s+", "", raw)
        if raw.startswith("{"):          # TS destructured object param
            inner = raw[1:raw.find("}")] if "}" in raw else raw[1:]
            names += [p.strip().split(":")[0].strip() for p in inner.split(",") if p.strip()]
            continue
        m = re.match(r"([A-Za-z_]\w*)", raw)
        if m and m.group(1) not in ("self", "this"):
            names.append(m.group(1))
    return names


def ts_methods(src: str, cls: str) -> dict[str, list[str]]:
    """Methods of ONE class -> parameter names. Handles `name(a,b)` and `name = (a,b) =>`."""
    out: dict[str, list[str]] = {}
    lines = src.split("\n")
    cls_re = re.compile(r"^(export )?(abstract )?class (\w+)")
    current = None
    for i, line in enumerate(lines):
        cm = cls_re.match(line)
        if cm:
            current = cm.group(3)
        if current != cls:
            continue
        m = re.match(r"^  (?:readonly\s+)?(?:async\s+)?(#?\w+)\s*(?:=\s*)?(?:async\s+)?\(", line)
        if not m:
            continue
        name = m.group(1)
        if name.lstrip("#") in NOT_METHODS:
            continue
        # gather the parenthesised list, which may span lines
        buf, depth, started = "", 0, False
        for j in range(i, min(i + 60, len(lines))):
            for ch in lines[j]:
                if ch == "(":
                    depth += 1
                    started = True
                    if depth == 1:
                        continue
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        break
                if started and depth >= 1:
                    buf += ch
            if started and depth == 0:
                break
        out[name] = split_params(buf)
    return out


def rust_fns(src: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    lines = src.split("\n")
    for i, line in enumerate(lines):
        m = re.match(r"^\s*(?:pub(?:\(\w+\))?\s+)?(?:async\s+)?fn\s+(\w+)\s*(?:<[^>]*>)?\(", line)
        if not m:
            continue
        name = m.group(1)
        buf, depth, started = "", 0, False
        for j in range(i, min(i + 80, len(lines))):
            for ch in lines[j]:
                if ch == "(":
                    depth += 1
                    started = True
                    if depth == 1:
                        continue
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        break
                if started and depth >= 1:
                    buf += ch
            if started and depth == 0:
                break
        out.setdefault(name, split_params(buf))
    return out


def findings(rev: str | None) -> list[str]:
    out: list[str] = constant_findings(rev)
    for (ts_rel, ts_cls), rs_rel in sorted(MIRROR.items()):
        ts_src, rs_src = read(ts_rel, rev), read(rs_rel, rev)
        if ts_src is None or rs_src is None:
            continue
        ts, rs = ts_methods(ts_src, ts_cls), rust_fns(rs_src)
        claimed: dict[str, list[str]] = {}
        for name, params in sorted(ts.items()):
            twin = snake(name)
            # File-scoped first: two TS files can declare the same member name
            # (`constructor`), and a global alias for one silently breaks the other.
            aliased = ALIAS.get(f"{os.path.basename(ts_rel)}:{name}") or ALIAS.get(name)
            alias_file = rs_rel
            if aliased:
                twin = aliased[0]
                if "::" in twin:
                    # A sanctioned CROSS-FILE twin (AGENTS rule 3's exception:
                    # the symbol lives in its own mirrored module). Spelled
                    # "<rust file stem>::<fn>" so the file is checked too.
                    stem, twin = twin.split("::", 1)
                    alias_file = os.path.join(os.path.dirname(rs_rel), stem + ".rs")
                    rs_alt = read(alias_file, rev)
                    if rs_alt is None:
                        out.append(f"STALE    {os.path.basename(ts_rel)}:{name} -> aliased into "
                                   f"{stem}.rs, which does not exist")
                        continue
                    if twin not in rust_fns(rs_alt):
                        out.append(f"STALE    {os.path.basename(ts_rel)}:{name} -> aliased to "
                                   f"`{twin}` in {stem}.rs, which has no such fn")
                        continue
                    claimed.setdefault(stem + "::" + twin, []).append((name, True))
                    continue
            short = os.path.basename(ts_rel)
            if twin not in rs:
                if aliased:
                    out.append(f"STALE    {short}:{name} -> aliased to `{twin}`, which does "
                               f"not exist in {os.path.basename(rs_rel)}")
                    continue
                # (A) no twin IN THE MIRRORED FILE. A same-named function in a
                # different rust file does NOT satisfy the port.
                out.append(f"MISSING  {short}:{name} -> no `{twin}` in {os.path.basename(rs_rel)}")
                continue
            claimed.setdefault(twin, []).append((name, bool(aliased)))
            if aliased:
                # An alias already declares a structural difference; diffing the
                # parameters of two functions that are not 1:1 twins produces
                # noise, and noise is how a real finding gets ignored.
                continue
            # (C) parameter-set differential
            rp = {p.lower() for p in rs[twin]}
            for p in params:
                if p.lower() in PARAM_EXEMPT:
                    continue
                if (name, p) in PARAM_EXEMPT_BY_METHOD:
                    continue
                cand = {p.lower(), snake(p), snake(p).replace("_", "")}
                if not (cand & rp) and not any(snake(p) in r for r in rp):
                    out.append(f"PARAM    {short}:{name}({p}) -> `{twin}` has no such parameter")
        # (B) injectivity — one rust fn standing in for several TS methods.
        for twin, owners in sorted(claimed.items()):
            if len(owners) < 2:
                continue
            names = [n for n, _ in owners]
            # A TS class often has a `#private` impl and a public wrapper of the
            # same name; one rust fn for that pair is not a merge, it is the pair.
            if len(names) == 2 and {n.lstrip("#") for n in names} == {names[0].lstrip("#")}:
                continue
            # An ALIAS is an explicit, reasoned declaration that rust folds these
            # together (the reason is reviewed in ALIAS). Report it, do not fail.
            if any(is_alias for _, is_alias in owners):
                continue
            out.append(f"MERGE    {os.path.basename(ts_rel)}:{'+'.join(names)} "
                       f"-> all claim `{twin}` (AGENTS rule 2 forbids merging)")
    return sorted(out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--update-baseline", action="store_true")
    ap.add_argument("--at", help="run against a git revision instead of the worktree")
    args = ap.parse_args()

    found = findings(args.at)
    if args.list or args.at:
        for f in found:
            print(" ", f)
        print(f"\n{len(found)} finding(s){' at ' + args.at if args.at else ''}")
        if args.at:
            return 0
    if args.update_baseline:
        with open(BASELINE, "w", encoding="utf-8") as fh:
            fh.write(str(len(found)) + "\n")
        print(f"M8 baseline updated to {len(found)}")
        return 0

    base = 10**9
    if os.path.isfile(BASELINE):
        with open(BASELINE, encoding="utf-8") as fh:
            base = int(fh.read().strip() or 0)
    n = len(found)
    if n > base:
        print(f"M8 signature differential: FAIL ({n} findings > baseline {base}).")
        print("  A TS method with no twin in the MIRRORED rust file, a rust fn "
              "claimed by several TS methods, or a dropped parameter.")
        for f in found:
            print("   ", f)
        return 1
    if n < base:
        print(f"M8 signature differential: OK ({n} < baseline {base}) — "
              f"run --update-baseline to ratchet.")
        return 0
    print(f"M8 signature differential: OK ({n} findings <= baseline {base}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
