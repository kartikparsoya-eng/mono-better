#!/usr/bin/env python3
"""
M14 — log differential (STATIC half): every rust log callsite vs its TS twin.

WHY THIS LAYER EXISTS
---------------------
Nothing else compares what the two engines SAY. L1 checks a symbol exists, L2
diffs bodies, L3 the call topology, L8 which functions ran, M13 the protocol
frames, and the ART diff-oracle the result rows. Logs are operator-observable
but not client-observable, so they fall through every one of those. G13
(`xyne-art tools/log_gate.py`) scans EACH arm for known-bad signatures; it never
joins the two.

That hole cost us real signal. A 2026-09-08 read of the GKE sandbox rust pod
found, in ~11 minutes of one user's traffic:
  * `init pipelines@…` logged on EVERY query-set change (39 lines for ONE client
    group in 3.5 min) where TS logs it once per pipeline init;
  * `Slow query materialization` emitted once per PASS over the whole pass's
    wall time, where TS emits it per QUERY over that query's process time — so
    slow queries went unnamed and fast ones got warned about;
  * a purged-client CVR load logged at ERROR where TS's `ClientNotFoundError`
    carries `'warn'` — turning a routine reconnect into a paging-level event,
    against "0 ERROR" being the prod health signal;
  * a 10x-wrong slow-hydrate threshold read from a rust-only env name.
Every one is a log-surface divergence. Every gate was green throughout.

WHAT THIS CHECKS (static, no replay needed — runs in local CI)
-------------------------------------------------------------
Extracts every log callsite from both trees and joins them on the normalized
message template:

  rust   tracing::{info,warn,error,debug,trace}!( [fields,] "msg {x}" [, args] )
  TS     <lc>.{info,warn,error,debug}?.( `msg ${x}` [, args] )

Buckets:
  LEVEL-MISMATCH  same message, different severity  => FAIL. This is the
                  purged-client ERROR-vs-warn class. Severity is a contract
                  with whoever pages on the logs.
  RUST-ONLY       a rust line with no TS twin. Legitimate for rust-only
                  inventions (I-*) and diagnostics, so it is RATCHETED, not
                  banned: the count may not grow without updating the baseline
                  in the same commit (HARD RULE 13 — a status claim expires
                  with its status).
  TS-ONLY         TS says something rust never does. Informational: rust
                  deliberately does not port every `debug` line, and many TS
                  sites are in files with no rust twin at all.
  PAIRED          same message, same level => log-surface parity.

The RUNTIME half — normalize both arms' captured logs over one replay, then
diff signature COUNTS (the 39-vs-1 class, which no static check can see) —
lives in the harness next to the other replay tooling:
`xyne-art tools/log_diff.py`.

Usage:
  python3 parity/log_differential.py                # report + enforce ratchet
  python3 parity/log_differential.py --report       # full listing, never fails
  python3 parity/log_differential.py --update-baseline
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASELINE = os.path.join(ROOT, "parity", "log_differential_baseline.json")

RUST_TREES = [
    "packages/rust-syncer/src",
    "packages/rust-cvr/src",
    "packages/rust-ivm/src",
]
# The ported server surface. zero-cache is the TS twin of all three crates.
TS_TREES = [
    "packages/zero-cache/src",
]

RUST_LEVELS = ("error", "warn", "info", "debug", "trace")
TS_LEVELS = ("error", "warn", "info", "debug")
# TS has no `trace`; its `debug` is the floor. Compare only what both can express.
LEVEL_RANK = {"trace": 0, "debug": 1, "info": 2, "warn": 3, "error": 4}


# ---------------------------------------------------------------- scanning ---
def split_top_level(src: str) -> list[str]:
    """Split a macro/call argument list on commas that are not nested inside
    (), [], {}, or a string/char literal."""
    out, depth, i, start = [], 0, 0, 0
    in_str: str | None = None
    while i < len(src):
        c = src[i]
        if in_str:
            if c == "\\":
                i += 2
                continue
            if c == in_str:
                in_str = None
        elif c in "\"'`":
            in_str = c
        elif c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == "," and depth == 0:
            out.append(src[start:i])
            start = i + 1
        i += 1
    out.append(src[start:])
    return [a.strip() for a in out if a.strip()]


def balanced(src: str, open_at: int) -> tuple[str, int]:
    """Return (inside, index_after_close) for the (...) starting at open_at."""
    depth, i = 0, open_at
    in_str: str | None = None
    while i < len(src):
        c = src[i]
        if in_str:
            if c == "\\":
                i += 2
                continue
            if c == in_str:
                in_str = None
        elif c in "\"'`":
            in_str = c
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return src[open_at + 1 : i], i + 1
        i += 1
    return "", len(src)


PLACEHOLDER = "{}"


def norm_template(msg: str) -> str:
    """Normalize a message template to a cross-language key."""
    # interpolations -> {}
    msg = re.sub(r"\$\{[^}]*\}", PLACEHOLDER, msg)  # TS `${x}`
    msg = re.sub(r"\{[^{}]*\}", PLACEHOLDER, msg)  # rust `{x}` / `{}`
    msg = msg.replace("\\n", " ").replace("\\t", " ").replace('\\"', '"')
    # a rust format string broken across lines with a trailing backslash
    msg = re.sub(r"\s+", " ", msg).strip()
    return msg.strip(" :,-")


def rust_string_literal(arg: str) -> str | None:
    """The message template if `arg` is a (possibly raw, possibly multi-line
    concatenated) string literal; else None."""
    a = arg.strip()
    m = re.match(r'^r(#*)"(.*)"\1$', a, re.S)
    if m:
        return m.group(2)
    if not a.startswith('"'):
        return None
    parts = re.findall(r'"((?:[^"\\]|\\.)*)"', a, re.S)
    if not parts:
        return None
    lit = "".join(parts)
    # Rust's line-continuation escape: a trailing `\` swallows the newline AND
    # the next line's indentation. Leaving it in put a stray backslash in the
    # key, so every wrapped rust message (thrashing, rowSetSignature drift,
    # "Rejecting sync connection…") looked TS-only.
    return re.sub(r"\\\s*\n\s*", "", lit)


def ts_string_literal(arg: str) -> str | None:
    """The message template when `arg` is a string/template literal, or a `+`
    concatenation of them.

    TS wraps long messages across source lines as `` `a ${x} ` + `b ${y}` ``
    (view-syncer.ts does this for the drift / thrashing / syncQueryPipelineSet
    lines). Treating that as one literal is required, not cosmetic: leaving the
    "` + `" in the key made 6 lines that rust DOES emit — `flushed cvr@…` among
    them — look TS-only.
    """
    a = arg.strip()
    if not a or a[0] not in "'\"`":
        return None
    pieces: list[str] = []
    i, n = 0, len(a)
    while i < n:
        c = a[i]
        if c in "'\"`":
            q, j, buf = c, i + 1, []
            while j < n:
                if a[j] == "\\":
                    buf.append(a[j : j + 2])
                    j += 2
                    continue
                if a[j] == q:
                    break
                buf.append(a[j])
                j += 1
            pieces.append("".join(buf))
            i = j + 1
        else:
            # a non-literal operand in the concatenation
            if not a[i].isspace() and a[i] != "+":
                depth, j = 0, i
                while j < n:
                    if a[j] in "([{":
                        depth += 1
                    elif a[j] in ")]}":
                        depth -= 1
                    elif a[j] == "+" and depth == 0:
                        break
                    j += 1
                pieces.append(PLACEHOLDER)
                i = j
            else:
                i += 1
    return "".join(pieces) if pieces else None


def strip_rust_comments(src: str) -> str:
    """Blank out // line comments and /* */ blocks so commented-out callsites
    are not scanned. Preserves offsets (and therefore line numbers)."""
    out, i, n = [], 0, len(src)
    in_str: str | None = None
    while i < n:
        c = src[i]
        if in_str:
            out.append(c)
            if c == "\\":
                if i + 1 < n:
                    out.append(src[i + 1])
                i += 2
                continue
            if c == in_str:
                in_str = None
            i += 1
            continue
        if c in "\"'":
            in_str = c
            out.append(c)
            i += 1
            continue
        if c == "/" and i + 1 < n and src[i + 1] == "/":
            while i < n and src[i] != "\n":
                out.append(" ")
                i += 1
            continue
        if c == "/" and i + 1 < n and src[i + 1] == "*":
            while i < n and not (src[i] == "*" and i + 1 < n and src[i + 1] == "/"):
                out.append("\n" if src[i] == "\n" else " ")
                i += 1
            out.append("  ")
            i += 2
            continue
        out.append(c)
        i += 1
    return "".join(out)


RUST_CALL = re.compile(r"\btracing::(%s)!\s*\(" % "|".join(RUST_LEVELS))
# A raw write to stderr. It bypasses the tracing subscriber entirely: no level,
# no structured fields, and it ignores ZERO_LOG_LEVEL — so an ERROR-count alert
# or gate (G13's error-volume watch, the "0 ERROR" prod health signal) can never
# see it, while TS emits the same event through the LogContext at a real level.
RUST_PRINTLN = re.compile(r"\b(eprintln|println)!\s*\(")
# Files whose whole purpose is an env-gated diagnostic stream that deliberately
# sidesteps `tracing` (CVR_TRACE / SYNCER_TRACE / RUST_IVM_PERF_TRACE), plus the
# standalone binaries, whose stdout IS their interface.
UNSTRUCTURED_OK = (
    "/trace.rs", "/tracer.rs", "/perf_trace.rs", "/live_count.rs",
    "/src/bin/", "/main.rs",
)
# `lc`, `this.#lc`, `queryLC`, `coverageLC`, `_lc` … then `.info?.(`
TS_CALL = re.compile(r"\b[\w.#]*[lL][cC]\s*\.\s*(%s)\s*\?\.\s*\(" % "|".join(TS_LEVELS))
# `lc[logLevel]?.(...)` — connection.ts picks the severity at runtime from the
# thrown error's class (`sendError`, connection.ts:428). The message still has a
# rust twin, so it must be paired; its level is `dynamic` and is exempt from the
# level check (rust derives the same level in `classify_error_log_level`).
# The subscript is an EXPRESSION, commonly a call: `lc[getLogLevel(e)]?.(...)`
# (view-syncer.ts:1241). Restricting it to `[\w.#]+` missed every such site, so
# TS lines rust DOES emit looked rust-only (caught 2026-09-08 on
# `closing connection with error`).
TS_CALL_DYNAMIC = re.compile(r"\b[\w.#]*[lL][cC]\s*\[\s*[^\]\n]{1,120}\]\s*\?\.\s*\(")


def test_regions(src: str) -> list[tuple[int, int]]:
    """Byte ranges covered by `#[cfg(test)]` items.

    Brace-matched, not "everything after the attribute": rust-syncer puts
    `#[cfg(test)]` on individual struct FIELDS and test-seam fns near the top of
    `view_syncer.rs`, so a naive scan hides most of the file's production
    callsites (it hid 88 of 172 on the first run of this script).
    """
    regions = []
    for m in re.finditer(r"#\[cfg\(test\)\]", src):
        i, n = m.end(), len(src)
        while i < n and src[i] in " \t\r\n":
            i += 1
        # skip any further attributes on the same item
        while src.startswith("#[", i):
            depth, j = 0, i + 1
            while j < n:
                if src[j] == "[":
                    depth += 1
                elif src[j] == "]":
                    depth -= 1
                    if depth == 0:
                        break
                j += 1
            i = j + 1
            while i < n and src[i] in " \t\r\n":
                i += 1
        # the item runs to its balanced `{...}` block, or to the `;`/`,` that
        # terminates a field / use / const declaration. STRING-AWARE: a `"}"` in
        # a test's assertion message would otherwise close the block early and
        # mark the rest of the file as test code (it hid 65 production callsites
        # in view_syncer.rs, whose `mod tests` sits between two impl blocks).
        depth, j, opened = 0, i, False
        in_str: str | None = None
        while j < n:
            c = src[j]
            if in_str:
                if c == "\\":
                    j += 2
                    continue
                if c == in_str:
                    in_str = None
                j += 1
                continue
            if c == '"':
                in_str = c
                j += 1
                continue
            if c == "'":
                # char literal or a lifetime (`&'a str`) — only a closing quote
                # within 4 chars makes it a literal.
                k = src.find("'", j + 1)
                if 0 < k <= j + 4:
                    j = k + 1
                    continue
                j += 1
                continue
            if c == "{":
                depth += 1
                opened = True
            elif c == "}":
                depth -= 1
                if opened and depth == 0:
                    j += 1
                    break
            elif c in ";," and depth == 0 and not opened:
                j += 1
                break
            j += 1
        regions.append((m.start(), min(j, n)))
    return regions


def in_test_region(path: str, regions: list[tuple[int, int]], pos: int) -> bool:
    """True when the callsite is in a test file or inside a `#[cfg(test)]` item."""
    if "/tests/" in path or path.endswith("_test.rs") or ".test." in path:
        return True
    return any(a <= pos < b for a, b in regions)


def scan_rust() -> list[dict]:
    found = []
    for tree in RUST_TREES:
        base = os.path.join(ROOT, tree)
        for dirpath, _dirs, files in os.walk(base):
            for fn in files:
                if not fn.endswith(".rs"):
                    continue
                path = os.path.join(dirpath, fn)
                rel = os.path.relpath(path, ROOT)
                raw = open(path, encoding="utf-8", errors="replace").read()
                src = strip_rust_comments(raw)
                regions = test_regions(src)
                for m in RUST_PRINTLN.finditer(src):
                    if in_test_region(rel, regions, m.start()):
                        continue
                    if any(k in "/" + rel for k in UNSTRUCTURED_OK):
                        continue
                    inside, _end = balanced(src, m.end() - 1)
                    args = split_top_level(inside)
                    msg = None
                    for arg in args:
                        lit = rust_string_literal(arg)
                        if lit is not None:
                            msg = lit
                            break
                    if msg is None:
                        continue
                    found.append(
                        {
                            "lang": "rust",
                            "level": "unstructured",
                            "template": norm_template(msg),
                            "file": rel,
                            "line": src.count("\n", 0, m.start()) + 1,
                            "test": False,
                        }
                    )
                for m in RUST_CALL.finditer(src):
                    level = m.group(1)
                    inside, _end = balanced(src, m.end() - 1)
                    args = split_top_level(inside)
                    msg = None
                    for a in args:
                        # skip `field = value`, `%expr`, `?expr`, bare idents
                        if re.match(r"^[\w.]+\s*=", a) or a[:1] in ("%", "?"):
                            continue
                        lit = rust_string_literal(a)
                        if lit is not None:
                            msg = lit
                            break
                    if msg is None:
                        continue
                    found.append(
                        {
                            "lang": "rust",
                            "level": level,
                            "template": norm_template(msg),
                            "file": rel,
                            "line": src.count("\n", 0, m.start()) + 1,
                            "test": in_test_region(rel, regions, m.start()),
                        }
                    )
    return found


def scan_ts() -> list[dict]:
    found = []
    for tree in TS_TREES:
        base = os.path.join(ROOT, tree)
        for dirpath, _dirs, files in os.walk(base):
            if "node_modules" in dirpath:
                continue
            for fn in files:
                if not fn.endswith(".ts") or fn.endswith(".d.ts"):
                    continue
                path = os.path.join(dirpath, fn)
                rel = os.path.relpath(path, ROOT)
                src = open(path, encoding="utf-8", errors="replace").read()
                is_test = ".test." in fn or "/test/" in rel
                hits = [(m, m.group(1)) for m in TS_CALL.finditer(src)]
                hits += [(m, "dynamic") for m in TS_CALL_DYNAMIC.finditer(src)]
                for m, level in hits:
                    inside, _end = balanced(src, m.end() - 1)
                    args = split_top_level(inside)
                    if not args:
                        continue
                    lit = ts_string_literal(args[0])
                    if lit is None:
                        continue
                    found.append(
                        {
                            "lang": "ts",
                            "level": level,
                            "template": norm_template(lit),
                            "file": rel,
                            "line": src.count("\n", 0, m.start()) + 1,
                            "test": is_test,
                        }
                    )
    return found


# ------------------------------------------------------------------- join ----
def loose(tpl: str) -> str:
    """Pairing fallback: rust often appends a value the TS site passes as a
    SEPARATE argument (rust `"Sending error on WebSocket: {:?}"` vs TS
    `('Sending error on WebSocket', errorBody)`). Same line, same meaning — the
    trailing interpolation is a formatting choice, not a divergence."""
    t = re.sub(r"[\s:,\-=]*(\{\})+[\s.]*$", "", tpl).strip()
    return t.strip(" :,-").lower()


def join(rust: list[dict], ts: list[dict]) -> dict:
    rust = [r for r in rust if not r["test"] and r["template"]]
    unstructured = [r for r in rust if r["level"] == "unstructured"]
    rust = [r for r in rust if r["level"] != "unstructured"]
    ts = [t for t in ts if not t["test"] and t["template"]]

    ts_by_tpl: dict[str, list[dict]] = {}
    ts_by_loose: dict[str, list[dict]] = {}
    for t in ts:
        ts_by_tpl.setdefault(t["template"], []).append(t)
        ts_by_loose.setdefault(loose(t["template"]), []).append(t)

    paired, level_mismatch, rust_only = [], [], []
    matched_tpls = set()
    for r in rust:
        twins = ts_by_tpl.get(r["template"]) or ts_by_loose.get(loose(r["template"]))
        if not twins:
            rust_only.append(r)
            continue
        matched_tpls.update(t["template"] for t in twins)
        # rust `trace` has no TS expression; treat it as debug for comparison.
        rl = "debug" if r["level"] == "trace" else r["level"]
        # `dynamic` = TS picks the level at runtime from the thrown error; rust
        # reproduces that in `classify_error_log_level`, so any level pairs.
        if any(t["level"] in (rl, "dynamic") for t in twins):
            paired.append((r, twins[0]))
        else:
            level_mismatch.append((r, twins))
    ts_only = [t for t in ts if t["template"] not in matched_tpls]
    # An unstructured line whose text TS logs at a level is the sharp case: the
    # event exists on both sides, but only TS's is visible to level-based
    # filtering and error-count alerting.
    for u in unstructured:
        twins = ts_by_tpl.get(u["template"]) or ts_by_loose.get(loose(u["template"]))
        u["ts_twin"] = (
            f"{twins[0]['level']} {twins[0]['file'].split('/')[-1]}:{twins[0]['line']}"
            if twins
            else None
        )
        if twins:
            matched_tpls.update(t["template"] for t in twins)
    ts_only = [t for t in ts if t["template"] not in matched_tpls]
    return {
        "paired": paired,
        "level_mismatch": level_mismatch,
        "rust_only": rust_only,
        "ts_only": ts_only,
        "unstructured": unstructured,
    }


def load_baseline() -> dict:
    if not os.path.exists(BASELINE):
        return {"rust_only_count": None, "rust_only": []}
    return json.load(open(BASELINE))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", action="store_true",
                    help="print the full listing and always exit 0")
    ap.add_argument("--update-baseline", action="store_true",
                    help="rewrite the rust-only ratchet baseline")
    ap.add_argument("--json", default=None, help="write the raw join here")
    a = ap.parse_args()

    rust, ts = scan_rust(), scan_ts()
    j = join(rust, ts)
    base = load_baseline()

    print("== M14 log differential (static callsite join) ==")
    print(
        f"rust callsites {len([r for r in rust if not r['test']])} "
        f"(+{len([r for r in rust if r['test']])} in tests) | "
        f"TS callsites {len([t for t in ts if not t['test']])} "
        f"(+{len([t for t in ts if t['test']])} in tests)"
    )
    print(
        f"PAIRED {len(j['paired'])} | LEVEL-MISMATCH {len(j['level_mismatch'])} "
        f"| RUST-ONLY {len(j['rust_only'])} | TS-ONLY {len(j['ts_only'])}"
    )
    failed = False

    unstruct = j["unstructured"]
    twinned = [u for u in unstruct if u["ts_twin"]]
    print(
        f"UNSTRUCTURED {len(unstruct)} (bypass the tracing subscriber; "
        f"{len(twinned)} have a TS twin AT A LEVEL)"
    )
    if twinned:
        print("\n-- UNSTRUCTURED WITH A LEVELLED TS TWIN (invisible to level "
              "filtering + error-count alerting) --")
        for u in sorted(twinned, key=lambda x: x["file"]):
            print(f"  rust eprintln  {u['file']}:{u['line']}")
            print(f"       TS  {u['ts_twin']}   {u['template'][:80]!r}")
        failed = True

    if j["level_mismatch"]:
        print("\n-- LEVEL MISMATCH (same message, different severity) --")
        for r, twins in sorted(j["level_mismatch"], key=lambda x: x[0]["template"]):
            tl = "/".join(sorted({t["level"] for t in twins}))
            print(f"  rust {r['level']:<5} vs TS {tl:<5}  {r['template'][:88]!r}")
            print(f"      rust {r['file']}:{r['line']}")
            print(f"      TS   {twins[0]['file']}:{twins[0]['line']}")
        failed = True

    if a.report:
        print("\n-- RUST-ONLY (no TS twin: invention or rust diagnostic) --")
        for r in sorted(j["rust_only"], key=lambda x: (x["level"], x["template"])):
            print(f"  {r['level']:<5} {r['template'][:86]!r}  ({r['file']}:{r['line']})")
        print("\n-- TS-ONLY (TS says it, rust never does) --")
        for t in sorted(j["ts_only"], key=lambda x: (x["level"], x["template"]))[:200]:
            print(f"  {t['level']:<5} {t['template'][:86]!r}  ({t['file']}:{t['line']})")
        if len(j["ts_only"]) > 200:
            print(f"  … and {len(j['ts_only']) - 200} more")

    loud = [r for r in j["rust_only"] if r["level"] in ("error", "warn")]
    print(
        f"RUST-ONLY at ERROR/WARN {len(loud)}  <- the pager surface: a severity "
        f"the TS operator never sees"
    )
    if a.report and loud:
        print("\n-- RUST-ONLY at ERROR/WARN (highest signal) --")
        for r in sorted(loud, key=lambda x: (x["level"], x["template"])):
            print(f"  {r['level']:<5} {r['template'][:84]!r}  ({r['file']}:{r['line']})")

    keys = sorted({f"{r['level']}|{r['template']}" for r in j["rust_only"]})
    if a.update_baseline:
        json.dump(
            {"rust_only_count": len(keys), "rust_only": keys},
            open(BASELINE, "w"),
            indent=1,
        )
        print(f"\nbaseline updated: {len(keys)} rust-only signatures")
        return 0

    if base["rust_only_count"] is None:
        print("\nNOTE: no baseline yet — run with --update-baseline to arm the ratchet.")
    else:
        added = sorted(set(keys) - set(base["rust_only"]))
        if len(keys) > base["rust_only_count"] or added:
            print(
                f"\n-- RATCHET: rust-only signatures {base['rust_only_count']} -> {len(keys)} --"
            )
            for k in added:
                lvl, _, tpl = k.partition("|")
                print(f"  + {lvl:<5} {tpl[:86]!r}")
            print(
                "  A new rust-only log line needs a TS twin, or a justification in the\n"
                "  commit + `--update-baseline` in the SAME commit (HARD RULE 13)."
            )
            failed = True

    if a.json:
        json.dump(
            {
                k: [
                    (x[0] if isinstance(x, tuple) else x)
                    for x in v
                ]
                for k, v in j.items()
            },
            open(a.json, "w"),
            indent=1,
        )

    if a.report:
        return 0
    print("\nM14 log differential: " + ("FAIL" if failed else "PASS"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
