#!/usr/bin/env python3
"""
Layer-8 (traffic-driven path differential).

L2 proves matched functions agree on FIXTURES; ART/L5 prove the client-visible
FRAMES agree under live traffic. Neither proves rust actually WALKED the same
code TS walked to produce those frames — a ported-but-never-wired function
(the G8 flip-planner class: `planQuery` hot in TS, `plan_query` 0 executions
in rust) is invisible to both until some query shape finally yields a wrong
answer. L8 closes that: record exactly which functions each side executes
under IDENTICAL traffic, join the two coverage sets through the Layer-1
ledger's resolved TS↔Rust symbol pairs, and diff.

Capture (see parity/L8-RUNBOOK.md for the full recipe):
  TS side    NODE_V8_COVERAGE=/coverage on the zero-cache container (tsx keeps
             module URLs on the original .ts files; V8 writes exact
             per-function invocation counts on graceful exit).
  rust side  image built with --build-arg RUST_SYNCER_COVERAGE=1
             (-C instrument-coverage) + LLVM_PROFILE_FILE=/coverage/... ;
             llvm-profdata merge + llvm-cov export -> JSON.
  traffic    xyne-art diff_oracle.py --primary <rust> --mirror <ts>
             --full-catalog : byte-identical deterministic query traffic at
             both sides (plus its convergence verdict, for free).

Join buckets, per resolved ledger pair (rust kind == fn only):
  BOTH-HOT      executed on both sides => path parity for this traffic
  TS-HOT/RUST-COLD   ** divergence candidate ** — TS took the path, rust never
                entered its twin (unwired port, or a divergent upstream branch)
  RUST-HOT/TS-COLD   rust executed code TS didn't (extra path / divergent branch)
  BOTH-COLD     traffic never exercised the pair — a TRAFFIC gap, not parity
                signal; bounded by what the catalog replays

Count-ratio anomalies (BOTH-HOT, rust/ts >= 100x either way) flag topology
divergences (the pre-fix EXISTS over-drain class).

The covered-SET is deterministic given identical input; COUNTS are not
(concurrency, retries) — sets are compared strictly, counts only at
order-of-magnitude level.

Usage:
  python3 parity/layer8_path_diff.py --ts-cov DIR --rust-cov FILE \
      [--crate cvr|ivm|syncer|all] [--json OUT.json] [--strict] > L8-PATH-DIFF.md
  python3 parity/layer8_path_diff.py --self-test
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from parity_ledger import (  # noqa: E402
    CRATES, REPO, canon, tokens, jaccard, distinctive, FUZZY_THRESHOLD,
    extract_rust, extract_ts, expand_ts_files, walk_rs,
)

# ---------------------------------------------------------------------------
# Ledger pairs (same resolution as parity_ledger.main: exact canon match, then
# greedy best-first fuzzy rename resolution; aliases are INLINED/N-A style
# resolutions with no fn twin, so they cannot be path-joined and are skipped).
# ---------------------------------------------------------------------------
def build_pairs(crate):
    spec = CRATES[crate]
    rust_syms, ts_syms = {}, {}
    rust_root = os.path.join(REPO, spec["rust_dir"])
    for fn in walk_rs(rust_root):
        for c, name, kind, ln, _sig in extract_rust(os.path.join(rust_root, fn)):
            rust_syms.setdefault(c, []).append((name, kind, fn, ln))
    for rp in expand_ts_files(spec):
        path = os.path.join(REPO, rp)
        if not os.path.exists(path):
            continue
        for c, name, kind, ln, _sig in extract_ts(path):
            ts_syms.setdefault(c, []).append((name, kind, rp, ln))

    aliases = spec.get("aliases", {})
    matched = sorted(set(ts_syms) & set(rust_syms))
    ts_only = sorted(set(ts_syms) - set(rust_syms))
    rust_only = sorted(set(rust_syms) - set(ts_syms))

    cands = []
    for tc in ts_only:
        if tc in aliases:
            continue
        tt = tokens(ts_syms[tc][0][0])
        for rc in rust_only:
            rt = tokens(rust_syms[rc][0][0])
            s = jaccard(tt, rt)
            if s >= FUZZY_THRESHOLD and distinctive(tt & rt):
                cands.append((s, tc, rc))
    cands.sort(key=lambda x: (-x[0], x[1], x[2]))
    used_ts, used_rust, fuzzy = set(), set(), []
    for s, tc, rc in cands:
        if tc in used_ts or rc in used_rust:
            continue
        used_ts.add(tc)
        used_rust.add(rc)
        fuzzy.append((tc, rc))

    pairs = []
    for tc in matched:
        tn, tk, tf, tl = ts_syms[tc][0]
        rn, rk, rf, rl = rust_syms[tc][0]
        pairs.append({"ts_name": tn, "ts_kind": tk, "ts_file": tf, "ts_line": tl,
                      "rust_name": rn, "rust_kind": rk, "rust_file": rf,
                      "rust_line": rl, "how": "exact"})
    for tc, rc in fuzzy:
        tn, tk, tf, tl = ts_syms[tc][0]
        rn, rk, rf, rl = rust_syms[rc][0]
        pairs.append({"ts_name": tn, "ts_kind": tk, "ts_file": tf, "ts_line": tl,
                      "rust_name": rn, "rust_kind": rk, "rust_file": rf,
                      "rust_line": rl, "how": "fuzzy"})
    return pairs


# ---------------------------------------------------------------------------
# TS side: NODE_V8_COVERAGE json dir -> {(relpath, canon): count} + {canon: count}
# ---------------------------------------------------------------------------
_TS_NAME_STRIP = re.compile(r"^(?:get |set |async )+")

def _ts_fn_key(function_name):
    """V8 functionName -> ledger canon key ('' = anonymous, unattributable)."""
    n = _TS_NAME_STRIP.sub("", function_name).split(".")[-1].lstrip("#")
    return canon(n)

def load_ts_coverage(cov_dir):
    by_file_fn = defaultdict(int)   # (repo-relpath, canonfn) -> count
    by_fn = defaultdict(int)        # canonfn -> count
    files_seen = set()
    for fname in sorted(os.listdir(cov_dir)):
        if not fname.endswith(".json"):
            continue
        try:
            with open(os.path.join(cov_dir, fname), encoding="utf-8") as f:
                doc = json.load(f)
        except (OSError, ValueError):
            continue
        for script in doc.get("result", []):
            url = script.get("url", "")
            if "/packages/" not in url or not url.endswith(".ts"):
                continue
            # container path file:///app/mono/packages/... -> packages/...
            rel = "packages/" + url.split("/packages/", 1)[1]
            files_seen.add(rel)
            for fn in script.get("functions", []):
                ranges = fn.get("ranges") or []
                count = ranges[0].get("count", 0) if ranges else 0
                if count <= 0:
                    continue
                key = _ts_fn_key(fn.get("functionName", ""))
                if not key:
                    continue
                by_file_fn[(rel, key)] += count
                by_fn[key] += count
    return by_file_fn, by_fn, files_seen


# ---------------------------------------------------------------------------
# rust side: llvm-cov export json -> {(crate, relfile, canonfn): count} + ...
# Symbols are demangled with a minimal length-prefixed-ident parser that
# handles BOTH legacy (_ZN..17h<hash>E) and v0 (_R..) schemes well enough to
# recover crate / module / fn segments (closure segments are attributed to
# their enclosing fn; the trailing legacy hash segment is dropped).
# ---------------------------------------------------------------------------
_RUST_CRATES = {"rust_syncer": "syncer", "rust_ivm": "ivm", "rust_cvr": "cvr"}
_LEN_IDENT = re.compile(r"(\d+)")
# v0 crate-root disambiguators (`Cs<base62>_`) start with DIGITS inside the
# base62 hash — a naive length-prefix parser reads them as huge idents and
# swallows the rest of the symbol (that hid `delete_unreferenced_rows` on the
# first run). Strip them before parsing. rustfilt, when installed, replaces
# this heuristic entirely.
_V0_CRATE_HASH = re.compile(r"Cs[a-zA-Z0-9]{4,}?_")

def _mangled_segments(name):
    if name.startswith("_R"):
        name = _V0_CRATE_HASH.sub("", name)
    segs, i = [], 0
    while i < len(name):
        m = _LEN_IDENT.match(name, i)
        if not m:
            i += 1
            continue
        n = int(m.group(1))
        start = m.end()
        seg = name[start:start + n]
        # only accept a plausible identifier chunk of exactly that length
        if len(seg) == n and re.fullmatch(r"[A-Za-z_$][A-Za-z0-9_$.]*", seg):
            segs.append(seg)
            i = start + n
        else:
            i = m.end()
    return segs


def _rustfilt_demangle(names):
    """Batch-demangle via rustfilt when available: returns {mangled:
    demangled} or None. Demangled paths parse deterministically — no
    heuristics."""
    import shutil
    import subprocess
    exe = shutil.which("rustfilt") or os.path.expanduser("~/.cargo/bin/rustfilt")
    if not os.path.exists(exe):
        return None
    out = subprocess.run([exe], input="\n".join(names), capture_output=True,
                         text=True)
    if out.returncode != 0:
        return None
    lines = out.stdout.splitlines()
    return dict(zip(names, lines)) if len(lines) == len(names) else None


def _demangled_idents(text):
    """Idents from a DEMANGLED path like `<rust_cvr::cvr::Updater>::finish`."""
    return re.findall(r"[A-Za-z_][A-Za-z0-9_]*", text)

def _demangle_fn(name):
    """mangled -> (crate_key or None, [plausible fn idents]).

    v0 mangling appends GENERIC-ARGUMENT paths after the function ident
    (`_RI..35compute_serving_lag_distribution_msINt..core5slice..`), so "the
    last segment" is often a segment of the generic arg, not the fn. Rather
    than guess which segment is the fn, credit EVERY plausible ident — the
    join looks names up by exact length-prefixed ident, and the primary
    (file, name) lookup keeps precision. Missing a name here produced the
    false TS-HOT/RUST-COLD verdicts of the first L8 run.
    """
    segs = _mangled_segments(name)
    crate = next((c for s in segs for c in (_RUST_CRATES.get(s.lstrip("_")),)
                  if c), None)
    fns = [s for s in segs
           if not re.fullmatch(r"h[0-9a-f]{16}", s)   # legacy generic hash
           and "closure" not in s and "$" not in s and s != "_ZN"]
    return crate, fns

def load_rust_coverage(export_json):
    with open(export_json, encoding="utf-8") as f:
        doc = json.load(f)
    by_file_fn = defaultdict(int)   # (crate, src-relfile, canonfn) -> count
    by_fn = defaultdict(int)        # (crate, canonfn) -> count
    # Prefer exact demangling (rustfilt) over the heuristic parser.
    all_names = [fn.get("name", "") for data in doc.get("data", [])
                 for fn in data.get("functions", []) if fn.get("count", 0) > 0]
    demangled = _rustfilt_demangle(sorted(set(all_names))) or {}
    for data in doc.get("data", []):
        for fn in data.get("functions", []):
            count = fn.get("count", 0)
            if count <= 0:
                continue
            raw = fn.get("name", "")
            if raw in demangled:
                idents = [s for s in _demangled_idents(demangled[raw])
                          if s != "closure"]
                crate = next((c for s in idents
                              for c in (_RUST_CRATES.get(s),) if c), None)
            else:
                crate, idents = _demangle_fn(raw)
            fname = next((f for f in fn.get("filenames", [])
                          if "packages/rust-" in f), None)
            if fname and not crate:
                m = re.search(r"packages/rust-(\w+)/", fname)
                crate = m.group(1) if m and m.group(1) in ("syncer", "ivm", "cvr") \
                    else None
            if not crate or not idents:
                continue
            for ident in idents:
                key = canon(ident)
                by_fn[(crate, key)] += count
                if fname:
                    rel = fname.split("/src/", 1)[-1]
                    by_file_fn[(crate, rel, key)] += count
    return by_file_fn, by_fn


# ---------------------------------------------------------------------------
# Join
# ---------------------------------------------------------------------------
def join_crate(crate, pairs, ts_ff, ts_f, ru_ff, ru_f):
    rows = []
    for p in pairs:
        if p["rust_kind"] != "fn":
            continue  # coverage only observes function execution
        tkey, rkey = canon(p["ts_name"]), canon(p["rust_name"])
        tcount = ts_ff.get((p["ts_file"], tkey), 0) or ts_f.get(tkey, 0)
        rcount = ru_ff.get((crate, p["rust_file"], rkey), 0) \
            or ru_f.get((crate, rkey), 0)
        bucket = ("BOTH-HOT" if tcount and rcount else
                  "TS-HOT/RUST-COLD" if tcount else
                  "RUST-HOT/TS-COLD" if rcount else "BOTH-COLD")
        rows.append({**p, "ts_count": tcount, "rust_count": rcount,
                     "bucket": bucket})
    return rows


def report(all_rows, out_json=None):
    print("# Layer-8 traffic-driven path differential\n")
    print("_Same traffic at both sides; a pair is a ledger-resolved TS fn and "
          "its rust twin. `TS-HOT/RUST-COLD` = TS took the path, rust never "
          "entered its twin — the unwired-port class. `BOTH-COLD` = the "
          "traffic never exercised the pair (traffic gap, not divergence)._\n")
    divergent = 0
    for crate, rows in all_rows.items():
        b = defaultdict(list)
        for r in rows:
            b[r["bucket"]].append(r)
        n = len(rows)
        hot = len(b["BOTH-HOT"])
        print(f"## {crate} — {n} fn-pairs: {hot} BOTH-HOT, "
              f"{len(b['TS-HOT/RUST-COLD'])} TS-HOT/RUST-COLD, "
              f"{len(b['RUST-HOT/TS-COLD'])} RUST-HOT/TS-COLD, "
              f"{len(b['BOTH-COLD'])} BOTH-COLD "
              f"(traffic exercised {hot}/{n})\n")
        for bucket, mark in (("TS-HOT/RUST-COLD", "❌"), ("RUST-HOT/TS-COLD", "⚠️")):
            if b[bucket]:
                divergent += len(b[bucket]) if bucket == "TS-HOT/RUST-COLD" else 0
                print(f"### {mark} {bucket}\n")
                print("| TS symbol (file:line) | rust twin (file:line) | ts# | rust# | how |")
                print("|---|---|---|---|---|")
                for r in sorted(b[bucket], key=lambda r: -max(r["ts_count"],
                                                              r["rust_count"])):
                    print(f"| `{r['ts_name']}` ({r['ts_file'].split('/src/')[-1]}"
                          f":{r['ts_line']}) | `{r['rust_name']}` "
                          f"({r['rust_file']}:{r['rust_line']}) | {r['ts_count']} "
                          f"| {r['rust_count']} | {r['how']} |")
                print()
        anomalies = [r for r in b["BOTH-HOT"]
                     if max(r["ts_count"], r["rust_count"])
                     >= 100 * max(1, min(r["ts_count"], r["rust_count"]))]
        if anomalies:
            print("### ⚠️ count-ratio anomalies (≥100× — check topology)\n")
            print("| pair | ts# | rust# |")
            print("|---|---|---|")
            for r in sorted(anomalies, key=lambda r: -max(r["ts_count"],
                                                          r["rust_count"]))[:25]:
                print(f"| `{r['ts_name']}` → `{r['rust_name']}` "
                      f"| {r['ts_count']} | {r['rust_count']} |")
            print()
    if out_json:
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(all_rows, f, indent=1)
        print(f"\n_full row set: {out_json}_")
    return divergent


# ---------------------------------------------------------------------------
def self_test():
    """Synthetic end-to-end check of both parsers + the join (no I/O deps)."""
    # legacy + closure + hash
    c, fns = _demangle_fn(
        "_ZN11rust_syncer6router7CgState17query_context_for17h0123456789abcdefE")
    assert c == "syncer" and "query_context_for" in fns
    c, fns = _demangle_fn(
        "_ZN8rust_ivm3ivm4take4Take5_push28_$u7b$$u7b$closure$u7d$$u7d$"
        "17hfedcba9876543210E")
    assert c == "ivm" and "_push" in fns
    # v0 generic instantiation: generic-arg path follows the fn ident — the fn
    # must still be credited (the first-run false-cold class).
    c, fns = _demangle_fn(
        "_RINvNtNtCs2UINqNd1QsX_11rust_syncer7workers6syncer"
        "35compute_serving_lag_distribution_msINtNtNtCsfo0Ls1ZyrQZ_4core"
        "5slice4iter4IterNtB4_16ReplicaReadyStateE")
    assert c == "syncer" and "compute_serving_lag_distribution_ms" in fns
    assert _ts_fn_key("ViewSyncer.#hydrateUnsafe") == canon("hydrateUnsafe")
    assert _ts_fn_key("get ttlClock") == canon("ttlClock")
    pairs = [{"ts_name": "planQuery", "ts_kind": "function", "ts_file": "f.ts",
              "ts_line": 1, "rust_name": "plan_query", "rust_kind": "fn",
              "rust_file": "planner/plan.rs", "rust_line": 1, "how": "exact"}]
    rows = join_crate("ivm", pairs,
                      {("f.ts", canon("planQuery")): 5}, {canon("planQuery"): 5},
                      {}, {})
    assert rows[0]["bucket"] == "TS-HOT/RUST-COLD", rows
    rows = join_crate("ivm", pairs,
                      {("f.ts", canon("planQuery")): 5}, {canon("planQuery"): 5},
                      {("ivm", "planner/plan.rs", canon("plan_query")): 7},
                      {("ivm", canon("plan_query")): 7})
    assert rows[0]["bucket"] == "BOTH-HOT", rows
    print("layer8 self-test: OK")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ts-cov", help="NODE_V8_COVERAGE output dir")
    ap.add_argument("--rust-cov", help="llvm-cov export json")
    ap.add_argument("--crate", default="all", choices=["all", *CRATES])
    ap.add_argument("--json", help="write full row set to this path")
    ap.add_argument("--strict", action="store_true",
                    help="exit 1 if any TS-HOT/RUST-COLD pair exists")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        self_test()
        return 0
    if not args.ts_cov or not args.rust_cov:
        ap.error("--ts-cov and --rust-cov are required (or --self-test)")
    ts_ff, ts_f, _files = load_ts_coverage(args.ts_cov)
    ru_ff, ru_f = load_rust_coverage(args.rust_cov)
    crates = list(CRATES) if args.crate == "all" else [args.crate]
    all_rows = {c: join_crate(c, build_pairs(c), ts_ff, ts_f, ru_ff, ru_f)
                for c in crates}
    divergent = report(all_rows, args.json)
    return 1 if (args.strict and divergent) else 0


if __name__ == "__main__":
    sys.exit(main())
