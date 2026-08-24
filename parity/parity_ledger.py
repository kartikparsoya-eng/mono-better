#!/usr/bin/env python3
"""
Deterministic TS<->Rust parity ledger.

Given a crate spec (a set of Rust files and the TS files they were ported from),
extract every symbol from both sides, normalize names across the
camelCase<->snake_case boundary, and print exactly what is NOT one-to-one:

  * MATCHED   - symbol exists on both sides (name-normalized)
  * TS-ONLY   - exists in TS, no Rust counterpart  => candidate "not ported"
  * RUST-ONLY - exists in Rust, no TS origin       => candidate "invented / renamed / drift"

This is a *name-level* ledger. It cannot judge whether the bodies agree
("function task wise") - it narrows the surface so a human/agent only has to
deep-read the deltas instead of thousands of lines. Signatures are captured so
arity / async / return differences are eyeballable on matched rows too.

Usage:
    python3 parity/parity_ledger.py cvr > parity/LEDGER-cvr.md
"""

import os
import re
import sys
from collections import defaultdict

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------------------
# Crate specs: which Rust files map to which TS files.
# TS files are the ORIGIN; Rust files are the PORT.
# ---------------------------------------------------------------------------
V = "packages/zero-cache/src/services/view-syncer"
CRATES = {
    "cvr": {
        "rust_dir": "packages/rust-cvr/src",
        # TS origin files that were actually ported INTO rust-cvr.
        # NOTE: query-covering.ts is intentionally excluded — it is ported into
        # packages/rust-syncer/src/query_covering.rs, not rust-cvr.
        "ts_files": [
            f"{V}/cvr.ts",
            f"{V}/cvr-store.ts",
            f"{V}/row-record-cache.ts",
            f"{V}/row-set-signature.ts",
            f"{V}/ttl-clock.ts",
            f"{V}/client-handler.ts",
            f"{V}/schema/cvr.ts",
            f"{V}/schema/types.ts",
        ],
        # Core files carry the sync/merge algorithms — a missing behavioral
        # symbol here is HIGH risk. schema/* is DDL+zod => structural, LOW risk.
        "core_ts": {"cvr.ts", "cvr-store.ts", "row-record-cache.ts",
                    "client-handler.ts", "row-set-signature.ts", "ttl-clock.ts"},
        # Rust files with no 1:1 TS origin (infra / idiom) => Rust-only here is
        # expected, not drift.
        "infra_rust": {"hash.rs", "tracer.rs", "otel_metrics.rs", "live_count.rs",
                       "parity_check.rs", "row_key.rs", "lib.rs",
                       "change_processor.rs", "ttl.rs", "shards.rs"},
        # TS files that are pure structure (DDL builders + zod codecs). Their fns
        # became inline SQL / serde derives, so they are NOT behavioral gaps.
        "structural_ts": {"schema/cvr.ts", "schema/types.ts"},
        # Confirmed resolutions the fuzzy pass can't infer (logic became inline SQL,
        # a TS fn maps to a Rust enum, or the conversion is identity). Keys are
        # canon() of the TS name.
        "aliases": {
            "convertttlvalues": ("INLINED", "cvr_store.rs upsert SQL: ttl/1000 + null-on-negative"),
            "getttlclock": ("INLINED", "cvr_store.rs SELECT instances.\"ttlClock\" (load path)"),
            "updatettlclock": ("INLINED", "cvr_store.rs UPDATE instances SET lastActive,ttlClock"),
            "ttlclockasnumber": ("IDENTITY", "TTLClock = i64 (ttl_clock.rs); no conversion"),
            "ttlclockfromnumber": ("IDENTITY", "TTLClock = i64 (ttl_clock.rs); no conversion"),
            "cvrerrorkind": ("CVRStoreError enum (cvr_store.rs)", "fn→enum discriminant"),
            "inspectqueries": ("send_inspect_response (client_handler.rs)", "inspector path"),
            "assert": ("assert_new_version (cvr.rs)", "rename"),
        },
    },
}

# Rust method names that are accessors / trait impls, not ported logic.
RUST_IDIOM_NAMES = {
    "new", "from", "default", "drop", "clone", "fmt", "eq", "hash", "get",
    "insert", "id", "base", "base_mut", "as_str", "len", "is_empty", "iter",
    "into", "try_from", "deref", "borrow", "next", "poll", "emit", "inc",
    "dec", "empty", "build",
}
# TS symbol kinds that are structural (types/schemas/DDL), not behavior.
STRUCTURAL_KINDS = {"type", "interface", "const", "enum"}

# ---------------------------------------------------------------------------
# Name normalization: collapse camelCase and snake_case to one canonical key.
#   mergeRefCounts -> mergerefcounts ; merge_ref_counts -> mergerefcounts
# ---------------------------------------------------------------------------
def canon(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())

# --- token-level similarity, for catching RENAMES that canon() can't ---
# (e.g. cvrErrorKind -> CVRStoreError, rowIDSignatureUnit -> signature_unit,
#  shouldDrain -> should_drain, and file renames like drain-coordinator -> drain)
STOP_TOKENS = {"get", "set", "is", "to", "as", "the", "of", "a", "fn", "id"}

def tokens(name: str):
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", name)  # camel split
    s = s.replace("_", " ").replace("-", " ")
    return {t.lower() for t in s.split() if len(t) >= 2 and t.lower() not in STOP_TOKENS}

def jaccard(a, b):
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)

FUZZY_THRESHOLD = 0.40  # a single shared generic verb (0.33) is not enough

# domain words too common to prove a rename on their own (inspectQueries vs
# delete_queries share only "queries" — not a real match).
COMMON_TOKENS = {
    "query", "queries", "client", "clients", "row", "rows", "patch", "record",
    "records", "value", "values", "desired", "version", "index", "name", "type",
    "data", "cvr", "store", "table", "column", "schema",
}

def distinctive(shared):
    """True if the shared tokens include something specific enough to trust."""
    return any(len(t) >= 4 and t not in COMMON_TOKENS for t in shared)

# ---------------------------------------------------------------------------
# Rust extraction
# ---------------------------------------------------------------------------
RUST_FN = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+(\w+)")
RUST_TYPE = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(struct|enum|trait)\s+(\w+)")
RUST_CONST = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:const|static)\s+(\w+)")

RUST_TEST_MOD = re.compile(r"^\s*(?:pub\s+)?mod\s+tests?\b|^\s*mod\s+test_\w+\b")

def extract_rust(path):
    """Return list of (canon, name, kind, lineno, signature).

    Skips the body of `mod tests { ... }` via brace-depth counting so that
    test-gated *imports* (`#[cfg(test)] use ...`) don't cause us to drop real
    code, while unit-test fns inside the module are still excluded.
    """
    out = []
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()
    skip_depth = 0          # >0 => currently inside a test module
    for i, line in enumerate(lines, 1):
        if skip_depth > 0:
            skip_depth += line.count("{") - line.count("}")
            continue
        if RUST_TEST_MOD.match(line):
            # enter skip; account for braces on this same line
            skip_depth = 1 + line.count("{") - line.count("}")
            if skip_depth < 1:
                skip_depth = 1
            continue
        m = RUST_FN.match(line)
        if m:
            out.append((canon(m.group(1)), m.group(1), "fn", i, line.strip()))
            continue
        m = RUST_TYPE.match(line)
        if m:
            out.append((canon(m.group(2)), m.group(2), m.group(1), i, line.strip()))
            continue
        m = RUST_CONST.match(line)
        if m:
            out.append((canon(m.group(1)), m.group(1), "const", i, line.strip()))
    return out

# ---------------------------------------------------------------------------
# TS extraction
# ---------------------------------------------------------------------------
TS_TOP = re.compile(
    r"^export\s+(?:default\s+)?(?:abstract\s+)?"
    r"(function|const|class|type|interface|enum)\s+(\w+)"
)
TS_PLAIN_FN = re.compile(r"^(?:async\s+)?function\s+(\w+)")
# indented class member: `  foo(`, `  async foo(`, `  foo<T>(`, `  static foo(`
TS_METHOD = re.compile(
    r"^  (?:public |private |protected |static |readonly |async |get |set |override )*"
    r"(\w+)\s*[(<]"
)
TS_METHOD_ARROW = re.compile(
    r"^  (?:public |private |protected |static |readonly )*"
    r"(?:#?\w+)\s*(?::[^=]+)?=\s*(?:async\s*)?\("
)
# keywords that look like methods but aren't
TS_KW = {
    "if", "for", "while", "switch", "catch", "return", "do", "else",
    "constructor", "function", "typeof", "await", "new", "throw", "yield",
    "case", "super", "this", "void", "in", "of", "as",
}

def extract_ts(path):
    out = []
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()
    for i, line in enumerate(lines, 1):
        m = TS_TOP.match(line)
        if m:
            out.append((canon(m.group(2)), m.group(2), m.group(1), i, line.strip()))
            continue
        m = TS_PLAIN_FN.match(line)
        if m:
            out.append((canon(m.group(1)), m.group(1), "function", i, line.strip()))
            continue
        m = TS_METHOD.match(line)
        if m and m.group(1) not in TS_KW:
            out.append((canon(m.group(1)), m.group(1), "method", i, line.strip()))
    return out

# ---------------------------------------------------------------------------
# Build ledger
# ---------------------------------------------------------------------------
def rel(p):
    return os.path.relpath(p, REPO)

def main():
    crate = sys.argv[1] if len(sys.argv) > 1 else "cvr"
    spec = CRATES[crate]

    rust_syms = {}   # canon -> list of (name, kind, file, line, sig)
    rust_root = os.path.join(REPO, spec["rust_dir"])
    rust_files = []
    for dirpath, _dirs, files in os.walk(rust_root):
        for f in files:
            if f.endswith(".rs"):
                # label relative to rust_dir so subdir files read as "schema/cvr.rs"
                rust_files.append(os.path.relpath(os.path.join(dirpath, f), rust_root))
    for fn in sorted(rust_files):
        path = os.path.join(rust_root, fn)
        for c, name, kind, ln, sig in extract_rust(path):
            rust_syms.setdefault(c, []).append((name, kind, fn, ln, sig))

    ts_syms = {}
    for rp in spec["ts_files"]:
        path = os.path.join(REPO, rp)
        if not os.path.exists(path):
            continue
        base = rel(path).split("view-syncer/")[-1]
        for c, name, kind, ln, sig in extract_ts(path):
            ts_syms.setdefault(c, []).append((name, kind, base, ln, sig))

    core_ts = spec.get("core_ts", set())
    infra_rust = spec.get("infra_rust", set())

    ts_keys = set(ts_syms)
    rust_keys = set(rust_syms)
    matched = sorted(ts_keys & rust_keys)
    ts_only = sorted(ts_keys - rust_keys)
    rust_only = sorted(rust_keys - ts_keys)

    def first(d, k):
        return d[k][0]

    aliases = spec.get("aliases", {})  # canon_ts -> (target|"INLINED"|"ABSENT", note)

    # === resolve renames via fuzzy token overlap (greedy global best-first) ===
    cands = []
    for tc in ts_only:
        if tc in aliases:
            continue
        tt = tokens(first(ts_syms, tc)[0])
        for rc in rust_only:
            rt = tokens(first(rust_syms, rc)[0])
            s = jaccard(tt, rt)
            if s >= FUZZY_THRESHOLD and distinctive(tt & rt):
                cands.append((s, tc, rc))
    cands.sort(key=lambda x: (-x[0], x[1], x[2]))
    used_ts, used_rust, fuzzy = set(), set(), {}
    for s, tc, rc in cands:
        if tc in used_ts or rc in used_rust:
            continue
        used_ts.add(tc); used_rust.add(rc); fuzzy[tc] = (rc, s)

    unresolved_ts = [k for k in ts_only if k not in fuzzy and k not in aliases]
    added_rust = [k for k in rust_only if k not in used_rust]

    # === file-structure edges (TS file -> Rust file) from exact + fuzzy pairs ===
    edges = defaultdict(lambda: defaultdict(int))     # tf -> rf -> count
    rust_incoming = defaultdict(set)                  # rf -> {tf}
    # per-Rust-file buckets of resolved pairs
    pairs_by_rf = defaultdict(list)                   # rf -> [(ts_name, rust_name, tag)]
    for k in matched:
        tn, _, tf, tl, _ = first(ts_syms, k)
        rn, _, rf, rl, _ = first(rust_syms, k)
        edges[tf][rf] += 1; rust_incoming[rf].add(tf)
        pairs_by_rf[rf].append((tf, tn, tl, rn, rl, "exact"))
    for tc, (rc, s) in fuzzy.items():
        tn, _, tf, tl, _ = first(ts_syms, tc)
        rn, _, rf, rl, _ = first(rust_syms, rc)
        edges[tf][rf] += 1; rust_incoming[rf].add(tf)
        pairs_by_rf[rf].append((tf, tn, tl, rn, rl, f"fuzzy {s:.2f}"))
    # pinned aliases that name a Rust file also count as a file edge, so a TS file
    # resolved entirely via aliases (e.g. ttl-clock.ts) isn't mislabelled DROPPED.
    for tc, (tgt, note) in aliases.items():
        if tc not in ts_syms:
            continue
        m = re.search(r"(\w+\.rs)", f"{tgt} {note}")
        if m:
            tf = first(ts_syms, tc)[2]
            edges[tf][m.group(1)] += 1; rust_incoming[m.group(1)].add(tf)

    # LOC per file
    def loc(path):
        try:
            with open(path, encoding="utf-8") as f:
                return sum(1 for _ in f)
        except OSError:
            return 0
    ts_loc = {}
    for rp in spec["ts_files"]:
        p = os.path.join(REPO, rp)
        if os.path.exists(p):
            ts_loc[rel(p).split("view-syncer/")[-1]] = loc(p)
    all_rust_files = sorted(fn for fn in os.listdir(os.path.join(REPO, spec["rust_dir"]))
                            if fn.endswith(".rs"))
    rust_loc = {fn: loc(os.path.join(REPO, spec["rust_dir"], fn)) for fn in all_rust_files}

    # classify each TS file's relationship
    def rel_kind(tf):
        tgt = edges.get(tf, {})
        if not tgt:
            return "DROPPED", []
        rfs = sorted(tgt.items(), key=lambda x: -x[1])
        top = rfs[0][1]
        # a secondary target only counts as a real split if it's substantial
        sig = [rf for rf, n in rfs[1:] if n >= max(3, top * 0.25)]
        if sig:
            return "SPLIT", rfs
        primary = rfs[0][0]
        return ("MERGED" if len(rust_incoming[primary]) > 1 else "1:1"), rfs

    new_rust = [fn for fn in all_rust_files
                if fn not in rust_incoming and (rust_loc[fn] > 0)]

    # ---------------------------------------------------------------- output
    print(f"# TS ⇄ Rust parity map — `{crate}` crate\n")
    print("_Deterministic. File edges + symbol pairs are derived from **shared symbol "
          "content**, never filenames — so renamed files (e.g. `drain-coordinator.ts`→"
          "`drain.rs`) and renamed symbols (`cvrErrorKind`→`CVRStoreError`) still bind. "
          "Bodies are not compared; behavior drift needs Layer-2 body review._\n")
    print(f"- symbols: TS **{len(ts_keys)}**, Rust **{len(rust_keys)}** · resolved pairs "
          f"**{len(matched)+len(fuzzy)}** (exact {len(matched)} + fuzzy {len(fuzzy)}) "
          f"+ aliases {len(aliases)}")
    structural_ts = spec.get("structural_ts", set())
    unresolved_behav = [k for k in unresolved_ts
                        if first(ts_syms, k)[1] in ("function", "method")
                        and first(ts_syms, k)[2] not in structural_ts]
    print(f"- 🟥 TS UNRESOLVED: **{len(unresolved_ts)}** "
          f"(**{len(unresolved_behav)}** behavioral ⇒ investigate · "
          f"{len(unresolved_ts)-len(unresolved_behav)} structural: zod/DDL/type-alias "
          f"⇒ serde/inline-SQL, expected) · 🟦 Rust-only ADDED: **{len(added_rust)}**\n")
    if unresolved_behav:
        print("> ⚠️ **Behavioral TS symbols with no Rust resolution — check these:** "
              + ", ".join(f"`{first(ts_syms, k)[0]}` ({first(ts_syms, k)[2]})"
                          for k in sorted(unresolved_behav)) + "\n")

    # ---- §1 FILE STRUCTURE DIFF ----
    print("## 1 · File structure diff\n")
    print(f"TS origin files: **{len(ts_loc)}**  ·  Rust files: **{len(all_rust_files)}** "
          f"({len(new_rust)} new)\n")
    print("| TS file (LOC) | rel | Rust file(s) (shared syms) |")
    print("|---|---|---|")
    for tf in sorted(ts_loc):
        kind, rfs = rel_kind(tf)
        rhs = ", ".join(f"`{rf}` ({n})" for rf, n in rfs) or "—"
        print(f"| `{tf}` ({ts_loc[tf]}) | **{kind}** | {rhs} |")
    print("\n**New Rust files (no TS origin — added in the port):**  "
          + (", ".join(f"`{fn}` ({rust_loc[fn]})" for fn in new_rust) or "none"))
    merges = {rf: s for rf, s in rust_incoming.items() if len(s) > 1}
    if merges:
        print("\n**Merges (many TS → one Rust file):**")
        for rf in sorted(merges):
            print(f"- `{rf}` ⟵ " + ", ".join(f"`{t}`" for t in sorted(merges[rf])))

    # ---- §2 PER-FILE FUNCTIONAL DIVERGENCE ----
    print("\n## 2 · Per-file functional divergence\n")
    # attribute unresolved TS symbols to their expected Rust file (via file edge)
    unresolved_by_rf = defaultdict(list)
    orphan_ts = []
    for k in unresolved_ts:
        tn, tk, tf, tl, _ = first(ts_syms, k)
        tgt = edges.get(tf, {})
        rf = max(tgt, key=tgt.get) if tgt else None
        (unresolved_by_rf[rf] if rf else orphan_ts).append((tn, tk, tf, tl))
    added_by_rf = defaultdict(list)
    for k in added_rust:
        rn, rk, rf, rl, _ = first(rust_syms, k)
        added_by_rf[rf].append((rn, rk, rl))

    for rf in all_rust_files:
        pairs = pairs_by_rf.get(rf, [])
        added = added_by_rf.get(rf, [])
        missing = unresolved_by_rf.get(rf, [])
        if not (pairs or added or missing):
            continue
        srcs = ", ".join(f"`{t}`" for t in sorted(rust_incoming.get(rf, []))) or "_(new)_"
        print(f"### `{rf}`  ⟵  {srcs}\n")
        if pairs:
            print("| TS symbol | Rust symbol | match |")
            print("|---|---|---|")
            for tf, tn, tl, rn, rl, tag in sorted(pairs, key=lambda x: x[1].lower()):
                print(f"| `{tn}` ({tf}:{tl}) | `{rn}` (:{rl}) | {tag} |")
        if missing:
            print(f"\n🟥 **TS symbols not resolved into this file ({len(missing)}):** "
                  + ", ".join(f"`{n}`" for n, *_ in sorted(missing)))
        if added:
            print(f"\n🟦 **Rust-only added here ({len(added)}):** "
                  + ", ".join(f"`{n}`" for n, *_ in sorted(added)))
        print()

    # ---- §3 FLAT ONE-TO-ONE MAP ----
    print("## 3 · Flat one-to-one symbol map (every TS symbol resolved)\n")
    print("| TS symbol | origin | → Rust | status |")
    print("|---|---|---|---|")
    for k in sorted(ts_keys, key=lambda k: (first(ts_syms, k)[2], first(ts_syms, k)[3])):
        tn, tk, tf, tl, _ = first(ts_syms, k)
        if k in matched:
            rn, _, rf, rl, _ = first(rust_syms, k)
            print(f"| `{tn}` | {tf}:{tl} | `{rn}` {rf}:{rl} | ✅ exact |")
        elif k in fuzzy:
            rc, s = fuzzy[k]; rn, _, rf, rl, _ = first(rust_syms, rc)
            print(f"| `{tn}` | {tf}:{tl} | `{rn}` {rf}:{rl} | 🔁 rename {s:.2f} |")
        elif k in aliases:
            tgt, note = aliases[k]
            print(f"| `{tn}` | {tf}:{tl} | {tgt} | 📌 {note} |")
        else:
            print(f"| `{tn}` | {tf}:{tl} | — | 🟥 UNRESOLVED |")

if __name__ == "__main__":
    main()
