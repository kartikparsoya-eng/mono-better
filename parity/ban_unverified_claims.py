#!/usr/bin/env python3
"""
M5 — unverified TS-parity claim guard (parity/ layer).

The 2026-09-02 `hydrate_unchanged_queries` divergence was carried by a doc-comment
that ASSERTED parity as fact — "re-executes every alive same-hash pipeline on every
sync (TS's design)" — WITHOUT citing the TS line/gate. It was wrong: TS gates that
call behind `#pipelinesSynced` and runs it once. Had the comment been required to
cite the exact TS site, the divergence would have been obvious at review.

This guard scans the rust crates for comment BLOCKS that assert behavioral parity
with TS but carry NO TS source citation (`something.ts` or `.ts:<line>`) anywhere in
the same contiguous `//` block. Block-level (not per-line) so a claim on one line and
its `view-syncer.ts:592` citation on the next line is correctly treated as verified.

Contract: a parity ASSERTION must be falsifiable — it must point at the TS code it
claims to match. Unverified assertions are the HARD-RULE-13 anti-pattern.

Ratchet: fails only if the unverified-claim count EXCEEDS the recorded baseline
(parity/.ban_claims_baseline). New unverified claims are rejected; the existing
backlog is burned down over time. Run `--update-baseline` after reducing it.

SECOND CHECK — DANGLING CODE CITATIONS (added with the F-12 fix).

A citation is only falsifiable if the thing it points at EXISTS. Eleven
doc-comments across rust-ivm described the panic blast radius in terms of "the
napi `catch_unwind` (napi/src/lib.rs:222)" long after the napi cdylib was
deleted (a5e502ad9, M11/M12). That is worse than a stale pointer: it told the
reader a hard SQLite read error became a thrown JS error at an FFI boundary,
when the real boundary is `pipeline_driver`'s per-pull `catch_unwind` with
different recovery. HARD RULE 13 — a status claim expires with the status.

So every rust file/line citation in a comment must RESOLVE: a token with a `/`
is checked against the repo root and each crate root; a bare `foo.rs` is
checked by basename anywhere in the repo. `.ts` citations are checked the same
way (a renamed TS file silently orphans its rust twin's citation).

Usage:
  python3 parity/ban_unverified_claims.py            # enforce (CI)
  python3 parity/ban_unverified_claims.py --list      # show all current hits
  python3 parity/ban_unverified_claims.py --update-baseline
"""
from __future__ import annotations
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CRATES = ["packages/rust-syncer/src", "packages/rust-cvr/src", "packages/rust-ivm/src"]
BASELINE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".ban_claims_baseline")

# Phrases that assert behavioral equivalence AS FACT (not merely "port of").
# Deliberately tight/high-signal — we want the claims that need a citation to be
# falsifiable, not every mention of "TS".
ASSERTION = re.compile(
    r"\b("
    r"TS'?s design"
    r"|matches TS"
    r"|match(?:es)? the TS"
    r"|same as TS"
    r"|same as (?:the )?TS"
    r"|identical to TS"
    r"|exactly like TS"
    r"|just like TS"
    r"|1:1 with TS"
    r"|mirrors? TS"
    r"|parity with TS"
    r"|TS parity"
    r"|behaves? like TS"
    r"|equivalent to TS"
    r")\b",
    re.IGNORECASE,
)
# A TS citation: any *.ts filename (optionally with :line). This is the evidence.
CITATION = re.compile(r"[\w./-]+\.ts\b", re.IGNORECASE)

DANGLING_BASELINE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".dangling_refs_baseline"
)

# A source citation inside a comment: `path/to/file.rs`, `file.rs:123`,
# `some-file.ts:45`. Kept to .rs/.ts because those are the two trees a citation
# can point into.
SOURCE_REF = re.compile(r"(?<![\w./-])([\w][\w./-]*\.(?:rs|ts))\b")

# Citations that name a thing rather than a path, or that a filesystem check
# cannot adjudicate.
REF_ALLOW = {
    "mod.rs",  # ambiguous by design — dozens of them
    "lib.rs",
    "main.rs",
    "build.rs",
    "index.ts",
    "mod.ts",
}

# A citation is EXEMPT when the surrounding prose says the file is absent —
# "rust has no `worker_dispatcher.rs` twin", "the former `sync_engine.rs`" — or
# when it names a file in an EXTERNAL crate's source (tokio's, serde_json's),
# which is not in this tree by definition. The window checked is the text
# BEFORE the reference on its own line plus the preceding comment line, so an
# unrelated "no" elsewhere in a long block cannot excuse a real dangling ref.
ABSENT_CONTEXT = re.compile(
    r"\b(no|not|never|former(?:ly)?|removed|deleted|gone|dissolved|replaced"
    r"|obsolete|was|used to|pre-|external|crate's own|upstream)\b",
    re.IGNORECASE,
)


def _repo_index():
    """basename -> set of repo-relative paths, plus the set of all rel paths."""
    by_base: dict[str, set[str]] = {}
    rels: set[str] = set()
    skip = {".git", "node_modules", "target", "__pycache__", ".venv", "dist"}
    for dirpath, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in skip]
        for f in files:
            if not (f.endswith(".rs") or f.endswith(".ts")):
                continue
            rel = os.path.relpath(os.path.join(dirpath, f), ROOT)
            rels.add(rel)
            by_base.setdefault(f, set()).add(rel)
    return by_base, rels


def _resolves(ref: str, by_base, rels, crate_dir: str) -> bool:
    if ref in REF_ALLOW:
        return True
    if "/" in ref:
        # Repo-relative or crate-relative first.
        if ref in rels or os.path.exists(os.path.join(ROOT, ref)):
            return True
        if os.path.exists(os.path.join(crate_dir, ref)):
            return True
        # Then the two shorthands this codebase actually writes: a path SUFFIX
        # (`zql/src/ivm/take.ts` for `packages/zql/src/ivm/take.ts`) and an
        # elided middle (`zqlite/table-source.ts` for
        # `packages/zqlite/src/table-source.ts`). Both are unambiguous ways to
        # name a real file, so both resolve; what must NOT resolve is a citation
        # whose basename exists nowhere, or whose directory prefix contradicts
        # every real path with that basename.
        if any(r.endswith("/" + ref) for r in rels):
            return True
        want = ref.split("/")
        for cand in by_base.get(want[-1], ()):
            parts = cand.split("/")
            i = 0
            for seg in parts:
                if i < len(want) and seg == want[i]:
                    i += 1
            if i == len(want):
                return True
        return False
    return ref in by_base


def find_dangling():
    """Comment citations of a .rs/.ts file that no longer exists."""
    by_base, rels = _repo_index()
    hits = []
    for crate in CRATES:
        base = os.path.join(ROOT, crate)
        crate_dir = os.path.dirname(base)  # .../packages/rust-ivm
        for dirpath, _dirs, files in os.walk(base):
            for f in files:
                if not f.endswith(".rs"):
                    continue
                p = os.path.join(dirpath, f)
                for start, block in iter_comment_blocks(p):
                    for i, line in enumerate(block):
                        for m in SOURCE_REF.finditer(line):
                            ref = m.group(1)
                            if _resolves(ref, by_base, rels, crate_dir):
                                continue
                            window = line[: m.start()]
                            if i:
                                window = block[i - 1] + window
                            if ABSENT_CONTEXT.search(window):
                                continue
                            hits.append((os.path.relpath(p, ROOT), start + i, ref))
    hits.sort()
    return hits


def iter_comment_blocks(path: str):
    """Yield (start_line, [lines]) for each contiguous run of `//` comment lines."""
    with open(path, encoding="utf-8", errors="replace") as fh:
        lines = fh.readlines()
    i = 0
    n = len(lines)
    while i < n:
        stripped = lines[i].lstrip()
        if stripped.startswith("//"):
            start = i
            block = []
            while i < n and lines[i].lstrip().startswith("//"):
                block.append(lines[i])
                i += 1
            yield start + 1, block
        else:
            i += 1


def find_hits():
    hits = []
    for crate in CRATES:
        base = os.path.join(ROOT, crate)
        for dirpath, _dirs, files in os.walk(base):
            for f in files:
                if not f.endswith(".rs"):
                    continue
                p = os.path.join(dirpath, f)
                for start, block in iter_comment_blocks(p):
                    text = "".join(block)
                    if ASSERTION.search(text) and not CITATION.search(text):
                        rel = os.path.relpath(p, ROOT)
                        m = ASSERTION.search(text)
                        hits.append((rel, start, m.group(0).strip()))
    hits.sort()
    return hits


def read_baseline() -> int:
    try:
        with open(BASELINE_FILE) as fh:
            return int(fh.read().strip() or "0")
    except FileNotFoundError:
        return 0


def read_dangling_baseline() -> int:
    try:
        with open(DANGLING_BASELINE_FILE) as fh:
            return int(fh.read().strip() or "0")
    except FileNotFoundError:
        return 0


def main() -> int:
    hits = find_hits()
    dangling = find_dangling()
    if "--list" in sys.argv:
        for rel, ln, phrase in hits:
            print(f"  {rel}:{ln}: unverified parity claim '{phrase}' (no .ts citation in block)")
        print(f"total: {len(hits)}")
        for rel, ln, ref in dangling:
            print(f"  {rel}:{ln}: citation '{ref}' does not resolve to a file")
        print(f"total dangling: {len(dangling)}")
        return 0
    if "--update-baseline" in sys.argv:
        with open(BASELINE_FILE, "w") as fh:
            fh.write(str(len(hits)) + "\n")
        with open(DANGLING_BASELINE_FILE, "w") as fh:
            fh.write(str(len(dangling)) + "\n")
        print(f"baseline updated to {len(hits)} claims / {len(dangling)} dangling refs")
        return 0

    dangling_baseline = read_dangling_baseline()
    if len(dangling) > dangling_baseline:
        print(
            f"M5 dangling-citation guard: FAIL — {len(dangling)} comment citations "
            f"point at a file that does not exist (baseline {dangling_baseline}):"
        )
        for rel, ln, ref in dangling:
            print(f"  {rel}:{ln}: '{ref}' — the file is gone or renamed; cite what is TRUE now")
        return 1

    baseline = read_baseline()
    count = len(hits)
    if count > baseline:
        print(
            f"M5 unverified-claim guard: FAIL — {count} unverified TS-parity claims "
            f"(baseline {baseline}). New parity assertions must cite a `.ts` source:"
        )
        # Show the newest offenders (best-effort: show all beyond baseline is not
        # order-stable, so print all and let the author spot the new one).
        for rel, ln, phrase in hits:
            print(f"  {rel}:{ln}: '{phrase}' — add the TS file:line it matches, or soften the claim")
        return 1
    print(
        f"M5 unverified-claim guard: OK ({count} unverified claims <= baseline {baseline}; "
        f"assertions of TS parity carry a `.ts` citation or are within the ratchet). "
        f"Dangling citations: {len(dangling)} <= baseline {dangling_baseline}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
