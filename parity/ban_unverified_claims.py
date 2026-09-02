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


def main() -> int:
    hits = find_hits()
    if "--list" in sys.argv:
        for rel, ln, phrase in hits:
            print(f"  {rel}:{ln}: unverified parity claim '{phrase}' (no .ts citation in block)")
        print(f"total: {len(hits)}")
        return 0
    if "--update-baseline" in sys.argv:
        with open(BASELINE_FILE, "w") as fh:
            fh.write(str(len(hits)) + "\n")
        print(f"baseline updated to {len(hits)}")
        return 0

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
        f"assertions of TS parity carry a `.ts` citation or are within the ratchet)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
