#!/usr/bin/env python3
r"""
M3 — lifecycle state-flag registry (parity/ layer; a slice of the M0 matrix).

The 2026-09-02 divergence was a TS lifecycle FLAG (`#pipelinesSynced`) whose GATE was
dropped when rust re-architected the surrounding run-loop. Every TS boolean lifecycle
flag is a small state machine (set/reset/read sites); if rust drops or mis-wires the
flag — or gates the wrong call on it — behavior diverges while symbols/bodies stay 1:1.

This registry enumerates the TS lifecycle flags across the ported zero-cache surface,
records the rust counterpart + its status, and (guard) fails if a flag marked `ported`
has no rust symbol. `audit` rows are candidate gaps surfaced for verification;
`mechanism`/`invention` rows are sanctioned differences (cite where).

Regenerate the TS side with:
  grep -rnoE '#\w+ = (true|false)' packages/zero-cache/src/services packages/zero-cache/src/workers
"""
from __future__ import annotations
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ts_flag, ts_site, rust_symbol (None if n/a), rust_file_hint, status, note
REGISTRY = [
    ("#pipelinesSynced", "view-syncer.ts:274/576/606",
     "pipelines_synced", "packages/rust-syncer/src/services/view_syncer/view_syncer.rs",
     "ported", "gate for hydrate_unchanged_queries; set(TS:606)/reset(TS:575-576) mirrored (2026-09-02 fix b8b997674)"),
    ("#sharedRetransformReady", "connection-context-manager.ts:195",
     "shared_retransform_ready", "packages/rust-syncer/src/services/view_syncer/connection_context_manager.rs",
     "ported", "background-retransform readiness; set/reset via set_shared_retransform_ready"),
    ("#closed", "connection.ts:95/172",
     "closed", "packages/rust-syncer/src/ws_server.rs",
     "ported", "per-connection closed flag on the ws sink"),
    ("#everPoked", "client-handler.ts:126/338",
     None, "packages/rust-syncer/src/services/view_syncer/client_handler.rs",
     "audit", "CANDIDATE GAP: no rust counterpart found — verify rust replicates the first-poke/never-poked branch or port it"),
    ("#servingLagDistributionCacheClearQueued", "syncer.ts:335/480/483",
     "DISTRIBUTION_CACHE_TTL_MS", "packages/rust-syncer/src/workers/syncer.rs",
     "mechanism", "rust bounds cache-clear rate with a 200ms TTL instead of TS's queued-clear debounce flag; same observable effect"),
    ("#isStopped", "pusher.ts:77/253, mutagen.ts:69/162",
     None, "packages/rust-syncer/src/services/mutagen/pusher.rs",
     "invention", "rust push path is the Option-A relay invention (see INVENTIONS.md); TS pusher lifecycle does not map 1:1"),
]


def rust_symbol_present(symbol: str, hint: str) -> bool:
    path = os.path.join(ROOT, hint)
    targets = [path]
    if not os.path.isfile(path):
        # hint may be a dir-ish; search the crate
        targets = []
        base = os.path.join(ROOT, "packages/rust-syncer/src")
        for dp, _d, fs in os.walk(base):
            for f in fs:
                if f.endswith(".rs"):
                    targets.append(os.path.join(dp, f))
    pat = re.compile(r"\b" + re.escape(symbol) + r"\b")
    for t in targets:
        try:
            with open(t, encoding="utf-8", errors="replace") as fh:
                if pat.search(fh.read()):
                    return True
        except FileNotFoundError:
            continue
    return False


def main() -> int:
    rc = 0
    print("== M3 lifecycle state-flag registry (TS -> rust) ==")
    audits = []
    for ts_flag, ts_site, rust_sym, hint, status, note in REGISTRY:
        if status == "ported":
            present = rust_sym and rust_symbol_present(rust_sym, hint)
            mark = "OK " if present else "MISSING"
            if not present:
                rc = 1
            print(f"  [{mark}] {ts_flag} ({ts_site}) -> {rust_sym}  — {note}")
        else:
            tag = status.upper()
            print(f"  [{tag}] {ts_flag} ({ts_site}) -> {rust_sym or '—'}  — {note}")
            if status == "audit":
                audits.append(ts_flag)
    if audits:
        print(f"\n  {len(audits)} AUDIT flag(s) need verification (candidate gaps): {', '.join(audits)}")
    print("\nM3 state-flag registry:", "PASS" if rc == 0 else "FAIL (a ported flag lost its rust symbol)")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
