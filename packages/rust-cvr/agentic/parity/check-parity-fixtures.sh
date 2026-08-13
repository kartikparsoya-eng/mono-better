#!/usr/bin/env bash
# Verify the committed TS-derived parity fixtures are FRESH — i.e. the Rust
# parity tests are validating against CURRENT TS behavior, not stale captured
# output. Regenerates each fixture from the real TS implementations and compares
# to the committed copy. Deterministic fixtures are diffed exactly; the auth
# fixture (non-deterministic ECDSA/RSA key + signature material) is compared
# semantically on the decision fields only.
#
# Run from CI / the release gate:  check-parity-fixtures.sh <mono-root>
set -euo pipefail
ROOT="${1:?usage: check-parity-fixtures.sh <mono-root>}"
cd "$ROOT"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail=0

check_exact() {
  local gen="$1" committed="$2" name="$3"
  npx --no-install tsx "$gen" > "$tmp/out.json"
  if diff -q "$committed" "$tmp/out.json" >/dev/null 2>&1; then
    echo "  fresh: $name"
  else
    echo "STALE FIXTURE: $name differs from current TS output. Regenerate:"
    echo "    npx tsx $gen > $committed"
    diff "$committed" "$tmp/out.json" | head -30 || true
    fail=1
  fi
}

check_exact packages/rust-cvr/agentic/parity/generate-fixture.mjs \
  packages/rust-cvr/agentic/parity/parity-fixture.json "rust-cvr primitives + version + cvr"
check_exact packages/rust-syncer/agentic/parity/generate-perms-fixture.mjs \
  packages/rust-syncer/agentic/parity/perms-fixture.json "permissions transform"

# auth: semantic diff — crypto material rotates each run, so compare only the
# per-case decision fields (desc/issuer/audience/userID/tsAccept). A TS
# verifyToken semantic change flips tsAccept and is caught here.
npx --no-install tsx packages/rust-syncer/agentic/parity/generate-auth-fixture.mjs \
  > "$tmp/auth.json"
if python3 - "$tmp/auth.json" packages/rust-syncer/agentic/parity/auth-fixture.json <<'PY'
import json, sys
new = json.load(open(sys.argv[1])); old = json.load(open(sys.argv[2]))
def key(c): return (c['desc'], c.get('issuer'), c.get('audience'), c['userID'], c['tsAccept'])
if sorted(map(key, new['cases'])) == sorted(map(key, old['cases'])):
    print("  fresh: JWT auth (semantic)"); sys.exit(0)
print("STALE FIXTURE: JWT auth decision set changed vs current TS. Regenerate:")
print("    npx tsx packages/rust-syncer/agentic/parity/generate-auth-fixture.mjs > packages/rust-syncer/agentic/parity/auth-fixture.json")
sys.exit(1)
PY
then :; else fail=1; fi

if [ "$fail" != 0 ]; then
  echo "parity fixtures STALE — regenerate the listed fixture(s) and re-run the Rust parity tests." >&2
  exit 1
fi
echo "parity fixtures: FRESH"
