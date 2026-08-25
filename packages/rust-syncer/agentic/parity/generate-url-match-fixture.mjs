#!/usr/bin/env node
/**
 * Generates the TS-vs-Rust url_match parity fixture.
 *
 * Drives the REAL TS `urlMatch(url, [compileUrlPattern(pattern)])` (custom/fetch.ts,
 * backed by the native WHATWG `URLPattern`) over a battery of (pattern, url) pairs
 * within the literal / `*` / `:name` subset the Rust `url_match` implements. The
 * Rust `url_match(pattern, url)` must return the same bool — this is the
 * security-relevant custom-query URL allowlist, so a divergence is a real finding.
 *
 * Patterns here stay inside the ported subset; full URLPattern features (regex
 * groups, optional segments) are intentionally out of scope for the Rust port.
 *
 * Usage:
 *   npx tsx packages/rust-syncer/agentic/parity/generate-url-match-fixture.mjs \
 *     > packages/rust-syncer/agentic/parity/url-match-fixture.json
 */

import {compileUrlPattern, urlMatch} from '../../../zero-cache/src/custom/fetch.ts';

// [pattern, url]
const PAIRS = [
  ['https://api.example.com/query', 'https://api.example.com/query'],
  ['https://api.example.com/query', 'https://api.example.com/query?tenant=123'],
  ['https://api.example.com/query', 'https://api.example.com/other'],
  ['https://api.example.com/query', 'https://evil.example.com/query'],
  ['https://api.example.com/*', 'https://api.example.com/v2/nested/path'],
  ['https://api.example.com/*', 'https://api.example.com/'],
  ['https://api.example.com/v1/*', 'https://api.example.com/v1/query'],
  ['https://api.example.com/v1/*', 'https://api.example.com/v2/query'],
  ['https://*.example.com/query', 'https://tenant-a.example.com/query'],
  ['https://*.example.com/query', 'https://example.com/query'],
  ['https://api.example.com/:tenant/query', 'https://api.example.com/acme/query'],
  ['https://api.example.com/:tenant/query', 'https://api.example.com/a/b/query'],
  ['https://api.example.com/:tenant/query', 'https://api.example.com//query'],
  ['http://localhost:8080/query', 'http://localhost:8080/query'],
  ['http://localhost:8080/query', 'http://localhost:9090/query'],
  ['https://api.example.com/query', 'https://api.example.com/query#frag'],
  ['https://api.example.com/*/items', 'https://api.example.com/tenant/items'],
  // --- adversarial: component-boundary crossing (F-FETCH-1) ---
  // Multi-level subdomain: URLPattern `*` crosses `.` within the host component
  // (so this MATCHES — the F-FETCH-1 claim that it wouldn't is wrong).
  ['https://*.example.com/*', 'https://api.v1.example.com/x'],
  ['https://*.example.com/query', 'https://a.b.c.example.com/query'],
  // Host/path boundary: `*` must NOT cross `://` or the host→path `/`. A flat
  // glob would let `evil.com/…` satisfy `*.example.com` via the PATH — a real
  // allowlist bypass URLPattern rejects (host is `evil.com`).
  ['https://*.example.com/query', 'https://evil.com/api.example.com/query'],
  ['https://api.example.com/*', 'https://evil.com/api.example.com/x'],
  ['https://api.example.com/query', 'https://evil.com/https://api.example.com/query'],
  // Scheme boundary: `*` in host must not swallow a different scheme.
  ['https://*.example.com/query', 'http://api.example.com/query'],
  // Host `*` must not cross into port/path.
  ['https://*.example.com/query', 'https://api.example.com:9999/query'],
];

const cases = PAIRS.map(([pattern, url]) => {
  let matched;
  try {
    matched = urlMatch(url, [compileUrlPattern(pattern)]);
  } catch (e) {
    matched = {compileError: e instanceof Error ? e.message : String(e)};
  }
  return {pattern, url, matched};
});

process.stdout.write(JSON.stringify({cases}, null, 2) + '\n');
