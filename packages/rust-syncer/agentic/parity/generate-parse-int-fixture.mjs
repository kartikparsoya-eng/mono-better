#!/usr/bin/env node
/**
 * Generates the TS-vs-Rust JS-parseInt parity fixture.
 *
 * The connect-params parser reads `ts` / `lmid` via TS `URLParams.getInteger`,
 * which is `parseInt(value)` (NaN → throw). Rust reimplements JS `parseInt` as
 * `connect_params::parse_js_integer` — a subtle, quirk-laden contract (truncate
 * at '.', accept leading whitespace + sign, stop at trailing junk, auto-hex on
 * 0x, stop at 'e'/'E'). This drives the REAL JS `parseInt` over a battery of
 * strings and captures its result (null for NaN); the Rust `parse_js_integer`
 * must agree for every one.
 *
 * Values stay within i64 range (Rust parse_js_integer is i64; JS parseInt is an
 * f64) so the comparison is exact.
 *
 * Usage:
 *   npx tsx packages/rust-syncer/agentic/parity/generate-parse-int-fixture.mjs \
 *     > packages/rust-syncer/agentic/parity/parse-int-fixture.json
 */

const INPUTS = [
  '0',
  '42',
  '-42',
  '+5',
  '1700000000',
  '1786564382909.802', // truncate at '.'
  '  -42tail',          // leading ws + sign, stop at trailing junk
  '0x10',               // auto-hex
  '0X1F',
  '1e3',                // stop at 'e' -> 1
  '3.99',               // truncate -> 3
  '   17   ',           // surrounding whitespace
  '007',                // leading zeros (NOT octal in parseInt)
  '-0',
  '',                   // NaN -> null
  'abc',                // NaN -> null
  'ts',                 // NaN -> null
  '+',                  // NaN -> null
  '.5',                 // NaN -> null (no leading digit)
  '12abc34',            // stop at first non-digit -> 12
  '9007199254740991',   // Number.MAX_SAFE_INTEGER
];

const cases = INPUTS.map(input => {
  const n = parseInt(input);
  return {input, result: Number.isNaN(n) ? null : n};
});

process.stdout.write(JSON.stringify({cases}, null, 2) + '\n');
