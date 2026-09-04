/**
 * M13 TS oracle — drives the REAL `upstreamSchema` over the M13 frame corpus
 * and records what TS does with each frame.
 *
 * This reproduces `Connection.#handleMessage` (zero-cache/src/workers/
 * connection.ts:203-204) exactly: `JSON.parse(data)` followed by
 * `valita.parse(value, upstreamSchema)`, with any throw from either becoming an
 * `InvalidMessage` close. Nothing here re-implements the schema — that is the
 * whole point, so the golden cannot drift from the source of truth.
 *
 * Usage:  node_modules/.bin/tsx parity/ts_frame_oracle.mts
 * Writes: parity/coverage/frame-oracle-ts.ndjson  (checked in; the TS golden
 *         that `packages/rust-syncer/tests/frame_parity_test.rs` asserts against)
 */
import {readFileSync, writeFileSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import * as v from '../packages/shared/src/valita.ts';
import {upstreamSchema} from '../packages/zero-protocol/src/up.ts';

const here = dirname(fileURLToPath(import.meta.url));
const corpusPath = join(here, 'frame-fixtures', 'frame-corpus.ndjson');
const outPath = join(here, 'frame-fixtures', 'frame-oracle-ts.ndjson');

type Row = {id: string; frame: string};

const rows: Row[] = readFileSync(corpusPath, 'utf8')
  .split('\n')
  .filter(line => line.trim() !== '')
  .map(line => JSON.parse(line) as Row);

const out: string[] = [];
for (const {id, frame} of rows) {
  // Mirror connection.ts:201-206 — one try/catch spanning BOTH steps, because
  // TS cannot distinguish them either: each throws into the same handler.
  let accepted: boolean;
  let tag: string | null = null;
  let stage: 'json' | 'valita' | null = null;
  try {
    const value = JSON.parse(frame);
    try {
      const msg = v.parse(value, upstreamSchema);
      accepted = true;
      tag = String((msg as unknown[])[0]);
    } catch {
      accepted = false;
      stage = 'valita';
    }
  } catch {
    accepted = false;
    stage = 'json';
  }
  out.push(JSON.stringify({id, accepted, tag, stage}));
}

writeFileSync(outPath, out.join('\n') + '\n', 'utf8');
const accepted = out.filter(l => JSON.parse(l).accepted).length;
console.log(
  `M13 TS oracle: ${rows.length} frames -> ${outPath} ` +
    `(${accepted} accepted, ${rows.length - accepted} rejected)`,
);
