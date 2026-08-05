#!/usr/bin/env node
// oracle/napi-advance-diff.mjs — compare TS advance-oracle output vs
// napi-advance-runner output across hydrate, advance, and finalView.
//
// Usage: node napi-advance-diff.mjs <expected.json> <actual.json>
//   expected.json = ts-advance-runner.mjs output
//   actual.json   = napi-advance-runner.mjs output

import {readFileSync} from 'node:fs';

const META_FIELDS = new Set(['queryId', 'isHidden']);

function stripMeta(rc) {
  const out = {};
  for (const k of Object.keys(rc)) {
    if (!META_FIELDS.has(k)) out[k] = rc[k];
  }
  return out;
}

function canon(v) {
  if (v === null) return null;
  if (typeof v === 'number') {
    if (Object.is(v, -0)) return 0;
    if (Number.isFinite(v) && Math.round(v) === v) return Math.round(v);
    return v;
  }
  if (typeof v === 'boolean' || typeof v === 'string') return v;
  if (Array.isArray(v)) return v.map(canon);
  if (typeof v === 'object') {
    const out = {};
    for (const k of Object.keys(v).sort()) out[k] = canon(v[k]);
    return out;
  }
  return v;
}

function deepEqual(a, b) {
  if (a === b) return true;
  if (typeof a !== typeof b) return false;
  if (a === null || b === null) return a === b;
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    return a.every((x, i) => deepEqual(x, b[i]));
  }
  if (typeof a === 'object' && typeof b === 'object') {
    const ka = Object.keys(a), kb = Object.keys(b);
    if (ka.length !== kb.length) return false;
    return ka.every(k => deepEqual(a[k], b[k]));
  }
  return false;
}

function rowChangeKey(rc) {
  const rowKeyStr = JSON.stringify(canon(rc.rowKey));
  const rowStr = rc.row ? JSON.stringify(canon(rc.row)) : 'null';
  return `${rc.changeType ?? 0}|${rc.table}|${rowKeyStr}|${rowStr}`;
}

function rowChangeCmp(a, b) {
  const ka = rowChangeKey(a), kb = rowChangeKey(b);
  return ka < kb ? -1 : ka > kb ? 1 : 0;
}

function compareRowChangeSets(expected, actual) {
  const dropHidden = rows => rows.filter(r => r.isHidden !== true).map(({isHidden, ...rest}) => rest);
  const exp = dropHidden(expected).map(stripMeta).map(canon);
  const act = dropHidden(actual).map(stripMeta).map(canon);
  const expSorted = [...exp].sort(rowChangeCmp);
  const actSorted = [...act].sort(rowChangeCmp);

  if (expSorted.length !== actSorted.length) {
    const expSet = new Set(expSorted.map(rowChangeKey));
    const actSet = new Set(actSorted.map(rowChangeKey));
    const missing = expSorted.filter(r => !actSet.has(rowChangeKey(r))).slice(0, 5);
    const extra = actSorted.filter(r => !expSet.has(rowChangeKey(r))).slice(0, 5);
    return {
      path: `length (expected=${expSorted.length} actual=${actSorted.length})`,
      missing: missing.map(r => JSON.stringify({table: r.table, rowKey: r.rowKey, row: r.row})),
      extra: extra.map(r => JSON.stringify({table: r.table, rowKey: r.rowKey, row: r.row})),
    };
  }

  for (let i = 0; i < expSorted.length; i++) {
    if (!deepEqual(expSorted[i], actSorted[i])) {
      return {path: `row[${i}]`, expected: expSorted[i], actual: actSorted[i]};
    }
  }
  return null;
}

function main() {
  const [expectedPath, actualPath] = process.argv.slice(2);
  if (!expectedPath || !actualPath) {
    console.error('Usage: napi-advance-diff.mjs <expected.json> <actual.json>');
    process.exit(2);
  }

  const expected = JSON.parse(readFileSync(expectedPath, 'utf8'));
  const actual = JSON.parse(readFileSync(actualPath, 'utf8'));

  for (const phase of ['hydrate', 'advance', 'finalView']) {
    const diff = compareRowChangeSets(expected[phase] || [], actual[phase] || []);
    if (diff) {
      console.error(`${phase.toUpperCase()} DIFF: ${diff.path}`);
      if (diff.missing) {
        console.error(`  missing from napi: ${diff.missing.join(', ')}`);
        console.error(`  extra in napi: ${diff.extra.join(', ')}`);
      } else {
        console.error(`  expected: ${JSON.stringify(diff.expected)}`);
        console.error(`  actual:   ${JSON.stringify(diff.actual)}`);
      }
      process.exit(1);
    }
  }

  // #2 wedge/divergence detector: an unexpected in-place `-2` reset (e.g.
  // take-bound-divergence, or an unlawful scalar-subquery reset) means the
  // engine hit a state the production-representative fixtures should never
  // reach. Correctness diffing SKIPS reset rows, so surface them as failures.
  if (Array.isArray(actual.resets) && actual.resets.length > 0) {
    console.error(`UNEXPECTED RESET(S): ${JSON.stringify(actual.resets)}`);
    process.exit(1);
  }

  // #1 WAL-growth detector: a BUSY checkpoint after the engine is destroyed
  // means a snapshot connection leaked / a lagging snapshot was never released.
  if (actual.checkpointBusyAfterDestroy === 1) {
    console.error(
      'CHECKPOINT BUSY AFTER DESTROY: a snapshot connection leaked ' +
        '(lagging-snapshot / WAL-growth class)',
    );
    process.exit(1);
  }
  if (actual.checkpointBusyAfterDestroy === -1) {
    console.error(
      'CHECKPOINT PROBE DIED AFTER DESTROY (must never pass silently): ' +
        String(actual.checkpointProbeError || 'unknown'),
    );
    process.exit(1);
  }

  // #1b per-phase probes are telemetry (busy is structurally 0 on wal2 —
  // see wal2-probe-matrix.mjs); busy=1 only fires on plain-wal harnesses.
  for (const probe of actual.phaseCheckpointProbes || []) {
    if (probe.busy === 1) {
      console.error(
        `CHECKPOINT BUSY AFTER ${String(probe.phase).toUpperCase()}: ` +
          'a stale read-mark is pinned below the live snapshots ' +
          `(zombie/lagging-snapshot WAL-growth class): ${JSON.stringify(probe)}`,
      );
      process.exit(1);
    }
  }

  // #1c the STRONG zombie detector: after destroy there are no live read-marks,
  // so a write+PASSIVE-checkpoint loop must reclaim the whole wal2 log. A
  // frozen `checkpointed` means a connection was torn down with its read txn
  // open (zombie pin — the unbounded WAL-growth mechanism). TRUNCATE-busy (#1)
  // is blind to this on wal2; this probe is not.
  const reclaim = actual.walReclaimAfterDestroy;
  if (reclaim && reclaim.reclaimed === false) {
    console.error(
      'WAL NOT RECLAIMABLE AFTER DESTROY: a zombie read-mark survives teardown ' +
        `(leaked pinned connection / WAL-growth class): ${JSON.stringify(reclaim)}`,
    );
    process.exit(1);
  }

  console.log('EQUAL');
}

main();
