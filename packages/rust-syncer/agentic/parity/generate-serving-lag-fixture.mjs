#!/usr/bin/env node
/**
 * Generates the TS-vs-Rust serving-lag parity fixture.
 *
 * Runs the REAL TS `computeServingLagStatsMs` + `computeMaxServingLagMs`
 * (workers/syncer.ts) over a battery of (now, replicaReadyStates, viewSyncers)
 * scenarios and captures the resulting `ServingLagStats` + `maxMs`. The Rust
 * `serving_lag::compute_serving_lag_stats_ms` / `compute_max_serving_lag_ms`
 * must reproduce them exactly — pinning the whole chain (percentileNearestRank,
 * findFirstUnservedIndex, lower/upperBound, distribution + prune) to TS rather
 * than the porter's reading.
 *
 * Note: the TS fns MUTATE replicaReadyStates (the prune). We snapshot the
 * ORIGINAL states into the fixture and call each fn on a fresh copy, so the Rust
 * side can feed the same original inputs.
 *
 * Usage:
 *   npx tsx packages/rust-syncer/agentic/parity/generate-serving-lag-fixture.mjs \
 *     > packages/rust-syncer/agentic/parity/serving-lag-fixture.json
 */

import {
  computeServingLagStatsMs,
  computeMaxServingLagMs,
} from '../../../zero-cache/src/workers/syncer.ts';

// Watermarks are lexicographically-ordered version strings; the log is ascending
// in both watermark and time. `vs(created, served, eligible)`.
const st = (watermark, replicaReadyTimeMs) => ({watermark, replicaReadyTimeMs});
const vs = (createdAtMs, servedVersion, servingLagEligible) => ({
  createdAtMs,
  servedVersion,
  servingLagEligible,
});

// A larger monotonic log for percentile-edge coverage (10 states, t=100..1000).
const bigLog = Array.from({length: 10}, (_, i) =>
  st(String(10 + i).padStart(2, '0'), (i + 1) * 100),
);
// 10 eligible CGs, each served up to a different watermark -> 10 distinct lags.
const bigSyncers = Array.from({length: 10}, (_, i) =>
  vs(0, i === 0 ? null : String(10 + i - 1).padStart(2, '0'), true),
);

const SCENARIOS = [
  {desc: 'empty log + no syncers', now: 1000, states: [], syncers: []},
  {
    desc: 'empty log, eligible syncer -> lag 0',
    now: 1000,
    states: [],
    syncers: [vs(0, null, true)],
  },
  {
    desc: 'all fully served -> all lags 0',
    now: 1000,
    states: [st('02', 100), st('04', 200)],
    syncers: [vs(0, '04', true), vs(0, '04', true)],
  },
  {
    desc: 'never-served, created before all -> lag from idx 0',
    now: 1000,
    states: [st('02', 100), st('04', 200), st('06', 300)],
    syncers: [vs(0, null, true)],
  },
  {
    desc: 'created after everything -> fully served (lag 0)',
    now: 1000,
    states: [st('02', 100), st('04', 200)],
    syncers: [vs(999, null, true)],
  },
  {
    desc: 'ineligible syncers are skipped entirely',
    now: 1000,
    states: [st('02', 100), st('04', 200)],
    syncers: [vs(0, null, false), vs(0, '02', false)],
  },
  {
    desc: 'mixed: served-at-boundary vs never-served, one ineligible',
    now: 1000,
    states: [st('02', 100), st('04', 200), st('06', 300)],
    syncers: [vs(0, '02', true), vs(0, '04', true), vs(0, null, false)],
  },
  {
    desc: 'served exactly at last watermark -> fully served',
    now: 5000,
    states: [st('0a', 100), st('0b', 250), st('0c', 900)],
    syncers: [vs(0, '0c', true)],
  },
  {
    desc: 'created between states (lowerBound picks a middle index)',
    now: 2000,
    states: [st('02', 100), st('04', 200), st('06', 300), st('08', 400)],
    syncers: [vs(250, null, true)],
  },
  {desc: 'percentile edges: 10 states, 10 distinct lags', now: 1100, states: bigLog, syncers: bigSyncers},
  {
    desc: 'watermark upperBound past end -> fully served',
    now: 1000,
    states: [st('02', 100), st('04', 200)],
    syncers: [vs(0, '09', true)],
  },
];

const cases = SCENARIOS.map(sc => {
  // Each TS fn mutates its states arg (the prune) — call on fresh copies.
  const forStats = sc.states.map(s => ({...s}));
  const forMax = sc.states.map(s => ({...s}));
  const stats = computeServingLagStatsMs(sc.now, forStats, sc.syncers);
  const maxMs = computeMaxServingLagMs(sc.now, forMax, sc.syncers);
  return {
    desc: sc.desc,
    now: sc.now,
    replicaReadyStates: sc.states,
    viewSyncers: sc.syncers,
    stats,
    maxMs,
  };
});

process.stdout.write(JSON.stringify({cases}, null, 2) + '\n');
