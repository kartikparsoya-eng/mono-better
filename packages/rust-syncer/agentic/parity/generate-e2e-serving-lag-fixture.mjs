#!/usr/bin/env node
/**
 * Generates the TS-vs-Rust e2e-serving-lag parity fixture.
 *
 * Drives the REAL TS `E2EServingLagTracker` (services/view-syncer/e2e-serving-lag.ts)
 * through sequences of `onVersionReady` / `onVersionServed` events (all timestamps
 * injected, so it is deterministic) and captures each served event's observation
 * (`{lagMs, clamped}` or null). The Rust `E2EServingLagTracker` must reproduce the
 * same observation for the same event sequence — pinning the pending-commit
 * coalescing (keep-oldest), the watermark replay-guard, and the negative-lag clamp.
 *
 * Usage:
 *   npx tsx packages/rust-syncer/agentic/parity/generate-e2e-serving-lag-fixture.mjs \
 *     > packages/rust-syncer/agentic/parity/e2e-serving-lag-fixture.json
 */

import {E2EServingLagTracker} from '../../../zero-cache/src/services/view-syncer/e2e-serving-lag.ts';

// An event is either a `ready` (feed a replica-ready commit) or a `served`
// (mark a version served + capture the returned observation | null).
const ready = (watermark, upstreamCommitTimeMs) => ({kind: 'ready', watermark, upstreamCommitTimeMs});
const served = (version, nowMs) => ({kind: 'served', version, nowMs});

const SCENARIOS = [
  {desc: 'basic lag then pending consumed', events: [
    ready('02', 1000), served('02', 1350), served('02', 1400),
  ]},
  {desc: 'serving a later version covers the commit', events: [
    ready('02', 1000), served('05', 1500),
  ]},
  {desc: 'serving behind the watermark is ignored until caught up', events: [
    ready('05', 1000), served('02', 1500), served('05', 1800),
  ]},
  {desc: 'coalesced notifications measure from the oldest commit', events: [
    ready('02', 1000), ready('03', 1100), ready('04', 1250), served('04', 2000),
  ]},
  {desc: 'notifications without a commit time are ignored', events: [
    ready('02', undefined), served('02', 1500),
    ready('03', 1000), ready('04', undefined), served('04', 1600),
  ]},
  {desc: 'clock skew: negative lag clamps to 0', events: [
    ready('02', 5000), served('02', 4000),
  ]},
  {desc: 'legitimate zero lag is not clamped', events: [
    ready('02', 1000), served('02', 1000),
  ]},
  {desc: 'serve with no pending returns null', events: [
    served('02', 1000),
  ]},
  {desc: 'missing watermark is ignored', events: [
    ready(undefined, 1000), served('02', 1500),
  ]},
  {desc: 're-ready after consume measures the new commit', events: [
    ready('02', 1000), served('02', 1200), ready('04', 1500), served('04', 1900),
  ]},
];

const cases = SCENARIOS.map(sc => {
  const tracker = new E2EServingLagTracker();
  const observations = [];
  for (const ev of sc.events) {
    if (ev.kind === 'ready') {
      tracker.onVersionReady({
        watermark: ev.watermark,
        upstreamCommitTimeMs: ev.upstreamCommitTimeMs,
      });
    } else {
      const obs = tracker.onVersionServed(ev.version, ev.nowMs);
      observations.push(obs === null ? null : {lagMs: obs.lagMs, clamped: obs.clamped});
    }
  }
  return {desc: sc.desc, events: sc.events, observations};
});

process.stdout.write(JSON.stringify({cases}, null, 2) + '\n');
