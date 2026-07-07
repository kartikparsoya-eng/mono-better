import {describe, expect, test} from 'vitest';
import {goHydrateStampVersion} from './pipeline-driver.ts';

// Gen-6 regression (CVR version-skew teardown, 2026-07-07 sandbox run at
// ec88d231): in Go-primary mode the CVR hydrate updater was stamped at TS's
// snapshotter version while the row data hydrated from GO's own snapshot,
// pinned LATER (handleInit runs after TS's pin; every advance re-pins Go
// after TS). On reconnects the re-executed queries carry UNCHANGED
// transformation hashes — nothing bumps the CVR version — so the updater's
// stateVersion equalled the committed CVR version exactly, and the first
// Go-delivered gap row fired cvr.ts:778
//
//   Expected CVR version to have been bumped above original
//   (orig=6pxfax4g, curr=6pxfax4g).
//   Row {"table":"reactions",...}: existing=undefined, new=6pxfet9s
//
// → full client-group teardown (8 clients lost per 240s high-intensity arm;
// stock TS: zero, because its single snapshot makes data version ≡ stamp).
//
// goHydrateStampVersion is the decision function that closes the gap: the
// hydrate stamp must be max(TS version, Go's init-time snapshotter pin,
// Go's latest advance-reported version) — an upper bound on every row the
// hydrate can deliver, so the CVRQueryDrivenUpdater constructor bumps
// whenever gap rows are possible. The version literals below are the real
// ones from the incident.
describe('view-syncer/pipeline-driver: gen-6 hydrate stamp version', () => {
  // The incident shape: TS pinned (and CVR committed) at 6pxfax4g; Go's
  // snapshotter pinned at 6py0f1l4; the asserting reactions row was written
  // at 6pxfet9s — inside the (6pxfax4g, 6py0f1l4] gap.
  const TS = '6pxfax4g';
  const ROW = '6pxfet9s';
  const GO_PIN = '6py0f1l4';

  test('Go pin ahead of TS → stamp at the Go pin (covers the gap row)', () => {
    const stamp = goHydrateStampVersion(TS, GO_PIN, null);
    expect(stamp).toBe(GO_PIN);
    // The property that makes cvr.ts:778 unreachable: the stamp strictly
    // exceeds the committed CVR version (== TS here), so the
    // CVRQueryDrivenUpdater constructor bumps and the gap row (6pxfet9s)
    // gets a legal patchVersion.
    expect(stamp > TS).toBe(true);
    expect(stamp >= ROW).toBe(true);
  });

  test('warm add: last advance version ahead of both → stamp at it', () => {
    // Mid-run query add on a live CG: Go's data plane is at its last
    // advance-reported version, ahead of both the stale init pin and TS.
    const stamp = goHydrateStampVersion(TS, TS, GO_PIN);
    expect(stamp).toBe(GO_PIN);
  });

  test('Go pin behind TS (TS re-pinned after init) → stamp at TS', () => {
    expect(goHydrateStampVersion(GO_PIN, TS, null)).toBe(GO_PIN);
    expect(goHydrateStampVersion(GO_PIN, TS, TS)).toBe(GO_PIN);
  });

  test('no Go components (pre-gen-6 sidecar / no advance yet) → TS version', () => {
    expect(goHydrateStampVersion(TS, undefined, null)).toBe(TS);
  });

  test('empty-string Go components are ignored, never regress the stamp', () => {
    // '' < any lexi version; a naive max would be fine but a naive MIN-style
    // mixup or unguarded inclusion must never surface '' (the same guard
    // reconcileGoPrimaryWatermark needs for Go omitting `version`).
    expect(goHydrateStampVersion(TS, '', null)).toBe(TS);
    expect(goHydrateStampVersion(TS, '', '')).toBe(TS);
    expect(goHydrateStampVersion(TS, undefined, '')).toBe(TS);
  });

  test('all three present → max wins regardless of order', () => {
    expect(goHydrateStampVersion(TS, GO_PIN, ROW)).toBe(GO_PIN);
    expect(goHydrateStampVersion(TS, ROW, GO_PIN)).toBe(GO_PIN);
    expect(goHydrateStampVersion(GO_PIN, TS, ROW)).toBe(GO_PIN);
  });

  test('equal versions → identity (the no-writes reconnect fast path)', () => {
    // When nothing was written between the CVR commit and the re-hydrate,
    // all three agree and the stamp equals the CVR version — the
    // #hydrateUnchangedQueries no-updater fast path stays reachable exactly
    // when it is sound (pipeline row set provably equals the CVR row set).
    expect(goHydrateStampVersion(TS, TS, TS)).toBe(TS);
  });
});
