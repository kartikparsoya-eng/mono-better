import {describe, expect, test} from 'vitest';
import {deriveGoSidecarSpawnEnv} from './spawn-env.ts';

// User's-audit wiring bug: goPrimaryTrigger sources every user-query
// advance through advanceToHead[Stream], and the sidecar REJECTS
// advanceToHeadStream without drive mode ("advanceToHeadStream requires
// drive mode (GO_IVM_ADVANCE_DRIVE=true)", go-ivm advance_to_head.go) —
// drive is what arms the per-CG Snapshotters. Pre-fix the spawn env set
// GO_IVM_ADVANCE_DRIVE only from advanceDrive, so a spawned trigger-primary
// sidecar failed every advance; production never noticed because the
// Docker images pair GO_PRIMARY_TRIGGER=true with ADVANCE_DRIVE=true at
// image build time (the mask).
describe('deriveGoSidecarSpawnEnv', () => {
  test('goPrimaryTrigger alone arms DRIVE + ADVANCE_TO_HEAD + table mode', () => {
    expect(deriveGoSidecarSpawnEnv({goPrimaryTrigger: true}, 'zero')).toEqual({
      GO_IVM_APP_ID: 'zero',
      GO_IVM_ADVANCE_TO_HEAD: 'true',
      GO_IVM_SOURCE_MODE: 'table',
      GO_IVM_ADVANCE_DRIVE: 'true', // pre-fix: missing — every advance failed
    });
  });

  test('advanceDrive alone arms DRIVE (unchanged behavior)', () => {
    expect(deriveGoSidecarSpawnEnv({advanceDrive: true}, 'zero')).toEqual({
      GO_IVM_APP_ID: 'zero',
      GO_IVM_ADVANCE_TO_HEAD: 'true',
      GO_IVM_SOURCE_MODE: 'table',
      GO_IVM_ADVANCE_DRIVE: 'true',
    });
  });

  test('advanceToHead alone stays derive-only (P1 shadow — no DRIVE)', () => {
    expect(deriveGoSidecarSpawnEnv({advanceToHead: true}, 'zero')).toEqual({
      GO_IVM_APP_ID: 'zero',
      GO_IVM_ADVANCE_TO_HEAD: 'true',
      GO_IVM_SOURCE_MODE: 'table',
    });
  });

  test('no mode flags → appID only', () => {
    expect(deriveGoSidecarSpawnEnv({}, 'myapp')).toEqual({
      GO_IVM_APP_ID: 'myapp',
    });
  });
});
