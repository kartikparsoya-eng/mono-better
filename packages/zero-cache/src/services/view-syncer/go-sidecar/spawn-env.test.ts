import {describe, expect, test} from 'vitest';
import {deriveGoSidecarSpawnEnv} from './spawn-env.ts';

// The engine always runs the production configuration (table-mode sources,
// self-derived advance); the only per-deployment env is the shard's appID —
// the permissions-table-watch fallback when the wire didn't carry one.
describe('deriveGoSidecarSpawnEnv', () => {
  test('carries only the appID', () => {
    expect(deriveGoSidecarSpawnEnv('zero')).toEqual({
      GO_IVM_APP_ID: 'zero',
    });
  });

  test('appID is passed through verbatim', () => {
    expect(deriveGoSidecarSpawnEnv('myapp')).toEqual({
      GO_IVM_APP_ID: 'myapp',
    });
  });
});
