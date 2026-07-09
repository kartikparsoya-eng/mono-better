import {describe, expect, test} from 'vitest';
import {replicaFileName} from '../../../workers/replicator.ts';
import {deriveGoSidecarSpawnEnv} from './spawn-env.ts';

// The engine always runs the production configuration (table-mode sources,
// self-derived advance); the only per-deployment env is the shard's appID —
// the permissions-table-watch fallback when the wire didn't carry one.
describe('deriveGoSidecarSpawnEnv', () => {
  test('carries appID and the exact replica file used by Snapshotter', () => {
    expect(deriveGoSidecarSpawnEnv('zero', '/tmp/zero/replica.db')).toEqual({
      GO_IVM_APP_ID: 'zero',
      GO_IVM_REPLICA_DB_PATH: '/tmp/zero/replica.db',
    });
  });

  test('appID is passed through verbatim', () => {
    expect(deriveGoSidecarSpawnEnv('myapp', '/tmp/zero/replica.db')).toEqual({
      GO_IVM_APP_ID: 'myapp',
      GO_IVM_REPLICA_DB_PATH: '/tmp/zero/replica.db',
    });
  });

  test('serving-copy mode passes the serving-copy file to Go', () => {
    const replicaFile = replicaFileName('/tmp/zero/replica.db', 'serving-copy');

    expect(deriveGoSidecarSpawnEnv('zero', replicaFile)).toEqual({
      GO_IVM_APP_ID: 'zero',
      GO_IVM_REPLICA_DB_PATH: '/tmp/zero/replica.db-serving-copy',
    });
  });
});
