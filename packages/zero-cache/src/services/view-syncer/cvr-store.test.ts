import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {CVRStore} from './cvr-store.ts';
import type {CVRSnapshot} from './cvr.ts';
import {ttlClockFromNumber} from './ttl-clock.ts';

describe('view-syncer/cvr-store', () => {
  test('discardPendingWrites clears queued row writes before flush', async () => {
    const lc = createSilentLogContext();
    const db = vi.fn(() => {
      throw new Error('unexpected database access');
    }) as unknown as PostgresDB;
    const store = new CVRStore(
      lc,
      db,
      {appID: 'roze', shardNum: 1},
      'task',
      'cvr',
      e => {
        throw e;
      },
    );

    store.putRowRecord({
      id: {schema: '', table: 'issues', rowKey: {id: '1'}},
      rowVersion: '02',
      patchVersion: {stateVersion: '02'},
      refCounts: {query: 1},
    });

    store.discardPendingWrites();

    await expect(
      store.flush(
        lc,
        {stateVersion: '01'},
        {
          id: 'cvr',
          version: {stateVersion: '02'},
          lastActive: 0,
          ttlClock: ttlClockFromNumber(0),
          replicaVersion: '01',
          clients: {},
          queries: {},
          clientSchema: null,
          profileID: null,
        } satisfies CVRSnapshot,
        0,
      ),
    ).resolves.toBeNull();
    expect(db).not.toHaveBeenCalled();
  });
});
