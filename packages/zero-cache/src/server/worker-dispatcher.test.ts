import {mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {parsePath, SyncerAssignmentRouter} from './worker-dispatcher.ts';

test.each([
  ['/sync/v1/connect', {version: '1', worker: 'sync', action: 'connect'}],
  ['/sync/v2/connect', {version: '2', worker: 'sync', action: 'connect'}],
  [
    '/sync/v3/connect?foo=bar',
    {version: '3', worker: 'sync', action: 'connect'},
  ],
  [
    '/api/sync/v1/connect',
    {base: 'api', worker: 'sync', version: '1', action: 'connect'},
  ],
  [
    '/api/sync/v1/connect?a=b&c=d',
    {base: 'api', worker: 'sync', version: '1', action: 'connect'},
  ],
  [
    '/zero/sync/v1/connect',
    {base: 'zero', worker: 'sync', version: '1', action: 'connect'},
  ],
  [
    '/zero-api/sync/v0/connect',
    {base: 'zero-api', worker: 'sync', version: '0', action: 'connect'},
  ],
  [
    '/zero-api/sync/v2/connect?',
    {base: 'zero-api', worker: 'sync', version: '2', action: 'connect'},
  ],

  ['/mutate/v1/connect', {version: '1', worker: 'mutate', action: 'connect'}],
  ['/mutate/v2/connect', {version: '2', worker: 'mutate', action: 'connect'}],
  [
    '/mutate/v3/connect?foo=bar',
    {version: '3', worker: 'mutate', action: 'connect'},
  ],
  [
    '/api/mutate/v1/connect',
    {base: 'api', worker: 'mutate', version: '1', action: 'connect'},
  ],
  [
    '/api/mutate/v1/connect?a=b&c=d',
    {base: 'api', worker: 'mutate', version: '1', action: 'connect'},
  ],
  [
    '/zero/mutate/v1/connect',
    {base: 'zero', worker: 'mutate', version: '1', action: 'connect'},
  ],
  [
    '/zero-api/mutate/v0/connect',
    {base: 'zero-api', worker: 'mutate', version: '0', action: 'connect'},
  ],
  [
    '/zero-api/mutate/v2/connect?',
    {base: 'zero-api', worker: 'mutate', version: '2', action: 'connect'},
  ],
  [
    '/replication/v1/changes',
    {version: '1', worker: 'replication', action: 'changes'},
  ],
  [
    '/replication/v2/changes',
    {version: '2', worker: 'replication', action: 'changes'},
  ],
  [
    '/replication/v3/changes?foo=bar',
    {version: '3', worker: 'replication', action: 'changes'},
  ],
  [
    '/replication/v3/snapshot?id=foobar',
    {version: '3', worker: 'replication', action: 'snapshot'},
  ],
  [
    '/api/replication/v1/changes',
    {base: 'api', worker: 'replication', version: '1', action: 'changes'},
  ],
  [
    '/api/replication/v1/changes?a=b&c=d',
    {base: 'api', worker: 'replication', version: '1', action: 'changes'},
  ],

  ['/zero-api/sync/v2/connect/not/match', undefined],
  ['/too/many/components/sync/v0/connect', undefined],
  ['/random/path', undefined],
  ['/', undefined],
  ['', undefined],
])('parseSyncPath %s', (path, result) => {
  expect(parsePath(new URL(path, 'http://foo/'))).toEqual(result);
});

test('load-aware syncer routing chooses the lowest live score and keeps sticky assignments', () => {
  const dir = mkdtempSync(join(tmpdir(), 'zero-syncer-router-'));
  try {
    const assignmentsFile = join(dir, 'assignments.json');
    const router = new SyncerAssignmentRouter({
      taskID: 'task-a',
      syncerCount: 3,
      assignmentsFile,
      lc: createSilentLogContext(),
    });

    router.updateLoad(0, {
      workerIndex: 0,
      activeClientGroups: 5,
      activeConnections: 5,
      queries: 0,
      rows: 0,
      clientGroups: [],
      timestamp: 1,
    });
    router.updateLoad(1, {
      workerIndex: 1,
      activeClientGroups: 1,
      activeConnections: 1,
      queries: 0,
      rows: 0,
      clientGroups: [],
      timestamp: 1,
    });
    router.updateLoad(2, {
      workerIndex: 2,
      activeClientGroups: 3,
      activeConnections: 3,
      queries: 0,
      rows: 0,
      clientGroups: [],
      timestamp: 1,
    });

    expect(router.assign('cg-new')).toBe(1);

    router.updateLoad(1, {
      workerIndex: 1,
      activeClientGroups: 99,
      activeConnections: 99,
      queries: 0,
      rows: 0,
      clientGroups: [],
      timestamp: 2,
    });
    expect(router.assign('cg-new')).toBe(1);
    expect(JSON.parse(readFileSync(assignmentsFile, 'utf-8'))).toEqual({
      'cg-new': 1,
    });
  } finally {
    rmSync(dir, {recursive: true, force: true});
  }
});

test('load-aware syncer routing scores from live reports instead of stale persisted assignment counts', () => {
  const dir = mkdtempSync(join(tmpdir(), 'zero-syncer-router-'));
  try {
    const assignmentsFile = join(dir, 'assignments.json');
    writeFileSync(assignmentsFile, JSON.stringify({'old-cg': 0}));
    const router = new SyncerAssignmentRouter({
      taskID: 'task-a',
      syncerCount: 2,
      assignmentsFile,
      lc: createSilentLogContext(),
    });

    router.updateLoad(0, {
      workerIndex: 0,
      activeClientGroups: 0,
      activeConnections: 0,
      queries: 0,
      rows: 0,
      clientGroups: [],
      timestamp: 1,
    });
    router.updateLoad(1, {
      workerIndex: 1,
      activeClientGroups: 10,
      activeConnections: 10,
      queries: 0,
      rows: 0,
      clientGroups: [],
      timestamp: 1,
    });

    expect(router.assign('new-cg')).toBe(0);
  } finally {
    rmSync(dir, {recursive: true, force: true});
  }
});

test('controlled rehome moves a hot assigned client group after sustained imbalance', () => {
  const dir = mkdtempSync(join(tmpdir(), 'zero-syncer-router-'));
  try {
    const assignmentsFile = join(dir, 'assignments.json');
    writeFileSync(assignmentsFile, JSON.stringify({'hot-cg': 0}));
    const moves: unknown[] = [];
    const router = new SyncerAssignmentRouter({
      taskID: 'task-a',
      syncerCount: 2,
      assignmentsFile,
      lc: createSilentLogContext(),
      controlledRehome: {
        enabled: true,
        sustainedReports: 2,
        minScoreDelta: 1000,
        minDurationMs: 0,
        cooldownMs: 60_000,
        onRehome: move => moves.push(move),
      },
    });

    router.updateLoad(0, {
      workerIndex: 0,
      activeClientGroups: 4,
      activeConnections: 4,
      queries: 100,
      rows: 100_000,
      clientGroups: [
        {
          clientGroupID: 'hot-cg',
          activeConnections: 1,
          queries: 100,
          rows: 100_000,
        },
      ],
      timestamp: 1,
    });
    expect(moves).toHaveLength(0);

    router.updateLoad(1, {
      workerIndex: 1,
      activeClientGroups: 0,
      activeConnections: 0,
      queries: 0,
      rows: 0,
      clientGroups: [],
      timestamp: 1,
    });
    expect(moves).toHaveLength(0);

    router.updateLoad(0, {
      workerIndex: 0,
      activeClientGroups: 4,
      activeConnections: 4,
      queries: 100,
      rows: 100_000,
      clientGroups: [
        {
          clientGroupID: 'hot-cg',
          activeConnections: 1,
          queries: 100,
          rows: 100_000,
        },
      ],
      timestamp: 2,
    });

    expect(moves).toEqual([
      expect.objectContaining({
        clientGroupID: 'hot-cg',
        fromWorkerIndex: 0,
        toWorkerIndex: 1,
      }),
    ]);
    expect(router.assign('hot-cg')).toBe(1);
    expect(JSON.parse(readFileSync(assignmentsFile, 'utf-8'))).toEqual({
      'hot-cg': 1,
    });
  } finally {
    rmSync(dir, {recursive: true, force: true});
  }
});

test('controlled rehome does not move into an already-hot target', () => {
  const dir = mkdtempSync(join(tmpdir(), 'zero-syncer-router-'));
  try {
    const assignmentsFile = join(dir, 'assignments.json');
    writeFileSync(assignmentsFile, JSON.stringify({'hot-cg': 0}));
    const moves: unknown[] = [];
    const router = new SyncerAssignmentRouter({
      taskID: 'task-a',
      syncerCount: 3,
      assignmentsFile,
      lc: createSilentLogContext(),
      controlledRehome: {
        enabled: true,
        sustainedReports: 1,
        minScoreDelta: 1000,
        minDurationMs: 0,
        cooldownMs: 0,
        onRehome: move => moves.push(move),
      },
    });

    router.updateLoad(0, {
      workerIndex: 0,
      activeClientGroups: 100,
      activeConnections: 100,
      queries: 0,
      rows: 0,
      clientGroups: [
        {
          clientGroupID: 'hot-cg',
          activeConnections: 1,
          queries: 10,
          rows: 10_000,
        },
      ],
      timestamp: 1,
    });
    router.updateLoad(1, {
      workerIndex: 1,
      activeClientGroups: 95,
      activeConnections: 95,
      queries: 0,
      rows: 0,
      clientGroups: [],
      timestamp: 1,
    });
    router.updateLoad(2, {
      workerIndex: 2,
      activeClientGroups: 96,
      activeConnections: 96,
      queries: 0,
      rows: 0,
      clientGroups: [],
      timestamp: 1,
    });

    expect(moves).toHaveLength(0);
    expect(router.assign('hot-cg')).toBe(0);
  } finally {
    rmSync(dir, {recursive: true, force: true});
  }
});

test('controlled rehome caps rehomes in a rolling window even when cooldown is low', () => {
  const dir = mkdtempSync(join(tmpdir(), 'zero-syncer-router-'));
  try {
    const assignmentsFile = join(dir, 'assignments.json');
    writeFileSync(assignmentsFile, JSON.stringify({'hot-a': 0, 'hot-b': 0}));
    const moves: unknown[] = [];
    const router = new SyncerAssignmentRouter({
      taskID: 'task-a',
      syncerCount: 2,
      assignmentsFile,
      lc: createSilentLogContext(),
      controlledRehome: {
        enabled: true,
        sustainedReports: 1,
        minScoreDelta: 1000,
        minDurationMs: 0,
        cooldownMs: 0,
        maxRehomesPerWindow: 1,
        rehomeWindowMs: 60_000,
        onRehome: move => moves.push(move),
      },
    });

    const hotClientGroups = [
      {
        clientGroupID: 'hot-a',
        activeConnections: 1,
        queries: 100,
        rows: 100_000,
      },
      {
        clientGroupID: 'hot-b',
        activeConnections: 1,
        queries: 100,
        rows: 100_000,
      },
    ];
    const hotReport = {
      workerIndex: 0,
      activeClientGroups: 4,
      activeConnections: 4,
      queries: 100,
      rows: 100_000,
      clientGroups: hotClientGroups,
      timestamp: 1,
    };
    const coldReport = {
      workerIndex: 1,
      activeClientGroups: 0,
      activeConnections: 0,
      queries: 0,
      rows: 0,
      clientGroups: [],
      timestamp: 1,
    };

    router.updateLoad(0, hotReport);
    router.updateLoad(1, coldReport);
    expect(moves).toHaveLength(1);

    router.updateLoad(0, {...hotReport, timestamp: 2});
    router.updateLoad(1, {...coldReport, timestamp: 2});
    expect(moves).toHaveLength(1);
  } finally {
    rmSync(dir, {recursive: true, force: true});
  }
});
