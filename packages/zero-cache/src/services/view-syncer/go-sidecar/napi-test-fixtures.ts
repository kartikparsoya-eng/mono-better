// Shared table-mode test fixture for the NAPI E2E tests. Creates a SQLite
// replica with the _zero metadata tables, matching the Go side's
// makeReplica pattern (fixtures_test.go). The removal sweep deleted memory
// mode (loadRows-fed MemorySource), so the Go engine always reads from a
// real SQLite file. Tests pre-seed data into the replica BEFORE the addon
// starts, then init is schema-only.

import {Database} from '../../../../../zqlite/src/db.ts';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {mkdtempSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';

export type ReplicaHandle = {
  path: string;
  db: Database;
  bumpVersion: (version: string) => void;
  addChangeLog: (
    version: string,
    pos: number,
    table: string,
    rowKey: string,
    op: string,
  ) => void;
};

const ZERO_REPLICATION_STATE = `CREATE TABLE "_zero.replicationState" (stateVersion TEXT NOT NULL, writeTimeMs INTEGER, lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1))`;
const ZERO_CHANGE_LOG = `CREATE TABLE "_zero.changeLog2" ("stateVersion" TEXT NOT NULL,"pos" INT NOT NULL,"table" TEXT NOT NULL,"rowKey" TEXT NOT NULL,"op" TEXT NOT NULL,"backfillingColumnVersions" TEXT DEFAULT '{}',PRIMARY KEY("stateVersion","pos"),UNIQUE("table","rowKey"))`;

export function makeTestReplica(): ReplicaHandle {
  const dir = mkdtempSync(join(tmpdir(), 'goivm-test-'));
  const path = join(dir, 'replica.db');
  const lc = createSilentLogContext();
  const db = new Database(lc, path);
  db.pragma('journal_mode = WAL');
  db.exec(ZERO_REPLICATION_STATE);
  db.exec(ZERO_CHANGE_LOG);
  db
    .prepare(
      'INSERT INTO "_zero.replicationState" (stateVersion, lock) VALUES (?, 1)',
    )
    .run('0000000001');

  return {
    path,
    db,
    bumpVersion: (version: string) => {
      db
        .prepare(
          'INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES (?, 1)',
        )
        .run(version);
    },
    addChangeLog: (
      version: string,
      pos: number,
      table: string,
      rowKey: string,
      op: string,
    ) => {
      db
        .prepare(
          'INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES (?,?,?,?,?)',
        )
        .run(version, pos, table, rowKey, op);
    },
  };
}
