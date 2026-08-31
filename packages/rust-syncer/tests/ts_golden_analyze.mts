// TS-golden driver for the analyzeQuery parity test.
//
// Drives the REAL TypeScript `analyzeQuery` (zero-cache/src/services/analyze.ts)
// over a SQLite replica and prints the `AnalyzeQueryResult` as JSON on stdout,
// so the Rust `analyze_query` (its 1:1 port) can be asserted to match
// field-for-field (excluding the nondeterministic `start`/`end`/`elapsed`).
//
// Invoked by `tests/analyze_query_golden_test.rs` via `npx tsx`. Two modes:
//   build   <path> <fixture>                            — create a replica file
//   analyze <path> <astJson> <synced> <vended> <perms> <auth> <joinPlans>
//
// `perms`/`auth` are JSON or the empty string for "none"; the numeric flags are
// "0"/"1".

import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Database} from '../../zqlite/src/db.ts';
import {analyzeQuery} from '../../zero-cache/src/services/analyze.ts';

function buildReplica(path: string, fixture: string): void {
  using db = new Database(createSilentLogContext(), path);
  db.pragma('journal_mode = wal2');
  db.exec(`
    CREATE TABLE "_zero.replicationConfig" (
      lock TEXT PRIMARY KEY DEFAULT 'singleton',
      replicaVersion TEXT NOT NULL, publications TEXT NOT NULL);
    CREATE TABLE "_zero.replicationState" (
      lock TEXT PRIMARY KEY DEFAULT 'singleton', stateVersion TEXT NOT NULL);
    INSERT INTO "_zero.replicationConfig" VALUES ('singleton', 'v1', '[]');
    INSERT INTO "_zero.replicationState" VALUES ('singleton', 'v1');
    CREATE TABLE "_zero.tableMetadata" (
      "schema" TEXT NOT NULL, "table" TEXT NOT NULL, "minRowVersion" TEXT,
      PRIMARY KEY ("schema", "table"));
  `);
  if (fixture === 'users') {
    db.exec(`
      CREATE TABLE "users" (
        "id" "text|NOT_NULL", "name" "text|NOT_NULL", "_0_version" "text");
      CREATE UNIQUE INDEX "u_id" ON "users" ("id");
      INSERT INTO "users" VALUES
        ('u1','Alice','01'),('u2','Bob','01'),('u3','Carol','01');
    `);
  } else if (fixture === 'issues_comments') {
    db.exec(`
      CREATE TABLE "issue" ("id" "text|NOT_NULL", "_0_version" "text");
      CREATE UNIQUE INDEX "i_id" ON "issue" ("id");
      INSERT INTO "issue" VALUES ('i1','01'),('i2','01'),('i3','01');
      CREATE TABLE "comment" (
        "id" "text|NOT_NULL", "issueId" "text|NOT_NULL", "_0_version" "text");
      CREATE UNIQUE INDEX "c_id" ON "comment" ("id");
      INSERT INTO "comment" VALUES ('c1','i1','01'),('c2','i1','01'),('c3','i2','01');
    `);
  } else {
    throw new Error(`unknown fixture ${fixture}`);
  }
}

async function analyze(argv: string[]): Promise<void> {
  const [path, astJson, synced, vended, perms, auth, joinPlans, clientSchemaJson] =
    argv;
  const config = {
    replica: {file: path},
    log: {level: 'error'},
    enableQueryPlanner: true,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any;
  const clientSchema = JSON.parse(clientSchemaJson);
  const ast = JSON.parse(astJson);
  const permissions = perms ? JSON.parse(perms) : undefined;
  const authData = auth ? JSON.parse(auth) : undefined;

  const result = await analyzeQuery(
    createSilentLogContext(),
    config,
    clientSchema,
    ast,
    synced === '1',
    vended === '1',
    permissions,
    authData,
    joinPlans === '1',
  );
  process.stdout.write(JSON.stringify(result));
}

const mode = process.argv[2];
if (mode === 'build') {
  buildReplica(process.argv[3], process.argv[4]);
} else if (mode === 'analyze') {
  await analyze(process.argv.slice(3));
} else {
  throw new Error(`unknown mode ${mode}`);
}
