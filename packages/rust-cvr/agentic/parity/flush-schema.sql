CREATE SCHEMA IF NOT EXISTS "roze_1/cvr";
CREATE TABLE "roze_1/cvr".instances (
  "clientGroupID"  TEXT PRIMARY KEY,
  "version"        TEXT NOT NULL,                       -- Sortable representation of CVRVersion, e.g. "5nbqa2w:09"
  "lastActive"     TIMESTAMPTZ NOT NULL,                -- For garbage collection
  "ttlClock"       DOUBLE PRECISION NOT NULL DEFAULT 0, -- The ttl clock gets "paused" when disconnected.
  "replicaVersion" TEXT,                                -- Identifies the replica (i.e. initial-sync point) from which the CVR data comes.
  "owner"          TEXT,                                -- The ID of the task / server that has been granted ownership of the CVR.
  "grantedAt"      TIMESTAMPTZ,                         -- The time at which the current owner was last granted ownership (most recent connection time).
  "clientSchema"   JSONB,                               -- ClientSchema of the client group
  "profileID"      TEXT,                                -- Stable profile id ("p..."), falling back to the clientGroupID ("cg{clientGroupID}") for old clients
  "deleted"        BOOL DEFAULT FALSE                   -- Tombstone column for deleted CVRs; instances rows are kept longer for usage stats
);

-- For garbage collection.
CREATE INDEX instances_last_active
  ON "roze_1/cvr".instances ("lastActive") WHERE NOT "deleted";
CREATE INDEX tombstones_last_active
  ON "roze_1/cvr".instances ("lastActive") WHERE "deleted";

-- For usage stats; the composite index allows a 
-- SELECT COUNT(DISTINCT("profileID")) query to be answered by
-- an index scan without additional table lookups.
CREATE INDEX profile_ids_last_active ON "roze_1/cvr".instances ("lastActive", "profileID")
  WHERE "profileID" IS NOT NULL;

CREATE TABLE "roze_1/cvr".clients (
  "clientGroupID"      TEXT,
  "clientID"           TEXT,

  PRIMARY KEY ("clientGroupID", "clientID"),

  CONSTRAINT fk_clients_client_group
    FOREIGN KEY("clientGroupID")
    REFERENCES "roze_1/cvr".instances("clientGroupID")
    ON DELETE CASCADE
);


CREATE TABLE "roze_1/cvr".queries (
  "clientGroupID"         TEXT,
  "queryHash"             TEXT, -- this is the hash of the client query AST
  "clientAST"             JSONB, -- this is nullable as custom queries will not persist an AST
  "queryName"             TEXT, -- the name of the query if it is a custom query
  "queryArgs"             JSON, -- the arguments of the query if it is a custom query
  "patchVersion"          TEXT,  -- NULL if only desired but not yet "got"
  "transformationHash"    TEXT,
  "transformationVersion" TEXT,
  "internal"              BOOL,  -- If true, no need to track / send patches
  "deleted"               BOOL,  -- put vs del "got" query
  "rowSetSignature"       TEXT,  -- Hex XOR of h64([schema,table,rowKey]) for rows in this query (drift detection for Cap)

  PRIMARY KEY ("clientGroupID", "queryHash"),

  CONSTRAINT fk_queries_client_group
    FOREIGN KEY("clientGroupID")
    REFERENCES "roze_1/cvr".instances("clientGroupID")
    ON DELETE CASCADE
);

-- For catchup patches.
CREATE INDEX queries_patch_version 
  ON "roze_1/cvr".queries ("patchVersion" NULLS FIRST);

CREATE TABLE "roze_1/cvr".desires (
  "clientGroupID"      TEXT,
  "clientID"           TEXT,
  "queryHash"          TEXT,
  "patchVersion"       TEXT NOT NULL,
  "deleted"            BOOL,  -- put vs del "desired" query
  "ttl"                INTERVAL,  -- DEPRECATED: Use ttlMs instead. Time to live for this client
  "ttlMs"              DOUBLE PRECISION,  -- Time to live in milliseconds
  "inactivatedAt"      TIMESTAMPTZ,  -- DEPRECATED: Use inactivatedAtMs instead. Time at which this row was inactivated
  "inactivatedAtMs"    DOUBLE PRECISION,  -- Time at which this row was inactivated (milliseconds since client group start)

  PRIMARY KEY ("clientGroupID", "clientID", "queryHash"),

  CONSTRAINT fk_desires_query
    FOREIGN KEY("clientGroupID", "queryHash")
    REFERENCES "roze_1/cvr".queries("clientGroupID", "queryHash")
    ON DELETE CASCADE
);

-- For catchup patches.
CREATE INDEX desires_patch_version
  ON "roze_1/cvr".desires ("patchVersion");

CREATE INDEX desires_inactivated_at
  ON "roze_1/cvr".desires ("inactivatedAt");

CREATE TABLE "roze_1/cvr"."rowsVersion" (
  "clientGroupID" TEXT PRIMARY KEY,
  "version"       TEXT NOT NULL
);

CREATE TABLE "roze_1/cvr".rows (
  "clientGroupID"    TEXT,
  "schema"           TEXT,
  "table"            TEXT,
  "rowKey"           JSONB,
  "rowVersion"       TEXT NOT NULL,
  "patchVersion"     TEXT NOT NULL,
  "refCounts"        JSONB,  -- {[queryHash: string]: number}, NULL for tombstone

  PRIMARY KEY ("clientGroupID", "schema", "table", "rowKey"),

  CONSTRAINT fk_rows_client_group
    FOREIGN KEY("clientGroupID")
    REFERENCES "roze_1/cvr"."rowsVersion" ("clientGroupID")
    ON DELETE CASCADE
);

-- For catchup patches.
CREATE INDEX row_patch_version 
  ON "roze_1/cvr".rows ("patchVersion");

-- For listing rows returned by one or more query hashes. e.g.
-- SELECT * FROM cvr_shard.rows WHERE "refCounts" ?| array[...queryHashes...];
CREATE INDEX row_ref_counts ON "roze_1/cvr".rows 
  USING GIN ("refCounts");
