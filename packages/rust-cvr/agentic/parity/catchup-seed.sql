-- Shared seed for the catchup live-Postgres differential. Both the TS golden
-- generator and the Rust pg-test run this verbatim so they operate on byte-
-- identical CVR state. Schema is dropped + recreated for idempotency.
--
-- Scenario (clientGroupID = 'cg1', head version = '0c'):
--   r1 id=1  pv=02  refCounts {qA:1}
--   r2 id=2  pv=04  refCounts {qB:1}
--   r3 id=3  pv=06  refCounts {qA:1,qB:1}
--   r4 id=4  pv=08  refCounts {qA:1}   <- POISONED rowKey (carries a non-PK col)
--   r5 id=5  pv=0a  refCounts NULL     <- tombstone
--   r6 id=6  pv=0c  refCounts {qA:1}
DROP SCHEMA IF EXISTS cvr_parity CASCADE;
CREATE SCHEMA cvr_parity;

CREATE TABLE cvr_parity.instances (
  "clientGroupID"  TEXT PRIMARY KEY,
  "version"        TEXT NOT NULL,
  "lastActive"     TIMESTAMPTZ NOT NULL,
  "replicaVersion" TEXT,
  "owner"          TEXT,
  "grantedAt"      TIMESTAMPTZ,
  "clientSchema"   JSONB,
  "profileID"      TEXT,
  "deleted"        BOOL DEFAULT FALSE
);

CREATE TABLE cvr_parity.rows (
  "clientGroupID"  TEXT,
  "schema"         TEXT,
  "table"          TEXT,
  "rowKey"         JSONB,
  "rowVersion"     TEXT NOT NULL,
  "patchVersion"   TEXT NOT NULL,
  "refCounts"      JSONB,
  PRIMARY KEY ("clientGroupID", "schema", "table", "rowKey")
);

INSERT INTO cvr_parity.instances ("clientGroupID", "version", "lastActive")
VALUES ('cg1', '0c', now());

INSERT INTO cvr_parity.rows
  ("clientGroupID","schema","table","rowKey","rowVersion","patchVersion","refCounts")
VALUES
  ('cg1','public','issue','{"id":"1"}','r1','02','{"qA":1}'),
  ('cg1','public','issue','{"id":"2"}','r2','04','{"qB":1}'),
  ('cg1','public','issue','{"id":"3"}','r3','06','{"qA":1,"qB":1}'),
  ('cg1','public','issue','{"id":"4","_leaked":"oops"}','r4','08','{"qA":1}'),
  ('cg1','public','issue','{"id":"5"}','r5','0a',NULL),
  ('cg1','public','issue','{"id":"6"}','r6','0c','{"qA":1}');
