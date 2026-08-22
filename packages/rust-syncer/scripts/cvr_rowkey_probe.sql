-- CVR row-key poison probe (one-shot, auto-discovers the CVR schema).
--
-- Detects the "Got undefined" client crash class: a stored CVR rowKey whose
-- column-set does not match its table's client primary key (e.g. a rowKey keyed
-- by a surrogate `id` for a table whose client PK is [channelId,userId]). A
-- healthy table has exactly ONE rowKey column-set (= its primary key); a
-- poisoned table shows a short/extra/second set.
--
-- Intended as an ART post-run gate and an ops diagnostic. Run against the CVR
-- Postgres (the ZERO_CVR_DB database), read-only:
--
--   PGURL="$(kubectl -n <ns> get secret <cvr-secret> -o jsonpath='{.data.DATABASE_URL}' | base64 -d)"
--   psql "$PGURL" -f cvr_rowkey_probe.sql
--
-- To turn this into a hard gate, fail the run if any table reports more than one
-- rowkey_columns set (or a set that omits a known client-PK column).

\echo === CVR schema(s) found ===
SELECT nspname FROM pg_namespace WHERE nspname LIKE '%cvr%' ORDER BY nspname;

\echo === rowKey column-sets per table (flag any table with a short set or >1 set) ===
SELECT format(
  'SELECT %L AS cvr_schema, "table",'
  '  (SELECT array_agg(k ORDER BY k) FROM jsonb_object_keys("rowKey") AS k) AS rowkey_columns,'
  '  count(*) AS n_rows'
  ' FROM %I.rows GROUP BY "table", rowkey_columns ORDER BY "table", rowkey_columns',
  nspname, nspname)
FROM pg_namespace
WHERE nspname LIKE '%cvr%'
\gexec
