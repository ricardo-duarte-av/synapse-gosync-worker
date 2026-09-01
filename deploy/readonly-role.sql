-- Read-only PostgreSQL role for the gosync worker.
--
-- NOT used on aguiarvieira.pt, which reuses synapse-gopro-worker's `gopro_ro`:
-- it already grants SELECT on every table with default_transaction_read_only
-- set, and a second identical role would only be another thing to keep in step
-- with schema migrations. Kept for a deployment that has no such role.
--
-- The worker only ever reads. This role makes that a guarantee enforced by the
-- database rather than a property of the code: default_transaction_read_only
-- means even a bug that issues a write cannot commit one.
--
-- Run as a superuser against the Synapse database:
--   psql -h /var/sockets -U synapse -d synapse-db -f readonly-role.sql
--
-- To undo:  DROP OWNED BY gosync_ro; DROP ROLE gosync_ro;

BEGIN;

CREATE ROLE gosync_ro WITH LOGIN;

GRANT CONNECT ON DATABASE "synapse-db" TO gosync_ro;
GRANT USAGE ON SCHEMA public TO gosync_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO gosync_ro;

-- Covers tables added by future Synapse schema migrations.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO gosync_ro;

-- Belt and braces: reject writes at the transaction level.
ALTER ROLE gosync_ro SET default_transaction_read_only = on;

-- A query that outlives its request is pure waste.
ALTER ROLE gosync_ro SET statement_timeout = '60s';

COMMIT;

-- Verify: the first succeeds, the second must fail.
--   psql -h /var/sockets -U gosync_ro -d synapse-db -c 'select count(*) from rooms;'
--   psql -h /var/sockets -U gosync_ro -d synapse-db -c 'create table t(x int);'
