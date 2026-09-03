-- Writing PostgreSQL role and schema for sliding sync's per-connection state.
--
-- Sliding sync is not a read-only workload and cannot be made into one. Every
-- response records what it sent so the next request can send only the
-- difference, and even READING that state writes: Synapse's
-- `_get_and_clear_connection_positions_txn` bumps `last_used_ts`, deletes every
-- other position on the connection, and rewrites lazy-member rows before it
-- returns anything. The `pos` token a client carries is literally a sequence
-- value from `sliding_sync_connection_positions` -- it cannot be minted without
-- an INSERT.
--
-- So this worker gets a second writing role, its second ever, and the same
-- containment argument as device-inbox-role.sql applies: the grant is narrow,
-- it lives in one package (internal/slidingstore) behind its own pool, and the
-- worker VERIFIES the narrowness at startup rather than trusting this file was
-- run as written.
--
-- The narrowness here is a whole schema rather than one table. `gosync_ss`
-- owns everything in `gosync` and has NOTHING in `public` -- it cannot read
-- Synapse's tables, let alone write them. The main pool keeps the read-only
-- role and its check, so "every query in internal/store is a SELECT" stays
-- literally true.
--
-- WHY OUR OWN TABLES RATHER THAN SYNAPSE'S
--
-- Synapse has these six tables already, and writing to them would let a client
-- move between this worker and Synapse's with its `pos` intact. That was
-- considered and rejected (docs/decisions.md, 2026-09-03): it means a bug in
-- our connection bookkeeping corrupts the state of real clients on the real
-- hostname. Ours are separate, so the blast radius of a mistake is the split
-- host and nothing else. The cost is that our `pos` is not interchangeable
-- with Synapse's -- a client switching hosts mid-connection gets
-- M_UNKNOWN_POS and re-bootstraps, which is a supported path that 9.3% of live
-- requests already exercise.
--
-- The shape below mirrors Synapse's (schema deltas 87/02 and 93/02) so the
-- port stays readable against the original. Where it differs, it says so.
--
-- Run as a superuser against the Synapse database:
--   psql -h /var/sockets -U synapse -d synapse-db -f sliding-sync-role.sql
--
-- To undo:  DROP SCHEMA gosync CASCADE; DROP ROLE gosync_ss;

BEGIN;

-- Idempotent throughout, so re-running after a schema change is safe. CREATE
-- ROLE has no IF NOT EXISTS.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gosync_ss') THEN
        CREATE ROLE gosync_ss WITH LOGIN;
    END IF;
END $$;

GRANT CONNECT ON DATABASE "synapse-db" TO gosync_ss;

CREATE SCHEMA IF NOT EXISTS gosync AUTHORIZATION gosync_ss;

-- Deliberately NOT granted: USAGE on schema public. Without it this role
-- cannot so much as name Synapse's tables, which is the guarantee the startup
-- check verifies. Note that `gosync` must therefore be the whole search_path.
ALTER ROLE gosync_ss SET search_path = gosync;
ALTER ROLE gosync_ss SET statement_timeout = '60s';

SET search_path = gosync;

-- One row per logical client connection. Element X runs THREE per device
-- (conn_id "room-list", "notifications", and ""), so this is keyed by the
-- triple and not by the device.
CREATE TABLE IF NOT EXISTS sliding_sync_connections (
    connection_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id TEXT NOT NULL,
    effective_device_id TEXT NOT NULL,
    conn_id TEXT NOT NULL,
    created_ts BIGINT NOT NULL,
    last_used_ts BIGINT
);
CREATE INDEX IF NOT EXISTS sliding_sync_connections_idx
    ON sliding_sync_connections (user_id, effective_device_id, conn_id);
CREATE INDEX IF NOT EXISTS sliding_sync_connections_ts_idx
    ON sliding_sync_connections (created_ts);
CREATE INDEX IF NOT EXISTS sliding_sync_connections_last_used_ts_idx
    ON sliding_sync_connections (last_used_ts) WHERE last_used_ts IS NOT NULL;

-- The `pos` token's left half. Identity, because the client only ever hands it
-- back to us and it must be unguessable-adjacent and monotonic; ownership is
-- checked against the connection triple on every use, since a `pos` is a
-- user-supplied value.
--
-- Normally at most two rows live per connection: the one the client is using
-- and the one we just wrote. Reading a position deletes the others, which is
-- what keeps these tables from growing without bound -- see the note on
-- forking below.
CREATE TABLE IF NOT EXISTS sliding_sync_connection_positions (
    connection_position BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    connection_key BIGINT NOT NULL
        REFERENCES sliding_sync_connections (connection_key) ON DELETE CASCADE,
    created_ts BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS sliding_sync_connection_positions_key
    ON sliding_sync_connection_positions (connection_key);
CREATE INDEX IF NOT EXISTS sliding_sync_connection_positions_ts_idx
    ON sliding_sync_connection_positions (created_ts);

-- required_state configs, deduplicated per connection. Many rooms share one
-- config, and on the live server this is the largest of the six tables by row
-- count even so.
CREATE TABLE IF NOT EXISTS sliding_sync_connection_required_state (
    required_state_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    connection_key BIGINT NOT NULL
        REFERENCES sliding_sync_connections (connection_key) ON DELETE CASCADE,
    required_state TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS sliding_sync_connection_required_state_conn_pos
    ON sliding_sync_connection_required_state (connection_key);

-- What we told the connection about each room, per position. Rows are copied
-- forward from the previous position and then the changes upserted, so each
-- position is a complete snapshot. Measured on the live server: ~248 rows per
-- position for a 654-room connection.
CREATE TABLE IF NOT EXISTS sliding_sync_connection_room_configs (
    connection_position BIGINT NOT NULL
        REFERENCES sliding_sync_connection_positions (connection_position) ON DELETE CASCADE,
    room_id TEXT NOT NULL,
    timeline_limit BIGINT NOT NULL,
    required_state_id BIGINT NOT NULL
        REFERENCES sliding_sync_connection_required_state (required_state_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS sliding_sync_connection_room_configs_idx
    ON sliding_sync_connection_room_configs (connection_position, room_id);
CREATE INDEX IF NOT EXISTS sliding_sync_connection_room_configs_required_state_id_idx
    ON sliding_sync_connection_room_configs (required_state_id);

-- "Have I sent this room down this stream, and if so up to what point?"
-- room_status is 'live' or 'previously'; last_token is set only for
-- 'previously'. Same copy-forward-then-upsert as room_configs; ~725 rows per
-- position for that same 654-room connection.
CREATE TABLE IF NOT EXISTS sliding_sync_connection_streams (
    connection_position BIGINT NOT NULL
        REFERENCES sliding_sync_connection_positions (connection_position) ON DELETE CASCADE,
    stream TEXT NOT NULL,
    room_id TEXT NOT NULL,
    room_status TEXT NOT NULL,
    last_token TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS sliding_sync_connection_streams_idx
    ON sliding_sync_connection_streams (connection_position, room_id, stream);

-- Which lazily-loaded memberships this connection has already been given.
--
-- Explicitly a CACHE: dropping a row costs a member event sent twice, never a
-- wrong answer, so entries may be evicted freely. connection_position is NULL
-- for entries a client has confirmed (they hold for every future position on
-- the connection) and set for entries written against a position not yet
-- acknowledged, which is how a forked request is unwound.
CREATE TABLE IF NOT EXISTS sliding_sync_connection_lazy_members (
    connection_key BIGINT NOT NULL
        REFERENCES sliding_sync_connections (connection_key) ON DELETE CASCADE,
    connection_position BIGINT
        REFERENCES sliding_sync_connection_positions (connection_position) ON DELETE CASCADE,
    room_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    last_seen_ts BIGINT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS sliding_sync_connection_lazy_members_idx
    ON sliding_sync_connection_lazy_members (connection_key, room_id, user_id);
CREATE INDEX IF NOT EXISTS sliding_sync_connection_lazy_members_conn_pos_idx
    ON sliding_sync_connection_lazy_members (connection_position)
    WHERE connection_position IS NOT NULL;
CREATE INDEX IF NOT EXISTS sliding_sync_connection_lazy_members_pos_idx
    ON sliding_sync_connection_lazy_members (connection_key, connection_position)
    WHERE connection_position IS NOT NULL;

-- The tables above were created by the superuser running this file, so they are
-- owned by it, not by gosync_ss -- and a schema owner has no rights over
-- objects inside it that it does not own. Without this the role can reach its
-- own schema and nothing in it. Ownership rather than GRANT ALL because the
-- identity sequences behind `connection_key` and `connection_position` follow
-- the table, and a grant would have to enumerate them separately.
ALTER TABLE sliding_sync_connections               OWNER TO gosync_ss;
ALTER TABLE sliding_sync_connection_positions      OWNER TO gosync_ss;
ALTER TABLE sliding_sync_connection_required_state OWNER TO gosync_ss;
ALTER TABLE sliding_sync_connection_room_configs   OWNER TO gosync_ss;
ALTER TABLE sliding_sync_connection_streams        OWNER TO gosync_ss;
ALTER TABLE sliding_sync_connection_lazy_members   OWNER TO gosync_ss;

COMMIT;

-- Verify: the first succeeds, the rest must all fail.
--   psql -h /var/sockets -U gosync_ss -d synapse-db \
--     -c 'select count(*) from sliding_sync_connections;'
--   psql -h /var/sockets -U gosync_ss -d synapse-db -c 'select count(*) from public.events;'
--   psql -h /var/sockets -U gosync_ss -d synapse-db \
--     -c 'select count(*) from public.sliding_sync_connections;'
--   psql -h /var/sockets -U gosync_ss -d synapse-db -c 'delete from public.device_inbox;'
