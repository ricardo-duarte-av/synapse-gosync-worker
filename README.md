# synapse-gosync-worker

A Go reimplementation of Synapse's classic `/sync` worker. It reads Synapse's
PostgreSQL directly and follows Synapse's replication stream over Redis.

Sliding sync and simplified sliding sync are out of scope.

Nothing is routed to this worker. It is driven deliberately by `cmd/syncdiff`
and a test account, and compared against a real Synapse sync worker, until its
answers are trusted.

## Status

| Endpoint | State |
|---|---|
| `/_matrix/client/*/rooms/{roomId}/initialSync` | **served**, at parity with Synapse on all 9 test rooms |
| `/_matrix/client/*/initialSync` | not implemented |
| `/_matrix/client/*/sync` | not implemented |
| `/_matrix/client/*/events` | not implemented |

### Deliberate deviations

Both are answered with `501` rather than approximated, on the principle that a
loud failure beats a quiet wrong answer:

- **Rooms the user has left.** Synapse serves a snapshot of the room state as it
  was at the leave event; resolving that needs state groups, which are not built
  yet.
- **Rooms needing per-event state resolution.** History visibility other than
  `shared` or `world_readable`, a room whose visibility has changed over its
  life, a room with an erased sender, or a room with a retention policy.

One field is deliberately **not** emitted where Synapse sometimes emits it:
`prev_content` on *state* events. Synapse's `events_worker` writes that field
into its shared event cache, so whether it appears depends on which request
happened to load the event first. It is not reproducible, and reproducing it is
not possible without reproducing the cache.

## Building and testing

```sh
go build ./...
go test -race ./...
```

Live tests are gated on environment variables and skip without them, so they
never fail CI:

```sh
GOSYNC_TEST_DSN="host=/var/sockets user=gopro_ro dbname=synapse-db" \
GOSYNC_LIVE_REF_SOCKET=/var/sockets/nginx/av-sync-worker-2.sock \
GOSYNC_LIVE_TOKEN_FILE=~/.gosync-test-token \
  go test ./... -run Live -v
```

## Configuration

See `deploy/gosync-worker.example.yaml`, which documents every field and why it
is set the way it is. The worker needs a read-only PostgreSQL role
(`deploy/readonly-role.sql`), a Redis/KeyDB socket, and a Synapse client-API
endpoint to validate access tokens against.

## Documentation

`CLAUDE.md` is the index. The reasoning lives in `docs/` — start with
`docs/comparability.md`, which explains why `/sync` cannot be compared the way a
normal endpoint can, and what this project does about it.
