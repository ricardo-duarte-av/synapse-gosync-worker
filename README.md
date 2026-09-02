# synapse-gosync-worker

A Go reimplementation of Synapse's classic `/sync` worker. It reads Synapse's
PostgreSQL directly and follows Synapse's replication stream over Redis.

Sliding sync and simplified sliding sync are out of scope.

Nothing is routed to this worker. It is driven deliberately by `cmd/syncdiff`
and a test account, and compared against a real Synapse sync worker, until its
answers are trusted.

It follows Synapse's replication stream over Redis (SUBSCRIBE only), which is
what lets it report typing, hold accurate stream positions, and long-poll.

Client filters are honoured, lazy-loaded members included — which is what a
real client's `/sync` request actually looks like.

## Status

| Endpoint | State |
|---|---|
| `/_matrix/client/*/rooms/{roomId}/initialSync` | **served**, at parity on all 39 rooms of two test accounts |
| `/_matrix/client/*/initialSync` | **served**, at parity for both test accounts (except `archived=true`) |
| `/_matrix/client/*/sync` | **initial and incremental served**, at parity for both test accounts, with or without MSC4222 `state_after`, and with or without a filter. `to_device` is served only when configured; see below |
| `/_matrix/client/*/events` | not implemented |

### Deliberate deviations

Both are answered with `501` rather than approximated, on the principle that a
loud failure beats a quiet wrong answer:

- **Rooms the user has left**, including `/initialSync?archived=true`. Synapse
  serves a snapshot of the room state as it was at the leave event. The state
  resolver can now answer that; the handler has not been wired to it.
- **An erased sender's event that should be served pruned.** When the caller was
  not joined at the time, Synapse returns a redacted copy; we drop the event.
  The per-room-version prune exists, so this is a wiring job. Dropping withholds
  content rather than publishing content that should have been stripped.

**`to_device` is served only when the worker can delete.** Synapse's `/sync`
deletes the to-device messages a device has acknowledged, bounded by its
`since`. A worker that served the section without deleting would hand a client
the same room keys on every sync for ever, so the two are one setting rather
than two: with `to_device.enabled: false` the section is omitted entirely, which
is correct as long as a real Synapse worker is also syncing that device.

Enabling it is the one place this worker writes. It opens a *second* connection
as a role granted `SELECT, DELETE` on `device_inbox` and nothing else
(`deploy/device-inbox-role.sql`), verifies at startup that the role really is
that narrow — refusing to run if it can delete from `events` or insert into
`device_inbox` — and keeps the main pool on the read-only role. Every query in
`internal/store` is still a `SELECT`.

**One upstream bug is deliberately not reproduced.** Synapse's
`_get_unread_counts_by_pos_txn` adds a room's post-rotation main-timeline counts
to a leftover loop variable rather than to the main timeline, so they land on
whichever thread came last in an unordered result set. It is invisible unless a
client asks for per-thread counts, and it is not reproducible even in principle
— there is no ORDER BY to agree with. We attribute main-timeline counts to the
main timeline.

Filter **validation** is looser than Synapse's. Synapse runs the filter through
a JSON schema and `parse_and_validate_server_name`; we check the shapes and the
sigils but take a permissive view of the domain half of a user or room ID. The
consequence is one-directional and chosen: a deliberately malformed-but-
plausible domain is accepted here and rejected there, so we never turn a filter
Synapse would accept into a `400`.

One field is deliberately **not** emitted where Synapse sometimes emits it:
`prev_content` on *state* events. Synapse's `events_worker` writes that field
into its shared event cache, so whether it appears depends on which request
happened to load the event first. It is not reproducible, and reproducing it is
not possible without reproducing the cache.

### Not reproducible by anyone

Two differences are not deviations but coin flips, and are counted by name by
the comparator rather than treated as either matches or mismatches. Both are
explained in `docs/comparability.md`:

- **Which of two colliding events fills a state key.** When `_calculate_state`
  holds two events for one `(type, state_key)`, Synapse picks between them by
  iterating a Python set, whose order is randomised per process. Its own answer
  changes when the worker restarts.
- **Which lazy-loaded members a sync repeats.** That depends on what the serving
  process has already sent to that device, held only in memory. An initial sync
  clears the cache first and is therefore deterministic; an incremental one is
  not.

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
GOSYNC_TEST_TODEVICE_DSN="host=/var/sockets user=gosync_inbox dbname=synapse-db" \
  go test ./... -run Live -v
```

## Configuration

See `deploy/gosync-worker.example.yaml`, which documents every field and why it
is set the way it is. The worker needs a read-only PostgreSQL role
(`deploy/readonly-role.sql`), a Redis/KeyDB socket, and a Synapse client-API
endpoint to validate access tokens against. Serving `to_device` additionally
needs the narrow deleting role in `deploy/device-inbox-role.sql`.

## Documentation

`CLAUDE.md` is the index. The reasoning lives in `docs/` — start with
`docs/comparability.md`, which explains why `/sync` cannot be compared the way a
normal endpoint can, and what this project does about it.
