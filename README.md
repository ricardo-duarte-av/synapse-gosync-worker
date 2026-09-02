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
| `/_matrix/client/*/sync` | **initial and incremental served**, at parity for both test accounts, with or without MSC4222 `state_after`, and with or without a filter |
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

**`unread_thread_notifications` is not implemented.** A filter may set it
(`msc3773_enabled` is on for this deployment, so both the stable and unstable
spellings are live), and we parse it, but we do not act on it: thread
notification counts are still folded into the room's `notification_count` and
`highlight_count`, exactly as they are when the flag is absent, and the
`unread_thread_notifications` key is not emitted. The counts are therefore
*higher* than Synapse's for a client that asked for the split. Per-thread counts
belong with the rest of the notification work in M7; parsing the flag without
honouring it is recorded here rather than left to be discovered.

**The `msc4354_sticky` room section is not implemented.** MSC4354 sticky events
are served in the timeline like any other event, and the per-event
`unsigned.msc4354_sticky_duration_ttl_ms` is emitted, but the separate `sticky`
section a room carries is not built. Synapse removes from that section any
sticky event already in the room's timeline, so the omission is invisible while
the event is recent and appears once it ages out of the timeline — or as soon
as a filter excludes it. Implementing it will also move `next_batch`: Synapse
rewrites the sticky field of its own now_token to the last row it returns.

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
