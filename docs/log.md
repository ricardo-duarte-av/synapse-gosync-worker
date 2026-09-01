# Work log

Newest first. Numbers are measurements, not estimates.

## 2026-09-01 — M1 complete: `/rooms/{roomId}/initialSync` at parity

**9 of 9 joined rooms match Synapse exactly, at limits 1, 5, 20 and 100.**
That includes the two large federated rooms the test account was already in —
hundreds of state events, bridged users, bans, displayname changes, room
version 10 — not just the seven purpose-built ones.

Built: the store queries (current state, recent events, receipts, room account
data, tags, presence, membership timeline), `internal/visibility`,
`internal/handlers`, and `cmd/syncdiff`.

**`cmd/syncdiff` is the deliverable that matters.** It pins both halves of the
question:

1. Ask the reference worker; read the `end` token it minted, and recover the
   exact millisecond it used from any event (`origin_server_ts + unsigned.age`).
2. Ask ours for the same window *and the same instant*, via `_gosync_now` and
   `_gosync_time_now`.
3. Diff structurally — ordered for the timeline, set-wise for state, presence,
   receipts and account data.

*Verified non-vacuous:* building the worker with the `/sync` serialisation
format instead of the legacy v1 one made syncdiff report every missing `age` and
`user_id`, name the exact events, and exit non-zero.

**Four defects the comparator caught**, none of which a unit test could have
found, all written up in [synapse-notes.md](synapse-notes.md):

- `unsigned` is rebuilt from a six-field allowlist; a stored remote-supplied
  `age` must be dropped. We were emitting ages from 2023.
- `unsigned.membership` is the caller's membership *at each event*, not now.
- `prev_content` on state events is Synapse's shared-cache leaking; tolerated
  upstream-only, never emitted.
- Presence is emitted for joined members with no stored presence row.

Plus one bug of our own making: an over-strict visibility guard that refused any
window spanning the user's join. Synapse's lax path never consults membership.

**Deliberate gaps**, both answered with 501 rather than a wrong answer:

- Rooms the user has *left* (`_room_initial_sync_parted` needs the room state at
  the leave event).
- Rooms needing per-event state: history visibility other than `shared`/
  `world_readable`, visibility that has changed, an erased sender, or a
  retention policy.

## 2026-08-31 — M1: stream tokens and event serialisation

**`internal/streamtoken`.** The 14-field token, and all three `RoomKey`
encodings (`s` live, `t` topological, `m` vector clock). Ten tokens pulled live
from `av-sync-worker-2` round-trip byte-exactly (`TestLiveTokensRoundTrip`).

*What bit us:* the vector clock's instance entries are **order-sensitive**.
Synapse serialises by iterating a Python dict, which preserves insertion order,
and on parse that order is the order in the token string. The first
implementation used a `map[int]int64` and sorted on output — which produces a
semantically identical token that compares *unequal*. That would have surfaced
later as a parity mismatch that means nothing. `Instances` is an ordered slice.

*Also:* instance ids are kept as ids rather than resolved to worker names
through the `instance_map` table. The round trip is identical either way, but
keeping ids means a token can be parsed, modified and re-serialised without
touching PostgreSQL — and the commonest operation, replacing only the room key,
happens on every paginated response.

**`internal/clientevent`.** Serialisation for all three client formats, built by
splicing stored JSON with `sjson` rather than unmarshalling and re-encoding.

**Result: 126 events across 7 rooms, both room versions, zero mismatches**
against what `av-sync-worker-2` actually returned
(`TestLiveSerializationMatchesSynapse`).

*The trick that made `age` checkable:* Synapse stamps `age = time_now - age_ts`,
and its `time_now` is unknowable after the fact — but recoverable, because
`age + origin_server_ts` is exactly the millisecond it used. Feeding that back
makes `age` a compared field instead of an ignored one. This is the same move as
pinning `now_token`, applied to the clock.

*Confirmed not vacuous:* all five room-version-12 create events genuinely have
no `room_id` in `event_json.json`, yet our output matched Synapse's, which has
one. The MSC4291 restoration path really ran.

## 2026-08-31 — the database cannot mint a `now_token`

Measured 12 of 14 token fields reproducible by `SELECT MAX(...)`; typing not at
all, `push_rules` and `thread_subscriptions` drifting. Full table and the
architectural consequence in [tokens.md](tokens.md).

## 2026-08-31 — deployment findings

- **Room version 12 is the default on this server.** A v12 room ID is the hash
  of its create event, so that event carries no `room_id`.
- **The legacy endpoints use a different serialiser from `/sync`** —
  `format_event_for_client_v1` emits `user_id` and a top-level `age`.
- `/login` against `av-request-worker-1.sock` returns `M_UNKNOWN`; the public
  HTTPS endpoint works. Non-sync endpoints go through
  `https://aguiarvieira.pt/_matrix/` so nginx routes them to the right worker
  and they appear in the logs. Only sync endpoints are reached directly on
  `av-sync-worker-2.sock`.
- A test token was printed to a terminal during setup and was immediately
  invalidated with `/logout`. If a token reaches a log, rotate it rather than
  reasoning about who saw it.

## 2026-08-31 — foundation

`config` (strict YAML, unknown fields rejected), `store` (pgxpool on the
existing read-only `gopro_ro` role, verified with `SHOW
default_transaction_read_only`), `auth` (whoami over a unix socket, LRU +
singleflight), `matrixerr`, `metrics`, `server` (listener, request log, router),
and `cmd/gosync-worker` with `-check`, `-healthcheck` and `-version`.

Verified live: `-check` reports `database_read_only=true`; the worker serves
`/health` over its unix socket at mode 660, answers `M_UNRECOGNIZED` for
unimplemented endpoints as Synapse does, and shuts down cleanly on SIGTERM.
