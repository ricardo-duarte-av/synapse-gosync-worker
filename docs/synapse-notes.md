# Things Synapse's source does not tell you

Each of these would have been a bug if the Python had been followed
literally, or was found only by querying the live database.

- **Room version 12 is the default on this server**, and a v12 room ID is
  derived from the hash of its create event — so the **create event carries no
  `room_id`**, while every other event in the room does. Code that requires
  `room_id` unconditionally fails on a well-formed v12 create event. The test
  rooms deliberately span v12 and v10 for this reason.
- **The legacy endpoints serialise events differently from `/sync`.**
  `/rooms/{id}/initialSync` uses `format_event_for_client_v1`, which emits
  **`user_id`** (a duplicate of `sender`) and a top-level **`age`** alongside
  `unsigned.age`. `/sync` uses `format_event_for_client_v2_without_room_id`,
  which emits neither and drops `room_id`. Observed on the live reference
  worker; the full key set for v1 is
  `age, content, event_id, origin_server_ts, room_id, sender, state_key, type,
  unsigned, user_id`.
- **`messages.start` is a topological token, `messages.end` is a stream token.**
  A real pair from the reference worker:
  `t12-13908046_286691157_…` and `s13908064_286691157_…`. Both are full
  `StreamToken`s; only the leading `room_key` differs in form.
- **The stream token has 14 underscore-joined fields**, in a fixed order:
  `room_key, presence, typing, receipt, account_data, push_rules, to_device,
  device_list, groups (unused), un_partial_stated_rooms, thread_subscriptions,
  sticky_events, quarantined_media, profile_updates`
  (`types/__init__.py:1143`). `from_string` **right-pads missing trailing fields
  with `"0"`**, so an old short token still parses. Fields 1, 4, 8 and 13 are
  not plain integers: `room_key` is a `RoomStreamToken` (`s…`, `t…-…`, or the
  vector clock `m56~2.58~3.59`, whose integers are *instance ids* resolved
  through the database, not instance names), and 4/8/13 are
  `MultiWriterStreamToken`s.
- **Access tokens are not hashed.** `_query_for_auth`
  (`storage/databases/main/registration.py:766`) is a plain `WHERE token = ?`.
  We still do not use it — see §5.
- **On a worker, the notifier is driven entirely by replication.** A worker
  never learns of a new event by persisting one. Without the Redis subscription
  `/sync` would still answer correctly, but only ever on timeout — it would look
  merely slow, not broken.
- **`wait_for_stream_token` exists for a reason** (`notifier.py:835`). A `since`
  minted by a worker ahead of us must be waited for — polling
  `get_current_token()` every 500ms, capped at 10s — before answering, or we
  serve a stale sync. `bound_future_token` (`streams/events.py:110`) clamps
  tokens from the future rather than trusting them.

## Found by the comparator, on 2026-09-01

Four defects that unit tests could never have caught, because each needed a real
room whose history had something in it that the test rooms did not. All four
were invisible until `syncdiff` was pointed at the account's *other* rooms.

### `unsigned` is rebuilt from a six-field allowlist, not passed through

Synapse models `unsigned` as a typed struct — `age_ts`, `replaces_state`,
`invite_room_state`, `knock_room_state`, `prev_content`, `prev_sender`
(`rust/src/events/unsigned.rs`) — so anything else in storage is **dropped when
the event is loaded**.

This is not a formality. Remote servers send `age` in `unsigned`, and Synapse
*stores* it verbatim. Splicing the stored object through therefore emits a
**stale age, baked in at receive time**, on events for which Synapse emits no
age at all. Three federated membership events here carried `{"age": 310}` and
`{"age": 4}` from 2025 and 2023.

Every locally created event agreed, because those store `age_ts` and no `age` —
which is exactly why seven test rooms passed and two real ones did not.

### `unsigned.membership` is per-event, not current

MSC4115's `unsigned.membership` is the **caller's membership at that event**.
Stamping their current membership is right only for a user who has been joined
since the room was created; in a room joined later, Synapse reports `leave` for
every event before the join, and we reported `join`.

Synapse resolves it through state groups, but for a single user the answer is
far cheaper: their membership at an event is the most recent `m.room.member`
event for them at or before it. There is only ever one state key in play, so
state-group resolution buys nothing. See `Store.UserMembershipTimeline`.

### `prev_content` on state events is Synapse's cache leaking

`get_recent_events_for_room` asks for `prev_content`; `get_current_state` does
not. Synapse nevertheless emits it on *some* state events, because
`events_worker` writes the field into the **shared cached event** — its own
comment says "This mutates the cached event, but that's fine" — and a later
state read of that cached event carries it.

Whether it appears depends on whether some other request happened to load that
event first. It is not reproducible and not ours to imitate. `syncdiff`
tolerates it **upstream-only** and counts it; emitting it where Synapse does not
would still be our bug, and is still reported.

This is the same hazard gopro's CLAUDE.md §4 recorded from the federation side.

### Presence is emitted for members with no stored state

Synapse's presence handler substitutes a default `{"presence": "offline"}` for a
joined member with no `presence_stream` row, rather than omitting them. Four of
28 members in the largest test room had no row, and omitting them lost four
entries from the response.

### The membership guard that was simply wrong

An early version of `internal/visibility` refused any window in which the user's
membership changed. That was a misreading: Synapse's lax path
(`_check_history_visibility`) returns ALLOWED **without consulting membership at
all**. `shared` means joined members see all history, including history from
before they joined. The guard refused every room whose window reached back past
the user's own join — which is every room, at a large enough limit.

## Found by the comparator, on 2026-09-01 (M2, `/initialSync`)

The two initialSync endpoints are less alike than they look. Four differences,
all of which produced mismatches on the first run.

### `/initialSync` never reveals `transaction_id`

`_snapshot_all_rooms` builds its serializer config **without a requester**
(`initial_sync.py:171`), while `_room_initial_sync_joined` passes
`requester=requester`. `unsigned.transaction_id` is gated on the requester
matching the sender, so `/initialSync` omits it always and
`/rooms/{roomId}/initialSync` includes it for your own events.

It reads like an upstream oversight. It is still the behaviour to match.

### The two endpoints use different receipt queries

- `_get_linearized_receipts_for_room` (singular) — used by the per-room
  endpoint — does not even `SELECT thread_id`, and emits the stored data
  unchanged.
- `_get_linearized_receipts_for_rooms` (plural) — used by `/initialSync` —
  selects `thread_id` and merges through `ReceiptInRoom.merge_to_content`, which
  stamps `thread_id` into threaded receipts and applies MSC4102 (an unthreaded
  receipt replaces a threaded one for the same user and event).

So the same receipt is rendered two ways depending on which endpoint you ask.

**And the two can contaminate each other.** The plural method is a
`@cachedList` over the singular one, so a plural call populates the singular
method's cache. When both are called with the same receipt token, whichever ran
first decides the shape for both. Receipts advance constantly here, so the
tokens usually differ and each endpoint runs its own query — but the collision
is real, and `syncdiff` tolerates `thread_id` in either direction because of it.

### `/initialSync` re-reads its clock per room

`time_now = self.clock.time_msec()` sits **inside** `handle_room`, and there is
another reading at the end for presence. Synapse's own response is therefore not
internally consistent: two rooms in one snapshot carry ages computed
milliseconds apart.

This bounds what pinning can achieve. However the clock pin is chosen, a single
instant cannot reproduce a response built from several. `syncdiff` accepts
age-like fields within 1000ms and reports the largest gap seen — 14–18ms in
practice. That is far tighter than any real defect: a wrong `age_ts`, a stale
stored `age`, or a missing age are off by hours or years, not milliseconds.

### `visibility` in `/initialSync` is the room directory, not history visibility

Each room carries `"visibility": "public" | "private"`, from `rooms.is_public` —
whether the room is published in the directory. Nothing to do with
`m.room.history_visibility`, which governs who may read history. Two unrelated
concepts, one word.

### Reading Synapse's source

| What | Where |
|---|---|
| `/sync` servlet | `synapse/rest/client/sync.py:72` (`SyncRestServlet`), response at `:297` |
| Sync algorithm | `synapse/handlers/sync.py:1796` (`generate_sync_result`) |
| Long-poll entry | `synapse/handlers/sync.py:431` (`_wait_for_sync_for_user`) |
| Initial-sync room changes | `synapse/handlers/sync.py:2987` |
| Incremental room changes | `synapse/handlers/sync.py:2727` |
| State delta | `synapse/handlers/sync.py:1119`, `:1316` (full), `:1465` (incremental) |
| Room summary / heroes | `synapse/handlers/sync.py:953` |
| Legacy initialSync | `synapse/handlers/initial_sync.py:91`, `:306`, `:349` |
| `/events` | `synapse/rest/client/events.py:41` |
| Stream tokens | `synapse/types/__init__.py:801` (room), `:1020` (multi-writer), `:1143` (StreamToken) |
| Notifier | `synapse/notifier.py:634` (`wait_for_events`), `:835` (`wait_for_stream_token`) |
| Replication → notifier | `synapse/replication/tcp/client.py:107` (`on_rdata`) |
| Redis subscribe | `synapse/replication/tcp/redis.py:152` (`_send_subscribe`) |
| Replication wire format | `synapse/replication/tcp/commands.py` |
| Auth | `synapse/api/auth/internal.py:141`, `storage/…/registration.py:766` |
| Filters | `synapse/api/filtering.py:116` (`USER_FILTER_SCHEMA`) |
| Room stream queries | `synapse/storage/databases/main/stream.py:584` |
| Client event serialisation | `synapse/events/utils.py:224` (`serialize_event`) |

---
