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

## Found by widening to a second account, 2026-09-01

The first test account had 9 rooms, all `shared`, all local, one room version
apart. A second account with **30 joined rooms — versions 1, 10, 11 and 12;
`shared`, `world_readable` and `invited`; five encrypted; several federated and
backfilled** — found five more defects in an hour. None of them could have been
found with the first account's rooms.

The lesson is not "test more"; it is that the *shape* of the corpus decides
what a comparator can see. Room version 1, backfilled history and a room the
account was invited to before joining each falsified a specific assumption.

### `stream_ordering` is not history order — backfill makes it negative

`unsigned.membership` was computed by finding the user's most recent
`m.room.member` event at or before the event's `stream_ordering`. That is wrong
whenever a room has backfilled history.

`stream_ordering` is a **server-local insertion counter**. In a room this server
joined after it already had history, the create event and early state are stored
at around **-23,964,688**, while the user's own invite sits at **+9,100,251**.
Ordering by stream therefore puts the whole of the room's earlier history *after*
events that follow it, and every backfilled event is reported with the
membership the user had before they were ever invited.

The room's own order is `(topological_ordering, stream_ordering)`. Membership
resolution now uses that pair.

### Presence uses Python truthiness, so a stored `0` is absent

`format_user_presence_state` guards with `if state.last_active_ts:` and
`if state.status_msg:`. A stored `0` is falsy and omitted exactly like a NULL,
as is an empty status message. Treating NULL as the only absence emits a
`last_active_ago` of **1,788,254,542,122** — the whole Unix epoch — for any user
whose `last_active_ts` is zero.

### The redaction copy goes the opposite way to the obvious reading

MSC2174 moved `redacts` *into* `content` from room version 11, so the canonical
place is the opposite of what the field name suggests. `Event.redacts()`
(`rust/src/events/mod.rs:623`) reads **`content.redacts`** when
`updated_redaction_rules` is set and **top-level `redacts`** otherwise; the
serialiser then writes it to the *other* place for clients written against
either version.

Getting the direction backwards does not duplicate the field — it drops it
entirely, because the read finds nothing.

### `unsigned.delay_id` is `org.matrix.msc4140.delay_id`

The unsigned key is the MSC-prefixed name
(`rust/src/events/constants.rs`, `unsigned_field::DELAY_ID`), not a bare
`delay_id`.

### MSC4354 sticky events carry their remaining lifetime

When `experimental.msc4354_enabled` is on — it is on this deployment, and
Synapse defaults it off — an event with an `msc4354_sticky.duration_ms` gets
`unsigned.msc4354_sticky_duration_ttl_ms`: the time it has **left**, not its
configured duration, so the value shrinks as the event ages and disappears once
it expires. Capped at one hour, and the origin timestamp is clamped to now so a
remote server cannot claim a future timestamp to stick for longer than the cap.

Since `homeserver.yaml` is never mounted, this is configured in our own
`experimental:` block using Synapse's field name.

## The state-group resolver, and what it uncovered (2026-09-01)

With per-event state resolved, every deliberate 501 gap closed: **all 30 of the
second account's rooms now match**, including `invited` history visibility,
rooms whose visibility changed 2 and 3 times, encrypted rooms, room version 1
and backfilled history.

Resolving state exposed the next layer of the onion.

### Redaction is applied on READ, and we were not doing it

A redacted event **keeps its original body in `event_json`** until a background
job censors it, and Synapse censors in place only after
`redaction_retention_period`. Serving stored JSON unchanged therefore publishes
exactly what the redaction was meant to remove — on this server, for events
redacted years ago.

Synapse prunes in the storage layer (`_maybe_redact_event_row`), so **every**
event it hands out is already redacted: state events as much as timeline ones.
Getting the timeline right and forgetting the state block leaves a redacted
`m.room.topic` sitting in current state with its content intact.

The allowlist is per room version, and the differences are not cosmetic:
`m.room.aliases` keeps its aliases in versions 1–5 and loses them afterwards; a
create event keeps its *whole* content from version 11 (MSC2176); versions
before 11 keep three extra top-level fields. Redaction does not clear
`unsigned`, it rebuilds it, keeping only `age_ts` and `replaces_state`.

Which redactions count is also a rule, not a lookup:

- redactions of `m.room.create` are ignored outright;
- a rejected redaction does not count;
- a redaction only redacts within its own room;
- from room version 3, a redaction that arrived before its target is rechecked
  on read and counts only if its sender shares a domain with the target's.

### MSC3391: account data with empty content is deleted

`get_global_account_data_for_user` appends `AND content != '{}'` when
`msc3391_enabled` — on here, off by default in Synapse. Sixteen deleted entries
were being served, mostly per-device notification settings.

Note the comparison is against the literal text `{}`, not parsed JSON, so an
entry stored as `{ }` survives. Reproduced exactly rather than "improved": the
point is to return what Synapse returns.

### `/initialSync` presence cannot be pinned at all

Synapse reads it with `get_new_events(from_key=None)` — the *current* presence
of everyone the caller shares a room with, **not presence as of a token**. On a
server with active bots those timestamps move every second, so `last_active_ago`
differs by however long elapsed between the two requests: up to nine minutes in
one observed case where a user became active in between.

This is a different category from the per-room clock skew. That is bounded and
tolerated within 1000ms; this is unbounded and genuinely untestable. syncdiff
reports the two separately, and still compares *which* users appear, their
presence state and their `currently_active` — so a missing user or a wrong state
is still a mismatch.

### Room version flags are cumulative, and flattening them is where they get lost

`rust/src/room_versions.rs` defines each version with struct-update syntax
inheriting from the previous one, so v9's `restricted_join_rule_fix` does not
replace v8's `restricted_join_rule` — it joins it. A first attempt to extract the
table by regex silently dropped every inherited flag. The Go table is flattened
by hand for that reason, with the inheritance spelled out in a comment.

## `/sync` is not `/initialSync` with different keys (2026-09-01)

Four differences found while building M3, none of them cosmetic.

### `/sync` paginates by stream ordering; the legacy endpoints paginate topologically

`/rooms/{id}/initialSync` hands back a historical `t426-2633508` token;
`/sync` hands back a live `s13907984`. They are not two spellings of one
position: stream ordering is the server's insertion order and goes **negative**
for backfilled events, so a room with imported history is ordered quite
differently by the two, and `/sync` will not return backfilled events at all
where the legacy endpoint does.

`prev_batch` in a sync timeline is `RoomStreamToken(stream=first.stream_ordering - 1)`
(`handlers/sync.py:800`) -- always the live form.

### `summary` is only computed when the filter enables lazy-loading

`compute_summary` is guarded on `sync_config.filter_collection.lazy_load_members()`
(`handlers/sync.py:3244`), because heroes exist for clients that lack the
memberships to name a room themselves. With the default filter Synapse sends
`"summary": {}` -- an empty object, not a populated one and not a missing key.

### The per-room and whole-account account-data queries disagree, exactly like receipts

`get_account_data_for_room` -- the per-room query behind
`/rooms/{roomId}/initialSync` -- is a plain select with no MSC3391 clause, while
`get_room_account_data_for_user`, behind `/initialSync` and `/sync`, filters out
empty content. So a *deleted* account-data entry is hidden by one endpoint and
shown by the other.

That is the second such pair, after the two receipt queries. The lesson
generalises: **on these endpoints, "which query does this handler call" is part
of the contract**, not an implementation detail, because Synapse's singular and
plural accessors have drifted apart.

### `device_one_time_keys_count` always reports `signed_curve25519`

Even at zero. Synapse's comment cites element-hq/element-android#3725: a client
cannot otherwise tell "no keys left" from "this server does not report counts".

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
