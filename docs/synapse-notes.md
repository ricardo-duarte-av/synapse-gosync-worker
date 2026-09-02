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

## Push rules are synthesised, not stored (2026-09-01)

Synapse does not keep a user's ruleset. It keeps only their *deviations* from a
built-in base ruleset and rebuilds the whole thing on every read, so reporting
`m.push_rules` means reproducing forty base rules exactly and interleaving the
user's rows into them in the right places.

`internal/pushrules/baserules.go` is a port of
`rust/src/push/base_rules.rs`, **extracted mechanically** by
`tools/extract-base-rules.py` rather than typed out: the conditions serialise
through serde, and hand-transcribing forty rules is a long list of silent
opportunities to be subtly wrong. The extractor itself proved the point twice —
its first version dropped six rules whose `rule_id` wrapped onto its own line,
and lost two more that were written inline as `= &[PushRule {`. Both were caught
by the comparator, in seconds, which is the argument for having one.

Things that are easy to get wrong:

- **The interleave order is the ruleset.** A client evaluates rules in order, so
  the sequence `prepend override | user override | append override | user
  content | append content | append postcontent | user room | user sender | user
  underride | append underride` is not a detail.
- **A user rule whose id matches a base rule replaces that rule's actions in
  place**, keeping the base rule's conditions and position. Appending it would
  both duplicate the rule and move it.
- **`pattern_type` and `value_type` are placeholders**, substituted for the
  requesting user's id or localpart and then removed. They exist so one base
  ruleset can serve every user; a client must never see them.
- **`room` and `sender` rules are identified by the pattern of their single
  condition** — a `room` rule is named by the room it applies to — and report no
  conditions at all.
- **MSC4210 has the opposite sense to the other flags**: enabling it *removes*
  the legacy mention rules.
- **`.m.rule.master` defaults to disabled.** It is the silence-everything
  switch, and the only base rule whose `default_enabled` is false.

## Unread counts are relative to a read receipt (2026-09-01)

Three things make `unread_notifications` more than a `SELECT SUM`:

- Counts start at the user's **latest unthreaded read receipt**, not at the
  start of the room. Without that bound every count is the room's whole
  history — one room reported 3,950 against Synapse's 39.
- `event_push_summary` is a rollup, and a row is usable only when its
  `last_receipt_stream_ordering` matches the receipt actually in force. A newer
  receipt means the rollup has not caught up and the row must be ignored.
- **An all-zero rollup row does not count as a summary** (Synapse filters
  `notif_count != 0 OR unread_count != 0`). Treating it as one makes the thread
  look already-counted, so only post-rotation events get added, and the count
  comes out low.

MSC2654's `unread_count` is a *different number* from `notification_count`: it
counts events marked `unread` rather than events that would notify, so a muted
room accumulates one and not the other.

## `prev_batch` and `next_batch` can disagree within one response

`prev_batch` is `now_token.copy_and_replace(ROOM, ...)`, and Synapse **mutates
its own `now_token` while building the response**: both
`_generate_sync_entry_for_presence` and `_generate_sync_entry_for_to_device`
reassign `sync_result_builder.now_token` (`handlers/sync.py:2529`, `:2158`). So
a room's `prev_batch` carries whatever those streams were at when that room was
built, while `next_batch` carries their final values.

Pinning cannot fix this: the comparator is given one token and Synapse used
several. syncdiff accepts a `prev_batch` whose **room key** is right — the only
part that decides where pagination resumes — and reports the rest separately.

## Closing the last /sync gaps (2026-09-01)

### `m.room.aliases` is dropped from the state block too

Not only from the timeline. `compute_state_delta` ends with an explicit
`if e.type != EventTypes.Aliases` (`handlers/sync.py:1313`), a second and
separate place from `_check_filter_send_to_client`. Until MSC2261 a malicious
alias event cannot be redacted, so Synapse refuses to carry the type anywhere.

Note this is `/sync`-specific: the legacy initialSync endpoints do carry aliases
in their state.

### When both ends of the timeline disagree, the later event wins

`_calculate_state` unions the state at the start and end of the timeline into a
**set of event ids**, subtracts what the timeline carries, then rebuilds a map
keyed by state key. If a key changed outside the timeline, both its events
survive the subtraction and only one can be reported -- and which one depends on
Python set iteration order, which is not defined.

Observed consistently to be the *later* event, which is also the more useful
answer: it is the state the client will have going forward. We resolve the
collision that way deliberately.

### Unread counts fold threads into the room total

With the default filter, `unread_thread_notifications` is off, and Synapse then
**adds every thread's counts into the room's single figure**
(`handlers/sync.py:3326`). Counting only the main timeline comes out low --
229 against 2,083 in one room here.

The receipt bound is per thread as well: a threaded receipt ahead of the
unthreaded one moves that thread's starting point on its own.

### Typing is a missing response *section*, not just a token field

The first concrete consequence of having no replication. Typing is never
persisted -- it lives in an in-memory counter on the typing worker and reaches
other workers only over the replication stream -- so a room where somebody is
typing gets an `m.typing` EDU from Synapse and nothing from us. syncdiff counts
it as a named known gap rather than a mismatch, so it stays visible without
drowning the signal.

### Bundled aggregations are not implemented

An initial sync is `limited`, and Synapse bundles relations into a limited
timeline: `unsigned.m.relations` carries thread summaries (count, the serialised
latest event, whether the caller participated), edits, and references
(`handlers/relations.py:418`). Three events across three rooms in this corpus
have them.

## MSC4222: `state_after` (2026-09-01)

Opt-in per request via `?org.matrix.msc4222.use_state_after=true`, and only
offered when the server enables it. The response field is a **different key**,
`org.matrix.msc4222.state_after` — deliberately, so a client that did not opt in
never starts receiving state with the opposite meaning.

The two are computed quite differently, and neither is a filter over the other:

- **Full sync** (initial, or a newly joined room): `state_after` is simply the
  state at the end token. No union with the timeline start, nothing subtracted —
  the client is being told where the room ended up, not what to apply first.
- **Incremental sync**: it comes straight from `current_state_delta_stream` for
  the window. A delta whose `event_id` is NULL is a state key being *removed*,
  which the MSC cannot express, so Synapse skips it.

Cheaper for us than for Synapse, which computes the full-sync case as
current-state-rolled-back-by-deltas purely as an optimisation for joined rooms.

Two things bit while implementing it:

- **`m.room.aliases` state keys are server names**, not the empty string, so a
  room has one per server that ever set an alias. Deleting a single key removes
  at most one of them.
- **The membership scan has to follow the renamed key.** `calculate_user_changes`
  reads member events out of the state block to decide `device_lists` and the
  extra presence; looking only for `"state"` silently loses everyone who joined.

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

## Filters and lazy loading (2026-09-02, M6)

### `prev_batch` is the token you were given, unless the timeline was trimmed

`_load_filtered_recents` keeps two variables that look interchangeable and are
not:

```python
room_key = upto_token.room_key
end_key = room_key
while limited and len(filtered_recents) < timeline_limit and max_repeat:
    events, end_key, limited = await pagination_method(from_key=end_key, ...)
...
if len(filtered_recents) > timeline_limit:
    filtered_recents = filtered_recents[-timeline_limit:]
    room_key = RoomStreamToken(stream=filtered_recents[0]...stream_ordering - 1)
prev_batch_token = upto_token.copy_and_replace(StreamKeyType.ROOM, room_key)
```

The pagination cursor lands in `end_key`. `room_key` — the one that becomes
`prev_batch` — is only ever reassigned by the **trim**. So a timeline that came
back shorter than the limit reports a `prev_batch` equal to the sync point
itself, even though pagination walked well past it.

Reporting where the walk stopped is the obvious reading and is wrong. It stayed
hidden through M3–M5 because with the default filter every room in the corpus
had more than ten events and always trimmed. The first `types` filter exposed
it in nine rooms at once.

### The timeline is re-paginated up to five times

A page is thinned twice after it is loaded — once by the client's filter, once
by history visibility — so Synapse loops, `max_repeat = 5`, while the timeline
is still under the limit. `load_limit = max(timeline_limit * 2, 10)` per pass.

With the default filter one pass is nearly always enough, which is why the loop
can be left out and not noticed. With a selective filter it is the difference
between a full timeline and a nearly empty one that also claims `limited`.

### `set_timeline_upper_limit` applies to inline filters only

`rest/client/sync.py` calls it on the `filter_id.startswith("{")` branch and
not on the stored-filter branch. A client that uploads a filter with
`"limit": 5000` and passes its ID is not capped; the same filter inline is. The
asymmetry is upstream's, and it means `filter_timeline_limit` is not a bound on
anything. This deployment sets it to **1000**, not the default 100.

### The lazy-loaded members cache is smaller than it says

`LruCache(max_size=LAZY_LOADED_MEMBERS_CACHE_MAX_SIZE)` asks for 100, then
multiplies by `caches.global_factor`, whose default is **0.5** — so the real
size on a default deployment is 50. `av-sync-worker-2` sets `global_factor:
1.0`, so the reference worker's is 100 and ours is configured to match.

The outer `ExpiringCache` is built without `reset_expiry_on_get`, so a device's
cache expires 30 minutes after it was **created**, not after it was last used.
A continuously syncing client loses its lazy-loading state on a fixed schedule
and is re-sent every member it already has.

### `members_to_fetch` differs under MSC4222

Under the classic `state` block a sender whose membership is already in the
timeline is skipped — the client will get it from the timeline. Under
`state_after` the block describes where the room *ended up*, so a membership in
the timeline says nothing about it and the sender is fetched regardless.

### Lazy loading changes `_calculate_state`, and only one of its subtractions

With `lazy_load_members` set, the member events present in `timeline_start` are
removed from `previous_timeline_end_ids` before the subtraction. The events
carried by the timeline itself are still subtracted as normal. Merging the two
exclusion sets — the obvious simplification — sends members twice.

### A gappy incremental sync turns lazy loading off, partly

`if batch.limited: state_filter = StateFilter.all()` applies to the two
whole-state fetches (`previous` and `end`) but **not** to `timeline_start`,
which was fetched earlier and stays restricted. The comment upstream calls this
"counterintuitively" and it is: the restricted start is what lets
`_calculate_state`'s lazy-load rule pull the timeline senders' memberships back
in while the unrestricted ends supply everything else.

### `summary` is three different values

`result["summary"] = room.summary` is unconditional, and `room.summary` is:

- `{}` when no summary was computed — not lazy loading, or nothing changed;
- `null` when `compute_summary` ran and found no events in the room at all;
- the object otherwise.

So an empty object and a null mean different things and neither is a missing
key.

### `compute_summary` can add to the state block

A hero whose membership the client has not been sent is useless — a user ID
with no profile to render. So `compute_summary` writes the missing member
events into `state` after `compute_state_delta` has finished with it. It runs
*after* `compute_state_delta`, which matters on an initial sync: the cache has
already been cleared and repopulated by then, so the hero check sees exactly
the members that were just selected.

### `timeline_gaps` makes a timeline `limited` with nothing trimmed

`_load_filtered_recents` consults `get_timeline_gaps` before anything else. A
gap is recorded when Synapse persists an event whose predecessors it did not
have — a federated room going quiet and coming back. The events either side are
adjacent in stream ordering and not in the room's history.

Two effects:

- the room's timeline is `limited` even when nothing was trimmed, so the client
  paginates across the hole rather than treating the events as contiguous;
- on an incremental sync the loaded window is **discarded** and re-paginated
  from the sync point back to the gap, so events before the gap are not sent.

This deployment has **99,053 gap rows across 1,392 rooms**. It is invisible
whenever the timeline was long enough to be trimmed — which with the default
filter is nearly always — which is why it survived M3 to M5 unnoticed.

`persisted_after` is per writer: `token.get_stream_pos_for_instance(instance) <
row.stream`. With a sharded event persister a plain stream comparison is only an
approximation and both bounds have to be re-checked against the writer that
recorded the gap.

### The state block cannot keep a room in the response

`_generate_room_entry` decides whether to emit a room at all:

```python
if not (always_include or batch or account_data_events or ephemeral
        or full_state or sticky_event_ids):
    return
```

`state` is not in that list, and this runs **before** `compute_state_delta`. So
a room whose only news is a state change outside the timeline is dropped
entirely — the state delta is never even computed.

Checking afterwards and counting `state` emits rooms Synapse omits. As with the
gap above, it only becomes visible once something can empty the timeline while
leaving a state delta behind, which is exactly what a `types` filter does.

### `unread_thread_notifications` changes the main counts too

Without the filter flag, every thread's notification and highlight counts are
folded into the room's `notification_count` and `highlight_count`. With it, the
threads are reported separately under `unread_thread_notifications` and the main
counts carry only the main timeline. It is not an additive field.

## To-device messages (2026-09-02, M8)

**`/sync` deletes, and the deletion is bounded by `since`, not by the window.**
`_wait_for_sync_for_user` calls `delete_messages_for_device(user, device,
since_token.to_device_key)` before it does anything else — before the notifier
wait, once per request, not once per pass round the long-poll loop. So a
response's own messages are never deleted by the request that returns them:
they go out in `(since, now]`, and the *next* request's `since` is what
acknowledges them. That is what makes a replayed comparison safe.

**The section is served on an initial sync too.** The guard is
`device_id is not None and since_stream_id != now_token.to_device_key`, with
`since_stream_id = 0` when there is no `since`. A device that has never synced
still has an inbox waiting for it. Nothing is deleted on that path, because
there is no `since` to prove the client received anything.

**100 at a time, and the token is wound back to match.** `get_messages_for_device`
takes `limit=100` and `/sync` never overrides it. When the limit is reached the
storage layer returns *the stream id of the last row*, not the requested upper
bound, and `_generate_sync_entry_for_to_device` assigns that to the now token:
`now_token.copy_and_replace(TO_DEVICE, stream_id)`. So `next_batch` resumes
mid-backlog and the remainder arrives on the following sync. Leave the token
alone and everything past the hundredth message is skipped for ever. Measured:
120 messages waiting, first sync returns 1..100 and a `next_batch` at the 100th
message's position, second returns 101..120.

Under 100 the same function returns the *requested upper bound* rather than the
last row's position, which matters when the last row is not the last message in
the stream — the device's inbox is sparse in a stream shared by every device on
the server.

**Deleting needs `SELECT` as well as `DELETE`.** PostgreSQL cannot put a `LIMIT`
on a `DELETE`, so `delete_messages_for_device_between` first reads
`MAX(stream_id)` over a windowed subquery and then deletes up to it, 1000 rows
at a time, keeping a moving lower bound so successive batches do not rescan what
the previous ones removed.

**Stream ids are not unique.** One to-device message sent to several devices
shares a stream id across them; ids are unique only within a `(user_id,
device_id)` pair. That is why Synapse will only apply a limit when querying a
single device.

## `_load_filtered_recents`, in full (2026-09-02)

The function every /sync timeline goes through, and four of its details are
counter-intuitive enough that we got each one wrong at least once.

**`limited` is decided before anything is filtered.** It is
`potential_recents is None or newly_joined_room or timeline_limit <
len(potential_recents)` — the size of the window that was LOADED, against the
timeline limit. Not how many survived the client's filter, and not the load
limit. A timeline that comes back short because a filter removed things is not
limited, because there is nothing more for the client to fetch: /messages
applies the same filter.

**The window is loaded with `timeline_limit + 1`.**
`_get_room_changes_for_incremental_sync` asks
`get_room_events_stream_for_rooms(limit=timeline_limit + 1)`, and that one extra
row IS the `limited` test above. Loading more than that — twice the limit, say —
does not make the answer safer, it makes it unanswerable.

**`prev_batch` moves only when the timeline is trimmed.** `room_key` starts at
`upto_token.room_key` and is reassigned in exactly one place, the
`len(filtered_recents) > timeline_limit` branch. The pagination cursor goes to
`end_key`, a different variable. So an untrimmed timeline reports where the
WINDOW began, even though pagination may have walked far past it.

The `upto_token` that anchors it is per room, and it is not the now token:

| Room | `upto_token` |
|---|---|
| Joined, events in the window | `now_token` with the room key set to the chunk's start — `oldest_returned.stream_ordering - 1` |
| Joined, nothing in the window | **`since_token`**, the whole token, not just its room key |
| Joined, newly joined | the chunk start, as above; `since_token` is passed as None but `upto_token` is not |
| Archived | the leave token |
| Initial sync | `now_token` |

**Which ordering pagination uses depends on whether there is a `since`.**
`pagination_method` is topological when `since_key is None` and by stream
ordering otherwise, with the comment "use topological_ordering for historical
events, stream_ordering for updates". An incremental sync asks what arrived
since the client last looked, which is a question about the order the server
received things in; an initial sync asks for a historical slice of the room,
which is a question about the DAG. An event backfilled today sits in a different
place in each order. The incremental walk is also bounded below by `since` (or
by the gap token when there is a gap), which is what stops a thinned page from
being topped up with events the client already has.

`generate_next_token` subtracts one from the stream ordering when paginating
backwards, because "tokens are positions between events" — and it carries the
topological ordering only when the walk was topological, so the two paths
produce different token FORMS as well as different events.

## Thread notification counts, and sticky events (2026-09-02, M7)

**The same query answers two different questions, and the filter picks which.**
`_generate_sync_entry_for_rooms` reads one `RoomNotifCounts` per room and then
either reports `main_timeline` alone with the threads in their own section, or
adds every thread into the room's single figure. There is no third option and no
"both": a client that asks for `unread_thread_notifications` must see the room
count DROP to the main timeline, and one that does not must see the threads
folded in. Reporting the section without dropping the room count is the failure
mode that looks right and is not.

`org.matrix.msc2654.unread_count` follows the same rule, but only the folded
branch adds thread unread counts to it — the split branch leaves it at the main
timeline's, and MSC3773's per-thread entries carry only `notification_count` and
`highlight_count`, never an unread count.

**A thread with all-zero counts is still reported.** Synapse's `_get_thread()`
creates the entry as a side effect of seeing any row for that thread, and the
per-thread action query groups by thread without requiring a non-zero count, so
a thread whose events neither notify nor count as unread appears as a pair of
zeroes. Filtering those out is wrong.

**`main` is a thread id, not a NULL.** MAIN_TIMELINE is the literal string
"main" in `event_push_actions` and `event_push_summary` alike. Receipts are the
exception: `receipts_linearized.thread_id` really is NULL for an unthreaded
receipt, which is why Synapse's `USING (thread_id)` join never matches one — a
NULL joins to nothing, and the unthreaded receipt is applied as the fallback
bound instead.

**An upstream bug, worth knowing before it is mistaken for ours.** In the loop
that adds post-rotation counts for summarised threads:

```python
for notif_count, unread_count, thread_id in unread_counts:
    if thread_id not in summarised_threads:
        continue
    if thread_id == MAIN_TIMELINE:
        counts.notify_count += notif_count      # `counts` is left over
```

`counts` was last assigned in the summary loop above, so main-timeline counts
are added to whichever thread that loop saw last. The totals are unaffected,
which is why it is invisible until per-thread counts are reported, and the
result set has no ORDER BY, so there is nothing to agree with.

### Sticky events (MSC4354)

**`sticky_events_by_room` runs before any room entry is built, and moves the now
token.** It reassigns `sync_result_builder.now_token` to the last sticky row
returned, so the wound-back position reaches every `prev_batch` in the response
as well as `next_batch`. The same shape as the to-device wind-back, and the same
trap: compute the section after the rooms and the tokens disagree.

**An event already in the timeline is removed from the section**, per the MSC:
the client learns of it either way, and sticky events are spammable. This is
what hides an unimplemented section — it appears only once an event ages out of
the timeline, or the moment a filter excludes it.

**"History visibility checks MUST NOT be applied" is implemented by RUNNING the
visibility pass**, with every sticky event id in `always_include_ids`. That is
not the same as skipping it: the pass is also what stamps `unsigned.membership`
(MSC4115) onto each event. Skip it and the events come out visibly different.

The query filters on `expires_at` and on soft-failed events, so the answer
depends on the wall clock as well as the stream position — and the cap is 100
events per sync across all rooms, because anyone may send one.

## /events, the pre-sync stream (2026-09-02)

**Five sources, in a fixed order, and the order is part of the answer.**
`_EventSourcesInner.get_sources()` returns room, presence, typing, receipt,
account_data, and `check_for_updates` appends each source's output in that
order. The chunk is a JSON list, so this is not an implementation detail.

**The end token starts as the CLIENT's token, not the current one.** Only
sources that actually produced something advance their field
(`end_token.copy_and_replace(keyname, new_key)`), so a quiet stream stays where
the client left it and nothing is skipped. And a source is consulted at all only
if its field differs between the before and after tokens.

**The room source ignores the `to` it was given.** `RoomEventSource.get_new_events`
calls `self.get_current_key()` for its upper bound rather than using
`after_token`, and its returned key is the stream position of the LAST EVENT IT
RETURNED — which the limit may have cut short. So the end token of a limited
/events response is derived from the response, and pinning a comparison to it
hands the implementation its own answer.

**The limit applies to room events only.** EDUs are appended afterwards and are
not counted, so a response with `limit=10` can legitimately carry thirteen
entries.

**Membership changes are fetched separately, and are not filtered by room.**
`get_membership_changes_for_user` runs alongside the per-room query, which is
what lets an invite in a room the caller has not joined reach them.

**Presence for joins is emitted once per EVENT, not once per user.** The handler
loops over the returned events and extends `to_add` for each join, so three
joins by the same person yield that person's presence three times. For the
caller's own join it sends presence for every member of the room; for anyone
else's, just the joiner.

**/events cannot be asked not to disturb presence.** `EventStreamHandler.get_stream`
calls `presence_handler.user_syncing(..., affect_presence=not is_guest,
presence_state=ONLINE)` unconditionally: there is no `set_presence` parameter.
Every other comparison here sends `set_presence=offline` to keep the harness
invisible; a /events comparison marks the test account online whether it likes
it or not.

**A guest must name a room.** Without `room_id` there is no way to decide what a
guest may see, so the servlet rejects it with a 400 before anything else. A
named room the caller has not joined is allowed only if it is world-readable,
and then that room alone.

**The timeout is a DEFAULT, not a cap** — 30 seconds when the client says
nothing, which is the opposite of /sync, where a missing timeout means "answer
now". A non-zero timeout is floored at 500ms and jittered by ±10%.

## CORS, and why a web client could not use this worker (2026-09-02)

**The CORS headers on `/_matrix` come from Synapse, not from the reverse proxy.**
Verified against the live server rather than assumed: nginx adds
`Access-Control-Allow-Origin` on some static locations, but for `/_matrix`
every header a browser sees is set by `synapse/http/server.py set_cors_headers`.
A worker serving those paths that omits them is one a web client cannot use at
all -- the browser makes the request, receives the answer, and then refuses to
hand it to the page.

```
access-control-allow-origin: *
access-control-allow-methods: GET, HEAD, POST, PUT, DELETE, OPTIONS
access-control-allow-headers: X-Requested-With, Content-Type, Authorization, Date
access-control-expose-headers: Synapse-Trace-Id, Server
```

**OPTIONS is answered for every path, before routing.** `OptionsResource`
overrides `getChildWithDefault` to select itself whenever the method is
OPTIONS, so a preflight for a path Synapse does not implement still gets **204
with no body**, not 404. That is not a nicety: a browser that gets 404 for the
preflight will not send the real request, even to an endpoint that would have
worked.

`Synapse-Trace-Id` is exposed whether or not anything emits it.

## Filters are validated on upload, not on read (2026-09-02)

Three paths, and they do not agree:

| Path | Validated? |
|---|---|
| `POST /user/{id}/filter` | yes — `add_user_filter` calls `check_valid_filter` |
| `?filter={...}` inline | yes — `sync.py` calls `check_valid_filter`, cached per literal string |
| `?filter=<id>` stored | **no** — `get_user_filter` hands the stored JSON straight to `FilterCollection` |

So a filter in the table is used exactly as stored, whatever it contains. A
worker that re-validates on read can refuse a filter the homeserver has been
serving for months, and there is no way for the client to recover: it did
nothing wrong and the filter is not going to change.

**`presence` and top-level `account_data` use FILTER_SCHEMA, which declares no
`rooms` or `not_rooms` at all** — only `limit`, `senders`, `not_senders`,
`types`, `not_types` and the MSC3874 rel_types — and it sets
`additionalProperties: true`. A room list under `presence` is therefore never
checked, even on the upload path. gomuks uploads
`{"presence":{"not_rooms":["*"]}}` and Synapse stores it happily.

At runtime the key is read anyway (`Filter.__init__` takes `not_rooms` for any
filter) but never matches, because a presence EDU has no room for `_check` to
test against.

## Rejected events are never served (2026-09-02)

`get_events_as_list` defaults to `allow_rejected=False`, and
`_get_events_from_cache_or_db` **asserts** that no rejected event escapes when
it is false. Range queries filter them in SQL:
`LEFT JOIN rejections USING (event_id) ... AND rejections.event_id IS NULL`,
which this schema also denormalises into `events.rejection_reason`.

`outlier = FALSE` is not enough. A rejected event is not an outlier: it is a
real event, in the room, at a real position, that failed an auth check. Serving
one is worse than a parity bug -- it is showing a client something the server
decided was not allowed to happen.

They are common enough to matter. One federated room here holds 13 rejected and
12 soft-failed events out of 727, and two rejected `m.room.member` events inside
a single 100-event window were enough to shift the whole timeline by two.

## Typing is bounded by the token, like everything else (2026-09-02)

`ephemeral_by_room` passes `since_token.typing_key` to the typing source, which
returns events only for rooms whose serial is above it, and then rewrites the
now token's typing field to what it returned. A room carries `m.typing` when
its typists have CHANGED since the client last looked -- not when somebody
happens to be typing.

That distinction is the difference between a working long poll and a hot loop.
A sync returns as soon as it has anything to say; if "anything to say" includes
the current typist list, then a room with somebody typing makes every sync
return immediately, the client stores next_batch and asks again, and the pair
spin for as long as the typing lasts. Measured at 35 requests a second against
an otherwise idle account.

It is invisible to a pinned comparator: one request in isolation carries the
same typing event either way, and `m.typing` differences are a tolerated known
gap precisely because typing lives only in memory. Only a client that syncs in
a loop can see it.

## Invites from ignored users are not reported (2026-09-02)

Both room-change paths drop them, and it is easy to miss because it is two
lines in the middle of a long function:

```python
elif event.membership == Membership.INVITE:
    if event.sender in ignored_users:
        continue
    if invite_config.get_invite_rule(event.sender) == InviteRule.IGNORE:
        continue
```

`_get_room_changes_for_initial_sync` has that; the incremental one has the same
test around `last_non_join.sender`. The sender of the membership event is the
inviter, which is what is checked against `m.ignored_user_list`.

Nothing else about an ignored user's invite is special: the room stays in
`local_current_membership` as `invite` for ever, so any implementation that
lists memberships and reports them will show invitations from people the user
ignored years ago. A 500-room account here carried four such invites from
March 2025, invisible in Element against Synapse and present against this
worker until the filter was added.

The MSC4155 half (`invite_config.get_invite_rule`) is NOT implemented here. It
reads a separate account-data event that no account on this server has set, so
it changes nothing today -- recorded as a gap rather than silently skipped.

## Ask the state store for what a lazy client keeps (2026-09-02)

`StateFilter.from_lazy_load_member_list(members)` is `{m.room.member: {those
users}}` with `include_others=True`, and Synapse passes it all the way down to
the state store. It never resolves a room's whole state map and then filters in
Python.

The distinction is invisible on a small room and decisive on a large one. State
resolution walks `state_groups_state` -- the largest table in the database, 17GB
here -- through the state-group edge chain, and the cost of the walk is roughly
the number of rows it returns. A public room's state is mostly members, and a
lazy-loading client keeps a handful of them.

Measured on one 3,094-entry room here:

| | rows | time |
|---|---|---|
| whole state map | 3094 | 14.7ms |
| lazy (one member) | 49 | 6.1ms |

Two and a half times faster, sixty times less to hold in memory -- and that room
is small. The account that surfaced this has 500 rooms including public ones
with tens of thousands of members; resolving them the wrong way cost 209 seconds
and 1.5GB for a single initial sync.

The same applies to any question about one state key. `compute_summary` asks
whether a room has a name; resolving the entire state map to read
`m.room.name` is the same answer at four orders of magnitude more work, and a
summary is computed for every lazy-loading room in an initial sync.

## Rooms are built ten at a time (2026-09-02)

`_generate_sync_entry_for_rooms` ends with

```python
await concurrently_execute(handle_room_entries, room_entries, 10)
```

so Synapse builds ten room entries at once. This is not an optimisation bolted
on to a sequential design; it is the design, and a port that walks rooms one at
a time is slower than upstream by roughly that factor on any account large
enough to notice. 500 rooms sequentially took 193 seconds here.

The number matters beyond throughput. The lazy-loaded member cache is written as
rooms are built, so how many run concurrently decides which members a later room
considers already sent -- another reason the two implementations can disagree
about lazy loading, and another reason to match the constant rather than pick
one.

## Rejected events consume the limit (2026-09-02)

The correction to the note above, found by comparing a 654-room account.

Synapse pages the events table with **no rejection predicate** and drops
rejected events afterwards, in `get_events_as_list` (`allow_rejected=False`).
The order matters: a rejected event occupies a slot in the `LIMIT`, and then is
not returned. The page therefore reaches exactly as far back as it would have
with the rejected events included, and a room whose recent history is entirely
`auth_error` returns an EMPTY timeline rather than older messages.

Filtering `rejection_reason IS NULL` in the WHERE clause is not equivalent and
is much worse than it looks: the page reaches further back, and one bridged
matrix.org room here returned a message 63,000 stream positions old that Synapse
never sends -- taking its `prev_batch` and its whole state block with it.

The same applies to the token: `next_key` comes from the oldest row PAGED,
rejected or not.

Anchor queries are the exception and do filter in SQL --
`get_last_event_pos_in_room_before_stream_ordering` has `LEFT JOIN rejections
... AND rejections.event_id IS NULL` -- because there is no limit to consume.

## An open question: extra members in Synapse's lazy state block

On 5 of 654 rooms, Synapse's lazy-loaded `state` carries a member event for
somebody who is neither a timeline sender nor the caller, and we do not. It is
deterministic across repeated requests, so it is not the shared-cache artefact
that `prev_content` is.

Nothing in `_compute_state_delta_for_full_sync` explains it: `members_to_fetch`
is the senders of `batch.events` plus the caller, the same `state_filter` is
used for both ends of the timeline, and `compute_summary` returns before
computing heroes when a room has a name -- which every affected room does.

The likeliest explanation, unproven: `_get_state_for_groups` returns a superset
of what a filter asks for when a state group's delta is small enough to be
returned whole. The extra member's event does live in a delta row of a nearby
state group. If that is right, matching it would mean reproducing the storage
layout rather than the algorithm, and returning exactly what was asked for is
the better answer -- but it is recorded as unexplained rather than dismissed.

## An empty typing list is an event (2026-09-02)

`TypingNotificationEventSource._make_event_for` builds
`{"type": "m.typing", "room_id": ..., "content": {"user_ids": list(typing)}}`
for every room the source returns, and the source returns rooms by SERIAL, not
by whether anyone is typing. So a room where the last typist stopped yields
`{"user_ids": []}`.

That empty event is the only thing that ever clears a typing indicator. There
is no timeout at the client end and no later sync will mention the room again
until somebody types in it once more, so omitting it leaves a user shown as
typing indefinitely -- which is exactly what happened here, and it looked like a
stuck client rather than a missing event.

The rule is the same on both paths, with only the starting position differing:
report a room when its typing serial has moved past the client's token (0 on an
initial sync), and then render whatever the current list is, empty or not.
