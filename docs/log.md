# Work log

Newest first. Numbers are measurements, not estimates.

## 2026-09-01 — M5: replication, typing, and long-polling

The worker now follows Synapse's replication stream, and three things follow
from that.

**Typing works.** It is the one part of a sync response that exists nowhere but
memory — Synapse keeps it in a counter on the typing worker and never writes it
down — so before this the comparator counted it as a known gap. It now matches:
one missing typist on the first comparison after startup, and none on the two
after that, as the view fills in.

**Stream positions are real.** 11 of the 14 token fields now match Synapse
exactly, `typing` among them. See [tokens.md](tokens.md) for the table and for
why the two remaining ones are safe to be behind.

**Long-polling works end to end.** Measured: a message sent three seconds into a
thirty-second poll came back at 3.15s. Before this, `timeout` was ignored and
every client would have hot-looped at full rate.

The ordering inside the poll is the load-bearing part, and it is what Synapse's
notifier comments dwell on: **interest is registered before the answer is
computed**. An event landing during the computation still wakes the waiter.
Register afterwards and everything arriving in that gap is lost, and the client
hangs for its whole timeout on news that had already come.

Two deliberate choices worth knowing:

- The subscriber is **SUBSCRIBE-only**. A real Synapse worker also publishes a
  `REPLICATE` command on connect to ask for current positions; doing that would
  make every other worker broadcast POSITION rows on our account. Positions are
  seeded from the database and corrected as traffic arrives instead.
- Every position overlay is a **maximum**, so a stale replication value can
  never drag a token backwards — which would ask a client to replay what it
  already has.

No regression: both accounts still match on all four comparator endpoints.

## 2026-09-01 — MSC4222 `state_after`

Implemented and matching for **both accounts, on initial and incremental sync,
at every rewind tested** — and with no regression to the default behaviour.

It is not a filter over the normal state block. On a full sync `state_after` is
the state at the end token, with nothing subtracted; on an incremental one it
comes straight from `current_state_delta_stream`. Synapse computes the full-sync
case as current-state-rolled-back only as an optimisation, so the resolver made
this cheap.

Two things bit: `m.room.aliases` state keys are **server names**, so deleting
one key removes at most one alias event; and the membership scan that feeds
`device_lists` and the extra presence has to follow the renamed key, or it
silently loses everyone who joined.

## 2026-09-01 — M4: incremental `/sync` serving

An incremental `/sync` matches for five of six test windows across both
accounts. The initial-sync endpoints are unaffected: still 9/9, 30/30 and both
`/initialSync`.

The comparator builds a `since` by **rewinding the room key** of a token Synapse
just minted, rather than taking two snapshots a moment apart — back-to-back
tokens produce an empty delta, which proves nothing. Rewinding 3,000 / 30,000 /
200,000 positions produces rooms that changed, rooms that did not, and
membership transitions.

Four things the comparator taught, in order of how much they mattered:

- **A room joined inside the window is treated exactly like an initial sync**:
  full state, a timeline paginated back from now rather than only events since
  `since`, and `limited` set. The client has never seen the room, so a delta
  against state it does not have would be meaningless. This was 235 of the first
  run's diffs.
- **Joining a room entitles you to the presence and device lists of everyone
  already in it**, whatever those streams have been doing — and to the presence
  of anyone who joined or was invited to a room you were already in. None of
  them need have touched the presence stream, so none are found by a window
  query.
- **`device_lists.left` is decided by reading `unsigned.prev_content`** off
  Synapse's in-memory event, which is reliably present on timeline events and
  present on *state* events only when an earlier reader polluted the shared
  cache. Derived from the timeline only, and tolerated as the same cache
  artefact one step removed.
- **Ephemeral stays bounded by `since` even for a newly joined room.** Receipts
  are a stream like any other; joining does not entitle you to a replay.

The `state` shortcut is worth noting for what it saves: when the timeline is
unlimited **and** its events form an unbroken chain from where the client left
off, the `state` block is empty, because the timeline already carries every
change. Only a gap or a fork makes a state block necessary.

Then finished: the `invite`, `knock` and `leave` sections, and two bugs that
only a deep rewind exposed.

**`prev_events` is a list of `[event_id, hashes]` PAIRS in room versions 1 and
2.** Reading an entry as a string yields the whole array, so the linearity check
never matched and a v1 room always got a `state` block Synapse omits entirely.
Nine of this server's 1,165 rooms are v1 or v2, and one is in the corpus.

**The incremental timeline needs the same `load_limit` and always-include rules
as the initial one.** Fetching `limit + 1` leaves the timeline short once
visibility filtering runs, and a short timeline shifts both `prev_batch` and the
state delta that hangs off its first event.

Ten windows now match across both accounts. One caveat remains: at a
1.3M-position rewind, a long-abandoned room gets 6 of its 10 state entries. The
anchor and the `previous` subtraction are ruled out — that room has no events
before the `since` at all — so the state-group walk is under-returning there.

## 2026-09-01 — M3 done: bundled aggregations, and a fully green baseline

**Both accounts match on all three endpoints.** 39 rooms across room versions 1,
10, 11 and 12; encrypted rooms, backfilled history, redactions, threads, edits.
The only remaining difference anywhere is `m.typing`, which cannot be produced
without the replication stream and is counted by name rather than as a failure.

Bundled aggregations were the last piece: an initial sync is always `limited`,
and Synapse bundles `unsigned.m.relations` into a limited timeline — thread
summaries (count, the fully serialised latest reply, whether the caller
participated), edits, and references. A client given the whole history can
aggregate for itself; one given a window cannot see the replies outside it.

Two rules in there are easy to miss:

- An event that is **itself** an edit or annotation gets no bundle, but a thread
  reply does — so the relation *type* matters, not just its presence.
- Threads are computed only for events that are not themselves a relation, so a
  reply inside a thread does not sprout a nested thread summary. The thread's
  latest reply does still get its own edits and references, one level deep.

*Verified non-vacuous:* disabling the bundling made syncdiff name all three
affected events and exit non-zero.

This was done before M4 deliberately. Starting a large milestone against a
comparator that already fails makes new breakage indistinguishable from old, and
M4 inherits the aggregations anyway — a gappy incremental sync is `limited` too.

## 2026-09-01 — the three /sync items closed; one gap tracked, one found

All three known items are fixed. `@goworker` matches on all three endpoints;
`@test` matches on `/rooms/{id}/initialSync` (30/30) and `/initialSync`, and its
`/sync` now differs only by bundled aggregations.

- **`m.room.aliases` is dropped from the `state` block too**, a second and
  separate place from the timeline filter (`handlers/sync.py:1313`). That was 3
  of the 5 state entries.
- **When both ends of the timeline carry a different event for one key**, only
  one can be reported, and Synapse's choice comes from Python set iteration
  order. Observed consistently to be the later event, which is also the more
  useful answer. That was the other 2.
- **Unread counts fold every thread into the room total** with the default
  filter, and the receipt bound is per thread. Counting only the main timeline
  gave 229 against Synapse's 2,083.
- **`m.typing` is a missing response section**, not just a missing token field —
  the first concrete consequence of having no replication. Tracked by syncdiff
  as a named known gap so it stays visible without drowning the signal.

**Newly surfaced, once the louder differences cleared:** bundled aggregations.
An initial sync is `limited`, and Synapse bundles `unsigned.m.relations` into a
limited timeline — thread summaries with the serialised latest event, edits and
references. Three events across three rooms here. Not a quick fix: it needs
`event_relations`, thread summaries, and a nested serialisation.

## 2026-09-01 — M3: push rules done, `/sync` matching for one account

`m.push_rules` is synthesised and matches Synapse exactly. `/sync` now matches
in full for `@goworker`; `@test` has three narrow items left (below). M1 and M2
still match for both accounts on all 39 rooms.

**Push rules.** `internal/pushrules` ports Synapse's forty built-in rules,
extracted mechanically from `rust/src/push/base_rules.rs` by
`tools/extract-base-rules.py`. The extractor dropped eight rules across two
early versions — six whose `rule_id` wrapped onto its own line, two written
inline — and the comparator caught both in seconds. That is the argument for
generating rather than transcribing, and for having a comparator at all.

**Other things M3 turned up**, in [synapse-notes.md](synapse-notes.md):

- **Initial `/sync` paginates topologically**, not by stream ordering. Only
  incremental syncs use stream ordering (`handlers/sync.py:852`). The earlier
  reading was wrong, and it showed up only in a room with backfilled history.
- **`timeline_contains` is the LAST state event per state key**, not every state
  event in the timeline. Subtracting all of them drops a key whose state changed
  twice inside the timeline, leaving the client interpreting it against nothing.
- **The `state` block is the state at the now token, not current state.** They
  differ whenever the room changed between the token being minted and the query
  running.
- **Unread counts are relative to a read receipt**, and an all-zero rollup row
  does not count as a summary.
- **`prev_batch` and `next_batch` can disagree within one response**, because
  Synapse mutates its own `now_token` while building it.

**Remaining for M3**, all on `@test` and none on `@goworker`:

- 2 rooms where MSC2654's `unread_count` is still low.
- 5 state entries across 2 rooms, where our `state` block and Synapse's pick
  different events for a key, or we include keys it does not.
- 2 rooms where the ephemeral receipt set differs.

## 2026-09-01 — M3 started: initial `/sync` serving, two gaps left

An initial `/sync` is served and most of it matches. Building it turned up four
more ways `/sync` differs from the legacy endpoints, all in
[synapse-notes.md](synapse-notes.md):

- **`/sync` paginates by stream ordering**, the legacy endpoints
  topologically — so `prev_batch` is a live `s...` token there and a historical
  `t...-...` here, and backfilled events (negative stream orderings) are ordered
  quite differently by the two.
- **`summary` is only computed when the filter enables lazy-loading.** With the
  default filter Synapse sends `"summary": {}`.
- **The per-room and whole-account account-data queries disagree** about MSC3391
  deleted entries, exactly as the two receipt queries disagree about `thread_id`.
  That is now two such pairs: which accessor a handler calls is part of the
  contract, not an implementation detail.
- **`device_one_time_keys_count` always reports `signed_curve25519`**, even at
  zero.

Remaining for M3: `m.push_rules` is not yet synthesised into `account_data`
(Synapse layers the `push_rules` table over a long built-in base ruleset), and
one state-block divergence on a key that changed outside the timeline, where
`_calculate_state` may itself be order-dependent.

M1 and M2 still match on both accounts: 9/9 and 30/30 rooms, and `/initialSync`
for each.

## 2026-09-01 — state-group resolver: every gap closed

**Both accounts, both endpoints, fully at parity.** `@test` now matches on all
**30 of 30** rooms (was 24) and on `/initialSync` (was a 501); `@goworker`
still matches 9/9 and `/initialSync`.

Built `internal/store/state.go` — the recursive walk over `state_group_edges`
with `DISTINCT ON (type, state_key)`, ported from
`_get_state_groups_from_groups_txn`, plus `SET LOCAL enable_seqscan = off`
because `state_groups_state` is the largest table in the database and the
planner will sequential-scan it otherwise. The walk is **filtered to the two
keys a visibility decision needs**; resolving the whole map to answer "what is
the history visibility" would mean reading every state event in the room.

`internal/visibility` is now a close port of `filter_events_for_client` rather
than a fast path with a refusal attached: per-event history visibility and
membership, the membership-transition rule for the caller's own events, the
boundary rule for history-visibility events themselves, outliers, erased
senders, retention, ignored users and soft-failed events.

Three things the resolver uncovered, all in
[synapse-notes.md](synapse-notes.md):

- **Redaction is applied on read, and we were not doing it.** A redacted event
  keeps its original body in storage until a background job censors it, so we
  were serving content that had been redacted, in some cases years ago. Synapse
  prunes in the storage layer, so state events need it as much as the timeline.
  The allowlist is per room version and the differences are real.
- **MSC3391**: account data with empty content is deleted and must be omitted.
  Sixteen entries were being served that Synapse drops.
- **`/initialSync` presence cannot be pinned at all** — Synapse reads it with no
  stream bound, so the timestamps move between the two requests. Reported
  separately from clock skew, and only the timestamp is exempt: which users
  appear and their state are still compared.

**Deliberate deviation, newly introduced:** when an erased sender's event should
be served *pruned* (the caller was not joined at the time), we drop it instead.
Redaction-on-erasure needs the same per-room-version prune, which now exists, so
this can be closed; until then dropping withholds content rather than
publishing content that should have been stripped.

## 2026-09-01 — a second account found five more defects

Ran both comparators as `@test:aguiarvieira.pt`: **30 joined rooms, versions 1,
10, 11 and 12; `shared`, `world_readable` and `invited`; five encrypted; several
federated with backfilled history.** The first account had 9 rooms, all local,
all `shared`.

First run: **17 matched, 13 mismatched.** After five fixes: **24 matched, 6
mismatched, and all six are the documented 501 gaps** — four rooms with
`invited` history visibility, two whose visibility has changed (2 and 3 times).
The first account still passes 9/9 plus `/initialSync`.

The five defects, all in [synapse-notes.md](synapse-notes.md):

- **`stream_ordering` is not history order.** Backfilled events get *negative*
  values — a room's early state at -23,964,688 while the user's invite sits at
  +9,100,251 — so membership resolution has to order by
  `(topological_ordering, stream_ordering)`. Ordering by stream reported every
  backfilled event with the membership the user had before being invited.
- **Presence uses Python truthiness.** A stored `last_active_ts` of 0 is omitted
  like a NULL; treating NULL as the only absence emitted a `last_active_ago` of
  1,788,254,542,122 — the whole epoch.
- **The redaction copy runs the opposite way** to the obvious reading: MSC2174
  moved `redacts` *into* content at v11, so `Event.redacts()` reads content for
  v11+ and top level otherwise. Backwards, the field is dropped, not duplicated.
- **`unsigned.delay_id` is `org.matrix.msc4140.delay_id`.**
- **MSC4354 sticky events** carry `msc4354_sticky_duration_ttl_ms`, the time
  *left*, capped at an hour, with the origin timestamp clamped to now. Enabled
  on this deployment; Synapse defaults it off, so it lives in our own
  `experimental:` config block.

**What this says about the corpus.** None of these were reachable with the first
account's rooms. The lesson is not "test more" but that the *shape* of the
corpus decides what the comparator can see: room version 1, backfilled history,
and a room joined after an invite each falsified one specific assumption.

**What it says about the state-group resolver.** `/initialSync` for this account
returns 501, because the snapshot is all-or-nothing and one of its 30 rooms
needs per-event state. For any real account that is the likely outcome, which
makes the resolver the blocking item rather than a nice-to-have.

## 2026-09-01 — M2 complete: `/initialSync` at parity

`/initialSync` matches Synapse across all 9 rooms in one snapshot, stable over
repeated runs. `/rooms/{roomId}/initialSync` still matches 9/9, so M1 did not
regress.

`cmd/syncdiff` grew `-endpoint initial_sync`, and its diff now keys `.rooms` by
room_id, `.presence` by user_id and `.receipts` by room_id, so a difference is
reported against the right entry and the right field instead of as one opaque
"set differs".

**Four upstream differences between the two initialSync endpoints**, all written
up in [synapse-notes.md](synapse-notes.md):

- `/initialSync` builds its serializer config without a requester, so it **never**
  reveals `unsigned.transaction_id` — while the per-room endpoint does.
- The endpoints use **different receipt queries**: the plural one selects
  `thread_id` and applies MSC4102, the singular one does not select it at all.
  And they share a cache, so with the same receipt token they contaminate each
  other. Tolerated in that one key.
- `/initialSync` **re-reads its clock per room**, so its own response is not
  internally consistent. syncdiff accepts age-like fields within 1000ms and
  reports the largest gap: 14–18ms in practice.
- `visibility` on a room is the **room directory's** public/private, not history
  visibility.

*Verified non-vacuous:* building the worker so `/initialSync` passes a requester
made syncdiff report every leaked `transaction_id` and exit non-zero.

**Deliberate gap:** `archived=true` returns 501. Left rooms need the state as it
was at the leave event.

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

## M6 — filters and lazy-loading (2026-09-02)

`internal/filter` is a port of `synapse/api/filtering.py`: a Collection of seven
Filters, with the room-level `rooms`/`not_rooms` filter ANDed into each section.
The `filter` query parameter resolves either as inline JSON or as an ID looked
up in `user_filters`.

Applied to the timeline (limit, types, senders, labels, `contains_url`,
`related_by_*`), the state block, room and global account data, ephemeral,
presence, `include_leave`, `event_fields` and `event_format`, plus the
`blocks_all_*` short-circuits that skip a query entirely rather than running it
and discarding the result.

Lazy loading is `internal/lazyload` plus `internal/handlers/lazystate.go`: the
timeline's senders become `members_to_fetch`, the state block is restricted to
them, an in-memory per-`(user, device)` LRU drops members already sent, and the
summary block — member counts and heroes — is computed and can add hero
memberships back into the state block.

### What the comparator found

Four pre-existing bugs, none about filters:

- **`prev_batch` was wrong on any timeline shorter than the limit.** Synapse
  only moves that token when it trims; we were reporting where pagination
  stopped. Nine rooms at once, the moment a `types` filter produced a short
  timeline. See synapse-notes.md.
- **The timeline re-pagination loop was missing.** One pass is enough for the
  default filter and not for a selective one.
- **`timeline_gaps` was never consulted.** 99,053 rows across 1,392 rooms. A gap
  makes the timeline `limited` with nothing trimmed and, on an incremental sync,
  makes Synapse discard the window and re-paginate back to the gap.
- **The state block was keeping rooms in the response.** Synapse decides whether
  to emit a room before it computes the state delta, and `state` is not one of
  the things that keeps a room alive.

Each of the four is masked by the default filter, and for the same reason: with
a ten-event timeline in a busy room the timeline is always trimmed and never
empty, so `limited` is true anyway and the room is never a candidate for being
dropped. Filters are the first thing that produces a short or empty timeline.

And one thing that is not a bug and cannot be fixed: when `_calculate_state`
holds two events for the same state key, Synapse chooses between them by Python
set iteration order, which is randomised per process. Demonstrated by running
the same choice under eight `PYTHONHASHSEED` values: five pick one event, three
the other. Confirmed pre-existing by rebuilding the M5 worker and reproducing it
on the same account. Now a named comparator bucket.

One further gap was found and deliberately not closed: the `msc4354_sticky`
room section, which we do not build at all. It surfaced because a filter that
excludes a sticky event from the timeline makes Synapse report it in that
section instead. Named in the comparator and in README.md rather than
half-implemented, because closing it also moves `next_batch`.

### Verified

Both accounts, initial and incremental, across ten filters: timeline limit,
`types`, `not_types`, `event_fields`, `event_format: federation`, blocked
presence, blocked rooms, state type lists, `lazy_load_members`,
`include_redundant_members`, `include_leave`. Heroes exercised in two rooms.

Non-vacuity checked twice, as CLAUDE.md requires. Disabling the lazy-load state
restriction fails the lazy filter and still passes the default one. Disabling
the lazy-load cache dedupe fails at two of three rewinds — which is what
establishes that the incremental passes are not vacuous.

## M8 — to-device, and the one write (2026-09-02)

The blocker recorded since M1 was decided rather than worked around: a narrow
`DELETE` on `device_inbox` alone, the owner's call. `device_lists` and the key
counts had worked since M4, so what M8 actually added is `to_device` — and with
it the first non-`SELECT` this project issues.

The grant is kept narrow by construction, not by intention. A separate role
(`gosync_inbox`, `SELECT, DELETE` on `device_inbox` and nothing else), a
separate package and pool (`internal/deviceinbox`), and a startup check that
refuses to run if the role is read-only, cannot delete from `device_inbox`, can
delete from `events`, or can insert into `device_inbox`. A role that is too
powerful fails as loudly as one that is too weak; `internal/store` keeps its
read-only role, and every query in it is still a `SELECT`.

Serving and deleting are one setting. There is no "serve but do not delete":
that would hand a client the same room keys on every sync for ever.

### What the pin could not see

The interesting part was not the section but its token. Above 100 waiting
messages Synapse returns the first hundred and winds the `to_device` field of
its now token back to the last one it sent. Pin us to that already-wound token
and a worker that never winds anything back computes the identical window,
returns the identical hundred messages and reports the identical `next_batch`.
Built deliberately, the defect passed a pinned comparison.

So `syncdiff` gained `-endpoint to_device`, the one comparison here that does
not pin: it sends the messages itself, lets both sides find their own token,
compares the section *and* the token position, then asks both again from their
own `next_batch`. The deliberate defect is named twice by that, and a second
deliberate defect — dropping one message — is named twice as well.

Its second step is also the destructive-endpoint trap in concrete form. Both
sides delete what their `since` acknowledges, so whoever is asked second sees an
inbox the first has been through. Asking us first turned "we skipped the rest of
the backlog" into two identical empty answers; Synapse is now always asked
first. That cost a run to find and is exactly the failure mode
comparability.md's source 3 warns about.

### Verified

Both accounts. Identical sections and `next_batch` for 3, 5, 105 and 120
waiting messages; truncation at 100 and the resume that follows it; deletion
observed at the row level — the syncing device's rows gone, every other device
of the same user untouched. Full M1–M6 regression re-run green: 39 rooms of
`room_initial_sync`, `/initialSync`, initial and incremental `/sync` at rewinds
of 30k, 200k and 500k, `state_after`, and four filters, on both accounts.

One reproducible defect surfaced by that regression is **not** fixed, because
the M6 build reproduces it: with a `types` filter, `incrementalRoomEntry`
reports `limited: true` and a `prev_batch` of `first_event - 1` where Synapse
reports `false` and the `since` token. It has its own path rather than going
through `loadFilteredRecents`, and differs from `_load_filtered_recents` in
three ways. Diagnosed in milestones.md; it is the next thing to fix.

## One timeline path instead of three (2026-09-02)

The defect M8's regression turned up, fixed. `incrementalRoomEntry` and
`archivedRoomEntry` each had their own timeline code; now every path goes
through `loadFilteredRecents`, which is the port of `_load_filtered_recents`
and nothing else is.

Symptom: with a `types` filter one busy room reported `limited: true` and a
`prev_batch` of `first_event - 1` where Synapse reported `false` and the `since`
token, on the same single event. Four things were wrong at once, and each of
them is invisible under the default filter:

- `limited` was computed after filtering and against the load limit. Synapse
  decides it *before* filtering and against the timeline limit: a timeline that
  is short because the filter removed things is not limited.
- The window was loaded at twice the limit. Synapse loads `limit + 1`, and the
  extra row is the question being asked — a room that returns it held more than
  the client may be given.
- `prev_batch` always followed the first event. Synapse moves it only when the
  timeline is trimmed; otherwise it stays where the window began.
- There was no re-pagination loop outside the initial-sync path, so a filter
  that thinned the page was never topped up.

Two smaller things fell out of doing it properly. Backwards pagination by
*stream* ordering, bounded below by `since`, is a different query from the
topological walk the initial path uses — Synapse chooses between them on
whether there is a `since`, because an incremental sync asks what arrived since
the client last looked. And a newly joined room keeps the window's
`upto_token`: Synapse builds its chunk like any other joined room and marks it
newly joined afterwards, where we were handing it to the initial-sync path
whole.

Also tolerated in the comparator: a `device_lists` object Synapse emits and we
do not, when `left` is all it contains. `.device_lists.left` was already a named
tolerance — the same artefact, reported as a missing key rather than missing
entries when `left` was the only thing in it. Narrow on purpose: a
`device_lists` carrying `changed` is still a mismatch.

### Verified

Both accounts, everything green: 39 rooms of `room_initial_sync`,
`/initialSync`, initial `/sync`, incremental at rewinds of 3k / 30k / 200k /
500k / 1M, `state_after`, `to_device`, and the filter set including two shapes
that did not exist before — `{"limit":100,"types":[...]}`, which leaves the
timeline unlimited while removing its oldest events, and `include_leave` with a
`types` filter for the archived path.

Non-vacuity checked twice. Following the first event unconditionally is named on
a `types` filter; never trusting the window is named only by the unlimited-plus-
types filter, because the two prev_batch rules agree whenever nothing was
filtered out of the front of the window. The default filter catches neither, and
that is the whole reason this survived from M3 to M8.

## M7, and the sticky section (2026-09-02)

Receipts, typing and presence were already done, so M7 came down to per-thread
notification counts — a wrong number rather than a missing feature. A client
asking for `unread_thread_notifications` was being handed every thread's counts
folded into the room's single figure, on the one endpoint whose job is to say
how much has been missed.

The filter chooses between two answers to one query: the split pulls threads out
and drops the room's figure to the main timeline alone, and anything else folds
them back in. `org.matrix.msc2654.unread_count` follows the same rule, while the
per-thread entries carry only the notification and highlight counts.

Three rooms on the second account carry a thread section, so the comparison is
not vacuous, and one of them has a threaded receipt ahead of the unthreaded one
— which is what makes the per-thread bounds worth having.

**An upstream bug found while porting it, and deliberately not reproduced.**
`_get_unread_counts_by_pos_txn` adds a room's post-rotation main-timeline counts
to a variable left over from an earlier loop, so they land on whichever thread
was last in an unordered result set. Invisible while everything is summed into
one figure, which is why it survived this long, and not reproducible in
principle: there is no ORDER BY to agree with. Recorded in synapse-notes.md and
README.md.

### The sticky section

Named as a gap at M6 and closed here. It moves the now token — Synapse rewrites
the sticky field of its own `now_token` to the last row it returns — so the
section is loaded once for every joined room BEFORE any room entry is built,
exactly where Synapse does it, and the wound-back position reaches every
prev_batch as well as next_batch. The same shape as M8's to-device wind-back;
having done that one first made this one obvious.

The first comparison caught the detail that reading the MSC would not: "history
visibility MUST NOT be applied" is implemented by *running* the visibility pass
with every sticky event in the always-include set, because that pass is also
what stamps `unsigned.membership`. Skipping it left the events visibly different
from Synapse's.

A live RTC bot on this server posts sticky events every four minutes, so there
is real data for it. The section only shows up when a filter pushes those events
out of the timeline, which is the filter now in the regression set.

### Verified

Both accounts, everything green, with two filter shapes added:
`unread_thread_notifications`, and a timeline restricted to `m.room.member`.
Non-vacuity checked twice — folding thread counts regardless of the filter is
named on three rooms at once, and returning no sticky section is named as a
missing key.

The comparator lost a known gap and gained a tolerance. `.msc4354_sticky` is no
longer expected to differ. `m.typing` is now recognised in BOTH directions: it
is the one field neither side reads from the database, so whoever is asked while
someone is mid-keystroke reports it and the other does not — which had been
showing up as an unexplained extra entry, and cost a few minutes of staring
before the asymmetry in the tolerance became obvious.

## /events (2026-09-02)

The last endpoint in scope, and the one with the weakest case for existing:
zero requests in 21 hours against 558,398 for /sync. It was scoped into M5 and
quietly not built, and "the milestones are done" was not true while it was
missing.

Most of the machinery already existed. Three queries were new — forward
pagination (the limit cuts the OLDEST events here, not the newest), the caller's
own membership events (which land in rooms the joined-set query cannot see, and
are what carry an invite), and updated tags (the stream records only that a
room's tags changed, so the answer is the whole current set, including an empty
one). Typing needed per-room serials in the replication subscriber, because it
is the one source with no table to ask "what changed since?".

### The pin handed us the answer, twice

/events' `end` token is derived from what was returned: its room field is the
position of the last event sent, not the current one. Two deliberate defects --
cutting the newest events instead of the oldest, and jumping the end token to
the current position -- **both passed** a comparison pinned to Synapse's `end`.

The comparator now pins to Synapse's `end` with the room key replaced by the
live one from the probe. Every other field of `end` is a live position, so
pinning to it reproduces Synapse's window; only the room field is derived, and
leaving it live puts the limit and the end token back in our hands. Both defects
are named after that change.

That is the third instance of the same shape, after the to-device wind-back and
prev_batch. It is worth stating as a rule: anything derived from a token we
supplied cannot be tested by supplying it.

### Two things the first comparison caught

Presence for joins is emitted once per JOIN EVENT, not once per user -- three
joins by the same person produce that person's presence three times -- and it
covers other people's joins, not just the caller's. Our first version handled
only the caller's own join and produced none of them.

And an empty chunk is `[]`, not `null`. Go marshals a nil slice as null, and for
this endpoint the empty chunk is the single most common response there is.

### Verified

Both accounts, 24 combinations of rewind and limit, plus an explicit room_id,
raw=1, and no `from` at all. The long poll was driven end to end: a 20-second
request returned the instant a message was sent, carrying exactly that event.
The rest of the regression is unchanged and green.

Two transient mismatches during the sweep, both on the second account and
neither reproducible: one sticky section and one device_lists.changed. The
second was checked against a build from before this work and behaves the same
there, so it is live data rather than a regression -- that account is in
federated rooms busy enough that membership moves between two requests.

## CORS: the thing that made a browser client impossible (2026-09-02)

Found by pointing Element Web at the worker for the first time. The log filled
with `endpoint=unknown method=OPTIONS status=404`: the router registered only
`GET` for each endpoint, so every preflight fell through to the catch-all, and
a browser that gets 404 for a preflight never sends the real request.

Two halves, and doing only the first would have looked fixed while staying
broken. OPTIONS is now answered for ANY path with 204 and no body -- Synapse's
OptionsResource selects itself before routing, so even an unimplemented path
gets a preflight answer -- and the CORS headers are set on EVERY response,
including errors. A preflight that passes is useless if the response to the
real request has no `Access-Control-Allow-Origin`, because the browser discards
it.

The header values were taken from the live server, not from the source, and
match byte for byte -- including `Access-Control-Expose-Headers:
Synapse-Trace-Id, Server`, a header we never emit. Exposing it costs nothing
and diverging would be observable.

Worth recording why this went unnoticed for eight milestones: every test until
now has been curl or syncdiff, and neither sends a preflight or enforces CORS.
The comparator compares bodies. Nothing in it looks at a header, so nothing in
it could have caught this.

## Two bugs a real client found in ten minutes (2026-09-02)

gomuks connected, and every sync came back 400. Both bugs were in code that
passed the whole comparator suite, and neither could have been found by it.

### Refusing a filter Synapse accepts

`{"presence":{"not_rooms":["*"]}}` -- gomuks uploads it, Synapse stores it, and
we answered `"*" is not a valid room ID in presence.not_rooms` to every sync
until the client gave up retrying.

Two upstream facts, both wrong in our port. Synapse validates a filter when it
is UPLOADED and when it arrives inline in the query string; `get_user_filter`
reads a stored one back with no schema check at all. And `presence` uses
FILTER_SCHEMA, which declares no `rooms`/`not_rooms` and allows additional
properties, so a room list there is never checked even on upload.

So stored filters are now parsed unvalidated, inline ones keep their checks
(`filter.NewInline`), and room lists are validated only in the sections whose
schema declares them. My own comment on the old code said "a stored filter
passed validation when it was uploaded, so failing to read it back is our bug"
-- true, and the premise that our validation matched Synapse's was the bug.

### Serving events the server rejected

Found while checking the first fix: with gomuks's `timeline.limit: 100`, one
federated room's timeline started two events later than Synapse's and carried
two member events Synapse never sends. They are REJECTED events --
`rejection_reason = 'auth_error'` -- and `outlier = FALSE` does not exclude
them. A rejected event is not an outlier; it is a real event at a real position
that failed an auth check.

Synapse filters them in every range query (`LEFT JOIN rejections ... IS NULL`)
and asserts in `_get_events_from_cache_or_db` that none escapes. Every
event-range query here now filters `rejection_reason IS NULL`. One of ours
already did -- redactions.go -- which makes it a thing known once and not
generalised.

That room has 13 rejected events in 727, and two of them inside a 100-event
window were enough to move the timeline. The default ten-event timeline never
reached far enough back to include one, which is why eight milestones of
comparison never saw it.

### What this says about the comparator

Neither bug was reachable from syncdiff. The first needs a filter uploaded by a
client -- the comparator only ever sends filters it wrote itself, and it wrote
valid ones. The second needs a timeline long enough to span a rejected event,
and every test used a limit of 5, 10 or 100 against rooms chosen for being
interesting in other ways.

Ten minutes of a real client beat a day of a comparator. Both are now covered:
the filter case by unit tests carrying gomuks's document verbatim, and the
rejected-event case by the `filter=3` comparison, which reproduces it.

## The typing firehose (2026-09-02)

gomuks, against an idle account, at 35 requests a second: 655-byte responses,
27ms apart, the same `since` over and over.

An incremental sync reported the CURRENT typists of every room, with no token
bound at all, while Synapse reports a room's typing only when its serial has
moved past `since_token.typing_key`. So a single person typing anywhere made
every sync return instantly with the same event, and the client -- correctly --
asked again the moment it got an answer. The loop lasts as long as the typing
does.

Fixed by asking the replication subscriber which rooms' typists have changed
since the client's token, which is exactly the machinery `/events` needed a few
hours ago and already had.

Worth noting what could and could not have caught this. The comparator sees one
request at a time, and a single response carries the same typing event under
both behaviours; `m.typing` differences are a tolerated known gap because typing
exists only in memory. Nothing in a pinned comparison can see a loop -- it takes
a client that syncs, stores the token, and syncs again.

Also fixed: the Grafana dashboard's variables. The query variables used the
object form of `query`, which older Grafana ignores, leaving `$job` empty and
every panel with it. They now use the plain-string form, default to All, and
carry `allValue: ".*"` so the expressions match even if the variable never
populates.

## Four invites from 2025 (2026-09-02)

A 500-room account synced against this worker and showed four invites that
Element does not show against Synapse -- the oldest from March 2025. All four
senders are on the account's `m.ignored_user_list` (161 users).

Synapse drops an invite whose sender is ignored, in both the initial and the
incremental path, and we reported every invite `local_current_membership` knew
about. Nothing ever clears those rows: an ignored user's invitation sits there
for ever, so the bug is permanent and grows with the account.

Neither test account ignores anybody, which is why 39 rooms of comparison never
saw it. That is now three defects in a row that needed a real account rather
than a real client -- scale and history, not behaviour.

MSC4155 invite rules are the other half of Synapse's test and are not
implemented. No account on this server has that config, so it changes nothing
today; recorded in README.md as a gap.

## Lazy state, because 500 rooms cost 209 seconds (2026-09-02)

A real account -- 500 rooms, big public ones among them -- did an initial sync
against this worker: **223,720,043 bytes in 209 seconds**, at 1.5GB resident.
The client then spent eight minutes writing it inside one SQLite transaction,
which locked its own database and made everything else look broken.

The response SIZE turned out to be right. Same request, same account, both
workers: Synapse 9,607,785 bytes, us 9,583,417 -- 0.25% apart, and we were
marginally faster at 30 rooms. A fresh login on a 500-room account is simply a
very large sync, and parity forbids making it smaller.

The 209 seconds and the 1.5GB were ours. `syncStateBlock` resolved each room's
ENTIRE state map twice -- `StateIDsAt` and `FullStateForGroup` -- and then threw
nearly all of it away in `restrictToMembers`. On a public room with tens of
thousands of members that is tens of thousands of rows fetched to keep three.
`roomHasName` was worse in its way: a full state resolution to answer a yes/no
question about two keys, once per room per initial sync.

Now the queries ask for what is kept: everything except members, plus the
members the timeline mentions. Measured on a 3,094-entry room: 49 rows instead
of 3,094, 6.1ms instead of 14.7ms. The win grows with the room.

### Two things this cost me

I nearly shipped a conclusion twice over. The first timing run showed us at
5.99s against Synapse's 1.22s and looked like a serious regression -- it was a
cold cache on a just-started process; warm, we are at 1.0s against 1.1s. And a
`prev_content.pinned` difference appeared on the new build three runs running
while the old build passed, which looked exactly like causation; ten minutes
later it had vanished from both. It was the shared-event-cache artefact in the
busiest room on the server.

The lazy comparisons then flapped, which was also my fault: two workers were
running against the same account, and each keeps its own lazy-load member cache.
With one worker it is 10/10. That is comparability source 8 biting the person
who wrote it down.

## Ten at a time (2026-09-02)

The lazy state fetch was necessary and not sufficient: the 500-room initial sync
went from 209 seconds to 193. Per room we are fine -- warm, we serve the same
9.6MB initial sync in 1.1s against Synapse's 1.3s -- but we were doing 500 rooms
one after another, and each room is a handful of sequential round trips to a
database with a 17GB state table.

Synapse builds ten room entries at once (`concurrently_execute(...,
room_entries, 10)`, sync.py:2700). We now do the same, with the same limit.

Measured cold on 30 rooms, which is small enough that only three batches run:
**5.4-6.2s sequential, 2.0-2.2s concurrent**. At 500 rooms the ratio should be
closer to the full ten.

The lazy-load member cache is written while rooms are built, so concurrency
changes which members a later room thinks it has already sent. That is a reason
to match Synapse's number exactly rather than pick a bigger one -- and the race
detector is clean, because the cache and the LRUs were mutex-guarded from the
start.

## The main account, and what it found (2026-09-02)

654 joined rooms, 308 left, 4 invites. The comparison had to block presence to
be readable at all -- an initial sync reads presence with no stream bound, and
on an account with thousands of co-occupants it churns faster than two requests
can be made -- but with `{"presence":{"not_types":["*"]}}` the noise cleared and
left a real defect.

**Rejected events must consume the limit.** Synapse pages the events table with
no rejection predicate and drops rejected events afterwards, so a rejected event
takes a slot in the LIMIT and is not returned. This morning's fix filtered them
in SQL instead, which is not the same thing: the page then reaches further back.
One bridged matrix.org room, whose recent history is entirely `auth_error`
events, returned a message 63,000 stream positions old that Synapse never sends,
with `prev_batch` and the state block dragged along with it. Now the rows are
paged as Synapse pages them and dropped in Go afterwards, and that room returns
the empty timeline Synapse returns.

That is two corrections to the same rule in one day. "Do not serve rejected
events" was right; "filter them in the query" was not.

**What remains** is small: 5 rooms of 654 where Synapse's lazy state block
carries one extra member event, deterministically, that nothing in
_compute_state_delta_for_full_sync accounts for. Recorded in synapse-notes.md as
an open question with the evidence, including the three-run check that shows
Synapse is self-consistent about it.

Everything else that showed up was live data: presence, receipts, typing, and
unread counts drifting between the two requests on an account this busy.

## The typist who never stopped (2026-09-02)

Visible symptom: a user left "typing" in a room for ever. Cause: `typingEvent`
returned nil for an empty typist list, so "stopped typing" was never sent.
Synapse always builds the event -- `{"user_ids": []}` -- for any room whose
typing serial moved, and that empty event is the only thing that clears the
indicator; there is no timeout at the client end.

Bounding typing by the token this evening made it visible rather than causing
it: before that, the current typists were re-sent on every sync, so a client
would keep being told somebody was typing and never that they had stopped.
Either way the indicator stuck; only the reason changed.

Initial and incremental now follow the same rule with different starting
positions -- report a room whose serial moved past the client's token, then
render whatever the list is. On an initial sync that position is 0, which is
what Synapse's typing source is given when there is no `since`.

### The short polls were not ours

The same report came with 1.5 syncs a second and 15% CPU, which looked like the
hot loop again. It is not. Measured against the same account with the same
token, Synapse answers in 0.05-0.62s with one or two presence events and
339-481 bytes; we answer in ~1s with the same shape. An account in 654 rooms
gets a presence update roughly every half second, both implementations return as
soon as they have something to say, and Synapse would drive a TIGHTER loop than
we do.

So the cadence is inherent to the account, and the CPU is what 1.5 syncs a second
over 654 rooms costs us. Synapse spends less because it answers "has this room
changed since X?" from in-memory stream-change caches before touching the
database; we ask the database every time. That is the next optimisation if one is
wanted, and it is not a correctness problem.
