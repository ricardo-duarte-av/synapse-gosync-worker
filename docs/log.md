# Work log

Newest first. Numbers are measurements, not estimates.

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
