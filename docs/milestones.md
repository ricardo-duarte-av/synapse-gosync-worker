# Milestones

Ordering reflects two deliberate choices: the legacy endpoints come early as
warm-ups (they exercise auth + DB + room state end to end at a fraction of
`/sync`'s complexity), and initial sync comes before stream tokens.

Status is recorded here; what actually happened is in [log.md](log.md).

| # | Milestone | Status |
|---|---|---|
| M1 | Skeleton, auth, `/rooms/{roomId}/initialSync` | **done — 9/9 rooms at parity** |
| M2 | `/initialSync` | **done — at parity** |
| M3 | Initial `/sync` (no `since`) | **done** — both accounts, all endpoints |
| M4 | Stream tokens and incremental `/sync` | **done** — 10 windows, two accounts; one deep-history caveat |
| M5 | Long-polling and Redis replication | **done** |
| M6 | Filters and lazy-loading | **done** |
| — | MSC4222 `state_after` | **done** |
| M7 | Ephemeral: receipts, typing, presence | **done** |
| M8 | To-device and device lists | **done** |
| — | MSC4354 sticky events | **done** |
| M9 | Soak, then possibly the promotion ladder | not started |
| M10 | Stream-change caches (`internal/streamcache`) | **done** |
| M11 | Sliding sync: the connection store | **done** |
| M12 | Sliding sync: the endpoint, lists and rooms | **done** — served on both paths |
| M13 | Sliding sync: extensions | **done** — all seven |

M10 onwards exist because sliding sync came into scope on 2026-09-03; the plan
is in `.claude/plans/`. M10 was pulled out of it as its own milestone because
it pays for itself on classic sync alone.

## M1 and M2 — done, no gaps

Both endpoints match Synapse on both test accounts: 9/9 and 30/30 rooms, and
`/initialSync` for each. The state-group resolver closed the two deliberate
gaps that M1 shipped with.

One deviation remains, and it is smaller than it was: when an erased sender's
event should be served **pruned**, we drop it. The per-room-version prune now
exists (`clientevent.Redact`), so this is a wiring job rather than a missing
capability.

`archived=true` and rooms the user has *left* are still unimplemented: they need
the state at the leave event, which the resolver can now answer — also a wiring
job.

## M2 — done

`/initialSync` matches Synapse across all 9 rooms, stable over repeated runs.
The two initialSync endpoints differ upstream in four ways that are easy to miss;
see [synapse-notes.md](synapse-notes.md) before touching either.

`archived=true` returns 501: left rooms need the state at the leave event.

## M3 — done

Initial `/sync` matches Synapse for **both** test accounts, alongside
`/initialSync` and `/rooms/{id}/initialSync`. That is 39 rooms across room
versions 1, 10, 11 and 12, encrypted rooms, backfilled history, redactions,
threads and edits.

One known gap remains and cannot be closed here: **`m.typing`**. Typing is never
persisted, so no query can produce it; it arrives only over the replication
stream (M5). syncdiff counts it by name rather than treating it as a mismatch.

An incremental `/sync` (`since` present) returns 501; that is M4.

## M4 — done

An incremental `/sync` matches Synapse across **ten test windows** on both
accounts (rewinds of 3,000 / 30,000 / 200,000 / 500,000 / 1,000,000 stream
positions). The `invite`, `knock` and `leave` sections are implemented.

**One caveat, in one room.** At a 1.3M-position rewind — far beyond anything a
real client sends — a room the account left long ago gets 6 of its 10 state
entries. The anchor event and the `previous` subtraction are both ruled out (the
room has no events at all before that `since`), so the state-group walk itself
is under-returning for that room. Worth chasing when the state resolver is next
touched.

`to_device` is not implemented; that is M8.

## M5 — done

The worker follows the replication stream, so it can answer without being told
what time it is. Typing works, 11 of 14 token fields match exactly, and a
long poll woke in 3.15s for a message sent at 3s.

## M6 — done

Filters are honoured: the timeline limit, `types` / `not_types`, `senders`,
`rooms`, `contains_url`, labels, `related_by_*`, `include_leave`,
`event_fields`, `event_format`, and the whole `blocks_all_*` family of
short-circuits. Lazy loading works, summary and heroes included.

Verified against both accounts, initial and incremental, with filters for
timeline limit, type allow and deny lists, `event_fields`, `event_format:
federation`, blocked presence, blocked rooms, state type lists,
`lazy_load_members`, `include_redundant_members` and `include_leave`.

Five things came out of it that were not filter bugs:

**`prev_batch` on an untrimmed timeline was wrong, and had been all along.**
Synapse's `_load_filtered_recents` keeps `room_key` at the token it was given
and only moves it when the timeline is trimmed to the limit — the pagination
cursor is assigned to `end_key`, a different variable. So a timeline shorter
than the limit reports a `prev_batch` equal to the sync point. We were
reporting where the topological walk stopped. It never showed up before
because with the default filter every room in the corpus had more than ten
events and always trimmed. A `types` filter is the first thing that produces a
short timeline in a busy room.

**The timeline re-pagination loop was missing.** Synapse re-paginates up to five
times while the timeline is under the limit, because both the client's filter
and history visibility thin the page after it is loaded. One pass is enough for
the default filter and visibly not enough for a selective one.

**`timeline_gaps` was never consulted.** A hole in a room's history makes the
timeline `limited` with nothing trimmed, and on an incremental sync makes
Synapse discard the window and re-paginate back to the gap. 99,053 gap rows
across 1,392 rooms here, and invisible until a filter produced a timeline short
enough not to be trimmed.

**The state block was keeping rooms in the response.** Synapse decides whether
to emit a room *before* computing its state delta, and `state` is not one of
the things that keeps it alive. We were checking afterwards and counting state,
so we emitted rooms Synapse omits.

**A state-key collision that Synapse decides at random.** See
[comparability.md](comparability.md); it is now a named tolerance bucket. It
predates M6 — the pre-M6 worker reproduces it — and it is not fixable.

Two deliberate gaps, both recorded in README.md and named by the comparator:
`unread_thread_notifications`, and the `msc4354_sticky` room section. The
second was found the same way: a sticky event still in the timeline is removed
from that section by Synapse, so the section appears only once the event ages
out — or the moment a filter excludes it from the timeline.

## The incremental timeline path, fixed (2026-09-02)

Surfaced by the M8 regression and reproduced on the M6 build, so it predated
that milestone. With `{"room":{"timeline":{"limit":5,"types":
["m.room.message"]}}}` one busy room reported `limited: true` and a `prev_batch`
of `first_event - 1` where Synapse reported `false` and the `since` token — the
same single event on both sides.

The cause was that `incrementalRoomEntry` and `archivedRoomEntry` each had
their own timeline path instead of going through `loadFilteredRecents`, and all
three differed from `_load_filtered_recents`. `loadFilteredRecents` is now the
single port of that function and every path calls it: initial sync, incremental,
newly joined, and archived. What changed:

1. **`limited` is decided before any filtering**, from how many events the
   window held against the TIMELINE limit — not from how many survived, and not
   against the load limit. A timeline that ends up short because the filter
   removed things is not limited; the client has everything there was.
2. **The window is loaded with `timeline_limit + 1`**, which is what Synapse
   asks for. The extra row is the question, not spare capacity: a room that
   returns it held more than the client may be given. Loading a larger page
   makes that unanswerable.
3. **`prev_batch` only moves when the timeline is trimmed.** Otherwise it stays
   at the room's `upto_token` — the start of the loaded chunk, which is the
   `since` token itself for a room whose events all fit. This is the M6
   `prev_batch` finding again, in the two paths M6 did not touch.
4. **The re-pagination loop now runs on every path**, so a filter that thins the
   page pulls older events in as Synapse does — bounded by `since`, and ordered
   by stream position rather than topologically. Synapse picks between the two
   orderings on whether there is a `since`: an incremental sync asks what
   arrived since the client last looked, which is a question about the order the
   server received things in. `Store.PaginateBackwardsStream` is the new half of
   that pair.
5. **A newly joined room keeps the window's `upto_token`.** Synapse builds the
   chunk like any other joined room and marks it newly joined afterwards, so an
   untrimmed timeline's `prev_batch` is the chunk start, not the now token. Ours
   was handing the room to the initial-sync path wholesale.

Non-vacuity checked twice, as CLAUDE.md requires. Following the first event
unconditionally is named on a `types` filter; never trusting the window is named
by `{"timeline":{"limit":100,"types":[...]}}` — a filter that leaves the
timeline UNLIMITED while removing its oldest events, which is the only shape
that separates the two prev_batch rules. The default filter catches neither.

## M8 — done

`device_lists.changed` / `left` and the key counts had worked since M4. What M8
added is `to_device`, and with it **the only write this worker makes**.

**The blocker was decided rather than worked around** (2026-09-02, the owner's
call): a narrow `DELETE` on `device_inbox` alone. Synapse's `/sync` deletes the
to-device messages a device has acknowledged, and serving the section without
deleting hands a client the same room keys on every sync for ever — so the two
are one decision, not two. See [decisions.md](decisions.md).

The grant is kept honest by three things rather than by intention:

- a **separate role** (`deploy/device-inbox-role.sql`), granted `SELECT, DELETE`
  on `device_inbox` and nothing else;
- a **separate package and pool** (`internal/deviceinbox`), so `internal/store`
  keeps its read-only role and its startup check, and every query in it is
  still a `SELECT`;
- a **startup check that refuses to run** if the role is read-only, cannot
  delete from `device_inbox`, can delete from `events`, or can insert into
  `device_inbox`. A role that is too powerful fails as loudly as one that is
  too weak.

Verified against both accounts: identical `to_device` sections and identical
`next_batch`, the 100-message truncation, the resume that follows it, and the
deletion — 3 messages gone for the syncing device, and every other device of
that user untouched to the row.

**What the pin hides, and the comparator's answer to it.** With more than 100
messages waiting Synapse returns the first 100 and winds the `to_device` field
of its now token back to the last one it sent. Pin us to that already-wound
token and a worker that never winds anything back computes the same window,
returns the same hundred messages and reports the same `next_batch` — the defect
is invisible. `syncdiff -endpoint to_device` is therefore the one comparison
that does not pin: it sends the messages itself, lets both sides find their own
token, and then asks both again from their own `next_batch`. Confirmed by
building the defect deliberately: unpinned it is named twice, pinned it passes.

Order matters in that second step, and it is the destructive-endpoint trap in
concrete form: whoever is asked second sees an inbox the first has already been
through. Ask us first and we delete the very messages Synapse is about to be
asked for, turning "we skipped the rest of the backlog" into two identical empty
answers. Synapse is always asked first.

## M7 — done, and the sticky section with it

Receipts, typing and presence had been delivered by M1–M5, so what M7 actually
owed was **per-thread notification counts**, and it is a wrong number rather
than a missing feature: without it, a client that asks for
`unread_thread_notifications` is given every thread's counts ADDED INTO the
room's single figure. Too high, on the one endpoint whose whole job is to tell a
client how much it has missed.

The filter decides which of two answers the same query produces. Asking for the
split pulls threads into their own section and drops the room's figure to the
main timeline alone; any other filter folds them back in. Both are now built
from one `RoomNotifCounts`, and the section is emitted under the stable name and
the MSC3773 one together, as Synapse does — and only when there is at least one
thread.

**One upstream bug is NOT reproduced.** In `_get_unread_counts_by_pos_txn`, the
loop that adds post-rotation counts for summarised threads writes main-timeline
counts into `counts`, a variable left over from the previous loop — so they land
on whichever thread happened to be last in an unordered result set. It is
invisible while everything is summed into one figure, which is why it never
showed before, and it is not reproducible even in principle: there is no ORDER
BY to agree with. We attribute main-timeline counts to the main timeline. If a
comparison ever lands on it, that is the cause.

**The `msc4354_sticky` section**, named as a gap since M6, is also done. Three
details carried across:

- The section is loaded ONCE for all rooms, before any room entry is built,
  because it moves the now token — Synapse rewrites the sticky field of its own
  `now_token` to the last row returned, and that reaches every `prev_batch` in
  the response as well as `next_batch`. Same shape as the to-device wind-back
  from M8.
- An event already in the timeline is REMOVED from the section. That is what
  made the gap invisible: it only appears once an event ages out of the
  timeline, or the moment a filter excludes it.
- History visibility is not applied — but that means running the ordinary
  visibility pass with every sticky event in the always-include set, not
  skipping it. Skipping it loses `unsigned.membership`, which the first
  comparison caught.

Expired and soft-failed events are excluded, so the answer depends on the wall
clock as well as the stream position. The comparator's `_gosync_time_now` pin
covers that; without it the two sides disagree by the milliseconds between them.

### Verified

Both accounts, everything green, including two filter shapes added for this
work: `unread_thread_notifications`, and a timeline restricted to
`m.room.member` — which is what pushes the live RTC sticky events out of the
timeline and into their own section. Three rooms carry a thread section and one
carries sticky events, so neither comparison is vacuous.

Non-vacuity checked twice, as ever. Folding thread counts regardless of the
filter is named on three rooms at once, counts and `unread_count` alike;
returning no sticky section is named as a missing key.

The comparator lost a known gap and gained a tolerance: `.msc4354_sticky` is no
longer expected to differ, and `m.typing` is now recognised in BOTH directions.
It is the one field neither side reads from the database, so whoever is asked
while someone is mid-keystroke reports it and the other does not — which had
been reported as an unexplained extra entry.

## /events — done

The last endpoint in this worker's scope, and the one with the weakest case for
existing: **zero requests in 21 hours**, against 558,398 for `/sync`. It was
scoped into M5 and quietly not built. It is built now because "M1 to M8 are
done" was not true while a target endpoint was missing, and because it is the
only thing here that exercises the notifier without the whole sync machinery in
front of it.

It is a much smaller endpoint than /sync: one flat chunk of events and EDUs
between two tokens, assembled from five sources in a fixed order (room,
presence, typing, receipts, account data) and then the presence of anyone who
just joined. Most of the parts already existed. What was new:

- **`RoomEventsForward`**, because /events cuts the OTHER END of the window. A
  sync returns the newest events and calls the timeline limited; /events streams
  forward from where the client stopped, so a capped page is the OLDEST events
  and the next request resumes after them.
- **`MembershipEventsForUser`**, which is not redundant with it: an invite or a
  leave lands in a room the caller is not joined to, so a query over the joined
  set cannot see it. Without it /events can never tell a client it has been
  invited anywhere.
- **`UpdatedTags`**, since the tag stream records only THAT a room's tags
  changed. The answer is always the room's whole current tag set, including an
  empty one — which is how a client learns the last tag was removed.
- **Per-room typing serials in the replication subscriber.** Typing is the one
  source with no table to ask "what changed since?", so the only record of when
  a room's typists last moved is the one now kept as the rows arrive. Rooms
  quiet since startup have no entry, which is the same limitation typing has
  everywhere here.

### The pin hides this endpoint's answer, twice

/events' `end` token is derived from what was returned: its room field is the
position of the LAST EVENT SENT, not the current one. Pin us to Synapse's `end`
and both the limit and the token are handed to us. Two deliberate defects — one
cutting the newest events instead of the oldest, one jumping the end token to
the current position — **both passed** a comparison pinned that way.

The comparator now pins to Synapse's `end` with its ROOM KEY REPLACED by the
live one from the probe. Every other field of `end` is that source's live
position, so pinning to it reproduces Synapse's window exactly; only the room
field is derived, and leaving it live puts the limit and the end token back in
our hands. Both defects are named after that change. The cost is a race in the
milliseconds between probe and request, which shows only when the window holds
fewer events than the limit — so compare with a rewind large enough that the
limit is what does the cutting.

This is the third instance of the same shape, after the to-device wind-back and
`prev_batch`: **anything derived from a token we supplied cannot be tested by
supplying it.**

### Verified

Both accounts, 24 combinations of rewind (30k / 200k / 500k / 1M) and limit
(3 / 10 / 50), plus an explicit `room_id`, `raw=1`, and no `from` at all. The
long poll was driven end to end: a request with a 20s timeout returned the
instant a message was sent, carrying exactly that event.

One defect found by the first comparison and worth recording: presence for
people who just joined is emitted **once per join event, not once per user**.
Synapse loops over the events and extends the chunk each time, so three joins by
the same person produce that person's presence three times. Our first version
only handled the caller's own join and produced none of them.


## M10 — done

`internal/streamcache` answers "has entity X changed since position P?" from
memory, a port of Synapse's `StreamChangeCache`. Six caches — events and
receipts keyed by room, membership, account_data, to_device and presence keyed
by user — fed by a second replication listener and prefilled at every connect.

**A false positive is free; a false negative is a lost event.** Every decision
in the package resolves in that direction, and the horizon is what makes the
useful answer safe: the cache is a complete record of changes above it, so an
entity it has never heard of provably did not change in a range it covers. That
is why the horizon may never move backwards, and why eviction raises it.

Two gates use it so far:

| Gate | 5 quiet syncs, caches off | caches on |
|---|---|---|
| `PresenceSince` | 5 queries | **2** |
| `TimelineGaps` | 5 queries | **0** |

The presence query is the expensive one: a correlated subquery over
`presence_stream` and `current_state_events`, 25 ms warm and 430 ms cold, 803 s
of database time over the measured window, on the request path of every sync.

### Two defects found on the way, both pre-existing

**Batched replication rows were applied at the wrong position.** A batch's rows
carry the literal token `batch` and only the last names a position. The worker
passed 0 for the rest, which was harmless for a notifier and would have been a
false-negative generator for a stream cache: the plan's proposed fix —
substitute the last known position, as `updateTyping` does — files a change
*below* where it happened, which is precisely the answer that loses an event.
Synapse buffers the rows and applies them at the final token
(`ReplicationCommandHandler._pending_batches`); so do we now.

**`state` and `state-all` rows on the events stream named no room.** The events
stream merges three sources with three row shapes, and only `ev` was parsed. A
current-state delta is emitted alongside every `ev` row for a state change, so
one join woke every parked client twice. This is the same class as the `caches`
and `presence_federation` findings and was found the same way — by needing the
row's subject for something else.

### What the comparator cannot test here, and why

`cmd/syncdiff` says "ok" to a presence gate hard-wired to "nothing changed", on
both accounts and for opposite reasons. The test account has 9 rooms and sees
**zero** presence events in an incremental sync, so there is nothing to lose.
The main account sees **189**, and the *unmutated* build already mismatches
there — presence is live and unpinnable, which is why CLAUDE.md §6's
large-account recipe blocks it with `not_types: ["*"]`.

So the gates are tested directly instead, in
`internal/store/streamgates_live_test.go`: for every range the cache claims to
know, its answer must agree with the query it exists to skip. Over-reporting is
allowed and counted; under-reporting fails. Both gates were verified to bite by
mutation, and the two mutations the live tests *cannot* see are named in that
file with a pointer to the unit tests that do.

A fourth thing worth keeping: a cache configured to hold nothing used to report
`gosync_stream_cache_armed 1`. It behaves exactly like a disarmed cache, so that
drew a flat, healthy line for a cache answering nothing — which is how a cache
turned off by a config change goes unnoticed for months.


## M11 — done

`internal/slidingstore` holds sliding sync's per-connection state, and
`deploy/sliding-sync-role.sql` creates the role and schema it writes. Nothing is
user-visible yet; the endpoint is M12.

This is the project's **second** write, and unlike the first it is not a
read-only workload that needed one exception — it cannot be made read-only at
all. Why, and why our own tables rather than Synapse's, is in
[decisions.md](decisions.md). The containment is the same shape as the
to-device grant: one package, one pool, one role that owns the `gosync` schema
and has nothing in `public`, and a startup check that refuses a role which can
read `public.events`.

### What is hard here, and it is not the SQL being long

Every other defect this project has found was visible in one response body.
"This room was marked sent but never was" only shows up on the request after
next. So `live_test.go` tests the properties directly against the real database,
and all of them were verified to bite by mutation on 2026-09-03:

| Break | Caught by |
|---|---|
| `RecordUnsentRooms` does nothing | `TestLivePreviouslyKeepsItsToken` |
| Position ownership unchecked | `TestLiveAPositionBelongsToOneTriple` |
| Forks never pruned | `TestLiveUsingAPositionPrunesTheForks` |
| `HasUpdates` always true | `TestLiveAnUnchangedResponseWritesNothing` |
| `pos=0` treated as unknown | `TestLiveUnknownPositionIsNotAnError` |
| required_state rows not reused | `TestLiveRequiredStateIsDeduplicated` |
| Copy-forward **and** full upsert removed | two tests |

That last row is the interesting one. Each connection position must be a
complete snapshot, and Synapse guarantees that **twice**: by copying the
previous position's rows forward, and by upserting the flattened state rather
than only the changes. Removing either alone leaves the tests green; removing
both fails them. Both are kept — they cover the same guarantee from opposite
ends, one from the row already stored and one from the state in memory — but a
reader looking for the single place completeness is enforced will not find it,
so `copyForward`'s comment says so.

Two more things the tests could not see until they were rewritten to look
properly, and both are worth the pattern:

- **The fork test was not building forks.** Reading a position is itself what
  prunes, so a loop that reads before each write never accumulates more than
  two positions. The real case — several responses computed from one position
  that is not read again in between — had to be constructed deliberately.
- **The dedup test could not see a broken dedup.** The collector in `loadState`
  tidies up behind it: each request writes a fresh copy of the required state,
  orphans the previous one, and the next read deletes it. The row count stays
  right while every request pays an extra INSERT and DELETE on the largest of
  the six tables. The test asserts the row's **identity** instead — reuse means
  the id does not move.

### Deliberately not wired yet

The pool, the config section and the reaper's caller land in M12 with the
handler that needs them. Recorded here rather than left loose because this
project has been bitten once already by a documented contract with no
implementation behind it — `PurgeCaches()` had no callers for three milestones.
`DeleteOldConnections` must not repeat that: without it the six tables grow
without bound, because reading a position prunes only its own connection's
forks and nothing prunes a connection whose client simply went away.


## M12 — in progress

Room lists are done and validated against the real Synapse. The endpoint itself
is not registered yet: the per-room result is what remains, and half a response
body is worse than none.

### Done

`internal/slidingsync` holds the request model, `required_state` normalisation,
and the room-list computation — the new-tables path of
`_compute_interested_rooms_new_tables`, plus `filter_rooms_using_tables` and
`sort_rooms`. `internal/store/slidingsync.go` reads the three materialised
tables Synapse's event persister maintains, which is what makes a 654-room list
computable without a state resolution per room.

On the main account: **653 rooms, ordered stably by activity**, and the boolean
filters partition it exactly — 81 encrypted + 572 unencrypted, 66 spaces + 587
non-spaces, 158 DMs.

The materialised tables were checked against the truth they derive from before
anything was built on them: 653 joined rooms agree with `local_current_membership`,
and 100 of 100 room names agree with the `m.room.name` state event.

### The order is right, and finding that out took a detour

Compared against `av-sync-worker-2`, our list came back with the same 9 rooms in
a **different order** — which looked like a straightforward bug and was not.
Synapse skips sorting entirely when one range covers the whole list:

```python
# Optimization: If we are asking for the full range, we don't
# need to sort the list.
```

…and then returns the rooms in Python dict order, matching neither
`event_stream_ordering` nor `bump_stamp`. Asked for a **partial** range —
`[[0,4]]` against those 9 rooms — Synapse sorts, and its order is **identical to
ours**. `[[0,8]]` and `[[0,99]]` are not.

So the sort is verified, and there is a new entry in
[comparability.md](comparability.md): compare the room SET for a full-range
list and the room ORDER only for a partial one.
`internal/slidingsync/parity_live_test.go` makes that split, and it is the first
test in this project that checks a sliding sync answer against Synapse's.

We sort unconditionally, which is a deliberate deviation. Skipping it would buy
nothing — the metadata is already fetched, so it is an in-memory sort of a few
hundred entries — and would make our own answers irreproducible, which costs us
more than it costs a client that has the whole list and `bump_stamp` to sort by.

### `_required_state_changes` — done

The ~380-line pure function that decides what must be re-sent when a room's
required state changes. One idea repeated in several places holds it together:

> A state key is only forgotten when the client removes it **and** the state
> behind it changed.

Drop `m.room.topic` from `required_state` and add it back, and the server must
not re-send a topic the client already has. Everything in there that looks like
over-complication is a version of that rule.

**It is tested against Synapse's own test table**, extracted mechanically from
`tests/handlers/test_sliding_sync.py` into
`internal/slidingsync/testdata/required_state_changes.json` — 31 cases, each run
twice (with the state deltas and without, as Synapse runs them), 62 assertions.
Copying the reference implementation's tests rather than writing our own from a
reading of its source is deliberate: tests we wrote would mostly encode our
reading of the code, which is the thing that needs checking. The extractor is
committed beside the fixture so it can be re-run on a Synapse upgrade.

All 62 passed on the first run. Seven mutations were then tried, and **three
survived the table** — each a real gap, each now covered by a test of our own:

| Mutation the table missed | Now caught by |
|---|---|
| The remembered-keys cap removed entirely | `TestRememberedStateKeysAreCapped` |
| The cap dropping previously-sent keys instead of backfilling | same |
| `$LAZY` fetched as if it were a state key | `TestLazyIsIgnoredForNonMembershipTypes` |

The cap is covered by two *procedural* tests beside Synapse's table, which
generate their inputs rather than listing them; those are ported by hand. The
`$LAZY` case Synapse's tests do not cover at all: the branch looks unreachable
and is reachable only for a client writing `$LAZY` as the state key of a
non-membership type.

Three findings are recorded in [synapse-notes.md](synapse-notes.md), including a
**leaked Python loop variable** that decides which members get remembered as
lazily loaded. We implement the evident intent and record the deviation:
reproducing a scoping accident is not possible in Go, and the value only gates a
cache whose worst failure in either direction is one member event sent twice.

### `get_room_sync_data` — mostly done, and compared against Synapse

The per-room result: the timeline, `required_state` resolution, heroes,
membership counts, the room name and avatar, `bump_stamp`, `limited`,
`num_live`, `prev_batch` and the invite/knock stripped state.

**Compared field by field against a real Synapse sync worker**, five rooms on
the test account and five on the 654-room account, for an initial request:
`name`, `avatar`, `joined_count`, `invited_count`, `is_dm`, `heroes`,
`limited`, `bump_stamp`, the `required_state` selection, the timeline's event
IDs and order, and the event bodies byte for byte. Also with
`["m.room.member","$LAZY"]`, which selects members differently.

Only an *initial* request is compared. An incremental one is a delta against
per-connection state, and the two sides keep that in different tables with
different positions — putting them in the same place needs a lockstep driver,
which is syncdiff's job.

Two things worth carrying forward:

- **`notification_count` and `highlight_count` are hard-coded zeros upstream.**
  Synapse's own comment: *"These are just dummy values. We could potentially
  just remove these since notifications can only really be done correctly on
  the client anyway (encrypted rooms)."* Ours are zero for parity, not by
  omission.
- **The stored PDU is not a client event.** The first version emitted
  `event_json.json` directly and produced five timeline events with **empty
  event IDs** — room version 3 and later derive the ID from a hash rather than
  storing it. `unsigned` also has to be rebuilt from an allowlist and a redacted
  event pruned on read. All of that already exists in `internal/clientevent`,
  and sharing it is why the two endpoints agree byte for byte on an event body.
  The parity test caught this immediately; nothing else would have.

### `internal/eventfilter`, and three gaps the widened comparison found

Closing the visibility gap meant lifting `filterVisible` out of
`internal/handlers`, where sliding sync could not reach it. Two more things came
with it once the comparison could see far enough: **redactions** and **bundled
aggregations**. All three now live in `internal/eventfilter` and are used by
both endpoints; `internal/handlers` keeps thin wrappers so fifteen call sites
are untouched and the move's diff is readable.

**Redaction was the serious one.** Synapse applies redactions on READ rather
than rewriting the stored event, so an endpoint that skips that step serves the
**original content** of a redacted event. Not cosmetic, and invisible until a
compared room happened to have a redaction in its recent timeline.

`unsigned.membership` (MSC4115) and `unsigned.transaction_id` fell out of the
same change — membership is what the visibility decision produces, and
`transaction_id` needs the requester's device, now asked for over `/whoami`
rather than read from `access_tokens`.

The parity comparison now covers **all of `unsigned`** except what genuinely
cannot match, and runs on all three accounts.

#### What the widened window taught

The first version compared the top five rooms and found nothing. Five rooms was
every room the test account has at `shared` history visibility, so a build that
threw the visibility filter's results away compared **clean**. Widening to fifty
found five real differences in one run: three bundled aggregations, one
redaction, and one `prev_content`.

Two of those five turned out to be the comparator's fault, and both are now in
[comparability.md](comparability.md) as source 11:

- **Clock-derived fields nest.** A bundle contains whole events — a thread's
  `latest_event`, a `redacted_because` — each with its own `unsigned.age`.
  Stripping only the outer one reports three differences that are nothing of the
  kind. MSC4354's sticky TTL is the same shape. Drift measured here: 85 ms and
  86 ms.
- **The `prev_content` cache leak runs BOTH ways.** synapse-notes.md recorded it
  as upstream-only, which was true of everything classic sync could show —
  classic sync calls `AttachPrevContent` deliberately, so our side is never the
  surprising one there. Sliding sync does not: 128 events where the reference
  had it and we did not, and one where we had it and it did not. The note is
  corrected.

#### What is still not exercised

**The filter's event-DROPPING path.** Its membership output is verified against
Synapse event by event, so the call is exercised and its result used. But no
room on any of the three accounts actually withholds anything from these users:
the `history_visibility: invited` rooms are ones the account has been in from
the start, and the deepest such room holds 15 events, all of which the reference
returns. Discarding the filter's event list still compares clean.

That is a property of the available rooms, not evidence. What makes it
acceptable is that the dropping rules have unit tests in `internal/visibility`
and the same `eventfilter.ForClient` is on classic sync's path, which syncdiff
exercises — verified after the lift: `sync`, `incremental_sync` and
`initial_sync` all still match, with only the documented tolerated categories.
Sharing one implementation is the entire argument; a second copy is what would
make this untested.

### The rewind and newly-joined/left — done

The two corrections that make a *current* room list answer for a *past* token.
Both are no-ops on a sync whose token is current, which is almost every sync —
so a build with either removed compares clean against Synapse all day. They are
tested against the database instead, by picking a membership change that really
happened and asserting the functions notice it. Five of five mutations bite.

Three findings, all in [synapse-notes.md](synapse-notes.md):

- **A server admin can ask to see soft-failed events**, via
  `io.element.synapse.admin_client_config`, and Synapse marks each one in
  `unsigned`. Found only because a sliding sync comparison ran against the
  **server owner's** account — the test accounts are not admins, so classic sync
  had matched for weeks without it. Now implemented for both endpoints. The
  lesson is CLAUDE.md §3's, extended: an account with different **rights**
  exercises different code, not just an account with more rooms.
- **`prev_membership` is often the wrong user's.** The join→join filter reaches
  the previous membership through `event_edges`, which gives DAG parents rather
  than state predecessors: a usable membership only 51.7% of the time, and
  62.3% of those belong to somebody else. The failure is safe only by accident
  of structure, and both outcomes are now pinned by tests.
- **The snapshot token is a vector clock.** Our first version collapsed it to a
  single position, which would stop the rewind at the slowest of the six event
  persisters in use here and miss everything the others did above it. Every
  test still passed, because the tests supply single-writer tokens of their own.

#### And a defect in M10's caches, found by the same tests

`streamcache.New` returned an **armed** cache with horizon 0 and no entries — a
cache claiming a complete record of all history, which therefore answers
"unchanged" to every question. The exact false negative the package exists to
prevent, arrived at by doing nothing.

Reachable in production: a deployment with `replication.enabled: false` never
fires the connect callback that prefills and arms the caches, so its presence
gate would have answered "nothing changed" forever. Caches now start disarmed
and only `Arm` enables them.

### The endpoint — done, and it answers

Registered on both paths MSC4186 defines: the unstable one every client uses
today and `/_matrix/client/v4/sync`. Both **POST**. Not registered at all when
`sliding_sync.enabled` is false, so probing either returns `M_UNRECOGNIZED`
rather than failing on the first request.

Partial-state exclusion came with it. The question is narrower than "is this
room partial": partial state is complete for everything except REMOTE
memberships, so a request wanting none of those is served perfectly well from
it. Answering "yes" too readily hides a freshly joined room for as long as
backfill takes.

#### Verified in lockstep, not one request at a time

A sliding sync response is a delta against per-connection state, so a single
request proves almost nothing. Six rounds against `av-sync-worker-2`, on each of
the three accounts, feeding each side its own `pos` — including **widening the
window mid-sequence** and **re-sending `pos=0/<token>`**:

| Round | What it exercises | Rooms (ours / reference) |
|---|---|---|
| 1 | initial | 5 / 5 |
| 2 | quiet poll | 0 / 0 |
| 3 | window widened 0–4 → 0–7 | 3 / 3 |
| 4 | quiet poll | 0 / 0 |
| 5 | `pos=0/<token>` re-bootstrap | 8 / 8 |
| 6 | quiet poll | 0 / 0 |

Zero mismatches on all three, including the 654-room account. And the long poll:
a 25-second request returned in **4,146 ms** carrying exactly the message sent
at the four-second mark, with `num_live: 1`.

#### Two bugs the round trip found that a single request could not

**Every response re-sent every room in the window.** `_filter_relevant_rooms_to_send`
was missing, so a quiet poll returned all five rooms again — the difference
between a sliding sync and a very expensive `/sync`. The M10 events stream cache
is what answers it without a query.

**Then responses alternated between five rooms and none, for ever.** The
`record_unsent_rooms` set must be the rooms OUTSIDE every list range that have
had events — not every room this response did not describe. `PREVIOUSLY` means
"there ARE updates we withheld", so marking a room that simply had nothing new
made the next request dutifully re-send it. Both are visible only from round
three onwards.

### Not done, and none of it silently

- **The `spaces` list filter** is refused rather than ignored, matching
  Synapse's `NotImplementedError`. Silently returning unfiltered rooms would
  show a client rooms it asked not to see.


## M13 — done, all seven extensions

`to_device` (MSC3885), `e2ee` (MSC3884), `account_data` (MSC3959), `receipts`
(MSC3960), `typing` (MSC3961), `io.element.msc4308.thread_subscriptions`
(MSC4308) and `org.matrix.msc4354.sticky_events` (MSC4354) — every one Synapse
implements. `docs/decisions.md` records why MSC4262 (profiles) and MSC4360
(threads) are not among them.

Every extension is off unless the request enables it, which is what keeps the
endpoint cheap: a client that only wants a room list pays for nothing else.

Three of them track per-connection state exactly as rooms do — a room the client
has never been given gets all of its account data or receipts, one it has gets
only the difference. Sending everything every time is what makes an account-data
extension expensive on a 654-room account.

### Compared against Synapse, and it took five corrections

Both sides driven twice with the position fed back, on all three accounts.
Everything matched on the second attempt except five things, each a real
difference and none guessable from the source:

| What was wrong | What Synapse does |
|---|---|
| Room account data omitted when empty | An initial room gets an entry **always**, even `[]`; a merely-updated one is omitted |
| Initial receipts sent for the whole room | Only receipts for events **in the timeline**, plus the user's own — the spec's rule, and the reason is that a busy room's receipt list grows with its membership |
| `sticky_events` sent empty | Omitted entirely when there is nothing; Synapse's encoder tests it for truthiness |
| Sticky `next_batch` was `s<n>` | `sticky_<n>` — prefixed so it cannot be swapped with the room key or the thread-subscriptions token |
| Sticky events lost `unsigned.membership` | They bypass the visibility DECISION (MSC4354 says history visibility must not apply) but still go THROUGH the filter, because that is what computes MSC4115's membership |

That last one is the shape worth remembering: "this data skips the visibility
rules" is not the same as "this data skips the visibility code".

### Two more comparability sources

- **An account-data list has no defined order.** Twenty entries, identical as
  sets, sharing not one position: Synapse iterates a dict, we sort. Compare as
  a set.
- **The typing view is only as old as the worker.** Which rooms appear is a
  question about the typing stream, and a room whose typists were cleared
  reports `user_ids: []`. That stream is memory-only, so a freshly started
  worker has not seen clears Synapse remembers from hours ago. A room both
  sides report must match; one only the reference reports is counted.
