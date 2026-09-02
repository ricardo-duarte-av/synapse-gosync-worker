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
| M7 | Ephemeral: receipts, typing, presence | not started |
| M8 | To-device and device lists | **done** |
| M9 | Soak, then possibly the promotion ladder | not started |

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
