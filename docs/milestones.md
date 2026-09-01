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
| M5 | Long-polling and Redis replication | not started |
| M6 | Filters and lazy-loading | not started |
| M7 | Ephemeral: receipts, typing, presence | not started |
| M8 | To-device and device lists | not started |
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

`to_device` is not implemented; that is M8, and it needs the deletion decision
first.

## M5 is not just long-polling

It is the milestone at which this worker can answer **without being told what
time it is**. Typing has no database representation at all, and two other
streams drift above their table maxima, so a correct `now_token` can only come
from the replication stream. See [tokens.md](tokens.md).

## M8 has an unresolved blocker

Synapse's `/sync` deletes the to-device messages it returns; a read-only role
cannot. See [decisions.md](decisions.md) before starting it.
