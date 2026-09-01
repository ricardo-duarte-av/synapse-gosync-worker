# Milestones

Ordering reflects two deliberate choices: the legacy endpoints come early as
warm-ups (they exercise auth + DB + room state end to end at a fraction of
`/sync`'s complexity), and initial sync comes before stream tokens.

Status is recorded here; what actually happened is in [log.md](log.md).

| # | Milestone | Status |
|---|---|---|
| M1 | Skeleton, auth, `/rooms/{roomId}/initialSync` | **done — 9/9 rooms at parity** |
| M2 | `/initialSync` | **done — at parity** |
| M3 | Initial `/sync` (no `since`) | next |
| M4 | Stream tokens and incremental `/sync` | token package done in M1 |
| M5 | Long-polling and Redis replication | not started |
| M6 | Filters and lazy-loading | not started |
| M7 | Ephemeral: receipts, typing, presence | not started |
| M8 | To-device and device lists | not started |
| M9 | Soak, then possibly the promotion ladder | not started |

## M1 — done

`/rooms/{roomId}/initialSync` matches Synapse on all 9 joined rooms at limits 1,
5, 20 and 100, verified by `cmd/syncdiff`. See [log.md](log.md).

Two deliberate gaps, both answered 501 rather than approximated:

- **Rooms the user has left.** `_room_initial_sync_parted` resolves the room
  state at the leave event, which needs state groups.
- **Rooms needing per-event state.** History visibility other than `shared` or
  `world_readable`, visibility that has changed over the room's life, an erased
  sender, or a retention policy.

Both are lifted by the same piece of work — a state-group resolver — which is
also what M3's `compute_state_delta` needs. It is the largest single unbuilt
thing in the project, and measuring a second account promoted it from
nice-to-have to blocking: 6 of that account's 30 rooms need it, and because
`/initialSync` is all-or-nothing, one such room makes the whole snapshot 501.

## M2 — done

`/initialSync` matches Synapse across all 9 rooms, stable over repeated runs.
The two initialSync endpoints differ upstream in four ways that are easy to miss;
see [synapse-notes.md](synapse-notes.md) before touching either.

`archived=true` returns 501: left rooms need the state at the leave event.

## M5 is not just long-polling

It is the milestone at which this worker can answer **without being told what
time it is**. Typing has no database representation at all, and two other
streams drift above their table maxima, so a correct `now_token` can only come
from the replication stream. See [tokens.md](tokens.md).

## M8 has an unresolved blocker

Synapse's `/sync` deletes the to-device messages it returns; a read-only role
cannot. See [decisions.md](decisions.md) before starting it.
