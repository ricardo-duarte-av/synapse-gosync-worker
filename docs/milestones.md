# Milestones

Ordering reflects two deliberate choices: the legacy endpoints come early as
warm-ups (they exercise auth + DB + room state end to end at a fraction of
`/sync`'s complexity), and initial sync comes before stream tokens.

Status is recorded here; what actually happened is in [log.md](log.md).

| # | Milestone | Status |
|---|---|---|
| M1 | Skeleton, auth, `/rooms/{roomId}/initialSync` | **done — 9/9 rooms at parity** |
| M2 | `/initialSync` | **done — at parity** |
| M3 | Initial `/sync` (no `since`) | **in progress** — serving, two known gaps |
| M4 | Stream tokens and incremental `/sync` | token package done in M1 |
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

## M3 — in progress

An initial `/sync` (no `since`) is served and most of it matches. Two gaps
remain:

- **`m.push_rules` is not injected into `account_data`.** Synapse synthesises
  it from the `push_rules` table layered over its built-in base rules. This is
  the larger of the two: the base ruleset is a long hardcoded list that has to
  be reproduced exactly.
- **One state-block divergence**, on a key whose state event changed *outside*
  the timeline. `_calculate_state` unions the state at both ends of the timeline
  into a set of event IDs and then rebuilds a map keyed by state key, so when
  both ends carry a different event for the same key, which one survives depends
  on Python set iteration order. Needs pinning down before it can be called
  either a bug of ours or nondeterminism of Synapse's.

An incremental `/sync` (`since` present) returns 501 rather than being answered
as though it were an initial sync, which would resend the client's whole
history. That is M4.

## M5 is not just long-polling

It is the milestone at which this worker can answer **without being told what
time it is**. Typing has no database representation at all, and two other
streams drift above their table maxima, so a correct `now_token` can only come
from the replication stream. See [tokens.md](tokens.md).

## M8 has an unresolved blocker

Synapse's `/sync` deletes the to-device messages it returns; a read-only role
cannot. See [decisions.md](decisions.md) before starting it.
