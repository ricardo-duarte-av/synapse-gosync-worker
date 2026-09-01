# Stream tokens

The 14-field sync token, and why the database cannot mint one.

### The database cannot mint a `now_token` (measured 2026-08-31)

Every sync response is bounded by a `now_token` — Synapse snapshots
`event_sources.get_current_token()` once per request. The obvious plan is to
rebuild those 14 positions with `SELECT MAX(...)` per stream. **It does not
work**, and it is worth knowing exactly how far it gets before relying on it.

Measured against a token minted seconds earlier by `av-sync-worker-2`
(`s13908451_286696197_100002_25817840_1528577_1592_289008_40735216_0_1710_2_3587_5_69`):

| # | Field | Source that reproduces it | Exact? |
|---|---|---|---|
| 1 | room | `MAX(stream_ordering)` from `events` | yes |
| 2 | presence | `MAX(stream_id)` from `presence_stream` | yes |
| 3 | **typing** | — | **no: never in the database** |
| 4 | receipt | `MAX(stream_id)` from `receipts_linearized` | yes |
| 5 | account_data | `GREATEST` over `account_data`, `room_account_data`, `room_tags_revisions` | yes |
| 6 | **push_rules** | `push_rules_stream` gives 1587, token says 1592 | **no: drifts** |
| 7 | to_device | `MAX(stream_id)` from `device_inbox` | yes |
| 8 | device_list | `MAX(stream_id)` from `device_lists_stream` | yes |
| 9 | groups | constant 0 | yes (dead field) |
| 10 | un_partial_stated_rooms | `un_partial_stated_room_stream` | yes |
| 11 | **thread_subscriptions** | table is empty, token says 2 | **no: drifts** |
| 12 | sticky_events | `MAX(stream_id)` from `sticky_events` | yes |
| 13 | quarantined_media | `MAX(stream_id)` from `quarantined_media_changes` | yes |
| 14 | profile_updates | `MAX(stream_id)` from `profile_updates` | yes |

Twelve of fourteen are exact. The three that are not are not a matter of
finding a better query:

- **Typing is never persisted at all.** It lives in an in-memory counter on the
  typing worker and reaches other workers only over replication. No `SELECT`
  can ever produce it.
- **`push_rules` and `thread_subscriptions` drift upward** because their id
  generators allocate ids that no surviving row records. The table maximum is a
  lower bound, not the position.

`stream_positions` is not the answer either: it is a periodic checkpoint the id
generators write for restart recovery, so it lags — it showed `events` at
13908434 while the live token said 13908451.

**The conclusion is architectural, not a workaround.** A real Synapse worker
does not query the database for its current token either; it tracks positions
from the replication stream. So does this one, from M5 onward. Until then the
`end` token is derived from the table maxima and is *approximate*, which is
exactly why the pinned replay of §3 exists and why every parity run uses it.

M5 is therefore not merely "add long-polling". It is **the milestone at which
this worker can answer without being told what time it is.**

## What replication fixed (2026-09-01)

With the worker following the replication stream, **11 of the 14 token fields
now match Synapse exactly**, including `typing`, which no query could ever have
produced. Measured against a token minted seconds apart:

| Field | Ours | Synapse | |
|---|---|---|---|
| room, receipt, account_data, to_device, device_list, un_partial, sticky, quarantined, profile, groups | — | — | exact |
| typing | 129934 | 129934 | **exact, and impossible before** |
| presence | 287022166 | 287022168 | moves ~1/s; the two requests were milliseconds apart |
| push_rules | 1593 | 1594 | seeded from the table max; see below |
| thread_subscriptions | 0 | 2 | seeded from the table max; see below |

`push_rules` and `thread_subscriptions` are the two streams whose id generators
allocate ids that no surviving row records, so the table maximum is a **lower
bound**, and both streams are nearly silent — so the seed can stay behind for a
long time. That is safe rather than merely tolerable: a position that is too low
asks a client to re-receive something it already has, never to skip something it
has not. A position that was too high would lose data.

Closing the gap entirely would mean PUBLISHing a `REPLICATE` command to ask the
other workers for their positions, as a real Synapse worker does on connect.
This worker deliberately does not: it would make every other worker broadcast
POSITION rows on our account, and the project's standing rule is not to perturb
the deployment it measures.

## Typing is only as complete as what we have seen

Typing state lives nowhere but memory, so the worker knows about a typist only
if it was connected when they started. A subscriber that has just connected has
an empty view, and fills in as people type. Synapse's typing notifications
expire after about half a minute, so the view converges quickly — measured as
one missing typist on the first comparison after startup, and none on the two
that followed.

Losing the connection empties the view rather than keeping it: a stale list
would leave a room showing somebody typing forever, which is worse than showing
nobody.
