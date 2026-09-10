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

## Three positions may go DOWN, and have to be allowed to

Every other stream here is backed by a table, and its position may only ever
move forwards: dragging one backwards would ask a client to replay what it
already has. `advance` clamps to a maximum for exactly that reason.

Typing is the exception, because its serial is not a sequence but a counter in
the typing writer's memory (`TypingHandler._latest_room_serial`). When that
worker restarts, the counter restarts at zero and the serials it issues are
suddenly far below the ones it issued yesterday.

Clamping then breaks typing completely, and quietly. The worker keeps the
pre-restart maximum, every `next_batch` carries it as the typing key, and
`TypingChangedSince(since.Typing)` compares live serials against a number none
of them will reach for days — so no room is ever reported as having changed.
Both `/sync` and the sliding-sync `typing` extension go silent, no error is
logged, and nothing recovers until this process is restarted.

Synapse takes the token as given and throws away what it knows, because serials
from before the restart no longer mean anything:

```python
if self._latest_room_serial > token:
    # The typing worker has gone backwards (e.g. it may have restarted).
    # To prevent inconsistent data, just clear everything.
    self._reset()
self._latest_room_serial = token
```

We do the same, and by Synapse's own classification rather than by naming
typing: `resettableStreams` is the set of streams whose class does not override
`can_discard_position`, and for those `advance` takes the position as given and
drops whatever the old serials described. The typing branch of `handleRDATA`
advances the position BEFORE applying the rows, so the reset does not discard
the rows that arrived with the lower position.

Two details that are easy to get wrong:

- **Backwards is judged per WRITER, not per stream.** `positions` is one number
  per stream because that is what a token wants, but several streams here have
  more than one writer, and the one that is behind reports a position below the
  maximum as a matter of course. Reading that as a reset would throw typing
  away on ordinary traffic, so the judgement uses `lastByInstance`, which is
  per (stream, writer).
- **A lower position on a database-backed stream is clamped and ignored, in
  silence.** It is not an anomaly: a writer of a multi-writer stream announces
  `max(its own position, the position everything is persisted up to)`, which
  can fall -- `av-inbound-federation-worker-1` announced `caches` 85749540 and
  then 85749539 within fifteen minutes here. Synapse discards those without
  comment (`can_discard_position`), and an earlier version of this change
  logged and counted them, which produced its first false alarm inside a
  quarter of an hour.

Found in production on 2026-09-10: the EDU worker had restarted under a running
sync worker, which held 59,314 while the writer had climbed back to only
29,500. No typing indicator had rendered in any client since.

## POSITION carries what we were supposed to have seen

`POSITION <stream> <instance> <prev> <new>` names two positions, and the first
one is the interesting one: it is where the writer believes we were. Synapse
tests both at once --

```python
missing_updates = not (cmd.prev_token <= current_token <= cmd.new_token)
```

-- and on a gap fetches the missing rows over HTTP. We have no such path and
need none: every response here is read from the database at request time, and
the one thing that claims to know what changed without asking is a
stream-change cache, which already gives up its horizon when a notification
names no room and no user (`cmd/gosync-worker/streamfeed.go`).

So `prev` is recorded rather than acted on:
`gosync_replication_stream_discontinuities_total{reason="gap"}`. It is the
difference between a worker that is BEHIND and one that has quietly lost rows,
and no other metric here separates them. `reason="reset"` on the same counter
is the case above.
