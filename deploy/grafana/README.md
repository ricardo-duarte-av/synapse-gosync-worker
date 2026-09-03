# Grafana dashboard

`gosync-worker-dashboard.json` — import it, or drop it into a provisioned
dashboards directory. It has no hard-coded datasource or job name: both are
template variables populated from the data, so it works wherever it lands.

## Wiring it up

The client API is served on a **unix socket, which Prometheus cannot scrape**,
so the worker opens a second listener for metrics alone:

```yaml
metrics:
  addr: ":9201"
```

`/metrics` is unauthenticated. Keep that port on an internal network and never
route it through the reverse proxy — only `/_matrix/...` paths should be
publicly reachable. The same listener also answers `/health`, which is the
easier way to probe a worker whose real socket is a unix socket.

Prometheus needs to reach the container by name on the compose network:

```yaml
  - job_name: gosync-worker
    metrics_path: /metrics
    static_configs:
      - targets: ["av-gosync-worker-1:9201"]
```

## If every panel is empty

The data is almost certainly fine -- check Prometheus first
(`gosync_build_info` should return one series), and if it does, the problem is
the dashboard's variables, not the worker.

Pick the **Datasource** at the top of the dashboard. On import Grafana asks
which Prometheus to use, and if that is skipped every expression resolves
against nothing. The `Job` and `Endpoint` variables both default to `All` and
fall back to `.*`, so they work even before they have populated.

## Reading it

**Long polls parked** is the panel that tells you whether anything is actually
using the worker. A `/sync` client spends nearly all of its life waiting, so on
a healthy deployment this sits at roughly the number of connected clients and
returns to that level after every wakeup. Zero, with a client open, means the
client is not reaching us — check the reverse proxy before anything else.

**Duration percentiles** will look alarming and should not. A `/sync` that waits
the client's full timeout and returns nothing is a SUCCESS, and it lands at 30
seconds. The high percentiles are the normal shape of a long-polling endpoint;
what matters is the low end, where an initial sync or a sync carrying news
should be well under a second. The duration heatmap makes this clearer: expect
two clusters, one fast and one at the client's timeout, and worry about the
middle filling in.

**Replication** is the gauge to alert on. While it is 0 the worker cannot report
typing at all and its stream positions are only as fresh as the last database
seed — and `/health` will still say `ok`, because it does not check this.

**Outcomes** separates `served`, `refused` and `client_gone`. A client hanging
up mid-long-poll is ordinary — Element does it on every navigation — and is
deliberately not counted as an error. A rising `refused` is worth reading with
the auth panels: `whoami unavailable` means Synapse could not be asked, and the
worker answers 502 rather than 401 on purpose, because refusing a valid token
would log real clients out.

**To-device** is the only write this worker makes. With an encrypted client
connected and `to_device.enabled: true`, deletions should track how fast
messages arrive for that device. Zero means the section is disabled — and that
client will never receive its room keys.

**The Process row is scoped to this worker, and that took fixing.** Every
Prometheus client on the server exports `process_resident_memory_bytes` and
`process_cpu_seconds_total` -- Synapse's Python workers included -- and every
Go process exports `go_goroutines` and the heap metrics. The `$job` variable
was declared with `allValue: ".*"`, so selecting *All*, which is the default,
turned each of those panels into a sum over **all 31 exporters on the host**.
It read as 4.5GB resident and 50% of a core; the worker was actually using
23MB and 10%. Nothing else on the dashboard was affected, because every other
panel reads a `gosync_`-prefixed metric that only this worker exports -- which
is exactly what made it convincing.

Two changes, and the second is the one that matters:

- The `job` variable no longer sets `allValue`, so *All* expands to the jobs
  that actually export `gosync_build_info` rather than to `.*`.
- Every panel reading a metric this worker does not have to itself is joined
  against `gosync_build_info`:

  ```promql
  sum(process_resident_memory_bytes{job=~"$job"} and on(instance, job) gosync_build_info)
  ```

  which cannot report another service's numbers whatever the variable is set
  to. If you add a panel using any `process_*` or `go_*` metric, add the join
  too. A panel that silently sums the whole host does not look broken -- it
  looks like a memory leak.

**Cache hit rate is the next panel that will mislead you**, and in the same
shape as the Process row did. `gosync_cache_hits_total` and
`gosync_cache_misses_total` are both zero for a cache that is *disarmed*, which
is indistinguishable on a ratio panel from a cache nothing asks for. Read
`gosync_cache_armed` beside it: while that is 0 the derived caches are refusing
to answer, which happens whenever replication is down -- so it is a replication
alert wearing a cache label.

Two more things about those numbers that look like faults and are not. A cache
shows misses with no hits for a moment after every reconnect, because a derived
cache only answers once replication has caught up to the token the response
will report. And `gosync_cache_invalidation_rows_total{action="purge"}` should
sit at or near zero: that stream carries roughly a row every two seconds, almost
all of it Synapse invalidating caches this worker does not have, and an earlier
version treated all of them as destructive -- which emptied the state caches
continuously and cost 4,910 extra queries on a single initial sync. A rising
`purge` means that is back.

**The stream-cache row measures a different thing from the cache row above it,
and its hit rate is the one to ignore.** A derived cache holds an answer; a
stream cache holds only whether asking is worth it. So a "hit" there means a
question was answered from memory whether the answer was "changed" or
"unchanged" -- a 100% hit rate is compatible with every gated query still
running. **Queries the gates skipped** is the panel that says whether they are
paying: watch `PresenceSince` against the sync rate, because on a quiet server
most syncs should run neither of the two gated queries.

**Gate horizon lag is the panel that matters**, and it is the one that catches
the failure a hit-rate panel cannot see. A stream cache is a complete record of
changes above its horizon and knows nothing below it; the lag is how far back
that reaches. A cache too small for its stream has the lag shrink towards zero,
at which point every question falls below the horizon, every gate answers
"changed", and the queries quietly come back -- with no error, no wrong answer,
and no other symptom. Read it with evictions beside it.

Two series on that panel sit at zero legitimately. `account_data` and
`membership` are armed empty rather than prefilled: account_data's position
space is shared by three tables, so seeding from any one of them produces a
horizon far below what it can account for, and membership has no table to seed
from at all. Both fill from live traffic. Measured on this deployment with the
defaults: events 100,041, receipts 121,038, presence 8,903,677, to_device 566 --
that last one is shallow because Synapse scans only 1,000 device_inbox rows,
which is right for a table that is drained as clients acknowledge messages.

And `gosync_stream_cache_armed` reads 0 for a cache configured to hold nothing,
not just for a disarmed one. That is deliberate: such a cache answers "changed"
to everything, which is indistinguishable from disarmed to every caller, and
reporting it armed would draw a flat healthy line for a cache doing nothing.

**The sliding sync row starts with one panel, and it is the one that matters.**
`Responses by outcome` says whether the long poll is working. Healthy: `woken`
and `timed_out` carry the traffic, and `immediate` appears only as often as
clients start new connections. A worker answering `immediate` on nearly every
request is **not** a busy worker — it is one whose emptiness rule is wrong, so
the poll never waits and every client is answered instantly and asks again.

That is not hypothetical. On 2026-09-03 this endpoint treated any *present*
extension as news, and several are present on every response: `e2ee` carries
one-time-key counts, `to_device` carries a `next_batch`, `typing` and `receipts`
carry an empty `rooms` object, and a room entry can carry nothing but a
`bump_stamp`. SchildiChat was answered about ten times a second per connection,
and it took reading an nginx access log to notice. With this panel it is one
glance.

**`Connection-store writes` is the other one to learn.** Sliding sync is the
only endpoint here that writes, and the write is proportional to the
connection's *room count* rather than to what changed — each new position copies
the previous one's rows forward. Compare `positions minted` against
`responses`: if they track each other, the "reuse the position when nothing
changed" short-circuit has stopped working, and a 654-room connection is paying
about a thousand rows on every poll.

**`Connection store size` is a slow alarm.** These tables are small by design,
because reading a position prunes the others and the reaper removes unused
connections. A row count that climbs steadily means one of those two has
stopped: `reaped` flat at zero for days is the reaper, positions far above
connections is the prune. `store_up` at 0 means the scrape could not reach the
database, so the rest of that row is stale rather than zero.

## What this dashboard cannot tell you

Two blind spots, both known and neither instrumented yet:

- **Replication lag.** `gosync_replication_connected` says the subscription is
  alive, not that it is keeping up. A worker that is connected but minutes
  behind serves stale answers and looks perfectly healthy here.
  `gosync_replication_position` now exports where each stream has reached, which
  is half of it -- but there is nothing to compare it against, because Synapse's
  own position is not scraped here.
- **Parity.** Nothing on this dashboard says whether the answers are *right*.
  That is `cmd/syncdiff`'s job, and it currently prints to a terminal rather
  than exporting anything. Agreement with Synapse is measured by running the
  comparator, not by watching graphs.
