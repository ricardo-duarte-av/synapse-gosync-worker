# Open decisions

Things deliberately not yet decided, recorded so they are decisions
rather than late discoveries.

### To-device deletion asymmetry (M8) — DECIDED 2026-09-02

Synapse's `/sync` **deletes** the to-device messages it has just acknowledged
(`_wait_for_sync_for_user` → `delete_messages_for_device`, bounded by
`since_token.to_device_key`). A read-only role cannot, and the two failure modes
were: serve `to_device` without deleting and a device sees the same room keys on
every sync for ever; or let both workers serve one device and the real one
deletes messages we have not yet handed over.

Three options were on the table — leave `to_device` to Synapse permanently,
grant a narrow `DELETE` on `device_inbox` alone, or call Synapse's own `/sync`
for that section. **The owner chose the narrow grant.** The other two either cap
the project at "cannot serve a client on its own", or put an HTTP round trip
back into the hot path of every sync, which is the coupling this worker exists
to remove.

What that means in practice:

- `deploy/device-inbox-role.sql` creates `gosync_inbox`, granted `SELECT,
  DELETE` on `device_inbox` and **nothing else** — no other table, no INSERT,
  no default privileges on future tables.
- `internal/deviceinbox` is a separate package with a separate pool. Everything
  else keeps the read-only role and its `SHOW default_transaction_read_only`
  check, so "every query in internal/store is a SELECT" stays literally true.
- The worker verifies the narrowness at startup and refuses to run if the role
  can delete from `events` or insert into `device_inbox`. The argument for the
  grant rests entirely on it being narrow, so the process checks rather than
  trusts.
- Serving and deleting are one setting. With `to_device.enabled: false` the
  section is omitted entirely; there is no "serve but do not delete".

The remaining sharp edge is not the grant but the device: a device syncing
against both workers is fine, because each deletes only what that client's own
`since` acknowledges, but anything that syncs a device with a token running
*ahead* of a real client will delete messages that client has not seen. Point it
at test devices.

### Sliding sync's connection state: our tables, not Synapse's — DECIDED 2026-09-03

Sliding sync (MSC4186) is not a read-only workload and cannot be made into one.
Every response records what it sent so the next can send only the difference,
and even **reading** that state writes: Synapse's
`_get_and_clear_connection_positions_txn` bumps `last_used_ts`, deletes every
other position on the connection, and rewrites lazy-member rows before it
returns anything. The `pos` token a client carries is literally a sequence value
from `sliding_sync_connection_positions` — it cannot be minted without an
`INSERT`.

Three options were on the table: write Synapse's own six
`sliding_sync_connection_*` tables; keep our own in a schema of our own; or hold
the state in memory and lose it on every restart. **The owner chose our own
tables.**

Sharing Synapse's would have been the highest-fidelity answer — a client could
move between `gosync.aguiarvieira.pt` and `aguiarvieira.pt` with its `pos`
intact — and that is exactly what makes it the wrong one: any divergence in our
bookkeeping would corrupt the connection state of real clients on the real
hostname. In-memory would have kept the read-only contract literally true, at
the cost of invalidating every connection on every restart.

What that means in practice:

- `deploy/sliding-sync-role.sql` creates `gosync_ss` and a `gosync` schema, and
  the role **owns that schema and has nothing in `public`** — it cannot read
  Synapse's tables, let alone write them. `internal/store` stays 100% `SELECT`
  and its role check keeps meaning what it means today.
- `internal/slidingstore` is a separate package with a separate pool, and
  `Open` refuses a role that can read `public.events`, is read-only, or cannot
  write its own schema. The argument for the grant rests entirely on it being
  narrow, so the process checks rather than trusts — the same shape as the
  to-device grant above.
- **Our `pos` is not interchangeable with Synapse's.** A client switching hosts
  mid-connection gets `M_UNKNOWN_POS` and re-bootstraps. That is a supported
  path rather than an error: 2,409 of 25,767 live `pos`-carrying requests
  already start from no connection state.

The sharp edge is not the grant but the bookkeeping. Every other defect this
project has found was visible in one response body; "this room was marked sent
but never was" only shows up on the request after next. That is what
`internal/slidingstore/live_test.go` exists for.

### MSC4262 and MSC4360 are out of scope — DECIDED 2026-09-03

`profiles` (MSC4262) and `threads` (MSC4360) are sliding sync extensions the
owner asked about. Neither is implemented in Synapse `v1.159.0` — grepped across
`handlers/sliding_sync/`, `types/`, `rest/client/sync.py` and
`config/experimental.py`. Parity is the contract here, and with no reference
implementation there is nothing to compare a response against, so building them
would put the first unverifiable code in the project. Revisit on a Synapse
upgrade.

The seven extensions Synapse **does** implement are in scope: `to_device`
(MSC3885), `e2ee` (MSC3884), `account_data` (MSC3959), `receipts` (MSC3960),
`typing` (MSC3961), `io.element.msc4308.thread_subscriptions` (MSC4308) and
`org.matrix.msc4354.sticky_events` (MSC4480/MSC4354). Note the wire keys: this
Synapse does **not** accept MSC4508's `org.matrix.msc4508.typing` alias.

### Others

- Whether the ladder (§3) is ever worth building, given that this worker may
  stay a research instrument.
- Whether `/events` and `/initialSync` are ever served in anger. They have zero
  traffic; they exist here as warm-up targets.

---
