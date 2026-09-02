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

### Others

- Whether the ladder (§3) is ever worth building, given that this worker may
  stay a research instrument.
- Whether `/events` and `/initialSync` are ever served in anger. They have zero
  traffic; they exist here as warm-up targets.

---
