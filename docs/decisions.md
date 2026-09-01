# Open decisions

Things deliberately not yet decided, recorded so they are decisions
rather than late discoveries.

### To-device deletion asymmetry (M8) — deferred, deliberately

Synapse's `/sync` **deletes** the to-device messages it has just acknowledged
(`_wait_for_sync_for_user` → `delete_messages_for_device`, bounded by
`since_token.to_device_key`). This worker runs as a read-only role and
**cannot**. So:

- if we serve `to_device` and Synapse does not, messages are never deleted and
  the device sees them again on every sync;
- if both serve the same device, the real worker deletes messages we have not
  yet handed over.

Options, none chosen yet: leave `to_device` to Synapse permanently; grant a
narrow `DELETE` on `device_inbox` alone; or have the worker call Synapse's own
`/sync` for that section. **We will cross that bridge at M8** — recorded here so
it is a decision rather than a late discovery.

### Others

- Whether the ladder (§3) is ever worth building, given that this worker may
  stay a research instrument.
- Whether `/events` and `/initialSync` are ever served in anger. They have zero
  traffic; they exist here as warm-up targets.

---
