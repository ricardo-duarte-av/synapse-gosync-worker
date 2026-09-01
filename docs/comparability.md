# Why `/sync` is hard to compare, and what we do about it

The reasoning behind `cmd/syncdiff` and the `testing.allow_pin_now` flag.
Read this before writing any parity test.

gopro's `proxy → shadow → canary:N → native` ladder works because
`/event?event_id=X` is a pure function of an immutable argument: ask both sides,
diff the bytes. `/sync` is not that. **Sending the same request twice — even to
the same Synapse worker — legitimately yields different answers.** A naive A/B
diff would drown in false positives and teach us nothing.

Six independent sources of divergence:

1. **`now_token` differs.** Each side snapshots
   `event_sources.get_current_token()` at its own start
   (`get_sync_result_builder`, `handlers/sync.py:1969`). Anything persisted
   between the two calls appears in one answer and not the other. No amount of
   care in the comparator fixes this from the outside.

   → **Two-phase pinned replay.** Ask Synapse first with `timeout=0`; take its
   `next_batch` as `T_now`; ask ours for the same `since` with
   `?_gosync_now=T_now`, which forces the window. This is the hinge the entire
   verification story turns on, which is why it is built in M1 and not later.
   It accepts a window that has not happened yet, so it is gated behind
   `testing.allow_pin_now` and must be false in production.

2. **Synapse's `ResponseCache`**, keyed by `(user, timeout, since, filter_id,
   full_state, device_id, ignored-users-streampos, use_state_after)`
   (`rest/client/sync.py:201`), TTL `sync_response_cache_duration` — **2m by
   default**, 5m on `av-sync-worker-1`. A replayed request gets a frozen earlier
   answer and is reported as a *match*, which is the most dangerous possible
   failure mode: it succeeds.

   → The reference worker runs with `sync_response_cache_duration: 0`. Verify
   this is still true before trusting a comparison run.

3. **`/sync` is destructive.** `_wait_for_sync_for_user` calls
   `store.delete_messages_for_device` up to `since_token.to_device_key`.
   Querying Synapse first *deletes the to-device messages our replay would then
   need to see.* This is the trap most likely to be mistaken for a bug in our
   code.

4. **`set_presence` mutates state** and emits `USER_SYNC` over replication.
   The comparator always sends `set_presence=offline`.

5. **Unordered collections.** `state.events`, `summary.m.heroes` and
   `device_lists.changed` have no spec-mandated order; `timeline.events` does.
   The comparator is structural, not a byte diff.

6. **Genuinely racy fields.** `unread_notifications` reads `event_push_summary`,
   which a background job rolls up asynchronously. Recorded as
   `tolerated{reason=…}` rather than counted as either a match or a mismatch —
   the same treatment gopro gives `walk_truncated`.

**Consequence.** The proxy/shadow/canary ladder is *not* built first: it depends
on comparing two answers to the same live request, which sources 1–4 make
unsound. The replay comparator comes first; the ladder is retrofitted at M9, and
its shadow mode is then cheap because pinned comparison already exists.

---
