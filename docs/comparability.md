# Why `/sync` is hard to compare, and what we do about it

The reasoning behind `cmd/syncdiff` and the `testing.allow_pin_now` flag.
Read this before writing any parity test.

gopro's `proxy → shadow → canary:N → native` ladder works because
`/event?event_id=X` is a pure function of an immutable argument: ask both sides,
diff the bytes. `/sync` is not that. **Sending the same request twice — even to
the same Synapse worker — legitimately yields different answers.** A naive A/B
diff would drown in false positives and teach us nothing.

Nine independent sources of divergence:

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
   code — and since M8 **both sides delete**, so it cuts both ways.

   Deletion is bounded by the client's own `since`, which is what makes the
   ordinary comparison safe: the window a response returns is `(since, now]`,
   and only messages at or below `since` are removed. Replaying the same
   `since`, or a rewound one, changes nothing.

   It stops being safe the moment the two sides carry **different** `since`
   tokens, which is exactly the situation `-endpoint to_device` creates on
   purpose in its second step. There, whoever is asked second sees an inbox the
   first has already been through. **Synapse is always asked first.** Ask us
   first and we delete the messages Synapse is about to be asked for, and a
   worker that skipped the rest of a backlog looks identical to one that did
   not.

4. **`set_presence` mutates state** and emits `USER_SYNC` over replication.
   The comparator always sends `set_presence=offline`.

5. **Unordered collections.** `state.events`, `summary.m.heroes` and
   `device_lists.changed` have no spec-mandated order; `timeline.events` does.
   The comparator is structural, not a byte diff.

6. **Genuinely racy fields.** `unread_notifications` reads `event_push_summary`,
   which a background job rolls up asynchronously. Recorded as
   `tolerated{reason=…}` rather than counted as either a match or a mismatch —
   the same treatment gopro gives `walk_truncated`.

7. **A state key whose value Synapse picks at random.** `_calculate_state` can
   end up holding two different events for the same `(type, state_key)`: one
   from the state at the start of the timeline, one from the state at its end,
   with neither carried by the timeline itself to subtract. Synapse then builds
   `{event_id_to_state_key[e]: e for e in state_ids}` from a Python **set** of
   event IDs, so which of the two survives depends on string hash order — and
   Python randomises that per process.

   Measured, not inferred. For the `m.room.server_acl` pair this first appeared
   on, `PYTHONHASHSEED` 0, 1, 4, 6 and 7 select the newer event and 2, 3 and 5
   the older:

   ```sh
   for seed in 0 1 2 3 4 5 6 7; do PYTHONHASHSEED=$seed python3 -c '...'; done
   ```

   So Synapse's answer here changes when the worker restarts. There is no value
   to match, only a coin to call.

   → Counted by name. The bucket is deliberately narrow: **both** sides must
   carry an entry for the same `(type, state_key)` and the two event IDs must
   differ. A state key only one side emits is still a mismatch.

8. **The lazy-loaded members cache has no representation in the database.**
   Which member events a lazy-loading sync sends depends on which ones that
   *process* has already sent to that device, held in an in-memory LRU keyed by
   `(user, device)`. Two implementations cannot agree from the same inputs.

   Three things keep it bounded. An initial sync **clears** the cache before
   using it, so that case is fully deterministic. `include_redundant_members`
   skips the deduplication entirely. And the whole per-device cache expires 30
   minutes after it was *created* — not after it was last used, since Synapse
   builds it without `reset_expiry_on_get` — so both sides periodically forget
   everything and re-send.

   In practice the comparator's own traffic is what warms both caches, in the
   same order, so they agree. That is a property of the harness and not of the
   implementation: do not read a passing incremental lazy-load comparison as
   proof that two *independently driven* caches would agree. They would not,
   and nothing requires them to — a given client is served by one worker.

9. **The pin hides the to-device token wind-back.** When more than 100 to-device
   messages are waiting, `_generate_sync_entry_for_to_device` returns the first
   hundred and **replaces** the `to_device` field of its own now token with the
   position of the last one it sent, so the client's `next_batch` resumes
   mid-backlog rather than skipping the remainder.

   The pin then hands us Synapse's already-wound token. A worker that never
   winds anything back computes the same window from it, returns the same
   hundred messages, and reports the same `next_batch`: under a pin the defect
   is not merely hard to see, it is **unobservable**. Confirmed by building it
   deliberately — pinned, the comparison passes.

   → `syncdiff -endpoint to_device` is the one comparison that does not pin. It
   sends the messages itself (so the section is not compared empty), lets both
   sides find their own now token, compares the section *and* the `to_device`
   position of `next_batch`, then asks both again from their own `next_batch`
   and compares what comes back. The deliberate defect is named twice by that.

   The same shape of blindness applies wherever Synapse mutates its own
   now_token mid-response — `prev_batch` is the other case, already tolerated.
   Anything derived from a token we supplied cannot be tested by supplying it.

10. **A sliding sync list's ORDER is undefined when one range covers the whole
    list.** `SlidingSyncRoomLists._compute_interested_rooms_new_tables` carries
    an explicit shortcut:

    ```python
    # Optimization: If we are asking for the full range, we don't
    # need to sort the list.
    if (len(list_config.ranges) == 1 and ranges[0][0] == 0
        and ranges[0][1] >= len(filtered_sync_room_map) - 1):
        sorted_room_info = list(filtered_sync_room_map.values())
    ```

    So a full-range request comes back in Python dict order — matching neither
    `event_stream_ordering` nor `bump_stamp`, and not reproducible by any other
    implementation. Measured against `av-sync-worker-2` on a 9-room account,
    2026-09-03: `[[0,4]]` came back sorted and **identical to ours**, while
    `[[0,8]]` and `[[0,99]]` came back in an order that matches neither column.

    We sort unconditionally, which is a deliberate deviation: skipping the sort
    would buy us nothing (the metadata is already fetched, so it is an in-memory
    sort of a few hundred entries) and would make our own answers
    irreproducible, which costs us more than it costs a client — the client has
    every room in the list and `bump_stamp` to sort them by.

    **Consequence for the comparator: compare the room SET for a full-range
    list, and the room ORDER only for a partial one.** A partial range is the
    only case where Synapse's order is defined, and it is the case that
    validates the sort — `internal/slidingsync/parity_live_test.go` does exactly
    this split.

11. **Clock-derived fields nest.** `unsigned.age` is the obvious one, and each
    side recomputes it from its own clock. What is easy to miss is that a
    bundled aggregation contains **whole events** -- a thread's `latest_event`,
    a `redacted_because` -- each carrying its own `unsigned.age`. A comparator
    that strips only the outer one reports three differences that are nothing of
    the kind, which is exactly what the first sliding sync comparison did.

    MSC4354's `unsigned.msc4354_sticky_duration_ttl_ms` is the same shape: it is
    a *remaining* lifetime, so it too is computed per request. Measured drift
    between the two sides on this deployment: 85 ms for `age`, 86 ms for the
    sticky TTL.

    **Strip clock-derived fields recursively, not at the top level.**

**Consequence.** The proxy/shadow/canary ladder is *not* built first: it depends
on comparing two answers to the same live request, which sources 1–4 make
unsound. The replay comparator comes first; the ladder is retrofitted at M9, and
its shadow mode is then cheap because pinned comparison already exists.

---
