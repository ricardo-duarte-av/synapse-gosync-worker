package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/streamtoken"
)

// fullStateGroupQuery resolves the entire state map at a group.
//
// Same recursive walk as the filtered version, without the type restriction.
// Only used where the whole map is genuinely needed -- the `state` block of an
// initial /sync -- because a large room's state runs to six figures.
const fullStateGroupQuery = `
	WITH RECURSIVE sgs(state_group) AS (
		VALUES($1::bigint)
	  UNION ALL
		SELECT prev_state_group FROM state_group_edges e, sgs s
		WHERE s.state_group = e.state_group
	)
	SELECT DISTINCT ON (type, state_key) type, state_key, event_id
	  FROM state_groups_state
	  INNER JOIN sgs USING (state_group)
	 ORDER BY type, state_key, state_group DESC`

// FullStateForGroup resolves the complete state map at a state group.
func (s *Store) FullStateForGroup(ctx context.Context, group int64) (map[StateKey]string, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		return nil, fmt.Errorf("store: disable seqscan: %w", err)
	}
	rows, err := tx.Query(ctx, fullStateGroupQuery, group)
	if err != nil {
		return nil, fmt.Errorf("store: full state for group: %w", err)
	}
	defer rows.Close()

	state := make(map[StateKey]string)
	for rows.Next() {
		var k StateKey
		var eventID string
		if err := rows.Scan(&k.Type, &k.StateKey, &eventID); err != nil {
			return nil, fmt.Errorf("store: full state for group: %w", err)
		}
		state[k] = eventID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: full state for group: %w", err)
	}
	return state, nil
}

// CurrentStateIDs is the room's current state as a map, for state calculations
// that work in event IDs rather than whole events.
func (s *Store) CurrentStateIDs(ctx context.Context, roomID string) (map[StateKey]string, error) {
	const q = `
		SELECT type, state_key, event_id FROM current_state_events WHERE room_id = $1`
	rows, err := s.pool.Query(ctx, q, roomID)
	if err != nil {
		return nil, fmt.Errorf("store: current state ids: %w", err)
	}
	defer rows.Close()

	state := make(map[StateKey]string)
	for rows.Next() {
		var k StateKey
		var eventID string
		if err := rows.Scan(&k.Type, &k.StateKey, &eventID); err != nil {
			return nil, fmt.Errorf("store: current state ids: %w", err)
		}
		state[k] = eventID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: current state ids: %w", err)
	}
	return state, nil
}

// MemberSummary is the membership breakdown a room summary is built from.
type MemberSummary struct {
	// Counts by membership.
	Counts map[string]int
	// Members lists up to six members, ordered join, invite, leave, then
	// anything else, and by stream ordering within each -- so the earliest
	// members come first and the heroes are stable between requests.
	Members []SummaryMember
}

// SummaryMember is one member in a room summary.
type SummaryMember struct {
	UserID     string
	Membership string
}

// RoomSummary loads the member counts and the first few members of a room.
//
// Mirrors get_room_summary. The ordering is load-bearing: heroes are taken from
// this list in order, so an unstable sort would rename a room between two
// otherwise identical syncs.
func (s *Store) RoomSummary(ctx context.Context, roomID string) (MemberSummary, error) {
	out := MemberSummary{Counts: map[string]int{}}

	const countQ = `
		SELECT membership, COUNT(*) FROM current_state_events
		 WHERE type = 'm.room.member' AND room_id = $1 AND membership IS NOT NULL
		 GROUP BY membership`
	rows, err := s.pool.Query(ctx, countQ, roomID)
	if err != nil {
		return out, fmt.Errorf("store: room summary counts: %w", err)
	}
	for rows.Next() {
		var membership string
		var count int
		if err := rows.Scan(&membership, &count); err != nil {
			rows.Close()
			return out, fmt.Errorf("store: room summary counts: %w", err)
		}
		out.Counts[membership] = count
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("store: room summary counts: %w", err)
	}

	// Six rather than five: one of them may be the calling user, who is never
	// their own hero.
	const memberQ = `
		SELECT state_key, membership FROM current_state_events
		 WHERE type = 'm.room.member' AND room_id = $1 AND membership IS NOT NULL
		 ORDER BY CASE membership
		            WHEN 'join' THEN 1 WHEN 'invite' THEN 2 WHEN 'leave' THEN 3
		            ELSE 4 END ASC,
		          event_stream_ordering ASC
		 LIMIT 6`
	mrows, err := s.pool.Query(ctx, memberQ, roomID)
	if err != nil {
		return out, fmt.Errorf("store: room summary members: %w", err)
	}
	defer mrows.Close()
	for mrows.Next() {
		var m SummaryMember
		if err := mrows.Scan(&m.UserID, &m.Membership); err != nil {
			return out, fmt.Errorf("store: room summary members: %w", err)
		}
		out.Members = append(out.Members, m)
	}
	if err := mrows.Err(); err != nil {
		return out, fmt.Errorf("store: room summary members: %w", err)
	}
	return out, nil
}

// Heroes picks the users that represent a room without a name, from the
// perspective of `me`.
//
// Joined and invited members first, in stream order; only if there are none
// does it fall back to those who have left or been banned. Spec: "Room
// Summary" in the client-server API.
func (ms MemberSummary) Heroes(me string) []string {
	var joined, invited, gone []string
	for _, m := range ms.Members {
		if m.UserID == me {
			continue
		}
		switch m.Membership {
		case "join":
			joined = append(joined, m.UserID)
		case "invite":
			invited = append(invited, m.UserID)
		case "leave", "ban":
			gone = append(gone, m.UserID)
		}
	}
	if len(joined) > 0 || len(invited) > 0 {
		return firstN(append(joined, invited...), 5)
	}
	return firstN(gone, 5)
}

func firstN(xs []string, n int) []string {
	if len(xs) > n {
		return xs[:n]
	}
	return xs
}

// UnreadCounts is a room's notification counts for one user.
type UnreadCounts struct {
	NotifyCount    int
	HighlightCount int
}

// UnreadNotifications reads a user's unread counts for a room.
//
// event_push_summary is a rollup maintained by a background job, covering
// everything up to its stream_ordering; anything newer is still in
// event_push_actions and has to be added on. Reading only the summary
// undercounts by however far behind the rollup is.
func (s *Store) UnreadNotifications(ctx context.Context, roomID, userID string) (UnreadCounts, error) {
	const q = `
		WITH summary AS (
			SELECT COALESCE(SUM(notif_count), 0) AS notif,
			       COALESCE(MAX(stream_ordering), 0) AS pos
			  FROM event_push_summary
			 WHERE room_id = $1 AND user_id = $2 AND thread_id = 'main'
		)
		SELECT summary.notif
		     + COALESCE((SELECT COUNT(*) FROM event_push_actions a
		                  WHERE a.room_id = $1 AND a.user_id = $2
		                    AND a.notif = 1 AND a.thread_id = 'main'
		                    AND a.stream_ordering > summary.pos), 0),
		       COALESCE((SELECT COUNT(*) FROM event_push_actions a
		                  WHERE a.room_id = $1 AND a.user_id = $2
		                    AND a.highlight = 1 AND a.thread_id = 'main'), 0)
		  FROM summary`
	var c UnreadCounts
	if err := s.pool.QueryRow(ctx, q, roomID, userID).Scan(&c.NotifyCount, &c.HighlightCount); err != nil {
		return UnreadCounts{}, fmt.Errorf("store: unread notifications: %w", err)
	}
	return c, nil
}

// DeviceKeyCounts is the end-to-end key inventory a sync reports.
type DeviceKeyCounts struct {
	// OneTimeKeys counts unclaimed one-time keys by algorithm.
	OneTimeKeys map[string]int
	// UnusedFallbackKeyTypes lists algorithms with an unused fallback key.
	UnusedFallbackKeyTypes []string
}

// DeviceKeyCounts loads the one-time and fallback key inventory for a device.
//
// Synapse always emits both fields, even when empty: a client cannot otherwise
// tell "no keys" from "nothing changed".
func (s *Store) DeviceKeyCounts(ctx context.Context, userID, deviceID string) (DeviceKeyCounts, error) {
	out := DeviceKeyCounts{
		// signed_curve25519 is always reported, even at zero. Synapse does the
		// same: a client cannot otherwise tell "no keys left" from "this
		// server does not report key counts", and Element Android got that
		// wrong (element-hq/element-android#3725).
		OneTimeKeys:            map[string]int{"signed_curve25519": 0},
		UnusedFallbackKeyTypes: []string{},
	}
	if deviceID == "" {
		return out, nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT algorithm, COUNT(*) FROM e2e_one_time_keys_json
		 WHERE user_id = $1 AND device_id = $2 GROUP BY algorithm`, userID, deviceID)
	if err != nil {
		return out, fmt.Errorf("store: one time keys: %w", err)
	}
	for rows.Next() {
		var alg string
		var n int
		if err := rows.Scan(&alg, &n); err != nil {
			rows.Close()
			return out, fmt.Errorf("store: one time keys: %w", err)
		}
		out.OneTimeKeys[alg] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("store: one time keys: %w", err)
	}

	frows, err := s.pool.Query(ctx, `
		SELECT algorithm FROM e2e_fallback_keys_json
		 WHERE user_id = $1 AND device_id = $2 AND used IS FALSE`, userID, deviceID)
	if err != nil {
		return out, fmt.Errorf("store: fallback keys: %w", err)
	}
	defer frows.Close()
	for frows.Next() {
		var alg string
		if err := frows.Scan(&alg); err != nil {
			return out, fmt.Errorf("store: fallback keys: %w", err)
		}
		out.UnusedFallbackKeyTypes = append(out.UnusedFallbackKeyTypes, alg)
	}
	if err := frows.Err(); err != nil {
		return out, fmt.Errorf("store: fallback keys: %w", err)
	}
	return out, nil
}

// EventsByID loads stored events for serialisation, keyed by event id.
func (s *Store) EventsByID(ctx context.Context, eventIDs []string, roomVersion string) (map[string]StateEvent, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	const q = `
		SELECT e.event_id, e.room_id, e.type, COALESCE(e.state_key, ''), e.sender,
		       ej.json, ej.internal_metadata
		  FROM events e JOIN event_json ej USING (event_id)
		 WHERE e.event_id = ANY($1)`
	rows, err := s.pool.Query(ctx, q, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: events by id: %w", err)
	}
	defer rows.Close()

	out := make(map[string]StateEvent, len(eventIDs))
	for rows.Next() {
		var ev StateEvent
		if err := rows.Scan(&ev.EventID, &ev.RoomID, &ev.Type, &ev.StateKey, &ev.Sender,
			&ev.JSON, &ev.InternalMetadata); err != nil {
			return nil, fmt.Errorf("store: events by id: %w", err)
		}
		ev.RoomVersion = roomVersion
		out[ev.EventID] = ev
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: events by id: %w", err)
	}
	return out, nil
}

// RecentEventsByStream returns a room's most recent events ordered by stream
// ordering, with a token pointing just before the earliest returned.
//
// /sync paginates by STREAM ordering; the legacy initialSync endpoints
// paginate topologically. The difference is visible in the token they hand
// back -- a live `s...` here, a historical `t...-...` there -- and in what they
// return: backfilled events carry negative stream orderings, so a room's
// imported history is ordered quite differently by the two.
func (s *Store) RecentEventsByStream(ctx context.Context, roomID, roomVersion string,
	limit int, end streamtoken.RoomKey) ([]TimelineEvent, streamtoken.RoomKey, error) {

	if limit <= 0 {
		return nil, end, nil
	}
	const q = `
		SELECT e.event_id, e.type, e.sender, e.stream_ordering,
		       e.topological_ordering, COALESCE(e.instance_name, ''),
		       COALESCE(e.state_key, ''), e.state_key IS NOT NULL,
		       ej.json, ej.internal_metadata
		  FROM events e JOIN event_json ej USING (event_id)
		 WHERE e.outlier = FALSE AND e.room_id = $1 AND e.stream_ordering <= $2
		 ORDER BY e.stream_ordering DESC
		 LIMIT $3`
	// Twice the limit, because the result set is filtered for visibility
	// afterwards and would otherwise come up short.
	rows, err := s.pool.Query(ctx, q, roomID, end.MaxStreamPos(), limit*2)
	if err != nil {
		return nil, end, fmt.Errorf("store: recent events by stream: %w", err)
	}
	defer rows.Close()

	var descending []TimelineEvent
	for rows.Next() {
		var ev TimelineEvent
		if err := rows.Scan(&ev.EventID, &ev.Type, &ev.Sender, &ev.StreamOrdering,
			&ev.TopologicalOrder, &ev.InstanceName, &ev.StateKey, &ev.IsState,
			&ev.JSON, &ev.InternalMetadata); err != nil {
			return nil, end, fmt.Errorf("store: recent events by stream: %w", err)
		}
		ev.RoomID = roomID
		ev.RoomVersion = roomVersion
		descending = append(descending, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, end, fmt.Errorf("store: recent events by stream: %w", err)
	}

	if len(descending) > limit {
		descending = descending[:limit]
	}
	if len(descending) == 0 {
		return nil, end, nil
	}

	// A token names a position between events, so the batch starts one before
	// its oldest event.
	oldest := descending[len(descending)-1]
	start := streamtoken.Live(oldest.StreamOrdering - 1)

	ascending := make([]TimelineEvent, len(descending))
	for i, ev := range descending {
		ascending[len(descending)-1-i] = ev
	}
	return ascending, start, nil
}
