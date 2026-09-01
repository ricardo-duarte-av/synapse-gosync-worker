package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/pushrules"
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
	// UnreadCount is MSC2654's count, a different number from NotifyCount: it
	// counts events marked `unread` rather than events that would notify. A
	// muted room accumulates unread events and no notifications.
	UnreadCount int
}

// UnreadNotifications reads a user's unread counts for a room's main timeline.
//
// A port of _get_unread_counts_by_receipt_txn. Three things make this more than
// a SELECT SUM:
//
//   - Counts are relative to the user's latest READ RECEIPT, not to the start
//     of the room. Without that bound every count is the room's whole history.
//     A user with no receipt at all is measured from their own membership
//     event, which Synapse assumes is a join.
//   - event_push_summary is a rollup, and a row is only usable when its
//     `last_receipt_stream_ordering` matches the receipt actually in force. A
//     newer receipt means the rollup has not caught up and the row must be
//     ignored, not added.
//   - Highlights are never summarised, so they are always counted from
//     event_push_actions directly.
//
// Only the main timeline is reported here; per-thread counts belong in
// unread_thread_notifications.
func (s *Store) UnreadNotifications(ctx context.Context, roomID, userID string) (UnreadCounts, error) {
	const q = `
		WITH receipt AS (
			-- The user's latest unthreaded read receipt, or failing that their
			-- own membership event.
			SELECT COALESCE(
				(SELECT MAX(event_stream_ordering) FROM receipts_linearized
				  WHERE user_id = $2 AND room_id = $1 AND thread_id IS NULL
				    AND receipt_type IN ('m.read', 'm.read.private')),
				(SELECT e.stream_ordering FROM local_current_membership c
				   JOIN events e ON e.event_id = c.event_id
				  WHERE c.room_id = $1 AND c.user_id = $2),
				0) AS pos
		),
		rotated AS (
			SELECT COALESCE((SELECT stream_ordering FROM event_push_summary_stream_ordering), 0) AS pos
		),
		summary AS (
			-- Usable only when the rollup reflects the receipt in force.
			SELECT COALESCE(SUM(notif_count), 0) AS notif,
			       COALESCE(SUM(COALESCE(unread_count, 0)), 0) AS unread,
			       COUNT(*) AS rows
			  FROM event_push_summary, receipt
			 WHERE room_id = $1 AND user_id = $2 AND thread_id = 'main'
			   AND ((last_receipt_stream_ordering IS NULL AND stream_ordering > receipt.pos)
			        OR last_receipt_stream_ordering = receipt.pos)
			   -- An all-zero rollup row does not count as a summary. Treating
			   -- it as one makes the thread look already-counted, and only
			   -- events after the last rotation get added -- which undercounts
			   -- by everything between the receipt and the rotation.
			   AND (notif_count != 0 OR COALESCE(unread_count, 0) != 0)
		)
		SELECT
			summary.notif
			  + COALESCE((SELECT COUNT(*) FROM event_push_actions a, receipt, rotated
			               WHERE a.room_id = $1 AND a.user_id = $2 AND a.thread_id = 'main'
			                 AND a.notif = 1 AND a.stream_ordering > receipt.pos
			                 -- A summarised range is already counted; only
			                 -- what came after the last rotation is added.
			                 AND (summary.rows = 0 OR a.stream_ordering > rotated.pos)), 0),
			COALESCE((SELECT COUNT(*) FROM event_push_actions a, receipt
			           WHERE a.room_id = $1 AND a.user_id = $2 AND a.thread_id = 'main'
			             AND a.highlight = 1 AND a.stream_ordering > receipt.pos), 0),
			summary.unread
			  + COALESCE((SELECT COUNT(*) FROM event_push_actions a, receipt, rotated
			               WHERE a.room_id = $1 AND a.user_id = $2 AND a.thread_id = 'main'
			                 AND a.unread = 1 AND a.stream_ordering > receipt.pos
			                 AND (summary.rows = 0 OR a.stream_ordering > rotated.pos)), 0)
		  FROM summary`
	var c UnreadCounts
	if err := s.pool.QueryRow(ctx, q, roomID, userID).Scan(
		&c.NotifyCount, &c.HighlightCount, &c.UnreadCount); err != nil {
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

// PaginateBackwards loads a page of a room's history, newest first, in the
// room's topological order.
//
// This is Synapse's _paginate_room_events_by_topological_ordering_txn, and an
// initial /sync uses it -- NOT the stream-ordering variant. `pagination_method`
// picks topological when there is no `since` and stream ordering only for
// updates (handlers/sync.py:852). The difference is visible in any room with
// backfilled history, where stream ordering is negative and bears no relation
// to the room's own order.
//
// Returns the page in ascending order, a token just before its earliest event,
// and whether more events were available than were returned.
func (s *Store) PaginateBackwards(ctx context.Context, roomID, roomVersion string,
	limit int, from streamtoken.RoomKey) ([]TimelineEvent, streamtoken.RoomKey, bool, error) {

	if limit <= 0 {
		return nil, from, false, nil
	}

	// Synapse fetches twice what it was asked for, because the result set is
	// filtered afterwards and would otherwise come up short.
	requested := limit * 2

	var (
		sql  string
		args []any
	)
	const cols = `e.event_id, e.type, e.sender, e.stream_ordering,
	       e.topological_ordering, COALESCE(e.instance_name, ''),
	       COALESCE(e.state_key, ''), e.state_key IS NOT NULL,
	       ej.json, ej.internal_metadata`
	if from.IsHistorical() {
		sql = `SELECT ` + cols + `
		  FROM events e JOIN event_json ej USING (event_id)
		 WHERE e.outlier = FALSE AND e.room_id = $1
		   AND (e.topological_ordering < $2
		        OR (e.topological_ordering = $2 AND e.stream_ordering <= $3))
		 ORDER BY e.topological_ordering DESC, e.stream_ordering DESC
		 LIMIT $4`
		args = []any{roomID, from.Topological, from.Stream, requested}
	} else {
		sql = `SELECT ` + cols + `
		  FROM events e JOIN event_json ej USING (event_id)
		 WHERE e.outlier = FALSE AND e.room_id = $1
		   AND e.stream_ordering <= $2
		 ORDER BY e.topological_ordering DESC, e.stream_ordering DESC
		 LIMIT $3`
		args = []any{roomID, from.MaxStreamPos(), requested}
	}

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, from, false, fmt.Errorf("store: paginate backwards: %w", err)
	}
	defer rows.Close()

	var descending []TimelineEvent
	for rows.Next() {
		var ev TimelineEvent
		if err := rows.Scan(&ev.EventID, &ev.Type, &ev.Sender, &ev.StreamOrdering,
			&ev.TopologicalOrder, &ev.InstanceName, &ev.StateKey, &ev.IsState,
			&ev.JSON, &ev.InternalMetadata); err != nil {
			return nil, from, false, fmt.Errorf("store: paginate backwards: %w", err)
		}
		ev.RoomID = roomID
		ev.RoomVersion = roomVersion
		descending = append(descending, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, from, false, fmt.Errorf("store: paginate backwards: %w", err)
	}

	limited := len(descending) >= requested
	if len(descending) > limit {
		limited = true
		descending = descending[:limit]
	}
	if len(descending) == 0 {
		return nil, from, limited, nil
	}

	// A token names a position between events, so the page starts one before
	// its oldest event. Historical form, because that is what the topological
	// walk produced.
	oldest := descending[len(descending)-1]
	next := streamtoken.Historical(oldest.TopologicalOrder, oldest.StreamOrdering-1)

	ascending := make([]TimelineEvent, len(descending))
	for i, ev := range descending {
		ascending[len(descending)-1-i] = ev
	}
	return ascending, next, limited, nil
}

// EventsInCurrentState reports which of the given events are part of the
// room's current state.
//
// A state event that is still current is shown in the timeline regardless of
// history visibility (`always_include_ids` in filter_events_for_client): a
// client that can see the room's current state gains nothing from having the
// event that established it withheld.
func (s *Store) EventsInCurrentState(ctx context.Context, eventIDs []string) (map[string]bool, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	const q = `SELECT event_id FROM current_state_events WHERE event_id = ANY($1)`
	rows, err := s.pool.Query(ctx, q, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: events in current state: %w", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: events in current state: %w", err)
		}
		out[id] = true
	}
	return out, rows.Err()
}

// PushRules loads a user's stored push rules and their enabled flags.
//
// Synapse stores only a user's *deviations* from the built-in ruleset, so this
// is usually empty and the reported ruleset is entirely the base one.
//
// Sorted by (priority_class DESC, priority DESC), which is the order
// get_push_rules_for_user applies before handing the rows to the Rust. Rule
// order decides which notification a user gets, so this is not cosmetic.
func (s *Store) PushRules(ctx context.Context, userID string) ([]pushrules.UserRule, map[string]bool, error) {
	const q = `
		SELECT rule_id, priority_class, priority, conditions, actions
		  FROM push_rules WHERE user_name = $1
		 ORDER BY priority_class DESC, priority DESC`
	rows, err := s.pool.Query(ctx, q, userID)
	if err != nil {
		return nil, nil, fmt.Errorf("store: push rules: %w", err)
	}
	var rules []pushrules.UserRule
	for rows.Next() {
		var r pushrules.UserRule
		if err := rows.Scan(&r.RuleID, &r.PriorityClass, &r.Priority, &r.Conditions, &r.Actions); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("store: push rules: %w", err)
		}
		rules = append(rules, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("store: push rules: %w", err)
	}

	erows, err := s.pool.Query(ctx,
		`SELECT rule_id, enabled FROM push_rules_enable WHERE user_name = $1`, userID)
	if err != nil {
		return nil, nil, fmt.Errorf("store: push rules enabled: %w", err)
	}
	defer erows.Close()
	enabled := map[string]bool{}
	for erows.Next() {
		var ruleID string
		var on *int
		if err := erows.Scan(&ruleID, &on); err != nil {
			return nil, nil, fmt.Errorf("store: push rules enabled: %w", err)
		}
		enabled[ruleID] = on != nil && *on != 0
	}
	if err := erows.Err(); err != nil {
		return nil, nil, fmt.Errorf("store: push rules enabled: %w", err)
	}
	return rules, enabled, nil
}

// StateIDsAt resolves the room state at a stream position.
//
// This is NOT the same as the room's current state, and using current state in
// its place is wrong in exactly the case that matters: a busy room, where state
// changes between the token being minted and the query running. The sync
// response would then describe state the client's token does not cover.
//
// Mirrors get_state_ids_at: find the last event at or before the position, then
// take the state after it. Synapse notes this returns an arbitrary one of
// several forward extremities when the room had a fork at that moment; we
// inherit that, because we resolve the same event it does.
func (s *Store) StateIDsAt(ctx context.Context, roomID string, key streamtoken.RoomKey) (map[StateKey]string, error) {
	const lastQ = `
		SELECT event_id FROM events
		 WHERE room_id = $1 AND stream_ordering <= $2 AND outlier = FALSE
		 ORDER BY stream_ordering DESC LIMIT 1`
	var eventID string
	err := s.pool.QueryRow(ctx, lastQ, roomID, key.MaxStreamPos()).Scan(&eventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return map[StateKey]string{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: last event before position: %w", err)
	}

	groups, err := s.StateGroupsForEvents(ctx, []string{eventID})
	if err != nil {
		return nil, err
	}
	group, ok := groups[eventID]
	if !ok {
		return map[StateKey]string{}, nil
	}
	return s.FullStateForGroup(ctx, group)
}
