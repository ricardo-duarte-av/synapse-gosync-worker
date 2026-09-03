package store

import (
	"context"
	"encoding/json"
	"fmt"
)

// RoomForUser is one room the user has a membership in.
//
// Mirrors Synapse's RoomsForUser, from local_current_membership joined to the
// membership event itself.
type RoomForUser struct {
	RoomID      string
	RoomVersion string
	Membership  string
	// EventID and Sender are the membership event and who sent it. For an
	// invite, the sender IS the inviter, which is what /initialSync reports.
	EventID        string
	Sender         string
	StreamOrdering int64
	IsPublic       bool
}

// RoomsForUser lists the rooms where the user's membership is one of the given
// values, and whether each is published in the public room directory.
//
// local_current_membership rather than current_state_events: it is Synapse's
// own index for this question, and it is maintained for local users only, which
// is all /initialSync ever asks about.
func (s *Store) RoomsForUser(ctx context.Context, userID string, memberships []string) ([]RoomForUser, error) {
	const q = `
		SELECT c.room_id, r.room_version, c.membership, c.event_id, e.sender,
		       e.stream_ordering, COALESCE(r.is_public, FALSE)
		  FROM local_current_membership c
		  JOIN events e USING (room_id, event_id)
		  JOIN rooms r USING (room_id)
		 WHERE c.user_id = $1 AND c.membership = ANY($2)
		 ORDER BY c.room_id`
	rows, err := s.query(ctx, "RoomsForUser", q, userID, memberships)
	if err != nil {
		return nil, fmt.Errorf("store: rooms for user: %w", err)
	}
	defer rows.Close()

	var out []RoomForUser
	for rows.Next() {
		var r RoomForUser
		if err := rows.Scan(&r.RoomID, &r.RoomVersion, &r.Membership, &r.EventID,
			&r.Sender, &r.StreamOrdering, &r.IsPublic); err != nil {
			return nil, fmt.Errorf("store: rooms for user: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: rooms for user: %w", err)
	}
	return out, nil
}

// GlobalAccountData loads a user's account data that is not tied to a room.
//
// msc3391 treats an entry with empty content as deleted and omits it. Synapse
// compares the stored text against the literal `{}` rather than parsing it, so
// an entry stored as `{ }` survives; the comparison is reproduced exactly
// rather than "improved", because the point is to return what Synapse returns.
func (s *Store) GlobalAccountData(ctx context.Context, userID string, msc3391 bool) ([]AccountDataEntry, error) {
	q := `
		SELECT account_data_type, content
		  FROM account_data
		 WHERE user_id = $1`
	if msc3391 {
		q += ` AND content != '{}'`
	}
	q += ` ORDER BY account_data_type`
	rows, err := s.query(ctx, "GlobalAccountData", q, userID)
	if err != nil {
		return nil, fmt.Errorf("store: global account data: %w", err)
	}
	defer rows.Close()

	var out []AccountDataEntry
	for rows.Next() {
		var e AccountDataEntry
		var content string
		if err := rows.Scan(&e.Type, &content); err != nil {
			return nil, fmt.Errorf("store: global account data: %w", err)
		}
		e.Content = json.RawMessage(content)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: global account data: %w", err)
	}
	return out, nil
}

// AllRoomAccountData loads a user's per-room account data and tags for every
// room at once, keyed by room ID.
//
// One query each rather than one per room: /initialSync touches every room the
// user is in, and this account is in nine while a real one is in hundreds.
func (s *Store) AllRoomAccountData(ctx context.Context, userID string, msc3391 bool) (map[string][]AccountDataEntry, error) {
	byRoom := map[string][]AccountDataEntry{}

	// Tags first: Synapse emits the synthetic m.tag event ahead of stored room
	// account data.
	tagRows, err := s.query(ctx, "AllRoomAccountData",
		`SELECT room_id, tag, content FROM room_tags WHERE user_id = $1`, userID)
	if err != nil {
		return nil, fmt.Errorf("store: all room tags: %w", err)
	}
	tagsByRoom := map[string]map[string]json.RawMessage{}
	for tagRows.Next() {
		var roomID, tag, content string
		if err := tagRows.Scan(&roomID, &tag, &content); err != nil {
			tagRows.Close()
			return nil, fmt.Errorf("store: all room tags: %w", err)
		}
		if tagsByRoom[roomID] == nil {
			tagsByRoom[roomID] = map[string]json.RawMessage{}
		}
		tagsByRoom[roomID][tag] = json.RawMessage(content)
	}
	tagRows.Close()
	if err := tagRows.Err(); err != nil {
		return nil, fmt.Errorf("store: all room tags: %w", err)
	}
	for roomID, tags := range tagsByRoom {
		body, err := json.Marshal(map[string]any{"tags": tags})
		if err != nil {
			return nil, fmt.Errorf("store: all room tags: %w", err)
		}
		byRoom[roomID] = append(byRoom[roomID], AccountDataEntry{Type: "m.tag", Content: body})
	}

	roomQ := `
		SELECT room_id, account_data_type, content
		  FROM room_account_data
		 WHERE user_id = $1`
	if msc3391 {
		roomQ += ` AND content != '{}'`
	}
	roomQ += ` ORDER BY room_id, account_data_type`
	rows, err := s.query(ctx, "AllRoomAccountData", roomQ, userID)
	if err != nil {
		return nil, fmt.Errorf("store: all room account data: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var roomID string
		var e AccountDataEntry
		var content string
		if err := rows.Scan(&roomID, &e.Type, &content); err != nil {
			return nil, fmt.Errorf("store: all room account data: %w", err)
		}
		e.Content = json.RawMessage(content)
		byRoom[roomID] = append(byRoom[roomID], e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: all room account data: %w", err)
	}
	return byRoom, nil
}

// SharedRoomPresence loads the presence of everyone who shares a joined room
// with the user, plus the user themselves, excluding offline states.
//
// This is PresenceEventSource.get_new_events with from_key=None and
// include_offline=False, collapsed into one query. Synapse walks every joined
// room and unions their members; the SQL does the same thing as a join.
//
// Offline is filtered in SQL rather than in Go because on a server of this size
// the vast majority of co-occupants are offline, and they are exactly the rows
// there is no point transferring.
func (s *Store) SharedRoomPresence(ctx context.Context, userID string) ([]PresenceState, error) {
	const q = `
		SELECT p.user_id, p.state, p.last_active_ts, p.status_msg, p.currently_active
		  FROM presence_stream p
		 WHERE p.state IS NOT NULL AND p.state <> 'offline'
		   AND (p.user_id = $1 OR p.user_id IN (
				SELECT cse.state_key
				  FROM current_state_events cse
				 WHERE cse.type = 'm.room.member' AND cse.membership = 'join'
				   AND cse.room_id IN (
						SELECT room_id FROM local_current_membership
						 WHERE user_id = $1 AND membership = 'join')))`
	rows, err := s.query(ctx, "SharedRoomPresence", q, userID)
	if err != nil {
		return nil, fmt.Errorf("store: shared room presence: %w", err)
	}
	defer rows.Close()
	return scanPresence(rows)
}

// MultiRoomReceipts loads receipts for several rooms at once.
func (s *Store) MultiRoomReceipts(ctx context.Context, roomIDs []string, toMax int64) (map[string][]ReceiptRow, error) {
	if len(roomIDs) == 0 {
		return nil, nil
	}
	const q = `
		SELECT room_id, stream_id, COALESCE(instance_name, ''), receipt_type,
		       user_id, event_id, COALESCE(thread_id, ''), data
		  FROM receipts_linearized
		 WHERE room_id = ANY($1) AND stream_id <= $2`
	rows, err := s.query(ctx, "MultiRoomReceipts", q, roomIDs, toMax)
	if err != nil {
		return nil, fmt.Errorf("store: multi room receipts: %w", err)
	}
	defer rows.Close()

	out := map[string][]ReceiptRow{}
	for rows.Next() {
		var roomID string
		var r ReceiptRow
		var data string
		if err := rows.Scan(&roomID, &r.StreamID, &r.InstanceName, &r.ReceiptType,
			&r.UserID, &r.EventID, &r.ThreadID, &data); err != nil {
			return nil, fmt.Errorf("store: multi room receipts: %w", err)
		}
		r.Data = json.RawMessage(data)
		out[roomID] = append(out[roomID], r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: multi room receipts: %w", err)
	}
	return out, nil
}

// InviteEvent loads a single event for the `invite` field of /initialSync.
func (s *Store) InviteEvent(ctx context.Context, eventID, roomID, roomVersion string) (StateEvent, error) {
	const q = `
		SELECT e.type, COALESCE(e.state_key, ''), e.sender, ej.json, ej.internal_metadata
		  FROM events e JOIN event_json ej USING (event_id)
		 WHERE e.event_id = $1`
	var ev StateEvent
	if err := s.queryRow(ctx, "InviteEvent", q, eventID).Scan(
		&ev.Type, &ev.StateKey, &ev.Sender, &ev.JSON, &ev.InternalMetadata); err != nil {
		return StateEvent{}, fmt.Errorf("store: invite event: %w", err)
	}
	ev.EventID = eventID
	ev.RoomID = roomID
	ev.RoomVersion = roomVersion
	return ev, nil
}
