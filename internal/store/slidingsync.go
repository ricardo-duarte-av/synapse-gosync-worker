package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/clientevent"
)

func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

// Sliding sync reads three tables classic sync never touches:
// sliding_sync_membership_snapshots, sliding_sync_joined_rooms and
// sliding_sync_joined_rooms_to_recalculate. They are a materialised read model,
// maintained by Synapse's event persister inside the same transaction that
// persists the events -- so they are ordinary rows subject to normal
// replication, and this worker can read them without maintaining them.
//
// That is what makes a 654-room list computable at all. Without them, "which
// rooms is this user in, what type are they, are they encrypted, what are they
// called, and when did each last see activity" is a state resolution per room.
//
// Synapse keeps a fallback path (`_compute_interested_rooms_fallback`) for
// deployments whose background updates have not finished backfilling these.
// It is deliberately NOT ported: it is ~190 lines Synapse itself marks FIXME
// for removal, and on a server where it would be needed we should refuse to
// serve the endpoint rather than answer from tables we know are incomplete.
// See SlidingSyncTablesReady.

// SlidingRoom is one row of a user's membership snapshot.
//
// The metadata is captured AS OF the membership event for a non-joined room and
// taken from the live joined-rooms table for a joined one, which is why the two
// sources are COALESCEd. A room the user left in 2024 should be described as it
// was then, not as it is now.
type SlidingRoom struct {
	RoomID        string
	Sender        string
	Membership    string
	EventID       string
	RoomVersion   string
	EventInstance string
	EventStream   int64
	HasKnownState bool
	RoomType      *string
	IsEncrypted   bool
}

// SlidingRoomsForUser returns every room a user has any membership in, as of
// now.
//
// "As of now" is the catch, and the caller must rewind: these are CURRENT
// memberships, and a sliding sync answers as of a token that may be behind
// them. Two exclusions are Synapse's and both matter:
//
//   - forgotten rooms, which the user has asked never to see again;
//   - rooms the user left THEMSELVES (`membership = 'leave' AND user_id =
//     sender`), as opposed to being kicked or banned, which stay visible.
//
// Unknown room versions are dropped rather than served: their metadata may be
// in a broken state, and a room missing from a list is a far cheaper failure
// than a request that fails.
func (s *Store) SlidingRoomsForUser(ctx context.Context, userID string) (map[string]SlidingRoom, error) {
	const q = `
		SELECT m.room_id, m.sender, m.membership, m.membership_event_id,
		       r.room_version, m.event_instance_name, m.event_stream_ordering,
		       m.has_known_state,
		       COALESCE(j.room_type, m.room_type),
		       COALESCE(j.is_encrypted, m.is_encrypted)
		  FROM sliding_sync_membership_snapshots AS m
		  JOIN rooms AS r USING (room_id)
		  LEFT JOIN sliding_sync_joined_rooms AS j
		         ON j.room_id = m.room_id AND m.membership = 'join'
		 WHERE m.user_id = $1
		   AND m.forgotten = 0
		   AND (m.membership != 'leave' OR m.user_id != m.sender)`
	rows, err := s.query(ctx, "SlidingRoomsForUser", q, userID)
	if err != nil {
		return nil, fmt.Errorf("store: sliding rooms for user: %w", err)
	}
	defer rows.Close()
	return scanSlidingRooms(rows)
}

// SlidingSelfLeavesAfter returns rooms the user left themselves at a position
// ABOVE the sync's token.
//
// A sister query to SlidingRoomsForUser, which excludes self-leaves entirely.
// It has to: a room the user leaves tomorrow is one they were still in today,
// so a sync answering as of a token before the leave must still see it. Kept
// separate from the main query because that one is cached per user and this one
// is keyed by a token nobody asks for twice.
//
// The bound is the room key's MINIMUM stream position, so a vector-clock token
// pulls more rows than strictly needed; the caller filters the rest.
func (s *Store) SlidingSelfLeavesAfter(
	ctx context.Context, userID string, minStream int64,
) (map[string]SlidingRoom, error) {
	const q = `
		SELECT m.room_id, m.sender, m.membership, m.membership_event_id,
		       r.room_version, m.event_instance_name, m.event_stream_ordering,
		       m.has_known_state, m.room_type, m.is_encrypted
		  FROM sliding_sync_membership_snapshots AS m
		  JOIN rooms AS r USING (room_id)
		 WHERE m.user_id = $1
		   AND m.forgotten = 0
		   AND m.membership = 'leave'
		   AND m.user_id = m.sender
		   AND m.event_stream_ordering > $2`
	rows, err := s.query(ctx, "SlidingSelfLeavesAfter", q, userID, minStream)
	if err != nil {
		return nil, fmt.Errorf("store: sliding self leaves: %w", err)
	}
	defer rows.Close()
	return scanSlidingRooms(rows)
}

func scanSlidingRooms(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) (map[string]SlidingRoom, error) {
	out := map[string]SlidingRoom{}
	for rows.Next() {
		var r SlidingRoom
		if err := rows.Scan(&r.RoomID, &r.Sender, &r.Membership, &r.EventID,
			&r.RoomVersion, &r.EventInstance, &r.EventStream,
			&r.HasKnownState, &r.RoomType, &r.IsEncrypted); err != nil {
			return nil, fmt.Errorf("store: scan sliding room: %w", err)
		}
		if !clientevent.IsKnownRoomVersion(r.RoomVersion) {
			continue
		}
		out[r.RoomID] = r
	}
	return out, rows.Err()
}

// SlidingJoinedRoom is the live metadata for a room the server is in.
type SlidingJoinedRoom struct {
	RoomID string
	// EventStream is the stream ordering of the room's latest event.
	EventStream int64
	// BumpStamp is the stream ordering of the last event whose type counts as
	// activity for ordering purposes. Nullable: a room can exist with no such
	// event yet.
	BumpStamp   *int64
	RoomType    *string
	RoomName    *string
	IsEncrypted bool
	Tombstone   *string
}

// SlidingJoinedRooms returns the live metadata for rooms, in one query.
//
// This is where bump_stamp, the room name, the type and the encryption flag
// come from for the response and for list filtering -- all of it precomputed by
// the persister, none of it needing state resolution.
func (s *Store) SlidingJoinedRooms(
	ctx context.Context, roomIDs []string,
) (map[string]SlidingJoinedRoom, error) {
	if len(roomIDs) == 0 {
		return map[string]SlidingJoinedRoom{}, nil
	}
	const q = `
		SELECT room_id, event_stream_ordering, bump_stamp, room_type, room_name,
		       is_encrypted, tombstone_successor_room_id
		  FROM sliding_sync_joined_rooms
		 WHERE room_id = ANY($1)`
	rows, err := s.query(ctx, "SlidingJoinedRooms", q, roomIDs)
	if err != nil {
		return nil, fmt.Errorf("store: sliding joined rooms: %w", err)
	}
	defer rows.Close()

	out := make(map[string]SlidingJoinedRoom, len(roomIDs))
	for rows.Next() {
		var r SlidingJoinedRoom
		if err := rows.Scan(&r.RoomID, &r.EventStream, &r.BumpStamp, &r.RoomType,
			&r.RoomName, &r.IsEncrypted, &r.Tombstone); err != nil {
			return nil, fmt.Errorf("store: scan sliding joined room: %w", err)
		}
		out[r.RoomID] = r
	}
	return out, rows.Err()
}

// SlidingSyncTablesReady reports whether the materialised tables can be
// trusted, and why not if they cannot.
//
// Two ways they cannot. The background updates that backfill them may still be
// running, or a room may be queued for recomputation because stale data was
// detected -- which Synapse does after a downgrade-and-upgrade cycle. In either
// case Synapse falls back to computing room lists the slow way; we have no
// fallback, so this is checked at startup and the endpoint refused rather than
// answered from tables known to be incomplete. A room silently missing from a
// client's room list is exactly the failure nobody reports as a bug.
func (s *Store) SlidingSyncTablesReady(ctx context.Context) (bool, string, error) {
	var pending int
	var backfilling int
	const q = `
		SELECT (SELECT count(*) FROM sliding_sync_joined_rooms_to_recalculate),
		       (SELECT count(*) FROM background_updates
		         WHERE update_name LIKE 'sliding_sync%')`
	if err := s.queryRow(ctx, "SlidingSyncTablesReady", q).Scan(&pending, &backfilling); err != nil {
		return false, "", fmt.Errorf("store: sliding sync readiness: %w", err)
	}
	switch {
	case backfilling > 0:
		return false, fmt.Sprintf(
			"%d sliding sync background update(s) still running in Synapse", backfilling), nil
	case pending > 0:
		return false, fmt.Sprintf(
			"%d room(s) queued in sliding_sync_joined_rooms_to_recalculate", pending), nil
	}
	return true, "", nil
}

// DirectRooms returns the room IDs in the user's `m.direct` account data.
//
// The `is_dm` list filter is the only consumer, and the shape of `m.direct` is
// a map from user ID to a list of room IDs -- so a room is a DM if it appears
// under ANY user, not under a particular one.
func (s *Store) DirectRooms(ctx context.Context, userID string) (map[string]bool, error) {
	const q = `
		SELECT content FROM account_data
		 WHERE user_id = $1 AND account_data_type = 'm.direct'
		 ORDER BY stream_id DESC LIMIT 1`
	var content *string
	if err := s.queryRow(ctx, "DirectRooms", q, userID).Scan(&content); err != nil {
		if isNoRows(err) {
			return map[string]bool{}, nil
		}
		return nil, fmt.Errorf("store: direct rooms: %w", err)
	}
	out := map[string]bool{}
	if content == nil {
		return out, nil
	}
	// Content is user_id -> [room_id, ...]. Anything that is not a list of
	// strings is ignored rather than rejected: this is client-supplied account
	// data and a malformed entry must not fail the whole sync.
	gjson.Parse(*content).ForEach(func(_, rooms gjson.Result) bool {
		rooms.ForEach(func(_, roomID gjson.Result) bool {
			if roomID.Type == gjson.String {
				out[roomID.String()] = true
			}
			return true
		})
		return true
	})
	return out, nil
}

// RoomTagsForUser returns the tag names set on each of the user's rooms.
//
// The `tags`/`not_tags` list filters are the consumers. One query for every
// room rather than one per room: a 654-room account would otherwise pay 654
// round trips to answer a filter that usually matches nothing.
func (s *Store) RoomTagsForUser(ctx context.Context, userID string) (map[string]map[string]bool, error) {
	const q = `SELECT room_id, tag FROM room_tags WHERE user_id = $1`
	rows, err := s.query(ctx, "RoomTagsForUser", q, userID)
	if err != nil {
		return nil, fmt.Errorf("store: room tags: %w", err)
	}
	defer rows.Close()
	out := map[string]map[string]bool{}
	for rows.Next() {
		var roomID, tag string
		if err := rows.Scan(&roomID, &tag); err != nil {
			return nil, fmt.Errorf("store: room tags: %w", err)
		}
		if out[roomID] == nil {
			out[roomID] = map[string]bool{}
		}
		out[roomID][tag] = true
	}
	return out, rows.Err()
}

// LastEventPosBefore returns each room's newest non-outlier, non-rejected event
// at or below a stream position.
//
// Only the sort path uses it, and only for the rooms whose recorded latest
// event is AHEAD of the sync's token -- which is rare, because a token is
// usually current. Rejected and outlier events are excluded because a user
// cannot see them, so ordering a room by one would put it in the wrong place
// for a reason invisible to the client.
func (s *Store) LastEventPosBefore(
	ctx context.Context, roomIDs []string, maxStream int64,
) (map[string]int64, error) {
	if len(roomIDs) == 0 {
		return map[string]int64{}, nil
	}
	const q = `
		SELECT r.room_id, (
			SELECT e.stream_ordering FROM events e
			  LEFT JOIN rejections USING (event_id)
			 WHERE e.room_id = r.room_id
			   AND e.stream_ordering <= $2
			   AND NOT e.outlier
			   AND rejection_reason IS NULL
			 ORDER BY e.stream_ordering DESC LIMIT 1)
		  FROM rooms r WHERE r.room_id = ANY($1)`
	rows, err := s.query(ctx, "LastEventPosBefore", q, roomIDs, maxStream)
	if err != nil {
		return nil, fmt.Errorf("store: last event pos before: %w", err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var roomID string
		var pos *int64
		if err := rows.Scan(&roomID, &pos); err != nil {
			return nil, fmt.Errorf("store: last event pos before: %w", err)
		}
		if pos != nil {
			out[roomID] = *pos
		}
	}
	return out, rows.Err()
}

// LastBumpEventPosBefore returns the stream position of the newest event of one
// of the given types at or below a position.
//
// The fallback path for a room's bump_stamp: the precomputed
// sliding_sync_joined_rooms.bump_stamp is the answer whenever it is safely
// below the sync's token, and this is what runs when it is not. Rejected and
// outlier events are excluded, because a client cannot see them and would
// otherwise have a room sorted by an event that does not exist for it.
func (s *Store) LastBumpEventPosBefore(
	ctx context.Context, roomID string, eventTypes []string, maxStream int64,
) (int64, bool, error) {
	const q = `
		SELECT e.stream_ordering FROM events e
		  LEFT JOIN rejections USING (event_id)
		 WHERE e.room_id = $1 AND e.type = ANY($2)
		   AND e.stream_ordering <= $3
		   AND NOT e.outlier AND rejection_reason IS NULL
		 ORDER BY e.stream_ordering DESC LIMIT 1`
	var pos int64
	err := s.queryRow(ctx, "LastBumpEventPosBefore", q, roomID, eventTypes, maxStream).Scan(&pos)
	if isNoRows(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("store: last bump event: %w", err)
	}
	return pos, true, nil
}

// MemberCounts returns the number of members in each membership state.
//
// Cheaper than RoomSummary when only the counts are wanted: the summary orders
// members so that heroes are stable, and that ordering is the expensive part.
func (s *Store) MemberCounts(ctx context.Context, roomID string) (map[string]int, error) {
	const q = `
		SELECT membership, count(*) FROM current_state_events
		 WHERE room_id = $1 AND type = 'm.room.member'
		 GROUP BY membership`
	rows, err := s.query(ctx, "MemberCounts", q, roomID)
	if err != nil {
		return nil, fmt.Errorf("store: member counts: %w", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var membership *string
		var n int
		if err := rows.Scan(&membership, &n); err != nil {
			return nil, fmt.Errorf("store: member counts: %w", err)
		}
		if membership != nil {
			out[*membership] = n
		}
	}
	return out, rows.Err()
}

// InviteOrKnockStrippedState returns the stripped state a client is given for a
// room it has been invited to or knocked on, plus the membership event itself.
//
// Such a room has no timeline and no resolvable state for this user -- they are
// not in it -- so `unsigned.invite_room_state` (or `knock_room_state`) is all
// there is to identify it by. Synapse appends a stripped copy of the membership
// event to that list, which is what tells the client who invited them.
func (s *Store) InviteOrKnockStrippedState(
	ctx context.Context, eventID string,
) (stripped []byte, membershipEvent []byte, err error) {
	const q = `SELECT json FROM event_json WHERE event_id = $1`
	var raw string
	if err := s.queryRow(ctx, "InviteOrKnockStrippedState", q, eventID).Scan(&raw); err != nil {
		if isNoRows(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("store: invite state: %w", err)
	}
	membership := gjson.Get(raw, "content.membership").String()
	key := "unsigned.invite_room_state"
	if membership == "knock" {
		key = "unsigned.knock_room_state"
	}
	if v := gjson.Get(raw, key); v.Exists() && v.IsArray() {
		stripped = []byte(v.Raw)
	}
	return stripped, []byte(raw), nil
}
