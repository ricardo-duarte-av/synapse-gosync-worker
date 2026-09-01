package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tidwall/sjson"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/clientevent"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/streamtoken"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/visibility"
)

// errNeedsState is returned when a room cannot be rendered without per-event
// state resolution. Callers turn it into a 501 rather than guessing.
var errNeedsState = fmt.Errorf("room needs per-event state resolution")

// roomSnapshot is the messages/state pair both initialSync endpoints build.
type roomSnapshot struct {
	Chunk []json.RawMessage
	Start streamtoken.RoomKey
	State []json.RawMessage
}

// buildRoomSnapshot renders a joined room's recent timeline and current state.
//
// Shared by /rooms/{roomId}/initialSync and /initialSync, which differ in what
// they wrap around this but agree exactly on what goes inside it.
func buildRoomSnapshot(ctx context.Context, d Deps, room store.RoomForUser,
	userID string, end streamtoken.RoomKey, limit int, timeNow int64,
	requester clientevent.Requester) (roomSnapshot, error) {

	var snap roomSnapshot

	state, err := d.Store.CurrentState(ctx, room.RoomID, room.RoomVersion)
	if err != nil {
		return snap, err
	}
	messages, start, err := d.Store.RecentEvents(ctx, room.RoomID, room.RoomVersion, limit, end)
	if err != nil {
		return snap, err
	}
	snap.Start = start

	if len(messages) > 0 {
		senders := distinctSenders(messages)
		vis, err := d.Store.VisibilityContext(ctx, room.RoomID, userID, senders)
		if err != nil {
			return snap, err
		}
		// is_peeking is false: callers only reach here for a room the user is
		// a member of.
		if err := vis.Check(false); err != nil {
			return snap, fmt.Errorf("%w: %v", errNeedsState, err)
		}
		kept := messages[:0]
		for _, ev := range messages {
			if vis.Visible(visibility.Event{Type: ev.Type, Sender: ev.Sender, IsState: ev.IsState}) {
				kept = append(kept, ev)
			}
		}
		messages = kept
	}

	// Timeline only: see the note in RoomInitialSync on Synapse's shared event
	// cache leaking prev_content into state reads.
	prevTargets := make([]*clientevent.Stored, 0, len(messages))
	for i := range messages {
		prevTargets = append(prevTargets, &messages[i].Stored)
	}
	if err := d.Store.AttachPrevContent(ctx, prevTargets); err != nil {
		return snap, err
	}

	memberships, err := d.Store.UserMembershipTimeline(ctx, room.RoomID, userID)
	if err != nil {
		return snap, err
	}

	cfg := clientevent.Config{Format: clientevent.FormatV1, Requester: requester,
		MSC4354Enabled: d.MSC4354Enabled}

	snap.State = make([]json.RawMessage, 0, len(state))
	for _, ev := range state {
		// Synapse wraps state with FilteredEvent.state(), whose membership is
		// None, so the state block carries no unsigned.membership.
		body, err := clientevent.Serialize(ev.Stored, timeNow, cfg)
		if err != nil {
			return snap, err
		}
		snap.State = append(snap.State, body)
	}

	snap.Chunk = make([]json.RawMessage, 0, len(messages))
	for _, ev := range messages {
		stored := ev.Stored
		stored.Membership = store.MembershipAt(memberships, ev.TopologicalOrder, ev.StreamOrdering)
		body, err := clientevent.Serialize(stored, timeNow, cfg)
		if err != nil {
			return snap, err
		}
		snap.Chunk = append(snap.Chunk, body)
	}
	return snap, nil
}

func distinctSenders(messages []store.TimelineEvent) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(messages))
	for _, ev := range messages {
		if !seen[ev.Sender] {
			seen[ev.Sender] = true
			out = append(out, ev.Sender)
		}
	}
	return out
}

// accountDataEvents renders account data entries as client events.
func accountDataEvents(entries []store.AccountDataEntry) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(entries))
	for _, e := range entries {
		body, err := json.Marshal(map[string]any{"type": e.Type, "content": e.Content})
		if err != nil {
			return nil, err
		}
		out = append(out, body)
	}
	return out, nil
}

// presenceEvents renders m.presence events.
//
// Mirrors format_user_presence_state: last_active_ago and status_msg appear
// only when set, currently_active only when the user is online.
func presenceEvents(states []store.PresenceState, timeNow int64) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(states))
	for _, p := range states {
		content := map[string]any{"presence": p.State, "user_id": p.UserID}
		// Synapse guards these with plain Python truthiness -- `if
		// state.last_active_ts:` -- so a stored 0 is omitted just like a NULL,
		// and an empty status message is omitted just like a missing one.
		// Treating NULL as the only absence emits `last_active_ago` equal to
		// the whole epoch.
		if p.HasLastActive && p.LastActiveTS != 0 {
			content["last_active_ago"] = timeNow - p.LastActiveTS
		}
		if p.HasStatusMsg && p.StatusMsg != "" {
			content["status_msg"] = p.StatusMsg
		}
		if p.State == "online" {
			content["currently_active"] = p.CurrentlyActive
		}
		body, err := json.Marshal(map[string]any{"type": "m.presence", "content": content})
		if err != nil {
			return nil, err
		}
		out = append(out, body)
	}
	return out, nil
}

// receiptEvent renders one room's receipts as the single m.receipt event the
// legacy endpoints emit, or nil when there are none.
//
// withThreads selects between Synapse's TWO receipt queries, which really do
// differ:
//
//   - `_get_linearized_receipts_for_room` (singular), used by
//     /rooms/{roomId}/initialSync, does not even SELECT thread_id and emits the
//     stored data unchanged.
//   - `_get_linearized_receipts_for_rooms` (plural), used by /initialSync,
//     selects thread_id and merges through ReceiptInRoom.merge_to_content,
//     which stamps `thread_id` into the data of threaded receipts and applies
//     MSC4102.
//
// So the same receipt is rendered differently by the two endpoints. Emitting
// thread_id from the singular endpoint, or omitting it from the plural one,
// differs from Synapse on every threaded receipt.
//
// Private read receipts of other users are removed in both:
// ReceiptEventSource.filter_out_private_receipts. Skipping that would publish
// exactly what m.read.private exists to keep private.
func receiptEvent(roomID string, rows []store.ReceiptRow, userID string, withThreads bool) (json.RawMessage, error) {
	visible := make([]store.ReceiptRow, 0, len(rows))
	for _, row := range rows {
		if row.ReceiptType == "m.read.private" && row.UserID != userID {
			continue
		}
		visible = append(visible, row)
	}

	// MSC4102: an unthreaded receipt always wins over a threaded one for the
	// same (user, event). The MSC is explicit that this drops only
	// semantically meaningless receipts.
	unthreaded := map[[2]string]bool{}
	if withThreads {
		for _, row := range visible {
			if row.ThreadID == "" {
				unthreaded[[2]string{row.UserID, row.EventID}] = true
			}
		}
	}

	content := map[string]map[string]map[string]json.RawMessage{}
	for _, row := range visible {
		data := row.Data
		if withThreads && row.ThreadID != "" {
			if unthreaded[[2]string{row.UserID, row.EventID}] {
				continue
			}
			var err error
			data, err = sjson.SetBytes(data, "thread_id", row.ThreadID)
			if err != nil {
				return nil, err
			}
		}
		byType, ok := content[row.EventID]
		if !ok {
			byType = map[string]map[string]json.RawMessage{}
			content[row.EventID] = byType
		}
		byUser, ok := byType[row.ReceiptType]
		if !ok {
			byUser = map[string]json.RawMessage{}
			byType[row.ReceiptType] = byUser
		}
		byUser[row.UserID] = data
	}
	if len(content) == 0 {
		return nil, nil
	}
	return json.Marshal(map[string]any{
		"type": "m.receipt", "room_id": roomID, "content": content,
	})
}
