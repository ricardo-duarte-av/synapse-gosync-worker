package handlers

import (
	"context"
	"encoding/json"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/filter"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/streamtoken"
)

// maxTimelineRepeats is Synapse's `max_repeat`: how many times a room's
// timeline may be re-paginated to make up for events the filters removed.
//
// The cap matters. A filter matching nothing in a large room would otherwise
// walk the room's entire history looking for a tenth event that does not
// exist, once per room, on every sync.
const maxTimelineRepeats = 5

// loadFilteredRecents builds a room's timeline for an initial sync.
//
// Port of Synapse's _load_filtered_recents with `potential_recents` empty,
// which is the initial-sync case: there is no window to work from, so events
// are paginated backwards from the now token.
//
// The loop is the part that is easy to leave out and wrong to. Events are
// loaded, then thinned twice -- once by the client's filter, once by history
// visibility -- and either pass can leave the timeline short of the limit. When
// it does, Synapse goes back for more, up to five times. Without the loop, a
// filter selecting one event type returns a nearly empty timeline for a busy
// room, and `limited` says the client should paginate for the rest, which sends
// it round the same filter again.
func loadFilteredRecents(ctx context.Context, d Deps, room store.RoomForUser, userID string,
	endKey streamtoken.RoomKey, timeNow int64, f *filter.Collection) (
	[]store.TimelineEvent, []string, streamtoken.RoomKey, bool, error) {

	timelineLimit := f.TimelineLimit()

	// A hole in the room's history makes the timeline `limited` whatever else
	// happens, so the client knows to paginate rather than assume the events
	// it was given are contiguous. On an initial sync that is the gap's only
	// effect -- there is no `since` for it to move.
	_, gap, err := d.Store.TimelineGap(ctx, room.RoomID, nil, endKey)
	if err != nil {
		return nil, nil, endKey, false, err
	}

	// A filter that can match nothing gets an empty, explicitly unlimited
	// timeline: telling the client to paginate would be pointless, because the
	// same filter applies to /messages.
	if f.BlocksAllRoomTimeline() || f.BlocksAllRooms() || timelineLimit <= 0 {
		return nil, nil, endKey, false, nil
	}

	loadLimit := timelineLimit * 2
	if loadLimit < 10 {
		loadLimit = 10
	}

	var (
		messages    []store.TimelineEvent
		memberships []string
		start       = endKey
		limited     = true
		from        = endKey
	)
	for repeat := 0; repeat < maxTimelineRepeats && limited && len(messages) < timelineLimit; repeat++ {
		loaded, next, more, err := d.Store.PaginateBackwards(ctx, room.RoomID,
			room.RoomVersion, loadLimit, from)
		if err != nil {
			return nil, nil, endKey, false, err
		}
		if len(loaded) == 0 && !more {
			limited = false
			break
		}
		// `from` advances so the next pass walks further back; `start` does
		// NOT. Synapse keeps `room_key` at the token it was given and only
		// moves it when the timeline is trimmed below, so an untrimmed
		// timeline reports a prev_batch equal to the sync point itself --
		// even though pagination walked past it. Following the pagination
		// cursor instead looks more correct and is measurably not what
		// Synapse returns.
		from, limited = next, more

		loaded = filterTimeline(f, room.RoomID, loaded)
		loaded, err = applyRelationFilter(ctx, d, f.TimelineFilter(), loaded)
		if err != nil {
			return nil, nil, endKey, false, err
		}

		// The always-include set is computed from what SURVIVED the client's
		// filter, not from what was loaded: an event the client filtered out
		// is gone, and re-admitting it because it happens to be current state
		// would override the client's own request.
		alwaysInclude, err := stateEventIDsInCurrentState(ctx, d, loaded)
		if err != nil {
			return nil, nil, endKey, false, err
		}
		visible, ms, err := filterVisibleAlways(ctx, d, room.RoomID, userID, loaded,
			false, timeNow, alwaysInclude)
		if err != nil {
			return nil, nil, endKey, false, err
		}

		// Each pass walks further back, so its events are OLDER than what we
		// already have and belong in front of them.
		messages = append(visible, messages...)
		memberships = append(ms, memberships...)
	}

	// Trim to the requested limit, keeping the NEWEST events. The token form
	// changes with it: a trimmed page reports a live position just before the
	// first event kept, an untrimmed one reports where the topological walk
	// stopped.
	if len(messages) > timelineLimit {
		limited = true
		messages = messages[len(messages)-timelineLimit:]
		memberships = memberships[len(memberships)-timelineLimit:]
		start = streamtoken.Live(messages[0].StreamOrdering - 1)
	}
	return messages, memberships, start, limited || gap, nil
}

// filterTimeline applies the client's timeline filter, room filter included.
func filterTimeline(f *filter.Collection, roomID string, events []store.TimelineEvent) []store.TimelineEvent {
	out := make([]store.TimelineEvent, 0, len(events))
	for _, ev := range events {
		if f.CheckRoomTimeline(timelineFilterEvent(roomID, ev)) {
			out = append(out, ev)
		}
	}
	return out
}

// filterAccountDataEntries applies the room account data filter.
func filterAccountDataEntries(f *filter.Collection, roomID string,
	entries []store.AccountDataEntry) []store.AccountDataEntry {

	out := make([]store.AccountDataEntry, 0, len(entries))
	for _, e := range entries {
		if f.CheckRoomAccountData(accountDataFilterEvent(roomID, e)) {
			out = append(out, e)
		}
	}
	return out
}

// filterAccountDataEvents applies the GLOBAL account data filter to
// already-rendered events.
//
// The global section is filtered after rendering because m.push_rules is
// synthesised rather than stored: there is no AccountDataEntry to filter.
func filterAccountDataEvents(f *filter.Collection, roomID string,
	events []json.RawMessage) []json.RawMessage {

	out := make([]json.RawMessage, 0, len(events))
	for _, e := range events {
		if f.CheckGlobalAccountData(ephemeralFilterEvent(roomID, e)) {
			out = append(out, e)
		}
	}
	return out
}

// filterEphemeral applies the ephemeral filter to rendered receipts and typing.
func filterEphemeral(f *filter.Collection, roomID string, events []json.RawMessage) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(events))
	for _, e := range events {
		if f.CheckRoomEphemeral(ephemeralFilterEvent(roomID, e)) {
			out = append(out, e)
		}
	}
	return out
}

// filterPresence applies the presence filter.
func filterPresence(f *filter.Collection, states []store.PresenceState) []store.PresenceState {
	out := make([]store.PresenceState, 0, len(states))
	for _, p := range states {
		if f.CheckPresence(presenceFilterEvent(p.UserID)) {
			out = append(out, p)
		}
	}
	return out
}

// stripRoomIDs removes room_id from every ephemeral event.
//
// Applied after filtering, not before: the spec says a client must not receive
// room_id on a receipt or typing notification nested under its room, but the
// filter is entitled to see it -- Synapse builds the dict with room_id, filters
// it, and strips the key on the way into the response.
func stripRoomIDs(events []json.RawMessage) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(events))
	for _, e := range events {
		out = append(out, stripRoomID(e))
	}
	return out
}

// filterStateBlock applies the state filter to an already-loaded state block,
// returning the event IDs that survive in their original order.
func filterStateBlock(f *filter.Collection, roomID string, ids []string,
	loaded map[string]store.StateEvent) []string {

	out := make([]string, 0, len(ids))
	for _, id := range ids {
		ev, ok := loaded[id]
		if !ok {
			// Not loaded, so it cannot be rendered either. Dropping it here
			// keeps the two loops over this slice in agreement.
			continue
		}
		if f.CheckRoomState(stateFilterEvent(roomID, ev)) {
			out = append(out, id)
		}
	}
	return out
}
