package handlers

import (
	"context"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/visibility"
)

// historyVisKey and memberKey are the two state entries a visibility decision
// needs. Resolving the whole state map to get them would mean reading every
// state event in the room.
func visibilityKeys(userID string) []store.StateKey {
	return []store.StateKey{
		{Type: "m.room.history_visibility", StateKey: ""},
		{Type: "m.room.member", StateKey: userID},
	}
}

// filterVisible applies visibility to a timeline, returning the events the
// caller may see with their MSC4115 membership attached.
//
// Mirrors filter_events_for_client: the room state is resolved *at each event*
// and the decision is made per event. Everything the decision needs beyond the
// event itself is gathered here in bulk -- one query for the state groups, one
// per distinct group for the state, one each for the ignore list, erased
// senders and the retention policy -- rather than per event.
func filterVisible(ctx context.Context, d Deps, roomID, userID string,
	events []store.TimelineEvent, isPeeking bool, nowMS int64) ([]store.TimelineEvent, []string, error) {
	return filterVisibleAlways(ctx, d, roomID, userID, events, isPeeking, nowMS, nil)
}

// filterVisibleAlways is filterVisible with an escape hatch: events in
// alwaysInclude bypass the visibility decision entirely, which is Synapse's
// `always_include_ids`.
func filterVisibleAlways(ctx context.Context, d Deps, roomID, userID string,
	events []store.TimelineEvent, isPeeking bool, nowMS int64,
	alwaysInclude map[string]bool) ([]store.TimelineEvent, []string, error) {

	if len(events) == 0 {
		return events, nil, nil
	}

	eventIDs := make([]string, len(events))
	senders := make([]string, 0, len(events))
	seen := map[string]bool{}
	for i, ev := range events {
		eventIDs[i] = ev.EventID
		if !seen[ev.Sender] {
			seen[ev.Sender] = true
			senders = append(senders, ev.Sender)
		}
	}

	groups, err := d.Store.StateGroupsForEvents(ctx, eventIDs)
	if err != nil {
		return nil, nil, err
	}

	keys := visibilityKeys(userID)
	// Several events usually share a state group -- consecutive messages
	// change no state -- so resolving per group rather than per event is the
	// difference between one walk and a dozen.
	stateByGroup := make(map[int64]map[store.StateKey]store.StateEntry, len(groups))
	for _, group := range groups {
		if _, done := stateByGroup[group]; done {
			continue
		}
		state, err := d.Store.FilteredStateForGroup(ctx, group, keys)
		if err != nil {
			return nil, nil, err
		}
		stateByGroup[group] = state
	}

	extra, err := d.Store.VisibilityExtras(ctx, roomID, userID, senders)
	if err != nil {
		return nil, nil, err
	}

	vctx := visibility.Context{
		UserID:                 userID,
		IsPeeking:              isPeeking,
		IgnoredSenders:         extra.IgnoredSenders,
		ErasedSenders:          extra.ErasedSenders,
		RetentionMaxLifetimeMS: extra.RetentionMaxLifetimeMS,
		NowMS:                  nowMS,
	}

	kept := make([]store.TimelineEvent, 0, len(events))
	memberships := make([]string, 0, len(events))
	var pruned []string
	for _, ev := range events {
		state := visibility.StateAtEvent{}
		if group, ok := groups[ev.EventID]; ok {
			state.Present = true
			resolved := stateByGroup[group]
			state.HistoryVisibility = resolved[store.StateKey{
				Type: "m.room.history_visibility"}].HistoryVisibility
			state.UserMembership = resolved[store.StateKey{
				Type: "m.room.member", StateKey: userID}].Membership
		}

		verdict := visibility.Check(vctx, timelineToVisibilityEvent(ev), state)
		if alwaysInclude[ev.EventID] {
			verdict.Visible = true
			verdict.Pruned = false
		}
		if !verdict.Visible {
			continue
		}
		if verdict.Pruned {
			// The sender has been erased and the caller was not joined at the
			// time, so Synapse serves a redacted copy. Redaction is per room
			// version and belongs with the event machinery, not here; until it
			// exists, dropping the event is the safe direction -- it withholds
			// content rather than publishing content that should have been
			// stripped.
			pruned = append(pruned, ev.EventID)
			continue
		}
		kept = append(kept, ev)
		memberships = append(memberships, verdict.Membership)
	}
	return kept, memberships, nil
}

func timelineToVisibilityEvent(ev store.TimelineEvent) visibility.Event {
	out := visibility.Event{
		EventID:        ev.EventID,
		Type:           ev.Type,
		StateKey:       ev.StateKey,
		IsState:        ev.IsState,
		Sender:         ev.Sender,
		OriginServerTS: gjson.GetBytes(ev.JSON, "origin_server_ts").Int(),
		SoftFailed:     softFailed(ev.InternalMetadata),
	}
	if ev.Type == "m.room.member" {
		out.Membership = gjson.GetBytes(ev.JSON, "content.membership").String()
		out.PrevMembership = gjson.GetBytes(ev.JSON, "unsigned.prev_content.membership").String()
	}
	if ev.Type == "m.room.history_visibility" {
		out.PrevHistoryVisibility = gjson.GetBytes(ev.JSON,
			"unsigned.prev_content.history_visibility").String()
	}
	return out
}

// softFailed reads internal_metadata.soft_failed.
//
// Synapse drops soft-failed events from client responses before any visibility
// check runs. Only a server admin who has explicitly asked for them sees them.
func softFailed(internalMetadata []byte) bool {
	if len(internalMetadata) == 0 {
		return false
	}
	return gjson.GetBytes(internalMetadata, "soft_failed").Bool()
}
