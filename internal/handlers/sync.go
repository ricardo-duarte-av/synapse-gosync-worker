package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"

	"github.com/tidwall/sjson"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/auth"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/clientevent"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/matrixerr"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/pushrules"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/server"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/streamtoken"
)

// defaultTimelineLimit is the default filter's timeline limit.
const defaultTimelineLimit = 10

// Sync serves /sync.
//
// Only an initial sync -- no `since` -- is implemented (M3). An incremental
// sync needs the full stream-token machinery and is M4; a `since` is refused
// rather than answered as though it were an initial sync, which would resend
// the client's entire history.
func Sync(d Deps) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ann := server.Annotate(r.Context())
		if ann != nil {
			ann.Endpoint = "sync"
		}

		verdict, ok := authenticate(w, r, d, ann)
		if !ok {
			return
		}

		var (
			body   []byte
			status int
			mxErr  *matrixerr.Error
		)
		if since := r.URL.Query().Get("since"); since != "" {
			if ann != nil {
				ann.Since = since
			}
			body, status, mxErr = incrementalSync(r, d, verdict, since)
		} else {
			body, status, mxErr = initialSyncV2(r, d, verdict)
		}
		if mxErr != nil {
			refuse(w, ann, status, *mxErr)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
}

func initialSyncV2(r *http.Request, d Deps, verdict auth.Verdict) ([]byte, int, *matrixerr.Error) {
	ctx := r.Context()
	ann := server.Annotate(ctx)

	now, _, mxErr := nowToken(r, d)
	if mxErr != nil {
		return nil, http.StatusBadRequest, mxErr
	}
	timeNow, mxErr := nowMillis(r, d)
	if mxErr != nil {
		return nil, http.StatusBadRequest, mxErr
	}
	if ann != nil {
		ann.NextBatch = now.String()
	}

	rooms, err := d.Store.RoomsForUser(ctx, verdict.UserID, []string{"invite", "join"})
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "rooms for user", err)
	}

	requester := clientevent.Requester{
		UserID:   verdict.UserID,
		DeviceID: verdict.DeviceID,
		IsGuest:  verdict.IsGuest,
	}
	if tokenID, err := d.Store.AccessTokenID(ctx, auth.ExtractToken(r)); err == nil {
		requester.TokenID = tokenID
	}
	// /sync strips room_id: the room is already the key in the response.
	cfg := clientevent.Config{Format: clientevent.FormatV2NoRoomID, Requester: requester,
		MSC4354Enabled: d.MSC4354Enabled}
	// The invite section carries stripped room state, which every other
	// section must not.
	strippedCfg := cfg
	strippedCfg.IncludeStrippedRoomState = true

	accountDataByRoom, err := d.Store.AllRoomAccountData(ctx, verdict.UserID, d.MSC3391Enabled)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "room account data", err)
	}

	joinedRooms := map[string]any{}
	invitedRooms := map[string]any{}

	for _, room := range rooms {
		if room.Membership == "invite" {
			invite, err := d.Store.InviteEvent(ctx, room.EventID, room.RoomID, room.RoomVersion)
			if err != nil {
				return nil, http.StatusInternalServerError, internalError(d, "invite event", err)
			}
			body, err := clientevent.Serialize(invite.Stored, timeNow, strippedCfg)
			if err != nil {
				return nil, http.StatusInternalServerError, internalError(d, "serialise invite", err)
			}
			invitedRooms[room.RoomID] = map[string]any{
				"invite_state": map[string]any{"events": []json.RawMessage{body}},
			}
			continue
		}

		entry, err := syncRoomEntry(ctx, d, room, verdict.UserID, now.Room, timeNow, cfg,
			accountDataByRoom[room.RoomID], now)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "room entry", err)
		}
		joinedRooms[room.RoomID] = entry
	}

	resp := map[string]any{"next_batch": now.String()}

	globalAD, err := d.Store.GlobalAccountData(ctx, verdict.UserID, d.MSC3391Enabled)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "global account data", err)
	}
	events, err := accountDataEvents(globalAD)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "global account data", err)
	}

	// m.push_rules is synthesised, not stored: Synapse keeps only a user's
	// deviations from a built-in ruleset and rebuilds the whole thing on every
	// read. It is prepended, as _generate_sync_entry_for_account_data does.
	userRules, ruleEnabled, err := d.Store.PushRules(ctx, verdict.UserID)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "push rules", err)
	}
	rulesContent, err := pushrules.Format(verdict.UserID, userRules, ruleEnabled, d.PushRuleFeatures)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "push rules", err)
	}
	rulesEvent, err := json.Marshal(map[string]any{
		"type": "m.push_rules", "content": rulesContent,
	})
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "push rules", err)
	}
	events = append([]json.RawMessage{rulesEvent}, events...)

	if len(events) > 0 {
		resp["account_data"] = map[string]any{"events": events}
	}

	presenceStates, err := d.Store.SharedRoomPresence(ctx, verdict.UserID)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "presence", err)
	}
	if len(presenceStates) > 0 {
		events, err := syncPresenceEvents(presenceStates, timeNow)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "presence", err)
		}
		resp["presence"] = map[string]any{"events": events}
	}

	keys, err := d.Store.DeviceKeyCounts(ctx, verdict.UserID, verdict.DeviceID)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "device keys", err)
	}
	// Always present, even when empty: a client cannot otherwise tell "no keys"
	// from "nothing changed".
	resp["device_one_time_keys_count"] = keys.OneTimeKeys
	resp["device_unused_fallback_key_types"] = keys.UnusedFallbackKeyTypes

	roomsOut := map[string]any{}
	if len(joinedRooms) > 0 {
		roomsOut["join"] = joinedRooms
	}
	if len(invitedRooms) > 0 {
		roomsOut["invite"] = invitedRooms
	}
	if len(roomsOut) > 0 {
		resp["rooms"] = roomsOut
	}

	body, err := json.Marshal(resp)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "encode response", err)
	}
	return body, http.StatusOK, nil
}

// syncPresenceEvents renders presence for /sync, which differs from the legacy
// endpoints: the user is named by a top-level `sender`, not inside `content`.
func syncPresenceEvents(states []store.PresenceState, timeNow int64) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(states))
	for _, p := range states {
		content := map[string]any{"presence": p.State}
		if p.HasLastActive && p.LastActiveTS != 0 {
			content["last_active_ago"] = timeNow - p.LastActiveTS
		}
		if p.HasStatusMsg && p.StatusMsg != "" {
			content["status_msg"] = p.StatusMsg
		}
		if p.State == "online" {
			content["currently_active"] = p.CurrentlyActive
		}
		body, err := json.Marshal(map[string]any{
			"type": "m.presence", "sender": p.UserID, "content": content,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, body)
	}
	return out, nil
}

// syncRoomEntry builds one joined room's section of an initial /sync.
func syncRoomEntry(ctx context.Context, d Deps, room store.RoomForUser, userID string,
	endKey streamtoken.RoomKey, timeNow int64, cfg clientevent.Config,
	accountData []store.AccountDataEntry, now streamtoken.Token) (map[string]any, error) {

	// Load twice the timeline limit, because visibility filtering happens after
	// and would otherwise leave the timeline short. Synapse's
	// `load_limit = max(timeline_limit * 2, 10)`.
	loadLimit := defaultTimelineLimit * 2
	if loadLimit < 10 {
		loadLimit = 10
	}
	messages, pageStart, limited, err := d.Store.PaginateBackwards(ctx, room.RoomID,
		room.RoomVersion, loadLimit, endKey)
	if err != nil {
		return nil, err
	}

	// A state event that is still part of current state is shown regardless of
	// history visibility: a client that can see the current state gains nothing
	// from having the event that established it withheld.
	alwaysInclude, err := stateEventIDsInCurrentState(ctx, d, messages)
	if err != nil {
		return nil, err
	}

	messages, memberships, err := filterVisibleAlways(ctx, d, room.RoomID, userID, messages,
		false, timeNow, alwaysInclude)
	if err != nil {
		return nil, err
	}

	// Trim to the requested limit, keeping the NEWEST events. The token form
	// changes with it: a trimmed page reports a live position just before the
	// first event kept, an untrimmed one reports where the topological walk
	// stopped.
	start := pageStart
	if len(messages) > defaultTimelineLimit {
		limited = true
		messages = messages[len(messages)-defaultTimelineLimit:]
		memberships = memberships[len(memberships)-defaultTimelineLimit:]
		start = streamtoken.Live(messages[0].StreamOrdering - 1)
	}

	// The `state` block is what the client needs to interpret the timeline that
	// follows it, so it is the state at the START of the timeline, minus
	// anything the timeline itself carries. See _calculate_state.
	stateIDs, err := syncStateBlock(ctx, d, room, messages, endKey)
	if err != nil {
		return nil, err
	}

	stateEventIDs := make([]string, 0, len(stateIDs))
	for _, id := range stateIDs {
		stateEventIDs = append(stateEventIDs, id)
	}
	sort.Strings(stateEventIDs)
	stateEvents, err := d.Store.EventsByID(ctx, stateEventIDs, room.RoomVersion)
	if err != nil {
		return nil, err
	}

	ids := timelineEventIDs(messages)
	ids = append(ids, stateEventIDs...)
	redactions, err := d.Store.Redactions(ctx, ids)
	if err != nil {
		return nil, err
	}

	prevTargets := make([]*clientevent.Stored, 0, len(messages))
	for i := range messages {
		prevTargets = append(prevTargets, &messages[i].Stored)
	}
	if err := d.Store.AttachPrevContent(ctx, prevTargets); err != nil {
		return nil, err
	}

	// Bundled aggregations, for a limited timeline only -- which an initial
	// sync always is. A client given the whole history can aggregate for
	// itself; one given a window cannot see the replies outside it.
	var aggs map[string]aggregation
	var nestedIDs []string
	if limited {
		vis, err := d.Store.VisibilityExtras(ctx, room.RoomID, userID, nil)
		if err != nil {
			return nil, err
		}
		aggs, nestedIDs, err = bundleAggregations(ctx, d, userID, messages, vis.IgnoredSenders)
		if err != nil {
			return nil, err
		}
	}
	// The events nested inside a bundle -- a thread's latest reply, an edit --
	// are serialised in full, so they have to be loaded.
	var nested map[string]store.StateEvent
	if len(aggs) > 0 {
		want := append([]string(nil), nestedIDs...)
		for _, a := range aggs {
			if a.replaceID != "" {
				want = append(want, a.replaceID)
			}
		}
		nested, err = d.Store.EventsByID(ctx, want, room.RoomVersion)
		if err != nil {
			return nil, err
		}
	}

	timeline := make([]json.RawMessage, 0, len(messages))
	for i, ev := range messages {
		stored := ev.Stored
		stored.Membership = memberships[i]
		if err := attachRedaction(&stored, redactions, timeNow, cfg); err != nil {
			return nil, err
		}
		body, err := clientevent.Serialize(stored, timeNow, cfg)
		if err != nil {
			return nil, err
		}
		if agg, ok := aggs[ev.EventID]; ok {
			body, err = attachAggregations(body, agg, aggs, nested, timeNow, cfg)
			if err != nil {
				return nil, err
			}
		}
		timeline = append(timeline, body)
	}

	stateJSON := make([]json.RawMessage, 0, len(stateEventIDs))
	for _, id := range stateEventIDs {
		ev, ok := stateEvents[id]
		if !ok {
			continue
		}
		stored := ev.Stored
		if err := attachRedaction(&stored, redactions, timeNow, cfg); err != nil {
			return nil, err
		}
		body, err := clientevent.Serialize(stored, timeNow, cfg)
		if err != nil {
			return nil, err
		}
		stateJSON = append(stateJSON, body)
	}

	adEvents, err := accountDataEvents(accountData)
	if err != nil {
		return nil, err
	}

	receiptsByRoom, err := d.Store.MultiRoomReceipts(ctx, []string{room.RoomID}, now.Receipt.MaxStreamPos())
	if err != nil {
		return nil, err
	}
	receiptRows := receiptsByRoom[room.RoomID]
	ephemeral := []json.RawMessage{}
	// withThreads: /sync uses the multi-room receipt path, which selects
	// thread_id and applies MSC4102 -- unlike /rooms/{id}/initialSync.
	if ev, err := receiptEvent(room.RoomID, receiptRows, userID, true); err != nil {
		return nil, err
	} else if ev != nil {
		// /sync's ephemeral receipts carry no room_id: the room is the key.
		ephemeral = append(ephemeral, stripRoomID(ev))
	}

	unread, err := d.Store.UnreadNotifications(ctx, room.RoomID, userID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"timeline": map[string]any{
			"events":     timeline,
			"prev_batch": now.WithRoomKey(start).String(),
			"limited":    limited,
		},
		"state":        map[string]any{"events": stateJSON},
		"account_data": map[string]any{"events": adEvents},
		"ephemeral":    map[string]any{"events": ephemeral},
		"unread_notifications": map[string]any{
			"notification_count": unread.NotifyCount,
			"highlight_count":    unread.HighlightCount,
		},
		// Synapse computes a summary only when the filter enables lazy-loading
		// of members (handlers/sync.py:3242), because that is when a client
		// lacks the memberships to name the room itself. With the default
		// filter it sends an empty object -- not a populated one, and not a
		// missing key.
		"summary": map[string]any{},
		// MSC2654, enabled on this deployment.
		"org.matrix.msc2654.unread_count": unread.UnreadCount,
	}, nil
}

// syncStateBlock works out the `state` block for an initial sync.
//
// Synapse's _calculate_state, with previous_timeline_end empty:
//
//	state = (state_at_timeline_end | state_at_timeline_start) - state_in_timeline
//
// Both ends are needed, not just the start. With a forked DAG a state event can
// sit off to one side of the timeline: it is absent from the state at the
// timeline's first event, yet the client still needs it. And the start is
// needed too, for a state event superseded *within* the timeline -- the end
// only knows about the later one, but the earlier one may be what an event in
// the timeline was interpreted under.
//
// Events carried by the timeline itself are subtracted: sending them twice
// would be redundant, and Synapse does not.
func syncStateBlock(ctx context.Context, d Deps, room store.RoomForUser,
	messages []store.TimelineEvent, endKey streamtoken.RoomKey) (map[store.StateKey]string, error) {

	// The state at the END of the timeline -- which is the now token, NOT the
	// room's current state. They differ whenever the room changed between the
	// token being minted and this query running, which in a busy room is most
	// of the time, and the response would then describe state the client's
	// token does not cover.
	end, err := d.Store.StateIDsAt(ctx, room.RoomID, endKey)
	if err != nil {
		return nil, err
	}

	start := end
	if len(messages) > 0 {
		groups, err := d.Store.StateGroupsForEvents(ctx, []string{messages[0].EventID})
		if err != nil {
			return nil, err
		}
		if group, ok := groups[messages[0].EventID]; ok {
			// Strictly this is the state *after* the first timeline event
			// rather than before it, which is what Synapse uses too.
			start, err = d.Store.FullStateForGroup(ctx, group)
			if err != nil {
				return nil, err
			}
		}
	}

	// What the timeline itself contributes to room state: the LAST state event
	// per state key, not every state event in the timeline.
	//
	// The distinction decides whether an earlier state event in the same
	// timeline is repeated in the `state` block. It must be: a client reads
	// `state` as the state before the timeline, so if the timeline changes a
	// key twice, the block has to carry the value in force at the start.
	// Subtracting every state event in the timeline drops that key entirely,
	// and the client is left interpreting the timeline against nothing.
	timelineContains := map[store.StateKey]string{}
	for _, ev := range messages {
		if ev.IsState {
			timelineContains[store.StateKey{Type: ev.Type, StateKey: ev.StateKey}] = ev.EventID
		}
	}
	inTimeline := make(map[string]bool, len(timelineContains))
	for _, id := range timelineContains {
		inTimeline[id] = true
	}

	out := map[store.StateKey]string{}
	// `end` last: when the state at both ends of the timeline carries a
	// different event for one key, and neither is in the timeline itself, both
	// survive the subtraction and only one can be reported. Synapse resolves
	// that collision by iterating a Python set, so its choice is arbitrary --
	// but observed consistently to be the later event, and the later one is
	// what the client's state will be going forward.
	for _, m := range []map[store.StateKey]string{start, end} {
		for k, id := range m {
			if inTimeline[id] {
				continue
			}
			// m.room.aliases is dropped from the state block outright, a
			// second and separate place from the timeline filter that drops
			// it. Until MSC2261, a malicious alias event cannot be redacted.
			if k.Type == "m.room.aliases" {
				continue
			}
			out[k] = id
		}
	}
	return out, nil
}

// stripRoomID removes the room_id from an ephemeral event.
//
// The legacy endpoints put receipts in a flat list and so must say which room
// each belongs to; /sync nests them under the room and does not.
func stripRoomID(body json.RawMessage) json.RawMessage {
	out, err := sjson.DeleteBytes(body, "room_id")
	if err != nil {
		return body
	}
	return out
}

// stateEventIDsInCurrentState returns the timeline's state events that are
// still part of the room's current state.
func stateEventIDsInCurrentState(ctx context.Context, d Deps,
	messages []store.TimelineEvent) (map[string]bool, error) {

	var ids []string
	for _, ev := range messages {
		if ev.IsState {
			ids = append(ids, ev.EventID)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return d.Store.EventsInCurrentState(ctx, ids)
}
