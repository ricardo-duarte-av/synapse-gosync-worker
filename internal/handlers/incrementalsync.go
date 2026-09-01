package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/auth"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/clientevent"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/matrixerr"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/server"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/streamtoken"
)

// incrementalSync serves a /sync carrying a `since`.
//
// The shape differs from an initial sync in one structural way that governs
// everything else: a room appears ONLY if something happened in it. Synapse
// drops a room whose timeline, state, account data and ephemeral are all empty
// (`_generate_room_entry`), and a response where nothing at all happened has no
// `rooms` key whatsoever.
func incrementalSync(r *http.Request, d Deps, verdict auth.Verdict, sinceRaw string) (
	[]byte, int, *matrixerr.Error) {

	ctx := r.Context()
	ann := server.Annotate(ctx)

	since, err := streamtoken.Parse(sinceRaw)
	if err != nil {
		return nil, http.StatusBadRequest, &matrixerr.Error{
			ErrCode: matrixerr.CodeInvalidParam, Error: err.Error()}
	}

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

	sincePos := since.Room.MaxStreamPos()
	nowPos := now.Room.MaxStreamPos()

	rooms, err := d.Store.RoomsForUser(ctx, verdict.UserID, []string{"invite", "join"})
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "rooms for user", err)
	}
	roomVersions := map[string]string{}
	joinedIDs := make([]string, 0, len(rooms))
	for _, room := range rooms {
		roomVersions[room.RoomID] = room.RoomVersion
		if room.Membership == "join" {
			joinedIDs = append(joinedIDs, room.RoomID)
		}
	}

	requester := clientevent.Requester{
		UserID: verdict.UserID, DeviceID: verdict.DeviceID, IsGuest: verdict.IsGuest,
	}
	if tokenID, err := d.Store.AccessTokenID(ctx, auth.ExtractToken(r)); err == nil {
		requester.TokenID = tokenID
	}
	cfg := clientevent.Config{Format: clientevent.FormatV2NoRoomID, Requester: requester,
		MSC4354Enabled: d.MSC4354Enabled}
	strippedCfg := cfg
	strippedCfg.IncludeStrippedRoomState = true

	timelines, err := d.Store.RoomTimelineSince(ctx, joinedIDs, roomVersions,
		sincePos, nowPos, defaultTimelineLimit+1)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "room timelines", err)
	}

	accountDataByRoom, err := d.Store.RoomAccountDataSince(ctx, verdict.UserID,
		since.AccountData, now.AccountData, d.MSC3391Enabled)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "room account data", err)
	}
	receiptsByRoom, err := d.Store.ReceiptsSince(ctx, joinedIDs,
		since.Receipt.MaxStreamPos(), now.Receipt.MaxStreamPos())
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "receipts", err)
	}

	joinedRooms := map[string]any{}
	var newlyJoined []string
	for _, room := range rooms {
		if room.Membership != "join" {
			continue
		}

		// A room joined inside the window is "newly joined", and Synapse then
		// treats it exactly as an initial sync would: full state, a timeline
		// paginated back from now rather than only the events since `since`,
		// and `limited` set. The client has never seen this room, so sending
		// it a delta against a state it does not have would be meaningless.
		wasJoined, err := d.Store.MembershipAtPosition(ctx, room.RoomID, verdict.UserID, since.Room)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "membership at since", err)
		}
		if wasJoined != "join" {
			newlyJoined = append(newlyJoined, room.RoomID)
			entry, err := syncRoomEntry(ctx, d, room, verdict.UserID, now.Room, timeNow, cfg,
				accountDataByRoom[room.RoomID], now)
			if err != nil {
				return nil, http.StatusInternalServerError, internalError(d, "room entry", err)
			}
			// Ephemeral is bounded by `since` even for a newly joined room:
			// receipts are a stream like any other, and the client is not
			// entitled to a replay of them just for joining.
			ephemeral := []json.RawMessage{}
			if rows := receiptsByRoom[room.RoomID]; len(rows) > 0 {
				ev, err := receiptEvent(room.RoomID, rows, verdict.UserID, true)
				if err != nil {
					return nil, http.StatusInternalServerError, internalError(d, "receipts", err)
				}
				if ev != nil {
					ephemeral = append(ephemeral, stripRoomID(ev))
				}
			}
			entry["ephemeral"] = map[string]any{"events": ephemeral}
			joinedRooms[room.RoomID] = entry
			continue
		}

		entry, err := incrementalRoomEntry(ctx, d, room, verdict.UserID, since, now,
			timeNow, cfg, timelines[room.RoomID],
			accountDataByRoom[room.RoomID], receiptsByRoom[room.RoomID])
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "room entry", err)
		}
		if entry != nil {
			joinedRooms[room.RoomID] = entry
		}
	}

	// Membership transitions visible in what we just emitted decide who else
	// the client must re-key. Synapse derives them from the response it has
	// built rather than from a query, so the two cannot disagree.
	joinedOrInvited, leaveCandidates := userChangesFromRooms(joinedRooms)
	leftUsers, err := resolveLeftUsers(ctx, d, leaveCandidates)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "previous memberships", err)
	}

	// Invites are reported only when the membership event itself is new.
	changes, err := d.Store.MembershipChangesForUser(ctx, verdict.UserID, sincePos, nowPos)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "membership changes", err)
	}
	invitedRooms := map[string]any{}
	lastChange := map[string]store.MembershipChange{}
	for _, c := range changes {
		lastChange[c.RoomID] = c
	}
	for roomID, c := range lastChange {
		if c.Membership != "invite" {
			continue
		}
		invite, err := d.Store.InviteEvent(ctx, c.EventID, roomID, roomVersions[roomID])
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "invite event", err)
		}
		body, err := clientevent.Serialize(invite.Stored, timeNow, strippedCfg)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "serialise invite", err)
		}
		invitedRooms[roomID] = map[string]any{
			"invite_state": map[string]any{"events": []json.RawMessage{body}},
		}
	}

	resp := map[string]any{"next_batch": now.String()}

	globalAD, err := d.Store.GlobalAccountDataSince(ctx, verdict.UserID,
		since.AccountData, now.AccountData, d.MSC3391Enabled)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "global account data", err)
	}
	if len(globalAD) > 0 {
		events, err := accountDataEvents(globalAD)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "global account data", err)
		}
		resp["account_data"] = map[string]any{"events": events}
	}

	presenceStates, err := d.Store.PresenceSince(ctx, verdict.UserID, since.Presence, now.Presence)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "presence", err)
	}
	// Joining a room entitles the caller to the presence of everyone already in
	// it, whatever the presence stream has been doing. Without this, a client
	// that joins a busy room sees every member as offline until each of them
	// happens to change state.
	// Presence is owed for anyone who just became visible: every member of a
	// room the caller joined, and anyone who joined or was invited to a room
	// they were already in. Neither group need have touched the presence
	// stream, so neither is found by the window query above.
	extraUsers := append([]string(nil), joinedOrInvited...)
	if len(newlyJoined) > 0 {
		members, err := d.Store.JoinedMembersOf(ctx, newlyJoined)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "newly joined members", err)
		}
		extraUsers = append(extraUsers, members...)
	}
	if len(extraUsers) > 0 {
		seenExtra := map[string]bool{}
		extra := make([]string, 0, len(extraUsers))
		for _, m := range extraUsers {
			if m == verdict.UserID || seenExtra[m] {
				continue
			}
			seenExtra[m] = true
			extra = append(extra, m)
		}
		stored, err := d.Store.PresenceForUsers(ctx, extra)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "extra presence", err)
		}
		// A user with no presence_stream row still gets an entry: the presence
		// handler substitutes a default offline state rather than omitting
		// them. Omitting them loses a member from every room whose users have
		// never set presence.
		byUser := make(map[string]store.PresenceState, len(stored))
		for _, p := range stored {
			byUser[p.UserID] = p
		}
		states := make([]store.PresenceState, 0, len(extra))
		for _, u := range extra {
			if p, ok := byUser[u]; ok {
				states = append(states, p)
				continue
			}
			states = append(states, store.PresenceState{UserID: u, State: "offline"})
		}

		seen := map[string]bool{}
		merged := make([]store.PresenceState, 0, len(presenceStates)+len(states))
		for _, p := range append(presenceStates, states...) {
			if seen[p.UserID] {
				continue
			}
			seen[p.UserID] = true
			merged = append(merged, p)
		}
		presenceStates = merged
	}
	if len(presenceStates) > 0 {
		events, err := syncPresenceEvents(presenceStates, timeNow)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "presence", err)
		}
		resp["presence"] = map[string]any{"events": events}
	}

	// device_lists tells an end-to-end encrypted client whose keys to re-fetch.
	// Missing an entry here leaves it unable to decrypt, which is why joining a
	// room adds every member: the client has none of their keys yet.
	changed, err := d.Store.DeviceListChanges(ctx, verdict.UserID, joinedIDs,
		since.DeviceList.MaxStreamPos(), now.DeviceList.MaxStreamPos())
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "device lists", err)
	}
	if len(newlyJoined) > 0 {
		members, err := d.Store.JoinedMembersOf(ctx, newlyJoined)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "newly joined members", err)
		}
		changed = append(changed, members...)
	}
	changed = append(changed, joinedOrInvited...)

	var left []string
	if len(leftUsers) > 0 {
		sharing, err := d.Store.UsersSharingAnyRoom(ctx, verdict.UserID)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "shared rooms", err)
		}
		for _, u := range leftUsers {
			// Still in another room with us, so not left from our point of
			// view: telling the client otherwise makes it drop keys it needs.
			if !sharing[u] {
				left = append(left, u)
			}
		}
	}

	deviceLists := map[string]any{}
	if len(changed) > 0 {
		seen := map[string]bool{}
		list := make([]string, 0, len(changed))
		for _, u := range changed {
			if seen[u] {
				continue
			}
			seen[u] = true
			list = append(list, u)
		}
		deviceLists["changed"] = list
	}
	if len(left) > 0 {
		deviceLists["left"] = left
	}
	if len(deviceLists) > 0 {
		resp["device_lists"] = deviceLists
	}

	keys, err := d.Store.DeviceKeyCounts(ctx, verdict.UserID, verdict.DeviceID)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "device keys", err)
	}
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

// incrementalRoomEntry builds one joined room's delta, or nil if nothing
// happened in it.
func incrementalRoomEntry(ctx context.Context, d Deps, room store.RoomForUser, userID string,
	since, now streamtoken.Token, timeNow int64, cfg clientevent.Config,
	raw []store.TimelineEvent, accountData []store.AccountDataEntry,
	receipts []store.ReceiptRow) (map[string]any, error) {

	// One more than the limit was fetched, so "was there more" is answerable
	// without a second query.
	limited := len(raw) > defaultTimelineLimit
	if limited {
		raw = raw[len(raw)-defaultTimelineLimit:]
	}

	messages, memberships, err := filterVisible(ctx, d, room.RoomID, userID, raw, false, timeNow)
	if err != nil {
		return nil, err
	}

	stateIDs, err := incrementalStateDelta(ctx, d, room, messages, limited, since, now)
	if err != nil {
		return nil, err
	}

	adEvents, err := accountDataEvents(accountData)
	if err != nil {
		return nil, err
	}

	ephemeral := []json.RawMessage{}
	if len(receipts) > 0 {
		ev, err := receiptEvent(room.RoomID, receipts, userID, true)
		if err != nil {
			return nil, err
		}
		if ev != nil {
			ephemeral = append(ephemeral, stripRoomID(ev))
		}
	}

	// A room with nothing in it is left out of the response entirely.
	if len(messages) == 0 && len(stateIDs) == 0 && len(adEvents) == 0 && len(ephemeral) == 0 {
		return nil, nil
	}

	stateEventIDs := make([]string, 0, len(stateIDs))
	for _, id := range stateIDs {
		stateEventIDs = append(stateEventIDs, id)
	}
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

	var aggs map[string]aggregation
	var nestedIDs []string
	var nested map[string]store.StateEvent
	if limited {
		vis, err := d.Store.VisibilityExtras(ctx, room.RoomID, userID, nil)
		if err != nil {
			return nil, err
		}
		aggs, nestedIDs, err = bundleAggregations(ctx, d, userID, messages, vis.IgnoredSenders)
		if err != nil {
			return nil, err
		}
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

	prevBatch := now
	if len(messages) > 0 {
		prevBatch = now.WithRoomKey(streamtoken.Live(messages[0].StreamOrdering - 1))
	}

	unread, err := d.Store.UnreadNotifications(ctx, room.RoomID, userID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"timeline": map[string]any{
			"events":     timeline,
			"prev_batch": prevBatch.String(),
			"limited":    limited,
		},
		"state":        map[string]any{"events": stateJSON},
		"account_data": map[string]any{"events": adEvents},
		"ephemeral":    map[string]any{"events": ephemeral},
		"unread_notifications": map[string]any{
			"notification_count": unread.NotifyCount,
			"highlight_count":    unread.HighlightCount,
		},
		"summary":                         map[string]any{},
		"org.matrix.msc2654.unread_count": unread.UnreadCount,
	}, nil
}

// incrementalStateDelta works out the `state` block for an incremental sync.
//
// Synapse's _compute_state_delta_for_incremental_sync. The shortcut at the top
// carries most of the traffic: when the timeline is unlimited AND its events
// form an unbroken chain from where the client left off, the `state` block is
// EMPTY -- everything that changed is in the timeline, and repeating it would
// be waste. Only a gap, or a fork in the DAG, makes a state block necessary.
func incrementalStateDelta(ctx context.Context, d Deps, room store.RoomForUser,
	messages []store.TimelineEvent, limited bool,
	since, now streamtoken.Token) (map[store.StateKey]string, error) {

	if !limited && len(messages) > 0 {
		linear, err := isLinearTimeline(ctx, d, room.RoomID, messages, since.Room)
		if err != nil {
			return nil, err
		}
		if linear {
			return nil, nil
		}
	}
	if !limited && len(messages) == 0 {
		// Nothing happened, so nothing changed.
		return nil, nil
	}

	var start map[store.StateKey]string
	var err error
	if len(messages) > 0 {
		groups, err := d.Store.StateGroupsForEvents(ctx, []string{messages[0].EventID})
		if err != nil {
			return nil, err
		}
		if group, ok := groups[messages[0].EventID]; ok {
			start, err = d.Store.FullStateForGroup(ctx, group)
			if err != nil {
				return nil, err
			}
		}
	}
	if start == nil {
		start, err = d.Store.StateIDsAt(ctx, room.RoomID, now.Room)
		if err != nil {
			return nil, err
		}
	}

	previous, err := d.Store.StateIDsAt(ctx, room.RoomID, since.Room)
	if err != nil {
		return nil, err
	}
	end, err := d.Store.StateIDsAt(ctx, room.RoomID, now.Room)
	if err != nil {
		return nil, err
	}

	return calculateState(start, end, previous, messages), nil
}

// isLinearTimeline reports whether the timeline is an unbroken chain from the
// last event the client saw.
//
// Each event must name exactly its predecessor as its only prev_event. Anything
// else -- a fork, a merge, a gap -- means the client cannot reconstruct the
// room from the timeline alone and needs a state block.
func isLinearTimeline(ctx context.Context, d Deps, roomID string,
	messages []store.TimelineEvent, sinceKey streamtoken.RoomKey) (bool, error) {

	prev, err := d.Store.LastEventBefore(ctx, roomID, sinceKey)
	if err != nil {
		return false, err
	}
	for _, ev := range messages {
		prevEvents := gjson.GetBytes(ev.JSON, "prev_events").Array()
		if len(prevEvents) != 1 || prevEvents[0].String() != prev {
			return false, nil
		}
		prev = ev.EventID
	}
	return true, nil
}

// calculateState is Synapse's _calculate_state.
//
//	(timeline_end | timeline_start) - previous_timeline_end - timeline_contains
//
// `previous_timeline_end` is what makes an incremental sync incremental: state
// the client already had is subtracted, so only what changed is sent.
func calculateState(start, end, previous map[store.StateKey]string,
	messages []store.TimelineEvent) map[store.StateKey]string {

	// The last state event per key in the timeline, not every one: an earlier
	// change to the same key still belongs in the state block.
	timelineContains := map[store.StateKey]string{}
	for _, ev := range messages {
		if ev.IsState {
			timelineContains[store.StateKey{Type: ev.Type, StateKey: ev.StateKey}] = ev.EventID
		}
	}
	excluded := map[string]bool{}
	for _, id := range timelineContains {
		excluded[id] = true
	}
	for _, id := range previous {
		excluded[id] = true
	}

	out := map[store.StateKey]string{}
	for _, m := range []map[store.StateKey]string{start, end} {
		for k, id := range m {
			if excluded[id] || k.Type == "m.room.aliases" {
				continue
			}
			out[k] = id
		}
	}
	return out
}

// userChangesFromRooms works out who joined and who left, from the membership
// events in the response we are about to send.
//
// Synapse derives these from its own built response rather than from a query
// (SyncResultBuilder.calculate_user_changes), so the device-list section cannot
// disagree with the rooms section.
//
// A leave counts only when the previous membership was `join`: a leave that
// follows an invite is a declined invite, and the user was never visible to us
// in the first place.
func userChangesFromRooms(joined map[string]any) (joinedOrInvited []string, leaveCandidates map[string]string) {
	joinedSet := map[string]bool{}
	leaveCandidates = map[string]string{}

	consider := func(raw json.RawMessage, inTimeline bool) {
		if gjson.GetBytes(raw, "type").String() != "m.room.member" {
			return
		}
		user := gjson.GetBytes(raw, "state_key").String()
		if user == "" {
			return
		}
		switch gjson.GetBytes(raw, "content.membership").String() {
		case "join", "invite", "knock":
			joinedSet[user] = true
		default:
			// Whether this counts as "left" depends on the PREVIOUS
			// membership, which is not in our output: `replaces_state` names
			// the event to look it up from.
			// Only from the timeline. Synapse decides this by reading
			// `unsigned.prev_content` off its in-memory event, which
			// events_worker populates for the timeline but leaves on STATE
			// events only when some earlier reader happened to ask for it and
			// polluted the shared cache. Deriving `left` from the state block
			// therefore reproduces a cache artefact, not a rule.
			if !inTimeline {
				return
			}
			if replaces := gjson.GetBytes(raw, "unsigned.replaces_state").String(); replaces != "" {
				leaveCandidates[replaces] = user
				delete(joinedSet, user)
			}
		}
	}

	for _, entry := range joined {
		room, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		// Timeline only for the leave side; see the note on leaveCandidates.
		for _, section := range []string{"state", "timeline"} {
			block, ok := room[section].(map[string]any)
			if !ok {
				continue
			}
			events, ok := block["events"].([]json.RawMessage)
			if !ok {
				continue
			}
			for _, ev := range events {
				consider(ev, section == "timeline")
			}
		}
	}

	for u := range joinedSet {
		joinedOrInvited = append(joinedOrInvited, u)
	}
	// A user who also appears as joined elsewhere in the response has not left.
	for replaces, user := range leaveCandidates {
		if joinedSet[user] {
			delete(leaveCandidates, replaces)
		}
	}
	return joinedOrInvited, leaveCandidates
}

// resolveLeftUsers keeps the leave events whose previous membership was `join`.
//
// A leave that follows an invite is a declined invite: the user was never
// visible to us, so they have not "left" anything.
func resolveLeftUsers(ctx context.Context, d Deps, candidates map[string]string) ([]string, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(candidates))
	for id := range candidates {
		ids = append(ids, id)
	}
	previous, err := d.Store.MembershipOfEvents(ctx, ids)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	for replaces, user := range candidates {
		if previous[replaces] != "join" || seen[user] {
			continue
		}
		seen[user] = true
		out = append(out, user)
	}
	return out, nil
}
