package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/auth"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/clientevent"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/filter"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/matrixerr"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/metrics"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/pushrules"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/server"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/streamtoken"
)

// Sync serves /sync.
//
// The client's filter decides most of what follows -- the timeline limit, which
// rooms and event types appear, and whether member events are lazy-loaded -- so
// it is resolved once here and threaded through both the initial and the
// incremental path.
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
		)
		// MSC4222 is opt-in per request, and only offered when enabled, exactly
		// as Synapse gates it.
		useStateAfter := d.MSC4222Enabled &&
			r.URL.Query().Get("org.matrix.msc4222.use_state_after") == "true"

		f, mxErr := resolveFilter(r.Context(), d, verdict.UserID, filterQueryParam(r))
		if mxErr != nil {
			refuse(w, ann, http.StatusBadRequest, *mxErr)
			return
		}

		if since := r.URL.Query().Get("since"); since != "" {
			if ann != nil {
				ann.Since = since
			}
			// Synapse deletes acknowledged to-device messages once per request,
			// before it starts waiting -- not once per pass round the long-poll
			// loop, and not per room. Doing it here keeps that shape.
			if tok, err := streamtoken.Parse(since); err == nil {
				deleteAcknowledgedToDevice(r.Context(), d, verdict, tok)
			}
			body, status, mxErr = longPoll(r, d, verdict, since, useStateAfter, f, ann)
		} else {
			body, status, mxErr = initialSyncV2(r, d, verdict, useStateAfter, f)
		}
		if mxErr != nil {
			refuse(w, ann, status, *mxErr)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
}

func initialSyncV2(r *http.Request, d Deps, verdict auth.Verdict, useStateAfter bool,
	f *filter.Collection) ([]byte, int, *matrixerr.Error) {
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
	// Before next_batch is recorded anywhere: a truncated to-device backlog
	// winds the token back, and the annotation must show what the client will
	// actually be handed.
	toDevice, err := toDeviceEvents(ctx, d, verdict, 0, &now)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "to-device", err)
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
	cfg := clientevent.Config{
		Format:         eventFormat(f, clientevent.FormatV2NoRoomID),
		Requester:      requester,
		MSC4354Enabled: d.MSC4354Enabled,
		EventFields:    f.EventFields,
	}
	// The invite section carries stripped room state, which every other
	// section must not.
	strippedCfg := cfg
	strippedCfg.IncludeStrippedRoomState = true

	// A filter that can match no room account data at all is answered without
	// the query, as Synapse does: there is nothing for the query to return.
	accountDataByRoom := map[string][]store.AccountDataEntry{}
	if !f.BlocksAllRooms() && !f.BlocksAllRoomAccountData() {
		accountDataByRoom, err = d.Store.AllRoomAccountData(ctx, verdict.UserID, d.MSC3391Enabled)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "room account data", err)
		}
	}

	// Invites from ignored users are dropped, as Synapse's
	// _get_room_changes_for_initial_sync does. Being shown the invitations of
	// somebody you have ignored is the one case where ignoring plainly does
	// not work -- and on a real account it is not rare: four invites from 2025
	// reappeared the first time a 500-room account synced here.
	ignored, err := d.Store.IgnoredUsers(ctx, verdict.UserID)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "ignored users", err)
	}

	joinedIDs := make([]string, 0, len(rooms))
	for _, room := range rooms {
		if room.Membership == "join" {
			joinedIDs = append(joinedIDs, room.RoomID)
		}
	}
	// Before any room entry is built, because it moves the now token and every
	// prev_batch in the response carries that token. An initial sync asks from
	// position 0: the client has seen nothing, so every unexpired sticky event
	// is news.
	sticky, err := stickyByRoom(ctx, d, joinedIDs, 0, &now, timeNow)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "sticky events", err)
	}

	// From position 0, which is what Synapse's typing source is given when
	// there is no `since`.
	typingRooms := map[string]bool{}
	if d.Replication != nil {
		for _, id := range d.Replication.TypingChangedSince(0) {
			typingRooms[id] = true
		}
	}
	if ann != nil {
		ann.NextBatch = now.String()
	}

	joinedRooms := map[string]any{}
	invitedRooms := map[string]any{}

	// Invites first, and sequentially: there are few of them and each is one
	// event, so there is nothing to parallelise.
	var toBuild []store.RoomForUser
	for _, room := range rooms {
		// A filter naming no rooms drops the whole section, which is cheaper
		// than building it and discarding every entry.
		if f.BlocksAllRooms() {
			break
		}
		if room.Membership == "invite" {
			if ignored[room.Sender] {
				continue
			}
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
		toBuild = append(toBuild, room)
	}

	// Rooms are built ten at a time, which is what Synapse does --
	// `concurrently_execute(handle_room_entries, room_entries, 10)`,
	// handlers/sync.py:2700 -- and not a liberty taken for speed.
	//
	// It is the difference between a usable initial sync and an unusable one at
	// real scale. Each room costs a handful of sequential round trips to a
	// database holding a 17GB state table, and 500 of them one after another
	// took 193 seconds on a live account. The response is a map keyed by room
	// id, so the order rooms complete in does not reach the client.
	group, gctx := errgroup.WithContext(ctx)
	group.SetLimit(roomEntryConcurrency)
	var mu sync.Mutex
	for _, room := range toBuild {
		group.Go(func() error {
			entry, err := syncRoomEntry(gctx, d, room, verdict.UserID, now.Room, timeNow, cfg,
				accountDataByRoom[room.RoomID], now, useStateAfter, f, verdict.DeviceID, true,
				timelineSource{upto: now}, sticky[room.RoomID], typingRooms[room.RoomID])
			if err != nil {
				return err
			}
			mu.Lock()
			joinedRooms[room.RoomID] = entry
			mu.Unlock()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "room entry", err)
	}

	resp := map[string]any{"next_batch": now.String()}

	// Absent rather than empty when there is nothing, as Synapse's
	// `if sync_result.to_device:` gives.
	if len(toDevice) > 0 {
		resp["to_device"] = map[string]any{"events": toDevice}
	}

	if !f.BlocksAllGlobalAccountData() {
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

		// The filter is applied to the whole section, m.push_rules included:
		// Synapse builds the dict first and filters it afterwards, so a client
		// asking only for `m.direct` does not get its push rules as well.
		events = filterAccountDataEvents(f, "", events)

		if len(events) > 0 {
			resp["account_data"] = map[string]any{"events": events}
		}
	}

	var presenceStates []store.PresenceState
	if !f.BlocksAllPresence() {
		presenceStates, err = d.Store.SharedRoomPresence(ctx, verdict.UserID)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "presence", err)
		}
		presenceStates = filterPresence(f, presenceStates)
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

// timelineSource describes where a room's timeline is to come from.
//
// Both callers of syncRoomEntry give the room a full-state entry, but they do
// not build its timeline the same way, and Synapse does not either. An initial
// sync has no window and no `since`, so everything is paginated backwards from
// the now token. A room joined inside an incremental sync's window HAS a
// window: Synapse loads it like any other joined room and only then treats it
// as newly joined, which leaves the room's `upto_token` — and so the prev_batch
// of an untrimmed timeline — at the start of that chunk rather than at the now
// token.
type timelineSource struct {
	// upto is Synapse's upto_token: where a client paginates from when the
	// timeline was not trimmed.
	upto streamtoken.Token
	// potential is the window already loaded, with hasPotential separating
	// "loaded, and there were none" from "not loaded at all".
	potential    []store.TimelineEvent
	hasPotential bool
	// newlyJoined makes the timeline limited whatever the pagination says: the
	// client has none of this room's history.
	newlyJoined bool
}

// syncRoomEntry builds one joined room's full-state section.
func syncRoomEntry(ctx context.Context, d Deps, room store.RoomForUser, userID string,
	endKey streamtoken.RoomKey, timeNow int64, cfg clientevent.Config,
	accountData []store.AccountDataEntry, now streamtoken.Token,
	useStateAfter bool, f *filter.Collection, deviceID string, initial bool,
	src timelineSource, stickyIDs []string, typingChanged bool) (map[string]any, error) {

	// No `since` in either case: a newly joined room is paginated as history,
	// exactly as an initial sync is, because the client has none of it.
	messages, memberships, prevBatch, limited, err := loadFilteredRecents(ctx, d, room, userID,
		now, src.upto, nil, src.potential, src.hasPotential, src.newlyJoined, timeNow, f)
	if err != nil {
		return nil, err
	}

	// Lazy loading restricts the state block to the memberships this timeline
	// actually needs. Our own membership is always included on a full-state
	// sync: without it a client cannot tell whether it is still in the room,
	// and Element got this wrong for exactly that reason (riot-web#7209).
	var stateMembers map[string]bool
	if f.LazyLoadMembers() {
		stateMembers = timelineMembers(messages, useStateAfter)
		stateMembers[userID] = true
	}

	// The `state` block is what the client needs to interpret the timeline that
	// follows it, so it is the state at the START of the timeline, minus
	// anything the timeline itself carries. See _calculate_state.
	// MSC4222 reports the state AFTER the timeline rather than before it, which
	// is simply the state at the end token -- no union with the timeline start,
	// and nothing subtracted, because the client is being told where the room
	// ended up rather than what it must apply first.
	var stateIDs map[store.StateKey]string
	if useStateAfter {
		if stateMembers != nil {
			stateIDs, err = d.Store.LazyStateIDsAt(ctx, room.RoomID, endKey,
				memberList(stateMembers))
		} else {
			stateIDs, err = d.Store.StateIDsAt(ctx, room.RoomID, endKey)
		}
		if err != nil {
			return nil, err
		}
		restrictToMembers(stateIDs, stateMembers)
		dropAliases(stateIDs)
	} else {
		stateIDs, err = syncStateBlock(ctx, d, room, messages, endKey, stateMembers)
		if err != nil {
			return nil, err
		}
	}
	dedupeLazyMembers(d, f, userID, deviceID, stateIDs, messages, initial)

	// The summary is computed BEFORE the state block is loaded, because it can
	// add to it: a hero whose membership the client has never been sent is
	// added here, and would otherwise be named in the summary with no profile
	// to render.
	summary := map[string]any{}
	var summaryValue any = summary
	if wantSummary(f, messages, stateIDs, limited, initial) {
		computed, err := roomSummary(ctx, d, room.RoomID, userID, deviceID, f, stateIDs,
			messages, endKey)
		if err != nil {
			return nil, err
		}
		// A room with no events at all reports a null summary, not an empty
		// one: Synapse assigns compute_summary's None straight into the
		// response.
		if computed == nil {
			summaryValue = nil
		} else {
			summaryValue = computed
		}
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
	stateEventIDs = filterStateBlock(f, room.RoomID, stateEventIDs, stateEvents)

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

	adEvents, err := accountDataEvents(filterAccountDataEntries(f, room.RoomID, accountData))
	if err != nil {
		return nil, err
	}

	ephemeral := []json.RawMessage{}
	if !f.BlocksAllRoomEphemeral() {
		receiptsByRoom, err := d.Store.MultiRoomReceipts(ctx, []string{room.RoomID}, now.Receipt.MaxStreamPos())
		if err != nil {
			return nil, err
		}
		receiptRows := receiptsByRoom[room.RoomID]
		// withThreads: /sync uses the multi-room receipt path, which selects
		// thread_id and applies MSC4102 -- unlike /rooms/{id}/initialSync.
		if ev, err := receiptEvent(room.RoomID, receiptRows, userID, true); err != nil {
			return nil, err
		} else if ev != nil {
			ephemeral = append(ephemeral, ev)
		}

		// Synapse asks the typing source from position 0 on an initial sync,
		// which yields every room whose serial it knows -- and nothing for
		// rooms that have never had a typist. Asking unconditionally instead
		// would attach an empty m.typing to all 654 rooms of a large account.
		if typingChanged {
			if ev, err := typingEvent(d, room.RoomID); err != nil {
				return nil, err
			} else if ev != nil {
				ephemeral = append(ephemeral, ev)
			}
		}
		// The filter sees the room_id, which is stripped only afterwards:
		// Synapse filters the dict it built and removes the key on the way out.
		ephemeral = stripRoomIDs(filterEphemeral(f, room.RoomID, ephemeral))
	}

	unread, err := d.Store.UnreadNotifications(ctx, room.RoomID, userID)
	if err != nil {
		return nil, err
	}

	entry := map[string]any{
		"timeline": map[string]any{
			"events":     timeline,
			"prev_batch": prevBatch.String(),
			"limited":    limited,
		},
		stateKeyName(useStateAfter): map[string]any{"events": stateJSON},
		"account_data":              map[string]any{"events": adEvents},
		"ephemeral":                 map[string]any{"events": ephemeral},
		// A summary is computed only when the filter enables lazy-loading of
		// members, because that is when a client lacks the memberships to name
		// the room itself. Otherwise the key is present and empty -- not
		// populated, and not missing.
		"summary": summaryValue,
	}
	applyUnreadCounts(entry, unread, f, d.MSC3773Enabled)
	stickyBlock, err := stickySection(ctx, d, room, userID, stickyIDs, messages, timeNow, cfg)
	if err != nil {
		return nil, err
	}
	if stickyBlock != nil {
		entry["msc4354_sticky"] = stickyBlock
	}
	return entry, nil
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
	messages []store.TimelineEvent, endKey streamtoken.RoomKey,
	members map[string]bool) (map[store.StateKey]string, error) {

	// A lazy-loading client keeps every non-member state event and the members
	// its timeline mentions, so that is what is asked for. Resolving the whole
	// state map and then discarding it is the same answer and a different
	// order of cost: 500 rooms that way took 209 seconds and 1.5GB on a real
	// account, because a large public room's state is six figures of members
	// and every one of them was fetched to be thrown away.
	//
	// StateFilter.from_lazy_load_member_list is the same idea upstream.
	wanted := memberList(members)

	// The state at the END of the timeline -- which is the now token, NOT the
	// room's current state. They differ whenever the room changed between the
	// token being minted and this query running, which in a busy room is most
	// of the time, and the response would then describe state the client's
	// token does not cover.
	var (
		end map[store.StateKey]string
		err error
	)
	if members != nil {
		end, err = d.Store.LazyStateIDsAt(ctx, room.RoomID, endKey, wanted)
	} else {
		end, err = d.Store.StateIDsAt(ctx, room.RoomID, endKey)
	}
	if err != nil {
		return nil, err
	}
	restrictToMembers(end, members)

	start := end
	if len(messages) > 0 {
		groups, err := d.Store.StateGroupsForEvents(ctx, []string{messages[0].EventID})
		if err != nil {
			return nil, err
		}
		if group, ok := groups[messages[0].EventID]; ok {
			// Strictly this is the state *after* the first timeline event
			// rather than before it, which is what Synapse uses too.
			if members != nil {
				start, err = d.Store.LazyStateForGroup(ctx, group, wanted)
			} else {
				start, err = d.Store.FullStateForGroup(ctx, group)
			}
			if err != nil {
				return nil, err
			}
			restrictToMembers(start, members)
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

// stateKeyName is the response field the state block goes under.
//
// MSC4222 deliberately uses a different key rather than changing what `state`
// means: a client that did not opt in must not silently start receiving state
// with the opposite meaning.
func stateKeyName(useStateAfter bool) string {
	if useStateAfter {
		return "org.matrix.msc4222.state_after"
	}
	return "state"
}

// dropAliases removes every m.room.aliases entry from a state map.
//
// Their state key is the server name, not the empty string, so there is one per
// server that ever set an alias -- deleting a single key misses them all but
// the one lucky match. Synapse refuses to carry the type in any state block
// until MSC2261 lands.
func dropAliases(state map[store.StateKey]string) {
	for k := range state {
		if k.Type == "m.room.aliases" {
			delete(state, k)
		}
	}
}

// longPoll answers an incremental sync, waiting for something to happen if
// there is nothing yet.
//
// The order here is the whole point, and it is the thing Synapse's notifier
// comments dwell on: the waiter REGISTERS ITS INTEREST BEFORE computing an
// answer. An event that lands while the answer is being computed then still
// wakes it. Registering afterwards loses everything that arrives in the gap,
// and the client hangs for its full timeout on news that had already come.
func longPoll(r *http.Request, d Deps, verdict auth.Verdict, since string,
	useStateAfter bool, f *filter.Collection, ann *server.Annotation) ([]byte, int, *matrixerr.Error) {

	timeout := time.Duration(intParam(r, "timeout", 0)) * time.Millisecond
	if ann != nil {
		ann.Timeout = intParam(r, "timeout", 0)
	}
	// A pinned request is a comparison, not a client: waiting would only make
	// the comparator slow, and the window is fixed anyway.
	if timeout <= 0 || r.URL.Query().Get("_gosync_now") != "" || d.Notifier == nil {
		return incrementalSync(r, d, verdict, since, useStateAfter, f)
	}
	if timeout > maxSyncTimeout {
		timeout = maxSyncTimeout
	}

	ctx := r.Context()
	deadline := time.Now().Add(timeout)
	started := time.Now()

	// Counted for as long as the client is parked here, which on a worker
	// serving real clients is almost all of the time.
	metrics.SyncWaiters.Inc()
	defer metrics.SyncWaiters.Dec()

	// The rooms this caller is in, so a wakeup elsewhere on the server does not
	// wake them. Recomputed on each pass, because joining a room mid-poll
	// should start mattering.
	for {
		rooms, err := d.Store.RoomsForUser(ctx, verdict.UserID, []string{"invite", "join"})
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "rooms for user", err)
		}
		roomIDs := make([]string, 0, len(rooms))
		for _, room := range rooms {
			roomIDs = append(roomIDs, room.RoomID)
		}

		handle := d.Notifier.Register(roomIDs, []string{verdict.UserID})

		body, status, mxErr := incrementalSync(r, d, verdict, since, useStateAfter, f)
		if mxErr != nil || !isEmptySync(body) {
			handle.Close()
			if ann != nil {
				ann.Waited = time.Since(started)
			}
			return body, status, mxErr
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			handle.Close()
			if ann != nil {
				ann.Waited = time.Since(started)
			}
			return body, status, mxErr
		}
		woken := handle.Wait(ctx, remaining)
		handle.Close()
		if !woken {
			if ann != nil {
				ann.Waited = time.Since(started)
			}
			return body, status, mxErr
		}
		if ctx.Err() != nil {
			// The client hung up. Ordinary for a long poll, and not a failure.
			if ann != nil {
				ann.Outcome = "client_gone"
			}
			return body, status, mxErr
		}
	}
}

// roomEntryConcurrency is how many rooms are built at once.
//
// Ten, because that is Synapse's number: `concurrently_execute(...,
// room_entries, 10)`. Matching it matters for more than speed -- the
// lazy-loaded member cache is written as rooms are built, so how many run
// concurrently changes which members a sync considers already sent.
const roomEntryConcurrency = 10

// maxSyncTimeout caps how long a client may ask to wait.
//
// Without a cap a client can pin a connection and a goroutine indefinitely, and
// the server's own idle timeouts are a blunter instrument than refusing.
const maxSyncTimeout = 5 * time.Minute

// isEmptySync reports whether a sync response carries nothing worth returning.
//
// Synapse asks the same question of its SyncResult before deciding to keep
// waiting. The device key counts and next_batch are always present and never
// count as content: returning on those alone would turn every long poll into a
// busy loop.
func isEmptySync(body []byte) bool {
	for _, section := range []string{
		"rooms", "presence", "account_data", "to_device", "device_lists",
	} {
		if gjson.GetBytes(body, section).Exists() {
			return false
		}
	}
	return true
}
