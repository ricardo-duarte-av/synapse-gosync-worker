// Package handlers implements the client API endpoints.
package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/auth"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/clientevent"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/matrixerr"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/server"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/streamtoken"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/visibility"
)

// Deps are what every handler needs.
type Deps struct {
	Store *store.Store
	Auth  *auth.Authenticator
	Log   zerolog.Logger
	// AllowPinNow enables ?_gosync_now=<token>. See docs/comparability.md.
	AllowPinNow bool
}

// defaultPaginationLimit is Synapse's PaginationConfig default for the legacy
// initialSync endpoints.
const defaultPaginationLimit = 10

// RoomInitialSync serves /rooms/{roomId}/initialSync.
//
// Mirrors synapse/handlers/initial_sync.py room_initial_sync ->
// _room_initial_sync_joined.
func RoomInitialSync(d Deps) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ann := server.Annotate(r.Context())
		if ann != nil {
			ann.Endpoint = "room_initial_sync"
		}

		verdict, ok := authenticate(w, r, d, ann)
		if !ok {
			return
		}

		roomID := r.PathValue("roomId")
		limit := intParam(r, "limit", defaultPaginationLimit)

		body, status, mxErr := roomInitialSync(r, d, verdict, roomID, limit)
		if mxErr != nil {
			refuse(w, ann, status, *mxErr)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
}

func roomInitialSync(r *http.Request, d Deps, verdict auth.Verdict, roomID string, limit int) (
	[]byte, int, *matrixerr.Error) {

	ctx := r.Context()
	ann := server.Annotate(ctx)

	info, err := d.Store.RoomInfo(ctx, roomID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, http.StatusNotFound, &matrixerr.Error{
			ErrCode: matrixerr.CodeNotFound, Error: "Room not found"}
	}
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "room info", err)
	}
	// Synapse checks this before anything else and answers 403.
	if info.Blocked {
		return nil, http.StatusForbidden, &matrixerr.Error{
			ErrCode: matrixerr.CodeForbidden, Error: "This room has been blocked on this server"}
	}

	membership, err := d.Store.CurrentMembership(ctx, roomID, verdict.UserID)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "membership", err)
	}
	switch membership.Membership {
	case "join":
		// The path implemented below.
	case "leave":
		// _room_initial_sync_parted resolves the room state *at the leave
		// event*, which needs state-group resolution this worker does not have
		// yet. Refusing is correct: a snapshot built from current state would
		// show a departed user everything that happened after they left.
		return nil, http.StatusNotImplemented, &matrixerr.Error{
			ErrCode: matrixerr.CodeUnknown,
			Error:   "Snapshots of a room the user has left are not implemented yet"}
	default:
		return nil, http.StatusForbidden, &matrixerr.Error{
			ErrCode: matrixerr.CodeForbidden, Error: "You are not in the room."}
	}

	now, pinned, mxErr := nowToken(r, d)
	if mxErr != nil {
		return nil, http.StatusBadRequest, mxErr
	}
	if ann != nil {
		ann.NextBatch = now.String()
	}

	timeNow, mxErr := nowMillis(r, d)
	if mxErr != nil {
		return nil, http.StatusBadRequest, mxErr
	}

	state, err := d.Store.CurrentState(ctx, roomID, info.RoomVersion)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "current state", err)
	}

	messages, start, err := d.Store.RecentEvents(ctx, roomID, info.RoomVersion, limit, now.Room)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "recent events", err)
	}

	// Visibility before serialisation: nothing that fails this check should be
	// rendered at all, let alone written to a response buffer.
	senders := make([]string, 0, len(messages))
	seen := map[string]bool{}
	for _, ev := range messages {
		if !seen[ev.Sender] {
			seen[ev.Sender] = true
			senders = append(senders, ev.Sender)
		}
	}
	vis, err := d.Store.VisibilityContext(ctx, roomID, verdict.UserID, senders)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "visibility context", err)
	}
	// is_peeking is false: the user is joined, checked above.
	if err := vis.Check(false); err != nil {
		d.Log.Warn().Err(err).Str("room_id", roomID).
			Msg("refusing to serve a room that needs per-event state resolution")
		return nil, http.StatusNotImplemented, &matrixerr.Error{
			ErrCode: matrixerr.CodeUnknown,
			Error:   "This room needs per-event state resolution, which is not implemented yet"}
	}
	visible := messages[:0]
	for _, ev := range messages {
		if vis.Visible(visibility.Event{Type: ev.Type, Sender: ev.Sender, IsState: ev.IsState}) {
			visible = append(visible, ev)
		}
	}
	messages = visible

	// Synapse attaches prev_content in the storage layer, before serialisation,
	// so the v1 format lifts it to the top level along with the other copy keys.
	//
	// TIMELINE ONLY. get_recent_events_for_room passes get_prev_content=True;
	// get_current_state does not. Synapse nevertheless emits prev_content on
	// *some* state events, because events_worker writes the field into the
	// shared cached event ("This mutates the cached event, but that's fine")
	// and a later reader of that cache picks it up. Whether it appears depends
	// on whether some other request happened to load that event first, which
	// is not reproducible and not ours to imitate. syncdiff tolerates it
	// upstream-only for exactly this reason.
	prevTargets := make([]*clientevent.Stored, 0, len(messages))
	for i := range messages {
		prevTargets = append(prevTargets, &messages[i].Stored)
	}
	if err := d.Store.AttachPrevContent(ctx, prevTargets); err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "prev content", err)
	}

	requester := clientevent.Requester{
		UserID:   verdict.UserID,
		DeviceID: verdict.DeviceID,
		IsGuest:  verdict.IsGuest,
	}
	// Only needed for events stored without a device_id, which are rare, so the
	// lookup is done once per request rather than never.
	if tokenID, err := d.Store.AccessTokenID(ctx, auth.ExtractToken(r)); err == nil {
		requester.TokenID = tokenID
	}

	cfg := clientevent.Config{Format: clientevent.FormatV1, Requester: requester}

	// Synapse wraps state events with FilteredEvent.state(), whose membership is
	// None, so the state block carries no unsigned.membership. Timeline events
	// do carry it.
	stateJSON := make([]json.RawMessage, 0, len(state))
	for _, ev := range state {
		b, err := clientevent.Serialize(ev.Stored, timeNow, cfg)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "serialise state", err)
		}
		stateJSON = append(stateJSON, b)
	}

	// MSC4115's unsigned.membership is the caller's membership *at each event*,
	// not their membership now. In a room joined part way through its history,
	// Synapse reports "leave" for everything before the join.
	memberships, err := d.Store.UserMembershipTimeline(ctx, roomID, verdict.UserID, 0)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "membership timeline", err)
	}

	chunk := make([]json.RawMessage, 0, len(messages))
	for _, ev := range messages {
		stored := ev.Stored
		stored.Membership = store.MembershipAt(memberships, ev.StreamOrdering)
		b, err := clientevent.Serialize(stored, timeNow, cfg)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "serialise timeline", err)
		}
		chunk = append(chunk, b)
	}

	receipts, err := roomReceipts(ctx, d, roomID, verdict.UserID, now.Receipt)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "receipts", err)
	}

	accountData, err := roomAccountData(ctx, d, verdict.UserID, roomID)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "account data", err)
	}

	presence, err := roomPresence(ctx, d, state, timeNow)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "presence", err)
	}

	resp := map[string]any{
		"room_id":    roomID,
		"membership": membership.Membership,
		"messages": map[string]any{
			"chunk": chunk,
			// The room key moves; the other thirteen streams stay where the
			// now token put them.
			"start": now.WithRoomKey(start).String(),
			"end":   now.String(),
		},
		"state":        stateJSON,
		"presence":     presence,
		"receipts":     receipts,
		"account_data": accountData,
	}
	body, err := json.Marshal(resp)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "encode response", err)
	}
	if ann != nil && pinned {
		ann.Reason = "pinned_now"
	}
	return body, http.StatusOK, nil
}

// nowToken returns the token bounding this response.
//
// Normally derived from the database, which is approximate (see
// docs/tokens.md). The comparator supplies an exact one via ?_gosync_now=,
// which is the only way two implementations can be asked the same question.
func nowToken(r *http.Request, d Deps) (streamtoken.Token, bool, *matrixerr.Error) {
	if pin := r.URL.Query().Get("_gosync_now"); pin != "" {
		if !d.AllowPinNow {
			return streamtoken.Token{}, false, &matrixerr.Error{
				ErrCode: matrixerr.CodeUnrecognized,
				Error:   "_gosync_now is not enabled on this worker"}
		}
		tok, err := streamtoken.Parse(pin)
		if err != nil {
			return streamtoken.Token{}, false, &matrixerr.Error{
				ErrCode: matrixerr.CodeInvalidParam, Error: err.Error()}
		}
		return tok, true, nil
	}
	tok, err := d.Store.CurrentToken(r.Context())
	if err != nil {
		return streamtoken.Token{}, false, &matrixerr.Error{
			ErrCode: matrixerr.CodeUnknown, Error: "Internal server error"}
	}
	return tok, false, nil
}

// nowMillis returns the wall clock this response is computed against.
//
// Pinnable for the same reason the stream position is. Synapse stamps
// `age = time_now - age_ts` on every event and `last_active_ago` on every
// presence entry, so two implementations asked a second apart disagree on
// every event in the response -- for no reason but the passage of time.
// Pinning the clock turns `age` from a field the comparator must ignore into
// one it can check, and `age` is worth checking: getting it wrong is invisible
// in casual testing.
//
// Gated behind the same testing.allow_pin_now as the stream position: a worker
// that lets a caller choose what time it is has no business serving anyone.
func nowMillis(r *http.Request, d Deps) (int64, *matrixerr.Error) {
	if pin := r.URL.Query().Get("_gosync_time_now"); pin != "" {
		if !d.AllowPinNow {
			return 0, &matrixerr.Error{
				ErrCode: matrixerr.CodeUnrecognized,
				Error:   "_gosync_time_now is not enabled on this worker"}
		}
		ms, err := strconv.ParseInt(pin, 10, 64)
		if err != nil {
			return 0, &matrixerr.Error{
				ErrCode: matrixerr.CodeInvalidParam, Error: "_gosync_time_now must be milliseconds"}
		}
		return ms, nil
	}
	return time.Now().UnixMilli(), nil
}

func intParam(r *http.Request, name string, def int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return def
	}
	return n
}
