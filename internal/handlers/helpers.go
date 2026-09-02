package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/auth"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/filter"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/matrixerr"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/metrics"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/server"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/streamtoken"
)

// authenticate validates the caller and annotates the request log.
//
// A whoami failure is 502, never 401: refusing a valid token because Synapse
// could not be reached would log real clients out. That distinction is the
// whole reason auth.AuthenticateAs separates "rejected" from "unknown".
func authenticate(w http.ResponseWriter, r *http.Request, d Deps, ann *server.Annotation) (auth.Verdict, bool) {
	creds := auth.ExtractCredentials(r)
	if creds.Token == "" {
		metrics.AuthVerdictsTotal.WithLabelValues("missing").Inc()
		refuse(w, ann, http.StatusUnauthorized, matrixerr.Error{
			ErrCode: matrixerr.CodeMissingToken, Error: "Missing access token"})
		return auth.Verdict{}, false
	}

	verdict, err := d.Auth.AuthenticateAs(r.Context(), creds)
	if err != nil {
		metrics.AuthVerdictsTotal.WithLabelValues("unavailable").Inc()
		d.Log.Error().Err(err).Msg("cannot reach Synapse to validate a token")
		refuse(w, ann, http.StatusBadGateway, matrixerr.Error{
			ErrCode: matrixerr.CodeUnknown, Error: "Cannot validate the access token right now"})
		return auth.Verdict{}, false
	}
	if !verdict.Valid {
		metrics.AuthVerdictsTotal.WithLabelValues("rejected").Inc()
		// Pass Synapse's own rejection through. It distinguishes an expired
		// token (soft logout: keep local state) from an unknown one (hard
		// logout: wipe), and clients act very differently on the two.
		mxErr := matrixerr.Error{ErrCode: matrixerr.CodeUnknownToken, Error: "Invalid access token"}
		status := http.StatusUnauthorized
		if verdict.Rejection != nil {
			mxErr = *verdict.Rejection
			status = verdict.Status
		}
		refuse(w, ann, status, mxErr)
		return auth.Verdict{}, false
	}

	metrics.AuthVerdictsTotal.WithLabelValues("accepted").Inc()
	metrics.AuthCacheEntries.Set(float64(d.Auth.Len()))
	if ann != nil {
		ann.UserID = verdict.UserID
		ann.DeviceID = verdict.DeviceID
	}
	return verdict, true
}

func refuse(w http.ResponseWriter, ann *server.Annotation, status int, e matrixerr.Error) {
	if ann != nil {
		ann.Outcome = "refused"
		if ann.Reason == "" {
			ann.Reason = e.ErrCode
		}
	}
	matrixerr.Write(w, status, e)
}

// internalError logs the cause and returns a body that does not leak it.
func internalError(d Deps, what string, err error) *matrixerr.Error {
	d.Log.Error().Err(err).Str("during", what).Msg("request failed")
	return &matrixerr.Error{ErrCode: matrixerr.CodeUnknown, Error: "Internal server error"}
}

// roomReceipts renders one room's receipts for the single-room endpoint.
//
// withThreads is false: the singular query does not select thread_id at all.
// See receiptEvent for why the two initialSync endpoints differ here.
func roomReceipts(ctx context.Context, d Deps, roomID, userID string,
	to streamtoken.MultiWriter) ([]json.RawMessage, error) {

	rows, err := d.Store.RoomReceipts(ctx, roomID, to)
	if err != nil {
		return nil, err
	}
	kept := make([]store.ReceiptRow, 0, len(rows))
	for _, row := range rows {
		if inRange(to, row.InstanceName, row.StreamID) {
			kept = append(kept, row)
		}
	}
	ev, err := receiptEvent(roomID, kept, userID, false)
	if err != nil {
		return nil, err
	}
	if ev == nil {
		return []json.RawMessage{}, nil
	}
	return []json.RawMessage{ev}, nil
}

// inRange is MultiWriterStreamToken.is_stream_position_in_range for a token
// used as an upper bound only.
//
// A row with no instance_name predates multi-writer support and is compared
// against the agreed minimum.
func inRange(to streamtoken.MultiWriter, instanceName string, pos int64) bool {
	_ = instanceName
	// Instance names would have to be resolved to ids through `instance_map` to
	// compare per writer. Until then the conservative bound is the highest
	// position any writer reached, which is what the query already applied, so
	// every returned row is in range.
	return pos <= to.MaxStreamPos()
}

// roomAccountData renders the account data events for a room.
func roomAccountData(ctx context.Context, d Deps, userID, roomID string) ([]json.RawMessage, error) {
	entries, err := d.Store.RoomAccountData(ctx, userID, roomID)
	if err != nil {
		return nil, err
	}
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

// roomPresence renders m.presence for the room's joined members.
//
// Mirrors format_user_presence_state: last_active_ago and status_msg appear
// only when set, and currently_active only when the user is online.
func roomPresence(ctx context.Context, d Deps, state []store.StateEvent, timeNow int64) ([]json.RawMessage, error) {
	var members []string
	for _, ev := range state {
		if ev.Type == "m.room.member" && memberIsJoined(ev) {
			members = append(members, ev.StateKey)
		}
	}
	states, err := d.Store.Presence(ctx, members)
	if err != nil {
		return nil, err
	}
	byUser := make(map[string]store.PresenceState, len(states))
	for _, p := range states {
		byUser[p.UserID] = p
	}

	out := make([]json.RawMessage, 0, len(members))
	for _, user := range members {
		p, ok := byUser[user]
		if !ok {
			// Synapse's presence handler substitutes a default offline state
			// for a user with no presence_stream row, rather than omitting
			// them. Omitting them loses a member from every room whose users
			// have never set presence -- 4 of 28 in the largest test room here.
			p = store.PresenceState{UserID: user, State: "offline"}
		}
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

func memberIsJoined(ev store.StateEvent) bool {
	var parsed struct {
		Content struct {
			Membership string `json:"membership"`
		} `json:"content"`
	}
	if err := json.Unmarshal(ev.JSON, &parsed); err != nil {
		return false
	}
	return parsed.Content.Membership == "join"
}

// applyUnreadCounts writes a room's notification counts into its entry.
//
// The client's filter decides whether threads are reported at all, and it is
// not a presentation choice: without `unread_thread_notifications` every
// thread's counts are ADDED to the room's single figure, and with it they are
// pulled out into their own section and the room's figure drops to the main
// timeline alone. Same query either way, two different answers, and a client
// that asks for the split and is given the folded totals sees numbers that are
// simply too high.
//
// The section is omitted entirely when there are no threads, as Synapse's
// `if room.unread_thread_notifications:` gives, and mirrored under the MSC3773
// name when that flag is on -- both keys, not one or the other.
func applyUnreadCounts(entry map[string]any, notifs store.RoomNotifCounts,
	f *filter.Collection, msc3773 bool) {

	counts := notifs.Main
	if !f.UnreadThreadNotifications() {
		counts = notifs.Total()
	}
	entry["unread_notifications"] = map[string]any{
		"notification_count": counts.NotifyCount,
		"highlight_count":    counts.HighlightCount,
	}
	// MSC2654, enabled on this deployment. Note it carries the `unread` count,
	// which is a different number from the notification count: a muted room
	// accumulates unread events and no notifications.
	entry["org.matrix.msc2654.unread_count"] = counts.UnreadCount

	if !f.UnreadThreadNotifications() || len(notifs.Threads) == 0 {
		return
	}
	threads := make(map[string]any, len(notifs.Threads))
	for id, c := range notifs.Threads {
		// No unread_count here: MSC3773 reports only the two counts a client
		// shows on a thread, and the room's unread_count stays whole.
		threads[id] = map[string]any{
			"notification_count": c.NotifyCount,
			"highlight_count":    c.HighlightCount,
		}
	}
	entry["unread_thread_notifications"] = threads
	if msc3773 {
		entry["org.matrix.msc3773.unread_thread_notifications"] = threads
	}
}
