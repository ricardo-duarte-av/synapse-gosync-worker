package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/auth"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/matrixerr"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/metrics"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/server"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/slidingstore"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/slidingsync"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/streamtoken"
)

// SlidingSync serves MSC4186, Simplified Sliding Sync.
//
// Registered on two paths: the unstable one every client uses today, and the
// stable `/_matrix/client/v4/sync` the MSC defines. See NewMux.
//
// Mirrors SlidingSyncRestServlet.on_POST and
// SlidingSyncHandler.wait_for_sync_for_user.
func SlidingSync(d Deps) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ann := server.Annotate(r.Context())
		if ann != nil {
			ann.Endpoint = "sliding_sync"
		}

		verdict, ok := authenticate(w, r, d, ann)
		if !ok {
			return
		}

		// Synapse gates this endpoint on a per-user experimental feature rather
		// than on /versions, so a user it is disabled for gets M_UNRECOGNIZED
		// -- the endpoint is meant to look absent, not forbidden.
		enabled, err := d.Store.ExperimentalFeatureEnabled(
			r.Context(), verdict.UserID, "msc3575", d.MSC3575Enabled)
		if err != nil {
			refuse(w, ann, http.StatusInternalServerError,
				*internalError(d, "experimental features", err))
			return
		}
		if !enabled {
			refuse(w, ann, http.StatusNotFound, matrixerr.Error{
				ErrCode: matrixerr.CodeUnrecognized, Error: "Unrecognized request"})
			return
		}

		// A device is part of a connection's identity, and there is no sensible
		// fallback: two logins on one account must not share connection state.
		if verdict.DeviceID == "" {
			refuse(w, ann, http.StatusBadRequest, matrixerr.Error{
				ErrCode: matrixerr.CodeUnknown, Error: "Sliding sync requires a device"})
			return
		}

		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxSlidingSyncBody))
		if err != nil {
			refuse(w, ann, http.StatusBadRequest, matrixerr.Error{
				ErrCode: matrixerr.CodeNotJSON, Error: "Unable to read request body"})
			return
		}
		req, err := slidingsync.ParseRequest(body)
		if err != nil {
			refuse(w, ann, http.StatusBadRequest, matrixerr.Error{
				ErrCode: matrixerr.CodeInvalidParam, Error: err.Error()})
			return
		}

		// Synapse reads `pos` and `timeout` from the QUERY STRING
		// (rest/client/sync.py), while MSC4186 specifies them as body fields.
		// The query wins, and the body is a fallback -- so today's clients and
		// the stable endpoint's clients both work, and nothing Synapse would
		// answer is answered differently.
		posParam := r.URL.Query().Get("pos")
		if posParam == "" && req.Pos != nil {
			posParam = *req.Pos
		}
		timeoutMS := intParam(r, "timeout", -1)
		if timeoutMS < 0 {
			timeoutMS = 0
			if req.Timeout != nil {
				timeoutMS = *req.Timeout
			}
		}
		if ann != nil {
			ann.Timeout = timeoutMS
			ann.Since = posParam
		}

		out, status, mxErr := slidingSyncPoll(r, d, verdict, req, posParam, timeoutMS, ann)
		if mxErr != nil {
			refuse(w, ann, status, *mxErr)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if status != http.StatusOK {
			w.WriteHeader(status)
		}
		_, _ = w.Write(out)
	})
}

// maxSlidingSyncBody bounds the request. A sliding sync body carries up to 100
// lists and 100 room subscriptions with their required_state, which is
// generous; anything beyond this is not a client.
const maxSlidingSyncBody = 1 << 20

// slidingSyncPoll computes a response, waiting for one if the client asked to.
//
// The shape differs from classic sync's long poll in a way that matters: the
// `pos` stays FIXED across passes while the now token advances, and a response
// is computed BEFORE any waiting because the request's own config may have
// changed since the last one. A client that adds a room subscription and polls
// must be answered immediately, not when the next event happens to arrive.
func slidingSyncPoll(r *http.Request, d Deps, verdict auth.Verdict,
	req *slidingsync.Request, posParam string, timeoutMS int,
	ann *server.Annotation) ([]byte, int, *matrixerr.Error) {

	body, status, mxErr := slidingSyncOnce(r, d, verdict, req, posParam)
	if mxErr != nil || status != http.StatusOK {
		return body, status, mxErr
	}

	timeout := time.Duration(timeoutMS) * time.Millisecond
	// An initial request always returns immediately: there is nothing to wait
	// for when the client has nothing. A pinned request is a comparison rather
	// than a client.
	if timeout <= 0 || posParam == "" || d.Notifier == nil ||
		r.URL.Query().Get("_gosync_now") != "" || !slidingSyncIsEmpty(body) {
		return body, status, mxErr
	}
	if timeout > maxSyncTimeout {
		timeout = maxSyncTimeout
	}

	ctx := r.Context()
	deadline := time.Now().Add(timeout)
	started := time.Now()
	metrics.SyncWaiters.Inc()
	defer metrics.SyncWaiters.Dec()

	for {
		// Interest is registered BEFORE the answer is computed, exactly as in
		// classic sync: an event landing during the computation must still wake
		// us, or the client hangs for its whole timeout on news that had
		// already arrived.
		rooms, err := d.Store.SlidingRoomsForUser(ctx, verdict.UserID)
		if err != nil {
			return nil, http.StatusInternalServerError, internalError(d, "rooms for user", err)
		}
		roomIDs := make([]string, 0, len(rooms))
		for roomID := range rooms {
			roomIDs = append(roomIDs, roomID)
		}
		handle := d.Notifier.Register(roomIDs, []string{verdict.UserID})

		body, status, mxErr = slidingSyncOnce(r, d, verdict, req, posParam)
		if mxErr != nil || status != http.StatusOK || !slidingSyncIsEmpty(body) {
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
	}
}

// slidingSyncOnce computes one response.
func slidingSyncOnce(r *http.Request, d Deps, verdict auth.Verdict,
	req *slidingsync.Request, posParam string) ([]byte, int, *matrixerr.Error) {

	ctx := r.Context()

	var from *streamtoken.Token
	var connectionPos int64
	if posParam != "" {
		parsed, err := slidingsync.ParsePos(posParam)
		if err != nil {
			// A malformed `pos` is the client's, so it is told to start over
			// rather than given a 500.
			return unknownPos(), http.StatusBadRequest, nil
		}
		connectionPos = parsed.ConnectionPosition
		tok := parsed.StreamToken
		from = &tok
	}

	now, _, mxErr := nowToken(r, d)
	if mxErr != nil {
		return nil, http.StatusBadRequest, mxErr
	}
	nowMS, mxErr := nowMillis(r, d)
	if mxErr != nil {
		return nil, http.StatusBadRequest, mxErr
	}

	res, err := slidingsync.Build(ctx, d.slidingDeps(), slidingsync.BuildRequest{
		UserID:        verdict.UserID,
		DeviceID:      verdict.DeviceID,
		Request:       req,
		From:          from,
		ConnectionPos: connectionPos,
		Now:           now,
		NowMS:         nowMS,
	})
	if errors.Is(err, slidingstore.ErrUnknownPosition) {
		// Not a failure: the connection state is gone, and the client is told
		// to start a fresh one. See internal/slidingstore.
		return unknownPos(), http.StatusBadRequest, nil
	}
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "sliding sync", err)
	}

	body, err := json.Marshal(res)
	if err != nil {
		return nil, http.StatusInternalServerError, internalError(d, "encode sliding sync", err)
	}
	return body, http.StatusOK, nil
}

func unknownPos() []byte {
	body, _ := json.Marshal(map[string]string{
		"errcode": "M_UNKNOWN_POS",
		"error":   "Unknown position",
	})
	return body
}

// slidingSyncIsEmpty reports whether a response is worth returning early.
//
// `pos` and `lists` are always present -- the counts alone are news a client
// acts on -- so emptiness is about rooms and extensions.
func slidingSyncIsEmpty(body []byte) bool {
	var parsed struct {
		Rooms      map[string]json.RawMessage `json:"rooms"`
		Extensions map[string]json.RawMessage `json:"extensions"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return false
	}
	return len(parsed.Rooms) == 0 && len(parsed.Extensions) == 0
}

func (d Deps) slidingDeps() slidingsync.Deps {
	return slidingsync.Deps{
		Store:            d.Store,
		Sliding:          d.Sliding,
		Inbox:            d.Inbox,
		Replication:      d.Replication,
		ExcludedRooms:    d.ExcludedRooms,
		ServerName:       d.ServerName,
		MSC4354Enabled:   d.MSC4354Enabled,
		MSC3391Enabled:   d.MSC3391Enabled,
		MSC4308Enabled:   d.MSC4308Enabled,
		PushRuleFeatures: d.PushRuleFeatures,
	}
}
