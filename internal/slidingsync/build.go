package slidingsync

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"golang.org/x/sync/errgroup"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/slidingstore"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/streamtoken"
)

// Build assembles a whole sliding sync response.
//
// A port of SlidingSyncHandler.current_sync_for_user: read the connection
// state, work out which rooms are interesting, describe each of them, then
// record what the client has now been told and hand back the position that
// names it.

// BuildRequest is one request's inputs.
type BuildRequest struct {
	UserID   string
	DeviceID string
	Request  *Request

	// From is the stream token half of the client's `pos`, nil when it sent
	// none. ConnectionPos is the other half, zero when the client has no
	// connection state -- which is NOT the same thing, and both are possible
	// together.
	From          *streamtoken.Token
	ConnectionPos int64

	Now   streamtoken.Token
	NowMS int64
	// TokenID is the access token's row id, for `unsigned.transaction_id` on
	// events stored before device ids were recorded.
	TokenID int64
}

// Response is the response body.
type Response struct {
	Pos        string                     `json:"pos"`
	Lists      map[string]listJSON        `json:"lists"`
	Rooms      map[string]roomJSON        `json:"rooms"`
	Extensions map[string]json.RawMessage `json:"extensions"`
}

type listJSON struct {
	Count int      `json:"count"`
	Ops   []opJSON `json:"ops"`
}

type opJSON struct {
	Op      string   `json:"op"`
	Range   [2]int   `json:"range"`
	RoomIDs []string `json:"room_ids"`
}

type roomJSON struct {
	Name                     *string           `json:"name,omitempty"`
	Avatar                   *string           `json:"avatar,omitempty"`
	Heroes                   []heroJSON        `json:"heroes,omitempty"`
	IsDM                     bool              `json:"is_dm,omitempty"`
	Initial                  bool              `json:"initial,omitempty"`
	RequiredState            []json.RawMessage `json:"required_state,omitempty"`
	Timeline                 []json.RawMessage `json:"timeline,omitempty"`
	InviteState              []json.RawMessage `json:"invite_state,omitempty"`
	PrevBatch                *string           `json:"prev_batch,omitempty"`
	Limited                  *bool             `json:"limited,omitempty"`
	NumLive                  *int              `json:"num_live,omitempty"`
	UnstableExpandedTimeline bool              `json:"unstable_expanded_timeline,omitempty"`
	BumpStamp                *int64            `json:"bump_stamp,omitempty"`
	JoinedCount              *int              `json:"joined_count,omitempty"`
	InvitedCount             *int              `json:"invited_count,omitempty"`
	NotificationCount        int               `json:"notification_count"`
	HighlightCount           int               `json:"highlight_count"`
}

type heroJSON struct {
	UserID      string  `json:"user_id"`
	DisplayName *string `json:"displayname,omitempty"`
	AvatarURL   *string `json:"avatar_url,omitempty"`
}

// roomConcurrency matches Synapse's concurrently_execute(..., 20) for the room
// loop. Deliberately NOT raised: classic sync records that how many rooms are
// built at once changes which members a lazy-loading sync considers already
// sent, and the same hazard applies here.
const roomConcurrency = 20

// Build computes a response.
func Build(ctx context.Context, d Deps, req BuildRequest) (*Response, error) {
	if d.Sliding == nil {
		return nil, errors.New("slidingsync: no connection store configured")
	}

	connID := req.Request.ConnID

	// Reading the connection state is itself a write, and it can fail with
	// ErrUnknownPosition -- which the caller turns into M_UNKNOWN_POS rather
	// than an error. See internal/slidingstore.
	prev, err := d.Sliding.GetAndClear(ctx, req.UserID, req.DeviceID, connID, req.ConnectionPos)
	if err != nil {
		return nil, err
	}
	next := &slidingstore.PerConnectionState{
		Rooms:       slidingstore.NewRoomStatusMap(prev.Rooms.All()),
		Receipts:    slidingstore.NewRoomStatusMap(prev.Receipts.All()),
		AccountData: slidingstore.NewRoomStatusMap(prev.AccountData.All()),
	}
	for roomID, cfg := range prev.RoomConfigs {
		if next.RoomConfigs == nil {
			next.RoomConfigs = map[string]slidingstore.RoomSyncConfig{}
		}
		next.RoomConfigs[roomID] = cfg
	}

	lists, err := ComputeRoomLists(ctx, d, req.UserID, req.Request, req.From, req.Now)
	if err != nil {
		return nil, err
	}

	allRelevant := make([]string, 0, len(lists.Relevant))
	for roomID := range lists.Relevant {
		allRelevant = append(allRelevant, roomID)
	}
	sort.Strings(allRelevant)

	meta, err := d.Store.SlidingJoinedRooms(ctx, allRelevant)
	if err != nil {
		return nil, err
	}

	// Narrow to the rooms the client actually needs. Without this every
	// response re-sends every room in the window, which is the difference
	// between a sliding sync and a very expensive /sync.
	toSend, err := filterRoomsToSend(ctx, d, lists.Relevant, meta, prev, req.From, req.NowMS)
	if err != nil {
		return nil, err
	}
	roomIDs := make([]string, 0, len(toSend))
	for roomID := range toSend {
		roomIDs = append(roomIDs, roomID)
	}
	sort.Strings(roomIDs)

	results := make([]*RoomResult, len(roomIDs))
	states := make([]*slidingstore.PerConnectionState, len(roomIDs))

	group, gctx := errgroup.WithContext(ctx)
	group.SetLimit(roomConcurrency)
	for i, roomID := range roomIDs {
		i, roomID := i, roomID
		group.Go(func() error {
			m, hasMeta := meta[roomID]
			// Each room accumulates into its OWN state and they are merged in
			// index order afterwards. Sharing one would make the stored
			// connection state depend on which goroutine finished first.
			roomState := &slidingstore.PerConnectionState{}
			res, err := GetRoomData(gctx, d, RoomDataRequest{
				UserID:      req.UserID,
				DeviceID:    req.DeviceID,
				TokenID:     req.TokenID,
				RoomID:      roomID,
				Config:      toSend[roomID],
				Room:        lists.Membership[roomID],
				Meta:        m,
				HasMeta:     hasMeta,
				From:        req.From,
				To:          req.Now,
				NowMS:       req.NowMS,
				Previous:    prev,
				New:         roomState,
				NewlyJoined: lists.NewlyJoined[roomID],
				NewlyLeft:   lists.NewlyLeft[roomID],
				IsDM:        lists.DMRooms[roomID],
			})
			if err != nil {
				return err
			}
			results[i], states[i] = res, roomState
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	rooms := make(map[string]roomJSON, len(roomIDs))
	sent := make([]string, 0, len(roomIDs))
	for i, roomID := range roomIDs {
		if results[i] == nil {
			continue
		}
		rooms[roomID] = renderRoom(results[i])
		sent = append(sent, roomID)
		mergeRoomState(next, roomID, states[i])
	}
	next.Rooms.RecordSentRooms(sent)

	// Rooms the client cares about that fall OUTSIDE every list range and
	// subscription, and that have had events. Those are marked as having
	// updates pending, so that when one later scrolls into the window it is
	// sent from where the client left off rather than treated as up to date.
	//
	// Outside the window is the important qualifier. Marking every room this
	// response did not describe would include the ones that were in range and
	// simply had nothing new -- and PREVIOUSLY means "there ARE updates we
	// withheld", so the next request would dutifully re-send them. Measured
	// before this was narrowed: responses alternated between five rooms and
	// none, for ever.
	if req.From != nil {
		missing := make([]string, 0, len(lists.AllRooms))
		for roomID := range lists.AllRooms {
			if _, inWindow := lists.Relevant[roomID]; !inWindow {
				missing = append(missing, roomID)
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			missingMeta, err := d.Store.SlidingJoinedRooms(ctx, missing)
			if err != nil {
				return nil, err
			}
			unsent, err := roomsWithUpdates(ctx, d, missing, missingMeta, req.From)
			if err != nil {
				return nil, err
			}
			sort.Strings(unsent)
			next.Rooms.RecordUnsentRooms(unsent, req.From.Room.String())
		}
	}

	position := req.ConnectionPos
	if len(req.Request.Lists) > 0 || len(req.Request.RoomSubscriptions) > 0 {
		position, err = d.Sliding.Persist(ctx, req.UserID, req.DeviceID, connID, req.ConnectionPos, next)
		if err != nil {
			return nil, err
		}
	}

	out := &Response{
		Pos:        Pos{ConnectionPosition: position, StreamToken: req.Now}.String(),
		Lists:      make(map[string]listJSON, len(lists.Lists)),
		Rooms:      rooms,
		Extensions: map[string]json.RawMessage{},
	}
	for key, l := range lists.Lists {
		ops := make([]opJSON, 0, len(l.Ops))
		for _, op := range l.Ops {
			ids := op.RoomIDs
			if ids == nil {
				ids = []string{}
			}
			ops = append(ops, opJSON{Op: op.Op, Range: op.Range, RoomIDs: ids})
		}
		out.Lists[key] = listJSON{Count: l.Count, Ops: ops}
	}
	return out, nil
}

// mergeRoomState folds one room's bookkeeping into the connection's.
func mergeRoomState(dst *slidingstore.PerConnectionState, roomID string, src *slidingstore.PerConnectionState) {
	if src == nil {
		return
	}
	if cfg, ok := src.RoomConfigs[roomID]; ok {
		dst.SetRoomConfig(roomID, cfg)
	}
	if lm, ok := src.LazyMembership[roomID]; ok {
		if dst.LazyMembership == nil {
			dst.LazyMembership = map[string]*slidingstore.LazyMembers{}
		}
		dst.LazyMembership[roomID] = lm
	}
}

func renderRoom(r *RoomResult) roomJSON {
	out := roomJSON{
		Name: r.Name, Avatar: r.Avatar, IsDM: r.IsDM, Initial: r.Initial,
		RequiredState: r.RequiredState, Timeline: r.Timeline,
		InviteState: r.StrippedState, PrevBatch: r.PrevBatch,
		Limited: r.Limited, NumLive: r.NumLive,
		UnstableExpandedTimeline: r.UnstableExpandedTimeline,
		BumpStamp:                r.BumpStamp,
		JoinedCount:              r.JoinedCount, InvitedCount: r.InvitedCount,
		NotificationCount: r.NotificationCount, HighlightCount: r.HighlightCount,
	}
	for _, h := range r.Heroes {
		out.Heroes = append(out.Heroes, heroJSON{
			UserID: h.UserID, DisplayName: h.DisplayName, AvatarURL: h.AvatarURL,
		})
	}
	return out
}

var _ = store.SlidingRoom{}
