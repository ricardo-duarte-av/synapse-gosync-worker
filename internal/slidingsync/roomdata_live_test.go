package slidingsync

import (
	"encoding/json"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/slidingstore"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/store"
)

// Per-room results, compared field by field against a real Synapse sync worker.
//
// Only an INITIAL request is compared. An incremental one is a delta against
// per-connection state, and the two sides keep that state in different tables
// with different positions, so there is no way to put them in the same place
// without driving both through the same sequence of requests -- which is
// syncdiff's job (M12, still to come), not a unit test's.
//
//	GOSYNC_TEST_DSN=... GOSYNC_LIVE_REF_SOCKET=... \
//	GOSYNC_LIVE_TOKEN_FILE=~/.gosync-test-token \
//	GOSYNC_PARITY_USER=@goworker:aguiarvieira.pt \
//	  go test ./internal/slidingsync/ -run LiveRoomData -v

type refRoom struct {
	Name          *string           `json:"name"`
	Avatar        *string           `json:"avatar"`
	Initial       bool              `json:"initial"`
	IsDM          bool              `json:"is_dm"`
	RequiredState []json.RawMessage `json:"required_state"`
	Timeline      []json.RawMessage `json:"timeline"`
	Limited       *bool             `json:"limited"`
	BumpStamp     *int64            `json:"bump_stamp"`
	JoinedCount   *int              `json:"joined_count"`
	InvitedCount  *int              `json:"invited_count"`
	NumLive       *int              `json:"num_live"`
	Heroes        []struct {
		UserID      string  `json:"user_id"`
		DisplayName *string `json:"displayname"`
	} `json:"heroes"`
	NotificationCount int `json:"notification_count"`
	HighlightCount    int `json:"highlight_count"`
}

func refRooms(t *testing.T, body map[string]any) map[string]refRoom {
	t.Helper()
	c, token := refClient(t)
	body["conn_id"] = "gosync-roomdata-" + t.Name()
	raw := refRaw(t, c, token, body)
	var parsed struct {
		Rooms map[string]refRoom `json:"rooms"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatal(err)
	}
	return parsed.Rooms
}

func stateKeysOf(events []json.RawMessage) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, gjson.GetBytes(e, "type").String()+"/"+gjson.GetBytes(e, "state_key").String())
	}
	sort.Strings(out)
	return out
}

func eventIDsOf(events []json.RawMessage) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, gjson.GetBytes(e, "event_id").String())
	}
	return out
}

func TestLiveRoomDataParity(t *testing.T) {
	d, _, now, ctx := liveDeps(t)
	nowMS := time.Now().UnixMilli()
	userID := os.Getenv("GOSYNC_PARITY_USER")
	if userID == "" {
		t.Skip("GOSYNC_PARITY_USER not set")
	}

	const timelineLimit = 5
	required := [][2]string{
		{"m.room.name", ""},
		{"m.room.topic", ""},
		{"m.room.encryption", ""},
	}

	body := map[string]any{
		"lists": map[string]any{"all": map[string]any{
			"ranges":         [][2]int{{0, 4}},
			"required_state": required,
			"timeline_limit": timelineLimit,
		}},
	}
	theirs := refRooms(t, body)
	if len(theirs) == 0 {
		t.Skip("reference returned no rooms")
	}

	req := &Request{Lists: map[string]List{"all": {
		CommonRoomParameters: CommonRoomParameters{
			RequiredState: required, TimelineLimit: timelineLimit,
		},
		Ranges: [][2]int{{0, 4}},
	}}}
	lists, err := ComputeRoomLists(ctx, d, userID, req, now)
	if err != nil {
		t.Fatal(err)
	}

	meta, err := d.Store.SlidingJoinedRooms(ctx, lists.Lists["all"].Ops[0].RoomIDs)
	if err != nil {
		t.Fatal(err)
	}

	compared := 0
	for _, roomID := range lists.Lists["all"].Ops[0].RoomIDs {
		theirRoom, ok := theirs[roomID]
		if !ok {
			t.Errorf("%s is in our window but not in the reference response", roomID)
			continue
		}
		m, hasMeta := meta[roomID]
		newState := &slidingstore.PerConnectionState{}

		ours, err := GetRoomData(ctx, d, RoomDataRequest{
			UserID:   userID,
			RoomID:   roomID,
			Config:   lists.Relevant[roomID],
			Room:     lists.Membership[roomID],
			Meta:     m,
			HasMeta:  hasMeta,
			From:     nil,
			To:       now,
			NowMS:    nowMS,
			Previous: &slidingstore.PerConnectionState{},
			New:      newState,
			IsDM:     lists.DMRooms[roomID],
		})
		if err != nil {
			t.Fatalf("%s: %v", roomID, err)
		}
		compared++

		t.Run(roomID, func(t *testing.T) {
			if !ours.Initial || !theirRoom.Initial {
				t.Errorf("initial = %v, reference %v; both should be true for a first request",
					ours.Initial, theirRoom.Initial)
			}
			comparePtrString(t, "name", ours.Name, theirRoom.Name)
			comparePtrString(t, "avatar", ours.Avatar, theirRoom.Avatar)
			comparePtrInt(t, "joined_count", ours.JoinedCount, theirRoom.JoinedCount)
			comparePtrInt(t, "invited_count", ours.InvitedCount, theirRoom.InvitedCount)
			if ours.IsDM != theirRoom.IsDM {
				t.Errorf("is_dm = %v, reference %v", ours.IsDM, theirRoom.IsDM)
			}
			if ours.NotificationCount != theirRoom.NotificationCount ||
				ours.HighlightCount != theirRoom.HighlightCount {
				t.Errorf("counts = %d/%d, reference %d/%d (both are dummy zeros upstream)",
					ours.NotificationCount, ours.HighlightCount,
					theirRoom.NotificationCount, theirRoom.HighlightCount)
			}

			// required_state is compared by (type, state_key): the events
			// themselves are byte-identical stored JSON on both sides, and what
			// is being tested here is the SELECTION.
			gotKeys, wantKeys := stateKeysOf(ours.RequiredState), stateKeysOf(theirRoom.RequiredState)
			if !equalStrings(gotKeys, wantKeys) {
				t.Errorf("required_state = %v, reference %v", gotKeys, wantKeys)
			}

			// Timeline compared by event ID and order. A message landing
			// between the two requests would show up here as a genuine
			// difference, which is why the test account is a quiet one.
			gotIDs, wantIDs := eventIDsOf(ours.Timeline), eventIDsOf(theirRoom.Timeline)
			if !equalStrings(gotIDs, wantIDs) {
				t.Errorf("timeline = %v, reference %v", gotIDs, wantIDs)
			} else {
				// And the bodies. `unsigned` is dropped from both sides for
				// now: we do not yet populate `membership` (MSC4115) or
				// `transaction_id`, because both come from the visibility
				// filter and the requester's device, and neither is wired into
				// this path yet. Recorded in docs/milestones.md; this
				// comparison tightens to include `unsigned` when they are.
				for i := range gotIDs {
					g := withoutUnsigned(t, ours.Timeline[i])
					w := withoutUnsigned(t, theirRoom.Timeline[i])
					if g != w {
						t.Errorf("timeline event %s differs:\n  ours %s\n  ref  %s", gotIDs[i], g, w)
					}
				}
			}

			// Same for the state events.
			if equalStrings(gotKeys, wantKeys) {
				oursByKey := byStateKey(ours.RequiredState)
				theirsByKey := byStateKey(theirRoom.RequiredState)
				for k, g := range oursByKey {
					w, ok := theirsByKey[k]
					if !ok {
						continue
					}
					if withoutUnsigned(t, g) != withoutUnsigned(t, w) {
						t.Errorf("required_state %s differs:\n  ours %s\n  ref  %s",
							k, withoutUnsigned(t, g), withoutUnsigned(t, w))
					}
				}
			}

			if (ours.Limited == nil) != (theirRoom.Limited == nil) {
				t.Errorf("limited presence differs: %v vs %v", ours.Limited, theirRoom.Limited)
			} else if ours.Limited != nil && *ours.Limited != *theirRoom.Limited {
				t.Errorf("limited = %v, reference %v", *ours.Limited, *theirRoom.Limited)
			}

			comparePtrInt64(t, "bump_stamp", ours.BumpStamp, theirRoom.BumpStamp)

			var gotHeroes, wantHeroes []string
			for _, h := range ours.Heroes {
				gotHeroes = append(gotHeroes, h.UserID)
			}
			for _, h := range theirRoom.Heroes {
				wantHeroes = append(wantHeroes, h.UserID)
			}
			if !equalStrings(gotHeroes, wantHeroes) {
				t.Errorf("heroes = %v, reference %v", gotHeroes, wantHeroes)
			}
		})

		// The room config must have been recorded, or the next response repeats
		// everything.
		if _, ok := newState.RoomConfigs[roomID]; !ok {
			t.Errorf("%s: no room config recorded for the connection", roomID)
		}
	}
	t.Logf("compared %d rooms", compared)
}

// Lazy-loaded members are what a real client asks for, and the selection
// differs from the explicit case in a way worth checking against the reference.
func TestLiveRoomDataParityWithLazyMembers(t *testing.T) {
	d, _, now, ctx := liveDeps(t)
	nowMS := time.Now().UnixMilli()
	userID := os.Getenv("GOSYNC_PARITY_USER")
	if userID == "" {
		t.Skip("GOSYNC_PARITY_USER not set")
	}

	required := [][2]string{{"m.room.member", "$LAZY"}, {"m.room.name", ""}}
	body := map[string]any{
		"lists": map[string]any{"all": map[string]any{
			"ranges": [][2]int{{0, 2}}, "required_state": required, "timeline_limit": 5,
		}},
	}
	theirs := refRooms(t, body)
	if len(theirs) == 0 {
		t.Skip("reference returned no rooms")
	}

	req := &Request{Lists: map[string]List{"all": {
		CommonRoomParameters: CommonRoomParameters{RequiredState: required, TimelineLimit: 5},
		Ranges:               [][2]int{{0, 2}},
	}}}
	lists, err := ComputeRoomLists(ctx, d, userID, req, now)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := d.Store.SlidingJoinedRooms(ctx, lists.Lists["all"].Ops[0].RoomIDs)
	if err != nil {
		t.Fatal(err)
	}

	for _, roomID := range lists.Lists["all"].Ops[0].RoomIDs {
		theirRoom, ok := theirs[roomID]
		if !ok {
			continue
		}
		m, hasMeta := meta[roomID]
		newState := &slidingstore.PerConnectionState{}
		ours, err := GetRoomData(ctx, d, RoomDataRequest{
			UserID: userID, RoomID: roomID,
			Config: lists.Relevant[roomID], Room: lists.Membership[roomID],
			Meta: m, HasMeta: hasMeta, To: now, NowMS: nowMS,
			Previous: &slidingstore.PerConnectionState{}, New: newState,
			IsDM: lists.DMRooms[roomID],
		})
		if err != nil {
			t.Fatalf("%s: %v", roomID, err)
		}

		got, want := stateKeysOf(ours.RequiredState), stateKeysOf(theirRoom.RequiredState)
		if !equalStrings(got, want) {
			t.Errorf("%s: lazy required_state = %v, reference %v", roomID, got, want)
		}

		// Everyone in the timeline must have their membership included, or the
		// client cannot put a name to a message.
		senders := map[string]bool{}
		for _, e := range ours.Timeline {
			senders[gjson.GetBytes(e, "sender").String()] = true
		}
		included := map[string]bool{}
		for _, e := range ours.RequiredState {
			if gjson.GetBytes(e, "type").String() == "m.room.member" {
				included[gjson.GetBytes(e, "state_key").String()] = true
			}
		}
		for sender := range senders {
			if !included[sender] {
				t.Errorf("%s: %s sent a timeline event but their membership was not included",
					roomID, sender)
			}
		}
		if newState.LazyMembership[roomID] == nil && len(senders) > 0 {
			t.Errorf("%s: lazy membership was not recorded for the connection", roomID)
		}
	}
}

func comparePtrString(t *testing.T, field string, got, want *string) {
	t.Helper()
	switch {
	case got == nil && want == nil:
	case got == nil || want == nil:
		t.Errorf("%s = %v, reference %v", field, deref(got), deref(want))
	case *got != *want:
		t.Errorf("%s = %q, reference %q", field, *got, *want)
	}
}

func comparePtrInt(t *testing.T, field string, got, want *int) {
	t.Helper()
	switch {
	case got == nil && want == nil:
	case got == nil || want == nil:
		t.Errorf("%s presence differs: %v vs %v", field, got, want)
	case *got != *want:
		t.Errorf("%s = %d, reference %d", field, *got, *want)
	}
}

func comparePtrInt64(t *testing.T, field string, got, want *int64) {
	t.Helper()
	switch {
	case got == nil && want == nil:
	case got == nil || want == nil:
		t.Errorf("%s presence differs: %v vs %v", field, got, want)
	case *got != *want:
		t.Errorf("%s = %d, reference %d", field, *got, *want)
	}
}

func deref(s *string) string {
	if s == nil {
		return "<absent>"
	}
	return *s
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var _ = store.StateKey{}

// withoutUnsigned normalises an event for comparison by dropping `unsigned`.
//
// `age` could never match -- it is recomputed from the wall clock on each side
// (comparability.md, source 1) -- and `membership` and `transaction_id` are not
// populated on this path yet. Dropping the whole object rather than three named
// fields keeps the reason in one place: when the visibility filter is wired in,
// this becomes withoutAge again and the comparison tightens.
func withoutUnsigned(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	delete(v, "unsigned")
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func byStateKey(events []json.RawMessage) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for _, e := range events {
		k := gjson.GetBytes(e, "type").String() + "/" + gjson.GetBytes(e, "state_key").String()
		out[k] = e
	}
	return out
}
