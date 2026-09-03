package slidingsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// The one test here that compares against Synapse rather than against our own
// reasoning. It asks a real sync worker for a room list and checks ours orders
// the same rooms the same way.
//
//	GOSYNC_TEST_DSN="host=/var/sockets user=gopro_ro dbname=synapse-db" \
//	GOSYNC_LIVE_REF_SOCKET=/var/sockets/nginx/av-sync-worker-2.sock \
//	GOSYNC_LIVE_TOKEN_FILE=~/.gosync-test-token \
//	GOSYNC_PARITY_USER=@goworker:aguiarvieira.pt \
//	  go test ./internal/slidingsync/ -run LiveListParity -v
//
// It is READ-ONLY on the reference except for one thing worth knowing: a
// sliding sync request creates per-connection state on the server it asks, so
// each run leaves a row in Synapse's own sliding_sync_connections. Every
// request here uses its own conn_id, and Synapse reaps them after seven days.

func refClient(t *testing.T) (*http.Client, string) {
	t.Helper()
	socket := os.Getenv("GOSYNC_LIVE_REF_SOCKET")
	tokenFile := os.Getenv("GOSYNC_LIVE_TOKEN_FILE")
	if socket == "" || tokenFile == "" {
		t.Skip("GOSYNC_LIVE_REF_SOCKET or GOSYNC_LIVE_TOKEN_FILE not set")
	}
	if strings.HasPrefix(tokenFile, "~/") {
		tokenFile = os.Getenv("HOME") + tokenFile[1:]
	}
	raw, err := os.ReadFile(tokenFile)
	if err != nil {
		t.Skipf("token file: %v", err)
	}
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socket)
			},
		},
	}, strings.TrimSpace(string(raw))
}

// refRaw posts a sliding sync request to the reference and returns the body.
func refRaw(t *testing.T, c *http.Client, token string, body map[string]any) []byte {
	t.Helper()
	if _, ok := body["conn_id"]; !ok {
		body["conn_id"] = fmt.Sprintf("gosync-parity-%d", time.Now().UnixNano())
	}
	enc, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost,
		"http://localhost/_matrix/client/unstable/org.matrix.simplified_msc3575/sync?timeout=0",
		bytes.NewReader(enc))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("reference: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reference returned %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func refList(t *testing.T, c *http.Client, token string, body map[string]any) (int, []string) {
	t.Helper()
	body["conn_id"] = fmt.Sprintf("gosync-parity-%d", time.Now().UnixNano())
	enc, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost,
		"http://localhost/_matrix/client/unstable/org.matrix.simplified_msc3575/sync?timeout=0",
		bytes.NewReader(enc))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("reference: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reference returned %d", resp.StatusCode)
	}
	var parsed struct {
		Lists map[string]struct {
			Count int `json:"count"`
			Ops   []struct {
				RoomIDs []string `json:"room_ids"`
			} `json:"ops"`
		} `json:"lists"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatal(err)
	}
	l := parsed.Lists["all"]
	if len(l.Ops) == 0 {
		return l.Count, nil
	}
	return l.Count, l.Ops[0].RoomIDs
}

// TestLiveListParity is the real check: for a PARTIAL range, Synapse sorts, and
// our order must be its order.
//
// A full range is deliberately not compared. Synapse skips sorting entirely
// when one range covers the whole list ("Optimization: If we are asking for the
// full range, we don't need to sort the list"), and returns the rooms in Python
// dict order -- so there is no order to match, only an arbitrary one. Verified
// on 2026-09-01..03: ranges [[0,4]] against a 9-room account come back sorted
// and identical to ours, while [[0,8]] and [[0,99]] come back in an order that
// matches neither event_stream_ordering nor bump_stamp. See
// docs/comparability.md.
func TestLiveListParityOnAPartialRange(t *testing.T) {
	d, _, now, ctx := liveDeps(t)
	c, token := refClient(t)

	userID := os.Getenv("GOSYNC_PARITY_USER")
	if userID == "" {
		t.Skip("GOSYNC_PARITY_USER not set")
	}

	// Establish how many rooms there are, so the range can be kept strictly
	// inside the list and the sort therefore actually runs.
	total, _ := refList(t, c, token, map[string]any{
		"lists": map[string]any{"all": map[string]any{
			"ranges": [][2]int{{0, 999}}, "required_state": [][2]string{}, "timeline_limit": 1,
		}},
	})
	if total < 3 {
		t.Skipf("only %d rooms; a partial range needs more", total)
	}
	end := total - 2

	refCount, refRooms := refList(t, c, token, map[string]any{
		"lists": map[string]any{"all": map[string]any{
			"ranges": [][2]int{{0, end}}, "required_state": [][2]string{}, "timeline_limit": 1,
		}},
	})

	req := &Request{Lists: map[string]List{"all": {
		CommonRoomParameters: CommonRoomParameters{TimelineLimit: 1},
		Ranges:               [][2]int{{0, end}},
	}}}
	got, err := ComputeRoomLists(ctx, d, userID, req, now)
	if err != nil {
		t.Fatal(err)
	}
	ours := got.Lists["all"]

	if ours.Count != refCount {
		t.Errorf("count = %d, reference says %d", ours.Count, refCount)
	}
	oursRooms := ours.Ops[0].RoomIDs
	if len(oursRooms) != len(refRooms) {
		t.Fatalf("window holds %d rooms, reference holds %d", len(oursRooms), len(refRooms))
	}
	for i := range refRooms {
		if oursRooms[i] != refRooms[i] {
			t.Errorf("position %d: we say %s, reference says %s", i, oursRooms[i], refRooms[i])
		}
	}
	if !t.Failed() {
		t.Logf("%d rooms in the same order as the reference", len(refRooms))
	}
}

// The room SET must match even where the order does not, which is what makes
// the full-range case still worth comparing.
func TestLiveListParityOnTheFullSet(t *testing.T) {
	d, _, now, ctx := liveDeps(t)
	c, token := refClient(t)

	userID := os.Getenv("GOSYNC_PARITY_USER")
	if userID == "" {
		t.Skip("GOSYNC_PARITY_USER not set")
	}

	refCount, refRooms := refList(t, c, token, map[string]any{
		"lists": map[string]any{"all": map[string]any{
			"ranges": [][2]int{{0, 999}}, "required_state": [][2]string{}, "timeline_limit": 1,
		}},
	})

	req := &Request{Lists: map[string]List{"all": {
		CommonRoomParameters: CommonRoomParameters{TimelineLimit: 1},
		Ranges:               [][2]int{{0, 999}},
	}}}
	got, err := ComputeRoomLists(ctx, d, userID, req, now)
	if err != nil {
		t.Fatal(err)
	}
	ours := got.Lists["all"]

	if ours.Count != refCount {
		t.Errorf("count = %d, reference says %d", ours.Count, refCount)
	}
	inRef := map[string]bool{}
	for _, r := range refRooms {
		inRef[r] = true
	}
	for _, r := range ours.Ops[0].RoomIDs {
		if !inRef[r] {
			t.Errorf("we return %s and the reference does not", r)
		}
		delete(inRef, r)
	}
	for r := range inRef {
		t.Errorf("the reference returns %s and we do not", r)
	}
	if !t.Failed() {
		t.Logf("%d rooms, same set (order not compared: Synapse does not sort a full range)",
			len(refRooms))
	}
}
