package slidingsync

import (
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"
)

// The extensions, compared against a real Synapse sync worker.
//
// Driven through the WORKER rather than through Build, because three of the
// seven depend on per-connection state and two more on what the room section
// contains -- so the thing worth comparing is a whole response, twice, with the
// position fed back.
//
//	GOSYNC_LIVE_SS_SOCKET=/tmp/claude-1001/gs-ss.sock \
//	GOSYNC_LIVE_REF_SOCKET=/var/sockets/nginx/av-sync-worker-2.sock \
//	GOSYNC_LIVE_TOKEN_FILE=~/.gosync-test-token-2 \
//	  go test ./internal/slidingsync/ -run LiveExtensions -v
//
// NOT read-only: `to_device` deletes on whichever side is asked for it, so only
// OUR side is asked. Point it at a test account.

func TestLiveExtensionsParity(t *testing.T) {
	ours := os.Getenv("GOSYNC_LIVE_SS_SOCKET")
	if ours == "" {
		t.Skip("GOSYNC_LIVE_SS_SOCKET not set; needs a running worker with sliding_sync enabled")
	}
	c, token := refClient(t)

	body := func(connID string) map[string]any {
		return map[string]any{
			"conn_id": connID,
			"lists": map[string]any{"all": map[string]any{
				"ranges": [][2]int{{0, 4}}, "required_state": [][2]string{{"m.room.name", ""}},
				"timeline_limit": 3,
			}},
			"extensions": map[string]any{
				"e2ee":         map[string]any{"enabled": true},
				"account_data": map[string]any{"enabled": true},
				"receipts":     map[string]any{"enabled": true},
				"typing":       map[string]any{"enabled": true},
				"io.element.msc4308.thread_subscriptions": map[string]any{"enabled": true, "limit": 20},
				"org.matrix.msc4354.sticky_events":        map[string]any{"enabled": true, "limit": 20},
			},
		}
	}

	var oursPos, refPos string
	for round := 1; round <= 2; round++ {
		o := postSliding(t, c, ours, token, body("ext-test-ours"), oursPos)
		r := postSliding(t, c, os.Getenv("GOSYNC_LIVE_REF_SOCKET"), token, body("ext-test-ref"), refPos)
		oursPos, refPos = o.Pos, r.Pos

		t.Run(roundName(round), func(t *testing.T) {
			for _, key := range extensionKeys(o.Extensions, r.Extensions) {
				a, aOK := o.Extensions[key]
				b, bOK := r.Extensions[key]
				switch {
				case !aOK && !bOK:
				case !aOK:
					t.Errorf("%s: absent from ours, present in the reference", key)
				case !bOK:
					t.Errorf("%s: present in ours, absent from the reference", key)
				default:
					compareExtension(t, key, a, b, round)
				}
			}
		})
	}
}

type slidingResponse struct {
	Pos        string                     `json:"pos"`
	Extensions map[string]json.RawMessage `json:"extensions"`
}

func postSliding(t *testing.T, c interface{}, sock, token string,
	body map[string]any, pos string) slidingResponse {
	t.Helper()
	raw := refRawTo(t, sock, token, body, pos)
	var parsed slidingResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("%s: %v", sock, err)
	}
	if parsed.Pos == "" {
		t.Fatalf("%s returned no pos: %s", sock, string(raw)[:200])
	}
	return parsed
}

func roundName(n int) string {
	if n == 1 {
		return "initial"
	}
	return "incremental"
}

func extensionKeys(a, b map[string]json.RawMessage) []string {
	seen := map[string]bool{}
	for k := range a {
		seen[k] = true
	}
	for k := range b {
		seen[k] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func compareExtension(t *testing.T, key string, ours, ref json.RawMessage, round int) {
	t.Helper()

	switch key {
	case "account_data":
		// The `global` list has no defined order: Synapse builds it by
		// iterating a dict, and ours sorts. Compared as a set; see
		// comparability.md source 12.
		compareAccountData(t, ours, ref)
		return

	case "typing":
		// Which rooms appear depends on the typing STREAM, which lives only in
		// memory: a worker that started a minute ago has not seen the clears
		// that Synapse's typing worker remembers from hours back. So an
		// initial request legitimately reports fewer rooms here until the view
		// fills in -- the same warm-up gap classic sync has, in a new place.
		// What must never differ is a room BOTH report.
		compareTyping(t, ours, ref, round)
		return

	case "org.matrix.msc4354.sticky_events":
		// `age` and the remaining sticky lifetime are recomputed per request on
		// each side (comparability.md source 11).
		if a, b := stripClock(t, ours), stripClock(t, ref); a != b {
			t.Errorf("%s differs:\n  ours %s\n  ref  %s", key, a, b)
		}
		return
	}

	if a, b := canonical(t, ours), canonical(t, ref); a != b {
		t.Errorf("%s differs:\n  ours %s\n  ref  %s", key, a, b)
	}
}

func compareAccountData(t *testing.T, ours, ref json.RawMessage) {
	t.Helper()
	type section struct {
		Global []json.RawMessage            `json:"global"`
		Rooms  map[string][]json.RawMessage `json:"rooms"`
	}
	var a, b section
	if err := json.Unmarshal(ours, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(ref, &b); err != nil {
		t.Fatal(err)
	}

	as, bs := canonicalSet(t, a.Global), canonicalSet(t, b.Global)
	if !reflect.DeepEqual(as, bs) {
		t.Errorf("account_data.global differs as a SET (not merely in order):\n"+
			"  ours-only %v\n  ref-only  %v", missing(as, bs), missing(bs, as))
	}

	for _, roomID := range extensionKeys(toRaw(a.Rooms), toRaw(b.Rooms)) {
		ra, okA := a.Rooms[roomID]
		rb, okB := b.Rooms[roomID]
		if okA != okB {
			t.Errorf("account_data.rooms[%s]: ours=%v ref=%v", roomID, okA, okB)
			continue
		}
		if !reflect.DeepEqual(canonicalSet(t, ra), canonicalSet(t, rb)) {
			t.Errorf("account_data.rooms[%s] differs", roomID)
		}
	}
}

func compareTyping(t *testing.T, ours, ref json.RawMessage, round int) {
	t.Helper()
	type section struct {
		Rooms map[string]json.RawMessage `json:"rooms"`
	}
	var a, b section
	if err := json.Unmarshal(ours, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(ref, &b); err != nil {
		t.Fatal(err)
	}
	for roomID, av := range a.Rooms {
		bv, ok := b.Rooms[roomID]
		if !ok {
			t.Errorf("typing.rooms[%s]: we report it, the reference does not", roomID)
			continue
		}
		if canonical(t, av) != canonical(t, bv) {
			t.Errorf("typing.rooms[%s] differs:\n  ours %s\n  ref  %s",
				roomID, canonical(t, av), canonical(t, bv))
		}
	}
	if extra := len(b.Rooms) - len(a.Rooms); extra > 0 {
		t.Logf("reference reports %d typing room(s) we do not: the replication "+
			"typing view has only been warm since this worker started", extra)
	}
}

func toRaw(m map[string][]json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(m))
	for k := range m {
		out[k] = nil
	}
	return out
}

func canonicalSet(t *testing.T, xs []json.RawMessage) []string {
	t.Helper()
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		out = append(out, canonical(t, x))
	}
	sort.Strings(out)
	return out
}

func missing(a, b []string) []string {
	in := map[string]bool{}
	for _, x := range b {
		in[x] = true
	}
	var out []string
	for _, x := range a {
		if !in[x] {
			out = append(out, x)
		}
	}
	return out
}

// canonical re-encodes with sorted keys so two equal objects compare equal.
func canonical(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func stripClock(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	v := decode(t, raw)
	dropClockDerived(v)
	return encodeJSON(t, v)
}
