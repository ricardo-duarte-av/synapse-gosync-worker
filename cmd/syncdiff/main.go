// Command syncdiff compares this worker's answers against a real Synapse sync
// worker.
//
// It exists because `/sync` and its legacy relatives cannot be compared the way
// `/event` can. Asking two implementations the same question a second apart
// gives legitimately different answers: each snapshots the stream position at
// its own start, and each stamps `age` from its own clock. A naive A/B diff
// drowns in false positives.
//
// So syncdiff owns both sides of the conversation and pins the question:
//
//  1. Ask the reference worker. Read the `end` token it minted, and recover the
//     exact millisecond it used from any event: `origin_server_ts + unsigned.age`.
//  2. Ask this worker for the same window and the same instant, via
//     `_gosync_now` and `_gosync_time_now`.
//  3. Compare structurally -- ordered where the spec orders, set-wise where it
//     does not.
//
// See docs/comparability.md for the full reasoning and the list of divergence
// sources this does and does not neutralise.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

func main() {
	var (
		goSocket  = flag.String("go-socket", "", "unix socket of the worker under test")
		refSocket = flag.String("ref-socket", "", "unix socket of the reference Synapse sync worker")
		tokenFile = flag.String("token-file", "", "file holding the test account's access token")
		rooms     = flag.String("rooms", "", "comma-separated room IDs; default is every joined room")
		limit     = flag.Int("limit", 10, "pagination limit to request")
		endpoint  = flag.String("endpoint", "room_initial_sync", "room_initial_sync | initial_sync | sync")
		verbose   = flag.Bool("v", false, "print each compared response")
	)
	flag.Parse()

	if *goSocket == "" || *refSocket == "" || *tokenFile == "" {
		fmt.Fprintln(os.Stderr, "-go-socket, -ref-socket and -token-file are required")
		os.Exit(2)
	}
	raw, err := os.ReadFile(expandHome(*tokenFile))
	if err != nil {
		fmt.Fprintf(os.Stderr, "reading token: %v\n", err)
		os.Exit(1)
	}
	token := strings.TrimSpace(string(raw))

	ours := unixClient(*goSocket)
	ref := unixClient(*refSocket)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if *endpoint == "sync" {
		res := compareSync(ctx, ours, ref, token, *limit, *verbose)
		printOne("/sync", res)
		report(boolToInt(res.kind == resultMatch), boolToInt(res.kind == resultMismatch),
			boolToInt(res.kind == resultSkip))
		if res.kind == resultMismatch {
			os.Exit(1)
		}
		return
	}

	if *endpoint == "initial_sync" {
		res := compareInitialSync(ctx, ours, ref, token, *limit, *verbose)
		printOne("/initialSync", res)
		report(boolToInt(res.kind == resultMatch), boolToInt(res.kind == resultMismatch),
			boolToInt(res.kind == resultSkip))
		if res.kind == resultMismatch {
			os.Exit(1)
		}
		return
	}

	roomIDs := splitNonEmpty(*rooms)
	if len(roomIDs) == 0 {
		roomIDs, err = joinedRooms(ctx, ref, token)
		if err != nil {
			fmt.Fprintf(os.Stderr, "listing joined rooms: %v\n", err)
			os.Exit(1)
		}
	}
	if len(roomIDs) == 0 {
		fmt.Fprintln(os.Stderr, "no rooms to compare")
		os.Exit(1)
	}

	var matched, mismatched, skipped int
	for _, roomID := range roomIDs {
		res := compareRoomInitialSync(ctx, ours, ref, token, roomID, *limit, *verbose)
		switch res.kind {
		case resultMatch:
			matched++
			fmt.Printf("  ok        %s\n", roomID)
		case resultSkip:
			skipped++
			fmt.Printf("  skip      %s: %s\n", roomID, res.detail)
		default:
			mismatched++
			fmt.Printf("  MISMATCH  %s\n%s\n", roomID, indent(res.detail))
		}
	}

	report(matched, mismatched, skipped)
	if mismatched > 0 {
		os.Exit(1)
	}
}

func report(matched, mismatched, skipped int) {
	fmt.Printf("\n%d matched, %d mismatched, %d skipped", matched, mismatched, skipped)
	if tolerated > 0 {
		fmt.Printf("\n  %d tolerated cache-dependent fields (prev_content on state, receipt thread_id)", tolerated)
	}
	if liveDataSkew > 0 {
		fmt.Printf("\n  %d unpinnable live-data fields (presence timestamps: /initialSync reads presence with no stream bound)",
			liveDataSkew)
	}
	if clockSkewCount > 0 {
		fmt.Printf("\n  %d age-like fields within clock skew (max %dms; Synapse re-reads its clock per room)",
			clockSkewCount, clockSkewMaxMS)
	}
	fmt.Println()
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// compareInitialSync compares the whole-account snapshot.
//
// The pin is recovered the same way as for a single room, but from a nested
// path: the response has no top-level event list, so the clock comes from the
// first room that has any state.
func compareInitialSync(ctx context.Context, ours, ref *http.Client, token string,
	limit int, verbose bool) result {

	path := "/_matrix/client/r0/initialSync"
	q := url.Values{"limit": {fmt.Sprint(limit)}}

	refBody, status, err := get(ctx, ref, path+"?"+q.Encode(), token)
	if err != nil {
		return result{resultSkip, fmt.Sprintf("reference request failed: %v", err)}
	}
	if status != http.StatusOK {
		return result{resultSkip, fmt.Sprintf("reference returned %d: %s", status, truncate(refBody))}
	}
	var refResp map[string]any
	if err := json.Unmarshal(refBody, &refResp); err != nil {
		return result{resultSkip, fmt.Sprintf("reference body is not JSON: %v", err)}
	}

	end, ok := refResp["end"].(string)
	if !ok {
		return result{resultSkip, "reference response has no end token"}
	}
	timeNow, ok := recoverTimeNowFromRooms(refResp)
	if !ok {
		return result{resultSkip, "cannot recover the reference clock from any room"}
	}

	q.Set("_gosync_now", end)
	q.Set("_gosync_time_now", fmt.Sprint(timeNow))
	ourBody, status, err := get(ctx, ours, path+"?"+q.Encode(), token)
	if err != nil {
		return result{resultMismatch, fmt.Sprintf("our request failed: %v", err)}
	}
	if status != http.StatusOK {
		return result{resultMismatch, fmt.Sprintf("we returned %d: %s", status, truncate(ourBody))}
	}
	var ourResp map[string]any
	if err := json.Unmarshal(ourBody, &ourResp); err != nil {
		return result{resultMismatch, fmt.Sprintf("our body is not JSON: %v", err)}
	}
	if verbose {
		fmt.Printf("--- /initialSync ---\n%s\n", truncate(ourBody))
	}

	var diffs []string
	diff("", ourResp, refResp, &diffs)
	if len(diffs) == 0 {
		return result{resultMatch, ""}
	}
	return result{resultMismatch, strings.Join(diffs, "\n")}
}

func recoverTimeNowFromRooms(resp map[string]any) (int64, bool) {
	roomList, ok := resp["rooms"].([]any)
	if !ok {
		return 0, false
	}
	for _, item := range roomList {
		room, ok := item.(map[string]any)
		if !ok {
			continue
		}
		state, ok := room["state"].([]any)
		if !ok {
			continue
		}
		for _, e := range state {
			ev, ok := e.(map[string]any)
			if !ok {
				continue
			}
			ts, tsOK := ev["origin_server_ts"].(float64)
			unsigned, uOK := ev["unsigned"].(map[string]any)
			if !tsOK || !uOK {
				continue
			}
			if age, ok := unsigned["age"].(float64); ok {
				return int64(ts) + int64(age), true
			}
		}
	}
	return 0, false
}

// tolerated counts upstream-only fields accepted by isToleratedUpstreamOnly.
var tolerated int

// clockSkew tracks age-like fields accepted by isClockDerived, and the largest
// discrepancy seen.
var (
	clockSkewCount int
	clockSkewMaxMS int64
)

// clockSkewToleranceMS bounds how far an age-like field may differ before it is
// a mismatch rather than clock jitter.
//
// /initialSync re-reads the clock **per room** -- `time_now =
// self.clock.time_msec()` sits inside handle_room -- and once more at the end
// for presence. So Synapse's own response is not internally consistent: two
// rooms in one snapshot carry ages computed milliseconds apart. Pinning our
// clock to one instant therefore cannot match exactly, however the pin is
// chosen.
//
// One second is far tighter than any real defect. The failures this is meant to
// still catch are wrong `age_ts`, a stale stored `age` passed through, or a
// missing age entirely -- all of which are off by hours or years, not
// milliseconds.
const clockSkewToleranceMS = 1000

// isClockDerived reports whether a field is computed from the serialiser's
// wall clock, and so is bounded by clock skew alone.
func isClockDerived(path string) bool {
	for _, suffix := range []string{
		".age", ".unsigned.age",
		// MSC4354's sticky TTL is "time left", so it is computed from the
		// clock exactly like age is.
		".unsigned.msc4354_sticky_duration_ttl_ms",
	} {
		if strings.HasSuffix(path, suffix) {
			return true
		}
	}
	// A per-room presence block is read at the same instant as the room, so
	// its last_active_ago is bounded like an age. /initialSync's is not; see
	// isLiveData.
	return strings.HasSuffix(path, ".last_active_ago") && !strings.HasPrefix(path, ".presence[")
}

// liveDataSkew counts fields that cannot be pinned at all.
var liveDataSkew int

// isLiveData reports whether a field is read from a table with no stream bound,
// and so genuinely differs between two calls however they are pinned.
//
// /initialSync's presence block is the case. Synapse reads it with
// `get_new_events(from_key=None)` -- the *current* presence of everyone the
// caller shares a room with, not presence as of a token. On a server with
// active bots those timestamps move every second, so `last_active_ago` differs
// by however long elapsed between the two requests, which was up to nine
// minutes in one observed case where a user became active in between.
//
// Only the timestamp is untestable. Which users appear, their presence state,
// and their currently_active flag are all still compared, so a missing user or
// a wrong state is still a mismatch.
func isLiveData(path string) bool {
	return strings.HasPrefix(path, ".presence[") && strings.HasSuffix(path, ".last_active_ago")
}

// withinClockSkew records and accepts a small difference in an age-like field.
func withinClockSkew(path string, got, want any) bool {
	if !isClockDerived(path) {
		return false
	}
	g, gok := got.(float64)
	w, wok := want.(float64)
	if !gok || !wok {
		return false
	}
	delta := int64(g - w)
	if delta < 0 {
		delta = -delta
	}
	if delta > clockSkewToleranceMS {
		return false
	}
	clockSkewCount++
	if delta > clockSkewMaxMS {
		clockSkewMaxMS = delta
	}
	return true
}

type resultKind int

const (
	resultMatch resultKind = iota
	resultMismatch
	resultSkip
)

type result struct {
	kind   resultKind
	detail string
}

func compareRoomInitialSync(ctx context.Context, ours, ref *http.Client, token, roomID string,
	limit int, verbose bool) result {

	path := "/_matrix/client/r0/rooms/" + url.PathEscape(roomID) + "/initialSync"
	q := url.Values{"limit": {fmt.Sprint(limit)}}

	// Phase 1: the reference answer, which also defines the question.
	refBody, status, err := get(ctx, ref, path+"?"+q.Encode(), token)
	if err != nil {
		return result{resultSkip, fmt.Sprintf("reference request failed: %v", err)}
	}
	if status != http.StatusOK {
		return result{resultSkip, fmt.Sprintf("reference returned %d: %s", status, truncate(refBody))}
	}

	var refResp map[string]any
	if err := json.Unmarshal(refBody, &refResp); err != nil {
		return result{resultSkip, fmt.Sprintf("reference body is not JSON: %v", err)}
	}

	end, ok := digString(refResp, "messages", "end")
	if !ok {
		return result{resultSkip, "reference response has no messages.end"}
	}
	timeNow, ok := recoverTimeNow(refResp)
	if !ok {
		return result{resultSkip, "cannot recover the reference clock: no event carried origin_server_ts and unsigned.age"}
	}

	// Phase 2: the same window, the same instant.
	q.Set("_gosync_now", end)
	q.Set("_gosync_time_now", fmt.Sprint(timeNow))
	ourBody, status, err := get(ctx, ours, path+"?"+q.Encode(), token)
	if err != nil {
		return result{resultMismatch, fmt.Sprintf("our request failed: %v", err)}
	}
	if status != http.StatusOK {
		return result{resultMismatch, fmt.Sprintf("we returned %d: %s", status, truncate(ourBody))}
	}
	var ourResp map[string]any
	if err := json.Unmarshal(ourBody, &ourResp); err != nil {
		return result{resultMismatch, fmt.Sprintf("our body is not JSON: %v", err)}
	}

	if verbose {
		fmt.Printf("--- %s ---\n%s\n", roomID, truncate(ourBody))
	}

	var diffs []string
	diff("", ourResp, refResp, &diffs)
	if len(diffs) == 0 {
		return result{resultMatch, ""}
	}
	return result{resultMismatch, strings.Join(diffs, "\n")}
}

// recoverTimeNow reconstructs the millisecond the reference used.
//
// Synapse stamps `age = time_now - age_ts`, where age_ts is the event's
// origin_server_ts. The sum recovers time_now exactly, which is what makes
// `age` a checkable field rather than one the comparator has to ignore.
func recoverTimeNow(resp map[string]any) (int64, bool) {
	for _, key := range []string{"state"} {
		if list, ok := resp[key].([]any); ok {
			for _, item := range list {
				ev, ok := item.(map[string]any)
				if !ok {
					continue
				}
				ts, tsOK := ev["origin_server_ts"].(float64)
				unsigned, uOK := ev["unsigned"].(map[string]any)
				if !tsOK || !uOK {
					continue
				}
				age, ageOK := unsigned["age"].(float64)
				if !ageOK {
					continue
				}
				return int64(ts) + int64(age), true
			}
		}
	}
	return 0, false
}

// setPaths are response paths whose order carries no meaning. Synapse builds
// them from dicts and sets, so its order is an implementation detail; comparing
// them as sequences would report differences that are not differences.
//
// `messages.chunk` is deliberately absent: a timeline IS ordered, and a
// reordered timeline is a real bug.
var setPaths = map[string]string{
	".state": "event_id",
	// Keyed by the user so a per-entry difference is reported against the right
	// user -- and so last_active_ago can be compared field by field and pass
	// through the clock-skew tolerance, instead of the whole set differing.
	".presence": "content.user_id",
	// Keyed by room so a difference is reported against the right room and the
	// right field, rather than as one opaque "set differs".
	".receipts":     "room_id",
	".account_data": "",
	// /initialSync fans out over a dict of rooms, so its order is Synapse's
	// iteration order and means nothing. Matched on room_id.
	".rooms": "room_id",
	// /sync nests invite state under the room.
	".invite_state.events": "event_id",
	// /sync's state and account data blocks are unordered sets of events; its
	// timeline is not.
	".state.events":        "event_id",
	".account_data.events": "",
	".ephemeral.events":    "",
	".presence.events":     "sender",
}

// setPathFor maps a response path to its set-comparison rule, if it has one.
//
// An /initialSync per-room path such as `.rooms[!x:y].state` gets the same rule
// as a single room's `.state`, so the two endpoints agree on what is ordered
// and what is not.
func setPathFor(path string) (string, bool) {
	if key, ok := setPaths[path]; ok {
		return key, true
	}
	// A nested path such as `.rooms.join.!x:y.state.events` gets the same rule
	// as the bare section name, so the two endpoints and every room agree on
	// what is ordered and what is not. Matched by suffix because a room id
	// contains dots and colons and cannot be parsed out reliably.
	for suffix, key := range setPaths {
		if strings.HasSuffix(path, suffix) {
			return key, true
		}
	}
	return "", false
}

// toleratedUpstreamOnly are fields Synapse sometimes emits and we deliberately
// do not. They are reported separately rather than ignored: a tolerance that
// leaves no trace is indistinguishable from a bug nobody noticed.
//
// prev_content/prev_sender on STATE events is Synapse's shared-event-cache
// leaking. events_worker computes those fields for readers that asked for them
// and writes them into the cached event ("This mutates the cached event, but
// that's fine"); a later state read of that same cached event then carries
// them. Whether they appear depends on whether some other request happened to
// load the event first, so it is not reproducible. Emitting them where Synapse
// does not would still be our bug, and is still reported.
func isToleratedUpstreamOnly(path string) bool {
	// `.state[...]` on the legacy endpoints, `.state.events[...]` on /sync:
	// the same shared-event-cache artefact reaches both.
	if strings.Contains(path, ".state[") || strings.Contains(path, ".state.events[") {
		for _, suffix := range []string{
			".prev_content", ".unsigned.prev_content", ".unsigned.prev_sender",
		} {
			if strings.HasSuffix(path, suffix) {
				return true
			}
		}
	}
	return isReceiptThreadID(path)
}

// isReceiptThreadID matches a receipt's thread_id, which Synapse may or may not
// include depending on which endpoint warmed a shared cache.
//
// The two initialSync endpoints use different receipt queries -- the plural one
// selects thread_id and merges through ReceiptInRoom.merge_to_content, the
// singular one does not select it at all -- but `_get_linearized_receipts_for_rooms`
// is a @cachedList over `_get_linearized_receipts_for_room`, so a plural call
// populates the singular method's cache. When both endpoints are called with
// the same receipt token, whichever ran first decides the shape for both.
//
// Receipts advance constantly on a busy server, so the tokens usually differ
// and each endpoint runs its own query, which is what we mirror. The collision
// is rare but real, and tolerating it in one narrow key beats a comparator that
// flaps.
func isReceiptThreadID(path string) bool {
	return strings.HasPrefix(path, ".receipts[") && strings.HasSuffix(path, ".thread_id")
}

func diff(path string, got, want any, out *[]string) {
	if len(*out) > 40 {
		return
	}
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: we sent %T, Synapse sent an object", path, got))
			return
		}
		keys := map[string]bool{}
		for k := range w {
			keys[k] = true
		}
		for k := range g {
			keys[k] = true
		}
		for _, k := range sortedKeys(keys) {
			wv, inWant := w[k]
			gv, inGot := g[k]
			switch {
			case !inGot:
				if isToleratedUpstreamOnly(path + "." + k) {
					tolerated++
					continue
				}
				*out = append(*out, fmt.Sprintf("%s.%s: missing from our response (Synapse: %s)", path, k, brief(wv)))
			case !inWant:
				if isReceiptThreadID(path + "." + k) {
					tolerated++
					continue
				}
				*out = append(*out, fmt.Sprintf("%s.%s: we sent it, Synapse did not (%s)", path, k, brief(gv)))
			default:
				diff(path+"."+k, gv, wv, out)
			}
		}

	case []any:
		g, ok := got.([]any)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: we sent %T, Synapse sent a list", path, got))
			return
		}
		if key, isSet := setPathFor(path); isSet {
			compareAsSet(path, key, g, w, out)
			return
		}
		if len(g) != len(w) {
			*out = append(*out, fmt.Sprintf("%s: we sent %d entries, Synapse sent %d", path, len(g), len(w)))
			return
		}
		for i := range w {
			diff(fmt.Sprintf("%s[%d]", path, i), g[i], w[i], out)
		}

	default:
		if fmt.Sprint(got) == fmt.Sprint(want) {
			return
		}
		if withinClockSkew(path, got, want) {
			return
		}
		if isLiveData(path) {
			liveDataSkew++
			return
		}
		*out = append(*out, fmt.Sprintf("%s: ours=%s synapse=%s", path, brief(got), brief(want)))
	}
}

// compareAsSet matches entries by a key field when there is one, so a
// difference is reported against the right element rather than as a wholesale
// reordering.
func compareAsSet(path, key string, got, want []any, out *[]string) {
	if key == "" {
		gs, ws := canonicalStrings(got), canonicalStrings(want)
		if strings.Join(gs, "\n") != strings.Join(ws, "\n") {
			*out = append(*out, fmt.Sprintf("%s: set differs\n  ours:    %s\n  synapse: %s",
				path, strings.Join(gs, " "), strings.Join(ws, " ")))
		}
		return
	}
	index := func(list []any) map[string]any {
		m := map[string]any{}
		for _, item := range list {
			if id, ok := digString(itemAsMap(item), strings.Split(key, ".")...); ok {
				m[id] = item
				continue
			}
			m[canonical(item)] = item
		}
		return m
	}
	g, w := index(got), index(want)
	ids := map[string]bool{}
	for k := range g {
		ids[k] = true
	}
	for k := range w {
		ids[k] = true
	}
	for _, id := range sortedKeys(ids) {
		gv, inGot := g[id]
		wv, inWant := w[id]
		switch {
		case !inGot:
			*out = append(*out, fmt.Sprintf("%s: missing entry %s", path, id))
		case !inWant:
			*out = append(*out, fmt.Sprintf("%s: extra entry %s", path, id))
		default:
			diff(fmt.Sprintf("%s[%s]", path, id), gv, wv, out)
		}
	}
}

func itemAsMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func canonical(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

func canonicalStrings(list []any) []string {
	out := make([]string, 0, len(list))
	for _, item := range list {
		out = append(out, canonical(item))
	}
	sort.Strings(out)
	return out
}

func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func brief(v any) string {
	s := canonical(v)
	if len(s) > 160 {
		return s[:160] + "…"
	}
	return s
}

func digString(m map[string]any, path ...string) (string, bool) {
	var cur any = m
	for _, key := range path {
		obj, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		cur, ok = obj[key]
		if !ok {
			return "", false
		}
	}
	s, ok := cur.(string)
	return s, ok
}

func joinedRooms(ctx context.Context, client *http.Client, token string) ([]string, error) {
	body, status, err := get(ctx, client, "/_matrix/client/v3/joined_rooms", token)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("status %d: %s", status, truncate(body))
	}
	var resp struct {
		JoinedRooms []string `json:"joined_rooms"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	return resp.JoinedRooms, nil
}

func get(ctx context.Context, client *http.Client, path, token string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost"+path, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	return body, resp.StatusCode, err
}

func unixClient(socket string) *http.Client {
	return &http.Client{
		Timeout: 2 * time.Minute,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
	}
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func truncate(b []byte) string {
	if len(b) > 400 {
		return string(b[:400]) + "…"
	}
	return string(b)
}

func indent(s string) string {
	return "      " + strings.ReplaceAll(s, "\n", "\n      ")
}

func expandHome(path string) string {
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return home + path[1:]
		}
	}
	return path
}

func printOne(name string, res result) {
	switch res.kind {
	case resultMatch:
		fmt.Println("  ok        " + name)
	case resultSkip:
		fmt.Printf("  skip      %s: %s\n", name, res.detail)
	default:
		fmt.Printf("  MISMATCH  %s\n%s\n", name, indent(res.detail))
	}
}

// compareSync compares an initial /sync.
//
// set_presence=offline throughout: a sync marks the user online and emits
// USER_SYNC over replication, and a comparator must not perturb the deployment
// it is measuring.
func compareSync(ctx context.Context, ours, ref *http.Client, token string,
	limit int, verbose bool) result {

	path := "/_matrix/client/v3/sync"
	q := url.Values{"timeout": {"0"}, "set_presence": {"offline"}}

	refBody, status, err := get(ctx, ref, path+"?"+q.Encode(), token)
	if err != nil {
		return result{resultSkip, fmt.Sprintf("reference request failed: %v", err)}
	}
	if status != http.StatusOK {
		return result{resultSkip, fmt.Sprintf("reference returned %d: %s", status, truncate(refBody))}
	}
	var refResp map[string]any
	if err := json.Unmarshal(refBody, &refResp); err != nil {
		return result{resultSkip, fmt.Sprintf("reference body is not JSON: %v", err)}
	}
	end, ok := refResp["next_batch"].(string)
	if !ok {
		return result{resultSkip, "reference response has no next_batch"}
	}
	timeNow, ok := recoverTimeNowFromSync(refResp)
	if !ok {
		return result{resultSkip, "cannot recover the reference clock from any room"}
	}

	q.Set("_gosync_now", end)
	q.Set("_gosync_time_now", fmt.Sprint(timeNow))
	ourBody, status, err := get(ctx, ours, path+"?"+q.Encode(), token)
	if err != nil {
		return result{resultMismatch, fmt.Sprintf("our request failed: %v", err)}
	}
	if status != http.StatusOK {
		return result{resultMismatch, fmt.Sprintf("we returned %d: %s", status, truncate(ourBody))}
	}
	var ourResp map[string]any
	if err := json.Unmarshal(ourBody, &ourResp); err != nil {
		return result{resultMismatch, fmt.Sprintf("our body is not JSON: %v", err)}
	}
	if verbose {
		fmt.Printf("--- /sync ---\n%s\n", truncate(ourBody))
	}

	var diffs []string
	diff("", ourResp, refResp, &diffs)
	if len(diffs) == 0 {
		return result{resultMatch, ""}
	}
	return result{resultMismatch, strings.Join(diffs, "\n")}
}

// recoverTimeNowFromSync reconstructs the millisecond the reference used, from
// any event carrying both origin_server_ts and unsigned.age.
func recoverTimeNowFromSync(resp map[string]any) (int64, bool) {
	rooms, _ := resp["rooms"].(map[string]any)
	join, _ := rooms["join"].(map[string]any)
	for _, room := range join {
		r, _ := room.(map[string]any)
		for _, section := range []string{"state", "timeline"} {
			block, _ := r[section].(map[string]any)
			events, _ := block["events"].([]any)
			for _, e := range events {
				ev, _ := e.(map[string]any)
				ts, tsOK := ev["origin_server_ts"].(float64)
				unsigned, uOK := ev["unsigned"].(map[string]any)
				if !tsOK || !uOK {
					continue
				}
				if age, ok := unsigned["age"].(float64); ok {
					return int64(ts) + int64(age), true
				}
			}
		}
	}
	return 0, false
}
