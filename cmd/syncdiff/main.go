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
	"bytes"
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
	"strconv"
	"strings"
	"time"
)

func main() {
	var (
		goSocket   = flag.String("go-socket", "", "unix socket of the worker under test")
		refSocket  = flag.String("ref-socket", "", "unix socket of the reference Synapse sync worker")
		tokenFile  = flag.String("token-file", "", "file holding the test account's access token")
		rooms      = flag.String("rooms", "", "comma-separated room IDs; default is every joined room")
		limit      = flag.Int("limit", 10, "pagination limit to request")
		endpoint   = flag.String("endpoint", "room_initial_sync", "room_initial_sync | initial_sync | sync | incremental_sync | to_device")
		stateAfter = flag.Bool("state-after", false, "request MSC4222 org.matrix.msc4222.state_after")
		rewind     = flag.Int("rewind", 2000, "for incremental_sync: how far to rewind the room key to build a `since`")
		verbose    = flag.Bool("v", false, "print each compared response")
		filterJSON = flag.String("filter", "", "inline sync filter JSON, sent as ?filter=")
		toDevice   = flag.Int("to-device", 105, "for -endpoint to_device: how many messages to send to the account's own device. More than 100 exercises truncation")
		homeserver = flag.String("homeserver", "", "base URL of the homeserver, e.g. https://example.com; required by -to-device")
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

	if *endpoint == "to_device" {
		if *homeserver == "" {
			fmt.Fprintln(os.Stderr, "-endpoint to_device needs -homeserver")
			os.Exit(2)
		}
		res := compareToDevice(ctx, ours, ref, token, *homeserver, *toDevice, *verbose)
		printOne("/sync (to_device)", res)
		report(boolToInt(res.kind == resultMatch), boolToInt(res.kind == resultMismatch),
			boolToInt(res.kind == resultSkip))
		if res.kind == resultMismatch {
			os.Exit(1)
		}
		return
	}

	if *endpoint == "incremental_sync" {
		res := compareIncrementalSync(ctx, ours, ref, token, *rewind, *verbose, *stateAfter,
			*filterJSON)
		printOne("/sync (incremental)", res)
		report(boolToInt(res.kind == resultMatch), boolToInt(res.kind == resultMismatch),
			boolToInt(res.kind == resultSkip))
		if res.kind == resultMismatch {
			os.Exit(1)
		}
		return
	}

	if *endpoint == "sync" {
		res := compareSync(ctx, ours, ref, token, *limit, *verbose, *stateAfter, *filterJSON)
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
	if coinFlips > 0 {
		fmt.Printf("\n  %d state entries where two events collide on one state key (Synapse picks by Python set order, which is randomised per process)", coinFlips/2)
	}
	if tolerated > 0 {
		fmt.Printf("\n  %d tolerated cache-dependent fields (prev_content on state, receipt thread_id)", tolerated)
	}
	if liveDataSkew > 0 {
		fmt.Printf("\n  %d unpinnable live-data fields (presence timestamps: /initialSync reads presence with no stream bound)",
			liveDataSkew)
	}
	if knownGaps > 0 {
		fmt.Printf("\n  %d known gaps (m.typing before the replication view fills in; msc4354_sticky, not implemented)", knownGaps)
	}
	if tokenCarrySkew > 0 {
		fmt.Printf("\n  %d prev_batch tokens right in the room key, differing in carried streams (Synapse mutates its own now_token mid-response)",
			tokenCarrySkew)
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

// tokenCarrySkew counts prev_batch tokens that agree on the room key but not on
// the streams carried alongside it.
var tokenCarrySkew int

// knownGaps counts differences caused by something this worker cannot do yet,
// as opposed to something it does wrongly.
var knownGaps int

// isKnownGap matches a difference that is expected until a milestone lands.
//
// Two so far.
//
// m.typing is never persisted -- it lives in an in-memory counter on the typing
// worker and reaches other workers over replication -- so no query can produce
// it and no amount of care in this worker will, until it subscribes to the
// replication stream (M5).
//
// msc4354_sticky is the room's sticky-events section, which we do not build at
// all. It is hard to notice: a sticky event that is still recent enough to be
// in the room's timeline is REMOVED from this section by Synapse, so the
// section only appears once the event has aged out of the timeline -- or as
// soon as a filter excludes it from the timeline, which is how it was found.
// Note that implementing it will also move `next_batch`: Synapse rewrites the
// sticky field of its own now_token to the last row it returned.
//
// Counted and named rather than silently ignored: a gap that leaves no trace in
// the output is indistinguishable from a gap nobody remembered.
func isKnownGap(path string) bool {
	return strings.HasSuffix(path, ".ephemeral.events[m.typing]") ||
		strings.HasSuffix(path, ".msc4354_sticky")
}

// prevBatchDiffersOnlyOutsideRoomKey accepts a prev_batch whose room key is
// right but whose other thirteen stream positions are not.
//
// A prev_batch is `now_token.copy_and_replace(ROOM, ...)`, and Synapse MUTATES
// its own now_token while building the response -- `_generate_sync_entry_for_presence`
// and `_generate_sync_entry_for_to_device` both reassign
// `sync_result_builder.now_token` (handlers/sync.py:2529, :2158). So a room's
// prev_batch carries whatever those streams were at when that room was built,
// while next_batch carries their final values, and the two disagree within one
// response.
//
// Pinning cannot fix that: we are given one token and Synapse used several.
// Only the room key decides where pagination resumes, and that is still
// compared exactly -- a wrong room key is still a mismatch.
func prevBatchDiffersOnlyOutsideRoomKey(path string, got, want any) bool {
	if !strings.HasSuffix(path, ".timeline.prev_batch") &&
		!strings.HasSuffix(path, ".messages.start") {
		return false
	}
	g, gok := got.(string)
	w, wok := want.(string)
	if !gok || !wok {
		return false
	}
	gRoom, _, gFound := strings.Cut(g, "_")
	wRoom, _, wFound := strings.Cut(w, "_")
	return gFound && wFound && gRoom == wRoom
}

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
	".state.events":                          "event_id",
	".org.matrix.msc4222.state_after.events": "event_id",
	".account_data.events":                   "",
	// Keyed by type so a difference lands on the right EDU rather than
	// reporting the whole set as changed.
	".ephemeral.events": "type",
	".presence.events":  "sender",
	// device_lists is built from a Python set, so its order carries nothing.
	".device_lists.changed": "",
	".device_lists.left":    "",
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
	// `.state[...]` on the legacy endpoints, `.state.events[...]` on /sync, and
	// `.org.matrix.msc4222.state_after.events[...]` when a client opts into
	// MSC4222: the same shared-event-cache artefact reaches all three.
	if strings.Contains(path, ".state[") || strings.Contains(path, ".state.events[") ||
		strings.Contains(path, ".state_after.events[") {
		for _, suffix := range []string{
			".prev_content", ".unsigned.prev_content", ".unsigned.prev_sender",
		} {
			if strings.HasSuffix(path, suffix) {
				return true
			}
		}
	}
	if isReceiptThreadID(path) {
		return true
	}
	// device_lists.left is decided by reading unsigned.prev_content off
	// Synapse's in-memory event, which is present on a STATE event only when
	// some earlier reader polluted the shared cache. The same artefact as
	// prev_content itself, one step removed.
	return strings.HasPrefix(path, ".device_lists.left")
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

// isLeftOnlyDeviceLists matches a whole `device_lists` object that Synapse
// emitted and we did not, when its ONLY content is `left`.
//
// The same tolerance as `.device_lists.left`, reached by a different route: if
// `left` is all Synapse had to say, then omitting those entries omits the
// object with them, and the diff reports the missing key rather than the
// missing entries. Deliberately narrow -- a `device_lists` carrying `changed`
// is still a mismatch, because failing to name a user whose keys changed is
// how a client ends up unable to decrypt.
func isLeftOnlyDeviceLists(path string, want any) bool {
	if path != ".device_lists" {
		return false
	}
	obj, ok := want.(map[string]any)
	if !ok || len(obj) == 0 {
		return false
	}
	for k := range obj {
		if k != "left" {
			return false
		}
	}
	return true
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
				if isLeftOnlyDeviceLists(path+"."+k, wv) {
					tolerated++
					continue
				}
				if isKnownGap(path + "." + k) {
					knownGaps++
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
		if prevBatchDiffersOnlyOutsideRoomKey(path, got, want) {
			tokenCarrySkew++
			return
		}
		*out = append(*out, fmt.Sprintf("%s: ours=%s synapse=%s", path, brief(got), brief(want)))
	}
}

// compareAsSet matches entries by a key field when there is one, so a
// difference is reported against the right element rather than as a wholesale
// reordering.
func compareAsSet(path, key string, got, want []any, out *[]string) {
	// Some sets differ wholesale for a reason that is not a defect; check
	// before comparing rather than per entry, because an unkeyed set reports
	// no entries.
	if isToleratedUpstreamOnly(path) {
		tolerated++
		return
	}
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
	collisions := stateKeyCollisions(g, w)
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
			if isKnownGap(fmt.Sprintf("%s[%s]", path, id)) {
				knownGaps++
				continue
			}
			if collisions[id] {
				coinFlips++
				continue
			}
			*out = append(*out, fmt.Sprintf("%s: missing entry %s", path, id))
		case !inWant:
			if collisions[id] {
				coinFlips++
				continue
			}
			*out = append(*out, fmt.Sprintf("%s: extra entry %s", path, id))
		default:
			diff(fmt.Sprintf("%s[%s]", path, id), gv, wv, out)
		}
	}
}

// coinFlips counts differences Synapse itself decides at random.
var coinFlips int

// stateKeyCollisions finds state entries that disagree only in WHICH event was
// chosen for a state key both sides carry.
//
// _calculate_state can legitimately end up with two different events for the
// same (type, state_key): one from the state at the start of the timeline and
// one from the state at its end, with neither in the timeline itself to
// subtract. Synapse then builds `{event_id_to_state_key[e]: e for e in
// state_ids}` from a Python SET of event IDs, so which one survives depends on
// string hash order -- and Python randomises that per process.
//
// Measured, not assumed: for the two m.room.server_acl events this first
// appeared on, PYTHONHASHSEED 0,1,4,6,7 select the newer event and 2,3,5 the
// older. There is no answer to match here, only a coin to call, so the pair is
// counted by name rather than reported as a mismatch.
//
// The condition is deliberately narrow: BOTH sides must carry an entry for the
// same (type, state_key), and the two event IDs must differ. A state key only
// we emit, or only Synapse emits, is still a mismatch.
func stateKeyCollisions(g, w map[string]any) map[string]bool {
	keyOf := func(item any) (string, bool) {
		m := itemAsMap(item)
		typ, ok1 := m["type"].(string)
		sk, ok2 := m["state_key"].(string)
		if !ok1 || !ok2 {
			return "", false
		}
		return typ + "\x00" + sk, true
	}
	byKey := func(m map[string]any) map[string]string {
		out := map[string]string{}
		for id, item := range m {
			if k, ok := keyOf(item); ok {
				out[k] = id
			}
		}
		return out
	}
	gk, wk := byKey(g), byKey(w)
	out := map[string]bool{}
	for k, gid := range gk {
		wid, ok := wk[k]
		if ok && gid != wid {
			out[gid] = true
			out[wid] = true
		}
	}
	return out
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
	limit int, verbose, useStateAfter bool, filterJSON string) result {

	path := "/_matrix/client/v3/sync"
	q := url.Values{"timeout": {"0"}, "set_presence": {"offline"}}
	if useStateAfter {
		q.Set("org.matrix.msc4222.use_state_after", "true")
	}
	if filterJSON != "" {
		q.Set("filter", filterJSON)
	}

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

// compareIncrementalSync compares a /sync carrying a `since`.
//
// The `since` is built by rewinding the room key of a token Synapse just
// minted, rather than by taking two snapshots a moment apart. Back-to-back
// tokens produce an empty delta, which would prove nothing; rewinding a few
// thousand stream positions produces a response with real content in it --
// rooms that changed, rooms that did not, and membership transitions.
//
// Everything else is pinned exactly as for an initial sync: the reference
// answers first and its next_batch becomes our now.
func compareIncrementalSync(ctx context.Context, ours, ref *http.Client, token string,
	rewind int, verbose, useStateAfter bool, filterJSON string) result {

	path := "/_matrix/client/v3/sync"

	// A first request only to learn where the stream is.
	probe := url.Values{"timeout": {"0"}, "set_presence": {"offline"}}
	probeBody, status, err := get(ctx, ref, path+"?"+probe.Encode(), token)
	if err != nil || status != http.StatusOK {
		return result{resultSkip, fmt.Sprintf("probe failed: %v (status %d)", err, status)}
	}
	var probeResp map[string]any
	if err := json.Unmarshal(probeBody, &probeResp); err != nil {
		return result{resultSkip, fmt.Sprintf("probe body is not JSON: %v", err)}
	}
	current, ok := probeResp["next_batch"].(string)
	if !ok {
		return result{resultSkip, "probe returned no next_batch"}
	}
	since, err := rewindRoomKey(current, rewind)
	if err != nil {
		return result{resultSkip, err.Error()}
	}

	q := url.Values{"timeout": {"0"}, "set_presence": {"offline"}, "since": {since}}
	if useStateAfter {
		q.Set("org.matrix.msc4222.use_state_after", "true")
	}
	if filterJSON != "" {
		q.Set("filter", filterJSON)
	}
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
		// An incremental sync can legitimately contain no event at all, in
		// which case there is no clock to recover and none is needed.
		timeNow = 0
	}

	q.Set("_gosync_now", end)
	if timeNow != 0 {
		q.Set("_gosync_time_now", fmt.Sprint(timeNow))
	}
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
		fmt.Printf("--- since=%s ---\n%s\n", since, truncate(ourBody))
	}

	var diffs []string
	diff("", ourResp, refResp, &diffs)
	if len(diffs) == 0 {
		return result{resultMatch, ""}
	}
	return result{resultMismatch, strings.Join(diffs, "\n")}
}

// compareToDevice compares the to_device section, end to end and UNPINNED.
//
// It is the one comparison here that deliberately does not pin, because the pin
// hides the defect it is looking for. When more than 100 to-device messages are
// waiting, Synapse returns the first 100 and winds the to_device field of its
// now token back to the last one it sent, so that the client's next sync
// resumes mid-backlog instead of skipping the rest. Pin us to Synapse's
// already-wound token and a worker that never winds anything back computes the
// same window, returns the same hundred messages and reports the same
// next_batch: the bug is invisible for as long as we are only ever asked once,
// with an answer we were handed.
//
// So both sides find their own now token, and the check is made in two steps:
//
//  1. both sync from the same `since` and must return the same messages AND
//     the same to_device position in next_batch;
//  2. both then sync again from their OWN next_batch, and must return the same
//     remainder. A worker that did not wind its token back returns nothing
//     here, because it has already claimed to be past the backlog.
//
// Unpinning is sound because only to_device is compared, and neither side's
// answer depends on a clock or on where the event stream happens to be. It
// needs the messages to stop arriving, which is why the comparator sends them
// itself.
//
// Both sides delete as they go -- that is what makes step 2 meaningful -- so
// this runs against a test account's device and nothing else.
func compareToDevice(ctx context.Context, ours, ref *http.Client, token,
	homeserver string, n int, verbose bool) result {

	path := "/_matrix/client/v3/sync"
	probe := url.Values{"timeout": {"0"}, "set_presence": {"offline"}}

	probeBody, status, err := get(ctx, ref, path+"?"+probe.Encode(), token)
	if err != nil || status != http.StatusOK {
		return result{resultSkip, fmt.Sprintf("probe failed: %v (status %d)", err, status)}
	}
	var probeResp map[string]any
	if err := json.Unmarshal(probeBody, &probeResp); err != nil {
		return result{resultSkip, fmt.Sprintf("probe body is not JSON: %v", err)}
	}
	since, ok := probeResp["next_batch"].(string)
	if !ok {
		return result{resultSkip, "probe returned no next_batch"}
	}

	if err := sendToDevice(ctx, homeserver, token, n); err != nil {
		return result{resultSkip, fmt.Sprintf("sending to-device messages failed: %v", err)}
	}

	q := url.Values{"timeout": {"0"}, "set_presence": {"offline"}, "since": {since}}

	// Synapse first, here and in step two, and it matters. Both sides delete
	// what their `since` acknowledges, so whoever is asked SECOND sees an inbox
	// the first has already been through. In step two the two sides resume from
	// different tokens when the bug is present, and asking us first would have
	// us delete the very messages Synapse is about to be asked for -- turning
	// "we skipped the rest of the backlog" into two identical empty answers.
	refResp, res := syncOnce(ctx, ref, path+"?"+q.Encode(), token, "Synapse")
	if res != nil {
		return *res
	}
	ourResp, res := syncOnce(ctx, ours, path+"?"+q.Encode(), token, "we")
	if res != nil {
		return *res
	}

	var diffs []string
	diff(".to_device", ourResp["to_device"], refResp["to_device"], &diffs)

	ourNext, _ := ourResp["next_batch"].(string)
	refNext, _ := refResp["next_batch"].(string)
	ourPos, refPos := toDeviceField(ourNext), toDeviceField(refNext)
	if ourPos != refPos {
		diffs = append(diffs, fmt.Sprintf(
			".next_batch to_device position: we said %s, Synapse said %s", ourPos, refPos))
	}
	if verbose {
		fmt.Printf("--- to_device: since=%s, we resume at %s, Synapse at %s ---\n",
			since, ourPos, refPos)
	}

	// Step two: each side resumes from its own token.
	q.Set("since", refNext)
	refAgain, res := syncOnce(ctx, ref, path+"?"+q.Encode(), token, "Synapse")
	if res != nil {
		return *res
	}
	q.Set("since", ourNext)
	ourAgain, res := syncOnce(ctx, ours, path+"?"+q.Encode(), token, "we")
	if res != nil {
		return *res
	}
	diff(".resumed.to_device", ourAgain["to_device"], refAgain["to_device"], &diffs)

	if len(diffs) == 0 {
		return result{resultMatch, ""}
	}
	return result{resultMismatch, strings.Join(diffs, "\n")}
}

// syncOnce performs one sync and decodes it, or returns the result to report.
func syncOnce(ctx context.Context, client *http.Client, url, token, who string) (
	map[string]any, *result) {

	body, status, err := get(ctx, client, url, token)
	if err != nil {
		return nil, &result{resultSkip, fmt.Sprintf("%s: request failed: %v", who, err)}
	}
	if status != http.StatusOK {
		return nil, &result{resultMismatch, fmt.Sprintf("%s: returned %d: %s", who, status, truncate(body))}
	}
	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, &result{resultMismatch, fmt.Sprintf("%s: body is not JSON: %v", who, err)}
	}
	return resp, nil
}

// toDeviceField pulls the seventh underscore-separated field out of a stream
// token, which is where to_device lives.
func toDeviceField(tok string) string {
	parts := strings.Split(tok, "_")
	if len(parts) < 7 {
		return "?"
	}
	return parts[6]
}

// rewindRoomKey moves a token's room key back by n stream positions, leaving
// every other stream where it was.
//
// Only the room key is moved: rewinding the others would ask for a replay of
// account data, receipts and device lists as well, which is a different test.
func rewindRoomKey(tokenStr string, n int) (string, error) {
	room, rest, ok := strings.Cut(tokenStr, "_")
	if !ok || !strings.HasPrefix(room, "s") {
		return "", fmt.Errorf("cannot rewind %q: expected a live room key", tokenStr)
	}
	pos, err := strconv.ParseInt(room[1:], 10, 64)
	if err != nil {
		return "", fmt.Errorf("cannot rewind %q: %w", tokenStr, err)
	}
	if pos-int64(n) < 0 {
		return "", fmt.Errorf("cannot rewind %q by %d: would go negative", tokenStr, n)
	}
	return fmt.Sprintf("s%d_%s", pos-int64(n), rest), nil
}

// sendToDevice puts n to-device messages into the account's OWN device inbox,
// so that a comparison has something to compare.
//
// Without it the to_device section is vacuous. An incremental comparison builds
// its `since` by rewinding only the room key, so the to-device position of that
// `since` is already the current one and the window (since, now] is empty --
// both sides correctly return nothing, and a section that was never built would
// pass just as well as one that was.
//
// The account sends to itself, which needs no second account and no second
// token. It goes over the homeserver's public API rather than a worker socket
// deliberately: sendToDevice is not a sync endpoint, and nginx knows which
// worker should have it.
//
// This WRITES to the homeserver. Only ever point it at a test account.
func sendToDevice(ctx context.Context, hs, token string, n int) error {
	hs = strings.TrimSuffix(hs, "/")
	client := &http.Client{Timeout: 30 * time.Second}

	whoami, err := http.NewRequestWithContext(ctx, http.MethodGet,
		hs+"/_matrix/client/v3/account/whoami", nil)
	if err != nil {
		return err
	}
	whoami.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(whoami)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("whoami returned %d: %s", resp.StatusCode, truncate(body))
	}
	var who struct {
		UserID   string `json:"user_id"`
		DeviceID string `json:"device_id"`
	}
	if err := json.Unmarshal(body, &who); err != nil {
		return err
	}
	if who.DeviceID == "" {
		return fmt.Errorf("token has no device_id; to-device messages need one")
	}

	stamp := time.Now().UnixNano()
	for i := 1; i <= n; i++ {
		payload, err := json.Marshal(map[string]any{
			"messages": map[string]any{
				who.UserID: map[string]any{
					who.DeviceID: map[string]any{"syncdiff": i, "batch": stamp},
				},
			},
		})
		if err != nil {
			return err
		}
		url := fmt.Sprintf("%s/_matrix/client/v3/sendToDevice/pt.aguiarvieira.syncdiff/%d-%d",
			hs, stamp, i)
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("sendToDevice returned %d: %s", resp.StatusCode, truncate(body))
		}
	}
	return nil
}
