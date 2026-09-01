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

	fmt.Printf("\n%d matched, %d mismatched, %d skipped", matched, mismatched, skipped)
	if tolerated > 0 {
		fmt.Printf(" (%d tolerated upstream-only fields: Synapse's event-cache leaking prev_content)", tolerated)
	}
	fmt.Println()
	if mismatched > 0 {
		os.Exit(1)
	}
}

// tolerated counts upstream-only fields accepted by isToleratedUpstreamOnly.
var tolerated int

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
	".state":        "event_id",
	".presence":     "",
	".receipts":     "",
	".account_data": "",
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
	if !strings.HasPrefix(path, ".state[") {
		return false
	}
	for _, suffix := range []string{
		".prev_content", ".unsigned.prev_content", ".unsigned.prev_sender",
	} {
		if strings.HasSuffix(path, suffix) {
			return true
		}
	}
	return false
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
		if key, isSet := setPaths[path]; isSet {
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
		if fmt.Sprint(got) != fmt.Sprint(want) {
			*out = append(*out, fmt.Sprintf("%s: ours=%s synapse=%s", path, brief(got), brief(want)))
		}
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
			if ev, ok := item.(map[string]any); ok {
				if id, ok := ev[key].(string); ok {
					m[id] = ev
					continue
				}
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
