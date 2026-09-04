package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// The severity of a request line decides whether anybody looks at it, and the
// pressure runs one way: a long poll exists to be abandoned, so an endpoint
// that logs every abandoned request at error level buries the ones that matter.
// The first real clients on sliding sync produced four such lines a second.
//
// The opposite mistake is worse, so both directions are asserted here.
func TestRequestLogSeverity(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		outcome string
		want    string
	}{
		{"a served request", http.StatusOK, "served", "info"},
		{
			// The caller walked away, so the status is whatever we had no
			// chance to deliver. Not a server fault.
			name: "abandoned by the caller", status: http.StatusInternalServerError,
			outcome: "client_gone", want: "debug",
		},
		{
			// The failure that must never be quiet.
			name: "a real server error", status: http.StatusInternalServerError,
			outcome: "error", want: "error",
		},
		{"a rejected request", http.StatusBadRequest, "refused", "warn"},
		{
			// M_UNRECOGNIZED is what a client probing for an endpoint gets, and
			// it is not a warning.
			name: "an unrecognised path", status: http.StatusNotFound,
			outcome: "refused", want: "info",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := zerolog.New(&buf).Level(zerolog.DebugLevel)

			h := WithRequestLog(log, http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					if ann := Annotate(r.Context()); ann != nil {
						ann.Outcome = tc.outcome
					}
					w.WriteHeader(tc.status)
				}))

			req := httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/sync", nil)
			h.ServeHTTP(httptest.NewRecorder(), req)

			var line struct {
				Level   string `json:"level"`
				Status  int    `json:"status"`
				Outcome string `json:"outcome"`
			}
			for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
				if raw == "" {
					continue
				}
				if err := json.Unmarshal([]byte(raw), &line); err != nil {
					t.Fatalf("log line %q: %v", raw, err)
				}
			}
			if line.Level != tc.want {
				t.Errorf("logged at %q, want %q (status %d, outcome %q)",
					line.Level, tc.want, line.Status, line.Outcome)
			}
		})
	}
}

// A CORS preflight is answered before any handler names the endpoint, so
// without naming it here it lands under "unknown" -- which then means two
// different things at once. On the live host preflights were 3,623 of 60,000
// requests, all for /sync: a phantom 10% on the dashboard, and cover for
// anything genuinely unrouted.
func TestPreflightIsNamed(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf).Level(zerolog.DebugLevel)

	h := WithRequestLog(log, WithCORS(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			t.Error("a preflight reached the handler")
		})))

	req := httptest.NewRequest(http.MethodOptions, "/_matrix/client/v3/sync", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rec.Code)
	}
	var line struct {
		Endpoint string `json:"endpoint"`
		Level    string `json:"level"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &line); err != nil {
		t.Fatalf("log line: %v", err)
	}
	if line.Endpoint != "preflight" {
		t.Errorf("endpoint = %q, want %q", line.Endpoint, "preflight")
	}
	if line.Level != "info" {
		t.Errorf("level = %q, want info: a preflight is a normal request", line.Level)
	}
}

// The counterpart: a path this worker really does not route must still be
// "unknown", or the label stops being able to tell us anything.
func TestAnUnroutedPathIsStillUnknown(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf).Level(zerolog.DebugLevel)

	h := WithRequestLog(log, WithCORS(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) })))

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/nope", nil))

	var line struct {
		Endpoint string `json:"endpoint"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &line); err != nil {
		t.Fatalf("log line: %v", err)
	}
	if line.Endpoint != "unknown" {
		t.Errorf("endpoint = %q, want unknown", line.Endpoint)
	}
}
