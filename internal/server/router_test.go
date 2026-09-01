package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Synapse answers an unimplemented endpoint with M_UNRECOGNIZED, not a bare
// 404. Clients probe for optional endpoints and need to tell "this server does
// not implement it" from "this resource does not exist".
func TestUnknownPathReturnsUnrecognized(t *testing.T) {
	mux := NewMux(Routes{})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/nonexistent", nil))

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v (%s)", err, rec.Body.String())
	}
	if body["errcode"] != "M_UNRECOGNIZED" {
		t.Errorf("errcode = %v, want M_UNRECOGNIZED", body["errcode"])
	}
}

func TestHealth(t *testing.T) {
	mux := NewMux(Routes{})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status = %q", body["status"])
	}
}

// Synapse registers the legacy endpoints under api/v1 as well as the modern
// version segments. A client pinned to api/v1 is exactly the kind of caller
// these endpoints still exist for.
func TestRoomInitialSyncRegisteredOnEveryLegacyVersion(t *testing.T) {
	var seen int
	routes := Routes{RoomInitialSync: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen++
		if got := r.PathValue("roomId"); got != "!abc:example.com" {
			t.Errorf("roomId = %q", got)
		}
		w.WriteHeader(http.StatusOK)
	})}
	mux := NewMux(routes)

	for _, v := range legacyVersions {
		path := "/_matrix/client/" + v + "/rooms/!abc:example.com/initialSync"
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status = %d, want 200", path, rec.Code)
		}
	}
	if seen != len(legacyVersions) {
		t.Errorf("handler ran %d times, want %d", seen, len(legacyVersions))
	}
}

// Nothing is registered unless a handler is supplied, so an endpoint that is
// not implemented yet reports M_UNRECOGNIZED rather than 500ing on a nil
// handler.
func TestUnimplementedEndpointIsNotRegistered(t *testing.T) {
	mux := NewMux(Routes{})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/_matrix/client/r0/rooms/!a:b/initialSync", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}
