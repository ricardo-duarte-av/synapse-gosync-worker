package handlers

import "testing"

// A self-leave must reach rooms.leave on an incremental sync. Synapse's
// include_leave filter applies only to an initial sync; applying it here left
// the client believing it was still joined, so the room stayed in its room
// list and a later invite to the same room did not render.
func TestIncrementalSectionAlwaysReportsALeave(t *testing.T) {
	for _, membership := range []string{"leave", "ban"} {
		if got := incrementalSection(membership); got != "leave" {
			t.Errorf("incrementalSection(%q) = %q, want %q", membership, got, "leave")
		}
	}
}

func TestIncrementalSectionNamesTheOtherSections(t *testing.T) {
	cases := map[string]string{
		"invite":  "invite",
		"knock":   "knock",
		"join":    "",
		"":        "",
		"unknown": "",
	}
	for membership, want := range cases {
		if got := incrementalSection(membership); got != want {
			t.Errorf("incrementalSection(%q) = %q, want %q", membership, got, want)
		}
	}
}
