package store

import "testing"

// MSC4115's unsigned.membership is the caller's membership at each event, not
// their membership now. Stamping the current one is right only for a user who
// has been joined since the room was created; in a room joined later, Synapse
// reports "leave" for everything before the join.
func TestMembershipAt(t *testing.T) {
	timeline := []MembershipChange{
		{Topological: 1, Stream: 100, Membership: "invite"},
		{Topological: 2, Stream: 200, Membership: "join"},
		{Topological: 4, Stream: 400, Membership: "leave"},
		{Topological: 5, Stream: 500, Membership: "join"},
	}
	cases := []struct {
		name   string
		topo   int64
		stream int64
		want   string
	}{
		{"before any membership event", 0, 50, "leave"},
		{"at the invite", 1, 100, "invite"},
		{"between invite and join", 1, 150, "invite"},
		{"at the join", 2, 200, "join"},
		{"while joined", 3, 300, "join"},
		{"after leaving", 4, 450, "leave"},
		{"after rejoining", 6, 600, "join"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := MembershipAt(timeline, tc.topo, tc.stream); got != tc.want {
				t.Errorf("MembershipAt(%d,%d) = %q, want %q", tc.topo, tc.stream, got, tc.want)
			}
		})
	}
}

// stream_ordering is a server-local insertion counter, and backfilled events
// get NEGATIVE values. A room joined after it already had history stores its
// early state at around -23,964,688 while the user's own invite sits at
// +9,100,251 -- so ordering by stream puts the room's earlier history *after*
// events that follow it, and every backfilled event is reported with the
// membership the user had before they were ever invited.
func TestMembershipAtUsesTopologicalOrderNotStream(t *testing.T) {
	timeline := []MembershipChange{
		{Topological: 8, Stream: 9100251, Membership: "invite"},
		{Topological: 10, Stream: 9100427, Membership: "join"},
	}
	// Backfilled, so a hugely negative stream, but topologically after the
	// invite and before the join.
	if got := MembershipAt(timeline, 9, -23964685); got != "invite" {
		t.Errorf("backfilled event = %q, want invite", got)
	}
	// Backfilled and topologically before the invite.
	if got := MembershipAt(timeline, 4, -23964688); got != "leave" {
		t.Errorf("early backfilled event = %q, want leave", got)
	}
}

// A user with no membership event at all is "leave": for visibility purposes,
// never having been in the room is indistinguishable from having left.
func TestMembershipAtEmptyTimeline(t *testing.T) {
	if got := MembershipAt(nil, 1, 100); got != "leave" {
		t.Errorf("MembershipAt on an empty timeline = %q, want leave", got)
	}
}
