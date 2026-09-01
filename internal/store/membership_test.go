package store

import "testing"

// MSC4115's unsigned.membership is the caller's membership at each event, not
// their membership now. Stamping the current one is right only for a user who
// has been joined since the room was created; in a room joined later, Synapse
// reports "leave" for everything before the join.
func TestMembershipAt(t *testing.T) {
	timeline := []MembershipChange{
		{StreamOrdering: 100, Membership: "invite"},
		{StreamOrdering: 200, Membership: "join"},
		{StreamOrdering: 400, Membership: "leave"},
		{StreamOrdering: 500, Membership: "join"},
	}
	cases := []struct {
		name  string
		order int64
		want  string
	}{
		{"before any membership event", 50, "leave"},
		{"at the invite", 100, "invite"},
		{"between invite and join", 150, "invite"},
		{"at the join", 200, "join"},
		{"while joined", 300, "join"},
		{"after leaving", 450, "leave"},
		{"after rejoining", 600, "join"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := MembershipAt(timeline, tc.order); got != tc.want {
				t.Errorf("MembershipAt(%d) = %q, want %q", tc.order, got, tc.want)
			}
		})
	}
}

// A user with no membership event at all is "leave": for visibility purposes,
// never having been in the room is indistinguishable from having left.
func TestMembershipAtEmptyTimeline(t *testing.T) {
	if got := MembershipAt(nil, 100); got != "leave" {
		t.Errorf("MembershipAt on an empty timeline = %q, want leave", got)
	}
}
