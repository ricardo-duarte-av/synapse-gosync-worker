package visibility

import (
	"errors"
	"testing"
)

func TestCheckAllowsLaxVisibility(t *testing.T) {
	cases := []struct {
		name      string
		ctx       Context
		isPeeking bool
		wantOK    bool
	}{
		{"shared, joined", Context{Visibility: Shared}, false, true},
		{"shared, peeking", Context{Visibility: Shared}, true, false},
		{"world_readable, joined", Context{Visibility: WorldReadable}, false, true},
		{"world_readable, peeking", Context{Visibility: WorldReadable}, true, true},
		{"invited", Context{Visibility: Invited}, false, false},
		{"joined", Context{Visibility: Joined}, false, false},
		// Synapse defaults to shared when the room has no such event.
		{"unset defaults to shared", Context{}, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.ctx.Check(tc.isPeeking)
			if (err == nil) != tc.wantOK {
				t.Errorf("Check = %v, want ok=%v", err, tc.wantOK)
			}
			if err != nil && !errors.Is(err, ErrNeedsPerEventState) {
				t.Errorf("error should wrap ErrNeedsPerEventState, got %v", err)
			}
		})
	}
}

// The fast path is only safe while the visibility that applied across the whole
// window is the one we can read now.
func TestCheckRefusesWhenVisibilityChanged(t *testing.T) {
	ctx := Context{Visibility: Shared, VisibilityEventCount: 2}
	if err := ctx.Check(false); err == nil {
		t.Fatal("expected a refusal: a permissive value now says nothing about the past")
	}
}

func TestCheckRefusesErasedSenderAndRetention(t *testing.T) {
	if err := (Context{Visibility: Shared, HasErasedSender: true}).Check(false); err == nil {
		t.Error("an erased sender needs the membership check Synapse falls through to")
	}
	if err := (Context{Visibility: Shared, HasRetentionPolicy: true}).Check(false); err == nil {
		t.Error("a retention policy needs server config this worker does not read")
	}
}

// Membership is deliberately NOT consulted on the fast path. "shared" means
// joined members see all history, including history from before they joined --
// an earlier version guarded on this and refused every room whose window
// reached past the user's own join.
func TestCheckIgnoresMembershipHistory(t *testing.T) {
	if err := (Context{Visibility: Shared}).Check(false); err != nil {
		t.Errorf("Check = %v, want nil regardless of when the user joined", err)
	}
}

func TestVisibleDropsWhatSynapseDrops(t *testing.T) {
	ctx := Context{Visibility: Shared, IgnoredSenders: map[string]bool{"@spam:e": true}}
	cases := []struct {
		name string
		ev   Event
		want bool
	}{
		{"ordinary message", Event{Type: "m.room.message", Sender: "@u:e"}, true},
		{"dummy event", Event{Type: "org.matrix.dummy_event", Sender: "@u:e"}, false},
		{"aliases are dropped wholesale", Event{Type: "m.room.aliases", Sender: "@u:e", IsState: true}, false},
		{"message from an ignored user", Event{Type: "m.room.message", Sender: "@spam:e"}, false},
		// Ignoring someone hides their messages, not their membership changes:
		// you still need to see them join, leave or be banned.
		{"state from an ignored user", Event{Type: "m.room.member", Sender: "@spam:e", IsState: true}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ctx.Visible(tc.ev); got != tc.want {
				t.Errorf("Visible = %v, want %v", got, tc.want)
			}
		})
	}
}
