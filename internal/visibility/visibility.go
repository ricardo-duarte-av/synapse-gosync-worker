// Package visibility decides which events a user is allowed to see.
//
// This is the one place in the worker where a mistake serves private room
// history to somebody who should not have it, so it is deliberately narrow: it
// implements only the cases it can decide with certainty and refuses the rest,
// rather than approximating.
//
// # What is implemented, and what is refused
//
// Synapse's filter_events_for_client (synapse/visibility.py) runs two stages
// per event. The first, _check_filter_send_to_client, is implemented here in
// full: it needs no room state, only the room's retention policy and the
// caller's ignore list.
//
// The second stage resolves the room state *at each event* -- the history
// visibility then in force, and the user's membership then -- via state groups.
// That machinery does not exist yet (M1). But it has a fast path,
// _check_history_visibility, which decides without consulting membership at
// all:
//
//	visibility == world_readable            -> allowed
//	visibility == shared  && !is_peeking    -> allowed
//
// When the room's history visibility has never changed, the visibility at every
// event equals the current one, so that fast path can be taken for the whole
// window from current state alone. Anything else returns ErrNeedsPerEventState,
// which the handler turns into a 501 rather than a wrong answer.
//
// Note what is deliberately *not* checked on the fast path: the user's
// membership at each event. Synapse does not check it either -- `shared` means
// joined members see all history, including history from before they joined.
// An earlier version of this package guarded on membership changing inside the
// window, and it was simply wrong: it refused every room where the window
// reached back past the user's own join.
package visibility

import (
	"errors"
	"fmt"
)

// History visibility values, most permissive first (Synapse's
// VISIBILITY_PRIORITY order).
const (
	WorldReadable = "world_readable"
	Shared        = "shared"
	Invited       = "invited"
	Joined        = "joined"
)

// ErrNeedsPerEventState means the answer depends on state resolved at each
// event, which this package cannot do yet.
var ErrNeedsPerEventState = errors.New("visibility: this room needs per-event state resolution")

// Context is what is known about a room and caller, gathered once per request.
type Context struct {
	// Visibility is the room's current effective history visibility. Synapse
	// defaults to "shared" when the room has no m.room.history_visibility
	// event.
	Visibility string
	// VisibilityEventCount is how many m.room.history_visibility events the
	// room has ever had. More than one means the current value does not
	// describe the whole window, so the fast path is unsafe -- even if the
	// current value is permissive, because the old one may not have been.
	VisibilityEventCount int
	// HasErasedSender reports whether any event in the window was sent by a
	// user since erased (GDPR). Synapse then falls through to the membership
	// check even on the lax path, and prunes the event if the user was not
	// joined at the time.
	HasErasedSender bool
	// HasRetentionPolicy reports whether the room has a retention policy.
	// Applying one needs the server's retention config, which this worker
	// deliberately does not read (it lives in homeserver.yaml alongside
	// secrets), so such a room is refused.
	HasRetentionPolicy bool
	// IgnoredSenders is the caller's m.ignored_user_list.
	IgnoredSenders map[string]bool
}

// Event is the little this package needs to know about an event.
type Event struct {
	Type    string
	Sender  string
	IsState bool
}

// Check verifies the window can be decided at all, before any event is
// examined. A failure here is about the room, not about one event.
func (c Context) Check(isPeeking bool) error {
	if c.VisibilityEventCount > 1 {
		return fmt.Errorf("%w: history visibility changed %d times",
			ErrNeedsPerEventState, c.VisibilityEventCount)
	}
	if c.HasErasedSender {
		return fmt.Errorf("%w: an event sender has been erased", ErrNeedsPerEventState)
	}
	if c.HasRetentionPolicy {
		return fmt.Errorf("%w: the room has a retention policy", ErrNeedsPerEventState)
	}

	visibility := c.Visibility
	if visibility == "" {
		visibility = Shared // Synapse's default when the room has no such event.
	}
	switch {
	case visibility == WorldReadable:
		return nil
	case visibility == Shared && !isPeeking:
		return nil
	default:
		return fmt.Errorf("%w: history visibility is %q (peeking=%v)",
			ErrNeedsPerEventState, visibility, isPeeking)
	}
}

// Visible reports whether one event survives _check_filter_send_to_client.
//
// Call only after Check has succeeded for the window.
func (c Context) Visible(ev Event) bool {
	// A dummy event is padding Synapse inserts to advance the DAG. It is never
	// shown to anyone.
	if ev.Type == "org.matrix.dummy_event" {
		return false
	}
	// m.room.aliases is filtered out entirely: until MSC2261 lands, a malicious
	// alias event cannot be redacted, so Synapse drops the type wholesale.
	if ev.Type == "m.room.aliases" {
		return false
	}
	// Ignoring a user hides their messages but not their state changes -- you
	// still need to see them join, leave or be banned.
	if !ev.IsState && c.IgnoredSenders[ev.Sender] {
		return false
	}
	return true
}
