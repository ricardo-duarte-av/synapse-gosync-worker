package handlers

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// clientGone decides whether a failed request is logged as an error or shrugged
// off, so it must be narrow. A long poll exists to be abandoned -- 4% of real
// sliding sync traffic ends that way -- but a deadline that passed is OURS, and
// silencing those would hide the failures the log exists for.
func TestClientGone(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"the caller hung up", context.Canceled, true},
		{"wrapped by a store method", fmt.Errorf("store: visibility extras: %w", context.Canceled), true},
		{"wrapped twice", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", context.Canceled)), true},

		// OURS, not theirs: a statement timeout or a context we bounded. These
		// are real failures and must still be reported.
		{"a deadline we set", context.DeadlineExceeded, false},
		{"a wrapped deadline", fmt.Errorf("store: %w", context.DeadlineExceeded), false},
		{"an ordinary error", errors.New("connection refused"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clientGone(tc.err); got != tc.want {
				t.Errorf("clientGone(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
