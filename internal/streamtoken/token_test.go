package streamtoken

import (
	"strings"
	"testing"
)

// Tokens observed on the live deployment (av-sync-worker-2) and in Synapse's
// own docstring. Round-tripping these byte-for-byte is the whole contract: a
// token we hand back must be one Synapse would accept, and one Synapse minted
// must survive a trip through us unchanged.
var realTokens = []string{
	// Synapse's docstring example, types/__init__.py:1148.
	"s2633508_17_338_6732159_1082514_541479_274711_265584_1_379_4242_4141_4343_4444",
	// Captured from /rooms/{id}/initialSync on av-sync-worker-2: the `end` of
	// a message chunk (live) and its `start` (topological).
	"s13908064_286691157_99270_25816942_1528560_1592_288994_40734453_0_1710_2_3583_5_69",
	"t12-13908046_286691157_99270_25816942_1528560_1592_288994_40734453_0_1710_2_3583_5_69",
}

func TestRoundTrip(t *testing.T) {
	for _, want := range realTokens {
		tok, err := Parse(want)
		if err != nil {
			t.Errorf("Parse(%q): %v", want, err)
			continue
		}
		if got := tok.String(); got != want {
			t.Errorf("round trip:\n got %q\nwant %q", got, want)
		}
	}
}

func TestParseFields(t *testing.T) {
	tok, err := Parse("s2633508_17_338_6732159_1082514_541479_274711_265584_1_379_4242_4141_4343_4444")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tok.Room.IsHistorical() {
		t.Error("room key should be live")
	}
	for _, c := range []struct {
		name string
		got  int64
		want int64
	}{
		{"room.stream", tok.Room.Stream, 2633508},
		{"presence", tok.Presence, 17},
		{"typing", tok.Typing, 338},
		{"receipt", tok.Receipt.Stream, 6732159},
		{"account_data", tok.AccountData, 1082514},
		{"push_rules", tok.PushRules, 541479},
		{"to_device", tok.ToDevice, 274711},
		{"device_list", tok.DeviceList.Stream, 265584},
		{"groups", tok.Groups, 1},
		{"un_partial_stated_rooms", tok.UnPartialStatedRooms, 379},
		{"thread_subscriptions", tok.ThreadSubscriptions, 4242},
		{"sticky_events", tok.StickyEvents, 4141},
		{"quarantined_media", tok.QuarantinedMedia.Stream, 4343},
		{"profile_updates", tok.ProfileUpdates, 4444},
	} {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
}

// Clients hold tokens across Synapse upgrades, and Synapse has added fields
// over time. from_string right-pads with "0"; so must we, or every client that
// slept through an upgrade gets a 400.
func TestShortTokenIsPadded(t *testing.T) {
	// Thirteen fields, from the docstring's example response.
	tok, err := Parse("s12_4_0_1_1_1_1_4_1_1_1_1_1")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tok.Room.Stream != 12 {
		t.Errorf("room.stream = %d", tok.Room.Stream)
	}
	if tok.ProfileUpdates != 0 {
		t.Errorf("padded field = %d, want 0", tok.ProfileUpdates)
	}
	if got, want := tok.String(), "s12_4_0_1_1_1_1_4_1_1_1_1_1_0"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

// A token from a *newer* Synapse carries fields we do not know. Truncating it
// would hand back a token that rewinds a stream the client had already
// advanced past, losing events silently. Refusing is the safe failure.
func TestOverlongTokenIsRejected(t *testing.T) {
	long := "s1_2_3_4_5_6_7_8_9_10_11_12_13_14_15"
	_, err := Parse(long)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "more than") {
		t.Errorf("error = %v", err)
	}
}

func TestWithRoomKeyLeavesOtherStreamsAlone(t *testing.T) {
	const orig = "s13908064_286691157_99270_25816942_1528560_1592_288994_40734453_0_1710_2_3583_5_69"
	tok, err := Parse(orig)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := tok.WithRoomKey(Historical(12, 13908046)).String()
	want := "t12-13908046_286691157_99270_25816942_1528560_1592_288994_40734453_0_1710_2_3583_5_69"
	if got != want {
		t.Errorf("WithRoomKey:\n got %q\nwant %q", got, want)
	}
}

func TestParseRejects(t *testing.T) {
	cases := []struct{ name, token string }{
		{"empty", ""},
		{"bad room prefix", "x1_0_0_0_0_0_0_0_0_0_0_0_0_0"},
		{"non-numeric room", "sabc_0_0_0_0_0_0_0_0_0_0_0_0_0"},
		{"topological without dash", "t12_0_0_0_0_0_0_0_0_0_0_0_0_0"},
		{"non-numeric presence", "s1_x_0_0_0_0_0_0_0_0_0_0_0_0"},
		{"malformed vector clock", "m5~2_0_0_0_0_0_0_0_0_0_0_0_0_0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Parse(tc.token); err == nil {
				t.Errorf("Parse(%q) succeeded, want an error", tc.token)
			}
		})
	}
}
