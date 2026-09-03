package replication

import (
	"reflect"
	"testing"

	"github.com/rs/zerolog"
)

// Rows captured from the live replication channel. Getting a shape wrong costs
// a missed wakeup, which is a client hanging until its timeout for news that
// already arrived.
func TestRowSubjects(t *testing.T) {
	cases := []struct {
		name      string
		stream    string
		row       string
		wantRooms []string
		wantUsers []string
	}{
		{
			name:   "events",
			stream: StreamEvents,
			row: `["ev",["$lHGx90YsHWSp0DWN1_OqFN7C_O2T6S3HOilmqX9ftkQ",` +
				`"!8DitJr99fJMIvafK4U42dkFtaeHdJkxgwOgFWWZWDoc","m.room.encrypted",` +
				`null,null,null,null,false,false]]`,
			wantRooms: []string{"!8DitJr99fJMIvafK4U42dkFtaeHdJkxgwOgFWWZWDoc"},
		},
		{
			name: "typing", stream: StreamTyping,
			row:       `["!NaLbfexyNIBQDbzGwB:bonifacelabs.ca",["@thedreadpirate:matrix.blackmoon.tv"]]`,
			wantRooms: []string{"!NaLbfexyNIBQDbzGwB:bonifacelabs.ca"},
		},
		{
			name: "receipts", stream: StreamReceipts,
			row: `["!8DitJr99fJMIvafK4U42dkFtaeHdJkxgwOgFWWZWDoc","m.read",` +
				`"@lda:unredacted.org","$RvOiv46F1AruFjqG07RV",null,{"ts":1788289726158}]`,
			wantRooms: []string{"!8DitJr99fJMIvafK4U42dkFtaeHdJkxgwOgFWWZWDoc"},
			wantUsers: []string{"@lda:unredacted.org"},
		},
		{
			name: "presence", stream: StreamPresence,
			row:       `["@dodoid:dodoid.com","online",1788289714840,1788289725798,0,null,true]`,
			wantUsers: []string{"@dodoid:dodoid.com"},
		},
		{
			name: "room account data", stream: StreamAccountData,
			row: `["@daedric:aguiarvieira.pt","!8DitJr99fJMIvafK4U42dkFtaeHdJkxgwOgFWWZWDoc",` +
				`"m.fully_read"]`,
			wantRooms: []string{"!8DitJr99fJMIvafK4U42dkFtaeHdJkxgwOgFWWZWDoc"},
			wantUsers: []string{"@daedric:aguiarvieira.pt"},
		},
		{
			name: "global account data", stream: StreamAccountData,
			row:       `["@daedric:aguiarvieira.pt",null,"m.push_rules"]`,
			wantUsers: []string{"@daedric:aguiarvieira.pt"},
		},
		{
			name: "to_device", stream: StreamToDevice,
			row:       `["@someone:example.com"]`,
			wantUsers: []string{"@someone:example.com"},
		},
		{
			// Sticky events are served in /sync, so this row has to wake the
			// room -- it was previously waking everybody instead.
			name: "sticky_events", stream: StreamStickyEvents,
			row:       `["!8DitJr99fJMIvafK4U42dkFtaeHdJkxgwOgFWWZWDoc","$stickyEventId"]`,
			wantRooms: []string{"!8DitJr99fJMIvafK4U42dkFtaeHdJkxgwOgFWWZWDoc"},
		},
		{
			name: "thread_subscriptions", stream: StreamThreadSubscriptions,
			row: `["@daedric:aguiarvieira.pt","!8DitJr99fJMIvafK4U42dkFtaeHdJkxgwOgFWWZWDoc",` +
				`"$threadRootEventId"]`,
			wantRooms: []string{"!8DitJr99fJMIvafK4U42dkFtaeHdJkxgwOgFWWZWDoc"},
			wantUsers: []string{"@daedric:aguiarvieira.pt"},
		},
		{
			name: "un_partial_stated_room", stream: StreamUnPartialStatedRoom,
			row:       `["!8DitJr99fJMIvafK4U42dkFtaeHdJkxgwOgFWWZWDoc"]`,
			wantRooms: []string{"!8DitJr99fJMIvafK4U42dkFtaeHdJkxgwOgFWWZWDoc"},
		},
		{
			name: "profile_updates", stream: StreamProfileUpdates,
			row:       `["@daedric:aguiarvieira.pt","update",["displayname"]]`,
			wantUsers: []string{"@daedric:aguiarvieira.pt"},
		},
		{
			// Its row names neither a room nor a user, and its position only
			// ever reaches a token. Left waking everybody on purpose: see the
			// note at the bottom of rowDetails.
			name: "quarantined_media stays global", stream: StreamQuarantinedMedia,
			row: `["example.com","abcdef",true]`,
		},
		{name: "unknown stream", stream: "caches", row: `["x",["y"],1]`},
		{name: "not an array", stream: StreamEvents, row: `{"a":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rooms, users := rowSubjects(tc.stream, tc.row)
			if !reflect.DeepEqual(rooms, tc.wantRooms) {
				t.Errorf("rooms = %v, want %v", rooms, tc.wantRooms)
			}
			if !reflect.DeepEqual(users, tc.wantUsers) {
				t.Errorf("users = %v, want %v", users, tc.wantUsers)
			}
		})
	}
}

func newTestSub() *Subscriber {
	return New(Config{}, zerolog.Nop(), nil)
}

// A typing row carries the WHOLE current set for a room, not a delta. Merging
// instead of replacing would leave people typing forever.
func TestTypingRowReplacesRatherThanMerges(t *testing.T) {
	s := newTestSub()
	s.setLive(true)

	s.updateTyping(`["!r:e",["@a:e","@b:e"]]`, 1)
	if got := s.TypingIn("!r:e"); len(got) != 2 {
		t.Fatalf("typing = %v, want two users", got)
	}
	s.updateTyping(`["!r:e",["@a:e"]]`, 1)
	if got := s.TypingIn("!r:e"); len(got) != 1 || got[0] != "@a:e" {
		t.Errorf("typing = %v, want only @a:e", got)
	}
	s.updateTyping(`["!r:e",[]]`, 1)
	if got := s.TypingIn("!r:e"); got != nil {
		t.Errorf("typing = %v, want nobody", got)
	}
}

// Typing exists only in memory, so losing the connection means we no longer
// know who is typing. Keeping a stale list would show a typist forever.
func TestTypingIsClearedWhenNotLive(t *testing.T) {
	s := newTestSub()
	s.setLive(true)
	s.updateTyping(`["!r:e",["@a:e"]]`, 1)
	s.setLive(false)
	if got := s.TypingIn("!r:e"); got != nil {
		t.Errorf("typing survived a disconnect: %v", got)
	}
	if s.Live() {
		t.Error("Live should be false")
	}
}

// Positions only ever move forward: a stale value must never drag a token
// backwards, which would ask a client to replay what it already has.
func TestPositionsOnlyAdvance(t *testing.T) {
	s := newTestSub()
	s.advance(StreamEvents, 100)
	s.advance(StreamEvents, 90)
	if got := s.Position(StreamEvents); got != 100 {
		t.Errorf("position = %d, want 100", got)
	}
	s.Seed(map[string]int64{StreamEvents: 50, StreamTyping: 7})
	if got := s.Position(StreamEvents); got != 100 {
		t.Errorf("a lower seed moved the position to %d", got)
	}
	if got := s.Position(StreamTyping); got != 7 {
		t.Errorf("typing seed = %d, want 7", got)
	}
}

func TestHandleParsesLiveCommands(t *testing.T) {
	s := newTestSub()
	s.handle(`RDATA typing av-edu-worker 129777 ["!r:e",["@a:e"]]`)
	if got := s.Position(StreamTyping); got != 129777 {
		t.Errorf("typing position = %d", got)
	}
	if got := s.TypingIn("!r:e"); len(got) != 1 {
		t.Errorf("typing = %v", got)
	}

	s.handle(`POSITION events av-event-persister-1 13927100 13927184`)
	if got := s.Position(StreamEvents); got != 13927184 {
		t.Errorf("events position = %d", got)
	}

	// Traffic for other workers shares the channel and must be ignored, not
	// treated as an error.
	s.handle(`LOCK_RELEASED ["av-event-persister-1","lock","!r:e"]`)
	s.handle(`REMOTE_SERVER_UP example.com`)
	s.handle(`PING 123`)
}

// All but the last row of a batch carries the literal "batch" instead of a
// position. Parsing it as a number and failing would drop the row's wakeup.
func TestBatchTokenIsAccepted(t *testing.T) {
	s := newTestSub()
	s.advance(StreamEvents, 500)
	s.handle(`RDATA events av-event-persister-1 batch ["ev",["$x","!r:e","m.room.message",null,null,null,null,false,false]]`)
	if got := s.Position(StreamEvents); got != 500 {
		t.Errorf("a batch row moved the position to %d", got)
	}
}

// TestTypingSerialsBoundWhatASyncReports is a regression test with a real cost
// behind it.
//
// An incremental sync must report a room's typists only when they have changed
// since the client last looked, which is what Synapse's ephemeral_by_room does
// by asking the typing source for rooms above since_token.typing_key. Reporting
// the CURRENT typists on every sync instead looks harmless and is not: a room
// with somebody typing makes every sync return immediately with the same event,
// the client stores next_batch and asks again, and the two spin as fast as the
// network allows for as long as the typing lasts. gomuks managed 35 requests a
// second against an otherwise idle account.
func TestTypingSerialsBoundWhatASyncReports(t *testing.T) {
	s := newTestSub()
	s.setLive(true)

	if rooms := s.TypingChangedSince(0); len(rooms) != 0 {
		t.Fatalf("a subscriber that has seen nothing reported %v", rooms)
	}

	s.updateTyping(`["!r:e",["@a:e"]]`, 5)

	if rooms := s.TypingChangedSince(4); len(rooms) != 1 || rooms[0] != "!r:e" {
		t.Errorf("TypingChangedSince(4) = %v, want the room", rooms)
	}
	// The client has now been told. Telling it again is the loop.
	if rooms := s.TypingChangedSince(5); len(rooms) != 0 {
		t.Errorf("TypingChangedSince(5) = %v, want nothing", rooms)
	}

	// Someone stops typing: that is a change too, and must be reported once.
	s.updateTyping(`["!r:e",[]]`, 9)
	if rooms := s.TypingChangedSince(5); len(rooms) != 1 {
		t.Errorf("TypingChangedSince(5) after a stop = %v, want the room", rooms)
	}
	if rooms := s.TypingChangedSince(9); len(rooms) != 0 {
		t.Errorf("TypingChangedSince(9) = %v, want nothing", rooms)
	}

	// Nothing is reported at all while the subscription is unhealthy, because
	// the typist list is not trustworthy then either.
	s.setLive(false)
	if rooms := s.TypingChangedSince(0); rooms != nil {
		t.Errorf("TypingChangedSince while not live = %v, want nothing", rooms)
	}
}

// TestTypingStoppingIsStillAChange: a room whose typists went to zero has
// still changed, and must be reported. Its serial moves like any other, and
// the empty m.typing event that results is the only thing that clears the
// indicator on a client -- there is no timeout at the other end.
func TestTypingStoppingIsStillAChange(t *testing.T) {
	s := newTestSub()
	s.setLive(true)

	s.updateTyping(`["!r:e",["@a:e"]]`, 5)
	s.updateTyping(`["!r:e",[]]`, 6)

	if got := s.TypingIn("!r:e"); got != nil {
		t.Errorf("TypingIn = %v, want nobody", got)
	}
	if rooms := s.TypingChangedSince(5); len(rooms) != 1 || rooms[0] != "!r:e" {
		t.Errorf("TypingChangedSince(5) = %v, want the room -- stopping is a change", rooms)
	}
}

// rowDetails carries the type and state key that rowSubjects discards. Cache
// invalidation depends on both: a membership change drops the room list of the
// event's STATE KEY, which for an invite is the person invited rather than the
// sender.
func TestRowDetailsCarriesTypeAndStateKey(t *testing.T) {
	row := `["ev",["$evid","!room:example.com","m.room.member","@invited:example.com",` +
		`null,null,"invite",false,false]]`
	d := rowDetails(StreamEvents, row)

	if d.Type != "m.room.member" {
		t.Errorf("Type = %q, want m.room.member", d.Type)
	}
	if d.StateKey != "@invited:example.com" {
		t.Errorf("StateKey = %q, want the invited user", d.StateKey)
	}
	if !reflect.DeepEqual(d.RoomIDs, []string{"!room:example.com"}) {
		t.Errorf("RoomIDs = %v", d.RoomIDs)
	}
}

// A message is not a state event, and must not be read as one -- a non-null
// state key would invalidate some unrelated user's room list.
func TestRowDetailsLeavesStateKeyEmptyForAMessage(t *testing.T) {
	row := `["ev",["$evid","!room:example.com","m.room.message",null,null,null,null,false,false]]`
	d := rowDetails(StreamEvents, row)

	if d.Type != "m.room.message" {
		t.Errorf("Type = %q", d.Type)
	}
	if d.StateKey != "" {
		t.Errorf("StateKey = %q, want empty for a non-state event", d.StateKey)
	}
}

// The scope label is what makes a stream waking everybody visible. If a row
// names a room or a user it must not be counted global, and if it names
// neither it must be -- that classification is the entire signal.
func TestRowScopeClassification(t *testing.T) {
	cases := []struct {
		name       string
		stream     string
		row        string
		wantGlobal bool
	}{
		{"sticky names a room", StreamStickyEvents,
			`["!r:example.com","$e"]`, false},
		{"profile update names a user", StreamProfileUpdates,
			`["@u:example.com","update",["displayname"]]`, false},
		{"quarantined media names neither", StreamQuarantinedMedia,
			`["example.com","abc",true]`, true},
		{"an unknown stream names neither", "some_future_stream",
			`["whatever"]`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := rowDetails(tc.stream, tc.row)
			global := len(d.RoomIDs) == 0 && len(d.UserIDs) == 0
			if global != tc.wantGlobal {
				t.Errorf("global = %v, want %v (rooms=%v users=%v)",
					global, tc.wantGlobal, d.RoomIDs, d.UserIDs)
			}
		})
	}
}

// The caches stream must never wake a sync. It carries Synapse's own cache
// invalidations, its position is in no stream token, and Synapse's own
// on_rdata does not notify for it -- but it names no room and no user, so
// reaching the notifier at all means waking every parked client, about once a
// second on a live server.
func TestSilentStreamsWakeNobody(t *testing.T) {
	rows := map[string]string{
		StreamCaches: `["get_destination_retry_timings",["c3.krbonne.net"],1788390297977]`,
		// [destination, user_id]: presence being forwarded to a remote server.
		// The user's own presence change arrives on the presence stream.
		StreamPresenceFederation: `["matrix.org","@daedric:aguiarvieira.pt"]`,
	}
	for stream, row := range rows {
		t.Run(stream, func(t *testing.T) {
			sub := newTestSub()
			rec := &recordingListener{}
			sub.listener = rec

			sub.handle("RDATA " + stream + " someworker 42 " + row)
			if rec.calls != 0 {
				t.Fatalf("%s woke the notifier %d times, want 0", stream, rec.calls)
			}
		})
	}

	// The guard must not be a blanket: a stream that should wake still does.
	sub := newTestSub()
	rec := &recordingListener{}
	sub.listener = rec
	sub.handle(`RDATA typing av-typing 42 ["!r:example.com",["@u:example.com"]]`)
	if rec.calls != 1 {
		t.Fatalf("typing woke the notifier %d times, want 1", rec.calls)
	}
}

// Every silent stream needs a stated reason, so the set cannot quietly grow
// into "streams somebody once found noisy".
func TestEverySilentStreamHasAReason(t *testing.T) {
	if len(silentStreams) == 0 {
		t.Fatal("no silent streams declared")
	}
	for stream, reason := range silentStreams {
		if reason == "" {
			t.Errorf("%s is silenced with no reason given", stream)
		}
	}
}

type recordingListener struct {
	calls  int
	stream string
	rooms  []string
	users  []string
}

func (r *recordingListener) OnStreamAdvance(stream string, _ int64, rooms, users []string) {
	r.calls++
	r.stream, r.rooms, r.users = stream, rooms, users
}
