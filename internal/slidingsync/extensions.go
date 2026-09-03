package slidingsync

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/clientevent"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/eventfilter"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/pushrules"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/slidingstore"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/streamtoken"
)

// The extension section: the parts of a sync that are not rooms.
//
// Ported from SlidingSyncExtensionHandler. Seven are implemented, which is
// every one Synapse implements -- docs/decisions.md records why MSC4262
// (profiles) and MSC4360 (threads) are not among them.
//
// Every extension is OFF unless the request enables it. That is the MSC's
// design and it is what makes the endpoint cheap: a client that only wants a
// room list pays for nothing else.

// extensionInputs is what building the extension section needs from the rest of
// the response.
type extensionInputs struct {
	UserID   string
	DeviceID string
	Request  *Request

	From  *streamtoken.Token
	Now   streamtoken.Token
	NowMS int64

	// Lists and Subscribed are what the response actually contains, which is
	// what the reserved `lists`/`rooms` extension keys select from.
	Lists      map[string]ListResult
	Subscribed map[string]bool
	// AllRooms is every room the client is interested in, INCLUDING those
	// outside the window. Sticky events are scoped to it rather than to the
	// window, because a sticky event in a room just off-screen still matters.
	AllRooms map[string]bool
	// Rooms is what was described this time, for the receipts extension's
	// "re-send receipts for a room sent from scratch" rule.
	Rooms map[string]*RoomResult
	// Membership supplies room versions for serialising sticky events.
	Membership map[string]store.SlidingRoom

	Previous *slidingstore.PerConnectionState
	New      *slidingstore.PerConnectionState
}

// buildExtensions assembles the extension section.
func buildExtensions(ctx context.Context, d Deps, in extensionInputs) (map[string]json.RawMessage, error) {
	out := map[string]json.RawMessage{}
	ext := in.Request.Extensions
	if ext == nil {
		return out, nil
	}

	if ext.ToDevice != nil && ext.ToDevice.Enabled {
		v, err := toDeviceExtension(ctx, d, in, ext.ToDevice)
		if err != nil {
			return nil, err
		}
		if err := set(out, "to_device", v); err != nil {
			return nil, err
		}
	}
	if ext.E2EE != nil && ext.E2EE.Enabled {
		v, err := e2eeExtension(ctx, d, in)
		if err != nil {
			return nil, err
		}
		if err := set(out, "e2ee", v); err != nil {
			return nil, err
		}
	}
	if ext.AccountData != nil && ext.AccountData.Enabled {
		v, err := accountDataExtension(ctx, d, in, &ext.AccountData.ExtensionScope)
		if err != nil {
			return nil, err
		}
		if err := set(out, "account_data", v); err != nil {
			return nil, err
		}
	}
	if ext.Receipts != nil && ext.Receipts.Enabled {
		v, err := receiptsExtension(ctx, d, in, &ext.Receipts.ExtensionScope)
		if err != nil {
			return nil, err
		}
		if err := set(out, "receipts", v); err != nil {
			return nil, err
		}
	}
	if ext.Typing != nil && ext.Typing.Enabled {
		v := typingExtension(d, in, &ext.Typing.ExtensionScope)
		if err := set(out, "typing", v); err != nil {
			return nil, err
		}
	}
	if ext.ThreadSubscriptions != nil && ext.ThreadSubscriptions.Enabled && d.MSC4308Enabled {
		v, err := threadSubscriptionsExtension(ctx, d, in, ext.ThreadSubscriptions)
		if err != nil {
			return nil, err
		}
		// Synapse omits this one entirely when there is nothing to say, unlike
		// the others, which are present whenever enabled.
		if v != nil {
			if err := set(out, "io.element.msc4308.thread_subscriptions", v); err != nil {
				return nil, err
			}
		}
	}
	if ext.StickyEvents != nil && ext.StickyEvents.Enabled && d.MSC4354Enabled {
		v, err := stickyEventsExtension(ctx, d, in, ext.StickyEvents)
		if err != nil {
			return nil, err
		}
		if v != nil {
			if err := set(out, "org.matrix.msc4354.sticky_events", v); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func set(out map[string]json.RawMessage, key string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	out[key] = body
	return nil
}

// relevantRooms resolves the reserved `lists` and `rooms` keys every
// room-scoped extension shares.
//
// `["*"]` -- the default -- means every list, or every room subscription. An
// EMPTY list means none, which is a different thing and the reason both are
// pointers in the request: a client that wants receipts for one list only sends
// `{"lists": ["that-one"], "rooms": []}`.
//
// Only rooms actually in the response are eligible. An extension that returned
// data for a room the client was not sent would be describing something it
// cannot place.
func relevantRooms(in extensionInputs, scope *ExtensionScope) map[string]bool {
	out := map[string]bool{}

	for _, roomID := range scope.Rooms {
		if roomID == Wildcard {
			for id := range in.Subscribed {
				out[id] = true
			}
			break
		}
		if in.Subscribed[roomID] {
			out[roomID] = true
		}
	}

	for _, listKey := range scope.Lists {
		if listKey == Wildcard {
			for _, l := range in.Lists {
				for _, op := range l.Ops {
					for _, roomID := range op.RoomIDs {
						out[roomID] = true
					}
				}
			}
			break
		}
		if l, ok := in.Lists[listKey]; ok {
			for _, op := range l.Ops {
				for _, roomID := range op.RoomIDs {
					out[roomID] = true
				}
			}
		}
	}
	return out
}

func sortedRoomIDs(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// --- to_device (MSC3885) ---

type toDeviceJSON struct {
	NextBatch string            `json:"next_batch"`
	Events    []json.RawMessage `json:"events"`
}

// toDeviceExtension serves and DELETES to-device messages.
//
// The deletion is the same one classic sync makes and the same package makes
// it: a worker that serves this section without deleting hands a device the
// same room keys for ever. With `to_device.enabled` false the section is
// omitted rather than served undeleted -- there is no third option.
func toDeviceExtension(
	ctx context.Context, d Deps, in extensionInputs, req *ToDeviceExtension,
) (*toDeviceJSON, error) {

	if in.DeviceID == "" || d.Inbox == nil {
		// Synapse answers with an empty section rather than omitting it, so a
		// client can tell "nothing for you" from "not supported".
		return &toDeviceJSON{
			NextBatch: strconv.FormatInt(in.Now.ToDevice, 10),
			Events:    []json.RawMessage{},
		}, nil
	}

	var since int64
	if req.Since != "" {
		n, err := strconv.ParseInt(req.Since, 10, 64)
		if err != nil {
			return nil, err
		}
		since = n
		if in.Now.ToDevice < since {
			// A `since` from the future, which Synapse warns about and answers
			// empty rather than treating as an error.
			return &toDeviceJSON{NextBatch: req.Since, Events: []json.RawMessage{}}, nil
		}
		// Everything at or below `since` has demonstrably reached the device.
		if _, err := d.Inbox.DeleteUpTo(ctx, in.UserID, in.DeviceID, since); err != nil {
			return nil, err
		}
	}

	limit := req.Limit
	if limit <= 0 || limit > 100 {
		// Synapse caps at 100 regardless of what was asked for.
		limit = 100
	}
	messages, next, err := d.Store.MessagesForDevice(
		ctx, in.UserID, in.DeviceID, since, in.Now.ToDevice, limit)
	if err != nil {
		return nil, err
	}
	events := make([]json.RawMessage, 0, len(messages))
	for _, m := range messages {
		events = append(events, m)
	}
	return &toDeviceJSON{NextBatch: strconv.FormatInt(next, 10), Events: events}, nil
}

// --- e2ee (MSC3884) ---

type e2eeJSON struct {
	DeviceOneTimeKeysCount   map[string]int   `json:"device_one_time_keys_count"`
	DeviceUnusedFallbackKeys []string         `json:"device_unused_fallback_key_types"`
	DeviceLists              *deviceListsJSON `json:"device_lists,omitempty"`
}

type deviceListsJSON struct {
	Changed []string `json:"changed"`
	Left    []string `json:"left"`
}

func e2eeExtension(ctx context.Context, d Deps, in extensionInputs) (*e2eeJSON, error) {
	out := &e2eeJSON{
		// Both are ALWAYS present, even when empty. Synapse's comment points at
		// element-android#3725: a client cannot tell "no keys" from "nothing
		// changed" if the field can be absent.
		DeviceOneTimeKeysCount:   map[string]int{},
		DeviceUnusedFallbackKeys: []string{},
	}

	if in.DeviceID != "" {
		counts, err := d.Store.DeviceKeyCounts(ctx, in.UserID, in.DeviceID)
		if err != nil {
			return nil, err
		}
		if counts.OneTimeKeys != nil {
			out.DeviceOneTimeKeysCount = counts.OneTimeKeys
		}
		if counts.UnusedFallbackKeyTypes != nil {
			out.DeviceUnusedFallbackKeys = counts.UnusedFallbackKeyTypes
		}
	}

	// Device list changes need a range, so an initial request has none. That is
	// not a gap: a client with no `pos` is about to fetch keys for everyone it
	// shares a room with anyway.
	if in.From != nil {
		roomIDs := sortedRoomIDs(in.AllRooms)
		changed, err := d.Store.DeviceListChanges(ctx, in.UserID, roomIDs,
			in.From.DeviceList.Stream, in.Now.DeviceList.MaxStreamPos())
		if err != nil {
			return nil, err
		}
		sort.Strings(changed)
		if changed == nil {
			changed = []string{}
		}
		out.DeviceLists = &deviceListsJSON{Changed: changed, Left: []string{}}
	}
	return out, nil
}

// --- typing (MSC3961) ---

type typingJSON struct {
	Rooms map[string]json.RawMessage `json:"rooms"`
}

// typingExtension reports who is typing, from the replication view.
//
// No connection tracking, deliberately, and Synapse says why: a typing
// notification times out in about thirty seconds, so a room that fell out of
// range for longer than that has nothing worth catching up on. Anything still
// live arrives when the room comes back.
func typingExtension(d Deps, in extensionInputs, scope *ExtensionScope) *typingJSON {
	out := &typingJSON{Rooms: map[string]json.RawMessage{}}
	if d.Replication == nil {
		return out
	}
	relevant := relevantRooms(in, scope)

	// Which rooms to report is a question about the typing STREAM, not about
	// who is typing now: Synapse asks its typing source for everything since
	// the client's typing key, and a room whose typists were cleared is a
	// change like any other. So a room reports `user_ids: []` -- nobody is
	// typing, and the client is being told so.
	//
	// An initial request asks from zero, which is every room that has ever had
	// a typist. Ours reported only rooms with someone typing NOW, and the
	// reference caught it on a request where two rooms should have carried an
	// empty list.
	var from int64
	if in.From != nil {
		from = in.From.Typing
	}
	changed := d.Replication.TypingChangedSince(from)

	for _, roomID := range changed {
		if !relevant[roomID] {
			continue
		}
		users := d.Replication.TypingIn(roomID)
		if users == nil {
			users = []string{}
		}
		body, err := json.Marshal(map[string]any{
			"type":    "m.typing",
			"content": map[string]any{"user_ids": users},
		})
		if err != nil {
			continue
		}
		out.Rooms[roomID] = body
	}
	return out
}

// --- account_data (MSC3959) ---

type accountDataJSON struct {
	Global []json.RawMessage            `json:"global"`
	Rooms  map[string][]json.RawMessage `json:"rooms"`
}

// accountDataExtension serves global and per-room account data.
//
// Per-room account data is tracked per connection, exactly as rooms are: a room
// the client has never been given gets ALL of its account data, one it has gets
// only what changed. The alternative -- sending everything every time -- is
// what makes an account-data extension expensive on a 654-room account.
func accountDataExtension(
	ctx context.Context, d Deps, in extensionInputs, scope *ExtensionScope,
) (*accountDataJSON, error) {

	out := &accountDataJSON{Global: []json.RawMessage{}, Rooms: map[string][]json.RawMessage{}}

	// Global. On an initial request everything; otherwise the changes.
	var global []store.AccountDataEntry
	var err error
	if in.From == nil {
		global, err = d.Store.GlobalAccountData(ctx, in.UserID, d.MSC3391Enabled)
	} else {
		global, err = d.Store.GlobalAccountDataSince(ctx, in.UserID,
			in.From.AccountData, in.Now.AccountData, d.MSC3391Enabled)
	}
	if err != nil {
		return nil, err
	}
	for _, e := range global {
		body, err := json.Marshal(map[string]any{"type": e.Type, "content": e.Content})
		if err != nil {
			return nil, err
		}
		out.Global = append(out.Global, body)
	}

	// m.push_rules is synthesised rather than stored, and it belongs in the
	// global section on an initial request or whenever the rules changed.
	if in.From == nil || in.From.PushRules < in.Now.PushRules {
		rules, err := pushRulesEvent(ctx, d, in.UserID)
		if err != nil {
			return nil, err
		}
		if rules != nil {
			out.Global = append(out.Global, rules)
		}
	}

	relevant := relevantRooms(in, scope)
	if len(relevant) == 0 {
		return out, nil
	}

	// Split by what this connection has already been told.
	var initial []string
	previously := map[string]int64{}
	live := map[string]bool{}
	for _, roomID := range sortedRoomIDs(relevant) {
		if in.From == nil {
			initial = append(initial, roomID)
			continue
		}
		status := in.Previous.AccountData.HaveSentRoom(roomID)
		switch status.Status {
		case slidingstore.FlagLive:
			live[roomID] = true
		case slidingstore.FlagPreviously:
			pos, err := strconv.ParseInt(status.LastToken, 10, 64)
			if err != nil {
				// An unreadable resume point: send everything rather than
				// resume from a position we cannot trust.
				initial = append(initial, roomID)
				continue
			}
			previously[roomID] = pos
		default:
			initial = append(initial, roomID)
		}
	}

	// Everything that changed since `from`, fetched once. It answers the live
	// rooms directly and tells us which rooms outside the window have pending
	// updates.
	var changedByRoom map[string][]store.AccountDataEntry
	if in.From != nil {
		changedByRoom, err = d.Store.RoomAccountDataSince(ctx, in.UserID,
			in.From.AccountData, in.Now.AccountData, d.MSC3391Enabled)
		if err != nil {
			return nil, err
		}
		for roomID := range live {
			if entries, ok := changedByRoom[roomID]; ok && len(entries) > 0 {
				if err := addRoomAccountData(out, roomID, entries); err != nil {
					return nil, err
				}
			}
		}
	}

	for roomID, from := range previously {
		entries, err := d.Store.RoomAccountDataSince(ctx, in.UserID, from, in.Now.AccountData, d.MSC3391Enabled)
		if err != nil {
			return nil, err
		}
		if e, ok := entries[roomID]; ok && len(e) > 0 {
			if err := addRoomAccountData(out, roomID, e); err != nil {
				return nil, err
			}
		}
	}

	if len(initial) > 0 {
		all, err := d.Store.AllRoomAccountData(ctx, in.UserID, d.MSC3391Enabled)
		if err != nil {
			return nil, err
		}
		for _, roomID := range initial {
			// An entry ALWAYS, even an empty one. A room being sent from
			// scratch is being described in full, and "this room has no
			// account data" is part of that description; a room merely
			// updated is omitted when nothing changed. Synapse draws the
			// line in the same place, and the reference returns `[]` here
			// where we first returned nothing at all.
			out.Rooms[roomID] = []json.RawMessage{}
			if entries, ok := all[roomID]; ok && len(entries) > 0 {
				if err := addRoomAccountData(out, roomID, entries); err != nil {
					return nil, err
				}
			}
		}
	}

	// Record what the connection now has.
	sent := append(append([]string{}, initial...), sortedRoomIDs(boolSet(previously))...)
	sort.Strings(sent)
	in.New.AccountData.RecordSentRooms(sent)

	// And which rooms have account data we did NOT send, so they resume from
	// here rather than looking up to date when they next come into range.
	if in.From != nil {
		var missing []string
		for roomID := range changedByRoom {
			if !relevant[roomID] {
				missing = append(missing, roomID)
			}
		}
		sort.Strings(missing)
		in.New.AccountData.RecordUnsentRooms(missing, strconv.FormatInt(in.From.AccountData, 10))
	}
	return out, nil
}

func addRoomAccountData(out *accountDataJSON, roomID string, entries []store.AccountDataEntry) error {
	for _, e := range entries {
		body, err := json.Marshal(map[string]any{"type": e.Type, "content": e.Content})
		if err != nil {
			return err
		}
		out.Rooms[roomID] = append(out.Rooms[roomID], body)
	}
	return nil
}

func boolSet[V any](m map[string]V) map[string]bool {
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}

// --- receipts (MSC3960) ---

type receiptsJSON struct {
	Rooms map[string]json.RawMessage `json:"rooms"`
}

// receiptsExtension serves read receipts, tracked per connection like account
// data.
//
// One rule is not about tracking at all: a room sent from scratch, or one whose
// timeline was expanded, has its receipts re-sent regardless of what the
// connection was told before. A client that has just been handed a screenful of
// timeline needs the read markers that go with it.
func receiptsExtension(
	ctx context.Context, d Deps, in extensionInputs, scope *ExtensionScope,
) (*receiptsJSON, error) {

	out := &receiptsJSON{Rooms: map[string]json.RawMessage{}}
	relevant := relevantRooms(in, scope)

	var initial []string
	previously := map[string]int64{}
	var live []string
	for _, roomID := range sortedRoomIDs(relevant) {
		if in.From == nil {
			initial = append(initial, roomID)
			continue
		}
		if room := in.Rooms[roomID]; room != nil && (room.Initial || room.UnstableExpandedTimeline) {
			initial = append(initial, roomID)
			continue
		}
		status := in.Previous.Receipts.HaveSentRoom(roomID)
		switch status.Status {
		case slidingstore.FlagLive:
			live = append(live, roomID)
		case slidingstore.FlagPreviously:
			pos, err := strconv.ParseInt(status.LastToken, 10, 64)
			if err != nil {
				initial = append(initial, roomID)
				continue
			}
			previously[roomID] = pos
		default:
			initial = append(initial, roomID)
		}
	}

	byRoom := map[string][]store.ReceiptRow{}

	if len(live) > 0 && in.From != nil {
		rows, err := d.Store.ReceiptsSince(ctx, live,
			in.From.Receipt.Stream, in.Now.Receipt.MaxStreamPos())
		if err != nil {
			return nil, err
		}
		for roomID, r := range rows {
			byRoom[roomID] = append(byRoom[roomID], r...)
		}
	}
	for roomID, from := range previously {
		rows, err := d.Store.ReceiptsSince(ctx, []string{roomID}, from, in.Now.Receipt.MaxStreamPos())
		if err != nil {
			return nil, err
		}
		for id, r := range rows {
			byRoom[id] = append(byRoom[id], r...)
		}
	}
	if len(initial) > 0 {
		// A room sent from scratch gets receipts for the events IN ITS
		// TIMELINE, plus the user's own wherever they point. Not every receipt
		// in the room: that list grows with the membership, and sending all of
		// it beside a three-event timeline is most of the response for none of
		// the value. Synapse notes this is in the spec.
		byEvent := map[string][]string{}
		for _, roomID := range initial {
			room := in.Rooms[roomID]
			if room == nil {
				continue
			}
			for _, ev := range room.Timeline {
				if id := gjson.GetBytes(ev, "event_id").String(); id != "" {
					byEvent[roomID] = append(byEvent[roomID], id)
				}
			}
		}
		rows, err := d.Store.ReceiptsForEvents(ctx, byEvent)
		if err != nil {
			return nil, err
		}
		for roomID, r := range rows {
			byRoom[roomID] = append(byRoom[roomID], r...)
		}

		// The user's own read position, wherever it points -- without it a
		// client cannot draw its own unread marker.
		mine, err := d.Store.ReceiptsForUserInRooms(ctx, in.UserID, initial, in.Now.Receipt.MaxStreamPos())
		if err != nil {
			return nil, err
		}
		for roomID, r := range mine {
			byRoom[roomID] = append(byRoom[roomID], r...)
		}
	}

	for _, roomID := range sortedRoomIDs(boolSet(byRoom)) {
		// The same renderer classic sync uses, which is what applies the
		// private-receipt rule: a m.read.private receipt belongs to its owner
		// and nobody else.
		body, err := eventfilter.ReceiptEvent(roomID, byRoom[roomID], in.UserID, true)
		if err != nil {
			return nil, err
		}
		if body == nil {
			continue
		}
		// The extension carries type and content, without the room_id the
		// legacy shape has -- the room is the map key.
		var full map[string]json.RawMessage
		if err := json.Unmarshal(body, &full); err != nil {
			return nil, err
		}
		delete(full, "room_id")
		trimmed, err := json.Marshal(full)
		if err != nil {
			return nil, err
		}
		out.Rooms[roomID] = trimmed
	}

	sent := append(append([]string{}, initial...), sortedRoomIDs(boolSet(previously))...)
	sort.Strings(sent)
	in.New.Receipts.RecordSentRooms(sent)

	// Only rooms we have previously sent receipts for need marking: a NEVER
	// room is handled correctly when it first comes into range, and a
	// PREVIOUSLY one already points where it should.
	if in.From != nil {
		var candidates []string
		for roomID, status := range in.Previous.Receipts.All() {
			if status.Status == slidingstore.FlagLive && !relevant[roomID] {
				candidates = append(candidates, roomID)
			}
		}
		sort.Strings(candidates)
		if len(candidates) > 0 {
			rows, err := d.Store.ReceiptsSince(ctx, candidates,
				in.From.Receipt.Stream, in.Now.Receipt.MaxStreamPos())
			if err != nil {
				return nil, err
			}
			changed := make([]string, 0, len(rows))
			for roomID := range rows {
				changed = append(changed, roomID)
			}
			sort.Strings(changed)
			in.New.Receipts.RecordUnsentRooms(changed, strconv.FormatInt(in.From.Receipt.Stream, 10))
		}
	}
	return out, nil
}

// --- thread subscriptions (MSC4308) ---

type threadSubscriptionsJSON struct {
	Subscribed   map[string]map[string]threadSubJSON   `json:"subscribed,omitempty"`
	Unsubscribed map[string]map[string]threadUnsubJSON `json:"unsubscribed,omitempty"`
	PrevBatch    *string                               `json:"prev_batch,omitempty"`
}

type threadSubJSON struct {
	Automatic bool  `json:"automatic"`
	BumpStamp int64 `json:"bump_stamp"`
}

type threadUnsubJSON struct {
	BumpStamp int64 `json:"bump_stamp"`
}

// threadSubscriptionsExtension reports which threads the user started or
// stopped following.
//
// Returns nil -- and so is omitted entirely -- when nothing changed, unlike the
// extensions above. That is Synapse's choice and worth matching: a client
// polling every thirty seconds would otherwise carry an empty object for ever.
func threadSubscriptionsExtension(
	ctx context.Context, d Deps, in extensionInputs, req *ThreadSubscriptionsExtension,
) (*threadSubscriptionsJSON, error) {

	var from int64
	if in.From != nil {
		from = in.From.ThreadSubscriptions
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}

	updates, err := d.Store.ThreadSubscriptionsSince(
		ctx, in.UserID, from, in.Now.ThreadSubscriptions, limit)
	if err != nil {
		return nil, err
	}
	if len(updates) == 0 {
		return nil, nil
	}

	out := &threadSubscriptionsJSON{}
	for _, u := range updates {
		if u.Subscribed {
			if out.Subscribed == nil {
				out.Subscribed = map[string]map[string]threadSubJSON{}
			}
			if out.Subscribed[u.RoomID] == nil {
				out.Subscribed[u.RoomID] = map[string]threadSubJSON{}
			}
			out.Subscribed[u.RoomID][u.ThreadRoot] = threadSubJSON{
				Automatic: u.Automatic, BumpStamp: u.StreamID,
			}
			continue
		}
		if out.Unsubscribed == nil {
			out.Unsubscribed = map[string]map[string]threadUnsubJSON{}
		}
		if out.Unsubscribed[u.RoomID] == nil {
			out.Unsubscribed[u.RoomID] = map[string]threadUnsubJSON{}
		}
		out.Unsubscribed[u.RoomID][u.ThreadRoot] = threadUnsubJSON{BumpStamp: u.StreamID}
	}

	// Filling the limit means there may be more behind, and the client is told
	// where to paginate from. The minus one is because the bound is inclusive
	// and the oldest returned update has already been seen.
	if len(updates) == limit {
		prev := "ts" + strconv.FormatInt(updates[0].StreamID-1, 10)
		out.PrevBatch = &prev
	}
	return out, nil
}

// --- sticky events (MSC4354) ---

type stickyEventsJSON struct {
	NextBatch string                    `json:"next_batch"`
	Rooms     map[string]stickyRoomJSON `json:"rooms,omitempty"`
}

type stickyRoomJSON struct {
	Events []json.RawMessage `json:"events"`
}

// stickyEventsExtension serves events that linger for a while.
//
// Two things separate it from the room-scoped extensions. It is scoped to every
// room the client is interested in rather than to the window, because a sticky
// event in a room just off-screen still matters. And history visibility is NOT
// applied: MSC4354 says any joined user may see a sticky event for as long as
// it is sticky, so the events bypass the filter rather than being checked by it.
//
// Events already in a room's timeline are dropped here: the client learns about
// them either way, and duplicating them is exactly the spam the MSC worries
// about.
func stickyEventsExtension(
	ctx context.Context, d Deps, in extensionInputs, req *StickyEventsExtension,
) (*stickyEventsJSON, error) {

	// The token is `sticky_<n>`, prefixed so it cannot be confused with the
	// other tokens a sliding sync response carries -- the thread-subscriptions
	// one is `ts<n>` and the room key is `s<n>`, which are otherwise easy to
	// swap by accident. Ours emitted a bare `s<n>` first and the reference
	// caught it.
	var from int64
	if req.SinceSet && req.Since != "" {
		raw, ok := strings.CutPrefix(req.Since, stickyTokenPrefix)
		if !ok {
			return nil, fmt.Errorf("sticky events since token %q is not %s<n>",
				req.Since, stickyTokenPrefix)
		}
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("sticky events since token %q: %w", req.Since, err)
		}
		from = n
	}
	limit := req.Limit
	if limit <= 0 || limit > stickyMaxEventsInSync {
		limit = stickyMaxEventsInSync
	}

	roomIDs := sortedRoomIDs(in.AllRooms)
	to, byRoom, err := d.Store.StickyEvents(ctx, roomIDs, from, in.Now.StickyEvents, in.NowMS, limit)
	if err != nil {
		return nil, err
	}

	out := &stickyEventsJSON{NextBatch: stickyTokenPrefix + strconv.FormatInt(to, 10)}
	if len(byRoom) == 0 {
		// Omitted entirely rather than sent empty. Synapse's encoder tests the
		// extension for truthiness, and an extension with no rooms is falsy --
		// so a client polling a server with no sticky events never sees the
		// key at all.
		return nil, nil
	}

	var wanted []string
	for _, ids := range byRoom {
		wanted = append(wanted, ids...)
	}
	if len(wanted) == 0 {
		return out, nil
	}

	out.Rooms = map[string]stickyRoomJSON{}
	for _, roomID := range sortedRoomIDs(boolSet(byRoom)) {
		room := in.Rooms[roomID]
		inTimeline := map[string]bool{}
		if room != nil {
			for _, ev := range room.Timeline {
				inTimeline[gjson.GetBytes(ev, "event_id").String()] = true
			}
		}

		var ids []string
		for _, id := range byRoom[roomID] {
			if !inTimeline[id] {
				ids = append(ids, id)
			}
		}
		if len(ids) == 0 {
			continue
		}

		version := ""
		if r, ok := in.Membership[roomID]; ok {
			version = r.RoomVersion
		}
		events, err := d.Store.EventsByID(ctx, ids, version)
		if err != nil {
			return nil, err
		}

		loaded := make([]store.TimelineEvent, 0, len(ids))
		always := make(map[string]bool, len(ids))
		for _, id := range ids {
			ev, ok := events[id]
			if !ok {
				// The sticky row outlived its event, which a purge can do.
				continue
			}
			loaded = append(loaded, store.TimelineEvent{
				Stored: ev.Stored, Sender: ev.Sender, StateKey: ev.StateKey,
			})
			always[id] = true
		}

		// Every sticky event bypasses the visibility decision, because MSC4354
		// says so: "History visibility checks MUST NOT be applied to sticky
		// events. Any joined user is authorised to see sticky events for the
		// duration they remain sticky."
		//
		// It still goes THROUGH the filter rather than around it, and that is
		// the point: the filter is what works out the caller's membership at
		// each event, which MSC4115 puts in `unsigned`. Serialising these
		// directly loses that field, and the reference caught it.
		filtered, err := eventfilter.ForClient(ctx, d.Store, roomID, in.UserID,
			loaded, false, in.NowMS, always)
		if err != nil {
			return nil, err
		}

		redactions, err := d.Store.Redactions(ctx, ids)
		if err != nil {
			return nil, err
		}

		cfg := d.EventConfigAdmin(in.UserID, in.DeviceID, 0, filtered.AdminMetadata)
		var rendered []json.RawMessage
		for i, ev := range filtered.Events {
			stored := ev.Stored
			if i < len(filtered.Memberships) {
				stored.Membership = filtered.Memberships[i]
			}
			if err := eventfilter.AttachRedaction(&stored, redactions, in.NowMS, cfg); err != nil {
				return nil, err
			}
			body, err := clientevent.Serialize(stored, in.NowMS, cfg)
			if err != nil {
				return nil, err
			}
			rendered = append(rendered, body)
		}
		if len(rendered) > 0 {
			out.Rooms[roomID] = stickyRoomJSON{Events: rendered}
		}
	}
	if len(out.Rooms) == 0 {
		// Everything was already in a timeline, so there is nothing to add.
		return nil, nil
	}
	return out, nil
}

// stickyMaxEventsInSync is Synapse's StickyEvent.MAX_EVENTS_IN_SYNC.
const stickyMaxEventsInSync = 100

// stickyTokenPrefix is the `sticky_` in `sticky_<n>`.
const stickyTokenPrefix = "sticky_"

// pushRulesEvent renders the synthesised m.push_rules account data event.
func pushRulesEvent(ctx context.Context, d Deps, userID string) (json.RawMessage, error) {
	rules, enabled, err := d.Store.PushRules(ctx, userID)
	if err != nil {
		return nil, err
	}
	content, err := pushrules.Format(userID, rules, enabled, d.PushRuleFeatures)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"type": "m.push_rules", "content": content})
}
