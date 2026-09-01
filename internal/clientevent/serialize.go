package clientevent

import (
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Format selects the client event shape.
type Format int

const (
	// FormatV1 is the legacy `/events` and `/initialSync` shape: it keeps
	// `room_id`, adds `user_id` as a duplicate of `sender`, and copies six
	// fields out of `unsigned` to the top level.
	FormatV1 Format = iota
	// FormatV2 is the modern shape, keeping `room_id`.
	FormatV2
	// FormatV2NoRoomID is what `/sync` emits: v2 with `room_id` stripped, since
	// the room is already the key in the response.
	FormatV2NoRoomID
)

// v2DropKeys are the federation-only fields no client format carries.
// rust/src/events/serialize.rs:61.
var v2DropKeys = [...]string{
	"auth_events", "prev_events", "hashes", "signatures", "depth", "origin", "prev_state",
}

// v1CopyKeys are lifted from `unsigned` to the top level by the v1 format.
// rust/src/events/serialize.rs:72. The order matters only for readability;
// JSON objects are compared by key.
var v1CopyKeys = [...]string{
	"age", "redacted_because", "replaces_state", "prev_content",
	"invite_room_state", "knock_room_state",
}

// Requester is the caller an event is being serialised for.
//
// It decides one thing: whether `unsigned.transaction_id` is revealed. A
// transaction ID is the sender's own idempotency key, so it is shown back only
// to the session that produced it -- another client of the same user must not
// see it.
type Requester struct {
	UserID   string
	DeviceID string
	// TokenID is `access_tokens.id`, used only for events stored without a
	// device_id: old events, and those from appservices, guests or admin-API
	// tokens. Zero means unknown, in which case the fallback is skipped.
	TokenID int64
	IsGuest bool
	// AppServiceID is non-empty when the caller is an appservice. Synapse
	// cannot check which session an appservice used, so it assumes the same
	// one.
	AppServiceID string
}

// Config controls one serialisation.
type Config struct {
	Format    Format
	Requester Requester
	// IncludeStrippedRoomState keeps `unsigned.invite_room_state` and
	// `knock_room_state`. Off everywhere except the invite section of /sync.
	IncludeStrippedRoomState bool
}

// persistedUnsignedFields is the complete set of `unsigned` keys Synapse keeps
// when loading an event. rust/src/events/unsigned.rs.
var persistedUnsignedFields = [...]string{
	"age_ts", "replaces_state", "invite_room_state", "knock_room_state",
	"prev_content", "prev_sender",
}

// filterUnsigned returns the stored `unsigned` reduced to the allowlist.
func filterUnsigned(stored []byte) []byte {
	out := []byte(`{}`)
	unsigned := gjson.GetBytes(stored, "unsigned")
	if !unsigned.Exists() {
		return out
	}
	for _, key := range persistedUnsignedFields {
		v := unsigned.Get(key)
		if !v.Exists() {
			continue
		}
		// Keys are known constants, so SetRawBytes cannot fail on the path;
		// a malformed value would already have failed the gjson lookup.
		next, err := sjson.SetRawBytes(out, key, []byte(v.Raw))
		if err != nil {
			continue
		}
		out = next
	}
	return out
}

// Stored is an event as it comes out of the database.
type Stored struct {
	EventID string
	RoomID  string
	Type    string
	// JSON is `event_json.json` verbatim: the PDU as stored.
	JSON []byte
	// InternalMetadata is `event_json.internal_metadata` verbatim.
	InternalMetadata []byte
	RoomVersion      string
	// Membership is the sender's membership at this event, for MSC4115's
	// `unsigned.membership`. Empty to omit.
	Membership string
}

// Serialize renders a stored event for a client.
//
// It follows rust/src/events/serialize.rs, whose ordering is load-bearing:
// `unsigned.age` is computed before the format transform, so the v1 format
// copies the computed age rather than the stored `age_ts`.
func Serialize(ev Stored, nowMS int64, cfg Config) ([]byte, error) {
	out := ev.JSON
	var err error

	// Room version v3+ PDUs do not carry their own event_id -- it is a hash of
	// the event. Clients still expect the field, so it is always inserted.
	if out, err = sjson.SetBytes(out, "event_id", ev.EventID); err != nil {
		return nil, err
	}

	// Rebuild `unsigned` from the allowlist rather than passing the stored
	// object through.
	//
	// Synapse models unsigned as a typed struct of exactly six fields
	// (rust/src/events/unsigned.rs), so anything else in storage is dropped
	// when the event is loaded. That is not a formality: remote servers send
	// `age` and Synapse *stores* it, so a passthrough emits a stale age --
	// baked in at receive time, years out of date -- on events Synapse gives no
	// age at all. Found by comparing federated rooms; every locally created
	// event agreed, because those store `age_ts` instead.
	//
	// `unsigned` is always present in the output, empty as {}: the field is
	// serialised unconditionally, and the v1 format reads it without checking.
	if out, err = sjson.SetRawBytes(out, "unsigned", filterUnsigned(ev.JSON)); err != nil {
		return nil, err
	}

	// age_ts becomes a wall-clock age. It is generated by us and so should be
	// an integer, but an out-of-range value is dropped rather than erroring: a
	// once-valid event must not start failing to serialise.
	if ageTS := gjson.GetBytes(out, "unsigned.age_ts"); ageTS.Exists() && ageTS.Type == gjson.Number {
		if out, err = sjson.SetBytes(out, "unsigned.age", nowMS-ageTS.Int()); err != nil {
			return nil, err
		}
		if out, err = sjson.DeleteBytes(out, "unsigned.age_ts"); err != nil {
			return nil, err
		}
	}

	if out, err = applyTransactionID(out, ev, cfg.Requester); err != nil {
		return nil, err
	}

	if !cfg.IncludeStrippedRoomState {
		if out, err = sjson.DeleteBytes(out, "unsigned.invite_room_state"); err != nil {
			return nil, err
		}
		if out, err = sjson.DeleteBytes(out, "unsigned.knock_room_state"); err != nil {
			return nil, err
		}
	}

	if out, err = applyFormat(out, cfg.Format); err != nil {
		return nil, err
	}

	rv := LookupRoomVersion(ev.RoomVersion)

	// An MSC4291 room derives its ID from the create event's hash, so that
	// event has no room_id of its own. Put it back, or a client cannot tell
	// which room the create event belongs to.
	//
	// Note this runs *after* the format transform, so it also restores the
	// field on /sync's v2-without-room-id -- which is what Synapse does.
	if ev.Type == "m.room.create" && rv.RoomIDsAsHashes {
		if out, err = sjson.SetBytes(out, "room_id", ev.RoomID); err != nil {
			return nil, err
		}
	}

	// A redaction names its target in a different place per room version. It is
	// already in the version-correct one; copy it to the other as well, for
	// clients written against either.
	if ev.Type == "m.room.redaction" {
		if out, err = mirrorRedacts(out, rv); err != nil {
			return nil, err
		}
	}

	// MSC4115: the sender's membership at the point of this event, so a client
	// can render history without resolving state itself.
	if ev.Membership != "" {
		if out, err = sjson.SetBytes(out, "unsigned.membership", ev.Membership); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func applyFormat(out []byte, format Format) ([]byte, error) {
	var err error
	for _, key := range v2DropKeys {
		if out, err = sjson.DeleteBytes(out, key); err != nil {
			return nil, err
		}
	}
	switch format {
	case FormatV2:
		return out, nil

	case FormatV2NoRoomID:
		return sjson.DeleteBytes(out, "room_id")

	case FormatV1:
		// `user_id` duplicates `sender`. A present-but-null sender is left
		// alone, matching the Rust's `filter(|v| !v.is_null())`.
		if sender := gjson.GetBytes(out, "sender"); sender.Exists() && sender.Type != gjson.Null {
			if out, err = sjson.SetBytes(out, "user_id", sender.Value()); err != nil {
				return nil, err
			}
		}
		for _, key := range v1CopyKeys {
			v := gjson.GetBytes(out, "unsigned."+key)
			if !v.Exists() {
				continue
			}
			if out, err = sjson.SetRawBytes(out, key, []byte(v.Raw)); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	return out, nil
}

func mirrorRedacts(out []byte, rv RoomVersion) ([]byte, error) {
	// Read whichever place is version-correct, write the other. A
	// present-but-null value is skipped, matching Synapse's guard on
	// `e.redacts is not None`.
	from, to := "content.redacts", "redacts"
	if rv.UpdatedRedactionRules {
		from, to = "redacts", "content.redacts"
	}
	v := gjson.GetBytes(out, from)
	if !v.Exists() || v.Type == gjson.Null {
		return out, nil
	}
	return sjson.SetRawBytes(out, to, []byte(v.Raw))
}

// applyTransactionID reveals `unsigned.transaction_id` only to the session that
// sent the event.
//
// The transaction ID is the sender's own idempotency key. Showing it to another
// of the user's sessions would leak which client sent what, so Synapse matches
// on device first and falls back to the access token for events stored before
// device IDs were recorded.
func applyTransactionID(out []byte, ev Stored, req Requester) ([]byte, error) {
	if len(ev.InternalMetadata) == 0 || req.UserID == "" {
		return out, nil
	}
	if gjson.GetBytes(out, "sender").String() != req.UserID {
		return out, nil
	}
	meta := gjson.ParseBytes(ev.InternalMetadata)

	txnID := meta.Get("txn_id")
	if txnID.Exists() && txnID.Type == gjson.String {
		reveal := false
		if eventDevice := meta.Get("device_id"); eventDevice.Exists() {
			reveal = eventDevice.String() == req.DeviceID && req.DeviceID != ""
		} else {
			// No device is stored for old events and for those created by
			// appservices, guests or admin-API tokens. Fall back to the access
			// token; for guests and appservices, which cannot be checked,
			// Synapse assumes the same session.
			eventToken := meta.Get("token_id")
			tokenMatches := eventToken.Exists() && req.TokenID != 0 && eventToken.Int() == req.TokenID
			reveal = tokenMatches || req.IsGuest || req.AppServiceID != ""
		}
		if reveal {
			var err error
			if out, err = sjson.SetBytes(out, "unsigned.transaction_id", txnID.String()); err != nil {
				return nil, err
			}
		}
	}

	if delayID := meta.Get("delay_id"); delayID.Exists() && delayID.Type == gjson.String {
		var err error
		if out, err = sjson.SetBytes(out, "unsigned.delay_id", delayID.String()); err != nil {
			return nil, err
		}
	}
	return out, nil
}
