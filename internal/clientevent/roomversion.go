// Package clientevent renders stored Synapse events into the shapes the client
// API sends.
//
// Responses are built by splicing the stored `event_json.json` with sjson
// rather than unmarshalling into a struct and re-encoding. 14,654 events on
// this server contain escaped NUL characters that PostgreSQL `jsonb` cannot
// even cast; a round trip through a Go map would also reorder keys and
// normalise numbers. Splicing keeps every byte we are not deliberately
// changing.
package clientevent

// RoomVersion carries the two flags event serialisation branches on.
//
// Synapse's table is much larger (rust/src/room_versions.rs), but the rest of
// it governs auth rules and redaction algorithms, which this package does not
// implement.
type RoomVersion struct {
	// UpdatedRedactionRules is MSC3820, from room version 11. It moves a
	// redaction's target from `content.redacts` to a top-level `redacts`.
	UpdatedRedactionRules bool
	// RoomIDsAsHashes is MSC4291, from room version 12. The room ID is derived
	// from the hash of the create event, so **the create event carries no
	// `room_id`** while every other event in the room does. Serialisation has
	// to put it back for clients.
	RoomIDsAsHashes bool
}

// roomVersions mirrors rust/src/room_versions.rs for the two flags above.
// Unknown versions get the conservative pre-v11 behaviour.
var roomVersions = map[string]RoomVersion{
	"1":                     {},
	"2":                     {},
	"3":                     {},
	"4":                     {},
	"5":                     {},
	"6":                     {},
	"7":                     {},
	"8":                     {},
	"9":                     {},
	"10":                    {},
	"org.matrix.msc3389.10": {},
	"org.matrix.msc1767.10": {},
	"org.matrix.msc3757.10": {},
	"11":                    {UpdatedRedactionRules: true},
	"org.matrix.msc3757.11": {UpdatedRedactionRules: true},
	"org.matrix.hydra.11":   {UpdatedRedactionRules: true, RoomIDsAsHashes: true},
	"12":                    {UpdatedRedactionRules: true, RoomIDsAsHashes: true},
	"org.matrix.msc4242.12": {UpdatedRedactionRules: true, RoomIDsAsHashes: true},
}

// LookupRoomVersion returns the flags for a room version identifier.
//
// An unknown version is not an error: Synapse may add one before we do, and
// refusing to serialise every event in such a room would be a far worse
// failure than serialising it with pre-v11 rules.
func LookupRoomVersion(id string) RoomVersion {
	return roomVersions[id]
}
