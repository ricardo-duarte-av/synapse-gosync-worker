package replication

import (
	"encoding/json"
	"sort"

	"github.com/tidwall/gjson"
)

// rowSubjects works out which rooms and users a replication row concerns, so a
// waiting sync can be woken only when something it cares about happens.
//
// The rows are positional JSON arrays, one shape per stream
// (synapse/replication/tcp/streams/*.py). Getting a shape wrong costs a missed
// wakeup rather than a wrong answer -- the sync recomputes from the database
// either way -- but a missed wakeup is a client that hangs until its timeout.
func rowSubjects(stream, row string) (roomIDs, userIDs []string) {
	r := gjson.Parse(row)
	if !r.IsArray() {
		return nil, nil
	}
	a := r.Array()
	get := func(i int) string {
		if i < len(a) {
			return a[i].String()
		}
		return ""
	}

	switch stream {
	case StreamEvents:
		// ["ev", [event_id, room_id, type, state_key, ...]]
		if len(a) >= 2 && a[0].String() == "ev" {
			inner := a[1].Array()
			if len(inner) >= 2 {
				return []string{inner[1].String()}, nil
			}
		}
	case StreamTyping:
		// [room_id, [user_id, ...]]
		return []string{get(0)}, nil
	case StreamReceipts:
		// [room_id, receipt_type, user_id, event_id, thread_id, data]
		return []string{get(0)}, []string{get(2)}
	case StreamPresence:
		// [user_id, state, ...]
		return nil, []string{get(0)}
	case StreamAccountData:
		// [user_id, room_id, account_data_type]
		if room := get(1); room != "" {
			return []string{room}, []string{get(0)}
		}
		return nil, []string{get(0)}
	case StreamToDevice, StreamDeviceLists, StreamPushRules:
		// [user_id, ...] for all three.
		return nil, []string{get(0)}
	}
	return nil, nil
}

// updateTyping replaces a room's typist list.
//
// The row carries the WHOLE current set for that room, not a delta, so a user
// who stopped typing simply is not in it. Merging instead of replacing would
// leave people typing forever.
// TypingChangedSince returns the rooms whose typists changed after a typing
// stream position, oldest change first.
//
// Only rooms this process has SEEN change since it started: the serials are
// built from the replication stream, and a room that has been quiet since
// startup has no entry at all. That is the same limitation typing has
// everywhere here -- there is nothing in the database to fall back on -- and it
// resolves itself as soon as anyone types.
func (s *Subscriber) TypingChangedSince(pos int64) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.live {
		return nil
	}
	type change struct {
		roomID string
		serial int64
	}
	var changed []change
	for roomID, serial := range s.typingSerial {
		if serial > pos {
			changed = append(changed, change{roomID, serial})
		}
	}
	sort.Slice(changed, func(i, j int) bool {
		if changed[i].serial != changed[j].serial {
			return changed[i].serial < changed[j].serial
		}
		return changed[i].roomID < changed[j].roomID
	})
	out := make([]string, 0, len(changed))
	for _, c := range changed {
		out = append(out, c.roomID)
	}
	return out
}

func (s *Subscriber) updateTyping(row string, pos int64) {
	var parsed []json.RawMessage
	if err := json.Unmarshal([]byte(row), &parsed); err != nil || len(parsed) < 2 {
		return
	}
	var roomID string
	if err := json.Unmarshal(parsed[0], &roomID); err != nil {
		return
	}
	var users []string
	if err := json.Unmarshal(parsed[1], &users); err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// A batched row carries the literal token "batch" rather than a position,
	// so pos is 0 for all but the last of a batch. The stream position already
	// held is the best available answer, and it never goes backwards.
	serial := pos
	if serial <= 0 {
		serial = s.positions[StreamTyping]
	}
	if serial > s.typingSerial[roomID] {
		s.typingSerial[roomID] = serial
	}

	if len(users) == 0 {
		delete(s.typing, roomID)
		return
	}
	s.typing[roomID] = users
}
