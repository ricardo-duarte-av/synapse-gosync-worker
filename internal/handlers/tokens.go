package handlers

import (
	"context"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/replication"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/streamtoken"
)

// replicationStreams maps a replication stream name onto the token field it
// advances.
//
// Every field except the room key is a plain integer, so the overlay is a
// maximum. The room key is a RoomStreamToken and gets its own handling.
var replicationStreams = map[string]func(*streamtoken.Token, int64){
	replication.StreamPresence:            func(t *streamtoken.Token, p int64) { t.Presence = max64(t.Presence, p) },
	replication.StreamTyping:              func(t *streamtoken.Token, p int64) { t.Typing = max64(t.Typing, p) },
	replication.StreamAccountData:         func(t *streamtoken.Token, p int64) { t.AccountData = max64(t.AccountData, p) },
	replication.StreamPushRules:           func(t *streamtoken.Token, p int64) { t.PushRules = max64(t.PushRules, p) },
	replication.StreamToDevice:            func(t *streamtoken.Token, p int64) { t.ToDevice = max64(t.ToDevice, p) },
	replication.StreamUnPartialStatedRoom: func(t *streamtoken.Token, p int64) { t.UnPartialStatedRooms = max64(t.UnPartialStatedRooms, p) },
	replication.StreamThreadSubscriptions: func(t *streamtoken.Token, p int64) { t.ThreadSubscriptions = max64(t.ThreadSubscriptions, p) },
	replication.StreamStickyEvents:        func(t *streamtoken.Token, p int64) { t.StickyEvents = max64(t.StickyEvents, p) },
	replication.StreamProfileUpdates:      func(t *streamtoken.Token, p int64) { t.ProfileUpdates = max64(t.ProfileUpdates, p) },
}

// currentToken builds the token this response is bounded by.
//
// The database supplies a seed and replication corrects it. The correction is
// not cosmetic: `typing` has no database representation at all, and `push_rules`
// and `thread_subscriptions` drift above their table maxima because their id
// generators allocate ids that no surviving row records. See docs/tokens.md.
//
// Every overlay is a MAXIMUM, so a stale replication position can never drag a
// token backwards -- which would ask a client to replay events it already has.
func currentToken(ctx context.Context, d Deps) (streamtoken.Token, error) {
	tok, err := d.Store.CurrentToken(ctx)
	if err != nil {
		return streamtoken.Token{}, err
	}
	if d.Replication == nil || !d.Replication.Live() {
		return tok, nil
	}
	for stream, apply := range replicationStreams {
		if pos := d.Replication.Position(stream); pos > 0 {
			apply(&tok, pos)
		}
	}
	if pos := d.Replication.Position(replication.StreamEvents); pos > tok.Room.Stream {
		tok.Room = streamtoken.Live(pos)
	}
	if pos := d.Replication.Position(replication.StreamReceipts); pos > tok.Receipt.Stream {
		tok.Receipt = streamtoken.MultiWriter{Stream: pos}
	}
	if pos := d.Replication.Position(replication.StreamDeviceLists); pos > tok.DeviceList.Stream {
		tok.DeviceList = streamtoken.MultiWriter{Stream: pos}
	}
	if pos := d.Replication.Position(replication.StreamQuarantinedMedia); pos > tok.QuarantinedMedia.Stream {
		tok.QuarantinedMedia = streamtoken.MultiWriter{Stream: pos}
	}
	return tok, nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
