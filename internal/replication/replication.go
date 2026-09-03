// Package replication follows Synapse's replication stream over Redis.
//
// This is what lets the worker answer without being told what time it is. Every
// other source of a stream position is either a lagging checkpoint
// (`stream_positions`) or an approximation from table maxima that is wrong for
// three streams and impossible for a fourth -- typing is never persisted at
// all. See docs/tokens.md.
//
// It is SUBSCRIBE-only. A real Synapse worker also PUBLISHes a `REPLICATE`
// command on connect to ask for current positions; this one deliberately does
// not, because publishing to the shared bus would make every other worker
// broadcast POSITION rows on our account. Positions are seeded from the
// database and corrected as traffic arrives.
package replication

import (
	"context"
	"github.com/tidwall/gjson"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/metrics"
)

// Config describes how to reach Redis.
type Config struct {
	Enabled bool
	// Address is a unix socket path, or host:port.
	Address string
	// Channel MUST equal Synapse's server_name: the channel is named after it.
	// Subscribing to the wrong channel raises no error and delivers nothing, so
	// the worker would simply never wake up and would look merely idle.
	Channel  string
	Password string
	DB       int
}

// Stream names carried on the replication channel that matter to a sync.
const (
	StreamEvents              = "events"
	StreamPresence            = "presence"
	StreamTyping              = "typing"
	StreamReceipts            = "receipts"
	StreamAccountData         = "account_data"
	StreamPushRules           = "push_rules"
	StreamToDevice            = "to_device"
	StreamDeviceLists         = "device_lists"
	StreamUnPartialStatedRoom = "un_partial_stated_room"
	StreamThreadSubscriptions = "thread_subscriptions"
	StreamStickyEvents        = "sticky_events"
	StreamQuarantinedMedia    = "quarantined_media"
	StreamProfileUpdates      = "profile_updates"
	// StreamCaches carries Synapse's own cache invalidations, as
	// [cache_func, keys, invalidation_ts]. It is the only signal that
	// something we cached as immutable has been DELETED -- a purged room, a
	// deleted room -- rather than merely changed.
	StreamCaches = "caches"
)

// Listener is notified when a stream advances.
//
// roomIDs and userIDs name who the change concerns, so a waiting sync can be
// woken only if it cares. An empty pair means "everyone", which is the
// conservative answer when a row does not say.
type Listener interface {
	OnStreamAdvance(stream string, position int64, roomIDs, userIDs []string)
}

// Invalidator is told what each replication row makes stale.
//
// A second listener rather than an addition to Listener, because the two want
// opposite defaults. The notifier treats "no subjects" as "wake everybody",
// which is a harmless over-wake. An invalidator that treated it as "drop
// everything" would throw the caches away on every row it could not parse, and
// one that treated it as "drop nothing" would serve stale answers. It needs to
// make that choice itself, per stream.
type Invalidator interface {
	// OnRow is called for every row, in arrival order, before the position is
	// reported as applied.
	OnRow(stream string, pos int64, detail RowDetail)
	// OnRoomInvalidated says one room's derived data is stale.
	OnRoomInvalidated(roomID string)
	// OnPurge asks for everything to be dropped.
	OnPurge(reason string)
}

// Subscriber follows the replication channel.
type Subscriber struct {
	cfg         Config
	log         zerolog.Logger
	listener    Listener
	invalidator Invalidator

	mu sync.RWMutex
	// live is false whenever the subscription is not known to be healthy.
	// Positions and typing are only trustworthy while it is true.
	live      bool
	positions map[string]int64
	// typingSerial maps a room to the typing stream position at which it last
	// changed. /events needs it: unlike every other source, typing has no
	// table to ask "what changed since?", so the only record of when a room's
	// typists last moved is the one kept here as the rows arrive.
	typingSerial map[string]int64
	// typing maps a room to the users currently typing in it. Held only in
	// memory because that is the only place it exists anywhere: Synapse keeps
	// it in a counter on the typing worker and never writes it down.
	typing map[string][]string

	// onDrop and onConnect run on the edges of the subscription's health. Set
	// by the caller rather than called directly so this package need not know
	// about the store.
	onDrop    func()
	onConnect func(positions map[string]int64)
}

// SetInvalidator registers the cache invalidator.
func (s *Subscriber) SetInvalidator(inv Invalidator) { s.invalidator = inv }

// SetOnDrop registers a callback for the moment the subscription is lost.
//
// While we are following the stream, a purged or deleted room arrives as a
// `caches` invalidation we can act on. While we are not, rooms can be purged
// underneath us unseen -- so anything cached on the strength of "this row is
// immutable" has to be thrown away, because immutable is not the same as
// still there.
//
// Not called on the initial transition to live: there is nothing to purge
// before the first connection, and doing so would make startup log a discard
// of an empty cache.
func (s *Subscriber) SetOnDrop(f func()) { s.onDrop = f }

// SetOnConnect registers a callback for the moment the subscription becomes
// healthy, given the stream positions known at that instant.
//
// Those positions are the seed from the database plus anything already seen.
// A cache armed here starts empty, so claiming that everything up to them has
// been applied claims it of nothing -- and every entry added afterwards is
// read from a database that is at or beyond them.
func (s *Subscriber) SetOnConnect(f func(positions map[string]int64)) { s.onConnect = f }

// Positions returns a copy of every stream position currently known.
func (s *Subscriber) Positions() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]int64, len(s.positions))
	for k, v := range s.positions {
		out[k] = v
	}
	return out
}

// New builds a Subscriber.
func New(cfg Config, log zerolog.Logger, listener Listener) *Subscriber {
	return &Subscriber{
		cfg:          cfg,
		log:          log,
		listener:     listener,
		positions:    map[string]int64{},
		typing:       map[string][]string{},
		typingSerial: map[string]int64{},
	}
}

// Seed sets a starting position for a stream, from the database.
//
// Used at startup so the worker has an answer before any traffic arrives. A
// seeded position is a lower bound; the first RDATA for that stream replaces
// it with the truth.
func (s *Subscriber) Seed(positions map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for stream, pos := range positions {
		if pos > s.positions[stream] {
			s.positions[stream] = pos
		}
	}
}

// Position returns the last seen position of a stream.
func (s *Subscriber) Position(stream string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.positions[stream]
}

// Live reports whether the subscription is currently healthy.
//
// While it is false, typing is empty and positions may be stale. Callers that
// need to know whether an answer is authoritative ask this rather than assuming.
func (s *Subscriber) Live() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.live
}

// TypingIn returns the users currently typing in a room.
func (s *Subscriber) TypingIn(roomID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	users := s.typing[roomID]
	if len(users) == 0 {
		return nil
	}
	out := make([]string, len(users))
	copy(out, users)
	return out
}

// Run follows the channel until the context is cancelled, reconnecting with
// backoff.
func (s *Subscriber) Run(ctx context.Context) {
	if !s.cfg.Enabled {
		s.log.Info().Msg("replication disabled; stream positions will be approximated from the database")
		return
	}
	backoff := time.Second
	for ctx.Err() == nil {
		if err := s.session(ctx); err != nil && ctx.Err() == nil {
			s.log.Warn().Err(err).Dur("retry_in", backoff).Msg("replication connection lost")
		}
		s.setLive(false)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (s *Subscriber) session(ctx context.Context) error {
	opts := &redis.Options{Addr: s.cfg.Address, Password: s.cfg.Password, DB: s.cfg.DB}
	if strings.HasPrefix(s.cfg.Address, "/") {
		opts.Network = "unix"
	}
	client := redis.NewClient(opts)
	defer func() { _ = client.Close() }()

	// The main channel carries every stream. Synapse puts a few commands on
	// `<channel>/SUFFIX` sub-channels -- USER_IP is one -- which are none of
	// our business.
	pubsub := client.Subscribe(ctx, s.cfg.Channel)
	defer func() { _ = pubsub.Close() }()

	if _, err := pubsub.Receive(ctx); err != nil {
		return err
	}
	s.setLive(true)
	s.log.Info().Str("channel", s.cfg.Channel).Msg("following the replication stream")

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			s.handle(msg.Payload)
		}
	}
}

func (s *Subscriber) setLive(live bool) {
	if live {
		metrics.ReplicationConnected.Set(1)
	} else {
		metrics.ReplicationConnected.Set(0)
	}
	s.mu.Lock()
	was := s.live
	if !live {
		// Typing exists only here, so a lost connection means we no longer
		// know who is typing. Claiming otherwise would leave a room showing a
		// typist forever.
		s.typing = map[string][]string{}
		s.typingSerial = map[string]int64{}
	}
	s.live = live
	s.mu.Unlock()

	// Only on the edge: the reconnect loop calls setLive(false) on every
	// attempt, and purging the caches once per second while a Redis outage
	// lasts would turn a lost connection into a slow one.
	//
	// Outside the lock -- the callback reaches into the store, and holding a
	// subscriber mutex across it invites a deadlock that would only show up
	// under exactly the failure this exists to handle.
	if was && !live && s.onDrop != nil {
		s.onDrop()
	}
	if !was && live && s.onConnect != nil {
		s.onConnect(s.Positions())
	}
}

// handle parses one replication command.
//
// The wire format is `COMMAND <args>`, one per message. Only RDATA and POSITION
// carry stream positions; everything else -- REMOTE_SERVER_UP, LOCK_RELEASED,
// PING -- is ignored rather than treated as an error, because the channel
// carries traffic for every worker on the server.
func (s *Subscriber) handle(payload string) {
	switch {
	case strings.HasPrefix(payload, "RDATA "):
		s.handleRDATA(payload)
	case strings.HasPrefix(payload, "POSITION "):
		s.handlePosition(payload)
	}
}

func (s *Subscriber) handleRDATA(payload string) {
	// RDATA <stream> <instance> <token> <row json>
	parts := strings.SplitN(payload, " ", 5)
	if len(parts) < 5 {
		return
	}
	stream, token, row := parts[1], parts[3], parts[4]

	// A batched row carries the literal "batch" instead of a position; only the
	// last row of the batch names the token. Ignoring the position is right:
	// the batch's final row supplies it.
	var pos int64
	if token != "batch" {
		n, err := strconv.ParseInt(token, 10, 64)
		if err != nil {
			return
		}
		pos = n
	}

	detail := rowDetails(stream, row)

	// "global" means the row named neither a room nor a user, so every parked
	// client gets woken. Recorded per stream because that is the only way to
	// notice a busy stream in that state -- reading the code is how the last
	// four were found.
	scope := "targeted"
	if len(detail.RoomIDs) == 0 && len(detail.UserIDs) == 0 {
		scope = "global"
	}
	metrics.ReplicationRows.WithLabelValues(stream, scope).Inc()

	if stream == StreamTyping {
		s.updateTyping(row, pos)
	}

	// Invalidation runs before the position is advanced and before anything is
	// woken. A sync that starts between the two would find the cache already
	// dropped and the position not yet raised, which costs a query; the other
	// order costs a stale answer.
	if s.invalidator != nil {
		if stream == StreamCaches {
			s.handleCacheInvalidation(row)
		} else {
			s.invalidator.OnRow(stream, pos, detail)
		}
	}

	if pos > 0 {
		s.advance(stream, pos)
	}
	if s.listener != nil {
		s.listener.OnStreamAdvance(stream, pos, detail.RoomIDs, detail.UserIDs)
	}
}

// Synapse's sentinel names for the invalidations that mean data has been
// destroyed rather than merely changed
// (storage/databases/main/cache.py). The row is
// [cache_func, keys, invalidation_ts].
const (
	cacheNamePurgeHistory = "ph_cache_fake"
	cacheNameDeleteRoom   = "dr_cache_fake"
	cacheNameCurrentState = "cs_cache_fake"
)

// handleCacheInvalidation acts on the `caches` stream.
//
// Only three names matter, and every other row is ignored on purpose. This
// stream is NOT quiet -- 24 rows in 45 seconds on this deployment -- and it is
// almost entirely Synapse invalidating its own caches for things this worker
// does not hold: get_server_key_json_for_remote, _get_server_keys_json,
// get_destination_retry_timings, _get_bare_e2e_cross_signing_keys. Treating
// any row as a reason to purge threw the state caches away every two seconds,
// which measured as 4,910 extra state-group queries on a single initial sync.
//
// An unparseable row is ignored rather than treated as destructive, for the
// same reason: if the row format ever changes, every row becomes unparseable,
// and a purge on each one is far more damaging than missing the invalidation
// of a room that has been deleted -- which the next restart or reconnect
// clears anyway.
func (s *Subscriber) handleCacheInvalidation(row string) {
	r := gjson.Parse(row)
	if !r.IsArray() {
		metrics.CacheInvalidationRows.WithLabelValues("unparsed").Inc()
		return
	}
	a := r.Array()
	if len(a) == 0 {
		metrics.CacheInvalidationRows.WithLabelValues("unparsed").Inc()
		return
	}

	name := a[0].String()
	switch name {
	case cacheNamePurgeHistory, cacheNameDeleteRoom:
		// Events and state groups have been deleted. Nothing here can map a
		// cached state group back to a room, so this is the one case that
		// genuinely has to drop everything -- and both are rare.
		metrics.CacheInvalidationRows.WithLabelValues("purge").Inc()
		s.invalidator.OnPurge(name)

	case cacheNameCurrentState:
		// A room's current state changed. Keyed by room, so this is targeted.
		metrics.CacheInvalidationRows.WithLabelValues("room").Inc()
		if len(a) >= 2 {
			for _, k := range a[1].Array() {
				s.invalidator.OnRoomInvalidated(k.String())
			}
		}

	default:
		metrics.CacheInvalidationRows.WithLabelValues("ignored").Inc()
	}
}

func (s *Subscriber) handlePosition(payload string) {
	// POSITION <stream> <instance> <prev> <new>
	parts := strings.Fields(payload)
	if len(parts) < 5 {
		return
	}
	pos, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return
	}
	s.advance(parts[1], pos)
	if s.listener != nil {
		s.listener.OnStreamAdvance(parts[1], pos, nil, nil)
	}
}

func (s *Subscriber) advance(stream string, pos int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if pos > s.positions[stream] {
		s.positions[stream] = pos
	}
}
