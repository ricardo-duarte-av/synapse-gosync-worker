package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/streamtoken"
)

// ErrNoSuchFilter is returned when the user has no filter with that ID.
//
// Synapse surfaces the same condition as a 404 from the storage layer and the
// REST layer rewrites it to a 400 "No such filter"; the split is kept here so
// the handler owns the HTTP shape.
var ErrNoSuchFilter = errors.New("store: no such filter")

// UserFilter loads a filter the client previously uploaded.
//
// Keyed on `full_user_id`, not `user_id`: the latter is a legacy localpart
// column that a background migration is still backfilling the other way round,
// and two users on different servers could collide on it.
func (s *Store) UserFilter(ctx context.Context, userID string, filterID int64) ([]byte, error) {
	const q = `
		SELECT filter_json
		  FROM user_filters
		 WHERE full_user_id = $1 AND filter_id = $2`
	var raw []byte
	err := s.pool.QueryRow(ctx, q, userID, filterID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoSuchFilter
	}
	if err != nil {
		return nil, fmt.Errorf("store: user filter: %w", err)
	}
	return raw, nil
}

// EventsWithRelations returns which of the given events have a relation from
// one of the given senders, of one of the given types.
//
// This backs a filter's `related_by_senders` / `related_by_rel_types`. With
// neither constraint set every event qualifies, which Synapse short-circuits
// before touching the database and so do we.
func (s *Store) EventsWithRelations(ctx context.Context, parentIDs, senders, relTypes []string) (map[string]bool, error) {
	out := make(map[string]bool, len(parentIDs))
	if len(senders) == 0 && len(relTypes) == 0 {
		for _, id := range parentIDs {
			out[id] = true
		}
		return out, nil
	}
	if len(parentIDs) == 0 {
		return out, nil
	}

	q := `
		SELECT relates_to_id
		  FROM event_relations
		  JOIN events USING (event_id)
		 WHERE relates_to_id = ANY($1)`
	args := []any{parentIDs}
	if len(senders) > 0 {
		args = append(args, senders)
		q += fmt.Sprintf(" AND sender = ANY($%d)", len(args))
	}
	if len(relTypes) > 0 {
		args = append(args, relTypes)
		q += fmt.Sprintf(" AND relation_type = ANY($%d)", len(args))
	}

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("store: events with relations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: events with relations: %w", err)
		}
		out[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: events with relations: %w", err)
	}
	return out, nil
}

// InstanceIDs maps a writer's name to the id a stream token records it under.
//
// A token carries integer instance ids; the event tables carry names. Comparing
// a row against a vector-clock token needs the mapping, and `instance_map` is
// append-only, so it is read once and kept.
func (s *Store) InstanceIDs(ctx context.Context) (map[string]int, error) {
	s.instanceMu.Lock()
	defer s.instanceMu.Unlock()
	if s.instanceIDs != nil {
		return s.instanceIDs, nil
	}
	rows, err := s.pool.Query(ctx, `SELECT instance_name, instance_id FROM instance_map`)
	if err != nil {
		return nil, fmt.Errorf("store: instance map: %w", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var name string
		var id int
		if err := rows.Scan(&name, &id); err != nil {
			return nil, fmt.Errorf("store: instance map: %w", err)
		}
		out[name] = id
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: instance map: %w", err)
	}
	s.instanceIDs = out
	return out, nil
}

// TimelineGap reports whether the room's timeline has a hole in (from, to], and
// where.
//
// A gap is recorded when Synapse persists an event whose predecessors it did
// not already have -- typically a federated room going quiet and then coming
// back. The events either side are contiguous in stream ordering but not in the
// room's own history, so a client handed both would silently miss whatever fell
// in between.
//
// Synapse's answer is a token positioned so the event AT the gap is included,
// since the gap is *before* that event. It has two effects that are easy to
// miss: the room's timeline is reported `limited` even when nothing was
// trimmed, and an incremental sync stops paginating at the gap rather than at
// the client's `since`.
//
// This deployment has 99,053 gap rows across 1,392 rooms, so it is not an edge
// case -- it is merely invisible whenever the timeline was long enough to be
// trimmed, which with the default filter is nearly always.
func (s *Store) TimelineGap(ctx context.Context, roomID string,
	from *streamtoken.RoomKey, to streamtoken.RoomKey) (int64, bool, error) {

	fromStream := int64(0)
	if from != nil {
		fromStream = from.Stream
	}
	const q = `
		SELECT COALESCE(instance_name, ''), stream_ordering
		  FROM timeline_gaps
		 WHERE room_id = $1 AND stream_ordering > $2 AND stream_ordering <= $3
		 ORDER BY stream_ordering`
	rows, err := s.pool.Query(ctx, q, roomID, fromStream, to.MaxStreamPos())
	if err != nil {
		return 0, false, fmt.Errorf("store: timeline gaps: %w", err)
	}
	defer rows.Close()

	type gap struct {
		instance string
		stream   int64
	}
	var gaps []gap
	for rows.Next() {
		var g gap
		if err := rows.Scan(&g.instance, &g.stream); err != nil {
			return 0, false, fmt.Errorf("store: timeline gaps: %w", err)
		}
		gaps = append(gaps, g)
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("store: timeline gaps: %w", err)
	}
	if len(gaps) == 0 {
		return 0, false, nil
	}

	ids, err := s.InstanceIDs(ctx)
	if err != nil {
		return 0, false, err
	}
	// persisted_after(token) is per writer: a gap counts only if the token's
	// position FOR THE WRITER THAT RECORDED IT is behind it. With a sharded
	// event persister the plain stream comparison in the query above is only an
	// approximation, and both bounds have to be re-checked here.
	last := int64(0)
	found := false
	for _, g := range gaps {
		id, ok := ids[g.instance]
		if !ok {
			// An unknown writer cannot be placed per instance; fall back to the
			// agreed minimum, which is what a token without that writer means.
			id = -1
		}
		if from != nil && !(from.StreamPosForInstance(id) < g.stream) {
			continue
		}
		if to.StreamPosForInstance(id) < g.stream {
			continue
		}
		last, found = g.stream, true
	}
	if !found {
		return 0, false, nil
	}
	// The gap sits *before* the event at this position, so the token has to be
	// one below it for that event to be included.
	return last - 1, true, nil
}
