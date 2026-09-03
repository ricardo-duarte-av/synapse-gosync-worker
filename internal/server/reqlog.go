package server

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/metrics"
)

type annotationKey struct{}

// Annotation carries per-request detail that only the handler knows, so the
// single log line emitted at the end can describe what actually happened.
type Annotation struct {
	// Endpoint is the low-cardinality route name, e.g. "sync" or
	// "room_initial_sync". Never a raw path: room IDs would explode the label.
	Endpoint string
	// UserID and DeviceID identify the caller once authenticated. These go in
	// the log, never in a metric label.
	UserID   string
	DeviceID string
	// Outcome distinguishes served / refused / client_gone / error.
	Outcome string
	// Reason gives detail for a refusal or error.
	Reason string
	// Since and NextBatch are the sync tokens in and out. The single most
	// useful thing in the log when a client complains it missed an event.
	Since     string
	NextBatch string
	// Timeout is the long-poll budget the client asked for, in ms.
	Timeout int
	// Waited is how long the long-poll actually blocked.
	Waited time.Duration
}

// Annotate returns the request's annotation, or nil if there is none.
func Annotate(ctx context.Context) *Annotation {
	a, _ := ctx.Value(annotationKey{}).(*Annotation)
	return a
}

// WithAnnotation attaches a fresh annotation to the request context.
func WithAnnotation(ctx context.Context, a *Annotation) context.Context {
	return context.WithValue(ctx, annotationKey{}, a)
}

// statusRecorder captures the status and byte count for the log line.
type statusRecorder struct {
	http.ResponseWriter
	status  int
	written int64
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	n, err := s.ResponseWriter.Write(b)
	s.written += int64(n)
	return n, err
}

// Flush lets a handler stream. Long-poll responses are small enough not to
// need it, but a missing Flusher silently disables streaming elsewhere.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// WithRequestLog wraps a handler with one structured log line and the request
// metrics, per request.
//
// Health and metrics endpoints are excluded: a scrape every fifteen seconds
// would bury everything else.
func WithRequestLog(log zerolog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isOperationalPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		ann := &Annotation{}
		rec := &statusRecorder{ResponseWriter: w}
		r = r.WithContext(WithAnnotation(r.Context(), ann))

		next.ServeHTTP(rec, r)

		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}
		elapsed := time.Since(start)

		endpoint := ann.Endpoint
		if endpoint == "" {
			endpoint = "unknown"
		}
		outcome := ann.Outcome
		if outcome == "" {
			// A client hanging up mid-long-poll is ordinary for /sync, and
			// must not be reported as a failure just because no response was
			// written.
			if errors.Is(r.Context().Err(), context.Canceled) {
				outcome = "client_gone"
			} else {
				outcome = "served"
			}
		}

		metrics.RequestsTotal.WithLabelValues(endpoint, strconv.Itoa(status), outcome).Inc()
		metrics.RequestDuration.WithLabelValues(endpoint).Observe(elapsed.Seconds())

		ev := log.Info()
		switch {
		case outcome == "client_gone":
			// The caller walked away, so the status is whatever we had no
			// chance to deliver -- usually 500, because a cancelled query
			// fails like any other. That is not a server fault and must not be
			// logged as one: a long poll exists to be abandoned, and 4% of
			// real sliding sync traffic ends this way. Reported at debug so
			// the line is still there when somebody goes looking.
			ev = log.Debug()
		case status >= 500:
			ev = log.Error()
		case status >= 400 && status != http.StatusNotFound:
			ev = log.Warn()
		}

		ev = ev.
			Str("endpoint", endpoint).
			Str("method", r.Method).
			Int("status", status).
			Str("outcome", outcome).
			Dur("duration", elapsed).
			Int64("bytes", rec.written)

		// Fields are omitted rather than zeroed when they do not apply: a
		// `waited=0` on a request that never long-polled reads as a fact, and
		// it is not one.
		if ann.Reason != "" {
			ev = ev.Str("reason", ann.Reason)
		}
		if ann.UserID != "" {
			ev = ev.Str("user_id", ann.UserID)
		}
		if ann.DeviceID != "" {
			ev = ev.Str("device_id", ann.DeviceID)
		}
		if ann.Since != "" {
			ev = ev.Str("since", ann.Since)
		}
		if ann.NextBatch != "" {
			ev = ev.Str("next_batch", ann.NextBatch)
		}
		if ann.Timeout > 0 {
			ev = ev.Int("timeout_ms", ann.Timeout)
		}
		if ann.Waited > 0 {
			ev = ev.Dur("waited", ann.Waited)
		}
		ev.Msg("request")
	})
}

func isOperationalPath(path string) bool {
	return path == "/health" || path == "/metrics"
}
