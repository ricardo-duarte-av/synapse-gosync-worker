// Package metrics defines the worker's Prometheus instrumentation.
//
// Labels are deliberately low-cardinality. Per-user or per-room labels would
// be the obvious thing to want on a sync worker and are exactly what must not
// be done: this homeserver has thousands of rooms and hundreds of users, and
// per-request detail belongs in the request log, which is already structured.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// RequestsTotal counts served requests.
	//
	// `outcome` separates a request we answered from one we refused and one
	// the client abandoned. A client that hangs up mid-long-poll is entirely
	// normal for /sync and must not be counted as an error, which is a
	// distinction the status code alone cannot carry.
	RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gosync_requests_total",
		Help: "Client API requests served, by endpoint, status and outcome.",
	}, []string{"endpoint", "status", "outcome"})

	// RequestDuration measures the whole request.
	//
	// The buckets run to 300s because a long-poll legitimately occupies the
	// upper end: a /sync with timeout=30000 that sees no traffic takes the full
	// 30 seconds and is a success. Sizing these like a normal HTTP histogram
	// would put every healthy idle sync in +Inf.
	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gosync_request_duration_seconds",
		Help:    "Client API request duration, including long-poll wait.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300},
	}, []string{"endpoint"})

	// AuthVerdictsTotal counts token validations by outcome.
	//
	// `unavailable` is the one to watch: it means Synapse could not be asked,
	// and the correct response is 502, never 401. Refusing a valid token
	// because whoami was unreachable would log real clients out.
	AuthVerdictsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gosync_auth_verdicts_total",
		Help: "Access token validations, by verdict.",
	}, []string{"verdict"})

	// AuthCacheEntries reports the size of the token cache.
	AuthCacheEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gosync_auth_cache_entries",
		Help: "Cached access token verdicts.",
	})

	// DatabaseReadOnly is 1 when the connected role cannot write.
	//
	// A gauge rather than a startup check alone, so that a role changed
	// underneath a running worker is visible.
	DatabaseReadOnly = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gosync_database_read_only",
		Help: "1 if the database role has default_transaction_read_only set.",
	})

	// BuildInfo carries the version as a label on a constant 1.
	BuildInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gosync_build_info",
		Help: "Build information.",
	}, []string{"version"})
)
