// Command gosync-worker is a Go reimplementation of Synapse's classic /sync
// worker.
//
// It reads Synapse's PostgreSQL directly and follows Synapse's replication
// stream over Redis. Nothing is routed to it: it exists to be driven by
// cmd/syncdiff and a test account, and compared against a real Synapse sync
// worker, until its answers are trusted.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/auth"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/config"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/deviceinbox"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/handlers"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/lazyload"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/metrics"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/notifier"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/presence"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/pushrules"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/replication"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/server"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/slidingstore"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/store"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/synapsecfg"
)

// Build information, stamped by the Docker build via -ldflags, in the same
// three variables gopro-worker and media-worker use so that one CI workflow
// shape serves all three. The defaults apply to a plain "go build".
//
// They are passed in rather than derived because .dockerignore strips .git:
// the build cannot see the repository it came from.
var (
	tag       = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

func main() {
	configPath := flag.String("config", "gosync-worker.yaml", "path to the configuration file")
	check := flag.Bool("check", false, "validate the config and database access, then exit")
	showVersion := flag.Bool("version", false, "print build information and exit")
	healthcheck := flag.Bool("healthcheck", false, "probe this worker's own /health over its configured transport, then exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("gosync-worker %s (commit %s, built %s, %s)\n",
			tag, commit, buildTime, runtime.Version())
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// The runtime image is distroless: no shell, no curl. The binary probes
	// itself over the same transport it was configured to serve.
	if *healthcheck {
		if err := probeHealth(cfg); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	log := newLogger(cfg.Log)
	metrics.BuildInfo.WithLabelValues(tag).Set(1)

	if err := run(cfg, log, *check); err != nil {
		log.Error().Err(err).Msg("fatal")
		os.Exit(1)
	}
}

func run(cfg *config.Config, log zerolog.Logger, checkOnly bool) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	openCtx, openCancel := context.WithTimeout(ctx, cfg.Database.ConnectTimeout())
	defer openCancel()

	db, err := store.Open(openCtx, store.Config{
		DSN:                         cfg.Database.DSN,
		MaxConns:                    cfg.Database.Conns(),
		ConnectTimeout:              cfg.Database.ConnectTimeout(),
		EventStateGroupCacheEntries: cfg.Caches.StateGroupCacheSize,
		FilteredStateCacheEntries:   cfg.Caches.FilteredStateCacheSize,

		UserFilterCacheEntries:        cacheSize(cfg.Caches.Enabled, cfg.Caches.UserFilterCacheSize),
		RoomInfoCacheEntries:          cacheSize(cfg.Caches.Enabled, cfg.Caches.RoomInfoCacheSize),
		AccessTokenCacheEntries:       cacheSize(cfg.Caches.Enabled, cfg.Caches.AccessTokenCacheSize),
		RoomSummaryCacheEntries:       cacheSize(cfg.Caches.Enabled, cfg.Caches.RoomSummaryCacheSize),
		HistoryVisibilityCacheEntries: cacheSize(cfg.Caches.Enabled, cfg.Caches.HistoryVisibilityCacheSize),
		IgnoredUsersCacheEntries:      cacheSize(cfg.Caches.Enabled, cfg.Caches.IgnoredUsersCacheSize),
		RoomsForUserCacheEntries:      cacheSize(cfg.Caches.Enabled, cfg.Caches.RoomsForUserCacheSize),

		StreamCaches: store.StreamCacheSizes{
			Events:      cacheSize(cfg.Caches.Enabled, cfg.Caches.EventsStreamCacheSize),
			Membership:  cacheSize(cfg.Caches.Enabled, cfg.Caches.MembershipStreamCacheSize),
			Receipts:    cacheSize(cfg.Caches.Enabled, cfg.Caches.ReceiptsStreamCacheSize),
			AccountData: cacheSize(cfg.Caches.Enabled, cfg.Caches.AccountDataStreamCacheSize),
			ToDevice:    cacheSize(cfg.Caches.Enabled, cfg.Caches.ToDeviceStreamCacheSize),
			Presence:    cacheSize(cfg.Caches.Enabled, cfg.Caches.PresenceStreamCacheSize),
		},
	})
	if err != nil {
		return err
	}
	defer db.Close()

	// Reported rather than enforced. Refusing to start against a writable role
	// would be the wrong trade in a deployment that has not run
	// readonly-role.sql yet, but running against one unknowingly is exactly the
	// situation the SQL file exists to prevent.
	metrics.RegisterCaches(db.DerivedCacheStats)
	metrics.RegisterStreamCaches(db.StreamCacheStats)

	readOnly, err := db.IsReadOnly(openCtx)
	if err != nil {
		return err
	}
	if readOnly {
		metrics.DatabaseReadOnly.Set(1)
	} else {
		metrics.DatabaseReadOnly.Set(0)
		log.Warn().Msg("database role is NOT read-only; see deploy/readonly-role.sql")
	}

	// The one writing connection, and only when to_device is configured. It
	// holds a role granted SELECT and DELETE on device_inbox alone, verified at
	// startup by deviceinbox.Open -- so the main pool above keeps its read-only
	// role and its check, and the guarantee is weakened in exactly one place.
	var inbox *deviceinbox.Deleter
	if cfg.ToDevice.Enabled {
		inbox, err = deviceinbox.Open(openCtx, deviceinbox.Config{
			DSN:            cfg.ToDevice.DSN,
			MaxConns:       cfg.ToDevice.Conns(),
			ConnectTimeout: cfg.ToDevice.ConnectTimeout(),
		})
		if err != nil {
			return err
		}
		defer inbox.Close()
		log.Info().Msg("to_device enabled: acknowledged messages will be deleted")
	}

	// Presence, resolved out of Synapse's own homeserver.yaml at every start
	// rather than duplicated into ours. See internal/synapsecfg for why.
	//
	// This is the only thing this worker TELLS the homeserver. Without it every
	// account it serves appears permanently offline, because Synapse's /sync
	// calls user_syncing() and ours would say nothing.
	var presenceClient *presence.Client
	if cfg.SynapseConfig != "" {
		sc, err := synapsecfg.Load(cfg.SynapseConfig)
		if err != nil {
			return err
		}
		switch {
		case !sc.PresenceEnabled:
			// Refusing to relay is right: with presence off the writer ignores
			// what we would send, and pretending otherwise hides a
			// configuration the operator chose.
			log.Info().Str("synapse_config", cfg.SynapseConfig).
				Msg("presence is disabled on this homeserver; not relaying")
		default:
			presenceClient, err = presence.New(presence.Config{
				Socket: sc.PresenceWriter.Socket,
				URL:    sc.PresenceWriter.URL,
				Secret: sc.ReplicationSecret,
				RelayInterval: presence.DeriveRelayInterval(
					sc.SyncOnlineTimeout, sc.LastActiveGranularity),
			}, log)
			if err != nil {
				return err
			}
			metrics.RegisterPresence(presenceClient.Tracked)
			log.Info().
				Str("writer", sc.PresenceWriter.Name).
				Str("address", sc.PresenceWriter.Socket+sc.PresenceWriter.URL).
				Dur("relay_interval", presence.DeriveRelayInterval(
					sc.SyncOnlineTimeout, sc.LastActiveGranularity)).
				Msg("presence relaying enabled")
		}
	}

	authenticator, err := auth.New(auth.Config{
		WhoamiSocket: cfg.Auth.WhoamiSocket,
		WhoamiURL:    cfg.Auth.WhoamiURL,
		PositiveTTL:  cfg.Auth.Positive(),
		NegativeTTL:  cfg.Auth.Negative(),
		MaxEntries:   cfg.Auth.Entries(),
		Timeout:      cfg.Auth.Timeout(),
	})
	if err != nil {
		return err
	}

	if cfg.Testing.AllowPinNow {
		// Loud on purpose. The pin accepts a window that has not happened yet,
		// so a worker left in this mode would serve answers about the future.
		log.Warn().Msg("testing.allow_pin_now is enabled: ?_gosync_now= is accepted. Never enable this in production.")
	}

	spec := listenSpec(cfg)
	if checkOnly {
		log.Info().
			Str("listen", server.Describe(spec)).
			Bool("database_read_only", readOnly).
			Bool("to_device", inbox != nil).
			Str("replication_channel", cfg.Replication.Channel).
			Msg("configuration and database access are usable")
		return nil
	}

	// The notifier is the replication subscriber's listener: a stream advancing
	// is what wakes a waiting sync.
	notif := notifier.New()
	sub := replication.New(replication.Config{
		Enabled:  cfg.Replication.Enabled,
		Address:  cfg.Replication.Address,
		Channel:  cfg.Replication.Channel,
		Password: cfg.Replication.Password,
		DB:       cfg.Replication.DB,
	}, log, notif)

	// The stream-change caches ride alongside the notifier rather than inside
	// it. Both read every row; see streamfeed.go for why they cannot share a
	// reading of one.
	sub.AddListener(&streamFeeder{db: db})
	metrics.RegisterStreamPositions(sub.Positions)

	// The state caches hold data that is immutable but not permanent: a purged
	// room's state groups stay valid right up until they stop existing. While
	// we follow the stream that deletion is visible; while we do not, it is
	// not -- so a lost connection discards them.
	sub.SetOnDrop(func() {
		groups, filtered := db.CacheLen()
		db.PurgeCaches()
		// The derived caches are DISARMED, not merely emptied. Their
		// invalidations arrive over the connection that has just gone: while
		// it is down, every answer they could give is a guess about what has
		// changed since, and a guess is not a fallback.
		db.DisarmDerivedCaches()
		// Same reasoning, different failure: a disarmed stream cache answers
		// "changed" to everything, which is precisely the behaviour this worker
		// had before it had one. An armed cache with a stale horizon would
		// answer "unchanged" for events it never saw arrive.
		db.DisarmStreamCaches()
		log.Warn().Int("state_groups", groups).Int("filtered_state", filtered).
			Msg("replication dropped; discarded state caches, disarmed derived and stream caches")
	})
	sub.SetOnConnect(func(positions map[string]int64) {
		db.ArmDerivedCaches(positions)

		// Prefill runs on every connect, not just the first. Arming without it
		// leaves each horizon at "now": every question falls below it, every
		// gate says "changed", and the only symptom is queries that never went
		// away. Six scans, bounded, off the request path.
		//
		// If it fails the caches stay disarmed rather than armed-and-empty --
		// an empty cache above a "now" horizon is useless, but an empty cache
		// below one is wrong.
		prefillCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		err := db.PrefillStreamCaches(prefillCtx, positions)
		cancel()
		if err != nil {
			db.DisarmStreamCaches()
			log.Error().Err(err).Msg("replication live; stream caches left disarmed")
			return
		}
		log.Info().Msg("replication live; derived and stream caches armed")
	})
	if cfg.Caches.Enabled {
		sub.SetInvalidator(&cacheInvalidator{db: db, log: log})
	} else {
		log.Warn().Msg("caches.enabled is false: every derived cache is disabled, and this worker will query the database for everything")
	}

	// Seed from the database so the worker has an answer before any traffic
	// arrives. Every seeded value is a lower bound that the first row on that
	// stream replaces.
	if seed, err := db.StreamPositions(ctx); err != nil {
		log.Warn().Err(err).Msg("could not seed stream positions")
	} else {
		sub.Seed(seed)
	}
	go sub.Run(ctx)

	// Sliding sync's per-connection state: the project's second write, in its
	// own schema behind its own role. Nil leaves both sliding sync paths
	// unregistered, so probing either returns M_UNRECOGNIZED.
	var sliding *slidingstore.Store
	if cfg.SlidingSync.Enabled {
		sliding, err = slidingstore.Open(openCtx, slidingstore.Config{
			DSN:            cfg.SlidingSync.DSN,
			MaxConns:       int32(cfg.SlidingSync.MaxConns),
			ConnectTimeout: time.Duration(cfg.SlidingSync.ConnectTimeoutSeconds) * time.Second,
		})
		if err != nil {
			return err
		}
		defer sliding.Close()

		// The materialised tables sliding sync reads are maintained by
		// Synapse's event persister. If its background updates have not
		// finished, or a room is queued for recomputation, they are incomplete
		// -- and a room silently missing from a client's list is the failure
		// nobody reports as a bug. Synapse has a slow fallback path; we refuse.
		ready, why, err := db.SlidingSyncTablesReady(openCtx)
		if err != nil {
			return err
		}
		if !ready {
			return fmt.Errorf("sliding sync cannot be served yet: %s", why)
		}

		metrics.RegisterSlidingSyncStore(func() (metrics.SlidingStoreCounts, error) {
			// A scrape must not hang on the database, so it gets its own short
			// deadline rather than the process's lifetime context.
			cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			c, err := sliding.Count(cctx)
			if err != nil {
				return metrics.SlidingStoreCounts{}, err
			}
			return metrics.SlidingStoreCounts{
				Connections: c.Connections, Positions: c.Positions, Rows: c.Rows,
			}, nil
		})

		go reapSlidingConnections(ctx, sliding, cfg.SlidingSync.ReapIntervalMinutes, log)
		log.Info().Msg("sliding sync enabled")
	}

	deps := handlers.Deps{
		Store:          db,
		Sliding:        sliding,
		MSC3575Enabled: cfg.Experimental.MSC3575Enabled,
		MSC4308Enabled: cfg.Experimental.MSC4308Enabled,
		ServerName:     cfg.ServerName,
		Auth:           authenticator,
		Log:            log,
		AllowPinNow:    cfg.Testing.AllowPinNow,
		MSC4354Enabled: cfg.Experimental.MSC4354Enabled,
		MSC3391Enabled: cfg.Experimental.MSC3391Enabled,
		MSC4222Enabled: cfg.Experimental.MSC4222Enabled,
		MSC3773Enabled: cfg.Experimental.MSC3773Enabled,
		MSC3874Enabled: cfg.Experimental.MSC3874Enabled,
		// Synapse caps an inline filter's timeline limit at 100 by default.
		FilterTimelineLimit: cfg.TimelineLimitCap(),
		Replication:         sub,
		Notifier:            notif,
		LazyLoad: lazyload.New(cfg.Caches.LazyLoadMembersCacheSize,
			cfg.Caches.LazyLoadMembersCacheTTL),
		Inbox:    inbox,
		Presence: presenceClient,
		PushRuleFeatures: pushrules.Features{
			MSC1767Enabled:             cfg.Experimental.MSC1767Enabled,
			MSC3381PollsEnabled:        cfg.Experimental.MSC3381PollsEnabled,
			MSC3664Enabled:             cfg.Experimental.MSC3664Enabled,
			MSC4028PushEncryptedEvents: cfg.Experimental.MSC4028PushEncryptedEvents,
			MSC4210Enabled:             cfg.Experimental.MSC4210Enabled,
			MSC4306Enabled:             cfg.Experimental.MSC4306Enabled,
		},
	}
	mux := server.NewMux(server.Routes{
		RoomInitialSync: handlers.RoomInitialSync(deps),
		InitialSync:     handlers.InitialSync(deps),
		Events:          handlers.Events(deps),
		Sync:            handlers.Sync(deps),
		SlidingSync:     slidingSyncRoute(deps, sliding),
	})
	// CORS inside the request log, so a preflight is still logged -- it is the
	// first thing a browser client sends, and an unlogged 204 is invisible
	// when working out whether a client reached us at all.
	handler := server.WithRequestLog(log, server.WithCORS(mux))

	listener, err := server.Listen(spec)
	if err != nil {
		return err
	}

	httpServer := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       120 * time.Second,
		// Deliberately no WriteTimeout. A /sync long-poll legitimately holds a
		// connection open for the client's whole timeout -- thirty seconds is
		// the common case and Synapse permits far more -- and a WriteTimeout
		// would sever exactly the requests that are working correctly.
	}

	errs := make(chan error, 1)
	go func() {
		log.Info().
			Str("listen", server.Describe(spec)).
			Str("version", tag).
			Msg("serving")
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			errs <- err
		}
	}()

	// A SECOND listener, for metrics only.
	//
	// The client API is served on a unix socket, and Prometheus cannot scrape
	// one. Without this, `metrics.addr` would be configuration that does
	// nothing and every dashboard panel would be empty -- which is worse than
	// having no dashboard, because it looks like the worker is idle.
	//
	// /metrics is unauthenticated, so this belongs on an internal network and
	// must never be reachable through the reverse proxy. Only /_matrix paths
	// should be public.
	var metricsServer *http.Server
	if cfg.Metrics.Addr != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("GET /metrics", promhttp.Handler())
		metricsMux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		})
		metricsServer = &http.Server{
			Addr:              cfg.Metrics.Addr,
			Handler:           metricsMux,
			ReadHeaderTimeout: 15 * time.Second,
		}
		go func() {
			log.Info().Str("addr", cfg.Metrics.Addr).Msg("serving metrics")
			if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				errs <- err
			}
		}()
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errs:
		return err
	case sig := <-signals:
		log.Info().Str("signal", sig.String()).Msg("shutting down")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if metricsServer != nil {
		_ = metricsServer.Shutdown(shutdownCtx)
	}
	return httpServer.Shutdown(shutdownCtx)
}

func listenSpec(cfg *config.Config) server.ListenSpec {
	mode, _ := cfg.Listen.ParsedSocketMode() // validated at load
	return server.ListenSpec{
		Socket:     cfg.Listen.Socket,
		Addr:       cfg.Listen.Addr,
		SocketMode: mode,
	}
}

func newLogger(cfg config.Log) zerolog.Logger {
	level, err := zerolog.ParseLevel(strings.ToLower(cfg.Level))
	if err != nil || cfg.Level == "" {
		level = zerolog.InfoLevel
	}
	var log zerolog.Logger
	if cfg.Pretty {
		log = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	} else {
		log = zerolog.New(os.Stderr)
	}
	return log.Level(level).With().Timestamp().Logger()
}

// cacheSize resolves one derived cache's bound.
//
// A negative size disables a cache in internal/lru, so the master switch is
// expressed the same way rather than as a second mechanism the caches would
// each have to check.
func cacheSize(enabled bool, configured int) int {
	if !enabled {
		return -1
	}
	return configured
}

// slidingSyncRoute returns the handler only when the connection store exists.
//
// A nil handler leaves both paths unregistered, so a probe gets
// M_UNRECOGNIZED rather than a 500 on the first request -- which is what a
// client should see from a server that does not implement the endpoint.
func slidingSyncRoute(deps handlers.Deps, sliding *slidingstore.Store) http.Handler {
	if sliding == nil {
		return nil
	}
	return handlers.SlidingSync(deps)
}

// reapSlidingConnections collects connections nobody has used for a week.
//
// Not optional. Reading a position prunes that connection's own forks and
// nothing else, so a client that simply stops coming back leaves its connection
// and every row hanging off it behind for ever. Synapse runs the same job
// hourly (CONNECTION_EXPIRY_FREQUENCY).
func reapSlidingConnections(ctx context.Context, sliding *slidingstore.Store,
	intervalMinutes int, log zerolog.Logger) {

	if intervalMinutes <= 0 {
		intervalMinutes = 60
	}
	ticker := time.NewTicker(time.Duration(intervalMinutes) * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reapCtx, cancel := context.WithTimeout(ctx, time.Minute)
			n, err := sliding.DeleteOldConnections(reapCtx)
			cancel()
			if err != nil {
				log.Error().Err(err).Msg("could not reap sliding sync connections")
				continue
			}
			if n > 0 {
				log.Info().Int64("connections", n).Msg("reaped stale sliding sync connections")
			}
		}
	}
}
