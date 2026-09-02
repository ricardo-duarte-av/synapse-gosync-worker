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

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/auth"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/config"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/deviceinbox"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/handlers"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/lazyload"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/metrics"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/notifier"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/pushrules"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/replication"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/server"
	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/store"
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
		DSN:            cfg.Database.DSN,
		MaxConns:       cfg.Database.Conns(),
		ConnectTimeout: cfg.Database.ConnectTimeout(),
	})
	if err != nil {
		return err
	}
	defer db.Close()

	// Reported rather than enforced. Refusing to start against a writable role
	// would be the wrong trade in a deployment that has not run
	// readonly-role.sql yet, but running against one unknowingly is exactly the
	// situation the SQL file exists to prevent.
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

	// Seed from the database so the worker has an answer before any traffic
	// arrives. Every seeded value is a lower bound that the first row on that
	// stream replaces.
	if seed, err := db.StreamPositions(ctx); err != nil {
		log.Warn().Err(err).Msg("could not seed stream positions")
	} else {
		sub.Seed(seed)
	}
	go sub.Run(ctx)

	deps := handlers.Deps{
		Store:          db,
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
		Inbox: inbox,
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
	})
	handler := server.WithRequestLog(log, mux)

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
