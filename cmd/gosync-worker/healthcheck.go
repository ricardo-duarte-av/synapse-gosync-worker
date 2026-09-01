package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/ricardo-duarte-av/synapse-gosync-worker/internal/config"
)

// probeHealth requests /health over the transport the worker is configured to
// serve on.
//
// The runtime image is distroless, so there is no shell and no curl for a
// Docker HEALTHCHECK to use. Probing over the configured transport rather than
// a fixed port also means the check exercises the unix socket in the
// deployment that uses one -- a TCP-only probe against a socket-serving worker
// would pass while nothing could actually reach it.
func probeHealth(cfg *config.Config) error {
	transport := &http.Transport{}
	url := "http://localhost/health"
	if cfg.Listen.Socket != "" {
		socket := cfg.Listen.Socket
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		}
	} else {
		url = "http://" + cfg.Listen.Addr + "/health"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: status %d", resp.StatusCode)
	}
	return nil
}
