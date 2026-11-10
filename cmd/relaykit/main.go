package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Yermukhambet-raiymbek/relaykit/internal/relay"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func run() error {
	token := os.Getenv("RELAY_API_TOKEN")
	secret := os.Getenv("RELAY_SIGNING_SECRET")
	if len(token) < 24 || len(secret) < 24 {
		return errors.New("set RELAY_API_TOKEN and RELAY_SIGNING_SECRET to separate random values of at least 24 characters")
	}
	if token == secret {
		return errors.New("API token and signing secret must differ")
	}
	path := env("RELAY_DB", "./data/relaykit.db")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	// New database/WAL files must not be world-readable; payloads may be private.
	syscall.Umask(0077)
	store, err := relay.Open(path)
	if err != nil {
		return err
	}
	defer store.Close()
	policy := relay.Policy{AllowLocalDemo: os.Getenv("RELAY_ALLOW_LOCAL_DEMO") == "true"}
	if policy.AllowLocalDemo {
		slog.Warn("local demo mode enabled: literal loopback HTTP destinations are permitted")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	worker := relay.Worker{Store: store, Client: relay.NewHTTPClient(policy, 10*time.Second), Policy: policy, Secret: secret, Now: time.Now, MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: time.Minute, Lease: 30 * time.Second}
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				worked, e := worker.Step(ctx)
				if e != nil && ctx.Err() == nil {
					slog.Error("worker error", "error", e)
				}
				if worked && e == nil {
					slog.Info("delivery attempt completed")
				}
			}
		}
	}()
	api := relay.API{Store: store, Token: token, Policy: policy, Now: time.Now}
	server := &http.Server{Addr: env("RELAY_ADDR", "127.0.0.1:8080"), Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	errCh := make(chan error, 1)
	go func() { slog.Info("RelayKit listening", "addr", server.Addr); errCh <- server.ListenAndServe() }()
	select {
	case e := <-errCh:
		cancel()
		<-workerDone
		return e
	case <-ctx.Done():
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer stopCancel()
	err = server.Shutdown(stopCtx)
	<-workerDone
	return err
}
func main() {
	if err := run(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("stopped", "error", err)
		os.Exit(1)
	}
}
