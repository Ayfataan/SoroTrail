// Command sorotrail runs the SoroTrail indexer: a Stellar RPC event ingester
// and a query API in one process.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/khaylebfortune/sorotrail/internal/api"
	"github.com/khaylebfortune/sorotrail/internal/config"
	"github.com/khaylebfortune/sorotrail/internal/decode"
	"github.com/khaylebfortune/sorotrail/internal/ingester"
	"github.com/khaylebfortune/sorotrail/internal/plugins"
	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sorotrail:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := store.Migrate(cfg.DatabaseURL); err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("pinging postgres: %w", err)
	}

	st := store.NewPostgres(pool)
	for _, id := range cfg.WatchedContracts {
		if err := st.AddWatchedContract(ctx, id); err != nil {
			return err
		}
	}

	rpcClient := rpc.NewHTTPClient(cfg.RPCURL)

	pluginMgr, err := plugins.NewManager(ctx, cfg.DecoderPluginsDir, plugins.Limits{
		Timeout:       int64(cfg.PluginTimeoutMS),
		MemoryMiB:     cfg.PluginMemoryMiB,
		OutputCap:     cfg.PluginOutputMaxBytes,
		FailThreshold: cfg.PluginFailThreshold,
	}, log)
	if err != nil {
		return fmt.Errorf("loading decoder plugins: %w", err)
	}
	defer pluginMgr.Close(ctx)

	ing := ingester.New(rpcClient, st, decode.XDRDecoder{}, pluginMgr, log, ingester.Options{
		PollInterval:     cfg.PollInterval,
		StartLedger:      cfg.StartLedger,
		RetentionLedgers: cfg.RetentionLedgers,
	})

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(st, rpcClient, log).Router(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		log.Info("ingester starting", "rpc_url", cfg.RPCURL, "poll_interval", cfg.PollInterval)
		if err := ing.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("ingester: %w", err)
		} else {
			errCh <- nil
		}
	}()
	go func() {
		log.Info("http api listening", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		} else {
			errCh <- nil
		}
	}()

	var firstErr error
	remaining := 2 // both goroutines send exactly once
	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case firstErr = <-errCh:
		remaining--
		stop() // one component failed; wind down the other
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "error", err)
	}
	for ; remaining > 0; remaining-- {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	log.Info("shutdown complete")
	return firstErr
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
