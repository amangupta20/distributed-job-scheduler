package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/pulsequeue/worker/internal/config"
	"github.com/pulsequeue/worker/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(log)

	cfg := config.Load()

	log.Info("starting PulseQueue worker",
		"name", cfg.WorkerName,
		"version", cfg.WorkerVersion,
		"concurrency", cfg.Concurrency,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to database with retry
	pool, err := worker.WaitForDB(ctx, cfg.DatabaseURL, log)
	if err != nil {
		log.Error("failed to connect to database", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	w := worker.New(cfg, pool, log)

	// Register worker
	if err := w.Register(ctx); err != nil {
		log.Error("failed to register worker", "err", err)
		os.Exit(1)
	}
	log.Info("worker registered", "id", w.ID())

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	// Run the worker loop in background
	go func() {
		w.Run(ctx)
	}()

	// Wait for shutdown signal
	sig := <-sigCh
	log.Info("received shutdown signal, draining...", "signal", sig)
	cancel()

	// Mark worker as draining
	drainCtx := context.Background()
	if err := w.Deregister(drainCtx); err != nil {
		log.Warn("deregister worker", "err", err)
	}

	// Wait for active jobs to finish (up to drain timeout)
	w.Drain(cfg.DrainTimeout())

	log.Info("worker shutdown complete")
}
