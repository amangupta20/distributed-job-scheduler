package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()

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

	// Run the worker loop in background
	go func() {
		w.Run(ctx)
	}()

	// The signal context stops the claim loop. Execution contexts are separate and
	// remain valid until completion or the configured drain timeout.
	<-ctx.Done()
	log.Info("received shutdown signal, draining...")
	w.BeginDrain()

	// Mark worker as draining
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
	if err := w.Deregister(drainCtx); err != nil {
		log.Warn("deregister worker", "err", err)
	}
	cancelDrain()

	// Wait for active jobs to finish (up to drain timeout)
	w.Drain(cfg.DrainTimeout())
	offlineCtx, cancelOffline := context.WithTimeout(context.Background(), 5*time.Second)
	if err := w.MarkOffline(offlineCtx); err != nil {
		log.Warn("mark worker offline", "err", err)
	}
	cancelOffline()

	log.Info("worker shutdown complete")
}
