package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/brian-nunez/scheduler-worker/internal/worker"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := worker.LoadConfigFromEnv()
	if err != nil {
		logger.Error("invalid scheduler config", "error", err)
		os.Exit(1)
	}

	svc, err := worker.New(cfg, logger)
	if err != nil {
		logger.Error("failed to create scheduler worker", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- svc.Start(ctx)
	}()

	select {
	case err = <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("scheduler worker stopped with error", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := svc.Stop(shutdownCtx); err != nil {
		logger.Error("scheduler worker shutdown failed", "error", err)
		os.Exit(1)
	}

	logger.Info("scheduler worker exited cleanly")
}
