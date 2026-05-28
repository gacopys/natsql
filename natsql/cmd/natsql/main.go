// Command natsql is the standalone binary for the NATSQL materialized view engine.
//
// It loads a YAML/JSON configuration file, connects to NATS, creates the engine,
// and starts processing events. The process blocks until SIGINT or SIGTERM is
// received, then performs a graceful shutdown.
//
// Usage:
//
//	natsql [config-path]
//
// If no config-path is provided, defaults to "config.yaml" in the current directory.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	natsqlpkg "natsql"
	"natsql/engine"
)

func main() {
	// 1. Parse config path from args (default: "config.yaml")
	cfgPath := "config.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	// 2. Load config
	cfg, err := natsqlpkg.LoadConfig(cfgPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	slog.Info("config loaded", "path", cfgPath, "views", len(cfg.Views))

	// 3. Connect to NATS
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		slog.Error("failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()
	slog.Info("connected to NATS", "url", nats.DefaultURL)

	// 4. Create JetStream context
	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("failed to create JetStream context", "error", err)
		os.Exit(1)
	}

	// 5. Create engine
	eng, err := engine.New(js, cfg)
	if err != nil {
		slog.Error("failed to create engine", "error", err)
		os.Exit(1)
	}

	// 6. Start engine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := eng.Start(ctx); err != nil {
		slog.Error("failed to start engine", "error", err)
		os.Exit(1)
	}
	slog.Info("engine started")

	// 7. Wait for SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("received signal, shutting down", "signal", sig)

	// 8. Graceful shutdown
	if err := eng.Close(); err != nil {
		slog.Error("error during shutdown", "error", err)
		os.Exit(1)
	}
	slog.Info("engine shut down gracefully")
}
