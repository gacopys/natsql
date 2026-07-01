// Command natsql is the standalone binary for the NATS-native materialized view engine.
//
// It provides a cobra-based CLI with a `server` subcommand supporting three
// deployment modes (D-51):
//   - External NATS via config file (default)
//   - Embedded NATS (--embedded)
//   - Explicit external NATS URL (--nats-url)
//
// Config file is the primary source; CLI flags override matching config fields (D-52).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"

	"github.com/gacopys/natsql"
)

var rootCmd = &cobra.Command{
	Use:   "natsql",
	Short: "NATS-native materialized view engine",
	Long: `natsql is a NATS-native materialized view engine.

Define stream-to-KV materializations declaratively (YAML/JSON) and query
the resulting state with read-only SQL via NATS request-reply or HTTP.

For NATS developers building event-driven systems who need simple queryable
state without running Postgres, Redis, or Kafka alongside their NATS cluster.`,
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the natsql server",
	Long: `Start the natsql server with configured materialized views.

Three deployment modes (D-51):
  natsql server --config=config.yaml         External NATS (config or default URL)
  natsql server --config=config.yaml -e      Embedded NATS (zero infrastructure)
  natsql server --config=config.yaml -u nats://host:4222  Explicit NATS URL

CLI flags override config file values (D-52).`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runServer()
	},
}

// CLI flags.
var (
	cfgPath       string
	embedded      bool
	natsURL       string
	storeDir      string
	httpPort      int
	createStreams bool
)

func init() {
	serverCmd.Flags().StringVarP(&cfgPath, "config", "c", "config.yaml", "Path to config file")
	serverCmd.Flags().BoolVarP(&embedded, "embedded", "e", false, "Start embedded NATS server")
	serverCmd.Flags().StringVarP(&natsURL, "nats-url", "u", "", "NATS server URL (overrides config)")
	serverCmd.Flags().StringVar(&storeDir, "store-dir", "", "JetStream store directory (embedded mode)")
	serverCmd.Flags().IntVarP(&httpPort, "port", "p", 0, "HTTP query API port (overrides config)")
	serverCmd.Flags().BoolVar(&createStreams, "create-streams", false, "Create source streams in external mode (embedded: always created)")
	rootCmd.AddCommand(serverCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runServer() error {
	cfg, err := natsql.LoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	if embedded {
		cfg.NATS.Embedded = true
	}
	if natsURL != "" {
		cfg.NATS.URL = natsURL
	}
	if storeDir != "" {
		cfg.NATS.StoreDir = storeDir
	}
	if httpPort > 0 {
		cfg.HTTP.Port = httpPort
	}

	cfg.SetDefaults()

	logger := slog.Default()
	eng, err := createEngine(cfg, logger)
	if err != nil {
		return fmt.Errorf("creating engine: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	createSourceStreams(ctx, eng, cfg, logger)

	mode := "external"
	if cfg.NATS.Embedded {
		mode = "embedded"
	}
	natsDisplay := cfg.NATS.URL
	if cfg.NATS.Embedded {
		natsDisplay = "embedded"
	}

	logger.Info("starting natsql server",
		"mode", mode,
		"nats_url", natsDisplay,
		"views", len(cfg.Views),
		"http_port", cfg.HTTP.Port,
	)
	for _, v := range cfg.Views {
		logger.Info("configured view", "name", v.Name, "source_stream", v.SourceStream)
	}

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("starting engine: %w", err)
	}
	logger.Info("engine started")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("received signal, shutting down", "signal", sig)

	if err := eng.Close(); err != nil {
		return fmt.Errorf("error during shutdown: %w", err)
	}
	logger.Info("server shut down gracefully")
	return nil
}

func createEngine(cfg *natsql.Config, logger *slog.Logger) (*natsql.Engine, error) {
	if cfg.NATS.Embedded {
		logger.Info("creating engine with embedded NATS")
		return natsql.NewEmbedded(cfg)
	}
	logger.Info("connecting to NATS", "url", cfg.NATS.URL)
	nc, cerr := nats.Connect(cfg.NATS.URL, nats.Timeout(10*time.Second))
	if cerr != nil {
		return nil, fmt.Errorf("connecting to NATS at %s: %w", cfg.NATS.URL, cerr)
	}
	return natsql.NewWithNATS(nc, cfg)
}

func createSourceStreams(ctx context.Context, eng *natsql.Engine, cfg *natsql.Config, logger *slog.Logger) {
	if !cfg.NATS.Embedded && !createStreams {
		logger.Warn("external mode: not creating source streams (use --create-streams to opt in)")
		return
	}
	js, jserr := jetstream.New(eng.NC())
	if jserr != nil {
		return
	}
	seen := map[string]bool{}
	for _, v := range cfg.Views {
		if seen[v.SourceStream] {
			continue
		}
		seen[v.SourceStream] = true

		subjects := []string{v.SourceSubject}
		if v.SourceSubject == "" {
			subjects = []string{v.SourceStream + ".>"}
		}

		if !cfg.NATS.Embedded {
			if _, err := js.Stream(ctx, v.SourceStream); err == nil {
				logger.Info("source stream already exists, skipping", "stream", v.SourceStream)
				continue
			}
		}

		_, serr := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     v.SourceStream,
			Subjects: subjects,
		})
		if serr != nil {
			logger.Warn("failed to create source stream", "stream", v.SourceStream, "error", serr)
		} else {
			logger.Info("created source stream", "stream", v.SourceStream, "subjects", subjects)
		}
	}
}
