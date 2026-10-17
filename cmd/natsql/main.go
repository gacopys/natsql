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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"

	"github.com/gacopys/natsql/v2"
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
	RunE: func(cmd *cobra.Command, args []string) error { //nolint:revive // cobra convention: cmd is unused
		return runServer()
	},
}

var dropViewCmd = &cobra.Command{
	Use:   "drop-view",
	Short: "Delete a view's materialized state so it is rebuilt on the next start",
	Long: `Delete a view's durable consumer, rows and schema, in the current and the
original key layout, so the next server start replays the view from the
beginning of its source stream.

Run it when the server refuses to start with "view must be rebuilt": after a
view's columns, mappings, key fields, separator or source changed, or after
upgrading from a natsql version with the earlier key layout. Stop every
server materializing the view first. Events the source stream no longer
retains can't be replayed.

  natsql drop-view --config=config.yaml --view=users
  natsql drop-view --config=config.yaml -e --store-dir=./data --view=users`,
	RunE: func(cmd *cobra.Command, args []string) error { //nolint:revive // cobra convention: args is unused
		return runDropView(cmd.Context())
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
	dropViewName  string
)

func init() {
	for _, cmd := range []*cobra.Command{serverCmd, dropViewCmd} {
		cmd.Flags().StringVarP(&cfgPath, "config", "c", "config.yaml", "Path to config file")
		cmd.Flags().BoolVarP(&embedded, "embedded", "e", false, "Start embedded NATS server")
		cmd.Flags().StringVarP(&natsURL, "nats-url", "u", "", "NATS server URL (overrides config)")
		cmd.Flags().StringVar(&storeDir, "store-dir", "", "JetStream store directory (embedded mode)")
	}
	serverCmd.Flags().IntVarP(&httpPort, "port", "p", 0, "HTTP query API port (overrides config)")
	serverCmd.Flags().BoolVar(&createStreams, "create-streams", false, "Create source streams in external mode (embedded: always created)")
	dropViewCmd.Flags().StringVar(&dropViewName, "view", "", "Name of the view to drop (required)")
	_ = dropViewCmd.MarkFlagRequired("view") // only fails for an unknown flag name
	rootCmd.AddCommand(serverCmd, dropViewCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// loadConfig reads the config file and applies the CLI flag overrides (D-52).
func loadConfig() (*natsql.Config, error) {
	cfg, err := natsql.LoadConfig(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("loading config: %w", err)
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
	return cfg, nil
}

func runServer() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	// Handle signals from the start, so SIGTERM during startup still shuts
	// down cleanly (OP-09).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := slog.Default()
	connClosed := make(chan struct{})
	eng, err := createEngine(cfg, logger, connClosed)
	if err != nil {
		return fmt.Errorf("creating engine: %w", err)
	}

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
		_ = eng.Close() // releases the embedded server or the connection; the Start error is the one to report
		return fmt.Errorf("starting engine: %w", err)
	}
	logger.Info("engine started")

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("received signal, shutting down")
	case <-connClosed:
		// OP-02: the connection gave up for good; exit non-zero so a
		// supervisor restarts the process instead of it idling as "up".
		runErr = errors.New("NATS connection closed")
		logger.Error("NATS connection closed, shutting down")
	}

	if err := eng.Close(); err != nil {
		return errors.Join(runErr, fmt.Errorf("error during shutdown: %w", err))
	}
	logger.Info("server shut down gracefully")
	return runErr
}

func runDropView(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if cfg.NATS.Embedded && cfg.NATS.StoreDir == "" {
		return errors.New("drop-view in embedded mode needs --store-dir or nats.store_dir: without one the embedded store is temporary")
	}

	eng, err := createEngine(cfg, slog.Default(), nil)
	if err != nil {
		return fmt.Errorf("creating engine: %w", err)
	}
	defer func() { _ = eng.Close() }() // never started: only releases the connection

	if err := eng.DropView(ctx, dropViewName); err != nil {
		return fmt.Errorf("dropping view %q: %w", dropViewName, err)
	}
	fmt.Printf("view %q dropped; it is rebuilt from its source stream on the next start\n", dropViewName)
	return nil
}

// createEngine connects to NATS (or starts the embedded server) and creates
// the engine. In external mode the connection retries forever through
// outages and closes connClosed, if non-nil, once it has closed for good.
func createEngine(cfg *natsql.Config, logger *slog.Logger, connClosed chan struct{}) (*natsql.Engine, error) {
	if cfg.NATS.Embedded {
		logger.Info("creating engine with embedded NATS")
		return natsql.NewEmbedded(cfg, natsql.WithLogger(logger))
	}
	logger.Info("connecting to NATS", "url", cfg.NATS.URL)
	var closeOnce sync.Once
	nc, cerr := nats.Connect(cfg.NATS.URL,
		nats.Timeout(10*time.Second),
		nats.MaxReconnects(-1), // OP-02: never give up on an outage
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Warn("NATS disconnected, reconnecting", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("NATS reconnected", "url", nc.ConnectedUrlRedacted())
		}),
		nats.ClosedHandler(func(*nats.Conn) {
			if connClosed != nil {
				closeOnce.Do(func() { close(connClosed) })
			}
		}),
	)
	if cerr != nil {
		return nil, fmt.Errorf("connecting to NATS at %s: %w", cfg.NATS.URL, cerr)
	}
	return natsql.NewWithNATS(nc, cfg, natsql.WithLogger(logger))
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
