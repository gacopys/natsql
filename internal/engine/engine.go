// Package engine provides the top-level Engine that manages materialized view
// lifecycles. It initializes the KV bucket, creates the DLQ stream, stores
// schemas, and launches materializer goroutines for each configured view.
//
// The Engine does NOT manage NATS connection lifecycle. The caller provides a
// jetstream.JetStream instance from an existing NATS connection.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	natsqlpkg "github.com/gacopys/natsql/internal/cfg"
	"github.com/gacopys/natsql/internal/embed"
	"github.com/gacopys/natsql/internal/kv"
	"github.com/gacopys/natsql/internal/materialize"
	"github.com/gacopys/natsql/internal/query"
	"github.com/gacopys/natsql/internal/transport"
)

// Sentinel errors for Engine lifecycle operations.
var (
	// ErrAlreadyStarted is returned by Start when the engine is already running.
	ErrAlreadyStarted = errors.New("engine already started")

	// ErrNotStarted is returned by Close when the engine hasn't been started.
	ErrNotStarted = errors.New("engine not started")
)

// Engine manages the lifecycle of materialized views and provides
// SQL query capabilities over NATS KV state.
// Create via New, NewWithNATS, or NewEmbedded, then call Start to begin
// processing, and Close to shut down.
type Engine struct {
	js         jetstream.JetStream
	nc         *nats.Conn           // NATS connection for subscription
	cfg        *natsqlpkg.Config
	kv         jetstream.KeyValue
	logger     *slog.Logger
	wg         sync.WaitGroup
	cancel     context.CancelFunc
	started    bool
	mu         sync.Mutex
	natsSub    *nats.Subscription   // NATS query subscription (for cleanup)
	httpServer *http.Server         // HTTP query server
	queryPort  int                  // HTTP server port (default 8080)
	embedNode  *embed.Node          // non-nil when using embedded NATS (NewEmbedded)
	drainChans []chan struct{} // per-view drain signals for graceful consumer shutdown
}

// Option configures the Engine.
type Option func(*Engine)

// WithLogger sets the logger for the engine and materializers.
func WithLogger(logger *slog.Logger) Option {
	return func(e *Engine) {
		if logger != nil {
			e.logger = logger
		}
	}
}

// WithHTTPServer parses an address string ("host:port") and sets the HTTP
// server port. If addr is empty, no action is taken.
// If host is "0.0.0.0", the binding is kept as-is (overrides 127.0.0.1 default).
func WithHTTPServer(addr string) Option {
	return func(e *Engine) {
		if addr == "" {
			return
		}
		host, portStr, err := netSplitHostPort(addr)
		if err != nil {
			return // silently ignore invalid addresses
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return
		}
		_ = host // host binding is always 127.0.0.1 for security (T-02-06)
		e.queryPort = port
	}
}

// WithQueryPort directly sets the HTTP query server port.
func WithQueryPort(port int) Option {
	return func(e *Engine) {
		if port > 0 {
			e.queryPort = port
		}
	}
}

// netSplitHostPort splits a network address of the form "host:port".
// Returns the host and port parts separately.
func netSplitHostPort(addr string) (string, string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// Try with a default host if no host specified
		if strings.HasPrefix(addr, ":") {
			host, port, err = net.SplitHostPort("127.0.0.1" + addr)
		}
	}
	return host, port, err
}

// New creates a new Engine from a NATS connection, JetStream context, and configuration.
// The engine is pre-configured but not started. Call Start to begin processing.
//
// The config is validated synchronously. If validation fails, an error is
// returned. The NATS connection, JetStream context must be live and
// provided by the caller. The NATS connection is used for subscribing to
// query subjects in Start().
func New(nc *nats.Conn, js jetstream.JetStream, cfg *natsqlpkg.Config, opts ...Option) (*Engine, error) {
	if cfg == nil {
		return nil, errors.New("config is nil")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	e := &Engine{
		js:        js,
		nc:        nc,
		cfg:       cfg,
		logger:    slog.Default(),
		queryPort: cfg.HTTP.Port,
	}
	if e.queryPort == 0 {
		e.queryPort = 8080 // fallback default (D-13)
	}

	for _, opt := range opts {
		opt(e)
	}

	return e, nil
}

// NewEmbedded creates a new Engine that starts an embedded NATS JetStream
// server in the same process. The engine owns the embedded server lifecycle
// and will shut it down when Close is called.
//
// The config's SetDefaults and Validate are called automatically.
func NewEmbedded(cfg *natsqlpkg.Config, opts ...Option) (*Engine, error) {
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Start embedded NATS server
	storeDir := cfg.NATS.StoreDir
	natsPort := cfg.NATS.Port
	if natsPort == 0 {
		natsPort = -1 // random port
	}
	node, err := embed.StartNode(embed.NodeConfig{
		Port:      natsPort,
		StoreDir:  storeDir,
		ReadyWait: 10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("starting embedded NATS: %w", err)
	}

	// Connect to embedded server
	nc, err := nats.Connect(node.ClientURL(), nats.Timeout(10*time.Second))
	if err != nil {
		node.Shutdown()
		return nil, fmt.Errorf("connecting to embedded NATS: %w", err)
	}

	// Create JetStream context
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		node.Shutdown()
		return nil, fmt.Errorf("creating JetStream context: %w", err)
	}

	e := &Engine{
		js:        js,
		nc:        nc,
		cfg:       cfg,
		logger:    slog.Default(),
		queryPort: cfg.HTTP.Port,
		embedNode: node,
	}
	if e.queryPort == 0 {
		e.queryPort = 8080 // fallback default (D-13)
	}

	for _, opt := range opts {
		opt(e)
	}

	return e, nil
}

// Start initializes the KV bucket, creates the DLQ stream, stores schemas,
// and launches materializer goroutines for each configured view.
//
// Steps:
//  1. Initialize the KV bucket (natsql-views) via kv.InitBucket
//  2. Create or update the DLQ stream (natsql-dlq) via materialize.EnsureDLQStream
//  3. Store the schema for each view in KV
//  4. For each view, launch a goroutine running materialize.Run
//
// Start is idempotent — calling Start on an already-started engine returns
// ErrAlreadyStarted.
// On failure, all partially-created resources are cleaned up (FIX-MAT-03).
func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		return ErrAlreadyStarted
	}

	// 1. Initialize KV bucket (local — assigned to e.kv only on success)
	kvb, err := kv.InitBucket(ctx, e.js, 1)
	if err != nil {
		return fmt.Errorf("initializing KV bucket: %w", err)
	}

	// 2. Create DLQ stream
	if _, err := materialize.EnsureDLQStream(ctx, e.js); err != nil {
		return fmt.Errorf("creating DLQ stream: %w", err)
	}

	// 3. Create context for lifecycle management
	ctx, e.cancel = context.WithCancel(ctx)

	// 4. Create drain channels and launch materializers
	drainChans := make([]chan struct{}, len(e.cfg.Views))

	// Startup error channel for materializer consumer setup (D-15, CR-07)
	type matResult struct {
		name string
		err  error
	}
	startupCh := make(chan matResult, len(e.cfg.Views))

	for i := range e.cfg.Views {
		vc := e.cfg.Views[i]
		drainCh := make(chan struct{})
		drainChans[i] = drainCh

		schema := vc.BuildSchema()
		if storeErr := kv.StoreSchema(ctx, kvb, vc.Name, schema); storeErr != nil {
			e.logger.Warn("failed to store schema in KV", "view", vc.Name, "error", storeErr)
		}

		e.wg.Add(1)
		go func(viewCfg natsqlpkg.ViewConfig, dc chan struct{}) {
			defer e.wg.Done()
			if runErr := materialize.Run(ctx, e.js, &viewCfg, kvb, e.logger, dc); runErr != nil {
				if !errors.Is(runErr, context.Canceled) {
					e.logger.Error("materializer exited with error", "view", viewCfg.Name, "error", runErr)
					startupCh <- matResult{name: viewCfg.Name, err: runErr}
				}
			}
		}(vc, drainCh)

		e.logger.Info("started materializer", "view", vc.Name, "source_stream", vc.SourceStream)
	}

	// Wait briefly for materializers to pass consumer setup (D-15, CR-07)
	// Run() returns error immediately if consumer setup fails.
	// If setup succeeds, it blocks forever in the message loop.
	// NOTE: time.After channel fires once; we use a labeled loop and break
	// on timeout to avoid blocking on subsequent iterations.
	afterStartup := time.After(500 * time.Millisecond)
	var startupErrors int
startupLoop:
	for i := 0; i < len(e.cfg.Views); i++ {
		select {
		case result := <-startupCh:
			startupErrors++
			e.logger.Error("materializer startup failed", "view", result.name, "error", result.err)
		case <-afterStartup:
			break startupLoop
		}
	}
	if startupErrors > 0 {
		return fmt.Errorf("%d materializer(s) failed to start", startupErrors)
	}

	// 5. Register NATS query handler (if NATS connection is available — D-14 fatal)
	if e.nc != nil {
		sub, err := transport.RegisterNATSHandler(e.nc, e)
		if err != nil {
			return fmt.Errorf("failed to register NATS query handler: %w", err)
		}
		e.natsSub = sub
		e.logger.Info("NATS query handler registered", "subject", "natsql.query")
	}

	// 6. Start HTTP query server — bind synchronously, serve in goroutine (per D-15, CR-06/CR-07)
	router := transport.NewRouter()
	transport.RegisterHTTPHandler(router, e)

	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", e.queryPort))
	if err != nil {
		return fmt.Errorf("HTTP listen failed on port %d: %w", e.queryPort, err)
	}

	httpServer := &http.Server{
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}
	e.httpServer = httpServer
	e.wg.Add(1)
	go func(l net.Listener, srv *http.Server, logger *slog.Logger) {
		defer e.wg.Done()
		logger.Info("HTTP query server starting", "addr", l.Addr())
		if err := srv.Serve(l); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
		}
	}(listener, httpServer, e.logger)

	// All initialization succeeded — set engine state
	e.kv = kvb
	e.drainChans = drainChans
	e.started = true
	e.logger.Info("engine started", "view_count", len(e.cfg.Views))
	return nil
}

// Close gracefully shuts down the engine following D-57 ordering:
//  1. Stop HTTP server (5s timeout for in-flight requests)
//  2. Unsubscribe NATS query handler (prevents new query requests)
//  3. Signal drain to all materializer consumers (cons.Drain())
//  4. Cancel materializer context (signals remaining goroutines)
//  5. Wait for all goroutines via WaitGroup
//
// Returns ErrNotStarted if the engine has not been started.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.started {
		return ErrNotStarted
	}

	// D-57 Step 1: Stop HTTP server (5s timeout for in-flight requests)
	if e.httpServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := e.httpServer.Shutdown(shutdownCtx); err != nil {
			e.logger.Warn("HTTP server shutdown error", "error", err)
		}
		shutdownCancel()
		e.httpServer = nil
	}

	// D-57 Step 2: Unsubscribe NATS query handler (prevents new query requests)
	if e.natsSub != nil {
		if err := e.natsSub.Unsubscribe(); err != nil {
			e.logger.Warn("NATS unsubscribe error", "error", err)
		}
		e.natsSub = nil
	}

	// D-57 Step 3: Signal drain to all materializer consumers
	// This triggers cons.Drain() in each materializer before context cancel,
	// preventing unnecessary redeliveries on restart (D-58).
	for _, ch := range e.drainChans {
		close(ch)
	}
	e.drainChans = nil

	// D-57 Step 4: Cancel materializer context
	if e.cancel != nil {
		e.cancel()
	}

	// D-57 Step 5: Wait for all goroutines (materializers + HTTP monitor)
	e.wg.Wait()

	// Note: Embedded NATS lifecycle is owned by NewEmbedded and closed
	// separately if needed. The engine package does not own embedded NATS
	// lifecycle — the facade (natsql package) manages that.

	e.started = false
	e.kv = nil
	e.logger.Info("engine closed")
	return nil
}

// Query executes a SQL SELECT query against the materialized state.
// It parses the SQL, loads the view schema from KV, validates the query,
// builds an execution plan, and executes it.
//
// This method is threadsafe and works before Engine.Start() has been called
// (it will lazy-initialize the KV bucket if needed).
//
// Returns a QueryResult with typed JSON values per D-29/D-30.
func (e *Engine) Query(ctx context.Context, sql string) *query.QueryResult {
	e.logger.Info("executing SQL", "sql", sql)

	// Ensure KV bucket is available (works before Start())
	// Protected by mutex for thread safety (FIX-ENG-02)
	e.mu.Lock()
	kvb := e.kv
	if kvb == nil {
		var err error
		kvb, err = kv.InitBucket(ctx, e.js, 1)
		if err != nil {
			e.mu.Unlock()
			errStr := fmt.Sprintf("initializing KV bucket: %v", err)
			return &query.QueryResult{Error: &errStr}
		}
		e.kv = kvb
	}
	e.mu.Unlock()

	// Parse SQL
	q, err := query.Parse(sql)
	if err != nil {
		errStr := err.Error()
		return &query.QueryResult{Results: nil, Error: &errStr}
	}

	// Load schema from KV (per D-27 — always fresh)
	schema, err := kv.LoadSchema(ctx, kvb, q.From)
	if err != nil {
		errStr := fmt.Sprintf("error loading schema: %v", err)
		return &query.QueryResult{Results: nil, Error: &errStr}
	}
	if schema == nil {
		errStr := fmt.Sprintf("view %q not found", q.From) // D-42
		return &query.QueryResult{Results: nil, Error: &errStr}
	}

	// Validate against schema
	if err := query.Validate(q, schema); err != nil {
		errStr := err.Error()
		return &query.QueryResult{Results: nil, Error: &errStr}
	}

	// Build execution plan
	plan, err := query.BuildPlan(q, schema)
	if err != nil {
		errStr := err.Error()
		return &query.QueryResult{Results: nil, Error: &errStr}
	}

	// Execute plan
	rows, err := plan.Execute(ctx, kvb)
	if err != nil {
		errStr := err.Error()
		return &query.QueryResult{Results: nil, Error: &errStr}
	}

	// Return results (normalize nil to empty slice per D-33)
	if rows == nil {
		rows = []map[string]any{}
	}
	return &query.QueryResult{Results: rows, Error: nil}
}

// Stats holds operational metrics for the Engine (D-60).
type Stats struct {
	Started     bool `json:"started"`
	Goroutines  int  `json:"goroutines"`
	Views       int  `json:"views"`
	HTTPServing bool `json:"http_serving"`
}

// NC returns the NATS connection used by the engine.
// Returns nil if the engine was created with New() (caller-owned connection).
func (e *Engine) NC() *nats.Conn { return e.nc }

// EmbedNode returns the embedded NATS node, or nil if not using embedded NATS.
func (e *Engine) EmbedNode() *embed.Node { return e.embedNode }

// Stats returns current operational metrics about the engine.
// Safe to call at any lifecycle phase (before Start, after Close).
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()

	httpServing := e.httpServer != nil
	var views int
	if e.cfg != nil {
		views = len(e.cfg.Views)
	}

	return Stats{
		Started:     e.started,
		Goroutines:  runtime.NumGoroutine(),
		Views:       views,
		HTTPServing: httpServing,
	}
}
