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
	"net/http"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	natsqlpkg "natsql"
	"natsql/kv"
	"natsql/materialize"
	"natsql/query"
	"natsql/transport"
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
// Create via New, then call Start to begin processing, and Close to shut down.
type Engine struct {
	js         jetstream.JetStream
	nc         *nats.Conn          // NATS connection for subscription
	cfg        *natsqlpkg.Config
	kv         jetstream.KeyValue
	logger     *slog.Logger
	wg         sync.WaitGroup
	cancel     context.CancelFunc
	started    bool
	mu         sync.Mutex
	natsSub    *nats.Subscription  // NATS query subscription (for cleanup)
	httpServer *http.Server        // HTTP query server
	queryPort  int                 // HTTP server port (default 8080)
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
		queryPort: 8080,
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
func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		return ErrAlreadyStarted
	}

	// 1. Initialize KV bucket
	var err error
	e.kv, err = kv.InitBucket(ctx, e.js, 1)
	if err != nil {
		return fmt.Errorf("initializing KV bucket: %w", err)
	}

	// 2. Create DLQ stream
	dlqStream, err := materialize.EnsureDLQStream(ctx, e.js)
	if err != nil {
		return fmt.Errorf("creating DLQ stream: %w", err)
	}

	// 3. Create context for lifecycle management
	ctx, e.cancel = context.WithCancel(ctx)

	// 4. Store schemas and launch materializers
	for i := range e.cfg.Views {
		vc := e.cfg.Views[i] // copy to avoid loop variable capture

		// Store schema in KV (non-fatal if it fails)
		schema := vc.BuildSchema()
		if storeErr := kv.StoreSchema(ctx, e.kv, vc.Name, schema); storeErr != nil {
			e.logger.Warn("failed to store schema in KV", "view", vc.Name, "error", storeErr)
		}

		// Launch materializer goroutine
		e.wg.Add(1)
		go func(viewCfg natsqlpkg.ViewConfig) {
			defer e.wg.Done()
			if runErr := materialize.Run(ctx, e.js, &viewCfg, e.kv, dlqStream, e.logger); runErr != nil {
				if !errors.Is(runErr, context.Canceled) {
					e.logger.Error("materializer exited with error", "view", viewCfg.Name, "error", runErr)
				}
			}
		}(vc)

		e.logger.Info("started materializer", "view", vc.Name, "source_stream", vc.SourceStream)
	}

	// 5. Register NATS query handler (if NATS connection is available)
	if e.nc != nil {
		sub, err := transport.RegisterNATSHandler(e.nc, e)
		if err != nil {
			e.logger.Error("failed to register NATS query handler", "error", err)
		} else {
			e.natsSub = sub
			e.logger.Info("NATS query handler registered", "subject", "natsql.query")
		}
	}

	// 6. Start HTTP query server (bound to localhost per T-02-06)
	router := transport.NewRouter()
	transport.RegisterHTTPHandler(router, e)
	httpServer := &http.Server{
		Addr:    fmt.Sprintf("127.0.0.1:%d", e.queryPort),
		Handler: router,
	}
	e.httpServer = httpServer
	e.wg.Add(1)
	go func(srv *http.Server, logger *slog.Logger) {
		defer e.wg.Done()
		logger.Info("HTTP query server starting", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
		}
	}(httpServer, e.logger)

	e.started = true
	e.logger.Info("engine started", "view_count", len(e.cfg.Views))
	return nil
}

// Close gracefully shuts down the engine.
// It cancels the materializer contexts, waits for all materializer goroutines
// to exit, then resets internal state. After Close, Start can be called again
// to restart.
//
// Returns ErrNotStarted if the engine has not been started.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.started {
		return ErrNotStarted
	}

	// Signal all materializers to stop
	if e.cancel != nil {
		e.cancel()
	}

	// Unsubscribe NATS query handler (prevents new NATS queries)
	if e.natsSub != nil {
		if err := e.natsSub.Unsubscribe(); err != nil {
			e.logger.Warn("error unsubscribing NATS handler", "error", err)
		}
		e.natsSub = nil
	}

	// Shutdown HTTP server (causes HTTP goroutine to exit)
	if e.httpServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := e.httpServer.Shutdown(shutdownCtx); err != nil {
			e.logger.Warn("error shutting down HTTP server", "error", err)
		}
		e.httpServer = nil
	}

	// Wait for all goroutines (materializers + HTTP server)
	e.wg.Wait()

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
	// Ensure KV bucket is available (works before Start())
	kvb := e.kv
	if kvb == nil {
		var err error
		kvb, err = kv.InitBucket(ctx, e.js, 1)
		if err != nil {
			errStr := fmt.Sprintf("initializing KV bucket: %v", err)
			return &query.QueryResult{Error: &errStr}
		}
	}

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
