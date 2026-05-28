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
	"sync"

	"github.com/nats-io/nats.go/jetstream"

	natsqlpkg "natsql"
	"natsql/kv"
	"natsql/materialize"
)

// Sentinel errors for Engine lifecycle operations.
var (
	// ErrAlreadyStarted is returned by Start when the engine is already running.
	ErrAlreadyStarted = errors.New("engine already started")

	// ErrNotStarted is returned by Close when the engine hasn't been started.
	ErrNotStarted = errors.New("engine not started")
)

// Engine manages the lifecycle of materialized views.
// Create via New, then call Start to begin processing, and Close to shut down.
type Engine struct {
	js      jetstream.JetStream
	cfg     *natsqlpkg.Config
	kv      jetstream.KeyValue
	logger  *slog.Logger
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	started bool
	mu      sync.Mutex
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

// New creates a new Engine from a JetStream context and configuration.
// The engine is pre-configured but not started. Call Start to begin processing.
//
// The config is validated synchronously. If validation fails, an error is
// returned. The NATS connection and JetStream context must be live and
// provided by the caller.
func New(js jetstream.JetStream, cfg *natsqlpkg.Config, opts ...Option) (*Engine, error) {
	if cfg == nil {
		return nil, errors.New("config is nil")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	e := &Engine{
		js:     js,
		cfg:    cfg,
		logger: slog.Default(),
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

	// Wait for all goroutines to complete
	e.wg.Wait()

	e.started = false
	e.kv = nil
	e.logger.Info("engine closed")
	return nil
}
