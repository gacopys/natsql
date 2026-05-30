// Package natsql provides the top-level public API for the natsql materialized view engine.
//
// This is the root package facade (D-46) that library consumers import.
// Three constructors are provided (D-47):
//   - New() — caller-owned JetStream context
//   - NewWithNATS() — caller-provided NATS connection (facade owns nc.Close())
//   - NewEmbedded() — embedded NATS server (facade owns shutdown)
//
// Basic usage for Go library consumers:
//
//	import "github.com/gacopys/natsql"
//
//	eng, err := natsql.New(js, cfg, natsql.WithLogger(logger))
//	if err != nil { ... }
//	defer eng.Close()
//	eng.Start(ctx)
//	result := eng.Query(ctx, "SELECT * FROM users WHERE id = 'abc'")
package natsql

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql/internal/embed"
	"github.com/gacopys/natsql/internal/engine"
	"github.com/gacopys/natsql/internal/query"
)

// Engine wraps the internal engine with lifecycle ownership (D-48).
// Create via New, NewWithNATS, or NewEmbedded.
//
// Methods from the underlying engine.Engine (Query, Start, Close, Stats)
// are accessible directly on this type through embedding.
type Engine struct {
	*engine.Engine
	ownedNC   *nats.Conn  // closed on Close() if non-nil (D-48)
	embedNode *embed.Node // shutdown on Close() if non-nil
}

// New creates an Engine from a caller-owned JetStream context (D-47).
// The caller is responsible for closing the NATS connection and JetStream context.
// Use this when embedding natsql in an existing NATS application.
//
// The config must be non-nil; Config.SetDefaults() and Config.Validate() are
// called automatically.
func New(js jetstream.JetStream, cfgVal *Config, opts ...Option) (*Engine, error) {
	if cfgVal == nil {
		return nil, fmt.Errorf("config is nil")
	}
	cfgVal.SetDefaults()
	if err := cfgVal.Validate(); err != nil {
		return nil, err
	}
	eng, err := engine.New(nil, js, cfgVal, convertOpts(opts...)...)
	if err != nil {
		return nil, err
	}
	return &Engine{Engine: eng}, nil
}

// NewWithNATS creates an Engine from a NATS connection (D-47, D-48).
// The engine creates a JetStream context from the connection and owns
// nc.Close() — the caller should not call nc.Close() after passing it here.
//
// The config must be non-nil; Config.SetDefaults() and Config.Validate() are
// called automatically.
func NewWithNATS(nc *nats.Conn, cfgVal *Config, opts ...Option) (*Engine, error) {
	if nc == nil {
		return nil, fmt.Errorf("nats connection is nil")
	}
	if cfgVal == nil {
		return nil, fmt.Errorf("config is nil")
	}
	cfgVal.SetDefaults()
	if err := cfgVal.Validate(); err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("creating JetStream context: %w", err)
	}
	eng, err := engine.New(nc, js, cfgVal, convertOpts(opts...)...)
	if err != nil {
		return nil, err
	}
	return &Engine{Engine: eng, ownedNC: nc}, nil
}

// NewEmbedded creates an Engine with an embedded NATS server (D-47, D-48).
// This is the zero-infrastructure deployment mode — no external NATS required.
// The engine starts an embedded NATS JetStream server, connects to it, and
// owns the server lifecycle (shutdown on Close()).
//
// The config path can specify cfg.NATS.StoreDir for persistent storage.
// Config.SetDefaults() and Config.Validate() are called automatically.
func NewEmbedded(cfgVal *Config, opts ...Option) (*Engine, error) {
	if cfgVal == nil {
		return nil, fmt.Errorf("config is nil")
	}
	eng, err := engine.NewEmbedded(cfgVal, convertOpts(opts...)...)
	if err != nil {
		return nil, err
	}
	return &Engine{Engine: eng, ownedNC: eng.NC(), embedNode: eng.EmbedNode()}, nil
}

// Close gracefully shuts down the engine and owned resources (D-48).
// Closes the internal engine first (stops materializers, HTTP, etc.),
// then shuts down embedded NATS if started by this facade, and finally
// closes the owned NATS connection.
//
// Safe to call multiple times — subsequent calls are no-ops.
func (e *Engine) Close() error {
	// Close internal engine first (shuts down materializers, HTTP, etc.)
	err := e.Engine.Close()

	// Shutdown embedded NATS server if owned
	if e.embedNode != nil {
		e.embedNode.Shutdown()
		e.embedNode = nil
	}

	// Close owned NATS connection
	if e.ownedNC != nil {
		e.ownedNC.Close()
		e.ownedNC = nil
	}

	return err
}

// Query executes a SQL SELECT query against the materialized state.
// Delegates to the underlying engine.Engine.Query.
func (e *Engine) Query(ctx context.Context, sql string) *query.QueryResult {
	return e.Engine.Query(ctx, sql)
}

// ---------------------------------------------------------------------------
// QueryResult is the response envelope returned by Engine.Query.
type QueryResult = query.QueryResult

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// Option configures the Engine.
type Option func(*engine.Engine)

func convertOpts(opts ...Option) []engine.Option {
	result := make([]engine.Option, len(opts))
	for i, opt := range opts {
		result[i] = engine.Option(opt)
	}
	return result
}

// WithLogger sets the logger for the engine and materializers.
func WithLogger(logger *slog.Logger) Option {
	return func(e *engine.Engine) {
		if logger != nil {
			engine.WithLogger(logger)(e)
		}
	}
}

// WithHTTPServer parses an address string ("host:port") and sets the HTTP
// server port. If addr is empty, no action is taken.
func WithHTTPServer(addr string) Option {
	return func(e *engine.Engine) {
		engine.WithHTTPServer(addr)(e)
	}
}

// WithQueryPort directly sets the HTTP query server port.
func WithQueryPort(port int) Option {
	return func(e *engine.Engine) {
		engine.WithQueryPort(port)(e)
	}
}
