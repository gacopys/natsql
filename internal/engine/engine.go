// Package engine provides the top-level Engine that manages materialized view
// lifecycles. It initializes the KV bucket, creates the DLQ stream, stores
// schemas, and supervises a materializer for each configured view.
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
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	natsqlpkg "github.com/gacopys/natsql/v2/internal/cfg"
	"github.com/gacopys/natsql/v2/internal/embed"
	"github.com/gacopys/natsql/v2/internal/kv"
	"github.com/gacopys/natsql/v2/internal/materialize"
	"github.com/gacopys/natsql/v2/internal/query"
	"github.com/gacopys/natsql/v2/internal/transport"
)

// Sentinel errors for Engine lifecycle operations.
var (
	// ErrAlreadyStarted is returned by Start when the engine is already
	// running, and by DropView while it is running.
	ErrAlreadyStarted = errors.New("engine already started")

	// ErrNotStarted is returned by Close when the engine hasn't been started.
	ErrNotStarted = errors.New("engine not started")

	// ErrViewRebuildRequired is returned by Start when a view's stored rows
	// were produced by a different definition (columns, mappings, keys,
	// separator, source, or an earlier key layout) than the configured one.
	// Resuming would mix incompatible rows, so the view must be dropped with
	// DropView and rebuilt from its source stream (WR-09).
	ErrViewRebuildRequired = errors.New("view must be rebuilt")
)

// httpShutdownGrace is how long Close lets in-flight HTTP queries finish.
const httpShutdownGrace = 5 * time.Second

// drainGrace is how long Close waits for materializers to finish and ack the
// events they already received before canceling them (D-58).
const drainGrace = 10 * time.Second

const (
	// Bounds of the backoff between restarts of a failed materializer, and
	// how long a run must last before the backoff resets (LC-03).
	baseRestartDelay  = time.Second
	maxRestartDelay   = time.Minute
	stableRunDuration = time.Minute
)

// Engine manages the lifecycle of materialized views and provides
// SQL query capabilities over NATS KV state.
// Create via New or NewEmbedded, then call Start to begin processing, and
// Close to shut down.
type Engine struct {
	js        jetstream.JetStream
	nc        *nats.Conn // NATS connection for the query subscription; nil disables it
	cfg       *natsqlpkg.Config
	logger    *slog.Logger
	queryPort int         // HTTP server port (default 8080)
	embedNode *embed.Node // non-nil when using embedded NATS (NewEmbedded)

	// lifecycle serializes Start, Close and DropView. It is held across
	// network I/O and shutdown waits, so neither the query path nor Stats
	// ever takes it (LC-04).
	lifecycle sync.Mutex

	// mu guards run and kv. It is only held to read or swap them, never
	// across I/O.
	mu  sync.Mutex
	run *running           // non-nil while started
	kv  jetstream.KeyValue // opened lazily by the query path or by Start

	queries       atomic.Int64
	queryErrors   atomic.Int64
	activeQueries atomic.Int64
}

// running holds everything a successful Start acquired, so Close, or a
// failing Start, can release all of it (LC-01).
type running struct {
	cancel     context.CancelFunc
	views      []*viewRun
	viewsWG    sync.WaitGroup // view supervisors
	nats       *transport.NATSHandler
	httpServer *http.Server
	httpAddr   net.Addr
	httpWG     sync.WaitGroup // the HTTP Serve goroutine
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
// server port. If addr is empty, no action is taken. The server always binds
// 127.0.0.1 (T-02-06); the host part is ignored.
func WithHTTPServer(addr string) Option {
	return func(e *Engine) {
		if addr == "" {
			return
		}
		_, portStr, err := netSplitHostPort(addr)
		if err != nil {
			return // silently ignore invalid addresses
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return
		}
		e.queryPort = port
	}
}

// WithQueryPort directly sets the HTTP query server port.
// A port of 0 lets the OS choose a free random port, which avoids port
// conflicts when multiple engines run concurrently (e.g. in parallel tests).
// Stats().HTTPAddr reports the port chosen.
func WithQueryPort(port int) Option {
	return func(e *Engine) {
		if port >= 0 {
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
// query subjects in Start(); with a nil connection, queries are only served
// over HTTP and the Go API.
func New(nc *nats.Conn, js jetstream.JetStream, cfg *natsqlpkg.Config, opts ...Option) (*Engine, error) {
	if cfg == nil {
		return nil, errors.New("config is nil")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	e := newEngine(cfg, opts)
	e.nc, e.js = nc, js
	return e, nil
}

// NewEmbedded creates a new Engine that starts an embedded NATS JetStream
// server in the same process. The engine owns the embedded server lifecycle
// and will shut it down when Close is called.
//
// Without cfg.NATS.StoreDir, the server stores JetStream data in a private
// temporary directory that is deleted on shutdown (OP-01); set StoreDir to
// keep views across restarts.
//
// The config's SetDefaults and Validate are called automatically.
func NewEmbedded(cfg *natsqlpkg.Config, opts ...Option) (*Engine, error) {
	if cfg == nil {
		return nil, errors.New("config is nil")
	}
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	e := newEngine(cfg, opts)

	natsPort := cfg.NATS.Port
	if natsPort == 0 {
		natsPort = -1 // random port
	}
	node, err := embed.StartNode(embed.NodeConfig{
		Port:      natsPort,
		StoreDir:  cfg.NATS.StoreDir,
		ReadyWait: 10 * time.Second,
		Logger:    e.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("starting embedded NATS: %w", err)
	}

	nc, err := nats.Connect(node.ClientURL(), nats.Timeout(10*time.Second))
	if err != nil {
		node.Shutdown()
		return nil, fmt.Errorf("connecting to embedded NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		node.Shutdown()
		return nil, fmt.Errorf("creating JetStream context: %w", err)
	}

	e.nc, e.js, e.embedNode = nc, js, node
	return e, nil
}

// newEngine builds an Engine with defaults and applies opts.
func newEngine(cfg *natsqlpkg.Config, opts []Option) *Engine {
	e := &Engine{
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
	return e
}

// Start initializes the KV bucket, creates the DLQ stream, publishes schemas,
// and launches a supervised materializer for each configured view.
//
// Steps:
//  1. Open the KV bucket (natsql-views), creating it if missing, via kv.InitBucket
//  2. Open the DLQ stream (natsql-dlq), creating it if missing, via materialize.EnsureDLQStream
//  3. Publish each view's schema, refusing views whose stored rows came from
//     a different definition (ErrViewRebuildRequired, WR-09)
//  4. Start each view's materializer and wait until every one is consuming
//     (LC-03); a view that fails before then fails Start
//  5. Register the NATS query handler and start the HTTP server
//
// ctx bounds these steps only. The materializers keep running after ctx ends,
// until Close (LC-02).
//
// Calling Start on an already-started engine returns ErrAlreadyStarted.
// On failure, everything Start acquired is released before it returns, so no
// materializer, subscription or listener is left running (LC-01, FIX-MAT-03).
func (e *Engine) Start(ctx context.Context) (err error) {
	e.lifecycle.Lock()
	defer e.lifecycle.Unlock()

	if e.current() != nil {
		return ErrAlreadyStarted
	}

	kvb, err := kv.InitBucket(ctx, e.js, 1)
	if err != nil {
		return fmt.Errorf("initializing KV bucket: %w", err)
	}
	e.setBucket(kvb)

	if _, err := materialize.EnsureDLQStream(ctx, e.js); err != nil {
		return fmt.Errorf("creating DLQ stream: %w", err)
	}

	if err := e.publishSchemas(ctx, kvb); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	r := &running{cancel: cancel}
	defer func() {
		if err != nil {
			e.shutdown(r)
		}
	}()

	if err := e.startViews(ctx, runCtx, kvb, r); err != nil {
		return err
	}

	if e.nc != nil {
		h, err := transport.RegisterNATSHandler(e.nc, e.cfg.NATS.QuerySubject, e, e.logger)
		if err != nil {
			return fmt.Errorf("failed to register NATS query handler: %w", err)
		}
		r.nats = h
		e.logger.Info("NATS query handler registered", "subject", e.cfg.NATS.QuerySubject, "queue", transport.QueueGroup)
	}

	if err := e.startHTTPServer(ctx, r); err != nil {
		return err
	}

	e.mu.Lock()
	e.run = r
	e.mu.Unlock()
	e.logger.Info("engine started", "view_count", len(e.cfg.Views))
	return nil
}

// publishSchemas stores each view's schema, the single place schemas are
// written (WR-09). A view whose stored schema is missing or has a different
// fingerprint is refused if it has materialized state (a durable consumer or
// stored rows), because that state came from another definition or an
// earlier key layout.
func (e *Engine) publishSchemas(ctx context.Context, kvb jetstream.KeyValue) error {
	var errs []error
	for i := range e.cfg.Views {
		if err := e.publishSchema(ctx, kvb, &e.cfg.Views[i]); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (e *Engine) publishSchema(ctx context.Context, kvb jetstream.KeyValue, vc *natsqlpkg.ViewConfig) error {
	want := vc.BuildSchema()
	have, err := kv.LoadSchema(ctx, kvb, vc.Name)
	if err != nil {
		return fmt.Errorf("view %q: %w", vc.Name, err)
	}
	if have != nil && have.Fingerprint == want.Fingerprint {
		return nil
	}

	materialized, err := e.hasMaterializedState(ctx, vc, have)
	if err != nil {
		return err
	}
	switch {
	case materialized && have != nil:
		return fmt.Errorf("%w: view %q: its definition (columns, mappings, key fields, separator or source) changed since it was materialized; %s",
			ErrViewRebuildRequired, vc.Name, rebuildHint(vc.Name))
	case materialized:
		return fmt.Errorf("%w: view %q was materialized by an earlier natsql version, which used another key layout (or its schema was deleted); %s",
			ErrViewRebuildRequired, vc.Name, rebuildHint(vc.Name))
	case have != nil:
		// The old schema was published, but nothing was materialized from it
		// (e.g. its source stream didn't exist), so it can simply be replaced.
		e.logger.Info("replacing the schema of a view with no materialized state", "view", vc.Name)
	}

	if err := kv.StoreSchema(ctx, kvb, vc.Name, want); err != nil {
		return fmt.Errorf("view %q: %w", vc.Name, err)
	}
	return nil
}

// hasMaterializedState reports whether a view has a durable consumer on its
// configured or previously stored source stream, or rows in the bucket.
func (e *Engine) hasMaterializedState(ctx context.Context, vc *natsqlpkg.ViewConfig, stored *kv.ViewSchema) (bool, error) {
	streams := []string{vc.SourceStream}
	if stored != nil && stored.SourceStream != "" && stored.SourceStream != vc.SourceStream {
		streams = append(streams, stored.SourceStream)
	}
	for _, stream := range streams {
		_, err := e.js.Consumer(ctx, stream, materialize.ConsumerName(vc.Name))
		switch {
		case err == nil:
			return true, nil
		case errors.Is(err, jetstream.ErrConsumerNotFound), errors.Is(err, jetstream.ErrStreamNotFound):
			// No consumer there. A missing configured stream fails later, in consumer setup.
		default:
			return false, fmt.Errorf("view %q: looking up its durable consumer: %w", vc.Name, err)
		}
	}
	return kv.HasRows(ctx, e.js, vc.Name)
}

func rebuildHint(view string) string {
	return fmt.Sprintf("stop every engine materializing it, run `natsql drop-view --view %s` (or Engine.DropView), and start again to replay it from its source stream; events the stream no longer retains can't be replayed", view)
}

// startViews starts a supervised materializer per view and waits until each
// is consuming or has failed, or until ctx ends.
func (e *Engine) startViews(ctx, runCtx context.Context, kvb jetstream.KeyValue, r *running) error {
	for i := range e.cfg.Views {
		v := &viewRun{
			cfg:     e.cfg.Views[i],
			drain:   make(chan struct{}),
			startup: make(chan error, 1),
			state:   viewStarting,
		}
		r.views = append(r.views, v)
		r.viewsWG.Go(func() { e.superviseView(runCtx, kvb, v) })
		e.logger.Info("started materializer", "view", v.cfg.Name, "source_stream", v.cfg.SourceStream)
	}

	var errs []error
	for _, v := range r.views {
		select {
		case err := <-v.startup:
			if err != nil {
				e.logger.Error("materializer startup failed", "view", v.cfg.Name, "error", err)
				errs = append(errs, fmt.Errorf("materializer for view %q failed to start: %w", v.cfg.Name, err))
			}
		case <-ctx.Done():
			return fmt.Errorf("waiting for materializers to start: %w", ctx.Err())
		}
	}
	return errors.Join(errs...)
}

// superviseView runs one view's materializer until the engine stops it. It
// reports the first run's readiness or failure on v.startup. After that, a
// failed run is restarted with exponential backoff and the view reports
// "restarting" meanwhile; a closed NATS connection can't recover, so it
// leaves the view "failed" (LC-03, OP-02).
func (e *Engine) superviseView(ctx context.Context, kvb jetstream.KeyValue, v *viewRun) {
	ready := false
	failures := 0
	for {
		started := time.Now()
		err := materialize.Run(ctx, e.js, &v.cfg, kvb, e.logger, materialize.Options{
			Drain: v.drain,
			Stats: &v.stats,
			OnReady: func() {
				v.setState(viewRunning)
				if !ready {
					ready = true
					v.startup <- nil
				}
			},
		})
		if err == nil || ctx.Err() != nil || v.draining() {
			v.setState(viewStopped)
			return
		}

		v.stats.RecordError(err)
		if !ready {
			v.setState(viewFailed)
			v.startup <- err
			return
		}
		if errors.Is(err, jetstream.ErrConnectionClosed) || errors.Is(err, nats.ErrConnectionClosed) {
			v.setState(viewFailed)
			e.logger.Error("materializer stopped: the NATS connection is closed", "view", v.cfg.Name, "error", err)
			return
		}

		if time.Since(started) >= stableRunDuration {
			failures = 0
		}
		failures++
		delay := restartDelay(failures)
		v.setState(viewRestarting)
		v.restarts.Add(1)
		e.logger.Error("materializer failed, restarting", "view", v.cfg.Name, "error", err, "retry_in", delay)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			v.setState(viewStopped)
			return
		case <-v.drain:
			v.setState(viewStopped)
			return
		}
	}
}

// restartDelay returns the pause before the n-th consecutive restart: 1s
// doubling up to a minute.
func restartDelay(n int) time.Duration {
	return min(baseRestartDelay<<min(n-1, 6), maxRestartDelay)
}

func (e *Engine) startHTTPServer(ctx context.Context, r *running) error {
	router := transport.NewRouter()
	transport.RegisterHTTPHandler(router, e, e)

	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", e.queryPort))
	// CR-07: Listen before Serve so bind errors propagate synchronously.
	if err != nil {
		return fmt.Errorf("HTTP listen failed on port %d: %w", e.queryPort, err)
	}

	httpServer := &http.Server{
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}
	r.httpServer = httpServer
	r.httpAddr = listener.Addr()
	r.httpWG.Go(func() {
		e.logger.Info("HTTP query server starting", "addr", listener.Addr())
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			e.logger.Error("HTTP server error", "error", err)
		}
	})
	return nil
}

// Close gracefully shuts down the engine following D-57 ordering:
//  1. Stop HTTP server (5s grace for in-flight requests)
//  2. Unsubscribe the NATS query handler and wait for in-flight queries
//  3. Drain every materializer's consumer and wait for them to finish the
//     events they already received (up to 10s, D-58)
//  4. Cancel the materializer context (stops anything still running)
//  5. Wait for all goroutines
//
// Queries keep working during and after Close for as long as the NATS
// connection is open; only the subscription and listener stop.
//
// Returns ErrNotStarted if the engine is not running.
func (e *Engine) Close() error {
	e.lifecycle.Lock()
	defer e.lifecycle.Unlock()

	e.mu.Lock()
	r := e.run
	e.run = nil
	e.mu.Unlock()
	if r == nil {
		return ErrNotStarted
	}

	e.shutdown(r)
	e.logger.Info("engine closed")
	return nil
}

// shutdown releases everything in r, in D-57 order. It tolerates partially
// filled state, so a failing Start uses it to unwind (LC-01).
func (e *Engine) shutdown(r *running) {
	// D-57 Step 1: Stop HTTP server (in-flight requests get a grace period)
	if r.httpServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), httpShutdownGrace)
		if err := r.httpServer.Shutdown(shutdownCtx); err != nil {
			e.logger.Warn("HTTP server shutdown error", "error", err)
		}
		shutdownCancel()
	}

	// D-57 Step 2: Unsubscribe NATS query handler and finish in-flight queries
	if r.nats != nil {
		if err := r.nats.Close(); err != nil {
			e.logger.Warn("NATS query handler shutdown error", "error", err)
		}
	}

	// D-57 Step 3: Drain each consumer and let the materializers finish the
	// events already delivered before anything is canceled (D-58).
	for _, v := range r.views {
		close(v.drain)
	}
	drained := make(chan struct{})
	go func() {
		r.viewsWG.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(drainGrace):
		e.logger.Warn("materializers did not drain in time, canceling", "grace", drainGrace)
	}

	// D-57 Step 4: Cancel materializer context
	r.cancel()

	// D-57 Step 5: Wait for all goroutines (materializers + HTTP server)
	<-drained
	r.httpWG.Wait()
}

// DropView deletes a view's materialized state so that the next Start
// rebuilds it from the beginning of its source stream (WR-09): its durable
// consumer, its rows and its schema, in the current and the original key
// layout. Use it after Start reports ErrViewRebuildRequired.
//
// The engine must not be running (ErrAlreadyStarted otherwise), and no other
// engine may be materializing the view. The view need not be in the config,
// but then its consumer can only be found through its stored schema.
func (e *Engine) DropView(ctx context.Context, name string) error {
	e.lifecycle.Lock()
	defer e.lifecycle.Unlock()

	if e.current() != nil {
		return ErrAlreadyStarted
	}
	if !natsqlpkg.ValidViewName(name) {
		return fmt.Errorf("invalid view name %q", name)
	}

	kvb, err := kv.OpenBucket(ctx, e.js)
	if err != nil && !errors.Is(err, jetstream.ErrBucketNotFound) {
		return fmt.Errorf("opening KV bucket: %w", err)
	}

	stream, err := e.sourceStreamOf(ctx, kvb, name)
	if err != nil {
		return err
	}
	if stream == "" {
		e.logger.Warn("view's source stream is unknown, so its durable consumer was not deleted", "view", name, "consumer", materialize.ConsumerName(name))
	} else {
		err := e.js.DeleteConsumer(ctx, stream, materialize.ConsumerName(name))
		if err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) && !errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("deleting durable consumer of view %q: %w", name, err)
		}
	}

	if kvb == nil {
		return nil // nothing was ever stored
	}
	legacy, err := kv.PurgeView(ctx, e.js, kvb, name)
	if err != nil {
		return err
	}
	e.logger.Info("dropped view", "view", name, "source_stream", stream, "legacy_keys_removed", legacy)
	return nil
}

// sourceStreamOf returns the source stream of a view from the config, or
// else from its stored schema; "" if neither knows it.
func (e *Engine) sourceStreamOf(ctx context.Context, kvb jetstream.KeyValue, name string) (string, error) {
	for _, v := range e.cfg.Views {
		if v.Name == name {
			return v.SourceStream, nil
		}
	}
	if kvb == nil {
		return "", nil
	}
	schema, err := kv.LoadSchema(ctx, kvb, name)
	if err != nil {
		return "", fmt.Errorf("view %q: %w", name, err)
	}
	if schema == nil {
		return "", nil
	}
	return schema.SourceStream, nil
}

// Query executes a SQL SELECT query against the materialized state.
// It parses the SQL, loads the view schema from KV, validates the query,
// builds an execution plan, and executes it.
//
// This method is threadsafe and works before Engine.Start() has been called
// (it lazily opens the KV bucket if another engine has created it). It never
// waits for Start or Close (LC-04).
//
// Returns a QueryResult with typed JSON values per D-29/D-30.
func (e *Engine) Query(ctx context.Context, sql string) *query.QueryResult {
	e.queries.Add(1)
	e.activeQueries.Add(1)
	defer e.activeQueries.Add(-1)

	result := e.query(ctx, sql)
	if result.Error != nil {
		e.queryErrors.Add(1)
	}
	return result
}

func (e *Engine) query(ctx context.Context, sql string) *query.QueryResult {
	e.logger.Info("executing SQL", "sql", sql)

	// Parse before touching storage, so invalid SQL costs no KV round trip.
	q, err := query.Parse(sql)
	if err != nil {
		return errorResult(err.Error())
	}

	kvb, err := e.queryBucket(ctx)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return errorResult(fmt.Sprintf("view %q not found", q.From)) // nothing materialized yet (D-42)
	}
	if err != nil {
		return errorResult(fmt.Sprintf("opening KV bucket: %v", err))
	}

	// Load schema from KV (per D-27 — always fresh)
	schema, err := kv.LoadSchema(ctx, kvb, q.From)
	if err != nil {
		return errorResult(fmt.Sprintf("error loading schema: %v", err))
	}
	if schema == nil {
		return errorResult(fmt.Sprintf("view %q not found", q.From)) // D-42
	}

	if err := query.Validate(q, schema); err != nil {
		return errorResult(err.Error())
	}

	plan, err := query.BuildPlan(q, schema)
	if err != nil {
		return errorResult(err.Error())
	}

	rows, err := plan.Execute(ctx, kvb)
	if err != nil {
		return errorResult(err.Error())
	}

	// Return results (normalize nil to empty slice per D-33)
	if rows == nil {
		rows = []map[string]any{}
	}
	return &query.QueryResult{Results: rows, Error: nil}
}

func errorResult(msg string) *query.QueryResult {
	return &query.QueryResult{Results: nil, Error: &msg}
}

// queryBucket returns the KV bucket for the read path, opening it on first
// use (FIX-ENG-02). The open happens without holding any lock, so a slow or
// unreachable server delays only the queries that need it (LC-04). It never
// creates or reconfigures the bucket (WR-05): its settings belong to whoever
// provisioned it, and queries should only need read permissions.
func (e *Engine) queryBucket(ctx context.Context) (jetstream.KeyValue, error) {
	e.mu.Lock()
	kvb := e.kv
	e.mu.Unlock()
	if kvb != nil {
		return kvb, nil
	}

	kvb, err := kv.OpenBucket(ctx, e.js)
	if err != nil {
		return nil, err
	}
	e.setBucket(kvb)
	return kvb, nil
}

func (e *Engine) setBucket(kvb jetstream.KeyValue) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.kv == nil {
		e.kv = kvb
	}
}

// current returns the running state, or nil when the engine isn't started.
func (e *Engine) current() *running {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.run
}

// Stats holds operational metrics for the Engine (D-60, OP-03).
type Stats struct {
	Started       bool         `json:"started"`
	Ready         bool         `json:"ready"`          // started, connected, and every view's materializer is consuming
	NATSConnected bool         `json:"nats_connected"` // false while the connection is down or reconnecting
	Goroutines    int          `json:"goroutines"`     // process-wide
	Views         int          `json:"views"`
	HTTPServing   bool         `json:"http_serving"`
	HTTPAddr      string       `json:"http_addr,omitempty"` // bound address, e.g. with WithQueryPort(0)
	Queries       int64        `json:"queries"`
	QueryErrors   int64        `json:"query_errors"`
	ActiveQueries int64        `json:"active_queries"`
	ViewStatus    []ViewStatus `json:"view_status"`
}

// ViewStatus reports one view's materializer.
type ViewStatus struct {
	Name string `json:"name"`
	// State is "starting", "running", "restarting" (after a failure),
	// "failed" (won't be restarted) or "stopped".
	State           string     `json:"state"`
	Restarts        int64      `json:"restarts"`
	EventsProcessed int64      `json:"events_processed"`
	DeadLettered    int64      `json:"dead_lettered"`
	Retried         int64      `json:"retried"`
	LastError       string     `json:"last_error,omitempty"`
	LastErrorAt     *time.Time `json:"last_error_at,omitempty"`
}

// NC returns the NATS connection used by the engine.
// Returns nil if the engine was created with New() (caller-owned connection).
func (e *Engine) NC() *nats.Conn { return e.nc }

// EmbedNode returns the embedded NATS node, or nil if not using embedded NATS.
func (e *Engine) EmbedNode() *embed.Node { return e.embedNode }

// Stats returns current operational metrics about the engine.
// Safe to call at any lifecycle phase (before Start, after Close), and never
// blocks on Start or Close.
func (e *Engine) Stats() Stats {
	r := e.current()
	connected := e.connected()
	s := Stats{
		Started:       r != nil,
		Ready:         r != nil && connected,
		NATSConnected: connected,
		Goroutines:    runtime.NumGoroutine(),
		Views:         len(e.cfg.Views),
		Queries:       e.queries.Load(),
		QueryErrors:   e.queryErrors.Load(),
		ActiveQueries: e.activeQueries.Load(),
		ViewStatus:    []ViewStatus{},
	}
	if r == nil {
		return s
	}

	s.HTTPServing = r.httpServer != nil
	if r.httpAddr != nil {
		s.HTTPAddr = r.httpAddr.String()
	}
	for _, v := range r.views {
		vs := v.status()
		if vs.State != string(viewRunning) {
			s.Ready = false
		}
		s.ViewStatus = append(s.ViewStatus, vs)
	}
	return s
}

// connected reports whether the engine's NATS connection is up.
func (e *Engine) connected() bool {
	nc := e.nc
	if nc == nil && e.js != nil {
		nc = e.js.Conn() // New(js) without a connection: check the JetStream one
	}
	return nc != nil && nc.IsConnected()
}

// Health reports readiness and the Stats document for the HTTP probes.
func (e *Engine) Health() (bool, any) {
	s := e.Stats()
	return s.Ready, s
}

// viewState is a view materializer's lifecycle state.
type viewState string

const (
	viewStarting   viewState = "starting"
	viewRunning    viewState = "running"
	viewRestarting viewState = "restarting"
	viewFailed     viewState = "failed"
	viewStopped    viewState = "stopped"
)

// viewRun is one view's supervised materializer.
type viewRun struct {
	cfg      natsqlpkg.ViewConfig
	drain    chan struct{} // closed by shutdown to drain the consumer (D-58)
	startup  chan error    // receives nil once first ready, or the first run's error
	stats    materialize.Stats
	restarts atomic.Int64

	mu    sync.Mutex
	state viewState
}

func (v *viewRun) setState(s viewState) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.state = s
}

func (v *viewRun) draining() bool {
	select {
	case <-v.drain:
		return true
	default:
		return false
	}
}

func (v *viewRun) status() ViewStatus {
	v.mu.Lock()
	state := v.state
	v.mu.Unlock()

	snap := v.stats.Snapshot()
	vs := ViewStatus{
		Name:            v.cfg.Name,
		State:           string(state),
		Restarts:        v.restarts.Load(),
		EventsProcessed: snap.EventsProcessed,
		DeadLettered:    snap.DeadLettered,
		Retried:         snap.Retried,
		LastError:       snap.LastError,
	}
	if !snap.LastErrorAt.IsZero() {
		vs.LastErrorAt = &snap.LastErrorAt
	}
	return vs
}
