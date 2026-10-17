// Package transport provides NATS request-reply and HTTP handlers
// for executing SQL queries against the engine.
package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/gacopys/natsql/v2/internal/query"
)

// QueryHandler is the interface transports use to execute queries.
type QueryHandler interface {
	Query(ctx context.Context, sql string) *query.QueryResult
}

// QueueGroup is the queue group of the NATS query subscription. Engines
// listening on one subject share the requests instead of each answering
// every request (TR-01).
const QueueGroup = "natsql"

// maxConcurrentNATSQueries bounds how many NATS queries execute at once, so
// one slow scan can't block every other request behind it and a burst can't
// run unbounded scans in parallel (TR-01).
const maxConcurrentNATSQueries = 16

// maxPendingNATSQueries bounds queries executing or waiting for a slot.
// Beyond it, requests are answered at once with an overload error.
const maxPendingNATSQueries = 256

// natsQueryTimeout bounds a NATS query, measured from its arrival, so time
// spent waiting for a slot counts against it.
const natsQueryTimeout = 30 * time.Second

// natsCloseGrace is how long Close lets in-flight queries finish before
// canceling them.
const natsCloseGrace = 5 * time.Second

// natsFlushTimeout bounds the round trip that confirms the subscription is
// registered (CR-19). Tests shorten it.
var natsFlushTimeout = 5 * time.Second

// NATSHandler answers SQL queries sent as NATS requests. The request body is
// the raw SQL string (D-35); the reply is the JSON QueryResult envelope.
type NATSHandler struct {
	nc      *nats.Conn
	sub     *nats.Subscription
	handler QueryHandler
	logger  *slog.Logger

	slots   chan struct{} // one token per executing query
	pending atomic.Int64  // queries executing or waiting for a slot

	ctx    context.Context // parent of every request context; canceled by Close
	cancel context.CancelFunc

	mu     sync.Mutex // guards closed against wg.Add
	closed bool
	wg     sync.WaitGroup
}

// RegisterNATSHandler subscribes to subject in queue group QueueGroup (D-34,
// TR-01) and answers each request on its own goroutine, with at most
// maxConcurrentNATSQueries running at once. Close stops it.
func RegisterNATSHandler(nc *nats.Conn, subject string, handler QueryHandler, logger *slog.Logger) (*NATSHandler, error) {
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &NATSHandler{
		nc:      nc,
		handler: handler,
		logger:  logger,
		slots:   make(chan struct{}, maxConcurrentNATSQueries),
		ctx:     ctx,
		cancel:  cancel,
	}

	sub, err := nc.QueueSubscribe(subject, QueueGroup, h.dispatch)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("subscribing to %s: %w", subject, err)
	}
	if err := nc.FlushTimeout(natsFlushTimeout); err != nil { // CR-19: verify subscription is registered before returning
		_ = sub.Unsubscribe() // the flush error is the one to report
		cancel()
		return nil, fmt.Errorf("flushing subscription: %w", err)
	}
	h.sub = sub
	return h, nil
}

// dispatch runs on the subscription's delivery goroutine, so it only hands
// the request off and never blocks on a query.
func (h *NATSHandler) dispatch(msg *nats.Msg) {
	if msg.Reply == "" {
		return // nobody is waiting for an answer
	}
	received := time.Now()

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	if h.pending.Add(1) > maxPendingNATSQueries {
		h.pending.Add(-1)
		h.mu.Unlock()
		h.respond(msg, errorEnvelope("server busy: too many queries in progress, retry later"))
		return
	}
	h.wg.Add(1)
	h.mu.Unlock()

	go func() {
		defer h.wg.Done()
		defer h.pending.Add(-1)
		h.serve(msg, received)
	}()
}

func (h *NATSHandler) serve(msg *nats.Msg, received time.Time) {
	ctx, cancel := context.WithDeadline(h.ctx, received.Add(natsQueryTimeout))
	defer cancel()

	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	case <-ctx.Done():
		h.respond(msg, errorEnvelope("query timed out waiting to run: "+ctx.Err().Error()))
		return
	}

	result := h.handler.Query(ctx, string(msg.Data))
	data, err := json.Marshal(result)
	if err != nil {
		h.respond(msg, errorEnvelope("internal error: "+err.Error()))
		return
	}
	if limit := h.nc.MaxPayload(); int64(len(data)) > limit {
		// TR-02: a reply over the max payload would never arrive.
		h.respond(msg, errorEnvelope(fmt.Sprintf(
			"result too large for a NATS reply (%d bytes, max payload %d): add a LIMIT or narrower WHERE, or use the HTTP API",
			len(data), limit)))
		return
	}
	h.respond(msg, data)
}

func (h *NATSHandler) respond(msg *nats.Msg, data []byte) {
	if err := msg.Respond(data); err != nil {
		h.logger.Warn("failed to respond to NATS query", "subject", msg.Subject, "error", err)
	}
}

// errorEnvelope returns a QueryResult envelope carrying msg as its error,
// encoded with encoding/json so any message is valid JSON (TR-08).
func errorEnvelope(msg string) []byte {
	data, _ := json.Marshal(query.QueryResult{Error: &msg}) // a string pointer always marshals
	return data
}

// Close unsubscribes, so no new requests arrive, then waits for in-flight
// queries: up to natsCloseGrace on their own, then canceled. It is safe to
// call more than once.
func (h *NATSHandler) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	h.mu.Unlock()

	err := h.sub.Unsubscribe()
	if err != nil {
		err = fmt.Errorf("unsubscribing NATS query handler: %w", err)
	}

	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(natsCloseGrace):
		h.cancel()
		<-done
	}
	h.cancel()
	return err
}
