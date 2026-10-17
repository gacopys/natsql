package materialize

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	natsql "github.com/gacopys/natsql/v2/internal/cfg"
)

// testHookProcessGoroutine is set by tests to observe which goroutine processes events.
// When non-nil, it's called from the processing path before each processEvent call.
// Zero overhead when nil (the standard case).
var testHookProcessGoroutine func()

// DLQStreamName is the name of the dead-letter queue stream.
const DLQStreamName = "natsql-dlq"

// DLQSubject is the subject for DLQ messages.
const DLQSubject = "natsql.dlq"

// Bounds of the exponential backoff applied when a transient failure NAKs an
// event (WR-02).
const (
	baseRedeliveryDelay = 250 * time.Millisecond
	maxRedeliveryDelay  = 30 * time.Second
)

// Bounds of the backoff between failed consumer Next calls, so a lost
// connection doesn't log an error every few milliseconds (OP-02).
const (
	baseNextErrorDelay = 100 * time.Millisecond
	maxNextErrorDelay  = 5 * time.Second
)

// maxDLQErrorLen caps the error text stored in a DLQ envelope; error messages
// can quote event fields of any size.
const maxDLQErrorLen = 4096

// heartbeatInterval is how often a running materializer logs its progress.
// Tests shorten it.
var heartbeatInterval = 60 * time.Second

// jsErrCodeMessageTooLarge is the JetStream API error code for a message
// larger than the stream's maximum message size; it is not exported by nats.go.
const jsErrCodeMessageTooLarge jetstream.ErrorCode = 10054

// EnsureDLQStream opens the dead-letter queue stream, creating it if it does
// not exist (D-13). A new stream uses FileStorage with 7-day retention. An
// existing stream is never reconfigured, so operator changes to its retention,
// limits or replicas survive restarts (WR-05).
func EnsureDLQStream(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	stream, err := js.Stream(ctx, DLQStreamName)
	if err == nil {
		return stream, nil
	}
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, fmt.Errorf("opening DLQ stream: %w", err)
	}

	stream, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      DLQStreamName,
		Subjects:  []string{DLQSubject},
		Storage:   jetstream.FileStorage,
		MaxAge:    7 * 24 * time.Hour,
		Retention: jetstream.LimitsPolicy,
	})
	if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		// Another engine created it between our lookup and create.
		stream, err = js.Stream(ctx, DLQStreamName)
	}
	if err != nil {
		return nil, fmt.Errorf("creating DLQ stream: %w", err)
	}
	return stream, nil
}

// dlqEnvelope is the JSON structure for DLQ messages: the original event,
// where it came from, and why it was rejected (WR-08, WR-12).
type dlqEnvelope struct {
	OriginalMessageB64 string `json:"original_message_b64"`
	// PayloadOmitted is set when the event is too large to embed once
	// base64-encoded; fetch it from the source stream by sequence instead.
	PayloadOmitted   bool   `json:"payload_omitted,omitempty"`
	OriginalSize     int    `json:"original_size"`
	OriginalStream   string `json:"original_stream,omitempty"`
	OriginalSubject  string `json:"original_subject,omitempty"`
	OriginalSequence uint64 `json:"original_sequence,omitempty"`
	NumDelivered     uint64 `json:"num_delivered,omitempty"`
	ViewName         string `json:"view_name"`
	Error            string `json:"error"`
	Timestamp        string `json:"timestamp"`
}

// publishToDLQ sends a failed event to the dead-letter queue stream.
// Returns nil on success, or an error if the publish fails.
//
// The envelope embeds the event base64-encoded, which grows it by a third, so
// an event that fit the server's max payload may not fit once wrapped. When
// the server or the DLQ stream rejects the envelope as too large, it is sent
// again with the payload left out and a reference to the event's source
// stream and sequence in its place (WR-08).
func publishToDLQ(ctx context.Context, js jetstream.JetStream, msg jetstream.Msg, viewName string, cause error) error {
	envelope := dlqEnvelope{
		OriginalMessageB64: base64.StdEncoding.EncodeToString(msg.Data()),
		OriginalSize:       len(msg.Data()),
		OriginalSubject:    msg.Subject(),
		ViewName:           viewName,
		Error:              truncate(cause.Error(), maxDLQErrorLen),
		Timestamp:          time.Now().UTC().Format(time.RFC3339Nano),
	}
	if meta, err := msg.Metadata(); err == nil {
		envelope.OriginalStream = meta.Stream
		envelope.OriginalSequence = meta.Sequence.Stream
		envelope.NumDelivered = meta.NumDelivered
	}

	err := publishEnvelope(ctx, js, &envelope)
	if isTooLarge(err) {
		envelope.OriginalMessageB64 = ""
		envelope.PayloadOmitted = true
		err = publishEnvelope(ctx, js, &envelope)
	}
	return err
}

func publishEnvelope(ctx context.Context, js jetstream.JetStream, envelope *dlqEnvelope) error {
	data, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshaling DLQ envelope: %w", err)
	}
	if _, err := js.Publish(ctx, DLQSubject, data); err != nil {
		return fmt.Errorf("publishing to DLQ: %w", err)
	}
	return nil
}

// isTooLarge reports whether err rejects a message for its size, either the
// server's max payload or a stream's max message size.
func isTooLarge(err error) bool {
	if errors.Is(err, nats.ErrMaxPayload) {
		return true
	}
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode == jsErrCodeMessageTooLarge
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…(truncated)"
}

// Stats counts what one view's materializer has done, across restarts
// (OP-03). It is safe for concurrent use; the zero value is ready to use.
type Stats struct {
	processed    atomic.Int64
	deadLettered atomic.Int64
	retried      atomic.Int64

	mu          sync.Mutex
	lastError   string
	lastErrorAt time.Time
}

// StatsSnapshot is a point-in-time copy of Stats.
type StatsSnapshot struct {
	EventsProcessed int64     // events taken from the consumer
	DeadLettered    int64     // events sent to the DLQ
	Retried         int64     // NAKs that asked for a redelivery
	LastError       string    // most recent processing error, if any
	LastErrorAt     time.Time // when LastError happened
}

// Snapshot returns the current counters.
func (s *Stats) Snapshot() StatsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return StatsSnapshot{
		EventsProcessed: s.processed.Load(),
		DeadLettered:    s.deadLettered.Load(),
		Retried:         s.retried.Load(),
		LastError:       s.lastError,
		LastErrorAt:     s.lastErrorAt,
	}
}

// RecordError remembers err as the most recent error.
func (s *Stats) RecordError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastError = err.Error()
	s.lastErrorAt = time.Now()
}

// Options are the optional settings of Run.
type Options struct {
	// Drain, when closed, drains the consumer: Run finishes and acks the
	// events already delivered, then returns nil (D-58).
	Drain <-chan struct{}

	// OnReady, if set, is called once the durable consumer exists and the
	// message iterator is open, so callers can report readiness instead of
	// guessing with a timer (LC-03).
	OnReady func()

	// Stats, if set, receives event counters and errors (OP-03).
	Stats *Stats
}

// viewRunner holds what processing one view's events needs.
type viewRunner struct {
	js     jetstream.JetStream
	view   *natsql.ViewConfig
	mapper *Mapper
	writer *Writer
	logger *slog.Logger
	stats  *Stats
}

// Run starts the materializer for a single view. It blocks until the context is
// canceled, the drain signal in opts is received, or consuming fails.
//
// The processing loop (D-01, D-02):
//  1. Sets up a durable consumer from the source stream
//  2. Creates a mapper for the view config
//  3. Creates a KV writer for the view
//  4. Reports readiness through opts.OnReady
//  5. Processes events sequentially: consume → map → write → ack
//  6. Logs a periodic heartbeat
//
// The caller owns the view's schema in KV; Run doesn't write it (WR-09).
//
// Messages are processed sequentially in a single goroutine (the caller's goroutine),
// preserving JetStream per-subject ordering. No worker pool, no bridge goroutine,
// no buffered channel — the consumer's Messages() drives processing directly.
//
// When opts.Drain is closed, the consumer is drained via MessagesContext.Drain
// before exiting (D-58), so in-flight messages are acknowledged instead of
// being redelivered on restart. Run returns nil after a drain, ctx.Err() after
// cancellation, and an error when consuming stops for any other reason (the
// connection closed, a panic), so a supervisor can restart it.
//
// Error handling per ARCHITECTURE.md §5.2 and D-14:
//   - Malformed events (ErrMalformedEvent): published to DLQ, acked, continue
//   - Terminal KV write failures: original published to DLQ, acked, continue
//   - Transient KV write failures: NAK'd with backoff; DLQ'd once the retry budget is used
//   - Context canceled: return immediately
//   - Consumer errors: logged, retried with backoff
func Run(ctx context.Context, js jetstream.JetStream, viewCfg *natsql.ViewConfig, bucket jetstream.KeyValue, logger *slog.Logger, opts Options) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("materializer recovered from panic", "view", viewCfg.Name, "panic", r)
			err = fmt.Errorf("materializer for view %q panicked: %v", viewCfg.Name, r)
		}
	}()

	r, msgCtx, err := newViewRunner(ctx, js, viewCfg, bucket, logger, opts.Stats)
	if err != nil {
		return err
	}
	defer msgCtx.Stop() // LC-05: never leave the pull subscription behind

	helperCtx, stopHelpers := context.WithCancel(ctx)
	var helpers sync.WaitGroup
	defer func() {
		stopHelpers()
		helpers.Wait()
	}()

	var draining atomic.Bool
	if opts.Drain != nil {
		helpers.Go(func() {
			select {
			case <-opts.Drain:
				draining.Store(true)
				msgCtx.Drain()
			case <-helperCtx.Done():
			}
		})
	}
	helpers.Go(func() { r.heartbeat(helperCtx) })

	if opts.OnReady != nil {
		opts.OnReady()
	}

	err = r.processMessages(ctx, msgCtx, &draining)
	logger.Info("materializer shutting down", "view", viewCfg.Name, "events_processed", r.stats.processed.Load())
	return err
}

func newViewRunner(ctx context.Context, js jetstream.JetStream, viewCfg *natsql.ViewConfig, bucket jetstream.KeyValue, logger *slog.Logger, stats *Stats) (*viewRunner, jetstream.MessagesContext, error) {
	cons, err := SetupConsumer(ctx, js, viewCfg.SourceStream, viewCfg.Name, viewCfg.SourceSubject, viewCfg.Consumer)
	if err != nil {
		return nil, nil, fmt.Errorf("setting up consumer for view %q: %w", viewCfg.Name, err)
	}

	source, err := lookupSource(ctx, js, viewCfg.SourceStream)
	if err != nil {
		return nil, nil, fmt.Errorf("identifying source stream for view %q: %w", viewCfg.Name, err)
	}

	mapper, err := NewMapper(viewCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("creating mapper for view %q: %w", viewCfg.Name, err)
	}

	msgCtx, err := cons.Messages()
	if err != nil {
		return nil, nil, fmt.Errorf("getting messages context for view %q: %w", viewCfg.Name, err)
	}

	if stats == nil {
		stats = &Stats{}
	}
	return &viewRunner{
		js:     js,
		view:   viewCfg,
		mapper: mapper,
		writer: NewWriter(bucket, viewCfg.Name, mapper.schema.KeySeparator, source),
		logger: logger,
		stats:  stats,
	}, msgCtx, nil
}

func (r *viewRunner) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.logger.Info("materializer heartbeat", "view", r.view.Name, "events_processed", r.stats.processed.Load())
		case <-ctx.Done():
			return
		}
	}
}

func (r *viewRunner) processMessages(ctx context.Context, msgCtx jetstream.MessagesContext, draining *atomic.Bool) error {
	var nextErrors int
	for {
		msg, err := msgCtx.Next(jetstream.NextContext(ctx))
		if err != nil {
			switch {
			case ctx.Err() != nil:
				return ctx.Err()
			case errors.Is(err, jetstream.ErrMsgIteratorClosed):
				if draining.Load() {
					return nil
				}
				return fmt.Errorf("consuming view %q: %w", r.view.Name, err)
			}
			nextErrors++
			delay := nextErrorDelay(nextErrors)
			r.stats.RecordError(err)
			r.logger.Error("consumer Next error", "view", r.view.Name, "error", err, "retry_in", delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		nextErrors = 0
		r.stats.processed.Add(1)
		if testHookProcessGoroutine != nil {
			testHookProcessGoroutine()
		}

		eventCtx, eventCancel := context.WithTimeout(ctx, 30*time.Second)
		r.processEvent(eventCtx, msg)
		eventCancel()
	}
}

// nextErrorDelay returns the pause after the n-th consecutive failed Next
// call: 100ms doubling up to 5s.
func nextErrorDelay(n int) time.Duration {
	return min(baseNextErrorDelay<<min(n-1, 6), maxNextErrorDelay)
}

// errorClass categorizes KV write errors for processEvent routing.
type errorClass int

const (
	errorClassTransient errorClass = iota
	errorClassTerminal
)

// classifyWriteError categorizes a Writer.Apply error as transient or terminal
// (WR-02).
//
// Only errors that can never succeed on retry are terminal: an invalid KV key,
// a row that can't be encoded, or a row larger than the server accepts. Every
// other error, including ones not recognized here, is transient. That is the
// safe default: treating a recoverable error as terminal acks a valid event
// and silently drops its update from the view, while treating a permanent
// error as transient only delays it, because it still goes to the DLQ once
// its retry budget is used. Errors are matched by identity and JetStream
// error code, never by message text, which includes the primary key.
func classifyWriteError(err error) errorClass {
	if errors.Is(err, jetstream.ErrInvalidKey) ||
		errors.Is(err, errEncodeRow) ||
		errors.Is(err, errNilMutation) ||
		isTooLarge(err) {

		return errorClassTerminal
	}
	return errorClassTransient
}

// redeliveryDelay returns how long to wait before redelivering an event that
// has been delivered numDelivered times. The delay starts at 250ms and doubles
// per delivery up to 30s, so a short outage can't use up MaxDeliver within
// milliseconds.
func redeliveryDelay(numDelivered uint64) time.Duration {
	if numDelivered == 0 {
		numDelivered = 1
	}
	return min(baseRedeliveryDelay<<min(numDelivered-1, 7), maxRedeliveryDelay)
}

// retryBudgetUsed reports whether msg has used the view's retry budget, so a
// transient failure should dead-letter it rather than ask for another
// delivery. A budget of -1 never runs out.
func retryBudgetUsed(msg jetstream.Msg, budget int) bool {
	meta, err := msg.Metadata()
	if err != nil || budget <= 0 {
		return false
	}
	return meta.NumDelivered >= uint64(budget)
}

// nakWithBackoff NAKs msg so it is redelivered after redeliveryDelay.
func (r *viewRunner) nakWithBackoff(msg jetstream.Msg) {
	var numDelivered uint64
	if meta, err := msg.Metadata(); err == nil {
		numDelivered = meta.NumDelivered
	}
	r.stats.retried.Add(1)
	if err := msg.NakWithDelay(redeliveryDelay(numDelivered)); err != nil {
		r.logger.Warn("failed to nak event", "view", r.view.Name, "seq", getMsgSeq(msg), "error", err)
	}
}

// deadLetter publishes msg to the DLQ and acks it. If the DLQ publish fails,
// msg is NAK'd with backoff instead, so the event isn't lost: the consumer
// never stops redelivering (WR-07), and the next attempt tries the DLQ again.
func (r *viewRunner) deadLetter(ctx context.Context, msg jetstream.Msg, cause error) {
	seq := getMsgSeq(msg)
	if err := publishToDLQ(ctx, r.js, msg, r.view.Name, cause); err != nil {
		r.stats.RecordError(err)
		r.logger.Error("DLQ publish failed, nacking event", "view", r.view.Name, "seq", seq, "error", err)
		r.nakWithBackoff(msg)
		return
	}
	r.stats.deadLettered.Add(1)
	if err := msg.Ack(); err != nil {
		r.logger.Warn("failed to ack event after DLQ publish", "view", r.view.Name, "seq", seq, "error", err)
	}
}

func (r *viewRunner) processEvent(ctx context.Context, msg jetstream.Msg) {
	mut, mapErr := r.mapper.MapRow(msg)
	if mapErr != nil {
		r.handleMapError(ctx, msg, mapErr)
		return
	}

	if mut != nil {
		if writeErr := r.writer.Apply(ctx, mut); writeErr != nil {
			r.handleWriteError(ctx, msg, writeErr)
			return
		}
	}

	if ackErr := msg.Ack(); ackErr != nil {
		r.logger.Warn("failed to ack processed event", "view", r.view.Name, "seq", getMsgSeq(msg), "error", ackErr)
	}
}

func (r *viewRunner) handleMapError(ctx context.Context, msg jetstream.Msg, mapErr error) {
	seq := getMsgSeq(msg)
	r.stats.RecordError(mapErr)
	if !errors.Is(mapErr, ErrMalformedEvent) {
		r.logger.Error("unexpected mapper error", "view", r.view.Name, "seq", seq, "error", mapErr)
		r.nakWithBackoff(msg)
		return
	}

	if ctx.Err() != nil {
		_ = msg.Nak() // context canceled; nak so message is redelivered on restart
		return
	}

	r.logger.Warn("skipped malformed event", "view", r.view.Name, "seq", seq, "error", mapErr)
	r.deadLetter(ctx, msg, mapErr)
}

func (r *viewRunner) handleWriteError(ctx context.Context, msg jetstream.Msg, writeErr error) {
	seq := getMsgSeq(msg)
	if ctx.Err() != nil {
		if nakErr := msg.Nak(); nakErr != nil {
			r.logger.Warn("failed to nak event after context cancellation", "view", r.view.Name, "seq", seq, "error", nakErr)
		}
		return
	}

	r.stats.RecordError(writeErr)
	switch classifyWriteError(writeErr) {
	case errorClassTransient:
		if !retryBudgetUsed(msg, retryBudget(r.view.Consumer)) {
			r.logger.Warn("transient write failure, retrying with backoff", "view", r.view.Name, "seq", seq, "error", writeErr)
			r.nakWithBackoff(msg)
			return
		}
		r.logger.Error("transient write failure used up the retry budget, sending to DLQ", "view", r.view.Name, "seq", seq, "error", writeErr)
	case errorClassTerminal:
		r.logger.Error("terminal write failure, sending to DLQ", "view", r.view.Name, "seq", seq, "error", writeErr)
	}
	r.deadLetter(ctx, msg, writeErr)
}

// getMsgSeq safely extracts the stream sequence from a message's metadata.
// Returns 0 if metadata is not available.
func getMsgSeq(msg jetstream.Msg) uint64 {
	meta, err := msg.Metadata()
	if err != nil {
		return 0
	}
	return meta.Sequence.Stream
}
