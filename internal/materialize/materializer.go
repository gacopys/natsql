package materialize

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	natsql "github.com/gacopys/natsql/internal/cfg"
	kvpkg "github.com/gacopys/natsql/internal/kv"
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

// dlqEnvelope is the JSON structure for DLQ messages.
// Per CONTEXT.md: includes original message bytes, view name, error reason, timestamp.
type dlqEnvelope struct {
	OriginalMessageB64 string `json:"original_message_b64"`
	ViewName           string `json:"view_name"`
	Error              string `json:"error"`
	Timestamp          string `json:"timestamp"`
}

// publishToDLQ sends a failed event to the dead-letter queue stream.
// Returns nil on success, or an error if the publish fails.
func publishToDLQ(ctx context.Context, js jetstream.JetStream, msg jetstream.Msg, viewName string, err error) error {
	envelope := dlqEnvelope{
		OriginalMessageB64: base64.StdEncoding.EncodeToString(msg.Data()),
		ViewName:           viewName,
		Error:              err.Error(),
		Timestamp:          time.Now().UTC().Format(time.RFC3339),
	}

	data, marshalErr := json.Marshal(envelope)
	if marshalErr != nil {
		return fmt.Errorf("marshaling DLQ envelope: %w", marshalErr)
	}

	if _, pubErr := js.Publish(ctx, DLQSubject, data); pubErr != nil {
		return fmt.Errorf("publishing to DLQ: %w", pubErr)
	}
	return nil
}

// Run starts the materializer for a single view. It blocks until the context is canceled
// or a drain signal is received via drainCh.
//
// The processing loop (D-01, D-02):
//  1. Sets up a durable consumer from the source stream
//  2. Creates a mapper for the view config
//  3. Creates a KV writer for the view
//  4. Stores the view schema in KV
//  5. Processes events sequentially: consume → map → write → ack
//  6. Logs a periodic heartbeat
//
// Messages are processed sequentially in a single goroutine (the caller's goroutine),
// preserving JetStream per-subject ordering. No worker pool, no bridge goroutine,
// no buffered channel — the consumer's Messages() drives processing directly.
//
// When drainCh is signaled (closed or receives a value), the consumer is drained
// via cons.Drain() before exiting (D-58). This prevents unnecessary redeliveries
// on restart by allowing in-flight messages to be acknowledged.
//
// Error handling per ARCHITECTURE.md §5.2 and D-14:
//   - Malformed events (ErrMalformedEvent): published to DLQ, acked, continue
//   - Terminal KV write failures: original published to DLQ, acked, continue
//   - Transient KV write failures: NAK'd with backoff; DLQ'd on the final delivery
//   - Context canceled: return immediately
//   - Consumer errors: logged, continue
func Run(ctx context.Context, js jetstream.JetStream, viewCfg *natsql.ViewConfig, bucket jetstream.KeyValue, logger *slog.Logger, drainCh <-chan struct{}) error {
	cons, err := SetupConsumer(ctx, js, viewCfg.SourceStream, viewCfg.Name, viewCfg.SourceSubject, viewCfg.Consumer)
	if err != nil {
		return fmt.Errorf("setting up consumer for view %q: %w", viewCfg.Name, err)
	}

	source, err := lookupSource(ctx, js, viewCfg.SourceStream)
	if err != nil {
		return fmt.Errorf("identifying source stream for view %q: %w", viewCfg.Name, err)
	}

	mapper, err := NewMapper(viewCfg)
	if err != nil {
		return fmt.Errorf("creating mapper for view %q: %w", viewCfg.Name, err)
	}

	sep := viewCfg.KeySeparator
	if sep == "" {
		sep = "/"
	}
	writer := NewWriter(bucket, viewCfg.Name, sep, source)

	schema := viewCfg.BuildSchema()
	if err := kvpkg.StoreSchema(ctx, bucket, viewCfg.Name, schema); err != nil {
		logger.Warn("failed to store schema in KV", "view", viewCfg.Name, "error", err)
	}

	msgCtx, err := cons.Messages()
	if err != nil {
		return fmt.Errorf("getting messages context for view %q: %w", viewCfg.Name, err)
	}

	var eventCount atomic.Int64

	defer func() {
		if r := recover(); r != nil {
			logger.Error("materializer recovered from panic", "view", viewCfg.Name, "panic", r)
		}
	}()

	drainDone := startDrainHandler(ctx, msgCtx, drainCh)
	go runHeartbeat(ctx, logger, viewCfg.Name, &eventCount)

	processMsgLoop(ctx, js, mapper, writer, msgCtx, viewCfg, logger, &eventCount)

	if drainDone != nil {
		<-drainDone
	}
	logger.Info("materializer shutting down", "view", viewCfg.Name, "events_processed", eventCount.Load())
	return ctx.Err()
}

func startDrainHandler(ctx context.Context, msgCtx jetstream.MessagesContext, drainCh <-chan struct{}) chan struct{} {
	if drainCh == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-drainCh:
			msgCtx.Drain()
		case <-ctx.Done():
		}
	}()
	return done
}

func runHeartbeat(ctx context.Context, logger *slog.Logger, viewName string, eventCount *atomic.Int64) {
	heartbeat := time.NewTicker(60 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-heartbeat.C:
			logger.Info("materializer heartbeat", "view", viewName, "events_processed", eventCount.Load())
		case <-ctx.Done():
			return
		}
	}
}

func processMsgLoop(ctx context.Context, js jetstream.JetStream, mapper *Mapper, writer *Writer, msgCtx jetstream.MessagesContext, viewCfg *natsql.ViewConfig, logger *slog.Logger, eventCount *atomic.Int64) {
	for {
		msg, nextErr := msgCtx.Next(jetstream.NextContext(ctx))
		if nextErr != nil {
			if ctx.Err() != nil {
				break
			}
			if errors.Is(nextErr, jetstream.ErrMsgIteratorClosed) {
				break
			}
			logger.Error("consumer Next error", "view", viewCfg.Name, "error", nextErr)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		eventCount.Add(1)
		if testHookProcessGoroutine != nil {
			testHookProcessGoroutine()
		}

		eventCtx, eventCancel := context.WithTimeout(ctx, 30*time.Second)
		processEvent(eventCtx, js, mapper, writer, msg, viewCfg, logger)
		eventCancel()
	}
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
// error as transient only delays it, because the final delivery attempt still
// goes to the DLQ. Errors are matched by identity and JetStream error code,
// never by message text, which includes the primary key.
func classifyWriteError(err error) errorClass {
	if errors.Is(err, jetstream.ErrInvalidKey) ||
		errors.Is(err, nats.ErrMaxPayload) ||
		errors.Is(err, errEncodeRow) ||
		errors.Is(err, errNilMutation) {

		return errorClassTerminal
	}
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode == jsErrCodeMessageTooLarge {
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

// isFinalDelivery reports whether JetStream won't redeliver msg if it is
// NAK'd, because it has reached the consumer's MaxDeliver.
func isFinalDelivery(msg jetstream.Msg, maxDeliver int) bool {
	meta, err := msg.Metadata()
	if err != nil || maxDeliver <= 0 {
		return false
	}
	return meta.NumDelivered >= uint64(maxDeliver)
}

// nakWithBackoff NAKs msg so it is redelivered after redeliveryDelay.
func nakWithBackoff(msg jetstream.Msg, logger *slog.Logger) {
	var numDelivered uint64
	if meta, err := msg.Metadata(); err == nil {
		numDelivered = meta.NumDelivered
	}
	if err := msg.NakWithDelay(redeliveryDelay(numDelivered)); err != nil {
		logger.Warn("failed to nak event", "seq", getMsgSeq(msg), "error", err)
	}
}

// deadLetter publishes msg to the DLQ and acks it. If the DLQ publish fails,
// msg is NAK'd with backoff instead, so the event isn't lost.
func deadLetter(ctx context.Context, js jetstream.JetStream, msg jetstream.Msg, viewName string, cause error, logger *slog.Logger) {
	seq := getMsgSeq(msg)
	if err := publishToDLQ(ctx, js, msg, viewName, cause); err != nil {
		logger.Error("DLQ publish failed, nacking event", "seq", seq, "error", err)
		nakWithBackoff(msg, logger)
		return
	}
	if err := msg.Ack(); err != nil {
		logger.Warn("failed to ack event after DLQ publish", "seq", seq, "error", err)
	}
}

func processEvent(ctx context.Context, js jetstream.JetStream, mapper *Mapper, writer *Writer, msg jetstream.Msg, viewCfg *natsql.ViewConfig, logger *slog.Logger) {
	mut, mapErr := mapper.MapRow(msg)
	if mapErr != nil {
		handleMapError(ctx, js, msg, viewCfg.Name, mapErr, logger)
		return
	}

	if mut != nil {
		if writeErr := writer.Apply(ctx, mut); writeErr != nil {
			handleWriteError(ctx, js, msg, viewCfg, writeErr, logger)
			return
		}
	}

	if ackErr := msg.Ack(); ackErr != nil {
		logger.Warn("failed to ack processed event", "seq", getMsgSeq(msg), "error", ackErr)
	}
}

func handleMapError(ctx context.Context, js jetstream.JetStream, msg jetstream.Msg, viewName string, mapErr error, logger *slog.Logger) {
	seq := getMsgSeq(msg)
	if !errors.Is(mapErr, ErrMalformedEvent) {
		logger.Error("unexpected mapper error", "seq", seq, "error", mapErr)
		nakWithBackoff(msg, logger)
		return
	}

	if ctx.Err() != nil {
		_ = msg.Nak() // context canceled; nak so message is redelivered on restart
		return
	}

	logger.Warn("skipped malformed event", "seq", seq, "error", mapErr)
	deadLetter(ctx, js, msg, viewName, mapErr, logger)
}

func handleWriteError(ctx context.Context, js jetstream.JetStream, msg jetstream.Msg, viewCfg *natsql.ViewConfig, writeErr error, logger *slog.Logger) {
	seq := getMsgSeq(msg)
	if ctx.Err() != nil {
		if nakErr := msg.Nak(); nakErr != nil {
			logger.Warn("failed to nak event after context cancellation", "seq", seq, "error", nakErr)
		}
		return
	}

	switch classifyWriteError(writeErr) {
	case errorClassTransient:
		if !isFinalDelivery(msg, effectiveMaxDeliver(viewCfg.Consumer)) {
			logger.Warn("transient write failure, retrying with backoff", "seq", seq, "error", writeErr)
			nakWithBackoff(msg, logger)
			return
		}
		// JetStream won't redeliver a NAK'd final attempt, so dead-letter it
		// rather than strand it outside both the view and the DLQ.
		logger.Error("transient write failure on final delivery attempt, sending to DLQ", "seq", seq, "error", writeErr)
	case errorClassTerminal:
		logger.Error("terminal write failure, sending to DLQ", "seq", seq, "error", writeErr)
	}
	deadLetter(ctx, js, msg, viewCfg.Name, writeErr, logger)
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
