package materialize

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

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

// EnsureDLQStream creates or updates the dead-letter queue stream.
// Per D-13: DLQ stream is auto-created on startup if it does not exist.
// Uses FileStorage with 7-day retention.
func EnsureDLQStream(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	cfg := jetstream.StreamConfig{
		Name:      DLQStreamName,
		Subjects:  []string{DLQSubject},
		Storage:   jetstream.FileStorage,
		MaxAge:    7 * 24 * time.Hour,
		Retention: jetstream.LimitsPolicy,
	}
	stream, err := js.CreateOrUpdateStream(ctx, cfg)
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
// Error handling per ARCHITECTURE.md §2.6 and D-14:
//   - Malformed events (ErrMalformedEvent): published to DLQ, acked, continue
//   - KV write failures: original published to DLQ, acked, continue
//   - Context canceled: return immediately
//   - Consumer errors: logged, continue
func Run(ctx context.Context, js jetstream.JetStream, viewCfg *natsql.ViewConfig, bucket jetstream.KeyValue, logger *slog.Logger, drainCh <-chan struct{}) error {
	cons, err := SetupConsumer(ctx, js, viewCfg.SourceStream, viewCfg.Name, viewCfg.SourceSubject, viewCfg.Consumer)
	if err != nil {
		return fmt.Errorf("setting up consumer for view %q: %w", viewCfg.Name, err)
	}

	mapper, err := NewMapper(viewCfg)
	if err != nil {
		return fmt.Errorf("creating mapper for view %q: %w", viewCfg.Name, err)
	}

	sep := viewCfg.KeySeparator
	if sep == "" {
		sep = "/"
	}
	writer := NewWriter(bucket, viewCfg.Name, sep)

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

// classifyWriteError categorizes a Writer.Apply error as transient or terminal.
// Transient errors: temporary infrastructure issues that may resolve on retry.
// Terminal errors: bad data or configuration that will never succeed.
func classifyWriteError(err error) errorClass {
	if err == nil {
		return errorClassTerminal // should not happen, be safe
	}

	// Context cancellation is always transient
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return errorClassTransient
	}

	errStr := err.Error()

	// NATS connection/network errors — transient
	if strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "no leader") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "connection closed") {

		return errorClassTransient
	}

	// Everything else is terminal — bad data, bad config, etc.
	return errorClassTerminal
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
		if nakErr := msg.Nak(); nakErr != nil {
			logger.Warn("failed to nak event", "seq", seq, "error", nakErr)
		}
		logger.Error("unexpected mapper error", "seq", seq, "error", mapErr)
		return
	}

	if ctx.Err() != nil {
		_ = msg.Nak() // context canceled; nak so message is redelivered on restart
		return
	}

	if dlqErr := publishToDLQ(ctx, js, msg, viewName, mapErr); dlqErr != nil {
		logger.Error("DLQ publish failed, nacking event", "seq", seq, "error", dlqErr)
		if nakErr := msg.Nak(); nakErr != nil {
			logger.Warn("failed to nak event after DLQ failure", "seq", seq, "error", nakErr)
		}
	} else {
		if ackErr := msg.Ack(); ackErr != nil {
			logger.Warn("failed to ack malformed event", "seq", seq, "error", ackErr)
		}
	}
	logger.Warn("skipped malformed event", "seq", seq, "error", mapErr)
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
		logger.Warn("transient write failure, nacking for redelivery", "seq", seq, "error", writeErr)
		if nakErr := msg.Nak(); nakErr != nil {
			logger.Warn("failed to nak transient event", "seq", seq, "error", nakErr)
		}
	case errorClassTerminal:
		logger.Error("terminal write failure, sending to DLQ", "seq", seq, "error", writeErr)
		if dlqErr := publishToDLQ(ctx, js, msg, viewCfg.Name, writeErr); dlqErr != nil {
			logger.Error("DLQ publish failed, nacking event", "seq", seq, "error", dlqErr)
			if nakErr := msg.Nak(); nakErr != nil {
				logger.Warn("failed to nak event after DLQ failure", "seq", seq, "error", nakErr)
			}
		} else {
			if ackErr := msg.Ack(); ackErr != nil {
				logger.Warn("failed to ack event after DLQ publish", "seq", seq, "error", ackErr)
			}
		}
	}
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
