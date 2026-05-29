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

	"github.com/nats-io/nats.go/jetstream"

	natsql "natsql/cfg"
	kvpkg "natsql/kv"
)

const materializerWorkers = 16

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

// Run starts the materializer for a single view. It blocks until the context is cancelled
// or a drain signal is received via drainCh.
//
// The processing loop:
//  1. Sets up a durable consumer from the source stream
//  2. Creates a mapper for the view config
//  3. Creates a KV writer for the view
//  4. Stores the view schema in KV
//  5. Processes events: consume → map → write → ack
//  6. Logs a periodic heartbeat
//
// When drainCh is signaled (closed or receives a value), the consumer is drained
// via cons.Drain() before exiting (D-58). This prevents unnecessary redeliveries
// on restart by allowing in-flight messages to be acknowledged.
//
// Error handling per ARCHITECTURE.md §2.6 and D-14:
//   - Malformed events (ErrMalformedEvent): published to DLQ, acked, continue
//   - KV write failures: original published to DLQ, acked, continue
//   - Context cancelled: return immediately
//   - Consumer errors: logged, continue
func Run(ctx context.Context, js jetstream.JetStream, viewCfg *natsql.ViewConfig, bucket jetstream.KeyValue, dlqStream jetstream.Stream, logger *slog.Logger, drainCh <-chan struct{}) error {
	// 1. Create durable consumer
	cons, err := SetupConsumer(ctx, js, viewCfg.SourceStream, viewCfg.Name, viewCfg.SourceSubject, viewCfg.Consumer)
	if err != nil {
		return fmt.Errorf("setting up consumer for view %q: %w", viewCfg.Name, err)
	}

	// 2. Create mapper
	mapper, err := NewMapper(viewCfg)
	if err != nil {
		return fmt.Errorf("creating mapper for view %q: %w", viewCfg.Name, err)
	}

	// 3. Create writer
	writer := NewWriter(bucket, viewCfg.Name)

	// 4. Store schema in KV
	schema := viewCfg.BuildSchema()
	if err := kvpkg.StoreSchema(ctx, bucket, viewCfg.Name, schema); err != nil {
		logger.Warn("failed to store schema in KV", "view", viewCfg.Name, "error", err)
		// Non-fatal: materializer can still process events
	}

	// 5. Setup messages context
	msgCtx, err := cons.Messages()
	if err != nil {
		return fmt.Errorf("getting messages context for view %q: %w", viewCfg.Name, err)
	}

	// D-58: Use a child context for the fetch loop so we can unblock
	// NextContext independently of the main ctx (for drain support).
	fetchCtx, fetchCancel := context.WithCancel(ctx)

	// Bridge goroutine: MessagesContext.Next() → channel for select-based loop
	// Uses fetchCtx so that drain can cancel it without affecting the main ctx.
	msgCh := make(chan jetstream.Msg, 64)
	go func() {
		defer close(msgCh)
		defer fetchCancel()
		for {
			msg, nextErr := msgCtx.Next(jetstream.NextContext(fetchCtx))
			if nextErr != nil {
				// If the main context is done, it's a normal shutdown
				if ctx.Err() != nil {
					return
				}
				// If fetchCtx is cancelled but main ctx is alive, drain was requested
				if errors.Is(nextErr, context.Canceled) {
					select {
					case <-drainCh:
						// Drain requested: drain the MessagesContext (D-58)
						// This allows in-flight messages to be acked before exit,
						// preventing unnecessary redeliveries on restart.
						msgCtx.Drain()
					default:
						// Main context still alive, unexpected cancellation
					}
					return
				}
				if errors.Is(nextErr, jetstream.ErrMsgIteratorClosed) {
					return // iterator closed normally
				}
				logger.Error("consumer Next error", "view", viewCfg.Name, "error", nextErr)
				time.Sleep(100 * time.Millisecond) // brief backoff before retry
				continue
			}
			select {
			case msgCh <- msg:
			case <-fetchCtx.Done():
				return
			}
		}
	}()

	// Monitor goroutine: listens for drain signal and cancels the fetch context
	// to unblock the bridge goroutine's NextContext call.
	if drainCh != nil {
		go func() {
			select {
			case <-drainCh:
				fetchCancel()
			case <-ctx.Done():
				fetchCancel()
			}
		}()
	}

	// 6. Worker pool: parallel processing pipeline
	var (
		eventCount atomic.Int64
		workerWg  sync.WaitGroup
	)
	workerWg.Add(materializerWorkers)

	for i := 0; i < materializerWorkers; i++ {
		go func() {
			defer workerWg.Done()
			for msg := range msgCh {
				eventCount.Add(1)
				processEvent(ctx, js, mapper, writer, msg, viewCfg, logger)
			}
		}()
	}

	// 7. Heartbeat logs progress every 60s in background
	go func() {
		heartbeat := time.NewTicker(60 * time.Second)
		defer heartbeat.Stop()
		for {
			select {
			case <-heartbeat.C:
				logger.Info("materializer heartbeat", "view", viewCfg.Name, "events_processed", eventCount.Load())
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for all workers to finish (msgCh closes when consumer exits)
	workerWg.Wait()
	logger.Info("materializer shutting down", "view", viewCfg.Name, "events_processed", eventCount.Load())
	return ctx.Err()
}

// processEvent handles one event: map → write/error → ack/Nak.
func processEvent(ctx context.Context, js jetstream.JetStream, mapper *Mapper, writer *Writer, msg jetstream.Msg, viewCfg *natsql.ViewConfig, logger *slog.Logger) {
	mut, mapErr := mapper.MapRow(msg)
	if mapErr != nil {
		if errors.Is(mapErr, ErrMalformedEvent) {
			if ctx.Err() != nil {
				msg.Nak()
				return
			}
			if dlqErr := publishToDLQ(ctx, js, msg, viewCfg.Name, mapErr); dlqErr != nil {
				logger.Error("DLQ publish failed, nacking event", "seq", getMsgSeq(msg), "error", dlqErr)
				if nakErr := msg.Nak(); nakErr != nil {
					logger.Warn("failed to nak event after DLQ failure", "seq", getMsgSeq(msg), "error", nakErr)
				}
			} else {
				if ackErr := msg.Ack(); ackErr != nil {
					logger.Warn("failed to ack malformed event", "seq", getMsgSeq(msg), "error", ackErr)
				}
			}
			logger.Warn("skipped malformed event", "seq", getMsgSeq(msg), "error", mapErr)
			return
		}
		if nakErr := msg.Nak(); nakErr != nil {
			logger.Warn("failed to nak event", "seq", getMsgSeq(msg), "error", nakErr)
		}
		logger.Error("unexpected mapper error", "seq", getMsgSeq(msg), "error", mapErr)
		return
	}

	if mut != nil {
		if writeErr := writer.Apply(ctx, mut); writeErr != nil {
			if ctx.Err() != nil {
				msg.Nak()
				return
			}
			if dlqErr := publishToDLQ(ctx, js, msg, viewCfg.Name, writeErr); dlqErr != nil {
				logger.Error("DLQ publish failed, nacking event", "seq", getMsgSeq(msg), "error", dlqErr)
				if nakErr := msg.Nak(); nakErr != nil {
					logger.Warn("failed to nak event after DLQ failure", "seq", getMsgSeq(msg), "error", nakErr)
				}
			} else {
				if ackErr := msg.Ack(); ackErr != nil {
					logger.Warn("failed to ack event after DLQ publish", "seq", getMsgSeq(msg), "error", ackErr)
				}
			}
			logger.Error("write failed, sent to DLQ", "seq", getMsgSeq(msg), "error", writeErr)
			return
		}
	}

	if ackErr := msg.Ack(); ackErr != nil {
		logger.Warn("failed to ack processed event", "seq", getMsgSeq(msg), "error", ackErr)
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
