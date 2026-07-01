package materialize

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	natsql "github.com/gacopys/natsql/internal/cfg"
)

// ConsumerName returns the durable consumer name for a view.
// Format: natsql-{viewName} per D-10.
func ConsumerName(viewName string) string {
	return "natsql-" + viewName
}

// SetupConsumer creates or resumes a durable pull consumer for a materialized view.
// On first creation, it starts from the beginning of the stream (DeliverAll).
// On restart, the durable consumer resumes from the last acknowledged message.
// Returns an error if the stream does not exist.
func SetupConsumer(ctx context.Context, js jetstream.JetStream, streamName string, viewName string, sourceSubject string, cfg natsql.ConsumerConfig) (jetstream.Consumer, error) {
	// Get stream handle — error if stream doesn't exist
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("stream %q not found: %w", streamName, err)
	}

	// Apply defaults for zero values
	maxDeliver := cfg.MaxDeliver
	if maxDeliver <= 0 {
		maxDeliver = 10
	}

	ackWaitSeconds := cfg.AckWaitSeconds
	if ackWaitSeconds <= 0 {
		ackWaitSeconds = 30
	}

	maxAckPending := cfg.MaxAckPending
	if maxAckPending <= 0 {
		maxAckPending = 50
	}

	consumerCfg := jetstream.ConsumerConfig{
		Durable:       ConsumerName(viewName),
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxDeliver:    maxDeliver,
		AckWait:       time.Duration(ackWaitSeconds) * time.Second,
		MaxAckPending: maxAckPending,
		// CR-11: No InactiveThreshold — durable consumers persist until explicitly deleted.
	}

	if sourceSubject != "" {
		consumerCfg.FilterSubject = sourceSubject
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		return nil, fmt.Errorf("creating consumer for view %q: %w", viewName, err)
	}

	return cons, nil
}
