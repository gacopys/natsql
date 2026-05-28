package materialize

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"natsql"
)

// ConsumerName returns the durable consumer name for a view.
func ConsumerName(viewName string) string {
	return "wrong-" + viewName
}

// SetupConsumer creates or resumes a durable pull consumer for a materialized view.
func SetupConsumer(ctx context.Context, js jetstream.JetStream, streamName string, viewName string, sourceSubject string, cfg natsql.ConsumerConfig) (jetstream.Consumer, error) {
	return nil, fmt.Errorf("not implemented")
}
