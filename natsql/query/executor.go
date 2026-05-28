package query

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

// Execute performs a direct PK lookup using kv.Get().
// This is a stub for the RED phase.
func (p *PKLookupPlan) Execute(ctx context.Context, kv jetstream.KeyValue) ([]map[string]any, error) {
	return nil, nil
}

// Execute performs a full scan using kv.ListKeys() with client-side filtering.
// This is a stub for the RED phase.
func (p *FullScanPlan) Execute(ctx context.Context, kv jetstream.KeyValue) ([]map[string]any, error) {
	return nil, nil
}
