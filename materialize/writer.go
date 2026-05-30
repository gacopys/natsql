package materialize

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql/kv"
)

// Writer handles KV row writes for a materialized view.
type Writer struct {
	kv       jetstream.KeyValue
	viewName string
}

// NewWriter creates a new Writer for the given KV bucket and view name.
func NewWriter(kv jetstream.KeyValue, viewName string) *Writer {
	return &Writer{kv: kv, viewName: viewName}
}

// Apply writes a RowMutation to the KV store.
// Writes row data as JSON at kv.PkKey(viewName, mut.PK) with _meta metadata.
// Uses kv.Put (not Create) for idempotent upsert per D-11.
// Returns error if mutation is nil.
func (w *Writer) Apply(ctx context.Context, mut *RowMutation) error {
	if mut == nil {
		return fmt.Errorf("mutation is nil")
	}

	// Build row with _meta metadata
	row := make(map[string]any, len(mut.RowData)+1)
	for k, v := range mut.RowData {
		row[k] = v
	}
	row["_meta"] = map[string]any{
		"stream_seq": mut.StreamSeq,
		"updated_at": mut.Timestamp.Format("2006-01-02T15:04:05.999999999Z07:00"), // RFC3339Nano
	}

	rowJSON, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("marshaling row: %w", err)
	}

	pkKey := kv.PkKey(w.viewName, mut.PK)
	_, err = w.kv.Put(ctx, pkKey, rowJSON)
	if err != nil {
		return fmt.Errorf("putting row at %q: %w", pkKey, err)
	}

	return nil
}
