package materialize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql/internal/kv"
)

// Writer handles KV row writes for a materialized view.
type Writer struct {
	kv        jetstream.KeyValue
	viewName  string
	separator string
}

// NewWriter creates a new Writer for the given KV bucket, view name, and PK separator.
func NewWriter(kv jetstream.KeyValue, viewName string, separator string) *Writer {
	return &Writer{kv: kv, viewName: viewName, separator: separator}
}

// Apply writes a RowMutation to the KV store.
// Writes row data as JSON at BuildPKKey(viewName, pkParts, separator) with _meta metadata.
// Uses kv.Put (not Create) for idempotent upsert per D-11.
// Returns error if mutation is nil.
func (w *Writer) Apply(ctx context.Context, mut *RowMutation) error {
	if mut == nil {
		return errors.New("mutation is nil")
	}

	// Build row with _meta metadata
	row := make(map[string]any, len(mut.RowData)+1)
	for k, v := range mut.RowData {
		row[k] = v
	}
	row["_meta"] = map[string]any{
		"stream_seq": mut.StreamSeq,
		"updated_at": mut.Timestamp.Format(time.RFC3339Nano), // RFC3339Nano
	}

	rowJSON, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("marshaling row: %w", err)
	}

	pkKey := kv.BuildPKKey(w.viewName, mut.PKParts, w.separator)
	_, err = w.kv.Put(ctx, pkKey, rowJSON)
	if err != nil {
		return fmt.Errorf("putting row at %q: %w", pkKey, err)
	}

	return nil
}
