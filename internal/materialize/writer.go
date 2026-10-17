package materialize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql/v2/internal/kv"
)

// maxCASAttempts bounds how often Apply re-reads a row after losing a
// compare-and-swap race to a concurrent writer before giving up; the caller
// then retries the event later.
const maxCASAttempts = 10

var (
	errNilMutation = errors.New("mutation is nil")

	// Serialization failures wrap errEncodeRow: such a row can never be
	// written, so retrying the event cannot help.
	errEncodeRow = errors.New("encoding row")
)

// Source identifies one incarnation of a view's source stream. Stream
// sequences restart when a stream is deleted and recreated, so Writer only
// compares sequences between rows written from the same Source (WR-01).
type Source struct {
	Stream  string
	Created time.Time
}

func (s Source) created() string {
	if s.Created.IsZero() {
		return ""
	}
	return s.Created.UTC().Format(time.RFC3339Nano)
}

// rowMeta is the part of a stored row's _meta that ordering decisions use.
type rowMeta struct {
	StreamSeq     uint64 `json:"stream_seq"`
	Stream        string `json:"stream"`
	StreamCreated string `json:"stream_created"`
}

// Writer handles KV row writes for a materialized view.
type Writer struct {
	kv        jetstream.KeyValue
	viewName  string
	separator string
	source    Source
}

// NewWriter creates a new Writer for the given KV bucket, view name, PK
// separator, and the source stream incarnation the view consumes.
func NewWriter(kv jetstream.KeyValue, viewName string, separator string, source Source) *Writer {
	return &Writer{kv: kv, viewName: viewName, separator: separator, source: source}
}

// Apply writes a RowMutation to the KV store as JSON at
// BuildPKKey(viewName, pkParts, separator), with _meta metadata.
//
// Writes are ordered by source stream sequence (WR-01). JetStream can deliver
// an older event after a newer one (NAK, lost ack, AckWait expiry), so if the
// stored row was already written from the same Source at the same or a later
// sequence, Apply leaves it untouched and returns nil. Otherwise it writes
// with Create or Update against the revision it just read, so a concurrent
// writer can't be overwritten blindly either; on a lost race it re-reads and
// tries again.
func (w *Writer) Apply(ctx context.Context, mut *RowMutation) error {
	if mut == nil {
		return errNilMutation
	}

	rowJSON, err := w.encodeRow(mut)
	if err != nil {
		return err
	}

	pkKey := kv.BuildPKKey(w.viewName, mut.PKParts, w.separator)
	for range maxCASAttempts {
		err := w.applyOnce(ctx, pkKey, rowJSON, mut.StreamSeq)
		if !errors.Is(err, jetstream.ErrKeyExists) {
			return err
		}
	}
	return fmt.Errorf("writing row at %q: lost %d compare-and-swap races in a row: %w", pkKey, maxCASAttempts, jetstream.ErrKeyExists)
}

func (w *Writer) encodeRow(mut *RowMutation) ([]byte, error) {
	row := make(map[string]any, len(mut.RowData)+1)
	for k, v := range mut.RowData {
		row[k] = v
	}
	row["_meta"] = map[string]any{
		"stream_seq":     mut.StreamSeq,
		"stream":         w.source.Stream,
		"stream_created": w.source.created(),
		"updated_at":     mut.Timestamp.Format(time.RFC3339Nano),
	}

	rowJSON, err := json.Marshal(row)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errEncodeRow, err)
	}
	return rowJSON, nil
}

// applyOnce makes one read-compare-write attempt. It returns an error
// matching jetstream.ErrKeyExists when another writer changed the row between
// the read and the write.
func (w *Writer) applyOnce(ctx context.Context, pkKey string, rowJSON []byte, seq uint64) error {
	entry, err := w.kv.Get(ctx, pkKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		if _, err := w.kv.Create(ctx, pkKey, rowJSON); err != nil {
			return fmt.Errorf("creating row at %q: %w", pkKey, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading row at %q: %w", pkKey, err)
	}

	if w.alreadyApplied(entry.Value(), seq) {
		return nil
	}
	if _, err := w.kv.Update(ctx, pkKey, rowJSON, entry.Revision()); err != nil {
		return fmt.Errorf("updating row at %q: %w", pkKey, err)
	}
	return nil
}

// alreadyApplied reports whether the stored row reflects the event at seq or
// a later one from this writer's Source. A row from another Source (a
// recreated or different stream), a row written before _meta recorded its
// source, and an undecodable row are all treated as older and overwritten.
func (w *Writer) alreadyApplied(stored []byte, seq uint64) bool {
	var row map[string]json.RawMessage
	if err := json.Unmarshal(stored, &row); err != nil {
		return false
	}
	var meta rowMeta
	if err := json.Unmarshal(row["_meta"], &meta); err != nil {
		return false
	}
	if meta.Stream != w.source.Stream || meta.StreamCreated != w.source.created() {
		return false
	}
	return meta.StreamSeq >= seq
}
