package materialize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql/v2/internal/kv"
	"github.com/gacopys/natsql/v2/internal/testutil"
)

func TestNewWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc, js := testutil.StartEmbeddedNATS(t)

	defer nc.Close()

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	w := NewWriter(kvb, "users", "|", Source{})
	if w == nil {
		t.Fatal("NewWriter returned nil")
	}
}

func TestWriter_Apply_WritesRowAtCorrectKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc, js := testutil.StartEmbeddedNATS(t)

	defer nc.Close()

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	w := NewWriter(kvb, "users", "|", Source{})

	mut := &RowMutation{
		PKParts: []string{"abc123"},
		RowData: map[string]any{
			"user_id": "abc123",
			"name":    "Alice",
			"age":     float64(30),
		},
		StreamSeq: 42,
		Timestamp: time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
	}

	err = w.Apply(ctx, mut)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Verify the row exists at the correct key
	expectedKey := kv.BuildPKKey("users", []string{"abc123"}, "|")
	entry, err := kvb.Get(ctx, expectedKey)
	if err != nil {
		t.Fatalf("Get(%q) failed: %v", expectedKey, err)
	}
	if entry == nil {
		t.Fatal("Get returned nil entry")
	}

	// Parse the stored JSON
	var stored map[string]any
	if err := json.Unmarshal(entry.Value(), &stored); err != nil {
		t.Fatalf("unmarshal stored row failed: %v", err)
	}

	// Verify column values
	if stored["user_id"] != "abc123" {
		t.Errorf("stored[user_id] = %v, want %q", stored["user_id"], "abc123")
	}
	if stored["name"] != "Alice" {
		t.Errorf("stored[name] = %v, want %q", stored["name"], "Alice")
	}
	if stored["age"] != float64(30) {
		t.Errorf("stored[age] = %v, want 30", stored["age"])
	}

	// Verify _meta
	meta, ok := stored["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("_meta field missing or wrong type: %T", stored["_meta"])
	}
	if meta["stream_seq"] != float64(42) {
		t.Errorf("_meta.stream_seq = %v, want 42", meta["stream_seq"])
	}
	updatedAt, ok := meta["updated_at"].(string)
	if !ok || updatedAt == "" {
		t.Errorf("_meta.updated_at missing or wrong type: %T", meta["updated_at"])
	}
}

func TestWriter_Apply_IdempotentOverwrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc, js := testutil.StartEmbeddedNATS(t)

	defer nc.Close()

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	w := NewWriter(kvb, "users", "|", Source{})

	// First write
	mut1 := &RowMutation{
		PKParts: []string{"abc123"},
		RowData: map[string]any{
			"user_id": "abc123",
			"name":    "Alice",
		},
		StreamSeq: 42,
		Timestamp: time.Now(),
	}
	if err := w.Apply(ctx, mut1); err != nil {
		t.Fatalf("First Apply failed: %v", err)
	}

	// Second write (overwrite with different data)
	mut2 := &RowMutation{
		PKParts: []string{"abc123"},
		RowData: map[string]any{
			"user_id": "abc123",
			"name":    "Alice Updated",
		},
		StreamSeq: 43,
		Timestamp: time.Now(),
	}
	if err := w.Apply(ctx, mut2); err != nil {
		t.Fatalf("Second Apply failed: %v", err)
	}

	// Verify the data was updated
	entry, err := kvb.Get(ctx, kv.BuildPKKey("users", []string{"abc123"}, "|"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	var stored map[string]any
	dec := json.NewDecoder(bytes.NewReader(entry.Value()))
	dec.UseNumber()
	if err := dec.Decode(&stored); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if stored["name"] != "Alice Updated" {
		t.Errorf("stored[name] = %v, want %q", stored["name"], "Alice Updated")
	}

	// Verify _meta updated
	meta, ok := stored["_meta"].(map[string]any)
	if !ok {
		t.Fatal("_meta missing after overwrite")
	}
	seq, ok := meta["stream_seq"].(json.Number)
	if !ok {
		t.Fatalf("expected json.Number, got %T", meta["stream_seq"])
	}
	if seq.String() != "43" {
		t.Errorf("_meta.stream_seq = %s, want 43", seq.String())
	}
}

func TestWriter_Apply_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc, js := testutil.StartEmbeddedNATS(t)

	defer nc.Close()

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	w := NewWriter(kvb, "users", "|", Source{})

	// Create a canceled context
	cancelledCtx, cancelFn := context.WithCancel(context.Background())
	cancelFn() // Cancel immediately

	mut := &RowMutation{
		PKParts: []string{"abc123"},
		RowData: map[string]any{
			"user_id": "abc123",
		},
		StreamSeq: 1,
		Timestamp: time.Now(),
	}

	err = w.Apply(cancelledCtx, mut)
	if err == nil {
		t.Fatal("expected error for canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestWriter_Apply_NilMutation_ReturnsError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc, js := testutil.StartEmbeddedNATS(t)

	defer nc.Close()

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	w := NewWriter(kvb, "users", "|", Source{})
	err = w.Apply(ctx, nil)
	if err == nil {
		t.Fatal("expected error for nil mutation, got nil")
	}
}

// storedRow reads and decodes the row at key, preserving number precision.
func storedRow(t *testing.T, ctx context.Context, kvb jetstream.KeyValue, key string) map[string]any {
	t.Helper()
	entry, err := kvb.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get(%q) failed: %v", key, err)
	}
	var row map[string]any
	dec := json.NewDecoder(bytes.NewReader(entry.Value()))
	dec.UseNumber()
	if err := dec.Decode(&row); err != nil {
		t.Fatalf("decoding row at %q failed: %v", key, err)
	}
	return row
}

func nameMutation(name string, seq uint64) *RowMutation {
	return &RowMutation{
		PKParts:   []string{"abc123"},
		RowData:   map[string]any{"user_id": "abc123", "name": name},
		StreamSeq: seq,
		Timestamp: time.Now(),
	}
}

// TestWriter_Apply_OlderEventDoesNotOverwrite verifies that a redelivered
// older event can't replace newer state (WR-01).
func TestWriter_Apply_OlderEventDoesNotOverwrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, js := testutil.StartEmbeddedNATS(t)
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	source := Source{Stream: "EVENTS", Created: time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)}
	w := NewWriter(kvb, "users", "/", source)
	key := kv.BuildPKKey("users", []string{"abc123"}, "/")

	for _, mut := range []*RowMutation{
		nameMutation("newer", 7),
		nameMutation("older", 5),    // redelivered after a NAK
		nameMutation("same seq", 7), // redelivered after a lost ack
	} {
		if err := w.Apply(ctx, mut); err != nil {
			t.Fatalf("Apply(seq %d) failed: %v", mut.StreamSeq, err)
		}
	}

	row := storedRow(t, ctx, kvb, key)
	if row["name"] != "newer" {
		t.Errorf("name = %v, want %q (older redelivery overwrote newer state)", row["name"], "newer")
	}
	meta, ok := row["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("_meta missing: %v", row)
	}
	if meta["stream"] != "EVENTS" || meta["stream_created"] != "2026-01-02T03:04:05.000000006Z" {
		t.Errorf("_meta source = %v / %v, want EVENTS / 2026-01-02T03:04:05.000000006Z", meta["stream"], meta["stream_created"])
	}
}

// TestWriter_Apply_NewSourceOverwrites verifies that sequence ordering only
// applies within one stream incarnation: after the source stream is
// recreated, its sequences restart and must still overwrite (WR-01).
func TestWriter_Apply_NewSourceOverwrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, js := testutil.StartEmbeddedNATS(t)
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}
	key := kv.BuildPKKey("users", []string{"abc123"}, "/")

	oldSource := Source{Stream: "EVENTS", Created: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	if err := NewWriter(kvb, "users", "/", oldSource).Apply(ctx, nameMutation("old stream", 1000)); err != nil {
		t.Fatalf("Apply from old source failed: %v", err)
	}

	newSource := Source{Stream: "EVENTS", Created: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)}
	if err := NewWriter(kvb, "users", "/", newSource).Apply(ctx, nameMutation("new stream", 1)); err != nil {
		t.Fatalf("Apply from new source failed: %v", err)
	}

	if row := storedRow(t, ctx, kvb, key); row["name"] != "new stream" {
		t.Errorf("name = %v, want %q (recreated stream's events were ignored)", row["name"], "new stream")
	}
}

// TestWriter_Apply_LegacyRowOverwritten verifies that rows written before
// _meta recorded their source are treated as older and overwritten.
func TestWriter_Apply_LegacyRowOverwritten(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, js := testutil.StartEmbeddedNATS(t)
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}
	key := kv.BuildPKKey("users", []string{"abc123"}, "/")
	legacy := `{"user_id":"abc123","name":"legacy","_meta":{"stream_seq":500,"updated_at":"2026-01-01T00:00:00Z"}}`
	if _, err := kvb.Put(ctx, key, []byte(legacy)); err != nil {
		t.Fatalf("Put legacy row failed: %v", err)
	}

	source := Source{Stream: "EVENTS", Created: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	if err := NewWriter(kvb, "users", "/", source).Apply(ctx, nameMutation("current", 1)); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if row := storedRow(t, ctx, kvb, key); row["name"] != "current" {
		t.Errorf("name = %v, want %q", row["name"], "current")
	}
}

// racingKV changes the row between the writer's read and its write once, so
// the writer's compare-and-swap fails and it must re-read.
type racingKV struct {
	jetstream.KeyValue
	raceValue []byte
	raced     bool
}

func (r *racingKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	entry, err := r.KeyValue.Get(ctx, key)
	if !r.raced {
		r.raced = true
		if _, putErr := r.Put(ctx, key, r.raceValue); putErr != nil {
			return nil, putErr
		}
	}
	return entry, err
}

// TestWriter_Apply_RetriesLostCASRace verifies that a concurrent write
// between read and write is detected and re-evaluated rather than
// overwritten blindly.
func TestWriter_Apply_RetriesLostCASRace(t *testing.T) {
	source := Source{Stream: "EVENTS", Created: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	racer := `{"user_id":"abc123","name":"racer","_meta":{"stream_seq":6,"stream":"EVENTS","stream_created":"2026-01-01T00:00:00Z"}}`

	tests := []struct {
		name     string
		seq      uint64
		wantName string
	}{
		{"newer event wins after re-read", 7, "mine"},
		{"older event yields after re-read", 5, "racer"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			_, js := testutil.StartEmbeddedNATS(t)
			kvb, err := kv.InitBucket(ctx, js, 1)
			if err != nil {
				t.Fatalf("InitBucket failed: %v", err)
			}
			key := kv.BuildPKKey("users", []string{"abc123"}, "/")
			if _, err := kvb.Put(ctx, key, []byte(`{"user_id":"abc123","name":"original"}`)); err != nil {
				t.Fatalf("Put failed: %v", err)
			}

			racing := &racingKV{KeyValue: kvb, raceValue: []byte(racer)}
			if err := NewWriter(racing, "users", "/", source).Apply(ctx, nameMutation("mine", tt.seq)); err != nil {
				t.Fatalf("Apply failed: %v", err)
			}
			if !racing.raced {
				t.Fatal("race was not injected")
			}
			if row := storedRow(t, ctx, kvb, key); row["name"] != tt.wantName {
				t.Errorf("name = %v, want %q", row["name"], tt.wantName)
			}
		})
	}
}
