package materialize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/gacopys/natsql/internal/kv"
	"github.com/gacopys/natsql/internal/testutil"
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

	w := NewWriter(kvb, "users", "|")
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

	w := NewWriter(kvb, "users", "|")

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

	w := NewWriter(kvb, "users", "|")

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

	w := NewWriter(kvb, "users", "|")

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

	w := NewWriter(kvb, "users", "|")
	err = w.Apply(ctx, nil)
	if err == nil {
		t.Fatal("expected error for nil mutation, got nil")
	}
}
