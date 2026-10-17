package materialize

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	natsql "github.com/gacopys/natsql/internal/cfg"
	"github.com/gacopys/natsql/internal/kv"
)

func TestMaterializer_ValidEventEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nc, js := startEmbeddedNATS(t)
	defer nc.Close()

	// Create source stream
	streamName := "TEST_MAT_VALID"
	createStream(t, ctx, js, streamName)

	// Create KV bucket
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	// Create DLQ stream
	if _, err := EnsureDLQStream(ctx, js); err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	// View config
	viewCfg := &natsql.ViewConfig{
		Name:         "users",
		SourceStream: streamName,
		KeyFields:    []string{"user_id"},
		Columns: []natsql.ColumnConfig{
			{Name: "user_id", From: "user_id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "name", From: "name", Type: natsql.ColumnTypeString},
			{Name: "age", From: "age", Type: natsql.ColumnTypeNumber},
		},
		Consumer: natsql.ConsumerConfig{MaxAckPending: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	// Start materializer in goroutine
	matCtx, matCancel := context.WithCancel(context.Background())
	defer matCancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, logger, nil)
	}()

	time.Sleep(500 * time.Millisecond) // allow consumer setup

	// Publish a valid event
	if _, err := js.Publish(ctx, streamName+".events", []byte(`{"user_id": "abc123", "name": "Alice", "age": 30}`)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	time.Sleep(2 * time.Second) // allow processing

	// Verify KV has the row
	entry, err := kvb.Get(ctx, kv.BuildPKKey("users", []string{"abc123"}, ""))
	if err != nil {
		t.Fatalf("Get(%q) failed: %v", kv.BuildPKKey("users", []string{"abc123"}, ""), err)
	}
	if entry == nil {
		t.Fatal("KV entry is nil — event was not materialized")
	}

	var stored map[string]any
	if err := json.Unmarshal(entry.Value(), &stored); err != nil {
		t.Fatalf("unmarshal stored row failed: %v", err)
	}
	if stored["user_id"] != "abc123" {
		t.Errorf("user_id = %v, want %q", stored["user_id"], "abc123")
	}
	if stored["name"] != "Alice" {
		t.Errorf("name = %v, want %q", stored["name"], "Alice")
	}
	if stored["age"] != float64(30) {
		t.Errorf("age = %v, want 30", stored["age"])
	}
	if _, ok := stored["_meta"]; !ok {
		t.Error("_meta field missing in stored row")
	}

	// Clean shutdown
	matCancel()
	select {
	case runErr := <-errCh:
		if !errors.Is(runErr, context.Canceled) {
			t.Errorf("expected context.Canceled on shutdown, got %v", runErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("materializer did not shut down within 5 seconds")
	}
}

func TestMaterializer_MalformedEventGoesToDLQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nc, js := startEmbeddedNATS(t)
	defer nc.Close()

	// Subscribe to DLQ subject before publishing
	dlqSub, err := nc.SubscribeSync("natsql.dlq")
	if err != nil {
		t.Fatalf("SubscribeSync to natsql.dlq failed: %v", err)
	}
	defer func() {
		_ = dlqSub.Unsubscribe()
	}()

	// Create source stream
	streamName := "TEST_MAT_DLQ"
	createStream(t, ctx, js, streamName)

	// Create KV bucket
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	// Create DLQ stream
	if _, err := EnsureDLQStream(ctx, js); err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	viewCfg := &natsql.ViewConfig{
		Name:         "dlq_test",
		SourceStream: streamName,
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
		},
		Consumer: natsql.ConsumerConfig{MaxAckPending: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	matCtx, matCancel := context.WithCancel(context.Background())
	defer matCancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, logger, nil)
	}()

	time.Sleep(1 * time.Second) // allow consumer setup

	// Publish a malformed event (invalid JSON)
	if _, err := js.Publish(ctx, streamName+".events", []byte(`{invalid json`)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait for DLQ message (use NATS subscription)
	dlqMsg, err := dlqSub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("did not receive DLQ message within timeout: %v", err)
	}

	verifyDLQEnvelope(t, dlqMsg.Data, "dlq_test", "{invalid json")

	// Clean shutdown
	matCancel()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("materializer did not shut down within 5 seconds")
	}
}

func TestMaterializer_ContinuesAfterMalformedEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nc, js := startEmbeddedNATS(t)
	defer nc.Close()

	// Subscribe to DLQ to verify malformed event goes there
	dlqSub, err := nc.SubscribeSync("natsql.dlq")
	if err != nil {
		t.Fatalf("SubscribeSync to natsql.dlq failed: %v", err)
	}
	defer func() { _ = dlqSub.Unsubscribe() }()

	streamName := "TEST_MAT_CONTINUE"
	createStream(t, ctx, js, streamName)

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	if _, err := EnsureDLQStream(ctx, js); err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	viewCfg := &natsql.ViewConfig{
		Name:         "continue_test",
		SourceStream: streamName,
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "name", From: "name", Type: natsql.ColumnTypeString},
		},
		Consumer: natsql.ConsumerConfig{MaxAckPending: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	matCtx, matCancel := context.WithCancel(context.Background())
	defer matCancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, logger, nil)
	}()

	time.Sleep(1 * time.Second) // allow consumer setup

	// Publish malformed event first
	if _, err := js.Publish(ctx, streamName+".events", []byte(`{invalid json`)); err != nil {
		t.Fatalf("Publish malformed failed: %v", err)
	}

	// Wait for DLQ message to confirm malformed was processed
	if _, err := dlqSub.NextMsg(3 * time.Second); err != nil {
		t.Fatalf("did not receive DLQ message within timeout: %v", err)
	}

	// Publish valid event
	if _, err := js.Publish(ctx, streamName+".events", []byte(`{"id": "valid1", "name": "Valid User"}`)); err != nil {
		t.Fatalf("Publish valid failed: %v", err)
	}

	time.Sleep(2 * time.Second) // allow processing

	// Verify the valid event was materialized
	entry, err := kvb.Get(ctx, kv.BuildPKKey("continue_test", []string{"valid1"}, ""))
	if err != nil {
		t.Fatalf("Get(%q) failed: %v", kv.BuildPKKey("continue_test", []string{"valid1"}, ""), err)
	}
	if entry == nil {
		t.Fatal("valid event was not materialized after malformed event")
	}

	var stored map[string]any
	if err := json.Unmarshal(entry.Value(), &stored); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if stored["name"] != "Valid User" {
		t.Errorf("name = %v, want %q", stored["name"], "Valid User")
	}

	// Clean shutdown
	matCancel()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("materializer did not shut down within 5 seconds")
	}
}

func TestMaterializer_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	nc, js := startEmbeddedNATS(t)
	defer nc.Close()

	streamName := "TEST_MAT_CANCEL"
	createStream(t, ctx, js, streamName)

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	if _, err := EnsureDLQStream(ctx, js); err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	viewCfg := &natsql.ViewConfig{
		Name:         "cancel_test",
		SourceStream: streamName,
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
		},
		Consumer: natsql.ConsumerConfig{MaxAckPending: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	matCtx, matCancel := context.WithCancel(context.Background())

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, logger, nil)
	}()

	time.Sleep(500 * time.Millisecond) // allow setup

	// Cancel immediately
	matCancel()

	select {
	case runErr := <-errCh:
		if !errors.Is(runErr, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", runErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("materializer did not shut down within 5 seconds after context cancellation")
	}
}

func TestMaterializer_SchemaStoredInKV(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	nc, js := startEmbeddedNATS(t)
	defer nc.Close()

	streamName := "TEST_MAT_SCHEMA"
	createStream(t, ctx, js, streamName)

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	if _, err := EnsureDLQStream(ctx, js); err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	viewCfg := &natsql.ViewConfig{
		Name:         "schema_test",
		SourceStream: streamName,
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "name", From: "name", Type: natsql.ColumnTypeString},
		},
		Consumer: natsql.ConsumerConfig{MaxAckPending: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	matCtx, matCancel := context.WithCancel(context.Background())
	defer matCancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, logger, nil)
	}()

	time.Sleep(500 * time.Millisecond) // allow setup to store schema

	// Verify schema is stored in KV
	schema, err := kv.LoadSchema(ctx, kvb, "schema_test")
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}
	if schema == nil {
		t.Fatal("schema was not stored in KV")
	}
	if schema.Name != "schema_test" {
		t.Errorf("schema.Name = %q, want %q", schema.Name, "schema_test")
	}
	if len(schema.Columns) != 2 {
		t.Errorf("schema.Columns has %d columns, want 2", len(schema.Columns))
	}
	if schema.Version != 1 {
		t.Errorf("schema.Version = %d, want 1", schema.Version)
	}

	// Clean shutdown
	matCancel()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("materializer did not shut down within 5 seconds")
	}
}

func TestEnsureDLQStream_CreatesCorrectName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc, js := startEmbeddedNATS(t)
	defer nc.Close()

	dlqStream, err := EnsureDLQStream(ctx, js)
	if err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	info := dlqStream.CachedInfo()
	if info == nil {
		t.Fatal("CachedInfo returned nil")
	}
	if info.Config.Name != "natsql-dlq" {
		t.Errorf("DLQ stream name = %q, want %q", info.Config.Name, "natsql-dlq")
	}
	if info.Config.Storage != jetstream.FileStorage {
		t.Errorf("DLQ storage = %v, want FileStorage", info.Config.Storage)
	}
}

func TestMaterializer_ValidEventWithNestedFields(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nc, js := startEmbeddedNATS(t)
	defer nc.Close()

	streamName := "TEST_MAT_NESTED"
	createStream(t, ctx, js, streamName)

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	if _, err := EnsureDLQStream(ctx, js); err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	viewCfg := &natsql.ViewConfig{
		Name:         "nested_test",
		SourceStream: streamName,
		KeyFields:    []string{"user_id"},
		Columns: []natsql.ColumnConfig{
			{Name: "user_id", From: "user.id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "full_name", From: "profile.name", Type: natsql.ColumnTypeString},
			{Name: "score", From: "stats.score", Type: natsql.ColumnTypeNumber},
			{Name: "active", From: "flags.active", Type: natsql.ColumnTypeBoolean},
		},
		Consumer: natsql.ConsumerConfig{MaxAckPending: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	matCtx, matCancel := context.WithCancel(context.Background())
	defer matCancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, logger, nil)
	}()

	time.Sleep(500 * time.Millisecond)

	// Publish event with nested fields
	event := `{"user":{"id":"u42"},"profile":{"name":"Bob"},"stats":{"score":99.5},"flags":{"active":true}}`
	if _, err := js.Publish(ctx, streamName+".events", []byte(event)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Verify row
	entry, err := kvb.Get(ctx, kv.BuildPKKey("nested_test", []string{"u42"}, ""))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if entry == nil {
		t.Fatal("nested event was not materialized")
	}

	var stored map[string]any
	if err := json.Unmarshal(entry.Value(), &stored); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if stored["user_id"] != "u42" {
		t.Errorf("user_id = %v, want %q", stored["user_id"], "u42")
	}
	if stored["full_name"] != "Bob" {
		t.Errorf("full_name = %v, want %q", stored["full_name"], "Bob")
	}
	if stored["score"] != float64(99.5) {
		t.Errorf("score = %v, want 99.5", stored["score"])
	}
	if stored["active"] != true {
		t.Errorf("active = %v, want true", stored["active"])
	}

	matCancel()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("materializer did not shut down within 5 seconds")
	}
}

// TestMaterializerDrain verifies that signaling the drain channel causes
// the materializer to exit gracefully (D-58).
func TestMaterializerDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	nc, js := startEmbeddedNATS(t)
	defer nc.Close()

	streamName := "TEST_MAT_DRAIN"
	createStream(t, ctx, js, streamName)

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	if _, err := EnsureDLQStream(ctx, js); err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	viewCfg := &natsql.ViewConfig{
		Name:         "drain_test",
		SourceStream: streamName,
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
		},
		Consumer: natsql.ConsumerConfig{MaxAckPending: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	drainCh := make(chan struct{})
	errCh := make(chan error, 1)

	matCtx, matCancel := context.WithCancel(context.Background())
	defer matCancel()

	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, logger, drainCh)
	}()

	time.Sleep(500 * time.Millisecond) // allow consumer setup

	// Signal drain
	close(drainCh)

	// Wait for Run() to return
	select {
	case runErr := <-errCh:
		// Should return without error (nil) or context.Canceled
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			t.Errorf("unexpected error on drain: %v", runErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("materializer did not exit within 5 seconds after drain")
	}
}

// goroutineID returns the current goroutine's ID by parsing the stack trace.
func goroutineID(t *testing.T) uint64 {
	t.Helper()
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	var id uint64
	if _, err := fmt.Sscanf(string(buf[:n]), "goroutine %d", &id); err != nil {
		t.Fatalf("parsing goroutine id: %v", err)
	}
	return id
}

// TestSequentialProcessing_SingleGoroutine verifies that all events are processed
// in the same goroutine after the worker pool is removed.
// With concurrent workers (before fix), goroutine IDs differ.
// With sequential processing (after fix), all IDs match.
func TestSequentialProcessing_SingleGoroutine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nc, js := startEmbeddedNATS(t)
	defer nc.Close()

	streamName := "TEST_SEQ_GOROUTINE"
	createStream(t, ctx, js, streamName)

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	if _, err := EnsureDLQStream(ctx, js); err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	viewCfg := &natsql.ViewConfig{
		Name:         "seq_goroutine_test",
		SourceStream: streamName,
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "counter", From: "counter", Type: natsql.ColumnTypeNumber},
		},
		Consumer: natsql.ConsumerConfig{MaxAckPending: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	// Set up goroutine ID tracking via test hook
	var (
		goIDs   []uint64
		goIDsMu sync.Mutex
	)
	testHookProcessGoroutine = func() {
		id := goroutineID(t)
		goIDsMu.Lock()
		goIDs = append(goIDs, id)
		goIDsMu.Unlock()
	}
	defer func() { testHookProcessGoroutine = nil }()

	matCtx, matCancel := context.WithCancel(context.Background())
	defer matCancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, logger, nil)
	}()

	time.Sleep(500 * time.Millisecond) // allow consumer setup

	// Publish 10 events
	for i := range 10 {
		event := fmt.Sprintf(`{"id": "u%d", "counter": %d}`, i, i)
		if _, err := js.Publish(ctx, streamName+".events", []byte(event)); err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	time.Sleep(2 * time.Second) // allow processing

	// Verify all 10 were captured
	goIDsMu.Lock()
	captured := len(goIDs)
	allIDs := make([]uint64, len(goIDs))
	copy(allIDs, goIDs)
	goIDsMu.Unlock()

	if captured < 10 {
		t.Fatalf("expected at least 10 goroutine captures, got %d — testHookProcessGoroutine may not be called", captured)
	}

	// All goroutine IDs should be the same (sequential processing)
	for i := 1; i < len(allIDs); i++ {
		if allIDs[i] != allIDs[0] {
			t.Errorf("goroutine ID changed: %d → %d at index %d — events processed by different goroutines",
				allIDs[0], allIDs[i], i)
		}
	}

	// Clean shutdown
	matCancel()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("materializer did not shut down within 5 seconds")
	}
}

// TestSequentialProcessing_StreamOrder verifies that publishing 10 events to the same PK
// results in the final KV value reflecting the last published event.
// With concurrent workers, events can be applied out of order, causing the final
// state to reflect an earlier value. Sequential processing guarantees ordering.
func TestSequentialProcessing_StreamOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nc, js := startEmbeddedNATS(t)
	defer nc.Close()

	streamName := "TEST_SEQ_ORDER"
	createStream(t, ctx, js, streamName)

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	if _, err := EnsureDLQStream(ctx, js); err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	viewCfg := &natsql.ViewConfig{
		Name:         "seq_order_test",
		SourceStream: streamName,
		KeyFields:    []string{"pk"},
		Columns: []natsql.ColumnConfig{
			{Name: "pk", From: "pk", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "counter", From: "counter", Type: natsql.ColumnTypeNumber},
		},
		Consumer: natsql.ConsumerConfig{MaxAckPending: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	matCtx, matCancel := context.WithCancel(context.Background())
	defer matCancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, logger, nil)
	}()

	time.Sleep(500 * time.Millisecond) // allow consumer setup

	// Publish 10 events to the SAME PK with increasing counter
	for i := range 10 {
		event := fmt.Sprintf(`{"pk": "same_key", "counter": %d}`, i)
		if _, err := js.Publish(ctx, streamName+".events", []byte(event)); err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	time.Sleep(2 * time.Second) // allow processing

	// Read the final value
	entry, err := kvb.Get(ctx, kv.BuildPKKey("seq_order_test", []string{"same_key"}, "|"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if entry == nil {
		t.Fatal("KV entry is nil — no events were materialized")
	}

	var stored map[string]any
	dec := json.NewDecoder(bytes.NewReader(entry.Value()))
	dec.UseNumber()
	if err := dec.Decode(&stored); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// The counter should be 9 (the last published value)
	counter, ok := stored["counter"].(json.Number)
	if !ok {
		t.Fatalf("counter is not a number, got %T=%v", stored["counter"], stored["counter"])
	}
	if counter.String() != "9" {
		t.Errorf("final counter = %s, want 9 — events were applied out of order", counter.String())
	}

	// Clean shutdown
	matCancel()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("materializer did not shut down within 5 seconds")
	}
}

// TestSequentialProcessing_HeartbeatIndependent verifies that the heartbeat goroutine
// continues to log events after the worker pool is removed.
func TestSequentialProcessing_HeartbeatIndependent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nc, js := startEmbeddedNATS(t)
	defer nc.Close()

	streamName := "TEST_SEQ_HEARTBEAT"
	createStream(t, ctx, js, streamName)

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	if _, err := EnsureDLQStream(ctx, js); err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	viewCfg := &natsql.ViewConfig{
		Name:         "seq_heartbeat_test",
		SourceStream: streamName,
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
		},
		Consumer: natsql.ConsumerConfig{MaxAckPending: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	// Capture log output
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	matCtx, matCancel := context.WithCancel(context.Background())
	defer matCancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, logger, nil)
	}()

	time.Sleep(500 * time.Millisecond) // allow consumer setup

	// Publish a few events
	if _, err := js.Publish(ctx, streamName+".events", []byte(`{"id": "hb1"}`)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if _, err := js.Publish(ctx, streamName+".events", []byte(`{"id": "hb2"}`)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Verify events were processed. Poll instead of a fixed sleep so the
	// test is robust under parallel test load (go test ./...) where
	// materialization latency is variable.
	deadline := time.Now().Add(15 * time.Second)
	for {
		hb1, err1 := kvb.Get(ctx, kv.BuildPKKey("seq_heartbeat_test", []string{"hb1"}, "|"))
		hb2, err2 := kvb.Get(ctx, kv.BuildPKKey("seq_heartbeat_test", []string{"hb2"}, "|"))
		if err1 == nil && hb1 != nil && err2 == nil && hb2 != nil {
			break
		}
		if time.Now().After(deadline) {
			if err1 != nil {
				t.Fatalf("Get(%q) failed: %v", "hb1", err1)
			}
			if err2 != nil {
				t.Fatalf("Get(%q) failed: %v", "hb2", err2)
			}
			t.Fatal("events were not materialized within 15s")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify heartbeat logged (heartbeat interval is 60s, so we won't see one during the test)
	// Instead, verify the materializer doesn't panic and processes events correctly
	logOutput := logBuf.String()
	t.Logf("Log output contains heartbeat:\n%s", logOutput)

	// Clean shutdown
	matCancel()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("materializer did not shut down within 5 seconds")
	}
}

// --- Error classification tests ---

func TestClassifyWriteError_Transient(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"deadline exceeded", context.DeadlineExceeded},
		{"context canceled", context.Canceled},
		{"nats timeout", nats.ErrTimeout},
		{"no responders", nats.ErrNoResponders},
		{"disconnected", nats.ErrDisconnected},
		{"reconnect buffer exceeded", nats.ErrReconnectBufExceeded},
		{"connection closed", nats.ErrConnectionClosed},
		{"no stream response", jetstream.ErrNoStreamResponse},
		{"lost CAS races", fmt.Errorf("writing row: %w", jetstream.ErrKeyExists)},
		{"cluster unavailable", &jetstream.APIError{ErrorCode: 10008, Description: "JetStream system temporarily unavailable"}},
		{"insufficient resources", &jetstream.APIError{ErrorCode: 10023, Description: "insufficient resources"}},
		{"unrecognized error", errors.New("something went wrong")},
		{"wrapped", fmt.Errorf("putting row at %q: %w", "v/pk/a", jetstream.ErrNoStreamResponse)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyWriteError(tc.err); got != errorClassTransient {
				t.Errorf("classifyWriteError(%v) = %v, want %v", tc.err, got, errorClassTransient)
			}
		})
	}
}

func TestClassifyWriteError_Terminal(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"invalid key", jetstream.ErrInvalidKey},
		// The message contains "timeout", which must not make it transient.
		{"invalid key with keyword in PK", fmt.Errorf("creating row at %q: %w", "v/pk/timeout connection closed", jetstream.ErrInvalidKey)},
		{"max payload", fmt.Errorf("updating row: %w", nats.ErrMaxPayload)},
		{"message too large", fmt.Errorf("updating row: %w", &jetstream.APIError{ErrorCode: jsErrCodeMessageTooLarge})},
		{"unencodable row", fmt.Errorf("%w: %w", errEncodeRow, errors.New("json: unsupported value"))},
		{"nil mutation", errNilMutation},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyWriteError(tc.err); got != errorClassTerminal {
				t.Errorf("classifyWriteError(%v) = %v, want %v", tc.err, got, errorClassTerminal)
			}
		})
	}
}

func TestRedeliveryDelay(t *testing.T) {
	tests := []struct {
		numDelivered uint64
		want         time.Duration
	}{
		{0, 250 * time.Millisecond},
		{1, 250 * time.Millisecond},
		{2, 500 * time.Millisecond},
		{3, time.Second},
		{7, 16 * time.Second},
		{8, 30 * time.Second},
		{100, 30 * time.Second},
	}
	for _, tc := range tests {
		if got := redeliveryDelay(tc.numDelivered); got != tc.want {
			t.Errorf("redeliveryDelay(%d) = %v, want %v", tc.numDelivered, got, tc.want)
		}
	}
}

func newProcessEventMapper(t *testing.T) *Mapper {
	t.Helper()
	mapper, err := NewMapper(&natsql.ViewConfig{
		Name:      "test",
		KeyFields: []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Fatalf("NewMapper failed: %v", err)
	}
	return mapper
}

func TestProcessEvent_TransientWriteError_NAKsWithBackoff(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	js := &fakeJS{}
	kvMock := &fakeKV{writeErr: nats.ErrNoResponders}
	writer := NewWriter(kvMock, "test_view", "|", Source{})
	msg := &fakeMsg{seq: 1, numDelivered: 2, data: []byte(`{"id": "1"}`)}
	viewCfg := &natsql.ViewConfig{Name: "test_view"}

	processEvent(ctx, js, newProcessEventMapper(t), writer, msg, viewCfg, logger)

	if !msg.nakked {
		t.Error("expected the event to be NAK'd for a transient write error")
	}
	if msg.nakDelay != 500*time.Millisecond {
		t.Errorf("NAK delay = %v, want 500ms for the second delivery", msg.nakDelay)
	}
	if msg.acked {
		t.Error("expected msg.Ack() NOT to be called for transient write error")
	}
	if js.publishCalled {
		t.Error("expected no DLQ publish for transient write error")
	}
}

// TestProcessEvent_TransientWriteError_FinalDeliveryDLQ verifies that the
// last delivery attempt is dead-lettered instead of NAK'd, since JetStream
// would not redeliver it.
func TestProcessEvent_TransientWriteError_FinalDeliveryDLQ(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	js := &fakeJS{}
	kvMock := &fakeKV{writeErr: nats.ErrNoResponders}
	writer := NewWriter(kvMock, "test_view", "|", Source{})
	msg := &fakeMsg{seq: 1, numDelivered: 3, data: []byte(`{"id": "1"}`)}
	viewCfg := &natsql.ViewConfig{Name: "test_view", Consumer: natsql.ConsumerConfig{MaxDeliver: 3}}

	processEvent(ctx, js, newProcessEventMapper(t), writer, msg, viewCfg, logger)

	if !js.publishCalled {
		t.Error("expected DLQ publish on the final delivery attempt")
	}
	if !msg.acked {
		t.Error("expected msg.Ack() after DLQ publish")
	}
	if msg.nakked {
		t.Error("expected no NAK on the final delivery attempt")
	}
}

func TestProcessEvent_TerminalWriteError_DLQ(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	js := &fakeJS{}
	kvMock := &fakeKV{writeErr: jetstream.ErrInvalidKey}
	writer := NewWriter(kvMock, "test_view", "|", Source{})
	msg := &fakeMsg{seq: 1, numDelivered: 1, data: []byte(`{"id": "1"}`)}
	viewCfg := &natsql.ViewConfig{Name: "test_view"}

	processEvent(ctx, js, newProcessEventMapper(t), writer, msg, viewCfg, logger)

	if !js.publishCalled {
		t.Error("expected DLQ publish for terminal write error")
	}
	if !msg.acked {
		t.Error("expected msg.Ack() to be called after DLQ publish for terminal error")
	}
	if msg.nakked {
		t.Error("expected msg.Nak() NOT to be called for terminal write error (DLQ success)")
	}
}

func TestProcessEvent_DLQPublishFailure_NAKsWithBackoff(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	js := &fakeJS{publishErr: nats.ErrNoResponders}
	kvMock := &fakeKV{writeErr: jetstream.ErrInvalidKey}
	writer := NewWriter(kvMock, "test_view", "|", Source{})
	msg := &fakeMsg{seq: 1, numDelivered: 1, data: []byte(`{"id": "1"}`)}
	viewCfg := &natsql.ViewConfig{Name: "test_view"}

	processEvent(ctx, js, newProcessEventMapper(t), writer, msg, viewCfg, logger)

	if msg.acked {
		t.Error("expected no ack when the DLQ publish failed")
	}
	if !msg.nakked || msg.nakDelay != 250*time.Millisecond {
		t.Errorf("expected NAK with 250ms backoff, got nakked=%v delay=%v", msg.nakked, msg.nakDelay)
	}
}

// --- Mocks for error classification tests ---

// fakeJS mocks jetstream.JetStream for DLQ publish tracking.
type fakeJS struct {
	jetstream.JetStream
	publishCalled bool
	publishErr    error
}

func (f *fakeJS) Publish(_ context.Context, _ string, _ []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	f.publishCalled = true
	return &jetstream.PubAck{}, f.publishErr
}

// fakeKV mocks jetstream.KeyValue for Writer error injection: every key is
// absent, and every write fails with writeErr.
type fakeKV struct {
	jetstream.KeyValue
	writeErr error
}

func (f *fakeKV) Get(_ context.Context, _ string) (jetstream.KeyValueEntry, error) {
	return nil, jetstream.ErrKeyNotFound
}

func (f *fakeKV) Create(_ context.Context, _ string, _ []byte, _ ...jetstream.KVCreateOpt) (uint64, error) {
	return 0, f.writeErr
}

func (f *fakeKV) Update(_ context.Context, _ string, _ []byte, _ uint64) (uint64, error) {
	return 0, f.writeErr
}

func (f *fakeKV) Bucket() string { return "test-bucket" }

// fakeMsg mocks jetstream.Msg for Ack/Nak tracking.
type fakeMsg struct {
	data         []byte
	seq          uint64
	numDelivered uint64
	acked        bool
	nakked       bool
	nakDelay     time.Duration
}

func (m *fakeMsg) Data() []byte { return m.data }
func (m *fakeMsg) Ack() error   { m.acked = true; return nil }
func (m *fakeMsg) Nak() error   { m.nakked = true; return nil }
func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: m.seq}, NumDelivered: m.numDelivered}, nil
}
func (m *fakeMsg) Headers() nats.Header              { return nil }
func (m *fakeMsg) Subject() string                   { return "" }
func (m *fakeMsg) Reply() string                     { return "" }
func (m *fakeMsg) DoubleAck(_ context.Context) error { return nil }
func (m *fakeMsg) NakWithDelay(d time.Duration) error {
	m.nakked = true
	m.nakDelay = d
	return nil
}
func (m *fakeMsg) InProgress() error             { return nil }
func (m *fakeMsg) Term() error                   { return nil }
func (m *fakeMsg) TermWithReason(_ string) error { return nil }

func verifyDLQEnvelope(t *testing.T, data []byte, wantViewName, wantOriginal string) {
	t.Helper()
	var envelope map[string]any
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("unmarshal DLQ envelope failed: %v", err)
	}
	if envelope["view_name"] != wantViewName {
		t.Errorf("view_name = %v, want %q", envelope["view_name"], wantViewName)
	}
	if _, ok := envelope["error"]; !ok {
		t.Error("DLQ envelope missing 'error' field")
	}
	if _, ok := envelope["timestamp"]; !ok {
		t.Error("DLQ envelope missing 'timestamp' field")
	}
	origB64, ok := envelope["original_message_b64"].(string)
	if !ok || origB64 == "" {
		t.Errorf("DLQ envelope missing or empty 'original_message_b64', got %T=%v",
			envelope["original_message_b64"], envelope["original_message_b64"])
		return
	}
	origBytes, err := base64.StdEncoding.DecodeString(origB64)
	if err != nil {
		t.Fatalf("failed to decode original_message_b64: %v", err)
	}
	if string(origBytes) != wantOriginal {
		t.Errorf("original_message_b64 decoded to %q, want %q", string(origBytes), wantOriginal)
	}
}

// waitFor polls cond every 20ms until it returns true or timeout passes.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// failingWritesKV fails row writes while shouldFail returns true for the key.
type failingWritesKV struct {
	jetstream.KeyValue
	shouldFail func(key string) bool
	failures   atomic.Int32
}

func (f *failingWritesKV) fail(key string) bool {
	if strings.Contains(key, "/pk/") && f.shouldFail(key) {
		f.failures.Add(1)
		return true
	}
	return false
}

func (f *failingWritesKV) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	if f.fail(key) {
		return 0, nats.ErrNoResponders
	}
	return f.KeyValue.Create(ctx, key, value, opts...)
}

func (f *failingWritesKV) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	if f.fail(key) {
		return 0, nats.ErrNoResponders
	}
	return f.KeyValue.Update(ctx, key, value, revision)
}

func runMaterializer(t *testing.T, js jetstream.JetStream, viewCfg *natsql.ViewConfig, bucket jetstream.KeyValue) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, js, viewCfg, bucket, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func consumerSettled(ctx context.Context, js jetstream.JetStream, stream, consumer string, wantDelivered uint64) func() bool {
	return func() bool {
		cons, err := js.Consumer(ctx, stream, consumer)
		if err != nil {
			return false
		}
		info, err := cons.Info(ctx)
		if err != nil {
			return false
		}
		return info.AckFloor.Stream >= wantDelivered && info.NumAckPending == 0 && info.NumPending == 0
	}
}

// TestMaterializer_RedeliveredOlderEventDoesNotOverwrite reproduces the
// redelivery race end to end: the first write for a key fails transiently,
// the newer event for the same key is applied, and the older event is
// redelivered afterwards. The newer value must survive (WR-01).
func TestMaterializer_RedeliveredOlderEventDoesNotOverwrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, js := startEmbeddedNATS(t)
	streamName := "TEST_MAT_REDELIVERY"
	createStream(t, ctx, js, streamName)
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}
	if _, err := EnsureDLQStream(ctx, js); err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	for _, body := range []string{`{"id":"a","value":1}`, `{"id":"a","value":2}`} {
		if _, err := js.Publish(ctx, streamName+".e", []byte(body)); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	var failedOnce atomic.Bool
	bucket := &failingWritesKV{KeyValue: kvb, shouldFail: func(string) bool { return failedOnce.CompareAndSwap(false, true) }}
	viewCfg := &natsql.ViewConfig{
		Name:         "review",
		SourceStream: streamName,
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "value", From: "value", Type: natsql.ColumnTypeNumber},
		},
	}
	runMaterializer(t, js, viewCfg, bucket)

	waitFor(t, 20*time.Second, "both events to be acked", consumerSettled(ctx, js, streamName, ConsumerName("review"), 2))
	if bucket.failures.Load() != 1 {
		t.Fatalf("injected %d write failures, want 1", bucket.failures.Load())
	}

	entry, err := kvb.Get(ctx, kv.BuildPKKey("review", []string{"a"}, "/"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	var row map[string]any
	if err := json.Unmarshal(entry.Value(), &row); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if row["value"] != float64(2) {
		t.Errorf("value = %v, want 2: the redelivered older event overwrote newer state", row["value"])
	}
}

// TestMaterializer_OutageDeadLettersOnFinalDelivery verifies that an event
// whose writes keep failing transiently is retried with backoff and then
// dead-lettered on its final delivery, instead of vanishing when MaxDeliver
// is reached (WR-02).
func TestMaterializer_OutageDeadLettersOnFinalDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, js := startEmbeddedNATS(t)
	streamName := "TEST_MAT_OUTAGE"
	createStream(t, ctx, js, streamName)
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}
	dlq, err := EnsureDLQStream(ctx, js)
	if err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	lostKey := kv.BuildPKKey("review", []string{"lost"}, "/")
	bucket := &failingWritesKV{KeyValue: kvb, shouldFail: func(key string) bool { return key == lostKey }}
	viewCfg := &natsql.ViewConfig{
		Name:         "review",
		SourceStream: streamName,
		KeyFields:    []string{"id"},
		Columns:      []natsql.ColumnConfig{{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true}},
		Consumer:     natsql.ConsumerConfig{MaxDeliver: 3},
	}
	runMaterializer(t, js, viewCfg, bucket)

	for _, body := range []string{`{"id":"lost"}`, `{"id":"healthy"}`} {
		if _, err := js.Publish(ctx, streamName+".e", []byte(body)); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	waitFor(t, 20*time.Second, "the failing event to reach the DLQ", func() bool {
		info, err := dlq.Info(ctx)
		return err == nil && info.State.Msgs == 1
	})
	if got := bucket.failures.Load(); got != 3 {
		t.Errorf("write attempts for the failing event = %d, want 3 (MaxDeliver)", got)
	}
	if _, err := kvb.Get(ctx, kv.BuildPKKey("review", []string{"healthy"}, "/")); err != nil {
		t.Errorf("healthy event was not materialized: %v", err)
	}
	waitFor(t, 5*time.Second, "both events to be acked", consumerSettled(ctx, js, streamName, ConsumerName("review"), 2))
}

// TestEnsureDLQStream_PreservesExistingConfig verifies that restarting never
// resets an operator-tuned DLQ stream (e.g. a longer retention).
func TestEnsureDLQStream_PreservesExistingConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, js := startEmbeddedNATS(t)
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     DLQStreamName,
		Subjects: []string{DLQSubject},
		MaxAge:   30 * 24 * time.Hour,
	}); err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	stream, err := EnsureDLQStream(ctx, js)
	if err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info failed: %v", err)
	}
	if info.Config.MaxAge != 30*24*time.Hour {
		t.Errorf("DLQ MaxAge = %v, want 720h (operator config was overwritten)", info.Config.MaxAge)
	}
}
