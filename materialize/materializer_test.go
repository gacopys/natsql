package materialize

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	natsql "github.com/gacopys/natsql/cfg"
	"github.com/gacopys/natsql/kv"
)

func TestMaterializer_ValidEventEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
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
	dlqStream, err := EnsureDLQStream(ctx, js)
	if err != nil {
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
		Consumer: natsql.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	// Start materializer in goroutine
	matCtx, matCancel := context.WithCancel(context.Background())
	defer matCancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, dlqStream, logger, nil)
	}()

	time.Sleep(500 * time.Millisecond) // allow consumer setup

	// Publish a valid event
	if _, err := js.Publish(ctx, streamName+".events", []byte(`{"user_id": "abc123", "name": "Alice", "age": 30}`)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	time.Sleep(2 * time.Second) // allow processing

	// Verify KV has the row
	entry, err := kvb.Get(ctx, kv.PkKey("users", "abc123"))
	if err != nil {
		t.Fatalf("Get(%q) failed: %v", kv.PkKey("users", "abc123"), err)
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

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
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
	dlqStream, err := EnsureDLQStream(ctx, js)
	if err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	viewCfg := &natsql.ViewConfig{
		Name:         "dlq_test",
		SourceStream: streamName,
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
		},
		Consumer: natsql.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	matCtx, matCancel := context.WithCancel(context.Background())
	defer matCancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, dlqStream, logger, nil)
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

	// Check envelope
	var envelope map[string]any
	if err := json.Unmarshal(dlqMsg.Data, &envelope); err != nil {
		t.Fatalf("unmarshal DLQ envelope failed: %v", err)
	}

	if envelope["view_name"] != "dlq_test" {
		t.Errorf("view_name = %v, want %q", envelope["view_name"], "dlq_test")
	}
	if _, ok := envelope["error"]; !ok {
		t.Error("DLQ envelope missing 'error' field")
	}
	if _, ok := envelope["timestamp"]; !ok {
		t.Error("DLQ envelope missing 'timestamp' field")
	}
	origB64, ok := envelope["original_message_b64"].(string)
	if !ok || origB64 == "" {
		t.Errorf("DLQ envelope missing or empty 'original_message_b64', got %T=%v", envelope["original_message_b64"], envelope["original_message_b64"])
	}
	// Decode and verify original bytes
	origBytes, err := base64.StdEncoding.DecodeString(origB64)
	if err != nil {
		t.Fatalf("failed to decode original_message_b64: %v", err)
	}
	if string(origBytes) != "{invalid json" {
		t.Errorf("original_message_b64 decoded to %q, want %q", string(origBytes), "{invalid json")
	}

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

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
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

	dlqStream, err := EnsureDLQStream(ctx, js)
	if err != nil {
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
		Consumer: natsql.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	matCtx, matCancel := context.WithCancel(context.Background())
	defer matCancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, dlqStream, logger, nil)
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
	entry, err := kvb.Get(ctx, kv.PkKey("continue_test", "valid1"))
	if err != nil {
		t.Fatalf("Get(%q) failed: %v", kv.PkKey("continue_test", "valid1"), err)
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

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_MAT_CANCEL"
	createStream(t, ctx, js, streamName)

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	dlqStream, err := EnsureDLQStream(ctx, js)
	if err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	viewCfg := &natsql.ViewConfig{
		Name:         "cancel_test",
		SourceStream: streamName,
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
		},
		Consumer: natsql.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	matCtx, matCancel := context.WithCancel(context.Background())

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, dlqStream, logger, nil)
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

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_MAT_SCHEMA"
	createStream(t, ctx, js, streamName)

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	dlqStream, err := EnsureDLQStream(ctx, js)
	if err != nil {
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
		Consumer: natsql.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	matCtx, matCancel := context.WithCancel(context.Background())
	defer matCancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, dlqStream, logger, nil)
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

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
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

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_MAT_NESTED"
	createStream(t, ctx, js, streamName)

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	dlqStream, err := EnsureDLQStream(ctx, js)
	if err != nil {
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
		Consumer: natsql.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	matCtx, matCancel := context.WithCancel(context.Background())
	defer matCancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, dlqStream, logger, nil)
	}()

	time.Sleep(500 * time.Millisecond)

	// Publish event with nested fields
	event := `{"user":{"id":"u42"},"profile":{"name":"Bob"},"stats":{"score":99.5},"flags":{"active":true}}`
	if _, err := js.Publish(ctx, streamName+".events", []byte(event)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Verify row
	entry, err := kvb.Get(ctx, kv.PkKey("nested_test", "u42"))
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

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_MAT_DRAIN"
	createStream(t, ctx, js, streamName)

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	dlqStream, err := EnsureDLQStream(ctx, js)
	if err != nil {
		t.Fatalf("EnsureDLQStream failed: %v", err)
	}

	viewCfg := &natsql.ViewConfig{
		Name:         "drain_test",
		SourceStream: streamName,
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
		},
		Consumer: natsql.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	drainCh := make(chan struct{})
	errCh := make(chan error, 1)

	matCtx, matCancel := context.WithCancel(context.Background())
	defer matCancel()

	go func() {
		errCh <- Run(matCtx, js, viewCfg, kvb, dlqStream, logger, drainCh)
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
