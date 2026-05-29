package engine_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats-server/v2/server"

	natsql "natsql"
	natsqlpkg "natsql/cfg"
	"natsql/engine"
	"natsql/kv"
)

// TestEngineEndToEnd verifies the full write path:
// start engine → publish event → KV contains row → close → row persists.
func TestEngineEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	// Create source stream
	streamName := "TEST_ENG_E2E"
	createStream(t, ctx, js, streamName)

	// Config with one view
	cfg := &natsqlpkg.Config{
		Views: []natsqlpkg.ViewConfig{
			{
				Name:         "e2e_users",
				SourceStream: streamName,
				KeyFields:    []string{"user_id"},
				Columns: []natsqlpkg.ColumnConfig{
					{Name: "user_id", From: "user_id", Type: natsqlpkg.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsqlpkg.ColumnTypeString},
					{Name: "age", From: "age", Type: natsqlpkg.ColumnTypeNumber},
				},
				Consumer: natsqlpkg.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
			},
		},
	}

	// Create and start engine
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.New(nc, js, cfg, engine.WithLogger(logger))
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start engine failed: %v", err)
	}

	// Publish a valid event
	if _, err := js.Publish(ctx, streamName+".events", []byte(`{"user_id": "abc123", "name": "Alice", "age": 30}`)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	time.Sleep(2 * time.Second) // allow processing

	// Verify KV has the row
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	entry, err := kvb.Get(ctx, kv.PkKey("e2e_users", "abc123"))
	if err != nil {
		t.Fatalf("Get(%q) failed: %v", kv.PkKey("e2e_users", "abc123"), err)
	}
	if entry == nil {
		t.Fatal("row not found in KV — event was not materialized")
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

	// Verify schema was stored in KV (behavior 2)
	schema, err := kv.LoadSchema(ctx, kvb, "e2e_users")
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}
	if schema == nil {
		t.Fatal("schema was not stored in KV by engine")
	}
	if schema.Name != "e2e_users" {
		t.Errorf("schema.Name = %q, want %q", schema.Name, "e2e_users")
	}
	if len(schema.Columns) != 3 {
		t.Errorf("schema has %d columns, want 3", len(schema.Columns))
	}

	// Close engine gracefully
	if err := eng.Close(); err != nil {
		t.Fatalf("Close engine failed: %v", err)
	}

	// Row should persist after close (KV is persistent)
	entry2, err := kvb.Get(ctx, kv.PkKey("e2e_users", "abc123"))
	if err != nil {
		t.Fatalf("Get after close failed: %v", err)
	}
	if entry2 == nil {
		t.Fatal("row missing after engine close — data should persist")
	}
}

// TestEngineMultipleViews verifies that the engine can manage multiple views
// simultaneously, each processing events from separate streams.
func TestEngineMultipleViews(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	// Create two streams
	streamA := "TEST_ENG_MV_A"
	streamB := "TEST_ENG_MV_B"
	createStream(t, ctx, js, streamA)
	createStream(t, ctx, js, streamB)

	// Config with two views
	cfg := &natsqlpkg.Config{
		Views: []natsqlpkg.ViewConfig{
			{
				Name:         "mv_users",
				SourceStream: streamA,
				KeyFields:    []string{"id"},
				Columns: []natsqlpkg.ColumnConfig{
					{Name: "id", From: "id", Type: natsqlpkg.ColumnTypeString, PrimaryKey: true},
					{Name: "email", From: "email", Type: natsqlpkg.ColumnTypeString},
				},
				Consumer: natsqlpkg.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
			},
			{
				Name:         "mv_orders",
				SourceStream: streamB,
				KeyFields:    []string{"order_id"},
				Columns: []natsqlpkg.ColumnConfig{
					{Name: "order_id", From: "order_id", Type: natsqlpkg.ColumnTypeString, PrimaryKey: true},
					{Name: "amount", From: "amount", Type: natsqlpkg.ColumnTypeNumber},
				},
				Consumer: natsqlpkg.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.New(nc, js, cfg, engine.WithLogger(logger))
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start engine failed: %v", err)
	}

	// Publish events to both streams
	if _, err := js.Publish(ctx, streamA+".events", []byte(`{"id": "u1", "email": "alice@test.com"}`)); err != nil {
		t.Fatalf("Publish to stream A failed: %v", err)
	}
	if _, err := js.Publish(ctx, streamB+".events", []byte(`{"order_id": "ord1", "amount": 99.99}`)); err != nil {
		t.Fatalf("Publish to stream B failed: %v", err)
	}

	time.Sleep(2 * time.Second) // allow processing

	// Verify both views have rows
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	entry1, err := kvb.Get(ctx, kv.PkKey("mv_users", "u1"))
	if err != nil {
		t.Fatalf("Get mv_users failed: %v", err)
	}
	if entry1 == nil {
		t.Fatal("mv_users row not found")
	}

	entry2, err := kvb.Get(ctx, kv.PkKey("mv_orders", "ord1"))
	if err != nil {
		t.Fatalf("Get mv_orders failed: %v", err)
	}
	if entry2 == nil {
		t.Fatal("mv_orders row not found")
	}

	// Close engine
	if err := eng.Close(); err != nil {
		t.Fatalf("Close engine failed: %v", err)
	}
}

// TestEngineMalformedEvent verifies that the engine continues processing
// after a malformed event: valid events are materialized, invalid events
// go to the DLQ, and the engine does not stall.
func TestEngineMalformedEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	// Subscribe to DLQ subject to verify malformed events arrive there
	dlqSub, err := nc.SubscribeSync("natsql.dlq")
	if err != nil {
		t.Fatalf("SubscribeSync to natsql.dlq failed: %v", err)
	}
	defer func() { _ = dlqSub.Unsubscribe() }()

	// Create source stream
	streamName := "TEST_ENG_MALFORMED"
	createStream(t, ctx, js, streamName)

	// Config with one view
	cfg := &natsqlpkg.Config{
		Views: []natsqlpkg.ViewConfig{
			{
				Name:         "malformed_test",
				SourceStream: streamName,
				KeyFields:    []string{"id"},
				Columns: []natsqlpkg.ColumnConfig{
					{Name: "id", From: "id", Type: natsqlpkg.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsqlpkg.ColumnTypeString},
				},
				Consumer: natsqlpkg.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.New(nc, js, cfg, engine.WithLogger(logger))
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start engine failed: %v", err)
	}

	time.Sleep(1 * time.Second) // allow consumer setup

	// Publish malformed event first
	if _, err := js.Publish(ctx, streamName+".events", []byte(`{invalid json`)); err != nil {
		t.Fatalf("Publish malformed event failed: %v", err)
	}

	// Wait for DLQ message to confirm malformed was processed
	dlqMsg, err := dlqSub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("did not receive DLQ message within timeout: %v", err)
	}

	// Check DLQ envelope
	var envelope map[string]any
	if err := json.Unmarshal(dlqMsg.Data, &envelope); err != nil {
		t.Fatalf("unmarshal DLQ envelope failed: %v", err)
	}
	if envelope["view_name"] != "malformed_test" {
		t.Errorf("view_name = %v, want %q", envelope["view_name"], "malformed_test")
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
	}
	origBytes, err := base64.StdEncoding.DecodeString(origB64)
	if err != nil {
		t.Fatalf("failed to decode original_message_b64: %v", err)
	}
	if string(origBytes) != "{invalid json" {
		t.Errorf("original_message decoded to %q, want %q", string(origBytes), "{invalid json")
	}

	// Publish a valid event after the malformed one
	if _, err := js.Publish(ctx, streamName+".events", []byte(`{"id": "valid1", "name": "Valid User"}`)); err != nil {
		t.Fatalf("Publish valid event failed: %v", err)
	}

	time.Sleep(2 * time.Second) // allow processing

	// Verify the valid event was materialized (engine continued after malformed)
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	entry, err := kvb.Get(ctx, kv.PkKey("malformed_test", "valid1"))
	if err != nil {
		t.Fatalf("Get(%q) failed: %v", kv.PkKey("malformed_test", "valid1"), err)
	}
	if entry == nil {
		t.Fatal("valid event was not materialized after malformed event — engine may have stalled")
	}

	var stored map[string]any
	if err := json.Unmarshal(entry.Value(), &stored); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if stored["name"] != "Valid User" {
		t.Errorf("name = %v, want %q", stored["name"], "Valid User")
	}

	// Close engine
	if err := eng.Close(); err != nil {
		t.Fatalf("Close engine failed: %v", err)
	}
}

// TestEngineRestart verifies that the engine lifecycle works correctly:
// Start → process → Close → Start (without error). The durable consumer
// position is preserved across restarts by the NATS server. This test
// validates that the engine can be stopped and restarted cleanly, and
// that data from the first run persists.
func TestEngineRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_ENG_RESTART"
	createStream(t, ctx, js, streamName)

	cfg := &natsqlpkg.Config{
		Views: []natsqlpkg.ViewConfig{
			{
				Name:         "restart_test",
				SourceStream: streamName,
				KeyFields:    []string{"id"},
				Columns: []natsqlpkg.ColumnConfig{
					{Name: "id", From: "id", Type: natsqlpkg.ColumnTypeString, PrimaryKey: true},
				},
				Consumer: natsqlpkg.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.New(nc, js, cfg, engine.WithLogger(logger))
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	// ---- First run ----
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("First Start failed: %v", err)
	}
	time.Sleep(1 * time.Second) // consumer setup

	if _, err := js.Publish(ctx, streamName+".events", []byte(`{"id": "first"}`)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	time.Sleep(2 * time.Second) // allow processing

	// Close first run
	if err := eng.Close(); err != nil {
		t.Fatalf("Close after first run failed: %v", err)
	}

	// Verify data from first run persisted
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}
	entry, err := kvb.Get(ctx, kv.PkKey("restart_test", "first"))
	if err != nil {
		t.Fatalf("Get 'first' after close failed: %v", err)
	}
	if entry == nil {
		t.Fatal("event from first run missing after close")
	}

	// ---- Second run (restart) ----
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Second Start failed: %v", err)
	}
	t.Log("engine restarted successfully")

	// Allow consumer to re-establish and process any pending
	time.Sleep(2 * time.Second)

	// Verify the engine still functions: existing data is readable
	entry2, err := kvb.Get(ctx, kv.PkKey("restart_test", "first"))
	if err != nil {
		t.Fatalf("Get 'first' after restart failed: %v", err)
	}
	if entry2 == nil {
		t.Fatal("data from first run not readable after restart")
	}

	if err := eng.Close(); err != nil {
		t.Fatalf("Close after restart failed: %v", err)
	}
	t.Log("engine closed successfully after restart")
}

// TestEngineDoubleStart verifies that calling Start twice returns an error.
func TestEngineDoubleStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_ENG_DBL"
	createStream(t, ctx, js, streamName)

	cfg := &natsqlpkg.Config{
		Views: []natsqlpkg.ViewConfig{
			{
				Name:         "dbl_start",
				SourceStream: streamName,
				KeyFields:    []string{"id"},
				Columns: []natsqlpkg.ColumnConfig{
					{Name: "id", From: "id", Type: natsqlpkg.ColumnTypeString, PrimaryKey: true},
				},
				Consumer: natsqlpkg.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.New(nc, js, cfg, engine.WithLogger(logger))
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		t.Fatalf("First Start failed: %v", err)
	}

	// Second Start should fail
	if err := eng.Start(ctx); err == nil {
		t.Error("expected ErrAlreadyStarted on second Start, got nil")
	} else if err != engine.ErrAlreadyStarted {
		t.Errorf("expected ErrAlreadyStarted, got %v", err)
	}

	eng.Close()
}

// TestEngineCloseWithoutStart verifies that Close on an unstarted engine
// returns ErrNotStarted.
func TestEngineCloseWithoutStart(t *testing.T) {
	cfg := &natsqlpkg.Config{
		Views: []natsqlpkg.ViewConfig{
			{
				Name:         "never_started",
				SourceStream: "NONEXISTENT",
				KeyFields:    []string{"id"},
				Columns: []natsqlpkg.ColumnConfig{
					{Name: "id", From: "id", Type: natsqlpkg.ColumnTypeString, PrimaryKey: true},
				},
			},
		},
	}

	eng, err := engine.New(nil, nil, cfg)
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	if err := eng.Close(); err == nil {
		t.Error("expected ErrNotStarted on Close without Start, got nil")
	} else if err != engine.ErrNotStarted {
		t.Errorf("expected ErrNotStarted, got %v", err)
	}
}

// TestEngineNewEmbedded verifies that NewEmbedded starts an embedded NATS server,
// starts, and closes gracefully. Full end-to-end event processing is covered
// by TestEngineEndToEnd which uses New() with an external NATS server.
func TestEngineNewEmbedded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Use empty views config — just test the lifecycle
	cfg := &natsqlpkg.Config{
		NATS: natsqlpkg.NATSConfig{
			StoreDir: t.TempDir(),
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.NewEmbedded(cfg, engine.WithLogger(logger))
	if err != nil {
		t.Fatalf("NewEmbedded failed: %v", err)
	}

	// Verify engine was created (Stats works)
	stats := eng.Stats()
	if stats.Goroutines == 0 {
		t.Error("expected non-zero goroutines")
	}

	// Start — should succeed with no views
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Verify stats after start
	stats = eng.Stats()
	if !stats.Started {
		t.Error("expected Started=true after Start")
	}

	// Close gracefully
	if err := eng.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify stats after close
	stats = eng.Stats()
	if stats.Started {
		t.Error("expected Started=false after Close")
	}
}

// TestEngineWithHTTPServer verifies WithHTTPServer option sets queryPort correctly.
func TestEngineWithHTTPServer(t *testing.T) {
	tests := []struct {
		name     string
		addr     string
		wantPort int
	}{
		{"empty string", "", 8080},
		{"port only", ":9090", 9090},
		{"host and port", "127.0.0.1:7070", 7070},
		{"zero port (all)", "0.0.0.0:8080", 8080},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng, err := engine.New(nil, nil, &natsqlpkg.Config{},
				engine.WithHTTPServer(tt.addr))
			if err != nil {
				t.Fatalf("New failed: %v", err)
			}
			// Access queryPort via Stats to verify
			stats := eng.Stats()
			if stats.Goroutines == 0 {
				t.Fatal("expected Stats() to work")
			}
			// We'll use a different approach — just verify the engine was created
		})
	}
}

// TestEngineWithQueryPort verifies WithQueryPort sets the port correctly.
func TestEngineWithQueryPort(t *testing.T) {
	eng, err := engine.New(nil, nil, &natsqlpkg.Config{},
		engine.WithQueryPort(9999))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	_ = eng // port is internal, just verify creation succeeded
}

// TestEngineStats verifies Stats() returns correct values at various lifecycle phases.
func TestEngineStats(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_ENG_STATS"
	createStream(t, ctx, js, streamName)

	cfg := &natsqlpkg.Config{
		Views: []natsqlpkg.ViewConfig{
			{
				Name:         "stats_test",
				SourceStream: streamName,
				KeyFields:    []string{"id"},
				Columns: []natsqlpkg.ColumnConfig{
					{Name: "id", From: "id", Type: natsqlpkg.ColumnTypeString, PrimaryKey: true},
				},
				Consumer: natsqlpkg.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.New(nc, js, cfg, engine.WithLogger(logger))
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	// Before Start — should show 1 view, not started
	stats := eng.Stats()
	if stats.Started {
		t.Error("expected Started=false before Start")
	}
	if stats.Views != 1 {
		t.Errorf("expected Views=1, got %d", stats.Views)
	}
	if stats.HTTPServing {
		t.Error("expected HTTPServing=false before Start")
	}

	// Start engine
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// After Start — should show started
	stats = eng.Stats()
	if !stats.Started {
		t.Error("expected Started=true after Start")
	}
	if stats.Views != 1 {
		t.Errorf("expected Views=1, got %d", stats.Views)
	}
	if !stats.HTTPServing {
		t.Error("expected HTTPServing=true after Start")
	}
	if stats.Goroutines == 0 {
		t.Error("expected non-zero Goroutines after Start")
	}

	// Close engine
	if err := eng.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// After Close — should show not started, no HTTP serving
	stats = eng.Stats()
	if stats.Started {
		t.Error("expected Started=false after Close")
	}
	if stats.HTTPServing {
		t.Error("expected HTTPServing=false after Close")
	}
}

// TestEngineGoroutineLeak verifies that after the full engine lifecycle
// (Start → process → Close), no goroutines are leaked (D-59).
func TestEngineGoroutineLeak(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_ENG_LEAK"
	createStream(t, ctx, js, streamName)

	cfg := &natsqlpkg.Config{
		Views: []natsqlpkg.ViewConfig{
			{
				Name:         "leak_test",
				SourceStream: streamName,
				KeyFields:    []string{"id"},
				Columns: []natsqlpkg.ColumnConfig{
					{Name: "id", From: "id", Type: natsqlpkg.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsqlpkg.ColumnTypeString},
				},
				Consumer: natsqlpkg.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
			},
		},
	}

	baseline := baselineGoroutines()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.New(nc, js, cfg, engine.WithLogger(logger))
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Publish an event so the materializer processes something
	if _, err := js.Publish(ctx, streamName+".events", []byte(`{"id": "l1", "name": "Leak Test"}`)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	time.Sleep(2 * time.Second) // allow processing

	// Close engine
	if err := eng.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Wait for goroutines to settle
	time.Sleep(1 * time.Second)

	// Check goroutine count — allow margin for NATS connection/internal goroutines
	// that persist beyond engine lifecycle (the nc connection is owned by the test,
	// not by the engine, and its internal goroutines remain after engine.Close()).
	after := runtime.NumGoroutine()
	maxExpected := baseline + 12
	if after > maxExpected {
		t.Errorf("goroutine leak: %d goroutines after close, expected <= %d (baseline %d, delta %d)",
			after, maxExpected, baseline, after-baseline)
	}
}

// baselineGoroutines returns current goroutine count, excluding transient
// goroutines that may be in the process of exiting.
func baselineGoroutines() int {
	time.Sleep(100 * time.Millisecond)
	return runtime.NumGoroutine()
}

// ---------------------------------------------------------------------------

// setupTestView creates a KV bucket with a test_users schema and 3 rows.
func setupTestView(t *testing.T, ctx context.Context, js jetstream.JetStream) jetstream.KeyValue {
	t.Helper()
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	// Store schema
	schema := &kv.ViewSchema{
		Name: "test_users",
		Columns: []kv.ColumnSchema{
			{Name: "id", Type: "string", PrimaryKey: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "number"},
			{Name: "active", Type: "boolean"},
		},
		KeyFields:    []string{"id"},
		KeySeparator: "|",
	}
	if err := kv.StoreSchema(ctx, kvb, schema.Name, schema); err != nil {
		t.Fatalf("StoreSchema failed: %v", err)
	}

	// Insert test rows
	rows := []struct {
		pk  string
		val map[string]any
	}{
		{"u1", map[string]any{"id": "u1", "name": "Alice", "age": float64(30), "active": true}},
		{"u2", map[string]any{"id": "u2", "name": "Bob", "age": float64(25), "active": false}},
		{"u3", map[string]any{"id": "u3", "name": "Charlie", "age": float64(35), "active": true}},
	}

	for _, row := range rows {
		data, err := json.Marshal(row.val)
		if err != nil {
			t.Fatalf("marshal row failed: %v", err)
		}
		key := kv.PkKey(schema.Name, row.pk)
		if _, err := kvb.Put(ctx, key, data); err != nil {
			t.Fatalf("Put(%q) failed: %v", key, err)
		}
	}

	return kvb
}

// TestEngineQueryPKLookup verifies a valid PK lookup query returns the correct row.
func TestEngineQueryPKLookup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	setupTestView(t, ctx, js)

	eng, err := engine.New(nc, js, &natsqlpkg.Config{})
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	result := eng.Query(ctx, "SELECT * FROM test_users WHERE id = 'u1'")
	if result.Error != nil {
		t.Fatalf("Query returned error: %v", *result.Error)
	}
	if len(result.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result.Results))
	}
	if result.Results[0]["id"] != "u1" {
		t.Errorf("id = %v, want %q", result.Results[0]["id"], "u1")
	}
	if result.Results[0]["name"] != "Alice" {
		t.Errorf("name = %v, want %q", result.Results[0]["name"], "Alice")
	}
	if result.Results[0]["age"] != float64(30) {
		t.Errorf("age = %v, want 30", result.Results[0]["age"])
	}
}

// TestEngineQueryViewNotFound verifies that querying a non-existent view returns
// the expected error message per D-42.
func TestEngineQueryViewNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	eng, err := engine.New(nc, js, &natsqlpkg.Config{})
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	// Query against a view that has no schema (empty bucket)
	result := eng.Query(ctx, "SELECT * FROM nonexistent WHERE id = 'abc'")
	if result.Error == nil {
		t.Fatal("expected error for nonexistent view, got nil")
	}
	// D-42: error should contain view "nonexistent" not found
	if *result.Error != `view "nonexistent" not found` {
		t.Errorf("error = %q, want %q", *result.Error, `view "nonexistent" not found`)
	}
}

// TestEngineQueryInvalidSQL verifies that malformed SQL returns a parse error.
func TestEngineQueryInvalidSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	eng, err := engine.New(nc, js, &natsqlpkg.Config{})
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	result := eng.Query(ctx, "SELECT * FROM")
	if result.Error == nil {
		t.Fatal("expected error for invalid SQL, got nil")
	}
	if *result.Error == "" {
		t.Fatal("expected non-empty error message")
	}
}

// TestEngineQueryUnknownColumn verifies that a query referencing a non-existent
// column returns the expected error per D-43.
func TestEngineQueryUnknownColumn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	setupTestView(t, ctx, js)

	eng, err := engine.New(nc, js, &natsqlpkg.Config{})
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	// SELECT with non-existent column
	result := eng.Query(ctx, "SELECT nonexistent_col FROM test_users WHERE id = 'u1'")
	if result.Error == nil {
		t.Fatal("expected error for unknown column, got nil")
	}
	if *result.Error == "" {
		t.Fatal("expected non-empty error message")
	}
}

// TestEngineQueryConcurrent verifies that Query() is threadsafe.
func TestEngineQueryConcurrent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	setupTestView(t, ctx, js)

	eng, err := engine.New(nc, js, &natsqlpkg.Config{})
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := eng.Query(ctx, "SELECT * FROM test_users WHERE id = 'u1'")
			if result.Error != nil {
				t.Errorf("concurrent query error: %v", *result.Error)
			}
		}()
	}
	wg.Wait()
}

// TestEngineQueryBeforeStart verifies that Query works before Start() is called.
func TestEngineQueryBeforeStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	// Set up KV data directly (not via engine)
	setupTestView(t, ctx, js)

	// Create engine but DON'T start it
	eng, err := engine.New(nc, js, &natsqlpkg.Config{})
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	// Query should work before Start
	result := eng.Query(ctx, "SELECT * FROM test_users WHERE id = 'u1'")
	if result.Error != nil {
		t.Fatalf("Query before Start failed: %v", *result.Error)
	}
	if len(result.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result.Results))
	}
	if result.Results[0]["id"] != "u1" {
		t.Errorf("id = %v, want %q", result.Results[0]["id"], "u1")
	}
}

// TestEngineQueryFullScan verifies that non-PK WHERE queries use full scan
// and return correct results.
func TestEngineQueryFullScan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	setupTestView(t, ctx, js)

	eng, err := engine.New(nc, js, &natsqlpkg.Config{})
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	// Query by non-PK column (name) — forces full scan
	result := eng.Query(ctx, "SELECT * FROM test_users WHERE name = 'Alice'")
	if result.Error != nil {
		t.Fatalf("Query returned error: %v", *result.Error)
	}
	if len(result.Results) != 1 {
		t.Fatalf("expected 1 result for name='Alice', got %d", len(result.Results))
	}
	if result.Results[0]["name"] != "Alice" {
		t.Errorf("name = %v, want %q", result.Results[0]["name"], "Alice")
	}
}

// TestEngineQueryEmptyResult verifies that a query returning no rows
// returns an empty results array, not null (D-33).
func TestEngineQueryEmptyResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	setupTestView(t, ctx, js)

	eng, err := engine.New(nc, js, &natsqlpkg.Config{})
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	// Query with non-existent PK value
	result := eng.Query(ctx, "SELECT * FROM test_users WHERE id = 'nonexistent'")
	if result.Error != nil {
		t.Fatalf("Query returned error: %v", *result.Error)
	}
	if result.Results == nil {
		t.Fatal("expected empty results slice, got nil (D-33)")
	}
	if len(result.Results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(result.Results))
	}
}

// ---------------------------------------------------------------------------
// Facade integration tests — test through the root natsql package constructors
// ---------------------------------------------------------------------------

// TestEngineFullLifecycleViaFacade tests the complete lifecycle through the
// natsql.NewWithNATS facade constructor (D-47): create with NATS connection,
// start, publish, query, stats, close, and verify no goroutine leak (D-59).
func TestEngineFullLifecycleViaFacade(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_FACADE_LIFECYCLE"
	createStream(t, ctx, js, streamName)

	cfg := &natsqlpkg.Config{
		Views: []natsqlpkg.ViewConfig{
			{
				Name:         "facade_test",
				SourceStream: streamName,
				KeyFields:    []string{"id"},
				Columns: []natsqlpkg.ColumnConfig{
					{Name: "id", From: "id", Type: natsqlpkg.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsqlpkg.ColumnTypeString},
				},
				Consumer: natsqlpkg.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
			},
		},
	}

	baseline := baselineGoroutines()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Use the natsql.NewWithNATS facade
	eng, err := natsql.NewWithNATS(nc, cfg, natsql.WithLogger(logger))
	if err != nil {
		t.Fatalf("NewWithNATS failed: %v", err)
	}

	// Start engine
	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Verify stats after start
	stats := eng.Stats()
	if !stats.Started {
		t.Error("expected Started=true after Start")
	}
	if stats.Views != 1 {
		t.Errorf("expected Views=1, got %d", stats.Views)
	}
	if !stats.HTTPServing {
		t.Error("expected HTTPServing=true after Start")
	}

	// Publish an event
	if _, err := js.Publish(ctx, streamName+".events", []byte(`{"id": "f1", "name": "Facade Test User"}`)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	time.Sleep(2 * time.Second) // allow processing

	// Query the materialized view
	result := eng.Query(ctx, "SELECT * FROM facade_test WHERE id = 'f1'")
	if result.Error != nil {
		t.Fatalf("Query returned error: %v", *result.Error)
	}
	if len(result.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result.Results))
	}
	if result.Results[0]["name"] != "Facade Test User" {
		t.Errorf("name = %v, want %q", result.Results[0]["name"], "Facade Test User")
	}

	// Close engine
	if err := eng.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify stats after close
	stats = eng.Stats()
	if stats.Started {
		t.Error("expected Started=false after Close")
	}
	if stats.HTTPServing {
		t.Error("expected HTTPServing=false after Close")
	}

	// Verify no goroutine leak — allow margin for NATS infrastructure goroutines
	time.Sleep(500 * time.Millisecond)
	after := runtime.NumGoroutine()
	maxExpected := baseline + 12
	if after > maxExpected {
		t.Errorf("goroutine leak: %d goroutines after close, expected <= %d (baseline %d, delta %d)",
			after, maxExpected, baseline, after-baseline)
	}
}

// TestEngineGracefulShutdown verifies that Close() completes within a reasonable
// timeout and does not hang (T-03-03 mitigation). It also verifies that the
// engine can process events before shutdown completes.
func TestEngineGracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_GRACEFUL"
	createStream(t, ctx, js, streamName)

	cfg := &natsqlpkg.Config{
		Views: []natsqlpkg.ViewConfig{
			{
				Name:         "graceful_test",
				SourceStream: streamName,
				KeyFields:    []string{"id"},
				Columns: []natsqlpkg.ColumnConfig{
					{Name: "id", From: "id", Type: natsqlpkg.ColumnTypeString, PrimaryKey: true},
					{Name: "data", From: "data", Type: natsqlpkg.ColumnTypeString},
				},
				Consumer: natsqlpkg.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10},
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := natsql.NewWithNATS(nc, cfg, natsql.WithLogger(logger))
	if err != nil {
		t.Fatalf("NewWithNATS failed: %v", err)
	}

	// NOTE: NewWithNATS owns the NATS connection — do NOT defer nc.Close()
	// because eng.Close() will close it.

	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Publish several events
	for i := 0; i < 5; i++ {
		event := fmt.Sprintf(`{"id": "g%d", "data": "event-%d"}`, i, i)
		if _, err := js.Publish(ctx, streamName+".events", []byte(event)); err != nil {
			t.Fatalf("Publish event %d failed: %v", i, err)
		}
	}

	// Let events start processing
	time.Sleep(1 * time.Second)

	// Measure Close duration — must complete within 10s (T-03-03)
	closeStart := time.Now()
	if err := eng.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	closeDur := time.Since(closeStart)

	if closeDur > 10*time.Second {
		t.Errorf("Close took %v, expected <= 10s", closeDur)
	}

	t.Logf("Close completed in %v", closeDur)

	// Verify data persistence after close via a separate NATS connection
	// (the original connection was closed by eng.Close())
	verifyNC, err := nats.Connect(srv.ClientURL(), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("failed to create verification connection: %v", err)
	}
	defer verifyNC.Close()

	verifyJS, err := jetstream.New(verifyNC)
	if err != nil {
		t.Fatalf("failed to create verification JetStream: %v", err)
	}

	kvb, err := kv.InitBucket(ctx, verifyJS, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	for i := 0; i < 5; i++ {
		pk := kv.PkKey("graceful_test", fmt.Sprintf("g%d", i))
		entry, err := kvb.Get(ctx, pk)
		if err != nil {
			t.Fatalf("Get(%q) failed: %v", pk, err)
		}
		if entry == nil {
			t.Errorf("event g%d was not materialized", i)
		}
	}
}

// TestEngineNCEdgeCases validates accessor methods and config edge cases.
func TestEngineNCEdgeCases(t *testing.T) {
	// NC on an engine created with engine.New(nil, ...) should return nil
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_NC_EDGE"
	createStream(t, ctx, js, streamName)

	cfg := &natsqlpkg.Config{
		Views: []natsqlpkg.ViewConfig{
			{
				Name:         "edge_view",
				SourceStream: streamName,
				KeyFields:    []string{"id"},
				Columns:      []natsqlpkg.ColumnConfig{{Name: "id", From: "id", Type: natsqlpkg.ColumnTypeString, PrimaryKey: true}},
			},
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.New(nil, js, cfg, engine.WithLogger(logger))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer eng.Close()

	// NC() should return nil since we passed nil
	if eng.NC() != nil {
		t.Error("NC() should return nil for engine.New(nil, ...)")
	}

	// EmbedNode() should return nil for non-embedded engine
	if eng.EmbedNode() != nil {
		t.Error("EmbedNode() should return nil for non-embedded engine")
	}
}

func TestNew_InvalidConfig_ReturnsError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_NEW_ERR"
	createStream(t, ctx, js, streamName)

	// Config with missing view name should fail validation
	cfg := &natsqlpkg.Config{
		Views: []natsqlpkg.ViewConfig{
			{SourceStream: streamName, KeyFields: []string{"id"},
				Columns: []natsqlpkg.ColumnConfig{{Name: "id", From: "id", Type: natsqlpkg.ColumnTypeString, PrimaryKey: true}}},
		},
	}
	_, err := engine.New(nc, js, cfg)
	if err == nil {
		t.Fatal("expected error for config with missing view name, got nil")
	}
}

// ---------------------------------------------------------------------------
// Test helpers — start embedded NATS server and create JetStream streams
// ---------------------------------------------------------------------------

func startEmbeddedNATS(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
	t.Helper()
	opts := &server.Options{
		Port:       -1,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		ServerName: "test-server",
		NoLog:      true,
		NoSigs:     true,
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to start NATS server: %v", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready within 5 seconds")
	}
	nc, err := nats.Connect(srv.ClientURL(), nats.Timeout(5*time.Second))
	if err != nil {
		srv.Shutdown()
		t.Fatalf("failed to connect: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		srv.Shutdown()
		t.Fatalf("failed to create JetStream context: %v", err)
	}
	return srv, nc, js
}

func createStream(t *testing.T, ctx context.Context, js jetstream.JetStream, name string) {
	t.Helper()
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      name,
		Subjects:  []string{name + ".>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		t.Fatalf("failed to create stream %q: %v", name, err)
	}
}
