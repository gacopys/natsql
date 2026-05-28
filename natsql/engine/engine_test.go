package engine_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats-server/v2/server"

	natsqlpkg "natsql"
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
	eng, err := engine.New(js, cfg, engine.WithLogger(logger))
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
	eng, err := engine.New(js, cfg, engine.WithLogger(logger))
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
	eng, err := engine.New(js, cfg, engine.WithLogger(logger))
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
	eng, err := engine.New(js, cfg, engine.WithLogger(logger))
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
	eng, err := engine.New(js, cfg, engine.WithLogger(logger))
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

	eng, err := engine.New(nil, cfg)
	if err != nil {
		t.Fatalf("New engine failed: %v", err)
	}

	if err := eng.Close(); err == nil {
		t.Error("expected ErrNotStarted on Close without Start, got nil")
	} else if err != engine.ErrNotStarted {
		t.Errorf("expected ErrNotStarted, got %v", err)
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
