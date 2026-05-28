package kv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPkKey(t *testing.T) {
	result := PkKey("users", "abc123")
	expected := "users/pk/abc123"
	if result != expected {
		t.Errorf("PkKey(\"users\", \"abc123\") = %q, want %q", result, expected)
	}
}

func TestPkKey_SpecialChars(t *testing.T) {
	result := PkKey("my-view", "user@example.com")
	expected := "my-view/pk/user@example.com"
	if result != expected {
		t.Errorf("PkKey = %q, want %q", result, expected)
	}
}

func TestSchemaKey(t *testing.T) {
	result := SchemaKey("users")
	expected := "users/meta/schema"
	if result != expected {
		t.Errorf("SchemaKey(\"users\") = %q, want %q", result, expected)
	}
}

func TestEncodePKValue_String(t *testing.T) {
	result := EncodePKValue("hello")
	if result != "hello" {
		t.Errorf("EncodePKValue(\"hello\") = %q, want %q", result, "hello")
	}
}

func TestEncodePKValue_WithSlash_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for string containing '/', got none")
		}
	}()
	EncodePKValue("abc/123")
}

func TestEncodePKValue_WithColon_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for string containing ':', got none")
		}
	}()
	EncodePKValue("abc:123")
}

func TestEncodePKValue_Int(t *testing.T) {
	tests := []struct {
		val  any
		want string
	}{
		{int(42), "42"},
		{int64(123456789), "123456789"},
		{int32(-5), "-5"},
	}
	for _, tt := range tests {
		got := EncodePKValue(tt.val)
		if got != tt.want {
			t.Errorf("EncodePKValue(%v) = %q, want %q", tt.val, got, tt.want)
		}
	}
}

func TestEncodePKValue_Float(t *testing.T) {
	result := EncodePKValue(float64(3.14))
	if result == "" {
		t.Error("EncodePKValue(3.14) returned empty")
	}
}

func TestEncodePKValue_Bool(t *testing.T) {
	if EncodePKValue(true) != "true" {
		t.Error("EncodePKValue(true) should be 'true'")
	}
	if EncodePKValue(false) != "false" {
		t.Error("EncodePKValue(false) should be 'false'")
	}
}

func TestEncodePKValue_Nil(t *testing.T) {
	result := EncodePKValue(nil)
	if result != "" {
		t.Errorf("EncodePKValue(nil) = %q, want empty", result)
	}
}

func TestInitBucket_CreatesBucket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	kv, err := InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}
	if kv == nil {
		t.Fatal("InitBucket returned nil")
	}

	// Verify the bucket exists by reading a non-existent key
	_, err = kv.Get(ctx, "test-key")
	if err != nil {
		// Expected: key not found, but bucket exists
		if !isNATSKeyNotFound(err) {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestStoreAndLoadSchema_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	kv, err := InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	schema := &ViewSchema{
		Name:         "users",
		KeyFields:    []string{"user_id"},
		KeySeparator: "|",
		Version:      1,
		Columns: []ColumnSchema{
			{Name: "user_id", Type: "string", PrimaryKey: true},
			{Name: "name", Type: "string", PrimaryKey: false},
		},
	}

	if err := StoreSchema(ctx, kv, "users", schema); err != nil {
		t.Fatalf("StoreSchema failed: %v", err)
	}

	loaded, err := LoadSchema(ctx, kv, "users")
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("LoadSchema returned nil")
	}

	if loaded.Name != schema.Name {
		t.Errorf("Name: got %q, want %q", loaded.Name, schema.Name)
	}
	if loaded.Version != schema.Version {
		t.Errorf("Version: got %d, want %d", loaded.Version, schema.Version)
	}
	if len(loaded.Columns) != len(schema.Columns) {
		t.Fatalf("Columns: got %d, want %d", len(loaded.Columns), len(schema.Columns))
	}
	if loaded.Columns[0].Name != "user_id" || !loaded.Columns[0].PrimaryKey {
		t.Error("column 0 mismatch")
	}
	if loaded.Columns[1].Name != "name" || loaded.Columns[1].PrimaryKey {
		t.Error("column 1 mismatch")
	}
}

func TestLoadSchema_MissingKey_ReturnsNil(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	kv, err := InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	schema, err := LoadSchema(ctx, kv, "nonexistent_view")
	if err != nil {
		t.Fatalf("LoadSchema for missing key returned error: %v", err)
	}
	if schema != nil {
		t.Fatal("LoadSchema for missing key should return nil, got non-nil")
	}
}

func TestStoreSchema_OverwriteUpdatesSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	kv, err := InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	v1 := &ViewSchema{Name: "users", Version: 1}
	if err := StoreSchema(ctx, kv, "users", v1); err != nil {
		t.Fatalf("StoreSchema v1 failed: %v", err)
	}

	v2 := &ViewSchema{Name: "users", Version: 2}
	if err := StoreSchema(ctx, kv, "users", v2); err != nil {
		t.Fatalf("StoreSchema v2 failed: %v", err)
	}

	loaded, err := LoadSchema(ctx, kv, "users")
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}
	if loaded.Version != 2 {
		t.Errorf("expected version 2, got %d", loaded.Version)
	}
}

func TestMustInitBucket_PanicsOnError(t *testing.T) {
	ctx := context.Background()
	// nil JetStream should cause panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic with nil JetStream, got none")
		}
	}()
	MustInitBucket(ctx, nil, 1)
}

// Helpers

func startEmbeddedNATS(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
	t.Helper()

	opts := &server.Options{
		Port:           -1,
		JetStream:      true,
		StoreDir:       t.TempDir(),
		ServerName:     "test-server",
		NoLog:          true,
		NoSigs:         true,
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

func isNATSKeyNotFound(err error) bool {
	return errors.Is(err, jetstream.ErrKeyNotFound)
}
