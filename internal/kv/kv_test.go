package kv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql/internal/testutil"
)

func TestSchemaKey(t *testing.T) {
	result := SchemaKey("users")
	expected := "users/meta/schema"
	if result != expected {
		t.Errorf("SchemaKey(\"users\") = %q, want %q", result, expected)
	}
}

func TestInitBucket_CreatesBucket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, _, js := startEmbeddedNATS(t)

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

	_, _, js := startEmbeddedNATS(t)

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

	_, _, js := startEmbeddedNATS(t)

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

	_, _, js := startEmbeddedNATS(t)

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

func TestSanitizePK(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"abc", "abc"},
		{"a|b", "a_pb"},
		{"a/b", "a_sb"},
		{"a*b", "a_ab"},
		{"a>b", "a_gb"},
		{"a_b", "a__b"},
		{"a|/", "a_p_s"},
	}
	for _, tc := range tests {
		got := SanitizePK(tc.input)
		if got != tc.want {
			t.Errorf("SanitizePK(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestPkKey_Sanitization(t *testing.T) {
	got := PkKey("view", "a|b/c*d>e_f")
	want := "view/pk/a_pb_sc_ad_ge__f"
	if got != want {
		t.Errorf("PkKey = %q, want %q", got, want)
	}
}

func TestBuildPkKey(t *testing.T) {
	tests := []struct {
		name      string
		viewName  string
		pkParts   []string
		separator string
		want      string
	}{
		{"single part", "users", []string{"abc"}, "|", "users/pk/abc"},
		{"composite", "users", []string{"a", "b"}, "|", "users/pk/a|b"},
		{"underscore", "users", []string{"a_b"}, "|", "users/pk/a__b"},
		{"pipe", "users", []string{"a|b"}, "|", "users/pk/a_pb"},
		{"slash", "users", []string{"a/b"}, "|", "users/pk/a_sb"},
		{"star", "users", []string{"a*b"}, "|", "users/pk/a_ab"},
		{"greater", "users", []string{"a>b"}, "|", "users/pk/a_gb"},
		{"double underscore", "users", []string{"a__b"}, "|", "users/pk/a____b"},
		{"custom sep slash", "users", []string{"hello", "world"}, "/", "users/pk/hello/world"},
		{"custom sep colon", "users", []string{"a", "b"}, ":", "users/pk/a:b"},
		{"empty parts", "users", []string{""}, "|", "users/pk/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildPkKey(tt.viewName, tt.pkParts, tt.separator)
			if got != tt.want {
				t.Errorf("BuildPkKey(%q, %v, %q) = %q, want %q", tt.viewName, tt.pkParts, tt.separator, got, tt.want)
			}
		})
	}
}

func TestPkKey_BackwardCompat(t *testing.T) {
	result := PkKey("users", "abc123")
	expected := "users/pk/abc123"
	if result != expected {
		t.Errorf("PkKey backward compat: got %q, want %q", result, expected)
	}
}

// Helpers

func startEmbeddedNATS(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
	t.Helper()
	nc, js := testutil.StartEmbeddedNATS(t)
	return nil, nc, js
}

func isNATSKeyNotFound(err error) bool {
	return errors.Is(err, jetstream.ErrKeyNotFound)
}
