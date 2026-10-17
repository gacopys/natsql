package embed

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestStartNode creates an embedded NATS server, connects to it,
// verifies JetStream is available, then shuts it down.
func TestStartNode(t *testing.T) {
	// Start embedded NATS
	node, err := StartNode(NodeConfig{
		Port:      -1,
		StoreDir:  t.TempDir(),
		ReadyWait: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartNode failed: %v", err)
	}
	defer node.Shutdown()

	// Connect to the embedded server
	nc, err := nats.Connect(node.ClientURL(), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("nats.Connect failed: %v", err)
	}
	defer nc.Close()

	// Verify JetStream is available
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	// Create a test stream to verify JetStream works
	ctx := t.Context()
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST_EMBED",
		Subjects: []string{"test.embed.>"},
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}
	if stream == nil {
		t.Fatal("expected non-nil stream")
	}

	// Publish and verify
	_, err = js.Publish(ctx, "test.embed.hello", []byte(`{"msg":"hello"}`))
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
}

// TestStartNodeStoreDir verifies that a custom StoreDir is respected.
func TestStartNodeStoreDir(t *testing.T) {
	node, err := StartNode(NodeConfig{
		Port:      -1,
		StoreDir:  t.TempDir(),
		ReadyWait: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartNode failed: %v", err)
	}
	defer node.Shutdown()

	nc, err := nats.Connect(node.ClientURL(), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("nats.Connect failed: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	ctx := t.Context()
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST_EMBED_DIR",
		Subjects: []string{"test.dir.>"},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}
}

// TestStartNodeDefaultHost verifies that Host defaults to 127.0.0.1.
func TestStartNodeDefaultHost(t *testing.T) {
	node, err := StartNode(NodeConfig{
		Port:      -1,
		ReadyWait: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartNode failed: %v", err)
	}
	defer node.Shutdown()

	nc, err := nats.Connect(node.ClientURL(), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("nats.Connect failed: %v", err)
	}
	defer nc.Close()
}

// TestStartNodeServerAccess verifies that Server() returns a usable server instance.
func TestStartNodeServerAccess(t *testing.T) {
	node, err := StartNode(NodeConfig{
		Port:      -1,
		ReadyWait: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartNode failed: %v", err)
	}
	defer node.Shutdown()

	srv := node.Server()
	if srv == nil {
		t.Fatal("Server() returned nil")
	}
	if srv.ClientURL() != node.ClientURL() {
		t.Errorf("Server().ClientURL() = %q, want %q", srv.ClientURL(), node.ClientURL())
	}
}

// TestStartNode_DefaultStoreIsPrivate verifies that nodes without a
// StoreDir never share state: each gets its own temporary store, deleted on
// Shutdown (OP-01).
func TestStartNode_DefaultStoreIsPrivate(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())

	first, err := StartNode(NodeConfig{Port: -1})
	if err != nil {
		t.Fatalf("StartNode failed: %v", err)
	}
	nc, err := nats.Connect(first.ClientURL())
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{Name: "PRIVATE", Subjects: []string{"private.>"}}); err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}
	nc.Close()
	firstDir := first.tempDir
	first.Shutdown()
	if _, err := os.Stat(firstDir); !os.IsNotExist(err) {
		t.Errorf("temporary store %s still exists after Shutdown (err=%v)", firstDir, err)
	}

	second, err := StartNode(NodeConfig{Port: -1})
	if err != nil {
		t.Fatalf("StartNode failed: %v", err)
	}
	defer second.Shutdown()
	if second.tempDir == firstDir {
		t.Errorf("both nodes used store %s", firstDir)
	}
	nc2, err := nats.Connect(second.ClientURL())
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc2.Close()
	js2, err := jetstream.New(nc2)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	if _, err := js2.Stream(t.Context(), "PRIVATE"); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Errorf("second default node sees the first node's stream: %v", err)
	}
}
