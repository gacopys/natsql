package materialize

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	natsql "github.com/gacopys/natsql/internal/cfg"
)

func TestConsumerName(t *testing.T) {
	name := ConsumerName("users")
	expected := "natsql-users"
	if name != expected {
		t.Errorf("ConsumerName(\"users\") = %q, want %q", name, expected)
	}
}

func TestSetupConsumer_CreatesDurableConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_STREAM"
	createStream(t, ctx, js, streamName)

	cfg := natsql.ConsumerConfig{
		BatchSize:      10,
		MaxDeliver:     5,
		AckWaitSeconds: 10,
	}
	viewName := "users"

	cons, err := SetupConsumer(ctx, js, streamName, viewName, "", cfg)
	if err != nil {
		t.Fatalf("SetupConsumer failed: %v", err)
	}
	if cons == nil {
		t.Fatal("SetupConsumer returned nil consumer")
	}

	// Verify consumer exists by checking cached info
	info := cons.CachedInfo()
	if info == nil {
		t.Fatal("CachedInfo returned nil")
	}
	if info.Name != ConsumerName(viewName) {
		t.Errorf("consumer name = %q, want %q", info.Name, ConsumerName(viewName))
	}
}

func TestSetupConsumer_ResumesExistingConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_STREAM_RESUME"
	createStream(t, ctx, js, streamName)

	cfg := natsql.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10}

	// First call: creates consumer
	cons1, err := SetupConsumer(ctx, js, streamName, "resume-view", "", cfg)
	if err != nil {
		t.Fatalf("First SetupConsumer failed: %v", err)
	}
	if cons1 == nil {
		t.Fatal("First SetupConsumer returned nil")
	}

	// Second call: should succeed (idempotent - resumes/updates existing)
	cons2, err := SetupConsumer(ctx, js, streamName, "resume-view", "", cfg)
	if err != nil {
		t.Fatalf("Second SetupConsumer failed: %v", err)
	}
	if cons2 == nil {
		t.Fatal("Second SetupConsumer returned nil")
	}
}

func TestSetupConsumer_ConfigFieldsApplied(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_CONFIG_STREAM"
	createStream(t, ctx, js, streamName)

	cfg := natsql.ConsumerConfig{
		BatchSize:      25,
		MaxDeliver:     7,
		AckWaitSeconds: 15,
	}

	cons, err := SetupConsumer(ctx, js, streamName, "cfg-view", "", cfg)
	if err != nil {
		t.Fatalf("SetupConsumer failed: %v", err)
	}

	info := cons.CachedInfo()

	if info.Config.MaxDeliver != 7 {
		t.Errorf("MaxDeliver = %d, want 7", info.Config.MaxDeliver)
	}
	if info.Config.AckWait != 15*time.Second {
		t.Errorf("AckWait = %v, want 15s", info.Config.AckWait)
	}
	if info.Config.MaxAckPending != 50 {
		t.Errorf("MaxAckPending = %d, want 50 (25*2)", info.Config.MaxAckPending)
	}
}

func TestSetupConsumer_FilterSubjectApplied(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_FILTER_STREAM"
	createStreamWithSubjects(t, ctx, js, streamName, []string{streamName + ".>"})

	cfg := natsql.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10}
	sourceSubject := streamName + ".events.>"

	cons, err := SetupConsumer(ctx, js, streamName, "filter-view", sourceSubject, cfg)
	if err != nil {
		t.Fatalf("SetupConsumer failed: %v", err)
	}

	info := cons.CachedInfo()

	if info.Config.FilterSubject != sourceSubject {
		t.Errorf("FilterSubject = %q, want %q", info.Config.FilterSubject, sourceSubject)
	}
}

func TestSetupConsumer_DeliverAllPolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "TEST_DELIVER_ALL"
	createStream(t, ctx, js, streamName)

	cfg := natsql.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10}

	cons, err := SetupConsumer(ctx, js, streamName, "deliver-view", "", cfg)
	if err != nil {
		t.Fatalf("SetupConsumer failed: %v", err)
	}

	info := cons.CachedInfo()
	if info.Config.DeliverPolicy != jetstream.DeliverAllPolicy {
		t.Errorf("DeliverPolicy = %v, want DeliverAll", info.Config.DeliverPolicy)
	}
}

func TestSetupConsumer_StreamNotFound_ReturnsError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	cfg := natsql.ConsumerConfig{BatchSize: 10, MaxDeliver: 5, AckWaitSeconds: 10}

	_, err := SetupConsumer(ctx, js, "NONEXISTENT_STREAM", "test", "", cfg)
	if err == nil {
		t.Fatal("expected error for nonexistent stream, got nil")
	}
}

// Helpers

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
	createStreamWithSubjects(t, ctx, js, name, []string{name + ".>"})
}

func createStreamWithSubjects(t *testing.T, ctx context.Context, js jetstream.JetStream, name string, subjects []string) {
	t.Helper()
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      name,
		Subjects:  subjects,
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		t.Fatalf("failed to create stream %q: %v", name, err)
	}
}
