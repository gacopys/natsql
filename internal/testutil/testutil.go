package testutil

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// StartEmbeddedNATS starts an embedded NATS server for testing.
// Returns a connected NATS client and JetStream handle.
// The server and connection are automatically cleaned up on test completion.
func StartEmbeddedNATS(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()

	srv, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatalf("StartEmbeddedNATS: NewServer: %v", err)
	}
	t.Cleanup(srv.Shutdown)

	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		t.Fatalf("StartEmbeddedNATS: server not ready within 10s")
	}

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("StartEmbeddedNATS: Connect: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("StartEmbeddedNATS: JetStream: %v", err)
	}

	return nc, js
}

// CreateStream creates a test stream with a standard config.
// The stream is created with subjects "{name}.>" for prefix-based matching.
func CreateStream(t *testing.T, ctx context.Context, js jetstream.JetStream, name string) jetstream.Stream {
	t.Helper()

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     name,
		Subjects: []string{name + ".>"},
	})
	if err != nil {
		t.Fatalf("CreateStream(%q): %v", name, err)
	}
	return stream
}
