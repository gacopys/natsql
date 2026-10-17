// 05-library-embed — use natsql as a Go library within your own application.
//
// Run: go run .
//
// This shows the pattern for embedding natsql in a Go app that
// already has a NATS connection. You create the engine from your
// existing connection instead of using embedded NATS.
//
// Two constructor patterns:
//   A. natsql.New(js, cfg) — you already have a JetStream context
//   B. natsql.NewWithNATS(nc, cfg) — you have a NATS connection (engine creates JS)
//
// The program checks every result and exits non-zero if one is wrong.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql/v2"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// Start your own NATS server (or connect to existing). Give it its own
	// store directory: without one, nats-server uses a shared default path
	// and would see state left by other runs.
	storeDir, err := os.MkdirTemp("", "natsql-example-05-")
	if err != nil {
		return fmt.Errorf("MkdirTemp: %w", err)
	}
	defer os.RemoveAll(storeDir)

	srv, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  storeDir,
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		return fmt.Errorf("NewServer: %w", err)
	}
	go srv.Start()
	defer srv.Shutdown()
	if !srv.ReadyForConnections(10 * time.Second) {
		return errors.New("NATS server not ready")
	}

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		return fmt.Errorf("Connect: %w", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("JetStream: %w", err)
	}

	engA, err := patternA(ctx, js)
	if err != nil {
		return fmt.Errorf("pattern A: %w", err)
	}
	defer engA.Close()

	engB, err := patternB(ctx, srv.ClientURL(), js)
	if err != nil {
		return fmt.Errorf("pattern B: %w", err)
	}
	defer engB.Close()

	// Both engines keep working side by side on one NATS server.
	if res := engA.Query(ctx, "SELECT * FROM items WHERE id = 'item-1'"); res.Error != nil || len(res.Results) != 1 {
		return fmt.Errorf("engine A lost its view: %+v", res)
	}

	fmt.Println("\n✅ natsql works as an embedded Go library!")
	return nil
}

// patternA creates an engine from a JetStream context the application owns.
func patternA(ctx context.Context, js jetstream.JetStream) (*natsql.Engine, error) {
	fmt.Println("═══ Pattern A: natsql.New(js, cfg) ═══")
	fmt.Println("You own the NATS connection and JetStream context.")
	fmt.Println()

	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name: "items", SourceStream: "item-stream", KeyFields: []string{"id"},
				Columns: []natsql.ColumnConfig{
					{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "label", From: "label", Type: natsql.ColumnTypeString},
				},
			},
		},
	}

	// Create stream before starting engine
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "item-stream", Subjects: []string{"items.>"},
	}); err != nil {
		return nil, fmt.Errorf("CreateStream: %w", err)
	}

	eng, err := natsql.New(js, cfg) // You own the connection!
	if err != nil {
		return nil, fmt.Errorf("New: %w", err)
	}
	if err := eng.Start(ctx); err != nil {
		eng.Close()
		return nil, fmt.Errorf("Start: %w", err)
	}

	// Publish from your own NATS code
	if err := publishAndCheck(ctx, eng, js, "items.created", `{"id": "item-1", "label": "My First Item"}`,
		"SELECT * FROM items WHERE id = 'item-1'", "label", "My First Item"); err != nil {
		eng.Close()
		return nil, err
	}
	fmt.Println()
	return eng, nil
}

// patternB creates an engine from a NATS connection it then owns.
func patternB(ctx context.Context, url string, js jetstream.JetStream) (*natsql.Engine, error) {
	fmt.Println("═══ Pattern B: natsql.NewWithNATS(nc, cfg) ═══")
	fmt.Println("Pass your NATS connection — engine handles JetStream setup.")
	fmt.Println()

	// A separate connection, because NewWithNATS takes ownership of it and
	// closes it on Close().
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("Connect: %w", err)
	}

	// Create stream before starting engine
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "note-stream", Subjects: []string{"notes.>"},
	}); err != nil {
		nc.Close()
		return nil, fmt.Errorf("CreateStream: %w", err)
	}

	eng, err := natsql.NewWithNATS(nc, &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name: "notes", SourceStream: "note-stream", KeyFields: []string{"id"},
				Columns: []natsql.ColumnConfig{
					{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "text", From: "text", Type: natsql.ColumnTypeString},
				},
			},
		},
	}, natsql.WithQueryPort(8081))
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("NewWithNATS: %w", err)
	}
	if err := eng.Start(ctx); err != nil {
		eng.Close()
		return nil, fmt.Errorf("Start: %w", err)
	}

	if err := publishAndCheck(ctx, eng, js, "notes.created", `{"id": "note-1", "text": "Hello from NewWithNATS!"}`,
		"SELECT * FROM notes WHERE id = 'note-1'", "text", "Hello from NewWithNATS!"); err != nil {
		eng.Close()
		return nil, err
	}
	return eng, nil
}

// publishAndCheck publishes event, waits for sql to return one row, and
// checks one of its columns.
func publishAndCheck(ctx context.Context, eng *natsql.Engine, js jetstream.JetStream, subject, event, sql, col, want string) error {
	if _, err := js.Publish(ctx, subject, []byte(event)); err != nil {
		return fmt.Errorf("Publish: %w", err)
	}
	rows, err := waitForRows(ctx, eng, sql, 1)
	if err != nil {
		return err
	}
	if got := fmt.Sprint(rows[0][col]); got != want {
		return fmt.Errorf("%s: %s = %q, want %q", sql, col, got, want)
	}
	b, _ := json.Marshal(rows) // rows always encode: they came from JSON
	fmt.Printf("  ✓ Query result: %s\n", b)
	return nil
}

// waitForRows runs sql until it returns want rows, so the example doesn't
// depend on how quickly events are materialized. It gives up after 10s.
func waitForRows(ctx context.Context, eng *natsql.Engine, sql string, want int) ([]map[string]any, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		res := eng.Query(ctx, sql)
		if res.Error != nil {
			return nil, fmt.Errorf("%s: %s", sql, *res.Error)
		}
		if len(res.Results) == want {
			return res.Results, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("%s: got %d rows, want %d", sql, len(res.Results), want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
