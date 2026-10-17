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

	// ── Option A: You already manage NATS yourself ──────────────────
	fmt.Println("═══ Pattern A: natsql.New(js, cfg) ═══")
	fmt.Println("You own the NATS connection and JetStream context.")
	fmt.Println()

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

	// Create engine from existing JetStream context
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
		return fmt.Errorf("CreateStream: %w", err)
	}

	eng, err := natsql.New(js, cfg) // You own the connection!
	if err != nil {
		return fmt.Errorf("New: %w", err)
	}
	defer eng.Close()

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}

	// Publish from your own NATS code
	if _, err := js.Publish(ctx, "items.created", []byte(`{"id": "item-1", "label": "My First Item"}`)); err != nil {
		return fmt.Errorf("Publish: %w", err)
	}

	rows, err := waitForRows(ctx, eng, "SELECT * FROM items WHERE id = 'item-1'", 1)
	if err != nil {
		return err
	}
	if got := fmt.Sprint(rows[0]["label"]); got != "My First Item" {
		return fmt.Errorf("item-1 label = %q, want %q", got, "My First Item")
	}
	b, _ := json.Marshal(rows) // rows always encode: they came from JSON
	fmt.Printf("  ✓ Query result: %s\n\n", b)

	// ── Option B: You have a NATS connection (convenience) ──────────
	fmt.Println("═══ Pattern B: natsql.NewWithNATS(nc, cfg) ═══")
	fmt.Println("Pass your NATS connection — engine handles JetStream setup.")
	fmt.Println()

	// Create a separate connection for Pattern B to avoid lifecycle hazard
	// (NewWithNATS takes ownership of the connection, closing it on Close())
	nc2, err := nats.Connect(srv.ClientURL())
	if err != nil {
		return fmt.Errorf("Connect (B): %w", err)
	}

	// Create stream before starting engine (Pattern B)
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "note-stream", Subjects: []string{"notes.>"},
	}); err != nil {
		nc2.Close()
		return fmt.Errorf("CreateStream: %w", err)
	}

	eng2, err := natsql.NewWithNATS(nc2, &natsql.Config{
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
		nc2.Close()
		return fmt.Errorf("NewWithNATS: %w", err)
	}
	defer eng2.Close()

	if err := eng2.Start(ctx); err != nil {
		return fmt.Errorf("Start (B): %w", err)
	}
	if _, err := js.Publish(ctx, "notes.created", []byte(`{"id": "note-1", "text": "Hello from NewWithNATS!"}`)); err != nil {
		return fmt.Errorf("Publish: %w", err)
	}

	rows, err = waitForRows(ctx, eng2, "SELECT * FROM notes WHERE id = 'note-1'", 1)
	if err != nil {
		return err
	}
	if got := fmt.Sprint(rows[0]["text"]); got != "Hello from NewWithNATS!" {
		return fmt.Errorf("note-1 text = %q, want %q", got, "Hello from NewWithNATS!")
	}
	b, _ = json.Marshal(rows) // rows always encode: they came from JSON
	fmt.Printf("  ✓ Query result: %s\n", b)

	// Each engine only knows its own views.
	if res := eng.Query(ctx, "SELECT * FROM items WHERE id = 'item-1'"); res.Error != nil || len(res.Results) != 1 {
		return fmt.Errorf("engine A lost its view: %+v", res)
	}

	fmt.Println("\n✅ natsql works as an embedded Go library!")
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
