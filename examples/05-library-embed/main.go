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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql"
)

func main() {
	ctx := context.Background()

	// ── Option A: You already manage NATS yourself ──────────────────
	fmt.Println("═══ Pattern A: natsql.New(js, cfg) ═══")
	fmt.Println("You own the NATS connection and JetStream context.")
	fmt.Println()

	// Start your own NATS server (or connect to existing)
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host:     "127.0.0.1",
		Port:     -1,
		JetStream: true,
		NoLog:    true,
		NoSigs:   true,
	})
	if err != nil {
		log.Fatalf("NewServer: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		log.Fatal("Server not ready")
	}
	defer srv.Shutdown()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		log.Fatalf("Connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}

	// Create engine from existing JetStream context
	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{Name: "items", SourceStream: "item-stream", KeyFields: []string{"id"},
				Columns: []natsql.ColumnConfig{
					{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "label", From: "label", Type: natsql.ColumnTypeString},
				},
			},
		},
	}

	// Create stream before starting engine
	js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "item-stream", Subjects: []string{"items.>"},
	})

	eng, err := natsql.New(js, cfg) // You own the connection!
	if err != nil {
		log.Fatalf("New: %v", err)
	}
	defer eng.Close()

	eng.Start(ctx)

	// Publish from your own NATS code
	js.Publish(ctx, "items.created", []byte(`{"id": "item-1", "label": "My First Item"}`))

	time.Sleep(time.Second)

	res := eng.Query(ctx, "SELECT * FROM items WHERE id = 'item-1'")
	b, _ := json.Marshal(res.Results)
	fmt.Printf("  Query result: %s\n\n", b)




	
	// ── Option B: You have a NATS connection (convenience) ──────────
	fmt.Println("═══ Pattern B: natsql.NewWithNATS(nc, cfg) ═══")
	fmt.Println("Pass your NATS connection — engine handles JetStream setup.")
	fmt.Println()

	// Create stream before starting engine (Pattern B)
	js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "note-stream", Subjects: []string{"notes.>"},
	})

	eng2, err := natsql.NewWithNATS(nc, &natsql.Config{
		Views: []natsql.ViewConfig{
			{Name: "notes", SourceStream: "note-stream", KeyFields: []string{"id"},
				Columns: []natsql.ColumnConfig{
					{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "text", From: "text", Type: natsql.ColumnTypeString},
				},
			},
		},
	}, natsql.WithQueryPort(8081))
	if err != nil {
		log.Fatalf("NewWithNATS: %v", err)
	}
	defer eng2.Close()

	eng2.Start(ctx)
	js.Publish(ctx, "notes.created", []byte(`{"id": "note-1", "text": "Hello from NewWithNATS!"}`))

	time.Sleep(time.Second)

	res = eng2.Query(ctx, "SELECT * FROM notes WHERE id = 'note-1'")
	b, _ = json.Marshal(res.Results)
	fmt.Printf("  Query result: %s\n", b)

	fmt.Println("\n✅ natsql works as an embedded Go library!")
}
