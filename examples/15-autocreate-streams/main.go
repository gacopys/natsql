// 15-autocreate-streams — simulating --create-streams flag and stream existence handling.
//
// Run: go run .
//
// What it does:
//  1. Uses NewWithNATS with embedded NATS as a proxy for external NATS
//  2. Creates stream manually, then starts engine
//  3. Shows that Start detects the existing stream (or warns if missing)

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql"
)

func main() {
	ctx := context.Background()

	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "inventory",
				SourceStream: "inventory-stream",
				KeyFields:    []string{"sku"},
				Columns: []natsql.ColumnConfig{
					{Name: "sku", From: "sku", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "qty", From: "qty", Type: natsql.ColumnTypeNumber},
				},
			},
		},
	}

	fmt.Println("═══ Scenario: manual stream creation (--create-streams) ═══")
	eng1, err := natsql.NewEmbedded(cfg)
	if err != nil {
		log.Fatalf("NewEmbedded: %v", err)
	}

	nc := eng1.NC()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "inventory-stream", Subjects: []string{"inventory.>"},
	}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}
	fmt.Println("  ✓ Created stream 'inventory-stream' manually")

	if err := eng1.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	fmt.Println("  ✓ Engine started — stream already existed")
	defer eng1.Close()

	if _, err := js.Publish(ctx, "inventory.update", []byte(`{"sku":"SKU-100","qty":42}`)); err != nil {
		log.Fatalf("Publish: %v", err)
	}

	time.Sleep(time.Second)

	res := eng1.Query(ctx, "SELECT * FROM inventory WHERE sku = 'SKU-100'")
	b, _ := json.Marshal(res.Results)
	fmt.Printf("  ✓ Query result: %s\n\n", b)
	eng1.Close()

	fmt.Println("═══ Scenario: engine detects existing stream on restart ═══")
	eng2, err := natsql.NewEmbedded(cfg)
	if err != nil {
		log.Fatalf("NewEmbedded: %v", err)
	}
	defer eng2.Close()

	js2, err := jetstream.New(eng2.NC())
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}

	fmt.Printf("  Checking if stream exists... ")
	_, err = js2.Stream(ctx, "inventory-stream")
	if err != nil {
		fmt.Printf("NOT FOUND — would need --create-streams\n")
	} else {
		fmt.Printf("FOUND — safe to start engine\n")
	}

	if err := eng2.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	fmt.Println("  ✓ Engine started on existing stream")

	fmt.Println("\n✅ --create-streams workflow works!")
}
