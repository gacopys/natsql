// 02-composite-keys — materialized view with multi-field PK and nested JSON paths.
//
// Run: go run .
//
// What it does:
//  1. Creates a view "orders" with composite key (org_id + order_id)
//  2. Column mapping uses dot paths to extract nested fields from events
//  3. Publishes events with nested JSON structure
//  4. Queries by different key combinations

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
	// Config with composite key (key_fields: [org_id, order_id]) + nested JSON paths
	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "orders",
				SourceStream: "orders-stream",
				KeyFields:    []string{"org_id", "order_id"},
				KeySeparator: "-",
				Columns: []natsql.ColumnConfig{
					{Name: "org_id", From: "org.id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "order_id", From: "order.id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "total", From: "order.total", Type: natsql.ColumnTypeNumber},
					{Name: "status", From: "order.status", Type: natsql.ColumnTypeString},
					{Name: "customer", From: "customer.name", Type: natsql.ColumnTypeString},
				},
			},
		},
	}

	ctx := context.Background()

	// Create engine but don't start yet
	eng, err := natsql.NewEmbedded(cfg)
	if err != nil {
		log.Fatalf("NewEmbedded: %v", err)
	}
	defer eng.Close()

	// Create stream before starting engine
	js, err := jetstream.New(eng.NC())
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "orders-stream",
		Subjects: []string{"orders.>"},
	}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	fmt.Println("✓ Engine started")

	// Publish events with nested JSON — column mapping uses dot paths
	events := []string{
		`{"org": {"id": "acme"}, "order": {"id": "ord-1", "total": 42.50, "status": "shipped"}, "customer": {"name": "Alice"}}`,
		`{"org": {"id": "acme"}, "order": {"id": "ord-2", "total": 99.99, "status": "pending"}, "customer": {"name": "Bob"}}`,
		`{"org": {"id": "globex"}, "order": {"id": "ord-3", "total": 15.00, "status": "shipped"}, "customer": {"name": "Carol"}}`,
	}
	for _, e := range events {
		if _, err := js.Publish(ctx, "orders.created", []byte(e)); err != nil {
			log.Fatalf("Publish: %v", err)
		}
		fmt.Println("  Published:", e)
	}

	time.Sleep(time.Second)

	// Query by full composite key (both key fields in WHERE)
	res := eng.Query(ctx, "SELECT * FROM orders WHERE org_id = 'acme' AND order_id = 'ord-1'")
	pretty(res, "acme:ord-1")
	if len(res.Results) == 0 {
		fmt.Println("  ⚠ No results — FullScanPlan may be needed for non-PK WHERE")
	}

	// Query all orders for one org (full scan on non-PK field)
	res = eng.Query(ctx, "SELECT customer, total FROM orders WHERE org_id = 'acme'")
	pretty(res, "acme orders (projected columns)")

	// Stats
	s := eng.Stats()
	fmt.Printf("\n✓ Stats: %d views, %d goroutines\n", s.Views, s.Goroutines)
	fmt.Println("✅ Composite keys work with nested JSON paths!")
}

func pretty(r *natsql.QueryResult, label string) {
	if r.Error != nil {
		fmt.Printf("  ✗ %s: %s\n", label, *r.Error)
		return
	}
	b, _ := json.MarshalIndent(r.Results, "    ", "  ")
	fmt.Printf("  ✓ %s: %s\n", label, string(b))
}
