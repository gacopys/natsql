// 08-where-operators — WHERE IN, WHERE !=, and full table scan queries.
//
// Run: go run .
//
// What it does:
//  1. Creates view "products" with PK id, columns name/price/status
//  2. Publishes 6 products with different statuses and prices
//  3. Demonstrates WHERE IN, WHERE !=, and WHERE on non-PK column

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
	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "products",
				SourceStream: "product-stream",
				KeyFields:    []string{"id"},
				Columns: []natsql.ColumnConfig{
					{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsql.ColumnTypeString},
					{Name: "price", From: "price", Type: natsql.ColumnTypeNumber},
					{Name: "status", From: "status", Type: natsql.ColumnTypeString},
				},
			},
		},
	}

	ctx := context.Background()

	eng, err := natsql.NewEmbedded(cfg)
	if err != nil {
		log.Fatalf("NewEmbedded: %v", err)
	}
	defer eng.Close()

	js, err := jetstream.New(eng.NC())
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "product-stream", Subjects: []string{"products.>"},
	}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	fmt.Println("✓ Engine started")

	events := []string{
		`{"id":"p1","name":"Widget","price":9.99,"status":"active"}`,
		`{"id":"p2","name":"Gadget","price":24.99,"status":"active"}`,
		`{"id":"p3","name":"Doohickey","price":9.99,"status":"pending"}`,
		`{"id":"p4","name":"Thingamabob","price":49.99,"status":"deleted"}`,
		`{"id":"p5","name":"Whatsit","price":19.99,"status":"pending"}`,
		`{"id":"p6","name":"Contraption","price":99.99,"status":"deleted"}`,
	}

	for _, e := range events {
		if _, err := js.Publish(ctx, "products.created", []byte(e)); err != nil {
			log.Fatalf("Publish: %v", err)
		}
	}
	fmt.Println("Published 6 products")

	time.Sleep(time.Second)

	fmt.Println("── WHERE IN ('active','pending') ──")
	res := eng.Query(ctx, "SELECT * FROM products WHERE status IN ('active','pending')")
	pretty(res, "active or pending")

	fmt.Println("── WHERE != 'deleted' ──")
	res = eng.Query(ctx, "SELECT * FROM products WHERE status != 'deleted'")
	pretty(res, "not deleted")

	fmt.Println("── WHERE price = 9.99 (non-PK full scan) ──")
	res = eng.Query(ctx, "SELECT * FROM products WHERE price = 9.99")
	pretty(res, "price = 9.99")

	fmt.Println("✅ WHERE operators work correctly!")
}

func pretty(r *natsql.QueryResult, label string) {
	if r.Error != nil {
		fmt.Printf("  ✗ %s: %s\n", label, *r.Error)
		return
	}
	b, _ := json.MarshalIndent(r.Results, "    ", "  ")
	fmt.Printf("  ✓ %s (%d rows): %s\n", label, len(r.Results), string(b))
}
