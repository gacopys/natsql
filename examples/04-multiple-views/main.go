// 04-multiple-views — two independent materialized views sharing one engine.
//
// Run: go run .
//
// What it does:
//  1. Creates two views: "users" and "products" with different schemas
//  2. Both views run in the same engine, use the same bucket
//  3. Publish events to both streams
//  4. Query both views independently
//  5. Shows that views are isolated by name prefix in KV

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
	// Two independent views in one config
	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "users",
				SourceStream: "user-stream",
				KeyFields:    []string{"user_id"},
				Columns: []natsql.ColumnConfig{
					{Name: "user_id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsql.ColumnTypeString},
					{Name: "role", From: "role", Type: natsql.ColumnTypeString},
				},
			},
			{
				Name:         "products",
				SourceStream: "product-stream",
				KeyFields:    []string{"sku"},
				Columns: []natsql.ColumnConfig{
					{Name: "sku", From: "sku", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "title", From: "title", Type: natsql.ColumnTypeString},
					{Name: "price", From: "price", Type: natsql.ColumnTypeNumber},
					{Name: "in_stock", From: "in_stock", Type: natsql.ColumnTypeBoolean},
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

	// Create streams before starting engine
	nc := eng.NC()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: "user-stream", Subjects: []string{"users.>"}}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: "product-stream", Subjects: []string{"products.>"}}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	fmt.Println("✓ Engine started with 2 views\n")

	// Publish user events
	for _, u := range []string{
		`{"id": "u1", "name": "Alice", "role": "admin"}`,
		`{"id": "u2", "name": "Bob", "role": "viewer"}`,
	} {
		if _, err := js.Publish(ctx, "users.created", []byte(u)); err != nil {
			log.Fatalf("Publish user: %v", err)
		}
	}
	fmt.Println("  Published 2 user events")

	// Publish product events
	for _, p := range []string{
		`{"sku": "SKU-001", "title": "Widget", "price": 9.99, "in_stock": true}`,
		`{"sku": "SKU-002", "title": "Gadget", "price": 24.99, "in_stock": false}`,
		`{"sku": "SKU-003", "title": "Doohickey", "price": 49.99, "in_stock": true}`,
	} {
		if _, err := js.Publish(ctx, "products.created", []byte(p)); err != nil {
			log.Fatalf("Publish product: %v", err)
		}
	}
	fmt.Println("  Published 3 product events\n")

	time.Sleep(time.Second)

	// Query users
	fmt.Println("── Users ──")
	res := eng.Query(ctx, "SELECT * FROM users WHERE user_id = 'u1'")
	pretty(res, "Alice")
	res = eng.Query(ctx, "SELECT name, role FROM users WHERE user_id = 'u2'")
	pretty(res, "Bob (projected)")

	// Query products
	fmt.Println("── Products ──")
	res = eng.Query(ctx, "SELECT * FROM products WHERE sku = 'SKU-002'")
	pretty(res, "Gadget")

	// Query a non-existent view — should error clearly
	res = eng.Query(ctx, "SELECT * FROM nonexistent WHERE id = 'x'")
	if res.Error != nil {
		fmt.Printf("  ✓ Unknown view error: %s\n", *res.Error)
	}

	// Query a non-existent column — should error clearly
	res = eng.Query(ctx, "SELECT fake_column FROM users WHERE user_id = 'u1'")
	if res.Error != nil {
		fmt.Printf("  ✓ Unknown column error: %s\n", *res.Error)
	}

	fmt.Println("\n✅ Multiple views work independently in one engine!")
}

func pretty(r *natsql.QueryResult, label string) {
	if r.Error != nil {
		fmt.Printf("  ✗ %s: %s\n", label, *r.Error)
		return
	}
	b, _ := json.MarshalIndent(r.Results, "    ", "  ")
	fmt.Printf("  ✓ %s: %s\n", label, string(b))
}
