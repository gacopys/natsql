// 17-dual-engine — two independent embedded engines in one process.
//
// Run: go run .
//
// What it does:
//  1. Creates TWO embedded engines with different HTTP ports (8081, 8082)
//  2. Engine 1: view "users" with id(PK), name
//  3. Engine 2: view "orders" with id(PK), total, status
//  4. Publishes and queries each independently
//  5. Shows they don't interfere

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

	userCfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "users",
				SourceStream: "users-stream",
				KeyFields:    []string{"id"},
				Columns: []natsql.ColumnConfig{
					{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsql.ColumnTypeString},
				},
			},
		},
	}

	orderCfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "orders",
				SourceStream: "orders-stream",
				KeyFields:    []string{"id"},
				Columns: []natsql.ColumnConfig{
					{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "total", From: "total", Type: natsql.ColumnTypeNumber},
					{Name: "status", From: "status", Type: natsql.ColumnTypeString},
				},
			},
		},
	}

	fmt.Println("═══ Engine 1: users (port 8081) ═══")
	eng1, err := natsql.NewEmbedded(userCfg, natsql.WithQueryPort(8081))
	if err != nil {
		log.Fatalf("NewEmbedded (1): %v", err)
	}
	defer eng1.Close()

	js1, err := jetstream.New(eng1.NC())
	if err != nil {
		log.Fatalf("JetStream (1): %v", err)
	}
	if _, err := js1.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "users-stream", Subjects: []string{"users.>"},
	}); err != nil {
		log.Fatalf("CreateStream (1): %v", err)
	}
	if err := eng1.Start(ctx); err != nil {
		log.Fatalf("Start (1): %v", err)
	}
	fmt.Println("  ✓ Engine 1 started on port 8081")

	fmt.Println()
	fmt.Println("═══ Engine 2: orders (port 8082) ═══")
	eng2, err := natsql.NewEmbedded(orderCfg, natsql.WithQueryPort(8082))
	if err != nil {
		log.Fatalf("NewEmbedded (2): %v", err)
	}
	defer eng2.Close()

	js2, err := jetstream.New(eng2.NC())
	if err != nil {
		log.Fatalf("JetStream (2): %v", err)
	}
	if _, err := js2.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "orders-stream", Subjects: []string{"orders.>"},
	}); err != nil {
		log.Fatalf("CreateStream (2): %v", err)
	}
	if err := eng2.Start(ctx); err != nil {
		log.Fatalf("Start (2): %v", err)
	}
	fmt.Println("  ✓ Engine 2 started on port 8082")

	fmt.Println()
	fmt.Println("Publishing to both engines...")
	if _, err := js1.Publish(ctx, "users.created", []byte(`{"id":"u1","name":"Alice"}`)); err != nil {
		log.Fatalf("Publish (1): %v", err)
	}
	if _, err := js2.Publish(ctx, "orders.created", []byte(`{"id":"o1","total":99.99,"status":"shipped"}`)); err != nil {
		log.Fatalf("Publish (2): %v", err)
	}

	time.Sleep(time.Second)

	fmt.Println()
	fmt.Println("── Query Engine 1 (users) ──")
	res1 := eng1.Query(ctx, "SELECT * FROM users WHERE id = 'u1'")
	b1, _ := json.Marshal(res1.Results)
	fmt.Printf("  ✓ %s\n", b1)

	fmt.Println("── Query Engine 2 (orders) ──")
	res2 := eng2.Query(ctx, "SELECT * FROM orders WHERE id = 'o1'")
	b2, _ := json.Marshal(res2.Results)
	fmt.Printf("  ✓ %s\n", b2)

	s1 := eng1.Stats()
	s2 := eng2.Stats()
	fmt.Printf("\n  Engine 1: %d views, %d goroutines\n", s1.Views, s1.Goroutines)
	fmt.Printf("  Engine 2: %d views, %d goroutines\n", s2.Views, s2.Goroutines)

	fmt.Println("\n✅ Two independent engines run side-by-side without interference!")
}
