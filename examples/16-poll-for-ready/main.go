// 16-poll-for-ready — polling for materialization readiness with timeout.
//
// Run: go run .
//
// What it does:
//  1. Creates view "orders" with PK order_id, columns product/amount
//  2. Publishes 30 events
//  3. Polls with a loop until count >= 30, showing progress
//  4. Demonstrates LIMIT query

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
				Name:         "orders",
				SourceStream: "order-stream",
				KeyFields:    []string{"order_id"},
				Columns: []natsql.ColumnConfig{
					{Name: "order_id", From: "order_id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "product", From: "product", Type: natsql.ColumnTypeString},
					{Name: "amount", From: "amount", Type: natsql.ColumnTypeNumber},
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
		Name: "order-stream", Subjects: []string{"orders.>"},
	}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	fmt.Println("✓ Engine started")

	const total = 30
	fmt.Printf("Publishing %d orders...\n", total)
	products := []string{"Widget", "Gadget", "Doohickey", "Thingamabob", "Whatsit"}

	for i := 0; i < total; i++ {
		event := fmt.Sprintf(`{"order_id":"ord-%03d","product":"%s","amount":%.2f}`,
			i, products[i%len(products)], float64(i)*1.5+10.0)
		if _, err := js.Publish(ctx, "orders.created", []byte(event)); err != nil {
			log.Fatalf("Publish: %v", err)
		}
	}
	fmt.Println()

	fmt.Println("Polling for materialization readiness...")
	fmt.Printf("  Timeout: 10s | Target: %d rows\n\n", total)

	pollStart := time.Now()
	var lastCount int
	for {
		res := eng.Query(ctx, "SELECT * FROM orders WHERE order_id != ''")
		if res.Error != nil {
			log.Fatalf("Poll query: %s", *res.Error)
		}
		count := len(res.Results)
		if count != lastCount {
			fmt.Printf("  Progress: %d / %d rows\n", count, total)
			lastCount = count
		}
		if count >= total {
			break
		}
		if time.Since(pollStart) > 10*time.Second {
			log.Fatalf("Timeout: got %d / %d rows after %v", count, total, time.Since(pollStart))
		}
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Printf("\n✓ All %d rows materialized in %v\n\n", total, time.Since(pollStart))

	fmt.Println("── LIMIT query ──")
	res := eng.Query(ctx, "SELECT * FROM orders WHERE order_id != '' LIMIT 5")
	b, _ := json.MarshalIndent(res.Results, "    ", "  ")
	fmt.Printf("  ✓ LIMIT 5:\n%s\n", string(b))

	fmt.Println("✅ Polling for materialization readiness works!")
}
