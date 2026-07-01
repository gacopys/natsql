// 11-consumer-tuning — JetStream consumer configuration tuning.
//
// Run: go run .
//
// What it does:
//  1. Creates view "fast_events" with consumer config (max_ack_pending, max_deliver, ack_wait)
//  2. Publishes 20 events
//  3. Queries all rows and prints count

package main

import (
	"context"
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
				Name:         "fast_events",
				SourceStream: "fast-stream",
				KeyFields:    []string{"event_id"},
				Columns: []natsql.ColumnConfig{
					{Name: "event_id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "payload", From: "payload", Type: natsql.ColumnTypeString},
				},
				Consumer: natsql.ConsumerConfig{
					MaxAckPending:  64,
					MaxDeliver:     3,
					AckWaitSeconds: 5,
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
		Name: "fast-stream", Subjects: []string{"fast.>"},
	}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	fmt.Println("✓ Engine started with consumer config:")
	fmt.Println("    max_ack_pending: 64")
	fmt.Println("    max_deliver:     3")
	fmt.Println("    ack_wait_seconds: 5")
	fmt.Println()

	const n = 20
	fmt.Printf("Publishing %d events...\n", n)
	for i := 0; i < n; i++ {
		event := fmt.Sprintf(`{"id":"evt-%02d","payload":"data-%02d"}`, i, i)
		if _, err := js.Publish(ctx, "fast.evt", []byte(event)); err != nil {
			log.Fatalf("Publish: %v", err)
		}
	}

	time.Sleep(time.Second)

	res := eng.Query(ctx, "SELECT * FROM fast_events WHERE event_id != ''")
	if res.Error != nil {
		log.Fatalf("Query: %s", *res.Error)
	}
	fmt.Printf("✓ Query returned %d / %d rows\n", len(res.Results), n)
	fmt.Printf("  View: fast_events (tuned consumer)\n")

	fmt.Println("\n✅ Consumer config tuning works!")
}
