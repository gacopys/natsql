// 11-consumer-tuning — JetStream consumer configuration tuning.
//
// Run: go run .
//
// What it does:
//  1. Creates view "fast_events" with consumer config (max_ack_pending, max_deliver, ack_wait)
//  2. Publishes 20 events
//  3. Queries all rows and checks the per-view counters in Stats
//
// The program checks every result and exits non-zero if one is wrong.

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql/v2"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
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
					MaxAckPending:  64, // events in flight before the consumer waits for acks
					MaxDeliver:     3,  // attempts before a failing event goes to the DLQ (-1: retry forever)
					AckWaitSeconds: 5,  // redeliver an event not acked within 5s
				},
			},
		},
	}

	ctx := context.Background()

	eng, err := natsql.NewEmbedded(cfg)
	if err != nil {
		return fmt.Errorf("NewEmbedded: %w", err)
	}
	defer eng.Close()

	js, err := jetstream.New(eng.NC())
	if err != nil {
		return fmt.Errorf("JetStream: %w", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "fast-stream", Subjects: []string{"fast.>"},
	}); err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	fmt.Println("✓ Engine started with consumer config:")
	fmt.Println("    max_ack_pending: 64")
	fmt.Println("    max_deliver:     3")
	fmt.Println("    ack_wait_seconds: 5")
	fmt.Println()

	const n = 20
	fmt.Printf("Publishing %d events...\n", n)
	for i := range n {
		event := fmt.Sprintf(`{"id":"evt-%02d","payload":"data-%02d"}`, i, i)
		if _, err := js.Publish(ctx, "fast.evt", []byte(event)); err != nil {
			return fmt.Errorf("Publish: %w", err)
		}
	}

	rows, err := waitForRows(ctx, eng, "SELECT * FROM fast_events WHERE event_id != ''", n)
	if err != nil {
		return err
	}
	fmt.Printf("✓ Query returned %d / %d rows\n", len(rows), n)

	status := eng.Stats().ViewStatus[0]
	if status.EventsProcessed != n || status.DeadLettered != 0 || status.Retried != 0 {
		return fmt.Errorf("view status = %+v, want %d events processed and no retries or DLQ", status, n)
	}
	fmt.Printf("✓ View %s: state=%s events_processed=%d dead_lettered=%d retried=%d\n",
		status.Name, status.State, status.EventsProcessed, status.DeadLettered, status.Retried)

	fmt.Println("\n✅ Consumer config tuning works!")
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
