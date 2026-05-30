// 03-malformed-events — error handling and DLQ (dead letter queue).
//
// Run: go run .
//
// What it does:
//  1. Creates a view "users" with type validation
//  2. Publishes a mix of valid and invalid events:
//     - Valid JSON → materializes normally
//     - Invalid JSON → goes to DLQ
//     - Type mismatch (string instead of number) → goes to DLQ
//     - Missing key field → goes to DLQ
//  3. Reads the DLQ stream to show what was rejected
//  4. Shows that valid events still materialize despite bad ones

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
	// View with strict types (D-16: type mismatch → DLQ)
	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "users",
				SourceStream: "events",
				KeyFields:    []string{"user_id"},
				Columns: []natsql.ColumnConfig{
					{Name: "user_id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsql.ColumnTypeString},
					{Name: "age", From: "age", Type: natsql.ColumnTypeNumber},
					{Name: "active", From: "active", Type: natsql.ColumnTypeBoolean},
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

	// Create stream before starting engine
	nc := eng.NC()
	js, _ := jetstream.New(nc)
	js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "events",
		Subjects: []string{"events.>"},
	})

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	fmt.Println("✓ Engine started\n")

	// Subscribe to DLQ BEFORE publishing so we don't miss messages
	dlqSub, err := nc.SubscribeSync("natsql.dlq")
	if err != nil {
		log.Fatalf("DLQ subscribe: %v", err)
	}
	defer dlqSub.Unsubscribe()

	// Publish a mix of valid and invalid events
	testEvents := []struct {
		label string
		data  string
	}{
		{"VALID event", `{"id": "abc", "name": "Alice", "age": 30, "active": true}`},
		{"INVALID JSON", `not valid json at all`},
		{"TYPE MISMATCH (age should be number)", `{"id": "def", "name": "Bob", "age": "not-a-number", "active": true}`},
		{"MISSING KEY (no id)", `{"name": "Carol", "age": 25, "active": true}`},
		{"VALID event", `{"id": "ghi", "name": "Dave", "age": 35, "active": false}`},
	}

	for _, e := range testEvents {
		_, err := js.Publish(ctx, "events.user-updated", []byte(e.data))
		if err != nil {
			log.Fatalf("Publish: %v", err)
		}
		fmt.Printf("  Published [%s]: %s\n", e.label, e.data)
	}

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Check which valid events made it into KV
	fmt.Println("\n── Materialized rows ──")
	res := eng.Query(ctx, "SELECT * FROM users WHERE user_id = 'abc'")
	check(res, "alice")
	res = eng.Query(ctx, "SELECT * FROM users WHERE user_id = 'ghi'")
	check(res, "dave (should exist)")

	// Read DLQ messages (subscribed before publishing so we catch them)
	fmt.Println("\n── DLQ contents (rejected events) ──")

	for i := 0; i < 5; i++ {
		msg, err := dlqSub.NextMsg(500 * time.Millisecond)
		if err != nil {
			break // no more messages
		}
		var envelope map[string]any
		json.Unmarshal(msg.Data, &envelope)
		b, _ := json.MarshalIndent(envelope, "    ", "  ")
		fmt.Printf("  DLQ[%d]: %s\n", i+1, string(b))
	}
	fmt.Println("\n✅ Malformed events go to DLQ; valid events process correctly!")
}

func check(r *natsql.QueryResult, label string) {
	if r.Error != nil {
		fmt.Printf("  ✗ %s: %s\n", label, *r.Error)
		return
	}
	fmt.Printf("  ✓ %s: %d result(s)\n", label, len(r.Results))
	for _, row := range r.Results {
		b, _ := json.Marshal(row)
		fmt.Printf("       %s\n", string(b))
	}
}
