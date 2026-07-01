// 10-source-subject — source_subject filtering across two views sharing one stream.
//
// Run: go run .
//
// What it does:
//  1. Creates one stream "all-events" with subjects "all-events.>"
//  2. Creates two views filtering different subjects (user.created vs user.deleted)
//  3. Publishes events to both subjects
//  4. Verifies each view only sees its subset

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
				Name:          "user_creates",
				SourceStream:  "all-events",
				SourceSubject: "all-events.user.created",
				KeyFields:     []string{"user_id"},
				Columns: []natsql.ColumnConfig{
					{Name: "user_id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsql.ColumnTypeString},
				},
			},
			{
				Name:          "user_deletes",
				SourceStream:  "all-events",
				SourceSubject: "all-events.user.deleted",
				KeyFields:     []string{"user_id"},
				Columns: []natsql.ColumnConfig{
					{Name: "user_id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsql.ColumnTypeString},
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
		Name: "all-events", Subjects: []string{"all-events.>"},
	}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	fmt.Println("✓ Engine started with source_subject filtering")

	fmt.Println("Publishing to all-events.user.created:")
	for _, e := range []string{
		`{"id":"u1","name":"Alice"}`,
		`{"id":"u2","name":"Bob"}`,
	} {
		if _, err := js.Publish(ctx, "all-events.user.created", []byte(e)); err != nil {
			log.Fatalf("Publish: %v", err)
		}
		fmt.Printf("  → %s\n", e)
	}

	fmt.Println("Publishing to all-events.user.deleted:")
	for _, e := range []string{
		`{"id":"u3","name":"Carol"}`,
	} {
		if _, err := js.Publish(ctx, "all-events.user.deleted", []byte(e)); err != nil {
			log.Fatalf("Publish: %v", err)
		}
		fmt.Printf("  → %s\n", e)
	}

	time.Sleep(time.Second)

	fmt.Println()
	fmt.Println("── Query user_creates ──")
	res := eng.Query(ctx, "SELECT * FROM user_creates WHERE user_id = 'u1'")
	pretty(res, "user_creates (should have u1)")

	res = eng.Query(ctx, "SELECT * FROM user_creates WHERE user_id != ''")
	fmt.Printf("  Total rows in user_creates: %d\n", len(res.Results))

	fmt.Println("── Query user_deletes ──")
	res = eng.Query(ctx, "SELECT * FROM user_deletes WHERE user_id = 'u3'")
	pretty(res, "user_deletes (should have u3)")

	res = eng.Query(ctx, "SELECT * FROM user_deletes WHERE user_id != ''")
	fmt.Printf("  Total rows in user_deletes: %d\n", len(res.Results))

	fmt.Println("\n✅ source_subject filtering isolates views on the same stream!")
}

func pretty(r *natsql.QueryResult, label string) {
	if r.Error != nil {
		fmt.Printf("  ✗ %s: %s\n", label, *r.Error)
		return
	}
	b, _ := json.MarshalIndent(r.Results, "    ", "  ")
	fmt.Printf("  ✓ %s: %s\n", label, string(b))
}
