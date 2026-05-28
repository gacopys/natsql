// 01-hello-natsql — simplest possible natsql demo.
//
// Run: go run .
//
// What it does:
//  1. Starts an embedded NATS server (no infrastructure needed)
//  2. Creates one materialized view "users" over stream "events"
//  3. Publishes a JSON event
//  4. Queries the materialized row via SQL
//  5. Queries via HTTP
//  6. Prints the Engine stats
//  7. Cleans up

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"natsql"
)

func main() {
	// ── Config: one materialized view "users" ──────────────────────
	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "users",
				SourceStream: "events",
				KeyFields:    []string{"user_id"},
				Columns: []natsql.ColumnConfig{
					{Name: "user_id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsql.ColumnTypeString},
					{Name: "email", From: "email", Type: natsql.ColumnTypeString},
				},
			},
		},
	}

	ctx := context.Background()

	// ── Step 1: Create engine but don't start yet ──────────────────
	eng, err := natsql.NewEmbedded(cfg)
	if err != nil {
		log.Fatalf("NewEmbedded: %v", err)
	}
	defer eng.Close()

	// ── Step 2: Create source stream *before* starting engine ──────
	nc := eng.NC()
	js, _ := jetstream.New(nc)

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "events",
		Subjects: []string{"events.>"},
	})
	if err != nil {
		log.Fatalf("CreateStream: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	fmt.Println("✓ Engine started with embedded NATS")

	// ── Step 3: Publish events ──────────────────────────────────────
	event := `{"id": "abc123", "name": "Alice", "email": "alice@example.com"}`
	_, err = js.Publish(ctx, "events.user-created", []byte(event))
	if err != nil {
		log.Fatalf("Publish: %v", err)
	}
	fmt.Println("✓ Published:", event)

	// ── Step 4: Wait for materializer, then query via SQL ──────────
	time.Sleep(time.Second)

	res := eng.Query(ctx, "SELECT * FROM users WHERE user_id = 'abc123'")
	if res.Error != nil {
		log.Fatalf("SQL query error: %s", *res.Error)
	}
	fmt.Printf("✓ SQL query: %+v\n", res.Results)

	// ── Step 5: Query via HTTP ─────────────────────────────────────
	body, _ := json.Marshal(map[string]string{"sql": "SELECT * FROM users WHERE user_id = 'abc123'"})
	rsp, err := http.Post("http://127.0.0.1:8080/api/v1/query", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatalf("HTTP query: %v", err)
	}
	var httpResult map[string]any
	json.NewDecoder(rsp.Body).Decode(&httpResult)
	rsp.Body.Close()
	fmt.Printf("✓ HTTP query: %+v\n", httpResult)

	// ── Step 6: Stats ──────────────────────────────────────────────
	stats := eng.Stats()
	fmt.Printf("✓ Stats: started=%v views=%d goroutines=%d\n", stats.Started, stats.Views, stats.Goroutines)
	fmt.Println("\n✅ natsql works end-to-end with zero infrastructure!")
}
