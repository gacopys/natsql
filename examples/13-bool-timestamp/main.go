// 13-bool-timestamp — boolean and timestamp column types.
//
// Run: go run .
//
// What it does:
//  1. Creates view "sessions" with PK token, columns active(boolean), created(timestamp), user_id(string)
//  2. Publishes events with active=true/false, created=ISO8601 timestamps
//  3. Queries WHERE active = true, WHERE active = false

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
				Name:         "sessions",
				SourceStream: "session-stream",
				KeyFields:    []string{"token"},
				Columns: []natsql.ColumnConfig{
					{Name: "token", From: "token", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "active", From: "active", Type: natsql.ColumnTypeBoolean},
					{Name: "created", From: "created", Type: natsql.ColumnTypeTimestamp},
					{Name: "user_id", From: "user_id", Type: natsql.ColumnTypeString},
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
		Name: "session-stream", Subjects: []string{"sessions.>"},
	}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	fmt.Println("✓ Engine started with bool + timestamp columns")

	events := []string{
		`{"token":"tok-aaa","active":true,"created":"2025-06-01T12:00:00Z","user_id":"u1"}`,
		`{"token":"tok-bbb","active":true,"created":"2025-06-02T08:30:00Z","user_id":"u2"}`,
		`{"token":"tok-ccc","active":false,"created":"2025-06-03T14:15:00Z","user_id":"u3"}`,
		`{"token":"tok-ddd","active":false,"created":"2025-06-04T20:45:00Z","user_id":"u4"}`,
	}

	for _, e := range events {
		if _, err := js.Publish(ctx, "sessions.login", []byte(e)); err != nil {
			log.Fatalf("Publish: %v", err)
		}
		fmt.Printf("  Published: %s\n", e)
	}

	time.Sleep(time.Second)

	fmt.Println()
	fmt.Println("── WHERE active = true ──")
	res := eng.Query(ctx, "SELECT * FROM sessions WHERE active = true")
	pretty(res, "active sessions")

	for _, r := range res.Results {
		fmt.Printf("  token=%v active=%v created=%v user_id=%v\n",
			r["token"], r["active"], r["created"], r["user_id"])
	}

	fmt.Println()
	fmt.Println("── WHERE active = false ──")
	res = eng.Query(ctx, "SELECT * FROM sessions WHERE active = false")
	pretty(res, "inactive sessions")

	fmt.Println("\n✅ Boolean and timestamp column types work!")
}

func pretty(r *natsql.QueryResult, label string) {
	if r.Error != nil {
		fmt.Printf("  ✗ %s: %s\n", label, *r.Error)
		return
	}
	b, _ := json.MarshalIndent(r.Results, "    ", "  ")
	fmt.Printf("  ✓ %s (%d rows): %s\n", label, len(r.Results), string(b))
}
