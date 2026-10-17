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
//
// The program checks every result and exits non-zero if one is wrong.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
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
		return fmt.Errorf("NewEmbedded: %w", err)
	}
	defer eng.Close()

	// ── Step 2: Create source stream *before* starting engine ──────
	js, err := jetstream.New(eng.NC())
	if err != nil {
		return fmt.Errorf("JetStream: %w", err)
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "events",
		Subjects: []string{"events.>"},
	})
	if err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	fmt.Println("✓ Engine started with embedded NATS")

	// ── Step 3: Publish events ──────────────────────────────────────
	event := `{"id": "abc123", "name": "Alice", "email": "alice@example.com"}`
	if _, err := js.Publish(ctx, "events.user-created", []byte(event)); err != nil {
		return fmt.Errorf("Publish: %w", err)
	}
	fmt.Println("✓ Published:", event)

	// ── Step 4: Wait for the materializer, then query via SQL ───────
	rows, err := waitForRows(ctx, eng, "SELECT * FROM users WHERE user_id = 'abc123'", 1)
	if err != nil {
		return err
	}
	if err := expectRow(rows[0], map[string]string{"user_id": "abc123", "name": "Alice", "email": "alice@example.com"}); err != nil {
		return fmt.Errorf("SQL query: %w", err)
	}
	fmt.Printf("✓ SQL query: %+v\n", rows)

	// ── Step 5: Query via HTTP ─────────────────────────────────────
	body, err := json.Marshal(map[string]string{"sql": "SELECT * FROM users WHERE user_id = 'abc123'"})
	if err != nil {
		return fmt.Errorf("encoding HTTP request: %w", err)
	}
	rsp, err := http.Post("http://"+eng.Stats().HTTPAddr+"/api/v1/query", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("HTTP query: %w", err)
	}
	defer rsp.Body.Close()
	var httpResult natsql.QueryResult
	if err := json.NewDecoder(rsp.Body).Decode(&httpResult); err != nil {
		return fmt.Errorf("decoding HTTP response: %w", err)
	}
	if rsp.StatusCode != http.StatusOK || httpResult.Error != nil || len(httpResult.Results) != 1 {
		return fmt.Errorf("HTTP query: status %d, result %+v, want one row", rsp.StatusCode, httpResult)
	}
	if err := expectRow(httpResult.Results[0], map[string]string{"user_id": "abc123", "name": "Alice"}); err != nil {
		return fmt.Errorf("HTTP query: %w", err)
	}
	fmt.Printf("✓ HTTP query: %+v\n", httpResult.Results)

	// ── Step 6: Stats ──────────────────────────────────────────────
	stats := eng.Stats()
	if !stats.Ready || stats.Views != 1 {
		return fmt.Errorf("stats = %+v, want a ready engine with 1 view", stats)
	}
	fmt.Printf("✓ Stats: started=%v ready=%v views=%d events=%d\n",
		stats.Started, stats.Ready, stats.Views, stats.ViewStatus[0].EventsProcessed)
	fmt.Println("\n✅ natsql works end-to-end with zero infrastructure!")
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

// expectRow checks columns of a result row, compared as text.
func expectRow(row map[string]any, want map[string]string) error {
	var errs []error
	for col, w := range want {
		if got := fmt.Sprint(row[col]); got != w {
			errs = append(errs, fmt.Errorf("column %s = %s, want %s", col, got, w))
		}
	}
	return errors.Join(errs...)
}
