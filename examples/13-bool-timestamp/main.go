// 13-bool-timestamp — boolean and timestamp column types.
//
// Run: go run .
//
// What it does:
//  1. Creates view "sessions" with PK token, columns active(boolean), created(timestamp), user_id(string)
//  2. Publishes events with active=true/false, created=RFC 3339 timestamps
//  3. Queries WHERE active = true, WHERE active = false
//
// The program checks every result and exits non-zero if one is wrong.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"strings"
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
		return fmt.Errorf("NewEmbedded: %w", err)
	}
	defer eng.Close()

	js, err := jetstream.New(eng.NC())
	if err != nil {
		return fmt.Errorf("JetStream: %w", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "session-stream", Subjects: []string{"sessions.>"},
	}); err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
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
			return fmt.Errorf("Publish: %w", err)
		}
		fmt.Printf("  Published: %s\n", e)
	}

	if _, err := waitForRows(ctx, eng, "SELECT token FROM sessions WHERE token != ''", len(events)); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("── WHERE active = true ──")
	rows, err := waitForRows(ctx, eng, "SELECT * FROM sessions WHERE active = true", 2)
	if err != nil {
		return err
	}
	if got := column(rows, "token"); got != "tok-aaa,tok-bbb" {
		return fmt.Errorf("active sessions = %s, want tok-aaa,tok-bbb", got)
	}
	for _, r := range rows {
		fmt.Printf("  token=%v active=%v created=%v user_id=%v\n", r["token"], r["active"], r["created"], r["user_id"])
	}

	fmt.Println()
	fmt.Println("── WHERE active = false ──")
	rows, err = waitForRows(ctx, eng, "SELECT * FROM sessions WHERE active = false", 2)
	if err != nil {
		return err
	}
	if got := column(rows, "token"); got != "tok-ccc,tok-ddd" {
		return fmt.Errorf("inactive sessions = %s, want tok-ccc,tok-ddd", got)
	}
	b, _ := json.MarshalIndent(rows, "    ", "  ") // rows always encode: they came from JSON
	fmt.Printf("  ✓ inactive sessions: %s\n", b)

	// Timestamps come back as RFC 3339 strings, and can be a primary key or
	// a filter value.
	fmt.Println()
	fmt.Println("── WHERE created = '2025-06-03T14:15:00Z' ──")
	rows, err = waitForRows(ctx, eng, "SELECT token, created FROM sessions WHERE created = '2025-06-03T14:15:00Z'", 1)
	if err != nil {
		return err
	}
	if got := fmt.Sprint(rows[0]["token"]); got != "tok-ccc" {
		return fmt.Errorf("session created at 14:15 = %s, want tok-ccc", got)
	}
	fmt.Printf("  ✓ %v\n", rows)

	fmt.Println("\n✅ Boolean and timestamp column types work!")
	return nil
}

// column returns one column of rows as sorted, comma-joined text, so a
// result can be compared regardless of row order.
func column(rows []map[string]any, col string) string {
	vals := make([]string, 0, len(rows))
	for _, r := range rows {
		vals = append(vals, fmt.Sprint(r[col]))
	}
	slices.Sort(vals)
	return strings.Join(vals, ",")
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
