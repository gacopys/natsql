// 10-source-subject — source_subject filtering across two views sharing one stream.
//
// Run: go run .
//
// What it does:
//  1. Creates one stream "all-events" with subjects "all-events.>"
//  2. Creates two views filtering different subjects (user.created vs user.deleted)
//  3. Publishes events to both subjects
//  4. Verifies each view only sees its subset
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
		return fmt.Errorf("NewEmbedded: %w", err)
	}
	defer eng.Close()

	js, err := jetstream.New(eng.NC())
	if err != nil {
		return fmt.Errorf("JetStream: %w", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "all-events", Subjects: []string{"all-events.>"},
	}); err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	fmt.Println("✓ Engine started with source_subject filtering")

	publish := map[string][]string{
		"all-events.user.created": {`{"id":"u1","name":"Alice"}`, `{"id":"u2","name":"Bob"}`},
		"all-events.user.deleted": {`{"id":"u3","name":"Carol"}`},
		"all-events.user.renamed": {`{"id":"u4","name":"Dave"}`}, // matches neither view
	}
	for _, subject := range []string{"all-events.user.created", "all-events.user.deleted", "all-events.user.renamed"} {
		fmt.Printf("Publishing to %s:\n", subject)
		for _, e := range publish[subject] {
			if _, err := js.Publish(ctx, subject, []byte(e)); err != nil {
				return fmt.Errorf("Publish: %w", err)
			}
			fmt.Printf("  → %s\n", e)
		}
	}

	views := []struct{ view, wantIDs string }{
		{"user_creates", "u1,u2"},
		{"user_deletes", "u3"},
	}
	for _, v := range views {
		fmt.Printf("\n── Query %s ──\n", v.view)
		sql := "SELECT * FROM " + v.view + " WHERE user_id != ''"
		rows, err := waitForRows(ctx, eng, sql, strings.Count(v.wantIDs, ",")+1)
		if err != nil {
			return err
		}
		if got := column(rows, "user_id"); got != v.wantIDs {
			return fmt.Errorf("%s holds %s, want %s", v.view, got, v.wantIDs)
		}
		b, _ := json.MarshalIndent(rows, "    ", "  ") // rows always encode: they came from JSON
		fmt.Printf("  ✓ %d rows: %s\n", len(rows), b)
	}

	fmt.Println("\n✅ source_subject filtering isolates views on the same stream!")
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
