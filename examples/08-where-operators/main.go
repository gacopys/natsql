// 08-where-operators — WHERE IN, WHERE !=, and full table scan queries.
//
// Run: go run .
//
// What it does:
//  1. Creates view "products" with PK id, columns name/price/status
//  2. Publishes 6 products with different statuses and prices
//  3. Demonstrates WHERE IN, WHERE !=, and WHERE on non-PK column
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
				Name:         "products",
				SourceStream: "product-stream",
				KeyFields:    []string{"id"},
				Columns: []natsql.ColumnConfig{
					{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsql.ColumnTypeString},
					{Name: "price", From: "price", Type: natsql.ColumnTypeNumber},
					{Name: "status", From: "status", Type: natsql.ColumnTypeString},
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
		Name: "product-stream", Subjects: []string{"products.>"},
	}); err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	fmt.Println("✓ Engine started")

	events := []string{
		`{"id":"p1","name":"Widget","price":9.99,"status":"active"}`,
		`{"id":"p2","name":"Gadget","price":24.99,"status":"active"}`,
		`{"id":"p3","name":"Doohickey","price":9.99,"status":"pending"}`,
		`{"id":"p4","name":"Thingamabob","price":49.99,"status":"deleted"}`,
		`{"id":"p5","name":"Whatsit","price":19.99,"status":"pending"}`,
		`{"id":"p6","name":"Contraption","price":99.99,"status":"deleted"}`,
	}

	for _, e := range events {
		if _, err := js.Publish(ctx, "products.created", []byte(e)); err != nil {
			return fmt.Errorf("Publish: %w", err)
		}
	}
	fmt.Println("Published 6 products")

	if _, err := waitForRows(ctx, eng, "SELECT id FROM products WHERE id != ''", len(events)); err != nil {
		return err
	}

	queries := []struct {
		title, sql string
		wantIDs    string
	}{
		{"WHERE IN ('active','pending')", "SELECT * FROM products WHERE status IN ('active','pending')", "p1,p2,p3,p5"},
		{"WHERE != 'deleted'", "SELECT * FROM products WHERE status != 'deleted'", "p1,p2,p3,p5"},
		{"WHERE price = 9.99 (non-PK full scan)", "SELECT * FROM products WHERE price = 9.99", "p1,p3"},
		{"WHERE id IN ('p2','p6','nope') (PK lookups)", "SELECT * FROM products WHERE id IN ('p2','p6','nope')", "p2,p6"},
	}
	for _, q := range queries {
		fmt.Printf("── %s ──\n", q.title)
		res := eng.Query(ctx, q.sql)
		if res.Error != nil {
			return fmt.Errorf("%s: %s", q.sql, *res.Error)
		}
		if got := column(res.Results, "id"); got != q.wantIDs {
			return fmt.Errorf("%s: ids %s, want %s", q.sql, got, q.wantIDs)
		}
		b, _ := json.MarshalIndent(res.Results, "    ", "  ") // rows always encode: they came from JSON
		fmt.Printf("  ✓ %d rows: %s\n", len(res.Results), string(b))
	}

	fmt.Println("✅ WHERE operators work correctly!")
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
