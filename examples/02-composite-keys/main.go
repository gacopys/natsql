// 02-composite-keys — materialized view with multi-field PK and nested JSON paths.
//
// Run: go run .
//
// What it does:
//  1. Creates a view "orders" with composite key (org_id + order_id)
//  2. Column mapping uses dot paths to extract nested fields from events
//  3. Publishes events with nested JSON structure
//  4. Queries by different key combinations
//
// The program checks every result and exits non-zero if one is wrong.

package main

import (
	"context"
	"encoding/json"
	"errors"
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
	// Config with composite key (key_fields: [org_id, order_id]) + nested JSON paths
	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "orders",
				SourceStream: "orders-stream",
				KeyFields:    []string{"org_id", "order_id"},
				KeySeparator: "-",
				Columns: []natsql.ColumnConfig{
					{Name: "org_id", From: "org.id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "order_id", From: "order.id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "total", From: "order.total", Type: natsql.ColumnTypeNumber},
					{Name: "status", From: "order.status", Type: natsql.ColumnTypeString},
					{Name: "customer", From: "customer.name", Type: natsql.ColumnTypeString},
				},
			},
		},
	}

	ctx := context.Background()

	// Create engine but don't start yet
	eng, err := natsql.NewEmbedded(cfg)
	if err != nil {
		return fmt.Errorf("NewEmbedded: %w", err)
	}
	defer eng.Close()

	// Create stream before starting engine
	js, err := jetstream.New(eng.NC())
	if err != nil {
		return fmt.Errorf("JetStream: %w", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "orders-stream",
		Subjects: []string{"orders.>"},
	}); err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	fmt.Println("✓ Engine started")

	// Publish events with nested JSON — column mapping uses dot paths
	events := []string{
		`{"org": {"id": "acme"}, "order": {"id": "ord-1", "total": 42.50, "status": "shipped"}, "customer": {"name": "Alice"}}`,
		`{"org": {"id": "acme"}, "order": {"id": "ord-2", "total": 99.99, "status": "pending"}, "customer": {"name": "Bob"}}`,
		`{"org": {"id": "globex"}, "order": {"id": "ord-3", "total": 15.00, "status": "shipped"}, "customer": {"name": "Carol"}}`,
	}
	for _, e := range events {
		if _, err := js.Publish(ctx, "orders.created", []byte(e)); err != nil {
			return fmt.Errorf("Publish: %w", err)
		}
		fmt.Println("  Published:", e)
	}

	// Query by full composite key (both key fields in WHERE) — one KV lookup
	rows, err := waitForRows(ctx, eng, "SELECT * FROM orders WHERE org_id = 'acme' AND order_id = 'ord-1'", 1)
	if err != nil {
		return err
	}
	if err := expectRow(rows[0], map[string]string{"org_id": "acme", "order_id": "ord-1", "total": "42.50", "status": "shipped", "customer": "Alice"}); err != nil {
		return fmt.Errorf("acme:ord-1: %w", err)
	}
	pretty(rows, "acme:ord-1")

	// Query all orders for one org (only part of the key → full scan of the view)
	rows, err = waitForRows(ctx, eng, "SELECT customer, total FROM orders WHERE org_id = 'acme'", 2)
	if err != nil {
		return err
	}
	if got := column(rows, "customer"); got != "Alice,Bob" {
		return fmt.Errorf("acme orders: customers %s, want Alice,Bob", got)
	}
	if _, ok := rows[0]["status"]; ok {
		return errors.New("acme orders: projection returned the unselected column status")
	}
	pretty(rows, "acme orders (projected columns)")

	// The other org's order is separate
	if _, err := waitForRows(ctx, eng, "SELECT * FROM orders WHERE org_id = 'globex' AND order_id = 'ord-3'", 1); err != nil {
		return err
	}

	fmt.Printf("\n✓ Stats: %d views, ready=%v\n", eng.Stats().Views, eng.Stats().Ready)
	fmt.Println("✅ Composite keys work with nested JSON paths!")
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

func pretty(rows []map[string]any, label string) {
	b, _ := json.MarshalIndent(rows, "    ", "  ") // rows always encode: they came from JSON
	fmt.Printf("  ✓ %s: %s\n", label, string(b))
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
