// 04-multiple-views — two independent materialized views sharing one engine.
//
// Run: go run .
//
// What it does:
//  1. Creates two views: "users" and "products" with different schemas
//  2. Both views run in the same engine, use the same bucket
//  3. Publish events to both streams
//  4. Query both views independently
//  5. Shows that views are isolated by name prefix in KV
//
// The program checks every result and exits non-zero if one is wrong.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	// Two independent views in one config
	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "users",
				SourceStream: "user-stream",
				KeyFields:    []string{"user_id"},
				Columns: []natsql.ColumnConfig{
					{Name: "user_id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsql.ColumnTypeString},
					{Name: "role", From: "role", Type: natsql.ColumnTypeString},
				},
			},
			{
				Name:         "products",
				SourceStream: "product-stream",
				KeyFields:    []string{"sku"},
				Columns: []natsql.ColumnConfig{
					{Name: "sku", From: "sku", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "title", From: "title", Type: natsql.ColumnTypeString},
					{Name: "price", From: "price", Type: natsql.ColumnTypeNumber},
					{Name: "in_stock", From: "in_stock", Type: natsql.ColumnTypeBoolean},
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

	// Create streams before starting engine
	js, err := jetstream.New(eng.NC())
	if err != nil {
		return fmt.Errorf("JetStream: %w", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: "user-stream", Subjects: []string{"users.>"}}); err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: "product-stream", Subjects: []string{"products.>"}}); err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	fmt.Print("✓ Engine started with 2 views\n\n")

	// Publish user events
	for _, u := range []string{
		`{"id": "u1", "name": "Alice", "role": "admin"}`,
		`{"id": "u2", "name": "Bob", "role": "viewer"}`,
	} {
		if _, err := js.Publish(ctx, "users.created", []byte(u)); err != nil {
			return fmt.Errorf("Publish user: %w", err)
		}
	}
	fmt.Println("  Published 2 user events")

	// Publish product events
	for _, p := range []string{
		`{"sku": "SKU-001", "title": "Widget", "price": 9.99, "in_stock": true}`,
		`{"sku": "SKU-002", "title": "Gadget", "price": 24.99, "in_stock": false}`,
		`{"sku": "SKU-003", "title": "Doohickey", "price": 49.99, "in_stock": true}`,
	} {
		if _, err := js.Publish(ctx, "products.created", []byte(p)); err != nil {
			return fmt.Errorf("Publish product: %w", err)
		}
	}
	fmt.Print("  Published 3 product events\n\n")

	// Each view holds only its own rows
	if _, err := waitForRows(ctx, eng, "SELECT * FROM users WHERE user_id != ''", 2); err != nil {
		return err
	}
	if _, err := waitForRows(ctx, eng, "SELECT * FROM products WHERE sku != ''", 3); err != nil {
		return err
	}

	// Query users
	fmt.Println("── Users ──")
	rows, err := waitForRows(ctx, eng, "SELECT * FROM users WHERE user_id = 'u1'", 1)
	if err != nil {
		return err
	}
	if err := expectRow(rows[0], map[string]string{"user_id": "u1", "name": "Alice", "role": "admin"}); err != nil {
		return fmt.Errorf("Alice: %w", err)
	}
	pretty(rows, "Alice")

	rows, err = waitForRows(ctx, eng, "SELECT name, role FROM users WHERE user_id = 'u2'", 1)
	if err != nil {
		return err
	}
	if err := expectRow(rows[0], map[string]string{"name": "Bob", "role": "viewer"}); err != nil {
		return fmt.Errorf("Bob: %w", err)
	}
	if len(rows[0]) != 2 {
		return fmt.Errorf("Bob: projection returned %d columns, want 2", len(rows[0]))
	}
	pretty(rows, "Bob (projected)")

	// Query products
	fmt.Println("── Products ──")
	rows, err = waitForRows(ctx, eng, "SELECT * FROM products WHERE sku = 'SKU-002'", 1)
	if err != nil {
		return err
	}
	if err := expectRow(rows[0], map[string]string{"title": "Gadget", "price": "24.99", "in_stock": "false"}); err != nil {
		return fmt.Errorf("Gadget: %w", err)
	}
	pretty(rows, "Gadget")

	// Query a non-existent view — should error clearly
	res := eng.Query(ctx, "SELECT * FROM nonexistent WHERE id = 'x'")
	if res.Error == nil || !strings.Contains(*res.Error, `view "nonexistent" not found`) {
		return fmt.Errorf("unknown view: got %+v, want a not-found error", res)
	}
	fmt.Printf("  ✓ Unknown view error: %s\n", *res.Error)

	// Query a non-existent column — should error clearly
	res = eng.Query(ctx, "SELECT fake_column FROM users WHERE user_id = 'u1'")
	if res.Error == nil || !strings.Contains(*res.Error, `column "fake_column" not found`) {
		return fmt.Errorf("unknown column: got %+v, want a not-found error", res)
	}
	fmt.Printf("  ✓ Unknown column error: %s\n", *res.Error)

	fmt.Println("\n✅ Multiple views work independently in one engine!")
	return nil
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
