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
	for stream, subject := range map[string]string{"user-stream": "users.>", "product-stream": "products.>"} {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: stream, Subjects: []string{subject}}); err != nil {
			return fmt.Errorf("CreateStream: %w", err)
		}
	}

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	fmt.Print("✓ Engine started with 2 views\n\n")

	if err := publishAll(ctx, js, "users.created", []string{
		`{"id": "u1", "name": "Alice", "role": "admin"}`,
		`{"id": "u2", "name": "Bob", "role": "viewer"}`,
	}); err != nil {
		return err
	}
	fmt.Println("  Published 2 user events")

	if err := publishAll(ctx, js, "products.created", []string{
		`{"sku": "SKU-001", "title": "Widget", "price": 9.99, "in_stock": true}`,
		`{"sku": "SKU-002", "title": "Gadget", "price": 24.99, "in_stock": false}`,
		`{"sku": "SKU-003", "title": "Doohickey", "price": 49.99, "in_stock": true}`,
	}); err != nil {
		return err
	}
	fmt.Print("  Published 3 product events\n\n")

	// Each view holds only its own rows
	if _, err := waitForRows(ctx, eng, "SELECT * FROM users WHERE user_id != ''", 2); err != nil {
		return err
	}
	if _, err := waitForRows(ctx, eng, "SELECT * FROM products WHERE sku != ''", 3); err != nil {
		return err
	}

	checks := []struct {
		heading, label, sql string
		want                map[string]string
	}{
		{"── Users ──", "Alice", "SELECT * FROM users WHERE user_id = 'u1'", map[string]string{"user_id": "u1", "name": "Alice", "role": "admin"}},
		{"", "Bob (projected)", "SELECT name, role FROM users WHERE user_id = 'u2'", map[string]string{"name": "Bob", "role": "viewer"}},
		{"── Products ──", "Gadget", "SELECT * FROM products WHERE sku = 'SKU-002'", map[string]string{"sku": "SKU-002", "title": "Gadget", "price": "24.99", "in_stock": "false"}},
	}
	for _, c := range checks {
		if c.heading != "" {
			fmt.Println(c.heading)
		}
		if err := checkRow(ctx, eng, c.sql, c.label, c.want); err != nil {
			return err
		}
	}

	if err := checkErrors(ctx, eng); err != nil {
		return err
	}

	fmt.Println("\n✅ Multiple views work independently in one engine!")
	return nil
}

// checkErrors checks that unknown views and columns fail with clear errors.
func checkErrors(ctx context.Context, eng *natsql.Engine) error {
	for sql, wantErr := range map[string]string{
		"SELECT * FROM nonexistent WHERE id = 'x'":           `view "nonexistent" not found`,
		"SELECT fake_column FROM users WHERE user_id = 'u1'": `column "fake_column" not found`,
	} {
		res := eng.Query(ctx, sql)
		if res.Error == nil || !strings.Contains(*res.Error, wantErr) {
			return fmt.Errorf("%s: got %+v, want error %q", sql, res, wantErr)
		}
		fmt.Printf("  ✓ Error: %s\n", *res.Error)
	}
	return nil
}

func publishAll(ctx context.Context, js jetstream.JetStream, subject string, events []string) error {
	for _, e := range events {
		if _, err := js.Publish(ctx, subject, []byte(e)); err != nil {
			return fmt.Errorf("Publish to %s: %w", subject, err)
		}
	}
	return nil
}

// checkRow runs sql, expects exactly one row with exactly the wanted
// columns and values, and prints it.
func checkRow(ctx context.Context, eng *natsql.Engine, sql, label string, want map[string]string) error {
	rows, err := waitForRows(ctx, eng, sql, 1)
	if err != nil {
		return err
	}
	if err := expectRow(rows[0], want); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	if len(rows[0]) != len(want) {
		return fmt.Errorf("%s: %d columns, want %d", label, len(rows[0]), len(want))
	}
	pretty(rows, label)
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
