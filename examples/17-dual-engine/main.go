// 17-dual-engine — two independent engines in one process.
//
// Run: go run .
//
// What it does:
//  1. Starts two embedded engines, each with its own NATS server, store and HTTP port
//  2. Materializes "users" in engine 1 and "orders" in engine 2
//  3. Queries both, and checks that neither sees the other's view
//
// The program checks every result and exits non-zero if one is wrong.

package main

import (
	"context"
	"encoding/json"
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
	ctx := context.Background()

	userCfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "users",
				SourceStream: "users-stream",
				KeyFields:    []string{"id"},
				Columns: []natsql.ColumnConfig{
					{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsql.ColumnTypeString},
				},
			},
		},
	}

	orderCfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "orders",
				SourceStream: "orders-stream",
				KeyFields:    []string{"id"},
				Columns: []natsql.ColumnConfig{
					{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "total", From: "total", Type: natsql.ColumnTypeNumber},
					{Name: "status", From: "status", Type: natsql.ColumnTypeString},
				},
			},
		},
	}

	fmt.Println("═══ Engine 1: users (port 8081) ═══")
	eng1, js1, err := startEngine(ctx, userCfg, 8081, "users-stream", "users.>")
	if err != nil {
		return fmt.Errorf("engine 1: %w", err)
	}
	defer eng1.Close()
	fmt.Println("  ✓ Engine 1 started on port 8081")

	fmt.Println()
	fmt.Println("═══ Engine 2: orders (port 8082) ═══")
	eng2, js2, err := startEngine(ctx, orderCfg, 8082, "orders-stream", "orders.>")
	if err != nil {
		return fmt.Errorf("engine 2: %w", err)
	}
	defer eng2.Close()
	fmt.Println("  ✓ Engine 2 started on port 8082")

	fmt.Println()
	fmt.Println("Publishing to both engines...")
	if _, err := js1.Publish(ctx, "users.created", []byte(`{"id":"u1","name":"Alice"}`)); err != nil {
		return fmt.Errorf("Publish (1): %w", err)
	}
	if _, err := js2.Publish(ctx, "orders.created", []byte(`{"id":"o1","total":99.99,"status":"shipped"}`)); err != nil {
		return fmt.Errorf("Publish (2): %w", err)
	}

	fmt.Println()
	fmt.Println("── Query Engine 1 (users) ──")
	rows, err := waitForRows(ctx, eng1, "SELECT * FROM users WHERE id = 'u1'", 1)
	if err != nil {
		return fmt.Errorf("engine 1: %w", err)
	}
	if got := fmt.Sprint(rows[0]["name"]); got != "Alice" {
		return fmt.Errorf("engine 1: u1 name = %q, want Alice", got)
	}
	b, _ := json.Marshal(rows) // rows always encode: they came from JSON
	fmt.Printf("  ✓ %s\n", b)

	fmt.Println("── Query Engine 2 (orders) ──")
	rows, err = waitForRows(ctx, eng2, "SELECT * FROM orders WHERE id = 'o1'", 1)
	if err != nil {
		return fmt.Errorf("engine 2: %w", err)
	}
	if got := fmt.Sprint(rows[0]["status"]); got != "shipped" {
		return fmt.Errorf("engine 2: o1 status = %q, want shipped", got)
	}
	b, _ = json.Marshal(rows) // rows always encode: they came from JSON
	fmt.Printf("  ✓ %s\n", b)

	// Each engine has its own NATS server and store, so neither sees the
	// other's view.
	fmt.Println("── Isolation ──")
	if res := eng1.Query(ctx, "SELECT * FROM orders WHERE id = 'o1'"); res.Error == nil || !strings.Contains(*res.Error, "not found") {
		return fmt.Errorf("engine 1 can query engine 2's view: %+v", res)
	}
	if res := eng2.Query(ctx, "SELECT * FROM users WHERE id = 'u1'"); res.Error == nil || !strings.Contains(*res.Error, "not found") {
		return fmt.Errorf("engine 2 can query engine 1's view: %+v", res)
	}
	fmt.Println("  ✓ Neither engine sees the other's view")

	s1, s2 := eng1.Stats(), eng2.Stats()
	fmt.Printf("\n  Engine 1: %d views, HTTP %s\n", s1.Views, s1.HTTPAddr)
	fmt.Printf("  Engine 2: %d views, HTTP %s\n", s2.Views, s2.HTTPAddr)

	fmt.Println("\n✅ Two independent engines run side-by-side without interference!")
	return nil
}

// startEngine starts an embedded engine serving HTTP on port, after creating
// its source stream.
func startEngine(ctx context.Context, cfg *natsql.Config, port int, stream, subjects string) (*natsql.Engine, jetstream.JetStream, error) {
	eng, err := natsql.NewEmbedded(cfg, natsql.WithQueryPort(port))
	if err != nil {
		return nil, nil, fmt.Errorf("NewEmbedded: %w", err)
	}
	js, err := jetstream.New(eng.NC())
	if err != nil {
		eng.Close()
		return nil, nil, fmt.Errorf("JetStream: %w", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: stream, Subjects: []string{subjects}}); err != nil {
		eng.Close()
		return nil, nil, fmt.Errorf("CreateStream: %w", err)
	}
	if err := eng.Start(ctx); err != nil {
		eng.Close()
		return nil, nil, fmt.Errorf("Start: %w", err)
	}
	return eng, js, nil
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
