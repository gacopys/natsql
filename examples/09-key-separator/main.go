// 09-key-separator — custom key_separator and escaped PK values.
//
// Run: go run .
//
// What it does:
//  1. Creates view "tenant_items" with composite key [tenant_id, item_id] and separator "-"
//  2. Publishes events whose key parts contain the separator and other special characters
//  3. Queries by composite PK, showing that such keys stay distinct and reachable
//
// The program checks every result and exits non-zero if one is wrong.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
				Name:         "tenant_items",
				SourceStream: "tenant-stream",
				KeyFields:    []string{"tenant_id", "item_id"},
				KeySeparator: "-",
				Columns: []natsql.ColumnConfig{
					{Name: "tenant_id", From: "tenant_id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "item_id", From: "item_id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "label", From: "label", Type: natsql.ColumnTypeString},
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
		Name: "tenant-stream", Subjects: []string{"tenants.>"},
	}); err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	fmt.Println("✓ Engine started with key_separator '-'")

	// ("globex-sales", "widget") and ("globex", "sales-widget") would both
	// join to "globex-sales-widget"; natsql escapes the separator inside key
	// parts, so they remain two rows.
	events := []struct{ tenant, item, label string }{
		{"acme", "widget", "Acme Widget"},
		{"acme", "gadget", "Acme Gadget"},
		{"globex-sales", "widget", "Globex Sales Widget"},
		{"globex", "sales-widget", "Globex Sales-Widget"},
		{"foo/bar", "doohickey", "FooBar Doohickey"},
		{"ops@example.com", "pager", "Ops Pager"},
	}

	fmt.Printf("Key separator: \"-\"\n\n")
	for _, ev := range events {
		event, err := json.Marshal(map[string]string{"tenant_id": ev.tenant, "item_id": ev.item, "label": ev.label})
		if err != nil {
			return fmt.Errorf("encoding event: %w", err)
		}
		if _, err := js.Publish(ctx, "tenants.event", event); err != nil {
			return fmt.Errorf("Publish: %w", err)
		}
		fmt.Printf("  Published: tenant_id=%q item_id=%q\n", ev.tenant, ev.item)
	}

	if _, err := waitForRows(ctx, eng, "SELECT label FROM tenant_items WHERE label != ''", len(events)); err != nil {
		return fmt.Errorf("every event should be its own row: %w", err)
	}

	fmt.Println()
	for _, ev := range events {
		sql := fmt.Sprintf("SELECT * FROM tenant_items WHERE tenant_id = '%s' AND item_id = '%s'", ev.tenant, ev.item)
		fmt.Printf("── %s ──\n", sql)
		rows, err := waitForRows(ctx, eng, sql, 1)
		if err != nil {
			return err
		}
		if got := fmt.Sprint(rows[0]["label"]); got != ev.label {
			return fmt.Errorf("%s: label %q, want %q", sql, got, ev.label)
		}
		b, _ := json.Marshal(rows) // rows always encode: they came from JSON
		fmt.Printf("  ✓ %s\n", b)
	}

	fmt.Println("\n✅ Custom key_separator works, and keys containing it stay distinct!")
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
