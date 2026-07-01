// 09-key-separator — custom key_separator and sanitized PK values.
//
// Run: go run .
//
// What it does:
//  1. Creates view "tenant_items" with composite key [tenant_id, item_id] and separator "-"
//  2. Publishes events with tenant_ids containing special characters
//  3. Queries by composite PK, demonstrating key sanitization

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
		log.Fatalf("NewEmbedded: %v", err)
	}
	defer eng.Close()

	js, err := jetstream.New(eng.NC())
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "tenant-stream", Subjects: []string{"tenants.>"},
	}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	fmt.Println("✓ Engine started with key_separator '-'")

	events := []struct{ id, tenant, item, label string }{
		{"ev1", "acme", "widget", "Acme Widget"},
		{"ev2", "acme", "gadget", "Acme Gadget"},
		{"ev3", "globex-sales", "widget", "Globex Widget"},
		{"ev4", "foo/bar", "doohickey", "FooBar Doohickey"},
	}

	fmt.Printf("Key separator: \"-\"\n")
	fmt.Printf("Valid KV key chars: [-/_=.a-zA-Z0-9]\n\n")

	for _, ev := range events {
		event := fmt.Sprintf(`{"tenant_id":"%s","item_id":"%s","label":"%s"}`,
			ev.tenant, ev.item, ev.label)
		if _, err := js.Publish(ctx, "tenants.event", []byte(event)); err != nil {
			log.Fatalf("Publish: %v", err)
		}
		fmt.Printf("  Published: tenant_id=%s (raw key part would need sanitization for KV)\n", ev.tenant)
	}

	time.Sleep(time.Second)

	fmt.Println()
	fmt.Println("── Query: WHERE tenant_id='acme' AND item_id='widget' ──")
	res := eng.Query(ctx, "SELECT * FROM tenant_items WHERE tenant_id = 'acme' AND item_id = 'widget'")
	pretty(res, "acme:widget")

	fmt.Println("── Query: WHERE tenant_id='globex-sales' AND item_id='widget' ──")
	res = eng.Query(ctx, "SELECT * FROM tenant_items WHERE tenant_id = 'globex-sales' AND item_id = 'widget'")
	pretty(res, "globex-sales:widget")

	fmt.Println("── Query: WHERE tenant_id='foo/bar' AND item_id='doohickey' ──")
	res = eng.Query(ctx, "SELECT * FROM tenant_items WHERE tenant_id = 'foo/bar' AND item_id = 'doohickey'")
	pretty(res, "foo/bar:doohickey")

	fmt.Println("\n✅ Custom key_separator works (KV keys use \"-\" between PK parts)!")
}

func pretty(r *natsql.QueryResult, label string) {
	if r.Error != nil {
		fmt.Printf("  ✗ %s: %s\n", label, *r.Error)
		return
	}
	b, _ := json.MarshalIndent(r.Results, "    ", "  ")
	fmt.Printf("  ✓ %s: %s\n", label, string(b))
}
