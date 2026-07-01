// 18-nats-request — pure NATS request-reply query without HTTP.
//
// Run: go run .
//
// What it does:
//  1. Uses natsql.NewWithNATS(nc, cfg) without HTTP options
//  2. Creates stream, starts engine
//  3. Uses nc.Request("natsql.query", sql, timeout) to query
//  4. Parses JSON response — no HTTP needed

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
	ctx := context.Background()

	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "metrics",
				SourceStream: "metrics-stream",
				KeyFields:    []string{"id"},
				Columns: []natsql.ColumnConfig{
					{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "value", From: "value", Type: natsql.ColumnTypeNumber},
					{Name: "label", From: "label", Type: natsql.ColumnTypeString},
				},
			},
		},
	}

	eng, err := natsql.NewEmbedded(cfg)
	if err != nil {
		log.Fatalf("NewEmbedded: %v", err)
	}
	defer eng.Close()

	nc := eng.NC()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "metrics-stream", Subjects: []string{"metrics.>"},
	}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	fmt.Println("✓ Engine started (no HTTP, NATS request-reply only)")

	events := []string{
		`{"id":"m1","value":42,"label":"cpu_temp"}`,
		`{"id":"m2","value":99,"label":"mem_usage"}`,
		`{"id":"m3","value":15,"label":"disk_io"}`,
	}
	for _, e := range events {
		if _, err := js.Publish(ctx, "metrics.event", []byte(e)); err != nil {
			log.Fatalf("Publish: %v", err)
		}
		fmt.Printf("  Published: %s\n", e)
	}

	time.Sleep(time.Second)

	fmt.Println()

	queries := []string{
		"SELECT * FROM metrics WHERE id = 'm1'",
		"SELECT * FROM metrics WHERE id != ''",
		"SELECT value, label FROM metrics WHERE id = 'm3'",
	}

	for _, sql := range queries {
		msg, err := nc.Request("natsql.query", []byte(sql), 2*time.Second)
		if err != nil {
			log.Fatalf("Request: %v", err)
		}

		var result natsql.QueryResult
		if err := json.Unmarshal(msg.Data, &result); err != nil {
			log.Fatalf("Unmarshal response: %v", err)
		}

		if result.Error != nil {
			fmt.Printf("  ✗ %s → error: %s\n", sql, *result.Error)
			continue
		}

		b, _ := json.MarshalIndent(result.Results, "      ", "  ")
		fmt.Printf("  ── NATS request: %q ──\n", sql)
		fmt.Printf("  Response: %s\n\n", string(b))
	}

	fmt.Println("✅ NATS request-reply queries work without HTTP!")
}
