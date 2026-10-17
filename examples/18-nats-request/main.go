// 18-nats-request — querying over NATS request-reply.
//
// Run: go run .
//
// What it does:
//  1. Creates view "metrics" and publishes three events
//  2. Sends SQL queries as NATS requests to the "natsql.query" subject
//  3. Decodes the JSON replies — no HTTP client involved
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

	"github.com/nats-io/nats.go"
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
		return fmt.Errorf("NewEmbedded: %w", err)
	}
	defer eng.Close()

	nc := eng.NC()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("JetStream: %w", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "metrics-stream", Subjects: []string{"metrics.>"},
	}); err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	fmt.Println("✓ Engine started (all queries below use NATS request-reply)")

	events := []string{
		`{"id":"m1","value":42,"label":"cpu_temp"}`,
		`{"id":"m2","value":99,"label":"mem_usage"}`,
		`{"id":"m3","value":15,"label":"disk_io"}`,
	}
	for _, e := range events {
		if _, err := js.Publish(ctx, "metrics.event", []byte(e)); err != nil {
			return fmt.Errorf("Publish: %w", err)
		}
		fmt.Printf("  Published: %s\n", e)
	}
	fmt.Println()

	queries := []struct {
		sql  string
		want []string // each row's columns, sorted and joined as text
	}{
		{"SELECT * FROM metrics WHERE id = 'm1'", []string{"id=m1 label=cpu_temp value=42"}},
		{"SELECT * FROM metrics WHERE id != ''", []string{"id=m1 label=cpu_temp value=42", "id=m2 label=mem_usage value=99", "id=m3 label=disk_io value=15"}},
		{"SELECT value, label FROM metrics WHERE id = 'm3'", []string{"label=disk_io value=15"}},
	}

	for _, q := range queries {
		rows, err := waitForNATSRows(ctx, nc, q.sql, len(q.want))
		if err != nil {
			return err
		}
		got := make([]string, 0, len(rows))
		for _, row := range rows {
			got = append(got, describe(row))
		}
		slices.Sort(got)
		if !slices.Equal(got, q.want) {
			return fmt.Errorf("%s returned %q, want %q", q.sql, got, q.want)
		}
		b, _ := json.MarshalIndent(rows, "      ", "  ") // rows always encode: they came from JSON
		fmt.Printf("  ── NATS request: %q ──\n", q.sql)
		fmt.Printf("  ✓ Response: %s\n\n", string(b))
	}

	// Errors come back in the same envelope.
	result, err := request(ctx, nc, "SELECT * FROM nowhere WHERE id = 'x'")
	if err != nil {
		return err
	}
	if result.Error == nil || !strings.Contains(*result.Error, `view "nowhere" not found`) {
		return fmt.Errorf("unknown view: got %+v, want a not-found error", result)
	}
	fmt.Printf("  ✓ Error reply: %s\n", *result.Error)

	fmt.Println("\n✅ NATS request-reply queries work without HTTP!")
	return nil
}

// describe renders a row as "col=value" pairs in column order.
func describe(row map[string]any) string {
	cols := slices.Sorted(func(yield func(string) bool) {
		for k := range row {
			if !yield(k) {
				return
			}
		}
	})
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		parts = append(parts, fmt.Sprintf("%s=%v", c, row[c]))
	}
	return strings.Join(parts, " ")
}

// request sends sql to the natsql.query subject and decodes the reply.
func request(ctx context.Context, nc *nats.Conn, sql string) (*natsql.QueryResult, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	msg, err := nc.RequestWithContext(reqCtx, "natsql.query", []byte(sql))
	if err != nil {
		return nil, fmt.Errorf("request %q: %w", sql, err)
	}
	var result natsql.QueryResult
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		return nil, fmt.Errorf("decoding reply to %q: %w", sql, err)
	}
	return &result, nil
}

// waitForNATSRows sends sql over NATS until it returns want rows, so the
// example doesn't depend on how quickly events are materialized. It gives up
// after 10s.
func waitForNATSRows(ctx context.Context, nc *nats.Conn, sql string, want int) ([]map[string]any, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		result, err := request(ctx, nc, sql)
		if err != nil {
			return nil, err
		}
		if result.Error != nil {
			return nil, fmt.Errorf("%s: %s", sql, *result.Error)
		}
		if len(result.Results) == want {
			return result.Results, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("%s: got %d rows, want %d", sql, len(result.Results), want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
