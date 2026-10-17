// 12-http-options — WithHTTPServer and WithQueryPort options.
//
// Run: go run .
//
// What it does:
//  1. Creates engine with WithQueryPort(9090)
//  2. Starts, publishes event, queries via HTTP on port 9090
//  3. Also demonstrates WithHTTPServer(":0") to let OS pick a free port
//  4. Reports the actual port used (Stats().HTTPAddr) and checks /readyz
//
// The program checks every result and exits non-zero if one is wrong.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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

	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "items",
				SourceStream: "items-stream",
				KeyFields:    []string{"id"},
				Columns: []natsql.ColumnConfig{
					{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsql.ColumnTypeString},
				},
			},
		},
	}

	fmt.Println("═══ WithQueryPort(9090) ═══")
	if err := runEngine(ctx, cfg, natsql.WithQueryPort(9090), "a1", "Alpha"); err != nil {
		return fmt.Errorf("WithQueryPort(9090): %w", err)
	}

	fmt.Println("═══ WithHTTPServer(\":0\") — OS picks port ═══")
	if err := runEngine(ctx, cfg, natsql.WithHTTPServer(":0"), "b2", "Beta"); err != nil {
		return fmt.Errorf(`WithHTTPServer(":0"): %w`, err)
	}

	fmt.Println("\n✅ HTTP options work!")
	return nil
}

// runEngine starts an embedded engine with the given HTTP option, publishes
// one item, and reads it back over HTTP.
func runEngine(ctx context.Context, cfg *natsql.Config, httpOption natsql.Option, id, name string) error {
	eng, err := natsql.NewEmbedded(cfg, httpOption)
	if err != nil {
		return fmt.Errorf("NewEmbedded: %w", err)
	}
	defer eng.Close()

	// Each embedded engine without a store_dir gets its own private store,
	// so every engine needs its source stream created.
	js, err := jetstream.New(eng.NC())
	if err != nil {
		return fmt.Errorf("JetStream: %w", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "items-stream", Subjects: []string{"items.>"},
	}); err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	addr := eng.Stats().HTTPAddr
	fmt.Printf("  ✓ Engine started, HTTP serving on %s\n", addr)

	if err := expectStatus(ctx, "http://"+addr+"/readyz", http.StatusOK); err != nil {
		return err
	}
	fmt.Println("  ✓ GET /readyz → 200")

	event := fmt.Sprintf(`{"id":%q,"name":%q}`, id, name)
	if _, err := js.Publish(ctx, "items.created", []byte(event)); err != nil {
		return fmt.Errorf("Publish: %w", err)
	}

	rows, err := waitForHTTPRows(ctx, addr, fmt.Sprintf("SELECT * FROM items WHERE id = '%s'", id), 1)
	if err != nil {
		return err
	}
	if got := fmt.Sprint(rows[0]["name"]); got != name {
		return fmt.Errorf("name = %q, want %q", got, name)
	}
	fmt.Printf("  ✓ HTTP query on %s: %+v\n\n", addr, rows)
	return nil
}

func expectStatus(ctx context.Context, url string, want int) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}
	rsp.Body.Close()
	if rsp.StatusCode != want {
		return fmt.Errorf("GET %s = %d, want %d", url, rsp.StatusCode, want)
	}
	return nil
}

// waitForHTTPRows posts sql to the HTTP query API until it returns want
// rows, so the example doesn't depend on how quickly events are
// materialized. It gives up after 10s.
func waitForHTTPRows(ctx context.Context, addr, sql string, want int) ([]map[string]any, error) {
	body, err := json.Marshal(map[string]string{"sql": sql})
	if err != nil {
		return nil, fmt.Errorf("encoding request: %w", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/api/v1/query", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		rsp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("HTTP query: %w", err)
		}
		var result natsql.QueryResult
		err = json.NewDecoder(rsp.Body).Decode(&result)
		rsp.Body.Close()
		switch {
		case err != nil:
			return nil, fmt.Errorf("decoding HTTP response: %w", err)
		case rsp.StatusCode != http.StatusOK || result.Error != nil:
			return nil, fmt.Errorf("%s: HTTP %d, %+v", sql, rsp.StatusCode, result)
		case len(result.Results) == want:
			return result.Results, nil
		case time.Now().After(deadline):
			return nil, fmt.Errorf("%s: got %d rows, want %d", sql, len(result.Results), want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
