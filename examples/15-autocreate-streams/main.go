// 15-autocreate-streams — simulating --create-streams flag and stream existence handling.
//
// Run: go run .
//
// What it does:
//  1. Starts an engine on a persistent store, creates its source stream manually, then starts it
//  2. Restarts on the same store and checks that the stream and the view survived
//  3. Shows that Start fails clearly when the source stream doesn't exist
//
// The program checks every result and exits non-zero if one is wrong.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
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

	// Both engines share one store directory, so the second one restarts on
	// the first one's streams and views. Without a store_dir, every embedded
	// engine gets a private temporary store that is deleted on Close.
	storeDir, err := os.MkdirTemp("", "natsql-example-15-")
	if err != nil {
		return fmt.Errorf("MkdirTemp: %w", err)
	}
	defer os.RemoveAll(storeDir)

	cfg := &natsql.Config{
		NATS: natsql.NATSConfig{StoreDir: storeDir},
		Views: []natsql.ViewConfig{
			{
				Name:         "inventory",
				SourceStream: "inventory-stream",
				KeyFields:    []string{"sku"},
				Columns: []natsql.ColumnConfig{
					{Name: "sku", From: "sku", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "qty", From: "qty", Type: natsql.ColumnTypeNumber},
				},
			},
		},
	}

	fmt.Println("═══ Scenario: manual stream creation (--create-streams) ═══")
	if err := firstRun(ctx, cfg); err != nil {
		return err
	}

	fmt.Println("═══ Scenario: engine detects existing stream on restart ═══")
	if err := restart(ctx, cfg); err != nil {
		return err
	}

	fmt.Println("═══ Scenario: source stream missing ═══")
	missing := *cfg
	missing.NATS.StoreDir = "" // a fresh, empty store
	if err := startWithoutStream(ctx, &missing); err != nil {
		return err
	}

	fmt.Println("\n✅ --create-streams workflow works!")
	return nil
}

func firstRun(ctx context.Context, cfg *natsql.Config) error {
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
		Name: "inventory-stream", Subjects: []string{"inventory.>"},
	}); err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}
	fmt.Println("  ✓ Created stream 'inventory-stream' manually")

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	fmt.Println("  ✓ Engine started — stream already existed")

	if _, err := js.Publish(ctx, "inventory.update", []byte(`{"sku":"SKU-100","qty":42}`)); err != nil {
		return fmt.Errorf("Publish: %w", err)
	}
	rows, err := waitForRows(ctx, eng, "SELECT * FROM inventory WHERE sku = 'SKU-100'", 1)
	if err != nil {
		return err
	}
	if got := fmt.Sprint(rows[0]["qty"]); got != "42" {
		return fmt.Errorf("SKU-100 qty = %s, want 42", got)
	}
	b, _ := json.Marshal(rows) // rows always encode: they came from JSON
	fmt.Printf("  ✓ Query result: %s\n\n", b)
	return nil
}

func restart(ctx context.Context, cfg *natsql.Config) error {
	eng, err := natsql.NewEmbedded(cfg)
	if err != nil {
		return fmt.Errorf("NewEmbedded: %w", err)
	}
	defer eng.Close()

	js, err := jetstream.New(eng.NC())
	if err != nil {
		return fmt.Errorf("JetStream: %w", err)
	}
	if _, err := js.Stream(ctx, "inventory-stream"); err != nil {
		return fmt.Errorf("stream missing after restart (it would need --create-streams): %w", err)
	}
	fmt.Println("  ✓ Stream 'inventory-stream' FOUND — safe to start engine")

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	fmt.Println("  ✓ Engine started on existing stream")

	// The row is there immediately: it was stored before the restart.
	res := eng.Query(ctx, "SELECT * FROM inventory WHERE sku = 'SKU-100'")
	if res.Error != nil || len(res.Results) != 1 || fmt.Sprint(res.Results[0]["qty"]) != "42" {
		return fmt.Errorf("expected the row materialized before the restart, got %+v", res)
	}
	b, _ := json.Marshal(res.Results) // rows always encode: they came from JSON
	fmt.Printf("  ✓ Row survived the restart: %s\n\n", b)
	return nil
}

func startWithoutStream(ctx context.Context, cfg *natsql.Config) error {
	eng, err := natsql.NewEmbedded(cfg)
	if err != nil {
		return fmt.Errorf("NewEmbedded: %w", err)
	}
	defer eng.Close()

	err = eng.Start(ctx)
	if err == nil {
		return errors.New("Start succeeded although the source stream doesn't exist")
	}
	if !strings.Contains(err.Error(), "stream not found") {
		return fmt.Errorf("Start without the stream = %w, want a stream-not-found error", err)
	}
	fmt.Printf("  ✓ Start refused: %v\n", err)
	fmt.Println("    (the CLI creates missing streams in embedded mode, or with --create-streams)")
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
