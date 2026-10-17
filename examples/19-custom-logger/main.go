// 19-custom-logger — structured logging with slog, WithLogger, log levels.
//
// Run: go run .
//
// What it does:
//  1. Creates custom slog.Logger with TextHandler (debug level)
//  2. Creates engine with WithLogger(customLogger)
//  3. Shows startup, materialization, query and embedded NATS server logs appear
//  4. Demonstrates suppressing logs with io.Discard
//
// The program checks every result and exits non-zero if one is wrong.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"strings"
	"sync"
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
				Name:         "logs_demo",
				SourceStream: "log-stream",
				KeyFields:    []string{"id"},
				Columns: []natsql.ColumnConfig{
					{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "msg", From: "msg", Type: natsql.ColumnTypeString},
				},
			},
		},
	}

	fmt.Println("═══ TextHandler with debug level ═══")
	fmt.Println("  (Log output below from TextHandler, including the embedded NATS server's)")
	var captured syncBuffer
	textLogger := slog.New(slog.NewTextHandler(io.MultiWriter(os.Stdout, &captured), &slog.HandlerOptions{Level: slog.LevelDebug}))

	if err := runEngine(ctx, cfg, textLogger, "e1", "hello"); err != nil {
		return err
	}
	logs := captured.String()
	for _, want := range []string{`msg="engine started"`, `msg="executing SQL"`, `component=nats-server`, `msg="engine closed"`} {
		if !strings.Contains(logs, want) {
			return fmt.Errorf("custom logger did not receive a %s line", want)
		}
	}
	fmt.Print("\n  ✓ The custom logger received the engine's and the NATS server's logs\n\n")

	fmt.Println("═══ JSONHandler disabled (io.Discard) ═══")
	fmt.Println("  (No log output below — redirected to io.Discard)")
	noOp := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := runEngine(ctx, cfg, noOp, "e2", "silent"); err != nil {
		return err
	}

	fmt.Println("\n✅ Custom slog.Logger with WithLogger works!")
	fmt.Println("  Use TextHandler for debugging, JSONHandler for production, io.Discard to suppress.")
	return nil
}

// runEngine runs an embedded engine that logs to logger, materializes one
// event and queries it back. Each embedded engine without a store_dir has
// its own private store, so each one creates the source stream.
func runEngine(ctx context.Context, cfg *natsql.Config, logger *slog.Logger, id, msg string) error {
	eng, err := natsql.NewEmbedded(cfg, natsql.WithLogger(logger))
	if err != nil {
		return fmt.Errorf("NewEmbedded: %w", err)
	}
	defer eng.Close()

	js, err := jetstream.New(eng.NC())
	if err != nil {
		return fmt.Errorf("JetStream: %w", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "log-stream", Subjects: []string{"logs.>"},
	}); err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}
	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}

	event := fmt.Sprintf(`{"id":%q,"msg":%q}`, id, msg)
	if _, err := js.Publish(ctx, "logs.evt", []byte(event)); err != nil {
		return fmt.Errorf("Publish: %w", err)
	}

	rows, err := waitForRows(ctx, eng, fmt.Sprintf("SELECT * FROM logs_demo WHERE id = '%s'", id), 1)
	if err != nil {
		return err
	}
	if got := fmt.Sprint(rows[0]["msg"]); got != msg {
		return fmt.Errorf("%s msg = %q, want %q", id, got, msg)
	}
	b, _ := json.Marshal(rows) // rows always encode: they came from JSON
	fmt.Printf("\n  ✓ Query result: %s\n", b)
	return eng.Close() // logs "engine closed" before the deferred Close, which is then a no-op
}

// syncBuffer is a bytes.Buffer that the logger, writing from several
// goroutines, and the example can share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
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
