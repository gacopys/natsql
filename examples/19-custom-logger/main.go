// 19-custom-logger — structured logging with slog, WithLogger, log levels.
//
// Run: go run .
//
// What it does:
//  1. Creates custom slog.Logger with TextHandler (debug level)
//  2. Creates engine with WithLogger(customLogger)
//  3. Shows startup, materialization, and query logs appear
//  4. Demonstrates suppressing logs with io.Discard

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql"
)

func main() {
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
	textLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	fmt.Println("  (Log output below from TextHandler)")

	eng, err := natsql.NewEmbedded(cfg, natsql.WithLogger(textLogger))
	if err != nil {
		log.Fatalf("NewEmbedded: %v", err)
	}
	defer eng.Close()

	js, err := jetstream.New(eng.NC())
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "log-stream", Subjects: []string{"logs.>"},
	}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}

	if _, err := js.Publish(ctx, "logs.evt", []byte(`{"id":"e1","msg":"hello"}`)); err != nil {
		log.Fatalf("Publish: %v", err)
	}

	time.Sleep(time.Second)

	res := eng.Query(ctx, "SELECT * FROM logs_demo WHERE id = 'e1'")
	b, _ := json.Marshal(res.Results)
	fmt.Printf("\n  Query result (logs above): %s\n\n", b)
	eng.Close()

	fmt.Println("═══ JSONHandler disabled (io.Discard) ═══")
	noOp := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))
	fmt.Println("  (No log output below — redirected to io.Discard)")

	eng2, err := natsql.NewEmbedded(cfg, natsql.WithLogger(noOp))
	if err != nil {
		log.Fatalf("NewEmbedded: %v", err)
	}
	defer eng2.Close()

	js2, err := jetstream.New(eng2.NC())
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}
	if _, err := js2.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "log-stream-2", Subjects: []string{"logs2.>"},
	}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}
	if err := eng2.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	if _, err := js2.Publish(ctx, "logs2.evt", []byte(`{"id":"e2","msg":"silent"}`)); err != nil {
		log.Fatalf("Publish: %v", err)
	}

	time.Sleep(time.Second)

	res = eng2.Query(ctx, "SELECT * FROM logs_demo WHERE id = 'e2'")
	b2, _ := json.Marshal(res.Results)
	fmt.Printf("  Query result: %s\n", b2)

	fmt.Println("\n✅ Custom slog.Logger with WithLogger works!")
	fmt.Println("  Use TextHandler for debugging, JSONHandler for production, io.Discard to suppress.")
}
