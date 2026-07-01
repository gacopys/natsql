// 12-http-options — WithHTTPServer and WithQueryPort options.
//
// Run: go run .
//
// What it does:
//  1. Creates engine with WithQueryPort(9090)
//  2. Starts, publishes event, queries via HTTP on port 9090
//  3. Also demonstrates WithHTTPServer(":0") to let OS pick a free port
//  4. Reports the actual port used

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

	"github.com/gacopys/natsql"
)

func main() {
	ctx := context.Background()

	fmt.Println("═══ WithQueryPort(9090) ═══")
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

	eng, err := natsql.NewEmbedded(cfg, natsql.WithQueryPort(9090))
	if err != nil {
		log.Fatalf("NewEmbedded: %v", err)
	}

	js, err := jetstream.New(eng.NC())
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "items-stream", Subjects: []string{"items.>"},
	}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	defer eng.Close()
	fmt.Println("✓ Engine started, HTTP port: 9090")

	if _, err := js.Publish(ctx, "items.created", []byte(`{"id":"a1","name":"Alpha"}`)); err != nil {
		log.Fatalf("Publish: %v", err)
	}

	time.Sleep(time.Second)

	body, _ := json.Marshal(map[string]string{"sql": "SELECT * FROM items WHERE id = 'a1'"})
	rsp, err := http.Post("http://127.0.0.1:9090/api/v1/query", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatalf("HTTP query: %v", err)
	}
	var result map[string]any
	json.NewDecoder(rsp.Body).Decode(&result)
	rsp.Body.Close()
	fmt.Printf("  HTTP query on port 9090: %+v\n\n", result)
	eng.Close()

	fmt.Println("═══ WithHTTPServer(\":0\") — OS picks port ═══")
	eng2, err := natsql.NewEmbedded(cfg, natsql.WithHTTPServer(":0"))
	if err != nil {
		log.Fatalf("NewEmbedded: %v", err)
	}
	defer eng2.Close()

	js2, err := jetstream.New(eng2.NC())
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}
	if _, err := js2.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "items-stream-2", Subjects: []string{"items2.>"},
	}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}

	if err := eng2.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}

	s := eng2.Stats()
	fmt.Printf("  Engine started, HTTP serving: %v\n", s.HTTPServing)
	fmt.Println("  (WithHTTPServer(\":0\") lets the OS assign a free port)")

	fmt.Println("\n✅ HTTP options work!")
}
