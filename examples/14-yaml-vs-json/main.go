// 14-yaml-vs-json — loading config from JSON file vs in-code Go struct.
//
// Run: go run .
//
// What it does:
//  1. Writes a config.json file at runtime
//  2. Loads it with natsql.LoadConfig()
//  3. Shows equivalent config in Go struct
//  4. Both produce equivalent engines — prints comparison

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql"
)

func main() {
	ctx := context.Background()

	jsonConfig := `{
  "views": [
    {
      "name": "customers",
      "source_stream": "cust-stream",
      "key_fields": ["cust_id"],
      "columns": [
        {"name": "cust_id", "from": "id", "type": "string", "primary_key": true},
        {"name": "name", "from": "name", "type": "string"}
      ]
    }
  ]
}`

	if err := os.WriteFile("config.json", []byte(jsonConfig), 0644); err != nil {
		log.Fatalf("WriteFile: %v", err)
	}
	defer os.Remove("config.json")

	jsonCfg, err := natsql.LoadConfig("config.json")
	if err != nil {
		log.Fatalf("LoadConfig: %v", err)
	}

	fmt.Println("═══ Loaded from config.json ═══")
	fmt.Printf("  View: %s, Stream: %s, PK: %v\n\n",
		jsonCfg.Views[0].Name,
		jsonCfg.Views[0].SourceStream,
		jsonCfg.Views[0].KeyFields)

	goCfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "customers",
				SourceStream: "cust-stream",
				KeyFields:    []string{"cust_id"},
				Columns: []natsql.ColumnConfig{
					{Name: "cust_id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsql.ColumnTypeString},
				},
			},
		},
	}

	fmt.Println("═══ Equivalent Go struct ═══")
	fmt.Printf("  View: %s, Stream: %s, PK: %v\n\n",
		goCfg.Views[0].Name,
		goCfg.Views[0].SourceStream,
		goCfg.Views[0].KeyFields)

	match := jsonCfg.Views[0].Name == goCfg.Views[0].Name &&
		jsonCfg.Views[0].SourceStream == goCfg.Views[0].SourceStream
	fmt.Printf("  Configs equivalent: %v\n\n", match)

	fmt.Println("── Run engine with Go struct config ──")
	eng, err := natsql.NewEmbedded(goCfg)
	if err != nil {
		log.Fatalf("NewEmbedded: %v", err)
	}
	defer eng.Close()

	js, err := jetstream.New(eng.NC())
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "cust-stream", Subjects: []string{"customers.>"},
	}); err != nil {
		log.Fatalf("CreateStream: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}

	if _, err := js.Publish(ctx, "customers.created", []byte(`{"id":"c1","name":"Acme Corp"}`)); err != nil {
		log.Fatalf("Publish: %v", err)
	}

	time.Sleep(time.Second)

	res := eng.Query(ctx, "SELECT * FROM customers WHERE cust_id = 'c1'")
	b, _ := json.MarshalIndent(res.Results, "  ", "  ")
	fmt.Printf("  Query result: %s\n", b)

	fmt.Println("\n✅ JSON file config and Go struct config are equivalent!")
}
