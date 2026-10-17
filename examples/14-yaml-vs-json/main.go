// 14-yaml-vs-json — loading config from YAML and JSON files vs an in-code Go struct.
//
// Run: go run .
//
// What it does:
//  1. Writes the same view config as config.yaml and config.json
//  2. Loads both with natsql.LoadConfig()
//  3. Compares them with the equivalent Go struct
//  4. Runs an engine from the JSON-loaded config and queries it
//
// The program checks every result and exits non-zero if one is wrong.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql/v2"
)

const yamlConfig = `views:
  - name: customers
    source_stream: cust-stream
    key_fields: [cust_id]
    columns:
      - {name: cust_id, from: id, type: string, primary_key: true}
      - {name: name, from: name, type: string}
`

const jsonConfig = `{
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

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "natsql-example-14-")
	if err != nil {
		return fmt.Errorf("MkdirTemp: %w", err)
	}
	defer os.RemoveAll(dir)

	yamlCfg, err := writeAndLoad(filepath.Join(dir, "config.yaml"), yamlConfig)
	if err != nil {
		return err
	}
	jsonCfg, err := writeAndLoad(filepath.Join(dir, "config.json"), jsonConfig)
	if err != nil {
		return err
	}

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
	goCfg.SetDefaults() // LoadConfig applies defaults too

	for name, cfg := range map[string]*natsql.Config{"config.yaml": yamlCfg, "config.json": jsonCfg} {
		if !reflect.DeepEqual(cfg, goCfg) {
			return fmt.Errorf("%s loaded as %+v, want %+v", name, *cfg, *goCfg)
		}
		fmt.Printf("✓ %s matches the Go struct: view %s, stream %s, PK %v\n",
			name, cfg.Views[0].Name, cfg.Views[0].SourceStream, cfg.Views[0].KeyFields)
	}

	fmt.Println("\n── Run engine with the JSON-loaded config ──")
	eng, err := natsql.NewEmbedded(jsonCfg)
	if err != nil {
		return fmt.Errorf("NewEmbedded: %w", err)
	}
	defer eng.Close()

	js, err := jetstream.New(eng.NC())
	if err != nil {
		return fmt.Errorf("JetStream: %w", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "cust-stream", Subjects: []string{"customers.>"},
	}); err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}

	if _, err := js.Publish(ctx, "customers.created", []byte(`{"id":"c1","name":"Acme Corp"}`)); err != nil {
		return fmt.Errorf("Publish: %w", err)
	}

	rows, err := waitForRows(ctx, eng, "SELECT * FROM customers WHERE cust_id = 'c1'", 1)
	if err != nil {
		return err
	}
	if got := fmt.Sprint(rows[0]["name"]); got != "Acme Corp" {
		return fmt.Errorf("c1 name = %q, want %q", got, "Acme Corp")
	}
	b, _ := json.MarshalIndent(rows, "  ", "  ") // rows always encode: they came from JSON
	fmt.Printf("  ✓ Query result: %s\n", b)

	fmt.Println("\n✅ YAML, JSON and Go struct configs are equivalent!")
	return nil
}

func writeAndLoad(path, content string) (*natsql.Config, error) {
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return nil, fmt.Errorf("WriteFile: %w", err)
	}
	cfg, err := natsql.LoadConfig(path)
	if err != nil {
		return nil, fmt.Errorf("LoadConfig(%s): %w", filepath.Base(path), err)
	}
	return cfg, nil
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
