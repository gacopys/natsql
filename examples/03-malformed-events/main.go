// 03-malformed-events — error handling and DLQ (dead letter queue).
//
// Run: go run .
//
// What it does:
//  1. Creates a view "users" with type validation
//  2. Publishes a mix of valid and invalid events:
//     - Valid JSON → materializes normally
//     - Invalid JSON → goes to DLQ
//     - Type mismatch (string instead of number) → goes to DLQ
//     - Missing key field → goes to DLQ
//  3. Reads the DLQ stream to show what was rejected
//  4. Shows that valid events still materialize despite bad ones
//
// The program checks every result and exits non-zero if one is wrong.

package main

import (
	"context"
	"encoding/base64"
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
	// View with strict types (D-16: type mismatch → DLQ)
	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "users",
				SourceStream: "events",
				KeyFields:    []string{"user_id"},
				Columns: []natsql.ColumnConfig{
					{Name: "user_id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsql.ColumnTypeString},
					{Name: "age", From: "age", Type: natsql.ColumnTypeNumber},
					{Name: "active", From: "active", Type: natsql.ColumnTypeBoolean},
				},
			},
		},
	}

	ctx := context.Background()

	eng, err := natsql.NewEmbedded(cfg)
	if err != nil {
		return fmt.Errorf("NewEmbedded: %w", err)
	}
	defer eng.Close()

	// Create stream before starting engine
	nc := eng.NC()
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("JetStream: %w", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "events",
		Subjects: []string{"events.>"},
	}); err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	fmt.Print("✓ Engine started\n\n")

	// Subscribe to DLQ BEFORE publishing so we don't miss messages
	dlqSub, err := nc.SubscribeSync("natsql.dlq")
	if err != nil {
		return fmt.Errorf("DLQ subscribe: %w", err)
	}
	defer dlqSub.Unsubscribe()

	// Publish a mix of valid and invalid events
	testEvents := []struct {
		label string
		data  string
		valid bool
	}{
		{"VALID event", `{"id": "abc", "name": "Alice", "age": 30, "active": true}`, true},
		{"INVALID JSON", `not valid json at all`, false},
		{"TYPE MISMATCH (age should be number)", `{"id": "def", "name": "Bob", "age": "not-a-number", "active": true}`, false},
		{"MISSING KEY (no id)", `{"name": "Carol", "age": 25, "active": true}`, false},
		{"VALID event", `{"id": "ghi", "name": "Dave", "age": 35, "active": false}`, true},
	}

	var rejected []string
	for _, e := range testEvents {
		if _, err := js.Publish(ctx, "events.user-updated", []byte(e.data)); err != nil {
			return fmt.Errorf("Publish: %w", err)
		}
		if !e.valid {
			rejected = append(rejected, e.data)
		}
		fmt.Printf("  Published [%s]: %s\n", e.label, e.data)
	}

	// Only the two valid events become rows
	fmt.Println("\n── Materialized rows ──")
	rows, err := waitForRows(ctx, eng, "SELECT * FROM users WHERE user_id != ''", 2)
	if err != nil {
		return err
	}
	if got := column(rows, "name"); got != "Alice,Dave" {
		return fmt.Errorf("materialized names = %s, want Alice,Dave", got)
	}
	for _, row := range rows {
		b, _ := json.Marshal(row) // rows always encode: they came from JSON
		fmt.Printf("  ✓ %s\n", b)
	}

	// Every rejected event is in the DLQ, with its original bytes
	fmt.Println("\n── DLQ contents (rejected events) ──")
	dead, err := readDLQ(dlqSub, len(rejected))
	if err != nil {
		return err
	}
	slices.Sort(dead)
	slices.Sort(rejected)
	if !slices.Equal(dead, rejected) {
		return fmt.Errorf("DLQ holds %q, want %q", dead, rejected)
	}
	if extra, err := dlqSub.NextMsg(500 * time.Millisecond); err == nil {
		return fmt.Errorf("unexpected extra DLQ message: %s", extra.Data)
	}

	fmt.Println("\n✅ Malformed events go to DLQ; valid events process correctly!")
	return nil
}

// readDLQ reads n DLQ messages from sub and returns the original events they
// carry, checking that each names the view and an error.
func readDLQ(sub *nats.Subscription, n int) ([]string, error) {
	var events []string
	for len(events) < n {
		msg, err := sub.NextMsg(10 * time.Second)
		if err != nil {
			return nil, fmt.Errorf("got %d of %d DLQ messages: %w", len(events), n, err)
		}
		var envelope struct {
			OriginalMessageB64 string `json:"original_message_b64"`
			ViewName           string `json:"view_name"`
			Error              string `json:"error"`
		}
		if err := json.Unmarshal(msg.Data, &envelope); err != nil {
			return nil, fmt.Errorf("decoding DLQ envelope: %w", err)
		}
		original, err := base64.StdEncoding.DecodeString(envelope.OriginalMessageB64)
		if err != nil {
			return nil, fmt.Errorf("decoding DLQ payload: %w", err)
		}
		if envelope.ViewName != "users" || envelope.Error == "" {
			return nil, fmt.Errorf("DLQ envelope = %+v, want view users and an error", envelope)
		}
		events = append(events, string(original))
		fmt.Printf("  DLQ[%d]: %s\n          error: %s\n", len(events), original, envelope.Error)
	}
	return events, nil
}

// column returns one column of rows as sorted, comma-joined text, so a
// result can be compared regardless of row order.
func column(rows []map[string]any, col string) string {
	vals := make([]string, 0, len(rows))
	for _, r := range rows {
		vals = append(vals, fmt.Sprint(r[col]))
	}
	slices.Sort(vals)
	return strings.Join(vals, ",")
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
