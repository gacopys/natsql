// client — talks to the natsql server started from ../config.yaml, the way
// any application would: it publishes events to NATS and queries the views
// over HTTP and NATS request-reply. It needs only nats.go, not natsql.
//
// Run, with the server running (see ../README.md):
//
//	go run .
//
// The program checks every result and exits non-zero if one is wrong.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	natsURL = "nats://127.0.0.1:4223" // nats.port in ../config.yaml
	httpURL = "http://127.0.0.1:8080" // http.port in ../config.yaml
)

// queryResult is the JSON envelope natsql returns over HTTP and NATS.
type queryResult struct {
	Results []map[string]any `json:"results"`
	Error   *string          `json:"error"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if err := waitForReady(ctx); err != nil {
		return err
	}
	fmt.Println("✓ Server is ready (GET /readyz → 200)")

	nc, err := nats.Connect(natsURL)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", natsURL, err)
	}
	defer nc.Close()

	steps := []func(context.Context, *nats.Conn) error{publishEvents, checkHTTP, checkNATS, checkError}
	for _, step := range steps {
		if err := step(ctx, nc); err != nil {
			return err
		}
	}

	fmt.Println("\n✅ The natsql server materializes events and answers queries!")
	return nil
}

// publishEvents publishes one event to each view's source stream; the server
// created the "events" and "inventory" streams at startup.
func publishEvents(ctx context.Context, nc *nats.Conn) error {
	events := map[string]string{
		"events.user-created":     `{"id": "u1", "name": "Alice", "email": "alice@test.com", "age": 30}`,
		"inventory.product-added": `{"sku": "SKU-001", "title": "Widget", "price": 9.99, "stock": true}`,
	}
	for subject, event := range events {
		if err := nc.Publish(subject, []byte(event)); err != nil {
			return fmt.Errorf("publishing to %s: %w", subject, err)
		}
		fmt.Printf("✓ Published to %s: %s\n", subject, event)
	}
	if err := nc.FlushWithContext(ctx); err != nil {
		return fmt.Errorf("flushing: %w", err)
	}
	return nil
}

// checkHTTP queries both views over HTTP until the events show up.
func checkHTTP(ctx context.Context, _ *nats.Conn) error {
	checks := []struct {
		sql  string
		want map[string]string
	}{
		{"SELECT * FROM users WHERE user_id = 'u1'", map[string]string{"name": "Alice", "email": "alice@test.com", "age": "30"}},
		{"SELECT title, price, in_stock FROM products WHERE sku = 'SKU-001'", map[string]string{"title": "Widget", "price": "9.99", "in_stock": "true"}},
	}
	for _, c := range checks {
		row, err := waitForRow(ctx, c.sql)
		if err != nil {
			return err
		}
		for col, want := range c.want {
			if got := fmt.Sprint(row[col]); got != want {
				return fmt.Errorf("%s: column %s = %s, want %s", c.sql, col, got, want)
			}
		}
		fmt.Printf("✓ HTTP  %s → %v\n", c.sql, row)
	}
	return nil
}

// checkNATS runs a query over NATS request-reply.
func checkNATS(ctx context.Context, nc *nats.Conn) error {
	sql := "SELECT name FROM users WHERE user_id = 'u1'"
	msg, err := nc.RequestWithContext(ctx, "natsql.query", []byte(sql))
	if err != nil {
		return fmt.Errorf("NATS query: %w", err)
	}
	var res queryResult
	if err := json.Unmarshal(msg.Data, &res); err != nil {
		return fmt.Errorf("decoding NATS reply: %w", err)
	}
	if res.Error != nil || len(res.Results) != 1 || res.Results[0]["name"] != "Alice" {
		return fmt.Errorf("NATS query %q = %s, want Alice", sql, msg.Data)
	}
	fmt.Printf("✓ NATS  %s → %v\n", sql, res.Results[0])
	return nil
}

// checkError checks that errors come back in the same envelope.
func checkError(ctx context.Context, _ *nats.Conn) error {
	res, status, err := query(ctx, "SELECT * FROM ghost WHERE id = 'x'")
	if err != nil {
		return err
	}
	if status != http.StatusOK || res.Error == nil || !strings.Contains(*res.Error, `view "ghost" not found`) {
		return fmt.Errorf("unknown view: HTTP %d, %+v, want a not-found error", status, res)
	}
	fmt.Printf("✓ HTTP  unknown view → error %q\n", *res.Error)
	return nil
}

// waitForReady polls GET /readyz until the server reports 200.
func waitForReady(ctx context.Context) error {
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, httpURL+"/readyz", nil)
		if err != nil {
			return err
		}
		rsp, err := http.DefaultClient.Do(req)
		if err == nil {
			rsp.Body.Close()
			if rsp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return errors.New("server never became ready (is it running? see ../README.md)")
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// waitForRow queries over HTTP until sql returns exactly one row.
func waitForRow(ctx context.Context, sql string) (map[string]any, error) {
	for {
		res, status, err := query(ctx, sql)
		switch {
		case err != nil:
			return nil, err
		case status != http.StatusOK || res.Error != nil:
			return nil, fmt.Errorf("%s: HTTP %d, %+v", sql, status, res)
		case len(res.Results) == 1:
			return res.Results[0], nil
		case len(res.Results) > 1:
			return nil, fmt.Errorf("%s: %d rows, want 1", sql, len(res.Results))
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%s: no row before the deadline", sql)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// query posts sql to the HTTP query API.
func query(ctx context.Context, sql string) (*queryResult, int, error) {
	body, err := json.Marshal(map[string]string{"sql": sql})
	if err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, httpURL+"/api/v1/query", bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("HTTP query: %w", err)
	}
	defer rsp.Body.Close()
	var res queryResult
	if err := json.NewDecoder(rsp.Body).Decode(&res); err != nil {
		return nil, rsp.StatusCode, fmt.Errorf("decoding HTTP response: %w", err)
	}
	return &res, rsp.StatusCode, nil
}
