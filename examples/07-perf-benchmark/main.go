// 07-perf-benchmark — performance benchmarking for natsql.
//
// Run: go run .                  (1,000,000 events)
//
//	go run . -events=10000   (a quick run; CI uses -events=2000)
//
// What it does:
//  1. Starts embedded NATS, creates view "users", publishes the events
//  2. Waits for full materialization
//  3. Runs 7 query types with timing, prints a comparison table
//
// Supported SQL operators in v1: =, !=, IN
// Queries using >, <, >=, <= are NOT supported.
//
// Query types tested:
//
//	A. PK equality (O(1) fast path)
//	B. PK equality with column projection
//	C. Full scan — no matching rows (PK miss + filter miss)
//	D. Full scan — stopped early by LIMIT
//	E. Full scan — filtered by non-key column
//	F. Full scan — two conditions (AND)
//	G. Full scan — return all rows
//
// Every event's fields are a function of its number, so the program knows
// how many rows each query must return, checks every result, and exits
// non-zero if one is wrong.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql/v2"
)

const numPublishers = 64

var (
	cities     = []string{"Berlin", "London", "Tokyo", "New York", "Paris", "Sydney", "Toronto", "Mumbai"}
	firstNames = []string{
		"Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy",
		"Karl", "Linda", "Mike", "Nancy", "Oscar", "Pam", "Quinn", "Rob", "Sara", "Tom",
	}
)

// user is the event published as number j.
type user struct {
	id, name, email, city string
	age                   int
	active                bool
}

func userAt(j int) user {
	name := firstNames[(j*7)%len(firstNames)]
	id := fmt.Sprintf("user-%07d", j)
	return user{
		id:     id,
		name:   name,
		email:  fmt.Sprintf("%s.%s@example.com", strings.ToLower(name), id),
		city:   cities[(j*3)%len(cities)],
		age:    18 + (j*13)%70,
		active: j%2 == 0,
	}
}

func main() {
	events := flag.Int("events", 1000000, "number of events to publish")
	flag.Parse()
	if err := run(*events); err != nil {
		log.Fatal(err)
	}
}

func run(totalEvents int) error {
	if totalEvents < numPublishers {
		return fmt.Errorf("-events must be at least %d", numPublishers)
	}
	ctx := context.Background()

	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "users",
				SourceStream: "events",
				KeyFields:    []string{"user_id"},
				Columns: []natsql.ColumnConfig{
					{Name: "user_id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsql.ColumnTypeString},
					{Name: "email", From: "email", Type: natsql.ColumnTypeString},
					{Name: "age", From: "age", Type: natsql.ColumnTypeNumber},
					{Name: "city", From: "city", Type: natsql.ColumnTypeString},
					{Name: "active", From: "active", Type: natsql.ColumnTypeBoolean},
				},
			},
		},
	}

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
		Name: "events", Subjects: []string{"events.>"},
	}); err != nil {
		return fmt.Errorf("CreateStream: %w", err)
	}

	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	fmt.Print("✓ Engine started\n\n")

	if err := publishEvents(ctx, js, totalEvents); err != nil {
		return err
	}
	if err := waitForMaterialization(ctx, eng, totalEvents); err != nil {
		return err
	}
	if err := runBenchmarks(ctx, eng, totalEvents); err != nil {
		return err
	}
	fmt.Println("✅ Performance benchmark complete!")
	fmt.Println("  PK lookups are O(1) — near-instant regardless of dataset size.")
	fmt.Println("  Full scans read every row of the view — performance degrades with view size.")
	fmt.Println("  Use WHERE on key_fields + LIMIT to keep queries fast.")
	return nil
}

func publishEvents(ctx context.Context, js jetstream.JetStream, totalEvents int) error {
	fmt.Printf("Publishing %d events...\n", totalEvents)

	publishStart := time.Now()
	done := make(chan struct{})
	var published atomic.Int64

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n := published.Load()
				pct := float64(n) * 100 / float64(totalEvents)
				rate := float64(n) / time.Since(publishStart).Seconds()
				fmt.Printf("\r  Published %d / %d (%.1f%%) — %.0f msg/s    ", n, totalEvents, pct, rate)
			case <-done:
				return
			}
		}
	}()
	defer close(done)

	var (
		pubWg    sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	eventsPerWorker := totalEvents / numPublishers

	for w := range numPublishers {
		start := w * eventsPerWorker
		end := start + eventsPerWorker
		if w == numPublishers-1 {
			end = totalEvents
		}
		pubWg.Go(func() {
			pubCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			for j := start; j < end; j++ {
				u := userAt(j)
				event := fmt.Sprintf(
					`{"id":%q,"name":%q,"email":%q,"age":%d,"city":%q,"active":%t}`,
					u.id, u.name, u.email, u.age, u.city, u.active,
				)
				if _, err := js.Publish(pubCtx, "events.user-created", []byte(event)); err != nil {
					errOnce.Do(func() { firstErr = fmt.Errorf("publishing event %d: %w", j, err) })
					return
				}
				published.Add(1)
			}
		})
	}
	pubWg.Wait()
	if firstErr != nil {
		return firstErr
	}
	fmt.Printf("\r  Published %d / %d (100.0%%) — done.          \n", totalEvents, totalEvents)
	fmt.Printf("  Published %d events in %v (%.0f msg/s)\n\n", totalEvents, time.Since(publishStart),
		float64(totalEvents)/time.Since(publishStart).Seconds())
	return nil
}

func waitForMaterialization(ctx context.Context, eng *natsql.Engine, totalEvents int) error {
	fmt.Print("Waiting for materialization...")
	pollStart := time.Now()
	for {
		time.Sleep(500 * time.Millisecond)
		// The view's own counter avoids scanning a million rows per poll.
		processed := eng.Stats().ViewStatus[0].EventsProcessed
		fmt.Printf(" %d", processed)
		if processed >= int64(totalEvents) {
			break
		}
		if time.Since(pollStart) > 10*time.Minute {
			return fmt.Errorf("timed out waiting for materialization (processed %d/%d)", processed, totalEvents)
		}
	}

	// Processed isn't yet written: wait until the scan sees every row.
	for {
		res := eng.Query(ctx, "SELECT user_id FROM users WHERE user_id != ''")
		if res.Error != nil {
			return fmt.Errorf("poll query failed: %s", *res.Error)
		}
		if len(res.Results) == totalEvents {
			break
		}
		if len(res.Results) > totalEvents || time.Since(pollStart) > 10*time.Minute {
			return fmt.Errorf("view holds %d rows, want %d", len(res.Results), totalEvents)
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Printf("\n  All %d rows materialized in %v\n\n", totalEvents, time.Since(pollStart))
	return nil
}

// count returns how many published users satisfy match.
func count(totalEvents int, match func(user) bool) int {
	n := 0
	for j := range totalEvents {
		if match(userAt(j)) {
			n++
		}
	}
	return n
}

func runBenchmarks(ctx context.Context, eng *natsql.Engine, totalEvents int) error {
	fmt.Println("── Running performance benchmarks ────────────────")
	trials := 3
	first, middle, last := userAt(0).id, userAt(totalEvents/2).id, userAt(totalEvents-1).id
	berlin30 := func(u user) bool { return u.city == "Berlin" && u.age == 30 }

	type benchCase struct {
		group, label, sql, note string
		want                    int
	}
	cases := []benchCase{
		{"A", "PK = first user", "SELECT * FROM users WHERE user_id = '" + first + "'", "O(1) KV.Get", 1},
		{"A", "PK = middle user", "SELECT * FROM users WHERE user_id = '" + middle + "'", "O(1) KV.Get", 1},
		{"A", "PK = last user", "SELECT * FROM users WHERE user_id = '" + last + "'", "O(1) KV.Get", 1},
		{"B", "PK + projection (name, age)", "SELECT name, age FROM users WHERE user_id = '" + first + "'", "O(1) + projection", 1},
		{"B", "PK + projection (email, city, active)", "SELECT email, city, active FROM users WHERE user_id = '" + middle + "'", "O(1) + projection", 1},
		{"C", "No match, PK miss", "SELECT * FROM users WHERE user_id = 'nonexistent'", "PK miss (O(1))", 0},
		{"C", "No match, filter miss", "SELECT * FROM users WHERE city = 'Nowhere' AND name = 'Nemo'", "filter miss (full scan)", 0},
		{"D", "Full scan, LIMIT 5", "SELECT * FROM users WHERE user_id != '' LIMIT 5", "LIMIT stops early", min(5, totalEvents)},
		{"D", "Full scan, LIMIT 50", "SELECT * FROM users WHERE user_id != '' LIMIT 50", "LIMIT stops early", min(50, totalEvents)},
		{"D", "Full scan, LIMIT 500", "SELECT * FROM users WHERE user_id != '' LIMIT 500", "LIMIT stops early", min(500, totalEvents)},
		{"E", "WHERE city = 'Berlin'", "SELECT * FROM users WHERE city = 'Berlin'", "full scan, filtered", count(totalEvents, func(u user) bool { return u.city == "Berlin" })},
		{"E", "WHERE city = 'Tokyo'", "SELECT * FROM users WHERE city = 'Tokyo'", "full scan, filtered", count(totalEvents, func(u user) bool { return u.city == "Tokyo" })},
		{"E", "WHERE age = 30", "SELECT * FROM users WHERE age = 30", "full scan, filtered", count(totalEvents, func(u user) bool { return u.age == 30 })},
		{"F", "city='Berlin' AND age=30", "SELECT * FROM users WHERE city = 'Berlin' AND age = 30", "full scan, 2 filters", count(totalEvents, berlin30)},
		{"F", "city='London' AND name='Alice'", "SELECT * FROM users WHERE city = 'London' AND name = 'Alice'", "full scan, 2 filters", count(totalEvents, func(u user) bool { return u.city == "London" && u.name == "Alice" })},
		{"G", "Full scan, all rows", "SELECT * FROM users WHERE user_id != ''", "full scan, all rows", totalEvents},
		{"G", "Full scan, all rows, LIMIT 5000", "SELECT * FROM users WHERE user_id != '' LIMIT 5000", "full scan, LIMIT 5000", min(5000, totalEvents)},
	}

	type result struct {
		benchCase
		avgDur time.Duration
	}
	var results []result
	var errs []error

	for _, c := range cases {
		var totalDur time.Duration
		for range trials {
			start := time.Now()
			res := eng.Query(ctx, c.sql)
			totalDur += time.Since(start)
			if res.Error != nil {
				return fmt.Errorf("query %q failed: %s", c.sql, *res.Error)
			}
			if len(res.Results) != c.want {
				errs = append(errs, fmt.Errorf("%s: %d rows, want %d", c.sql, len(res.Results), c.want))
				break
			}
		}
		avg := totalDur / time.Duration(trials)
		results = append(results, result{c, avg})
		fmt.Printf("  %s) %-44s → %7d rows  avg %v\n", c.group, c.label, c.want, avg)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("── Query Performance Summary ─────────────────────")
	fmt.Println()
	fmt.Printf("Total rows in view: %d\n\n", totalEvents)

	header := fmt.Sprintf("%-6s %-46s %7s  %-12s  %s", "Type", "Query Pattern", "Rows", "Avg Time", "Notes")
	fmt.Println(header)
	fmt.Println(strings.Repeat("─", len(header)))
	for _, r := range results {
		fmt.Printf("%-6s %-46s %7d  %-12v  %s\n",
			"["+r.group+"]", r.label, r.want, r.avgDur.Round(time.Microsecond), r.note)
	}
	fmt.Println()
	return nil
}
