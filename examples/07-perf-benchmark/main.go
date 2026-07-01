// 07-perf-benchmark — performance benchmarking for natsql.
//
// Run: go run .
//
// What it does:
//  1. Starts embedded NATS, creates view "users", publishes 10,000 events
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
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql"
)

const numPublishers = 64

func main() {
	ctx := context.Background()

	// ── View config: "users" with 6 columns ─────────────────────
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

	// ── Step 1: Engine setup ────────────────────────────────────
	eng, err := natsql.NewEmbedded(cfg)
	if err != nil {
		log.Fatalf("NewEmbedded: %v", err)
	}
	defer eng.Close()

	nc := eng.NC()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "events",
		Subjects: []string{"events.>"},
	})
	if err != nil {
		log.Fatalf("CreateStream: %v", err)
	}

	if err := eng.Start(ctx); err != nil {
		log.Fatalf("Start: %v", err)
	}
	fmt.Println("✓ Engine started\n")

	// ── Step 2: Generate and publish events via parallel publishers ─
	const totalEvents = 1000000
	fmt.Printf("Publishing %d events...\n", totalEvents)

	cities := []string{"Berlin", "London", "Tokyo", "New York", "Paris", "Sydney", "Toronto", "Mumbai"}
	firstNames := []string{"Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy",
		"Karl", "Linda", "Mike", "Nancy", "Oscar", "Pam", "Quinn", "Rob", "Sara", "Tom"}

	publishStart := time.Now()
	done := make(chan struct{})
	var published atomic.Int64

	// Progress reporter
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n := published.Load()
				pct := float64(n) * 100 / float64(totalEvents)
				elapsed := time.Since(publishStart).Seconds()
				rate := float64(0)
				if elapsed > 0 {
					rate = float64(n) / elapsed
				}
				fmt.Printf("\r  Published %d / %d (%.1f%%) — %.0f msg/s    ", n, totalEvents, pct, rate)
			case <-done:
				return
			}
		}
	}()

	var pubWg sync.WaitGroup
	pubWg.Add(numPublishers)

	eventsPerWorker := totalEvents / numPublishers

	for w := 0; w < numPublishers; w++ {
		start := w * eventsPerWorker
		end := start + eventsPerWorker
		if w == numPublishers-1 {
			end = totalEvents
		}

		go func(start, end int) {
			defer pubWg.Done()
			rng := rand.New(rand.NewSource(42 + int64(w)))
			pubCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			for j := start; j < end; j++ {
				uid := fmt.Sprintf("user-%06d", j)
				name := firstNames[rng.Intn(len(firstNames))]
				age := rng.Intn(70) + 18
				city := cities[rng.Intn(len(cities))]
				email := fmt.Sprintf("%s.%s@example.com", strings.ToLower(name), uid)
				active := rng.Intn(2) == 0

				event := fmt.Sprintf(
					`{"id":%q,"name":%q,"email":%q,"age":%d,"city":%q,"active":%t}`,
					uid, name, email, age, city, active,
				)

				if _, err := js.Publish(pubCtx, "events.user-created", []byte(event)); err != nil {
					log.Fatalf("Publish event %d: %v", j, err)
				}
				published.Add(1)
			}
		}(start, end)
	}

	pubWg.Wait()
	close(done)
	fmt.Printf("\r  Published %d / %d (100.0%%) — done.          \n", totalEvents, totalEvents)
	publishElapsed := time.Since(publishStart)
	fmt.Printf("  Published %d events in %v (%.0f msg/s)\n\n", totalEvents, publishElapsed,
		float64(totalEvents)/publishElapsed.Seconds())

	// ── Step 3: Wait for full materialization ───────────────────
	fmt.Print("Waiting for materialization...")

	var rowCount int
	pollStart := time.Now()
	for {
		time.Sleep(500 * time.Millisecond)
		res := eng.Query(ctx, "SELECT * FROM users WHERE user_id != ''")
		if res.Error != nil {
			log.Fatalf("Poll query failed: %s", *res.Error)
		}
		rowCount = len(res.Results)
		fmt.Printf(" %d", rowCount)
		if rowCount >= totalEvents {
			break
		}
		if time.Since(pollStart) > 60*time.Second {
			log.Fatalf("Timed out waiting for materialization (got %d/%d)", rowCount, totalEvents)
		}
	}
	fmt.Printf("\n  All %d rows materialized in %v\n\n", rowCount, time.Since(pollStart))

	// ── Step 4: Run performance queries ─────────────────────────
	fmt.Println("── Running performance benchmarks ────────────────")

	type benchCase struct {
		label string
		sql   string
	}
	trials := 3

	cases := []benchCase{
		// A — PK equality (O(1) fast path)
		{"A) PK = 'user-000000'", "SELECT * FROM users WHERE user_id = 'user-000000'"},
		{"A) PK = 'user-004999'", "SELECT * FROM users WHERE user_id = 'user-004999'"},
		{"A) PK = 'user-009999'", "SELECT * FROM users WHERE user_id = 'user-009999'"},

		// B — PK equality with column projection
		{"B) PK + projection (name, age)", "SELECT name, age FROM users WHERE user_id = 'user-000000'"},
		{"B) PK + projection (email, city, active)", "SELECT email, city, active FROM users WHERE user_id = 'user-005000'"},

		// C — Full scan, no matching rows
		{"C) No match, PK miss", "SELECT * FROM users WHERE user_id = 'nonexistent'"},
		{"C) No match, filter miss", "SELECT * FROM users WHERE city = 'Nowhere' AND name = 'Nemo'"},

		// D — Full scan with LIMIT early stop
		{"D) Full scan, LIMIT 5", "SELECT * FROM users WHERE user_id != '' LIMIT 5"},
		{"D) Full scan, LIMIT 50", "SELECT * FROM users WHERE user_id != '' LIMIT 50"},
		{"D) Full scan, LIMIT 500", "SELECT * FROM users WHERE user_id != '' LIMIT 500"},

		// E — Non-key WHERE (must full-scan)
		{"E) WHERE city = 'Berlin'", "SELECT * FROM users WHERE city = 'Berlin'"},
		{"E) WHERE city = 'Tokyo'", "SELECT * FROM users WHERE city = 'Tokyo'"},
		{"E) WHERE age = 30", "SELECT * FROM users WHERE age = 30"},

		// F — Two conditions (AND)
		{"F) city='Berlin' AND age=30", "SELECT * FROM users WHERE city = 'Berlin' AND age = 30"},
		{"F) city='London' AND name='Alice'", "SELECT * FROM users WHERE city = 'London' AND name = 'Alice'"},

		// G — Full scan (all rows)
		{"G) Full scan, all rows", "SELECT * FROM users WHERE user_id != ''"},
		{"G) Full scan, all rows, LIMIT 5000", "SELECT * FROM users WHERE user_id != '' LIMIT 5000"},
	}

	type result struct {
		label  string
		rows   int
		avgDur time.Duration
	}
	var results []result

	for _, c := range cases {
		var totalDur time.Duration
		var rows int

		for t := 0; t < trials; t++ {
			start := time.Now()
			res := eng.Query(ctx, c.sql)
			elapsed := time.Since(start)

			if res.Error != nil {
				log.Fatalf("Query %q failed: %s", c.sql, *res.Error)
			}
			totalDur += elapsed
			rows = len(res.Results)
		}
		avg := totalDur / time.Duration(trials)
		results = append(results, result{c.label, rows, avg})
		fmt.Printf("  %-46s → %5d rows  avg %v\n", c.label, rows, avg)
	}

	// ── Step 5: Summary table ───────────────────────────────────
	fmt.Println()
	fmt.Println("── Query Performance Summary ─────────────────────")
	fmt.Println()
	fmt.Printf("Total rows in view: %d\n\n", totalEvents)

	header := fmt.Sprintf("%-6s %-46s %7s  %-12s  %s", "Type", "Query Pattern", "Rows", "Avg Time", "Notes")
	fmt.Println(header)
	fmt.Println(strings.Repeat("─", len(header)))

	notes := []string{
		"O(1) KV.Get",
		"O(1) KV.Get",
		"O(1) KV.Get",
		"O(1) + projection",
		"O(1) + projection",
		"PK miss (O(1))",
		"filter miss (full scan)",
		"LIMIT stops early",
		"LIMIT stops early",
		"LIMIT stops early",
		"full scan, filtered",
		"full scan, filtered",
		"full scan, filtered",
		"full scan, 2 filters",
		"full scan, 2 filters",
		"full scan, all rows",
		"full scan, LIMIT 5000",
	}

	groups := []int{3, 2, 2, 3, 3, 2, 2} // A..G group sizes
	gi := 0
	gn := 0
	for i, r := range results {
		groupLetter := fmt.Sprintf("[%c]", 'A'+gi)
		fmt.Printf("%-6s %-46s %7d  %-12v  %s\n",
			groupLetter,
			r.label,
			r.rows,
			r.avgDur.Round(time.Microsecond),
			notes[i],
		)
		gn++
		if gn >= groups[gi] {
			gi++
			gn = 0
		}
	}

	fmt.Println()
	fmt.Println("✅ Performance benchmark complete!")
	fmt.Println("  PK lookups are O(1) — near-instant regardless of dataset size.")
	fmt.Println("  Full scans enumerate every KV key — performance degrades with dataset size.")
	fmt.Println("  Use WHERE on key_fields + LIMIT to keep queries fast.")
}
