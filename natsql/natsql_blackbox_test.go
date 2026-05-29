// Black-box integration tests for the natsql query engine.
//
// These tests go through the full pipeline:
//  1. Start embedded NATS server
//  2. Create Engine with view config via public API
//  3. Publish events to JetStream
//  4. Wait for materialization
//  5. Query via Engine.Query() (the public API)
//  6. Assert results
//
// No internal state inspection — every assertion goes through the public Query() method.
// Test data is fully deterministic so expected results are known in advance.
package natsql_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"io"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats-server/v2/server"

	natsql "natsql"
)

// testRow is a row in the materialized view, keyed by user_id.
type testRow struct {
	UserID string `json:"user_id"`
	Name   string `json:"name"`
	Age    int    `json:"age"`
	City   string `json:"city"`
	Active bool   `json:"active"`
}

// allTestRows is the authoritative set of test data.
// Every row is here so query expectations can be computed by filtering this slice.
var allTestRows = []testRow{
	// Berlin — 5 users (3 active, 2 inactive)
	{"user-001", "Alice", 25, "Berlin", true},
	{"user-002", "Bob", 30, "Berlin", true},
	{"user-003", "Carol", 30, "Berlin", false},
	{"user-004", "Dave", 35, "Berlin", true},
	{"user-005", "Eve", 40, "Berlin", false},
	// London — 5 users (3 active, 2 inactive)
	{"user-006", "Frank", 22, "London", true},
	{"user-007", "Grace", 28, "London", false},
	{"user-008", "Heidi", 30, "London", true},
	{"user-009", "Ivan", 35, "London", false},
	{"user-010", "Judy", 45, "London", true},
	// Tokyo — 5 users (3 active, 2 inactive)
	{"user-011", "Karl", 30, "Tokyo", true},
	{"user-012", "Linda", 30, "Tokyo", true},
	{"user-013", "Mike", 35, "Tokyo", false},
	{"user-014", "Nancy", 40, "Tokyo", true},
	{"user-015", "Oscar", 50, "Tokyo", false},
	// Paris — 5 users (3 active, 2 inactive)
	{"user-016", "Pam", 20, "Paris", true},
	{"user-017", "Quinn", 25, "Paris", false},
	{"user-018", "Rob", 30, "Paris", true},
	{"user-019", "Sara", 35, "Paris", true},
	{"user-020", "Tom", 40, "Paris", false},
	// New York — 5 users (3 active, 2 inactive)
	{"user-021", "Uma", 27, "New York", true},
	{"user-022", "Victor", 30, "New York", true},
	{"user-023", "Wendy", 33, "New York", false},
	{"user-024", "Xavier", 38, "New York", true},
	{"user-025", "Yara", 42, "New York", false},
	// Sydney — 5 users (4 active, 1 inactive)
	{"user-026", "Zed", 23, "Sydney", true},
	{"user-027", "Alice", 28, "Sydney", false},
	{"user-028", "Bob", 32, "Sydney", true},
	{"user-029", "Carol", 35, "Sydney", true},
	{"user-030", "Dave", 45, "Sydney", true},
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// rowMapByPK returns the test rows indexed by user_id for quick lookup.
func rowMapByPK() map[string]testRow {
	m := make(map[string]testRow, len(allTestRows))
	for _, r := range allTestRows {
		m[r.UserID] = r
	}
	return m
}

// filterRows returns rows matching a predicate.
func filterRows(rows []testRow, fn func(testRow) bool) []testRow {
	var out []testRow
	for _, r := range rows {
		if fn(r) {
			out = append(out, r)
		}
	}
	return out
}

// requireResultCount asserts the query returns exactly n rows.
// Returns the results for further assertions.
func requireResultCount(t *testing.T, res *natsql.QueryResult, label string, want int) []map[string]any {
	t.Helper()
	if res.Error != nil {
		t.Fatalf("[%s] unexpected error: %s", label, *res.Error)
	}
	if len(res.Results) != want {
		t.Fatalf("[%s] got %d rows, want %d\n  results: %+v", label, len(res.Results), want, res.Results)
	}
	return res.Results
}

// requireError asserts the query returns an error containing substr.
func requireError(t *testing.T, res *natsql.QueryResult, label string, substr string) {
	t.Helper()
	if res.Error == nil {
		t.Fatalf("[%s] expected error containing %q, got nil error with %d results",
			label, substr, len(res.Results))
	}
}

// assertField checks a single field in a result row.
func assertField(t *testing.T, row map[string]any, col string, want any, label string) {
	t.Helper()
	got, ok := row[col]
	if !ok {
		t.Errorf("[%s] missing column %q in result row %+v", label, col, row)
		return
	}
	// Compare via JSON for type-safe comparison (numbers come as float64 from JSON)
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if string(gotJSON) != string(wantJSON) {
		t.Errorf("[%s] column %q = %v (type %T), want %v (type %T)",
			label, col, got, got, want, want)
	}
}

// ---------------------------------------------------------------------------
// Black-box query engine integration tests
// ---------------------------------------------------------------------------

func TestBlackBox_Queries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "EVENTS_BLACKBOX"
	createStream(t, ctx, js, streamName)

	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "users",
				SourceStream: streamName,
				KeyFields:    []string{"user_id"},
				Columns: []natsql.ColumnConfig{
					{Name: "user_id", From: "user_id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "name", From: "name", Type: natsql.ColumnTypeString},
					{Name: "age", From: "age", Type: natsql.ColumnTypeNumber},
					{Name: "city", From: "city", Type: natsql.ColumnTypeString},
					{Name: "active", From: "active", Type: natsql.ColumnTypeBoolean},
				},
				Consumer: natsql.ConsumerConfig{BatchSize: 30, MaxDeliver: 5, AckWaitSeconds: 10},
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := natsql.New(js, cfg, natsql.WithLogger(logger))
	if err != nil {
		t.Fatalf("New engine: %v", err)
	}
	defer eng.Close()

	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start engine: %v", err)
	}

	// Publish all test events
	for _, row := range allTestRows {
		event, _ := json.Marshal(row)
		if _, err := js.Publish(ctx, streamName+".evt", event); err != nil {
			t.Fatalf("Publish %s: %v", row.UserID, err)
		}
	}

	// Wait for all rows to materialize
	pollForCount(t, ctx, eng, "users", 30)

	// -----------------------------------------------------------------------
	// A: PK equality
	// -----------------------------------------------------------------------

	t.Run("PK_hit", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE user_id = 'user-001'")
		rows := requireResultCount(t, res, "PK_hit", 1)
		assertField(t, rows[0], "user_id", "user-001", "PK_hit")
		assertField(t, rows[0], "name", "Alice", "PK_hit")
		assertField(t, rows[0], "age", float64(25), "PK_hit")
		assertField(t, rows[0], "city", "Berlin", "PK_hit")
		assertField(t, rows[0], "active", true, "PK_hit")
	})

	t.Run("PK_miss", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE user_id = 'nonexistent'")
		requireResultCount(t, res, "PK_miss", 0)
	})

	// -----------------------------------------------------------------------
	// B: PK + non-key post-filter
	// -----------------------------------------------------------------------

	t.Run("PK_with_nonkey_filter_pass", func(t *testing.T) {
		// user-001 is active=true in Berlin
		res := eng.Query(ctx, "SELECT * FROM users WHERE user_id = 'user-001' AND active = true")
		requireResultCount(t, res, "PK_nonkey_pass", 1)
	})

	t.Run("PK_with_nonkey_filter_reject", func(t *testing.T) {
		// user-003 is active=false
		res := eng.Query(ctx, "SELECT * FROM users WHERE user_id = 'user-003' AND active = true")
		requireResultCount(t, res, "PK_nonkey_reject", 0)
	})

	t.Run("PK_with_nonkey_filter_age_reject", func(t *testing.T) {
		// user-001 is age 25, not 30
		res := eng.Query(ctx, "SELECT * FROM users WHERE user_id = 'user-001' AND age = 30")
		requireResultCount(t, res, "PK_nonkey_age_reject", 0)
	})

	t.Run("PK_with_two_nonkey_filters", func(t *testing.T) {
		// user-002 is Bob, 30, Berlin, active=true
		res := eng.Query(ctx, "SELECT * FROM users WHERE user_id = 'user-002' AND city = 'Berlin' AND active = true")
		requireResultCount(t, res, "PK_two_nonkey", 1)
	})

	// -----------------------------------------------------------------------
	// C: Projection
	// -----------------------------------------------------------------------

	t.Run("Projection_single_col", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT name FROM users WHERE user_id = 'user-001'")
		rows := requireResultCount(t, res, "proj_single", 1)
		if _, ok := rows[0]["user_id"]; ok {
			t.Error("user_id should not appear in projected results")
		}
		if _, ok := rows[0]["age"]; ok {
			t.Error("age should not appear in projected results")
		}
		assertField(t, rows[0], "name", "Alice", "proj_single")
	})

	t.Run("Projection_multi_col", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT name, age, city FROM users WHERE user_id = 'user-002'")
		rows := requireResultCount(t, res, "proj_multi", 1)
		assertField(t, rows[0], "name", "Bob", "proj_multi")
		assertField(t, rows[0], "age", float64(30), "proj_multi")
		assertField(t, rows[0], "city", "Berlin", "proj_multi")
		if _, ok := rows[0]["user_id"]; ok {
			t.Error("user_id should not appear in projected results")
		}
	})

	// -----------------------------------------------------------------------
	// D: Non-key field filters (full scan)
	// -----------------------------------------------------------------------

	t.Run("Where_city_berlin", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE city = 'Berlin'")
		rows := requireResultCount(t, res, "city=Berlin", 5)
		for _, r := range rows {
			assertField(t, r, "city", "Berlin", "city=Berlin")
		}
	})

	t.Run("Where_city_no_match", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE city = 'Mars'")
		requireResultCount(t, res, "city=Mars", 0)
	})

	t.Run("Where_age_30", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE age = 30")
		rows := requireResultCount(t, res, "age=30", 7)
		for _, r := range rows {
			assertField(t, r, "age", float64(30), "age=30")
		}
	})

	t.Run("Where_active_true", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE active = true")
		rows := requireResultCount(t, res, "active=true", 19)
		for _, r := range rows {
			assertField(t, r, "active", true, "active=true")
		}
	})

	t.Run("Where_active_false", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE active = false")
		rows := requireResultCount(t, res, "active=false", 11)
		for _, r := range rows {
			assertField(t, r, "active", false, "active=false")
		}
	})

	t.Run("Where_name_shared", func(t *testing.T) {
		// Alice appears in Berlin (user-001) and Sydney (user-027)
		res := eng.Query(ctx, "SELECT * FROM users WHERE name = 'Alice'")
		requireResultCount(t, res, "name=Alice", 2)
	})

	t.Run("Where_name_single", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE name = 'Uma'")
		requireResultCount(t, res, "name=Uma", 1)
	})

	// -----------------------------------------------------------------------
	// E: AND conditions
	// -----------------------------------------------------------------------

	t.Run("AND_city_age", func(t *testing.T) {
		// Berlin + age 30 → Bob (active) and Carol (inactive)
		res := eng.Query(ctx, "SELECT * FROM users WHERE city = 'Berlin' AND age = 30")
		requireResultCount(t, res, "Berlin+age=30", 2)
	})

	t.Run("AND_city_age_active", func(t *testing.T) {
		// Berlin + age 30 + active=true → only Bob
		res := eng.Query(ctx, "SELECT * FROM users WHERE city = 'Berlin' AND age = 30 AND active = true")
		requireResultCount(t, res, "Berlin+age=30+active", 1)
	})

	t.Run("AND_city_name", func(t *testing.T) {
		// Sydney + name=Alice → user-027
		res := eng.Query(ctx, "SELECT * FROM users WHERE city = 'Sydney' AND name = 'Alice'")
		requireResultCount(t, res, "Sydney+Alice", 1)
	})

	t.Run("AND_no_match", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE city = 'London' AND age = 35 AND active = true")
		requireResultCount(t, res, "London+35+active", 0) // Ivan is 35 but inactive
	})

	t.Run("AND_city_not_equal", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE city = 'New York' AND age != 30")
		requireResultCount(t, res, "NY+age!=30", 4) // 5 NY users, Victor is 30
	})

	// -----------------------------------------------------------------------
	// F: IN clause
	// -----------------------------------------------------------------------

	t.Run("IN_two_cities", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE city IN ('Berlin', 'Tokyo')")
		requireResultCount(t, res, "IN Berlin+Tokyo", 10) // 5 + 5
	})

	t.Run("IN_single", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE city IN ('Paris')")
		requireResultCount(t, res, "IN Paris", 5)
	})

	t.Run("IN_no_match", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE city IN ('Mars', 'Venus')")
		requireResultCount(t, res, "IN no match", 0)
	})

	t.Run("IN_on_number", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE age IN (20, 22, 23)")
		requireResultCount(t, res, "IN ages", 3) // Pam(20), Frank(22), Zed(23)
	})

	// -----------------------------------------------------------------------
	// G: != operator
	// -----------------------------------------------------------------------

	t.Run("NotEqual_city", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE city != 'Berlin'")
		requireResultCount(t, res, "!=Berlin", 25)
	})

	t.Run("NotEqual_age", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE age != 30")
		requireResultCount(t, res, "!=30", 23)
	})

	t.Run("NotEqual_active", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE active != true")
		requireResultCount(t, res, "!=true", 11) // same as active=false
	})

	// -----------------------------------------------------------------------
	// H: LIMIT
	// -----------------------------------------------------------------------

	t.Run("LIMIT_3", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE user_id != '' LIMIT 3")
		requireResultCount(t, res, "LIMIT 3", 3)
	})

	t.Run("LIMIT_10", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE user_id != '' LIMIT 10")
		requireResultCount(t, res, "LIMIT 10", 10)
	})

	t.Run("LIMIT_exceeds_rows", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE user_id != '' LIMIT 100")
		requireResultCount(t, res, "LIMIT 100", 30)
	})

	// -----------------------------------------------------------------------
	// I: Full scan — all rows
	// -----------------------------------------------------------------------

	t.Run("FullScan_all", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE user_id != ''")
		requireResultCount(t, res, "all rows", 30)
	})

	// -----------------------------------------------------------------------
	// J: Error cases
	// -----------------------------------------------------------------------

	t.Run("Error_no_WHERE", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users")
		requireError(t, res, "no WHERE", "WHERE clause is required")
	})

	t.Run("Error_unknown_column_in_SELECT", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT nonexistent FROM users WHERE user_id = 'user-001'")
		requireError(t, res, "unknown col SELECT", "not found in schema")
	})

	t.Run("Error_unknown_column_in_WHERE", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE bogus = 'x'")
		requireError(t, res, "unknown col WHERE", "not found in schema")
	})

	t.Run("Error_unknown_view", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM nonexistent_view WHERE id = 'x'")
		requireError(t, res, "unknown view", "not found")
	})

	t.Run("Error_malformed_SQL", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT invalid SQL")
		requireError(t, res, "malformed SQL", "parse error")
	})

	// -----------------------------------------------------------------------
	// K: PK hit with LIMIT (PK+limit is same as PK)
	// -----------------------------------------------------------------------

	t.Run("PK_with_LIMIT", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE user_id = 'user-010' LIMIT 5")
		requireResultCount(t, res, "PK+LIMIT", 1)
	})

	// -----------------------------------------------------------------------
	// L: Cross-field type integrity — verify booleans and numbers come out typed
	// -----------------------------------------------------------------------

	t.Run("Result_type_integrity", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE user_id = 'user-001'")
		rows := requireResultCount(t, res, "type integrity", 1)
		row := rows[0]

		// user_id is a string
		if _, ok := row["user_id"].(string); !ok {
			t.Errorf("user_id type = %T, want string", row["user_id"])
		}
		// age is a number (float64 from JSON)
		if _, ok := row["age"].(float64); !ok {
			t.Errorf("age type = %T, want float64", row["age"])
		}
		// active is a boolean
		if _, ok := row["active"].(bool); !ok {
			t.Errorf("active type = %T, want bool", row["active"])
		}
	})

	// -----------------------------------------------------------------------
	// M: JSON output integrity — marshaling round-trips correctly
	// -----------------------------------------------------------------------

	t.Run("JSON_marshal_roundtrip", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM users WHERE user_id = 'user-001'")
		rows := requireResultCount(t, res, "JSON roundtrip", 1)

		data, err := json.Marshal(res)
		if err != nil {
			t.Fatalf("json.Marshal QueryResult: %v", err)
		}
		if !json.Valid(data) {
			t.Fatal("QueryResult JSON is not valid")
		}

		var decoded natsql.QueryResult
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("json.Unmarshal QueryResult: %v", err)
		}
		if len(decoded.Results) != 1 {
			t.Fatalf("roundtrip: got %d results, want 1", len(decoded.Results))
		}
		// Check the round-tripped JSON matches the original results
		origJSON, _ := json.Marshal(rows[0])
		rtJSON, _ := json.Marshal(decoded.Results[0])
		if string(origJSON) != string(rtJSON) {
			t.Errorf("roundtrip mismatch:\n  original: %s\n  decoded:  %s", origJSON, rtJSON)
		}
	})
}

// ---------------------------------------------------------------------------
// Composite key black-box test
// ---------------------------------------------------------------------------

func TestBlackBox_CompositeKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	streamName := "EVENTS_COMPOSITE"
	createStream(t, ctx, js, streamName)

	cfg := &natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "orders",
				SourceStream: streamName,
				KeyFields:    []string{"org_id", "order_id"},
				KeySeparator: "|",
				Columns: []natsql.ColumnConfig{
					{Name: "org_id", From: "org_id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "order_id", From: "order_id", Type: natsql.ColumnTypeString, PrimaryKey: true},
					{Name: "product", From: "product", Type: natsql.ColumnTypeString},
					{Name: "amount", From: "amount", Type: natsql.ColumnTypeNumber},
				},
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := natsql.New(js, cfg, natsql.WithLogger(logger))
	if err != nil {
		t.Fatalf("New engine: %v", err)
	}
	defer eng.Close()

	if err := eng.Start(ctx); err != nil {
		t.Fatalf("Start engine: %v", err)
	}

	compositeRows := []struct {
		OrgID   string `json:"org_id"`
		OrderID string `json:"order_id"`
		Product string `json:"product"`
		Amount  int    `json:"amount"`
	}{
		{"acme", "ord-001", "widget", 100},
		{"acme", "ord-002", "gadget", 250},
		{"acme", "ord-003", "widget", 150},
		{"globex", "ord-001", "gadget", 300},
		{"globex", "ord-002", "widget", 200},
		{"initech", "ord-001", "thing", 500},
	}

	for _, row := range compositeRows {
		event, _ := json.Marshal(row)
		if _, err := js.Publish(ctx, streamName+".evt", event); err != nil {
			t.Fatalf("Publish %s/%s: %v", row.OrgID, row.OrderID, err)
		}
	}

	pollForCount(t, ctx, eng, "orders", 6)

	t.Run("composite_PK_both_fields", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM orders WHERE org_id = 'acme' AND order_id = 'ord-001'")
		rows := requireResultCount(t, res, "comp PK full", 1)
		assertField(t, rows[0], "org_id", "acme", "comp PK")
		assertField(t, rows[0], "order_id", "ord-001", "comp PK")
		assertField(t, rows[0], "product", "widget", "comp PK")
		assertField(t, rows[0], "amount", float64(100), "comp PK")
	})

	t.Run("composite_PK_partial_no_match", func(t *testing.T) {
		// Only one PK field → doesn't match composite key criteria → full scan
		res := eng.Query(ctx, "SELECT * FROM orders WHERE org_id = 'nonexistent'")
		requireResultCount(t, res, "comp PK partial", 0)
	})

	t.Run("composite_PK_full_miss", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM orders WHERE org_id = 'acme' AND order_id = 'nonexistent'")
		requireResultCount(t, res, "comp PK miss", 0)
	})

	t.Run("composite_where_product", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM orders WHERE product = 'widget'")
		requireResultCount(t, res, "comp product widget", 3)
	})

	t.Run("composite_where_org_acme", func(t *testing.T) {
		res := eng.Query(ctx, "SELECT * FROM orders WHERE org_id = 'acme'")
		requireResultCount(t, res, "comp org acme", 3)
	})
}

// ---------------------------------------------------------------------------
// Facade constructor tests
// ---------------------------------------------------------------------------

func TestFacade_New_NilConfig(t *testing.T) {
	_, err := natsql.New(nil, nil)
	if err == nil {
		t.Fatal("expected error for nil config, got nil")
	}
}

func TestFacade_NewWithNATS_NilConn(t *testing.T) {
	_, err := natsql.NewWithNATS(nil, nil)
	if err == nil {
		t.Fatal("expected error for nil conn, got nil")
	}
}

func TestFacade_NewWithNATS_InvalidConfig(t *testing.T) {
	srv, nc, _ := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	// Config with view that has no name — should fail validation
	_, err := natsql.NewWithNATS(nc, &natsql.Config{
		Views: []natsql.ViewConfig{
			{SourceStream: "s", KeyFields: []string{"k"},
				Columns: []natsql.ColumnConfig{{Name: "k", From: "k", Type: natsql.ColumnTypeString, PrimaryKey: true}}},
		},
	})
	if err == nil {
		t.Fatal("expected error for config with unnamed view")
	}
}

func TestFacade_NewEmbedded_InvalidConfig(t *testing.T) {
	_, err := natsql.NewEmbedded(nil)
	if err == nil {
		t.Fatal("expected error for nil config, got nil")
	}
}

func TestFacade_WithHTTPServer(t *testing.T) {
	eng, err := natsql.NewEmbedded(&natsql.Config{
		Views: []natsql.ViewConfig{
			{
				Name:         "test",
				SourceStream: "test",
				KeyFields:    []string{"id"},
				Columns:      []natsql.ColumnConfig{{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true}},
			},
		},
	}, natsql.WithHTTPServer(":9091"), natsql.WithQueryPort(9092))
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	eng.Close()
}

// ---------------------------------------------------------------------------
// NATS test helpers
// ---------------------------------------------------------------------------

func startEmbeddedNATS(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
	t.Helper()
	opts := &server.Options{
		Port:       -1,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		ServerName: "bb-test",
		NoLog:      true,
		NoSigs:     true,
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("start NATS: %v", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS not ready")
	}
	nc, err := nats.Connect(srv.ClientURL(), nats.Timeout(5*time.Second))
	if err != nil {
		srv.Shutdown()
		t.Fatalf("connect: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		srv.Shutdown()
		t.Fatalf("JetStream: %v", err)
	}
	return srv, nc, js
}

func createStream(t *testing.T, ctx context.Context, js jetstream.JetStream, name string) {
	t.Helper()
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      name,
		Subjects:  []string{name + ".>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		t.Fatalf("create stream %q: %v", name, err)
	}
}

func pollForCount(t *testing.T, ctx context.Context, eng *natsql.Engine, view string, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)

	// Use a column that exists in the view for the != filter
	filterCol := "user_id"
	if view == "orders" {
		filterCol = "org_id"
	}

	for {
		res := eng.Query(ctx, fmt.Sprintf("SELECT * FROM %s WHERE %s != ''", view, filterCol))
		if res.Error == nil && len(res.Results) >= want {
			return
		}
		if time.Now().After(deadline) {
			got := 0
			if res.Error == nil {
				got = len(res.Results)
			}
			t.Fatalf("timed out waiting for %d rows in %s, got %d (error: %v)", want, view, got, res.Error)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
