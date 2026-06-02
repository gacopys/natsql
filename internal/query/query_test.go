package query

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats-server/v2/server"

	"github.com/gacopys/natsql/internal/kv"
)

// ---------------------------------------------------------------------------
// Test schemas
// ---------------------------------------------------------------------------

var testSchema = &kv.ViewSchema{
	Name: "test_users",
	Columns: []kv.ColumnSchema{
		{Name: "id", Type: "string", PrimaryKey: true},
		{Name: "name", Type: "string"},
		{Name: "age", Type: "number"},
		{Name: "active", Type: "boolean"},
	},
	KeyFields:    []string{"id"},
	KeySeparator: "|",
}

// ---------------------------------------------------------------------------
// Validator unit tests
// ---------------------------------------------------------------------------

func TestValidateValid(t *testing.T) {
	q := &ValidatedQuery{
		Select: []string{"id", "name"},
		From:   "test_users",
		Where:  []Condition{{Column: "id", Op: OpEq, Value: "abc"}},
	}
	err := Validate(q, testSchema)
	if err != nil {
		t.Fatalf("Validate failed for valid query: %v", err)
	}
}

func TestValidateStarSelect(t *testing.T) {
	q := &ValidatedQuery{
		Select: nil, // nil = SELECT *
		From:   "test_users",
		Where:  []Condition{{Column: "id", Op: OpEq, Value: "abc"}},
	}
	err := Validate(q, testSchema)
	if err != nil {
		t.Fatalf("Validate failed for SELECT *: %v", err)
	}
}

func TestValidateUnknownColumnInSelect(t *testing.T) {
	q := &ValidatedQuery{
		Select: []string{"nonexistent"},
		From:   "test_users",
		Where:  []Condition{{Column: "id", Op: OpEq, Value: "abc"}},
	}
	err := Validate(q, testSchema)
	if err == nil {
		t.Fatal("expected error for unknown column in SELECT, got nil")
	}
}

func TestValidateUnknownColumnInWhere(t *testing.T) {
	q := &ValidatedQuery{
		Select: nil,
		From:   "test_users",
		Where:  []Condition{{Column: "bogus_col", Op: OpEq, Value: "abc"}},
	}
	err := Validate(q, testSchema)
	if err == nil {
		t.Fatal("expected error for unknown column in WHERE, got nil")
	}
}

// ---------------------------------------------------------------------------
// Planner unit tests
// ---------------------------------------------------------------------------

func TestBuildPlanPKLookup(t *testing.T) {
	q := &ValidatedQuery{
		Select: nil,
		From:   "test_users",
		Where:  []Condition{{Column: "id", Op: OpEq, Value: "abc"}},
	}
	plan, err := BuildPlan(q, testSchema)
	if err != nil {
		t.Fatalf("BuildPlan failed: %v", err)
	}
	if _, ok := plan.(*PKLookupPlan); !ok {
		t.Fatalf("expected PKLookupPlan, got %T", plan)
	}
	pkPlan := plan.(*PKLookupPlan)
	if pkPlan.ViewName != "test_users" {
		t.Errorf("ViewName = %q, want %q", pkPlan.ViewName, "test_users")
	}
	if len(pkPlan.PkParts) != 1 || pkPlan.PkParts[0] != "abc" {
		t.Errorf("PkParts = %v, want %v", pkPlan.PkParts, []string{"abc"})
	}
	if pkPlan.Separator != "|" {
		t.Errorf("Separator = %q, want %q", pkPlan.Separator, "|")
	}
}

func TestBuildPlanFullScanNonPKWhere(t *testing.T) {
	q := &ValidatedQuery{
		Select: nil,
		From:   "test_users",
		Where:  []Condition{{Column: "name", Op: OpEq, Value: "Alice"}},
	}
	plan, err := BuildPlan(q, testSchema)
	if err != nil {
		t.Fatalf("BuildPlan failed: %v", err)
	}
	if _, ok := plan.(*FullScanPlan); !ok {
		t.Fatalf("expected FullScanPlan, got %T", plan)
	}
}

func TestBuildPlanFullScanNonEqPK(t *testing.T) {
	q := &ValidatedQuery{
		Select: nil,
		From:   "test_users",
		Where:  []Condition{{Column: "id", Op: OpNeq, Value: "abc"}},
	}
	plan, err := BuildPlan(q, testSchema)
	if err != nil {
		t.Fatalf("BuildPlan failed: %v", err)
	}
	if _, ok := plan.(*FullScanPlan); !ok {
		t.Fatalf("expected FullScanPlan for non-equality PK, got %T", plan)
	}
}

func TestBuildPlan_ContradictoryPK_ReturnsEmptyPlan(t *testing.T) {
	q := &ValidatedQuery{
		Select: nil,
		From:   "test_users",
		Where: []Condition{
			{Column: "id", Op: OpEq, Value: "a"},
			{Column: "id", Op: OpEq, Value: "b"},
		},
	}
	plan, err := BuildPlan(q, testSchema)
	if err != nil {
		t.Fatalf("BuildPlan failed: %v", err)
	}
	if _, ok := plan.(*EmptyPlan); !ok {
		t.Fatalf("expected EmptyPlan for contradictory PK, got %T", plan)
	}
}

func TestBuildPlan_DuplicatePK_NotContradictory(t *testing.T) {
	q := &ValidatedQuery{
		Select: nil,
		From:   "test_users",
		Where: []Condition{
			{Column: "id", Op: OpEq, Value: "a"},
			{Column: "id", Op: OpEq, Value: "a"},
		},
	}
	plan, err := BuildPlan(q, testSchema)
	if err != nil {
		t.Fatalf("BuildPlan failed: %v", err)
	}
	if _, ok := plan.(*PKLookupPlan); !ok {
		t.Fatalf("expected PKLookupPlan for duplicate same-value PK, got %T", plan)
	}
	pkPlan := plan.(*PKLookupPlan)
	// All conditions should be in Where (including the PK condition)
	if len(pkPlan.Where) != 2 {
		t.Errorf("expected 2 Where conditions (all preserved), got %d", len(pkPlan.Where))
	}
}

func TestBuildPlan_AllConditionsKeptAsPostFilters(t *testing.T) {
	q := &ValidatedQuery{
		Select: nil,
		From:   "test_users",
		Where: []Condition{
			{Column: "id", Op: OpEq, Value: "u1"},
			{Column: "name", Op: OpEq, Value: "Alice"},
		},
	}
	plan, err := BuildPlan(q, testSchema)
	if err != nil {
		t.Fatalf("BuildPlan failed: %v", err)
	}
	pkPlan, ok := plan.(*PKLookupPlan)
	if !ok {
		t.Fatalf("expected PKLookupPlan, got %T", plan)
	}
	// All conditions should be in Where (both PK and non-PK)
	if len(pkPlan.Where) != 2 {
		t.Errorf("expected 2 Where conditions (all preserved), got %d", len(pkPlan.Where))
	}
}

func TestEmptyPlan_Execute(t *testing.T) {
	// EmptyPlan returns empty results regardless of context or KV
	plan := &EmptyPlan{}
	results, err := plan.Execute(context.Background(), nil)
	if err != nil {
		t.Fatalf("EmptyPlan.Execute failed: %v", err)
	}
	if results == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestBuildPlanPKLookupWithLimit(t *testing.T) {
	q := &ValidatedQuery{
		Select: nil,
		From:   "test_users",
		Where:  []Condition{{Column: "id", Op: OpEq, Value: "abc"}},
		Limit:  10,
	}
	plan, err := BuildPlan(q, testSchema)
	if err != nil {
		t.Fatalf("BuildPlan failed: %v", err)
	}
	if _, ok := plan.(*PKLookupPlan); !ok {
		t.Fatalf("expected PKLookupPlan, got %T", plan)
	}
	// Limit should be discarded for PK lookup — PK returns 0 or 1 rows
}

// ---------------------------------------------------------------------------
// Embedded NATS helpers for integration tests
// ---------------------------------------------------------------------------

func startEmbeddedNATS(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
	t.Helper()
	opts := &server.Options{
		Port:       -1,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		ServerName: "test-query",
		NoLog:      true,
		NoSigs:     true,
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to start NATS server: %v", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready within 5 seconds")
	}
	nc, err := nats.Connect(srv.ClientURL(), nats.Timeout(5*time.Second))
	if err != nil {
		srv.Shutdown()
		t.Fatalf("failed to connect: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		srv.Shutdown()
		t.Fatalf("failed to create JetStream context: %v", err)
	}
	return srv, nc, js
}

// setupTestData creates a KV bucket, stores a schema, and inserts test rows.
func setupTestData(t *testing.T, ctx context.Context, js jetstream.JetStream) jetstream.KeyValue {
	t.Helper()
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	// Store schema
	if err := kv.StoreSchema(ctx, kvb, testSchema.Name, testSchema); err != nil {
		t.Fatalf("StoreSchema failed: %v", err)
	}

	// Insert test rows
	rows := []struct {
		pk  string
		val map[string]any
	}{
		{"u1", map[string]any{"id": "u1", "name": "Alice", "age": float64(30), "active": true}},
		{"u2", map[string]any{"id": "u2", "name": "Bob", "age": float64(25), "active": false}},
		{"u3", map[string]any{"id": "u3", "name": "Charlie", "age": float64(35), "active": true}},
		{"u4", map[string]any{"id": "u4", "name": "Diana", "age": float64(28), "active": true}},
	}

	for _, row := range rows {
		data, err := json.Marshal(row.val)
		if err != nil {
			t.Fatalf("marshal row failed: %v", err)
		}
		key := kv.BuildPkKey(testSchema.Name, []string{row.pk}, testSchema.KeySeparator)
		if _, err := kvb.Put(ctx, key, data); err != nil {
			t.Fatalf("Put(%q) failed: %v", key, err)
		}
	}

	return kvb
}

// ---------------------------------------------------------------------------
// Executor integration tests
// ---------------------------------------------------------------------------

func TestPKLookupFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	kvb := setupTestData(t, ctx, js)

	plan := &PKLookupPlan{
		ViewName:  "test_users",
		PkParts:   []string{"u1"},
		Separator: "|",
		Columns:   nil, // all columns
	}

	results, err := plan.Execute(ctx, kvb)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0]["id"] != "u1" {
		t.Errorf("id = %v, want %q", results[0]["id"], "u1")
	}
	if results[0]["name"] != "Alice" {
		t.Errorf("name = %v, want %q", results[0]["name"], "Alice")
	}
}

func TestPKLookupNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	kvb := setupTestData(t, ctx, js)

	plan := &PKLookupPlan{
		ViewName:  "test_users",
		PkParts:   []string{"nonexistent"},
		Separator: "|",
		Columns:   nil,
	}

	results, err := plan.Execute(ctx, kvb)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results for missing PK, got %d", len(results))
	}
}

func TestFullScanAll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	kvb := setupTestData(t, ctx, js)

	plan := &FullScanPlan{
		ViewName: "test_users",
		Columns:  nil, // all columns
	}

	results, err := plan.Execute(ctx, kvb)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}
}

func TestFullScanWithWhere(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	kvb := setupTestData(t, ctx, js)

	plan := &FullScanPlan{
		ViewName: "test_users",
		Columns:  nil,
		Where:    []Condition{{Column: "name", Op: OpEq, Value: "Alice"}},
	}

	results, err := plan.Execute(ctx, kvb)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result (Alice), got %d", len(results))
	}
	if results[0]["name"] != "Alice" {
		t.Errorf("name = %v, want %q", results[0]["name"], "Alice")
	}
}

func TestFullScanWithLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	kvb := setupTestData(t, ctx, js)

	plan := &FullScanPlan{
		ViewName: "test_users",
		Columns:  nil,
		Limit:    2,
	}

	results, err := plan.Execute(ctx, kvb)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results with LIMIT 2, got %d", len(results))
	}
}

func TestProjectionSelectStar(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	kvb := setupTestData(t, ctx, js)

	plan := &PKLookupPlan{
		ViewName:  "test_users",
		PkParts:   []string{"u1"},
		Separator: "|",
		Columns:   nil, // SELECT *
	}

	results, err := plan.Execute(ctx, kvb)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	// Should have all 4 columns
	expectedCols := []string{"id", "name", "age", "active"}
	for _, col := range expectedCols {
		if _, ok := results[0][col]; !ok {
			t.Errorf("missing column %q in SELECT * results", col)
		}
	}
}

func TestProjectionSelectCols(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	kvb := setupTestData(t, ctx, js)

	plan := &PKLookupPlan{
		ViewName:  "test_users",
		PkParts:   []string{"u1"},
		Separator: "|",
		Columns:   []string{"name", "age"},
	}

	results, err := plan.Execute(ctx, kvb)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if _, ok := results[0]["id"]; ok {
		t.Error("column 'id' should not be in projected results")
	}
	if results[0]["name"] != "Alice" {
		t.Errorf("name = %v, want %q", results[0]["name"], "Alice")
	}
	if results[0]["age"] != float64(30) {
		t.Errorf("age = %v, want %v", results[0]["age"], float64(30))
	}
}

func TestProjectionMissingCol(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	kvb := setupTestData(t, ctx, js)

	plan := &PKLookupPlan{
		ViewName:  "test_users",
		PkParts:   []string{"u1"},
		Separator: "|",
		Columns:   []string{"nonexistent"},
	}

	results, err := plan.Execute(ctx, kvb)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	// Missing column should be null in result per D-31
	val, hasKey := results[0]["nonexistent"]
	if !hasKey {
		t.Error("column 'nonexistent' should be present (as null) in results per D-31")
	}
	if val != nil {
		t.Errorf("column 'nonexistent' should be nil, got %v", val)
	}
}

func TestTypedJSON(t *testing.T) {
	// Verify that QueryResult produces typed JSON per D-30
	r := QueryResult{
		Results: []map[string]any{
			{
				"name":   "Alice",
				"age":    float64(30),
				"active": true,
				"null_col": nil,
			},
		},
	}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	// String quoted, number unquoted, bool literal, null for nil
	if !json.Valid(data) {
		t.Fatal("invalid JSON output")
	}
	var parsed QueryResult
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if len(parsed.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(parsed.Results))
	}
	row := parsed.Results[0]
	if row["name"] != "Alice" {
		t.Errorf("name = %v, want %q (string)", row["name"], "Alice")
	}
	if row["age"] != float64(30) {
		t.Errorf("age = %v, want %v (number)", row["age"], float64(30))
	}
	if row["active"] != true {
		t.Errorf("active = %v, want true (bool)", row["active"])
	}
	// nil values are present in JSON as null
	if _, ok := row["null_col"]; !ok {
		t.Error("null_col should be present in JSON (explicit null)")
	}
}

func TestViewNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	// Query against a view that has no schema stored
	schema, err := kv.LoadSchema(ctx, kvb, "nonexistent_view")
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}
	if schema != nil {
		t.Fatal("expected nil schema for nonexistent view")
	}
}

func TestPKEncoding_SpecialCharacters_WriteRead(t *testing.T) {
	// Black-box integration test: write a row with special PK characters
	// using BuildPkKey (simulating write path), then read it back using
	// PKLookupPlan with BuildPkKey (simulating read path).
	// Proves write and read produce identical KV keys.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	// Use a test schema with "|" separator
	schema := &kv.ViewSchema{
		Name:         "special_pk_test",
		Columns:      []kv.ColumnSchema{{Name: "id", Type: "string", PrimaryKey: true}},
		KeyFields:    []string{"id"},
		KeySeparator: "|",
	}

	// PK values containing each special character
	specialPKs := []struct {
		pkParts []string
		label   string
	}{
		{[]string{"abc"}, "plain"},
		{[]string{"a_b"}, "underscore"},
		{[]string{"a|b"}, "pipe"},
		{[]string{"a/b"}, "slash"},
		{[]string{"a*b"}, "star"},
		{[]string{"a>b"}, "greater"},
		{[]string{"a__b"}, "double_underscore"},
		{[]string{"a|b/c*d>e_f"}, "all_special"},
	}

	// Write test data
	for _, sp := range specialPKs {
		rowData := map[string]any{
			"id":   sp.pkParts[0],
			"name": sp.label,
		}
		data, err := json.Marshal(rowData)
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}
		key := kv.BuildPkKey(schema.Name, sp.pkParts, schema.KeySeparator)
		if _, err := kvb.Put(ctx, key, data); err != nil {
			t.Fatalf("Put(%q) failed: %v", key, err)
		}
	}

	// Read back using PKLookupPlan (same BuildPkKey call)
	for _, sp := range specialPKs {
		plan := &PKLookupPlan{
			ViewName:  schema.Name,
			PkParts:   sp.pkParts,
			Separator: schema.KeySeparator,
			Columns:   nil,
		}
		results, err := plan.Execute(ctx, kvb)
		if err != nil {
			t.Fatalf("Execute failed for %q: %v", sp.label, err)
		}
		if len(results) != 1 {
			t.Errorf("expected 1 result for %q (pkParts=%v), got %d", sp.label, sp.pkParts, len(results))
			continue
		}
		if results[0]["name"] != sp.label {
			t.Errorf("name mismatch for %q: got %v, want %q", sp.label, results[0]["name"], sp.label)
		}
	}
}
