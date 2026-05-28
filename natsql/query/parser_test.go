package query

import (
	"encoding/json"
	"testing"
)

// ---------------------------------------------------------------------------
// Parse function tests
// ---------------------------------------------------------------------------

func TestParseSelectStar(t *testing.T) {
	q, err := Parse(`SELECT * FROM users WHERE id = 'abc'`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	// Select=nil means "*"
	if q.Select != nil {
		t.Errorf("Select = %v, want nil (star)", q.Select)
	}
	if q.From != "users" {
		t.Errorf("From = %q, want %q", q.From, "users")
	}
	if len(q.Where) != 1 {
		t.Fatalf("Where = %v, want 1 condition", q.Where)
	}
	if q.Where[0].Column != "id" {
		t.Errorf("Where[0].Column = %q, want %q", q.Where[0].Column, "id")
	}
	if q.Where[0].Op != OpEq {
		t.Errorf("Where[0].Op = %q, want %q", q.Where[0].Op, OpEq)
	}
	if q.Where[0].Value != "abc" {
		t.Errorf("Where[0].Value = %v, want %q", q.Where[0].Value, "abc")
	}
}

func TestParseSelectColumns(t *testing.T) {
	q, err := Parse(`SELECT name, age FROM users`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(q.Select) != 2 {
		t.Fatalf("Select = %v, want 2 columns", q.Select)
	}
	if q.Select[0] != "name" {
		t.Errorf("Select[0] = %q, want %q", q.Select[0], "name")
	}
	if q.Select[1] != "age" {
		t.Errorf("Select[1] = %q, want %q", q.Select[1], "age")
	}
	if q.From != "users" {
		t.Errorf("From = %q, want %q", q.From, "users")
	}
	if len(q.Where) != 0 {
		t.Errorf("Where = %v, want 0 conditions (no WHERE clause)", q.Where)
	}
	if q.Limit != 0 {
		t.Errorf("Limit = %d, want 0", q.Limit)
	}
}

func TestParseWithLimit(t *testing.T) {
	q, err := Parse(`SELECT * FROM users WHERE id = 'abc' LIMIT 10`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if q.Select != nil {
		t.Errorf("Select = %v, want nil (star)", q.Select)
	}
	if q.From != "users" {
		t.Errorf("From = %q, want %q", q.From, "users")
	}
	if len(q.Where) != 1 {
		t.Fatalf("Where = %v, want 1 condition", q.Where)
	}
	if q.Limit != 10 {
		t.Errorf("Limit = %d, want 10", q.Limit)
	}
}

func TestParseWithInClause(t *testing.T) {
	q, err := Parse(`SELECT * FROM users WHERE id IN ('a','b')`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(q.Where) != 1 {
		t.Fatalf("Where = %v, want 1 condition", q.Where)
	}
	if q.Where[0].Column != "id" {
		t.Errorf("Where[0].Column = %q, want %q", q.Where[0].Column, "id")
	}
	if q.Where[0].Op != OpIn {
		t.Errorf("Where[0].Op = %q, want %q", q.Where[0].Op, OpIn)
	}
	vals, ok := q.Where[0].Value.([]any)
	if !ok {
		t.Fatalf("Where[0].Value type = %T, want []any", q.Where[0].Value)
	}
	if len(vals) != 2 {
		t.Fatalf("Where[0].Value = %v, want 2 values", vals)
	}
	if vals[0] != "a" {
		t.Errorf("vals[0] = %v, want %q", vals[0], "a")
	}
	if vals[1] != "b" {
		t.Errorf("vals[1] = %v, want %q", vals[1], "b")
	}
}

func TestParseRejectsNoWhere(t *testing.T) {
	_, err := Parse(`SELECT * FROM users`)
	if err == nil {
		t.Fatal("expected error for missing WHERE clause, got nil")
	}
}

func TestParseMalformedSQL(t *testing.T) {
	_, err := Parse(`SELECT invalid SQL`)
	if err == nil {
		t.Fatal("expected error for malformed SQL, got nil")
	}
}

func TestParseWithNotEqual(t *testing.T) {
	q, err := Parse(`SELECT * FROM users WHERE id != 'abc'`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(q.Where) != 1 {
		t.Fatalf("Where = %v, want 1 condition", q.Where)
	}
	if q.Where[0].Column != "id" {
		t.Errorf("Where[0].Column = %q, want %q", q.Where[0].Column, "id")
	}
	if q.Where[0].Op != OpNeq {
		t.Errorf("Where[0].Op = %q, want %q", q.Where[0].Op, OpNeq)
	}
	if q.Where[0].Value != "abc" {
		t.Errorf("Where[0].Value = %v, want %q", q.Where[0].Value, "abc")
	}
}

// ---------------------------------------------------------------------------
// QueryResult JSON marshaling tests
// ---------------------------------------------------------------------------

func TestQueryResultMarshalWithRows(t *testing.T) {
	r := QueryResult{
		Results: []map[string]any{
			{"name": "Alice", "age": float64(30)},
		},
		Error: nil,
	}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	got := string(data)
	// Strings quoted, numbers unquoted
	want := `{"results":[{"age":30,"name":"Alice"}],"error":null}`
	if got != want {
		t.Errorf("JSON = %s, want %s", got, want)
	}

	// Verify round-trip
	var parsed QueryResult
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if parsed.Error != nil {
		t.Errorf("Error = %v, want nil", *parsed.Error)
	}
	if len(parsed.Results) != 1 {
		t.Fatalf("Results = %v, want 1 row", parsed.Results)
	}
	if parsed.Results[0]["name"] != "Alice" {
		t.Errorf("Results[0].name = %v, want %q", parsed.Results[0]["name"], "Alice")
	}
}

func TestQueryResultMarshalWithError(t *testing.T) {
	errMsg := "view not found"
	r := QueryResult{
		Results: []map[string]any{},
		Error:   &errMsg,
	}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	got := string(data)
	want := `{"results":[],"error":"view not found"}`
	if got != want {
		t.Errorf("JSON = %s, want %s", got, want)
	}
}

func TestQueryResultMarshalEmpty(t *testing.T) {
	r := QueryResult{
		Results: []map[string]any{},
		Error:   nil,
	}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	got := string(data)
	want := `{"results":[],"error":null}`
	if got != want {
		t.Errorf("JSON = %s, want %s", got, want)
	}
}
