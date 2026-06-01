package query

import (
	"encoding/json"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
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
	q, err := Parse(`SELECT name, age FROM users WHERE id = 'x'`)
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
	if len(q.Where) != 1 {
		t.Errorf("Where conditions = %v, want 1 condition", q.Where)
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

func TestParse_MultiFrom_Rejected(t *testing.T) {
	_, err := Parse("SELECT * FROM a, b WHERE a.id = 'x'")
	if err == nil {
		t.Fatal("expected error for multi-table SELECT, got nil")
	}
}

func TestParseRejectsNoWhere(t *testing.T) {
	_, err := Parse(`SELECT * FROM users`)
	if err == nil {
		t.Fatal("expected error for missing WHERE clause, got nil")
	}
}

func TestParseRejectsNoWhereWithColumns(t *testing.T) {
	_, err := Parse(`SELECT name, age FROM users`)
	if err == nil {
		t.Fatal("expected error for missing WHERE clause with explicit columns, got nil")
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

// ---------------------------------------------------------------------------
// extractConditions unit tests
// ---------------------------------------------------------------------------

func TestExtractConditions_AND_TwoConditions(t *testing.T) {
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse("SELECT * FROM t WHERE a = 1 AND b = 2")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := stmt.(*sqlparser.Select)
	conds, err := extractConditions(sel.Where.Expr)
	if err != nil {
		t.Fatalf("extractConditions: %v", err)
	}
	if len(conds) != 2 {
		t.Fatalf("expected 2 conditions, got %d", len(conds))
	}
}

func TestExtractConditions_OR_Rejected(t *testing.T) {
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse("SELECT * FROM t WHERE a = 1 OR b = 2")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := stmt.(*sqlparser.Select)
	_, err = extractConditions(sel.Where.Expr)
	if err == nil {
		t.Fatal("expected error for OR, got nil")
	}
}

func TestExtractConditions_UnsupportedExpr(t *testing.T) {
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse("SELECT * FROM t WHERE NOT a = 1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := stmt.(*sqlparser.Select)
	_, err = extractConditions(sel.Where.Expr)
	if err == nil {
		t.Fatal("expected error for unsupported expression, got nil")
	}
}

// ---------------------------------------------------------------------------
// comparisonToCondition unit tests
// ---------------------------------------------------------------------------

func TestComparisonToCondition_InvalidLeftOperand(t *testing.T) {
	// Use a comparison where the left side is a literal, not a column
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse("SELECT * FROM t WHERE 1 = a")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := stmt.(*sqlparser.Select)
	_, err = extractConditions(sel.Where.Expr)
	if err == nil {
		t.Fatal("expected error for invalid left operand, got nil")
	}
}

func TestComparisonToCondition_UnsupportedOperator(t *testing.T) {
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse("SELECT * FROM t WHERE a > 1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := stmt.(*sqlparser.Select)
	_, err = extractConditions(sel.Where.Expr)
	if err == nil {
		t.Fatal("expected error for unsupported operator, got nil")
	}
}

// ---------------------------------------------------------------------------
// extractLimit unit tests
// ---------------------------------------------------------------------------

func TestExtractLimit_Negative(t *testing.T) {
	_, err := Parse("SELECT * FROM t WHERE a = 1 LIMIT -5")
	if err == nil {
		t.Fatal("expected error for negative LIMIT, got nil")
	}
}

func TestExtractLimit_NonInteger(t *testing.T) {
	_, err := Parse("SELECT * FROM t WHERE a = 1 LIMIT 1.5")
	if err == nil {
		t.Fatal("expected error for non-integer LIMIT, got nil")
	}
}

// ---------------------------------------------------------------------------
// extractSelectExprs unit tests
// ---------------------------------------------------------------------------

func TestExtractSelectExprs_NilExprs(t *testing.T) {
	result := extractSelectExprs(nil)
	if result != nil {
		t.Errorf("expected nil for nil exprs, got %v", result)
	}
}

func TestExtractSelectExprs_EmptyExprs(t *testing.T) {
	result := extractSelectExprs(&sqlparser.SelectExprs{})
	if result != nil {
		t.Errorf("expected nil for empty exprs, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// ValuesEqual unit tests
// ---------------------------------------------------------------------------

func TestValuesEqual(t *testing.T) {
	tests := []struct {
		a, b any
		want bool
	}{
		{a: nil, b: nil, want: true},
		{a: nil, b: "x", want: false},
		{a: "x", b: nil, want: false},
		{a: float64(1), b: int64(1), want: true},
		{a: int64(1), b: float64(1), want: true},
		{a: int64(1), b: int64(2), want: false},
		{a: float64(1.5), b: float64(1.5), want: true},
		{a: float64(1.5), b: float64(2.5), want: false},
		{a: true, b: true, want: true},
		{a: true, b: false, want: false},
		{a: "hello", b: "hello", want: true},
		{a: "hello", b: "world", want: false},
		{a: float64(1), b: "1", want: false},
		{a: true, b: "true", want: false},
	}
	for _, tc := range tests {
		got := valuesEqual(tc.a, tc.b)
		if got != tc.want {
			t.Errorf("valuesEqual(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// filterRow unit tests
// ---------------------------------------------------------------------------

func TestFilterRow_OpEq_Pass(t *testing.T) {
	if !filterRow(map[string]any{"a": "x"}, []Condition{{Column: "a", Op: OpEq, Value: "x"}}) {
		t.Error("expected filterRow to pass for matching OpEq")
	}
}

func TestFilterRow_OpNeq_Pass(t *testing.T) {
	if !filterRow(map[string]any{"a": "x"}, []Condition{{Column: "a", Op: OpNeq, Value: "y"}}) {
		t.Error("expected filterRow to pass for non-matching OpNeq")
	}
}

func TestFilterRow_OpNeq_Fail(t *testing.T) {
	if filterRow(map[string]any{"a": "x"}, []Condition{{Column: "a", Op: OpNeq, Value: "x"}}) {
		t.Error("expected filterRow to fail for matching OpNeq")
	}
}

func TestFilterRow_OpIn_Match(t *testing.T) {
	if !filterRow(map[string]any{"a": "x"}, []Condition{{Column: "a", Op: OpIn, Value: []any{"x", "y"}}}) {
		t.Error("expected filterRow to pass for matching OpIn")
	}
}

func TestFilterRow_OpIn_NoMatch(t *testing.T) {
	if filterRow(map[string]any{"a": "z"}, []Condition{{Column: "a", Op: OpIn, Value: []any{"x", "y"}}}) {
		t.Error("expected filterRow to fail for non-matching OpIn")
	}
}

func TestFilterRow_OpIn_InvalidValue(t *testing.T) {
	row := map[string]any{"a": 1}
	conds := []Condition{{Column: "a", Op: OpIn, Value: "not-a-slice"}}
	// If OpIn value is not []any, filter should return false (no match)
	if filterRow(row, conds) {
		t.Error("filterRow should return false for OpIn with non-slice value")
	}
}

func TestFilterRow_MissingColumn(t *testing.T) {
	row := map[string]any{"a": 1}
	conds := []Condition{{Column: "missing", Op: OpEq, Value: "x"}}
	if filterRow(row, conds) {
		t.Error("filterRow should return false for missing column")
	}
}

// ---------------------------------------------------------------------------
// projectRow unit tests
// ---------------------------------------------------------------------------

func TestProjectRow_StarInExplicit(t *testing.T) {
	row := map[string]any{"a": 1, "b": 2}
	result := projectRow(row, []string{"*"})
	if len(result) != 2 {
		t.Errorf("expected full row with *, got %d cols", len(result))
	}
}

// ---------------------------------------------------------------------------
// extractValue unit tests
// ---------------------------------------------------------------------------

func TestExtractValue_NullVal(t *testing.T) {
	v, err := extractValue(&sqlparser.NullVal{})
	if err != nil {
		t.Fatalf("extractValue(NullVal): %v", err)
	}
	if v != nil {
		t.Errorf("expected nil, got %v", v)
	}
}

func TestExtractValue_BoolVal(t *testing.T) {
	v, err := extractValue(sqlparser.BoolVal(true))
	if err != nil {
		t.Fatalf("extractValue(BoolVal): %v", err)
	}
	if v != true {
		t.Errorf("expected true, got %v", v)
	}
}

func TestExtractValue_UnsupportedType(t *testing.T) {
	_, err := extractValue(&sqlparser.ColName{})
	if err == nil {
		t.Fatal("expected error for unsupported value type")
	}
}

// ---------------------------------------------------------------------------
// additional Parse error cases
// ---------------------------------------------------------------------------

func TestParse_NonSelectStatement(t *testing.T) {
	_, err := Parse("CREATE TABLE t (id int)")
	if err == nil {
		t.Fatal("expected error for non-SELECT statement")
	}
}

func TestParse_NonAliasedFrom(t *testing.T) {
	_, err := Parse("SELECT * FROM (SELECT 1) AS sub WHERE a = 1")
	if err == nil {
		t.Fatal("expected error for non-aliased FROM")
	}
}

// ---------------------------------------------------------------------------
// literalToGo unit tests
// ---------------------------------------------------------------------------

func TestLiteralToGo_IntParseError(t *testing.T) {
	// A very large int literal that ParseInt can't handle falls back to the raw value
	_ = literalToGo(&sqlparser.Literal{Type: sqlparser.IntVal, Val: "999999999999999999999999"})
}

func TestLiteralToGo_FloatVal(t *testing.T) {
	v := literalToGo(&sqlparser.Literal{Type: sqlparser.FloatVal, Val: "3.14"})
	if v != float64(3.14) {
		t.Errorf("expected 3.14, got %v", v)
	}
}

func TestLiteralToGo_DecimalVal(t *testing.T) {
	v := literalToGo(&sqlparser.Literal{Type: sqlparser.DecimalVal, Val: "10.5"})
	if v != float64(10.5) {
		t.Errorf("expected 10.5, got %v", v)
	}
}

func TestLiteralToGo_UnknownType(t *testing.T) {
	v := literalToGo(&sqlparser.Literal{Type: sqlparser.HexVal, Val: "0x1A"})
	if v != "0x1A" {
		t.Errorf("expected raw value, got %v", v)
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

// ---------------------------------------------------------------------------
// Unsupported construct rejection tests (FND-02, CR-05)
// ---------------------------------------------------------------------------

func TestParse_RejectsDistinct(t *testing.T) {
	_, err := Parse(`SELECT DISTINCT name FROM users WHERE id = 'x'`)
	if err == nil {
		t.Fatal("expected error for DISTINCT, got nil")
	}
	if !strings.Contains(err.Error(), "DISTINCT") {
		t.Errorf("error should mention DISTINCT: %v", err)
	}
}

func TestParse_RejectsOrderBy(t *testing.T) {
	_, err := Parse(`SELECT * FROM users WHERE id = 'x' ORDER BY name`)
	if err == nil {
		t.Fatal("expected error for ORDER BY, got nil")
	}
	if !strings.Contains(err.Error(), "ORDER BY") {
		t.Errorf("error should mention ORDER BY: %v", err)
	}
}

func TestParse_RejectsGroupBy(t *testing.T) {
	_, err := Parse(`SELECT name FROM users WHERE id = 'x' GROUP BY name`)
	if err == nil {
		t.Fatal("expected error for GROUP BY, got nil")
	}
	if !strings.Contains(err.Error(), "GROUP BY") {
		t.Errorf("error should mention GROUP BY: %v", err)
	}
}

func TestParse_RejectsHaving(t *testing.T) {
	_, err := Parse(`SELECT name FROM users WHERE id = 'x' GROUP BY name HAVING COUNT(*) > 1`)
	if err == nil {
		t.Fatal("expected error for HAVING, got nil")
	}
	if !strings.Contains(err.Error(), "HAVING") {
		t.Errorf("error should mention HAVING: %v", err)
	}
}

func TestParse_RejectsAggregationCount(t *testing.T) {
	_, err := Parse(`SELECT COUNT(*) FROM users WHERE id = 'x'`)
	if err == nil {
		t.Fatal("expected error for aggregation, got nil")
	}
}

func TestParse_RejectsNonColumnSelect(t *testing.T) {
	_, err := Parse(`SELECT 1 FROM users WHERE id = 'x'`)
	if err == nil {
		t.Fatal("expected error for non-column SELECT, got nil")
	}
}

func TestParse_RejectsStringLiteralSelect(t *testing.T) {
	_, err := Parse(`SELECT 'hello' FROM users WHERE id = 'x'`)
	if err == nil {
		t.Fatal("expected error for string literal SELECT, got nil")
	}
}

func TestParse_RejectsExpressionSelect(t *testing.T) {
	_, err := Parse(`SELECT a + b FROM users WHERE id = 'x'`)
	if err == nil {
		t.Fatal("expected error for expression SELECT, got nil")
	}
}

func TestParse_ValidQueryStillWorksAfterRejection(t *testing.T) {
	q, err := Parse(`SELECT name, age FROM users WHERE id = 'x'`)
	if err != nil {
		t.Fatalf("valid query rejected: %v", err)
	}
	if q.From != "users" {
		t.Errorf("From = %q, want %q", q.From, "users")
	}
	if len(q.Select) != 2 {
		t.Errorf("Select = %v, want 2 columns", q.Select)
	}
}
