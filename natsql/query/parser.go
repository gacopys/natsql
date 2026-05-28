package query

import (
	"errors"
	"fmt"
	"strconv"

	"vitess.io/vitess/go/vt/sqlparser"
)

// Parse parses a SQL SELECT statement and returns a ValidatedQuery.
// Uses vitess sqlparser for SQL parsing.
func Parse(sql string) (*ValidatedQuery, error) {
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, errors.New("only SELECT statements are supported")
	}

	// Extract the FROM table (single table only for v1)
	if len(sel.From) == 0 {
		return nil, errors.New("FROM clause is required")
	}
	if len(sel.From) > 1 {
		return nil, errors.New("only single-table SELECT is supported")
	}

	aliased, ok := sel.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, errors.New("only simple SELECT FROM view is supported")
	}

	tblName, ok := aliased.Expr.(sqlparser.TableName)
	if !ok {
		return nil, errors.New("only simple SELECT FROM view is supported")
	}

	q := &ValidatedQuery{
		From: tblName.Name.String(),
	}

	// Extract SELECT expressions
	if sel.SelectExprs != nil {
		q.Select = extractSelectExprs(sel.SelectExprs)
	}

	// Extract WHERE clause
	if sel.Where == nil {
		return nil, errors.New("WHERE clause is required")
	}
	conditions, err := extractConditions(sel.Where.Expr)
	if err != nil {
		return nil, err
	}
	q.Where = conditions

	// Extract LIMIT
	if sel.Limit != nil {
		limit, err := extractLimit(sel.Limit)
		if err != nil {
			return nil, err
		}
		q.Limit = limit
	}

	return q, nil
}

// extractSelectExprs converts vitess SelectExprs to column name slice.
// Returns nil for SELECT *.
func extractSelectExprs(exprs *sqlparser.SelectExprs) []string {
	if exprs == nil || len(exprs.Exprs) == 0 {
		return nil
	}

	// Check if single "*"
	if len(exprs.Exprs) == 1 {
		if _, ok := exprs.Exprs[0].(*sqlparser.StarExpr); ok {
			return nil // nil means "*"
		}
	}

	cols := make([]string, 0, len(exprs.Exprs))
	for _, expr := range exprs.Exprs {
		switch e := expr.(type) {
		case *sqlparser.AliasedExpr:
			col, ok := e.Expr.(*sqlparser.ColName)
			if !ok {
				continue
			}
			cols = append(cols, col.Name.String())
		case *sqlparser.StarExpr:
			cols = append(cols, "*")
		}
	}
	return cols
}

// extractConditions recursively walks a WHERE expression and extracts conditions.
// Only AND-connected ComparisonExpr nodes are supported for v1.
func extractConditions(expr sqlparser.Expr) ([]Condition, error) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		cond, err := comparisonToCondition(e)
		if err != nil {
			return nil, err
		}
		return []Condition{*cond}, nil

	case *sqlparser.AndExpr:
		left, err := extractConditions(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := extractConditions(e.Right)
		if err != nil {
			return nil, err
		}
		return append(left, right...), nil

	case *sqlparser.OrExpr:
		return nil, errors.New("OR conditions are not supported in v1")

	default:
		return nil, fmt.Errorf("unsupported WHERE expression: %T", e)
	}
}

// comparisonToCondition converts a vitess ComparisonExpr to a Condition.
func comparisonToCondition(ce *sqlparser.ComparisonExpr) (*Condition, error) {
	// Extract column name from Left side
	colName, ok := ce.Left.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("unsupported left operand in WHERE: %T", ce.Left)
	}
	column := colName.Name.String()

	// Map operator
	var op Op
	switch ce.Operator {
	case sqlparser.EqualOp:
		op = OpEq
	case sqlparser.NotEqualOp:
		op = OpNeq
	case sqlparser.InOp:
		op = OpIn
	default:
		return nil, fmt.Errorf("unsupported operator: %v", ce.Operator)
	}

	// Extract value from Right side
	value, err := extractValue(ce.Right)
	if err != nil {
		return nil, fmt.Errorf("extracting value for column %q: %w", column, err)
	}

	return &Condition{
		Column: column,
		Op:     op,
		Value:  value,
	}, nil
}

// extractValue extracts the Go value from a vitess expression.
// Handles Literal (strings, ints, floats) and ValTuple (for IN).
func extractValue(expr sqlparser.Expr) (any, error) {
	switch e := expr.(type) {
	case *sqlparser.Literal:
		return literalToGo(e), nil

	case sqlparser.ValTuple:
		vals := make([]any, len(e))
		for i, v := range e {
			lit, ok := v.(*sqlparser.Literal)
			if !ok {
				return nil, fmt.Errorf("unsupported value type in tuple: %T", v)
			}
			vals[i] = literalToGo(lit)
		}
		return vals, nil

	case *sqlparser.NullVal:
		return nil, nil

	default:
		return nil, fmt.Errorf("unsupported value type: %T", expr)
	}
}

// literalToGo converts a vitess Literal to a Go value.
func literalToGo(lit *sqlparser.Literal) any {
	switch lit.Type {
	case sqlparser.StrVal:
		return lit.Val
	case sqlparser.IntVal:
		n, err := strconv.ParseInt(lit.Val, 10, 64)
		if err == nil {
			return n
		}
		return lit.Val
	case sqlparser.FloatVal, sqlparser.DecimalVal:
		f, err := strconv.ParseFloat(lit.Val, 64)
		if err == nil {
			return f
		}
		return lit.Val
	default:
		return lit.Val
	}
}

// extractLimit extracts the limit value from a vitess Limit clause.
func extractLimit(limit *sqlparser.Limit) (int, error) {
	lit, ok := limit.Rowcount.(*sqlparser.Literal)
	if !ok {
		return 0, fmt.Errorf("unsupported LIMIT expression: %T", limit.Rowcount)
	}
	if lit.Type != sqlparser.IntVal {
		return 0, errors.New("LIMIT must be an integer")
	}
	n, err := strconv.ParseInt(lit.Val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid LIMIT value: %w", err)
	}
	if n < 0 {
		return 0, errors.New("LIMIT must be non-negative")
	}
	return int(n), nil
}
