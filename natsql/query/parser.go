package query

import (
	"errors"
)

// Parse parses a SQL SELECT statement and returns a ValidatedQuery.
// Uses vitess sqlparser for SQL parsing.
func Parse(sql string) (*ValidatedQuery, error) {
	return nil, errors.New("not implemented")
}
