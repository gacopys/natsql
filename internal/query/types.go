// Package query provides the SQL query engine for natsql.
// It parses SELECT statements, validates them against view schemas,
// builds execution plans, and executes them against NATS KV.
package query

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

// Op is a comparison operator in a WHERE condition.
type Op string

const (
	// OpEq is the equality operator ("=").
	OpEq Op = "="
	// OpNeq is the not-equal operator ("!=").
	OpNeq Op = "!="
	// OpIn is the set-membership operator ("in").
	OpIn Op = "in"
)

// Condition is a single WHERE clause predicate (AND-connected).
type Condition struct {
	Column string `json:"column"`
	Op     Op     `json:"op"`
	Value  any    `json:"value"`
}

// ValidatedQuery is a parsed and validated SELECT query ready for planning.
type ValidatedQuery struct {
	Select []string    // nil means "*"
	From   string      // view name
	Where  []Condition // AND-connected; non-nil for v1
	Limit  int         // row cap; 0 = no cap unless HasLimit is set
	// HasLimit reports an explicit LIMIT clause, so "LIMIT 0" (no rows) is
	// told apart from no LIMIT at all (QE-03).
	HasLimit bool
}

// Plan is the interface for query execution plans.
type Plan interface {
	Execute(ctx context.Context, kv jetstream.KeyValue) ([]map[string]any, error)
}

// PKLookupPlan reads rows by primary key: one point lookup per candidate
// key, which covers PK equality and PK IN lists (PF-01).
type PKLookupPlan struct {
	ViewName  string
	Keys      [][]string  // candidate keys; each is the raw, canonical PK components (kv.FormatPKValue)
	Separator string      // separator used to join each key's components
	Columns   []string    // nil = every public field
	Where     []Condition // all conditions, applied as a post-filter (CR-03)
	Limit     int         // 0 = no limit
}

// FullScanPlan reads every row of a view, filtered on the server by key
// prefix, and applies the WHERE conditions client-side.
type FullScanPlan struct {
	ViewName string
	Columns  []string // nil = every public field
	Where    []Condition
	Limit    int // 0 = no limit
}

// EmptyPlan is returned when WHERE predicates are contradictory
// (e.g., WHERE id = 'a' AND id = 'b'). Execution returns an empty
// result set immediately with no KV I/O.
type EmptyPlan struct {
	Columns []string
}

// Execute returns an empty result set with no KV I/O. Used when
// contradictory predicates make the query impossible to satisfy.
func (p *EmptyPlan) Execute(_ context.Context, _ jetstream.KeyValue) ([]map[string]any, error) {
	return []map[string]any{}, nil
}

// QueryResult is the JSON response envelope per D-29.
//
//nolint:revive // QueryResult is the established public name; renaming to Result would stutter with query.Result
type QueryResult struct {
	Results []map[string]any `json:"results"`
	Error   *string          `json:"error"`
}
