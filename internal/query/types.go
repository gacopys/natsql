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
	Limit  int         // 0 = no limit
}

// Plan is the interface for query execution plans.
type Plan interface {
	Execute(ctx context.Context, kv jetstream.KeyValue) ([]map[string]any, error)
}

// PKLookupPlan is a direct primary key point lookup.
type PKLookupPlan struct {
	ViewName  string
	PKParts   []string    // raw PK component values (not sanitized, not joined)
	Separator string      // separator used to join PKParts
	Columns   []string    // nil = all
	Where     []Condition // non-PK conditions to apply as post-filter
}

// FullScanPlan iterates all keys for a view, applies filters client-side.
type FullScanPlan struct {
	ViewName string
	Columns  []string // nil = all
	Where    []Condition
	Limit    int
}

// EmptyPlan is returned when WHERE predicates are contradictory
// (e.g., WHERE id = 'a' AND id = 'b'). Execution returns an empty
// result set immediately with no KV I/O.
type EmptyPlan struct {
	Columns []string
}

// Execute returns an empty result set with no KV I/O. Used when
// contradictory predicates make the query impossible to satisfy.
//

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
