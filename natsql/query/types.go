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
	OpEq  Op = "="
	OpNeq Op = "!="
	OpIn  Op = "in"
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
	ViewName string
	PkValue  string
	Columns  []string // nil = all
}

// Execute performs a direct PK lookup using kv.Get().
func (p *PKLookupPlan) Execute(ctx context.Context, kv jetstream.KeyValue) ([]map[string]any, error) {
	// Implemented in executor.go
	panic("stub — implemented in Task 2")
}

// FullScanPlan iterates all keys for a view, applies filters client-side.
type FullScanPlan struct {
	ViewName string
	Columns  []string // nil = all
	Where    []Condition
	Limit    int
}

// Execute performs a full scan using kv.ListKeys() with client-side filtering.
func (p *FullScanPlan) Execute(ctx context.Context, kv jetstream.KeyValue) ([]map[string]any, error) {
	// Implemented in executor.go
	panic("stub — implemented in Task 2")
}

// QueryResult is the JSON response envelope per D-29.
type QueryResult struct {
	Results []map[string]any `json:"results"`
	Error   *string         `json:"error"`
}
