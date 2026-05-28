package query

import (
	"natsql/kv"
)

// BuildPlan creates an execution plan from a validated query.
// This is a stub for the RED phase.
func BuildPlan(q *ValidatedQuery, schema *kv.ViewSchema) (Plan, error) {
	return &PKLookupPlan{
		ViewName: q.From,
		Columns:  q.Select,
	}, nil
}
