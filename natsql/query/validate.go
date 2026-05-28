package query

import (
	"natsql/kv"
)

// Validate checks a parsed query against a view schema.
// This is a stub for the RED phase.
func Validate(q *ValidatedQuery, schema *kv.ViewSchema) error {
	return nil
}
