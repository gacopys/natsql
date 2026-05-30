package query

import (
	"fmt"

	"github.com/gacopys/natsql/kv"
)

// Validate checks a parsed query against a view schema.
// It verifies:
//   - All SELECT columns exist in the schema (if not SELECT *)
//   - All WHERE columns exist in the schema
//
// Error messages follow D-42/D-43 conventions.
func Validate(q *ValidatedQuery, schema *kv.ViewSchema) error {
	if schema == nil {
		return fmt.Errorf("view %q not found", q.From)
	}

	// Build column lookup map
	colMap := make(map[string]bool, len(schema.Columns))
	for _, c := range schema.Columns {
		colMap[c.Name] = true
	}

	// Check SELECT columns
	if q.Select != nil {
		for _, col := range q.Select {
			if col == "*" {
				continue
			}
			if !colMap[col] {
				return fmt.Errorf("column %q not found in view %q", col, q.From)
			}
		}
	}

	// Check WHERE columns
	for _, cond := range q.Where {
		if !colMap[cond.Column] {
			return fmt.Errorf("column %q not found in view %q", cond.Column, q.From)
		}
	}

	return nil
}
