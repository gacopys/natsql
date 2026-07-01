package query

import (
	"fmt"

	"github.com/gacopys/natsql/internal/kv"
)

// BuildPlan creates an execution plan from a validated query.
//
// Rules per D-28:
// - If any WHERE condition matches a PK column with OpEq → PKLookupPlan
// - For composite keys: all key fields must have equality conditions
// - Otherwise → FullScanPlan
//
// For PKLookupPlan with multiple matching PK-eq conditions on a composite key,
// the PkValue is constructed by joining values with the KeySeparator.
func BuildPlan(q *ValidatedQuery, schema *kv.ViewSchema) (Plan, error) {
	if schema == nil {
		return nil, fmt.Errorf("view %q not found", q.From)
	}

	// Check if we can do a PK lookup:
	// All PK fields must have an equality condition in the WHERE clause.
	pkConditions := findPKEqConditions(q.Where, schema.KeyFields)

	if len(pkConditions) == len(schema.KeyFields) && len(schema.KeyFields) > 0 {
		// All key fields have equality conditions → PKLookupPlan
		pkValues := make([]string, len(schema.KeyFields))
		for i, kf := range schema.KeyFields {
			pkValues[i] = fmt.Sprint(pkConditions[kf].Value)
		}
		separator := schema.KeySeparator
		if separator == "" {
			separator = "/" // must be a valid NATS KV key char; see kv.BuildPKKey
		}

		// Check for contradictory PK predicates (D-02)
		// Same PK column with multiple different OpEq values → empty result
		pkSeen := make(map[string]any)
		for _, cond := range q.Where {
			if cond.Op != OpEq {
				continue
			}
			if _, isPK := pkConditions[cond.Column]; isPK {
				if existing, ok := pkSeen[cond.Column]; ok {
					if !valuesEqual(existing, cond.Value) {
						// Contradictory PK conditions → empty result (D-02)
						return &EmptyPlan{Columns: q.Select}, nil
					}
				} else {
					pkSeen[cond.Column] = cond.Value
				}
			}
		}

		// Note: pkValues are raw (not sanitized) — BuildPKKey sanitizes once at KV boundary
		// CR-03: ALL conditions (PK + non-PK) are kept as post-filters (D-01/D-03)
		return &PKLookupPlan{
			ViewName:  q.From,
			PKParts:   pkValues,
			Separator: separator,
			Columns:   q.Select,
			Where:     q.Where, // ALL conditions (PK + non-PK) as post-filters (D-01/D-03)
		}, nil
	}

	// Fall back to full scan
	return &FullScanPlan{
		ViewName: q.From,
		Columns:  q.Select,
		Where:    q.Where,
		Limit:    q.Limit,
	}, nil
}

// findPKEqConditions finds equality conditions matching key fields.
// Returns a map of key field name → condition, containing only fields
// that have an OpEq condition. If a key field has no matching condition,
// it won't be in the map.
func findPKEqConditions(conditions []Condition, keyFields []string) map[string]Condition {
	keyFieldSet := make(map[string]bool, len(keyFields))
	for _, kf := range keyFields {
		keyFieldSet[kf] = true
	}

	result := make(map[string]Condition, len(keyFields))

	for _, cond := range conditions {
		if cond.Op != OpEq {
			continue
		}
		if keyFieldSet[cond.Column] {
			result[cond.Column] = cond
		}
	}

	return result
}
