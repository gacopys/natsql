package query

import (
	"fmt"
	"strings"

	"natsql/kv"
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
			separator = "|"
		}
		pkValue := strings.Join(pkValues, separator)

		return &PKLookupPlan{
			ViewName: q.From,
			PkValue:  pkValue,
			Columns:  q.Select,
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
