package query

import (
	"fmt"
	"slices"

	"github.com/gacopys/natsql/v2/internal/kv"
)

// maxPKLookups caps how many point lookups an IN list may expand into; a
// larger candidate set is cheaper to answer with one scan.
const maxPKLookups = 256

// BuildPlan creates an execution plan from a validated query.
//
// Rules per D-28:
//   - LIMIT 0, contradictory PK equalities (D-02) and PK conditions no row
//     can satisfy (NULL) → EmptyPlan
//   - Every key field constrained by '=' or IN → PKLookupPlan with one point
//     lookup per candidate key, up to maxPKLookups (PF-01)
//   - Otherwise → FullScanPlan
//
// SELECT * is expanded to the schema's columns, so internal fields such as
// _meta, and fields of rows written under an older schema, never appear in
// results, whichever plan runs (QE-01).
func BuildPlan(q *ValidatedQuery, schema *kv.ViewSchema) (Plan, error) {
	if schema == nil {
		return nil, fmt.Errorf("view %q not found", q.From)
	}

	columns := resolveColumns(q.Select, schema)
	if q.HasLimit && q.Limit == 0 {
		return &EmptyPlan{Columns: columns}, nil // QE-03
	}
	if hasContradictoryPK(q.Where, schema.KeyFields) {
		return &EmptyPlan{Columns: columns}, nil
	}

	if keys, ok := pkCandidateKeys(q.Where, schema.KeyFields); ok {
		if len(keys) == 0 {
			return &EmptyPlan{Columns: columns}, nil
		}
		separator := schema.KeySeparator
		if separator == "" {
			separator = "/" // must be a valid NATS KV key char; see kv.BuildPKKey
		}
		// CR-03: ALL conditions (PK + non-PK) are kept as post-filters (D-01/D-03)
		return &PKLookupPlan{
			ViewName:  q.From,
			Keys:      keys,
			Separator: separator,
			Columns:   columns,
			Where:     q.Where,
			Limit:     q.Limit,
		}, nil
	}

	return &FullScanPlan{
		ViewName: q.From,
		Columns:  columns,
		Where:    q.Where,
		Limit:    q.Limit,
	}, nil
}

// resolveColumns expands '*' in a projection into the schema's columns, in
// schema order. A nil projection means SELECT *.
func resolveColumns(selected []string, schema *kv.ViewSchema) []string {
	all := make([]string, len(schema.Columns))
	for i, c := range schema.Columns {
		all[i] = c.Name
	}
	if selected == nil {
		return all
	}
	columns := make([]string, 0, len(selected))
	for _, col := range selected {
		if col == "*" {
			columns = append(columns, all...)
		} else {
			columns = append(columns, col)
		}
	}
	return columns
}

// hasContradictoryPK reports whether one key field is required to equal two
// different values (WHERE id = 'a' AND id = 'b'), which no row satisfies
// (D-02). It doesn't need every key field to be constrained (QE-10).
func hasContradictoryPK(conditions []Condition, keyFields []string) bool {
	seen := make(map[string]any, len(keyFields))
	for _, cond := range conditions {
		if cond.Op != OpEq || !slices.Contains(keyFields, cond.Column) {
			continue
		}
		if existing, ok := seen[cond.Column]; ok && !valuesEqual(existing, cond.Value) {
			return true
		}
		seen[cond.Column] = cond.Value
	}
	return false
}

// pkCandidateKeys returns every primary key a matching row can have, or
// ok=false when some key field isn't constrained by '=' or IN, or the
// candidates would exceed maxPKLookups. Each key field takes its values from
// its first '=' condition, or else its first IN list; the other conditions
// still filter the rows that are found. Values are canonicalized with
// kv.FormatPKValue exactly as the materializer does (WR-06); values that can
// never be a key (NULL) are dropped, so an empty result means no row can
// match.
func pkCandidateKeys(conditions []Condition, keyFields []string) ([][]string, bool) {
	if len(keyFields) == 0 {
		return nil, false
	}
	keys := [][]string{{}}
	for _, kf := range keyFields {
		values, ok := keyFieldValues(conditions, kf)
		if !ok || len(keys)*len(values) > maxPKLookups {
			return nil, false
		}
		next := make([][]string, 0, len(keys)*len(values))
		for _, prefix := range keys {
			for _, v := range values {
				next = append(next, append(slices.Clip(prefix), v))
			}
		}
		keys = next
	}
	return keys, true
}

// keyFieldValues returns the distinct canonical values the conditions allow
// for one key field.
func keyFieldValues(conditions []Condition, keyField string) ([]string, bool) {
	var raw []any
	found := false
	for _, cond := range conditions {
		if cond.Column != keyField || cond.Op != OpEq {
			continue
		}
		raw, found = []any{cond.Value}, true
		break
	}
	if !found {
		for _, cond := range conditions {
			if cond.Column != keyField || cond.Op != OpIn {
				continue
			}
			list, ok := cond.Value.([]any)
			if !ok {
				return nil, false
			}
			raw, found = list, true
			break
		}
	}
	if !found {
		return nil, false
	}

	values := make([]string, 0, len(raw))
	for _, v := range raw {
		part, err := kv.FormatPKValue(v)
		if err != nil || slices.Contains(values, part) {
			continue // NULL never equals a key; duplicates need one lookup
		}
		values = append(values, part)
	}
	return values, true
}
