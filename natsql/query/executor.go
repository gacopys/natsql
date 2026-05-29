package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

	"natsql/kv"
)

// Execute performs a direct PK lookup using kv.Get().
// Returns a single row or empty slice if not found.
// If the plan has non-PK WHERE conditions, they are applied as a post-filter.
func (p *PKLookupPlan) Execute(ctx context.Context, kvb jetstream.KeyValue) ([]map[string]any, error) {
	key := kv.PkKey(p.ViewName, p.PkValue)
	entry, err := kvb.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("PK lookup failed: %w", err)
	}

	var row map[string]any
	if err := json.Unmarshal(entry.Value(), &row); err != nil {
		return nil, fmt.Errorf("marshaling row: %w", err)
	}

	// Apply non-PK WHERE conditions as post-filter (FIX-ENG-01)
	if len(p.Where) > 0 && !filterRow(row, p.Where) {
		return nil, nil
	}

	return []map[string]any{projectRow(row, p.Columns)}, nil
}

// Execute performs a full scan using kv.ListKeys() with client-side filtering.
// The scan filters keys by the view's PK prefix, retrieves each value,
// applies WHERE conditions, and limits results.
func (p *FullScanPlan) Execute(ctx context.Context, kvb jetstream.KeyValue) ([]map[string]any, error) {
	keyLister, err := kvb.ListKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing keys: %w", err)
	}

	prefix := p.ViewName + "/pk/"
	var results []map[string]any

	for key := range keyLister.Keys() {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		entry, err := kvb.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return nil, fmt.Errorf("getting key %q: %w", key, err)
		}

		var fullRow map[string]any
		if err := json.Unmarshal(entry.Value(), &fullRow); err != nil {
			return nil, fmt.Errorf("unmarshaling row: %w", err)
		}

		// Apply WHERE filter on full row before projection
		if !filterRow(fullRow, p.Where) {
			continue
		}

		row := fullRow
		if p.Columns != nil {
			row = projectRow(fullRow, p.Columns)
		}

		results = append(results, row)

		// Apply LIMIT
		if p.Limit > 0 && len(results) >= p.Limit {
			break
		}
	}

	if results == nil {
		return []map[string]any{}, nil
	}
	return results, nil
}

// projectRow filters a row to only the requested columns.
// If columns is nil (SELECT *), the row is returned as-is.
// Missing columns in the projection are set to nil per D-31.
func projectRow(row map[string]any, columns []string) map[string]any {
	if columns == nil {
		return row // SELECT *
	}

	projected := make(map[string]any, len(columns))
	for _, col := range columns {
		if col == "*" {
			return row
		}
		val, ok := row[col]
		if ok {
			projected[col] = val
		} else {
			projected[col] = nil
		}
	}
	return projected
}

// filterRow checks whether a row matches all WHERE conditions (AND logic).
// Uses type-aware comparison (FIX-ENG-03).
func filterRow(row map[string]any, conditions []Condition) bool {
	for _, c := range conditions {
		val := row[c.Column]
		switch c.Op {
		case OpEq:
			if !valuesEqual(val, c.Value) {
				return false
			}
		case OpNeq:
			if valuesEqual(val, c.Value) {
				return false
			}
		case OpIn:
			values, ok := c.Value.([]any)
			if !ok {
				return false
			}
			found := false
			for _, v := range values {
				if valuesEqual(val, v) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}

// valuesEqual compares two values with type awareness.
// Handles float64, int64, bool, string, and nil types.
// Falls back to fmt.Sprint for unhandled types.
func valuesEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == b
	}

	// Normalize int64 → float64 for comparison (JSON numbers decode as float64)
	af, aIsFloat := a.(float64)
	bf, bIsFloat := b.(float64)
	ai, aIsInt := a.(int64)
	bi, bIsInt := b.(int64)

	if aIsFloat && bIsInt {
		return af == float64(bi)
	}
	if aIsInt && bIsFloat {
		return float64(ai) == bf
	}
	if aIsFloat && bIsFloat {
		return af == bf
	}
	if aIsInt && bIsInt {
		return ai == bi
	}

	// Boolean comparison
	ab, aIsBool := a.(bool)
	bb, bIsBool := b.(bool)
	if aIsBool && bIsBool {
		return ab == bb
	}

	// String comparison
	as, aIsStr := a.(string)
	bs, bIsStr := b.(string)
	if aIsStr && bIsStr {
		return as == bs
	}

	// Fall back to Sprint for unhandled types (conservative — won't false-match)
	// because type-mismatched pairs almost always have different Sprint output
	return fmt.Sprint(a) == fmt.Sprint(b)
}
