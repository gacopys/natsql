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
		return nil, fmt.Errorf("unmarshaling row: %w", err)
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
// Uses string-based comparison for value equality/inequality.
func filterRow(row map[string]any, conditions []Condition) bool {
	for _, c := range conditions {
		val := row[c.Column]
		switch c.Op {
		case OpEq:
			if fmt.Sprint(val) != fmt.Sprint(c.Value) {
				return false
			}
		case OpNeq:
			if fmt.Sprint(val) == fmt.Sprint(c.Value) {
				return false
			}
		case OpIn:
			values, ok := c.Value.([]any)
			if !ok {
				return false
			}
			found := false
			for _, v := range values {
				if fmt.Sprint(val) == fmt.Sprint(v) {
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
