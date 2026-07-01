package query

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql/internal/kv"
)

const fullScanWorkers = 16

// Execute performs a direct PK lookup using kv.Get().
// Returns a single row or empty slice if not found.
// If the plan has non-PK WHERE conditions, they are applied as a post-filter.
func (p *PKLookupPlan) Execute(ctx context.Context, kvb jetstream.KeyValue) ([]map[string]any, error) {
	key := kv.BuildPkKey(p.ViewName, p.PkParts, p.Separator)
	entry, err := kvb.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("PK lookup failed: %w", err)
	}

	var row map[string]any
	decoder := json.NewDecoder(bytes.NewReader(entry.Value()))
	decoder.UseNumber()
	if err := decoder.Decode(&row); err != nil {
		return nil, fmt.Errorf("unmarshaling row: %w", err)
	}

	// Apply non-PK WHERE conditions as post-filter (FIX-ENG-01)
	if len(p.Where) > 0 && !filterRow(row, p.Where) {
		return nil, nil
	}

	return []map[string]any{projectRow(row, p.Columns)}, nil
}

// Execute performs a full scan using kvb.WatchAll() which streams all
// key-value pairs from the bucket in a single subscription (no per-key Gets),
// then processes them in parallel via a worker pool for unmarshal, filtering,
// and projection.
func (p *FullScanPlan) Execute(ctx context.Context, kvb jetstream.KeyValue) ([]map[string]any, error) {
	prefix := p.ViewName + "/pk/"

	// Note: WatchAll is used because KV Watch pattern matching uses NATS subject
	// semantics (tokenizing on '.'), but our keys use '/' separators. WatchAll +
	// client-side HasPrefix filter is the correct approach (D-11 fallback per A1).
	watcher, err := kvb.WatchAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("watching keys: %w", err)
	}
	defer func() { _ = watcher.Stop() }()

	var (
		mu      sync.Mutex
		results []map[string]any
		wg      sync.WaitGroup
		sem     = make(chan struct{}, fullScanWorkers)
		errCh   = make(chan error, 1)
	)

	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}
		if !strings.HasPrefix(entry.Key(), prefix) {
			continue
		}

		mu.Lock()
		limitReached := p.Limit > 0 && len(results) >= p.Limit
		mu.Unlock()
		if limitReached || ctx.Err() != nil {
			break
		}

		sem <- struct{}{}
		wg.Add(1)

		go func(data []byte) {
			defer wg.Done()
			defer func() { <-sem }()

			if p.Limit > 0 {
				mu.Lock()
				alreadyFull := len(results) >= p.Limit
				mu.Unlock()
				if alreadyFull {
					return
				}
			}

			var fullRow map[string]any
			fDecoder := json.NewDecoder(bytes.NewReader(data))
			fDecoder.UseNumber()
			if uerr := fDecoder.Decode(&fullRow); uerr != nil {
				select {
				case errCh <- fmt.Errorf("unmarshaling row: %w", uerr):
				default:
				}
				return
			}

			if !filterRow(fullRow, p.Where) {
				return
			}

			row := fullRow
			if p.Columns != nil {
				row = projectRow(fullRow, p.Columns)
			}

			mu.Lock()
			if p.Limit > 0 && len(results) >= p.Limit {
				mu.Unlock()
				return
			}
			results = append(results, row)
			mu.Unlock()
		}(entry.Value())
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	if p.Limit > 0 && len(results) > p.Limit {
		results = results[:p.Limit]
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
	if columns == nil { // SELECT * — exclude internal fields starting with _
		projected := make(map[string]any, len(row))
		for k, v := range row {
			if !strings.HasPrefix(k, "_") {
				projected[k] = v
			}
		}
		return projected
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

// convertJSONNumber converts a json.Number to int64 or float64
// for comparison with WHERE clause literal values.
// json.Number with no decimal point → int64; with decimal → float64.
func convertJSONNumber(n json.Number) any {
	s := n.String()
	if !strings.Contains(s, ".") {
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i
		}
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s // fallback
}

// valuesEqual compares two values with type awareness.
// Handles float64, int64, bool, string, json.Number, and nil types.
// Falls back to fmt.Sprint for unhandled types.
func valuesEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == b
	}

	// Normalize json.Number for comparison (D-09)
	an, aIsNum := a.(json.Number)
	bn, bIsNum := b.(json.Number)
	if aIsNum || bIsNum {
		aVal := a
		bVal := b
		if aIsNum {
			aVal = convertJSONNumber(an)
		}
		if bIsNum {
			bVal = convertJSONNumber(bn)
		}
		return valuesEqual(aVal, bVal)
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

	// For same unhandled types, compare string representations
	// For different types, return false to avoid false matches
	return fmt.Sprintf("%T", a) == fmt.Sprintf("%T", b) && fmt.Sprint(a) == fmt.Sprint(b)
}
