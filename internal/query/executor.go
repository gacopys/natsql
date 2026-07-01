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
	key := kv.BuildPKKey(p.ViewName, p.PKParts, p.Separator)
	entry, err := kvb.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("PK lookup failed: %w", err)
	}

	var row map[string]any
	decoder := json.NewDecoder(bytes.NewReader(entry.Value()))
	decoder.UseNumber() // CR-09: preserve exact numeric precision
	if err := decoder.Decode(&row); err != nil {
		return nil, fmt.Errorf("unmarshaling row: %w", err)
	}

	// Apply non-PK WHERE conditions as post-filter (FIX-ENG-01)
	if len(p.Where) > 0 && !filterRow(row, p.Where) {
		return nil, nil
	}

	return []map[string]any{projectRow(row, p.Columns)}, nil
}

// Execute performs a full scan over all KV keys for a view, applying
// WHERE filters and LIMIT client-side. Keys outside the view prefix are skipped.
// CR-09: Uses json.Decoder.UseNumber to preserve >2^53 integer precision.
func (p *FullScanPlan) Execute(ctx context.Context, kvb jetstream.KeyValue) ([]map[string]any, error) {
	prefix := p.ViewName + "/pk/"

	watcher, err := kvb.WatchAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("watching keys: %w", err)
	}
	defer func() { _ = watcher.Stop() }() // best-effort stop; context may already be done

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
		go p.processEntry(entry.Value(), &mu, &results, sem, &wg, errCh)
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

func (p *FullScanPlan) processEntry(data []byte, mu *sync.Mutex, results *[]map[string]any, sem chan struct{}, wg *sync.WaitGroup, errCh chan error) {
	defer wg.Done()
	defer func() { <-sem }()

	mu.Lock()
	alreadyFull := p.Limit > 0 && len(*results) >= p.Limit
	mu.Unlock()
	if alreadyFull {
		return
	}

	var fullRow map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&fullRow); err != nil {
		select {
		case errCh <- fmt.Errorf("unmarshaling row: %w", err):
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
	defer mu.Unlock()
	if p.Limit > 0 && len(*results) >= p.Limit {
		return
	}
	*results = append(*results, row)
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

// Json.Number with no decimal point → int64; with decimal → float64.
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

	aVal, bVal := normalizeValue(a), normalizeValue(b)

	switch av := aVal.(type) {
	case float64:
		switch bv := bVal.(type) {
		case float64:
			return av == bv
		case int64:
			return av == float64(bv)
		}
	case int64:
		switch bv := bVal.(type) {
		case int64:
			return av == bv
		case float64:
			return float64(av) == bv
		}
	case bool:
		if bv, ok := bVal.(bool); ok {
			return av == bv
		}
	case string:
		if bv, ok := bVal.(string); ok {
			return av == bv
		}
	}

	return fmt.Sprintf("%T", aVal) == fmt.Sprintf("%T", bVal) && fmt.Sprint(aVal) == fmt.Sprint(bVal)
}

func normalizeValue(v any) any {
	n, ok := v.(json.Number)
	if !ok {
		return v
	}
	return convertJSONNumber(n)
}
