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

	"github.com/gacopys/natsql/v2/internal/kv"
)

const fullScanWorkers = 16

// errScanIncomplete reports a full scan that ended before it had read every
// row, so its results can't be told apart from "no more matches" (QE-02).
var errScanIncomplete = errors.New("scan interrupted before all rows were read")

// Execute reads each candidate key with kv.Get, in order, until Limit rows
// are found. Missing keys are skipped, and every WHERE condition is applied
// as a post-filter (CR-03).
func (p *PKLookupPlan) Execute(ctx context.Context, kvb jetstream.KeyValue) ([]map[string]any, error) {
	results := []map[string]any{}
	for _, parts := range p.Keys {
		if p.Limit > 0 && len(results) >= p.Limit {
			break
		}
		key := kv.BuildPKKey(p.ViewName, parts, p.Separator)
		entry, err := kvb.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("PK lookup failed: %w", err)
		}

		row, err := decodeRow(entry.Value())
		if err != nil {
			return nil, err
		}
		if filterRow(row, p.Where) { // FIX-ENG-01
			results = append(results, projectRow(row, p.Columns))
		}
	}
	return results, nil
}

// decodeRow decodes a stored row, keeping numbers as json.Number (CR-09:
// exact precision beyond 2^53).
func decodeRow(data []byte) (map[string]any, error) {
	var row map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&row); err != nil {
		return nil, fmt.Errorf("unmarshaling row: %w", err)
	}
	return row, nil
}

// Execute reads every row of the view and applies WHERE filters and LIMIT
// client-side. The server only sends this view's rows (PF-01) and skips
// delete markers.
//
// The scan succeeds only after it has seen the watcher's end-of-snapshot
// marker; if the context ends or the watcher closes first, it returns an
// error rather than partial results (QE-02). It stops at the first row that
// can't be decoded.
func (p *FullScanPlan) Execute(ctx context.Context, kvb jetstream.KeyValue) ([]map[string]any, error) {
	watcher, err := kvb.Watch(ctx, kv.PKFilter(p.ViewName), jetstream.IgnoreDeletes())
	if err != nil {
		return nil, fmt.Errorf("watching keys: %w", err)
	}
	defer stopWatcher(watcher)

	scan := &fullScan{plan: p, sem: make(chan struct{}, fullScanWorkers)}
	for {
		select {
		case <-ctx.Done():
			scan.wg.Wait()
			return nil, fmt.Errorf("%w: %w", errScanIncomplete, ctx.Err())
		case entry, ok := <-watcher.Updates():
			switch {
			case !ok:
				scan.wg.Wait()
				if ctx.Err() != nil {
					return nil, fmt.Errorf("%w: %w", errScanIncomplete, ctx.Err())
				}
				return nil, fmt.Errorf("%w: the watcher closed", errScanIncomplete)
			case entry == nil: // every row of the initial snapshot has been read
				return scan.result()
			case scan.done():
				return scan.result()
			}
			select {
			case scan.sem <- struct{}{}:
				scan.wg.Go(func() { scan.process(entry.Value()) })
			case <-ctx.Done():
				// This entry is skipped, so a later end-of-snapshot marker
				// must not be taken as completion.
				scan.wg.Wait()
				return nil, fmt.Errorf("%w: %w", errScanIncomplete, ctx.Err())
			}
		}
	}
}

// stopWatcher stops w and keeps draining its updates until nats.go closes
// the channel (QE-04). The watch callback sends into a 256-entry channel
// while holding the watcher's lock, so a scan that stops reading early would
// otherwise leave the callback goroutine blocked forever.
func stopWatcher(w jetstream.KeyWatcher) {
	_ = w.Stop() // the subscription may already be gone with its context
	go func() {
		for range w.Updates() {
		}
	}()
}

// fullScan collects the rows of one FullScanPlan execution, decoding and
// filtering entries on up to fullScanWorkers goroutines.
type fullScan struct {
	plan *FullScanPlan
	sem  chan struct{}
	wg   sync.WaitGroup

	mu      sync.Mutex
	results []map[string]any
	err     error
}

// done reports whether the scan can stop reading: LIMIT rows were found or
// a row failed to decode.
func (s *fullScan) done() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err != nil || s.plan.Limit > 0 && len(s.results) >= s.plan.Limit
}

func (s *fullScan) process(data []byte) {
	defer func() { <-s.sem }()
	if s.done() {
		return
	}

	row, err := decodeRow(data)
	if err == nil && !filterRow(row, s.plan.Where) {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err != nil:
		if s.err == nil {
			s.err = err
		}
	case s.plan.Limit == 0 || len(s.results) < s.plan.Limit:
		s.results = append(s.results, projectRow(row, s.plan.Columns))
	}
}

// result waits for in-flight rows and returns the rows found, or the first
// decode error.
func (s *fullScan) result() ([]map[string]any, error) {
	s.wg.Wait()
	if s.err != nil {
		return nil, s.err
	}
	if s.results == nil {
		return []map[string]any{}, nil
	}
	return s.results, nil
}

// projectRow returns the requested columns of a row, with nil for missing
// ones (D-31). The planner expands SELECT * into the schema's columns; a nil
// list (or a '*' entry), which only plans built by hand contain, selects the
// row's public fields, i.e. those not starting with '_'.
func projectRow(row map[string]any, columns []string) map[string]any {
	if columns == nil {
		columns = []string{"*"}
	}
	projected := make(map[string]any, len(columns))
	for _, col := range columns {
		if col != "*" {
			projected[col] = row[col]
			continue
		}
		for k, v := range row {
			if !strings.HasPrefix(k, "_") {
				projected[k] = v
			}
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
