package materialize

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	natsql "natsql/cfg"
	"natsql/kv"
)

const maxNestingDepth = 8 // T-02-02: limit JSON path depth

// Sentinel errors for mapper.
var (
	// ErrMalformedEvent indicates an event cannot be processed
	// and should be acked + sent to DLQ. Never blocks the stream.
	ErrMalformedEvent = fmt.Errorf("malformed event")

	// ErrSkipAndAck indicates the event should be silently skipped.
	// No error logging needed.
	ErrSkipAndAck = fmt.Errorf("skip and ack")
)

// RowMutation represents the result of mapping one event to a row mutation.
type RowMutation struct {
	PK        string         // encoded primary key value
	RowData   map[string]any // column name → typed value
	StreamSeq uint64         // stream sequence from message metadata
	Timestamp time.Time      // event timestamp from message metadata
}

// Mapper converts JetStream events to RowMutations using config-driven
// JSON path extraction and type validation.
type Mapper struct {
	viewCfg *natsql.ViewConfig
	schema  *kv.ViewSchema
}

// NewMapper creates a new Mapper from a view configuration.
// Returns an error if the config is nil or has no columns.
func NewMapper(viewCfg *natsql.ViewConfig) (*Mapper, error) {
	if viewCfg == nil {
		return nil, fmt.Errorf("view config is nil")
	}
	if len(viewCfg.Columns) == 0 {
		return nil, fmt.Errorf("view config has no columns")
	}

	schema := viewCfg.BuildSchema()

	return &Mapper{
		viewCfg: viewCfg,
		schema:  schema,
	}, nil
}

// MapRow extracts and validates column values from a JetStream event,
// producing a RowMutation.
func (m *Mapper) MapRow(msg jetstream.Msg) (*RowMutation, error) {
	// 1. Parse JSON with number preservation (FIX-MAT-04)
	var data map[string]any
	decoder := json.NewDecoder(bytes.NewReader(msg.Data()))
	decoder.UseNumber()
	if err := decoder.Decode(&data); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %v", ErrMalformedEvent, err)
	}

	// 2. Extract column values
	rowData := make(map[string]any, len(m.viewCfg.Columns))
	for _, col := range m.viewCfg.Columns {
		val, err := extractNestedField(data, col.From)
		if err != nil {
			return nil, fmt.Errorf("%w: column %q: %v", ErrMalformedEvent, col.Name, err)
		}

		typedVal, err := validateType(val, col.Type, col.Name)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrMalformedEvent, err)
		}

		rowData[col.Name] = typedVal
	}

	// 3. Extract PK value
	pkParts := make([]string, len(m.schema.KeyFields))
	for i, keyField := range m.schema.KeyFields {
		// Look up the column config for this key field
		val, ok := rowData[keyField]
		if !ok {
			return nil, fmt.Errorf("%w: key field %q not found in extracted data", ErrMalformedEvent, keyField)
		}
		pkParts[i] = stringifyValue(val)
	}

	separator := m.schema.KeySeparator
	if separator == "" {
		separator = "|"
	}
	pk := strings.Join(pkParts, separator)

	// 4. Extract metadata
	meta, err := msg.Metadata()
	if err != nil {
		return nil, fmt.Errorf("%w: reading metadata: %v", ErrMalformedEvent, err)
	}

	return &RowMutation{
		PK:        pk,
		RowData:   rowData,
		StreamSeq: meta.Sequence.Stream,
		Timestamp: meta.Timestamp,
	}, nil
}

// extractNestedField extracts a value from a nested map using dot-notation path.
// Supports paths like "user.id" → data["user"]["id"].
// Limits nesting depth to 8 levels per T-02-02.
func extractNestedField(data map[string]any, path string) (any, error) {
	parts := strings.Split(path, ".")
	if len(parts) > maxNestingDepth {
		return nil, fmt.Errorf("path %q exceeds maximum nesting depth of %d", path, maxNestingDepth)
	}

	current := data
	for i, part := range parts {
		if i == len(parts)-1 {
			val, ok := current[part]
			if !ok {
				return nil, fmt.Errorf("field %q not found in event data", path)
			}
			return val, nil
		}

		next, ok := current[part]
		if !ok {
			return nil, fmt.Errorf("path %q not found at segment %q", path, part)
		}

		nextMap, ok := next.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("path %q: segment %q is not an object", path, part)
		}
		current = nextMap
	}

	// Should not reach here for non-empty paths
	return nil, fmt.Errorf("field %q not found in event data", path)
}

// validateType checks that a value matches the expected column type.
// Returns the validated/coerced value or an error.
func validateType(val any, colType natsql.ColumnType, colName string) (any, error) {
	switch colType {
	case natsql.ColumnTypeString:
		switch v := val.(type) {
		case string:
			return v, nil
		case json.Number:
			return v.String(), nil
		case float64:
			return fmt.Sprint(v), nil
		default:
			return nil, fmt.Errorf("column %q: expected string, got %T", colName, val)
		}

	case natsql.ColumnTypeNumber:
		switch v := val.(type) {
		case json.Number:
			// Validate it's a valid number, but keep as json.Number to
			// preserve exact representation through json.Marshal (FIX-MAT-04)
			if _, err := v.Float64(); err != nil {
				return nil, fmt.Errorf("column %q: invalid number %q: %w", colName, v, err)
			}
			return v, nil
		case float64:
			return v, nil
		default:
			return nil, fmt.Errorf("column %q: expected number, got %T", colName, val)
		}

	case natsql.ColumnTypeBoolean:
		switch v := val.(type) {
		case bool:
			return v, nil
		default:
			return nil, fmt.Errorf("column %q: expected boolean, got %T", colName, val)
		}

	case natsql.ColumnTypeTimestamp:
		switch v := val.(type) {
		case string:
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return nil, fmt.Errorf("column %q: invalid timestamp %q: %w", colName, v, err)
			}
			return t, nil
		default:
			return nil, fmt.Errorf("column %q: expected timestamp string, got %T", colName, val)
		}

	default:
		return nil, fmt.Errorf("column %q: unknown column type %q", colName, colType)
	}
}

// stringifyValue converts a typed value to its string representation
// for use in PK construction, with sanitization for KV key safety.
func stringifyValue(val any) string {
	var s string
	switch v := val.(type) {
	case string:
		s = v
	case json.Number:
		s = v.String() // exact representation, no float64 precision loss
	case float64:
		s = strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			s = "true"
		} else {
			s = "false"
		}
	case time.Time:
		s = v.Format(time.RFC3339Nano)
	default:
		s = fmt.Sprint(v)
	}
	return sanitizePK(s)
}

// sanitizePK encodes characters that have special meaning in NATS KV keys.
// NATS KV keys support [a-zA-Z0-9_\-./=].
// Characters like /, *, >, . need encoding to prevent key injection.
func sanitizePK(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 8)
	for _, r := range s {
		switch r {
		case '_':
			b.WriteString("__")
		case '|':
			b.WriteString("_p")
		case '/':
			b.WriteString("_s")
		case '*':
			b.WriteString("_a")
		case '>':
			b.WriteString("_g")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}
