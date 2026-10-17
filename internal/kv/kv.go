// Package kv provides NATS JetStream Key-Value operations for natsql.
//
// This package handles schema storage, row read/write, and canonical
// primary-key encoding (BuildPKKey, FormatPKValue). All state is stored in a
// single JetStream KV bucket.
//
// Key layout (KeyLayoutVersion 2, PF-01):
//
//	{view}.pk.{encodedPK}  → row JSON
//	{view}.meta.schema     → ViewSchema JSON
//
// Keys are '.'-delimited so the server can filter one view's rows with the
// wildcard "{view}.pk.>" instead of streaming the whole bucket. Layout 1
// ("{view}/pk/{pk}", "{view}/meta/schema") is no longer read; views stored
// in it must be rebuilt (see PurgeView).
//
// Known limitations:
//   - Tombstone/delete semantics are not yet supported (planned for v2).
//     Rows cannot be removed from the materialized view once written.
//     A delete mode (operation field, subject convention, or tombstone
//     predicate) will be designed and implemented in a future release.
package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// DefaultBucket is the single JetStream KV bucket used for all view state.
	DefaultBucket = "natsql-views" // D-06

	// KeyLayoutVersion identifies the key layout and PK encoding. It is part
	// of every view fingerprint, so a layout change makes existing views
	// require a rebuild instead of silently mixing two layouts (WR-09).
	KeyLayoutVersion = 2
)

// maxCanonicalNumberLen bounds the length of a canonical numeric PK, so a
// literal like 1e1000000 can't expand into a megabyte-long key.
const maxCanonicalNumberLen = 256

// ErrInvalidPKValue reports a value that cannot be part of a primary key:
// NULL, a non-finite or out-of-range number, or an unsupported type.
var ErrInvalidPKValue = errors.New("invalid primary key value")

// ViewSchema is the immutable schema for a materialized view, stored in KV.
type ViewSchema struct {
	Name         string         `json:"name"`
	Columns      []ColumnSchema `json:"columns"`
	KeyFields    []string       `json:"key_fields"`
	KeySeparator string         `json:"key_separator"`
	Version      int            `json:"version"` // increment on rebuilds

	// SourceStream is the stream the view materializes. It lets PurgeView's
	// callers find the view's durable consumer even after the view is
	// removed from config.
	SourceStream string `json:"source_stream,omitempty"`

	// Fingerprint identifies the materialization definition (columns,
	// mappings, keys, separator, source and key layout). A stored row set is
	// only valid for the definition that produced it (WR-09).
	Fingerprint string `json:"fingerprint,omitempty"`
}

// ColumnSchema describes a single column in the view schema.
type ColumnSchema struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	PrimaryKey bool   `json:"primary_key"`
}

// InitBucket opens the natsql-views KV bucket, creating it only if it does
// not exist. An existing bucket is never reconfigured: its history, TTL, size
// limits and replica count belong to whoever provisioned it, and re-applying
// defaults would silently discard retained revisions (WR-05). The replicas
// argument only applies when the bucket is created.
func InitBucket(ctx context.Context, js jetstream.JetStream, replicas int) (jetstream.KeyValue, error) {
	kvb, err := OpenBucket(ctx, js)
	if err == nil {
		return kvb, nil
	}
	if !errors.Is(err, jetstream.ErrBucketNotFound) {
		return nil, fmt.Errorf("opening KV bucket: %w", err)
	}

	kvb, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   DefaultBucket,
		Storage:  jetstream.FileStorage,
		Replicas: replicas,
	})
	if errors.Is(err, jetstream.ErrBucketExists) {
		// Another engine created it between our lookup and create.
		kvb, err = OpenBucket(ctx, js)
	}
	if err != nil {
		return nil, fmt.Errorf("creating KV bucket: %w", err)
	}
	return kvb, nil
}

// OpenBucket opens the existing natsql-views KV bucket without creating or
// modifying it, so the read path needs no stream management permissions.
// It returns an error matching jetstream.ErrBucketNotFound if the bucket
// does not exist yet.
func OpenBucket(ctx context.Context, js jetstream.JetStream) (jetstream.KeyValue, error) {
	return js.KeyValue(ctx, DefaultBucket)
}

// PKFilter returns the KV watch filter that matches every row of a view and
// nothing else, so scans are filtered on the server (PF-01).
func PKFilter(viewName string) string {
	return viewName + ".pk.>"
}

// BuildPKKey constructs the full KV key for a row in the given view.
// This is the SINGLE canonical function for PK key construction (CR-02).
// Both materializer (write path) and query engine (read path) MUST use this,
// on component values produced by FormatPKValue.
//
// It takes raw PK component values, joins them with the separator (which is
// not escaped), and returns "{viewName}.pk.{encoded}". The result is always
// a valid NATS KV key with no empty subject tokens, and distinct component
// lists of the same length (a view's key_fields count is fixed) always
// produce distinct keys (WR-03, WR-04):
//
//   - '_' is written as "__".
//   - An empty component is written as "_e".
//   - Any byte outside [A-Za-z0-9-=], including '.', is written as "_x" plus
//     two lowercase hex digits, so '@', ' ', ':' (timestamps), '+' and UTF-8
//     work and components never add subject tokens of their own.
//   - In a composite key, every character that occurs in the separator is
//     also escaped as "_xHH" inside components. cfg validation rejects
//     separators containing '_' or "..", which no escaping can make safe.
func BuildPKKey(viewName string, pkParts []string, separator string) string {
	var b strings.Builder
	b.WriteString(viewName)
	b.WriteString(".pk.")

	escapeChars := ""
	if len(pkParts) > 1 {
		escapeChars = separator
	}
	for i, part := range pkParts {
		if i > 0 {
			b.WriteString(separator)
		}
		writePKPart(&b, part, escapeChars)
	}
	return b.String()
}

// writePKPart appends the encoding of one PK component to b. Bytes found in
// escapeChars are always hex-escaped.
func writePKPart(b *strings.Builder, part, escapeChars string) {
	if part == "" {
		b.WriteString("_e")
		return
	}
	for i := range len(part) {
		c := part[i]
		switch {
		case c == '_':
			b.WriteString("__") // underscore escapes itself
		case strings.IndexByte(escapeChars, c) < 0 && isPlainKeyByte(c):
			b.WriteByte(c)
		default:
			writeHexEscape(b, c)
		}
	}
}

// isPlainKeyByte reports whether c can appear unescaped in an encoded PK
// component: the NATS KV key alphabet minus '_' (the escape character), '/'
// and '.' (which would add subject tokens).
func isPlainKeyByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
		c == '-' || c == '='
}

func writeHexEscape(b *strings.Builder, c byte) {
	const hexDigits = "0123456789abcdef"
	b.WriteString("_x")
	b.WriteByte(hexDigits[c>>4])
	b.WriteByte(hexDigits[c&0x0f])
}

// FormatPKValue returns the canonical string form of one primary-key value.
// The mapper (write path) and the planner (read path) both call it, so a PK
// written from an event and a PK named in a WHERE clause produce the same
// key whenever they are equal values (WR-06):
//
//   - Numbers (json.Number, integers, float64) are written as exact plain
//     decimals with no exponent, leading zeros or trailing fractional zeros,
//     so 1, 1.0, 1e0 and 10e-1 are all "1", and -0 is "0".
//   - Strings are used as-is, booleans are "true"/"false", and timestamps
//     use RFC 3339 with nanoseconds.
//
// NULL, NaN, infinities and unsupported types return ErrInvalidPKValue.
func FormatPKValue(v any) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case json.Number:
		return CanonicalNumber(val.String())
	case int64:
		return strconv.FormatInt(val, 10), nil
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return "", fmt.Errorf("%w: %v", ErrInvalidPKValue, val)
		}
		return CanonicalNumber(strconv.FormatFloat(val, 'g', -1, 64))
	case bool:
		return strconv.FormatBool(val), nil
	case time.Time:
		return val.Format(time.RFC3339Nano), nil
	case nil:
		return "", fmt.Errorf("%w: NULL", ErrInvalidPKValue)
	default:
		return "", fmt.Errorf("%w: unsupported type %T", ErrInvalidPKValue, v)
	}
}

// CanonicalNumber rewrites a decimal number literal ("-1.50e3", "0.000001",
// "1e21") as an exact plain decimal ("-1500", "0.000001",
// "1000000000000000000000"). It never goes through float64, so it is exact
// for any number of digits.
func CanonicalNumber(s string) (string, error) {
	neg, digits, exp, err := parseDecimal(s)
	if err != nil {
		return "", err
	}

	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return "0", nil // every zero, including -0, has one form
	}
	trimmed := strings.TrimRight(digits, "0")
	exp += len(digits) - len(trimmed)
	digits = trimmed

	// value = digits × 10^exp
	intLen := len(digits) + exp // digits before the decimal point
	if intLen > maxCanonicalNumberLen || len(digits)-intLen > maxCanonicalNumberLen {
		return "", fmt.Errorf("%w: number %q is too large or too small for a key", ErrInvalidPKValue, s)
	}

	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	switch {
	case exp >= 0:
		b.WriteString(digits)
		b.WriteString(strings.Repeat("0", exp))
	case intLen > 0:
		b.WriteString(digits[:intLen])
		b.WriteByte('.')
		b.WriteString(digits[intLen:])
	default:
		b.WriteString("0.")
		b.WriteString(strings.Repeat("0", -intLen))
		b.WriteString(digits)
	}
	return b.String(), nil
}

// parseDecimal splits a number literal into its sign, its significant digits
// without a decimal point, and the power of ten they are scaled by.
func parseDecimal(s string) (neg bool, digits string, exp int, err error) {
	invalid := func() (bool, string, int, error) {
		return false, "", 0, fmt.Errorf("%w: %q is not a decimal number", ErrInvalidPKValue, s)
	}

	rest := s
	if rest != "" && (rest[0] == '-' || rest[0] == '+') {
		neg = rest[0] == '-'
		rest = rest[1:]
	}

	mantissa := rest
	if i := strings.IndexAny(rest, "eE"); i >= 0 {
		mantissa = rest[:i]
		e, convErr := strconv.Atoi(rest[i+1:])
		if convErr != nil || e > math.MaxInt32 || e < math.MinInt32 {
			return invalid()
		}
		exp = e
	}

	intPart, fracPart, _ := strings.Cut(mantissa, ".")
	if intPart == "" && fracPart == "" || !allDigits(intPart) || !allDigits(fracPart) {
		return invalid()
	}
	return neg, intPart + fracPart, exp - len(fracPart), nil
}

func allDigits(s string) bool {
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// SchemaKey returns the KV key for the schema of a view.
// Format: "{view_name}.meta.schema" per D-08.
func SchemaKey(viewName string) string {
	return viewName + ".meta.schema"
}

// StoreSchema marshals a ViewSchema and stores it in KV at the schema key.
func StoreSchema(ctx context.Context, kv jetstream.KeyValue, viewName string, schema *ViewSchema) error {
	data, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("marshaling schema: %w", err)
	}
	_, err = kv.Put(ctx, SchemaKey(viewName), data)
	if err != nil {
		return fmt.Errorf("storing schema: %w", err)
	}
	return nil
}

// LoadSchema reads a ViewSchema from KV. If the key does not exist,
// returns (nil, nil).
func LoadSchema(ctx context.Context, kv jetstream.KeyValue, viewName string) (*ViewSchema, error) {
	entry, err := kv.Get(ctx, SchemaKey(viewName))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("loading schema: %w", err)
	}
	var schema ViewSchema
	if err := json.Unmarshal(entry.Value(), &schema); err != nil {
		return nil, fmt.Errorf("unmarshaling schema: %w", err)
	}
	return &schema, nil
}

// HasRows reports whether any row of the view is stored in the current key
// layout. It asks the bucket's stream for the last message matching
// "{view}.pk.>", so it costs one request however large the view is.
func HasRows(ctx context.Context, js jetstream.JetStream, viewName string) (bool, error) {
	stream, err := js.Stream(ctx, "KV_"+DefaultBucket)
	if err != nil {
		return false, fmt.Errorf("opening KV stream: %w", err)
	}
	_, err = stream.GetLastMsgForSubject(ctx, "$KV."+DefaultBucket+"."+PKFilter(viewName))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, jetstream.ErrMsgNotFound):
		return false, nil
	default:
		return false, fmt.Errorf("looking for rows of view %q: %w", viewName, err)
	}
}

// PurgeView removes every key a view owns from the bucket, in both the
// current layout ("{view}.>") and layout 1 ("{view}/..."), including its
// schema, so the view can be rebuilt from its source stream (WR-09). It
// purges the bucket's underlying stream, so no delete markers are left
// behind, and returns how many layout-1 keys it removed.
//
// The caller must make sure nothing materializes the view meanwhile.
func PurgeView(ctx context.Context, js jetstream.JetStream, kvb jetstream.KeyValue, viewName string) (int, error) {
	stream, err := js.Stream(ctx, "KV_"+DefaultBucket)
	if err != nil {
		return 0, fmt.Errorf("opening KV stream: %w", err)
	}
	subjectPrefix := "$KV." + DefaultBucket + "."

	if err := stream.Purge(ctx, jetstream.WithPurgeSubject(subjectPrefix+viewName+".>")); err != nil {
		return 0, fmt.Errorf("purging view %q: %w", viewName, err)
	}

	// Layout-1 keys are single subject tokens, so a wildcard can't select
	// them; find them by listing every key.
	lister, err := kvb.ListKeys(ctx)
	if err != nil {
		return 0, fmt.Errorf("listing keys: %w", err)
	}
	var legacy []string
	for key := range lister.Keys() {
		if strings.HasPrefix(key, viewName+"/") {
			legacy = append(legacy, key)
		}
	}
	_ = lister.Stop() // the lister stops itself once Keys is drained; a second Stop only reports that
	for _, key := range legacy {
		if err := stream.Purge(ctx, jetstream.WithPurgeSubject(subjectPrefix+key)); err != nil {
			return 0, fmt.Errorf("purging key %q: %w", key, err)
		}
	}
	return len(legacy), nil
}
