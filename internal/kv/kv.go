// Package kv provides NATS JetStream Key-Value operations for natsql.
//
// This package handles schema storage, row read/write, and canonical
// primary-key encoding (BuildPKKey). All state is stored in a single
// JetStream KV bucket.
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
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// DefaultBucket is the single JetStream KV bucket used for all view state.
	DefaultBucket = "natsql-views" // D-06
)

// ViewSchema is the immutable schema for a materialized view, stored in KV.
type ViewSchema struct {
	Name         string         `json:"name"`
	Columns      []ColumnSchema `json:"columns"`
	KeyFields    []string       `json:"key_fields"`
	KeySeparator string         `json:"key_separator"`
	Version      int            `json:"version"` // increment on rebuilds
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

// PKKey returns the KV key for a row in the given view.
// Format: "{view_name}/pk/{pkValue}" per D-07 (adapted for NATS KV key restrictions).
//
// Deprecated: Use BuildPKKey instead. Kept for backward compatibility.
func PKKey(viewName, pkValue string) string {
	return BuildPKKey(viewName, []string{pkValue}, "")
}

// SanitizePK encodes a single primary-key value as a NATS-KV-safe key
// fragment. It is the encoding BuildPKKey applies to a one-component key.
func SanitizePK(s string) string {
	var b strings.Builder
	writePKPart(&b, s, "", true)
	return b.String()
}

// BuildPKKey constructs the full KV key for a row in the given view.
// This is the SINGLE canonical function for PK key construction (CR-02).
// Both materializer (write path) and query engine (read path) MUST use this.
//
// It takes raw PK component values, joins them with the separator (which is
// not escaped), and returns "{viewName}/pk/{encoded}". The result is always
// a valid NATS KV key, and distinct component lists of the same length (a
// view's key_fields count is fixed) always produce distinct keys (WR-03,
// WR-04):
//
//   - '_' '|' '/' '*' '>' keep their original escapes "__" "_p" "_s" "_a" "_g".
//   - Any other byte outside [A-Za-z0-9-=.] is written as "_x" plus two
//     lowercase hex digits, so '@', ' ', ':' (timestamps), '+' and UTF-8 work.
//   - A '.' that ends the key or precedes another '.' is written as "_x2e":
//     NATS rejects keys ending in '.' and drops writes with empty tokens.
//   - In a composite key, every character that occurs in the separator is
//     escaped as "_xHH" inside components, and if the separator contains '.',
//     an empty component is written as "_e". cfg validation rejects
//     separators containing '_' or "..", which no escaping can make safe.
//
// Single-component keys and default-separator keys that were valid before
// these rules existed encode exactly as they did.
func BuildPKKey(viewName string, pkParts []string, separator string) string {
	var b strings.Builder
	b.WriteString(viewName)
	b.WriteString("/pk/")

	composite := len(pkParts) > 1
	escapeChars := ""
	if composite {
		escapeChars = separator
	}
	for i, part := range pkParts {
		if i > 0 {
			b.WriteString(separator)
		}
		if composite && part == "" && strings.Contains(separator, ".") {
			b.WriteString("_e")
			continue
		}
		writePKPart(&b, part, escapeChars, i == len(pkParts)-1)
	}
	return b.String()
}

// writePKPart appends the encoding of one PK component to b. Bytes found in
// escapeChars are always hex-escaped; last marks the component that ends the
// key, where a trailing '.' must be escaped.
func writePKPart(b *strings.Builder, part, escapeChars string, last bool) {
	for i := range len(part) {
		c := part[i]
		switch {
		case c == '_':
			b.WriteString("__") // underscore escapes itself
		case c == '|':
			b.WriteString("_p")
		case c == '/':
			b.WriteString("_s")
		case c == '*':
			b.WriteString("_a")
		case c == '>':
			b.WriteString("_g")
		case strings.IndexByte(escapeChars, c) >= 0:
			writeHexEscape(b, c)
		case c == '.' && ((i == len(part)-1 && last) || (i+1 < len(part) && part[i+1] == '.')):
			writeHexEscape(b, c)
		case isPlainKeyByte(c):
			b.WriteByte(c)
		default:
			writeHexEscape(b, c)
		}
	}
}

// isPlainKeyByte reports whether c can appear unescaped in an encoded PK
// component: the NATS KV key alphabet minus '_' and '/', which are escaped.
func isPlainKeyByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
		c == '-' || c == '=' || c == '.'
}

func writeHexEscape(b *strings.Builder, c byte) {
	const hexDigits = "0123456789abcdef"
	b.WriteString("_x")
	b.WriteByte(hexDigits[c>>4])
	b.WriteByte(hexDigits[c&0x0f])
}

// SchemaKey returns the KV key for the schema of a view.
// Format: "{view_name}/meta/schema" per D-08 (adapted for NATS KV key restrictions).
func SchemaKey(viewName string) string {
	return viewName + "/meta/schema"
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
