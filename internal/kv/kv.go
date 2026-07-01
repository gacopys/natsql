// Package kv provides NATS JetStream Key-Value operations for natsql.
//
// This package handles schema storage, row read/write, and canonical
// primary-key encoding (BuildPkKey). All state is stored in a single
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
	DefaultBucket = "natsql-views" // D-06: single bucket for all views
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

// InitBucket creates or opens the natsql-views KV bucket.
func InitBucket(ctx context.Context, js jetstream.JetStream, replicas int) (jetstream.KeyValue, error) {
	cfg := jetstream.KeyValueConfig{
		Bucket:   DefaultBucket,
		Storage:  jetstream.FileStorage,
		Replicas: replicas,
	}
	return js.CreateOrUpdateKeyValue(ctx, cfg)
}

// PkKey returns the KV key for a row in the given view.
// Format: "{view_name}/pk/{pkValue}" per D-07 (adapted for NATS KV key restrictions).
// NATS KV keys only support [a-zA-Z0-9_\-./=], so '/' is used instead of ':'.
// PK values are sanitized to prevent key injection from special characters.
//
// Deprecated: Use BuildPkKey instead. Kept for backward compatibility.
func PkKey(viewName, pkValue string) string {
	return BuildPkKey(viewName, []string{pkValue}, "")
}

// sanitizeKVPK encodes characters not allowed in NATS KV keys.
// NATS KV keys only allow: [a-zA-Z0-9_\-./=]
// Uses underscore-prefixed codes that are themselves valid key chars.
func SanitizePK(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 8)
	for _, r := range s {
		switch r {
		case '_':
			b.WriteString("__") // underscore escapes itself
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

// BuildPkKey constructs the full KV key for a row in the given view.
// This is the SINGLE canonical function for PK key construction.
// Both materializer (write path) and query engine (read path) MUST use this.
//
// Takes raw PK component values (not sanitized), joins them with the separator,
// sanitizes PK part values (not the separator), and returns "{viewName}/pk/{sanitized}".
func BuildPkKey(viewName string, pkParts []string, separator string) string {
	// 1. Sanitize each PK part individually (preserves separator characters)
	sanitizedParts := make([]string, len(pkParts))
	for i, part := range pkParts {
		sanitizedParts[i] = SanitizePK(part)
	}

	// 2. Join sanitized parts with separator (separator is NOT sanitized)
	pk := strings.Join(sanitizedParts, separator)

	// 3. Build full key
	return viewName + "/pk/" + pk
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


