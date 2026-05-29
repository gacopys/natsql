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
	SchemaPrefix  = "schemas:"     // D-08: schema key prefix
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

// MustInitBucket is a convenience wrapper around InitBucket that panics on error.
func MustInitBucket(ctx context.Context, js jetstream.JetStream, replicas int) jetstream.KeyValue {
	kv, err := InitBucket(ctx, js, replicas)
	if err != nil {
		panic(fmt.Sprintf("failed to init bucket: %v", err))
	}
	return kv
}

// PkKey returns the KV key for a row in the given view.
// Format: "{view_name}/pk/{pkValue}" per D-07 (adapted for NATS KV key restrictions).
// NATS KV keys only support [a-zA-Z0-9_\-./=], so '/' is used instead of ':'.
// PK values are sanitized to prevent key injection from special characters.
func PkKey(viewName, pkValue string) string {
	return viewName + "/pk/" + SanitizePK(pkValue)
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

// EncodePKValue returns a string representation of a PK value for use in KV keys.
// Strings are returned as-is (must not contain ":").
// Numbers are formatted with fmt.Sprint.
// Booleans become "true" or "false".
// nil returns empty string.
func EncodePKValue(val any) string {
	switch v := val.(type) {
	case string:
		if strings.ContainsAny(v, "/:") {
			panic(fmt.Sprintf("EncodePKValue: string value %q contains '/' or ':' which are not allowed in KV keys", v))
		}
		return v
	case int:
		return fmt.Sprint(v)
	case int32:
		return fmt.Sprint(v)
	case int64:
		return fmt.Sprint(v)
	case float32:
		return fmt.Sprint(v)
	case float64:
		return fmt.Sprint(v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}
