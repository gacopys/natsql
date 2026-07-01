// Package cfg provides the configuration types for natsql.
//
// This package is split from the root natsql package to break an import cycle
// between the root package (which imports engine) and engine (which imports config types).
// The root natsql package re-exports all types via type aliases for backward compatibility.
package cfg

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nats-io/nats.go"
	"gopkg.in/yaml.v3"

	"github.com/gacopys/natsql/internal/kv"
)

// ColumnType represents the type of a column in a materialized view.
type ColumnType string

const (
	ColumnTypeString    ColumnType = "string"
	ColumnTypeNumber    ColumnType = "number"
	ColumnTypeBoolean   ColumnType = "boolean"
	ColumnTypeTimestamp ColumnType = "timestamp"
)

// Valid returns true if the ColumnType is one of the known types.
func (ct ColumnType) Valid() bool {
	switch ct {
	case ColumnTypeString, ColumnTypeNumber, ColumnTypeBoolean, ColumnTypeTimestamp:
		return true
	default:
		return false
	}
}

// NATSConfig configures the NATS connection for the engine.
type NATSConfig struct {
	URL      string `yaml:"url,omitempty" json:"url,omitempty"`             // default "nats://localhost:4222"
	Embedded bool   `yaml:"embedded,omitempty" json:"embedded,omitempty"`   // start embedded NATS
	StoreDir string `yaml:"store_dir,omitempty" json:"store_dir,omitempty"` // JetStream store dir
	Port     int    `yaml:"port,omitempty" json:"port,omitempty"`           // embedded NATS port (0 = random)
}

// HTTPConfig configures the HTTP query API server.
type HTTPConfig struct {
	Port int `yaml:"port,omitempty" json:"port,omitempty"` // default 8080
}

// Config is the top-level configuration, loaded from YAML or JSON.
type Config struct {
	NATS  NATSConfig   `yaml:"nats,omitempty" json:"nats,omitempty"`
	HTTP  HTTPConfig   `yaml:"http,omitempty" json:"http,omitempty"`
	Views []ViewConfig `yaml:"views" json:"views"`
}

// SetDefaults fills zero-valued fields with sensible defaults.
func (cfg *Config) SetDefaults() {
	if cfg.NATS.URL == "" {
		cfg.NATS.URL = nats.DefaultURL
	}
	if cfg.HTTP.Port == 0 {
		cfg.HTTP.Port = 8080
	}

	// Migrate deprecated BatchSize to MaxAckPending
	for i := range cfg.Views {
		v := &cfg.Views[i]
		if v.Consumer.BatchSize > 0 && v.Consumer.MaxAckPending == 0 {
			v.Consumer.MaxAckPending = v.Consumer.BatchSize
		}
	}
}

// ViewConfig defines one materialized view.
type ViewConfig struct {
	Name          string         `yaml:"name" json:"name"`
	SourceStream  string         `yaml:"source_stream" json:"source_stream"`
	SourceSubject string         `yaml:"source_subject,omitempty" json:"source_subject,omitempty"`
	KeyFields     []string       `yaml:"key_fields" json:"key_fields"`
	KeySeparator  string         `yaml:"key_separator,omitempty" json:"key_separator,omitempty"`
	Columns       []ColumnConfig `yaml:"columns" json:"columns"`
	Indexes       []IndexConfig  `yaml:"indexes,omitempty" json:"indexes,omitempty"`
	Consumer      ConsumerConfig `yaml:"consumer,omitempty" json:"consumer,omitempty"`
}

// ColumnConfig defines one column in the view schema.
type ColumnConfig struct {
	Name       string     `yaml:"name" json:"name"`
	From       string     `yaml:"from" json:"from"`
	Type       ColumnType `yaml:"type" json:"type"`
	PrimaryKey bool       `yaml:"primary_key,omitempty" json:"primary_key,omitempty"`
}

// IndexConfig is a placeholder for Phase 2.
// Included now for forward compatibility (D-05), ignored by the materializer.
type IndexConfig struct {
	Column string `yaml:"column" json:"column"`
}

// ConsumerConfig configures the JetStream consumer for a view.
type ConsumerConfig struct {
	MaxAckPending  int `yaml:"max_ack_pending,omitempty" json:"max_ack_pending,omitempty"`
	MaxDeliver     int `yaml:"max_deliver,omitempty" json:"max_deliver,omitempty"`
	AckWaitSeconds int `yaml:"ack_wait_seconds,omitempty" json:"ack_wait_seconds,omitempty"`

	// Deprecated: Use MaxAckPending instead. Kept for backward compat.
	BatchSize int `yaml:"batch_size,omitempty" json:"batch_size,omitempty"`
}

// LoadConfig reads a YAML or JSON config file and returns the parsed Config.
// The file format is determined by the file extension (.yaml, .yml, or .json).
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	cfg := &Config{}
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parsing YAML config: %w", err)
		}
	case ".json":
		if err := json.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parsing JSON config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported config file extension: %s (supported: .yaml, .yml, .json)", ext)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate checks the entire config and returns all validation errors.
func (cfg *Config) Validate() error {
	var errs []string

	cfg.SetDefaults()

	viewNames := make(map[string]bool)
	for i, v := range cfg.Views {
		prefix := fmt.Sprintf("views[%d]", i)

		if v.Name == "" {
			errs = append(errs, prefix+": name is required")
		} else if viewNames[v.Name] {
			errs = append(errs, fmt.Sprintf("%s: duplicate view name %q", prefix, v.Name))
		}
		viewNames[v.Name] = true

		if v.SourceStream == "" {
			errs = append(errs, prefix+": source_stream is required")
		}

		if len(v.KeyFields) == 0 {
			errs = append(errs, prefix+": at least one key_field is required")
		}

		// Validate key_separator: every char must be a valid NATS KV key
		// character (valid set: [-/_=.a-zA-Z0-9]). An invalid separator
		// produces unstoreable composite keys at runtime ("nats: invalid key").
		// Empty is allowed (defaults to "/" in BuildSchema).
		if v.KeySeparator != "" && !isValidKeySeparator(v.KeySeparator) {
			errs = append(errs, fmt.Sprintf("%s: key_separator %q contains characters that are not valid in a NATS KV key (allowed: letters, digits, and - / _ . =)", prefix, v.KeySeparator))
		}

		if len(v.Columns) == 0 {
			errs = append(errs, prefix+": at least one column is required")
		}

		hasPK := false
		for j, c := range v.Columns {
			colPrefix := fmt.Sprintf("%s.columns[%d]", prefix, j)

			if c.Name == "" {
				errs = append(errs, colPrefix+": column name is required")
			}

			if c.From == "" {
				errs = append(errs, fmt.Sprintf("%s: column 'from' is required for column %q", colPrefix, c.Name))
			}

			if !c.Type.Valid() {
				errs = append(errs, fmt.Sprintf("%s: invalid column type %q (must be one of: string, number, boolean, timestamp)", colPrefix, c.Type))
			}

			if c.PrimaryKey {
				hasPK = true
			}
		}

		if !hasPK {
			errs = append(errs, prefix+": at least one column must have primary_key=true")
		}

		// CR-08 / FND-03: Cross-validate key_fields and primary_key columns
		colNames := make(map[string]bool)
		pkColNames := make(map[string]bool)

		for _, c := range v.Columns {
			if c.Name == "" {
				continue // already caught above
			}
			if colNames[c.Name] {
				errs = append(errs, fmt.Sprintf("%s: duplicate column name %q", prefix, c.Name))
			}
			colNames[c.Name] = true
			if c.PrimaryKey {
				pkColNames[c.Name] = true
			}
		}

		// Every key_field must reference a column that exists and has primary_key=true
		for _, kf := range v.KeyFields {
			if !pkColNames[kf] {
				if colNames[kf] {
					errs = append(errs, fmt.Sprintf("%s: key_field %q references column %q which does not have primary_key=true", prefix, kf, kf))
				} else {
					errs = append(errs, fmt.Sprintf("%s: key_field %q does not reference any declared column", prefix, kf))
				}
			}
		}

		// Every column with primary_key=true must be listed in key_fields
		for pkName := range pkColNames {
			found := false
			for _, kf := range v.KeyFields {
				if kf == pkName {
					found = true
					break
				}
			}
			if !found {
				errs = append(errs, fmt.Sprintf("%s: column %q has primary_key=true but is not listed in key_fields", prefix, pkName))
			}
		}
	}

	// CR-16 / CLN-02: Reject index configurations (not yet supported)
	for i, v := range cfg.Views {
		if len(v.Indexes) > 0 {
			errs = append(errs, fmt.Sprintf("views[%d]: secondary indexes are not yet supported — remove indexes block from config", i))
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("validation errors:\n  - %s", strings.Join(errs, "\n  - "))
}

// isValidKeySeparator reports whether every character in sep is a valid NATS KV
// key character. NATS KV keys must match ^[-/_=.a-zA-Z0-9]+$.
func isValidKeySeparator(sep string) bool {
	for _, r := range sep {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '/' || r == '_' || r == '.' || r == '=':
		default:
			return false
		}
	}
	return true
}

// BuildSchema derives an immutable ViewSchema from the ViewConfig.
func (vc *ViewConfig) BuildSchema() *kv.ViewSchema {
	sep := vc.KeySeparator
	if sep == "" {
		// Default separator must be a valid NATS KV key character
		// (valid set: [-/_=.a-zA-Z0-9]). '/' is chosen because it is
		// already escaped in PK part values by SanitizePK (→ "_s"),
		// so composite keys cannot collide with embedded slashes.
		sep = "/"
	}

	columns := make([]kv.ColumnSchema, len(vc.Columns))
	for i, c := range vc.Columns {
		columns[i] = kv.ColumnSchema{
			Name:       c.Name,
			Type:       string(c.Type),
			PrimaryKey: c.PrimaryKey,
		}
	}

	return &kv.ViewSchema{
		Name:         vc.Name,
		Columns:      columns,
		KeyFields:    vc.KeyFields,
		KeySeparator: sep,
		Version:      1,
	}
}
