package natsql

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nats-io/nats.go"
	"gopkg.in/yaml.v3"

	"natsql/kv"
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
	URL      string `yaml:"url,omitempty" json:"url,omitempty"`           // default "nats://localhost:4222"
	Embedded bool   `yaml:"embedded,omitempty" json:"embedded,omitempty"` // start embedded NATS
	StoreDir string `yaml:"store_dir,omitempty" json:"store_dir,omitempty"` // JetStream store dir
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
	BatchSize      int `yaml:"batch_size,omitempty" json:"batch_size,omitempty"`
	MaxDeliver     int `yaml:"max_deliver,omitempty" json:"max_deliver,omitempty"`
	AckWaitSeconds int `yaml:"ack_wait_seconds,omitempty" json:"ack_wait_seconds,omitempty"`
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
			errs = append(errs, fmt.Sprintf("%s: name is required", prefix))
		} else if viewNames[v.Name] {
			errs = append(errs, fmt.Sprintf("%s: duplicate view name %q", prefix, v.Name))
		}
		viewNames[v.Name] = true

		if v.SourceStream == "" {
			errs = append(errs, fmt.Sprintf("%s: source_stream is required", prefix))
		}

		if len(v.KeyFields) == 0 {
			errs = append(errs, fmt.Sprintf("%s: at least one key_field is required", prefix))
		}

		if len(v.Columns) == 0 {
			errs = append(errs, fmt.Sprintf("%s: at least one column is required", prefix))
		}

		hasPK := false
		for j, c := range v.Columns {
			colPrefix := fmt.Sprintf("%s.columns[%d]", prefix, j)

			if c.Name == "" {
				errs = append(errs, fmt.Sprintf("%s: column name is required", colPrefix))
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
			errs = append(errs, fmt.Sprintf("%s: at least one column must have primary_key=true", prefix))
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("validation errors:\n  - %s", strings.Join(errs, "\n  - "))
}

// BuildSchema derives an immutable ViewSchema from the ViewConfig.
func (vc *ViewConfig) BuildSchema() *kv.ViewSchema {
	sep := vc.KeySeparator
	if sep == "" {
		sep = "|"
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
