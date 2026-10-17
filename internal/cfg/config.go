// Package cfg provides the configuration types for natsql.
//
// This package is split from the root natsql package to break an import cycle
// between the root package (which imports engine) and engine (which imports config types).
// The root natsql package re-exports all types via type aliases for backward compatibility.
package cfg

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/nats-io/nats.go"
	"gopkg.in/yaml.v3"

	"github.com/gacopys/natsql/v2/internal/kv"
)

// ColumnType represents the type of a column in a materialized view.
type ColumnType string

const (
	// ColumnTypeString is a column containing UTF-8 string values.
	ColumnTypeString ColumnType = "string"
	// ColumnTypeNumber is a column containing JSON numeric values (int or float).
	ColumnTypeNumber ColumnType = "number"
	// ColumnTypeBoolean is a column containing true/false values.
	ColumnTypeBoolean ColumnType = "boolean"
	// ColumnTypeTimestamp is a column containing RFC 3339 timestamp strings.
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

// DefaultQuerySubject is the NATS subject the query endpoint listens on when
// NATSConfig.QuerySubject is empty (D-34).
const DefaultQuerySubject = "natsql.query"

// NATSConfig configures the NATS connection for the engine.
type NATSConfig struct {
	URL      string `yaml:"url,omitempty" json:"url,omitempty"`             // default "nats://localhost:4222"
	Embedded bool   `yaml:"embedded,omitempty" json:"embedded,omitempty"`   // start embedded NATS
	StoreDir string `yaml:"store_dir,omitempty" json:"store_dir,omitempty"` // JetStream store dir; empty = private temp dir removed on Close
	Port     int    `yaml:"port,omitempty" json:"port,omitempty"`           // embedded NATS port (0 = random)

	// QuerySubject is the request-reply subject for SQL queries (TR-01).
	// Engines sharing a subject form one queue group and split requests, so
	// give independent deployments on one account different subjects.
	QuerySubject string `yaml:"query_subject,omitempty" json:"query_subject,omitempty"` // default "natsql.query"
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
	if cfg.NATS.QuerySubject == "" {
		cfg.NATS.QuerySubject = DefaultQuerySubject
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
	MaxAckPending int `yaml:"max_ack_pending,omitempty" json:"max_ack_pending,omitempty"`

	// MaxDeliver is how many delivery attempts an event gets before a
	// transient failure sends it to the DLQ. 0 means the default (10) and
	// -1 means retry until it succeeds (WR-07).
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

	if !isValidSubject(cfg.NATS.QuerySubject) {
		errs = append(errs, fmt.Sprintf("nats.query_subject %q is not a valid NATS subject (no spaces, wildcards or empty tokens)", cfg.NATS.QuerySubject))
	}

	viewNames := make(map[string]bool)
	for i, v := range cfg.Views {
		prefix := fmt.Sprintf("views[%d]", i)
		errs = append(errs, validateView(&v, prefix, viewNames)...)
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

// validateViewName checks that a view has a valid, unique name (CF-01).
func validateViewName(name, prefix string, viewNames map[string]bool) []string {
	defer func() { viewNames[name] = true }()
	switch {
	case name == "":
		return []string{prefix + ": name is required"}
	case !ValidViewName(name):
		return []string{fmt.Sprintf("%s: view name %q is invalid (use letters, digits and '_', starting with a letter or '_')", prefix, name)}
	case viewNames[name]:
		return []string{fmt.Sprintf("%s: duplicate view name %q", prefix, name)}
	}
	return nil
}

func validateView(v *ViewConfig, prefix string, viewNames map[string]bool) []string {
	errs := validateViewName(v.Name, prefix, viewNames)

	if v.SourceStream == "" {
		errs = append(errs, prefix+": source_stream is required")
	}

	if len(v.KeyFields) == 0 {
		errs = append(errs, prefix+": at least one key_field is required")
	}

	if v.KeySeparator != "" && !isValidKeySeparator(v.KeySeparator) {
		errs = append(errs, fmt.Sprintf("%s: key_separator %q is not allowed (use letters, digits, and - / . =; '_' is reserved for PK escaping and \"..\" would form an empty NATS subject token)", prefix, v.KeySeparator))
	}

	if len(v.Columns) == 0 {
		errs = append(errs, prefix+": at least one column is required")
	}

	if v.Consumer.MaxDeliver < -1 {
		errs = append(errs, fmt.Sprintf("%s: consumer.max_deliver %d is invalid (use -1 for unlimited, 0 for the default, or a positive count)", prefix, v.Consumer.MaxDeliver))
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

	errs = append(errs, crossValidateKeyFields(v, prefix)...)

	return errs
}

func crossValidateKeyFields(v *ViewConfig, prefix string) []string {
	var errs []string

	colNames := make(map[string]bool)
	pkColNames := make(map[string]bool)

	for _, c := range v.Columns {
		if c.Name == "" {
			continue
		}
		if colNames[c.Name] {
			errs = append(errs, fmt.Sprintf("%s: duplicate column name %q", prefix, c.Name))
		}
		colNames[c.Name] = true
		if c.PrimaryKey {
			pkColNames[c.Name] = true
		}
	}

	for _, kf := range v.KeyFields {
		if !pkColNames[kf] {
			if colNames[kf] {
				errs = append(errs, fmt.Sprintf("%s: key_field %q references column %q which does not have primary_key=true", prefix, kf, kf))
			} else {
				errs = append(errs, fmt.Sprintf("%s: key_field %q does not reference any declared column", prefix, kf))
			}
		}
	}

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

	return errs
}

// viewNamePattern is the set of valid view names (CF-01). A view name is used
// as a SQL identifier, a NATS KV key token and part of the durable consumer
// name, so it may not contain '.', '/', '-', wildcards or whitespace; a '/'
// could even alias another view's keys.
var viewNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ValidViewName reports whether name is a valid view name (CF-01).
func ValidViewName(name string) bool {
	return viewNamePattern.MatchString(name)
}

// isValidSubject reports whether s is a literal NATS subject: non-empty,
// with no whitespace, no wildcards and no empty tokens.
func isValidSubject(s string) bool {
	if s == "" || strings.ContainsAny(s, " \t\r\n") {
		return false
	}
	for _, tok := range strings.Split(s, ".") {
		if tok == "" || tok == "*" || tok == ">" {
			return false
		}
	}
	return true
}

// isValidKeySeparator reports whether sep can join composite PK components
// without ambiguity (WR-04). Every character must be a valid NATS KV key
// character (^[-/_=.a-zA-Z0-9]+$) except '_', which kv.BuildPKKey uses as its
// escape character, so a '_' separator could not be told apart from an
// escape. The separator must also not contain "..", which forms an empty
// subject token that NATS drops.
func isValidKeySeparator(sep string) bool {
	if strings.Contains(sep, "..") {
		return false
	}
	for _, r := range sep {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '/' || r == '.' || r == '=':
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
		// (valid set: [-/_=.a-zA-Z0-9]). '/' is chosen because
		// kv.BuildPKKey always escapes it inside PK components (→ "_x2f")
		// and it doesn't add a subject token to the key.
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
		SourceStream: vc.SourceStream,
		Fingerprint:  vc.fingerprint(sep),
	}
}

// fingerprint hashes everything that decides which rows a view holds and
// under which keys: the source stream and filter, the column mappings and
// types, the key fields and separator, and the KV key layout. Two configs
// with the same fingerprint produce identical rows from the same events, so
// a stored view can be resumed; any other change needs a rebuild (WR-09).
// Consumer tuning is deliberately left out.
func (vc *ViewConfig) fingerprint(sep string) string {
	type column struct {
		Name       string `json:"name"`
		From       string `json:"from"`
		Type       string `json:"type"`
		PrimaryKey bool   `json:"primary_key"`
	}
	def := struct {
		KeyLayout     int      `json:"key_layout"`
		SourceStream  string   `json:"source_stream"`
		SourceSubject string   `json:"source_subject"`
		KeyFields     []string `json:"key_fields"`
		KeySeparator  string   `json:"key_separator"`
		Columns       []column `json:"columns"`
	}{
		KeyLayout:     kv.KeyLayoutVersion,
		SourceStream:  vc.SourceStream,
		SourceSubject: vc.SourceSubject,
		KeyFields:     vc.KeyFields,
		KeySeparator:  sep,
	}
	for _, c := range vc.Columns {
		def.Columns = append(def.Columns, column{
			Name:       c.Name,
			From:       strings.TrimPrefix(c.From, "$."), // "$.a" and "a" read the same field
			Type:       string(c.Type),
			PrimaryKey: c.PrimaryKey,
		})
	}
	data, _ := json.Marshal(def) // strings, ints and bools always marshal
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:16])
}
