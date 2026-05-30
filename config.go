package natsql

import (
	"github.com/gacopys/natsql/internal/cfg"
)

// Config types re-exported from the cfg sub-package.
// This split breaks the import cycle between the root natsql package
// (which imports engine) and engine (which imports config types).
type (
	Config         = cfg.Config
	ViewConfig     = cfg.ViewConfig
	ColumnConfig   = cfg.ColumnConfig
	IndexConfig    = cfg.IndexConfig
	ConsumerConfig = cfg.ConsumerConfig
	ColumnType     = cfg.ColumnType
	NATSConfig     = cfg.NATSConfig
	HTTPConfig     = cfg.HTTPConfig
)

// Column type constants.
const (
	ColumnTypeString    = cfg.ColumnTypeString
	ColumnTypeNumber    = cfg.ColumnTypeNumber
	ColumnTypeBoolean   = cfg.ColumnTypeBoolean
	ColumnTypeTimestamp = cfg.ColumnTypeTimestamp
)

// LoadConfig reads a YAML or JSON config file and returns the parsed Config.
func LoadConfig(path string) (*Config, error) {
	return cfg.LoadConfig(path)
}
