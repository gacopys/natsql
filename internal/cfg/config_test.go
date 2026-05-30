package cfg

import (
	"os"
	"path/filepath"
	"testing"
)

func TestColumnTypeValid(t *testing.T) {
	tests := []struct {
		ct    ColumnType
		valid bool
	}{
		{ColumnTypeString, true},
		{ColumnTypeNumber, true},
		{ColumnTypeBoolean, true},
		{ColumnTypeTimestamp, true},
		{ColumnType("unknown"), false},
		{ColumnType(""), false},
	}
	for _, tc := range tests {
		if got := tc.ct.Valid(); got != tc.valid {
			t.Errorf("ColumnType(%q).Valid() = %v, want %v", tc.ct, got, tc.valid)
		}
	}
}

func TestSetDefaults(t *testing.T) {
	cfg := &Config{}
	cfg.SetDefaults()
	if cfg.NATS.URL == "" {
		t.Errorf("default NATS URL = %q, want nats://localhost:4222", cfg.NATS.URL)
	}
	if cfg.HTTP.Port != 8080 {
		t.Errorf("default HTTP port = %d, want 8080", cfg.HTTP.Port)
	}
}

func TestValidate_EmptyName(t *testing.T) {
	cfg := &Config{
		Views: []ViewConfig{
			{
				SourceStream: "stream",
				KeyFields:    []string{"k"},
				Columns: []ColumnConfig{
					{Name: "k", From: "k", Type: ColumnTypeString, PrimaryKey: true},
				},
			},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for empty view name")
	}
}

func TestValidate_DuplicateName(t *testing.T) {
	cfg := &Config{
		Views: []ViewConfig{
			{
				Name:         "dup",
				SourceStream: "s1",
				KeyFields:    []string{"k"},
				Columns:      []ColumnConfig{{Name: "k", From: "k", Type: ColumnTypeString, PrimaryKey: true}},
			},
			{
				Name:         "dup",
				SourceStream: "s2",
				KeyFields:    []string{"k"},
				Columns:      []ColumnConfig{{Name: "k", From: "k", Type: ColumnTypeString, PrimaryKey: true}},
			},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for duplicate view name")
	}
}

func TestValidate_MissingSourceStream(t *testing.T) {
	cfg := &Config{
		Views: []ViewConfig{
			{
				Name:      "v",
				KeyFields: []string{"k"},
				Columns:   []ColumnConfig{{Name: "k", From: "k", Type: ColumnTypeString, PrimaryKey: true}},
			},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing source_stream")
	}
}

func TestValidate_MissingKeyFields(t *testing.T) {
	cfg := &Config{
		Views: []ViewConfig{
			{
				Name:         "v",
				SourceStream: "s",
				Columns:      []ColumnConfig{{Name: "k", From: "k", Type: ColumnTypeString, PrimaryKey: true}},
			},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for no key_fields")
	}
}

func TestValidate_MissingColumns(t *testing.T) {
	cfg := &Config{
		Views: []ViewConfig{
			{Name: "v", SourceStream: "s", KeyFields: []string{"k"}},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for no columns")
	}
}

func TestValidate_ColumnMissingName(t *testing.T) {
	cfg := &Config{
		Views: []ViewConfig{
			{
				Name:         "v",
				SourceStream: "s",
				KeyFields:    []string{"k"},
				Columns:      []ColumnConfig{{From: "k", Type: ColumnTypeString, PrimaryKey: true}},
			},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for column with no name")
	}
}

func TestValidate_ColumnMissingFrom(t *testing.T) {
	cfg := &Config{
		Views: []ViewConfig{
			{
				Name:         "v",
				SourceStream: "s",
				KeyFields:    []string{"k"},
				Columns:      []ColumnConfig{{Name: "k", Type: ColumnTypeString, PrimaryKey: true}},
			},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for column with no from")
	}
}

func TestValidate_InvalidColumnType(t *testing.T) {
	cfg := &Config{
		Views: []ViewConfig{
			{
				Name:         "v",
				SourceStream: "s",
				KeyFields:    []string{"k"},
				Columns:      []ColumnConfig{{Name: "k", From: "k", Type: "bogus", PrimaryKey: true}},
			},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for invalid column type")
	}
}

func TestValidate_NoPrimaryKey(t *testing.T) {
	cfg := &Config{
		Views: []ViewConfig{
			{
				Name:         "v",
				SourceStream: "s",
				KeyFields:    []string{"k"},
				Columns:      []ColumnConfig{{Name: "k", From: "k", Type: ColumnTypeString}},
			},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for no primary_key column")
	}
}

func TestBuildSchema_DefaultSeparator(t *testing.T) {
	vc := &ViewConfig{
		Name:      "test",
		KeyFields: []string{"a", "b"},
		Columns: []ColumnConfig{
			{Name: "a", From: "a", Type: ColumnTypeString, PrimaryKey: true},
			{Name: "b", From: "b", Type: ColumnTypeString, PrimaryKey: true},
			{Name: "val", From: "val", Type: ColumnTypeNumber},
		},
	}
	s := vc.BuildSchema()
	if s.Name != "test" {
		t.Errorf("Name = %q, want %q", s.Name, "test")
	}
	if s.KeySeparator != "|" {
		t.Errorf("KeySeparator = %q, want %q", s.KeySeparator, "|")
	}
	if len(s.Columns) != 3 {
		t.Fatalf("got %d columns, want 3", len(s.Columns))
	}
	if s.Columns[2].Name != "val" {
		t.Errorf("column[2].Name = %q", s.Columns[2].Name)
	}
	if s.Columns[2].Type != "number" {
		t.Errorf("column[2].Type = %q", s.Columns[2].Type)
	}
}

func TestBuildSchema_CustomSeparator(t *testing.T) {
	vc := &ViewConfig{
		Name:         "test",
		KeyFields:    []string{"a"},
		KeySeparator: ":",
		Columns:      []ColumnConfig{{Name: "a", From: "a", Type: ColumnTypeString, PrimaryKey: true}},
	}
	s := vc.BuildSchema()
	if s.KeySeparator != ":" {
		t.Errorf("KeySeparator = %q, want %q", s.KeySeparator, ":")
	}
}

func TestLoadConfig_YAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.yaml")
	content := `
views:
  - name: users
    source_stream: stream
    key_fields: [id]
    columns:
      - {name: id, from: id, type: string, primary_key: true}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.Views) != 1 {
		t.Fatalf("got %d views", len(cfg.Views))
	}
}

func TestLoadConfig_JSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.json")
	content := `{"views":[{"name":"v","source_stream":"s","key_fields":["k"],"columns":[{"name":"k","from":"k","type":"string","primary_key":true}]}]}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig JSON: %v", err)
	}
	if len(cfg.Views) != 1 {
		t.Fatalf("got %d views", len(cfg.Views))
	}
}

func TestLoadConfig_UnsupportedExt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte("key=val"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for unsupported extension")
	}
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte(":::invalid"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoadConfig_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte("{bad json}"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestLoadConfig_ReadError(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestValidate_ValidMinimal(t *testing.T) {
	cfg := &Config{
		Views: []ViewConfig{
			{
				Name:         "v",
				SourceStream: "s",
				KeyFields:    []string{"k"},
				Columns:      []ColumnConfig{{Name: "k", From: "k", Type: ColumnTypeString, PrimaryKey: true}},
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
