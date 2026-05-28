package natsql

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig_ValidYAML_SingleView(t *testing.T) {
	content := `
views:
  - name: users
    source_stream: USERS_stream
    key_fields:
      - user_id
    columns:
      - name: user_id
        from: $.user_id
        type: string
        primary_key: true
      - name: name
        from: $.name
        type: string
`
	path := writeTempFile(t, "config.yaml", content)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if cfg == nil {
		t.Fatal("LoadConfig returned nil config")
	}
	if len(cfg.Views) != 1 {
		t.Fatalf("expected 1 view, got %d", len(cfg.Views))
	}
	if cfg.Views[0].Name != "users" {
		t.Errorf("expected view name 'users', got %q", cfg.Views[0].Name)
	}
	if cfg.Views[0].SourceStream != "USERS_stream" {
		t.Errorf("expected source_stream 'USERS_stream', got %q", cfg.Views[0].SourceStream)
	}
}

func TestLoadConfig_MissingFile_ReturnsError(t *testing.T) {
	_, err := LoadConfig("/tmp/nonexistent_file_xyz.yaml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestLoadConfig_MultipleViews(t *testing.T) {
	content := `
views:
  - name: users
    source_stream: USERS_stream
    key_fields:
      - user_id
    columns:
      - name: user_id
        from: $.user_id
        type: string
        primary_key: true
  - name: orders
    source_stream: ORDERS_stream
    key_fields:
      - order_id
    columns:
      - name: order_id
        from: $.order_id
        type: string
        primary_key: true
      - name: total
        from: $.total
        type: number
`
	path := writeTempFile(t, "multi.yaml", content)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if len(cfg.Views) != 2 {
		t.Fatalf("expected 2 views, got %d", len(cfg.Views))
	}
	names := map[string]bool{}
	for _, v := range cfg.Views {
		names[v.Name] = true
	}
	if !names["users"] {
		t.Error("expected view 'users'")
	}
	if !names["orders"] {
		t.Error("expected view 'orders'")
	}
}

func TestLoadConfig_CompositeKeyFieldsWithCustomSeparator(t *testing.T) {
	content := `
views:
  - name: events
    source_stream: EVENTS_stream
    key_fields:
      - tenant_id
      - event_id
    key_separator: "::"
    columns:
      - name: tenant_id
        from: $.tenant_id
        type: string
        primary_key: true
      - name: event_id
        from: $.event_id
        type: string
        primary_key: true
`
	path := writeTempFile(t, "composite.yaml", content)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	v := cfg.Views[0]
	if v.KeySeparator != "::" {
		t.Errorf("expected key_separator '::', got %q", v.KeySeparator)
	}
	if len(v.KeyFields) != 2 {
		t.Fatalf("expected 2 key_fields, got %d", len(v.KeyFields))
	}
	if v.KeyFields[0] != "tenant_id" || v.KeyFields[1] != "event_id" {
		t.Errorf("unexpected key_fields: %v", v.KeyFields)
	}
}

func TestLoadConfig_InvalidColumnType_ReturnsError(t *testing.T) {
	content := `
views:
  - name: users
    source_stream: USERS_stream
    key_fields:
      - user_id
    columns:
      - name: user_id
        from: $.user_id
        type: invalid_type
        primary_key: true
`
	path := writeTempFile(t, "bad_type.yaml", content)
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for invalid column type, got nil")
	}
}

func TestLoadConfig_MissingSourceStream_ReturnsError(t *testing.T) {
	content := `
views:
  - name: users
    key_fields:
      - user_id
    columns:
      - name: user_id
        from: $.user_id
        type: string
        primary_key: true
`
	path := writeTempFile(t, "no_stream.yaml", content)
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for missing source_stream, got nil")
	}
}

func TestLoadConfig_JSONFormat(t *testing.T) {
	content := `{
	"views": [
		{
			"name": "users",
			"source_stream": "USERS_stream",
			"key_fields": ["user_id"],
			"columns": [
				{"name": "user_id", "from": "$.user_id", "type": "string", "primary_key": true}
			]
		}
	]
}`
	path := writeTempFile(t, "config.json", content)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if cfg == nil {
		t.Fatal("LoadConfig returned nil config")
	}
	if len(cfg.Views) != 1 {
		t.Fatalf("expected 1 view, got %d", len(cfg.Views))
	}
	if cfg.Views[0].Name != "users" {
		t.Errorf("expected view name 'users', got %q", cfg.Views[0].Name)
	}
}

func TestLoadConfig_IndexesField_ParsesButIgnored(t *testing.T) {
	content := `
views:
  - name: users
    source_stream: USERS_stream
    key_fields:
      - user_id
    columns:
      - name: user_id
        from: $.user_id
        type: string
        primary_key: true
    indexes:
      - column: name
      - column: age
`
	path := writeTempFile(t, "with_indexes.yaml", content)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if len(cfg.Views[0].Indexes) != 2 {
		t.Fatalf("expected 2 indexes, got %d", len(cfg.Views[0].Indexes))
	}
	if cfg.Views[0].Indexes[0].Column != "name" {
		t.Errorf("expected index column 'name', got %q", cfg.Views[0].Indexes[0].Column)
	}
}

func TestLoadConfig_EmptyKeyFields_ReturnsError(t *testing.T) {
	content := `
views:
  - name: users
    source_stream: USERS_stream
    key_fields: []
    columns:
      - name: user_id
        from: $.user_id
        type: string
        primary_key: true
`
	path := writeTempFile(t, "empty_keys.yaml", content)
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for empty key_fields, got nil")
	}
}

func TestLoadConfig_NoPrimaryKey_ReturnsError(t *testing.T) {
	content := `
views:
  - name: users
    source_stream: USERS_stream
    key_fields:
      - user_id
    columns:
      - name: user_id
        from: $.user_id
        type: string
        primary_key: false
`
	path := writeTempFile(t, "no_pk.yaml", content)
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for no primary key, got nil")
	}
}

func TestLoadConfig_DuplicateViewNames_ReturnsError(t *testing.T) {
	content := `
views:
  - name: users
    source_stream: USERS_stream
    key_fields:
      - user_id
    columns:
      - name: user_id
        from: $.user_id
        type: string
        primary_key: true
  - name: users
    source_stream: OTHER_stream
    key_fields:
      - id
    columns:
      - name: id
        from: $.id
        type: string
        primary_key: true
`
	path := writeTempFile(t, "dup.yaml", content)
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for duplicate view names, got nil")
	}
}

func TestValidate_ReturnsAllErrors(t *testing.T) {
	cfg := &Config{
		Views: []ViewConfig{
			{
				Name:         "",
				SourceStream: "",
				KeyFields:    []string{},
				Columns: []ColumnConfig{
					{Name: "", From: "", Type: ColumnType("bad_type"), PrimaryKey: false},
				},
			},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	// Should mention missing name, missing source_stream, empty key_fields, invalid type, no primary key
	for _, substr := range []string{"name", "source_stream", "key_field", "type", "primary_key"} {
		if !containsSubstr(err.Error(), substr) {
			t.Errorf("expected error to contain %q, got: %s", substr, err.Error())
		}
	}
}

func TestBuildSchema(t *testing.T) {
	vc := &ViewConfig{
		Name:         "users",
		SourceStream: "USERS_stream",
		KeyFields:    []string{"user_id"},
		KeySeparator: "|",
		Columns: []ColumnConfig{
			{Name: "user_id", From: "$.user_id", Type: ColumnTypeString, PrimaryKey: true},
			{Name: "name", From: "$.name", Type: ColumnTypeString, PrimaryKey: false},
		},
	}
	schema := vc.BuildSchema()
	if schema == nil {
		t.Fatal("BuildSchema returned nil")
	}
	if schema.Name != "users" {
		t.Errorf("expected schema name 'users', got %q", schema.Name)
	}
	if len(schema.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(schema.Columns))
	}
	if schema.Columns[0].Name != "user_id" || !schema.Columns[0].PrimaryKey {
		t.Error("expected first column to be user_id with PrimaryKey=true")
	}
	if schema.Columns[1].Name != "name" || schema.Columns[1].PrimaryKey {
		t.Error("expected second column to be name with PrimaryKey=false")
	}
	if schema.KeySeparator != "|" {
		t.Errorf("expected key_separator '|', got %q", schema.KeySeparator)
	}
	if schema.Version != 1 {
		t.Errorf("expected initial version 1, got %d", schema.Version)
	}
}

func TestLoadConfig_MissingName_ReturnsError(t *testing.T) {
	content := `
views:
  - source_stream: USERS_stream
    key_fields:
      - user_id
    columns:
      - name: user_id
        from: $.user_id
        type: string
        primary_key: true
`
	path := writeTempFile(t, "no_name.yaml", content)
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for missing name, got nil")
	}
}

func TestLoadConfig_NoColumns_ReturnsError(t *testing.T) {
	content := `
views:
  - name: users
    source_stream: USERS_stream
    key_fields:
      - user_id
    columns: []
`
	path := writeTempFile(t, "no_cols.yaml", content)
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for empty columns, got nil")
	}
}

func TestLoadConfig_EmptyColumnName_ReturnsError(t *testing.T) {
	content := `
views:
  - name: users
    source_stream: USERS_stream
    key_fields:
      - user_id
    columns:
      - name: ""
        from: $.user_id
        type: string
        primary_key: true
`
	path := writeTempFile(t, "empty_col.yaml", content)
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for empty column name, got nil")
	}
}

func TestColumnType_Valid(t *testing.T) {
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
	for _, tt := range tests {
		got := tt.ct.Valid()
		if got != tt.valid {
			t.Errorf("ColumnType(%q).Valid() = %v, want %v", tt.ct, got, tt.valid)
		}
	}
}

// Helpers

func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	return path
}

func containsSubstr(s, substr string) bool {
	return len(s) >= len(substr) && stringContains(s, substr)
}

func stringContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
