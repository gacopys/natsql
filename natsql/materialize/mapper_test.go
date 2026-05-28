package materialize

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"natsql"
)

func TestNewMapper_NilConfig_ReturnsError(t *testing.T) {
	m, err := NewMapper(nil)
	if err == nil {
		t.Fatal("expected error for nil config, got nil")
	}
	if m != nil {
		t.Fatal("expected nil mapper for nil config")
	}
}

func TestNewMapper_EmptyColumns_ReturnsError(t *testing.T) {
	vc := &natsql.ViewConfig{
		Name:         "test",
		SourceStream: "TEST",
		KeyFields:    []string{"id"},
		Columns:      []natsql.ColumnConfig{},
	}
	m, err := NewMapper(vc)
	if err == nil {
		t.Fatal("expected error for empty columns, got nil")
	}
	if m != nil {
		t.Fatal("expected nil mapper for empty columns")
	}
}

func TestNewMapper_ValidConfig_Succeeds(t *testing.T) {
	vc := &natsql.ViewConfig{
		Name:         "users",
		SourceStream: "USERS",
		KeyFields:    []string{"user_id"},
		Columns: []natsql.ColumnConfig{
			{Name: "user_id", From: "user_id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "name", From: "name", Type: natsql.ColumnTypeString},
		},
	}
	m, err := NewMapper(vc)
	if err != nil {
		t.Fatalf("NewMapper failed: %v", err)
	}
	if m == nil {
		t.Fatal("NewMapper returned nil")
	}
}

// --- MapRow tests ---

func TestMapRow_SimpleTopLevelFields(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "users",
		SourceStream: "USERS",
		KeyFields:    []string{"user_id"},
		Columns: []natsql.ColumnConfig{
			{Name: "user_id", From: "user_id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "name", From: "name", Type: natsql.ColumnTypeString},
		},
	})

	msg := &testMsg{
		data: []byte(`{"user_id": "abc123", "name": "Alice"}`),
		meta: &jetstream.MsgMetadata{
			Timestamp: time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
		},
	}
	// Fill in Stream sequence
	msg.meta.Sequence.Stream = 42

	mut, err := m.MapRow(msg)
	if err != nil {
		t.Fatalf("MapRow failed: %v", err)
	}
	if mut == nil {
		t.Fatal("MapRow returned nil mutation")
	}
	if mut.PK != "abc123" {
		t.Errorf("PK = %q, want %q", mut.PK, "abc123")
	}
	if mut.RowData["user_id"] != "abc123" {
		t.Errorf("RowData[user_id] = %v, want %q", mut.RowData["user_id"], "abc123")
	}
	if mut.RowData["name"] != "Alice" {
		t.Errorf("RowData[name] = %v, want %q", mut.RowData["name"], "Alice")
	}
	if mut.StreamSeq != 42 {
		t.Errorf("StreamSeq = %d, want 42", mut.StreamSeq)
	}
	if !mut.Timestamp.Equal(time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("Timestamp = %v, want 2026-05-28T12:00:00Z", mut.Timestamp)
	}
}

func TestMapRow_NestedJSONPath(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "users",
		SourceStream: "USERS",
		KeyFields:    []string{"user_id"},
		Columns: []natsql.ColumnConfig{
			{Name: "user_id", From: "user.id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "name", From: "profile.name", Type: natsql.ColumnTypeString},
			{Name: "age", From: "profile.age", Type: natsql.ColumnTypeNumber},
		},
	})

	msg := &testMsg{
		data: []byte(`{"user": {"id": "abc123"}, "profile": {"name": "Alice", "age": 30}}`),
		meta: &jetstream.MsgMetadata{Timestamp: time.Now()},
	}
	msg.meta.Sequence.Stream = 1

	mut, err := m.MapRow(msg)
	if err != nil {
		t.Fatalf("MapRow failed: %v", err)
	}
	if mut == nil {
		t.Fatal("MapRow returned nil mutation")
	}
	if mut.PK != "abc123" {
		t.Errorf("PK = %q, want %q", mut.PK, "abc123")
	}
	if mut.RowData["user_id"] != "abc123" {
		t.Errorf("RowData[user_id] = %v, want %q", mut.RowData["user_id"], "abc123")
	}
	if mut.RowData["name"] != "Alice" {
		t.Errorf("RowData[name] = %v, want %q", mut.RowData["name"], "Alice")
	}
	if mut.RowData["age"] != float64(30) {
		t.Errorf("RowData[age] = %v (%T), want float64(30)", mut.RowData["age"], mut.RowData["age"])
	}
}

func TestMapRow_StringType_ConvertsNumber(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "items",
		SourceStream: "ITEMS",
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "code", From: "code", Type: natsql.ColumnTypeString},
		},
	})

	// JSON number for string-typed column: should convert via Sprint
	msg := &testMsg{
		data: []byte(`{"id": "item1", "code": 12345}`),
		meta: &jetstream.MsgMetadata{Timestamp: time.Now()},
	}
	msg.meta.Sequence.Stream = 1

	mut, err := m.MapRow(msg)
	if err != nil {
		t.Fatalf("MapRow failed: %v", err)
	}
	if mut.RowData["code"] != "12345" {
		t.Errorf("RowData[code] = %v (%T), want string \"12345\"", mut.RowData["code"], mut.RowData["code"])
	}
}

func TestMapRow_NumberType_ValidFloat(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "items",
		SourceStream: "ITEMS",
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "price", From: "price", Type: natsql.ColumnTypeNumber},
		},
	})

	msg := &testMsg{
		data: []byte(`{"id": "item1", "price": 29.99}`),
		meta: &jetstream.MsgMetadata{Timestamp: time.Now()},
	}
	msg.meta.Sequence.Stream = 1

	mut, err := m.MapRow(msg)
	if err != nil {
		t.Fatalf("MapRow failed: %v", err)
	}
	if mut.RowData["price"] != float64(29.99) {
		t.Errorf("RowData[price] = %v, want 29.99", mut.RowData["price"])
	}
}

func TestMapRow_NumberType_RejectsString(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "items",
		SourceStream: "ITEMS",
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "price", From: "price", Type: natsql.ColumnTypeNumber},
		},
	})

	msg := &testMsg{
		data: []byte(`{"id": "item1", "price": "twenty"}`),
		meta: &jetstream.MsgMetadata{Timestamp: time.Now()},
	}
	msg.meta.Sequence.Stream = 1

	_, err := m.MapRow(msg)
	if err == nil {
		t.Fatal("expected error for string→number type mismatch, got nil")
	}
	if !errors.Is(err, ErrMalformedEvent) {
		t.Errorf("expected ErrMalformedEvent, got %v", err)
	}
}

func TestMapRow_BooleanType_Valid(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "users",
		SourceStream: "USERS",
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "active", From: "active", Type: natsql.ColumnTypeBoolean},
		},
	})

	msg := &testMsg{
		data: []byte(`{"id": "u1", "active": true}`),
		meta: &jetstream.MsgMetadata{Timestamp: time.Now()},
	}
	msg.meta.Sequence.Stream = 1

	mut, err := m.MapRow(msg)
	if err != nil {
		t.Fatalf("MapRow failed: %v", err)
	}
	if mut.RowData["active"] != true {
		t.Errorf("RowData[active] = %v, want true", mut.RowData["active"])
	}

	// Test false too
	msg2 := &testMsg{
		data: []byte(`{"id": "u2", "active": false}`),
		meta: &jetstream.MsgMetadata{Timestamp: time.Now()},
	}
	msg2.meta.Sequence.Stream = 2

	mut2, err := m.MapRow(msg2)
	if err != nil {
		t.Fatalf("MapRow failed for false: %v", err)
	}
	if mut2.RowData["active"] != false {
		t.Errorf("RowData[active] = %v, want false", mut2.RowData["active"])
	}
}

func TestMapRow_BooleanType_RejectsString(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "users",
		SourceStream: "USERS",
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "active", From: "active", Type: natsql.ColumnTypeBoolean},
		},
	})

	msg := &testMsg{
		data: []byte(`{"id": "u1", "active": "true"}`),
		meta: &jetstream.MsgMetadata{Timestamp: time.Now()},
	}
	msg.meta.Sequence.Stream = 1

	_, err := m.MapRow(msg)
	if err == nil {
		t.Fatal("expected error for string→boolean type mismatch, got nil")
	}
	if !errors.Is(err, ErrMalformedEvent) {
		t.Errorf("expected ErrMalformedEvent, got %v", err)
	}
}

func TestMapRow_TimestampType_ValidRFC3339(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "events",
		SourceStream: "EVENTS",
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "ts", From: "ts", Type: natsql.ColumnTypeTimestamp},
		},
	})

	msg := &testMsg{
		data: []byte(`{"id": "e1", "ts": "2026-05-28T12:00:00Z"}`),
		meta: &jetstream.MsgMetadata{Timestamp: time.Now()},
	}
	msg.meta.Sequence.Stream = 1

	mut, err := m.MapRow(msg)
	if err != nil {
		t.Fatalf("MapRow failed: %v", err)
	}

	// Timestamp values are stored as time.Time in RowData
	ts, ok := mut.RowData["ts"].(time.Time)
	if !ok {
		t.Fatalf("RowData[ts] is %T, want time.Time", mut.RowData["ts"])
	}
	expected, _ := time.Parse(time.RFC3339, "2026-05-28T12:00:00Z")
	if !ts.Equal(expected) {
		t.Errorf("RowData[ts] = %v, want %v", ts, expected)
	}
}

func TestMapRow_MissingKeyField_ReturnsErrMalformedEvent(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "users",
		SourceStream: "USERS",
		KeyFields:    []string{"user_id"},
		Columns: []natsql.ColumnConfig{
			{Name: "user_id", From: "user_id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "name", From: "name", Type: natsql.ColumnTypeString},
		},
	})

	msg := &testMsg{
		data: []byte(`{"name": "Alice"}`), // missing user_id
		meta: &jetstream.MsgMetadata{Timestamp: time.Now()},
	}
	msg.meta.Sequence.Stream = 1

	_, err := m.MapRow(msg)
	if err == nil {
		t.Fatal("expected error for missing key field, got nil")
	}
	if !errors.Is(err, ErrMalformedEvent) {
		t.Errorf("expected ErrMalformedEvent, got %v", err)
	}
}

func TestMapRow_InvalidJSON_ReturnsErrMalformedEvent(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "users",
		SourceStream: "USERS",
		KeyFields:    []string{"user_id"},
		Columns: []natsql.ColumnConfig{
			{Name: "user_id", From: "user_id", Type: natsql.ColumnTypeString, PrimaryKey: true},
		},
	})

	msg := &testMsg{
		data: []byte(`{invalid json`),
		meta: &jetstream.MsgMetadata{Timestamp: time.Now()},
	}
	msg.meta.Sequence.Stream = 1

	_, err := m.MapRow(msg)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
	if !errors.Is(err, ErrMalformedEvent) {
		t.Errorf("expected ErrMalformedEvent, got %v", err)
	}
}

func TestMapRow_TypeMismatch_NumberColumnWithString(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "users",
		SourceStream: "USERS",
		KeyFields:    []string{"user_id"},
		Columns: []natsql.ColumnConfig{
			{Name: "user_id", From: "user_id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "age", From: "age", Type: natsql.ColumnTypeNumber},
		},
	})

	msg := &testMsg{
		data: []byte(`{"user_id": "u1", "age": "thirty"}`),
		meta: &jetstream.MsgMetadata{Timestamp: time.Now()},
	}
	msg.meta.Sequence.Stream = 1

	_, err := m.MapRow(msg)
	if err == nil {
		t.Fatal("expected error for type mismatch, got nil")
	}
	if !errors.Is(err, ErrMalformedEvent) {
		t.Errorf("expected ErrMalformedEvent, got %v", err)
	}
}

func TestMapRow_CompositeKey_JoinedBySeparator(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "events",
		SourceStream: "EVENTS",
		KeyFields:    []string{"tenant_id", "event_id"},
		KeySeparator: "::",
		Columns: []natsql.ColumnConfig{
			{Name: "tenant_id", From: "tenant", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "event_id", From: "eid", Type: natsql.ColumnTypeNumber, PrimaryKey: true},
			{Name: "data", From: "payload", Type: natsql.ColumnTypeString},
		},
	})

	msg := &testMsg{
		data: []byte(`{"tenant": "acme", "eid": 42, "payload": "hello"}`),
		meta: &jetstream.MsgMetadata{Timestamp: time.Now()},
	}
	msg.meta.Sequence.Stream = 1

	mut, err := m.MapRow(msg)
	if err != nil {
		t.Fatalf("MapRow failed: %v", err)
	}
	if mut.PK != "acme::42" {
		t.Errorf("PK = %q, want %q", mut.PK, "acme::42")
	}
}

func TestMapRow_MetadataPopulated(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "events",
		SourceStream: "EVENTS",
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
		},
	})

	ts := time.Date(2026, 5, 28, 15, 30, 0, 0, time.UTC)
	msg := &testMsg{
		data: []byte(`{"id": "e1"}`),
		meta: &jetstream.MsgMetadata{
			Timestamp: ts,
		},
	}
	msg.meta.Sequence.Stream = 99

	mut, err := m.MapRow(msg)
	if err != nil {
		t.Fatalf("MapRow failed: %v", err)
	}
	if mut.StreamSeq != 99 {
		t.Errorf("StreamSeq = %d, want 99", mut.StreamSeq)
	}
	if !mut.Timestamp.Equal(ts) {
		t.Errorf("Timestamp = %v, want %v", mut.Timestamp, ts)
	}
}

func TestMapRow_UnknownColumnPath_ReturnsErrMalformedEvent(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "users",
		SourceStream: "USERS",
		KeyFields:    []string{"user_id"},
		Columns: []natsql.ColumnConfig{
			{Name: "user_id", From: "user_id", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "missing_col", From: "nonexistent.path", Type: natsql.ColumnTypeString},
		},
	})

	msg := &testMsg{
		data: []byte(`{"user_id": "u1", "name": "Alice"}`),
		meta: &jetstream.MsgMetadata{Timestamp: time.Now()},
	}
	msg.meta.Sequence.Stream = 1

	_, err := m.MapRow(msg)
	if err == nil {
		t.Fatal("expected error for unresolvable column path, got nil")
	}
	if !errors.Is(err, ErrMalformedEvent) {
		t.Errorf("expected ErrMalformedEvent, got %v", err)
	}
}

func TestMapRow_DefaultKeySeparator_Pipe(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "events",
		SourceStream: "EVENTS",
		KeyFields:    []string{"tenant", "id"},
		// KeySeparator not set — should default to "|"
		Columns: []natsql.ColumnConfig{
			{Name: "tenant", From: "tenant", Type: natsql.ColumnTypeString, PrimaryKey: true},
			{Name: "id", From: "id", Type: natsql.ColumnTypeString, PrimaryKey: true},
		},
	})

	msg := &testMsg{
		data: []byte(`{"tenant": "acme", "id": "e1"}`),
		meta: &jetstream.MsgMetadata{Timestamp: time.Now()},
	}
	msg.meta.Sequence.Stream = 1

	mut, err := m.MapRow(msg)
	if err != nil {
		t.Fatalf("MapRow failed: %v", err)
	}
	if mut.PK != "acme|e1" {
		t.Errorf("PK = %q, want %q", mut.PK, "acme|e1")
	}
}

func TestMapRow_NestedPathDepthLimit(t *testing.T) {
	m := mustNewMapper(t, &natsql.ViewConfig{
		Name:         "deep",
		SourceStream: "DEEP",
		KeyFields:    []string{"id"},
		Columns: []natsql.ColumnConfig{
			{Name: "id", From: "a.b.c.d.e.f.g.h.i.j", Type: natsql.ColumnTypeString, PrimaryKey: true},
		},
	})

	// Build deeply nested JSON: a.b.c.d.e.f.g.h.i.j = "too_deep"
	raw := `{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":{"j":"val"}}}}}}}}}}`
	msg := &testMsg{
		data: []byte(raw),
		meta: &jetstream.MsgMetadata{Timestamp: time.Now()},
	}
	msg.meta.Sequence.Stream = 1

	_, err := m.MapRow(msg)
	if err == nil {
		t.Fatal("expected error for path exceeding depth limit, got nil")
	}
	if !errors.Is(err, ErrMalformedEvent) {
		t.Errorf("expected ErrMalformedEvent, got %v", err)
	}
}

// --- testMsg mock implements jetstream.Msg ---

type testMsg struct {
	data    []byte
	meta    *jetstream.MsgMetadata
	subject string
}

func (m *testMsg) Data() []byte                              { return m.data }
func (m *testMsg) Metadata() (*jetstream.MsgMetadata, error) { return m.meta, nil }
func (m *testMsg) Ack() error                                { return nil }
func (m *testMsg) DoubleAck(ctx context.Context) error        { return nil }
func (m *testMsg) Nak() error                                { return nil }
func (m *testMsg) NakWithDelay(delay time.Duration) error    { return nil }
func (m *testMsg) InProgress() error                         { return nil }
func (m *testMsg) Term() error                               { return nil }
func (m *testMsg) TermWithReason(reason string) error        { return nil }
func (m *testMsg) Msg() *nats.Msg                            { return nil }
func (m *testMsg) Headers() nats.Header                      { return nil }
func (m *testMsg) Subject() string                           { return m.subject }
func (m *testMsg) Reply() string                             { return "" }

// --- helper ---

func mustNewMapper(t *testing.T, vc *natsql.ViewConfig) *Mapper {
	t.Helper()
	m, err := NewMapper(vc)
	if err != nil {
		t.Fatalf("NewMapper failed: %v", err)
	}
	return m
}
