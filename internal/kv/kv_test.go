package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql/v2/internal/testutil"
)

func TestSchemaKey(t *testing.T) {
	result := SchemaKey("users")
	expected := "users.meta.schema"
	if result != expected {
		t.Errorf("SchemaKey(\"users\") = %q, want %q", result, expected)
	}
}

func TestInitBucket_CreatesBucket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, js := startEmbeddedNATS(t)

	kv, err := InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}
	if kv == nil {
		t.Fatal("InitBucket returned nil")
	}

	// Verify the bucket exists by reading a non-existent key
	_, err = kv.Get(ctx, "test-key")
	if err != nil {
		// Expected: key not found, but bucket exists
		if !isNATSKeyNotFound(err) {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestStoreAndLoadSchema_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, js := startEmbeddedNATS(t)

	kv, err := InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	schema := &ViewSchema{
		Name:         "users",
		KeyFields:    []string{"user_id"},
		KeySeparator: "|",
		Version:      1,
		Columns: []ColumnSchema{
			{Name: "user_id", Type: "string", PrimaryKey: true},
			{Name: "name", Type: "string", PrimaryKey: false},
		},
	}

	if err := StoreSchema(ctx, kv, "users", schema); err != nil {
		t.Fatalf("StoreSchema failed: %v", err)
	}

	loaded, err := LoadSchema(ctx, kv, "users")
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("LoadSchema returned nil")
	}

	if loaded.Name != schema.Name {
		t.Errorf("Name: got %q, want %q", loaded.Name, schema.Name)
	}
	if loaded.Version != schema.Version {
		t.Errorf("Version: got %d, want %d", loaded.Version, schema.Version)
	}
	if len(loaded.Columns) != len(schema.Columns) {
		t.Fatalf("Columns: got %d, want %d", len(loaded.Columns), len(schema.Columns))
	}
	if loaded.Columns[0].Name != "user_id" || !loaded.Columns[0].PrimaryKey {
		t.Error("column 0 mismatch")
	}
	if loaded.Columns[1].Name != "name" || loaded.Columns[1].PrimaryKey {
		t.Error("column 1 mismatch")
	}
}

func TestLoadSchema_MissingKey_ReturnsNil(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, js := startEmbeddedNATS(t)

	kv, err := InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	schema, err := LoadSchema(ctx, kv, "nonexistent_view")
	if err != nil {
		t.Fatalf("LoadSchema for missing key returned error: %v", err)
	}
	if schema != nil {
		t.Fatal("LoadSchema for missing key should return nil, got non-nil")
	}
}

func TestStoreSchema_OverwriteUpdatesSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, js := startEmbeddedNATS(t)

	kv, err := InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	v1 := &ViewSchema{Name: "users", Version: 1}
	if err := StoreSchema(ctx, kv, "users", v1); err != nil {
		t.Fatalf("StoreSchema v1 failed: %v", err)
	}

	v2 := &ViewSchema{Name: "users", Version: 2}
	if err := StoreSchema(ctx, kv, "users", v2); err != nil {
		t.Fatalf("StoreSchema v2 failed: %v", err)
	}

	loaded, err := LoadSchema(ctx, kv, "users")
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}
	if loaded.Version != 2 {
		t.Errorf("expected version 2, got %d", loaded.Version)
	}
}

func TestBuildPKKey(t *testing.T) {
	tests := []struct {
		name      string
		viewName  string
		pkParts   []string
		separator string
		want      string
	}{
		{"single part", "users", []string{"abc"}, "/", "users.pk.abc"},
		{"composite", "users", []string{"a", "b"}, "/", "users.pk.a/b"},
		{"underscore", "users", []string{"a_b"}, "/", "users.pk.a__b"},
		{"pipe", "users", []string{"a|b"}, "/", "users.pk.a_x7cb"},
		{"slash", "users", []string{"a/b"}, "/", "users.pk.a_x2fb"},
		{"star", "users", []string{"a*b"}, "/", "users.pk.a_x2ab"},
		{"greater", "users", []string{"a>b"}, "/", "users.pk.a_x3eb"},
		{"double underscore", "users", []string{"a__b"}, "/", "users.pk.a____b"},
		{"custom sep dash", "users", []string{"hello", "world"}, "-", "users.pk.hello-world"},
		{"empty part", "users", []string{""}, "/", "users.pk._e"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildPKKey(tt.viewName, tt.pkParts, tt.separator)
			if got != tt.want {
				t.Errorf("BuildPKKey(%q, %v, %q) = %q, want %q", tt.viewName, tt.pkParts, tt.separator, got, tt.want)
			}
		})
	}
}

// Helpers

func startEmbeddedNATS(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	return testutil.StartEmbeddedNATS(t)
}

func isNATSKeyNotFound(err error) bool {
	return errors.Is(err, jetstream.ErrKeyNotFound)
}

// TestBuildPKKey_EscapesInvalidKeyCharacters covers PK values that are not
// valid NATS KV key characters (WR-03).
func TestBuildPKKey_EscapesInvalidKeyCharacters(t *testing.T) {
	tests := []struct {
		value string
		want  string
	}{
		{"alice@example.com", "users.pk.alice_x40example_x2ecom"},
		{"a b", "users.pk.a_x20b"},
		{"2026-10-17T12:00:00Z", "users.pk.2026-10-17T12_x3a00_x3a00Z"},
		{"zażółć", "users.pk.za_xc5_xbc_xc3_xb3_xc5_x82_xc4_x87"},
		{"1e+21", "users.pk.1e_x2b21"},
		{"a.", "users.pk.a_x2e"},
		{"a..b", "users.pk.a_x2e_x2eb"},
		{".a", "users.pk._x2ea"},
		{"user-001", "users.pk.user-001"},
		{"k=v", "users.pk.k=v"},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			if got := BuildPKKey("users", []string{tt.value}, "/"); got != tt.want {
				t.Errorf("BuildPKKey(%q) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

// TestBuildPKKey_CompositeSeparatorEscaping covers composite keys whose
// components contain the separator (WR-04).
func TestBuildPKKey_CompositeSeparatorEscaping(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
		sep   string
		want  string
	}{
		{"dash in first part", []string{"globex-sales", "widget"}, "-", "t.pk.globex_x2dsales-widget"},
		{"dash in second part", []string{"globex", "sales-widget"}, "-", "t.pk.globex-sales_x2dwidget"},
		{"dot separator", []string{"a.b", "c"}, ".", "t.pk.a_x2eb.c"},
		{"dot separator, empty last", []string{"a", ""}, ".", "t.pk.a._e"},
		{"dot separator, empty first", []string{"", "b"}, ".", "t.pk._e.b"},
		{"letter separator", []string{"axb", "c"}, "x", "t.pk.a_x78bxc"},
		{"empty parts with slash", []string{"", ""}, "/", "t.pk._e/_e"},
		{"single part ignores separator", []string{"a-b"}, "-", "t.pk.a-b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BuildPKKey("t", tt.parts, tt.sep); got != tt.want {
				t.Errorf("BuildPKKey(%q, %q) = %q, want %q", tt.parts, tt.sep, got, tt.want)
			}
		})
	}
}

// validKVKey mirrors the nats.go key check plus the server's refusal of
// empty subject tokens.
var validKVKey = regexp.MustCompile(`^[-/_=.a-zA-Z0-9]+$`)

func checkValidKey(t *testing.T, key string, parts []string) {
	t.Helper()
	if !validKVKey.MatchString(key) || slices.Contains(strings.Split(key, "."), "") {
		t.Fatalf("parts %q encode to invalid KV key %q", parts, key)
	}
	if !strings.HasPrefix(key, "v.pk.") {
		t.Fatalf("parts %q encode to %q, outside the view's key space", parts, key)
	}
}

// TestBuildPKKey_ValidAndInjective checks, exhaustively over a small
// adversarial alphabet, that every two-component key is a valid NATS KV key
// and that no two component pairs share a key, for every kind of separator
// that validation accepts (WR-03, WR-04). The alphabet always includes the
// escape character, '.', a byte that needs hex escaping, and the separator's
// own characters.
func TestBuildPKKey_ValidAndInjective(t *testing.T) {
	for _, sep := range []string{"/", "-", ".", "=", "--", "-.-", "x", "e", "2"} {
		t.Run(sep, func(t *testing.T) {
			alphabet := []string{"a", "_", ".", ":"}
			for _, c := range sep {
				if !slices.Contains(alphabet, string(c)) {
					alphabet = append(alphabet, string(c))
				}
			}
			values := componentValues(alphabet, 3)

			seen := make(map[string][2]string, len(values)*len(values))
			for _, a := range values {
				for _, b := range values {
					parts := []string{a, b}
					key := BuildPKKey("v", parts, sep)
					checkValidKey(t, key, parts)
					if prev, ok := seen[key]; ok {
						t.Fatalf("collision: %q and %q both encode to %q", prev, parts, key)
					}
					seen[key] = [2]string{a, b}
				}
			}
		})
	}
}

// componentValues returns every string of up to maxLen symbols drawn from
// alphabet, including the empty string.
func componentValues(alphabet []string, maxLen int) []string {
	values := []string{""}
	level := []string{""}
	for range maxLen {
		var next []string
		for _, v := range level {
			for _, c := range alphabet {
				next = append(next, v+c)
			}
		}
		values = append(values, next...)
		level = next
	}
	return values
}

// TestBuildPKKey_RoundTripThroughNATS writes and reads keys built from
// awkward values against a real server, which is the final authority on
// what a valid key is (e.g. it silently drops keys with empty tokens).
func TestBuildPKKey_RoundTripThroughNATS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, js := startEmbeddedNATS(t)
	kvb, err := InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	cases := []struct {
		parts []string
		sep   string
	}{
		{[]string{"alice@example.com"}, "/"},
		{[]string{"a b"}, "/"},
		{[]string{"zażółć"}, "/"},
		{[]string{"2026-10-17T12:00:00+02:00"}, "/"},
		{[]string{"a."}, "/"},
		{[]string{"a..b"}, "/"},
		{[]string{"."}, "/"},
		{[]string{""}, "/"},
		{[]string{"a", ""}, "."},
		{[]string{"", ""}, "."},
		{[]string{"x.", ".y"}, "-.-"},
		{[]string{"globex-sales", "widget"}, "-"},
	}
	for _, tc := range cases {
		key := BuildPKKey("view", tc.parts, tc.sep)
		if _, err := kvb.Put(ctx, key, []byte(`{}`)); err != nil {
			t.Errorf("Put(%q) for parts %q failed: %v", key, tc.parts, err)
			continue
		}
		if _, err := kvb.Get(ctx, key); err != nil {
			t.Errorf("Get(%q) for parts %q failed: %v", key, tc.parts, err)
		}
	}
}

// TestInitBucket_PreservesExistingConfig verifies that opening an existing
// bucket never reconfigures it or discards retained history (WR-05).
func TestInitBucket_PreservesExistingConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, js := startEmbeddedNATS(t)

	existing, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   DefaultBucket,
		History:  5,
		TTL:      time.Hour,
		MaxBytes: 1 << 20,
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		t.Fatalf("CreateKeyValue failed: %v", err)
	}
	for i := range 3 {
		if _, err := existing.Put(ctx, "view/pk/a", []byte{byte('0' + i)}); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	kvb, err := InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	status, err := kvb.Status(ctx)
	if err != nil {
		t.Fatalf("Status failed: %v", err)
	}
	if status.History() != 5 || status.TTL() != time.Hour {
		t.Errorf("bucket reconfigured: history=%d ttl=%v, want history=5 ttl=1h", status.History(), status.TTL())
	}
	history, err := kvb.History(ctx, "view/pk/a")
	if err != nil {
		t.Fatalf("History failed: %v", err)
	}
	if len(history) != 3 {
		t.Errorf("retained revisions = %d, want 3", len(history))
	}
}

// TestOpenBucket_NotFound verifies that the read-path open never creates the
// bucket (WR-05).
func TestOpenBucket_NotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, js := startEmbeddedNATS(t)

	if _, err := OpenBucket(ctx, js); !errors.Is(err, jetstream.ErrBucketNotFound) {
		t.Fatalf("OpenBucket on empty server: got %v, want ErrBucketNotFound", err)
	}
	if _, err := js.KeyValue(ctx, DefaultBucket); !errors.Is(err, jetstream.ErrBucketNotFound) {
		t.Errorf("OpenBucket created the bucket: %v", err)
	}
}

// TestFormatPKValue_NumbersAreCanonical verifies that every spelling of the
// same number maps to one key component, whichever path produced it (WR-06).
func TestFormatPKValue_NumbersAreCanonical(t *testing.T) {
	tests := []struct {
		in   any
		want string
	}{
		{json.Number("1"), "1"},
		{json.Number("1.0"), "1"},
		{json.Number("1e0"), "1"},
		{json.Number("10e-1"), "1"},
		{json.Number("1e3"), "1000"},
		{json.Number("1E+3"), "1000"},
		{json.Number("0.000001"), "0.000001"},
		{json.Number("1e-6"), "0.000001"},
		{json.Number("-0"), "0"},
		{json.Number("-0.0e5"), "0"},
		{json.Number("-1.50e3"), "-1500"},
		{json.Number("12.340"), "12.34"},
		{json.Number("1e21"), "1000000000000000000000"},
		{json.Number("9007199254740993"), "9007199254740993"},
		{json.Number("123456789012345678901234567890.5"), "123456789012345678901234567890.5"},
		{int64(1000), "1000"},
		{int64(-7), "-7"},
		{float64(1), "1"},
		{float64(1e21), "1000000000000000000000"},
		{float64(0.000001), "0.000001"},
		{float64(-2.5), "-2.5"},
		{"1.0", "1.0"}, // strings are never reinterpreted
		{true, "true"},
		{false, "false"},
		{time.Date(2026, 10, 17, 12, 0, 0, 5, time.UTC), "2026-10-17T12:00:00.000000005Z"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%T(%v)", tt.in, tt.in), func(t *testing.T) {
			got, err := FormatPKValue(tt.in)
			if err != nil {
				t.Fatalf("FormatPKValue(%v) error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("FormatPKValue(%v) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestFormatPKValue_Rejects verifies that values with no key form are
// reported rather than encoded as text like "<nil>" (WR-06).
func TestFormatPKValue_Rejects(t *testing.T) {
	for _, in := range []any{nil, math.NaN(), math.Inf(1), json.Number("1e100000"), json.Number("1e-100000"), json.Number("abc"), json.Number(""), json.Number("1e"), []any{1}} {
		if got, err := FormatPKValue(in); !errors.Is(err, ErrInvalidPKValue) {
			t.Errorf("FormatPKValue(%v) = %q, %v; want ErrInvalidPKValue", in, got, err)
		}
	}
}

// TestPKFilter_SelectsOnlyOneView verifies that the server-side watch filter
// returns one view's rows and nothing from other views or schemas (PF-01).
func TestPKFilter_SelectsOnlyOneView(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, js := startEmbeddedNATS(t)
	kvb, err := InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	keys := []string{
		BuildPKKey("users", []string{"a"}, "/"),
		BuildPKKey("users", []string{"b.c", "d"}, "."),
		BuildPKKey("users_archive", []string{"a"}, "/"),
		BuildPKKey("user", []string{"a"}, "/"),
		SchemaKey("users"),
	}
	for _, key := range keys {
		if _, err := kvb.Put(ctx, key, []byte(`{}`)); err != nil {
			t.Fatalf("Put(%q) failed: %v", key, err)
		}
	}

	lister, err := kvb.ListKeysFiltered(ctx, PKFilter("users"))
	if err != nil {
		t.Fatalf("ListKeysFiltered failed: %v", err)
	}
	var got []string
	for key := range lister.Keys() {
		got = append(got, key)
	}
	slices.Sort(got)
	want := []string{keys[0], keys[1]}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("keys matching %q = %q, want %q", PKFilter("users"), got, want)
	}
}

// TestPurgeView_RemovesBothLayouts verifies that PurgeView deletes a view's
// rows and schema in the current and the original key layout, and leaves
// other views alone (WR-09).
func TestPurgeView_RemovesBothLayouts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, js := startEmbeddedNATS(t)
	kvb, err := InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}

	purged := []string{BuildPKKey("users", []string{"a"}, "/"), SchemaKey("users"), "users/pk/a", "users/meta/schema"}
	kept := []string{BuildPKKey("users_archive", []string{"a"}, "/"), "users_archive/pk/a", SchemaKey("orders")}
	for _, key := range append(slices.Clone(purged), kept...) {
		if _, err := kvb.Put(ctx, key, []byte(`{}`)); err != nil {
			t.Fatalf("Put(%q) failed: %v", key, err)
		}
	}

	legacy, err := PurgeView(ctx, js, kvb, "users")
	if err != nil {
		t.Fatalf("PurgeView failed: %v", err)
	}
	if legacy != 2 {
		t.Errorf("PurgeView removed %d layout-1 keys, want 2", legacy)
	}
	for _, key := range purged {
		if _, err := kvb.Get(ctx, key); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Errorf("Get(%q) after purge: %v, want ErrKeyNotFound", key, err)
		}
	}
	for _, key := range kept {
		if _, err := kvb.Get(ctx, key); err != nil {
			t.Errorf("Get(%q) after purging another view: %v", key, err)
		}
	}

	// HasRows sees only the view's own rows in the current layout.
	for view, want := range map[string]bool{"users": false, "users_archive": true, "orders": false} {
		if got, err := HasRows(ctx, js, view); err != nil || got != want {
			t.Errorf("HasRows(%q) = %v, %v; want %v", view, got, err, want)
		}
	}
}
