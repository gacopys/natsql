package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql/v2/internal/kv"
	"github.com/gacopys/natsql/v2/internal/testutil"
)

// runSQL parses, validates, plans and executes sql against kvb.
func runSQL(ctx context.Context, t *testing.T, kvb jetstream.KeyValue, schema *kv.ViewSchema, sql string) ([]map[string]any, Plan) {
	t.Helper()
	q, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q) failed: %v", sql, err)
	}
	if err := Validate(q, schema); err != nil {
		t.Fatalf("Validate(%q) failed: %v", sql, err)
	}
	plan, err := BuildPlan(q, schema)
	if err != nil {
		t.Fatalf("BuildPlan(%q) failed: %v", sql, err)
	}
	rows, err := plan.Execute(ctx, kvb)
	if err != nil {
		t.Fatalf("Execute(%q) failed: %v", sql, err)
	}
	return rows, plan
}

// putRow stores row under the key the materializer would use for pkParts.
func putRow(ctx context.Context, t *testing.T, kvb jetstream.KeyValue, schema *kv.ViewSchema, pkParts []string, row string) {
	t.Helper()
	if _, err := kvb.Put(ctx, kv.BuildPKKey(schema.Name, pkParts, schema.KeySeparator), []byte(row)); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
}

var reviewSchema = &kv.ViewSchema{
	Name:         "review",
	Columns:      []kv.ColumnSchema{{Name: "id", Type: "string", PrimaryKey: true}, {Name: "value", Type: "number"}},
	KeyFields:    []string{"id"},
	KeySeparator: "/",
}

// TestQuery_OutputKeysForEveryPlan verifies that SELECT * returns exactly the
// schema's columns, never _meta or fields outside the schema, on every plan
// and in mixed projections (QE-01).
func TestQuery_OutputKeysForEveryPlan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, js := testutil.StartEmbeddedNATS(t)
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}
	putRow(ctx, t, kvb, reviewSchema, []string{"a"}, `{"id":"a","value":1,"dropped_column":"x","_meta":{"stream_seq":1}}`)

	tests := []struct {
		sql      string
		wantKeys []string
		wantPlan string
	}{
		{"SELECT * FROM review WHERE id = 'a'", []string{"id", "value"}, "*query.PKLookupPlan"},
		{"SELECT * FROM review WHERE value = 1", []string{"id", "value"}, "*query.FullScanPlan"},
		{"SELECT * FROM review WHERE id IN ('a')", []string{"id", "value"}, "*query.PKLookupPlan"},
		{"SELECT *, id FROM review WHERE id = 'a'", []string{"id", "value"}, "*query.PKLookupPlan"},
		{"SELECT *, value FROM review WHERE value != 2", []string{"id", "value"}, "*query.FullScanPlan"},
		{"SELECT value FROM review WHERE value = 1", []string{"value"}, "*query.FullScanPlan"},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			rows, plan := runSQL(ctx, t, kvb, reviewSchema, tt.sql)
			if got := fmt.Sprintf("%T", plan); got != tt.wantPlan {
				t.Errorf("plan = %s, want %s", got, tt.wantPlan)
			}
			if len(rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rows))
			}
			if got := slices.Sorted(maps.Keys(rows[0])); !slices.Equal(got, tt.wantKeys) {
				t.Errorf("output keys = %q, want %q", got, tt.wantKeys)
			}
		})
	}
}

// TestQuery_LimitAndOffset verifies LIMIT 0 returns nothing on every plan,
// LIMIT n caps PK IN lookups too, and OFFSET is rejected (QE-03).
func TestQuery_LimitAndOffset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, js := testutil.StartEmbeddedNATS(t)
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}
	for _, id := range []string{"a", "b", "c"} {
		putRow(ctx, t, kvb, reviewSchema, []string{id}, `{"id":"`+id+`","value":1}`)
	}

	for sql, want := range map[string]int{
		"SELECT * FROM review WHERE id = 'a' LIMIT 0":              0,
		"SELECT * FROM review WHERE value = 1 LIMIT 0":             0,
		"SELECT * FROM review WHERE id IN ('a','b') LIMIT 0":       0,
		"SELECT * FROM review WHERE id IN ('a','b','c') LIMIT 2":   2,
		"SELECT * FROM review WHERE value = 1 LIMIT 2":             2,
		"SELECT * FROM review WHERE value = 1":                     3,
		"SELECT * FROM review WHERE id IN ('a','b','c','a')":       3,
		"SELECT * FROM review WHERE id IN ('a','zz') AND id = 'a'": 1,
	} {
		t.Run(sql, func(t *testing.T) {
			if rows, _ := runSQL(ctx, t, kvb, reviewSchema, sql); len(rows) != want {
				t.Errorf("got %d rows, want %d", len(rows), want)
			}
		})
	}

	for _, sql := range []string{
		"SELECT * FROM review WHERE id = 'a' LIMIT 1 OFFSET 1",
		"SELECT * FROM review WHERE id = 'a' LIMIT 1, 1",
	} {
		if _, err := Parse(sql); err == nil || !strings.Contains(err.Error(), "OFFSET") {
			t.Errorf("Parse(%q) = %v, want an OFFSET error", sql, err)
		}
	}
}

// TestBuildPlan_PKShapes verifies which PK conditions become point lookups
// and which are answered without I/O (PF-01, D-02, QE-10).
func TestBuildPlan_PKShapes(t *testing.T) {
	composite := &kv.ViewSchema{
		Name:         "orders",
		Columns:      []kv.ColumnSchema{{Name: "a", Type: "string", PrimaryKey: true}, {Name: "b", Type: "number", PrimaryKey: true}, {Name: "c", Type: "string"}},
		KeyFields:    []string{"a", "b"},
		KeySeparator: "/",
	}
	tests := []struct {
		sql      string
		wantPlan string
		wantKeys [][]string
	}{
		{"SELECT * FROM orders WHERE a = 'x' AND b = 1", "*query.PKLookupPlan", [][]string{{"x", "1"}}},
		{"SELECT * FROM orders WHERE a IN ('x','y') AND b = 1.0", "*query.PKLookupPlan", [][]string{{"x", "1"}, {"y", "1"}}},
		{"SELECT * FROM orders WHERE a = 'x' AND b IN (1, 2, 1e0)", "*query.PKLookupPlan", [][]string{{"x", "1"}, {"x", "2"}}},
		{"SELECT * FROM orders WHERE a = 'x' AND b = NULL", "*query.EmptyPlan", nil},
		{"SELECT * FROM orders WHERE a = 'x' AND a = 'y'", "*query.EmptyPlan", nil}, // contradiction without b (QE-10)
		{"SELECT * FROM orders WHERE a = 'x'", "*query.FullScanPlan", nil},
		{"SELECT * FROM orders WHERE a != 'x' AND b = 1", "*query.FullScanPlan", nil},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			q, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			plan, err := BuildPlan(q, composite)
			if err != nil {
				t.Fatalf("BuildPlan failed: %v", err)
			}
			if got := fmt.Sprintf("%T", plan); got != tt.wantPlan {
				t.Fatalf("plan = %s, want %s", got, tt.wantPlan)
			}
			if pk, ok := plan.(*PKLookupPlan); ok && !slices.EqualFunc(pk.Keys, tt.wantKeys, slices.Equal) {
				t.Errorf("keys = %q, want %q", pk.Keys, tt.wantKeys)
			}
		})
	}

	// An IN list too long for point lookups falls back to one scan.
	values := make([]string, maxPKLookups+1)
	for i := range values {
		values[i] = fmt.Sprintf("'%d'", i)
	}
	q, err := Parse("SELECT * FROM orders WHERE b = 1 AND a IN (" + strings.Join(values, ",") + ")")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if plan, _ := BuildPlan(q, composite); fmt.Sprintf("%T", plan) != "*query.FullScanPlan" {
		t.Errorf("IN list of %d values planned as %T, want a full scan", len(values), plan)
	}
}

// TestQuery_NumericPKSpellings verifies that every spelling of a numeric PK
// finds the row the materializer stored under the canonical key, by '=' and
// by IN alike (WR-06).
func TestQuery_NumericPKSpellings(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, js := testutil.StartEmbeddedNATS(t)
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}
	schema := &kv.ViewSchema{
		Name:         "nums",
		Columns:      []kv.ColumnSchema{{Name: "id", Type: "number", PrimaryKey: true}},
		KeyFields:    []string{"id"},
		KeySeparator: "/",
	}
	for _, stored := range []string{"1.0", "1e3", "0.000001", "9007199254740993"} {
		key, err := kv.FormatPKValue(json.Number(stored)) // what the mapper does
		if err != nil {
			t.Fatalf("FormatPKValue(%s) failed: %v", stored, err)
		}
		putRow(ctx, t, kvb, schema, []string{key}, `{"id":`+stored+`}`)
	}

	for _, literal := range []string{"1", "1.0", "1.00", "1e0", "1000", "1e3", "1000.0", "0.000001", "1e-6", "9007199254740993"} {
		for _, form := range []string{"id = %s", "id IN (%s)"} {
			sql := "SELECT id FROM nums WHERE " + fmt.Sprintf(form, literal)
			if rows, plan := runSQL(ctx, t, kvb, schema, sql); len(rows) != 1 {
				t.Errorf("%s (%T) returned %d rows, want 1", sql, plan, len(rows))
			}
		}
	}
}

// TestFullScan_OnlyReadsItsView verifies that a scan receives only the
// target view's rows from the server, not the rest of the bucket (PF-01).
func TestFullScan_OnlyReadsItsView(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nc, js := testutil.StartEmbeddedNATS(t)
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}
	padding := strings.Repeat("x", 256)
	for i := range 500 {
		if _, err := kvb.Put(ctx, kv.BuildPKKey("unrelated", []string{strconv.Itoa(i)}, "/"), []byte(`{"padding":"`+padding+`"}`)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	putRow(ctx, t, kvb, reviewSchema, []string{"a"}, `{"id":"a","value":1}`)

	before := nc.Stats().InBytes
	rows, _ := runSQL(ctx, t, kvb, reviewSchema, "SELECT * FROM review WHERE value = 1")
	received := nc.Stats().InBytes - before
	if len(rows) != 1 {
		t.Errorf("got %d rows, want 1", len(rows))
	}
	if received > 20_000 { // the unrelated rows alone are over 128 KiB
		t.Errorf("scan of a one-row view received %d bytes", received)
	}
}

// TestFullScan_SkipsDeleteMarkers verifies that a key deleted with the KV
// API doesn't break scans of its view (QE-09).
func TestFullScan_SkipsDeleteMarkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, js := testutil.StartEmbeddedNATS(t)
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}
	putRow(ctx, t, kvb, reviewSchema, []string{"a"}, `{"id":"a","value":1}`)
	putRow(ctx, t, kvb, reviewSchema, []string{"b"}, `{"id":"b","value":1}`)
	if err := kvb.Delete(ctx, kv.BuildPKKey("review", []string{"a"}, "/")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if rows, _ := runSQL(ctx, t, kvb, reviewSchema, "SELECT id FROM review WHERE value = 1"); len(rows) != 1 {
		t.Errorf("got %d rows, want 1", len(rows))
	}
}

// stalledWatcher never delivers anything until it is stopped.
type stalledWatcher struct{ updates chan jetstream.KeyValueEntry }

func (w *stalledWatcher) Updates() <-chan jetstream.KeyValueEntry { return w.updates }
func (w *stalledWatcher) Stop() error                             { close(w.updates); return nil }

type stalledKV struct {
	jetstream.KeyValue
	watcher *stalledWatcher
}

func (k stalledKV) Watch(context.Context, string, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	return k.watcher, nil
}

// cancelOnWatchKV cancels the query context right after the real watch is
// created, before its snapshot has been read.
type cancelOnWatchKV struct {
	jetstream.KeyValue
	cancel context.CancelFunc
}

func (k cancelOnWatchKV) Watch(ctx context.Context, keys string, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	w, err := k.KeyValue.Watch(ctx, keys, opts...)
	k.cancel()
	return w, err
}

// TestFullScan_CancellationIsAnError verifies that a scan interrupted by its
// context reports an error instead of returning partial results as success
// (QE-02).
func TestFullScan_CancellationIsAnError(t *testing.T) {
	t.Run("while waiting for updates", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := (&FullScanPlan{ViewName: "review"}).Execute(ctx, stalledKV{watcher: &stalledWatcher{updates: make(chan jetstream.KeyValueEntry)}})
			done <- err
		}()
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, errScanIncomplete) || !errors.Is(err, context.Canceled) {
				t.Errorf("canceled scan returned %v, want an incomplete-scan error", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("scan ignored cancellation")
		}
	})

	t.Run("before the snapshot completes", func(t *testing.T) {
		ctx := context.Background()
		_, js := testutil.StartEmbeddedNATS(t)
		kvb, err := kv.InitBucket(ctx, js, 1)
		if err != nil {
			t.Fatalf("InitBucket failed: %v", err)
		}
		// More rows than the watcher buffers, so the snapshot can't be
		// complete when the context is canceled.
		const n = 1000
		for i := range n {
			putRow(ctx, t, kvb, reviewSchema, []string{strconv.Itoa(i)}, `{"id":"a","value":1}`)
		}

		for range 20 {
			runCtx, cancel := context.WithCancel(ctx)
			rows, err := (&FullScanPlan{ViewName: "review"}).Execute(runCtx, cancelOnWatchKV{KeyValue: kvb, cancel: cancel})
			cancel()
			if err == nil && len(rows) != n {
				t.Fatalf("canceled scan returned %d of %d rows as success", len(rows), n)
			}
			if err != nil && !errors.Is(err, errScanIncomplete) {
				t.Fatalf("canceled scan error = %v, want an incomplete-scan error", err)
			}
		}
	})
}

// TestFullScan_DecodeErrorStopsScan verifies that an undecodable row fails
// the scan.
func TestFullScan_DecodeErrorStopsScan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, js := testutil.StartEmbeddedNATS(t)
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}
	putRow(ctx, t, kvb, reviewSchema, []string{"a"}, `not json`)
	if _, err := (&FullScanPlan{ViewName: "review"}).Execute(ctx, kvb); err == nil || !strings.Contains(err.Error(), "unmarshaling row") {
		t.Errorf("scan over an undecodable row = %v, want an unmarshaling error", err)
	}
}

// fullBufferKV waits until the watcher's 256-entry buffer is full before
// handing it to the scan, so the nats.go callback is blocked on a send.
type fullBufferKV struct{ jetstream.KeyValue }

func (k fullBufferKV) Watch(ctx context.Context, keys string, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	w, err := k.KeyValue.Watch(ctx, keys, opts...)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(w.Updates()) < cap(w.Updates()) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	return w, nil
}

// TestFullScan_LimitDoesNotLeakWatchers verifies that stopping a scan early
// releases the watcher's callback goroutine even when its buffer is full
// (QE-04).
func TestFullScan_LimitDoesNotLeakWatchers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, js := testutil.StartEmbeddedNATS(t)
	kvb, err := kv.InitBucket(ctx, js, 1)
	if err != nil {
		t.Fatalf("InitBucket failed: %v", err)
	}
	for i := range 1000 {
		putRow(ctx, t, kvb, reviewSchema, []string{strconv.Itoa(i)}, `{"id":"x","value":1}`)
	}

	callbacks := func() int {
		buf := make([]byte, 1<<22)
		n := runtime.Stack(buf, true)
		return strings.Count(string(buf[:n]), "jetstream.(*kvs).WatchFiltered.func1")
	}
	before := callbacks()
	for range 5 {
		rows, err := (&FullScanPlan{ViewName: "review", Limit: 1}).Execute(ctx, fullBufferKV{kvb})
		if err != nil || len(rows) != 1 {
			t.Fatalf("LIMIT 1 scan = %d rows, %v", len(rows), err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for callbacks() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := callbacks(); after > before {
		t.Errorf("watch callbacks still running after 5 LIMIT scans: before=%d after=%d", before, after)
	}
}
