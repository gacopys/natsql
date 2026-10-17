package transport_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql/v2/internal/query"
	"github.com/gacopys/natsql/v2/internal/transport"
)

// ---------------------------------------------------------------------------
// Mock handler for transport-level tests
// ---------------------------------------------------------------------------

type mockHandler struct {
	result *query.QueryResult
}

func (m *mockHandler) Query(_ context.Context, _ string) *query.QueryResult {
	return m.result
}

// ---------------------------------------------------------------------------
// Embedded NATS helpers
// ---------------------------------------------------------------------------

func startEmbeddedNATS(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
	t.Helper()
	opts := &server.Options{
		Port:       -1,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		ServerName: "test-transport",
		NoLog:      true,
		NoSigs:     true,
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to start NATS server: %v", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready within 5 seconds")
	}
	nc, err := nats.Connect(srv.ClientURL(), nats.Timeout(5*time.Second))
	if err != nil {
		srv.Shutdown()
		t.Fatalf("failed to connect: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		srv.Shutdown()
		t.Fatalf("failed to create JetStream context: %v", err)
	}
	return srv, nc, js
}

// ---------------------------------------------------------------------------
// Transport tests
// ---------------------------------------------------------------------------

// TestRegisterNATSHandler_FlushError verifies that RegisterNATSHandler
// reports a failed flush after a successful subscribe, and removes the
// subscription it couldn't confirm (CR-19, TS-01).
func TestRegisterNATSHandler_FlushError(t *testing.T) {
	transport.SetFlushTimeout(t, 200*time.Millisecond)

	srv, _, _ := startEmbeddedNATS(t)
	nc, err := nats.Connect(srv.ClientURL(), nats.ReconnectWait(time.Hour), nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	srv.Shutdown()
	deadline := time.Now().Add(5 * time.Second)
	for !nc.IsReconnecting() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !nc.IsReconnecting() {
		t.Fatal("connection did not notice the server shutdown")
	}

	// Subscribing succeeds while reconnecting; only the flush can fail.
	_, err = transport.RegisterNATSHandler(nc, "natsql.query", &mockHandler{result: &query.QueryResult{}}, quietLogger())
	if err == nil || !strings.Contains(err.Error(), "flushing subscription") {
		t.Fatalf("RegisterNATSHandler on a disconnected connection = %v, want a flush error", err)
	}
	if n := nc.NumSubscriptions(); n != 0 {
		t.Errorf("%d subscriptions left after the failed registration, want 0", n)
	}
}

func quietLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// registerNATS registers handler on natsql.query and closes it at cleanup.
func registerNATS(t *testing.T, nc *nats.Conn, handler transport.QueryHandler) {
	t.Helper()
	h, err := transport.RegisterNATSHandler(nc, "natsql.query", handler, quietLogger())
	if err != nil {
		t.Fatalf("RegisterNATSHandler failed: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
}

// TestNATSRequestReply verifies that a NATS request on "natsql.query"
// returns the expected JSON response from the handler.
func TestNATSRequestReply(t *testing.T) {
	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()
	_ = js // available for future use

	handler := &mockHandler{
		result: &query.QueryResult{
			Results: []map[string]any{
				{"id": "u1", "name": "Alice"},
			},
		},
	}

	registerNATS(t, nc, handler)

	// Send request
	msg, err := nc.Request("natsql.query", []byte("SELECT * FROM test_users WHERE id = 'u1'"), 2*time.Second)
	if err != nil {
		t.Fatalf("NATS request failed: %v", err)
	}

	var result query.QueryResult
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		t.Fatalf("unmarshal response failed: %v", err)
	}
	if result.Error != nil {
		t.Fatalf("expected no error, got: %v", *result.Error)
	}
	if len(result.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result.Results))
	}
	if result.Results[0]["id"] != "u1" {
		t.Errorf("id = %v, want %q", result.Results[0]["id"], "u1")
	}
}

// TestNATSRequestError verifies that errors from the handler
// are propagated in the JSON response.
func TestNATSRequestError(t *testing.T) {
	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()
	_ = js

	errMsg := "test error"
	handler := &mockHandler{
		result: &query.QueryResult{
			Error: &errMsg,
		},
	}

	registerNATS(t, nc, handler)

	msg, err := nc.Request("natsql.query", []byte("INVALID SQL"), 2*time.Second)
	if err != nil {
		t.Fatalf("NATS request failed: %v", err)
	}

	var result query.QueryResult
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		t.Fatalf("unmarshal response failed: %v", err)
	}
	if result.Error == nil {
		t.Fatal("expected error, got nil")
	}
	if *result.Error != "test error" {
		t.Errorf("error = %q, want %q", *result.Error, "test error")
	}
}

// TestHTTPQueryEndpoint verifies POST /api/v1/query returns JSON.
func TestHTTPQueryEndpoint(t *testing.T) {
	handler := &mockHandler{
		result: &query.QueryResult{
			Results: []map[string]any{
				{"id": "u1", "name": "Alice"},
			},
		},
	}

	router := transport.NewRouter()
	transport.RegisterHTTPHandler(router, handler, nil)

	ts := httptest.NewServer(router)
	defer ts.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, ts.URL+"/api/v1/query", strings.NewReader(`{"sql":"SELECT * FROM test"}`))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var result query.QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if result.Error != nil {
		t.Fatalf("expected no error, got: %v", *result.Error)
	}
	if len(result.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result.Results))
	}
}

// TestHTTPInvalidBody verifies POST with bad JSON returns 400.
func TestHTTPInvalidBody(t *testing.T) {
	handler := &mockHandler{
		result: &query.QueryResult{},
	}

	router := transport.NewRouter()
	transport.RegisterHTTPHandler(router, handler, nil)

	ts := httptest.NewServer(router)
	defer ts.Close()

	// Send invalid JSON body
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, ts.URL+"/api/v1/query", strings.NewReader(`{invalid json`))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

// TestHTTPContentType verifies the response has Content-Type: application/json.
// TestHTTPBodyTooLarge verifies that oversized request bodies are rejected with 413.
func TestHTTPBodyTooLarge(t *testing.T) {
	handler := &mockHandler{result: &query.QueryResult{}}

	router := transport.NewRouter()
	transport.RegisterHTTPHandler(router, handler, nil)

	ts := httptest.NewServer(router)
	defer ts.Close()

	// Build a body larger than maxRequestBodySize (1MB)
	largeSQL := `{"sql":"SELECT * FROM test WHERE x = '` + strings.Repeat("A", 2<<20) + `'"}`
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, ts.URL+"/api/v1/query", strings.NewReader(largeSQL))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want %d (413)", resp.StatusCode, http.StatusRequestEntityTooLarge)
	}
}

// TestHTTPBodyTrailingData verifies that trailing non-whitespace data
// after the JSON body is rejected with 400.
func TestHTTPBodyTrailingData(t *testing.T) {
	handler := &mockHandler{
		result: &query.QueryResult{},
	}

	router := transport.NewRouter()
	transport.RegisterHTTPHandler(router, handler, nil)

	ts := httptest.NewServer(router)
	defer ts.Close()

	// Body with trailing non-whitespace data
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, ts.URL+"/api/v1/query",
		strings.NewReader(`{"sql":"SELECT 1"}extra`))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want %d (400 Bad Request)", resp.StatusCode, http.StatusBadRequest)
	}
}

// TestHTTPBodyTrailingWhitespaceOK verifies that trailing whitespace
// after the JSON body is accepted (200 OK).
func TestHTTPBodyTrailingWhitespaceOK(t *testing.T) {
	handler := &mockHandler{
		result: &query.QueryResult{},
	}

	router := transport.NewRouter()
	transport.RegisterHTTPHandler(router, handler, nil)

	ts := httptest.NewServer(router)
	defer ts.Close()

	// Body with trailing whitespace/newline — should be accepted
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, ts.URL+"/api/v1/query",
		strings.NewReader("{\"sql\":\"SELECT 1\"}  \n  "))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d (200 OK)", resp.StatusCode, http.StatusOK)
	}
}

func TestHTTPContentType(t *testing.T) {
	handler := &mockHandler{
		result: &query.QueryResult{},
	}

	router := transport.NewRouter()
	transport.RegisterHTTPHandler(router, handler, nil)

	ts := httptest.NewServer(router)
	defer ts.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, ts.URL+"/api/v1/query", strings.NewReader(`{"sql":"SELECT * FROM test"}`))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/json")
	}
}

// funcHandler answers queries with a function.
type funcHandler func(ctx context.Context, sql string) *query.QueryResult

func (f funcHandler) Query(ctx context.Context, sql string) *query.QueryResult { return f(ctx, sql) }

func requestResult(t *testing.T, nc *nats.Conn, sql string, timeout time.Duration) query.QueryResult {
	t.Helper()
	msg, err := nc.Request("natsql.query", []byte(sql), timeout)
	if err != nil {
		t.Fatalf("request %q failed: %v", sql, err)
	}
	var result query.QueryResult
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		t.Fatalf("reply %q is not a JSON envelope: %v", msg.Data, err)
	}
	return result
}

// TestNATSHandler_SlowQueryDoesNotBlockOthers verifies that requests are
// served concurrently, so a long scan doesn't hold up a PK lookup queued
// behind it (TR-01).
func TestNATSHandler_SlowQueryDoesNotBlockOthers(t *testing.T) {
	srv, nc, _ := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	release := make(chan struct{})
	defer close(release)
	registerNATS(t, nc, funcHandler(func(ctx context.Context, sql string) *query.QueryResult {
		if sql == "slow" {
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return &query.QueryResult{Results: []map[string]any{{"sql": sql}}}
	}))

	if err := nc.PublishRequest("natsql.query", nats.NewInbox(), []byte("slow")); err != nil {
		t.Fatalf("publishing the slow query failed: %v", err)
	}
	result := requestResult(t, nc, "fast", time.Second)
	if len(result.Results) != 1 || result.Results[0]["sql"] != "fast" {
		t.Errorf("fast query result = %+v", result)
	}
}

// TestNATSHandler_QueueGroup verifies that engines sharing the query subject
// split requests instead of each executing every one (TR-01).
func TestNATSHandler_QueueGroup(t *testing.T) {
	srv, nc, _ := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	var calls atomic.Int32
	counting := funcHandler(func(context.Context, string) *query.QueryResult {
		calls.Add(1)
		return &query.QueryResult{Results: []map[string]any{}}
	})
	registerNATS(t, nc, counting)
	registerNATS(t, nc, counting)

	for range 10 {
		requestResult(t, nc, "q", time.Second)
	}
	if got := calls.Load(); got != 10 {
		t.Errorf("10 requests executed %d queries, want 10", got)
	}
}

// TestNATSHandler_ResultTooLarge verifies that a result over the max
// payload gets an error reply instead of no reply (TR-02).
func TestNATSHandler_ResultTooLarge(t *testing.T) {
	srv, nc, _ := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	big := strings.Repeat("x", int(nc.MaxPayload()))
	registerNATS(t, nc, &mockHandler{result: &query.QueryResult{Results: []map[string]any{{"big": big}}}})

	result := requestResult(t, nc, "SELECT * FROM v WHERE id != ''", 2*time.Second)
	if result.Error == nil || !strings.Contains(*result.Error, "result too large") {
		t.Errorf("oversized result reply = %+v, want a too-large error", result)
	}
}

// TestNATSHandler_ErrorEnvelopeIsJSON verifies that an unencodable result
// still gets a valid JSON error reply (TR-08).
func TestNATSHandler_ErrorEnvelopeIsJSON(t *testing.T) {
	srv, nc, _ := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	registerNATS(t, nc, &mockHandler{result: &query.QueryResult{Results: []map[string]any{{"n": json.Number(`"invalid"`)}}}})
	result := requestResult(t, nc, "q", 2*time.Second)
	if result.Error == nil || !strings.Contains(*result.Error, "internal error") {
		t.Errorf("reply = %+v, want an internal error", result)
	}
}

// TestNATSHandler_CloseWaitsForInFlight verifies that Close lets a running
// query finish and reply before returning.
func TestNATSHandler_CloseWaitsForInFlight(t *testing.T) {
	srv, nc, _ := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()

	started := make(chan struct{})
	h, err := transport.RegisterNATSHandler(nc, "natsql.query", funcHandler(func(context.Context, string) *query.QueryResult {
		close(started)
		time.Sleep(200 * time.Millisecond)
		return &query.QueryResult{Results: []map[string]any{{"done": true}}}
	}), quietLogger())
	if err != nil {
		t.Fatalf("RegisterNATSHandler failed: %v", err)
	}

	replies := make(chan *nats.Msg, 1)
	go func() {
		msg, err := nc.Request("natsql.query", []byte("q"), 5*time.Second)
		if err != nil {
			msg = nil // reported below as a missing reply
		}
		replies <- msg
	}()
	<-started
	if err := h.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	select {
	case msg := <-replies:
		if msg == nil || !strings.Contains(string(msg.Data), `"done":true`) {
			t.Errorf("in-flight query reply = %v, want its result", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight query was never answered")
	}
	if err := h.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
}

type fakeHealth struct {
	ready bool
}

func (f fakeHealth) Health() (bool, any) { return f.ready, map[string]any{"ready": f.ready} }

// TestHTTPHealthProbes verifies the liveness and readiness endpoints
// (OP-03).
func TestHTTPHealthProbes(t *testing.T) {
	for _, ready := range []bool{true, false} {
		router := transport.NewRouter()
		transport.RegisterHTTPHandler(router, &mockHandler{}, fakeHealth{ready: ready})

		for path, want := range map[string]int{"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable} {
			if ready {
				want = http.StatusOK
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
			if w.Code != want {
				t.Errorf("ready=%v GET %s = %d, want %d", ready, path, w.Code, want)
			}
			if !strings.Contains(w.Body.String(), `"ready":`) {
				t.Errorf("ready=%v GET %s body = %q, want the status document", ready, path, w.Body.String())
			}
		}
	}
}
