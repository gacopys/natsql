package transport_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gacopys/natsql/internal/query"
	"github.com/gacopys/natsql/internal/transport"
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
// returns any Flush error to the caller.
func TestRegisterNATSHandler_FlushError(t *testing.T) {
	srv, nc, js := startEmbeddedNATS(t)
	defer srv.Shutdown()
	defer nc.Close()
	_ = js

	handler := &mockHandler{
		result: &query.QueryResult{},
	}

	// Close NATS connection before registering — Flush will fail
	nc.Close()

	_, err := transport.RegisterNATSHandler(nc, handler)
	if err == nil {
		t.Fatal("expected error from RegisterNATSHandler with closed connection, got nil")
	}
	t.Logf("Got expected error: %v", err)
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

	sub, err := transport.RegisterNATSHandler(nc, handler)
	if err != nil {
		t.Fatalf("RegisterNATSHandler failed: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

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

	sub, err := transport.RegisterNATSHandler(nc, handler)
	if err != nil {
		t.Fatalf("RegisterNATSHandler failed: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

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
	transport.RegisterHTTPHandler(router, handler)

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
	transport.RegisterHTTPHandler(router, handler)

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
	transport.RegisterHTTPHandler(router, handler)

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
	transport.RegisterHTTPHandler(router, handler)

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
	transport.RegisterHTTPHandler(router, handler)

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
	transport.RegisterHTTPHandler(router, handler)

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
