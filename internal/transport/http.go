package transport

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/gacopys/natsql/internal/transport/oapi"
)

const maxRequestBodySize = 1 << 20 // 1 MB

// QueryRequest is the JSON body for POST /api/v1/query.
type QueryRequest = oapi.QueryRequest

// httpAdapter implements oapi.ServerInterface by delegating to a QueryHandler.
// It preserves the canonical body validation (D-17/D-18, T-02-06): a 1 MiB
// cap, a single JSON document requirement (no trailing non-whitespace data),
// and the standard QueryResult envelope (D-29) as the response body.
type httpAdapter struct {
	handler QueryHandler
}

// RunQuery handles POST /api/v1/query. The HTTP status is always 200 for a
// syntactically valid request body — query-time failures are reported via the
// `error` field of the JSON envelope (mirrors the NATS request-reply path).
//
// (POST /api/v1/query)
func (a *httpAdapter) RunQuery(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)

	var req QueryRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"results":[],"error":"request body too large (max %d bytes)"}`, maxRequestBodySize))) // best-effort write; response already committed
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"results":[],"error":"invalid JSON body"}`)) // best-effort write; caller sees 400 status
		return
	}
	// Check for trailing data after JSON body (D-17/D-18)
	// Use json.Decoder's double-decode pattern since the decoder's
	// internal bufio buffer may have consumed the underlying reader.
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"results":[],"error":"unexpected data after JSON body"}`)) // best-effort write; caller sees 400 status
		return
	}
	result := a.handler.Query(r.Context(), req.Sql)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result) // best-effort encode; response already committing
}

// RegisterHTTPHandler mounts the query endpoint on the chi router (D-37),
// using the chi server interface generated from openapi.yaml. The endpoint
// expects POST with JSON body {"sql": "..."} and returns the standard JSON
// query result envelope per D-29.
func RegisterHTTPHandler(router chi.Router, handler QueryHandler) {
	_ = oapi.HandlerFromMux(&httpAdapter{handler: handler}, router)
}

// NewRouter creates a chi router with standard middleware for the query API.
func NewRouter() chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))
	return r
}
