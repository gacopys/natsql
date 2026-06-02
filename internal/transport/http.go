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
)

const maxRequestBodySize = 1 << 20 // 1 MB

// QueryRequest is the JSON body for POST /api/v1/query.
type QueryRequest struct {
	SQL string `json:"sql"`
}

// RegisterHTTPHandler mounts the query endpoint on the chi router (D-37).
// The endpoint expects POST with JSON body {"sql": "..."} and returns
// the standard JSON query result envelope per D-29.
func RegisterHTTPHandler(router chi.Router, handler QueryHandler) {
	router.Post("/api/v1/query", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)

		var req QueryRequest
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&req); err != nil {
			w.Header().Set("Content-Type", "application/json")
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				w.Write([]byte(fmt.Sprintf(`{"results":[],"error":"request body too large (max %d bytes)"}`, maxRequestBodySize)))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"results":[],"error":"invalid JSON body"}`))
			return
		}
		// Check for trailing data after JSON body (D-17/D-18)
		// Use json.Decoder's double-decode pattern since the decoder's
		// internal bufio buffer may have consumed the underlying reader.
		var trailing json.RawMessage
		if err := decoder.Decode(&trailing); err != io.EOF {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"results":[],"error":"unexpected data after JSON body"}`))
			return
		}
		result := handler.Query(r.Context(), req.SQL)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})
}

// NewRouter creates a chi router with standard middleware for the query API.
func NewRouter() chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))
	return r
}
