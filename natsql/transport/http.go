package transport

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// QueryRequest is the JSON body for POST /api/v1/query.
type QueryRequest struct {
	SQL string `json:"sql"`
}

// RegisterHTTPHandler mounts the query endpoint on the chi router (D-37).
// The endpoint expects POST with JSON body {"sql": "..."} and returns
// the standard JSON query result envelope per D-29.
func RegisterHTTPHandler(router chi.Router, handler QueryHandler) {
	router.Post("/api/v1/query", func(w http.ResponseWriter, r *http.Request) {
		var req QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"results":[],"error":"invalid JSON body"}`))
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
