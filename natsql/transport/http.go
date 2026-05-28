package transport

import (
	"github.com/go-chi/chi/v5"
)

// RegisterHTTPHandler mounts the query endpoint on the chi router (D-37).
func RegisterHTTPHandler(router chi.Router, handler QueryHandler) {
	// stub - no-op
}

// NewRouter creates a chi router with standard middleware for the query API.
func NewRouter() chi.Router {
	return chi.NewRouter()
}
