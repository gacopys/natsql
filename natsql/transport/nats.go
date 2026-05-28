// Package transport provides NATS request-reply and HTTP handlers
// for executing SQL queries against the engine.
package transport

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"

	"natsql/query"
)

// QueryHandler is the interface transports use to execute queries.
type QueryHandler interface {
	Query(ctx context.Context, sql string) *query.QueryResult
}

// RegisterNATSHandler subscribes to natsql.query (D-34) and responds
// with the JSON query result. The request body is the raw SQL string (D-35).
// Returns the subscription for lifecycle management.
func RegisterNATSHandler(nc *nats.Conn, handler QueryHandler) (*nats.Subscription, error) {
	return nil, fmt.Errorf("not implemented")
}
