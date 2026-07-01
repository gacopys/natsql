// Package transport provides NATS request-reply and HTTP handlers
// for executing SQL queries against the engine.
package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/gacopys/natsql/internal/query"
)

// QueryHandler is the interface transports use to execute queries.
type QueryHandler interface {
	Query(ctx context.Context, sql string) *query.QueryResult
}

// RegisterNATSHandler subscribes to natsql.query (D-34) and responds
// with the JSON query result. The request body is the raw SQL string (D-35).
// Returns the subscription for lifecycle management.
func RegisterNATSHandler(nc *nats.Conn, handler QueryHandler) (*nats.Subscription, error) {
	sub, err := nc.Subscribe("natsql.query", func(msg *nats.Msg) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		sql := string(msg.Data)
		result := handler.Query(ctx, sql)
		data, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			errResp := fmt.Sprintf(`{"results":[],"error":"internal error: %s"}`, marshalErr.Error())
			if respondErr := msg.Respond([]byte(errResp)); respondErr != nil {
				slog.Warn("failed to respond with error to NATS query", "error", respondErr)
			}
			return
		}
		if err := msg.Respond(data); err != nil {
			slog.Warn("failed to respond to NATS query", "error", err)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("subscribing to natsql.query: %w", err)
	}
	if err := nc.Flush(); err != nil { // CR-19: verify subscription is registered before returning
		_ = sub.Unsubscribe()
		return nil, fmt.Errorf("flushing subscription: %w", err)
	}
	return sub, nil
}
