// Package transport provides NATS request-reply and HTTP handlers
// for executing SQL queries against the engine.
package transport

import (
	"context"
	"encoding/json"
	"fmt"
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
			msg.Respond([]byte(errResp))
			return
		}
		msg.Respond(data)
	})
	if err != nil {
		return nil, fmt.Errorf("subscribing to natsql.query: %w", err)
	}
	nc.Flush()
	return sub, nil
}
